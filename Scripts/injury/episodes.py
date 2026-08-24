"""Injury episodes: when a player went out, how long for, and what he did on return.

The training table for everything else in this package, and useful on its own -- it is
what turns ten seasons of weekly designations into a printable table of average weeks
missed and same-season recurrence rate by body part.

**Three absence signals, unioned, because each one alone is wrong.**

The injury report says ``Out`` and then goes quiet: once a player lands on injured
reserve he stops appearing on it altogether. Measured across 2016-2025, the report alone
yields only 99 skill-position episodes of four weeks or longer, which would suggest long
absences barely happen. Roster status recovers 1,310 reserve stints, 707 of them ending
in a return. And neither notices a player who is quietly absent from the box score. So:

1. ``report_status == "Out"`` on the weekly injury report;
2. an **allowlisted** roster reserve code -- see :func:`reserve_evidence`;
3. absent from the box score while on a roster whose team played.

Signal 3 is **never** an episode opener. It catches healthy scratches and buried
backups as readily as injuries, so it counts only inside a run of absences that
signal 1 or 2 has already vouched for.

**Byes bridge, and are not games missed.** The grid is built from the weeks a team
actually played, the same primitive :func:`Scripts.usage.context.team_games` counts --
rosters' distinct weeks include the bye, and counting it produced an availability of
17/16. A run of absence is consecutive over a player's own sequence of team gamedays, so
a bye in the middle of an absence neither ends it nor inflates ``weeks_out``, and a
mid-season team change does not split it.

**Censoring comes in three kinds and they must not be conflated.** A run that reaches
the end of the schedule is a lower bound on duration. A run that ends because the player
left the league is not a duration observation at all. Treating the second as the first is
how you conclude that knee injuries end careers. See :data:`CENSOR_REASONS`.

**The recovery clock counts appearances, not weeks.** A bye or a rest week after a return
must shorten the history rather than blank a slot in it, so post-return rows are indexed
1..N over the games the player actually played.

Polars throughout, per ``CLAUDE.md``.

Usage::

    python -m Scripts.injury.episodes --rebuild
    python -m Scripts.injury.episodes --report
"""

from __future__ import annotations

import datetime
import json
import subprocess
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import polars as pl

from Scripts import paths
from Scripts.injury import lexicon
from Scripts.usage import context as ctx
from Scripts.usage import nflverse as nv

#: Seasons with the weekly context pulls behind them.
DEFAULT_SEASONS = tuple(range(2016, 2026))

#: The positions a fantasy projection cares about.
SKILL_POSITIONS = ("QB", "RB", "WR", "TE")

#: Roster statuses that can describe an injury absence.
#:
#: ``RES`` is reserve of every kind -- injured, non-football, designated for return --
#: and ``PUP`` is the physically-unable-to-perform list. Which reserve *codes* count is
#: derived rather than assumed; see :func:`reserve_evidence`.
RESERVE_STATUSES = ("RES", "PUP")

#: Seasons in which the league ran a COVID-19 reserve list.
#:
#: Load-bearing, not trivia. ``R59`` appears in 2020 and 2021 only (313 skill-position
#: player-weeks) and ``R62`` in 2020 only (173) -- both are pandemic reserve, not injury,
#: and both are 100% absent, so every generic "is he missing?" rule admits them. Left in,
#: they would have contributed ~456 fabricated episodes concentrated in two seasons.
#: Any code confined to exactly these seasons is a pandemic artifact.
COVID_SEASONS = (2020, 2021)

#: Reserve codes that mean "practising, about to return" rather than "shelved".
#:
#: ``R48`` is designated-for-return. It is 100% absent like ``R01``, but **32.3% of its
#: player-weeks also carry an injury-report row** against ``R01``'s 3.8%, and only 54% of
#: those say ``Out`` against ``R01``'s 90.3% -- the player is back at practice and listed
#: Questionable. Pooling it with ``R01`` would mix the shelved with the nearly-recovered,
#: which is the exact distinction a recovery curve is trying to measure.
DESIGNATED_RETURN_CODES = ("R48",)

#: Minimum player-weeks before a reserve code's behaviour can be judged.
MIN_CODE_ROWS = 25

#: A code must be absent at least this often to count as an absence.
MIN_CODE_ABSENT_RATE = 0.98

#: Why an episode has no observed end.
#:
#: ``season_end`` is a genuine lower bound on duration and may be used by the duration
#: model; ``off_roster`` is not an observation of anything and is excluded from both
#: fits. ``returned`` episodes are the only ones the recovery curve can see.
CENSOR_REASONS = ("returned", "season_end", "off_roster")

#: Appearances after a return that the recovery curve looks at.
POST_RETURN_WINDOW = 6

#: Appearances before an episode that form the baseline.
BASELINE_WINDOW = 4

#: Minimum baseline appearances for a usable ratio.
MIN_BASELINE_APPEARANCES = 3

#: Minimum baseline fantasy points before a ratio means anything.
#:
#: A materiality floor, and it has to be applied to the **control cohort on exactly the
#: same terms** or the comparison is not a comparison. Without it a baseline of 0.02
#: points produces a ratio in the trillions and a handful of deep-bench player-weeks
#: dominate every mean in the table -- measured: control appearance-1 mean of 1.5e13
#: against a floored value near 0.97. Set at 6.0 because that is the filter the
#: measurements in ``docs/plans/27-injury-model.md`` were made under, and a player
#: averaging under six points is not one a projection adjustment will change a decision
#: about.
MIN_BASELINE_POINTS = 6.0

#: Minimum baseline snap share, same reasoning.
#:
#: Separates a starter having a quiet month from a backup who happened to score once.
MIN_BASELINE_SNAP = 0.3

#: Weeks after a return within which a fresh episode counts as a recurrence.
RECURRENCE_WINDOW = 6


# --- reading the inputs ---------------------------------------------------

def _git_sha() -> Optional[str]:
    """The commit the artifact was built from, or None outside a work tree."""
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=paths.REPO_ROOT,
                             capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None if out.returncode == 0 else None


def _require_player_weeks(season: int):
    """Resolve a season's box score, naming the command that builds it.

    ``player_weeks.parquet`` is written by ``GetUsage.R``, not ``GetContext.R``, and the
    two have different upstream availability -- which is exactly why
    :mod:`Scripts.usage.context` and :mod:`Scripts.usage.nflverse` are separate readers.
    Borrowing ``context._require`` here would name the wrong R script in the error.

    Args:
        season: Season year.

    Returns:
        Path: The existing parquet.

    Raises:
        FileNotFoundError: When the season has not been pulled.
    """
    path = nv.player_weeks_path(season)
    if not path.is_file():
        raise FileNotFoundError(
            f"No box score for {season} ({path} is missing). Pull it with "
            f"`Rscript R/GetUsage.R {season} {season}`.")
    return path


def load_weekly(seasons: Sequence[int],
                positions: Sequence[str] = SKILL_POSITIONS) -> pl.DataFrame:
    """Per player-week production and snap share, for players who appeared.

    "Appeared" is the **union** of the box score and the snap count, because each misses
    the other's tail. Measured over 2023-2024: 15.7% of player-weeks with offensive snaps
    record no statistic at all -- a blocking tight end, a decoy receiver -- and 3.3% of
    box-score rows carry no offensive snaps. A player who took twenty snaps and was not
    thrown to is not injured, and a definition that says he is would open an episode on
    every quiet week.

    Args:
        seasons: Season years to read.
        positions: Positions to keep.

    Returns:
        pl.DataFrame: One row per ``(season, week, gsis_id)`` that appeared, with
        ``team``, ``position``, ``fantasy_points_ppr`` and ``offense_pct``.
    """
    frames: List[pl.DataFrame] = []
    for season in sorted(seasons):
        frame = pl.read_parquet(
            _require_player_weeks(season),
            columns=["season", "week", "gsis_id", "position", "team",
                     "fantasy_points_ppr"])
        frames.append(frame.with_columns(pl.col("season").cast(pl.Int32),
                                        pl.col("week").cast(pl.Int32)))
    box = (pl.concat(frames)
           .filter(pl.col("position").is_in(list(positions)))
           .filter(pl.col("gsis_id").is_not_null())
           .unique(subset=["season", "week", "gsis_id"], keep="first"))

    snaps = (ctx.load_snap_counts(seasons)
             .filter(pl.col("gsis_id").is_not_null())
             .select(["season", "week", "gsis_id", "offense_snaps", "offense_pct"])
             .unique(subset=["season", "week", "gsis_id"], keep="first"))

    # Full outer, so a snap-only appearance survives with a null points line rather
    # than being dropped -- it is still evidence the player was on the field.
    merged = box.join(snaps, on=["season", "week", "gsis_id"], how="full",
                      coalesce=True)
    return (merged
            .filter((pl.col("fantasy_points_ppr").is_not_null())
                    | (pl.col("offense_snaps").fill_null(0) > 0))
            .with_columns(pl.col("fantasy_points_ppr").fill_null(0.0))
            .sort(["gsis_id", "season", "week"]))


def team_gamedays(seasons: Sequence[int]) -> pl.DataFrame:
    """The weeks each team actually played.

    Derived from the box score rather than from rosters, which is the distinction
    :func:`Scripts.usage.context.team_games` records: rosters list a player every week
    including the bye, and a denominator built from them reproduces neither the bye nor
    the 2022 Buffalo-Cincinnati game that was never played.

    Args:
        seasons: Season years to read.

    Returns:
        pl.DataFrame: Unique ``(season, week, team)``.
    """
    frames: List[pl.DataFrame] = []
    for season in sorted(seasons):
        frame = pl.read_parquet(_require_player_weeks(season),
                                columns=["season", "week", "team"])
        frames.append(frame.with_columns(pl.col("season").cast(pl.Int32),
                                         pl.col("week").cast(pl.Int32)))
    return (pl.concat(frames).drop_nulls().unique()
            .sort(["season", "team", "week"]))


# --- which reserve codes mean "hurt" --------------------------------------

def reserve_evidence(rosters: pl.DataFrame, appeared: pl.DataFrame,
                     injuries: pl.DataFrame,
                     gamedays: pl.DataFrame) -> pl.DataFrame:
    """Cross-tab every roster status code against absence and report corroboration.

    The allowlist is **derived, not asserted**, and this is the table it is derived
    from. Three columns decide a code:

    ``absent_rate``
        share of player-weeks in which the player did not appear. A code that means
        "unavailable" is absent essentially always.
    ``on_report`` / ``report_out``
        how often the injury report also has something to say. This is what separates
        ``R01`` (3.8% on report, and 90.3% of those say ``Out``) from ``R48``
        (32.3% on report, 54% ``Out``) -- the same absence, a different situation.
    ``seasons``
        which seasons the code appears in at all. The COVID-19 reserve codes are
        indistinguishable from injury reserve on every other column and are given away
        entirely by being confined to 2020-2021.

    Args:
        rosters: :func:`Scripts.usage.context.load_rosters` output.
        appeared: :func:`load_weekly` output.
        injuries: :func:`Scripts.usage.context.load_injuries` output.
        gamedays: :func:`team_gamedays` output.

    Returns:
        pl.DataFrame: One row per ``(status, status_description_abbr)`` with ``n``,
        ``absent_rate``, ``on_report``, ``report_out`` and ``seasons``.
    """
    grid = (rosters.join(gamedays, on=["season", "week", "team"], how="inner")
            .join(appeared.select(["season", "week", "gsis_id"])
                  .with_columns(pl.lit(True).alias("appeared")),
                  on=["season", "week", "gsis_id"], how="left")
            .join(injuries.select(["season", "week", "gsis_id", "report_status"]),
                  on=["season", "week", "gsis_id"], how="left")
            .with_columns(pl.col("appeared").fill_null(False)))

    code = pl.col("status_description_abbr").fill_null("(none)")
    return (grid.with_columns(code.alias("code"))
            .group_by(["status", "code"])
            .agg(pl.len().alias("n"),
                 (~pl.col("appeared")).mean().alias("absent_rate"),
                 pl.col("report_status").is_not_null().mean().alias("on_report"),
                 (pl.col("report_status") == "Out").mean().alias("report_out"),
                 pl.col("season").unique().sort().alias("seasons"))
            .sort(["status", "n"], descending=[False, True]))


def admit_reserve_codes(evidence: pl.DataFrame) -> pl.DataFrame:
    """Decide which ``(status, code)`` pairs count as an injury absence.

    Four rules, each of which exists because a code fails it:

    1. the status must be a reserve status -- ``CUT`` and ``RET`` are 100% absent too,
       and neither is an injury;
    2. the player must actually be absent (:data:`MIN_CODE_ABSENT_RATE`);
    3. there must be enough player-weeks to judge (:data:`MIN_CODE_ROWS`), except for
       the unlabelled code, which is the *only* signal available before 2020 and is
       admitted on its status alone;
    4. the code must not be confined to :data:`COVID_SEASONS`.

    Args:
        evidence: :func:`reserve_evidence` output.

    Returns:
        pl.DataFrame: ``evidence`` with ``admitted`` (bool), ``designated_return``
        (bool) and ``verdict`` (a human-readable reason) added.
    """
    covid = set(COVID_SEASONS)
    rows = []
    for row in evidence.iter_rows(named=True):
        status, code, n = row["status"], row["code"], row["n"]
        seasons = set(row["seasons"] or [])
        absent = row["absent_rate"] or 0.0

        if status not in RESERVE_STATUSES:
            verdict, admitted = f"rejected: {status} is not a reserve status", False
        elif absent < MIN_CODE_ABSENT_RATE:
            verdict, admitted = (
                f"rejected: absent only {absent:.1%} of the time", False)
        elif n < MIN_CODE_ROWS and code != "(none)":
            # Checked before the COVID rule so a small code that happens to fall in
            # 2020-2021 is reported as too small rather than accused of being pandemic
            # bookkeeping -- the verdict string is evidence and has to be accurate.
            verdict, admitted = f"rejected: only {n} player-weeks", False
        elif seasons and seasons <= covid:
            verdict, admitted = (
                "rejected: confined to "
                f"{sorted(seasons)} -- COVID-19 reserve, not injury", False)
        else:
            verdict, admitted = "admitted", True

        rows.append({**row, "admitted": admitted,
                     "designated_return": code in DESIGNATED_RETURN_CODES,
                     "verdict": verdict})
    return pl.DataFrame(rows, schema_overrides={"seasons": pl.List(pl.Int32)})


# --- the episode table ----------------------------------------------------

def absence_grid(seasons: Sequence[int] = DEFAULT_SEASONS,
                 positions: Sequence[str] = SKILL_POSITIONS
                 ) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """One row per player per team gameday, with the absence signals attached.

    Args:
        seasons: Season years to read.
        positions: Positions to keep.

    Returns:
        tuple: ``(grid, code_evidence)``. ``grid`` carries ``slot`` -- the player's own
        index over the gamedays his team played, which is what makes a run of absence
        consecutive across a bye and across a mid-season trade -- plus ``appeared``,
        ``sig_out``, ``sig_reserve`` and ``strong``.
    """
    seasons = sorted(seasons)
    rosters = ctx.load_rosters(seasons, positions=positions)
    weekly = load_weekly(seasons, positions=positions)
    injuries = ctx.load_injuries(seasons)
    gamedays = team_gamedays(seasons)

    evidence = admit_reserve_codes(
        reserve_evidence(rosters, weekly, injuries, gamedays))
    admitted = {(r["status"], r["code"])
                for r in evidence.filter("admitted").iter_rows(named=True)}
    designated = {(r["status"], r["code"])
                  for r in evidence.filter("designated_return").iter_rows(named=True)}

    report = injuries.select(
        ["season", "week", "gsis_id", "report_status", "report_primary_injury"])

    grid = (rosters
            .join(gamedays, on=["season", "week", "team"], how="inner")
            .join(weekly.select(["season", "week", "gsis_id", "fantasy_points_ppr",
                                 "offense_pct"])
                  .with_columns(pl.lit(True).alias("appeared")),
                  on=["season", "week", "gsis_id"], how="left")
            .join(report, on=["season", "week", "gsis_id"], how="left")
            .with_columns(pl.col("appeared").fill_null(False),
                          pl.col("status_description_abbr").fill_null("(none)")
                          .alias("code")))

    pair = pl.concat_str([pl.col("status"), pl.lit("|"), pl.col("code")])
    admitted_keys = [f"{s}|{c}" for s, c in sorted(admitted)]
    designated_keys = [f"{s}|{c}" for s, c in sorted(designated)]

    # A body part naming roster bookkeeping -- a rested starter, a personal matter --
    # is not an injury however the report is worded, so it cannot open an episode.
    injury_part = pl.col("report_primary_injury").map_elements(
        lambda v: lexicon.is_injury(v), return_dtype=pl.Boolean)

    grid = grid.with_columns(
        # Listed Out with a body part that is not roster bookkeeping. A null body part
        # still counts: being listed Out is itself the evidence.
        ((pl.col("report_status") == "Out")
         & (pl.col("report_primary_injury").is_null() | injury_part)
         ).fill_null(False).alias("sig_out"),
        pair.is_in(admitted_keys).alias("sig_reserve"),
        pair.is_in(designated_keys).alias("sig_designated"),
    ).with_columns(
        (pl.col("sig_out") | pl.col("sig_reserve")).alias("strong"))

    grid = grid.sort(["gsis_id", "season", "week"]).with_columns(
        pl.int_range(pl.len()).over(["gsis_id", "season"]).alias("slot"))
    return grid, evidence


def build_episodes(grid: pl.DataFrame,
                   gamedays: pl.DataFrame) -> pl.DataFrame:
    """Group absence weeks into episodes and resolve how each one ended.

    A **run** is a maximal stretch of consecutive gamedays on which the player did not
    appear. A run becomes an **episode** only if it contains at least one strong signal
    -- the injury report or an allowlisted reserve code -- which is what keeps healthy
    scratches and buried backups out.

    ``strong_weeks`` and ``strong_share`` are reported rather than acted on. A ten-week
    run vouched for by a single early ``Out`` may be an injury that moved to reserve, or
    it may be one bad week followed by a benching, and the honest thing is to record
    which runs are thinly corroborated and let the fit decide.

    Args:
        grid: :func:`absence_grid` output.
        gamedays: :func:`team_gamedays` output.

    Returns:
        pl.DataFrame: One row per episode.
    """
    last_gameday = (gamedays.group_by(["season", "team"])
                    .agg(pl.col("week").max().alias("team_last_week")))

    runs = grid.sort(["gsis_id", "season", "slot"]).with_columns(
        (~pl.col("appeared")).alias("absent"))
    # A run id that increments whenever the appeared/absent state flips. Two passes,
    # because Polars will not nest one window expression inside another.
    runs = runs.with_columns(
        (pl.col("absent") != pl.col("absent").shift(1).over(["gsis_id", "season"]))
        .fill_null(True).alias("_flip"))
    runs = runs.with_columns(
        pl.col("_flip").cum_sum().over(["gsis_id", "season"]).alias("run"))

    absent_runs = runs.filter("absent")
    episodes = (absent_runs.group_by(["gsis_id", "season", "run"])
                .agg(pl.col("week").min().alias("first_out_week"),
                     pl.col("week").max().alias("last_out_week"),
                     pl.col("slot").min().alias("first_slot"),
                     pl.col("slot").max().alias("last_slot"),
                     pl.len().cast(pl.Int32).alias("weeks_out"),
                     pl.col("strong").sum().cast(pl.Int32).alias("strong_weeks"),
                     pl.col("sig_out").sum().cast(pl.Int32).alias("report_out_weeks"),
                     pl.col("sig_reserve").sum().cast(pl.Int32).alias("reserve_weeks"),
                     pl.col("sig_designated").any().alias("designated_return"),
                     pl.col("position").first().alias("position"),
                     pl.col("full_name").first().alias("full_name"),
                     pl.col("team").first().alias("team"),
                     pl.col("status").last().alias("end_status"),
                     # The report goes quiet on reserve, so the body part is whatever
                     # it said while it was still talking.
                     pl.col("report_primary_injury").drop_nulls().first()
                     .alias("body_part_raw"))
                .filter(pl.col("strong_weeks") > 0))

    episodes = episodes.with_columns(
        (pl.col("strong_weeks") / pl.col("weeks_out")).alias("strong_share"),
        # ``map_elements`` skips nulls, and a null body part is not an absent value
        # here -- it is 232 episodes averaging 6.7 weeks out where the player went
        # straight onto reserve and the injury report never said a word. They are real
        # injuries of unknown kind, so they land in ``other``, which
        # :data:`Scripts.injury.lexicon.RECOVERY_EXCLUDED_GROUPS` keeps out of the
        # curve fit while availability still counts them.
        pl.col("body_part_raw").fill_null("undisclosed").map_elements(
            lambda v: lexicon.group(v), return_dtype=pl.Utf8).alias("body_part"),
        pl.col("body_part_raw").is_null().alias("body_part_unknown"),
    )

    # How each run ended: the next slot in the player's own grid, if there is one.
    slot_max = (grid.group_by(["gsis_id", "season"])
                .agg(pl.col("slot").max().alias("slot_max"),
                     pl.col("week").max().alias("grid_last_week"),
                     pl.col("team").last().alias("last_team")))
    episodes = (episodes.join(slot_max, on=["gsis_id", "season"], how="left")
                .join(last_gameday, left_on=["season", "last_team"],
                      right_on=["season", "team"], how="left", suffix="_tm"))

    returned = pl.col("last_slot") < pl.col("slot_max")
    ran_out = (pl.col("grid_last_week") >= pl.col("team_last_week")).fill_null(False)
    episodes = episodes.with_columns(
        pl.when(returned).then(pl.lit("returned"))
        .when(ran_out).then(pl.lit("season_end"))
        .otherwise(pl.lit("off_roster")).alias("outcome"))

    # The slot the player came back on, and the week it was.
    return_slot = pl.when(pl.col("outcome") == "returned") \
        .then(pl.col("last_slot") + 1).otherwise(None)
    episodes = episodes.with_columns(return_slot.alias("return_slot"))
    back = grid.select(["gsis_id", "season", "slot", "week"]).rename(
        {"slot": "return_slot", "week": "return_week"})
    episodes = episodes.join(back, on=["gsis_id", "season", "return_slot"],
                             how="left")

    return episodes.drop(["last_team", "team_last_week", "grid_last_week"],
                         strict=False).sort(["season", "gsis_id", "first_out_week"])


# --- what happened after the return ---------------------------------------

def _usable_baseline() -> pl.Expr:
    """The baseline filter, as one expression used by both cohorts.

    Written once and called twice deliberately. The injured curve is only interpretable
    divided by the control curve, and that division is only valid if both sides were
    selected identically -- so the filter cannot be allowed to drift between the two
    call sites, which is what would happen if it were spelled out in each.

    Returns:
        pl.Expr: Boolean over ``baseline_n``, ``base_pts`` and ``base_snap``.
    """
    return ((pl.col("baseline_n") >= MIN_BASELINE_APPEARANCES)
            & (pl.col("base_pts") >= MIN_BASELINE_POINTS)
            & (pl.col("base_snap").fill_null(0.0) >= MIN_BASELINE_SNAP))


def _appearances(grid: pl.DataFrame) -> pl.DataFrame:
    """The rows a player actually played, indexed in order within a season.

    Args:
        grid: :func:`absence_grid` output.

    Returns:
        pl.DataFrame: Appeared rows with ``appearance``, a 1-based index over the games
        the player played that season.
    """
    played = grid.filter("appeared").sort(["gsis_id", "season", "week"])
    return played.with_columns(
        (pl.int_range(pl.len()).over(["gsis_id", "season"]) + 1).alias("appearance"))


def baselines(grid: pl.DataFrame, episodes: pl.DataFrame) -> pl.DataFrame:
    """Each episode's pre-injury level, from the player's own last few games.

    His own recent form, not a positional average: a projection is already anchored on
    the player, and the question here is what the injury did to *him*.

    The window is appearances rather than weeks, so a bye before the injury does not
    silently shorten the baseline to three games.

    Args:
        grid: :func:`absence_grid` output.
        episodes: :func:`build_episodes` output.

    Returns:
        pl.DataFrame: ``(gsis_id, season, run)`` with ``base_pts``, ``base_snap`` and
        ``baseline_n``.
    """
    played = _appearances(grid).select(
        ["gsis_id", "season", "week", "fantasy_points_ppr", "offense_pct"])
    keys = episodes.select(["gsis_id", "season", "run", "first_out_week"])

    joined = played.join(keys, on=["gsis_id", "season"], how="inner").filter(
        pl.col("week") < pl.col("first_out_week"))
    # Rank backwards from the injury and keep the most recent BASELINE_WINDOW.
    joined = joined.sort(["gsis_id", "season", "run", "week"], descending=[
        False, False, False, True]).with_columns(
        pl.int_range(pl.len()).over(["gsis_id", "season", "run"]).alias("back"))

    return (joined.filter(pl.col("back") < BASELINE_WINDOW)
            .group_by(["gsis_id", "season", "run"])
            .agg(pl.col("fantasy_points_ppr").mean().alias("base_pts"),
                 pl.col("offense_pct").mean().alias("base_snap"),
                 pl.len().cast(pl.Int32).alias("baseline_n")))


def post_return(grid: pl.DataFrame, episodes: pl.DataFrame) -> pl.DataFrame:
    """One row per appearance in the window after a return, as a ratio to baseline.

    The unit of the recovery curve. ``appearance_back`` counts games played, not weeks
    elapsed, because a bye or a rest week has to shorten the history rather than leave a
    hole in it -- a curve indexed on calendar weeks would read a bye as a week of
    recovery that never happened.

    Args:
        grid: :func:`absence_grid` output.
        episodes: :func:`build_episodes` output.

    Returns:
        pl.DataFrame: ``gsis_id``, ``season``, ``run``, ``appearance_back`` (1-based),
        ``pts_ratio``, ``snap_ratio``, plus the episode's ``body_part``, ``weeks_out``,
        ``duration_bucket`` and ``position``.
    """
    returned = episodes.filter(pl.col("outcome") == "returned")
    if returned.is_empty():
        return pl.DataFrame()

    base = baselines(grid, returned)
    keys = (returned.select(["gsis_id", "season", "run", "return_week", "body_part",
                             "body_part_unknown", "weeks_out", "position",
                             "strong_share"])
            .join(base, on=["gsis_id", "season", "run"], how="inner")
            .filter(_usable_baseline()))

    played = _appearances(grid).select(
        ["gsis_id", "season", "week", "fantasy_points_ppr", "offense_pct"])

    after = (played.join(keys, on=["gsis_id", "season"], how="inner")
             .filter(pl.col("week") >= pl.col("return_week"))
             .sort(["gsis_id", "season", "run", "week"]))
    after = after.with_columns(
        (pl.int_range(pl.len()).over(["gsis_id", "season", "run"]) + 1)
        .alias("appearance_back"))

    return (after.filter(pl.col("appearance_back") <= POST_RETURN_WINDOW)
            .with_columns(
                (pl.col("fantasy_points_ppr") / pl.col("base_pts")).alias("pts_ratio"),
                pl.when(pl.col("base_snap") > 0)
                .then(pl.col("offense_pct") / pl.col("base_snap"))
                .otherwise(None).alias("snap_ratio"),
                duration_bucket(pl.col("weeks_out")).alias("duration_bucket"))
            .sort(["season", "gsis_id", "run", "appearance_back"]))


def duration_bucket(weeks: pl.Expr) -> pl.Expr:
    """Bucket ``weeks_out`` the way the effect actually splits.

    Finding 5 of the plan measured appearance-1 multipliers of 0.95, 0.75 and 0.66 for
    one, two and three-or-more weeks missed. Duration is the only severity signal that
    exists for all ten seasons -- ``report_primary_injury`` says ``"Ankle"`` and never
    ``"high ankle"`` -- so it stands in for severity, and these are the cut points the
    data put there rather than round numbers.

    Args:
        weeks: Expression yielding weeks out.

    Returns:
        pl.Expr: One of ``"1"``, ``"2"``, ``"3-4"``, ``"5+"``.
    """
    return (pl.when(weeks <= 1).then(pl.lit("1"))
            .when(weeks == 2).then(pl.lit("2"))
            .when(weeks <= 4).then(pl.lit("3-4"))
            .otherwise(pl.lit("5+")))


def control_cohort(grid: pl.DataFrame, episodes: pl.DataFrame) -> pl.DataFrame:
    """The placebo: healthy players measured exactly the way injured ones are.

    **The single most important frame in this package.** Applying the same baseline
    filter to players who were never hurt yields a points ratio of 0.84 that is *flat*
    across all six following appearances -- because a four-game mean is a selected high
    point and weekly fantasy scoring is right-skewed, so "below your own recent average"
    is the normal condition of a healthy player. Measured over 44,409 control
    appearances. A recovery curve fitted against 1.0 instead of against this would
    attribute all of that to injury and haircut every returning player by ~16% for
    reasons that have nothing to do with his ankle.

    Anchored the same way an episode is: a baseline of the previous appearances, then the
    following ``POST_RETURN_WINDOW`` appearances as ratios. Excluded are any players
    inside an episode or within the window after one, and -- so the control is genuinely
    healthy rather than merely playing -- any player carrying an injury-report
    designation of any severity during either the baseline or the window.

    Args:
        grid: :func:`absence_grid` output.
        episodes: :func:`build_episodes` output.

    The raw ``fantasy_points_ppr`` travels alongside the ratio, because the ratio is not
    the quantity the fit uses. **125 of 2,121 post-return appearances score exactly zero**
    -- a player is back on the field and does nothing -- so a mean-of-log-ratios cannot be
    formed and a mean-of-ratios is the wrong estimator for a multiplicative factor anyway.
    :mod:`Scripts.injury.model` divides summed points by summed expectation instead, which
    needs the numerator and denominator rather than their quotient. Dropping those 125 rows
    would discard precisely the worst outcomes and bias every curve upward.

    Returns:
        pl.DataFrame: ``position``, ``anchor``, ``appearance_back``,
        ``fantasy_points_ppr``, ``base_pts``, ``pts_ratio``, ``snap_ratio``.
    """
    played = _appearances(grid).select(
        ["gsis_id", "season", "week", "position", "appearance",
         "fantasy_points_ppr", "offense_pct", "report_status"])

    # Weeks to keep clear of: every absence week, and the window after a return.
    tainted = (episodes.select(["gsis_id", "season", "first_out_week",
                                "last_out_week"])
               .with_columns((pl.col("last_out_week") + POST_RETURN_WINDOW + 1)
                             .alias("clear_until")))

    rows: List[pl.DataFrame] = []
    horizon = BASELINE_WINDOW + POST_RETURN_WINDOW
    max_anchor = int(played["appearance"].max() or 0)
    for anchor in range(BASELINE_WINDOW + 1, max_anchor - POST_RETURN_WINDOW + 2):
        window = played.filter(
            (pl.col("appearance") >= anchor - BASELINE_WINDOW)
            & (pl.col("appearance") <= anchor + POST_RETURN_WINDOW - 1))
        if window.is_empty():
            continue

        # Drop any player-season overlapping an episode or its aftermath anywhere in
        # the window, and any player carrying a designation during it.
        flagged = (window.join(tainted, on=["gsis_id", "season"], how="left")
                   .with_columns(
                       ((pl.col("week") >= pl.col("first_out_week") - 1)
                        & (pl.col("week") < pl.col("clear_until"))
                        ).fill_null(False).alias("near_episode"))
                   .group_by(["gsis_id", "season"])
                   .agg(pl.col("near_episode").any().alias("near_episode"),
                        pl.col("report_status").is_not_null().any().alias("dinged")))
        clean = flagged.filter(~pl.col("near_episode") & ~pl.col("dinged")).select(
            ["gsis_id", "season"])
        if clean.is_empty():
            continue

        window = window.join(clean, on=["gsis_id", "season"], how="inner")
        base = (window.filter(pl.col("appearance") < anchor)
                .group_by(["gsis_id", "season"])
                .agg(pl.col("fantasy_points_ppr").mean().alias("base_pts"),
                     pl.col("offense_pct").mean().alias("base_snap"),
                     pl.len().cast(pl.Int32).alias("baseline_n")))
        forward = (window.filter(pl.col("appearance") >= anchor)
                   .join(base, on=["gsis_id", "season"], how="inner")
                   .filter(_usable_baseline()))
        if forward.is_empty():
            continue
        rows.append(forward.with_columns(
            pl.lit(anchor).cast(pl.Int32).alias("anchor"),
            (pl.col("appearance") - anchor + 1).cast(pl.Int32)
            .alias("appearance_back"),
            (pl.col("fantasy_points_ppr") / pl.col("base_pts")).alias("pts_ratio"),
            pl.when(pl.col("base_snap") > 0)
            .then(pl.col("offense_pct") / pl.col("base_snap"))
            .otherwise(None).alias("snap_ratio"),
        ).select(["gsis_id", "season", "position", "anchor", "appearance_back",
                  "fantasy_points_ppr", "base_pts", "base_snap", "pts_ratio",
                  "snap_ratio"]))

    if not rows:
        return pl.DataFrame()
    return pl.concat(rows)


def recurrence(episodes: pl.DataFrame,
               window: int = RECURRENCE_WINDOW) -> pl.DataFrame:
    """Flag each returned episode with whether the same body part went again.

    The second cost channel, and for some injuries the only one. Hamstrings show almost
    no lasting efficiency loss once a player is back, and an 11.9% chance of going again
    inside six weeks -- so a model that priced only the ramp would call a hamstring
    cheap. That 11.9% also matches the published NFL same-season hamstring reinjury rate
    exactly, which is the external check that the episode logic above is sound.

    Only counts a **fresh episode of the same body part**, so a knee and a hamstring in
    the same season are two injuries rather than a recurrence. Unknown body parts cannot
    recur by definition and are excluded.

    Args:
        episodes: :func:`build_episodes` output.
        window: Weeks after the return within which a new episode counts.

    Returns:
        pl.DataFrame: ``episodes`` with ``recurred`` and ``weeks_to_recurrence``.
    """
    later = episodes.select(
        ["gsis_id", "season", "body_part", "first_out_week", "run"]).rename(
        {"first_out_week": "next_out_week", "run": "next_run"})

    pairs = (episodes.filter(pl.col("outcome") == "returned")
             .join(later, on=["gsis_id", "season", "body_part"], how="left")
             .filter((pl.col("next_run") != pl.col("run"))
                     & (pl.col("next_out_week") > pl.col("last_out_week")))
             .with_columns((pl.col("next_out_week") - pl.col("return_week"))
                           .alias("gap")))

    nearest = (pairs.filter((pl.col("gap") >= 0) & (pl.col("gap") <= window)
                            & (~pl.col("body_part_unknown")))
               .group_by(["gsis_id", "season", "run"])
               .agg(pl.col("gap").min().cast(pl.Int32)
                    .alias("weeks_to_recurrence")))

    return (episodes.join(nearest, on=["gsis_id", "season", "run"], how="left")
            .with_columns(pl.col("weeks_to_recurrence").is_not_null()
                          .alias("recurred")))


# --- artifacts ------------------------------------------------------------

def build(seasons: Sequence[int] = DEFAULT_SEASONS
          ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, Dict]:
    """Build every frame in one pass.

    Args:
        seasons: Season years to read.

    Returns:
        tuple: ``(episodes, post, controls, meta)``.
    """
    seasons = sorted(seasons)
    grid, evidence = absence_grid(seasons)
    gamedays = team_gamedays(seasons)

    episodes = recurrence(build_episodes(grid, gamedays))
    post = post_return(grid, episodes)
    controls = control_cohort(grid, episodes)

    returned = episodes.filter(pl.col("outcome") == "returned")
    by_part = (returned.group_by("body_part")
               .agg(pl.len().alias("episodes"),
                    pl.col("weeks_out").mean().alias("mean_weeks_out"),
                    pl.col("recurred").mean().alias("recurrence_rate"))
               .sort("episodes", descending=True))

    # A body part whose episodes pile up in one season is not a body part, it is a
    # season. The repo's recurring failure mode is an absent source reading as
    # agreement, and the equivalent here is a concentrated cell reading as a fit.
    per_season = (returned.group_by(["body_part", "season"]).len()
                  .join(returned.group_by("body_part").len().rename({"len": "total"}),
                        on="body_part", how="left")
                  .with_columns((pl.col("len") / pl.col("total")).alias("share")))
    concentrated = per_season.filter((pl.col("share") > 0.25)
                                     & (pl.col("total") >= 20))

    unmapped = lexicon.unmapped(episodes["body_part_raw"].drop_nulls().to_list())

    meta = {
        "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "git_sha": _git_sha(),
        "seasons": list(seasons),
        "grid_rows": grid.height,
        "episodes": episodes.height,
        "post_return_rows": post.height,
        "control_rows": controls.height,
        "outcomes": {r["outcome"]: r["len"] for r in
                     episodes.group_by("outcome").len().iter_rows(named=True)},
        "episodes_by_season": {str(r["season"]): r["len"] for r in
                               episodes.group_by("season").len()
                               .sort("season").iter_rows(named=True)},
        "by_body_part": by_part.to_dicts(),
        "concentrated_cells": concentrated.to_dicts(),
        "unmapped_body_parts": unmapped,
        "reserve_code_evidence": evidence.with_columns(
            pl.col("seasons").cast(pl.List(pl.Int64))).to_dicts(),
        "thresholds": {
            "MIN_CODE_ROWS": MIN_CODE_ROWS,
            "MIN_CODE_ABSENT_RATE": MIN_CODE_ABSENT_RATE,
            "MIN_BASELINE_APPEARANCES": MIN_BASELINE_APPEARANCES,
            "MIN_BASELINE_POINTS": MIN_BASELINE_POINTS,
            "MIN_BASELINE_SNAP": MIN_BASELINE_SNAP,
            "POST_RETURN_WINDOW": POST_RETURN_WINDOW,
            "BASELINE_WINDOW": BASELINE_WINDOW,
            "RECURRENCE_WINDOW": RECURRENCE_WINDOW,
            "COVID_SEASONS": list(COVID_SEASONS),
        },
    }
    return episodes, post, controls, meta


def write(episodes: pl.DataFrame, post: pl.DataFrame, controls: pl.DataFrame,
          meta: Dict) -> Dict[str, object]:
    """Persist the frames and their provenance.

    ``Data/NFL/`` is already a mirrored S3 tier, so
    :func:`Scripts.s3_store.mirror_key` publishes these at ``nfl/injury_*.parquet`` on
    the next ``python -m Scripts.sync --push`` with no new plumbing.

    Args:
        episodes: :func:`build_episodes` output, with recurrence.
        post: :func:`post_return` output.
        controls: :func:`control_cohort` output.
        meta: :func:`build`'s metadata.

    Returns:
        dict: Artifact name -> path written.
    """
    paths.INJURY_EPISODES_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    episodes.write_parquet(paths.INJURY_EPISODES_PARQUET)
    post.write_parquet(paths.INJURY_POST_RETURN_PARQUET)
    controls.write_parquet(paths.INJURY_CONTROLS_PARQUET)
    with open(paths.INJURY_META_JSON, "w") as handle:
        json.dump(meta, handle, indent=2, default=str)
    return {"episodes": paths.INJURY_EPISODES_PARQUET,
            "post_return": paths.INJURY_POST_RETURN_PARQUET,
            "controls": paths.INJURY_CONTROLS_PARQUET,
            "meta": paths.INJURY_META_JSON}


def load_episodes() -> pl.DataFrame:
    """Read the episode table.

    Returns:
        pl.DataFrame: One row per episode.

    Raises:
        FileNotFoundError: When it has not been built.
    """
    return _read(paths.INJURY_EPISODES_PARQUET, "episodes")


def load_post_return() -> pl.DataFrame:
    """Read the post-return appearance table."""
    return _read(paths.INJURY_POST_RETURN_PARQUET, "post-return")


def load_controls() -> pl.DataFrame:
    """Read the matched healthy control cohort."""
    return _read(paths.INJURY_CONTROLS_PARQUET, "control")


def load_meta() -> Optional[Dict]:
    """Read the build metadata, or None when it has not been built."""
    if not paths.INJURY_META_JSON.is_file():
        return None
    with open(paths.INJURY_META_JSON) as handle:
        return json.load(handle)


def _read(path, label: str) -> pl.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(
            f"No injury {label} table at {path}. Build it with "
            f"`python -m Scripts.injury.episodes --rebuild`.")
    return pl.read_parquet(path)


# --- the descriptive report -----------------------------------------------

def report(episodes: pl.DataFrame, post: pl.DataFrame,
           controls: pl.DataFrame) -> str:
    """The table this phase exists to produce, with no fitted model involved.

    Average weeks missed and same-season recurrence rate by body part, plus the
    placebo-corrected post-return curve. Useful in a draft room and on a Tuesday whether
    or not a multiplier ever ships.

    Args:
        episodes: Episode table with recurrence.
        post: Post-return appearances.
        controls: Matched healthy control.

    Returns:
        str: A printable report.
    """
    lines: List[str] = []
    returned = episodes.filter(pl.col("outcome") == "returned")
    lines.append(f"  {episodes.height} episodes, {returned.height} ending in a return")
    outcomes = episodes.group_by("outcome").len().sort("len", descending=True)
    lines.append("  " + ", ".join(f"{o}={n}" for o, n in outcomes.rows()))
    lines.append(f"  {returned.filter(pl.col('weeks_out') >= 4).height} returned "
                 f"absences of 4+ games -- the injury report alone finds 99")

    control_mean = {r["appearance_back"]: r["m"] for r in
                    controls.group_by("appearance_back")
                    .agg(pl.col("pts_ratio").mean().alias("m"))
                    .iter_rows(named=True)}

    lines.append("")
    lines.append("  Healthy control, mean points ratio to own baseline "
                 f"(n={controls.height}):")
    lines.append("    " + "  ".join(
        f"a{k}={control_mean[k]:.3f}" for k in sorted(control_mean)))
    lines.append("    Flat, and below 1.0 -- that is skew, not injury. Every net "
                 "figure below divides by it.")

    lines.append("")
    lines.append("  By body part (returned episodes):")
    header = (f"    {'body part':<19}{'n':>5}{'wks out':>9}{'recur':>8}   "
              "net multiplier by appearance back")
    lines.append(header)
    summary = (returned.group_by("body_part")
               .agg(pl.len().alias("n"),
                    pl.col("weeks_out").mean().alias("wks"),
                    pl.col("recurred").mean().alias("recur"),
                    pl.col("body_part_unknown").mean().alias("unknown"))
               .sort("n", descending=True))
    for row in summary.iter_rows(named=True):
        cells = post.filter(pl.col("body_part") == row["body_part"])
        nets = []
        if not cells.is_empty():
            grouped = (cells.group_by("appearance_back")
                       .agg(pl.col("pts_ratio").mean().alias("m"),
                            pl.len().alias("k"))
                       .sort("appearance_back"))
            for cell in grouped.iter_rows(named=True):
                base = control_mean.get(cell["appearance_back"])
                if base:
                    nets.append(f"{cell['m'] / base:.2f}")
        recur = "  n/a" if row["unknown"] > 0.5 else f"{row['recur']:7.1%}"
        lines.append(f"    {row['body_part']:<19}{row['n']:>5}{row['wks']:>9.2f}"
                     f"{recur}   " + " ".join(nets))

    lines.append("")
    lines.append("  Same-body-part recurrence within "
                 f"{RECURRENCE_WINDOW} weeks of returning. Hamstring is the external "
                 "check:")
    ham = summary.filter(pl.col("body_part") == "hamstring")
    if not ham.is_empty():
        rate = ham["recur"][0]
        verdict = "matches" if 0.09 <= rate <= 0.15 else "OUTSIDE"
        lines.append(f"    hamstring {rate:.1%} against a published 11.9% -- {verdict}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rebuild", action="store_true",
                        help="Rebuild the episode tables from the season pulls.")
    parser.add_argument("--report", action="store_true",
                        help="Print the descriptive table from what is on disk.")
    parser.add_argument("--seasons", default=None,
                        help="Season range, e.g. 2016-2025.")
    args = parser.parse_args(argv)

    if args.seasons:
        first, _, last = args.seasons.partition("-")
        seasons = range(int(first), int(last or first) + 1)
    else:
        seasons = DEFAULT_SEASONS

    if args.rebuild:
        print(f"\n===== Injury episodes: {min(seasons)}-{max(seasons)} =====")
        episodes, post, controls, meta = build(seasons)
        written = write(episodes, post, controls, meta)
        print(report(episodes, post, controls))
        if meta["unmapped_body_parts"]:
            print(f"\n  unmapped body parts: {meta['unmapped_body_parts']}")
        if meta["concentrated_cells"]:
            print(f"\n  WARNING single-season concentration: "
                  f"{meta['concentrated_cells']}")
        print()
        for name, path in written.items():
            print(f"  wrote {name}: {path}")
        return 0

    if args.report:
        print(report(load_episodes(), load_post_return(), load_controls()))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
