"""Read the availability and role pulls that ``R/GetContext.R`` writes.

Sibling of :mod:`Scripts.usage.nflverse`, split the same way the R scripts are:
one reader per writer, because the two have different upstream availability and
conflating them hides that. ``GetUsage.R``'s output does not exist for a season
until games have been played; ``GetContext.R``'s ``rosters_weekly`` exists the
moment rosters are set, which is what makes a pre-season model possible at all.

**Why availability is the first feature family.** Plan 16's step 0 fitted the
crudest usage model and failed its accuracy gate -- but on rows where the player
actually took snaps the effect was −0.16% to +0.35%. Essentially the whole deficit
was not knowing who plays. Out is 100% deterministic, Doubtful 99.2%, and a
Questionable player's practice column splits 57% missed against 22%. Nothing else
in this pipeline models any of it.

**What a pre-season model can and cannot see.** Verified live on 2026-08-07:
``load_rosters_weekly(2026)`` serves a week-1 snapshot updated that morning, while
injuries, snap counts and depth charts all raise ``seasons <=
most_recent_season()``. So before week 1 there is no injury report to read, and
availability has to come from trailing games played plus roster status -- which is
what :func:`season_availability` builds. The weekly head reads the live report;
the season head cannot.

Polars throughout, per ``CLAUDE.md``.
"""

import json
from typing import Dict, List, Optional, Sequence

import polars as pl

from Scripts.paths import PLAYER_IDS_PARQUET, nfl_season_dir

#: Report designations, worst first. Ordered because the useful summary is
#: "the worst thing said about him this week", and a player can carry a report
#: status and a practice status that disagree.
REPORT_SEVERITY: Dict[str, int] = {"Out": 3, "Doubtful": 2, "Questionable": 1}

#: Practice participation, worst first. The column that splits Questionable in
#: half: 57.2% missed on "Did Not Participate" against 22.3% on "Full".
PRACTICE_SEVERITY: Dict[str, int] = {
    "Did Not Participate In Practice": 3,
    "Limited Participation in Practice": 2,
    "Full Participation in Practice": 1,
}

#: Roster statuses that mean the player is on the active roster. Everything else
#: -- CUT, RES (reserve/injured), RET (retired), E14 -- means he is not available,
#: and is the distinction ``player_weeks`` alone cannot make. Plan 16 flagged this
#: as the caveat inflating its 23% baseline miss rate: the weekly injury report
#: drops a player once he lands on IR, so a grid running to season end counts him
#: as healthy-and-absent.
ACTIVE_STATUSES = ("ACT",)

#: Files ``GetContext.R`` writes, and whether a season is expected to have one
#: before week 1.
ARTIFACTS: Dict[str, bool] = {
    "rosters_weekly": True,    # served pre-season
    "injuries": False,         # in-season only
    "snap_counts": False,      # in-season only
    "depth_charts": False,     # in-season only
}


def artifact_path(season: int, what: str):
    """Path to one context artifact. Not created.

    Args:
        season: Season year.
        what: An :data:`ARTIFACTS` key.

    Returns:
        Path: The parquet path, whether or not it exists.

    Raises:
        KeyError: On an unknown artifact name.
    """
    if what not in ARTIFACTS:
        raise KeyError(f"Unknown context artifact {what!r}. "
                       f"Known: {sorted(ARTIFACTS)}.")
    return nfl_season_dir(season, f"{what}.parquet")


def has_artifact(season: int, what: str) -> bool:
    """Whether one context artifact has been pulled for a season."""
    return artifact_path(season, what).is_file()


def load_meta(season: int) -> Optional[Dict]:
    """A season's ``context_meta.json``, including upstream release timestamps.

    Args:
        season: Season year.

    Returns:
        dict | None: The metadata, or None when the season has not been pulled.
    """
    path = nfl_season_dir(season, "context_meta.json")
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def _require(season: int, what: str):
    """Resolve a context artifact, naming the command that builds it.

    Args:
        season: Season year.
        what: An :data:`ARTIFACTS` key.

    Returns:
        Path: The existing parquet.

    Raises:
        FileNotFoundError: When it has not been pulled. The message distinguishes
            "not pulled" from "does not exist upstream yet", because before week 1
            three of the four genuinely cannot be pulled and telling someone to
            re-run the script would be wrong.
    """
    path = artifact_path(season, what)
    if path.is_file():
        return path

    if ARTIFACTS[what]:
        raise FileNotFoundError(
            f"No {what} for {season} ({path} is missing). Pull it with "
            f"`Rscript R/GetContext.R {season} {season}`."
        )
    raise FileNotFoundError(
        f"No {what} for {season} ({path} is missing). Upstream publishes it only "
        f"once the season is under way -- nflreadr refuses it while "
        f"most_recent_season() is behind {season} -- so pre-season this is "
        f"expected. The season model's availability head falls back to trailing "
        f"games played; see Scripts.usage.context.season_availability."
    )


def load_rosters(seasons: Sequence[int],
                 positions: Optional[Sequence[str]] = None) -> pl.DataFrame:
    """Weekly rosters: who was on which team, with what status.

    The join hub for everything else here -- the only frame carrying ``gsis_id``,
    ``espn_id`` and ``pfr_id`` together -- and the only current-season context a
    pre-season model has.

    Args:
        seasons: Season years to read.
        positions: Positions to keep. None keeps all.

    Returns:
        pl.DataFrame: One row per ``(season, week, gsis_id)``.

    Raises:
        FileNotFoundError: When a season has not been pulled.
    """
    columns = ["season", "week", "team", "position", "depth_chart_position",
               "status", "full_name", "gsis_id", "espn_id", "pfr_id",
               "years_exp", "entry_year", "rookie_year", "draft_number",
               "birth_date", "ngs_position"]
    frames = []
    for season in sorted(seasons):
        frame = pl.read_parquet(_require(season, "rosters_weekly"))
        keep = [c for c in columns if c in frame.columns]
        frame = frame.select(keep).with_columns(
            pl.col("season").cast(pl.Int32), pl.col("week").cast(pl.Int32))
        if positions is not None:
            frame = frame.filter(pl.col("position").is_in(list(positions)))
        frames.append(frame)
    return pl.concat(frames, how="diagonal").sort(["gsis_id", "season", "week"])


def load_injuries(seasons: Sequence[int]) -> pl.DataFrame:
    """The weekly injury report, with severity ranks attached.

    Args:
        seasons: Season years to read. Each must be a completed or in-progress
            season -- see :func:`_require`.

    Returns:
        pl.DataFrame: One row per ``(season, week, gsis_id)`` with
        ``report_status``, ``practice_status`` and integer ``report_rank`` /
        ``practice_rank``, higher meaning worse. A player on the report with no
        designation ranks 0.

    Raises:
        FileNotFoundError: When a season has not been pulled or does not exist
            upstream yet.
    """
    frames = []
    for season in sorted(seasons):
        frames.append(pl.read_parquet(_require(season, "injuries")))
    out = pl.concat(frames, how="diagonal")

    return out.select(
        pl.col("season").cast(pl.Int32),
        pl.col("week").cast(pl.Int32),
        "gsis_id", "position", "full_name",
        "report_status", "practice_status",
        "report_primary_injury", "practice_primary_injury",
    ).with_columns(
        pl.col("report_status").replace_strict(
            REPORT_SEVERITY, default=0, return_dtype=pl.Int32).alias("report_rank"),
        pl.col("practice_status").replace_strict(
            PRACTICE_SEVERITY, default=0,
            return_dtype=pl.Int32).alias("practice_rank"),
    ).sort(["gsis_id", "season", "week"])


def load_snap_counts(seasons: Sequence[int],
                     crosswalk: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Offensive snap share per player-week, resolved to ``gsis_id``.

    Upstream keys this on ``pfr_player_id`` and carries no ``gsis_id``, so it has
    to be joined. **Through the crosswalk, not through the rosters' own
    ``pfr_id``**: measured over all ten seasons on the QB/RB/WR/TE snap
    population, ``player_ids.parquet`` resolves 98.7-99.5% every year while rosters
    manage 71.2% in 2016, because rosters' ``pfr_id`` is only 45.9% populated that
    season against 75.0% in 2025. Joining through rosters would leave snap share
    quietly unusable in exactly the early training seasons.

    Args:
        seasons: Season years to read.
        crosswalk: Override the ``pfr_id``/``gsis_id`` table, for tests.

    Returns:
        pl.DataFrame: One row per ``(season, week, gsis_id)`` with
        ``offense_snaps``, ``offense_pct`` and the originating ``pfr_player_id``.
        Rows that do not resolve are dropped -- a snap count with no id is not
        joinable to anything, and keeping it would only inflate row counts.

    Raises:
        FileNotFoundError: When a season, or the crosswalk, is missing.
    """
    if crosswalk is None:
        if not PLAYER_IDS_PARQUET.is_file():
            raise FileNotFoundError(
                f"No player-id crosswalk at {PLAYER_IDS_PARQUET}, and snap counts "
                f"carry no gsis_id of their own. Generate it with "
                f"`Rscript R/GetPlayerIDs.R`."
            )
        crosswalk = pl.read_parquet(PLAYER_IDS_PARQUET,
                                    columns=["pfr_id", "gsis_id"])

    # A pfr_id mapping to two gsis_ids would fan out rows on the join. The
    # crosswalk already refuses ambiguous ids upstream; this is the guard that the
    # frame handed in here honours that.
    pairs = (crosswalk.select("pfr_id", "gsis_id").drop_nulls()
             .unique().filter(pl.col("pfr_id").is_not_null()))
    ambiguous = (pairs.group_by("pfr_id").len().filter(pl.col("len") > 1)
                 ["pfr_id"].to_list())
    if ambiguous:
        pairs = pairs.filter(~pl.col("pfr_id").is_in(ambiguous))

    frames = []
    for season in sorted(seasons):
        frame = pl.read_parquet(_require(season, "snap_counts"))
        frames.append(frame.select(
            pl.col("season").cast(pl.Int32),
            pl.col("week").cast(pl.Int32),
            "pfr_player_id", "player", "position", "team",
            pl.col("offense_snaps").cast(pl.Float64),
            pl.col("offense_pct").cast(pl.Float64),
        ))

    out = pl.concat(frames, how="vertical")
    return (
        out.join(pairs, left_on="pfr_player_id", right_on="pfr_id", how="inner")
        .unique(subset=["season", "week", "gsis_id"], keep="first")
        .sort(["gsis_id", "season", "week"])
    )


def team_games(player_weeks: pl.DataFrame) -> pl.DataFrame:
    """How many games each team actually played, per season.

    The denominator for availability, and it has to be derived rather than
    assumed. Three candidates were checked against each other on 2016-2025 and
    only one is right:

    - **Rosters' distinct weeks** count *calendar* weeks, so they include the bye.
      Wrong by one, and it produced an availability of 17/16 = 1.06 for a player
      who played every game.
    - **``opportunity.posteam``'s distinct weeks** gives 17 for eight teams in 2019,
      a season in which every team played 16. Something in that frame attributes a
      row to a team in a week it did not play.
    - **``player_weeks``' distinct (team, week)** is exactly right: 16 for all 32
      teams in 2016-2020, 17 in 2021-2025, and 16 for precisely two teams in 2022 --
      Buffalo and Cincinnati, whose game was cancelled. It reproduces the real
      exception rather than smoothing over it.

    Args:
        player_weeks: Frame with ``season``, ``week`` and ``team`` -- one row per
            player appearance, i.e. the raw ``player_weeks.parquet``.

    Returns:
        pl.DataFrame: ``season``, ``team``, ``team_games``.

    Raises:
        KeyError: When ``player_weeks`` lacks a ``team`` column, which is the
            failure mode worth naming -- a frame selected down to
            ``(season, week, gsis_id)`` looks usable here and is not.
    """
    if "team" not in player_weeks.columns:
        raise KeyError(
            "team_games needs a `team` column to count each team's games; got "
            f"{sorted(player_weeks.columns)[:10]}. Read player_weeks.parquet "
            "including `team` rather than selecting it away."
        )
    return (
        player_weeks.select(pl.col("season").cast(pl.Int32),
                            pl.col("week").cast(pl.Int32), "team")
        .unique()
        .group_by(["season", "team"])
        .agg(pl.len().cast(pl.Int32).alias("team_games"))
        .sort(["season", "team"])
    )


def season_availability(seasons: Sequence[int],
                        player_weeks: pl.DataFrame) -> pl.DataFrame:
    """Games played against games available, per player-season.

    This is the availability signal a **pre-season** model can have. It needs no
    injury report, which is the point: before week 1 there is none.

    Appearances come from ``player_weeks`` -- a player has a row for a week only if
    he was on the field for a play -- and the denominator from the weeks his
    team(s) played. Roster status supplies the piece appearances cannot: a player
    with 6 appearances who spent the rest of the season on reserve is a different
    prediction from one who was active and benched, and telling those apart is the
    caveat plan 16 recorded against its own injury table.

    Args:
        seasons: Season years to cover.
        player_weeks: Frame with ``season``, ``week``, ``gsis_id`` and ``team`` --
            one row per appearance, i.e. the raw ``player_weeks.parquet``. The
            ``team`` column is needed for the denominator; see :func:`team_games`.

    Returns:
        pl.DataFrame: One row per ``(season, gsis_id)`` with ``games_played``,
        ``games_available``, ``games_missed``, ``availability`` (played /
        available, capped at 1.0), ``weeks_active`` and ``weeks_on_reserve``.

    Raises:
        FileNotFoundError: When a season's rosters have not been pulled.
        KeyError: When ``player_weeks`` lacks ``team``.
    """
    seasons = sorted(seasons)
    scoped = player_weeks.filter(pl.col("season").is_in(seasons))

    appearances = (
        scoped.select(pl.col("season").cast(pl.Int32),
                      pl.col("week").cast(pl.Int32), "gsis_id")
        .unique()
        .group_by(["season", "gsis_id"])
        # Int32, not the UInt32 `len` returns. An unsigned count wraps to
        # 4,294,967,295 on any subtraction that should have gone negative, which is
        # what the first version of games_missed did for a player whose appearances
        # exceeded his denominator.
        .agg(pl.len().cast(pl.Int32).alias("games_played"))
    )

    games = team_games(scoped)
    # The season's usual slate, for a player whose roster team never appears in the
    # played-games table -- a practice-squad team code, or a team that played no
    # games in a truncated season.
    per_season = (games.group_by("season")
                  .agg(pl.col("team_games").max().alias("season_games")))

    rosters = load_rosters(seasons).select(
        "season", "week", "gsis_id", "team", "status")

    per_player = (
        rosters.join(games, on=["season", "team"], how="left")
        .group_by(["season", "gsis_id"])
        .agg(
            pl.col("week").n_unique().cast(pl.Int32).alias("weeks_on_a_roster"),
            (pl.col("status").is_in(ACTIVE_STATUSES)).sum()
            .cast(pl.Int32).alias("weeks_active"),
            (~pl.col("status").is_in(ACTIVE_STATUSES)).sum()
            .cast(pl.Int32).alias("weeks_on_reserve"),
            # A traded player's denominator is the larger of his teams' slates,
            # not their sum: he still had one season's worth of games to play in.
            pl.col("team_games").max().alias("games_available"),
        )
        .join(per_season, on="season", how="left")
        .with_columns(pl.col("games_available").fill_null(pl.col("season_games")))
        .drop("season_games")
    )

    return (
        per_player.join(appearances, on=["season", "gsis_id"], how="left")
        .with_columns(pl.col("games_played").fill_null(0))
        .with_columns(
            (pl.col("games_available") - pl.col("games_played"))
            .clip(lower_bound=0).alias("games_missed"),
            pl.when(pl.col("games_available") > 0)
            .then((pl.col("games_played") / pl.col("games_available")).clip(
                upper_bound=1.0))
            .otherwise(None)
            .alias("availability"),
        )
        .sort(["gsis_id", "season"])
    )
