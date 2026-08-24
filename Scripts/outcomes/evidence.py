"""The measurements behind ``docs/plans/28-outcome-distributions.md``.

Four questions, each answered against this repo's own 2016-2025 data rather than against
a prior. Run as a module to reprint every table in that plan::

    python -m Scripts.outcomes.evidence

**Two design notes about the measurements themselves**, because both changed a result.

*The lead player must be identified without leakage.* Defining "the lead back" by *this*
season's opportunity total conditions on the outcome: a back who got hurt in week 2 and
lost the job is not in the sample, so prior fragility appears to predict nothing (1.52
against 1.69 games missed). :func:`fragility` re-identifies the incumbent from season
S-1 plus a week-1 roster, which more than doubles the measured absence rate (3.29 games)
and is the only version of the question a drafter can actually ask.

*``games_missed`` is two quantities.* It counts injury and benching alike. Population
persistence is strong (r = 0.31) and incumbent-only persistence is nearly flat, and the
gap between them is the benching component -- which is plan 18's finding at
``Scripts/usage/features.py``:721 arriving from the other direction.
"""

from __future__ import annotations

from typing import Sequence, Tuple

import polars as pl

from Scripts import paths
from Scripts.usage import context as ctx

#: Seasons the episode table and the box scores both cover.
SEASONS: Tuple[int, ...] = tuple(range(2016, 2026))

#: Positions with a fantasy-relevant depth chart.
SKILL: Tuple[str, ...] = ("QB", "RB", "WR", "TE")

#: What counts as an opportunity, per position group. A backfield's currency is carries
#: *and* targets; a receiver room's is targets alone.
OPPORTUNITY = {"RB": ("carries", "targets"), "WR": ("targets",), "TE": ("targets",)}

#: Games a player-season needs before its per-game rate is used as a baseline, and the
#: points-per-game floor under it. Without the floor, a 0.4-ppg deep reserve produces
#: ratios in the hundreds -- the same failure ``Scripts.injury.episodes`` hit with
#: ``MIN_BASELINE_POINTS``.
MIN_BASELINE_GAMES: int = 6
MIN_BASELINE_PPG: float = 3.0


def load_weeks(seasons: Sequence[int] = SEASONS) -> pl.DataFrame:
    """Weekly box scores across seasons, with the columns these measurements need.

    Args:
        seasons: Season years to read.

    Returns:
        pl.DataFrame: One row per player appearance, with ``season``, ``week``,
        ``gsis_id``, ``team``, ``position``, ``carries``, ``targets``, ``attempts`` and
        ``fantasy_points_ppr``.

    Raises:
        FileNotFoundError: When a season's usage pull is missing.
    """
    return pl.concat(
        [pl.read_parquet(paths.DATA_DIR / "NFL" / str(s) / "player_weeks.parquet")
           .select("season", "week", "gsis_id", "player_display_name", "team", "position",
                   "carries", "targets", "attempts", "fantasy_points_ppr")
           .with_columns(pl.col("season").cast(pl.Int32), pl.col("week").cast(pl.Int32))
         for s in seasons],
        how="vertical_relaxed")


def _ranked(weeks: pl.DataFrame, position: str) -> pl.DataFrame:
    """Weekly rows for one position, tagged with each player's rank in his own room.

    **The tie-break is explicit, and it has to be.** ``rank("ordinal")`` numbers ties in
    row order, and row order out of a multi-threaded ``group_by`` is not stable -- so the
    first version of this returned different figures on consecutive runs (a TE room's
    volume moved 7.145 to 7.136 and a cohort went from 104 to 103) because deep reserves
    tie at zero or one target and swap places. Sorting on ``gsis_id`` after the volume
    makes the ordering total, which is the difference between a measurement and a draw.
    """
    cols = OPPORTUNITY[position]
    p = (weeks.filter(pl.col("position") == position)
              .with_columns(sum(pl.col(c).fill_null(0) for c in cols).alias("opp")))
    ranks = (p.group_by(["season", "team", "gsis_id"])
              .agg(pl.col("opp").sum().alias("season_opp"))
              .sort(["season", "team", "season_opp", "gsis_id"],
                    descending=[False, False, True, False])
              .with_columns(pl.int_range(1, pl.len() + 1)
                              .over(["season", "team"]).alias("rk")))
    return p.join(ranks.select("season", "team", "gsis_id", "rk"),
                  on=["season", "team", "gsis_id"])


def _lead_state(weeks: pl.DataFrame, ranked: pl.DataFrame) -> pl.DataFrame:
    """One row per team-week, flagging whether the room's lead player appeared.

    The frame is every week a *team* played -- from ``weeks`` rather than from the
    position's own rows -- because a week in which no back appeared is still a week, and
    building the grid from appearances would silently drop it.
    """
    played = (ranked.filter(pl.col("rk") == 1)
                    .select("season", "team", "week").unique()
                    .with_columns(pl.lit(True).alias("lead_played")))
    return (weeks.select("season", "team", "week").unique()
                 .join(played, on=["season", "team", "week"], how="left")
                 .with_columns(pl.col("lead_played").fill_null(False)))


def closure(weeks: pl.DataFrame, position: str,
            min_in: int = 3, min_out: int = 2) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Does vacated opportunity stay inside the position group?

    The question a redistribution rule has to answer before it may exist. Restricted to
    team-seasons that experienced both states, so the comparison is within-team rather
    than between a team whose starter was durable and a team whose starter was not.

    Args:
        weeks: :func:`load_weeks` output.
        position: ``"RB"``, ``"WR"`` or ``"TE"``.
        min_in: Games the lead must have played for the team-season to qualify.
        min_out: Games the lead must have missed.

    Returns:
        tuple: ``(group, by_rank)``. ``group`` carries the room's total opportunity and
        points per game in each state; ``by_rank`` the per-rank opportunity in each.
    """
    ranked = _ranked(weeks, position)
    state = _lead_state(weeks, ranked)
    room = (ranked.group_by(["season", "team", "week"])
                  .agg(pl.col("opp").sum().alias("group_opp"),
                       pl.col("fantasy_points_ppr").fill_null(0).sum().alias("group_pts")))
    both = (state.group_by(["season", "team"])
                 .agg(pl.col("lead_played").sum().alias("n_in"),
                      (~pl.col("lead_played")).sum().alias("n_out"))
                 .filter((pl.col("n_in") >= min_in) & (pl.col("n_out") >= min_out))
                 .select("season", "team"))
    group = (state.join(room, on=["season", "team", "week"], how="left")
                  .with_columns(pl.lit(both.height).alias("team_seasons"))
                  .drop_nulls("group_opp")
                  .join(both, on=["season", "team"])
                  .group_by("lead_played")
                  .agg(pl.col("group_opp").mean(), pl.col("group_pts").mean(),
                       pl.col("team_seasons").first(), pl.len().alias("team_weeks"))
                  .sort("lead_played", descending=True))
    by_rank = (ranked.filter(pl.col("rk") <= 4)
                     .group_by(["season", "team", "week", "rk"])
                     .agg(pl.col("opp").sum())
                     .join(both, on=["season", "team"])
                     .join(state, on=["season", "team", "week"], how="left")
                     .group_by(["rk", "lead_played"])
                     .agg(pl.col("opp").mean())
                     .sort(["rk", "lead_played"], descending=[False, True]))
    return group, by_rank


def transfer(weeks: pl.DataFrame, position: str, min_each: int = 3) -> pl.DataFrame:
    """The same mechanism from the recipient's row, paired within player-season.

    Each backup is his own control: his mean usage in weeks the lead played against his
    mean in weeks the lead sat. Pairing is what removes the confound a between-player
    comparison cannot -- backups behind fragile starters are not a random sample of
    backups.

    Args:
        weeks: :func:`load_weeks` output.
        position: ``"RB"``, ``"WR"`` or ``"TE"``.
        min_each: Games required in *each* state, so a pair is real rather than one
            week against sixteen.

    Returns:
        pl.DataFrame: ``rk``, ``n``, and mean opportunity and points per game in each
        state, for ranks 2 and 3.
    """
    ranked = _ranked(weeks, position)
    state = _lead_state(weeks, ranked)
    others = (ranked.select("season", "team", "gsis_id", "rk").unique()
                    .filter(pl.col("rk").is_between(2, 3)))
    # Zero-fill the weeks he did not appear: a backup who was inactive got no
    # opportunity, and dropping the row would score him only on games he played.
    grid = (others.join(state, on=["season", "team"], how="left")
                  .join(ranked.select("season", "team", "week", "gsis_id", "opp",
                                      "fantasy_points_ppr"),
                        on=["season", "team", "week", "gsis_id"], how="left")
                  .with_columns(pl.col("opp").fill_null(0),
                                pl.col("fantasy_points_ppr").fill_null(0)))
    counts = (grid.group_by(["season", "team", "gsis_id"])
                  .agg(pl.col("lead_played").sum().alias("n_in"),
                       (~pl.col("lead_played")).sum().alias("n_out"))
                  .filter((pl.col("n_in") >= min_each) & (pl.col("n_out") >= min_each))
                  .select("season", "team", "gsis_id"))
    return (grid.join(counts, on=["season", "team", "gsis_id"])
                .group_by(["season", "team", "gsis_id", "rk"])
                .agg(pl.col("opp").filter(pl.col("lead_played")).mean().alias("opp_in"),
                     pl.col("opp").filter(~pl.col("lead_played")).mean().alias("opp_out"),
                     pl.col("fantasy_points_ppr").filter(pl.col("lead_played")).mean().alias("pts_in"),
                     pl.col("fantasy_points_ppr").filter(~pl.col("lead_played")).mean().alias("pts_out"))
                .group_by("rk")
                .agg(pl.len().alias("n"), pl.col("opp_in").mean(), pl.col("opp_out").mean(),
                     pl.col("pts_in").mean(), pl.col("pts_out").mean())
                .sort("rk"))


def spillover(weeks: pl.DataFrame, position: str = "WR") -> pl.DataFrame:
    """Where a missing lead player's work goes when it leaves his room.

    :func:`closure` shows a receiver room recapturing only 44% of a vacated target. This
    is the other 56%: it crosses position groups, and some of it stops existing because
    the offence throws less. A redistribution rule that kept it inside the room would be
    inventing about 2.8 targets a game.

    Args:
        weeks: :func:`load_weeks` output.
        position: The room whose lead is absent.

    Returns:
        pl.DataFrame: ``lead_played`` against per-game targets for WR, RB and TE, plus
        the team's pass attempts.
    """
    ranked = _ranked(weeks, position)
    state = _lead_state(weeks, ranked)
    targets = (weeks.filter(pl.col("position").is_in(["WR", "RB", "TE"]))
                    .group_by(["season", "team", "week", "position"])
                    .agg(pl.col("targets").fill_null(0).sum().alias("targets")))
    attempts = (weeks.group_by(["season", "team", "week"])
                     .agg(pl.col("attempts").fill_null(0).sum().alias("pass_attempts")))
    wide = (targets.join(state, on=["season", "team", "week"])
                   .pivot(on="position", index=["season", "team", "week", "lead_played"],
                          values="targets")
                   .join(attempts, on=["season", "team", "week"]))
    return (wide.group_by("lead_played")
                .agg(pl.col("WR").mean(), pl.col("RB").mean(), pl.col("TE").mean(),
                     pl.col("pass_attempts").mean(), pl.len().alias("team_weeks"))
                .sort("lead_played", descending=True))


def player_seasons(weeks: pl.DataFrame,
                   seasons: Sequence[int] = SEASONS) -> pl.DataFrame:
    """Availability and per-game production per player-season, for skill positions.

    Args:
        weeks: :func:`load_weeks` output.
        seasons: Season years to cover.

    Returns:
        pl.DataFrame: :func:`Scripts.usage.context.season_availability`'s columns plus
        ``position``, ``pts`` and ``ppg``.
    """
    avail = ctx.season_availability(seasons, weeks)
    agg = (weeks.group_by(["season", "gsis_id"])
                .agg(pl.col("position").drop_nulls().first().alias("position"),
                     pl.col("fantasy_points_ppr").sum().alias("pts")))
    return (avail.join(agg, on=["season", "gsis_id"], how="left")
                 .filter(pl.col("position").is_in(SKILL))
                 .with_columns((pl.col("pts") / pl.col("games_played")).alias("ppg")))


def next_season_by_duration(ps: pl.DataFrame) -> pl.DataFrame:
    """Season S+1 outcome by how long an episode in season S kept a player out.

    Duration stands in for severity because severity does not exist historically: of 992
    episodes of 8+ weeks, 73.1% carry no body part at all, since the injury report goes
    quiet once a player lands on reserve. See ``docs/plans/27-injury-model.md``.

    The control -- a season S with 14+ games and no episode of 2+ weeks -- is selected on
    durability, so its *level* is an upper bound rather than a neutral placebo. Both
    cohorts index to S-1, so the ratio is matched; the plan's phase 4 owes a control
    passing the identical filter.

    Args:
        ps: :func:`player_seasons` output.

    Returns:
        pl.DataFrame: ``cohort``, ``n``, ``games_next``, ``ppg_ratio_median``,
        ``p_within_90pct`` and ``p_full_slate``.
    """
    ep = pl.read_parquet(paths.INJURY_EPISODES_PARQUET)
    prev = ps.select(pl.col("season").alias("s"), "gsis_id",
                     pl.col("ppg").alias("ppg_prev"),
                     pl.col("games_played").alias("gp_prev"))
    nxt = ps.select(pl.col("season").alias("s"), "gsis_id",
                    pl.col("ppg").alias("ppg_next"),
                    pl.col("games_played").alias("gp_next"))

    def summarise(anchors: pl.DataFrame, label: str) -> pl.DataFrame:
        out = (anchors
               .join(prev, left_on=[pl.col("season") - 1, "gsis_id"], right_on=["s", "gsis_id"])
               .join(nxt, left_on=[pl.col("season") + 1, "gsis_id"], right_on=["s", "gsis_id"])
               .filter((pl.col("gp_prev") >= MIN_BASELINE_GAMES)
                       & (pl.col("ppg_prev") > MIN_BASELINE_PPG))
               .with_columns((pl.col("ppg_next") / pl.col("ppg_prev")).alias("ratio")))
        return pl.DataFrame({
            "cohort": [label], "n": [out.height],
            "games_next": [out["gp_next"].mean()],
            "ppg_ratio_median": [out["ratio"].median()],
            "p_within_90pct": [(out["ratio"] >= 0.90).mean()],
            "p_full_slate": [(out["gp_next"] >= 14).mean()]})

    hurt = ep.filter(pl.col("weeks_out") >= 2).select("season", "gsis_id").unique()
    rows = [summarise(ps.filter(pl.col("games_played") >= 14)
                        .join(hurt, on=["season", "gsis_id"], how="anti")
                        .select("season", "gsis_id"), "healthy control")]
    for lo, hi, label in [(1, 3, "1-3 weeks out"), (4, 7, "4-7 weeks out"),
                          (8, 99, "8+ weeks out")]:
        rows.append(summarise(
            ep.filter(pl.col("weeks_out").is_between(lo, hi))
              .select("season", "gsis_id").unique(), label))
    return pl.concat(rows)


def incumbents(weeks: pl.DataFrame, position: str = "RB") -> pl.DataFrame:
    """Players who led a room in season S-1 and are on that team's week-1 roster in S.

    The **pre-season** identity, which is the only one a drafter has and the only one this
    question may use. Identifying the lead from season S's own touches conditions on the
    outcome -- a starter who tore something in week 2 and lost the job never appears -- and
    that leakage is what made prior fragility look uninformative (1.52 games missed against
    3.29 once corrected).

    Availability is joined from :func:`Scripts.usage.context.season_availability`
    **unfiltered**. Routing it through :func:`player_seasons`, which filters to
    :data:`SKILL`, dropped the 8 of 217 incumbents who appeared in no game at all -- a
    player with no appearance has no position recorded -- and those eight are the whole
    severe tail. The cohort size was 217 either way, so nothing looked wrong.

    Args:
        weeks: :func:`load_weeks` output.
        position: Room to take the lead of.

    Returns:
        pl.DataFrame: One row per incumbent-season, with his season-S availability and
        ``missed_prev`` / ``gp_prev`` from S-1.
    """
    avail = ctx.season_availability(SEASONS, weeks)
    season_ranks = _ranked(weeks, position).select(
        "season", "team", "gsis_id", "rk").unique()
    led = (season_ranks.filter(pl.col("rk") == 1)
                       .select((pl.col("season") + 1).alias("season"), "team", "gsis_id"))
    week_one = (ctx.load_rosters(SEASONS).filter(pl.col("week") == 1)
                   .select("season", "gsis_id", "team").unique())
    prior = avail.select(pl.col("season").alias("s"), "gsis_id",
                         pl.col("games_missed").alias("missed_prev"),
                         pl.col("games_played").alias("gp_prev"))
    return (led.join(week_one, on=["season", "team", "gsis_id"])
               .join(avail, on=["season", "gsis_id"], how="left")
               .join(prior, left_on=[pl.col("season") - 1, "gsis_id"],
                     right_on=["s", "gsis_id"])
               .filter(pl.col("gp_prev") >= MIN_BASELINE_GAMES))


def incumbent_two_season(weeks: pl.DataFrame, position: str = "RB") -> pl.DataFrame:
    """The same incumbents, bucketed by *two* prior seasons rather than one.

    Reported because a pattern is the intuitive version of "constantly injured" and it
    deserves its own test rather than an extrapolation from the one-season table. It comes
    out non-monotone -- both-bad sits below one-bad -- which is the result that stops this
    from being a feature.

    Args:
        weeks: :func:`load_weeks` output.
        position: Room to take the lead of.

    Returns:
        pl.DataFrame: ``pattern``, ``n``, ``games_played``, ``games_missed``,
        ``p_miss_3plus``.
    """
    avail = ctx.season_availability(SEASONS, weeks)
    two_ago = avail.select((pl.col("season") + 2).alias("s2"), "gsis_id",
                           pl.col("games_missed").alias("missed_2ago"))
    held = (incumbents(weeks, position)
            .join(two_ago, left_on=["season", "gsis_id"], right_on=["s2", "gsis_id"]))
    patterns = [
        ("both clean (0-1, 0-1)", (pl.col("missed_2ago") <= 1) & (pl.col("missed_prev") <= 1)),
        ("one bad (>=3)", ((pl.col("missed_2ago") >= 3).cast(int)
                           + (pl.col("missed_prev") >= 3).cast(int)) == 1),
        ("both bad (>=3, >=3)", (pl.col("missed_2ago") >= 3) & (pl.col("missed_prev") >= 3)),
    ]
    return pl.concat([
        pl.DataFrame({"pattern": [label], "n": [g.height],
                      "games_played": [g["games_played"].mean()],
                      "games_missed": [g["games_missed"].mean()],
                      "p_miss_3plus": [(g["games_missed"] >= 3).mean()]})
        for label, expr in patterns if (g := held.filter(expr)).height])


def fragility(weeks: pl.DataFrame,
              position: str = "RB") -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Does an incumbent starter's absence history predict his next season, or his backup's?

    The incumbent is identified **pre-season** -- he led his room in season S-1 and is on
    a week-1 roster in S -- because identifying him from season S's own touches selects
    out the players who got hurt, which is the leakage that reversed this result.

    **Availability comes from the unfiltered frame, not from :func:`player_seasons`.**
    That function filters to :data:`SKILL`, and a player who appeared in *no* game has no
    position recorded, so the filter drops him -- which silently removed the 8 of 217
    incumbents who missed an entire season, the exact subpopulation the question is about.
    It moved the "missed 1-2" bucket from 4.00 games missed to 3.22 and biased every
    bucket toward the answer that fragility does not matter.

    Args:
        weeks: :func:`load_weeks` output.
        position: Room to measure. Only ``"RB"`` has enough of a next-man-up to matter.

    Returns:
        tuple: ``(incumbent, backup)``, each bucketed by the incumbent's prior-season
        games missed.
    """
    held = incumbents(weeks, position)
    buckets = [(0, 0, "missed 0"), (1, 2, "missed 1-2"),
               (3, 5, "missed 3-5"), (6, 99, "missed 6+")]
    incumbent = pl.concat([
        pl.DataFrame({"bucket": [label], "n": [s.height],
                      "games_played": [s["games_played"].mean()],
                      "games_missed": [s["games_missed"].mean()],
                      "p_miss_3plus": [(s["games_missed"] >= 3).mean()]})
        for lo, hi, label in buckets
        if (s := held.filter(pl.col("missed_prev").is_between(lo, hi))).height])

    season_ranks = _ranked(weeks, position).select(
        "season", "team", "gsis_id", "rk").unique()
    seconds = (season_ranks.filter(pl.col("rk") == 2)
                           .select("season", "team", pl.col("gsis_id").alias("b_id")))
    totals = (_ranked(weeks, position).group_by(["season", "team", "gsis_id"])
                    .agg(pl.col("opp").sum().alias("b_opp"),
                         pl.col("fantasy_points_ppr").sum().alias("b_pts")))
    pairs = (held.select("season", "team", "missed_prev")
                 .join(seconds, on=["season", "team"])
                 .join(totals, left_on=["season", "team", "b_id"],
                       right_on=["season", "team", "gsis_id"]))
    backup = pl.concat([
        pl.DataFrame({"bucket": [label], "n": [s.height],
                      "backup_opp": [s["b_opp"].mean()],
                      "backup_pts": [s["b_pts"].mean()],
                      "p_over_150": [(s["b_pts"] > 150).mean()]})
        for lo, hi, label in buckets
        if (s := pairs.filter(pl.col("missed_prev").is_between(lo, hi))).height])
    return incumbent, backup


def persistence(ps: pl.DataFrame) -> pl.DataFrame:
    """Season-over-season persistence of games missed, across all skill players.

    Reported *beside* :func:`fragility` rather than instead of it, because the two
    disagree and the disagreement is the finding: this measurement conditions only on
    having had a role, so it carries role loss as well as injury. Its persistence is
    strong; the incumbent-only version is nearly flat.

    Args:
        ps: :func:`player_seasons` output.

    Returns:
        pl.DataFrame: ``pattern``, ``n``, ``p_miss_3plus``, ``mean_missed``, with a
        ``correlation`` column repeating the pooled figure on every row.
    """
    nxt = ps.select((pl.col("season") - 1).alias("prev"), "gsis_id",
                    pl.col("games_missed").alias("missed_next"))
    pair = (ps.filter(pl.col("games_played") >= 8)
              .join(nxt, left_on=["season", "gsis_id"], right_on=["prev", "gsis_id"]))
    two_ago = ps.select((pl.col("season") + 2).alias("s2"), "gsis_id",
                        pl.col("games_missed").alias("missed_2ago"))
    two = pair.join(two_ago, left_on=["season", "gsis_id"], right_on=["s2", "gsis_id"])
    r = pair.select(pl.corr("games_missed", "missed_next")).item()
    patterns = [
        ("both clean (0, 0)", (pl.col("missed_2ago") == 0) & (pl.col("games_missed") == 0)),
        ("one bad (>=3)", ((pl.col("missed_2ago") >= 3).cast(int)
                           + (pl.col("games_missed") >= 3).cast(int)) == 1),
        ("both bad (>=3, >=3)", (pl.col("missed_2ago") >= 3) & (pl.col("games_missed") >= 3)),
    ]
    return pl.concat([
        pl.DataFrame({"pattern": [label], "n": [s.height],
                      "p_miss_3plus": [(s["missed_next"] >= 3).mean()],
                      "mean_missed": [s["missed_next"].mean()],
                      "correlation": [r]})
        for label, expr in patterns if (s := two.filter(expr)).height])


def report() -> str:
    """Every table in ``docs/plans/28-outcome-distributions.md``, as text.

    Returns:
        str: The rendered report.
    """
    weeks = load_weeks()
    ps = player_seasons(weeks)
    out = ["=== vacated opportunity: does it stay in the room? ==="]
    for position in ("RB", "WR", "TE"):
        group, by_rank = closure(weeks, position)
        out.append(f"\n-- {position} room --\n{group}\n{by_rank}")
    out.append(f"\n=== a missing lead WR: where the work goes ===\n"
               f"{spillover(weeks, 'WR')}")
    out.append("\n=== the recipient's view, paired within player-season ===")
    for position in ("RB", "WR", "TE"):
        out.append(f"\n-- {position} --\n{transfer(weeks, position)}")
    out.append(f"\n=== next season, by how long he was out ===\n"
               f"{next_season_by_duration(ps)}")
    incumbent, backup = fragility(weeks)
    out.append(f"\n=== incumbent lead RB, two prior seasons ===\n"
               f"{incumbent_two_season(weeks)}")
    out.append(f"\n=== incumbent lead RB, identified pre-season ===\n{incumbent}"
               f"\n\n=== and the RB2 behind him ===\n{backup}")
    out.append(f"\n=== games missed, season over season, all skill players ===\n"
               f"{persistence(ps)}")
    return "\n".join(out)


if __name__ == "__main__":
    print(report())
