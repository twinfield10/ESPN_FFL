"""Season-level features for the pre-season usage head, with the as-of guarantee.

The season counterpart to :func:`Scripts.usage.baseline.as_of_features`, which works
in trailing weeks. Here the unit is a player-season and the question is what was
knowable **before a draft**: everything from completed seasons, plus the current
season's roster snapshot, and nothing else.

Two things make that boundary sharper than it looks.

**The roster snapshot is legitimately current.** ``load_rosters_weekly(2026)`` served
a week-1 snapshot on 2026-08-07, updated that morning, carrying team, status,
``years_exp``, ``entry_year`` and ``draft_number``. A drafter has all of that, so a
model may use it. It is the only current-season input that exists: injuries, snap
counts and depth charts are all refused by nflreadr until the season is under way.

**Positional baselines have to be fitted on the training seasons too.** Shrinking a
player's touchdown rate toward "the positional average" is leakage if that average
includes the season being predicted. It is a small leak and it flatters every
efficiency feature at once, which is the kind that survives review. So
:func:`positional_baselines` takes the seasons it may look at, and
:func:`season_features` passes it only the prior ones.

Volume and efficiency are kept apart throughout, because plan 16 measured them as
different signals: opportunity is sticky year over year at r 0.86-0.92, touchdown
rate at 0.234. Averaging them into one number lets the noisy half contaminate the
predictable half and leaves you unable to tell which half a miss came from.

Polars throughout, per ``CLAUDE.md``.
"""

from typing import Dict, List, Optional, Sequence

import polars as pl

from Scripts.usage import context as ctx
from Scripts.usage import scheme as sc
from Scripts.usage.nflverse import (
    ACTUAL_PREFIX,
    EXPECTED_PREFIX,
    USAGE_STATS,
    load_opportunity,
    player_weeks_path,
)

#: Per-game volume columns taken from ``player_weeks``, as ``<name>_pg``.
#:
#: These are the sticky half. Plan 16's year-over-year measurement:
#: carries/game +0.915, air-yards share +0.903, WOPR +0.885, target share +0.858.
VOLUME_STATS: Dict[str, str] = {
    "targets": "targets",
    "carries": "carries",
    "attempts": "pass_attempts",
    "receptions": "receptions",
    "receiving_air_yards": "air_yards",
}

#: Season-mean share columns, already normalised by upstream so they are not
#: divided by games.
SHARE_STATS: List[str] = ["target_share", "air_yards_share", "wopr"]

#: Efficiency rates, as ``(name, numerator, denominator)``. Ratios of season totals
#: rather than means of weekly ratios -- a one-target week would otherwise weigh as
#: much as a twelve-target one.
EFFICIENCY_RATES = (
    ("yards_per_target", "receiving_yards", "targets"),
    ("catch_rate", "receptions", "targets"),
    ("rec_td_per_target", "receiving_tds", "targets"),
    ("yards_per_carry", "rushing_yards", "carries"),
    ("rush_td_per_carry", "rushing_tds", "carries"),
    ("yards_per_attempt", "passing_yards", "attempts"),
    ("pass_td_per_attempt", "passing_tds", "attempts"),
    ("int_per_attempt", "passing_interceptions", "attempts"),
)

#: Shrinkage strength, in denominator units. A player with ``k`` targets is pulled
#: halfway to his position's baseline; with 4k he keeps 80% of his own rate.
#:
#: Set per rate rather than globally because the denominators are not comparable --
#: 50 targets is a season of real volume, 50 pass attempts is two games of a backup.
#: Touchdown rates get the heaviest shrinkage, which is what plan 16's +0.234
#: year-over-year stickiness argues for: a player's own TD rate is mostly noise.
SHRINKAGE_K: Dict[str, float] = {
    "yards_per_target": 40.0,
    "catch_rate": 40.0,
    "rec_td_per_target": 120.0,
    "yards_per_carry": 60.0,
    "rush_td_per_carry": 150.0,
    "yards_per_attempt": 150.0,
    "pass_td_per_attempt": 300.0,
    "int_per_attempt": 300.0,
}

#: Positions the season head speaks for. Kickers and team defences have no usage
#: features at all, and plan 18 fixes abstention rather than a positional default:
#: a model that quietly emits a positional average looks like full coverage and
#: drags the blend toward the mean for exactly the players a board must
#: differentiate.
MODELLED_POSITIONS = ("QB", "RB", "WR", "TE")

#: Prefix for a feature drawn from the season before the one being predicted.
LAG1_PREFIX = "p1_"

#: Prefix for two seasons before.
LAG2_PREFIX = "p2_"


def load_player_weeks(seasons: Sequence[int],
                      columns: Optional[Sequence[str]] = None) -> pl.DataFrame:
    """Observed weekly usage for several seasons.

    Args:
        seasons: Season years. Each must have been pulled by ``R/GetUsage.R``.
        columns: Columns to read. None reads everything this module needs.

    Returns:
        pl.DataFrame: One row per player-week appearance.

    Raises:
        FileNotFoundError: When a season has not been pulled.
    """
    if columns is None:
        needed = {"season", "week", "gsis_id", "position", "team"}
        needed |= set(VOLUME_STATS)
        needed |= set(SHARE_STATS)
        for _, numerator, denominator in EFFICIENCY_RATES:
            needed |= {numerator, denominator}
        columns = sorted(needed)

    frames = []
    for season in sorted(seasons):
        path = player_weeks_path(season)
        if not path.is_file():
            raise FileNotFoundError(
                f"No player_weeks for {season} ({path} is missing). Pull it with "
                f"`Rscript R/GetUsage.R {season} {season}`."
            )
        available = pl.read_parquet_schema(path)
        frame = pl.read_parquet(path, columns=[c for c in columns if c in available])
        frames.append(frame.with_columns(pl.col("season").cast(pl.Int32),
                                         pl.col("week").cast(pl.Int32)))
    return pl.concat(frames, how="diagonal")


def season_totals(player_weeks: pl.DataFrame) -> pl.DataFrame:
    """Season totals and per-game rates per player, from weekly appearances.

    Args:
        player_weeks: :func:`load_player_weeks` output.

    Returns:
        pl.DataFrame: One row per ``(season, gsis_id)`` with ``games``, ``<name>_pg``
        per :data:`VOLUME_STATS`, season means of :data:`SHARE_STATS`, and the raw
        totals the efficiency rates need.
    """
    totals = [pl.len().cast(pl.Int32).alias("games"),
              pl.col("position").drop_nulls().first().alias("position"),
              pl.col("team").drop_nulls().last().alias("team")]

    summed = set()
    for source in VOLUME_STATS:
        if source in player_weeks.columns:
            totals.append(pl.col(source).sum().alias(f"tot_{source}"))
            summed.add(source)
    for _, numerator, denominator in EFFICIENCY_RATES:
        for column in (numerator, denominator):
            if column in player_weeks.columns and column not in summed:
                totals.append(pl.col(column).sum().alias(f"tot_{column}"))
                summed.add(column)
    for share in SHARE_STATS:
        if share in player_weeks.columns:
            totals.append(pl.col(share).mean().alias(share))

    grouped = (player_weeks.group_by(["season", "gsis_id"])
               .agg(totals)
               .sort(["gsis_id", "season"]))

    per_game = [
        (pl.col(f"tot_{source}") / pl.col("games")).alias(f"{name}_pg")
        for source, name in VOLUME_STATS.items()
        if f"tot_{source}" in grouped.columns
    ]
    return grouped.with_columns(per_game)


def positional_baselines(totals: pl.DataFrame) -> pl.DataFrame:
    """Each position's pooled efficiency rate, for shrinking individuals toward.

    Pooled from season *totals* rather than averaged over players, so a position's
    baseline is what that position actually did per opportunity rather than the mean
    of its part-timers' noisy rates.

    **Fit only on seasons the model is allowed to see.** Including the predicted
    season here leaks, subtly and in the flattering direction, into every efficiency
    feature at once. :func:`season_features` passes prior seasons only.

    Args:
        totals: :func:`season_totals` output, restricted to permitted seasons.

    Returns:
        pl.DataFrame: ``position`` plus one column per :data:`EFFICIENCY_RATES` name.
    """
    exprs = []
    for name, numerator, denominator in EFFICIENCY_RATES:
        num, den = f"tot_{numerator}", f"tot_{denominator}"
        if num not in totals.columns or den not in totals.columns:
            continue
        exprs.append(
            pl.when(pl.col(den).sum() > 0)
            .then(pl.col(num).sum() / pl.col(den).sum())
            .otherwise(None)
            .alias(name)
        )
    if not exprs:
        return pl.DataFrame(schema={"position": pl.String})
    return totals.group_by("position").agg(exprs).sort("position")


def attach_efficiency(totals: pl.DataFrame, baselines: pl.DataFrame,
                      shrinkage: Optional[Dict[str, float]] = None) -> pl.DataFrame:
    """Efficiency rates, shrunk toward the positional baseline by volume.

    ``(n * own_rate + k * baseline) / (n + k)``, the standard credibility weighting.
    A player with no opportunity at all gets the baseline outright rather than a
    null, because the alternative -- dropping him -- would make the model's coverage
    depend on last season's usage, and a drafter still has to price him.

    Args:
        totals: :func:`season_totals` output.
        baselines: :func:`positional_baselines` output.
        shrinkage: Per-rate ``k``. Defaults to :data:`SHRINKAGE_K`.

    Returns:
        pl.DataFrame: ``totals`` plus one shrunk column per rate, and ``<name>_raw``
        holding the unshrunk value for auditability.
    """
    shrinkage = SHRINKAGE_K if shrinkage is None else shrinkage
    rates = [name for name, _, _ in EFFICIENCY_RATES if name in baselines.columns]
    if not rates:
        return totals

    out = totals.join(
        baselines.rename({name: f"base_{name}" for name in rates}),
        on="position", how="left",
    )

    exprs = []
    for name, numerator, denominator in EFFICIENCY_RATES:
        if name not in rates:
            continue
        num, den = f"tot_{numerator}", f"tot_{denominator}"
        if num not in out.columns or den not in out.columns:
            continue
        k = float(shrinkage.get(name, 50.0))
        own = pl.when(pl.col(den) > 0).then(pl.col(num) / pl.col(den)).otherwise(None)
        exprs.append(own.alias(f"{name}_raw"))
        exprs.append(
            ((pl.col(den).fill_null(0) * own.fill_null(0)
              + k * pl.col(f"base_{name}"))
             / (pl.col(den).fill_null(0) + k)).alias(name)
        )

    return out.with_columns(exprs).drop([f"base_{name}" for name in rates])


def expected_production(seasons: Sequence[int]) -> pl.DataFrame:
    """Per-game expected and actual production per player-season.

    ffopportunity's expected columns are the reason this whole layer exists: plan 16
    measured trailing *expected* production beating trailing actual at predicting
    next week (R² 0.2907 against 0.2702), and expected points per game as the more
    stable season metric (+0.816 against +0.792).

    Args:
        seasons: Season years to read.

    Returns:
        pl.DataFrame: One row per ``(season, gsis_id)`` with ``exp_<stat>_pg`` and
        ``act_<stat>_pg`` per :data:`Scripts.usage.nflverse.USAGE_STATS`, plus
        ``opportunity_games``.

    Raises:
        FileNotFoundError: When a season has not been pulled.
    """
    usage = load_opportunity(seasons)
    stats = [s for s in USAGE_STATS if f"{ACTUAL_PREFIX}{s}" in usage.columns]

    aggregated = usage.group_by(["season", "gsis_id"]).agg(
        [pl.len().cast(pl.Int32).alias("opportunity_games")]
        + [pl.col(f"{prefix}{stat}").mean().alias(f"{prefix}{stat}_pg")
           for stat in stats for prefix in (ACTUAL_PREFIX, EXPECTED_PREFIX)]
    )
    return aggregated.sort(["gsis_id", "season"])


def prior_season_frame(target_season: int, history: Sequence[int],
                       shrinkage: Optional[Dict[str, float]] = None) -> pl.DataFrame:
    """Everything knowable about players from seasons before ``target_season``.

    Args:
        target_season: The season being predicted. Excluded from every input.
        history: Completed seasons available to learn from.
        shrinkage: Passed to :func:`attach_efficiency`.

    Returns:
        pl.DataFrame: One row per ``(season, gsis_id)`` for each season in
        ``history`` before ``target_season``, with volume, share, efficiency,
        expected-production and availability columns.

    Raises:
        ValueError: When ``history`` contains ``target_season`` or later. The whole
            point of this function is the boundary, so crossing it is an error
            rather than a filter.
        FileNotFoundError: When a season has not been pulled.
    """
    future = sorted(s for s in history if s >= target_season)
    if future:
        raise ValueError(
            f"prior_season_frame for {target_season} was given {future}, which it "
            f"is not allowed to see. Pass only completed prior seasons."
        )
    seasons = sorted(set(history))
    if not seasons:
        return pl.DataFrame(schema={"season": pl.Int32, "gsis_id": pl.String})

    weekly = load_player_weeks(seasons)
    totals = season_totals(weekly)
    baselines = positional_baselines(totals)
    totals = attach_efficiency(totals, baselines, shrinkage)

    availability = ctx.season_availability(seasons, weekly).drop(
        [c for c in ("weeks_on_a_roster",) if True], strict=False)

    return (
        totals
        .join(expected_production(seasons), on=["season", "gsis_id"], how="left")
        .join(availability, on=["season", "gsis_id"], how="left")
        .sort(["gsis_id", "season"])
    )


def roster_context(target_season: int, prior: pl.DataFrame) -> pl.DataFrame:
    """The target season's pre-season roster, with what a drafter can read off it.

    The only current-season input a pre-season model has, and it is a real one --
    served and updated daily before week 1, unlike injuries, snap counts and depth
    charts, which nflreadr refuses until the season is under way.

    ``team_changed`` is the feature worth the join: usage is sticky for a player in
    a stable situation, and plan 18 records that the model will be confidently wrong
    about exactly the players whose situation moved. Knowing which players those are
    is what lets the interval widen instead of the point estimate lying.

    Args:
        target_season: The season being predicted.
        prior: :func:`prior_season_frame` output, for the previous team.

    Returns:
        pl.DataFrame: One row per ``(gsis_id)`` on the target season's roster, with
        ``team``, ``position``, ``status``, ``depth_chart_position``, ``years_exp``,
        ``is_rookie``, ``draft_number`` and ``team_changed``.

    Raises:
        FileNotFoundError: When the target season's rosters have not been pulled.
    """
    rosters = ctx.load_rosters([target_season])

    # The earliest snapshot, which pre-season is the only one. In-season this keeps
    # the function answering the pre-season question rather than drifting to "who is
    # on the roster now".
    first_week = rosters["week"].min()
    snapshot = rosters.filter(pl.col("week") == first_week)

    last_team = (
        prior.filter(pl.col("season") == pl.col("season").max())
        .select("gsis_id", pl.col("team").alias("prior_team"))
        if prior.height and "team" in prior.columns
        else pl.DataFrame(schema={"gsis_id": pl.String, "prior_team": pl.String})
    )

    out = snapshot.select(
        "gsis_id", "team", "position", "status", "full_name",
        *[pl.col(c) for c in ("depth_chart_position", "years_exp", "draft_number")
          if c in snapshot.columns],
        *([pl.col("entry_year")] if "entry_year" in snapshot.columns else []),
    ).join(last_team, on="gsis_id", how="left")

    exprs = [
        pl.when(pl.col("prior_team").is_null())
        .then(None)
        .otherwise(pl.col("team") != pl.col("prior_team"))
        .alias("team_changed"),
    ]
    if "entry_year" in out.columns:
        exprs.append((pl.col("entry_year") >= target_season).alias("is_rookie"))
    elif "years_exp" in out.columns:
        exprs.append((pl.col("years_exp").fill_null(0) == 0).alias("is_rookie"))

    return out.with_columns(exprs)


def attach_context(features: pl.DataFrame, target_season: int,
                   history: Sequence[int]) -> pl.DataFrame:
    """Join the situational context: depth chart, coach prior, team prior.

    All three are legitimately knowable before a draft. The depth chart is a
    current-season fact published daily; the coach and team priors use only seasons
    before ``target_season``. See ``docs/plans/21-coaching-and-scheme.md``.

    Missing context is not fatal. The coaching table is committed but the depth-chart
    pull is not, and a feature frame without them is still a feature frame -- the
    model abstains on a null feature the same way it abstains on a null lag.

    Args:
        features: :func:`season_features` output so far, with ``team``.
        target_season: Season being predicted.
        history: Completed seasons the priors may use.

    Returns:
        pl.DataFrame: ``features`` plus the depth and prior columns it could resolve.
    """
    out = features

    try:
        depth = ctx.depth_features([target_season])
        out = out.join(depth.drop("season"), on="gsis_id", how="left")
    except FileNotFoundError:
        pass

    try:
        staff = sc.load_staff()
        profile = sc.team_profile(load_player_weeks(sorted(history)))
        out = sc.attach(out, profile, staff, target_season)
    except FileNotFoundError:
        pass

    return out


def season_features(target_season: int, history: Sequence[int],
                    positions: Sequence[str] = MODELLED_POSITIONS,
                    shrinkage: Optional[Dict[str, float]] = None,
                    include_context: bool = True) -> pl.DataFrame:
    """One row per rostered player, with lagged features and current context.

    The frame the season head fits and predicts on. Features carry
    :data:`LAG1_PREFIX` for the season before ``target_season`` and
    :data:`LAG2_PREFIX` for the one before that, so "last year" and "the year
    before" stay distinguishable -- a running back coming off an injured season
    looks nothing like one coming off two.

    Args:
        target_season: Season to build features for.
        history: Completed seasons available. Any at or after ``target_season`` is
            an error, not a filter.
        positions: Positions to keep. Defaults to :data:`MODELLED_POSITIONS`.
        shrinkage: Passed to :func:`attach_efficiency`.
        include_context: Join the depth-chart and coaching context. False keeps the
            frame to lagged production only, which is what the backtest compares
            against. Named for the parameter it is rather than `context`, which
            collides with the local holding the roster frame.

    Returns:
        pl.DataFrame: One row per ``gsis_id`` on the target season's roster at the
        modelled positions, with ``season`` stamped as ``target_season``.

    Raises:
        ValueError: When ``history`` reaches ``target_season`` or beyond.
        FileNotFoundError: When a required pull is missing.
    """
    prior = prior_season_frame(target_season, history, shrinkage)
    context = roster_context(target_season, prior)

    if positions:
        context = context.filter(pl.col("position").is_in(list(positions)))

    out = context.with_columns(pl.lit(target_season).cast(pl.Int32).alias("season"))
    for lag, prefix in ((1, LAG1_PREFIX), (2, LAG2_PREFIX)):
        season = target_season - lag
        lagged = prior.filter(pl.col("season") == season)
        # Position and team come from the current roster; a lagged copy would only
        # invite a join on the wrong one.
        lagged = lagged.drop([c for c in ("season", "position", "team")
                              if c in lagged.columns])
        lagged = lagged.rename({c: f"{prefix}{c}" for c in lagged.columns
                                if c != "gsis_id"})
        out = out.join(lagged, on="gsis_id", how="left")

    if include_context:
        out = attach_context(out, target_season, history)
    return out.sort("gsis_id")


def leakage_columns(frame: pl.DataFrame, target_season: int) -> List[str]:
    """Columns that could only have been computed from ``target_season`` or later.

    A named check rather than a comment, because leakage is the risk plan 16 calls
    out as the one that makes a backtest excellent and the live model useless.
    Every feature column must be lag-prefixed or come from the roster snapshot; a
    bare stat name means an unlagged join slipped through.

    Args:
        frame: :func:`season_features` output.
        target_season: The season it claims to be for.

    Returns:
        list: Offending column names. Empty is the passing result.
    """
    allowed = {
        "gsis_id", "season", "team", "position", "status", "full_name",
        "depth_chart_position", "years_exp", "draft_number", "entry_year",
        "prior_team", "team_changed", "is_rookie",
        # Current-season facts a drafter can read off a roster or a depth chart.
        "depth_rank", "is_first_string", "depth_position_group", "depth_team",
        "head_coach", "prior_head_coach", "coach_seasons", "coach_is_new",
        "staff_continuity",
    }
    # Coach and team priors are computed from prior seasons only -- the boundary is
    # enforced in Scripts.usage.scheme and tested there -- so they are allowed by
    # prefix rather than being listed one by one.
    prior_prefixes = (sc.COACH_PREFIX, sc.TEAM_PREFIX)
    return sorted(
        column for column in frame.columns
        if column not in allowed
        and not column.startswith((LAG1_PREFIX, LAG2_PREFIX) + prior_prefixes)
    )
