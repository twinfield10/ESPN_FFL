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

from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts.usage import context as ctx
from Scripts.usage import scheme as sc
from Scripts.usage.nflverse import (
    ACTUAL_PREFIX,
    EXPECTED_PREFIX,
    USAGE_STATS,
    load_advanced,
    load_contracts,
    load_opportunity,
    player_weeks_path,
)

#: Month and day the regular season opens, for measuring age against.
#:
#: Approximate on purpose -- the opener moves by a few days a year and no feature is
#: sensitive to that. What it is *not* is a bare year subtraction, which puts a
#: January and a December birthday most of a year apart.
SEASON_START = (9, 1)

#: Days per year, leap-adjusted, for converting a day count to an age.
DAYS_IN_YEAR = 365.25

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

#: Regressors for each efficiency rate's *baseline*, from the ``R/GetAdvanced.R``
#: pulls. See :func:`fit_rate_baselines` for what is done with them.
#:
#: The efficiency head has never had a feature. It shrinks every player toward his
#: position's pooled rate, which is a good default and a strictly information-free
#: one: two receivers with the same target count get the same prior no matter that
#: one of them runs go routes and the other runs slants.
#:
#: What is here is chosen mechanically rather than by search. A rate is a ratio, so
#: its baseline should be built from the things that determine the *denominator's
#: quality*: how far downfield the ball was thrown, how open the man was, how
#: stacked the box was, and -- for touchdown rates -- how close to the goal line the
#: opportunity came. Catch rate falls with depth of target; yards per carry rises
#: with yards over expected; a touchdown rate is mostly field position wearing a
#: rate's clothing. None of that is a surprising claim, which is the point: a
#: baseline is a prior, and a prior built from surprising claims is how a model
#: acquires confident nonsense.
#:
#: Passing rates are absent, and that is a data gap rather than a judgement. NGS
#: publishes a passing feed with completion percentage over expected in it, which is
#: the obvious regressor for ``yards_per_attempt`` and ``pass_td_per_attempt``, and
#: ``R/GetAdvanced.R`` does not currently pull it. Recorded in plan 22 as untested.
RATE_BASELINE_FEATURES: Dict[str, Tuple[str, ...]] = {
    "yards_per_target": ("ngs_adot", "ngs_separation", "ngs_yac_oe",
                         "ngs_air_yards_share"),
    "catch_rate":       ("ngs_adot", "ngs_separation", "ngs_cushion"),
    "rec_td_per_target": ("rz10_target_share", "ez_targets_pg", "ngs_adot"),
    "yards_per_carry":  ("ngs_ryoe_per_att", "ngs_stacked_box_pct",
                         "ngs_time_to_los"),
    "rush_td_per_carry": ("rz5_carry_share", "rz10_carry_share"),
}

#: Minimum opportunities before a player-season may inform a baseline fit.
#:
#: Per rate, in denominator units, and set well below :data:`SHRINKAGE_K` on
#: purpose. This threshold governs who *teaches* the baseline, not who receives it;
#: everyone receives one. A player with twenty targets is a noisy observation but an
#: unbiased one, and the fit is weighted by opportunity anyway.
RATE_FIT_MIN_DENOMINATOR: float = 20.0

#: Fewest player-seasons before a rate baseline may be fitted at all.
RATE_FIT_MIN_ROWS: int = 60

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

#: Windowed-history prefixes: the best and the average season inside the last N.
#:
#: **Deliberately not ``p3_``/``p5_``.** Plan 32 named them that, and it is the wrong
#: name in this module: ``p1_`` and ``p2_`` mean *lag one* and *lag two*, so ``p3_``
#: reads as "three seasons ago" rather than "the best of the last three". These are
#: aggregates over a window, not a third lag, and the two are used differently enough
#: that a reader must not have to check.
#:
#: The window is what the model was missing at quarterback. Two lags cannot separate
#: "lost the job for eight weeks" from "is not a starter" -- both look like a low
#: ``p1_``. A peak over three seasons can, and plan 32 measured it at +4.5% MAE on
#: quarterback movers with ``peak3`` outweighing ``p1_`` in the fit.
PEAK_PREFIXES: Dict[int, str] = {3: "peak3_", 5: "peak5_"}

#: Prefix for the mean season inside the last three.
#:
#: Peak alone rewards one outlying year. The mean beside it is what says whether the
#: peak was the player or the schedule -- fitted together, plan 32 measured the pair
#: at +4.5% where ``peak3`` alone gave +5.2% on movers but less on all quarterbacks.
MEAN3_PREFIX = "mean3_"

#: Column counting seasons actually observed inside the three-season window.
#:
#: Without it a peak of zero is ambiguous in exactly the way that matters: a rookie
#: with no window and a veteran who genuinely did nothing both read as 0.0, and the
#: first is the population the rookie arm exists for.
WINDOW_SEASONS_COLUMN = "peak3_seasons"


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
                      shrinkage: Optional[Dict[str, float]] = None,
                      rate_baselines: Optional[
                          Dict[Tuple[str, str], Dict[str, float]]] = None,
                      ) -> pl.DataFrame:
    """Efficiency rates, shrunk toward a baseline by volume.

    ``(n * own_rate + k * baseline) / (n + k)``, the standard credibility weighting.
    A player with no opportunity at all gets the baseline outright rather than a
    null, because the alternative -- dropping him -- would make the model's coverage
    depend on last season's usage, and a drafter still has to price him.

    With ``rate_baselines`` the target of that shrinkage becomes the *fitted*
    baseline from :func:`fit_rate_baselines` rather than the positional constant.
    Nothing else changes: the same ``k``, the same arithmetic, the same fallback.
    A player whose advanced features are missing, or whose ``(position, rate)`` was
    never fitted, still gets the constant -- so this can only add information, never
    withdraw coverage.

    Args:
        totals: :func:`season_totals` output.
        baselines: :func:`positional_baselines` output.
        shrinkage: Per-rate ``k``. Defaults to :data:`SHRINKAGE_K`.
        rate_baselines: :func:`fit_rate_baselines` output. None keeps the positional
            constant, which is the behaviour every result before plan 22 was
            measured against.

    Returns:
        pl.DataFrame: ``totals`` plus one shrunk column per rate, ``<name>_raw``
        holding the unshrunk value, and ``<name>_prior`` holding whatever it was
        shrunk toward -- both for auditability.
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

        # The prior: the positional constant, overridden per position wherever a fit
        # exists and every one of its regressors resolves on this frame.
        prior = pl.col(f"base_{name}")
        for (position, fitted_rate), fit in (rate_baselines or {}).items():
            if fitted_rate != name:
                continue
            regressors = [c for c in fit
                          if c not in ("intercept", "n", "r2") and c in out.columns]
            if len(regressors) != len(
                    [c for c in fit if c not in ("intercept", "n", "r2")]):
                continue
            predicted = pl.lit(float(fit["intercept"]))
            resolved = pl.lit(True)
            for column in regressors:
                predicted = predicted + pl.col(column).cast(pl.Float64) * fit[column]
                resolved = resolved & pl.col(column).is_not_null()
            # Clipped into the range the positional constant lives in. An
            # extrapolated negative catch rate is worse than no feature at all, and
            # a linear fit on a bounded quantity will produce one eventually.
            predicted = predicted.clip(lower_bound=0.0,
                                       upper_bound=pl.col(f"base_{name}") * 3.0)
            prior = pl.when((pl.col("position") == position) & resolved).then(
                predicted).otherwise(prior)

        own = pl.when(pl.col(den) > 0).then(pl.col(num) / pl.col(den)).otherwise(None)
        exprs.append(own.alias(f"{name}_raw"))
        exprs.append(prior.alias(f"{name}_prior"))
        exprs.append(
            ((pl.col(den).fill_null(0) * own.fill_null(0)
              + k * prior)
             / (pl.col(den).fill_null(0) + k)).alias(name)
        )

    return out.with_columns(exprs).drop([f"base_{name}" for name in rates])


def advanced_totals(seasons: Sequence[int]) -> pl.DataFrame:
    """Season aggregates of the routes, NGS and red-zone pulls.

    All three arrive per player-week from ``R/GetAdvanced.R`` and are reduced here
    to the player-season grain the rest of this module works in. Absent pulls are
    skipped, not raised on -- see :func:`Scripts.usage.nflverse.load_advanced`.

    Counting rules, each chosen for a reason:

    * Routes are **summed and re-divided**, so ``route_share`` is routes over the
      dropbacks his teams actually threw, not the mean of his weekly shares. A
      player who ran 40 routes in a 45-dropback game and 2 in a 10-dropback game is
      not a 54%-share player.
    * NGS columns are **averaged over weeks**, because they arrive as per-week
      averages already and there is no count to re-weight them by.
    * Red-zone counts are summed, and shares are taken against the team totals that
      travel with them. Five goal-line carries on a team with eight is a role; five
      on a team with forty is not.

    Args:
        seasons: Season years to read.

    Returns:
        pl.DataFrame: One row per ``(season, gsis_id)``. Empty with the key schema
        when none of the three has been pulled.
    """
    empty = pl.DataFrame(schema={"season": pl.Int32, "gsis_id": pl.String})
    pieces = []

    routes = load_advanced(seasons, "routes")
    if routes.height:
        pieces.append(
            routes.group_by(["season", "gsis_id"]).agg(
                pl.col("routes").sum().alias("routes"),
                pl.col("team_dropbacks").sum().alias("route_dropbacks"),
                pl.len().cast(pl.Int32).alias("route_games"),
            ).with_columns(
                (pl.col("routes") / pl.col("route_dropbacks")).alias("route_share"),
                (pl.col("routes") / pl.col("route_games")).alias("routes_pg"),
            )
        )

    ngs = load_advanced(seasons, "ngs")
    if ngs.height:
        measures = [c for c in ngs.columns if c.startswith("ngs_")]
        pieces.append(
            ngs.group_by(["season", "gsis_id"]).agg(
                [pl.col(c).mean().alias(c) for c in measures])
        )

    red_zone = load_advanced(seasons, "red_zone")
    if red_zone.height:
        counts = [c for c in red_zone.columns
                  if c.startswith(("rz", "ez", "team_rz", "team_ez"))]
        totals = red_zone.group_by(["season", "gsis_id"]).agg(
            [pl.col(c).sum().alias(c) for c in counts]
            + [pl.len().cast(pl.Int32).alias("red_zone_games")])

        shares = []
        for band in ("rz20", "rz10", "rz5"):
            for plural, singular in (("carries", "carry"), ("targets", "target")):
                own, team = f"{band}_{plural}", f"team_{band}_{plural}"
                if own in totals.columns and team in totals.columns:
                    shares.append(
                        pl.when(pl.col(team) > 0)
                        .then(pl.col(own) / pl.col(team))
                        .otherwise(0.0)
                        .alias(f"{band}_{singular}_share")
                    )
        if "ez_targets" in totals.columns:
            shares.append(
                (pl.col("ez_targets") / pl.col("red_zone_games")).alias("ez_targets_pg"))
            if "team_ez_targets" in totals.columns:
                shares.append(
                    pl.when(pl.col("team_ez_targets") > 0)
                    .then(pl.col("ez_targets") / pl.col("team_ez_targets"))
                    .otherwise(0.0).alias("ez_target_share"))
        pieces.append(totals.with_columns(shares) if shares else totals)

    if not pieces:
        return empty

    out = pieces[0]
    for piece in pieces[1:]:
        out = out.join(piece, on=["season", "gsis_id"], how="full", coalesce=True)

    # NaN to null on the way out, once, so no consumer has to remember. NGS ships
    # NaN for a player-week it could not measure, and polars' `is_not_null()` is
    # True for a NaN -- so a guard written the obvious way lets one through, it
    # propagates silently through the shrinkage arithmetic, and the first visible
    # symptom is a NaN mean three layers downstream. That is exactly what happened
    # on the first run of the efficiency experiment: every receiving MAE came back
    # NaN and the experiment looked like a result rather than a bug. Same failure
    # the interval code documents; fixed here at the source instead of at each use.
    floats = [c for c, dtype in out.schema.items() if dtype in (pl.Float32, pl.Float64)]
    if floats:
        out = out.with_columns([pl.col(c).fill_nan(None) for c in floats])
    return out.sort(["gsis_id", "season"])


def fit_rate_baselines(
    totals: pl.DataFrame,
    baselines: pl.DataFrame,
    features: Optional[Dict[str, Tuple[str, ...]]] = None,
) -> Dict[Tuple[str, str], Dict[str, float]]:
    """Per-player efficiency baselines, fitted instead of asserted.

    :func:`attach_efficiency` shrinks a player's own rate toward his position's
    pooled rate. That constant is a prior, and this replaces it with a *conditional*
    one: the rate a player with his route depth, separation and goal-line role would
    be expected to post. The credibility weighting is unchanged, so a player with a
    lot of opportunity still keeps mostly his own number. What moves is where the
    player with little opportunity gets pulled to.

    Weighted least squares, weighted by the denominator, because the alternative
    lets a twenty-target season argue as loudly as a hundred-and-fifty-target one
    about what a hundred-and-fifty-target season looks like.

    **Fitted on whatever seasons it is given, and the caller owns that boundary.**
    :func:`prior_season_frame` passes prior seasons only, for the same reason
    :func:`positional_baselines` gets the same treatment.

    Args:
        totals: :func:`season_totals` output, joined to :func:`advanced_totals`.
        baselines: :func:`positional_baselines` output, the fallback intercept.
        features: Rate name to regressors. Defaults to
            :data:`RATE_BASELINE_FEATURES`.

    Returns:
        dict: ``(position, rate)`` to ``{"intercept": ..., "<feature>": ..., "n":
        ..., "r2": ...}``. A missing key means no fit was possible and the caller
        should fall back to the positional constant.
    """
    features = RATE_BASELINE_FEATURES if features is None else features
    fits: Dict[Tuple[str, str], Dict[str, float]] = {}
    if not totals.height or "position" not in totals.columns:
        return fits

    for rate, numerator, denominator in EFFICIENCY_RATES:
        regressors = [c for c in features.get(rate, ()) if c in totals.columns]
        if not regressors:
            continue
        num, den = f"tot_{numerator}", f"tot_{denominator}"
        if num not in totals.columns or den not in totals.columns:
            continue

        usable = totals.filter(
            (pl.col(den) >= RATE_FIT_MIN_DENOMINATOR)
            & pl.col(num).is_not_null()
        ).with_columns((pl.col(num) / pl.col(den)).alias("_rate"))

        for position in usable["position"].drop_nulls().unique().to_list():
            # `fill_nan(None)` before `drop_nulls`, and it is load-bearing rather
            # than defensive. Polars treats NaN and null as different things and
            # `drop_nulls` keeps NaN, so an NGS column carrying one -- they do --
            # reaches numpy intact and makes the normal equations unsolvable. This
            # is the same "absent reads as present" failure `stat_intervals`
            # documents, in the same float-shaped disguise, and it surfaced here as
            # `SVD did not converge` rather than as a wrong number.
            rows = usable.filter(pl.col("position") == position).select(
                *[pl.col(c).cast(pl.Float64).fill_nan(None) for c in regressors],
                pl.col("_rate").cast(pl.Float64).fill_nan(None),
                pl.col(den).cast(pl.Float64).fill_nan(None).alias("_w"),
            ).drop_nulls()
            if rows.height < RATE_FIT_MIN_ROWS:
                continue

            design = np.column_stack(
                [np.ones(rows.height)]
                + [rows[c].to_numpy() for c in regressors])
            y = rows["_rate"].to_numpy()
            w = np.sqrt(rows["_w"].to_numpy())
            if not (np.all(np.isfinite(design)) and np.all(np.isfinite(y))
                    and np.all(np.isfinite(w))):
                continue
            # A regressor that never varies in this slice contributes a column of
            # constants that the intercept already spans. lstsq tolerates the rank
            # deficiency; the fit is meaningless, so skip rather than record it.
            if np.any(design[:, 1:].std(axis=0) == 0):
                continue

            try:
                beta, *_ = np.linalg.lstsq(design * w[:, None], y * w, rcond=None)
            except np.linalg.LinAlgError:
                continue

            residual = y - design @ beta
            variance = float(np.sum((y - y.mean()) ** 2))
            r2 = 1.0 - float(np.sum(residual ** 2)) / variance if variance else 0.0

            fit = {"intercept": float(beta[0]), "n": float(rows.height), "r2": r2}
            fit.update({c: float(v) for c, v in zip(regressors, beta[1:])})
            fits[(position, rate)] = fit

    return fits


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
                       shrinkage: Optional[Dict[str, float]] = None,
                       rate_baselines: bool = False) -> pl.DataFrame:
    """Everything knowable about players from seasons before ``target_season``.

    Args:
        target_season: The season being predicted. Excluded from every input.
        history: Completed seasons available to learn from.
        shrinkage: Passed to :func:`attach_efficiency`.
        rate_baselines: Shrink efficiency toward a fitted per-player baseline
            instead of the positional constant. Off by default, because every
            result recorded before plan 22 was measured against the constant and a
            silent change of prior would invalidate the comparison rather than
            improve on it.

    Returns:
        pl.DataFrame: One row per ``(season, gsis_id)`` for each season in
        ``history`` before ``target_season``, with volume, share, efficiency,
        expected-production, availability and advanced-role columns.

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

    # Joined *before* the efficiency shrinkage, because the fitted baseline is a
    # function of these columns. Left join, so a season without the pull behaves
    # exactly as it did before R/GetAdvanced.R existed.
    advanced = advanced_totals(seasons)
    if advanced.height:
        totals = totals.join(advanced, on=["season", "gsis_id"], how="left")

    fitted = (fit_rate_baselines(totals, baselines)
              if rate_baselines and advanced.height else None)
    totals = attach_efficiency(totals, baselines, shrinkage, fitted)

    availability = ctx.season_availability(seasons, weekly).drop(
        [c for c in ("weeks_on_a_roster",) if True], strict=False)

    return (
        totals
        .join(expected_production(seasons), on=["season", "gsis_id"], how="left")
        .join(availability, on=["season", "gsis_id"], how="left")
        .join(snap_share(seasons), on=["season", "gsis_id"], how="left")
        .sort(["gsis_id", "season"])
    )


def contract_context(target_season: int) -> pl.DataFrame:
    """Each player's live contract as of the offseason before ``target_season``.

    A current-season fact, like ``team_changed`` and ``age`` and unlike every lagged
    feature: a deal signed in March is public in August. It is also the only
    forward-looking evidence this model has about a player who changed teams, which
    is the population plan 18 measured as carrying **+32% median rank error** --
    the worst of its three thin-evidence flags.

    Measured before it was built, on next-season volume over prior volume, games and
    ``team_changed``, train 2017-2024 and test 2025. On everyone it is a wash or
    worse: WR −0.0117 R-squared, TE −0.0023, RB +0.0118, QB +0.0226. On changed-teams
    rows alone it is positive at **all four** positions: WR +0.0259 (n=50), RB
    +0.0566 (n=20), TE +0.0995 (n=23), QB +0.0202 (n=19). A settled veteran's own
    prior volume already encodes what his team thinks of him; a mover's does not.
    Hence the interaction rather than a main effect -- see
    :data:`Scripts.usage.season.CONTRACT_REGRESSORS`.

    ``apy_cap_pct`` rather than ``apy``: annual value as a share of the salary cap is
    comparable across a decade in which the cap went from $155M to $280M. Raw dollars
    would fit a coefficient that is mostly inflation.

    **The leakage boundary here is imperfect and the imperfection is named.**
    Contracts are filtered to ``year_signed <= target_season``, which admits a
    mid-season extension signed *during* the season being predicted -- genuinely
    unknowable at draft time. The alternative, ``< target_season``, excludes all of
    March free agency and so discards the entire signal for exactly the movers this
    exists for. The population is dominated by offseason signings, so the residual
    is small; it is recorded rather than hidden, and it biases toward *over*-stating
    the feature's value in the backtest.

    Args:
        target_season: The season being projected.

    Returns:
        pl.DataFrame: One row per ``gsis_id`` with ``contract_apy_pct``,
        ``contract_guaranteed``, ``contract_years``, ``contract_age`` (seasons since
        signing) and ``contract_is_new``. Empty with the key schema when the pull is
        missing.
    """
    contracts = load_contracts()
    if not contracts.height or "apy_cap_pct" not in contracts.columns:
        return pl.DataFrame(schema={"gsis_id": pl.String})

    return (
        contracts
        .filter(pl.col("year_signed") <= target_season)
        .sort(["gsis_id", "year_signed"])
        .group_by("gsis_id")
        .last()
        .select(
            "gsis_id",
            pl.col("apy_cap_pct").cast(pl.Float64).alias("contract_apy_pct"),
            # log1p on millions rather than raw dollars: guarantees run from zero to
            # nine figures and the untransformed column would give one Mahomes deal
            # more leverage than the whole of the tight-end position.
            (pl.col("guaranteed").cast(pl.Float64).clip(lower_bound=0.0)
             .log1p()).alias("contract_guaranteed"),
            pl.col("years").cast(pl.Float64).alias("contract_years"),
            (pl.lit(target_season) - pl.col("year_signed"))
            .cast(pl.Float64).alias("contract_age"),
            (pl.col("year_signed") == target_season).alias("contract_is_new"),
        )
    )


def snap_share(seasons: Sequence[int]) -> pl.DataFrame:
    """Mean offensive snap share per player-season.

    **A role-security signal, and it earns its place on the availability head rather
    than on the volume head.** Measured over 1,605 player-season pairs, predicting
    next season's games played: prior games alone gives R-squared 0.203, and adding
    prior snap share gives **0.230**. That is the largest single improvement
    available to the model's weakest arm, which sits at R-squared ~0.19 and drives
    every fade on the board.

    It reads as role security rather than durability. A player taking 85% of snaps is
    entrenched; one at 25% is one depth-chart move from inactive, and being inactive
    is most of what "games played" measures once a player is on a roster.

    **It is deliberately not used to normalise the volume rates**, which was tried
    first and measured much worse -- see
    ``docs/plans/18-season-usage-model.md`` §Snap share.

    Args:
        seasons: Season years to read.

    Returns:
        pl.DataFrame: ``season``, ``gsis_id``, ``snap_share`` and ``snap_games``.
        Empty when the pull is missing, since snap counts are an in-season dataset
        and a pre-season projection must still build without them.
    """
    empty = pl.DataFrame(schema={"season": pl.Int32, "gsis_id": pl.String,
                                 "snap_share": pl.Float64,
                                 "snap_games": pl.UInt32})
    wanted = sorted(set(seasons))
    if not wanted:
        return empty
    try:
        snaps = ctx.load_snap_counts(wanted)
    except FileNotFoundError:
        return empty
    return (
        snaps.filter(pl.col("offense_snaps") > 0)
        .group_by(["season", "gsis_id"])
        .agg(pl.col("offense_pct").mean().alias("snap_share"),
             pl.len().alias("snap_games"))
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
        *([pl.col("birth_date")] if "birth_date" in snapshot.columns else []),
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

    # Age at the start of the season being projected.
    #
    # A current-season fact, like `team_changed` and unlike every lagged feature: a
    # birth date does not move, so a player's 2026 age is knowable in 2026. Measured
    # against the season opener rather than by subtracting years, because a January
    # birthday and a December one are most of a year apart and the running-back
    # decline is steep enough for that to matter.
    if "birth_date" in out.columns:
        opener = date(target_season, *SEASON_START)
        exprs.append(
            ((pl.lit(opener) - pl.col("birth_date")).dt.total_days() / DAYS_IN_YEAR)
            .alias("age"))

    out = out.with_columns(exprs)
    return out.drop("birth_date", strict=False)


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


def windowed_history(prior: pl.DataFrame, target_season: int,
                     stats: Sequence[str] = ("targets_pg", "carries_pg",
                                             "pass_attempts_pg")) -> pl.DataFrame:
    """A player's best and average season inside the last three and five.

    **The model looked back exactly two seasons and that is too short at
    quarterback.** Plan 32 measured it: a three-season peak is worth +4.5% MAE on
    quarterback movers and ~1% on all quarterbacks, and in the fitted coefficients
    ``peak3`` (+0.221) *outweighs last season* (``p1_`` at +0.061). The mechanism is
    that quarterback usage is close to binary -- last season's attempts confound
    "lost the job for eight weeks" with "is not a starter", and a window separates
    them where a lag cannot.

    Leak-free by construction rather than by filtering: ``prior`` already holds only
    seasons before ``target_season`` -- :func:`prior_season_frame` raises otherwise --
    so the window is bounded below and the upper bound is inherited.

    A player absent from a season contributes nothing to it rather than a zero. The
    two are different and the difference is the whole point of
    :data:`WINDOW_SEASONS_COLUMN`: a quarterback who missed 2024 injured should not
    have a zero averaged into his mean, while one who was a healthy backup taking no
    snaps genuinely did nothing. ``prior`` carries a row only for a season a player
    actually appeared in, so "absent" is already "no row" and the aggregate skips it.

    Args:
        prior: :func:`prior_season_frame` output, one row per player-season.
        target_season: Season being predicted. The window ends the season before it.
        stats: Volume columns to summarise. Defaults to
            :data:`Scripts.usage.season.VOLUME_TARGETS`, spelled out here to avoid
            importing the model into the feature layer.

    Returns:
        pl.DataFrame: One row per ``gsis_id``, with ``peak3_<stat>``,
        ``mean3_<stat>``, ``peak5_<stat>`` and :data:`WINDOW_SEASONS_COLUMN`.
        Players with no season in any window are absent, not zero-filled.
    """
    present = [c for c in stats if c in prior.columns]
    if not present or "season" not in prior.columns:
        return pl.DataFrame(schema={"gsis_id": pl.String})

    frames = []
    for span, prefix in sorted(PEAK_PREFIXES.items()):
        window = prior.filter(
            (pl.col("season") < target_season)
            & (pl.col("season") >= target_season - span))
        aggregations = [pl.col(c).cast(pl.Float64).max().alias(f"{prefix}{c}")
                        for c in present]
        if span == 3:
            aggregations += [
                pl.col(c).cast(pl.Float64).mean().alias(f"{MEAN3_PREFIX}{c}")
                for c in present]
            aggregations.append(
                pl.col("season").n_unique().cast(pl.Int32)
                  .alias(WINDOW_SEASONS_COLUMN))
        frames.append(window.group_by("gsis_id").agg(aggregations))

    out = frames[0]
    for extra in frames[1:]:
        out = out.join(extra, on="gsis_id", how="full", coalesce=True)
    return out


def season_features(target_season: int, history: Sequence[int],
                    positions: Sequence[str] = MODELLED_POSITIONS,
                    shrinkage: Optional[Dict[str, float]] = None,
                    include_context: bool = True,
                    rate_baselines: bool = False,
                    contracts: bool = False) -> pl.DataFrame:
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
        rate_baselines: Passed to :func:`prior_season_frame`.
        contracts: Join :func:`contract_context`. Off by default for the same reason
            as ``rate_baselines`` -- both are plan 22 candidates, and the shipped
            default is whatever the walk-forward endorsed.

    Returns:
        pl.DataFrame: One row per ``gsis_id`` on the target season's roster at the
        modelled positions, with ``season`` stamped as ``target_season``.

    Raises:
        ValueError: When ``history`` reaches ``target_season`` or beyond.
        FileNotFoundError: When a required pull is missing.
    """
    prior = prior_season_frame(target_season, history, shrinkage, rate_baselines)
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

    # After the lags and before everything else, because it reads the same `prior`
    # frame they do. Left-joined like them, so a player with no window keeps nulls
    # rather than zeros -- `_veteran_terms` fills to zero at the point of use, where
    # the arm can tell the two apart.
    out = out.join(windowed_history(prior, target_season), on="gsis_id", how="left")

    if contracts:
        out = out.join(contract_context(target_season), on="gsis_id", how="left")

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
        #
        # `age` was missing here until plan 22 and the check never caught it, because
        # the fixtures build rosters without a `birth_date` so `roster_context` never
        # creates the column. It is legitimate -- a birth date does not move, so a
        # 2026 age is knowable in 2026 -- but it was legitimate by luck rather than
        # by declaration. See test_leakage_guard_covers_age.
        "age",
        "depth_rank", "is_first_string", "depth_position_group", "depth_team",
        "head_coach", "prior_head_coach", "coach_is_new", "staff_continuity",
        # A contract signed in March is public in August. Filtered to
        # `year_signed <= target_season` by contract_context, which documents the
        # one case that boundary admits and should not.
        "contract_apy_pct", "contract_guaranteed", "contract_years",
        "contract_age", "contract_is_new",
        # The keys each prior is joined on, which come off the current roster.
        *sc.PRIOR_KEYS.values(),
    }
    # Coach and team priors are computed from prior seasons only -- the boundary is
    # enforced in Scripts.usage.scheme and tested there -- so they are allowed by
    # prefix rather than listed one by one. Derived from PRIOR_KEYS so adding a fourth
    # prior does not silently trip this check, which is what adding the coordinator
    # and offensive-lead priors did.
    prior_prefixes = tuple(sc.PRIOR_KEYS) + (sc.TEAM_PREFIX,)
    # The windowed-history prefixes, allowed for the same reason as the lags and on
    # the same evidence: `windowed_history` aggregates `prior_season_frame`, which
    # raises rather than filters if history reaches the target season, and bounds
    # its own window below at `target_season - span`. Derived from the constants
    # rather than spelled out, so widening the window cannot silently slip past
    # this check -- which is exactly what adding the coordinator prior did.
    window_prefixes = tuple(PEAK_PREFIXES.values()) + (MEAN3_PREFIX,)
    return sorted(
        column for column in frame.columns
        if column not in allowed
        and not column.startswith((LAG1_PREFIX, LAG2_PREFIX)
                                  + prior_prefixes + window_prefixes)
    )
