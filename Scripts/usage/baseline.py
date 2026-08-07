"""The crudest usage model there is: two trailing terms per stat.

``docs/plans/16-usage-data-layer.md`` step 0 asks for exactly this and nothing
more. It is not meant to be good. It exists so that the gates have residuals to
measure, before any effort goes into features that might turn out to be worth
nothing -- the plan's own measurements already killed most of the speculative
feature engineering, and the independence question could kill the rest.

Per stat, fitted on seasons the target season is not in::

    actual_<stat> at week N  ~  b0 + b1 * t3_actual + b2 * t3_expected

where the two trailing terms are means over the player's previous up-to-three
appearances *within the same season*. Two consequences worth being explicit
about:

* **Nothing about week N enters week N's features.** That is the one mistake that
  makes a backtest look excellent and a live model useless, so the trailing means
  shift before they roll and :mod:`tests.test_usage_baseline` pins the property.
* **It abstains rather than guessing.** A player with no prior appearance gets no
  prediction, which is how a wholly-absent source already degrades in this
  pipeline: ``impute_columns`` fills the column from ``MEAN_`` and
  ``compute_weighted_stats`` renormalises the imputed weight away.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts.usage.nflverse import (
    ACTUAL_PREFIX,
    EXPECTED_PREFIX,
    USAGE_STATS,
    load_opportunity,
)

#: Trailing window, in appearances rather than weeks. Three matches the
#: measurement in the plan (``t3_actual``/``t3_exp``), and a window counted in
#: appearances rather than calendar weeks is what makes a bye or a missed game
#: shorten the history instead of blanking it.
TRAILING_WINDOW = 3

#: Prefix for a trailing feature: ``t3_act_receivingYards``.
TRAILING_PREFIX = "t3_"

#: Prefix the usage model's output carries into the blend, beside ``ESPN_``,
#: ``FP_``, ``PINNY_`` and ``BOL_``. Not ``PBP_``, which the earlier plan used and
#: which would misdescribe a model fed by expected production rather than raw
#: play-by-play.
USAGE_PREFIX = "USG_"


def trailing_columns(stat: str) -> Tuple[str, str]:
    """The two feature column names for ``stat``.

    Args:
        stat: ESPN stat name, e.g. ``"receivingYards"``.

    Returns:
        tuple: ``(trailing actual, trailing expected)`` column names.
    """
    return (f"{TRAILING_PREFIX}{ACTUAL_PREFIX}{stat}",
            f"{TRAILING_PREFIX}{EXPECTED_PREFIX}{stat}")


def trailing_features(usage: pl.DataFrame,
                      stats: Optional[Sequence[str]] = None,
                      window: int = TRAILING_WINDOW) -> pl.DataFrame:
    """Add the trailing actual and expected means, built from prior weeks only.

    The ``shift(1)`` is the leakage guard and is the whole reason this is a
    function rather than three lines inline: week *N*'s own row must not reach
    week *N*'s features. Nulls inside a window are skipped rather than poisoning
    it, matching ``R/UsageEvidence.R``'s ``trailing_mean``.

    Args:
        usage: Frame from :func:`Scripts.usage.nflverse.load_opportunity`.
        stats: ESPN stat names to build features for. Defaults to every stat the
            frame carries an ``act_``/``exp_`` pair for.
        window: How many prior appearances to average.

    Returns:
        pl.DataFrame: ``usage`` sorted by ``(gsis_id, season, week)`` with two
        added columns per stat, null where the player has no prior appearance
        that season.
    """
    if stats is None:
        stats = [s for s in USAGE_STATS
                 if f"{ACTUAL_PREFIX}{s}" in usage.columns
                 and f"{EXPECTED_PREFIX}{s}" in usage.columns]

    # Sorted here rather than assumed: rolling windows are positional, so a frame
    # in the wrong order produces a feature built from the wrong weeks with no
    # error anywhere.
    out = usage.sort(["gsis_id", "season", "week"])

    exprs = []
    for stat in stats:
        for prefix in (ACTUAL_PREFIX, EXPECTED_PREFIX):
            source = f"{prefix}{stat}"
            exprs.append(
                pl.col(source)
                .shift(1)
                .rolling_mean(window_size=window, min_periods=1)
                .over(["gsis_id", "season"])
                .alias(f"{TRAILING_PREFIX}{source}")
            )
    return out.with_columns(exprs)


def as_of_features(usage: pl.DataFrame, grid: pl.DataFrame,
                   stats: Optional[Sequence[str]] = None,
                   window: int = TRAILING_WINDOW) -> pl.DataFrame:
    """Trailing features for an arbitrary player-week grid, as of each week.

    :func:`trailing_features` can only describe weeks the player *has a usage row
    for*, which are exactly the weeks he played. Building the model's coverage
    that way would make it depend on the outcome: it would project only players
    who turned out to take the field, while ESPN, FantasyPros and the books
    project everyone and eat the error when a projected starter is inactive.

    So the grid is an input. For each requested ``(gsis_id, week)`` this takes the
    snapshot of trailing usage that existed *before* that week, whether or not the
    player appeared in it -- which is also what a live weekly run has: a roster to
    project and a history to project it from.

    Args:
        usage: Single-season frame from
            :func:`Scripts.usage.nflverse.load_opportunity`.
        grid: Frame with ``gsis_id`` and ``week``, the player-weeks to describe.
        stats: ESPN stat names. Defaults to every stat ``usage`` carries a pair for.
        window: Prior appearances to average.

    Returns:
        pl.DataFrame: ``grid`` plus the trailing feature columns, ``weeks_of_history``
        and ``last_posteam``. Feature columns are null where the player has no prior
        appearance.

    Raises:
        ValueError: When ``usage`` spans more than one season. The window is
            partitioned by player only, so two seasons in one frame would let
            December feed the following September -- silently, and in the direction
            that flatters the model.
    """
    if "season" in usage.columns and usage["season"].n_unique() > 1:
        raise ValueError(
            f"as_of_features takes one season at a time; got "
            f"{sorted(usage['season'].unique())}. Trailing history must not cross "
            f"a season boundary."
        )
    if stats is None:
        stats = [s for s in USAGE_STATS
                 if f"{ACTUAL_PREFIX}{s}" in usage.columns
                 and f"{EXPECTED_PREFIX}{s}" in usage.columns]

    exprs = []
    for stat in stats:
        for prefix in (ACTUAL_PREFIX, EXPECTED_PREFIX):
            source = f"{prefix}{stat}"
            exprs.append(
                pl.col(source)
                .rolling_mean(window_size=window, min_periods=1)
                .over("gsis_id")
                .alias(f"{TRAILING_PREFIX}{source}")
            )

    # One snapshot per appearance, valid from the *following* week. No shift() is
    # needed because the offset lives in valid_from instead: an as-of match at week
    # N can never reach a snapshot stamped N+1.
    snapshots = (
        usage.sort(["gsis_id", "week"])
        .with_columns(exprs)
        .with_columns(
            (pl.col("week") + 1).alias("valid_from"),
            pl.col("week").cum_count().over("gsis_id").alias("weeks_of_history"),
            pl.col("posteam").alias("last_posteam"),
        )
        .select(["gsis_id", "valid_from", "weeks_of_history", "last_posteam"]
                + [f"{TRAILING_PREFIX}{p}{s}" for s in stats
                   for p in (ACTUAL_PREFIX, EXPECTED_PREFIX)])
        .sort("valid_from")
    )

    return (
        grid.sort("week")
        .join_asof(snapshots, left_on="week", right_on="valid_from",
                   by="gsis_id", strategy="backward")
        .drop("valid_from")
    )


@dataclass(frozen=True)
class StatFit:
    """One stat's fitted two-term regression.

    Attributes:
        stat: ESPN stat name.
        intercept: Fitted constant.
        beta_actual: Coefficient on the trailing actual mean.
        beta_expected: Coefficient on the trailing expected mean.
        n: Rows fitted on.
        r2: In-sample R-squared, for reporting only.
    """
    stat: str
    intercept: float
    beta_actual: float
    beta_expected: float
    n: int
    r2: float


@dataclass(frozen=True)
class UsageBaseline:
    """A fitted baseline: one :class:`StatFit` per stat.

    Attributes:
        fits: Stat name to its fit.
        train_seasons: Seasons the coefficients came from. Held so that a
            prediction can be checked for having been made out of sample.
        window: Trailing window used to build the features.
    """
    fits: Dict[str, StatFit]
    train_seasons: Tuple[int, ...]
    window: int = TRAILING_WINDOW

    @property
    def stats(self) -> List[str]:
        """Stats this baseline can predict."""
        return list(self.fits)

    def predict(self, features: pl.DataFrame) -> pl.DataFrame:
        """Add a ``USG_<stat>`` column per fitted stat.

        Args:
            features: Frame from :func:`trailing_features`.

        Returns:
            pl.DataFrame: ``features`` plus one prediction column per stat, null
            where the player has no trailing history, and null where he has no
            trailing *opportunity of that kind* -- see the abstention note below.

        Raises:
            KeyError: When a fitted stat's feature columns are absent.
        """
        exprs = []
        for stat, fit in self.fits.items():
            actual_column, expected_column = trailing_columns(stat)
            missing = [c for c in (actual_column, expected_column)
                       if c not in features.columns]
            if missing:
                raise KeyError(
                    f"{missing} missing from the feature frame; build it with "
                    f"trailing_features() or as_of_features()."
                )
            prediction = (
                pl.lit(fit.intercept)
                + pl.col(actual_column) * fit.beta_actual
                + pl.col(expected_column) * fit.beta_expected
            )
            exprs.append(
                pl.when(pl.col(actual_column).is_null()
                        | pl.col(expected_column).is_null()
                        # No trailing opportunity of this kind means the fit does
                        # not apply: it was estimated on players who had some, so
                        # what is left at zero is the intercept -- 38 passing yards
                        # for every wide receiver. The plan's own positional-
                        # coverage risk names this exact failure: a model that
                        # quietly emits a positional average has apparent full
                        # coverage and drags the blend toward the mean. Abstaining
                        # is the absent-source path, which the blend already
                        # renormalises away.
                        | ((pl.col(actual_column) <= 0)
                           & (pl.col(expected_column) <= 0)))
                .then(None)
                # A negative stat line is not a thing, and the blend would price
                # one. Clipping is the only judgement in this model.
                .otherwise(prediction.clip(lower_bound=0.0))
                .alias(f"{USAGE_PREFIX}{stat}")
            )
        return features.with_columns(exprs)

    def summary(self) -> str:
        """A printable coefficient table.

        Returns:
            str: One line per stat, with coefficients, n and in-sample R-squared.
        """
        lines = [f"  trained on {min(self.train_seasons)}-{max(self.train_seasons)}, "
                 f"trailing window {self.window}",
                 f"  {'stat':<22}{'n':>8}{'intercept':>11}{'b_actual':>10}"
                 f"{'b_expected':>12}{'R2':>8}"]
        for stat, fit in self.fits.items():
            lines.append(
                f"  {stat:<22}{fit.n:>8}{fit.intercept:>11.3f}"
                f"{fit.beta_actual:>10.3f}{fit.beta_expected:>12.3f}{fit.r2:>8.4f}"
            )
        return "\n".join(lines)


def _fit_one(frame: pl.DataFrame, stat: str) -> Optional[StatFit]:
    """Least squares for a single stat.

    Fitted only where the player had *some* opportunity of this kind in the
    trailing window. Without that filter every quarterback's receiving line and
    every receiver's passing line -- structural zeros, perfectly predicted -- would
    dominate the fit and flatter the R-squared while pulling the slope toward the
    zeros. Predictions are still produced for those rows; they come out at roughly
    the intercept, which is the right answer.

    Args:
        frame: Feature frame for the training seasons.
        stat: ESPN stat name.

    Returns:
        StatFit | None: None when too few rows qualify to fit on.
    """
    actual_column, expected_column = trailing_columns(stat)
    target = f"{ACTUAL_PREFIX}{stat}"

    sample = frame.filter(
        pl.col(actual_column).is_not_null()
        & pl.col(expected_column).is_not_null()
        & pl.col(target).is_not_null()
        & ((pl.col(actual_column) > 0) | (pl.col(expected_column) > 0))
    )
    if sample.height < 100:
        return None

    design = np.column_stack([
        np.ones(sample.height),
        sample[actual_column].to_numpy(),
        sample[expected_column].to_numpy(),
    ])
    y = sample[target].to_numpy()
    coefficients, *_ = np.linalg.lstsq(design, y, rcond=None)
    residuals = y - design @ coefficients
    total = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - float((residuals ** 2).sum()) / total if total > 0 else float("nan")

    return StatFit(
        stat=stat,
        intercept=float(coefficients[0]),
        beta_actual=float(coefficients[1]),
        beta_expected=float(coefficients[2]),
        n=sample.height,
        r2=r2,
    )


def fit(features: pl.DataFrame,
        stats: Optional[Sequence[str]] = None,
        window: int = TRAILING_WINDOW) -> UsageBaseline:
    """Fit one two-term regression per stat.

    Args:
        features: Feature frame from :func:`trailing_features`, holding **only**
            the training seasons. Nothing here excludes the target season -- the
            caller owns that, and :func:`fit_seasons` is the safe path.
        stats: Stats to fit. Defaults to every stat with feature columns present.
        window: Recorded on the result; does not affect the fit.

    Returns:
        UsageBaseline: Fitted coefficients, skipping stats with too little data.

    Raises:
        ValueError: When ``features`` carries no season column or no stat fits.
    """
    if "season" not in features.columns:
        raise ValueError("features must carry a season column")

    if stats is None:
        stats = [s for s in USAGE_STATS
                 if trailing_columns(s)[0] in features.columns]

    fits = {}
    for stat in stats:
        result = _fit_one(features, stat)
        if result is not None:
            fits[stat] = result
    if not fits:
        raise ValueError(
            "No stat had enough rows to fit. Check that the feature frame is "
            "not empty and that R/GetUsage.R has been run for the training "
            "seasons."
        )

    seasons = tuple(sorted(int(s) for s in features["season"].unique()))
    return UsageBaseline(fits=fits, train_seasons=seasons, window=window)


def fit_seasons(train_seasons: Sequence[int],
                stats: Optional[Sequence[str]] = None,
                window: int = TRAILING_WINDOW) -> UsageBaseline:
    """Load the training seasons and fit the baseline on them.

    Args:
        train_seasons: Seasons to fit on. The season being evaluated must not be
            among them, or the residuals below are in-sample and the comparison
            against four genuinely ex-ante sources is not a fair one.
        stats: Stats to fit. Defaults to all of :data:`USAGE_STATS`.
        window: Trailing window.

    Returns:
        UsageBaseline: The fitted baseline.

    Raises:
        FileNotFoundError: When a training season has not been pulled.
    """
    usage = load_opportunity(sorted(train_seasons), stats=stats)
    return fit(trailing_features(usage, stats=stats, window=window),
               stats=stats, window=window)


def predict_season(baseline: UsageBaseline, season: int,
                   grid: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Predict one season's stat lines, week by week.

    Args:
        baseline: A fitted baseline.
        season: Season to predict. Loaded on its own, so a player's week-1
            features come from nothing rather than from December of the year
            before -- which is both what a real weekly run has available and what
            the feature builders partition by.
        grid: ``(gsis_id, week)`` player-weeks to predict. Defaults to the weeks
            the players actually appeared in, which is fine for exploration but
            **not** for scoring the model against sources that project everybody:
            pass the grid being evaluated. See :func:`as_of_features`.

    Returns:
        pl.DataFrame: ``season``, ``week``, ``gsis_id``, ``weeks_of_history``,
        ``last_posteam`` and one ``USG_<stat>`` column per fitted stat.

    Raises:
        ValueError: When ``season`` is one of the training seasons, or ``grid``
            lacks its keys.
        FileNotFoundError: When the season has not been pulled.
    """
    if season in baseline.train_seasons:
        raise ValueError(
            f"{season} is in the baseline's training seasons "
            f"{baseline.train_seasons}; predicting it would be in-sample. Fit "
            f"with fit_seasons([...]) excluding {season}."
        )

    usage = load_opportunity([season], stats=baseline.stats)
    if grid is None:
        grid = usage.select(["gsis_id", "week"])
    missing = [c for c in ("gsis_id", "week") if c not in grid.columns]
    if missing:
        raise ValueError(f"grid must carry {missing}")

    features = as_of_features(usage,
                              grid.select(["gsis_id", "week"]).unique(),
                              stats=baseline.stats, window=baseline.window)
    predicted = baseline.predict(features).with_columns(
        pl.lit(season).cast(pl.Int32).alias("season")
    )
    return predicted.select(
        ["season", "week", "gsis_id", "weeks_of_history", "last_posteam"]
        + [f"{USAGE_PREFIX}{s}" for s in baseline.stats]
    )
