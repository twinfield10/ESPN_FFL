"""The season usage head: volume x efficiency x games, emitted as stat lines.

``docs/plans/18-season-usage-model.md``. Three heads, kept apart on purpose:

    expected games   x   per-game volume   x   efficiency per opportunity
    (availability)       (opportunity)         (shrunk to positional baselines)
            │                    │                        │
            └────────────────────┴────────────────────────┘
                                 ▼
                      USG_<stat> season total

Why apart rather than one regression onto season points:

- **The halves have different signal.** Plan 16 measured opportunity at r 0.86-0.92
  year over year and touchdown rate at 0.234. Fitting them jointly lets the noisy
  half contaminate the predictable half.
- **It is auditable.** "14.6 games x 7.1 targets x 8.4 yards" tells you which term
  was wrong when the projection misses. A single 780-yard number does not.
- **It has to emit stat lines.** Nine leagues price the same line differently --
  a 6-team standard league, a 16-team IDP league, a superflex -- so the model
  produces ``USG_receivingYards`` and ``proj_to_score`` prices it nine ways. A
  points model would need refitting per league.

**Where it abstains.** Kickers and team defences have no usage features, and neither
do rookies until the draft-capital arm earns its place on the walk-forward. Those
rows emit nothing, which the blend already handles as an absent source: imputed from
``MEAN_``, flagged, renormalised out of ``TRUE_``. A model that quietly emits a
positional average would look like full coverage while dragging the blend toward the
mean for exactly the players a board must differentiate -- and this bit during plan
16's step 0, where a passing-yards intercept projected 38 yards for every receiver
in the league.
"""

import json
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts.paths import DATA_DIR
from Scripts.usage import features as ft

#: Prefix the model's output carries into the blend, beside ``ESPN_``/``FP_``/
#: ``PINNY_``/``BOL_``.
USAGE_PREFIX = "USG_"

#: ESPN stat -> (volume feature, efficiency rate). The decomposition itself: every
#: modelled stat is an opportunity count times a rate per opportunity.
STAT_TERMS: Dict[str, Tuple[str, str]] = {
    "receivingYards":       ("targets_pg", "yards_per_target"),
    "receivingReceptions":  ("targets_pg", "catch_rate"),
    "receivingTouchdowns":  ("targets_pg", "rec_td_per_target"),
    "rushingYards":         ("carries_pg", "yards_per_carry"),
    "rushingTouchdowns":    ("carries_pg", "rush_td_per_carry"),
    "passingYards":         ("pass_attempts_pg", "yards_per_attempt"),
    "passingTouchdowns":    ("pass_attempts_pg", "pass_td_per_attempt"),
    "passingInterceptions": ("pass_attempts_pg", "int_per_attempt"),
}

#: Volume features the model predicts forward, each from its own regression.
VOLUME_TARGETS: Tuple[str, ...] = ("targets_pg", "carries_pg", "pass_attempts_pg")

#: Regressors for a volume model. Lagged volume plus the two context flags a
#: drafter can see pre-season.
VOLUME_REGRESSORS: Tuple[str, ...] = (
    "p1_volume", "p2_volume", "p1_games", "team_changed",
)

#: Regressors for the expected-games head.
#:
#: A fitted regression rather than shrinkage toward a positional constant, and the
#: first version got this wrong in an instructive way. Shrinking toward the mean
#: games of every rostered player at a position gives QB 8.2, RB 9.8, TE 9.3, WR
#: 10.2 -- numbers dragged down by the large majority of rostered players who barely
#: play, so a genuine starter was projected for eleven games.
#:
#: Regressing on the player's own prior games fixes that and estimates the shrinkage
#: instead of asserting it: the fitted slope comes out below 1, which is shrinkage,
#: and the intercept carries the role mean. That is what the measurement supports --
#: prior games predict next season at r = +0.663 over the whole pool but only +0.343
#: among players who managed 8+ games, so the relationship is real but far from
#: one-to-one. See plan 18 §Expected games played.
#: ``p1_availability`` is deliberately absent: it is ``games_played /
#: games_available``, so it is collinear with ``p1_games`` up to the 16-to-17-game
#: schedule change. Fitting both gave offsetting nonsense -- RB came out at +1.056
#: on games and −10.160 on availability, QB at +0.052 and +8.620 -- coefficients
#: that cannot be read and will not transfer. ``p1_weeks_on_reserve`` replaces it
#: and is not mechanically tied to appearances: it separates "hurt" from "healthy
#: and benched", and correlates −0.462 with next season's games played.
GAMES_REGRESSORS: Tuple[str, ...] = ("p1_games", "p1_weeks_on_reserve",
                                     "team_changed")

#: Minimum rows before a volume regression is trusted.
MIN_FIT_ROWS = 40

#: Model version, bumped when the structure changes rather than when it is refitted.
MODEL_VERSION = "1.0.0"


@dataclass(frozen=True)
class VolumeFit:
    """One (position, volume stat) regression.

    Attributes:
        position: Position it applies to.
        target: Volume column predicted, e.g. ``targets_pg``.
        intercept: Fitted constant.
        coefficients: Regressor name to coefficient.
        n: Rows fitted on.
        r2: In-sample R-squared, for reporting only.
    """
    position: str
    target: str
    intercept: float
    coefficients: Dict[str, float]
    n: int
    r2: float


@dataclass(frozen=True)
class SeasonUsageModel:
    """A fitted season head.

    Attributes:
        volume: ``(position, target)`` to its fit.
        games: Position to its expected-games fit.
        games_by_position: Position to mean games played, used only when a position
            has no fit and as the reported role mean.
        train_seasons: Seasons the coefficients came from.
        version: :data:`MODEL_VERSION` at fit time.
        fitted_at: Caller-supplied timestamp, or None.
    """
    volume: Dict[Tuple[str, str], VolumeFit]
    games: Dict[str, VolumeFit] = field(default_factory=dict)
    games_by_position: Dict[str, float] = field(default_factory=dict)
    train_seasons: Tuple[int, ...] = ()
    version: str = MODEL_VERSION
    fitted_at: Optional[str] = None

    # --- prediction ------------------------------------------------------

    def expected_games(self, frame: pl.DataFrame) -> pl.Expr:
        """Games played, predicted from the player's own prior availability.

        Falls back to the position's mean where a position has no fit, and to the
        overall mean where it has neither -- both of which are only reachable for a
        position with almost no training rows.

        Args:
            frame: Feature frame from :func:`Scripts.usage.features.season_features`.

        Returns:
            pl.Expr: Expected games played, clipped to a plausible slate.
        """
        fallback = float(np.mean(list(self.games_by_position.values()))
                         if self.games_by_position else 16.0)
        expression = pl.col("position").replace_strict(
            self.games_by_position, default=fallback, return_dtype=pl.Float64)

        values = {
            "p1_games": pl.col(f"{ft.LAG1_PREFIX}games").cast(pl.Float64),
            "p1_weeks_on_reserve": pl.col(f"{ft.LAG1_PREFIX}weeks_on_reserve")
            .cast(pl.Float64),
            "team_changed": pl.col("team_changed").cast(pl.Float64),
        }
        for position, fit in self.games.items():
            terms = pl.lit(fit.intercept)
            for name, coefficient in fit.coefficients.items():
                if name in values:
                    terms = terms + values[name].fill_null(0.0) * coefficient
            expression = pl.when(pl.col("position") == position).then(
                terms).otherwise(expression)

        # A player cannot play more than the slate, and a negative prediction is not
        # a prediction. 18 rather than 17 because a team's weeks, not its games, is
        # what the feature counts when a season runs long.
        return expression.clip(lower_bound=0.0, upper_bound=18.0)

    def predict_volume(self, frame: pl.DataFrame, target: str) -> pl.Expr:
        """One volume stat, predicted forward per position.

        Args:
            frame: Feature frame.
            target: A :data:`VOLUME_TARGETS` entry.

        Returns:
            pl.Expr: Predicted per-game volume, clipped at zero, null where the
            position has no fit.
        """
        lag1, lag2 = f"{ft.LAG1_PREFIX}{target}", f"{ft.LAG2_PREFIX}{target}"
        expression = pl.lit(None, dtype=pl.Float64)

        for (position, fitted_target), fit in self.volume.items():
            if fitted_target != target:
                continue
            terms = pl.lit(fit.intercept)
            values = {
                "p1_volume": pl.col(lag1).cast(pl.Float64).fill_null(0.0),
                "p2_volume": pl.col(lag2).cast(pl.Float64).fill_null(0.0),
                "p1_games": pl.col(f"{ft.LAG1_PREFIX}games").cast(pl.Float64)
                .fill_null(0.0),
                "team_changed": pl.col("team_changed").cast(pl.Float64).fill_null(0.0),
            }
            for name, coefficient in fit.coefficients.items():
                if name in values:
                    terms = terms + values[name] * coefficient
            expression = pl.when(pl.col("position") == position).then(
                terms.clip(lower_bound=0.0)).otherwise(expression)

        return expression

    def predict(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Attach ``USG_<stat>`` season totals, plus the terms behind them.

        The intermediate columns are returned on purpose. Plan 18 asks for
        "18 points per game x 14.2 games" to be visible rather than collapsed, and a
        board that shows target share next to a projection is the deliverable.

        Args:
            frame: Feature frame from
                :func:`Scripts.usage.features.season_features`.

        Returns:
            pl.DataFrame: ``frame`` plus ``expected_games``, ``pred_<volume>`` per
            :data:`VOLUME_TARGETS`, and ``USG_<stat>`` per :data:`STAT_TERMS`. The
            ``USG_`` columns are null wherever the model abstains.
        """
        out = frame.with_columns(self.expected_games(frame).alias("expected_games"))
        out = out.with_columns(
            [self.predict_volume(out, target).alias(f"pred_{target}")
             for target in VOLUME_TARGETS]
        )

        # Abstain outright for a player with no prior season. That is every rookie,
        # which is plan 18's stated v1: the draft-capital arm ships only if it beats
        # abstention on the walk-forward, and a wrong confident answer about rookies
        # is expensive on draft day.
        has_history = pl.col(f"{ft.LAG1_PREFIX}games").is_not_null()

        exprs = []
        for stat, (volume, rate) in STAT_TERMS.items():
            rate_column = f"{ft.LAG1_PREFIX}{rate}"
            if rate_column not in out.columns:
                continue
            predicted = (pl.col("expected_games")
                         * pl.col(f"pred_{volume}")
                         * pl.col(rate_column))
            exprs.append(
                pl.when(has_history
                        & pl.col(f"pred_{volume}").is_not_null()
                        & pl.col(rate_column).is_not_null()
                        # No opportunity of this kind means this stat is not his.
                        # Without the guard a receiver gets an intercept's worth of
                        # passing yards, which is exactly how step 0's baseline
                        # projected 38 passing yards for every wideout.
                        & (pl.col(f"pred_{volume}") > 0))
                .then(predicted.clip(lower_bound=0.0))
                .otherwise(None)
                .alias(f"{USAGE_PREFIX}{stat}")
            )
        return out.with_columns(exprs)

    # --- reporting and persistence ---------------------------------------

    def summary(self) -> str:
        """A printable coefficient table."""
        lines = [
            f"  version {self.version}, trained on "
            f"{min(self.train_seasons)}-{max(self.train_seasons)}",
            "",
            "  expected games (fitted, not a shrinkage constant)",
            f"  {'position':<10}{'n':>7}{'const':>9}{'p1_gms':>9}"
            f"{'reserve':>10}{'moved':>8}{'R2':>8}",
        ]
        for position, fit in sorted(self.games.items()):
            c = fit.coefficients
            lines.append(
                f"  {position:<10}{fit.n:>7}{fit.intercept:>9.3f}"
                f"{c.get('p1_games', 0):>9.3f}"
                f"{c.get('p1_weeks_on_reserve', 0):>10.3f}"
                f"{c.get('team_changed', 0):>8.3f}{fit.r2:>8.4f}")
        lines += [
            "",
            "  volume",
            f"  {'position':<10}{'target':<18}{'n':>7}{'const':>9}"
            f"{'p1':>8}{'p2':>8}{'p1_gms':>8}{'moved':>8}{'R2':>8}",
        ]
        for (position, target), fit in sorted(self.volume.items()):
            c = fit.coefficients
            lines.append(
                f"  {position:<10}{target:<18}{fit.n:>7}{fit.intercept:>9.3f}"
                f"{c.get('p1_volume', 0):>8.3f}{c.get('p2_volume', 0):>8.3f}"
                f"{c.get('p1_games', 0):>8.3f}{c.get('team_changed', 0):>8.3f}"
                f"{fit.r2:>8.4f}"
            )
        return "\n".join(lines)

    def to_dict(self) -> Dict:
        """Serialisable form, with the metadata ``CLAUDE.md`` asks models to carry."""
        return {
            "version": self.version,
            "fitted_at": self.fitted_at,
            "train_seasons": list(self.train_seasons),
            "games_by_position": self.games_by_position,
            "games_fits": [asdict(fit) for fit in self.games.values()],
            "volume_fits": [asdict(fit) for fit in self.volume.values()],
        }

    def save(self, path=None):
        """Write the fitted coefficients as JSON.

        Args:
            path: Destination. Defaults to
                ``Data/NFL/models/season_usage_<version>.json``.

        Returns:
            Path: Where it was written.
        """
        if path is None:
            directory = DATA_DIR / "NFL" / "models"
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"season_usage_{self.version}.json"
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True))
        return path


def _fit_volume(frame: pl.DataFrame, position: str, target: str) -> Optional[VolumeFit]:
    """Least squares for one position's volume stat.

    Args:
        frame: Training rows carrying the lagged regressors and ``y_<target>``.
        position: Position to fit.
        target: Volume column, e.g. ``targets_pg``.

    Returns:
        VolumeFit | None: None when fewer than :data:`MIN_FIT_ROWS` rows qualify.
    """
    lag1, lag2 = f"{ft.LAG1_PREFIX}{target}", f"{ft.LAG2_PREFIX}{target}"
    outcome = f"y_{target}"
    needed = [lag1, lag2, f"{ft.LAG1_PREFIX}games", "team_changed", outcome]
    if any(column not in frame.columns for column in needed):
        return None

    rows = frame.filter(
        (pl.col("position") == position)
        & pl.col(outcome).is_not_null()
        # Fitted only on players who had *some* opportunity of this kind. Otherwise
        # every quarterback's target line and every receiver's attempt line --
        # structural zeros, perfectly predicted -- dominate the fit, flatter the
        # R-squared and pull the slope toward zero.
        & (pl.col(lag1).fill_null(0) > 0)
    ).select(
        pl.col(lag1).cast(pl.Float64).fill_null(0.0).alias("p1_volume"),
        pl.col(lag2).cast(pl.Float64).fill_null(0.0).alias("p2_volume"),
        pl.col(f"{ft.LAG1_PREFIX}games").cast(pl.Float64).fill_null(0.0)
        .alias("p1_games"),
        pl.col("team_changed").cast(pl.Float64).fill_null(0.0).alias("team_changed"),
        pl.col(outcome).cast(pl.Float64).alias("y"),
    ).drop_nulls()

    return _least_squares(rows, VOLUME_REGRESSORS, position, target)


def _least_squares(rows: pl.DataFrame, regressors: Sequence[str],
                   position: str, target: str) -> Optional[VolumeFit]:
    """Ordinary least squares with an intercept, packaged as a :class:`VolumeFit`.

    Args:
        rows: Frame carrying each regressor and ``y``, already null-free.
        regressors: Column names, in the order the coefficients are reported.
        position: Position the fit applies to.
        target: What it predicts, for the record.

    Returns:
        VolumeFit | None: None below :data:`MIN_FIT_ROWS`.
    """
    if rows.height < MIN_FIT_ROWS:
        return None

    design = np.column_stack(
        [np.ones(rows.height)] + [rows[name].to_numpy() for name in regressors])
    y = rows["y"].to_numpy()
    beta, *_ = np.linalg.lstsq(design, y, rcond=None)
    residual = y - design @ beta
    variance = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - float(np.sum(residual ** 2)) / variance if variance > 0 else 0.0

    return VolumeFit(
        position=position,
        target=target,
        intercept=float(beta[0]),
        coefficients={name: float(value)
                      for name, value in zip(regressors, beta[1:])},
        n=rows.height,
        r2=r2,
    )


def _fit_games(frame: pl.DataFrame, position: str) -> Optional[VolumeFit]:
    """Expected games played for one position.

    Fitted only on players who were on a roster the prior season, because that is
    the population the prediction is made for. Rookies have no prior availability at
    all and the model abstains on them entirely.

    Args:
        frame: Training rows carrying ``p1_games``, ``p1_availability`` and
            ``y_games``.
        position: Position to fit.

    Returns:
        VolumeFit | None: None below :data:`MIN_FIT_ROWS`.
    """
    lag_games = f"{ft.LAG1_PREFIX}games"
    lag_reserve = f"{ft.LAG1_PREFIX}weeks_on_reserve"
    if any(c not in frame.columns for c in (lag_games, "y_games")):
        return None

    rows = frame.filter(
        (pl.col("position") == position)
        & pl.col("y_games").is_not_null()
        & pl.col(lag_games).is_not_null()
    ).select(
        pl.col(lag_games).cast(pl.Float64).alias("p1_games"),
        (pl.col(lag_reserve).cast(pl.Float64).fill_null(0.0)
         if lag_reserve in frame.columns
         else pl.lit(0.0)).alias("p1_weeks_on_reserve"),
        pl.col("team_changed").cast(pl.Float64).fill_null(0.0).alias("team_changed"),
        pl.col("y_games").cast(pl.Float64).alias("y"),
    ).drop_nulls()

    return _least_squares(rows, GAMES_REGRESSORS, position, "games")


def training_frame(seasons: Sequence[int], history_start: int,
                   positions: Sequence[str] = ft.MODELLED_POSITIONS) -> pl.DataFrame:
    """Feature rows for several seasons, each with that season's realised outcome.

    One :func:`Scripts.usage.features.season_features` call per season, so every
    row's features come only from seasons before it -- including the positional
    baselines the efficiency features are shrunk toward. Pooling the seasons first
    and lagging afterwards would be simpler and would leak.

    Args:
        seasons: Seasons to build training rows for.
        history_start: Earliest season the features may look back to.
        positions: Positions to model.

    Returns:
        pl.DataFrame: Features plus ``y_<volume>`` per :data:`VOLUME_TARGETS` and
        ``y_<stat>`` season totals per :data:`STAT_TERMS`.

    Raises:
        FileNotFoundError: When a required pull is missing.
    """
    frames = []
    for season in sorted(seasons):
        history = [s for s in range(history_start, season)]
        if not history:
            continue
        features = ft.season_features(season, history, positions=positions)

        # The outcome, from the season itself -- the only place it may appear.
        weekly = ft.load_player_weeks([season])
        totals = ft.season_totals(weekly)
        outcome = totals.select(
            "gsis_id",
            *[pl.col(target).alias(f"y_{target}") for target in VOLUME_TARGETS
              if target in totals.columns],
            pl.col("games").alias("y_games"),
            *[(pl.col(f"tot_{numerator}")).alias(f"y_tot_{numerator}")
              for numerator in ("receiving_yards", "receptions", "receiving_tds",
                                "rushing_yards", "rushing_tds", "passing_yards",
                                "passing_tds", "passing_interceptions")
              if f"tot_{numerator}" in totals.columns],
        )
        frames.append(features.join(outcome, on="gsis_id", how="left"))

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal")


def fit(train: pl.DataFrame, train_seasons: Sequence[int],
        fitted_at: Optional[str] = None,
        positions: Sequence[str] = ft.MODELLED_POSITIONS) -> SeasonUsageModel:
    """Fit the volume regressions and the availability role means.

    Args:
        train: :func:`training_frame` output for the training seasons.
        train_seasons: The seasons it covers, recorded on the model.
        fitted_at: Timestamp to record. Passed in rather than read from the clock so
            a fit is reproducible.
        positions: Positions to fit.

    Returns:
        SeasonUsageModel: The fitted head.
    """
    volume: Dict[Tuple[str, str], VolumeFit] = {}
    games: Dict[str, VolumeFit] = {}
    for position in positions:
        for target in VOLUME_TARGETS:
            fitted = _fit_volume(train, position, target)
            if fitted is not None:
                volume[(position, target)] = fitted
        fitted_games = _fit_games(train, position)
        if fitted_games is not None:
            games[position] = fitted_games

    # The fallback for a position with no games fit, and the number reported. Taken
    # over players who were on a roster the prior season, matching the fitted
    # population -- the mean over *everyone* is dragged down by the majority who
    # barely play, and using it projected eleven games for genuine starters.
    means: Dict[str, float] = {}
    if "y_games" in train.columns:
        by_position = (
            train.filter(pl.col("y_games").is_not_null()
                         & pl.col(f"{ft.LAG1_PREFIX}games").is_not_null())
            .group_by("position")
            .agg(pl.col("y_games").mean().alias("mean_games"))
        )
        means = {k: float(v) for k, v in
                 zip(by_position["position"], by_position["mean_games"])}

    return SeasonUsageModel(
        volume=volume,
        games=games,
        games_by_position=means,
        train_seasons=tuple(sorted(train_seasons)),
        fitted_at=fitted_at,
    )
