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

#: The coach prior and depth chart are **not** here, and that is a measured decision
#: rather than an omission.
#:
#: Adding ``coach_volume`` and ``staff_continuity`` to the veteran arm was tried on
#: the full walk-forward and did nothing: within-position Spearman moved by at most
#: 0.0012 in either direction, per-stat MAE traded −0.3% on receiving for +0.2% on
#: rushing and passing, and **top-N hit rate got worse at three of four positions**
#: (QB 0.607 to 0.595, WR 0.671 to 0.663, TE 0.512 to 0.488). A veteran's own prior
#: volume already encodes his situation; the coach prior adds parameters without
#: information.
#:
#: The same features move the rookie arm substantially -- ρ from 0.602 to 0.659 at QB,
#: 0.618 to 0.645 at RB -- because a rookie has no prior volume for them to be
#: redundant with. Plan 21's criterion is that a feature keeps its place only if it
#: moves the numbers, so they live in :data:`ROOKIE_REGRESSORS` alone.
VETERAN_SITUATIONAL_REJECTED: Tuple[str, ...] = ("coach_volume", "staff_continuity")

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

#: Regressors for the rookie arm.
#:
#: ``log_pick`` rather than the raw pick number: measured on 670 drafted rookie
#: seasons 2017-2025, the log transform beats linear at every position (R² 0.374 vs
#: 0.352 for RB carries, 0.417 vs 0.360 for WR targets, 0.391 vs 0.326 for TE, 0.385
#: vs 0.327 for QB attempts) and ``1/pick`` is worst of the three. Production decays
#: with draft position rather than falling off a line.
#:
#: ``undrafted`` is a separate indicator with ``log_pick`` held at 0, so the two
#: coefficients read directly: the intercept plus ``undrafted`` is what an undrafted
#: rookie gets, and the intercept plus ``log_pick`` times the log of the pick is what
#: a drafted one gets. Undrafted is not "pick 300" -- it is a different population,
#: and 2 of 3 rookies are in it.
ROOKIE_REGRESSORS: Tuple[str, ...] = ("log_pick", "undrafted", "is_first_string")

#: Which of ``Scripts.usage.scheme``'s three priors an arm would use if one did.
#:
#: **None of them does, and that is measured.** All three were tried on the rookie
#: arm's walk-forward, against a version carrying only the depth-chart feature. Mean
#: within-position Spearman across QB/RB/WR/TE:
#:
#:     depth chart only                0.6403
#:     + offensive-coordinator prior   0.6367
#:     + offensive-lead prior          0.6366
#:     + head-coach prior              0.6353
#:     (draft capital alone            0.6132)
#:
#: So **the whole of the rookie arm's improvement is the depth chart**, and every
#: coach prior is a small net loss on top of it -- worst at tight end, where the
#: head-coach prior costs 0.026. An earlier commit credited the gain to the depth
#: chart and the coach prior together; separating them showed only the first earns it.
#:
#: The priors stay built and joined, because they cost nothing to carry, they are worth
#: showing on a board next to a projection, and plan 19's weekly head has not been
#: measured against them yet. They are simply not regressors.
SITUATIONAL_PREFIX = "coach_"

#: Rejected on measurement, in both arms. Kept named so the negative result is not
#: rediscovered -- see SITUATIONAL_PREFIX for the rookie numbers and
#: VETERAN_SITUATIONAL_REJECTED for the veteran ones.
COACH_PRIOR_REJECTED: Tuple[str, ...] = ("coach_volume",)

#: The prior column that informs each volume target -- the pool the position is
#: competing for under this coaching staff. See ``Scripts.usage.scheme``.
VOLUME_PRIOR_METRIC: Dict[str, str] = {
    "targets_pg": "team_wr_targets_pg",
    "carries_pg": "team_rb_carries_pg",
    "pass_attempts_pg": "team_pass_attempts_pg",
}


def coach_volume_column(target: str, prefix: Optional[str] = None) -> str:
    """The prior column for a volume target, under the selected prior.

    Args:
        target: A :data:`VOLUME_TARGETS` entry.
        prefix: Override :data:`SITUATIONAL_PREFIX`.

    Returns:
        str: The column name, or "" when the target has no prior metric.
    """
    metric = VOLUME_PRIOR_METRIC.get(target, "")
    if not metric:
        return ""
    return f"{prefix or SITUATIONAL_PREFIX}{metric}"

#: Minimum rows before a volume regression is trusted.
MIN_FIT_ROWS = 40

#: Minimum rookie rows before the rookie arm is trusted for a position. Lower than
#: :data:`MIN_FIT_ROWS` because rookies are a ninth of the pool -- QB has 96 drafted
#: rookie seasons across nine years -- and higher than nothing because the arm's
#: whole justification is that it beats saying nothing.
MIN_ROOKIE_FIT_ROWS = 30

#: Per-game volume a position must average before that volume is modelled for it.
#:
#: This is a relevance gate, and it exists because the fits it removes were doing
#: real damage quietly. Mean per-game volume in training, by position:
#:
#:     QB   targets 0.015   carries 1.781   attempts 14.316
#:     RB   targets 1.197   carries 4.026   attempts  0.002
#:     WR   targets 2.102   carries 0.083   attempts  0.007
#:     TE   targets 1.513   carries 0.012   attempts  0.001
#:
#: A regression on WR pass attempts fits 111 trick plays, lands at R² 0.0004, and
#: still returns a positive intercept -- so **315 of 389 receivers were given a
#: passing line**, median 2.12 yards, and 117 tight ends a rushing line. Tiny in
#: points, and exactly the failure plan 16's positional-coverage risk names: a model
#: emitting a number for a stat that is not the player's. 0.25 separates the six real
#: (position, volume) pairs from the six junk ones with an order of magnitude to
#: spare either side.
MIN_MEAN_VOLUME = 0.25

#: Draft-capital bins for the rookie availability head, as (first pick, last pick).
#: ``None`` is the undrafted bin.
#:
#: **Binned rather than fitted, and that is the second thing this arm got wrong.**
#: Rookie games played against draft position is flat across the early rounds and
#: then declines -- measured RB means by round: 13.2, 12.6, 10.6, 12.6, 10.0, 7.9,
#: 6.9 -- and no log fit captures flat-then-declining. Linear in log(pick) predicted
#: **21.7 games at pick 1**, clipped to 18, for a population whose round-1 average is
#: 13.2. Searching a shift parameter did not rescue it: the best shift per position
#: ranged 0 to 60, bought at most 0.01 R², and still put pick 1 at 15.9 games.
#:
#: A bin mean cannot extrapolate past what rookies actually did, which is the whole
#: property wanted here. Volume keeps the log fit, where the relationship really is
#: monotone and R² runs 0.39-0.42.
ROOKIE_GAMES_BINS: Tuple[Optional[Tuple[int, int]], ...] = (
    (1, 32), (33, 64), (65, 128), (129, 262), None,
)

#: Pooled opportunities a rookie rate needs before it is recorded at all.
#:
#: Without it the baselines table carries an ``int_per_attempt`` of 0.200 for running
#: backs -- one rookie's single pass attempt, which happened to be intercepted -- and
#: a ``rec_td_per_target`` of 0.200 for quarterbacks. Those rates are unreachable in
#: practice, because the relevance gate means no back gets a passing volume to
#: multiply them by, but they belong in neither the persisted model nor the printed
#: summary: a number built on five opportunities invites being used as though it
#: meant something.
MIN_RATE_DENOMINATOR = 50.0

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
    rookie_volume: Dict[Tuple[str, str], VolumeFit] = field(default_factory=dict)
    rookie_games: Dict[str, Dict[str, float]] = field(default_factory=dict)
    rookie_efficiency: Dict[str, Dict[str, float]] = field(default_factory=dict)
    train_seasons: Tuple[int, ...] = ()
    version: str = MODEL_VERSION
    fitted_at: Optional[str] = None

    # --- the rookie arm --------------------------------------------------

    @staticmethod
    def _rookie_terms(frame: pl.DataFrame,
                      target: Optional[str] = None) -> Dict[str, pl.Expr]:
        """The rookie regressors, as expressions.

        Args:
            frame: Any frame carrying ``draft_number``, and ideally the situational
                columns. A frame without them still works -- the terms go to zero,
                which is the same as the arm not using them.
            target: Volume target, to pick the matching coach-prior column.

        Returns:
            dict: Regressor name to expression.
        """
        def column(name: str) -> pl.Expr:
            return (pl.col(name).cast(pl.Float64).fill_null(0.0)
                    if name in frame.columns else pl.lit(0.0))

        pick = (pl.col("draft_number").cast(pl.Float64)
                if "draft_number" in frame.columns
                else pl.lit(None, dtype=pl.Float64))
        drafted = pick.is_not_null() & (pick > 0)
        coach_column = coach_volume_column(target) if target else ""
        return {
            "log_pick": pl.when(drafted).then(pick.log()).otherwise(0.0),
            "undrafted": pl.when(drafted).then(0.0).otherwise(1.0),
            "is_first_string": column("is_first_string"),
            "coach_volume": column(coach_column) if coach_column else pl.lit(0.0),
        }

    def rookie_expected_games(self, frame: pl.DataFrame) -> pl.Expr:
        """Games played for a rookie, from his draft-capital bin's mean.

        Args:
            frame: Feature frame carrying ``position`` and ``draft_number``.

        Returns:
            pl.Expr: Expected games, null for a position with no rookie table.
        """
        pick = (pl.col("draft_number").cast(pl.Float64)
                if "draft_number" in frame.columns
                else pl.lit(None, dtype=pl.Float64))
        label = draft_bin(pick)

        expression = pl.lit(None, dtype=pl.Float64)
        for position, by_bin in self.rookie_games.items():
            if not by_bin:
                continue
            # The position's own rookie mean, for a bin too thin to have its own.
            fallback = float(np.mean(list(by_bin.values())))
            per_position = pl.lit(fallback)
            for bin_name, games in by_bin.items():
                per_position = pl.when(label == bin_name).then(
                    pl.lit(float(games))).otherwise(per_position)
            expression = pl.when(pl.col("position") == position).then(
                per_position).otherwise(expression)
        return expression

    def _rookie_linear(self, frame: pl.DataFrame, fits: Dict, key,
                       target: Optional[str] = None) -> pl.Expr:
        """Evaluate a per-position rookie fit, keyed by ``key(position)``.

        Args:
            frame: Feature frame.
            fits: Position-keyed (or (position, target)-keyed) fits.
            key: Callable turning a position into the dict key.

        Returns:
            pl.Expr: The prediction, null for positions with no fit.
        """
        terms = self._rookie_terms(frame, target)
        expression = pl.lit(None, dtype=pl.Float64)
        for position in {p for p in
                         (k[0] if isinstance(k, tuple) else k for k in fits)}:
            fit = fits.get(key(position))
            if fit is None:
                continue
            value = pl.lit(fit.intercept)
            for name, coefficient in fit.coefficients.items():
                if name in terms:
                    value = value + terms[name] * coefficient
            expression = pl.when(pl.col("position") == position).then(
                value.clip(lower_bound=0.0)).otherwise(expression)
        return expression

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

    @staticmethod
    def _veteran_terms(frame: pl.DataFrame, target: str, lag1: str,
                       lag2: str) -> Dict[str, pl.Expr]:
        """The veteran regressors, as expressions.

        A frame without the situational columns still works: those terms go to zero,
        which is the same as the arm not using them. That keeps the model usable
        before ``Scripts.coaches`` has been run.

        Args:
            frame: Feature frame.
            target: Volume target, to pick the matching coach-prior column.
            lag1: Prior-season volume column.
            lag2: Two-seasons-prior volume column.

        Returns:
            dict: Regressor name to expression.
        """
        def column(name: str) -> pl.Expr:
            return (pl.col(name).cast(pl.Float64).fill_null(0.0)
                    if name in frame.columns else pl.lit(0.0))

        coach_column = coach_volume_column(target)
        return {
            "p1_volume": column(lag1),
            "p2_volume": column(lag2),
            "p1_games": column(f"{ft.LAG1_PREFIX}games"),
            "team_changed": column("team_changed"),
            "coach_volume": column(coach_column) if coach_column else pl.lit(0.0),
            "staff_continuity": column("staff_continuity"),
        }

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
            values = self._veteran_terms(frame, target, lag1, lag2)
            for name, coefficient in fit.coefficients.items():
                if name in values:
                    terms = terms + values[name] * coefficient
            expression = pl.when(pl.col("position") == position).then(
                terms.clip(lower_bound=0.0)).otherwise(expression)

        return expression

    def predict(self, frame: pl.DataFrame, rookies: bool = True) -> pl.DataFrame:
        """Attach ``USG_<stat>`` season totals, plus the terms behind them.

        The intermediate columns are returned on purpose. Plan 18 asks for
        "18 points per game x 14.2 games" to be visible rather than collapsed, and a
        board that shows target share next to a projection is the deliverable.

        Two arms, chosen per row. A player with a prior season gets the veteran arm,
        which extrapolates his own volume. A rookie has no prior season at all, so
        every veteran feature is null and the arm cannot run; the rookie arm predicts
        from draft capital instead and takes efficiency from the positional *rookie*
        baseline, since rookies are less efficient per opportunity than the pool.

        Args:
            frame: Feature frame from
                :func:`Scripts.usage.features.season_features`.
            rookies: Run the rookie arm. False abstains on rookies, which is the
                comparison plan 18 asks for -- the arm ships only if it beats saying
                nothing.

        Returns:
            pl.DataFrame: ``frame`` plus ``expected_games``, ``pred_<volume>`` per
            :data:`VOLUME_TARGETS`, ``usg_arm`` naming which arm spoke, and
            ``USG_<stat>`` per :data:`STAT_TERMS`. The ``USG_`` columns are null
            wherever the model abstains.
        """
        is_rookie = (pl.col("is_rookie").fill_null(False)
                     if "is_rookie" in frame.columns else pl.lit(False))
        has_history = pl.col(f"{ft.LAG1_PREFIX}games").is_not_null()
        # A rookie is a player with no prior season, and the flag has to agree with
        # that or the two arms would both claim the same row. Prior history wins:
        # a "rookie" with a prior season is a data problem, not a rookie.
        use_rookie = pl.lit(rookies) & is_rookie & ~has_history

        out = frame.with_columns(
            pl.when(use_rookie)
            .then(self.rookie_expected_games(frame))
            .otherwise(self.expected_games(frame))
            .clip(lower_bound=0.0, upper_bound=18.0)
            .alias("expected_games"),
            pl.when(use_rookie).then(pl.lit("rookie"))
            .when(has_history).then(pl.lit("veteran"))
            .otherwise(pl.lit("abstain")).alias("usg_arm"),
        )
        out = out.with_columns([
            pl.when(pl.col("usg_arm") == "rookie")
            .then(self._rookie_linear(
                out, self.rookie_volume, lambda p, t=target: (p, t), target=target))
            .otherwise(self.predict_volume(out, target))
            .alias(f"pred_{target}")
            for target in VOLUME_TARGETS
        ])

        speaks = pl.col("usg_arm") != "abstain"

        exprs = []
        for stat, (volume, rate) in STAT_TERMS.items():
            rate_column = f"{ft.LAG1_PREFIX}{rate}"
            if rate_column not in out.columns:
                continue
            # Rookies have no rate of their own, so they take the positional rookie
            # baseline. Using the whole pool's baseline would overstate them: a
            # rookie is less efficient per opportunity than an established player,
            # and that difference is measurable rather than assumed.
            rookie_rate = pl.lit(None, dtype=pl.Float64)
            for position, rates in self.rookie_efficiency.items():
                if rate in rates:
                    rookie_rate = pl.when(pl.col("position") == position).then(
                        pl.lit(float(rates[rate]))).otherwise(rookie_rate)

            effective_rate = (pl.when(pl.col("usg_arm") == "rookie")
                              .then(rookie_rate)
                              .otherwise(pl.col(rate_column)))
            predicted = (pl.col("expected_games")
                         * pl.col(f"pred_{volume}")
                         * effective_rate)
            exprs.append(
                pl.when(speaks
                        & pl.col(f"pred_{volume}").is_not_null()
                        & effective_rate.is_not_null()
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
            f"{'p1':>8}{'p2':>8}{'p1_gms':>8}{'moved':>8}{'coach':>8}"
            f"{'staff':>8}{'R2':>8}",
        ]
        for (position, target), fit in sorted(self.volume.items()):
            c = fit.coefficients
            lines.append(
                f"  {position:<10}{target:<18}{fit.n:>7}{fit.intercept:>9.3f}"
                f"{c.get('p1_volume', 0):>8.3f}{c.get('p2_volume', 0):>8.3f}"
                f"{c.get('p1_games', 0):>8.3f}{c.get('team_changed', 0):>8.3f}"
                f"{c.get('coach_volume', 0):>8.3f}"
                f"{c.get('staff_continuity', 0):>8.3f}{fit.r2:>8.4f}"
            )

        if self.rookie_volume or self.rookie_games:
            lines += [
                "",
                "  rookie arm (draft capital). undrafted column is the level for an "
                "undrafted rookie,",
                "  relative to the intercept; log_pick is per unit of log(overall "
                "pick).",
                f"  {'position':<10}{'target':<18}{'n':>7}{'const':>9}"
                f"{'log_pick':>10}{'undrafted':>11}{'R2':>8}",
            ]
            rookie_fits = list(self.rookie_volume.values())
            for rookie_fit in sorted(rookie_fits,
                                     key=lambda f: (f.position, f.target)):
                c = rookie_fit.coefficients
                lines.append(
                    f"  {rookie_fit.position:<10}{rookie_fit.target:<18}"
                    f"{rookie_fit.n:>7}{rookie_fit.intercept:>9.3f}"
                    f"{c.get('log_pick', 0):>10.3f}"
                    f"{c.get('undrafted', 0):>11.3f}{rookie_fit.r2:>8.4f}")

        if self.rookie_games:
            lines += ["", "  rookie games played, mean by draft-capital bin"]
            bins = [bin_label(b) for b in ROOKIE_GAMES_BINS]
            lines.append("  " + f"{'position':<10}"
                         + "".join(f"{name:>12}" for name in bins))
            for position, by_bin in sorted(self.rookie_games.items()):
                cells = "".join(
                    (f"{by_bin[name]:>12.1f}" if name in by_bin else f"{'—':>12}")
                    for name in bins)
                lines.append(f"  {position:<10}{cells}")

        if self.rookie_efficiency:
            lines += ["", "  rookie efficiency baselines (pooled, per position)"]
            for position, rates in sorted(self.rookie_efficiency.items()):
                shown = ", ".join(f"{name} {value:.3f}"
                                  for name, value in sorted(rates.items())
                                  if value)
                lines.append(f"  {position:<6}{shown}")

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
            "rookie_games_by_bin": self.rookie_games,
            "rookie_volume_fits": [asdict(fit)
                                   for fit in self.rookie_volume.values()],
            "rookie_efficiency": self.rookie_efficiency,
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
    )
    terms = SeasonUsageModel._veteran_terms(rows, target, lag1, lag2)
    rows = rows.with_columns(
        [expression.alias(name) for name, expression in terms.items()]
    ).select(
        *[pl.col(name) for name in VOLUME_REGRESSORS],
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


def _rookie_rows(frame: pl.DataFrame, position: str) -> pl.DataFrame:
    """Rookie training rows for one position, with the draft-capital regressors.

    Args:
        frame: Training rows.
        position: Position to select.

    Returns:
        pl.DataFrame: ``log_pick``, ``undrafted`` and the identity columns.
    """
    pick = (pl.col("draft_number").cast(pl.Float64)
            if "draft_number" in frame.columns
            else pl.lit(None, dtype=pl.Float64))
    drafted = pick.is_not_null() & (pick > 0)

    # The regressors are attached before any filtering, so an empty result still
    # carries them. Returning `frame.head(0)` early instead raised
    # ColumnNotFoundError on `log_pick` in the caller's select -- an empty frame with
    # the wrong schema is not an empty frame.
    out = frame.with_columns(
        pl.when(drafted).then(pick.log()).otherwise(0.0).alias("log_pick"),
        pl.when(drafted).then(0.0).otherwise(1.0).alias("undrafted"),
    )
    if "is_rookie" not in frame.columns:
        return out.clear()

    return out.filter(
        (pl.col("position") == position)
        & pl.col("is_rookie").fill_null(False)
        # A "rookie" with a prior season is a data problem. Excluded from the fit
        # rather than trusted, because the arm's whole purpose is the no-history case.
        & pl.col(f"{ft.LAG1_PREFIX}games").is_null()
    )


def _fit_rookie_volume(frame: pl.DataFrame, position: str,
                       target: str) -> Optional[VolumeFit]:
    """One position's rookie volume, from draft capital.

    Fitted over **all** rookies including the undrafted and those who never played,
    unlike the veteran arm which excludes structural zeros. That is deliberate: the
    zeros are the prediction here. Two of three rookies go undrafted and 79% of those
    never take a snap, so a model fitted only on rookies who produced would project
    every undrafted free agent as a contributor.

    Args:
        frame: Training rows.
        position: Position to fit.
        target: Volume column, e.g. ``targets_pg``.

    Returns:
        VolumeFit | None: None below :data:`MIN_ROOKIE_FIT_ROWS`.
    """
    outcome = f"y_{target}"
    if outcome not in frame.columns:
        return None
    prepared = _rookie_rows(frame, position)
    terms = SeasonUsageModel._rookie_terms(prepared, target)
    rows = prepared.with_columns(
        [expression.alias(name) for name, expression in terms.items()]
    ).select(
        *[pl.col(name) for name in ROOKIE_REGRESSORS],
        # A rookie who never appeared has no row in the outcome frame, and his
        # realised volume is zero rather than unknown.
        pl.col(outcome).cast(pl.Float64).fill_null(0.0).alias("y"),
    ).drop_nulls()
    if rows.height < MIN_ROOKIE_FIT_ROWS:
        return None
    return _least_squares(rows, ROOKIE_REGRESSORS, position, target)


def bin_label(first: Optional[Tuple[int, int]]) -> str:
    """A stable key for a draft-capital bin.

    Args:
        first: A :data:`ROOKIE_GAMES_BINS` entry.

    Returns:
        str: e.g. ``"1-32"`` or ``"undrafted"``.
    """
    return "undrafted" if first is None else f"{first[0]}-{first[1]}"


def draft_bin(pick: pl.Expr) -> pl.Expr:
    """Map a pick number onto a :data:`ROOKIE_GAMES_BINS` label.

    Args:
        pick: Expression yielding the overall pick, null for undrafted.

    Returns:
        pl.Expr: The bin label.
    """
    expression = pl.lit(bin_label(None))
    for bounds in ROOKIE_GAMES_BINS:
        if bounds is None:
            continue
        low, high = bounds
        expression = pl.when(pick.is_not_null()
                             & (pick >= low) & (pick <= high)) \
            .then(pl.lit(bin_label(bounds))).otherwise(expression)
    return expression


def _fit_rookie_games(frame: pl.DataFrame, position: str) -> Dict[str, float]:
    """One position's mean rookie games played, per draft-capital bin.

    A bin mean rather than a regression -- see :data:`ROOKIE_GAMES_BINS` for the
    measurement that forced it.

    Args:
        frame: Training rows.
        position: Position to summarise.

    Returns:
        dict: Bin label to mean games. Empty when there are too few rookie rows.
    """
    if "y_games" not in frame.columns:
        return {}
    rows = _rookie_rows(frame, position)
    if rows.height < MIN_ROOKIE_FIT_ROWS:
        return {}

    pick = (pl.col("draft_number").cast(pl.Float64)
            if "draft_number" in rows.columns
            else pl.lit(None, dtype=pl.Float64))
    grouped = (
        rows.with_columns(draft_bin(pick).alias("bin"))
        .group_by("bin")
        .agg(pl.col("y_games").cast(pl.Float64).fill_null(0.0).mean().alias("games"),
             pl.len().alias("n"))
        # A bin with almost nobody in it is noise, and its mean would be applied to
        # every rookie who lands there. Dropped rather than trusted; the fallback is
        # the position's overall rookie mean.
        .filter(pl.col("n") >= 5)
    )
    return {row["bin"]: float(row["games"]) for row in grouped.iter_rows(named=True)}


def rookie_efficiency(frame: pl.DataFrame,
                      positions: Sequence[str] = ft.MODELLED_POSITIONS
                      ) -> Dict[str, Dict[str, float]]:
    """Pooled efficiency rates for rookies, per position.

    Separate from :func:`Scripts.usage.features.positional_baselines`, which pools
    the whole population. A rookie is less efficient per opportunity than an
    established player, and using the pool's rate would overstate every rookie
    projection by that difference.

    Pooled from realised totals, so a rookie with two targets does not weigh as much
    as one with a hundred.

    Args:
        frame: Training rows carrying ``y_tot_<stat>`` outcomes.
        positions: Positions to compute.

    Returns:
        dict: ``{position: {rate_name: value}}``, omitting rates with no volume.
    """
    out: Dict[str, Dict[str, float]] = {}
    for position in positions:
        rows = _rookie_rows(frame, position)
        if rows.is_empty():
            continue
        rates: Dict[str, float] = {}
        for name, numerator, denominator in ft.EFFICIENCY_RATES:
            num, den = f"y_tot_{numerator}", f"y_tot_{denominator}"
            if num not in rows.columns or den not in rows.columns:
                continue
            totals = rows.select(
                pl.col(num).cast(pl.Float64).sum().alias("num"),
                pl.col(den).cast(pl.Float64).sum().alias("den"),
            ).row(0, named=True)
            if totals["den"] and totals["den"] >= MIN_RATE_DENOMINATOR:
                rates[name] = totals["num"] / totals["den"]
        if rates:
            out[position] = rates
    return out


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
            # Both halves of every rate, derived from the rate definitions rather
            # than listed: the numerators are the per-stat MAE outcomes, and the
            # denominators are what `rookie_efficiency` pools over. Hardcoding the
            # numerators alone is what left the rookie arm with no rate to apply.
            *[pl.col(f"tot_{column}").alias(f"y_tot_{column}")
              for column in sorted({c for _, num, den in ft.EFFICIENCY_RATES
                                    for c in (num, den)})
              if f"tot_{column}" in totals.columns],
        )
        frames.append(features.join(outcome, on="gsis_id", how="left"))

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal")


def models_volume(train: pl.DataFrame, position: str, target: str,
                  minimum: float = MIN_MEAN_VOLUME) -> bool:
    """Whether this position gets enough of this volume to be worth modelling.

    The relevance gate. See :data:`MIN_MEAN_VOLUME` for the measured reason it
    exists: without it, a regression fitted on 111 trick plays gave four in five
    receivers a passing line.

    Args:
        train: Training rows.
        position: Position to check.
        target: Volume column.
        minimum: Mean per-game volume required.

    Returns:
        bool: True when the position averages at least ``minimum`` per game.
    """
    outcome = f"y_{target}"
    if outcome not in train.columns:
        return False
    rows = train.filter(pl.col("position") == position)
    if rows.is_empty():
        return False
    mean = rows.select(pl.col(outcome).cast(pl.Float64).fill_null(0.0).mean()).item()
    return mean is not None and mean >= minimum


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
    rookie_vol: Dict[Tuple[str, str], VolumeFit] = {}
    rookie_gms: Dict[str, Dict[str, float]] = {}
    for position in positions:
        for target in VOLUME_TARGETS:
            if not models_volume(train, position, target):
                continue
            fitted = _fit_volume(train, position, target)
            if fitted is not None:
                volume[(position, target)] = fitted
            fitted_rookie = _fit_rookie_volume(train, position, target)
            if fitted_rookie is not None:
                rookie_vol[(position, target)] = fitted_rookie
        fitted_games = _fit_games(train, position)
        if fitted_games is not None:
            games[position] = fitted_games
        fitted_rookie_games = _fit_rookie_games(train, position)
        if fitted_rookie_games:
            rookie_gms[position] = fitted_rookie_games

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
        rookie_volume=rookie_vol,
        rookie_games=rookie_gms,
        rookie_efficiency=rookie_efficiency(train, positions),
        train_seasons=tuple(sorted(train_seasons)),
        fitted_at=fitted_at,
    )
