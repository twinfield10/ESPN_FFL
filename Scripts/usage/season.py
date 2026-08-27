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
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl
from scipy import stats

from Scripts.paths import DATA_DIR
from Scripts.usage import availability as av
from Scripts.usage import context as ctx
from Scripts.usage import features as ft
from Scripts.usage import predictive as pv
from Scripts.usage import role as rl

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

#: ESPN stat -> the ``y_tot_`` column holding what really happened.
#:
#: Derived from :data:`STAT_TERMS` and ``EFFICIENCY_RATES`` rather than listed, so a
#: new stat cannot be added to the model and silently left without an outcome to
#: score its predictive interval against. Each stat's rate names a numerator, and
#: that numerator *is* the outcome: ``yards_per_target = receiving_yards / targets``,
#: so ``receivingYards`` is measured against ``y_tot_receiving_yards``.
STAT_OUTCOMES: Dict[str, str] = {
    stat: f"y_tot_{numerator}"
    for stat, (_, rate) in STAT_TERMS.items()
    for name, numerator, _ in ft.EFFICIENCY_RATES
    if name == rate
}

#: Volume features the model predicts forward, each from its own regression.
VOLUME_TARGETS: Tuple[str, ...] = ("targets_pg", "carries_pg", "pass_attempts_pg")

#: Regressors for a volume model. Lagged volume plus the two context flags a
#: drafter can see pre-season.
#: ``age`` is the player's age at the season opener, and it is the one regressor here
#: that is a *current-season* fact rather than a lag -- a birth date does not move, so
#: 2026's age is knowable in 2026.
#:
#: Measured over 8,763 player-season pairs, predicting next-season volume beyond prior
#: volume and games: **WR targets 0.5462 -> 0.5645, RB carries 0.5114 -> 0.5211, TE
#: targets 0.5554 -> 0.5603**. Linear, not a curve: adding ``age^2`` moved every one of
#: those by less than 0.0003, so the quadratic the football-analytics literature likes
#: buys nothing over this population and this horizon.
VOLUME_REGRESSORS: Tuple[str, ...] = (
    "p1_volume", "p2_volume", "p1_games", "team_changed", "age",
    "depth_rank", "is_first_string",
)

#: The **coach prior** is not here, and that is a measured decision rather than an
#: omission. The **depth chart is**, as of 2026-08-07, and an earlier revision of this
#: note wrongly said otherwise -- it claimed both had been measured out when only the
#: coach prior had been tested. See :data:`VETERAN_SITUATIONAL_REJECTED`.
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
#:
#: **The depth chart is a different story and was never actually tested here.** The
#: prose above used to say "the coach prior and depth chart", but the experiment it
#: describes varied only the two names in this tuple. Measured properly on RB carries,
#: train 2020-2023 and test 2024-2025: prior carries + games + age gives R-squared
#: 0.5584, and adding ``depth_rank`` and ``is_first_string`` gives **0.6066**. On the
#: share of a team's carries -- which the team-then-allocate experiment identified as
#: the real bottleneck -- it moves 0.5193 to **0.5803**. It is in
#: :data:`VOLUME_REGRESSORS` now.
VETERAN_SITUATIONAL_REJECTED: Tuple[str, ...] = ("coach_volume", "staff_continuity")

#: Why ``peak3_volume`` is **defined and switched off**, beside ``mean3_volume`` and
#: ``peak5_volume``.
#:
#: Plan 32 predicted a quarterback-only feature. It shipped briefly as a feature for
#: every position on the strength of a pooled walk-forward MAE gain -- QB +1.8%, RB
#: +1.0%, WR +1.0%, TE +0.7%, and 6 of 6 folds at quarterback. **Re-measured on
#: 2026-08-26, the quarterback half of that does not reproduce.**
#:
#: ==========  ==========  ==========
#: position     claimed     re-measured
#: ==========  ==========  ==========
#: QB           +1.8%       **+0.34%**  (4/6 folds)
#: QB movers    +4.4%       **+0.03%**  (bar was +3%)
#: RB           +1.0%       +0.94%
#: WR           +1.0%       +1.28%
#: TE           +0.7%       +0.59%
#: ==========  ==========  ==========
#:
#: The skill positions reproduce closely and quarterback does not, which is the
#: opposite of what the plan and its correction both said. Measured at the original
#: commit in a detached worktree, the numbers are identical fold for fold, so this is
#: not drift: the claim was never reproducible from this code. The **fit** does improve
#: at quarterback -- R-squared 0.4503 to 0.4698, ``peak3_volume`` entering at +0.33 and
#: outweighing ``p1_volume`` -- so the window explains more variance there without that
#: converting into out-of-sample accuracy.
#:
#: **G-M1 was the affirmative gate and it fails**, so nothing turns on. G-M2 was a guard
#: against regression elsewhere, not a reason to ship, and the ~1% it now measures at
#: RB, WR and TE is real but needs a gate of its own before it earns a refit. Turning
#: this back on is one line here plus a ``MODEL_VERSION`` bump.

#: Depth rank used where the chart lists nobody, and the reason it is not zero.
#:
#: ``depth_rank`` is ordinal and *ascending* -- 1 is the starter -- so the zero that
#: :func:`_veteran_terms`'s ``column`` helper fills with would rank an unlisted player
#: ahead of every listed one. The chart's own catch-all bucket is 3 (589 of 909 rows
#: on the 2026 pull), which is what "not named as a starter or a backup" means.
DEFAULT_DEPTH_RANK: float = 3.0

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
#:
#: **The head works in share-of-slate, not in games** (v1.1.0). Both sides are
#: divided by the number of games that season actually offered: the target is
#: ``y_games / y_games_available`` and the regressor is ``p1_availability``, with
#: :meth:`SeasonUsageModel.expected_games` multiplying back up by the slate being
#: projected.
#:
#: The reason is a measured bias. The NFL went from 16 games to 17 in 2021, and 45%
#: of the training rows predate that. Fitted in raw games, the head learns a blend of
#: the two eras and projects a 17-game season too low: among players who had managed
#: 16+ the previous year, next-season games average **13.06** in the 16-game era
#: against **13.64** in the 17-game era, so a 2026 projection inherited roughly a
#: quarter-game of downward bias for no reason other than which seasons happened to
#: be in the sample. In share terms those two eras agree, which is what makes the
#: normalisation the fix rather than a fudge.
#:
#: An earlier revision recorded ``p1_availability`` as "deliberately absent" because
#: fitting it *alongside* ``p1_games`` gave offsetting nonsense -- RB +1.056 on games
#: against −10.160 on availability, QB +0.052 against +8.620. That objection was
#: about collinearity between the two, and it still holds: availability is used here
#: **instead of** raw games, never with it. The same note also observed that the two
#: are collinear "up to the 16-to-17-game schedule change" -- which named this bias
#: exactly, since that residual difference is the whole of it.
#:
#: ``p1_weeks_on_reserve`` stays in games rather than share: it is a count of weeks
#: unavailable, not an appearance rate, so it is not mechanically tied to the slate.
#: It separates "hurt" from "healthy and benched" and correlates −0.462 with next
#: season's games played.
#: ``p1_snap_share`` is role security rather than durability, and it is the largest
#: single gain available to this head: over 1,605 player-season pairs, predicting next
#: season's games played, prior games alone gives R-squared 0.203 and adding snap
#: share gives **0.230**. A player at 85% of snaps is entrenched; one at 25% is a
#: depth-chart move from inactive, and being inactive is most of what "games played"
#: measures once a player is on a roster. Injury-report features were measured on top
#: of it and added +0.003, so they are not here -- see plan 18 §Snap share.
#: ``age`` earns a place here too, and separately: predicting next-season games,
#: R-squared goes 0.1982 to **0.2100** on top of snap share. Mean games played falls
#: 11.36 at 21-24 to 10.10 at 29-30, which is the durability signal prior games alone
#: was never going to carry.
GAMES_REGRESSORS: Tuple[str, ...] = ("p1_availability", "p1_weeks_on_reserve",
                                     "p1_snap_share", "team_changed", "age")

#: Games the season being projected offers, when the data cannot say.
#:
#: The head predicts a *share* of the slate, so something has to supply the slate.
#: For a past season it is measured from ``player_weeks``; for the season being
#: drafted it cannot be, because nflreadr serves no future schedule. 17 has been the
#: slate since 2021.
DEFAULT_TARGET_SLATE: float = 17.0

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

#: Age used where none is known, in years.
#:
#: The pool mean, 26.2 rounded. Only reachable when the roster pull carries no birth
#: date at all -- a per-player gap falls back to the position's median first. It is a
#: constant rather than a null on purpose: the fits drop null regressor rows, so a
#: null here would empty a whole fit instead of disabling one term.
DEFAULT_AGE: float = 26.0

#: Most recent training seasons held out to fit the predictive dispersions.
#:
#: Two rather than one for row count: a single season leaves the thinner
#: (position, stat) pairs below :data:`Scripts.usage.predictive.MIN_FIT_ROWS`, and a
#: pair with no fit gets no interval at all.
HOLDOUT_SEASONS: int = 2

#: Model version, bumped when the structure changes rather than when it is refitted.
#:
#: 1.1.0 -- the two expected-games heads predict a share of the slate rather than a
#: count of games, removing the 16-to-17-game bias described in
#: :data:`GAMES_REGRESSORS`. The coefficients are on a different scale from 1.0.0's
#: and are not interchangeable with them, which is what the bump is for.
#:
#: 1.2.0 -- two new blocks, both fitted on the same held-out residuals the dispersions
#: already use and neither of which moves a projected mean: ``stat_dispersion_conditional``
#: (the spread *given* games played) and ``stat_correlation`` (how the eight stats'
#: residuals move together). Plan 28 needs both, and 1.1.0 files load fine without them
#: -- the fields default empty and every caller falls back explicitly.
#:
MODEL_VERSION = "1.2.0"

#: Positions the season head declines to project, whatever features it has for them.
#:
#: **Empty as of 2026-08-07.** It held ``"QB"`` because the walk-forward measured the
#: model as making quarterback ordering *worse* than the naive draft heuristic. That
#: deficit closed as the model improved, and the depth chart closed it decisively:
#:
#: ===============================  =========
#: original                          -0.0155
#: share-of-slate games head         -0.0153
#: + snap share                      -0.0119
#: + age                             -0.0115
#: + depth chart on veterans         **+0.0132**
#: ===============================  =========
#:
#: Not one metric turning over, either: QB top-12 hit rate goes 0.607 to 0.631 against
#: the baseline's 0.619, and all three passing MAEs flip from losing to the naive
#: heuristic to beating it by 7-17%. The reason is obvious in hindsight -- being the
#: listed starter is enormously predictive of pass attempts, and prior-season volume
#: alone cannot see a backup who has won the job.
#:
#: Kept as a mechanism rather than deleted: it is how a position gets declined on
#: evidence, and the next arm that measures worse should use it.
#:
#: :mod:`Scripts.usage.backtest` passes ``()`` explicitly, so its table keeps measuring
#: every arm including any that shipped code declines.
ABSTAIN_POSITIONS: Tuple[str, ...] = ()


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
    #: Beta-Binomial concentration for games played, per position. Absent for a
    #: position whose residuals are not overdispersed or too few to fit.
    games_dispersion: Dict[str, float] = field(default_factory=dict)
    #: Mean-variance coefficients per ``"<position>|<stat>"``, as
    #: ``{"phi": ..., "k": ...}`` for ``Var = phi * mu + mu^2 / k``. See
    #: :mod:`Scripts.usage.predictive`.
    stat_dispersion: Dict[str, Dict[str, float]] = field(default_factory=dict)
    #: The same coefficients, fitted **conditional on games played**.
    #:
    #: :attr:`stat_dispersion` is fitted against ``USG_<stat>``, which already carries
    #: ``expected_games`` inside it, so its spread contains the availability variance.
    #: That is the right object for "what will this player actually score"; it is the
    #: wrong one for two other questions the repo asks. An if-healthy interval built by
    #: rescaling it (:func:`Scripts.usage.project.to_full_slate`) carries availability
    #: variance around a mean that has availability divided out, and a simulation that
    #: draws games explicitly and then applies it counts availability **twice**.
    #:
    #: Fitted against ``USG_<stat> * y_games / mu_games`` -- the projection re-based onto
    #: the games the player really had -- this is ``Var(total | games)``. Evaluated at a
    #: full slate it is the if-healthy spread; mixed over the Beta-Binomial it rebuilds
    #: the unconditional one. See ``docs/plans/28-outcome-distributions.md``.
    stat_dispersion_conditional: Dict[str, Dict[str, float]] = field(
        default_factory=dict)
    #: Per ``"<position>|<stat>"``, how much of ``expected_games`` is availability
    #: rather than role -- the exponent in ``(games / expected_games) ** e``. Fitted at
    #: 0.32-0.49; assuming the 1.0 it looks like it should be over-projects a realised
    #: total by up to 27%. See :func:`_conditional_mean`.
    games_elasticity: Dict[str, float] = field(default_factory=dict)
    #: Residual correlation across stats, per position, as
    #: ``{position: {"stats": [...], "matrix": [[...]]}}``.
    #:
    #: The dispersions above describe each stat's spread on its own, and a season points
    #: total is a **sum** of them -- so summing independent marginals understates the
    #: spread by exactly the covariance. A player who beats his target projection beats
    #: his receiving-yard projection too; those are one event, not two.
    #:
    #: Stored as normal-score (PIT) correlations so a Gaussian copula reproduces the
    #: fitted marginals exactly rather than approximately.
    stat_correlation: Dict[str, Dict[str, object]] = field(default_factory=dict)
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

    def rookie_expected_games(
            self, frame: pl.DataFrame,
            target_slate: float = DEFAULT_TARGET_SLATE) -> pl.Expr:
        """Games played for a rookie, from his draft-capital bin's mean.

        The bins hold a **share of the slate**, so the share is multiplied back up
        by the season being projected -- the same normalisation the veteran head
        uses, and for the same measured reason.

        Args:
            frame: Feature frame carrying ``position`` and ``draft_number``.
            target_slate: Games the projected season offers.

        Returns:
            pl.Expr: Expected games, null for a position with no rookie table.
        """
        slate = float(target_slate) if target_slate else DEFAULT_TARGET_SLATE
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
        return expression.clip(lower_bound=0.0, upper_bound=1.0) * slate

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

    def expected_games(self, frame: pl.DataFrame,
                       target_slate: float = DEFAULT_TARGET_SLATE) -> pl.Expr:
        """Games played, predicted from the player's own prior availability.

        The head predicts a **share of the slate** (see :data:`GAMES_REGRESSORS`),
        so the share is multiplied back up by the games the projected season offers.
        Fitting in raw games instead let the 16-game seasons in the training range
        pull a 17-game projection down by about a quarter of a game.

        Falls back to the position's mean share where a position has no fit, and to
        the overall mean where it has neither -- both of which are only reachable for
        a position with almost no training rows.

        Args:
            frame: Feature frame from :func:`Scripts.usage.features.season_features`.
            target_slate: Games the projected season offers. Defaults to
                :data:`DEFAULT_TARGET_SLATE`; pass the measured slate when
                backtesting a season that really had a different one.

        Returns:
            pl.Expr: Expected games played, clipped to a plausible slate.
        """
        # `games_by_position` is stored in games, so the fallback converts to a share
        # before the multiply below turns it back. Storing it in games keeps the
        # printed summary readable and comparable with the earlier version.
        slate = float(target_slate) if target_slate else DEFAULT_TARGET_SLATE
        fallback = float(np.mean(list(self.games_by_position.values()))
                         if self.games_by_position else 16.0)
        shares = {position: games / slate
                  for position, games in self.games_by_position.items()}
        expression = pl.col("position").replace_strict(
            shares, default=fallback / slate, return_dtype=pl.Float64)

        values = {
            "p1_availability": pl.col(f"{ft.LAG1_PREFIX}availability")
            .cast(pl.Float64),
            "p1_weeks_on_reserve": pl.col(f"{ft.LAG1_PREFIX}weeks_on_reserve")
            .cast(pl.Float64),
            "p1_snap_share": (
                pl.col(f"{ft.LAG1_PREFIX}snap_share").cast(pl.Float64)
                .fill_null(pl.col(f"{ft.LAG1_PREFIX}availability").cast(pl.Float64))
                if f"{ft.LAG1_PREFIX}snap_share" in frame.columns
                else pl.col(f"{ft.LAG1_PREFIX}availability").cast(pl.Float64)),
            "team_changed": pl.col("team_changed").cast(pl.Float64),
            "age": age_expr(frame),
        }
        for position, fit in self.games.items():
            terms = pl.lit(fit.intercept)
            for name, coefficient in fit.coefficients.items():
                if name in values:
                    terms = terms + values[name].fill_null(0.0) * coefficient
            expression = pl.when(pl.col("position") == position).then(
                terms).otherwise(expression)

        # A player cannot play more than the slate, and a negative prediction is not
        # a prediction. The share is clipped before the multiply so the bound holds
        # whatever slate is passed; 18 rather than 17 because a team's weeks, not its
        # games, is what the feature counts when a season runs long -- a traded
        # player really can appear in all 18.
        return (expression.clip(lower_bound=0.0, upper_bound=1.0) * slate).clip(
            lower_bound=0.0, upper_bound=18.0)

    def dispersion_for(self, positions: Sequence[str]) -> np.ndarray:
        """Per-row concentration, falling back where a position has no fit.

        Args:
            positions: One position per row.

        Returns:
            np.ndarray: Concentration per row.
        """
        return np.array([self.games_dispersion.get(p, av.DEFAULT_KAPPA)
                         for p in positions], dtype=float)

    def games_interval(self, frame: pl.DataFrame,
                       target_slate: float = DEFAULT_TARGET_SLATE,
                       lower: float = 0.1,
                       upper: float = 0.9) -> pl.DataFrame:
        """Attach the predictive distribution of games played around the mean.

        The point estimate is the weakest thing this model reports -- R-squared 0.19,
        and prior games predict next season at r = +0.343 among players who managed
        8+. Reporting it alone invites it being read as a forecast. This adds the
        second moment and an interval, computed in closed form from the
        Beta-Binomial (:mod:`Scripts.usage.availability`) rather than by simulation.

        The distribution is strongly left-skewed -- most players are fine and a few
        miss most of the year -- so the interval is deliberately asymmetric around
        the mean, and a normal approximation would be wrong in exactly the tail a
        drafter cares about.

        Args:
            frame: Output of :meth:`predict`, carrying ``expected_games`` and
                ``position``.
            target_slate: Games the projected season offers.
            lower: Lower quantile.
            upper: Upper quantile.

        Returns:
            pl.DataFrame: ``frame`` plus ``games_sd``, ``games_low``, ``games_high``
            and ``games_implied_coverage``. Null wherever ``expected_games`` is.
        """
        slate = float(target_slate) if target_slate else DEFAULT_TARGET_SLATE
        n = int(round(slate))
        expected = frame["expected_games"].cast(pl.Float64).to_numpy()
        kappa = self.dispersion_for(frame["position"].to_list())

        known = np.isfinite(expected)
        mu = np.where(known, expected / slate, 0.5)

        sd = np.full(expected.shape, np.nan)
        low = np.full(expected.shape, np.nan)
        high = np.full(expected.shape, np.nan)
        implied = np.full(expected.shape, np.nan)

        # Grouped by concentration so the vectorised PMF runs once per distinct
        # value rather than once per player -- there are at most four.
        for value in np.unique(kappa):
            rows = known & (kappa == value)
            if not rows.any():
                continue
            _, variance = av.moments(slate, mu[rows], float(value))
            sd[rows] = np.sqrt(variance)

            cdf = np.cumsum(av.pmf(n, mu[rows], float(value)), axis=1)
            lo = (cdf < lower).sum(axis=1)
            hi = (cdf < upper).sum(axis=1)
            low[rows] = lo
            high[rows] = hi

            # What the model actually claims for the integers it picked, which is
            # more than `upper - lower`: a discrete support cannot be cut at exactly
            # 10% and 90%, so the realised band is always wider than nominal. Carried
            # so the backtest can judge coverage against the claim rather than
            # against a target the family cannot express.
            index = np.arange(lo.size)
            below = np.where(lo > 0, cdf[index, np.maximum(lo - 1, 0)], 0.0)
            above = 1.0 - cdf[index, hi]
            implied[rows] = 1.0 - below - above

        # NaN -> null throughout; see the note in `stat_intervals`. A NaN here
        # survives `is_not_null()` and then fails every comparison silently.
        return frame.with_columns(
            pl.Series("games_sd", sd).fill_nan(None),
            pl.Series("games_low", low).fill_nan(None),
            pl.Series("games_high", high).fill_nan(None),
            pl.Series("games_implied_coverage", implied).fill_nan(None),
        )

    def stat_intervals(self, frame: pl.DataFrame,
                       lower: float = 0.1,
                       upper: float = 0.9) -> pl.DataFrame:
        """Attach a predictive interval to each projected stat line.

        Negative Binomial for counts, Gamma for yardage, with the dispersion fitted
        per (position, stat) on the model's own residuals -- see
        :mod:`Scripts.usage.predictive` for why the dispersion is fitted end-to-end
        rather than composed from the games, volume and rate variances that the model
        decomposes into. Both families have closed-form quantiles.

        A stat with no fitted dispersion for its position gets no interval rather
        than a pooled one. Partial coverage is visible; an invented number is not.
        The same rule now withdraws a pair whose interval is *measured* not to cover
        -- :data:`Scripts.usage.predictive.UNCALIBRATED`, which holds
        ``QB|passingYards`` at 58.9% against a nominal 80%.

        Args:
            frame: Output of :meth:`predict`.
            lower: Lower quantile.
            upper: Upper quantile.

        Returns:
            pl.DataFrame: ``frame`` plus ``USG_<stat>_sd``, ``USG_<stat>_low`` and
            ``USG_<stat>_high`` for every stat with a fitted dispersion.
        """
        positions = frame["position"].to_list()
        columns = []

        for stat in STAT_TERMS:
            projected = f"{USAGE_PREFIX}{stat}"
            if projected not in frame.columns or pv.family_for(stat) is None:
                continue

            mu = frame[projected].cast(pl.Float64).to_numpy()
            fits = [self.stat_dispersion.get(pv.key(p, stat)) for p in positions]
            has_fit = np.array([f is not None for f in fits])

            sd = np.full(mu.shape, np.nan)
            low = np.full(mu.shape, np.nan)
            high = np.full(mu.shape, np.nan)

            # A pair whose interval is measured not to cover is withdrawn here, the
            # same way a pair with no fitted dispersion is: see
            # `predictive.UNCALIBRATED`. QB passing yards covers 58.9% against a
            # nominal 80% and the Gamma has the skew inverted, so there is no
            # dispersion that fixes it -- publishing a narrower or wider one would
            # still be publishing a false claim.
            calibrated = np.array([pv.is_calibrated(p, stat) for p in positions])
            usable = np.isfinite(mu) & has_fit & calibrated & (mu > 0)
            # One call per position rather than per player, since the coefficients
            # vary only by position.
            for position in {p for p, keep in zip(positions, usable) if keep}:
                rows = usable & np.array([p == position for p in positions])
                if not rows.any():
                    continue
                coefficients = self.stat_dispersion[pv.key(position, stat)]
                phi, k = coefficients["phi"], coefficients["k"]
                bust = coefficients.get("bust", 0.0)
                _, variance = pv.moments(stat, mu[rows], phi, k)
                sd[rows] = np.sqrt(variance)
                low[rows] = pv.quantile(stat, mu[rows], phi, k, lower, bust=bust)
                high[rows] = pv.quantile(stat, mu[rows], phi, k, upper, bust=bust)

            # NaN -> null, and the distinction is load-bearing. Polars treats them as
            # different things and `is_not_null()` is True for a NaN, so a float NaN
            # left in place reads downstream as a real value: it survives every
            # null filter and then compares False against everything, which scored
            # abstained players as outside their own interval and put measured
            # coverage at 6%. The same "absent reads as present" failure this repo
            # has now paid for three times, in its float-shaped disguise.
            columns += [pl.Series(f"{projected}_sd", sd).fill_nan(None),
                        pl.Series(f"{projected}_low", low).fill_nan(None),
                        pl.Series(f"{projected}_high", high).fill_nan(None)]

        return frame.with_columns(columns) if columns else frame

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
            # Not `column()`: a zero age is not a neutral value the way a zero
            # team-change flag is, and filling one would put every unknown player at
            # the far end of the decline curve.
            "age": age_expr(frame),
            # Same trap in the other direction -- rank 0 would be better than rank 1.
            "depth_rank": depth_rank_expr(frame),
            "is_first_string": column("is_first_string"),
            "coach_volume": column(coach_column) if coach_column else pl.lit(0.0),
            "staff_continuity": column("staff_continuity"),
            # --- plan 22 candidates ---------------------------------------
            # Present unconditionally and zero when their column is absent, exactly
            # like the coach prior above. That is what lets an experiment be a
            # one-line change to VOLUME_REGRESSORS rather than a branch through the
            # fit and the predict paths, which would be two places to get wrong.
            "p1_route_share": column(f"{ft.LAG1_PREFIX}route_share"),
            "p1_routes_pg": column(f"{ft.LAG1_PREFIX}routes_pg"),
            "p1_rz10_carry_share": column(f"{ft.LAG1_PREFIX}rz10_carry_share"),
            "p1_rz5_carry_share": column(f"{ft.LAG1_PREFIX}rz5_carry_share"),
            "p1_rz10_target_share": column(f"{ft.LAG1_PREFIX}rz10_target_share"),
            "p1_ez_target_share": column(f"{ft.LAG1_PREFIX}ez_target_share"),
            # Contracts enter **interacted with team_changed**, and the interaction
            # is the finding rather than a modelling flourish. A main effect was
            # measured first and is a wash or worse on the full population -- a
            # settled veteran's own prior volume already encodes what his team
            # thinks of him. On movers, where prior volume describes a job he no
            # longer has, the same columns are positive at all four positions. See
            # :func:`Scripts.usage.features.contract_context`.
            "moved_contract_apy": column("team_changed") * column("contract_apy_pct"),
            "moved_contract_gtd": column("team_changed") * column("contract_guaranteed"),
            "moved_contract_new": column("team_changed") * column("contract_is_new"),
            # --- plan 32 phase 1: the window, all three off -----------------
            # Two lags cannot separate "lost the job for eight weeks" from "is not
            # a starter" -- both are a low `p1_volume`, and a peak over three
            # seasons can. That mechanism is real and the fit agrees with it; what
            # does not follow is out-of-sample accuracy at quarterback. **None of
            # the three is in `VOLUME_REGRESSORS`** -- see the note on
            # `peak3_volume` there for the re-measurement that switched it off.
            # They stay defined because the feature layer is leak-guarded and
            # tested, and turning one on is a line there plus a version bump.
            "peak3_volume": column(f"{ft.PEAK_PREFIXES[3]}{target}"),
            "mean3_volume": column(f"{ft.MEAN3_PREFIX}{target}"),
            "peak5_volume": column(f"{ft.PEAK_PREFIXES[5]}{target}"),
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

    def predict(self, frame: pl.DataFrame, rookies: bool = True,
                abstain_positions: Optional[Sequence[str]] = None,
                target_slate: float = DEFAULT_TARGET_SLATE) -> pl.DataFrame:
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
            abstain_positions: Positions to decline entirely. Defaults to
                :data:`ABSTAIN_POSITIONS`. Pass ``()`` to project every position,
                which is what the backtest needs in order to keep measuring the
                quarterback arm that default exists because of.
            target_slate: Games the projected season offers. Both games heads
                predict a share of the slate, so this converts back to games.
                Defaults to :data:`DEFAULT_TARGET_SLATE`; the backtest passes each
                fold's measured slate so a 16-game season is scored as one.

        Returns:
            pl.DataFrame: ``frame`` plus ``expected_games``, ``pred_<volume>`` per
            :data:`VOLUME_TARGETS`, ``usg_arm`` naming which arm spoke, and
            ``USG_<stat>`` per :data:`STAT_TERMS`. The ``USG_`` columns are null
            wherever the model abstains.
        """
        if abstain_positions is None:
            abstain_positions = ABSTAIN_POSITIONS

        is_rookie = (pl.col("is_rookie").fill_null(False)
                     if "is_rookie" in frame.columns else pl.lit(False))
        has_history = pl.col(f"{ft.LAG1_PREFIX}games").is_not_null()
        # A rookie is a player with no prior season, and the flag has to agree with
        # that or the two arms would both claim the same row. Prior history wins:
        # a "rookie" with a prior season is a data problem, not a rookie.
        use_rookie = pl.lit(rookies) & is_rookie & ~has_history

        declined = (pl.col("position").is_in(list(abstain_positions))
                    if abstain_positions else pl.lit(False))

        out = frame.with_columns(
            pl.when(use_rookie)
            .then(self.rookie_expected_games(frame, target_slate=target_slate))
            .otherwise(self.expected_games(frame, target_slate=target_slate))
            .clip(lower_bound=0.0, upper_bound=18.0)
            .alias("expected_games"),
            # A declined position is an abstention like any other, so it reaches the
            # blend down the path plan 07 already built for a wholly absent source
            # rather than needing one of its own.
            pl.when(declined).then(pl.lit("abstain"))
            .when(use_rookie).then(pl.lit("rookie"))
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
            "  expected games, as a share of the slate (fitted, not a constant)",
            f"  {'position':<10}{'n':>7}{'const':>9}{'p1_avail':>10}"
            f"{'reserve':>10}{'moved':>8}{'R2':>8}{'@17gms':>9}",
        ]
        for position, fit in sorted(self.games.items()):
            c = fit.coefficients
            # What a fully-available player gets, in games, which is the number
            # anyone reading this table actually wants.
            full = ((fit.intercept + c.get("p1_availability", 0.0))
                    * DEFAULT_TARGET_SLATE)
            lines.append(
                f"  {position:<10}{fit.n:>7}{fit.intercept:>9.3f}"
                f"{c.get('p1_availability', 0):>10.3f}"
                f"{c.get('p1_weeks_on_reserve', 0):>10.3f}"
                f"{c.get('team_changed', 0):>8.3f}{fit.r2:>8.4f}{full:>9.2f}")
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
            lines += ["", f"  rookie games played by draft-capital bin, shown at a "
                          f"{DEFAULT_TARGET_SLATE:.0f}-game slate"]
            bins = [bin_label(b) for b in ROOKIE_GAMES_BINS]
            lines.append("  " + f"{'position':<10}"
                         + "".join(f"{name:>12}" for name in bins))
            for position, by_bin in sorted(self.rookie_games.items()):
                # Stored as a share; rendered in games, which is what the plan's
                # table records and what a reader can sanity-check.
                cells = "".join(
                    (f"{by_bin[name] * DEFAULT_TARGET_SLATE:>12.1f}"
                     if name in by_bin else f"{'—':>12}")
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
            "games_dispersion": self.games_dispersion,
            "stat_dispersion": self.stat_dispersion,
            "games_elasticity": self.games_elasticity,
            "stat_dispersion_conditional": self.stat_dispersion_conditional,
            "stat_correlation": self.stat_correlation,
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

    @classmethod
    def default_path(cls, version: Optional[str] = None):
        """Where :meth:`save` writes by default.

        Args:
            version: Model version. Defaults to :data:`MODEL_VERSION`.

        Returns:
            Path: ``Data/NFL/models/season_usage_<version>.json``.
        """
        return (DATA_DIR / "NFL" / "models"
                / f"season_usage_{version or MODEL_VERSION}.json")

    @classmethod
    def load(cls, path=None) -> "SeasonUsageModel":
        """Read back what :meth:`save` wrote.

        The inverse of :meth:`to_dict`. Without this the coefficients persisted
        but nothing could read them, so every caller wanting a projection had to
        re-run :func:`fit` over the full training frame -- around a minute of work
        to reproduce a file that already existed, and one more chance for the
        board and the backtest to be built from different coefficients.

        Args:
            path: Source file. Defaults to :meth:`default_path`.

        Returns:
            SeasonUsageModel: The fitted model.

        Raises:
            FileNotFoundError: If ``path`` does not exist.
        """
        path = cls.default_path() if path is None else Path(path)
        payload = json.loads(path.read_text())

        def fits(key: str) -> Dict[Tuple[str, str], VolumeFit]:
            return {(f["position"], f["target"]): VolumeFit(**f)
                    for f in payload.get(key, [])}

        return cls(
            volume=fits("volume_fits"),
            games={f["position"]: VolumeFit(**f)
                   for f in payload.get("games_fits", [])},
            games_by_position=payload.get("games_by_position", {}),
            rookie_volume=fits("rookie_volume_fits"),
            rookie_games=payload.get("rookie_games_by_bin", {}),
            rookie_efficiency=payload.get("rookie_efficiency", {}),
            games_dispersion=payload.get("games_dispersion", {}),
            stat_dispersion=payload.get("stat_dispersion", {}),
            # Absent from every 1.1.0 file. Empty means "not fitted", and each reader
            # falls back explicitly rather than inventing a dispersion.
            games_elasticity=payload.get("games_elasticity", {}),
            stat_dispersion_conditional=payload.get(
                "stat_dispersion_conditional", {}),
            stat_correlation=payload.get("stat_correlation", {}),
            train_seasons=tuple(payload.get("train_seasons", ())),
            version=payload.get("version", MODEL_VERSION),
            fitted_at=payload.get("fitted_at"),
        )



def age_expr(frame: pl.DataFrame) -> pl.Expr:
    """Age at the season opener, with a positional fallback.

    A missing birth date is filled with the position's median age rather than
    dropping the row. Coverage is 98.6% on the 2026 roster, but the 1.4% are not a
    random sample -- they are the players nflverse knows least about, which skews
    toward the fringe roster spots a draft board is least sure of anyway. Losing them
    from the fit would be a quiet selection effect.

    Args:
        frame: Feature frame, with ``position``.

    Returns:
        pl.Expr: Age in years, never null where any age is known for the position.
    """
    if "age" not in frame.columns:
        # A constant, never a null. The fits `drop_nulls()` their regressor block, so
        # a null age would empty the entire volume fit rather than merely disable one
        # term -- which is what a frame built before this feature existed, or by a
        # caller that never pulled rosters, would produce. A constant is collinear
        # with the intercept and therefore contributes nothing, which is the intended
        # "this arm does not use age" behaviour.
        # `pl.repeat` rather than `pl.lit`, so the expression is the frame's height in
        # a bare `select` as well as in `with_columns`. A length-1 literal silently
        # truncates a select to one row, which is the kind of thing that works
        # everywhere it is currently called from and breaks the first time it is not.
        return pl.repeat(DEFAULT_AGE, pl.len(), dtype=pl.Float64)
    age = pl.col("age").cast(pl.Float64)
    return (age.fill_null(age.median().over("position"))
            .fill_null(age.median())
            .fill_null(DEFAULT_AGE))



def depth_rank_expr(frame: pl.DataFrame) -> pl.Expr:
    """Depth-chart rank, with the catch-all bucket where the chart says nothing.

    The rank is ascending and ordinal: 1 is the starter. So the zero that a generic
    missing-column fill would use ranks an unlisted player *ahead* of every listed
    one, which is the opposite of what a missing entry means.

    Args:
        frame: Feature frame.

    Returns:
        pl.Expr: Depth rank, never null.
    """
    if "depth_rank" not in frame.columns:
        return pl.repeat(DEFAULT_DEPTH_RANK, pl.len(), dtype=pl.Float64)
    return pl.col("depth_rank").cast(pl.Float64).fill_null(DEFAULT_DEPTH_RANK)


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


#: Ridge penalty on the volume and games heads. Zero is ordinary least squares.
#:
#: A knob rather than a decision, and it defaults to off. Plan 22 tested whether the
#: functional form was leaving anything on the table -- n runs 450 to 1,500 per
#: position-target against seven regressors, which is comfortable for OLS, so the
#: expectation was no. See ``docs/model_lab.html``.
RIDGE_ALPHA: float = 0.0


def _ridge(design: np.ndarray, y: np.ndarray, alpha: float) -> np.ndarray:
    """Ridge regression, standardised, without penalising the intercept.

    Standardising is not optional here. The regressors are on wildly different
    scales -- ``p1_targets_pg`` around 5, ``age`` around 27, ``depth_rank`` 1 to 3 --
    and an unstandardised penalty would shrink the small-scale coefficients hardest
    for no reason but their units. The intercept is excluded because penalising it
    pulls every prediction toward zero rather than toward the mean.

    Args:
        design: Column of ones followed by the regressors.
        y: Outcome.
        alpha: Penalty strength, in standardised units.

    Returns:
        np.ndarray: Coefficients on the original scale, intercept first.
    """
    x = design[:, 1:]
    centre, scale = x.mean(axis=0), x.std(axis=0)
    scale = np.where(scale == 0, 1.0, scale)
    z = (x - centre) / scale

    penalty = alpha * np.eye(z.shape[1])
    beta_z = np.linalg.solve(z.T @ z + penalty, z.T @ (y - y.mean()))

    beta = beta_z / scale
    return np.concatenate([[y.mean() - float(centre @ beta)], beta])


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

    if RIDGE_ALPHA > 0:
        beta = _ridge(design, y, RIDGE_ALPHA)
    else:
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

    **Fitted in share of slate, both sides.** The outcome is ``y_games /
    y_games_available`` and the regressor is ``p1_availability``, so a player who
    made all 16 in 2019 and one who made all 17 in 2023 are the same observation
    rather than two different ones. See :data:`GAMES_REGRESSORS` for the measured
    bias this removes.

    Args:
        frame: Training rows carrying ``p1_availability``, ``y_games`` and
            ``y_games_available``.
        position: Position to fit.

    Returns:
        VolumeFit | None: None below :data:`MIN_FIT_ROWS`.
    """
    lag_availability = f"{ft.LAG1_PREFIX}availability"
    lag_reserve = f"{ft.LAG1_PREFIX}weeks_on_reserve"
    lag_snaps = f"{ft.LAG1_PREFIX}snap_share"
    if any(c not in frame.columns
           for c in (lag_availability, "y_games", "y_games_available")):
        return None

    rows = frame.filter(
        (pl.col("position") == position)
        & pl.col("y_games").is_not_null()
        & pl.col(lag_availability).is_not_null()
        # A zero slate would divide the outcome by nothing. It cannot arise from
        # `team_games`, which counts weeks a team really appeared, but the guard is
        # cheaper than the silent infinity it prevents.
        & (pl.col("y_games_available").cast(pl.Float64) > 0)
    ).select(
        pl.col(lag_availability).cast(pl.Float64).alias("p1_availability"),
        (pl.col(lag_reserve).cast(pl.Float64).fill_null(0.0)
         if lag_reserve in frame.columns
         else pl.lit(0.0)).alias("p1_weeks_on_reserve"),
        # Snap counts are an in-season pull and the early seasons resolve worse, so a
        # null is filled with the row's own availability rather than dropped: a player
        # with no snap record is not a player with no role, and dropping him would
        # shrink the fit to the seasons the crosswalk happens to cover well.
        (pl.col(lag_snaps).cast(pl.Float64)
         .fill_null(pl.col(lag_availability).cast(pl.Float64))
         if lag_snaps in frame.columns
         else pl.col(lag_availability).cast(pl.Float64)).alias("p1_snap_share"),
        pl.col("team_changed").cast(pl.Float64).fill_null(0.0).alias("team_changed"),
        age_expr(frame).alias("age"),
        (pl.col("y_games").cast(pl.Float64)
         / pl.col("y_games_available").cast(pl.Float64)).alias("y"),
    ).drop_nulls()

    return _least_squares(rows, GAMES_REGRESSORS, position, "games")


def _fit_stat_dispersion(holdout: pl.DataFrame,
                         positions: Sequence[str]) -> Dict[str, float]:
    """Predictive dispersion per (position, stat), from held-out residuals.

    Fitted on the finished stat line rather than composed from the variances of
    games, volume and rate. Those three are strongly correlated -- games against
    per-game volume runs +0.48 to +0.63 -- so a product of independent factors
    understates the spread, and dividing one out of another produced negative
    variances. Residuals of the finished line absorb all of it without needing any
    of it named.

    Args:
        holdout: Rows from :func:`_holdout_residuals`, carrying both the projected
            ``USG_<stat>`` and the realised ``y_tot_<column>``.
        positions: Positions to fit.

    Returns:
        dict: :func:`Scripts.usage.predictive.key` to dispersion.
    """
    out: Dict[str, float] = {}

    for stat, outcome in STAT_OUTCOMES.items():
        projected = f"{USAGE_PREFIX}{stat}"
        if any(c not in holdout.columns for c in (projected, outcome)):
            continue
        for position in positions:
            block = holdout.filter(
                (pl.col("position") == position)
                & pl.col(projected).is_not_null()
                & pl.col(outcome).is_not_null())
            if block.height < pv.MIN_FIT_ROWS:
                continue
            fitted = pv.fit_variance(
                block[outcome].cast(pl.Float64).to_numpy(),
                block[projected].cast(pl.Float64).to_numpy(),
                family=pv.family_for(stat))
            if fitted is not None:
                phi, k, bust = fitted
                out[pv.key(position, stat)] = {
                    "phi": float(phi), "k": float(k), "bust": float(bust)}
    return out


#: Seed for the randomised probability transform the correlation fit uses.
#:
#: Fixed rather than passed in, because the correlation matrix is part of a *fitted
#: model* and a model that came out different on a refit of identical data would not be
#: a model. See :func:`Scripts.usage.predictive.pit` for why the randomisation exists at
#: all.
CORRELATION_SEED: int = 28

#: Rows a position needs, with every included stat present, before its correlation is fitted.
#:
#: Higher than :data:`Scripts.usage.predictive.MIN_FIT_ROWS` because a correlation matrix
#: estimates ``n(n-1)/2`` numbers where a dispersion estimates two -- up to 28 for the
#: eight stats -- and a matrix fitted on 30 rows is mostly sampling noise wearing a
#: covariance structure. A position below this gets no matrix and the caller falls back
#: to independence, which is wrong in a known direction (too narrow) rather than wrong in
#: an unknown one.
MIN_CORRELATION_ROWS: int = 150


#: Cohorts the conditional dispersion is split by, matching plan 33's calibration.
COHORTS: Tuple[str, ...] = ("settled", "mover", "rookie")

#: Grid the games elasticity is searched over.
#:
#: Coarse and exhaustive rather than an optimiser, because the objective is a
#: one-dimensional sum of squares over a bounded parameter and a grid is deterministic,
#: has no starting point to get wrong, and cannot return a local minimum. 0.01 resolution
#: is far finer than the standard error on a few hundred rows.
_ELASTICITY_GRID = np.linspace(0.0, 1.5, 151)

#: Elasticity used for a (position, stat) with too few rows to fit its own.
#:
#: The pooled value, which lands near 0.45 across every position and stat measured. A
#: neutral-looking 1.0 would be the *worst* available default: it is exactly the
#: assumption the fit exists to reject.
DEFAULT_GAMES_ELASTICITY: float = 0.45


def _conditional_mean(projected: str, elasticity: float) -> pl.Expr:
    """``USG_<stat>`` re-based from the games the model expected onto the games he had.

    ``USG_<stat>`` is ``expected_games x volume x rate``, so the obvious re-basing is to
    divide by ``_mu_games`` and multiply by ``y_games`` -- assume the per-game line is
    what it is and let the games vary. **That assumption is measurably false**, and it
    was the first thing this fit was written to do. Measured on held-out residuals, the
    proportional re-base over-projects the realised total by **+8.8% to +26.7%** and
    drops the regression slope of realised on projected from ~1.00 to 0.32-0.70, while
    the unconditional projection on the same rows is unbiased with a slope of 0.92-1.10.
    The degradation is real, not the regression dilution a slope alone would suggest.

    The cause is that ``expected_games`` **carries role as well as health**, which
    ``docs/DATA_CATALOGUE.md`` states and this quantifies: a low number on a backup means
    buried, not fragile, and his per-game line is a buried player's line. Scaling him up
    to a starter's slate grants him a starter's games at a backup's rate and calls the
    product a projection.

    So the exponent is fitted rather than assumed::

        E[total | games] = USG_<stat> * (y_games / _mu_games) ** elasticity

    and it lands at **0.32 to 0.49** with the bias falling to within +-6% and the slope
    returning to 0.91-1.04. Read plainly: about half of the expected-games term is role
    rather than availability, so a player who plays twice the games the model expected
    produces roughly the square root of twice the output, not twice.

    Args:
        projected: The ``USG_<stat>`` column name.
        elasticity: The fitted exponent.

    Returns:
        pl.Expr: The conditional mean, null where ``_mu_games`` or ``y_games`` is
        missing or non-positive.
    """
    ratio = (pl.col("y_games").cast(pl.Float64)
             / pl.col("_mu_games").cast(pl.Float64))
    return (pl.when((pl.col("_mu_games").cast(pl.Float64) > 0)
                    & (pl.col("y_games").cast(pl.Float64) > 0))
            .then(pl.col(projected).cast(pl.Float64) * ratio.pow(float(elasticity)))
            .otherwise(None))


def _fit_games_elasticity(holdout: pl.DataFrame, positions: Sequence[str]
                          ) -> Dict[str, float]:
    """How much of the expected-games term is availability rather than role.

    See :func:`_conditional_mean` for what the number means and why assuming 1.0 is
    wrong. Fitted by least squares on the realised total, per (position, stat), because
    the answer differs by position in the direction plan 31 would predict -- quarterback
    is lowest, and quarterback expected-games is the most role-contaminated of the four.

    Args:
        holdout: Rows from :func:`_holdout_residuals`.
        positions: Positions to fit.

    Returns:
        dict: :func:`Scripts.usage.predictive.key` to elasticity. A pair with too few
        rows is absent; the caller falls back to
        :data:`DEFAULT_GAMES_ELASTICITY` explicitly.
    """
    if any(c not in holdout.columns for c in ("y_games", "_mu_games")):
        return {}

    out: Dict[str, float] = {}
    for stat, outcome in STAT_OUTCOMES.items():
        projected = f"{USAGE_PREFIX}{stat}"
        if any(c not in holdout.columns for c in (projected, outcome)):
            continue
        for position in positions:
            block = holdout.filter(
                (pl.col("position") == position)
                & pl.col(projected).is_not_null() & (pl.col(projected) > 0)
                & pl.col(outcome).is_not_null()
                & (pl.col("_mu_games").cast(pl.Float64) > 0)
                & (pl.col("y_games").cast(pl.Float64) > 0))
            if block.height < pv.MIN_FIT_ROWS:
                continue
            observed = block[outcome].cast(pl.Float64).to_numpy()
            mu = block[projected].cast(pl.Float64).to_numpy()
            ratio = (block["y_games"].cast(pl.Float64).to_numpy()
                     / block["_mu_games"].to_numpy())
            errors = [float(((observed - mu * ratio ** beta) ** 2).sum())
                      for beta in _ELASTICITY_GRID]
            out[pv.key(position, stat)] = float(
                _ELASTICITY_GRID[int(np.argmin(errors))])
    return out


def _fit_stat_dispersion_conditional(holdout: pl.DataFrame,
                                     positions: Sequence[str],
                                     elasticity: Dict[str, float]
                                     ) -> Dict[str, Dict[str, float]]:
    """Predictive dispersion per (position, stat), **given games played**.

    The same fit as :func:`_fit_stat_dispersion` against a different mean, and the
    difference between the two is the availability variance. Plan 28 needs them apart
    for two reasons that are really one:

    * An if-healthy interval cannot be made by rescaling an availability-inclusive one.
      :func:`Scripts.usage.project.to_full_slate` divides ``expected_games`` out of the
      *mean* so the blend compares like with like; rescaling the spread the same way
      leaves availability variance sitting around a quantity that no longer has any.
    * A simulation that draws games explicitly and then applies the unconditional
      dispersion counts availability twice, and would then pass a coverage gate by being
      too wide rather than by being right.

    Args:
        holdout: Rows from :func:`_holdout_residuals`, carrying the projected
            ``USG_<stat>``, the realised ``y_tot_<column>``, ``y_games`` and
            ``_mu_games``.
        positions: Positions to fit.

    Returns:
        dict: :func:`Scripts.usage.predictive.key` to ``{"phi", "k", "bust"}``. A pair
        with too few rows is absent rather than defaulted, so a missing fit cannot be
        mistaken for a measured one.
    """
    if any(c not in holdout.columns for c in ("y_games", "_mu_games")):
        return {}

    # Plan 33 phase 3. The pooled cell is fitted first and always; the cohort cells are
    # fitted on top and used where they exist. **Cohort is a real axis of this spread and
    # the shipped interval did not vary along it at all** -- measured on held-out
    # residuals, a rookie's coefficient of variation is 1.6x to 2.3x a settled player's
    # (RB rushing yards 0.70 settled against 1.28 rookie; TE receiving yards 0.57 against
    # 1.29), in exactly the order the role calibration predicts, since a listed settled
    # rank-1 really leads 58.8% of the time and a rookie 35.6%.
    #
    # It goes in the *dispersion* rather than into a separate role draw because the
    # residuals already contain role loss -- they are fitted end to end on the finished
    # line. Adding an explicit role effect on top of them would count it twice, which is
    # the same trap the vacancy transfer fell into.
    scoped = (holdout.with_columns(rl.cohort_expression().alias("_cohort"))
              if {"is_rookie", "team_changed"}.issubset(holdout.columns)
              else holdout.with_columns(pl.lit(None, dtype=pl.String).alias("_cohort")))

    out: Dict[str, Dict[str, float]] = {}
    for stat, outcome in STAT_OUTCOMES.items():
        projected = f"{USAGE_PREFIX}{stat}"
        if any(c not in holdout.columns for c in (projected, outcome)):
            continue
        for position in positions:
            beta = elasticity.get(pv.key(position, stat), DEFAULT_GAMES_ELASTICITY)
            rebased = scoped.with_columns(
                _conditional_mean(projected, beta).alias("_mu_cond"))
            usable = rebased.filter(
                (pl.col("position") == position)
                & pl.col("_mu_cond").is_not_null()
                & pl.col(outcome).is_not_null())

            for cohort in (None,) + COHORTS:
                block = (usable if cohort is None
                         else usable.filter(pl.col("_cohort") == cohort))
                if block.height < pv.MIN_FIT_ROWS:
                    continue
                fitted = pv.fit_variance(
                    block[outcome].cast(pl.Float64).to_numpy(),
                    block["_mu_cond"].to_numpy(),
                    family=pv.family_for(stat))
                if fitted is not None:
                    phi, k, bust = fitted
                    out[pv.key(position, stat, cohort)] = {
                        "phi": float(phi), "k": float(k), "bust": float(bust),
                        "n": int(block.height)}
    return out


def _nearest_correlation(matrix: np.ndarray) -> np.ndarray:
    """Force a correlation matrix to be positive semi-definite.

    A matrix of pairwise correlations estimated from real data is not guaranteed to be
    one -- rounding, and the fact that the eight stats are not all observed on the same
    rows, can leave a slightly negative eigenvalue. A Cholesky factorisation is what
    consumes this, and it simply fails on such a matrix, so the repair happens here
    where it can be explained rather than at the call site where it would look like a
    workaround.

    Clips the eigenvalues at zero, rebuilds, and renormalises the diagonal back to one.

    Args:
        matrix: A symmetric matrix with a unit diagonal.

    Returns:
        np.ndarray: The nearest positive semi-definite correlation matrix.
    """
    symmetric = (matrix + matrix.T) / 2.0
    values, vectors = np.linalg.eigh(symmetric)
    rebuilt = (vectors * np.clip(values, 0.0, None)) @ vectors.T
    scale = np.sqrt(np.clip(np.diag(rebuilt), _CORRELATION_EPS, None))
    return np.clip(rebuilt / np.outer(scale, scale), -1.0, 1.0)


_CORRELATION_EPS = 1e-12


def _fit_stat_correlation(holdout: pl.DataFrame, positions: Sequence[str],
                          dispersion: Dict[str, Dict[str, float]],
                          elasticity: Dict[str, float]
                          ) -> Dict[str, Dict[str, object]]:
    """Residual correlation across stats, per position, on the probability scale.

    Each stat's dispersion says how far a player can land from his own projection. It
    says nothing about whether the stats miss *together*, and they do: a receiver who
    beats his target projection beats his receiving-yard projection by the same event.
    Summing the marginals into season points without this understates the spread by
    exactly the covariance, which is the failure mode plan 18 already hit from the other
    direction when it tried to compose a season line out of independent factors and got
    negative variances back.

    Correlated on the **probability integral transform** rather than on raw residuals, so
    a Gaussian copula built from this reproduces the fitted marginals exactly: the
    transform is uniform by construction when the marginal is right, and the normal
    scores of a uniform are standard normal. Correlating raw residuals instead would mix
    the dependence with each stat's own skew.

    Args:
        holdout: Rows from :func:`_holdout_residuals`.
        positions: Positions to fit.
        dispersion: The conditional dispersions, since the correlation is used with them.

    Returns:
        dict: Position to ``{"stats": [...], "matrix": [[...]], "n": int}``. A position
        with fewer than :data:`MIN_CORRELATION_ROWS` complete rows is absent, and the
        caller falls back to independence.
    """
    if any(c not in holdout.columns for c in ("y_games", "_mu_games")):
        return {}

    rng = np.random.default_rng(CORRELATION_SEED)
    out: Dict[str, Dict[str, object]] = {}

    for position in positions:
        block = holdout.filter(pl.col("position") == position)
        if block.height < MIN_CORRELATION_ROWS:
            continue

        names, columns = [], []
        for stat, outcome in STAT_OUTCOMES.items():
            projected = f"{USAGE_PREFIX}{stat}"
            coefficients = dispersion.get(pv.key(position, stat))
            if coefficients is None or any(
                    c not in block.columns for c in (projected, outcome)):
                continue
            rebased = block.with_columns(
                _conditional_mean(
                    projected,
                    elasticity.get(pv.key(position, stat),
                                   DEFAULT_GAMES_ELASTICITY)).alias("_mu_cond"))
            mu = rebased["_mu_cond"].to_numpy()
            observed = rebased[outcome].cast(pl.Float64).to_numpy()
            # A stat this position does not accumulate -- a receiver's pass attempts --
            # has no mean to transform against, and including it would correlate an
            # arbitrary fill with everything else.
            usable = np.isfinite(mu) & np.isfinite(observed) & (mu > 0)
            if usable.sum() < MIN_CORRELATION_ROWS:
                continue
            transformed = np.full(mu.shape, np.nan)
            transformed[usable] = pv.pit(
                stat, mu[usable], coefficients["phi"], coefficients["k"],
                observed[usable], bust=coefficients.get("bust", 0.0), rng=rng)
            names.append(stat)
            columns.append(transformed)

        if len(names) < 2:
            continue

        stacked = np.column_stack(columns)
        # Complete rows only. A pairwise-complete matrix is assembled from different
        # subpopulations per cell and is routinely not positive semi-definite, which is
        # a harder problem than the rows it saves are worth.
        complete = np.isfinite(stacked).all(axis=1)
        if complete.sum() < MIN_CORRELATION_ROWS:
            continue

        scores = stats.norm.ppf(stacked[complete])
        matrix = _nearest_correlation(np.corrcoef(scores, rowvar=False))
        out[position] = {"stats": names,
                         "matrix": [[float(v) for v in row] for row in matrix],
                         "n": int(complete.sum())}
    return out


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

    Stored as a **share of the slate**, like the veteran head and for the same
    reason: 45% of the training rows come from 16-game seasons, so a bin mean taken
    in raw games projects a 17-game season low. See :data:`GAMES_REGRESSORS`.

    Returns:
        dict: Bin label to mean share of the slate. Empty when there are too few
        rookie rows.
    """
    if any(c not in frame.columns for c in ("y_games", "y_games_available")):
        return {}
    rows = _rookie_rows(frame, position)
    if rows.height < MIN_ROOKIE_FIT_ROWS:
        return {}

    # A missing slate is filled, never filtered. A rookie who never appeared has no
    # outcome row and therefore no measured denominator, and dropping him would
    # reweight the bin onto the minority who played -- 78.8% of undrafted rookies are
    # in that group, and it moved the undrafted bin from 1.1 games to 5.8. The rows'
    # own maximum is the season's slate, since every player shares it.
    slate = rows.select(
        pl.col("y_games_available").cast(pl.Float64).max()).item()
    rows = rows.with_columns(
        pl.col("y_games_available").cast(pl.Float64)
        .fill_null(float(slate or DEFAULT_TARGET_SLATE))
        .alias("y_games_available"))

    pick = (pl.col("draft_number").cast(pl.Float64)
            if "draft_number" in rows.columns
            else pl.lit(None, dtype=pl.Float64))
    grouped = (
        rows.filter(pl.col("y_games_available").cast(pl.Float64) > 0)
        .with_columns(draft_bin(pick).alias("bin"))
        .group_by("bin")
        .agg((pl.col("y_games").cast(pl.Float64).fill_null(0.0)
              / pl.col("y_games_available").cast(pl.Float64)).mean().alias("games"),
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
                   positions: Sequence[str] = ft.MODELLED_POSITIONS,
                   feature_kwargs: Optional[Dict[str, object]] = None
                   ) -> pl.DataFrame:
    """Feature rows for several seasons, each with that season's realised outcome.

    One :func:`Scripts.usage.features.season_features` call per season, so every
    row's features come only from seasons before it -- including the positional
    baselines the efficiency features are shrunk toward. Pooling the seasons first
    and lagging afterwards would be simpler and would leak.

    Args:
        seasons: Seasons to build training rows for.
        history_start: Earliest season the features may look back to.
        positions: Positions to model.
        feature_kwargs: Extra arguments for
            :func:`Scripts.usage.features.season_features`. The hook the plan 22
            lab uses to vary one thing at a time across a whole walk-forward
            without the experiment having to reimplement the fold loop.

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
        features = ft.season_features(season, history, positions=positions,
                                      **(feature_kwargs or {}))

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

        # The slate the outcome season actually offered, which is the denominator
        # that puts a 16-game and a 17-game season on one scale. Taken from
        # `season_availability` rather than from `team_games` directly, because it
        # already resolves the traded player -- his denominator is the larger of his
        # two teams' slates, and a player who moved mid-season can legitimately
        # appear in all 18 weeks when neither team's bye fell inside his tenure.
        availability = ctx.season_availability([season], weekly).select(
            "gsis_id", pl.col("games_available").alias("y_games_available"))
        outcome = outcome.join(availability, on="gsis_id", how="left")

        joined = features.join(outcome, on="gsis_id", how="left")

        # A player who never appeared has no availability row and no outcome row at
        # all, and he is not missing data -- he is a zero. The rookie bins are a mean
        # over *every* rookie including the majority who never play (78.8% of
        # undrafted are in that group), so leaving him without a denominator is not a
        # rounding error: it took the undrafted bin from 1.1 games to 5.8 and would
        # have projected a camp body as a third of a season. Filled after the join,
        # because that is where those rows first exist.
        season_slate = ctx.team_games(weekly).select(
            pl.col("team_games").cast(pl.Float64).max()).item()
        joined = joined.with_columns(
            pl.col("y_games_available").cast(pl.Float64)
            .fill_null(float(season_slate or DEFAULT_TARGET_SLATE))
            .alias("y_games_available"))

        frames.append(joined)

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
        positions: Sequence[str] = ft.MODELLED_POSITIONS,
        estimate_dispersion: bool = True) -> SeasonUsageModel:
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

    fitted = SeasonUsageModel(
        volume=volume,
        games=games,
        games_by_position=means,
        rookie_volume=rookie_vol,
        rookie_games=rookie_gms,
        rookie_efficiency=rookie_efficiency(train, positions),
        train_seasons=tuple(sorted(train_seasons)),
        fitted_at=fitted_at,
    )

    # The second moment, fitted after the first and given it. See
    # `Scripts.usage.availability` for why moments rather than a joint likelihood:
    # the mean is what plan 18 measured, and a joint fit would let the dispersion
    # move it.
    if not estimate_dispersion:
        return fitted

    # Dispersion is fitted on **held-out** residuals, not on the rows the mean was
    # fitted to. In-sample residuals are smaller by construction -- the volume
    # regressions run R-squared 0.35-0.63, so the gap is large -- and a dispersion
    # taken from them produces intervals that are far too narrow. Measured: yardage
    # coverage came out at 49-57% against a nominal 80% before this holdout existed.
    #
    # The sub-model is fitted with `estimate_dispersion=False`, which is what stops
    # this recursing.
    holdout = _holdout_residuals(train, train_seasons, positions)
    if holdout is None:
        return fitted

    elasticity = _fit_games_elasticity(holdout, positions)
    conditional = _fit_stat_dispersion_conditional(holdout, positions, elasticity)
    return replace(
        fitted,
        games_dispersion=_fit_dispersion(holdout, positions),
        stat_dispersion=_fit_stat_dispersion(holdout, positions),
        games_elasticity=elasticity,
        stat_dispersion_conditional=conditional,
        stat_correlation=_fit_stat_correlation(
            holdout, positions, conditional, elasticity),
    )


def _holdout_residuals(train: pl.DataFrame, train_seasons: Sequence[int],
                       positions: Sequence[str],
                       seasons_held: int = HOLDOUT_SEASONS
                       ) -> Optional[pl.DataFrame]:
    """Predictions for the most recent training seasons, from a model blind to them.

    Args:
        train: The full training frame.
        train_seasons: Seasons it covers.
        positions: Positions to fit.
        seasons_held: How many of the most recent seasons to hold out.

    Returns:
        pl.DataFrame | None: The held-out rows with predictions attached, or None
        when there is not enough history to hold anything out -- in which case the
        caller ships without intervals rather than with over-narrow ones.
    """
    seasons = sorted({int(s) for s in train_seasons})
    if len(seasons) < seasons_held + 2:
        return None

    earlier, held = seasons[:-seasons_held], seasons[-seasons_held:]
    inner = fit(train.filter(pl.col("season").is_in(earlier)), earlier,
                positions=positions, estimate_dispersion=False)

    rows = train.filter(pl.col("season").is_in(held))
    if rows.is_empty():
        return None

    slate = rows.select(pl.col("y_games_available").cast(pl.Float64).max()).item()
    predicted = inner.predict(rows, abstain_positions=(),
                              target_slate=slate or DEFAULT_TARGET_SLATE)
    return predicted.with_columns(
        inner.expected_games(rows, target_slate=slate or DEFAULT_TARGET_SLATE)
        .alias("_mu_games"))


def _fit_dispersion(holdout: pl.DataFrame,
                    positions: Sequence[str]) -> Dict[str, float]:
    """Beta-Binomial concentration per position, from held-out residuals.

    Args:
        holdout: Rows from :func:`_holdout_residuals`, carrying ``y_games``,
            ``y_games_available`` and ``_mu_games``.
        positions: Positions to fit.

    Returns:
        dict: Position to concentration. Positions with too few rows are absent
        rather than defaulted -- the caller falls back explicitly, so a missing fit
        cannot be mistaken for a measured one.
    """
    required = ("y_games", "y_games_available", "_mu_games")
    if any(c not in holdout.columns for c in required):
        return {}

    rows = holdout.filter(
        pl.col("y_games").is_not_null()
        & (pl.col("y_games_available").cast(pl.Float64) > 0)
        & pl.col("_mu_games").is_not_null())
    if rows.is_empty():
        return {}

    out: Dict[str, float] = {}
    for position in positions:
        block = rows.filter(pl.col("position") == position)
        if block.height < av.MIN_DISPERSION_ROWS:
            continue
        available = block["y_games_available"].cast(pl.Float64).to_numpy()
        kappa = av.fit_dispersion(
            block["y_games"].cast(pl.Float64).to_numpy(),
            available,
            block["_mu_games"].to_numpy() / available,
        )
        if kappa is not None:
            out[position] = float(kappa)
    return out
