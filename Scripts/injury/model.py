"""The fitted recovery curve and the re-injury hazard.

Two heads, one artifact, fitted from the same episode table on the same folds -- because
versioning them apart invites a curve and a hazard that were never fitted together.

**The estimator is a ratio of sums, not a mean of ratios, and that is not a detail.** A
multiplicative factor is estimated by dividing what happened by what was expected to
happen:

    M(w) = sum(points on appearance w) / sum(baseline x control expectation)

Three reasons it has to be this way. **125 of 2,121 post-return appearances score exactly
zero** -- a player is on the field and does nothing -- so no mean of log-ratios exists, and
dropping those rows would discard precisely the worst outcomes and bias every curve upward.
A mean of ratios is the wrong estimator for a multiplicative factor regardless: it weights a
30-point week off a 6-point baseline the same as one off a 20-point baseline. And the
denominator is where the placebo correction belongs -- ``expected`` is what a *comparable
healthy player* would have scored, so a curve fitted against it asymptotes to "as good as
his peers" rather than to "as good as his own selected four-game peak", which is
unattainable by construction.

**The curve is monotone and parametric because the empirical cells are noise.** Two
parameters:

    m(w) = 1 - a * exp(-(w - 1) / tau)

``a`` is the shortfall in the first game back and ``tau`` the recovery time constant --
the smallest form that can express "deep then fast" against "shallow then slow", which is
the knee-against-hamstring contrast the data actually shows. Per-cell empirical means at
n = 20-60 give hamstrings 1.39 in the sixth game back and illness 0.45 in the fourth; a
model shipping those would confidently predict that hamstrings overperform once healed.

**Shrinkage is empirical Bayes on the transformed parameters**, three levels deep, the same
way :mod:`Scripts.usage.availability` already pools its dispersion. A cell that cannot
support its own fit gets its parent's, and records that it did.

**Abstention is a multiplier of exactly 1.0.** A cell whose shortfall is inside two
bootstrap standard errors has not measured anything, and says so rather than shipping a
number it cannot stand behind.

Polars for the data, ``scipy.optimize`` and ``sklearn`` for the fits, no new dependency.

Usage::

    python -m Scripts.injury.model --fit
    python -m Scripts.injury.model --show
"""

from __future__ import annotations

import datetime
import json
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts import paths
from Scripts.injury import episodes as ep
from Scripts.injury import lexicon

#: Bumped when the fitted form changes, not when the data does.
MODEL_VERSION = "1.0.0"

#: Appearances the curve is defined over.
WINDOW = ep.POST_RETURN_WINDOW

#: Episode counts below which a cell takes its parent's parameters.
MIN_CELL_EPISODES = 30

#: Episode count below which even the parent abstains entirely.
MIN_PARENT_EPISODES = 60

#: The multiplier can never go below this.
#:
#: A runaway fit is a silent 50% haircut on a real player, and the shape of the failure --
#: ``a`` pinned at 1.0 by a handful of zero-score appearances in a thin cell -- is exactly
#: what a two-parameter fit on 30 episodes can produce.
MULTIPLIER_FLOOR = 0.50

#: Longest recovery constant the data can support, in appearances.
#:
#: **An identifiability bound, not a taste.** The curve is observed at six points, so a
#: time constant longer than that window is not estimable: the likelihood is essentially
#: flat above it and the optimiser lands wherever its starting value pushed it. Left
#: unbounded the ankle cell came back at ``tau = 112`` and its five-plus-week cell at
#: ``tau = 399``, which is the optimiser's way of saying "a constant 0.91 fits these six
#: points" -- there is no other way to write a flat line in this parameterisation.
#:
#: That is not a harmless reparameterisation. It asserts a *permanent* 9% talent reduction
#: from an ankle sprain, which nothing in the evidence supports -- the pooled net is back to
#: 1.0 by the third appearance -- and it triples what :meth:`InjuryModel.season_multiplier`
#: charges, because that sums the shortfall across the window.
MAX_TAU = float(WINDOW)

#: Shrinkage strengths to cross-validate over, in units of episodes.
#:
#: Swept rather than chosen. ``registry.py``'s discipline, verbatim: a gain that holds
#: across a wide range is a real bias-variance trade; a gain that spikes at one value is
#: the test set being fitted.
SHRINKAGE_GRID = (5, 10, 20, 40, 80)

#: Baseline-points deciles the control curve is stratified on.
#:
#: The skew that makes the placebo 0.98 rather than 1.00 depends on level, so a single
#: pooled control over-corrects the top of the pool and under-corrects the bottom.
DECILES = 10

#: Bootstrap resamples, over episodes rather than rows.
#:
#: Six appearances of one episode are one observation. Resampling rows would treat them as
#: six and understate every standard error by roughly the square root of that.
BOOTSTRAPS = 200

#: Weeks after a return the hazard is defined over.
HAZARD_WINDOW = ep.RECURRENCE_WINDOW


# --- the placebo curve ----------------------------------------------------

@dataclass
class ControlCurve:
    """What a comparable healthy player scores, relative to his own baseline.

    The denominator of every fitted multiplier. Keyed on ``(position, decile,
    appearance)`` with progressively coarser fallbacks, because a stratum can be empty and
    a missing denominator must not become a missing row.

    Attributes:
        by_stratum: ``"POS|decile|appearance"`` -> ratio of sums.
        by_appearance: ``appearance`` -> ratio of sums, pooled.
        decile_edges: Baseline-points quantile boundaries, from the control cohort.
        rows: How many control appearances went into it.
    """

    by_stratum: Dict[str, float] = field(default_factory=dict)
    by_appearance: Dict[str, float] = field(default_factory=dict)
    decile_edges: List[float] = field(default_factory=list)
    rows: int = 0

    def decile(self, base_pts: float) -> int:
        """Which baseline decile a player falls in."""
        return int(np.searchsorted(self.decile_edges, base_pts, side="right"))

    def expected(self, position: str, base_pts: float, appearance: int) -> float:
        """The ratio a comparable healthy player would post.

        Args:
            position: ``QB``/``RB``/``WR``/``TE``.
            base_pts: The player's own pre-injury baseline.
            appearance: 1-based games since returning.

        Returns:
            float: Expected ratio to baseline. Falls back through pooled-by-appearance to
            1.0, so an unseen stratum costs precision rather than a row.
        """
        key = f"{position}|{self.decile(base_pts)}|{appearance}"
        if key in self.by_stratum:
            return self.by_stratum[key]
        return self.by_appearance.get(str(appearance), 1.0)

    def to_dict(self) -> Dict:
        return {"by_stratum": self.by_stratum, "by_appearance": self.by_appearance,
                "decile_edges": self.decile_edges, "rows": self.rows}

    @classmethod
    def from_dict(cls, payload: Dict) -> "ControlCurve":
        return cls(by_stratum=payload.get("by_stratum", {}),
                   by_appearance=payload.get("by_appearance", {}),
                   decile_edges=list(payload.get("decile_edges", [])),
                   rows=int(payload.get("rows", 0)))


def fit_control(controls: pl.DataFrame, min_stratum: int = 40) -> ControlCurve:
    """Measure the placebo curve, stratified by position and baseline level.

    Args:
        controls: :func:`Scripts.injury.episodes.control_cohort` output.
        min_stratum: Rows a ``(position, decile, appearance)`` cell needs before it is
            trusted over the pooled figure.

    Returns:
        ControlCurve: The denominator every multiplier is measured against.
    """
    if controls.is_empty():
        return ControlCurve()

    # Deduplicated, because ``cut`` raises ``DuplicateError: breaks are not unique`` on
    # repeated boundaries -- which a narrow baseline distribution produces readily. A thin
    # cohort, or one season of a single position, can collapse every quantile onto the same
    # value, and the correct response is fewer strata rather than an exception.
    edges = sorted({float(controls["base_pts"].quantile(q / DECILES) or 0.0)
                    for q in range(1, DECILES)})
    if edges:
        frame = controls.with_columns(
            pl.col("base_pts").cut(edges, labels=[str(i) for i in range(len(edges) + 1)])
            .cast(pl.Utf8).alias("decile"))
    else:
        frame = controls.with_columns(pl.lit("0").alias("decile"))

    pooled = (frame.group_by("appearance_back")
              .agg(pl.col("fantasy_points_ppr").sum().alias("pts"),
                   pl.col("base_pts").sum().alias("base"))
              .with_columns((pl.col("pts") / pl.col("base")).alias("ratio")))
    by_appearance = {str(r["appearance_back"]): r["ratio"]
                     for r in pooled.iter_rows(named=True) if r["base"]}

    strata = (frame.group_by(["position", "decile", "appearance_back"])
              .agg(pl.col("fantasy_points_ppr").sum().alias("pts"),
                   pl.col("base_pts").sum().alias("base"),
                   pl.len().alias("n"))
              .filter((pl.col("n") >= min_stratum) & (pl.col("base") > 0)))
    by_stratum = {
        f"{r['position']}|{r['decile']}|{r['appearance_back']}": r["pts"] / r["base"]
        for r in strata.iter_rows(named=True)}

    return ControlCurve(by_stratum=by_stratum, by_appearance=by_appearance,
                        decile_edges=edges, rows=controls.height)


# --- the recovery curve ---------------------------------------------------

def curve(a: float, tau: float, appearance) -> np.ndarray:
    """``m(w) = 1 - a * exp(-(w - 1) / tau)``.

    Monotone non-decreasing in ``w`` and asymptotic to 1.0 by construction, so the fit
    cannot produce a player who recovers and then declines -- which the empirical cells do,
    and which is noise.

    Args:
        a: Shortfall in the first appearance back, in (0, 1).
        tau: Recovery time constant, in appearances.
        appearance: 1-based appearance index, scalar or array.

    Returns:
        np.ndarray: The multiplier.
    """
    w = np.asarray(appearance, dtype=float)
    return 1.0 - a * np.exp(-(w - 1.0) / max(tau, 1e-6))


def _pack(a: float, tau: float) -> np.ndarray:
    """Unbounded coordinates, so the optimiser cannot leave the valid region."""
    a = min(max(a, 1e-6), 1 - 1e-6)
    return np.array([math.log(a / (1 - a)), math.log(max(tau, 1e-6))])


def _unpack(theta: np.ndarray) -> Tuple[float, float]:
    a = 1.0 / (1.0 + math.exp(-float(np.clip(theta[0], -30, 30))))
    # Clamped here rather than at the call sites so the bound holds through the fit, the
    # bootstrap and the shrinkage alike -- a cap the shrinkage could step over would let a
    # combined cell inherit a tau its parents were not allowed to have.
    tau = math.exp(float(np.clip(theta[1], -6, math.log(MAX_TAU))))
    return a, tau


def observed(rows: pl.DataFrame, control: ControlCurve) -> pl.DataFrame:
    """The net multiplier per appearance, as a ratio of sums.

    Args:
        rows: Post-return appearances for one cell.
        control: The placebo curve.

    Returns:
        pl.DataFrame: ``appearance_back``, ``net`` (observed / expected), ``mass`` (the
        expected points the estimate rests on) and ``n``.
    """
    if rows.is_empty():
        return pl.DataFrame(schema={"appearance_back": pl.Int64, "net": pl.Float64,
                                    "mass": pl.Float64, "n": pl.UInt32})
    expected = [
        r["base_pts"] * control.expected(r["position"], r["base_pts"],
                                         r["appearance_back"])
        for r in rows.iter_rows(named=True)]
    return (rows.with_columns(pl.Series("expected", expected))
            .group_by("appearance_back")
            .agg(pl.col("fantasy_points_ppr").sum().alias("pts"),
                 pl.col("expected").sum().alias("mass"),
                 pl.len().alias("n"))
            .filter(pl.col("mass") > 0)
            .with_columns((pl.col("pts") / pl.col("mass")).alias("net"))
            .sort("appearance_back"))


def fit_curve(points: pl.DataFrame,
              start: Tuple[float, float] = (0.15, 1.0)) -> Tuple[float, float]:
    """Least-squares fit of :func:`curve` to observed net multipliers.

    Weighted by expected-points mass rather than by row count, because that is the
    precision of a ratio-of-sums estimate: twenty appearances by starters carry more
    information about a multiplier than twenty by fringe players.

    Args:
        points: :func:`observed` output.
        start: Initial ``(a, tau)``.

    Returns:
        tuple: Fitted ``(a, tau)``. The start values back when there is nothing to fit.
    """
    from scipy import optimize

    if points.height < 2:
        return start

    w = points["appearance_back"].to_numpy().astype(float)
    y = points["net"].to_numpy()
    weight = np.sqrt(points["mass"].to_numpy())

    def residual(theta):
        a, tau = _unpack(theta)
        return (curve(a, tau, w) - y) * weight

    try:
        # soft_l1 rather than plain least squares: a cell of thirty episodes can contain
        # one appearance where a returning starter scored 32 points, and a quadratic loss
        # would let it set the asymptote.
        result = optimize.least_squares(residual, _pack(*start), loss="soft_l1",
                                        f_scale=np.median(weight) or 1.0, max_nfev=2000)
    except (ValueError, RuntimeError):
        return start
    return _unpack(result.x)


@dataclass
class RecoveryCell:
    """One fitted cell of the recovery curve.

    Attributes:
        body_part: A :data:`Scripts.injury.lexicon.GROUPS` value, or ``"__global__"``.
        duration_bucket: A :func:`Scripts.injury.episodes.duration_bucket` value, or None
            for a body-part-level cell.
        a: Fitted shortfall in the first appearance back, after shrinkage.
        tau: Fitted recovery constant, after shrinkage.
        a_sd: Bootstrap standard error of ``a``.
        tau_sd: Bootstrap standard error of ``tau``.
        episodes: Episodes the cell's own fit rested on.
        rows: Appearances.
        parent: The cell shrunk toward.
        shrunk_from: ``own`` when the cell had enough to fit, ``parent`` when it did not.
        abstained: True when the shortfall is inside two standard errors, in which case
            :meth:`InjuryModel.multiplier` returns exactly 1.0.
        reason: One readable sentence.
    """

    body_part: str
    duration_bucket: Optional[str]
    a: float
    tau: float
    a_sd: float
    tau_sd: float
    episodes: int
    rows: int
    parent: Optional[str]
    shrunk_from: str
    abstained: bool
    reason: str

    @property
    def key(self) -> str:
        return f"{self.body_part}|{self.duration_bucket or '*'}"


def _episode_count(rows: pl.DataFrame) -> int:
    """Episodes, not appearances. Six games of one injury are one observation."""
    if rows.is_empty():
        return 0
    return rows.select(["gsis_id", "season", "run"]).unique().height


def _bootstrap(rows: pl.DataFrame, control: ControlCurve,
               start: Tuple[float, float], draws: int,
               seed: int = 0) -> Tuple[float, float]:
    """Standard errors by resampling **episodes** with replacement.

    Resampling rows would treat one injury's six appearances as six independent
    observations and understate every standard error by roughly ``sqrt(6)`` -- which
    matters here because the abstention rule is stated in standard errors.

    Args:
        rows: Post-return appearances for one cell.
        control: The placebo curve.
        start: Fitted ``(a, tau)`` to start each resample from.
        draws: Resamples.
        seed: Fixed, because ``Math.random``-style irreproducibility in a fitted artifact
            means two runs disagree and neither is wrong.

    Returns:
        tuple: ``(a_sd, tau_sd)``.
    """
    # **Sorted, and that is not cosmetic.** ``unique()`` does not guarantee row order --
    # it is unordered and parallelised -- so a seeded RNG indexing into it draws a
    # different sample on every run. That made the fitted standard errors non-reproducible,
    # which then moved the abstention decisions, which then moved the walk-forward's chosen
    # shrinkage: two runs of the same code on the same data disagreed and neither was wrong.
    # A fitted artifact has to be reproducible or its provenance means nothing.
    episodes = (rows.select(["gsis_id", "season", "run"]).unique()
                .sort(["gsis_id", "season", "run"]))
    if episodes.height < 5:
        return float("nan"), float("nan")

    rng = np.random.default_rng(seed)
    index = np.arange(episodes.height)
    fits: List[Tuple[float, float]] = []
    for _ in range(draws):
        picked = episodes[rng.choice(index, size=episodes.height, replace=True)]
        # A duplicated episode has to count twice, so join rather than filter.
        sample = picked.join(rows, on=["gsis_id", "season", "run"], how="left")
        fits.append(fit_curve(observed(sample, control), start=start))
    if not fits:
        return float("nan"), float("nan")
    array = np.array(fits)
    return float(np.nanstd(array[:, 0])), float(np.nanstd(array[:, 1]))


def _shrink(own: Tuple[float, float], parent: Tuple[float, float], n: int,
            k: float) -> Tuple[float, float]:
    """Empirical-Bayes blend of a cell's own fit toward its parent's.

    On the transformed scale, so the result cannot leave ``a in (0, 1)`` or ``tau > 0``
    however few episodes the cell has. ``n`` counts episodes and ``k`` is in the same
    units, so ``k = 20`` means "trust the cell's own fit once it has twenty episodes as
    much as the parent".

    Args:
        own: The cell's own ``(a, tau)``.
        parent: The parent's, after its own shrinkage.
        n: Episodes in the cell.
        k: Shrinkage strength.

    Returns:
        tuple: The shrunk ``(a, tau)``.
    """
    own_theta, parent_theta = _pack(*own), _pack(*parent)
    weight = n / (n + k) if (n + k) else 0.0
    return _unpack(weight * own_theta + (1 - weight) * parent_theta)


# --- the re-injury hazard -------------------------------------------------

def person_periods(episodes: pl.DataFrame) -> pl.DataFrame:
    """One row per post-return week at risk, until a recurrence or the window closes.

    The discrete-time survival layout. A returned episode contributes weeks 1..min(gap,
    window): if the same body part went again in week 3, that is two clean weeks and one
    event, not one row saying "recurred".

    Unknown body parts are excluded -- ``other`` is a bucket, not a diagnosis, and two
    reserve stints landing in it are not evidence of a recurrence.

    Args:
        episodes: :func:`Scripts.injury.episodes.recurrence` output.

    Returns:
        pl.DataFrame: ``body_part``, ``weeks_out``, ``week`` (1-based weeks since
        returning) and ``event`` (1 on the week it recurred).
    """
    returned = episodes.filter((pl.col("outcome") == "returned")
                               & (~pl.col("body_part_unknown")))
    if returned.is_empty():
        return pl.DataFrame()

    rows = []
    for row in returned.iter_rows(named=True):
        gap = row.get("weeks_to_recurrence")
        last = int(min(gap, HAZARD_WINDOW)) if gap is not None else HAZARD_WINDOW
        last = max(last, 1)
        for week in range(1, last + 1):
            rows.append({
                "body_part": row["body_part"],
                "weeks_out": float(row["weeks_out"]),
                "week": week,
                "event": int(gap is not None and week == last),
            })
    return pl.DataFrame(rows)


@dataclass
class Hazard:
    """Discrete-time logistic hazard of the same body part going again.

    Attributes:
        intercept: Model intercept.
        by_body_part: Group offset, L2-penalised toward zero.
        week: Coefficient on weeks since returning.
        log_weeks_out: Coefficient on ``log(1 + weeks_out)``.
        brier: In-sample Brier score.
        base_brier: Brier score of the constant base rate, which it must beat.
        base_rate: The pooled weekly recurrence rate.
        weeks_at_risk: Rows fitted.
        penalty: The chosen L2 strength.
    """

    intercept: float = 0.0
    by_body_part: Dict[str, float] = field(default_factory=dict)
    week: float = 0.0
    log_weeks_out: float = 0.0
    brier: float = float("nan")
    base_brier: float = float("nan")
    base_rate: float = 0.0
    weeks_at_risk: int = 0
    penalty: float = 1.0

    def weekly(self, body_part: str, weeks_out: float, week: int) -> float:
        """P(the same body part goes again in this week), given it has not yet.

        Args:
            body_part: A lexicon group.
            weeks_out: How long the original absence lasted.
            week: 1-based weeks since returning.

        Returns:
            float: A probability in (0, 1).
        """
        z = (self.intercept
             + self.by_body_part.get(body_part, 0.0)
             + self.week * week
             + self.log_weeks_out * math.log1p(max(weeks_out, 0.0)))
        return 1.0 / (1.0 + math.exp(-max(min(z, 30.0), -30.0)))

    def cumulative(self, body_part: str, weeks_out: float,
                   weeks: int = HAZARD_WINDOW) -> float:
        """P(it goes again at all inside ``weeks``).

        One minus the product of surviving each week, which is what a discrete hazard
        means -- summing the weekly probabilities would double-count.

        Args:
            body_part: A lexicon group.
            weeks_out: How long the original absence lasted.
            weeks: Horizon.

        Returns:
            float: A probability in (0, 1).
        """
        survive = 1.0
        for week in range(1, int(weeks) + 1):
            survive *= 1.0 - self.weekly(body_part, weeks_out, week)
        return 1.0 - survive

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Dict) -> "Hazard":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in (payload or {}).items() if k in known})


def fit_hazard(periods: pl.DataFrame,
               penalties: Sequence[float] = (0.1, 1.0, 10.0)) -> Hazard:
    """Fit the weekly recurrence hazard.

    **The L2 penalty on the body-part indicators is the partial pooling**, and it is worth
    saying so outright: with one-hot groups and a shared intercept, ridge pulls each
    group's offset toward the pooled rate in proportion to how little data it has. That is
    the same shrinkage the recovery curve does by hand, obtained here for free -- a reader
    who does not know that will think the pooling was forgotten.

    Args:
        periods: :func:`person_periods` output.
        penalties: L2 strengths to choose between, by Brier score.

    Returns:
        Hazard: The fitted head. Base-rate-only when there is nothing to fit.
    """
    from sklearn.linear_model import LogisticRegression

    if periods.is_empty() or periods["event"].sum() < 5:
        rate = float(periods["event"].mean()) if not periods.is_empty() else 0.0
        return Hazard(intercept=math.log(max(rate, 1e-6) / max(1 - rate, 1e-6)),
                      base_rate=rate, weeks_at_risk=periods.height)

    groups = sorted(periods["body_part"].unique().to_list())
    design = np.column_stack([
        np.array([[1.0 if r == g else 0.0 for g in groups]
                  for r in periods["body_part"].to_list()]),
        periods["week"].to_numpy().astype(float).reshape(-1, 1),
        np.log1p(periods["weeks_out"].to_numpy()).reshape(-1, 1),
    ])
    y = periods["event"].to_numpy().astype(int)
    base_rate = float(y.mean())
    base_brier = float(np.mean((base_rate - y) ** 2))

    best = None
    for penalty in penalties:
        model = LogisticRegression(C=1.0 / penalty, max_iter=2000,
                                   fit_intercept=True)
        model.fit(design, y)
        brier = float(np.mean((model.predict_proba(design)[:, 1] - y) ** 2))
        if best is None or brier < best[0]:
            best = (brier, penalty, model)

    brier, penalty, model = best
    coefficients = model.coef_[0]
    return Hazard(
        intercept=float(model.intercept_[0]),
        by_body_part={g: float(c) for g, c in zip(groups, coefficients[:len(groups)])},
        week=float(coefficients[len(groups)]),
        log_weeks_out=float(coefficients[len(groups) + 1]),
        brier=brier, base_brier=base_brier, base_rate=base_rate,
        weeks_at_risk=periods.height, penalty=penalty)


# --- what ESPN already prices --------------------------------------------

@dataclass
class EspnRamp:
    """How much of the post-return drop ESPN's own weekly projection already carries.

    Gate G-B0, kept in the artifact because the applied multiplier depends on it. Measured
    on 2025's stored weekly lineups, which carry ESPN's ``projPoints`` beside actual
    ``points``: for a player in his first three appearances back, net of a healthy control
    passing the identical baseline filter, ESPN prices **0.920 / 0.926 / 0.911** against a
    real **0.860 / 0.802 / 0.788**.

    **ESPN is not modelling recovery, it is applying a penalty.** A flat ~8% on anyone
    recently injured, which is why it under-prices the second and third games back where
    the real drop is 20%+. So the residual -- the part still unpriced -- is
    ``fitted / priced``, and it is *deeper* later, the opposite of the raw curve's shape.
    Applying the raw curve over the top of ESPN's would double count roughly half the
    first-game effect.

    **It applies to the weekly path only.** ESPN's *season* projection is published before
    anyone is "recently returned", so there is no ramp in it to net against and the season
    path takes the fitted curve unmodified.

    Attributes:
        by_appearance: ``appearance`` -> ESPN's net ratio.
        episodes: Distinct episodes behind it. Not row count -- the same player appears in
            up to nine league lineups, which inflated the apparent n threefold.
        seasons: Seasons measured. One, which is why this is a stored calibration with a
            caveat rather than a walk-forwarded parameter.
    """

    by_appearance: Dict[str, float] = field(default_factory=dict)
    episodes: int = 0
    seasons: List[int] = field(default_factory=list)

    def priced(self, appearance: int) -> float:
        """ESPN's own net ratio at an appearance.

        Measured for the first three appearances only, and **carried forward** beyond
        them rather than reverting to 1.0. That is using the finding rather than guessing
        past it: the measurement's central result is that ESPN applies a *flat* penalty
        (0.919, 0.926, 0.911) rather than a recovery curve, so flat is the extrapolation
        the data supports.

        Reverting to 1.0 instead made the residual non-monotone -- a knee came out at 1.00
        in the third appearance and 0.95 in the fourth, implying he gets worse as he heals,
        purely because the denominator vanished.

        Args:
            appearance: 1-based games since returning.

        Returns:
            float: ESPN's net ratio, or 1.0 when nothing was measured at all.
        """
        if not self.by_appearance:
            return 1.0
        key = str(appearance)
        if key in self.by_appearance:
            return float(self.by_appearance[key])
        last = max(int(k) for k in self.by_appearance)
        return float(self.by_appearance[str(last)]) if appearance > last else 1.0

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Dict) -> "EspnRamp":
        payload = payload or {}
        return cls(by_appearance=payload.get("by_appearance", {}),
                   episodes=int(payload.get("episodes", 0)),
                   seasons=list(payload.get("seasons", [])))


# --- the model ------------------------------------------------------------

@dataclass
class InjuryModel:
    """The fitted recovery curve, the re-injury hazard, and what ESPN already prices."""

    cells: Dict[str, RecoveryCell] = field(default_factory=dict)
    control: ControlCurve = field(default_factory=ControlCurve)
    hazard: Hazard = field(default_factory=Hazard)
    espn: EspnRamp = field(default_factory=EspnRamp)
    body_part_map: Dict[str, str] = field(default_factory=dict)
    shrinkage: Dict = field(default_factory=dict)
    train_seasons: Tuple[int, ...] = ()
    version: str = MODEL_VERSION
    fitted_at: Optional[str] = None
    git_sha: Optional[str] = None

    # --- reading it ------------------------------------------------------

    def cell(self, body_part: str,
             duration_bucket: Optional[str] = None) -> Optional[RecoveryCell]:
        """The most specific fitted cell for a body part and duration.

        Args:
            body_part: A lexicon group.
            duration_bucket: A duration bucket, or None.

        Returns:
            RecoveryCell | None: The duration-specific cell, else the body-part cell, else
            the global one, else None when nothing was fitted.
        """
        candidates: List[str] = []
        if duration_bucket:
            candidates += [f"{body_part}|{duration_bucket}",
                           f"{GLOBAL}|{duration_bucket}"]
        candidates += [f"{body_part}|*", f"{GLOBAL}|*"]
        for key in candidates:
            if key in self.cells:
                return self.cells[key]
        return None

    def multiplier(self, body_part: str, appearance: int,
                   duration_bucket: Optional[str] = None,
                   net_of_espn: bool = False) -> float:
        """The efficiency multiplier for one appearance after a return.

        Args:
            body_part: A lexicon group.
            appearance: 1-based games since returning. Outside the fitted window this is
                1.0 -- the curve says the effect is gone, and extrapolating a two-parameter
                exponential past its data is how a model invents a tail.
            duration_bucket: A duration bucket, or None.
            net_of_espn: Divide by what ESPN already prices. **True on the weekly path,
                False on the season path** -- see :class:`EspnRamp`.

        Returns:
            float: A multiplier in ``[MULTIPLIER_FLOOR, 1.0]``. Exactly 1.0 when the cell
            abstained, when the body part is excluded from the fit, or when no model was
            fitted at all.
        """
        if appearance < 1 or appearance > WINDOW:
            return 1.0
        if body_part in lexicon.RECOVERY_EXCLUDED_GROUPS:
            return 1.0
        found = self.cell(body_part, duration_bucket)
        if found is None or found.abstained:
            return 1.0

        value = float(curve(found.a, found.tau, appearance))
        if net_of_espn:
            priced = self.espn.priced(appearance)
            # Only ever *reduces* the haircut. If ESPN happened to price more than the
            # fitted drop at some appearance, the residual is nothing, not a bonus.
            value = min(value / priced, 1.0) if priced else value
        return float(min(max(value, MULTIPLIER_FLOOR), 1.0))

    def ladder(self, body_part: str, duration_bucket: Optional[str] = None,
               net_of_espn: bool = False) -> List[float]:
        """The whole ramp, for display and for the override file to be compared against."""
        return [self.multiplier(body_part, w, duration_bucket, net_of_espn)
                for w in range(1, WINDOW + 1)]

    def season_multiplier(self, body_part: str, games_remaining: float,
                          duration_bucket: Optional[str] = None) -> float:
        """The ramp translated into a single season-total factor.

        The honest translation, and the number that decides whether the season path is
        worth touching at all: the ramp costs a fixed quantity of partial games, so its
        share of a season shrinks the earlier the player returns.

        Args:
            body_part: A lexicon group.
            games_remaining: Games left after he is back.
            duration_bucket: A duration bucket, or None.

        Returns:
            float: A multiplier in ``[MULTIPLIER_FLOOR, 1.0]``.
        """
        if games_remaining <= 0:
            return 1.0
        cost = sum(1.0 - m for m in self.ladder(body_part, duration_bucket))
        return float(min(max(1.0 - cost / games_remaining, MULTIPLIER_FLOOR), 1.0))

    def reinjury_probability(self, body_part: str, weeks_out: float,
                             weeks: int = HAZARD_WINDOW) -> float:
        """P(the same body part goes again inside ``weeks`` of returning)."""
        if body_part in lexicon.RECOVERY_EXCLUDED_GROUPS:
            return 0.0
        return self.hazard.cumulative(body_part, weeks_out, weeks)

    def expected_games_lost_to_reinjury(self, body_part: str, weeks_out: float,
                                       weeks: int = HAZARD_WINDOW) -> float:
        """Games the recurrence risk is expected to cost, on top of the first absence.

        Channel C feeding channel A. A recurrence is a *new absence*, so it costs games
        rather than efficiency, and it is priced as the probability of one times how long
        one of those lasts -- for which the best available estimate is how long this one
        did.

        Args:
            body_part: A lexicon group.
            weeks_out: The original absence, in games.
            weeks: Horizon.

        Returns:
            float: Expected additional games missed.
        """
        return self.reinjury_probability(body_part, weeks_out, weeks) * max(weeks_out, 1.0)

    def is_stale(self, through_season: int) -> bool:
        """Whether the artifact was trained before a season that has since been played."""
        return not self.train_seasons or max(self.train_seasons) < through_season

    # --- writing it ------------------------------------------------------

    def to_dict(self) -> Dict:
        """Serialisable form, with the metadata ``CLAUDE.md`` asks models to carry."""
        return {
            "version": self.version,
            "fitted_at": self.fitted_at,
            "git_sha": self.git_sha,
            "train_seasons": list(self.train_seasons),
            "lexicon": self.body_part_map,
            "control": self.control.to_dict(),
            "recovery": [asdict(cell) for cell in self.cells.values()],
            "hazard": self.hazard.to_dict(),
            "espn_ramp": self.espn.to_dict(),
            "shrinkage": self.shrinkage,
        }

    def save(self, path=None):
        """Write the fitted coefficients as JSON."""
        path = Path(path) if path else self.default_path(self.version)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True))
        return path

    @classmethod
    def default_path(cls, version: Optional[str] = None):
        """``Data/NFL/models/injury_recovery_<version>.json``."""
        return (paths.DATA_DIR / "NFL" / "models"
                / f"injury_recovery_{version or MODEL_VERSION}.json")

    @classmethod
    def load(cls, path=None) -> "InjuryModel":
        """Read back what :meth:`save` wrote.

        Raises:
            FileNotFoundError: When the artifact has not been fitted.
        """
        path = cls.default_path() if path is None else Path(path)
        if not path.is_file():
            raise FileNotFoundError(
                f"No fitted injury model at {path}. Fit it with "
                f"`python -m Scripts.injury.model --fit`.")
        payload = json.loads(path.read_text())
        cells = {}
        for entry in payload.get("recovery", []):
            cell = RecoveryCell(**entry)
            cells[cell.key] = cell
        return cls(
            cells=cells,
            control=ControlCurve.from_dict(payload.get("control", {})),
            hazard=Hazard.from_dict(payload.get("hazard", {})),
            espn=EspnRamp.from_dict(payload.get("espn_ramp", {})),
            body_part_map=payload.get("lexicon", {}),
            shrinkage=payload.get("shrinkage", {}),
            train_seasons=tuple(payload.get("train_seasons", ())),
            version=payload.get("version", MODEL_VERSION),
            fitted_at=payload.get("fitted_at"),
            git_sha=payload.get("git_sha"),
        )


# --- fitting --------------------------------------------------------------

GLOBAL = "__global__"


def _fit_cell(rows: pl.DataFrame, control: ControlCurve, body_part: str,
              bucket: Optional[str], parent: Optional[RecoveryCell], k: float,
              draws: int, seed: int) -> RecoveryCell:
    """Fit one cell, shrink it toward its parent, and decide whether it may speak.

    Args:
        rows: Post-return appearances in the cell.
        control: The placebo curve.
        body_part: Lexicon group, or :data:`GLOBAL`.
        bucket: Duration bucket, or None.
        parent: The cell to shrink toward, or None for the global cell.
        k: Shrinkage strength, in episodes.
        draws: Bootstrap resamples.
        seed: Bootstrap seed.

    Returns:
        RecoveryCell: Fitted, shrunk and adjudicated.
    """
    episodes = _episode_count(rows)
    parent_params = (parent.a, parent.tau) if parent else (0.15, 1.0)
    parent_key = parent.key if parent else None

    if episodes < MIN_CELL_EPISODES:
        # Not enough to fit. Take the parent's numbers rather than inventing worse ones --
        # a two-parameter exponential on twelve episodes is a curve through noise.
        return RecoveryCell(
            body_part=body_part, duration_bucket=bucket,
            a=parent_params[0], tau=parent_params[1],
            a_sd=parent.a_sd if parent else float("nan"),
            tau_sd=parent.tau_sd if parent else float("nan"),
            episodes=episodes, rows=rows.height, parent=parent_key,
            shrunk_from="parent",
            abstained=parent.abstained if parent else True,
            reason=(f"{episodes} episodes is under the {MIN_CELL_EPISODES} a cell needs; "
                    f"using {parent_key or 'the default'}"))

    own = fit_curve(observed(rows, control), start=parent_params)
    a_sd, tau_sd = _bootstrap(rows, control, own, draws, seed=seed)
    a, tau = (_shrink(own, parent_params, episodes, k) if parent else own)

    if episodes < MIN_PARENT_EPISODES and parent is None:
        return RecoveryCell(body_part, bucket, a, tau, a_sd, tau_sd, episodes,
                            rows.height, parent_key, "own", True,
                            f"only {episodes} episodes in the whole pool")

    # The abstention rule. A shortfall inside two bootstrap standard errors has not been
    # measured, and shipping it as a multiplier would dress noise as a decision.
    abstained = bool(not math.isnan(a_sd) and a < 2 * a_sd)
    reason = ("" if not abstained else
              f"shortfall {a:.3f} is inside two standard errors ({a_sd:.3f})")
    return RecoveryCell(body_part, bucket, a, tau, a_sd, tau_sd, episodes,
                        rows.height, parent_key, "own", abstained, reason)


def _combine(part: RecoveryCell, bucket: RecoveryCell, root: RecoveryCell,
             body_part: str, duration_bucket: str, episodes: int,
             rows: int) -> RecoveryCell:
    """Build a body-part x duration cell from its two parents instead of from its data.

    **Why this exists.** Body part and duration are the two strongest severity signals and
    each is well powered on its own -- 19-77 episodes per body part, 48-157 per duration
    bucket -- but their *joint* cells are not: the largest is 45 episodes and most hold
    6-25. Fitting them directly means every one falls back to a single parent and one of
    the two signals is discarded. Measured at the global level the duration effect is
    unmistakable and is mostly in ``tau`` rather than ``a``: 0.67, 0.72, 1.50, 6.82 across
    one, two, three-to-four and five-plus games missed. A longer absence takes longer to
    shake off, and a ``knee`` cell that ignores that treats a one-week knee like a
    six-week one.

    So the joint cell is the additive-in-transformed-space combination:

        theta(part, bucket) = theta(part) + theta(global, bucket) - theta(global)

    which is the interaction-free reading -- each dimension contributes its own deviation
    from the root, and the cell reduces to either parent when the other has no deviation to
    contribute. It assumes no interaction, which 25 episodes cannot test and therefore
    should not be allowed to assert.

    Uncertainty adds in quadrature, and the cell **abstains only if both parents do**: "no
    measurable effect for concussions in general" and "three-to-four-game absences cost
    19%" are different claims, and a concussion that cost four games is not the average
    concussion.

    Args:
        part: The body-part cell.
        bucket: The global-level duration cell.
        root: The global cell both deviate from.
        body_part: Lexicon group.
        duration_bucket: Duration bucket.
        episodes: Episodes actually in the joint cell, recorded but not fitted on.
        rows: Appearances in it.

    Returns:
        RecoveryCell: The combined cell.
    """
    theta = _pack(part.a, part.tau) + _pack(bucket.a, bucket.tau) - _pack(root.a, root.tau)
    a, tau = _unpack(theta)
    a_sd = float(np.sqrt(np.nansum([part.a_sd ** 2, bucket.a_sd ** 2])))
    tau_sd = float(np.sqrt(np.nansum([part.tau_sd ** 2, bucket.tau_sd ** 2])))
    return RecoveryCell(
        body_part=body_part, duration_bucket=duration_bucket, a=a, tau=tau,
        a_sd=a_sd, tau_sd=tau_sd, episodes=episodes, rows=rows,
        parent=f"{part.key} + {bucket.key}", shrunk_from="combined",
        abstained=bool(part.abstained and bucket.abstained),
        reason=(f"{episodes} episodes is under the {MIN_CELL_EPISODES} a joint cell needs; "
                f"combined from {part.key} and {bucket.key}"))


def _fittable(post: pl.DataFrame) -> pl.DataFrame:
    """Post-return rows the curve is allowed to see.

    Excludes the groups with no recovery mechanism behind them: an illness costs
    availability and nothing else, and ``other`` is a bucket -- undisclosed injuries, a
    kidney, a shin -- whose members share no timetable.
    """
    return post.filter(~pl.col("body_part").is_in(list(
        lexicon.RECOVERY_EXCLUDED_GROUPS)))


def fit(post: pl.DataFrame, controls: pl.DataFrame, episodes: pl.DataFrame,
        k: float = 20.0, draws: int = BOOTSTRAPS, seed: int = 0,
        espn: Optional[EspnRamp] = None,
        train_seasons: Sequence[int] = ()) -> InjuryModel:
    """Fit the whole model: control curve, cell hierarchy, hazard.

    Args:
        post: :func:`Scripts.injury.episodes.post_return` output.
        controls: :func:`Scripts.injury.episodes.control_cohort` output.
        episodes: :func:`Scripts.injury.episodes.recurrence` output.
        k: Shrinkage strength, in episodes.
        draws: Bootstrap resamples. Lower it in a walk-forward, where the fit runs once
            per fold and the standard errors are not what is being scored.
        seed: Bootstrap seed.
        espn: What ESPN already prices, from gate G-B0.
        train_seasons: Seasons the fit saw, for the staleness check.

    Returns:
        InjuryModel: Fitted and ready to save.
    """
    control = fit_control(controls)
    usable = _fittable(post)

    cells: Dict[str, RecoveryCell] = {}
    root = _fit_cell(usable, control, GLOBAL, None, None, k, draws, seed)
    cells[root.key] = root

    # Duration first, at the global level, where it is well powered. Fitting it only
    # inside body parts would throw the signal away -- see :func:`_combine`.
    buckets: Dict[str, RecoveryCell] = {}
    for bucket in sorted(usable["duration_bucket"].unique().to_list()):
        rows = usable.filter(pl.col("duration_bucket") == bucket)
        cell = _fit_cell(rows, control, GLOBAL, bucket, root, k, draws, seed)
        buckets[bucket] = cell
        cells[cell.key] = cell

    for part in sorted(usable["body_part"].unique().to_list()):
        part_rows = usable.filter(pl.col("body_part") == part)
        part_cell = _fit_cell(part_rows, control, part, None, root, k, draws, seed)
        cells[part_cell.key] = part_cell

        for bucket in sorted(part_rows["duration_bucket"].unique().to_list()):
            bucket_rows = part_rows.filter(pl.col("duration_bucket") == bucket)
            # Named for the cell, not `episodes` -- that is this function's frame of
            # episode data and shadowing it silently passed an int to the hazard fit.
            joint = _episode_count(bucket_rows)
            if joint >= MIN_CELL_EPISODES:
                cell = _fit_cell(bucket_rows, control, part, bucket, part_cell, k,
                                 draws, seed)
            else:
                cell = _combine(part_cell, buckets.get(bucket, root), root, part,
                                bucket, joint, bucket_rows.height)
            cells[cell.key] = cell

    return InjuryModel(
        cells=cells,
        control=control,
        hazard=fit_hazard(person_periods(episodes)),
        espn=espn or EspnRamp(),
        body_part_map=lexicon.as_dict(),
        shrinkage={"k": k, "grid": list(SHRINKAGE_GRID), "bootstraps": draws},
        train_seasons=tuple(sorted(train_seasons)),
        fitted_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        git_sha=ep._git_sha(),
    )


def measure_espn_ramp(episodes: pl.DataFrame, seasons: Sequence[int] = (2025,),
                      baseline: int = ep.BASELINE_WINDOW,
                      appearances: int = 3) -> EspnRamp:
    """Gate G-B0: how much of the drop ESPN's own weekly projection already carries.

    Reads the stored weekly ``lineups`` artifacts, which are the only place ESPN's
    ``projPoints`` is kept beside actual ``points``. Same ratio-of-sums estimator as the
    curve, and the same placebo logic -- a healthy control drawn from players who never
    appear in an episode, so ESPN's own tendency to project a regression is netted out
    rather than counted as injury pricing.

    Args:
        episodes: :func:`Scripts.injury.episodes.recurrence` output.
        seasons: Seasons with a stored ``lineups`` artifact.
        baseline: Appearances of baseline.
        appearances: How many appearances back to measure.

    Returns:
        EspnRamp: What ESPN prices, per appearance. Empty when no artifact is readable,
        in which case :meth:`InjuryModel.multiplier` nets against nothing.
    """
    import glob

    from Scripts import crosswalk

    frames: List[pl.DataFrame] = []
    for season in seasons:
        for path in sorted(glob.glob(str(paths.DATA_DIR / "Store" / str(season)
                                         / "*" / "lineups.parquet"))):
            try:
                frame = pl.read_parquet(path, columns=["week", "player_id",
                                                       "projPoints", "points"])
            except Exception:                     # noqa: BLE001 -- an older artifact
                continue
            frames.append(frame.with_columns(
                pl.col("week").cast(pl.Int32),
                pl.col("player_id").cast(pl.Utf8),
                pl.lit(int(season)).cast(pl.Int32).alias("season"),
                pl.lit(Path(path).parent.name).alias("league")))
    if not frames:
        return EspnRamp()

    ids = (pl.read_parquet(paths.PLAYER_IDS_PARQUET, columns=["espn_id", "gsis_id"])
           .drop_nulls().unique())
    ambiguous = ids.group_by("espn_id").len().filter(pl.col("len") > 1)["espn_id"]
    ids = ids.filter(~pl.col("espn_id").is_in(ambiguous))

    lineups = (pl.concat(frames)
               .join(ids, left_on="player_id", right_on="espn_id", how="inner")
               .unique(subset=["league", "season", "week", "gsis_id"], keep="first")
               .filter(pl.col("projPoints").is_not_null()
                       & (pl.col("projPoints") > 0)))
    if lineups.is_empty():
        return EspnRamp()

    def ratios(keys: pl.DataFrame) -> Dict[int, Tuple[float, float]]:
        joined = lineups.join(keys, on=["season", "gsis_id"], how="inner")
        base = (joined.filter((pl.col("week") < pl.col("cut"))
                              & (pl.col("week") >= pl.col("cut") - baseline))
                .group_by(["league", "season", "gsis_id", "cut"])
                .agg(pl.col("projPoints").mean().alias("base_proj"),
                     pl.col("points").mean().alias("base_pts"),
                     pl.len().alias("n_base")))
        after = (joined.join(base, on=["league", "season", "gsis_id", "cut"],
                             how="inner")
                 .filter((pl.col("n_base") >= ep.MIN_BASELINE_APPEARANCES)
                         & (pl.col("base_proj") >= ep.MIN_BASELINE_POINTS)
                         & (pl.col("week") >= pl.col("resume")))
                 .sort(["league", "season", "gsis_id", "cut", "week"]))
        after = after.with_columns(
            (pl.int_range(pl.len()).over(["league", "season", "gsis_id", "cut"]) + 1)
            .alias("back"))
        summed = (after.filter(pl.col("back") <= appearances).group_by("back")
                  .agg(pl.col("projPoints").sum().alias("proj"),
                       pl.col("base_proj").sum().alias("base_proj"),
                       pl.col("points").sum().alias("pts"),
                       pl.col("base_pts").sum().alias("base_pts")))
        return {r["back"]: (r["proj"] / r["base_proj"], r["pts"] / r["base_pts"])
                for r in summed.iter_rows(named=True)
                if r["base_proj"] and r["base_pts"]}

    returned = episodes.filter((pl.col("outcome") == "returned")
                               & pl.col("season").is_in(list(seasons)))
    injured = ratios(returned.select([
        pl.col("season"), pl.col("gsis_id"),
        pl.col("first_out_week").alias("cut"),
        pl.col("return_week").alias("resume")]))

    hurt = set(returned["gsis_id"].to_list())
    clean = (lineups.filter(~pl.col("gsis_id").is_in(list(hurt)))
             .select(["season", "gsis_id"]).unique())
    control: Dict[int, List[Tuple[float, float]]] = {}
    for anchor in range(baseline + 1, 15):
        keys = clean.with_columns(pl.lit(anchor).cast(pl.Int32).alias("cut"),
                                  pl.lit(anchor).cast(pl.Int32).alias("resume"))
        for back, pair in ratios(keys).items():
            control.setdefault(back, []).append(pair)

    by_appearance: Dict[str, float] = {}
    for back, (proj, _) in sorted(injured.items()):
        pairs = control.get(back)
        if not pairs:
            continue
        control_proj = float(np.mean([p for p, _ in pairs]))
        if control_proj:
            by_appearance[str(back)] = proj / control_proj

    covered = (returned.join(lineups.select(["season", "gsis_id"]).unique(),
                             on=["season", "gsis_id"], how="inner")
               .select(["season", "gsis_id", "run"]).unique().height)
    return EspnRamp(by_appearance=by_appearance, episodes=covered,
                    seasons=sorted(int(s) for s in seasons))


def describe(model: InjuryModel) -> str:
    """A printable account of the fitted model."""
    lines = [f"  version {model.version}, fitted {model.fitted_at}",
             f"  trained on {list(model.train_seasons)}",
             f"  control cohort: {model.control.rows} appearances, "
             f"{len(model.control.by_stratum)} strata"]

    lines.append("")
    lines.append(f"  {'cell':24s}{'eps':>5}{'a':>8}{'a sd':>7}{'tau':>6}  "
                 f"{'from':9s} multiplier by appearance back")
    for key in sorted(model.cells,
                      key=lambda k: (model.cells[k].body_part != GLOBAL,
                                     model.cells[k].body_part,
                                     model.cells[k].duration_bucket or "")):
        cell = model.cells[key]
        ramp = " ".join(f"{x:.2f}" for x in
                        model.ladder(cell.body_part, cell.duration_bucket))
        mark = "  ABSTAIN" if cell.abstained else ""
        lines.append(f"  {key:24s}{cell.episodes:5d}{cell.a:8.3f}{cell.a_sd:7.3f}"
                     f"{cell.tau:6.2f}  {cell.shrunk_from:9s} {ramp}{mark}")

    at_cap = [k for k, c in model.cells.items() if c.tau >= MAX_TAU - 1e-6]
    if at_cap:
        lines.append(f"  {len(at_cap)} cell(s) sit at the tau cap of {MAX_TAU:.0f} "
                     f"appearances -- the window cannot resolve a slower recovery.")

    hazard = model.hazard
    lines.append("")
    lines.append(f"  hazard: {hazard.weeks_at_risk} weeks at risk, base rate "
                 f"{hazard.base_rate:.4f}, Brier {hazard.brier:.5f} against "
                 f"{hazard.base_brier:.5f} for the constant "
                 f"(ratio {hazard.brier / hazard.base_brier:.4f})"
                 if hazard.base_brier else "  hazard: not fitted")
    for part in sorted(hazard.by_body_part):
        lines.append(f"    {part:20s} P(recurs in {HAZARD_WINDOW} wks | 3 games out) = "
                     f"{model.reinjury_probability(part, 3.0):.3f}")

    if model.espn.by_appearance:
        lines.append("")
        lines.append(f"  ESPN already prices (G-B0, {model.espn.episodes} episodes in "
                     f"{model.espn.seasons}):")
        lines.append("    " + "  ".join(
            f"a{k}={v:.3f}" for k, v in sorted(model.espn.by_appearance.items())))
        lines.append("    Weekly residual (fitted / priced), global cell: " + " ".join(
            f"{model.multiplier(GLOBAL, w, net_of_espn=True):.2f}"
            for w in range(1, WINDOW + 1)))
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--fit", action="store_true", help="Fit and save the model.")
    parser.add_argument("--show", action="store_true",
                        help="Describe the saved model.")
    parser.add_argument("--k", type=float, default=20.0,
                        help="Shrinkage strength, in episodes.")
    parser.add_argument("--draws", type=int, default=BOOTSTRAPS,
                        help="Bootstrap resamples.")
    args = parser.parse_args(argv)

    if args.fit:
        print(f"\n===== Injury model {MODEL_VERSION} =====")
        post = ep.load_post_return()
        controls = ep.load_controls()
        episodes = ep.load_episodes()
        seasons = sorted(episodes["season"].unique().to_list())
        espn = measure_espn_ramp(episodes)
        model = fit(post, controls, episodes, k=args.k, draws=args.draws,
                    espn=espn, train_seasons=seasons)
        print(describe(model))
        print(f"\n  wrote {model.save()}")
        return 0

    if args.show:
        print(describe(InjuryModel.load()))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
