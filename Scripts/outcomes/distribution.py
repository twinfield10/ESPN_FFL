"""Season **points** as a distribution, from the per-stat marginals.

Every stat the season head projects arrives with a fitted predictive distribution
(:mod:`Scripts.usage.predictive`) and games played has one
(:mod:`Scripts.usage.availability`). The one number a drafter actually reads -- season
points in his league's rules -- has a point estimate and a *disagreement* band, and
``grep -rn "points_low\\|Points_sd\\|pts_sd" Scripts app`` returns nothing. This module
is the missing aggregation.

**Why it is a simulation when everything upstream went out of its way to stay closed
form.** Season points is a league-weighted sum of eight correlated stats, two of which
are discrete, one of which carries a point mass at zero, and -- once
:mod:`Scripts.outcomes.simulate` gets hold of it -- all of which sit under a shared
availability draw with a non-linear redistribution rule. There is no incomplete beta for
that. What *is* preserved is the marginals: each stat is sampled by pushing a uniform
through :func:`Scripts.usage.predictive.quantile`, the same function that produces the
published ``USG_<stat>_low``/``_high``, so a sampled decile and a printed decile cannot
disagree.

**The dependence is a Gaussian copula and the reason is arithmetic.** Summing
independent marginals understates a sum's spread by exactly the covariance, and the
covariance here is not small: a receiver who beats his target projection beats his
receiving-yard projection by the *same event*, not by a second one.
:func:`Scripts.usage.season._fit_stat_correlation` measures it on the probability scale
so imposing it leaves every marginal exactly where it was fitted.

**Which of three quantities this is.** ``USG_<stat>`` carries ``expected_games`` inside
it, so the dispersion fitted against it is the spread of the season a player *realises*,
availability included. That is the right object for a drafter and the wrong one for a
simulation that draws availability itself -- see
:attr:`Scripts.usage.season.SeasonUsageModel.stat_dispersion_conditional`. Both bases are
supported here and the caller must say which it means; there is no default, because
getting it wrong is invisible in the output and doubles the availability variance.

Usage::

    from Scripts.outcomes import distribution as dist

    spec = dist.player_spec(frame, model, conditional=False)
    sample = dist.sample_stats(spec, rng=np.random.default_rng(28), n_sims=5000)
    points = dist.season_points(sample, weights)      # (n_sims, n_players)
    dist.summarise(points, positions=spec.positions)

See ``docs/plans/28-outcome-distributions.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Tuple

import numpy as np
import polars as pl
from scipy import stats

from Scripts.usage import predictive as pv
from Scripts.usage import season as sn

#: Stats aggregated into points, in a fixed order.
#:
#: Pinned to :data:`Scripts.usage.season.STAT_OUTCOMES` rather than restated, so a stat
#: added to the model cannot be silently left out of its own points total. The order is
#: load-bearing: it indexes the correlation matrices and the sample's last axis.
STAT_ORDER: Tuple[str, ...] = tuple(sn.STAT_OUTCOMES)

#: Draws per player, unless a caller says otherwise.
#:
#: Sized from what the output is read at. The extreme quantile published is p10/p90, and
#: the Monte Carlo standard error of a p90 at n draws is roughly
#: ``sqrt(0.9 * 0.1 / n) / f(p90)``; at 5,000 draws that is under a fifth of a point on a
#: 150-point projection, comfortably inside the width of the digit the board prints.
#: ``p_top12`` is a proportion and is tighter still. Ten times this would buy a decimal
#: nobody reads and cost ten times the runtime across nine leagues.
DEFAULT_SIMS: int = 5000

#: Whether the shipped distribution splits its dispersion by plan 33's cohort.
#:
#: **False, and it is a measured rejection rather than a default.** Plan 33 phase 3
#: predicted that cohort is the axis the interval fails to vary along: a settled QB1 is
#: nearly certain and a mover TE2 is a coin flip, and the board showed both at about 9%.
#: The residuals agree that cohort matters -- a rookie's coefficient of variation is
#: 1.6x-2.3x a settled player's. Splitting the fitted dispersion on it is worth
#: **-0.1pp** of coverage walk-forward, and **-1.3pp** for the backups it was most
#: expected to help.
#:
#: The reason is the mean-variance function it was competing with. ``Var = phi * mu +
#: mu^2 / k`` already gives a proportionally wider interval at a smaller projection, and a
#: rookie's projection *is* smaller -- 182 rushing yards against a settled back's 382. So
#: most of the cohort effect was a level effect the two-parameter form already absorbed,
#: and splitting re-fits it on a third of the rows over a narrower range of mu.
#:
#: Measured per cohort, the interval was never failing along that axis: rookies covered
#: **0.797** against nominal 0.800, while *settled* players sat at 0.702. The premise had
#: the sign backwards. Cells are still fitted and persisted, so the finding is
#: reproducible; they are not used.
COHORT_DISPERSION: bool = False

#: Fixed seed for the shipped artifact.
#:
#: A board that changed between two builds of identical inputs would be indistinguishable
#: from a board that changed because the market moved, and telling those apart is most of
#: what the nightly snapshot is for.
DEFAULT_SEED: int = 28

_EPS = 1e-9


@dataclass(frozen=True)
class PlayerSpec:
    """Everything needed to sample one frame of players.

    Attributes:
        gsis_id: Player ids, length ``n_players``.
        positions: Position per player, same length.
        mu: ``(n_players, len(STAT_ORDER))`` projected means. NaN where the model
            projects the player no such stat, which is not the same as zero and is
            carried through to a null rather than to a draw of nothing.
        phi: Matching Poisson-like dispersion coefficients.
        k: Matching shape coefficients.
        bust: Matching zero point masses.
        correlation: Position to ``(len(STAT_ORDER), len(STAT_ORDER))`` correlation, with
            an identity for any position the model did not fit.
        conditional: Whether ``phi``/``k``/``bust`` are the games-conditional
            dispersions. Recorded rather than inferred so a caller cannot mix bases by
            accident.
        used_cohort: Per (player, stat), whether a cohort-specific dispersion was found
            or the pooled one stood in.
    """

    gsis_id: Tuple[str, ...]
    positions: Tuple[str, ...]
    mu: np.ndarray
    phi: np.ndarray
    k: np.ndarray
    bust: np.ndarray
    correlation: Dict[str, np.ndarray]
    conditional: bool
    used_cohort: Optional[np.ndarray] = None

    @property
    def cohort_share(self) -> float:
        """Share of fitted cells taking a cohort-specific dispersion rather than pooled.

        Reported rather than assumed: a split that binds on 10% of cells is a different
        claim from one that binds on 90%, and only the number says which this is.
        """
        if self.used_cohort is None:
            return 0.0
        fitted = np.isfinite(self.mu)
        return float(self.used_cohort[fitted].mean()) if fitted.any() else 0.0

    @property
    def n_players(self) -> int:
        """How many players this spec covers."""
        return len(self.gsis_id)

    @property
    def has_projection(self) -> np.ndarray:
        """Per player, whether any stat has both a projection and a fitted dispersion.

        The distinction :mod:`Scripts.usage.backtest` calls ``spoke``: a player the model
        declined to project must come out null, not zero. Without the guard an abstention
        scores 0.0 and reports perfect coverage.
        """
        return np.isfinite(self.mu).any(axis=1)


def _dispersions(model: sn.SeasonUsageModel, conditional: bool
                 ) -> Dict[str, Dict[str, float]]:
    """The dispersion block this run is built on.

    Args:
        model: Fitted season model.
        conditional: Take the games-conditional block.

    Returns:
        dict: :func:`Scripts.usage.predictive.key` to coefficients.

    Raises:
        ValueError: When the conditional block was asked for and the model has none --
            a 1.1.0 file, or a fit whose holdout lacked ``y_games``. Falling back to the
            unconditional block silently would double-count availability downstream,
            which is exactly the failure this parameter exists to prevent.
    """
    if not conditional:
        return model.stat_dispersion
    if not model.stat_dispersion_conditional:
        raise ValueError(
            f"model {model.version} carries no stat_dispersion_conditional; refit with "
            f"Scripts.usage.season >= 1.2.0 before asking for a conditional basis. "
            f"Falling back to the unconditional block would count availability twice.")
    return model.stat_dispersion_conditional


def player_spec(frame: pl.DataFrame, model: sn.SeasonUsageModel,
                conditional: bool,
                mu_columns: Optional[Dict[str, str]] = None,
                cohort_column: str = "usg_role_cohort",
                use_cohort: Optional[bool] = None) -> PlayerSpec:
    """Assemble the sampling parameters for a prediction frame.

    Args:
        frame: Any frame carrying ``gsis_id``, ``position`` and one mean column per
            stat -- the output of :meth:`Scripts.usage.season.SeasonUsageModel.predict`,
            or of :func:`Scripts.usage.backtest.run_season`.
        model: The fitted model the frame came from.
        conditional: Use the games-conditional dispersions. See the module docstring;
            there is deliberately no default.
        mu_columns: Stat to the column holding its mean. Defaults to ``USG_<stat>``.
        cohort_column: Where plan 33's cohort lives. Absent, every player takes the
            pooled dispersion, which is the behaviour before phase 3.
        use_cohort: Split the dispersion by cohort. None takes
            :data:`COHORT_DISPERSION`, which is False because G-R2 measured the split at
            -0.1pp. Pass it explicitly to run either arm of that comparison.

    Returns:
        PlayerSpec: Ready for :func:`sample_stats`.
    """
    columns = mu_columns or {stat: f"{sn.USAGE_PREFIX}{stat}" for stat in STAT_ORDER}
    dispersion = _dispersions(model, conditional)
    positions = frame["position"].to_list()
    n, width = frame.height, len(STAT_ORDER)

    mu = np.full((n, width), np.nan)
    phi = np.full((n, width), np.nan)
    k = np.full((n, width), np.nan)
    bust = np.zeros((n, width))

    # Plan 33 phase 3: cohort is a real axis of this spread, and until now the interval
    # did not vary along it at all. A rookie's residual CV is 1.6x-2.3x a settled
    # player's, so the *same* projection is far less knowable for one than the other.
    # Where a cohort cell was too thin to fit, the pooled cell stands in -- visibly, via
    # `cohort_cells`, rather than by silently pretending the split happened.
    split = COHORT_DISPERSION if use_cohort is None else use_cohort
    cohorts = (frame[cohort_column].to_list()
               if split and cohort_column in frame.columns
               else [None] * frame.height)
    used_cohort = np.zeros((n, width), dtype=bool)

    for index, stat in enumerate(STAT_ORDER):
        column = columns.get(stat)
        if column is None or column not in frame.columns:
            continue
        values = frame[column].cast(pl.Float64).to_numpy()
        for position in set(positions):
            for cohort in sorted(set(cohorts), key=lambda c: (c is None, c)):
                # The cohort cell when one was fitted, the pooled cell when it was not.
                # A thin cohort must not lose its interval altogether: partial coverage
                # is visible, a missing interval reads as the model declining to speak.
                coefficients = dispersion.get(pv.key(position, stat, cohort))
                split = coefficients is not None and cohort is not None
                if coefficients is None:
                    coefficients = dispersion.get(pv.key(position, stat))
                if coefficients is None:
                    # No fitted dispersion at all means no interval, which is what
                    # `stat_intervals` already does. An invented number is not.
                    continue
                rows = np.array([p == position for p in positions])
                if cohort is not None:
                    rows = rows & np.array([c == cohort for c in cohorts])
                keep = rows & np.isfinite(values) & (values > 0)
                if not keep.any():
                    continue
                mu[keep, index] = values[keep]
                phi[keep, index] = coefficients["phi"]
                k[keep, index] = coefficients["k"]
                bust[keep, index] = coefficients.get("bust", 0.0)
                used_cohort[keep, index] = split

    return PlayerSpec(
        gsis_id=tuple(frame["gsis_id"].to_list()),
        positions=tuple(positions),
        mu=mu, phi=phi, k=k, bust=bust,
        correlation=correlation_matrices(model),
        conditional=conditional,
        used_cohort=used_cohort,
    )


def correlation_matrices(model: sn.SeasonUsageModel) -> Dict[str, np.ndarray]:
    """The fitted correlations, expanded onto :data:`STAT_ORDER`.

    The model stores only the stats a position actually accumulates -- a receiver has no
    pass-attempt row -- so the stored matrices differ in size and order between positions.
    This lifts each onto the full stat axis, with an identity everywhere the fit had
    nothing to say, which is the correct null: uncorrelated.

    Args:
        model: Fitted season model.

    Returns:
        dict: Position to a ``(len(STAT_ORDER), len(STAT_ORDER))`` matrix.
    """
    index = {stat: i for i, stat in enumerate(STAT_ORDER)}
    out: Dict[str, np.ndarray] = {}
    for position, payload in (model.stat_correlation or {}).items():
        stored = list(payload.get("stats", []))
        matrix = np.asarray(payload.get("matrix", []), dtype=float)
        full = np.eye(len(STAT_ORDER))
        for a, stat_a in enumerate(stored):
            for b, stat_b in enumerate(stored):
                if stat_a in index and stat_b in index:
                    full[index[stat_a], index[stat_b]] = matrix[a, b]
        out[position] = full
    return out


def _cholesky(matrix: np.ndarray) -> np.ndarray:
    """Lower-triangular factor, with a jitter fallback.

    ``_nearest_correlation`` already makes the stored matrices positive semi-definite,
    but semi-definite is not definite -- a perfectly collinear pair gives a zero
    eigenvalue and ``cholesky`` raises on it. The jitter is the smallest thing that turns
    a semi-definite matrix into a definite one and moves no correlation by an amount
    anything downstream can see.
    """
    for jitter in (0.0, 1e-10, 1e-8, 1e-6, 1e-4):
        try:
            return np.linalg.cholesky(matrix + jitter * np.eye(matrix.shape[0]))
        except np.linalg.LinAlgError:
            continue
    return np.eye(matrix.shape[0])


def sample_stats(spec: PlayerSpec, rng: np.random.Generator,
                 n_sims: int = DEFAULT_SIMS,
                 mu_scale: Optional[np.ndarray] = None) -> np.ndarray:
    """Draw ``n_sims`` season stat lines per player.

    Inverse-transform sampling through :func:`Scripts.usage.predictive.quantile`, with
    the uniforms coupled across stats by a Gaussian copula. Because the marginal is
    applied last, every stat's sampled distribution is exactly the one the model fitted
    -- the copula changes only which draws land together.

    Args:
        spec: From :func:`player_spec`.
        rng: Explicit generator. There is no default and no module-level seeding: this
            runs once per league across nine leagues, which is precisely the situation
            where a global ``np.random.seed`` stops being reproducible.
        n_sims: Draws per player.
        mu_scale: The seam :mod:`Scripts.outcomes.simulate` uses to feed a room's
            availability draw and vacancy transfer in. Either an
            ``(n_sims, n_players)`` array applied to every stat alike, or anything with a
            ``scale(position, stat)`` method returning one -- which is what
            :class:`Scripts.outcomes.simulate.Modulation` provides, so the fitted games
            elasticity can differ by stat. Applied to the **mean**, not to the sample, so
            the dispersion is re-evaluated at the modulated mean rather than scaled
            around the original one.

    Returns:
        np.ndarray: ``(n_sims, n_players, len(STAT_ORDER))``, NaN wherever the player has
        no projection for that stat.
    """
    n_players, width = spec.mu.shape
    out = np.full((n_sims, n_players, width), np.nan)

    for position in sorted(set(spec.positions)):
        rows = np.flatnonzero(np.array([p == position for p in spec.positions]))
        if not rows.size:
            continue
        factor = _cholesky(spec.correlation.get(position, np.eye(width)))
        # One correlated normal per (sim, player, stat), then to uniforms. Drawn for
        # every stat even where the player has none, so that a receiver and a
        # quarterback consume the stream identically and the result does not depend on
        # who happens to be in the frame.
        normals = rng.standard_normal((n_sims, rows.size, width)) @ factor.T
        uniforms = stats.norm.cdf(normals)

        for index in range(width):
            usable = rows[np.isfinite(spec.mu[rows, index])]
            if not usable.size:
                continue
            mu = np.repeat(spec.mu[usable, index][None, :], n_sims, axis=0)
            if mu_scale is not None:
                factor = (mu_scale.scale(position, STAT_ORDER[index])
                          if hasattr(mu_scale, "scale") else mu_scale)
                mu = mu * factor[:, usable]
            mu = np.clip(mu, _EPS, None)
            # phi, k and bust vary only by position, so one call covers the block.
            take = np.searchsorted(rows, usable)
            out[:, usable, index] = pv.quantile(
                STAT_ORDER[index], mu,
                float(spec.phi[usable[0], index]),
                float(spec.k[usable[0], index]),
                uniforms[:, take, index],
                bust=float(spec.bust[usable[0], index]))
    return out


def season_points(sample: np.ndarray, weights: Dict[str, float]) -> np.ndarray:
    """Score a stat sample in one league's rules.

    The scoring map is a strict dot product -- :func:`Scripts.projection_utils._apply_scoring`
    sums ``points x stat`` with no thresholds or interactions -- so this is a matrix
    multiply, and the same sample can be scored for all nine leagues without redrawing.
    That is what makes "nine leagues, nine distributions, one model" literally true.

    A stat the sample has no draw for scores zero, matching ``_apply_scoring``'s
    treatment of a partial line. Whether the *player* spoke at all is
    :attr:`PlayerSpec.has_projection`'s job, not this function's.

    Args:
        sample: ``(n_sims, n_players, len(STAT_ORDER))`` from :func:`sample_stats`.
        weights: Stat to points per unit, from
            :func:`Scripts.usage.backtest.scoring_weights`.

    Returns:
        np.ndarray: ``(n_sims, n_players)`` season points.
    """
    vector = np.array([float(weights.get(stat, 0.0)) for stat in STAT_ORDER])
    return np.nan_to_num(sample, nan=0.0) @ vector


#: Position pool sizes ``p_top12`` is measured against.
#:
#: Pinned equal to :data:`Scripts.lab.run.TOP_N`, which is itself pinned to
#: :func:`Scripts.usage.backtest.report`. The column is named ``p_top12`` after the
#: quarterback and tight-end case because that is what a drafter says out loud; the
#: threshold is the position's own starter count in a twelve-team league, and using one
#: number for all four would ask whether a running back finishes top-12 among a pool
#: three times the size.
TOP_N: Dict[str, int] = {"QB": 12, "RB": 24, "WR": 36, "TE": 12}

#: Share of a player's own projection below which a season counts as a bust.
#:
#: Half, and it is a threshold on the *ratio* rather than on points so it means the same
#: thing to a first-round back and a bench receiver. Measured against the simulated mean
#: rather than the published projection so ``p_bust`` describes the distribution actually
#: drawn and cannot be moved by a rescaling somewhere else in the pipeline.
BUST_SHARE: float = 0.5


def summarise(points: np.ndarray, positions: Sequence[str],
              has_projection: Optional[np.ndarray] = None,
              top_n: Optional[Dict[str, int]] = None) -> pl.DataFrame:
    """Reduce a points sample to the columns a board can carry.

    Args:
        points: ``(n_sims, n_players)`` from :func:`season_points`.
        positions: Position per player.
        has_projection: Per player, whether the model spoke. Rows where it did not come
            out null rather than zero.
        top_n: Position to pool size for ``p_top12``. Defaults to :data:`TOP_N`.

    Returns:
        pl.DataFrame: ``pts_p10``, ``pts_p50``, ``pts_p90``, ``pts_mean``, ``pts_sd``,
        ``p_top12`` and ``p_bust``, one row per player in input order.
    """
    thresholds = top_n or TOP_N
    n_sims, n_players = points.shape
    spoke = (np.ones(n_players, dtype=bool) if has_projection is None
             else np.asarray(has_projection, dtype=bool))

    quantiles = np.percentile(points, [10, 50, 90], axis=0)
    mean = points.mean(axis=0)

    # `p_top12` is a *within-simulation* rank: in each drawn season, is he one of the
    # best n at his position? Ranking the summary quantiles instead would answer a
    # different and much less useful question -- whether his median beats their medians --
    # and would give every player the same answer his mean already gives.
    top = np.zeros(n_players)
    for position in set(positions):
        rows = np.flatnonzero(np.array([p == position for p in positions]) & spoke)
        if not rows.size:
            continue
        n = min(int(thresholds.get(position, 12)), rows.size)
        block = points[:, rows]
        cut = -np.sort(-block, axis=1)[:, n - 1][:, None]
        top[rows] = (block >= cut).mean(axis=0)

    bust = (points < BUST_SHARE * np.where(mean > 0, mean, np.inf)).mean(axis=0)

    def column(values: np.ndarray, name: str) -> pl.Series:
        # NaN -> null, never NaN. `season.py`:722 records what a stray float NaN did the
        # last time: it survived `is_not_null()`, failed every comparison silently, and
        # put measured interval coverage at 6%.
        return pl.Series(name, np.where(spoke, values, np.nan)).fill_nan(None)

    return pl.DataFrame([
        column(quantiles[0], "pts_p10"),
        column(quantiles[1], "pts_p50"),
        column(quantiles[2], "pts_p90"),
        column(mean, "pts_mean"),
        column(points.std(axis=0), "pts_sd"),
        column(top, "p_top12"),
        column(bust, "p_bust"),
    ])
