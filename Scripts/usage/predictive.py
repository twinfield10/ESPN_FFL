"""Closed-form predictive distributions for the projected stat lines.

:mod:`Scripts.usage.availability` gives games played a distribution. This does the
same for the stats themselves, so ``USG_receivingYards`` arrives with an interval
rather than as a bare number.

**Fitted end-to-end, not composed from parts, and that is a measurement rather than a
convenience.** The obvious design is to model the three factors separately -- games,
per-game volume, per-opportunity rate -- and multiply, since the model already
decomposes that way and the product of independent variables has an exact
coefficient-of-variation identity. Independence is what fails:

| pair | correlation |
|---|---|
| games vs per-game volume | **+0.48 to +0.63** |
| total opportunities vs per-opportunity rate | +0.17 to +0.37 |

Both are positive, and the first is large: a player who misses games also loses
touches in the games he plays, because those are the same underlying loss of role.
Multiplying independent factors would therefore understate the spread systematically,
and backing a per-game volume variance out of an opportunity variance produced
*negative* numbers for quarterbacks -- the arithmetic complaining about the
assumption. Fitting each stat's dispersion directly on its own residuals absorbs
every correlation without needing to name any of them.

**Families are chosen by support, and by the same overdispersion evidence.**

- **Counts** -- receptions, touchdowns, interceptions -- get a **Negative Binomial**.
  It is the Gamma-Poisson mixture, the count analogue of the Beta-Binomial used for
  games, and it handles the zeros that a lognormal cannot. Opportunity counts are
  **13x to 99x** overdispersed relative to Poisson, so the Poisson it generalises is
  not a candidate.
- **Yardage** gets a **Gamma**: positive, right-skewed, and the realised
  per-opportunity rates are mildly right-skewed (+0.13 to +0.47) with a coefficient
  of variation of 0.18 to 0.23.

Both have closed-form quantiles -- regularised incomplete beta and gamma
respectively -- so nothing here is simulated.

**One finding worth carrying into how the output is read.** Conditional on the
opportunity count, the bounded rates are barely overdispersed at all: 1.08x to 1.79x
Binomial, against 5.6-8.1x for games and 13-99x for volume. Once you know how many
targets a player gets, his touchdown rate is close to pure sampling noise. Nearly all
the reducible uncertainty in a season projection is **how much work he gets**, not
what he does with it -- which is the same conclusion plan 16's stickiness table
reached from the other direction.
"""

from __future__ import annotations

from typing import Dict, Optional, Sequence, Tuple

import numpy as np
from scipy import optimize, stats

#: Stats modelled as counts, by the suffix of their ``USG_`` column.
COUNT_STATS: Tuple[str, ...] = (
    "receivingReceptions", "receivingTouchdowns",
    "rushingTouchdowns", "passingTouchdowns", "passingInterceptions",
)

#: Stats modelled as continuous yardage.
YARDAGE_STATS: Tuple[str, ...] = (
    "receivingYards", "rushingYards", "passingYards",
)

#: Rows a (position, stat) pair needs before its dispersion is fitted.
MIN_FIT_ROWS: int = 30

#: Largest zero point mass that will be fitted.
#:
#: A guard rather than a tuning knob: past this the stat is not a projection with a
#: bust tail, it is a stat the model should not be projecting at all, and a huge
#: point mass would make the interval meaningless rather than wide.
MAX_BUST: float = 0.5

#: Shape past which the quadratic term is treated as absent.
#:
#: ``k = 1 / coefficient`` diverges as the fitted quadratic coefficient approaches
#: zero, so data with no constant-CV component yields an enormous and unstable
#: number. Football data is nowhere near it -- fitted k is single digits.
MAX_SHAPE: float = 1000.0

_EPS = 1e-9


def family_for(stat: str) -> Optional[str]:
    """Which family a stat belongs to.

    Args:
        stat: Stat name without the ``USG_`` prefix.

    Returns:
        str | None: ``"count"``, ``"yardage"``, or None when the stat is neither.
    """
    if stat in COUNT_STATS:
        return "count"
    if stat in YARDAGE_STATS:
        return "yardage"
    return None


def fit_variance(observed: Sequence[float], predicted: Sequence[float],
                 family: Optional[str] = None
                 ) -> Optional[Tuple[float, float, float]]:
    """Fit ``Var(mu) = phi * mu + mu^2 / k`` by non-negative least squares.

    **Two parameters rather than one, because the coefficient of variation is not
    constant and assuming it was produced intervals half the width they should be.**
    Measured on held-out residuals, CV falls steeply as the projection grows:

    | WR receivingYards | mean mu | CV |
    |---|---|---|
    | bottom quartile | 49 | 1.90 |
    | second | 189 | 0.73 |
    | third | 398 | 0.58 |
    | top | 740 | 0.48 |

    RB rushing yards runs 3.75 down to 0.50 over the same span. A single-parameter
    Gamma has ``CV = 1/sqrt(k)`` everywhere, and a moment fit weights by ``mu^2``, so
    it lands on the top quartile and then applies that width to the bottom -- where
    the truth is four times wider. Measured coverage was 49-57% against a nominal
    80%.

    The two-term form fixes it because the terms dominate at opposite ends:
    ``phi * mu`` gives ``CV^2 ~ phi / mu``, large for small projections, and
    ``mu^2 / k`` gives ``CV^2 -> 1/k``, flat for large ones. It is the
    Negative-Binomial variance function with the Poisson term freed, which is also
    why the count stats calibrated better than yardage before this existed.

    Both coefficients are constrained non-negative: a negative one would mean
    variance shrinking with the projection, which is not a thing, and unconstrained
    least squares will happily return one on a thin position.

    Args:
        observed: Realised totals.
        predicted: Projected totals.

    Args (continued):
        family: ``"count"`` suppresses the zero point mass, since the Negative
            Binomial already carries one.

    Returns:
        tuple | None: ``(phi, k, bust)``, or None when there are too few usable rows.
    """
    x = np.asarray(observed, dtype=float)
    mu = np.asarray(predicted, dtype=float)
    keep = np.isfinite(x) & np.isfinite(mu) & (mu > _EPS)
    x, mu = x[keep], mu[keep]
    if x.size < MIN_FIT_ROWS:
        return None

    design = np.column_stack([mu, mu ** 2])
    coefficients, _ = optimize.nnls(design, (x - mu) ** 2)
    phi, inverse_k = float(coefficients[0]), float(coefficients[1])

    k = 1.0 / inverse_k if inverse_k > 1.0 / MAX_SHAPE else MAX_SHAPE
    # The share that produced literally nothing. A Gamma puts no mass at zero, and
    # this population does: 10.5% of receiving-yard rows realise exactly 0, and 59%
    # of the rows falling below the p10 produced under 5% of their projection. Those
    # are players who got hurt or lost the job, and without a point mass for them the
    # lower tail leaks at twice its nominal rate.
    #
    # Counts need none of this -- a Negative Binomial already carries mass at zero --
    # so it is fitted only where the family cannot express it.
    bust = 0.0 if family == "count" else float(np.mean(x <= 0.0))
    return max(phi, 0.0), min(k, MAX_SHAPE), min(bust, MAX_BUST)


def variance_at(mu, phi: float, k: float) -> np.ndarray:
    """Evaluate the fitted mean-variance function."""
    mu = np.asarray(mu, dtype=float)
    return np.clip(phi, 0.0, None) * mu + mu ** 2 / max(float(k), _EPS)


def moments(stat: str, mu, phi: float, k: float) -> Optional[Tuple[np.ndarray,
                                                                   np.ndarray]]:
    """Mean and variance for ``stat`` under the fitted variance function."""
    if family_for(stat) is None:
        return None
    mu = np.asarray(mu, dtype=float)
    return mu, variance_at(mu, phi, k)


def quantile(stat: str, mu, phi: float, k: float, q: float,
             bust: float = 0.0) -> Optional[np.ndarray]:
    """Quantile for ``stat``, matched to the fitted mean and variance.

    Counts get a Negative Binomial and yardage a Gamma, each reparameterised so its
    variance equals :func:`variance_at`. Both quantiles are closed form -- the
    regularised incomplete beta and gamma inverses -- so nothing is sampled.

    Args:
        stat: Stat name without the ``USG_`` prefix.
        mu: Mean per player.
        phi: Poisson-like coefficient.
        k: Shape coefficient.
        q: Probability in ``(0, 1)``.

    Returns:
        np.ndarray | None: Quantiles, or None when the stat is in neither family.
    """
    reparameterised = _reparameterise(stat, mu, phi, k, bust)
    if reparameterised is None:
        return None
    family, parameters, share = reparameterised

    if family == "count":
        return stats.nbinom.ppf(q, *parameters)

    shape, scale = parameters
    # `share` is per-element: `_reparameterise` drops the zero mass only on the rows
    # where the mixture is infeasible, so one call can carry both cases. The `np.where`
    # below is already correct for a zero share -- `rescaled` becomes `q` and `q > 0`
    # selects the plain Gamma -- so the guard only has to ask whether any row needs it.
    if np.any(share > 0.0):
        # A mixture: mass `share` at zero, a Gamma for the rest, the Gamma scaled so
        # the mixture's mean is still `mu` -- the point estimate is what plan 18
        # measured and must not move when an interval is put around it. Below the
        # point mass the quantile *is* zero; the clip keeps the Gamma's own ppf off
        # the negative arguments it would otherwise be handed and return NaN for.
        q = np.asarray(q, dtype=float)
        rescaled = np.clip((q - share) / (1.0 - share), _EPS, 1.0 - _EPS)
        return np.where(q > share,
                        stats.gamma.ppf(rescaled, shape, scale=scale), 0.0)
    return stats.gamma.ppf(q, shape, scale=scale)


def band_probability(stat: str, mu, phi: float, k: float,
                     low: float, high: Optional[float] = None,
                     bust: float = 0.0) -> Optional[np.ndarray]:
    """``P(low <= Y < high)`` under the fitted family. Closed form, no sampling.

    Here for the same reason :func:`quantile` and :func:`pit` are here: they all go
    through :func:`_reparameterise`, so a probability this returns and a decile the
    board prints cannot disagree about the same distribution.

    **What needs it.** A yardage-milestone scoring rule -- "3 points for a 100-199
    yard rushing game" -- is a step function of a *weekly* quantity, so its expected
    value is a band probability times the games played, and it cannot be expressed
    as a rate on a season total. Measured over 2016-2025, evaluating the ladder at
    the season mean instead of integrating it recovers **13-18% of the first tier
    and none of the second** -- a mean can award a band in every game or in none,
    and almost no player's mean sits above an edge. A back averaging 80+ yards a
    game hits 100 in 41.5% of them. See :mod:`Scripts.usage.milestones`.

    The zero point mass is handled explicitly rather than folded into the Gamma,
    matching :func:`quantile`: mass ``share`` sits at zero and the Gamma carries the
    rest, so a band whose lower edge is above zero simply never sees it, and one that
    includes zero does.

    Args:
        stat: Stat name without the ``USG_`` prefix.
        mu: Mean per player, on the same footing the dispersion was fitted at --
            per *game* for a weekly band, not per season.
        phi: Poisson-like coefficient.
        k: Shape coefficient.
        low: Lower edge, inclusive.
        high: Upper edge, exclusive. None means unbounded above.
        bust: Zero point mass, yardage only.

    Returns:
        np.ndarray | None: Probability per element of ``mu``, or None when the stat
        is in neither family.
    """
    reparameterised = _reparameterise(stat, mu, phi, k, bust)
    if reparameterised is None:
        return None
    family, parameters, share = reparameterised

    if family == "count":
        # Counts are integers, so the band is inclusive of `low` and exclusive of
        # `high` on the integer lattice: sf(low - 1) - sf(high - 1).
        upper = (0.0 if high is None
                 else stats.nbinom.sf(np.ceil(high) - 1.0, *parameters))
        return np.clip(stats.nbinom.sf(np.ceil(low) - 1.0, *parameters) - upper,
                       0.0, 1.0)

    shape, scale = parameters
    survival = stats.gamma.sf(np.clip(low, _EPS, None), shape, scale=scale)
    if high is not None and np.isfinite(high):
        survival = survival - stats.gamma.sf(high, shape, scale=scale)
    # The point mass is at exactly zero, so it is inside the band only when the band
    # is. `low <= 0` cannot happen for a milestone but the mixture has to be right
    # for any caller. `share` is per-element, so this stays an array expression.
    mass = np.where(low <= 0.0, share, 0.0)
    return np.clip((1.0 - share) * survival + mass, 0.0, 1.0)


def _reparameterise(stat: str, mu, phi: float, k: float,
                    bust: float = 0.0):
    """The family's own parameters at ``mu``, shared by :func:`quantile` and :func:`pit`.

    Factored out so the sampler, the quantile and the probability transform cannot
    drift apart. Every expression here is lifted verbatim from :func:`quantile`.

    Args:
        stat: Stat name without the ``USG_`` prefix.
        mu: Mean per player.
        phi: Poisson-like coefficient.
        k: Shape coefficient.
        bust: Zero point mass, yardage only.

    Returns:
        tuple | None: ``(family, parameters, share)`` where ``parameters`` is
        ``(size, probability)`` for a count and ``(shape, scale)`` for yardage, and
        ``share`` is the zero mass actually applied. None when the stat has no family.
    """
    family = family_for(stat)
    if family is None:
        return None

    mu = np.clip(np.asarray(mu, dtype=float), _EPS, None)
    variance = np.clip(variance_at(mu, phi, k), mu * _EPS + _EPS, None)

    if family == "count":
        excess = np.clip(variance - mu, _EPS, None)
        size = mu ** 2 / excess
        return family, (size, size / (size + mu)), 0.0

    share = min(max(float(bust), 0.0), MAX_BUST)
    if share > 0.0:
        # **The mixture is not always feasible, and clipping the impossible case was
        # producing a point mass rather than a distribution.**
        #
        # A share `s` at zero plus a Gamma whose mixture mean is `mu` and mixture
        # variance is `variance` requires a positive conditional variance, and
        # rearranging gives the condition exactly:
        #
        #     Var(1 - s) > mu^2 * s     i.e.     CV^2 > s / (1 - s)
        #
        # `bust` is fitted pooled -- the share of *all* weekly or seasonal rows that
        # realised zero -- while CV falls as the projection grows. So at a large `mu`
        # a pooled share of 0.18 against a CV of 0.45 fails it, the conditional
        # variance comes out **negative**, and clipping it to _EPS collapsed the
        # Gamma to a spike: P(>= 100 | mu = 110) came out 0.820 where the plain
        # Gamma gives 0.522 and ten seasons of football give 0.510.
        #
        # Where the mixture is infeasible the zero mass is dropped rather than
        # forced. That is the limiting case of the same family and needs no tuning
        # constant, and the alternative on offer was a distribution with no width.
        # The low-`mu` end -- where the zero mass was measured to matter, and where
        # 10.5% of rows really do realise nothing -- is unaffected, because CV is
        # large there.
        feasible = variance * (1.0 - share) > mu ** 2 * share
        conditional_mean = mu / (1.0 - share)
        conditional_var = np.where(
            feasible,
            (variance + mu ** 2) / (1.0 - share) - conditional_mean ** 2,
            variance)
        conditional_mean = np.where(feasible, conditional_mean, mu)
        conditional_var = np.clip(conditional_var, _EPS, None)
        return (family,
                (conditional_mean ** 2 / conditional_var,
                 conditional_var / conditional_mean),
                np.where(feasible, share, 0.0))
    return family, (mu ** 2 / variance, variance / mu), 0.0


def pit(stat: str, mu, phi: float, k: float, observed, bust: float = 0.0,
        rng: Optional[np.random.Generator] = None) -> Optional[np.ndarray]:
    """Probability-integral transform of a realised value, randomised at the atoms.

    ``F(x)`` for a continuous distribution is Uniform(0, 1) when the distribution is
    right, which is what makes this the natural residual to correlate: it strips every
    stat's own scale and shape away and leaves only where in its own distribution the
    player landed. Correlating those is what a copula needs.

    **Randomised, and it has to be.** Both families here put point mass somewhere -- a
    Negative Binomial on every integer, a zero-inflated Gamma on zero -- and a plain
    ``F(x)`` at an atom is not uniform: every player who caught exactly four touchdowns
    returns the identical number, so the transform piles up on a few values and the
    correlation reads whatever the ties happen to do. Spreading each atom uniformly
    across the probability it owns restores uniformity exactly. Without it the fitted
    correlations are attenuated toward zero, worst for the low-count stats that have the
    most ties.

    Args:
        stat: Stat name without the ``USG_`` prefix.
        mu: Mean per player.
        phi: Poisson-like coefficient.
        k: Shape coefficient.
        observed: Realised totals, same shape as ``mu``.
        bust: Zero point mass, yardage only.
        rng: Generator for the randomisation. None draws deterministically at the
            midpoint of each atom, which is reproducible and adequate for a
            correlation, but is *not* uniform -- pass a Generator when that matters.

    Returns:
        np.ndarray | None: Values in ``(0, 1)``, or None when the stat has no family.
    """
    reparameterised = _reparameterise(stat, mu, phi, k, bust)
    if reparameterised is None:
        return None
    family, parameters, share = reparameterised

    x = np.asarray(observed, dtype=float)
    spread = (rng.random(x.shape) if rng is not None
              else np.full(x.shape, 0.5))

    if family == "count":
        size, probability = parameters
        upper = stats.nbinom.cdf(x, size, probability)
        lower = stats.nbinom.cdf(x - 1.0, size, probability)
    elif np.any(share > 0.0):
        shape, scale = parameters
        positive = x > 0.0
        upper = np.where(positive,
                         share + (1.0 - share) * stats.gamma.cdf(
                             np.clip(x, _EPS, None), shape, scale=scale),
                         share)
        lower = np.where(positive, upper, 0.0)
    else:
        shape, scale = parameters
        upper = stats.gamma.cdf(np.clip(x, 0.0, None), shape, scale=scale)
        lower = upper

    return np.clip(lower + spread * (upper - lower), _EPS, 1.0 - _EPS)


def key(position: str, stat: str, cohort: Optional[str] = None) -> str:
    """Dictionary key for a fitted dispersion.

    A string rather than a tuple because these persist to JSON, which has no tuple
    keys -- and a silently stringified tuple would not survive a round trip.

    Args:
        position: Modelled position.
        stat: Stat name without the ``USG_`` prefix.
        cohort: ``settled``, ``mover`` or ``rookie``. None gives the pooled key, which
            is what every caller wanted before plan 33 phase 3 and is still the fallback
            wherever a cohort cell is too thin to fit.
    """
    return f"{position}|{stat}" if cohort is None else f"{position}|{stat}|{cohort}"
