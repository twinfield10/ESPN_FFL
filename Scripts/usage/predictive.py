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
    family = family_for(stat)
    if family is None:
        return None

    mu = np.clip(np.asarray(mu, dtype=float), _EPS, None)
    variance = np.clip(variance_at(mu, phi, k), mu * _EPS + _EPS, None)

    if family == "yardage" and bust > 0.0:
        # A mixture: mass `bust` at zero, a Gamma for the rest. The Gamma is scaled
        # so the mixture's mean is still `mu` -- the point estimate is what plan 18
        # measured and must not move when an interval is put around it.
        share = min(float(bust), MAX_BUST)
        conditional_mean = mu / (1.0 - share)
        conditional_var = np.clip(
            (variance + mu ** 2) / (1.0 - share) - conditional_mean ** 2,
            _EPS, None)
        out = np.zeros_like(mu)
        upper_part = q > share
        if np.any(upper_part):
            rescaled = (q - share) / (1.0 - share)
            shape = conditional_mean ** 2 / conditional_var
            scale = conditional_var / conditional_mean
            out = np.where(upper_part,
                           stats.gamma.ppf(rescaled, shape, scale=scale),
                           0.0)
        return out

    if family == "count":
        # NegBin(n, p) with mean mu and the given variance. Variance must exceed the
        # mean for the family to exist; where it does not the count is effectively
        # Poisson and the shape is pushed high rather than allowed to go negative.
        excess = np.clip(variance - mu, _EPS, None)
        size = mu ** 2 / excess
        probability = size / (size + mu)
        return stats.nbinom.ppf(q, size, probability)

    shape = mu ** 2 / variance
    return stats.gamma.ppf(q, shape, scale=variance / mu)


def key(position: str, stat: str) -> str:
    """Dictionary key for a fitted (position, stat) dispersion.

    A string rather than a tuple because these persist to JSON, which has no tuple
    keys -- and a silently stringified tuple would not survive a round trip.
    """
    return f"{position}|{stat}"
