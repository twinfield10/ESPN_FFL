"""A Beta-Binomial predictive distribution for games played.

The season head projects expected games as a point estimate, and plan 18 already
records why that is the weakest thing it does: prior-season games predict the next
season at r = +0.343 among players who managed 8+, and the fitted R-squared is 0.19.
A number that uncertain should be reported as a distribution, not as a single value
that invites being read as a forecast.

**Why Beta-Binomial, and why it is not a modelling preference.** Games played is a
count out of a known slate, so the natural family is Binomial -- and the data rejects
that decisively. Measured over 3,942 player-seasons 2017-2025, the variance of the
games share is **5.6x to 8.1x** what a Binomial allows:

| Pos | mean share | observed Var | Binomial Var | ratio |
|---|---|---|---|---|
| QB | 0.546 | 0.1203 | 0.0149 | 8.1x |
| RB | 0.634 | 0.0918 | 0.0140 | 6.5x |
| WR | 0.669 | 0.0876 | 0.0134 | 6.6x |
| TE | 0.624 | 0.0786 | 0.0141 | 5.6x |

Beta-Binomial is the Binomial with its success probability given a Beta prior, which
is exactly the structure here: each player has his own durability, drawn from a
positional distribution, and we never observe it directly. It has a closed-form mean,
variance and PMF, so every quantity below is computed analytically -- no simulation.

**Parameterised by mean and concentration**, not by alpha and beta. The mean comes
from the existing fitted regression, which is measured and works; this module adds
only the second moment. Keeping them separate means the point estimate does not move
when the dispersion is re-estimated, and the walk-forward numbers in plan 18 stay
comparable.

``kappa`` is the concentration: ``kappa -> infinity`` recovers the Binomial, and
small values mean heavy overdispersion. Fitted on held-out 2025 residuals it lands at
**1.9 to 4.1** by position, so the distribution is far closer to "some players are
durable and some are not" than to "every player is a coin flip each week".
"""

from __future__ import annotations

from typing import Optional, Sequence, Tuple

import numpy as np
from scipy.special import gammaln

#: Concentration used when a position has too few rows to fit its own.
#:
#: The pooled value across positions rather than a round number, so a thin position
#: gets the pool's dispersion rather than an assertion.
DEFAULT_KAPPA: float = 2.5

#: Rows a position needs before its dispersion is fitted rather than pooled.
MIN_DISPERSION_ROWS: int = 30

#: Concentration above which the distribution is reported as Binomial.
#:
#: ``kappa = (n - R) / (R - 1)`` blows up as the dispersion ratio ``R`` approaches 1,
#: so data that is merely *close* to Binomial yields an enormous and unstable number
#: -- 11,199 on one synthetic draw, where a second draw would give something wildly
#: different. Every value past a few hundred describes the same distribution to more
#: precision than 17 trials can resolve, so they are collapsed. Real football data is
#: nowhere near this: the fitted values are 2.1 to 3.2.
MAX_KAPPA: float = 1000.0

#: Guard for the mean share. A mu of exactly 0 or 1 makes one Beta parameter zero,
#: which is not a distribution -- and both are reachable: the games head clips to
#: [0, 1], and a fully available player really does sit at the boundary.
_EPS: float = 1e-6


def _clip_mu(mu: np.ndarray) -> np.ndarray:
    """Keep the mean share strictly inside (0, 1)."""
    return np.clip(np.asarray(mu, dtype=float), _EPS, 1.0 - _EPS)


def moments(slate: float, mu, kappa: float) -> Tuple[np.ndarray, np.ndarray]:
    """Mean and variance of games played.

    ``Var = n mu (1 - mu) (kappa + n) / (kappa + 1)`` -- the Binomial variance times
    an overdispersion factor that is 1 when ``kappa`` is infinite and ``n`` when it
    is zero.

    Args:
        slate: Games the season offers.
        mu: Mean share of the slate, per player.
        kappa: Concentration.

    Returns:
        tuple: ``(mean, variance)`` arrays, in games.
    """
    n = float(slate)
    mu = _clip_mu(mu)
    kappa = max(float(kappa), _EPS)
    mean = n * mu
    variance = n * mu * (1.0 - mu) * (kappa + n) / (kappa + 1.0)
    return mean, variance


def log_pmf(slate: int, mu, kappa: float) -> np.ndarray:
    """Log probability of every attainable game count.

    Args:
        slate: Games the season offers. Must be a whole number.
        mu: Mean share of the slate, per player. Shape ``(N,)``.
        kappa: Concentration.

    Returns:
        np.ndarray: Shape ``(N, slate + 1)``; column ``k`` is ``log P(X = k)``.
    """
    n = int(round(float(slate)))
    mu = _clip_mu(np.atleast_1d(mu))
    kappa = max(float(kappa), _EPS)

    alpha = (kappa * mu)[:, None]
    beta = (kappa * (1.0 - mu))[:, None]
    k = np.arange(n + 1, dtype=float)[None, :]

    # log C(n, k) + log B(k + alpha, n - k + beta) - log B(alpha, beta)
    log_choose = (gammaln(n + 1.0) - gammaln(k + 1.0) - gammaln(n - k + 1.0))
    log_top = (gammaln(k + alpha) + gammaln(n - k + beta)
               - gammaln(n + alpha + beta))
    log_bottom = gammaln(alpha) + gammaln(beta) - gammaln(alpha + beta)
    return log_choose + log_top - log_bottom


def pmf(slate: int, mu, kappa: float) -> np.ndarray:
    """Probability of every attainable game count. See :func:`log_pmf`."""
    return np.exp(log_pmf(slate, mu, kappa))


def quantile(slate: int, mu, kappa: float, q: float) -> np.ndarray:
    """The smallest game count whose cumulative probability reaches ``q``.

    Exact rather than approximate: the support is ``0..slate``, so the CDF is a
    cumulative sum over at most 19 values and the quantile is a search within it.
    A normal approximation would be wrong in the place that matters, because the
    distribution is strongly left-skewed -- most players are fine and a few miss most
    of the year.

    Args:
        slate: Games the season offers.
        mu: Mean share of the slate, per player.
        kappa: Concentration.
        q: Probability in ``(0, 1)``.

    Returns:
        np.ndarray: Game counts, shape ``(N,)``.
    """
    cdf = np.cumsum(pmf(slate, mu, kappa), axis=1)
    # searchsorted per row, vectorised: count how many cumulative values fall short.
    return (cdf < float(q)).sum(axis=1).astype(float)


def fit_dispersion(observed: Sequence[float], slate: Sequence[float],
                   mu: Sequence[float]) -> Optional[float]:
    """Estimate ``kappa`` from residuals, by method of moments.

    Matching the sum of squared residuals to the sum of Beta-Binomial variances:

    ``R = sum (x - n mu)^2 / sum n mu (1 - mu)``  and  ``kappa = (n - R) / (R - 1)``

    Moments rather than maximum likelihood, deliberately. The mean is supplied by a
    regression fitted elsewhere, so a joint likelihood would let the dispersion pull
    the mean around and silently change the point estimates plan 18 measured. This
    estimates the second moment given the first, and leaves the first alone.

    Args:
        observed: Realised games played.
        slate: Games available in the season each row belongs to.
        mu: Predicted mean share for each row.

    Returns:
        float | None: The fitted concentration, or None when the residuals are not
        overdispersed at all -- in which case there is no Beta-Binomial to fit and
        the caller should fall back rather than invent one.
    """
    x = np.asarray(observed, dtype=float)
    n = np.asarray(slate, dtype=float)
    mu = _clip_mu(mu)

    keep = np.isfinite(x) & np.isfinite(n) & np.isfinite(mu) & (n > 0)
    x, n, mu = x[keep], n[keep], mu[keep]
    if x.size < MIN_DISPERSION_ROWS:
        return None

    residual_ss = float(((x - n * mu) ** 2).sum())
    binomial_ss = float((n * mu * (1.0 - mu)).sum())
    if binomial_ss <= 0:
        return None

    ratio = residual_ss / binomial_ss
    mean_slate = float(n.mean())
    # R >= n is beyond what the family can express -- every player would either play
    # everything or nothing -- so there is no kappa to return.
    if ratio >= mean_slate:
        return None
    # R <= 1 is under- or equi-dispersed: no overdispersion to model, which is a
    # finding rather than a failure. Reported as the Binomial limit rather than as
    # None, so the caller gets a usable distribution instead of falling back to a
    # pooled default that would assert *more* spread than the data shows.
    if ratio <= 1.0:
        return MAX_KAPPA
    return min((mean_slate - ratio) / (ratio - 1.0), MAX_KAPPA)


def calibration(observed: Sequence[float], slate: float, mu, kappa: float,
                lower: float = 0.1, upper: float = 0.9) -> Tuple[float, float]:
    """Share of realised outcomes falling inside a predicted interval.

    The check that decides whether any of this is worth reporting. A p10-p90 band
    should contain 80% of outcomes; much more and the interval is uselessly wide,
    much less and it is lying.

    Args:
        observed: Realised games played.
        slate: Games the season offered.
        mu: Predicted mean share per row.
        kappa: Concentration.
        lower: Lower quantile.
        upper: Upper quantile.

    Returns:
        tuple: ``(coverage, nominal)`` -- the realised share inside the band, and the
        share it was built to contain.
    """
    x = np.asarray(observed, dtype=float)
    low = quantile(int(round(slate)), mu, kappa, lower)
    high = quantile(int(round(slate)), mu, kappa, upper)
    inside = (x >= low) & (x <= high)
    return float(inside.mean()), float(upper - lower)
