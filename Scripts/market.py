"""What a sportsbook posts, and what a projection needs.

A posted market is a **line and a price**, and a projection is a number. Turning
one into the other takes two decisions -- remove the book's margin, and convert a
threshold into an expectation -- and until this module existed both were made twice,
in ``Scripts/scrape_BOL.py`` and ``Scripts/scrape_pinnacle.py``, differently, and
neither had ever been measured. ``docs/plans/35-market-lines-and-vig.md`` records
what the two copies were doing wrong. This is the one place that does it.

Everything below is measured on the archived raw store rather than assumed:
``Data/Projections/BetOnline/Landing/2025/BetOnline_AllProps_Raw.parquet`` for
prices, ``Data/NFL/<season>/player_weeks.parquet`` for outcomes.

**1. The margin is one number and it de-vigs both market shapes.** BetOnline's
two-way pairs sum to a median implied probability of **1.0640** (n=51, range
1.0621-1.0658) and nothing removed it, so every expectation built off those prices
carried the hold. Proportional de-vig -- ``q = p / (p + p')`` -- is exact there and
is what Pinnacle already did.

The ladder is harder, because it is *one-sided*: BetOnline posts ``P(X >= t)`` at 2
to 17 thresholds and never the complement, so there is nothing to normalise against.
Plan 35 proposed normalising "the ladder to its own total", and that is a **no-op by
construction**: differencing the survival function gives exact probabilities that
sum to ``P(X >= t_1)``, and the residual ``P(X < t_1)`` makes the total 1 whatever
the prices are.

So the ladder's hold was measured instead, against the two-way line on the same
player-stat-week: interpolate (yardage) or read the matching rung (counts), and
divide by the de-vigged ``P(over)``. It comes out at **1.0649** (n=48) -- the same
6.4% as the two-way market, at 1.063-1.065 on every count stat. Yardage reads
1.076-1.086 because interpolating a convex survival curve across a 10-yard rung gap
overshoots, not because the hold differs. **One measured overround therefore de-vigs
both shapes**, and :func:`measure_overround` takes it from the scrape in hand rather
than from this docstring.

**2. An odds tilt moves a line by a standard deviation, not by a fraction of its
level.** Both scrapers nudged the line by ``Juice_Diff * value * k`` with ``k = 0.5``
in one file and ``0.25`` in the other and no evidence for either. ``Juice_Diff`` is a
difference of decimal-odds-minus-one and ``value`` is the *level* of the line, so the
form assumes a constant coefficient of variation -- and CV falls 0.81 to 0.44 across
the rushing range and 0.52 to 0.25 across the passing range
(``docs/plans/34-stat-first-audit.md`` F3). The textbook conversion needs no
coefficient at all::

    mean = line + Phi^-1(q_novig_over) * sigma(line)

It is exact at ``q = 0.5``, monotone in ``q``, and it is in the small-correction
regime where a Gaussian read of a skewed distribution costs little: measured on the
archive, ``q`` has median exactly **0.5000** and ``|q - 0.5|`` a median of **0.0199**
and a maximum of **0.1388**. Books post the line at the median; the price only says
which side of it.

Measured against what it replaces, the old form over-adjusts a large line by roughly
the ratio of ``sigma/line`` to a constant: Kyle Pitts, receiving yards, line 54.5 --
``k = 0.5`` moved it **+6.27 yards**, the derivation moves it **+2.41**.

**3. The ladder carries a distribution, and for a count it is exact.** For
non-negative integers ``E[N] = sum_k P(N >= k)`` and
``E[N^2] = sum_k (2k - 1) P(N >= k)``, both over the rungs, so a count ladder gives a
mean *and* a variance with no family assumed. Kyle Pitts' anytime market -- 0.333,
0.077, 0.012 -- gives ``E[N] = 0.4220`` and ``sd = 0.6670``. The old code computed
the mean by the same arithmetic under another name and threw the variance away.
Nothing else in this repo has a market-implied dispersion at player-week grain.

**4. A yardage ladder is not a mean, and the old arithmetic was not an estimator of
anything.** The same differencing applied to a yardage ladder assigns each bucket's
mass to its *lower edge* and drops everything below the lowest rung entirely -- and
the lowest rung is nowhere near zero: Matthew Stafford's passing ladder starts at
253. Measured against the posted line on the same player-stat, ``sum(value *
exactProb)`` runs from **0.77 to 1.81** over 17 ladders, median 1.10. Both
directions, because two errors fight: the dropped low mass pulls it down, hardest
where the ladder starts well above the median (Stafford 209.5 against a line of
272.5, Cousins 188.7 against 229.5), and a right-skewed stat's mean genuinely sits
above its median, which pushes it up (David Sills 19.0 against a line of 10.5).

Today that is invisible because the code prefers the two-way line wherever one
exists -- which is every laddered yardage row in the archive. Plan 35's item 4 asked
for the preference to be dropped. Dropping it while keeping this arithmetic would
have shipped a number with a 0.77-1.81 spread against the book's own line.

So a yardage ladder is read for the two things it states directly. Its **median**,
interpolated to ``S = 0.5``, which is what a line is -- and which, de-vigged,
**reproduces the posted line**: 273.0 against 272.5 for Stafford, 230.0 against
229.5 for Cousins, 66.0 against 65.5 for Drake London. That is the de-vig in item 1
validated end to end on prices alone, with no outcome data involved. And its
**scale**, a normal-equivalent read of the half-span above the median, which needs
no tail extrapolation, no treatment of the mass below the first rung, and no family
(:data:`SCALE_SPAN`). The mean still comes from the line when there is one.

**The market's dispersion reads 1.64x the fitted one, and it ships beside the
projection rather than inside it.** Measured across the archived yardage ladders at
the same line. The gap has three components pointing two ways -- a fit whose x-axis
is a hindsight mean, a book shading its long shots, and the one-sided read meeting a
skewed shape -- and the archive cannot separate them, because the reference for the
second is the first. So ``sigma`` inside :func:`line_to_mean` stays the fitted
number, whose provenance is understood, and the market's number is emitted as
``<stat>_sd``. :data:`MARKET_OVER_FITTED_SCALE` carries the measurement and the
argument.

**5. A count market's threshold is not its mean, and this is the largest error
here.** A book posts interceptions at 0.5, an anytime touchdown at 0.5, a
quarterback's touchdowns at 1.5. Those are statements of ``P(N >= 1) = q`` and
``P(N >= 2) = q``, and the old code treated the threshold itself as the projection
with a juice nudge on top. Measured on the archive, a 0.5 interception line priced
at ``q = 0.412`` came out **0.415** where the answer is 0.531, against a population
of quarterbacks realising 0.663 a week. Pinnacle's touchdown column went further and
ran **negative** -- -0.698 to 1.124, 14 of 421 player-weeks below zero -- because
``0.5 + Juice_Diff * 0.125`` has no floor at a longshot price.

Plan 35 proposed a fitted positional multiplier for this, ``E[N] / P(>= 1)``, which
:meth:`MarketModel.td_scale` still carries: 1.2650 at running back, 1.1515 at
receiver, 1.1187 at tight end, 1.1385 at quarterback, over 174,374 player-weeks. It
is not what ships, because a flat ratio is right at one rate and the true ratio runs
from 1.0 at a rate of zero to 1.6 at a touchdown a game.
:func:`count_line_to_mean` inverts the distribution instead, and binned by realised
rate it recovers ``E[N]`` to within 2-5% across the range. The multiplier stays as
the fallback and as the number that made the defect visible.

**What all of this is worth, measured against realised outcomes.** 2025 calibration,
total projected over total realised on played rows, on the markets whose old
projection can be inverted back to the price that produced it (see
:func:`Scripts.lab.market.RECOVERABLE`):

| source | stat | n | before | after |
|---|---|---|---|---|
| BetOnline QB | passingInterceptions | 431 | 0.712 | **1.011** |
| Pinnacle RB | rushingTouchdowns | 389 | 0.679 | **0.996** |
| Pinnacle RB | receivingTouchdowns | 389 | 0.591 | **0.894** |
| BetOnline RB | rushingTouchdowns | 910 | 1.490 | 1.401 |
| BetOnline WR | receivingTouchdowns | 1,098 | 1.308 | 1.229 |
| BetOnline TE | receivingTouchdowns | 431 | 1.127 | 1.059 |
| BetOnline QB | rushingTouchdowns | 440 | 1.123 | 1.055 |

Every row moves toward 1.00. The three that move furthest are the count conversion;
the four that move 6.4% are the de-vig alone, since BetOnline's anytime ladder needed
no conversion.

**What is left, and it is not margin.** BetOnline's touchdown columns stay 40% high
at running back after de-vigging. Most of that is the *allocation*: all of a back's
anytime market goes to the rushing column and none to receiving, which is why
``Scripts/lab/accuracy.py`` reports BOL@RB receiving touchdowns at a ratio of 0.0 on
910 player-weeks. That is ``docs/plans/34-stat-first-audit.md`` F2's open item, not
this one's. The remainder is the book's own anytime price sitting above the realised
rate on the rows it priced, one season, and it belongs to the blend weights.

Usage::

    python -m Scripts.market --fit
    python -m Scripts.market --show
    python -m Scripts.market --report
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl
from scipy import stats as sps

from Scripts import paths
from Scripts.usage import predictive as pv

#: Model version. Bumped when a fit changes, so a stored artifact says what it is.
MODEL_VERSION = "1.0.0"

#: Where the fitted artifact lives, beside the other model files.
MODEL_PATH = paths.DATA_DIR / "NFL" / "models" / f"market_{MODEL_VERSION}.json"

#: The book's margin when a scrape has no two-way pair to measure it from.
#:
#: Measured, not chosen: the median two-way overround on the archived store, where
#: 51 pairs run 1.0621 to 1.0658. It is a fallback rather than the number in use --
#: :func:`measure_overround` reads the scrape in hand, because a book that changes
#: its hold should move this pipeline, not be papered over by a constant.
DEFAULT_OVERROUND: float = 1.0640

#: Widest margin that will be believed from a live scrape.
#:
#: A guard on the *measurement*, not on the book. A malformed pivot -- one side
#: missing, a probability read as odds -- produces a wild overround, and dividing a
#: whole week's survival probabilities by it would silently gut every projection.
#: Outside this range :func:`measure_overround` falls back to
#: :data:`DEFAULT_OVERROUND` and says so.
OVERROUND_BOUNDS: Tuple[float, float] = (1.0, 1.25)

#: Suffix marking a market-implied standard deviation beside its projection.
#:
#: ``BOL_receivingYards_sd`` is the dispersion of ``BOL_receivingYards``. Named
#: with a suffix rather than a prefix so the two sort together, and declared here so
#: the consumers that must skip it -- coverage counting, scoring -- can ask rather
#: than pattern-match.
SD_SUFFIX: str = "_sd"


@dataclass(frozen=True)
class MarketStat:
    """One stat a book posts, and how to find it in ``player_weeks``.

    Attributes:
        weekly: Polars expression naming the realised weekly quantity.
        kind: ``"count"`` for a non-negative integer, ``"yardage"`` for a
            continuous total. It decides whether a ladder is read by the exact
            discrete identity or by quantiles.
    """

    weekly: pl.Expr
    kind: str


def _total_tackles() -> pl.Expr:
    """Solo plus assisted, which is what a book means by "tackles"."""
    return (pl.col("def_tackles_solo").fill_null(0)
            + pl.col("def_tackle_assists").fill_null(0))


#: Every market this pipeline reads, mapped to the outcome that settles it.
#:
#: The keys are ESPN stat names, so a fitted dispersion is looked up by the same
#: name the projection column carries and the two cannot drift apart. ``kind`` is
#: this module's own split rather than :func:`Scripts.usage.predictive.family_for`'s,
#: which knows only the five stats the season model projects.
MARKET_STATS: Dict[str, MarketStat] = {
    "passingYards": MarketStat(pl.col("passing_yards"), "yardage"),
    "passingAttempts": MarketStat(pl.col("attempts"), "count"),
    "passingCompletions": MarketStat(pl.col("completions"), "count"),
    "passingTouchdowns": MarketStat(pl.col("passing_tds"), "count"),
    "passingInterceptions": MarketStat(pl.col("passing_interceptions"), "count"),
    "rushingYards": MarketStat(pl.col("rushing_yards"), "yardage"),
    "rushingAttempts": MarketStat(pl.col("carries"), "count"),
    "receivingYards": MarketStat(pl.col("receiving_yards"), "yardage"),
    "receivingReceptions": MarketStat(pl.col("receptions"), "count"),
    "defensiveTotalTackles": MarketStat(_total_tackles(), "count"),
    "defensiveSacks": MarketStat(pl.col("def_sacks"), "count"),
    "defensiveInterceptions": MarketStat(pl.col("def_interceptions"), "count"),
    "anytimeTouchdown": MarketStat(
        pl.col("rushing_tds").fill_null(0) + pl.col("receiving_tds").fill_null(0),
        "count"),
}

#: Columns :func:`weekly_panel` reads. Derived from :data:`MARKET_STATS` so adding a
#: market cannot forget to pull the column that settles it.
PANEL_COLUMNS: Tuple[str, ...] = (
    "season", "week", "gsis_id", "position",
    "passing_yards", "attempts", "completions", "passing_tds",
    "passing_interceptions", "rushing_yards", "carries", "receiving_yards",
    "receptions", "rushing_tds", "receiving_tds",
    "def_tackles_solo", "def_tackle_assists", "def_sacks", "def_interceptions",
)

#: Column name -> the market that column actually came from.
#:
#: **A lookup under the wrong name is silent and total.** Pinnacle's touchdown
#: column is named ``rushingTouchdowns`` -- ``prop_to_stat`` calls it that because
#: that is the column the yardage-share split writes -- while the market is any
#: scrimmage touchdown priced at a 0.5 line, and the fit is keyed
#: ``anytimeTouchdown``. Looked up as ``rushingTouchdowns`` there is no fitted
#: dispersion, so :meth:`MarketModel.mean_from_line` abstains and returns the line:
#: a flat 0.5 touchdowns for every player the book priced. The alias lives here
#: rather than in the scraper so the conversion cannot be reached by a name that
#: resolves to nothing.
MARKET_ALIASES: Dict[str, str] = {
    "rushingTouchdowns": "anytimeTouchdown",
    "receivingTouchdowns": "anytimeTouchdown",
}


def resolve_stat(stat: str) -> str:
    """The market name a column should be converted under. See :data:`MARKET_ALIASES`."""
    name = str(stat)
    return name if name in MARKET_STATS else MARKET_ALIASES.get(name, name)


#: Positions the anytime-touchdown multiplier is fitted for, plus a pooled receiver
#: key.
#:
#: ``REC`` exists because Pinnacle's feed carries no position at all and
#: :func:`position_from_markets` can tell a passer from a back from the markets a
#: book posts but cannot separate a wideout from a tight end. Pooling them costs
#: the 3% between 1.1515 and 1.1187, against the 20%+ the multiplier is there to
#: recover.
TD_POSITIONS: Tuple[str, ...] = ("QB", "RB", "WR", "TE")

#: Player-seasons a scoring rate is counted on.
#:
#: Every week, deliberately. The ratio ``E[N] / P(>= 1)`` is a property of the
#: touchdown distribution and does not need a minimum sample per player the way a
#: variance fit needs a stable per-game mean.
MIN_TD_WEEKS: int = 200

#: Quantiles the family-free scale is read between: the median and the 85th.
#:
#: **One-sided, because a book prices one side.** The inter-quartile range would be
#: the obvious span and it is not available: measured on the archive, the lowest
#: rung of a yardage ladder sits at a de-vigged ``S`` of 0.495 to 0.770, so eight of
#: seventeen ladders start at or *below* the median and no symmetric span is
#: bracketed by all of them. The top is rich -- ``S`` runs down to 0.026 -- because
#: a book posts the upside a bettor wants and not the downside.
#:
#: So the scale is read from the half-span above the median, which is where the
#: rungs are, and normalised to a standard deviation by ``Phi^-1(0.85)``. Measured,
#: it is insensitive to the span: 0.50-0.80, 0.50-0.85 and 0.50-0.90 give Bijan
#: Robinson's receiving ladder 49.2, 49.2 and 49.1 yards. 0.85 is the widest of the
#: three that 14 of 17 ladders reach.
#:
#: **What the caller is getting.** A normal-equivalent scale, not a symmetric
#: interval, and on a right-skewed weekly distribution it reads high -- about 6% on
#: the fitted Gamma's own shape. The other 55% is not skew. See
#: :func:`prefer_market_scale`.
SCALE_SPAN: Tuple[float, float] = (0.50, 0.85)

#: ``Phi^-1(0.85) - Phi^-1(0.50)``, the span of a standard normal over
#: :data:`SCALE_SPAN`.
SPAN_TO_SIGMA: float = float(sps.norm.ppf(SCALE_SPAN[1]) - sps.norm.ppf(SCALE_SPAN[0]))

_EPS = 1e-12


# --- price arithmetic ----------------------------------------------------------
#
# No fit and no artifact: these are identities about prices, and they are separated
# from the fitted part so a scraper can de-vig without loading a model.


def overround(p_over, p_under) -> np.ndarray:
    """Implied probability a two-way pair sums to.

    Args:
        p_over: Implied probability of the over, ``1 / decimal odds``.
        p_under: Implied probability of the under.

    Returns:
        np.ndarray: The sum. 1.0 is a fair market; the archive reads 1.0640.
    """
    return np.asarray(p_over, dtype=float) + np.asarray(p_under, dtype=float)


def devig_two_way(p_over, p_under) -> Tuple[np.ndarray, np.ndarray]:
    """Proportional de-vig of a two-way pair.

    The book's margin is spread across the two sides in proportion to the prices,
    which is the standard reading and the one Pinnacle's own ``ImpNoVig`` already
    used. Exact in the sense that matters: the two results sum to 1.

    Args:
        p_over: Implied probability of the over.
        p_under: Implied probability of the under.

    Returns:
        tuple: ``(q_over, q_under)``, summing to 1 wherever both inputs are
        finite and positive, and NaN where either is missing.
    """
    over = np.asarray(p_over, dtype=float)
    under = np.asarray(p_under, dtype=float)
    total = over + under
    with np.errstate(invalid="ignore", divide="ignore"):
        q_over = np.where(total > _EPS, over / total, np.nan)
        q_under = np.where(total > _EPS, under / total, np.nan)
    return q_over, q_under


def measure_overround(p_over, p_under,
                      default: float = DEFAULT_OVERROUND) -> float:
    """The book's margin, taken from the scrape rather than from a constant.

    Args:
        p_over: Implied probabilities of the overs in this scrape.
        p_under: Their unders, aligned.
        default: Value to fall back on.

    Returns:
        float: Median overround across the usable pairs, or ``default`` when there
        are none or the median falls outside :data:`OVERROUND_BOUNDS` -- a wild
        measurement is a malformed pivot, and dividing a week's prices by it would
        be worse than using the archived number.
    """
    total = overround(p_over, p_under)
    usable = total[np.isfinite(total)]
    if usable.size == 0:
        return float(default)
    found = float(np.median(usable))
    low, high = OVERROUND_BOUNDS
    return found if low <= found <= high else float(default)


def devig_survival(survival, hold: float = DEFAULT_OVERROUND) -> np.ndarray:
    """Remove the margin from a one-sided ladder.

    A ladder states ``P(X >= t)`` and never its complement, so there is nothing to
    normalise against and the hold has to come from outside -- measured at 1.0649
    against the two-way lines of the same scrape, against 1.0640 measured directly
    on those lines. One number does both.

    Args:
        survival: Implied ``P(X >= t)`` per rung.
        hold: Overround to divide out, from :func:`measure_overround`.

    Returns:
        np.ndarray: De-vigged survival probabilities, clipped to ``[0, 1]``.
    """
    values = np.asarray(survival, dtype=float)
    divisor = float(hold) if float(hold) > _EPS else 1.0
    return np.clip(values / divisor, 0.0, 1.0)


def monotone_survival(thresholds, survival) -> Tuple[np.ndarray, np.ndarray]:
    """A ladder sorted ascending, with its survival function forced non-increasing.

    ``P(X >= t)`` cannot rise with ``t``, and a feed that says otherwise is stale on
    one rung rather than describing a real distribution. Every consumer below
    differences or inverts this function, and a single inversion turns into a
    negative probability or a non-monotone quantile, so it is repaired once here.

    Args:
        thresholds: Rung values.
        survival: Implied ``P(X >= t)`` per rung, aligned.

    Returns:
        tuple: ``(thresholds, survival)`` sorted by threshold, with ``survival``
        replaced by its running minimum and any non-finite rung dropped.
    """
    edges = np.asarray(thresholds, dtype=float)
    values = np.asarray(survival, dtype=float)
    keep = np.isfinite(edges) & np.isfinite(values)
    edges, values = edges[keep], values[keep]
    order = np.argsort(edges, kind="stable")
    edges, values = edges[order], values[order]
    return edges, np.minimum.accumulate(values) if values.size else values


def count_moments(thresholds, survival) -> Tuple[float, float]:
    """Mean and standard deviation of a count, exactly, from its ladder.

    For a non-negative integer ``N`` with rungs ``1..K``::

        E[min(N, K)]   = sum_k P(N >= k)
        E[min(N, K)^2] = sum_k (2k - 1) P(N >= k)

    Both are identities, not approximations, so a count ladder yields a dispersion
    with no family assumed anywhere. The truncation at the top rung is real and
    small -- a book posts the anytime market to three touchdowns and the mass above
    it is under a thousandth.

    Args:
        thresholds: Rung values. Expected to be consecutive integers from 1; a
            ladder starting higher cannot state the mean and returns NaN.
        survival: De-vigged ``P(N >= k)`` per rung.

    Returns:
        tuple: ``(mean, sd)``, or ``(nan, nan)`` when the ladder does not start at
        1 -- ``E[N]`` needs the lower rungs and inventing them would be worse than
        abstaining.
    """
    edges, values = monotone_survival(thresholds, survival)
    if edges.size == 0 or not np.isclose(edges[0], 1.0):
        return float("nan"), float("nan")
    mean = float(values.sum())
    second = float((2.0 * edges - 1.0) @ values)
    return mean, float(math.sqrt(max(second - mean ** 2, 0.0)))


def ladder_median(thresholds, survival) -> float:
    """The ladder's own median, which is what a posted line is.

    Interpolated between the rungs that bracket ``S = 0.5``, so it states only what
    the prices state. Nothing is extrapolated: a ladder that does not bracket the
    median returns NaN rather than a guess.

    Args:
        thresholds: Rung values.
        survival: De-vigged ``P(X >= t)`` per rung.

    Returns:
        float: The interpolated median, or NaN.
    """
    edges, values = monotone_survival(thresholds, survival)
    if edges.size < 2 or values[0] < 0.5 or values[-1] > 0.5:
        return float("nan")
    # np.interp needs an increasing x, and survival decreases in the threshold.
    return float(np.interp(0.5, values[::-1], edges[::-1]))


def ladder_scale(thresholds, survival) -> float:
    """A family-free standard deviation from the ladder's own quantiles.

    The normal-equivalent scale of the half-span above the median -- see
    :data:`SCALE_SPAN` for why the span is one-sided and what that costs. Used
    instead of the survival integral because the integral needs the mass below the
    lowest rung, which a book does not price at all.

    Args:
        thresholds: Rung values.
        survival: De-vigged ``P(X >= t)`` per rung.

    Returns:
        float: The scale, or NaN when the ladder does not reach both ends of
        :data:`SCALE_SPAN`. NaN rather than an extrapolation: a book that priced
        only the middle of a range has not stated a dispersion.
    """
    edges, values = monotone_survival(thresholds, survival)
    if edges.size < 2:
        return float("nan")
    low, high = SCALE_SPAN
    if values[0] < 1.0 - low or values[-1] > 1.0 - high:
        return float("nan")
    at_low = float(np.interp(1.0 - low, values[::-1], edges[::-1]))
    at_high = float(np.interp(1.0 - high, values[::-1], edges[::-1]))
    spread = at_high - at_low
    return float(spread / SPAN_TO_SIGMA) if spread > 0 else float("nan")


def market_scale(thresholds, survival, kind: str) -> float:
    """The dispersion a ladder states, by the best route the ladder allows.

    Args:
        thresholds: Rung values.
        survival: De-vigged ``P(X >= t)`` per rung.
        kind: ``"count"`` or ``"yardage"``, from :data:`MARKET_STATS`.

    Returns:
        float: :func:`count_moments`' exact standard deviation for a count ladder
        rooted at 1, otherwise :func:`ladder_scale`'s quantile read, otherwise NaN.
        A count ladder rooted above 1 -- receptions from 4, carries from 7 -- falls
        through to the quantile read, which is what those rungs can support.
    """
    if kind == "count":
        _, sd = count_moments(thresholds, survival)
        if np.isfinite(sd):
            return float(sd)
    return ladder_scale(thresholds, survival)


#: How much wider the market's own dispersion reads than the fitted weekly one.
#:
#: Measured on the archived yardage ladders at the same line: **1.64x** the median
#: over 14 ladders. It is recorded rather than applied, and this comment is the
#: reason -- the gap has at least three components and the archive cannot separate
#: them:
#:
#: * **The fit's x-axis is hindsight.** ``Var(mu) = phi*mu + mu^2/k`` is fitted on
#:   weekly values against a player's *realised* season mean, which has already
#:   absorbed what a book does not know on Sunday.
#:   :func:`Scripts.usage.milestones.report` says so from the other side, and plan
#:   34 F3 measured the consequence as 49-57% coverage against a nominal 80%. This
#:   component argues the fit is too narrow.
#: * **Long-shot shading.** A book prices the upside a bettor wants. Against a Gamma
#:   matched to the fitted sigma and the ladder's own median, the de-vigged survival
#:   runs 0.99 of it below the median, **1.13** at q50-75, **1.42** at q75-90 and
#:   1.28 above -- the excess is entirely in the upper rungs, which is where
#:   :data:`SCALE_SPAN` reads. This component argues the market number is too wide.
#: * **The one-sided read meeting a right-skewed shape**, worth about 6% on the
#:   fitted Gamma's own quantiles. Small, and known.
#:
#: The first two point in opposite directions and the reference distribution for the
#: second *is* the first, so the test cannot decompose them. So the fitted sigma is
#: what moves a line in :func:`line_to_mean` -- its provenance is understood -- and
#: the market's number ships beside the projection as ``<stat>_sd`` for the
#: consumers that want a market-implied dispersion. Separating them needs realised
#: outcomes against archived prices, which is the measurement the per-week raw
#: copies in both scrapers now make possible.
MARKET_OVER_FITTED_SCALE: float = 1.64


def line_to_mean(line, q_over, sigma) -> np.ndarray:
    """Convert a threshold and a de-vigged price into an expectation.

    ``mean = line + Phi^-1(q) * sigma(line)``. Exact at ``q = 0.5``, monotone in
    ``q``, and evaluated with ``sigma`` at the line rather than at the answer --
    a linearisation that costs nothing in the regime a posted line lives in, where
    the measured ``|q - 0.5|`` has a median of 0.02.

    Args:
        line: Posted threshold.
        q_over: De-vigged ``P(X > line)``.
        sigma: Weekly standard deviation at the line, from
            :meth:`MarketModel.sigma`.

    Returns:
        np.ndarray: The expectation. Falls back to the bare line wherever ``sigma``
        is missing, which is a stat with no fitted dispersion: an unadjusted line is
        the book's median and is the right abstention.
    """
    level = np.asarray(line, dtype=float)
    price = np.asarray(q_over, dtype=float)
    scale = np.asarray(sigma, dtype=float)
    shift = np.where(
        np.isfinite(scale) & np.isfinite(price) & (price > 0.0) & (price < 1.0),
        sps.norm.ppf(np.clip(price, _EPS, 1.0 - _EPS)) * np.nan_to_num(scale),
        0.0)
    return level + shift


def count_line_to_mean(threshold, q_over, phi: float, k: float) -> np.ndarray:
    """Convert a count market's threshold and price into an expected count.

    **A Gaussian shift is wrong here and the error is not small.** ``line + Phi^-1(q)
    * sigma`` reads a threshold as a point on a symmetric distribution, and a count
    market's threshold is often 0.5 -- an interception line, an anytime-touchdown
    line -- where the fitted sigma is larger than the line itself. Measured on the
    archive, the Gaussian form sends a 0.5 interception line with ``q = 0.412`` to
    **0.345**, where inverting the count distribution gives 0.531 and the population
    of quarterbacks realises 0.663 a week.

    So the market is read for what it says. "Over ``t``" on a count is
    ``P(N >= floor(t) + 1) = q``, and there is exactly one mean that produces that
    probability under the family this repo already fits for counts -- Negative
    Binomial reparameterised so its variance is ``phi*mu + mu^2/k``, degenerating to
    Poisson where the fit is underdispersed, which every touchdown and interception
    market is. ``P(N >= k)`` rises monotonically in ``mu``, so a bisection finds it.

    **Measured against the alternative plan 35 proposed.** A flat positional
    multiplier ``E[N] / P(>= 1)`` is right only at the population's own rate,
    because the true ratio runs from 1.0 at a rate of zero to 1.6 at one touchdown a
    game. Binned by realised per-game rate over 2016-2025, this inversion recovers
    ``E[N]`` to within:

    | market | k | rate range | inversion / realised |
    |---|---|---|---|
    | anytime touchdown | 1 | 0.08 - 0.67 | 1.02 - 1.05 |
    | passing interceptions | 1 | 0.31 - 1.13 | 0.96 - 1.05 |
    | passing touchdowns | 2 | 0.74 - 2.30 | 1.02 - 1.13 |
    | receptions | 5 | 3.2 - 5.9 | 0.97 - 1.01 |

    The flat 1.2650 running-back multiplier, applied at a rate of 0.08, would be 22%
    high. :meth:`MarketModel.td_scale` is kept as the fallback where a stat has no
    fitted dispersion, and as the number that made the defect visible.

    Args:
        threshold: Posted line, e.g. 0.5 or 5.5.
        q_over: De-vigged ``P(N > threshold)``.
        phi: Poisson-like dispersion coefficient.
        k: Shape coefficient.

    Returns:
        np.ndarray: The mean, NaN where the price is degenerate or the fit absent.
    """
    edge = np.floor(np.asarray(threshold, dtype=float)) + 1.0
    price = np.asarray(q_over, dtype=float)
    usable = (np.isfinite(edge) & np.isfinite(price)
              & (price > _EPS) & (price < 1.0 - _EPS) & (edge >= 1.0))
    out = np.full(np.broadcast(edge, price).shape, np.nan, dtype=float)
    if not usable.any() or not (np.isfinite(phi) and np.isfinite(k)):
        return out

    edge, price = np.broadcast_arrays(edge, price)
    lo = np.zeros(out.shape)
    # A ceiling generous enough that the survival at it exceeds any posted price:
    # the largest market here is 40 pass attempts and the widest fitted dispersion
    # is a factor of three, so 20x the threshold has room to spare.
    hi = np.maximum(edge * 20.0, 10.0)
    for _ in range(BISECTION_STEPS):
        mid = 0.5 * (lo + hi)
        above = _count_survival(edge, mid, phi, k) < price
        lo = np.where(above, mid, lo)
        hi = np.where(above, hi, mid)
    out[usable] = (0.5 * (lo + hi))[usable]
    return out


#: Bisection steps for :func:`count_line_to_mean`.
#:
#: Sixty halvings of a bracket at most 800 wide leaves 1e-15, which is exact for
#: this purpose and costs sixty vectorised evaluations. There is no closed form:
#: ``P(N >= k)`` under a Negative Binomial whose size depends on ``mu`` is not
#: invertible in ``mu``.
BISECTION_STEPS: int = 60


def _count_survival(edge, mu, phi: float, k: float) -> np.ndarray:
    """``P(N >= edge)`` at ``mu``, under the count family this repo fits.

    Negative Binomial reparameterised so its variance equals
    :func:`Scripts.usage.predictive.variance_at`, falling back to Poisson wherever
    that variance does not exceed the mean -- which is where a Negative Binomial has
    no valid size, and where a Poisson is the limiting case rather than a
    substitution.
    """
    mu = np.clip(np.asarray(mu, dtype=float), _EPS, None)
    variance = pv.variance_at(mu, phi, k)
    excess = variance - mu
    out = sps.poisson.sf(edge - 1.0, mu)
    overdispersed = excess > mu * 1e-6
    if np.any(overdispersed):
        size = mu[overdispersed] ** 2 / excess[overdispersed]
        probability = size / (size + mu[overdispersed])
        out[overdispersed] = sps.nbinom.sf(
            np.broadcast_to(edge, mu.shape)[overdispersed] - 1.0,
            size, probability)
    return out


#: Markets that identify a position, because only that position gets volume in them.
#:
#: Deliberately *not* every market a book posts. Pinnacle's touchdown market maps to
#: ``rushingTouchdowns`` for naming reasons -- it is really any scrimmage touchdown
#: -- so a receiver with a touchdown market matches a bare "starts with rushing"
#: test and is classified as a back. Drake London was, which sent his touchdown
#: price through the running-back conversion. Only volume markets vote.
POSITION_MARKERS: Dict[str, Tuple[str, ...]] = {
    "QB": ("passingYards", "passingAttempts", "passingCompletions"),
    "RB": ("rushingYards", "rushingAttempts"),
}


def position_from_markets(posted: Iterable[str]) -> str:
    """Which position a book's own market set implies.

    Pinnacle's feed carries no position and the count conversions are indexed by
    one. What it does carry is which markets exist for a player, and that is nearly
    the same information: a passing line means a quarterback, a rushing line without
    one means a back, and receiving alone means a receiver.

    Args:
        posted: ESPN stat names this player has a market for.

    Returns:
        str: ``"QB"``, ``"RB"`` or ``"REC"`` -- the pooled receiver key, because
        this cannot separate a wideout from a tight end and their fitted numbers
        differ by 3%.
    """
    names = {str(name) for name in posted}
    for position, markers in POSITION_MARKERS.items():
        if names & set(markers):
            return position
    return "REC"


# --- the fitted part -----------------------------------------------------------


@dataclass
class MarketModel:
    """Weekly dispersion per position and stat, plus the anytime-touchdown ratio.

    Attributes:
        dispersion: ``"<position>|<stat>"`` -> ``(phi, k, n)`` for
            ``Var(mu) = phi*mu + mu^2/k``.
        pooled: Same, keyed by stat alone, for a position with too few rows or --
            as with Pinnacle -- no position at all.
        td_scale_by_position: ``"<position>"`` -> ``(E[N] / P(>= 1), n)``.
        overround: The margin measured at fit time, for the record. Live scrapes
            measure their own.
        seasons: Seasons the fit was trained on.
        version: :data:`MODEL_VERSION` at fit time.
    """

    dispersion: Dict[str, Tuple[float, float, int]] = field(default_factory=dict)
    pooled: Dict[str, Tuple[float, float, int]] = field(default_factory=dict)
    td_scale_by_position: Dict[str, Tuple[float, int]] = field(default_factory=dict)
    overround: float = DEFAULT_OVERROUND
    seasons: List[int] = field(default_factory=list)
    version: str = MODEL_VERSION

    def parameters(self, stat: str, position: Optional[str] = None
                   ) -> Optional[Tuple[float, float]]:
        """Dispersion for a stat, by position where one is known.

        Args:
            stat: ESPN stat name, a key of :data:`MARKET_STATS`.
            position: Position as the book reports it. None or unfitted falls back
                to the pooled fit.

        Returns:
            tuple | None: ``(phi, k)``, or None when neither fit exists.
        """
        found = None
        if position:
            found = self.dispersion.get(f"{position}|{stat}")
        found = found or self.pooled.get(stat)
        return None if found is None else (found[0], found[1])

    def sigma(self, stat: str, line, position=None) -> np.ndarray:
        """Weekly standard deviation at a line.

        Args:
            stat: ESPN stat name.
            line: Posted threshold, scalar or array.
            position: Position per line, scalar or array. None uses the pooled fit.

        Returns:
            np.ndarray: ``sqrt(phi*line + line^2/k)``, NaN where no fit applies.
            NaN is deliberate: :func:`line_to_mean` reads it as "leave the line
            alone", which is the honest answer for a stat this repo has never
            fitted a dispersion for.
        """
        level = np.asarray(line, dtype=float)
        out = np.full(level.shape, np.nan, dtype=float)
        if position is None:
            found = self.parameters(stat)
            if found is not None:
                out = np.sqrt(pv.variance_at(level, found[0], found[1]))
            return out

        positions = np.asarray(position, dtype=object)
        positions = np.broadcast_to(positions, level.shape)
        for name in np.unique(positions):
            found = self.parameters(stat, None if name is None else str(name))
            if found is None:
                continue
            rows = positions == name
            out[rows] = np.sqrt(pv.variance_at(level[rows], found[0], found[1]))
        return out

    def mean_from_line(self, stat: str, line, q_over, position=None) -> np.ndarray:
        """A posted line and its de-vigged price, as an expectation.

        The one entry point the scrapers call, so the choice of conversion lives
        here and not in two files. It dispatches on :data:`MARKET_STATS`:

        * **yardage** -- :func:`line_to_mean`, ``line + Phi^-1(q) * sigma(line)``.
        * **count** -- :func:`count_line_to_mean`, which inverts ``P(N >= k) = q``
          under the fitted count family. A Gaussian shift on a 0.5 line is not a
          small error; see that function.

        Args:
            stat: ESPN stat name.
            line: Posted threshold, scalar or array.
            q_over: De-vigged ``P(X > line)``, aligned.
            position: Position per line, or None for the pooled fit.

        Returns:
            np.ndarray: The expectation. Falls back to the bare line for a stat with
            no fitted dispersion, and for a count whose inversion did not resolve --
            the line is the book's own median and is the right abstention.
        """
        level = np.asarray(line, dtype=float)
        price = np.asarray(q_over, dtype=float)
        stat = resolve_stat(stat)
        kind = MARKET_STATS[stat].kind if stat in MARKET_STATS else "yardage"
        if kind == "yardage":
            return line_to_mean(level, price, self.sigma(stat, level, position))

        out = np.full(level.shape, np.nan, dtype=float)
        names = (np.broadcast_to(np.asarray(position, dtype=object), level.shape)
                 if position is not None
                 else np.full(level.shape, None, dtype=object))
        for name in np.unique(names):
            found = self.parameters(stat, None if name is None else str(name))
            rows = names == name
            if found is None:
                continue
            out[rows] = count_line_to_mean(level[rows], price[rows], *found)
        return np.where(np.isfinite(out), out, level)

    def td_scale(self, position=None) -> np.ndarray:
        """``E[touchdowns] / P(at least one)``, per position.

        Args:
            position: Position, scalar or array. ``"REC"`` and anything unfitted
                get the pooled receiver ratio.

        Returns:
            np.ndarray: The multiplier. 1.0 where nothing is fitted, which leaves
            the probability where it was rather than scaling it by a guess.
        """
        pooled = self.td_scale_by_position.get("REC")
        names = np.asarray(position, dtype=object)
        out = np.ones(names.shape, dtype=float)
        for name in np.unique(names):
            found = self.td_scale_by_position.get(
                "" if name is None else str(name)) or pooled
            if found is None:
                continue
            out[names == name] = float(found[0])
        return out

    def to_dict(self) -> Dict:
        """JSON-shaped form."""
        return {"version": self.version, "seasons": self.seasons,
                "overround": self.overround,
                "dispersion": {k: list(v) for k, v in self.dispersion.items()},
                "pooled": {k: list(v) for k, v in self.pooled.items()},
                "td_scale": {k: list(v)
                             for k, v in self.td_scale_by_position.items()}}

    def save(self, path=None) -> None:
        """Persist the fit."""
        path = MODEL_PATH if path is None else path
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as handle:
            json.dump(self.to_dict(), handle, indent=2, sort_keys=True)
            handle.write("\n")

    @classmethod
    def load(cls, path=None) -> "MarketModel":
        """Read a persisted fit.

        Raises:
            FileNotFoundError: When no artifact exists. Build one with
                ``python -m Scripts.market --fit``.
        """
        path = MODEL_PATH if path is None else path
        if not path.is_file():
            raise FileNotFoundError(
                f"No market model at {path}. Fit one with "
                f"`python -m Scripts.market --fit`.")
        with open(path) as handle:
            blob = json.load(handle)
        return cls(
            dispersion={k: tuple(v) for k, v in blob["dispersion"].items()},
            pooled={k: tuple(v) for k, v in blob["pooled"].items()},
            td_scale_by_position={k: tuple(v)
                                  for k, v in blob.get("td_scale", {}).items()},
            overround=float(blob.get("overround", DEFAULT_OVERROUND)),
            seasons=blob.get("seasons", []),
            version=blob.get("version", "unknown"),
        )


def load_model(path=None) -> Optional[MarketModel]:
    """The stored fit, or None when there is none.

    The scrapers call this. A missing artifact must not stop a scrape: without it
    :func:`line_to_mean` leaves lines unadjusted and the anytime multiplier is 1.0,
    which is a *visible* degradation -- lines at the book's median and probabilities
    unscaled -- rather than a crash on a Sunday morning.
    """
    try:
        return MarketModel.load(path)
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as problem:
        print(f"No usable market model ({problem}); lines will not be adjusted. "
              f"Fit one with `python -m Scripts.market --fit`.")
        return None


def weekly_panel(seasons: Sequence[int]) -> pl.DataFrame:
    """Player-weeks with each player-season's own per-game mean beside them.

    The mean is the x-axis of the variance fit, and it is the player's *realised*
    per-game mean rather than a projection, for the reason
    :func:`Scripts.usage.milestones.weekly_panel` gives: this fit is about weekly
    variation, and mixing projection error into it makes the two indistinguishable.

    Args:
        seasons: Seasons to pool.

    Returns:
        pl.DataFrame: One row per player-week, with ``position``, one column per
        market stat, ``games``, and ``mu_<stat>`` per stat.
    """
    from Scripts.usage import features as ft

    weeks = ft.load_player_weeks(seasons, columns=list(PANEL_COLUMNS))
    weeks = weeks.with_columns(
        *[found.weekly.alias(stat) for stat, found in MARKET_STATS.items()])
    means = weeks.group_by(["season", "gsis_id"]).agg(
        pl.len().alias("games"),
        *[pl.col(stat).mean().alias(f"mu_{stat}") for stat in MARKET_STATS])
    return weeks.join(means, on=["season", "gsis_id"], how="left")


#: Games a player-season needs before its weeks teach the dispersion.
#:
#: Same reasoning and same number as
#: :data:`Scripts.usage.milestones.MIN_FIT_GAMES`: a four-game sample gives a
#: per-game mean too noisy to be the x-axis of a variance fit, and those rows are
#: the part-time players whose weekly spread is widest, so including them inflates
#: the fitted dispersion for everyone.
MIN_FIT_GAMES: int = 8

#: How much of the leading position's volume a position needs before its own fit is
#: kept.
#:
#: A guard against fitting a market nobody posts. Two hundred tight ends have thrown
#: a pass since 2016, which is enough rows to satisfy :data:`MIN_FIT_ROWS` and
#: produced ``TE|passingYards`` with ``phi = 126`` -- forty times the quarterback
#: value, from a sample of trick plays. No book posts that line, so the fit would
#: never be consulted, but an artifact that carries numbers nobody may use is one
#: bad lookup away from using them.
#:
#: The rule asks how *material* the position is to the market, measured both ways
#: and both relative to the leading position rather than to the pool: how much it
#: does of the stat (:data:`MIN_MEAN_SHARE`) and how often (:data:`MIN_ROW_SHARE`).
#: Relative to the leading position because a pooled share cannot work for tackles,
#: where nine defensive positions each hold a few per cent of a real market.
#:
#: Measured, that keeps ``QB|rushingAttempts`` (4 a game against a back's 11), nine
#: defensive positions on the tackle and sack markets, and both backs and receivers
#: on receiving. It drops passing at running back, receiving at quarterback,
#: interceptions at tight end, tackles at every offensive position, and -- the one
#: judgement call -- ``WR|rushingYards``, where a gadget carry averages 6% of a
#: back's volume. Those lines are occasionally posted and now read the pooled
#: rushing fit, which is wider than a receiver's own and errs toward not moving the
#: line.
MIN_MEAN_SHARE: float = 0.10

#: Share of the leading position's row count a position needs. See
#: :data:`MIN_MEAN_SHARE`.
MIN_ROW_SHARE: float = 0.05


def fit(seasons: Sequence[int]) -> MarketModel:
    """Fit weekly dispersion per position and stat, and the touchdown ratio.

    Positions are not enumerated: every position that clears
    :data:`Scripts.usage.predictive.MIN_FIT_ROWS` rows and
    :data:`MIN_MEAN_SHARE` of the leading position's volume gets its own fit, and
    the rest fall back to the pooled one. That matters here more than it does for
    the yardage bands, because the tackle market spans nine defensive positions this
    repo has never listed anywhere.

    Args:
        seasons: Training seasons.

    Returns:
        MarketModel: The fit.
    """
    panel = weekly_panel(seasons)
    model = MarketModel(seasons=sorted(int(s) for s in seasons))
    fittable = panel.filter(pl.col("games") >= MIN_FIT_GAMES)

    for stat in MARKET_STATS:
        rows = fittable.filter(pl.col(stat).is_not_null()
                               & (pl.col(f"mu_{stat}") > 0.0))
        if rows.height == 0:
            continue
        found = pv.fit_variance(rows[stat].to_list(), rows[f"mu_{stat}"].to_list())
        if found:
            model.pooled[stat] = (found[0], found[1], rows.height)

        levels = (rows.group_by("position")
                      .agg(pl.col(stat).mean().alias("level"),
                           pl.len().alias("weeks"))
                      .drop_nulls("position"))
        if levels.height == 0:
            continue
        material = levels.filter(
            (pl.col("level") >= pl.col("level").max() * MIN_MEAN_SHARE)
            & (pl.col("weeks") >= pl.col("weeks").max() * MIN_ROW_SHARE))
        for position in sorted(material["position"].to_list()):
            here = rows.filter(pl.col("position") == position)
            if here.height < pv.MIN_FIT_ROWS:
                continue
            found = pv.fit_variance(here[stat].to_list(),
                                    here[f"mu_{stat}"].to_list())
            if found:
                model.dispersion[f"{position}|{stat}"] = (
                    found[0], found[1], here.height)

    # The anytime-touchdown ratio, counted on every week rather than on the
    # eight-game players: it is a property of the touchdown distribution, and a
    # week a book priced counts however few the player went on to play.
    scored = panel.with_columns(
        (pl.col("anytimeTouchdown") > 0).alias("scored"))
    for position in TD_POSITIONS:
        here = scored.filter(pl.col("position") == position)
        if here.height < MIN_TD_WEEKS:
            continue
        rate = float(here["scored"].mean())
        expected = float(here["anytimeTouchdown"].mean())
        if rate > 0:
            model.td_scale_by_position[position] = (expected / rate, here.height)

    receivers = scored.filter(pl.col("position").is_in(["WR", "TE"]))
    if receivers.height >= MIN_TD_WEEKS:
        rate = float(receivers["scored"].mean())
        if rate > 0:
            model.td_scale_by_position["REC"] = (
                float(receivers["anytimeTouchdown"].mean()) / rate,
                receivers.height)
    return model


def show(model: MarketModel) -> str:
    """The fitted numbers, as text."""
    lines = ["=== market dispersion: Var(mu) = phi*mu + mu^2/k, weekly ===",
             "  Only the (position, stat) pairs a book actually posts -- see "
             "MIN_MEAN_SHARE.",
             f"  {'position|stat':34}{'phi':>9}{'k':>10}{'n':>8}"]
    for key in sorted(model.dispersion):
        phi, k, n = model.dispersion[key]
        lines.append(f"  {key:34}{phi:>9.3f}{k:>10.2f}{n:>8}")

    lines.append("")
    lines.append("=== pooled per stat, used where the book reports no position ===")
    lines.append(f"  {'stat':34}{'phi':>9}{'k':>10}{'n':>8}")
    for key in sorted(model.pooled):
        phi, k, n = model.pooled[key]
        lines.append(f"  {key:34}{phi:>9.3f}{k:>10.2f}{n:>8}")

    lines.append("")
    lines.append("=== anytime touchdown: E[N] / P(N >= 1) ===")
    lines.append("  The multiplier that turns a book's anytime price into a count.")
    lines.append(f"  {'position':34}{'ratio':>9}{'n':>8}")
    for key in sorted(model.td_scale_by_position):
        ratio, n = model.td_scale_by_position[key]
        lines.append(f"  {key:34}{ratio:>9.4f}{n:>8}")

    lines.append("")
    lines.append(f"  overround at fit time: {model.overround:.4f}")
    lines.append(f"  seasons: {model.seasons[0] if model.seasons else '?'}"
                 f"..{model.seasons[-1] if model.seasons else '?'}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.market",
        description="Fit and evaluate the market line conversions.")
    parser.add_argument("--fit", action="store_true", help="fit and persist")
    parser.add_argument("--show", action="store_true", help="print the stored fit")
    parser.add_argument("--report", action="store_true",
                        help="measure the conversions on the archived raw store")
    parser.add_argument("--first", type=int, default=2016)
    parser.add_argument("--last", type=int, default=2025)
    args = parser.parse_args(argv)

    if not (args.fit or args.show or args.report):
        parser.error("pass --fit, --show or --report")

    if args.fit:
        model = fit(range(args.first, args.last + 1))
        model.save()
        print(show(model))
        print(f"\nwrote {MODEL_PATH}")
    if args.show and not args.fit:
        print(show(MarketModel.load()))
    if args.report:
        from Scripts.lab import market as lab_market
        print(lab_market.report(args.last)[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
