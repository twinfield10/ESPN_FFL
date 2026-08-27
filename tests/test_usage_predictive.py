"""Predictive intervals on the projected stat lines.

Three things are pinned, and each exists because the obvious version of it failed a
measurement first:

The **mean must survive**. Putting an interval around a projection must not move the
projection, or every walk-forward number in plan 18 changes silently.

The **variance function must not be single-parameter**. The coefficient of variation
falls from 1.90 to 0.48 across the projection range, so a constant-CV family fitted by
moments lands on the largest players and gives everyone else an interval half the
width it should be.

**NaN is not null.** Polars treats them as different, and `is_not_null()` is True for
a NaN -- so a float NaN left in an interval column survives every null filter and then
compares False against everything. That scored abstained players as outside their own
interval and put measured coverage at 6%.

Synthetic throughout. No network, no parquet.
"""

import numpy as np
import polars as pl
import pytest

from Scripts.usage import predictive as pv
from Scripts.usage import season as sn


# --- families -------------------------------------------------------------

def test_every_modelled_stat_has_a_family():
    """A stat with no family silently gets no interval."""
    for stat in sn.STAT_TERMS:
        assert pv.family_for(stat) in {"count", "yardage"}, stat


def test_counts_and_yardage_do_not_overlap():
    assert not set(pv.COUNT_STATS) & set(pv.YARDAGE_STATS)


def test_every_stat_has_an_outcome_to_be_scored_against():
    """Derived from the rate definitions, so a new stat cannot be added without one."""
    assert set(sn.STAT_OUTCOMES) == set(sn.STAT_TERMS)


# --- the variance function ------------------------------------------------

def test_the_variance_function_lets_cv_fall_with_the_mean():
    """The measured shape: CV 1.90 at the bottom of the range against 0.48 at the
    top. A single-parameter family cannot express that and was measured at 49-57%
    coverage against a nominal 80%."""
    phi, k = 140.0, 1000.0
    small = np.sqrt(pv.variance_at(50.0, phi, k)) / 50.0
    large = np.sqrt(pv.variance_at(740.0, phi, k)) / 740.0
    assert small > 1.0
    assert large < 0.6
    assert small > large


def test_variance_is_non_negative_and_increasing():
    mu = np.array([1.0, 10.0, 100.0, 1000.0])
    variance = pv.variance_at(mu, 12.0, 5.0)
    assert (variance > 0).all()
    assert (np.diff(variance) > 0).all()


def test_fit_recovers_a_variance_function_it_generated():
    rng = np.random.default_rng(0)
    mu = rng.uniform(20, 900, 4000)
    phi, k = 100.0, 8.0
    sd = np.sqrt(pv.variance_at(mu, phi, k))
    observed = np.clip(mu + rng.normal(0, sd), 0, None)

    fitted = pv.fit_variance(observed, mu, family="count")
    assert fitted is not None
    got_phi, got_k, _ = fitted
    assert got_phi == pytest.approx(phi, rel=0.35)
    assert got_k == pytest.approx(k, rel=0.5)


def test_coefficients_cannot_go_negative():
    """A negative coefficient means variance shrinking with the projection, which is
    not a thing -- and unconstrained least squares will return one on a thin fit."""
    rng = np.random.default_rng(3)
    mu = rng.uniform(10, 100, 200)
    observed = mu + rng.normal(0, 1, 200)
    phi, k, _ = pv.fit_variance(observed, mu, family="count")
    assert phi >= 0
    assert k > 0


def test_too_few_rows_returns_nothing():
    assert pv.fit_variance([1.0, 2.0], [1.0, 2.0]) is None


# --- quantiles ------------------------------------------------------------

@pytest.mark.parametrize("stat", ["receivingYards", "receivingReceptions"])
def test_quantiles_bracket_the_mean(stat):
    mu = np.array([300.0]) if stat.endswith("Yards") else np.array([60.0])
    low = pv.quantile(stat, mu, 40.0, 20.0, 0.1)
    high = pv.quantile(stat, mu, 40.0, 20.0, 0.9)
    assert low[0] < mu[0] < high[0]


def test_quantiles_are_monotone():
    mu = np.array([400.0])
    values = [pv.quantile("receivingYards", mu, 100.0, 10.0, q)[0]
              for q in (0.05, 0.25, 0.5, 0.75, 0.95)]
    assert values == sorted(values)


def test_a_wider_variance_gives_a_wider_interval():
    mu = np.array([400.0])
    narrow = (pv.quantile("receivingYards", mu, 10.0, 1000.0, 0.9)[0]
              - pv.quantile("receivingYards", mu, 10.0, 1000.0, 0.1)[0])
    wide = (pv.quantile("receivingYards", mu, 200.0, 1000.0, 0.9)[0]
            - pv.quantile("receivingYards", mu, 200.0, 1000.0, 0.1)[0])
    assert wide > narrow


def test_a_stat_outside_both_families_gets_nothing():
    assert pv.quantile("somethingElse", np.array([1.0]), 1.0, 1.0, 0.5) is None
    assert pv.moments("somethingElse", np.array([1.0]), 1.0, 1.0) is None


# --- the bust mass --------------------------------------------------------

def test_the_bust_mass_pushes_the_lower_tail_to_zero():
    """A Gamma has no mass at zero and this population does -- 10.5% of receiving
    yard rows realise exactly 0, and 59% of the rows below the p10 produced under 5%
    of their projection. Without it the lower tail leaked at twice its nominal
    rate."""
    mu = np.array([400.0])
    without = pv.quantile("receivingYards", mu, 100.0, 10.0, 0.1, bust=0.0)[0]
    with_bust = pv.quantile("receivingYards", mu, 100.0, 10.0, 0.1, bust=0.15)[0]
    assert without > 0
    assert with_bust == 0.0


def test_the_bust_mass_does_not_move_the_mean():
    """The point estimate is what plan 18 measured. An interval must not shift it."""
    mu = np.array([400.0])
    median = pv.quantile("receivingYards", mu, 100.0, 10.0, 0.5, bust=0.2)[0]
    assert median > 0
    # the conditional Gamma is rescaled up, so the median rises rather than falls
    plain = pv.quantile("receivingYards", mu, 100.0, 10.0, 0.5, bust=0.0)[0]
    assert median > plain


def test_counts_get_no_bust_mass_because_they_already_have_one():
    """A Negative Binomial carries mass at zero natively; adding a point mass on top
    would double-count it."""
    rng = np.random.default_rng(5)
    mu = rng.uniform(1, 20, 200)
    observed = rng.poisson(mu).astype(float)
    _, _, bust = pv.fit_variance(observed, mu, family="count")
    assert bust == 0.0


# --- integration ----------------------------------------------------------

def frame_with(positions, values):
    return pl.DataFrame({
        "position": positions,
        f"{sn.USAGE_PREFIX}receivingYards": values,
    })


def model_with(stat_dispersion):
    return sn.SeasonUsageModel(volume={}, stat_dispersion=stat_dispersion)


def test_intervals_are_null_not_nan_where_the_model_says_nothing():
    """The bug this file exists to prevent. Polars distinguishes null from NaN and
    `is_not_null()` is True for NaN, so a NaN here survives every downstream null
    filter and then compares False against everything -- which scored abstained
    players as outside their own interval and reported 6% coverage."""
    model = model_with({pv.key("WR", "receivingYards"): {"phi": 100.0, "k": 10.0}})
    out = model.stat_intervals(frame_with(["WR", "QB"], [400.0, None]))

    column = f"{sn.USAGE_PREFIX}receivingYards_low"
    assert out[column][0] is not None
    assert out[column][1] is None
    assert out.filter(pl.col(column).is_not_null()).height == 1


def test_a_position_without_a_fitted_pair_gets_no_interval():
    """Partial coverage is visible; an invented number is not."""
    model = model_with({pv.key("WR", "receivingYards"): {"phi": 100.0, "k": 10.0}})
    out = model.stat_intervals(frame_with(["WR", "TE"], [400.0, 400.0]))
    column = f"{sn.USAGE_PREFIX}receivingYards_low"
    assert out[column][0] is not None
    assert out[column][1] is None


def test_the_interval_does_not_alter_the_projection():
    model = model_with({pv.key("WR", "receivingYards"): {"phi": 100.0, "k": 10.0}})
    frame = frame_with(["WR"], [400.0])
    out = model.stat_intervals(frame)
    assert out[f"{sn.USAGE_PREFIX}receivingYards"][0] == pytest.approx(400.0)


def test_dispersion_survives_a_save_and_load(tmp_path):
    model = model_with({pv.key("WR", "receivingYards"):
                        {"phi": 100.0, "k": 10.0, "bust": 0.1}})
    back = sn.SeasonUsageModel.load(model.save(tmp_path / "m.json"))
    assert back.stat_dispersion == model.stat_dispersion


# --- the probability transform, added for plan 28 -------------------------

def test_the_transform_of_a_draw_from_the_fitted_law_is_uniform():
    """``pit`` is the inverse of ``quantile``, and that is what makes a copula honest.

    :mod:`Scripts.outcomes.distribution` correlates stats by correlating their
    probability transforms. If the transform is not uniform on draws from the fitted
    law, the correlations it measures are attenuated by whatever the non-uniformity is
    doing -- worst for the low-count stats, which have the most ties. Both families put
    mass on atoms, so the randomisation is not a refinement, it is the thing that makes
    this true at all.
    """
    rng = np.random.default_rng(4)
    for stat, bust in (("receivingYards", 0.12), ("receivingYards", 0.0),
                       ("receivingReceptions", 0.0), ("rushingTouchdowns", 0.0)):
        mu = np.full(40000, 700.0 if "Yards" in stat else 8.0)
        drawn = pv.quantile(stat, mu, 3.0, 4.0, rng.random(mu.size), bust=bust)
        transformed = pv.pit(stat, mu, 3.0, 4.0, drawn, bust=bust, rng=rng)
        for target in (10, 50, 90):
            assert np.percentile(transformed, target) == pytest.approx(
                target / 100, abs=0.015), f"{stat} bust={bust} at p{target}"


def test_the_transform_is_ordered_in_the_observation():
    """A bigger season transforms higher. Cheap, and it catches a swapped bound."""
    mu = np.full(5, 500.0)
    got = pv.pit("receivingYards", mu, 3.0, 4.0,
                 np.array([0.0, 100.0, 500.0, 900.0, 2000.0]), bust=0.1)
    assert list(got) == sorted(got)


def test_a_stat_with_no_family_has_no_transform():
    """Same contract as ``quantile``: None rather than an invented number."""
    assert pv.pit("passingCompletions", np.array([10.0]), 1.0, 1.0,
                  np.array([9.0])) is None


# --- the withdrawn interval, plan 34 F5 ----------------------------------

def test_the_quarterback_passing_interval_is_withdrawn_not_widened():
    """It covers 58.9% against a nominal 80%, and no dispersion fixes that.

    Realised QB season passing yards are **left**-skewed as a ratio of a 3,000+
    prior season -- p10 0.43, p50 0.90, p90 1.17, so
    ``(p90-p50)/(p50-p10) = 0.57`` -- while a Gamma at the matching shape gives
    1.57. Matched to the empirical p10 its p90 lands 44% too high; matched to the
    p90 its p10 lands at 0.84 against 0.43. The family has the skew inverted, so
    the interval is withdrawn on the principle ``stat_intervals`` already applies
    to a stat with no fitted dispersion.
    """
    assert not pv.is_calibrated("QB", "passingYards")
    assert pv.is_calibrated("QB", "passingTouchdowns")
    assert pv.is_calibrated("WR", "receivingYards")


def test_only_the_measured_pair_is_withdrawn():
    """A blanket withdrawal would take seven calibrated intervals with it."""
    assert pv.UNCALIBRATED == (("QB", "passingYards"),)


def test_a_gamma_cannot_hold_the_quarterback_shape():
    """The claim the withdrawal rests on, checked rather than asserted."""
    from scipy import optimize, stats

    empirical = {10: 0.43, 50: 0.90, 90: 1.17}
    shape = optimize.brentq(
        lambda s: stats.gamma.ppf(0.10, s, scale=1.0 / s) - empirical[10],
        0.5, 200.0)
    gamma_p90 = stats.gamma.ppf(0.90, shape, scale=1.0 / shape)
    assert gamma_p90 > empirical[90] * 1.30, "matched at p10, p90 must overshoot badly"

    def skew(p10, p50, p90):
        return (p90 - p50) / (p50 - p10)

    assert skew(**{"p10": empirical[10], "p50": empirical[50],
                   "p90": empirical[90]}) < 1.0, "the outcome is left-skewed"
    assert skew(p10=stats.gamma.ppf(0.10, shape, scale=1.0 / shape),
                p50=stats.gamma.ppf(0.50, shape, scale=1.0 / shape),
                p90=gamma_p90) > 1.0, "a Gamma is right-skewed"
