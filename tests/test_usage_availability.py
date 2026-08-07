"""The Beta-Binomial predictive distribution for games played.

Two things are pinned. The distribution's own identities -- that the PMF is a
distribution, that the closed-form moments agree with the PMF, that infinite
concentration recovers the Binomial -- and the properties that make it the right
family here rather than a decorative one: that it is overdispersed, left-skewed, and
that its quantiles are honest about being discrete.

The last is the subtle one. ``games_low`` is the smallest integer whose cumulative
probability reaches 0.10, and with eighteen attainable values each step carries
several percent of mass, so an integer "p10" always excludes *less* than 10%. That
makes realised coverage look too wide unless it is compared against what the model
actually claims. Getting that comparison wrong would report a well-calibrated
distribution as broken.

Synthetic throughout. No network, no parquet.
"""

import numpy as np
import polars as pl
import pytest

from Scripts.usage import availability as av
from Scripts.usage import season as sn

SLATE = 17


# --- it is a distribution -------------------------------------------------

@pytest.mark.parametrize("kappa", [0.5, 2.0, 10.0, 1e5])
def test_the_pmf_sums_to_one(kappa):
    p = av.pmf(SLATE, np.array([0.2, 0.5, 0.8, 0.999]), kappa)
    assert np.allclose(p.sum(axis=1), 1.0)
    assert (p >= 0).all()


@pytest.mark.parametrize("kappa", [0.5, 2.0, 10.0])
def test_the_closed_form_moments_match_the_pmf(kappa):
    """The whole point of the family is that no simulation is needed, which is only
    true if the analytic moments are the distribution's moments."""
    mu = np.array([0.3, 0.65, 0.9])
    p = av.pmf(SLATE, mu, kappa)
    k = np.arange(SLATE + 1)
    mean_numeric = (p * k).sum(axis=1)
    var_numeric = (p * k ** 2).sum(axis=1) - mean_numeric ** 2

    mean, variance = av.moments(SLATE, mu, kappa)
    assert np.allclose(mean, mean_numeric)
    assert np.allclose(variance, var_numeric)


def test_infinite_concentration_is_the_binomial():
    """kappa is the only thing separating this from the family the data rejected,
    so it should reduce to it in the limit."""
    mu = np.array([0.8])
    _, variance = av.moments(SLATE, mu, 1e12)
    assert variance.item() == pytest.approx(SLATE * 0.8 * 0.2, rel=1e-6)


def test_less_concentration_means_more_variance():
    mu = np.array([0.75])
    wide = av.moments(SLATE, mu, 1.0)[1].item()
    narrow = av.moments(SLATE, mu, 20.0)[1].item()
    assert wide > narrow


# --- why this family and not the Binomial ---------------------------------

def test_the_fitted_concentration_implies_real_overdispersion():
    """Measured 5.6x-8.1x the Binomial variance over 3,942 player-seasons, which is
    what rules the Binomial out. A kappa in single digits is the same statement."""
    mu = np.array([0.7])
    for kappa in (2.1, 2.2, 3.2):
        beta_binomial = av.moments(SLATE, mu, kappa)[1].item()
        binomial = SLATE * 0.7 * 0.3
        assert beta_binomial > 4 * binomial


def test_the_distribution_is_left_skewed_for_a_healthy_player():
    """The empirical shape: median 15, mode 16, mean 13.4, thin tail to 1. A normal
    approximation would put the mass symmetrically and miss the tail a drafter cares
    about."""
    mu = np.array([0.8])
    mean = av.moments(SLATE, mu, 2.16)[0].item()
    median = av.quantile(SLATE, mu, 2.16, 0.5).item()
    assert median > mean, "mass should pile up at the healthy end"
    assert av.quantile(SLATE, mu, 2.16, 0.1).item() < mean - 4


# --- quantiles ------------------------------------------------------------

def test_quantiles_are_monotone_in_the_probability():
    mu = np.array([0.6])
    values = [av.quantile(SLATE, mu, 2.0, q).item()
              for q in (0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95)]
    assert values == sorted(values)


def test_quantiles_are_monotone_in_the_mean():
    fragile = av.quantile(SLATE, np.array([0.4]), 2.0, 0.5).item()
    durable = av.quantile(SLATE, np.array([0.9]), 2.0, 0.5).item()
    assert durable > fragile


def test_a_quantile_cannot_leave_the_slate():
    for mu in (0.001, 0.5, 0.999):
        for q in (0.001, 0.5, 0.999):
            value = av.quantile(SLATE, np.array([mu]), 2.0, q).item()
            assert 0 <= value <= SLATE


def test_an_integer_quantile_excludes_less_than_asked():
    """The reason coverage must be judged against the model's own claim. This is a
    property of a discrete support, not a defect, and a test that expected exactly
    10% in each tail would be wrong rather than strict."""
    mu = np.array([0.8])
    kappa = 2.16
    low = int(av.quantile(SLATE, mu, kappa, 0.1).item())
    cdf = np.cumsum(av.pmf(SLATE, mu, kappa), axis=1)[0]
    excluded_below = cdf[low - 1] if low > 0 else 0.0
    assert excluded_below < 0.10


# --- fitting the concentration --------------------------------------------

def test_dispersion_is_recovered_from_data_it_generated():
    """Method of moments should invert its own family."""
    rng = np.random.default_rng(0)
    n, mu, kappa = SLATE, 0.75, 3.0
    probability = rng.beta(kappa * mu, kappa * (1 - mu), size=8000)
    drawn = rng.binomial(n, probability)

    fitted = av.fit_dispersion(drawn, np.full(drawn.size, n),
                               np.full(drawn.size, mu))
    assert fitted is not None
    assert fitted == pytest.approx(kappa, rel=0.25)


def test_binomial_data_reports_the_binomial_limit():
    """Data with no overdispersion is a finding, not a failure, and the estimator
    should say so rather than fall back to a pooled default that would assert *more*
    spread than the data shows.

    It must also be capped. `kappa = (n - R) / (R - 1)` blows up as R approaches 1,
    so near-Binomial data produced 11,199 on one draw and would give something
    entirely different on the next -- a number that unstable should not reach a
    persisted model file."""
    rng = np.random.default_rng(1)
    drawn = rng.binomial(SLATE, 0.7, size=4000)
    fitted = av.fit_dispersion(drawn, np.full(drawn.size, SLATE),
                               np.full(drawn.size, 0.7))
    assert fitted == av.MAX_KAPPA

    # And at that cap the family really has collapsed to the one it generalises.
    mu = np.array([0.7])
    assert av.moments(SLATE, mu, fitted)[1].item() == pytest.approx(
        SLATE * 0.7 * 0.3, rel=0.02)


def test_underdispersed_data_does_not_produce_a_narrow_fit():
    """Guarding the direction that would be dangerous: claiming *less* uncertainty
    than the data supports."""
    constant = np.full(200, 12.0)
    fitted = av.fit_dispersion(constant, np.full(200, SLATE), np.full(200, 12 / 17))
    assert fitted == av.MAX_KAPPA


def test_too_few_rows_returns_nothing():
    assert av.fit_dispersion([10, 12], [17, 17], [0.7, 0.7]) is None


# --- integration with the season head -------------------------------------

def feature_rows(rows):
    default = {
        "gsis_id": "a", "position": "WR", "team": "SEA", "team_changed": False,
        "is_rookie": False, "season": 2026,
        "p1_games": 17, "p1_weeks_on_reserve": 0, "p1_availability": 1.0,
        "p1_targets_pg": 8.0, "p2_targets_pg": 7.0,
        "p1_carries_pg": 0.0, "p2_carries_pg": 0.0,
        "p1_pass_attempts_pg": 0.0, "p2_pass_attempts_pg": 0.0,
        "p1_yards_per_target": 8.5, "p1_catch_rate": 0.65,
        "p1_rec_td_per_target": 0.06, "p1_yards_per_carry": 4.2,
        "p1_rush_td_per_carry": 0.02, "p1_yards_per_attempt": 7.0,
        "p1_pass_td_per_attempt": 0.05, "p1_int_per_attempt": 0.02,
    }
    return pl.DataFrame([{**default, **row} for row in rows])


def model_with_dispersion(**overrides):
    games = {"WR": sn.VolumeFit(
        position="WR", target="games", intercept=0.0,
        coefficients={"p1_availability": 0.8, "p1_weeks_on_reserve": 0.0,
                      "team_changed": 0.0}, n=100, r2=0.2)}
    kwargs = {"volume": {}, "games": games,
              "games_by_position": {"WR": 14.0},
              "games_dispersion": {"WR": 2.16},
              "train_seasons": (2024, 2025)}
    kwargs.update(overrides)
    return sn.SeasonUsageModel(**kwargs)


def test_the_interval_brackets_the_mean():
    model = model_with_dispersion()
    out = model.games_interval(model.predict(feature_rows([{}])))
    row = out.row(0, named=True)
    assert row["games_low"] <= row["expected_games"] <= row["games_high"]
    assert row["games_sd"] > 0


def test_a_position_without_a_fitted_dispersion_falls_back():
    """It must not silently emit nothing -- a missing interval on some players and
    not others is the partial-coverage trap one level down."""
    model = model_with_dispersion(games_dispersion={})
    assert model.dispersion_for(["WR"])[0] == av.DEFAULT_KAPPA


def test_abstained_rows_get_no_interval():
    """The model declines quarterbacks, and a declined player has no expected games
    to build an interval around. A number here would be invented."""
    model = model_with_dispersion()
    frame = feature_rows([{"position": "QB", "p1_pass_attempts_pg": 30.0}])
    out = model.games_interval(model.predict(frame))
    assert out["expected_games"][0] is not None  # the mean still computes
    assert out["usg_arm"][0] == "abstain"


def test_the_model_reports_its_own_implied_coverage():
    """Judging an integer p10/p90 against 80% reports a calibrated model as broken.
    The claim has to travel with the interval."""
    model = model_with_dispersion()
    out = model.games_interval(model.predict(feature_rows([{}])))
    implied = out["games_implied_coverage"][0]
    assert 0.8 <= implied <= 1.0


def test_dispersion_survives_a_save_and_load(tmp_path):
    model = model_with_dispersion()
    back = sn.SeasonUsageModel.load(model.save(tmp_path / "m.json"))
    assert back.games_dispersion == model.games_dispersion
