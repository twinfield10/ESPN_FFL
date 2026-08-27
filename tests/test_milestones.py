"""Yardage-milestone bands: the ladder, the tail, and the two bugs found building it.

A milestone bonus is a step function of a weekly quantity, so its expected value is
a band probability times games played. Three properties make that trustworthy and
each was wrong at some point during the build:

* **Monotonicity.** A player projected for more yards must not get a *lower*
  probability of clearing 100. The fitted zero point mass broke exactly this, because
  past ``CV^2 = s/(1-s)`` the zero-inflated mixture is infeasible.
* **The ladder partitions.** ``P(100-199) + P(200+)`` has to equal ``P(>= 100)``, or
  points are double-counted or lost between tiers.
* **The tail is counted, not extrapolated.** Taken from the fitted Gamma the 200+
  tiers came out 20-25x too small.

No data pull, no network: the model is constructed directly.
"""

import numpy as np
import pandas as pd
import pytest

from Scripts.projection_utils import blended_stats
from Scripts.scrape_player_stats import DERIVED_STATS
from Scripts.usage import milestones as ms
from Scripts.usage import predictive as pv


def model(**overrides) -> ms.MilestoneModel:
    """A model with WR receiving dispersion roughly as fitted, and counted tails."""
    built = ms.MilestoneModel(
        dispersion={"WR|receivingYards": (20.48, 55.56, 0.0, 20462),
                    "RB|rushingYards": (19.93, 1000.0, 0.0, 12649)},
        pooled={"receivingYards": (20.48, 55.56, 0.0, 20462)},
        tail_share={"receivingYards|100": (1.0, 1775),
                    "receivingYards|200": (0.0175, 31),
                    "rushingYards|100": (1.0, 973),
                    "rushingYards|200": (0.0257, 25)},
        seasons=[2016, 2025],
    )
    for key, value in overrides.items():
        setattr(built, key, value)
    return built


# --- the ladder ----------------------------------------------------------

def test_the_names_match_the_pipeline_exactly():
    """A band this writes must be a band the scoring registry prices."""
    assert set(ms.BANDS) == set(DERIVED_STATS)


def test_the_two_tiers_of_one_ladder_partition_the_tail():
    """``P(100-199) + P(200+)`` is ``P(>= 100)``; nothing is lost or counted twice."""
    m = model()
    mu = np.array([40.0, 70.0, 95.0])
    low = m.band_games("receivingYards100-199Game", mu, ["WR"] * 3, slate=1.0)
    high = m.band_games("receivingYards200+Game", mu, ["WR"] * 3, slate=1.0)
    entry = pv.band_probability("receivingYards", mu, 20.48, 55.56, 100.0, None, 0.0)
    np.testing.assert_allclose(low + high, entry, rtol=1e-9)


def test_the_upper_tier_comes_from_the_counted_rate_not_the_gamma():
    """Set the counted share to zero and the tier goes to zero, whatever the Gamma says."""
    m = model(tail_share={"receivingYards|100": (1.0, 1775),
                          "receivingYards|200": (0.0, 0)})
    high = m.band_games("receivingYards200+Game", np.array([95.0]), ["WR"], slate=1.0)
    assert high[0] == pytest.approx(0.0)
    # And the band below it absorbs the whole entry probability rather than losing it.
    low = m.band_games("receivingYards100-199Game", np.array([95.0]), ["WR"], slate=1.0)
    entry = pv.band_probability("receivingYards", np.array([95.0]), 20.48, 55.56,
                                100.0, None, 0.0)
    assert low[0] == pytest.approx(float(entry[0]))


def test_an_unknown_edge_withdraws_its_tier_rather_than_guessing():
    """No counted rate means no evidence, which is 0 and not a Gamma extrapolation."""
    m = model(tail_share={"receivingYards|100": (1.0, 1775)})
    assert m.share("receivingYards", 200.0) == 0.0
    assert m.share("receivingYards", 100.0) == 1.0


# --- monotonicity, the property the zero mass broke ----------------------

def test_more_projected_yards_never_means_fewer_expected_milestone_games():
    """The bug that made a 110-a-game receiver rank below a 90-a-game one.

    With the fitted zero point mass carried through, ``P(>= 100)`` went 0.470 at
    mu=85 and 0.522 at mu=110 but *0.700* at mu=95 -- because past
    ``CV^2 = s/(1-s)`` the mixture is infeasible and the Gamma collapsed to a spike.
    """
    m = model()
    mu = np.linspace(30.0, 130.0, 40)
    games = m.band_games("receivingYards100-199Game", mu, ["WR"] * mu.size, slate=1.0)
    total = games + m.band_games("receivingYards200+Game", mu, ["WR"] * mu.size,
                                 slate=1.0)
    assert np.all(np.diff(total) > -1e-12), "P(>= 100) must not fall with the projection"


def test_the_fit_drops_the_zero_point_mass_and_says_so():
    """It is stored as 0.0 rather than fitted, because the mixture is wrong here."""
    assert ms._no_bust((20.48, 55.56, 0.18)) == (20.48, 55.56, 0.0)


# --- the floor -----------------------------------------------------------

def test_a_mean_far_below_the_ladder_is_not_awarded_a_fraction_of_a_bonus():
    """A Gamma at mu=2 still returns a positive P(>= 100); across a league that sums."""
    m = model()
    games = m.band_games("receivingYards100-199Game", np.array([2.0, 90.0]),
                         ["WR", "WR"], slate=17.0)
    assert games[0] == 0.0
    assert games[1] > 1.0


def test_both_tiers_use_the_first_edge_as_the_floor():
    """Otherwise a player could qualify for the 200+ tier and not the 100-199 one."""
    m = model()
    mu = np.array([30.0])          # above 100*0.25, below 200*0.25
    low = m.band_games("receivingYards100-199Game", mu, ["WR"], slate=1.0)
    high = m.band_games("receivingYards200+Game", mu, ["WR"], slate=1.0)
    assert low[0] > 0.0 and high[0] > 0.0


def test_a_position_with_no_fit_is_left_at_zero_rather_than_pooled_silently():
    """A kicker has no receiving dispersion and must not inherit a receiver's."""
    m = model(pooled={})
    games = m.band_games("receivingYards100-199Game", np.array([90.0]), ["K"],
                         slate=17.0)
    assert games[0] == 0.0


# --- the blend must not see them ----------------------------------------

def test_derived_stats_are_excluded_from_the_blend():
    """Averaging four sources' readings of a non-linear function is meaningless.

    And ESPN's own six columns are identically zero, so blending them would have
    dragged every band to a fraction of itself.
    """
    out = blended_stats(["passingYards", "rushingYards100-199Game"])
    assert "rushingYards100-199Game" not in out
    assert "passingYards" in out


def test_a_league_with_no_milestone_rules_gets_no_columns():
    """Two of the nine leagues score these; the other seven must not change at all."""
    from Scripts.season_projections import attach_milestone_bands

    frame = pd.DataFrame({"primaryPosition": ["WR"], "TRUE_receivingYards": [1500.0]})
    out = attach_milestone_bands(frame.copy(), ["receivingYards"], ("TRUE",))
    pd.testing.assert_frame_equal(out, frame)


# --- the mixture feasibility guard, in predictive ------------------------

def test_an_infeasible_zero_mixture_gives_a_distribution_not_a_spike():
    """Clipping a negative conditional variance produced a point mass.

    At mu=110 with the pooled WR share of 0.18, the conditional variance comes out
    negative; clipped, the Gamma became a spike and ``P(>= 100)`` read 0.820 where
    the plain Gamma gives 0.522 and ten seasons of football give 0.510.
    """
    mu = np.array([110.0])
    p10 = float(pv.quantile("receivingYards", mu, 20.48, 55.56, 0.10, 0.18)[0])
    p90 = float(pv.quantile("receivingYards", mu, 20.48, 55.56, 0.90, 0.18)[0])
    assert p90 - p10 > 0.5 * 110.0, "an interval, not a spike"

    probability = float(pv.band_probability("receivingYards", mu, 20.48, 55.56,
                                            100.0, None, 0.18)[0])
    assert 0.45 < probability < 0.60


def test_a_feasible_zero_mixture_is_left_alone():
    """The low-mu end is where the zero mass was measured to matter."""
    mu = np.array([25.0])
    reparameterised = pv._reparameterise("receivingYards", mu, 20.48, 55.56, 0.18)
    assert reparameterised is not None
    _, _, share = reparameterised
    assert float(np.asarray(share).reshape(-1)[0]) == pytest.approx(0.18)
