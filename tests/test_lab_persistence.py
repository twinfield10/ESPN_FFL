"""Year-over-year persistence, and the two ways a persistence figure misleads.

* **A gap season is not an adjacent one.** A player who misses 2020 has a 2019 and
  a 2021; treating them as consecutive measures two years of drift and reports it
  as one, which biases every figure downward for exactly the players whose
  availability is already the hard part.
* **A rate off three targets is not an observation of efficiency.** Pooling thin
  denominators in drives every correlation toward zero for a reason that has
  nothing to do with forecastability, and the conclusion drawn from it -- shrink
  harder -- would be right by accident.

Synthetic frames. No data pull, no network.
"""

import math

import polars as pl
import pytest

from Scripts.lab import persistence as pers
from Scripts.usage import features as ft


@pytest.fixture(autouse=True)
def tiny_samples(monkeypatch):
    """Let a handful of rows be a valid measurement."""
    monkeypatch.setattr(pers, "MIN_PAIRS", 2)


def totals(rows):
    """Build a season-totals-shaped frame.

    Args:
        rows: Dicts with ``gsis_id``, ``season``, ``position`` and any of
            ``games``, ``carries_pg``, ``rush_td_per_carry``, ``tot_carries``.

    Returns:
        pl.DataFrame: The columns the study reads.
    """
    return pl.DataFrame({
        "gsis_id": [r["gsis_id"] for r in rows],
        "season": [int(r["season"]) for r in rows],
        "position": [r.get("position", "RB") for r in rows],
        "games": [int(r.get("games", 16)) for r in rows],
        "carries_pg": [float(r.get("carries_pg", 10.0)) for r in rows],
        "tot_carries": [float(r.get("tot_carries", 160.0)) for r in rows],
        "rush_td_per_carry": [float(r.get("rush_td_per_carry", 0.03))
                              for r in rows],
    })


# --- pairing -------------------------------------------------------------

def test_a_missed_season_produces_no_pair_across_the_gap():
    """2019 and 2021 are not consecutive, and must not be joined as if they were."""
    frame = totals([
        {"gsis_id": "a", "season": 2019}, {"gsis_id": "a", "season": 2021},
    ])
    assert pers.pairs(frame).height == 0


def test_consecutive_seasons_pair_and_carry_both_sides():
    """The later season arrives as ``_next`` on the earlier row."""
    frame = totals([
        {"gsis_id": "a", "season": 2019, "carries_pg": 8.0},
        {"gsis_id": "a", "season": 2020, "carries_pg": 12.0},
    ])
    paired = pers.pairs(frame)
    assert paired.height == 1
    assert paired["carries_pg"][0] == pytest.approx(8.0)
    assert paired["carries_pg_next"][0] == pytest.approx(12.0)


def test_pairs_never_cross_players():
    """Two players' seasons are not each other's next season."""
    frame = totals([
        {"gsis_id": "a", "season": 2019}, {"gsis_id": "b", "season": 2020},
    ])
    assert pers.pairs(frame).height == 0


# --- the denominator floor -----------------------------------------------

def test_a_thin_denominator_on_either_side_excludes_the_pair():
    """Both seasons must clear the floor, not just the one being read."""
    frame = totals([
        {"gsis_id": "a", "season": 2019, "tot_carries": 200.0},
        {"gsis_id": "a", "season": 2020, "tot_carries": 3.0},
        {"gsis_id": "b", "season": 2019, "tot_carries": 200.0},
        {"gsis_id": "b", "season": 2020, "tot_carries": 200.0},
        {"gsis_id": "c", "season": 2019, "tot_carries": 200.0},
        {"gsis_id": "c", "season": 2020, "tot_carries": 200.0},
    ])
    result = pers.rate_persistence(pers.pairs(frame), "rush_td_per_carry",
                                   "tot_carries", min_denominator=25.0)
    assert result["n"] == 2


def test_a_rate_is_measured_only_on_the_positions_it_belongs_to():
    """A quarterback's catch rate does not enter the receiving numbers."""
    assert "QB" not in pers.RATE_POSITIONS["catch_rate"]
    assert pers.RATE_POSITIONS["yards_per_attempt"] == ("QB",)


# --- the implied constant ------------------------------------------------

def test_implied_k_follows_the_credibility_identity():
    """``k = n (1 - r) / r`` at the median denominator, and nothing else."""
    frame = totals([
        {"gsis_id": chr(97 + i), "season": season,
         "tot_carries": 100.0,
         "rush_td_per_carry": value}
        for i, (a, b) in enumerate([(0.01, 0.02), (0.03, 0.05), (0.05, 0.04),
                                    (0.02, 0.01), (0.06, 0.07)])
        for season, value in ((2019, a), (2020, b))
    ])
    result = pers.rate_persistence(pers.pairs(frame), "rush_td_per_carry",
                                   "tot_carries", min_denominator=25.0)
    expected = (result["median_denominator"]
                * (1.0 - result["pearson"]) / result["pearson"])
    assert result["implied_k"] == pytest.approx(expected)
    assert result["median_denominator"] == pytest.approx(100.0)


def test_a_non_positive_correlation_implies_unbounded_shrinkage():
    """No signal means the player's own rate earns no weight, not a negative one."""
    frame = totals([
        {"gsis_id": chr(97 + i), "season": season, "tot_carries": 100.0,
         "rush_td_per_carry": value}
        for i, (a, b) in enumerate([(0.01, 0.09), (0.02, 0.08), (0.03, 0.07),
                                    (0.04, 0.06)])
        for season, value in ((2019, a), (2020, b))
    ])
    result = pers.rate_persistence(pers.pairs(frame), "rush_td_per_carry",
                                   "tot_carries", min_denominator=25.0)
    assert result["pearson"] < 0
    assert math.isinf(result["implied_k"])


def test_every_shipped_shrinkage_constant_is_reported_against_its_rate():
    """The comparison exists for every rate the model shrinks, or it proves nothing."""
    assert set(pers.RATE_POSITIONS) == set(ft.SHRINKAGE_K)
