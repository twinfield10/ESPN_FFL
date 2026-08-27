"""Scoring the shipping blend per stat, and the three ways that comparison lies.

The module under test exists because a fantasy-point MAE cannot see a per-stat
regression. These tests pin the properties that make the per-stat number
trustworthy in its place:

* **Pairing.** The blend is dense and its inputs are sparse, so a comparison that
  scores each on its own rows measures coverage and reports it as accuracy.
* **Provenance.** An imputed cell is the ESPN/FantasyPros mean wearing another
  source's badge; scoring a source against its own fill-in is how the old blend
  weights got their reputation.
* **Population.** A quarterback's ``receivingYards`` row is 0 projected and 0
  realised by every source, and there are more quarterback-weeks than tight-end
  weeks. Pooling them in makes every source look identical.

Synthetic frames. No store, no network.
"""

import polars as pl
import pytest

from Scripts.lab import accuracy
from Scripts.lab import registry as reg
from Scripts.usage.nflverse import ACTUAL_PREFIX

STAT = "receivingYards"


@pytest.fixture(autouse=True)
def tiny_samples(monkeypatch):
    """Let a four-row fixture be a valid comparison."""
    monkeypatch.setattr(accuracy, "MIN_PAIRED_ROWS", 1)


def frame(rows):
    """Build an eval-shaped frame for one stat.

    Args:
        rows: Dicts with ``actual``, ``espn``, ``fp``, ``blend``, optional
            ``fp_imputed`` and ``position``.

    Returns:
        pl.DataFrame: Columns the scoreboard reads.
    """
    return pl.DataFrame({
        "week": list(range(1, len(rows) + 1)),
        "player_id": [str(i) for i in range(len(rows))],
        "primaryPosition": [r.get("position", "WR") for r in rows],
        f"{ACTUAL_PREFIX}{STAT}": [float(r["actual"]) for r in rows],
        f"ESPN_{STAT}": [float(r["espn"]) for r in rows],
        f"FP_{STAT}": [float(r["fp"]) for r in rows],
        f"FP_{STAT}_is_imputed": [bool(r.get("fp_imputed", False)) for r in rows],
        f"TRUE_{STAT}": [float(r["blend"]) for r in rows],
        "played": [bool(r.get("played", True)) for r in rows],
        "team_played": [bool(r.get("team_played", True)) for r in rows],
    })


# --- pairing -------------------------------------------------------------

def test_both_numbers_come_from_the_same_cells(monkeypatch):
    """The blend is scored on the source's rows, not on all of its own.

    Row 2 is one the source has no line for. Including it would move the blend's
    number and not the source's, which is the comparison silently becoming a
    coverage report.
    """
    rows = [
        {"actual": 100.0, "espn": 90.0, "fp": 80.0, "blend": 85.0},
        {"actual": 100.0, "espn": 90.0, "fp": 0.0, "blend": 10.0,
         "fp_imputed": True},
    ]
    result = accuracy.paired(frame(rows), STAT, "FP")
    assert result["n"] == 1
    assert result["source_mae"] == pytest.approx(20.0)
    assert result["blend_mae"] == pytest.approx(15.0)
    assert result["delta_pct"] == pytest.approx(-25.0)


def test_an_imputed_cell_is_never_scored(monkeypatch):
    """A filled-in value is not that source's opinion and does not count."""
    rows = [{"actual": 100.0, "espn": 90.0, "fp": 95.0, "blend": 92.0,
             "fp_imputed": True}]
    assert accuracy.paired(frame(rows), STAT, "FP") is None
    # ESPN has no provenance column, which real_mask reads as always real.
    assert accuracy.paired(frame(rows), STAT, "ESPN")["n"] == 1


def test_too_few_paired_cells_returns_nothing(monkeypatch):
    """Below the floor the comparison is not reported at all, rather than shown thin."""
    monkeypatch.setattr(accuracy, "MIN_PAIRED_ROWS", 5)
    rows = [{"actual": 100.0, "espn": 90.0, "fp": 80.0, "blend": 85.0}]
    assert accuracy.paired(frame(rows), STAT, "FP") is None


# --- population ----------------------------------------------------------

def test_a_stat_is_scored_only_on_the_positions_it_is_a_question_for():
    """Quarterback receiving rows do not dilute the receiving comparison."""
    rows = [
        {"actual": 100.0, "espn": 60.0, "fp": 60.0, "blend": 60.0,
         "position": "WR"},
        {"actual": 0.0, "espn": 0.0, "fp": 0.0, "blend": 0.0, "position": "QB"},
    ]
    result = accuracy.paired(frame(rows), STAT, "ESPN")
    assert result["n"] == 1
    assert result["source_mae"] == pytest.approx(40.0)
    assert "QB" not in accuracy.STAT_POSITIONS[STAT]


def test_populations_filter_and_do_not_reweight():
    """``played`` drops the inactive row; the remaining number is unchanged by it."""
    rows = [
        {"actual": 100.0, "espn": 60.0, "fp": 60.0, "blend": 60.0},
        {"actual": 0.0, "espn": 80.0, "fp": 80.0, "blend": 80.0, "played": False},
    ]
    everything = accuracy.paired(frame(rows), STAT, "ESPN", population="all")
    playing = accuracy.paired(frame(rows), STAT, "ESPN", population="played")
    assert everything["n"] == 2 and playing["n"] == 1
    assert everything["source_mae"] == pytest.approx(60.0)
    assert playing["source_mae"] == pytest.approx(40.0)


def test_an_unknown_population_raises_rather_than_scoring_everything():
    """A typo must not silently become the ``all`` population."""
    rows = [{"actual": 100.0, "espn": 90.0, "fp": 80.0, "blend": 85.0}]
    with pytest.raises(ValueError, match="unknown population"):
        accuracy.paired(frame(rows), STAT, "ESPN", population="playedd")


# --- the decision rule ---------------------------------------------------

def test_defects_apply_the_registry_threshold_mechanically():
    """The bar is the lab's own, not one chosen here after seeing the numbers."""
    board = {
        "rushingTouchdowns": {
            "ESPN": {"n": 3200, "delta_pct": reg.MAX_STAT_MAE_INCREASE_PCT + 0.4},
            "BOL": {"n": 2604, "delta_pct": -9.4},
        },
        "receivingYards": {
            "ESPN": {"n": 3154, "delta_pct": reg.MAX_STAT_MAE_INCREASE_PCT - 0.1},
        },
    }
    found = accuracy.defects(board)
    assert [row["stat"] for row in found] == ["rushingTouchdowns"]
    assert [row["source"] for row in found] == ["ESPN"]


def test_defects_are_ordered_worst_first():
    """The reader should not have to sort the list to find the largest."""
    board = {"rushingTouchdowns": {"ESPN": {"n": 10, "delta_pct": 2.4},
                                   "FP": {"n": 10, "delta_pct": 5.1},
                                   "PINNY": {"n": 10, "delta_pct": 6.0}}}
    assert [row["source"] for row in accuracy.defects(board)] == \
        ["PINNY", "FP", "ESPN"]


# --- calibration, where MAE cannot judge ---------------------------------

def test_mae_prefers_predicting_zero_on_a_sparse_count():
    """The reason calibration exists beside MAE, shown on nine rows.

    An RB's weekly receiving touchdowns are 0 about 95% of the time. A source that
    projects a flat 0.0 beats one projecting the true expectation on MAE, because
    MAE is minimised by the median and the median is zero. BetOnline does exactly
    that -- 988 of 995 RB player-weeks carry ``BOL_receivingTouchdowns == 0``.
    """
    rows = [{"actual": 0.0, "espn": 0.30, "fp": 0.0, "blend": 0.30}] * 8
    rows += [{"actual": 1.0, "espn": 0.30, "fp": 0.0, "blend": 0.30}]
    f = frame(rows)

    honest = accuracy.paired(f, STAT, "ESPN")     # projects the expectation
    flat = accuracy.paired(f, STAT, "FP")         # projects zero
    assert flat["source_mae"] < honest["source_mae"], (
        "MAE rewards the flat zero, which is the whole problem")

    # Calibration says the opposite, and says it correctly.
    assert accuracy.calibration(f, STAT, source="ESPN")["ratio"] == \
        pytest.approx(2.7, rel=0.01)
    assert accuracy.calibration(f, STAT, source="FP")["ratio"] == pytest.approx(0.0)


def test_calibration_is_a_ratio_of_totals_not_a_mean_of_ratios():
    """A player-week with one realised touchdown must not dominate the number."""
    rows = [{"actual": 0.0, "espn": 0.1, "fp": 0.1, "blend": 0.1}] * 9
    rows += [{"actual": 1.0, "espn": 0.1, "fp": 0.1, "blend": 0.1}]
    result = accuracy.calibration(frame(rows), STAT, source="ESPN")
    assert result["realised"] == pytest.approx(1.0)
    assert result["projected"] == pytest.approx(1.0)
    assert result["ratio"] == pytest.approx(1.0)


def test_calibration_returns_nothing_when_nothing_realised():
    """A ratio with a zero denominator is not a calibration."""
    rows = [{"actual": 0.0, "espn": 0.1, "fp": 0.1, "blend": 0.1}] * 40
    assert accuracy.calibration(frame(rows), STAT, source="ESPN") is None


def test_only_the_sparse_counts_get_a_calibration_row():
    """Yardage is dense and its median is far from zero, so MAE judges it fairly."""
    assert "receivingYards" not in accuracy.CALIBRATION_STATS
    assert "rushingTouchdowns" in accuracy.CALIBRATION_STATS
