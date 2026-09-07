"""The start/sit engine: slot inference, optimal lineups, swaps, add/drop.

Covers ``app/lineup.py``. No network and no store on disk -- rosters are synthesised
to the shape ``clean_lineups`` returns.

The two properties worth stating up front, because they are what the module's own
docstrings claim and what the tests below pin:

* **The optimiser is exact, not a heuristic.** Fantasy eligibility is a transversal
  matroid, so greedy-by-weight is optimal. ``test_greedy_matches_brute_force`` checks
  that against an exhaustive search rather than taking the argument's word for it.
* **Σ swap gains equals the lineup's total gain.** A start/sit list whose numbers do
  not add up to the headline is worse than no list, and pairing by *slot* rather than
  by player identity broke exactly this -- reporting 28 points of changes on a lineup
  worth 1.5 more. ``test_swap_gains_sum_to_the_total`` is that bug's regression test.
"""

import itertools
import sys
from pathlib import Path

import polars as pl
import pytest

# `app/` is not a package: Streamlit runs the page scripts directly with app/ on
# sys.path, so the modules import each other by bare name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import lineup as lu  # noqa: E402


# --- fixtures ------------------------------------------------------------

#: What ESPN reports as eligible for each position, trimmed to what matters here.
ELIGIBLE = {
    "QB": ["QB", "OP", "BE", "IR"],
    "RB": ["RB", "RB/WR", "RB/WR/TE", "OP", "BE", "IR"],
    "WR": ["RB/WR", "WR", "WR/TE", "RB/WR/TE", "OP", "BE", "IR"],
    "TE": ["TE", "WR/TE", "RB/WR/TE", "OP", "BE", "IR"],
    "K": ["K", "BE", "IR"],
    "D/ST": ["D/ST", "BE", "IR"],
    "LB": ["LB", "DP", "BE", "IR"],
}


def player(name, position, points, *, slot=None, status="active", player_id=None,
           eligible=None):
    """One lineups row, with the columns ``lineup`` reads."""
    return {
        "player_id": player_id if player_id is not None else abs(hash(name)) % 10**6,
        "player_name": name,
        "player_position": position,
        "primaryPosition": position,
        "slotPosition": slot if slot is not None else "BE",
        "eligiblePositions": eligible if eligible is not None else ELIGIBLE[position],
        "player_active_status": status,
        "TRUE_Points": points,
        "team_owner": "Owner",
        "week": 1,
    }


#: A standard flex league.
FLEX_SLOTS = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1}

#: Superflex, as Weenieless Wanderers plays it.
SUPERFLEX_SLOTS = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 2, "OP": 1,
                   "D/ST": 1}


def brute_force(rows, slots, points_column="TRUE_Points"):
    """The true optimum, by exhaustive assignment. Only usable on small rosters."""
    openings = [slot for slot, count in slots.items() for _ in range(int(count))]
    playable = [r for r in rows
                if r.get(points_column) is not None
                and r.get("player_active_status") == "active"]

    best = 0.0
    # Every subset of players against every ordering of the slots, keeping the best
    # *feasible prefix*. Partial fills have to count: with two QB-only players and
    # one QB slot no assignment fills every opening, and requiring a full fill
    # scored that roster zero.
    for size in range(min(len(openings), len(playable)), 0, -1):
        for chosen in itertools.permutations(playable, size):
            for order in set(itertools.permutations(openings, size)):
                if all(slot in row["eligiblePositions"]
                       for row, slot in zip(chosen, order)):
                    best = max(best, sum(row[points_column] for row in chosen))
    return best


# --- slot inference ------------------------------------------------------

def test_slots_come_from_what_is_being_started():
    """The observed lineup wins where the metadata is behind.

    GOP Degenerates' metadata says its only defensive slot is `DP: 1` while its
    lineups hold players in CB, DE, DT, LB and S -- the two artifacts are written by
    different commands on different days. Trusting the metadata alone would leave
    every IDP player unassignable.
    """
    frame = pl.DataFrame([
        player("A", "LB", 12.0, slot="LB"),
        player("B", "LB", 10.0, slot="DP"),
        player("C", "QB", 20.0, slot="QB"),
    ])
    slots = lu.slot_counts(frame, {"starting_slots": {"QB": 1, "DP": 1}})
    assert slots["LB"] == 1
    assert slots["DP"] == 1
    assert slots["QB"] == 1


def test_a_slot_nobody_filled_survives_from_the_metadata():
    """The metadata is a floor, so an empty slot does not vanish."""
    frame = pl.DataFrame([player("A", "QB", 20.0, slot="QB")])
    slots = lu.slot_counts(frame, {"starting_slots": {"QB": 1, "K": 1}})
    assert slots["K"] == 1


def test_a_slot_count_is_the_widest_any_one_team_filled():
    """One manager leaving a slot empty must not shrink the league's slots."""
    frame = pl.DataFrame([
        player("A", "WR", 12.0, slot="WR") | {"team_owner": "One"},
        player("B", "WR", 11.0, slot="WR") | {"team_owner": "One"},
        player("C", "WR", 10.0, slot="WR") | {"team_owner": "Two"},
    ])
    assert lu.slot_counts(frame, {"starting_slots": {}})["WR"] == 2


def test_bench_and_ir_are_never_slots():
    frame = pl.DataFrame([
        player("A", "QB", 20.0, slot="QB"),
        player("B", "RB", 10.0, slot="BE"),
        player("C", "RB", 9.0, slot="IR"),
    ])
    slots = lu.slot_counts(frame, {"starting_slots": {"QB": 1, "BE": 5, "IR": 1}})
    assert "BE" not in slots and "IR" not in slots


# --- the optimiser -------------------------------------------------------

def test_flex_takes_the_best_leftover():
    rows = [
        player("QB1", "QB", 20.0), player("RB1", "RB", 18.0),
        player("RB2", "RB", 15.0), player("RB3", "RB", 14.0),
        player("WR1", "WR", 17.0), player("WR2", "WR", 16.0),
        player("TE1", "TE", 10.0), player("K1", "K", 8.0),
        player("D1", "D/ST", 7.0),
    ]
    starters, total = lu.optimal_lineup(rows, FLEX_SLOTS, "TRUE_Points")
    assert len(starters) == sum(FLEX_SLOTS.values())
    flex = next(r for r in starters if r["slot"] == "RB/WR/TE")
    assert flex["player_name"] == "RB3"
    assert total == pytest.approx(125.0)


def test_superflex_starts_a_second_quarterback():
    """The `OP` slot is what makes a superflex league a different league.

    Handled through ESPN's own `eligiblePositions` rather than a hardcoded slot map,
    which is why no code here knows what "superflex" means.
    """
    rows = [
        player("QB1", "QB", 22.0), player("QB2", "QB", 19.0),
        player("RB1", "RB", 14.0), player("RB2", "RB", 13.0),
        player("WR1", "WR", 15.0), player("WR2", "WR", 12.0),
        player("WR3", "WR", 11.0), player("WR4", "WR", 10.0),
        player("TE1", "TE", 9.0), player("D1", "D/ST", 7.0),
    ]
    starters, _ = lu.optimal_lineup(rows, SUPERFLEX_SLOTS, "TRUE_Points")
    op = next(r for r in starters if r["slot"] == "OP")
    assert op["player_name"] == "QB2"


def test_players_who_cannot_play_are_never_started():
    """A lineup that starts a bye-week player is not a lineup you would set."""
    rows = [
        player("QB1", "QB", 30.0, status="bye"),
        player("QB2", "QB", 12.0),
    ]
    starters, total = lu.optimal_lineup(rows, {"QB": 1}, "TRUE_Points")
    assert [r["player_name"] for r in starters] == ["QB2"]
    assert total == pytest.approx(12.0)


def test_an_unfillable_slot_is_left_empty():
    """A roster with no kicker scores nothing at kicker, rather than raising."""
    rows = [player("QB1", "QB", 20.0)]
    starters, total = lu.optimal_lineup(rows, {"QB": 1, "K": 1}, "TRUE_Points")
    assert len(starters) == 1
    assert total == pytest.approx(20.0)


def test_no_slots_means_no_lineup():
    assert lu.optimal_lineup([player("A", "QB", 20.0)], {}, "TRUE_Points") == ([], 0.0)


@pytest.mark.parametrize("slots", [FLEX_SLOTS, SUPERFLEX_SLOTS])
def test_greedy_matches_brute_force(slots):
    """Greedy is exact on a matroid. Checked, not asserted.

    Kept small because brute force is factorial; the point is that the two agree at
    all, since a greedy assignment that could be beaten would be beaten here.
    """
    rows = [
        player("QB1", "QB", 21.0), player("QB2", "QB", 17.0),
        player("RB1", "RB", 16.0), player("RB2", "RB", 12.0),
        player("WR1", "WR", 15.0), player("WR2", "WR", 11.0),
        player("TE1", "TE", 13.0),
    ]
    trimmed = {slot: n for slot, n in slots.items()
               if slot in ("QB", "RB", "WR", "TE", "RB/WR/TE", "OP")}
    _, greedy = lu.optimal_lineup(rows, trimmed, "TRUE_Points")
    assert greedy == pytest.approx(brute_force(rows, trimmed))


# --- swaps ---------------------------------------------------------------

def test_a_lineup_already_optimal_has_no_swaps():
    rows = [player("QB1", "QB", 20.0, slot="QB"), player("QB2", "QB", 10.0)]
    current, _ = lu.current_lineup(rows, "TRUE_Points")
    optimal, _ = lu.optimal_lineup(rows, {"QB": 1}, "TRUE_Points")
    assert lu.swaps(current, optimal, "TRUE_Points") == []


def test_a_swap_names_both_players_and_the_gain():
    rows = [player("QB1", "QB", 20.0), player("QB2", "QB", 10.0, slot="QB")]
    current, _ = lu.current_lineup(rows, "TRUE_Points")
    optimal, _ = lu.optimal_lineup(rows, {"QB": 1}, "TRUE_Points")
    change, = lu.swaps(current, optimal, "TRUE_Points")
    assert (change.start, change.sit) == ("QB1", "QB2")
    assert change.gain == pytest.approx(10.0)


def test_an_empty_slot_reads_as_empty_rather_than_as_a_swap():
    """`sit` is None where nothing was started, which is a different mistake."""
    rows = [player("QB1", "QB", 20.0)]
    current, _ = lu.current_lineup(rows, "TRUE_Points")
    optimal, _ = lu.optimal_lineup(rows, {"QB": 1}, "TRUE_Points")
    change, = lu.swaps(current, optimal, "TRUE_Points")
    assert change.sit is None


def test_moving_a_player_between_slots_is_not_a_swap():
    """The bug this module's docstring records.

    Pickens shifting from RB/WR/TE to WR while Williams shifts the other way is a
    permutation worth nothing. Paired by slot it read as two swaps gaining 26 points
    on a lineup worth 1.5 more.
    """
    rows = [
        player("WR1", "WR", 15.0, slot="RB/WR/TE"),
        player("WR2", "WR", 14.0, slot="WR"),
        player("RB1", "RB", 12.0, slot="RB"),
    ]
    current, current_total = lu.current_lineup(rows, "TRUE_Points")
    optimal, optimal_total = lu.optimal_lineup(
        rows, {"WR": 1, "RB": 1, "RB/WR/TE": 1}, "TRUE_Points")
    assert optimal_total == pytest.approx(current_total)
    assert lu.swaps(current, optimal, "TRUE_Points") == []


@pytest.mark.parametrize("slots", [FLEX_SLOTS, SUPERFLEX_SLOTS])
def test_swap_gains_sum_to_the_total(slots):
    """Σ gain == optimal − current, exactly. The list must reconcile.

    Verified against every one of the 114 real rosters across all ten 2026 leagues
    when this was written; here it is pinned on a lineup deliberately set wrong.
    """
    rows = [
        player("QB1", "QB", 22.0), player("QB2", "QB", 8.0, slot="QB"),
        player("RB1", "RB", 18.0, slot="BE"), player("RB2", "RB", 15.0, slot="RB"),
        player("RB3", "RB", 6.0, slot="RB"), player("WR1", "WR", 17.0, slot="WR"),
        player("WR2", "WR", 16.0), player("WR3", "WR", 5.0, slot="WR"),
        player("TE1", "TE", 11.0, slot="TE"), player("TE2", "TE", 10.0, slot="RB/WR/TE"),
        player("K1", "K", 8.0, slot="K"), player("D1", "D/ST", 7.0, slot="D/ST"),
    ]
    current, current_total = lu.current_lineup(rows, "TRUE_Points")
    optimal, optimal_total = lu.optimal_lineup(rows, slots, "TRUE_Points")
    changes = lu.swaps(current, optimal, "TRUE_Points")
    assert sum(c.gain for c in changes) == pytest.approx(optimal_total - current_total)
    assert all(c.gain > 0 for c in changes)


def test_a_swap_prefers_a_position_compatible_partner():
    """Any pairing gives the same total, so the readable one is chosen.

    Without the preference a flex addition gets paired against a benched
    quarterback: "start Rico Dowdle at RB/WR/TE, sit Philip Rivers".
    """
    rows = [
        player("QB1", "QB", 22.0), player("QB2", "QB", 8.0, slot="QB"),
        player("WR1", "WR", 15.0), player("WR2", "WR", 5.0, slot="RB/WR/TE"),
    ]
    current, _ = lu.current_lineup(rows, "TRUE_Points")
    optimal, _ = lu.optimal_lineup(rows, {"QB": 1, "RB/WR/TE": 1}, "TRUE_Points")
    changes = {c.start: c.sit for c in lu.swaps(current, optimal, "TRUE_Points")}
    assert changes == {"QB1": "QB2", "WR1": "WR2"}


# --- add / drop ----------------------------------------------------------

def test_add_drop_is_measured_on_the_lineup_not_on_the_players():
    """A free agent who would not start is worth nothing, however good he looks.

    The whole reason `add_drop_gain` exists: this candidate out-projects the man
    being dropped by 4 points and changes the startable lineup by zero, because a
    third quarterback cannot start in a one-quarterback league.
    """
    roster = [
        player("QB1", "QB", 25.0), player("QB2", "QB", 10.0),
        player("RB1", "RB", 14.0),
    ]
    candidate = player("QB3", "QB", 14.0)
    gain = lu.add_drop_gain(roster, {"QB": 1, "RB": 1}, "TRUE_Points",
                            candidate, drop_id=roster[1]["player_id"])
    assert gain == pytest.approx(0.0)


def test_add_drop_reports_a_real_upgrade():
    roster = [player("RB1", "RB", 5.0), player("RB2", "RB", 4.0)]
    candidate = player("RB3", "RB", 15.0)
    gain = lu.add_drop_gain(roster, {"RB": 1}, "TRUE_Points",
                            candidate, drop_id=roster[1]["player_id"])
    assert gain == pytest.approx(10.0)


def test_drop_candidates_put_non_starters_first():
    roster = [
        player("RB1", "RB", 20.0), player("RB2", "RB", 3.0),
        player("RB3", "RB", 9.0),
    ]
    order = [r["player_name"] for r in
             lu.weakest_starter_candidates(roster, {"RB": 1}, "TRUE_Points")]
    assert order[0] == "RB2"
    assert order[-1] == "RB1"


# --- sources -------------------------------------------------------------

def test_an_absent_source_is_dropped_rather_than_shown_as_agreement():
    """The imputation hazard, which is the reason `real_sources` exists.

    The blend fills a missing source from the ESPN/FantasyPros mean, so an absent
    book arrives looking unanimous rather than silent. On Knights week 1
    `PINNY_Points` and `BOL_Points` are both exactly `MEAN_Points`.
    """
    meta = {"weekly_sources_present": {"fantasypros": True, "pinnacle": False,
                                       "betonline": False}}
    assert lu.real_sources(meta) == ["ESPN", "FP"]


def test_every_source_present_keeps_all_four():
    meta = {"weekly_sources_present": {"fantasypros": True, "pinnacle": True,
                                       "betonline": True}}
    assert lu.real_sources(meta) == ["ESPN", "FP", "PINNY", "BOL"]


def test_no_metadata_assumes_present_rather_than_absent():
    """Silence is not evidence of absence; the coverage panel is."""
    assert lu.real_sources({}) == ["ESPN", "FP", "PINNY", "BOL"]


def test_one_real_source_reports_no_spread():
    """One number cannot disagree with itself."""
    frame = pl.DataFrame([{**player("A", "RB", 10.0), "ESPN_Points": 10.0}])
    out = lu.with_source_spread(frame, {"weekly_sources_present":
                                        {"fantasypros": False, "pinnacle": False,
                                         "betonline": False}})
    assert out["sources_real"][0] == 1
    assert out["source_spread"][0] is None


def test_two_real_sources_report_their_spread():
    frame = pl.DataFrame([{**player("A", "RB", 10.0),
                           "ESPN_Points": 8.0, "FP_Points": 12.0}])
    out = lu.with_source_spread(
        frame, {"weekly_sources_present": {"fantasypros": True, "pinnacle": False,
                                           "betonline": False}})
    assert out["sources_real"][0] == 2
    assert out["source_spread"][0] == pytest.approx(2.0)


def test_a_source_imputed_for_this_player_does_not_count_as_real():
    """League-level presence is not per-player presence.

    FantasyPros publishes weekly numbers for the players it covers and the blend
    imputes the rest -- 178 of 235 rostered players on Knights week 1.
    """
    frame = pl.DataFrame([
        {**player("Covered", "RB", 10.0), "ESPN_Points": 8.0, "FP_Points": 12.0,
         "FP_rushingYards_is_imputed": False},
        {**player("Not", "RB", 10.0), "ESPN_Points": 8.0, "FP_Points": 8.0,
         "FP_rushingYards_is_imputed": True},
    ])
    out = lu.with_source_spread(
        frame, {"weekly_sources_present": {"fantasypros": True, "pinnacle": False,
                                           "betonline": False}})
    assert out["sources_real"].to_list() == [2, 1]
    assert out["source_spread"][1] is None


# --- status --------------------------------------------------------------

def test_bye_and_out_finds_everyone_who_cannot_play():
    rows = [player("A", "RB", 10.0), player("B", "RB", 9.0, status="bye"),
            player("C", "RB", 8.0, status="inactive")]
    assert sorted(r["player_name"] for r in lu.bye_and_out(rows)) == ["B", "C"]


def test_team_sd_is_the_independent_sum():
    starters = [{"weekly_sd": 3.0}, {"weekly_sd": 4.0}]
    assert lu.team_total_sd(starters) == pytest.approx(5.0)


def test_team_sd_is_none_when_nothing_is_priced():
    assert lu.team_total_sd([{"weekly_sd": None}]) is None
