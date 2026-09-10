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


# --- slot ordering -------------------------------------------------------

def test_slots_sort_in_espns_order():
    """QB, RB, WR, TE, FLEX, OP, DP, D/ST, K — then the bench and IR."""
    slots = ["K", "IR", "QB", "BE", "D/ST", "DP", "OP", "RB/WR/TE", "TE", "WR", "RB"]
    assert sorted(slots, key=lu.slot_rank) == [
        "QB", "RB", "WR", "TE", "RB/WR/TE", "OP", "DP", "D/ST", "K", "BE", "IR"]


@pytest.mark.parametrize("flex", ["RB/WR/TE", "RB/WR", "WR/TE"])
def test_every_flex_spelling_sorts_as_flex(flex):
    """Aliased on the slash rather than enumerated, so a new flex needs no edit."""
    assert lu.slot_rank(flex) == lu.slot_rank("RB/WR/TE")


def test_dst_is_not_read_as_a_flex():
    """It carries a slash and is one position. The one exception to the rule above."""
    assert lu.slot_rank("D/ST") > lu.slot_rank("RB/WR/TE")
    assert lu.slot_rank("D/ST") < lu.slot_rank("K")


@pytest.mark.parametrize("idp", ["LB", "CB", "S", "DE", "DT"])
def test_individual_defenders_sort_with_dp(idp):
    """An IDP league reads in the same order as everything else, rather than
    scattering five new slots through it."""
    assert lu.slot_rank(idp) == lu.slot_rank("DP")


def test_an_unrecognised_slot_lands_between_the_kicker_and_the_bench():
    """Visible among the starters rather than hidden after IR — and distinct from
    K, which it collided with before the ranks were scaled."""
    assert lu.slot_rank("K") < lu.slot_rank("NEW") < lu.slot_rank("BE")


def test_position_and_slot_are_different_things():
    """A receiver in the flex sorts under FLEX, not among the receivers.

    The whole point of ordering on `slotPosition` rather than `player_position`.
    """
    frame = pl.DataFrame([
        player("Flexed", "WR", 12.0, slot="RB/WR/TE"),
        player("Started", "WR", 11.0, slot="WR"),
    ])
    out = lu.sort_by_slot(frame, slot_column="slotPosition")
    assert out["player_name"].to_list() == ["Started", "Flexed"]


def test_within_a_slot_the_higher_projection_comes_first():
    frame = pl.DataFrame([
        player("RB2", "RB", 8.0, slot="RB"),
        player("RB1", "RB", 18.0, slot="RB"),
    ])
    out = lu.sort_by_slot(frame, slot_column="slotPosition")
    assert out["player_name"].to_list() == ["RB1", "RB2"]


def test_a_league_missing_slots_keeps_the_order_of_what_it_has():
    """12 Dudes has no D/ST; Jeff's has no kicker. Missing slots just do not appear."""
    frame = pl.DataFrame([
        player("K1", "K", 8.0, slot="K"),
        player("Q1", "QB", 20.0, slot="QB"),
        player("F1", "WR", 10.0, slot="RB/WR/TE"),
    ])
    out = lu.sort_by_slot(frame, slot_column="slotPosition")
    assert out["slotPosition"].to_list() == ["QB", "RB/WR/TE", "K"]


def test_bench_and_ir_come_last_however_good_the_player():
    frame = pl.DataFrame([
        player("Stud", "RB", 30.0, slot="BE"),
        player("Hurt", "RB", 25.0, slot="IR"),
        player("Starter", "RB", 4.0, slot="RB"),
    ])
    out = lu.sort_by_slot(frame, slot_column="slotPosition")
    assert out["player_name"].to_list() == ["Starter", "Stud", "Hurt"]


def test_sorting_a_frame_with_no_slot_column_is_a_no_op():
    frame = pl.DataFrame({"player_name": ["A", "B"]})
    assert lu.sort_by_slot(frame).equals(frame)


def test_the_sort_column_is_not_left_behind():
    frame = pl.DataFrame([player("A", "RB", 10.0, slot="RB")])
    assert "__slot_rank" not in lu.sort_by_slot(frame, slot_column="slotPosition").columns


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


def test_changed_ids_is_the_symmetric_difference():
    """A player who merely moves slot made no decision."""
    rows = [
        player("WR1", "WR", 15.0, slot="RB/WR/TE"),
        player("WR2", "WR", 14.0, slot="WR"),
        player("RB1", "RB", 12.0, slot="RB"),
    ]
    current, _ = lu.current_lineup(rows, "TRUE_Points")
    optimal, _ = lu.optimal_lineup(
        rows, {"WR": 1, "RB": 1, "RB/WR/TE": 1}, "TRUE_Points")
    assert lu.changed_ids(current, optimal) == (set(), set())


def test_changed_ids_names_the_arrival_and_the_departure():
    rows = [player("QB1", "QB", 20.0), player("QB2", "QB", 10.0, slot="QB")]
    current, _ = lu.current_lineup(rows, "TRUE_Points")
    optimal, _ = lu.optimal_lineup(rows, {"QB": 1}, "TRUE_Points")
    added, dropped = lu.changed_ids(current, optimal)
    by_id = {r["player_id"]: r["player_name"] for r in rows}
    assert {by_id[i] for i in added} == {"QB1"}
    assert {by_id[i] for i in dropped} == {"QB2"}


def test_a_swap_carries_the_rows_it_paired():
    """The Roster tab draws the outgoing player under the man taking his place, so
    the pairing has to survive the call rather than being re-derived by name."""
    rows = [player("QB1", "QB", 20.0), player("QB2", "QB", 10.0, slot="QB")]
    current, _ = lu.current_lineup(rows, "TRUE_Points")
    optimal, _ = lu.optimal_lineup(rows, {"QB": 1}, "TRUE_Points")
    change, = lu.swaps(current, optimal, "TRUE_Points")
    assert change.start_row["player_name"] == "QB1"
    assert change.sit_row["player_name"] == "QB2"
    assert change.start_row["player_id"] != change.sit_row["player_id"]


def test_a_dropped_starter_with_no_replacement_is_still_a_change():
    """The case ``swaps`` cannot report and the Roster table has to catch anyway.

    A kicker on bye with nobody to cover him: the optimiser leaves the slot empty,
    so there is no arrival to pair the departure with and ``swaps`` has nothing to
    say. He is the change you most want flagged -- five of the 114 real 2026
    team-weeks are exactly this -- so ``changed_ids`` reports him even though no
    swap mentions him.
    """
    rows = [player("K1", "K", 9.0, slot="K", status="bye")]
    current, _ = lu.current_lineup(rows, "TRUE_Points")
    optimal, _ = lu.optimal_lineup(rows, {"K": 1}, "TRUE_Points")
    added, dropped = lu.changed_ids(current, optimal)

    assert optimal == []
    assert lu.swaps(current, optimal, "TRUE_Points") == []
    assert added == set()
    assert dropped == {rows[0]["player_id"]}


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


# --- who the pool can beat -----------------------------------------------

#: What ESPN says about every player nobody has rostered.
POOL = {"team_owner": "Free Agent", "player_active_status": lu.POOL_STATUS}


def free_agent(name, position, points, **kwargs):
    """A pool row, carrying the status ESPN really gives the whole pool."""
    return {**player(name, position, points, **kwargs), **POOL,
            "ESPN_Points": points}


def test_the_pool_status_is_not_an_availability_signal():
    """ESPN answers "is he in an active lineup slot", and an unrostered player is in
    nobody's -- so the entire pool reads `inactive` in all ten leagues."""
    assert lu.POOL_STATUS != "active"
    assert lu.pool_playable(free_agent("Somebody", "WR", 12.0))


def test_a_pool_player_with_no_game_is_not_playable():
    """ESPN projects exactly zero for a player whose team is not playing. Verified
    against all 81 rostered players it marks as on bye across the ten 2026 leagues."""
    assert not lu.pool_playable(free_agent("Bye Guy", "WR", 0.0))


def test_the_blend_cannot_stand_in_for_espn_on_availability():
    """`TRUE_Points` imputes an absent book from the ESPN/FantasyPros mean, so 24 of
    those 81 bye players carry a non-zero blend of up to 2.12."""
    leaked = dict(free_agent("Bye Guy", "WR", 0.0), TRUE_Points=2.12)
    assert not lu.pool_playable(leaked)


def test_a_pool_row_becomes_startable_for_the_optimiser():
    """The bug this closes: `optimal_lineup` drops a non-active player, so appending
    a raw pool row made `add_drop_gain` return exactly 0.0 for every candidate ever
    passed to it -- on all 114 team-weeks across all ten leagues."""
    candidate = free_agent("Better QB", "QB", 30.0)
    roster = [player("Worse QB", "QB", 5.0, slot="QB")]

    assert lu.optimal_lineup(roster + [candidate], {"QB": 1}, "TRUE_Points")[1] == 5.0
    assert lu.optimal_lineup(
        roster + [lu.as_rostered(candidate)], {"QB": 1}, "TRUE_Points")[1] == 30.0
    assert lu.add_drop_gain(roster, {"QB": 1}, "TRUE_Points", candidate,
                            roster[0]["player_id"]) == pytest.approx(25.0)


def test_players_compete_over_a_shared_slot_not_a_shared_position():
    """A back and a receiver never share a position and compete for the flex."""
    back = player("RB1", "RB", 12.0)
    receiver = player("WR1", "WR", 12.0)
    slots = {"RB": 2, "WR": 2, "RB/WR/TE": 1}
    assert lu.competing_slots(back, slots) & lu.competing_slots(receiver, slots) \
        == {"RB/WR/TE"}


def test_superflex_puts_a_quarterback_against_a_receiver():
    """`OP` is what makes the two superflex leagues here work."""
    passer = player("QB1", "QB", 20.0)
    receiver = player("WR1", "WR", 12.0)
    shared = (lu.competing_slots(passer, SUPERFLEX_SLOTS)
              & lu.competing_slots(receiver, SUPERFLEX_SLOTS))
    assert shared == {"OP"}


def test_a_player_this_league_cannot_start_competes_nowhere():
    defender = player("LB1", "LB", 9.0)
    assert lu.competing_slots(defender, FLEX_SLOTS) == set()


def test_beating_a_starter_is_critical():
    roster = [player("Weak QB", "QB", 10.0, slot="QB")]
    pool = [free_agent("Strong QB", "QB", 20.0)]
    upgrade, = lu.upgrades(pool, roster, {"QB": 1}, "TRUE_Points")
    assert upgrade.severity == lu.UPGRADE_CRITICAL
    assert upgrade.slot == "QB"
    assert upgrade.best["player_name"] == "Strong QB"
    assert upgrade.over["player_name"] == "Weak QB"
    assert upgrade.margin == pytest.approx(10.0)


def test_beating_only_the_bench_is_depth():
    """Nothing about Sunday changes, so it must not read as urgent."""
    roster = [player("Strong QB", "QB", 25.0, slot="QB"),
              player("Weak QB", "QB", 5.0, slot="BE")]
    pool = [free_agent("Middling QB", "QB", 15.0)]
    upgrade, = lu.upgrades(pool, roster, {"QB": 1}, "TRUE_Points")
    assert upgrade.severity == lu.UPGRADE_DEPTH
    assert upgrade.over["player_name"] == "Weak QB"


def test_the_two_severities_are_mutually_exclusive():
    """Beating a starter suppresses the depth row for the same slot: the user asked
    for two flags, not the same fact twice."""
    roster = [player("Weak QB", "QB", 10.0, slot="QB"),
              player("Weaker QB", "QB", 2.0, slot="BE")]
    pool = [free_agent("Strong QB", "QB", 20.0)]
    upgrade, = lu.upgrades(pool, roster, {"QB": 1}, "TRUE_Points")
    assert upgrade.severity == lu.UPGRADE_CRITICAL
    assert upgrade.beaten == 1  # the starter; the bench row is not a second alert


def test_an_edge_inside_the_sources_own_disagreement_is_not_reported():
    """The sources disagree by a median of 0.43 points, so a quarter-point edge is
    noise wearing a decision's clothes. Two real `+0.0` alerts came from this."""
    roster = [player("Yours", "QB", 12.0, slot="QB")]
    pool = [free_agent("Theirs", "QB", 12.25)]
    assert lu.upgrades(pool, roster, {"QB": 1}, "TRUE_Points") == []
    assert lu.upgrades(pool, roster, {"QB": 1}, "TRUE_Points", min_margin=0.1)


def test_a_free_agent_with_no_game_is_never_an_upgrade():
    roster = [player("Yours", "QB", 10.0, slot="QB")]
    pool = [free_agent("On Bye", "QB", 0.0)]
    pool[0]["TRUE_Points"] = 25.0          # the blend leaked; ESPN did not
    assert lu.upgrades(pool, roster, {"QB": 1}, "TRUE_Points") == []


def test_one_candidate_is_not_reported_at_every_slot_he_fits():
    """A superflex quarterback out-projects every receiver, back and end eligible for
    `OP`. Keyed by the rostered player that was twelve rows about one add."""
    roster = [player("WR1", "WR", 8.0, slot="WR"),
              player("RB1", "RB", 7.0, slot="RB"),
              player("QB1", "QB", 15.0, slot="QB")]
    pool = [free_agent("Star QB", "QB", 22.0)]
    found = lu.upgrades(pool, roster, SUPERFLEX_SLOTS, "TRUE_Points")
    assert len(found) == 1
    assert found[0].slot == "OP"           # where the gap is widest


def test_the_weakest_man_beaten_is_the_one_named():
    """He is the one you would actually replace, so the margin is the whole gap."""
    roster = [player("Good", "WR", 14.0, slot="WR"),
              player("Bad", "WR", 4.0, slot="WR")]
    pool = [free_agent("Best", "WR", 18.0)]
    upgrade, = lu.upgrades(pool, roster, {"WR": 2}, "TRUE_Points")
    assert upgrade.over["player_name"] == "Bad"
    assert upgrade.margin == pytest.approx(14.0)
    assert upgrade.beaten == 2


def test_a_clean_roster_gets_no_flags():
    roster = [player("Elite", "QB", 30.0, slot="QB")]
    pool = [free_agent("Nobody", "QB", 4.0)]
    assert lu.upgrades(pool, roster, {"QB": 1}, "TRUE_Points") == []


def test_criticals_come_before_depth_and_sort_by_margin():
    roster = [player("QB1", "QB", 10.0, slot="QB"),
              player("TE1", "TE", 9.0, slot="TE"),
              player("WR1", "WR", 2.0, slot="BE")]
    pool = [free_agent("QB2", "QB", 14.0), free_agent("TE2", "TE", 18.0),
            free_agent("WR2", "WR", 12.0)]
    found = lu.upgrades(pool, roster, {"QB": 1, "TE": 1, "WR": 1}, "TRUE_Points")
    assert [u.severity for u in found] == [lu.UPGRADE_CRITICAL, lu.UPGRADE_CRITICAL,
                                           lu.UPGRADE_DEPTH]
    assert [u.best["player_name"] for u in found] == ["TE2", "QB2", "WR2"]


def test_the_candidate_set_covers_every_slot_not_the_biggest_numbers():
    """The blind spot this closes: the positions are not on the same scale, so a
    top-N of the pool was six quarterbacks in a one-quarterback league."""
    pool = [free_agent(f"QB{i}", "QB", 20.0 - i) for i in range(6)]
    pool += [free_agent("The Kicker", "K", 9.0), free_agent("The Back", "RB", 13.0)]
    picked = lu.best_available_per_slot(
        pool, {"QB": 1, "RB": 2, "K": 1}, "TRUE_Points")
    assert {row["player_name"] for row in picked} == {"QB0", "The Back", "The Kicker"}


def test_one_player_winning_two_slots_is_listed_once():
    """The best available back is usually the best available flex too."""
    pool = [free_agent("The Back", "RB", 13.0)]
    picked = lu.best_available_per_slot(pool, FLEX_SLOTS, "TRUE_Points")
    assert len(picked) == 1


def test_a_candidate_with_no_game_is_never_offered():
    pool = [free_agent("On Bye", "QB", 0.0), free_agent("Playing", "QB", 8.0)]
    picked = lu.best_available_per_slot(pool, {"QB": 1}, "TRUE_Points")
    assert [row["player_name"] for row in picked] == ["Playing"]


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
    # `theathletic` is not in the metadata at all, and `real_sources` reads a missing
    # key as present -- see `test_no_metadata_assumes_present_rather_than_absent` for
    # why that default is deliberate.
    assert lu.real_sources(meta) == ["ESPN", "FP", "ATH"]


def test_every_source_present_keeps_all_of_them():
    meta = {"weekly_sources_present": {"fantasypros": True, "pinnacle": True,
                                       "betonline": True, "theathletic": True}}
    assert lu.real_sources(meta) == ["ESPN", "FP", "PINNY", "BOL", "ATH"]


def test_no_metadata_assumes_present_rather_than_absent():
    """Silence is not evidence of absence; the coverage panel is."""
    assert lu.real_sources({}) == ["ESPN", "FP", "PINNY", "BOL", "ATH"]


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
    imputes the rest. The flag has to travel with the stat it describes: this
    fixture used to carry ``FP_rushingYards_is_imputed`` and no
    ``FP_rushingYards``, which is not a shape any real frame has, and it is why
    the old mean-of-flags rule looked like it worked here.
    """
    frame = pl.DataFrame([
        {**player("Covered", "RB", 10.0), "ESPN_Points": 8.0, "FP_Points": 12.0,
         "FP_rushingYards": 60.0, "FP_rushingYards_is_imputed": False},
        {**player("Not", "RB", 10.0), "ESPN_Points": 8.0, "FP_Points": 8.0,
         "FP_rushingYards": 45.0, "FP_rushingYards_is_imputed": True},
    ])
    out = lu.with_source_spread(
        frame, {"weekly_sources_present": {"fantasypros": True, "pinnacle": False,
                                           "betonline": False}})
    assert out["sources_real"].to_list() == [2, 1]
    assert out["source_spread"][1] is None


def test_a_structural_zero_is_not_a_real_line():
    """A source that projected nothing is not a source that projected zero.

    These frames are dense with structural zeros: a kicker's ``FP_passingYards``
    is 0.0 and carries no imputation flag, because nobody imputed it and nobody
    asserted it either. Counting those made FantasyPros a real source for Cameron
    Dicker on the strength of twelve zeros, and his floor and ceiling came back
    exactly equal to ESPN's total -- a spread of zero, reported as agreement.
    """
    frame = pl.DataFrame([{**player("Dicker", "K", 8.0), "ESPN_Points": 8.0,
                           "FP_Points": 8.0, "FP_passingYards": 0.0,
                           "FP_rushingYards": 0.0}])
    out = lu.with_source_spread(
        frame, {"weekly_sources_present": {"fantasypros": True, "pinnacle": False,
                                           "betonline": False}})
    assert out["sources_real"][0] == 1
    assert out["source_spread"][0] is None


def test_a_source_with_more_flags_than_it_ever_fills_still_counts_as_real():
    """The regression test for a cut that never fired.

    ``with_source_spread`` used to count a source real when the *mean* of its
    imputation flags was below 0.5. Measured on Knights 2026 week 1, FantasyPros
    carries **47** flag columns and fills at most **15** of them for any player --
    the other 32 are kicker bands, D/ST bands, two-point conversions and targets it
    structurally never publishes -- so the minimum imputed share over 334 rows was
    **0.681** and no row cleared the cut. ``sources_real`` was uniformly 1 and
    ``source_spread`` null for every player, all season, which is why the Roster
    tab's "Single-Source Starters" counted every starter.

    Two real cells out of twelve is a real line. This fixture is that shape.
    """
    row = {**player("A", "WR", 10.0), "ESPN_Points": 8.0, "FP_Points": 12.0}
    for i in range(12):
        row[f"FP_stat{i}"] = 10.0
        row[f"FP_stat{i}_is_imputed"] = i >= 2
    out = lu.with_source_spread(
        pl.DataFrame([row]),
        {"weekly_sources_present": {"fantasypros": True, "pinnacle": False,
                                    "betonline": False}})
    assert out["sources_real"][0] == 2
    assert out["source_spread"][0] == pytest.approx(2.0)


def test_the_store_and_the_app_agree_on_which_sources_are_real():
    """One rule, two implementations, pinned against each other.

    ``Scripts.projection_utils.source_contributed`` is pandas and
    ``app.lineup._contributed`` is polars, so they cannot be the same call. This
    test is what buys that duplication: the sidebar's coverage percentage and this
    frame's ``sources_real`` have to mean the same thing, and the failure mode if
    they drift is the one this repo keeps hitting -- two numbers describing the
    same data and disagreeing, with nothing to say which is right.
    """
    from Scripts.projection_utils import source_contributed

    rows = [
        {"FP_rushingYards": 60.0, "FP_rushingYards_is_imputed": False,
         "FP_receivingYards": 0.0, "FP_receivingYards_is_imputed": False},
        {"FP_rushingYards": 45.0, "FP_rushingYards_is_imputed": True,
         "FP_receivingYards": 0.0, "FP_receivingYards_is_imputed": False},
        {"FP_rushingYards": 0.0, "FP_rushingYards_is_imputed": False,
         "FP_receivingYards": 30.0, "FP_receivingYards_is_imputed": False},
    ]
    frame = pl.DataFrame(rows)
    polars_answer = frame.select(lu._contributed(frame, "FP").alias("x"))["x"].to_list()
    pandas_answer = source_contributed(
        frame.to_pandas(), "FP", ["rushingYards", "receivingYards"]).tolist()

    assert polars_answer == pandas_answer == [True, False, True]


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


# --- kickoff, and what it takes off the table ----------------------------
#
# The optimiser's suggestion stopped being hypothetical the moment live scoring
# landed: a swap involving a player whose game has started is not a decision, it is
# an illegal move. What these pin is that the *achievable* optimum and the
# *hindsight* optimum are two different questions, and that a frame carrying no game
# state still answers the first one exactly the way it always did.

STARTERS = {"QB": 1, "RB": 2, "WR": 2, "TE": 1}


def locked(row, state="post"):
    """The same row, with its game already under way or over."""
    return {**row, lu.STATE_COLUMN: state, lu.LOCKED_COLUMN: True}


def unlocked(row):
    """The same row, explicitly before kickoff."""
    return {**row, lu.STATE_COLUMN: "pre", lu.LOCKED_COLUMN: False}


def test_a_frame_with_no_game_state_locks_nobody():
    """The compatibility guarantee. Every store written before live scoring must
    optimise exactly as it did, or shipping this would silently rewrite history."""
    rows = [player("Starter", "WR", 4.0, slot="WR"),
            player("Bench", "WR", 20.0),
            player("Filler", "WR", 3.0, slot="WR"),
            player("QB", "QB", 15.0, slot="QB"),
            player("RB1", "RB", 12.0, slot="RB"),
            player("RB2", "RB", 11.0, slot="RB"),
            player("TE", "TE", 8.0, slot="TE")]
    starters, total = lu.optimal_lineup(rows, STARTERS, "TRUE_Points")
    assert {r["player_name"] for r in starters if r["slot"] == "WR"} == {
        "Bench", "Starter"}
    assert total == pytest.approx(20.0 + 4.0 + 15.0 + 12.0 + 11.0 + 8.0)


def test_a_locked_starter_keeps_his_slot_however_bad_he_was():
    """He played and scored 1.2. Nothing can be done about it, and an optimiser that
    swaps him out is describing a lineup you are not allowed to set."""
    rows = [locked(player("Played Badly", "WR", 1.2, slot="WR")),
            unlocked(player("Bench", "WR", 20.0)),
            unlocked(player("Other", "WR", 18.0, slot="WR")),
            unlocked(player("QB", "QB", 15.0, slot="QB")),
            unlocked(player("RB1", "RB", 12.0, slot="RB")),
            unlocked(player("RB2", "RB", 11.0, slot="RB")),
            unlocked(player("TE", "TE", 8.0, slot="TE"))]
    starters, _ = lu.optimal_lineup(rows, STARTERS, "TRUE_Points")
    names = {r["player_name"] for r in starters}
    assert "Played Badly" in names
    # Only one WR slot was left, so the better of the two movable receivers takes it.
    assert "Bench" in names and "Other" not in names


def test_a_locked_bench_player_cannot_be_promoted():
    """His game is over. Whatever he scored, he scored it on your bench."""
    rows = [unlocked(player("Starter", "WR", 4.0, slot="WR")),
            locked(player("Big Game On The Bench", "WR", 31.0)),
            unlocked(player("Available", "WR", 9.0)),
            unlocked(player("QB", "QB", 15.0, slot="QB"))]
    starters, _ = lu.optimal_lineup(rows, {"QB": 1, "WR": 2}, "TRUE_Points")
    names = {r["player_name"] for r in starters}
    assert "Big Game On The Bench" not in names
    assert names == {"Starter", "Available", "QB"}


def test_a_locked_starter_is_seated_even_when_ruled_out():
    """`player_active_status` excludes him from the candidate pool, but he is not a
    candidate -- he is a fact. Dropping him would hand his slot to someone who
    cannot legally have it."""
    rows = [locked(player("Ruled Out", "WR", 0.0, slot="WR", status="inactive")),
            unlocked(player("Bench", "WR", 14.0))]
    starters, total = lu.optimal_lineup(rows, {"WR": 1}, "TRUE_Points")
    assert [r["player_name"] for r in starters] == ["Ruled Out"]
    assert total == pytest.approx(0.0)


def test_swaps_only_offers_moves_you_can_make():
    rows = [locked(player("Locked Dud", "WR", 1.0, slot="WR")),
            unlocked(player("Bench Star", "WR", 22.0)),
            unlocked(player("Weak Starter", "WR", 5.0, slot="WR"))]
    current, _ = lu.current_lineup(rows, "TRUE_Points")
    optimal, _ = lu.optimal_lineup(rows, {"WR": 2}, "TRUE_Points")
    changes = lu.swaps(current, optimal, "TRUE_Points")
    assert [(c.start_row["player_name"], c.sit_row["player_name"])
            for c in changes] == [("Bench Star", "Weak Starter")]


# --- hindsight, which is the other question ------------------------------

def test_hindsight_ignores_kickoff():
    rows = [locked(player("Locked Dud", "WR", 1.0, slot="WR")),
            locked(player("Bench Star", "WR", 22.0))]
    starters, total = lu.hindsight_lineup(rows, {"WR": 1}, "TRUE_Points")
    assert [r["player_name"] for r in starters] == ["Bench Star"]
    assert total == pytest.approx(22.0)


def test_points_left_on_bench_is_what_the_week_cost():
    rows = [locked(player("Started", "WR", 6.0, slot="WR")),
            locked(player("Should Have Started", "WR", 24.0))]
    assert lu.points_left_on_bench(rows, {"WR": 1}, "TRUE_Points") == pytest.approx(18.0)


def test_a_perfect_lineup_left_nothing_on_the_bench():
    """Never negative: hindsight maximises over a superset of what was started, so a
    negative number here would be a modelling artefact rather than a finding."""
    rows = [locked(player("Best", "WR", 24.0, slot="WR")),
            locked(player("Worse", "WR", 6.0))]
    assert lu.points_left_on_bench(rows, {"WR": 1}, "TRUE_Points") == 0.0


# --- the free-agent pool -------------------------------------------------

def test_a_free_agent_whose_game_has_finished_is_not_an_upgrade():
    """The question `ESPN_Points > 0` could never answer. He may have scored 30
    points; you cannot start him at 4:30 for a game that ended at 4:20."""
    finished = locked(player("Already Played", "WR", 12.0))
    finished["ESPN_Points"] = 12.0
    assert not lu.pool_playable(finished)


def test_a_free_agent_before_kickoff_is_playable():
    upcoming = unlocked(player("Sunday Night", "WR", 12.0))
    upcoming["ESPN_Points"] = 12.0
    assert lu.pool_playable(upcoming)


def test_a_free_agent_on_bye_is_not_playable():
    bye = locked(player("On Bye", "WR", 2.12), state="bye")
    bye["ESPN_Points"] = 0.0
    assert not lu.pool_playable(bye)


def test_the_espn_points_proxy_still_answers_for_an_older_store():
    """No `game_state` on the row, so the measured proxy is used -- which is what a
    2025 store carries."""
    has_game = player("Playing", "WR", 12.0)
    has_game["ESPN_Points"] = 12.0
    no_game = player("Bye", "WR", 2.12)
    no_game["ESPN_Points"] = 0.0
    assert lu.pool_playable(has_game)
    assert not lu.pool_playable(no_game)
