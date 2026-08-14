"""The draft board page's derivations: filters, roster needs, chart data.

The page script itself is layout and needs a Streamlit runtime; everything with a
decision in it lives in ``app/draft_view.py`` and is covered here. No network, no
store on disk -- the boards are synthesised to the shape ``build_board`` returns.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

# `app/` is not a package: Streamlit runs the page scripts directly with app/ on
# sys.path, so the modules import each other by bare name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import draft_view as dv  # noqa: E402


# --- fixtures ------------------------------------------------------------

def _board(rows):
    """A board frame with the columns the page reads, defaults filled in."""
    defaults = {
        "player_name": "Player", "primaryPosition": "RB", "pro_team": "DET",
        "on_team_id": 0, "team_owner": "Free Agent", "startable": True,
        "projection_missing": False, "TRUE_Points": 100.0, "pos_rank": 1.0,
        "replacement_rank": 30, "tier": 1.0, "vor": 10.0, "value": 5.0,
        "adp": 20.0, "bye_week": 6.0, "floor": 90.0, "ceiling": 110.0,
        "injury_status": "ACTIVE", "auction_value_filled": 20.0, "vor_rank": 1.0,
    }
    return pl.DataFrame([{**defaults, **row} for row in rows])


STANDARD = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1}


# --- roster needs --------------------------------------------------------

def test_an_empty_roster_needs_every_starting_slot():
    assert dv.roster_needs(STANDARD, []) == STANDARD


def test_a_dedicated_slot_is_filled_before_the_flex():
    """A second running back is a starter, not the flex."""
    needs = dv.roster_needs(STANDARD, ["RB", "RB"])
    assert "RB" not in needs
    assert needs["RB/WR/TE"] == 1


def test_the_third_back_lands_in_the_flex():
    needs = dv.roster_needs(STANDARD, ["RB", "RB", "RB"])
    assert "RB" not in needs and "RB/WR/TE" not in needs


def test_a_full_starting_lineup_needs_nothing():
    roster = ["QB", "RB", "RB", "WR", "WR", "TE", "WR", "K", "D/ST"]
    assert dv.roster_needs(STANDARD, roster) == {}
    assert dv.positions_needed(STANDARD, roster) == []


def test_bench_slots_are_never_a_need():
    """`BE` and `IR` hold a player without starting one."""
    needs = dv.roster_needs({**STANDARD, "BE": 6, "IR": 1}, [])
    assert "BE" not in needs and "IR" not in needs


def test_the_superflex_makes_a_quarterback_a_need_twice_over():
    """The OP slot accepts a QB, which is why that league values them differently."""
    superflex = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "OP": 1}
    assert "QB" in dv.positions_needed(superflex, ["QB"])
    assert "QB" not in dv.positions_needed(STANDARD, ["QB"])


def test_positions_needed_expands_a_flex_slot():
    needed = dv.positions_needed({"RB/WR/TE": 1}, [])
    assert set(needed) == {"RB", "WR", "TE"}


# --- filters -------------------------------------------------------------

def test_unprojected_players_are_hidden_by_default():
    """Their points are a literal 0.0, so unfiltered they rank as the worst players
    in the league rather than as unknowns."""
    board = _board([
        {"player_name": "Real", "TRUE_Points": 200.0, "projection_missing": False},
        {"player_name": "None", "TRUE_Points": 0.0, "projection_missing": True},
    ])
    assert dv.filter_board(board, None)["player_name"].to_list() == ["Real"]
    assert dv.filter_board(board, None, hide_unprojected=False).height == 2


def test_unstartable_positions_are_hidden_by_default():
    """The 32 team defences on the board of the league with no D/ST slot."""
    board = _board([
        {"player_name": "Back", "startable": True},
        {"player_name": "Defence", "primaryPosition": "D/ST", "startable": False},
    ])
    assert dv.filter_board(board, None)["player_name"].to_list() == ["Back"]


def test_drafted_players_are_dropped_from_the_available_pool():
    board = _board([
        {"player_name": "Free", "on_team_id": 0},
        {"player_name": "Taken", "on_team_id": 4, "team_owner": "Someone"},
    ])
    assert dv.available_only(board)["player_name"].to_list() == ["Free"]
    assert dv.filter_board(board, None, only_available=False).height == 2


def test_position_and_search_filters_compose():
    board = _board([
        {"player_name": "Bijan Robinson", "primaryPosition": "RB"},
        {"player_name": "Brian Robinson", "primaryPosition": "RB"},
        {"player_name": "Puka Nacua", "primaryPosition": "WR"},
    ])
    out = dv.filter_board(board, ["RB"], search="bijan")
    assert out["player_name"].to_list() == ["Bijan Robinson"]


def test_search_is_case_insensitive():
    board = _board([{"player_name": "Ja'Marr Chase"}])
    assert dv.filter_board(board, None, search="CHASE").height == 1


# --- the scarcity curve --------------------------------------------------

def test_the_curve_stops_a_fixed_multiple_past_replacement():
    """Past that, every position is flat near zero and the cliff is squeezed into
    the left edge -- which is the one thing the chart exists to show."""
    board = _board([
        {"player_name": f"RB{i}", "pos_rank": float(i), "replacement_rank": 10,
         "TRUE_Points": 300.0 - i}
        for i in range(1, 40)
    ])
    curve = dv.scarcity_curve(board, ["RB"], depth=1.6)
    assert curve["pos_rank"].max() == 16.0


def test_a_position_with_no_replacement_level_keeps_its_whole_curve():
    board = _board([
        {"player_name": "K1", "primaryPosition": "K", "pos_rank": 1.0,
         "replacement_rank": None},
        {"player_name": "K2", "primaryPosition": "K", "pos_rank": 40.0,
         "replacement_rank": None},
    ])
    assert dv.scarcity_curve(board, ["K"]).height == 2


def test_the_curve_excludes_unprojected_players():
    board = _board([
        {"player_name": "Real", "pos_rank": 1.0},
        {"player_name": "None", "pos_rank": 2.0, "projection_missing": True},
    ])
    assert dv.scarcity_curve(board, ["RB"])["player_name"].to_list() == ["Real"]


# --- tier runway ---------------------------------------------------------

def test_runway_counts_available_players_per_tier():
    board = _board([
        {"player_name": "A", "tier": 1.0, "TRUE_Points": 300.0},
        {"player_name": "B", "tier": 1.0, "TRUE_Points": 290.0},
        {"player_name": "C", "tier": 2.0, "TRUE_Points": 200.0},
        {"player_name": "D", "tier": 2.0, "TRUE_Points": 190.0, "on_team_id": 3},
    ])
    runway = dv.tier_runway(board, ["RB"]).sort("tier")
    assert runway["remaining"].to_list() == [2, 1]
    assert runway["best_points"].to_list() == [300.0, 200.0]


def test_runway_is_empty_rather_than_raising_on_an_empty_pool():
    board = _board([{"player_name": "A", "projection_missing": True}])
    assert dv.tier_runway(board, ["RB"]).is_empty()


# --- value targets -------------------------------------------------------

def test_value_targets_exclude_players_the_market_has_not_priced():
    """A best-values list whose rows are mostly nulls is worthless."""
    board = _board([
        {"player_name": "Falling", "value": 30.0},
        {"player_name": "Unpriced", "value": None},
    ])
    assert dv.value_targets(board)["player_name"].to_list() == ["Falling"]


def test_value_targets_are_sorted_by_how_far_they_fall():
    board = _board([
        {"player_name": "Small", "value": 5.0},
        {"player_name": "Big", "value": 40.0},
    ])
    assert dv.value_targets(board)["player_name"].to_list() == ["Big", "Small"]


# --- display -------------------------------------------------------------

def test_display_frame_renames_and_orders_columns():
    frame = dv.display_frame(_board([{}]))
    assert frame.columns[:4] == ["Player", "Pos", "NFL", "Bye"]


def test_display_frame_skips_columns_the_artifact_lacks():
    board = _board([{}]).drop(["floor", "ceiling"])
    frame = dv.display_frame(board)
    assert "Floor" not in frame.columns and "Player" in frame.columns


# --- the model's own account of itself -----------------------------------
#
# Five states, and the whole point of the column is that three of them would
# otherwise render as the same empty cell.

def _modelled(rows):
    """A board carrying the usage model's columns."""
    defaults = {"usg_arm": "veteran", "USG_Points": 90.0, "usg_evidence": "",
                "usg_expected_games": 15.0, "USG_PosRankDelta": 0.0}
    return _board([{**defaults, **row} for row in rows])


def _labels(board):
    return dv.with_model_evidence(board)["usg_evidence_label"].to_list()


def test_a_priced_player_with_nothing_flagged_reads_as_clear():
    assert _labels(_modelled([{}])) == [dv.EVIDENCE_CLEAR]


def test_a_flagged_player_shows_the_models_reason_verbatim():
    board = _modelled([{"usg_evidence": "changed teams"}])
    assert _labels(board) == ["changed teams"]


def test_a_position_the_model_never_covers_is_not_confused_with_agreement():
    board = _modelled([{"usg_arm": None, "USG_Points": None,
                        "usg_evidence": None, "usg_expected_games": None}])
    assert _labels(board) == [dv.EVIDENCE_NOT_MODELLED]


def test_an_abstention_reads_as_withdrawn_on_availability():
    board = _modelled([{"usg_arm": "abstain", "USG_Points": None,
                        "usg_expected_games": 4.9}])
    assert _labels(board) == [dv.EVIDENCE_WITHDRAWN_AVAILABILITY]


def test_an_injury_withdrawal_is_told_apart_from_an_abstention():
    """Same empty USG, different reason: the arm ran and the report pulled it."""
    board = _modelled([{"usg_arm": "veteran", "USG_Points": None,
                        "usg_expected_games": 11.3}])
    assert _labels(board) == [dv.EVIDENCE_WITHDRAWN_INJURY]


def test_a_withdrawal_outranks_the_evidence_text_it_also_carries():
    """The 7 real rows that are flagged *and* unpriced: absence is the headline."""
    board = _modelled([{"usg_arm": "veteran", "USG_Points": None,
                        "usg_evidence": "thin prior season; changed teams"}])
    assert _labels(board) == [dv.EVIDENCE_WITHDRAWN_INJURY]


def test_a_board_predating_the_model_is_returned_untouched():
    board = _board([{}])
    assert dv.with_model_evidence(board).columns == board.columns


def test_the_model_block_is_dropped_from_a_board_that_predates_it():
    frame = dv.display_frame(dv.with_model_evidence(_board([{}])))
    for label in ("USG", "Δrk", "Exp G", "Model evidence"):
        assert label not in frame.columns


def test_the_model_block_sits_between_the_market_and_the_status_columns():
    frame = dv.display_frame(dv.with_model_evidence(_modelled([{}])))
    order = frame.columns
    assert order.index("$") < order.index("USG")
    assert order.index("Model evidence") < order.index("Injury")


# --- where the points came from ------------------------------------------
#
# The join is on the *franchise*, not the manager, and these tests are mostly
# about why. See dv._franchise_key: ESPN spells a person four different ways
# across its two endpoints, and each way costs a manager their entire draft.

def _weeks(rows):
    """A lineups frame with the columns drafted_versus_added reads."""
    defaults = {"team_name": "Team Tommy", "team_owner": "Tommy Winfield",
                "player_id": 1, "slotPosition": "RB", "points": 10.0}
    return pl.DataFrame([{**defaults, **row} for row in rows])


def _picks(rows, season=2025):
    defaults = {"team_name": "Team Tommy", "player_id": 1, "season": season,
                "owner_id": "{GUID-TOMMY}"}
    return pl.DataFrame([{**defaults, **row} for row in rows])


#: A draft that exists but contains nobody these managers fielded. Not the same
#: thing as an empty picks frame, which means the draft is *unknown* — see
#: test_a_season_with_no_draft_data_refuses_to_answer.
NOBODY_RELEVANT = [{"team_name": "Team Tommy", "player_id": 9999}]


def _averaged_row(manager, season, drafted, added, owner_id="{GUID-JACK}",
                  moves=1, points_per_move=None):
    """One row of what `drafted_versus_added` returns, for averaging tests."""
    return pl.DataFrame([{
        "owner": manager, "manager": manager, "owner_id": owner_id,
        "drafted": drafted, "added": added, "total": drafted + added,
        "share_drafted": 100 * drafted / (drafted + added) if drafted + added else None,
        "moves": moves,
        "points_per_move": points_per_move if points_per_move is not None
        else (added / moves if moves else None),
        "season": season,
    }])


def test_points_split_between_the_draft_and_the_wire():
    weeks = _weeks([{"player_id": 1, "points": 30.0},
                    {"player_id": 2, "points": 12.0}])
    out = dv.drafted_versus_added(weeks, _picks([{"player_id": 1}]))
    assert out["drafted"].to_list() == [30.0]
    assert out["added"].to_list() == [12.0]
    assert out["share_drafted"].to_list() == [pytest.approx(71.428, rel=1e-3)]


@pytest.mark.parametrize("in_lineups,in_draft", [
    ("Hank Winfield", "hank Winfield"),      # set_owner_names applies str.title
    ("Zach Imel", "Zachary Imel"),           # a nickname
    ("Logan Tola", "Matt Logan Tola"),       # an extra given name
    ("Alex Holton", "Michael Beal"),         # the team changed hands
])
def test_the_draft_is_found_however_espn_spells_the_manager(in_lineups, in_draft):
    """All four pairs are real, in 2025, one league each. Joining on the manager
    loses the whole team and reports it as having drafted nobody — 2,592.95 points
    for Zach Imel. Joining on the franchise is immune to all four, including the
    last, which no string rule could fix."""
    weeks = _weeks([{"team_owner": in_lineups, "points": 40.0}])
    picks = _picks([{"player_id": 1}]).with_columns(
        pl.lit(in_draft).alias("owner"))
    out = dv.drafted_versus_added(weeks, picks)
    assert out["drafted"].to_list() == [40.0]
    assert out["added"].to_list() == [0.0]


def test_a_player_is_drafted_for_the_team_that_took_him_not_the_one_that_added_him():
    """One player, two rosters, one draft pick: the same points are not 'drafted'
    for both."""
    weeks = _weeks([{"team_name": "Team Tommy", "team_owner": "Tommy Winfield",
                     "points": 20.0},
                    {"team_name": "Team Jack", "team_owner": "Jack Winfield",
                     "points": 15.0}])
    picks = pl.concat([_picks([{"team_name": "Team Tommy"}]),
                       _picks([{"team_name": "Team Jack", "player_id": 777}])])
    out = dv.drafted_versus_added(weeks, picks)
    by_owner = dict(zip(out["owner"], out["drafted"]))
    assert by_owner["Tommy Winfield"] == 20.0
    assert by_owner["Jack Winfield"] == 0.0


def test_a_team_absent_from_the_draft_is_unknown_rather_than_zero():
    """Zero would claim they drafted nobody who scored. Null says the draft
    cannot be matched to them, which is the truth and the distinction that has
    already cost this repo three separate bugs."""
    weeks = _weeks([{"team_name": "Expansion Team", "team_owner": "New Guy",
                     "points": 55.0}])
    out = dv.drafted_versus_added(weeks, _picks([{"team_name": "Team Tommy"}]))
    assert out["drafted"].to_list() == [None]
    assert out["added"].to_list() == [None]
    assert out["share_drafted"].to_list() == [None]
    assert out["total"].to_list() == [55.0]


def test_bench_points_are_excluded_because_they_never_counted():
    weeks = _weeks([{"slotPosition": "RB", "points": 20.0},
                    {"slotPosition": "BE", "points": 50.0},
                    {"slotPosition": "IR", "points": 5.0}])
    picks = _picks([{"player_id": 1}])
    assert dv.drafted_versus_added(weeks, picks)["total"].to_list() == [20.0]
    assert dv.drafted_versus_added(
        weeks, picks, starters_only=False)["total"].to_list() == [75.0]


def test_free_agents_are_not_a_manager():
    """`team_owner` is 'Free Agent' for anyone nobody rostered that week."""
    weeks = _weeks([{"team_owner": "Free Agent", "team_name": "Free Agent",
                     "points": 99.0},
                    {"team_owner": "Tommy Winfield", "points": 10.0}])
    out = dv.drafted_versus_added(weeks, _picks(NOBODY_RELEVANT))
    assert out["owner"].to_list() == ["Tommy Winfield"]


def test_teams_are_ordered_by_what_they_scored():
    weeks = _weeks([{"team_name": "Small", "team_owner": "Small", "points": 10.0},
                    {"team_name": "Big", "team_owner": "Big", "points": 90.0}])
    picks = pl.concat([_picks([{"team_name": "Small", "player_id": 9999}]),
                       _picks([{"team_name": "Big", "player_id": 9999}])])
    assert dv.drafted_versus_added(weeks, picks)["owner"].to_list() == ["Big",
                                                                       "Small"]


def test_a_season_with_no_draft_data_refuses_to_answer():
    """Deliberate, and the opposite of what is convenient. With no picks every
    point would classify as 'added', which is a coherent-looking claim that the
    whole league built its season off waivers — this repo's recurring failure of
    an absent source reading as a real answer. Empty is the honest output."""
    assert dv.drafted_versus_added(_weeks([{"points": 25.0}]),
                                   pl.DataFrame()).is_empty()
    assert dv.drafted_versus_added(pl.DataFrame(), pl.DataFrame()).is_empty()


def test_a_lineups_frame_missing_the_columns_is_empty_rather_than_raising():
    """Stores written before this reach the same path."""
    thin = _weeks([{}]).drop("slotPosition")
    assert dv.drafted_versus_added(thin, _picks([{}])).is_empty()


def test_a_team_that_only_fielded_its_bench_does_not_divide_by_zero():
    weeks = _weeks([{"slotPosition": "BE", "points": 30.0}])
    assert dv.drafted_versus_added(weeks, _picks(NOBODY_RELEVANT)).is_empty()


# --- moves, and what a move is -------------------------------------------

def test_a_move_counts_a_pickup_even_if_he_never_started():
    """The denominator of points-per-move is players brought in, not players who
    worked out. Counting only the ones who started would flatter a manager who
    claimed twenty and got three right, which is what the ratio exists to expose."""
    weeks = _weeks([{"player_id": 1, "slotPosition": "RB", "points": 20.0},
                    {"player_id": 2, "slotPosition": "RB", "points": 30.0},
                    {"player_id": 3, "slotPosition": "BE", "points": 99.0}])
    out = dv.drafted_versus_added(weeks, _picks([{"player_id": 1}]))
    assert out["moves"].to_list() == [2]          # players 2 and 3
    assert out["added"].to_list() == [30.0]       # only player 2 started
    assert out["points_per_move"].to_list() == [15.0]


def test_the_same_player_added_twice_is_one_move():
    """A floor on real transactions, and said to be one on the page."""
    weeks = _weeks([{"player_id": 2, "week": 3, "points": 10.0},
                    {"player_id": 2, "week": 9, "points": 12.0}])
    out = dv.drafted_versus_added(weeks, _picks([{"player_id": 1}]))
    assert out["moves"].to_list() == [1]
    assert out["added"].to_list() == [22.0]


def test_a_manager_who_never_touched_the_wire_has_no_points_per_move():
    """Null, not zero — 0.0 would rank them with someone whose every pickup
    failed."""
    weeks = _weeks([{"player_id": 1, "points": 40.0}])
    out = dv.drafted_versus_added(weeks, _picks([{"player_id": 1}]))
    assert out["moves"].to_list() == [0]
    assert out["points_per_move"].to_list() == [None]


# --- across seasons -------------------------------------------------------

def test_each_season_is_matched_against_its_own_draft():
    """Team names drift between seasons: Jack's "Cococnut Crushers" became
    "Coconut Crushers" in 2023. Matching a season against the wrong draft would
    credit the wrong roster."""
    history = dv.acquisition_history(
        pl.concat([_picks([{"team_name": "Cococnut", "player_id": 1}], season=2022),
                   _picks([{"team_name": "Coconut", "player_id": 2}], season=2023)]),
        {2022: _weeks([{"team_name": "Cococnut", "player_id": 1, "points": 10.0}]),
         2023: _weeks([{"team_name": "Coconut", "player_id": 2, "points": 20.0}])},
    )
    assert history["season"].to_list() == [2022, 2023]
    assert history["drafted"].to_list() == [10.0, 20.0]
    assert history["added"].to_list() == [0.0, 0.0]


def test_a_manager_espn_renamed_is_still_one_manager():
    """ESPN recorded Jack Winfield as "J W" for one season, which split him into a
    separate manager with one season to his name. The owner GUID is stable and is
    what the averages group on."""
    history = pl.concat([
        _averaged_row("Jack Winfield", 2024, 100.0, 0.0),
        _averaged_row("J W", 2025, 200.0, 0.0),
    ])
    out = dv.acquisition_averages(history)
    assert out.height == 1
    assert out["seasons"].to_list() == [2]
    # The name they go by now, not an arbitrary one from the earliest season.
    assert out["manager"].to_list() == ["J W"]
    assert out["drafted"].to_list() == [150.0]


def test_averages_are_per_season_not_pooled():
    """So a manager is not weighted by how many seasons they happened to play."""
    history = pl.concat([_averaged_row("Solo", 2024, 100.0, 100.0),
                         _averaged_row("Solo", 2025, 300.0, 100.0)])
    out = dv.acquisition_averages(history)
    assert out["drafted"].to_list() == [200.0]


def test_no_seasons_is_empty_rather_than_raising():
    assert dv.acquisition_history(pl.DataFrame(), {}).is_empty()
    assert dv.acquisition_averages(pl.DataFrame()).is_empty()


# --- opponent notes, filtered to who is actually draftable ----------------

def _note(player, traits, description, seasons=10):
    return pl.DataFrame([{"owner": "Someone", "owner_display": "Someone",
                          "seasons": seasons, "favourite_player": player,
                          "traits": traits, "description": description}])


LOYALTY = "has drafted {} in 4 of the 5 drafts he was available in"
TIMING = "waits on K — round 14.7 against the room's 11.7"
TIMING2 = "waits on TE — round 7.5 against the room's 5.4"


def test_a_loyalty_clause_survives_when_the_player_is_draftable():
    note = _note("Bijan Robinson",
                 [TIMING, LOYALTY.format("Bijan Robinson")],
                 "Waits on K — round 14.7 against the room's 11.7. Has "
                 "drafted Bijan Robinson in 4 of the 5 drafts he was available "
                 "in. (10 drafts)")
    out = dv.notes_for_board(note, _board([{"player_name": "Bijan Robinson"}]))
    assert "Bijan Robinson" in out["description"][0]
    assert out["favourite_player_draftable"].to_list() == [True]


def test_a_loyalty_clause_goes_when_the_player_is_not_on_the_board():
    """Leonard Fournette is true and useless: a third of a note nobody has time
    to read, spent on a player nobody can take."""
    note = _note("Leonard Fournette",
                 [TIMING, LOYALTY.format("Leonard Fournette"), TIMING2],
                 "Waits on K — round 14.7 against the room's 11.7. "
                 "Has drafted Leonard Fournette in 4 of the 5 drafts he was "
                 "available in. Waits on TE — round 7.5 against the room's 5.4. "
                 "(10 drafts)")
    out = dv.notes_for_board(note, _board([{"player_name": "Bijan Robinson"}]))
    described = out["description"][0]
    assert "Fournette" not in described
    assert "Waits on K" in described and "Waits on TE" in described
    assert described.endswith("(10 drafts)")
    assert out["favourite_player_draftable"].to_list() == [False]


def test_a_name_full_of_full_stops_does_not_break_the_rewrite():
    """T.Y. Hilton is why the trait text is matched rather than the description
    being split on sentences."""
    note = _note("T.Y. Hilton",
                 [TIMING, LOYALTY.format("T.Y. Hilton")],
                 "Waits on K — round 14.7 against the room's 11.7. Has drafted "
                 "T.Y. Hilton in 4 of the 5 drafts he was available in. "
                 "(10 drafts)")
    out = dv.notes_for_board(note, _board([{"player_name": "Somebody Else"}]))
    assert out["description"][0] == ("Waits on K — round 14.7 against the "
                                     "room's 11.7. (10 drafts)")


def test_only_the_loyalty_clause_is_filtered():
    """Timing and team lean describe *how* a manager drafts and stay true
    whoever happens to be available."""
    note = _note("Leonard Fournette",
                 [TIMING, LOYALTY.format("Leonard Fournette")],
                 "Waits on K — round 14.7 against the room's 11.7. Has drafted "
                 "Leonard Fournette in 4 of the 5 drafts he was available in. "
                 "(10 drafts)")
    out = dv.notes_for_board(note, _board([{"player_name": "Nobody"}]))
    assert "Waits on K" in out["description"][0]
    assert TIMING in out["traits"][0].to_list()


def test_a_note_that_was_only_about_a_stale_player_says_so():
    note = _note("Leonard Fournette", [LOYALTY.format("Leonard Fournette")],
                 "Has drafted Leonard Fournette in 4 of the 5 drafts he was "
                 "available in. (10 drafts)")
    out = dv.notes_for_board(note, _board([{"player_name": "Nobody"}]))
    assert "Fournette" not in out["description"][0]
    assert "not on this year's board" in out["description"][0]


def test_notes_without_the_columns_to_decide_are_left_alone():
    assert dv.notes_for_board(pl.DataFrame(), _board([{}])).is_empty()
    note = _note("Someone", [TIMING], "Waits on K. (10 drafts)").drop("traits")
    assert dv.notes_for_board(note, _board([{}]))["description"][0] == \
        "Waits on K. (10 drafts)"


# --- colour --------------------------------------------------------------

def test_every_hued_position_has_a_colour_in_both_themes():
    for theme in ("light", "dark"):
        for slot in dv.POSITION_HUES.values():
            assert slot in dv.SERIES_COLORS[theme]


def test_position_colours_are_fixed_not_assigned_by_filter_order():
    """Deselecting kickers must not repaint running backs."""
    assert dv.POSITION_HUES["RB"] == 2
    assert len(set(dv.POSITION_HUES.values())) == len(dv.POSITION_HUES)


def test_tier_is_not_colour_encoded():
    """Eight tiers cannot be stepped down one hue with separable lightness -- the
    validator fails adjacent ΔL at 0.047 and so does the eye. Tier goes on an axis
    instead, so no tier ramp should exist to be tempted by."""
    assert not any(name.startswith("TIER_") for name in dir(dv))


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_the_palette_covers_every_hue_slot(theme):
    assert set(dv.POSITION_HUES.values()) <= set(dv.SERIES_COLORS[theme])
