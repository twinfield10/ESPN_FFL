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
