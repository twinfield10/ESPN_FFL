"""The draft board page's derivations: filters, roster needs, chart data.

The page script itself is layout and needs a Streamlit runtime; everything with a
decision in it lives in ``app/draft_view.py`` and is covered here. No network, no
store on disk -- the boards are synthesised to the shape ``build_board`` returns.
"""

import re
import sys
from datetime import date
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
        # ESPN's half of the four comparison groups, and the differences against it.
        # All from `build_board` -- see Scripts.draft.board._attach_espn_comparison.
        "ESPN_Points": 95.0, "espn_draft_rank": 3, "espn_pos_rank": 2.0,
        "points_delta": 5.0, "rank_delta": 2.0, "pos_rank_delta": 1.0,
        # The injury report's own two columns, joined onto the board at build time.
        "injury_return_date": None, "injury_note": None,
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


def test_search_matches_a_name_literally_rather_than_as_a_regex():
    """Half the interesting names on a board are regex syntax. "T.J." matched any
    three characters between the dots, and a typed "(" raised out of the page."""
    board = _board([
        {"player_name": "T.J. Hockenson"},
        {"player_name": "TQJX Nobody"},
    ])
    assert dv.filter_board(board, None, search="T.J.")["player_name"].to_list() \
        == ["T.J. Hockenson"]
    assert dv.filter_board(board, None, search="(unclosed").is_empty()


def test_the_team_filter_keeps_only_the_teams_named():
    board = _board([
        {"player_name": "Lion", "pro_team": "DET"},
        {"player_name": "Packer", "pro_team": "GB"},
        {"player_name": "Bear", "pro_team": "CHI"},
    ])
    assert dv.filter_board(board, None, teams=["DET", "CHI"])["player_name"] \
        .to_list() == ["Lion", "Bear"]


def test_an_empty_team_selection_keeps_every_team():
    board = _board([{"pro_team": "DET"}, {"pro_team": "GB"}])
    assert dv.filter_board(board, None, teams=[]).height == 2
    assert dv.filter_board(board, None, teams=None).height == 2


def test_the_bye_filter_keeps_only_the_weeks_named():
    """Include semantics, like every other filter on the page: "bye weeks 5 and 10"
    means show me those, not hide them."""
    board = _board([
        {"player_name": "Five", "bye_week": 5.0},
        {"player_name": "Ten", "bye_week": 10.0},
        {"player_name": "Fourteen", "bye_week": 14.0},
    ])
    assert dv.filter_board(board, None, byes=[5, 10])["player_name"].to_list() \
        == ["Five", "Ten"]


def test_a_bye_selection_drops_players_whose_bye_is_unknown():
    """"Keep only week 5" cannot honestly keep a player nobody knows the bye of."""
    board = _board([
        {"player_name": "Known", "bye_week": 5.0},
        {"player_name": "Unknown", "bye_week": None},
    ])
    assert dv.filter_board(board, None, byes=[5])["player_name"].to_list() == ["Known"]
    assert dv.filter_board(board, None, byes=[]).height == 2


def test_the_four_board_filters_compose():
    board = _board([
        {"player_name": "Jahmyr Gibbs", "primaryPosition": "RB",
         "pro_team": "DET", "bye_week": 5.0},
        {"player_name": "Jameson Williams", "primaryPosition": "WR",
         "pro_team": "DET", "bye_week": 5.0},
        {"player_name": "Josh Jacobs", "primaryPosition": "RB",
         "pro_team": "GB", "bye_week": 5.0},
        {"player_name": "Jahmyr Nobody", "primaryPosition": "RB",
         "pro_team": "DET", "bye_week": 9.0},
    ])
    out = dv.filter_board(board, ["RB"], teams=["DET"], byes=[5], search="jahmyr")
    assert out["player_name"].to_list() == ["Jahmyr Gibbs"]


# --- the filter options ---------------------------------------------------

def test_teams_and_byes_are_offered_from_what_the_board_actually_holds():
    board = _board([
        {"pro_team": "GB", "bye_week": 10.0},
        {"pro_team": "DET", "bye_week": 5.0},
        {"pro_team": "DET", "bye_week": 5.0},
    ])
    assert dv.board_teams(board) == ["DET", "GB"]
    assert dv.board_byes(board) == [5, 10]


def test_a_board_with_no_byes_offers_no_bye_filter_rather_than_raising():
    """What a board built before bye weeks were attached looks like."""
    board = _board([{}]).drop(["bye_week", "pro_team"])
    assert dv.board_byes(board) == [] and dv.board_teams(board) == []


# --- keepers --------------------------------------------------------------
#
# ESPN carries a keeper league's rosters into the new season before anyone has
# declared a keeper, so `on_team_id` means "was here last year" rather than
# "unavailable". GOP Degenerates' 2026 board arrives with 252 players held across
# 16 teams against a keeper limit of 2.

def _keeper_board(per_team, keepers_named=None):
    """A board where each team holds `per_team` players."""
    rows = []
    for team in (1, 2, 3):
        held = keepers_named if keepers_named is not None else per_team
        for i in range(held):
            rows.append({"player_name": f"T{team}P{i}", "on_team_id": team,
                         "team_owner": f"Owner {team}"})
    rows += [{"player_name": f"FA{i}"} for i in range(20)]
    return _board(rows)


def test_a_roster_bigger_than_the_keeper_limit_cannot_be_a_list_of_keepers():
    board = _keeper_board(15)
    assert dv.keepers_pending(board, keepers=2) is True


def test_rosters_at_the_keeper_limit_are_believed():
    """The day rosters shrink to the limit the board starts filtering again, with
    no flag anyone has to remember to flip."""
    board = _keeper_board(2)
    assert dv.keepers_pending(board, keepers=2) is False


def test_one_team_still_holding_a_full_roster_holds_the_whole_league_open():
    """Half-declared is not declared. Hiding the other fifteen managers' players
    on the strength of one having tidied up is the harmful direction."""
    board = _board(
        [{"player_name": f"A{i}", "on_team_id": 1} for i in range(2)]
        + [{"player_name": f"B{i}", "on_team_id": 2} for i in range(15)])
    assert dv.keepers_pending(board, keepers=2) is True


def test_a_redraft_league_is_never_pending():
    assert dv.keepers_pending(_keeper_board(15), keepers=0) is False


def test_a_store_predating_draft_settings_keeps_its_old_behaviour():
    """None is not zero. An old board should not silently change meaning."""
    assert dv.keepers_pending(_keeper_board(15), keepers=None) is False
    assert dv.keeper_count({}) is None
    assert dv.keeper_count({"draft_settings": {"keeper_count": 0}}) == 0
    assert dv.keeper_count({"draft_settings": {"keeper_count": 2}}) == 2


def test_rostered_counts_ignores_free_agents():
    counts = dv.rostered_counts(_keeper_board(3))
    assert counts == {1: 3, 2: 3, 3: 3}


def test_a_board_with_no_roster_column_is_not_pending():
    board = _board([{}]).drop("on_team_id")
    assert dv.rostered_counts(board) == {}
    assert dv.keepers_pending(board, keepers=2) is False


def test_the_tier_runway_can_count_players_the_rosters_claim():
    """"Three left in tier 1" is a lie if it excluded players nobody has kept."""
    board = _board([
        {"player_name": "Free", "tier": 1.0, "TRUE_Points": 300.0},
        {"player_name": "Carried", "tier": 1.0, "TRUE_Points": 290.0,
         "on_team_id": 4, "team_owner": "Someone"},
    ])
    assert dv.tier_runway(board, ["RB"])["remaining"].to_list() == [1]
    assert dv.tier_runway(board, ["RB"], only_available=False)["remaining"] \
        .to_list() == [2]


def test_value_targets_can_include_players_the_rosters_claim():
    board = _board([
        {"player_name": "Free", "value": 5.0},
        {"player_name": "Carried", "value": 40.0, "on_team_id": 4,
         "team_owner": "Someone"},
    ])
    assert dv.value_targets(board)["player_name"].to_list() == ["Free"]
    assert dv.value_targets(board, only_available=False)["player_name"].to_list() \
        == ["Carried", "Free"]


# --- the column spec table -------------------------------------------------
#
# The old pair of tests here asserted that `DISPLAY_COLUMNS` and `COLUMN_GLOSSARY`
# agreed in both directions. There is now one record per column carrying both, so
# that particular drift is structurally impossible and there is nothing left to
# assert about it. What can still go wrong is a record that is *incomplete*, which is
# what these check instead.

def test_every_column_carries_its_own_documentation():
    """A column with no account of where it came from is a number the reader has to
    guess at. Enforced on the record rather than across two collections, because
    there is only one collection now."""
    for column in dv.COLUMNS:
        assert column.source and column.group and column.label, column
        assert column.kind in ("text", "number", "button"), column
        assert column.source_of, column.label
        assert column.how, column.label


def test_a_number_column_has_a_format_and_a_text_column_does_not():
    """`st.column_config.TextColumn` takes no `format=`, so a text spec carrying one
    is a TypeError at render time and not before."""
    for column in dv.COLUMNS:
        if column.kind == "number":
            assert column.fmt, column.label
        else:
            assert column.fmt is None, column.label


def test_every_difference_column_is_emphasised_and_signed():
    """The colour scale and the bolding both key off `emphasis`, and the printed sign
    is what makes the scale readable without telling green from red — so a Δ column
    that lost its `%+` format would silently drop the second channel."""
    for column in dv.COLUMNS:
        if column.label.startswith("Δ") or (column.group == "Keepers"
                                            and column.label == "Value"):
            assert column.emphasis, column.label
            assert column.fmt.startswith("%+"), (column.group, column.label, column.fmt)


def test_no_two_specs_claim_the_same_header_in_the_same_lens():
    """The labels repeat across groups on purpose — that is what the spanners are
    for — but two specs under one `(group, label)` in one lens would render as
    duplicate columns and make the positional config ambiguous."""
    for lens in (dv.VALUE_LENS_ADP, dv.VALUE_LENS_CASH):
        keys = [(c.group, c.label) for c in dv.COLUMNS if c.lens in ("", lens)]
        assert len(keys) == len(set(keys)), lens


def test_the_identity_block_is_the_frozen_one():
    """Freezing is what makes 26 columns scrollable — lose the player's name off the
    left edge and the row stops being about anybody."""
    pinned = [(c.group, c.label) for c in dv.COLUMNS if c.pinned]
    assert pinned == [("Player Info", label)
                      for label in ("Player", "Pos", "NFL", "Bye", "Tier")]


# --- the glossary ---------------------------------------------------------

def test_the_glossary_is_scoped_to_the_columns_actually_rendered():
    """A redraft league must not be told about keeper prices it does not have."""
    md = dv.glossary_markdown([("Player Info", "Player"), ("Points", "VOR")])
    assert "**Player**" in md and "**VOR**" in md
    assert "Keeper" not in md and "USG" not in md


def test_the_glossary_keeps_the_tables_column_order():
    md = dv.glossary_markdown([("Points", "VOR"), ("Player Info", "Player"),
                               ("Draft Metric", "ESPN")])
    assert md.index("**Player**") < md.index("**VOR**") < md.index("**ESPN**")


def test_the_glossary_groups_by_spanner():
    """It reads in the same shape as the table it sits under, which is the whole
    reason to split it — `ESPN` means a different number in each group."""
    md = dv.glossary_markdown([("Player Info", "Player"), ("Ranks", "ESPN")])
    assert md.index("**Player Info**") < md.index("**Player**")
    assert md.index("**Ranks**") < md.index("**ESPN**")
    assert "**Points**" not in md


def test_the_glossary_has_exactly_one_row_per_rendered_column():
    """The two Draft Metric variants share their three headers, so selecting on
    `(group, label)` alone matched both: an auction league's glossary described its `Δ`
    twice, once as a dollar difference and once as an ADP rank difference, with no way
    to tell which the column above it was."""
    for lens in (dv.VALUE_LENS_ADP, dv.VALUE_LENS_CASH):
        frame = dv.display_frame(_board([{}]), lens)
        md = dv.glossary_markdown(frame.columns, lens)
        assert md.count("\n| **") == frame.shape[1], (lens, md.count("\n| **"),
                                                      frame.shape[1])


def test_the_glossary_describes_the_lens_that_is_on_screen():
    adp = dv.glossary_markdown([("Draft Metric", "ESPN")], dv.VALUE_LENS_ADP)
    cash = dv.glossary_markdown([("Draft Metric", "ESPN")], dv.VALUE_LENS_CASH)
    assert "averageDraftPosition" in adp and "auctionValueAverage" not in adp
    assert "auctionValueAverage" in cash and "averageDraftPosition" not in cash


def test_a_column_with_no_caveat_still_gets_a_cell():
    """An empty fourth cell would read as a missing row rather than as nothing to
    warn about."""
    md = dv.glossary_markdown([("Player Info", "Player")])
    assert md.rstrip().endswith("| — |")


def test_dollar_amounts_are_escaped_so_streamlit_does_not_read_them_as_maths():
    """Paired `$` is LaTeX to Streamlit, and a glossary about auction values is
    full of them — unescaped, the middle of the table renders as equations."""
    md = dv.glossary_markdown([("Draft Metric", "ESPN"), ("Draft Metric", "Us"),
                               ("Keepers", "Price")])
    assert "$" in md and r"\$" in md
    assert not re.search(r"(?<!\\)\$", md)


def test_the_whole_glossary_escapes_every_dollar():
    for lens in (dv.VALUE_LENS_ADP, dv.VALUE_LENS_CASH):
        assert not re.search(r"(?<!\\)\$", dv.glossary_markdown(lens=lens)), lens


def test_the_glossary_renders_a_four_column_markdown_table():
    md = dv.glossary_markdown([("Player Info", "Player")])
    lines = md.split("\n")
    assert lines[0] == "**Player Info**"
    assert lines[2].startswith("| Column | Source |")
    assert lines[3] == "|---|---|---|---|"
    assert lines[4].count("|") == 5


# --- the cash lens --------------------------------------------------------
#
# `value` compares ranks, which is the right comparison in a snake draft and the
# wrong one in an auction: being four places underrated does not say whether to bid
# $41 or $46.

AUCTION_META = {"team_count": 10, "roster_slots": {"QB": 1, "RB": 2, "WR": 2,
                                                   "TE": 1, "BE": 6, "IR": 2},
                "draft_settings": {"type": "AUCTION", "auction_budget": 200}}


def test_ir_slots_are_not_roster_spots_the_room_buys():
    """12 draftable slots x 10 teams. Counting IR would invent money nobody spends."""
    assert dv.draftable_spots(AUCTION_META) == 120


def test_the_whole_budget_is_allocated_and_no_more():
    """Ten teams at $200 is $2,000, and the priced players plus the $1 floors on
    every other spot must come to exactly that."""
    rows = [{"player_name": f"P{i}", "vor": float(200 - i)} for i in range(150)]
    board = dv.with_cash_value(_board(rows), AUCTION_META, 200)
    priced = board.filter(pl.col("our_dollars").is_not_null())
    assert priced.height == 120                       # one per spot, no more
    spent = priced["our_dollars"].sum()
    assert abs(spent - 2000.0) < 1e-6


def test_a_replacement_level_player_is_worth_the_minimum_bid():
    board = dv.with_cash_value(
        _board([{"player_name": "Star", "vor": 100.0},
                {"player_name": "Filler", "vor": 0.0}]), AUCTION_META, 200)
    prices = dict(zip(board["player_name"], board["our_dollars"]))
    assert prices["Filler"] is None                   # outside the money entirely
    assert prices["Star"] > 1


def test_streamed_positions_get_no_auction_money():
    """The mistake that once priced eight team defences as the league's best buys."""
    board = _board([{"player_name": "Back", "vor": 50.0, "is_streamed": False},
                    {"player_name": "Kicker", "primaryPosition": "K", "vor": 60.0,
                     "is_streamed": True}])
    out = dv.with_cash_value(board, AUCTION_META, 200)
    prices = dict(zip(out["player_name"], out["our_dollars"]))
    assert prices["Kicker"] is None and prices["Back"] is not None


def test_cash_delta_is_our_price_less_the_markets_in_the_same_currency():
    board = dv.at_budget(_board([{"vor": 100.0, "auction_value_filled": 40.0}]), 200)
    out = dv.with_cash_value(board, AUCTION_META, 200)
    assert out["auction_dollars"].to_list() == [40.0]
    assert abs(out["cash_delta"][0] - (out["our_dollars"][0] - 40.0)) < 1e-9


def test_a_board_with_no_roster_shape_is_returned_untouched():
    board = _board([{"vor": 100.0}])
    assert dv.with_cash_value(board, {}, 200).columns == board.columns
    assert "Our $" not in dv.display_frame(dv.with_cash_value(board, {}, 200)).columns


def test_auction_leagues_open_on_cash_and_snake_leagues_on_adp():
    assert dv.default_value_lens(AUCTION_META) == dv.VALUE_LENS_CASH
    assert dv.default_value_lens(
        {"draft_settings": {"type": "SNAKE"}}) == dv.VALUE_LENS_ADP
    assert dv.default_value_lens({}) == dv.VALUE_LENS_ADP


def test_the_lens_chooses_which_column_the_value_table_ranks_on():
    board = dv.at_budget(_board([
        {"player_name": "AdpFall", "value": 90.0, "vor": 10.0,
         "auction_value_filled": 1.0},
        {"player_name": "CashFall", "value": 1.0, "vor": 300.0,
         "auction_value_filled": 1.0},
    ]), 200)
    board = dv.with_cash_value(board, AUCTION_META, 200)
    assert dv.value_targets(board, lens=dv.VALUE_LENS_ADP)["player_name"][0] \
        == "AdpFall"
    assert dv.value_targets(board, lens=dv.VALUE_LENS_CASH)["player_name"][0] \
        == "CashFall"


def test_the_cash_lens_is_empty_rather_than_wrong_on_a_board_without_it():
    board = _board([{"value": 10.0}])
    assert dv.value_targets(board, lens=dv.VALUE_LENS_CASH).is_empty()
    assert not dv.value_targets(board, lens=dv.VALUE_LENS_ADP).is_empty()


# --- the keeper price -----------------------------------------------------

def test_the_keeper_price_is_shown_only_where_the_league_has_keepers():
    """Every board carries keeper_value, including the eight redraft leagues, where
    it is a number ESPN publishes for nobody's benefit."""
    board = _board([{"keeper_value": 90}])
    assert "keeper_price" not in dv.with_keeper_price(board, 0).columns
    assert "keeper_price" not in dv.with_keeper_price(board, None).columns
    assert "keeper_price" in dv.with_keeper_price(board, 2).columns


def test_a_waiver_pickup_keeps_for_the_minimum_bid_not_for_nothing():
    """ESPN reports keeperValue 0 for a player claimed off waivers -- there was no
    winning bid to record -- and shows $1 in its own UI. 65 of GOP's 252 held
    players are in that state; reading the zero as "no keeper price" blanked a
    quarter of the league's keepers, Malik Nabers among them."""
    out = dv.with_keeper_price(_board([
        {"player_name": "Waiver", "keeper_value": 0, "on_team_id": 7},
        {"player_name": "Auction", "keeper_value": 12, "on_team_id": 7},
    ]), 2)
    assert out["keeper_price"].to_list() == [1.0, 12.0]


def test_only_a_free_agent_has_no_keeper_price():
    """Being on a roster is what confers one. A blank says "nobody can keep him"."""
    out = dv.with_keeper_price(_board([
        {"player_name": "Rostered", "keeper_value": 0, "on_team_id": 7},
        {"player_name": "Free", "keeper_value": 0, "on_team_id": 0},
    ]), 2)
    assert out["keeper_price"].to_list() == [1.0, None]


def test_a_board_without_the_roster_column_falls_back_to_the_price():
    """Loses the waiver pickups rather than inventing keepers out of free agents."""
    board = _board([{"keeper_value": 0}, {"keeper_value": 12}]).drop("on_team_id")
    assert dv.with_keeper_price(board, 2)["keeper_price"].to_list() == [None, 12.0]


#: A meta with enough shape for `with_cash_value` to price a pool.
CASH_META = {"team_count": 10, "roster_slots": {"RB": 2, "BE": 4},
             "starting_roster_slots": {"RB": 2}}


def _priced(rows, budget=250):
    """A board through the money chain, in the order the page runs it."""
    board = dv.at_budget(_board(rows), budget)
    return dv.with_cash_value(board, CASH_META, budget)


def test_the_surplus_is_our_valuation_less_what_keeping_costs():
    """**Our** valuation, not ESPN's, and that is the point of the column: what decides
    whether to keep a player is whether *we* rate him that highly. The room's price is
    a fact about other people's money."""
    board = _priced([{"keeper_value": 40, "on_team_id": 7}])
    out = dv.with_keeper_price(board, 2)

    ours = out["our_dollars"].to_list()[0]
    assert ours is not None, "the fixture has to be inside the money"
    assert out["keeper_surplus"].to_list() == [ours - 40.0]


def test_our_dollars_are_denominated_in_the_budget():
    """The whole point of the column: the number is what to spend out of the money you
    actually have, so it never allocates more than the room has. Scaling it onto the
    market's price level was tried and reverted — it made the difference beside it
    tidier and destroyed the only property that makes the column actionable."""
    board = _priced([{"player_name": f"P{i}", "vor": float(50 - i)}
                     for i in range(20)])
    on_the_table = CASH_META["team_count"] * 250
    total = board["our_dollars"].sum()
    assert total <= on_the_table
    # The shortfall is the $1 floor on the roster spots this shallow pool cannot fill,
    # which is money the room would spend and our pool has nobody to spend it on.
    assert total > on_the_table - dv.draftable_spots(CASH_META) * dv.MIN_BID


def test_every_priced_player_clears_the_minimum_bid():
    board = _priced([{"player_name": f"P{i}", "vor": float(50 - i)}
                     for i in range(20)])
    assert board["our_dollars"].drop_nulls().min() >= dv.MIN_BID


def test_the_allocation_follows_our_own_ordering():
    """Who we rate above whom is what the dollars encode — nothing else touches it."""
    board = _priced([{"player_name": f"P{i}", "vor": float(50 - i)}
                     for i in range(20)])
    ours = board.sort("vor", descending=True)["our_dollars"].drop_nulls().to_list()
    assert ours == sorted(ours, reverse=True)


def test_the_cash_difference_reflects_our_rating_not_the_market_s_level():
    """Two players the market prices identically: the one we rate higher reads more
    positive. That ordering is what the column is for — it runs positive for most
    players inside the money, because we allocate the whole budget across the players
    worth rostering while the market spreads it over three times as many."""
    board = _priced([
        {"player_name": "We Like", "vor": 90.0, "auction_value_filled": 40.0},
        {"player_name": "We Fade", "vor": 10.0, "auction_value_filled": 40.0},
    ])
    deltas = dict(zip(board["player_name"].to_list(), board["cash_delta"].to_list()))
    assert deltas["We Like"] > deltas["We Fade"]


def test_a_board_with_no_market_price_still_prices_our_side():
    """Our valuation does not depend on ESPN's, so it survives a board ESPN never
    priced. The difference beside it is null, which is honest: no market to differ
    from."""
    board = _board([{"keeper_value": 40, "on_team_id": 7}]).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("auction_value_filled"))
    out = dv.with_cash_value(dv.at_budget(board, 250), CASH_META, 250)

    raw = dv.MIN_BID + (CASH_META["team_count"] * 250
                        - dv.draftable_spots(CASH_META) * dv.MIN_BID)
    assert out["our_dollars"].to_list() == [float(raw)]   # the whole pool, unscaled
    assert out["cash_delta"].to_list() == [None]


def test_the_surplus_is_blank_where_we_publish_no_valuation():
    """`our_dollars` is null outside the money and for the streamed positions, so the
    surplus is blank there rather than claiming a bargain we cannot price. The old
    market-based version could always produce a number, because ESPN prices everybody.
    """
    # A real back alongside the kicker, so there is a pool to price at all -- with
    # only streamed players `with_cash_value` has no positive VOR to split and bails.
    board = _priced([
        {"player_name": "Back", "keeper_value": 40, "on_team_id": 7},
        {"player_name": "Kicker", "primaryPosition": "K", "is_streamed": True,
         "keeper_value": 40, "on_team_id": 7},
    ])
    out = dv.with_keeper_price(board, 2)
    assert out["our_dollars"].to_list()[1] is None
    assert out["keeper_surplus"].to_list()[1] is None
    assert out["keeper_surplus"].to_list()[0] is not None


def test_there_is_no_surplus_column_before_the_cash_value_is_computed():
    """`display_frame` then drops it, as it does for anything else the board cannot
    support — rather than the page raising on a column that was never made."""
    out = dv.with_keeper_price(dv.at_budget(_board([{"keeper_value": 40}]), 250), 2)
    assert "keeper_surplus" not in out.columns
    assert "keeper_price" in out.columns


def test_the_keeper_columns_get_their_own_spanner():
    frame = dv.display_frame(dv.with_injury_code(dv.with_keeper_price(
        _priced([{"keeper_value": 40, "on_team_id": 7}]), 2)),
        dv.VALUE_LENS_CASH)
    order = list(frame.columns)
    assert order.index(("Keepers", "Price")) < order.index(("Keepers", "Value"))
    # After the money it is differenced against, and before the status block.
    assert order.index(("Draft Metric", "ESPN")) < order.index(("Keepers", "Price"))
    assert order.index(("Keepers", "Owner")) < order.index(("Notes", "Injury"))


# --- the auction budget ---------------------------------------------------

def test_the_budget_is_remembered_per_league_not_globally():
    """A shared key outlives a league change, and a keyed widget ignores its
    default once the key exists -- which rendered GOP's $250 auction at $200."""
    assert dv.budget_key("gop_degenerates") != dv.budget_key("winfield_football")
    assert dv.budget_key("gop_degenerates").startswith(dv.AUCTION_BUDGET_KEY)


def test_the_budget_comes_from_the_league_not_a_constant():
    """GOP Degenerates plays for $250 and the other eight for $200. Defaulting all
    nine to one number mispriced eight of them by 25%."""
    assert dv.league_auction_budget({"draft_settings": {"auction_budget": 250}}) == 250
    assert dv.league_auction_budget({"draft_settings": {"auction_budget": 200}}) == 200


def test_a_store_with_no_recorded_budget_falls_back():
    assert dv.league_auction_budget({}) == dv.DEFAULT_AUCTION_BUDGET
    assert dv.league_auction_budget({"draft_settings": {}}, default=300) == 300
    assert dv.league_auction_budget(
        {"draft_settings": {"auction_budget": None}}) == dv.DEFAULT_AUCTION_BUDGET

def test_auction_values_are_rescaled_from_espns_budget_to_this_leagues():
    """The stored number is a market average in ESPN's own $200 auction, so a
    league playing for $250 was reading a column denominated in other money."""
    board = dv.at_budget(_board([{"auction_value_filled": 20.0}]), 250)
    assert board["auction_share"].to_list() == [0.1]
    assert board["auction_dollars"].to_list() == [25.0]


def test_the_default_budget_leaves_the_share_alone_and_scales_the_dollars():
    board = dv.at_budget(_board([{"auction_value_filled": 64.0}]),
                         dv.BASE_AUCTION_BUDGET)
    assert board["auction_dollars"].to_list() == [64.0]


def test_the_dollar_column_reads_the_rescaled_value_not_the_stored_one():
    frame = dv.display_frame(dv.at_budget(_board([{"auction_value_filled": 20.0}]),
                                          400),
                             dv.VALUE_LENS_CASH)
    assert frame[("Draft Metric", "ESPN")].to_list() == [40.0]


def test_a_board_with_no_auction_column_is_returned_untouched():
    board = _board([{}]).drop("auction_value_filled")
    assert dv.at_budget(board, 250).columns == board.columns
    assert "$" not in dv.display_frame(dv.at_budget(board, 250)).columns


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


# --- the injury column ----------------------------------------------------

@pytest.mark.parametrize("status,code", [
    ("ACTIVE", "A"), ("QUESTIONABLE", "Q"), ("OUT", "O"),
    ("INJURY_RESERVE", "IR"), ("SUSPENSION", "SUS"), ("DOUBTFUL", "D"),
])
def test_the_injury_status_is_abbreviated(status, code):
    board = dv.with_injury_code(_board([{"injury_status": status}]))
    assert board["injury_code"].to_list() == [code]


def test_an_unknown_status_is_shown_in_full_rather_than_mangled():
    """ESPN's enum is not published and this repo has observed five of it.
    Abbreviating an unseen sixth to its first letter would collide with a real code —
    a hypothetical `TRADED` would render as the `T` nothing else uses — and blanking
    it would hide the player's status entirely. Ugly and correct beats tidy and wrong.
    """
    board = dv.with_injury_code(_board([{"injury_status": "PARENTAL_LEAVE"}]))
    assert board["injury_code"].to_list() == ["PARENTAL_LEAVE"]


def test_a_missing_status_stays_missing():
    board = dv.with_injury_code(_board([{"injury_status": None}]))
    assert board["injury_code"].to_list() == [None]


def test_a_season_ending_return_date_reads_as_a_word_not_a_date():
    """ESPN encodes "out for the year" as a return date past the end of the season,
    so the raw value is a real date that means something other than a date. A reader
    shown `2027-02-15` would have to know the convention to read it."""
    board = dv.with_injury_code(_board([
        {"player_name": "Gone", "injury_return_date": date(2027, 2, 15)},
        {"player_name": "Back", "injury_return_date": date(2026, 10, 5)},
        {"player_name": "Fine", "injury_return_date": None},
    ]))
    assert board["injury_return_date"].to_list() == [dv.RETURN_SEASON_ENDING,
                                                    "Oct 05", None]


def test_encoding_the_injury_twice_does_not_re_read_its_own_output():
    """The return date is rewritten in place, so a second pass over the same frame
    would try to parse `Oct 05` as a date. Streamlit reruns this function every
    render."""
    once = dv.with_injury_code(_board([{"injury_return_date": date(2026, 10, 5)}]))
    twice = dv.with_injury_code(once)
    assert twice["injury_return_date"].to_list() == ["Oct 05"]


def test_a_board_carrying_none_of_the_injury_columns_is_returned_untouched():
    board = _board([{}]).drop(["injury_status", "injury_return_date", "injury_note"])
    assert dv.with_injury_code(board).columns == board.columns


# --- the news mark, and the note behind it --------------------------------

def test_a_player_with_a_note_gets_a_mark_and_one_without_gets_nothing():
    """The column carries a mark rather than the sentence: a sentence needs 400px,
    which on a 26-column table is a quarter of the width spent on a cell that
    truncated mid-clause anyway."""
    board = dv.with_injury_code(_board([
        {"player_name": "Hurt", "injury_note": "Nabers (knee) isn't in uniform."},
        {"player_name": "Fine", "injury_note": None},
        {"player_name": "Blank", "injury_note": ""},
    ]))
    assert board["note_mark"].to_list() == [dv.NOTE_MARK, None, None]


def test_the_note_reads_back_with_the_player_it_belongs_to():
    board = dv.with_injury_code(_board([
        {"player_name": "Malik Nabers", "primaryPosition": "WR", "pro_team": "NYG",
         "injury_status": "QUESTIONABLE", "injury_return_date": date(2026, 8, 15),
         "injury_note": "Nabers (knee) isn't in uniform for Saturday's practice."},
    ]))
    note = dv.player_note(board, 0)
    assert "**Malik Nabers**" in note
    assert "WR NYG" in note and "Questionable" in note and "back Aug 15" in note
    assert "isn't in uniform" in note


def test_a_player_with_no_note_renders_no_panel():
    """None rather than an empty panel, so the caller shows nothing instead of a
    bordered box with a name in it."""
    board = dv.with_injury_code(_board([{"injury_note": None}]))
    assert dv.player_note(board, 0) is None


def test_an_out_of_range_selection_is_not_an_error():
    """A selection survives a rerun that shortened the table — filter the board down
    and the remembered row can point past the end."""
    board = dv.with_injury_code(_board([{"injury_note": "Something."}]))
    assert dv.player_note(board, 5) is None
    assert dv.player_note(board, -1) is None


def test_a_click_is_remembered_as_a_player_not_as_a_row():
    """A row number is a position in the sorted frame on screen: the moment the sort
    changes it names a different player, silently, which is the worst way to be wrong.
    Resolving to a player id at click time is what survives a re-sort."""
    board = dv.with_injury_code(_board([
        {"player_id": 1, "player_name": "First", "vor": 1.0, "injury_note": "One."},
        {"player_id": 2, "player_name": "Second", "vor": 99.0, "injury_note": "Two."},
    ]))
    by_vor = board.sort("vor", descending=True)          # Second is now row 0

    state = {dv.NOTE_CLICK_KEY: {"row": 0}}
    assert dv.remember_note_click(state, by_vor) == 2

    # Re-sorted the other way, with no new click: still Second.
    state.pop(dv.NOTE_CLICK_KEY)
    assert dv.remember_note_click(state, board.sort("vor")) == 2
    assert "Two." in dv.player_note_for(board, 2)


def test_clicking_the_open_players_mark_closes_it():
    """The only way to dismiss the panel without a second control."""
    board = dv.with_injury_code(_board([{"player_id": 7, "injury_note": "Something."}]))
    state = {dv.NOTE_CLICK_KEY: {"row": 0}}
    assert dv.remember_note_click(state, board) == 7

    state[dv.NOTE_CLICK_KEY] = {"row": 0}
    assert dv.remember_note_click(state, board) is None


def test_the_two_tables_remember_their_notes_separately():
    """Two widgets cannot share a key, and opening a note on the Values tab should not
    move the Board tab's."""
    board = dv.with_injury_code(_board([
        {"player_id": 1, "injury_note": "One."},
        {"player_id": 2, "injury_note": "Two."},
    ]))
    state = {dv.NOTE_CLICK_KEY: {"row": 0},
             dv.VALUES_NOTE_CLICK_KEY: {"row": 1}}
    assert dv.remember_note_click(state, board) == 1
    assert dv.remember_note_click(
        state, board, click_key=dv.VALUES_NOTE_CLICK_KEY) == 2


def test_a_note_is_remembered_per_league():
    """A note carried across a league change would point at a player the new board may
    not even hold — the reason the budget key is scoped the same way."""
    board = dv.with_injury_code(_board([{"player_id": 5, "injury_note": "Here."}]))
    state = {dv.NOTE_CLICK_KEY: {"row": 0}}
    assert dv.remember_note_click(state, board, league_key="one") == 5
    assert dv.remember_note_click(state, board, league_key="two") == 5
    state.pop(dv.NOTE_CLICK_KEY)
    assert dv.remember_note_click(state, board, league_key="three") is None


def test_no_click_leaves_the_panel_shut():
    board = dv.with_injury_code(_board([{"player_id": 1, "injury_note": "One."}]))
    assert dv.remember_note_click({}, board) is None


def test_a_stale_row_number_is_ignored():
    """A click can arrive against a table a filter has since shortened."""
    board = dv.with_injury_code(_board([{"player_id": 1, "injury_note": "One."}]))
    assert dv.remember_note_click({dv.NOTE_CLICK_KEY: {"row": 99}}, board) is None


def test_a_note_is_looked_up_on_the_whole_board_not_the_filtered_view():
    """So the panel does not blink out when a filter drops the row it was opened from."""
    board = dv.with_injury_code(_board([
        {"player_id": 1, "primaryPosition": "RB", "injury_note": "About the back."},
        {"player_id": 2, "primaryPosition": "WR", "injury_note": "About the wideout."},
    ]))
    assert "About the back" in dv.player_note_for(board, 1)
    assert dv.player_note_for(board.filter(pl.col("player_id") == 2), 1) is None


def test_the_news_mark_is_a_button_so_the_mark_itself_is_the_target():
    """Row selection was tried first: enabling it adds a checkbox column at the far
    left, so the thing you click is nowhere near the thing you are asking about."""
    spec = next(c for c in dv.COLUMNS if c.source == "note_mark")
    assert spec.kind == "button"
    assert spec.fmt is None


def test_the_note_is_indexed_against_the_sorted_frame_not_the_stored_board():
    """A selection's row number is a position on screen. Indexed against the board it
    would name whichever player happened to be stored there — silently, and wrongly."""
    board = dv.with_injury_code(_board([
        {"player_name": "First", "vor": 1.0, "injury_note": "About First."},
        {"player_name": "Second", "vor": 99.0, "injury_note": "About Second."},
    ]))
    table = board.sort("vor", descending=True)
    assert "About Second" in dv.player_note(table, 0)
    assert "About First" in dv.player_note(table, 1)


# --- the colour scale -----------------------------------------------------

def test_only_a_shaded_column_gets_thresholds():
    board = _board([{"vor": 10.0}, {"vor": 50.0}])
    shaded = {(c.group, c.label) for c in dv.COLUMNS if c.shade}
    assert set(dv.shade_scales(board)) <= shaded


def test_only_the_differences_are_shaded():
    """Colouring the raw points, ranks and prices too was built and removed: at
    seventeen shaded columns the table read as a heatmap and the columns carrying an
    opinion stopped being the ones that caught your eye."""
    board = _board([{"points_delta": 5.0, "TRUE_Points": 100.0},
                    {"points_delta": -5.0, "TRUE_Points": 300.0}])
    scales = dv.shade_scales(board)
    assert ("Points", "Δ") in scales
    assert ("Points", "Us") not in scales
    assert all(c.shade in ("", "delta") for c in dv.COLUMNS)


def test_the_delta_scale_is_per_column_because_the_units_are_not_shared():
    """Points differences run in tens, rank differences in hundreds. One absolute
    threshold would paint every rank cell and no points cell."""
    board = _board([{"points_delta": 5.0, "rank_delta": 400.0},
                    {"points_delta": 10.0, "rank_delta": 800.0}])
    scales = dv.shade_scales(board)
    assert scales[("Points", "Δ")].scale < scales[("Ranks", "Δ")].scale


def test_a_column_with_no_spread_is_not_painted():
    """A league where we and ESPN agree exactly is not a league of neutral-grey
    cells — it has nothing to say, so it says nothing."""
    board = _board([{"points_delta": 0.0}, {"points_delta": 0.0}])
    assert ("Points", "Δ") not in dv.shade_scales(board)


@pytest.mark.parametrize("value,step", [
    (100.0, 2), (30.0, 1), (5.0, 0), (0.0, 0),
    (-5.0, 0), (-30.0, -1), (-100.0, -2),
])
def test_a_difference_step_is_symmetric_around_zero(value, step):
    """Zero is the meaningful midpoint — it means we and ESPN agree — so the arms
    have to be mirror images or the same disagreement reads bigger one way."""
    assert dv.shade_step(value, dv.Shading(scale=100.0)) == step


def test_a_missing_value_gets_no_fill():
    shading = dv.Shading(scale=100.0)
    assert dv.shade_step(None, shading) == 0
    assert dv.shade_step(float("nan"), shading) == 0


def test_green_is_positive_and_red_is_negative_in_both_themes():
    """The one rule the whole scale rests on: positive means we are higher on the
    player than ESPN is, and positive is green everywhere."""
    for theme in ("light", "dark"):
        fills = dv.DELTA_FILLS[theme]
        assert fills[0] is None, theme
        for step in (1, 2, -1, -2):
            assert fills[step].startswith("#"), (theme, step)
        # The strong step is further from neutral than the soft one, per arm.
        assert fills[2] != fills[1] and fills[-2] != fills[-1]


def test_the_styler_emits_only_the_three_properties_streamlit_honours():
    """Streamlit's grid reads `color`, `background-color` and `font-weight` off a
    Styler and silently drops everything else — so anything else here is a rule that
    looks applied and is not."""
    board = _board([{"points_delta": 50.0}, {"points_delta": -50.0},
                    {"points_delta": 0.0}])
    frame = dv.display_frame(board)
    styled = dv.styled_frame(frame, dv.shade_scales(board), "light")

    rendered = styled._compute().ctx
    properties = {prop for styles in rendered.values() for prop, _ in styles}
    assert properties <= {"color", "background-color", "font-weight"}
    assert "font-weight" in properties


def test_a_neutral_difference_cell_is_still_bold_but_not_filled():
    """The emphasis says "this column is a judgement", which is true of the rows we
    agree with ESPN on too."""
    board = _board([{"points_delta": 0.0}, {"points_delta": 100.0}])
    frame = dv.display_frame(board)
    styled = dv.styled_frame(frame, dv.shade_scales(board), "light")

    ctx = styled._compute().ctx
    column = list(frame.columns).index(("Points", "Δ"))
    assert dict(ctx[(0, column)]) == {"font-weight": "700"}
    assert dict(ctx[(1, column)])["background-color"] == dv.DELTA_FILLS["light"][2]


def test_a_missing_text_value_renders_blank_rather_than_the_word_none():
    """Streamlit draws a missing cell as the literal word "None" unless something says
    otherwise, and for a *text* column the something is the Styler: it ships display
    strings beside the data, Streamlit prefers them where `column_config` has no format
    of its own, and pandas renders a missing value with `str` by default. On
    `Exp Return` that put "None" on 998 of 1,026 rows.

    A number column takes the other path — its display string is ignored in favour of
    `column_config`'s format — so its blank comes from `st.dataframe(placeholder="")`
    in the page. Both halves are load-bearing; this covers the Styler's."""
    board = dv.with_injury_code(_board([{"injury_return_date": None}]))
    frame = dv.display_frame(board)
    styled = dv.styled_frame(frame, dv.shade_scales(board), "light")

    cells = styled._translate(sparse_index=False, sparse_cols=False)["body"][0]
    values = [c["display_value"] for c in cells if c.get("type") == "td"]
    for key in (("Notes", "Exp Return"), ("Notes", "News")):
        assert values[list(frame.columns).index(key)] == "", key


def test_the_frame_keeps_its_nulls_rather_than_faking_a_blank():
    """The blank is a rendering concern and is fixed at render time. Filling the frame
    would make a free agent's keeper price a real `0`, or a "" that no longer sorts
    or aggregates — and `Keepers | Price` blank means nobody holds him."""
    board = _board([{"player_name": "Held", "on_team_id": 3, "keeper_value": 40},
                    {"player_name": "Free", "on_team_id": 0, "keeper_value": 0}])
    frame = dv.display_frame(
        dv.with_keeper_price(dv.at_budget(board, 250), 2), dv.VALUE_LENS_CASH)
    prices = frame[("Keepers", "Price")]
    assert prices.notna().iloc[0] and prices.isna().iloc[1]

    text = dv.display_frame(dv.with_injury_code(_board([{"injury_note": None}])))
    assert text[("Notes", "News")].isna().all()


def test_an_unknown_theme_falls_back_rather_than_raising():
    """A new Streamlit theme name should not take the page down."""
    board = _board([{"points_delta": 50.0}])
    frame = dv.display_frame(board)
    assert dv.styled_frame(frame, dv.shade_scales(board), "sepia") is not None


# --- the column config ----------------------------------------------------

def test_the_config_is_keyed_by_position_because_the_labels_repeat():
    """`ESPN`, `Us` and `Δ` each appear in several groups — that is what the spanners
    are for — so a name-keyed config would format the rank columns like points."""
    frame = dv.display_frame(_board([{}]))
    config = dv.column_config_specs(frame)

    offset = frame.index.nlevels
    assert set(config) == set(range(offset, frame.shape[1] + offset))
    for position, column in config.items():
        assert frame.columns[position - offset] == (column.group, column.label)


def test_the_positions_leave_room_for_the_hidden_index():
    """Streamlit numbers every column it was handed, index first, and `hide_index`
    hides the index without renumbering what follows. Off by one is not a crash —
    every column silently wears its neighbour's format, which is how `Tier` came to
    render as `1.0` and the fifth frozen column landed on the sixth."""
    frame = dv.display_frame(_board([{}]))
    config = dv.column_config_specs(frame)
    assert min(config) == frame.index.nlevels == 1
    assert config[1].label == "Player"


def test_every_rendered_column_has_a_config_entry():
    """A column with no entry renders unformatted, which for a difference column
    means losing the sign the colour scale depends on."""
    for lens in (dv.VALUE_LENS_ADP, dv.VALUE_LENS_CASH):
        frame = dv.display_frame(_board([{}]), lens)
        assert len(dv.column_config_specs(frame, lens)) == frame.shape[1]


# --- display -------------------------------------------------------------

def test_display_frame_renames_and_orders_columns():
    frame = dv.display_frame(_board([{}]))
    assert list(frame.columns[:4]) == [("Player Info", "Player"),
                                       ("Player Info", "Pos"),
                                       ("Player Info", "NFL"),
                                       ("Player Info", "Bye")]


def test_display_frame_carries_two_header_levels():
    """The spanners *are* the second level — Streamlit reads the last level as the
    header and the ones above it as the group, so a flat frame silently loses them."""
    frame = dv.display_frame(_board([{}]))
    assert frame.columns.nlevels == 2
    assert set(g for g, _ in frame.columns) <= set(dv.GROUPS)


def test_display_frame_skips_columns_the_artifact_lacks():
    """What lets one spec list serve a redraft league, a board built before the usage
    model, and an artifact written before the ESPN comparison columns existed."""
    board = _board([{}]).drop(["espn_draft_rank", "ESPN_Points"])
    frame = dv.display_frame(board)
    assert ("Ranks", "ESPN") not in frame.columns
    assert ("Points", "ESPN") not in frame.columns
    assert ("Player Info", "Player") in frame.columns


def test_the_draft_metric_group_switches_on_the_lens_not_on_presence():
    """Both source columns exist in every league, so this group cannot be selected by
    looking at the data — which currency it speaks is a fact about the draft."""
    board = dv.with_cash_value(
        dv.at_budget(_board([{"auction_value_filled": 20.0}]), 250),
        {"team_count": 10, "roster_slots": {"RB": 2}}, 250)

    adp = dv.display_frame(board, dv.VALUE_LENS_ADP)
    cash = dv.display_frame(board, dv.VALUE_LENS_CASH)
    metric = lambda f: [l for g, l in f.columns if g == "Draft Metric"]  # noqa: E731

    assert metric(adp) == ["ESPN", "Us", "Δ"]
    assert metric(cash) == ["ESPN", "Us", "Δ"]
    # Same headers, different numbers underneath: ADP reads a pick, Cash a dollar.
    assert adp[("Draft Metric", "ESPN")].to_list() == [20.0]        # `adp`
    assert cash[("Draft Metric", "ESPN")].to_list() == [25.0]       # rescaled dollars


def test_one_source_can_sit_under_two_headers():
    """`vor_rank` is both our overall rank and our implied pick order, and each
    comparison reads better with it than with a cross-reference. Polars will not
    select a column twice under one name, so this is the alias path working."""
    frame = dv.display_frame(_board([{"vor_rank": 7.0}]), dv.VALUE_LENS_ADP)
    assert frame[("Ranks", "Us")].to_list() == [7.0]
    assert frame[("Draft Metric", "Us")].to_list() == [7.0]


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
    for key in (("Points", "USG"), ("Position Ranks", "Δ USG"),
                ("Notes", "Exp G"), ("Notes", "Model Evidence")):
        assert key not in frame.columns


def test_the_model_sits_with_the_quantity_it_is_an_opinion_about():
    """`USG` beside the points it is a projection of, and its rank dissent beside the
    positional ranks it dissents from — which is the arrangement that makes the level
    mismatch visible instead of inviting the subtraction."""
    frame = dv.display_frame(
        dv.at_budget(dv.with_model_evidence(_modelled([{}])),
                     dv.DEFAULT_AUCTION_BUDGET),
        dv.VALUE_LENS_CASH)
    order = list(frame.columns)
    assert order.index(("Points", "Us")) < order.index(("Points", "USG"))
    assert order.index(("Position Ranks", "Δ")) < order.index(("Position Ranks", "Δ USG"))
    assert order.index(("Notes", "Exp G")) < order.index(("Notes", "Model Evidence"))


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


# --- calibration: us against ESPN ----------------------------------------

def _pool(position, deltas, base=100.0, name=None, **extra):
    """`len(deltas)` players at one position, each `delta` points off ESPN.

    ``name`` overrides the player-name prefix, which two pools at the same position
    need: names collide otherwise, and a test that looks a player up by name then
    silently reads whichever of the two the sort happened to put first.
    """
    prefix = name or position
    return [{"player_name": f"{prefix}{i}", "primaryPosition": position,
             "ESPN_Points": base, "TRUE_Points": base + delta, **extra}
            for i, delta in enumerate(deltas)]


def test_a_player_espn_has_no_projection_for_is_not_a_disagreement():
    board = _board([{"player_name": "Priced", "ESPN_Points": 90.0},
                    {"player_name": "Unpriced", "ESPN_Points": None}])
    frame = dv.agreement_frame(board)
    assert frame["player_name"].to_list() == ["Priced"]


def test_the_gap_is_recomputed_from_the_two_columns_actually_plotted():
    """Not read from the board's stored `points_delta`, so a chart drawing all
    three numbers cannot disagree with itself about what the third one is."""
    board = _board([{"ESPN_Points": 100.0, "TRUE_Points": 130.0,
                     "points_delta": -999.0}])
    frame = dv.agreement_frame(board)
    assert frame["points_delta"][0] == 30.0
    assert frame["agreement_mean"][0] == 115.0


def test_the_score_is_measured_within_position_not_across_the_pool():
    """A 30-point gap is unremarkable for a quarterback and an outlier for a back,
    and one pooled scale reports the quarterbacks as the model's whole problem.

    Both positions get the same spread and different centres, so the only thing
    separating the two players on 30 is which position they are measured against."""
    # Eight apiece, which is dv.AGREEMENT_MIN_PLAYERS; the last of each is the
    # player on +30. The quarterbacks centre on 30, the backs on ~4.
    board = _board(
        _pool("QB", [25.0, 27.0, 29.0, 31.0, 33.0, 35.0, 29.0, 30.0])
        + _pool("RB", [-5.0, -3.0, -1.0, 1.0, 3.0, 5.0, -1.0, 30.0]))
    frame = dv.agreement_frame(board)
    scores = dict(zip(frame["player_name"], frame["agreement_z"]))
    assert abs(scores["QB7"]) < 0.5
    assert scores["RB7"] > 2.0


def test_a_position_that_never_disagrees_is_scored_as_having_no_outliers():
    """Kickers, and the reason the floor exists at all: no source but ESPN projects
    one, so every delta is float dust around 1e-14 with a standard deviation of
    7e-15. Dividing by it ranked two kickers as the board's biggest disagreements
    on a delta of -0.00000000000003."""
    board = _board(_pool("K", [0.0, -2.8e-14] * (dv.AGREEMENT_MIN_PLAYERS // 2),
                         **{"startable": True}))
    frame = dv.agreement_frame(board)
    assert frame["agreement_z"].is_null().all()
    assert dv.agreement_outliers(frame).is_empty()


def test_a_position_with_too_few_players_left_carries_no_score():
    """Its spread would describe the survivors of a filter rather than the
    position."""
    board = _board(_pool("TE", [1.0, 40.0, -20.0]))
    assert dv.agreement_frame(board)["agreement_z"].is_null().all()


def test_the_score_moves_with_the_filter_because_the_scoping_is_the_analysis():
    """Narrowed to the players the market prices, the mean gap at WR moves from
    +8.8 to -3.4 on the real board -- so scoring against the whole pool would mark
    every priced receiver an outlier for belonging to the priced half."""
    deep = _pool("WR", [40.0] * dv.AGREEMENT_MIN_PLAYERS, name="Deep",
                 adp_is_priced=False)
    priced = _pool("WR", [0.0] * (dv.AGREEMENT_MIN_PLAYERS - 1) + [12.0],
                   name="Priced", adp_is_priced=True)
    board = _board(deep + priced)

    pooled = dv.agreement_frame(board)
    narrowed = dv.agreement_frame(board.filter(pl.col("adp_is_priced")))

    target = f"Priced{dv.AGREEMENT_MIN_PLAYERS - 1}"
    pooled_z = dict(zip(pooled["player_name"], pooled["agreement_z"]))
    narrow_z = dict(zip(narrowed["player_name"], narrowed["agreement_z"]))
    # Same player, same two projections, different question.
    assert pooled_z[target] < 0
    assert narrow_z[target] > 2.0


def test_outliers_come_back_biggest_first_from_both_tails():
    board = _board(_pool("RB", [0.0] * (dv.AGREEMENT_MIN_PLAYERS - 2)
                         + [60.0, -60.0]))
    frame = dv.agreement_frame(board)
    top = dv.agreement_outliers(frame, limit=2)
    assert top.height == 2
    assert sorted(top["points_delta"].to_list()) == [-60.0, 60.0]


def test_per_position_outliers_do_not_let_one_position_take_every_slot():
    """A pooled top ten lands eight labels in the WR facet and none in the rest."""
    board = _board(_pool("WR", [0.0] * (dv.AGREEMENT_MIN_PLAYERS - 3)
                         + [50.0, 45.0, 40.0])
                   + _pool("TE", [0.0] * (dv.AGREEMENT_MIN_PLAYERS - 1) + [30.0]))
    frame = dv.agreement_frame(board)
    picked = dv.agreement_outliers(frame, limit=2, per_position=True)
    counts = picked["primaryPosition"].value_counts().to_dict(as_series=False)
    assert dict(zip(counts["primaryPosition"], counts["count"])) == {"WR": 2,
                                                                    "TE": 2}


def test_the_table_and_the_chart_marks_cannot_pick_different_players():
    board = _board(_pool("RB", [0.0] * (dv.AGREEMENT_MIN_PLAYERS - 2)
                         + [55.0, -48.0]))
    frame = dv.agreement_frame(board)
    flagged = dv.with_outlier_flag(frame, limit=2)
    listed = dv.agreement_outliers(frame, limit=2)
    assert (set(flagged.filter(pl.col(dv.AGREEMENT_FLAG))["player_name"])
            == set(listed["player_name"]))


def test_an_unscored_player_is_never_flagged():
    board = _board(_pool("K", [0.0] * dv.AGREEMENT_MIN_PLAYERS))
    flagged = dv.with_outlier_flag(dv.agreement_frame(board), limit=3)
    assert not flagged[dv.AGREEMENT_FLAG].any()
    assert flagged[dv.AGREEMENT_RANK].is_null().all()


def test_label_slots_run_bottom_to_top_so_the_marks_own_spread_helps():
    """By |z| rank the offsets ran *against* the marks' spread -- the lowest mark's
    label was pushed up and the highest one's down, which collapsed three tight
    ends onto one line."""
    board = _board(_pool("TE", [0.0] * (dv.AGREEMENT_MIN_PLAYERS - 3)
                         + [40.0, -35.0, 30.0]))
    frame = dv.agreement_frame(board)
    slotted = dv.with_label_slots(
        dv.with_outlier_flag(frame, limit=3, per_position=True), "TRUE_Points")

    marks = slotted.filter(pl.col(dv.AGREEMENT_FLAG)).sort(dv.AGREEMENT_SLOT)
    assert marks[dv.AGREEMENT_SLOT].to_list() == [1, 2, 3]
    # Slot order is the vertical order of the marks themselves.
    assert marks["TRUE_Points"].to_list() == sorted(marks["TRUE_Points"].to_list())


def test_label_slots_follow_the_axis_the_view_actually_uses():
    """The two views put different columns on y, which is why the slot cannot be
    decided when the flag is."""
    board = _board(_pool("RB", [0.0] * (dv.AGREEMENT_MIN_PLAYERS - 2) + [50.0],
                         base=10.0, name="Small")
                   + _pool("RB", [-40.0], base=300.0, name="Big"))
    frame = dv.agreement_frame(board)
    flagged = dv.with_outlier_flag(frame, limit=2, per_position=True)

    by_points = dv.with_label_slots(flagged, "TRUE_Points")
    by_delta = dv.with_label_slots(flagged, "points_delta")

    def slot_of(frame_, name):
        row = frame_.filter(pl.col("player_name") == name)
        assert row.height == 1
        return row[dv.AGREEMENT_SLOT][0]

    # "Big0" sits at 260 points and −40 off ESPN: the highest of the two flagged
    # marks on the points axis and the lowest on the disagreement axis, so his slot
    # has to flip with the view or the labels stagger the wrong way in one of them.
    assert slot_of(by_points, "Big0") == 2
    assert slot_of(by_delta, "Big0") == 1


def test_unflagged_players_get_no_label_slot():
    board = _board(_pool("WR", [0.0] * (dv.AGREEMENT_MIN_PLAYERS - 1) + [50.0]))
    slotted = dv.with_label_slots(
        dv.with_outlier_flag(dv.agreement_frame(board), limit=1), "TRUE_Points")
    assert slotted.filter(~pl.col(dv.AGREEMENT_FLAG))[dv.AGREEMENT_SLOT] \
        .is_null().all()


def test_the_summary_separates_a_systematic_offset_from_a_player_argument():
    board = _board(_pool("QB", [25.0] * dv.AGREEMENT_MIN_PLAYERS)
                   + _pool("RB", [-40.0, 40.0] * (dv.AGREEMENT_MIN_PLAYERS // 2)))
    summary = dv.agreement_summary(dv.agreement_frame(board))
    rows = {row[0]: row for row in summary.iter_rows()}
    qb, rb = rows["QB"], rows["RB"]
    # A large mean with no spread is the blend offsetting the whole position.
    assert qb[2] == pytest.approx(25.0) and qb[3] == pytest.approx(0.0)
    # A zero mean with a wide spread is an argument about individuals.
    assert rb[2] == pytest.approx(0.0) and rb[3] > 30.0
    assert qb[4] == pytest.approx(100.0)


def test_a_position_the_palette_has_no_hue_for_is_still_scored():
    """GOP Degenerates starts cornerbacks and POSITION_HUES stops at eight slots.
    The chart draws them in muted ink; what must not happen is the tables scoring
    and ranking a position the chart then has no panel for."""
    board = _board(_pool("CB", [0.0] * (dv.AGREEMENT_MIN_PLAYERS - 1) + [30.0]))
    frame = dv.agreement_frame(board)
    assert "CB" not in dv.POSITION_HUES
    assert frame.height == dv.AGREEMENT_MIN_PLAYERS
    assert dv.agreement_outliers(frame, 1)["primaryPosition"].to_list() == ["CB"]
    assert dv.agreement_summary(frame)["primaryPosition"].to_list() == ["CB"]


def test_the_summary_says_which_positions_carry_no_score():
    board = _board(_pool("K", [0.0] * dv.AGREEMENT_MIN_PLAYERS)
                   + _pool("RB", [0.0, 20.0] * (dv.AGREEMENT_MIN_PLAYERS // 2)))
    summary = dv.agreement_summary(dv.agreement_frame(board))
    scored = dict(zip(summary["primaryPosition"], summary["scored"]))
    assert scored == {"K": False, "RB": True}


def test_an_empty_selection_still_answers_with_the_right_columns():
    """A filter that empties the tab must not raise -- see the K-only case."""
    empty = dv.agreement_frame(_board([]).head(0))
    assert set(dv.AGREEMENT_SCHEMA) <= set(empty.columns)
    assert dv.agreement_summary(empty).is_empty()
    assert dv.agreement_outliers(empty).is_empty()
    assert not dv.with_outlier_flag(empty)[dv.AGREEMENT_FLAG].any()


def test_a_board_without_espn_points_at_all_is_not_an_error():
    plain = pl.DataFrame({"primaryPosition": ["RB"], "TRUE_Points": [10.0]})
    assert dv.agreement_frame(plain).is_empty()
    assert list(dv.agreement_frame(plain).columns) == list(dv.AGREEMENT_SCHEMA)


def test_the_outlier_table_speaks_the_boards_own_vocabulary():
    """`ESPN`, `Us` and Δ mean here what the `Points` spanner means by them, so a
    reader arriving from the Board tab does not learn the columns twice."""
    board = _board(_pool("RB", [0.0] * (dv.AGREEMENT_MIN_PLAYERS - 1) + [40.0]))
    table = dv.agreement_table(dv.agreement_outliers(dv.agreement_frame(board), 1))
    assert table.columns[:6] == ["Player", "Pos", "NFL", "ESPN", "Us", "Δ"]
    assert table.height == 1


def test_the_table_skips_columns_the_frame_does_not_carry():
    frame = pl.DataFrame({"player_name": ["A"], "primaryPosition": ["RB"],
                          "ESPN_Points": [1.0], "TRUE_Points": [2.0]})
    assert dv.agreement_table(frame).columns == ["Player", "Pos", "ESPN", "Us"]


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


def test_the_role_withdrawal_marker_matches_its_producer():
    """`draft_view` duplicates the string rather than importing the board builder,
    which would drag the ESPN and scoring stack into a process that only reads
    parquet. Duplication is fine; silent divergence is not."""
    from Scripts.season_projections import ROLE_WITHDRAWN_EVIDENCE

    assert dv.EVIDENCE_ROLE_MARKER == ROLE_WITHDRAWN_EVIDENCE


def test_a_backup_withdrawal_does_not_read_as_an_injury():
    """Both null `USG_Points`, and they mean different things to a drafter: an
    injured starter comes back, a backup needs someone ahead of him to get hurt."""
    board = pl.DataFrame({
        "usg_arm": ["veteran", "veteran"],
        "usg_evidence": [dv.EVIDENCE_ROLE_MARKER, ""],
        "USG_Points": [None, None],
    })
    labels = dv.with_model_evidence(board)["usg_evidence_label"].to_list()
    assert labels == [dv.EVIDENCE_WITHDRAWN_ROLE, dv.EVIDENCE_WITHDRAWN_INJURY]
