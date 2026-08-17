"""The draft board page's derivations: filters, roster needs, chart data.

The page script itself is layout and needs a Streamlit runtime; everything with a
decision in it lives in ``app/draft_view.py`` and is covered here. No network, no
store on disk -- the boards are synthesised to the shape ``build_board`` returns.
"""

import re
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


# --- the glossary ---------------------------------------------------------

def test_every_column_the_table_shows_is_in_the_glossary():
    """A column added to the table and not the glossary is a silent gap — the
    reader gets a number with no account of where it came from."""
    shown = {label for _, label in dv.DISPLAY_COLUMNS}
    assert shown - set(dv.COLUMN_GLOSSARY) == set()


def test_the_glossary_describes_no_column_that_does_not_exist():
    """The other direction, which nobody would ever notice: an entry for a column
    that has been removed is documentation of something that is not there."""
    shown = {label for _, label in dv.DISPLAY_COLUMNS}
    assert set(dv.COLUMN_GLOSSARY) - shown == set()


def test_the_glossary_is_scoped_to_the_columns_actually_rendered():
    """A redraft league must not be told about keeper prices it does not have."""
    md = dv.glossary_markdown(["Player", "VOR"])
    assert "**Player**" in md and "**VOR**" in md
    assert "Keeper" not in md and "USG" not in md


def test_the_glossary_keeps_the_tables_column_order():
    md = dv.glossary_markdown(["VOR", "Player", "ADP"])
    assert md.index("**Player**") < md.index("**VOR**") < md.index("**ADP**")


def test_dollar_amounts_are_escaped_so_streamlit_does_not_read_them_as_maths():
    """Paired `$` is LaTeX to Streamlit, and a glossary about auction values is
    full of them — unescaped, the middle of the table renders as equations."""
    md = dv.glossary_markdown(["$", "Our $", "Keeper $"])
    assert "$" in md and r"\$" in md
    assert not re.search(r"(?<!\\)\$", md)


def test_the_glossary_renders_a_markdown_table():
    md = dv.glossary_markdown(["Player"])
    lines = md.split("\n")
    assert lines[0].startswith("| Column | Source |")
    assert lines[1] == "|---|---|---|"
    assert lines[2].count("|") == 4


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


def test_the_surplus_is_the_market_price_less_what_keeping_costs():
    """Both sides in the same currency: the keeper price is already in league
    dollars, the market value is not until at_budget has run."""
    board = dv.at_budget(_board([{"auction_value_filled": 80.0, "keeper_value": 40,
                                  "on_team_id": 7}]),
                         250)                       # 80/200 * 250 = $100 market
    out = dv.with_keeper_price(board, 2)
    assert out["keeper_surplus"].to_list() == [60.0]


def test_the_surplus_is_blank_where_the_market_has_no_price():
    board = dv.at_budget(_board([{"keeper_value": 40}]).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("auction_value_filled")), 250)
    out = dv.with_keeper_price(board, 2)
    assert out["keeper_surplus"].to_list() == [None]


def test_the_keeper_columns_sit_with_the_market_block():
    frame = dv.display_frame(dv.with_keeper_price(
        dv.at_budget(_board([{"keeper_value": 40}]), 250), 2))
    order = frame.columns
    assert order.index("$") < order.index("Keeper $") < order.index("Keeper +/-")
    assert order.index("Keeper +/-") < order.index("Injury")


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
                                          400))
    assert frame["$"].to_list() == [40.0]


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
    for label in ("USG", "Δ Rk", "Exp G", "Model Evidence"):
        assert label not in frame.columns


def test_the_model_block_sits_between_the_market_and_the_status_columns():
    # Through at_budget, because `$` is the budget-rescaled column now and the page
    # renders nothing that has not been through it.
    frame = dv.display_frame(
        dv.at_budget(dv.with_model_evidence(_modelled([{}])),
                     dv.DEFAULT_AUCTION_BUDGET))
    order = frame.columns
    assert order.index("$") < order.index("USG")
    assert order.index("Model Evidence") < order.index("Injury")


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
