"""The Sheet: dollar allocation, positional scarcity, the availability lens, panels.

Covers ``app/sheet_view.py`` and the three additions to ``app/draft_view.py`` that
plan 37 needed. The page script is layout and needs a Streamlit runtime; everything
with a decision in it is here. No network, no store on disk -- boards are synthesised
to the shape ``build_board`` returns.

The draft-state machine is deliberately testable with a plain dict rather than
``st.session_state``, which is why ``sheet_view`` takes a ``Mapping`` and imports no
Streamlit.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

# `app/` is not a package: Streamlit runs the page scripts directly with app/ on
# sys.path, so the modules import each other by bare name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import draft_view as dv  # noqa: E402
import sheet_view as sv  # noqa: E402


# --- fixtures ------------------------------------------------------------

def _board(rows):
    """A board frame with the columns The Sheet reads, defaults filled in."""
    defaults = {
        "player_id": 0, "player_name": "Player", "primaryPosition": "RB",
        "pro_team": "DET", "bye_week": 6.0, "on_team_id": 0, "startable": True,
        "projection_missing": False, "is_streamed": False,
        "TRUE_Points": 100.0, "vor": 10.0, "value": 5.0, "adp": 20.0,
        "tier": 1.0, "pos_rank": 1.0, "replacement_rank": 30,
        "auction_value_filled": 20.0, "usg_expected_games": 17.0,
    }
    out = []
    for index, row in enumerate(rows):
        merged = {**defaults, **row}
        # A row that names no id gets its position, so every fixture has unique ids
        # without every fixture having to say so.
        if "player_id" not in row:
            merged["player_id"] = index
        out.append(merged)
    return pl.DataFrame(out)


def _ladder(position="RB", n=10, streamed=False, start=100.0, step=None,
            first_id=0):
    """``n`` players at one position with strictly descending, positive VOR.

    ``step`` defaults to keeping the whole ladder above replacement, because a
    negative weight is the caller's job to mask -- ``_cash_eligible`` does it -- and a
    fixture that quietly produced them would be testing the mask rather than the
    arithmetic.

    ``first_id`` exists because ids must be unique *across* positions: two ladders
    both numbered from zero made "cross off RB2" also cross off QB2, which is the
    exact bug :func:`draft_view._drafted_expr` keys on id to avoid.
    """
    step = start / (n + 1) if step is None else step
    return [{"player_id": first_id + i, "player_name": f"{position}{i + 1}",
             "primaryPosition": position, "is_streamed": streamed,
             "vor": start - i * step, "TRUE_Points": start - i * step,
             "tier": float(i // 3 + 1)}
            for i in range(n)]


#: Ten teams, twelve draftable slots each -- 120 spots, $2,000 at $200.
AUCTION_META = {"team_count": 10,
                "roster_slots": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "BE": 6,
                                 "IR": 2},
                "draft_settings": {"type": "AUCTION", "auction_budget": 200}}

SNAKE_META = {**AUCTION_META, "draft_settings": {"type": "SNAKE"}}


# =========================================================================
# allocate_dollars
# =========================================================================

def test_the_pool_is_allocated_the_whole_budget_less_the_floors_it_cannot_fill():
    """Ten teams at $200 is $2,000. Every spot costs at least $1, so the pool takes
    the discretionary remainder plus its own floors -- and any spot the pool was too
    small to fill still costs its dollar, which is a real shortfall rather than a
    rounding error."""
    board = _board(_ladder(n=50))
    out = dv.allocate_dollars(board, pl.col("vor"), spots=120, teams=10,
                              budget=200, out="priced")
    total = out["priced"].sum()
    assert abs(total - (10 * 200 - (120 - 50) * dv.MIN_BID)) < 1e-6


def test_only_as_many_players_as_there_are_spots_get_a_price():
    """Money the room does not have cannot be allocated: the 121st-best player in a
    120-spot league is not worth a dollar of somebody's budget."""
    board = _board(_ladder(n=150, start=200.0, step=1.0))
    out = dv.allocate_dollars(board, pl.col("vor"), spots=120, teams=10,
                              budget=200, out="priced")
    assert out.filter(pl.col("priced").is_not_null()).height == 120
    assert abs(out["priced"].sum() - 10 * 200) < 1e-6


def test_price_outside_pool_extends_the_rate_without_diluting_it():
    """The market side wants this: ESPN prices more players than the room can roster,
    and what it pays for the 121st is worth showing. The rate is still set by the
    120 who can absorb the money, so the pool's own total does not move."""
    board = _board(_ladder(n=150, start=200.0, step=1.0))
    tight = dv.allocate_dollars(board, pl.col("vor"), spots=120, teams=10,
                                budget=200, out="priced")
    wide = dv.allocate_dollars(board, pl.col("vor"), spots=120, teams=10,
                               budget=200, out="priced", price_outside_pool=True)
    assert wide.filter(pl.col("priced").is_not_null()).height == 150
    # Same rate: the players inside the pool are priced identically either way.
    assert tight["priced"].head(120).to_list() == pytest.approx(
        wide["priced"].head(120).to_list())


def test_the_pool_sets_the_rate_and_the_weight_decides_who_is_paid():
    """Streamed positions are priced but must not set the rate -- a season-total VOR
    does not describe a position you stream."""
    rows = _ladder(n=5) + _ladder("D/ST", n=3, streamed=True, start=90.0,
                                  first_id=100)
    board = _board(rows)
    out = dv.allocate_dollars(
        board, pl.col("vor"), spots=8, teams=2, budget=50, out="priced",
        pool=~pl.col("is_streamed"), price_outside_pool=True)
    offence = out.filter(~pl.col("is_streamed"))
    # The offence alone takes the whole pool, even though defences got a number.
    assert abs(offence["priced"].sum() - (2 * 50 - (8 - 5) * dv.MIN_BID)) < 1e-6
    assert out.filter(pl.col("is_streamed"))["priced"].is_not_null().all()


@pytest.mark.parametrize("spots,teams", [(0, 10), (120, 0)])
def test_allocation_is_impossible_without_both_a_roster_shape_and_a_team_count(
        spots, teams):
    """None rather than an unchanged frame, so each caller degrades its own way --
    they do it differently."""
    assert dv.allocate_dollars(_board(_ladder()), pl.col("vor"), spots=spots,
                               teams=teams, budget=200, out="priced") is None


def test_a_board_where_nobody_clears_replacement_cannot_be_priced():
    board = _board([{"player_id": 0, "vor": -5.0}, {"player_id": 1, "vor": 0.0}])
    weight = pl.when(pl.col("vor") > 0).then(pl.col("vor")).otherwise(None)
    assert dv.allocate_dollars(board, weight, spots=10, teams=2, budget=50,
                               out="priced") is None


# =========================================================================
# at_budget: both sides of the cash lens on the same pool
# =========================================================================

def test_both_sides_of_the_cash_lens_land_on_the_same_pool():
    """**The bug this fixes.** `at_budget` scaled ESPN's $200 values by the budget
    ratio and never saw team count, so a 16-team $250 auction read $2,544 against
    $4,000 of real money -- 1.57x light -- while a six-team league ran heavy. Every
    `cash_delta` was differencing our correctly-pooled dollars against a mis-scaled
    market, and the sign of the error changed with league size."""
    board = _board(_ladder(n=40))
    out = dv.with_cash_value(
        dv.at_budget(board, 200, meta=AUCTION_META), AUCTION_META, 200)

    pool = out.filter(dv._cash_eligible(out))
    assert abs(pool["our_dollars"].sum() - pool["auction_dollars"].sum()) < 1e-6
    expected = 10 * 200 - (120 - 40) * dv.MIN_BID
    assert abs(pool["our_dollars"].sum() - expected) < 1e-6


def test_the_market_dollars_move_with_team_count_now():
    """The same board and the same per-team budget, in a six-team league and a
    sixteen-team one, must not put the same money on the table."""
    board = _board(_ladder(n=40))
    small = dict(AUCTION_META, team_count=6)
    large = dict(AUCTION_META, team_count=16)
    thin = dv.at_budget(board, 200, meta=small)["auction_dollars"].sum()
    fat = dv.at_budget(board, 200, meta=large)["auction_dollars"].sum()
    assert fat > thin
    assert abs(fat / thin - 16 / 6) < 0.1


def test_passing_meta_does_not_warn_and_omitting_it_does():
    board = _board(_ladder(n=40))
    with pytest.warns(dv.DraftViewWarning, match="team-count"):
        dv.at_budget(board, 250)
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        dv.at_budget(board, 250, meta=AUCTION_META)


def test_a_board_espn_never_priced_does_not_get_blamed_on_missing_meta():
    """`meta` was fine; there was simply no market. Warning "no meta" there would be
    a lie, and a lie in the one place the reader goes to find out what degraded."""
    board = _board(_ladder(n=5)).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("auction_value_filled"))
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        out = dv.at_budget(board, 200, meta=AUCTION_META)
    assert out["auction_dollars"].is_null().all()


def test_our_own_dollars_are_unchanged_by_the_refactor():
    """`with_cash_value` now calls the shared helper. Its numbers must not have moved
    -- the market side was the side that was wrong."""
    board = _board(_ladder(n=40))
    out = dv.with_cash_value(board, AUCTION_META, 200)
    spots, teams = dv.draftable_spots(AUCTION_META), 10
    discretionary = teams * 200 - spots * dv.MIN_BID
    total = sum(row["vor"] for row in _ladder(n=40))
    best = out.sort("vor", descending=True)["our_dollars"][0]
    assert abs(best - (dv.MIN_BID + discretionary * 100.0 / total)) < 1e-6


# =========================================================================
# positional_scarcity
# =========================================================================

def test_the_best_player_has_everything_but_himself_behind_him():
    out = dv.positional_scarcity(_board(_ladder(n=10)))
    ranked = out.sort("vor", descending=True)
    total = sum(row["vor"] for row in _ladder(n=10))
    assert ranked["ps"][0] == pytest.approx((total - 100.0) / total)


def test_the_last_player_above_replacement_has_nothing_behind_him():
    out = dv.positional_scarcity(_board(_ladder(n=10)))
    assert out.sort("vor", descending=True)["ps"][-1] == pytest.approx(0.0)


def test_scarcity_decays_as_the_position_is_drafted():
    """The whole point of the column. A fixed denominator is what makes it fall --
    normalising by the value *remaining* would read better and say nothing."""
    board = _board(_ladder(n=10))
    before = dv.positional_scarcity(board).sort("vor", descending=True)["ps"][0]
    after = dv.positional_scarcity(
        board, drafted=[1, 2, 3, 4]).sort("vor", descending=True)["ps"][0]
    assert after < before


def test_crossing_a_player_off_moves_only_the_players_above_him():
    """Scarcity is what is *below* you, so taking the fifth-best cannot change the
    answer for the sixth."""
    board = _board(_ladder(n=10))
    before = dv.positional_scarcity(board).sort("vor", descending=True)
    after = dv.positional_scarcity(board, drafted=[4]).sort("vor", descending=True)
    assert after["ps"][0] < before["ps"][0]           # above him: less left behind
    assert after["ps"][5] == pytest.approx(before["ps"][5])   # below him: untouched


def test_one_positions_draft_does_not_touch_another():
    board = _board(_ladder("RB", n=6) + _ladder("QB", n=6, first_id=100))
    before = dv.positional_scarcity(board)
    after = dv.positional_scarcity(board, drafted=[1, 2])
    qb_before = before.filter(pl.col("primaryPosition") == "QB").sort(
        "vor", descending=True)["ps"].to_list()
    qb_after = after.filter(pl.col("primaryPosition") == "QB").sort(
        "vor", descending=True)["ps"].to_list()
    assert qb_before == pytest.approx(qb_after)


def test_streamed_positions_get_no_scarcity_share():
    """A season-total VOR does not describe a position you stream, so a share built
    on one would be worse than a blank."""
    board = _board(_ladder("RB", n=4)
                   + _ladder("D/ST", n=4, streamed=True, first_id=100))
    out = dv.positional_scarcity(board)
    assert out.filter(pl.col("is_streamed"))["ps"].is_null().all()
    assert out.filter(~pl.col("is_streamed"))["ps"].is_not_null().all()


def test_scarcity_preserves_row_order():
    """It sorts internally to make `cum_sum` mean "below him"; the caller's order is
    what the page renders from and must survive."""
    board = _board(_ladder(n=8))
    out = dv.positional_scarcity(board)
    assert out["player_name"].to_list() == board["player_name"].to_list()


def test_a_board_with_no_vor_still_gets_the_column():
    board = _board(_ladder(n=4)).drop("vor")
    assert dv.positional_scarcity(board)["ps"].is_null().all()


def test_crossing_off_is_keyed_on_id_because_two_players_share_a_name():
    """`Scripts.draft.board`'s postscript found 16 colliding names in the IDP pool --
    Lamar Jackson the quarterback alongside Lamar Jackson the cornerback. Crossing
    off one must not take the other."""
    board = _board([
        {"player_id": 1, "player_name": "Lamar Jackson", "vor": 100.0},
        {"player_id": 2, "player_name": "Lamar Jackson", "vor": 50.0},
        {"player_id": 3, "player_name": "Somebody Else", "vor": 25.0},
    ])
    gone = board.with_columns(dv._drafted_expr(board, [1]).alias("gone"))
    assert gone["gone"].to_list() == [True, False, False]


def test_nothing_drafted_is_not_everything_drafted():
    board = _board(_ladder(n=3))
    assert board.with_columns(
        dv._drafted_expr(board, []).alias("gone"))["gone"].to_list() == [False] * 3


# =========================================================================
# the availability lens
# =========================================================================

def test_the_full_slate_is_pinned_to_the_model_that_defines_it():
    """Duplicated rather than imported -- the app must not pull the usage stack in to
    learn one float -- so a test holds the two equal."""
    from Scripts.usage.season import DEFAULT_TARGET_SLATE
    assert dv.FULL_SLATE == DEFAULT_TARGET_SLATE


def test_the_discount_is_the_share_of_the_season_he_is_expected_to_play():
    board = _board([{"TRUE_Points": 340.0, "usg_expected_games": 13.6}])
    out = dv.with_availability_points(board)
    assert out["avail_points"][0] == pytest.approx(340.0 * 13.6 / 17)


def test_a_player_with_no_estimate_is_not_discounted_and_says_so():
    """Roughly one priced-and-projected player in seven has no estimate. Sinking them
    to the bottom of the sort would be a filter disguised as a projection; silently
    passing them through would read as durability."""
    board = _board([{"player_id": 0, "TRUE_Points": 200.0,
                     "usg_expected_games": None},
                    {"player_id": 1, "TRUE_Points": 200.0,
                     "usg_expected_games": 8.5}])
    out = dv.with_availability_points(board)
    assert out["avail_points"][0] == pytest.approx(200.0)
    assert out["avail_evidence"][0] == dv.AVAIL_NO_ESTIMATE
    assert out["avail_points"][1] == pytest.approx(100.0)
    assert out["avail_evidence"][1] == ""


def test_the_discount_reorders_within_position():
    """It is not a monotone rescale, which is the reason it is worth a toggle at all:
    a durable second-best player passes a fragile best one."""
    board = _board([
        {"player_id": 0, "player_name": "Fragile", "TRUE_Points": 300.0,
         "usg_expected_games": 11.0},
        {"player_id": 1, "player_name": "Durable", "TRUE_Points": 280.0,
         "usg_expected_games": 17.0},
    ])
    out = dv.with_availability_points(board)
    best = out.sort("avail_pos_rank")["player_name"][0]
    assert best == "Durable"
    assert out.sort("TRUE_Points", descending=True)["player_name"][0] == "Fragile"


def test_a_board_with_no_availability_estimate_column_is_returned_untouched():
    board = _board(_ladder(n=3)).drop("usg_expected_games")
    assert dv.with_availability_points(board).columns == board.columns


# =========================================================================
# the panels
# =========================================================================

def test_a_panel_runs_to_twice_replacement():
    """The workbook prints QB40 / RB80 / WR80 / TE37 against baselines of 14 / 35 /
    42 / 17 -- one rule rather than four numbers."""
    board = _board(_ladder(n=100) )
    board = board.with_columns(pl.lit(35).alias("replacement_rank"))
    assert sv.panel_depth(board, "RB") == 70


def test_panel_depth_is_clamped_at_both_ends():
    """Twice replacement at tight end in a six-team league is eight rows, which is
    not a board; at receiver in a sixteen-team one it is 130, which is scrolling."""
    shallow = _board(_ladder(n=100)).with_columns(pl.lit(3).alias("replacement_rank"))
    deep = _board(_ladder(n=100)).with_columns(pl.lit(90).alias("replacement_rank"))
    assert sv.panel_depth(shallow, "RB") == sv.MIN_PANEL_ROWS
    assert sv.panel_depth(deep, "RB") == sv.MAX_PANEL_ROWS


def test_a_position_this_league_does_not_start_has_no_replacement_rank():
    board = _board(_ladder(n=4)).with_columns(
        pl.lit(None, dtype=pl.Int64).alias("replacement_rank"))
    assert sv.replacement_rank(board, "RB") is None


def test_value_is_dollars_in_an_auction_and_a_rank_difference_in_a_snake():
    """An auction has a price and the only question is whether he costs less than he
    is worth. A snake has no price -- a pick is a position in a queue."""
    assert sv.value_column(AUCTION_META) == "our_dollars"
    assert sv.value_column(SNAKE_META) == "value"
    assert sv.value_column({}) == "value"


def test_a_panel_carries_the_seven_columns_and_the_mark():
    board = dv.positional_scarcity(_board(_ladder(n=20)))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    assert list(panel.frame.columns) == [
        "Tier", "Player", "TM/BYE", "PTS", "VALUE", "PS", "ADP", "·"]
    assert panel.frame["TM/BYE"].iloc[0] == "DET/6"


def test_a_panel_is_ordered_by_vor_not_by_the_points_column():
    """VOR is what makes a quarterback comparable to a running back, and it is the
    order `PS` is defined against -- sorting on points while measuring scarcity on
    VOR would put the two in disagreement down the page."""
    board = dv.positional_scarcity(_board([
        {"player_id": 0, "player_name": "LowPointsHighVor", "TRUE_Points": 10.0,
         "vor": 90.0},
        {"player_id": 1, "player_name": "HighPointsLowVor", "TRUE_Points": 300.0,
         "vor": 5.0},
    ]))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    assert panel.frame["Player"].iloc[0] == "LowPointsHighVor"


def test_crossed_off_players_stay_in_place_rather_than_vanishing():
    """Watching the board empty where the players were is most of what a paper draft
    sheet is for, and a row that disappears takes its context with it."""
    board = dv.positional_scarcity(_board(_ladder(n=12)), drafted=[0])
    panel = sv.sheet_panel(board, "RB", SNAKE_META, drafted=[0])
    assert panel.frame["Player"].iloc[0] == "RB1"
    assert panel.frame["·"].iloc[0] == sv.MARK_DRAFTED
    assert panel.frame["·"].iloc[1] == sv.MARK_AVAILABLE
    assert panel.drafted[0] and not panel.drafted[1]


def test_the_count_above_a_panel_is_about_the_position_not_the_search_box():
    """"How many good ones are left" is a fact about the position. Searching for one
    name must not report that the position is down to one player."""
    board = dv.positional_scarcity(_board(_ladder(n=12)))
    whole = sv.sheet_panel(board, "RB", SNAKE_META)
    found = sv.sheet_panel(board, "RB", SNAKE_META, search="RB1")
    assert found.depth < whole.depth
    assert found.remaining == whole.remaining


def test_the_count_falls_as_players_are_crossed_off():
    board = _board(_ladder(n=12))
    whole = sv.sheet_panel(dv.positional_scarcity(board), "RB", SNAKE_META)
    after = sv.sheet_panel(dv.positional_scarcity(board, drafted=[0, 1]), "RB",
                           SNAKE_META, drafted=[0, 1])
    assert after.remaining == whole.remaining - 2


def test_a_search_matches_literally_because_names_are_full_of_regex():
    """"T.J. Hockenson" as a regex matched any three characters between the dots."""
    board = dv.positional_scarcity(_board([
        {"player_id": 0, "player_name": "T.J. Hockenson", "vor": 50.0},
        {"player_id": 1, "player_name": "TXJY Hockenson", "vor": 40.0},
    ]))
    panel = sv.sheet_panel(board, "RB", SNAKE_META, search="T.J.")
    assert panel.frame["Player"].tolist() == ["T.J. Hockenson"]


def test_unprojected_players_are_not_panelled():
    """Half the pool, and their projection is a literal 0.0 rather than a null -- so
    without this they rank as the worst players in the league rather than unknowns."""
    board = dv.positional_scarcity(_board(
        _ladder(n=4) + [{"player_id": 99, "player_name": "Nobody",
                         "projection_missing": True, "vor": 0.0}]))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    assert "Nobody" not in panel.frame["Player"].tolist()


def test_an_empty_position_gives_an_empty_panel_rather_than_raising():
    board = dv.positional_scarcity(_board(_ladder("RB", n=4)))
    panel = sv.sheet_panel(board, "TE", SNAKE_META)
    assert panel.depth == 0
    assert list(panel.frame.columns)[-1] == "·"


# --- formatting ----------------------------------------------------------

def test_the_bands_are_the_tiers_not_the_rows():
    """`ISEVEN($B5)` on the tier number, exactly as the workbook does it -- so the
    bands are the cliff and no second column has to encode it."""
    board = dv.positional_scarcity(_board(_ladder(n=9)))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    html = sv.panel_styler(panel, "light").to_html()
    assert sv.SHEET_FILLS["light"]["band"].lstrip("#").lower() in html.lower()


def test_a_crossed_off_row_is_dimmed_and_still_legible():
    """Rather than blacked out as the workbook does it: a drafted player is still
    information -- who went, and at what -- and you re-read the rows above the one
    you just lost."""
    board = dv.positional_scarcity(_board(_ladder(n=6)), drafted=[0])
    panel = sv.sheet_panel(board, "RB", SNAKE_META, drafted=[0])
    html = sv.panel_styler(panel, "dark").to_html()
    assert sv.SHEET_FILLS["dark"]["struck"].lstrip("#").lower() in html.lower()
    assert sv.SHEET_FILLS["dark"]["struck_ink"].lstrip("#").lower() in html.lower()


def test_an_unknown_theme_falls_back_rather_than_taking_the_page_down():
    board = dv.positional_scarcity(_board(_ladder(n=4)))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    assert sv.panel_styler(panel, "solarized").to_html()


def test_the_styler_paints_but_does_not_format():
    """Formatting is `column_config`'s job. A Styler that also formats hands Streamlit
    strings, and a string column cannot be sorted or aligned as a number -- the same
    division of labour `draft_view.styled_frame` keeps."""
    board = dv.positional_scarcity(_board(_ladder(n=6)))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    html = sv.panel_styler(panel).to_html()
    # The raw value, not a rounded or currency-prefixed one.
    assert "100.0" in html
    assert "$" not in html


def _spec(panel, label):
    return next(c for c in sv.column_specs(panel) if c.label == label)


def test_the_value_column_is_money_only_in_an_auction():
    board = dv.with_cash_value(
        dv.at_budget(dv.positional_scarcity(_board(_ladder(n=20))), 200,
                     meta=AUCTION_META), AUCTION_META, 200)
    cash = sv.sheet_panel(board, "RB", AUCTION_META)
    snake = sv.sheet_panel(board, "RB", SNAKE_META)
    assert _spec(cash, "VALUE").fmt == "$%.0f"
    # Signed, so the column does not depend on inferring direction from magnitude.
    assert _spec(snake, "VALUE").fmt == "%+.0f"
    assert "budget" in _spec(cash, "VALUE").help
    assert "fall" in _spec(snake, "VALUE").help


def test_every_column_of_the_frame_has_a_spec_and_no_spec_is_orphaned():
    """The invariant `SheetColumn` exists for: Streamlit silently ignores config for a
    column the frame does not carry, so a label that drifts stops formatting without
    saying so."""
    board = dv.positional_scarcity(_board(_ladder(n=6)))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    assert [c.label for c in sv.column_specs(panel)] == list(panel.frame.columns)


def test_the_mark_is_the_only_button_and_the_name_is_the_only_pin():
    board = dv.positional_scarcity(_board(_ladder(n=6)))
    specs = sv.column_specs(sv.sheet_panel(board, "RB", SNAKE_META))
    assert [c.label for c in specs if c.kind == "button"] == [sv.MARK_COLUMN]
    assert [c.label for c in specs if c.pinned] == ["Player"]


# =========================================================================
# draft state
# =========================================================================

def test_a_click_crosses_a_player_off_and_a_second_click_restores_him():
    """A misclick on draft night needs to cost one click, not a page reload."""
    board = dv.positional_scarcity(_board(_ladder(n=6)))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    state = {sv.click_key("RB"): {"row": 2}}

    assert sv.toggle_drafted(state, panel.table, "L", "RB") == {2}
    assert sv.toggle_drafted(state, panel.table, "L", "RB") == set()


def test_a_click_is_resolved_to_a_player_not_remembered_as_a_row():
    """A row number is a position in a panel that crossing somebody off has just
    changed, so it goes stale immediately -- and silently, which is the worst way for
    it to be wrong."""
    board = dv.positional_scarcity(_board(_ladder(n=6)))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    state = {sv.click_key("RB"): {"row": 0}}
    sv.toggle_drafted(state, panel.table, "L", "RB")

    # RB1 is gone, so the panel re-renders with somebody else in row 0. The set must
    # still name RB1.
    assert sv.drafted_set(state, "L") == {panel.table["player_id"][0]}


def test_each_panel_reads_only_its_own_clicks():
    """Four tables sharing a key would each read the others', and a click on the
    receiver panel would cross off a quarterback."""
    board = dv.positional_scarcity(
        _board(_ladder("RB", n=4) + _ladder("QB", n=4, first_id=100)))
    rb = sv.sheet_panel(board, "RB", SNAKE_META)
    state = {sv.click_key("QB"): {"row": 0}}
    assert sv.toggle_drafted(state, rb.table, "L", "RB") == set()


def test_the_crossed_off_set_is_scoped_per_league():
    """Same reason `budget_key` is: a shared key carries a half-finished Knights
    draft onto GOP's board."""
    state = {}
    board = dv.positional_scarcity(_board(_ladder(n=4)))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    state[sv.click_key("RB")] = {"row": 1}
    sv.toggle_drafted(state, panel.table, "knights", "RB")
    assert sv.drafted_set(state, "knights") == {1}
    assert sv.drafted_set(state, "gop") == set()


def test_no_click_leaves_the_set_alone():
    board = dv.positional_scarcity(_board(_ladder(n=4)))
    panel = sv.sheet_panel(board, "RB", SNAKE_META)
    state = {sv.drafted_key("L"): {3}}
    assert sv.toggle_drafted(state, panel.table, "L", "RB") == {3}


def test_a_stale_row_number_past_the_end_of_the_panel_is_ignored():
    """The panel shrinks when a search narrows it; a click held from the wider view
    must not name whoever is now off the end."""
    board = dv.positional_scarcity(_board(_ladder(n=4)))
    panel = sv.sheet_panel(board, "RB", SNAKE_META, search="RB1")
    state = {sv.click_key("RB"): {"row": 3}}
    assert sv.toggle_drafted(state, panel.table, "L", "RB") == set()
