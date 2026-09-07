"""The draft rundown: three bases, starters totals, ranks, intervals, notable picks.

Covers ``app/rundown.py``. No network and no store on disk -- picks and boards are
synthesised to the shape ``draft.parquet`` and ``board.parquet`` carry.

Two claims the module makes and these pin:

* **The headline is the best legal starting lineup, not the sum of the roster.** Four
  good quarterbacks are one good quarterback, and a rundown that adds them all up
  would grade a hoarder as a genius.
* **The band is not the sum of the players' own p10s.** That prices the world where
  every starter busts at once. On one real six-team roster it read 1,018 against a
  projection of 2,183 -- a "floor" 53% below the mean that nothing in the model
  supports. ``test_the_band_is_not_the_sum_of_the_player_floors`` is that regression.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

# `app/` is not a package: Streamlit runs the page scripts directly with app/ on
# sys.path, so the modules import each other by bare name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import rundown as rd  # noqa: E402


# --- fixtures ------------------------------------------------------------

ELIGIBLE = {
    "QB": ["QB", "OP", "BE", "IR"],
    "RB": ["RB", "RB/WR", "RB/WR/TE", "OP", "BE", "IR"],
    "WR": ["RB/WR", "WR", "WR/TE", "RB/WR/TE", "OP", "BE", "IR"],
    "TE": ["TE", "WR/TE", "RB/WR/TE", "OP", "BE", "IR"],
    "K": ["K", "BE", "IR"],
    "D/ST": ["D/ST", "BE", "IR"],
}

FLEX_SLOTS = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1}


def board(rows):
    """A board frame with the columns the rundown reads, defaults filled in."""
    defaults = {
        "player_id": 0, "primaryPosition": "RB", "eligible_slots": ELIGIBLE["RB"],
        "bye_week": 6.0, "tier": 1.0, "vor": 10.0, "value": 0.0,
        "value_rank_adp": 20.0, "adp": 20.0, "pts_p10": 80.0, "pts_p90": 200.0,
        "pts_sd": 40.0, "espn_pos_rank": 1.0, "ath_pos_rank": 1.0,
        "adp_is_priced": True, "ESPN_projected_total": 100.0,
        "ATH_projected_total": 100.0, "TRUE_Points": 100.0,
    }
    out = []
    for index, row in enumerate(rows):
        merged = {**defaults, "player_id": index + 1}
        merged.update(row)
        if "eligible_slots" not in row and "primaryPosition" in row:
            merged["eligible_slots"] = ELIGIBLE[row["primaryPosition"]]
        out.append(merged)
    return pl.DataFrame(out)


def picks(rows, season=2026):
    """A draft frame with the columns the rundown reads."""
    out = []
    for index, row in enumerate(rows):
        out.append({"season": season, "overall_pick": index + 1,
                    "round": index // 2 + 1, "owner": "A", "player_id": index + 1,
                    "player_name": f"P{index + 1}", "position": "RB", "bid": 0.0,
                    **row})
    return pl.DataFrame(out)


def full_roster(owner, points, *, offset=0):
    """One legal starting lineup's worth of picks, at a given points level."""
    positions = ["QB", "RB", "RB", "WR", "WR", "TE", "RB/WR/TE", "K", "D/ST"]
    actual = ["QB", "RB", "RB", "WR", "WR", "TE", "RB", "K", "D/ST"]
    pick_rows, board_rows = [], []
    for i, position in enumerate(actual):
        pick_rows.append({"owner": owner, "player_id": offset + i + 1,
                          "player_name": f"{owner}{i}", "position": position})
        board_rows.append({"player_id": offset + i + 1, "primaryPosition": position,
                           "eligible_slots": ELIGIBLE[position],
                           "ESPN_projected_total": points,
                           "ATH_projected_total": points, "TRUE_Points": points})
    return pick_rows, board_rows


# --- joining -------------------------------------------------------------

def test_a_league_that_has_not_drafted_returns_empty():
    """Seven of ten leagues were in this state the day this shipped."""
    joined = rd.picks_with_board(picks([{}], season=2025), board([{}]), 2026)
    assert joined.is_empty()


def test_picks_join_the_board_on_player_id():
    """Not on the name. Both artifacts carry ESPN's id, and the weekly pipeline's
    name-equality joins are the repo's standing source of missed matches."""
    joined = rd.picks_with_board(
        picks([{"player_name": "Spelled One Way"}]),
        board([{"TRUE_Points": 222.0}]), 2026)
    assert joined["TRUE_Points"].to_list() == [222.0]


def test_a_season_with_no_picks_column_is_not_a_crash():
    assert rd.picks_with_board(pl.DataFrame({"x": [1]}), board([{}]), 2026).is_empty()


# --- bases ---------------------------------------------------------------

def test_all_three_bases_are_offered_when_the_board_has_them():
    assert [b.key for b in rd.available_bases(board([{}]))] == ["espn", "ath", "ours"]


def test_a_basis_whose_column_is_entirely_null_is_dropped():
    """The Athletic only exists from 2026 -- it arrives as a hand-imported
    workbook. A season without it must show two bases, not three empty ones."""
    frame = board([{}]).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("ATH_projected_total"))
    assert [b.key for b in rd.available_bases(frame)] == ["espn", "ours"]


def test_a_basis_whose_column_is_missing_is_dropped():
    frame = board([{}]).drop("ATH_projected_total")
    assert [b.key for b in rd.available_bases(frame)] == ["espn", "ours"]


# --- starters ------------------------------------------------------------

def test_the_total_is_the_starting_lineup_not_the_roster():
    """Four good quarterbacks are one good quarterback."""
    rows = [{"primaryPosition": "QB", "TRUE_Points": 300.0, "player_id": i + 1}
            for i in range(4)]
    joined = rd.picks_with_board(
        picks([{"player_id": i + 1} for i in range(4)]), board(rows), 2026)
    _, total = rd.fill_starters(joined.to_dicts(), {"QB": 1}, "TRUE_Points")
    assert total == pytest.approx(300.0)


def test_flex_is_filled_after_the_dedicated_slots():
    rows = [
        {"primaryPosition": "RB", "TRUE_Points": 300.0},
        {"primaryPosition": "RB", "TRUE_Points": 200.0},
        {"primaryPosition": "RB", "TRUE_Points": 100.0},
    ]
    starters, total = rd.fill_starters(
        board(rows).to_dicts(), {"RB": 2, "RB/WR/TE": 1}, "TRUE_Points")
    assert total == pytest.approx(600.0)
    assert sorted(r["slot"] for r in starters) == ["RB", "RB", "RB/WR/TE"]


def test_a_superflex_slot_takes_a_second_quarterback():
    rows = [{"primaryPosition": "QB", "TRUE_Points": 300.0},
            {"primaryPosition": "QB", "TRUE_Points": 280.0}]
    starters, _ = rd.fill_starters(board(rows).to_dicts(), {"QB": 1, "OP": 1},
                                   "TRUE_Points")
    assert sorted(r["slot"] for r in starters) == ["OP", "QB"]


def test_a_slot_the_roster_cannot_fill_contributes_nothing():
    """The honest reading of a team that drafted no kicker."""
    rows = [{"primaryPosition": "QB", "TRUE_Points": 300.0}]
    starters, total = rd.fill_starters(board(rows).to_dicts(), {"QB": 1, "K": 1},
                                       "TRUE_Points")
    assert len(starters) == 1 and total == pytest.approx(300.0)


def test_dst_is_not_read_as_a_three_way_flex():
    """`D/ST` is a slot name with a slash in it that is not a flex.

    Scarcity is measured from the roster rather than by splitting the slot name,
    which is what keeps this right without a lookup table.
    """
    rows = [{"primaryPosition": "D/ST", "TRUE_Points": 100.0},
            {"primaryPosition": "RB", "TRUE_Points": 300.0}]
    starters, _ = rd.fill_starters(board(rows).to_dicts(), {"D/ST": 1, "RB": 1},
                                   "TRUE_Points")
    assert {r["slot"] for r in starters} == {"D/ST", "RB"}


def test_no_slots_means_no_starters():
    assert rd.fill_starters(board([{}]).to_dicts(), {}, "TRUE_Points") == ([], 0.0)


# --- the interval --------------------------------------------------------

def test_the_band_is_not_the_sum_of_the_player_floors():
    """Adding nine 10th percentiles prices "everyone busts at once".

    Nine starters at 100 points with sd 40 sum to 900 with an independent sd of 120,
    so the band is 900 ± 154. Summing the players' own p10s would have given 720 --
    a floor 20% below the mean off marginals that support nothing of the kind.
    """
    starters = [{"pts_sd": 40.0, "pts_p10": 80.0} for _ in range(9)]
    low, high, priced = rd.team_interval(starters, 900.0)
    assert priced == 9
    assert low == pytest.approx(900 - 1.2816 * 120.0)
    assert high == pytest.approx(900 + 1.2816 * 120.0)
    assert low > sum(r["pts_p10"] for r in starters)


def test_starters_with_no_spread_contribute_their_mean_only():
    """Kickers and defences carry no fitted spread; they still score."""
    starters = [{"pts_sd": 30.0}, {"pts_sd": None}, {"pts_sd": 40.0}]
    low, high, priced = rd.team_interval(starters, 500.0)
    assert priced == 2
    assert high - low == pytest.approx(2 * 1.2816 * 50.0)


def test_no_spread_anywhere_reports_no_band():
    """Blank rather than a band of zero width, which would read as certainty."""
    assert rd.team_interval([{"pts_sd": None}], 100.0) == (None, None, 0)


# --- the table -----------------------------------------------------------

def _league(levels):
    """A drafted league where team i's players are all worth ``levels[i]``."""
    pick_rows, board_rows = [], []
    for index, (owner, points) in enumerate(levels.items()):
        p, b = full_roster(owner, points, offset=index * 9)
        pick_rows += p
        board_rows += b
    return rd.picks_with_board(picks(pick_rows), board(board_rows), 2026)


def test_ranks_are_a_permutation_and_the_best_is_first():
    joined = _league({"Best": 300.0, "Middle": 200.0, "Worst": 100.0})
    table = rd.team_table(joined, FLEX_SLOTS, rd.available_bases(board([{}])))
    assert table["owner"].to_list() == ["Best", "Middle", "Worst"]
    for basis in ("espn", "ath", "ours"):
        assert sorted(table[f"{basis}_rank"].to_list()) == [1, 2, 3]
    assert table["consensus_rank"].to_list() == [1.0, 2.0, 3.0]


def test_vor_is_summed_over_starters_not_the_roster():
    """A deep bench is not a penalty.

    VOR is points above *replacement*, and replacement level is defined by the
    starting slots -- so it only means anything about a starter. Summed over a full
    roster it read an 8-team superflex team at -528 because seven of its sixteen
    players sit below a line they were never competing with.
    """
    pick_rows, board_rows = full_roster("A", 100.0)
    # A bench player miles below replacement.
    pick_rows.append({"owner": "A", "player_id": 99, "player_name": "Bench",
                      "position": "RB"})
    board_rows.append({"player_id": 99, "primaryPosition": "RB",
                       "eligible_slots": ELIGIBLE["RB"], "vor": -500.0,
                       "TRUE_Points": 1.0, "ESPN_projected_total": 1.0,
                       "ATH_projected_total": 1.0})
    joined = rd.picks_with_board(picks(pick_rows), board(board_rows), 2026)
    table = rd.team_table(joined, FLEX_SLOTS, rd.available_bases(board([{}])))
    assert table["vor"][0] == pytest.approx(90.0)      # 9 starters x 10, bench excluded


def test_value_is_summed_over_the_whole_roster():
    """`value` is rank-against-ADP: a bench player taken below his price is real."""
    pick_rows, board_rows = full_roster("A", 100.0)
    pick_rows.append({"owner": "A", "player_id": 99, "player_name": "Steal",
                      "position": "RB"})
    board_rows.append({"player_id": 99, "primaryPosition": "RB",
                       "eligible_slots": ELIGIBLE["RB"], "value": 40.0,
                       "TRUE_Points": 1.0, "ESPN_projected_total": 1.0,
                       "ATH_projected_total": 1.0})
    joined = rd.picks_with_board(picks(pick_rows), board(board_rows), 2026)
    table = rd.team_table(joined, FLEX_SLOTS, rd.available_bases(board([{}])))
    assert table["value"][0] == pytest.approx(40.0)


def test_median_gap_says_how_far_from_the_middle():
    """The part a rank cannot tell you: fourth by two points, or fourth by ninety."""
    joined = _league({"A": 300.0, "B": 200.0, "C": 100.0})
    bases = rd.available_bases(board([{}]))
    table = rd.median_gap(rd.team_table(joined, FLEX_SLOTS, bases), bases[-1])
    assert table["ours_vs_median"].to_list() == pytest.approx([900.0, 0.0, -900.0])


def test_an_empty_league_gives_an_empty_table():
    assert rd.team_table(pl.DataFrame({"owner": []}), FLEX_SLOTS, []).is_empty()


# --- grades --------------------------------------------------------------

def test_the_letter_is_a_rescaling_of_the_rank():
    """Monotone in rank, so the letter and the rank beside it cannot disagree."""
    grades = [rd.letter(rank, 10) for rank in range(1, 11)]
    assert grades[0] == "A" and grades[-1] == "D"
    order = ["A", "B+", "B", "C+", "C", "D"]
    assert all(order.index(a) <= order.index(b) for a, b in zip(grades, grades[1:]))


def test_first_of_two_is_an_a_and_second_is_a_d():
    assert rd.letter(1, 2) == "A"
    assert rd.letter(2, 2) == "D"


def test_a_one_team_league_does_not_divide_by_zero():
    assert rd.letter(1, 1) == "A"


# --- notable picks -------------------------------------------------------

def test_steals_and_reaches_come_off_value():
    rows = [{"value": v, "player_id": i + 1} for i, v in enumerate([30.0, 0.0, -30.0])]
    joined = rd.picks_with_board(
        picks([{"owner": "A", "player_id": i + 1,
                "player_name": n} for i, n in enumerate(["Steal", "Even", "Reach"])]),
        board(rows), 2026)
    steals, reaches = rd.notable_picks(joined, "A", count=1)
    assert steals["player_name"].to_list() == ["Steal"]
    assert reaches["player_name"].to_list() == ["Reach"]


def test_unpriced_players_are_excluded_from_the_value_ranking():
    """ESPN parks everyone it has no opinion about on one ADP plateau near 170.

    A "steal" measured against a plateau is an artefact of the plateau.
    """
    rows = [{"value": 500.0, "adp_is_priced": False, "player_id": 1},
            {"value": 30.0, "adp_is_priced": True, "player_id": 2}]
    joined = rd.picks_with_board(
        picks([{"owner": "A", "player_id": 1, "player_name": "Unpriced"},
               {"owner": "A", "player_id": 2, "player_name": "Real"}]),
        board(rows), 2026)
    steals, _ = rd.notable_picks(joined, "A", count=1)
    assert steals["player_name"].to_list() == ["Real"]


def test_a_board_with_nothing_priced_still_says_something():
    """Substituting the unfiltered set beats showing an empty table."""
    rows = [{"value": 30.0, "adp_is_priced": False, "player_id": 1}]
    joined = rd.picks_with_board(
        picks([{"owner": "A", "player_id": 1, "player_name": "Only"}]),
        board(rows), 2026)
    steals, _ = rd.notable_picks(joined, "A", count=1)
    assert steals["player_name"].to_list() == ["Only"]


# --- roster view ---------------------------------------------------------

def test_the_roster_marks_who_starts_and_who_does_not():
    pick_rows, board_rows = full_roster("A", 100.0)
    pick_rows.append({"owner": "A", "player_id": 99, "player_name": "Bench",
                      "position": "RB"})
    board_rows.append({"player_id": 99, "primaryPosition": "RB",
                       "eligible_slots": ELIGIBLE["RB"], "TRUE_Points": 1.0,
                       "ESPN_projected_total": 1.0, "ATH_projected_total": 1.0})
    joined = rd.picks_with_board(picks(pick_rows), board(board_rows), 2026)
    frame = rd.roster_frame(joined, "A", FLEX_SLOTS,
                            rd.available_bases(board([{}])))
    assert frame.filter(pl.col("slot") == "BE")["player_name"].to_list() == ["Bench"]
    assert frame.filter(pl.col("slot") != "BE").height == 9


def test_my_row_is_none_for_a_league_that_is_not_yours():
    """The normal case for the five leagues that belong to other people."""
    joined = _league({"Someone Else": 100.0})
    table = rd.team_table(joined, FLEX_SLOTS, rd.available_bases(board([{}])))
    assert rd.my_row(table, "Tommy Winfield") is None
    assert rd.my_row(table, None) is None
    assert rd.my_row(table, "Someone Else")["owner"] == "Someone Else"


def test_league_owners_lists_everyone_who_picked():
    joined = _league({"B": 100.0, "A": 200.0})
    assert rd.league_owners(joined) == ["A", "B"]
