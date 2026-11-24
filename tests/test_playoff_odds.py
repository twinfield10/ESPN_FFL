"""Playoff odds: the inputs each simulated manager acts on, and the simulation itself.

Small synthetic leagues throughout -- the real-data run (all nine 2026 leagues, about a
second each) is recorded in ``docs/plans/57-playoff-bracket.md``.
"""

from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest

from Scripts import playoff_odds as odds
from Scripts import playoffs as po


# --- player values -------------------------------------------------------------

def _lineups(rows):
    cols = ["week", "team_owner", "player_id", "player_name", "player_position",
            "pro_team", "player_active_status", "game_state", "TRUE_Points", "ACT_Points"]
    return pl.DataFrame(rows, schema=cols, orient="row")


def test_value_is_this_weeks_projection_then_last_projection_then_average():
    lu = _lineups([
        # A: projected this week -> that projection
        (3, "X", 1, "A", "RB", "KC", "active", "pre", 15.0, None),
        (2, "X", 1, "A", "RB", "KC", "active", "post", 9.0, 30.0),
        # B: on bye this week -> his most recent projection, not his average
        (3, "X", 2, "B", "WR", "LA", "bye", "bye", 0.0, None),
        (2, "X", 2, "B", "WR", "LA", "active", "post", 12.0, 2.0),
        # C: never projected above zero -> season average
        (3, "X", 3, "C", "TE", "NYJ", "inactive", "pre", 0.0, None),
        (1, "X", 3, "C", "TE", "NYJ", "active", "post", 0.0, 7.0),
        # free agents are not on any team
        (3, "Free Agent", 4, "D", "RB", "KC", "active", "pre", 20.0, None),
    ])
    v = odds.player_values(lu, 3).sort("player_id")
    assert v["player_id"].to_list() == [1, 2, 3]
    assert v["value"].to_list() == [15.0, 12.0, 7.0]
    assert v["value_source"].to_list() == ["projection", "last_projection", "season_average"]


def test_rank_implied_value_follows_ros_order_on_the_leagues_own_scale():
    """FantasyPros prefers RB "Low" to RB "High"; our projections disagree. The
    manager starts by the ranking, so Low takes the higher of the two values."""
    values = pl.DataFrame({
        "player_id": [1, 2, 3], "player_name": ["High", "Low", "Idp"],
        "player_position": ["RB", "RB", "LB"], "pro_team": ["KC", "BUF", "DAL"],
        "value": [18.0, 10.0, 6.0]})
    ros = pd.DataFrame({
        "player_name": ["Low", "High"], "position": ["RB", "RB"], "pro_team": ["BUF", "KC"],
        "rank_ecr": [1, 2], "pos_rank": ["RB1", "RB2"], "rank_std": [1.0, 1.0],
        "total_experts": [3, 3], "r2p_pts": [100.0, 90.0], "player_owned_avg": [99.0, 99.0],
        "captured_at": ["x", "x"]})
    out = odds.attach_order(values, ros).sort("player_id")
    order = dict(zip(out["player_name"].to_list(), out["_order"].to_list()))
    assert order == {"High": 10.0, "Low": 18.0, "Idp": 6.0}


def test_no_ros_ranks_orders_by_value():
    values = pl.DataFrame({"player_id": [1], "player_name": ["A"], "player_position": ["RB"],
                           "pro_team": ["KC"], "value": [12.5]})
    assert odds.attach_order(values, None)["_order"].to_list() == [12.5]


@pytest.mark.parametrize("row, week, expected", [
    ({"pro_team": "KC", "player_active_status": "active"}, 13, False),      # KC bye in 13
    ({"pro_team": "KC", "player_active_status": "active"}, 12, True),
    ({"pro_team": "BUF", "player_active_status": "inactive"}, 12, False),   # out this week
    ({"pro_team": "BUF", "player_active_status": "inactive",
      "injury_status": "OUT"}, 13, True),                                     # back next week
    ({"pro_team": "BUF", "player_active_status": "inactive",
      "injury_status": "INJURY_RESERVE"}, 13, False),                         # IR: out
])
def test_availability(row, week, expected):
    assert odds.available(row, week, current=12, byes={"KC": 13}) is expected


# --- simulation ------------------------------------------------------------------

def _table(results, teams, divisions=None):
    table = po.Table.empty(divisions or {t: "" for t in teams})
    for a, b, sa, sb in results:
        table.add(a, b, sa, sb)
    return table


def _settings(k, weeks):
    return {"playoff_team_count": k, "seeding_rule": "TOTAL_POINTS_SCORED",
            "regular_season_periods": weeks[0][0] - 1,
            "playoff_periods": [[w[0], w] for w in weeks]}


def test_certain_outcomes_come_out_certain():
    """No spread: the strongest team wins every remaining game and every playoff."""
    teams = ["A", "B", "C", "D"]
    table = _table([("A", "B", 100, 90), ("C", "D", 100, 90)], teams)
    mus = {"A": 150, "B": 100, "C": 120, "D": 80}
    weeks = [3, 4, 5]
    strengths = {(t, w): odds.Strength(mus[t], 0.0, 9) for t in teams for w in weeks}
    out = odds.simulate(table, [(3, "A", "C"), (3, "B", "D")], strengths,
                        _settings(2, [[4], [5]]), n=50).sort("team_owner")
    by = {r["team_owner"]: r for r in out.iter_rows(named=True)}
    assert by["A"]["p_seed_1"] == 1.0 and by["A"]["p_champion"] == 1.0
    assert by["C"]["p_seed_2"] == 1.0 and by["C"]["p_final"] == 1.0
    assert by["B"]["p_playoffs"] == 0.0 and by["D"]["p_playoffs"] == 0.0
    assert by["A"]["exp_wins"] == 2.0
    assert by["A"]["likeliest_opponent"] == "C" and by["A"]["likeliest_opponent_p"] == 1.0


def test_identical_teams_split_a_coin_flip_evenly():
    teams = ["A", "B"]
    strengths = {(t, w): odds.Strength(100.0, 20.0, 9) for t in teams for w in (2, 3)}
    out = odds.simulate(_table([], teams), [(2, "A", "B")], strengths,
                        _settings(2, [[3]]), n=4000)
    p = dict(zip(out["team_owner"].to_list(), out["p_seed_1"].to_list()))
    assert p["A"] == pytest.approx(0.5, abs=0.04)
    assert p["A"] + p["B"] == pytest.approx(1.0)


def test_division_and_bye_odds_and_bye_team_first_opponent():
    """Six teams, two divisions, six make it: the top two have byes, and a bye team's
    first opponent is whoever wins the round-1 game feeding its slot."""
    teams = list("ABCDEF")
    divs = {"A": "E", "B": "E", "C": "E", "D": "W", "E": "W", "F": "W"}
    mus = {"A": 200, "B": 150, "C": 100, "D": 190, "E": 140, "F": 90}
    strengths = {(t, w): odds.Strength(mus[t], 0.0, 9) for t in teams for w in (2, 3, 4, 5)}
    fixtures = [(2, "A", "D"), (2, "B", "E"), (2, "C", "F")]
    out = odds.simulate(_table([], teams, divs), fixtures, strengths,
                        _settings(6, [[3], [4], [5]]), n=20)
    by = {r["team_owner"]: r for r in out.iter_rows(named=True)}
    assert by["A"]["p_division"] == 1.0 and by["E"]["p_division"] == 0.0
    # D loses to A but wins the West, so D is the 2 seed with a bye over 3-seed B.
    assert by["A"]["p_bye"] == 1.0 and by["D"]["p_bye"] == 1.0 and by["B"]["p_bye"] == 0.0
    # 1 meets the 4/5 winner: E (5) beats C (4) on 140 vs 100.
    assert by["A"]["likeliest_opponent"] == "E"


def test_simulation_is_reproducible():
    teams = ["A", "B", "C", "D"]
    strengths = {(t, w): odds.Strength(100.0 + i, 15.0, 9)
                 for i, t in enumerate(teams) for w in (2, 3, 4)}
    args = (_table([], teams), [(2, "A", "B"), (2, "C", "D")], strengths,
            _settings(2, [[3], [4]]))
    assert odds.simulate(*args, n=300).equals(odds.simulate(*args, n=300))


def test_table_copy_is_independent():
    table = _table([("A", "B", 1, 0)], ["A", "B"])
    sim = table.copy()
    sim.add("B", "A", 5, 0)
    assert table.w["B"] == 0 and table.vs[("B", "A")] == [0.0, 1]
    assert sim.w["B"] == 1 and sim.vs[("B", "A")] == [1.0, 2]


# --- fixtures --------------------------------------------------------------------------

def test_fixtures_from_league_lists_each_game_once_through_the_regular_season():
    a, b, c, d = (SimpleNamespace(team_id=i, owner=o) for i, o in
                  enumerate(["Ann", "Bob", "Cy", "Di"], start=1))
    a.schedule, b.schedule, c.schedule, d.schedule = [b, c, d], [a, d, c], [d, a, b], [c, b, a]
    fx = po.fixtures_from_league(SimpleNamespace(teams=[a, b, c, d]), 2)
    assert fx == [[1, "Ann", "Bob"], [1, "Cy", "Di"], [2, "Ann", "Cy"], [2, "Bob", "Di"]]


def test_build_refuses_a_store_without_settings():
    with pytest.raises(ValueError, match="playoff_settings"):
        odds.build(pl.DataFrame(), pl.DataFrame(), {}, 2026)
