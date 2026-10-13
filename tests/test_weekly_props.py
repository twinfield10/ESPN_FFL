"""The four weekly prop books added 2026-10-13, and the pooled market they feed.

Fixtures are trimmed from the live week-6 payloads probed that day. No network.
"""

from datetime import date

import pandas as pd
import polars as pl
import pytest

from Scripts.books import (draftkings_props as dk, fanduel_props as fd,
                          fourcasters_props as c4, novig_props as nv, weekly_props as wp)
from Scripts.projection_utils import (IMPUTED_SUFFIX, WEEKLY_FIXED_SHARES, WEEKLY_WEIGHTS,
                                      build_market_consensus, compute_weighted_stats)

SCHED = pl.DataFrame({
    "NFL_game_id": ["2026_06_SEA_DEN", "2026_06_WAS_SF"],
    "week": [6, 6],
    "officialDate": [date(2026, 10, 15), date(2026, 10, 19)],
    "Away": ["SEA", "WAS"], "Home": ["DEN", "SF"],
})


# --- shared --------------------------------------------------------------------------

def test_team_resolver_reads_every_spelling_the_books_use():
    resolve = wp.team_resolver()
    assert resolve("Washington Commanders") == "WAS"
    assert resolve("WSH") == "WAS"
    assert resolve("Los Angeles Rams") == "LA"
    assert resolve("seattle seahawks") == "SEA"
    assert resolve("Nowhere Nobodies") is None


def test_game_lookup_ignores_order():
    find = wp.game_lookup(SCHED)
    assert find("Denver Broncos", "Seattle Seahawks") == "2026_06_SEA_DEN"
    assert find("Seattle Seahawks", "Denver Broncos") == "2026_06_SEA_DEN"
    assert find("Buffalo Bills", "Las Vegas Raiders") is None


def test_american_to_decimal():
    assert wp.american_to_decimal(150) == pytest.approx(2.5)
    assert wp.american_to_decimal(-200) == pytest.approx(1.5)


def _two_way(name, stat, line, p_over, p_under, position="WR"):
    base = {"book": "X", "NFL_game_id": "2026_06_SEA_DEN", "week": 6, "player_name": name,
            "team": None, "position": position, "espn_stat": stat, "prop_source": "OverUnder",
            "value": line}
    return [{**base, "type": "Over", "impProb": p_over, "odds": 1 / p_over},
            {**base, "type": "Under", "impProb": p_under, "odds": 1 / p_under}]


def test_project_stat_reads_an_even_line_as_itself():
    raw = wp.raw_frame(_two_way("A", "receivingYards", 54.5, 0.53, 0.53))
    out = wp.project_stat(raw, "receivingYards", None, wp.KEYS)
    assert out["proj_receivingYards"][0] == pytest.approx(54.5)


def test_project_book_splits_an_anytime_ladder_by_position():
    rows = []
    for name, pos in (("Back", "RB"), ("Wideout", "REC")):
        rows.append({"book": "X", "NFL_game_id": "2026_06_SEA_DEN", "week": 6,
                     "player_name": name, "position": pos, "espn_stat": "anytimeTouchdown",
                     "value": 1.0, "type": "Over", "impProb": 0.40, "prop_source": "Values"})
    out = wp.project_book(wp.raw_frame(rows), SCHED, fallback_hold=1.0)
    back = out.filter(pl.col("player_name") == "Back").row(0, named=True)
    wide = out.filter(pl.col("player_name") == "Wideout").row(0, named=True)
    assert back["proj_rushingTouchdowns"] > 0 and back["proj_receivingTouchdowns"] == 0
    assert wide["proj_receivingTouchdowns"] > 0 and wide["proj_rushingTouchdowns"] == 0


def test_book_overround_falls_back_without_a_pair():
    assert wp.book_overround(wp.raw_frame([]), default=1.07) == 1.07


# --- FanDuel -------------------------------------------------------------------------

def _runner(name, american, handicap=0, logo="seattle_seahawks_jersey.png"):
    return {"runnerName": name, "handicap": handicap, "runnerStatus": "ACTIVE",
            "logo": f"https://x/images/team/nfl/{logo}",
            "winRunnerOdds": {"americanDisplayOdds": {"americanOddsInt": american}}}


def test_fanduel_two_way_alt_and_touchdown_markets():
    team_of = lambda r: "SEA"  # noqa: E731
    base = {"book": "FanDuel", "NFL_game_id": "2026_06_SEA_DEN"}
    two_way = {"marketType": "PLAYER_X_RECEPTIONS_MEDIUM", "marketName": "AJ Barner - Total Receptions",
               "runners": [_runner("AJ Barner Over", -118, 2.5), _runner("AJ Barner Under", -112, 2.5)]}
    alt = {"marketType": "PLAYER_X_ALT_RECEIVING_YARDS_HIGH", "marketName": "AJ Barner - Alt Receiving Yds",
           "runners": [_runner("AJ Barner 25+ Yards", 120)]}
    tds = {"marketType": "TO_SCORE_2+_TOUCHDOWNS", "runners": [_runner("Jaxon Smith-Njigba", 750)]}
    skip = {"marketType": "ANYTIME_1ST_HALF_TD_SCORER", "runners": [_runner("X", 300)]}

    rows = fd.market_rows(two_way, base, team_of)
    assert [(r["type"], r["value"], r["prop_source"]) for r in rows] == [
        ("Over", 2.5, "OverUnder"), ("Under", 2.5, "OverUnder")]
    assert rows[0]["espn_stat"] == "receivingReceptions" and rows[0]["player_name"] == "AJ Barner"
    assert fd.market_rows(alt, base, team_of)[0]["value"] == 25.0
    td = fd.market_rows(tds, base, team_of)[0]
    assert (td["espn_stat"], td["value"], td["odds"]) == ("anytimeTouchdown", 2.0, pytest.approx(8.5))
    assert fd.market_rows(skip, base, team_of) == []


# --- DraftKings via ESPN -------------------------------------------------------------

def test_draftkings_stores_a_line_pair_at_even_money_and_skips_the_rest():
    def item(kind, athlete, value):
        return {"type": {"name": kind},
                "athlete": {"$ref": f"http://x/athletes/{athlete}?lang=en"},
                "current": {"target": {"value": value}}}
    payload = {
        "games": [{"away": "Seattle Seahawks", "home": "Denver Broncos", "props": [
            item("Total Receiving Yards (incl. overtime)", "1", 54.5),
            item("Total Receiving Yards (incl. overtime)", "1", 54.5),
            item("Total Passing Interceptions (incl. overtime)", "2", 0.5),
            item("Receptions Milestones", "1", 4.0),
        ]}],
        "rosters": {"25": {"team": {"abbreviation": "SEA"}, "athletes": [
            {"items": [{"id": "1", "fullName": "Jaxon Smith-Njigba", "position": {"abbreviation": "WR"}},
                       {"id": "2", "fullName": "Sam Darnold", "position": {"abbreviation": "QB"}}]}]}},
    }
    raw = dk.normalize(payload, SCHED)
    assert raw.height == 2
    assert set(raw["type"]) == {"Over", "Under"}
    assert raw["impProb"].to_list() == [0.5, 0.5]
    assert raw["position"][0] == "WR" and raw["team"][0] == "SEA"


# --- Novig ---------------------------------------------------------------------------

def test_novig_take_walks_the_opposite_bids():
    # A bid at 0.45 for 1,000 contracts lets a taker buy the other side at 0.55.
    assert nv.take([{"price": 0.45, "qty": 100000}]) == pytest.approx(0.55)
    assert nv.take([{"price": 0.45, "qty": 10}]) is None  # $5.50 cannot fill $50


def test_novig_main_rung_is_the_line_and_every_rung_a_ladder_step():
    def market(mid, strike, kind="RECEIVING_YARDS"):
        return {"marketId": mid, "eventId": "e", "marketType": kind, "strike": str(strike),
                "description": f"Jake Ferguson {strike} {kind}",
                "outcomes": [{"outcomeId": f"{mid}o", "name": f"Over {strike}"},
                             {"outcomeId": f"{mid}u", "name": f"Under {strike}"}]}

    def book(p_over, p_under):
        # Taking over reads the under bids, and vice versa.
        return {"orders": {"o": [{"price": 1 - p_under, "qty": 1e6}],
                           "u": [{"price": 1 - p_over, "qty": 1e6}]}}

    payload = {
        "events": [{"eventId": "e", "description": "Seattle Seahawks @ Denver Broncos"}],
        "markets": [market("m1", 30.5), market("m2", 50.5)],
        "books": {"m1": {"orders": {"m1o": book(0.52, 0.50)["orders"]["o"],
                                    "m1u": book(0.52, 0.50)["orders"]["u"]}},
                  "m2": {"orders": {"m2o": book(0.20, 0.82)["orders"]["o"],
                                    "m2u": book(0.20, 0.82)["orders"]["u"]}}},
    }
    raw = nv.normalize(payload, SCHED)
    ladder = raw.filter(pl.col("prop_source") == "Values").sort("value")
    line = raw.filter(pl.col("prop_source") == "OverUnder")
    assert ladder["value"].to_list() == [31.0, 51.0]
    assert line["value"].unique().to_list() == [30.5]
    assert raw["player_name"].unique().to_list() == ["Jake Ferguson"]


# --- 4Casters ------------------------------------------------------------------------

def test_fourcasters_vwap_adds_the_commission_and_refuses_a_thin_book():
    q = c4.vwap([(100, 1000)])
    assert q == pytest.approx(0.5 + 0.01 * 0.5)
    assert c4.vwap([(100, 10)]) is None


def test_fourcasters_main_rung_and_anytime_ladder():
    game = {"eventName": "JAYLEN WADDLE (TOTAL RECEIVING YARDS)", "mainTotal": 49.5,
            "participants": [{"longName": "Seattle Seahawks"}, {"longName": "Denver Broncos"}],
            "over": {"49.5": [{"odds": -115, "sumUntaken": 500}]},
            "under": {"49.5": [{"odds": -116, "sumUntaken": 500}]}}
    td = {"eventName": "TROY FRANKLIN (TOTAL TOUCHDOWNS)", "mainTotal": 0.5,
          "participants": game["participants"],
          "over": {"0.5": [{"odds": 555, "sumUntaken": 1800}]}, "under": {"0.5": []}}
    raw = c4.normalize({"data": {"games": [game, td]}}, SCHED)
    waddle = raw.filter(pl.col("player_name") == "Jaylen Waddle")
    assert sorted(waddle["prop_source"].to_list()) == ["OverUnder", "OverUnder", "Values"]
    franklin = raw.filter(pl.col("player_name") == "Troy Franklin")
    assert franklin.select("espn_stat", "value", "prop_source").row(0) == (
        "anytimeTouchdown", 1.0, "Values")


# --- the pooled market in the blend --------------------------------------------------

def test_market_consensus_is_the_median_of_real_books_only():
    df = pd.DataFrame({
        "MEAN_receivingYards": [40.0, 40.0],
        "BOL_receivingYards": [50.0, 40.0], "BOL_receivingYards" + IMPUTED_SUFFIX: [False, True],
        "FD_receivingYards": [54.0, 40.0], "FD_receivingYards" + IMPUTED_SUFFIX: [False, True],
        "NV_receivingYards": [52.0, 40.0], "NV_receivingYards" + IMPUTED_SUFFIX: [False, True],
    })
    out = build_market_consensus(df, ["receivingYards"], books=("BOL", "FD", "NV"),
                                 prefix="POOL")
    assert out["POOL_receivingYards"].tolist() == [52.0, 40.0]
    assert out["POOL_receivingYards" + IMPUTED_SUFFIX].tolist() == [False, True]


def test_sharp_and_square_hold_their_shares_whatever_else_is_missing():
    flag = IMPUTED_SUFFIX
    df = pd.DataFrame({
        "ESPN_receivingYards": [60.0, 60.0, 60.0, 60.0],
        "FP_receivingYards": [40.0] * 4, "FP_receivingYards" + flag: [False, True, False, False],
        "ATH_receivingYards": [50.0] * 4, "ATH_receivingYards" + flag: [False, True, False, False],
        "SHARP_receivingYards": [70.0] * 4, "SHARP_receivingYards" + flag: [False, False, True, True],
        "SQUARE_receivingYards": [80.0] * 4, "SQUARE_receivingYards" + flag: [False, False, False, True],
    })
    out = compute_weighted_stats(df, ["receivingYards"], WEEKLY_WEIGHTS, fixed=WEEKLY_FIXED_SHARES)
    rest = (60 * 0.1 + 40 * 0.2 + 50 * 0.2) / 0.5
    true = out["TRUE_receivingYards"]
    assert true[0] == pytest.approx(0.3 * 70 + 0.2 * 80 + 0.5 * rest)
    # FP and ATH missing: the books keep 30/20 and ESPN takes the rest.
    assert true[1] == pytest.approx(0.3 * 70 + 0.2 * 80 + 0.5 * 60)
    # Only the square books priced him: 20% market, not 50%.
    assert true[2] == pytest.approx(0.2 * 80 + 0.8 * rest)
    # No book: the three renormalise over the whole.
    assert true[3] == pytest.approx(rest)


def test_no_fixed_share_is_the_historical_renormalisation():
    df = pd.DataFrame({"ESPN_x": [10.0], "FP_x": [20.0], "FP_x" + IMPUTED_SUFFIX: [True]})
    weights = {"default": {"ESPN": 0.25, "FP": 0.25}}
    assert compute_weighted_stats(df.copy(), ["x"], weights)["TRUE_x"][0] == 10.0
    assert compute_weighted_stats(df.copy(), ["x"], weights, fixed={})["TRUE_x"][0] == 10.0


def test_books_are_withdrawn_only_for_a_player_espn_rules_out(monkeypatch):
    from Scripts import projection_utils as pu
    import Scripts.season_projections as sp

    monkeypatch.setattr(sp, "load_espn_injuries", lambda season: pd.DataFrame({
        "name_key": ["TREVEYON HENDERSON", "HEALTHY ZERO", "QUESTIONABLE STARTER"],
        "status": ["Questionable", "Active", "Questionable"]}))
    df = pd.DataFrame({
        "week": [6, 6, 6, 5],
        "player_name": ["TreVeyon Henderson", "Healthy Zero", "Questionable Starter",
                        "TreVeyon Henderson"],
        "projPoints": [0.0, 0.0, 12.0, 0.0],
        "BOL_rushingTouchdowns": [0.5, 0.5, 0.5, 0.5],
        "BOL_rushingTouchdowns" + IMPUTED_SUFFIX: [False] * 4,
    })
    out = pu.withdraw_books_for_unavailable(df, 2026, current_week=6)
    assert out["BOL_rushingTouchdowns" + IMPUTED_SUFFIX].tolist() == [True, False, False, False]
