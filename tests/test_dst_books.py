"""Each book's D/ST line: the D/ST model priced off that book's own game lines (plan 51).

Four groups, each pinning something that was wrong or easy to get wrong while building it.

**The spread's sign.** The store keys a spread on the home team's handicap (-3.0 = home
favoured), the opposite of nflverse's ``spread_line``. Getting it backwards is silent:
every favourite prices as an underdog and the output still looks like a D/ST line.

**The latest snapshot, full game only.** Each value a line has carried is its own stored
row, and Pinnacle quotes first-half team totals under the same title. Averaging either in
put Arizona at 15.0 points on a 43.5 total.

**Per-game coefficients for a per-game projection.** Points allowed runs 1.347 per point
of implied points allowed across seasons but 0.996 across games, so the weekly path must
not reuse the season fit.

**One fumble touchdown, paid once.** ESPN books D/ST fumble touchdowns under
``fumbleReturnTouchdowns`` only; eight leagues price ``fumbleRecoveredForTD`` too.

The odds store is faked; the 2026 schedule and team-name table are read from disk, as the
existing vegas tests do.
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from Scripts import vegas
from Scripts.books import store
from Scripts.dst import books as dst_books
from Scripts.dst import model as dm
import Scripts.projection_utils as pu

#: A real 2026 week-1 game, so the schedule join has something to find.
HOME, AWAY = "Seattle Seahawks", "New England Patriots"
DAY = "2026-09-09"


def row(title, side, line, ts, period="GAME", side_of=None, day=DAY, alt=False,
        book="Pinnacle"):
    return {"sportsbook": book, "officialDate": day, "Home": HOME, "Away": AWAY,
            "marketTitle": title, "gamePeriod": period, "betSide": side,
            "sideOf": side_of, "marketLine": line, "isAlt": alt, "snapshot_ts": ts,
            "propType": None}


def fake_store(monkeypatch, rows):
    frame = pl.DataFrame(rows)
    monkeypatch.setattr(store, "read_current", lambda season, book=None: frame)


def test_a_home_favourite_is_a_positive_margin_for_home_and_negative_for_away(monkeypatch):
    fake_store(monkeypatch, [
        row("Spread", "home", -3.0, "2026-09-08T06:00:00"),
        row("Spread", "away", -3.0, "2026-09-08T06:00:00"),
        row("Total", "over", 44.5, "2026-09-08T06:00:00"),
    ])
    g = vegas.book_game_lines(2026, "Pinnacle")
    sea = g.filter(pl.col("team") == "SEA").row(0, named=True)
    ne = g.filter(pl.col("team") == "NE").row(0, named=True)
    assert sea["margin"] == 3.0 and ne["margin"] == -3.0
    assert sea["implied_allowed"] == pytest.approx(44.5 / 2 - 1.5)
    assert sea["week"] == 1 and sea["implied_source"] == "derived"


def test_the_latest_snapshot_is_the_books_number(monkeypatch):
    fake_store(monkeypatch, [
        row("Spread", "home", -2.5, "2026-09-07T06:00:00"),
        row("Spread", "home", -3.5, "2026-09-09T06:00:00"),
        row("Spread", "home", -3.0, "2026-09-08T06:00:00"),
        row("Total", "over", 45.5, "2026-09-07T06:00:00"),
        row("Total", "over", 44.0, "2026-09-09T06:00:00"),
    ])
    sea = vegas.book_game_lines(2026, "Pinnacle").filter(pl.col("team") == "SEA")
    assert sea["margin"].item() == 3.5
    assert sea["total_line"].item() == 44.0


def test_a_preseason_game_under_the_same_names_lands_on_no_week(monkeypatch):
    fake_store(monkeypatch, [
        row("Spread", "home", -3.0, "2026-08-20T06:00:00", day="2026-08-21"),
        row("Total", "over", 40.5, "2026-08-20T06:00:00", day="2026-08-21"),
    ])
    assert vegas.book_game_lines(2026, "Pinnacle").is_empty()


def test_team_totals_ignore_the_first_half_and_take_the_latest_quote(monkeypatch):
    fake_store(monkeypatch, [
        row("TeamTotal", "over", 23.5, "2026-09-07T06:00:00", side_of="home"),
        row("TeamTotal", "over", 24.5, "2026-09-09T06:00:00", side_of="home"),
        row("TeamTotal", "over", 11.5, "2026-09-09T06:00:00", side_of="home", period="H1"),
        row("TeamTotal", "over", 20.5, "2026-09-09T06:00:00", side_of="away"),
        row("TeamTotal", "over", 9.5, "2026-09-09T06:00:00", side_of="away", period="H1"),
    ])
    q = vegas.book_team_totals(2026, "Pinnacle")
    sea = q.filter(pl.col("team") == "SEA").row(0, named=True)
    assert sea["quoted_own"] == 24.5
    assert sea["quoted_allowed"] == 20.5


# --- the model at game grain ----------------------------------------------

def model():
    """A minimal fitted-model dict whose two grains differ visibly."""
    resid = list(np.linspace(-10, 10, 41))
    yresid = list(np.linspace(-120, 120, 41))
    rates = {c: {"beta": [1.0, 0.0, 0.0], "shrunk_to_mean": False} for c in dm.RATES}
    rates["def_tds"] = {"beta": [0.1, 0.0, 0.0], "shrunk_to_mean": True}
    season_tiers = {"points_allowed": {"beta": [-8.0, 1.35, 0.0], "residuals": resid},
                    "yards_allowed": {"beta": [15.0, 14.5, 0.0], "residuals": yresid}}
    game_tiers = {"points_allowed": {"beta": [0.0, 1.0, 0.0], "residuals": resid},
                  "yards_allowed": {"beta": [100.0, 10.75, 0.0], "residuals": yresid}}
    return {"rates": rates, "tiers": season_tiers, "int_td_share": 0.6,
            "games": {"rates": rates, "tiers": game_tiers}}


def test_a_single_game_reads_the_per_game_coefficients():
    lines = pl.DataFrame({"season": [2026], "week": [4], "team": ["MIN"],
                          "implied_allowed": [14.75], "margin": [10.0]})
    out = dm.project_games(lines, model()).row(0, named=True)
    assert out["proj_defensivePointsAllowed"] == pytest.approx(14.75)
    season = dm.components(np.array([14.75]), np.array([10.0]), model(), 1.0)
    assert season["defensivePointsAllowed"][0] == pytest.approx(-8.0 + 1.35 * 14.75)


def test_one_game_tier_probabilities_sum_to_one():
    lines = pl.DataFrame({"season": [2026] * 2, "week": [4] * 2, "team": ["MIN", "MIA"],
                          "implied_allowed": [14.75, 24.75], "margin": [10.0, -10.0]})
    out = dm.project_games(lines, model())
    pa = sum(out[f"proj_defensive{t}"] for t, _, _ in dm.PA_TIERS)
    yd = sum(out[f"proj_defensive{t}"] for t, _, _ in dm.YD_TIERS)
    assert np.allclose(pa.to_numpy(), 1.0) and np.allclose(yd.to_numpy(), 1.0)


def test_a_fumble_touchdown_is_projected_under_one_name_only():
    vec = dm.components(np.array([20.0]), np.array([0.0]), model(), 17.0)
    assert vec["fumbleRecoveredForTD"][0] == 0.0
    assert vec["fumbleReturnTouchdowns"][0] > 0.0


# --- the seam into the weekly blend ---------------------------------------

def test_book_dst_rows_land_under_espn_names_and_keep_only_blended_stats(monkeypatch):
    weekly = pl.DataFrame({"season": [2026, 2026], "week": [4, 4], "team": ["LA", "WAS"],
                           "implied_source": ["quoted", "derived"],
                           "proj_defensiveSacks": [2.5, 2.0],
                           "proj_defensiveSoloTackles": [40.0, 38.0]})
    monkeypatch.setattr(dst_books, "weekly", lambda season, prefix, model=None: weekly)
    df = pd.DataFrame({"week": [4, 4, 4], "primaryPosition": ["D/ST", "D/ST", "WR"],
                       "pro_team": ["LAR", "WSH", "LAR"],
                       "player_name": ["Rams D/ST", "Commanders D/ST", "Puka Nacua"]})
    out = pu.book_dst_rows(df, 2026, "PINNY", ["defensiveSacks"])
    assert dict(zip(out["player_name"], out["proj_defensiveSacks"])) == {
        "Rams D/ST": 2.5, "Commanders D/ST": 2.0}
    assert "proj_defensiveSoloTackles" not in out.columns


def test_a_missing_model_degrades_to_no_rows(monkeypatch):
    def missing(season, prefix, model=None):
        raise FileNotFoundError("No D/ST model")
    monkeypatch.setattr(dst_books, "weekly", missing)
    df = pd.DataFrame({"week": [4], "primaryPosition": ["D/ST"], "pro_team": ["LAR"],
                       "player_name": ["Rams D/ST"]})
    assert pu.book_dst_rows(df, 2026, "BOL", ["defensiveSacks"]).empty
