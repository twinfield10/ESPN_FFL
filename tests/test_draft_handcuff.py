"""Backfield rank and handcuff value on the draft board.

The one piece of the game-script narrative that survived measurement. Plan 21 found
that a strong team's number-two back gets ~19 more carries than a weak team's while
RB1 stays flat, so a handcuff is worth more on a good team -- and nothing else on the
board said so.

What is pinned here is mostly restraint. The effect is small (R-squared 0.030 against
a residual standard deviation of 36 carries), it applies to exactly one player per
team, and it must not leak onto anyone else. A column that quietly attached itself to
starters, to non-backs, or to free agents would read as a projection rather than as
the tiebreaker it is.

Synthetic frames. No network.
"""

import polars as pl
import pytest

from Scripts.draft import handcuff as hc


def schedule_rows(rows):
    """A schedule in the shape ``load_schedules`` returns."""
    return pl.DataFrame(
        [{"season": s, "game_type": "REG", "home_team": h, "away_team": a,
          "spread_line": sp} for s, h, a, sp in rows],
        schema={"season": pl.Int64, "game_type": pl.String, "home_team": pl.String,
                "away_team": pl.String, "spread_line": pl.Float64})


# --- team strength --------------------------------------------------------

def test_strength_is_signed_from_each_team_s_own_view():
    """One spread per game, stored for the home team, so each game contributes twice
    with opposite signs."""
    strength = hc.team_strength(2026, schedule_rows([(2026, "KC", "ARI", 7.0)]))
    got = dict(zip(strength["pro_team"], strength["team_strength"]))
    assert got["KC"] == pytest.approx(7.0)
    assert got["ARI"] == pytest.approx(-7.0)


def test_the_league_average_strength_is_zero_on_a_balanced_schedule():
    """Every game has a favourite and an underdog, so the spreads cancel. A sign error
    would show up here and nowhere else.

    Balanced deliberately -- one game each. These are *mean* spreads, so they only sum
    to zero when every team plays the same number of games, which a real schedule
    guarantees and a synthetic one does not. Measured on the real 2026 schedule the
    sum is 0.117, off zero only because a handful of games carry no line."""
    strength = hc.team_strength(2026, schedule_rows([
        (2026, "KC", "ARI", 7.0), (2026, "SF", "NYJ", 3.5)]))
    assert strength["team_strength"].sum() == pytest.approx(0.0)


def test_the_two_renamed_teams_are_mapped():
    """The schedule says LA and WAS; the board says LAR and WSH. Unmapped, they
    would silently drop out of the join with a null strength."""
    strength = hc.team_strength(2026, schedule_rows([(2026, "LA", "WAS", 3.0)]))
    assert set(strength["pro_team"]) == {"LAR", "WSH"}


def test_an_unpriced_season_yields_nothing():
    assert hc.team_strength(1999, schedule_rows([(2026, "KC", "ARI", 7.0)])).is_empty()


def test_playoff_games_are_excluded():
    frame = schedule_rows([(2026, "KC", "ARI", 7.0)]).with_columns(
        pl.lit("POST").alias("game_type"))
    assert hc.team_strength(2026, frame).is_empty()


# --- attaching to a board -------------------------------------------------

def board_rows(rows):
    return pl.DataFrame(
        [{"player_name": n, "pro_team": t, "primaryPosition": p, "TRUE_Points": v}
         for n, t, p, v in rows],
        schema={"player_name": pl.String, "pro_team": pl.String,
                "primaryPosition": pl.String, "TRUE_Points": pl.Float64})


def attach(board, monkeypatch, strength=None, fit=(94.5, 1.65, 0.03, 36.5)):
    monkeypatch.setattr(hc, "team_strength",
                        lambda season, schedules=None: strength if strength is not None
                        else pl.DataFrame({"pro_team": ["KC", "ARI"],
                                           "team_strength": [7.0, -7.0]}))
    monkeypatch.setattr(hc, "fit_rb2_carries", lambda *a, **k: fit)
    return hc.attach_handcuff(board, 2026)


def test_only_the_second_back_gets_a_handcuff_value(monkeypatch):
    out = attach(board_rows([
        ("Starter", "KC", "RB", 200.0), ("Backup", "KC", "RB", 90.0),
        ("Third", "KC", "RB", 40.0)]), monkeypatch)
    got = dict(zip(out["player_name"], out["handcuff_carries"]))
    assert got["Starter"] is None
    assert got["Backup"] is not None
    assert got["Third"] is None


def test_a_strong_team_s_handcuff_is_worth_more(monkeypatch):
    out = attach(board_rows([
        ("KC1", "KC", "RB", 200.0), ("KC2", "KC", "RB", 90.0),
        ("AZ1", "ARI", "RB", 200.0), ("AZ2", "ARI", "RB", 90.0)]), monkeypatch)
    got = dict(zip(out["player_name"], out["handcuff_premium"]))
    assert got["KC2"] > 0 > got["AZ2"]
    # 1.65 carries per point of spread, over a 14-point strength gap.
    assert got["KC2"] - got["AZ2"] == pytest.approx(1.65 * 14.0)


def test_non_backs_never_get_one(monkeypatch):
    out = attach(board_rows([
        ("WR1", "KC", "WR", 200.0), ("WR2", "KC", "WR", 90.0),
        ("RB1", "KC", "RB", 150.0), ("RB2", "KC", "RB", 80.0)]), monkeypatch)
    got = dict(zip(out["player_name"], out["handcuff_carries"]))
    assert got["WR2"] is None
    assert got["RB2"] is not None


def test_free_agents_are_not_a_thirty_third_backfield(monkeypatch):
    """ESPN gives an unrostered player a `pro_team` of the literal string "None",
    which otherwise groups every free agent into a phantom team with its own RB1 and
    RB2 -- a handcuff to a team that does not exist."""
    out = attach(board_rows([
        ("KC1", "KC", "RB", 200.0), ("KC2", "KC", "RB", 90.0),
        ("FA1", "None", "RB", 50.0), ("FA2", "None", "RB", 40.0)]), monkeypatch)
    got = dict(zip(out["player_name"], out["backfield_rank"]))
    assert got["FA1"] is None and got["FA2"] is None
    assert got["KC2"] == hc.HANDCUFF_RANK


def test_an_unprojected_back_cannot_take_the_handcuff_slot(monkeypatch):
    """Ranking a null-points back above a projected one would name the wrong player."""
    out = attach(board_rows([
        ("Starter", "KC", "RB", 200.0), ("Unknown", "KC", "RB", None),
        ("Backup", "KC", "RB", 90.0)]), monkeypatch)
    got = dict(zip(out["player_name"], out["handcuff_carries"]))
    assert got["Backup"] is not None
    assert got["Unknown"] is None


def test_the_board_carries_the_r_squared_so_the_column_cannot_oversell_itself(monkeypatch):
    """Strength explains 3% of RB2 carry variance against a 36-carry residual. A
    reader who sees +10 carries without that context will price it as a projection."""
    out = attach(board_rows([("KC1", "KC", "RB", 200.0),
                             ("KC2", "KC", "RB", 90.0)]), monkeypatch)
    assert out["handcuff_r2"][0] == pytest.approx(0.03)
    assert out["handcuff_r2"][0] < 0.1


def test_a_missing_fit_leaves_the_board_alone(monkeypatch):
    """No columns beats invented ones."""
    board = board_rows([("KC1", "KC", "RB", 200.0), ("KC2", "KC", "RB", 90.0)])
    out = attach(board, monkeypatch, fit=None)
    assert "handcuff_carries" not in out.columns
    assert out.height == board.height


def test_a_board_without_the_needed_columns_is_returned_unchanged(monkeypatch):
    board = pl.DataFrame({"player_name": ["x"]})
    assert hc.attach_handcuff(board, 2026).equals(board)
