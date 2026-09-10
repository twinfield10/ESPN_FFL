"""Who plays whom, and what the matchup is worth.

Covers ``app/matchup_sim.py``. The measurement lives in
``Scripts/outcomes/weekly.py`` and is tested in ``test_weekly_dispersion.py``; this
is the application of it, plus the identity resolution that decides which two lineups
are being compared at all.

The identity half exists because of a real failure: on Weenieless_Wanderers week 1
ESPN served the box-score view no owner for one team -- ``"Unknown Owner"`` playing as
``"Team 11"`` -- while the roster view had it as Stephen Touchstone's ``"Sandusky
Shower Pals"``. That team was Tommy's week 1 opponent, so matching on owner alone left
the tab empty in the league it mattered in. GOP Degenerates is the mirror image, with
the *roster* side missing the name.
"""

import math
import sys
from pathlib import Path

import pytest

# `app/` is not a package: Streamlit runs the page scripts directly with app/ on
# sys.path, so the modules import each other by bare name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import matchup_sim as sim  # noqa: E402

#: A dispersion model shaped like the fitted one, with round numbers.
#:
#: ``phi = 1`` means a player's variance equals his projection, so nine 100-point
#: starters give a team sd of 30 exactly.
MODEL = {
    "version": "1.0.0",
    "train_seasons": [2025],
    "n_rows": 100,
    "pooled": {"phi": 1.0, "inv_k": 0.0},
    "positions": {
        "RB": {"phi": 1.0, "inv_k": 0.0, "n": 100, "source": "fitted"},
        "K": {"phi": 0.0, "inv_k": 0.0, "n": 100, "source": "fitted"},
    },
}


def starter(position="RB", points=100.0):
    return {"player_position": position, "TRUE_Points": points,
            "player_name": f"{position}{points}"}


# --- identity ------------------------------------------------------------

def test_owners_that_match_exactly_pair_straight_through():
    fixtures = [{"team_owner": "A", "team_name": "AA", "opp_owner": "B",
                 "opp_name": "BB"},
                {"team_owner": "B", "team_name": "BB", "opp_owner": "A",
                 "opp_name": "AA"}]
    pairs, notes = sim.opponent_map(fixtures, [("A", "AA"), ("B", "BB")])
    assert pairs == {"A": "B", "B": "A"}
    assert notes == []


def test_a_team_whose_owner_differs_matches_on_its_name():
    """Plan 25's rule: join within a season on team_name."""
    fixtures = [{"team_owner": "A", "team_name": "AA", "opp_owner": "Renamed",
                 "opp_name": "BB"},
                {"team_owner": "Renamed", "team_name": "BB", "opp_owner": "A",
                 "opp_name": "AA"}]
    pairs, notes = sim.opponent_map(fixtures, [("A", "AA"), ("B", "BB")])
    assert pairs == {"A": "B", "B": "A"}
    assert any("team name" in note for note in notes)


def test_the_last_unmatched_team_on_each_side_is_the_same_team():
    """Weenieless week 1: neither the owner nor the name lined up, and there was
    nowhere else for either to go."""
    fixtures = [
        {"team_owner": "Tommy", "team_name": "Boobs 2",
         "opp_owner": "Unknown Owner", "opp_name": "Team 11"},
        {"team_owner": "Unknown Owner", "team_name": "Team 11",
         "opp_owner": "Tommy", "opp_name": "Boobs 2"},
    ]
    pairs, notes = sim.opponent_map(
        fixtures, [("Tommy", "Boobs 2"), ("Stephen", "Sandusky Shower Pals")])
    assert pairs == {"Tommy": "Stephen", "Stephen": "Tommy"}
    assert len(notes) == 1
    assert "Stephen" in notes[0] and "Unknown Owner" in notes[0]


def test_two_unmatched_teams_on_one_side_are_left_alone():
    """The residual inference is only safe when it is forced. Two is a guess."""
    fixtures = [{"team_owner": "X", "team_name": "XX", "opp_owner": "Y",
                 "opp_name": "YY"}]
    pairs, notes = sim.opponent_map(fixtures, [("A", "AA"), ("B", "BB")])
    assert pairs == {}
    assert notes == []


def test_a_fixture_whose_opponent_has_no_roster_is_dropped():
    fixtures = [{"team_owner": "A", "team_name": "AA", "opp_owner": "Ghost",
                 "opp_name": "GG"}]
    pairs, _ = sim.opponent_map(fixtures, [("A", "AA"), ("B", "BB"), ("C", "CC")])
    assert "A" not in pairs


# --- one side ------------------------------------------------------------

def test_a_side_totals_its_starters():
    side = sim.side("A", [starter(points=10.0), starter(points=20.0)],
                    fitted=MODEL)
    assert side.projected == pytest.approx(30.0)
    assert side.starters == 2


def test_the_team_sd_is_the_independent_sum():
    """sqrt(Σ sd²). Measured to be right -- see gate G-W4."""
    side = sim.side("A", [starter(points=100.0)] * 9, fitted=MODEL)
    assert side.sd == pytest.approx(30.0)          # sqrt(9 * 100)


def test_a_kicker_adds_its_mean_and_no_variance():
    """Plan 28 fits its distribution on the usage model, which covers QB/RB/WR/TE.

    K and D/ST still score; they just carry no spread, and `priced` says so.
    """
    side = sim.side("A", [starter(points=100.0), starter("K", 8.0)], fitted=MODEL)
    assert side.projected == pytest.approx(108.0)
    assert side.sd == pytest.approx(10.0)
    assert (side.starters, side.priced) == (2, 1)


def test_an_unknown_position_falls_back_to_the_pooled_fit():
    """IDP positions turn up 24 times in a season; a fit on that is noise."""
    side = sim.side("A", [starter("LB", 100.0)], fitted=MODEL)
    assert side.sd == pytest.approx(10.0)


def test_no_model_means_no_spread():
    side = sim.side("A", [starter(points=100.0)], fitted=None)
    assert side.sd == 0.0 and side.priced == 0


def test_the_band_is_floored_at_zero():
    """A negative floor reads as a modelling artefact, because it is one."""
    side = sim.side("A", [starter(points=1.0)], fitted=MODEL)
    assert side.band()[0] == 0.0


# --- the matchup ---------------------------------------------------------

def test_equal_teams_are_a_coin_flip():
    side = sim.side("A", [starter(points=100.0)] * 9, fitted=MODEL)
    assert sim.outcome(side, side).win == pytest.approx(0.5)


def test_the_better_projected_team_is_favoured():
    home = sim.side("A", [starter(points=110.0)] * 9, fitted=MODEL)
    away = sim.side("B", [starter(points=100.0)] * 9, fitted=MODEL)
    result = sim.outcome(home, away)
    assert result.margin == pytest.approx(90.0)
    assert result.win > 0.9


def test_the_same_margin_is_worth_less_when_the_spread_is_wider():
    """The reason a projected margin is not a decision on its own."""
    tight = {"pooled": {"phi": 0.01, "inv_k": 0.0}, "positions": {}}
    wide = {"pooled": {"phi": 10.0, "inv_k": 0.0}, "positions": {}}
    def probability(model):
        home = sim.side("A", [starter(points=105.0)], fitted=model)
        away = sim.side("B", [starter(points=100.0)], fitted=model)
        return sim.outcome(home, away).win
    assert probability(tight) > probability(wide) > 0.5


def test_no_spread_reports_the_margin_and_no_probability():
    """Reported rather than papered over with a 50%."""
    home = sim.side("A", [starter(points=110.0)], fitted=None)
    away = sim.side("B", [starter(points=100.0)], fitted=None)
    result = sim.outcome(home, away)
    assert result.win is None
    assert result.margin == pytest.approx(10.0)


def test_the_probability_is_symmetric():
    home = sim.side("A", [starter(points=110.0)] * 9, fitted=MODEL)
    away = sim.side("B", [starter(points=100.0)] * 9, fitted=MODEL)
    assert (sim.outcome(home, away).win
            + sim.outcome(away, home).win) == pytest.approx(1.0)


# --- the swing -----------------------------------------------------------

def test_points_gained_move_the_probability_upward():
    home = sim.side("A", [starter(points=100.0)] * 9, fitted=MODEL)
    away = sim.side("B", [starter(points=100.0)] * 9, fitted=MODEL)
    assert sim.swing(home, away, 10.0) > 0


def test_the_same_points_are_worth_more_in_a_close_matchup():
    """Which is why the Matchup tab reads the swing rather than the points."""
    home = sim.side("A", [starter(points=100.0)] * 9, fitted=MODEL)
    close = sim.side("B", [starter(points=100.0)] * 9, fitted=MODEL)
    blowout = sim.side("C", [starter(points=10.0)] * 9, fitted=MODEL)
    assert sim.swing(home, close, 10.0) > sim.swing(home, blowout, 10.0)


def test_no_model_means_no_swing():
    home = sim.side("A", [starter(points=100.0)], fitted=None)
    assert sim.swing(home, home, 10.0) is None


# --- the caption ---------------------------------------------------------

def test_a_missing_model_says_so_rather_than_showing_a_number():
    note = sim.gate_note(None)
    assert note["kind"] == "missing"
    assert "Scripts.outcomes.weekly" in note["text"]


def test_a_fitted_model_names_what_it_was_fitted_on():
    note = sim.gate_note(MODEL)
    assert note["kind"] == "ok"
    assert "2025" in note["text"]


# --- what has already happened has no spread left ------------------------
#
# The change that makes a Sunday-night win probability mean anything. The spread is
# computed from `LIVE_remaining_points` rather than from the total, so a player whose
# game is over contributes his points and none of his uncertainty. Read off the total
# instead, a team that had banked 140 points with every game final would still be
# quoted at roughly 70% rather than 100%.


def played(position="RB", points=100.0):
    """A starter whose game is over: points banked, nothing left to come."""
    return {**starter(position, points), "LIVE_remaining_points": 0.0}


def midway(position="RB", points=100.0, remaining=50.0):
    """A starter halfway through his game."""
    return {**starter(position, points), "LIVE_remaining_points": remaining}


def test_a_finished_starter_has_no_variance():
    side = sim.side("A", [played()], fitted=MODEL)
    assert side.projected == pytest.approx(100.0)
    assert side.sd == 0.0


def test_a_lineup_of_finished_starters_is_a_certainty():
    home = sim.side("A", [played(points=20.0)] * 9, fitted=MODEL)
    away = sim.side("B", [played(points=15.0)] * 9, fitted=MODEL)
    result = sim.outcome(home, away)
    assert result.margin == pytest.approx(45.0)
    assert result.win == pytest.approx(1.0)


def test_the_losing_side_of_a_finished_week_is_a_certainty_too():
    home = sim.side("A", [played(points=15.0)] * 9, fitted=MODEL)
    away = sim.side("B", [played(points=20.0)] * 9, fitted=MODEL)
    assert sim.outcome(home, away).win == pytest.approx(0.0)


def test_a_midway_starter_keeps_the_spread_of_what_is_left():
    """`phi = 1.0` at RB in this model, so sd is sqrt of the remaining projection --
    the full 10.0 before kickoff and sqrt(50) halfway through."""
    assert sim.side("A", [midway(remaining=50.0)], fitted=MODEL).sd == pytest.approx(
        50.0 ** 0.5)
    assert sim.side("A", [midway(remaining=100.0)], fitted=MODEL).sd == pytest.approx(
        100.0 ** 0.5)


def test_a_row_with_no_remaining_column_falls_back_to_the_total():
    """A store written before live scoring. The shipped coverage of 0.802 describes
    this path, so it must not move."""
    with_column = sim.side("A", [midway(remaining=100.0)], fitted=MODEL)
    without = sim.side("A", [starter(points=100.0)], fitted=MODEL)
    assert without.sd == pytest.approx(with_column.sd)
    assert without.priced == with_column.priced == 1


def test_a_finished_starter_is_not_counted_as_unpriced():
    """`priced` counts players the dispersion model had something to say about. A
    finished player legitimately has sd 0, which is a statement rather than a gap --
    but it does read as unpriced, and the caption that surfaces `priced` should be
    read with that in mind."""
    side = sim.side("A", [played(), midway(remaining=100.0)], fitted=MODEL)
    assert side.starters == 2
    assert side.priced == 1
