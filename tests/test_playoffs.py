"""Playoff seeding and bracket -- each rule pinned by the ESPN case that taught it.

The seeding in :mod:`Scripts.playoffs` was checked against ESPN's own ``playoffSeed``
for every team in 36 league-seasons (all nine leagues, 2019-2026, Jeffs 2026 aside
because ESPN's records for it start at week 10) and matched all of them. The cases
below are the shapes that discriminated between candidate rules, reduced to a few
teams each.
"""

import polars as pl
import pytest

from Scripts import playoffs as po


def games(*results):
    """``(a, b, score_a, score_b)`` per game -> the two-rows-per-game frame."""
    rows = []
    for a, b, sa, sb in results:
        rows += [(a, b, float(sa), float(sb)), (b, a, float(sb), float(sa))]
    return pl.DataFrame(rows, schema=["team", "opp", "team_score", "opp_score"], orient="row")


def settings(n, rule="TOTAL_POINTS_SCORED", **kw):
    return {"playoff_team_count": n, "seeding_rule": rule, **kw}


def order(frame):
    return frame["team"].to_list()


# --- settings ----------------------------------------------------------------

def test_settings_from_espn_reads_rules_and_playoff_weeks():
    """Winfield_Football 2026: two-week rounds show up as two weeks per period."""
    raw = {"matchupPeriodCount": 13, "playoffTeamCount": 4,
           "playoffSeedingRule": "TOTAL_POINTS_SCORED", "playoffReseed": False,
           "divisions": [{"id": 0, "name": "East", "size": 3},
                         {"id": 1, "name": "West", "size": 3}],
           "matchupPeriods": {**{str(i): [i] for i in range(1, 14)},
                              "14": [14, 15], "15": [16, 17]}}
    s = po.settings_from_espn(raw, 6)
    assert s["playoff_team_count"] == 4
    assert s["regular_season_periods"] == 13
    assert s["playoff_periods"] == [[14, [14, 15]], [15, [16, 17]]]
    assert s["divisions"] == {"0": "East", "1": "West"}
    assert s["team_count"] == 6
    assert po.round_weeks(s, 2) == [[14, 15], [16, 17]]


# --- seeding -----------------------------------------------------------------

def test_points_for_breaks_a_record_tie():
    # C 2-0; A 1-1 on 170 and B 1-1 on 185, so B takes the two seed; D 0-2.
    g = games(("A", "B", 100, 90), ("C", "D", 120, 80), ("A", "C", 70, 110),
              ("B", "D", 95, 85))
    seeds = po.seed(g, {t: "" for t in "ABCD"}, settings(2))
    assert order(seeds) == ["C", "B", "A", "D"]
    assert seeds["in_playoffs"].to_list() == [True, True, False, False]


def test_division_winners_are_seeded_above_better_wildcards():
    """John_ATL 2026: a 7-4 division winner is the two seed over an 8-3 wildcard."""
    g = games(("E1", "E2", 100, 90), ("E1", "W2", 100, 90),
              ("W2", "W1", 100, 90), ("W2", "E2", 100, 90), ("W1", "E2", 100, 90))
    div = {"E1": "East", "E2": "East", "W1": "West", "W2": "West"}
    seeds = po.seed(g, div, settings(3))
    # E1 2-0 wins East; W2 2-1 wins West; W1 1-1 is a worse record but a wildcard.
    assert order(seeds)[:2] == ["E1", "W2"]
    assert seeds["division_winner"].to_list()[:2] == [True, True]


def test_h2h_three_way_tie_matches_jeffs_order():
    """Jeffs 2026 (weeks 1-11): Jeff 3-1, Sam 2-1, Tommy 0-3 in games among three
    7-4 teams, so the league's top scorer is seeded last of the three."""
    g = games(("J", "S", 81, 98), ("J", "T", 157, 145), ("S", "T", 146, 141),
              ("J", "S", 137, 110), ("J", "T", 178, 147),
              # level everyone at 3-3, with T far ahead on points
              ("Z", "J", 50, 40), ("Z", "J", 50, 40),
              ("S", "Z", 100, 50), ("Z", "S", 50, 40), ("Z", "S", 50, 40),
              ("T", "Z", 300, 50), ("T", "Z", 300, 50), ("T", "Z", 300, 50),
              # and keep Z itself off .500, or it joins the tie
              ("Y", "Z", 60, 50), ("Y", "Z", 60, 50))
    seeds = po.seed(g, {t: "" for t in "JSTYZ"}, settings(4, "H2H_RECORD"))
    level = seeds.filter(~pl.col("team").is_in(["Y", "Z"]))
    assert level["win_pct"].n_unique() == 1, level
    assert order(level) == ["J", "S", "T"]
    assert level.filter(pl.col("team") == "T")["points_for"].item() == level["points_for"].max()
    assert level["tiebreak"][0] == "3-way tie at 3-3: Head-to-head, 3-1 vs the tied teams"


def test_h2h_falls_to_points_when_a_pair_never_met_then_rechecks_the_rest():
    """GOP 2024: four 10-4 division winners. 10 never played the others, so the top
    two go on points (14, then 10). With 10 gone, 1 and 8 *have* met -- and 8 beat
    1 -- so 8 is third despite fewer points."""
    g = games(("14", "1", 121, 104), ("8", "14", 170, 149), ("8", "1", 152, 109),
              ("8", "14", 182, 174),
              # level everyone at 3-2; points 14 > 10 > 1 > 8
              ("14", "Z", 900, 1), ("14", "Z", 900, 1),
              ("10", "Z", 600, 1), ("10", "Z", 600, 1), ("10", "Z", 600, 1),
              ("Z", "10", 2, 1), ("Z", "10", 2, 1),
              ("1", "Z", 500, 1), ("1", "Z", 500, 1), ("1", "Z", 500, 1),
              ("Z", "8", 2, 1), ("Z", "8", 2, 1))
    seeds = po.seed(g, {t: "" for t in ["14", "10", "1", "8", "Z"]},
                    settings(4, "H2H_RECORD"))
    level = seeds.filter(pl.col("team") != "Z")
    assert level["win_pct"].n_unique() == 1, level
    assert order(level) == ["14", "10", "8", "1"]
    assert "not every pair has played" in level["tiebreak"][0]


def test_h2h_circle_settled_on_points_then_the_pair_rechecked():
    """GOP 2025: 1 beat 18, 18 beat 15, 15 beat 1 -- a circle, so points pick 1. The
    pair left behind is not left in points order: 18 beat 15, so 18 is next even
    though 15 scored more."""
    g = games(("1", "18", 152, 105), ("18", "15", 138, 128), ("15", "1", 158, 141),
              ("1", "Z", 700, 1), ("15", "Z", 500, 1), ("18", "Z", 300, 1))
    seeds = po.seed(g, {t: "" for t in ["1", "15", "18", "Z"]},
                    settings(3, "H2H_RECORD"))
    assert order(seeds) == ["1", "18", "15", "Z"]


def test_points_rule_ignores_head_to_head():
    """The same circle under TOTAL_POINTS_SCORED is plain points order."""
    g = games(("1", "18", 152, 105), ("18", "15", 138, 128), ("15", "1", 158, 141),
              ("1", "Z", 700, 1), ("15", "Z", 500, 1), ("18", "Z", 300, 1))
    seeds = po.seed(g, {t: "" for t in ["1", "15", "18", "Z"]}, settings(3))
    assert order(seeds) == ["1", "15", "18", "Z"]


def test_teams_with_no_games_are_still_seeded():
    seeds = po.seed(games(("A", "B", 10, 5)), {"A": "", "B": "", "C": ""}, settings(2))
    assert order(seeds) == ["A", "B", "C"]


# --- bracket -----------------------------------------------------------------

def _pairs(rnd):
    return [(g.high.seed, g.low.seed, g.high.winner_of, g.low.winner_of) for g in rnd]


@pytest.mark.parametrize("n, expected_byes", [(4, 0), (6, 2), (7, 1), (8, 0), (1, 0)])
def test_byes_go_to_the_gap_to_a_power_of_two(n, expected_byes):
    assert po.byes(n) == expected_byes


def test_six_team_bracket_gives_the_top_two_byes():
    rounds = po.bracket(6)
    assert len(rounds) == 3
    assert {(g.high.seed, g.low.seed) for g in rounds[0]} == {(4, 5), (3, 6)}
    # 1 meets the 4/5 winner, 2 meets the 3/6 winner
    r2 = {(g.high.seed, rounds[0][g.low.winner_of].high.seed) for g in rounds[1]}
    assert r2 == {(1, 4), (2, 3)}
    assert len(rounds[2]) == 1


def test_seven_team_bracket_gives_only_the_one_seed_a_bye():
    """GOP_Degenerates: seven teams, one bye."""
    rounds = po.bracket(7)
    assert {(g.high.seed, g.low.seed) for g in rounds[0]} == {(4, 5), (2, 7), (3, 6)}
    assert any(g.high.seed == 1 for g in rounds[1])


def test_eight_and_four_team_brackets_have_no_byes():
    assert {(g.high.seed, g.low.seed) for g in po.bracket(8)[0]} == {(1, 8), (4, 5), (2, 7), (3, 6)}
    assert {(g.high.seed, g.low.seed) for g in po.bracket(4)[0]} == {(1, 4), (2, 3)}
    assert po.bracket(1) == []


def test_round_names_end_at_the_final():
    assert po.round_names(2) == ["Semifinals", "Final"]
    assert po.round_names(3) == ["Quarterfinals", "Semifinals", "Final"]
    assert po.round_names(4)[0] == "Round 1"


# --- from team_stats -----------------------------------------------------------

def _team_stats(rows):
    cols = ["year", "week", "team_owner", "opp_owner", "team_score", "opp_score",
            "is_regular_season", "team_division"]
    return pl.DataFrame(rows, schema=cols, orient="row")


def test_games_from_team_stats_keeps_only_completed_regular_season_games():
    ts = _team_stats([
        (2026.0, 1.0, "A", "B", 100.0, 90.0, True, "East"),
        (2026.0, 2.0, "A", "B", 110.0, 95.0, True, "East"),   # week in progress
        (2026.0, 3.0, "A", "B", 0.0, 0.0, True, "East"),      # unplayed 0-0
        (2026.0, 14.0, "A", "B", 99.0, 98.0, False, "East"),  # playoffs
        (2025.0, 1.0, "A", "B", 50.0, 60.0, True, "West"),    # another season
    ])
    g = po.games_from_team_stats(ts, 2026, through_week=1)
    assert g["week"].to_list() == [1]
    assert po.divisions_from_team_stats(ts, 2026) == {"A": "East"}
