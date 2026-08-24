"""Scoring the pre-season depth chart against the one the season revealed.

Three things are worth pinning, and they are the three that decide whether the
calibration is a finding or an artefact.

**The injury guard.** A starter hurt in week 1 has one game of near-zero opportunity.
Without :data:`MIN_DERIVED_GAMES` he ranks behind his own backup and the pre-season
chart is scored as wrong about a player it had exactly right -- which would bias the
whole table toward "the chart is bad" for reasons that have nothing to do with the
chart. The guard has to drop him *before* ranking, not after, or he still pushes a
team-mate down a rank on his way out.

**Ties are not an ordering.** Two backs splitting a room evenly are both rank 1. Using
``ordinal`` would assert an order the games did not reveal and score the chart against
a coin flip.

**The cohort split is the finding.** Pooled, the chart looks like a 52%-accurate
signal and that number describes nobody -- it is a settled veteran's 59% and a
rookie's 36% averaged into a figure neither of them has.

Synthetic frames throughout. No network, no parquet.
"""

import polars as pl
import pytest

from Scripts.usage import role


def weekly(rows):
    """Player-weeks from ``(gsis_id, week, team, position, stat_value)``."""
    return pl.DataFrame({
        "gsis_id": [r[0] for r in rows],
        "week": [r[1] for r in rows],
        "team": [r[2] for r in rows],
        "position": [r[3] for r in rows],
        "targets": [float(r[4]) for r in rows],
        "carries": [0.0] * len(rows),
        "attempts": [0.0] * len(rows),
        "season": [2024] * len(rows),
    })


@pytest.fixture
def fake_weeks(monkeypatch):
    """Feed :func:`role.derived_chart` a synthetic season."""
    def install(rows):
        monkeypatch.setattr(role.ft, "load_player_weeks",
                            lambda seasons, **kw: weekly(rows))
    return install


# --- the derived chart ---------------------------------------------------

def test_the_room_is_ranked_by_per_game_opportunity(fake_weeks):
    fake_weeks([("wr1", w, "SEA", "WR", 9) for w in (1, 2, 3)]
               + [("wr2", w, "SEA", "WR", 5) for w in (1, 2, 3)]
               + [("wr3", w, "SEA", "WR", 1) for w in (1, 2, 3)])
    out = role.derived_chart(2024)
    ranks = dict(zip(out["gsis_id"], out["true_rank"]))
    assert (ranks["wr1"], ranks["wr2"], ranks["wr3"]) == (1, 2, 3)


def test_only_the_first_three_games_count(fake_weeks):
    """Week 4 onward is a different question -- a chart that takes five weeks to
    reveal itself has stopped describing the pre-season."""
    fake_weeks([("a", 1, "SEA", "WR", 9), ("a", 2, "SEA", "WR", 9),
                ("b", 1, "SEA", "WR", 1), ("b", 2, "SEA", "WR", 1),
                ("b", 4, "SEA", "WR", 99), ("b", 5, "SEA", "WR", 99)])
    out = role.derived_chart(2024)
    ranks = dict(zip(out["gsis_id"], out["true_rank"]))
    assert ranks["a"] == 1 and ranks["b"] == 2


def test_a_week_one_injury_does_not_read_as_a_demotion(fake_weeks):
    """The starter plays once and is dropped, rather than ranked behind his backup."""
    fake_weeks([("starter", 1, "SEA", "WR", 10),
                ("backup", 1, "SEA", "WR", 2), ("backup", 2, "SEA", "WR", 8),
                ("backup", 3, "SEA", "WR", 8)])
    out = role.derived_chart(2024)
    assert out["gsis_id"].to_list() == ["backup"]


def test_the_short_player_is_dropped_before_ranking_not_after(fake_weeks):
    """Dropping after would leave him occupying rank 1 and push the real starter to 2.

    The bug this pins is silent: the table still builds, and every team with an
    early injury reports its actual starter as a listed-1-turned-true-2.
    """
    fake_weeks([("hurt", 1, "SEA", "WR", 99),
                ("real", 1, "SEA", "WR", 8), ("real", 2, "SEA", "WR", 8),
                ("other", 1, "SEA", "WR", 2), ("other", 2, "SEA", "WR", 2)])
    out = role.derived_chart(2024)
    ranks = dict(zip(out["gsis_id"], out["true_rank"]))
    assert ranks == {"real": 1, "other": 2}


def test_players_split_evenly_share_a_rank(fake_weeks):
    """Ties, not an ordering the games did not reveal."""
    fake_weeks([(p, w, "SEA", "WR", 6) for p in ("a", "b") for w in (1, 2)])
    out = role.derived_chart(2024)
    assert out["true_rank"].to_list() == [1, 1]


def test_ranking_is_within_the_team(fake_weeks):
    """A depth chart is a team's, so a weak starter still outranks nobody else's."""
    fake_weeks([("sea", w, "SEA", "WR", 3) for w in (1, 2)]
               + [("buf", w, "BUF", "WR", 12) for w in (1, 2)])
    out = role.derived_chart(2024)
    assert set(out["true_rank"].to_list()) == {1}


def test_rank_is_clipped_to_the_scale_the_chart_uses(fake_weeks):
    """`depth_rank` is 1-3, so `true_rank` must be, or they are not comparable."""
    fake_weeks([(f"wr{i}", w, "SEA", "WR", 10 - i)
                for i in range(6) for w in (1, 2)])
    out = role.derived_chart(2024)
    assert out["true_rank"].max() == role.ctx.MAX_DEPTH_RANK


def test_a_season_with_no_usable_rows_returns_a_typed_empty_frame(fake_weeks):
    fake_weeks([("a", 1, "SEA", "WR", 5)])
    out = role.derived_chart(2024)
    assert out.height == 0
    assert {"gsis_id", "true_rank", "season"} <= set(out.columns)


# --- the cohort split ----------------------------------------------------

def test_a_rookie_is_a_rookie_rather_than_a_mover():
    """He has no prior team, so `team_changed` is not a fact about him."""
    frame = pl.DataFrame({"is_rookie": [True, False, False],
                          "team_changed": [True, True, False]})
    assert (frame.with_columns(role.cohort_expression())["cohort"].to_list()
            == ["rookie", "mover", "settled"])


# --- the calibration table -----------------------------------------------

def scored(rows):
    """From ``(cohort, depth_rank, true_rank)``."""
    return pl.DataFrame({
        "season": [2024] * len(rows), "gsis_id": [str(i) for i in range(len(rows))],
        "position": ["WR"] * len(rows),
        "cohort": [r[0] for r in rows],
        "depth_rank": [r[1] for r in rows],
        "true_rank": [r[2] for r in rows]},
        schema_overrides={"depth_rank": pl.Int32, "true_rank": pl.Int32})


def test_accuracy_is_the_diagonal():
    frame = scored([("settled", 1, 1), ("settled", 1, 1),
                    ("settled", 1, 2), ("settled", 1, 3)])
    out = role.calibration(frame)
    assert out["accuracy"][0] == pytest.approx(0.5)
    assert out["p_true_2"][0] == pytest.approx(0.25)


def test_the_row_is_a_distribution_and_sums_to_one():
    """Phase 3 draws from the whole row, so it has to be one."""
    frame = scored([("mover", 2, t) for t in (1, 1, 2, 3)])
    out = role.calibration(frame)
    total = sum(out[f"p_true_{r}"][0] for r in (1, 2, 3))
    assert total == pytest.approx(1.0)


def test_each_cohort_gets_its_own_row():
    """The split is the finding -- pooling hides a 23-point gap."""
    frame = scored([("settled", 1, 1)] * 3 + [("rookie", 1, 3)] * 3)
    out = role.calibration(frame)
    by_cohort = dict(zip(out["cohort"], out["accuracy"]))
    assert by_cohort["settled"] == pytest.approx(1.0)
    assert by_cohort["rookie"] == pytest.approx(0.0)


def test_an_empty_frame_calibrates_to_an_empty_table():
    out = role.calibration(scored([]).head(0))
    assert out.height == 0
    assert "accuracy" in out.columns
