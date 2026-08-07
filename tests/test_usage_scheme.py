"""Team usage profiles and coach priors: shares, shrinkage, and the as-of boundary.

The boundary is the property worth pinning hardest. A coach prior that includes the
season being predicted leaks the outcome into the feature *for every player on that
team at once*, which is both the largest leak available here and the least visible.

Synthetic frames throughout. No network, no parquet.
"""

import polars as pl
import pytest

from Scripts.usage import scheme as sc


def weeks(rows):
    """A player_weeks frame from ``(season, team, week, position, car, att, tgt)``."""
    return pl.DataFrame({
        "season": [r[0] for r in rows],
        "team": [r[1] for r in rows],
        "week": [r[2] for r in rows],
        "position": [r[3] for r in rows],
        "gsis_id": [f"{r[3]}{i}" for i, r in enumerate(rows)],
        "carries": [float(r[4]) for r in rows],
        "attempts": [float(r[5]) for r in rows],
        "targets": [float(r[6]) for r in rows],
    })


def simple_team(season, team, week, rb_car=20, qb_car=2, rb_tgt=5, wr_tgt=20,
                te_tgt=5, att=30):
    """One team-week: a back, a quarterback, a receiver and a tight end."""
    return [
        (season, team, week, "RB", rb_car, 0, rb_tgt),
        (season, team, week, "QB", qb_car, att, 0),
        (season, team, week, "WR", 0, 0, wr_tgt),
        (season, team, week, "TE", 0, 0, te_tgt),
    ]


def staff_frame(rows):
    """A coaching table from ``(season, team, coach)``."""
    return pl.DataFrame({
        "season": pl.Series([r[0] for r in rows], dtype=pl.Int32),
        "team": [r[1] for r in rows],
        "head_coach": [r[2] for r in rows],
    })


# --- the profile ---------------------------------------------------------

def test_shares_are_shares_not_totals():
    """A team that ran more plays must not look like a team that favoured a
    position."""
    busy = simple_team(2025, "AAA", 1) + simple_team(2025, "AAA", 2)
    quiet = simple_team(2025, "BBB", 1)
    profile = sc.team_profile(weeks(busy + quiet)).sort("team")
    shares = profile.select("team", "rb_carry_share", "rb_target_share")
    assert shares["rb_carry_share"].to_list() == pytest.approx([20 / 22, 20 / 22])
    assert shares["rb_target_share"].to_list() == pytest.approx([5 / 30, 5 / 30])


def test_per_game_rates_use_the_teams_own_games():
    profile = sc.team_profile(weeks(simple_team(2025, "AAA", 1)
                                    + simple_team(2025, "AAA", 2)))
    row = profile.row(0, named=True)
    assert row["team_games"] == 2
    assert row["team_rb_carries_pg"] == pytest.approx(20.0)
    assert row["plays_pg"] == pytest.approx(52.0)     # 22 carries + 30 attempts


def test_pass_rate_is_attempts_over_plays():
    profile = sc.team_profile(weeks(simple_team(2025, "AAA", 1, rb_car=10,
                                                qb_car=0, att=30)))
    assert profile["pass_rate"][0] == pytest.approx(30 / 40)


def test_group_volumes_are_the_pool_a_player_competes_for():
    """team_rb_carries_pg is what a rookie back is drafted into, which is the thing
    draft capital alone cannot see."""
    profile = sc.team_profile(weeks(simple_team(2025, "AAA", 1, rb_car=25)))
    assert profile["team_rb_carries_pg"][0] == pytest.approx(25.0)


def test_a_frame_without_team_is_refused():
    frame = weeks(simple_team(2025, "AAA", 1)).drop("team")
    with pytest.raises(KeyError, match="team_profile needs"):
        sc.team_profile(frame)


def test_a_position_absent_from_the_frame_reads_as_zero_not_null():
    """Otherwise every share built from it goes null and the team drops out of the
    join silently."""
    rows = [(2025, "AAA", 1, "RB", 20, 0, 5), (2025, "AAA", 1, "QB", 0, 30, 0)]
    profile = sc.team_profile(weeks(rows))
    assert profile["te_target_share"][0] == pytest.approx(0.0)
    assert profile["team_te_targets_pg"][0] == pytest.approx(0.0)


# --- the as-of boundary --------------------------------------------------

def test_a_coach_prior_never_sees_the_predicted_season():
    """The leak would apply to every player on the team at once."""
    rows = (simple_team(2024, "AAA", 1, rb_tgt=5, wr_tgt=20)
            + simple_team(2025, "AAA", 1, rb_tgt=25, wr_tgt=0))
    profile = sc.team_profile(weeks(rows))
    staff = staff_frame([(2024, "AAA", "Coach"), (2025, "AAA", "Coach")])

    prior = sc.coach_prior(profile, staff, target_season=2025)
    # 2024 only: 5 of 30 targets to the back. Had 2025 leaked, it would be far higher.
    row = prior.row(0, named=True)
    assert row["coach_seasons"] == 1
    assert row["coach_rb_target_share"] < 0.2


def test_a_team_prior_never_sees_the_predicted_season():
    rows = (simple_team(2024, "AAA", 1, rb_car=10)
            + simple_team(2025, "AAA", 1, rb_car=30))
    profile = sc.team_profile(weeks(rows))
    prior = sc.team_prior(profile, target_season=2025)
    assert prior["team_prior_team_rb_carries_pg"][0] == pytest.approx(10.0)


def test_an_empty_history_yields_an_empty_prior_not_an_error():
    profile = sc.team_profile(weeks(simple_team(2025, "AAA", 1)))
    staff = staff_frame([(2025, "AAA", "Coach")])
    assert sc.coach_prior(profile, staff, target_season=2025).is_empty()


# --- shrinkage -----------------------------------------------------------

def build_league(seasons, teams, rb_tgt_by_team):
    """A league where each team has its own RB target share."""
    rows = []
    for season in seasons:
        for team in teams:
            rows += simple_team(season, team, 1, rb_tgt=rb_tgt_by_team[team],
                                wr_tgt=30 - rb_tgt_by_team[team])
    return weeks(rows)


def test_a_coach_with_one_season_is_pulled_hard_toward_the_league():
    frame = build_league([2023, 2024], ["AAA", "BBB", "CCC"],
                         {"AAA": 25, "BBB": 5, "CCC": 5})
    profile = sc.team_profile(frame)
    staff = staff_frame(
        [(s, t, f"{t} coach") for s in (2023, 2024) for t in ("BBB", "CCC")]
        + [(2023, "AAA", "Rookie coach")])
    prior = sc.coach_prior(profile, staff, 2025, shrinkage=3.0)
    row = prior.filter(pl.col("head_coach") == "Rookie coach").row(0, named=True)
    history = profile.filter(pl.col("season") < 2025)
    league = sc.league_means(history)
    # Read his own share off the profile rather than recomputing the denominator by
    # hand -- doing that arithmetic in the test is how this first "failed" against
    # correct code, having forgotten the tight end's targets.
    own = history.filter((pl.col("team") == "AAA") & (pl.col("season") == 2023)) \
        ["rb_target_share"][0]
    # n=1, k=3 -> a quarter of the way from the league mean to his own.
    expected = (1 * own + 3 * league["rb_target_share"]) / 4
    assert row["coach_rb_target_share"] == pytest.approx(expected, rel=1e-6)


def test_more_seasons_means_less_shrinkage():
    frame = build_league(list(range(2018, 2025)), ["AAA", "BBB"],
                         {"AAA": 25, "BBB": 5})
    profile = sc.team_profile(frame)
    staff = staff_frame([(s, "AAA", "Long timer") for s in range(2018, 2025)]
                        + [(s, "BBB", "Other") for s in range(2018, 2025)])
    prior = sc.coach_prior(profile, staff, 2025, shrinkage=3.0)
    long_timer = prior.filter(pl.col("head_coach") == "Long timer") \
        .row(0, named=True)
    other = prior.filter(pl.col("head_coach") == "Other").row(0, named=True)
    assert long_timer["coach_seasons"] == 7

    history = profile.filter(pl.col("season") < 2025)
    league = sc.league_means(history)["rb_target_share"]
    own = history.filter(pl.col("team") == "AAA")["rb_target_share"][0]
    # Seven seasons against k = 3 keeps 70% of the distance from league to own.
    assert long_timer["coach_rb_target_share"] == pytest.approx(
        (7 * own + 3 * league) / 10, rel=1e-6)
    # And the two coaches still separate, which is the point of the feature.
    assert long_timer["coach_rb_target_share"] > other["coach_rb_target_share"]


# --- attaching to features ----------------------------------------------

def features_frame(rows):
    return pl.DataFrame({
        "gsis_id": [r[0] for r in rows],
        "team": [r[1] for r in rows],
        "position": [r[2] for r in rows],
    })


@pytest.fixture
def league():
    frame = build_league([2023, 2024], ["AAA", "BBB"], {"AAA": 25, "BBB": 5})
    profile = sc.team_profile(frame)
    staff = staff_frame([
        (2023, "AAA", "Stayer"), (2024, "AAA", "Stayer"), (2025, "AAA", "Stayer"),
        (2023, "BBB", "Goner"), (2024, "BBB", "Goner"), (2025, "BBB", "Newcomer"),
    ])
    return profile, staff


def test_attach_brings_the_current_coach_and_his_prior(league):
    profile, staff = league
    out = sc.attach(features_frame([("a", "AAA", "RB")]), profile, staff, 2025)
    row = out.row(0, named=True)
    assert row["head_coach"] == "Stayer"
    assert row["coach_seasons"] == 2
    assert row["coach_rb_target_share"] is not None


def test_staff_continuity_flags_a_new_coach(league):
    profile, staff = league
    out = sc.attach(features_frame([("a", "AAA", "RB"), ("b", "BBB", "RB")]),
                    profile, staff, 2025).sort("gsis_id")
    by_id = dict(zip(out["gsis_id"], out["staff_continuity"]))
    assert by_id["a"] is True
    assert by_id["b"] is False


def test_a_first_year_coach_has_no_prior_and_is_flagged(league):
    """Six of 32 teams had a first-year head coach for 2026, so this is a fifth of
    the league rather than an edge case."""
    profile, staff = league
    out = sc.attach(features_frame([("b", "BBB", "RB")]), profile, staff, 2025)
    row = out.row(0, named=True)
    assert row["head_coach"] == "Newcomer"
    assert row["coach_seasons"] == 0
    assert row["coach_is_new"] is True


def test_a_player_gets_his_new_teams_context_not_his_old_one(league):
    """The team on the feature frame is the current one from the pre-season roster,
    which is what makes a move visible."""
    profile, staff = league
    out = sc.attach(features_frame([("moved", "AAA", "RB")]), profile, staff, 2025)
    assert out["team_prior_team_rb_carries_pg"][0] is not None


# --- position routing ----------------------------------------------------

def test_a_receiver_is_not_handed_the_carry_distribution():
    assert "coach_rb_carry_share" not in sc.position_columns("WR")
    assert "coach_wr_target_share" in sc.position_columns("WR")


def test_every_modelled_position_has_metrics():
    from Scripts.usage.features import MODELLED_POSITIONS
    for position in MODELLED_POSITIONS:
        assert sc.position_columns(position)
        assert sc.position_columns(position, prefix=sc.TEAM_PREFIX)


def test_an_unmodelled_position_gets_nothing_rather_than_a_default():
    assert sc.position_columns("K") == []
