"""Rest-of-season value: the parse, the two join keys, and what abstention means.

Network-free. The pages are saved fixtures, because a scraper test that needs the
internet is a scraper test that stops running.

The contract worth pinning is not the column list. It is that **this source is
allowed to order players and not to set a level**, and that everything it cannot
say reads as an abstention rather than as a zero. FantasyPros publishes no
individual defenders at all -- 150 of 284 free-agent rows on the one IDP league --
and a `0.0` there would rank every defender in the league bottom of the wire.
"""

import pathlib

import pandas as pd
import pytest

from Scripts import ros as R
from Scripts import scrape_FP as fp

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def page(name):
    return (FIXTURES / name).read_text()


# --- the parse ------------------------------------------------------------

def test_the_rankings_are_read_from_the_embedded_json():
    """The table is rendered by JavaScript, so the DOM has no rows to read."""
    payload = fp.parse_ecr_data(page("fantasypros_ros_rb.html"))
    assert payload["type"] == "ROS"
    assert len(payload["players"]) == 3


def test_the_blob_stops_at_its_own_terminator():
    """Several `var` blocks follow `ecrData`. A greedy match swallows them and the
    JSON no longer decodes."""
    payload = fp.parse_ecr_data(page("fantasypros_ros_rb.html"))
    assert "b" not in payload
    assert payload["total_experts"] == 3


def test_a_restructured_page_raises_rather_than_returning_nothing():
    """**The failure that would otherwise be silent.** An empty rankings scrape and
    a week in which nobody was ranked produce the same file, and one of them is a
    bug. The same contract `normalise_bol_props` has."""
    with pytest.raises(ValueError, match="ecrData"):
        fp.parse_ecr_data("<html><body>no data here</body></html>")


def test_a_malformed_blob_raises():
    with pytest.raises(ValueError, match="JSON"):
        fp.parse_ecr_data("<script>\nvar ecrData = {not json};\n</script>\n")


# --- the columns ----------------------------------------------------------

def test_the_spread_travels_with_the_number():
    """`total_experts` is 2-3 on these pages against 100+ on the in-season
    consensus. A rank that thin has to carry its own disagreement."""
    df = fp.get_ros("rb", html=page("fantasypros_ros_rb.html"))
    assert set(["rank_min", "rank_max", "rank_std", "total_experts"]) <= set(df.columns)
    assert df["total_experts"].eq(3).all()
    assert df.loc[df["player_name"] == "James Cook III", "rank_std"].iloc[0] == 0.47


def test_an_unpriced_player_is_null_and_not_zero():
    df = fp.get_ros("rb", html=page("fantasypros_ros_rb.html"))
    unpriced = df[df["player_name"] == "Unpriced Back"]
    assert unpriced["r2p_pts"].isna().all()
    assert unpriced["rank_ecr"].iloc[0] == 99


# --- the two join keys ----------------------------------------------------

def test_a_defence_is_renamed_to_the_spelling_the_store_uses():
    """FantasyPros says "Houston Texans"; `lineups.parquet` says "Texans D/ST"."""
    df = fp.get_ros("dst", html=page("fantasypros_ros_dst.html"))
    assert "Texans D/ST" in set(df["player_name"])
    assert "Houston Texans" not in set(df["player_name"])


def test_the_two_teams_fantasypros_spells_differently_are_aliased():
    """`JAC` and `WAS` against ESPN's `JAX` and `WSH`. Two of thirty-two, and D/ST
    joins on this key rather than on the name -- so each one was a defence that
    silently never matched. Gate G-R0 caught it on the first capture: every league
    was missing exactly its Jaguars and its Commanders."""
    df = fp.get_ros("dst", html=page("fantasypros_ros_dst.html"))
    teams = set(df["pro_team"])
    assert {"HOU", "JAX", "WSH"} <= teams
    assert "JAC" not in teams and "WAS" not in teams


def test_a_defence_joins_on_team_and_a_skill_player_on_name():
    ros = pd.concat([fp.get_ros("rb", html=page("fantasypros_ros_rb.html")),
                     fp.get_ros("dst", html=page("fantasypros_ros_dst.html"))],
                    ignore_index=True)
    frame = pd.DataFrame([
        {"player_name": "Jahmyr Gibbs", "player_position": "RB", "pro_team": "DET"},
        # The store's spelling, which matches no FantasyPros name anywhere.
        {"player_name": "Jaguars D/ST", "player_position": "D/ST", "pro_team": "JAX"},
    ])
    out = R.attach_ros(frame, ros)
    assert out["ros_pos_rank"].tolist() == ["RB1", "DST9"]
    assert out["ros_missing_reason"].isna().all()


# --- abstention -----------------------------------------------------------

def test_an_idp_gets_no_number_and_says_why():
    """FantasyPros publishes no individual defenders. On `gop_degenerates` that is
    150 of 284 free-agent rows, and a zero there would rank every defender in the
    league bottom of the wire."""
    ros = fp.get_ros("rb", html=page("fantasypros_ros_rb.html"))
    frame = pd.DataFrame([{"player_name": "Quay Walker", "player_position": "LB",
                           "pro_team": "LV"}])
    out = R.attach_ros(frame, ros)
    assert out["ros_pos_rank"].isna().all()
    assert out["ros_ppg"].isna().all()
    assert out["ros_missing_reason"].iloc[0] == R.MISSING_NO_PUBLICATION


def test_a_ranked_position_the_source_omits_is_a_gap_not_a_publication_failure():
    """Different reason, because they want opposite responses: one is a fact about
    FantasyPros' coverage, the other would be a bug in ours."""
    ros = fp.get_ros("rb", html=page("fantasypros_ros_rb.html"))
    frame = pd.DataFrame([{"player_name": "Nobody At All", "player_position": "RB",
                           "pro_team": "DET"}])
    out = R.attach_ros(frame, ros)
    assert out["ros_missing_reason"].iloc[0] == R.MISSING_NOT_RANKED


def test_a_ranked_player_with_no_points_is_its_own_reason():
    ros = fp.get_ros("rb", html=page("fantasypros_ros_rb.html"))
    frame = pd.DataFrame([{"player_name": "Unpriced Back", "player_position": "RB",
                           "pro_team": "JAX"}])
    out = R.attach_ros(frame, ros)
    assert out["ros_pos_rank"].iloc[0] == "RB99"
    assert out["ros_missing_reason"].iloc[0] == R.MISSING_NO_POINTS


def test_no_capture_yet_is_not_an_error():
    """The scrape is a nightly stage and the app must render the day before it
    first runs."""
    frame = pd.DataFrame([{"player_name": "Jahmyr Gibbs", "player_position": "RB",
                           "pro_team": "DET"}])
    out = R.attach_ros(frame, None)
    assert out["ros_pos_rank"].isna().all()
    assert "ros_missing_reason" in out.columns


# --- the conversion, which is display only --------------------------------

def test_the_per_game_figure_needs_every_input():
    assert R.ros_ppg(None, 1.2, 17) is None
    assert R.ros_ppg(300.0, None, 17) is None
    assert R.ros_ppg(300.0, 1.2, 0) is None
    assert R.ros_ppg(300.0, 1.2, 12) == pytest.approx(30.0)


def test_a_league_scoring_exactly_std_converts_at_one():
    lineups = pd.DataFrame({
        "week": [1, 1, 1, 1],
        "player_name": ["A", "B", "C", "D"],
        "player_position": ["RB", "RB", "RB", "RB"],
        "FP_Points": [10.0, 20.0, 30.0, 40.0],
    })
    weekly = pd.DataFrame({
        "week": [1, 1, 1, 1],
        "player_name": ["A", "B", "C", "D"],
        "STD_FantasyPoints": [10.0, 20.0, 30.0, 40.0],
    })
    assert R.scoring_factors(lineups, weekly)["RB"] == pytest.approx(1.0)


def test_a_tiny_denominator_is_excluded_rather_than_exploding():
    """A near-zero weekly projection is exactly what the deep-pool players this
    feature is for carry, and it is what makes a per-player ratio unusable."""
    lineups = pd.DataFrame({
        "week": [1, 1, 1, 1],
        "player_name": ["A", "B", "C", "Scrub"],
        "player_position": ["RB"] * 4,
        "FP_Points": [10.0, 20.0, 30.0, 4.0],
    })
    weekly = pd.DataFrame({
        "week": [1, 1, 1, 1],
        "player_name": ["A", "B", "C", "Scrub"],
        "STD_FantasyPoints": [10.0, 20.0, 30.0, 0.01],
    })
    assert R.scoring_factors(lineups, weekly)["RB"] == pytest.approx(1.0)


def test_no_weekly_file_publishes_no_factor_rather_than_one():
    """An absent conversion must not default to 1.0 -- that would silently publish
    STD points into a league that does not score in STD."""
    lineups = pd.DataFrame({"week": [1], "player_name": ["A"],
                            "player_position": ["RB"], "FP_Points": [10.0]})
    assert R.scoring_factors(lineups, None) == {}


# --- the source is not a blend voter --------------------------------------

def test_no_column_is_named_like_a_blend_source():
    """`*_Points` is how `WEEKLY_PREFIXES`, `present_prefixes` and
    `lineup.points_columns` recognise a source. A rest-of-season rank is not a
    sixth vote on this week, and a column shaped like one would make
    `real_sources` lie."""
    ros = fp.get_ros("rb", html=page("fantasypros_ros_rb.html"))
    frame = pd.DataFrame([{"player_name": "Jahmyr Gibbs", "player_position": "RB",
                           "pro_team": "DET"}])
    out = R.attach_ros(frame, ros, factors={"RB": 1.2}, games_remaining=17)
    assert not [c for c in out.columns if c.endswith("_Points")]
    assert out["ros_ppg"].iloc[0] == pytest.approx(325.8 * 1.2 / 17)


def test_the_capture_is_keyed_by_date_so_history_accrues():
    """There is no archive of what a rest-of-season consensus said in a past week.
    Overwriting would leave the source permanently unmeasurable."""
    assert "captured_date" in fp.ROS_KEYS
