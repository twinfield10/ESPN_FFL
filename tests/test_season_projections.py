"""Season-long projections: the name crosswalk, BetOnline's typos, and ESPN's units.

The unit handling is the part worth guarding. In ESPN's season row, counts are
season totals but yardage is a per-game average -- so a naive read puts ESPN 17x
below a season-long book line and the blend collapses toward whichever source is
left.
"""

import types

import pandas as pd
import pytest

from Scripts import season_projections as sp


# --- name crosswalk ------------------------------------------------------

@pytest.mark.parametrize("a, b", [
    ("CeeDee Lamb", "CEEDEE LAMB"),          # BetOnline is uppercase
    ("A.J. Brown", "AJ BROWN"),              # punctuation differs
    ("De'Von Achane", "DEVON ACHANE"),
    ("Amon-Ra St. Brown", "AMON RA ST BROWN"),
    ("Patrick Mahomes II", "Patrick Mahomes"),
    ("Marvin Harrison Jr.", "Marvin Harrison"),
])
def test_names_from_different_sources_share_a_key(a, b):
    assert sp.normalise_name(a) == sp.normalise_name(b)


def test_known_misspelling_is_aliased():
    """BetOnline really does ship 'Dalton Kinciad'."""
    assert sp.normalise_name("Dalton Kinciad") == sp.normalise_name("Dalton Kincaid")


def test_distinct_players_do_not_collide():
    assert sp.normalise_name("Josh Allen") != sp.normalise_name("Keenan Allen")


def test_missing_name_is_none():
    assert sp.normalise_name(None) is None
    assert sp.normalise_name(float("nan")) is None


# --- BetOnline stat wording ---------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("Receiving Yards", "receivingYards"),
    ("Receiving Yrads", "receivingYards"),        # typo seen in the feed
    ("Receiving Yrards", "receivingYards"),       # and this one
    ("Passing Interceptions", "passingInterceptions"),
    ("Passing INT's", "passingInterceptions"),
    ("Passing IND's", "passingInterceptions"),    # D for T
    ('Passing TD"s', "passingTouchdowns"),
    ("Receiving TD;s", "receivingTouchdowns"),
    ("Rushing TD's", "rushingTouchdowns"),
    ("Receptions", "receivingReceptions"),
    ("Reception", "receivingReceptions"),         # singular
    ("Tackles & Assists", "defensiveTotalTackles"),
    ("Tackles & Assist", "defensiveTotalTackles"),
    ("Sacks", "defensiveSacks"),
    ("Interceptions", "defensiveInterceptions"),
])
def test_stat_wording_including_typos(raw, expected):
    assert sp.BOL_STAT_MAP[sp._normalise_stat_text(raw)] == expected


def test_combined_markets_are_kept_separate_not_split():
    """A combined rushing+receiving line cannot be apportioned between the two
    stats, so it gets its own column rather than being guessed apart."""
    for raw in ("Receiving & Rushing Yards", "Total Rushing & Receiving Yards"):
        assert sp.BOL_STAT_MAP[sp._normalise_stat_text(raw)] == sp.COMBINED_YARDS
    assert sp.BOL_STAT_MAP[sp._normalise_stat_text("Receiving & Rushing TD's")] == sp.COMBINED_TDS


# --- BetOnline row recovery ---------------------------------------------

def _bol(player, stat_type, line, stat_short=None):
    return {"player": player, "stat_type": stat_type, "line": line,
            "True_Line": line, "stat_short": stat_short}


def test_short_code_path_is_used_when_upstream_parsed_it():
    df = pd.DataFrame([_bol("DJ MOORE", "Receiving Yards", 825.5, "YDS_REC")])
    out = sp.normalise_bol_props(df)
    assert out["stat"].iloc[0] == "receivingYards"
    assert out["line"].iloc[0] == pytest.approx(825.5)


@pytest.mark.parametrize("stat_type, player, stat", [
    ("De'Von Achane MIA -Total Receiving & Rushing Yards",
     "De'Von Achane", sp.COMBINED_YARDS),
    ("Hunter Henry NE Total Receiving Yards", "Hunter Henry", "receivingYards"),
    ("Travis Kelce KC- Total Receiving TD's", "Travis Kelce", "receivingTouchdowns"),
    ("Jayden Daniels WSH - Rushing Yards", "Jayden Daniels", "rushingYards"),
    ("Javonte Williams DAL - Receiving & Rushing Yards",
     "Javonte Williams", sp.COMBINED_YARDS),
])
def test_player_recovered_when_upstream_split_failed(stat_type, player, stat):
    """These rows arrive with player == 'UNKNOWN' and the name embedded in the
    stat text, because the separators are irregular ('PHI-', 'KC -', '-Total')."""
    out = sp.normalise_bol_props(pd.DataFrame([_bol("UNKNOWN", stat_type, 100.0)]))
    assert len(out) == 1
    assert sp.normalise_name(out["player_name"].iloc[0]) == sp.normalise_name(player)
    assert out["stat"].iloc[0] == stat


def test_unrecoverable_unknown_rows_are_dropped_not_kept_as_unknown():
    out = sp.normalise_bol_props(pd.DataFrame([_bol("UNKNOWN", "???", 1.0)]))
    assert out.empty


# --- ESPN unit handling -------------------------------------------------

def _player(name, breakdown, position="RB", total=100.0, pid=1):
    return types.SimpleNamespace(
        name=name, playerId=pid, position=position, proTeam="DET",
        projected_total_points=total,
        stats={0: {"projected_breakdown": breakdown}},
    )


def _league(players):
    return types.SimpleNamespace(
        name="Test", year=2026, league_id=1,
        teams=[types.SimpleNamespace(roster=players, owner="Owner")],
        free_agents=lambda size=0, position=None: [],
    )


def test_yardage_is_converted_from_per_game_to_season():
    """The trap. ESPN reports 80.83 rushing yards in the season row; with 17 games
    that is 1374 season yards, which is what every cross-check agrees on."""
    p = _player("Jahmyr Gibbs", {
        "rushingYards": 80.83,          # per game
        "rushingAttempts": 283.19,      # season total
        "rushingTouchdowns": 14.51,     # season total
        sp.GAMES_PLAYED_KEY: 17,
    })
    out = sp.espn_season_projections(_league([p]))
    row = out.iloc[0]
    assert row["ESPN_rushingYards"] == pytest.approx(80.83 * 17)
    # counts must NOT be scaled
    assert row["ESPN_rushingAttempts"] == pytest.approx(283.19)
    assert row["ESPN_rushingTouchdowns"] == pytest.approx(14.51)


def test_games_played_drives_the_conversion():
    p = _player("Half Season", {"receivingYards": 50.0, sp.GAMES_PLAYED_KEY: 8})
    out = sp.espn_season_projections(_league([p]))
    assert out["ESPN_receivingYards"].iloc[0] == pytest.approx(400.0)


def test_missing_games_played_falls_back_to_a_full_season():
    p = _player("No Games Key", {"receivingYards": 50.0})
    out = sp.espn_season_projections(_league([p]))
    assert out["ESPN_receivingYards"].iloc[0] == pytest.approx(50.0 * sp.DEFAULT_GAMES)


def test_only_offensive_yardage_is_treated_as_per_game():
    """Guard the exact membership of the set. Scaling a count would be as wrong as
    not scaling a yardage, and ESPN is inconsistent between offensive and
    return/defensive yardage -- so this set is evidence-based, not a pattern."""
    assert sp.PER_GAME_IN_SEASON_ROW == {
        "passingYards", "rushingYards", "receivingYards",
    }


@pytest.mark.parametrize("stat", [
    "kickoffReturnYards", "puntReturnYards",
    "defensiveYardsAllowed", "defensivePointsAllowed",
])
def test_return_and_defensive_yardage_is_not_scaled(stat):
    """These arrive as season totals. Scaling puntReturnYards turned a 422-point
    D/ST projection into 2294 -- caught only because the number was absurd."""
    p = _player("Some D/ST", {stat: 300.0, sp.GAMES_PLAYED_KEY: 17}, position="D/ST")
    out = sp.espn_season_projections(_league([p]))
    assert out[f"ESPN_{stat}"].iloc[0] == pytest.approx(300.0)


def test_counts_are_never_scaled():
    p = _player("Counter", {
        "receivingReceptions": 67.79,
        "rushingTouchdowns": 14.51,
        "defensiveSacks": 42.28,
        sp.GAMES_PLAYED_KEY: 17,
    })
    out = sp.espn_season_projections(_league([p])).iloc[0]
    assert out["ESPN_receivingReceptions"] == pytest.approx(67.79)
    assert out["ESPN_rushingTouchdowns"] == pytest.approx(14.51)
    assert out["ESPN_defensiveSacks"] == pytest.approx(42.28)


def test_duplicate_players_are_not_double_counted():
    p1 = _player("Dupe", {"receivingYards": 10.0}, pid=7)
    p2 = _player("Dupe", {"receivingYards": 10.0}, pid=7)
    out = sp.espn_season_projections(_league([p1, p2]))
    assert len(out) == 1


# --- against real leagues ------------------------------------------------

@pytest.mark.live
def test_espn_points_reproduces_espn_projected_total():
    """The end-to-end check on the unit fix: scoring ESPN's own season stat lines
    with the league's own rules must land on ESPN's own projected total. If yardage
    were left per-game this ratio would be far below 1."""
    from Scripts.config_utils import build_lg_vars, get_season
    from Scripts.fetch_utils import fetch_league

    cfg = build_lg_vars()["Knights_FFL"]
    season = get_season()
    league = fetch_league(league_id=cfg["ID"], year=season,
                          swid=cfg["SWID"], espn_s2=cfg["ESPN_S2"])
    df = sp.build_season_projections(league, season=season)
    df = df[df["ESPN_projected_total"] > 20]

    assert len(df) > 100
    ratio = df["ESPN_Points"] / df["ESPN_projected_total"]
    assert ratio.median() == pytest.approx(1.0, abs=0.03)
    assert df["ESPN_Points"].corr(df["ESPN_projected_total"]) > 0.99


@pytest.mark.live
def test_every_betonline_prop_maps_to_a_stat():
    """A wording BetOnline changes must fail the build, not silently drop props."""
    from Scripts.config_utils import get_season
    from Scripts.paths import season_dir

    path = season_dir("BetOnline", get_season(), "BetOnline_SeasonProps_All.csv")
    if not path.exists():
        pytest.skip("run `Rscript R/GetSeasonProps.R` first")

    props = sp.normalise_bol_props(pd.read_csv(path))
    unmapped = props[props["stat"].isna()]
    assert unmapped.empty, (
        "unrecognised BetOnline wordings: "
        f"{sorted(unmapped['stat_text'].map(sp._normalise_stat_text).unique())}"
    )
