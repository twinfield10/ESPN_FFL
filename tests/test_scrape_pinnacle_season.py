"""Pinnacle season-prop parsing. Pure functions only -- no network."""

import pandas as pd
import pytest

from Scripts import scrape_pinnacle_season as sps


def _matchup(desc, over="Over 1199.5 yards", under="Under 1199.5 yards", mid=1):
    return {
        "id": mid,
        "special": {"category": sps.SPECIAL_CATEGORY, "description": desc},
        "participants": [
            {"id": mid * 10 + 1, "name": over},
            {"id": mid * 10 + 2, "name": under},
        ],
    }


def _prices(mid=1, over=-111, under=-109):
    return {mid: {mid * 10 + 1: over, mid * 10 + 2: under}}


# --- odds ----------------------------------------------------------------

@pytest.mark.parametrize("price, expected", [
    (-110, 0.5238095),
    (100, 0.5),
    (110, 0.4761905),
    (-200, 0.6666667),
    (200, 0.3333333),
])
def test_american_to_prob(price, expected):
    assert sps.american_to_prob(price) == pytest.approx(expected, rel=1e-6)


def test_no_vig_probabilities_sum_to_one():
    """The two sides of a real market sum to more than 1 by the book's margin.
    Leaving that in would overstate how likely the line is."""
    df = sps.parse_props([_matchup("NFL 2026/2027 - CeeDee Lamb Regular Season Receiving Yards")],
                         _prices())
    over = df["no_vig_over_prob"].iloc[0]
    assert 0 < over < 1
    # raw probabilities overround; the no-vig pair must not
    assert df["over_prob"].iloc[0] + df["under_prob"].iloc[0] > 1.0
    assert over + (1 - over) == pytest.approx(1.0)


# --- description parsing -------------------------------------------------

@pytest.mark.parametrize("desc, player, stat_raw, stat", [
    ("NFL 2026/2027 - CeeDee Lamb Regular Season Receiving Yards",
     "CeeDee Lamb", "Receiving Yards", "receivingYards"),
    ("NFL 2026/2027 - Jordan Love Regular Season Passing Yards",
     "Jordan Love", "Passing Yards", "passingYards"),
    ("NFL 2026/2027 - A.J. Brown Regular Season Receiving Yards",
     "A.J. Brown", "Receiving Yards", "receivingYards"),
    ("NFL 2026/2027 - James Cook Regular Season Rushing Yards",
     "James Cook", "Rushing Yards", "rushingYards"),
])
def test_parses_player_and_stat(desc, player, stat_raw, stat):
    df = sps.parse_props([_matchup(desc)], _prices())
    row = df.iloc[0]
    assert row["player_name"] == player
    assert row["stat_raw"] == stat_raw
    assert row["stat"] == stat


def test_line_comes_from_the_participant_name():
    df = sps.parse_props(
        [_matchup("NFL 2026/2027 - Bo Nix Regular Season Passing Yards",
                  over="Over 3499.5", under="Under 3499.5")],
        _prices(),
    )
    assert df["line"].iloc[0] == pytest.approx(3499.5)


def test_unmapped_stat_wording_is_kept_with_a_null_mapping():
    """A new wording must surface as an unmapped row rather than vanish -- the
    silent-drop failure mode this repo keeps running into."""
    df = sps.parse_props(
        [_matchup("NFL 2026/2027 - Some Player Regular Season Kicking Points")],
        _prices(),
    )
    assert len(df) == 1
    assert df["stat_raw"].iloc[0] == "Kicking Points"
    assert pd.isna(df["stat"].iloc[0])


def test_non_player_prop_specials_are_skipped():
    other = {
        "id": 2,
        "special": {"category": "Regular Season Wins",
                    "description": "Green Bay Packers Total Regular Season Wins"},
        "participants": [{"id": 21, "name": "Over 9.5"}, {"id": 22, "name": "Under 9.5"}],
    }
    df = sps.parse_props([other], {})
    assert df.empty


def test_straight_game_lines_are_skipped():
    df = sps.parse_props([{"id": 3, "special": None, "participants": []}], {})
    assert df.empty


def test_malformed_description_is_skipped_not_fatal():
    df = sps.parse_props(
        [{"id": 4,
          "special": {"category": sps.SPECIAL_CATEGORY, "description": "nonsense"},
          "participants": []}],
        {},
    )
    assert df.empty


def test_missing_prices_still_yield_a_line():
    """The line is the projection; odds only weight it. A price gap must not drop
    the prop."""
    df = sps.parse_props(
        [_matchup("NFL 2026/2027 - Bo Nix Regular Season Passing Yards")], {},
    )
    assert df["line"].iloc[0] == pytest.approx(1199.5)
    assert pd.isna(df["over_odds"].iloc[0])
    assert pd.isna(df["no_vig_over_prob"].iloc[0])


def test_scraper_does_not_run_at_import_time():
    """The other three scrapers execute on import, which is how a stray import
    wrote a projections file. This one must not."""
    import ast
    import pathlib
    src = pathlib.Path(sps.__file__).read_text()
    tree = ast.parse(src)
    bare_calls = [
        n for n in tree.body
        if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call)
    ]
    assert not bare_calls, f"module-level calls: {[n.lineno for n in bare_calls]}"
    assert any(
        isinstance(n, ast.If) and getattr(getattr(n.test, "left", None), "id", "") == "__name__"
        for n in tree.body
    ), "missing `if __name__ == '__main__':` guard"
