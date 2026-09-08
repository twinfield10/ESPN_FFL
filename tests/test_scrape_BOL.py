"""Tests for the BetOnline weekly props scraper.

There was no test file for this module before, which is part of why it sat broken for
a month behind a 403 that nothing asserted against. The payloads below are real
``/api/dfm/*`` responses captured from the widget on 2026-09-08, trimmed to two
players; keeping them as fixtures means the parse can be exercised without a browser.
"""

import polars as pl
import pytest

from Scripts import bol_widget
from Scripts.bol_widget import STATS, StatSpec, make_fetcher
from Scripts.scrape_BOL import (
    FULL_DF_SCHEMA,
    get_BOL_data,
    get_BOL_data_OU,
    get_week_ids,
    stats,
)

# A real marketsByOu body: two-way, so each player carries an Over (18) and Under (19).
OU_PAYLOAD = [
    {
        "statistic": {"id": 2176, "title": "Passing Yards"},
        "type": 18,
        "typeId": 3,
        "players": [
            {
                "position": {"id": 28, "title": "QB"},
                "name": "Deshaun Watson",
                "id": 355898,
                "team": "CLE",
                "markets": [
                    {"id": 50295517, "condition": 3, "game1Id": 281537,
                     "isActive": True, "isActual": True, "odds": 1.8,
                     "value": 174.5, "type": 18,
                     "statistic": {"id": 2176, "title": "Passing Yards"}},
                    {"id": 50295518, "condition": 3, "game1Id": 281537,
                     "isActive": True, "isActual": True, "odds": 2.0,
                     "value": 174.5, "type": 19,
                     "statistic": {"id": 2176, "title": "Passing Yards"}},
                ],
            }
        ],
    }
]

# A real marketsBySs body: a ladder, so one side per rung.
SS_PAYLOAD = [
    {
        "statistic": {"id": 1200, "title": "Touchdowns"},
        "type": 1,
        "typeId": 1,
        "players": [
            {
                "position": {"id": 32, "title": "TE"},
                "name": "Hunter Henry",
                "id": 269169,
                "team": "NE",
                "markets": [
                    {"id": 50021054, "condition": 3, "game1Id": 281532,
                     "isActive": True, "isActual": True, "odds": 19.0,
                     "value": 2.0, "type": 1,
                     "statistic": {"id": 1200, "title": "Touchdowns"}},
                ],
            },
            {
                # LVR/NOS/LAR are BetOnline spellings the scraper has to remap.
                "position": {"id": 24, "title": "RB"},
                "name": "Ashton Jeanty",
                "id": 500001,
                "team": "LVR",
                "markets": [
                    {"id": 50021055, "condition": 3, "game1Id": 281532,
                     "isActive": True, "isActual": True, "odds": 2.5,
                     "value": 1.0, "type": 1,
                     "statistic": {"id": 1200, "title": "Touchdowns"}},
                ],
            },
        ],
    }
]


def _fetcher(mapping):
    """A stand-in for the widget cache: no browser, no network."""
    return make_fetcher(mapping)


class TestGetBOLDataOU:
    def test_parses_both_sides_of_a_two_way_market(self):
        fetch = _fetcher({("marketsByOu", 281537, "Passing%2520Yards"): OU_PAYLOAD})
        df = get_BOL_data_OU(ids=[281537], link_stat="Passing%2520Yards",
                             espn_stat="passingYards", week=1, fetch=fetch)
        assert df.height == 2
        # 18/19 are DST's codes; the scraper stores the readable side.
        assert set(df["type"].to_list()) == {"Over", "Under"}
        assert df["prop_source"].unique().to_list() == ["OverUnder"]
        assert df["player_name"].unique().to_list() == ["Deshaun Watson"]
        assert df["position"].unique().to_list() == ["QB"]

    def test_implied_probability_is_the_reciprocal_of_decimal_odds(self):
        fetch = _fetcher({("marketsByOu", 281537, "Passing%2520Yards"): OU_PAYLOAD})
        df = get_BOL_data_OU(ids=[281537], link_stat="Passing%2520Yards",
                             espn_stat="passingYards", week=1, fetch=fetch)
        over = df.filter(pl.col("type") == "Over")
        assert over["impProb"].item() == pytest.approx(1 / 1.8)

    def test_conforms_to_the_declared_schema(self):
        fetch = _fetcher({("marketsByOu", 281537, "Passing%2520Yards"): OU_PAYLOAD})
        df = get_BOL_data_OU(ids=[281537], link_stat="Passing%2520Yards",
                             espn_stat="passingYards", week=1, fetch=fetch)
        assert set(df.columns) == set(FULL_DF_SCHEMA)

    def test_a_game_with_no_market_is_skipped_not_fatal(self):
        """Tackles and sacks exist for only a couple of games a week."""
        fetch = _fetcher({("marketsByOu", 281537, "Tackles"): OU_PAYLOAD})
        df = get_BOL_data_OU(ids=[281537, 281538, 281539], link_stat="Tackles",
                             espn_stat="defensiveTotalTackles", week=1, fetch=fetch)
        assert df.height == 2  # only the game that had a market

    def test_returns_none_when_nothing_was_posted(self):
        df = get_BOL_data_OU(ids=[999], link_stat="Receptions",
                             espn_stat="receivingReceptions", week=1,
                             fetch=_fetcher({}))
        assert df is None


class TestGetBOLData:
    def test_parses_a_ladder_market(self):
        fetch = _fetcher({("marketsBySs", 281532, "Touchdowns"): SS_PAYLOAD})
        df = get_BOL_data(ids=[281532], link_stat="Touchdowns",
                          espn_stat="anytimeTouchdown", week=1, fetch=fetch)
        assert df.height == 2
        assert df["prop_source"].unique().to_list() == ["Values"]

    def test_rewrites_betonline_team_abbreviations(self):
        """LVR/NOS/LAR are BetOnline's spellings; everything downstream joins on ESPN's."""
        fetch = _fetcher({("marketsBySs", 281532, "Touchdowns"): SS_PAYLOAD})
        df = get_BOL_data(ids=[281532], link_stat="Touchdowns",
                          espn_stat="anytimeTouchdown", week=1, fetch=fetch)
        assert "LVR" not in df["team"].to_list()
        assert "LV" in df["team"].to_list()


class TestGetWeekIds:
    def test_derives_ids_from_the_harvest_rather_than_guessing(self):
        cache = {
            ("marketsByOu", 281537, "Receptions"): OU_PAYLOAD,
            ("marketsBySs", 281532, "Touchdowns"): SS_PAYLOAD,
            ("marketsByOu", 281532, "Receptions"): OU_PAYLOAD,
        }
        assert get_week_ids(cache=cache, week_num=1) == {1: [281532, 281537]}

    def test_an_empty_harvest_yields_no_ids(self):
        assert get_week_ids(cache={}, week_num=1) == {1: []}


class TestStatMap:
    def test_the_scraper_and_the_widget_cannot_drift(self):
        assert stats == {s.espn_stat: s.statistic for s in STATS}

    def test_every_market_the_scraper_asks_for_is_one_the_widget_harvests(self):
        harvested = {(s.markets_route, s.statistic) for s in STATS}
        for espn, stat in stats.items():
            routes = {r for r, s in harvested if s == stat}
            assert routes, f"{espn} ({stat}) is not harvested by any widget market"

    def test_defensive_interceptions_is_deliberately_absent(self):
        """DST posts no such market -- see Scripts/bol_widget.py. A permanently red
        check is one nobody reads, so the scraper must not ask for it."""
        assert "defensiveInterceptions" not in stats


class TestWidgetSpecs:
    def test_routes_are_only_the_two_that_exist(self):
        assert {s.route for s in STATS} == {"Ss", "Ou"}

    def test_route_names_match_the_api(self):
        spec = StatSpec("x", "Passing", "Passing Yards", "Ss", "Passing%2520Yards")
        assert spec.markets_route == "marketsBySs"
        assert spec.games_route == "gamesBySs"

    def test_ladder_markets_are_not_filed_under_over_under(self):
        """A ladder tile lives under its position category; asking for it from the
        Over/Under tab silently returns the two-way market instead."""
        for spec in bol_widget.LADDER_STATS:
            assert spec.category != "Over/Under"

    def test_every_over_under_spec_sits_under_the_over_under_tab(self):
        for spec in bol_widget.OU_STATS:
            assert spec.category == "Over/Under"
