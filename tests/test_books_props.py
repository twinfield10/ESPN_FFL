"""Tests for player props as standard odds rows.

The dual write exists to give props line history without disturbing the blend, so
what matters here is that the conversion is faithful, that a prop and a game line stay
distinguishable in one store, and that neither can quietly delete the other.
"""

import polars as pl
import pytest

from Scripts.books.schema import LINE_KEYS, ODDS_SCHEMA
from Scripts.books.props import GAME_PERIOD, PROP_MARKET, props_to_odds_rows
from Scripts.books.store import detect_changes

SCHED = pl.DataFrame({
    "NFL_game_id": ["2026_01_CLE_JAX"],
    "week": [1],
    "officialDate": [pl.Series(["2026-09-13"]).str.strptime(pl.Date).item()],
    "Away": ["CLE"],
    "Home": ["JAX"],
})


def _raw(rows):
    """A frame shaped like Scripts.scrape_BOL's raw output."""
    base = {
        "BOL_game_id": 281537, "week": 1, "player_name": "Deshaun Watson",
        "player_id": 355898, "team": "CLE", "position": "QB", "market_id": 1,
        "condition": 3, "is_active": True, "is_actual": True,
        "type": "Over", "odds": 1.8, "value": 174.5,
        "statistic": "Passing Yards", "espn_stat": "passingYards",
        "impProb": 1 / 1.8, "prop_source": "OverUnder",
    }
    return pl.DataFrame([{**base, **r} for r in rows])


def _convert(rows):
    return props_to_odds_rows(_raw(rows), 2026, SCHED,
                              snapshot_ts="2026-09-08T18:00:00Z")


class TestShape:
    def test_output_conforms_to_the_standard_schema(self):
        out = _convert([{}])
        assert out.columns == list(ODDS_SCHEMA)
        assert out.schema == ODDS_SCHEMA

    def test_an_empty_scrape_still_has_the_shape(self):
        out = props_to_odds_rows(pl.DataFrame(), 2026, SCHED)
        assert out.is_empty()
        assert out.columns == list(ODDS_SCHEMA)

    def test_a_player_on_no_scheduled_team_is_dropped(self):
        """A stale roster or a renamed abbreviation must not invent a matchup."""
        out = _convert([{"team": "ZZZ"}])
        assert out.is_empty()


class TestFieldMapping:
    def test_the_player_is_the_side_the_price_is_about(self):
        assert _convert([{}])["sideOf"].item() == "Deshaun Watson"

    def test_prop_type_carries_the_repos_stat_name_not_the_books(self):
        """So a query spans books once a second one is migrated."""
        out = _convert([{}])
        assert out["propType"].item() == "passingYards"
        assert out["marketTitle"].item() == PROP_MARKET
        assert out["gamePeriod"].item() == GAME_PERIOD

    def test_matchup_matches_the_game_line_spelling(self):
        """Full names, "Away vs. Home" -- one string reaches a game's spread and its
        players' props alike, which is the point of sharing a store."""
        out = _convert([{}])
        assert out["matchup"].item() == "Cleveland Browns vs. Jacksonville Jaguars"
        assert out["Away"].item() == "Cleveland Browns"
        assert out["Home"].item() == "Jacksonville Jaguars"

    @pytest.mark.parametrize("side,expected", [("Over", "over"), ("Under", "under")])
    def test_two_way_sides_map_to_the_stored_vocabulary(self, side, expected):
        assert _convert([{"type": side}])["betSide"].item() == expected

    def test_a_ladder_rung_is_an_over_and_an_alternate(self):
        """marketsBySs prices "2 or more" as its own one-sided market."""
        out = _convert([{"type": "1", "prop_source": "Values"}])
        assert out["betSide"].item() == "over"
        assert out["isAlt"].item() is True

    def test_a_two_way_market_is_not_an_alternate(self):
        assert _convert([{}])["isAlt"].item() is False

    @pytest.mark.parametrize("decimal,american", [
        (2.0, 100.0),      # even money, the boundary
        (3.0, 200.0),      # underdog
        (1.8, -125.0),     # favourite
        (1.5, -200.0),
    ])
    def test_decimal_odds_become_american(self, decimal, american):
        out = _convert([{"odds": decimal, "impProb": 1 / decimal}])
        assert out["price"].item() == pytest.approx(american)

    def test_implied_probability_is_carried_through_untouched(self):
        out = _convert([{}])
        assert out["impProb"].item() == pytest.approx(1 / 1.8)


class TestDevig:
    def test_a_two_way_pair_devigs_to_one(self):
        out = _convert([
            {"type": "Over", "odds": 1.8, "impProb": 1 / 1.8},
            {"type": "Under", "odds": 2.0, "impProb": 1 / 2.0, "market_id": 2},
        ])
        assert out["fairProb"].sum() == pytest.approx(1.0, abs=1e-9)

    def test_a_ladder_rung_keeps_its_raw_probability(self):
        """Nothing to de-vig against, and the two columns being equal is the signal."""
        out = _convert([{"type": "1", "prop_source": "Values", "odds": 19.0,
                         "impProb": 1 / 19.0}])
        assert out["fairProb"].item() == pytest.approx(out["impProb"].item())

    def test_rungs_at_different_numbers_are_not_paired_with_each_other(self):
        """Two rungs are two markets, not two sides of one."""
        out = _convert([
            {"type": "1", "prop_source": "Values", "value": 1.0, "odds": 2.5,
             "impProb": 1 / 2.5},
            {"type": "1", "prop_source": "Values", "value": 2.0, "odds": 19.0,
             "impProb": 1 / 19.0, "market_id": 2},
        ])
        assert (out["fairProb"] == out["impProb"]).all()


class TestCoexistenceWithGameLines:
    """A prop and a game line share a book's directory. They must stay distinct."""

    def _game_line(self):
        row = {c: None for c in ODDS_SCHEMA}
        row.update(sportsbook="BetOnline", bookType="book", season=2026, week=1,
                   officialDate="2026-09-13", matchup="Cleveland Browns vs. Jacksonville Jaguars",
                   Home="Jacksonville Jaguars", Away="Cleveland Browns",
                   marketTitle="Total", gamePeriod=GAME_PERIOD, betSide="over",
                   sideOf=None, marketLine=44.5, value=44.5, price=-110.0,
                   impProb=0.5238, fairProb=0.5, isAlt=False, propType=None,
                   snapshot_ts="2026-09-08T18:00:00Z")
        return pl.DataFrame([row]).cast(ODDS_SCHEMA)

    def test_prop_type_is_what_separates_them_in_the_line_key(self):
        assert "propType" in LINE_KEYS
        prop = _convert([{}])
        line = self._game_line()
        assert prop["propType"].item() is not None
        assert line["propType"].item() is None

    def test_a_game_line_is_not_treated_as_a_moved_prop(self):
        """The null-in-a-composite-key trap: sideOf is null on every game market and
        propType on every prop, so a naive key matches neither and every row reads as
        new. store._key_expr exists for this; assert it holds across the two kinds."""
        both = pl.concat([self._game_line(), _convert([{}])], how="diagonal_relaxed")
        assert detect_changes(both, both).is_empty()

    def test_storing_props_does_not_shadow_an_existing_game_line(self):
        line = self._game_line()
        prop = _convert([{}])
        # The game line is already stored; the props arrive afterwards.
        moved = detect_changes(line, pl.concat([line, prop], how="diagonal_relaxed"))
        assert moved.height == 1
        assert moved["marketTitle"].item() == PROP_MARKET
