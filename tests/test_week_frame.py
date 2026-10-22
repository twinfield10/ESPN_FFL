"""The one-week ESPN fetch behind the intraday re-blend (docs/plans/56-intraday-reblend.md).

Measured against ESPN on 2026-10-22: for all nine leagues, ``build_week_frame``
matched ``build_league_frame``'s week-7 rows on every column, numeric and text, nulls
included. These pin the plumbing that makes that true without ESPN: which weeks are
fetched, and that both paths share one ingest.
"""

import pandas as pd
import pytest

import Scripts.equivalence as eq
import Scripts.scrape_player_stats as sps


class FakeLeague:
    """Just enough ``League`` for the week loop: it records which weeks were read."""

    def __init__(self, current_week=7, league_id=1, periods=None):
        self.current_week = current_week
        self.league_id = league_id
        self.loaded = []

        class settings:
            week_to_matchup_period = periods or {w: w for w in range(1, 18)}

        self.settings = settings

    def load_roster_week(self, week):
        self.loaded.append(week)

    def box_scores(self, week):
        return []                                   # no matchups: the loop is the point


@pytest.fixture
def league(monkeypatch):
    lg = FakeLeague()
    monkeypatch.setattr(sps, "fetch_league", lambda **kw: lg)
    import Scripts.scoring as scoring
    monkeypatch.setattr(scoring, "get_scoring_table", lambda league: pd.DataFrame())
    return lg


def test_the_full_fetch_reads_every_week_to_the_current_one(league):
    sps.get_ply_stats_by_matchup(1, 2026, "s", "e")
    assert league.loaded == [1, 2, 3, 4, 5, 6, 7]


def test_the_reblend_fetch_reads_the_current_week_alone(league):
    """The point of the argument: a full rebuild re-reads weeks 1..current every time."""
    sps.get_ply_stats_by_matchup(1, 2026, "s", "e", weeks=[7])
    assert league.loaded == [7]


def test_a_week_espn_has_no_box_scores_for_is_refused(league):
    with pytest.raises(ValueError, match="outside 1..7"):
        sps.get_ply_stats_by_matchup(1, 2026, "s", "e", weeks=[8])


def test_league_521152_runs_weeks_15_and_17_as_their_own_periods():
    two_week = {w: w for w in range(1, 15)} | {15: 15, 16: 15, 17: 16}
    assert sps.current_matchup_period(FakeLeague(17, 521152, two_week)) == 17
    assert sps.current_matchup_period(FakeLeague(17, 2, two_week)) == 16


@pytest.fixture
def ingest(monkeypatch):
    """Stub ESPN and the blend; record the ``weeks`` each build asked for."""
    asked = []
    lg = FakeLeague()
    import Scripts.fetch_utils as fu
    import Scripts.projection_utils as pu
    monkeypatch.setattr(eq, "resolve_league",
                        lambda name: {"ID": 1, "SWID": "s", "ESPN_S2": "e"})
    monkeypatch.setattr(fu, "fetch_league", lambda **kw: lg)

    def matchup(league_id, year, swid, espn_s2, weeks=None, league=None):
        assert league is lg, "the league fetched once is handed on, not fetched again"
        asked.append(weeks)
        return pd.DataFrame({"week": [7, 7], "player_name": ["A", "B"]})

    monkeypatch.setattr(sps, "get_ply_stats_by_matchup", matchup)
    monkeypatch.setattr(sps, "build_fa_market",
                        lambda league: pd.DataFrame({"week": [7, 7],
                                                     "player_name": ["B", "C"]}))
    monkeypatch.setattr(pu, "clean_lineups", lambda df, lg: df.reset_index(drop=True))
    return asked


def test_build_week_frame_asks_for_the_current_period_only(ingest):
    out = eq.build_week_frame("x", 2026)
    assert ingest == [[7]]
    # The same merge as the full build: a rostered row wins over the free-agent sample.
    assert out["player_name"].tolist() == ["A", "B", "C"]


def test_build_league_frame_still_asks_for_every_week(ingest):
    eq.build_league_frame("x", 2026)
    assert ingest == [None]
