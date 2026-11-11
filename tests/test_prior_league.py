"""A league recreated mid-season: config parsing, the week stitch, the freeze.

The case is Jeffs_League 2026, deleted after week 9 and recreated as a new ESPN id.
See ``Scripts/prior_league.py``.
"""

from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest

from Scripts import paths, prior_league
from Scripts.fetch_utils import pin_owner_names
from Scripts.prior_league import PriorSegment, parse_prior, segment_for, stitch_weeks


@pytest.fixture
def tmp_store(monkeypatch, tmp_path):
    """Redirect the store, the way tests/test_freeze.py does."""
    monkeypatch.setattr(paths, "STORE_DIR", tmp_path)
    return tmp_path


def _weeks(weeks, tag):
    return pd.DataFrame({"week": [float(w) for w in weeks], "src": tag})


# --- config ---------------------------------------------------------------

def test_no_prior_block_is_an_ordinary_league():
    assert parse_prior(None) is None
    assert parse_prior({}) is None


def test_prior_block_parses_with_integer_team_ids():
    seg = parse_prior({"id": 1650608988, "season": 2026, "through_week": 9,
                       "owners": {"2": "Jeff Wilhelm", 6: "Tommy Winfield"}})
    assert seg == PriorSegment(1650608988, 2026, 9,
                               {2: "Jeff Wilhelm", 6: "Tommy Winfield"})
    assert seg.first_week == 10


def test_half_written_prior_block_fails_loudly():
    # Read leniently it would fall back to fetching weeks ESPN cannot serve.
    with pytest.raises(ValueError, match="through_week"):
        parse_prior({"id": 1, "season": 2026})
    with pytest.raises(ValueError, match=">= 1"):
        parse_prior({"id": 1, "season": 2026, "through_week": 0})


def test_segment_applies_only_to_its_own_season():
    cfg = {"prior": PriorSegment(1, 2026, 9)}
    assert segment_for(cfg, 2026) is cfg["prior"]
    assert segment_for(cfg, 2027) is None
    assert prior_league.first_week(cfg, 2027) == 1
    assert prior_league.first_week(cfg, 2026) == 10
    assert prior_league.first_week({"prior": None}, 2026) == 1


# --- stitch ---------------------------------------------------------------

def test_stitch_takes_early_weeks_from_the_frozen_copy_only():
    # The new league's own weeks 1-9 are placeholders -- a 0-0 tie is not a result.
    fresh = _weeks([1, 9, 10, 11], "new")
    frozen = _weeks([1, 2, 9, 10], "old")
    out = stitch_weeks(fresh, frozen, through_week=9)
    assert out["week"].tolist() == [1, 2, 9, 10, 11]
    assert out["src"].tolist() == ["old", "old", "old", "new", "new"]


def test_stitch_without_a_frozen_copy_keeps_only_the_fresh_weeks():
    out = stitch_weeks(_weeks([9, 10], "new"), None, through_week=9)
    assert out["week"].tolist() == [10]


def test_stitch_passes_none_through():
    assert stitch_weeks(None, _weeks([1], "old"), through_week=9) is None


# --- owners ---------------------------------------------------------------

def test_pinned_owner_wins_over_espn_and_the_unknown_fallback():
    teams = [SimpleNamespace(team_id=2, owner="Unknown Owner (Car Wash Beers)"),
             SimpleNamespace(team_id=6, owner="Thomas Winfield"),
             SimpleNamespace(team_id=9, owner="Someone Else")]
    league = SimpleNamespace(teams=teams)
    pin_owner_names(league, {2: "Jeff Wilhelm", 6: "Tommy Winfield"})
    assert [t.owner for t in teams] == ["Jeff Wilhelm", "Tommy Winfield", "Someone Else"]


def test_empty_owner_map_changes_nothing():
    teams = [SimpleNamespace(team_id=1, owner="Logan Walker")]
    pin_owner_names(SimpleNamespace(teams=teams), {})
    assert teams[0].owner == "Logan Walker"


# --- freeze ---------------------------------------------------------------

def _seed_store(root, season=2026, key="jeffs_league"):
    d = root / str(season) / key
    d.mkdir(parents=True)
    _weeks(range(1, 11), "old").to_parquet(d / "lineups.parquet")
    _weeks(range(1, 11), "old").to_parquet(d / "team_stats.parquet")
    pd.DataFrame({"season": [2026], "pick": [1]}).to_parquet(d / "draft.parquet")
    return d


def test_freeze_cuts_weekly_artifacts_and_keeps_the_draft_whole(tmp_store):
    _seed_store(tmp_store)
    prior_league.freeze(2026, "jeffs_league", through_week=9)

    lineups = prior_league.read_prior(2026, "jeffs_league", "lineups")
    assert lineups["week"].max() == 9 and len(lineups) == 9
    assert len(prior_league.read_prior(2026, "jeffs_league", "draft")) == 1
    # Never built for this league: a gap, not an error.
    assert prior_league.read_prior(2026, "jeffs_league", "results") is None


def test_freeze_refuses_to_overwrite_what_cannot_be_rebuilt(tmp_store):
    _seed_store(tmp_store)
    prior_league.freeze(2026, "jeffs_league", through_week=9)
    with pytest.raises(FileExistsError, match="--force"):
        prior_league.freeze(2026, "jeffs_league", through_week=8)
    prior_league.freeze(2026, "jeffs_league", through_week=8, force=True)
    assert prior_league.read_prior(2026, "jeffs_league", "lineups")["week"].max() == 8


# --- draft ----------------------------------------------------------------

def test_replacement_season_draft_comes_from_the_frozen_copy(tmp_store):
    # The new league's draft is the offline roster entry; it must never be fetched.
    from Scripts.refresh import _draft_with_prior

    _seed_store(tmp_store)
    prior_league.freeze(2026, "jeffs_league", through_week=9)
    calls = []

    def fake_fetch(league_id, seasons, **_):
        calls.append(list(seasons))
        return pl.DataFrame({"season": list(seasons), "pick": [7] * len(seasons)})

    cfg = {"ID": 1976327133, "SWID": "s", "ESPN_S2": "e"}
    seg = PriorSegment(1650608988, 2026, 9)

    only_this_season = _draft_with_prior(cfg, range(2026, 2027), 2026,
                                         "jeffs_league", seg, fake_fetch)
    assert calls == [] and only_this_season["pick"].to_list() == [1]

    with_history = _draft_with_prior(cfg, range(2025, 2027), 2026,
                                     "jeffs_league", seg, fake_fetch)
    assert calls == [[2025]]
    assert sorted(with_history["season"].to_list()) == [2025, 2026]


# --- team_stats -----------------------------------------------------------

def test_team_stats_starts_at_the_cutover_and_carries_the_frozen_weeks(
        monkeypatch, tmp_store):
    """Weeks 1-9 go into the season's records; ESPN is asked only for 10 on."""
    from Scripts import refresh
    import Scripts.scrape_team_stats as sts

    _seed_store(tmp_store)
    prior_league.freeze(2026, "jeffs_league", through_week=9)
    seg = PriorSegment(1650608988, 2026, 9)
    cfg = {"ID": 1976327133, "SWID": "s", "ESPN_S2": "e", "start": 2026,
           "key": "jeffs_league", "display_name": "Jeffs_League", "prior": seg,
           "primary_own": "Tommy Winfield"}
    monkeypatch.setattr(refresh, "resolve_league", lambda name, config=None: cfg)

    seen = {}

    def fake_scrape(**kwargs):
        seen.update(kwargs)
        return pd.DataFrame({"year": [2026.0], "week": [10.0],
                             "team_owner": ["Tommy Winfield"],
                             "team_score": [0.0], "opp_score": [0.0]})

    monkeypatch.setattr(sts, "scrape_team_stats", fake_scrape)
    refresh.refresh_league("jeffs_league", 2026, what=["team_stats"])

    assert seen["league_id"] == 1976327133
    assert seen["first_week"] == 10
    assert seen["carried"]["week"].max() == 9


def test_team_stats_without_a_frozen_copy_refuses_rather_than_losing_weeks(
        monkeypatch, tmp_store):
    from Scripts import refresh

    cfg = {"ID": 1976327133, "SWID": "s", "ESPN_S2": "e", "start": 2026,
           "key": "jeffs_league", "display_name": "Jeffs_League",
           "prior": PriorSegment(1650608988, 2026, 9), "primary_own": "x"}
    monkeypatch.setattr(refresh, "resolve_league", lambda name, config=None: cfg)
    with pytest.raises(FileNotFoundError, match="--freeze"):
        refresh.refresh_league("jeffs_league", 2026, what=["team_stats"])
