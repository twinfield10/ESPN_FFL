"""The global selection: season pinning, week options, the free-agent guard.

Covers the pure half of ``app/session.py``. ``render_context`` draws widgets and needs
a Streamlit runtime, so it is exercised by the app-level smoke checks rather than
here; everything that makes a *decision* is below.

Two of these pin defects that were live when the four-tab app was built:

* ``available_weeks`` -- ``meta["weeks_present"]`` is absent from every 2026
  ``meta.json`` on disk, so the previous ``meta.get("weeks_present") or
  [current_week]`` offered exactly ``[1]`` and would have gone on offering ``[1]`` in
  December.
* ``team_owners`` -- ``lineups.parquet`` carries the free-agent pool as extra rows on
  a synthetic team, so any owner picker that does not exclude it offers "Free Agent"
  as a team you can look at.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

# `app/` is not a package: Streamlit runs the page scripts directly with app/ on
# sys.path, so the modules import each other by bare name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import session  # noqa: E402
import store  # noqa: E402


@pytest.fixture(autouse=True)
def no_cache():
    """Clear the cached season between tests.

    ``current_season`` is ``st.cache_data``-wrapped, and without a runtime the cache
    is process-global -- so one test's monkeypatched store would answer the next.
    """
    session.current_season.clear()
    yield
    session.current_season.clear()


# --- season pinning ------------------------------------------------------

def test_the_season_comes_from_the_schedule(monkeypatch):
    """Resolved rather than hardcoded, so the annual rollover stays one R script.

    `Data/NFL_Schedules.csv` is already the declared source of truth for both season
    and week, so reading it here means the app cannot disagree with the pipeline
    about what year it is.
    """
    monkeypatch.setattr(store, "list_seasons", lambda: [2026, 2025])
    from Scripts import nfl_utils
    monkeypatch.setattr(nfl_utils, "current_season", lambda: 2026)
    assert session.current_season() == 2026


def test_a_schedule_season_with_no_store_falls_back_to_disk(monkeypatch):
    """A season the app cannot read is worse than one a year stale.

    This is also the offline case: the schedule CSV lives under `Data/`, which a
    machine reading the store from S3 need not have.
    """
    monkeypatch.setattr(store, "list_seasons", lambda: [2025, 2024])
    from Scripts import nfl_utils
    monkeypatch.setattr(nfl_utils, "current_season", lambda: 2027)
    assert session.current_season() == 2025


def test_an_unreadable_schedule_falls_back_to_disk(monkeypatch):
    monkeypatch.setattr(store, "list_seasons", lambda: [2026, 2025])
    from Scripts import nfl_utils

    def explode():
        raise FileNotFoundError("no schedule")

    monkeypatch.setattr(nfl_utils, "current_season", explode)
    assert session.current_season() == 2026


def test_no_store_at_all_is_an_error_the_caller_turns_into_a_message(monkeypatch):
    """The state a fresh clone launches in, so it gets a real message not a
    traceback -- see `header.no_store_message`."""
    monkeypatch.setattr(store, "list_seasons", lambda: [])
    with pytest.raises(RuntimeError, match="complete store"):
        session.current_season()


# --- week options --------------------------------------------------------

def test_recorded_weeks_are_used_when_the_store_has_them():
    assert session.available_weeks(
        2026, "knights_ffl", {"weeks_present": [3, 1, 2]}) == [1, 2, 3]


def test_weeks_fall_back_to_the_artifact_when_the_metadata_is_silent(monkeypatch):
    """The live defect: no 2026 meta.json carries `weeks_present`, so the dropdown
    offered [1] and always would have. Reading the artifact is what makes the
    control work at all."""
    monkeypatch.setattr(
        store, "load_lineups",
        lambda season, league: pl.DataFrame({"week": [1, 1, 2, 3]}))
    assert session.available_weeks(2026, "knights_ffl",
                                   {"current_week": 1}) == [1, 2, 3]


def test_weeks_fall_back_to_the_current_week_with_no_artifact(monkeypatch):
    """The pre-draft state the app launches in."""
    def explode(season, league):
        raise FileNotFoundError("no lineups")

    monkeypatch.setattr(store, "load_lineups", explode)
    assert session.available_weeks(2026, "knights_ffl", {"current_week": 4}) == [4]


def test_weeks_are_never_empty(monkeypatch):
    def explode(season, league):
        raise FileNotFoundError("no lineups")

    monkeypatch.setattr(store, "load_lineups", explode)
    assert session.available_weeks(2026, "knights_ffl", {}) == [1]


def test_an_artifact_with_no_weeks_falls_through(monkeypatch):
    monkeypatch.setattr(store, "load_lineups",
                        lambda season, league: pl.DataFrame({"week": []}))
    assert session.available_weeks(2026, "knights_ffl", {"current_week": 2}) == [2]


# --- owners --------------------------------------------------------------

def test_the_free_agent_pool_is_not_a_team():
    """It arrives as a synthetic `team_owner`, which is what makes the Free Agents
    tab a filter rather than a second ingest -- and a trap for any owner picker."""
    frame = pl.DataFrame({"team_owner": ["Tommy", "Free Agent", "Andrew",
                                         "Free Agent"]})
    assert session.team_owners(frame) == ["Andrew", "Tommy"]


def test_nulls_are_not_owners():
    frame = pl.DataFrame({"team_owner": ["Tommy", None]})
    assert session.team_owners(frame) == ["Tommy"]


def test_a_frame_with_no_owner_column_has_no_owners():
    assert session.team_owners(pl.DataFrame({"x": [1]})) == []


def test_the_free_agent_sentinel_matches_what_espn_sends():
    assert session.FREE_AGENT_OWNER == "Free Agent"
