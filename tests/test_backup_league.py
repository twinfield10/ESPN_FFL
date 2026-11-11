"""Tests for Scripts.backup_league -- the parts that do not need ESPN."""

import json
from pathlib import Path

import pytest

from Scripts import backup_league as bl


class FakeResponse:
    """Minimal stand-in for ``requests.Response``."""

    def __init__(self, status, payload=None, text=""):
        self.status_code = status
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class FakeSession:
    """Returns queued responses and records every request."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return self.responses.pop(0)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(bl.time, "sleep", lambda s: None)


def fetcher(tmp_path, season=2026, current=True, responses=()):
    return bl.Fetcher(123, season, current, tmp_path, FakeSession(responses))


# --- last_scoring_period ------------------------------------------------------

@pytest.mark.parametrize("status, expected", [
    (None, 18),
    ({}, 18),
    ({"status": {"latestScoringPeriod": 10, "finalScoringPeriod": 17}}, 10),
    ({"status": {"latestScoringPeriod": 18, "finalScoringPeriod": 17}}, 17),
    ({"status": {"finalScoringPeriod": 17}}, 17),
    ({"status": {}, "scoringPeriodId": 6}, 6),
])
def test_last_scoring_period(status, expected):
    assert bl.last_scoring_period(status) == expected


# --- endpoint choice ----------------------------------------------------------

def test_2019_onward_uses_seasons_endpoint_even_when_past(tmp_path):
    fx = fetcher(tmp_path, season=2023, current=False)
    assert fx.league_url() == f"{bl.API}/seasons/2023/segments/0/leagues/123"
    assert fx.base_params() == {}


def test_pre_2019_uses_league_history(tmp_path):
    fx = fetcher(tmp_path, season=2017, current=False)
    assert fx.league_url("/x") == f"{bl.API}/leagueHistory/123/x"
    assert fx.base_params() == {"seasonId": 2017}


# --- grab ---------------------------------------------------------------------

def test_grab_writes_json_and_logs_success(tmp_path):
    fx = fetcher(tmp_path, responses=[FakeResponse(200, {"a": 1})])
    assert fx.grab("league/mTeam", {"view": "mTeam"}, filt={"f": 1}) == {"a": 1}
    assert json.loads((tmp_path / "league/mTeam.json").read_text()) == {"a": 1}
    assert fx.log[0].ok
    call = fx.session.calls[0]
    assert call["params"] == {"view": "mTeam"}
    assert json.loads(call["headers"]["x-fantasy-filter"]) == {"f": 1}


def test_grab_failure_is_logged_and_written_not_raised(tmp_path):
    fx = fetcher(tmp_path, responses=[FakeResponse(404, text="nope")])
    assert fx.grab("activity/all_topics", optional=True) is None
    assert fx.log[0].ok is False and fx.log[0].optional
    assert json.loads((tmp_path / "activity/all_topics.json").read_text())["status"] == 404
    assert len(fx.session.calls) == 1  # a 404 is not retried


def test_grab_retries_server_errors(tmp_path):
    fx = fetcher(tmp_path, responses=[FakeResponse(503), FakeResponse(200, {"ok": True})])
    assert fx.grab("league/_base") == {"ok": True}
    assert len(fx.session.calls) == 2


def test_history_response_is_unwrapped(tmp_path):
    fx = fetcher(tmp_path, season=2017, current=False,
                 responses=[FakeResponse(200, [{"seasonId": 2017}])])
    assert fx.grab("league/_base") == {"seasonId": 2017}
    assert fx.session.calls[0]["params"] == {"seasonId": 2017}


def test_empty_history_response_is_a_failure(tmp_path):
    fx = fetcher(tmp_path, season=2017, current=False, responses=[FakeResponse(200, [])])
    assert fx.grab("league/_base") is None
    assert not fx.log[0].ok


# --- copy_local ---------------------------------------------------------------

def test_copy_local_takes_store_and_only_this_leagues_scoring(tmp_path, monkeypatch):
    store = tmp_path / "Store"
    (store / "2026" / "jeffs_league").mkdir(parents=True)
    (store / "2026" / "jeffs_league" / "draft.parquet").write_bytes(b"x")
    scoring = tmp_path / "scoring.csv"
    scoring.write_text(
        "season,league,display\n"
        "2026,jeffs_league,Jeffs_League\n"
        "2026,other,Other\n"
        "2025,jeffs_league,Jeffs_League\n"
    )
    monkeypatch.setattr(bl.paths, "STORE_DIR", store)
    monkeypatch.setattr(bl, "SCORING_CSV", scoring)

    dest = tmp_path / "out"
    assert bl.copy_local("jeffs_league", 2026, dest) == ["draft.parquet", "scoring_rules.csv"]
    assert (dest / "scoring_rules.csv").read_text().splitlines() == [
        "season,league,display", "2026,jeffs_league,Jeffs_League"]


def test_copy_local_with_nothing_on_disk(tmp_path, monkeypatch):
    monkeypatch.setattr(bl.paths, "STORE_DIR", tmp_path / "missing")
    monkeypatch.setattr(bl, "SCORING_CSV", tmp_path / "missing.csv")
    assert bl.copy_local("jeffs_league", 2026, tmp_path / "out") == []
    assert not (tmp_path / "out").exists()


# --- backups never land in a tracked path -------------------------------------

def test_backup_dir_is_inside_gitignored_data():
    assert bl.BACKUP_DIR.is_relative_to(bl.paths.DATA_DIR)
    gitignore = (bl.paths.REPO_ROOT / ".gitignore").read_text().splitlines()
    assert "Data/" in gitignore
