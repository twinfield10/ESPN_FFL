"""The app's read path, now that the store lives in S3.

Two things here are worth pinning beyond "it reads".

**The cache key must move.** Streamlit hashes a cached function's arguments, so if
the version string did not change when S3 changed, a nightly refresh would be
invisible to the app for the whole TTL. That is a silent-staleness bug of exactly the
kind this repo keeps finding, and it would look like the app working.

**The fallback must actually fall back.** ``ESPN_FFL_STORE_SOURCE=local`` is the
draft-morning escape hatch. A hatch that has never been opened is not a hatch.

No network: the S3 client is stubbed by the shared ``s3_stub`` fixture.
"""

import json
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from Scripts import paths, s3_store              # noqa: E402
from Scripts.store import ARTIFACTS, META_FILENAME  # noqa: E402

import store as app_store                        # noqa: E402


@pytest.fixture(autouse=True)
def _clear_streamlit_cache():
    """Streamlit's cache outlives a test. Without this, one test's frame answers
    another test's read and the backend under test never runs."""
    import streamlit as st
    st.cache_data.clear()
    yield
    st.cache_data.clear()


@pytest.fixture
def local_store(s3_stub, tmp_path, monkeypatch):
    """A real one-league store on disk, plus the stubbed S3 it can be pushed to."""
    monkeypatch.delenv("ESPN_FFL_STORE_SOURCE", raising=False)
    directory = paths.store_dir(2026, "knights_ffl", create=True)
    pl.DataFrame({"player": ["Bijan", "Puka"], "VOR": [41.2, 33.7]}).write_parquet(
        directory / ARTIFACTS["board"])
    (directory / META_FILENAME).write_text(json.dumps({
        "season": 2026, "league_key": "knights_ffl",
        "built_at": "2026-08-11T06:00:00-04:00",
        "artifacts": {"board": {"rows": 2, "cols": 2}},
    }))
    return s3_stub


# --- source resolution ---------------------------------------------------

def test_the_default_source_is_s3(monkeypatch):
    """S3 is the system of record; local is the exception you opt into."""
    monkeypatch.delenv("ESPN_FFL_STORE_SOURCE", raising=False)
    assert app_store.source() == "s3"


def test_a_typo_in_the_env_var_does_not_take_the_app_down(monkeypatch):
    """It falls back to the default rather than raising in a render path."""
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "S3://nonsense")
    assert app_store.source() == "s3"


def test_the_source_is_read_at_call_time_not_import(monkeypatch):
    """So it can be changed without restarting Streamlit."""
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "local")
    assert app_store.source() == "local"
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "s3")
    assert app_store.source() == "s3"


# --- reading through each backend ----------------------------------------

def test_local_mode_reads_disk(local_store, monkeypatch):
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "local")
    board = app_store.load_board(2026, "knights_ffl")
    assert board.shape == (2, 2) and board["player"].to_list() == ["Bijan", "Puka"]


def test_s3_mode_reads_the_bucket(local_store, monkeypatch):
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "s3")
    s3_store.push_league_store(2026, "knights_ffl")
    board = app_store.load_board(2026, "knights_ffl")
    assert board.shape == (2, 2) and board["VOR"].to_list() == [41.2, 33.7]


def test_s3_mode_does_not_silently_read_local(local_store, monkeypatch):
    """Nothing was pushed. Reading the local copy anyway would make an empty bucket
    look like a working one -- the failure mode that makes S3-as-record a lie."""
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "s3")
    with pytest.raises((FileNotFoundError, RuntimeError)):
        app_store.load_board(2026, "knights_ffl")


def test_meta_comes_back_from_whichever_backend_serves_it(local_store, monkeypatch):
    s3_store.push_league_store(2026, "knights_ffl")
    for mode in ("local", "s3"):
        monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", mode)
        assert app_store.load_meta(2026, "knights_ffl")["league_key"] == "knights_ffl"


# --- auto, the resilient mode --------------------------------------------

def test_auto_prefers_s3_when_it_has_the_league(local_store, monkeypatch):
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "auto")
    s3_store.push_league_store(2026, "knights_ffl")
    assert app_store._resolve(2026, "knights_ffl") == "s3"


def test_auto_falls_back_when_s3_has_nothing(local_store, monkeypatch):
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "auto")
    assert app_store._resolve(2026, "knights_ffl") == "local"
    assert app_store.load_board(2026, "knights_ffl").shape == (2, 2)


def test_auto_falls_back_when_s3_raises(local_store, monkeypatch):
    """No credentials, no network, no bucket. Surviving this is the whole point."""
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "auto")

    def explode(*a, **kw):
        raise RuntimeError("Unable to locate credentials")
    monkeypatch.setattr(s3_store, "prefix_fingerprint", explode)

    assert app_store._resolve(2026, "knights_ffl") == "local"
    assert app_store.load_board(2026, "knights_ffl").shape == (2, 2)


# --- the cache key, which is what makes a refresh visible ----------------

def test_the_version_moves_when_the_bucket_changes(local_store, monkeypatch):
    """If this stopped moving, a nightly refresh would be invisible for the TTL and
    the app would look entirely normal while showing yesterday's board."""
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "s3")
    s3_store.push_league_store(2026, "knights_ffl")
    before = app_store._version(2026, "knights_ffl", "s3")

    pl.DataFrame({"player": ["Bijan"], "VOR": [44.0]}).write_parquet(
        paths.store_dir(2026, "knights_ffl") / ARTIFACTS["board"])
    s3_store.push_league_store(2026, "knights_ffl")

    assert app_store._version(2026, "knights_ffl", "s3") != before


def test_a_refreshed_board_is_actually_re_read(local_store, monkeypatch):
    """The end-to-end version of the above, through the Streamlit cache."""
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "s3")
    s3_store.push_league_store(2026, "knights_ffl")
    assert app_store.load_board(2026, "knights_ffl").shape == (2, 2)

    pl.DataFrame({"player": ["Bijan"], "VOR": [44.0]}).write_parquet(
        paths.store_dir(2026, "knights_ffl") / ARTIFACTS["board"])
    s3_store.push_league_store(2026, "knights_ffl")

    assert app_store.load_board(2026, "knights_ffl").shape == (1, 2)


def test_the_two_backends_do_not_share_a_cache_entry(local_store, monkeypatch):
    """The backend is part of the key, so switching sources cannot serve a frame
    read from the other one."""
    assert (app_store._version(2026, "knights_ffl", "local")
            != app_store._version(2026, "knights_ffl", "s3"))


# --- artifact presence ---------------------------------------------------

def test_has_artifact_reads_the_meta_block_on_s3(local_store, monkeypatch):
    """meta.json already records which artifacts were written, and it is already
    cached -- so this costs no extra request."""
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "s3")
    s3_store.push_league_store(2026, "knights_ffl")
    assert app_store.has_artifact(2026, "knights_ffl", "board")
    assert not app_store.has_artifact(2026, "knights_ffl", "team_stats")


def test_has_artifact_is_false_rather_than_raising_with_no_store(monkeypatch,
                                                                s3_stub):
    """Pages call this to decide whether to render at all."""
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "s3")
    assert not app_store.has_artifact(2026, "nobody", "board")


def test_listing_leagues_and_seasons_routes_to_the_backend(local_store, monkeypatch):
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "local")
    assert app_store.list_leagues(2026) == ["knights_ffl"]

    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "s3")
    assert app_store.list_leagues(2026) == []
    s3_store.push_league_store(2026, "knights_ffl")
    assert app_store.list_leagues(2026) == ["knights_ffl"]
    assert app_store.list_seasons() == [2026]


# --- the escape hatch has to be discoverable -----------------------------

def test_an_s3_failure_names_the_env_var_that_reads_local(local_store, monkeypatch):
    """An error in a render path should say what to do about it."""
    monkeypatch.setenv("ESPN_FFL_STORE_SOURCE", "s3")
    s3_store.push_league_store(2026, "knights_ffl")

    def explode(*a, **kw):
        raise RuntimeError("connection reset by peer")
    monkeypatch.setattr(s3_store, "get_bytes", explode)

    with pytest.raises(RuntimeError, match="ESPN_FFL_STORE_SOURCE=local"):
        app_store.load_board(2026, "knights_ffl")
