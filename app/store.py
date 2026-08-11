"""Cached, Polars-native reads over ``Data/Store``. The app's only data source.

Nothing here touches ESPN. The whole point of the store boundary is that a page
render is a parquet read -- measured at 11ms against 7.4s to rebuild the same
frame pre-season, and ~23s once a full season of box scores exists.

Frames come back as Polars per ``CLAUDE.md``, converting at the store boundary
even though ``clean_lineups`` upstream is Pandas.

**On the cache key.** Streamlit's ``cache_data`` hashes every argument whose name
does not start with an underscore, so the store's mtime has to be a real
parameter for a refresh to invalidate the cache. The public functions take
``(season, league_key)`` and pass the mtime through to a private cached
implementation -- so a refresh is picked up with no manual cache clear anywhere.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import polars as pl
import streamlit as st

from Scripts import store as _store

#: How long a cached frame survives without an mtime change. Short, because the
#: cost of a miss is one parquet read.
CACHE_TTL = 300


# --- cached implementations ----------------------------------------------

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _load_artifact(season: int, league_key: str, what: str, mtime: float) -> pl.DataFrame:
    """Read one artifact. ``mtime`` is part of the cache key, not used directly."""
    return pl.read_parquet(_store.require_artifact(season, league_key, what))


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _load_meta(season: int, league_key: str, mtime: float) -> dict:
    """Read ``meta.json``. ``mtime`` is part of the cache key, not used directly."""
    return _store.read_meta(season, league_key)


# --- public contract -----------------------------------------------------

def load_lineups(season: int, league_key: str) -> pl.DataFrame:
    """The blended lineup frame for one league-season.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        pl.DataFrame: ``clean_lineups`` output -- one row per player-week with
        ``ESPN_``/``FP_``/``MEAN_``/``PINNY_``/``BOL_``/``TRUE_`` stat columns and
        matching ``*_Points``.

    Raises:
        FileNotFoundError: When no store has been built for this league-season.
    """
    return _load_artifact(season, league_key, "lineups",
                          _store.store_mtime(season, league_key))


def load_team_stats(season: int, league_key: str) -> pl.DataFrame:
    """The team/matchup history for one league-season, when it has been built.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        pl.DataFrame: ``scrape_team_stats`` output.

    Raises:
        FileNotFoundError: When ``--what team_stats`` has not been run.
    """
    return _load_artifact(season, league_key, "team_stats",
                          _store.store_mtime(season, league_key))


def load_board(season: int, league_key: str) -> pl.DataFrame:
    """The draft board for one league-season, when it has been built.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        pl.DataFrame: ``Scripts.draft.board.build_board`` output -- VOR, tiers, ADP
        and value in this league's own scoring and roster shape.

    Raises:
        FileNotFoundError: When ``--what board`` has not been run.
    """
    return _load_artifact(season, league_key, "board",
                          _store.store_mtime(season, league_key))


def load_draft(season: int, league_key: str) -> pl.DataFrame:
    """Every pick this league has ever made, when the history has been built.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        pl.DataFrame: ``Scripts.draft.history.fetch_draft_history`` output -- one
        row per pick across every season the league has existed.

    Raises:
        FileNotFoundError: When ``--what draft`` has not been run.
    """
    return _load_artifact(season, league_key, "draft",
                          _store.store_mtime(season, league_key))


def load_tendencies(season: int, league_key: str) -> pl.DataFrame:
    """One row per manager: what they reliably do that the room does not.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        pl.DataFrame: ``Scripts.draft.tendencies.build_tendencies`` output.

    Raises:
        FileNotFoundError: When ``--what draft`` has not been run.
    """
    return _load_artifact(season, league_key, "tendencies",
                          _store.store_mtime(season, league_key))


def load_meta(season: int, league_key: str) -> dict:
    """The store's metadata: build time, current week, coverage, versions.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        dict: The ``meta.json`` payload.

    Raises:
        FileNotFoundError: When no store has been built for this league-season.
    """
    return _load_meta(season, league_key, _store.store_mtime(season, league_key))


def has_artifact(season: int, league_key: str, what: str) -> bool:
    """Whether one artifact exists, for deciding whether to offer a page.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        what: A :data:`Scripts.store.ARTIFACTS` key.

    Returns:
        bool: True when the parquet is on disk.
    """
    return _store.artifact_path(season, league_key, what).is_file()


# Re-exported so pages import one module rather than reaching past this one into
# Scripts.store for half their answers.
has_store = _store.has_store
is_stale = _store.is_stale
list_leagues = _store.list_leagues
list_seasons = _store.list_seasons
store_age_minutes = _store.store_age_minutes
