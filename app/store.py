"""Cached, Polars-native reads over the store. The app's only data source.

Nothing here touches ESPN. The whole point of the store boundary is that a page
render is a parquet read -- measured at 11ms against 7.4s to rebuild the same frame
pre-season, and ~23s once a full season of box scores exists.

Frames come back as Polars per ``CLAUDE.md``, converting at the store boundary even
though ``clean_lineups`` upstream is Pandas.

**Where the store lives.** S3 is the system of record, so by default this reads
``s3://espn-ffl-data`` rather than ``Data/Store``. ``ESPN_FFL_STORE_SOURCE`` selects:

    s3      (default) read S3 only. A failure surfaces rather than being papered over.
    local             read Data/Store only. Offline, and what every writer still uses.
    auto              S3 when it has this league-season, otherwise local.

It is read at call time, not import, so it can be changed without restarting the app.
The escape hatch is not decoration: ``ESPN_FFL_STORE_SOURCE=local`` is the difference
between a slow morning and a lost one if S3 is unreachable on a draft day.

**On the cache key.** Streamlit's ``cache_data`` hashes every argument whose name does
not start with an underscore, so the store's version has to be a real parameter for a
refresh to invalidate the cache. Locally that is the newest mtime; against S3 it is
:func:`Scripts.s3_store.prefix_fingerprint`, a digest over every ETag under the
league's prefix, which costs **one** ``ListObjectsV2`` rather than a ``HeadObject``
per artifact. Same architecture either way -- the public functions take
``(season, league_key)`` and pass the version through to a private cached
implementation -- so a refresh is picked up with no manual cache clear anywhere.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import io
import json
import os

import polars as pl
import streamlit as st

from Scripts import s3_store as _s3
from Scripts import store as _store

#: How long a cached frame survives without a version change. Short, because the cost
#: of a miss is one parquet read.
CACHE_TTL = 300

#: Read when the environment says nothing.
DEFAULT_SOURCE = "s3"

VALID_SOURCES = ("s3", "local", "auto")


# --- source resolution ----------------------------------------------------

def source() -> str:
    """The configured store source, read at call time.

    Returns:
        str: One of :data:`VALID_SOURCES`. An unrecognised value falls back to the
        default rather than raising -- a typo in an env var should not take the app
        down, and the sidebar reports which source is actually in use.
    """
    configured = os.environ.get("ESPN_FFL_STORE_SOURCE", DEFAULT_SOURCE).lower()
    return configured if configured in VALID_SOURCES else DEFAULT_SOURCE


def _resolve(season: int, league_key: str) -> str:
    """Which backend actually serves this league-season: ``"s3"`` or ``"local"``.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        str: The backend to read from.
    """
    configured = source()
    if configured != "auto":
        return configured
    prefix = _s3.store_prefix(season, league_key)
    try:
        return "s3" if _s3.prefix_fingerprint(prefix) else "local"
    except Exception:                                       # noqa: BLE001
        # auto exists precisely to survive this: no credentials, no network, no
        # bucket. Falling back is the whole contract.
        return "local"


def _version(season: int, league_key: str, backend: str) -> str:
    """A string that changes whenever this league-season's store changes.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        backend: ``"s3"`` or ``"local"``.

    Returns:
        str: The cache-key component.
    """
    if backend == "s3":
        return "s3:" + _s3.prefix_fingerprint(_s3.store_prefix(season, league_key))
    return f"local:{_store.store_mtime(season, league_key)}"


# --- cached implementations ----------------------------------------------

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _load_artifact(season: int, league_key: str, what: str,
                   backend: str, version: str) -> pl.DataFrame:
    """Read one artifact. ``version`` is part of the cache key, not used directly."""
    if backend == "s3":
        raw = _s3.get_bytes(_s3.store_key(season, league_key, what))
        return pl.read_parquet(io.BytesIO(raw))
    return pl.read_parquet(_store.require_artifact(season, league_key, what))


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _load_meta(season: int, league_key: str,
               backend: str, version: str) -> dict:
    """Read ``meta.json``. ``version`` is part of the cache key, not used directly."""
    if backend == "s3":
        return json.loads(_s3.get_bytes(_s3.store_key(season, league_key, "meta")))
    return _store.read_meta(season, league_key)


def _artifact(season: int, league_key: str, what: str) -> pl.DataFrame:
    """Resolve the backend and read one artifact through the cache.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        what: A :data:`Scripts.store.ARTIFACTS` key.

    Returns:
        pl.DataFrame: The artifact.

    Raises:
        FileNotFoundError: When the artifact has not been built, naming the command
            that builds it and -- on the S3 path -- the env var that reads local.
    """
    backend = _resolve(season, league_key)
    try:
        return _load_artifact(season, league_key, what, backend,
                              _version(season, league_key, backend))
    except FileNotFoundError:
        raise
    except Exception as e:                                  # noqa: BLE001
        if backend != "s3":
            raise
        raise RuntimeError(
            f"Could not read {what} for {league_key} {season} from "
            f"s3://{_s3.BUCKET}: {type(e).__name__}: {e}. To read the local copy "
            f"instead, set ESPN_FFL_STORE_SOURCE=local."
        ) from e


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
    return _artifact(season, league_key, "lineups")


def load_results(season: int, league_key: str) -> pl.DataFrame:
    """What was actually scored in one league-season, with no projections in it.

    The artifact that reaches back past the store. ``lineups`` is richer and
    cannot be built for a finished season -- it carries FantasyPros columns and
    FantasyPros serves no season parameter -- whereas this needs only ESPN box
    scores, which exist from 2019.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        pl.DataFrame: One row per player-week: ``week``, ``team_owner``,
        ``team_name``, ``player_id``, ``player_name``, ``slotPosition``,
        ``primaryPosition``, ``points``. ``slotPosition`` matters -- ``BE`` and
        ``IR`` points counted for nobody.

    Raises:
        FileNotFoundError: When this league-season has no results artifact.
    """
    return _artifact(season, league_key, "results")


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
    return _artifact(season, league_key, "team_stats")


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
    return _artifact(season, league_key, "board")


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
    return _artifact(season, league_key, "draft")


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
    return _artifact(season, league_key, "tendencies")


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
    backend = _resolve(season, league_key)
    return _load_meta(season, league_key, backend,
                      _version(season, league_key, backend))


def has_artifact(season: int, league_key: str, what: str) -> bool:
    """Whether one artifact exists, for deciding whether to offer a page.

    On the S3 path this reads ``meta.json``'s ``artifacts`` block rather than issuing
    a ``HeadObject`` per artifact -- the metadata already records exactly which ones
    were written, and it is already cached, so the check costs nothing.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        what: A :data:`Scripts.store.ARTIFACTS` key.

    Returns:
        bool: True when the artifact is present.
    """
    if _resolve(season, league_key) == "local":
        return _store.artifact_path(season, league_key, what).is_file()
    try:
        meta = load_meta(season, league_key)
    except (FileNotFoundError, RuntimeError):
        return False
    return what in (meta.get("artifacts") or {})


def has_store(season: int, league_key: str) -> bool:
    """Whether a complete store exists for a league-season.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        bool: True when the store is complete -- keyed on ``meta.json``, which is
        written and uploaded last.
    """
    if _resolve(season, league_key) == "local":
        return _store.has_store(season, league_key)
    return _s3.has_store(season, league_key)


def list_leagues(season: int) -> list:
    """League keys with a complete store for ``season``.

    Args:
        season: Season year.

    Returns:
        list: Sorted league keys.
    """
    configured = source()
    if configured == "local":
        return _store.list_leagues(season)
    try:
        found = _s3.list_leagues(season)
    except Exception:                                       # noqa: BLE001
        if configured == "s3":
            raise
        return _store.list_leagues(season)
    if found or configured == "s3":
        return found
    return _store.list_leagues(season)


def list_seasons() -> list:
    """Seasons that have at least one complete league store.

    Returns:
        list: Season years, newest first.
    """
    configured = source()
    if configured == "local":
        return _store.list_seasons()
    try:
        found = _s3.list_seasons()
    except Exception:                                       # noqa: BLE001
        if configured == "s3":
            raise
        return _store.list_seasons()
    if found or configured == "s3":
        return found
    return _store.list_seasons()


# Pure functions over a meta payload -- no backend involved, so they are re-exported
# unchanged rather than wrapped.
is_stale = _store.is_stale
store_age_minutes = _store.store_age_minutes
