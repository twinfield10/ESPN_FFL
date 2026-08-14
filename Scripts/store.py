"""The local data store: the boundary between ESPN ingest and presentation.

ESPN data was never persisted anywhere. The weekly pipeline fetched it, blended
it in memory, pushed it to Google Sheets and discarded it -- so re-examining last
week meant re-fetching it, and any UI that recomputed on interaction would cost
seconds per click. Measured on Knights_FFL: ~8s per league pre-season (only week
1 exists) rising toward ~23s with a full season of box scores, against 0.01s to
read the same result back from parquet.

So ingest and presentation are separated by files on disk. ``Scripts/refresh.py``
writes; ``app/`` only ever reads. Nothing in the render path talks to ESPN.

    Data/Store/<season>/<league_key>/
        lineups.parquet      # clean_lineups output
        team_stats.parquet    # scrape_team_stats history (opt-in)
        meta.json             # built_at, current_week, source coverage, versions

Two invariants make this safe to read while a refresh is writing:

1. **Artifacts are written atomically** -- to a ``.tmp`` sibling, then
   ``os.replace``. A half-written parquet read from the app would otherwise raise
   in the render path.
2. **``meta.json`` is written last**, and its presence is what
   :func:`has_store` keys on. A store without it is a store mid-build, not a
   store.

Scoring is deliberately not in here: ``extract_player_stats`` needs the
``colName`` list before it can pull any stats, so scoring is an *input* to
ingest, not an output. It lives in the registry -- see :mod:`Scripts.scoring`.

See ``docs/plans/07-frontend-foundation.md``.
"""

import datetime
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from Scripts import paths

#: Bump when a field changes meaning or is removed. Readers can then refuse a
#: store they do not understand rather than mis-render it.
SCHEMA_VERSION = 1

#: Artifact name -> filename. The keys double as the ``--what`` values accepted by
#: ``Scripts/refresh.py``.
#:
#: ``draft`` and ``tendencies`` are written together by one ``--what draft`` run
#: and kept as two artifacts rather than one: the picks are the evidence and the
#: tendencies are a reading of it, and every threshold in that reading is a
#: judgement call that will be revised. Storing only the reading would mean
#: re-pulling ten seasons from ESPN every time one of them moves.
ARTIFACTS = {
    "lineups": "lineups.parquet",
    "team_stats": "team_stats.parquet",
    "board": "board.parquet",
    "draft": "draft.parquet",
    "tendencies": "tendencies.parquet",
    # What was actually scored, and nothing else -- no projections, no blend.
    # `lineups` is the richer artifact and would be the obvious thing to read
    # instead, except that it *cannot be built for a past season*: it carries
    # FP_/PINNY_/BOL_ columns, and FantasyPros serves no season parameter, so
    # those inputs no longer exist for any season nobody archived them in. This
    # one needs only ESPN box scores, which are available from 2019. It is how
    # any question about how a season actually went reaches years before the
    # store existed. See docs/plans/25-results-backfill.md.
    "results": "results.parquet",
}

META_FILENAME = "meta.json"

#: Stats broken out individually in ``meta.json``'s coverage block. Same tuple
#: ``Scripts.projection_utils.print_coverage_report`` prints, so the header in the
#: app and the console report cannot disagree.
KEY_STATS = (
    "passingYards",
    "passingTouchdowns",
    "rushingYards",
    "receivingYards",
    "receivingReceptions",
)


# --- paths ---------------------------------------------------------------

def league_store_dir(season: int, league_key: str, *, create: bool = False) -> Path:
    """The directory holding one league-season's store.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        create: Create it and its parents.

    Returns:
        Path: The league-season directory.
    """
    return paths.store_dir(season, league_key, create=create)


def artifact_path(season: int, league_key: str, what: str) -> Path:
    """Path to one artifact within a league-season's store.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        what: An :data:`ARTIFACTS` key, e.g. ``"lineups"``.

    Returns:
        Path: The parquet location. Not created.

    Raises:
        KeyError: On an unknown artifact name, listing the valid ones.
    """
    if what not in ARTIFACTS:
        raise KeyError(
            f"Unknown store artifact {what!r}. Known: {sorted(ARTIFACTS)}."
        )
    return league_store_dir(season, league_key) / ARTIFACTS[what]


def meta_path(season: int, league_key: str) -> Path:
    """Path to a league-season's ``meta.json``. Not created."""
    return league_store_dir(season, league_key) / META_FILENAME


# --- writing -------------------------------------------------------------

def _write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    """Write ``df`` to ``path`` so no reader ever sees it half-written.

    The index is dropped first. Both ``clean_lineups`` and ``scrape_team_stats``
    build their output with ``pd.concat``, leaving a duplicated integer index that
    carries no information -- and pandas serialises a non-default index as a real
    parquet column named ``__index_level_0__``. Polars then reads that back as a
    470th column of a 469-column frame, which is how the app first reported the
    wrong width. Dropping it makes both readers agree on the schema.

    Args:
        df: Frame to write.
        path: Destination parquet path. Its parent must exist.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.reset_index(drop=True).to_parquet(tmp, index=False)
    os.replace(tmp, path)


def _git_sha() -> Optional[str]:
    """The repo's current short commit, for provenance in ``meta.json``.

    Returns:
        str | None: Short SHA, or None outside a git checkout or without git.
    """
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=paths.REPO_ROOT, capture_output=True, text=True, timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None


def _versions() -> Dict[str, Optional[str]]:
    """Versions of the packages whose behaviour the store's contents depend on.

    ``espn-api`` matters most: 0.46.0 repurposed ``points_breakdown`` from raw
    stats to applied points, so a store built against the wrong version holds
    point values where stats belong. Recording the version makes that diagnosable
    from the store alone.

    Returns:
        dict: Package name to version string, plus ``git_sha``.
    """
    versions: Dict[str, Optional[str]] = {}
    for module_name, dist in (("espn_api", "espn_api"), ("pandas", "pandas"),
                              ("polars", "polars")):
        try:
            versions[dist] = __import__(module_name).__version__
        except Exception:                                  # noqa: BLE001
            versions[dist] = None
    versions["git_sha"] = _git_sha()
    return versions


def coverage_summary(lineups: pd.DataFrame) -> Dict[str, Any]:
    """Per-source projection coverage, for ``meta.json``.

    Reuses :func:`Scripts.projection_utils.coverage_report`, written for plan 03,
    so the store reports the same numbers the console report prints. The point is
    that a degraded source shows in the app rather than being absorbed by
    imputation -- pre-season, Pinnacle and BetOnline are 0% real.

    Args:
        lineups: A ``clean_lineups`` frame, carrying ``*_is_imputed`` flags.

    Returns:
        dict: ``{"overall": {source: pct}, "key_stats": {stat: {source: pct}}}``.
        Empty dicts when the frame carries no coverage information.
    """
    # Deferred: projection_utils pulls in the ESPN and scoring stack, and the app
    # imports this module purely to read parquet.
    from Scripts.projection_utils import coverage_report

    report = coverage_report(lineups)
    if report.empty:
        return {"overall": {}, "key_stats": {}}

    overall = report.groupby("source")["real_pct"].mean().round(1).to_dict()
    by_stat = report.set_index(["stat", "source"])["real_pct"]
    key_stats: Dict[str, Dict[str, float]] = {}
    for stat in KEY_STATS:
        if stat in by_stat.index.get_level_values("stat"):
            key_stats[stat] = {
                source: round(float(pct), 1)
                for source, pct in by_stat.loc[stat].items()
            }
    return {"overall": {k: round(float(v), 1) for k, v in overall.items()},
            "key_stats": key_stats}


def build_meta(
    season: int,
    league_key: str,
    *,
    written: Dict[str, pd.DataFrame],
    league=None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble a league-season's ``meta.json`` payload.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        written: Artifact name to the frame just written, for row/column counts.
        league: The live ESPN ``League`` the artifacts came from, when available.
            Supplies ``current_week`` and the league's own name.
        extra: Additional keys merged in last -- display name, primary owner,
            per-source presence, and anything a caller wants recorded.

    Returns:
        dict: The payload :func:`write_league_store` serialises.
    """
    meta: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "season": int(season),
        "league_key": league_key,
        # Timezone-aware on purpose: is_stale() compares this against now, and a
        # naive timestamp makes that comparison ambiguous rather than wrong-by-an-
        # hour, which is worse.
        "built_at": datetime.datetime.now().astimezone().isoformat(),
        "artifacts": {
            name: {"rows": int(df.shape[0]), "cols": int(df.shape[1])}
            for name, df in written.items()
        },
        "versions": _versions(),
    }

    if league is not None:
        meta["league_name"] = getattr(league, "name", None)
        meta["current_week"] = int(getattr(league, "current_week", 0) or 0)
        week_map = getattr(getattr(league, "settings", None),
                           "week_to_matchup_period", None) or {}
        meta["current_matchup_period"] = int(
            week_map.get(meta["current_week"], meta["current_week"]))
        roster_settings = getattr(league, "roster_settings", {}) or {}
        meta["roster_slots"] = roster_settings.get("roster_slots", {})
        # Replacement level is starting slots x teams, so a board cannot be
        # recomputed or checked from the store without the team count.
        meta["starting_slots"] = roster_settings.get("starting_roster_slots", {})
        meta["team_count"] = len(getattr(league, "teams", []) or [])

    if "lineups" in written:
        meta["coverage"] = coverage_summary(written["lineups"])
        weeks = written["lineups"].get("week")
        meta["weeks_present"] = (
            sorted(int(w) for w in pd.unique(weeks.dropna())) if weeks is not None else []
        )

    if extra:
        meta.update(extra)
    return meta


def write_league_store(
    season: int,
    league_key: str,
    *,
    lineups: Optional[pd.DataFrame] = None,
    team_stats: Optional[pd.DataFrame] = None,
    board: Optional[pd.DataFrame] = None,
    draft: Optional[pd.DataFrame] = None,
    tendencies: Optional[pd.DataFrame] = None,
    results: Optional[pd.DataFrame] = None,
    league=None,
    meta_extra: Optional[Dict[str, Any]] = None,
) -> Path:
    """Write one league-season's artifacts and its ``meta.json``.

    Artifacts land atomically and ``meta.json`` is written last, so a concurrent
    reader sees either the previous complete store or the new one.

    Passing only some artifacts updates only those; the others are left in place
    and their existing ``meta.json`` entries are carried forward, so
    ``--what lineups`` does not make a previously built ``team_stats`` invisible.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        lineups: ``clean_lineups`` output.
        team_stats: ``scrape_team_stats`` output.
        board: ``Scripts.draft.board.build_board`` output.
        draft: ``Scripts.draft.history.fetch_draft_history`` output.
        tendencies: ``Scripts.draft.tendencies.build_tendencies`` output.
        results: ``get_ply_stats_by_matchup`` output, trimmed to what was
            scored. The only artifact buildable for a season before the store
            existed.
        league: The live ESPN ``League``, for metadata.
        meta_extra: Extra keys for ``meta.json``.

    Returns:
        Path: The league-season store directory.

    Raises:
        ValueError: When no artifact is supplied -- writing a store with no
            contents would only serve to refresh ``built_at``.
    """
    candidates = {"lineups": lineups, "team_stats": team_stats, "board": board,
                  "draft": draft, "tendencies": tendencies, "results": results}
    written = {name: df for name, df in candidates.items() if df is not None}
    if not written:
        raise ValueError(
            "write_league_store needs at least one artifact; got none. Passing "
            "none would move built_at without changing any data."
        )

    directory = league_store_dir(season, league_key, create=True)
    for name, df in written.items():
        _write_parquet_atomic(df, directory / ARTIFACTS[name])

    meta = build_meta(season, league_key, written=written, league=league,
                      extra=meta_extra)

    # Carry forward artifacts this call did not touch, so a lineups-only refresh
    # does not erase the record of an earlier team_stats backfill.
    previous = read_meta(season, league_key, missing_ok=True) or {}
    for name, entry in (previous.get("artifacts") or {}).items():
        if name in meta["artifacts"] or name not in ARTIFACTS:
            continue
        if (directory / ARTIFACTS[name]).is_file():
            meta["artifacts"][name] = entry

    tmp = directory / (META_FILENAME + ".tmp")
    tmp.write_text(json.dumps(meta, indent=2, sort_keys=True, default=str))
    os.replace(tmp, directory / META_FILENAME)
    return directory


# --- reading -------------------------------------------------------------

def has_store(season: int, league_key: str) -> bool:
    """Whether a complete store exists for a league-season.

    Keys on ``meta.json`` rather than on the parquet, because ``meta.json`` is
    written last -- so a directory with a parquet and no meta is a build in
    progress or one that failed partway.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        bool: True when the store is complete.
    """
    return meta_path(season, league_key).is_file()


def read_meta(season: int, league_key: str,
              *, missing_ok: bool = False) -> Optional[Dict[str, Any]]:
    """Read a league-season's ``meta.json``.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        missing_ok: Return None instead of raising when there is no store.

    Returns:
        dict | None: The metadata.

    Raises:
        FileNotFoundError: When no store exists and ``missing_ok`` is False, with
            the command that would build one.
    """
    path = meta_path(season, league_key)
    if not path.is_file():
        if missing_ok:
            return None
        raise FileNotFoundError(
            f"No store for {league_key} {season} ({path} is missing). Build one "
            f"with `python -m Scripts.refresh --league {league_key} "
            f"--season {season}`."
        )
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as e:
        if missing_ok:
            return None
        raise ValueError(f"{path} is not valid JSON: {e}") from e


def _restore_list_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Turn parquet's ndarray cells back into the lists that were written.

    ``clean_lineups`` carries ``eligiblePositions`` as a Python list per row.
    pyarrow writes that as a parquet list and reads it back as a ``numpy.ndarray``,
    so a straight round-trip is value-identical but not ``DataFrame.equals``-identical.
    That would make the store look like it had altered the frame, and would break
    any caller that expects a list.

    Detection reads the first non-null cell of each object column only, so the
    cost does not scale with the 469-column width of a real lineups frame.

    Args:
        df: Frame just read from parquet. Modified in place.

    Returns:
        pd.DataFrame: ``df``.
    """
    import numpy as np

    for col in df.columns[df.dtypes == object]:
        non_null = df[col].dropna()
        if non_null.empty or not isinstance(non_null.iloc[0], np.ndarray):
            continue
        df[col] = df[col].map(lambda v: v.tolist() if isinstance(v, np.ndarray) else v)
    return df


def require_artifact(season: int, league_key: str, what: str = "lineups") -> Path:
    """Resolve an artifact path, failing with the command that would build it.

    Shared by the pandas reader here and the Polars reader in ``app/store.py``, so
    both report a missing artifact the same way.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        what: An :data:`ARTIFACTS` key.

    Returns:
        Path: The existing parquet.

    Raises:
        FileNotFoundError: When the artifact has not been built.
    """
    path = artifact_path(season, league_key, what)
    if not path.is_file():
        raise FileNotFoundError(
            f"No {what} in the store for {league_key} {season} ({path} is "
            f"missing). Build it with `python -m Scripts.refresh --league "
            f"{league_key} --season {season} --what {what}`."
        )
    return path


def read_league_store(season: int, league_key: str,
                      what: str = "lineups") -> pd.DataFrame:
    """Read one artifact out of a league-season's store, as pandas.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        what: An :data:`ARTIFACTS` key.

    Returns:
        pd.DataFrame: The artifact, equal to what was written -- see
        :func:`_restore_list_columns`.

    Raises:
        FileNotFoundError: When the artifact has not been built, naming the
            command that builds it.
    """
    return _restore_list_columns(pd.read_parquet(require_artifact(season, league_key, what)))


def store_mtime(season: int, league_key: str) -> float:
    """Newest modification time across a league-season's store files.

    This is the app's cache key. Streamlit's ``cache_data`` hashes its arguments,
    so passing this in means a refresh invalidates the cache with no explicit
    clear anywhere.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        float: Unix mtime, or ``0.0`` when there is no store.
    """
    directory = league_store_dir(season, league_key)
    if not directory.is_dir():
        return 0.0
    times = [p.stat().st_mtime for p in directory.iterdir() if p.is_file()]
    return max(times) if times else 0.0


def list_leagues(season: int) -> List[str]:
    """League keys with a complete store for ``season``.

    Args:
        season: Season year.

    Returns:
        list: Sorted league keys. Directories without a ``meta.json`` are
        skipped -- see :func:`has_store`.
    """
    root = paths.store_root() / str(season)
    if not root.is_dir():
        return []
    return sorted(
        p.name for p in root.iterdir()
        if p.is_dir() and (p / META_FILENAME).is_file()
    )


def list_seasons() -> List[int]:
    """Seasons that have at least one complete league store.

    Returns:
        list: Season years, newest first.
    """
    root = paths.store_root()
    if not root.is_dir():
        return []
    seasons = []
    for p in root.iterdir():
        if not (p.is_dir() and p.name.isdigit()):
            continue
        if list_leagues(int(p.name)):
            seasons.append(int(p.name))
    return sorted(seasons, reverse=True)


def is_stale(meta: Optional[Dict[str, Any]], max_age_min: int = 60) -> bool:
    """Whether a store is old enough that the app should say so.

    Args:
        meta: A ``meta.json`` payload, or None.
        max_age_min: Age in minutes past which the store counts as stale.

    Returns:
        bool: True when stale. Also True when ``meta`` is None or ``built_at`` is
        missing or unparseable -- an unreadable build time is a reason to warn,
        not a reason to claim freshness.
    """
    age = store_age_minutes(meta)
    return age is None or age > max_age_min


def store_age_minutes(meta: Optional[Dict[str, Any]]) -> Optional[float]:
    """How long ago a store was built.

    Args:
        meta: A ``meta.json`` payload, or None.

    Returns:
        float | None: Age in minutes, or None when ``built_at`` is absent or
        unparseable.
    """
    if not meta or not meta.get("built_at"):
        return None
    try:
        built = datetime.datetime.fromisoformat(str(meta["built_at"]))
    except ValueError:
        return None
    now = datetime.datetime.now().astimezone()
    if built.tzinfo is None:
        # A store written by an older build, before built_at carried an offset.
        # Interpreting it as local time is the only available reading.
        built = built.astimezone()
    return (now - built).total_seconds() / 60.0
