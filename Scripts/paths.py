"""Canonical filesystem locations for the repo.

Every data path in the codebase used to be a bare relative string like
``"Data/Projections/..."``, which only resolved when the interpreter's working
directory happened to be the repo root. That made the scrapers (run from
``Scripts/``) and the analysis modules (run from the root) mutually
incompatible. Resolving everything against ``REPO_ROOT`` here removes the
working-directory dependency entirely.
"""

from pathlib import Path
from typing import Optional, Union

# Scripts/paths.py -> Scripts/ -> repo root
REPO_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = REPO_ROOT / "Data"
PROJECTIONS_DIR = DATA_DIR / "Projections"

#: The local data store that separates ESPN ingest from presentation. Written by
#: ``Scripts/refresh.py``, read by ``app/``, and gitignored -- it is regenerable
#: and large. See ``docs/plans/07-frontend-foundation.md``.
STORE_DIR = DATA_DIR / "Store"

# NFL reference data written by R/GetNFL.R
NFL_SCHEDULE_CSV = DATA_DIR / "NFL_Schedules.csv"
NFL_TACKLES_CSV = DATA_DIR / "NFL_Tackles_By_Position.csv"

#: Cross-provider player identity table written by ``R/GetPlayerIDs.R``. Not
#: season-scoped -- a player's ids do not change year to year.
PLAYER_IDS_PARQUET = DATA_DIR / "NFL" / "player_ids.parquet"

#: Hand-maintained injury severity, one file per season.
#:
#: **Tracked in git, unlike everything under ``Data/``**, and that is the whole reason it
#: lives here. The file holds judgements a human made from a beat report -- severity no feed
#: carries -- each stamped with an ``as_of`` and a ``source``. Version history is the point:
#: it records what was believed and when, which is exactly what you want to re-read in
#: November when a call made in August turns out wrong.
#:
#: Note the ``.gitignore`` nuance. ``config*.yaml`` is ignored so that ``config.yaml``'s
#: league ids never leave the machine, and that pattern matches on *basename*, so
#: ``config/injuries/2026.yaml`` is unaffected. An explicit un-ignore sits beside it anyway,
#: so the intent survives a future edit to the ignore rules.
INJURY_OVERRIDES_DIR = REPO_ROOT / "config" / "injuries"

#: Injury episodes and their matched healthy control, written by
#: ``python -m Scripts.injury.episodes --rebuild``.
#:
#: Deliberately **not** season-scoped, and deliberately not in
#: :data:`Scripts.store.ARTIFACTS`. Not season-scoped because pooling across seasons is
#: the entire point -- a body part yields 40-220 episodes over ten years and single-digit
#: counts within one. Not a store artifact because the store is league-scoped and
#: ``artifact_path`` takes a ``league_key``: an injury to a running back is the same
#: injury in all nine leagues, and putting it there would imply nine copies of it.
INJURY_EPISODES_PARQUET = DATA_DIR / "NFL" / "injury_episodes.parquet"
INJURY_POST_RETURN_PARQUET = DATA_DIR / "NFL" / "injury_post_return.parquet"
INJURY_CONTROLS_PARQUET = DATA_DIR / "NFL" / "injury_controls.parquet"
INJURY_META_JSON = DATA_DIR / "NFL" / "injury_meta.json"


def nfl_season_dir(season: int, *parts: str, create: bool = False) -> Path:
    """Season-scoped NFL reference data, written by the ``R/Get*.R`` scripts.

    Holds ``NFL_Stats.csv`` from ``GetNFL.R`` and the nflverse usage pulls from
    ``GetUsage.R``. Unlike :func:`season_dir` this does **not** create by
    default: readers ask whether a season has been pulled, and a read that
    created the directory would make an unpulled season look pulled.

    Args:
        season: Season year.
        *parts: Additional components beneath the season directory.
        create: Create the directory and its parents. Writers set this.

    Returns:
        Path: An absolute path.
    """
    p = DATA_DIR / "NFL" / str(season)
    if create:
        p.mkdir(parents=True, exist_ok=True)
    return p.joinpath(*parts)


def resolve(path: Union[str, Path]) -> Path:
    """Resolve ``path`` against the repo root unless it is already absolute.

    Args:
        path: Absolute path, or a path relative to the repo root.

    Returns:
        Path: An absolute path.
    """
    p = Path(path)
    return p if p.is_absolute() else REPO_ROOT / p


def projection_path(source: str, *parts: str, season: Optional[int] = None) -> Path:
    """Build a path under ``Data/Projections/<source>``.

    Args:
        source: Provider directory name, e.g. ``"FantasyPros"``, ``"BetOnline"``.
        *parts: Additional path components, e.g. ``"Season"``, ``"file.parquet"``.
        season: Optional season year. When given it is inserted directly beneath
            the source directory so that seasons cannot overwrite one another.

    Returns:
        Path: An absolute path. Parent directories are not created.
    """
    base = PROJECTIONS_DIR / source
    if season is not None:
        base = base / str(season)
    return base.joinpath(*parts)


def season_dir(source: str, season: int, *parts: str, create: bool = True) -> Path:
    """Season-scoped output directory for a projection source.

    Output paths carried no season component before 2026, so a new season's
    scrape merged into the previous season's files. Every scraper write goes
    through here now.

    Args:
        source: Provider directory name, e.g. ``"BetOnline"``.
        season: Season year.
        *parts: Additional components beneath the season directory.
        create: Create the season directory and its parents. True by default so
            writers need no ceremony. **Readers should pass False** -- otherwise
            merely asking whether a source has a file for a season creates an
            empty directory for it, which is how three ``2999/`` directories once
            appeared under ``Data/Projections`` from a test that only checked
            ``.exists()``.

    Returns:
        Path: An absolute path.
    """
    p = PROJECTIONS_DIR / source / "Season" / str(season)
    if create:
        p.mkdir(parents=True, exist_ok=True)
    return p.joinpath(*parts)


def store_root() -> Path:
    """The store root, read at call time.

    Resolved through a function rather than referenced directly so that tests can
    redirect the store with ``monkeypatch.setattr(paths, "STORE_DIR", tmp_path)``.
    A module that did ``from Scripts.paths import STORE_DIR`` would bind the
    original value at import time and ignore the patch.

    Returns:
        Path: :data:`STORE_DIR`.
    """
    return STORE_DIR


def store_dir(season: int, league_key: str, *parts: str, create: bool = False) -> Path:
    """Season- and league-scoped store directory.

    Unlike :func:`season_dir` and :func:`landing_dir`, this does **not** create
    parents by default. Reads go through here too, and a read that creates an
    empty directory would make it show up in ``store.list_leagues()`` as a league
    that has a store when it does not.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key, e.g. ``"knights_ffl"``. The key
            rather than the display name, so paths hold no spaces.
        *parts: Additional components beneath the league directory.
        create: Create the league directory and its parents. Writers set this.

    Returns:
        Path: An absolute path.
    """
    p = store_root() / str(season) / league_key
    if create:
        p.mkdir(parents=True, exist_ok=True)
    return p.joinpath(*parts)


def landing_dir(source: str, season: int, *parts: str) -> Path:
    """Season-scoped landing (raw scrape) directory for a projection source.

    Args:
        source: Provider directory name, e.g. ``"Pinnacle"``.
        season: Season year.
        *parts: Additional components beneath the season directory.

    Returns:
        Path: An absolute path with parent directories created.
    """
    p = PROJECTIONS_DIR / source / "Landing" / str(season)
    p.mkdir(parents=True, exist_ok=True)
    return p.joinpath(*parts)
