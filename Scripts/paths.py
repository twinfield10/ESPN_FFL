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

# NFL reference data written by R/GetNFL.R
NFL_SCHEDULE_CSV = DATA_DIR / "NFL_Schedules.csv"
NFL_TACKLES_CSV = DATA_DIR / "NFL_Tackles_By_Position.csv"


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


def season_dir(source: str, season: int, *parts: str) -> Path:
    """Season-scoped output directory for a projection source.

    Output paths carried no season component before 2026, so a new season's
    scrape merged into the previous season's files. Every scraper write goes
    through here now.

    Args:
        source: Provider directory name, e.g. ``"BetOnline"``.
        season: Season year.
        *parts: Additional components beneath the season directory.

    Returns:
        Path: An absolute path with parent directories created.
    """
    p = PROJECTIONS_DIR / source / "Season" / str(season)
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
