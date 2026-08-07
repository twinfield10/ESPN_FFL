"""Read the nflverse usage pulls that ``R/GetUsage.R`` writes.

The whole point of the extraction layer landing in parquet is that the modelling
side never talks to nflverse, so it is fast, offline and reproducible -- the same
separation ``Data/Store`` gives the app.

The stat names here are **ESPN's**, not nflverse's. That is deliberate: this repo
produces stat lines and scores them through each league's own rules, so a usage
model has to emit ``USG_receivingYards`` beside ``ESPN_receivingYards`` to be
priced by :func:`Scripts.projection_utils.proj_to_score`. Translating once, on
read, keeps the mapping in one place rather than scattered through the feature
code.

Polars throughout, per ``CLAUDE.md`` and ``STATE_OF_THE_REPO.md``'s "new code
should be Polars".
"""

import json
from typing import Dict, List, Optional, Sequence, Tuple

import polars as pl

from Scripts.paths import nfl_season_dir

#: ESPN scoring column -> (ffopportunity actual column, its expected counterpart).
#:
#: Only stats ffopportunity models **both** halves of are here, because the whole
#: design rests on separating earned opportunity from realised efficiency: a stat
#: with no ``_exp`` column has no opportunity half to separate. That excludes
#: fumbles (``rush_fumble_lost``/``rec_fumble_lost`` have no expectation) and
#: everything kicking or defensive, which ffopportunity does not model at all.
USAGE_STATS: Dict[str, Tuple[str, str]] = {
    "passingYards":         ("pass_yards_gained", "pass_yards_gained_exp"),
    "passingTouchdowns":    ("pass_touchdown",    "pass_touchdown_exp"),
    "passingInterceptions": ("pass_interception", "pass_interception_exp"),
    "rushingYards":         ("rush_yards_gained", "rush_yards_gained_exp"),
    "rushingTouchdowns":    ("rush_touchdown",    "rush_touchdown_exp"),
    "receivingYards":       ("rec_yards_gained",  "rec_yards_gained_exp"),
    "receivingTouchdowns":  ("rec_touchdown",     "rec_touchdown_exp"),
    "receivingReceptions":  ("receptions",        "receptions_exp"),
}

#: Prefix for the actual value of a stat in a usage frame.
ACTUAL_PREFIX = "act_"

#: Prefix for ffopportunity's expected value of a stat.
EXPECTED_PREFIX = "exp_"

#: Identity columns every usage frame carries.
ID_COLUMNS = ("season", "week", "gsis_id", "full_name", "position", "posteam")


def opportunity_path(season: int):
    """Path to a season's expected-production parquet. Not created."""
    return nfl_season_dir(season, "opportunity.parquet")


def player_weeks_path(season: int):
    """Path to a season's observed-usage parquet. Not created."""
    return nfl_season_dir(season, "player_weeks.parquet")


def meta_path(season: int):
    """Path to a season's ``usage_meta.json``. Not created."""
    return nfl_season_dir(season, "usage_meta.json")


def seasons_available(candidates: Sequence[int]) -> List[int]:
    """Which of ``candidates`` have been pulled.

    Args:
        candidates: Season years to check.

    Returns:
        list: Sorted seasons with an ``opportunity.parquet`` on disk.
    """
    return sorted(s for s in candidates if opportunity_path(s).is_file())


def load_meta(season: int) -> Optional[Dict]:
    """A season's pull metadata, including the upstream release timestamp.

    ffopportunity is ``ffverse`` release data rather than nflverse core, so it can
    stop updating mid-season while every file still reads fine. The recorded
    timestamp is how a stale pull becomes visible instead of silently reused --
    see the upstream-dependency risk in ``docs/plans/16-usage-data-layer.md``.

    Args:
        season: Season year.

    Returns:
        dict | None: The metadata, or None when the season has not been pulled.
    """
    path = meta_path(season)
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def _require(season: int):
    """Resolve a season's opportunity parquet, naming the command that builds it.

    Args:
        season: Season year.

    Returns:
        Path: The existing parquet.

    Raises:
        FileNotFoundError: When the season has not been pulled.
    """
    path = opportunity_path(season)
    if not path.is_file():
        raise FileNotFoundError(
            f"No usage data for {season} ({path} is missing). Pull it with "
            f"`Rscript R/GetUsage.R {season} {season}`."
        )
    return path


def teams_by_week(season: int) -> pl.DataFrame:
    """Which teams played in each week, derived from the usage data itself.

    Used to tell a bye week from an absence. Taken from the opportunity frame
    rather than from ``Data/NFL_Schedules.csv``, which holds whichever season
    ``GetNFL.R`` last wrote and would silently answer for the wrong year. A team
    with an offensive play that week played that week.

    Args:
        season: Season year.

    Returns:
        pl.DataFrame: Unique ``week`` and ``posteam``.

    Raises:
        FileNotFoundError: When the season has not been pulled.
    """
    return (
        pl.read_parquet(_require(season), columns=["week", "posteam"])
        .unique()
        .with_columns(pl.col("week").cast(pl.Int32))
    )


def load_opportunity(seasons: Sequence[int],
                     stats: Optional[Sequence[str]] = None) -> pl.DataFrame:
    """Expected and actual production per player-week, in ESPN stat names.

    Args:
        seasons: Season years to read. Each must have been pulled.
        stats: ESPN stat names to carry. Defaults to every key of
            :data:`USAGE_STATS`.

    Returns:
        pl.DataFrame: One row per ``(season, week, gsis_id)`` with
        :data:`ID_COLUMNS` plus ``act_<stat>`` and ``exp_<stat>`` per stat.

    Raises:
        FileNotFoundError: When a season has not been pulled.
        KeyError: On an unknown stat name.
    """
    stats = list(USAGE_STATS) if stats is None else list(stats)
    unknown = [s for s in stats if s not in USAGE_STATS]
    if unknown:
        raise KeyError(
            f"Unknown usage stat(s) {unknown}. Known: {sorted(USAGE_STATS)}."
        )

    wanted = list(ID_COLUMNS)
    renames: Dict[str, str] = {}
    for stat in stats:
        actual, expected = USAGE_STATS[stat]
        wanted += [actual, expected]
        renames[actual] = f"{ACTUAL_PREFIX}{stat}"
        renames[expected] = f"{EXPECTED_PREFIX}{stat}"

    frames = []
    for season in sorted(seasons):
        frame = pl.read_parquet(_require(season), columns=wanted)
        frames.append(frame.rename(renames))

    out = pl.concat(frames, how="vertical")
    value_columns = [c for c in out.columns if c not in ID_COLUMNS]
    return out.with_columns(
        pl.col(value_columns).cast(pl.Float64)
    ).sort(["gsis_id", "season", "week"])
