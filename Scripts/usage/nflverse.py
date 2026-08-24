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

from Scripts.paths import DATA_DIR, nfl_season_dir

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


# --- The role-resolving pulls, from R/GetAdvanced.R -------------------------
#
# Separate from the block above because they have a different failure mode. The
# opportunity and player_weeks pulls are the model's spine: without them there is no
# model, so :func:`_require` raises. These three are candidate features under test,
# and a season that lacks them should degrade to the model without them rather than
# refuse to run -- which is what :func:`load_advanced` returning an empty frame
# does. The feature layer left-joins, so absent reads as null reads as abstain.

#: The advanced parquets and the key each is unique on.
ADVANCED_FILES: Dict[str, Tuple[str, ...]] = {
    "routes": ("season", "week", "gsis_id"),
    "ngs": ("season", "week", "gsis_id"),
    "red_zone": ("season", "week", "gsis_id"),
}

#: Contract table, not season-scoped. A player's live contract may have been signed
#: five years ago, so filtering to what was knowable before a given season is a
#: feature-layer decision -- see :func:`Scripts.usage.features.contract_context`.
CONTRACTS_PARQUET = DATA_DIR / "NFL" / "contracts.parquet"


def advanced_path(season: int, name: str):
    """Path to one of :data:`ADVANCED_FILES` for a season. Not created."""
    if name not in ADVANCED_FILES:
        raise KeyError(f"Unknown advanced pull {name!r}. "
                       f"Known: {sorted(ADVANCED_FILES)}.")
    return nfl_season_dir(season, f"{name}.parquet")


def load_advanced(seasons: Sequence[int], name: str) -> pl.DataFrame:
    """One of the advanced player-week pulls, for several seasons.

    Missing seasons are skipped rather than raised on, unlike
    :func:`load_opportunity`. These are features under test: the model has to stay
    runnable for anyone who has not run ``R/GetAdvanced.R``, and a null feature is
    already a case every head handles.

    Args:
        seasons: Season years to read.
        name: One of :data:`ADVANCED_FILES`.

    Returns:
        pl.DataFrame: One row per ``(season, week, gsis_id)``. Empty with the right
        key schema when no season has been pulled.

    Raises:
        KeyError: On an unknown pull name.
    """
    frames = []
    for season in sorted(set(seasons)):
        path = advanced_path(season, name)
        if not path.is_file():
            continue
        frames.append(pl.read_parquet(path).with_columns(
            pl.col("season").cast(pl.Int32), pl.col("week").cast(pl.Int32)))

    if not frames:
        return pl.DataFrame(schema={"season": pl.Int32, "week": pl.Int32,
                                    "gsis_id": pl.String})
    return pl.concat(frames, how="diagonal").sort(["gsis_id", "season", "week"])


def load_contracts() -> pl.DataFrame:
    """Every contract OverTheCap records, unfiltered.

    Returns:
        pl.DataFrame: One row per contract with ``gsis_id`` and ``year_signed``.
        Empty with the key schema when ``R/GetAdvanced.R`` has not been run.
    """
    if not CONTRACTS_PARQUET.is_file():
        return pl.DataFrame(schema={"gsis_id": pl.String, "year_signed": pl.Int32})
    return pl.read_parquet(CONTRACTS_PARQUET).with_columns(
        pl.col("year_signed").cast(pl.Int32))


def advanced_available(seasons: Sequence[int]) -> Dict[str, List[int]]:
    """Which advanced pulls exist for which seasons.

    Reported rather than assumed, because a feature fitted on nine seasons and
    missing in the tenth is the failure this repo has already paid for once with
    the depth-chart schema change.

    Args:
        seasons: Season years to check.

    Returns:
        dict: Pull name to the sorted seasons present on disk.
    """
    return {name: sorted(s for s in seasons if advanced_path(s, name).is_file())
            for name in ADVANCED_FILES}


# --- The play-by-play archive -------------------------------------------------
#
# `R/GetAdvanced.R` downloads full play-by-play, derives `routes` and `red_zone`
# from it, and discards the other 370 columns. `R/GetPBP.R` keeps it, so a
# play-level question is a `group_by` rather than a new R script and a re-download.

#: Archive pulls `R/GetPBP.R` writes, and the grain each is keyed on.
#:
#: Unlike :data:`ADVANCED_FILES` these are **not** all player-week frames, which is
#: why the grain is recorded rather than assumed. Play-by-play and participation are
#: keyed on the *play* and carry no ``gsis_id`` at all.
#:
#: The three PFR pulls are player-week -- verified unique on
#: ``(season, week, pfr_player_id)`` and not on ``(season, pfr_player_id)`` -- but
#: they key on **PFR's** player id, not nflverse's. Joining them to anything in this
#: repo needs a crosswalk hop that does not exist yet; :mod:`Scripts.crosswalk` maps
#: ``gsis_id`` to ESPN and stops there.
#:
#: ``pfr_pass`` is the one plan 32 went looking for and could not find: it carries
#: ``times_pressured``, ``times_blitzed``, ``times_hurried``, ``times_hit`` and
#: ``times_sacked`` per quarterback per game, which is the pressure-rate and
#: offensive-line evidence that plan's phase 3 needs.
PBP_FILES: Dict[str, Tuple[str, ...]] = {
    "pbp": ("season", "week", "play_id"),
    "participation": ("season", "play_id"),
    "ftn_charting": ("season", "week", "nflverse_play_id"),
    "pfr_pass": ("season", "week", "pfr_player_id"),
    "pfr_rush": ("season", "week", "pfr_player_id"),
    "pfr_rec": ("season", "week", "pfr_player_id"),
}

#: Weeks a fantasy pipeline scores. Weeks 19-22 are the post-season.
REGULAR_SEASON_MAX_WEEK = 18


def pbp_path(season: int, name: str = "pbp"):
    """Path to one of :data:`PBP_FILES` for a season. Not created."""
    if name not in PBP_FILES:
        raise KeyError(f"Unknown play-by-play pull {name!r}. "
                       f"Known: {sorted(PBP_FILES)}.")
    return nfl_season_dir(season, f"{name}.parquet")


def pbp_seasons_available(candidates: Sequence[int],
                          name: str = "pbp") -> List[int]:
    """Which of ``candidates`` have this archive pull on disk.

    Worth calling before a wide read: the archive is ~26 MB a season, so a caller
    that wants 1999-2025 should know it is about to read 600 MB rather than
    discover it.

    Args:
        candidates: Season years to check.
        name: One of :data:`PBP_FILES`.

    Returns:
        list: Sorted seasons present.
    """
    return sorted(s for s in candidates if pbp_path(s, name).is_file())


def load_pbp(seasons: Sequence[int],
             columns: Optional[Sequence[str]] = None,
             season_type: Optional[str] = "REG",
             max_week: Optional[int] = REGULAR_SEASON_MAX_WEEK) -> pl.DataFrame:
    """Play-by-play for several seasons.

    **The filters are here rather than at write time, and that is the design.**
    Every other pull in this repo is filtered to regular-season weeks 1-18 before
    it reaches disk, because a fantasy pipeline never scores weeks 19-22 and
    playoff games would corrupt per-game denominators. Correct for a feature table;
    wrong for an archive, since a filter applied at write time cannot be undone and
    post-season snaps are real evidence about a player.

    So the file holds everything and this defaults to the same REG weeks 1-18 every
    existing caller already assumed. Pass ``season_type=None`` to see the rest.

    **Pass ``columns``.** Play-by-play is 372 columns and ~20 MB a season on disk;
    a projection pushed into the parquet read is the difference between a 600 MB
    load and a 20 MB one across the full archive.

    Args:
        seasons: Season years to read. Missing ones are skipped, not raised on --
            the archive starts where nflverse does and a caller asking for 1998
            should get an empty frame rather than a traceback.
        columns: Columns to read. None reads all 372.
        season_type: ``"REG"``, ``"POST"``, or None for both.
        max_week: Drop weeks above this. None keeps all.

    Returns:
        pl.DataFrame: One row per play, sorted by season, week and play id. Empty
        with a ``season``/``week`` schema when no season has been pulled.
    """
    frames = []
    for season in sorted(set(seasons)):
        path = pbp_path(season, "pbp")
        if not path.is_file():
            continue
        want = list(columns) if columns else None
        if want is not None:
            # A projection naming a column the season does not have would raise
            # mid-read, and the seasons genuinely differ -- `qb_epa` predates
            # `xpass`. Narrowing to what is really there keeps a 1999-2025 read
            # from failing on its oldest file.
            present = set(pl.scan_parquet(path).collect_schema().names())
            want = [c for c in want if c in present]
            for key in ("season", "week", "season_type"):
                if key in present and key not in want:
                    want.append(key)
        frames.append(pl.read_parquet(path, columns=want))

    if not frames:
        return pl.DataFrame(schema={"season": pl.Int32, "week": pl.Int32})

    out = pl.concat(frames, how="diagonal")
    if season_type is not None and "season_type" in out.columns:
        out = out.filter(pl.col("season_type") == season_type)
    if max_week is not None and "week" in out.columns:
        out = out.filter(pl.col("week") <= max_week)

    order = [c for c in ("season", "week", "play_id") if c in out.columns]
    return out.sort(order) if order else out


def load_pbp_annotation(seasons: Sequence[int], name: str) -> pl.DataFrame:
    """One of the non-play-by-play archive pulls, for several seasons.

    Participation, FTN charting and the three PFR tables. Skipped rather than
    raised on where absent, because absence is the normal case: participation
    starts in 2016, PFR in 2018 and FTN in 2022, so any read spanning the training
    window walks through all three boundaries.

    Args:
        seasons: Season years to read.
        name: One of :data:`PBP_FILES`, other than ``"pbp"``.

    Returns:
        pl.DataFrame: The concatenated frames. Empty with a ``season`` schema when
        no season has been pulled.

    Raises:
        KeyError: On an unknown pull name.
        ValueError: When called for ``"pbp"``, which has its own loader and its
            own filtering contract.
    """
    if name == "pbp":
        raise ValueError("Use load_pbp for play-by-play -- it filters season type "
                         "and week, which this does not.")
    frames = [pl.read_parquet(pbp_path(s, name))
              for s in sorted(set(seasons)) if pbp_path(s, name).is_file()]
    if not frames:
        return pl.DataFrame(schema={"season": pl.Int32})
    return pl.concat(frames, how="diagonal")


def load_pbp_meta(season: int) -> Optional[Dict]:
    """A season's ``pbp_meta.json``, or None when it has not been pulled.

    Args:
        season: Season year.

    Returns:
        dict: Release timestamps, row counts and the season-type breakdown.
    """
    path = nfl_season_dir(season, "pbp_meta.json")
    if not path.is_file():
        return None
    return json.loads(path.read_text())
