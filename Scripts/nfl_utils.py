"""NFL schedule reference data and the season/week the pipeline is processing.

The schedule CSV is produced by ``R/GetNFL.R`` and is the source of truth for
both the current week and the current season -- deriving them from the same
file guarantees the two can never disagree.

This module used to read the CSV at import time against a bare relative path,
which meant importing it from anywhere other than the repo root raised
``FileNotFoundError``. Loading is now lazy and repo-root-relative. The
module-level ``NFL_SCHEDULE`` and ``DATE_WEEK`` names still work -- they are
resolved on first access via :pep:`562` -- so existing callers are unaffected.
"""

from typing import Optional

import polars as pl

from Scripts.paths import NFL_SCHEDULE_CSV

_SCHEDULE: Optional[pl.DataFrame] = None

#: ESPN's team abbreviations that differ from the schedule's, ESPN spelling first.
#:
#: Lives here rather than beside its first caller because it is a fact about the
#: schedule file, and two modules now need it: the draft board's bye weeks and
#: :mod:`Scripts.game_state`'s offline fallback. ``Scripts.draft.board`` re-exports
#: it under the same name, so nothing that already imports it from there breaks.
ESPN_TEAM_ALIASES = {"LAR": "LA", "WSH": "WAS"}

#: The schedule's spellings, keyed by ESPN's. The inverse of
#: :data:`ESPN_TEAM_ALIASES`, for going the other way.
SCHEDULE_TEAM_ALIASES = {v: k for k, v in ESPN_TEAM_ALIASES.items()}



def load_schedule(refresh: bool = False) -> pl.DataFrame:
    """Load and cache the NFL schedule.

    Args:
        refresh: Re-read from disk instead of returning the cached frame.

    Returns:
        pl.DataFrame: The full schedule as written by ``R/GetNFL.R``.

    Raises:
        FileNotFoundError: If the schedule CSV has not been generated yet.
        ValueError: If the schedule contains anything other than regular-season
            games.
    """
    global _SCHEDULE
    if _SCHEDULE is None or refresh:
        if not NFL_SCHEDULE_CSV.exists():
            raise FileNotFoundError(
                f"{NFL_SCHEDULE_CSV} is missing. Generate it with R/GetNFL.R "
                "before running the scrapers."
            )
        _SCHEDULE = _validate(
            pl.read_csv(NFL_SCHEDULE_CSV, infer_schema_length=10000)
        )
    return _SCHEDULE


def _validate(sched: pl.DataFrame) -> pl.DataFrame:
    """Reject a schedule that is not purely regular-season.

    ``R/GetNFL.R`` filters to ``game_type == 'REG'``, but the file on disk can be
    stale, hand-edited, or produced by an older version of the script. Fantasy
    scores the regular season only, and both :func:`current_week` and
    :func:`date_week` assume every row is a regular-season game -- a postseason or
    preseason row shifts week detection and mis-assigns props rather than failing.

    Args:
        sched: The schedule as read from disk.

    Returns:
        pl.DataFrame: ``sched`` unchanged, when it validates.

    Raises:
        ValueError: If a non-``REG`` ``game_type`` is present.
    """
    if "game_type" not in sched.columns:
        return sched
    other = sorted(set(sched["game_type"].unique().to_list()) - {"REG"})
    if other:
        raise ValueError(
            f"{NFL_SCHEDULE_CSV} contains non-regular-season game types "
            f"{other}. Fantasy scores the regular season only, and week "
            f"detection assumes every row is a regular-season game. Regenerate "
            f"with `Rscript R/GetNFL.R <season>`."
        )
    return sched


def current_season() -> int:
    """The season the schedule file covers.

    Returns:
        int: Season year, e.g. ``2026``.

    Raises:
        ValueError: If the schedule spans multiple seasons, which would make
            every season-keyed output path ambiguous.
    """
    seasons = load_schedule()["season"].unique().to_list()
    if len(seasons) != 1:
        raise ValueError(
            f"{NFL_SCHEDULE_CSV} covers multiple seasons {sorted(seasons)}; "
            "regenerate it for a single season."
        )
    return int(seasons[0])


def current_week() -> int:
    """The first week that still has unplayed games.

    Returns:
        int: Week number. Returns ``1`` before the season opens and the final
        week once every game has a score, rather than the null the previous
        implementation produced (which crashed ``range(1, WEEK + 1)``).
    """
    sched = load_schedule()
    unplayed = sched.filter(pl.col("away_score").cast(pl.Utf8, strict=False) == "NA")
    if unplayed.height:
        return int(unplayed["week"].min())
    played = sched["week"]
    return int(played.max()) if played.len() else 1


def is_preseason() -> bool:
    """Whether the season has yet to start -- no game in the schedule has a score.

    **This is the repo's notion of "cold", and until now it existed only as shell.**
    ``run_daily_refresh.sh`` derived exactly this predicate inline as ``SEASON_STARTED``
    and used it for one thing: deciding whether an empty sportsbook market is a failure
    or a book winding down a season-long line. Nothing in Python could ask the question,
    so every other phase-dependent decision was carried as prose -- most expensively the
    draft board, which is a pre-season artifact rebuilt nightly all year because the only
    thing that knew otherwise was a comment.

    Same evidence, opposite meaning, which is why the predicate is worth naming once:
    an empty market pre-season is broken, and in-season is expected.

    **Deliberately derived from the schedule rather than a date.** A hard-coded
    cutoff drifts a week every year and is wrong in both directions during camp. The
    schedule is the same file :func:`current_week` and :func:`current_season` read, so
    the pipeline and the app cannot disagree about what phase it is.

    **The failure direction is "in-season".** A frozen or truncated
    ``NFL_Schedules.csv`` reads as no games played and would report pre-season
    forever -- so callers that spend real money on being wrong should treat this as
    necessary, not sufficient. The board gate pairs it with the freeze, which is a
    positive declaration that a league's draft is over.

    Returns:
        bool: True when no game has been played.
    """
    sched = load_schedule()
    played = sched.filter(
        pl.col("away_score").cast(pl.Utf8, strict=False) != "NA")
    return played.height == 0


def date_week() -> pl.DataFrame:
    """Distinct gameday-to-week mapping.

    Returns:
        pl.DataFrame: Columns ``gameday`` and ``week``.
    """
    return load_schedule()[["gameday", "week"]].unique()


def __getattr__(name: str):
    """Resolve the legacy module-level constants lazily (:pep:`562`)."""
    if name == "NFL_SCHEDULE":
        return load_schedule()
    if name == "DATE_WEEK":
        return date_week()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
