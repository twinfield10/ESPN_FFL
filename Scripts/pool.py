"""The waiver wire, kept — because the nightly destroys it.

**Nothing in this repo has ever recorded what the free-agent pool looked like in a
past week, in any season.** ``build_fa_market`` calls ``league.free_agents()``,
which ESPN only serves *as of now*, and ``extract_fa_stats`` stamps every row with
``league.current_week``. ``equivalence.build_league_frame`` concatenates that single
snapshot onto a full roster history and ``clean_lineups`` rebuilds the frame, so the
06:00 refresh is destructive by design: the 2025 stores carry free-agent rows on
**week 17 only**, and the 2026 stores on week 1 only.

The consequence is that no claim about a waiver suggestion is falsifiable. There is
no way to ask "who was actually available in week 4, and would taking him have
helped?" once week 4 has passed, and no backfill can recover it -- the same sentence
``scrape_espn_injuries`` had to write about injury severity, for the same reason.

So this artifact accumulates. One row per pool player-week, narrow rather than the
625 columns ``lineups.parquet`` carries, appended each time the nightly runs and
**re-written idempotently within a week** -- the nightly runs once a day and
``--what live`` every ten minutes, so "append" has to mean "replace this week's
rows", not "add more of them".

Paired with the realised points already in ``lineups.parquet``, this is what makes
a rest-of-season waiver engine measurable rather than merely plausible.

See ``docs/plans/49-rest-of-season-waivers.md``.
"""

from typing import Iterable, Optional, Sequence

import pandas as pd

from Scripts.scrape_player_stats import FREE_AGENT_OWNER

#: Identity and status columns kept on every snapshot row.
#:
#: ``eligiblePositions`` earns its place despite being a list column: slot-by-slot
#: comparison is the whole basis of :func:`app.lineup.upgrades`, and a snapshot that
#: cannot reproduce it could not replay a suggestion.
BASE_COLUMNS: Sequence[str] = (
    "league_id", "year", "week", "player_id", "player_name", "player_position",
    "primaryPosition", "eligiblePositions", "pro_team", "player_active_status",
    "game_state", "game_elapsed", "game_locked", "points",
)

#: Projection columns kept, where the frame has them.
#:
#: The per-source points and the blend, not the ~45 stats behind each -- a snapshot
#: is for replaying a *decision*, and the decision was taken on these. ``LIVE_`` is
#: kept because it is the column the app actually ranks on once a game has started.
POINTS_COLUMNS: Sequence[str] = (
    "ESPN_Points", "FP_Points", "PINNY_Points", "BOL_Points", "ATH_Points",
    "MEAN_Points", "TRUE_Points", "LIVE_Points", "PosRank", "TRUE_PosRank",
    "sources_real",
)

#: Columns carried from the season board when one is supplied.
BOARD_COLUMNS: Sequence[str] = ("percent_owned", "injury_status")


def snapshot(lineups: pd.DataFrame, week: int,
             board: Optional[pd.DataFrame] = None,
             *, captured_at: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """This week's free-agent pool, narrowed to what a replay needs.

    Args:
        lineups: A ``clean_lineups`` frame for the league-season.
        week: The scoring period the pool belongs to. Free-agent rows carry
            ``league.current_week`` already, but it is passed explicitly rather
            than inferred so a caller cannot silently snapshot the wrong week.
        board: Optional ``board.parquet``, for ``percent_owned`` and
            ``injury_status`` -- neither is on the weekly artifact. Joined on
            ``player_id``, which is 1:1 (measured: no 2026 board has a duplicate).
        captured_at: Override for the capture stamp, for tests.

    Returns:
        pd.DataFrame: One row per available player. Empty when the frame carries
        no pool for that week, which the caller must treat as a failure rather
        than as "nobody was available" -- see :func:`accumulate`.
    """
    if lineups is None or lineups.empty or "team_owner" not in lineups.columns:
        return pd.DataFrame()

    pool = lineups[(lineups["team_owner"] == FREE_AGENT_OWNER)
                   & (lineups["week"] == week)]
    if pool.empty:
        return pd.DataFrame()

    keep = [c for c in (*BASE_COLUMNS, *POINTS_COLUMNS) if c in pool.columns]
    out = pool[keep].copy()

    if board is not None and not board.empty and "player_id" in board.columns:
        carry = [c for c in BOARD_COLUMNS if c in board.columns]
        if carry:
            out = out.merge(
                board[["player_id", *carry]].drop_duplicates(subset=["player_id"]),
                on="player_id", how="left")

    out["captured_at"] = (pd.Timestamp.now(tz="UTC") if captured_at is None
                          else captured_at)
    return out.reset_index(drop=True)


def accumulate(existing: Optional[pd.DataFrame],
               fresh: pd.DataFrame) -> pd.DataFrame:
    """Merge a new week's snapshot into the stored history.

    **Replace-this-week rather than append**, because the nightly runs daily and
    ``--what live`` every ten minutes; a true append would hold ninety copies of
    week 4 by Friday. The most recent capture of a week wins, which is also what
    you want on a Tuesday when the wire has moved since Monday.

    Args:
        existing: The stored artifact, or None on the first run.
        fresh: :func:`snapshot` output.

    Returns:
        pd.DataFrame: History with ``fresh``'s weeks replaced.

    Raises:
        ValueError: When ``fresh`` is empty. In an accumulating store "nothing was
            available" and "the pull broke" produce the same file, so an empty
            write is refused rather than allowed to silently truncate a season --
            the rule ``Scripts/books/store.py`` already follows.
    """
    if fresh is None or fresh.empty:
        raise ValueError(
            "Refusing to write an empty pool snapshot. An accumulating artifact "
            "cannot tell 'nobody was available' from 'the fetch failed', and the "
            "history it would overwrite is not rebuildable."
        )
    if existing is None or existing.empty:
        return fresh.reset_index(drop=True)

    weeks = set(fresh["week"].unique())
    kept = existing[~existing["week"].isin(weeks)]
    return pd.concat([kept, fresh], ignore_index=True).reset_index(drop=True)


def weeks_present(frame: Optional[pd.DataFrame]) -> list:
    """Sorted weeks a pool history covers.

    Args:
        frame: The stored artifact, or None.

    Returns:
        list: Week numbers, ascending.
    """
    if frame is None or frame.empty or "week" not in frame.columns:
        return []
    return sorted(int(w) for w in frame["week"].unique())
