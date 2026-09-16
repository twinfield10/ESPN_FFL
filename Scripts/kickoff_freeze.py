"""A projection stops being writable when its own game kicks off.

``clean_lineups`` rebuilds **every week from 1 to current** on every run -- the
nightly, the sidebar button, any manual ``--what lineups``. That is right for a game
that has not started and wrong for one that has: after kickoff a pre-game projection
can only be contaminated, never improved, and the number the blend voted with is the
only thing that makes a week gradeable afterwards.

**This was measured before it was fixed.** ``Data/.s3cache`` keeps every version of
every artifact the app has downloaded, keyed by ETag, so joining consecutive versions
of ``winfield_football/lineups.parquet`` on ``(week, player_id)`` says exactly what
moved and when. Across the 2026 week-1 Wednesday opener, **132 of 241 week-1 rows had
``TRUE_Points`` rewritten**; across the Sunday slate, 128 of the 187 players whose
game had already finished. Week 1 then stopped moving entirely, because
``get_ply_stats_by_matchup`` bounds its loop by ``currentMatchupPeriod`` and stops
fetching a week once the counter turns over. So the exposure window is precise: **from
the first kickoff of a week until the following Tuesday's rollover.**

**And be honest about the size of it.** Mean absolute movement was 0.078 points, max
1.68. It is small because no NFL game kicks off before 06:00, so the nightly's one
daily write has always landed pre-kickoff for that day's games, and because a book
retires a market once its game starts so there is usually nothing new to overwrite
with. None of that is enforced anywhere. Move the cron to the evening, add a second
run, or press the sidebar button at 16:00 on a Sunday, and every one of those numbers
grows with no log line and no symptom. This is a guard built while the cost of not
having it is still two decimal places.

**Why here and not in the blend.** ``clean_lineups`` is the pure projection path --
the equivalence harness snapshots it and ``populateGoogleSheet`` runs it -- and it has
no access to the store by design. The freeze is a statement about what gets
*published*, not about what the blend computes, so it belongs at the store boundary in
``Scripts.refresh``, where the previously written frame is one read away. The harness
and the Sheets path keep seeing the live blend, which is what they want.

What is frozen, and what is deliberately not::

    ESPN_* FP_* MEAN_* PINNY_* BOL_* ATH_* USG_* TRUE_*   frozen at kickoff
    projPoints, espn_unpriced                             frozen at kickoff
    -----------------------------------------------------------------------
    points, ACT_*, LIVE_*                                 never -- these are actuals
    passingYards, rushingYards, ... (bare stat names)     never -- these are actuals
    slotPosition, team_owner, roster columns              never -- these move in-week
    game_state, game_elapsed, game_locked                 never -- these are the clock

``projPoints`` is on the frozen list and is the one that needed saying out loud: it is
ESPN's own weekly projection, it moves continuously during a game, and
``Scripts.live.PATCH_COLUMNS`` overwrites it every ten minutes through a slate. It is
the single column where the live job's "touches no projection column" contract did not
hold, because every other projection column carries a source prefix and this one does
not.
"""

from __future__ import annotations

import datetime
import warnings
from typing import Dict, Iterable, Optional, Sequence, Tuple

import pandas as pd

from Scripts.game_state import IN, POST
from Scripts.live import STATE_COLUMN
from Scripts.projection_utils import WEEKLY_PREFIXES

#: States in which a projection is no longer writable. ``bye`` is deliberately absent:
#: a bye is an absence of a game, not a game that has started, and its projection is
#: zero either way -- so freezing it would add a rule that protects nothing and one
#: more thing to explain.
LOCKED_STATES = (IN, POST)

#: Column families that are a *projection* and therefore freeze. Taken from
#: :data:`Scripts.projection_utils.WEEKLY_PREFIXES` rather than restated, so a source
#: registered there is frozen the day it lands instead of the day somebody remembers
#: this file.
FROZEN_PREFIXES: Tuple[str, ...] = tuple(WEEKLY_PREFIXES)

#: Projection columns that carry no source prefix. ``projPoints`` is ESPN's own weekly
#: number; ``espn_unpriced`` is ``projPoints - ESPN_Points`` and would otherwise be a
#: frozen minuend against a live subtrahend.
FROZEN_COLUMNS: Tuple[str, ...] = ("projPoints", "espn_unpriced")

#: When this row's projection was first frozen. Kept beside the frozen values rather
#: than in ``meta.json`` because the question it answers is per player-week -- "is the
#: number I am reading a pre-game one, and as of when" -- and a store-level timestamp
#: cannot answer it for a Thursday game and a Monday game in the same week.
STAMP_COLUMN = "projection_frozen_at"

#: Join key. ``player_id`` rather than ``player_name`` for the reason
#: ``draft_view._franchise_key`` exists: ESPN is not consistent about spelling across
#: its own endpoints, and a name join would silently thaw whoever it failed to match.
KEYS = ("week", "player_id")


class KickoffFreezeWarning(UserWarning):
    """The freeze could not be applied, wholly or in part."""


def _warn(message: str) -> None:
    """Warn past ``fetch_utils``' module-scope ``filterwarnings("ignore")``.

    Same local override :mod:`Scripts.game_state` and :mod:`Scripts.live` use, and for
    the same reason: that call silences every warning in the process, so a plain
    ``warnings.warn`` here is invisible in any run that has imported the ESPN fetch
    layer -- which is every run that would want this.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("always", KickoffFreezeWarning)
        warnings.warn(message, KickoffFreezeWarning, stacklevel=3)


def frozen_columns(columns: Iterable[str]) -> list:
    """Which of ``columns`` are projections that freeze at kickoff.

    Args:
        columns: Column names from a lineups frame.

    Returns:
        list: The subset to carry forward, in the order given.
    """
    columns = list(columns)
    prefixed = tuple(f"{p}_" for p in FROZEN_PREFIXES)
    return [c for c in columns
            if c.startswith(prefixed) or c in FROZEN_COLUMNS]


def locked_rows(df: pd.DataFrame, *, state_column: str = STATE_COLUMN) -> pd.Series:
    """A boolean mask of rows whose game has started.

    Args:
        df: A lineups frame, after ``Scripts.live.attach_game_state``.
        state_column: Where the per-row game state lives.

    Returns:
        pd.Series: True where the projection is no longer writable. All False when
        the frame carries no game state at all -- an unknown clock must not freeze
        anything, because the failure it would cause (a season pinned to whatever was
        stored the day the scoreboard broke) is far worse than the one it prevents.
    """
    if state_column not in df.columns:
        return pd.Series(False, index=df.index)
    return df[state_column].isin(LOCKED_STATES).fillna(False)


def apply(fresh: pd.DataFrame, stored: Optional[pd.DataFrame], *,
          state_column: str = STATE_COLUMN,
          now: Optional[str] = None,
          columns: Optional[Sequence[str]] = None
          ) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Carry a started game's projection forward from the stored frame.

    Args:
        fresh: The frame ``build_league_frame`` just produced.
        stored: The frame currently in the store, or None on a first build. None
            freezes nothing -- there is no earlier opinion to keep, and inventing one
            is worse than publishing the fresh number once.
        state_column: Where the per-row game state lives on ``fresh``.
        now: ISO timestamp to stamp newly frozen rows with. Defaults to UTC now.
            An argument so a test does not have to patch the clock.
        columns: Explicit column list to freeze. None derives it from
            :func:`frozen_columns`, which is what every caller should want.

    Returns:
        tuple: ``(frame, counts)``. ``frame`` is a copy; ``fresh`` is not modified.
        ``counts`` carries:

        * ``locked`` -- rows whose game has started.
        * ``frozen`` -- rows actually carried forward from ``stored``.
        * ``unmatched`` -- locked rows with no stored counterpart, which keep their
          fresh values. Normally a player added mid-week, after the last build; the
          alternative is a row with no projection at all.
        * ``columns`` -- how many columns were carried.
        * ``newly_frozen`` -- rows this call stamped for the first time, i.e. games
          that kicked off since the previous build.
    """
    out = fresh.copy()
    locked = locked_rows(out, state_column=state_column)
    counts = {"locked": int(locked.sum()), "frozen": 0, "unmatched": 0,
              "columns": 0, "newly_frozen": 0}

    if STAMP_COLUMN not in out.columns:
        out[STAMP_COLUMN] = pd.NA

    if stored is None or stored.empty or not counts["locked"]:
        return out, counts

    missing_keys = [k for k in KEYS if k not in out.columns or k not in stored.columns]
    if missing_keys:
        _warn(f"cannot freeze started games: {missing_keys} missing from the fresh "
              f"or stored frame, so every projection was rebuilt. Games already "
              f"played will have moved.")
        return out, counts

    carry = columns if columns is not None else frozen_columns(out.columns)
    carry = [c for c in carry if c in stored.columns]
    if not carry:
        return out, counts
    counts["columns"] = len(carry)

    # A stored frame from before this guard shipped has no stamp column. Treat that as
    # "not yet stamped" rather than as an error, so the first run after the change
    # stamps every already-locked row instead of refusing.
    stamp_source = (stored[STAMP_COLUMN] if STAMP_COLUMN in stored.columns
                    else pd.Series(pd.NA, index=stored.index))

    prior = (stored.assign(**{STAMP_COLUMN: stamp_source})
                   .drop_duplicates(subset=list(KEYS), keep="first")
                   .set_index(list(KEYS)))

    index = pd.MultiIndex.from_frame(out.loc[locked, list(KEYS)])
    matched = index.isin(prior.index)
    counts["frozen"] = int(matched.sum())
    counts["unmatched"] = int((~matched).sum())
    if not counts["frozen"]:
        return out, counts

    target = out.index[locked][matched]
    source = index[matched]

    for column in carry:
        values = prior[column].reindex(source).to_numpy()
        # `.values` on the left, not `.loc[target, column] = series`: aligning on a
        # MultiIndex against a positional target is how a silent all-NaN assignment
        # happens, and an all-NaN projection column would look exactly like an absent
        # source to the blend that reads it next.
        out.loc[target, column] = values

    stamps = prior[STAMP_COLUMN].reindex(source)
    fresh_stamp = now or datetime.datetime.now(datetime.timezone.utc).isoformat(
        timespec="seconds")
    counts["newly_frozen"] = int(stamps.isna().sum())
    out.loc[target, STAMP_COLUMN] = stamps.fillna(fresh_stamp).to_numpy()
    return out, counts


def started_teams(season: int, weeks: Iterable[int]) -> set:
    """``(week, team)`` pairs whose game has kicked off.

    The same question :func:`locked_rows` answers off a frame, asked of the
    scoreboard directly -- for callers that have a source file rather than a lineups
    frame. :mod:`Scripts.scrape_FP` is the one that needs it.

    Args:
        season: Season year.
        weeks: Week numbers to resolve.

    Returns:
        set: ``(int, str)`` pairs, in ESPN's team spelling. A team on bye is absent,
        which is right -- there is no game to have started.

    Raises:
        Exception: Whatever the scoreboard read raises. Deliberately not swallowed:
            a caller freezing a *source file* has to decide for itself what an
            unknown clock means, and for a cumulative file the safe answer is
            "change nothing", not "rewrite everything".
    """
    from Scripts import game_state as gs

    weeks = sorted({int(w) for w in weeks})
    if not weeks:
        return set()
    board = gs.board(season, weeks, refresh_current=max(weeks))
    started = board.filter(board["state"].is_in(list(LOCKED_STATES)))
    return {(int(w), str(t))
            for w, t in zip(started["week"].to_list(), started["team"].to_list())}


def summary(counts: Dict[str, int]) -> str:
    """One line for the refresh log.

    Args:
        counts: :func:`apply`'s second return value.

    Returns:
        str: Human-readable, and deliberately says "nothing locked" rather than
        printing zeros -- most of the week there is no football on, and a line of
        zeros every night trains people to stop reading it.
    """
    if not counts["locked"]:
        return "no game has started in any stored week"

    # Every locked row unmatched is not "a busy waiver week" -- it is the join
    # failing, and the most likely cause is `week` or `player_id` differing in dtype
    # between the fresh frame and a store written by an older build. That reads as a
    # perfectly ordinary count unless it is called out, and the consequence is that
    # nothing is frozen at all while the log says the freeze ran.
    if counts["frozen"] == 0 and counts["unmatched"] == counts["locked"]:
        return (f"NOTHING HELD -- all {counts['locked']} locked rows failed to match "
                f"the stored frame on {list(KEYS)}. Every projection for a played "
                f"game was rebuilt. Check the dtypes of those columns on both frames.")

    parts = [f"{counts['frozen']} of {counts['locked']} locked rows held "
             f"across {counts['columns']} projection columns"]
    if counts["newly_frozen"]:
        parts.append(f"{counts['newly_frozen']} newly frozen")
    if counts["unmatched"]:
        parts.append(f"{counts['unmatched']} locked but not in the stored frame "
                     f"(added since the last build)")
    return "; ".join(parts)
