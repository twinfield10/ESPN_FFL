"""One number per player-week that knows whether the game has been played.

``lineups.parquet`` has always carried both halves -- ``points`` from ESPN's box
score and ``TRUE_Points`` from the blend -- and no way to choose between them. This
resolves them into ``LIVE_Points``, which is what every weekly surface should read:

===============  ====================================================
``pre``          ``TRUE_Points``. Nothing has happened; the blend stands.
``in``           ``points`` + ``TRUE_Points`` x (1 - ``game_elapsed``).
``post``         ``points``. It is over; the actual is the answer.
``bye``          ``0.0``.
===============  ====================================================

Which is one expression rather than four branches, because scoring is linear in the
stat line::

    banked    = points where the game has started, else 0
    remaining = TRUE_Points x (1 - game_elapsed)
    LIVE_Points = banked + remaining

At ``elapsed = 0`` that is exactly ``TRUE_Points``, so **this is inert before the
first kickoff** -- the regression guard in ``tests/test_live_points.py`` asserts it.
At ``elapsed = 1`` the remainder is zero and only what was scored survives.

**A finished game uses ESPN's number, not our scoring of the line.** This is the one
place the repo's ``X_Points == score(X_ stat line)`` identity is deliberately not
extended, and it is not a shortcut. Measured 2026-09-10 on week 1: scoring the actual
stat line with the league's own rules reproduced ESPN's ``points`` to float noise for
all 13 players who had played in ``winfield_football`` -- kickers and both D/ST units
included -- but in ``john_pc_league`` it came out **5.00 points short** on Jaxon
Smith-Njigba, because that league scores five long-touchdown bonuses this pipeline
does not map. ESPN's ``points`` *is* the league's official score. An app that
disagrees with the box score about a finished game is wrong, whatever the
architecture prefers.

So the scored value is kept beside it as ``ACT_Points``, and the gap is a named
column: ``actual_unpriced = points - ACT_Points``. That is the same shape as the
existing ``espn_unpriced`` (``projection_utils`` step 6a) and it is the more useful
of the two. ``report_silent_zero_stats`` can only say "this rule is always zero" on
the projection side; this says "ESPN paid 5.00 points for a real week that we cannot
price", against realised data, naming the player. Closing it is stat-level work --
see ``docs/plans/34-stat-first-audit.md``.

**What is deliberately not here.** No ``LIVE_<stat>`` hybrid stat line. It would be
~45 columns that nothing reads, and an unconsumed column shaped like a source is
this repo's oldest failure mode. Add it when something wants it.
"""

from __future__ import annotations

import warnings
from typing import Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from Scripts.game_state import BYE, IN, POST, PRE

#: Column-name prefix for the resolved layer, matching the repo's ``<SRC>_<stat>``
#: convention. Deliberately **not** added to
#: :data:`Scripts.projection_utils.WEEKLY_PREFIXES`: that tuple is the register of
#: projection *sources*, and ``Scripts.lab.sources`` reads it to decide what a source
#: is. A resolved output listed there would be reported as a sixth opinion.
LIVE_PREFIX = "LIVE"

#: The state of the game a player's team is in. A string, not a flag, because
#: ``build_league_frame`` runs ``df.fillna(0)`` over the whole frame -- a numeric or
#: nullable state column would be silently overwritten with zeros.
STATE_COLUMN = "game_state"

#: Fraction of regulation played, 0.0 to 1.0.
ELAPSED_COLUMN = "game_elapsed"

#: Whether the player's game has started. The one flag consumers branch on: it is
#: what makes a lineup change impossible and a free agent unstartable.
LOCKED_COLUMN = "game_locked"

#: The resolved number. What the weekly surfaces read.
LIVE_POINTS = "LIVE_Points"

#: What is still to come: ``TRUE_Points`` x (1 - elapsed). The variance is computed
#: from this rather than from ``LIVE_Points`` -- points already banked have no
#: uncertainty left, and a player at 40 points would otherwise be given the spread of
#: a 40-point projection.
LIVE_REMAINING = "LIVE_remaining_points"

#: This league's rules applied to the actual stat line. The audit half.
ACT_POINTS = "ACT_Points"

#: ``points - ACT_Points`` on a started game: what ESPN paid that we cannot price.
ACTUAL_UNPRICED = "actual_unpriced"

#: Columns this module adds, in the order they are documented.
LIVE_COLUMNS = (STATE_COLUMN, ELAPSED_COLUMN, LOCKED_COLUMN,
                LIVE_POINTS, LIVE_REMAINING, ACT_POINTS, ACTUAL_UNPRICED)

#: A gap this large on a single player-week is worth naming.
#:
#: Not zero: floating-point scoring of a 45-rule table accumulates noise in the
#: 1e-13 range, and a warning that fires on every row is a warning nobody reads.
UNPRICED_TOLERANCE = 0.01


class UnpricedActualWarning(UserWarning):
    """ESPN scored points for a finished game that this pipeline cannot reproduce."""


class MissingGameStateWarning(UserWarning):
    """Some players' teams were not in the game-state board."""


def _warn(message: str, category) -> None:
    """Warn past ``fetch_utils``' module-scope ``filterwarnings("ignore")``."""
    with warnings.catch_warnings():
        warnings.simplefilter("always", category)
        warnings.warn(message, category, stacklevel=3)


def attach_game_state(df: pd.DataFrame, states, *, week_column: str = "week",
                      team_column: str = "pro_team") -> pd.DataFrame:
    """Join the game-state board onto a player-week frame.

    Joined on team and week, so it is correct for a frame holding several weeks --
    which ``lineups.parquet`` always is.

    A player whose team is not in the board is left at :data:`PRE`, which keeps his
    projection, and **counted in a warning**. That is the conservative direction and
    it is the honest one: an unsigned player carries ``proTeam`` of ``"None"``, and
    silently locking him would zero a projection nobody asked to zero.

    Args:
        df: Player-week frame carrying ``week`` and ``pro_team``.
        states: :func:`Scripts.game_state.states` output. Accepts a polars or
            pandas frame.
        week_column: Week column name.
        team_column: Team abbreviation column name, in ESPN's spelling.

    Returns:
        pd.DataFrame: ``df`` with :data:`STATE_COLUMN`, :data:`ELAPSED_COLUMN` and
        :data:`LOCKED_COLUMN`. A copy; the input is not modified.
    """
    board = states.to_pandas() if hasattr(states, "to_pandas") else states
    board = board[["team", "week", "state", "elapsed"]].rename(
        columns={"team": team_column, "week": week_column,
                 "state": STATE_COLUMN, "elapsed": ELAPSED_COLUMN})

    out = df.merge(board, on=[week_column, team_column], how="left")
    unknown = out[STATE_COLUMN].isna()
    if unknown.any():
        teams = sorted(set(out.loc[unknown, team_column].dropna().astype(str)))
        _warn(f"{int(unknown.sum())} player-week(s) have no game state, so their "
              f"projection stands unchanged. Teams: {', '.join(teams) or 'none'}. "
              f"A team here that plays real games means the board and `pro_team` "
              f"disagree on spelling.", MissingGameStateWarning)
    out[STATE_COLUMN] = out[STATE_COLUMN].fillna(PRE)
    out[ELAPSED_COLUMN] = out[ELAPSED_COLUMN].fillna(0.0).astype(float)
    out[LOCKED_COLUMN] = out[STATE_COLUMN] != PRE
    return out


def actual_points(df: pd.DataFrame, league, stats: Sequence[str]) -> pd.Series:
    """This league's rules applied to the actual stat line.

    Reuses ``proj_to_score`` rather than re-implementing the sum, because scoring is
    **per lineup slot** -- a sack is priced differently for a D/ST unit than for an
    individual defender -- and a second implementation would get that wrong exactly
    where it is hardest to notice. The prefixed columns are built on a throwaway view
    so the 45 duplicates never reach the stored frame.

    Args:
        df: Frame carrying the bare actual stat columns and ``primaryPosition``.
        league: ESPN ``League``, for the scoring table.
        stats: Bare stat column names to score.

    Returns:
        pd.Series: Points, indexed like ``df``. NaN where no stat was recorded at
        all, which is ``_apply_scoring``'s "this source said nothing" convention and
        is why a real zero survives as a zero.
    """
    from Scripts.projection_utils import proj_to_score

    present = [stat for stat in stats if stat in df.columns]
    view = pd.DataFrame({"primaryPosition": df["primaryPosition"]}, index=df.index)
    for stat in present:
        view[f"ACT_{stat}"] = df[stat]
    scored = proj_to_score(view, league, col_pfix_list=["ACT"])
    # `proj_to_score` splits D/ST from the rest and concatenates, so row order comes
    # back permuted. Reindex rather than assume.
    return scored["ACT_Points"].reindex(df.index)


def resolve(df: pd.DataFrame, league, stats: Sequence[str], *,
            blend_points: str = "TRUE_Points",
            actual_points_column: str = "points",
            label: str = "") -> pd.DataFrame:
    """Add the resolved live columns. Requires :func:`attach_game_state` first.

    Args:
        df: Player-week frame with the state columns, ``points`` and
            ``TRUE_Points``.
        league: ESPN ``League``, for the scoring table.
        stats: Bare actual stat column names, for :func:`actual_points`.
        blend_points: The projection column to decay.
        actual_points_column: ESPN's applied points.
        label: League name, for the unpriced warning.

    Returns:
        pd.DataFrame: ``df`` with :data:`LIVE_COLUMNS`. A copy.

    Raises:
        KeyError: When the state columns are absent -- resolving without them would
            silently return the projection for every row, which looks exactly like
            a working live feature and is not one.
    """
    missing = [c for c in (STATE_COLUMN, ELAPSED_COLUMN) if c not in df.columns]
    if missing:
        raise KeyError(
            f"resolve() needs {missing} on the frame; call attach_game_state() "
            f"first. Without them every row would resolve to its projection, "
            f"which is indistinguishable from a live feature that is not working."
        )

    out = df.copy()
    state = out[STATE_COLUMN]
    elapsed = out[ELAPSED_COLUMN].astype(float).clip(0.0, 1.0)
    over = state.isin([POST, BYE])

    banked = pd.to_numeric(out.get(actual_points_column), errors="coerce").fillna(0.0)
    banked = banked.where(out[LOCKED_COLUMN], 0.0)
    projection = pd.to_numeric(out.get(blend_points), errors="coerce")

    # NaN `TRUE_Points` means no source had an opinion, and that has to survive for a
    # game not yet played. It must *not* survive a finished one -- NaN * 0 is NaN in
    # pandas, so a player nobody projected would end a game with a null score instead
    # of what he actually did. Hence the branch on `over` rather than relying on
    # elapsed == 1 to zero the term.
    remaining = (projection * (1.0 - elapsed)).where(~over, 0.0)
    out[LIVE_REMAINING] = remaining.fillna(0.0).where(~projection.isna() | over, np.nan)
    out[LIVE_POINTS] = np.where(over, banked, banked + remaining)
    out[LIVE_POINTS] = pd.to_numeric(out[LIVE_POINTS], errors="coerce")
    # A bye is zero, not a blend that leaked. `app/lineup.pool_playable` documents
    # `TRUE_Points` of up to 2.12 on 24 of 81 bye players, imputed from the
    # ESPN/FantasyPros mean; game state settles it directly.
    out.loc[state == BYE, [LIVE_POINTS, LIVE_REMAINING]] = 0.0

    out[ACT_POINTS] = actual_points(out, league, stats)
    gap = pd.to_numeric(out[actual_points_column], errors="coerce") - out[ACT_POINTS]
    out[ACTUAL_UNPRICED] = gap.where(out[LOCKED_COLUMN] & (state != BYE))
    _report_unpriced(out, label)
    return out


def _report_unpriced(df: pd.DataFrame, label: str) -> None:
    """Warn when a finished game scored points this pipeline cannot reproduce."""
    gap = df[ACTUAL_UNPRICED]
    offenders = df[gap.abs() > UNPRICED_TOLERANCE]
    if offenders.empty:
        return
    worst = offenders.reindex(
        gap.abs().sort_values(ascending=False).index).dropna(subset=[ACTUAL_UNPRICED])
    named = ", ".join(
        f"{row['player_name']} {row[ACTUAL_UNPRICED]:+.2f}"
        for _, row in worst.head(3).iterrows())
    _warn(f"{label or 'this league'}: ESPN scored {offenders[ACTUAL_UNPRICED].sum():+.2f} "
          f"points across {len(offenders)} finished player-week(s) that this "
          f"pipeline cannot price ({named}). `LIVE_Points` uses ESPN's number and is "
          f"correct; the gap is a scoring-coverage defect and is measured in "
          f"`{ACTUAL_UNPRICED}`. See docs/plans/34-stat-first-audit.md.",
          UnpricedActualWarning)


def points_column(columns: Iterable[str]) -> str:
    """The best weekly points column a frame supports.

    Every consumer goes through this rather than naming :data:`LIVE_POINTS`
    directly, so a store written before this landed still renders instead of raising
    on a missing column.

    Args:
        columns: The frame's column names.

    Returns:
        str: :data:`LIVE_POINTS` when present, else ``"TRUE_Points"``.
    """
    return LIVE_POINTS if LIVE_POINTS in set(columns) else "TRUE_Points"


def state_counts(rows) -> dict:
    """How many player-weeks are in each state, for a log line or a caption.

    Accepts a polars frame, a pandas frame, or the list of dicts the optimiser works
    in -- the callers alternate between all three, and a helper that only took one of
    them would just move the conversion to the call sites.

    Args:
        rows: Anything carrying :data:`STATE_COLUMN`.

    Returns:
        dict: ``{state: count}``, ordered ``pre``, ``in``, ``post``, ``bye``. Empty
        when there is no game state, which reads as "nothing is known" rather than
        "everything is pre".
    """
    if hasattr(rows, "to_pandas"):
        rows = rows.to_pandas()
    if hasattr(rows, "columns"):
        if STATE_COLUMN not in rows.columns:
            return {}
        found = list(rows[STATE_COLUMN])
    else:
        found = [r.get(STATE_COLUMN) for r in rows]
    counts: dict = {}
    for state in found:
        if state is not None:
            counts[state] = counts.get(state, 0) + 1
    return {state: counts[state] for state in (PRE, IN, POST, BYE)
            if counts.get(state)}


def locked_ids(df, *, id_column: str = "player_id") -> List:
    """Player ids whose game has started, so a lineup change is no longer possible.

    Args:
        df: A frame carrying :data:`LOCKED_COLUMN`. Polars or pandas.
        id_column: Identity column.

    Returns:
        list: Ids. Empty when the frame carries no state, which reads as "nothing is
        locked" and preserves the pre-live behaviour.
    """
    if hasattr(df, "to_pandas"):
        df = df.to_pandas()
    if LOCKED_COLUMN not in df.columns or id_column not in df.columns:
        return []
    return df.loc[df[LOCKED_COLUMN].fillna(False).astype(bool), id_column].tolist()


# --- the light in-week refresh -------------------------------------------
#
# `--what live` exists because the full path is the wrong shape for a Sunday. It
# re-reads every elapsed week's box scores, re-scrapes nothing but re-merges every
# source file, re-blends 45 stats and re-scores nine leagues, and raises a
# `StaleProjectionSourceWarning` every run -- to change the handful of numbers a game
# in progress moves. This patches those numbers onto the frame already in the store
# and leaves every projection column exactly as the nightly built it.
#
# The saving is structural rather than a stopwatch. In week 1 it is 7.2s a league
# against 9.8s, because the full build's cost is mostly the weeks already played --
# `get_ply_stats_by_matchup` loops 1..current while `week_box_scores` reads one. What
# makes this safe to run 144 times a day is that it reads no projection source at
# all, so it cannot go stale and cannot rewrite what the blend voted with.

#: Columns the patch takes from the live box score, beside the stat line.
#:
#: The roster half is here on purpose. A manager who moves a bench player into a
#: flex before kickoff changes ``slotPosition``, and a waiver claim changes
#: ``team_owner`` -- so a refresh that patched only ``points`` would show live
#: scores against last night's lineup, which is a worse kind of wrong than showing
#: projections.
#: ``player_name`` is in the list because an *added* row is built from these columns
#: and nothing else -- without it a mid-week waiver claim landed in the frame as an
#: unnamed row, invisible in every table that renders a name.
PATCH_COLUMNS = ("player_name", "points", "projPoints", "slotPosition",
                 "primaryPosition", "eligiblePositions", "player_active_status",
                 "team_owner", "team_name", "team_division", "current_team_id",
                 "pro_team", "player_position")

#: Marks a row the live refresh added because the box score had a player the stored
#: frame did not -- someone added mid-week, after the nightly built the blend.
#:
#: Such a row carries actuals and **no projection**, so before his kickoff he
#: contributes nothing to a team total rather than something. That understates, which
#: is why the count is logged rather than absorbed: the nightly repairs it.
LIVE_ONLY_COLUMN = "live_only"


class LiveRefreshError(RuntimeError):
    """The stored frame cannot be patched, so nothing was written."""


def week_box_scores(league, week: int, score_cols: Sequence[str]) -> pd.DataFrame:
    """Every rostered player's live line for one week.

    ``get_ply_stats_by_matchup`` walks weeks 1..current and re-fetches the whole
    season; this is the one week that can still move. Both go through
    ``extract_player_stats``, so the row shape cannot drift between them.

    Args:
        league: A fetched ESPN ``League``.
        week: Scoring period.
        score_cols: The league's scored stat names.

    Returns:
        pd.DataFrame: One row per rostered player.
    """
    from espn_api.football import Team

    from Scripts.scrape_player_stats import extract_player_stats

    league.load_roster_week(int(week))
    frames = []
    for matchup in league.box_scores(int(week)):
        if not isinstance(matchup.home_team, Team) or not isinstance(
                matchup.away_team, Team):
            continue                              # a bye in an odd-sized league
        for team, lineup in ((matchup.home_team, matchup.home_lineup),
                             (matchup.away_team, matchup.away_lineup)):
            frames.append(extract_player_stats(
                team, lineup, int(week), score_cols=list(score_cols),
                curr_week=int(week)))
    if not frames:
        raise LiveRefreshError(
            f"ESPN returned no box scores for week {week}, so there is nothing to "
            f"patch. An empty patch written over a good frame would read as "
            f"'every game scored zero'."
        )
    return pd.concat(frames, ignore_index=True)


def patch(stored: pd.DataFrame, box: pd.DataFrame, week: int, *,
          stats: Sequence[str]) -> Tuple[pd.DataFrame, dict]:
    """Overlay a week's live box score onto the stored frame.

    Only that week's rows are touched, and within them only the actual and roster
    columns -- every ``ESPN_``/``FP_``/``TRUE_`` cell is left exactly as the nightly
    computed it.

    Args:
        stored: The frame from ``lineups.parquet``.
        box: :func:`week_box_scores` output.
        week: The week being patched.
        stats: Bare actual stat column names.

    Returns:
        tuple: ``(frame, counts)``. ``counts`` carries ``patched``, ``added`` and
        ``dropped``, which the caller logs.

    Raises:
        LiveRefreshError: When the stored frame holds no rows for ``week``.
            Refusing is the point: patching a week that is not there would append a
            second, projection-free copy of the week and halve every team total.
    """
    week = int(week)
    in_week = stored["week"].astype("Int64") == week
    if not bool(in_week.any()):
        held = sorted(stored["week"].dropna().unique().tolist())
        raise LiveRefreshError(
            f"lineups.parquet holds weeks {held}, not {week}, so there is nothing "
            f"to patch. Build it first: `python -m Scripts.refresh --what lineups`."
        )

    columns = [c for c in list(PATCH_COLUMNS) + list(stats) if c in box.columns]
    # Same dedup key and order as `build_league_frame`: a rostered row wins over the
    # free-agent sample, which really does return some rostered players.
    incoming = box.drop_duplicates(subset=["player_name"], keep="first")
    # And the same NaN treatment. `extract_fa_stats` emits only the scored columns,
    # so a free-agent row has no `passingAttempts` at all -- and the full build turns
    # that into 0 with its `fillna(0)`. Without this, patching would write a NaN over
    # a perfectly good zero.
    present_stats = [s for s in stats if s in incoming.columns]
    incoming[present_stats] = incoming[present_stats].fillna(0)
    incoming = incoming.drop_duplicates(subset=["player_id"]).set_index(
        "player_id")[columns]

    out = stored.copy()
    target = out.index[in_week]
    ids = out.loc[target, "player_id"]
    known = ids.isin(incoming.index)

    for column in columns:
        if column not in out.columns:
            out[column] = pd.NA
        values = ids[known].map(incoming[column])
        out.loc[target[known.to_numpy()], column] = values.to_numpy()

    added = incoming.index.difference(set(ids.tolist()))
    if len(added):
        fresh = incoming.loc[added].reset_index()
        for column in ("league_id", "year"):
            if column in out.columns:
                fresh[column] = out[column].dropna().iloc[0] if out[
                    column].notna().any() else pd.NA
        fresh["week"] = week
        fresh[LIVE_ONLY_COLUMN] = True
        out = pd.concat([out, fresh], ignore_index=True)
    # Written unconditionally, so the frame's schema does not depend on whether
    # anybody happened to make a waiver claim this week.
    if LIVE_ONLY_COLUMN not in out.columns:
        out[LIVE_ONLY_COLUMN] = False
    # `.notna()` rather than `.fillna(False).astype(bool)`: the concat above leaves an
    # object-dtype column of True and NaN, and filling that is a pandas
    # `FutureWarning` about silent downcasting.
    out[LIVE_ONLY_COLUMN] = (out[LIVE_ONLY_COLUMN].notna()
                             & out[LIVE_ONLY_COLUMN].astype("object").eq(True))

    counts = {
        "patched": int(known.sum()),
        "added": int(len(added)),
        # Rows the live pull did not cover. Two innocent causes and one that is not:
        # a player dropped mid-week is gone from every box score, and the free-agent
        # pool is a *sample* (top 20-30 per position), so a fringe free agent is
        # legitimately absent. Their projections stand and their owner may be stale
        # until the nightly. Counted rather than absorbed, because the third cause is
        # a join that has stopped working, and that looks identical from here.
        "uncovered": int((~known).sum()),
    }
    return out, counts


def refresh_live(name: str, season: int, *, league=None
                 ) -> Tuple[pd.DataFrame, dict]:
    """Patch one league's stored lineups with the current week's live scoring.

    Args:
        name: League display name or config key.
        season: Season year.
        league: An already-fetched ``League``, to avoid a second round-trip.

    Returns:
        tuple: ``(lineups, counts)`` -- the patched frame ready for
        ``store.write_league_store``, and a dict for the log line.

    Raises:
        LiveRefreshError: When there is no stored frame to patch, or ESPN returns
            no box scores.
        FileNotFoundError: When the league-season has no store at all.
    """
    from Scripts import game_state as gs
    from Scripts import store
    from Scripts.config_utils import resolve_league
    from Scripts.fetch_utils import fetch_league
    from Scripts.projection_utils import blended_stats
    from Scripts.scoring import get_scoring_table
    from Scripts.scrape_player_stats import VOLUME_STATS

    cfg = resolve_league(name)
    if league is None:
        league = fetch_league(league_id=cfg["ID"], year=season,
                              swid=cfg["SWID"], espn_s2=cfg["ESPN_S2"])

    stored = store.read_league_store(season, cfg["key"], "lineups")
    week = int(league.current_week)
    scored = get_scoring_table(league)["colName"].to_list()
    stats = [s for s in list(scored) + [v for v in VOLUME_STATS if v not in scored]
             if s in stored.columns]

    # Rostered lines and the free-agent pool, concatenated the way
    # `build_league_frame` does. The pool is not optional: the Free Agents tab has an
    # "Actual" column and `pool_playable` decides whether a player can still be
    # started, so a refresh that patched only rosters would leave that whole surface
    # reading last night's numbers -- and half-live is a worse failure than not-live,
    # because nothing about it looks broken. It costs ~4s of the ~6s per league.
    from Scripts.scrape_player_stats import build_fa_market

    box = week_box_scores(league, week, blended_stats(scored))
    pool = build_fa_market(league=league)
    pool["week"] = week
    incoming = pd.concat([box, pool], ignore_index=True)
    out, counts = patch(stored, incoming, week, stats=stats)

    board = gs.board(season, out["week"].dropna().unique().tolist(),
                     refresh_current=week)
    out = attach_game_state(out.drop(columns=[c for c in
                                              (STATE_COLUMN, ELAPSED_COLUMN,
                                               LOCKED_COLUMN)
                                              if c in out.columns]), board)
    out = resolve(out, league, stats, label=cfg["key"])

    # Ranks are derived from the numbers that just moved, so they move with them.
    out["PosRank"] = out.groupby(["week", "primaryPosition"])["points"].rank(
        ascending=False, method="dense")
    out[f"{LIVE_PREFIX}_PosRank"] = out.groupby(
        ["week", "primaryPosition"])[LIVE_POINTS].rank(
        ascending=False, method="dense")

    counts["week"] = week
    counts["states"] = state_counts(out)
    counts["live"] = gs.anything_live(board.filter(board["week"] == week))
    return out, counts
