"""Make TOMCAT's own column describe a team that plays seventeen games.

:mod:`Scripts.usage.season` projects every player alone. Nothing in the model knows
that eleven of them share a huddle, so the team-level accounting identities it implies
come out wrong -- on the 2026 board ``sum receivingYards / sum passingYards`` runs
**0.658 to 1.704** across the 32 teams, against 1.000 flat for ESPN.

:func:`Scripts.season_projections.reconcile_team_totals` already repairs the blend, and
deliberately touches ``TRUE_`` only so that ``points_delta`` against ESPN stays a real
comparison. This module is the same repair applied to ``USG_`` **before** it reaches
the blend, which is phase 1 of :doc:`plan 31 <../../docs/plans/31-team-coherent-tomcat>`.

Two passes, in this order, because the second is wrong without the first
-----------------------------------------------------------------------

**The quarterback room, then the identity.** A team's passing sum has to be plausible
before it is worth averaging against its receiving sum: Atlanta projects 6,037 passing
yards, and taking the midpoint against its receivers would still leave 5,112 -- a total
three teams in NFL history have beaten. Normalising the room first brings it to 4,682,
and *then* the midpoint means something.

Where the room's error comes from is not what plan 31 first wrote down
----------------------------------------------------------------------

The plan attributed Cleveland's 1,938 projected passing yards to Shedeur Sanders
being priced at 10.2 expected games, with the other 6.8 games' volume simply leaving
the team. That is the mechanism in :func:`Scripts.usage.backtest.run_season`, whose
line is an expected value with the games term still in it. **It is not the mechanism
on the board.** :func:`Scripts.usage.project.to_full_slate` divides each player's own
``expected_games`` back out before the parquet is written, so every shipped ``USG_``
line already describes seventeen games. Cleveland's number is 16.7 attempts a game at
6.8 yards an attempt over a full slate -- the model shrinking an unproven rookie toward
a positional baseline, not availability deleting volume.

The double-count is real in the other direction and is the same arithmetic seen from
its other end. **Every projected quarterback is put on a full slate, and a team fields
one at a time.** The two teams with two quarterbacks priced are exactly the two teams
over the line:

==========  ====  ==========  =============
team          QB  room games  passing yards
==========  ====  ==========  =============
ATL            2        21.9          6,037
LV             2        20.1          5,652
30 others      1        12.5          3,900
==========  ====  ==========  =============

So the fix scales each quarterback's line by **the smaller of one and
``slate / (the room's expected games)``** -- Atlanta's two share a season instead of
playing one each, and everybody else is left exactly where the model put them.

Only one direction, and that was measured rather than chosen
------------------------------------------------------------

Plan 31 asked for both directions: normalise every room to the slate, lifting the
short ones as well as trimming the long. **Built and rejected.** Multiplying a line
that is already on a full slate by ``17 / 3.7`` does not say "he plays seventeen
games", it says he plays seventy-eight. Miami is the case that shows it: the only
Dolphin quarterback the model prices is Malik Willis at 3.7 expected games, because
the starter is not projected at all, and the uncapped rule takes Miami to **8,268
projected passing yards** against an all-time record of 5,477.

The four candidates, scored as mean absolute error against ESPN's 2026 team passing
totals -- a plausibility anchor rather than an outcome, and the tightest of the four
sources at 3,672-4,428 across the league:

=============================  =========  ==========  ==================
room rule                        raw MAE    mid MAE    team span, midpoint
=============================  =========  ==========  ==================
none (status quo)                    431        309     2,620 - 5,112
``eg_i / room`` (shares to 1)        384        286     2,620 - 4,606
``17 / room`` (plan 31 as writ)    1,442        722     3,268 - **8,268**
``min(1, 17 / room)`` (this)     **361**    **274**     2,620 - 4,606
=============================  =========  ==========  ==================

The share rule and the cap differ only on the two teams with two quarterbacks priced,
which is too thin to separate them on error. The cap is preferred on what it cannot
do: it is bounded above by one, so it can only ever remove a double-count, and it is
an exact no-op on the thirty teams that have none.

**The short rooms are left short on purpose.** Cleveland stays at 1,938 before the
identity pass, and that shortfall is real -- but the missing volume belongs to a
quarterback the model does not project, and inventing it on Shedeur Sanders' row
would be wrong at the player level to buy tidiness at the team level. Moving it to
whoever actually replaces him is phase 3 of the plan, and it needs a replacement row
to move it to.

Cleveland is also the case plan 31 read as an availability failure and is not one:
16.7 attempts a game at 6.8 yards an attempt over a full slate is the model shrinking
an unproven rookie toward a positional baseline. No games term is involved.

What this does not touch
------------------------

``usg_expected_games`` **stays the player's own availability estimate.** Normalising it
to the slate would read on the board's "Exp G" column as a claim that Tua Tagovailoa
plays seventeen games, when the model's estimate is 3.7 and the injury work in plan 27
depends on that number meaning what it says. The room's arithmetic travels beside it
instead, as :data:`TEAM_GAMES_COLUMN` and :data:`ROOM_SCALE_COLUMN`.

``passingCompletions`` / ``receivingReceptions`` is skipped for a duller reason: the
model projects no completions column, so there is nothing to reconcile it against.
:func:`reconcile_identities` skips any pair it cannot see both ends of.
"""

from __future__ import annotations

from typing import Iterable, Sequence, Tuple

import polars as pl

#: Values of the team column that are not a team.
#:
#: ESPN's board buckets every free agent under the literal string ``"None"`` -- 522 of
#: 2,504 rows on the 2026 GOP board. Grouping them as though they were a franchise
#: would scale a dozen unsigned quarterbacks into a single seventeen-game season, which
#: is not a reconciliation of anything.
NON_TEAMS: Tuple[str, ...] = ("None", "FA", "", "0")

#: Diagnostic: the team's projected quarterback-games *before* normalisation.
TEAM_GAMES_COLUMN = "usg_team_qb_games"

#: Diagnostic: the factor :func:`normalise_qb_room` applied to that team's passers.
#:
#: 1.0 on any team whose quarterback room is not over-subscribed, which is thirty of
#: the thirty-two. A value below one is the size of the double-count removed.
ROOM_SCALE_COLUMN = "usg_qb_room_scale"

#: Stat pairs that are the same event counted from each end.
#:
#: Mirrors :data:`Scripts.season_projections.TEAM_IDENTITIES`. Kept here rather than
#: imported because that module imports this one's caller, and a module-level import
#: back would close the cycle.
IDENTITIES: Tuple[Tuple[str, str], ...] = (
    ("passingYards", "receivingYards"),
    ("passingTouchdowns", "receivingTouchdowns"),
    ("passingCompletions", "receivingReceptions"),
)


def _real_team(team_column: str) -> pl.Expr:
    """Rows whose team column names an actual franchise."""
    return (pl.col(team_column).is_not_null()
            & pl.col(team_column).is_in(list(NON_TEAMS)).not_())


def stat_columns(frame: pl.DataFrame, prefix: str = "USG_") -> list:
    """Every projected stat line under ``prefix``, intervals included.

    Args:
        frame: Frame to scan.
        prefix: Column prefix, e.g. ``"USG_"``.

    Returns:
        list: Column names to scale. Excludes the ``_is_imputed`` provenance flags,
        which are booleans and carry no volume.
    """
    return [c for c in frame.columns
            if c.startswith(prefix) and not c.endswith("_is_imputed")
            and frame.schema[c].is_numeric()]


def normalise_qb_room(frame: pl.DataFrame,
                      team_column: str = "pro_team",
                      position_column: str = "primaryPosition",
                      games_column: str = "usg_expected_games",
                      prefix: str = "USG_",
                      slate: float = 17.0,
                      anchor: str = "passingYards") -> pl.DataFrame:
    """Trim each team's quarterbacks so the room plays one season, not one each.

    See the module docstring for why this runs before :func:`reconcile_identities`,
    and why the factor is capped at one rather than applied in both directions.

    **A no-op on any team whose room does not exceed the slate**, which on the 2026
    board is thirty of the thirty-two. Only Atlanta and Las Vegas -- the two teams
    with two quarterbacks priced -- are touched at all.

    Only quarterbacks the model actually spoke about count toward the room. An
    abstention has no line to scale and no games to contribute -- counting its
    ``expected_games`` would shrink the passers who *were* projected in order to make
    room for a projection that does not exist.

    Args:
        frame: Projection frame carrying the team, position, games and stat columns.
        team_column: Column naming the franchise.
        position_column: Column naming the position.
        games_column: Per-player expected games. Read, never written.
        prefix: Stat-line prefix to scale.
        slate: Games a team's season offers.
        anchor: Stat whose presence decides that the model spoke for this passer.

    Returns:
        pl.DataFrame: ``frame`` with quarterback stat lines scaled, and
        :data:`TEAM_GAMES_COLUMN` / :data:`ROOM_SCALE_COLUMN` attached.
    """
    needed = {team_column, position_column, games_column}
    if not needed.issubset(frame.columns):
        return frame

    anchor_column = f"{prefix}{anchor}"
    counts = (pl.col(position_column) == "QB") & _real_team(team_column)
    if anchor_column in frame.columns:
        counts = counts & pl.col(anchor_column).is_not_null()

    room = (pl.when(counts).then(pl.col(games_column).cast(pl.Float64))
              .otherwise(None)
              .sum().over(team_column))

    frame = frame.with_columns(room.alias(TEAM_GAMES_COLUMN))
    # Capped at one. See the module docstring: the uncapped form multiplies a line
    # that is already on a full slate, so a room short of the slate is not lifted to
    # it but pushed past it -- Miami to 8,268 passing yards on a 3.7-game backup.
    frame = frame.with_columns(
        pl.when(pl.col(TEAM_GAMES_COLUMN) > 0)
          .then(pl.min_horizontal(
              pl.lit(1.0), pl.lit(float(slate)) / pl.col(TEAM_GAMES_COLUMN)))
          .otherwise(None)
          .alias(ROOM_SCALE_COLUMN))

    # Applied to the passers only. A team's receivers are reconciled by the identity
    # pass that follows; scaling them here as well would double the correction.
    scale = (pl.when(counts & pl.col(ROOM_SCALE_COLUMN).is_not_null())
               .then(pl.col(ROOM_SCALE_COLUMN)).otherwise(1.0))

    return frame.with_columns(
        [(pl.col(c).cast(pl.Float64) * scale).alias(c)
         for c in stat_columns(frame, prefix)])


def reconcile_identities(frame: pl.DataFrame,
                         team_column: str = "pro_team",
                         prefix: str = "USG_",
                         identities: Iterable[Tuple[str, str]] = IDENTITIES,
                         ) -> pl.DataFrame:
    """Force each team's passing and receiving lines to describe the same season.

    The polars twin of :func:`Scripts.season_projections.reconcile_team_totals`, and
    the same midpoint rule for the same measured reason: against 2025 realised team
    passing yards the receiver sum is the better single estimator (MAE 263 to 321) but
    wins the head-to-head only 16-14, which is too thin to scale quarterbacks alone by
    up to 20%. The midpoint takes the best MAE of the three and asserts nothing.

    **Both** sides must be non-empty. A team with nothing on one side has no identity
    to enforce, and its midpoint would be half the side that does exist -- halving that
    side while leaving the empty one at zero is a deletion, not a reconciliation.

    Nulls are abstentions and stay null: they contribute nothing to a team's sum, and
    multiplying them by the ratio leaves them absent, which is what the blend has to
    see.

    Args:
        frame: Projection frame carrying the team and stat columns.
        team_column: Column naming the franchise.
        prefix: Stat-line prefix to reconcile.
        identities: ``(passing, receiving)`` pairs. Pairs missing either end are
            skipped -- the model projects no completions column.

    Returns:
        pl.DataFrame: ``frame`` with those columns scaled in place.
    """
    if team_column not in frame.columns:
        return frame

    for passing, receiving in identities:
        left, right = f"{prefix}{passing}", f"{prefix}{receiving}"
        if left not in frame.columns or right not in frame.columns:
            continue

        real = _real_team(team_column)
        totals = {c: (pl.when(real).then(pl.col(c).cast(pl.Float64))
                        .otherwise(None).sum().over(team_column))
                  for c in (left, right)}
        target = (totals[left] + totals[right]) / 2.0
        usable = real & (totals[left] > 0) & (totals[right] > 0)

        frame = frame.with_columns(
            [(pl.col(c).cast(pl.Float64)
              * pl.when(usable).then(target / totals[c]).otherwise(1.0)).alias(c)
             for c in (left, right)])

    return frame


def make_coherent(frame: pl.DataFrame,
                  team_column: str = "pro_team",
                  position_column: str = "primaryPosition",
                  games_column: str = "usg_expected_games",
                  prefix: str = "USG_",
                  slate: float = 17.0) -> pl.DataFrame:
    """Both passes, in the order that makes the second one meaningful.

    Args:
        frame: Projection frame.
        team_column: Column naming the franchise.
        position_column: Column naming the position.
        games_column: Per-player expected games.
        prefix: Stat-line prefix.
        slate: Games a team's season offers.

    Returns:
        pl.DataFrame: A frame whose team totals a real season could produce.
    """
    frame = normalise_qb_room(frame, team_column, position_column, games_column,
                              prefix, slate)
    return reconcile_identities(frame, team_column, prefix)


def identity_report(frame: pl.DataFrame,
                    team_column: str = "pro_team",
                    prefix: str = "USG_",
                    passing: str = "passingYards",
                    receiving: str = "receivingYards") -> pl.DataFrame:
    """Per-team identity ratio and quarterback-games, for the gates to read.

    Args:
        frame: Projection frame.
        team_column: Column naming the franchise.
        prefix: Stat-line prefix.
        passing: Passing side of the identity.
        receiving: Receiving side.

    Returns:
        pl.DataFrame: One row per real team -- ``passing``, ``receiving``, ``ratio``
        and, where the column survives, ``qb_games``.
    """
    left, right = f"{prefix}{passing}", f"{prefix}{receiving}"
    aggregations: Sequence[pl.Expr] = [
        pl.col(left).cast(pl.Float64).sum().alias("passing"),
        pl.col(right).cast(pl.Float64).sum().alias("receiving"),
    ]
    if TEAM_GAMES_COLUMN in frame.columns:
        aggregations = list(aggregations) + [
            pl.col(TEAM_GAMES_COLUMN).max().alias("qb_games")]

    out = (frame.filter(_real_team(team_column))
                .group_by(team_column).agg(aggregations))
    return (out.with_columns(
                pl.when(pl.col("passing") > 0)
                  .then(pl.col("receiving") / pl.col("passing"))
                  .otherwise(None).alias("ratio"))
               .sort("ratio", nulls_last=True))
