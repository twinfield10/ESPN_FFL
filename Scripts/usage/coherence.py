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

from typing import Dict, Iterable, Sequence, Tuple

import polars as pl

from Scripts.usage import season as sn

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

#: The identity the volume correction is anchored on.
#:
#: Yards rather than touchdowns, because it is the pair with real coverage on both ends
#: and the one G-T0 is written against. The rest are closed afterwards, on the residual.
ANCHOR: Tuple[str, str] = ("passingYards", "receivingYards")


def volume_families(prefix: str = "USG_") -> Dict[str, Tuple[str, ...]]:
    """Stat columns grouped by the volume term they are all built from.

    Read off :data:`Scripts.usage.season.STAT_TERMS` rather than written out here, so a
    stat added to the model joins its family without anyone remembering to come back.

    Args:
        prefix: Stat-line prefix.

    Returns:
        dict: Volume term -> the prefixed stat columns derived from it. ``targets_pg``
        carries receiving yards, receptions and receiving touchdowns.
    """
    families: Dict[str, list] = {}
    for stat, (volume, _rate) in sn.STAT_TERMS.items():
        families.setdefault(volume, []).append(f"{prefix}{stat}")
    return {volume: tuple(columns) for volume, columns in families.items()}


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


#: Realised quarterback **starts** by pre-season depth rank and cohort.
#:
#: The allocation rule phase 2 needs, and it had to be measured rather than assumed.
#: 644 quarterback player-seasons, 2018-2025, leak-free in the same sense as plan 33's
#: calibration: the label is a start in a season already over, read against the depth
#: chart as it stood *before* that season. A start is the passer with the most attempts
#: in a team-week, which makes a team's starts sum to the slate by construction --
#: unlike appearances, where a starter who leaves injured and his replacement both
#: count and the room sums to a median of 20.
#:
#: =========  ======  ======  ======
#: cohort      rank 1  rank 2  rank 3
#: =========  ======  ======  ======
#: settled     13.88    2.73    2.00
#: mover       10.11    2.21    1.02
#: rookie       9.06    1.58    0.47
#: =========  ======  ======  ======
#:
#: **The cohort split is the whole point, and it is plan 33's finding in the currency
#: phase 2 spends.** That plan measured how often a listed starter really is one --
#: 59% settled, 45% a mover, 36% a rookie. Here the same three cohorts separate a
#: listed QB1's season by nearly five starts. A rule that ignored cohort would give a
#: rookie first-stringer the same 13.88 as an entrenched veteran.
#:
#: Ranks are clipped to three by :data:`Scripts.usage.context.MAX_DEPTH_RANK`, so
#: there is no fourth row to hold. A quarterback the chart does not list at all falls
#: back to :data:`UNLISTED_START_PRIOR`.
QB_START_PRIORS: Dict[Tuple[str, int], float] = {
    ("settled", 1): 13.88, ("settled", 2): 2.73, ("settled", 3): 2.00,
    ("mover", 1): 10.11,   ("mover", 2): 2.21,   ("mover", 3): 1.02,
    ("rookie", 1): 9.06,   ("rookie", 2): 1.58,  ("rookie", 3): 0.47,
}

#: Prior for a projected passer the pre-season chart does not rank.
#:
#: Between a rookie third-stringer's 0.47 and a listed rank-3's 1.02. He is on the
#: roster and the model spoke for him, so he is not zero; the chart declining to rank
#: him is evidence, so he is not a backup either.
UNLISTED_START_PRIOR: float = 0.75

#: Diagnostic: the starts phase 2 allocated this passer out of the team's slate.
ALLOCATED_STARTS_COLUMN = "usg_qb_allocated_starts"


def allocate_qb_starts(frame: pl.DataFrame,
                       team_column: str = "pro_team",
                       position_column: str = "primaryPosition",
                       cohort_column: str = "usg_role_cohort",
                       rank_column: str = "depth_rank",
                       prefix: str = "USG_",
                       slate: float = 17.0,
                       anchor: str = "passingYards") -> pl.DataFrame:
    """Split a team's slate of starts across its quarterbacks, by role.

    Phase 2 of :doc:`plan 31 <../../docs/plans/31-team-coherent-tomcat>`, and the
    replacement for :func:`normalise_qb_room` rather than an addition to it -- running
    both would scale the room twice.

    **Why the room needs more than a scale.** :func:`normalise_qb_room` multiplies
    every passer on a team by one number, ``min(1, slate / room)``. That closes the
    team total and cannot touch the order inside the room, which is measurably why
    G-T2 missed: phase 1 moved league-wide quarterback ranks by a Spearman of 0.956
    against itself, so ordering had nowhere to go. Allocating gives each passer his
    own factor, and the starter and his backup move in opposite directions.

    **Why the model needs it at all.** ``expected_games`` predicts *appearances*, and
    it predicts them one player at a time with no knowledge that a team fields one
    quarterback. Summed over a room it reaches a median 25.9 games against a
    seventeen-game season, and the excess sits almost entirely on backups -- a listed
    QB2 is given 6.58 games against a realised 2.24. Scaling that room proportionally,
    which is what "constrain the room to the slate" literally asks for, would take the
    *starter* from 12.46 games to 9.6 against a realised 14.03. The shares are wrong,
    not just the total, so the fix has to reallocate rather than shrink.

    The published line is on a full slate -- :func:`Scripts.usage.project.to_full_slate`
    divides ``expected_games`` back out -- so a passer's expected contribution is that
    line times his share of the slate, and a room's shares sum to one. A team with a
    single projected passer is therefore untouched, which is what stops this
    re-creating the Miami blow-up the uncapped phase 1 rule produced: his share is one,
    not ``slate / expected_games``.

    Args:
        frame: Projection frame carrying team, position, cohort, rank and stat columns.
        team_column: Column naming the franchise.
        position_column: Column naming the position.
        cohort_column: Column naming the role cohort. See :data:`QB_START_PRIORS`.
        rank_column: Pre-season depth-chart rank, clipped to three.
        prefix: Stat-line prefix to scale.
        slate: Starts a team's season offers.
        anchor: Stat whose presence decides that the model spoke for this passer.

    Returns:
        pl.DataFrame: ``frame`` with quarterback stat lines allocated, and
        :data:`ALLOCATED_STARTS_COLUMN` attached. Returned unchanged when the frame
        lacks the columns the allocation needs, so a caller without a depth chart
        keeps phase 1 behaviour rather than failing.
    """
    needed = {team_column, position_column, cohort_column, rank_column}
    if not needed.issubset(frame.columns):
        return frame

    anchor_column = f"{prefix}{anchor}"
    counts = (pl.col(position_column) == "QB") & _real_team(team_column)
    if anchor_column in frame.columns:
        counts = counts & pl.col(anchor_column).is_not_null()

    # The prior, from the measured table. An unranked or unknown-cohort passer falls
    # back rather than dropping out: a room that silently loses a member reallocates
    # his starts to everyone else, which is the double-count coming back in.
    prior = pl.lit(UNLISTED_START_PRIOR)
    for (cohort, rank), value in QB_START_PRIORS.items():
        prior = (pl.when((pl.col(cohort_column) == cohort)
                         & (pl.col(rank_column).cast(pl.Int64, strict=False) == rank))
                   .then(pl.lit(float(value))).otherwise(prior))

    weight = pl.when(counts).then(prior).otherwise(None)
    room_weight = weight.sum().over(team_column)

    frame = frame.with_columns(
        pl.when(counts & (room_weight > 0))
          .then(pl.lit(float(slate)) * weight / room_weight)
          .otherwise(None)
          .alias(ALLOCATED_STARTS_COLUMN))

    # Share of the slate, not a ratio against `expected_games`: the line is already on
    # a full slate, so multiplying by `allocated / expected_games` would divide the
    # availability term back in on top of a line that no longer carries it.
    scale = (pl.when(counts & pl.col(ALLOCATED_STARTS_COLUMN).is_not_null())
               .then(pl.col(ALLOCATED_STARTS_COLUMN) / pl.lit(float(slate)))
               .otherwise(1.0))

    return frame.with_columns(
        [(pl.col(c).cast(pl.Float64) * scale).alias(c)
         for c in stat_columns(frame, prefix)])


def reconcile_identities(frame: pl.DataFrame,
                         team_column: str = "pro_team",
                         prefix: str = "USG_",
                         identities: Iterable[Tuple[str, str]] = IDENTITIES,
                         ) -> pl.DataFrame:
    """Close the team accounting identities without moving anyone's rates.

    Phase 3 of :doc:`plan 31 <../../docs/plans/31-team-coherent-tomcat>`.

    **What this used to do, and why it was wrong.** Each identity pair was scaled on its
    own -- passing yards against receiving yards, passing touchdowns against receiving
    touchdowns. But every published stat is ``volume x rate`` off a *shared* volume term:
    receiving yards, receptions and receiving touchdowns are all ``targets_pg`` times
    something. Scaling one member of a family and not another rewrites the rate between
    them. It moved the implied yards per reception of **all 665** projected pass-catchers
    by a median of 18.4% and up to 33.6%, taking the league median from a realistic 10.81
    to 8.98.

    ``receivingReceptions`` was the worst of it. Its counterpart ``passingCompletions``
    is not a stat this model projects -- :data:`IDENTITIES` names the pair, the loop
    skipped it, and receptions were never scaled at all while the yards beside them were.
    Team receptions ran 365-640 against a real 300-450, and **every league here scores a
    reception at ten times a receiving yard**, so that was a systematic inflation of every
    pass-catcher's price rather than a cosmetic gap.

    **What it does instead.** A team-level disagreement about yards is a disagreement
    about *volume*, so the correction belongs to the volume term and everything built on
    it moves together: one factor per family from the :data:`ANCHOR` pair, applied across
    the family, then the remaining identities closed on what is left. Rushing has no
    counterpart to reconcile against and is left alone.

    Both identities still close exactly. Yards per reception now moves by **0.00%** and
    touchdowns per reception by 2.19% against 16.77% -- and that residual is the model
    disagreeing with itself about touchdown rates, surfacing rather than being papered
    over by arithmetic.

    Args:
        frame: Projection frame carrying the team and stat columns.
        team_column: Column naming the franchise.
        prefix: Stat-line prefix to reconcile.
        identities: ``(passing, receiving)`` pairs. Pairs missing either end are
            skipped -- the model projects no completions column.

    Returns:
        pl.DataFrame: ``frame`` with the identities closed and the rates intact.
    """
    if team_column not in frame.columns:
        return frame

    real = _real_team(team_column)

    def total(column: str) -> pl.Expr:
        return (pl.when(real).then(pl.col(column).cast(pl.Float64))
                  .otherwise(None).sum().over(team_column))

    def factors(left: str, right: str):
        """Midpoint factors for a pair, or None where the pair cannot be scored."""
        if left not in frame.columns or right not in frame.columns:
            return None
        target = (total(left) + total(right)) / 2.0
        usable = real & (total(left) > 0) & (total(right) > 0)
        return tuple(pl.when(usable).then(target / total(column)).otherwise(1.0)
                     for column in (left, right))

    # Materialised because it is read twice, and because `identities` is what decides
    # whether the anchor runs at all -- a caller asking for one pair gets that pair
    # reconciled and nothing else.
    wanted = tuple(identities)
    families = volume_families(prefix)
    anchor = (factors(f"{prefix}{ANCHOR[0]}", f"{prefix}{ANCHOR[1]}")
              if ANCHOR in wanted else None)

    if anchor is not None:
        # The volume correction. A stat's family decides its factor, so receptions
        # travel with the receiving yards they were counted from.
        by_column = {}
        for stat, factor in zip(ANCHOR, anchor):
            for column in families.get(sn.STAT_TERMS[stat][0], ()):
                by_column[column] = factor
        frame = frame.with_columns(
            [(pl.col(column).cast(pl.Float64) * factor).alias(column)
             for column, factor in by_column.items() if column in frame.columns])

    # Whatever the volume correction did not close, closed on the residual -- the same
    # midpoint rule applied one level down, to a rate disagreement rather than a volume
    # one.
    for passing, receiving in wanted:
        if anchor is not None and (passing, receiving) == ANCHOR:
            continue
        left, right = f"{prefix}{passing}", f"{prefix}{receiving}"
        pair = factors(left, right)
        if pair is None:
            continue
        frame = frame.with_columns(
            [(pl.col(column).cast(pl.Float64) * factor).alias(column)
             for column, factor in zip((left, right), pair)])

    return frame


def make_coherent(frame: pl.DataFrame,
                  team_column: str = "pro_team",
                  position_column: str = "primaryPosition",
                  games_column: str = "usg_expected_games",
                  prefix: str = "USG_",
                  slate: float = 17.0,
                  allocate: bool = True,
                  cohort_column: str = "usg_role_cohort",
                  rank_column: str = "depth_rank") -> pl.DataFrame:
    """Both passes, in the order that makes the second one meaningful.

    The room pass comes in two forms and they are alternatives, never both: phase 2's
    :func:`allocate_qb_starts` when the frame carries a cohort and a depth rank, and
    phase 1's :func:`normalise_qb_room` otherwise. ``allocate=False`` forces the phase 1
    form, which is how G-T1 and G-T2 are read against each other rather than asserted.

    Args:
        frame: Projection frame.
        team_column: Column naming the franchise.
        position_column: Column naming the position.
        games_column: Per-player expected games, read by the phase 1 form.
        prefix: Stat-line prefix.
        slate: Games a team's season offers.
        allocate: Prefer phase 2's role allocation where the inputs are present.
        cohort_column: Column naming the role cohort.
        rank_column: Pre-season depth-chart rank.

    Returns:
        pl.DataFrame: A frame whose team totals a real season could produce.
    """
    allocated = False
    if allocate:
        before = frame
        frame = allocate_qb_starts(frame, team_column, position_column, cohort_column,
                                   rank_column, prefix, slate)
        allocated = frame is not before
    if not allocated:
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
