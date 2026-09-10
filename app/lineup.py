"""Whose points count this week: the optimal lineup, and the swaps that reach it.

Streamlit-free, like :mod:`draft_view` and :mod:`sheet_view`, so every decision here
is testable without a browser. The Roster and Free Agents tabs draw it.

**Why not** ``Scripts.analytic_utils.get_best_proj_lineup``. That function already
computes an optimal lineup, and it cannot be used here for two reasons. It takes a
live ``espn_api.League``, and this app promises never to put an ESPN client in a
render path. And it returns a *float* -- the optimal total -- when the actionable
half is which player to bench for which. An efficiency score tells you that you left
points on the table; it does not tell you to start Chase Brown.

**Slots are read from the data, not from the metadata.** ``meta["starting_slots"]``
is what ESPN said the last time a *board* was built, and the two artifacts are built
by different commands on different days -- GOP Degenerates' Sep-7 metadata says its
only defensive slot is ``DP: 1`` while its Aug-14 lineups hold players in ``CB``,
``DE``, ``DT``, ``LB`` and ``S``. Inferring the slots from the lineups themselves
means the optimiser is always solving the league that is actually being played. See
:func:`slot_counts`.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import math
from typing import Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple

import polars as pl

from Scripts.draft.board import NON_STARTING_SLOTS
from Scripts.live import (LIVE_POINTS, LIVE_REMAINING, LOCKED_COLUMN,
                          STATE_COLUMN)
from Scripts.live import points_column as live_points_column
from Scripts.live import state_counts

#: Weekly per-source points columns, and the ``meta["weekly_sources_present"]`` key
#: each one answers to.
#:
#: ``ESPN`` has no entry because it is never absent -- it is the artifact's own
#: projection and the imputation base every other source falls back to. ``USG`` is not
#: here at all: there is no weekly TOMCAT head (plan 19 is unstarted).
#:
#: ``ATH`` **joined on 2026-09-09**, when The Athletic began publishing weekly slates.
#: This comment used to say it "publishes seasons, not weeks", which was true when it
#: was written and is the kind of fact that stops being true without anything breaking.
WEEKLY_SOURCES: Tuple[Tuple[str, Optional[str]], ...] = (
    ("ESPN", None),
    ("FP", "fantasypros"),
    ("PINNY", "pinnacle"),
    ("BOL", "betonline"),
    ("ATH", "theathletic"),
)

#: The blend. Named separately because it is not one of the votes.
BLEND = "TRUE"


class Swap(NamedTuple):
    """One start/sit decision.

    Attributes:
        slot: The starting slot in question.
        start: Player to move in.
        sit: Player to move out. None when the slot is simply empty, which is a
            different mistake and reads differently.
        gain: Points the change is projected to add. Always positive.
        start_row: The incoming player's whole row, for a caller that has to show
            more of him than his name -- the Roster tab marks him in the lineup
            table and needs his projections.
        sit_row: The outgoing player's row, or None where ``sit`` is None. This is
            **the pairing**, and it is the reason the rows are carried at all: which
            outgoing player a given incoming one displaces is decided here, and a
            caller that re-derived it by name could not honour the
            position-compatibility preference below.
    """
    slot: str
    start: str
    sit: Optional[str]
    gain: float
    start_row: Optional[dict] = None
    sit_row: Optional[dict] = None


def real_sources(meta: dict) -> List[str]:
    """Weekly source prefixes this league actually has, in blend order.

    A source the store marked absent is **excluded rather than shown as zero**, and
    that is the whole point of the function. The blend imputes a missing source from
    the ESPN/FantasyPros mean, so an absent book does not arrive as a blank column --
    it arrives as a column that agrees perfectly with the mean. Rendering it beside
    the sources that do have an opinion turns absence into unanimity. Verified on
    Knights week 1: ``PINNY_Points`` and ``BOL_Points`` are both 9.84, exactly
    ``MEAN_Points``.

    Args:
        meta: The store's ``meta.json``.

    Returns:
        list: Prefixes, e.g. ``["ESPN", "FP", "ATH"]`` -- which is every league's
        answer once a week's Athletic workbook is imported and while Pinnacle has no
        weekly props.
    """
    present = meta.get("weekly_sources_present") or {}
    return [prefix for prefix, key in WEEKLY_SOURCES
            if key is None or present.get(key, True)]


def points_columns(frame: pl.DataFrame, meta: dict) -> List[str]:
    """Per-source points columns worth showing, blend last.

    Args:
        frame: A lineups frame.
        meta: The store's ``meta.json``.

    Returns:
        list: Column names present on ``frame``.
    """
    columns = [f"{prefix}_Points" for prefix in real_sources(meta)]
    columns.append(f"{BLEND}_Points")
    return [c for c in columns if c in frame.columns]


def _contributed(frame: pl.DataFrame, prefix: str) -> Optional[pl.Expr]:
    """Did ``prefix`` supply at least one real cell for this player?

    The polars twin of :func:`Scripts.projection_utils.source_contributed`, and it
    has to be a twin rather than a call: that module is pandas and this one is
    polars. ``tests/test_lineup.py`` pins the two against each other on one frame so
    they cannot drift on what "real" means.

    There is no ``<prefix>_Points_is_imputed`` flag -- imputation is tracked per stat
    -- so a source's realness for one player is read off its stat flags. Two clauses:
    a **zero does not count** (these frames are dense with structural zeros; a
    kicker's ``FP_passingYards`` is 0.0 and unflagged because nobody imputed it and
    nobody asserted it either), and an **imputed cell does not count**.

    **The sidebar's coverage panel deliberately answers differently for ESPN, and
    that is not this function drifting.** ``player_coverage`` passes
    ``zero_is_real=True`` for the root source, because ESPN publishes ``0.0`` for an
    inactive or bye player and the panel's question is *does this source speak for
    this player*. The question here is *how far apart are the opinions*, and "ESPN
    says none" against no other line is not a spread. Same cell, two different
    facts; the pinning test compares this against ``source_contributed``'s default,
    which is still clause-for-clause identical.

    **This replaced a mean-of-flags share cut at 0.5, which never fired.** Measured
    on Knights 2026 week 1: FantasyPros carries 47 flag columns and fills at most
    **15** of them for any player -- the other 32 are kicker bands, D/ST bands,
    two-point conversions and targets it structurally never publishes -- so the
    minimum imputed share over 334 rows is **0.681** and **no row** cleared the cut.
    ``sources_real`` was therefore uniformly 1 and ``source_spread`` null for every
    player, all season, which is why the Roster tab's "Single-Source Starters"
    counted every starter.

    Args:
        frame: A lineups frame.
        prefix: Source prefix.

    Returns:
        pl.Expr | None: True where the source contributed, or None when it has no
        stat column at all -- which is the case for a points-only frame, and is what
        the caller falls back to ``<prefix>_Points`` being non-null for.
    """
    suffix = "_is_imputed"
    start = f"{prefix}_"
    stats = [c for c in frame.columns
             if c.startswith(start) and not c.endswith(suffix)
             and c[len(start):] not in ("Points", "PosRank")
             and frame.schema[c].is_numeric()]
    if not stats:
        return None

    terms = []
    for column in stats:
        real = pl.col(column).is_not_null() & (pl.col(column) != 0)
        flag = column + suffix
        if flag in frame.columns:
            real = real & pl.col(flag).fill_null(True).not_()
        terms.append(real)
    return pl.any_horizontal(terms)


def with_source_spread(frame: pl.DataFrame, meta: dict) -> pl.DataFrame:
    """Add how many sources really had an opinion, and how far apart they were.

    A wide spread is a risk signal the single blended number hides, and it is
    honest only when measured over sources that are genuinely present -- which is
    why realness is checked twice: at league level through
    :func:`real_sources`, and per player through :func:`_contributed`. On Knights
    2026 week 1 that leaves 256 rows on a single opinion, 48 with two and 10 with
    none, so most rows correctly report no spread at all.

    Args:
        frame: A lineups frame.
        meta: The store's ``meta.json``.

    Returns:
        pl.DataFrame: ``frame`` with ``sources_real`` (int) and ``source_spread``
        (standard deviation of the real sources' points, null below two).
    """
    prefixes = [p for p in real_sources(meta) if f"{p}_Points" in frame.columns]
    if not prefixes:
        return frame.with_columns(
            pl.lit(0).alias("sources_real"),
            pl.lit(None, dtype=pl.Float64).alias("source_spread"))

    # A source counts for a player when it is present at league level and really
    # supplied at least one of his stat cells. A source with no stat column on the
    # frame at all falls back to having a points total, which is the only thing left
    # to read -- see `_contributed`.
    values, counts = [], []
    for prefix in prefixes:
        contributed = _contributed(frame, prefix)
        real = pl.lit(True) if contributed is None else contributed
        real = real & pl.col(f"{prefix}_Points").is_not_null()
        values.append(pl.when(real).then(pl.col(f"{prefix}_Points"))
                        .otherwise(None).alias(f"__real_{prefix}"))
        counts.append(real.cast(pl.Int32))

    frame = frame.with_columns(values + [pl.sum_horizontal(counts).alias("sources_real")])
    real_columns = [f"__real_{p}" for p in prefixes]

    if len(real_columns) < 2:
        frame = frame.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("source_spread"))
    else:
        mean = pl.mean_horizontal([pl.col(c) for c in real_columns])
        squares = [((pl.col(c) - mean) ** 2) for c in real_columns]
        # Population sd over however many sources this row actually has. Null below
        # two, because one number cannot disagree with itself.
        frame = frame.with_columns(
            pl.when(pl.col("sources_real") >= 2)
            .then((pl.sum_horizontal(squares) / pl.col("sources_real")).sqrt())
            .otherwise(None).alias("source_spread"))

    return frame.drop(real_columns)


def slot_counts(frame: pl.DataFrame, meta: dict) -> Dict[str, int]:
    """This league's real starting slots, inferred from what is being started.

    For each slot, the **most** any one team has filled this week, unioned with
    ``meta["starting_slots"]``. Two failure modes, and the union covers both:

    * A manager who left a slot empty would understate that slot, so it is a max
      across teams rather than a count from one.
    * A slot nobody filled all week -- a league with an empty IDP slot across the
      board -- would vanish entirely, so the metadata is a floor.

    Taking the metadata alone is what does not work: it is written when a *board* is
    built, and the lineups it would be applied to are written by a different command.

    **Grouped by owner *and* team name, because an owner name is not a team key.**
    ESPN serves no owner for some teams and ``fetch_utils.set_owner_names`` calls
    them all ``"Unknown Owner"``, so two such teams merge into one 32-row roster.
    Grouping on the owner alone then reads that merged roster as a team filling
    ``QB`` twice and **doubles every slot in the league**: on 2026 week 1
    ``big_red_fantasy_football`` returned QB 2, RB 4, WR 4 against a declared
    QB1/RB2/WR2, so its optimiser was solving a lineup twice the real size and every
    add/drop number it produced was wrong. The team name disambiguates them, and for
    a league whose owners are all named it changes nothing -- one owner has one team.

    Args:
        frame: A lineups frame for one week, all teams.
        meta: The store's ``meta.json``.

    Returns:
        dict: Slot name to count, bench and IR excluded.
    """
    counts: Dict[str, int] = {
        slot: int(n) for slot, n in (meta.get("starting_slots") or {}).items()
        if slot not in NON_STARTING_SLOTS}

    if frame.is_empty() or "slotPosition" not in frame.columns:
        return counts

    started = frame.filter(~pl.col("slotPosition").is_in(list(NON_STARTING_SLOTS)))
    if started.is_empty():
        return counts

    keys = [c for c in ("team_owner", "team_name") if c in started.columns]
    per_team = started.group_by([*keys, "slotPosition"]).len()
    widest = per_team.group_by("slotPosition").agg(pl.col("len").max())
    for slot, n in widest.iter_rows():
        counts[slot] = max(counts.get(slot, 0), int(n))
    return counts


#: Starting slots in the order ESPN lists them, which is the order a roster reads in.
#:
#: Position and *slot* are different things and this orders on the slot: a receiver
#: filling the flex belongs under FLEX, not among the receivers. Bench and IR come
#: last because they did not play.
#:
#: A league has some subset of these, never all -- 12 Dudes has no D/ST, Jeff's has no
#: kicker, only GOP has DP. Missing slots simply do not appear; the order of what
#: remains is unchanged.
SLOT_ORDER: Tuple[str, ...] = (
    "QB", "RB", "WR", "TE", "FLEX", "OP", "DP", "D/ST", "K", "BE", "IR",
)

#: Individual defensive positions, which sort with the DP slot they fill.
#:
#: ESPN reports a roster's defensive slot as ``DP`` in every league configured here,
#: but it names the specific position when a league defines per-position defensive
#: slots. Grouping them with DP keeps an IDP league reading in the same order as
#: everything else rather than scattering five new slots through it.
IDP_SLOTS = frozenset({"DL", "DE", "DT", "NT", "LB", "OLB", "CB", "S", "DB"})


def slot_rank(slot: Optional[str]) -> int:
    """Where a slot sorts, by :data:`SLOT_ORDER`.

    Three aliases, so the map does not need extending every time ESPN names a flex
    differently:

    * Any slot naming more than one position -- ``RB/WR/TE``, ``RB/WR``, ``WR/TE`` --
      is a flex. ``D/ST`` is the exception: it carries a slash and is one position.
    * An individual defensive position sorts with ``DP``.
    * Anything unrecognised sorts just before the bench, so a slot nobody anticipated
      is visible among the starters rather than hidden after IR.

    Args:
        slot: A ``slotPosition`` value, or the optimiser's assigned ``slot``.

    Returns:
        int: Sort key.
    """
    # Scaled by ten so an unrecognised slot can sit *between* two known ones rather
    # than colliding with whichever it rounds to. Unscaled, "just before the bench"
    # resolved to the same key as K.
    if not slot:
        return len(SLOT_ORDER) * 10
    if slot in SLOT_ORDER:
        return SLOT_ORDER.index(slot) * 10
    if slot in IDP_SLOTS:
        return SLOT_ORDER.index("DP") * 10
    if "/" in slot:
        return SLOT_ORDER.index("FLEX") * 10
    return SLOT_ORDER.index("BE") * 10 - 5


def sort_by_slot(frame: pl.DataFrame, slot_column: str = "slot",
                 points_column: str = "TRUE_Points") -> pl.DataFrame:
    """Order a roster the way ESPN shows it: by slot, then by projection.

    Within a slot the higher projection comes first, so a two-back league reads RB1
    then RB2 rather than in whatever order the artifact happened to hold them.

    Args:
        frame: Rows carrying ``slot_column``.
        slot_column: ``"slot"`` for an optimiser assignment, ``"slotPosition"`` for
            the lineup as ESPN has it set.
        points_column: Tie-break within a slot.

    Returns:
        pl.DataFrame: Sorted. Returned unchanged when it has no slot column.
    """
    if frame.is_empty() or slot_column not in frame.columns:
        return frame
    ordered = frame.with_columns(
        pl.col(slot_column)
        .map_elements(slot_rank, return_dtype=pl.Int32)
        .alias("__slot_rank"))
    by = ["__slot_rank"]
    descending = [False]
    if points_column in ordered.columns:
        by.append(points_column)
        descending.append(True)
    return ordered.sort(by, descending=descending).drop("__slot_rank")


def _eligible(row: dict) -> List[str]:
    """Slots a player may legally fill.

    Args:
        row: One lineups row as a dict.

    Returns:
        list: Slot names. Falls back to the player's own position, which is what a
        store with no ``eligiblePositions`` leaves us and is right for K and D/ST.
    """
    slots = row.get("eligiblePositions")
    if slots is not None and len(slots):
        return list(slots)
    position = row.get("player_position") or row.get("primaryPosition")
    return [position] if position else []


def _is_locked(row: dict) -> bool:
    """Whether this player's game has started, so his slot is settled.

    Args:
        row: A lineups row.

    Returns:
        bool: False when the frame carries no game state, which preserves the
        behaviour of every store written before live scoring landed.
    """
    return bool(row.get(LOCKED_COLUMN) or False)


def optimal_lineup(rows: Sequence[dict], slots: Dict[str, int],
                   points_column: str, *,
                   respect_locks: bool = True) -> Tuple[List[dict], float]:
    """The highest-projecting legal lineup a roster can field.

    Greedy, descending by projection, each player taken into the **scarcest** slot he
    is eligible for. Greedy is exact rather than approximate here: fantasy
    eligibility is a transversal matroid -- ``QB ⊂ OP``, ``RB ⊂ RB/WR ⊂ RB/WR/TE ⊂
    OP`` -- and greedy by weight is optimal on a matroid. What greedy can still get
    wrong is *which* eligible slot to spend on a player, so slots are filled
    scarcest-first, scarcity being how many of these players can fill them.

    Measuring scarcity from the roster rather than from the slot name is what makes
    ``D/ST`` work (a slot name with a slash that is not a flex) and ``OP`` work
    (superflex) without a lookup table that has to track ESPN.

    **Players on bye or ruled out are excluded**, because a lineup that starts them
    is not a lineup you would set. ``player_active_status`` carries ``"bye"`` and
    ``"inactive"`` beside ``"active"``.

    **Once a player's game has kicked off he cannot be moved**, and with
    ``respect_locks`` the optimiser says so: a locked starter keeps his slot whatever
    he is now worth, and a locked bench player can no longer be promoted. Without it
    the suggestion is a lineup you are not allowed to set -- which on a Sunday
    afternoon is most of them. The lock is read from
    :data:`Scripts.live.LOCKED_COLUMN`; a frame that does not carry it locks nobody,
    so a store written before live scoring behaves exactly as it did.

    A locked starter is seated **regardless of ``player_active_status``**. A player
    who was ruled out an hour before kickoff scored zero and is still occupying the
    slot; excluding him would quietly hand it to somebody who cannot legally have
    it.

    Args:
        rows: One team's lineups rows as dicts, starters and bench together.
        slots: From :func:`slot_counts`.
        points_column: Which projection to maximise.
        respect_locks: Honour kickoff. False gives the hindsight optimum -- the best
            lineup with the week's results known, which is what "points left on the
            bench" is measured against. See :func:`hindsight_lineup`.

    Returns:
        tuple: ``(starters, total)``. Each starter gains a ``"slot"`` key. Slots the
        roster cannot fill are left empty and contribute nothing -- the honest
        reading of a bye week you have no cover for.
    """
    if not slots:
        return [], 0.0

    openings: List[str] = []
    for slot, count in slots.items():
        openings.extend([slot] * int(count))

    starters: List[dict] = []
    total = 0.0
    candidates = list(rows)

    if respect_locks:
        locked = [r for r in candidates if _is_locked(r)]
        candidates = [r for r in candidates if not _is_locked(r)]
        for row in locked:
            slot = row.get("slotPosition")
            if slot in NON_STARTING_SLOTS or slot not in openings:
                # Locked on the bench, or in a slot this league does not start.
                # Either way he is not available and not startable.
                continue
            starters.append({**row, "slot": slot})
            total += float(row.get(points_column) or 0.0)
            openings.remove(slot)

    playable = [r for r in candidates
                if r.get(points_column) is not None
                and (r.get("player_active_status") or "active") == "active"]
    ranked = sorted(playable, key=lambda r: r[points_column], reverse=True)

    demand = {slot: sum(1 for r in ranked if slot in _eligible(r))
              for slot in set(openings)}
    openings.sort(key=lambda slot: demand.get(slot, 0))

    for row in ranked:
        eligible = _eligible(row)
        for index, slot in enumerate(openings):
            if slot in eligible:
                starters.append({**row, "slot": slot})
                total += float(row[points_column])
                openings.pop(index)
                break
        if not openings:
            break
    return starters, total


def current_lineup(rows: Iterable[dict], points_column: str
                   ) -> Tuple[List[dict], float]:
    """What is actually set to start, and what it projects.

    Read straight off ``slotPosition`` rather than inferred -- this is ESPN's own
    answer to "who is starting", and there is nothing to improve on it.

    Args:
        rows: One team's lineups rows.
        points_column: Which projection to total.

    Returns:
        tuple: ``(starters, total)``, each starter carrying ``"slot"``.
    """
    starters = [{**r, "slot": r.get("slotPosition")} for r in rows
                if r.get("slotPosition") not in NON_STARTING_SLOTS]
    total = sum(float(r[points_column]) for r in starters
                if r.get(points_column) is not None)
    return starters, total


def hindsight_lineup(rows: Sequence[dict], slots: Dict[str, int],
                     points_column: str = LIVE_POINTS
                     ) -> Tuple[List[dict], float]:
    """The best lineup a roster could have fielded, kickoff ignored.

    The *after* question, and a different one from :func:`optimal_lineup`'s. Once
    the week is played there is nothing to decide, and the only thing worth knowing
    is how much the roster held that the lineup did not use. Asking that with locks
    on would always answer "nothing", because by then every slot is settled.

    Args:
        rows: One team's lineups rows.
        slots: From :func:`slot_counts`.
        points_column: What to maximise. Defaults to the resolved live number.

    Returns:
        tuple: ``(starters, total)``.
    """
    return optimal_lineup(rows, slots, points_column, respect_locks=False)


def points_left_on_bench(rows: Sequence[dict], slots: Dict[str, int],
                         points_column: str = LIVE_POINTS) -> float:
    """What the best legal lineup would have scored, minus what was started.

    Zero rather than negative when the lineup that was set *is* the best one --
    :func:`hindsight_lineup` maximises over a superset of what was started, so the
    difference cannot be negative unless a slot was left empty, and reporting a
    negative there would read as a modelling artefact.

    Args:
        rows: One team's lineups rows.
        slots: From :func:`slot_counts`.
        points_column: What to total.

    Returns:
        float: Points a different lineup would have scored.
    """
    _, best = hindsight_lineup(rows, slots, points_column)
    _, started = current_lineup(rows, points_column)
    return max(0.0, best - started)


def changed_ids(current: Sequence[dict], optimal: Sequence[dict]
                ) -> Tuple[set, set]:
    """Who the optimiser adds and who it drops, as ``player_id`` sets.

    **The symmetric difference, computed on who is in each lineup rather than on
    which slot they sit in.** A slot-by-slot comparison double-counts a player who
    merely *moves*: on one real Knights roster, Pickens shifting from ``RB/WR/TE`` to
    ``WR`` and Williams shifting the other way is a permutation worth nothing, and
    pairing by slot read it as two separate swaps gaining 26 points on a lineup worth
    1.5 more. Anyone in both lineups made no decision, whatever slot he occupies.

    Extracted from :func:`swaps` so the Roster tab's table and its start/sit list
    cannot disagree about which rows are a change -- one highlights them and the
    other names them, and a row painted green that the list does not mention is a
    table nobody would trust again.

    Args:
        current: From :func:`current_lineup`.
        optimal: From :func:`optimal_lineup`.

    Returns:
        tuple: ``(coming_in, going_out)``. Both empty when the lineup is already
        optimal, which is the answer you want most weeks.
    """
    starting = {r.get("player_id") for r in current}
    optimal_ids = {r.get("player_id") for r in optimal}
    return (
        {r.get("player_id") for r in optimal if r.get("player_id") not in starting},
        {r.get("player_id") for r in current if r.get("player_id") not in optimal_ids},
    )


def swaps(current: Sequence[dict], optimal: Sequence[dict],
          points_column: str) -> List[Swap]:
    """The changes that turn the set lineup into the optimal one.

    **Computed on who is in each lineup, not on which slot they sit in**, and that
    distinction is the whole function. A slot-by-slot comparison double-counts a
    player who merely *moves*: on one real Knights roster, Pickens shifting from
    ``RB/WR/TE`` to ``WR`` and Williams shifting the other way is a permutation worth
    nothing, and pairing by slot read it as two separate swaps gaining 26 points. The
    lineup was worth 1.5 more in total. A start/sit list whose numbers do not add up
    to the total is worse than no list.

    So the decision set is the symmetric difference -- see :func:`changed_ids`.
    **Σ gain therefore equals the lineup's total gain exactly**, which is what
    :func:`swaps` can be checked against.

    Args:
        current: From :func:`current_lineup`.
        optimal: From :func:`optimal_lineup`.
        points_column: Which projection the gain is measured in.

    Returns:
        list: Swaps worth making, largest gain first. Empty when the lineup is
        already optimal, which is the answer you want most weeks.
    """
    def points(row: Optional[dict]) -> float:
        return float((row or {}).get(points_column) or 0.0)

    added, dropped = changed_ids(current, optimal)
    coming_in = sorted((r for r in optimal if r.get("player_id") in added),
                       key=points, reverse=True)
    going_out = sorted((r for r in current if r.get("player_id") in dropped),
                       key=points, reverse=True)

    # Every pairing of these two sets gives the same total, so the pairing is chosen
    # for how it reads: prefer an outgoing player who could have filled the slot the
    # incoming one takes. Without that preference a flex addition gets paired against
    # a benched quarterback and the line says "start Rico Dowdle at RB/WR/TE, sit
    # Philip Rivers", which is arithmetically fine and advice nobody asked for.
    out: List[Swap] = []
    remaining = list(going_out)
    for incoming in coming_in:
        slot = incoming.get("slot")
        outgoing = next((r for r in remaining if slot in _eligible(r)), None)
        if outgoing is None:
            # No position-compatible partner left. `sit` stays None where the set
            # lineup simply left the slot empty -- a different mistake from starting
            # the wrong player, and it should read differently.
            outgoing = remaining[0] if remaining else None
        if outgoing is not None:
            remaining.remove(outgoing)
        out.append(Swap(
            slot=slot,
            start=incoming.get("player_name") or "?",
            sit=(outgoing or {}).get("player_name"),
            gain=points(incoming) - points(outgoing),
            start_row=incoming,
            sit_row=outgoing,
        ))
    return sorted(out, key=lambda s: s.gain, reverse=True)


#: What ``player_active_status`` says about a player nobody has rostered.
#:
#: **It is not an injury report.** ESPN is answering "is this player in an active
#: lineup slot", and an unrostered player is in nobody's, so the *entire* free-agent
#: pool reads ``inactive`` -- 146 of 146 on Winfield week 1, and every pool in all
#: ten leagues.
#:
#: Reading it as availability is what silently broke :func:`add_drop_gain`.
#: :func:`optimal_lineup` drops a non-active player before it scores him, so every
#: candidate inserted into the after-lineup vanished, every gain came out at exactly
#: 0.0, and the Free Agents tab reported "none of these would improve your lineup" on
#: **all 114** team-weeks across all ten leagues -- a confident negative that was
#: really a filter. See :func:`pool_playable` and :func:`as_rostered`.
POOL_STATUS = "inactive"


def pool_playable(row: dict, points_column: str = f"{BLEND}_Points") -> bool:
    """Whether an unrostered player can still be started this week.

    **Game state answers this directly, where everything before it was a proxy.**
    A player is startable when his game has not kicked off. That is one condition
    covering two questions the old rule could only answer one of: he is not on bye,
    *and* his game is not already over -- and the second one matters, because a free
    agent whose game finished at 16:20 cannot help a lineup at 16:30 however many
    points he scored.

    The proxy it replaces was ``ESPN_Points > 0``, measured across all ten 2026
    leagues as exactly "no game": true for all 81 rostered players ESPN marks as on
    bye, and for 3 of 1,581 it marks active. It was right about byes and blind to
    kickoff. It is kept as the fallback for a store written before live scoring, and
    the note that forced it is still worth keeping: **the blend cannot stand in for
    either version**, because ``TRUE_Points`` imputes an absent source from the
    ESPN/FantasyPros mean, so 24 of those 81 bye players carry a non-zero
    ``TRUE_Points`` of up to 2.12 -- enough to leak a player with no game into a
    comparison, if not enough to win one.

    Args:
        row: A free-agent row.
        points_column: Fallback signal, for a frame with no ESPN column at all.

    Returns:
        bool: True when he can be started this week.
    """
    if row.get(STATE_COLUMN):
        return not _is_locked(row)
    signal = "ESPN_Points" if "ESPN_Points" in row else points_column
    return float(row.get(signal) or 0.0) > 0


#: ESPN designations that may occupy an IR slot.
#:
#: The vocabulary is ESPN's fantasy enum, the same one ``draft_view.INJURY_CODES``
#: names for the board: ``ACTIVE``, ``QUESTIONABLE``, ``DOUBTFUL``, ``OUT``,
#: ``INJURY_RESERVE``, ``SUSPENSION``. Only the last two of those describe a man ESPN
#: will let you park outside the roster count, and ``SUSPENSION`` is excluded because
#: leagues configure it separately and none of these nine do.
IR_ELIGIBLE_STATUSES = frozenset({"INJURY_RESERVE", "OUT"})


def ir_eligible(row: dict) -> bool:
    """Whether this player could be placed in an IR slot.

    Args:
        row: A lineups row carrying ``injury_status``.

    Returns:
        bool: False when the column is absent or null, which is the conservative
        answer -- see :func:`droppable_for`.
    """
    return (row.get("injury_status") or "") in IR_ELIGIBLE_STATUSES


def droppable_for(drop: dict, candidate: dict) -> bool:
    """Whether dropping ``drop`` actually makes room for ``candidate``.

    **An IR slot sits outside the roster count**, so dropping the man in it frees an
    IR slot rather than a bench spot, and only a player ESPN would let *into* that
    slot can use it. Adding a healthy free agent means dropping somebody who is
    occupying a real roster place.

    Without this rule the drop side is actively wrong rather than merely unhelpful,
    because :func:`weakest_starter_candidates` ranks on projection ascending and an
    IR player projects ``0.0`` -- so he sorts **first**. On 2026 week 1 that made
    Jordyn Tyson the top suggested drop on Brian Barrett's ``gop_degenerates``
    roster and Tank Dell the second on Ryan Bonifay's, 19 such rows across the nine
    leagues.

    Args:
        drop: The rostered row being given up.
        candidate: The free agent being added.

    Returns:
        bool: True when the swap is one ESPN would let you make.
    """
    if (drop.get("slotPosition") or "") != "IR":
        return True
    return ir_eligible(candidate)


def playable_pool(frame: pl.DataFrame,
                  points_column: str = f"{BLEND}_Points") -> pl.DataFrame:
    """The free-agent rows that can still be started this week.

    The frame-level counterpart of :func:`pool_playable`, with the same precedence
    and the same fallback, so the table and the suggestions cannot disagree about
    who is available.

    Args:
        frame: Free-agent rows.
        points_column: Fallback signal, for a frame with no ESPN column at all.

    Returns:
        pl.DataFrame: Rows whose game has not kicked off.
    """
    if STATE_COLUMN in frame.columns:
        return frame.filter(
            pl.col(STATE_COLUMN).is_null()
            | ~pl.col(LOCKED_COLUMN).fill_null(False))
    signal = "ESPN_Points" if "ESPN_Points" in frame.columns else points_column
    if signal not in frame.columns:
        return frame
    return frame.filter(pl.col(signal).fill_null(0.0) > 0)


def as_rostered(row: dict) -> dict:
    """A pool row as it would look on a roster, for the optimiser to score.

    The status is **replaced rather than trusted**, because on a pool row it is not a
    statement about the player -- see :data:`POOL_STATUS`. Whether he can actually
    play is settled before this, by :func:`pool_playable`, on a signal that means
    what it says.

    Args:
        row: A free-agent row.

    Returns:
        dict: A copy ESPN would call active.
    """
    return {**row, "player_active_status": "active"}


#: A starter is out-projected by somebody who is sitting on the waiver wire.
#:
#: The urgent one: it does not say your roster could be better in the abstract, it
#: says the lineup you are about to play is worse than one you could field today.
UPGRADE_CRITICAL = "critical"

#: A bench player is out-projected, and no starter is.
#:
#: The same comparison one rung down. Worth knowing and not worth interrupting for --
#: nothing about Sunday changes.
UPGRADE_DEPTH = "depth"


#: How much better an available player must project before it is worth saying.
#:
#: Measured from the blend's own uncertainty rather than picked: across all ten 2026
#: leagues, the standard deviation *between the sources* is a median of **0.43**
#: points and a mean of 0.48. An edge of a quarter of a point is smaller than the
#: disagreement among the numbers it was computed from, so reporting it as an upgrade
#: dresses noise up as a decision. Two real rows were doing exactly that -- a
#: ``+0.0`` critical alert on Winfield and another on GOP.
UPGRADE_MIN_MARGIN = 0.5


class Upgrade(NamedTuple):
    """One starting slot the available pool can improve.

    **Keyed by the slot, which is the only key that does not flood.** Keyed by the
    free agent, one thin roster spot produces an alert per candidate: seven available
    quarterbacks out-project the only one on Winfield's roster, so seven identical
    rows. Keyed by the rostered player, one good candidate produces an alert per
    victim: in the two superflex leagues a free-agent quarterback out-projects every
    receiver, back and end who is eligible for ``OP``, which was twelve rows about
    one player. There are never more slots than the league has slots.

    Attributes:
        slot: The starting slot the candidate would fill.
        severity: :data:`UPGRADE_CRITICAL` when a **starter** at this slot is
            out-projected, :data:`UPGRADE_DEPTH` when only a bench player is. One or
            the other, never both -- critical wins and the depth row is suppressed.
        best: The highest-projecting available player eligible for the slot.
        over: The weakest of your players at this slot that he out-projects -- the
            one you would actually replace. Carries his own ``slotPosition``, which
            is not always ``slot``: in a superflex league a receiver is eligible for
            ``OP`` while starting at ``WR``.
        margin: ``best`` minus ``over``. At least :data:`UPGRADE_MIN_MARGIN`.
        beaten: How many of your players at this slot he out-projects. One is a
            decision; five means a position you have not addressed.
        better: How many available players out-project ``over``. Large numbers are
            the story -- 57 available defenders beat one GOP linebacker.
    """

    slot: str
    severity: str
    best: dict
    over: dict
    margin: float
    beaten: int
    better: int


def competing_slots(row: dict, slots: Dict[str, int]) -> set:
    """Which of this league's starting slots a player may actually fill.

    The join between two players is **a shared slot, not a shared position**, and
    that is what makes the comparison right in the leagues that are not
    one-position-per-slot. A running back and a receiver never share a position and
    compete directly for ``RB/WR/TE``; a quarterback and a running back compete for
    ``OP`` in the two superflex leagues here; five different defensive positions
    compete for ``DP`` in GOP. Comparing on ``player_position`` would miss every one
    of those, and a hand-written map of which positions fill which slot is a second
    copy of something ESPN already tells us.

    Args:
        row: A lineups row.
        slots: From :func:`slot_counts` -- the *starting* slots, so the bench and IR
            are already out.

    Returns:
        set: Slot names, empty for a player this league cannot start anywhere.
    """
    return set(_eligible(row)) & set(slots)


def upgrades(pool: Sequence[dict], roster: Sequence[dict], slots: Dict[str, int],
             points_column: str, *,
             min_margin: float = UPGRADE_MIN_MARGIN) -> List[Upgrade]:
    """Starting slots the available pool can improve, most urgent first.

    **Compared against the lineup as ESPN has it set**, not against the optimal one,
    because that is the lineup that will actually play. The two can disagree: a
    starter the Roster tab already wants benched shows up here as out-projected even
    though fixing the lineup would settle it without a waiver claim, which is why the
    page says to fix the lineup first.

    **The comparison is on a shared slot, never on a shared position** -- see
    :func:`competing_slots`. That is what puts a free-agent quarterback up against a
    receiver in a superflex league's ``OP``, a back up against a receiver in the
    flex, and five defensive positions up against each other in ``DP``.

    Args:
        pool: The free-agent rows. Pass the **whole** pool rather than a filtered
            view: a flag that disappears when you filter the table to quarterbacks is
            not a flag.
        roster: One team's rows, starters and bench together.
        slots: From :func:`slot_counts`.
        points_column: Which projection to compare on.
        min_margin: See :data:`UPGRADE_MIN_MARGIN`.

    Returns:
        list: At most one :class:`Upgrade` per available player -- and so never more
        than the league has slots -- criticals first and by margin within a
        severity. Empty is the common answer and the one worth trusting.
    """
    def points(row: dict) -> float:
        return float(row.get(points_column) or 0.0)

    available = [row for row in pool
                 if row.get(points_column) is not None
                 and pool_playable(row, points_column)
                 and competing_slots(row, slots)]
    if not available:
        return []

    found: List[Upgrade] = []
    for slot in sorted(slots, key=slot_rank):
        candidates = [row for row in available if slot in competing_slots(row, slots)]
        # `not _is_locked`: a man whose game has kicked off cannot be replaced this
        # week, so flagging him as out-projected is an alert about a decision that
        # is already made. His number is also no longer a projection -- on a
        # finished game it is the banked score -- so the margin would not mean what
        # the column says it means.
        mine = [row for row in roster
                if slot in competing_slots(row, slots) and not _is_locked(row)]
        if not candidates or not mine:
            continue
        best = max(candidates, key=points)

        starting = [r for r in mine
                    if r.get("slotPosition") not in NON_STARTING_SLOTS]
        benched = [r for r in mine if r.get("slotPosition") in NON_STARTING_SLOTS]
        for severity, group in ((UPGRADE_CRITICAL, starting),
                                (UPGRADE_DEPTH, benched)):
            beaten = [r for r in group if points(best) - points(r) >= min_margin]
            if not beaten:
                continue
            # The weakest man beaten, because he is the one you would replace -- and
            # the margin is therefore the whole size of the gap rather than its
            # narrowest edge.
            over = min(beaten, key=points)
            found.append(Upgrade(
                slot=slot, severity=severity, best=best, over=over,
                margin=points(best) - points(over),
                beaten=len(beaten),
                better=sum(1 for c in candidates
                           if points(c) - points(over) >= min_margin),
            ))
            break  # critical wins; the two tiers are mutually exclusive

    # One row per candidate, at the slot where he does the most good. Without this a
    # superflex league reports the same free-agent quarterback at ``OP`` and again at
    # ``QB``, and every league reports the same receiver at ``WR`` and again at the
    # flex -- two rows about one add, and the second one teaches you nothing. Which
    # slot he ends up filling is `add_drop_gain`'s problem, not the flag's.
    #
    # Ordered before deduplicating, so a candidate who is critical somewhere keeps
    # his critical row even when a depth slot shows a wider gap.
    ordered = sorted(found,
                     key=lambda u: (u.severity != UPGRADE_CRITICAL, -u.margin))
    seen, kept = set(), []
    for upgrade in ordered:
        if upgrade.best.get("player_id") in seen:
            continue
        seen.add(upgrade.best.get("player_id"))
        kept.append(upgrade)
    return kept


def best_available_per_slot(pool: Sequence[dict], slots: Dict[str, int],
                            points_column: str) -> List[dict]:
    """The best available player for each starting slot, deduplicated.

    **The candidate set the add/drop pairing should have been using all along.**
    Taking the top N of the pool by raw projection is position-blind, and the
    positions are not on the same scale: on Winfield week 1 the top six available
    players are six quarterbacks, in a league that starts one. The best available
    running back, tight end and kicker were never scored at all.

    One player can win several slots -- the best available back is usually the best
    available flex too -- so the result is deduplicated by ``player_id`` and is
    therefore no longer than the slot list.

    Args:
        pool: The free-agent rows.
        slots: From :func:`slot_counts`.
        points_column: Which projection to rank on.

    Returns:
        list: Rows, highest projection first.
    """
    def points(row: dict) -> float:
        return float(row.get(points_column) or 0.0)

    playable = [row for row in pool
                if row.get(points_column) is not None
                and pool_playable(row, points_column)]

    picked: Dict[object, dict] = {}
    for slot in slots:
        eligible = [row for row in playable if slot in _eligible(row)]
        if not eligible:
            continue
        best = max(eligible, key=points)
        picked.setdefault(best.get("player_id"), best)
    return sorted(picked.values(), key=points, reverse=True)


def add_drop_gain(roster: Sequence[dict], slots: Dict[str, int], points_column: str,
                  candidate: dict, drop_id) -> float:
    """What adding one player and dropping another does to the optimal lineup.

    **The metric the waiver decision actually turns on.** Comparing a free agent's
    projection to a rostered player's compares two benches; what matters is whether
    the lineup you would field on Sunday gets better, which is a question about the
    whole roster. A receiver who out-projects your worst bench player by four points
    is worth nothing if he would not start.

    Args:
        roster: The team's current rows.
        slots: From :func:`slot_counts`.
        points_column: Which projection to optimise.
        candidate: The free agent's row.
        drop_id: ``player_id`` of the player being dropped.

    Returns:
        float: Points the optimal lineup gains. Zero or negative means the move does
        not help this week.
    """
    _, before = optimal_lineup(roster, slots, points_column)
    after_rows = [r for r in roster if r.get("player_id") != drop_id]
    # `as_rostered`, not `candidate`: the optimiser drops a non-active player, and
    # every pool row is marked inactive. Appending him raw made this function return
    # 0.0 for every candidate ever passed to it.
    after_rows.append(as_rostered(candidate))
    _, after = optimal_lineup(after_rows, slots, points_column)
    return after - before


def weakest_starter_candidates(roster: Sequence[dict], slots: Dict[str, int],
                               points_column: str, *,
                               exclude_locked: bool = True) -> List[dict]:
    """Roster players ordered by how little the lineup would miss them.

    The drop side of an add/drop. Ordered by projection ascending among players who
    do **not** make the optimal lineup, then by projection among those who do -- so
    the first suggestions are people whose absence costs nothing this week.

    **A player whose game has kicked off is not a drop candidate.** His points are
    banked either way, so dropping him cannot recover them, and the projection this
    sorts on is no longer a projection. Left unfiltered he sorts *first*, because a
    finished zero looks exactly like a worthless bench player: on 2026 week 1 the
    top suggested drop on Ryan Bonifay's ``gop_degenerates`` roster was Nick
    Emmanwori, already ``post`` at 0.0. 5-20 rostered players per league were in
    that state on a single Wednesday.

    Args:
        roster: The team's current rows.
        slots: From :func:`slot_counts`.
        points_column: Which projection to optimise.
        exclude_locked: Drop players whose game has started from the result. Off
            only for callers reasoning about a week that is already over.

    Returns:
        list: Rows, most droppable first.
    """
    candidates = [r for r in roster if not (exclude_locked and _is_locked(r))]
    starters, _ = optimal_lineup(roster, slots, points_column)
    starting = {r.get("player_id") for r in starters}
    return sorted(
        candidates,
        key=lambda r: (r.get("player_id") in starting,
                       float(r.get(points_column) or 0.0)))


def bye_and_out(rows: Iterable[dict]) -> List[dict]:
    """Rostered players who cannot play this week.

    Args:
        rows: One team's lineups rows.

    Returns:
        list: Rows whose ``player_active_status`` is not ``"active"``.
    """
    return [r for r in rows
            if (r.get("player_active_status") or "active") != "active"]


def team_total_sd(starters: Sequence[dict], sd_column: str = "weekly_sd"
                  ) -> Optional[float]:
    """Independent standard deviation of a lineup's weekly total.

    Args:
        starters: From :func:`optimal_lineup` or :func:`current_lineup`.
        sd_column: Per-player weekly sd column.

    Returns:
        float | None: ``sqrt(Σ sd²)``, or None when no starter carries one.
    """
    variances = [float(r[sd_column]) ** 2 for r in starters
                 if r.get(sd_column) is not None]
    return math.sqrt(sum(variances)) if variances else None
