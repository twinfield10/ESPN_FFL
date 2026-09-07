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

#: Weekly per-source points columns, and the ``meta["weekly_sources_present"]`` key
#: each one answers to.
#:
#: ``ESPN`` has no entry because it is never absent -- it is the artifact's own
#: projection and the imputation base every other source falls back to. ``USG`` and
#: ``ATH`` are not here at all: there is no weekly TOMCAT head (plan 19 is unstarted)
#: and The Athletic publishes seasons, not weeks.
WEEKLY_SOURCES: Tuple[Tuple[str, Optional[str]], ...] = (
    ("ESPN", None),
    ("FP", "fantasypros"),
    ("PINNY", "pinnacle"),
    ("BOL", "betonline"),
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
    """
    slot: str
    start: str
    sit: Optional[str]
    gain: float


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
        list: Prefixes, e.g. ``["ESPN", "FP"]`` -- which is every league's answer in
        2026, both books having no weekly props.
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


def _imputed_share(frame: pl.DataFrame, prefix: str) -> Optional[pl.Expr]:
    """Share of ``prefix``'s stat columns that were imputed, per row.

    There is no ``<prefix>_Points_is_imputed`` flag -- imputation is tracked per stat
    -- so a source's realness for one player has to be read off its stat flags. A
    source imputed on most of its stats did not have an opinion about that player.

    Args:
        frame: A lineups frame.
        prefix: Source prefix.

    Returns:
        pl.Expr | None: Mean of the flags, or None when the source has none (which
        is the case for ESPN, and for a store built before the flags existed).
    """
    flags = [c for c in frame.columns
             if c.startswith(f"{prefix}_") and c.endswith("_is_imputed")]
    if not flags:
        return None
    return pl.mean_horizontal([pl.col(c).cast(pl.Float64) for c in flags])


def with_source_spread(frame: pl.DataFrame, meta: dict) -> pl.DataFrame:
    """Add how many sources really had an opinion, and how far apart they were.

    A wide spread is a risk signal the single blended number hides, and it is
    honest only when measured over sources that are genuinely present -- which is
    why realness is checked twice: at league level through
    :func:`real_sources`, and per player through :func:`_imputed_share`. On Knights
    week 1, FantasyPros is imputed for 178 of 235 rostered players, so most rows are
    a single opinion and correctly report no spread at all.

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

    # A source counts for a player when it is present at league level and not mostly
    # imputed for him. Half is the cut: a source imputed on more stats than it
    # projected is not describing that player.
    values, counts = [], []
    for prefix in prefixes:
        share = _imputed_share(frame, prefix)
        real = pl.lit(True) if share is None else (share < 0.5)
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

    per_team = started.group_by(["team_owner", "slotPosition"]).len()
    widest = per_team.group_by("slotPosition").agg(pl.col("len").max())
    for slot, n in widest.iter_rows():
        counts[slot] = max(counts.get(slot, 0), int(n))
    return counts


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


def optimal_lineup(rows: Sequence[dict], slots: Dict[str, int],
                   points_column: str) -> Tuple[List[dict], float]:
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

    Args:
        rows: One team's lineups rows as dicts, starters and bench together.
        slots: From :func:`slot_counts`.
        points_column: Which projection to maximise.

    Returns:
        tuple: ``(starters, total)``. Each starter gains a ``"slot"`` key. Slots the
        roster cannot fill are left empty and contribute nothing -- the honest
        reading of a bye week you have no cover for.
    """
    if not slots:
        return [], 0.0

    playable = [r for r in rows
                if r.get(points_column) is not None
                and (r.get("player_active_status") or "active") == "active"]
    ranked = sorted(playable, key=lambda r: r[points_column], reverse=True)

    demand = {slot: sum(1 for r in ranked if slot in _eligible(r)) for slot in slots}
    openings: List[str] = []
    for slot, count in slots.items():
        openings.extend([slot] * int(count))
    openings.sort(key=lambda slot: demand.get(slot, 0))

    starters: List[dict] = []
    total = 0.0
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

    So the decision set is the symmetric difference: players the optimiser starts who
    are currently benched, against players currently starting who it would bench.
    Anyone in both lineups made no decision, whatever slot they occupy.
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

    starting = {r.get("player_id") for r in current}
    optimal_ids = {r.get("player_id") for r in optimal}

    coming_in = sorted((r for r in optimal if r.get("player_id") not in starting),
                       key=points, reverse=True)
    going_out = sorted((r for r in current if r.get("player_id") not in optimal_ids),
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
        ))
    return sorted(out, key=lambda s: s.gain, reverse=True)


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
    after_rows.append(candidate)
    _, after = optimal_lineup(after_rows, slots, points_column)
    return after - before


def weakest_starter_candidates(roster: Sequence[dict], slots: Dict[str, int],
                               points_column: str) -> List[dict]:
    """Roster players ordered by how little the lineup would miss them.

    The drop side of an add/drop. Ordered by projection ascending among players who
    do **not** make the optimal lineup, then by projection among those who do -- so
    the first suggestions are people whose absence costs nothing this week.

    Args:
        roster: The team's current rows.
        slots: From :func:`slot_counts`.
        points_column: Which projection to optimise.

    Returns:
        list: Rows, most droppable first.
    """
    starters, _ = optimal_lineup(roster, slots, points_column)
    starting = {r.get("player_id") for r in starters}
    return sorted(
        roster,
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
