"""The lineup optimiser, shared by the app and the pipeline.

Moved verbatim out of ``app/lineup.py`` so ``Scripts`` code -- the playoff odds
simulation, which picks every team's lineup for every remaining week -- can call it
without importing from ``app/``. ``app/lineup.py`` re-exports every name here, so
nothing that reads it changed. The rationale for each function stays in its
docstring.

See ``docs/plans/57-playoff-bracket.md``.
"""

from typing import Dict, List, Sequence, Tuple

import polars as pl

from Scripts.draft.board import NON_STARTING_SLOTS
from Scripts.live import LOCKED_COLUMN


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
