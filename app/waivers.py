"""Which waiver move to make, and how loudly to say it.

`lineup` answers arithmetic questions about a roster. This answers the decision
question on top of them, and it exists because the arithmetic alone was producing
211 recommended moves across 113 team-weeks -- 195 of them flagged as urgent, 58 at
the two positions everybody streams. A panel that fires that often is not advice.

Four things narrow it, and none of them is a hand-tuned constant.

**The threshold.** :func:`lineup.lineup_threshold` says what a player must project
to change your lineup at all. Below it an add improves your bench, and a bench that
never plays is worth nothing -- so the move is suppressed unless rest-of-season or
upside says he will not stay there.

**Scarcity, priced.** :func:`lineup.insurance_value` is what the man you are
dropping would be worth the week a starter above him sits. Subtracting it means a
thin position defends itself in points, competing with the value of the add on one
scale, rather than by a veto that cannot be wrong and therefore cannot be tested.

**Streaming, separated.** Kicker and D/ST are 41% of the successful in-season adds
in the 2025 study and are nearly costless each time, so they get their own quiet
line instead of crowding the table. They are also the two positions whose
rest-of-season conversion is loosest (plan 49, G-R1), which is a second reason
arrived at independently.

**Confidence instead of a margin.** ``lineup.UPGRADE_MIN_MARGIN`` is 0.5, sized
from *between-source* disagreement -- a median sd of 0.43 across the five
projections. That is how much the sources argue, not how wrong they are together.
What decides whether a move was right is *outcome* dispersion, and this repo has
already fitted it: ``Data/NFL/models/weekly_dispersion_1.0.0.json``, 15,989 started
player-weeks. A swap of one skill player for another has a realised-difference
standard deviation near **11 points**, so a ``+0.5`` alert is **0.045 sd** -- a 52%
coin flip wearing a red badge. Reporting the probability says that out loud, and it
is the reason rest-of-season matters arithmetically rather than sentimentally: gain
accumulates linearly in weeks and noise accumulates as the square root of them.

See ``docs/plans/49-rest-of-season-waivers.md``.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import math
import re
from typing import Dict, Iterable, List, NamedTuple, Optional, Sequence

import lineup as lu
from Scripts.draft.board import STREAMED_POSITIONS

#: A move that puts a better player in your starting lineup this week.
VERDICT_LINEUP = "lineup"

#: Same, at a position everybody churns. Maintenance, not an emergency.
VERDICT_STREAM = "stream"

#: Does not help this week, but the rest-of-season consensus says he is a starter
#: in a league this shape -- or his own upside band reaches over your bar.
VERDICT_ROS = "ros"

#: Helps this week, but the man going out is your only cover at a thin slot.
VERDICT_COSTLY = "costly"

#: How each verdict reads. Nothing driven by rest-of-season is ever an emergency:
#: the critical tier belongs to the five-source weekly blend, which has earned it.
VERDICT_STYLE = {
    VERDICT_LINEUP: ("🚨", "error"),
    VERDICT_COSTLY: ("⚠️", "warning"),
    VERDICT_ROS: ("📈", "warning"),
    VERDICT_STREAM: ("🔁", "info"),
}

#: Minimum experts behind a rest-of-season rank before it may rescue a move.
#:
#: Two. With one there is no consensus, only a person. These pages carry 2-3.
MIN_ROS_EXPERTS = 2

#: ``"RB27"`` -> 27.
_POS_RANK = re.compile(r"(\d+)\s*$")


class Move(NamedTuple):
    """One add/drop pairing, scored and judged."""

    add: dict
    drop: dict
    week_gain: float
    add_value: float
    drop_cost: float
    insurance: float
    net: float
    confidence: Optional[float]
    verdict: str
    reason: str


def pos_rank_number(pos_rank) -> Optional[int]:
    """The integer inside a positional rank like ``"RB27"``.

    Args:
        pos_rank: The ``ros_pos_rank`` string, or None.

    Returns:
        int | None: The rank, or None when unparseable.
    """
    if not pos_rank:
        return None
    found = _POS_RANK.search(str(pos_rank))
    return int(found.group(1)) if found else None


def swap_sd(model: Optional[dict], add: dict, over: Optional[dict],
            points_column: str) -> Optional[float]:
    """Standard deviation of the realised difference a swap makes.

    ``sqrt(sd_in^2 + sd_out^2)``, the independent sum -- which plan 42 measured to
    hold at team level (coverage 0.802 against a nominal 0.800), and two players on
    different teams are a weaker assumption than that one.

    Args:
        model: ``Scripts.outcomes.weekly.load()`` output, or None.
        add: The incoming player's row.
        over: The starter he displaces, or None when he displaces nobody.
        points_column: Which projection is the mean.

    Returns:
        float | None: None without a fitted model, which means no confidence is
        published rather than a made-up one.
    """
    if model is None:
        return None
    from Scripts.outcomes.weekly import player_sd

    incoming = player_sd(model, add.get("player_position"),
                         float(add.get(points_column) or 0.0))
    outgoing = 0.0
    if over is not None:
        outgoing = player_sd(model, over.get("player_position"),
                             float(over.get(points_column) or 0.0))
    total = math.sqrt(incoming ** 2 + outgoing ** 2)
    return total or None


def confidence(gain: float, sd: Optional[float]) -> Optional[float]:
    """P(the move actually helps), on a normal with the fitted dispersion.

    Args:
        gain: The projected gain.
        sd: From :func:`swap_sd`.

    Returns:
        float | None: A probability in (0, 1), or None when no model is loaded.
    """
    if sd is None or sd <= 0:
        return None
    return 0.5 * (1.0 + math.erf(gain / (sd * math.sqrt(2.0))))


def ros_startable(row: dict, replacement: Dict[str, int]) -> bool:
    """Whether the rest-of-season consensus makes him a starter in *this* league.

    A rank rather than a points comparison, which is the only thing FantasyPros'
    number can honestly support -- see ``Scripts/ros``. ``replacement`` comes from
    ``Scripts.draft.board.replacement_ranks``, so a superflex league's quarterback
    bar is deeper than a one-QB league's and a position nobody starts is absent
    rather than set to one.

    Args:
        row: A pool row carrying ``ros_pos_rank`` and ``ros_experts``.
        replacement: Position to replacement rank.

    Returns:
        bool: False whenever the source is silent or too thin to speak.
    """
    experts = row.get("ros_experts")
    if experts is None or float(experts) < MIN_ROS_EXPERTS:
        return False
    rank = pos_rank_number(row.get("ros_pos_rank"))
    bar = replacement.get(row.get("player_position"))
    if rank is None or bar is None:
        return False
    return rank <= int(bar)


def upside_per_game(row: dict) -> Optional[float]:
    """His 90th-percentile outcome as a **per-game** figure.

    ``pts_p90`` on the season board is a *season total* -- Jahmyr Gibbs reads 621 --
    and a slot threshold is a *weekly* number around 10 to 25. Comparing them
    directly is a unit error, and a loud one: it fired on **324 of 369** upside
    rescues across the nine leagues, which is to say on essentially everybody.
    Measured before it shipped, which is the only reason it is not in the panel.

    Args:
        row: A pool row carrying ``pts_p90`` and ``games``.

    Returns:
        float | None: Points per game at the 90th percentile, or None when either
        input is missing. ``games`` is not defaulted to 17 -- an assumed
        denominator is how the season and weekly grains got mixed in the first
        place.
    """
    ceiling = row.get("pts_p90")
    games = row.get("games")
    if ceiling is None or games is None:
        return None
    try:
        ceiling, games = float(ceiling), float(games)
    except (TypeError, ValueError):
        return None
    if games <= 0 or ceiling != ceiling or games != games:
        return None
    return ceiling / games


#: **A ceiling is shown and never gated on, and that was decided by measurement.**
#:
#: The obvious upside rule -- promote an add whose ``pts_p90`` clears the slot
#: threshold -- was built and cut. Three findings, in the order they arrived:
#:
#: 1. ``pts_p90`` is a **season** total (Jahmyr Gibbs reads 621) and a threshold is
#:    a **weekly** number near 10-25. Compared directly the rule fired on 324 of
#:    369 rescues, i.e. on everybody. Per-game it still fired 195 times, 1.7 per
#:    team-week.
#: 2. Comparing a **90th percentile** against a **median** bar is structurally
#:    generous whatever the units: a bench player's ceiling beats a central
#:    projection most of the time, by construction.
#: 3. ``p_top12`` looked like the calibrated replacement, and its own top of the
#:    pool refutes it -- Troy Franklin at **0.41** on a 3.3-point projection,
#:    Alvin Kamara at 0.28 on 2.31. A wide band on a player the model knows little
#:    about is **ignorance, not upside**, which is the trap plan 18 recorded when
#:    the usage model "inflated exactly the players it knew nothing about".
#:
#: So the distribution columns travel with a suggestion as *context* -- they are
#: real and worth seeing beside a drop you are unsure about -- and nothing is ever
#: recommended because of them. Rest-of-season rank, backed by people rather than
#: by our own uncertainty, is the only thing allowed to rescue a move.
UPSIDE_IS_CONTEXT_NOT_A_GATE = True


def is_streamed(row: dict) -> bool:
    """Whether this is a position the league churns without cost.

    Reuses ``Scripts.draft.board.STREAMED_POSITIONS``, which the draft board
    already refuses to compute season value for, for the same reason.
    """
    return (row.get("player_position") or "") in STREAMED_POSITIONS


def judge(add: dict, drop: dict, *, week_gain: float, add_val: float,
          cost: float, insurance: float, bar: Optional[float],
          replacement: Dict[str, int], min_margin: float) -> tuple:
    """Classify one pairing, and say why in a sentence.

    Returns:
        tuple: ``(verdict, reason)``, or ``(None, reason)`` for a move not worth
        showing.
    """
    net = week_gain - insurance

    if is_streamed(add):
        if week_gain < min_margin:
            return None, "a streaming swap inside the noise"
        return VERDICT_STREAM, "the week's best available at a streamed position"

    if week_gain >= min_margin and insurance > week_gain:
        return VERDICT_COSTLY, (
            f"{drop.get('player_name')} is your cover at a thin slot — worth "
            f"{insurance:.1f} the week a starter above him sits, against "
            f"{week_gain:+.1f} gained now")

    if week_gain >= min_margin and net > 0:
        return VERDICT_LINEUP, "improves the lineup you would field on Sunday"

    # A rest-of-season rescue has to be **cheap**, not merely justified. The drop
    # is chosen to maximise this week's gain, which on a thin roster can be a real
    # starter: Jeff's league week 1 proposed giving up Terry McLaurin -- worth
    # 11.9 the week somebody above him sits -- for a player who does nothing now.
    # A claim that does not change your Sunday is only worth a roster spot when
    # the spot is genuinely spare.
    if insurance > min_margin:
        return None, (
            f"rest-of-season says yes, but {drop.get('player_name')} is real cover "
            f"at {insurance:.1f}")

    # And it must not make *this* week measurably worse. A claim for December is
    # still a claim you play Sunday with, and a rest-of-season rank cannot buy back
    # points you gave away now -- it has no units to buy them in.
    if week_gain < -min_margin:
        return None, (
            f"rest-of-season says yes, but the swap costs {-week_gain:.1f} now")

    if ros_startable(add, replacement):
        return VERDICT_ROS, (
            f"no help this week, but the rest-of-season consensus has him "
            f"{add.get('ros_pos_rank')} — a starter in a league this shape, and "
            f"{drop.get('player_name')} costs you nothing to give up")

    return None, "improves the bench only"


def rank_moves(roster: Sequence[dict], pool: Sequence[dict],
               slots: Dict[str, int], points_column: str, *,
               candidates: Optional[Sequence[dict]] = None,
               drops: Optional[Sequence[dict]] = None,
               replacement: Optional[Dict[str, int]] = None,
               model: Optional[dict] = None,
               min_margin: float = lu.UPGRADE_MIN_MARGIN) -> List[Move]:
    """Every add/drop worth showing, best first.

    Each candidate keeps only his best legal drop, as before. What is new is that
    the pairing is judged rather than merely scored, and that a move improving only
    the bench is dropped unless something says it will not stay there.

    Args:
        roster: The team's rows.
        pool: The free-agent rows, unfiltered.
        slots: From :func:`lineup.slot_counts`.
        points_column: Which projection to optimise.
        candidates: Override for :func:`lineup.best_available_per_slot`.
        drops: Override for :func:`lineup.weakest_starter_candidates`.
        replacement: From ``Scripts.draft.board.replacement_ranks``.
        model: The fitted weekly dispersion, for the confidence column.
        min_margin: See :data:`lineup.UPGRADE_MIN_MARGIN`.

    Returns:
        list: Moves, sorted by verdict severity then by net gain.
    """
    replacement = replacement or {}
    if candidates is None:
        candidates = lu.best_available_per_slot(pool, slots, points_column)
    if drops is None:
        droppable = lu.weakest_starter_candidates(roster, slots, points_column)
        on_ir = [r for r in droppable if (r.get("slotPosition") or "") == "IR"]
        drops = [r for r in droppable
                 if (r.get("slotPosition") or "") != "IR"][:4] + on_ir

    starters, _ = lu.optimal_lineup(roster, slots, points_column)
    moves: List[Move] = []

    for candidate in candidates:
        best = None
        for drop in drops:
            if not lu.droppable_for(drop, candidate):
                continue
            gain = lu.add_drop_gain(roster, slots, points_column, candidate,
                                    drop.get("player_id"))
            if best is None or gain > best[1]:
                best = (drop, gain)
        if best is None:
            continue

        drop, week_gain = best
        remaining = [r for r in roster if r.get("player_id") != drop.get("player_id")]
        bar = lu.lineup_threshold(remaining, slots, points_column,
                                  lu._eligible(candidate))
        add_val = lu.add_value(remaining, slots, points_column,
                               lu.as_rostered(candidate))
        cost = lu.drop_cost(roster, slots, points_column, drop.get("player_id"))
        insurance = lu.insurance_value(roster, slots, points_column,
                                       drop.get("player_id"))

        verdict, reason = judge(
            candidate, drop, week_gain=week_gain, add_val=add_val, cost=cost,
            insurance=insurance, bar=bar, replacement=replacement,
            min_margin=min_margin)
        if verdict is None:
            continue

        displaced = _displaced(starters, candidate, slots, points_column)
        moves.append(Move(
            add=candidate, drop=drop, week_gain=week_gain, add_value=add_val,
            drop_cost=cost, insurance=insurance, net=week_gain - insurance,
            confidence=confidence(week_gain,
                                  swap_sd(model, candidate, displaced,
                                          points_column)),
            verdict=verdict, reason=reason))

    order = {VERDICT_LINEUP: 0, VERDICT_COSTLY: 1, VERDICT_ROS: 2,
             VERDICT_STREAM: 3}
    return sorted(moves, key=lambda m: (order[m.verdict], -m.net))


def _displaced(starters: Sequence[dict], candidate: dict, slots: Dict[str, int],
               points_column: str) -> Optional[dict]:
    """The weakest starter this candidate could displace, for the sd estimate."""
    mine = set(lu._eligible(candidate)) & set(slots)
    reachable = [s for s in starters if set(lu._eligible(s)) & mine]
    if not reachable:
        return None
    return min(reachable, key=lambda r: float(r.get(points_column) or 0.0))


def best_streamers(pool: Sequence[dict], roster: Sequence[dict],
                   slots: Dict[str, int], points_column: str) -> List[dict]:
    """The best available kicker and defence, against what you have.

    Their own line, because they are 41% of the successful in-season adds in the
    2025 study and each one is nearly costless -- so they crowd out the decisions
    that matter if they compete in the main table, and they are worth a glance
    every week if they do not.

    Args:
        pool: The free-agent rows.
        roster: The team's rows.
        slots: From :func:`lineup.slot_counts`.
        points_column: Which projection to rank on.

    Returns:
        list: One dict per streamed position the league starts, carrying ``slot``,
        ``best``, ``mine`` and ``gain``.
    """
    def points(row):
        return float(row.get(points_column) or 0.0)

    out = []
    for slot in sorted(slots, key=lu.slot_rank):
        if slot not in STREAMED_POSITIONS:
            continue
        available = [r for r in pool
                     if slot in lu._eligible(r) and lu.pool_playable(r, points_column)]
        if not available:
            continue
        best = max(available, key=points)
        mine = [r for r in roster if slot in lu._eligible(r)
                and not lu._is_locked(r)]
        held = max(mine, key=points) if mine else None
        gain = points(best) - (points(held) if held else 0.0)
        # Only when the wire is actually better. "The defence you already have is
        # the best one available" is true most weeks and is not worth a row --
        # this block exists to be glanced at, and a block that is always there
        # stops being glanced at.
        if gain <= 0:
            continue
        out.append({"slot": slot, "best": best, "mine": held, "gain": gain})
    return out


def threshold_table(roster: Sequence[dict], pool: Sequence[dict],
                    slots: Dict[str, int], points_column: str) -> List[dict]:
    """What it takes to play for this team, slot by slot, and who is available.

    The panel leads with this because it reduces a few hundred pool rows to one
    sentence per slot, and because it is the only part of the page that owes
    nothing to any external source -- it is your own optimiser, read back.

    Args:
        roster: The team's rows.
        pool: The free-agent rows.
        slots: From :func:`lineup.slot_counts`.
        points_column: Which projection to optimise.

    Returns:
        list: One dict per slot with ``slot``, ``bar``, ``best``, ``beats``.
    """
    def points(row):
        return float(row.get(points_column) or 0.0)

    rows = []
    for slot, bar in lu.slot_thresholds(roster, slots, points_column).items():
        available = [r for r in pool
                     if slot in lu._eligible(r) and lu.pool_playable(r, points_column)]
        best = max(available, key=points) if available else None
        beats = (best is not None and bar is not None and bar != math.inf
                 and points(best) > bar)
        rows.append({"slot": slot, "bar": bar, "best": best, "beats": beats})
    return rows
