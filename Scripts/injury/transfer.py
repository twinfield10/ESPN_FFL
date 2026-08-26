"""Where an injured starter's work goes on the board, not just in the simulation.

:mod:`Scripts.outcomes.vacancy` fitted the transfer -- what share of a vacated lead's
opportunity reappears on the men behind him -- and plan 28 wired it into the room-level
Monte Carlo, which failed G-D2 and is off by default. It was never applied to a *mean*.
So the board has been doing half of the arithmetic:

* **The absence is priced.** ESPN and FantasyPros dock a known absence themselves --
  ``_apply_injury_adjustment``'s docstring names Ricky Pearsall at 0.0 -- and ``USG_`` is
  scaled by ``games_available / 17`` on top. The starter loses the games.
* **Nobody gains them.** No mechanism credits the room, so the work leaves the roster.

Measured on the 2026 board: Jeremiyah Love misses five weeks at ADP 22.9, and his direct
backup Tyler Allgeier sits at the **37th percentile of ESPN points among depth-rank-2
running backs** -- below the median, behind a lead back who will miss five games. The
transfer is not priced by the market either.

**Two position groups, and the third is the finding.** A backfield is near zero-sum: the
lead's vacated opportunity is 81% recovered inside the room. A receiver room is not --
45% of a lead receiver's targets reappear, his understudy gains 0.59 of 7.72, and the
offence simply throws 1.25 fewer times. So WR gets **no transfer at all**: a rule handing
a WR1's targets to the WR2 would invent 2.8 targets a game. That asymmetry is why this
cannot be one rule applied uniformly.

**What is transferred is volume, at the lead's rate.** The fitted share is a share of
*opportunity*, and the board carries stat lines rather than carries and targets, so a
beneficiary inherits the vacated line as though he converted it like the man he replaced.
That is an approximation and it is the one most likely to be wrong -- a backup is usually
the less efficient player -- so it is stated here rather than buried, and the gate below
is what decides whether it survives.

Ordering, which :mod:`Scripts.injury.apply` wrote down before anything needed it: this
attaches **after** ``reconcile_team_totals`` and **before** ``proj_to_score``. Earlier and
the team midpoint drags the transfer back out across the roster; later and no league's
scoring rules ever see it.
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple

import pandas as pd

#: Games a season offers.
SLATE: float = 17.0

#: Stats a vacancy moves. Volume the replacement actually inherits.
#:
#: Passing is absent on purpose. A backup quarterback's room is handled by plan 31
#: phase 2, which allocates a team's seventeen starts by role rather than transferring
#: a line, and doing both would count the same vacancy twice.
TRANSFER_STATS: Tuple[str, ...] = (
    "rushingYards", "rushingTouchdowns",
    "receivingYards", "receivingReceptions", "receivingTouchdowns",
)

#: Prefix for the pre-adjustment ``USG_`` line, stashed by
#: :func:`Scripts.season_projections._apply_injury_adjustment`.
HEALTHY_PREFIX: str = "usg_healthy_"

#: Positions whose rooms redistribute. See the module docstring for why WR does not.
ROOM_POSITIONS: Tuple[str, ...] = ("RB", "TE")

#: Applied shares, from :mod:`Scripts.outcomes.vacancy`'s fit.
#:
#: ``(rank 2, ranks 3 and below)``. Published in the form a depth chart can express,
#: because ``load_depth_charts`` clips rank at three and the fit is on the realised
#: season order. Ranks below two split their share in proportion to their own baseline,
#: which is self-normalising and indifferent to how many bodies a room has.
SHARES: Dict[str, Tuple[float, float]] = {
    "RB": (0.410, 0.404),
    "TE": (0.263, 0.208),
}

#: Column recording what a player inherited, for the board to show and a gate to read.
INHERITED_COLUMN = "inj_vacancy_inherited"


def applied_shares() -> Dict[str, Tuple[float, float]]:
    """The shares to apply, from the persisted fit where there is one.

    :data:`SHARES` is the fallback rather than the source of truth: the rule is fitted
    by ``python -m Scripts.outcomes.vacancy --write`` and re-fitting it must not require
    editing a constant here. A position the fit does not apply a rule to is **absent**
    from the result, so a caller iterating it cannot accidentally hand a receiver room
    a transfer.

    Returns:
        dict: Position to ``(rank 2 share, ranks 3+ share)``.
    """
    try:
        from Scripts.outcomes import vacancy as vac
        rule = vac.applied_rule(vac.load())
    except Exception:                                  # pragma: no cover - fit absent
        return dict(SHARES)
    out = {position: (float(block["rank_2"]), float(block["rank_rest"]))
           for position, block in rule.items() if position in ROOM_POSITIONS}
    return out or dict(SHARES)


def redistribute(base: pd.DataFrame, slate: float = SLATE,
                 shares: Optional[Dict[str, Tuple[float, float]]] = None) -> pd.DataFrame:
    """Move a vacated starter's volume onto the men behind him.

    Args:
        base: Board frame, **after** ``reconcile_team_totals``, carrying
            ``pro_team``, ``primaryPosition``, ``usg_depth_rank``,
            ``inj_expected_absence_weeks`` and the ``TRUE_`` stat block.
        slate: Games a season offers.
        shares: Override the applied shares, for a gate that needs to vary them.

    Returns:
        pd.DataFrame: ``base`` with ``TRUE_`` volume moved and
        :data:`INHERITED_COLUMN` attached. Returned unchanged when the frame lacks
        what the transfer needs -- the same contract every other attacher here has.
    """
    required = {"pro_team", "primaryPosition", "usg_depth_rank",
                "inj_expected_absence_weeks"}
    if not required.issubset(base.columns):
        return base

    shares = applied_shares() if shares is None else shares
    weeks = pd.to_numeric(base["inj_expected_absence_weeks"], errors="coerce")
    rank = pd.to_numeric(base["usg_depth_rank"], errors="coerce")
    base[INHERITED_COLUMN] = 0.0

    rooms = 0
    for (team, position), room in base.groupby(["pro_team", "primaryPosition"]):
        if position not in ROOM_POSITIONS or not isinstance(team, str):
            continue
        if position not in shares:
            continue
        share_two, share_rest = shares[position]

        leads = room.index[(rank.loc[room.index] == 1) & (weeks.loc[room.index] > 0)]
        if not len(leads):
            continue

        second = room.index[rank.loc[room.index] == 2]
        rest = room.index[rank.loc[room.index] >= 3]
        if not len(second) and not len(rest):
            continue

        for lead in leads:
            out = min(float(weeks.loc[lead]), slate)
            if out <= 0:
                continue
            for stat in TRANSFER_STATS:
                healthy_col, true_col = f"{HEALTHY_PREFIX}{stat}", f"TRUE_{stat}"
                if true_col not in base.columns:
                    continue
                healthy = _healthy_line(base, lead, healthy_col, true_col, out, slate)
                if healthy is None or healthy <= 0:
                    continue
                vacated = healthy * (out / slate)

                if len(second):
                    per = share_two * vacated / len(second)
                    base.loc[second, true_col] = (
                        pd.to_numeric(base.loc[second, true_col], errors="coerce")
                        .fillna(0.0) + per)
                    base.loc[second, INHERITED_COLUMN] += per

                if len(rest):
                    baseline = pd.to_numeric(
                        base.loc[rest, true_col], errors="coerce").fillna(0.0).clip(lower=0)
                    weights = (baseline / baseline.sum() if baseline.sum() > 0
                               else pd.Series(1.0 / len(rest), index=rest))
                    add = share_rest * vacated * weights
                    base.loc[rest, true_col] = (
                        pd.to_numeric(base.loc[rest, true_col], errors="coerce")
                        .fillna(0.0) + add)
                    base.loc[rest, INHERITED_COLUMN] += add
            rooms += 1

    if rooms:
        gained = int((base[INHERITED_COLUMN] > 0).sum())
        print(f"  Vacancy: {rooms} vacated starter(s) redistributed onto {gained} "
              f"teammate(s); at most {max(s[0] + s[1] for s in shares.values()):.0%} "
              f"of a vacated line reappears, the rest leaves the offence.")
    return base


def _healthy_line(base: pd.DataFrame, lead, healthy_col: str, true_col: str,
                  out: float, slate: float) -> Optional[float]:
    """The starter's line as though he played the whole season.

    Args:
        base: Board frame.
        lead: Index label of the vacating starter.
        healthy_col: Stashed pre-adjustment ``USG_`` column.
        true_col: Blended column.
        out: Weeks he is expected to miss.
        slate: Games a season offers.

    Returns:
        float: The full-slate line, or None when neither source can supply one --
        which is an abstention, not a zero.
    """
    if healthy_col in base.columns:
        healthy = pd.to_numeric(base.loc[lead, healthy_col], errors="coerce")
        if pd.notna(healthy) and healthy > 0:
            return float(healthy)
    # `TRUE_` is already docked, so grossing it back up recovers the healthy line --
    # except at a full-season absence, where it is zero and there is nothing to gross.
    played = slate - out
    if played <= 0:
        return None
    blended = pd.to_numeric(base.loc[lead, true_col], errors="coerce")
    if pd.isna(blended) or blended <= 0:
        return None
    return float(blended) * slate / played
