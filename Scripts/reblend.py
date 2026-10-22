"""The intraday re-blend: this week's projections rebuilt, every other week left alone.

The 06:00 nightly is the only run that moves a projection column, so Friday's injury
reports, Sunday's inactives and the line moves after them reach the store the next
morning, after the games. ``--what live`` patches actuals and rosters every ten
minutes on a game day and deliberately leaves every projection alone. This is the
other half: ``Scripts.equivalence.build_week_frame`` re-reads the engine-fed sources
and ESPN's current week, and the result replaces the stored frame's current week.

**Why one week is enough.** On 2026-10-22 a current-week build matched the full
rebuild's current-week rows in all nine leagues with no differing cell: no
current-week projection reads an earlier week's rows. And ESPN is 93% of a full
rebuild, most of it re-reading weeks already played.

**What it must not do:**

* Move a started game's projection. :func:`Scripts.kickoff_freeze.apply` runs on the
  fresh week against the stored one, exactly as the full rebuild does.
* Touch another week. :func:`splice` keeps every other week's rows as stored.
* Mint an S3 version for nothing. The bucket keeps every PUT for 90 days and already
  held 2.95 GB of old ``lineups.parquet`` versions on 2026-10-22, so
  :func:`material_change` decides whether a league is written at all.

See ``docs/plans/56-intraday-reblend.md``.
"""

from typing import Dict, Tuple

import pandas as pd

from Scripts.live import LOCKED_COLUMN

#: A pre-game player whose ``TRUE_Points`` moved by at least this much is a change
#: worth publishing. A re-blend nearly always moves some decimals, because the market
#: moves by the minute, and below a tenth of a point no start/sit or waiver call on the
#: app changes.
MATERIAL_POINTS = 0.1

#: Identity of a row, as :mod:`Scripts.kickoff_freeze` keys it.
KEYS = ["week", "player_id"]

#: Roster and availability facts: any change to one is published whatever the points
#: did. A player moved to the bench or ruled out is the change a manager needs to see,
#: even when the projection had already priced it.
ROSTER_COLUMNS = ("team_owner", "slotPosition", "player_active_status", "injury_status")


def splice(stored: pd.DataFrame, week_frame: pd.DataFrame, week: int) -> pd.DataFrame:
    """The stored frame with ``week`` replaced by ``week_frame``.

    Args:
        stored: ``lineups.parquet`` as stored.
        week_frame: :func:`Scripts.equivalence.build_week_frame` output, frozen.
        week: The week ``week_frame`` holds.

    Returns:
        pd.DataFrame: Every other week's rows exactly as stored and in stored order,
        then ``week``'s new rows. Not re-sorted: a full rebuild writes its rows in two
        passes over the weeks, so a sort by week would reorder every other week. Only
        where the current week sits changes; its own row order is the one-week
        build's, which is the full rebuild's order for that week.

    Raises:
        ValueError: When ``week_frame`` holds any week but ``week``. Splicing a frame
            that carried other weeks would duplicate them.
    """
    held = set(pd.to_numeric(week_frame["week"]).dropna().astype(int))
    if held != {int(week)}:
        raise ValueError(f"the re-blended frame holds weeks {sorted(held)}, not only "
                         f"{week}; splicing it would duplicate rows")
    keep = stored[pd.to_numeric(stored["week"]).astype("Int64") != int(week)]
    return pd.concat([keep, week_frame], ignore_index=True)


def material_change(stored_week: pd.DataFrame, fresh_week: pd.DataFrame,
                    threshold: float = MATERIAL_POINTS) -> Tuple[bool, Dict[str, object]]:
    """Whether the re-blended week differs from the stored one enough to publish.

    Args:
        stored_week: The stored frame's rows for the week.
        fresh_week: The re-blended rows for the same week, after the freeze.
        threshold: Smallest ``TRUE_Points`` move on a pre-game player that counts.

    Returns:
        tuple: ``(material, counts)``. ``counts`` carries ``added`` and ``removed``
        (players in one frame and not the other), ``roster`` (rows whose roster or
        availability facts changed), ``moved`` (pre-game players past the threshold)
        and ``max_move``, for the log line.
    """
    def keyed(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=KEYS, keep="first").set_index(KEYS)

    old, new = keyed(stored_week), keyed(fresh_week)
    both = old.index.intersection(new.index)
    counts: Dict[str, object] = {
        "added": int((~new.index.isin(old.index)).sum()),
        "removed": int((~old.index.isin(new.index)).sum()),
    }

    roster = [c for c in ROSTER_COLUMNS if c in old.columns and c in new.columns]
    changed = pd.Series(False, index=both)
    for column in roster:
        changed |= (old.loc[both, column].astype(str) != new.loc[both, column].astype(str))
    counts["roster"] = int(changed.sum())

    pre = ~new.loc[both, LOCKED_COLUMN].fillna(False).astype(bool) \
        if LOCKED_COLUMN in new.columns else pd.Series(True, index=both)
    move = (pd.to_numeric(new.loc[both, "TRUE_Points"], errors="coerce")
            - pd.to_numeric(old.loc[both, "TRUE_Points"], errors="coerce")).abs()
    move = move[pre.to_numpy()]
    # Less a hair, so 10.1 - 10.0 (0.0999... in floating point) counts as a tenth.
    counts["moved"] = int((move >= threshold - 1e-9).sum())
    counts["max_move"] = round(float(move.max()), 3) if move.notna().any() else 0.0

    material = bool(counts["added"] or counts["removed"] or counts["roster"]
                    or counts["moved"])
    return material, counts


def summary(counts: Dict[str, object]) -> str:
    """One log line for :func:`material_change`'s counts."""
    return (f"{counts['moved']} pre-game player(s) moved ≥ {MATERIAL_POINTS} "
            f"(max {counts['max_move']}), {counts['roster']} roster change(s), "
            f"{counts['added']} added, {counts['removed']} removed")
