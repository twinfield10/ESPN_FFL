"""Shared furniture for the week-to-week tabs: Roster, Free Agents, Matchup.

All three render the same underlying thing -- rows out of ``lineups.parquet`` with
per-source projected points beside them -- so the labels, the tooltips and the
imputed-source rule live here once. The rule is the reason this module exists: a
source the store marked absent must not appear as a column, because the blend fills
it in from the ESPN/FantasyPros mean and an absent book therefore arrives looking
like unanimous agreement rather than like silence.

Logic belongs in :mod:`lineup`, which is Streamlit-free and tested. This is layout.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

from typing import Dict, List, Optional, Sequence

import polars as pl
import streamlit as st

import lineup as lu

#: Source prefix to how it is labelled on screen, and what to say about it.
SOURCE_HELP: Dict[str, str] = {
    "ESPN": "ESPN's own weekly projection. Present for every player, and the base "
            "every other source is imputed from when it has no opinion.",
    "FP": "FantasyPros' weekly consensus. Real for the players it publishes and "
          "imputed for the rest — check Sources against it.",
    "PINNY": "Pinnacle's weekly player props, converted to points.",
    "BOL": "BetOnline's weekly player props, converted to points.",
    "TRUE": "The blend, in this league's own scoring. One equal vote per source "
            "that has an opinion about this player.",
}

#: How a points column is labelled.
SOURCE_LABELS: Dict[str, str] = {
    "ESPN_Points": "ESPN", "FP_Points": "FP", "PINNY_Points": "Pinnacle",
    "BOL_Points": "BetOnline", "TRUE_Points": "Us",
}

#: Identity and status columns, in reading order.
BASE_LABELS: Dict[str, str] = {
    "slot": "Slot",
    "slotPosition": "Slot",
    "player_name": "Player",
    "player_position": "Pos",
    "primaryPosition": "Pos",
    "pro_team": "NFL",
    "player_active_status": "Status",
    "points": "Actual",
    "sources_real": "Sources",
    "source_spread": "Spread",
}


def missing_sources_note(meta: dict) -> Optional[str]:
    """A sentence naming the weekly sources this league does not have, if any.

    Shown rather than silently omitting the columns, because "there is no Pinnacle
    column" and "Pinnacle agrees exactly with the mean" look identical on a table
    and mean opposite things.

    Args:
        meta: The store's ``meta.json``.

    Returns:
        str | None: The note, or None when every source is present.
    """
    present = meta.get("weekly_sources_present") or {}
    absent = sorted(name for name, ok in present.items() if not ok)
    if not absent:
        return None
    return (
        f"No weekly props this season for **{', '.join(absent)}**, so those columns "
        f"are not shown. They exist in the artifact, but they hold the ESPN/"
        f"FantasyPros mean — showing them would turn an absent source into a "
        f"unanimous one."
    )


def display_columns(frame: pl.DataFrame, meta: dict, *,
                    lead: Sequence[str] = ("slot", "player_name", "player_position",
                                           "pro_team"),
                    tail: Sequence[str] = ("points", "sources_real",
                                           "source_spread")) -> List[str]:
    """Columns to show, in reading order: identity, then sources, then status.

    Args:
        frame: A lineups frame, ideally through :func:`lineup.with_source_spread`.
        meta: The store's ``meta.json``.
        lead: Identity columns to try first.
        tail: Status columns to try last.

    Returns:
        list: Column names present on ``frame``.
    """
    columns = [c for c in lead if c in frame.columns]
    columns += lu.points_columns(frame, meta)
    columns += [c for c in tail if c in frame.columns]
    # `points` is zero for every row until a game is played, and a column of zeros
    # reads as "he scored nothing" rather than "nothing has happened yet".
    if "points" in columns and frame.height and frame["points"].sum() == 0:
        columns.remove("points")
    return columns


def render_table(frame: pl.DataFrame, meta: dict, *, columns: Sequence[str],
                 height: Optional[int] = None) -> None:
    """Draw one weekly table with house labels and tooltips.

    Args:
        frame: The rows to show, already ordered.
        meta: The store's ``meta.json``.
        columns: From :func:`display_columns`.
        height: Optional pixel cap.
    """
    labels = {**BASE_LABELS, **SOURCE_LABELS}
    present = [c for c in columns if c in frame.columns]
    shown = frame.select(present).rename(
        {c: labels[c] for c in present if c in labels})

    config: Dict[str, object] = {
        # 100px rather than auto: auto sizes to the *header*, and "Slot" is four
        # characters while `RB/WR/TE` is eight, so the widest slot in the league
        # rendered as "RB/WR," — truncating the one column the ordering exists for.
        "Slot": st.column_config.TextColumn(
            width=100, pinned=True,
            help="The starting slot this player occupies — the slot, not the "
                 "position, so a receiver in the flex shows as the flex. `BE` is the "
                 "bench and `IR` is injured reserve; neither scores."),
        "Player": st.column_config.TextColumn(pinned=True),
        "Pos": st.column_config.TextColumn(),
        "NFL": st.column_config.TextColumn(),
        "Actual": st.column_config.NumberColumn(
            format="%.1f", help="Points actually scored this week."),
        "Status": st.column_config.TextColumn(
            help="ESPN's own reading: `active`, `bye`, or `inactive`. A player who "
                 "is not active is excluded from the optimal lineup."),
        "Sources": st.column_config.NumberColumn(
            format="%.0f",
            help="How many sources really had an opinion about this player, after "
                 "dropping the ones that were imputed from the mean. `1` means the "
                 "projection beside it is a single source's view."),
        "Spread": st.column_config.NumberColumn(
            format="%.1f",
            help="Standard deviation across those real sources. Blank below two, "
                 "because one number cannot disagree with itself. A wide spread is "
                 "a risk the single blended number hides."),
    }
    for column, label in SOURCE_LABELS.items():
        prefix = column.split("_")[0]
        config[label] = st.column_config.NumberColumn(
            format="%.1f", help=SOURCE_HELP.get(prefix, ""))

    st.dataframe(shown, width="stretch", hide_index=True, column_config=config,
                 placeholder="", lazy=False,
                 **({"height": height} if height else {}))


def render_swaps(changes, points_label: str = "Us") -> None:
    """Draw a start/sit list, or say the lineup is already right.

    Args:
        changes: From :func:`lineup.swaps`.
        points_label: Which projection the gains are in.
    """
    if not changes:
        st.success("This lineup is already the best one available.", icon="✅")
        return

    total = sum(change.gain for change in changes)
    st.warning(f"**{total:+.1f} points** available from "
               f"{len(changes)} change{'s' if len(changes) > 1 else ''}.", icon="↕️")
    for change in changes:
        if change.sit:
            st.markdown(
                f"- **{change.slot}** · start **{change.start}** over "
                f"*{change.sit}* — `{change.gain:+.1f}`")
        else:
            st.markdown(
                f"- **{change.slot}** is empty · start **{change.start}** — "
                f"`{change.gain:+.1f}`")
    st.caption(
        f"Measured on {points_label}, against the lineup ESPN currently has set. "
        f"Players on bye or ruled out are excluded. The gains sum to the difference "
        f"between the two lineups exactly."
    )
