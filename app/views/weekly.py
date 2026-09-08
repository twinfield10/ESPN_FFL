"""Shared furniture for the week-to-week tabs: Roster, Free Agents, Matchup.

All three render the same underlying thing -- rows out of ``lineups.parquet`` with
per-source projected points beside them -- so the labels, the tooltips and the
imputed-source rule live here once. The rule is the reason this module exists: a
source the store marked absent must not appear as a column, because the blend fills
it in from the ESPN/FantasyPros mean and an absent book therefore arrives looking
like unanimous agreement rather than like silence.

**Two renderers, because the tabs ask two different questions.** Roster and Matchup
draw a *lineup*: fixed rows in slot order, spanner headers, a total, and -- on a
matchup -- the same columns mirrored so the two sides meet in the middle. That is
:mod:`lineup_table`, emitted as HTML because Streamlit's grid merges no cells and
stacks no third header row. Free Agents draws a *pool*: hundreds of rows you sort and
search, which is exactly what the grid is for, so it keeps ``st.dataframe`` below.

Logic belongs in :mod:`lineup` and :mod:`lineup_table`, both Streamlit-free and
tested. This is layout.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

from typing import Dict, List, Mapping, Optional, Sequence

import polars as pl
import streamlit as st

import lineup as lu
import lineup_table as ltab

#: Source prefix to what it is. Owned by :mod:`lineup_table`, which both renderers
#: read, so the grid below and the HTML tables cannot disagree about what a column
#: means.
SOURCE_HELP: Dict[str, str] = ltab.SOURCE_HELP

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


def render_lineup(rows: Sequence[dict], info: Sequence[ltab.Col],
                  points: Sequence[ltab.Col], *, label: str,
                  slot_column: str = "slot",
                  total_rows: Optional[Sequence[dict]] = None,
                  marks: Optional[Mapping[object, str]] = None,
                  below: Optional[Sequence[dict]] = None) -> None:
    """Draw one lineup under spanner headers, with a total row.

    Args:
        rows: Already ordered -- see :func:`lineup.sort_by_slot`.
        info: From :func:`lineup_table.info_columns`.
        points: From :func:`lineup_table.points_columns`.
        label: The team, shown across the total row's identity columns.
        slot_column: ``"slot"`` for the optimiser's assignment, ``"slotPosition"``
            for the lineup as ESPN has it set.
        total_rows: The rows the total is over, when that is not all of them.
        marks: ``player_id`` to ``"in"`` or ``"out"``, from
            :func:`lineup.changed_ids`.
        below: Rows to draw after the total row -- the bench.
    """
    st.html(ltab.side_html(rows, info, points, label=label,
                           slot_column=slot_column, total_rows=total_rows,
                           marks=marks, below=below))


def render_matchup(rows: Sequence[ltab.SlotRow], info: Sequence[ltab.Col],
                   points: Sequence[ltab.Col], *, home_label: str,
                   away_label: str) -> None:
    """Draw both lineups as one mirrored table.

    Args:
        rows: From :func:`lineup_table.pair_by_slot`.
        info: From :func:`lineup_table.info_columns`.
        points: From :func:`lineup_table.points_columns`.
        home_label: The left side.
        away_label: The right side.
    """
    st.html(ltab.matchup_html(rows, info, points, home_label=home_label,
                              away_label=away_label))


#: How each upgrade severity announces itself: the callout, and the icon.
#:
#: Critical is an ``st.error`` because it is the one sentence on this page that means
#: *the lineup you are about to play is worse than one you could field today*. Depth
#: is a warning: it is the same comparison one rung down, and nothing about Sunday
#: changes.
UPGRADE_CALLOUTS = {
    lu.UPGRADE_CRITICAL: ("🚨", "starting"),
    lu.UPGRADE_DEPTH: ("↕️", "carrying on the bench"),
}

#: The upgrade tables' columns, sharing the Add/Drop panel's vocabulary.
UPGRADE_CONFIG: Dict[str, object] = {
    "Slot": st.column_config.TextColumn(
        pinned=True,
        help="The starting slot the available player would fill — the slot, not his "
             "position, so a receiver who would fill your flex reads FLEX. This is "
             "where he does the most good; he may be eligible for others."),
    "Add": st.column_config.TextColumn(pinned=True),
    "Pos": st.column_config.TextColumn(),
    "Add Proj": st.column_config.NumberColumn(
        format="%.1f", help="His projection on our blend, in this league's scoring."),
    "Instead Of": st.column_config.TextColumn(
        help="The weakest of your players eligible for that slot who he out-projects "
             "— the one you would actually replace."),
    "Now": st.column_config.TextColumn(
        help="Where ESPN has your player at the moment. Not always the same slot: in "
             "a superflex league a receiver is eligible for `OP` while starting at "
             "`WR`."),
    "Their Proj": st.column_config.NumberColumn(format="%.1f"),
    "Margin": st.column_config.NumberColumn(
        format="%+.1f",
        help="How much better the available player projects. Never below 0.5, "
             "because the sources that make up the blend disagree with each other by "
             "a median of 0.43 points and an edge smaller than that is noise."),
    "Yours Beaten": st.column_config.NumberColumn(
        format="%.0f",
        help="How many of your players eligible for this slot he out-projects. One "
             "is a decision; five is a position you have not addressed."),
    "Better Available": st.column_config.NumberColumn(
        format="%.0f",
        help="How many available players out-project the man in `Instead Of`. A "
             "large number is the story: it means the position is thin on your "
             "roster and deep on the wire."),
}


def _upgrade_frame(upgrades: Sequence[lu.Upgrade]) -> pl.DataFrame:
    """One row per upgrade, in the Add/Drop panel's vocabulary."""
    return pl.DataFrame([{
        "Slot": upgrade.slot,
        "Add": upgrade.best.get("player_name"),
        "Pos": upgrade.best.get("player_position"),
        "Add Proj": upgrade.best.get("TRUE_Points"),
        "Instead Of": upgrade.over.get("player_name"),
        "Now": upgrade.over.get("slotPosition"),
        "Their Proj": upgrade.over.get("TRUE_Points"),
        "Margin": upgrade.margin,
        "Yours Beaten": upgrade.beaten,
        "Better Available": upgrade.better,
    } for upgrade in upgrades])


def render_upgrades(upgrades: Sequence[lu.Upgrade], *, owner: str) -> None:
    """Draw the two waiver flags, or say the roster is clean.

    Args:
        upgrades: From :func:`lineup.upgrades`, over the **unfiltered** pool.
        owner: Whose roster is being flagged.
    """
    if not upgrades:
        st.success(
            f"Nobody available out-projects anyone on {owner}'s roster at a slot "
            f"they could both fill. That is the common answer and the one worth "
            f"trusting.", icon="✅")
        return

    for severity in (lu.UPGRADE_CRITICAL, lu.UPGRADE_DEPTH):
        found = [u for u in upgrades if u.severity == severity]
        if not found:
            continue
        icon, where = UPGRADE_CALLOUTS[severity]
        headline = (f"**{len(found)} slot{'s' if len(found) > 1 else ''}** where an "
                    f"available player out-projects someone {owner} is {where}.")
        (st.error if severity == lu.UPGRADE_CRITICAL else st.warning)(
            headline, icon=icon)
        st.dataframe(_upgrade_frame(found), width="stretch", hide_index=True,
                     placeholder="", lazy=False, column_config=UPGRADE_CONFIG)
