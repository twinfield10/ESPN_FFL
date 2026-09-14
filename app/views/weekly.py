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
from Scripts import live

#: Source prefix to what it is. Owned by :mod:`lineup_table`, which both renderers
#: read, so the grid below and the HTML tables cannot disagree about what a column
#: means.
SOURCE_HELP: Dict[str, str] = ltab.SOURCE_HELP

#: How a points column is labelled.
SOURCE_LABELS: Dict[str, str] = {
    "ESPN_Points": "ESPN", "FP_Points": "FP", "PINNY_Points": "Pinnacle",
    "BOL_Points": "BetOnline", "ATH_Points": "The Athletic", "TRUE_Points": "Us",
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
    live.LIVE_POINTS: "Live",
    "sources_real": "Sources",
    "source_spread": "Spread",
    "percent_owned": "Owned %",
}

#: Tooltips for the live block. The grid needs its own copy because
#: :func:`render_table` builds ``column_config`` from labels rather than from
#: :class:`lineup_table.Col` specs.
LIVE_HELP: Dict[str, str] = {
    "Live": ltab.LIVE_HELP,
    "Actual": ltab.ACTUAL_HELP,
}

#: Labels rendered bold, and the one rendered italic.
#:
#: Same split as :data:`lineup_table.EMPHASIS`, and it has to be duplicated because
#: this renderer is a Streamlit dataframe rather than hand-emitted HTML -- the grid is
#: canvas-drawn, so no stylesheet reaches its cells and the emphasis has to travel as
#: a pandas ``Styler``. Keep the two in step: a reader moving between the Roster table
#: and this grid should not have to work out which number each one is built around.
BOLD_LABELS = ("Live", "Us")
ITALIC_LABELS = ("Δ",)

#: The painted columns, as ``(frame column, lineup_table label, grid label)``.
#:
#: Three names for two columns, because the grid and the HTML tables head them
#: differently -- ``TRUE_Points`` is ``TRUE`` on a lineup table and ``Us`` here. The
#: middle name is what :func:`lineup_table.cell_fill` checks against
#: :data:`lineup_table.PAINTED_LABELS`, so the two renderers cannot come to disagree
#: about which cells are painted; only about what the header says.
#:
#: The same two columns :data:`BOLD_LABELS` emboldens, and deliberately so: the fill
#: says whether the number is any good, and the only numbers worth asking that about
#: are the ones the table is read for.
PAINTED: tuple = (
    (live.LIVE_POINTS, "LIVE", "Live"),
    (f"{lu.BLEND}_Points", "TRUE", "Us"),
)


def missing_sources_note(meta: dict) -> Optional[str]:
    """A sentence naming the weekly sources this league does not have, if any.

    Shown rather than silently omitting the columns, because "there is no Pinnacle
    column" and "Pinnacle agrees exactly with the mean" look identical on a table
    and mean opposite things.

    Says "projections" rather than "props" since 2026-09-09: The Athletic is one of
    these sources now and has never had a prop in its life.

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
        f"No weekly projections this season for **{', '.join(absent)}**, so those "
        f"columns "
        f"are not shown. They exist in the artifact, but they hold the ESPN/"
        f"FantasyPros mean — showing them would turn an absent source into a "
        f"unanimous one."
    )


def display_columns(frame: pl.DataFrame, meta: dict, *,
                    lead: Sequence[str] = ("slot", "player_name", "player_position",
                                           "pro_team"),
                    tail: Sequence[str] = ("points", "sources_real")) -> List[str]:
    """Columns to show, in reading order: identity, then sources, then status.

    The live block leads the numbers when the frame carries it, because it is the
    number the table is read for. ``Spread`` is no longer in the default ``tail`` --
    see :func:`lineup_table.points_columns`, which dropped it from the lineup tables
    for the same reason: at this width the disagreement *between* sources was
    competing with the number the grid exists for. ``Actual`` is shown **once any game
    in the frame has started**, which is a different rule from the one it replaces: that test was
    ``frame["points"].sum() == 0``, which is a proxy for "nothing has been played"
    that stops being true the instant one player scores -- so the column appeared
    mid-week and, worse, would have stayed hidden through a week in which everybody
    genuinely scored zero. Game state says it directly.

    Args:
        frame: A lineups frame, ideally through :func:`lineup.with_source_spread`.
        meta: The store's ``meta.json``.
        lead: Identity columns to try first.
        tail: Status columns to try last.

    Returns:
        list: Column names present on ``frame``.
    """
    columns = [c for c in lead if c in frame.columns]
    if live.LIVE_POINTS in frame.columns:
        columns.append(live.LIVE_POINTS)
    columns += lu.points_columns(frame, meta)
    columns += [c for c in tail if c in frame.columns]
    if "points" in columns and not _anything_played(frame):
        columns.remove("points")
    return columns


def _anything_played(frame: pl.DataFrame) -> bool:
    """Whether any row in the frame belongs to a game that has started.

    Args:
        frame: A lineups frame.

    Returns:
        bool: Falls back to the old sum-of-points proxy for a store written before
        live scoring, so an older season still hides the column the way it used to.
    """
    if not frame.height:
        return False
    if live.LOCKED_COLUMN in frame.columns:
        return bool(frame[live.LOCKED_COLUMN].fill_null(False).any())
    return "points" in frame.columns and bool(frame["points"].sum() != 0)


def render_table(frame: pl.DataFrame, meta: dict, *, columns: Sequence[str],
                 height: Optional[int] = None,
                 scales: Optional[Mapping[str, ltab.PointsScale]] = None) -> None:
    """Draw one weekly table with house labels and tooltips.

    Args:
        frame: The rows to show, already ordered.
        meta: The store's ``meta.json``.
        columns: From :func:`display_columns`.
        height: Optional pixel cap.
        scales: From :func:`lineup_table.points_scales`, computed on the
            **unfiltered** league-week rather than on ``frame`` -- a colour that
            moves when you filter the pool is not encoding the number.
    """
    labels = {**BASE_LABELS, **SOURCE_LABELS}
    present = [c for c in columns if c in frame.columns]
    # Off ``frame`` rather than ``shown``, because the fill rule reads columns the
    # table does not show: the player's own position, and ``game_state`` -- a bye is
    # a scored 0.0 that means "no game", and painting it deepest red would be the
    # table asserting a bad week nobody had.
    fills = _fills(frame, scales)
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
        "Live": st.column_config.NumberColumn(
            format="%.1f", help=LIVE_HELP["Live"]),
        "Actual": st.column_config.NumberColumn(
            format="%.1f", help=LIVE_HELP["Actual"]),
        "Status": st.column_config.TextColumn(
            help="ESPN's own reading: `active`, `bye`, or `inactive`. A player who "
                 "is not active is excluded from the optimal lineup."),
        "Sources": st.column_config.NumberColumn(
            format="%.0f",
            help="How many sources really had an opinion about this player, after "
                 "dropping the ones that were imputed from the mean. `1` means the "
                 "projection beside it is a single source's view."),
        # Not in the default `tail` -- the Free Agents pool asks for it by name,
        # because "is he gettable" is a question only the wire has.
        "Owned %": st.column_config.NumberColumn(
            format="%.0f%%",
            help="Share of ESPN leagues rostering this player, from the draft "
                 "board. High ownership on an unrostered player means this league "
                 "is shallower than most, not that he is available everywhere."),
        # Kept though `display_columns` no longer emits it: `tail` is a parameter,
        # so a caller can still ask for the column and it should arrive configured.
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

    st.dataframe(_emphasised(shown, fills), width="stretch", hide_index=True,
                 column_config=config, placeholder="", lazy=False,
                 **({"height": height} if height else {}))


def _numeric_format(label: str) -> str:
    """The pandas format for one column, matching its ``column_config``.

    The two have to agree because a ``Styler`` sends its own *display values* to the
    frontend alongside the styles. Left to pandas' default they would be raw reprs --
    ``12.339999999999999`` beside a column claiming ``%.1f``.

    Args:
        label: The rendered column label.

    Returns:
        str: A ``str.format`` template.
    """
    if label in ITALIC_LABELS:
        # A difference states its direction, the same rule `lineup_table.FORMATS`
        # follows: `2.4` is ambiguous about which way it points.
        return "{:+.1f}"
    if label == "Sources":
        return "{:.0f}"
    if label == "Owned %":
        # Must match the `%.0f%%` in `column_config`, or the Styler's display
        # values and the grid's format disagree about the same cell.
        return "{:.0f}%"
    return "{:.1f}"


def _fills(frame: pl.DataFrame,
           scales: Optional[Mapping[str, ltab.PointsScale]]
           ) -> List[Dict[str, str]]:
    """One dict of ``grid label -> CSS colour`` per row, in ``frame``'s own order.

    Computed here rather than inside the ``Styler`` because the rule needs columns
    the grid does not display, and because going through
    :func:`lineup_table.cell_fill` is what keeps this grid and the HTML tables
    agreeing about which cells are painted and on which ruler.

    Args:
        frame: The rows to show, before renaming.
        scales: From :func:`lineup_table.points_scales`, or None to paint nothing.

    Returns:
        list: One dict per row. Empty when there is nothing to paint, which the
        caller reads as "no fills" without having to special-case it.
    """
    if not scales:
        return []
    painted = [(source, label, shown) for source, label, shown in PAINTED
               if source in frame.columns]
    if not painted:
        return []
    wanted = {c for c in ltab.SCALE_INPUTS if c in frame.columns}
    wanted.update(source for source, _, _ in painted)
    wanted.add(live.STATE_COLUMN)
    rows = frame.select(sorted(wanted & set(frame.columns))).to_dicts()
    out = []
    for row in rows:
        got = {}
        for source, label, shown in painted:
            fill = ltab.cell_fill(row, ltab.Col(source, label, "points", ""), scales)
            if fill:
                got[shown] = f"background-color: {fill}"
        out.append(got)
    return out


def _emphasised(shown: pl.DataFrame,
                fills: Optional[Sequence[Mapping[str, str]]] = None):
    """The frame as a pandas ``Styler``, with the reading columns emphasised.

    A Streamlit dataframe is drawn on a canvas, so no stylesheet reaches its cells and
    ``column_config`` has no weight or slant option. A ``Styler`` is the one route
    that works: Streamlit marshals its computed CSS declarations through to the
    frontend verbatim.

    Falls back to the plain frame if anything here fails. A table that renders
    unemphasised is a cosmetic loss; one that raises takes the page with it, and this
    is the last thing between the data and the screen.

    Args:
        shown: The renamed, ordered frame.
        fills: From :func:`_fills`, one dict per row and aligned to ``shown``'s row
            order. Applied row-wise rather than per-cell because a row's colour
            depends on the player's *own* position, which ``Styler.map`` cannot see.

    Returns:
        A pandas ``Styler``, or ``shown`` unchanged on failure.
    """
    try:
        pandas_frame = shown.to_pandas()
        numeric = [c for c in pandas_frame.columns
                   if pandas_frame[c].dtype.kind in "if"]
        styler = pandas_frame.style.format(
            {c: _numeric_format(str(c)) for c in numeric}, na_rep="")
        bold = [c for c in pandas_frame.columns if c in BOLD_LABELS]
        italic = [c for c in pandas_frame.columns if c in ITALIC_LABELS]
        if bold:
            styler = styler.set_properties(subset=bold, **{"font-weight": "700"})
        if italic:
            styler = styler.set_properties(subset=italic, **{"font-style": "italic"})
        if fills:
            held = list(fills)

            def paint(row):
                # ``to_pandas`` always hands back a fresh 0..n-1 index, so the row
                # label *is* its position in `held`. Guarded anyway: a mismatch would
                # colour the wrong player, which is worse than colouring nobody.
                got = held[row.name] if isinstance(row.name, int) and \
                    0 <= row.name < len(held) else {}
                return [got.get(str(column), "") for column in row.index]

            styler = styler.apply(paint, axis=1)
        return styler
    except Exception:                    # noqa: BLE001 - cosmetic, never fatal
        return shown


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
                  below: Optional[Sequence[dict]] = None,
                  scales: Optional[Mapping[str, ltab.PointsScale]] = None) -> None:
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
        scales: From :func:`lineup_table.points_scales`, on the unfiltered
            league-week.
    """
    st.html(ltab.side_html(rows, info, points, label=label,
                           slot_column=slot_column, total_rows=total_rows,
                           marks=marks, below=below, scales=scales))


def render_matchup(rows: Sequence[ltab.SlotRow], info: Sequence[ltab.Col],
                   points: Sequence[ltab.Col], *, home_label: str,
                   away_label: str,
                   scales: Optional[Mapping[str, ltab.PointsScale]] = None) -> None:
    """Draw both lineups as one mirrored table.

    Args:
        rows: From :func:`lineup_table.pair_by_slot`.
        info: From :func:`lineup_table.info_columns`.
        points: From :func:`lineup_table.points_columns`.
        home_label: The left side.
        away_label: The right side.
        scales: From :func:`lineup_table.points_scales`, on the unfiltered
            league-week. One set for both halves, so a receiver reads the same colour
            whichever side of the table he is on.
    """
    st.html(ltab.matchup_html(rows, info, points, home_label=home_label,
                              away_label=away_label, scales=scales))


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
