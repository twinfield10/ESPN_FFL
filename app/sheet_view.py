"""The Sheet — four position panels on one screen, and the state behind them.

A second view of the same ``board.parquet`` the Draft Board page reads, organised the
way ``DraftSheets_2026.xlsx`` (the BeerSheets replacement) organises a board: one
narrow panel per position, side by side, banded by tier, with a cross-off column and a
live positional-scarcity read. See ``docs/plans/37-draft-sheet.md``.

**Why a second view rather than a mode on the first.** The Draft Board page is 45
columns across eight spanner groups, and that is the right shape for the hour before a
draft — it is where you decide whether you believe the numbers. It is the wrong shape
for the ninety seconds you have on the clock, where the question is "who, at which
position, and is there anything left behind him". Those are different tables, not one
table with a filter.

The engine is entirely the existing one. Replacement level, VOR, tiers and dollars come
from ``Scripts.draft.board`` and ``draft_view``; this module chooses what to show and
what a click means. The two things the DraftSheet had that this repo did not --
positional scarcity per row, and an availability discount applied to the whole blend --
live in :mod:`draft_view` beside their siblings, not here.

Layout only, and testable without a Streamlit runtime: nothing in this module imports
``streamlit`` or reads a file.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

from typing import Dict, List, Mapping, NamedTuple, Optional, Sequence

import pandas as pd
import polars as pl

import draft_view as dv

#: The four panels, in reading order. Deliberately the same four the DraftSheet prints
#: and in the same order: it is the order a draft board is scanned in, and QB-first
#: holds even in the leagues where quarterbacks go last.
SHEET_POSITIONS: tuple = ("QB", "RB", "WR", "TE")

#: Offered below the four, collapsed. The DraftSheet omits kickers and defences
#: entirely, and it is right to: ``Scripts.draft.board.STREAMED_POSITIONS`` records why
#: a season-total VOR does not describe a position you stream, and ``value`` is NaN for
#: them for that reason. But our leagues do start them, and "which defence, eventually"
#: is still a question — so they get a panel that is out of the way rather than no
#: panel.
SHEET_STREAMED: tuple = ("K", "D/ST")

#: Session-state prefix for the set of players crossed off. Per league, for the reason
#: :func:`draft_view.budget_key` is per league: a shared key carries a half-finished
#: Knights draft onto GOP's board.
DRAFTED_KEY = "sheet_drafted"

#: Session-state prefix for one panel's button-column click.
SHEET_CLICK_KEY = "sheet_click"

#: What the cross-off button says. The cell value *is* the button label — see
#: ``_column_config`` in the board page — so these are the control, not a legend.
MARK_AVAILABLE = "✕"
MARK_DRAFTED = "↺"

#: Header for the cross-off column. A single character, because the button is the
#: column and any word above it would be wider than the control.
MARK_COLUMN = "·"

#: How deep each panel runs, as a multiple of the position's replacement rank.
#:
#: The DraftSheet prints QB40 / RB80 / WR80 / TE37 against baselines of 14 / 35 / 42 /
#: 17 — so between 2.2x and 1.9x, which is one rule rather than four numbers. Twice
#: replacement is the last player who could plausibly start for somebody, and a panel
#: that runs past him is scrolling rather than reading.
PANEL_DEPTH_MULTIPLE = 2.0

#: Bounds on panel depth. The floor matters in a six-team league, where twice
#: replacement at tight end is eight rows and the panel stops being a board; the cap
#: matters at receiver in a sixteen-team league, where it is 130.
MIN_PANEL_ROWS, MAX_PANEL_ROWS = 12, 80

#: Row banding and the crossed-off fill, per theme.
#:
#: The DraftSheet bands on ``ISEVEN($B5)`` — the *tier* number, not the row number — so
#: the bands are the tiers and the eye gets the cliff for free. Kept exactly, because it
#: is the sheet's best formatting decision and it costs nothing.
#:
#: ``struck`` deliberately keeps its text legible instead of blacking the row out as the
#: workbook does. A drafted player is still information — he is who went, and at what —
#: and on draft night you re-read the rows above the one you just lost.
SHEET_FILLS: Dict[str, Dict[str, str]] = {
    "light": {"band": "#f0efe9", "struck": "#d9d8d2", "struck_ink": "#6b6a66"},
    "dark": {"band": "#1b1b1a", "struck": "#2c2c2a", "struck_ink": "#7c7b77"},
}


class Panel(NamedTuple):
    """One position's panel: what to draw, and what a click on it means.

    Attributes:
        position: The position this panel holds.
        table: The rows behind ``frame``, in the same order, carrying ``player_id``. A
            click reports a row number and a row number is only meaningful against the
            frame it was drawn from — see :func:`toggle_drafted`.
        frame: Display-ready, seven columns plus the mark.
        drafted: Per row, whether that player is crossed off. Parallel to ``frame``.
        depth: Rows shown.
        replacement: This league's replacement rank at the position, or None where the
            league does not start it.
        remaining: Available players still worth more than replacement, across the
            whole position rather than just the rows shown.
        cash: Whether ``VALUE`` is dollars. Recorded rather than re-derived, so the
            formatter does not have to guess a league's draft type from its columns.
    """

    position: str
    table: pl.DataFrame
    frame: pd.DataFrame
    drafted: List[bool]
    depth: int
    replacement: Optional[int]
    remaining: int
    cash: bool


# =========================================================================
# Who is off the board
# =========================================================================


def drafted_key(league_key: str) -> str:
    """The session-state key holding the crossed-off set, for one league.

    Args:
        league_key: ``config.yaml`` league key.

    Returns:
        str: The key.
    """
    return f"{DRAFTED_KEY}::{league_key}"


def click_key(position: str) -> str:
    """The session-state key one panel's button column reports clicks on.

    One per panel rather than one per page: four tables sharing a key would each read
    the others' clicks, and a click on the receiver panel would cross off a
    quarterback.

    Args:
        position: The panel's position.

    Returns:
        str: The key.
    """
    return f"{SHEET_CLICK_KEY}::{position}"


def drafted_set(state: Mapping, league_key: str) -> set:
    """Who has been crossed off in this league, as a set of player ids.

    Args:
        state: ``st.session_state``, or any mapping — this module keeps its
            no-Streamlit rule so the whole draft-state machine is testable with a dict.
        league_key: ``config.yaml`` league key.

    Returns:
        set: Player ids, empty before anything is crossed off.
    """
    return set(state.get(drafted_key(league_key)) or ())


def toggle_drafted(state, table: pl.DataFrame, league_key: str,
                   position: str) -> set:
    """Cross a clicked player off, or put him back, and remember it.

    **A click reports a row number, which goes stale immediately.** It is a position in
    the panel as currently sorted and filtered, so resolving it to a ``player_id`` at
    click time is what keeps it pointing at the player you clicked rather than at
    whoever moves into that row once the panel re-renders — which, since crossing
    somebody off changes what the panel shows, is every single time. Same reasoning and
    same shape as :func:`draft_view.remember_note_click`.

    Clicking a crossed-off player restores him. A misclick on draft night needs to cost
    one click, not a page reload.

    Args:
        state: ``st.session_state`` or a plain dict.
        table: The frame the panel was rendered from, in the same order.
        league_key: ``config.yaml`` league key.
        position: The panel's position, for :func:`click_key`.

    Returns:
        set: The updated set of crossed-off player ids. Also written back to ``state``.
    """
    held = drafted_key(league_key)
    click = state.get(click_key(position))

    row = None
    if click is not None:
        row = click.get("row") if hasattr(click, "get") else getattr(click, "row", None)

    gone = drafted_set(state, league_key)
    if row is not None and 0 <= row < table.height and "player_id" in table.columns:
        clicked = table.row(row, named=True).get("player_id")
        if clicked is not None:
            gone.discard(clicked) if clicked in gone else gone.add(clicked)

    state[held] = gone
    return gone


#: Session-state prefix for whether the store's rostered players are currently folded
#: into the crossed-off set. Remembered so the toggle can be applied on the *flip*
#: rather than continuously -- see :func:`apply_rostered`.
ROSTERED_SEEDED_KEY = "sheet_rostered_seeded"


def rostered_ids(board: pl.DataFrame) -> set:
    """Players the store says somebody already holds.

    ``on_team_id`` is 0 for a free agent and the fantasy team's id once someone holds
    them. **Whether that means "unavailable" is not this function's question** -- in a
    keeper league before declarations it is last season's roster, which is what
    :func:`draft_view.keepers_pending` exists to detect and what the page checks before
    calling this.

    Args:
        board: A stored draft board.

    Returns:
        set: Player ids, empty when the board carries no roster column.
    """
    if "on_team_id" not in board.columns or "player_id" not in board.columns:
        return set()
    held = board.filter(pl.col("on_team_id").fill_null(0) != 0)
    return set(held["player_id"].to_list())


def apply_rostered(state, league_key: str, held: set, show: bool) -> set:
    """Fold the store's rostered players in or out of the crossed-off set, on the flip.

    **Applied when the toggle changes rather than continuously, and the difference is
    the whole design.** A continuous union would leave a store-held row struck no matter
    what you clicked, so the cross-off button would silently do nothing on exactly the
    rows a stale ESPN roster makes you want it. Folding on the flip keeps one set, so
    every row behaves the same way: click to cross off, click to restore, and an
    individual restore survives until you flip the toggle again.

    Turning it off removes only the players it added. Anyone you crossed off by hand
    stays crossed off, which is what you want when the toggle was a convenience rather
    than the point.

    Args:
        state: ``st.session_state`` or a plain dict.
        league_key: ``config.yaml`` league key.
        held: From :func:`rostered_ids`.
        show: Whether the toggle is currently on.

    Returns:
        set: The crossed-off set to use this run. Written back to ``state`` only when
        the toggle actually flipped.
    """
    seeded_key = f"{ROSTERED_SEEDED_KEY}::{league_key}"
    was = bool(state.get(seeded_key))
    drafted = drafted_set(state, league_key)

    if show == was:
        return drafted

    drafted = (drafted | held) if show else (drafted - held)
    state[drafted_key(league_key)] = drafted
    state[seeded_key] = show
    return drafted


def forget_rostered(state, league_key: str) -> None:
    """Forget that the store's rostered players were folded in.

    Called when the crossed-off set is cleared, so a toggle left on re-seeds on the next
    run instead of reading as "already applied" over an empty set.

    Args:
        state: ``st.session_state`` or a plain dict.
        league_key: ``config.yaml`` league key.
    """
    state[f"{ROSTERED_SEEDED_KEY}::{league_key}"] = False


# =========================================================================
# The panels
# =========================================================================


def replacement_rank(board: pl.DataFrame, position: str) -> Optional[int]:
    """This league's replacement rank at one position, off the board's own rows.

    Recorded per row by ``build_board`` rather than only in ``meta.json``, which is what
    lets a panel size itself without the store.

    Args:
        board: A stored draft board.
        position: The position.

    Returns:
        int | None: The rank, or None where this league starts nobody there.
    """
    if "replacement_rank" not in board.columns:
        return None
    rows = board.filter((pl.col("primaryPosition") == position)
                        & pl.col("replacement_rank").is_not_null())
    return int(rows["replacement_rank"][0]) if rows.height else None


def panel_depth(board: pl.DataFrame, position: str,
                multiple: float = PANEL_DEPTH_MULTIPLE) -> int:
    """How many rows this position's panel should show.

    Args:
        board: A stored draft board.
        position: The position.
        multiple: Multiple of replacement rank to run to.

    Returns:
        int: Rows, clamped to :data:`MIN_PANEL_ROWS` and :data:`MAX_PANEL_ROWS`.
    """
    replacement = replacement_rank(board, position)
    target = int(round((replacement or MIN_PANEL_ROWS) * multiple))
    return max(MIN_PANEL_ROWS, min(MAX_PANEL_ROWS, target))


def value_column(meta: Mapping) -> str:
    """Which column the ``VALUE`` header speaks in, for this league's draft type.

    An auction has a price and the only question is whether he costs less than he is
    worth, so the column is dollars. A snake draft has no price — a pick is a position
    in a queue — so the column is how far the room is letting him fall. The DraftSheet
    switches the same header on the same setting.

    Args:
        meta: The store's ``meta.json``.

    Returns:
        str: ``our_dollars`` or ``value``.
    """
    return "our_dollars" if dv.is_auction(meta) else "value"


def sheet_panel(board: pl.DataFrame, position: str, meta: Mapping, *,
                drafted: Sequence = (), points_column: str = "TRUE_Points",
                depth: Optional[int] = None, search: str = "") -> Panel:
    """Build one position's panel.

    The seven columns are the DraftSheet's, with two honest renamings: its ``ECR`` is
    our ``ADP`` because that is the market number we actually carry, and ``PS`` is
    computed over the position's whole pool rather than only the rows printed.

    Rows are ordered by ``vor`` — not by the points column — even when the availability
    toggle is on. VOR is what makes a quarterback comparable to a running back, and it
    is the order the panel's own ``PS`` column is defined against; sorting on points
    while measuring scarcity on VOR would put the two in disagreement down the page.

    Crossed-off players are **kept in place** rather than filtered out. Watching the
    board empty where the players were is most of what a paper draft sheet is for, and a
    row that vanishes takes its context with it.

    Args:
        board: A stored draft board, already through :func:`draft_view.at_budget`,
            :func:`draft_view.with_cash_value` and
            :func:`draft_view.positional_scarcity`.
        position: The position to panel.
        meta: The store's ``meta.json``, for the draft type.
        drafted: Player ids crossed off.
        points_column: ``TRUE_Points``, or ``avail_points`` with the toggle on.
        depth: Rows to show. None sizes it from replacement level.
        search: Case-insensitive substring of the player name. Narrows the panel rather
            than highlighting inside it — at seven columns there is no room for a
            highlight to read as anything but a mistake.

    Returns:
        Panel: Ready to render.
    """
    pool = board.filter(pl.col("primaryPosition") == position)
    if "projection_missing" in pool.columns:
        pool = pool.filter(~pl.col("projection_missing").fill_null(False))

    gone = set(drafted)
    value = value_column(meta)

    # Counted off `pool` before the search narrows it. "How many good ones are left"
    # is a fact about the position, not about what is currently typed in the box --
    # searching for one name must not report that the position is down to one player.
    remaining = 0
    if "vor" in pool.columns:
        open_now = pool.filter(pl.col("vor") > 0)
        if gone and "player_id" in open_now.columns:
            open_now = open_now.filter(~pl.col("player_id").is_in(list(gone)))
        remaining = open_now.height

    shown = pool
    if search:
        shown = pool.filter(pl.col("player_name").str.to_lowercase()
                            .str.contains(search.strip().lower(), literal=True))

    rows = depth if depth is not None else panel_depth(board, position)
    table = shown.sort("vor", descending=True, nulls_last=True).head(rows)

    ids = table["player_id"].to_list() if "player_id" in table.columns else []
    struck = [pid in gone for pid in ids] or [False] * table.height

    return Panel(
        position=position,
        table=table,
        frame=_display(table, struck, meta, points_column),
        drafted=struck,
        depth=table.height,
        replacement=replacement_rank(board, position),
        remaining=remaining,
        cash=value == "our_dollars",
    )


def _display(table: pl.DataFrame, struck: List[bool], meta: Mapping,
             points_column: str) -> pd.DataFrame:
    """The seven display columns plus the mark, as pandas for the Styler.

    Args:
        table: One panel's rows, already ordered.
        struck: Per row, whether crossed off — decides the mark's label.
        meta: The store's ``meta.json``.
        points_column: Which projection to print.

    Returns:
        pandas.DataFrame: ``Tier``, ``Player``, ``TM/BYE``, ``PTS``, ``VALUE``, ``PS``,
        ``ADP`` and the mark. A source the board does not carry comes through as blanks
        rather than dropping the column, so the four panels stay aligned with each
        other.
    """
    value = value_column(meta)

    def col(name: str) -> pl.Expr:
        return (pl.col(name) if name in table.columns
                else pl.lit(None).cast(pl.Float64).alias(name))

    team = col("pro_team").cast(pl.String).fill_null("-")
    bye = col("bye_week").cast(pl.Int64).cast(pl.String).fill_null("-")

    out = table.select(
        col("tier").alias("Tier"),
        col("player_name").alias("Player"),
        (team + "/" + bye).alias("TM/BYE"),
        col(points_column).alias("PTS"),
        col(value).alias("VALUE"),
        (col("ps") * 100).alias("PS"),
        col("adp").alias("ADP"),
    ).to_pandas()

    # The mark is last, so the eye reaches it after the numbers it is deciding on.
    out[MARK_COLUMN] = [MARK_DRAFTED if hit else MARK_AVAILABLE for hit in struck]
    # A clean RangeIndex, so `panel_styler` can read `row.name` as a row position.
    return out.reset_index(drop=True)


def panel_styler(panel: Panel, theme: str = "light"):
    """Band the panel by tier and dim the players who are gone.

    A ``Styler`` rather than ``column_config`` for the reason
    :func:`draft_view.styled_frame` gives: Streamlit's grid takes cell colour only this
    way, and honours exactly ``color``, ``background-color`` and ``font-weight``.

    Args:
        panel: From :func:`sheet_panel`.
        theme: ``light`` or ``dark``. An unknown theme falls back to light rather than
            raising.

    Returns:
        pandas.io.formats.style.Styler: Ready for ``st.dataframe``.
    """
    ink = SHEET_FILLS.get(theme, SHEET_FILLS["light"])
    tiers = panel.frame["Tier"] if "Tier" in panel.frame.columns else None

    def row_style(row: pd.Series) -> List[str]:
        position = int(row.name)
        if position < len(panel.drafted) and panel.drafted[position]:
            gone = (f"background-color: {ink['struck']}; "
                    f"color: {ink['struck_ink']}")
            return [gone] * len(row)
        tier = None if tiers is None else tiers.iloc[position]
        # `ISEVEN($B5)` on the tier, exactly as the workbook does it: the bands are the
        # tiers, so the cliff is visible without a second column encoding it.
        if pd.notna(tier) and int(tier) % 2 == 0:
            return [f"background-color: {ink['band']}"] * len(row)
        return [""] * len(row)

    # Paint only. Number formatting is `column_config`'s job -- see
    # :func:`column_specs` -- because a Styler that also formats hands Streamlit
    # strings, and a string column cannot be sorted or aligned as a number. Same
    # division of labour as `draft_view.styled_frame` and the board page.
    return panel.frame.style.apply(row_style, axis=1).format(na_rep="")


class SheetColumn(NamedTuple):
    """One panel column: how to render it and what it means.

    One record per column rather than a format dict here and a help dict in the page,
    for the reason :class:`draft_view.Column` gives at length: keeping two keyed
    collections in step needs a test to enforce what a single record makes true by
    construction, and the page's copy cannot be tested at all.

    Attributes:
        label: The header, and the frame's column name.
        kind: ``text``, ``number``, or ``button`` for the cell that is itself the
            control.
        fmt: printf format for a number column.
        help: Tooltip. The glossary, such as it is -- seven columns need no page of
            prose beside them.
        pinned: Freeze against horizontal scrolling.
    """

    label: str
    kind: str
    help: str
    fmt: Optional[str] = None
    pinned: bool = False


def column_specs(panel: Panel) -> List[SheetColumn]:
    """The panel's columns, in render order, with ``VALUE`` resolved to its currency.

    Args:
        panel: From :func:`sheet_panel`.

    Returns:
        List[SheetColumn]: One per column of ``panel.frame``.
    """
    return [
        SheetColumn("Tier", "number", fmt="%d",
                    help="1-D KMeans on projected points within position, so the "
                         "breaks land where the gaps are rather than every N players. "
                         "The alternating shading *is* the tier — the bands are the "
                         "cliff."),
        SheetColumn("Player", "text", pinned=True,
                    help="The name as ESPN spells it."),
        SheetColumn("TM/BYE", "text",
                    help="Pro team and bye week. Two starters sharing a bye is a real "
                         "cost, and nothing else on this panel carries it."),
        SheetColumn("PTS", "number", fmt="%.0f",
                    help="Projected points in this league's own scoring — ESPN, "
                         "FantasyPros, two sportsbooks and our own usage model, "
                         "blended as a stat line and then scored. With Availability "
                         "on, discounted by the games he is expected to miss."),
        # Dollars in an auction, a signed rank difference in a snake -- and the sign is
        # printed as well as coloured, so the column does not depend on the reader
        # inferring direction from magnitude.
        SheetColumn("VALUE", "number", fmt="$%.0f" if panel.cash else "%+.0f",
                    help=("What he is worth out of your budget: our value over "
                          "replacement, priced against the money actually on the "
                          "table." if panel.cash else
                          "How far the room is letting him fall past our rank. "
                          "Positive means he is available later than he should be.")),
        SheetColumn("PS", "number", fmt="%.0f%%",
                    help="Positional scarcity: how much of this position's value over "
                         "replacement is still sitting *below* him and undrafted. High "
                         "means plenty behind him, so no urgency; low means the cliff "
                         "is here. It falls as you cross players off."),
        SheetColumn("ADP", "number", fmt="%.1f",
                    help="ESPN's average draft position — where the room takes him."),
        SheetColumn(MARK_COLUMN, "button",
                    help="Click to cross him off. Click again to put him back."),
    ]
