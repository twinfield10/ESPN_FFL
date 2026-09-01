"""The Sheet — the on-the-clock board (``docs/plans/37-draft-sheet.md``).

Four position panels side by side, banded by tier, with a cross-off column and a live
positional-scarcity read. Reads ``board.parquet`` and nothing else; the engine is the
same one the Draft Board page uses.

**Why this page exists next to that one.** The Draft Board is 45 columns across eight
spanner groups, which is the right shape for the hour *before* a draft — it is where
you decide whether you believe the numbers, and where the disagreement with ESPN and
with the room lives. It is the wrong shape for the ninety seconds you have on the
clock. This page answers one question, in one screen, with no horizontal scroll: **who,
at which position, and is there anything left behind him.**

The organisation is lifted from ``DraftSheets_2026.xlsx``, the BeerSheets replacement,
because it is a genuinely good draft-day interface. Its projections are hand-entered
stat lines and its VBD math has column-drift bugs in two of four positions -- the plan
records them -- so what is taken is the layout and the auction discipline, not the
arithmetic. The numbers are ours.

**No ESPN client in the render path.** ``app/main.py`` states the app is read-only by
construction and this page keeps that: crossing a player off is a click, exactly as the
workbook has you type an ``x``. Polling the live draft endpoint is
``docs/plans/09-frontend-draft-views.md`` §2 and stays unbuilt on purpose -- it is the
half of that page most likely to break on the night, and the manual fallback is the half
that cannot.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import streamlit as st

import draft_view as dv
import sheet_view as sv
import store
from components.header import render_sidebar


def _column_config(panel):
    """Streamlit column config for one panel, from `sv.column_specs`.

    Generated rather than hand-written for the reason the board page's version is: a
    format dict here and a help dict in `sheet_view` cannot be kept in step, and
    Streamlit ignores config for a column the frame does not carry — so a stale label
    stops formatting in silence.

    Args:
        panel: A `sv.Panel`.

    Returns:
        dict: Column name to a `st.column_config` object.
    """
    config = {}
    for column in sv.column_specs(panel):
        tooltip = dv.escape_dollars(column.help)
        if column.kind == "number":
            config[column.label] = st.column_config.NumberColumn(
                format=column.fmt, help=tooltip, pinned=column.pinned)
        elif column.kind == "button":
            # The cell value *is* the button label, which is what makes the mark
            # itself the thing you click. `key=` is what enables the click at all.
            config[column.label] = st.column_config.ButtonColumn(
                help=tooltip, width="small", type="tertiary",
                key=sv.click_key(panel.position))
        else:
            config[column.label] = st.column_config.TextColumn(
                help=tooltip, pinned=column.pinned)
    return config


selection = render_sidebar()
meta = selection.meta

st.title(f"The Sheet · {selection.display_name} {selection.season}")

if not store.has_artifact(selection.season, selection.league_key, "board"):
    st.warning(
        "No draft board in this store. It is a separate artifact because it costs "
        "one `kona_player_info` request per league."
    )
    st.code(
        f"python -m Scripts.refresh --league {selection.display_name} "
        f"--season {selection.season} --what board",
        language="bash",
    )
    st.stop()

theme = getattr(getattr(st.context, "theme", None), "type", "light") or "light"

# Same per-league budget key the Board page owns, read rather than set: the two pages
# must price at the same number, and a second input for the same quantity is how they
# would drift apart. See dv.budget_key for why it is scoped per league.
budget = int(st.session_state.get(dv.budget_key(selection.league_key),
                                  dv.league_auction_budget(meta)))

board = dv.at_budget(store.load_board(selection.season, selection.league_key),
                     budget, meta=meta)
board = dv.with_cash_value(board, meta, budget)
board = dv.with_availability_points(board)

# --- the controls ---------------------------------------------------------
#
# Four, and no more. Every control on this page is one you would reach for with a pick
# clock running; anything you would only touch beforehand belongs on the Board page.
controls = st.columns([2, 1, 1, 1])
search = controls[0].text_input(
    "Find A Player", placeholder="Surname is enough",
    help="Narrows every panel. Matched literally, not as a regex — the names on a "
         "board are full of dots and hyphens.")
depth_multiple = controls[1].slider(
    "Panel Depth", 1.0, 3.0, sv.PANEL_DEPTH_MULTIPLE, 0.5,
    help="As a multiple of this league's replacement rank. Twice replacement is the "
         "last player who could plausibly start for somebody.")
use_availability = controls[2].toggle(
    "Availability", value=False,
    help="Discount every projection by the games the model expects him to miss. Off "
         "by default: the availability head is the weakest arm of the model that "
         "produces it (r = +0.343 on prior-season games), and it moves the top of the "
         "board. Worth looking at, not worth being the default.")
show_streamed = controls[3].toggle(
    "K / D·ST", value=False,
    help="Kickers and team defences. Off by default because a season-total value over "
         "replacement does not describe a position you stream — `VALUE` is blank for "
         "them for that reason.")

points_column = "avail_points" if use_availability else "TRUE_Points"
if use_availability and "avail_points" not in board.columns:
    st.info(
        "This board carries no `usg_expected_games`, so there is no availability "
        "estimate to discount by. Showing `TRUE_Points`."
    )
    points_column = "TRUE_Points"

# --- who is off the board -------------------------------------------------
#
# Read before the panels are drawn, so a click lands on the same run it is made rather
# than one behind. Each panel resolves its own click to a player id immediately, because
# a row number is a position in a panel that crossing somebody off has just changed.
drafted = sv.drafted_set(st.session_state, selection.league_key)

positions = list(sv.SHEET_POSITIONS)
if show_streamed:
    positions += list(sv.SHEET_STREAMED)
# Scarcity is measured against who is still available, which is the entire point of the
# column -- so it is computed after the crossed-off set is known and before any panel is
# built from it.
board = dv.positional_scarcity(board, drafted=drafted)

panels = {
    position: sv.sheet_panel(
        board, position, meta, drafted=drafted, points_column=points_column,
        depth=sv.panel_depth(board, position, depth_multiple), search=search)
    for position in positions
}

for position, panel in panels.items():
    updated = sv.toggle_drafted(st.session_state, panel.table,
                               selection.league_key, position)
    if updated != drafted:
        # A click changed the board, and `ps` and every panel below this one are now
        # stale. Rerunning is cheaper and much clearer than patching them in place.
        st.rerun()

# --- the panels -----------------------------------------------------------
columns = st.columns(len(sv.SHEET_POSITIONS))
for column, position in zip(columns, sv.SHEET_POSITIONS):
    panel = panels[position]
    with column:
        left = panel.remaining
        st.markdown(f"**{position}** · {left} left above replacement")
        st.dataframe(
            sv.panel_styler(panel, theme),
            width="stretch", hide_index=True, height=min(640, 60 + 35 * panel.depth),
            column_config=_column_config(panel),
            # A blank cell rather than the word "None", which on an unpriced player's
            # VALUE reads as an answer rather than as an absence.
            placeholder="",
            lazy=False,
        )

if show_streamed:
    st.divider()
    streamed = st.columns(len(sv.SHEET_STREAMED))
    for column, position in zip(streamed, sv.SHEET_STREAMED):
        panel = panels[position]
        with column:
            st.markdown(f"**{position}** · streamed, so `VALUE` is blank")
            st.dataframe(
                sv.panel_styler(panel, theme), width="stretch", hide_index=True,
                height=min(400, 60 + 35 * panel.depth),
                column_config=_column_config(panel),
                placeholder="", lazy=False,
            )

# --- the state, and how to clear it ---------------------------------------
st.divider()
footer = st.columns([3, 1])
footer[0].caption(
    f"**{len(drafted)} crossed off.** Held for this browser session and this league "
    f"only — nothing is written to the store, and closing the tab loses it. `PS` and "
    f"the counts above each panel are measured against who is still available, so they "
    f"decay as the draft runs. Prices are in **${budget}**, set on the Draft Board "
    f"page."
)
if footer[1].button("Clear Crossed Off", width="stretch",
                    disabled=not drafted):
    st.session_state[sv.drafted_key(selection.league_key)] = set()
    st.rerun()
