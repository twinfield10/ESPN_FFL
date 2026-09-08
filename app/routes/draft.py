"""Draft — everything about acquiring a roster, before and after the fact.

Six sub-tabs over one board read, in the order you reach for them:

============= =========================================================
**Board**     the working surface: find a player, filter, read the table
**Sheet**     the on-the-clock view -- four position panels, cross-off
**Values**    where the room and our valuation disagree
**League**    what does not change during a draft
**Calibration** whether the numbers the other tabs stand on are believable
**Rundown**   after the draft: how your roster grades against the room
============= =========================================================

Board and Sheet lead because they are the two surfaces you drive a draft from; the
rest are what you read beforehand and afterwards.

The board is read, rescaled to this league's budget and enriched **once**, by
:func:`views.draft_tabs.prepare`, and the result is handed to every sub-tab. That
matters beyond the ~11ms: the Board and the Sheet must price at the same budget, and
sharing one frame makes that true by construction rather than by both sides
remembering to read the same session key.

See ``docs/plans/09-frontend-draft-views.md`` and
``docs/plans/40-frontend-restructure.md``.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import streamlit as st

import session
import store
from views import draft_tabs, rundown_tab, sheet_tab

selection = session.current()

if not store.has_artifact(selection.season, selection.league_key, "board"):
    st.title("Draft")
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

ctx = draft_tabs.prepare(selection)

board_tab, sheet, values_tab, league_tab, calibration_tab, rundown = st.tabs(
    ["Board", "Sheet", "Values", "League", "Calibration", "Rundown"])

with board_tab:
    draft_tabs.render_board(ctx)

with sheet:
    sheet_tab.render_sheet(ctx)

with values_tab:
    draft_tabs.render_values(ctx)

with league_tab:
    draft_tabs.render_league(ctx)

with calibration_tab:
    draft_tabs.render_calibration(ctx)

with rundown:
    rundown_tab.render_rundown(ctx)
