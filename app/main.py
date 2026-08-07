"""Entry point for the local app. Run from the repo root::

    streamlit run app/main.py

Read-only by construction: every page reads ``Data/Store`` and nothing here
imports an ESPN client. Ingest is ``python -m Scripts.refresh``, which the sidebar
can shell out to. The separation is the point -- one league is seconds of ESPN
round-trips against 11ms to read the same frame back from parquet, so recomputing
on interaction would make the UI unusable.

Pages are registered below. Plans 08 and 09 add to this list; the navigation and
the sidebar do not change as they do.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import streamlit as st

st.set_page_config(
    page_title="Fantasy Football",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded",
)

#: Every page. Each script calls ``components.header.render_sidebar()`` itself, so
#: the league/season/week selection is identical wherever you are.
PAGES = [
    st.Page("pages/overview.py", title="Store overview", icon="📦", default=True),
    st.Page("pages/draft_board.py", title="Draft board", icon="📋"),
    # Plan 08: My Matchup, League Slate, Free Agents, Player Explorer,
    #          Projection Accuracy, Playoff Odds, Standings, History
    # Plan 09: Live Draft (needs the ESPN draft endpoint in the render path),
    #          Draft History (needs roadmap Phase 1's backfill)
]

st.navigation(PAGES).run()
