"""Entry point for the local app. Run from the repo root::

    streamlit run app/main.py

Read-only by construction: every page reads ``Data/Store`` and nothing here imports
an ESPN client. Ingest is ``python -m Scripts.refresh``, which the sidebar can shell
out to. The separation is the point -- one league is seconds of ESPN round-trips
against 11ms to read the same frame back from parquet, so recomputing on interaction
would make the UI unusable.

**This file is the router frame, and that is load-bearing.** Streamlit executes the
entrypoint on every rerun before it executes the current page, so anything drawn here
appears above every tab and -- more importantly -- is guaranteed to have rendered.
That is why the league and week selectors live in :func:`session.render_context`
rather than on the pages: a widget Streamlit has not yet rendered on the current page
has its state discarded, which twice showed the wrong league. See
:func:`components.header.sticky_selectbox`.

Four tabs, in the order you use them across a season: you draft, then you set a
lineup, then you work the wire, then you find out whether you won.

Every title, header and column label in this app is **Title Case** -- one house
style, applied to labels rather than to prose. Captions and explanatory paragraphs
stay sentence case, because they are sentences.

Which leagues a page may open is decided in :mod:`auth`, not here. There is no login
yet; that module is the seam one lands in.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import streamlit as st

st.set_page_config(
    page_title="Fantasy Football",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded",
)

import session                                    # noqa: E402
from components import header                     # noqa: E402

#: The four tabs. Rendered in the top header rather than the sidebar so the sidebar
#: is free for store health, and so the tab bar sits above the one context row that
#: governs all four.
#:
#: Plan 08's remaining pages -- Player Explorer, Projection Accuracy, Playoff Odds,
#: Standings, History -- are not a fifth tab. They are sub-tabs of whichever of
#: these four they answer a question for. See ``docs/plans/40-frontend-restructure.md``.
#:
#: **The directory is ``routes/`` and must not be renamed to ``pages/``.** Streamlit
#: still carries its first multi-page mechanism, and the whole trigger for it is a
#: directory called ``pages`` beside the entrypoint: ``PagesManager`` sets a
#: process-wide ``uses_pages_directory`` flag from ``main_script_parent / "pages"``
#: existing, before any application code runs, and while it is set the script runner
#: executes **the requested page file alone** through its own private navigation --
#: this file, and therefore :func:`session.render_context`, never runs at all.
#:
#: Only the *public* ``st.navigation`` below clears that flag, so the app recovered
#: as soon as anything landed on the default page and looked fine thereafter. Open
#: ``/roster`` as the first request to a freshly started server and it did not:
#: every page raised ``session.current() before session.render_context()`` under a
#: legacy sidebar nav. Renaming the directory is the only fix available, the flag
#: being decided before there is anywhere to intervene.
PAGES = [
    st.Page("routes/draft.py", title="Draft", icon="📋", default=True),
    st.Page("routes/roster.py", title="Roster", icon="📊"),
    st.Page("routes/free_agents.py", title="Free Agents", icon="🔎"),
    st.Page("routes/matchup.py", title="Matchup", icon="⚔️"),
]

# Order matters. The context row is drawn before the page body so it reads as a
# header for it; the sidebar is drawn after, because it needs the resolved league.
selection = session.render_context()
header.render_sidebar_health(selection)
st.divider()

st.navigation(PAGES, position="top").run()
