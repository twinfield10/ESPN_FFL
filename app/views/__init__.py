"""Renderers for the sub-tabs, and the bodies, of the five top-level tabs.

A module here draws part of a tab. It may import Streamlit -- that is the difference
between this package and :mod:`draft_view` / :mod:`sheet_view`, which hold the
decisions and are Streamlit-free so they can be unit-tested. Keep new logic there and
new layout here.

The split exists because a module in ``routes/`` is a *route*, registered with
``st.Page`` in ``main.py``. A sub-tab is not a route, so it lives here and is called by
the route that owns it.

``routes/`` rather than ``pages/`` deliberately: a directory of that name beside the
entrypoint switches Streamlit into its legacy multi-page mode, which runs a page file
without the entrypoint. See the note on ``main.PAGES``.
"""
