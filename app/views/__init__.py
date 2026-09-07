"""Renderers for the sub-tabs of the four top-level tabs.

A module here draws part of a tab. It may import Streamlit -- that is the difference
between this package and :mod:`draft_view` / :mod:`sheet_view`, which hold the
decisions and are Streamlit-free so they can be unit-tested. Keep new logic there and
new layout here.

The split exists because ``pages/`` is Streamlit's own directory contract: a module in
it is a *route*. A sub-tab is not a route, so it lives here and is called by the page
that owns it.
"""
