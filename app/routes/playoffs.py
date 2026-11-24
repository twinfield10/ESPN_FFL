"""Playoffs — the seeds and bracket for the selected league, if the season ended today.

Thin on purpose: :mod:`views.playoffs_tab` draws it and :mod:`Scripts.playoffs`
decides it. The route exists to be registered in ``main.PAGES``.

See ``docs/plans/57-playoff-bracket.md``.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import session
from views import playoffs_tab

playoffs_tab.render_playoffs(session.current())
