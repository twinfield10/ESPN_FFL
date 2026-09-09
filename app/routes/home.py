"""Home — the landing page: every league you are in, and what each one needs.

The only tab that is not about the selected league. The League selector still governs
the other four, and a card's button sets it on the way through, so arriving on Roster
from here lands you on the league whose card you pressed.

Thin on purpose: :mod:`views.home_tab` draws it and :mod:`home` decides it. The route
exists to be registered in ``main.PAGES``.

See ``docs/plans/40-frontend-restructure.md``.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import session
from views import home_tab

home_tab.render_home(session.current())
