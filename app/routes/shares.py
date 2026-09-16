"""Player Shares — who to root for, across every league at once.

The second tab that is not about the selected league. Roster and Matchup answer one
league at a time; this one totals a player's worth across all of them, so a player
you own in two leagues and face in a third arrives as a single signed number.

Thin on purpose: :mod:`views.shares_tab` draws it and :mod:`player_shares` decides
it. The route exists to be registered in ``main.PAGES``.

See ``docs/plans/42-weekly-matchup-odds.md`` for the probability it differentiates.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import session
from views import shares_tab

shares_tab.render_shares(session.current())
