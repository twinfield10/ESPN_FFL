"""Who is looking at this app, and which leagues they are allowed to see.

**There is no authentication here yet, and this module does not pretend there is.**
It is the seam a login will be wired into: every page and component asks
:func:`current_viewer` who the user is and :func:`visible_leagues` what they may
open, so the day a real identity provider lands, the change is
:func:`current_viewer` returning a :class:`Viewer` built from a session token
instead of :data:`DEFAULT_VIEWER`. Nothing else moves.

The distinction matters because the alternative -- pages reading
``store.list_leagues()`` directly -- is what makes retrofitting auth a rewrite. Nine
leagues live in ``config.yaml``, five of them belong to other owners, and every page
that reaches past this module is a page that would have to be found and changed
later.

**This is not a security boundary and must not be read as one.** It scopes a
*local, single-user* Streamlit app whose data comes from a store the same laptop
already has full read access to. Filtering the league picker is a statement about
what is worth showing, not about what is reachable; a viewer restriction here keeps
nobody out of ``Data/Store`` or the bucket. When real login lands, the enforcement
belongs at the store read, not in the sidebar.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import os
from typing import List, NamedTuple, Optional, Sequence, Tuple

import streamlit as st


class Viewer(NamedTuple):
    """The person the app is being rendered for.

    Attributes:
        user_id: Stable identifier. Will be whatever the identity provider calls
            them; a literal until then.
        display_name: How to greet them in the sidebar.
        leagues: ``config.yaml`` league keys they may open, in the order they
            should be offered. **Empty means unrestricted** -- see
            :func:`visible_leagues`, which is the only place that reading is
            applied.
        default_league: The league key to land on. Ignored when it is not among
            the ones actually built for the selected season.
    """
    user_id: str
    display_name: str
    leagues: Tuple[str, ...]
    default_league: str


#: The only account that exists until login lands: the repo's owner, scoped to the
#: five leagues he plays in. The other five in ``config.yaml`` are other owners'
#: -- the pipeline still builds and publishes them, and the Google Sheet is how
#: those owners read them (see plan 14), but they are not this viewer's leagues and
#: showing them in his picker is how the wrong board gets opened on draft night.
#:
#: Ordered deliberately rather than alphabetically: the default first, then the
#: rest by how often they are actually opened.
#:
#: **This tuple is the one thing adding a league to ``config.yaml`` does not
#: update.** ``jeffs_league`` was configured, refreshed and published on 2026-09-01
#: and still did not appear in the picker, because scoping happens here and nowhere
#: else -- which is the module's whole design and also its one sharp edge. If a
#: league is missing from the app but present in ``store.list_leagues``, this is
#: why.
DEFAULT_VIEWER = Viewer(
    user_id="tommy",
    display_name="Tommy Winfield",
    leagues=("winfield_football", "knights_ffl", "gop_degenerates",
             "weenieless_wanderers", "jeffs_league"),
    default_league="winfield_football",
)

#: Where a signed-in viewer is kept. ``st.session_state`` rather than a module
#: global because sessions are per-browser-tab and a global would leak one user's
#: identity into another's session the moment this is served to more than one
#: person -- which is the entire premise of adding login.
SESSION_KEY = "viewer"

#: Set this to see every configured league regardless of who the viewer is.
#:
#: Not a backdoor -- see the module docstring on why this is not a security
#: boundary. It exists because five of the nine leagues belong to other owners who
#: read their numbers off the Google Sheet, and when one of those Sheets looks
#: wrong the app is where you go to find out why. Scoping the picker must not cost
#: the ability to answer that question.
ALL_LEAGUES_ENV = "ESPN_FFL_ALL_LEAGUES"

#: The unrestricted viewer :data:`ALL_LEAGUES_ENV` resolves to. Empty ``leagues``
#: is the "no restriction" sentinel, so this is the default viewer with the scope
#: dropped and the same landing league.
UNRESTRICTED_VIEWER = DEFAULT_VIEWER._replace(user_id="all", leagues=())


def _unrestricted() -> bool:
    """Whether :data:`ALL_LEAGUES_ENV` is set to something truthy.

    Read at call time rather than import, matching ``app.store``'s handling of
    ``ESPN_FFL_STORE_SOURCE`` -- the env var can be changed without restarting.

    Returns:
        bool: True when the scope should be dropped.
    """
    return os.environ.get(ALL_LEAGUES_ENV, "").strip().lower() in {"1", "true", "yes"}


def current_viewer() -> Viewer:
    """The viewer this render is for.

    The one function a login has to change. Today it resolves, in order: the
    environment escape hatch, a viewer a sign-in put in session state, then
    :data:`DEFAULT_VIEWER`.

    Returns:
        Viewer: Never None. An app with no viewer has nothing to render, so the
        fallback is a real account rather than an anonymous one.
    """
    if _unrestricted():
        return UNRESTRICTED_VIEWER
    stored = st.session_state.get(SESSION_KEY)
    return stored if isinstance(stored, Viewer) else DEFAULT_VIEWER


def sign_in(viewer: Viewer) -> None:
    """Record who is looking, for the rest of this session.

    Nothing calls this yet. It is the other half of :func:`current_viewer`, and it
    is here so the eventual login callback has an obvious place to hand its result
    to rather than reaching into session state itself.

    Args:
        viewer: The authenticated viewer.
    """
    st.session_state[SESSION_KEY] = viewer


def sign_out() -> None:
    """Forget the signed-in viewer, falling back to :data:`DEFAULT_VIEWER`."""
    st.session_state.pop(SESSION_KEY, None)


def visible_leagues(viewer: Viewer, league_keys: Sequence[str]) -> List[str]:
    """Filter league keys down to the ones this viewer may open.

    The caller's order is preserved rather than the viewer's, because the caller's
    order is the store's -- sorted, and stable across seasons. A viewer whose
    ``leagues`` is empty is unrestricted and gets the list back unchanged.

    Args:
        viewer: From :func:`current_viewer`.
        league_keys: League keys that actually have a store, e.g.
            ``store.list_leagues(season)``.

    Returns:
        list: The subset the viewer may open. Possibly empty, which is a state the
        caller has to handle -- a viewer whose leagues have not been built for the
        selected season is not an error.
    """
    if not viewer.leagues:
        return list(league_keys)
    allowed = set(viewer.leagues)
    return [key for key in league_keys if key in allowed]


def default_league(viewer: Viewer,
                   league_keys: Sequence[str]) -> Optional[str]:
    """Which league to land on, given what is actually available.

    Args:
        viewer: From :func:`current_viewer`.
        league_keys: Keys already filtered through :func:`visible_leagues`.

    Returns:
        str | None: The viewer's default when it is available, otherwise the first
        key offered, otherwise None when nothing is.
    """
    keys = list(league_keys)
    if viewer.default_league in keys:
        return viewer.default_league
    return keys[0] if keys else None
