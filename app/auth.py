"""Who is looking at this app, and which leagues they are allowed to see.

**There is no authentication here yet, and this module does not pretend there is.**
It is the seam a login will be wired into: every page and component asks
:func:`current_viewer` who the user is and :func:`visible_leagues` what they may
open, so the day a real identity provider lands, the change is
:func:`current_viewer` returning a :class:`Viewer` built from a session token
instead of :data:`DEFAULT_VIEWER`. Nothing else moves.

The distinction matters because the alternative -- pages reading
``store.list_leagues()`` directly -- is what makes retrofitting auth a rewrite. Eight
leagues live in ``config.yaml``, four of them belong to other owners, and every page
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
        owner_names: The ``team_owner`` spellings that mean *this viewer*, for the
            one question :attr:`display_name` must never be asked: **which team in
            a league is his**. They are different questions -- a display name is
            how to greet somebody, and an owner name is a join key -- and
            conflating them would work today and break on the first viewer ESPN
            spells differently from their own greeting.

            A tuple rather than a string because the same person is not always one
            name. ``fetch_utils.set_owner_names`` builds it from ESPN's first and
            last name and ``.title()``-cases the result, and ESPN is not
            consistent across its own endpoints -- ``draft_view._franchise_key``
            exists for exactly that, and ``knights_ffl``'s draft history carries
            both ``"andrew blair"`` and ``"Andrew Blair"``. Verified as one
            spelling for this viewer in all four 2026 stores; the tuple is where
            the second one goes when it is not.

            Defaulted, so every existing :class:`Viewer` construction still works.
    """
    user_id: str
    display_name: str
    leagues: Tuple[str, ...]
    default_league: str
    owner_names: Tuple[str, ...] = ()


#: The only account that exists until login lands: the repo's owner, scoped to the
#: four leagues he plays in. The other five in ``config.yaml`` are other owners'
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
#:
#: **Removing a league is three edits, and leaving the data behind was the bug.**
#: ``weenieless_wanderers`` came out of ``config.yaml`` and out of here on
#: 2026-09-09, ``big_red_fantasy_football`` on 2026-09-15. Dropping a league from
#: the config stops the *pipeline* fetching it; dropping it from this tuple stops
#: the *app* offering it. Neither stops it being **published**, and for six weeks
#: nothing did: ``Scripts.sync`` fanned out over store prefixes rather than the
#: config, so both leagues kept getting pushed and kept minting a dated board
#: snapshot every night from a board nothing rebuilt. Seven of Weenieless's
#: snapshots are byte-identical copies of 2026-09-09's. That hole is closed in
#: ``Scripts.sync._league_seasons``, and both leagues' data was purged on
#: 2026-09-16 -- 1.26 GB across 2,583 object versions.
#:
#: **What survives is their G2 archive**, at ``archive/g2/season=2026/``. It is the
#: pre-season counterfactual behind the published fit in ``Scripts/lab/results.json``
#: and cannot be rebuilt at any price, so it is exempt from the purge and from every
#: lifecycle rule. Nothing in the app reads it.
DEFAULT_VIEWER = Viewer(
    user_id="tommy",
    display_name="Tommy Winfield",
    leagues=("winfield_football", "knights_ffl", "gop_degenerates",
             "jeffs_league"),
    default_league="winfield_football",
    owner_names=("Tommy Winfield",),
)

#: Where a signed-in viewer is kept. ``st.session_state`` rather than a module
#: global because sessions are per-browser-tab and a global would leak one user's
#: identity into another's session the moment this is served to more than one
#: person -- which is the entire premise of adding login.
SESSION_KEY = "viewer"

#: Set this to see every configured league regardless of who the viewer is.
#:
#: Not a backdoor -- see the module docstring on why this is not a security
#: boundary. It exists because four of the eight configured leagues belong to other
#: owners who read their numbers off the Google Sheet, and when one of those Sheets
#: looks wrong the app is where you go to find out why. Scoping the picker must not
#: cost the ability to answer that question.
#:
#: It reaches every league the *store* holds, which since the 2026-09-16 purge is
#: every league the config holds. It is no longer a way to reach a disconnected
#: league, because disconnected leagues no longer have data to reach: the config is
#: now the authority for what gets published, so a league that leaves the config
#: stops accumulating a store rather than quietly keeping one.
ALL_LEAGUES_ENV = "ESPN_FFL_ALL_LEAGUES"

#: Set this to ``0`` to remove the sidebar's Owner selector.
#:
#: The selector is the local convenience that replaces restarting the app with a
#: different :data:`VIEWER_ENV`, and it is **on by default** because needing a flag
#: to get it would cost exactly the friction it exists to remove. It is not a
#: security boundary and does not pretend to be one -- neither is anything else in
#: this module; see the module docstring. It is off-switchable rather than
#: on-switchable so that a served deployment has one thing to set, and plan 26's
#: real login is what removes it for good.
OWNER_PICKER_ENV = "ESPN_FFL_OWNER_PICKER"

#: The unrestricted viewer :data:`ALL_LEAGUES_ENV` resolves to. Empty ``leagues``
#: is the "no restriction" sentinel, so this is the default viewer with the scope
#: dropped and the same landing league.
UNRESTRICTED_VIEWER = DEFAULT_VIEWER._replace(user_id="all", leagues=())

#: The other owner this laptop actually opens the app as. ``john_pc_league`` and
#: ``john_atl_league`` are his two of the eight in ``config.yaml``; the pipeline has
#: always built them and the Google Sheet is how he normally reads them, but the
#: Sheet cannot answer "why does this number look wrong", and the app could not be
#: pointed at him without editing :data:`DEFAULT_VIEWER`.
#:
#: ``owner_names`` is a join key, not a greeting -- see :class:`Viewer`. ``John
#: Baizer`` is the spelling ``team_owner`` actually carries in both leagues'
#: ``lineups`` and ``team_stats`` for 2026, checked rather than assumed, and it is
#: one spelling in both. The tuple is where a second one goes if ESPN ever
#: disagrees with itself about him the way it does about ``knights_ffl``'s Andrew
#: Blair.
JOHN_VIEWER = Viewer(
    user_id="john",
    display_name="John Baizer",
    leagues=("john_pc_league", "john_atl_league"),
    default_league="john_pc_league",
    owner_names=("John Baizer",),
)

#: The third owner, added 2026-09-22 with ``richardson_invitational``. Her league
#: is the first in ``config.yaml`` that no Winfield is a member of, so it carries
#: its own cookie pair and it is the only one she can open.
#:
#: ``owner_names`` is a join key, not a greeting. ``Emma Richardson`` is the
#: spelling ``team_owner`` carries in this league's ``lineups``, ``team_stats``
#: and ``results`` and that ``owner`` carries in ``draft`` -- checked in all four
#: rather than assumed, and one spelling in each. The tuple is where a second one
#: goes if ESPN ever disagrees with itself about her.
EMMA_VIEWER = Viewer(
    user_id="emma",
    display_name="Emma Richardson",
    leagues=("richardson_invitational",),
    default_league="richardson_invitational",
    owner_names=("Emma Richardson",),
)

#: Every viewer this app can be launched as, by :data:`VIEWER_ENV` value.
#:
#: Not a user table and not a step toward one -- a real login brings its own. It is
#: the three accounts that exist on this laptop, so that opening the app as somebody
#: else is a launch flag rather than a diff. Adding a fourth owner here is one entry
#: plus their ``team_owner`` spelling, verified against their league's parquet.
VIEWERS = {
    "tommy": DEFAULT_VIEWER,
    "john": JOHN_VIEWER,
    "emma": EMMA_VIEWER,
}

#: Which of :data:`VIEWERS` to render for. Unset means :data:`DEFAULT_VIEWER`.
#:
#: Read at call time rather than import, like :data:`ALL_LEAGUES_ENV`. An
#: unrecognised value is a hard error rather than a fallback: silently rendering
#: Tommy's four leagues to somebody who asked for John's two is exactly the
#: wrong-board-on-draft-night failure the scoping exists to prevent, and a typo in a
#: launch flag is the likeliest way to ask for it.
VIEWER_ENV = "ESPN_FFL_VIEWER"


def _unrestricted() -> bool:
    """Whether :data:`ALL_LEAGUES_ENV` is set to something truthy.

    Read at call time rather than import, matching ``app.store``'s handling of
    ``ESPN_FFL_STORE_SOURCE`` -- the env var can be changed without restarting.

    Returns:
        bool: True when the scope should be dropped.
    """
    return os.environ.get(ALL_LEAGUES_ENV, "").strip().lower() in {"1", "true", "yes"}


def _named_viewer() -> Optional[Viewer]:
    """The viewer :data:`VIEWER_ENV` names, if it names one.

    Returns:
        Viewer | None: None when the variable is unset or empty, which is the
        ordinary case.

    Raises:
        ValueError: When it is set to something that is not a key of
            :data:`VIEWERS`. Loud on purpose -- Streamlit renders the traceback on
            the page, which is the only outcome better than quietly showing the
            wrong owner's leagues.
    """
    name = os.environ.get(VIEWER_ENV, "").strip().lower()
    if not name:
        return None
    try:
        return VIEWERS[name]
    except KeyError:
        raise ValueError(
            f"{VIEWER_ENV}={name!r} is not a known viewer. "
            f"Known: {', '.join(sorted(VIEWERS))}."
        ) from None


def current_viewer() -> Viewer:
    """The viewer this render is for.

    The one function a login has to change. Today it resolves, in order: a viewer
    a sign-in put in session state, the viewer :data:`VIEWER_ENV` names, then
    :data:`DEFAULT_VIEWER` -- and then :data:`ALL_LEAGUES_ENV` drops whichever
    one of those it landed on down to no scope at all, rather than replacing him.
    A sign-in outranks the launch flag because the launch flag is only a default;
    the scope hatch outranks both because it is a statement about the *picker*,
    not about who is looking.

    Returns:
        Viewer: Never None. An app with no viewer has nothing to render, so the
        fallback is a real account rather than an anonymous one.
    """
    stored = st.session_state.get(SESSION_KEY)
    if isinstance(stored, Viewer):
        viewer = stored
    else:
        viewer = _named_viewer() or DEFAULT_VIEWER
    if _unrestricted():
        return viewer._replace(user_id="all", leagues=())
    return viewer


def owner_picker_enabled() -> bool:
    """Whether the sidebar should offer the Owner selector.

    Read at call time rather than import, like :func:`_unrestricted`, so the
    variable can be changed without restarting.

    Returns:
        bool: True unless :data:`OWNER_PICKER_ENV` is set to a falsey word, or
        there is only one viewer to choose between -- a one-option dropdown is
        furniture, not a control.
    """
    off = os.environ.get(OWNER_PICKER_ENV, "").strip().lower() in {"0", "false", "no"}
    return not off and len(VIEWERS) > 1


def selected_viewer_key() -> str:
    """Which entry of :data:`VIEWERS` the app is currently drawing for.

    Deliberately *not* ``current_viewer().user_id``: :func:`current_viewer`
    rewrites ``user_id`` to ``"all"`` under :data:`ALL_LEAGUES_ENV`, which is a
    statement about the picker rather than about who is looking, and the Owner
    selector must keep showing the real owner underneath it. This reads the same
    two sources in the same order, and stops before that rewrite.

    Returns:
        str: A key of :data:`VIEWERS`. Falls back to the first one when a
        sign-in put a viewer in session state that is not one of them -- the
        guest case in ``tests/test_app_auth.py``, which has no key to name.
    """
    stored = st.session_state.get(SESSION_KEY)
    target = stored if isinstance(stored, Viewer) else (_named_viewer() or DEFAULT_VIEWER)
    for key, viewer in VIEWERS.items():
        if viewer.user_id == target.user_id:
            return key
    return next(iter(VIEWERS))


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


def owner_for(viewer: Viewer, owners: Sequence[str]) -> Optional[str]:
    """Which team in a league is this viewer's, by ``team_owner`` name.

    The cross-league join, and it is a **name** join rather than an id join because
    there is no id to join on: ``lineups`` and ``team_stats`` carry ``team_owner``
    and nothing else. ``owner_id`` exists, on ``draft`` and ``tendencies`` only, and
    it is not the shortcut it looks like -- this viewer has *two* ESPN SWIDs,
    ``{DA9F7430-...}`` in ``winfield_football`` and ``{796FF49A-...}`` in the other
    three, so an id join would need the union before it beat the name.

    Falls back to :attr:`Viewer.display_name` when :attr:`Viewer.owner_names` is
    empty, which is what every viewer built before that field existed looks like.

    Args:
        viewer: From :func:`current_viewer`.
        owners: The league's real ``team_owner`` values, e.g.
            ``session.team_owners(frame)``.

    Returns:
        str | None: The first of the viewer's names this league actually has. None
        when he is not in it -- which is an ordinary answer, not an error: a viewer
        can have a league in :attr:`Viewer.leagues` without playing in it.
    """
    known = set(owners)
    for name in (viewer.owner_names or (viewer.display_name,)):
        if name in known:
            return name
    return None


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
