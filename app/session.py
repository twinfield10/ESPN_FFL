"""The one selection every tab reads: which league, which week, whose team.

This is the module every tab shares. ``main.py`` calls :func:`render_context`
once as the app's router frame; every page then calls :func:`current`, which draws
nothing and hands back what the frame already resolved.

**Why the selectors are drawn from here rather than from the pages.** They used to be
three sidebar widgets drawn by ``components.header.render_sidebar()``, which every
page called for itself. That was structurally exposed to a Streamlit behaviour which
had already cost two silent wrong-league renders: *widget state is discarded when you
navigate to a page that has not yet rendered that widget*. See
:func:`components.header.sticky_selectbox` for the full account.

Drawing them from the entrypoint removes the condition rather than defending against
it. Streamlit executes the entrypoint on **every** rerun -- that is what makes it a
router -- so there is no longer any page that has not rendered the league selector.
The unconditional-write pattern is kept anyway, because it costs one line and the
test that pins it is still worth having.

**Which is a fact about *what draws them*, not about where they land.** They sit in
the sidebar, under the identity block, stacked one above the other: they are the two
controls that govern all five tabs, and the sidebar is the app's control surface --
store health and the refresh button are already there. A row of selectors in the body
made the page's own title the second thing on it, and cost Home, whose cards are
sixteen columns wide, a row it needed. The invariant is preserved because
``main.py`` still calls this function, before ``st.navigation``.

**One season, and it is not a choice.** See :func:`current_season`.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

from typing import Dict, List, NamedTuple, Optional

import polars as pl
import streamlit as st

import auth
import store
from components import header
from Scripts.scrape_player_stats import FREE_AGENT_OWNER  # noqa: F401  -- re-exported

#: Where :func:`render_context` leaves its result for :func:`current` to read.
#:
#: Not a module global: Streamlit sessions are per-browser-tab and a global would
#: leak one tab's league into another's. Same reasoning as ``auth.SESSION_KEY``.
SELECTION_KEY = "_selection"

#: The synthetic ``team_owner`` ESPN's unrostered players arrive under.
#:
#: ``lineups.parquet`` holds the free-agent pool as extra rows on a synthetic
#: team rather than as a separate artifact. That is why the Free Agents tab is a
#: filter and not a second ingest -- and why every owner picker has to exclude it.
#:
#: Re-exported from ``Scripts.projection_utils`` rather than defined here, which is
#: where it now lives: ``coverage_population`` needs it and ``Scripts/`` cannot import
#: ``app/`` -- the dependency runs the other way. Imported at module scope rather than
#: deferred the way ``app.store`` defers the same module, because ``app.lineup``
#: already pulls it in transitively through ``Scripts.draft.board``, so the app pays
#: this import either way. Every caller of ``session.FREE_AGENT_OWNER`` is unchanged,
#: and there is now one copy of the string instead of two that could drift.

#: ``st.session_state`` keys the two sidebar selectors own.
LEAGUE_KEY = "league_key"
WEEK_KEY = "week"


class Selection(NamedTuple):
    """What the selectors resolved to. Passed to every renderer.

    Attributes:
        season: Season year. Always :func:`current_season` -- there is no picker.
        league_key: ``config.yaml`` league key.
        display_name: The league's display name.
        week: Selected week.
        meta: The store's ``meta.json`` payload.
        my_owner: The ``team_owner`` value to treat as "mine" on Roster and
            Matchup, from ``meta["primary_owner"]``. None when the store predates
            that key. For the five leagues that are other owners', this is
            correctly *their* name -- the tab is showing you their team because it
            is their league.
    """
    season: int
    league_key: str
    display_name: str
    week: int
    meta: dict
    my_owner: Optional[str]


@st.cache_data(ttl=600, show_spinner=False)
def current_season() -> int:
    """The one season this app renders. 2026 today.

    **There is no season picker, deliberately.** Every tab here answers a question
    about the season in progress -- who to start this week, who is on the wire, do I
    win on Sunday -- and none of them mean anything for a season that has finished.
    A control nobody moves is a control that eventually gets moved by accident.

    Views that genuinely need history read it as an explicit lookback instead: the
    weekly dispersion fit reads 2025 by name, because it is fitting on last year's
    residuals rather than showing you last year.

    Resolved rather than hardcoded so the annual rollover stays one R script
    (``Rscript R/GetNFL.R 2027``). ``Data/NFL_Schedules.csv`` is already the declared
    source of truth for both season and week -- see
    :func:`Scripts.nfl_utils.current_season` and ``docs/SEASON_ROLLOVER.md`` step 3 --
    so reading it here means the app cannot disagree with the pipeline about what
    year it is.

    Falls back to what is actually in the store, because a season the app cannot
    read is worse than a season that is a year stale. That is also the offline case:
    the schedule CSV lives under ``Data/``, which a machine reading the store from S3
    need not have.

    Returns:
        int: Season year.

    Raises:
        RuntimeError: When no season has a store at all. The caller turns this into
            :func:`components.header.no_store_message`; it is the state a fresh
            clone launches in.
    """
    built = store.list_seasons()
    if not built:
        raise RuntimeError("no season has a complete store")

    try:
        from Scripts import nfl_utils
        season = int(nfl_utils.current_season())
    except Exception:                                       # noqa: BLE001
        return max(built)
    return season if season in built else max(built)


def available_weeks(season: int, league_key: str, meta: dict) -> List[int]:
    """Weeks the Week selector should offer, ascending.

    Three sources, in order of how much they can be trusted:

    1. ``meta["weeks_present"]``, which the store records when it knows.
    2. The distinct ``week`` values actually in ``lineups.parquet``.
    3. ``[meta["current_week"]]``.

    **The fallback is load-bearing, not defensive.** ``weeks_present`` is absent from
    every 2026 ``meta.json`` on disk, so the previous ``meta.get("weeks_present") or
    [current_week]`` resolved to a single-element list and the Week dropdown offered
    exactly ``[1]`` -- and would have gone on offering ``[1]`` in December. Reading
    the artifact is what makes the control work at all. It is a cached parquet read,
    so it costs about 11ms.

    Args:
        season: Season year.
        league_key: League key.
        meta: The store's ``meta.json``.

    Returns:
        list: Week numbers, ascending. Never empty.
    """
    recorded = meta.get("weeks_present")
    if recorded:
        return sorted({int(w) for w in recorded})

    try:
        weeks = store.load_lineups(season, league_key)["week"]
        found = sorted({int(w) for w in weeks.unique().to_list() if w is not None})
        if found:
            return found
    except Exception:                                       # noqa: BLE001
        # No lineups artifact yet -- the pre-draft state the app launches in.
        pass

    return [int(meta.get("current_week") or 1)]


def render_context() -> Selection:
    """Draw the two global selectors into the sidebar and return what they resolved to.

    Called **once**, from ``main.py``, before ``st.navigation(...).run()`` and after
    :func:`components.header.render_identity`, which is the heading they sit under.
    Pages call :func:`current` instead.

    Returns:
        Selection: League, week and metadata every tab reads.
    """
    season = current_season()
    viewer = auth.current_viewer()
    configured = header.configured_leagues()

    built = store.list_leagues(season)
    if not built:
        header.no_store_message(store.list_seasons())

    # The one place the app narrows nine configured leagues to this viewer's.
    # Everything downstream reads Selection.league_key, so nothing else has to know.
    mine = auth.visible_leagues(viewer, built)
    if not mine:
        header.no_visible_league_message(viewer, season, configured)

    with st.sidebar:
        league_key = header.sticky_selectbox(
            "League", LEAGUE_KEY, mine,
            default=auth.default_league(viewer, mine),
            format_func=lambda k: configured.get(k, k),
        )

        meta = store.load_meta(season, league_key)
        display_name = (meta.get("display_name")
                        or configured.get(league_key, league_key))
        weeks = available_weeks(season, league_key, meta)
        current_week = int(meta.get("current_week") or weeks[-1])

        week = header.sticky_selectbox(
            "Week", WEEK_KEY, weeks,
            default=current_week if current_week in weeks else weeks[-1],
        )

    selection = Selection(
        season=season, league_key=league_key, display_name=display_name,
        week=week, meta=meta, my_owner=meta.get("primary_owner"),
    )
    st.session_state[SELECTION_KEY] = selection
    return selection


def current() -> Selection:
    """The selection the context row already resolved, for a page to read.

    Draws nothing. Safe to call at the top of any page.

    Returns:
        Selection: What :func:`render_context` resolved this run.

    Raises:
        RuntimeError: If a page is rendered without the frame having run. That
            cannot happen through ``st.navigation``, which executes the entrypoint
            first; it means the page was run directly with ``streamlit run``.
    """
    selection = st.session_state.get(SELECTION_KEY)
    if selection is None:
        raise RuntimeError(
            "session.current() before session.render_context(). Pages are run "
            "through app/main.py -- try `streamlit run app/main.py`."
        )
    return selection


def team_owners(frame: pl.DataFrame) -> List[str]:
    """Real team owners in a lineups frame, excluding the free-agent pool.

    ``lineups.parquet`` carries every unrostered player under a synthetic
    ``team_owner`` of ``"Free Agent"`` -- which is what makes the Free Agents tab a
    filter rather than a second ingest, and what makes it a trap for any owner
    picker that does not exclude it.

    Args:
        frame: A lineups frame.

    Returns:
        list: Owner names, sorted, without ``"Free Agent"``.
    """
    if "team_owner" not in frame.columns:
        return []
    owners = frame["team_owner"].unique().to_list()
    return sorted(o for o in owners if o and o != FREE_AGENT_OWNER)

