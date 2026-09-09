"""Home — one card per league, and the tables underneath them.

Layout only. Every decision a card states is made in :mod:`home`, which is
Streamlit-free and tested; this reads five summaries and draws them.

**The store reads live here rather than in :mod:`home`**, so a summary can be cached
on the same fingerprint the artifact readers use -- see :func:`summary` and
:func:`store.version`. Without that, the sidebar's refresh button would repaint every
deep tab and leave the landing page showing the store from before it.

**Navigation is deferred through ``session_state``, and that is not a style choice.**
A card's button has to do two things: point the global league selector at that league,
and open the tab. Doing the first from an ``on_click`` callback is the documented way
to set another widget's value -- but ``st.switch_page`` cannot be called from that
callback, because callbacks run *before* the script body and therefore before
``st.navigation`` has declared what the pages are. It fails with ``Could not find
page: routes/roster.py``, the legacy-``pages/`` error, which is a misleading way to
learn this. So the callback writes :data:`PENDING_KEY` and the page body, which runs
after ``st.navigation``, acts on it. See :data:`main.PAGES` for the other half of the
same Streamlit behaviour.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

from typing import Dict, List, Optional, Sequence

import streamlit as st

import auth
import home
import matchup_sim as sim
import session
import store

#: Where a card's button leaves the route it wants opened, for the page body to act on.
#:
#: Underscored like :data:`session.SELECTION_KEY`: it is app plumbing living in the
#: same namespace as the widget keys, and the prefix is what keeps the two apart.
PENDING_KEY = "_home_pending_route"

#: Every card's links, in the order a week is worked: set the lineup, work the wire,
#: then find out whether you win.
#:
#: Drawn on **every** card rather than only where there is something to do, so the
#: buttons are in the same place on all five. The one you need is the one painted
#: primary -- see :func:`_render_links`.
LINKS = (
    ("Roster", home.ROUTE_ROSTER),
    ("Free Agents", home.ROUTE_FREE_AGENTS),
    ("Matchup", "routes/matchup.py"),
)

#: How many cards sit side by side. Two, because a card holds a fixture line, up to
#: two callouts and three buttons, and three across makes the callouts wrap to four
#: lines each on a laptop.
CARD_COLUMNS = 2

#: The standings table's columns.
#:
#: ``This Week`` and ``Projected`` are both here on purpose: one is what has been
#: scored and the other is what we think will be, and a week in progress cannot be
#: described by either alone. See :func:`home.standings`.
STANDINGS_CONFIG: Dict[str, object] = {
    "Rk": st.column_config.NumberColumn(
        format="%d", pinned=True, width="small",
        help="Win percentage, then points for, then this week's projection — which "
             "only separates two teams that are level on both, and before week 1 "
             "separates all of them."),
    "Owner": st.column_config.TextColumn(pinned=True),
    "W-L-T": st.column_config.TextColumn(
        width="small",
        help="Over weeks that have actually been played. An unplayed fixture is "
             "recorded by ESPN as a 0-0 tie, and is not counted here."),
    "Win%": st.column_config.NumberColumn(
        format="%.3f", width="small", help="A tie counts as half a win."),
    "PF": st.column_config.NumberColumn(format="%.1f", width="small",
                                        help="Points for."),
    "PA": st.column_config.NumberColumn(format="%.1f", width="small",
                                        help="Points against."),
    "This Week": st.column_config.NumberColumn(
        format="%.1f", width="small",
        help="Actually scored this week. Empty until the week is played."),
    "Projected": st.column_config.NumberColumn(
        format="%.1f", width="small",
        help="Our blend's total for the lineup ESPN currently has set — the same "
             "basis the Matchup tab quotes, not the best lineup available."),
}


def _go(league_key: str, week: int, route: str) -> None:
    """Point the global selectors at one league and queue a route to open.

    An ``on_click`` callback, which is the only place Streamlit lets you set another
    widget's value: callbacks run before the script reruns, so the write lands before
    :func:`session.render_context` reads the key. It deliberately does **not** call
    ``st.switch_page`` -- see the module docstring.

    Args:
        league_key: League to select.
        week: Week to select. Skipped when the target league does not have it, which
            :func:`home.week_for` has already resolved.
        route: Route file to open on the next run.
    """
    st.session_state[session.LEAGUE_KEY] = league_key
    st.session_state[session.WEEK_KEY] = week
    st.session_state[PENDING_KEY] = route


@st.cache_data(ttl=store.CACHE_TTL, show_spinner="Reading your leagues…")
def summary(season: int, league_key: str, display_name: str,
            requested_week: Optional[int], version: str) -> home.LeagueSummary:
    """One league's card, read and reduced.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        display_name: The league's name, for the card's heading.
        requested_week: The globally selected week. Resolved against this league's
            own by :func:`home.week_for`.
        version: From :func:`store.version`. Part of the cache key and not used
            directly -- the same trick :func:`store._load_artifact` uses, and the
            reason a refresh invalidates this too.

    Returns:
        home.LeagueSummary: With ``notes`` naming whatever it could not answer.
    """
    del version                              # cache key only

    meta = store.load_meta(season, league_key)
    week = home.week_for(meta, requested_week)
    lineups = store.load_lineups(season, league_key)
    team_stats = (store.load_team_stats(season, league_key)
                  if store.has_artifact(season, league_key, "team_stats") else None)

    return home.summarise(
        league_key=league_key, display_name=display_name, week=week, meta=meta,
        lineups=lineups, team_stats=team_stats, fitted=sim.model(),
        free_agent_owner=session.FREE_AGENT_OWNER)


def _render_links(card: home.LeagueSummary) -> None:
    """The three tab links at the foot of a card.

    The route an action points at is painted primary, so which button to press is
    answered by the same colour that raised the alarm. Only a *critical* action earns
    it: three cards each with a primary button is a page with no emphasis on it.

    Args:
        card: The league being drawn.
    """
    urgent = {action.route for action in card.actions
              if action.severity == home.ACTION_CRITICAL}
    columns = st.columns(len(LINKS))
    for column, (label, route) in zip(columns, LINKS):
        column.button(
            label, key=f"home_go_{card.league_key}_{route}", width="stretch",
            type="primary" if route in urgent else "secondary",
            on_click=_go, args=(card.league_key, card.week, route))


def _render_card(card: home.LeagueSummary, requested_week: Optional[int]) -> None:
    """One league: where it stands, who it plays, and what it needs.

    Args:
        card: From :func:`summary`.
        requested_week: The globally selected week, so the card can say when it is
            answering about a different one.
    """
    with st.container(border=True):
        st.markdown(f"##### {card.display_name}")

        wins, losses, ties = card.record
        standing = (f"{_ordinal(card.rank)} of {card.teams}"
                    if card.rank and card.teams else None)
        st.caption(" · ".join(part for part in (
            standing, f"{wins}-{losses}-{ties}", f"Week {card.week}") if part))

        if requested_week is not None and int(requested_week) != card.week:
            st.caption(f"⚠️ this league has no week {int(requested_week)}.")

        if card.owner is None:
            for note in card.notes:
                st.caption(f"⚠️ {note}")
            _render_links(card)
            return

        _render_fixture(card)

        if card.actions:
            for action in card.actions:
                callout = (st.error if action.severity == home.ACTION_CRITICAL
                           else st.warning)
                callout(action.headline, icon=action.icon)
        else:
            st.success(
                "Lineup is the best one available, and nobody on the wire "
                "out-projects anyone on the roster.", icon="✅")

        _render_links(card)

        for note in card.notes:
            st.caption(f"⚠️ {note}")


def _render_fixture(card: home.LeagueSummary) -> None:
    """The projected score line, and the probability where there is one.

    Args:
        card: The league being drawn.
    """
    if card.opponent is None or card.opponent_projected is None:
        st.markdown(f"**{card.owner}** · `{card.projected:.1f}` projected")
        return

    row = st.columns([3, 2], vertical_alignment="center")
    row[0].markdown(
        f"**{card.owner}** `{card.projected:.1f}`  \n"
        f"vs **{card.opponent}** `{card.opponent_projected:.1f}`")
    if card.win is None:
        row[1].metric("Projected Margin", f"{card.margin:+.1f}",
                      help="No fitted dispersion, so no probability.")
    else:
        row[1].metric("Win Probability", f"{card.win * 100:.0f}%",
                      f"{card.margin:+.1f}")


def _ordinal(number: int) -> str:
    """``1`` to ``"1st"``. Ordinals read better than "rank 1 of 6" on a card."""
    if 10 <= number % 100 <= 20:
        return f"{number}th"
    return f"{number}{ {1: 'st', 2: 'nd', 3: 'rd'}.get(number % 10, 'th') }"


def _render_standings(cards: Sequence[home.LeagueSummary]) -> None:
    """One table per league, as tabs.

    **Tabs stay in store order while the cards above reorder by urgency.** A tab bar
    is a control, and the app has already paid twice for a control that moved under
    the pointer -- see :func:`components.header.sticky_selectbox`. Store order is
    :func:`auth.visible_leagues`', which is sorted and stable across seasons rather
    than the viewer's own; either would do, and what matters is that it does not move.
    The cards are a readout, so sorting those is free.

    Args:
        cards: In store order, not urgency order.
    """
    st.divider()
    st.subheader("Standings")

    tabled = [card for card in cards
              if card.standings is not None and not card.standings.is_empty()]
    if not tabled:
        st.info(
            "No league here has a `team_stats` artifact, which is what carries the "
            "results a table is built from. It is opt-in because it re-derives a "
            "league's whole history — ~40s for an eleven-season league, 1.5s for a "
            "first-season one.")
        st.code("python -m Scripts.refresh --all --what lineups,team_stats",
                language="bash")
        return

    for tab, card in zip(st.tabs([card.display_name for card in tabled]), tabled):
        with tab:
            # `width="content"` rather than the `"stretch"` every other table in
            # the app uses, and the difference is visible: eight narrow columns
            # stretched across 1430px came out mostly whitespace, with `Projected`
            # -- the column you actually read in September -- pushed to the far
            # edge away from the name it belongs to. Narrowing the columns alone did
            # nothing, because a stretched table redistributes the slack. Found by
            # screenshot; `AppTest` reports the frame a page rendered, never the
            # width it rendered into.
            st.dataframe(card.standings, width="content", hide_index=True,
                         placeholder="", lazy=False,
                         column_config=STANDINGS_CONFIG)
            st.caption(
                f"Week {card.week}. `This Week` is what has been scored and "
                f"`Projected` is our blend for the lineup as set — both, because a "
                f"week in progress is described honestly by neither alone. Ranked on "
                f"win percentage, then points for, then `Projected`, which breaks a "
                f"tie in the first two and before week 1 is the only thing "
                f"separating anybody.")

    missing = [card.display_name for card in cards if card not in tabled]
    if missing:
        st.caption(f"No `team_stats` for {', '.join(missing)}, so no table: "
                   f"`python -m Scripts.refresh --all --what lineups,team_stats`.")


def render_home(selection: session.Selection) -> None:
    """Draw the landing page.

    Args:
        selection: The global selection. Only the season and the week are read --
            Home is the one tab that is *not* about the selected league.
    """
    pending = st.session_state.pop(PENDING_KEY, None)
    if pending:
        st.switch_page(pending)

    viewer = auth.current_viewer()
    leagues = auth.visible_leagues(viewer, store.list_leagues(selection.season))

    st.title("Home")

    cards: List[home.LeagueSummary] = []
    for league_key in leagues:
        try:
            cards.append(summary(
                selection.season, league_key,
                store.load_meta(selection.season, league_key).get("display_name")
                or league_key,
                selection.week, store.version(selection.season, league_key)))
        except FileNotFoundError:
            # A league with a store but no `lineups` -- the pre-draft state. Named
            # rather than dropped, because a league silently missing from your own
            # landing page is how you find out in December that it was never built.
            st.caption(f"⚠️ `{league_key}` has no weekly lineups yet.")

    if not cards:
        st.info("None of your leagues has a weekly lineup artifact yet.")
        st.code("python -m Scripts.refresh --all", language="bash")
        return

    ordered = home.by_urgency(cards)
    urgent = sum(1 for card in ordered
                 for action in card.actions
                 if action.severity == home.ACTION_CRITICAL)
    st.caption(
        f"{len(ordered)} league{'s' if len(ordered) != 1 else ''}, most urgent "
        f"first — {urgent or 'no'} critical "
        f"{'item' if urgent == 1 else 'items'}. A card's highlighted button is the "
        f"tab that can act on it; opening it switches the league selector too.")

    for row_start in range(0, len(ordered), CARD_COLUMNS):
        row = st.columns(CARD_COLUMNS)
        for column, card in zip(row, ordered[row_start:row_start + CARD_COLUMNS]):
            with column:
                _render_card(card, selection.week)

    _render_standings(cards)
