"""The sidebar: how fresh the store is, what it covers, and how to rebuild it.

**The league, season and week selectors are no longer here.** They moved to
:mod:`session`, which draws them in the app's body as one context row above every
tab -- see that module for why. What is left is the half of the old sidebar that was
never a selection: build time, the stale badge, per-source coverage, the refresh
button and this viewer's unbuilt leagues.

Freshness is deliberately loud. The failure mode this app exists to avoid is
rendering an hour-old number as though it were live, so the build time, the
per-source coverage and a stale badge are all on screen rather than buried on a
settings page.

Two helpers stay here because they are the sidebar's own vocabulary and are reused
by the context row: :func:`sticky_selectbox`, which is the only dropdown primitive
in the app and carries the account of two bugs that each rendered the wrong league,
and :func:`stale_after_minutes`, which knows that pre-season and game-day are
different questions.

Which leagues a viewer may open is decided in :mod:`auth`, not here.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import subprocess
import sys
from datetime import date
from typing import Dict, List, Optional

import streamlit as st

import auth
import store
from Scripts.config_utils import build_lg_vars
from Scripts.paths import REPO_ROOT
from Scripts.usage.features import SEASON_START

#: Age past which the badge turns red **in season**, in minutes.
#:
#: An hour, and it is short on purpose. In season this badge is not asking "is the
#: data recent", it is asking "did you refresh before locking a lineup" -- injury
#: news an hour before kickoff is the entire reason the app renders a build time at
#: all. A badge that goes red an hour after your last refresh is doing its job.
STALE_AFTER_MIN_IN_SEASON = 60

#: Age past which the badge turns red **before week 1**, in minutes.
#:
#: 25 hours, matching ``run_daily_refresh.sh``'s 6am cron and
#: :data:`Scripts.refresh_status.DEFAULT_MAX_AGE_HOURS`. Pre-season there is nothing
#: to do between nightly runs: the depth chart moves once a day, no games are being
#: played, and no lineup is being locked. Holding the in-season hour here would paint
#: the badge red 23 hours out of 24 for a month, and a badge that is always red is
#: one nobody reads -- which costs you the one week in September when it means
#: something.
#:
#: The extra hour over 24 is slack for a slow run, not tolerance for a skipped one.
STALE_AFTER_MIN_PRE_SEASON = 25 * 60


def stale_after_minutes(season: int, today: Optional[date] = None) -> int:
    """The staleness threshold appropriate to where the season is.

    Two cadences, so two numbers. See :data:`STALE_AFTER_MIN_IN_SEASON` and
    :data:`STALE_AFTER_MIN_PRE_SEASON` for why one constant cannot serve both.

    The boundary is :data:`Scripts.usage.features.SEASON_START`, reused rather than
    redeclared -- it is the same "when does the season start" the age feature already
    measures against, and it is approximate there for the same reason it can be
    approximate here. Nothing turns on being a few days out; what turns on it is not
    holding a game-day threshold through August.

    Args:
        season: Season year the store is for.
        today: Overridable for tests. Defaults to the actual date.

    Returns:
        int: Minutes.
    """
    today = date.today() if today is None else today
    opener = date(season, *SEASON_START)
    return (STALE_AFTER_MIN_PRE_SEASON if today < opener
            else STALE_AFTER_MIN_IN_SEASON)


@st.cache_data(ttl=600, show_spinner=False)
def configured_leagues() -> Dict[str, str]:
    """League key to display name, from ``config.yaml``.

    Narrowed to the two fields the UI needs on purpose. ``config.yaml`` also holds
    live ESPN and FantasyPros cookies, so no caller gets the whole dict.

    Public because :mod:`session` needs the same map for the league picker, and one
    cached reader beats two.

    Returns:
        dict: ``{league_key: display_name}``.
    """
    return {cfg["key"]: cfg["display_name"] for cfg in build_lg_vars().values()}


def format_age(minutes: Optional[float]) -> str:
    """Render a store age the way you'd say it out loud.

    Args:
        minutes: Age in minutes, or None when unknown. Callers handle None
            themselves -- see :func:`_render_freshness`, which needs a phrase that
            reads without the leading "built".

    Returns:
        str: e.g. ``"14 min ago"``, ``"3.2 h ago"``, ``"unknown"``.
    """
    if minutes is None:
        return "unknown"
    if minutes < 1:
        return "just now"
    if minutes < 90:
        return f"{int(round(minutes))} min ago"
    if minutes < 48 * 60:
        return f"{minutes / 60:.1f} h ago"
    return f"{minutes / 1440:.1f} days ago"


def no_store_message(seasons: List[int]) -> None:
    """Explain how to build a store, and stop the page.

    This is the state the app launches in, so it gets a real message rather than
    a traceback.

    Args:
        seasons: Seasons found on disk, if any.
    """
    st.title("No Store Yet")
    st.markdown(
        "The app only reads `Data/Store`, and there is nothing in it for any "
        "league. Building it is an explicit step because it costs seconds per "
        "league of ESPN round-trips."
    )
    st.code(
        "# one league, fastest way to see something\n"
        "python -m Scripts.refresh --league Knights_FFL\n\n"
        "# every league in config.yaml\n"
        "python -m Scripts.refresh --all",
        language="bash",
    )
    if seasons:
        st.caption(f"Seasons with a partial store on disk: {seasons}")
    st.stop()


def no_visible_league_message(viewer: auth.Viewer, season: int,
                               configured: Dict[str, str]) -> None:
    """Explain that this viewer's leagues are not built for this season, and stop.

    Distinct from :func:`no_store_message` on purpose: a store that holds five
    other owners' leagues and none of yours is not an empty store, and telling you
    to run ``--all`` would be answering a question you did not ask.

    Args:
        viewer: The current viewer.
        season: Season year.
        configured: League key to display name.
    """
    st.title("No Leagues for You in This Season")
    names = [configured.get(key, key) for key in viewer.leagues]
    st.markdown(
        f"`{season}` has a store, but none of it is yours. Signed in as "
        f"**{viewer.display_name}**, whose leagues are: "
        + ", ".join(f"**{name}**" for name in names) + "."
    )
    st.code(
        "\n".join(f"python -m Scripts.refresh --league {configured.get(key, key)} "
                  f"--season {season}" for key in viewer.leagues),
        language="bash",
    )
    st.caption(
        f"To browse every configured league instead, set `{auth.ALL_LEAGUES_ENV}=1` "
        f"before starting the app."
    )
    st.stop()


def _run_refresh(display_name: str, season: int) -> None:
    """Shell out to the refresh CLI, streaming its output.

    A subprocess on purpose: the ingest path is seconds of blocking ESPN calls,
    and running it inside a Streamlit rerun would freeze the whole session.
    ``cwd`` is the repo root because modules import as ``Scripts.*``.

    Args:
        display_name: League to refresh.
        season: Season year.
    """
    cmd = [sys.executable, "-m", "Scripts.refresh",
           "--league", display_name, "--season", str(season)]
    with st.status(f"Refreshing {display_name} {season}…", expanded=True) as status:
        st.caption(" ".join(cmd))
        output = st.empty()
        lines: List[str] = []
        process = subprocess.Popen(
            cmd, cwd=REPO_ROOT, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            lines.append(line.rstrip())
            # Tail only: a full run prints the whole coverage report and every
            # unmatched player name.
            output.code("\n".join(lines[-25:]))
        code = process.wait()

        if code == 0:
            status.update(label=f"Refreshed {display_name} {season}", state="complete")
        else:
            status.update(label=f"Refresh failed (exit {code})", state="error")

    if code == 0:
        # store_mtime changed, so the cached readers miss and re-read on the
        # rerun. No cache_data.clear() needed.
        st.rerun()


def sticky_selectbox(label, state_key, options, default=None, format_func=str):
    """A selectbox whose value survives page navigation and changing options.

    **The widget owns ``state_key``.** That is the fix for a bug that ate every
    second league change: the previous version passed no ``key=`` and steered the
    widget with ``index=``, and a keyless widget's identity is derived from its
    arguments -- ``index`` among them. Switching leagues changed the remembered
    value, which changed ``index`` on the next run, which minted a *new* widget id;
    the selection the user had just made was recorded against the old id and thrown
    away. Winfield → GOP worked, GOP → Knights silently did not, and the sidebar
    showed the league you had left.

    What that version was defending against is real and is still handled, just
    earlier: these selectors are dependent -- the league list depends on the season,
    the week list on the league -- so a remembered value can stop being offered, and
    a widget key holding a value that is not in ``options`` is what misbehaves. It is
    corrected *before* the widget registers, which is the only point at which
    Streamlit allows the write.

    **The write is unconditional, and that is the second half of the fix.**
    Streamlit discards a widget's state when you navigate to a page that has not
    rendered it, so on the first run of a newly-opened page the key is dropped and
    the widget falls back to its first option -- even though ``session_state`` still
    reads correctly at the top of the script. Writing only when the remembered value
    was *invalid* therefore fixed nothing on navigation: the value was perfectly
    valid, nothing touched the key, and opening the Draft Board from the Store
    Overview silently moved you from Winfield_Football to GOP_Degenerates. Touching
    the key every run is Streamlit's documented "keep" pattern and is what carries a
    selection across pages.

    Args:
        label: Widget label.
        state_key: ``st.session_state`` key the widget stores its choice under.
        options: Selectable values. Must be non-empty.
        default: Value to select when nothing valid is remembered. Defaults to
            the first option.
        format_func: Display formatter.

    Returns:
        The selected value.
    """
    options = list(options)
    remembered = st.session_state.get(state_key)
    if remembered not in options:
        remembered = default if default in options else options[0]
    st.session_state[state_key] = remembered

    return st.selectbox(label, options, key=state_key, format_func=format_func)


def render_sidebar_health(selection) -> None:
    """Draw the sidebar: who is looking, how fresh the store is, and how to rebuild.

    Called once from ``main.py``, after :func:`session.render_context` has resolved
    which league we are looking at. It draws no selectors -- see the module
    docstring.

    Args:
        selection: A :class:`session.Selection`. Typed loosely because
            :mod:`session` imports this module, so naming the class here would be a
            cycle. Reads ``.season``, ``.display_name`` and ``.meta``.
    """
    viewer = auth.current_viewer()
    with st.sidebar:
        st.markdown("### Fantasy Football")
        st.caption(f"Signed in as **{viewer.display_name}**")

        _render_freshness(selection.meta, selection.season, selection.display_name)
        _render_coverage(selection.meta)

        built = store.list_leagues(selection.season)
        _render_missing_leagues(viewer, configured_leagues(),
                                auth.visible_leagues(viewer, built))


def _render_freshness(meta: dict, season: int, display_name: str) -> None:
    """Build time, staleness badge and the refresh button.

    Args:
        meta: The store's ``meta.json``.
        season: Season year.
        display_name: League display name, passed to the refresh CLI.
    """
    st.divider()
    age = store.store_age_minutes(meta)
    threshold = stale_after_minutes(season)
    stale = store.is_stale(meta, threshold)
    when = "Build Time Unknown" if age is None else f"Built {format_age(age)}"
    label = f"{when} · Week {meta.get('current_week', '?')}"

    if stale:
        st.error(label, icon="⚠️")
    else:
        st.success(label, icon="✅")

    # Which clock is running, so a green badge at 14 hours old is not read as a bug.
    # Pre-season the nightly cron is the cadence and this is really reporting on it,
    # so it names the check that gives the fuller answer.
    if threshold >= STALE_AFTER_MIN_PRE_SEASON:
        st.caption("Pre-season: refreshed nightly at 6am. "
                   "`python -m Scripts.refresh_status` says whether it ran.")

    if st.button("Refresh This League", width="stretch",
                 help="Runs Scripts.refresh in a subprocess. Seconds of ESPN "
                      "round-trips, which is why it is not automatic."):
        _run_refresh(display_name, season)


#: Sources in reading order. Anything the store reports that is not named here is
#: appended, so a source registering weekly appears without editing this file.
COVERAGE_ORDER = ("ESPN", "FP", "PINNY", "BOL", "ATH", "USG")

#: How a source prefix is labelled on the sidebar.
COVERAGE_LABELS = {"FP": "FantasyPros", "PINNY": "Pinnacle", "BOL": "BetOnline",
                   "ATH": "The Athletic", "USG": "TOMCAT"}


def _render_coverage(meta: dict) -> None:
    """Per-source projection coverage, so a dead source cannot hide.

    The blend imputes a missing source from the ESPN/FantasyPros mean, which
    makes an absent book look like agreement rather than absence. These numbers
    are what distinguish the two.

    **Reads ``players`` in preference to ``overall``, and the difference is the
    whole point of this panel.** ``overall`` averages a source's real-cell share
    over every stat column it carries -- including the thirty-odd a source
    structurally never publishes -- so it answered a question about cells while the
    label claimed a question about players. Two consequences, both measured on the
    2026 stores on 2026-09-08: FantasyPros read **12.4%** when it had a real line for
    **21.8%** of the players in the league, and **Pinnacle and BetOnline both read
    4.1% for sources with no weekly line whatsoever** -- 4.1% being exactly the two
    derived columns (``_Points``, ``_PosRank``) that have no provenance flag and so
    counted as always real. A store built before that fix has no ``players`` key and
    falls back to the old number rather than showing nothing.

    Args:
        meta: The store's ``meta.json``.
    """
    coverage = meta.get("coverage") or {}
    players = coverage.get("players") or {}
    shown = players or coverage.get("overall") or {}
    if not shown:
        return

    st.divider()
    st.caption("Projection Sources (% of Players With a Real Line)" if players
               else "Projection Sources (% Real, Not Imputed)")

    ordered = [s for s in COVERAGE_ORDER if s in shown]
    ordered += [s for s in sorted(shown) if s not in COVERAGE_ORDER]
    for source in ordered:
        pct = shown[source]
        st.progress(min(max(pct / 100.0, 0.0), 1.0),
                    text=f"{COVERAGE_LABELS.get(source, source)} {pct:.0f}%")

    population = coverage.get("population") or {}
    if players and population.get("rows"):
        rostered = population.get("rostered")
        per_position = population.get("free_agents_per_position")
        detail = f"{population['rows']} players"
        if rostered is not None and per_position:
            detail = (f"{population['rows']} players — {rostered} rostered plus the "
                      f"top {per_position} free agents at each position")
        st.caption(
            f"Measured over {detail}. Individual defensive players are excluded: no "
            f"source but ESPN publishes a line for them."
        )

    absent = [name for name, present
              in (meta.get("weekly_sources_present") or {}).items() if not present]
    if absent:
        st.caption(
            f"⚠️ no weekly props this season for {', '.join(sorted(absent))} — "
            f"those columns are the ESPN/FP mean and are dropped from the blend."
        )


def _render_missing_leagues(viewer: auth.Viewer, configured: Dict[str, str],
                            visible: List[str]) -> None:
    """List *this viewer's* leagues that have no store, with the build command.

    Scoped to the viewer for the same reason the picker is: a list of five other
    owners' unbuilt leagues is noise on a sidebar whose job is to say whether what
    you are looking at is current.

    Args:
        viewer: The current viewer.
        configured: League key to display name.
        visible: League keys the viewer may open that do have a store.
    """
    expected = set(viewer.leagues) & set(configured) if viewer.leagues else set(configured)
    missing = sorted(expected - set(visible))
    if not missing:
        return
    st.divider()
    with st.expander(f"{len(missing)} of Your Leagues Not Built"):
        st.caption("Not selectable until refreshed.")
        st.code("python -m Scripts.refresh --all", language="bash")
        for key in missing:
            st.write(f"· {configured[key]}")


# The private spellings these four had before the context row moved out of the
# sidebar. Kept because ``tests/test_header_selection.py`` pins ``sticky_selectbox``
# under its old name, and that test is the record of two bugs that each silently
# rendered the wrong league -- renaming it would be editing the evidence.
_sticky_selectbox = sticky_selectbox
_format_age = format_age
_no_store_message = no_store_message
_no_visible_league_message = no_visible_league_message
_configured_leagues = configured_leagues
