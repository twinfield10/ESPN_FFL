"""The sidebar: who is looking, what league, what week, how fresh, and refresh.

Selections live in ``st.session_state`` so they persist as you move between
pages -- every page reads the same ``(season, league_key, week)``.

**The league list is scoped to the viewer**, through :mod:`auth`. There is no login
yet and this module does not implement one; it asks ``auth.current_viewer()`` who is
looking and ``auth.visible_leagues()`` what they may open, so a real identity
provider lands in one function in one module rather than in every page that ever
called ``store.list_leagues``. Nine leagues are configured and five belong to other
owners; the picker offers the four the viewer plays in.

Freshness is deliberately loud. The failure mode this app exists to avoid is
rendering an hour-old number as though it were live, so the build time, the
per-source coverage and a stale badge are all in the sidebar rather than buried
on a settings page.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import subprocess
import sys
from datetime import date
from typing import Dict, List, NamedTuple, Optional

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


class Selection(NamedTuple):
    """What the sidebar resolved to.

    Attributes:
        season: Season year.
        league_key: ``config.yaml`` league key.
        display_name: The league's display name.
        week: Selected week.
        meta: The store's ``meta.json`` payload.
    """
    season: int
    league_key: str
    display_name: str
    week: int
    meta: dict


@st.cache_data(ttl=600, show_spinner=False)
def _configured_leagues() -> Dict[str, str]:
    """League key to display name, from ``config.yaml``.

    Returns:
        dict: ``{league_key: display_name}``.
    """
    return {cfg["key"]: cfg["display_name"] for cfg in build_lg_vars().values()}


def _format_age(minutes: Optional[float]) -> str:
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


def _no_store_message(seasons: List[int]) -> None:
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


def _no_visible_league_message(viewer: auth.Viewer, season: int,
                               configured: Dict[str, str]) -> None:
    """Explain that this viewer's leagues are not built for this season, and stop.

    Distinct from :func:`_no_store_message` on purpose: a store that holds five
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


def _sticky_selectbox(label, state_key, options, default=None, format_func=str):
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
    if st.session_state.get(state_key) not in options:
        st.session_state[state_key] = default if default in options else options[0]

    return st.selectbox(label, options, key=state_key, format_func=format_func)


def render_sidebar() -> Selection:
    """Draw the sidebar and return the current selection.

    Call this once at the top of every page.

    Returns:
        Selection: The resolved season, league, week and metadata.
    """
    configured = _configured_leagues()
    viewer = auth.current_viewer()
    seasons = store.list_seasons()
    if not seasons:
        _no_store_message([])

    with st.sidebar:
        st.markdown("### Fantasy Football")
        st.caption(f"Signed in as **{viewer.display_name}**")

        season = _sticky_selectbox("Season", "season", seasons)

        built = store.list_leagues(season)
        if not built:
            _no_store_message(seasons)

        # The one place the app narrows nine leagues to this viewer's. Everything
        # downstream reads `Selection.league_key`, so nothing else has to know.
        mine = auth.visible_leagues(viewer, built)
        if not mine:
            _no_visible_league_message(viewer, season, configured)

        league_key = _sticky_selectbox(
            "League", "league_key", mine,
            default=auth.default_league(viewer, mine),
            format_func=lambda k: configured.get(k, k),
        )

        meta = store.load_meta(season, league_key)
        display_name = meta.get("display_name") or configured.get(league_key, league_key)

        weeks = meta.get("weeks_present") or [meta.get("current_week") or 1]
        current_week = meta.get("current_week") or weeks[-1]
        week = _sticky_selectbox(
            "Week", "week", weeks,
            default=current_week if current_week in weeks else weeks[-1],
        )

        _render_freshness(meta, season, display_name)
        _render_coverage(meta)
        _render_missing_leagues(viewer, configured, mine)

    return Selection(season=season, league_key=league_key,
                     display_name=display_name, week=week, meta=meta)


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
    when = "Build Time Unknown" if age is None else f"Built {_format_age(age)}"
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


def _render_coverage(meta: dict) -> None:
    """Per-source projection coverage, so a dead source cannot hide.

    The blend imputes a missing source from the ESPN/FantasyPros mean, which
    makes an absent book look like agreement rather than absence. These numbers
    are what distinguish the two.

    Args:
        meta: The store's ``meta.json``.
    """
    overall = (meta.get("coverage") or {}).get("overall") or {}
    if not overall:
        return

    st.divider()
    st.caption("Projection Sources (% Real, Not Imputed)")
    for source in ("ESPN", "FP", "PINNY", "BOL"):
        if source not in overall:
            continue
        pct = overall[source]
        st.progress(min(max(pct / 100.0, 0.0), 1.0), text=f"{source} {pct:.0f}%")

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
