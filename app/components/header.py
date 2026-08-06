"""The sidebar: what league, what week, how fresh, and the refresh button.

Selections live in ``st.session_state`` so they persist as you move between
pages -- every page reads the same ``(season, league_key, week)``.

Freshness is deliberately loud. The failure mode this app exists to avoid is
rendering an hour-old number as though it were live, so the build time, the
per-source coverage and a stale badge are all in the sidebar rather than buried
on a settings page.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import subprocess
import sys
from typing import Dict, List, NamedTuple, Optional

import streamlit as st

import store
from Scripts.config_utils import build_lg_vars
from Scripts.paths import REPO_ROOT

#: Age past which the badge turns red.
STALE_AFTER_MIN = 60


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
    st.title("No store yet")
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

    Session state is managed here rather than handed to the widget via ``key=``
    because these selectors are dependent: the league list depends on the season
    and the week list depends on the league. A widget key holding a value that is
    no longer in ``options`` -- last week's week 14 after switching to a league
    that only has week 1 -- is exactly the case that misbehaves. Falling back to
    the default keeps navigation predictable.

    Args:
        label: Widget label.
        state_key: ``st.session_state`` key to persist the choice under.
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

    chosen = st.selectbox(label, options, index=options.index(remembered),
                          format_func=format_func)
    st.session_state[state_key] = chosen
    return chosen


def render_sidebar() -> Selection:
    """Draw the sidebar and return the current selection.

    Call this once at the top of every page.

    Returns:
        Selection: The resolved season, league, week and metadata.
    """
    configured = _configured_leagues()
    seasons = store.list_seasons()
    if not seasons:
        _no_store_message([])

    with st.sidebar:
        st.markdown("### Fantasy Football")

        season = _sticky_selectbox("Season", "season", seasons)

        built = store.list_leagues(season)
        if not built:
            _no_store_message(seasons)

        league_key = _sticky_selectbox(
            "League", "league_key", built,
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
        _render_missing_leagues(configured, built)

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
    stale = store.is_stale(meta, STALE_AFTER_MIN)
    when = "build time unknown" if age is None else f"built {_format_age(age)}"
    label = f"{when} · week {meta.get('current_week', '?')}"

    if stale:
        st.error(label, icon="⚠️")
    else:
        st.success(label, icon="✅")

    if st.button("Refresh this league", width="stretch",
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
    st.caption("Projection sources (% real, not imputed)")
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


def _render_missing_leagues(configured: Dict[str, str], built: List[str]) -> None:
    """List configured leagues that have no store, with the command to build them.

    Args:
        configured: League key to display name.
        built: League keys that do have a store.
    """
    missing = sorted(set(configured) - set(built))
    if not missing:
        return
    st.divider()
    with st.expander(f"{len(missing)} league(s) not built"):
        st.caption("Not selectable until refreshed.")
        st.code("python -m Scripts.refresh --all", language="bash")
        for key in missing:
            st.write(f"· {configured[key]}")
