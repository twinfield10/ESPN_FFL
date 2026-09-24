"""Player Shares — the layout. What :mod:`player_shares` decides, drawn.

The IO lives here rather than in the logic module, the same split
:func:`views.home_tab.summary` uses: this function reads the store and caches on
``store.version``, so the sidebar's Refresh button invalidates it, and
:mod:`player_shares` stays a pure function of loaded frames.

**This is a section of Home, not a tab of its own.** It was the second tab that
ignored the League selector, and that was the tell: Home reads the same
:class:`session.Selection` -- season and week, never a league -- and asks the
neighbouring question. Which of my leagues needs me before kickoff, and then, once
the lineups are set, who do I want the ball to go to. :func:`views.home_tab.render_home`
draws it between the league cards and Standings. The selector stays live because it
governs the other four tabs, and the section says in one line that it does not apply
here, so a control that looks inert is explained rather than puzzling.

Leagues are listed in **store order** -- sorted, stable across seasons, which is
what :func:`auth.visible_leagues` preserves. Only the charts are reordered.

**Two charts and a table rather than one long ranked list.** The page answers two
questions -- who do I want the ball to go to, and who do I want it kept from -- and
splitting them means each chart is a *single series* with one hue, so neither
needs a legend and its title carries the identity. The full ranking is still here,
one expander down: a chart showing ten of fifty-eight players is a summary, and the
reader has to be able to get past it.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import html
from typing import List, Optional, Sequence, Tuple

import altair as alt
import pandas as pd
import polars as pl
import streamlit as st

import auth
import draft_view as dv
import home
import matchup_sim as sim
import player_shares as ps
import session
import store

#: How many players each chart draws. Ten keeps the bars thicker than the 4px
#: corner radius they carry, and below it the rows are a rounding difference apart.
CHART_ROWS = 10

#: How many league cards sit in a row before wrapping. Four is what this viewer
#: has, and the strip is a readout rather than Home's grid of decisions -- but
#: ``st.columns(len(matchups))`` would squeeze a fifth league into an unreadable
#: sliver rather than starting a second row, and a viewer gaining a league is a
#: config edit rather than a rewrite.
LEAGUE_COLUMNS = 4

#: Which slots of the app's categorical palette carry the two directions.
#:
#: **Reused rather than redefined.** ``draft_view`` already owns the one palette
#: this app paints charts from, per theme, and one meaning per colour across every
#: chart is the property worth keeping -- green is *with you* on the Roster tab's
#: ``IN`` marks and in the Matchup tab's ``ADV`` fills, and must not mean something
#: else here.
#:
#: **The pair was measured, because green-against-red is the textbook colour-vision
#: trap.** On the light surface it separates at **ΔE 7.2** (protan), inside the 6-8
#: band that is legal *only* alongside a second encoding, and clears 3:1 contrast;
#: on the dark surface it passes outright at **ΔE 8.6**. The second encoding is
#: carried three times over -- the directions are **separate charts** with their own
#: titles and axes, **every bar is labelled with its own number**, and the full
#: table sits below -- so nothing on this page asks a reader to tell the two apart
#: by hue. A clean-passing blue/red was available at ΔE 21.6 and was **not** taken:
#: it would have made this the one chart in the app where green does not mean good.
FOR_SLOT = 6
AGAINST_SLOT = 8

#: Alpha ceiling for the signed fill on a table's stake column. Matches the weight
#: :data:`lineup_table.CSS` uses for its own points fills, so the tables read as one
#: system rather than two.
FILL_ALPHA = 0.28

#: The stake, in expected wins, that saturates that fill. A tenth of a win is a
#: genuinely big weekly rooting interest -- the largest on week 2 was 0.53 -- so
#: scaling to the largest stake *on the page* would make a quiet week look loud.
#: A fixed ruler keeps two weeks comparable.
FILL_FULL_SCALE = 0.30

#: Theme-neutral rgb for the table fills, the same two directions as the slots.
FOR_RGB = "34, 160, 90"
AGAINST_RGB = "200, 60, 60"

CSS = """
<style>
.psh { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
.psh th, .psh td { padding: 4px 8px; text-align: right; white-space: nowrap;
                   border-bottom: 1px solid rgba(128,128,128,0.15); }
.psh th { font-weight: 600; font-size: 0.78rem; text-align: right;
          color: rgba(128,128,128,0.95); border-bottom: 1px solid rgba(128,128,128,0.35); }
.psh td.psh-l, .psh th.psh-l { text-align: left; }
.psh td.psh-num { font-weight: 600; }
.psh td.psh-dim { color: rgba(128,128,128,0.85); font-size: 0.85rem; }
.psh tr:hover td { background: rgba(128,128,128,0.06); }
.psh-wrap { overflow-x: auto; }
.psh-chip { display: inline-block; padding: 0 5px; margin-right: 3px;
            border-radius: 3px; font-size: 0.74rem;
            background: rgba(128,128,128,0.16); }
</style>
"""


@st.cache_data(ttl=store.CACHE_TTL, show_spinner="Reading your leagues…")
def _matchup(season: int, league_key: str, display_name: str,
             viewer: auth.Viewer, requested_week: Optional[int], version: str):
    """One league's fixture, read and priced.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        display_name: The league's name.
        viewer: Who is looking. A :class:`auth.Viewer` is a NamedTuple of strings
            and tuples, so it hashes and can be part of the cache key.
        requested_week: The globally selected week, resolved against this league's
            own by :func:`home.week_for`.
        version: From :func:`store.version`. Cache key only -- the same trick
            :func:`store._load_artifact` uses, and the reason a refresh invalidates
            this too.

    Returns:
        tuple: ``(matchup, notes, week)``. The matchup is None when this league
        cannot contribute and ``notes`` says why.
    """
    del version                              # cache key only

    meta = store.load_meta(season, league_key)
    week = home.week_for(meta, requested_week)
    lineups = store.load_lineups(season, league_key)
    team_stats = (store.load_team_stats(season, league_key)
                  if store.has_artifact(season, league_key, "team_stats") else None)

    owners = session.team_owners(lineups.filter(pl.col("week") == week))
    owner = auth.owner_for(viewer, owners)
    if owner is None:
        return None, [f"**{display_name}**: you do not have a team in this "
                      f"league."], week

    matchup, notes = ps.league_matchup(
        league_key=league_key, display_name=display_name, owner=owner, week=week,
        meta=meta, lineups=lineups, team_stats=team_stats, fitted=sim.model(),
        free_agent_owner=session.FREE_AGENT_OWNER)
    return matchup, notes, week


def _theme() -> Tuple[dict, dict]:
    """The series palette and chart ink for the viewer's theme.

    Returns:
        tuple: ``(colors, ink)`` from :mod:`draft_view` -- the same two the draft
        charts are painted from, so a theme switch moves every chart together.
    """
    name = getattr(getattr(st.context, "theme", None), "type", "light") or "light"
    return dv.SERIES_COLORS[name], dv.CHART_INK[name]


def _chart(rows: Sequence[ps.Share], *, colour: str, ink: dict,
           axis_title: str) -> alt.LayerChart:
    """One direction's top players, as a horizontal bar chart.

    Horizontal because the label is a player's name, and a vertical chart would
    stand ten of them on end. Sorted by magnitude, which is both the order the
    reader wants and the order the list was truncated in.

    **Magnitude on the axis; direction in the title and the hue.** Plotting the
    signed number would send one chart's bars leftward and leave the two unable to
    be compared by length, which is the one thing two bar charts side by side are
    for. The signed value is in the tooltip, and the sign is never the only thing
    separating the charts: they carry different titles and different axis labels.

    Args:
        rows: Shares for one direction, already filtered and truncated.
        colour: Bar fill, from the app's validated palette.
        ink: Chart ink for the theme.
        axis_title: What the magnitude means, in words.

    Returns:
        alt.LayerChart: Bars plus their direct labels.
    """
    frame = pd.DataFrame([{
        "Player": share.player_name,
        "Magnitude": abs(share.stake),
        "Stake": share.stake,
        "Rate": share.rate * 100,
        "Pos": share.position or "—",
        "Team": share.pro_team or "—",
        "Leagues": ", ".join(share.owned or share.faced) or "—",
        "Label": f"{abs(share.stake):.3f}",
    } for share in rows])

    # An explicit order rather than `sort="-x"`, so the label layer cannot end up
    # ordering its own axis differently from the bars it is annotating.
    order = list(frame["Player"])

    base = alt.Chart(frame).encode(
        y=alt.Y("Player:N", sort=order, title=None,
                axis=alt.Axis(labelColor=ink["text"], labelLimit=170,
                              domain=False, ticks=False)),
        x=alt.X("Magnitude:Q", title=axis_title,
                axis=alt.Axis(gridColor=ink["grid"], labelColor=ink["muted"],
                              titleColor=ink["muted"], domain=False,
                              format=".2f", tickCount=4)),
    )
    bars = base.mark_bar(cornerRadiusEnd=4, stroke=None, color=colour).encode(
        tooltip=[alt.Tooltip("Player:N"),
                 alt.Tooltip("Pos:N"),
                 alt.Tooltip("Team:N"),
                 alt.Tooltip("Stake:Q", title="Δ Wins", format="+.3f"),
                 alt.Tooltip("Rate:Q", title="%/Pt", format="+.2f"),
                 alt.Tooltip("Leagues:N")],
    )
    # The labels are not decoration: the light palette puts this pair in the 6-8
    # colour-vision band, where a second encoding is the condition of using it at
    # all. Text ink rather than the bar's colour, which is the app's own rule --
    # a number wearing the series hue is a colour doing a label's job.
    labels = base.mark_text(align="left", dx=5, fontSize=11, fontWeight=600,
                            color=ink["text"]).encode(text="Label:N")

    return (bars + labels).properties(
        height=alt.Step(26),
        padding={"left": 0, "top": 0, "bottom": 0, "right": 34})


def _fill(stake: float) -> str:
    """The signed background for a stake cell.

    ``rgba`` with an alpha rather than a literal ink colour, which is what lets
    :data:`lineup_table.CSS` follow a theme switch with no ``theme`` argument. Same
    convention, so these tables sit with the app's others.

    Args:
        stake: Signed Δ expected wins.

    Returns:
        str: A ``style`` attribute, or the empty string for a negligible stake.
    """
    if abs(stake) < ps.HEDGE_EPSILON:
        return ""
    weight = min(1.0, abs(stake) / FILL_FULL_SCALE) * FILL_ALPHA
    rgb = FOR_RGB if stake > 0 else AGAINST_RGB
    return f' style="background: rgba({rgb}, {weight:.3f})"'


def _chips(names: Tuple[str, ...]) -> str:
    """League names as chips, or an em dash.

    Args:
        names: Display names.

    Returns:
        str: HTML.
    """
    if not names:
        return '<span class="psh-dim">—</span>'
    return "".join(f'<span class="psh-chip">{html.escape(n)}</span>'
                   for n in names)


def _verdict(share: ps.Share) -> str:
    """What to do about a player, in words.

    So the conflicts table never leaves a colour or a sign carrying the reading on
    its own -- and so a netted-to-nothing row says *a wash* rather than looking
    like a very small opinion.

    Args:
        share: From :func:`player_shares.shares`.

    Returns:
        str: The reading.
    """
    if share.hedged:
        return "A wash"
    return "Root for" if share.stake > 0 else "Root against"


def _table(rows: Sequence[ps.Share], modelled: bool,
           verdict: bool = False) -> str:
    """A ranked table of shares.

    Hand-written HTML rather than ``st.dataframe`` for the same reason
    :mod:`lineup_table` is: this carries a signed fill and chips inside a cell, and
    the dataframe widget can do neither.

    Args:
        rows: Shares, already sorted and truncated.
        modelled: Whether there is a fitted dispersion. Without one the stake and
            rate columns give way to net projected points.
        verdict: Whether to carry the in-words reading. On for the conflicts table,
            where the number alone understates how odd the row is.

    Returns:
        str: A complete ``<table>``.
    """
    head = ("<th class='psh-l'>Player</th><th class='psh-l'>Pos</th>"
            "<th class='psh-l'>Team</th><th class='psh-l'>Game</th>")
    head += ("<th>Δ Wins</th><th>%/Pt</th>" if modelled else "<th>Net Proj</th>")
    if verdict:
        head += "<th class='psh-l'>Reading</th>"
    head += "<th class='psh-l'>For</th><th class='psh-l'>Against</th>"

    body = []
    for share in rows:
        cells = [
            f"<td class='psh-l'>{html.escape(share.player_name)}</td>",
            f"<td class='psh-l psh-dim'>{html.escape(share.position or '')}</td>",
            f"<td class='psh-l psh-dim'>{html.escape(share.pro_team or '')}</td>",
            f"<td class='psh-l psh-dim'>{html.escape(share.game_state or '')}</td>",
        ]
        if modelled:
            cells.append(f"<td class='psh-num'{_fill(share.stake)}>"
                         f"{share.stake:+.3f}</td>")
            cells.append(f"<td class='psh-dim'>{share.rate * 100:+.2f}</td>")
        else:
            cells.append(f"<td class='psh-num'>{ps.net_projected(share):+.1f}</td>")
        if verdict:
            cells.append(f"<td class='psh-l'>{_verdict(share)}</td>")
        cells.append(f"<td class='psh-l'>{_chips(share.owned)}</td>")
        cells.append(f"<td class='psh-l'>{_chips(share.faced)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")

    return (f"{CSS}<div class='psh-wrap'><table class='psh'><thead><tr>{head}"
            f"</tr></thead><tbody>{''.join(body)}</tbody></table></div>")


def _render_leagues(matchups: List[ps.LeagueMatchup]) -> None:
    """One card per league: who you play, and where it stands.

    In **store order** rather than by how close the matchup is -- sorted and stable
    across seasons, the same rule Home follows for its tab bar. Only the charts
    below are reordered.

    Args:
        matchups: In store order.
    """
    for start in range(0, len(matchups), LEAGUE_COLUMNS):
        row = matchups[start:start + LEAGUE_COLUMNS]
        # Padded to a full row so two leagues do not each get half the page.
        for column, matchup in zip(st.columns(LEAGUE_COLUMNS), row):
            with column:
                with st.container(border=True):
                    margin = matchup.mine.projected - matchup.theirs.projected
                    st.markdown(f"**{matchup.display_name}**")
                    st.caption(f"vs {matchup.opponent}")
                    if matchup.win is None:
                        st.metric("Margin", f"{margin:+.1f}")
                    else:
                        st.metric("Win Probability", f"{matchup.win:.0%}",
                                  f"{margin:+.1f} pts", delta_color="off")
                    st.caption(
                        f"{matchup.mine.projected:.1f} ± {matchup.mine.sd:.1f} · "
                        f"{matchup.theirs.projected:.1f} ± {matchup.theirs.sd:.1f}")


def _render_charts(rows: List[ps.Share]) -> None:
    """The two directions, side by side.

    Args:
        rows: Every share, already sorted by magnitude.
    """
    colors, ink = _theme()
    good = [s for s in rows if s.stake > 0][:CHART_ROWS]
    bad = [s for s in rows if s.stake < 0][:CHART_ROWS]

    left, right = st.columns(2)
    for column, subset, slot, heading, axis in (
            (left, good, FOR_SLOT, "Root For", "Δ Wins He Adds"),
            (right, bad, AGAINST_SLOT, "Root Against", "Δ Wins He Costs You")):
        with column:
            st.markdown(f"##### {heading}")
            if not subset:
                st.caption("Nobody this week.")
                continue
            st.altair_chart(
                _chart(subset, colour=colors[slot], ink=ink, axis_title=axis)
                .configure_view(strokeWidth=0)
                .configure_scale(bandPaddingInner=0.25),
                width="stretch")


def _render_conflicts(rows: List[ps.Share]) -> None:
    """Players the viewer is on both sides of.

    The part of this tab that exists nowhere else in the app, and the reason it was
    built: a player you own in one league and face in another is not a rooting
    interest until the two are netted. Neither chart above shows him at his gross
    worth, which is the point -- they rank him on the net.

    Args:
        rows: Every share.
    """
    st.markdown("##### Conflicts")
    conflicted = [s for s in rows if s.conflicted]
    if not conflicted:
        st.caption("No player is on both sides of your week.")
        return

    st.caption(
        f"{len(conflicted)} player{'s' if len(conflicted) != 1 else ''} you own in "
        f"one league and face in another. **Δ Wins is what is left after the two "
        f"sides cancel** — a big interest on both sides can net to nothing, and "
        f"that is a real answer rather than a missing one.")
    st.html(_table(conflicted, modelled=True, verdict=True))


def _render_detail(rows: List[ps.Share]) -> None:
    """The per-league arithmetic behind a player's total.

    Where a *rate* is unambiguous: within one league a point is a point, so this is
    the one place the per-point number can be read without the caveat the summed
    column carries.

    Args:
        rows: Every share.
    """
    with st.expander("Where a player's number comes from"):
        named = {s.player_name: s for s in rows}
        pick = st.selectbox("Player", list(named), key="shares_detail")
        share = named[pick]
        st.caption(
            f"Δ Wins is the sum of the per-league stakes below. Each one recomputes "
            f"that league's win probability with **{share.player_name}** taken out "
            f"of both the projection and the spread, then put back at his p10 and "
            f"his p90.")
        st.dataframe(
            [{"League": i.display_name,
              "Side": "Yours" if i.sign > 0 else "Opponent's",
              "Proj": round(i.projected, 2),
              "SD": round(i.sd, 2),
              "%/Pt": round(i.rate * 100, 2),
              "Δ Wins": round(i.stake, 4)}
             for i in share.interests],
            hide_index=True, width="stretch")


def render_shares(selection: session.Selection) -> None:
    """Draw the Player Shares section of Home.

    Called by :func:`views.home_tab.render_home` above :func:`_render_standings`,
    and it returns early -- after its heading, so the message is labelled -- when no
    league has both a lineup and a fixture. Standings still draws below either way.

    Args:
        selection: The global selection. Only the season and the week are read --
            this section spans every league the viewer has.
    """
    viewer = auth.current_viewer()
    leagues = auth.visible_leagues(viewer, store.list_leagues(selection.season))

    st.divider()
    st.subheader(f"Player Shares · Week {selection.week}")

    matchups: List[ps.LeagueMatchup] = []
    notes: List[str] = []
    for league_key in leagues:
        try:
            matchup, league_notes, _ = _matchup(
                selection.season, league_key,
                store.load_meta(selection.season, league_key).get("display_name")
                or league_key,
                viewer, selection.week,
                store.version(selection.season, league_key))
        except FileNotFoundError:
            # A league with a store but no `lineups` -- the pre-draft state. Named
            # rather than dropped: a league silently missing from a cross-league
            # total is how a wrong number looks completely normal.
            notes.append(f"**{league_key}** has no weekly lineups yet.")
            continue
        notes.extend(league_notes)
        if matchup is not None:
            matchups.append(matchup)

    if not matchups:
        st.info("None of your leagues has both a lineup and a fixture this week.")
        for note in notes:
            st.caption(f"⚠️ {note}")
        st.code(f"python -m Scripts.refresh --all --season {selection.season} "
                f"--what lineups,team_stats", language="bash")
        return

    fitted = sim.model()
    rows = ps.shares(matchups, fitted)
    total = ps.expected_wins(matchups)

    left, right = st.columns([1, 3])
    with left:
        if total is None:
            st.metric("Leagues", f"{len(matchups)}")
        else:
            st.metric("Expected Wins", f"{total:.2f}", f"of {len(matchups)}",
                      delta_color="off")
    with right:
        st.caption(
            f"Every player started in your {len(matchups)} matchups this week, "
            f"ranked by how much he moves that total. **Δ Wins** is how far your "
            f"expected wins travel if he has a p90 week instead of a p10 one — "
            f"positive means root for him. Like the rest of Home this spans all your "
            f"leagues, so the League selector in the sidebar does not apply to it.")

    _render_leagues(matchups)

    modelled = any(m.win is not None for m in matchups)
    if modelled:
        _render_charts(rows)
    else:
        # No probability means no Δ Wins to chart. Net projected points is the
        # honest fallback and a table is the honest shape for it.
        st.markdown("##### Rooting Interest")
        st.caption("No fitted dispersion, so there is no probability to chart. "
                   "This is net projected points, owned minus faced.")
        st.html(_table(rows[:CHART_ROWS * 2], modelled=False))

    _render_conflicts(rows)

    with st.expander(f"All {len(rows)} started players"):
        st.html(_table(rows, modelled))
    _render_detail(rows)

    # --- what the reader is owed about where these numbers come from -------
    note = sim.gate_note(fitted)
    st.caption(note["text"])

    divergent = ps.scoring_divergence(rows)
    if divergent and modelled:
        worst, low, high = divergent[0]
        st.caption(
            f"**Δ Wins is the number to act on; %/Pt is an index.** Your leagues do "
            f"not agree about what a point is — {worst} projects {low:.2f} in one "
            f"and {high:.2f} in another, and {len(divergent)} players differ this "
            f"way. Δ Wins is immune because each league's points become a "
            f"probability, in that league's own scoring, before anything is added "
            f"up. A summed per-point rate is not.")

    st.caption(
        "Bench players are absent because they cannot score for anyone. Lineups are "
        "taken as they stand, so an opponent promoting a starter before kickoff "
        "moves these numbers. Players in the same NFL game are treated as "
        "independent — measured at team level in plan 42, not at this one.")

    for note_text in notes:
        st.caption(f"⚠️ {note_text}")
