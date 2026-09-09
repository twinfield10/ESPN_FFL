"""Matchup — who you are playing, and whether you win.

Reads the pairing from the ``team_stats`` artifact and the lineups from
``lineups.parquet``, and puts a calibrated win probability on the two.

**Two artifacts, because neither has both halves.** ``lineups.parquet`` knows every
player's projection and whose roster he is on; it does not know who plays whom.
``team_stats.parquet`` knows the fixture and the score; it knows nothing about
players. The pairing is read from the second and everything else from the first.

``team_stats`` is opt-in — it re-derives a league's entire history, which is ~40
seconds of ESPN round-trips — so it is not part of a plain refresh. It also could not
be built at all for the current season until 2026-09-07: an unplayed season has a
median score of zero, and the cross-season score normalisation divided by it. See
``Scripts/scrape_team_stats.py``.

The probability is the closed form, not a simulation, and it ships only because it
passed its gates. See :mod:`Scripts.outcomes.weekly`.

See ``docs/plans/42-weekly-matchup-odds.md``.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import polars as pl
import streamlit as st

import lineup as lu
import lineup_table as ltab
import matchup_sim as sim
import session
import store
from views import weekly

selection = session.current()

st.title(f"Matchup · Week {selection.week}")

for artifact, absent_note in (
    ("lineups", "No weekly lineups in this store, so there are no projections to "
                "put a matchup on."),
    ("team_stats", "No `team_stats` in this store, so nothing knows who plays whom. "
                   "It is opt-in because it re-derives the league's whole history — "
                   "eleven seasons and ~40s for Winfield, 1.5s for a league in its "
                   "first."),
):
    state = store.artifact_state(selection.season, selection.league_key, artifact)
    if state == "present":
        continue

    # "Built but not published" and "never built" are different problems with
    # different fixes, and the app reads S3 by default — so right after a refresh the
    # first is the common one. Telling someone to re-run the refresh they just ran is
    # the worst available answer.
    if state == "unpublished":
        st.warning(
            f"`{artifact}` is built on this machine but has not been published, and "
            f"the app is reading **{store.source()}**. Push it, or point the app at "
            f"local disk."
        )
        st.code(
            f"python -m Scripts.sync --push --what store --season {selection.season}\n"
            f"# or, without publishing:\n"
            f"ESPN_FFL_STORE_SOURCE=local streamlit run app/main.py",
            language="bash",
        )
    else:
        st.warning(absent_note)
        st.code(
            f"python -m Scripts.refresh --league {selection.display_name} "
            f"--season {selection.season} --what lineups,team_stats",
            language="bash",
        )
        if artifact == "team_stats":
            st.caption(
                "A league in its first season builds this like any other. It used to "
                "be skipped -- the score normalisation had nothing to divide by -- "
                "which is why `jeffs_league` had none until 2026-09-09."
            )
    st.stop()

lineups = store.load_lineups(selection.season, selection.league_key)
schedule = store.load_team_stats(selection.season, selection.league_key)

week_rows = lineups.filter(pl.col("week") == selection.week)
fixtures = schedule.filter(
    (pl.col("year") == selection.season) & (pl.col("week") == selection.week))

if week_rows.is_empty() or fixtures.is_empty():
    st.info(
        f"Week {selection.week} is not in both artifacts yet. Lineups hold "
        f"{sorted(lineups['week'].unique().to_list())}; the schedule holds "
        f"{sorted(int(w) for w in fixtures['week'].unique().to_list()) or '—'}."
    )
    st.stop()

rostered = week_rows.filter(pl.col("team_owner") != session.FREE_AGENT_OWNER)

# The fixture list and the rosters are two artifacts that do not always spell a team
# the same way -- see `matchup_sim.opponent_map`, which is where that is reconciled.
identities = (rostered.select(["team_owner", "team_name"]).unique()
              .iter_rows() if "team_name" in rostered.columns
              else [(o, None) for o in session.team_owners(week_rows)])
pairs, identity_notes = sim.opponent_map(
    fixtures.select(["team_owner", "team_name", "opp_owner", "opp_name"]).to_dicts(),
    list(identities))
owners = [o for o in session.team_owners(week_rows) if o in pairs]

if not owners:
    st.warning(
        "The fixture list and the rosters share no team this week. ESPN describes a "
        "team differently to its box-score and roster views, and neither the owner "
        "nor the team name lined up for any of them."
    )
    st.stop()

default_owner = selection.my_owner if selection.my_owner in owners else owners[0]
picker = st.columns([2, 4], vertical_alignment="bottom")
owner = picker[0].selectbox(
    "Team", owners, index=owners.index(default_owner), key="matchup_owner")
opponent = pairs.get(owner)

if not opponent or opponent not in set(rostered["team_owner"].to_list()):
    st.info(f"**{owner}** has no opponent with a stored roster in week "
            f"{selection.week} — a bye, or a name the two artifacts spell "
            f"differently.")
    st.stop()

picker[1].caption(f"**{owner}** vs **{opponent}** · week {selection.week}")

for identity_note in identity_notes:
    st.caption(f"⚠️ {identity_note}")

slots = lu.slot_counts(rostered, selection.meta)
fitted = sim.model()

sides, lineups_by_owner = {}, {}
for name in (owner, opponent):
    rows = lu.with_source_spread(
        rostered.filter(pl.col("team_owner") == name), selection.meta).to_dicts()
    current, _ = lu.current_lineup(rows, "TRUE_Points")
    lineups_by_owner[name] = (rows, current)
    sides[name] = sim.side(name, current, "TRUE_Points", fitted)

home, away = sides[owner], sides[opponent]
result = sim.outcome(home, away)

# --- the headline ---------------------------------------------------------
top = st.columns([2, 2, 3])
top[0].metric(owner, f"{home.projected:.1f}",
              help="Projected points from the lineup currently set, on our blend.")
top[1].metric(opponent, f"{away.projected:.1f}")
if result.win is None:
    top[2].metric("Projected Margin", f"{result.margin:+.1f}",
                  help="No fitted dispersion, so no probability.")
else:
    top[2].metric("Win Probability", f"{result.win * 100:.0f}%",
                  f"{result.margin:+.1f} projected margin",
                  delta_color="normal")

note = sim.gate_note(fitted)
(st.caption if note["kind"] == "ok" else st.warning)(note["text"])

if result.win is not None:
    band_home, band_away = home.band(), away.band()
    st.caption(
        f"80% of the time **{owner}** scores between **{band_home[0]:.0f}** and "
        f"**{band_home[1]:.0f}** (σ {home.sd:.1f} over {home.priced} of "
        f"{home.starters} starters), and **{opponent}** between "
        f"**{band_away[0]:.0f}** and **{band_away[1]:.0f}** (σ {away.sd:.1f}). "
        f"Those ranges overlap heavily, which is why a "
        f"{abs(result.margin):.0f}-point projected margin is a "
        f"{max(result.win, 1 - result.win) * 100:.0f}% matchup rather than a "
        f"decided one."
    )

st.divider()

# --- what a change is worth ----------------------------------------------
rows, current = lineups_by_owner[owner]
optimal, optimal_total = lu.optimal_lineup(rows, slots, "TRUE_Points")
changes = lu.swaps(current, optimal, "TRUE_Points")

left, right = st.columns([3, 2])

with left:
    st.subheader("Start / Sit")
    weekly.render_swaps(changes)
    if changes and result.win is not None:
        gain = sum(change.gain for change in changes)
        moved = sim.swing(home, away, gain)
        st.info(
            f"Making all {len(changes)} change"
            f"{'s' if len(changes) > 1 else ''} adds **{gain:+.1f}** projected "
            f"points and moves the win probability by **{moved * 100:+.1f}pp**, to "
            f"**{(result.win + moved) * 100:.0f}%**.",
            icon="📈",
        )
        st.caption(
            "The same two points are worth more in a close matchup than in a "
            "blowout, which is the reason to read the swing rather than the points."
        )

with right:
    st.subheader("Cannot Play")
    trouble = False
    for name in (owner, opponent):
        out = lu.bye_and_out(lineups_by_owner[name][0])
        starting = [r for r in out
                    if r.get("slotPosition") not in ("BE", "IR")]
        if not out:
            continue
        st.markdown(f"**{name}** · {len(out)} out, {len(starting)} of them starting")
        for row in sorted(starting, key=lambda r: -(r.get("TRUE_Points") or 0))[:4]:
            st.markdown(
                f"- {row.get('player_name')} ({row.get('player_position')}) · "
                f"`{row.get('player_active_status')}` · "
                f"{row.get('TRUE_Points'):.1f} projected")
        trouble = trouble or bool(starting)
    if not trouble:
        st.success("Nobody unavailable is in either starting lineup.", icon="✅")

# --- the two lineups ------------------------------------------------------
st.divider()
st.subheader("The Matchup, Slot By Slot")
st.caption(
    f"One table, mirrored: **{owner}** reads outward from the left, **{opponent}** "
    f"outward from the right, and the two meet at the slot they are both filling. "
    f"Rows are in ESPN's slot order — QB, RB, WR, TE, FLEX, OP, DP, D/ST, K — and "
    f"paired best against best within a slot. **ADV** is that side's points at that "
    f"slot minus the other side's, so the column sums to the projected margin, and "
    f"**Δ** is our blend against ESPN's own number."
)

missing = weekly.missing_sources_note(selection.meta)
if missing:
    st.caption(missing)

# Columns come off the whole week's frame rather than off either lineup, so both
# halves are the same shape even when one side has a player no source priced.
#
# No `Sources`/`Spread` here, unlike the Roster tab. They answer "how well
# corroborated is this projection", which is a question about your own roster; across
# a fixture it says nothing about whether your receiver beats theirs, and the two
# columns a side were four of the 25 that made the table need a scrollbar.
shape = lu.with_source_spread(rostered, selection.meta)
info_columns = ltab.info_columns(shape.columns)
points_columns = ltab.points_columns(shape.columns, selection.meta,
                                     corroboration=False)

weekly.render_matchup(
    ltab.pair_by_slot(lineups_by_owner[owner][1], lineups_by_owner[opponent][1],
                      slots),
    info_columns, points_columns, home_label=owner, away_label=opponent)
st.caption(
    f"`TOTAL` adds the points columns down each side — {owner} "
    f"{sides[owner].projected:.1f}, {opponent} {sides[opponent].projected:.1f} — "
    f"and the two `ADV` cells carry the same margin from each side's point of view. "
    f"How well corroborated each projection is lives on the Roster tab: it is a "
    f"question about your own bench, and it does not help you read a fixture."
)
