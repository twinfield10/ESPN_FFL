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
                   "about 40 seconds of ESPN round-trips per league."),
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
                "A league in its first season gets none: the score normalisation "
                "needs one prior season as a baseline."
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
st.subheader("Both Lineups, As Set")
st.caption(
    "Both sides in ESPN's slot order — QB, RB, WR, TE, FLEX, OP, DP, D/ST, K — so "
    "the two columns line up row for row and a matchup can be read across."
)

missing = weekly.missing_sources_note(selection.meta)
if missing:
    st.caption(missing)

# Derived from a frame that carries `slot`. The starters do; `rostered` carries
# `slotPosition` instead, and deriving from it silently dropped the Slot column —
# which is the column the whole ordering is there to make readable.
columns = weekly.display_columns(
    lu.with_source_spread(rostered, selection.meta)
      .with_columns(pl.col("slotPosition").alias("slot")),
    selection.meta)
pair = st.columns(2)
for column, name in zip(pair, (owner, opponent)):
    with column:
        st.markdown(f"**{name}** · {sides[name].projected:.1f}")
        starters = lineups_by_owner[name][1]
        frame = pl.DataFrame(starters) if starters else rostered.head(0)
        weekly.render_table(lu.sort_by_slot(frame), selection.meta, columns=columns)
