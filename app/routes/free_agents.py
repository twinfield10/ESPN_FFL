"""Free Agents — who is available, and whether adding them changes anything.

Replaces the eight ``FA_*`` Google Sheets tabs with one page: a position filter, a
search box, and the same per-source columns the Roster tab uses.

**The free-agent pool is not a separate artifact.** ``lineups.parquet`` carries every
unrostered player as extra rows on a synthetic team called ``Free Agent`` -- 15
"owners" for a 14-team league. So this is a filter over the same frame the Roster tab
reads, which is why the two agree by construction about what a player projects.

**Add/drop is scored as one decision, not two rankings.** A receiver who out-projects
your worst bench player by four points is worth nothing if he still would not start.
What matters is whether the lineup you would actually field on Sunday improves, which
is a question about the whole roster -- so every suggestion is the difference between
two optimal lineups. See :func:`lineup.add_drop_gain`.

See ``docs/plans/08-frontend-weekly-views.md`` and
``docs/plans/40-frontend-restructure.md``.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import polars as pl
import streamlit as st

import lineup as lu
import session
import store
from views import weekly

#: How many drop candidates each addition is weighed against.
#:
#: Every pair costs two optimal-lineup solves. The candidates are no longer a top-N
#: of the pool -- they are the best available player for each *starting slot*, which
#: :func:`lineup.best_available_per_slot` bounds at the number of slots the league
#: has, so the pairing is at most eight by four and lands in milliseconds.
#:
#: **The old top-N was position-blind and that was not a cap, it was a blind spot.**
#: The positions are not on the same scale, so "the top six available by projection"
#: was six quarterbacks on Winfield week 1, in a league that starts one. The best
#: available running back, tight end and kicker were never scored at all.
ADD_DROP_DROPS = 4

selection = session.current()

if not store.has_artifact(selection.season, selection.league_key, "lineups"):
    st.title("Free Agents")
    st.warning("No weekly lineups in this store, so there is no free-agent pool.")
    st.code(
        f"python -m Scripts.refresh --league {selection.display_name} "
        f"--season {selection.season}",
        language="bash",
    )
    st.stop()

lineups = store.load_lineups(selection.season, selection.league_key)
week = lineups.filter(pl.col("week") == selection.week)

if week.is_empty():
    st.title("Free Agents")
    st.info(
        f"Nothing stored for week {selection.week}. Weeks present: "
        f"{sorted(lineups['week'].unique().to_list())}."
    )
    st.stop()

pool = week.filter(pl.col("team_owner") == session.FREE_AGENT_OWNER)
rostered = week.filter(pl.col("team_owner") != session.FREE_AGENT_OWNER)

if pool.is_empty():
    st.title("Free Agents")
    st.info(
        "Every player in this week's store is on a roster. In a deep league that is "
        "the real answer; it can also mean the store was built before the draft."
    )
    st.stop()

st.title(f"Free Agents · Week {selection.week}")

pool = lu.with_source_spread(pool, selection.meta)
positions = sorted(p for p in pool["player_position"].unique().to_list() if p)

with st.container(border=True):
    controls = st.columns([3, 3, 2])
    search = controls[0].text_input(
        "Find A Player", key="fa_search", placeholder="Surname is enough",
        help="Matched literally, not as a regex — the names on a board are full of "
             "dots and hyphens.")
    keep = controls[1].multiselect(
        "Positions", positions, key="fa_positions",
        help="Empty keeps everything.")
    min_points = controls[2].number_input(
        "Min Projected", min_value=0.0, value=0.0, step=1.0, key="fa_min",
        help="Hides the long tail of players projected near zero.")

filtered = pool
if search.strip():
    filtered = filtered.filter(
        pl.col("player_name").str.contains(search.strip(), literal=True))
if keep:
    filtered = filtered.filter(pl.col("player_position").is_in(keep))
if min_points:
    filtered = filtered.filter(pl.col("TRUE_Points") >= min_points)

note = weekly.missing_sources_note(selection.meta)
if note:
    st.caption(note)

# --- add / drop -----------------------------------------------------------
owners = session.team_owners(week)
default_owner = selection.my_owner if selection.my_owner in owners else (
    owners[0] if owners else None)

if default_owner:
    st.subheader("Add / Drop")
    picker = st.columns([2, 4], vertical_alignment="bottom")
    owner = picker[0].selectbox(
        "Your Team", owners, index=owners.index(default_owner), key="fa_owner")

    roster = rostered.filter(pl.col("team_owner") == owner).to_dicts()
    slots = lu.slot_counts(rostered, selection.meta)
    drops = lu.weakest_starter_candidates(roster, slots, "TRUE_Points")[:ADD_DROP_DROPS]

    # Off `pool`, not `filtered`. A flag that disappears when you narrow the table
    # below to quarterbacks is not a flag, and the same goes for the pairing: the
    # controls scope the *pool table*, not what the app is willing to tell you.
    every = pool.to_dicts()
    candidates = lu.best_available_per_slot(every, slots, "TRUE_Points")

    st.markdown("**Better Than What You Have**")
    weekly.render_upgrades(
        lu.upgrades(every, roster, slots, "TRUE_Points"), owner=owner)
    st.caption(
        "Compared **slot by slot, not position by position** — which is what puts a "
        "free-agent quarterback up against a receiver in a superflex `OP`, a back up "
        "against a receiver in the flex, and five defensive positions up against "
        "each other in `DP`. Eligibility is ESPN's own `eligiblePositions`. Measured "
        "against the lineup ESPN currently has set, so if the Roster tab says to "
        "bench that player anyway, fix that first — it may cost you no waiver claim "
        "at all."
    )

    st.markdown("**What The Move Is Worth**")
    moves = []
    for candidate in candidates:
        best = None
        for drop in drops:
            gain = lu.add_drop_gain(roster, slots, "TRUE_Points", candidate,
                                    drop.get("player_id"))
            if best is None or gain > best[1]:
                best = (drop, gain)
        if best and best[1] > 0.01:
            moves.append({
                "Add": candidate.get("player_name"),
                "Pos": candidate.get("player_position"),
                "Add Proj": candidate.get("TRUE_Points"),
                "Drop": best[0].get("player_name"),
                "Drop Proj": best[0].get("TRUE_Points"),
                "Lineup Gain": best[1],
            })

    picker[1].caption(
        f"Weighing the best available player at each of {len(slots)} starting slots "
        f"— {len(candidates)} distinct players — against the {len(drops)} most "
        f"droppable on {owner}'s roster."
    )

    if moves:
        st.dataframe(
            pl.DataFrame(moves).sort("Lineup Gain", descending=True),
            width="stretch", hide_index=True, placeholder="", lazy=False,
            column_config={
                "Add": st.column_config.TextColumn(pinned=True),
                "Pos": st.column_config.TextColumn(),
                "Add Proj": st.column_config.NumberColumn(format="%.1f"),
                "Drop": st.column_config.TextColumn(),
                "Drop Proj": st.column_config.NumberColumn(format="%.1f"),
                "Lineup Gain": st.column_config.NumberColumn(
                    format="%+.1f",
                    help="What this move adds to the best lineup you could field "
                         "this week — the difference between two optimal lineups, "
                         "not the difference between two players."),
            },
        )
        st.caption(
            "`Lineup Gain` is the difference between two **optimal lineups**, not "
            "between two players — which is why it can be zero for someone the flags "
            "above call an upgrade: a player who beats your worst starter is worth "
            "nothing extra if the optimiser was going to bench that man anyway. "
            "**This week only.** A move that gains nothing this week can still be "
            "right for the rest of the season; rest-of-season value needs the weekly "
            "model that plan 19 has not built, so it is not claimed here."
        )
    else:
        st.info(
            f"None of the {len(candidates)} best-available players would improve the "
            f"best lineup {owner} could field this week. A waiver claim that does "
            f"not change your Sunday is a roster spot spent on nothing — though the "
            f"flags above still apply to the lineup as it is actually set."
        )

# --- the pool -------------------------------------------------------------
st.divider()
st.subheader(f"Available · {filtered.height} of {pool.height}")

# No `player_active_status` column, unlike the Roster tab. It is ESPN's answer to
# "is he in an active lineup slot", so every unrostered player reads `inactive` --
# 146 of 146 here -- and a column of that reads as an injury report for the entire
# waiver wire. See `lineup.POOL_STATUS`.
columns = weekly.display_columns(
    filtered, selection.meta,
    lead=("player_name", "player_position", "pro_team"))
weekly.render_table(filtered.sort("TRUE_Points", descending=True), selection.meta,
                    columns=columns, height=560)
st.caption(
    "Everyone ESPN lists as unrostered in this league, projected in its own scoring. "
    "**A player with no game this week projects 0.0 on ESPN** — that is how a bye "
    "shows up here, because ESPN's availability field says `inactive` for every "
    "unrostered player and so says nothing at all. Ownership percentage is on the "
    "draft board but not in the weekly artifact, so this cannot yet be narrowed to "
    "players who are genuinely gettable rather than merely unrostered."
)
