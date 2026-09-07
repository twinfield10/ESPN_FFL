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

#: How many free agents per position to consider for add/drop, and how many drop
#: candidates to weigh each against.
#:
#: Every pair costs two optimal-lineup solves, so the pairing is quadratic. Six by
#: four is 24 pairs per position and lands in milliseconds; the honest reason for a
#: cap is that a seventh-best waiver receiver is not a decision anybody is weighing.
#: The cap is stated on the page rather than left implicit -- a silent top-N reads as
#: "we checked everything".
ADD_DROP_CANDIDATES = 6
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
    candidates = (filtered.sort("TRUE_Points", descending=True)
                  .head(ADD_DROP_CANDIDATES).to_dicts())

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
        f"Weighing the top {len(candidates)} available against the "
        f"{len(drops)} most droppable on {owner}'s roster."
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
            "**This week only.** A move that gains nothing this week can still be "
            "right for the rest of the season; rest-of-season value needs the weekly "
            "model that plan 19 has not built, so it is not claimed here."
        )
    else:
        st.info(
            f"None of the top {len(candidates)} available players would improve "
            f"{owner}'s best lineup this week. That is the common answer, and it is "
            f"the one worth trusting — a waiver claim that does not change your "
            f"Sunday is a roster spot spent on nothing."
        )

# --- the pool -------------------------------------------------------------
st.divider()
st.subheader(f"Available · {filtered.height} of {pool.height}")

columns = weekly.display_columns(
    filtered, selection.meta,
    lead=("player_name", "player_position", "pro_team", "player_active_status"))
weekly.render_table(filtered.sort("TRUE_Points", descending=True), selection.meta,
                    columns=columns, height=560)
st.caption(
    "Everyone ESPN lists as unrostered in this league, projected in its own scoring. "
    "Ownership percentage is on the draft board but not in the weekly artifact, so "
    "this cannot yet be narrowed to players who are genuinely gettable rather than "
    "merely unrostered."
)
