"""Roster — who to start this week, and what it costs to get it wrong.

The Sunday-morning page. One team, one week, the lineup ESPN has set beside the best
one available, and the specific swaps between them.

**The swaps are the deliverable, not the efficiency score.** The notebook already
computed an optimal-lineup total and buried the useful half: knowing you left 19
points on the table does not tell you to start Trevor Lawrence over Philip Rivers.
See :func:`lineup.swaps`.

Reads ``lineups.parquet`` and nothing else. Week comes from the global context row;
which weeks exist comes from the artifact rather than from ``meta["weeks_present"]``,
which is absent from every 2026 store -- see :func:`session.available_weeks`.

See ``docs/plans/08-frontend-weekly-views.md`` and
``docs/plans/40-frontend-restructure.md``.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import polars as pl
import streamlit as st

import lineup as lu
import session
import store
from Scripts.draft.board import NON_STARTING_SLOTS
from views import weekly

selection = session.current()

if not store.has_artifact(selection.season, selection.league_key, "lineups"):
    st.title("Roster")
    st.warning(
        "No weekly lineups in this store. This is the default artifact, so a plain "
        "refresh builds it."
    )
    st.code(
        f"python -m Scripts.refresh --league {selection.display_name} "
        f"--season {selection.season}",
        language="bash",
    )
    st.stop()

lineups = store.load_lineups(selection.season, selection.league_key)
week = lineups.filter(pl.col("week") == selection.week)

if week.is_empty():
    st.title("Roster")
    st.info(
        f"Nothing stored for week {selection.week} in {selection.display_name}. "
        f"Weeks present: {sorted(lineups['week'].unique().to_list())}."
    )
    st.stop()

rostered = week.filter(pl.col("team_owner") != session.FREE_AGENT_OWNER)
owners = session.team_owners(week)
if not owners:
    st.title("Roster")
    st.info("This week's lineups hold no rostered players — every row is a free agent.")
    st.stop()

# Default to whoever owns this league. For the five leagues that are other people's,
# `primary_owner` is correctly their name -- you are looking at their team because it
# is their league.
default_owner = (selection.my_owner if selection.my_owner in owners
                 else owners[0])
head = st.columns([2, 3], vertical_alignment="bottom")
owner = head[0].selectbox(
    "Team", owners, index=owners.index(default_owner), key="roster_owner",
    help="Defaults to this league's primary owner.")

slots = lu.slot_counts(rostered, selection.meta)
head[1].caption(
    f"Week {selection.week} · {sum(slots.values())} starting slots · "
    + " · ".join(f"{slot} {n}" for slot, n in sorted(slots.items()))
)

st.title(f"Roster · {owner}")

team = lu.with_source_spread(
    rostered.filter(pl.col("team_owner") == owner), selection.meta)
rows = team.to_dicts()

current, current_total = lu.current_lineup(rows, "TRUE_Points")
optimal, optimal_total = lu.optimal_lineup(rows, slots, "TRUE_Points")
changes = lu.swaps(current, optimal, "TRUE_Points")

metrics = st.columns(4)
metrics[0].metric("Lineup As Set", f"{current_total:.1f}",
                  help="Projected points from the lineup ESPN currently has, on our "
                       "blend.")
metrics[1].metric("Best Available", f"{optimal_total:.1f}",
                  f"{optimal_total - current_total:+.1f}",
                  help="The highest-projecting legal lineup this roster can field.")
inactive = lu.bye_and_out(rows)
metrics[2].metric("Cannot Play", f"{len(inactive)}",
                  help="On bye or ruled out. Excluded from the best available "
                       "lineup, because a lineup that starts them is not one you "
                       "would set.")
thin = [r for r in current if (r.get("sources_real") or 0) < 2]
metrics[3].metric("Single-Source Starters", f"{len(thin)}",
                  help="Starters whose projection rests on one source that really "
                       "had an opinion. Not wrong — but nothing is corroborating it.")

st.divider()

left, right = st.columns([3, 2])

with left:
    st.subheader("Start / Sit")
    weekly.render_swaps(changes)

with right:
    if inactive:
        st.subheader("Cannot Play This Week")
        for row in sorted(inactive, key=lambda r: -(r.get("TRUE_Points") or 0)):
            where = ("starting" if row.get("slotPosition") not in NON_STARTING_SLOTS
                     else "benched")
            st.markdown(
                f"- **{row.get('player_name')}** "
                f"({row.get('player_position')}) · "
                f"`{row.get('player_active_status')}` · {where}")
        if any(r.get("slotPosition") not in NON_STARTING_SLOTS for r in inactive):
            st.error("Someone who cannot play is in your starting lineup.",
                     icon="🚨")
    else:
        st.subheader("Availability")
        st.success("Everyone on this roster can play.", icon="✅")

note = weekly.missing_sources_note(selection.meta)
if note:
    st.caption(note)

st.divider()

# Derived from a frame that carries `slot`. `team` carries `slotPosition`, and
# deriving from it dropped the Slot column from both tables below — which is the
# column the slot ordering exists to make readable.
columns = weekly.display_columns(
    team.with_columns(pl.col("slotPosition").alias("slot")), selection.meta)

st.subheader("The Best Lineup Available")
best = pl.DataFrame(optimal) if optimal else team.head(0)
weekly.render_table(lu.sort_by_slot(best), selection.meta, columns=columns)
st.caption(
    "In ESPN's slot order — QB, RB, WR, TE, FLEX, OP, DP, D/ST, K — and by "
    "projection within a slot. Slot is where the optimiser puts each player, not "
    "where ESPN has him. Eligibility comes from ESPN's own `eligiblePositions`, "
    "which is what makes superflex (`OP`) and this league's defensive slots work "
    "without a lookup table."
)

st.subheader("Everyone On The Roster")
weekly.render_table(
    lu.sort_by_slot(team, slot_column="slotPosition"), selection.meta,
    columns=[c if c != "slot" else "slotPosition" for c in columns])
st.caption(
    "As ESPN has it set, in ESPN's own slot order, with the bench and IR last. "
    "`Slot` here is the real one, not the optimiser's — and it is the *slot*, not "
    "the position, so a receiver in the flex sits under FLEX."
)
