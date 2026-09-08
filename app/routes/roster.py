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
import lineup_table as ltab
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

# The Matchup tab's columns, minus the mirroring and the advantage pair that only a
# fixture has. `blend_first` is the one deliberate difference: this is a decision
# table, so the number the lineup is chosen on sits beside the player's name rather
# than four columns downstream of it.
info_columns = ltab.info_columns(team.columns)
points_columns = ltab.points_columns(team.columns, selection.meta, blend_first=True)

st.subheader("The Best Lineup Available")

# One table, and the changes are marked in it rather than shown in a second one.
#
# The optimal lineup on its own could only ever mark the players coming *in* -- the
# ones going out are, by definition, not in it. So each outgoing player is drawn
# directly beneath the man who displaces him, which is what makes a swap legible as
# one decision instead of two rows to hunt for. He keeps `slotPosition`, the slot
# ESPN actually has him in, rather than borrowing the incoming player's: the two are
# usually the same and stating the wrong one when they are not would be a lie in
# service of a tidier column.
added, dropped = lu.changed_ids(current, optimal)
displaced = {change.start_row.get("player_id"): change.sit_row
             for change in changes
             if change.start_row is not None and change.sit_row is not None}

table_rows, marks = [], {}
for row in lu.sort_by_slot(pl.DataFrame(optimal)).to_dicts() if optimal else []:
    player_id = row.get("player_id")
    table_rows.append(row)
    if player_id in added:
        marks[player_id] = "in"
    leaving = displaced.get(player_id)
    if leaving is not None:
        table_rows.append({**leaving, "slot": leaving.get("slotPosition")})
        marks[leaving.get("player_id")] = "out"

# A starter the optimiser drops without putting anyone in his place -- someone ruled
# out with no cover, which is the case you most want flagged. `swaps` pairs each
# arrival with a departure and has nothing to pair him with, so he would otherwise
# be the one change the table did not show.
for row in sorted((r for r in current if r.get("player_id") in dropped
                   and r.get("player_id") not in marks),
                  key=lambda r: -(r.get("TRUE_Points") or 0)):
    table_rows.append({**row, "slot": row.get("slotPosition")})
    marks[row.get("player_id")] = "out"

# The bench, under the total rather than over it: these are the rows the total
# deliberately excludes, and putting them above it would make the number look wrong.
# Anyone already drawn above -- a player coming in, or one being dropped -- is not
# repeated here, so every player on the roster appears exactly once.
#
# `slot` is set from `slotPosition` for the same reason the red rows carry it: the
# table reads one slot column, and these rows come off the raw frame rather than out
# of the optimiser, so without it their slot cell renders blank -- BE and IR are the
# only thing distinguishing a bench row at a glance.
shown = {row.get("player_id") for row in table_rows}
bench = [{**row, "slot": row.get("slotPosition")} for row in
         lu.sort_by_slot(team, slot_column="slotPosition").to_dicts()
         if row.get("player_id") not in shown]

weekly.render_lineup(table_rows, info_columns, points_columns, label=owner,
                     total_rows=optimal, marks=marks, below=bench)

changed = sum(1 for mark in marks.values() if mark == "in")
marking = (
    f"**Green `IN`** is a player to start who is currently benched; **red `OUT`** "
    f"is one currently starting who this lineup drops, drawn under the man taking "
    f"his place. {changed} change{'s' if changed != 1 else ''}. "
) if marks else "Nothing is marked: this lineup is already the best one available. "
st.caption(
    f"The lineup is above `TOTAL`, in ESPN's slot order — QB, RB, WR, TE, FLEX, OP, "
    f"DP, D/ST, K — and by projection within a slot; the {len(bench)} rows below it "
    f"are the bench, which is why they are below it. {marking}"
    f"`TOTAL` counts only the {len(optimal)} rows in the lineup — the **Best "
    f"Available** metric above. Slot is where the optimiser puts each player; on a "
    f"red or a bench row it is where ESPN has him now. Eligibility comes from ESPN's "
    f"own `eligiblePositions`, which is what makes superflex (`OP`) and this "
    f"league's defensive slots work without a lookup table."
)
