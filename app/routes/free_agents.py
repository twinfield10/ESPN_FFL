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

**And only moves that can actually be made.** Two rules the panel shipped without,
both of which were producing confident, illegal advice:

* A player whose game has kicked off is neither available nor droppable. His points are banked either way, and on a finished game the number the ranking reads is a score rather than a projection -- so an unfiltered pool overstates what the wire is offering, and an unfiltered drop list ranks a settled zero as the most droppable man on the roster.
* An **IR slot sits outside the roster count**, so dropping the man in it frees an IR slot rather than a bench place. Only an add ESPN would let *into* that slot can use it. See :func:`lineup.droppable_for`.

``injury_status`` and ``percent_owned`` come from ``board.parquet``, joined on
``player_id`` -- neither is on the weekly artifact, and the join is 1:1 on all ten
2026 stores.

See ``docs/plans/08-frontend-weekly-views.md``,
``docs/plans/40-frontend-restructure.md`` and
``docs/plans/49-rest-of-season-waivers.md``.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import polars as pl
import streamlit as st

import lineup as lu
import lineup_table as ltab
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

# --- what the weekly artifact does not carry -----------------------------
#
# `injury_status` and `percent_owned` live on the season-grain board, not on
# `lineups.parquet`, and the IR rule and the gettability filter both need them.
# Measured on all ten 2026 stores before relying on it: no board has a duplicate
# `player_id`, so this is 1:1; `percent_owned` reaches 100% of every pool; and all
# 19 IR-slot rows across the nine leagues carry an `injury_status`. The 32 nulls
# per league are D/ST, which has no injury designation by nature.
#
# **The board is rebuilt by the 06:00 nightly and `--what live` does not touch it**
# (`Scripts/live.py` PATCH_COLUMNS covers the lineups frame only), so a designation
# here can be a day old. That is right for an IR placement, which is not an
# intraday event, and wrong for anything needing the hour.
BOARD_COLUMNS = ("injury_status", "percent_owned")

if store.has_artifact(selection.season, selection.league_key, "board"):
    board = store.load_board(selection.season, selection.league_key)
    carry = [c for c in BOARD_COLUMNS if c in board.columns]
    if carry:
        week = week.join(
            board.select(["player_id", *carry]).unique(subset=["player_id"]),
            on="player_id", how="left")

# A store with no board leaves both columns absent. `lineup.ir_eligible` reads that
# as "not IR-eligible", which makes an IR-slotted player undroppable -- conservative,
# and closer to true than ranking him as the best available drop.
for column in BOARD_COLUMNS:
    if column not in week.columns:
        week = week.with_columns(pl.lit(None).alias(column))

pool = week.filter(pl.col("team_owner") == session.FREE_AGENT_OWNER)
rostered = week.filter(pl.col("team_owner") != session.FREE_AGENT_OWNER)

#: The positional colour rulers -- see the note in ``routes/roster.py``.
scales = ltab.points_scales(
    week.select([c for c in ltab.SCALE_INPUTS
                 if c in week.columns]).to_dicts(),
    week.columns)

#: See the note in ``routes/roster.py``.
points_col = lu.live_points_column(week.columns)

if pool.is_empty():
    st.title("Free Agents")
    st.info(
        "Every player in this week's store is on a roster. In a deep league that is "
        "the real answer; it can also mean the store was built before the draft."
    )
    st.stop()

st.title(f"Free Agents · Week {selection.week}")

everyone = lu.with_source_spread(pool, selection.meta)

# A player whose game has kicked off is not available, whatever ESPN still lists.
# He cannot be added to a lineup that has already played, and on a finished game
# his number is a banked score rather than a projection -- so leaving him in the
# pool overstates what the wire is offering. 1-16 rows per league were in that
# state on 2026 week 1, mid-Wednesday, and the count grows through the weekend.
pool = lu.playable_pool(everyone, points_col)
kicked_off = everyone.height - pool.height

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
    # IR-slotted players are held out of the cap rather than counted against it.
    # They sort first -- an IR man projects 0.0 -- but `lineup.droppable_for` will
    # refuse them for all but an IR/OUT add, so letting them occupy two of the four
    # places would quietly halve the drop candidates a healthy add is weighed
    # against. They ride along instead: cheap, and still available for the one kind
    # of add that can legally take the slot.
    droppable = lu.weakest_starter_candidates(roster, slots, points_col)
    on_ir = [r for r in droppable if (r.get("slotPosition") or "") == "IR"]
    drops = [r for r in droppable
             if (r.get("slotPosition") or "") != "IR"][:ADD_DROP_DROPS] + on_ir

    # Off `pool`, not `filtered`. A flag that disappears when you narrow the table
    # below to quarterbacks is not a flag, and the same goes for the pairing: the
    # controls scope the *pool table*, not what the app is willing to tell you.
    every = pool.to_dicts()
    candidates = lu.best_available_per_slot(every, slots, points_col)

    st.markdown("**Better Than What You Have**")
    weekly.render_upgrades(
        lu.upgrades(every, roster, slots, points_col), owner=owner)
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
            # An IR slot is outside the roster count, so giving up the man in it
            # makes room only for somebody ESPN would let into that slot. See
            # `lineup.droppable_for` -- without it an IR player, projecting 0.0,
            # is ranked the most droppable man on the roster.
            if not lu.droppable_for(drop, candidate):
                continue
            gain = lu.add_drop_gain(roster, slots, points_col, candidate,
                                    drop.get("player_id"))
            if best is None or gain > best[1]:
                best = (drop, gain)
        if best and best[1] > 0.01:
            moves.append({
                "Add": candidate.get("player_name"),
                "Pos": candidate.get("player_position"),
                "Add Proj": candidate.get(points_col),
                "Drop": best[0].get("player_name"),
                "Drop Proj": best[0].get(points_col),
                "Lineup Gain": best[1],
            })

    blurb = (
        f"Weighing the best available player at each of {len(slots)} starting slots "
        f"— {len(candidates)} distinct players — against the "
        f"{len(drops) - len(on_ir)} most droppable on {owner}'s roster"
    )
    if on_ir:
        blurb += (
            f", plus {len(on_ir)} on IR — who free an IR slot rather than a bench "
            f"place, so they are only offered against an add ESPN would let into it"
        )
    picker[1].caption(blurb + ".")

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

positions = sorted(p for p in pool["player_position"].unique().to_list() if p)
gettable = "percent_owned" in pool.columns and pool["percent_owned"].is_not_null().any()

# The controls sit here, directly above the table they scope, rather than at the
# top of the page. They have never applied to the Add/Drop panel above -- that
# reads the whole pool on purpose, because a flag that disappears when you filter
# the table to quarterbacks is not a flag.
with st.container(border=True):
    controls = st.columns([3, 3, 2, 2] if gettable else [3, 3, 2])
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
    max_owned = controls[3].number_input(
        "Max Owned %", min_value=0, max_value=100, value=100, step=5,
        key="fa_owned",
        help="Ownership across ESPN, from the draft board. Lower it to the players "
             "who are genuinely gettable rather than merely unrostered here."
    ) if gettable else 100

filtered = pool
if search.strip():
    filtered = filtered.filter(
        pl.col("player_name").str.contains(search.strip(), literal=True))
if keep:
    filtered = filtered.filter(pl.col("player_position").is_in(keep))
if min_points:
    filtered = filtered.filter(pl.col(points_col) >= min_points)
if gettable and max_owned < 100:
    filtered = filtered.filter(
        pl.col("percent_owned").fill_null(0.0) <= float(max_owned))

heading = f"Available · {filtered.height} of {pool.height}"
if kicked_off:
    heading += f" · {kicked_off} already kicked off"
st.subheader(heading)

# No `player_active_status` column, unlike the Roster tab. It is ESPN's answer to
# "is he in an active lineup slot", so every unrostered player reads `inactive` --
# 146 of 146 here -- and a column of that reads as an injury report for the entire
# waiver wire. See `lineup.POOL_STATUS`.
columns = weekly.display_columns(
    filtered, selection.meta,
    lead=("player_name", "player_position", "pro_team"),
    tail=("percent_owned", "points", "sources_real") if gettable
         else ("points", "sources_real"))
weekly.render_table(filtered.sort(points_col, descending=True), selection.meta,
                    columns=columns, height=560, scales=scales)
caption = (
    "Everyone ESPN lists as unrostered in this league who **can still be started "
    "this week**, projected in its own scoring. A player on bye, or whose game has "
    "already kicked off, is not on this list — his points are settled either way, "
    "so the wire is not really offering him. "
)
if kicked_off:
    caption += (
        f"{kicked_off} of the {everyone.height} unrostered players here have played. "
    )
if gettable:
    caption += (
        "`Owned %` is ESPN-wide ownership, off the draft board and refreshed by the "
        "06:00 nightly — the filter above narrows this to players who are genuinely "
        "gettable rather than merely unrostered in this league."
    )
st.caption(caption)
