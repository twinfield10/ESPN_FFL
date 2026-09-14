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

import math

import polars as pl
import streamlit as st

import lineup as lu
import lineup_table as ltab
import session
import store
import waivers as wv
from views import weekly

from Scripts import ros
from Scripts.draft import board as board_ranks
from Scripts.outcomes import weekly as outcomes_weekly

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
#: ``pts_p90``, ``p_top12``, ``games`` and ``usg_depth_rank`` ride along as
#: **context, never as a gate** -- see ``waivers.UPSIDE_IS_CONTEXT_NOT_A_GATE`` for
#: the three measurements that decided that.
BOARD_COLUMNS = ("injury_status", "percent_owned", "pts_p90", "p_top12", "games",
                 "usg_depth_rank")

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

# --- rest of season -------------------------------------------------------
#
# A rank, never a level. `Scripts/ros` carries the argument: `r2p_pts` is a total
# over the games *FantasyPros* expects, it is denominated in STD while every league
# here scores its own way, and the consensus behind it is 2-3 people. So it enters
# the decision as a positional rank against this league's own `replacement_rank`,
# which is a comparison ranks can support and points cannot.
ros_frame = ros.load_ros(selection.season)
pool_rows = ros.attach_ros(pool.to_pandas(), ros_frame).to_dict("records")

#: How deep each position is started in *this* league, which is what makes a
#: rest-of-season rank mean something here rather than in the abstract. Handles
#: superflex (Jeff's `OP` pushes QB replacement to 16 in an 8-team league) and IDP
#: (a position nobody really starts is omitted, not floored at 1).
try:
    replacement = board_ranks.replacement_ranks(
        lu.slot_counts(rostered, selection.meta),
        int(selection.meta.get("team_count") or 0),
        pool=week.to_pandas(), points_column=points_col)
except Exception:                                   # noqa: BLE001 - context, not data
    replacement = {}

#: The fitted outcome dispersion, for the confidence column. Absent on a fresh
#: clone, and then no confidence is published rather than a made-up one.
try:
    dispersion = outcomes_weekly.load()
except Exception:                                   # noqa: BLE001
    dispersion = None

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

    # --- what it takes to play here ---------------------------------------
    #
    # The panel leads with this because it reduces a few hundred pool rows to one
    # sentence per slot, and because it is the only part of the page that owes
    # nothing to any outside source: it is this team's own optimiser read back.
    # `lineup.lineup_threshold` gets it in two exact solves off the matroid kink
    # rather than by searching -- and `add_drop_gain` is the same arithmetic's two
    # halves, verified identical on all 448 pairings of the 16 GOP rosters.
    st.markdown("**What It Takes To Play For You**")
    bars = wv.threshold_table(roster, pool_rows, slots, points_col)
    st.dataframe(
        pl.DataFrame([{
            "Slot": row["slot"],
            "To Start": (None if row["bar"] is None
                         else (None if row["bar"] == math.inf else row["bar"])),
            "Settled": row["bar"] == math.inf,
            "Best Available": (row["best"] or {}).get("player_name"),
            "Projects": (None if row["best"] is None
                         else float(row["best"].get(points_col) or 0.0)),
            "Beats It": row["beats"],
        } for row in bars]),
        width="stretch", hide_index=True, placeholder="—", lazy=False,
        column_config={
            "Slot": st.column_config.TextColumn(width=100, pinned=True),
            "To Start": st.column_config.NumberColumn(
                format="%.1f",
                help="What a free agent must project to displace whoever holds "
                     "this slot in your optimal lineup. Blank where the slot is "
                     "already settled by a kickoff."),
            "Settled": st.column_config.CheckboxColumn(
                help="The man in this slot has played. Nothing can reach it this "
                     "week, whatever he scored."),
            "Best Available": st.column_config.TextColumn(),
            "Projects": st.column_config.NumberColumn(format="%.1f"),
            "Beats It": st.column_config.CheckboxColumn(
                help="The wire can improve this slot today."),
        },
    )
    beaten = [row["slot"] for row in bars if row["beats"]]
    settled = [row["slot"] for row in bars if row["bar"] == math.inf]
    lead = (f"The wire can improve **{', '.join(beaten)}**."
            if beaten else
            "**Nothing available can crack this lineup today.**")
    if settled:
        lead += (f" {', '.join(settled)} {'is' if len(settled) == 1 else 'are'} "
                 f"already settled — those players have kicked off.")
    st.caption(
        lead + " A slot with no bar is one this league cannot start you at. "
        "**The bar is what you would actually lose, not who is in the slot**: a "
        "receiver who displaces your WR2 pushes him into the flex, and it is "
        "whoever falls out of the *flex* that you give up. That is why two slots "
        "linked by a flex often show the same number — and why a bar can sit below "
        "the man currently filling the slot."
    )

    # --- the flag, kept because Home mirrors it ---------------------------
    st.markdown("**Better Than What You Have**")
    weekly.render_upgrades(
        lu.upgrades(pool_rows, roster, slots, points_col), owner=owner)
    st.caption(
        "Compared **slot by slot, not position by position** — which is what puts a "
        "free-agent quarterback up against a receiver in a superflex `OP`, a back up "
        "against a receiver in the flex, and five defensive positions up against "
        "each other in `DP`. Eligibility is ESPN's own `eligiblePositions`. Measured "
        "against the lineup ESPN currently has set, where the table above is "
        "measured against your *optimal* one — so if the Roster tab says to bench "
        "that player anyway, fix that first; it may cost you no waiver claim at all."
    )

    # --- the moves --------------------------------------------------------
    st.markdown("**What The Move Is Worth**")
    moves = wv.rank_moves(roster, pool_rows, slots, points_col,
                          replacement=replacement, model=dispersion)
    shown = [m for m in moves if m.verdict != wv.VERDICT_STREAM]

    picker[1].caption(
        f"Weighing the best available player at each of {len(slots)} starting "
        f"slots against the most droppable on {owner}'s roster. A move that only "
        f"improves the bench is not shown unless the rest-of-season consensus says "
        f"he starts in a league this shape."
    )

    if shown:
        st.dataframe(
            pl.DataFrame([{
                "": wv.VERDICT_STYLE[m.verdict][0],
                "Add": m.add.get("player_name"),
                "Pos": m.add.get("player_position"),
                "ROS": m.add.get("ros_pos_rank"),
                "Drop": m.drop.get("player_name"),
                "Week Gain": m.week_gain,
                "Cover Cost": m.insurance or None,
                "Net": m.net,
                "Confidence": m.confidence,
                "Why": m.reason,
            } for m in shown]),
            width="stretch", hide_index=True, placeholder="—", lazy=False,
            column_config={
                "": st.column_config.TextColumn(width=40),
                "Add": st.column_config.TextColumn(pinned=True),
                "Pos": st.column_config.TextColumn(),
                "ROS": st.column_config.TextColumn(
                    help="FantasyPros' rest-of-season positional rank. Backed by "
                         "two or three experts, so it is used to order players and "
                         "never to set a level — and it covers no individual "
                         "defenders at all."),
                "Drop": st.column_config.TextColumn(),
                "Week Gain": st.column_config.NumberColumn(
                    format="%+.1f",
                    help="The difference between two optimal lineups this week — "
                         "not the difference between two players."),
                "Cover Cost": st.column_config.NumberColumn(
                    format="%.1f",
                    help="What the man going out would be worth the week a starter "
                         "above him sits. This is positional scarcity priced rather "
                         "than vetoed, so a thin position defends itself in points."),
                "Net": st.column_config.NumberColumn(
                    format="%+.1f", help="Week Gain minus Cover Cost. The sort key."),
                "Confidence": st.column_config.NumberColumn(
                    format="%.0f%%",
                    help="P(this move actually helps), on the fitted per-position "
                         "outcome dispersion — 15,989 started player-weeks. A "
                         "+0.5 edge is about 52%, which is the point of showing it."),
                "Why": st.column_config.TextColumn(width="large"),
            },
        )
        st.caption(
            "Sorted by **Net**, because a waiver claim is a decision about a roster "
            "spot rather than about Sunday alone. `Confidence` is the number to "
            "read last and trust most: a margin of half a point against an outcome "
            "spread near 11 is a coin flip however confident the projection looks, "
            "and no amount of ranking makes it otherwise."
        )
    else:
        st.info(
            f"Nothing on the wire would improve {owner}'s starting lineup or beat "
            f"it rest-of-season. A claim that does not change your Sunday and does "
            f"not survive to December is a roster spot spent on nothing."
        )

    # --- streaming --------------------------------------------------------
    #
    # Their own line rather than the table above. K and D/ST were 64 of the 156
    # successful in-season adds across the nine 2025 leagues -- 41% -- but each one
    # is nearly costless and fully reversible, so they crowd out the decisions that
    # matter if they compete on the same list. They are also the two positions
    # whose rest-of-season conversion is loosest (plan 49, G-R1), which is a second
    # reason arrived at independently.
    streamers = wv.best_streamers(pool_rows, roster, slots, points_col)
    if streamers:
        st.markdown("**Streaming**")
        st.dataframe(
            pl.DataFrame([{
                "Slot": row["slot"],
                "Best Available": row["best"].get("player_name"),
                "Projects": float(row["best"].get(points_col) or 0.0),
                "You Have": (row["mine"] or {}).get("player_name"),
                "Gain": row["gain"],
            } for row in streamers]),
            width="stretch", hide_index=True, placeholder="—", lazy=False,
            column_config={
                "Slot": st.column_config.TextColumn(width=100),
                "Best Available": st.column_config.TextColumn(),
                "Projects": st.column_config.NumberColumn(format="%.1f"),
                "You Have": st.column_config.TextColumn(),
                "Gain": st.column_config.NumberColumn(format="%+.1f"),
            },
        )
        st.caption(
            "Kicker and defence, kept apart from the table above because the "
            "decision is a different shape: 41% of the in-season adds that worked "
            "across the nine 2025 leagues were at these two positions, and each one "
            "is nearly costless and undone next week. Worth a glance, never an "
            "emergency."
        )

    #: FantasyPros publishes no individual defenders, so on an IDP league a large
    #: part of the pool carries no rest-of-season number at all. Saying so is the
    #: house rule -- an absent source is dropped, never zeroed -- and it matters
    #: most exactly where it is least visible.
    silent = sum(1 for r in pool_rows
                 if r.get("ros_missing_reason") == ros.MISSING_NO_PUBLICATION)
    if silent:
        st.caption(
            f"**{silent} of {len(pool_rows)} available players have no "
            f"rest-of-season number**, because FantasyPros does not rank individual "
            f"defenders. They are judged on this week alone. That is a gap in the "
            f"source, not a verdict on the players."
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
