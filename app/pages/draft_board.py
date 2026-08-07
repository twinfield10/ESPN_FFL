"""Draft Board — the pre-draft artifact (``docs/plans/09-frontend-draft-views.md``).

Reads ``board.parquet`` and nothing else. Replacement level, VOR, tiers and value
are computed during ``python -m Scripts.refresh --what board``, not here, because a
page render has to stay a parquet read: the board takes ~1.6s of ESPN round-trips
per league to build against 11ms to read back.

The page is built around two things the plan argues are what a board is for:

- **Sorted by value, not by rank.** A rank-ordered list is a worse version of what
  ESPN already shows you. Where our valuation disagrees with the room is the only
  thing a board can tell you that the room cannot.
- **Tiers over ranks.** "Three running backs left before the drop" is a decision;
  "this player is one spot better than that one" is not.

League-awareness is the point: replacement level comes from each league's real
starting slots, so the same player is legitimately ranked differently across the
nine leagues. Josh Allen is a different player in the superflex.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import altair as alt
import polars as pl
import streamlit as st

import draft_view as dv
import store
from components.header import render_sidebar

selection = render_sidebar()
meta = selection.meta

st.title(f"Draft board · {selection.display_name} {selection.season}")

if not store.has_artifact(selection.season, selection.league_key, "board"):
    st.warning(
        "No draft board in this store. It is a separate artifact because it costs "
        "one `kona_player_info` request per league."
    )
    st.code(
        f"python -m Scripts.refresh --league {selection.display_name} "
        f"--season {selection.season} --what board",
        language="bash",
    )
    st.stop()

board = store.load_board(selection.season, selection.league_key)
theme = getattr(getattr(st.context, "theme", None), "type", "light") or "light"
colors = dv.SERIES_COLORS[theme]
ink = dv.CHART_INK[theme]

starting_slots = meta.get("starting_slots") or {}
my_owner = meta.get("primary_owner")
my_roster = board.filter(
    pl.col("team_owner").fill_null("") == (my_owner or "\0")
) if "team_owner" in board.columns else board.head(0)
needed = dv.positions_needed(starting_slots, my_roster["primaryPosition"].to_list())

# --- what this league is -------------------------------------------------

replacement = (
    board.filter(pl.col("replacement_rank").is_not_null())
    .group_by("primaryPosition").agg(pl.col("replacement_rank").first())
    .sort("primaryPosition")
)
cols = st.columns(4)
cols[0].metric("Teams", meta.get("team_count", "?"))
cols[1].metric("Pool", f"{board.height:,}")
projected = int((~board["projection_missing"]).sum()) if "projection_missing" in board.columns else board.height
cols[2].metric("With a real projection", f"{projected:,}")
priced = int(board["adp_is_priced"].sum()) if "adp_is_priced" in board.columns else 0
cols[3].metric("Priced by the market", f"{priced:,}")

st.caption(
    "Replacement level, from this league's own starting slots: "
    + " · ".join(f"**{row[0]}{int(row[1])}**" for row in replacement.iter_rows())
)

# --- filters -------------------------------------------------------------

all_positions = dv.board_positions(board)
default_positions = [p for p in all_positions if p in ("QB", "RB", "WR", "TE")]

with st.container(border=True):
    row = st.columns([3, 2, 2, 2])
    positions = row[0].multiselect(
        "Positions", all_positions,
        default=default_positions or all_positions,
        help="Colour is fixed per position, so changing this never recolours the rest.",
    )
    only_need = row[1].toggle(
        "Roster needs only", value=False,
        help=("Filters to positions that would fill a starting slot you have not "
              "filled yet. Pre-draft that is every position, so it does nothing "
              "until you own players."),
    )
    only_available = row[2].toggle("Available only", value=True)
    search = row[3].text_input("Find a player", placeholder="name contains…")

if only_need and needed:
    positions = [p for p in positions if p in needed] or needed

shown = dv.filter_board(board, positions, only_available=only_available,
                        search=search)

if shown.is_empty():
    st.warning("Nothing matches these filters.")
    st.stop()

# --- the scarcity curve --------------------------------------------------

st.subheader("The positional cliff")
st.caption(
    "Projected points against rank within position, out to "
    f"{dv.SCARCITY_DEPTH:g}× replacement level. Where a line falls away steeply is "
    "where waiting costs you; where it is flat, wait. The dashed rule is this "
    "league's replacement level for that position."
)

charted = [p for p in positions if p in dv.POSITION_HUES]
dropped = [p for p in positions if p not in dv.POSITION_HUES]
curve = dv.scarcity_curve(shown if only_available else board, charted)

if curve.is_empty():
    st.info("No projected players in this selection to chart.")
else:
    domain = [p for p in dv.POSITION_HUES if p in charted]
    scale = alt.Scale(domain=domain,
                      range=[colors[dv.POSITION_HUES[p]] for p in domain])
    # One colour encoding, reused by every layer. Vega-Lite shares the colour scale
    # across a layered chart and a `legend: null` on any layer suppresses the merged
    # legend -- which is how the first version of this shipped with direct labels
    # and no legend at all.
    hue = alt.Color("primaryPosition:N", scale=scale,
                    legend=alt.Legend(title=None, orient="top",
                                      labelColor=ink["text"]))
    base = alt.Chart(curve.to_pandas())

    # Room at the right end for the direct labels, which sit past the last point.
    # Chart `padding` does not do this -- it pads outside the plotting area, and the
    # label is drawn at a data position inside it, so it gets clipped instead.
    rank_max = float(curve["pos_rank"].max()) * 1.06 + 1

    lines = base.mark_line(strokeWidth=2, interpolate="monotone").encode(
        x=alt.X("pos_rank:Q", title="Rank within position",
                scale=alt.Scale(nice=False, domainMax=rank_max, zero=False),
                axis=alt.Axis(grid=False, labelColor=ink["muted"],
                              titleColor=ink["muted"], domainColor=ink["grid"])),
        y=alt.Y("TRUE_Points:Q", title="Projected season points",
                axis=alt.Axis(gridColor=ink["grid"], labelColor=ink["muted"],
                              titleColor=ink["muted"], domain=False)),
        color=hue,
    )
    # Hover: a point per player, so the tooltip reads a real row rather than an
    # interpolated position on the line.
    hover = base.mark_circle(size=64, opacity=0).encode(
        x="pos_rank:Q", y="TRUE_Points:Q", color=hue,
        tooltip=[alt.Tooltip("player_name:N", title="Player"),
                 alt.Tooltip("primaryPosition:N", title="Pos"),
                 alt.Tooltip("pos_rank:Q", title="Pos rank", format=".0f"),
                 alt.Tooltip("TRUE_Points:Q", title="Projected", format=".1f"),
                 alt.Tooltip("tier:Q", title="Tier", format=".0f")],
    )
    # Direct labels, which the light palette's sub-3:1 slots oblige and which also
    # make the chart readable without tracing a legend colour. Anchored right of the
    # last point, so the chart needs padding for them -- see `padding` below.
    labels = (
        base.transform_aggregate(
            pos_rank="max(pos_rank)", TRUE_Points="min(TRUE_Points)",
            groupby=["primaryPosition"])
        .mark_text(align="left", dx=6, dy=-2, fontSize=11, fontWeight=600)
        .encode(x="pos_rank:Q", y="TRUE_Points:Q",
                text="primaryPosition:N", color=hue)
    )
    rules = (
        alt.Chart(curve.filter(pl.col("replacement_rank").is_not_null())
                  .select(["primaryPosition", "replacement_rank"]).unique()
                  .to_pandas())
        .mark_rule(strokeDash=[4, 3], strokeWidth=1, opacity=0.6)
        .encode(x="replacement_rank:Q", color=hue)
    )

    st.altair_chart(
        (rules + lines + labels + hover)
        .properties(height=320, padding={"left": 0, "top": 0, "bottom": 0,
                                        "right": 28})
        .configure_view(strokeWidth=0)
        .interactive(),
        width="stretch",
    )

if dropped:
    st.caption(
        f"⚠️ {', '.join(dropped)} not charted: the palette holds eight "
        "colour-blind-safe hues and a ninth would have to be invented. They are "
        "still in the table below."
    )

# --- tier runway ---------------------------------------------------------

runway = dv.tier_runway(board, charted or positions)
if not runway.is_empty():
    st.subheader("How many are left in each tier")
    st.caption(
        "Available players per tier. Tiers are 1-D KMeans within position, so the "
        "breaks sit where the gaps in projected points actually are — the "
        "count that matters is the one in the tier you are about to draft from."
    )
    # Tier goes on the axis and position carries the colour -- the same colour it
    # carries on the curve above. An eight-step blue ramp is what this chart wants
    # to be and cannot: see the note in draft_view.
    runway_domain = [p for p in dv.POSITION_HUES if p in runway["primaryPosition"]]
    bars = (
        alt.Chart(runway.to_pandas())
        .mark_bar(cornerRadiusEnd=4, stroke=None)
        .encode(
            x=alt.X("tier:O", title="Tier (1 is best)",
                    axis=alt.Axis(labelAngle=0, labelColor=ink["text"],
                                  titleColor=ink["muted"], domain=False,
                                  ticks=False)),
            xOffset=alt.XOffset("primaryPosition:N", sort=runway_domain),
            y=alt.Y("remaining:Q", title="Players available",
                    axis=alt.Axis(gridColor=ink["grid"], labelColor=ink["muted"],
                                  titleColor=ink["muted"], domain=False)),
            color=alt.Color(
                "primaryPosition:N",
                scale=alt.Scale(domain=runway_domain,
                                range=[colors[dv.POSITION_HUES[p]]
                                       for p in runway_domain]),
                legend=alt.Legend(title=None, orient="top",
                                  labelColor=ink["text"]),
            ),
            tooltip=[alt.Tooltip("primaryPosition:N", title="Pos"),
                     alt.Tooltip("tier:O", title="Tier"),
                     alt.Tooltip("remaining:Q", title="Left"),
                     alt.Tooltip("best_points:Q", title="Best", format=".1f")],
        )
        .properties(height=240)
    )
    st.altair_chart(bars.configure_view(strokeWidth=0)
                    .configure_scale(bandPaddingInner=0.15), width="stretch")

# --- value on the board --------------------------------------------------

targets = dv.value_targets(board.filter(pl.col("primaryPosition").is_in(positions))
                           if positions else board)
if not targets.is_empty():
    st.subheader("Falling past their price")
    st.caption(
        "`value` is our VOR rank against the market's ADP rank, both ranked over "
        "the same population. Positive means the room is letting them fall."
    )
    st.dataframe(
        dv.display_frame(targets), width="stretch", hide_index=True,
        column_config={"Proj": st.column_config.NumberColumn(format="%.1f"),
                       "VOR": st.column_config.NumberColumn(format="%.1f"),
                       "Value": st.column_config.NumberColumn(format="%+.0f"),
                       "ADP": st.column_config.NumberColumn(format="%.1f")},
    )

# --- the board -----------------------------------------------------------

st.subheader(f"The board — {shown.height:,} players")

sort_options = {
    "Value (market vs us)": ("value", True),
    "VOR": ("vor", True),
    "Projected points": ("TRUE_Points", True),
    "ADP": ("adp", False),
    "Auction value": ("auction_value_filled", True),
}
choice = st.radio("Sort by", list(sort_options), horizontal=True,
                  help="Value first, on purpose — see the page docstring.")
sort_col, descending = sort_options[choice]
table = shown.sort(sort_col, descending=descending, nulls_last=True)

st.dataframe(
    dv.display_frame(table), width="stretch", hide_index=True, height=560,
    column_config={
        "Proj": st.column_config.NumberColumn(format="%.1f"),
        "Floor": st.column_config.NumberColumn(format="%.0f"),
        "Ceil": st.column_config.NumberColumn(format="%.0f"),
        "VOR": st.column_config.NumberColumn(format="%.1f"),
        "VOR rk": st.column_config.NumberColumn(format="%.0f"),
        "Pos rk": st.column_config.NumberColumn(format="%.0f"),
        "ADP": st.column_config.NumberColumn(format="%.1f"),
        "Value": st.column_config.NumberColumn(format="%+.0f"),
        "$": st.column_config.NumberColumn(format="$%.0f"),
        "Bye": st.column_config.NumberColumn(format="%.0f"),
        "Tier": st.column_config.NumberColumn(format="%.0f"),
    },
)

# --- what these numbers do not say --------------------------------------

with st.expander("What is missing from these columns, and why"):
    measured = int(board["floor"].is_not_null().sum()) if "floor" in board.columns else 0
    unpriced = board.height - priced
    st.markdown(f"""
- **`Value` is blank for {unpriced:,} of {board.height:,} players.** ESPN parks
  every player it does not price on one ADP — 758 of 1,000 shared exactly 170.0 in
  2026 — and ranking inside that plateau is noise. A high-VOR player the market has
  no opinion on is its own signal, shown rather than scored.
- **`Value` is also blank for K and D/ST.** Season-total VOR asks "how many more
  points than the last startable player", which assumes you hold one all season.
  You do not; you stream them. The first version of this board scored eight team
  defences as the league's best values on exactly that mistake.
- **`Floor`/`Ceil` are only measured for {measured:,} players.** They are the range
  across the sources that really have a line, and a source imputed from the
  ESPN/FantasyPros mean does not count — otherwise the players nobody has priced
  would report the *narrowest* range, which is backwards. Fewer than two real
  sources means no spread, not a spread of zero. Prior-season variance, the other
  half of what plan 09 asks for here, is not in yet.
- **Half the pool has no projection at all** and is hidden: their blended points are
  a literal 0.0 rather than a null, so they would otherwise sort as the league's
  worst players rather than as unknowns.
- **Draft history is not here.** Points-over-expectation per manager and positional
  tendency by round are roadmap Phase 1, which has not been backfilled.
""")
