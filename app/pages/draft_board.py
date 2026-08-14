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

# One config for every table on the page. Both the board and the value-targets table
# render through `dv.display_frame`, so they share a column vocabulary; they had
# drifted into two literal dicts that agreed on Proj/VOR/Value/ADP by hand. Streamlit
# ignores config for a column a frame does not carry, so one dict serves both.
COLUMN_CONFIG = {
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
    "USG": st.column_config.NumberColumn(
        format="%.1f",
        help="The usage model's own season projection, and the one number on this "
             "table that is **not** comparable to Proj. USG is an expected value "
             "over the games the model expects the player to be available for "
             "(Exp G); Proj assumes a healthy 17. Subtracting the two means "
             "nothing. Δrk is the comparison that survives.",
    ),
    "Δrk": st.column_config.NumberColumn(
        format="%+.0f",
        help="The model's rank within position minus the blend's. Positive means "
             "the model likes the player more than ESPN and FantasyPros do, "
             "negative less. Being a rank, it is immune to the level mismatch that "
             "makes USG and Proj incomparable — which is why the model's dissent is "
             "carried here rather than in Floor/Ceil.",
    ),
    "Exp G": st.column_config.NumberColumn(
        format="%.1f",
        help="Games out of 17 the model expects this player to be available for, "
             "from ESPN's estimated return date where there is one. USG is already "
             "scaled by it, so a low number here explains a low USG that is not a "
             "statement about the player's per-game quality.",
    ),
    "Model evidence": st.column_config.TextColumn(
        help="The model's own account of where its evidence is thin: a prior season "
             "under 8 games, a team change, or bottom-quartile prior volume — each "
             "measured to raise rank error. “—” means it priced the player and "
             "flagged nothing; “withdrawn” means it produced no number at all.",
    ),
}

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

board = dv.with_model_evidence(store.load_board(selection.season,
                                                selection.league_key))
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
        column_config=COLUMN_CONFIG,
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
# Sorting by the model's dissent is how the Δrk column gets used: the interesting
# players are at both ends, so it is offered in both directions rather than one.
if "USG_PosRankDelta" in shown.columns:
    sort_options["Model highest above us"] = ("USG_PosRankDelta", True)
    sort_options["Model lowest below us"] = ("USG_PosRankDelta", False)

choice = st.radio("Sort by", list(sort_options), horizontal=True,
                  help="Value first, on purpose — see the page docstring.")
sort_col, descending = sort_options[choice]
table = shown.sort(sort_col, descending=descending, nulls_last=True)

st.dataframe(
    dv.display_frame(table), width="stretch", hide_index=True, height=560,
    column_config=COLUMN_CONFIG,
)

# --- who you are drafting against ---------------------------------------

if store.has_artifact(selection.season, selection.league_key, "tendencies"):
    tendencies = store.load_tendencies(selection.season, selection.league_key)
    picks = store.load_draft(selection.season, selection.league_key)

    st.subheader("Who you are drafting against")
    st.caption(
        "Every number here is measured against the room the manager was actually "
        f"sitting in, with that manager left out of the baseline — "
        f"{picks['season'].n_unique()} drafts, {picks.height:,} picks, "
        f"{picks['season'].min()}–{picks['season'].max()}. "
        "These are tendencies, not verdicts: nothing here says whether any of it "
        "worked."
    )

    cards = st.columns(2)
    for index, row in enumerate(tendencies.iter_rows(named=True)):
        with cards[index % 2].container(border=True):
            mine = " · you" if row["owner"] == my_owner else ""
            st.markdown(f"**{row['owner_display']}**  \n"
                        f"<span style='opacity:0.7;font-size:0.85em'>"
                        f"{row['headline']}{mine}</span>",
                        unsafe_allow_html=True)
            st.write(row["description"])

    # Deliberately not filtered by the page's position multiselect. That control
    # scopes the *board* -- which players you are browsing -- and its default of
    # QB/RB/WR/TE would hide the single most distinctive tendency these leagues
    # have: the manager who takes a kicker in round 5.
    timing = dv.timing_matrix(picks, dv.TIMED_CHART_POSITIONS)
    if timing.is_empty():
        st.caption(
            "No timing chart for this league: it drafts by auction, where the "
            "order players are nominated in is not what anyone paid for them. The "
            "budget columns below carry the equivalent."
        )
    else:
        st.markdown("**When each position comes off the board**")
        st.caption(
            "Every position, whatever the filter above is set to. Rounds earlier "
            "or later than the rest of the room — left of the line is early. A "
            "manager who never took the position in a season counts as one round "
            "past the end of that draft rather than being dropped: never taking a "
            "kicker is the tendency."
        )
        # Sorted by mean deviation so the aggressive managers are at the top and
        # the patient ones at the bottom; within a row the dots are the positions.
        order = (timing.group_by("owner").agg(pl.col("delta").mean())
                 .sort("delta")["owner"].to_list())

        def _label(name: str) -> str:
            """The manager's rendered name, marking the league's own owner."""
            shown = dv.owner_label(name)
            return f"{shown} (you)" if name == my_owner else shown

        labelled = timing.with_columns(
            pl.col("owner").map_elements(_label, return_dtype=pl.Utf8)
            .alias("manager"))
        row_order = [_label(name) for name in order]

        present_positions = timing["position"].unique().to_list()
        chart_positions = [p for p in dv.TIMED_CHART_POSITIONS
                           if p in present_positions]
        scale = alt.Scale(domain=chart_positions,
                          range=[colors[dv.POSITION_HUES[p]] for p in chart_positions])
        base = alt.Chart(labelled.to_pandas())
        # The room is the zero line, not an average of averages -- each dot is
        # already a deviation from its own draft's own baseline.
        room = (alt.Chart(pl.DataFrame({"zero": [0.0]}).to_pandas())
                .mark_rule(strokeDash=[4, 3], strokeWidth=1, opacity=0.7,
                           color=ink["muted"])
                .encode(x="zero:Q"))
        dots = base.mark_circle(size=110, opacity=0.95, strokeWidth=1.5,
                                stroke=ink["surface"]).encode(
            x=alt.X("delta:Q", title="Rounds vs the room  (left is earlier)",
                    axis=alt.Axis(gridColor=ink["grid"], labelColor=ink["muted"],
                                  titleColor=ink["muted"], domain=False)),
            y=alt.Y("manager:N", title=None, sort=row_order,
                    # labelLimit defaults to 180px, which clipped "Tommy Winfield
                    # (you)" to an ellipsis in the league it matters most in.
                    axis=alt.Axis(labelColor=ink["text"], domain=False,
                                  ticks=False, grid=True, gridColor=ink["grid"],
                                  labelLimit=220)),
            color=alt.Color("position:N", scale=scale,
                            legend=alt.Legend(title=None, orient="top",
                                              labelColor=ink["text"])),
            tooltip=[alt.Tooltip("manager:N", title="Manager"),
                     alt.Tooltip("position:N", title="Pos"),
                     alt.Tooltip("own_round:Q", title="His round", format=".1f"),
                     alt.Tooltip("room_round:Q", title="The room", format=".1f"),
                     alt.Tooltip("delta:Q", title="Difference", format="+.1f"),
                     alt.Tooltip("seasons:Q", title="Drafts", format=".0f")],
        )
        st.altair_chart(
            # 40px a row: six positions land on one row and several cluster near
            # zero, so the band has to be tall enough for them to sit apart.
            (room + dots).properties(height=40 * len(row_order) + 20)
            .configure_view(strokeWidth=0),
            width="stretch",
        )

    with st.expander("The measurements behind these descriptions"):
        st.dataframe(
            dv.tendency_frame(tendencies), width="stretch", hide_index=True,
            column_config={
                "Rds early": st.column_config.NumberColumn(format="%+.1f"),
                "Rds late": st.column_config.NumberColumn(format="%+.1f"),
                "Extra picks": st.column_config.NumberColumn(format="%+.1f"),
                "Rookie %": st.column_config.NumberColumn(format="percent"),
                "Auto %": st.column_config.NumberColumn(format="percent"),
                "Top-3 $": st.column_config.NumberColumn(format="percent"),
            },
        )
        st.markdown("""
- **The baseline leaves the manager out.** In a six-team league a manager is a
  sixth of the room, and including them shrinks their own deviation by 17% — in
  exactly the leagues with the longest history.
- **Nothing is pooled across eras.** Rookie appetite, positional shares and NFL-team
  leans are each measured within a season first and averaged after, because these
  leagues drafted 5 rookies in 2016 and 168 in 2025. Pooled, every long-serving
  manager would read as rookie-averse purely from when they arrived.
- **A tendency needs two drafts.** Managers with one are named and left alone.
- **No outcome is measured.** Whether the kicker in round 5 was a mistake needs
  every past season scored in this league's own rules, which the store does not
  hold. Predictable is what a board can use; correct is a different question.
""")


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
- **Draft history says what managers do, not how it went.** Positional tendency by
  round is above. Points-over-expectation per manager is not: it needs every past
  season scored in this league's own rules, and the store holds one season.
""")

    if "usg_evidence_label" in board.columns:
        label = board["usg_evidence_label"]
        not_modelled = int(label.eq(dv.EVIDENCE_NOT_MODELLED).sum())
        withdrawn = int(label.is_in([dv.EVIDENCE_WITHDRAWN_AVAILABILITY,
                                     dv.EVIDENCE_WITHDRAWN_INJURY]).sum())
        flagged = int(label.is_in([dv.EVIDENCE_CLEAR, dv.EVIDENCE_NOT_MODELLED,
                                   dv.EVIDENCE_WITHDRAWN_AVAILABILITY,
                                   dv.EVIDENCE_WITHDRAWN_INJURY]).not_().sum())
        st.markdown(f"""
- **`USG` is not comparable to `Proj`, and that is not a rounding difference.** The
  model projects an expected value — per-game production times the games it expects
  the player to actually be available for, which `Exp G` shows. `Proj` projects a
  healthy 17-game season, because ESPN and FantasyPros do. Subtracting one from the
  other mixes two quantities and the result means nothing. **`Δrk` is the comparison
  that survives**, being a rank; it is the same reason the model is deliberately kept
  out of `Floor`/`Ceil`, where it once widened the median disagreement interval from
  8.5% to 24.0% by sitting below all four other sources for half the pool.
- **The model contributes a third of `Proj`.** ESPN, FantasyPros and the usage model
  are an equal three-way split; Pinnacle and BetOnline are weighted zero. So `Δrk`
  is not an outside second opinion — it is one of the three voices already inside the
  number to its left, shown separately so you can see it pull.
- **The model says nothing about {not_modelled:,} of {board.height:,} players, and
  withdrew on {withdrawn:,} more.** It has never modelled K or D/ST, and it declines
  a player whose expected games are too low to price or whose injury report withdraws
  them outright. An empty `USG` is one of those three, and `Model evidence` says
  which — a blank there would read as agreement.
- **{flagged:,} players carry a thin-evidence flag,** and the flags were chosen by
  measurement rather than intuition: a prior season under 8 games raises rank error
  42%, a team change 32%, bottom-quartile prior volume 23%. Two plausible candidates
  were *rejected* by the same measurement — one prior season is no worse than two,
  and a rookie orders **14% better** than the pool, so flagging rookies would have
  marked the model's strongest arm as its weakest.
""")
