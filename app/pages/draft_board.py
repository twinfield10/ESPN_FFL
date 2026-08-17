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

**Three tabs, split by what you are doing at that moment.** The page had grown to
one scroll holding a filterable table, two charts, six manager cards and a decade of
acquisition history, which is three jobs stacked vertically:

- **Board** — the working surface. Find a player, filter the pool, read the table.
  Defaults to VOR order: on the tab you drive the draft from, the question is who is
  worth the most, and value against ADP is a second opinion the Values tab is for.
- **Values** — where the room and our valuation disagree, on its own so it is a
  place you go rather than something you scroll past.
- **League** — what does not change during a draft: the shape of this league, the
  positional cliff, the tier runway, and who you are sitting across from.

Each tab carries its own position filter. They are deliberately independent: the
Board's filter is "what am I looking at right now" and gets narrowed constantly,
while the League tab's is "which curves am I comparing" and should not be dragged
around by it.
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
#
# The keys are `dv.DISPLAY_COLUMNS`' labels and have to stay Title Case with them --
# a stale key here does not raise, it silently stops formatting a column.
COLUMN_CONFIG = {
    "Proj": st.column_config.NumberColumn(format="%.1f"),
    "Floor": st.column_config.NumberColumn(format="%.0f"),
    "Ceil": st.column_config.NumberColumn(format="%.0f"),
    "VOR": st.column_config.NumberColumn(format="%.1f"),
    "VOR Rk": st.column_config.NumberColumn(format="%.0f"),
    "Pos Rk": st.column_config.NumberColumn(format="%.0f"),
    "ADP": st.column_config.NumberColumn(format="%.1f"),
    "Value": st.column_config.NumberColumn(format="%+.0f"),
    "$": st.column_config.NumberColumn(
        format="$%.0f",
        help="ESPN's market auction value, rescaled from the \\$200 budget it is "
             "published against to the one set on the Board tab. It is a straight "
             "proportion of a budget, so it does not model the \\$1 minimum bid that "
             "does not scale with one.",
    ),
    "Our $": st.column_config.NumberColumn(
        format="$%.0f",
        help="What this player is worth out of one team's budget on our own "
             "numbers: every roster spot the room fills costs at least \\$1, and "
             "what is left over is split in proportion to points above "
             "replacement. Blank for players outside the money, and for K and "
             "D/ST, where a season-total VOR does not describe a streamed "
             "position.",
    ),
    "Cash +/-": st.column_config.NumberColumn(
        format="%+.0f",
        help="Our price minus ESPN's average auction price, both in this "
             "league's dollars. Positive means the room underpays. This is the "
             "auction answer to the same question `Value` asks of a snake draft — "
             "being four places underrated does not tell you whether to bid \\$41 "
             "or \\$46.",
    ),
    "Keeper $": st.column_config.NumberColumn(
        format="$%.0f",
        help="What it costs the manager holding this player to keep him — which is "
             "what **they** paid to acquire him, not what the draft paid. Measured: "
             "130 of GOP's keepers carry exactly their 2025 auction bid, and where "
             "a player changed hands the price follows the current holder — Jayden "
             "Daniels went for \\$46 and keeps for \\$1. A player claimed off "
             "waivers has no winning bid to record, so ESPN reports 0 and he keeps "
             "for the \\$1 minimum. **Blank means nobody holds him**, which is the "
             "only reason there is no price.",
    ),
    "Keeper +/-": st.column_config.NumberColumn(
        format="%+.0f",
        help="Market price minus keeper price, in this league's dollars. Positive "
             "means keeping him is cheaper than buying him back — the only question "
             "a keeper price is asked. Blank where the market has not priced him.",
    ),
    "Bye": st.column_config.NumberColumn(format="%.0f"),
    "Tier": st.column_config.NumberColumn(format="%.0f"),
    "USG": st.column_config.NumberColumn(
        format="%.1f",
        help="The usage model's own season projection, and the one number on this "
             "table that is **not** comparable to Proj. USG is an expected value "
             "over the games the model expects the player to be available for "
             "(Exp G); Proj assumes a healthy 17. Subtracting the two means "
             "nothing. Δ Rk is the comparison that survives.",
    ),
    "Δ Rk": st.column_config.NumberColumn(
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
    "Model Evidence": st.column_config.TextColumn(
        help="The model's own account of where its evidence is thin: a prior season "
             "under 8 games, a team change, or bottom-quartile prior volume — each "
             "measured to raise rank error. “—” means it priced the player and "
             "flagged nothing; “withdrawn” means it produced no number at all.",
    ),
}

# The acquisition tables, which share a vocabulary with each other rather than with
# the board: their unit is a manager-season, not a player.
ACQUISITION_CONFIG = {
    "Manager": st.column_config.TextColumn(),
    "Seasons": st.column_config.NumberColumn(
        format="%.0f", help="Finished seasons this manager has both a draft and "
                            "played weeks for."),
    "Points": st.column_config.NumberColumn(
        format="%.0f", help="Points scored from a starting slot. Bench and IR are "
                            "excluded — those counted for nobody."),
    "From the Draft": st.column_config.NumberColumn(
        format="%.0f", help="Scored by players this manager drafted themselves."),
    "From the Wire": st.column_config.NumberColumn(
        format="%.0f", help="Scored by everyone else they fielded — waiver claims, "
                            "free agents, trade returns."),
    "% Drafted": st.column_config.NumberColumn(
        format="%.1f%%", help="How much of what they scored their own draft "
                              "supplied."),
    "Moves": st.column_config.NumberColumn(
        format="%.1f",
        help="Distinct players brought in, bench included — claiming a player is a "
             "move whether or not you ever start them. A floor on real "
             "transactions: added, dropped and re-added counts once."),
    "Pts / Move": st.column_config.NumberColumn(
        format="%.1f",
        help="Wire points per player brought in. Blank for a manager who never "
             "made a move, which is not the same as one whose moves returned "
             "nothing."),
}

selection = render_sidebar()
meta = selection.meta

st.title(f"Draft Board · {selection.display_name} {selection.season}")

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

# This league's own budget, from ESPN, as the input's starting value. It really does
# vary -- GOP Degenerates plays for $250 and the other eight for $200 -- while the
# market values on every board are denominated in ESPN's $200 default regardless.
default_budget = dv.league_auction_budget(meta)

# Per-league key: a shared one is remembered across a league change, and a keyed
# widget ignores its `value=` once the key exists -- so GOP's $250 auction rendered
# at Winfield's $200. See dv.budget_key.
#
# Read before the widget that sets it is drawn. Streamlit reruns the whole script
# with session state already updated, so this is the budget the user just typed --
# which is what lets the Values tab price at it even though the input lives on the
# Board tab, and what keeps the $ column right on the same run rather than one late.
budget_state_key = dv.budget_key(selection.league_key)
budget = int(st.session_state.get(budget_state_key, default_budget))

keepers = dv.keeper_count(meta)

board = dv.at_budget(
    dv.with_model_evidence(store.load_board(selection.season, selection.league_key)),
    budget,
)
# Both of these subtract from the market price, so both come after the rescale that
# puts the market price in this league's dollars.
board = dv.with_cash_value(board, meta, budget)
board = dv.with_keeper_price(board, keepers)
theme = getattr(getattr(st.context, "theme", None), "type", "light") or "light"
colors = dv.SERIES_COLORS[theme]
ink = dv.CHART_INK[theme]

# Whether `on_team_id` means anything yet. In a keeper league ESPN carries last
# season's rosters into the new season, so before keepers are declared the column
# says who was on that roster in 2025 -- 252 players across GOP's 16 teams, against
# a keeper limit of 2. Everyone is available until those 2 are named.
pending = dv.keepers_pending(board, keepers)
held_now = sum(dv.rostered_counts(board).values())

starting_slots = meta.get("starting_slots") or {}
my_owner = meta.get("primary_owner")
# Empty while keepers are pending, and for the same reason the availability filter
# is off: those 15 players are last season's roster, not this year's team. Counting
# them as filled slots reports every position as covered and quietly turns the
# roster-needs toggle into a no-op.
my_roster = board.filter(
    pl.col("team_owner").fill_null("") == (my_owner or "\0")
) if "team_owner" in board.columns and not pending else board.head(0)
needed = dv.positions_needed(starting_slots, my_roster["primaryPosition"].to_list())

all_positions = dv.board_positions(board)
default_positions = [p for p in all_positions if p in ("QB", "RB", "WR", "TE")]

board_tab, values_tab, league_tab = st.tabs(["Board", "Values", "League"])

# =========================================================================
# Board — the working surface
# =========================================================================

with board_tab:

    # --- filters ---------------------------------------------------------

    with st.container(border=True):
        top = st.columns([3, 2, 2, 2])
        search = top[0].text_input(
            "Search Players", placeholder="Name contains…",
            help="Matched literally and case-insensitively, so a name with a full "
                 "stop or a hyphen in it searches for itself.",
        )
        positions = top[1].multiselect(
            "Positions", all_positions,
            default=default_positions or all_positions,
            help="Colour is fixed per position, so changing this never recolours "
                 "the charts on the League tab.",
        )
        teams = top[2].multiselect(
            "NFL Teams", dv.board_teams(board),
            help="Empty keeps every team.",
        )
        byes = top[3].multiselect(
            "Bye Weeks", dv.board_byes(board),
            help="Keeps **only** the weeks you pick, so it answers \"who is on bye "
                 "in 10\" rather than \"who is not\". Empty keeps every week.",
        )

        bottom = st.columns([2, 2, 2])
        bottom[0].number_input(
            "Auction Budget", min_value=1, max_value=10_000,
            value=default_budget, step=25, key=budget_state_key,
            help=f"What each team has to spend — {selection.display_name} plays "
                 f"for \\${default_budget} according to ESPN. The stored auction "
                 f"values are market averages against ESPN's own "
                 f"\\${dv.BASE_AUCTION_BUDGET:.0f} default, and the \\$ column "
                 f"rescales them to this.",
        )
        # Off by default while keepers are undeclared, because the thing it filters
        # on is not yet a fact. Still offered rather than hidden: "who was on a
        # roster last year" is a question worth being able to ask, it is just not
        # the same question as "who can I draft".
        only_available = bottom[1].toggle(
            "Available Only", value=not pending,
            help=("Hides players a team already holds. Off by default here: "
                  "keepers are not declared yet, so nobody is held."
                  if pending else
                  "Hides players a team already holds."),
        )
        only_need = bottom[2].toggle(
            "Roster Needs Only", value=False,
            help=("Filters to positions that would fill a starting slot you have "
                  "not filled yet. Pre-draft that is every position, so it does "
                  "nothing until you own players."),
        )

    if pending:
        st.caption(
            f"⚠️ **Keepers are not final.** ESPN carries last season's rosters "
            f"into a keeper league, so this board arrives with **{held_now:,} "
            f"players** shown as held across teams that may each keep only "
            f"**{keepers}**. None of them has been kept yet, so every one is "
            f"still available and *Available Only* is off. It turns itself back "
            f"on once rosters shrink to the keeper limit."
        )

    if only_need and needed:
        positions = [p for p in positions if p in needed] or needed

    shown = dv.filter_board(board, positions, only_available=only_available,
                            search=search, teams=teams, byes=byes)

    # --- the board -------------------------------------------------------

    if shown.is_empty():
        st.warning("Nothing matches these filters.")
    else:
        # VOR leads, and that is the change this tab makes. Value against ADP is a
        # second opinion about the *room*, which is the Values tab's whole subject;
        # the surface you draft from should answer "who is worth the most" first.
        sort_options = {
            "VOR": ("vor", True),
            "Value (Market vs Us)": ("value", True),
            # The auction equivalent of the row above it, and next to it on purpose.
            "Cash (Market vs Us)": ("cash_delta", True),
            "Projected Points": ("TRUE_Points", True),
            "ADP": ("adp", False),
            "Auction Value": ("auction_dollars", True),
        }
        # Sorting by the model's dissent is how the Δ Rk column gets used: the
        # interesting players are at both ends, so it is offered in both directions
        # rather than one.
        if "USG_PosRankDelta" in shown.columns:
            sort_options["Model Highest Above Us"] = ("USG_PosRankDelta", True)
            sort_options["Model Lowest Below Us"] = ("USG_PosRankDelta", False)
        # Only in a keeper league, where the column exists at all. "Which keepers
        # are underpriced" is the question the price is looked up to answer, and
        # sorting is the whole way to ask it of 187 rows.
        if "keeper_surplus" in shown.columns:
            sort_options["Keeper Bargain"] = ("keeper_surplus", True)

        sort_options = {label: pair for label, pair in sort_options.items()
                        if pair[0] in shown.columns}

        st.subheader(f"The Board — {shown.height:,} Players")
        choice = st.radio("Sort By", list(sort_options), horizontal=True,
                          help="VOR first, on purpose — see the page docstring.")
        sort_col, descending = sort_options[choice]
        table = shown.sort(sort_col, descending=descending, nulls_last=True)

        st.dataframe(
            dv.display_frame(table), width="stretch", hide_index=True, height=560,
            column_config=COLUMN_CONFIG,
        )

    # --- what these numbers do not say ------------------------------------

    priced = int(board["adp_is_priced"].sum()) if "adp_is_priced" in board.columns else 0

    with st.expander("What Is Missing From These Columns, and Why"):
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
- **`$` is a rescale, not a valuation.** It is ESPN's market average moved from its
  \\${dv.BASE_AUCTION_BUDGET:.0f} budget to this league's, in straight proportion. A
  real auction's minimum bid does not scale with the budget, so the top of the board
  is worth slightly more than the multiple says — and nothing here knows what your
  room actually pays for a quarterback.
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
  round is on the League tab. Points-over-expectation per manager is not: it needs
  every past season scored in this league's own rules, and the store holds one
  season.
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
  other mixes two quantities and the result means nothing. **`Δ Rk` is the comparison
  that survives**, being a rank; it is the same reason the model is deliberately kept
  out of `Floor`/`Ceil`, where it once widened the median disagreement interval from
  8.5% to 24.0% by sitting below all four other sources for half the pool.
- **The model contributes a third of `Proj`.** ESPN, FantasyPros and the usage model
  are an equal three-way split; Pinnacle and BetOnline are weighted zero. So `Δ Rk`
  is not an outside second opinion — it is one of the three voices already inside the
  number to its left, shown separately so you can see it pull.
- **The model says nothing about {not_modelled:,} of {board.height:,} players, and
  withdrew on {withdrawn:,} more.** It has never modelled K or D/ST, and it declines
  a player whose expected games are too low to price or whose injury report withdraws
  them outright. An empty `USG` is one of those three, and `Model Evidence` says
  which — a blank there would read as agreement.
- **{flagged:,} players carry a thin-evidence flag,** and the flags were chosen by
  measurement rather than intuition: a prior season under 8 games raises rank error
  42%, a team change 32%, bottom-quartile prior volume 23%. Two plausible candidates
  were *rejected* by the same measurement — one prior season is no worse than two,
  and a rookie orders **14% better** than the pool, so flagging rookies would have
  marked the model's strongest arm as its weakest.
""")

# =========================================================================
# Values — where the room and our valuation disagree
# =========================================================================

with values_tab:
    st.subheader("Falling Past Their Price")

    # Which question "value" means, and it is a property of the draft rather than a
    # preference: a snake pick is a place in a queue, so rank-against-rank is the
    # comparison; an auction has no queue, only a price. Auction leagues open on
    # Cash. Both stay available -- an auction manager still benefits from knowing
    # the room takes a player two rounds late.
    lenses = [dv.VALUE_LENS_ADP, dv.VALUE_LENS_CASH]
    available_lenses = [l for l in lenses
                        if dv.VALUE_LENS_COLUMNS[l][0] in board.columns]
    preferred = dv.default_value_lens(meta)
    lens = st.radio(
        "Measure Value By", available_lenses or lenses,
        index=(available_lenses.index(preferred)
               if preferred in available_lenses else 0),
        horizontal=True, key="value_lens",
        help="ADP compares our VOR rank to the market's draft position. Cash "
             "compares our dollar valuation to what the room actually pays.",
    )

    if lens == dv.VALUE_LENS_CASH:
        st.caption(
            f"Our dollar valuation against ESPN's average auction price, both at "
            f"**\\${budget}**. We split what the room has left after every one of "
            f"its {dv.draftable_spots(meta):,} roster spots costs at least "
            f"\\$1, in proportion to points above replacement. Positive means the "
            f"market is underpaying."
        )
    else:
        st.caption(
            "`value` is our VOR rank against the market's ADP rank, both ranked "
            "over the same population. Positive means the room is letting them "
            "fall."
        )

    value_positions = st.multiselect(
        "Positions", all_positions, default=default_positions or all_positions,
        key="value_positions",
        help="Independent of the Board tab's filter — this one answers \"where is "
             "the room wrong about running backs\", which is a different question "
             "from what you are browsing.",
    )
    depth = st.slider("How Many to Show", min_value=5, max_value=50, value=12,
                      step=1, key="value_depth")

    targets = dv.value_targets(
        board.filter(pl.col("primaryPosition").is_in(value_positions))
        if value_positions else board,
        limit=depth,
        only_available=not pending,
        lens=lens,
    )

    if targets.is_empty():
        st.info(
            "Nothing to show. Both lenses need a price the market actually set, and "
            "neither is computed for the positions you stream — so a selection of "
            "only K and D/ST has no rows by construction, not by accident."
        )
    else:
        st.dataframe(
            dv.display_frame(targets), width="stretch", hide_index=True,
            column_config=COLUMN_CONFIG,
        )
        st.caption(
            f"{targets.height} of {board.height:,} players. The rest are either "
            "unpriced by the market or streamed — see *What Is Missing From These "
            "Columns* on the Board tab."
        )

# =========================================================================
# League — what does not change during a draft
# =========================================================================

with league_tab:

    # --- toplines --------------------------------------------------------

    replacement = (
        board.filter(pl.col("replacement_rank").is_not_null())
        .group_by("primaryPosition").agg(pl.col("replacement_rank").first())
        .sort("primaryPosition")
    )
    cols = st.columns(4)
    cols[0].metric("Teams", meta.get("team_count", "?"))
    cols[1].metric("Pool", f"{board.height:,}")
    projected = (int((~board["projection_missing"]).sum())
                 if "projection_missing" in board.columns else board.height)
    cols[2].metric("With a Real Projection", f"{projected:,}")
    priced = int(board["adp_is_priced"].sum()) if "adp_is_priced" in board.columns else 0
    cols[3].metric("Priced by the Market", f"{priced:,}")

    st.caption(
        "Replacement level, from this league's own starting slots: "
        + " · ".join(f"**{row[0]}{int(row[1])}**" for row in replacement.iter_rows())
    )

    league_positions = st.multiselect(
        "Positions", all_positions, default=default_positions or all_positions,
        key="league_positions",
        help="Scopes the two charts below. Kept apart from the Board tab's filter "
             "on purpose: which curves you are comparing is not the same question "
             "as which players you are browsing.",
    )

    charted = [p for p in league_positions if p in dv.POSITION_HUES]
    dropped = [p for p in league_positions if p not in dv.POSITION_HUES]

    # --- the scarcity curve ----------------------------------------------

    st.subheader("The Positional Cliff")
    st.caption(
        "Projected points against rank within position, out to "
        f"{dv.SCARCITY_DEPTH:g}× replacement level. Where a line falls away steeply "
        "is where waiting costs you; where it is flat, wait. The dashed rule is "
        "this league's replacement level for that position."
    )

    curve = dv.scarcity_curve(
        dv.filter_board(board, charted, only_available=not pending), charted)

    if curve.is_empty():
        st.info("No projected players in this selection to chart.")
    else:
        domain = [p for p in dv.POSITION_HUES if p in charted]
        scale = alt.Scale(domain=domain,
                          range=[colors[dv.POSITION_HUES[p]] for p in domain])
        # One colour encoding, reused by every layer. Vega-Lite shares the colour
        # scale across a layered chart and a `legend: null` on any layer suppresses
        # the merged legend -- which is how the first version of this shipped with
        # direct labels and no legend at all.
        hue = alt.Color("primaryPosition:N", scale=scale,
                        legend=alt.Legend(title=None, orient="top",
                                          labelColor=ink["text"]))
        base = alt.Chart(curve.to_pandas())

        # Room at the right end for the direct labels, which sit past the last
        # point. Chart `padding` does not do this -- it pads outside the plotting
        # area, and the label is drawn at a data position inside it, so it gets
        # clipped instead.
        rank_max = float(curve["pos_rank"].max()) * 1.06 + 1

        lines = base.mark_line(strokeWidth=2, interpolate="monotone").encode(
            x=alt.X("pos_rank:Q", title="Rank Within Position",
                    scale=alt.Scale(nice=False, domainMax=rank_max, zero=False),
                    axis=alt.Axis(grid=False, labelColor=ink["muted"],
                                  titleColor=ink["muted"], domainColor=ink["grid"])),
            y=alt.Y("TRUE_Points:Q", title="Projected Season Points",
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
                     alt.Tooltip("pos_rank:Q", title="Pos Rank", format=".0f"),
                     alt.Tooltip("TRUE_Points:Q", title="Projected", format=".1f"),
                     alt.Tooltip("tier:Q", title="Tier", format=".0f")],
        )
        # Direct labels, which the light palette's sub-3:1 slots oblige and which
        # also make the chart readable without tracing a legend colour. Anchored
        # right of the last point, so the chart needs padding for them.
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
            "still in the table on the Board tab."
        )

    # --- tier runway -----------------------------------------------------

    runway = dv.tier_runway(board, charted or league_positions,
                            only_available=not pending)
    if not runway.is_empty():
        st.subheader("How Many Are Left in Each Tier")
        st.caption(
            "Available players per tier. Tiers are 1-D KMeans within position, so "
            "the breaks sit where the gaps in projected points actually are — the "
            "count that matters is the one in the tier you are about to draft from."
        )
        # Tier goes on the axis and position carries the colour -- the same colour
        # it carries on the curve above. An eight-step blue ramp is what this chart
        # wants to be and cannot: see the note in draft_view.
        runway_domain = [p for p in dv.POSITION_HUES if p in runway["primaryPosition"]]
        bars = (
            alt.Chart(runway.to_pandas())
            .mark_bar(cornerRadiusEnd=4, stroke=None)
            .encode(
                x=alt.X("tier:O", title="Tier (1 Is Best)",
                        axis=alt.Axis(labelAngle=0, labelColor=ink["text"],
                                      titleColor=ink["muted"], domain=False,
                                      ticks=False)),
                xOffset=alt.XOffset("primaryPosition:N", sort=runway_domain),
                y=alt.Y("remaining:Q", title="Players Available",
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

    # --- who you are drafting against ------------------------------------

    if store.has_artifact(selection.season, selection.league_key, "tendencies"):
        tendencies = dv.notes_for_board(
            store.load_tendencies(selection.season, selection.league_key), board)
        picks = store.load_draft(selection.season, selection.league_key)

        st.subheader("Who You Are Drafting Against")
        st.caption(
            "Every number here is measured against the room the manager was "
            "actually sitting in, with that manager left out of the baseline — "
            f"{picks['season'].n_unique()} drafts, {picks.height:,} picks, "
            f"{picks['season'].min()}–{picks['season'].max()}. "
            "These are tendencies, not verdicts: nothing here says whether any of "
            "it worked."
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

        # Deliberately not filtered by this tab's position multiselect. That control
        # scopes the *charts above it*, and its default of QB/RB/WR/TE would hide the
        # single most distinctive tendency these leagues have: the manager who takes
        # a kicker in round 5.
        timing = dv.timing_matrix(picks, dv.TIMED_CHART_POSITIONS)
        if timing.is_empty():
            st.caption(
                "No timing chart for this league: it drafts by auction, where the "
                "order players are nominated in is not what anyone paid for them. "
                "The budget columns below carry the equivalent."
            )
        else:
            st.markdown("**When Each Position Comes Off the Board**")
            st.caption(
                "Every position, whatever the filter above is set to. Rounds "
                "earlier or later than the rest of the room — left of the line is "
                "early. A manager who never took the position in a season counts as "
                "one round past the end of that draft rather than being dropped: "
                "never taking a kicker is the tendency."
            )
            # Sorted by mean deviation so the aggressive managers are at the top and
            # the patient ones at the bottom; within a row the dots are the positions.
            order = (timing.group_by("owner").agg(pl.col("delta").mean())
                     .sort("delta")["owner"].to_list())

            def _label(name: str) -> str:
                """The manager's rendered name, marking the league's own owner."""
                shown_name = dv.owner_label(name)
                return f"{shown_name} (you)" if name == my_owner else shown_name

            labelled = timing.with_columns(
                pl.col("owner").map_elements(_label, return_dtype=pl.Utf8)
                .alias("manager"))
            row_order = [_label(name) for name in order]

            present_positions = timing["position"].unique().to_list()
            chart_positions = [p for p in dv.TIMED_CHART_POSITIONS
                               if p in present_positions]
            scale = alt.Scale(domain=chart_positions,
                              range=[colors[dv.POSITION_HUES[p]]
                                     for p in chart_positions])
            base = alt.Chart(labelled.to_pandas())
            # The room is the zero line, not an average of averages -- each dot is
            # already a deviation from its own draft's own baseline.
            room = (alt.Chart(pl.DataFrame({"zero": [0.0]}).to_pandas())
                    .mark_rule(strokeDash=[4, 3], strokeWidth=1, opacity=0.7,
                               color=ink["muted"])
                    .encode(x="zero:Q"))
            dots = base.mark_circle(size=110, opacity=0.95, strokeWidth=1.5,
                                    stroke=ink["surface"]).encode(
                x=alt.X("delta:Q", title="Rounds vs the Room  (Left Is Earlier)",
                        axis=alt.Axis(gridColor=ink["grid"], labelColor=ink["muted"],
                                      titleColor=ink["muted"], domain=False)),
                y=alt.Y("manager:N", title=None, sort=row_order,
                        # labelLimit defaults to 180px, which clipped "Tommy
                        # Winfield (you)" to an ellipsis in the league it matters
                        # most in.
                        axis=alt.Axis(labelColor=ink["text"], domain=False,
                                      ticks=False, grid=True, gridColor=ink["grid"],
                                      labelLimit=220)),
                color=alt.Color("position:N", scale=scale,
                                legend=alt.Legend(title=None, orient="top",
                                                  labelColor=ink["text"])),
                tooltip=[alt.Tooltip("manager:N", title="Manager"),
                         alt.Tooltip("position:N", title="Pos"),
                         alt.Tooltip("own_round:Q", title="His Round", format=".1f"),
                         alt.Tooltip("room_round:Q", title="The Room", format=".1f"),
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

        # --- and how much of it the draft actually supplied ----------------
        #
        # Everything above this point is what managers *do*; this is the first thing
        # on the tab that touches how it went. It needs finished seasons, so it looks
        # back rather than at the season being drafted, and it reads `results` rather
        # than `lineups` because `lineups` cannot be built for a past season at all --
        # it carries FantasyPros columns and FantasyPros serves no season parameter.
        # `results` where it exists and `lineups` otherwise. The two carry the same
        # five columns this needs, and preferring results is what reaches back past
        # 2025 -- but a league that has only ever had lineups built still answers for
        # the season it has, rather than losing the section because the newer artifact
        # has not been backfilled for it yet.
        def _played(season: int):
            for artifact, load in (("results", store.load_results),
                                   ("lineups", store.load_lineups)):
                if store.has_artifact(season, selection.league_key, artifact):
                    return load(season, selection.league_key)
            return None

        played = {season: frame
                  for season in sorted(picks["season"].unique().to_list())
                  if season < selection.season
                  for frame in [_played(season)] if frame is not None}
        history = dv.acquisition_history(picks, played)

        if not history.is_empty():
            seasons = sorted(history["season"].unique().to_list())
            st.markdown("**Where the Points Came From**")
            st.caption(
                f"Points scored **from a starting slot** — bench and IR excluded, "
                f"because those counted for nobody — split by who put the player on "
                f"the roster. {len(seasons)} finished season"
                f"{'s' if len(seasons) != 1 else ''}, {seasons[0]}–{seasons[-1]}. "
                "A player is drafted for whoever took them and added for whoever "
                "picked them up afterwards, so a mid-season pickup counts to the "
                "manager who made the move."
            )

            averages = dv.acquisition_averages(history)
            st.dataframe(dv.acquisition_frame(averages), width="stretch",
                         hide_index=True, column_config=ACQUISITION_CONFIG)
            st.caption(
                "Per-season averages, so nobody is weighted by how many seasons "
                "they happened to play. **Moves** counts the distinct players a "
                "manager brought in — bench included, because claiming a player is "
                "a move whether or not you ever start them — and **Pts / Move** is "
                "what those pickups returned from a starting slot. It is a floor on "
                "real transactions: a player added, dropped and re-added counts "
                "once."
            )

            # --- one season at a time --------------------------------------
            st.markdown("**One Season at a Time**")
            chosen = st.selectbox(
                "Season", seasons, index=len(seasons) - 1,
                help="Every finished season this league has both a draft and played "
                     "weeks for. 2016–2018 are absent because ESPN serves no box "
                     "scores before 2019.",
            )
            split = history.filter(pl.col("season") == chosen)

            # A team whose name is not in that season's draft carries nulls rather
            # than zeros, so it is named rather than drawn as having drafted nobody.
            unmatched = split.filter(pl.col("drafted").is_null())
            split = split.filter(pl.col("drafted").is_not_null())

            if not unmatched.is_empty():
                st.caption(
                    "⚠️ Left out, because their draft could not be matched to them: "
                    + ", ".join(f"**{name}**" for name in unmatched["manager"])
                    + ". Shown as missing rather than as having drafted nobody."
                )

            totals = split.with_columns(
                pl.when(pl.col("owner") == my_owner)
                .then(pl.col("manager") + pl.lit(" (you)"))
                .otherwise(pl.col("manager")).alias("who"))
            bar_order = totals["who"].to_list()

            long = (totals.unpivot(on=["drafted", "added"],
                                   index=["who", "total", "share_drafted"],
                                   variable_name="source", value_name="points")
                    # An explicit rank, because ordering on the label would stack
                    # them alphabetically and put "Added in Season" at the baseline.
                    # Drafted belongs against the axis: it is what came first.
                    .with_columns(
                        pl.col("source").replace_strict({"drafted": 0, "added": 1},
                                                        return_dtype=pl.Int8)
                        .alias("stack_rank"),
                        pl.col("source").replace_strict(
                            {"drafted": dv.SOURCE_DRAFTED,
                             "added": dv.SOURCE_ADDED})))

            # Slots 1 and 2, the palette's fixed first pair for a two-series
            # categorical. Validated against both surfaces rather than assumed:
            # worst adjacent separation is dE 24.7 protan on white and 26.8 on the
            # dark surface, well clear of the 8 the check asks for.
            source_scale = alt.Scale(domain=[dv.SOURCE_DRAFTED, dv.SOURCE_ADDED],
                                     range=[colors[1], colors[2]])
            bars = (alt.Chart(long.to_pandas())
                    # 1px of surface on each segment is the 2px gap between fills;
                    # without it a manager who added almost nothing reads as one bar.
                    .mark_bar(cornerRadiusEnd=4, stroke=ink["surface"], strokeWidth=1)
                    .encode(
                        x=alt.X("points:Q", title="Points From a Starting Slot",
                                stack=True,
                                axis=alt.Axis(gridColor=ink["grid"],
                                              labelColor=ink["muted"],
                                              titleColor=ink["muted"], domain=False)),
                        y=alt.Y("who:N", title=None, sort=bar_order,
                                axis=alt.Axis(labelColor=ink["text"], domain=False,
                                              ticks=False, grid=False,
                                              labelLimit=220)),
                        color=alt.Color("source:N", scale=source_scale,
                                        legend=alt.Legend(title=None, orient="top",
                                                          labelColor=ink["text"])),
                        order=alt.Order("stack_rank:Q"),
                        tooltip=[alt.Tooltip("who:N", title="Manager"),
                                 alt.Tooltip("source:N", title="From"),
                                 alt.Tooltip("points:Q", title="Points", format=".1f"),
                                 alt.Tooltip("total:Q", title="Season Total",
                                             format=".1f"),
                                 alt.Tooltip("share_drafted:Q", title="% Drafted",
                                             format=".1f")],
                    ))
            # One direct label a bar, not one a segment: the share is the number the
            # chart is actually about, and it wears a text token rather than a series
            # colour so identity stays with the marks.
            labels = (alt.Chart(totals.to_pandas())
                      .mark_text(align="left", dx=6, fontSize=12, color=ink["muted"])
                      .encode(x=alt.X("total:Q"), y=alt.Y("who:N", sort=bar_order),
                              text=alt.Text("share_drafted:Q", format=".0f")))
            st.altair_chart(
                (bars + labels).properties(height=40 * split.height + 20)
                .configure_view(strokeWidth=0),
                width="stretch",
            )
            st.caption(
                f"The number past each bar is the percentage that came from the "
                f"draft. {chosen} range: {split['share_drafted'].min():.0f}%–"
                f"{split['share_drafted'].max():.0f}%."
            )
            with st.expander(f"{chosen} in Numbers"):
                st.dataframe(dv.acquisition_frame(split), width="stretch",
                             hide_index=True, column_config=ACQUISITION_CONFIG)
