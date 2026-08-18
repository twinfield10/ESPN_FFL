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

**Four tabs, split by what you are doing at that moment.** The page had grown to
one scroll holding a filterable table, two charts, six manager cards and a decade of
acquisition history, which is three jobs stacked vertically:

- **Board** — the working surface. Find a player, filter the pool, read the table.
  Defaults to VOR order: on the tab you drive the draft from, the question is who is
  worth the most, and value against ADP is a second opinion the Values tab is for.
- **Values** — where the room and our valuation disagree, on its own so it is a
  place you go rather than something you scroll past.
- **League** — what does not change during a draft: the shape of this league, the
  positional cliff, the tier runway, and who you are sitting across from.
- **Calibration** — where *we* disagree with ESPN, and whether that disagreement is
  a player or the model. The only tab that is not read during a draft: it is where
  you go beforehand to decide whether the numbers the other three are built on are
  ones you believe.

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


def _column_config(frame, lens, click_key=None):
    """Streamlit column config for a rendered board frame, keyed by position.

    Generated from `dv.COLUMNS` rather than hand-written, which closes two problems
    the literal dict had. It could not be keyed by label any more -- `ESPN`, `Us` and
    `Δ` each repeat across the spanner groups, so one key would format four different
    columns -- and it could not be tested, because Streamlit ignores config for a
    column a frame does not carry: a label that stopped existing stopped formatting in
    silence. Positions read off the frame can do neither.

    The `help=` tooltip is the glossary's own prose, so the two cannot disagree about
    what a column means. Escaped for markdown; `format=` is printf and is left alone.

    Args:
        frame: A `dv.display_frame` result.
        lens: Which currency the Draft Metric group speaks.

    Returns:
        dict: Column position to a `st.column_config` object.
    """
    config = {}
    for position, column in dv.column_config_specs(frame, lens).items():
        tooltip = dv.escape_dollars(
            f"{column.how} {column.caveat}".strip() if column.caveat else column.how)
        if column.kind == "number":
            config[position] = st.column_config.NumberColumn(
                format=column.fmt, help=tooltip, pinned=column.pinned)
        elif column.kind == "button" and click_key:
            # The cell value *is* the button label, which is what makes the mark
            # itself the thing you click. `key=` is what enables the click at all;
            # without one the column renders as inert buttons. Tertiary so it reads
            # as an icon in a table rather than a row of raised controls.
            config[position] = st.column_config.ButtonColumn(
                help=tooltip, width=column.width, type="tertiary", key=click_key)
        else:
            # No `format=`: TextColumn does not take one. A text spec carries no
            # `fmt` either, so there is nothing to pass on. A button spec with no
            # `click_key` lands here too and renders as plain text -- degraded, but
            # readable, rather than a button that does nothing when pressed.
            config[position] = st.column_config.TextColumn(
                help=tooltip, pinned=column.pinned, width=column.width)
    return config


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

# The Calibration tab's two tables. Its first six columns are the board's own
# vocabulary and reuse the board's tooltips through `dv.COLUMNS`; only the two
# columns that exist nowhere else need prose written here.
AGREEMENT_CONFIG = {
    "Player": st.column_config.TextColumn(pinned=True),
    "Pos": st.column_config.TextColumn(pinned=True),
    "NFL": st.column_config.TextColumn(),
    "ESPN": st.column_config.NumberColumn(format="%.1f"),
    "Us": st.column_config.NumberColumn(format="%.1f"),
    "Δ": st.column_config.NumberColumn(
        format="%+.1f", help="Us − ESPN. Positive means we project more points."),
    "σ vs Position": st.column_config.NumberColumn(
        format="%+.2f",
        help="How many standard deviations this player's Δ sits from the average Δ "
             "*at his own position*, measured over the players currently shown. ±2 "
             "is the usual place to start looking."),
    "ADP": st.column_config.NumberColumn(
        format="%.1f",
        help="The market's average draft position. Around 170 means the market has "
             "no opinion at all — every unpriced player is parked on one value."),
    "Tier": st.column_config.NumberColumn(format="%.0f"),
}

AGREEMENT_SUMMARY_CONFIG = {
    "Pos": st.column_config.TextColumn(),
    "Players": st.column_config.NumberColumn(format="%.0f"),
    "Mean Δ": st.column_config.NumberColumn(
        format="%+.1f",
        help="The average gap at this position. A large one is an offset the blend "
             "applies to everybody here, which is a model question rather than a "
             "player one."),
    "Spread": st.column_config.NumberColumn(
        format="%.1f",
        help="Standard deviation of the gap. Small next to a large mean means "
             "systematic; large next to a small mean means we agree about the "
             "position and argue about individuals."),
    "We're Higher": st.column_config.NumberColumn(
        format="%.0f%%",
        help="Share of the position we project above ESPN on. 50% is an even "
             "split; 100% is a one-way offset."),
    "Scored": st.column_config.TextColumn(
        help="Whether players here carry a σ. 'no' means the position has too few "
             "players shown, or too little disagreement to measure one — kickers "
             "have no second source, so our number *is* ESPN's."),
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
# Abbreviates the injury status and turns the estimated return into something a
# reader can act on. Last, so it sees whichever of those columns the artifact has.
board = dv.with_injury_code(board)

# Which currency the Draft Metric group speaks: a snake draft has a queue, so the
# comparison is rank against rank; an auction has a price. Read from the league's
# own ESPN draft settings rather than offered as a toggle, because it is a fact
# about the draft. The Values tab lets you ask for the other one.
board_lens = dv.default_value_lens(meta)
# Measured on the unfiltered board so the fills do not shift when a filter changes.
shading = dv.shade_scales(board, board_lens)

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

board_tab, values_tab, league_tab, calibration_tab = st.tabs(
    ["Board", "Values", "League", "Calibration"])

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
        # Sorting by a disagreement is how a Δ column gets used, and the interesting
        # players are at *both* ends of one, so each is offered in both directions
        # rather than one. The positional Δ leads the two ESPN comparisons because a
        # positional rank is what you choose between on the clock.
        if "pos_rank_delta" in shown.columns:
            sort_options["We're Highest Above ESPN"] = ("pos_rank_delta", True)
            sort_options["ESPN Highest Above Us"] = ("pos_rank_delta", False)
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

        frame = dv.display_frame(table, board_lens)
        st.dataframe(
            dv.styled_frame(frame, shading, theme),
            width="stretch", hide_index=True, height=560,
            column_config=_column_config(frame, board_lens,
                                         click_key=dv.NOTE_CLICK_KEY),
            # A blank cell rather than the word "None". Streamlit's default for a
            # missing value is the literal string, which on `Exp Return` and on every
            # unpriced player's `Δ` reads as an answer rather than as an absence.
            placeholder="",
            # Lazy loading is incompatible with both the spanner headers and the
            # Styler, and at ~2,500 rows Streamlit would not choose it anyway. Said
            # out loud so that stays true rather than lucky.
            lazy=False,
        )

        # The note behind the mark, remembered by *player* rather than by row.
        #
        # A click reports a row number, which is a position in `table` -- the sorted,
        # filtered frame those rows were drawn from -- and means nothing the moment the
        # sort changes. Resolving it to a player id immediately is what keeps the panel
        # pointing at the player you asked about after a re-sort, rather than at
        # whoever now occupies that row.
        held = dv.remember_note_click(st.session_state, table,
                                      league_key=selection.league_key)
        note = dv.player_note_for(board, held) if held is not None else None
        if note:
            with st.container(border=True):
                st.markdown(note)

        st.caption(
            "The five identity columns are frozen — scroll sideways and they stay. "
            "**Δ** columns are the differences, and they read the same way "
            "everywhere: green and positive means we are higher on the player than "
            "ESPN or the room is. The sign is printed as well as coloured, so the "
            "column does not depend on telling green from red. Under *News*, click "
            "the note icon to read what ESPN's injury report says about him."
        )

    # --- what every column is, and what it does not say --------------------
    #
    # One expander, not two. The glossary and the "what is missing" panel had grown
    # into two accounts of the same twenty-odd columns, and a caveat is only useful
    # next to the column it is about -- a reader who wants to know why `Δ` is blank
    # should not have to know that the answer lives in the second of two panels.
    #
    # Scoped to the columns this league's board actually renders, so a redraft league
    # is not told about keeper prices it does not have, and grouped by spanner so the
    # reference reads in the same order as the table above it.
    #
    # What stays as prose below the tables is what is *not* about a column: the shape
    # of the pool, and what the artifact as a whole cannot answer. Those have no cell
    # to sit in.

    with st.expander("Glossary — Where Every Column Comes From"):
        # Scoped to the whole board's columns rather than the filtered table's, so
        # the reference does not shrink when a filter empties the table.
        st.markdown(dv.glossary_markdown(
            dv.display_frame(board, board_lens).columns, board_lens))
        st.caption(
            "Source is where the number *originates*, not where it is stored — "
            "every column on this page is read out of one parquet file, which is "
            "not the useful answer. **Board build** is computed once by "
            "`Scripts.refresh --what board`; **Derived here** is computed by the "
            "page, because it depends on the budget you set above."
        )

        priced = (int(board["adp_is_priced"].sum())
                  if "adp_is_priced" in board.columns else 0)
        unpriced = board.height - priced
        st.markdown(f"""
#### What none of these columns say

- **The market has no opinion on {unpriced:,} of {board.height:,} players.** ESPN
  parks every player it does not price on one ADP — 758 of 1,000 shared exactly 170.0
  in 2026 — so `Draft Metric` and its Δ are blank for most of the pool. A high-VOR
  player the market has no opinion on is its own signal, shown rather than scored.
- **Half the pool has no projection at all** and is hidden: their blended points are
  a literal 0.0 rather than a null, so they would otherwise sort as the league's
  worst players rather than as unknowns.
- **The two ESPN columns are two different opinions, and neither is the room.**
  `Points | ESPN` is ESPN's projection; `Ranks | ESPN` is ESPN's editors; the room is
  `Draft Metric`. All three disagree, which is most of what this table is for.
- **The disagreement between forecasters is no longer shown.** `Floor` and `Ceil` —
  the range across the sources with a real, non-imputed line — were measured for only
  221 of 2,503 players and were cut when the table was grouped. Prior-season variance,
  the other half of what plan 09 asks for there, was never in.
- **Draft history says what managers do, not how it went.** Positional tendency by
  round is on the League tab. Points-over-expectation per manager is not: it needs
  every past season scored in this league's own rules, and the store holds one
  season.
""")

        if "usg_evidence_label" in board.columns:
            label = board["usg_evidence_label"]
            not_modelled = int(label.eq(dv.EVIDENCE_NOT_MODELLED).sum())
            withdrawals = [dv.EVIDENCE_WITHDRAWN_AVAILABILITY,
                           dv.EVIDENCE_WITHDRAWN_INJURY,
                           dv.EVIDENCE_WITHDRAWN_ROLE]
            withdrawn = int(label.is_in(withdrawals).sum())
            backups = int(label.eq(dv.EVIDENCE_WITHDRAWN_ROLE).sum())
            flagged = int(label.is_in([dv.EVIDENCE_CLEAR, dv.EVIDENCE_NOT_MODELLED,
                                       *withdrawals]).not_().sum())
            st.markdown(f"""
- **`Points | USG` is on the same footing as the columns beside it**, and has been
  since 2026-08-07: the model's line is put on a full healthy slate before it is
  blended, so all of them describe a 17-game season. `Exp G` shows the availability
  view separately rather than being baked in. **`Position Ranks | Δ USG` is still the
  cleaner comparison**, because the model shrinks toward positional baselines while
  ESPN extrapolates — so it reads a few percent low at the top of the board, which is
  disagreement about players and not about units. It is the same residual that keeps
  the model out of the floor/ceiling spread, where it still sits below all four other
  sources for 47% of draftable players.
- **The model says nothing about {not_modelled:,} of {board.height:,} players, and
  withdrew on {withdrawn:,} more — {backups:,} of those as backups.** It has never
  modelled K or D/ST; it declines a player whose expected games are too low to price
  or whose injury report withdraws them outright; and the board build withdraws it
  where the depth chart says backup *and* ESPN has priced him out, because a starter's
  slate is the wrong basis for a man who will not play. An empty `USG` is one of those
  four, and `Model Evidence` says which — a blank there would read as agreement.
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
                        if dv.VALUE_LENS_COLUMNS[l] in board.columns]
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
            f"\\$1, in proportion to points above replacement — so our side is what to "
            f"spend out of the budget you have. It is aggressive by construction: the "
            f"whole budget goes to the players worth rostering while the market spreads "
            f"it over three times as many, so read this list as an ordering of who the "
            f"room is *most* wrong about. Positive means the room is underpaying."
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
        # The lens the radio chose, not the league's default -- here it is the
        # question the reader asked, so the Draft Metric group answers in that
        # currency even in a league whose draft is the other kind.
        target_frame = dv.display_frame(targets, lens)
        st.dataframe(
            dv.styled_frame(target_frame, dv.shade_scales(board, lens), theme),
            width="stretch", hide_index=True,
            column_config=_column_config(target_frame, lens,
                                         click_key=dv.VALUES_NOTE_CLICK_KEY),
            placeholder="", lazy=False,
        )
        # Its own click key, because two widgets cannot share one. Remembered
        # separately too, so opening a note here does not move the Board tab's.
        shown_note = dv.player_note_for(
            board, dv.remember_note_click(st.session_state, targets,
                                          league_key=selection.league_key,
                                          click_key=dv.VALUES_NOTE_CLICK_KEY))
        if shown_note:
            with st.container(border=True):
                st.markdown(shown_note)
        st.caption(
            f"{targets.height} of {board.height:,} players. The rest are either "
            "unpriced by the market or streamed — see the glossary on the Board "
            "tab."
        )
        # The same columns as the board, so the same glossary. Repeated here rather
        # than pointed at, because a column you cannot read is a question you have
        # while looking at *this* table.
        with st.expander("Glossary — Where Every Column Comes From"):
            st.markdown(dv.glossary_markdown(target_frame.columns, lens))

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

# =========================================================================
# Calibration — is a disagreement a player, or is it the model
# =========================================================================
#
# **Faceted rather than pooled, and that is a measurement rather than a taste.** One
# panel per position, each holding one colour, because the repo's categorical palette
# passes the CVD checks for *adjacent* pairs — which is all a line chart or a grouped
# bar needs, since their series sit in a fixed order — and fails for *all* pairs,
# which is what a scatter needs, where any two dots can land next to each other. The
# validator puts `#eda100`↔`#eb6834` at ΔE 13.7 for normal vision (below the floor of
# 15) and `#008300`↔`#eb6834` at 3.2 under protanopia. A pooled scatter of six
# positions would ask the reader to tell those apart. A facet asks nobody to: the
# panel header carries the identity and the colour is redundant with it.
#
# The facets also happen to be the honest form for the data. The gap is strongly
# position-dependent — +27.3 points at QB against +5.9 at RB on the 2026 Winfield
# board — so one pooled cloud is six different calibration regimes drawn on top of
# each other, and the QB offset would read as the model's headline problem while
# hiding every real one.

with calibration_tab:

    st.subheader("Us Against ESPN")
    st.caption(
        "Both projections are scored through **this league's** rules, so they are "
        "directly comparable — and neither is what ESPN's own site shows you. "
        "Where a player sits far off the line is where FantasyPros and the usage "
        "model pulled the blend away from ESPN, which is either something they "
        "know or something we should fix."
    )

    # --- filters ---------------------------------------------------------

    with st.container(border=True):
        top = st.columns([3, 3, 2])
        cal_positions = top[0].multiselect(
            "Positions", all_positions, default=default_positions or all_positions,
            key="cal_positions",
            help="One panel each. Independent of every other tab's filter.",
        )
        cal_teams = top[1].multiselect(
            "NFL Teams", dv.board_teams(board), key="cal_teams",
            help="Empty keeps every team. Narrowing to one is how you ask whether "
                 "a disagreement is really about a *team* — a backfield we split "
                 "differently, or an offence we have more volume in.",
        )
        cal_search = top[2].text_input(
            "Search Players", placeholder="Name contains…", key="cal_search",
            help="Matched literally and case-insensitively.",
        )

        bottom = st.columns([2, 2, 2])
        cal_available = bottom[0].toggle(
            "Available Only", value=not pending, key="cal_available",
            help="Hides players a team already holds."
                 + ("" if not pending else " Off by default: keepers are not "
                    "declared, so nobody is really held yet."),
        )
        cal_priced = bottom[1].toggle(
            "Market-Priced Only", value=False, key="cal_priced",
            help="Keeps only players the market set a real ADP for — 200 of 525 on "
                 "the 2026 Winfield board. **This one changes the answer, not just "
                 "the view.** Across the whole pool we project above ESPN at every "
                 "position; among priced players the sign flips at WR, RB and TE. "
                 "The bias is deep players nobody drafts.",
        )
        cal_view = bottom[2].radio(
            "Read It As", [dv.AGREEMENT_VIEW_POINTS, dv.AGREEMENT_VIEW_DELTA],
            horizontal=True, key="cal_view",
            help="Points plots the two against each other against a 45° line. "
                 "Disagreement plots the gap itself — which is the one that "
                 "separates anything, because the two columns correlate at 0.98 "
                 "and the raw scatter squeezes every residual onto the diagonal.",
        )

    scoped = dv.filter_board(board, cal_positions, only_available=cal_available,
                             search=cal_search, teams=cal_teams)
    if cal_priced and "adp_is_priced" in scoped.columns:
        scoped = scoped.filter(pl.col("adp_is_priced").fill_null(False))

    # Scored over `scoped` rather than over the whole board on purpose — see
    # dv.agreement_frame. The filters above are the analysis, not a viewport.
    agreement = dv.agreement_frame(scoped)

    if agreement.is_empty():
        st.info(
            "Nothing to compare in this selection. A player needs **both** "
            "projections to appear here, and ESPN publishes no stat line at all "
            "for part of the pool — so a narrow filter can empty this while the "
            "Board tab still has rows."
        )
    else:
        # --- the chart ----------------------------------------------------

        shown_positions = agreement["primaryPosition"].unique().to_list()
        # Positions with a hue first, in the palette's own order, then anything
        # else. GOP Degenerates starts cornerbacks, and `POSITION_HUES` stops at
        # eight slots — a ninth generated hue is indistinguishable from an existing
        # one under CVD, so a hueless position gets muted ink instead. Nothing is
        # lost by that here: the panel is one position, so its colour distinguishes
        # it from nothing and the header is already carrying the identity. Dropping
        # them from the chart while the tables below still scored and ranked them
        # was the alternative, and 165 startable players missing from one league's
        # panels is a worse answer than a grey one.
        facet_order = ([p for p in dv.POSITION_HUES if p in shown_positions]
                       + sorted(p for p in shown_positions
                                if p not in dv.POSITION_HUES))
        scale = alt.Scale(
            domain=facet_order,
            range=[colors[dv.POSITION_HUES[p]] if p in dv.POSITION_HUES
                   else ink["muted"] for p in facet_order])
        # No legend: one position a panel, and the panel header names it. Colour is
        # redundant with the header rather than load-bearing, which is what keeps
        # identity off colour alone — see the note above this tab.
        hue = alt.Color("primaryPosition:N", scale=scale, legend=None)

        if cal_view == dv.AGREEMENT_VIEW_DELTA:
            x_field, x_title = "agreement_mean", "Size of the Player"
            y_field, y_title = "points_delta", "Us − ESPN"
        else:
            x_field, x_title = "ESPN_Points", "ESPN's Projection"
            y_field, y_title = "TRUE_Points", "Our Projection"

        # One dataset for every layer, with the marks to highlight flagged on it
        # rather than held in a second frame. Vega-Lite refuses to facet a layered
        # spec whose layers carry different data -- see dv.with_outlier_flag. Three
        # a panel rather than a pooled top ten, which would put eight labels in the
        # WR facet and none in the others. The label slot needs the y field, which
        # is why the view is decided above this rather than below it.
        plotted = dv.with_label_slots(
            dv.with_outlier_flag(agreement, limit=3, per_position=True), y_field)
        base = alt.Chart(plotted.to_pandas())

        tooltip = [alt.Tooltip("player_name:N", title="Player"),
                   alt.Tooltip("ESPN_Points:Q", title="ESPN", format=".1f"),
                   alt.Tooltip("TRUE_Points:Q", title="Us", format=".1f"),
                   alt.Tooltip("points_delta:Q", title="Δ", format="+.1f"),
                   alt.Tooltip("agreement_z:Q", title="σ vs Pos", format="+.2f")]
        if "adp" in agreement.columns:
            tooltip.append(alt.Tooltip("adp:Q", title="ADP", format=".1f"))

        x_axis = alt.Axis(grid=False, labelColor=ink["muted"],
                          titleColor=ink["muted"], domainColor=ink["grid"])
        y_axis = alt.Axis(gridColor=ink["grid"], labelColor=ink["muted"],
                          titleColor=ink["muted"], domain=False)

        if cal_view == dv.AGREEMENT_VIEW_DELTA:
            # One rule a panel, not one a row: `transform_aggregate` with a groupby
            # and no aggregates collapses 525 identical overlapping rules to six.
            references = [
                base.transform_aggregate(groupby=["primaryPosition"])
                .mark_rule(strokeWidth=1, color=ink["muted"], opacity=0.9)
                .encode(y=alt.datum(0)),
                # The position's own mean gap. The distance between this and zero is
                # the systematic half of the disagreement — what the blend does to
                # every player at the position — and what is left scattered around it
                # is the per-player half. Reading which of the two dominates is the
                # question this tab exists for.
                base.transform_aggregate(mu="mean(points_delta)",
                                         groupby=["primaryPosition"])
                .mark_rule(strokeWidth=1.5, strokeDash=[4, 3], opacity=0.9)
                .encode(y=alt.Y("mu:Q"), color=hue),
            ]
        else:
            # Agreement, drawn by binding the same field to both axes. That is also
            # what forces the y scale to cover the x range, so the 45° line is
            # actually at 45° instead of at whatever angle two independently-nice
            # domains happen to produce.
            references = [
                base.mark_line(strokeWidth=1, strokeDash=[4, 3],
                               color=ink["muted"], opacity=0.9)
                .encode(x=alt.X("ESPN_Points:Q"), y=alt.Y("ESPN_Points:Q"))
            ]

        # Headroom on the right for the labels, which are anchored past their mark
        # and are drawn at a data position *inside* the panel -- so chart padding,
        # which pads outside the plotting area, does not save them. The scales are
        # shared, so one padded domain serves every panel.
        x_lo = float(plotted[x_field].min())
        x_hi = float(plotted[x_field].max())
        x_span = (x_hi - x_lo) or 1.0
        position = dict(
            x=alt.X(f"{x_field}:Q", title=x_title, axis=x_axis,
                    scale=alt.Scale(zero=False, nice=False,
                                    domain=[x_lo - x_span * 0.04,
                                            x_hi + x_span * 0.22])),
            y=alt.Y(f"{y_field}:Q", title=y_title, axis=y_axis,
                    scale=alt.Scale(zero=False, nice=True)),
        )

        dots = (base.transform_filter(f"!datum.{dv.AGREEMENT_FLAG}")
                .mark_point(filled=True, size=42, opacity=0.55)
                .encode(color=hue, tooltip=tooltip, **position))
        # The surface-coloured ring is what makes a highlighted dot read as one mark
        # rather than as a darker patch of the cloud it sits in.
        points = (base.transform_filter(f"datum.{dv.AGREEMENT_FLAG}")
                  .mark_point(filled=True, size=120, opacity=1,
                              stroke=ink["surface"], strokeWidth=1.5)
                  .encode(color=hue, tooltip=tooltip, **position))
        # One text layer per slot, each at its own fixed vertical offset, because
        # Vega-Lite has no collision solver for point labels and the flagged marks
        # in a panel are routinely on top of each other -- the three flagged
        # quarterbacks are deep backups within 15 points of each other, and a single
        # layer printed all three names in the same 60 pixels.
        #
        # The slot is the mark's *vertical order* in the panel, not its rank by
        # disagreement, so the lowest mark's name is pushed further down and the
        # highest one's further up. See dv.with_label_slots: by |z| rank instead,
        # the offsets ran against the marks' own spread and the three tight ends
        # still landed on one line.
        #
        # Text wears a text token, not the series colour: the dot beside it already
        # carries the identity, and a coloured name is a name you have to read
        # through the palette.
        names = [
            base.transform_filter(f"datum.{dv.AGREEMENT_SLOT} == {slot}")
            .mark_text(align="left", dx=10, dy=offset, fontSize=10, fontWeight=600,
                       color=ink["text"], limit=100)
            .encode(text="player_name:N", **position)
            for slot, offset in ((1, 13), (2, 0), (3, -13))
        ]

        st.altair_chart(
            alt.layer(*references, dots, points, *names)
            .properties(width=350, height=250)
            .facet(facet=alt.Facet("primaryPosition:N", title=None, sort=facet_order,
                                   header=alt.Header(labelColor=ink["text"],
                                                     labelFontSize=13,
                                                     labelFontWeight=600,
                                                     labelAnchor="start")),
                   columns=2)
            # Shared, deliberately. Independent scales would draw the D/ST panel at
            # the same apparent spread as the QB one, and "quarterbacks disagree by
            # 29 points and defences by 3" is the finding rather than the nuisance.
            .resolve_scale(x="shared", y="shared")
            .configure_view(strokeWidth=0),
            width="content",
        )

        scored = agreement.filter(pl.col("agreement_z").is_not_null())
        unscored_positions = [row[0] for row in
                              dv.agreement_summary(agreement)
                              .filter(~pl.col("scored"))
                              .select("primaryPosition").iter_rows()]
        st.caption(
            ("The dashed rule is each position's **own** average gap; the solid one "
             "is zero. A cloud sitting off zero as a body is the blend applying an "
             "offset to the whole position — a model question. A cloud centred on "
             "its own mean with long tails is a player question."
             if cal_view == dv.AGREEMENT_VIEW_DELTA else
             "The dashed line is where the two agree. Above it we project more than "
             "ESPN, below it less. Both axes are shared across panels, so the "
             "panels are comparable to each other and not just to themselves.")
            + f" Labelled: the three biggest disagreements in each panel. "
              f"**{scored.height:,} of {agreement.height:,}** shown players carry a σ"
            + (f"; {', '.join(unscored_positions)} "
               f"{'does' if len(unscored_positions) == 1 else 'do'} not."
               if unscored_positions else ".")
        )

    # --- the outliers, as a table ----------------------------------------

    if not agreement.is_empty():
        st.subheader("The Biggest Disagreements")
        depth = st.slider("How Many to Show", min_value=5, max_value=60, value=15,
                          step=5, key="cal_depth")
        outliers = dv.agreement_outliers(agreement, limit=depth)

        if outliers.is_empty():
            st.info(
                "No player in this selection carries a σ. Every position shown "
                "either has fewer than "
                f"{dv.AGREEMENT_MIN_PLAYERS} players left after the filters, or "
                "too little disagreement to measure a spread — which is the true "
                "answer for kickers, whose projection has no second source."
            )
        else:
            st.dataframe(
                dv.agreement_table(outliers), width="stretch", hide_index=True,
                column_config=AGREEMENT_CONFIG, placeholder="", lazy=False,
            )
            st.caption(
                "Sorted by |σ|, so both tails are here — the players we are far "
                "*below* ESPN on sit next to the ones we are far above. An ADP "
                "near 170 means the market has no opinion on him at all, and a "
                "disagreement about a player nobody drafts is not one to spend a "
                "model change on."
            )

        with st.expander("How Each Position Sits Against ESPN"):
            summary = dv.agreement_summary(agreement)
            st.dataframe(
                summary.select(
                    pl.col("primaryPosition").alias("Pos"),
                    pl.col("players").alias("Players"),
                    pl.col("mean_delta").alias("Mean Δ"),
                    pl.col("sd_delta").alias("Spread"),
                    pl.col("share_above").alias("We're Higher"),
                    pl.when(pl.col("scored")).then(pl.lit("yes"))
                      .otherwise(pl.lit("no")).alias("Scored"),
                ),
                width="stretch", hide_index=True,
                column_config=AGREEMENT_SUMMARY_CONFIG, lazy=False,
            )
            st.markdown("""
#### What this comparison is not

- **These are not two independent forecasts.** ESPN is one of the three equal thirds
  inside `Us`, alongside FantasyPros and the usage model. The gap is damped by
  construction and reads as *how far the blend moved off ESPN* — a player two
  standard deviations out is one the other two sources dragged, which is the useful
  reading, but it is not two forecasters disagreeing.
- **σ is measured over what you are currently showing.** Change a filter and every
  score is recomputed against the new population, because "an outlier among the
  players the market prices" and "an outlier in the whole pool" are different
  questions and the second one is usually not the one being asked. It is why the
  *Market-Priced Only* toggle moves the WR mean from +8.8 to −3.4.
- **A big gap is not evidence either way on its own.** It says the sources disagree,
  not who is right. Nothing here is scored against outcomes — that needs finished
  seasons projected in advance and then graded, which the store does not hold.
- **Neither column knows about injuries.** Both project a healthy 17 games. The
  usage model is the only source that prices availability, and its own season number
  lives in `Points | USG` on the Board tab, deliberately kept out of this comparison
  because it measures a different quantity.
""")
