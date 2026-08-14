"""Derivations behind the draft board page: filters, roster needs, chart data.

Kept out of the page script so it is testable without a Streamlit runtime -- the
page is layout, this is the logic. Everything is Polars over the stored
``board.parquet``; nothing here reads a file or talks to ESPN.

The flex-slot definitions come from ``Scripts.draft.board`` rather than being
restated. Two copies of a positional-eligibility map is exactly the shape that has
already cost this repo twice (12 projection functions, then
``build_league_frame``), and a board that disagreed with its own page about what an
``OP`` slot accepts would be very hard to see.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

from typing import Dict, List, Mapping, Optional, Sequence

import polars as pl

from Scripts.draft.board import FLEX_SLOTS, NON_STARTING_SLOTS

#: Position to categorical slot, fixed. Colour follows the position, not its rank
#: in the current filter -- deselecting kickers must not repaint running backs.
#:
#: Eight positions get a hue, which is the palette's limit; a ninth would have to
#: be a generated colour, and generated hues are what break colour-blind safety.
#: The IDP league has more than eight positions in its pool, so the chart caps the
#: series it draws and says so rather than inventing colours.
POSITION_HUES: Dict[str, int] = {
    "QB": 1, "RB": 2, "WR": 3, "TE": 4, "K": 5, "D/ST": 6, "LB": 7, "DL": 8,
}

#: The validated categorical palette, keyed by slot, per theme.
SERIES_COLORS: Dict[str, Dict[int, str]] = {
    "light": {1: "#2a78d6", 2: "#eb6834", 3: "#1baf7a", 4: "#eda100",
              5: "#e87ba4", 6: "#008300", 7: "#4a3aa7", 8: "#e34948"},
    "dark": {1: "#3987e5", 2: "#d95926", 3: "#199e70", 4: "#c98500",
             5: "#d55181", 6: "#008300", 7: "#9085e9", 8: "#e66767"},
}

#: Chart ink, per theme: (gridline, axis/label muted, primary text, surface).
#: ``surface`` is the page background a mark sits on, used as the stroke on
#: overlapping marks so two dots at the same coordinate read as two.
CHART_INK: Dict[str, Dict[str, str]] = {
    "light": {"grid": "#e1e0d9", "muted": "#898781", "text": "#0b0b0b",
              "surface": "#ffffff"},
    "dark": {"grid": "#2c2c2a", "muted": "#898781", "text": "#ffffff",
             "surface": "#0e1117"},
}

# Tier is deliberately *not* colour-encoded anywhere. It reads as an ordinal blue
# ramp, and eight tiers cannot be stepped down one hue with visible gaps between
# them -- squeezing eight steps into the range whose light end still clears the
# surface leaves adjacent lightness differences of 0.047, which the validator fails
# and the eye cannot separate either. So the tier runway puts tier on an axis and
# colours by position instead, which reuses the categorical palette that does pass
# and keeps one meaning per colour across both charts on the page.

#: How deep the scarcity curve runs past replacement level. Two positions' curves
#: only compare where both still have startable players; far past that every
#: position is flat near zero and the cliff -- the thing the chart exists to show
#: -- is squeezed into the left edge.
SCARCITY_DEPTH = 1.6

#: What ``model_evidence`` says when the model priced a player and flagged nothing.
#: A visible mark rather than an empty cell, because a blank in this column would be
#: indistinguishable from the model having no opinion at all -- which is the whole
#: distinction the column exists to draw.
EVIDENCE_CLEAR = "—"

#: What it says when the model produced no projection. Two different reasons, kept
#: apart on purpose: ``availability`` is the model declining a player it could see
#: (its expected-games estimate was too low to price), ``injury`` is the report
#: withdrawing one it had already priced. Both read as an empty ``USG``, and telling
#: them apart is the difference between "no data" and "actively withheld".
EVIDENCE_WITHDRAWN_AVAILABILITY = "withdrawn (availability)"
EVIDENCE_WITHDRAWN_INJURY = "withdrawn (injury)"

#: And when the model does not cover the position at all -- K and D/ST, which it has
#: never modelled, plus anyone it had no usage history for.
EVIDENCE_NOT_MODELLED = "not modelled"

#: Column order for the board table, and the label each gets.
#:
#: The model block sits after the market block and before the status columns, so the
#: table reads left to right as: who they are, what we think, what the room thinks,
#: what the model thinks, what is wrong with them.
#:
#: Labels are Title Case, which is the house style for every header on every page.
#: They are also the keys of the page's ``column_config``, so the two move together
#: or a format silently stops applying -- Streamlit ignores config for a column the
#: frame does not carry rather than raising.
#:
#: ``$`` reads :func:`at_budget`'s output rather than the stored
#: ``auction_value_filled``, so the column shows this league's budget instead of the
#: $200 one ESPN publishes against.
DISPLAY_COLUMNS: List[tuple] = [
    ("player_name", "Player"),
    ("primaryPosition", "Pos"),
    ("pro_team", "NFL"),
    ("bye_week", "Bye"),
    ("tier", "Tier"),
    ("TRUE_Points", "Proj"),
    ("floor", "Floor"),
    ("ceiling", "Ceil"),
    ("vor", "VOR"),
    ("vor_rank", "VOR Rk"),
    ("pos_rank", "Pos Rk"),
    ("adp", "ADP"),
    ("value", "Value"),
    ("auction_dollars", "$"),
    ("USG_Points", "USG"),
    ("USG_PosRankDelta", "Δ Rk"),
    ("usg_expected_games", "Exp G"),
    ("usg_evidence_label", "Model Evidence"),
    ("injury_status", "Injury"),
    ("team_owner", "Owner"),
]


# --- the auction budget ---------------------------------------------------

#: The budget ESPN's published auction values are denominated in.
#:
#: ESPN's auction default is $200, and the 2026 pool agrees: the 338 players it has
#: actually priced sum to $1,871 of the $2,000 a ten-team $200 auction puts on the
#: table, with the remaining ~$129 spread across the deep bench it prices at pennies.
#: The board stores those dollars as published, so a league playing for $250 was
#: reading a column denominated in somebody else's money.
BASE_AUCTION_BUDGET = 200.0

#: What the budget input starts at.
DEFAULT_AUCTION_BUDGET = 250

#: ``st.session_state`` key the budget input owns. Named here rather than in the
#: page so the page can read the current budget *before* the widget that sets it is
#: drawn -- Streamlit reruns top to bottom with session state already updated, so
#: reading the key is what makes the ``$`` column right on the same run in which it
#: changed, and on tabs that render before the input.
AUCTION_BUDGET_KEY = "auction_budget"


def at_budget(board: pl.DataFrame, budget: float,
              base: float = BASE_AUCTION_BUDGET) -> pl.DataFrame:
    """Re-price ESPN's auction values into the budget this league actually plays for.

    The stored value is a market average in ESPN's own $200 auction. Held as a
    **share of a budget** it is portable, so this carries both: ``auction_share``
    is the fraction of one team's money the market puts on the player, and
    ``auction_dollars`` is that share at ``budget``. The table shows the dollars;
    the share is what makes them meaningful.

    The rescale is straight proportion, which is what makes it honest to a point
    and no further. A real auction's minimum bid does not scale -- the last roster
    spots cost $1 whatever the budget is -- so raising the budget adds slightly
    more to the top of the board than a flat multiple suggests. The distortion is
    small next to the disagreement between any two sources' valuations, and
    correcting it would need a roster size this function is not given.

    Args:
        board: A stored draft board.
        budget: This league's per-team auction budget.
        base: Budget the stored values are denominated in. Overridable so the
            assumption is visible rather than buried in a literal.

    Returns:
        pl.DataFrame: The board with ``auction_share`` and ``auction_dollars``
        added, or unchanged when it carries no auction column -- the ``$`` column
        then drops out of :func:`display_frame` exactly as any other absent column
        does.
    """
    if "auction_value_filled" not in board.columns or not base:
        return board
    share = pl.col("auction_value_filled") / float(base)
    return board.with_columns(
        share.alias("auction_share"),
        (share * float(budget)).alias("auction_dollars"),
    )


def available_only(board: pl.DataFrame) -> pl.DataFrame:
    """Players nobody has drafted yet.

    ``on_team_id`` is 0 for a free agent and the fantasy team's id once someone
    holds them, so it works pre-draft (where everyone is free) and mid-draft
    alike.

    Args:
        board: A stored draft board.

    Returns:
        pl.DataFrame: The undrafted rows, or ``board`` unchanged when the column
        is absent.
    """
    if "on_team_id" not in board.columns:
        return board
    return board.filter(pl.col("on_team_id").fill_null(0) == 0)


def roster_needs(starting_slots: Dict[str, int],
                 roster_positions: Sequence[str]) -> Dict[str, int]:
    """Starting slots still unfilled, after assigning the players already held.

    Dedicated slots are filled before flex ones. That is a convention rather than
    an optimisation -- an optimal assignment would need the projections -- but it
    is the one a drafter reasons with: a third running back fills the flex, it does
    not displace a starter.

    Args:
        starting_slots: ``meta.json``'s ``starting_slots``, e.g.
            ``{"QB": 1, "RB": 2, "RB/WR/TE": 1}``.
        roster_positions: ``primaryPosition`` of every player already rostered.

    Returns:
        dict: Slot name to openings remaining, omitting filled slots.
    """
    remaining = {slot: count for slot, count in starting_slots.items()
                 if slot not in NON_STARTING_SLOTS and count > 0}

    for position in roster_positions:
        if remaining.get(position, 0) > 0:
            remaining[position] -= 1
            continue
        for slot, eligible in FLEX_SLOTS.items():
            if position in eligible and remaining.get(slot, 0) > 0:
                remaining[slot] -= 1
                break

    return {slot: count for slot, count in remaining.items() if count > 0}


def positions_needed(starting_slots: Dict[str, int],
                     roster_positions: Sequence[str]) -> List[str]:
    """Positions that would fill one of the still-open starting slots.

    Args:
        starting_slots: ``meta.json``'s ``starting_slots``.
        roster_positions: ``primaryPosition`` of every player already rostered.

    Returns:
        list: Sorted positions. Empty when every starting slot is filled.
    """
    needs = roster_needs(starting_slots, roster_positions)
    positions = set()
    for slot in needs:
        positions.update(FLEX_SLOTS.get(slot, [slot]))
    return sorted(positions)


def board_positions(board: pl.DataFrame) -> List[str]:
    """Positions present in this league's pool, most-drafted first.

    Ordered by :data:`POSITION_HUES` so the familiar ones lead and the IDP
    positions follow, rather than alphabetically with cornerbacks first.

    Args:
        board: A stored draft board.

    Returns:
        list: Position names.
    """
    present = [p for p in board["primaryPosition"].drop_nulls().unique().to_list()]
    return sorted(present, key=lambda p: (POSITION_HUES.get(p, 99), p))


def board_teams(board: pl.DataFrame) -> List[str]:
    """NFL teams present in this league's pool, alphabetically.

    Args:
        board: A stored draft board.

    Returns:
        list: Team abbreviations. Empty when the board carries no ``pro_team``.
    """
    if "pro_team" not in board.columns:
        return []
    return sorted(board["pro_team"].drop_nulls().unique().to_list())


def board_byes(board: pl.DataFrame) -> List[int]:
    """Bye weeks present in this league's pool, in week order.

    Args:
        board: A stored draft board.

    Returns:
        list: Week numbers as ints. Empty when the board carries no ``bye_week``,
        which is what a board built before byes were attached looks like.
    """
    if "bye_week" not in board.columns:
        return []
    weeks = board["bye_week"].drop_nulls().unique().to_list()
    return sorted(int(week) for week in weeks)


def filter_board(
    board: pl.DataFrame,
    positions: Optional[Sequence[str]] = None,
    *,
    only_available: bool = True,
    hide_unstartable: bool = True,
    hide_unprojected: bool = True,
    max_tier: Optional[int] = None,
    search: str = "",
    teams: Optional[Sequence[str]] = None,
    byes: Optional[Sequence[int]] = None,
) -> pl.DataFrame:
    """Apply the page's filters.

    Every filter is an **include** list: an empty selection keeps everything, and a
    non-empty one keeps only what it names. That is the one rule that makes four
    controls composable without a legend explaining each -- and it is why ``byes``
    drops players whose bye is unknown, which "keep only weeks 5 and 10" has to
    mean if it is to mean anything.

    Args:
        board: A stored draft board.
        positions: Positions to keep. None or empty keeps all.
        only_available: Drop players somebody already holds.
        hide_unstartable: Drop positions this league has no starting slot for --
            the 32 team defences on the board of the league with no D/ST slot.
        hide_unprojected: Drop players no source has a line for. Half the pool,
            and their projection is a literal 0.0 rather than a null, so without
            this they rank as the worst players in the league rather than as
            unknowns.
        max_tier: Keep tiers at or above this one. None keeps all.
        search: Case-insensitive substring of the player name.
        teams: NFL teams to keep. None or empty keeps all.
        byes: Bye weeks to keep. None or empty keeps all; a non-empty selection
            also drops players with no recorded bye.

    Returns:
        pl.DataFrame: The filtered board, still in its stored ``vor`` order.
    """
    out = board
    if only_available:
        out = available_only(out)
    if hide_unstartable and "startable" in out.columns:
        out = out.filter(pl.col("startable").fill_null(True))
    if hide_unprojected and "projection_missing" in out.columns:
        out = out.filter(~pl.col("projection_missing").fill_null(False))
    if positions:
        out = out.filter(pl.col("primaryPosition").is_in(list(positions)))
    if teams and "pro_team" in out.columns:
        out = out.filter(pl.col("pro_team").is_in(list(teams)))
    if byes and "bye_week" in out.columns:
        out = out.filter(pl.col("bye_week").is_in([int(week) for week in byes]))
    if max_tier is not None and "tier" in out.columns:
        out = out.filter(pl.col("tier").is_null() | (pl.col("tier") <= max_tier))
    if search:
        # Matched literally, not as a regex. The names on a board are full of
        # characters a regex reads as syntax -- "T.J. Hockenson" matched any three
        # characters between the dots, "Amon-Ra St. Brown" the same, and a name
        # typed with an unclosed bracket raised out of the page instead of finding
        # nothing. Case folded on both sides rather than with an inline `(?i)`,
        # which a literal match does not honour.
        out = out.filter(pl.col("player_name").str.to_lowercase()
                         .str.contains(search.strip().lower(), literal=True))
    return out


def scarcity_curve(board: pl.DataFrame, positions: Sequence[str],
                   depth: float = SCARCITY_DEPTH) -> pl.DataFrame:
    """Projected points against positional rank -- the positional cliff.

    Runs to :data:`SCARCITY_DEPTH` times each position's replacement rank, so the
    visible range is the part of each curve a drafter can act on. Where a position
    has no replacement rank (this league does not start it) the whole curve is
    kept.

    Args:
        board: A stored draft board.
        positions: Positions to include.
        depth: Multiple of replacement rank to run to.

    Returns:
        pl.DataFrame: ``primaryPosition``, ``pos_rank``, ``TRUE_Points``,
        ``replacement_rank``, one row per player, projected players only.
    """
    curve = board.filter(
        pl.col("primaryPosition").is_in(list(positions))
        & pl.col("pos_rank").is_not_null()
        & pl.col("TRUE_Points").is_not_null()
    )
    if "projection_missing" in curve.columns:
        curve = curve.filter(~pl.col("projection_missing").fill_null(False))

    if "replacement_rank" in curve.columns:
        curve = curve.filter(
            pl.col("replacement_rank").is_null()
            | (pl.col("pos_rank") <= pl.col("replacement_rank") * depth)
        )

    keep = [c for c in ("primaryPosition", "player_name", "pos_rank",
                        "TRUE_Points", "replacement_rank", "tier")
            if c in curve.columns]
    return curve.select(keep).sort(["primaryPosition", "pos_rank"])


def tier_runway(board: pl.DataFrame, positions: Sequence[str]) -> pl.DataFrame:
    """How many players are left in each tier, per position.

    The question a board is actually read to answer: not "is this player one spot
    better than that one" but "how many of these are left before the drop". Counts
    available players only, so it empties as the draft runs.

    Args:
        board: A stored draft board.
        positions: Positions to include.

    Returns:
        pl.DataFrame: ``primaryPosition``, ``tier``, ``remaining``, ``best_points``,
        sorted by position then tier.
    """
    pool = filter_board(board, positions, only_available=True,
                        hide_unstartable=True, hide_unprojected=True)
    if pool.is_empty() or "tier" not in pool.columns:
        return pl.DataFrame(schema={"primaryPosition": pl.String, "tier": pl.Float64,
                                    "remaining": pl.UInt32, "best_points": pl.Float64})

    return (
        pool.filter(pl.col("tier").is_not_null())
        .group_by(["primaryPosition", "tier"])
        .agg(pl.len().alias("remaining"),
             pl.col("TRUE_Points").max().alias("best_points"))
        .sort(["primaryPosition", "tier"])
    )


def value_targets(board: pl.DataFrame, limit: int = 12) -> pl.DataFrame:
    """The players the room is letting fall furthest past our valuation.

    ``value`` is NaN for the 84% of the pool the market has not priced and for the
    streamed positions, where a season-total VOR does not describe how the position
    is used. Both are excluded here rather than sorted to the bottom, because a
    "best values" list is worthless if most of its rows are nulls.

    Args:
        board: A stored draft board.
        limit: Rows to return.

    Returns:
        pl.DataFrame: The top ``limit`` by ``value``, available players only.
    """
    if "value" not in board.columns:
        return board.head(0)
    return (
        filter_board(board, None, only_available=True, hide_unstartable=True,
                     hide_unprojected=True)
        .filter(pl.col("value").is_not_null())
        .sort("value", descending=True)
        .head(limit)
    )


#: Column order for the owner-tendency detail table, and the label each gets.
TENDENCY_COLUMNS: List[tuple] = [
    ("owner_display", "Manager"),
    ("seasons", "Drafts"),
    ("headline", "Reads As"),
    ("earliest_position", "Earliest"),
    ("earliest_delta", "Rds Early"),
    ("latest_position", "Latest"),
    ("latest_delta", "Rds Late"),
    ("favourite_team", "NFL Lean"),
    ("favourite_team_excess", "Extra Picks"),
    ("favourite_player", "His Guy"),
    ("favourite_player_times", "Times"),
    ("rookie_rate", "Rookie %"),
    ("auto_rate", "Auto %"),
    ("top3_share", "Top-3 $"),
]


def timing_matrix(picks: pl.DataFrame, positions: Sequence[str],
                  min_seasons: int = 2) -> pl.DataFrame:
    """Rounds each manager takes each position, against the same room.

    Computed here rather than stored because it is a reshape of ``draft.parquet``
    -- 960 rows for the deepest league -- and because reusing
    ``Scripts.draft.tendencies.positional_timing`` is the only way the chart and
    the descriptions cannot disagree about what "two rounds early" means.

    Args:
        picks: A stored pick history.
        positions: Positions to keep, in the page's current filter.
        min_seasons: Managers with fewer drafts are dropped; one draft is not a
            tendency and plotting it as one is the whole failure mode here.

    Returns:
        pl.DataFrame: ``owner``, ``position``, ``own_round``, ``room_round``,
        ``delta``, ``seasons``. Empty for an auction league, which has no rounds
        to be early in.
    """
    from Scripts.draft.tendencies import positional_timing

    timing = positional_timing(picks)
    if timing.is_empty():
        return timing
    keep = [p for p in positions if p in TIMED_CHART_POSITIONS]
    return timing.filter(pl.col("position").is_in(keep)
                         & (pl.col("seasons") >= min_seasons))


#: Positions the timing chart draws. The same six the tendencies module times,
#: intersected with the palette -- every one has a fixed hue already.
TIMED_CHART_POSITIONS = ("QB", "RB", "WR", "TE", "K", "D/ST")


def owner_label(owner: str) -> str:
    """A manager's name as it should be shown.

    Re-exported from the tendencies module rather than reimplemented, so the
    chart's axis and the cards above it cannot spell a manager two ways.

    Args:
        owner: The name as ESPN stores it.

    Returns:
        str: e.g. ``"Hank Winfield"``.
    """
    from Scripts.draft.tendencies import display_name

    return display_name(owner)


#: What a manager's points are split into: the players they drafted themselves,
#: against everyone else they fielded — waiver claims, free agents, trade returns.
#: Labels rather than booleans because they are read off a chart legend.
SOURCE_DRAFTED = "Drafted"
SOURCE_ADDED = "Added in Season"

#: ``team_owner`` in a lineups frame is this for a player nobody rostered that
#: week. It is not a manager and must not become a bar.
FREE_AGENT_OWNER = "Free Agent"


def _franchise_key(column: str) -> pl.Expr:
    """A team name reduced to something two artifacts can be joined on.

    **The franchise is the join key, not the manager,** and that was arrived at by
    measurement rather than preference. ESPN does not spell a person consistently
    across its endpoints, so joining on the manager silently loses whole teams and
    reports them as having drafted nothing at all. Every row below is real, in
    2025, in a different league:

    | ``lineups.parquet`` | ``draft.parquet`` | Cause |
    |---|---|---|
    | ``Hank Winfield`` | ``hank Winfield`` | ``str.title`` in ``set_owner_names`` |
    | ``Zach Imel`` | ``Zachary Imel`` | a nickname |
    | ``Logan Tola`` | ``Matt Logan Tola`` | an extra given name |
    | ``Alex Holton`` | ``Michael Beal`` | the team changed hands |

    Case-folding fixes only the first. The last cannot be fixed by any string rule
    and is what settles the design: the question is what a *roster* got from draft
    day, and a roster survives a handover even though the name against it does
    not. Each of these cost a manager their entire draft — 2,592.95 points for
    Zach Imel, all of it reported as waiver pickups.

    ``team_name`` was checked before being trusted, across all nine leagues for
    2025: no team renamed itself mid-season, no two teams shared a name, and all
    108 matched a drafting team. The manager is still who gets displayed.

    Args:
        column: Name of the column holding the team name.

    Returns:
        pl.Expr: The name trimmed and lowercased.
    """
    return pl.col(column).str.strip_chars().str.to_lowercase()


def drafted_versus_added(lineups: pl.DataFrame, picks: pl.DataFrame,
                         starters_only: bool = True) -> pl.DataFrame:
    """How much of each manager's season came from their own draft.

    A player counts as *drafted* for a team only if that team drafted them. The
    same player is drafted for whoever took them and added for whoever picked them
    up later, which is the honest reading of a mid-season move and falls out of
    joining on the pair rather than on the player. Teams are matched on
    ``team_name`` — see :func:`_franchise_key` for why the manager's name does not
    work.

    Starters only by default. Bench points are real but never counted for anyone,
    so including them would answer a different question — how much talent did you
    hold — than the one asked, which is how much of what you *scored* came from
    draft day.

    A team whose name is absent from ``picks`` gets **null** rather than zero for
    ``drafted`` and ``added``. Zero would be a claim that they drafted nobody who
    scored; null is the truth, which is that this season's draft cannot be matched
    to them. The distinction is the one that has already cost this repo three
    separate bugs.

    Args:
        lineups: One season's weekly rows, with ``team_name``, ``team_owner``,
            ``player_id``, ``slotPosition`` and ``points``.
        picks: Draft picks for **that same season**, with ``team_name`` and
            ``player_id``. Filter by season before calling; this cannot tell one
            season's picks from another's and would count a player drafted in any
            season as drafted in this one.
        starters_only: Count only points scored from a starting slot.

    Returns:
        pl.DataFrame: One row per team — ``owner``, ``manager`` (rendered),
        ``drafted``, ``added``, ``total`` and ``share_drafted`` (0–100), ordered
        by total points descending. ``drafted``/``added``/``share_drafted`` are
        null where the draft could not be matched. Empty if either input is, or if
        ``picks`` is empty — with no draft every point would classify as added,
        and a league that looks like it built itself off waivers is exactly the
        kind of confident wrong answer this codebase keeps having to unlearn.
    """
    needed = {"team_name", "team_owner", "player_id", "points", "slotPosition"}
    empty = pl.DataFrame(schema={"owner": pl.Utf8, "manager": pl.Utf8,
                                 "drafted": pl.Float64, "added": pl.Float64,
                                 "total": pl.Float64,
                                 "share_drafted": pl.Float64,
                                 "moves": pl.UInt32,
                                 "points_per_move": pl.Float64})
    if (lineups.is_empty() or picks.is_empty()
            or not needed <= set(lineups.columns)
            or "team_name" not in picks.columns):
        return empty

    drafted = (picks.select(_franchise_key("team_name").alias("_team"), "player_id")
               .unique()
               .with_columns(pl.lit(True).alias("_drafted")))
    # ESPN's owner GUID, carried through so seasons can be grouped by *person*.
    # Necessary because neither of the other two identities survives a decade:
    # team names change (Jack's "Cococnut Crushers" became "Coconut Crushers" in
    # 2023, Tommy renamed his four times) and so do owner names (ESPN recorded
    # Jack as "J W" in one season, which split him into a separate manager with
    # one season to his name). The GUID has been the same for all six managers
    # every year since 2019.
    identities = (picks.select(_franchise_key("team_name").alias("_team"),
                               pl.col("owner_id").alias("owner_id"))
                  .unique(subset=["_team"])
                  if "owner_id" in picks.columns else None)
    drafting_teams = drafted.select("_team").unique()

    rostered = (lineups
                .filter(pl.col("team_owner").is_not_null()
                        & (pl.col("team_owner") != FREE_AGENT_OWNER))
                .with_columns(_franchise_key("team_name").alias("_team"))
                .join(drafted, on=["_team", "player_id"], how="left")
                .with_columns(pl.col("_drafted").fill_null(False))
                .join(drafting_teams.with_columns(pl.lit(True).alias("_known")),
                      on="_team", how="left")
                .with_columns(pl.col("_known").fill_null(False)))

    # Counted before the starter filter, and deliberately. A move is a player you
    # brought in; whether you ever started them is the *result* of the move, not
    # part of its definition. Counting only the ones who started would flatter a
    # manager who claimed twenty players and got three right, which is exactly
    # what points-per-move is supposed to expose.
    moves = (rostered.filter(pl.col("_drafted").not_())
             .group_by("_team")
             .agg(pl.col("player_id").n_unique().alias("moves")))

    rows = (rostered.filter(
                pl.col("slotPosition").is_in(list(NON_STARTING_SLOTS)).not_())
            if starters_only else rostered)

    if rows.is_empty():
        return empty

    summary = (rows.group_by("_team")
               .agg(pl.col("team_owner").mode().first().alias("owner"),
                    pl.col("_known").first().alias("_known"),
                    pl.col("points").filter(pl.col("_drafted")).sum().alias("_drafted_pts"),
                    pl.col("points").filter(pl.col("_drafted").not_()).sum().alias("_added_pts"),
                    pl.col("points").sum().alias("total"))
               .join(moves, on="_team", how="left")
               .with_columns(pl.col("moves").fill_null(0))
               .with_columns(
                   pl.when("_known").then(pl.col("_drafted_pts")).alias("drafted"),
                   pl.when("_known").then(pl.col("_added_pts")).alias("added"))
               .with_columns(
                   pl.when(pl.col("total") > 0)
                   .then(100 * pl.col("drafted") / pl.col("total"))
                   .otherwise(None).alias("share_drafted"),
                   # Null rather than zero when nobody was added: a manager who
                   # never touched the wire has no points-per-move, and 0.0 would
                   # rank them alongside one whose every pickup failed.
                   pl.when(pl.col("moves") > 0)
                   .then(pl.col("added") / pl.col("moves"))
                   .otherwise(None).alias("points_per_move"),
                   pl.col("owner").map_elements(owner_label, return_dtype=pl.Utf8)
                   .alias("manager")))

    if identities is None:
        summary = summary.with_columns(pl.lit(None, dtype=pl.Utf8).alias("owner_id"))
    else:
        summary = summary.join(identities, on="_team", how="left")

    return (summary.select("owner", "manager", "owner_id", "drafted", "added",
                           "total", "share_drafted", "moves", "points_per_move")
            .sort("total", descending=True))


def acquisition_history(picks: pl.DataFrame,
                        results_by_season: Mapping[int, pl.DataFrame]
                        ) -> pl.DataFrame:
    """Run :func:`drafted_versus_added` over several seasons and stack the answers.

    Each season is matched against *its own* draft. That is the whole reason this
    is a loop rather than one join over a concatenated frame: team names and owner
    names both drift between seasons, so a player drafted by "Coconut Crushers" in
    2023 must not be credited to "Cococnut Crushers" in 2022.

    Args:
        picks: Draft picks across every season, carrying a ``season`` column —
            i.e. ``draft.parquet`` as stored.
        results_by_season: Season year to that season's ``results`` frame.

    Returns:
        pl.DataFrame: :func:`drafted_versus_added` output for each season with a
        ``season`` column added, concatenated. Seasons that produce nothing are
        skipped. Empty when none of them produce anything.
    """
    frames = []
    for season, results in sorted(results_by_season.items()):
        one = drafted_versus_added(results,
                                   picks.filter(pl.col("season") == season))
        if not one.is_empty():
            frames.append(one.with_columns(
                pl.lit(season, dtype=pl.Int64).alias("season")))
    if not frames:
        return drafted_versus_added(pl.DataFrame(), pl.DataFrame()).with_columns(
            pl.lit(None, dtype=pl.Int64).alias("season"))
    return pl.concat(frames)


def acquisition_averages(history: pl.DataFrame) -> pl.DataFrame:
    """Average a manager's acquisition split across every season they played.

    The per-season numbers answer "what happened that year"; this answers whether
    it is a habit. A manager who leans on the wire once had a bad draft; one who
    does it every year is telling you something about how they play.

    Averaged per season rather than pooled, so a manager is not weighted by how
    many seasons they happened to be in the league. ``share_drafted`` is the mean
    of the seasonal shares for the same reason — a pooled ratio would let their
    highest-scoring season dominate the description of their habits.

    Args:
        history: :func:`drafted_versus_added` output for several seasons,
            concatenated, carrying a ``season`` column.

    Returns:
        pl.DataFrame: One row per manager — ``manager``, ``seasons``, and the
        per-season means of ``drafted``, ``added``, ``total``, ``share_drafted``,
        ``moves`` and ``points_per_move`` — ordered by mean share drafted
        descending, so the managers who lived off their own drafts come first.
        Empty if the input is.
    """
    if history.is_empty() or "season" not in history.columns:
        return pl.DataFrame(schema={"manager": pl.Utf8, "seasons": pl.UInt32,
                                    "drafted": pl.Float64, "added": pl.Float64,
                                    "total": pl.Float64,
                                    "share_drafted": pl.Float64,
                                    "moves": pl.Float64,
                                    "points_per_move": pl.Float64})

    # Group on the GUID where there is one. Grouping on the displayed name splits
    # a manager in two the moment ESPN spells them differently for one season --
    # Jack Winfield came out as six seasons plus a separate "J W" with one.
    identity = ("owner_id" if "owner_id" in history.columns
                and history["owner_id"].null_count() == 0 else "manager")

    return (history.sort("season")
            .group_by(identity)
            .agg(pl.col("season").n_unique().alias("seasons"),
                 # The name they go by *now*, not an arbitrary one from 2019.
                 pl.col("manager").last().alias("manager"),
                 pl.col("drafted").mean(),
                 pl.col("added").mean(),
                 pl.col("total").mean(),
                 pl.col("share_drafted").mean(),
                 pl.col("moves").mean(),
                 pl.col("points_per_move").mean())
            .select("manager", "seasons", "drafted", "added", "total",
                    "share_drafted", "moves", "points_per_move")
            .sort("share_drafted", descending=True, nulls_last=True))


#: Column order and labels for the acquisition tables, both the multi-season
#: averages and one season on its own.
ACQUISITION_COLUMNS: List[tuple] = [
    ("manager", "Manager"),
    ("seasons", "Seasons"),
    ("total", "Points"),
    ("drafted", "From the Draft"),
    ("added", "From the Wire"),
    ("share_drafted", "% Drafted"),
    ("moves", "Moves"),
    ("points_per_move", "Pts / Move"),
]


def acquisition_frame(summary: pl.DataFrame) -> pl.DataFrame:
    """Select and rename the columns the acquisition tables show.

    Args:
        summary: :func:`acquisition_averages` or :func:`drafted_versus_added`
            output. The latter carries no ``seasons`` column, which is skipped
            rather than faked.

    Returns:
        pl.DataFrame: Renamed display columns, in :data:`ACQUISITION_COLUMNS`
        order.
    """
    present = [(source, label) for source, label in ACQUISITION_COLUMNS
               if source in summary.columns]
    return summary.select([pl.col(source).alias(label)
                           for source, label in present])


def notes_for_board(tendencies: pl.DataFrame,
                    board: pl.DataFrame) -> pl.DataFrame:
    """Drop the player clause from a note when that player is not draftable.

    A manager's loyalty is measured over every draft on record, which is what
    makes it a tendency and also what makes it go stale: three of the six
    Winfield_Football notes cited Leonard Fournette, LeSean McCoy or T.Y. Hilton,
    none of whom are on a 2026 board. True, and useless at a 2026 draft — it
    spends a third of a note nobody has time to read on a player nobody can take.

    Only the loyalty clause is filtered. Timing, team lean and rookie appetite are
    statements about *how* a manager drafts and stay true whoever is available.

    The clause is removed rather than replaced. The sentences a note is built from
    are chosen under constraints this cannot see — at most two timing traits, one
    per family — so promoting the next trait in the list could produce a note the
    builder would never have written. A shorter note is the honest outcome.

    Args:
        tendencies: A stored tendencies frame, carrying ``traits``,
            ``description`` and ``favourite_player``.
        board: This season's board, for ``player_name``.

    Returns:
        pl.DataFrame: The same frame with ``description`` and ``traits`` rewritten
        where the cited player is absent from the board, plus
        ``favourite_player_draftable``. Unchanged when either input lacks the
        columns to decide.
    """
    from Scripts.draft.tendencies import sentence_case

    needed = {"traits", "description", "favourite_player", "seasons"}
    if (tendencies.is_empty() or not needed <= set(tendencies.columns)
            or "player_name" not in board.columns):
        return tendencies

    draftable = set(board["player_name"].to_list())

    def _rewrite(row: dict) -> dict:
        player = row["favourite_player"]
        if not player or player in draftable:
            return {"description": row["description"], "traits": row["traits"],
                    "favourite_player_draftable": bool(player)}

        # The traits that made it into the note, in the order they appear there.
        # Matching on the trait text rather than splitting the description on
        # sentences, because "T.Y. Hilton" is full of full stops.
        text = (row["description"] or "").lower()
        used = [trait for trait in (row["traits"] or [])
                if trait and trait.lower() in text]
        kept = [trait for trait in used if player.lower() not in trait.lower()]
        seasons = row["seasons"]
        if not kept:
            rewritten = (f"{len(used)} of this manager's tendencies are about "
                         f"players who are not on this year's board. "
                         f"({seasons} drafts)")
        else:
            rewritten = (" ".join(sentence_case(trait) for trait in kept)
                         + f" ({seasons} drafts)")
        return {"description": rewritten,
                "traits": [t for t in (row["traits"] or [])
                           if player.lower() not in t.lower()],
                "favourite_player_draftable": False}

    rewritten = [_rewrite(row) for row in tendencies.iter_rows(named=True)]
    return tendencies.with_columns(
        pl.Series("description", [r["description"] for r in rewritten]),
        pl.Series("traits", [r["traits"] for r in rewritten]),
        pl.Series("favourite_player_draftable",
                  [r["favourite_player_draftable"] for r in rewritten]),
    )


def tendency_frame(tendencies: pl.DataFrame) -> pl.DataFrame:
    """Select and rename the columns the tendency detail table shows.

    Args:
        tendencies: A stored tendencies frame.

    Returns:
        pl.DataFrame: Renamed display columns, in :data:`TENDENCY_COLUMNS` order.
    """
    present = [(source, label) for source, label in TENDENCY_COLUMNS
               if source in tendencies.columns]
    return tendencies.select([pl.col(source).alias(label)
                              for source, label in present])


def with_model_evidence(board: pl.DataFrame) -> pl.DataFrame:
    """Add ``usg_evidence_label``: why the model's number is thin, or missing.

    The board carries the model's self-assessment across four columns, and an empty
    ``USG`` cell can mean three different things that matter differently at a draft:
    the model does not cover the position, the model declined to price a player it
    could see, or the injury report withdrew a price it had already made. Collapsing
    those into one blank throws away the distinction; this resolves them into one
    readable string instead.

    Order matters and is not arbitrary. A withdrawal is checked before the evidence
    text because a player can carry both -- the model flagged its evidence *and* then
    produced nothing -- and "there is no number here" is the more useful fact than
    why the number that does not exist would have been shaky.

    ``usg_evidence`` arrives as an empty string when the model ran and flagged
    nothing, and as null when the model never ran. Those are different facts and both
    would render as an empty cell, which is why neither is passed through as-is.

    Args:
        board: A stored draft board. Boards written before the usage model landed
            carry none of the ``usg_*`` columns.

    Returns:
        pl.DataFrame: The board with ``usg_evidence_label`` added, or returned
        unchanged if it carries no ``usg_arm`` to reason about — in which case
        :func:`display_frame` drops the column along with the rest of the model
        block, exactly as it does for any other artifact that predates a feature.
    """
    if "usg_arm" not in board.columns:
        return board

    evidence = (pl.col("usg_evidence") if "usg_evidence" in board.columns
                else pl.lit(None, dtype=pl.String))
    points = (pl.col("USG_Points") if "USG_Points" in board.columns
              else pl.lit(None, dtype=pl.Float64))

    return board.with_columns(
        pl.when(pl.col("usg_arm").is_null())
        .then(pl.lit(EVIDENCE_NOT_MODELLED))
        .when(pl.col("usg_arm") == "abstain")
        .then(pl.lit(EVIDENCE_WITHDRAWN_AVAILABILITY))
        .when(points.is_null())
        .then(pl.lit(EVIDENCE_WITHDRAWN_INJURY))
        .when(evidence.fill_null("") != "")
        .then(evidence)
        .otherwise(pl.lit(EVIDENCE_CLEAR))
        .alias("usg_evidence_label")
    )


def display_frame(board: pl.DataFrame) -> pl.DataFrame:
    """Select and rename the columns the table shows.

    Args:
        board: A filtered board.

    Returns:
        pl.DataFrame: Renamed display columns, in :data:`DISPLAY_COLUMNS` order,
        skipping any the artifact does not carry.
    """
    present = [(source, label) for source, label in DISPLAY_COLUMNS
               if source in board.columns]
    return board.select([pl.col(source).alias(label) for source, label in present])
