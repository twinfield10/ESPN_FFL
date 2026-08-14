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

from typing import Dict, List, Optional, Sequence

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
    ("vor_rank", "VOR rk"),
    ("pos_rank", "Pos rk"),
    ("adp", "ADP"),
    ("value", "Value"),
    ("auction_value_filled", "$"),
    ("USG_Points", "USG"),
    ("USG_PosRankDelta", "Δrk"),
    ("usg_expected_games", "Exp G"),
    ("usg_evidence_label", "Model evidence"),
    ("injury_status", "Injury"),
    ("team_owner", "Owner"),
]


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


def filter_board(
    board: pl.DataFrame,
    positions: Optional[Sequence[str]] = None,
    *,
    only_available: bool = True,
    hide_unstartable: bool = True,
    hide_unprojected: bool = True,
    max_tier: Optional[int] = None,
    search: str = "",
) -> pl.DataFrame:
    """Apply the page's filters.

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
    if max_tier is not None and "tier" in out.columns:
        out = out.filter(pl.col("tier").is_null() | (pl.col("tier") <= max_tier))
    if search:
        out = out.filter(pl.col("player_name").str.contains(f"(?i){search}"))
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
    ("headline", "Reads as"),
    ("earliest_position", "Earliest"),
    ("earliest_delta", "Rds early"),
    ("latest_position", "Latest"),
    ("latest_delta", "Rds late"),
    ("favourite_team", "NFL lean"),
    ("favourite_team_excess", "Extra picks"),
    ("favourite_player", "His guy"),
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
SOURCE_ADDED = "Added in season"

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
                                 "share_drafted": pl.Float64})
    if (lineups.is_empty() or picks.is_empty()
            or not needed <= set(lineups.columns)
            or "team_name" not in picks.columns):
        return empty

    drafted = (picks.select(_franchise_key("team_name").alias("_team"), "player_id")
               .unique()
               .with_columns(pl.lit(True).alias("_drafted")))
    drafting_teams = drafted.select("_team").unique()

    rows = (lineups
            .filter(pl.col("team_owner").is_not_null()
                    & (pl.col("team_owner") != FREE_AGENT_OWNER))
            .with_columns(_franchise_key("team_name").alias("_team"))
            .join(drafted, on=["_team", "player_id"], how="left")
            .with_columns(pl.col("_drafted").fill_null(False))
            .join(drafting_teams.with_columns(pl.lit(True).alias("_known")),
                  on="_team", how="left")
            .with_columns(pl.col("_known").fill_null(False)))

    if starters_only:
        rows = rows.filter(
            pl.col("slotPosition").is_in(list(NON_STARTING_SLOTS)).not_())

    if rows.is_empty():
        return empty

    return (rows.group_by("_team")
            .agg(pl.col("team_owner").mode().first().alias("owner"),
                 pl.col("_known").first().alias("_known"),
                 pl.col("points").filter(pl.col("_drafted")).sum().alias("_drafted_pts"),
                 pl.col("points").filter(pl.col("_drafted").not_()).sum().alias("_added_pts"),
                 pl.col("points").sum().alias("total"))
            .with_columns(
                pl.when("_known").then(pl.col("_drafted_pts")).alias("drafted"),
                pl.when("_known").then(pl.col("_added_pts")).alias("added"))
            .with_columns(
                pl.when(pl.col("total") > 0)
                .then(100 * pl.col("drafted") / pl.col("total"))
                .otherwise(None).alias("share_drafted"),
                pl.col("owner").map_elements(owner_label, return_dtype=pl.Utf8)
                .alias("manager"))
            .select("owner", "manager", "drafted", "added", "total",
                    "share_drafted")
            .sort("total", descending=True))


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
