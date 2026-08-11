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

#: Column order for the board table, and the label each gets.
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
