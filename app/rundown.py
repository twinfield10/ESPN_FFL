"""What the draft actually bought, scored three ways.

The question this answers is the one you ask the morning after: *did I do well?* It
is not answerable from one number, because "well" depends on whose projections you
believe -- so this scores every roster in the league under **three independent bases**
and shows them side by side rather than averaging them into a single verdict.

============ ========================= ==========================================
**ESPN**     ``ESPN_projected_total``  the market's own view, and the one the room
                                       was mostly drafting off
**Athletic** ``ATH_projected_total``   a sixth independent vote (plan 38), and the
                                       one that disagrees most about backfields
**Ours**     ``TRUE_Points``           the blend, in this league's own scoring
============ ========================= ==========================================

Streamlit-free on purpose, like :mod:`draft_view` and :mod:`sheet_view` -- every
decision here is testable without a browser. :mod:`views.rundown_tab` draws it.

**On the word "grade".** The repo does not hand out letters, and there is a reason:
inventing one would be a *fourth* opinion wearing the clothes of a summary of the
other three. So the primary output is rank-of-N, points, and the gap to the league
median, per basis. :func:`letter` exists because a letter is what you say out loud,
and it is a **pure rescaling of the within-league percentile** of that basis's
starters total -- no new evidence, and it cannot disagree with the rank it is derived
from.

**Starters, not rosters.** A season is scored out of a starting lineup, so the
headline number is the best legal lineup a roster can field, not the sum of every
player on it. That is what makes a team that drafted four good quarterbacks look like
what it is. See :func:`fill_starters`.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import math
from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple

import polars as pl


class Basis(NamedTuple):
    """One source's opinion, and how to read it.

    Attributes:
        key: Short identifier used in column names.
        label: How it is shown.
        points: Board column carrying that source's season points.
        blurb: One sentence for the caption, so a reader knows whose view it is.
    """
    key: str
    label: str
    points: str
    blurb: str


#: The three bases, in the order they are shown.
#:
#: Ours is last deliberately. Put first it reads as the answer with two footnotes;
#: last it reads as the third opinion, which is what it is.
BASES: Tuple[Basis, ...] = (
    Basis("espn", "ESPN", "ESPN_projected_total",
          "ESPN's own season projection -- the view most of the room drafted off."),
    Basis("ath", "The Athletic", "ATH_projected_total",
          "The Athletic's season projection, a sixth independent vote. It agrees "
          "with the room on how often a team runs and disagrees about who carries, "
          "so it flattens backfields."),
    Basis("ours", "Ours", "TRUE_Points",
          "Our blend, scored in this league's own rules. One equal vote per source "
          "that has an opinion."),
)

#: Board columns the rundown joins onto each pick.
BOARD_COLUMNS: Tuple[str, ...] = (
    "player_id", "primaryPosition", "eligible_slots", "bye_week", "tier",
    "vor", "value", "value_rank_adp", "adp", "pts_p10", "pts_p90", "pts_sd",
    "espn_pos_rank", "ath_pos_rank", "adp_is_priced",
) + tuple(basis.points for basis in BASES)


def available_bases(board: pl.DataFrame) -> List[Basis]:
    """The bases this board can actually support.

    A basis whose column is missing or entirely null is dropped rather than rendered
    as a column of zeros. That is not defensive tidiness: ``ATH_projected_total``
    only exists from 2026, because The Athletic arrives as a hand-imported workbook
    (plan 38), and a season without it must show two bases rather than three empty
    ones.

    Args:
        board: A board frame.

    Returns:
        list: The usable bases, in :data:`BASES` order. Possibly empty.
    """
    usable = []
    for basis in BASES:
        if basis.points not in board.columns:
            continue
        if board[basis.points].drop_nulls().len() == 0:
            continue
        usable.append(basis)
    return usable


def picks_with_board(picks: pl.DataFrame, board: pl.DataFrame,
                     season: int) -> pl.DataFrame:
    """This season's picks, joined to everything the board knows about them.

    Joined on ``player_id`` rather than on a name. Both artifacts carry the ESPN id
    and the weekly pipeline's name-equality joins are the repo's standing source of
    missed matches -- there is no reason to inherit that here.

    Args:
        picks: A ``draft.parquet`` frame. Carries every season the league has ever
            drafted, so it is filtered.
        board: A board frame.
        season: The season to keep.

    Returns:
        pl.DataFrame: One row per pick, with the board columns attached. Empty when
        this league has not drafted this season -- which is the state seven of ten
        leagues were in the day this was written, and is not an error.
    """
    if picks.is_empty() or "season" not in picks.columns:
        return picks.head(0)

    mine = picks.filter(pl.col("season") == season)
    if mine.is_empty():
        return mine

    columns = [c for c in BOARD_COLUMNS if c in board.columns]
    return mine.join(board.select(columns), on="player_id", how="left")


def _eligible(row: dict) -> List[str]:
    """Slots a drafted player can legally fill.

    Prefers the board's ``eligible_slots`` list, which is ESPN's own answer and is
    what makes a superflex league work without a hardcoded slot map. Falls back to
    the player's own position, which is right for D/ST and kickers and is the only
    thing available for a pick the board has no row for.

    Args:
        row: One joined pick.

    Returns:
        list: Slot names.
    """
    slots = row.get("eligible_slots")
    if slots is not None and len(slots):
        return list(slots)
    position = row.get("primaryPosition") or row.get("position")
    return [position] if position else []


def fill_starters(rows: Sequence[dict], starting_slots: Dict[str, int],
                  points_column: str) -> Tuple[List[dict], float]:
    """The best legal starting lineup a roster can field, and what it projects.

    Greedy, descending by points, each player taken into the **scarcest** slot he is
    eligible for. Greedy is not a heuristic here: eligibility in fantasy football is
    a transversal matroid -- ``QB ⊂ OP``, ``RB ⊂ RB/WR ⊂ RB/WR/TE ⊂ OP`` -- and greedy
    by weight is exactly optimal on a matroid. What greedy can still get wrong is
    *which* eligible slot to spend, so slots are filled scarcest-first.

    **Scarcity is measured from this roster, not from a slot-name table.** Counting
    how many of these players are eligible for each slot gets ``D/ST`` right (a slot
    name containing a slash that is not a flex) and ``OP`` right (superflex) without
    a map that has to be kept in step with ESPN. A name-splitting version would read
    ``D/ST`` as a three-position flex.

    Args:
        rows: Joined picks for one team.
        starting_slots: This league's real starting slots, from
            ``meta["starting_slots"]``. Bench and IR are already excluded there.
        points_column: Which basis to maximise.

    Returns:
        tuple: ``(starters, total)`` -- the rows that started, each with a
        ``"slot"`` key added, and their summed points. Slots a roster cannot fill
        are simply left empty and contribute nothing, which is the honest reading of
        a team that drafted no kicker.
    """
    if not starting_slots:
        return [], 0.0

    ranked = sorted(
        (r for r in rows if r.get(points_column) is not None),
        key=lambda r: r[points_column], reverse=True)

    demand = {slot: sum(1 for r in ranked if slot in _eligible(r))
              for slot in starting_slots}
    openings: List[str] = []
    for slot, count in starting_slots.items():
        openings.extend([slot] * int(count))
    openings.sort(key=lambda slot: demand[slot])

    starters: List[dict] = []
    total = 0.0
    for row in ranked:
        eligible = _eligible(row)
        for index, slot in enumerate(openings):
            if slot in eligible:
                starters.append({**row, "slot": slot})
                total += float(row[points_column])
                openings.pop(index)
                break
        if not openings:
            break
    return starters, total


def _percentile(rank: int, teams: int) -> float:
    """Where a rank sits, 1.0 for the best and 0.0 for the worst.

    Args:
        rank: 1-based, 1 being best.
        teams: How many teams.

    Returns:
        float: 0.0 to 1.0. Always 1.0 in a one-team league, which cannot happen but
        would otherwise divide by zero.
    """
    if teams <= 1:
        return 1.0
    return 1.0 - (rank - 1) / (teams - 1)


#: Percentile floors for :func:`letter`, best first.
LETTER_BANDS: Tuple[Tuple[float, str], ...] = (
    (0.85, "A"), (0.65, "B+"), (0.45, "B"), (0.25, "C+"), (0.10, "C"),
)


def letter(rank: int, teams: int) -> str:
    """A letter for a rank, as a rescaling and nothing more.

    **This adds no information.** It is a monotone relabelling of
    :func:`_percentile`, offered because "B+" is what you say out loud and "4th of 6
    on The Athletic's numbers" is what you mean. Because it is derived from the rank
    it is shown beside, the two cannot disagree -- which is the whole reason it is
    computed here rather than eyeballed.

    Args:
        rank: 1-based, 1 being best.
        teams: How many teams.

    Returns:
        str: One of A, B+, B, C+, C, D.
    """
    share = _percentile(rank, teams)
    for floor, grade in LETTER_BANDS:
        if share >= floor:
            return grade
    return "D"


#: Standard-normal z for a 10th/90th percentile. Plan 28 publishes p10/p90, so the
#: team band is quoted on the same two quantiles the player band is.
Z_P90 = 1.2816


def team_interval(starters: Sequence[dict], total: float
                  ) -> Tuple[Optional[float], Optional[float], int]:
    """An 80% band on a starting lineup's season total.

    **Not the sum of the players' own p10s, and that distinction is the whole
    function.** Adding up nine 10th percentiles prices the world where every starter
    busts *at once*, which is not a 10th percentile of anything -- for one real
    six-team roster it read 1,018 against a projection of 2,183, a "floor" 53% below
    the mean that nothing in the model supports. It is the perfectly-correlated
    corner, and quoting it as a floor would make every roster in every league look
    like a coin flip.

    So the band is built from the per-player standard deviations instead:
    ``sd_team = sqrt(Σ sd_i²)``, the independent sum, then ``total ± 1.2816 · sd_team``.

    Two honest caveats, stated rather than hidden:

    * **Independence understates the width.** Real starters are correlated -- a
      quarterback and his own receivers most of all -- and positive correlation
      widens a total. Plan 28's ``correlation_matrices`` is the seam that fixes this;
      until then the band is a lower bound on its own width.
    * **Kickers and defences contribute no variance.** Plan 28 fits its predictive
      distribution on the usage model, which covers QB/RB/WR/TE only -- 299 of 1,036
      board rows. K and D/ST still contribute their *means* to ``total``; they add
      nothing to the spread. For two streamed slots that is a small error in the
      direction of a narrower band, and ``interval_starters`` reports how many
      starters were actually priced so a reader can see it.

    Args:
        starters: From :func:`fill_starters`.
        total: Their summed projection.

    Returns:
        tuple: ``(floor, ceiling, priced)``. ``(None, None, 0)`` when no starter
        carries an ``sd`` at all, which is the pre-2026 case and renders as blank
        rather than as a band of zero width.
    """
    variances = [float(r["pts_sd"]) ** 2 for r in starters
                 if r.get("pts_sd") is not None]
    if not variances:
        return None, None, 0
    spread = Z_P90 * math.sqrt(sum(variances))
    return total - spread, total + spread, len(variances)


def team_table(joined: pl.DataFrame, starting_slots: Dict[str, int],
               bases: Sequence[Basis]) -> pl.DataFrame:
    """One row per manager: what they drafted, scored under every basis.

    Args:
        joined: From :func:`picks_with_board`.
        starting_slots: This league's real starting slots.
        bases: From :func:`available_bases`.

    Returns:
        pl.DataFrame: Columns ``owner``, ``picks``, ``spent``, ``value``, ``vor``,
        ``floor``, ``ceiling``, ``interval_starters``, then ``<key>_points`` / ``<key>_rank`` /
        ``<key>_grade`` per basis, then ``consensus_rank``. Sorted by consensus.
        Empty in, empty out.
    """
    if joined.is_empty():
        return joined.head(0)

    rows = joined.to_dicts()
    by_owner: Dict[str, List[dict]] = {}
    for row in rows:
        by_owner.setdefault(row.get("owner") or "—", []).append(row)

    records = []
    for owner, roster in by_owner.items():
        record: Dict[str, object] = {
            "owner": owner,
            "picks": len(roster),
            "spent": float(sum(r.get("bid") or 0.0 for r in roster)),
        }

        # `value` is read over the whole roster and `vor` over the starters, and the
        # split is not arbitrary. `value` is rank-against-ADP -- how far the room let
        # somebody fall -- and a bench player taken well below his price is real
        # value even in a week he does not start; that is what a bench is for. `vor`
        # is points above *replacement*, and replacement level is defined by this
        # league's starting slots, so it only means anything about a starter. Summed
        # over a full roster it reads a deep bench as a penalty: an 8-team superflex
        # roster came out at -528 because seven of its sixteen players sit below a
        # replacement line they were never competing with.
        record["value"] = float(sum(r.get("value") or 0.0 for r in roster))

        for basis in bases:
            starters, total = fill_starters(roster, starting_slots, basis.points)
            record[f"{basis.key}_points"] = total
            if basis.key == "ours":
                record["vor"] = float(sum(r.get("vor") or 0.0 for r in starters))
                low, high, covered = team_interval(starters, total)
                record["floor"] = low
                record["ceiling"] = high
                record["interval_starters"] = covered

        records.append(record)

    table = pl.DataFrame(records)
    teams = table.height

    rank_columns = []
    for basis in bases:
        points = f"{basis.key}_points"
        table = table.with_columns(
            pl.col(points).rank("min", descending=True).cast(pl.Int32)
            .alias(f"{basis.key}_rank"))
        table = table.with_columns(
            pl.col(f"{basis.key}_rank")
            .map_elements(lambda r, n=teams: letter(int(r), n), return_dtype=pl.Utf8)
            .alias(f"{basis.key}_grade"))
        rank_columns.append(f"{basis.key}_rank")

    if rank_columns:
        table = table.with_columns(
            pl.mean_horizontal([pl.col(c) for c in rank_columns])
            .alias("consensus_rank"))
        table = table.sort("consensus_rank")
    return table


def median_gap(table: pl.DataFrame, basis: Basis) -> pl.DataFrame:
    """Add each team's distance from the league median under one basis.

    The gap is the part a rank cannot tell you: finishing fourth of six by two points
    and finishing fourth by ninety are different drafts.

    Args:
        table: From :func:`team_table`.
        basis: Which basis.

    Returns:
        pl.DataFrame: ``table`` with a ``<key>_vs_median`` column added.
    """
    points = f"{basis.key}_points"
    if table.is_empty() or points not in table.columns:
        return table
    middle = table[points].median()
    return table.with_columns(
        (pl.col(points) - middle).alias(f"{basis.key}_vs_median"))


def notable_picks(joined: pl.DataFrame, owner: str, count: int = 3
                  ) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """One manager's best and worst picks against where the room took them.

    Ordered on ``value``, which is ``value_rank_adp - value_rank_vor`` -- how far the
    room let a player fall past what he is worth to *this* league. Positive is a
    steal. Restricted to market-priced players where the board says so: ESPN parks
    every unpriced player on one ADP plateau around 170, and a "steal" measured
    against a plateau is an artefact of the plateau.

    Args:
        joined: From :func:`picks_with_board`.
        owner: Whose picks.
        count: How many of each.

    Returns:
        tuple: ``(steals, reaches)``, each a frame of at most ``count`` rows.
    """
    if joined.is_empty() or "value" not in joined.columns:
        empty = joined.head(0)
        return empty, empty

    mine = joined.filter(
        (pl.col("owner") == owner) & pl.col("value").is_not_null())
    if "adp_is_priced" in mine.columns:
        priced = mine.filter(pl.col("adp_is_priced").fill_null(False))
        # Only substitute the unfiltered set when pricing would leave nothing to
        # show -- a board with no `adp_is_priced` at all should still say something.
        mine = priced if not priced.is_empty() else mine

    ordered = mine.sort("value", descending=True)
    return ordered.head(count), ordered.tail(count).reverse()


def roster_frame(joined: pl.DataFrame, owner: str, starting_slots: Dict[str, int],
                 bases: Sequence[Basis]) -> pl.DataFrame:
    """One manager's roster, marked with the slot each player starts in.

    Args:
        joined: From :func:`picks_with_board`.
        owner: Whose roster.
        starting_slots: This league's real starting slots.
        bases: From :func:`available_bases`. The **last** one decides the lineup,
            which is ``Ours`` in :data:`BASES` order -- the start/sit question is
            asked of our own numbers.

    Returns:
        pl.DataFrame: The roster in draft order, with ``slot`` set to the starting
        slot a player fills or ``"BE"`` when he does not.
    """
    if joined.is_empty() or not bases:
        return joined.head(0)

    roster = joined.filter(pl.col("owner") == owner).to_dicts()
    starters, _ = fill_starters(roster, starting_slots, bases[-1].points)
    slot_of = {r["player_id"]: r["slot"] for r in starters}

    out = []
    for row in roster:
        out.append({**row, "slot": slot_of.get(row.get("player_id"), "BE")})
    frame = pl.DataFrame(out)
    return frame.sort("overall_pick") if "overall_pick" in frame.columns else frame


def league_owners(joined: pl.DataFrame) -> List[str]:
    """Managers who made a pick this season.

    Args:
        joined: From :func:`picks_with_board`.

    Returns:
        list: Owner names, sorted.
    """
    if joined.is_empty() or "owner" not in joined.columns:
        return []
    return sorted({o for o in joined["owner"].to_list() if o})


def my_row(table: pl.DataFrame, owner: Optional[str]) -> Optional[dict]:
    """The viewer's row out of :func:`team_table`, if they are in this league.

    Args:
        table: From :func:`team_table`.
        owner: ``meta["primary_owner"]``.

    Returns:
        dict | None: None when the owner is unknown or did not draft -- which is the
        normal case for the five leagues that belong to other people.
    """
    if not owner or table.is_empty():
        return None
    mine = table.filter(pl.col("owner") == owner)
    return mine.to_dicts()[0] if not mine.is_empty() else None
