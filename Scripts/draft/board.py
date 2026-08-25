"""The draft board: replacement level, VOR, tiers and value, per league.

Projected points alone do not tell you who to draft. 300 points from a QB in a
one-QB league is worth less than 250 from a RB in a two-RB-plus-flex league,
because the QB you *don't* take is nearly as good and the RB you don't take is
not. That difference is value over replacement, and replacement level is a
property of the league's roster, not of the player.

Which is why this is computed nine times from one market pull. The scan found real
variety: 6 to 16 teams, a superflex ``OP`` slot in Weenieless Wanderers, IDP
``DP`` in GOP Degenerates, no D/ST in 12 Dudes. **The same player is legitimately
ranked differently across them**, and that is the entire reason to build a board
rather than read one off a website.

See ``docs/plans/15-draft-board.md``.
"""

import warnings
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import polars as pl

from Scripts.nfl_utils import load_schedule
from Scripts.paths import NFL_SCHEDULE_CSV
from Scripts.season_projections import PROJECTION_PREFIXES

#: ESPN's team abbreviations that differ from the schedule's, ESPN spelling first.
ESPN_TEAM_ALIASES: Dict[str, str] = {"LAR": "LA", "WSH": "WAS"}

#: Flex slots and the positions that may fill them. ESPN's ``OP`` is "offensive
#: player" -- a superflex that accepts a quarterback, which is what makes
#: Weenieless Wanderers value QBs completely differently from every other league.
FLEX_SLOTS: Dict[str, List[str]] = {
    "RB/WR": ["RB", "WR"],
    "WR/TE": ["WR", "TE"],
    "RB/WR/TE": ["RB", "WR", "TE"],
    "OP": ["QB", "RB", "WR", "TE"],
    "DP": ["LB", "DE", "DT", "CB", "S", "DL", "DB"],
}

#: Slots that hold a player without starting them.
NON_STARTING_SLOTS = {"BE", "IR", "", " "}

#: Every defensive position, individual or team unit.
DEFENSIVE_POSITIONS = {"D/ST", "DL", "DE", "LB", "NT", "CB", "S", "DT", "DB", "OLB"}

#: Positions that are streamed rather than held, and for which season-total VOR
#: therefore lies.
#:
#: VOR asks "how many more points than the last startable player at this
#: position". That question assumes you hold one player all season. For kickers and
#: team defences you do not -- you start whoever has the good matchup and drop them
#: after, so your real replacement level is close to the *best available in any
#: given week*, not to the season total of the 14th-best unit.
#:
#: The first version of this board did not distinguish them and its eight best
#: "values" in the league were all team defences: D/ST goes around pick 120-170 by
#: convention, so measured against a D/ST14 baseline every good defence looks like a
#: steal. It is not a steal, it is a position you stream. ``value`` is left NaN for
#: these so the board's headline signal stays trustworthy; ``vor`` is still
#: computed, so nothing is hidden.
STREAMED_POSITIONS = {"K", "D/ST"}

#: Target players per tier when choosing how many tiers to fit.
TIER_TARGET_SIZE = 6

#: Share of the pool that must pile up on one ADP value before it is read as
#: ESPN's "undrafted" filler rather than a real market price. Measured on 2026:
#: **758 of 1,000 players share an ADP of exactly 170.0**, and only 159 have a
#: genuine one. Ranking inside that plateau is noise, and comparing it to a
#: projection produces nonsense -- the first draft of this module surfaced backup
#: kickers as the best values in the league on exactly that mistake.
ADP_PLATEAU_MIN_SHARE = 0.05

#: Bounds on tiers per position. Below 2 there is nothing to see; above 8 the
#: bands stop being glanceable, which is the whole point of tiering.
MIN_TIERS, MAX_TIERS = 2, 8

#: KMeans is seeded so a rebuilt board is byte-identical to the last one.
TIER_RANDOM_STATE = 0


class DraftBoardWarning(UserWarning):
    """The board was built with a caveat worth surfacing in the UI."""


def _warn(msg: str) -> None:
    """Warn past the process-wide ``filterwarnings("ignore")`` in fetch_utils."""
    with warnings.catch_warnings():
        warnings.simplefilter("always", DraftBoardWarning)
        warnings.warn(msg, DraftBoardWarning, stacklevel=3)


def starting_slots(league) -> Dict[str, int]:
    """The league's starting lineup, slot to count.

    Args:
        league: A live ESPN ``League`` with ``roster_settings`` attached by
            :func:`Scripts.fetch_utils.fetch_league`.

    Returns:
        dict: e.g. ``{"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, ...}``.
    """
    settings = getattr(league, "roster_settings", {}) or {}
    slots = settings.get("starting_roster_slots") or {}
    return {slot: int(count) for slot, count in slots.items()
            if slot not in NON_STARTING_SLOTS and int(count) > 0}


def replacement_ranks(
    slots: Dict[str, int],
    teams: int,
    pool: Optional[pd.DataFrame] = None,
    points_column: str = "TRUE_Points",
) -> Dict[str, int]:
    """How deep each position gets drafted as a starter, in this league.

    Dedicated slots are exact: eight teams starting one QB each means QB8 is the
    last starting quarterback, so QB9 is replacement.

    Flex slots are not, and an even split across the eligible positions is simply
    wrong. The split is read off **this league's own projected points**: pool the
    eligible players, skip the ones a dedicated slot already claims, and give the
    remaining openings to whoever projects best. That models what a drafter
    actually does -- fill the flex with the best player available by their own
    valuation.

    Deliberately *not* allocated by ADP, which was the first approach and was
    wrong in a way worth recording. Global ADP comes overwhelmingly from
    single-QB leagues, so pooling by ADP filled Weenieless Wanderers' superflex
    ``OP`` slot almost entirely with running backs and left QB replacement at
    QB10 -- identical to a non-superflex league. Josh Allen came out *less*
    valuable in the superflex league than in a one-QB league, which is backwards.
    Points-based allocation gives the ``OP`` slot to quarterbacks, because in this
    league's scoring that is who is best available, which is also what happens in
    a real superflex draft.

    Args:
        slots: Starting slots from :func:`starting_slots`.
        teams: Number of teams in the league.
        pool: A frame with ``primaryPosition`` and ``points_column`` -- the
            projections. Without it, flex openings are split evenly and a warning
            says so.
        points_column: Which projection to allocate on.

    Returns:
        dict: Position to replacement rank -- the 1-based rank of the first
        non-starter at that position.
    """
    dedicated: Dict[str, float] = {}
    for slot, count in slots.items():
        if slot in FLEX_SLOTS:
            continue
        dedicated[slot] = dedicated.get(slot, 0.0) + teams * count

    flex_share: Dict[str, float] = {}
    for slot, count in slots.items():
        eligible = FLEX_SLOTS.get(slot)
        if not eligible:
            continue
        shares = _flex_split(eligible, teams * count, dedicated, pool, points_column)
        for position, share in shares.items():
            flex_share[position] = flex_share.get(position, 0.0) + share

    ranks = {}
    for position in set(dedicated) | set(flex_share):
        total = int(round(dedicated.get(position, 0.0)
                          + flex_share.get(position, 0.0)))
        # A position that rounds to zero starters is omitted rather than floored at
        # 1. GOP Degenerates' single ``DP`` slot goes almost entirely to
        # linebackers on projected points, leaving cornerbacks a fractional share:
        # reporting "replacement CB1" would imply the best cornerback in football is
        # already replacement level, when the truth is that no cornerback starts
        # there. Omitting the position marks it unstartable instead.
        if total >= 1:
            ranks[position] = total
    return ranks


def _flex_split(
    eligible: List[str],
    openings: float,
    dedicated: Dict[str, float],
    pool: Optional[pd.DataFrame],
    points_column: str,
) -> Dict[str, float]:
    """Divide flex openings among the positions that can fill them.

    Args:
        eligible: Positions the slot accepts.
        openings: Total openings across the league.
        dedicated: Positions already consumed by dedicated slots.
        pool: Projections frame, or None for an even split.
        points_column: Projection column to rank on.

    Returns:
        dict: Position to number of openings.
    """
    if openings <= 0:
        return {}

    usable = (pool is not None and not pool.empty
              and points_column in pool.columns
              and "primaryPosition" in pool.columns)
    if not usable:
        _warn(
            f"No projections for the flex split, so {openings:.0f} flex opening(s) "
            f"were divided evenly across {eligible}. Replacement level for these "
            f"positions is approximate."
        )
        return {position: openings / len(eligible) for position in eligible}

    ranked = pool[pool["primaryPosition"].isin(eligible)]
    ranked = ranked[ranked[points_column].notna()].sort_values(
        points_column, ascending=False)

    # Skip the players a dedicated slot already claims, so the flex pool starts
    # where the starting lineup leaves off.
    taken = {position: int(dedicated.get(position, 0)) for position in eligible}
    counted = {position: 0 for position in eligible}
    filled = 0

    for position in ranked["primaryPosition"]:
        if filled >= openings:
            break
        if taken.get(position, 0) > 0:
            taken[position] -= 1
            continue
        counted[position] = counted.get(position, 0) + 1
        filled += 1

    if filled == 0:
        return {position: openings / len(eligible) for position in eligible}
    # Scale in case the pool ran out before the openings did.
    scale = openings / filled
    return {position: count * scale for position, count in counted.items() if count}


def adp_plateau(adp: pd.Series, min_share: float = ADP_PLATEAU_MIN_SHARE) -> Optional[float]:
    """The ADP above which ESPN's number is filler rather than a market price.

    ESPN reports an ``averageDraftPosition`` for every player it knows about, but
    only prices the ones the market actually drafts. The rest are parked on a
    single value -- 170.0 in 2026, shared by 758 of 1,000 players. Detected rather
    than hardcoded, because the plateau moves with the season and the player pool.

    Args:
        adp: The market's ADP column.
        min_share: Share of the pool that must share one rounded value for it to
            count as a plateau.

    Returns:
        float | None: The ADP at which filler begins, or None when no plateau is
        found -- in which case every ADP is treated as real.
    """
    clean = adp.dropna()
    if clean.empty:
        return None
    counts = clean.round(0).value_counts()
    value, count = float(counts.index[0]), int(counts.iloc[0])
    # A pile-up needs at least two players on one value. Without this, every ADP in
    # a short all-distinct series is its own "modal bin" and trivially clears the
    # share threshold, so the whole board reads as unpriced.
    if count < 2 or count / len(clean) < min_share:
        return None
    # The modal bin is filler, so the plateau starts just below it.
    return value - 1.0


def bye_weeks(season: int) -> Dict[str, int]:
    """Each team's bye week, from the NFL schedule.

    A bye is an absence, so it is derived rather than read: the week in the
    season's range where a team appears in neither the home nor the away column.

    ESPN and nflverse disagree on two abbreviations -- ESPN says ``LAR`` and
    ``WSH`` where the schedule says ``LA`` and ``WAS`` -- so both spellings are
    returned and the caller can map either.

    Args:
        season: Season year. The schedule CSV holds one season at a time, and a
            mismatch returns ``{}`` rather than another season's byes.

    Returns:
        dict: ``{team_abbr: bye_week}``. Empty when the schedule is missing, is
        for a different season, or is mid-release with no clean single bye.
    """
    try:
        schedule = load_schedule()
    except FileNotFoundError as e:
        _warn(f"no NFL schedule, so the board carries no bye weeks ({e}).")
        return {}

    seasons = schedule["season"].unique().to_list()
    if seasons != [season]:
        _warn(f"{NFL_SCHEDULE_CSV.name} covers {seasons}, not {season}, so the "
              f"board carries no bye weeks. Re-run `Rscript R/GetNFL.R {season}`.")
        return {}

    all_weeks = set(schedule["week"].unique().to_list())
    played = {}
    for column in ("home_team", "away_team"):
        for team, weeks in schedule.group_by(column).agg("week").iter_rows():
            played.setdefault(team, set()).update(weeks)

    byes = {}
    for team, weeks in played.items():
        missing = sorted(all_weeks - weeks)
        # Exactly one missing week is a bye. Anything else means the schedule is
        # partial, and guessing would put a wrong bye on a draft board.
        if len(missing) == 1:
            byes[team] = missing[0]

    # ESPN's spellings, so a caller can map straight off pro_team.
    for espn_abbr, schedule_abbr in ESPN_TEAM_ALIASES.items():
        if schedule_abbr in byes:
            byes[espn_abbr] = byes[schedule_abbr]
    return byes


def assign_tiers(points: pd.Series) -> pd.Series:
    """Cluster one position's projected points into tiers.

    1-D KMeans, which on a single axis is Jenks natural breaks: it puts the tier
    boundaries where the gaps in projected points actually are. That matters more
    than rank -- the decision a board drives is "take one of these four before they
    run out", not "this player is one spot better than that one".

    The number of tiers targets :data:`TIER_TARGET_SIZE` players each, bounded by
    :data:`MIN_TIERS` and :data:`MAX_TIERS`, so a 359-deep position does not get
    60 bands and a 12-deep one does not get two.

    Args:
        points: Projected points for one position. Index is preserved.

    Returns:
        pd.Series: 1-based tier number, 1 being the best. Aligned to ``points``.
    """
    clean = points.dropna()
    if clean.empty:
        return pd.Series(index=points.index, dtype="float64")
    if clean.nunique() < MIN_TIERS:
        return pd.Series(1.0, index=points.index).where(points.notna())

    n_tiers = int(np.clip(round(len(clean) / TIER_TARGET_SIZE), MIN_TIERS, MAX_TIERS))
    n_tiers = min(n_tiers, clean.nunique())

    # Deferred: sklearn is a heavy import and only this function needs it.
    from sklearn.cluster import KMeans

    model = KMeans(n_clusters=n_tiers, n_init=10, random_state=TIER_RANDOM_STATE)
    labels = model.fit_predict(clean.to_numpy().reshape(-1, 1))

    # KMeans labels are arbitrary; renumber so tier 1 holds the highest points.
    order = np.argsort(-model.cluster_centers_.ravel())
    remap = {int(old): rank + 1 for rank, old in enumerate(order)}

    tiers = pd.Series(labels, index=clean.index).map(remap).astype(float)
    return tiers.reindex(points.index)


def build_board(
    league,
    projections: pd.DataFrame,
    market: pd.DataFrame,
    points_column: str = "TRUE_Points",
    crosswalk_warn_below: Optional[float] = 60.0,
    season: Optional[int] = None,
) -> pd.DataFrame:
    """Assemble one league's draft board.

    Args:
        league: A live ESPN ``League`` -- supplies team count and starting slots.
        projections: :func:`Scripts.season_projections.build_season_projections`
            output, carrying ``player_id`` and ``points_column``.
        market: :func:`Scripts.draft.adp.fetch_draft_market` output.
        points_column: Which projection to value on. ``TRUE_Points`` is the blend;
            ``ESPN_Points`` isolates ESPN.
        crosswalk_warn_below: Warn when fewer than this percentage of players
            resolve to a play-by-play id. None disables the check -- appropriate
            when the frame's ids are synthetic.
        season: Season year, for the bye-week join. Defaults to ``league.year``;
            without either, ``bye_week`` is NaN.

    Returns:
        pd.DataFrame: One row per player, sorted by ``vor`` descending, with
        ``vor``, ``vor_rank``, ``pos_rank``, ``tier``, ``adp``, ``adp_rank``,
        ``value``, ``adp_is_priced``, ``replacement_rank``, ``bye_week``, the four
        ESPN comparison columns of :func:`_attach_espn_comparison` and the market
        columns. ``value`` is NaN wherever the market has not priced the player --
        see :func:`adp_plateau`.

    Raises:
        KeyError: If ``projections`` lacks ``player_id`` or ``points_column``.
    """
    for required in ("player_id", points_column):
        if required not in projections.columns:
            raise KeyError(
                f"build_board needs {required!r} in projections; got "
                f"{sorted(projections.columns)[:12]}... Pass a frame from "
                f"build_season_projections(market=...)."
            )

    if season is None:
        season = getattr(league, "year", None)
    teams = len(getattr(league, "teams", []) or [])
    slots = starting_slots(league)
    replacement = replacement_ranks(slots, teams, projections, points_column)

    # player_id join: exact, unlike the name_key joins the book sources still need.
    # `injury_status` is excluded because the projections frame already carries it --
    # `build_season_projections` needs it to withdraw the usage model for players
    # ESPN lists as out. Merging it from both sides would suffix them into
    # `injury_status_x`/`_y` and silently break every consumer.
    market_cols = [c for c in market.columns if not c.startswith("ESPN_")
                   and c not in ("name_key", "player_name", "primaryPosition",
                                 "pro_team", "games", "injury_status")]
    board = projections.merge(market[market_cols], on="player_id", how="left")

    board["pos_rank"] = board.groupby("primaryPosition")[points_column].rank(
        ascending=False, method="min")
    board["replacement_rank"] = board["primaryPosition"].map(replacement)

    board["vor"] = _value_over_replacement(board, points_column, replacement)
    board["vor_rank"] = board["vor"].rank(ascending=False, method="min")

    # Only compare against an ADP the market actually set. Everything on the
    # plateau is ESPN filler, and ranking inside it is noise.
    plateau = adp_plateau(board["adp"]) if "adp" in board.columns else None
    if plateau is None:
        board["adp_is_priced"] = board.get("adp", pd.Series(dtype=float)).notna()
    else:
        board["adp_is_priced"] = board["adp"].notna() & (board["adp"] < plateau)
    board["adp_plateau"] = plateau

    # Season-total VOR does not describe a position you stream -- see
    # STREAMED_POSITIONS. Excluded from the value comparison, not from the board.
    board["is_streamed"] = board["primaryPosition"].isin(STREAMED_POSITIONS)

    priced = board["adp_is_priced"]
    comparable = priced & ~board["is_streamed"]

    board["adp_rank"] = board.loc[priced, "adp"].rank(ascending=True, method="min")

    # Ranked within the same population the value is compared over, so the
    # difference is apples-to-apples. Comparing a 1-160 ADP rank against a 1-1000
    # VOR rank manufactures value for anyone the market has not priced.
    board["value_rank_adp"] = board.loc[comparable, "adp"].rank(
        ascending=True, method="min")
    board["value_rank_vor"] = board.loc[comparable, "vor"].rank(
        ascending=False, method="min")

    # Positive means the room is letting them fall past where our projections put
    # them. NaN means either the market has no opinion -- which for a high-VOR
    # player is its own signal, one plan 09 should show rather than score -- or the
    # position is streamed and the comparison would not mean anything.
    board["value"] = board["value_rank_adp"] - board["value_rank_vor"]

    board = _attach_espn_comparison(board, points_column)

    board["tier"] = (
        board.groupby("primaryPosition")[points_column]
        .transform(assign_tiers)
    )

    # An auction value for every row: the market average where ESPN publishes one
    # (only the top ~313 of 1,000), ESPN's own suggested value otherwise.
    if "auction_value" in board.columns:
        board["auction_value_filled"] = board["auction_value"].fillna(
            board.get("espn_auction_value"))

    # The play-by-play join key, attached here so the board is the bridge between
    # ESPN's world and nflverse's. ~99% of individual players resolve; team D/ST
    # units have no player id and never will. Missing crosswalk is non-fatal --
    # the board is still a board without it.
    try:
        from Scripts.crosswalk import attach_gsis_id
        board = attach_gsis_id(board, espn_id_column="player_id",
                               warn_below=crosswalk_warn_below)
    except FileNotFoundError as e:
        board["gsis_id"] = pd.NA
        _warn(f"no player-id crosswalk, so the board carries no play-by-play join "
              f"key ({e}). Generate it with `Rscript R/GetPlayerIDs.R`.")

    # "No source produced a scored line", not "the blend is null". The blend
    # 0-fills, so the old `board[points_column].isna()` was False for all 1,026
    # rows in every league -- including the 503 whose projection is a literal 0.0,
    # two of which the market has even priced. The signal only became computable
    # once `_apply_scoring` stopped collapsing a sparse source to NaN.
    #
    # Distinct from `sources_real`, which counts sources with a *non-imputed*
    # line: a player whose only line is imputed from the ESPN/FP mean does have a
    # projection, it is just derived.
    opinion_points = [f"{prefix}_Points" for prefix in PROJECTION_PREFIXES
                      if f"{prefix}_Points" in board.columns]
    board["projection_missing"] = (
        board[opinion_points].isna().all(axis=1) if opinion_points
        else board[points_column].isna()
    ) | board[points_column].isna()

    # A bye week is draft-relevant: two starters sharing one is a real cost, and
    # nothing else on the board carries it. Attached here rather than in the app so
    # a page render stays a parquet read.
    byes = bye_weeks(season) if season is not None else {}
    board["bye_week"] = (
        board["pro_team"].map(byes)
        if byes and "pro_team" in board.columns
        else pd.Series(np.nan, index=board.index)
    )

    # A position with no starting slot is undraftable here -- 32 team defences on
    # 12 Dudes one Cup's board, which has no D/ST slot. They are kept rather than
    # dropped so the board is a complete picture of the pool, but flagged so plan
    # 09 can filter them out by default instead of showing a column of zeroes.
    board["startable"] = board["primaryPosition"].isin(replacement)
    # Recorded on the rows because a board is read standalone as often as it is
    # read next to its meta.json, and replacement level is meaningless without it.
    board["teams"] = teams

    # No IDP scoring caveat here. Building this board is what prompted
    # build_season_projections to score through proj_to_score, which applies the
    # slot-16 override to D/ST units and the base value to individual defenders --
    # plan 11's machinery, which the season path had not been using. Before that,
    # GOP Degenerates' linebackers were priced with the D/ST override of 0.0 for
    # tackles and projected near zero.

    # Backfield rank and handcuff value. The one part of the game-script narrative
    # that survived measurement: a strong team's number-two back gets ~19 more
    # carries than a weak team's while RB1 stays flat, so the handcuff is worth
    # more and nothing else on the board says so. Small -- see
    # `Scripts.draft.handcuff` for the R-squared it ships alongside itself.
    if season is not None:
        try:
            from Scripts.draft.handcuff import attach_handcuff
            board = attach_handcuff(
                pl.from_pandas(board), season, points_column=points_column
            ).to_pandas()
        except (FileNotFoundError, ImportError) as e:
            _warn(f"no handcuff columns on this board ({e}).")

    # Plan 28's season-points distribution. **Here rather than in
    # `build_season_projections`, and the reason is the join key**: the room draw matches
    # players against the depth chart on `gsis_id`, which this function attaches a few
    # dozen lines above and which the projection frame does not carry at all.
    #
    # Additive. The board is still ordered by `TRUE_Points`; whether `p_top12` may change
    # that is G-D3's decision, not this call's.
    if season is not None and league is not None:
        try:
            from Scripts.season_projections import attach_outcome_distribution
            from Scripts.scoring import get_scoring_table
            scored = get_scoring_table(league, season=season, verify=False)
            board = attach_outcome_distribution(
                board, season, league,
                [c for c in scored["colName"].dropna().unique()])
        # `AttributeError` is in the list for the same reason the others are: a league
        # object that cannot resolve its own scoring -- a stub in a test, a partially
        # constructed one -- is a missing input, and every attacher here degrades to
        # "no columns" rather than taking the board down with it.
        except (FileNotFoundError, ImportError, KeyError, AttributeError) as e:
            _warn(f"no outcome columns on this board ({e}).")

    # Sorted by VOR, not by value: value is NaN for everyone the market has not
    # priced, which in 2026 is 84% of the pool. Plan 09 offers sort-by-value as an
    # interaction; it is the wrong default for a stored artifact.
    return board.sort_values("vor", ascending=False, na_position="last").reset_index(
        drop=True)


def _attach_espn_comparison(board: pd.DataFrame, points_column: str) -> pd.DataFrame:
    """Rank ESPN's own board within position, and difference it against ours.

    Four columns, all of which are ESPN's opinion set beside ours. ESPN's half was
    already on every stored board and shown nowhere: ``ESPN_Points`` is ESPN's stat
    line scored in *this league's* rules, and ``espn_draft_rank`` is ESPN's published
    draft ranking, which is dense (1..N, no ties, no plateau) and populated for every
    row -- unlike ``adp``, where 758 of 1,000 players shared a single filler value in
    2026.

    **The two ESPN quantities are kept in their own lanes on purpose.** Points are
    compared against points and a draft ranking against our draft ranking; ESPN's rank
    is not re-derived from ``ESPN_Points`` and our rank is not re-derived from ours.
    Mixing them would produce a difference that moves when either the projection or
    the ranking method changes, and there would be no way to see which.

    Every difference here is oriented so that **positive means we are higher on the
    player than ESPN is** -- points differenced ours-minus-theirs because more points
    is better, ranks theirs-minus-ours because a lower rank is better. That matches
    the convention ``value`` already set (ADP rank minus VOR rank, positive where the
    room lets a player fall) and is what lets the page colour every difference column
    on one rule instead of five.

    Args:
        board: The merged frame, after ``pos_rank`` and ``vor_rank`` are computed.
        points_column: Our blended projection, for the points difference.

    Returns:
        pd.DataFrame: ``board`` with ``espn_pos_rank``, ``points_delta``,
        ``rank_delta`` and ``pos_rank_delta`` added. Any whose inputs are absent is
        NaN rather than missing, so the board's shape does not depend on which
        columns ESPN happened to return.
    """
    espn_points = "ESPN_Points"
    missing = pd.Series(np.nan, index=board.index)

    # Ranked ascending: `espn_draft_rank` is already a rank, so 1 is best and the
    # within-position ordering is a rank of a rank.
    board["espn_pos_rank"] = (
        board.groupby("primaryPosition")["espn_draft_rank"].rank(
            ascending=True, method="min")
        if "espn_draft_rank" in board.columns else missing
    )

    board["points_delta"] = (
        board[points_column] - board[espn_points]
        if espn_points in board.columns else missing
    )
    board["rank_delta"] = (
        board["espn_draft_rank"] - board["vor_rank"]
        if "espn_draft_rank" in board.columns else missing
    )
    board["pos_rank_delta"] = board["espn_pos_rank"] - board["pos_rank"]
    return board


def _value_over_replacement(
    board: pd.DataFrame,
    points_column: str,
    replacement: Dict[str, int],
) -> pd.Series:
    """Points above the last startable player at the same position.

    Args:
        board: Frame with ``primaryPosition``, ``pos_rank`` and ``points_column``.
        points_column: Projection to measure.
        replacement: Position to replacement rank.

    Returns:
        pd.Series: VOR, aligned to ``board``.
    """
    baselines = {}
    for position, group in board.groupby("primaryPosition"):
        rank = replacement.get(position)
        if rank is None:
            # A position this league does not start -- a kicker in a league with no
            # K slot. Its replacement level is its own best player, so VOR is <= 0
            # and it sorts to the bottom rather than being dropped.
            baselines[position] = group[points_column].max()
            continue
        at_or_past = group[group["pos_rank"] >= rank][points_column]
        # If the position is shallower than replacement level, the worst available
        # player is the baseline.
        baselines[position] = (at_or_past.max() if not at_or_past.empty
                               else group[points_column].min())

    return board[points_column] - board["primaryPosition"].map(baselines)


def board_summary(board: pd.DataFrame, points_column: str = "TRUE_Points") -> str:
    """A one-line description of a built board, for refresh output.

    Args:
        board: A :func:`build_board` frame.
        points_column: The projection it was valued on.

    Returns:
        str: e.g. ``"1000 players, 313 with a projection, replacement RB31/WR33"``.
    """
    if board.empty:
        return "0 players"
    # notna() would say 1026 of 1026 in every league, because the blend 0-fills.
    # See `projection_missing` in build_board.
    if "projection_missing" in board.columns:
        with_projection = int((~board["projection_missing"]).sum())
    else:
        with_projection = int(board[points_column].notna().sum())
    priced = int(board["adp_is_priced"].sum()) if "adp_is_priced" in board else 0
    replacement = (
        board.dropna(subset=["replacement_rank"])
        .groupby("primaryPosition")["replacement_rank"].first()
        .astype(int).to_dict()
    )
    shown = ", ".join(f"{pos}{rank}" for pos, rank in sorted(replacement.items()))
    return (f"{len(board)} players, {with_projection} projected, {priced} priced by "
            f"the market; replacement {shown}")
