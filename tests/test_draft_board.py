"""The draft board: market parsing, replacement level, tiers and value.

No network. The ``kona_player_info`` payload is synthesised to the shape the real
endpoint returns, so the parser is covered without credentials.

Several of these pin bugs found while building the board against live 2026 data,
and each of those says what it is guarding.
"""

import pandas as pd
import pytest

from Scripts.draft import adp, board as bd


# --- fixtures ------------------------------------------------------------

def _entry(player_id, name, position_slot, *, adp_value=None, auction=None,
           projected=200.0, prior=180.0, injury=None, on_team=0):
    """One ``players[]`` element, shaped like the live endpoint's.

    Args:
        player_id: ESPN player id.
        name: Full name.
        position_slot: Primary eligible slot id -- 0 QB, 2 RB, 4 WR, 6 TE, 17 K,
            16 D/ST, 10 LB.
        adp_value: ``ownership.averageDraftPosition``.
        auction: ``ownership.auctionValueAverage``.
        projected: Season projected points.
        prior: Prior-season actual points.
        injury: ``injuryStatus``; omitted entirely when None, which is what a
            healthy player looks like.
        on_team: Fantasy team id holding them.
    """
    player = {
        "id": player_id,
        "fullName": name,
        "defaultPositionId": 1,
        "eligibleSlots": [position_slot, 20, 21],
        "proTeamId": 1,
        "ownership": {
            "averageDraftPosition": adp_value,
            "auctionValueAverage": auction,
            "percentOwned": 55.5,
            "percentStarted": 40.0,
            "averageDraftPositionPercentChange": -0.1,
        },
        "draftRanksByRankType": {
            "PPR": {"rank": player_id, "auctionValue": 30},
            "STANDARD": {"rank": player_id, "auctionValue": 28},
        },
        # scoringPeriodId 0 is what marks a season row -- espn_api keys
        # ``Player.stats`` on it, and ``stats[0]`` is where the season projection
        # lives. A fixture without it parses to an empty projection.
        "stats": [
            {"seasonId": 2026, "scoringPeriodId": 0, "statSourceId": 1,
             "statSplitTypeId": 0, "appliedTotal": projected,
             "stats": {"24": 100.0, "42": 800.0, "210": 17.0}},
            {"seasonId": 2025, "scoringPeriodId": 0, "statSourceId": 0,
             "statSplitTypeId": 0, "appliedTotal": prior, "stats": {"24": 90.0}},
        ],
    }
    if injury is not None:
        player["injuryStatus"] = injury
    return {"id": player_id, "onTeamId": on_team, "draftAuctionValue": 12,
            "keeperValue": 3, "player": player}


@pytest.fixture
def pool():
    """Projections for a plausible pool: deep RB/WR, shallow QB/TE, plus K and D/ST."""
    rows = []
    for position, count, top in (("QB", 30, 300.0), ("RB", 60, 280.0),
                                 ("WR", 70, 270.0), ("TE", 25, 200.0),
                                 ("K", 20, 140.0), ("D/ST", 20, 130.0),
                                 ("LB", 40, 220.0)):
        for i in range(count):
            rows.append({"primaryPosition": position,
                         "player_name": f"{position}{i + 1}",
                         "player_id": len(rows) + 1,
                         "TRUE_Points": top - i * 4.0})
    return pd.DataFrame(rows)


# --- market parsing ------------------------------------------------------

def test_parses_market_and_projection_from_one_payload():
    entries = [_entry(1, "Best Player", 2, adp_value=1.5, auction=60.0),
               _entry(2, "Second Player", 4, adp_value=3.2, auction=55.0)]
    rows = [adp._parse_entry(e, 2026, "PPR") for e in entries]

    first = rows[0]
    assert first["player_id"] == 1
    assert first["player_name"] == "Best Player"
    assert first["primaryPosition"] == "RB"
    assert first["adp"] == pytest.approx(1.5)
    assert first["auction_value"] == pytest.approx(60.0)
    assert first["espn_draft_rank"] == 1
    assert first["ESPN_projected_total"] == pytest.approx(200.0)
    assert first["prior_season_points"] == pytest.approx(180.0)
    # Stat ids are translated to colNames by espn_api, not left numeric.
    assert "ESPN_rushingYards" in first


def test_per_game_yardage_is_converted_to_a_season_total():
    """ESPN stores these three per-game inside the season row."""
    row = adp._parse_entry(_entry(1, "A Runner", 2), 2026, "PPR")
    # stat 24 is rushingYards: 100/game x 17 games.
    assert row["ESPN_rushingYards"] == pytest.approx(1700.0)
    assert row["games"] == pytest.approx(17.0)


def test_absent_injury_status_does_not_become_an_empty_list():
    """espn_api's json_parsing returns [] for a missing key, not None, which made
    the column mixed-type and failed the parquet write with
    'ArrowTypeError: Expected bytes, got a list object'."""
    healthy = adp._parse_entry(_entry(1, "Healthy Player", 2), 2026, "PPR")
    hurt = adp._parse_entry(_entry(2, "Hurt Player", 2, injury="OUT"), 2026, "PPR")
    assert healthy["injury_status"] is None
    assert hurt["injury_status"] == "OUT"


@pytest.mark.parametrize("raw,expected", [(0, None), (-1, None), (None, None),
                                          (12.5, 12.5)])
def test_sentinel_ownership_values_read_as_missing(raw, expected):
    assert adp._positive_or_none(raw) == expected


def test_market_cache_is_keyed_on_the_league():
    """ADP is market-wide, but the *pool* ESPN returns reflects the league's roster
    slots -- only the IDP league's response carries individual defenders. Sharing
    one cache entry across leagues gave that league a board with no defenders."""
    adp.reset_cache()

    class FakeLeague:
        def __init__(self, league_id):
            self.league_id = league_id
            self.year = 2026
            self.endpoint = "https://example.invalid/?"
            self.cookies = {}

    seen = []
    original = adp._request_player_pool
    adp._request_player_pool = lambda league, **kw: (
        seen.append(league.league_id) or [_entry(1, "Someone", 2, adp_value=5.0)])
    try:
        adp.fetch_draft_market(FakeLeague(111))
        adp.fetch_draft_market(FakeLeague(111))     # cached
        adp.fetch_draft_market(FakeLeague(222))     # different league, refetched
    finally:
        adp._request_player_pool = original
        adp.reset_cache()

    assert seen == [111, 222]


# --- replacement level ---------------------------------------------------

def test_dedicated_slots_are_exact(pool):
    """Twelve teams starting one QB means QB13 is the first non-starter."""
    ranks = bd.replacement_ranks({"QB": 1, "TE": 1}, teams=12, pool=pool)
    assert ranks["QB"] == 12
    assert ranks["TE"] == 12


def test_flex_pushes_replacement_deeper(pool):
    """A standard league: RB/WR replacement must be deeper than dedicated slots
    alone, because the flex is filled from the same pool."""
    slots = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1}
    ranks = bd.replacement_ranks(slots, teams=12, pool=pool)
    assert ranks["RB"] > 24
    assert ranks["RB"] + ranks["WR"] + ranks["TE"] > 12 * (2 + 2 + 1)


def test_superflex_makes_quarterbacks_startable_much_deeper(pool):
    """Weenieless Wanderers' OP slot, and the bug that prompted this test.

    Allocating flex by ADP left QB replacement identical to a one-QB league,
    because global ADP comes overwhelmingly from one-QB leagues -- so Josh Allen
    came out *less* valuable in the superflex league. Allocating by this league's
    own projected points gives the OP slot to quarterbacks, which is what happens
    in a real superflex draft.
    """
    single = bd.replacement_ranks({"QB": 1, "RB": 2, "WR": 2}, teams=10, pool=pool)
    superflex = bd.replacement_ranks({"QB": 1, "RB": 2, "WR": 2, "OP": 1},
                                     teams=10, pool=pool)
    assert single["QB"] == 10
    assert superflex["QB"] == 20


def test_a_position_the_league_never_starts_is_omitted(pool):
    """12 Dudes one Cup has no D/ST slot, so no D/ST is startable there."""
    ranks = bd.replacement_ranks({"QB": 1, "RB": 2, "WR": 2, "K": 1},
                                 teams=12, pool=pool)
    assert "D/ST" not in ranks
    assert "K" in ranks


def test_idp_flex_goes_to_the_position_that_scores_there(pool):
    """GOP Degenerates' DP slot. On tackle-weighted scoring that is linebackers."""
    ranks = bd.replacement_ranks({"QB": 1, "DP": 1}, teams=16, pool=pool)
    assert ranks["LB"] >= 15


def test_even_split_without_a_pool_warns_rather_than_guessing_silently():
    with pytest.warns(bd.DraftBoardWarning, match="divided evenly"):
        ranks = bd.replacement_ranks({"RB/WR/TE": 1}, teams=12, pool=None)
    assert ranks["RB"] == 4          # 12 openings / 3 positions


# --- ADP saturation ------------------------------------------------------

def test_adp_plateau_is_detected():
    """758 of 1,000 real players shared an ADP of exactly 170.0 -- ESPN's filler for
    'undrafted'. Ranking inside it is noise, and comparing it to a projection made
    backup kickers the best values in the league."""
    priced = pd.Series([1.5, 4.2, 30.0, 88.0, 120.0])
    filler = pd.Series([170.0] * 60)
    assert bd.adp_plateau(pd.concat([priced, filler])) == pytest.approx(169.0)


def test_no_plateau_when_every_adp_is_distinct():
    assert bd.adp_plateau(pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])) is None


# --- tiers ---------------------------------------------------------------

def test_tiers_break_at_the_cliff():
    """Two clearly separated groups must not share a tier."""
    points = pd.Series([300.0, 295.0, 290.0, 285.0, 120.0, 115.0, 110.0, 105.0])
    tiers = bd.assign_tiers(points)
    assert tiers.iloc[:4].nunique() == 1
    assert tiers.iloc[4:].nunique() == 1
    assert tiers.iloc[0] < tiers.iloc[-1]


def test_tier_one_is_the_best_players():
    """KMeans labels are arbitrary and must be renumbered."""
    points = pd.Series([300.0, 200.0, 100.0, 50.0] * 5)
    assert bd.assign_tiers(points).loc[points.idxmax()] == 1.0


def test_tiers_survive_a_degenerate_position():
    assert bd.assign_tiers(pd.Series([], dtype=float)).empty
    assert bd.assign_tiers(pd.Series([50.0, 50.0, 50.0])).tolist() == [1.0, 1.0, 1.0]


def test_tier_count_stays_glanceable():
    points = pd.Series(range(300, 0, -1)).astype(float)
    assert bd.MIN_TIERS <= bd.assign_tiers(points).nunique() <= bd.MAX_TIERS


# --- the assembled board -------------------------------------------------

class _League:
    """Minimal stand-in for an ESPN League."""

    def __init__(self, teams, slots):
        self.teams = [object()] * teams
        self.roster_settings = {
            "roster_slots": {**slots, "BE": 5},
            "starting_roster_slots": slots,
        }


def _market_for(pool, adp_start=1.0, priced=40):
    """A market frame matching ``pool``, with only the first N players priced."""
    rows = []
    for i, row in enumerate(pool.itertuples()):
        rows.append({
            "player_id": row.player_id,
            "adp": adp_start + i if i < priced else 170.0,
            "auction_value": 50.0 - i if i < priced else None,
            "espn_auction_value": 5,
            "percent_owned": 50.0,
        })
    return pd.DataFrame(rows)


def test_board_has_the_columns_plan_09_renders(pool):
    league = _League(12, {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1,
                          "K": 1, "D/ST": 1})
    board = bd.build_board(league, pool, _market_for(pool), crosswalk_warn_below=None)
    for column in ("vor", "vor_rank", "pos_rank", "tier", "adp", "adp_rank",
                   "value", "replacement_rank", "startable", "is_streamed",
                   "adp_is_priced", "teams",
                   # ESPN's side of the four comparison groups, and the differences.
                   "espn_pos_rank", "points_delta", "rank_delta", "pos_rank_delta"):
        assert column in board.columns, column
    assert board["teams"].iloc[0] == 12


# --- ESPN's opinion beside ours -------------------------------------------
#
# The sign convention is the load-bearing part: the page paints every difference
# column on one scale, so all of them have to mean the same thing by "positive".

def test_espn_positional_rank_is_dense_within_each_position():
    """A rank of a rank: ESPN's draft ranking re-ranked inside the position, so it is
    ESPN's own ordering of that position rather than a re-ranking of its points."""
    board = pd.DataFrame({
        "primaryPosition": ["RB", "RB", "RB", "WR", "WR"],
        "espn_draft_rank": [40, 2, 17, 8, 3],
        "TRUE_Points": [1.0] * 5, "ESPN_Points": [1.0] * 5,
        "vor_rank": [1.0] * 5, "pos_rank": [1.0] * 5,
    })
    out = bd._attach_espn_comparison(board, "TRUE_Points")
    assert out["espn_pos_rank"].tolist() == [3.0, 1.0, 2.0, 2.0, 1.0]


def test_a_difference_is_positive_where_we_are_higher_on_the_player():
    """One rule for all four groups, because one colour scale serves all of them:
    positive means we like him more than ESPN does. Points subtract ours-minus-theirs
    because more points is better; ranks subtract theirs-minus-ours because a lower
    rank is better."""
    board = pd.DataFrame({
        "primaryPosition": ["RB", "RB"],
        "player_name": ["We Like Him", "ESPN Likes Him"],
        # We project more, and rank him better, than ESPN on the first row.
        "TRUE_Points": [200.0, 100.0],
        "ESPN_Points": [150.0, 180.0],
        "espn_draft_rank": [30, 4],
        "vor_rank": [5.0, 25.0],
        "pos_rank": [1.0, 9.0],
    })
    out = bd._attach_espn_comparison(board, "TRUE_Points")

    liked = out.iloc[0]
    assert liked["points_delta"] > 0 and liked["rank_delta"] > 0
    assert liked["pos_rank_delta"] > 0
    faded = out.iloc[1]
    assert faded["points_delta"] < 0 and faded["rank_delta"] < 0
    assert faded["pos_rank_delta"] < 0


def test_a_board_without_espn_columns_still_gets_all_four(pool):
    """Boards are read by a page that decides what to render from what is present, so
    a missing input has to mean an empty column rather than an absent one."""
    board = pd.DataFrame({
        "primaryPosition": ["RB"], "TRUE_Points": [100.0],
        "vor_rank": [1.0], "pos_rank": [1.0],
    })
    out = bd._attach_espn_comparison(board, "TRUE_Points")
    for column in ("espn_pos_rank", "points_delta", "rank_delta", "pos_rank_delta"):
        assert column in out.columns, column
        assert out[column].isna().all(), column


def test_value_is_positive_when_the_room_lets_a_player_fall(pool):
    """The sign convention: ADP later than our valuation is positive value."""
    league = _League(12, {"QB": 1, "RB": 2, "WR": 2, "TE": 1})
    market = _market_for(pool, priced=60)
    # Push the best RB's ADP far down the board without touching his projection.
    market.loc[market["player_id"] == pool.loc[
        pool["primaryPosition"] == "RB", "player_id"].iloc[0], "adp"] = 55.0

    board = bd.build_board(league, pool, market, crosswalk_warn_below=None)
    fallen = board[board["player_name"] == "RB1"].iloc[0]
    assert fallen["value"] > 0


def test_streamed_positions_are_excluded_from_value(pool):
    """Naive VOR overvalues K and D/ST because it assumes you hold one all season.
    Left in, the eight best 'values' in the league were all team defences."""
    league = _League(12, {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "K": 1, "D/ST": 1})
    board = bd.build_board(league, pool, _market_for(pool, priced=200),
                           crosswalk_warn_below=None)

    streamed = board[board["primaryPosition"].isin(["K", "D/ST"])]
    assert streamed["is_streamed"].all()
    assert streamed["value"].isna().all()
    # But their VOR is still computed, so nothing is hidden.
    assert streamed["vor"].notna().any()


def test_value_is_withheld_where_the_market_set_no_price(pool):
    league = _League(12, {"QB": 1, "RB": 2, "WR": 2, "TE": 1})
    board = bd.build_board(league, pool, _market_for(pool, priced=20),
                           crosswalk_warn_below=None)
    assert board.loc[~board["adp_is_priced"], "value"].isna().all()


def test_same_player_ranks_differently_across_league_shapes(pool):
    """The whole reason to build a board instead of reading one off a website."""
    market = _market_for(pool)
    standard = bd.build_board(_League(12, {"QB": 1, "RB": 2, "WR": 2, "TE": 1}),
                              pool, market, crosswalk_warn_below=None)
    superflex = bd.build_board(
        _League(12, {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "OP": 1}), pool, market,
        crosswalk_warn_below=None)

    def vor_of(board, name):
        return board.loc[board["player_name"] == name, "vor"].iloc[0]

    assert vor_of(superflex, "QB1") > vor_of(standard, "QB1")


def test_board_requires_a_player_id_to_join_on(pool):
    league = _League(12, {"QB": 1})
    with pytest.raises(KeyError, match="player_id"):
        bd.build_board(league, pool.drop(columns=["player_id"]), _market_for(pool),
                       crosswalk_warn_below=None)


# --- bye weeks -----------------------------------------------------------

def _schedule(season=2026, weeks=18, teams=("AAA", "BBB", "LA", "WAS")):
    """A schedule where every team plays every week except one."""
    import polars as pl
    rows = []
    for week in range(1, weeks + 1):
        for i in range(0, len(teams), 2):
            home, away = teams[i], teams[i + 1]
            # AAA/BBB sit out week 5, LA/WAS week 9.
            if week == (5 if i == 0 else 9):
                continue
            rows.append({"season": season, "week": week,
                         "home_team": home, "away_team": away})
    return pl.DataFrame(rows)


def test_bye_is_derived_as_the_week_a_team_does_not_appear(monkeypatch):
    monkeypatch.setattr(bd, "load_schedule", lambda: _schedule())
    byes = bd.bye_weeks(2026)
    assert byes["AAA"] == 5 and byes["BBB"] == 5
    assert byes["LA"] == 9 and byes["WAS"] == 9


def test_espn_spellings_are_aliased_onto_the_schedules(monkeypatch):
    """ESPN says LAR and WSH; the schedule says LA and WAS."""
    monkeypatch.setattr(bd, "load_schedule", lambda: _schedule())
    byes = bd.bye_weeks(2026)
    assert byes["LAR"] == byes["LA"]
    assert byes["WSH"] == byes["WAS"]


def test_a_schedule_for_another_season_yields_no_byes(monkeypatch):
    """Better an absent bye than another season's, on a draft board."""
    monkeypatch.setattr(bd, "load_schedule", lambda: _schedule(season=2025))
    with pytest.warns(bd.DraftBoardWarning, match="not 2026"):
        assert bd.bye_weeks(2026) == {}


def test_a_partial_schedule_yields_no_bye_for_that_team(monkeypatch):
    """Two missing weeks is an incomplete pull, not two byes."""
    import polars as pl
    sched = _schedule().filter(~((pl.col("home_team") == "AAA") &
                                (pl.col("week") == 12)))
    monkeypatch.setattr(bd, "load_schedule", lambda: sched)
    byes = bd.bye_weeks(2026)
    assert "AAA" not in byes and "BBB" not in byes


def test_board_carries_a_bye_week_column(pool, monkeypatch):
    monkeypatch.setattr(bd, "load_schedule", lambda: _schedule())
    league = _League(12, {"QB": 1, "RB": 2, "WR": 2, "TE": 1})
    projections = pool.copy()
    projections["pro_team"] = ["AAA"] * len(projections)
    board = bd.build_board(league, projections, _market_for(pool),
                           crosswalk_warn_below=None, season=2026)
    assert (board["bye_week"] == 5).all()


def test_a_board_without_pro_team_still_builds(pool, monkeypatch):
    """The bye join is a nicety; a board is still a board without it."""
    monkeypatch.setattr(bd, "load_schedule", lambda: _schedule())
    league = _League(12, {"QB": 1, "RB": 2, "WR": 2, "TE": 1})
    board = bd.build_board(league, pool, _market_for(pool),
                           crosswalk_warn_below=None, season=2026)
    assert board["bye_week"].isna().all()


# --- projection_missing --------------------------------------------------

def test_projection_missing_catches_a_zero_projection(pool):
    """It used to be `TRUE_Points.isna()`, which the 0-filling blend never trips.

    Measured on the live 2026 boards: False for all 1,026 rows in every league,
    including 504 players projected a literal 0.0, two of them priced by the
    market.
    """
    league = _League(12, {"QB": 1, "RB": 2, "WR": 2, "TE": 1})
    projections = pool.copy()
    # A player no source has a line for: the blend 0-fills, so TRUE_Points is
    # 0.0 rather than null, and every source's scored total is NaN.
    projections.loc[0, "TRUE_Points"] = 0.0
    for prefix in bd.PROJECTION_PREFIXES:
        projections[f"{prefix}_Points"] = 50.0
        projections.loc[0, f"{prefix}_Points"] = float("nan")

    board = bd.build_board(league, projections, _market_for(pool),
                           crosswalk_warn_below=None)
    flagged = board.loc[board["player_id"] == pool.loc[0, "player_id"],
                        "projection_missing"]
    assert bool(flagged.iloc[0])
    assert int(board["projection_missing"].sum()) == 1
