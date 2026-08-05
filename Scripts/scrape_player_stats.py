from typing import List
import warnings

import pandas as pd
from espn_api.football import League, Team, Player
from espn_api.requests.constant import FANTASY_BASE_ENDPOINT
from Scripts.fetch_utils import fetch_league


class ScoringCoverageWarning(UserWarning):
    """A league scores a stat this pipeline cannot model.

    Raised as a warning rather than an error because the resulting table is
    still usable -- it is just missing one rule's contribution.
    """


class UnmappedScoringRuleError(ValueError):
    """Strict-mode counterpart to :class:`ScoringCoverageWarning`."""


# Scoring rules ESPN expresses as "every N yards" rather than as a rate on the
# underlying yardage stat. Each is rewritten onto the stat it actually counts,
# with an equivalent per-yard rate, because ``proj_to_score`` can only multiply
# a stat column by a constant.
REPL_SCORING = {
    8: {'abbr': 'PY', 'label': 'Passing Yards', 'id': 3, 'points': 0.04},
    27: {'abbr': 'RY', 'label': 'Rushing Yards', 'id': 24, 'points': 0.1},
    28: {'abbr': 'RY', 'label': 'Rushing Yards', 'id': 24, 'points': 0.1},
    47: {'abbr': 'REY', 'label': 'Receiving Yards', 'id': 42, 'points': 0.1},
    48: {'abbr': 'REY', 'label': 'Receiving Yards', 'id': 42, 'points': 0.1},
    # FGY50 ("Every 50 FG Made yards"), new to GOP_Degenerates in 2026, where it
    # replaced the flat 0.1/yd rule on stat 214.
    #
    # The rate here is NOT the naive 5.0 / 50 = 0.1. ESPN awards this per game,
    # on the floor of the yardage: stat 221 == floor(stat 214 / 50) held on
    # 14/14 sampled player-weeks. The sub-50 remainder is discarded every game,
    # so the realised per-yard rate is well below 0.1. Measured across the 21
    # kickers with >=300 FG made yards in 2025: 0.0642 pts/yd.
    #
    # A single linear rate cannot capture a floor exactly -- high-volume kickers
    # waste proportionally less remainder (0.073/yd) than low-volume ones
    # (0.050/yd) -- but it is unbiased at the observed yardage distribution,
    # where 0.1/yd overstates a starting kicker by ~2.4 pts/week.
    221: {'abbr': 'FGY', 'label': 'FG Yards', 'id': 214, 'points': 0.064},
}

# Rules deliberately excluded: FG 60+ (201), 2-pt return (206), 1-pt safety
# (209). These are scored by ESPN but not modelled here.
IGNORED_SCORING_IDS = [201, 206, 209]


def build_scoring_table(league: League, strict: bool = False) -> pd.DataFrame:
    """Translate a league's ESPN scoring settings into stat->points rows.

    Args:
        league: ESPN ``League``. Reads ``league.settings.scoring_format``.
        strict: Raise instead of warning when a scoring rule cannot be mapped
            to a stat column.

    Returns:
        pd.DataFrame: One row per scoring rule with ``id``, ``abbr``, ``label``,
        ``points``, ``source_id`` and ``colName``. ``colName`` is the key to look
        up in a player's ``points_breakdown`` / ``projected_breakdown``. ``id`` is
        the stat the rule is scored against, which for an "every N yards" rule is
        not the id the commissioner configured -- ``source_id`` keeps that, for
        auditing.

    Raises:
        UnmappedScoringRuleError: Only when ``strict=True`` and the league
            scores a stat with no ``colName``.

    Warns:
        ScoringCoverageWarning: The league scores a stat with no ``colName``,
            so that rule contributes nothing to projections. This used to pass
            silently: an unrecognised id produced a NaN ``colName``, which
            ``proj_to_score`` turned into the column name ``"TRUE_nan"``, found
            missing, and skipped -- yielding normal-looking but wrong points.
    """

    # League Scoring DataFrame
    league_scoring = pd.DataFrame(league.settings.scoring_format)

    # Filter Stuffs + FGs 60+
    league_scoring = league_scoring[~league_scoring['id'].isin(IGNORED_SCORING_IDS)]

    # Preserve the commissioner-configured rule id before REPL_SCORING rewrites
    # `id` to the stat the rule actually counts. Without this an audit cannot
    # distinguish "rule 214 was repriced" from "rule 214 was replaced by rule
    # 221, and we chose to model it at 0.064/yd" -- the second is a scoring
    # change, the first is not.
    league_scoring = league_scoring.assign(source_id=league_scoring['id'])

    # Convert "Every" Stats To Decimals
    for key, changes in REPL_SCORING.items():
        league_scoring.loc[league_scoring['id'] == key, ['abbr', 'label', 'id', 'points']] = [
            changes['abbr'], changes['label'], changes['id'], changes['points']
        ]

    score_to_lab_dict = {
        1: 'passingCompletions',
        3: 'passingYards',
        4: 'passingTouchdowns',
        17: 'passingYards300to399Game',
        18: 'passingYards400PlusGame',
        19: 'passing2PtConversions',
        20: 'passingInterceptions',
        23: 'rushingAttempts',
        24: 'rushingYards',
        25: 'rushingTouchdowns',
        26: 'rushing2PtConversions',
        37: 'rushingYards100-199Game',
        38: 'rushingYards200+Game',
        42: 'receivingYards',
        43: 'receivingTouchdowns',
        44: 'receiving2PtConversions',
        53: 'receivingReceptions',
        56: 'receivingYards100-199Game',
        57: 'receivingYards200+Game',
        63: 'fumbleRecoveredForTD',
        72: 'fumbles',
        # ESPN's older id for FG made 50+; 198 below is the modern one. Scored by
        # Winfield_Football 2016-2019 and Weenieless_Wanderers 2017-2019 at
        # 5.0 pts, and silently dropped from those seasons until now. No
        # league-season scores both 74 and 198, so this cannot double-count.
        74: 'madeFieldGoalsFrom50Plus',
        77: 'madeFieldGoalsFrom40To49',
        # FGM40, new to GOP_Degenerates in 2026.
        79: 'missedFieldGoalsFrom40To49',
        80: 'madeFieldGoalsFromUnder40',
        82: 'missedFieldGoalsFromUnder40',
        85: 'missedFieldGoals',
        86: 'madeExtraPoints',
        88: 'missedExtraPoints',
        89: 'defensive0PointsAllowed',
        90: 'defensive1To6PointsAllowed',
        91: 'defensive7To13PointsAllowed',
        92: 'defensive14To17PointsAllowed',
        93: 'defensiveBlockedKickTD',
        95: 'defensiveInterceptions',
        96: 'defensiveFumbles',
        97: 'blockedKick',
        98: 'defensiveSafeties',
        99: 'defensiveSacks',
        109: 'defensiveTotalTackles',
        101: 'kickoffReturnTouchdowns',
        102: 'puntReturnTouchdowns',
        103: 'interceptionReturnTouchdowns',
        104: 'fumbleReturnTouchdowns',
        106: 'defensiveForcedFumbles',
        107: 'defensiveAssistedTackles',
        108: 'defensiveSoloTackles',
        111: '111',
        112: 'defensiveStuffs',
        113: 'defensivePassesDefensed',
        114: 'kickoffReturnYards',
        115: 'puntReturnYards',
        121: 'defensive18To21PointsAllowed',
        122: 'defensive22To27PointsAllowed',
        123: 'defensive28To34PointsAllowed',
        124: 'defensive35To45PointsAllowed',
        125: 'defensive45PlusPointsAllowed',
        128: 'defensive0To99YardsAllowed',
        129: 'defensive100To199YardsAllowed',
        130: 'defensive200To299YardsAllowed',
        131: 'defensive300To349YardsAllowed',
        132: 'defensive350To399YardsAllowed',
        133: 'defensive400To449YardsAllowed',
        134: 'defensive450To499YardsAllowed',
        135: 'defensive500To549YardsAllowed',
        136: 'defensive550PlusYardsAllowed',
        198: 'madeFieldGoalsFrom50Plus',
        210: '210',
        214: '214'
    }

    score_df = pd.DataFrame(score_to_lab_dict.items(), columns=['id', 'colName'])


    scores_df = league_scoring.merge(score_df, on='id', how='left')
    scores_df = scores_df.sort_values(['id'])

    _check_scoring_coverage(league, scores_df, strict=strict)

    return scores_df


def _check_scoring_coverage(
    league: League, scores_df: pd.DataFrame, strict: bool = False
) -> pd.DataFrame:
    """Flag scoring rules that will contribute nothing to projections.

    A rule with a NaN ``colName`` and non-zero ``points`` is a rule the league
    scores and this pipeline drops. Rules worth zero points are ignored -- they
    are inert either way, and every league carries a long tail of them.

    Args:
        league: The league the table was built from, for the message.
        scores_df: Output of :func:`build_scoring_table` before returning.
        strict: Raise rather than warn.

    Returns:
        pd.DataFrame: The offending rows, empty when coverage is complete.

    Raises:
        UnmappedScoringRuleError: When ``strict`` and there are offending rows.
    """
    unmapped = scores_df[scores_df['colName'].isna() & (scores_df['points'] != 0)]
    if unmapped.empty:
        return unmapped

    rules = ", ".join(
        f"id={int(row.id)} {row.abbr} ({row.label!r}) pts={row.points}"
        for row in unmapped.itertuples()
    )
    msg = (
        f"{getattr(league, 'name', league.league_id)} {league.year}: "
        f"{len(unmapped)} scoring rule(s) are not modelled, so they contribute "
        f"nothing to projections: {rules}. Map the stat id in "
        f"score_to_lab_dict (or REPL_SCORING, for an 'every N yards' rule) in "
        f"Scripts/scrape_player_stats.py, or add it to IGNORED_SCORING_IDS if "
        f"it genuinely cannot be modelled."
    )

    if strict:
        raise UnmappedScoringRuleError(msg)

    # Emitted inside catch_warnings so that Scripts/fetch_utils.py's module-level
    # warnings.filterwarnings("ignore") cannot swallow it. Filtering here rather
    # than narrowing that global filter keeps this independent of import order,
    # and of docs/plans/06-performance.md, which removes it properly.
    with warnings.catch_warnings():
        warnings.simplefilter("always", ScoringCoverageWarning)
        warnings.warn(msg, ScoringCoverageWarning, stacklevel=3)

    return unmapped


def extract_player_stats(
    team: Team, team_lineup: List[Player], week: int, score_cols: List[None], curr_week: int
) -> pd.DataFrame:
    df = pd.DataFrame()
    for i, player in enumerate(team_lineup):
        pp_bd = player.__dict__['projected_breakdown']
        p_bd = player.__dict__['points_breakdown']

        if player.active_status == 'bye' and pp_bd != {}:
            status = 'active'
        else:
            status = player.active_status

        player_data = {
            "week": week,
            "team_owner": team.owner,
            "team_name": team.team_name,
            "team_division": team.division_name,
            "player_name": player.name,
            "player_id": player.playerId,
            "points": player.__dict__['points'],
            "projPoints": player.__dict__['projected_points'],
            "slotPosition": player.lineupSlot,
            "primaryPosition": player.position,
            "eligiblePositions": player.eligibleSlots,
            "pro_team": player.proTeam,
            "current_team_id": player.onTeamId,
            "player_position": player.position,
            "player_active_status": status
        }


        bd_stats = score_cols

        if status != "bye":
            for bd_stat in bd_stats:
                # Actual
                try:
                    player_data[bd_stat] = p_bd.get(bd_stat)
                except:
                    player_data[bd_stat] = 0
                # Projections    
                try:
                    player_data[f"proj_{bd_stat}"] = pp_bd.get(bd_stat)
                except:
                    player_data[f"proj_{bd_stat}"] = 0
                

        if 0 in player.stats.keys():
            player_data["player_points_season"] = player.stats[0]["points"]
        else:
            player_data["player_points_season"] = 0

        df = pd.concat([df, pd.DataFrame([player_data])], ignore_index=True)
        

    return df


def get_ply_stats_by_matchup(
    league_id: int, year: int, swid: str, espn_s2: str
) -> pd.DataFrame:
    """This function creates a historical dataframe for the league in a given year.
    The data is based on player-level stats, and is organized by week and matchup.

    It generates this dataframe by:
        - For each week that has elapsed, get the BoxScores for that week:
            - For each Matchup in the BoxScores:
                Grab each stat by looking at the Matchup.home_team, Matchup.home_lineup, Matchup.away_team, and Matchup.away_lineup

    This is used for years in 2019 or later, where the BoxScores are available.

    Args:
        league_id (int): League ID
        year (int): Year of the league
        swid (str): User credential
        espn_s2 (str): User credential

    Returns:
        pd.DataFrame: Historical player stats dataframe
    """
    # Deferred: Scripts.scoring imports this module, so a top-level import cycles.
    from Scripts.scoring import get_scoring_table

    # Fetch league for year
    league = fetch_league(league_id=league_id,
                    year=year,
                    espn_s2=espn_s2,
                    swid=swid)

    score_settings = get_scoring_table(league)


    # Instantiate data frame
    df = pd.DataFrame()

    # Loop through each week that has happened
    current_matchup_period = league.settings.week_to_matchup_period[league.current_week]
    if league.league_id == 521152 and league.current_week in [15, 17]:
        current_matchup_period = league.current_week
        
    print(f"Current Week: {league.current_week}")
    print(f"Current Matchup Period: {current_matchup_period}")

    for week in range(current_matchup_period):
        league.load_roster_week(week + 1)
        box_scores = league.box_scores(week + 1)

        # Instantiate week data frame
        df_week = pd.DataFrame()
        for i, matchup in enumerate(box_scores):
            # Skip byes
            if (type(matchup.home_team) != Team) or (type(matchup.away_team) != Team):
                continue

            # Get stats for home team
            df_home_team = extract_player_stats(
                matchup.home_team, matchup.home_lineup, week + 1, score_cols=score_settings['colName'].to_list(), curr_week=current_matchup_period
            )

            # Get stats for away team
            df_away_team = extract_player_stats(
                matchup.away_team, matchup.away_lineup, week + 1, score_cols=score_settings['colName'].to_list(), curr_week=current_matchup_period
            )

            # Append to week data frame
            df_week = pd.concat([df_week, pd.DataFrame(df_home_team)])
            df_week = pd.concat([df_week, pd.DataFrame(df_away_team)])

        df = pd.concat([df, pd.DataFrame(df_week)])

    df["league_id"] = league_id
    df["year"] = year

    return df

def get_free_agent_stats(
        league: League, 
) -> pd.DataFrame:
    
    # Build FA Market
    def build_fa_market(qbs=10, rbs=20, wrs=20, tes=10, ks=10):
        return [league.free_agents(size = qbs, position = 'QB') +
                league.free_agents(size = rbs, position = 'RB') +
                league.free_agents(size = wrs, position = 'WR') +
                league.free_agents(size = tes, position = 'TE') +
                league.free_agents(size = ks, position = 'K') +
                league.free_agents(position = 'D/ST')]
    
    fas = build_fa_market()

    fa_df = extract_player_stats(None, fas, week=league.currentMatchupPeriod)

    return fa_df

def extract_fa_stats(
    team_lineup: List[Player], score_cols: List[None], league: League
) -> pd.DataFrame:
    df = pd.DataFrame()
    for i, player in enumerate(team_lineup):
        pp_bd = player.__dict__['projected_breakdown']
        p_bd = player.__dict__['points_breakdown']

        if player.active_status == 'bye' and pp_bd != {}:
            status = 'active'
        else:
            status = player.active_status

        player_data = {
            "week": league.current_week,
            "team_owner": 'Free Agent',
            "team_name": 'Free Agent',
            "team_division": 'Free Agent',
            "player_name": player.name,
            "player_id": player.playerId,
            "points": player.__dict__['points'],
            "projPoints": player.__dict__['projected_points'],
            "slotPosition": player.position,
            "primaryPosition": player.position,
            "eligiblePositions": player.eligibleSlots,
            "pro_team": player.proTeam,
            "current_team_id": player.onTeamId,
            "player_position": player.position,
            "player_active_status": status
        }


        bd_stats = score_cols


        if status != "bye":
            for bd_stat in bd_stats:
                # Actual
                try:
                    player_data[bd_stat] = p_bd.get(bd_stat)
                except:
                    player_data[bd_stat] = 0
                # Projections    
                try:
                    player_data[f"proj_{bd_stat}"] = pp_bd.get(bd_stat)
                except:
                    player_data[f"proj_{bd_stat}"] = 0
                

        if 0 in player.stats.keys():
            player_data["player_points_season"] = player.stats[0]["points"]
        else:
            player_data["player_points_season"] = 0

        df = pd.concat([df, pd.DataFrame([player_data])], ignore_index=True)

        df["league_id"] = league.league_id
        df["year"] = league.year
        

    return df

def build_fa_market(league:League, qbs=20, rbs=30, wrs=30, tes=20, ks=20):
    fa_list = [league.free_agents(size = qbs, position = 'QB') +
               league.free_agents(size = rbs, position = 'RB') +
               league.free_agents(size = wrs, position = 'WR') +
               league.free_agents(size = tes, position = 'TE') +
               league.free_agents(size = ks, position = 'K') +
               league.free_agents(size = wrs, position = 'DT') +
               league.free_agents(size = wrs, position = 'CB') +
               league.free_agents(size = wrs, position = 'S') +
               league.free_agents(size = wrs, position = 'LB') +
               league.free_agents(size = wrs, position = 'DE') +
               league.free_agents(position = 'D/ST')][0]
    
    # Deferred: Scripts.scoring imports this module, so a top-level import cycles.
    from Scripts.scoring import get_scoring_table

    score_settings = get_scoring_table(league)

    fa = extract_fa_stats(team_lineup = fa_list, score_cols=list(score_settings['colName']), league=league)

    return fa

    