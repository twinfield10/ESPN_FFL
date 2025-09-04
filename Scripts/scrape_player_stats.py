from typing import List
import pandas as pd
from espn_api.football import League, Team, Player
from espn_api.requests.constant import FANTASY_BASE_ENDPOINT
from Scripts.fetch_utils import fetch_league

def build_scoring_table(league: League):

    # League Scoring DataFrame
    league_scoring = pd.DataFrame(league.settings.scoring_format)

    # Filter Stuffs + FGs 60+
    league_scoring = league_scoring[~league_scoring['id'].isin([201, 206, 209])]

    # Convert "Every" Stats To Decimals
    repl_scoring = {
        8: {'abbr': 'PY','label': 'Passing Yards','id': 3,'points': 0.04},
        28:{'abbr': 'RY','label': 'Rushing Yards','id': 24,'points': 0.1},
        48:{'abbr': 'REY','label': 'Receiving Yards','id': 42,'points': 0.1}
    }

    for key, changes in repl_scoring.items():
        league_scoring.loc[league_scoring['id'] == key, ['abbr', 'label', 'id', 'points']] = [
            changes['abbr'], changes['label'], changes['id'], changes['points']
        ]

    score_to_lab_dict = {
        1: 'passingCompletions',
        3: 'passingYards',
        4: 'passingTouchdowns',
        19: 'passing2PtConversions',
        20: 'passingInterceptions',
        23: 'rushingAttempts',
        24: 'rushingYards',
        25: 'rushingTouchdowns',
        26: 'rushing2PtConversions',
        42: 'receivingYards',
        43: 'receivingTouchdowns',
        44: 'receiving2PtConversions',
        53: 'receivingReceptions',
        63: 'fumbleRecoveredForTD',
        72: 'fumbles',
        77: 'madeFieldGoalsFrom40To49',
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

    return scores_df


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
    # Fetch league for year
    league = fetch_league(league_id=league_id,
                    year=year,
                    espn_s2=espn_s2,
                    swid=swid)
    
    score_settings = build_scoring_table(league=league)


    # Instantiate data frame
    df = pd.DataFrame()

    # Loop through each week that has happened
    current_matchup_period = league.settings.week_to_matchup_period[league.current_week]

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
    
    score_settings = build_scoring_table(league=league)
    
    fa = extract_fa_stats(team_lineup = fa_list, score_cols=list(score_settings['colName']), league=league)

    return fa

    