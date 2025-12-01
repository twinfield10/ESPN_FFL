# Base
import time
import datetime

# Data Manipulation
import pandas as pd
import polars as pl
import numpy as np

# Modeling
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

# Scripts
from Scripts.fetch_utils import fetch_league
from Scripts.analytic_utils import *
from Scripts.scrape_player_stats import *
from Scripts.scrape_team_stats import *
from Scripts.luck_index import *
from Scripts.tidbit_utils import *
from Scripts.simulation_utils import *

# Google Sheet
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

# Config Leagues
import yaml

# Load the YAML file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Access your data
lg_vars = {}
for league_name, league_data in config['leagues'].items():
    # Convert snake_case back to your original naming
    original_name = {
        'winfield_football': 'Winfield_Football',
        'weenieless_wanderers': 'Weenieless_Wanderers', 
        'gop_degenerates': 'GOP_Degenerates',
        'knights_ffl': 'Knights_FFL',
        'twelve_dudes_one_cup': '12 Dudes one Cup',
        'big_red_fantasy_football': 'Big Red Fantasy Football',
        'john_pc_league': 'John_PC_League',
        'john_atl_league': 'John_ATL_League'
    }[league_name]
    
    # Resolve references and convert to your expected format
    espn_s2 = league_data.get('espn_s2') or config['credentials']['espn_id']
    swid = league_data.get('swid') or config['credentials']['s_id']
    
    lg_vars[original_name] = {
        'ID': league_data['id'],
        'ESPN_S2': espn_s2,
        'SWID': swid,
        'start': league_data['start'],
        'end': league_data['end'],
        'primary_own': league_data['primary_owner']
    }

# Load Tackle Split
TKLS_DIM = pd.read_csv('Data/NFL_Tackles_By_Position.csv')

def change_col_prefix(df, old_pfix, new_pfix):

    df = df
    df.columns = df.columns.str.replace(f'{old_pfix}', new_pfix, regex=False)
    return df

def impute_columns(df, target_prefix, source_prefix):
    target_cols = [col for col in df.columns if col.startswith(target_prefix)]
    source_cols = [col for col in df.columns if col.startswith(source_prefix)]

    for source_col in source_cols:
        # Define the corresponding target column name
        target_col = target_prefix + source_col[len(source_prefix):]
        
        # If the target column does not exist, create it by copying the values from the source column
        if target_col not in df.columns:
            df[target_col] = df[source_col]
        
        # If the target column exists, impute missing values from the source column
        elif source_col in df.columns:
            df[target_col] = df[target_col].fillna(df[source_col])
    
    return df

def create_mean_cols(df, target_prefix, source_prefix, mean_prefix='MEAN_'):
    target_cols = [col for col in df.columns if col.startswith(target_prefix)]
    source_cols = [col for col in df.columns if col.startswith(source_prefix)]

    for source_col in source_cols:
        target_col = target_prefix + source_col[len(source_prefix):]
        mean_col = mean_prefix + source_col[len(source_prefix):]

        if target_col in df.columns and source_col in df.columns:
            df[mean_col] = df[[target_col, source_col]].mean(axis=1)

    df = df[['week', 'player_name', 'primaryPosition','player_active_status']  + list(df.filter(like='MEAN_').columns)]

    return df

def clean_pinny(pinny_path="Data/Projections/Pinnacle/Season/Pinnacle_Props_Week_All.parquet"):

    # Constants
    prop_to_stat={
        "Touchdowns": 'rushingTouchdowns',
        "Rushing Yards": "rushingYards",
        "Rush Attempts": 'rushingAttempts',

        "Receiving Yards": "receivingYards",
        "Receptions": "receivingReceptions",

        "Touchdown Passes": "passingTouchdowns",
        "Pass Completions": "passingCompletions",
        "Pass Attempts": "passingAttempts",
        "Passing Yards": "passingYards",
        "Interceptions": "passingInterceptions"
    }

    name_changes={
        # Pinny -> ESPN #
        "Tre Harris": "Tre' Harris",
        "Marvin Mims": "Marvin Mims Jr.",
        "Travis Etienne": "Travis Etienne Jr.",
        "Aaron Jones": "Aaron Jones Sr.",
        "Kyle Pitts": "Kyle Pitts Sr.",
        "Calvin Austin": "Calvin Austin III",
        "Ollie Gordon":"Ollie Gordon II",
        "Marvin Harrison": "Marvin Harrison Jr.",
        "Kyle Pitts": "Kyle Pitts Sr.",
        "Marvin Mims": "Marvin Mims Jr.",
        "Travis Etienne": "Travis Etienne Jr.",
        "Aaron Jones": "Aaron Jones Sr."
    }

    # Load
    raw=pd.read_parquet(pinny_path)

    # Get Stats
    #raw.replace({"PropType": prop_to_stat}, inplace=True)

    # Filter
    #raw = raw[raw['PropType'].isin(list(prop_to_stat.values()))]

    # Clean Names
    raw.replace({"player_name": name_changes}, inplace=True)

    # Replace NaN in Value with ImpNoVig
    #raw['Value'] = raw['Value'].fillna(raw['ImpNoVig'])

    def adjust_value(df):
        pivoted_df = df.pivot_table(
            index=['officialDate', 'week', 'Away', 'Home', 'Player', 'PropType', 'Value', 'BetTimeStamp'],
            columns='OverUnder',
            values=['Price', 'Implied', 'ImpNoVig'],
            aggfunc='first'  # Use 'first' in case there are duplicates
        ).reset_index()

        # Flatten the column names and add OverUnder values as suffixes
        pivoted_df.columns.name = None
        new_columns = []

        for col in pivoted_df.columns:
            if col[0] in ['Price', 'Implied', 'ImpNoVig']:
                new_columns.append(f"{col[0]}_{col[1]}")
            else:
                new_columns.append(col[0])

        pivoted_df.columns = new_columns

        # Create Adjusted Values From Juice
        pivoted_df["Juice"] = pivoted_df['Implied_Over'] + pivoted_df['Implied_Under']
        pivoted_df["Over_Juice"] = (1 / pivoted_df['Implied_Over'] - 1)
        pivoted_df["Under_Juice"] = (1 / pivoted_df['Implied_Under'] - 1)
        pivoted_df["Juice_Diff"] = pivoted_df["Under_Juice"] - pivoted_df["Over_Juice"]
        pivoted_df["AdjValue"] = pivoted_df["Value"] + (pivoted_df["Juice_Diff"] * pivoted_df["Value"] * 0.5)

        return pivoted_df
    
    #adjusted = adjust_value(raw)


    #adjusted = adjusted[['week', 'Player', 'PropType', 'AdjValue']]
    #adjusted.columns = ['week', 'player_name', 'statType', 'statValue']

    # Pivot
    #clean = adjusted.pivot_table(index=['week','player_name'], columns='statType', values='statValue', aggfunc='mean').reset_index()

    

    # Split Touchdowns by Usage (Rushing/Receiving)
    #if 'rushingTouchdowns' in clean.columns:
    #    clean['receivingTouchdowns'] = clean['rushingTouchdowns'] * (clean['receivingYards'] / (clean['receivingYards'] + clean['rushingYards']))
    #    clean['rushingTouchdowns'] = clean['rushingTouchdowns'] - clean['receivingTouchdowns']

    #clean.columns = ['week', 'player_name'] + ['proj_' + str(col) for col in clean.columns[2:]]
    #clean = clean[['week', 'player_name'] + list(clean.columns[2:])]

    # Flatten If Needed
    #clean.columns.name = None
    #clean.columns = [col if col is not None else 'StatValue' for col in clean.columns]

    #final = proj_to_score(proj_df=clean, col_pfix="PINNY")

    return raw

def clean_bol(bol_path = "Data/Projections/BetOnline/Season/BetOnline_AllProps.parquet"):

    # Load
    raw = pd.read_parquet(bol_path).drop(columns=['team'])

    name_changes={
        # BOL -> ESPN #
        "Tre Harris": "Tre' Harris",
        "Kyle Pitts": "Kyle Pitts Sr.",
        "Deebo Samuel Sr.":"Deebo Samuel",
        "Cameron Ward":"Cam Ward",
        "Marquise Brown":"Hollywood Brown",
        "Ray-Ray McCloud": "Ray-Ray McCloud III",
        "Chris Godwin": "Chris Godwin Jr.",
        "Anthony Richardson": "Anthony Richardson Sr.",
        "Oronde Gadsden": "Oronde Gadsden II",
        "James Cook": "James Cook III"
    }

    if 'proj_defensiveTotalTackles' in raw.columns:
        raw = raw.merge(TKLS_DIM, left_on="position", right_on="pos", how="left")
        raw['proj_defensiveAssistedTackles'] = raw['proj_defensiveTotalTackles'] / (raw['tackle_ratio'] + 0.5)
        raw['proj_defensiveSoloTackles'] = raw['tackle_ratio'] * raw['proj_defensiveAssistedTackles']

    # Join Tackle DataFrame
    raw = raw.drop(columns=['position', 'pos'])

    raw.replace({"player_name": name_changes}, inplace=True)

    return raw

def get_match_details(df1, df2, keys, check_col2, tbl_lab, min_wk):

    # Only Check Weeks that Exist in Data w/ Projected Stats
    df1 = df1[((df1['week'] == min_wk) & (~df1['primaryPosition'].isin(['D/ST', 'K', 'DL', 'DE', 'LB', 'NT', 'CB', 'S', 'DT', 'DB', 'OLB'])))]
    df1 = df1[df1.filter(like='MEAN_').sum(axis=1) > 0]
    df1 = df1[df1['player_active_status'] == 'active']

    # Option 1 - Count unmatched values
    merged_df = pd.merge(df1, df2, on=keys, how='left')
    unmatched_from_df2 = merged_df[check_col2].isnull().sum()
    
    # If > 0, print more information
    if unmatched_from_df2 > 0:
        print(f'Unmatched from {tbl_lab}: {unmatched_from_df2}')

        merged_df = pd.merge(df1, df2, on=keys, how='left', indicator=True)
        unmatched_rows = merged_df[merged_df['_merge'] != 'both']
        unmatched_count = unmatched_rows.shape[0]
        print(unmatched_rows[['week', 'player_name']])

        # Option 3 - Grouping To get Count
        unmatched_count = merged_df['_merge'].value_counts()
        print(unmatched_count)
        print(" ")
    elif unmatched_from_df2 == 0:
        print(f"All Rows in {tbl_lab} Match")
        print(" ")

def compute_weighted_stats(df, stats_list, weights_dict):
    """
    Compute weighted stats for the given statistics in the DataFrame.

    Parameters:
    - df (pd.DataFrame): The DataFrame containing the stats.
    - stats_list (list): A list of stat prefixes to compute.
    - weights_dict (dict): A nested dictionary where keys are stat prefixes and values are dictionaries
                           of sources and their corresponding weights.

    Returns:
    - pd.DataFrame: The updated DataFrame with the new weighted stat columns.
    """
    for stat in stats_list:
        new_column_name = f'TRUE_{stat}'
        df[new_column_name] = 0  # Initialize the new column

        # Check if the stat has specific weights defined
        if stat in weights_dict:
            for source, weight in weights_dict[stat].items():
                # Construct the column name based on the source and stat
                col_name = f"{source}_{stat}"
                if col_name in df.columns:
                    # Add the weighted contribution to the new column
                    df[new_column_name] += df[col_name] * weight
        else:
            # If no specific weights are defined for the stat, use default weights
            for source, weight in weights_dict['default'].items():
                col_name = f"{source}_{stat}"
                if col_name in df.columns:
                    df[new_column_name] += df[col_name] * weight
    
    return df

def proj_to_score(proj_df, s_league, col_pfix_list=['ESPN', 'FP', 'MEAN', 'PINNY', 'BOL', 'TRUE']):

    s_df = build_scoring_table(league=s_league)

    # Update scores in s_df for D/ST positions and specific conditions
    if 1727104 in list(proj_df['league_id'].unique()):
        dp_df = proj_df[proj_df['primaryPosition'].isin(['DL', 'DE', 'LB', 'NT', 'CB', 'S', 'DT', 'DB', 'OLB'])]
        normal_df = proj_df[~proj_df['primaryPosition'].isin(['DL', 'DE', 'LB', 'NT', 'CB', 'S', 'DT', 'DB', 'OLB'])]

        s_df.loc[s_df['id'] == 99, 'points'] = 1
        s_df.loc[s_df['id'] == 109, 'points'] = 0
        s_df.loc[s_df['id'] == 112, 'points'] = 0
        s_df.loc[s_df['id'] == 113, 'points'] = 0
        s_df.loc[s_df['id'] == 95, 'points'] = 1
        s_df.loc[s_df['id'] == 97, 'points'] = 1

        # Iterate over each prefix in the prefix list
        for col_pfix in col_pfix_list:
            # Initialize the score column to 0
            normal_df[f'{col_pfix}_Points'] = 0.0

            # Iterate over each score row in s_df
            for _, score_row in s_df.iterrows():
                col_name = f"{col_pfix}_{score_row['colName']}"

                # If the corresponding stat column exists in proj_df, calculate the weighted score
                if col_name in normal_df.columns:
                    normal_df[f'{col_pfix}_Points'] += normal_df[col_name] * score_row['points']


        # Handle DP 
        s_df.loc[s_df['id'] == 95, 'points'] = 12
        s_df.loc[s_df['id'] == 97, 'points'] = 2
        s_df.loc[s_df['id'] == 99, 'points'] = 10
        s_df.loc[s_df['id'] == 112, 'points'] = 5
        s_df.loc[s_df['id'] == 113, 'points'] = 5
        s_df.loc[s_df['id'] == 107, 'points'] = 0.5
        s_df.loc[s_df['id'] == 108, 'points'] = 2

        # Iterate over each prefix in the prefix list
        for col_pfix in col_pfix_list:
            # Initialize the score column to 0
            dp_df[f'{col_pfix}_Points'] = 0.0

            # Iterate over each score row in s_df
            for _, score_row in s_df.iterrows():
                col_name = f"{col_pfix}_{score_row['colName']}"

                # If the corresponding stat column exists in proj_df, calculate the weighted score
                if col_name in dp_df.columns:
                    dp_df[f'{col_pfix}_Points'] += dp_df[col_name] * score_row['points']

        dp_df['TRUE_Points'] = (dp_df['ESPN_Points'] + dp_df['BOL_Points']) / 2

        proj_df = pd.concat([normal_df, dp_df])

    else:
        # Iterate over each prefix in the prefix list
        for col_pfix in col_pfix_list:
            # Initialize the score column to 0
            proj_df[f'{col_pfix}_Points'] = 0.0

            # Iterate over each score row in s_df
            for _, score_row in s_df.iterrows():
                col_name = f"{col_pfix}_{score_row['colName']}"

                # If the corresponding stat column exists in proj_df, calculate the weighted score
                if col_name in proj_df.columns:
                    proj_df[f'{col_pfix}_Points'] += proj_df[col_name] * score_row['points']
    
    return proj_df

def clean_lineups(df, lg):

    # Get Base of Projections (player_name, week, team, etc.)
    base_cols = ['league_id','year','week', 'team_owner', 'team_name', 'team_division', 'player_name', 'player_id', 'slotPosition', 'primaryPosition', 'eligiblePositions', 'pro_team', 'current_team_id' ,'player_position' ,'player_active_status', 'points', 'projPoints']
    scores_df = build_scoring_table(league=lg)
    actual_scoring_cols = scores_df['colName'].to_list()
    base = df[base_cols + actual_scoring_cols]

    curr_week = lg.current_week

    # Constants

    ## Defensive Positions
    d_pos = ['DL', 'DE', 'LB', 'NT', 'CB', 'S', 'DT', 'DB', 'OLB']

    ## Duplicate Error
    fix_list = ['ESPN_rushingYards', 'ESPN_receivingYards', 'ESPN_passingYards']

    # 1) Combine ESPN and Fantasy Pros Data

    ## a) Build ESPN From Raw Data
    espn_proj = df[['week', 'player_name', 'primaryPosition', 'player_active_status']  + list(df.filter(like='proj_').columns)]
    espn_proj = change_col_prefix(df=espn_proj, old_pfix="proj", new_pfix="ESPN")

    ## b) Build Fantasy Pros From Scrape
    fp_proj = pd.read_parquet("Data/Projections/FantasyPros/FantasyPros_Projections_Week_All.parquet").drop(columns=['STD_FantasyPoints', 'TimeStamp'])
    fp_proj = change_col_prefix(df=fp_proj, old_pfix="proj", new_pfix="FP")

    ## c) Combine ESPN and FP
    trans1_df = espn_proj.merge(fp_proj, how='left', on=['week', 'player_name'])
    get_match_details(df1=espn_proj, df2=fp_proj, keys=["week", "player_name"], check_col2="FP_rushingTouchdowns", min_wk=curr_week, tbl_lab="FantasyPros Table")

    ## d) Correct Error Where ESPN Duplicates Yards
    print("=============================== Correcting ESPN Doubled Yard Projections ===============================")
    print(" ")
    for column in fix_list:
        FP_Col = column.replace("ESPN_", "FP_")
        # Apply with a conditional print statement
        trans1_df[column] = trans1_df.apply(
            lambda row: (
                # Check if the condition is true
                print(f"Player: {row['player_name']}, Week: {row['week']}, "
                      f"Original {column}: {row[column]}, "
                      f"New {column}: {row[column] / 2}") or row[column] / 2
                if (row[column] > (row[FP_Col] * 1.75)) and (row[column] > 40) else row[column]
            ),
            axis=1
        )

    print(" ")
    print("======================================== End Doubled Correction ========================================")
    print(" ")

    ## e) Impute FP with ESPN + Create Means
    trans1_df = impute_columns(trans1_df, target_prefix='FP_', source_prefix='ESPN_')
    mean_df = create_mean_cols(trans1_df, target_prefix='FP_', source_prefix='ESPN_')

    ## f) Retain New ESPN Values For Join With Books + Add To Base
    base = base.merge(trans1_df, on=['week', 'player_name', 'primaryPosition','player_active_status'], how='left')

    ## f) Create Dataframe of Means For Imputing Sportsbook Data
    mean_df = create_mean_cols(trans1_df, target_prefix='FP_', source_prefix='ESPN_')
    

    # 2) Combine Pinnacle Data With ESPN and Impute
    ## a) Clean Pinnacle Data
    pinny_proj = clean_pinny() #pd.read_parquet("Data/Projections/Pinnacle/Season/Pinnacle_Props_Week_All.parquet")
    pinny_proj = change_col_prefix(df=pinny_proj, old_pfix="proj", new_pfix="PINNY")

    ## b) Impute Missing Data From ESPN
    trans2_df = mean_df.merge(pinny_proj, on=["week", "player_name"], how='left')
    get_match_details(df1=mean_df, df2=pinny_proj, keys=["week", "player_name"], check_col2="PINNY_receivingYards", min_wk=curr_week, tbl_lab="Pinnacle Sportsbook Table")
    trans2_df = impute_columns(trans2_df, target_prefix='PINNY_', source_prefix="MEAN_")
    

    ## c) Slim Columns To Only Pinnacle Data
    trans2_df = trans2_df[['week', 'player_name', 'primaryPosition','player_active_status'] + list(trans2_df.filter(like='PINNY').columns)]
    ## d) Join Slim Transformation Back To Base
    base = base.merge(trans2_df, on=['week', 'player_name', 'primaryPosition','player_active_status'], how='left')


    # 3) Combine BetOnline Data With ESPN and Impute
    bol_proj = clean_bol()
    bol_proj = change_col_prefix(df=bol_proj, old_pfix="proj", new_pfix="BOL")

    ## b) Impute Missing Data From ESPN
    trans3_df = mean_df.merge(bol_proj, on=["week", "player_name"], how='left')
    get_match_details(df1=mean_df, df2=bol_proj, keys=["week", "player_name"], check_col2="NFL_game_id", min_wk=curr_week, tbl_lab="BetOnline Sportsbook Table")
    trans3_df = impute_columns(trans3_df, target_prefix='BOL_', source_prefix="MEAN_")

    ## c) Slim Columns To Only BOL Data
    trans3_df = trans3_df[['week', 'player_name', 'primaryPosition','player_active_status'] + list(trans3_df.filter(like='BOL_').columns)]

    ## d) Join Slim Transformation Back To Base
    base = base.merge(trans3_df, on=['week', 'player_name', 'primaryPosition','player_active_status'], how='left')

    ## Clean Missing COlumns
    base = impute_columns(base, target_prefix='PINNY_', source_prefix='MEAN_')
    base = impute_columns(base, target_prefix='BOL_', source_prefix='MEAN_')


    ## 5) Create Aggregate Columns For Each Projection Type (Manual Weights)
    weights_dict = {
        'passingYards': {'ESPN': 0.1,'FP': 0.7,'PINNY': 0.1,'BOL': 0.1},
        'passingTouchdowns': {'ESPN': 0.1,'FP': 0.1,'PINNY': 0.4,'BOL': 0.4},
        'rushingYards': {'ESPN': 0.2,'FP': 0.3,'PINNY': 0.25,'BOL': 0.25},
        'receivingYards': {'ESPN': 0.2,'FP': 0.3,'PINNY': 0.25,'BOL': 0.25},
        'default': {'ESPN': 0.2,'FP': 0.3,'PINNY': 0.25,'BOL': 0.25}
    }

    final = compute_weighted_stats(df=base, stats_list=actual_scoring_cols, weights_dict=weights_dict)

    ## 6) Build Score Column
    final = proj_to_score(proj_df=final, s_league=lg)

    # a) Adjust For Extra ESPN Stats
    final['adjustment'] = final['projPoints'] - final['ESPN_Points']
    for i in ['ESPN', 'FP', 'MEAN', 'PINNY', 'BOL', 'TRUE']:
        final[f'{i}_Points'] = final['adjustment'] + final[f'{i}_Points']


    ## 7) Build Position Rank Columns
    for i in ['ESPN', 'FP', 'MEAN', 'PINNY', 'BOL', 'TRUE']:
        final[f"{i}_PosRank"] = final.groupby(['week', 'primaryPosition'])[f'{i}_Points'].rank(ascending=False, method='dense')

    # Actual
    final['PosRank'] = final.groupby(['week', 'primaryPosition'])['points'].rank(ascending=False, method='dense')

    return final

## Populate Table Functions
def check_week(lu, week, own):

    lu['trueDiff'] = lu['TRUE_Points'] - lu['projPoints']

    # Get My Team
    df = lu[(lu['week'] == week) & (lu['team_owner'] == own)][['week', 'team_name', 'player_name', 'slotPosition', 'primaryPosition',
                                                               'points', 'projPoints', 'FP_Points', 'PINNY_Points', 'BOL_Points', 'TRUE_Points', 'trueDiff',
                                                               'PosRank', 'ESPN_PosRank', 'FP_PosRank', 'PINNY_PosRank', 'BOL_PosRank', 'TRUE_PosRank']]
    
    df = df.rename(columns={
        'team_name': 'team',
        'player_name': 'player',
        'slotPosition': 'rosPos',
        'primaryPosition': 'primPos',
        'points': 'Actual_PTS',
        'projPoints': 'ESPN_PTS',
        'FP_Points': 'FP_PTS',
        'PINNY_Points': 'PINNY_PTS',
        'BOL_Points': 'BOL_PTS',
        'TRUE_Points': 'TRUE_PTS',
        'trueDiff': 'DIFF_PTS',
        'PosRank': 'Actual',
        'ESPN_PosRank': 'ESPN',
        'FP_PosRank': 'FP',
        'PINNY_PosRank': 'PINNY',
        'BOL_PosRank': 'BOL',
        'TRUE_PosRank': 'TRUE',
    })

    # Order
    bench = pd.DataFrame([{
        'week': week,
        'team': df['team'].iloc[0],
        'player': 'Total',
        'rosPos': 'Starting Lineup',
        'primPos': 'Starting Lineup',
        'Actual_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['Actual_PTS'].sum(),
        'ESPN_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['ESPN_PTS'].sum(),
        'FP_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['FP_PTS'].sum(),
        'PINNY_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['PINNY_PTS'].sum(),
        'BOL_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['BOL_PTS'].sum(),
        'TRUE_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['TRUE_PTS'].sum(),
        'DIFF_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['DIFF_PTS'].sum(),
        'Actual': '',
        'ESPN': '',
        'FP': '',
        'PINNY': '',
        'BOL': '',
        'TRUE': ''
        
    },
    {
        'week': week,
        'team': df['team'].iloc[0],
        'player': 'Total',
        'rosPos': 'Bench',
        'primPos': 'Bench',
        'Actual_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['Actual_PTS'].sum(),
        'ESPN_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['ESPN_PTS'].sum(),
        'FP_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['FP_PTS'].sum(),
        'PINNY_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['PINNY_PTS'].sum(),
        'BOL_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['BOL_PTS'].sum(),
        'TRUE_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['TRUE_PTS'].sum(),
        'DIFF_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['DIFF_PTS'].sum(),
        'Actual': '',
        'ESPN': '',
        'FP': '',
        'PINNY': '',
        'BOL': '',
        'TRUE': ''
        
    }])

    # Append the summary row to the DataFrame
    df = pd.concat([df, bench], ignore_index=True)

    pos_order = ['QB', 'RB', 'WR', 'TE', 'RB/WR/TE', 'OP', 'DP', 'D/ST', 'K', 'Starting Lineup', 'BE', 'Bench', 'IR']
    order_mapping = {val: idx for idx, val in enumerate(pos_order)}
    df = df.sort_values(by=['rosPos', 'TRUE_PTS'], key=lambda x: x.map(order_mapping))

    # Round
    df = df.round(decimals={'FP_PTS': 2, 'PINNY_PTS': 2, 'BOL_PTS': 2, 'TRUE_PTS': 2, 'DIFF_PTS': 3})

    # Drop Actual if Current Week
    if week == curr_week:
        df.drop(['Actual_PTS', 'Actual'], axis=1, inplace=True)

    return df

def get_league_projections(week, lu):
    df = lu[(lu['week'] == week) & (lu['team_owner'] != 'Free Agent') & (~lu['slotPosition'].isin(['BE', 'IR']))][['week', 'team_owner', 'team_name',
             'points', 'projPoints', 'FP_Points', 'BOL_Points', 'PINNY_Points', 'TRUE_Points']]
    
    df['TRUE_Points'] = df['TRUE_Points'].fillna(df['projPoints'])

    result = df.groupby(['week', 'team_owner', 'team_name'], as_index=False).agg({
        'points': 'sum',
        'projPoints': 'sum',
        'FP_Points': 'sum',
        'BOL_Points': 'sum',
        'PINNY_Points': 'sum',
        'TRUE_Points': 'sum'
        })
    
    result['point_diff'] = result['TRUE_Points'] - result['projPoints']

    return result.sort_values(by='TRUE_Points', ascending=False)

def get_rankings(pos, week, visualize = False, check_fa = False):
    df = LINEUPS[(LINEUPS['primaryPosition'].isin(pos)) & (LINEUPS['week'] == week)]
    df = df[['week', 'primaryPosition','player_name', 'team_owner', 'team_name',
                  'points', 'projPoints', 'FP_Points', 'BOL_Points', 'PINNY_Points', 'TRUE_Points',
                  'PosRank', 'ESPN_PosRank', 'FP_PosRank', 'BOL_PosRank', 'PINNY_PosRank', 'TRUE_PosRank']]
    df = df.drop(columns=['points', 'PosRank']).sort_values(by=['TRUE_Points'], ascending=False)

    if visualize == False:
        if check_fa == True:
            return df[df['team_owner'].isin([lg_vars[select_league]['primary_own'], 'Free Agent'])]
        else:
            return df

def write_to_google(df_dict, league_name):
    # Set up authentication
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    # Option 1: Using service account (recommended for automation)
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        'gs4creds.json', 
        scope
    )

    # Get Min and Max
    points_cols = ['projPoints', 'FP_Points', 'BOL_Points', 'PINNY_Points', 'TRUE_Points']
    scale_dict = {
        "MAX": {
            "TEAM": df_dict["League_Projections"][points_cols].max().max(),
            "QBs": df_dict["FA_QBs"][points_cols].max().max(),
            "RBs": df_dict["FA_RBs"][points_cols].max().max(),
            "WRs": df_dict["FA_WRs"][points_cols].max().max(),
            "TEs": df_dict["FA_TEs"][points_cols].max().max(),
            "FLX": df_dict["FA_FLX"][points_cols].max().max(),
            "DST": df_dict["FA_DST"][points_cols].max().max(),
            "KCK": df_dict["FA_KCK"][points_cols].max().max(),
            "IDP": df_dict["FA_IDP"][points_cols].max().max(),
            "Bench": 50
        },
        "MEDIAN": {
            "TEAM": df_dict["League_Projections"][points_cols].replace(0, np.nan).median().median(),
            "QBs": df_dict["FA_QBs"][df_dict["FA_QBs"]["team_owner"] != "Free Agent"][points_cols].replace(0, np.nan).median().median(), #
            "RBs": df_dict["FA_RBs"][df_dict["FA_RBs"]["team_owner"] != "Free Agent"][points_cols].replace(0, np.nan).median().median(), #
            "WRs": df_dict["FA_WRs"][df_dict["FA_WRs"]["team_owner"] != "Free Agent"][points_cols].replace(0, np.nan).median().median(), #
            "TEs": df_dict["FA_TEs"][df_dict["FA_TEs"]["team_owner"] != "Free Agent"][points_cols].replace(0, np.nan).median().median(), #
            "FLX": df_dict["FA_FLX"][df_dict["FA_FLX"]["team_owner"] != "Free Agent"][points_cols].replace(0, np.nan).median().median(), #
            "DST": df_dict["FA_DST"][points_cols].replace(0, np.nan).median().median(), #
            "KCK": df_dict["FA_KCK"][points_cols].replace(0, np.nan).median().median(),
            "IDP": df_dict["FA_IDP"][points_cols].replace(0, np.nan).median().median(),
            "Bench": 25
        },
        "MIN": {
            "TEAM": df_dict["League_Projections"][points_cols].min().min(),
            "QBs": 0,
            "RBs": 0,
            "WRs": 0,
            "TEs": 0,
            "FLX": 0,
            "DST": 0,
            "KCK": 0,
            "IDP": 0,
            "Bench": 0
        }
    }

    # Authorize and connect
    client = gspread.authorize(creds)

    # Open or create a spreadsheet
    spreadsheet = client.open(league_name)

    for sheet_name, df in df_dict.items():
        try:
            # Try to get existing worksheet
            worksheet = spreadsheet.worksheet(sheet_name)
            # Clear existing content
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            # Create new worksheet if it doesn't exist
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name, 
                rows=str(len(df) + 1), 
                cols=str(len(df.columns))
            )

        # Formatting Functions

        # Clear Formatting
        def clear_sheet_formatting(worksheet, data_df):
            """
            Clear all conditional formatting, text formatting, and borders from the worksheet.

            Args:
                worksheet: gspread worksheet object
                data_df: pandas DataFrame (to determine number of rows)
            """
            num_rows = len(data_df) + 1
            num_cols = len(data_df.columns)

            requests = []

            # 1. Clear all conditional format rules
            try:
                sheet_metadata = worksheet.spreadsheet.fetch_sheet_metadata()
                for sheet in sheet_metadata['sheets']:
                    if sheet['properties']['sheetId'] == worksheet.id:
                        rules_count = len(sheet.get('conditionalFormats', []))
                        # Delete all conditional format rules
                        for _ in range(rules_count):
                            requests.append({
                                "deleteConditionalFormatRule": {
                                    "sheetId": worksheet.id,
                                    "index": 0
                                }
                            })
                        break
            except Exception as e:
                print(f"Note clearing conditional formatting: {e}")

            # 2. Clear text formatting, borders, and background colors
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": num_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {},
                            "borders": {},
                            "backgroundColor": {"red": 1, "green": 1, "blue": 1}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat,userEnteredFormat.borders,userEnteredFormat.backgroundColor"
                }
            })

            # Execute all requests
            #if requests:
            #    worksheet.spreadsheet.batch_update({"requests": requests})

            return requests
        
        # Format Lineup Sheet
        def format_lineup_rows(worksheet, data_df, target_values=['Starting Lineup', 'Bench']):
            """
            Apply bold formatting and top/bottom borders to rows where a specific column
            contains one of the target values. Also applies gradient color formatting 
            (green to white) to columns F:J for rows NOT matching target values.

            Args:
                worksheet: gspread worksheet object
                data_df: pandas DataFrame (to determine number of rows)
                column_letter: Column to check (default 'D')
                target_values: List of values to match (default ['Starting Lineup', 'Bench'])
            """
            requests = []

            # Clear Formatting
            clear_requests = clear_sheet_formatting(worksheet, data_df)
            requests.append(clear_requests)

            # Get DF Stats + Indicies
            num_rows = len(data_df) + 1
            rospos_col_num = ord('D'.upper()) - ord('A') + 1
            primpos_col_num = ord('E'.upper()) - ord('A') + 1

            # 1) Format header row - bold, white text, navy blue background
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(data_df.columns)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {
                                "bold": True,
                                "foregroundColor": {"red": 1, "green": 1, "blue": 1}
                            },
                            "backgroundColor": {"red": 0, "green": 0, "blue": 0.5}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"
                }
            })

            # Format columns F:J to 2 decimal places
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,  # Row 2 (0-indexed)
                        "endRowIndex": num_rows,
                        "startColumnIndex": 5,  # Column F (0-indexed)
                        "endColumnIndex": 10  # Column J (0-indexed, exclusive)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "0.00"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

            # Find row numbers for Starting Lineup and Bench
            starting_lineup_row = None
            bench_row = None

            starting_lineup_cells = worksheet.findall('Starting Lineup', in_column=rospos_col_num)
            if starting_lineup_cells:
                starting_lineup_row = starting_lineup_cells[0].row

            bench_cells = worksheet.findall('Bench', in_column=rospos_col_num)
            if bench_cells:
                bench_row = bench_cells[0].row

            ## Conditional Formatting Of Points
            num_rows = len(data_df) + 1

            position_mapping = {
                'QB': 'QBs',
                'RB': 'RBs',
                'WR': 'WRs',
                'TE': 'TEs',
                'D/ST': 'DST',
                'LB': 'IDP',
                'S':  'IDP',
                'CB': 'IDP',
                'DE': 'IDP',
                'DT': 'IDP',
                'K':  'KCK',
                'Starting Lineup': "TEAM",
                "Bench": "Bench"
            }

            rules = []
            rule_index = 0
            for position_value, scale_key in position_mapping.items():
                matching_cells = worksheet.findall(position_value, in_column=primpos_col_num)

                if not matching_cells:
                    continue

                row_numbers = [cell.row for cell in matching_cells]
                #print(f"Found {len(row_numbers)} rows for position {position_value}: {row_numbers}")

                # Get the max and median values for this position
                max_value = scale_dict["MAX"][scale_key]
                median_value = scale_dict["MEDIAN"][scale_key]
                min_value = scale_dict["MIN"][scale_key]

                # Create a range for each row with this position
                for row_num in row_numbers:
                    rule = {
                        "addConditionalFormatRule": {
                            "rule": {
                                "ranges": [{
                                    "sheetId": worksheet.id,
                                    "startRowIndex": row_num - 1,  # Convert to 0-indexed
                                    "endRowIndex": row_num,  # Exclusive, so this is just the one row
                                    "startColumnIndex": 5,  # Column F (0-indexed)
                                    "endColumnIndex": 10  # Column J (0-indexed, exclusive)
                                }],
                                "gradientRule": {
                                    "minpoint": {
                                        "color": {"red": 0.95, "green": 0.42, "blue": 0.42},  # Red
                                        "type": "NUMBER",
                                        "value": str(min_value)
                                    },
                                    "midpoint": {
                                        "color": {"red": 1, "green": 1, "blue": 1},  # White
                                        "type": "NUMBER",
                                        "value": str(median_value)
                                    },
                                    "maxpoint": {
                                        "color": {"red": 0.42, "green": 0.66, "blue": 0.42},  # Green
                                        "type": "NUMBER",
                                        "value": str(max_value)
                                    }
                                }
                            },
                            "index": rule_index
                        }
                    }
                    rules.append(rule)
                    rule_index += 1

                #print(f"Applied {len(rules)} gradient formatting rules to F:J")

            ## Bold And Border Totals Rows
            matching_cells = []
            for value in target_values:
                cells = worksheet.findall(value, in_column=rospos_col_num)
                matching_cells.extend(cells)

            # Loop Bold And Border
            for cell in matching_cells:
                row_number = cell.row

                # Add request for this row
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startRowIndex": row_number - 1,  # Convert to 0-indexed
                            "endRowIndex": row_number,
                            "startColumnIndex": 0,
                            "endColumnIndex": len(data_df.columns)  # Or use a fixed number
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "bold": True
                                },
                                "borders": {
                                    "top": {
                                        "style": "SOLID",
                                        "width": 2,
                                        "color": {"red": 0, "green": 0, "blue": 0}
                                    },
                                    "bottom": {
                                        "style": "SOLID",
                                        "width": 2,
                                        "color": {"red": 0, "green": 0, "blue": 0}
                                    }
                                }
                            }
                        },
                        "fields": "userEnteredFormat.textFormat,userEnteredFormat.borders"
                    }
                })

            # Execute all row formatting in one batch update (after the loop)
            if rules:
                worksheet.spreadsheet.batch_update({"requests": requests + rules})
            else:
                worksheet.spreadsheet.batch_update({"requests": requests})

        # Format League Projections Sheet
        def format_league_projections(worksheet, data_df):
            """
            Format the League Projections sheet with header styling and gradient color scale.

            Args:
                worksheet: gspread worksheet object
                data_df: pandas DataFrame (to determine number of rows)
                scale_dict: Dictionary with MAX and MEDIAN values
            """
            # Initialize Write Requests
            requests = []

            # Clear Formatting
            clear_requests = clear_sheet_formatting(worksheet, data_df)
            requests.append(clear_requests)

            num_rows = len(data_df) + 1  # +1 for header

            # 1) Format header row - bold, white text, navy blue background
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(data_df.columns)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {
                                "bold": True,
                                "foregroundColor": {"red": 1, "green": 1, "blue": 1}
                            },
                            "backgroundColor": {"red": 0, "green": 0, "blue": 0.5}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"
                }
            })

            # 2) Format columns E:I to 2 decimal places
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,  # Row 2 (0-indexed)
                        "endRowIndex": num_rows,
                        "startColumnIndex": 4,  # Column E (0-indexed)
                        "endColumnIndex": 9  # Column I (0-indexed, exclusive)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "0.00"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

            # 3) Get TEAM values from scale_dict for gradient
            max_value = scale_dict["MAX"]["TEAM"]
            median_value = scale_dict["MEDIAN"]["TEAM"]
            min_value = scale_dict["MIN"]["TEAM"]

            # Create gradient rule for columns E:I (rows 2 onwards)
            rule = {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": worksheet.id,
                            "startRowIndex": 1,  # Start at row 2 (0-indexed)
                            "endRowIndex": num_rows,
                            "startColumnIndex": 4,  # Column E (0-indexed)
                            "endColumnIndex": 9  # Column I (0-indexed, exclusive)
                        }],
                        "gradientRule": {
                            "minpoint": {
                                "color": {"red": 0.95, "green": 0.42, "blue": 0.42},  # Red
                                "type": "NUMBER",
                                "value": str(min_value)
                            },
                            "midpoint": {
                                "color": {"red": 1, "green": 1, "blue": 1},  # White
                                "type": "NUMBER",
                                "value": str(median_value)
                            },
                            "maxpoint": {
                                "color": {"red": 0.42, "green": 0.66, "blue": 0.42},  # Green
                                "type": "NUMBER",
                                "value": str(max_value)
                            }
                        }
                    },
                    "index": 0
                }
            }

            # Execute the formatting
            worksheet.spreadsheet.batch_update({"requests": requests + [rule]})

        # Format Free Agents
        def format_free_agents(worksheet, data_df, position):
            """
            Format the League Projections sheet with header styling and gradient color scale.

            Args:
                worksheet: gspread worksheet object
                data_df: pandas DataFrame (to determine number of rows)
                scale_dict: Dictionary with MAX and MEDIAN values
            """
            requests = []

            # Clear Formatting
            clear_requests = clear_sheet_formatting(worksheet, data_df)
            requests.append(clear_requests)

            num_rows = len(data_df) + 1  # +1 for header

            # 1) Format header row - bold, white text, navy blue background
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(data_df.columns)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {
                                "bold": True,
                                "foregroundColor": {"red": 1, "green": 1, "blue": 1}
                            },
                            "backgroundColor": {"red": 0, "green": 0, "blue": 0.5}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"
                }
            })

            # 2) Format columns F:J to 2 decimal places
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,  # Row 2 (0-indexed)
                        "endRowIndex": num_rows,
                        "startColumnIndex": 5,  # Column F (0-indexed)
                        "endColumnIndex": 10  # Column J (0-indexed, exclusive)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "0.00"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

            # 2) Get TEAM values from scale_dict for gradient
            max_value = scale_dict["MAX"][position]
            median_value = scale_dict["MEDIAN"][position]
            min_value = scale_dict["MIN"][position]

            # Create gradient rule for columns E:I (rows 2 onwards)
            rule = {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": worksheet.id,
                            "startRowIndex": 1,  # Start at row 2 (0-indexed)
                            "endRowIndex": num_rows,
                            "startColumnIndex": 4,  # Column E (0-indexed)
                            "endColumnIndex": 10  # Column J (0-indexed, exclusive)
                        }],
                        "gradientRule": {
                            "minpoint": {
                                "color": {"red": 0.95, "green": 0.42, "blue": 0.42},  # Red
                                "type": "NUMBER",
                                "value": str(min_value)
                            },
                            "midpoint": {
                                "color": {"red": 1, "green": 1, "blue": 1},  # White
                                "type": "NUMBER",
                                "value": str(median_value)
                            },
                            "maxpoint": {
                                "color": {"red": 0.42, "green": 0.66, "blue": 0.42},  # Green
                                "type": "NUMBER",
                                "value": str(max_value)
                            }
                        }
                    },
                    "index": 0
                }
            }

            # Execute the formatting
            worksheet.spreadsheet.batch_update({"requests": requests + [rule]})

        if sheet_name == "Lineup":
            df.columns = ['WK', 'TEAM', 'PLAYER', 'SLOT', 'POS', 'ESPN_PTS', 'FP_PTS', 'PINNY_PTS', 'BOL_PTS', 'TRUE_PTS', 'DIFF_PTS', 'ESPN', 'FP', 'PINNY', 'BOL', 'TRUE']
            set_with_dataframe(worksheet, df, include_index=False)
            format_lineup_rows(worksheet, df)
            time.sleep(5)
        elif sheet_name == "League_Projections":
            df.columns = ['WK', 'OWNER', 'TEAM', 'ACTUAL', 'ESPN', 'FP', 'BOL', 'PINNY', 'TRUE', 'DIFF']
            set_with_dataframe(worksheet, df, include_index=False)
            format_league_projections(worksheet, df)
            time.sleep(5)
        elif "FA_" in sheet_name:
            try:
                position = sheet_name.split("FA_")[1]
                fa_df = df[df["team_owner"].isin(["Free Agent", lg_vars[select_league]['primary_own']])]
                fa_df.columns = ['WK', 'POS', 'PLAYER', 'OWNER', 'TEAM', 'ESPN_PTS', 'FP_PTS', 'BOL_PTS', 'PINNY_PTS', 'TRUE_PTS', 'ESPN', 'FP', 'BOL', 'PINNY', 'TRUE']
                set_with_dataframe(worksheet, fa_df, include_index=False)
                format_free_agents(worksheet, fa_df, position)
                print(f"Written {sheet_name} to Google Sheets")
                time.sleep(5)
            except:
                print(f"Error Loading {sheet_name} | Position Does Not Exist in League")

all = ['GOP_Degenerates', 'Winfield_Football', 'Knights_FFL', 'Weenieless_Wanderers', 'John_PC_League', 'John_ATL_League', "12 Dudes one Cup", 'Big Red Fantasy Football']
john = ['John_PC_League', 'John_ATL_League']
tommy = ['Winfield_Football', 'Knights_FFL', 'GOP_Degenerates', 'Weenieless_Wanderers']
will = ["12 Dudes one Cup"]
cooleen = ['Big Red Fantasy Football']

for l in all:
    select_league = l
    league = fetch_league(
        league_id=lg_vars[select_league]['ID'],
        year=lg_vars[select_league]['end'],
        swid=lg_vars[select_league]['SWID'],
        espn_s2=lg_vars[select_league]['ESPN_S2']
    )
    lineups = get_ply_stats_by_matchup(league_id=lg_vars[select_league]['ID'],
                    year=2025,
                    swid=lg_vars[select_league]['SWID'],
                    espn_s2=lg_vars[select_league]['ESPN_S2'])
    free_agents = build_fa_market(league=league)
    curr_week = league.current_week
    lineups = pd.concat([lineups, free_agents])
    lineups.fillna(0, inplace=True)
    lineups = lineups.drop_duplicates(subset=['week', 'player_name'])
    ## Build Basic Lineup Table
    LINEUPS = clean_lineups(df=lineups, lg=league)
    # Data Dictionary
    df_dict = {
        "League_Projections": get_league_projections(week = curr_week, lu=LINEUPS),
        "Lineup": check_week(lu = LINEUPS, week = curr_week, own=lg_vars[select_league]['primary_own']),
        "FA_QBs": get_rankings(pos = ['QB'], week = curr_week, visualize=False, check_fa=False),
        "FA_RBs": get_rankings(pos = ['RB'], week = curr_week, visualize=False, check_fa=False),
        "FA_WRs": get_rankings(pos = ['WR'], week = curr_week, visualize=False, check_fa=False),
        "FA_TEs": get_rankings(pos = ['TE'], week = curr_week, visualize=False, check_fa=False),
        "FA_FLX": get_rankings(pos = ['RB', 'WR','TE'], week = curr_week, visualize=False, check_fa=False),
        "FA_DST": get_rankings(pos = ['D/ST'], week = curr_week, visualize=False, check_fa=False),
        "FA_KCK": get_rankings(pos = ['K'], week = curr_week, visualize=False, check_fa=False),
        "FA_IDP": get_rankings(pos = ['LB', 'DE', 'S', 'CB', 'DT'], week = curr_week, visualize=False, check_fa=False),
    }
    # Save to Google Sheet
    print("")
    print(f"========= Now Writing Data For {select_league} ========")
    print("")
    write_to_google(df_dict=df_dict, league_name=select_league)
    print("")
    print(f"========= Successful Save For {select_league} ========")
    print("")
    
    # Sleep
    sleep_secs = 20
    time.sleep(sleep_secs)
    print(f"Now Sleeping For {sleep_secs} Seconds")


