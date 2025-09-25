import time
from datetime import datetime
import requests
import polars as pl
from pathlib import Path
#from nfl_utils import NFL_SCHEDULE


## Constants

# NFL Schedule
NFL_SCHEDULE = pl.read_csv('Data/NFL_Schedules.csv')
SLIM_SCHED = pl.DataFrame(NFL_SCHEDULE)\
                .select([
                    pl.col('game_id').alias('NFL_game_id')
                    ,pl.col('week').cast(pl.Int64)
                    ,pl.col('gameday').str.strptime(pl.Date, format="%Y-%m-%d").alias('officialDate')
                    ,pl.col('away_team').alias('Away')
                    ,pl.col('home_team').alias('Home')
                ])

# Current Week
week = NFL_SCHEDULE.filter(pl.col('away_score') == 'NA')['week'].min()
print(f"Now Loading NFL Week {week}:")

# BetOnline ID For First Game
id_var = 259370 #259386

# Statistic Mapping
stats = {
    'anytimeTouchdown': 'Touchdowns',
    'passingYards': 'Passing%2520Yards',
    'passingCompletions': 'Pass%2520Completions',
    'passingTouchdowns': 'Passing%2520TDs',
    'passingAttempts': 'Pass%2520Attempts',
    'passingInterceptions': 'Pass%2520Interceptions',
    'rushingYards': 'Rushing%2520Yards',
    'rushingAttempts': 'Carries',
    'receivingYards': 'Receiving%2520Yards',
    'receivingReceptions': 'Receptions',
    'defensiveTotalTackles': 'Tackles',
    'defensiveSacks': 'Sacks',
    'defensiveInterceptions': 'Interceptions'
}

## Functions

# Create BOL Keys
def get_week_ids(sched: pl.DataFrame, week_num: pl.Int32, id_start: int) -> dict:

    # Get Game Count
    current_week = (
        sched.group_by('week').agg([
            pl.col('officialDate').min().alias('week_start'),
            pl.col('officialDate').max().alias('week_end'),
            pl.col('NFL_game_id').n_unique().alias('game_count')
        ]).filter(pl.col('week') == week_num)
    )

    # Build ID List
    game_count = current_week.select(pl.col('game_count')).item()
    game_ids = {
        week_num: list(range(id_start, id_start + game_count))
    }

    return game_ids
def build_BOL_dim(ids:list, sched_df: pl.DataFrame):
    raw = pl.DataFrame()
    for i in ids:
        url = f'https://bv2-us.digitalsportstech.com/api/dfm/marketsBySs?sb=betonline&gameId={str(i)}&statistic=Touchdowns'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            players_data = data[0]['players']

            # Build DF
            rows = []
            for player in players_data:
                for market in player['markets']:
                    rows.append({
                        'team': player['team'],
                        'BOL_game_id': market['game1Id']
                    })
            raw = raw.vstack(pl.DataFrame(rows))
        else:
            print(f"Failed to retrieve data for Game ID {i}. Status code: {response.status_code}")

        dim = raw.unique()

    # Join To NFL Schedule:
    dim = (
        dim
        .join(sched_df, left_on=['team'], right_on=['Away'], how='left')
        .filter(~pl.col('NFL_game_id').is_null())
        .select([
             pl.col('NFL_game_id')
            ,pl.col('team').alias('Away')
            ,pl.col('Home')
            ,pl.col('officialDate')
            ,pl.col('week')
            ,pl.col('BOL_game_id')
        ])
        .unique()
    )

    return dim

# GET BOL json
def get_BOL_data(ids: list, link_stat: str, espn_stat: str) -> pl.DataFrame:
    # Initialize Polars DF
    raw = pl.DataFrame()
    for i in ids:
        # Build URL + Game Label
        url = f'https://bv2-us.digitalsportstech.com/api/dfm/marketsBySs?sb=betonline&gameId={str(i)}&statistic={link_stat}'

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()

            # Check Data Loaded
            try:
                players_data = data[0]['players']

                # Build DF
                rows = []
                for player in players_data:
                    for market in player['markets']:
                        rows.append({
                            'BOL_game_id': market['game1Id'],
                            'week': week,
                            'player_name': player['name'],
                            'player_id': player['id'],
                            'team': player['team'],
                            'position': player['position']['title'],
                            'market_id': market['id'],
                            'condition': market['condition'],
                            'is_active': market['isActive'],
                            'is_actual': market['isActual'],
                            'type': str(market['type']),
                            'odds': market['odds'],
                            'value': market['value'],
                            'statistic': market['statistic']['title'],
                            'espn_stat': espn_stat
                        })
                raw = raw.vstack(pl.DataFrame(rows).with_columns([(1/pl.col('odds')).alias('impProb'), pl.col('value').cast(pl.Float64)]))
            except:
                print(f"Data Retreived with Error for BOL Game ID: {i} | Stat: {link_stat}")
        else:
            print(f"Failed to retrieve {link_stat} data for BOL Game ID: {i}. Status code: {response.status_code}")

    team_map = {
        'LVR': 'LV',
        'NOS': 'NO',
        'LAR': 'LA' 
    }

    if raw.height > 0:
        raw = raw.with_columns(
                pl.col("team").str.replace_many(
                    list(team_map.keys()),
                    list(team_map.values())
                )
                .alias('team'),
                pl.lit('Values').alias('prop_source')
            )

        return raw
    else:
        return None
def get_BOL_data_OU(ids: list, link_stat: str, espn_stat: str) -> pl.DataFrame:
    # Initialize Polars DF
    raw = pl.DataFrame()
    for i in ids:
        # Build URL + Game Label
        url = f'https://bv2-us.digitalsportstech.com/api/dfm/marketsByOu?sb=betonline&gameId={str(i)}&statistic={link_stat}'

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()

            # Check Data Loaded
            try:
                players_data = data[0]['players']

                # Build DF
                rows = []
                for player in players_data:
                    for market in player['markets']:
                        rows.append({
                            'BOL_game_id': market['game1Id'],
                            'week': week,
                            'player_name': player['name'],
                            'player_id': player['id'],
                            'team': player['team'],
                            'position': player['position']['title'],
                            'market_id': market['id'],
                            'condition': market['condition'],
                            'is_active': market['isActive'],
                            'is_actual': market['isActual'],
                            'type': 'Over' if market['type'] == 18 else 'Under' if market['type'] == 19 else str(market['type']),
                            'odds': market['odds'],
                            'value': market['value'],
                            'statistic': market['statistic']['title'],
                            'espn_stat': espn_stat
                        })
                raw = raw.vstack(
                    pl.DataFrame(rows)\
                        .with_columns([
                            (1/pl.col('odds')).alias('impProb')
                            ])
                    )
            except:
                print(f"Data Retreived with Error for BOL Game ID: {i} | Stat: {link_stat}")
        else:
            print(f"Failed to retrieve {link_stat} data for BOL Game ID: {i}. Status code: {response.status_code}")

    team_map = {
        'LVR': 'LV',
        'NOS': 'NO',
        'LAR': 'LA' 
    }

    if raw.height > 0:
        raw = raw.with_columns(
                pl.col("team").str.replace_many(
                    list(team_map.keys()),
                    list(team_map.values())
                )
                .alias('team'),
                pl.lit('OverUnder').alias('prop_source')
            )

        return raw
    else:
        return None

# Reconcile Full File
def reconcile_BOL(prop_df: pl.DataFrame, base_path="Data/Projections/BetOnline/Season/BetOnline_AllProps_Week_"):

    # Outline Pathway + Load
    all_path = "Data/Projections/BetOnline/Season/BetOnline_AllProps.parquet"
    all_df = pl.read_parquet(all_path)
    old_df_rows = all_df.height
    old_df_games = all_df['BOL_game_id'].n_unique()

    # Current Data
    prop_df = prop_df.with_columns(pl.col('week').cast(pl.Int32))
    join_cols = [col for col in all_df.columns if col not in 'BetTimeStamp']
    full_df = all_df.join(prop_df, on=join_cols, how='full', suffix='_new')
 
    coalesce_cols = [
         pl.coalesce([pl.col(col), pl.col(f"{col}_new")]).alias(col)
         for col in join_cols + ["BetTimeStamp"]
     ]
    
    final_df = full_df.select(coalesce_cols)
 
    df_filtered = (
         final_df.sort("BetTimeStamp", descending=True)
           .group_by(['week', 'player_name', 'position', 'team'])
           .agg(pl.all().first())
         )
    
    df_filtered = df_filtered.sort(by=['week', 'player_name', 'position', 'team'])

    # Metrics
    new_df_rows = df_filtered.height
    new_df_games = df_filtered['BOL_game_id'].n_unique()

    add_rows = new_df_rows - old_df_rows
    add_games = new_df_games - old_df_games

    # Save All
    df_filtered.write_parquet(all_path)
    print(f"All BetOnline Player Prop File Contains {new_df_rows} Rows")
    print(f"{add_rows} Rows Added to BetOnline Player Prop File ({add_games} New Games)")
    print("")

    # Save - Split Into Weeks:
    weeks_list = df_filtered['week'].unique().to_list()
    for w in weeks_list:
        # Get Week DataFrame
        week_df = df_filtered.filter(pl.col('week') == w)
        n_games = week_df['BOL_game_id'].n_unique()

        # Identify Week Path + Create Folder If Not Exists
        week_path = f"Data/Projections/BetOnline/Season/Week {w}/BetOnline_AllProps_Week_{w}.parquet"
        Path(week_path).parent.mkdir(parents=True, exist_ok=True)

        # Save as Parquet
        week_df.write_parquet(week_path)
        print(f"WEEK {w} Bet Online Player Prop File Contains {week_df.height} Rows ({n_games} Games)")

# Get Stat by Name
def get_x_stat(stat = 'anytimeTouchdown'):
    # Load
    df = pl.read_parquet("Data/Projections/BetOnline/Landing/BetOnline_AllProps_Raw.parquet")\
           .filter(pl.col('espn_stat') == stat)\
           .drop('market_id', 'condition', 'is_active', 'is_actual')
    
    # Split
    df_ou = df.filter(pl.col('prop_source') == 'OverUnder')
    df_val = df.filter(pl.col('prop_source') == 'Values')

    # Handle OverUnder
    def ou_calc(df=df_ou):

        # Pivot
        df_wide = df.pivot(
            index=["BOL_game_id", "week", "player_name", "player_id", "team", "position", "statistic", "espn_stat", "prop_source", "value"],
            on="type",
            values=["odds", "impProb"]
        )

        # Clean
        clean = df_wide \
            .sort(by=['BOL_game_id', 'player_name', 'value'], descending=[False, False, True])\
            .with_columns([
                -1*(1 - (pl.col('impProb_Over') + (pl.col('impProb_Under')))).alias('Juice'),
                (1 / pl.col("impProb_Over") - 1).alias('Over_Juice'),
                (1 / pl.col("impProb_Under") - 1).alias('Under_Juice'),
                (((1 / pl.col("impProb_Under") - 1)) - ((1 / pl.col("impProb_Over") - 1))).alias("Juice_Diff"),
            ])\
            .with_columns([
                (pl.col('value') + (pl.col('Juice_Diff') * pl.col('value') * pl.lit(0.5))).alias(f'proj_{stat}')
            ])
        
        return clean.select(['BOL_game_id', 'week','player_name', 'position', 'team', f'proj_{stat}'])

    # Handle Values
    def value_calc(df=df_val):
        over_cols = ['week', 'BOL_game_id', 'player_name', 'player_id', 'team', 'position', 'statistic', 'espn_stat']
        clean = (
            df
            .sort(by=['BOL_game_id', 'player_name', 'value'], descending=[False, False, True])
            .with_columns(
                pl.when(pl.col('impProb') == pl.col('value').max().over(over_cols))
                  .then(pl.col('impProb'))
                  .otherwise(pl.col('impProb') - pl.col("impProb").shift(1).over(over_cols))
                  .alias('exactProb')
            )
            .with_columns(
                pl.coalesce(pl.col('exactProb'), pl.col('impProb'))
            )
            .with_columns([
                (pl.col('value') * pl.col('exactProb')).alias(f'proj_{stat}_vals')
            ])
            .group_by(over_cols)
            .agg(
                pl.col(f'proj_{stat}_vals').sum()
            )
        )

        return clean.select(['BOL_game_id', 'week','player_name', 'position', 'team', f'proj_{stat}_vals'])
    
    if df_ou.height > 0:
        clean_ou = ou_calc()
        clean_vals = value_calc()

        # Combine
        final_df = clean_ou.join(clean_vals, on=['BOL_game_id', 'week', 'player_name', 'position', 'team'])
        final_df = final_df\
            .with_columns([
                pl.coalesce([ pl.col(f'proj_{stat}'), pl.col(f'proj_{stat}_vals')]).alias(f'proj_{stat}')
            ])
        
        final_df = final_df.drop(f'proj_{stat}_vals').sort(by=[f'proj_{stat}'])
    else:
        final_df = value_calc().with_columns(pl.col(f'proj_{stat}_vals').alias(f'proj_{stat}')).drop(f'proj_{stat}_vals').sort(by=[f'proj_{stat}'])

    return final_df

# Clean Final Dataframe
def clean_bol(stats_list = list(stats.keys())):
    final_result = None
    for stat in stats_list:
        result = get_x_stat(stat)

        if final_result is None:
            final_result = result
        else:
            final_result = final_result.join(result, on=['BOL_game_id','player_name', 'team', 'week', 'position'], how='full')
            final_result = final_result.with_columns([
                pl.coalesce(pl.col('BOL_game_id'), 'BOL_game_id_right'),
                pl.coalesce(pl.col('player_name'), 'player_name_right'),
                pl.coalesce(pl.col('team'), 'team_right'),
                pl.coalesce(pl.col('week'), 'week_right'),
                pl.coalesce(pl.col('position'), 'position_right'),
                ]).drop('BOL_game_id_right','player_name_right', 'team_right', 'week_right', 'position_right')
            
    final_result = final_result.with_columns([
        (pl.when(pl.col('position').is_in(['QB', 'RB'])).then(pl.col('proj_anytimeTouchdown')).otherwise(pl.lit(0))).alias('proj_rushingTouchdowns'),
        (pl.when(pl.col('position').is_in(['WR', 'TE'])).then(pl.col('proj_anytimeTouchdown')).otherwise(pl.lit(0))).alias('proj_receivingTouchdowns')
    ])

    Long_Sched = CURRENT_SCHED.unpivot(
        index=["NFL_game_id", "week", "officialDate"],
        on=["Home", "Away"],
        variable_name="Location",
        value_name="team"
    )

    final_result = (
        final_result
        .with_columns(pl.lit(datetime.now()).alias('BetTimeStamp'))
        .drop('proj_anytimeTouchdown')
        .join(Long_Sched, on=['team', 'week'], how="left")
    )

    return final_result


## Execute

# Create Current IDs and Schedule
BOL_IDs = get_week_ids(sched = SLIM_SCHED, week_num = week, id_start=id_var)
CURRENT_SCHED = SLIM_SCHED.filter(pl.col('week') == week)


# Get BOL Data By Game
full_df_schema = {
    "BOL_game_id": pl.Int64,
    "week": pl.Int64,
    "player_name": pl.Utf8,
    "player_id": pl.Int64,
    "team": pl.Utf8,
    "position": pl.Utf8,
    "market_id": pl.Int64,
    "condition": pl.Int64,
    "is_active": pl.Boolean,
    "is_actual": pl.Boolean,
    "type": pl.Utf8,
    "odds": pl.Float64,
    "value": pl.Float64,
    "statistic": pl.Utf8,
    "espn_stat": pl.Utf8,
    "impProb": pl.Float64,
    "prop_source": pl.Utf8
}
full_df = pl.DataFrame(schema = full_df_schema)


for espn, bol in stats.items():
    df = get_BOL_data(ids=BOL_IDs[week],link_stat = bol,espn_stat=espn)
    if df is not None:
        full_df = full_df.vstack(df)

# Get BOL Data By Game
for espn, bol in stats.items():
    df = get_BOL_data_OU(ids=BOL_IDs[week],link_stat = bol,espn_stat=espn)
    if df is not None:
        full_df = full_df.vstack(df)

with pl.Config(tbl_cols=-1):
    print(full_df)

## Peek And Save Raw Data
full_df.write_parquet("Data/Projections/BetOnline/Landing/BetOnline_AllProps_Raw.parquet")


BOL_STATS = clean_bol()
BOL_STATS.write_parquet("Data/Projections/BetOnline/Landing/BetOnline_AllProps_Clean.parquet")
BOL_STATS.write_csv("Data/Projections/BetOnline/Landing/BetOnline_AllProps_Clean.csv")

# Reconcile
reconcile_BOL(prop_df=BOL_STATS)

print(BOL_STATS)
