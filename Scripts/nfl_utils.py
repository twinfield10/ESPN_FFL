import polars as pl

NFL_SCHEDULE = pl.read_csv('Data/NFL_Schedules.csv')
#STATS_WEEK = pl.read_csv('Data/NFL/NFL_Stats.csv')
#STATS_SEASON = pl.read_csv('Data/NFL/NFL_Season_Stats.csv')
DATE_WEEK = NFL_SCHEDULE[['gameday', 'week']].unique()

print(NFL_SCHEDULE.filter(pl.col('away_score') == 'NA')['week'].min())

