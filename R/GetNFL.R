#install.packages('nflfastR')
library(tidyverse)
library(nflfastR)

## Fantasy Football Data
schedules <- nflreadr::load_schedules(2025) %>%
  filter(game_type == 'REG') %>%
  filter(!is.na(total_line))
write_csv(schedules, file = "./Data/NFL_Schedules.csv")

pbp <- nflfastR::load_pbp(2025)

stats <- nflfastR::load_player_stats(2025)

# Sum Stats
cols_to_sum <- c(
                'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs', 'passing_epa', 'passing_2pt_conversions',
                'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles', 'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa', 'rushing_2pt_conversions',
                'receptions', 'targets', 'receiving_yards', 'receiving_tds', 'receiving_fumbles', 'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch','receiving_first_downs', 'receiving_epa', 'receiving_2pt_conversions'
                )

# Other
cols_to_avg <- c('pacr', 'dakota', 'racr', 'target_share', 'air_yards_share', 'wopr')

season_stats <- stats %>%
  group_by(season, player_id, player_name, player_display_name, position, position_group) %>%
  summarise(
    # Sum Statistics
    across(all_of(cols_to_sum), ~ sum(., na.rm=TRUE)),
    across(all_of(cols_to_avg), ~ sum(., na.rm=TRUE)),
    .groups = 'drop'
  )

# Save Datasets
write_csv(stats, file = ".\\Data\\NFL\\NFL_Stats.csv")
write_csv(season_stats, file = ".\\Data\\NFL\\NFL_Season_Stats.csv")

