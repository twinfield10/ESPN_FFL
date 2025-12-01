#install.packages('nflfastR')
library(tidyverse)
library(nflfastR)

setwd("/Users/tommywinfield/GitRepos/ESPN_FFL")

## Fantasy Football Data
schedules <- nflreadr::load_schedules(2025) %>%
  filter(game_type == 'REG') %>%
  filter(!is.na(total_line))
write_csv(schedules, file = "./Data/NFL_Schedules.csv")

pbp <- nflfastR::load_pbp(2025)

stats <- calculate_stats(2025, "season", "player")

def_pos_tkl <- stats %>%
  filter(position_group %in% c('DL', 'DB', 'LB')) %>%
  mutate(
    pos = case_when(
      position_group == 'LB' ~ 'LB',
      position == 'DE' ~ 'DE',
      position %in% c('DL', 'NT', 'DT') ~ 'DT',
      position %in% c('CB') ~ 'CB',
      position %in% c('SAF', 'S', 'FS', 'DB') ~ 'S',
      TRUE ~ NA
    )
  ) %>%
  group_by(pos) %>%
  summarise(
    solo = sum(def_tackles_solo, na.rm = TRUE),
    assists = sum(def_tackle_assists, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    solo_pct = solo / (solo + assists),
    assists_pct = assists / (solo + assists),
    tackle_ratio = solo / assists
  ) %>%
  select(pos, tackle_ratio)

write_csv(def_pos_tkl, file = "./Data/NFL_Tackles_By_Position.csv")

# Save Datasets
write_csv(stats, file = ".\\Data\\NFL\\NFL_Stats.csv")
write_csv(season_stats, file = ".\\Data\\NFL\\NFL_Season_Stats.csv")

rm(pbp)
