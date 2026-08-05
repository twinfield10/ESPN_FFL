# GetNFL.R
#
# Refreshes the NFL reference data the Python pipeline depends on:
#   Data/NFL_Schedules.csv          <- source of truth for current season + week
#   Data/NFL_Tackles_By_Position.csv <- solo/assist split used for IDP scoring
#   Data/NFL/NFL_Stats.csv           <- season player stats
#
# Run this first each week, and first of all when rolling over to a new season.
# See docs/SEASON_ROLLOVER.md.
#
# Usage:
#   Rscript R/GetNFL.R          # uses the default SEASON below
#   Rscript R/GetNFL.R 2026     # explicit season

# install.packages(c('tidyverse', 'nflfastR', 'nflreadr'))
library(tidyverse)
library(nflfastR)

# --- Season -------------------------------------------------------------
# Was hardcoded to 2025 in three separate places. Takes an optional CLI
# argument; otherwise defaults to the current NFL season (which rolls over in
# September, matching the June cutover in Scripts/fetch_utils.py).
args <- commandArgs(trailingOnly = TRUE)
SEASON <- if (length(args) >= 1) as.integer(args[1]) else {
  today <- Sys.Date()
  if (as.integer(format(today, "%m")) >= 7) {
    as.integer(format(today, "%Y"))
  } else {
    as.integer(format(today, "%Y")) - 1L
  }
}
message(sprintf("Loading NFL data for the %d season", SEASON))

# --- Paths --------------------------------------------------------------
# Previously setwd() to a hardcoded absolute home directory, and wrote two files
# with escaped Windows separators ("./\\Data\\NFL\\...") which on macOS created
# files whose *names* contained literal backslashes at the repo root.
# Resolve relative to this script instead.
script_path <- tryCatch(
  normalizePath(sys.frame(1)$ofile, mustWork = TRUE),
  error = function(e) NA_character_
)
repo_root <- if (!is.na(script_path)) {
  dirname(dirname(script_path))
} else {
  normalizePath(".", mustWork = TRUE)
}

data_dir <- file.path(repo_root, "Data")
# Season-scoped, matching Data/Projections/<source>/Season/<year>/ on the Python
# side. Output paths carried no season component before 2026, so each new season
# silently overwrote the last.
nfl_dir  <- file.path(data_dir, "NFL", as.character(SEASON))
dir.create(nfl_dir, recursive = TRUE, showWarnings = FALSE)

# --- Schedules ----------------------------------------------------------
# Regular season only. What this filter actually excludes is the *postseason* --
# game_type is one of REG / WC / DIV / CON / SB, 272 + 13 games. Verified against
# 2023-2026: nflreadr::load_schedules() never returns preseason games, so there is
# no PRE to drop. The validation below is what protects against that changing.
#
# Do NOT filter on !is.na(total_line) here. That filter came out of interactive
# exploration (see R/.Rhistory) and no Python consumer reads total_line, but it
# quietly wrecks this file's real job. Betting totals are only posted a few weeks
# ahead, so pre-season it kept 51 of 272 games -- weeks 1-4 only. This file is the
# source of truth for current season and current week, and DATE_WEEK is
# left-joined in scrape_pinnacle.py to assign a week to each prop, so missing
# weeks silently produce null weeks rather than an error. The column is still
# written; it is just not a filter.
schedules <- nflreadr::load_schedules(SEASON) %>%
  filter(game_type == 'REG')

# Validate before writing. This file drives current season and current week
# everywhere, and DATE_WEEK is left-joined in scrape_pinnacle.py to assign a week
# to each prop -- so a schedule that is truncated, or that carries anything other
# than regular-season games, produces silently wrong weeks rather than an error.
# Writing a bad file is worse than writing none, so this stops rather than warns.
stopifnot(
  "schedule is empty"                          = nrow(schedules) > 0,
  "schedule covers more than one season"       = length(unique(schedules$season)) == 1,
  "schedule contains non-regular-season games" = all(schedules$game_type == "REG"),
  "schedule has weeks outside 1-18"            = all(schedules$week >= 1 & schedules$week <= 18)
)
if (nrow(schedules) < 250) {
  stop(sprintf(paste0(
    "Only %d regular-season games for %d; a full season is 272.\n",
    "  Refusing to write a truncated schedule -- week detection would break for\n",
    "  the missing weeks. Check whether nflreadr has full data for this season."
  ), nrow(schedules), SEASON))
}

write_csv(schedules, file = file.path(data_dir, "NFL_Schedules.csv"))
message(sprintf(
  "  NFL_Schedules.csv: %d games, weeks %d-%d, game_type %s",
  nrow(schedules), min(schedules$week), max(schedules$week),
  paste(unique(schedules$game_type), collapse = "/")
))

# --- Player stats -------------------------------------------------------
# calculate_stats() needs play-by-play, which does not exist until the season
# actually starts. Pre-season that is expected rather than an error -- but it used
# to abort the script here with a stack trace, after the schedule was written and
# before the tackle ratios were. So a rollover run always appeared to fail, and
# whether it had done its most important job was not obvious.
stats <- tryCatch(
  nflfastR::calculate_stats(SEASON, "season", "player"),
  error = function(e) {
    message(sprintf("  no play-by-play for %d yet: %s",
                    SEASON, conditionMessage(e)))
    NULL
  }
)

if (is.null(stats)) {
  message("")
  message(sprintf("  Skipped NFL/%d/NFL_Stats.csv and NFL_Tackles_By_Position.csv.", SEASON))
  message("  The schedule above is written and is what drives season/week detection.")
  message("  NFL_Tackles_By_Position.csv keeps its previous values; league-wide tackle")
  message("  ratios are stable year to year, so the prior season's remain usable.")
  message("  Re-run this once games have been played.")
  message("Done.")
  quit(save = "no", status = 0)
}

# --- Tackle split by defensive position (IDP scoring) -------------------
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
  filter(!is.na(pos)) %>%
  select(pos, tackle_ratio)

write_csv(def_pos_tkl, file = file.path(data_dir, "NFL_Tackles_By_Position.csv"))
message(sprintf("  NFL_Tackles_By_Position.csv: %d positions", nrow(def_pos_tkl)))

# --- Save stats ---------------------------------------------------------
# The previous version also wrote a `season_stats` object that was never
# defined anywhere in the script, so that line always errored.
write_csv(stats, file = file.path(nfl_dir, "NFL_Stats.csv"))
message(sprintf("  NFL/%d/NFL_Stats.csv: %d rows", SEASON, nrow(stats)))

message("Done.")
