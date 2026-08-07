# GetCoaches.R
#
# Pulls the head-coach-per-game record the coaching features are built from:
#   Data/NFL/coaches_by_game.parquet   <- one row per team per game, with its coach
#   Data/NFL/team_names.parquet        <- abbreviation -> full name, for the
#                                          Wikipedia lookup Scripts/coaches.py does
#
# Per *game* rather than per season on purpose. A team-season does not have one head
# coach when someone is fired in October, and collapsing to one name here would throw
# away the only evidence of that. `Scripts/coaches.py` decides how to collapse it and
# records how many games each coach actually took.
#
# Usage:
#   Rscript R/GetCoaches.R              # 2010 to the current season
#   Rscript R/GetCoaches.R 2016 2026
#
# **This is not a trustworthy source for an unplayed season.** Checked live
# 2026-08-07: `load_schedules(2026)` records 7 of the offseason's coaching changes
# (BAL, CLE, LV, MIA, NYG, PIT, TEN) and misses Arizona's -- it still lists Jonathan
# Gannon where the real 2026 head coach is Mike LaFleur. Partially updated is worse
# than not updated, because it looks complete. So `Scripts/coaches.py` takes the
# current season from Wikipedia and uses this file only for seasons that have been
# played, where the coach who coached each game is a matter of record.
#
# See docs/plans/21-coaching-and-scheme.md.

# install.packages(c('nflreadr', 'arrow', 'dplyr'))
suppressMessages({
  library(dplyr)
})
options(nflreadr.verbose = FALSE, dplyr.summarise.inform = FALSE)

# --- Paths --------------------------------------------------------------
script_path <- tryCatch(
  normalizePath(sys.frame(1)$ofile, mustWork = TRUE),
  error = function(e) NA_character_
)
repo_root <- if (!is.na(script_path)) {
  dirname(dirname(script_path))
} else {
  normalizePath(".", mustWork = TRUE)
}
nfl_dir <- file.path(repo_root, "Data", "NFL")

# --- Seasons ------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
current_season <- function() {
  today <- Sys.Date()
  year <- as.integer(format(today, "%Y"))
  if (as.integer(format(today, "%m")) >= 7) year else year - 1L
}
FIRST <- if (length(args) >= 1) as.integer(args[1]) else 2010L
LAST  <- if (length(args) >= 2) as.integer(args[2]) else current_season()
stopifnot("first season must not be after last" = FIRST <= LAST)
message(sprintf("Coaches for %d-%d", FIRST, LAST))

# --- Coaches per team-game ----------------------------------------------
schedules <- nflreadr::load_schedules(FIRST:LAST)

by_game <- bind_rows(
  schedules %>%
    transmute(season = as.integer(season), week = as.integer(week), game_id,
              game_type, team = home_team, coach = home_coach, home = TRUE),
  schedules %>%
    transmute(season = as.integer(season), week = as.integer(week), game_id,
              game_type, team = away_team, coach = away_coach, home = FALSE)
) %>%
  filter(!is.na(team), team != "", !is.na(coach), coach != "") %>%
  arrange(season, week, team)

stopifnot(
  "no rows"                = nrow(by_game) > 0,
  "duplicated game/team"   =
    nrow(dplyr::distinct(by_game, game_id, team)) == nrow(by_game)
)

dir.create(nfl_dir, recursive = TRUE, showWarnings = FALSE)
arrow::write_parquet(by_game, file.path(nfl_dir, "coaches_by_game.parquet"))
message(sprintf("  coaches_by_game   %6d rows, %d seasons, %d distinct coaches",
                nrow(by_game), length(unique(by_game$season)),
                length(unique(by_game$coach))))

# How many team-seasons had more than one head coach -- the mid-season firings the
# per-game shape exists to preserve.
multi <- by_game %>%
  group_by(season, team) %>%
  summarise(coaches = n_distinct(coach), .groups = "drop") %>%
  filter(coaches > 1)
message(sprintf("  %d team-seasons had more than one head coach", nrow(multi)))

# --- Team names ---------------------------------------------------------
# `current = FALSE` so relocations are covered: OAK and LV, SD and LAC, STL/LA/LAR
# are all separate rows, which matters for a ten-season history.
teams <- nflreadr::load_teams(current = FALSE) %>%
  transmute(team_abbr, team_name, team_nick)
arrow::write_parquet(teams, file.path(nfl_dir, "team_names.parquet"))
message(sprintf("  team_names        %6d rows", nrow(teams)))

message("\nDone. Build the committed table with `python -m Scripts.coaches`.")
