# GetAdvanced.R
#
# Pulls the role-resolving data that docs/plans/22-feature-research.md tests:
#   Data/NFL/<season>/routes.parquet     <- participation x play-by-play dropbacks
#   Data/NFL/<season>/ngs.parquet        <- Next Gen Stats, receiving + rushing
#   Data/NFL/<season>/red_zone.parquet   <- play-by-play, by field position band
#   Data/NFL/<season>/advanced_meta.json <- release timestamps and row counts
#   Data/NFL/contracts.parquet           <- OverTheCap, not season-scoped
#
# Why these three and not others. Plans 18 and 21 between them measured out coach
# priors, team strength, game script, vacated opportunity share and finer depth
# rank, and arrived at a general finding: team-level context does not survive to
# player level, because role variance dominates it. The only feature that has ever
# moved this model is the one that resolves *role* -- the depth chart, worth +0.048
# R-squared on veteran carries. So this script pulls three more ways to measure
# role directly, rather than another way to describe a player's situation.
#
#   * routes      -- how often a player was actually on the field to be thrown to,
#                    which is role stripped of whether the quarterback liked him.
#   * ngs         -- where he ran his routes and how open he got, which is the
#                    first evidence the efficiency head has ever had. That head is
#                    currently pure shrinkage toward a positional constant.
#   * red_zone    -- who gets the ball inside the 10. Touchdown rates carry the
#                    heaviest shrinkage in the model (k = 120 to 300), which is to
#                    say the model presently treats them as almost pure noise. They
#                    are noisy *as rates*; goal-line role is not.
#
# Contracts are here rather than in GetContext.R because they are neither weekly
# nor season-keyed -- a contract signed in 2023 is the live fact about a player in
# 2026 -- and because filtering them to "signed strictly before the season being
# projected" is a leakage decision that belongs in the feature layer, not here.
#
# Usage:
#   Rscript R/GetAdvanced.R              # current season only
#   Rscript R/GetAdvanced.R 2016 2025    # explicit range, backfill
#
# Cost. Routes and red zone both need full play-by-play, which is ~50 MB a season
# cold and the reason this is not folded into GetUsage.R: that script's callers
# expect it to finish. A cold ten-season backfill is a few minutes; warm it is
# seconds. NGS and contracts are small.
#
# Upstream availability, checked live 2026-08-08 against nflreadr 1.4.1:
#
#   load_participation(2016..2025)   ->  45,184 to 48,513 rows, every season
#   load_nextgen_stats(2016..2025)   ->  present for receiving and rushing
#   load_contracts()                 ->  51,858 rows, 434 skill signings for 2026
#   load_pfr_advstats(2016)          ->  absent; the feed starts in 2018
#   load_ftn_charting(2021)          ->  absent; the feed starts in 2022
#
# The last two are why neither is pulled here. PFR advanced duplicates most of what
# NGS carries and is two seasons short of the training window; FTN starts in 2022,
# and four seasons is thin for a walk-forward that trains from 2016. Both were
# considered and set aside on coverage, not on merit -- see plan 22.
#
# See docs/plans/22-feature-research.md.

# install.packages(c('nflreadr', 'nflfastR', 'arrow', 'dplyr', 'tidyr', 'jsonlite'))
suppressMessages({
  library(dplyr)
  library(tidyr)
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

# --- Seasons ------------------------------------------------------------
# Same July cutover as GetUsage.R and GetNFL.R, so "no argument" means the same
# season everywhere in this repo.
args <- commandArgs(trailingOnly = TRUE)
current_season <- function() {
  today <- Sys.Date()
  year <- as.integer(format(today, "%Y"))
  if (as.integer(format(today, "%m")) >= 7) year else year - 1L
}
FIRST <- if (length(args) >= 1) as.integer(args[1]) else current_season()
LAST  <- if (length(args) >= 2) as.integer(args[2]) else FIRST
stopifnot("first season must not be after last" = FIRST <= LAST)
SEASONS <- FIRST:LAST
message(sprintf("Advanced usage data for %d-%d", FIRST, LAST))

#: Regular season only, matching GetUsage.R. A fantasy pipeline never scores
#: weeks 19-22, and including them would put playoff teams' extra games into
#: per-game denominators.
MAX_WEEK <- 18L

#: Field-position bands, in yards from the opponent's goal line. Three rather than
#: one because they answer different questions: inside the 20 is "scoring
#: territory" and mostly measures whether the offence is good, inside the 5 is
#: "who is the goal-line back" and is the role signal this is here for.
RZ_BANDS <- c(rz20 = 20L, rz10 = 10L, rz5 = 5L)

#' Millisecond-free release timestamp of an nflreadr frame.
#'
#' @param x A frame returned by an nflreadr loader.
#' @return Character timestamp, or NA when the attribute is absent.
release_timestamp <- function(x) {
  ts <- attr(x, "nflverse_timestamp")
  if (is.null(ts)) NA_character_ else as.character(ts)
}

#' Load something that may not exist upstream yet, without aborting the run.
#'
#' Pre-season and mid-backfill both produce legitimate absences, and the caller
#' has to be able to tell them from a real failure -- hence the message rather
#' than a silent NULL.
#'
#' @param label Name used in messages.
#' @param loader Function of one argument, the season.
#' @param season Season year.
#' @return The frame, or NULL when upstream has nothing.
load_optional <- function(label, loader, season) {
  out <- tryCatch(loader(season), error = function(e) {
    message(sprintf("  no %s for %d: %s", label, season, conditionMessage(e)))
    NULL
  })
  if (is.null(out) || nrow(out) == 0) NULL else out
}

#' Write a player-week frame, having checked it is worth writing.
#'
#' Same contract as GetUsage.R's writer, for the same reason: every consumer
#' downstream treats what is on disk as the season, so a truncated or wrongly
#' keyed file is worse than no file.
#'
#' @param df Frame to write.
#' @param path Destination parquet.
#' @param season The season it should contain.
#' @param label Name used in messages.
#' @return Invisibly, `df`.
write_checked <- function(df, path, season, label) {
  stopifnot(
    "frame is empty"                 = nrow(df) > 0,
    "no gsis_id column"              = "gsis_id" %in% names(df),
    "more than one season in a file" = length(unique(df$season)) == 1,
    "wrong season"                   = unique(df$season) == season,
    "weeks outside 1-18"             = all(df$week >= 1 & df$week <= MAX_WEEK),
    "duplicated season/week/gsis_id" =
      nrow(dplyr::distinct(df, season, week, gsis_id)) == nrow(df)
  )
  arrow::write_parquet(df, path)
  message(sprintf("  %-12s %7d rows x %2d cols  weeks %d-%d",
                  label, nrow(df), ncol(df), min(df$week), max(df$week)))
  invisible(df)
}

#' Routes run, derived from participation and play-by-play.
#'
#' There is no routes column anywhere in public NFL data. What there is, in
#' `load_participation`, is `offense_players` -- a semicolon-separated list of the
#' gsis_ids on the field for a play. Restricted to dropbacks, being on the field is
#' a route in all but the handful of cases where a back or tight end stays in to
#' block, so this is a good approximation and a well-worn one.
#'
#' The join is on `old_game_id` and `play_id`. Participation carries both
#' `nflverse_game_id` and `old_game_id`; play-by-play's `old_game_id` is numeric
#' where participation's is character, which is why both sides are cast before the
#' join rather than after a silent zero-row result.
#'
#' @param season Season year.
#' @param pbp Play-by-play for that season, already loaded.
#' @return A player-week frame, or NULL when participation is absent.
build_routes <- function(season, pbp) {
  part <- load_optional("participation", nflreadr::load_participation, season)
  if (is.null(part)) return(NULL)

  dropbacks <- pbp %>%
    filter(qb_dropback == 1, !is.na(posteam)) %>%
    select(old_game_id, play_id, week)

  part$old_game_id <- as.character(part$old_game_id)
  dropbacks$old_game_id <- as.character(dropbacks$old_game_id)

  joined <- part %>%
    select(old_game_id, play_id, possession_team, offense_players) %>%
    inner_join(dropbacks, by = c("old_game_id", "play_id"))

  # A zero-row join here means the id formats drifted upstream, which is silent
  # and catastrophic -- the feature would simply be missing for that season.
  stopifnot("participation did not join to play-by-play" = nrow(joined) > 0)

  team_dropbacks <- joined %>%
    count(season = season, week, posteam = possession_team, name = "team_dropbacks")

  joined %>%
    select(week, possession_team, offense_players) %>%
    separate_rows(offense_players, sep = ";") %>%
    filter(offense_players != "", !is.na(offense_players)) %>%
    count(season = season, week, posteam = possession_team,
          gsis_id = offense_players, name = "routes") %>%
    left_join(team_dropbacks, by = c("season", "week", "posteam")) %>%
    mutate(route_share = routes / team_dropbacks) %>%
    select(season, week, gsis_id, posteam, routes, team_dropbacks, route_share)
}

#' Next Gen Stats, receiving and rushing, at weekly grain.
#'
#' Weekly rows aggregated later rather than NGS's own `week == 0` season rows, and
#' the difference matters: the season rows only cover players who met NGS's
#' qualifying threshold, which measured out at 86% of the 30-plus-target
#' population. The weekly rows cover everyone who did anything.
#'
#' Receiving and rushing are separate feeds with disjoint columns, so they are
#' prefixed and full-joined -- a player can appear in one and not the other, and
#' an inner join would silently drop every receiving back.
#'
#' @param season Season year.
#' @return A player-week frame, or NULL when neither feed exists.
build_ngs <- function(season) {
  rec <- load_optional("ngs receiving",
                       function(s) nflreadr::load_nextgen_stats(s, "receiving"),
                       season)
  rush <- load_optional("ngs rushing",
                        function(s) nflreadr::load_nextgen_stats(s, "rushing"),
                        season)
  if (is.null(rec) && is.null(rush)) return(NULL)

  tidy <- function(df, keep) {
    if (is.null(df)) return(NULL)
    df %>%
      filter(season_type == "REG", week >= 1, week <= MAX_WEEK,
             !is.na(player_gsis_id), player_gsis_id != "") %>%
      mutate(season = as.integer(season), week = as.integer(week)) %>%
      rename(gsis_id = player_gsis_id) %>%
      select(season, week, gsis_id, all_of(names(keep))) %>%
      rename(!!!setNames(names(keep), unname(keep))) %>%
      distinct(season, week, gsis_id, .keep_all = TRUE)
  }

  rec_cols <- c(avg_cushion = "ngs_cushion",
                avg_separation = "ngs_separation",
                avg_intended_air_yards = "ngs_adot",
                percent_share_of_intended_air_yards = "ngs_air_yards_share",
                avg_yac_above_expectation = "ngs_yac_oe",
                avg_expected_yac = "ngs_expected_yac")
  rush_cols <- c(efficiency = "ngs_rush_efficiency",
                 percent_attempts_gte_eight_defenders = "ngs_stacked_box_pct",
                 avg_time_to_los = "ngs_time_to_los",
                 rush_yards_over_expected_per_att = "ngs_ryoe_per_att",
                 rush_pct_over_expected = "ngs_rush_pct_oe")

  rec_t <- tidy(rec, rec_cols)
  rush_t <- tidy(rush, rush_cols)

  if (is.null(rec_t)) return(rush_t)
  if (is.null(rush_t)) return(rec_t)
  full_join(rec_t, rush_t, by = c("season", "week", "gsis_id"))
}

#' Carries and targets by field-position band, with team totals.
#'
#' Team totals travel with the player rows so the feature layer can build shares
#' without a second pass over play-by-play. Shares are the point: five goal-line
#' carries on a team with eight is a role, five on a team with forty is not.
#'
#' End-zone targets are counted as `air_yards >= yardline_100`, which is the
#' standard approximation for a ball thrown into the end zone. It is not exact --
#' a back-shoulder throw at the pylon can miss it -- but it separates the players
#' who are thrown to for scores from the players who are not, which is the job.
#'
#' @param season Season year.
#' @param pbp Play-by-play for that season, already loaded.
#' @return A player-week frame.
build_red_zone <- function(season, pbp) {
  plays <- pbp %>% filter(!is.na(posteam))

  # Long form, one row per (player, play), so carries and targets share a code
  # path and the band logic is written once.
  touches <- bind_rows(
    plays %>%
      filter(rush_attempt == 1, !is.na(rusher_player_id), rusher_player_id != "") %>%
      transmute(week, posteam, gsis_id = rusher_player_id, yardline_100,
                kind = "carry", into_ez = FALSE),
    plays %>%
      filter(pass_attempt == 1, !is.na(receiver_player_id), receiver_player_id != "") %>%
      transmute(week, posteam, gsis_id = receiver_player_id, yardline_100,
                kind = "target",
                into_ez = !is.na(air_yards) & air_yards >= yardline_100)
  ) %>%
    filter(!is.na(yardline_100))

  banded <- touches %>%
    mutate(rz20 = yardline_100 <= RZ_BANDS[["rz20"]],
           rz10 = yardline_100 <= RZ_BANDS[["rz10"]],
           rz5  = yardline_100 <= RZ_BANDS[["rz5"]])

  per_player <- banded %>%
    group_by(week, posteam, gsis_id) %>%
    summarise(
      rz20_carries = sum(rz20 & kind == "carry"),
      rz10_carries = sum(rz10 & kind == "carry"),
      rz5_carries  = sum(rz5  & kind == "carry"),
      rz20_targets = sum(rz20 & kind == "target"),
      rz10_targets = sum(rz10 & kind == "target"),
      rz5_targets  = sum(rz5  & kind == "target"),
      ez_targets   = sum(into_ez & kind == "target"),
      .groups = "drop"
    )

  per_team <- per_player %>%
    group_by(week, posteam) %>%
    summarise(across(where(is.numeric), sum, .names = "team_{.col}"), .groups = "drop")

  per_player %>%
    left_join(per_team, by = c("week", "posteam")) %>%
    mutate(season = as.integer(season)) %>%
    filter(rz20_carries + rz20_targets + ez_targets > 0) %>%
    select(season, week, gsis_id, posteam, everything())
}

# --- Contracts ----------------------------------------------------------
# Pulled once, outside the season loop, because it is not season-scoped: the live
# fact about a 2026 player may be a contract he signed in 2022. `year_signed` is
# kept so the feature layer can filter to "signed strictly before the season being
# projected", which is where that leakage decision belongs.
#
# `apy_cap_pct` rather than `apy` is what the features should use -- annual value
# as a share of the salary cap is comparable across a decade in which the cap went
# from $155M to $280M, and raw dollars are not.
message("\n=== contracts ===")
contracts <- tryCatch(nflreadr::load_contracts(), error = function(e) {
  message(sprintf("  contracts unavailable: %s", conditionMessage(e)))
  NULL
})
if (!is.null(contracts) && nrow(contracts) > 0) {
  contracts_ts <- release_timestamp(contracts)
  contracts <- contracts %>%
    filter(!is.na(gsis_id), gsis_id != "", !is.na(year_signed), year_signed > 0) %>%
    select(gsis_id, player, position, team, year_signed, years, value, apy,
           guaranteed, apy_cap_pct, inflated_apy, inflated_guaranteed,
           draft_year, draft_round, draft_overall) %>%
    arrange(gsis_id, year_signed)
  arrow::write_parquet(contracts,
                       file.path(repo_root, "Data", "NFL", "contracts.parquet"))
  message(sprintf("  contracts    %7d rows x %2d cols  signed %d-%d  release %s",
                  nrow(contracts), ncol(contracts),
                  min(contracts$year_signed), max(contracts$year_signed),
                  contracts_ts))
}

# --- Per-season ---------------------------------------------------------
wrote_any <- FALSE

for (season in SEASONS) {
  message(sprintf("\n=== %d ===", season))
  nfl_dir <- file.path(repo_root, "Data", "NFL", as.character(season))
  stamps <- list()
  counts <- list()

  # Play-by-play is loaded once and shared by routes and red zone. It is the
  # expensive part of this script by an order of magnitude, and loading it twice
  # would double a ten-season backfill for nothing.
  pbp <- load_optional("play-by-play", nflreadr::load_pbp, season)
  if (!is.null(pbp)) {
    stamps$pbp <- release_timestamp(pbp)
    pbp <- pbp %>%
      filter(season_type == "REG", week <= MAX_WEEK) %>%
      mutate(week = as.integer(week))
  }
  if (is.null(pbp) || nrow(pbp) == 0) {
    message(sprintf("  skipped %d entirely -- no play-by-play yet.", season))
    next
  }

  dir.create(nfl_dir, recursive = TRUE, showWarnings = FALSE)

  routes <- build_routes(season, pbp)
  if (!is.null(routes)) {
    write_checked(routes, file.path(nfl_dir, "routes.parquet"), season, "routes")
    counts$routes <- nrow(routes)
  }

  ngs <- build_ngs(season)
  if (!is.null(ngs)) {
    write_checked(ngs, file.path(nfl_dir, "ngs.parquet"), season, "ngs")
    counts$ngs <- nrow(ngs)
  }

  red_zone <- build_red_zone(season, pbp)
  if (!is.null(red_zone)) {
    write_checked(red_zone, file.path(nfl_dir, "red_zone.parquet"), season, "red_zone")
    counts$red_zone <- nrow(red_zone)
  }

  meta <- list(
    season = season,
    pulled_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    nflverse_timestamps = stamps,
    rows = counts
  )
  writeLines(jsonlite::toJSON(meta, auto_unbox = TRUE, pretty = TRUE, null = "null"),
             file.path(nfl_dir, "advanced_meta.json"))
  wrote_any <- TRUE
}

if (!wrote_any) {
  message("")
  message("  Nothing written: no season in the requested range has play-by-play yet.")
  message("  That is the expected pre-season state, not a failure.")
}
message("\nDone.")
