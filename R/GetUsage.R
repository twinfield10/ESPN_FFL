# GetUsage.R
#
# Pulls the nflverse usage data the usage models are built on:
#   Data/NFL/<season>/player_weeks.parquet   <- nflfastR::calculate_stats, weekly
#   Data/NFL/<season>/opportunity.parquet    <- ffopportunity expected production
#   Data/NFL/<season>/usage_meta.json        <- release timestamps and row counts
#
# `opportunity` is the centrepiece: it publishes expected production at the *stat*
# level (receptions_exp, rec_yards_gained_exp, pass_touchdown_exp, ...) alongside
# the actuals and their difference, which is the scoring-agnostic shape this repo
# needs -- projections are produced as stat lines and scored per league.
# `player_weeks` adds the usage shares (target_share, air_yards_share, wopr) and
# the IDP and kicking stats that ffopportunity does not model.
#
# Usage:
#   Rscript R/GetUsage.R              # current season only
#   Rscript R/GetUsage.R 2016 2025    # explicit range, backfill
#
# ~3s per season warm for opportunity; calculate_stats needs play-by-play, so a
# cold ten-season backfill downloads ~150 MB and takes a couple of minutes. Kept
# out of GetNFL.R deliberately -- the weekly schedule refresh has to stay fast.
#
# Not written here yet: routes.parquet, the participation x play-by-play route
# derivation. It is only needed by the feature layer, so it lands with
# docs/plans/16-usage-data-layer.md step 3 rather than with the step 0 gates this
# script exists to feed.
#
# See docs/plans/16-usage-data-layer.md.

# install.packages(c('nflreadr', 'nflfastR', 'arrow', 'dplyr'))
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

# --- Seasons ------------------------------------------------------------
# Same July cutover as GetNFL.R and Scripts/fetch_utils.py, so "no argument"
# means the same season everywhere.
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
message(sprintf("Usage data for %d-%d", FIRST, LAST))

#: The ffopportunity release to pull. Recorded in usage_meta.json alongside the
#: release timestamp, because this is release data rather than nflverse core --
#: if it stops updating mid-season the weekly model loses its best feature, and a
#: stale file has to be visible rather than silently reused.
MODEL_VERSION <- "latest"

#: Regular season only. ffopportunity carries weeks 19-22 (playoffs) with no
#: season_type column to filter on, and a fantasy pipeline never scores them.
MAX_WEEK <- 18L

#' Millisecond-free release timestamp of an nflreadr frame.
#'
#' nflreadr attaches `nflverse_timestamp` to everything it loads. It is the only
#' way to tell a fresh pull from a cached one that upstream stopped updating.
#'
#' @param x A frame returned by an nflreadr loader.
#' @return Character timestamp, or NA when the attribute is absent.
release_timestamp <- function(x) {
  ts <- attr(x, "nflverse_timestamp")
  if (is.null(ts)) NA_character_ else as.character(ts)
}

#' Write a frame, having checked it is worth writing.
#'
#' Validation before the write, matching GetNFL.R: a truncated or wrongly-keyed
#' file is worse than no file, because every consumer downstream treats what is
#' on disk as the season.
#'
#' @param df Frame to write.
#' @param path Destination parquet.
#' @param season The season it should contain.
#' @param label Name used in messages.
#' @return Invisibly, `df`.
write_checked <- function(df, path, season, label) {
  stopifnot(
    "frame is empty"                  = nrow(df) > 0,
    "no gsis_id column"               = "gsis_id" %in% names(df),
    "more than one season in a file"  = length(unique(df$season)) == 1,
    "wrong season"                    = unique(df$season) == season,
    "weeks outside 1-18"              = all(df$week >= 1 & df$week <= MAX_WEEK),
    # A duplicated (season, week, gsis_id) fans out rows on every downstream
    # join, which silently double-counts a player's usage rather than erroring.
    "duplicated season/week/gsis_id"  =
      nrow(dplyr::distinct(df, season, week, gsis_id)) == nrow(df)
  )
  arrow::write_parquet(df, path)
  message(sprintf("  %-16s %6d rows x %3d cols  weeks %d-%d",
                  label, nrow(df), ncol(df), min(df$week), max(df$week)))
  invisible(df)
}

wrote_any <- FALSE

for (season in SEASONS) {
  message(sprintf("\n=== %d ===", season))
  nfl_dir <- file.path(repo_root, "Data", "NFL", as.character(season))

  # --- Expected production ---------------------------------------------
  # Pre-season this file does not exist upstream, which is expected rather than
  # an error: no games have been played, so there is no usage to pull. Exiting 0
  # here is what lets a rollover run be scripted alongside GetNFL.R.
  opportunity <- tryCatch(
    nflreadr::load_ff_opportunity(season, stat_type = "weekly",
                                  model_version = MODEL_VERSION),
    error = function(e) {
      message(sprintf("  no expected-production data for %d yet: %s",
                      season, conditionMessage(e)))
      NULL
    }
  )
  if (is.null(opportunity) || nrow(opportunity) == 0) {
    message(sprintf("  skipped %d entirely -- re-run once games have been played.",
                    season))
    next
  }
  opportunity_ts <- release_timestamp(opportunity)

  # Three traps, all of them silent if not handled here:
  #
  # 1. `season` arrives as *character*. The join to player_weeks fails loudly on
  #    that, which is the good outcome, but the cast has to happen on read.
  # 2. ~420 rows a season carry a NULL player_id -- team-level opportunity that
  #    could not be attributed to anyone. They are the only source of duplicated
  #    (season, week, player_id) keys, and a join that keeps them fans out rows.
  # 3. Weeks 19-22 are the playoffs.
  opportunity <- opportunity %>%
    mutate(season = as.integer(season), week = as.integer(week)) %>%
    filter(week <= MAX_WEEK, !is.na(player_id), player_id != "") %>%
    rename(gsis_id = player_id)

  # --- Observed usage ---------------------------------------------------
  # calculate_stats needs play-by-play. It can be absent while ffopportunity is
  # present (or the reverse) so it gets its own guard rather than sharing one.
  player_weeks <- tryCatch(
    nflfastR::calculate_stats(season, "week", "player"),
    error = function(e) {
      message(sprintf("  no play-by-play for %d yet: %s",
                      season, conditionMessage(e)))
      NULL
    }
  )
  player_weeks_ts <- NA_character_
  if (!is.null(player_weeks) && nrow(player_weeks) > 0) {
    player_weeks_ts <- release_timestamp(player_weeks)
    player_weeks <- player_weeks %>%
      filter(season_type == "REG", week <= MAX_WEEK) %>%
      mutate(season = as.integer(season), week = as.integer(week)) %>%
      rename(gsis_id = player_id)
  } else {
    player_weeks <- NULL
  }

  # --- Write ------------------------------------------------------------
  dir.create(nfl_dir, recursive = TRUE, showWarnings = FALSE)
  write_checked(opportunity, file.path(nfl_dir, "opportunity.parquet"),
                season, "opportunity")
  if (!is.null(player_weeks)) {
    write_checked(player_weeks, file.path(nfl_dir, "player_weeks.parquet"),
                  season, "player_weeks")
  }

  # Reported in this direction on purpose. player_weeks covers every player who
  # appeared in a play -- linemen, kickers, defenders -- while opportunity only
  # models offensive skill production, so "what share of player_weeks has an
  # expected line" is about 30% and says nothing. The useful question is whether
  # every expected line can be joined to observed usage, which is what the
  # feature layer needs.
  matched <- if (is.null(player_weeks)) NA_real_ else
    100 * mean(paste(opportunity$week, opportunity$gsis_id) %in%
                 paste(player_weeks$week, player_weeks$gsis_id))

  # The release timestamps are the point of this file. ffopportunity is release
  # data, not nflverse core, so "the file on disk is three weeks old" has to be
  # answerable without re-pulling.
  meta <- list(
    season = season,
    pulled_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    ffopportunity_model_version = MODEL_VERSION,
    opportunity = list(rows = nrow(opportunity), cols = ncol(opportunity),
                       nflverse_timestamp = opportunity_ts),
    player_weeks = if (is.null(player_weeks)) NULL else
      list(rows = nrow(player_weeks), cols = ncol(player_weeks),
           nflverse_timestamp = player_weeks_ts),
    opportunity_matched_to_player_weeks_pct = matched
  )
  writeLines(jsonlite::toJSON(meta, auto_unbox = TRUE, pretty = TRUE,
                              null = "null"),
             file.path(nfl_dir, "usage_meta.json"))
  message(sprintf("  opportunity release %s", opportunity_ts))
  if (!is.na(matched)) {
    message(sprintf("  %.1f%% of expected lines join to observed usage", matched))
  }
  wrote_any <- TRUE
}

if (!wrote_any) {
  message("")
  message("  Nothing written: no season in the requested range has data yet.")
  message("  That is the expected pre-season state, not a failure.")
}
message("\nDone.")
