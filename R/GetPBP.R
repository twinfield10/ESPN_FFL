# GetPBP.R
#
# Keeps the play-by-play instead of throwing it away.
#
#   Data/NFL/<season>/pbp.parquet            <- every play, every column
#   Data/NFL/<season>/participation.parquet  <- who was on the field for it
#   Data/NFL/<season>/ftn_charting.parquet   <- FTN's manual charting, 2022+
#   Data/NFL/<season>/pfr_pass.parquet       <- pressures and blitzes, 2018+
#   Data/NFL/<season>/pfr_rush.parquet       <- yards before/after contact, 2018+
#   Data/NFL/<season>/pfr_rec.parquet        <- separation and drops, 2018+
#   Data/NFL/<season>/pbp_meta.json          <- release stamps, rows, coverage
#
# Why this exists
# ---------------
# GetAdvanced.R already downloads full play-by-play -- its own header calls it "the
# expensive part of this script by an order of magnitude" -- derives two narrow
# frames from it (routes, red zone) and then **discards the other 370 columns**.
# Every future feature that wants a play-level fact therefore has to re-download a
# season to get at it, which is why plan 22 tested eleven ideas and none of them
# were play-level.
#
# The frames GetAdvanced.R builds are aggregates chosen in advance. This one keeps
# the thing they were aggregated from, so the next question does not need a new
# download and a new R script -- it needs a group_by.
#
# What this reverses, and on what grounds
# ---------------------------------------
# GetAdvanced.R's header sets PFR and FTN aside explicitly: "PFR advanced duplicates
# most of what NGS carries and is two seasons short of the training window; FTN
# starts in 2022, and four seasons is thin for a walk-forward that trains from 2016.
# Both were considered and set aside on coverage, not on merit."
#
# That reasoning is about **fitting** them, and it still stands -- nothing here
# enters a fitted arm. It is not a reason to decline to *hold* them. Plan 32 wanted
# pressure rate and an offensive-line proxy to test whether a receiver's destination
# is a better spot for opportunity, found neither in the repo, and could not measure
# the question at all. Collected, the coverage objection becomes something a gate can
# answer instead of something a header asserts.
#
# Usage:
#   Rscript R/GetPBP.R              # current season only -- what the nightly runs
#   Rscript R/GetPBP.R 1999 2025    # explicit range, one-time backfill
#
# Cost. ~20 MB a season for play-by-play and ~3 MB for the rest, so 1999-2025 is
# roughly 600 MB on disk and in the S3 mirror -- against 40 MB for all of Data/NFL
# today. At S3 standard that is about a penny a month. The download is the real
# cost: a cold full backfill is ~20 minutes, warm it is seconds, and the nightly
# touches one season.
#
# Season types. **Unfiltered**, unlike every other pull in this repo. GetUsage.R and
# GetAdvanced.R filter to REG weeks 1-18 before writing, because a fantasy pipeline
# never scores weeks 19-22 and playoff games would corrupt per-game denominators.
# That is correct for a feature table and wrong for an archive: a filter applied at
# write time cannot be undone, and post-season snaps are real evidence about a
# player. The filter belongs at read time, and `Scripts.usage.nflverse.load_pbp`
# defaults to REG so every current caller sees exactly what it saw before.
#
# Upstream availability, checked live 2026-08-24 against nflreadr 1.4.1:
#
#   load_pbp(1999..2025)            ->  46,136 to 49,492 rows, 372 cols, every season
#   load_participation(2016..2024)  ->  45,919 to 48,434 rows; absent before 2016
#   load_ftn_charting(2022..2025)   ->  41,643 to 48,031 rows; absent before 2022
#   load_pfr_advstats(2018..2025)   ->  646 to 697 rows per stat type; absent before 2018
#
# See docs/plans/32-movers.md and docs/plans/22-feature-research.md.

# install.packages(c('nflreadr', 'arrow', 'dplyr', 'jsonlite'))
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
# Same July cutover as GetUsage.R, GetAdvanced.R and GetNFL.R, so "no argument"
# means the same season everywhere in this repo.
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
message(sprintf("Play-by-play archive for %d-%d", FIRST, LAST))

#: The per-stat-type PFR pulls. Keyed on player-season, not player-week, which is
#: why they are written as separate small files rather than joined to anything.
PFR_TYPES <- c(pass = "pass", rush = "rush", rec = "rec")

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
#' Absence is the normal case here rather than the exceptional one -- participation
#' starts in 2016, PFR in 2018, FTN in 2022, and a backfill to 1999 walks through
#' all three boundaries. The message is what separates "upstream has nothing" from
#' "this broke", which is the distinction this repo's failure mode turns on.
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

#' Write an archive frame, having checked it is worth writing.
#'
#' A deliberately weaker contract than GetAdvanced.R's `write_checked`, and the
#' difference is the point. That one asserts a `gsis_id` key, one season per file
#' and weeks inside 1-18, because it writes *feature tables* whose consumers treat
#' what is on disk as the season. This writes an *archive*: play-by-play is keyed on
#' play, carries no `gsis_id`, and keeps weeks 19-22 on purpose.
#'
#' What is still checked is the part that has actually gone wrong in this repo --
#' a truncated or wrongly-seasoned file landing where a good one was, and being
#' read as the season.
#'
#' @param df Frame to write.
#' @param path Destination parquet.
#' @param season The season it should contain.
#' @param label Name used in messages.
#' @param min_rows Fewest rows that can be a real pull.
#' @return Invisibly, `df`.
write_archive <- function(df, path, season, label, min_rows = 1L) {
  stopifnot(
    "frame is empty"                 = nrow(df) >= min_rows,
    "no season column"               = "season" %in% names(df),
    "more than one season in a file" = length(unique(df$season)) == 1,
    "wrong season"                   = unique(df$season) == season
  )
  arrow::write_parquet(df, path)
  size_mb <- round(file.info(path)$size / 1e6, 1)
  message(sprintf("  %-16s %7d rows x %3d cols  %5.1f MB",
                  label, nrow(df), ncol(df), size_mb))
  invisible(df)
}

# --- Pull ---------------------------------------------------------------
wrote_any <- FALSE

for (season in SEASONS) {
  message(sprintf("\n=== %d ===", season))
  nfl_dir <- file.path(repo_root, "Data", "NFL", as.character(season))
  stamps <- list()
  counts <- list()

  # Play-by-play first: if this season has none there is nothing to archive, and
  # every other pull here is an annotation on it.
  pbp <- load_optional("play-by-play", nflreadr::load_pbp, season)
  if (is.null(pbp)) {
    message(sprintf("  skipped %d entirely -- no play-by-play yet.", season))
    next
  }

  dir.create(nfl_dir, recursive = TRUE, showWarnings = FALSE)
  stamps$pbp <- release_timestamp(pbp)

  # A completed regular season is 47,000-odd plays. A few hundred means the feed is
  # mid-publish for a season in progress, which is fine and normal in September --
  # the guard is against a *silent truncation* replacing a complete file, so it only
  # refuses what could not be a real pull at all.
  write_archive(pbp %>% mutate(season = as.integer(season)),
                file.path(nfl_dir, "pbp.parquet"), season, "pbp", min_rows = 100L)
  counts$pbp <- nrow(pbp)
  counts$pbp_reg <- sum(pbp$season_type == "REG", na.rm = TRUE)
  wrote_any <- TRUE

  participation <- load_optional("participation", nflreadr::load_participation, season)
  if (!is.null(participation)) {
    stamps$participation <- release_timestamp(participation)
    write_archive(participation %>% mutate(season = as.integer(season)),
                  file.path(nfl_dir, "participation.parquet"), season,
                  "participation", min_rows = 100L)
    counts$participation <- nrow(participation)
  }

  ftn <- load_optional("ftn charting", nflreadr::load_ftn_charting, season)
  if (!is.null(ftn)) {
    stamps$ftn <- release_timestamp(ftn)
    write_archive(ftn %>% mutate(season = as.integer(season)),
                  file.path(nfl_dir, "ftn_charting.parquet"), season,
                  "ftn_charting", min_rows = 100L)
    counts$ftn_charting <- nrow(ftn)
  }

  for (name in names(PFR_TYPES)) {
    stat <- PFR_TYPES[[name]]
    adv <- load_optional(sprintf("pfr %s", stat),
                         function(s) nflreadr::load_pfr_advstats(s, stat), season)
    if (is.null(adv)) next
    stamps[[paste0("pfr_", name)]] <- release_timestamp(adv)
    write_archive(adv %>% mutate(season = as.integer(season)),
                  file.path(nfl_dir, sprintf("pfr_%s.parquet", name)), season,
                  sprintf("pfr_%s", name))
    counts[[paste0("pfr_", name)]] <- nrow(adv)
  }

  meta <- list(
    season = season,
    pulled_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    nflverse_timestamps = stamps,
    rows = counts,
    season_types = as.list(table(pbp$season_type))
  )
  writeLines(jsonlite::toJSON(meta, auto_unbox = TRUE, pretty = TRUE, null = "null"),
             file.path(nfl_dir, "pbp_meta.json"))
}

if (!wrote_any) {
  message("\nNothing written -- no season in range has play-by-play yet.")
} else {
  message("\nDone.")
}
