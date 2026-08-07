# GetContext.R
#
# Pulls the availability and role data the usage models need:
#   Data/NFL/<season>/rosters_weekly.parquet  <- nflreadr::load_rosters_weekly
#   Data/NFL/<season>/injuries.parquet        <- nflreadr::load_injuries
#   Data/NFL/<season>/snap_counts.parquet     <- nflreadr::load_snap_counts
#   Data/NFL/<season>/depth_charts.parquet    <- nflreadr::load_depth_charts
#   Data/NFL/<season>/context_meta.json       <- release timestamps and row counts
#
# Availability is the priority, and the reason is measured rather than assumed.
# docs/plans/16-usage-data-layer.md step 0 fitted the crudest usage model and
# failed its accuracy gate -- but decomposing the failure showed that on rows where
# the player actually took snaps the effect was -0.16% to +0.35%. Essentially the
# whole deficit was not knowing who plays. Out is 100% deterministic, Doubtful
# 99.2%, and a Questionable player's practice column splits 57% missed against 22%.
# No projection source in this pipeline models any of that.
#
# Usage:
#   Rscript R/GetContext.R              # current season only
#   Rscript R/GetContext.R 2016 2026    # explicit range, backfill
#
# Split from GetUsage.R because the two have different upstream availability, and
# that difference is load-bearing rather than cosmetic. Checked live 2026-08-07:
#
#   load_rosters_weekly(2026)  ->  2,930 rows, week 1, updated that morning
#   load_injuries(2026)        ->  error: seasons <= most_recent_season()
#   load_snap_counts(2026)     ->  error: same
#   load_depth_charts(2026)    ->  error: same
#
# nflreadr's most_recent_season() is 2025 while most_recent_season(roster = TRUE)
# is 2026. So the pre-season roster is the *only* current-season context that
# exists before week 1, and it is a good one: team (so a change of team is
# visible), status, years_exp, entry_year and draft_number all arrive with it.
# Everything else is history for fitting on, which is what a pre-season model
# needs it for anyway.
#
# Two upstream schemas are not what plan 16 assumed, both verified live:
#
#   * snap_counts has no gsis_id. It keys on pfr_player_id and a player name, so
#     joining it to anything in this repo goes through a pfr_id -> gsis_id map.
#     Use Data/NFL/player_ids.parquet, not rosters_weekly's pfr_id: measured over
#     all ten seasons on the QB/RB/WR/TE snap population, the crosswalk resolves
#     98.7-99.5% every year while rosters manages 71.2% in 2016 and only reaches
#     98.9% in 2025, because rosters' own pfr_id is 45.9% populated in 2016
#     against 75.0% in 2025. Joining through rosters would have made snap share
#     quietly unusable in exactly the early training seasons.
#   * depth_charts has no week column. It is now a timestamped snapshot log --
#     221 distinct `dt` values for 2025, some of them stamped March 2026 -- and it
#     carries espn_id and gsis_id directly. It is pulled as it comes; choosing a
#     snapshot per week is a feature-layer decision, not an extraction one. For a
#     season model, rosters_weekly's depth_chart_position and ngs_position are the
#     cheaper role signal.
#
# See docs/plans/16-usage-data-layer.md.

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
# Same July cutover as GetNFL.R, GetUsage.R and Scripts/fetch_utils.py, so "no
# argument" means the same season everywhere.
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
message(sprintf("Context data for %d-%d", FIRST, LAST))

#: Regular season only, matching GetUsage.R. All three week-keyed sets carry
#: weeks 19-22, and a fantasy pipeline never scores them.
MAX_WEEK <- 18L

#' Millisecond-free release timestamp of an nflreadr frame.
#'
#' @param x A frame returned by an nflreadr loader.
#' @return Character timestamp, or NA when the attribute is absent.
release_timestamp <- function(x) {
  ts <- attr(x, "nflverse_timestamp")
  if (is.null(ts)) NA_character_ else as.character(ts)
}

#' Read an nflverse release asset directly, bypassing nflreadr's season guard.
#'
#' `load_depth_charts(2026)` refuses with "seasons <= most_recent_season() is not
#' TRUE" because `most_recent_season()` tracks the *game* season, which is 2025 until
#' week 1. The refusal is about the guard, not the data: checked live 2026-08-07, the
#' 2026 depth-chart asset holds **410,431 rows across 140 daily snapshots running to
#' that morning**, and it already lists the rookies -- Jeremiyah Love is RB1 for
#' Arizona ahead of Allgeier and Conner.
#'
#' That is the only pre-season depth chart there is, so it is worth reaching past the
#' guard for. Only for depth charts: injuries and snap counts genuinely do not exist
#' before games are played, and their assets return empty.
#'
#' @param dataset Release name, e.g. "depth_charts".
#' @param season Season year.
#' @return The frame, or NULL when the asset is absent or unreadable.
load_release_asset <- function(dataset, season) {
  url <- sprintf(
    "https://github.com/nflverse/nflverse-data/releases/download/%s/%s_%d.parquet",
    dataset, dataset, season)
  out <- tryCatch(
    suppressWarnings(nflreadr::load_from_url(url)),
    error = function(e) NULL
  )
  if (is.null(out) || nrow(out) == 0) NULL else out
}

#' Load one nflreadr dataset, treating "not published yet" as expected.
#'
#' Each of the four has to be guarded separately: rosters is served for a season
#' the other three are not, which is the whole reason this script exists as its
#' own file.
#'
#' @param label Name used in messages.
#' @param fn Loader, called as `fn(season)`.
#' @param season Season year.
#' @return The frame, or NULL when upstream has nothing for that season.
load_optional <- function(label, fn, season) {
  out <- tryCatch(fn(season), error = function(e) {
    message(sprintf("  %-16s not published for %d yet (%s)", label, season,
                    sub("\n.*", "", conditionMessage(e))))
    NULL
  })
  if (is.null(out) || nrow(out) == 0) NULL else out
}

#' Keep regular-season rows, whichever column this season's schema uses.
#'
#' The column is not stable across seasons. `injuries` carries `season_type` for
#' 2025 and `game_type` for 2016-2024; `snap_counts` carries `game_type`
#' throughout; the 2025 `depth_charts` carries neither. Naming one of them and
#' hoping is how a ten-season backfill dies on its first season, which is what the
#' first version of this did.
#'
#' @param df A frame from nflreadr.
#' @param season Season year, stamped as an integer.
#' @return `df`, filtered to regular-season weeks 1-MAX_WEEK where it can be.
filter_regular_season <- function(df, season) {
  df$season <- as.integer(season)
  for (column in c("game_type", "season_type")) {
    if (column %in% names(df)) {
      df <- df[df[[column]] == "REG", , drop = FALSE]
      break
    }
  }
  if ("week" %in% names(df)) {
    df$week <- as.integer(df$week)
    df <- df[!is.na(df$week) & df$week >= 1 & df$week <= MAX_WEEK, , drop = FALSE]
  }
  df
}

#' Write a frame, having checked it is worth writing.
#'
#' Validation before the write, matching GetNFL.R and GetUsage.R: a truncated or
#' wrongly-keyed file is worse than no file, because every consumer downstream
#' treats what is on disk as the season.
#'
#' `key` varies by dataset rather than being fixed at (season, week, gsis_id),
#' because two of these four cannot supply that: snap_counts has no gsis_id and
#' depth_charts has no week.
#'
#' @param df Frame to write.
#' @param path Destination parquet.
#' @param season The season it should contain.
#' @param label Name used in messages.
#' @param key Columns that must be unique together. NULL skips the check.
#' @return Invisibly, `df`.
write_checked <- function(df, path, season, label, key = NULL) {
  stopifnot(
    "frame is empty"                 = nrow(df) > 0,
    "no season column"               = "season" %in% names(df),
    "more than one season in a file" = length(unique(df$season)) == 1,
    "wrong season"                   = unique(df$season) == season
  )
  if ("week" %in% names(df)) {
    stopifnot("weeks outside 1-18" = all(df$week >= 1 & df$week <= MAX_WEEK))
  }
  if (!is.null(key)) {
    # A duplicated key fans out rows on every downstream join, which silently
    # multiplies a player's usage rather than erroring.
    stopifnot("duplicated key" =
                nrow(dplyr::distinct(df, dplyr::across(dplyr::all_of(key)))) == nrow(df))
  }
  arrow::write_parquet(df, path)
  weeks <- if ("week" %in% names(df)) {
    sprintf("  weeks %d-%d", min(df$week), max(df$week))
  } else {
    "  not week-keyed"
  }
  message(sprintf("  %-16s %7d rows x %3d cols%s", label, nrow(df), ncol(df), weeks))
  invisible(df)
}

wrote_any <- FALSE

for (season in SEASONS) {
  message(sprintf("\n=== %d ===", season))
  nfl_dir <- file.path(repo_root, "Data", "NFL", as.character(season))
  stamps <- list()
  counts <- list()

  # --- Rosters ----------------------------------------------------------
  # The one that exists pre-season, and the join hub for the other three: it is
  # the only frame carrying gsis_id, espn_id and pfr_id together.
  rosters <- load_optional("rosters_weekly", nflreadr::load_rosters_weekly, season)
  if (!is.null(rosters)) {
    stamps$rosters_weekly <- release_timestamp(rosters)
    rosters <- rosters %>%
      filter_regular_season(season) %>%
      filter(!is.na(gsis_id), gsis_id != "")
    # Measured on 2025: 18 rows carry no gsis_id and 8 (season, week, gsis_id)
    # keys repeat -- a player listed twice in one week, e.g. after a mid-week
    # move. Keep the first rather than erroring; the duplicate rows are
    # identical on everything the models read.
    before <- nrow(rosters)
    rosters <- rosters %>% distinct(season, week, gsis_id, .keep_all = TRUE)
    if (nrow(rosters) < before) {
      message(sprintf("  %-16s dropped %d duplicate season/week/gsis_id row(s)",
                      "rosters_weekly", before - nrow(rosters)))
    }
    counts$rosters_weekly <- nrow(rosters)
  }

  # --- Injuries ---------------------------------------------------------
  # The availability model's training data.
  injuries <- load_optional("injuries", nflreadr::load_injuries, season)
  if (!is.null(injuries)) {
    stamps$injuries <- release_timestamp(injuries)
    injuries <- injuries %>%
      filter_regular_season(season) %>%
      filter(!is.na(gsis_id), gsis_id != "") %>%
      distinct(season, week, gsis_id, .keep_all = TRUE)
    counts$injuries <- nrow(injuries)
  }

  # --- Snap counts ------------------------------------------------------
  # No gsis_id upstream, so it is keyed on pfr_player_id and joined through
  # rosters_weekly's pfr_id in the feature layer.
  snaps <- load_optional("snap_counts", nflreadr::load_snap_counts, season)
  if (!is.null(snaps)) {
    stamps$snap_counts <- release_timestamp(snaps)
    snaps <- snaps %>%
      filter_regular_season(season) %>%
      filter(!is.na(pfr_player_id), pfr_player_id != "") %>%
      distinct(season, week, pfr_player_id, .keep_all = TRUE)
    counts$snap_counts <- nrow(snaps)
  }

  # --- Depth charts -----------------------------------------------------
  # Two schemas, and which one you get depends on the season. Verified live
  # 2026-08-07: 2016-2024 are week-keyed, 15 columns, with `depth_position`;
  # 2025 is a timestamped snapshot log, 12 columns, with `dt` and `pos_rank` and
  # no week at all. Upstream rewrote the feed for the current season only.
  #
  # Written as it comes, with the shape recorded in context_meta.json. That
  # record is the point: a feature built on `depth_position` fits happily on nine
  # seasons of training data and then finds the column missing in the season it
  # has to predict. For a season model, rosters_weekly's `depth_chart_position`
  # is the role signal to use -- it is present and consistent in every season
  # including 2026.
  depth <- load_optional("depth_charts", nflreadr::load_depth_charts, season)
  if (is.null(depth)) {
    # The guard, not the data -- see load_release_asset.
    depth <- load_release_asset("depth_charts", season)
    if (!is.null(depth)) {
      message(sprintf(
        "  %-16s read past nflreadr's season guard from the release asset (%d rows)",
        "depth_charts", nrow(depth)))
    }
  }
  depth_shape <- NULL
  if (!is.null(depth)) {
    stamps$depth_charts <- release_timestamp(depth)
    depth <- depth %>%
      filter_regular_season(season) %>%
      filter(!is.na(gsis_id), gsis_id != "")
    depth_shape <- if ("week" %in% names(depth)) "weekly" else "snapshot_log"
    counts$depth_charts <- nrow(depth)
  }

  if (length(counts) == 0) {
    message(sprintf("  skipped %d entirely -- nothing published yet.", season))
    next
  }

  # --- Write ------------------------------------------------------------
  dir.create(nfl_dir, recursive = TRUE, showWarnings = FALSE)
  if (!is.null(rosters)) {
    write_checked(rosters, file.path(nfl_dir, "rosters_weekly.parquet"),
                  season, "rosters_weekly", c("season", "week", "gsis_id"))
  }
  if (!is.null(injuries)) {
    write_checked(injuries, file.path(nfl_dir, "injuries.parquet"),
                  season, "injuries", c("season", "week", "gsis_id"))
  }
  if (!is.null(snaps)) {
    write_checked(snaps, file.path(nfl_dir, "snap_counts.parquet"),
                  season, "snap_counts", c("season", "week", "pfr_player_id"))
  }
  if (!is.null(depth)) {
    write_checked(depth, file.path(nfl_dir, "depth_charts.parquet"),
                  season, "depth_charts", NULL)
  }

  # How much of the snap population the pfr_id -> gsis_id join can reach, over the
  # fantasy positions rather than all of them: snap_counts covers linemen too, and
  # a rate diluted by players no model will ever score says nothing. Reported
  # because snap_counts is the one set with no id in common with anything else
  # here, so a silent collapse of that join is the failure mode worth watching.
  #
  # Measured against the crosswalk, which is the hub the feature layer uses --
  # rosters' own pfr_id is far sparser in the early seasons. See the header.
  snap_join_pct <- NA_real_
  if (!is.null(snaps)) {
    crosswalk_path <- file.path(repo_root, "Data", "NFL", "player_ids.parquet")
    hub <- if (file.exists(crosswalk_path)) {
      arrow::read_parquet(crosswalk_path, col_select = "pfr_id")$pfr_id
    } else {
      message("  no player_ids.parquet, so the snap join is measured against ",
              "rosters' sparser pfr_id. Generate it with Rscript R/GetPlayerIDs.R")
      if (is.null(rosters)) character(0) else rosters$pfr_id
    }
    hub <- hub[!is.na(hub)]
    fantasy <- snaps$pfr_player_id[snaps$position %in% c("QB", "RB", "WR", "TE")]
    if (length(fantasy) > 0) {
      snap_join_pct <- 100 * mean(unique(fantasy) %in% hub)
    }
  }

  meta <- list(
    season = season,
    pulled_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    rows = counts,
    nflverse_timestamps = stamps,
    snap_counts_resolved_to_gsis_pct = snap_join_pct,
    # "weekly" or "snapshot_log" -- see the depth-charts block. Recorded so a
    # feature layer can refuse the mismatch instead of discovering it at predict
    # time.
    depth_charts_shape = depth_shape,
    # Recorded because the availability model behaves differently pre-season: with
    # no injury report there is nothing to read, and plan 18's fallback to
    # trailing games-missed applies.
    has_current_injury_report = !is.null(injuries)
  )
  writeLines(jsonlite::toJSON(meta, auto_unbox = TRUE, pretty = TRUE,
                              null = "null"),
             file.path(nfl_dir, "context_meta.json"))
  if (!is.na(snap_join_pct)) {
    message(sprintf("  %.1f%% of QB/RB/WR/TE snap rows resolve to a gsis_id",
                    snap_join_pct))
  }
  wrote_any <- TRUE
}

if (!wrote_any) {
  message("")
  message("  Nothing written: no season in the requested range has data yet.")
  message("  That is the expected pre-season state, not a failure.")
}
message("\nDone.")
