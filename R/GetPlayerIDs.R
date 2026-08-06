# GetPlayerIDs.R
#
# Writes the cross-provider player identity table:
#   Data/NFL/player_ids.parquet
#
# This is the ID crosswalk the pipeline has never had. Player matching everywhere
# else is (week, player_name) string equality, patched by ~140 hand-curated rename
# entries that need annual re-curation. A miss silently drops a player -- and a
# *collision* silently misattributes one, which is worse. GOP Degenerates' 2,503-
# player pool contains 16 shared names, including two Lamar Jacksons (a Ravens
# quarterback and a cornerback) and two Justin Jeffersons (a Vikings receiver and a
# Browns linebacker).
#
# nflreadr::load_ff_playerids() carries gsis_id, espn_id, fantasypros_id,
# sleeper_id, yahoo_id and sportradar_id together, and is refreshed daily upstream.
# It is the join key for play-by-play data (gsis_id) and for the ESPN store and
# draft board (espn_id) at the same time.
#
# Usage:
#   Rscript R/GetPlayerIDs.R
#
# Cheap enough to re-run whenever; rookies appear in it through the off-season.
# See docs/plans/16-usage-data-layer.md.

# install.packages(c('nflreadr', 'arrow', 'dplyr'))
suppressMessages(library(dplyr))

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
dir.create(nfl_dir, recursive = TRUE, showWarnings = FALSE)
out_path <- file.path(nfl_dir, "player_ids.parquet")

# --- Load ---------------------------------------------------------------
raw <- nflreadr::load_ff_playerids()
message(sprintf("  load_ff_playerids: %d rows x %d cols", nrow(raw), ncol(raw)))

# Only the columns the pipeline joins on or needs to sanity-check a join. Keeping
# it narrow makes the file small enough to commit, which makes the crosswalk
# auditable in git the way Data/Scoring/scoring.csv is.
KEEP <- c("name", "merge_name", "position", "team", "birthdate", "draft_year",
          "gsis_id", "espn_id", "fantasypros_id", "sleeper_id", "yahoo_id",
          "sportradar_id", "pfr_id")

ids <- raw %>%
  select(any_of(KEEP)) %>%
  # Every id arrives as character upstream; keep it that way. espn_id is numeric
  # in ESPN's own payloads, so the Python side casts on read rather than here --
  # doing it in R would turn a missing id into NA silently.
  mutate(across(any_of(c("gsis_id", "espn_id", "fantasypros_id", "sleeper_id",
                         "yahoo_id", "sportradar_id", "pfr_id")), as.character))

# --- Report ambiguity rather than resolving it here ---------------------
# Upstream has a handful of genuine data errors where two different players share
# an id -- e.g. Nate Jones (WR) and Nathan Jones (CB) both carry espn_id 5730 and
# gsis_id 00-0022828. They are all long-retired, but a naive merge on a duplicated
# key fans out rows, which would silently duplicate players on a draft board.
#
# The dedup decision belongs to the reader, which knows what it is joining, so this
# only counts them. Scripts/crosswalk.py refuses to build a lookup from an
# ambiguous key.
for (col in c("gsis_id", "espn_id", "fantasypros_id")) {
  if (!col %in% names(ids)) next
  present <- ids[[col]][!is.na(ids[[col]])]
  dupes <- length(present) - length(unique(present))
  message(sprintf("  %-15s %5d non-null, %d duplicated",
                  col, length(present), dupes))
}

both <- ids %>% filter(!is.na(gsis_id), !is.na(espn_id))
message(sprintf("  usable gsis<->espn pairs: %d", nrow(both)))

stopifnot(
  "crosswalk is empty"                  = nrow(ids) > 0,
  "no gsis_id column"                   = "gsis_id" %in% names(ids),
  "no espn_id column"                   = "espn_id" %in% names(ids),
  "implausibly few gsis<->espn pairs"   = nrow(both) > 5000
)

arrow::write_parquet(ids, out_path)
message(sprintf("  wrote Data/NFL/player_ids.parquet: %d rows x %d cols",
                nrow(ids), ncol(ids)))
message("Done.")
