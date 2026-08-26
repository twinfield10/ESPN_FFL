#!/usr/bin/env bash
#
# run_daily_refresh.sh -- pull the current-season inputs, rebuild the boards, and
# publish them to S3.
#
# The depth chart is the reason this exists. It is the only feature that has ever
# moved the season model (+0.048 R-squared on veteran carries, against roughly
# nothing for every situational feature plan 21 and plan 22 measured), it is the
# only current-season input nflreadr will serve before week 1, and it changes every
# day through camp. A board built on a three-day-old depth chart is a board that has
# not noticed a position battle resolving.
#
# What it does NOT do, deliberately:
#
#   * No historical nflverse backfill. R/GetUsage.R and R/GetAdvanced.R cover
#     2016-2025 and R/GetPBP.R covers 1999-2025, and completed seasons do not
#     change. Re-pulling them daily would download ~600 MB of play-by-play to
#     arrive at identical files. The play-by-play archive *is* pulled nightly, but
#     for the current season only -- that one does change, every week.
#   * No Google Sheets render. populateGoogleSheet.py spends ~9 minutes asleep in
#     rate-limit backoff, and pre-season there is nothing weekly to publish. Add it
#     once the season starts if you want the phone view refreshed nightly.
#   * No board rebuild when a pull failed. That is the whole point of the guard
#     below -- see "Why this fails loudly".
#   * No run at all off a branch other than main -- see "Why this refuses to run
#     off main". The board is published from merged code or not at all.
#
# Why this fails loudly. This repo's recurring failure mode, documented across plans
# 01, 03 and 22, is an absent source reading as agreement: something upstream stops
# answering, the pipeline carries on with stale or imputed data, and the output looks
# entirely normal. A nightly job is the ideal place for that to happen unnoticed for
# a month. So: `set -e` stops at the first failure, the board is only rebuilt if
# every pull succeeded, and the run ends by checking that the depth chart actually
# moved rather than that the command exited zero.
#
# Why this refuses to run off main. There is no `git checkout` anywhere below, so
# this runs whatever branch happens to be checked out. A branch that changes the
# projections -- a MODEL_VERSION bump forcing a refit, a new blend input -- would
# therefore reach all nine boards and S3 through cron alone, unmerged and untyped.
# The branch check is the first thing the run does, before any pull.
#
# Install (matches the existing crontab pattern):
#
#   chmod +x run_daily_refresh.sh
#   crontab -e
#   # ESPN FFL daily data refresh (6am -- overnight roster moves have landed)
#   0 6 * * * /Users/tommywinfield/bin/espn_ffl_nightly.sh
#
# Cron points at the wrapper, not at this file. The branch check below is defence in
# depth and is absent from any branch cut before it existed -- which is exactly the
# checkout it would need to catch. `ops/espn_ffl_nightly.sh` (live at
# ~/bin/espn_ffl_nightly.sh) sits outside the repo, so no checkout can remove it.
#
# Note on a sleeping laptop: cron does not run missed jobs on wake, so a night with
# the lid shut is a night skipped. That is usually fine here -- the next run catches
# up, because every pull is a full snapshot rather than an increment. If you want
# guaranteed daily execution, launchd's StartCalendarInterval does fire on wake.

set -euo pipefail

# Derived from this script's own location, never hardcoded. Cron runs a second
# checkout pinned to origin/main (see ops/espn_ffl_nightly.sh); a hardcoded path
# would send that copy back into the interactive tree and rebuild the boards off
# whatever branch happened to be checked out there -- the exact failure the
# separate checkout exists to prevent.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="/usr/local/bin/python3.11"
RSCRIPT="/usr/local/bin/Rscript"
LOG_DIR="${HOME}/logs"
LOG="${LOG_DIR}/espn_ffl_refresh.log"

STATUS="${REPO}/Data/refresh_status.json"

mkdir -p "${LOG_DIR}"
cd "${REPO}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG}"; }

# Status is written on **both** outcomes, and that is the point. A run that fails
# silently and a run that never happened are indistinguishable from the outside, and
# `Scripts/refresh_status.py` exists to tell them apart -- which it can only do if
# failure leaves a record rather than just a non-zero exit into cron's void.
write_status() {
  printf '{"result": "%s", "at": "%s", "stage": "%s", "season": "%s"}\n' \
    "$1" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "${2:-}" "${SEASON:-unknown}" \
    > "${STATUS}"
}

fail() {
  log "FAILED: $*"
  write_status "failed" "$*"
  # A failed nightly run should be visible without anyone opening a log. The
  # notification covers the case where the machine was awake and something broke;
  # the status file covers the case where it was asleep and nothing ran at all,
  # which no notification can ever report.
  /usr/bin/osascript -e "display notification \"$* — see ~/logs/espn_ffl_refresh.log\" with title \"ESPN FFL refresh failed\"" 2>/dev/null || true
  exit 1
}

log "=== daily refresh starting ==="

# --- 0. The revision, before anything else ------------------------------
# This script has no `git checkout`. It runs the tree it sits in, re-projects the
# usage head, rebuilds nine boards and pushes them to S3 -- so whatever is checked
# out here is what gets published.
#
# The check is against `origin/main` rather than a branch *name*, for two reasons.
# The nightly checkout is deliberately detached (a branch cannot be checked out in
# two worktrees at once), so a name test would reject the very tree meant to run.
# And a name test would pass on a local `main` carrying unpushed commits, which is
# not reviewed code.
#
# Belt to ops/espn_ffl_nightly.sh's braces: the wrapper already resets this tree to
# origin/main before calling. This catches the other route in -- someone running
# this script by hand from the interactive checkout, on a branch.
#
# ALLOW_ANY_BRANCH=1 overrides, for a deliberate run off an unpublished revision.
BRANCH="$(git -C "${REPO}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
HEAD_SHA="$(git -C "${REPO}" rev-parse HEAD 2>/dev/null || echo unknown)"
MAIN_SHA="$(git -C "${REPO}" rev-parse origin/main 2>/dev/null || echo unknown)"

if [ "${ALLOW_ANY_BRANCH:-0}" = "1" ]; then
  log "WARNING: revision check overridden, running ${BRANCH} at ${HEAD_SHA:0:8}"
elif [ "${HEAD_SHA}" = "unknown" ] || [ "${MAIN_SHA}" = "unknown" ]; then
  fail "cannot resolve HEAD or origin/main in ${REPO} -- refusing to publish blind."
elif [ "${HEAD_SHA}" != "${MAIN_SHA}" ]; then
  fail "${REPO} is at ${HEAD_SHA:0:8} (${BRANCH}), not origin/main ${MAIN_SHA:0:8} \
-- boards publish from reviewed, pushed main only. Set ALLOW_ANY_BRANCH=1 to override."
else
  log "revision ${HEAD_SHA:0:8} (${BRANCH}), matching origin/main"
fi

# The season comes from config.yaml rather than being hardcoded, so the annual
# rollover is one edit in one place. See docs/SEASON_ROLLOVER.md.
SEASON="$("${PYTHON}" -c "
from Scripts.config_utils import load_config
print(load_config()['season'])
" 2>/dev/null)" || fail "could not read season from config.yaml"
log "season ${SEASON}"

# The depth-chart snapshot as it stands now, to compare against afterwards. Read
# before the pull so a pull that quietly returns yesterday's file is visible.
depth_snapshot() {
  "${PYTHON}" -c "
import polars as pl
from Scripts.paths import nfl_season_dir
p = nfl_season_dir(${SEASON}, 'depth_charts.parquet')
if not p.is_file():
    print('absent')
else:
    d = pl.read_parquet(p)
    col = next((c for c in ('dt', 'last_updated') if c in d.columns), None)
    print(d[col].max() if col else f'{d.height} rows')
" 2>/dev/null || echo "unreadable"
}
BEFORE="$(depth_snapshot)"
log "depth chart before: ${BEFORE}"

# --- 1. Rosters, depth charts, injuries ---------------------------------
# GetContext.R handles nflreadr refusing injuries and snap counts for a season it
# does not consider current -- that is expected pre-season, not a failure, and the
# script exits 0 on it. Rosters and depth charts are served daily and are what
# matter here.
log "pulling rosters, depth charts, injuries (R/GetContext.R)"
"${RSCRIPT}" R/GetContext.R "${SEASON}" "${SEASON}" >>"${LOG}" 2>&1 \
  || fail "R/GetContext.R"

# --- 1b. Play-by-play archive -------------------------------------------
# Current season only. Completed seasons do not change, and re-pulling 1999-2025
# nightly would download ~600 MB to arrive at identical files -- backfill is an
# explicit `Rscript R/GetPBP.R 1999 2025`, run once.
#
# Before GetContext but after nothing in particular: it has no dependants inside
# this script. It runs here rather than at the end because `R/GetAdvanced.R` reads
# the archive it writes, so if that script is ever added to this nightly the
# ordering is already right.
#
# Pre-season this writes almost nothing -- the current season has no plays until
# week 1 -- and that is not a failure. GetPBP.R exits 0 having skipped the season.
log "pulling play-by-play archive (R/GetPBP.R)"
"${RSCRIPT}" R/GetPBP.R "${SEASON}" "${SEASON}" >>"${LOG}" 2>&1 \
  || fail "R/GetPBP.R"

# --- 2. ESPN injury report ----------------------------------------------
# A separate feed from nflreadr's, and the one that carries returnDate -- which the
# usage model uses to scale a player's expected games rather than zeroing him.
log "pulling ESPN injuries"
"${PYTHON}" -m Scripts.scrape_espn_injuries >>"${LOG}" 2>&1 \
  || fail "Scripts.scrape_espn_injuries"

# --- 2b. FantasyPros season projections ---------------------------------
# Added 2026-08-24, and it was the largest single data gap in the repo: this had never
# run nightly at all, so the board was blending FantasyPros numbers from whenever the
# scrape was last run by hand -- ten days stale when it was found.
#
# It also only returned 60 players until that day. Anonymously FantasyPros serves ten
# rows per position behind a registration fence; a free account lifts it to 592. The
# session lives in config.yaml under `fantasypros.cookie`.
#
# The row-count guard is the point. A cookie expires, and an expired cookie does not
# error -- it silently returns the ten-row teaser, which is exactly this repo's
# recurring failure mode of an absent source reading as agreement. So: fail loudly if
# the scrape comes back at teaser size.
log "pulling FantasyPros season projections"
"${PYTHON}" -m Scripts.scrape_FP --what season >>"${LOG}" 2>&1 \
  || fail "Scripts.scrape_FP"

FP_ROWS="$("${PYTHON}" -c "
import polars as pl
from Scripts.paths import season_dir
p = season_dir('FantasyPros', ${SEASON}, 'FantasyPros_Projections_Season.parquet')
print(pl.read_parquet(p).height if p.is_file() else 0)
" 2>/dev/null)" || FP_ROWS=0
log "FantasyPros rows: ${FP_ROWS}"
if [ "${FP_ROWS}" -le 60 ]; then
  fail "FantasyPros returned ${FP_ROWS} rows -- the registration fence is back, which \
means the session cookie in config.yaml has expired. Log in again and replace it."
fi

# --- 3. Re-project the usage head ---------------------------------------
# Reads the fresh depth chart and roster. Refits only if the stored model is stale;
# otherwise it reloads the fitted coefficients and re-predicts, which is what makes
# this cheap enough to run nightly.
log "re-projecting the usage model"
"${PYTHON}" -m Scripts.usage.project --season "${SEASON}" >>"${LOG}" 2>&1 \
  || fail "Scripts.usage.project"

# --- 4. Rebuild the boards ----------------------------------------------
# Only reached if every pull above succeeded. A board rebuilt on a failed pull is
# worse than no rebuild: it is stale data wearing a fresh timestamp.
log "rebuilding draft boards for all leagues"
"${PYTHON}" -m Scripts.refresh --all --what board >>"${LOG}" 2>&1 \
  || fail "Scripts.refresh --what board"

# --- 5. Verify the data actually moved ----------------------------------
# Exit code 0 means the commands ran. It does not mean upstream served anything new.
# This is the check that tells the two apart.
AFTER="$(depth_snapshot)"
log "depth chart after:  ${AFTER}"
if [ "${BEFORE}" = "${AFTER}" ]; then
  log "NOTE: depth chart did not advance. Fine on a quiet day or a re-run; worth a"
  log "      look if it repeats, since upstream publishes most weekdays in camp."
fi

COVERAGE="$("${PYTHON}" -c "
import json
from Scripts.paths import PROJECTIONS_DIR
p = PROJECTIONS_DIR / 'Usage' / 'Season' / '${SEASON}' / 'meta.json'
m = json.load(p.open())
print(f\"{m['projected']}/{m['rows']} ({100 * m['projected'] / m['rows']:.1f}%)\")
" 2>/dev/null)" || COVERAGE="unreadable"
log "usage coverage: ${COVERAGE}"

# --- 6. Push to S3 ------------------------------------------------------
# Last, and only on a clean run. Everything above either succeeded or called fail(),
# so reaching this line is the signal that the data is worth publishing -- which is
# the same guard that stops the boards being rebuilt on a failed pull, applied one
# step further along. S3 never receives stale data wearing a fresh timestamp.
#
# This also writes the dated board snapshot, which is the point of doing it here
# rather than by hand: `snapshots/board/season=/league=/date=/` accumulates one
# board per night, so the pre-season market becomes a time series instead of being
# overwritten every morning. Data/G2/ had to be built by hand precisely because that
# history did not exist -- see docs/plans/24-s3-data-flow.md.
log "pushing to S3"
"${PYTHON}" -m Scripts.sync --push >>"${LOG}" 2>&1 || fail "Scripts.sync --push"

write_status "ok" "complete"
log "=== daily refresh complete ==="
