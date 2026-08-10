#!/usr/bin/env bash
#
# run_daily_refresh.sh -- pull the current-season inputs and rebuild the boards.
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
#     2016-2025, and completed seasons do not change. Re-pulling them daily would
#     download ~500 MB of play-by-play to arrive at identical files.
#   * No Google Sheets render. populateGoogleSheet.py spends ~9 minutes asleep in
#     rate-limit backoff, and pre-season there is nothing weekly to publish. Add it
#     once the season starts if you want the phone view refreshed nightly.
#   * No board rebuild when a pull failed. That is the whole point of the guard
#     below -- see "Why this fails loudly".
#
# Why this fails loudly. This repo's recurring failure mode, documented across plans
# 01, 03 and 22, is an absent source reading as agreement: something upstream stops
# answering, the pipeline carries on with stale or imputed data, and the output looks
# entirely normal. A nightly job is the ideal place for that to happen unnoticed for
# a month. So: `set -e` stops at the first failure, the board is only rebuilt if
# every pull succeeded, and the run ends by checking that the depth chart actually
# moved rather than that the command exited zero.
#
# Install (matches the existing crontab pattern):
#
#   chmod +x run_daily_refresh.sh
#   crontab -e
#   # ESPN FFL daily data refresh (6am -- overnight roster moves have landed)
#   0 6 * * * /Users/tommywinfield/GitRepos/ESPN_FFL/run_daily_refresh.sh
#
# Note on a sleeping laptop: cron does not run missed jobs on wake, so a night with
# the lid shut is a night skipped. That is usually fine here -- the next run catches
# up, because every pull is a full snapshot rather than an increment. If you want
# guaranteed daily execution, launchd's StartCalendarInterval does fire on wake.

set -euo pipefail

REPO="/Users/tommywinfield/GitRepos/ESPN_FFL"
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

# --- 2. ESPN injury report ----------------------------------------------
# A separate feed from nflreadr's, and the one that carries returnDate -- which the
# usage model uses to scale a player's expected games rather than zeroing him.
log "pulling ESPN injuries"
"${PYTHON}" -m Scripts.scrape_espn_injuries >>"${LOG}" 2>&1 \
  || fail "Scripts.scrape_espn_injuries"

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

write_status "ok" "complete"
log "=== daily refresh complete ==="
