#!/usr/bin/env bash
#
# run_odds_refresh.sh -- pull sportsbook game lines and store what moved.
#
# Separate from run_daily_refresh.sh, and on a different clock, because it answers a
# different question. The nightly rebuilds the boards from projections that change
# once a day; a line moves all day, and the value of storing one is the *sequence*.
# Four runs a day is the cadence plan 36 asked for.
#
# What it does NOT do, deliberately:
#
#   * No board rebuild. Nothing here feeds the draft board directly -- game lines
#     reach it through `Scripts/vegas.py` and the special-teams heads, which the
#     nightly already re-runs. Rebuilding boards four times a day would republish
#     nine leagues to S3 to reflect a half-point move on one game.
#   * No S3 push. The odds store is local. It is a research artifact and a line
#     history, not something the app reads.
#   * **No EIP rotation, no shared cron lock, no seed-data fallback.** The repo this
#     borrows its adapters from runs on EC2 and answers an IP-block problem with
#     address rotation. Plan 36 says explicitly not to port that infrastructure, and
#     the probe that motivated it does not reproduce here: Pinnacle's league routes
#     answer 200 from this machine. Port the adapters, leave the plumbing.
#
# Why it fails loudly. Same reason as the nightly, and more sharply: this store is
# append-only, so a book that quietly stops answering writes exactly what a book with
# no line movement writes -- nothing. "No change" and "no data" are the same empty
# file. So the puller refuses an empty pull and asserts market coverage per book, and
# this script lets a non-zero exit stop the run rather than logging past it.
#
# Install:
#
#   chmod +x run_odds_refresh.sh
#   crontab -e
#   # Sportsbook game lines, four times a day
#   0 */6 * * * /Users/tommywinfield/GitRepos/ESPN_FFL/run_odds_refresh.sh
#
# Unlike the nightly this points at the working checkout rather than a second one
# pinned to origin/main. Nothing it writes is published, so a branch cannot ship a
# bad board -- the revision guard below is a warning rather than a refusal.
#
# 4Casters needs CAST4_USER and CAST4_PASS in the environment. Cron does not read
# your shell profile, so set them in the crontab or a file this sources. Without them
# the book is skipped with a reason and the run still succeeds.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="/usr/local/bin/python3.11"
LOG_DIR="${HOME}/logs"
LOG="${LOG_DIR}/espn_ffl_odds.log"

# Its own status file. An odds failure must not read as a nightly failure -- they run
# on different clocks and have different fixes, and one status file for both would
# make the more frequent job overwrite the more important one's verdict.
STATUS="${REPO}/Data/odds_status.json"

mkdir -p "${LOG_DIR}"
cd "${REPO}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG}"; }

write_status() {
  printf '{"result": "%s", "at": "%s", "stage": "%s", "season": "%s"}\n' \
    "$1" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "${2:-}" "${SEASON:-unknown}" \
    > "${STATUS}"
}

fail() {
  log "FAILED: $*"
  write_status "failed" "$*"
  /usr/bin/osascript -e "display notification \"$* — see ~/logs/espn_ffl_odds.log\" with title \"ESPN FFL odds pull failed\"" 2>/dev/null || true
  exit 1
}

log "=== odds refresh starting ==="

# A warning, not a refusal. The nightly refuses to run off main because it publishes
# boards; this publishes nothing, and a line pulled on a branch is the same line.
BRANCH="$(git -C "${REPO}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
HEAD_SHA="$(git -C "${REPO}" rev-parse HEAD 2>/dev/null || echo unknown)"
MAIN_SHA="$(git -C "${REPO}" rev-parse origin/main 2>/dev/null || echo unknown)"
if [ "${HEAD_SHA}" != "${MAIN_SHA}" ]; then
  log "NOTE: running ${BRANCH} at ${HEAD_SHA:0:8}, not origin/main. Nothing here is \
published, so this is a note rather than a refusal."
fi

SEASON="$("${PYTHON}" -c "
from Scripts.config_utils import load_config
print(load_config()['season'])
" 2>/dev/null)" || fail "could not read season from config.yaml"
log "season ${SEASON}"

# --- Pull every configured book -----------------------------------------
# One command, because the puller already owns the per-book policy: which markets
# each is expected to price, which are optional, and what an empty pull means. A
# stage per book here would duplicate that in a second language.
log "pulling sportsbook game lines"
"${PYTHON}" -m Scripts.books.pull --season "${SEASON}" >>"${LOG}" 2>&1 \
  || fail "Scripts.books.pull"

# --- Verify something is actually stored --------------------------------
# The pull can exit 0 having appended nothing, which is the correct and common
# outcome when no line moved. What must never be true is an empty *store*.
LINES="$("${PYTHON}" -c "
from Scripts.books.store import read_current
print(read_current(${SEASON}).height)
" 2>/dev/null)" || LINES=0
log "lines currently stored: ${LINES}"
if [ "${LINES}" -le 0 ]; then
  fail "the odds store holds no lines for ${SEASON} after a successful pull. \
That means every book returned nothing, or nothing is being written."
fi

write_status "ok" "complete"
log "=== odds refresh complete ==="
