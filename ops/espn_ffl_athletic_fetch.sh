#!/usr/bin/env bash
#
# espn_ffl_athletic_fetch.sh -- launchd's entry point for The Athletic weekly fetch.
#
# THIS TRACKED COPY IS NOT THE ONE LAUNCHD RUNS. The live copy is at
# ~/bin/espn_ffl_athletic_fetch.sh, outside the repo, for the reason
# espn_ffl_nightly.sh explains at length. If you change this one, copy it out:
#
#   cp ops/espn_ffl_athletic_fetch.sh ~/bin/espn_ffl_athletic_fetch.sh
#
# What it does. `Scripts.fetch_athletic --push`: find this week's rankings article in
# the sitemap, read the "Projections Download Link" off it in a visible Chromium
# window, download the slate, and land + push it only if its bytes changed. Most days
# that is one sitemap read, one page load, one ~30 KB download and "unchanged". It does
# not import: the nightly's `Scripts.parse_athletic` does (plan 55). See
# docs/plans/53-athletic-weekly-fetch.md.
#
# Why launchd and not cron, unlike every other job here. The page returns 403 to
# headless Chromium, so the browser has to open a real window -- and a cron job runs
# outside the login session, where a window is not guaranteed to open. A LaunchAgent
# runs inside it (gui/<uid>). That makes two requirements of the Mac mini:
#
#   * this user logged in at the desktop. FileVault is on, so there is no automatic
#     login -- but after a reboot nothing runs until the FileVault password is typed,
#     and typing it logs the user in. Only a logout leaves the nightly running and
#     this agent not; and
#   * the agent loaded in that session -- `launchctl bootstrap gui/$(id -u) ...`.
#
# Why 05:45. Fifteen minutes ahead of the 06:00 nightly, which imports what this lands
# and builds the lineups from it. A run takes under a minute; the gap is the margin.
#
# Why also 17:45 Thu/Fri/Sat and 12:00 Sun (added 2026-10-22). The slate is re-uploaded
# when injury reports move it, and those land late in the week. A file landed by one of
# these runs would otherwise wait for the next 06:00 nightly -- Monday's, for Sunday's
# -- so a run after 06:00 that lands new bytes publishes them itself: import, rebuild
# and push the lineups, republish the Knights Sheet. Most of these runs read
# "unchanged" and do nothing more. The 05:45 run never publishes: the nightly starts 15
# minutes later and resets this checkout.
#
# Why it takes the live job's lock to publish. espn_ffl_live.sh writes lineups.parquet
# every ten minutes while games are on -- which on a London Sunday includes noon -- and
# two writers to one parquet race. Holding /tmp/espn_ffl_live.lock for the ~3-minute
# rebuild makes the live job skip a tick instead. Games already kicked off are carried
# forward, not rebuilt (Scripts/kickoff_freeze.py).
#
# Deliberately no `git fetch`/`reset`, like espn_ffl_live.sh: this runs off whatever
# origin/main the previous 06:00 nightly pinned, so it cannot race that reset. A
# commit reaches this job the morning after it reaches the nightly.
#
# Why --push here and nowhere else. It pushes the whole archive tier, which is only
# safe from the nightly's own Data/ -- this machine's.
#
# Install: see "The Athletic fetch, on the Mac mini" in docs/SEASON_ROLLOVER.md.

set -uo pipefail

NIGHTLY="/Users/tommywinfield/GitRepos/ESPN_FFL-nightly"
PYTHON="/usr/local/bin/python3.11"
LOG="${HOME}/logs/espn_ffl_athletic.log"

mkdir -p "$(dirname "${LOG}")"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >>"${LOG}"; }

# Rotate at 8 MB, one generation kept -- same rule and reason as the other two jobs.
rotate_log() {
  local f="$1" max=$((8 * 1024 * 1024))
  [ -f "${f}" ] || return 0
  local size
  size=$(wc -c <"${f}" 2>/dev/null || echo 0)
  if [ "${size}" -gt "${max}" ]; then
    mv -f "${f}" "${f}.1" 2>/dev/null || true
  fi
}
rotate_log "${LOG}"

notify() {
  /usr/bin/osascript -e "display notification \"$* — see ~/logs/espn_ffl_athletic.log\" with title \"ESPN FFL Athletic fetch failed\"" 2>/dev/null || true
}

cd "${NIGHTLY}" 2>/dev/null || { log "no nightly checkout at ${NIGHTLY}"; notify "no nightly checkout"; exit 1; }
REV="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

log "athletic fetch at ${REV}"
if "${PYTHON}" -m Scripts.fetch_athletic --push >>"${LOG}" 2>&1; then
  log "athletic fetch finished"
else
  # Not fatal to anything else: the nightly still runs and uses the last import, and
  # refresh_status's eight-day ATH weekly clock is the alert that is seen in the app.
  # `--href <copied link>` is the manual way through.
  log "FAILED: Scripts.fetch_athletic --push"
  notify "Scripts.fetch_athletic --push"
  exit 1
fi

# --- publish a new file landed outside the nightly's window -------------------------
OUTCOME="$("${PYTHON}" -c "
from Scripts.fetch_athletic import load_status
print(load_status().get('last_run', {}).get('outcome', ''))
" 2>/dev/null)"
if [ "${OUTCOME}" != "fetched" ]; then
  exit 0
fi
if [ "$((10#$(date +%H)))" -lt 6 ]; then
  log "new slate landed; the 06:00 nightly imports it"
  exit 0
fi

LOCK="/tmp/espn_ffl_live.lock"
LOCK_STALE_MINUTES=30
HAVE_LOCK=0
for _ in $(seq 1 40); do            # up to 10 minutes; a live run takes about one
  if [ -d "${LOCK}" ] && [ -n "$(find "${LOCK}" -maxdepth 0 -mmin "+${LOCK_STALE_MINUTES}" 2>/dev/null)" ]; then
    log "WARNING: lock ${LOCK} is older than ${LOCK_STALE_MINUTES}m; taking it"
    rmdir "${LOCK}" 2>/dev/null || true
  fi
  if mkdir "${LOCK}" 2>/dev/null; then
    HAVE_LOCK=1
    break
  fi
  sleep 15
done
if [ "${HAVE_LOCK}" != "1" ]; then
  log "FAILED: could not take ${LOCK} in 10 minutes; the 06:00 nightly imports the slate"
  notify "live refresh lock held; slate not published"
  exit 1
fi
trap 'rmdir "${LOCK}" 2>/dev/null || true' EXIT

log "new slate landed after 06:00; publishing it"
if ! "${PYTHON}" -m Scripts.parse_athletic >>"${LOG}" 2>&1; then
  # The board keeps the last good import; the nightly retries and warns too.
  log "FAILED: Scripts.parse_athletic refused the slate (see above); nothing published"
  notify "Athletic slate refused by the parser"
  exit 1
fi
if ! "${PYTHON}" -m Scripts.refresh --all --what lineups --push >>"${LOG}" 2>&1; then
  log "FAILED: Scripts.refresh --what lineups --push; the 06:00 nightly rebuilds"
  notify "lineups rebuild after the Athletic import"
  exit 1
fi
"${PYTHON}" -m Scripts.sync --push --what archive --no-snapshot >>"${LOG}" 2>&1 \
  || log "NOTE: archive push failed; the nightly's push is the second chance"
if [ -e "${NIGHTLY}/gs4creds.json" ]; then
  "${PYTHON}" -c "
import sys
import populateGoogleSheet as p
sys.exit(0 if p.run(['Knights_FFL']) == {'Knights_FFL': 'ok'} else 1)
" >>"${LOG}" 2>&1 || log "NOTE: Knights Sheet publish failed; the nightly republishes it"
fi
log "athletic slate published"
