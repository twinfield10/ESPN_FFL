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
# window, and import + push only if the link changed. Most days that is one sitemap
# read, one page load and "unchanged". See docs/plans/53-athletic-weekly-fetch.md.
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
# Why 05:45. Fifteen minutes ahead of the 06:00 nightly, which reads the Athletic file
# while it builds the lineups. A run takes under a minute; the gap is the margin.
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
