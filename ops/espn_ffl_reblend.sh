#!/usr/bin/env bash
#
# espn_ffl_reblend.sh -- cron's entry point for the intraday re-blend (plan 56).
#
# THIS TRACKED COPY IS NOT THE ONE CRON RUNS. The live copy is at
# ~/bin/espn_ffl_reblend.sh, outside the repo, for the reason espn_ffl_nightly.sh
# explains at length. If you change this one, copy it out:
#
#   cp ops/espn_ffl_reblend.sh ~/bin/espn_ffl_reblend.sh
#
# What this is. The 06:00 nightly is the only run that moves a projection, so Friday's
# injury reports, Sunday's inactives and the line moves after them reached the store the
# next morning, after the games. This re-reads the sportsbook engine and ESPN's current
# week and rebuilds this week's projections only, behind the kickoff freeze. It writes
# and publishes a league only when a pre-game player moved at least 0.1 points or a
# roster fact changed. See docs/plans/56-intraday-reblend.md and `--what reblend` in
# Scripts/refresh.py.
#
# What it is not. It is not the live job (espn_ffl_live.sh), which patches actual
# scores onto the frame every ten minutes on a game day and never moves a projection.
# The two rewrite the same lineups.parquet, so they share one lock (below).
#
# Cost, measured 2026-10-22 on the MacBook at week 7: the engine read ~9s, the
# re-blend 73s for nine leagues, flat all season (it fetches one week, not every week
# since week 1). A firing with no game left to play is one HTTP request.
#
# Install (on the Mac mini, the machine cron runs on):
#   cp ops/espn_ffl_reblend.sh ~/bin/espn_ffl_reblend.sh
#   chmod +x ~/bin/espn_ffl_reblend.sh
#   crontab -e
#   # ESPN FFL intraday re-blend -- every 30 min, 08:05-23:35; self-gates on a game still to play
#   5,35 8-23 * * * /Users/tommywinfield/bin/espn_ffl_reblend.sh
#
# **At :05 and :35, not :00 and :30.** The live job fires every ten minutes from :00 and
# runs about a minute, and the two share a lock. On the hour and half hour the re-blend
# would race it for the lock and lose about half the time on a game day. Five minutes
# later the live run is done, and a re-blend (~90 s) finishes before the next one at :10.

set -uo pipefail

NIGHTLY="/Users/tommywinfield/GitRepos/ESPN_FFL-nightly"
PYTHON="/usr/local/bin/python3.11"
LOG="${HOME}/logs/espn_ffl_reblend.log"
# Touched on the closed-gate exit, the one path that writes nothing. See below.
HEARTBEAT="${HOME}/logs/espn_ffl_reblend.heartbeat"

mkdir -p "$(dirname "${LOG}")"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >>"${LOG}"; }

# Keep the log from growing without bound. Rotate at 8 MB, one generation kept. Same
# reasoning as espn_ffl_live.sh: no logrotate on macOS, and this fires 32 times a day.
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

# Never alongside the nightly. It runs from 06:00 for about nine minutes, two hours
# before this window opens, and it rebuilds lineups.parquet itself. It takes no lock,
# so a slow night is detected by process instead.
if pgrep -f "run_daily_refresh.sh" >/dev/null 2>&1; then
  log "the nightly is still running; skipped"
  exit 0
fi

# **The live job's lock, shared on purpose.** Both jobs read lineups.parquet, change it
# and write it back. `_write_parquet_atomic` makes each write atomic without making two
# of them ordered, so the slower writer's frame would win and drop the other's change.
# Whichever job finds the lock held skips that firing; the next one is at most 30
# minutes away. The staleness expiry is the live job's too, for its reason: a lock left
# by a killed run must not stop both jobs forever, silently.
LOCK="/tmp/espn_ffl_live.lock"
LOCK_STALE_MINUTES=30

if [ -d "${LOCK}" ] && [ -n "$(find "${LOCK}" -maxdepth 0 -mmin "+${LOCK_STALE_MINUTES}" 2>/dev/null)" ]; then
  log "WARNING: lock ${LOCK} is older than ${LOCK_STALE_MINUTES}m -- a previous run died without releasing it. Taking it."
  rmdir "${LOCK}" 2>/dev/null || true
fi

if ! mkdir "${LOCK}" 2>/dev/null; then
  log "the live refresh (or another re-blend) holds ${LOCK}; skipped"
  exit 0
fi
trap 'rmdir "${LOCK}" 2>/dev/null || true' EXIT

cd "${NIGHTLY}" 2>/dev/null || { log "no nightly checkout at ${NIGHTLY}"; exit 1; }

# No `git fetch`/`reset`, as in the live job: this runs the commit the 06:00 nightly
# pinned, and resetting the worktree 32 times a day would race the nightly's own reset.
REV="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

# 0 = a game this week is still to play; 3 = none is; anything else = the gate is
# broken. Three codes for the reason espn_ffl_live.sh gives: a broken gate must not
# look like a quiet day.
"${PYTHON}" -m Scripts.game_state --if-pending >/dev/null 2>>"${LOG}"
GATE=$?
if [ "${GATE}" -eq 3 ]; then
  # Monday night after kickoff until the nightly turns the week over. Silent, so the
  # heartbeat's mtime is the record that the gate was asked.
  touch "${HEARTBEAT}"
  exit 0
elif [ "${GATE}" -ne 0 ]; then
  log "FAILED: Scripts.game_state --if-pending exited ${GATE} -- the gate is broken, not closed. Nothing was re-blended."
  exit 1
fi

log "re-blend window open at ${REV}"

# The market, as of now. Live books only and their season files only: no raw archive,
# shadow or status entry (`--intraday`). Not fatal: on a failed read the books keep
# their last pull, and the re-blend still picks up ESPN's changes.
if ! "${PYTHON}" -m Scripts.books.engine --intraday >>"${LOG}" 2>&1; then
  log "NOTE: the engine read failed; re-blending on each book's last pull"
fi

# A slate The Athletic's 17:45 (Thu-Sat) or Sunday 12:00 fetch landed. Without this it
# would wait for the next nightly, after Sunday's games. Not fatal, as in the nightly:
# a refusal keeps the last good week.
if ! "${PYTHON}" -m Scripts.parse_athletic >>"${LOG}" 2>&1; then
  log "NOTE: Scripts.parse_athletic failed or refused a slate; The Athletic keeps its last good week"
fi

# `--push` publishes only lineups, and only for leagues the re-blend wrote. A league
# where nothing material moved is not written, so it uploads nothing.
if "${PYTHON}" -m Scripts.refresh --all --what reblend --push >>"${LOG}" 2>&1; then
  log "re-blended and published"
else
  log "FAILED: Scripts.refresh --all --what reblend --push"
fi
