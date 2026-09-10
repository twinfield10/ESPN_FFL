#!/usr/bin/env bash
#
# espn_ffl_live.sh -- cron's entry point for the in-week live scoring refresh.
#
# THIS TRACKED COPY IS NOT THE ONE CRON RUNS. The live copy is at
# ~/bin/espn_ffl_live.sh, outside the repo, for the reason espn_ffl_nightly.sh
# explains at length. If you change this one, copy it out:
#
#   cp ops/espn_ffl_live.sh ~/bin/espn_ffl_live.sh
#
# What this is, and what it is not. The 06:00 nightly rebuilds everything: it
# re-scrapes every source, re-blends 45 stats and re-scores nine leagues. This does
# none of that. It re-reads the current week's box scores and patches the actuals and
# the rosters onto the frame the nightly already built, reading no source file and
# moving no projection column. See `--what live` in Scripts/refresh.py.
#
# On cost: in week 1 this is 7.2s a league against the full build's 9.8s, which is
# less of a saving than it sounds. The saving grows -- the full build loops weeks
# 1..current and this reads one week -- but the reason it is safe at this frequency
# is structural rather than a stopwatch: it reads no projection source, so it cannot
# go stale, cannot be poisoned by a source going dark, and cannot move what the
# blend voted with.
#
# Why it is safe to run every ten minutes. The first thing it does is ask ESPN's
# scoreboard whether this week is worth refreshing at all
# (`Scripts.game_state --if-live`, which is true while a game is in progress and for
# six hours after each kickoff). On a Tuesday that is one HTTP request and an exit 0.
# The six-hour tail is deliberate and not slack: the run that captures the *final*
# score has to happen after the last game ends, and at that moment nothing is "in
# progress" any more -- so a gate of "anything live" alone would leave a mid-game
# hybrid as the store's permanent record of the week.
#
# Why it does not `set -e` around the per-league loop. A single league failing --
# expired cookies on one, a slow ESPN response -- must not stop the other eight
# publishing. `Scripts.refresh` already isolates per league and returns non-zero if
# any failed, which is what gets logged.
#
# Install:
#   chmod +x ~/bin/espn_ffl_live.sh
#   crontab -e
#   # ESPN FFL live scoring -- every 10 minutes; self-gates on the NFL scoreboard
#   */10 * * * * /Users/tommywinfield/bin/espn_ffl_live.sh

set -uo pipefail

NIGHTLY="/Users/tommywinfield/GitRepos/ESPN_FFL-nightly"
PYTHON="/usr/local/bin/python3.11"
LOG="${HOME}/logs/espn_ffl_live.log"

mkdir -p "$(dirname "${LOG}")"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >>"${LOG}"; }

# One instance at a time. A ten-minute cron against a run that can take a minute is
# usually fine, but a slow ESPN turns "usually" into two processes writing the same
# parquet -- and `_write_parquet_atomic` makes each write atomic without making two
# of them ordered, so the loser's stale frame would win.
LOCK="/tmp/espn_ffl_live.lock"
if ! mkdir "${LOCK}" 2>/dev/null; then
  log "another live refresh is still running (${LOCK}); skipped"
  exit 0
fi
trap 'rmdir "${LOCK}" 2>/dev/null || true' EXIT

cd "${NIGHTLY}" 2>/dev/null || { log "no nightly checkout at ${NIGHTLY}"; exit 1; }

# Deliberately no `git fetch`/`reset` here, unlike the nightly. This runs off whatever
# origin/main the 06:00 job already pinned: a ten-minute job that reset the worktree
# would race the nightly's own reset, and pulling code 144 times a day to run the same
# commit is noise. A commit lands in the live path at the next 06:00.
REV="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

if ! "${PYTHON}" -m Scripts.game_state --if-live >/dev/null 2>>"${LOG}"; then
  # Not an error: most of the week there is no football on.
  exit 0
fi

log "live window open at ${REV}; refreshing"
if "${PYTHON}" -m Scripts.refresh --all --what live >>"${LOG}" 2>&1; then
  # The app reads S3 by default, so a patch that is not pushed is a patch nobody
  # sees. `--what store` only: nothing else moved.
  if "${PYTHON}" -m Scripts.sync --push --what store --no-snapshot >>"${LOG}" 2>&1; then
    log "pushed"
  else
    log "FAILED: Scripts.sync --push"
  fi
else
  log "FAILED: Scripts.refresh --all --what live"
fi
