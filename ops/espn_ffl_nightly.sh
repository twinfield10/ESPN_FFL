#!/usr/bin/env bash
#
# espn_ffl_nightly.sh -- cron's entry point for the ESPN FFL daily refresh.
#
# THIS TRACKED COPY IS NOT THE ONE CRON RUNS. The live copy is at
# ~/bin/espn_ffl_nightly.sh, deliberately outside the repo -- see below. This one
# exists so the guard is reviewable and recoverable; if you change it, copy it out:
#
#   cp ops/espn_ffl_nightly.sh ~/bin/espn_ffl_nightly.sh
#
# Why cron gets its own checkout. run_daily_refresh.sh has no `git checkout` -- it
# runs the tree it sits in, re-projects the usage head, rebuilds all nine boards and
# pushes them to S3 with a dated snapshot. Pointed at the interactive checkout, that
# means the branch left checked out at 05:59 is the code that publishes at 06:00,
# merged or not: a branch bumping MODEL_VERSION forces a refit and moves every
# projection, and nothing in the log looks wrong.
#
# Refusing to run in that case was the first fix, and it traded a wrong board for a
# stale one -- days spent on a branch became days with no depth-chart update, which
# matters because the depth chart is the only current-season input that moves and the
# whole reason this job exists. So instead: a second checkout, pinned to origin/main,
# reset before every run. The interactive tree can be on any branch, mid-edit,
# mid-rebase; the boards still rebuild nightly, always from reviewed, pushed code.
#
# Layout, created once:
#
#   git -C ~/GitRepos/ESPN_FFL worktree add --detach ~/GitRepos/ESPN_FFL-nightly origin/main
#   ln -s ~/GitRepos/ESPN_FFL/Data        ~/GitRepos/ESPN_FFL-nightly/Data
#   ln -s ~/GitRepos/ESPN_FFL/config.yaml ~/GitRepos/ESPN_FFL-nightly/config.yaml
#
# Detached on purpose: a branch cannot be checked out in two worktrees at once, and
# a detached tree cannot drift. Data/ and config.yaml are symlinked rather than
# copied -- both checkouts must read and write one store (727 MB), and config.yaml
# is gitignored so the worktree would not otherwise have credentials at all.
#
# Install:
#   chmod +x ~/bin/espn_ffl_nightly.sh
#   crontab -e
#   0 6 * * * /Users/tommywinfield/bin/espn_ffl_nightly.sh

set -euo pipefail

NIGHTLY="/Users/tommywinfield/GitRepos/ESPN_FFL-nightly"
LOG="${HOME}/logs/espn_ffl_refresh.log"
STATUS="${NIGHTLY}/Data/refresh_status.json"

mkdir -p "$(dirname "${LOG}")"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG}"; }

refuse() {
  local msg="$1"
  log "REFUSED: ${msg}"
  # Same shape run_daily_refresh.sh writes, so Scripts.refresh_status and the app's
  # freshness badge read a refusal as a failed run rather than as one that never
  # happened. A guard nobody can see is a guard nobody trusts.
  [ -d "$(dirname "${STATUS}")" ] && printf \
    '{"result": "failed", "at": "%s", "stage": "%s", "season": "unknown"}\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "${msg}" > "${STATUS}"
  /usr/bin/osascript -e "display notification \"${msg}\" with title \"ESPN FFL refresh refused\"" 2>/dev/null || true
  exit 1
}

[ -e "${NIGHTLY}/.git" ]      || refuse "no nightly checkout at ${NIGHTLY} -- see the layout note in this file"
[ -e "${NIGHTLY}/Data" ]      || refuse "${NIGHTLY}/Data is missing -- the symlink to the real store is gone"
[ -e "${NIGHTLY}/config.yaml" ] || refuse "${NIGHTLY}/config.yaml is missing -- the symlink to credentials is gone"

# Fetch, then pin. `reset --hard` rather than `pull`: this tree is detached and must
# match origin/main exactly, and a merge or a rebase here is never wanted.
git -C "${NIGHTLY}" fetch --quiet origin main || refuse "git fetch failed -- no network, or no access to origin"
git -C "${NIGHTLY}" reset --quiet --hard FETCH_HEAD || refuse "could not reset ${NIGHTLY} to origin/main"

log "nightly checkout at $(git -C "${NIGHTLY}" rev-parse --short HEAD), pinned to origin/main"

exec "${NIGHTLY}/run_daily_refresh.sh"
