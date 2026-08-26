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
# This wrapper exists for one reason: run_daily_refresh.sh lives inside the repo,
# so any guard written into it is absent from every branch that predates it, and
# from any older commit checked out for any reason. The branch check therefore has
# to live somewhere no checkout can affect. That is here.
#
# What it is guarding against. run_daily_refresh.sh has no `git checkout` -- it runs
# the working tree, re-projects the usage head, rebuilds all nine boards and pushes
# them to S3 with a dated snapshot. So the branch left checked out at 05:59 is the
# code that publishes at 06:00, merged or not. A branch that bumps MODEL_VERSION
# forces a refit and moves every projection; nothing in the log looks wrong.
#
# A skipped night is cheap -- every pull is a full snapshot and tomorrow catches up.
# A board republished off an unmerged branch during draft season is not.
#
# ALLOW_ANY_BRANCH=1 overrides, for a deliberate run off a branch.
#
# Install:
#   chmod +x ~/bin/espn_ffl_nightly.sh
#   crontab -e
#   0 6 * * * /Users/tommywinfield/bin/espn_ffl_nightly.sh

set -euo pipefail

REPO="/Users/tommywinfield/GitRepos/ESPN_FFL"
LOG="${HOME}/logs/espn_ffl_refresh.log"
STATUS="${REPO}/Data/refresh_status.json"

mkdir -p "$(dirname "${LOG}")"

refuse() {
  local msg="$1"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] REFUSED: ${msg}" | tee -a "${LOG}"
  # Same shape run_daily_refresh.sh writes, so Scripts.refresh_status and the app's
  # freshness badge read a refusal as a failed run rather than as a run that never
  # happened. A guard nobody can see is a guard nobody trusts.
  [ -d "$(dirname "${STATUS}")" ] && printf \
    '{"result": "failed", "at": "%s", "stage": "%s", "season": "unknown"}\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "${msg}" > "${STATUS}"
  /usr/bin/osascript -e "display notification \"${msg}\" with title \"ESPN FFL refresh refused\"" 2>/dev/null || true
  exit 1
}

[ -d "${REPO}/.git" ] || refuse "no git repo at ${REPO}"

BRANCH="$(git -C "${REPO}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"

if [ "${ALLOW_ANY_BRANCH:-0}" = "1" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: branch check overridden, running off '${BRANCH}'" | tee -a "${LOG}"
elif [ "${BRANCH}" != "main" ]; then
  refuse "repo is on '${BRANCH}', not main -- boards publish from main only. Run: git -C ${REPO} checkout main"
fi

exec "${REPO}/run_daily_refresh.sh"
