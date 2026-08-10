"""Did last night's refresh run, and did it work?

A cron job cannot report its own absence. If the laptop was asleep at six, nothing
failed -- nothing happened, and there is no log line, no notification and no non-zero
exit to notice. The only way to catch that is to ask *how long since the last
success*, and to ask it at a moment when someone is present to care.

So this reads two independent things and reconciles them:

* ``Data/refresh_status.json``, which ``run_daily_refresh.sh`` writes on success and
  on failure. Absent or old means the job did not run.
* Each league store's ``built_at``, which is what the boards were actually built
  from. This is the number that matters, because a status file can say "ok" while
  the boards are older -- if someone ran the refresh script with a failure partway
  through a previous version, or rebuilt by hand.

Reporting both is deliberate. They agree almost always, and the case where they
disagree is exactly the case worth seeing.

Exits 1 when anything is stale, so it composes into a shell prompt or a pre-draft
check without needing its output parsed.

Usage:
    python -m Scripts.refresh_status
    python -m Scripts.refresh_status --max-age-hours 25
"""

import argparse
import json
from datetime import datetime, timezone
from typing import List, Optional

from Scripts.config_utils import build_lg_vars, load_config
from Scripts.paths import DATA_DIR, store_dir

#: Where ``run_daily_refresh.sh`` records each run's outcome.
STATUS_PATH = DATA_DIR / "refresh_status.json"

#: Hours past which the data is considered stale.
#:
#: 25 rather than 24, and rather than the app's 60 *minutes*. The job runs at 6am, so
#: anything older than about a day means at least one run was missed -- the extra hour
#: is slack for a slow run, not tolerance for a skipped one. The app's one-hour badge
#: is calibrated for in-season game-day checks, where injury news minutes before
#: kickoff is the thing that matters; against a nightly cadence it would read red 23
#: hours out of 24, and a badge that is always red is one nobody looks at.
DEFAULT_MAX_AGE_HOURS = 25.0


def _age_hours(stamp: str) -> Optional[float]:
    """Hours since an ISO timestamp, or None when it cannot be parsed."""
    try:
        when = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - when).total_seconds() / 3600.0


def _fmt(hours: Optional[float]) -> str:
    """Human-readable age."""
    if hours is None:
        return "unknown"
    if hours < 1:
        return f"{hours * 60:.0f} min ago"
    if hours < 48:
        return f"{hours:.1f} h ago"
    return f"{hours / 24:.1f} days ago"


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point. Returns 1 when anything is stale."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.refresh_status",
        description="Report whether the nightly data refresh ran and succeeded.")
    parser.add_argument("--max-age-hours", type=float,
                        default=DEFAULT_MAX_AGE_HOURS)
    args = parser.parse_args(argv)

    stale = False
    season = load_config()["season"]

    # --- the job itself ---------------------------------------------------
    if not STATUS_PATH.is_file():
        print("  refresh    NEVER RUN — no status file. "
              "Run ./run_daily_refresh.sh")
        stale = True
    else:
        status = json.loads(STATUS_PATH.read_text())
        age = _age_hours(status.get("at", ""))
        result = status.get("result", "unknown")
        if result != "ok":
            print(f"  refresh    FAILED {_fmt(age)} at stage "
                  f"'{status.get('stage', '?')}' — see ~/logs/espn_ffl_refresh.log")
            stale = True
        elif age is not None and age > args.max_age_hours:
            print(f"  refresh    DID NOT RUN — last success {_fmt(age)}, "
                  f"past the {args.max_age_hours:.0f}h threshold. "
                  f"Machine asleep at 6am?")
            stale = True
        else:
            print(f"  refresh    ok, {_fmt(age)}")

    # --- what the boards were actually built from -------------------------
    ages = []
    for display, meta_vars in sorted(build_lg_vars().items()):
        path = store_dir(season, meta_vars["key"]) / "meta.json"
        if not path.is_file():
            print(f"  {display:26} no store")
            stale = True
            continue
        built = json.loads(path.read_text()).get("built_at", "")
        age = _age_hours(built)
        ages.append((display, age))
        if age is None or age > args.max_age_hours:
            stale = True

    if ages:
        oldest = max((a for _, a in ages if a is not None), default=None)
        newest = min((a for _, a in ages if a is not None), default=None)
        flag = "STALE" if (oldest or 0) > args.max_age_hours else "ok"
        print(f"  boards     {flag}, {len(ages)} leagues built "
              f"{_fmt(newest)}" + (f" to {_fmt(oldest)}"
                                   if oldest and newest and oldest - newest > 1
                                   else ""))

    if stale:
        print("\n  Fix:  ./run_daily_refresh.sh")
    return 1 if stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
