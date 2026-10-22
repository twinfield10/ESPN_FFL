"""Import the weekly slates ``Scripts.fetch_athletic`` has landed.

    python -m Scripts.parse_athletic            # import every landed slate not yet imported
    python -m Scripts.parse_athletic --status   # what each week last imported, or why not

**The second half of a split fetch** (``docs/plans/55-athletic-fetch-parse-split.md``).
The fetch lands the raw ``.xlsx`` and records it in
:data:`Scripts.fetch_athletic.STATUS_PATH`; this reads that record, and any week whose
landed bytes differ from the last ones imported goes through
:func:`Scripts.load_athletic.build_weekly` -- which now refuses a slate whose format has
drifted (:func:`Scripts.load_athletic.check_weekly_format`) before writing anything.

**A refusal is not retried around.** The week stays pending, so every nightly tries it
again and fails again, loudly, until the parser is fixed -- and then the next nightly
imports the file that was landed days earlier, with no re-download. Meanwhile the
weekly parquet keeps the last good import.

Its own status file rather than a field in the fetch's: the fetch runs from a
LaunchAgent and this from the nightly, and two processes rewriting one JSON file is a
race that only shows up on the morning one of them runs late.
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

from Scripts import fetch_athletic
from Scripts.load_athletic import SOURCE
from Scripts.paths import PROJECTIONS_DIR, landing_dir

#: Per-week record of what was last imported, and of the last refusal.
STATUS_PATH = PROJECTIONS_DIR / SOURCE / "parse_status.json"

#: How many run outcomes the status file keeps.
RUN_HISTORY = 60


def _log(msg: str) -> None:
    """Print a progress line, flushed -- the nightly sends this to a log."""
    print(msg, flush=True)


def _split_key(key: str) -> tuple:
    """``"2026-07"`` -> ``(2026, 7)``."""
    season, week = key.split("-")
    return int(season), int(week)


def pending(fetched: Dict, parsed: Dict) -> List[str]:
    """Week keys whose landed file has not been imported yet, oldest first.

    Args:
        fetched: The fetch status file's contents.
        parsed: This module's status file's contents.

    Returns:
        list: Keys like ``"2026-07"`` whose fetched ``sha256`` is not the one last
        imported. A week that failed last time is still pending -- see the module
        docstring.
    """
    done = parsed.get("weeks", {})
    return sorted(key for key, entry in fetched.get("weeks", {}).items()
                  if entry.get("sha256")
                  and entry["sha256"] != done.get(key, {}).get("sha256"))


def record(status: Dict, outcomes: Dict[str, str], fetched: Dict) -> Dict:
    """Note this run in ``status``.

    Args:
        status: This module's status, updated in place.
        outcomes: Week key -> ``"imported"`` or the error message.
        fetched: The fetch status, for the file and hash each import came from.

    Returns:
        dict: ``status``.
    """
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    status["last_run"] = {"at": now, "outcomes": outcomes}
    weeks = status.setdefault("weeks", {})
    failures = status.setdefault("failures", {})
    for key, outcome in outcomes.items():
        entry = fetched["weeks"][key]
        if outcome == "imported":
            weeks[key] = {"parsed_at": now, "file": entry.get("file"),
                          "sha256": entry["sha256"]}
            failures.pop(key, None)
        else:
            failures[key] = {"at": now, "file": entry.get("file"),
                             "sha256": entry["sha256"], "error": outcome}
    line = f"{now} " + (", ".join(f"{k} {'imported' if v == 'imported' else 'FAILED'}"
                                  for k, v in outcomes.items()) or "nothing pending")
    status["runs"] = (status.get("runs", []) + [line])[-RUN_HISTORY:]
    return status


def import_week(key: str, entry: Dict) -> None:
    """Import one landed slate.

    Args:
        key: Week key, e.g. ``"2026-07"``.
        entry: Its record in the fetch status file.

    Raises:
        FileNotFoundError: If the landed file is not on this machine.
        WeeklyFormatError: If the slate's format has drifted.
        ValueError: If the sheet names a different season or week than ``key``.
    """
    from Scripts import load_athletic

    season, week = _split_key(key)
    path = landing_dir(SOURCE, season, entry["file"])
    if not path.is_file():
        raise FileNotFoundError(f"landed file {entry['file']} is not in "
                                f"{path.parent} -- pull the archive tier?")
    load_athletic.build_weekly(path, season=season, week=week)


def run(fetch_status_path: Path = fetch_athletic.STATUS_PATH,
        status_path: Path = STATUS_PATH) -> Dict[str, str]:
    """Import every pending week, record each outcome, and return them.

    One week failing does not stop the next: they are separate files.

    Returns:
        dict: Week key -> ``"imported"`` or the error, ``{}`` when nothing was pending.
    """
    fetched = fetch_athletic.load_status(fetch_status_path)
    status = fetch_athletic.load_status(status_path)
    outcomes: Dict[str, str] = {}
    for key in pending(fetched, status):
        entry = fetched["weeks"][key]
        _log(f"  {key}: importing {entry.get('file')}")
        try:
            import_week(key, entry)
            outcomes[key] = "imported"
        except Exception as exc:                                   # noqa: BLE001
            outcomes[key] = f"{type(exc).__name__}: {exc}"
            _log(f"  {key}: FAILED -- {outcomes[key]}")
    if not outcomes:
        _log("  nothing pending: every landed slate is already imported")
    fetch_athletic.save_status(record(status, outcomes, fetched), status_path)
    return outcomes


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point. Returns 1 when any week failed to import."""
    p = argparse.ArgumentParser(
        prog="python -m Scripts.parse_athletic",
        description="Import the Athletic weekly slates Scripts.fetch_athletic landed.")
    p.add_argument("--status", action="store_true",
                   help="print what the last run recorded and exit")
    args = p.parse_args(argv)

    if args.status:
        print(json.dumps(fetch_athletic.load_status(STATUS_PATH), indent=2,
                         sort_keys=True))
        return 0

    _log("===== The Athletic weekly parse =====")
    outcomes = run()
    failed = [k for k, v in outcomes.items() if v != "imported"]
    if failed:
        _log(f"FAILED: {', '.join(failed)} not imported; the last good import stands")
        return 1
    _log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
