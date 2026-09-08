"""Freeze the season projections once the drafts are done.

    python -m Scripts.freeze --all
    python -m Scripts.freeze --league Knights_FFL

Copies each league's ``board.parquet`` to ``board_frozen.parquet`` and stamps
``frozen_at`` and ``frozen_git_sha`` into ``meta.json``.

**Why this exists at all.** ``board.parquet`` is rebuilt every morning at 06:00 by
``run_daily_refresh.sh``, and that is correct -- ADP moves, injuries land, and a board
you draft off has to be current. But it means the artifact stops being able to answer
the one question a draft grade is: *what did this roster look like on the day it was
assembled?* By Thursday the board is a rest-of-season instrument. Grading a September
draft against a December board measures who got lucky, not who drafted well.

**There is a deadline, and it is narrow.** The freeze belongs after the last draft and
before the first game. The trap is the nightly: freezing "Wednesday morning" freezes a
board that already rebuilt at 06:00 with Wednesday's news in it. So the window is the
night the last draft finishes, and ``--allow-stale`` is the escape hatch for having
missed it, not the normal path.

**Not the S3 snapshot.** ``Scripts.sync`` already publishes a dated board snapshot
each night (plan 24), and it was tempting to read the right date back out of it
instead. Two reasons not to: the app must keep working under
``ESPN_FFL_STORE_SOURCE=local``, and a page that has to know a snapshot key layout is
a page coupled to the bucket. A frozen board is a store artifact like any other, so
``sync --push`` carries it and ``sync --verify`` checks it with no new code.

Idempotent by refusal: a league already frozen is left alone unless ``--refreeze``
says otherwise. Freezing twice is almost always a mistake -- the second one would
overwrite the very thing the first was protecting.

See ``docs/plans/41-projection-freeze.md``.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

import pandas as pd

from Scripts import paths, store
from Scripts.config_utils import build_lg_vars, get_season, resolve_league


def _log(message: str) -> None:
    """Print a progress line unbuffered, so a long run is watchable.

    Args:
        message: The line.
    """
    print(message, flush=True)


def drafted_picks(season: int, league_key: str) -> int:
    """How many picks this league has recorded for this season.

    The guard :func:`freeze_league` leans on. A league with no picks has not drafted,
    and a pre-draft board is the one thing there is no point freezing -- it is a
    projection of a roster nobody owns yet.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        int: Pick count for ``season``, or 0 when there is no draft artifact.
    """
    path = paths.store_dir(season, league_key) / store.ARTIFACTS["draft"]
    if not path.is_file():
        return 0
    try:
        picks = pd.read_parquet(path, columns=["season"])
    except Exception:                                       # noqa: BLE001
        return 0
    if "season" not in picks.columns:
        return 0
    return int((picks["season"] == season).sum())


def freeze_league(season: int, league_key: str, *, refreeze: bool = False,
                  allow_undrafted: bool = False) -> str:
    """Freeze one league's board.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        refreeze: Overwrite an existing frozen board.
        allow_undrafted: Freeze even with no recorded picks.

    Returns:
        str: ``"frozen"``, or a sentence saying why it was skipped. Skips are a
        normal outcome of ``--all`` -- most leagues in a run have already been done
        or have not drafted -- so they are reported rather than raised.
    """
    directory = paths.store_dir(season, league_key)
    board_path = directory / store.ARTIFACTS["board"]
    frozen_path = directory / store.ARTIFACTS["board_frozen"]

    if not board_path.is_file():
        return "skipped: no board.parquet to freeze"

    if frozen_path.is_file() and not refreeze:
        meta = store.read_meta(season, league_key, missing_ok=True) or {}
        when = meta.get("frozen_at", "an earlier run")
        return f"skipped: already frozen at {when} (--refreeze to replace)"

    picks = drafted_picks(season, league_key)
    if picks == 0 and not allow_undrafted:
        return ("skipped: no 2026 picks, so there is no drafted roster to grade "
                "(--allow-undrafted to freeze anyway)")

    board = pd.read_parquet(board_path)
    store.write_league_store(
        season, league_key,
        board_frozen=board,
        meta_extra={
            "frozen_at": datetime.now(timezone.utc).astimezone().isoformat(),
            "frozen_git_sha": store._git_sha(),
            "frozen_picks": picks,
        },
    )
    return f"frozen: {board.shape[0]} rows x {board.shape[1]} cols, {picks} picks"


def freeze(leagues: Optional[Sequence[str]] = None, season: Optional[int] = None,
           *, refreeze: bool = False, allow_undrafted: bool = False
           ) -> Dict[str, str]:
    """Freeze several leagues, isolating failures.

    One league failing must not abort the rest, for the reason
    :func:`Scripts.refresh.refresh` gives: a leaguemate's league going wrong should
    not cost you yours.

    Args:
        leagues: Display names or config keys. Defaults to every configured league.
        season: Season year. Defaults to the configured season.
        refreeze: Overwrite existing frozen boards.
        allow_undrafted: Freeze leagues with no recorded picks.

    Returns:
        dict: League key to outcome.
    """
    season = get_season() if season is None else int(season)
    targets = list(build_lg_vars()) if leagues is None else list(leagues)

    results: Dict[str, str] = {}
    _log(f"Freezing season {season}")
    for name in targets:
        try:
            config = resolve_league(name)
            key = config["key"]
        except Exception as e:                              # noqa: BLE001
            results[name] = f"{type(e).__name__}: {e}"
            _log(f"  {name:<30} {results[name]}")
            continue
        try:
            results[key] = freeze_league(
                season, key, refreeze=refreeze, allow_undrafted=allow_undrafted)
        except Exception as e:                              # noqa: BLE001
            results[key] = f"{type(e).__name__}: {e}"
        _log(f"  {config['display_name']:<30} {results[key]}")

    done = sum(1 for v in results.values() if v.startswith("frozen"))
    _log(f"\n{done} of {len(results)} leagues frozen.")
    if done:
        # `--what` selects a *bucket prefix*, not an artifact: `Scripts.sync` accepts
        # only store / archive / nfl, and `board_frozen` rides inside `store` like
        # every other artifact in `Scripts.store.ARTIFACTS`. This used to print
        # `--what board_frozen`, which exits 2 with "unknown --what value(s)" -- so
        # the one instruction printed at the one moment it matters did not run. Found
        # 2026-09-07, freezing the first five leagues.
        _log("Publish it:  python -m Scripts.sync --push")
        _log("Then verify: python -m Scripts.sync --verify --what store")
    return results


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point.

    Args:
        argv: Argument list, for tests. Defaults to ``sys.argv[1:]``.

    Returns:
        int: 0 when at least one league froze or every league was already frozen,
        1 when nothing could be.
    """
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.freeze",
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true",
                       help="every league in config.yaml")
    group.add_argument("--league", action="append", metavar="NAME",
                       help="a display name or config key; repeatable")
    parser.add_argument("--season", type=int, default=None,
                        help="season year (default: config.yaml's)")
    parser.add_argument("--refreeze", action="store_true",
                        help="replace an existing frozen board. Almost always a "
                             "mistake -- it overwrites what the first freeze was "
                             "protecting.")
    parser.add_argument("--allow-undrafted", action="store_true",
                        help="freeze a league with no recorded picks")
    args = parser.parse_args(argv)

    results = freeze(None if args.all else args.league, args.season,
                     refreeze=args.refreeze,
                     allow_undrafted=args.allow_undrafted)

    if not results:
        return 1
    usable = [v for v in results.values()
              if v.startswith("frozen") or "already frozen" in v]
    return 0 if usable else 1


if __name__ == "__main__":
    sys.exit(main())
