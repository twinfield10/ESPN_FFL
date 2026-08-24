"""ESPN's injury report, with the estimated return date.

The one thing no other source in the blend has. ESPN's *fantasy* API gives a status --
``ACTIVE``/``QUESTIONABLE``/``OUT``/``INJURY_RESERVE`` -- and nothing about when a
player comes back, so a season projection cannot tell "back next week" from "out until
November" from "done for the year". Its **site** API can::

    "details": {"type": "Knee", "location": "Knee", "detail": "ACL",
                "returnDate": "2026-10-11"}

Measured on the 2026-08-07 pull: **152 of 152 non-active records carry a
``returnDate``**, against ESPN's fantasy API where a free-text ``seasonOutlook``
exists for only 9 of 22 injured players and never in a parseable form.

**Why this endpoint and not the web page.** ``https://www.espn.com/nfl/injuries``
renders the same table, but ``site.api.espn.com`` serves it as JSON -- no HTML
parsing, no layout to break. On access: ``www.espn.com/robots.txt`` does not disallow
``/nfl/injuries`` for general agents, though it does block ten named AI crawlers
site-wide, ``anthropic-ai`` among them. The API host publishes no robots.txt at all --
a 403 on the file itself, which RFC 9309 classes as "unavailable" and permits. This is
a different situation from Pro-Football-Reference and BetOnline's weekly endpoint,
both of which sit behind active anti-bot controls and are deliberately left alone; see
``docs/STATE_OF_THE_REPO.md``.

**Two shapes of ``returnDate``, and they mean different things.** A real estimate is a
near-term date -- the 2026-08-07 pull carries 2026-09-13, 09-14, 09-28, 10-11 and
12-06. Season-ending injured reserve uses a **sentinel** far past the schedule, 22
records at ``2027-02-15``. :data:`SEASON_ENDING_AFTER` separates them, because
treating the sentinel as an estimate would put a player back for a week that does not
exist.

**It joins on a name.** The site API carries no athlete id -- only ``displayName`` --
so this lands on the same ``normalise_name`` key the book sources use, with the same
shared-name caveat ``_disambiguate_name_keys`` exists for.

**Every pull is kept.** The current-state file is overwritten each run, and until
2026-08-18 that was the only copy -- so ESPN's severity vocabulary, which is the only
severity any source in this repo carries, survived exactly one day. Measured on the
2026-08-18 pull: 114 of 800 records carry a structured ``injury_type``, and among them
``"Knee - ACL"`` and ``"Knee - ACL + MCL"`` are real diagnoses that ``nflreadr``'s
``report_primary_injury`` cannot express -- it says ``"Knee"``. :func:`write` now also
lands a dated partition, because **there is no backfill**: the history starts the day
the archive ships and nothing can recover the days before it. See
:func:`snapshot_path`.

Usage::

    python -m Scripts.scrape_espn_injuries --season 2026
"""

from __future__ import annotations

import datetime
import re
from typing import Any, Dict, List, Optional

import polars as pl
import requests

from Scripts.paths import DATA_DIR

#: The JSON behind ``espn.com/nfl/injuries``.
ENDPOINT = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/injuries"

#: A ``returnDate`` beyond this is a season-ending placeholder, not an estimate.
#:
#: ESPN stamps injured reserve with a date past the end of the schedule -- 2027-02-15
#: on the 2026-08-07 pull, which is after the Super Bowl. Read literally it would mean
#: "returns in week 23", and a games-available calculation would quietly produce a
#: negative slate. Anything past the regular season is treated as "out for the year".
SEASON_ENDING_AFTER = datetime.date(2027, 1, 15)

#: Statuses that mean the player is currently available.
ACTIVE_STATUSES = ("Active",)

#: A snapshot partition stamp. ``YYYY-MM-DD`` and nothing else.
#:
#: Validated rather than trusted because a malformed partition is one no query engine
#: can prune on, and it fails silently -- the file lands, ``--verify`` passes, and the
#: history simply has a hole in it that nothing reports.
_DATE_STAMP = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def injuries_path(season: int, create: bool = False):
    """Where a season's pull lives.

    Args:
        season: Season year.
        create: Create the directory.

    Returns:
        Path: ``Data/Injuries/<season>/espn_injuries.parquet``.
    """
    directory = DATA_DIR / "Injuries" / str(season)
    if create:
        directory.mkdir(parents=True, exist_ok=True)
    return directory / "espn_injuries.parquet"


def snapshot_path(season: int, stamp: str, create: bool = False):
    """Where one day's pull is kept, so the report has a history.

    The current-state file is overwritten on every run, which means ESPN's own severity
    vocabulary -- ``"Knee - ACL"``, ``injury_detail`` of ``"Surgery"`` -- exists only
    for as long as it takes the next pull to replace it. **There is no backfill.** A
    dated partition beside it turns a nightly snapshot into a time series: when a
    designation first appeared, how an estimated return date moved, and eventually a
    severity axis that ``nflreadr``'s generic body parts cannot express.

    ``Data/Injuries/`` is already a mirrored tier, and
    :func:`Scripts.s3_store.mirror_key` maps every path component through ``_hive``,
    which converts bare four-digit years and leaves everything else alone. So this
    lands at ``injuries/season=<season>/snapshots/date=<stamp>/espn_injuries.parquet``
    with no new S3 plumbing.

    Args:
        season: Season year.
        stamp: Partition date, ``YYYY-MM-DD``.
        create: Create the directory.

    Returns:
        Path: ``Data/Injuries/<season>/snapshots/date=<stamp>/espn_injuries.parquet``.

    Raises:
        ValueError: When ``stamp`` is not ``YYYY-MM-DD``.
    """
    if not _DATE_STAMP.match(str(stamp)):
        raise ValueError(
            f"snapshot stamp must be YYYY-MM-DD, got {stamp!r}. A malformed partition "
            f"is one no query can prune on, and it fails silently.")
    directory = DATA_DIR / "Injuries" / str(season) / "snapshots" / f"date={stamp}"
    if create:
        directory.mkdir(parents=True, exist_ok=True)
    return directory / "espn_injuries.parquet"


def fetch(timeout: int = 30) -> Dict[str, Any]:
    """Request the injury report.

    Args:
        timeout: Seconds to wait.

    Returns:
        dict: The decoded payload.

    Raises:
        requests.HTTPError: On a non-2xx response.
        ValueError: When the payload carries no ``injuries`` array.
    """
    response = requests.get(ENDPOINT, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if "injuries" not in payload:
        raise ValueError(
            f"{ENDPOINT} returned no 'injuries' array; keys were {sorted(payload)}."
        )
    return payload


def parse(payload: Dict[str, Any],
          pulled_at: Optional[datetime.datetime] = None) -> pl.DataFrame:
    """Flatten the nested payload into one row per injury record.

    Deliberately faithful: no games arithmetic here, no filtering to skill positions.
    The season-relative interpretation belongs with the schedule that defines week 1,
    and a pull that quietly dropped rows would be hard to audit later.

    ``pulled_at`` stamps every row with when the pull happened, which is what makes a
    snapshot self-describing: a concatenated history can be ordered and deduplicated
    from the data rather than from the partition names, and a re-run that lands in the
    wrong partition is visible instead of silent.

    Args:
        payload: :func:`fetch` output.
        pulled_at: When the pull happened. Defaults to now, UTC.

    Returns:
        pl.DataFrame: ``full_name``, ``name_key``, ``team``, ``position``,
        ``status``, ``return_date``, ``injury_detail``, ``injury_type``,
        ``injury_location``, ``news_date``, ``comment`` and ``pulled_at``.
    """
    from Scripts.season_projections import normalise_name

    if pulled_at is None:
        pulled_at = datetime.datetime.now(datetime.timezone.utc)
    stamp = pulled_at.isoformat()

    rows: List[Dict[str, Any]] = []
    for team in payload.get("injuries", []):
        team_name = team.get("displayName")
        for record in team.get("injuries", []):
            athlete = record.get("athlete") or {}
            details = record.get("details") or {}
            position = (athlete.get("position") or {}).get("abbreviation")
            rows.append({
                "full_name": athlete.get("displayName"),
                "name_key": normalise_name(athlete.get("displayName")),
                "team": team_name,
                "position": position,
                "status": record.get("status"),
                "return_date": details.get("returnDate"),
                "injury_detail": details.get("detail"),
                "injury_type": details.get("type"),
                "injury_location": details.get("location"),
                "news_date": record.get("date"),
                "comment": record.get("shortComment") or record.get("longComment"),
                "pulled_at": stamp,
            })

    if not rows:
        return pl.DataFrame(schema={"full_name": pl.String, "name_key": pl.String,
                                    "status": pl.String, "return_date": pl.Date,
                                    "pulled_at": pl.String})

    frame = pl.DataFrame(rows)
    # Cast to String before parsing. A pull in which *no* record carries a return date
    # -- an all-active week, or a single-record slice -- infers the column as Null,
    # and `.str.to_date()` raises a SchemaError on it rather than yielding nulls.
    return frame.with_columns(
        pl.col("return_date").cast(pl.String)
        .str.to_date(strict=False).alias("return_date"))


def write(season: int, frame: pl.DataFrame, stamp: Optional[str] = None):
    """Persist a pull, as both current state and a dated snapshot.

    Two writes, and **neither is behind a flag**. The current-state file keeps the path
    every existing reader already depends on -- :func:`Scripts.season_projections.
    load_espn_injuries` among them -- and the dated partition accumulates the history
    that overwriting has been discarding. An archive you have to remember to ask for is
    an archive with gaps, so ``run_daily_refresh.sh`` needs no change to get one.

    A second pull on the same day **overwrites** that date's partition rather than
    appending. Last pull of the day wins, which is the right resolution pre-season. In
    season the report moves within a week, and finer stamping belongs with the weekly
    availability head that cares -- see ``docs/plans/19-weekly-usage-model.md``.

    Args:
        season: Season year.
        frame: :func:`parse` output.
        stamp: Snapshot partition date, ``YYYY-MM-DD``. Defaults to today, UTC, so the
            partition agrees with the ``pulled_at`` the rows carry.

    Returns:
        Path: The current-state path, which is what the caller reports.
    """
    if stamp is None:
        stamp = datetime.datetime.now(datetime.timezone.utc).date().isoformat()

    path = injuries_path(season, create=True)
    frame.write_parquet(path)
    frame.write_parquet(snapshot_path(season, stamp, create=True))
    return path


def summary(frame: pl.DataFrame) -> str:
    """A printable description of what was pulled."""
    total = frame.height
    hurt = frame.filter(~pl.col("status").is_in(list(ACTIVE_STATUSES)))
    dated = hurt.filter(pl.col("return_date").is_not_null())
    ending = dated.filter(pl.col("return_date") > SEASON_ENDING_AFTER)
    lines = [f"  {total} records, {hurt.height} non-active",
             f"  {dated.height} with a return date "
             f"({100 * dated.height / max(hurt.height, 1):.0f}% of non-active)",
             f"  {ending.height} season-ending (return past "
             f"{SEASON_ENDING_AFTER})"]
    by_status = hurt.group_by("status").len().sort("len", descending=True)
    lines.append("  " + ", ".join(f"{s}={n}" for s, n in by_status.rows()))
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    import argparse

    from Scripts.nfl_utils import current_season

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--season", type=int, default=None,
                        help="Season to file the pull under.")
    args = parser.parse_args(argv)

    season = args.season if args.season is not None else current_season()
    print(f"\n===== ESPN injuries: {season} =====")
    frame = parse(fetch())
    print(summary(frame))
    print(f"  wrote {write(season, frame)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
