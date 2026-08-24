"""Move the data between local disk and S3. The only thing that crosses that line.

    python -m Scripts.sync --push                    # everything, plus a board snapshot
    python -m Scripts.sync --push --what store       # just the league stores
    python -m Scripts.sync --push --dry-run          # keys and bytes, no writes
    python -m Scripts.sync --pull --what store       # rebuild local from S3
    python -m Scripts.sync --verify                  # SHA-256, local against S3

**Three tiers, because they have different reasons to exist.** ``store`` is what the
app reads and is rebuilt in seconds. ``archive`` is the irreproducible half -- the G2
counterfactual above all, which cannot be rebuilt at any price once a season starts,
plus the projections, scoring and injury inputs. ``nfl`` is the nflverse cache, which
regenerates but not cheaply: routes and red zone both need full play-by-play, so a
cold backfill is minutes rather than seconds.

**Why ``--verify`` exits non-zero.** It is the gate in front of untracking ``Data/``
from git, and it is meant to compose -- into the nightly script, into a pre-draft
check, into a shell ``&&``. A verification you have to read is one you will eventually
skip reading.

**Why nothing here retries past boto3's own retries.** This repo's documented failure
mode, across plans 01, 03 and 22, is an absent source reading as agreement. A sync
that quietly papered over a failed upload would be a new instance of exactly that, so
a failure is counted, named, and turned into a non-zero exit.
"""

import argparse
import datetime
import sys
from typing import Dict, List, Optional, Sequence, Tuple

from Scripts import paths, s3_store
from Scripts.config_utils import get_season
from Scripts.store import ARTIFACTS, META_FILENAME

#: Tier to the ``Data/`` subdirectories it covers. ``store`` is absent because it is
#: not a mirrored tier -- it has its own Hive-partitioned schema and a per-league
#: push that must order ``meta.json`` last.
TIER_DIRS: Dict[str, Tuple[str, ...]] = {
    "archive": ("G2", "Projections", "Scoring", "Injuries"),
    "nfl": ("NFL",),
}

WHAT_CHOICES = ("store", "archive", "nfl")


def _log(msg: str) -> None:
    """Print a progress line, flushed -- the nightly script tees this to a log."""
    print(msg, flush=True)


def _tier_dirs(what: Sequence[str]) -> List[str]:
    """The ``Data/`` subdirectories covered by the requested mirrored tiers."""
    dirs: List[str] = []
    for tier in what:
        dirs.extend(TIER_DIRS.get(tier, ()))
    return dirs


def _league_seasons(season: int) -> List[Tuple[int, str]]:
    """Local league-seasons with a complete store, for ``season``.

    Args:
        season: Season year.

    Returns:
        list: ``(season, league_key)`` pairs.
    """
    from Scripts import store
    return [(season, key) for key in store.list_leagues(season)]


# --- push ----------------------------------------------------------------

def _unchanged(local, key: str) -> bool:
    """Whether S3 already holds this exact file.

    **This is a bandwidth guard, not a correctness one**, and it is here because
    the play-by-play archive changed the arithmetic. Before it, the mirror was
    ~40 MB and re-uploading all of it nightly was merely wasteful. `R/GetPBP.R`
    takes `Data/NFL` to ~540 MB, and 26 of its 27 seasons are completed seasons
    that cannot change -- so an unconditional push would send half a gigabyte
    every night to arrive at identical objects.

    Compares SHA-256 through :func:`Scripts.s3_store.verify` rather than size or
    mtime. Size collides trivially, and mtime is wrong in the direction that
    matters here: a re-run of `R/GetPBP.R` rewrites a completed season's parquet
    with fresh mtimes and identical bytes, which is exactly the case worth
    skipping.

    **Any doubt uploads.** An error reaching S3 returns False, so the push happens
    and the failure surfaces on the upload rather than being swallowed by a
    check -- an absent source reading as agreement is this repo's recurring
    failure mode, and a "nothing to do" that really means "could not tell" is that
    same shape.

    Args:
        local: Local file.
        key: Object key.

    Returns:
        bool: True only when S3 is known to hold identical bytes.
    """
    try:
        return s3_store.verify(local, key)
    except Exception:                                       # noqa: BLE001
        return False


def push(what: Sequence[str], season: int, *, dry_run: bool = False,
         snapshot_date: Optional[str] = None,
         skip_unchanged: bool = True) -> Tuple[int, List[str]]:
    """Upload the requested tiers.

    Args:
        what: Tiers from :data:`WHAT_CHOICES`.
        season: Season year, for the store tier and the board snapshot.
        dry_run: Resolve keys and checksums without writing anything.
        snapshot_date: ``YYYY-MM-DD`` to preserve today's boards under. None skips
            the snapshot entirely.
        skip_unchanged: Skip mirror files whose object in S3 already has the same
            SHA-256. See :func:`_unchanged`.

    Returns:
        tuple: ``(objects_uploaded, failures)``. Failures are human-readable and
        already logged.
    """
    uploaded = 0
    failures: List[str] = []
    prefix = "would upload" if dry_run else "uploaded"

    if "store" in what:
        pairs = _league_seasons(season)
        if not pairs:
            failures.append(
                f"no local store for {season} -- run `python -m Scripts.refresh --all`")
        for yr, league in pairs:
            try:
                objects = s3_store.push_league_store(yr, league, dry_run=dry_run)
                uploaded += len(objects)
                _log(f"  store      {league:<24} {prefix} {len(objects)} objects")
            except Exception as e:                          # noqa: BLE001
                failures.append(f"store/{league}: {type(e).__name__}: {e}")
                _log(f"  store      {league:<24} FAILED  {type(e).__name__}: {e}")

        if snapshot_date:
            for yr, league in pairs:
                try:
                    result = s3_store.snapshot_board(yr, league, snapshot_date,
                                                     dry_run=dry_run)
                    if result:
                        uploaded += 1
                except Exception as e:                      # noqa: BLE001
                    failures.append(f"snapshot/{league}: {type(e).__name__}: {e}")
            _log(f"  snapshot   boards {prefix} under date={snapshot_date}")

    dirs = _tier_dirs(what)
    if dirs:
        counts: Dict[str, int] = {}
        skipped = 0
        for local, key in s3_store.iter_mirror_files(dirs):
            if skip_unchanged and _unchanged(local, key):
                skipped += 1
                continue
            try:
                s3_store.put_file(local, key, dry_run=dry_run)
                uploaded += 1
                counts[key.split("/")[0]] = counts.get(key.split("/")[0], 0) + 1
            except Exception as e:                          # noqa: BLE001
                failures.append(f"{key}: {type(e).__name__}: {e}")
                _log(f"  mirror     {key:<40} FAILED  {type(e).__name__}: {e}")
        for tier, n in sorted(counts.items()):
            _log(f"  {tier:<12} {'':<22} {prefix} {n} objects")
        if skipped:
            _log(f"  mirror     {'':<22} skipped {skipped} unchanged")

    return uploaded, failures


# --- pull ----------------------------------------------------------------

def pull(what: Sequence[str], season: int) -> Tuple[int, List[str]]:
    """Download the requested tiers, rebuilding the local layout.

    This is what makes "S3 is the record" true rather than aspirational: a machine
    with an empty ``Data/`` must be able to reach a working app from here.

    Args:
        what: Tiers from :data:`WHAT_CHOICES`.
        season: Season year, for the store tier.

    Returns:
        tuple: ``(objects_downloaded, failures)``.
    """
    downloaded = 0
    failures: List[str] = []

    if "store" in what:
        leagues = s3_store.list_leagues(season)
        if not leagues:
            failures.append(f"no store in S3 for {season}")
        for league in leagues:
            directory = paths.store_dir(season, league, create=True)
            names = [(what_, ARTIFACTS[what_]) for what_ in ARTIFACTS]
            for artifact, filename in names + [("meta", META_FILENAME)]:
                key = s3_store.store_key(season, league, artifact)
                try:
                    s3_store.download(key, directory / filename)
                    downloaded += 1
                except FileNotFoundError:
                    continue                # not every league has every artifact
                except Exception as e:      # noqa: BLE001
                    failures.append(f"{key}: {type(e).__name__}: {e}")
            _log(f"  store      {league:<24} downloaded")

    for tier in ("archive", "nfl"):
        if tier not in what:
            continue
        prefixes = {"archive": ("archive/", "projections/", "scoring/", "injuries/"),
                    "nfl": ("nfl/",)}[tier]
        for prefix in prefixes:
            for key in sorted(s3_store.list_objects(prefix)):
                local = _local_for(key)
                if local is None:
                    continue
                try:
                    s3_store.download(key, local)
                    downloaded += 1
                except Exception as e:                      # noqa: BLE001
                    failures.append(f"{key}: {type(e).__name__}: {e}")
        _log(f"  {tier:<10} {'':<24} downloaded")

    return downloaded, failures


def _local_for(key: str):
    """Invert :func:`Scripts.s3_store.mirror_key` -- the S3 key back to a local path.

    Args:
        key: Object key in a mirrored tier.

    Returns:
        Path | None: Where the object belongs on disk, or None when the key is not
        in a mirrored tier.
    """
    tier, _, rest = key.partition("/")
    if not rest:
        return None

    inverse = {v.split("/")[0]: k for k, v in s3_store.MIRROR_TIERS.items()}
    if key.startswith("archive/g2/"):
        directory, rest = "G2", key[len("archive/g2/"):]
    elif tier in inverse:
        directory = inverse[tier]
    else:
        return None

    parts = [p.split("=", 1)[1] if p.startswith("season=") else p
             for p in rest.split("/")]

    # The loose top-level CSVs live at Data/<name>, not Data/NFL/<name>.
    if directory == "NFL" and len(parts) == 1 and parts[0] in s3_store.LOOSE_FILES:
        return paths.DATA_DIR / parts[0]
    return paths.DATA_DIR.joinpath(directory, *parts)


# --- verify --------------------------------------------------------------

def verify(what: Sequence[str], season: int) -> Tuple[int, List[str]]:
    """Compare every local file against its object in S3, by SHA-256.

    Args:
        what: Tiers from :data:`WHAT_CHOICES`.
        season: Season year, for the store tier.

    Returns:
        tuple: ``(files_checked, mismatches)``. A mismatch names the local path.
    """
    checked = 0
    mismatches: List[str] = []

    def _check(local, key):
        nonlocal checked
        checked += 1
        if not s3_store.verify(local, key):
            try:
                shown = local.relative_to(paths.REPO_ROOT)
            except ValueError:
                # A redirected store (tests, or a store on another volume) lives
                # outside the repo. Same guard as Scripts.refresh._log's summary.
                shown = local
            mismatches.append(str(shown))

    if "store" in what:
        from Scripts import store
        for league in store.list_leagues(season):
            directory = paths.store_dir(season, league)
            for artifact, filename in list(ARTIFACTS.items()) + [("meta",
                                                                 META_FILENAME)]:
                local = directory / filename
                if local.is_file():
                    _check(local, s3_store.store_key(season, league, artifact))
        _log(f"  store      {checked} files checked")

    dirs = _tier_dirs(what)
    if dirs:
        before = checked
        for local, key in s3_store.iter_mirror_files(dirs):
            _check(local, key)
        _log(f"  mirror     {checked - before} files checked")

    return checked, mismatches


# --- CLI -----------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point. See ``python -m Scripts.sync --help``."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.sync",
        description="Move the data store between local disk and S3.")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--push", action="store_true", help="local -> S3")
    action.add_argument("--pull", action="store_true", help="S3 -> local")
    action.add_argument("--verify", action="store_true",
                        help="compare SHA-256 both sides; exits 1 on any mismatch")
    parser.add_argument("--what", default=",".join(WHAT_CHOICES),
                        help=f"comma-separated, from {list(WHAT_CHOICES)} "
                             f"(default: all three)")
    parser.add_argument("--season", type=int, help="defaults to config.yaml season")
    parser.add_argument("--dry-run", action="store_true",
                        help="--push only: resolve keys and checksums, write nothing")
    parser.add_argument("--no-snapshot", action="store_true",
                        help="--push only: skip the dated board snapshot")
    parser.add_argument("--date", help="snapshot date, YYYY-MM-DD (default: today)")
    args = parser.parse_args(argv)

    what = [w.strip() for w in args.what.split(",") if w.strip()]
    unknown = [w for w in what if w not in WHAT_CHOICES]
    if unknown:
        parser.error(f"unknown --what value(s) {unknown}; known: {list(WHAT_CHOICES)}")

    season = get_season() if args.season is None else args.season

    _log(f"\n===== sync: s3://{s3_store.BUCKET} ({s3_store.REGION}) "
         f"season {season} =====")

    if args.push:
        date = None if args.no_snapshot else (
            args.date or datetime.date.today().isoformat())
        count, failures = push(what, season, dry_run=args.dry_run,
                               snapshot_date=date)
        verb = "would upload" if args.dry_run else "uploaded"
        _log(f"  TOTAL      {verb} {count} objects")
    elif args.pull:
        count, failures = pull(what, season)
        _log(f"  TOTAL      downloaded {count} objects")
    else:
        count, mismatches = verify(what, season)
        failures = mismatches
        if mismatches:
            _log(f"\n  {len(mismatches)} of {count} files DIFFER from S3:")
            for path in mismatches[:20]:
                _log(f"    {path}")
            if len(mismatches) > 20:
                _log(f"    ... and {len(mismatches) - 20} more")
            _log("\n  Fix:  python -m Scripts.sync --push")
        else:
            _log(f"  TOTAL      {count} files verified, all identical")

    if failures:
        if not args.verify:
            _log(f"\n{len(failures)} failure(s):")
            for detail in failures[:20]:
                _log(f"  {detail}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
