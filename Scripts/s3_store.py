"""S3 is the system of record. This module is the only thing that talks to it.

``Scripts/store.py`` still owns the *local* store and its invariants; this module
mirrors that store into ``s3://espn-ffl-data`` and reads it back. The split is
deliberate: writers keep writing to disk atomically and cheaply, and the network
appears exactly once, at the end of a refresh, where a failure can be reported
rather than half-applied.

**Key schema.** Two shapes, for two different jobs::

    store/season=2026/league=knights_ffl/board.parquet
    store/season=2026/league=knights_ffl/meta.json          <- uploaded last

    snapshots/board/season=2026/league=knights_ffl/date=2026-08-11/board.parquet

    archive/g2/season=2026/manifest.json                    <- irreproducible
    nfl/season=2026/depth_charts.parquet                    <- recompute cache
    projections/Usage/Season/season=2026/...

Hive-style ``key=value`` partitioning is not decoration: it is what lets Athena and
DuckDB discover partitions and prune on them without a catalog. Any path component
that is a bare four-digit year becomes ``season=YYYY``, uniformly, so a query layer
sees one convention across every tier rather than a special case per directory.

**Why the board snapshots exist.** The G2 archive under ``Data/G2/`` was built by
hand because a past board cannot be reconstructed -- FantasyPros serves no season
parameter, so a board is gone the moment it stops being current, and plan 18 records
G2 as unmeasurable on history for exactly that reason. A dated key per nightly build
retires that whole class of problem, and makes ADP drift through camp measurable.

**On atomicity.** S3 has no rename, so ``store.py``'s ``.tmp`` + ``os.replace`` does
not port. The invariant that actually matters does: each PUT is atomic, read-after-
write is strongly consistent, and :func:`push_league_store` uploads ``meta.json``
**last**. A reader keying on ``meta.json`` therefore sees the previous complete store
or the new one, exactly as on disk. What is genuinely lost is atomicity across the
*set* of five objects -- inherent to S3, and worth knowing rather than pretending
otherwise.

**On checksums.** Every upload goes through ``put_object`` rather than boto3's
``upload_file``. That is not an oversight: ``upload_file`` switches to multipart above
a threshold, and a multipart object's ``ChecksumSHA256`` is a hash *of the part
hashes*, not of the file -- so verification would silently stop comparing like with
like the first time an artifact grew past 8 MB. ``put_object`` is single-part up to
5 GB, and the largest artifact here is 2.5 MB.
"""

import base64
import functools
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from Scripts import paths
from Scripts.store import ARTIFACTS, META_FILENAME

#: Overridable so a test or a second environment can point somewhere harmless
#: without editing code. The default is the bucket that already exists.
BUCKET = os.environ.get("ESPN_FFL_S3_BUCKET", "espn-ffl-data")

#: The bucket's region. Matching the caller's default profile avoids cross-region
#: egress, which is the only meaningful cost lever at this data volume.
REGION = os.environ.get("ESPN_FFL_S3_REGION", "us-east-2")

STORE_PREFIX = "store"
SNAPSHOT_PREFIX = "snapshots"

#: Local disk cache for objects read back out of S3, keyed by ETag. Gitignored and
#: safe to delete at any time -- a cold cache costs one download per artifact.
CACHE_DIR = paths.DATA_DIR / ".s3cache"

#: ``Data/<dir>`` to its S3 tier. Directories absent from this table are **not**
#: uploaded, which is the mechanism by which ``Data/Equivalence/`` (74 MB of
#: before/after debug snapshots from the espn-api 0.46.0 migration -- evidence of a
#: fixed bug, not live data) stays out of the bucket.
MIRROR_TIERS = {
    "G2": "archive/g2",
    "NFL": "nfl",
    "Projections": "projections",
    "Scoring": "scoring",
    "Injuries": "injuries",
}

#: Loose files at the top of ``Data/`` that belong to a tier despite not living in
#: its directory. Both are written by ``R/GetNFL.R``.
LOOSE_FILES = {
    "NFL_Schedules.csv": "nfl",
    "NFL_Tackles_By_Position.csv": "nfl",
}

_YEAR = re.compile(r"^\d{4}$")

#: Read timeouts are short on purpose. This runs in a Streamlit render path, and a
#: hung socket there is indistinguishable from a broken app; failing fast lets the
#: caller fall back to local disk while the user is still looking at the screen.
_CONNECT_TIMEOUT = 5
_READ_TIMEOUT = 15
_MAX_ATTEMPTS = 3


# --- client ---------------------------------------------------------------

@functools.cache
def client():
    """Create (once) and return the S3 client.

    Built on demand rather than at import, so importing this module on a machine
    with no AWS credentials still works. ``Scripts/aws_utils.py`` -- which this
    module replaces -- carried the scar from getting that wrong: it constructed a
    boto3 resource at module scope, and a wildcard import meant the FantasyPros
    scraper could not run without credentials, for a code path that was disabled.

    Returns:
        The boto3 S3 client, configured with explicit timeouts and retries.

    Raises:
        ImportError: If boto3 is not installed.
    """
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        region_name=REGION,
        config=Config(
            connect_timeout=_CONNECT_TIMEOUT,
            read_timeout=_READ_TIMEOUT,
            retries={"max_attempts": _MAX_ATTEMPTS, "mode": "standard"},
        ),
    )


def reset_client() -> None:
    """Drop the cached client. For tests, and after changing :data:`BUCKET`."""
    client.cache_clear()


# --- key mapping ----------------------------------------------------------

def _hive(part: str) -> str:
    """Turn a bare four-digit year into a Hive partition component.

    Args:
        part: One path component, e.g. ``"2026"`` or ``"Season"``.

    Returns:
        str: ``"season=2026"`` for a year, otherwise ``part`` unchanged.
    """
    return f"season={part}" if _YEAR.match(part) else part


def store_prefix(season: int, league_key: str) -> str:
    """The key prefix holding one league-season's store.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        str: e.g. ``"store/season=2026/league=knights_ffl/"``. Trailing slash, so it
        cannot match a sibling league whose key is a prefix of this one.
    """
    return f"{STORE_PREFIX}/season={int(season)}/league={league_key}/"


def store_key(season: int, league_key: str, what: str) -> str:
    """The key for one artifact of one league-season.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        what: An :data:`Scripts.store.ARTIFACTS` key, or ``"meta"``.

    Returns:
        str: The object key.

    Raises:
        KeyError: On an unknown artifact name, listing the valid ones.
    """
    if what == "meta":
        filename = META_FILENAME
    elif what in ARTIFACTS:
        filename = ARTIFACTS[what]
    else:
        raise KeyError(
            f"Unknown store artifact {what!r}. Known: {sorted(ARTIFACTS) + ['meta']}."
        )
    return store_prefix(season, league_key) + filename


def snapshot_key(season: int, league_key: str, what: str, date: str) -> str:
    """The key for a dated snapshot of one artifact.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        what: An :data:`Scripts.store.ARTIFACTS` key, e.g. ``"board"``.
        date: ``YYYY-MM-DD``, the build date being preserved.

    Returns:
        str: e.g.
        ``"snapshots/board/season=2026/league=knights_ffl/date=2026-08-11/board.parquet"``.

    Raises:
        KeyError: On an unknown artifact name.
        ValueError: When ``date`` is not ``YYYY-MM-DD`` -- a malformed date would
            produce a partition no query could prune on, and silently.
    """
    if what not in ARTIFACTS:
        raise KeyError(f"Unknown store artifact {what!r}. Known: {sorted(ARTIFACTS)}.")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        raise ValueError(f"date must be YYYY-MM-DD, got {date!r}.")
    return (f"{SNAPSHOT_PREFIX}/{what}/season={int(season)}/league={league_key}/"
            f"date={date}/{ARTIFACTS[what]}")


def mirror_key(local: Path) -> Optional[str]:
    """Map a local path under ``Data/`` to its S3 key, or None if it is excluded.

    Returning None rather than raising is what makes a whole-tree walk safe: the
    caller filters, and anything not named in :data:`MIRROR_TIERS` or
    :data:`LOOSE_FILES` is skipped without ceremony. ``Data/Store/`` is excluded
    here too -- it has its own Hive-partitioned schema via :func:`store_key`.

    Args:
        local: Path to a file, absolute or relative to the repo root.

    Returns:
        str | None: The object key, or None when the path is not mirrored.
    """
    path = paths.resolve(local)
    try:
        rel = path.relative_to(paths.DATA_DIR)
    except ValueError:
        return None

    parts = rel.parts
    if not parts:
        return None

    if len(parts) == 1:
        tier = LOOSE_FILES.get(parts[0])
        return f"{tier}/{parts[0]}" if tier else None

    tier = MIRROR_TIERS.get(parts[0])
    if tier is None:
        return None
    return "/".join([tier] + [_hive(p) for p in parts[1:]])


# --- checksums ------------------------------------------------------------

def sha256_b64(path: Path) -> str:
    """The base64 SHA-256 of a file, in the form S3 reports it.

    S3 returns ``ChecksumSHA256`` base64-encoded, so encoding ours the same way
    makes verification a string comparison rather than a decode-and-compare.

    Args:
        path: File to hash.

    Returns:
        str: Base64-encoded SHA-256 digest.
    """
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return base64.b64encode(digest.digest()).decode()


# --- writing --------------------------------------------------------------

def put_file(local: Path, key: str, *, dry_run: bool = False) -> str:
    """Upload one file, letting S3 verify the bytes it received.

    ``ChecksumAlgorithm="SHA256"`` makes S3 compute the digest server-side and reject
    the PUT if it disagrees with ours -- so a corrupted upload fails at the point of
    upload rather than becoming a silently bad object that :func:`verify` catches
    later, or worse, never.

    Args:
        local: File to upload.
        key: Destination object key.
        dry_run: Compute and return without calling S3.

    Returns:
        str: The base64 SHA-256 of the uploaded bytes.
    """
    checksum = sha256_b64(local)
    if dry_run:
        return checksum
    with open(local, "rb") as handle:
        client().put_object(
            Bucket=BUCKET,
            Key=key,
            Body=handle,
            ChecksumAlgorithm="SHA256",
        )
    return checksum


def push_league_store(season: int, league_key: str,
                      *, dry_run: bool = False) -> List[Tuple[str, str]]:
    """Upload one league-season's store, ``meta.json`` last.

    The ordering is the whole contract -- see the module docstring. Artifacts that
    do not exist locally are skipped rather than erroring, because ``--what board``
    builds a store that legitimately has no ``lineups.parquet``.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        dry_run: Resolve keys and checksums without uploading.

    Returns:
        list: ``(key, checksum)`` in upload order.
    """
    directory = paths.store_dir(season, league_key)
    uploaded: List[Tuple[str, str]] = []

    for what, filename in ARTIFACTS.items():
        local = directory / filename
        if local.is_file():
            key = store_key(season, league_key, what)
            uploaded.append((key, put_file(local, key, dry_run=dry_run)))

    meta_local = directory / META_FILENAME
    if meta_local.is_file():
        key = store_key(season, league_key, "meta")
        uploaded.append((key, put_file(meta_local, key, dry_run=dry_run)))
    return uploaded


def snapshot_board(season: int, league_key: str, date: str,
                   *, dry_run: bool = False) -> Optional[Tuple[str, str]]:
    """Preserve today's board under a dated key.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        date: ``YYYY-MM-DD``.
        dry_run: Resolve the key and checksum without uploading.

    Returns:
        tuple | None: ``(key, checksum)``, or None when this league has no board.
    """
    local = paths.store_dir(season, league_key) / ARTIFACTS["board"]
    if not local.is_file():
        return None
    key = snapshot_key(season, league_key, "board", date)
    return key, put_file(local, key, dry_run=dry_run)


# --- reading --------------------------------------------------------------

def list_objects(prefix: str) -> Dict[str, Dict[str, object]]:
    """Every object under ``prefix``, with its ETag and size.

    Args:
        prefix: Key prefix.

    Returns:
        dict: Key to ``{"etag": str, "size": int, "last_modified": datetime}``.
        Empty when the prefix holds nothing.
    """
    paginator = client().get_paginator("list_objects_v2")
    found: Dict[str, Dict[str, object]] = {}
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            found[obj["Key"]] = {
                "etag": obj["ETag"].strip('"'),
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
            }
    return found


def prefix_fingerprint(prefix: str) -> str:
    """A short string that changes whenever anything under ``prefix`` changes.

    This is the S3 replacement for ``Scripts.store.store_mtime`` as the app's cache
    key, and it costs **one** ``ListObjectsV2`` rather than a ``HeadObject`` per
    artifact. Streamlit's ``cache_data`` hashes its arguments, so passing this in
    means an upload invalidates the app's cache with no explicit clear anywhere --
    the same mechanism the local store already relies on.

    Args:
        prefix: Key prefix, normally :func:`store_prefix`.

    Returns:
        str: A hex digest over every key and ETag beneath the prefix, or ``""``
        when the prefix is empty. Empty is a meaningful value: it means "no store".
    """
    objects = list_objects(prefix)
    if not objects:
        return ""
    joined = "\n".join(f"{k}:{v['etag']}" for k, v in sorted(objects.items()))
    return hashlib.sha256(joined.encode()).hexdigest()[:32]


def _cache_path(key: str, etag: str) -> Path:
    """Where a downloaded object is cached locally.

    The ETag is part of the filename rather than held in a sidecar, so a cache hit
    is a single ``exists()`` and a stale entry is simply a file nobody asks for.

    Args:
        key: Object key.
        etag: The object's ETag.

    Returns:
        Path: Cache location. Not created.
    """
    key_path = Path(key)
    return CACHE_DIR / key_path.parent / f"{etag}__{key_path.name}"


def get_bytes(key: str, *, etag: Optional[str] = None, cache: bool = True) -> bytes:
    """Read one object, using the local ETag cache when it is warm.

    Args:
        key: Object key.
        etag: The object's known ETag, when the caller has already listed the
            prefix. Supplying it turns a cache hit into pure local disk with no S3
            round trip at all; omitting it costs a ``HeadObject``.
        cache: Read and populate the local ETag cache. :func:`download` passes
            False: it is writing the object to its real home on disk, so caching a
            second copy under ``.s3cache/`` would double the footprint of every
            ``--pull`` for no benefit -- 35 MB of store became 70 MB before this
            was an argument.

    Returns:
        bytes: The object's contents.

    Raises:
        FileNotFoundError: When the object does not exist, naming the key.
    """
    if etag is None and cache:
        try:
            etag = client().head_object(Bucket=BUCKET, Key=key)["ETag"].strip('"')
        except Exception as e:                              # noqa: BLE001
            if _is_missing(e):
                raise FileNotFoundError(f"s3://{BUCKET}/{key} does not exist.") from e
            raise

    cached = _cache_path(key, etag) if cache else None
    if cached is not None and cached.is_file():
        return cached.read_bytes()

    try:
        body = client().get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except Exception as e:                                  # noqa: BLE001
        if _is_missing(e):
            raise FileNotFoundError(f"s3://{BUCKET}/{key} does not exist.") from e
        raise

    if cached is not None:
        # Atomic, for the same reason store.py writes artifacts atomically: the app
        # may be reading this cache from another process while a refresh warms it.
        cached.parent.mkdir(parents=True, exist_ok=True)
        tmp = cached.with_suffix(cached.suffix + ".tmp")
        tmp.write_bytes(body)
        os.replace(tmp, cached)
    return body


def _is_missing(exc: Exception) -> bool:
    """Whether an exception is S3's "no such key/bucket" rather than a real fault.

    Args:
        exc: The exception raised by a boto3 call.

    Returns:
        bool: True for 404-class errors.
    """
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    return code in {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}


def download(key: str, local: Path) -> Path:
    """Write one object to a local path, atomically.

    Bypasses the ETag cache: the object is landing at its real location on disk, so
    a cached duplicate would only cost footprint. It also saves a ``HeadObject`` per
    file, since the cache key is what needed the ETag.

    Args:
        key: Object key.
        local: Destination path. Parents are created.

    Returns:
        Path: ``local``.

    Raises:
        FileNotFoundError: When the object does not exist.
    """
    body = get_bytes(key, cache=False)
    local.parent.mkdir(parents=True, exist_ok=True)
    tmp = local.with_suffix(local.suffix + ".tmp")
    tmp.write_bytes(body)
    os.replace(tmp, local)
    return local


def verify(local: Path, key: str) -> bool:
    """Whether the object in S3 is byte-identical to the local file.

    Compares SHA-256 rather than size or ETag. Size collides trivially and ETag is
    only an MD5 for single-part objects, so neither is a safe basis for the decision
    this feeds -- whether it is safe to stop tracking the local copy in git.

    Args:
        local: Local file.
        key: Object key.

    Returns:
        bool: True when both exist and their digests match.
    """
    try:
        head = client().head_object(Bucket=BUCKET, Key=key, ChecksumMode="ENABLED")
    except Exception as e:                                  # noqa: BLE001
        if _is_missing(e):
            return False
        raise
    remote = head.get("ChecksumSHA256")
    if not remote:
        return False
    return local.is_file() and remote == sha256_b64(local)


# --- store-shaped reads ---------------------------------------------------

def read_meta(season: int, league_key: str,
              *, missing_ok: bool = False) -> Optional[Dict[str, object]]:
    """Read a league-season's ``meta.json`` out of S3.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        missing_ok: Return None instead of raising when there is no store.

    Returns:
        dict | None: The metadata payload.

    Raises:
        FileNotFoundError: When no store exists and ``missing_ok`` is False, naming
            the command that would build and push one.
    """
    try:
        return json.loads(get_bytes(store_key(season, league_key, "meta")))
    except FileNotFoundError:
        if missing_ok:
            return None
        raise FileNotFoundError(
            f"No store in S3 for {league_key} {season}. Build and push one with "
            f"`python -m Scripts.refresh --league {league_key} --season {season}` "
            f"then `python -m Scripts.sync --push`."
        ) from None


def has_store(season: int, league_key: str) -> bool:
    """Whether a complete store exists in S3 for a league-season.

    Keys on ``meta.json`` for the same reason the local store does: it is uploaded
    last, so its presence is what distinguishes a finished push from a partial one.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        bool: True when the store is complete.
    """
    return read_meta(season, league_key, missing_ok=True) is not None


def _partition_values(prefix: str, field: str) -> List[str]:
    """Distinct values of a Hive partition field directly beneath ``prefix``.

    Args:
        prefix: Key prefix to list under.
        field: Partition field name, e.g. ``"league"``.

    Returns:
        list: Sorted distinct values.
    """
    found = set()
    for key in list_objects(prefix):
        for part in key.split("/"):
            if part.startswith(f"{field}="):
                found.add(part.split("=", 1)[1])
                break
    return sorted(found)


def list_leagues(season: int) -> List[str]:
    """League keys with a complete store in S3 for ``season``.

    Args:
        season: Season year.

    Returns:
        list: Sorted league keys. A league whose ``meta.json`` is absent is skipped,
        matching :func:`Scripts.store.list_leagues`.
    """
    prefix = f"{STORE_PREFIX}/season={int(season)}/"
    objects = list_objects(prefix)
    complete = {
        key.split("league=", 1)[1].split("/", 1)[0]
        for key in objects
        if key.endswith(f"/{META_FILENAME}") and "league=" in key
    }
    return sorted(complete)


def list_seasons() -> List[int]:
    """Seasons with at least one complete league store in S3.

    Returns:
        list: Season years, newest first.
    """
    seasons = []
    for value in _partition_values(f"{STORE_PREFIX}/", "season"):
        if value.isdigit() and list_leagues(int(value)):
            seasons.append(int(value))
    return sorted(seasons, reverse=True)


def iter_mirror_files(dirs: Optional[Iterable[str]] = None) -> Iterable[Tuple[Path, str]]:
    """Every local file that belongs in the mirrored tiers, with its key.

    Args:
        dirs: ``Data/`` subdirectory names to walk, e.g. ``("NFL",)``. Defaults to
            every key of :data:`MIRROR_TIERS`. The loose top-level files are
            included only when ``"NFL"`` is among them, since both belong to that
            tier despite not living in its directory.

    Yields:
        tuple: ``(local_path, key)`` for each mirrored file that exists, sorted
        within each directory so a dry run reads the same way twice.
    """
    wanted = list(MIRROR_TIERS) if dirs is None else [d for d in dirs
                                                     if d in MIRROR_TIERS]

    if "NFL" in wanted:
        for name in LOOSE_FILES:
            local = paths.DATA_DIR / name
            if local.is_file():
                key = mirror_key(local)
                if key:
                    yield local, key

    for directory in wanted:
        root = paths.DATA_DIR / directory
        if not root.is_dir():
            continue
        for local in sorted(root.rglob("*")):
            if not local.is_file() or local.name.startswith("."):
                continue
            key = mirror_key(local)
            if key:
                yield local, key
