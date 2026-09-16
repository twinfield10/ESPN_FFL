"""The S3 boundary, against a stubbed client. No network, no credentials.

Three properties here are load-bearing rather than incidental, and each has a test
that fails loudly if it regresses:

1. **``meta.json`` is uploaded last.** It is the completeness sentinel on both sides
   of the boundary, so an upload order that put it first would make a half-pushed
   store indistinguishable from a finished one.
2. **The mirror excludes what it must.** ``mirror_key`` returning None is the only
   thing keeping 74 MB of Equivalence debug snapshots and the machine-local
   ``refresh_status.json`` out of the bucket.
3. **Verification compares digests, not sizes or ETags.** It is the gate in front of
   untracking data from git, so a false pass there loses data permanently.
"""

import base64
import hashlib
import json

import pytest

from Scripts import paths, s3_store


def _write_store(tmp_path, season=2026, league="knights_ffl", artifacts=("board",)):
    """Create a local league-season store with the named artifacts plus meta."""
    directory = paths.store_dir(season, league, create=True)
    from Scripts.store import ARTIFACTS, META_FILENAME
    for what in artifacts:
        (directory / ARTIFACTS[what]).write_bytes(f"{what}-bytes".encode())
    (directory / META_FILENAME).write_text(json.dumps({"season": season}))
    return directory


# --- key mapping ---------------------------------------------------------

def test_store_keys_are_hive_partitioned():
    """Athena and DuckDB prune on `key=value` components; a bare `2026/` segment is
    just a directory to them."""
    assert (s3_store.store_key(2026, "knights_ffl", "board")
            == "store/season=2026/league=knights_ffl/board.parquet")
    assert (s3_store.store_key(2026, "knights_ffl", "meta")
            == "store/season=2026/league=knights_ffl/meta.json")


def test_the_store_prefix_ends_in_a_slash():
    """Without it, league=gop would also match league=gop_degenerates."""
    prefix = s3_store.store_prefix(2026, "gop")
    assert prefix.endswith("/")
    assert not s3_store.store_key(2026, "gop_degenerates", "board").startswith(prefix)


def test_unknown_artifacts_are_rejected_by_name():
    with pytest.raises(KeyError, match="board"):
        s3_store.store_key(2026, "knights_ffl", "nonsense")


def test_snapshot_keys_carry_the_date_as_a_partition():
    assert (s3_store.snapshot_key(2026, "knights_ffl", "board", "2026-08-11")
            == "snapshots/board/season=2026/league=knights_ffl/"
               "date=2026-08-11/board.parquet")


def test_a_malformed_snapshot_date_raises_rather_than_writing():
    """A bad date would create a partition no query can prune on, silently."""
    for bad in ("2026-8-11", "11-08-2026", "today", ""):
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            s3_store.snapshot_key(2026, "knights_ffl", "board", bad)


@pytest.mark.parametrize("local,expected", [
    ("Data/G2/2026/manifest.json", "archive/g2/season=2026/manifest.json"),
    ("Data/NFL/2026/depth_charts.parquet", "nfl/season=2026/depth_charts.parquet"),
    ("Data/NFL/player_ids.parquet", "nfl/player_ids.parquet"),
    ("Data/Injuries/2026/espn_injuries.parquet",
     "injuries/season=2026/espn_injuries.parquet"),
    ("Data/NFL_Schedules.csv", "nfl/NFL_Schedules.csv"),
    ("Data/Projections/Usage/Season/2026/x.parquet",
     "projections/Usage/Season/season=2026/x.parquet"),
])
def test_mirror_keys_hive_partition_every_bare_year(local, expected):
    """One uniform rule across every tier, rather than a special case per directory."""
    assert s3_store.mirror_key(paths.resolve(local)) == expected


@pytest.mark.parametrize("excluded", [
    "Data/Equivalence/before/Knights_FFL_2025.parquet",
    "Data/refresh_status.json",
    "Data/Store/2026/knights_ffl/board.parquet",
    "README.md",
])
def test_the_mirror_excludes_what_does_not_belong_in_the_bucket(excluded):
    """Equivalence is 74 MB of evidence about a fixed bug; refresh_status is a fact
    about one laptop; the store has its own schema. None are mirrored."""
    assert s3_store.mirror_key(paths.resolve(excluded)) is None


# --- the meta-last invariant ---------------------------------------------

def test_meta_json_is_uploaded_last(s3_stub, tmp_path):
    """The completeness sentinel, on both sides of the boundary. Uploaded first, a
    half-pushed store would read as finished."""
    _write_store(tmp_path, artifacts=("board", "draft", "tendencies"))
    s3_store.push_league_store(2026, "knights_ffl")
    assert s3_stub.put_order[-1].endswith("meta.json")
    assert len(s3_stub.put_order) == 4


def test_absent_artifacts_are_skipped_not_errors(s3_stub, tmp_path):
    """`--what board` builds a store with no lineups.parquet, legitimately."""
    _write_store(tmp_path, artifacts=("board",))
    uploaded = s3_store.push_league_store(2026, "knights_ffl").uploaded
    assert [k.rsplit("/", 1)[1] for k in dict(uploaded)] == ["board.parquet",
                                                            "meta.json"]


def test_a_dry_run_writes_nothing(s3_stub, tmp_path):
    _write_store(tmp_path, artifacts=("board",))
    uploaded = s3_store.push_league_store(2026, "knights_ffl", dry_run=True).uploaded
    assert len(uploaded) == 2
    assert s3_stub.put_order == []


# --- fingerprint, the app's cache key ------------------------------------

def test_an_empty_prefix_fingerprints_as_empty(s3_stub):
    """Empty is meaningful: it means 'no store', and the app branches on it."""
    assert s3_store.prefix_fingerprint("store/season=2026/") == ""


def test_the_fingerprint_moves_when_an_artifact_changes(s3_stub, tmp_path):
    """This is what invalidates Streamlit's cache. If it did not move, a refresh
    would be invisible to the app until the TTL expired."""
    _write_store(tmp_path, artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")
    before = s3_store.prefix_fingerprint(s3_store.store_prefix(2026, "knights_ffl"))

    (paths.store_dir(2026, "knights_ffl") / "board.parquet").write_bytes(b"new bytes")
    s3_store.push_league_store(2026, "knights_ffl")
    after = s3_store.prefix_fingerprint(s3_store.store_prefix(2026, "knights_ffl"))

    assert before and after and before != after


def test_the_fingerprint_is_stable_when_nothing_changed(s3_stub, tmp_path):
    """A fingerprint that churned would defeat the cache entirely."""
    _write_store(tmp_path, artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")
    prefix = s3_store.store_prefix(2026, "knights_ffl")
    assert s3_store.prefix_fingerprint(prefix) == s3_store.prefix_fingerprint(prefix)


def test_one_league_does_not_fingerprint_another(s3_stub, tmp_path):
    _write_store(tmp_path, league="knights_ffl", artifacts=("board",))
    _write_store(tmp_path, league="gop_degenerates", artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")
    s3_store.push_league_store(2026, "gop_degenerates")
    assert (s3_store.prefix_fingerprint(s3_store.store_prefix(2026, "knights_ffl"))
            != s3_store.prefix_fingerprint(
                s3_store.store_prefix(2026, "gop_degenerates")))


# --- the ETag cache ------------------------------------------------------

def test_a_warm_cache_does_not_refetch(s3_stub, tmp_path):
    """The read path is a render path. One download per ETag, not per render."""
    _write_store(tmp_path, artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")
    key = s3_store.store_key(2026, "knights_ffl", "board")

    first = s3_store.get_bytes(key)
    second = s3_store.get_bytes(key)
    assert first == second == b"board-bytes"
    assert s3_stub.get_calls == [key]          # fetched once, not twice


def test_a_changed_object_busts_the_cache(s3_stub, tmp_path):
    """Keying the cache filename on the ETag is what makes this automatic."""
    _write_store(tmp_path, artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")
    key = s3_store.store_key(2026, "knights_ffl", "board")
    assert s3_store.get_bytes(key) == b"board-bytes"

    (paths.store_dir(2026, "knights_ffl") / "board.parquet").write_bytes(b"fresher")
    s3_store.push_league_store(2026, "knights_ffl")
    assert s3_store.get_bytes(key) == b"fresher"


def test_a_missing_object_raises_filenotfound_naming_the_key(s3_stub):
    with pytest.raises(FileNotFoundError, match="store/season=2026"):
        s3_store.get_bytes(s3_store.store_key(2026, "knights_ffl", "board"))


def test_download_does_not_also_populate_the_cache(s3_stub, tmp_path):
    """The object is landing at its real path, so a cached duplicate is pure
    footprint -- 35 MB of store became 70 MB on every --pull before this."""
    _write_store(tmp_path, artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")
    key = s3_store.store_key(2026, "knights_ffl", "board")

    destination = tmp_path / "pulled" / "board.parquet"
    s3_store.download(key, destination)

    assert destination.read_bytes() == b"board-bytes"
    assert not s3_store.CACHE_DIR.exists()


# --- verification, the gate in front of untracking from git --------------

def test_verify_passes_on_identical_bytes(s3_stub, tmp_path):
    _write_store(tmp_path, artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")
    local = paths.store_dir(2026, "knights_ffl") / "board.parquet"
    assert s3_store.verify(local, s3_store.store_key(2026, "knights_ffl", "board"))


def test_verify_fails_when_the_local_file_moved_on(s3_stub, tmp_path):
    """The dangerous direction: local changed, S3 did not, and we are about to stop
    tracking local in git."""
    _write_store(tmp_path, artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")
    local = paths.store_dir(2026, "knights_ffl") / "board.parquet"
    local.write_bytes(b"diverged")
    assert not s3_store.verify(local, s3_store.store_key(2026, "knights_ffl", "board"))


def test_verify_fails_rather_than_raises_when_the_object_is_absent(s3_stub, tmp_path):
    _write_store(tmp_path, artifacts=("board",))
    local = paths.store_dir(2026, "knights_ffl") / "board.parquet"
    assert not s3_store.verify(local, "store/season=2026/league=nobody/board.parquet")


def test_our_digest_matches_the_format_s3_reports(tmp_path):
    """Base64, not hex -- so verification is a string compare rather than a decode."""
    path = tmp_path / "f.bin"
    path.write_bytes(b"payload")
    expected = base64.b64encode(hashlib.sha256(b"payload").digest()).decode()
    assert s3_store.sha256_b64(path) == expected


# --- store-shaped reads --------------------------------------------------

def test_a_store_without_meta_is_not_a_store(s3_stub, tmp_path):
    """Same rule as on disk: meta.json is uploaded last, so its absence means the
    push is in flight or it failed partway."""
    _write_store(tmp_path, artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")
    del s3_stub.objects[s3_store.store_key(2026, "knights_ffl", "meta")]
    assert not s3_store.has_store(2026, "knights_ffl")
    assert s3_store.list_leagues(2026) == []


def test_list_leagues_and_seasons_read_the_partitions(s3_stub, tmp_path):
    _write_store(tmp_path, season=2026, league="knights_ffl", artifacts=("board",))
    _write_store(tmp_path, season=2026, league="gop_degenerates", artifacts=("board",))
    _write_store(tmp_path, season=2025, league="knights_ffl", artifacts=("lineups",))
    for season, league in ((2026, "knights_ffl"), (2026, "gop_degenerates"),
                           (2025, "knights_ffl")):
        s3_store.push_league_store(season, league)

    assert s3_store.list_leagues(2026) == ["gop_degenerates", "knights_ffl"]
    assert s3_store.list_seasons() == [2026, 2025]


def test_a_missing_store_names_the_command_that_builds_one(s3_stub):
    """The repo's convention: an error that tells you what to run."""
    with pytest.raises(FileNotFoundError, match="Scripts.sync --push"):
        s3_store.read_meta(2026, "knights_ffl")


def test_read_meta_can_be_asked_not_to_raise(s3_stub):
    assert s3_store.read_meta(2026, "knights_ffl", missing_ok=True) is None


# --- the dedup, which is why store/ was 7.7 GB ---------------------------

def test_identical_bytes_are_not_re_uploaded(s3_stub, tmp_path):
    """The bucket is versioned with a 90-day non-current expiry, so an identical PUT
    does not overwrite -- it mints a retained version.

    Measured 2026-09-16 on `winfield_football` alone: 207 versions of `board.parquet`
    holding 59 distinct boards, and 156 versions of `board_frozen.parquet` holding
    exactly one, because a frozen board cannot change and was re-sent every night and
    every ten minutes through a slate anyway. Across ten leagues that was 7.70 GB of
    non-current versions against 58 MB of current ones.
    """
    _write_store(tmp_path, artifacts=("board", "lineups"))
    first = s3_store.push_league_store(2026, "knights_ffl")
    assert len(first.uploaded) == 3 and not first.skipped

    second = s3_store.push_league_store(2026, "knights_ffl")
    assert not second.uploaded
    assert len(second.skipped) == 3


def test_a_changed_artifact_still_uploads_while_its_neighbours_are_skipped(
        s3_stub, tmp_path):
    """The live loop's shape: `--what live` rewrites one file of eight."""
    directory = _write_store(tmp_path, artifacts=("board", "lineups"))
    s3_store.push_league_store(2026, "knights_ffl")

    from Scripts.store import ARTIFACTS
    (directory / ARTIFACTS["lineups"]).write_bytes(b"new-lineups-bytes")

    result = s3_store.push_league_store(2026, "knights_ffl")
    assert [k.rsplit("/", 1)[1] for k, _ in result.uploaded] == ["lineups.parquet"]
    # `board.parquet` and `meta.json` both skipped. Meta is skipped here only because
    # this fixture rewrites identical bytes; a real build moves `built_at`, so it
    # moves too. Nothing keys change detection on meta alone -- `app/store._version`
    # fingerprints the whole prefix -- so an unchanged meta beside a changed artifact
    # still invalidates the app's cache.
    assert [k.rsplit("/", 1)[1] for k in result.skipped] == ["board.parquet",
                                                            "meta.json"]


def test_only_restricts_what_is_considered(s3_stub, tmp_path):
    """A refresh that built one artifact has no business listing and hashing eight."""
    _write_store(tmp_path, artifacts=("board", "lineups"))
    result = s3_store.push_league_store(2026, "knights_ffl", only=["lineups"],
                                        skip_unchanged=False)
    assert [k.rsplit("/", 1)[1] for k, _ in result.uploaded] == ["lineups.parquet",
                                                                "meta.json"]


def test_only_always_carries_meta(s3_stub, tmp_path):
    """`meta.json` is the completeness sentinel. A store whose artifacts moved
    without it is a store no reader notices has changed."""
    _write_store(tmp_path, artifacts=("board",))
    result = s3_store.push_league_store(2026, "knights_ffl", only=["board"],
                                        skip_unchanged=False)
    assert result.uploaded[-1][0].endswith("meta.json")


def test_an_unknown_only_value_raises_rather_than_pushing_nothing(s3_stub, tmp_path):
    """A typo that silently published zero objects would look exactly like a store
    that had not changed."""
    _write_store(tmp_path, artifacts=("board",))
    with pytest.raises(KeyError, match="lineup"):
        s3_store.push_league_store(2026, "knights_ffl", only=["lineup"])


def test_a_failed_listing_uploads_rather_than_skipping(s3_stub, tmp_path,
                                                       monkeypatch):
    """Any doubt uploads. A "nothing to do" that really means "could not tell" is
    this repo's recurring failure mode wearing a different hat."""
    _write_store(tmp_path, artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")

    def explode(prefix):
        raise RuntimeError("no network")

    monkeypatch.setattr(s3_store, "list_objects", explode)
    result = s3_store.push_league_store(2026, "knights_ffl")
    assert len(result.uploaded) == 2 and not result.skipped


def test_the_etag_is_the_local_md5_for_a_single_part_object(s3_stub, tmp_path):
    """What the whole dedup rests on. Verified against the live bucket 2026-09-16:
    `lineups.parquet`'s ETag equalled its local MD5 exactly."""
    directory = _write_store(tmp_path, artifacts=("board",))
    s3_store.push_league_store(2026, "knights_ffl")
    key = s3_store.store_key(2026, "knights_ffl", "board")
    from Scripts.store import ARTIFACTS
    assert (s3_store.list_objects(s3_store.store_prefix(2026, "knights_ffl"))[key]
            ["etag"] == s3_store.md5_hex(directory / ARTIFACTS["board"]))


def test_a_snapshot_of_an_unmoved_board_is_not_written(s3_stub, tmp_path):
    """A second dated copy of yesterday's bytes records nothing, and `snapshots/` has
    no non-current expiry rule and grows ~21 MB a night.

    This became the normal case when the board stage left the nightly on 2026-09-16:
    without the check, `sync --push` would mint an identical board under a new date
    every night for the rest of the season.
    """
    _write_store(tmp_path, artifacts=("board",))
    first = s3_store.snapshot_board(2026, "knights_ffl", "2026-09-16")
    assert first is not None

    assert s3_store.snapshot_board(2026, "knights_ffl", "2026-09-17") is None
    assert not any("date=2026-09-17" in k for k in s3_stub.objects)


def test_a_moved_board_still_snapshots(s3_stub, tmp_path):
    directory = _write_store(tmp_path, artifacts=("board",))
    s3_store.snapshot_board(2026, "knights_ffl", "2026-09-16")

    from Scripts.store import ARTIFACTS
    (directory / ARTIFACTS["board"]).write_bytes(b"adp-moved")

    assert s3_store.snapshot_board(2026, "knights_ffl", "2026-09-17") is not None
    assert any("date=2026-09-17" in k for k in s3_stub.objects)


def test_a_gap_in_the_dates_does_not_force_a_spurious_snapshot(s3_stub, tmp_path):
    """Compared against the newest existing snapshot, not against yesterday's key --
    so a missed nightly does not look like a board that moved."""
    _write_store(tmp_path, artifacts=("board",))
    s3_store.snapshot_board(2026, "knights_ffl", "2026-09-10")
    assert s3_store.snapshot_board(2026, "knights_ffl", "2026-09-16") is None
