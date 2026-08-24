"""The sync CLI, against a stubbed S3. No network, no credentials.

What is worth pinning here is not that a happy-path upload works -- it is that the
failing paths stay loud. This repo's recurring defect, recorded across plans 01, 03
and 22, is an absent source reading as agreement, and a sync layer is an ideal place
for that to recur: an upload that silently did nothing looks exactly like an upload
that worked, right up until the local copy is gone.

So: a dry run must write nothing, a failed upload must reach the exit code, and
``--verify`` must fail on a difference rather than on an exception.
"""

import json

import pytest

from Scripts import paths, s3_store, sync
from Scripts.store import ARTIFACTS, META_FILENAME


def _store(season=2026, league="knights_ffl", artifacts=("board",)):
    """A local league-season store with the named artifacts plus meta."""
    directory = paths.store_dir(season, league, create=True)
    for what in artifacts:
        (directory / ARTIFACTS[what]).write_bytes(f"{what}-bytes".encode())
    (directory / META_FILENAME).write_text(json.dumps({"season": season}))
    return directory


def _mirror_file(relative, data=b"payload"):
    """A file in a mirrored tier, e.g. ``G2/2026/manifest.json``."""
    path = paths.DATA_DIR / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


# --- push ----------------------------------------------------------------

def test_a_dry_run_writes_nothing_at_all(s3_env):
    """The point of --dry-run is to be safe to run against production by accident."""
    _store()
    _mirror_file("G2/2026/manifest.json")
    count, failures = sync.push(["store", "archive"], 2026, dry_run=True,
                                snapshot_date="2026-08-11")
    assert count > 0 and not failures
    assert s3_env.objects == {}


def test_push_uploads_the_store_and_the_mirror(s3_env):
    _store(artifacts=("board", "draft"))
    _mirror_file("G2/2026/manifest.json")
    _mirror_file("NFL/2026/depth_charts.parquet")

    count, failures = sync.push(["store", "archive", "nfl"], 2026,
                                snapshot_date=None)
    assert not failures
    assert count == len(s3_env.objects) == 5
    assert "store/season=2026/league=knights_ffl/board.parquet" in s3_env.objects
    assert "archive/g2/season=2026/manifest.json" in s3_env.objects
    assert "nfl/season=2026/depth_charts.parquet" in s3_env.objects


def test_an_unchanged_mirror_file_is_not_re_uploaded(s3_env):
    """The bandwidth guard the play-by-play archive made necessary.

    `R/GetPBP.R` takes ``Data/NFL`` from ~40 MB to ~540 MB, and 26 of its 27 seasons
    are completed seasons that cannot change. Without this, the nightly sends half a
    gigabyte every night to arrive at identical objects.
    """
    _mirror_file("NFL/2026/pbp.parquet", b"plays")
    first, _ = sync.push(["nfl"], 2026, snapshot_date=None)
    assert first == 1

    s3_env.put_order.clear()
    second, failures = sync.push(["nfl"], 2026, snapshot_date=None)
    assert (second, failures, s3_env.put_order) == (0, [], [])


def test_changed_bytes_are_uploaded_even_at_the_same_size(s3_env):
    """Same length, different content -- so the check cannot be size or mtime."""
    path = _mirror_file("NFL/2026/pbp.parquet", b"plays")
    sync.push(["nfl"], 2026, snapshot_date=None)

    path.write_bytes(b"PLAYS")
    s3_env.put_order.clear()
    count, failures = sync.push(["nfl"], 2026, snapshot_date=None)
    assert count == 1 and not failures
    assert s3_env.objects["nfl/season=2026/pbp.parquet"] == b"PLAYS"


def test_a_head_that_errors_uploads_rather_than_skipping(s3_env, monkeypatch):
    """Any doubt uploads. A "nothing to do" that really means "could not tell" is
    this repo's recurring failure mode wearing a different hat."""
    _mirror_file("NFL/2026/pbp.parquet", b"plays")
    sync.push(["nfl"], 2026, snapshot_date=None)

    def explode(*_a, **_k):
        raise RuntimeError("S3 unreachable")
    monkeypatch.setattr(s3_store, "verify", explode)

    s3_env.put_order.clear()
    count, failures = sync.push(["nfl"], 2026, snapshot_date=None)
    assert count == 1 and not failures


def test_skip_unchanged_can_be_turned_off(s3_env):
    """A forced re-push has to stay available -- it is the repair for a bucket
    someone edited out from under the mirror."""
    _mirror_file("NFL/2026/pbp.parquet", b"plays")
    sync.push(["nfl"], 2026, snapshot_date=None)
    s3_env.put_order.clear()
    count, _ = sync.push(["nfl"], 2026, snapshot_date=None, skip_unchanged=False)
    assert count == 1


def test_the_snapshot_lands_under_a_dated_partition(s3_env):
    """The capability G2 had to be hand-built to get: a board that survives the day
    it was built."""
    _store(artifacts=("board",))
    sync.push(["store"], 2026, snapshot_date="2026-08-11")
    assert ("snapshots/board/season=2026/league=knights_ffl/"
            "date=2026-08-11/board.parquet") in s3_env.objects


def test_no_snapshot_means_no_dated_key(s3_env):
    _store(artifacts=("board",))
    sync.push(["store"], 2026, snapshot_date=None)
    assert not any(k.startswith("snapshots/") for k in s3_env.objects)


def test_an_empty_local_store_is_a_failure_not_a_silent_success(s3_env):
    """Uploading nothing and reporting success is the exact shape of the bug this
    repo keeps finding."""
    count, failures = sync.push(["store"], 2026, snapshot_date=None)
    assert count == 0
    assert failures and "no local store" in failures[0]


def test_an_upload_error_is_collected_rather_than_raised(s3_env, monkeypatch):
    """One league failing must not abort the other eight, but it must still be
    reported -- the same contract Scripts.refresh already keeps."""
    _store(league="knights_ffl", artifacts=("board",))
    _store(league="gop_degenerates", artifacts=("board",))

    def explode(*a, **kw):
        raise RuntimeError("connection reset")
    monkeypatch.setattr(s3_store, "push_league_store", explode)

    count, failures = sync.push(["store"], 2026, snapshot_date=None)
    assert count == 0
    assert len(failures) == 2
    assert all("connection reset" in f for f in failures)


# --- verify --------------------------------------------------------------

def test_verify_passes_after_a_push(s3_env):
    _store(artifacts=("board", "draft"))
    _mirror_file("G2/2026/manifest.json")
    sync.push(["store", "archive"], 2026, snapshot_date=None)

    checked, mismatches = sync.verify(["store", "archive"], 2026)
    assert checked == 4 and mismatches == []


def test_verify_catches_a_local_file_that_moved_on(s3_env):
    """The direction that matters before untracking from git."""
    _store(artifacts=("board",))
    sync.push(["store"], 2026, snapshot_date=None)
    (paths.store_dir(2026, "knights_ffl") / "board.parquet").write_bytes(b"diverged")

    checked, mismatches = sync.verify(["store"], 2026)
    assert len(mismatches) == 1 and "board.parquet" in mismatches[0]


def test_verify_catches_a_file_that_was_never_uploaded(s3_env):
    _store(artifacts=("board",))
    _mirror_file("G2/2026/manifest.json")
    sync.push(["store"], 2026, snapshot_date=None)      # archive deliberately omitted

    checked, mismatches = sync.verify(["archive"], 2026)
    assert checked == 1 and len(mismatches) == 1


# --- pull, the claim that S3 is the record -------------------------------

def test_pull_rebuilds_a_store_from_nothing(s3_env):
    """A machine with an empty Data/ must reach a working app from S3 alone."""
    _store(artifacts=("board", "draft"))
    sync.push(["store"], 2026, snapshot_date=None)

    import shutil
    shutil.rmtree(paths.store_root())

    downloaded, failures = sync.pull(["store"], 2026)
    assert not failures and downloaded == 3
    directory = paths.store_dir(2026, "knights_ffl")
    assert (directory / "board.parquet").read_bytes() == b"board-bytes"
    assert json.loads((directory / META_FILENAME).read_text())["season"] == 2026


def test_pull_restores_mirrored_files_to_their_original_paths(s3_env):
    """The key mapping has to invert cleanly, including the Hive season= components
    and the loose top-level CSVs that do not live in their tier's directory."""
    _mirror_file("G2/2026/manifest.json", b"g2")
    _mirror_file("NFL/2026/depth_charts.parquet", b"depth")
    _mirror_file("NFL/player_ids.parquet", b"ids")
    _mirror_file("NFL_Schedules.csv", b"sched")
    sync.push(["archive", "nfl"], 2026, snapshot_date=None)

    for relative in ("G2/2026/manifest.json", "NFL/2026/depth_charts.parquet",
                     "NFL/player_ids.parquet", "NFL_Schedules.csv"):
        (paths.DATA_DIR / relative).unlink()

    downloaded, failures = sync.pull(["archive", "nfl"], 2026)
    assert not failures and downloaded == 4
    assert (paths.DATA_DIR / "G2/2026/manifest.json").read_bytes() == b"g2"
    assert (paths.DATA_DIR / "NFL/2026/depth_charts.parquet").read_bytes() == b"depth"
    assert (paths.DATA_DIR / "NFL/player_ids.parquet").read_bytes() == b"ids"
    assert (paths.DATA_DIR / "NFL_Schedules.csv").read_bytes() == b"sched"


# --- the CLI contract ----------------------------------------------------

def test_verify_exits_non_zero_on_a_mismatch(s3_env, capsys):
    """It is meant to compose into a shell && without its output being parsed."""
    _store(artifacts=("board",))
    sync.push(["store"], 2026, snapshot_date=None)
    (paths.store_dir(2026, "knights_ffl") / "board.parquet").write_bytes(b"diverged")

    assert sync.main(["--verify", "--what", "store"]) == 1
    assert "DIFFER" in capsys.readouterr().out


def test_verify_exits_zero_when_everything_matches(s3_env):
    _store(artifacts=("board",))
    sync.push(["store"], 2026, snapshot_date=None)
    assert sync.main(["--verify", "--what", "store"]) == 0


def test_an_unknown_tier_is_rejected_by_name(s3_env):
    with pytest.raises(SystemExit):
        sync.main(["--push", "--what", "nonsense"])


def test_push_and_pull_are_mutually_exclusive(s3_env):
    with pytest.raises(SystemExit):
        sync.main(["--push", "--pull"])
