"""The waiver-wire snapshot, which is the only store artifact that accumulates.

Every other artifact is rebuilt from ESPN on demand. This one cannot be:
``league.free_agents()`` serves the pool as it is *now*, so once a nightly has run,
that morning's wire is gone. The 2025 stores are the evidence -- nine leagues, all
seventeen weeks of rosters, and free-agent rows on **week 17 only**.

So the tests that matter here are not about the columns. They are about the two
ways an accumulating artifact silently loses a season: writing an empty frame over
a real history, and appending the same week once per nightly until the file is
ninety copies of week 4.
"""

import pandas as pd
import pytest

from Scripts import pool as pm


STAMP = pd.Timestamp("2026-09-10T06:00:00Z")


def lineups(week=1, owners=("Free Agent", "Tommy Winfield"), n=2):
    """A minimal ``clean_lineups`` frame with a pool on it."""
    rows = []
    for owner in owners:
        for i in range(n):
            rows.append({
                "league_id": 1727104, "year": 2026, "week": week,
                "team_owner": owner,
                "player_id": hash((owner, i)) % 10**6,
                "player_name": f"{owner}-{i}",
                "player_position": "WR", "primaryPosition": "WR",
                "eligiblePositions": ["WR", "RB/WR/TE"],
                "pro_team": "DEN", "player_active_status": "active",
                "game_state": "pre", "game_elapsed": 0.0, "game_locked": False,
                "points": 0.0, "ESPN_Points": 10.0 + i, "TRUE_Points": 9.0 + i,
            })
    return pd.DataFrame(rows)


def board(frame):
    """A season board carrying the two columns the weekly artifact lacks."""
    return pd.DataFrame({
        "player_id": frame["player_id"],
        "percent_owned": 12.5,
        "injury_status": "ACTIVE",
    })


def test_the_snapshot_is_the_pool_and_nothing_else():
    frame = lineups()
    snap = pm.snapshot(frame, 1)
    assert len(snap) == 2
    assert set(snap["player_name"]) == {"Free Agent-0", "Free Agent-1"}


def test_the_snapshot_carries_what_a_replay_needs():
    """Not the 625 columns of `lineups.parquet` -- but `eligiblePositions` has to
    survive, because slot-by-slot comparison is what the suggestions are built on
    and a snapshot that cannot reproduce it could not replay a decision."""
    snap = pm.snapshot(lineups(), 1)
    for column in ("week", "player_id", "eligiblePositions", "TRUE_Points",
                   "game_locked", "captured_at"):
        assert column in snap.columns


def test_the_board_supplies_what_the_weekly_artifact_does_not():
    frame = lineups()
    snap = pm.snapshot(frame, 1, board(frame))
    assert snap["percent_owned"].notna().all()
    assert snap["injury_status"].eq("ACTIVE").all()


def test_a_missing_board_is_not_an_error():
    """`--what pool` can run against a store whose board was never built."""
    snap = pm.snapshot(lineups(), 1, None)
    assert len(snap) == 2
    assert "percent_owned" not in snap.columns


def test_a_week_with_no_pool_snapshots_nothing():
    assert pm.snapshot(lineups(week=3), 1).empty


def test_the_same_week_twice_does_not_duplicate_it():
    """The nightly runs daily and `--what live` every ten minutes. A true append
    would hold ninety copies of week 4 by Friday."""
    snap = pm.snapshot(lineups(), 1, captured_at=STAMP)
    once = pm.accumulate(None, snap)
    twice = pm.accumulate(once, snap)
    assert len(once) == len(twice) == 2
    assert pm.weeks_present(twice) == [1]


def test_the_latest_capture_of_a_week_wins():
    """Tuesday's wire has moved since Monday's, and Tuesday is the truth."""
    monday = pm.snapshot(lineups(), 1, captured_at=STAMP)
    tuesday = monday.copy()
    tuesday["TRUE_Points"] = 99.0
    tuesday["captured_at"] = STAMP + pd.Timedelta(days=1)
    history = pm.accumulate(pm.accumulate(None, monday), tuesday)
    assert len(history) == 2
    assert history["TRUE_Points"].eq(99.0).all()


def test_a_new_week_is_added_rather_than_replacing_the_old():
    week1 = pm.snapshot(lineups(week=1), 1, captured_at=STAMP)
    week2 = pm.snapshot(lineups(week=2), 2, captured_at=STAMP)
    history = pm.accumulate(pm.accumulate(None, week1), week2)
    assert pm.weeks_present(history) == [1, 2]
    assert len(history) == 4


def test_an_empty_snapshot_is_refused_rather_than_written():
    """**The test that protects the ability to measure anything at all.** In an
    accumulating store "nobody was available" and "the fetch broke" produce the
    same file, and the history an empty write would truncate is not rebuildable --
    ESPN will not serve a past week's wire at any price."""
    history = pm.accumulate(None, pm.snapshot(lineups(), 1))
    with pytest.raises(ValueError, match="empty"):
        pm.accumulate(history, pd.DataFrame())


def test_weeks_present_on_nothing():
    assert pm.weeks_present(None) == []
    assert pm.weeks_present(pd.DataFrame()) == []


def test_the_artifact_is_registered_so_sync_and_catalogue_see_it():
    """`Scripts/sync.py` and `Scripts/catalogue.py` both iterate `ARTIFACTS`, so
    registration is what gets the file to S3 and into the catalogue. Forgetting it
    would mean a season of captures that never leave the laptop."""
    from Scripts.store import ARTIFACTS
    assert ARTIFACTS["pool"] == "pool.parquet"
