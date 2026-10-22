"""The intraday re-blend (docs/plans/56-intraday-reblend.md): splice, change, refresh."""

import json

import pandas as pd
import pytest

from Scripts import paths, reblend, store
from Scripts.live import LOCKED_COLUMN


def frame(week, players, points, *, locked=None, owner="A", status="active"):
    n = len(players)
    return pd.DataFrame({
        "week": [week] * n, "player_id": list(range(1, n + 1)),
        "player_name": players, "TRUE_Points": points,
        "team_owner": [owner] * n, "slotPosition": ["WR"] * n,
        "player_active_status": [status] * n,
        LOCKED_COLUMN: locked if locked is not None else [False] * n,
    })


# --- splice --------------------------------------------------------------

def test_splice_replaces_the_week_and_keeps_every_other_row_as_stored():
    stored = pd.concat([frame(6, ["A", "B"], [9.0, 8.0]), frame(7, ["A", "B"], [10.0, 7.0])])
    fresh = frame(7, ["A", "B"], [10.5, 7.2])
    out = reblend.splice(stored, fresh, 7)
    assert out[out["week"] == 6].reset_index(drop=True).equals(
        stored[stored["week"] == 6].reset_index(drop=True))
    assert out.loc[out["week"] == 7, "TRUE_Points"].tolist() == [10.5, 7.2]
    assert out["week"].tolist() == [6, 6, 7, 7]


def test_splice_refuses_a_frame_carrying_another_week():
    stored = frame(7, ["A"], [10.0])
    fresh = pd.concat([frame(6, ["A"], [9.0]), frame(7, ["A"], [10.0])])
    with pytest.raises(ValueError, match="duplicate"):
        reblend.splice(stored, fresh, 7)


# --- material_change -----------------------------------------------------

def test_decimals_alone_are_not_worth_a_push():
    material, counts = reblend.material_change(frame(7, ["A", "B"], [10.0, 7.0]),
                                               frame(7, ["A", "B"], [10.04, 6.97]))
    assert not material and counts["moved"] == 0


def test_a_tenth_of_a_point_on_a_pre_game_player_is():
    material, counts = reblend.material_change(frame(7, ["A"], [10.0]),
                                               frame(7, ["A"], [10.1]))
    assert material and counts["moved"] == 1


def test_a_started_game_never_counts_as_a_move():
    """The freeze holds it; any move left is the live job's, not a re-blend's."""
    material, _ = reblend.material_change(frame(7, ["A"], [10.0], locked=[True]),
                                          frame(7, ["A"], [14.0], locked=[True]))
    assert not material


def test_a_roster_or_status_change_is_published_whatever_the_points_did():
    material, counts = reblend.material_change(frame(7, ["A"], [10.0]),
                                               frame(7, ["A"], [10.0], status="out"))
    assert material and counts["roster"] == 1


def test_a_player_added_or_dropped_is_published():
    material, counts = reblend.material_change(frame(7, ["A"], [10.0]),
                                               frame(7, ["A", "B"], [10.0, 3.0]))
    assert material and counts["added"] == 1


# --- refresh --what reblend ----------------------------------------------

@pytest.fixture
def reblend_ingest(monkeypatch, tmp_path):
    """A stored week-6/7 frame, and a stubbed one-week build and freeze."""
    monkeypatch.setattr(paths, "STORE_DIR", tmp_path / "Store")
    import Scripts.equivalence as eq
    import Scripts.kickoff_freeze as kf
    import Scripts.projection_utils as pu
    from Scripts import refresh

    class FakeLeague:
        name = "Fake League"
        current_week = 7
        roster_settings = {"roster_slots": {"QB": 1, "BE": 5}}

        class settings:
            week_to_matchup_period = {7: 7}

    stored = pd.concat([frame(6, ["A", "B"], [9.0, 8.0]), frame(7, ["A", "B"], [10.0, 7.0])],
                       ignore_index=True)
    store.write_league_store(2026, "knights_ffl", lineups=stored,
                             meta_extra={"built_at_marker": True})
    meta = store.read_meta(2026, "knights_ffl")
    meta["built_at"] = "2026-10-22T10:00:00+00:00"
    store.meta_path(2026, "knights_ffl").write_text(json.dumps(meta))

    # CI runs on config.example.yaml, which has no `knights_ffl`; nothing here needs
    # the real league config, so resolve it without one.
    monkeypatch.setattr(refresh, "resolve_league", lambda name: {
        "key": "knights_ffl", "display_name": "Knights_FFL", "ID": 1,
        "SWID": "s", "ESPN_S2": "e", "primary_own": "Someone"})

    state = {"fresh": frame(7, ["A", "B"], [10.0, 7.0])}
    monkeypatch.setattr(eq, "build_week_frame",
                        lambda name, season, return_league=False:
                        (state["fresh"].copy(), FakeLeague()))
    monkeypatch.setattr(kf, "apply", lambda fresh, stored: (fresh, {}))
    monkeypatch.setattr(kf, "summary", lambda counts: "stubbed")
    monkeypatch.setattr(pu, "weekly_sources_present", lambda season, week=None: {})
    return refresh, state


def test_an_unchanged_week_is_not_written(reblend_ingest):
    refresh, state = reblend_ingest
    before = store.artifact_path(2026, "knights_ffl", "lineups").read_bytes()
    refresh.refresh_league("knights_ffl", 2026, what=("reblend",))
    assert store.artifact_path(2026, "knights_ffl", "lineups").read_bytes() == before
    assert "reblended_at" not in store.read_meta(2026, "knights_ffl")


def test_a_moved_week_is_spliced_in_and_built_at_kept(reblend_ingest):
    refresh, state = reblend_ingest
    state["fresh"] = frame(7, ["A", "B"], [11.0, 7.0])
    refresh.refresh_league("knights_ffl", 2026, what=("reblend",))
    out = store.read_league_store(2026, "knights_ffl", "lineups")
    assert out.loc[out["week"] == 7, "TRUE_Points"].tolist() == [11.0, 7.0]
    assert out.loc[out["week"] == 6, "TRUE_Points"].tolist() == [9.0, 8.0]
    meta = store.read_meta(2026, "knights_ffl")
    assert meta["built_at"] == "2026-10-22T10:00:00+00:00"
    assert meta["reblended_at"]


def test_reblend_pushes_only_lineups():
    from Scripts import refresh
    assert refresh.PUSH_ARTIFACTS["reblend"] == ("lineups",)


# --- the cron wrapper ----------------------------------------------------

def _shell_var(path, name):
    import re
    text = (paths.REPO_ROOT / path).read_text()
    found = re.findall(rf'^{name}="?([^"\n]+)"?$', text, re.M)
    assert len(found) == 1, f"{path} should set {name} exactly once"
    return found[0]


def test_the_reblend_and_live_wrappers_share_one_lock():
    """Both rewrite lineups.parquet; atomic writes are not ordered writes."""
    assert (_shell_var("ops/espn_ffl_reblend.sh", "LOCK")
            == _shell_var("ops/espn_ffl_live.sh", "LOCK"))
    assert (_shell_var("ops/espn_ffl_reblend.sh", "LOCK_STALE_MINUTES")
            == _shell_var("ops/espn_ffl_live.sh", "LOCK_STALE_MINUTES"))


def test_the_reblend_wrapper_runs_from_the_nightly_checkout_and_gates_first():
    text = (paths.REPO_ROOT / "ops/espn_ffl_reblend.sh").read_text()
    assert (_shell_var("ops/espn_ffl_reblend.sh", "NIGHTLY")
            == _shell_var("ops/espn_ffl_nightly.sh", "NIGHTLY"))
    gate = text.index("--if-pending")
    assert gate < text.index("Scripts.books.engine --intraday")
    assert gate < text.index("--what reblend --push")
