"""The refresh CLI's contract, without touching ESPN.

Ingest itself is exercised live (``docs/plans/07-frontend-foundation.md``
verification); what is worth pinning here is the behaviour around it: which
artifacts are built by default, that a bad ``--what`` is rejected before any
network call, and that one league failing does not take the others down.
"""

import pandas as pd
import pytest

from Scripts import paths, refresh, store


@pytest.fixture(autouse=True)
def redirect_store(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "STORE_DIR", tmp_path / "Store")


#: A real configured league whose ingest the fixture makes fail. It has to be a
#: configured one, so the failure lands in ingest rather than in league resolution
#: -- which is the case worth isolating (expired cookies, ESPN 500s).
EXPLODING = "GOP_Degenerates"


@pytest.fixture
def fake_ingest(monkeypatch):
    """Replace ``build_league_frame`` so no test needs ESPN.

    Returns:
        list: Records one ``(name, season)`` per call, so a test can assert which
        leagues were attempted.
    """
    calls = []

    class FakeLeague:
        name = "Fake League"
        current_week = 3
        roster_settings = {"roster_slots": {"QB": 1, "BE": 5}}

        class settings:
            week_to_matchup_period = {3: 3}

    def build_league_frame(name, season, *, return_league=False):
        calls.append((name, season))
        if name == EXPLODING:
            raise RuntimeError("ESPNAccessDenied stand-in")
        df = pd.DataFrame({
            "week": [3], "player_name": ["A"],
            "ESPN_rushingYards": [10.0], "TRUE_Points": [1.0],
        })
        return (df, FakeLeague()) if return_league else df

    import Scripts.equivalence as equivalence
    monkeypatch.setattr(equivalence, "build_league_frame", build_league_frame)
    return calls


# --- team_stats is opt-in ------------------------------------------------

def test_default_what_excludes_team_stats():
    """It re-derives a league's whole history -- 2016-2026 for Winfield_Football --
    and nothing about this week changes 2019."""
    assert "team_stats" not in refresh.DEFAULT_WHAT
    assert "lineups" in refresh.DEFAULT_WHAT
    assert "team_stats" in refresh.WHAT_CHOICES


# --- argument validation -------------------------------------------------

def test_unknown_what_is_rejected_before_any_work():
    with pytest.raises(SystemExit):
        refresh.main(["--league", "Knights_FFL", "--what", "lineups,nonsense"])


def test_a_target_is_required():
    with pytest.raises(SystemExit):
        refresh.main([])


def test_league_and_all_are_mutually_exclusive():
    with pytest.raises(SystemExit):
        refresh.main(["--all", "--league", "Knights_FFL"])


def test_refresh_league_rejects_an_unknown_artifact():
    with pytest.raises(ValueError, match="Unknown --what"):
        refresh.refresh_league("Knights_FFL", 2026, what=["nonsense"])


def test_refresh_league_rejects_an_unknown_league():
    with pytest.raises(ValueError, match="Unknown league"):
        refresh.refresh_league("Not A League", 2026)


# --- writing -------------------------------------------------------------

def test_refresh_writes_a_complete_store(fake_ingest):
    refresh.refresh_league("Knights_FFL", 2026)

    assert store.has_store(2026, "knights_ffl")
    meta = store.read_meta(2026, "knights_ffl")
    assert meta["display_name"] == "Knights_FFL"
    assert meta["current_week"] == 3
    assert meta["league_name"] == "Fake League"
    assert meta["roster_slots"] == {"QB": 1, "BE": 5}
    # Recorded so the app can show a degraded source rather than an ESPN-only
    # number wearing a four-source badge.
    assert set(meta["weekly_sources_present"]) == {
        "fantasypros", "pinnacle", "betonline"}


def test_refresh_resolves_a_config_key_as_well_as_a_display_name(fake_ingest):
    refresh.refresh_league("knights_ffl", 2026)
    assert store.has_store(2026, "knights_ffl")


# --- failure isolation ---------------------------------------------------

def test_one_league_failing_does_not_stop_the_others(fake_ingest):
    """Expired cookies on a leaguemate's league must not cost you your own store."""
    order = ["Knights_FFL", EXPLODING, "Weenieless_Wanderers"]
    results, _ = refresh.refresh(leagues=order, season=2026)

    assert results["Knights_FFL"] == "ok"
    assert results["Weenieless_Wanderers"] == "ok"
    assert "RuntimeError" in results[EXPLODING]

    # All three were attempted -- the failure did not abort the loop -- and the
    # two healthy leagues have stores.
    assert [n for n, _ in fake_ingest] == order
    assert store.list_leagues(2026) == ["knights_ffl", "weenieless_wanderers"]


def test_a_failed_league_leaves_its_previous_store_alone(fake_ingest):
    """Showing an older build time is honest; showing nothing is not."""
    lineups = pd.DataFrame({"week": [1], "player_name": ["A"]})
    store.write_league_store(2026, "gop_degenerates", lineups=lineups,
                             meta_extra={"marker": "original"})

    results, _ = refresh.refresh(leagues=[EXPLODING], season=2026)

    assert results[EXPLODING] != "ok"
    assert store.read_meta(2026, "gop_degenerates")["marker"] == "original"


def test_exit_code_is_nonzero_when_a_league_fails(fake_ingest):
    assert refresh.main(["--league", EXPLODING]) == 1
    assert refresh.main(["--league", "Knights_FFL"]) == 0
