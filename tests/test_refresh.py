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


def test_a_first_season_league_still_builds_team_stats(monkeypatch, fake_ingest):
    """The regression this exists for, and it was silent for a month.

    ``refresh`` used to skip ``team_stats`` whenever ``season <= cfg["start"]``,
    added 2026-08-06 against a real failure: ``scrape_team_stats`` normalised each
    season's scores against the median of ``end_year - 1``, and a league with one
    season in the frame had nothing to divide by. That was fixed at the source on
    2026-09-07 -- ``_multiplier`` resolves an absent or non-positive baseline to 1.0 --
    but the skip was not removed with it.

    The cost landed on ``jeffs_league``, configured ``start: 2026, end: 2026``: no
    fixture list, so no Matchup tab, no Home standings and no win probability, in a
    league whose data ESPN was serving the whole time. Nothing failed loudly, because
    a skip is not an error.

    Asserted through a stubbed ``scrape_team_stats`` rather than live, so this pins
    *that refresh calls it at all* -- which is the thing that broke. Whether the call
    then succeeds is ``scrape_team_stats``' own contract and is covered live.
    """
    called = []

    def fake_scrape(*, league_id, start_year, end_year, swid, espn_s2):
        called.append((league_id, start_year, end_year))
        return pd.DataFrame({"year": [2026.0], "week": [1.0],
                             "team_owner": ["Tommy Winfield"],
                             "team_score": [0.0], "opp_score": [0.0]})

    import Scripts.scrape_team_stats as sts
    monkeypatch.setattr(sts, "scrape_team_stats", fake_scrape)

    refresh.refresh_league("Jeffs_League", 2026, what=["team_stats"])

    assert called, "team_stats was skipped for a league in its first season"
    _, start_year, end_year = called[0]
    assert (start_year, end_year) == (2026, 2026), (
        "a first-season league asks ESPN for exactly its one season")
    assert store.artifact_path(2026, "jeffs_league", "team_stats").is_file()


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
    """Expired cookies on a leaguemate's league must not cost you your own store.

    The healthy pair has to be leagues ``config.yaml`` really holds, because
    ``refresh`` resolves every name through it before ingesting -- so this broke, with
    ``ValueError: Unknown league``, the moment ``Weenieless_Wanderers`` was
    disconnected on 2026-09-09. Winfield_Football took its place.
    """
    order = ["Knights_FFL", EXPLODING, "Winfield_Football"]
    results, _ = refresh.refresh(leagues=order, season=2026)

    assert results["Knights_FFL"] == "ok"
    assert results["Winfield_Football"] == "ok"
    assert "RuntimeError" in results[EXPLODING]

    # All three were attempted -- the failure did not abort the loop -- and the
    # two healthy leagues have stores.
    assert [n for n, _ in fake_ingest] == order
    assert store.list_leagues(2026) == ["knights_ffl", "winfield_football"]


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
