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
        "fantasypros", "pinnacle", "betonline", "theathletic"}


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


# --- the live stage ------------------------------------------------------
#
# `--what live` rewrites `lineups` rather than building a new artifact, which makes
# its failure modes different from every other stage's: the ways it can go wrong all
# end with a *plausible* frame in the store rather than an exception. So what is
# pinned here is mostly the refusals.


def test_live_is_a_what_choice_but_not_a_default():
    """It runs on a ten-minute cron, not at 06:00 -- the nightly builds `lineups`
    from fresher inputs and resolves the same columns as a by-product."""
    assert "live" in refresh.WHAT_CHOICES
    assert "live" not in refresh.DEFAULT_WHAT


def test_live_is_skipped_when_lineups_is_also_requested(monkeypatch, fake_ingest):
    """Patching the full build's own output would be a second ESPN round-trip to
    arrive at the numbers it just computed."""
    called = []

    def _never(*args, **kwargs):
        called.append(args)
        return None, {}

    monkeypatch.setattr("Scripts.live.refresh_live", _never)
    refresh.refresh_league("Jeffs_League", 2026, what=["lineups", "live"])
    assert called == []


def test_the_live_stage_writes_what_the_patch_returned(monkeypatch, fake_ingest):
    frame = pd.DataFrame({"week": [3], "player_name": ["A"], "points": [12.0],
                          "TRUE_Points": [9.0], "LIVE_Points": [12.0]})
    counts = {"week": 3, "patched": 1, "added": 0, "uncovered": 0,
              "states": {"post": 1}, "live": False}
    monkeypatch.setattr("Scripts.live.refresh_live",
                        lambda *a, **k: (frame, counts))
    timings = refresh.refresh_league("Jeffs_League", 2026, what=["live"])
    assert "live" in timings

    from Scripts import store
    written = store.read_league_store(2026, "jeffs_league", "lineups")
    assert written["LIVE_Points"].tolist() == [12.0]


def test_the_live_stage_refuses_a_week_the_store_does_not_hold(monkeypatch,
                                                               fake_ingest):
    """The refusal that matters most. Appending a week the frame does not have would
    add a second, projection-free copy of it and halve every team total -- and the
    result would look like a perfectly ordinary store."""
    from Scripts import live

    def _explode(*args, **kwargs):
        raise live.LiveRefreshError("lineups.parquet holds weeks [1], not 3")

    monkeypatch.setattr("Scripts.live.refresh_live", _explode)
    with pytest.raises(live.LiveRefreshError, match="holds weeks"):
        refresh.refresh_league("Jeffs_League", 2026, what=["live"])


# --- team_stats rebuilds the season in progress, not the archive ---------
#
# Added 2026-09-16. `team_stats` re-derived every season a league had ever played on
# every call -- 218.91s across eight leagues, 58.87s of it Winfield's 2016-2026 -- so
# it was kept off every schedule, and then only advanced when somebody ran it by
# hand. `scrape_team_stats` bounds its loops by `currentMatchupPeriod`, so week N+1's
# fixture cannot be fetched before ESPN's counter turns over; on 2026-09-15 the
# counter moved ~03:30 and week 2 was still missing from all ten stores twelve hours
# later, with the Matchup tab saying "Week 2 is not in `team_stats` yet."


def _team_stats_frame(years):
    """A stored-shaped frame spanning ``years``."""
    return pd.DataFrame({
        "year": [float(y) for y in years],
        "week": [1.0] * len(years),
        "team_owner": ["Tommy Winfield"] * len(years),
        "team_score": [100.0] * len(years),
        "opp_score": [90.0] * len(years),
    })


def _recording_scrape(monkeypatch, frame):
    """Stub ``scrape_team_stats``, recording how each call was scoped."""
    calls = []

    def fake_scrape(*, league_id, start_year, end_year, swid, espn_s2,
                    df_prev=None):
        calls.append({"start_year": start_year, "end_year": end_year,
                      "df_prev": df_prev})
        return frame

    import Scripts.scrape_team_stats as sts
    monkeypatch.setattr(sts, "scrape_team_stats", fake_scrape)
    return calls


def test_team_stats_rebuilds_only_the_season_in_progress(monkeypatch, fake_ingest):
    """The whole point: prior seasons are carried, not re-fetched."""
    seeded = _recording_scrape(monkeypatch, _team_stats_frame([2024, 2025, 2026]))
    refresh.refresh_league("Winfield_Football", 2026, what=["team_stats"])
    assert (seeded[0]["start_year"], seeded[0]["end_year"]) == (2016, 2026), (
        "first build of a league with no store is the full sweep")

    calls = _recording_scrape(monkeypatch, _team_stats_frame([2024, 2025, 2026]))
    refresh.refresh_league("Winfield_Football", 2026, what=["team_stats"])

    assert len(calls) == 1
    assert (calls[0]["start_year"], calls[0]["end_year"]) == (2026, 2026)

    # And the stored prior seasons go in, which is the correctness half rather than
    # the speed half: `scrape_team_stats` reads its `end_year - 1` baseline off the
    # frame it is handed, so without them `team_score_adj` changes meaning.
    carried = calls[0]["df_prev"]
    assert carried is not None, "prior seasons must be passed, not dropped"
    assert sorted(int(y) for y in carried["year"].unique()) == [2024, 2025]
    assert 2026 not in [int(y) for y in carried["year"].unique()], (
        "the season being rebuilt must not also be carried in")


def test_a_store_with_no_prior_seasons_falls_back_to_the_full_sweep(
        monkeypatch, fake_ingest):
    """A first-season league has nothing to be incremental about.

    This is how ``jeffs_league`` (2026-only) bootstraps, and how any new league
    joins. Carrying an empty frame would hand ``scrape_team_stats`` no baseline at
    all, which is the failure the 2026-08-06 skip was wrongly added against.
    """
    _recording_scrape(monkeypatch, _team_stats_frame([2026]))
    refresh.refresh_league("Jeffs_League", 2026, what=["team_stats"])

    calls = _recording_scrape(monkeypatch, _team_stats_frame([2026]))
    refresh.refresh_league("Jeffs_League", 2026, what=["team_stats"])

    assert (calls[0]["start_year"], calls[0]["end_year"]) == (2026, 2026)
    assert calls[0]["df_prev"] is None, (
        "a store holding only the season being rebuilt has no history to carry")


def test_rebuild_history_forces_the_full_sweep(monkeypatch, fake_ingest):
    """The escape hatch, for when a past season's scrape itself changes."""
    _recording_scrape(monkeypatch, _team_stats_frame([2024, 2025, 2026]))
    refresh.refresh_league("Winfield_Football", 2026, what=["team_stats"])

    calls = _recording_scrape(monkeypatch, _team_stats_frame([2024, 2025, 2026]))
    refresh.refresh_league("Winfield_Football", 2026, what=["team_stats"],
                           rebuild_history=True)

    assert (calls[0]["start_year"], calls[0]["end_year"]) == (2016, 2026)
    assert calls[0]["df_prev"] is None


def test_the_rebuild_history_flag_reaches_refresh_league(monkeypatch):
    """CLI wiring, which is the half that silently does nothing when it is missed."""
    seen = {}

    def fake_refresh(*, leagues, season, what, rebuild_history):
        seen.update(leagues=leagues, what=what, rebuild_history=rebuild_history)
        return {"X": "ok"}, {}

    monkeypatch.setattr(refresh, "refresh", fake_refresh)

    refresh.main(["--all", "--what", "team_stats"])
    assert seen["rebuild_history"] is False, "incremental is the default"

    refresh.main(["--all", "--what", "team_stats", "--rebuild-history"])
    assert seen["rebuild_history"] is True
