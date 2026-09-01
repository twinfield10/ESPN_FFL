"""The local data store: the boundary the app reads and refresh writes.

No network. Every test redirects ``paths.STORE_DIR`` at a tmp_path, which is why
:func:`Scripts.paths.store_root` resolves it through a function rather than
binding it at import time.
"""

import datetime
import json

import numpy as np
import pandas as pd
import pytest

from Scripts import paths, store


@pytest.fixture(autouse=True)
def redirect_store(tmp_path, monkeypatch):
    """Point the store at a tmp dir for the whole module."""
    monkeypatch.setattr(paths, "STORE_DIR", tmp_path / "Store")
    return tmp_path / "Store"


@pytest.fixture
def lineups():
    """A minimal stand-in for a ``clean_lineups`` frame.

    Carries the pieces the store reasons about: a ``week`` column, an
    ``eligiblePositions`` list column, and one ``*_is_imputed`` provenance flag.
    """
    return pd.DataFrame({
        "week": [1, 1, 2],
        "player_name": ["A", "B", "C"],
        "eligiblePositions": [["RB", "BE"], ["WR", "BE"], ["QB"]],
        "ESPN_rushingYards": [10.0, 20.0, 30.0],
        "PINNY_rushingYards": [10.0, 20.0, 30.0],
        "PINNY_rushingYards_is_imputed": [True, True, True],
        "TRUE_Points": [1.0, 2.0, 3.0],
    })


# --- paths ---------------------------------------------------------------

def test_store_dir_separates_seasons_and_leagues():
    a = paths.store_dir(2025, "knights_ffl")
    b = paths.store_dir(2026, "knights_ffl")
    c = paths.store_dir(2026, "gop_degenerates")
    assert len({a, b, c}) == 3
    assert "2025" in a.parts and "2026" in b.parts


def test_store_dir_does_not_create_unless_asked():
    """A read must not leave an empty directory behind: list_leagues() would then
    report a league as built when it has no store."""
    path = paths.store_dir(2026, "knights_ffl")
    assert not path.exists()
    created = paths.store_dir(2026, "knights_ffl", create=True)
    assert created.is_dir()


def test_artifact_path_rejects_unknown_names():
    with pytest.raises(KeyError, match="lineups"):
        store.artifact_path(2026, "knights_ffl", "nonsense")


# --- write / read round trip -------------------------------------------

def test_round_trip_is_lossless(lineups):
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    back = store.read_league_store(2026, "knights_ffl", "lineups")
    assert back.equals(lineups.reset_index(drop=True))


def test_list_columns_come_back_as_lists(lineups):
    """pyarrow reads a parquet list column back as an ndarray. Left alone, the
    store would look like it had altered the frame, and callers expecting a list
    would break."""
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    back = store.read_league_store(2026, "knights_ffl", "lineups")
    assert isinstance(back["eligiblePositions"].iloc[0], list)
    assert not isinstance(back["eligiblePositions"].iloc[0], np.ndarray)


def test_no_index_column_leaks_into_the_schema(lineups):
    """pandas serialises a non-default index as ``__index_level_0__``, which
    Polars then reads as a real column -- the app reported 470 columns for a
    469-column frame until the index was dropped on write."""
    concatenated = pd.concat([lineups, lineups])          # duplicated index
    store.write_league_store(2026, "knights_ffl", lineups=concatenated)
    path = store.artifact_path(2026, "knights_ffl", "lineups")
    assert not any(c.startswith("__index") for c in pd.read_parquet(path).columns)


def test_writing_nothing_is_an_error(lineups):
    with pytest.raises(ValueError, match="at least one artifact"):
        store.write_league_store(2026, "knights_ffl")


def test_reading_a_missing_artifact_names_the_command(lineups):
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    with pytest.raises(FileNotFoundError, match="--what team_stats"):
        store.read_league_store(2026, "knights_ffl", "team_stats")


def test_partial_refresh_keeps_the_other_artifact(lineups):
    """`--what lineups` must not make an earlier team_stats backfill invisible."""
    team_stats = pd.DataFrame({"year": [2025], "week": [1], "team_score": [100.0]})
    store.write_league_store(2026, "knights_ffl", lineups=lineups,
                             team_stats=team_stats)
    store.write_league_store(2026, "knights_ffl", lineups=lineups)

    meta = store.read_meta(2026, "knights_ffl")
    assert set(meta["artifacts"]) == {"lineups", "team_stats"}
    assert store.read_league_store(2026, "knights_ffl", "team_stats").shape == (1, 3)


class _League:
    """The pieces of a live ESPN ``League`` that ``build_meta`` reads."""

    name = "Knights"
    current_week = 3
    roster_settings = {
        "roster_slots": {"QB": 1, "RB": 2, "BE": 6},
        "starting_roster_slots": {"QB": 1, "RB": 2},
    }

    def __init__(self):
        self.teams = [object()] * 14
        self.settings = None


def test_a_refresh_without_a_live_league_keeps_the_league_settings(lineups):
    """**The cash lens depends on this and used to lose it silently.**

    ``build_meta`` fills ``team_count``, ``roster_slots``, ``starting_slots`` and
    ``draft_settings`` only when it is handed a live league, and the draft path has
    none to hand it. So ``--what draft`` overwrote meta.json with a copy missing all
    of them and nothing failed -- while ``draftable_spots`` went to 0, ``at_budget``
    fell back to a proportional rescale and ``with_cash_value`` stopped producing
    ``our_dollars`` at all. On an auction board that is the whole ``$`` column,
    gone, with no error.
    """
    draft = pd.DataFrame({"season": [2025], "overall_pick": [1], "bid": [50.0]})
    store.write_league_store(2026, "knights_ffl", lineups=lineups, league=_League())
    before = store.read_meta(2026, "knights_ffl")
    assert before.get("team_count") == 14                      # the league was seen

    store.write_league_store(2026, "knights_ffl", draft=draft)  # no league to pass
    after = store.read_meta(2026, "knights_ffl")

    for key in ("team_count", "roster_slots", "starting_slots", "league_name",
                "current_week"):
        assert after.get(key) == before.get(key), f"{key} was dropped by --what draft"
    # And the run's own work is still recorded.
    assert set(after["artifacts"]) == {"lineups", "draft"}


def test_a_fresh_key_still_overwrites_a_stale_one(lineups):
    """Carrying forward must not pin an old value. Only keys the run could not
    compute survive; anything it did compute wins."""
    store.write_league_store(2026, "knights_ffl", lineups=lineups, league=_League())
    first = store.read_meta(2026, "knights_ffl")["built_at"]
    store.write_league_store(2026, "knights_ffl", lineups=lineups,
                             meta_extra={"display_name": "Renamed"})
    second = store.read_meta(2026, "knights_ffl")
    assert second["display_name"] == "Renamed"
    assert second["built_at"] != first


# --- meta.json -----------------------------------------------------------

def test_meta_carries_the_required_keys(lineups):
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    meta = store.read_meta(2026, "knights_ffl")
    for key in ("schema_version", "season", "league_key", "built_at",
                "artifacts", "versions", "coverage", "weeks_present"):
        assert key in meta, f"meta.json is missing {key}"
    assert meta["artifacts"]["lineups"] == {"rows": 3, "cols": 7}
    assert meta["weeks_present"] == [1, 2]


def test_built_at_is_timezone_aware(lineups):
    """A naive timestamp makes is_stale() ambiguous rather than merely wrong."""
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    built = datetime.datetime.fromisoformat(
        store.read_meta(2026, "knights_ffl")["built_at"])
    assert built.tzinfo is not None


def test_meta_records_coverage_from_the_provenance_flags(lineups):
    """Plan 03's flags are what distinguish an absent source from an agreeing
    one; the store has to carry that through or the app cannot show it."""
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    coverage = store.read_meta(2026, "knights_ffl")["coverage"]["overall"]
    assert coverage["ESPN"] == pytest.approx(100.0)
    assert coverage["PINNY"] == pytest.approx(0.0)


def test_meta_extra_is_merged(lineups):
    store.write_league_store(2026, "knights_ffl", lineups=lineups,
                             meta_extra={"display_name": "Knights_FFL"})
    assert store.read_meta(2026, "knights_ffl")["display_name"] == "Knights_FFL"


def test_read_meta_without_a_store_names_the_command():
    with pytest.raises(FileNotFoundError, match="Scripts.refresh"):
        store.read_meta(2026, "knights_ffl")


def test_read_meta_missing_ok_returns_none():
    assert store.read_meta(2026, "knights_ffl", missing_ok=True) is None


# --- completeness --------------------------------------------------------

def test_a_store_without_meta_is_not_a_store(lineups, redirect_store):
    """meta.json is written last, so its absence means a build in progress or a
    build that died partway. Neither should render."""
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    store.meta_path(2026, "knights_ffl").unlink()

    assert store.artifact_path(2026, "knights_ffl", "lineups").is_file()
    assert not store.has_store(2026, "knights_ffl")
    assert store.list_leagues(2026) == []
    assert store.list_seasons() == []


def test_no_tmp_files_survive_a_write(lineups, redirect_store):
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    leftovers = list((redirect_store / "2026" / "knights_ffl").glob("*.tmp"))
    assert not leftovers


# --- cache key -----------------------------------------------------------

def test_store_mtime_is_zero_without_a_store():
    assert store.store_mtime(2026, "knights_ffl") == 0.0


def test_store_mtime_changes_on_rewrite(lineups, monkeypatch):
    """This is the app's cache key. If it does not move, a refresh does not show
    up until the TTL expires."""
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    first = store.store_mtime(2026, "knights_ffl")

    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    # Rewriting within the filesystem's mtime resolution can tie; bump explicitly
    # rather than sleeping, so the assertion is about the function not the clock.
    path = store.meta_path(2026, "knights_ffl")
    import os
    os.utime(path, (first + 10, first + 10))

    assert store.store_mtime(2026, "knights_ffl") > first


# --- listings ------------------------------------------------------------

def test_listings_ignore_non_store_directories(lineups, redirect_store):
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    store.write_league_store(2025, "gop_degenerates", lineups=lineups)
    (redirect_store / "not-a-season").mkdir(parents=True)
    (redirect_store / "2026" / "half_built").mkdir()

    assert store.list_seasons() == [2026, 2025]         # newest first
    assert store.list_leagues(2026) == ["knights_ffl"]
    assert store.list_leagues(2024) == []


# --- staleness -----------------------------------------------------------

def _meta_aged(minutes):
    built = datetime.datetime.now().astimezone() - datetime.timedelta(minutes=minutes)
    return {"built_at": built.isoformat()}


@pytest.mark.parametrize("minutes,stale", [(5, False), (59, False), (61, True)])
def test_is_stale_uses_the_threshold(minutes, stale):
    assert store.is_stale(_meta_aged(minutes), max_age_min=60) is stale


@pytest.mark.parametrize("meta", [None, {}, {"built_at": None},
                                  {"built_at": "not a timestamp"}])
def test_unreadable_build_time_counts_as_stale(meta):
    """Failing loud beats claiming freshness: an unparseable built_at is exactly
    the case where the numbers on screen cannot be vouched for."""
    assert store.is_stale(meta) is True
    assert store.store_age_minutes(meta) is None


def test_store_age_reads_a_naive_timestamp_as_local():
    """Stores written before built_at carried an offset must still be readable."""
    naive = (datetime.datetime.now() - datetime.timedelta(minutes=30)).isoformat()
    age = store.store_age_minutes({"built_at": naive})
    assert age == pytest.approx(30, abs=1)


def test_meta_json_is_valid_json_on_disk(lineups, redirect_store):
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    raw = (redirect_store / "2026" / "knights_ffl" / "meta.json").read_text()
    assert json.loads(raw)["league_key"] == "knights_ffl"


# --- ESPN calibration ------------------------------------------------------


def _calibration_board(rows):
    """A board carrying just the four columns the calibration reads."""
    return pd.DataFrame(rows, columns=["primaryPosition", "adp",
                                       "ESPN_Points", "TRUE_Points"])


def test_the_calibration_splits_by_adp_band():
    """The reason it is not one number per position.

    On the 2026 boards `TRUE_/ESPN` runs 0.96 in the first hundred picks and 1.24 in
    the second. Pooled, those average to something near agreement and report the
    opposite of what is happening.
    """
    board = _calibration_board(
        [("WR", 10.0, 200.0, 192.0), ("WR", 40.0, 180.0, 172.0)]
        + [("WR", 160.0 + i, 20.0, 30.0) for i in range(5)])

    summary = store.calibration_summary(board)["WR"]
    assert summary["0-50"]["median_ratio"] < 1.0
    assert summary["150-200"]["median_ratio"] > 1.0
    assert summary["0-50"]["n"] == 2 and summary["150-200"]["n"] == 5


def test_the_calibration_ignores_players_espn_has_not_projected():
    """A zero ESPN line would divide the headline ratio by nothing, and it is not
    disagreement anyway -- it is absence, which `coverage` is what counts."""
    board = _calibration_board([("RB", 20.0, 0.0, 50.0),
                                ("RB", 30.0, 100.0, 90.0)])
    bands = store.calibration_summary(board)["RB"]
    assert bands["0-50"]["n"] == 1
    assert bands["0-50"]["median_ratio"] == pytest.approx(0.9)


def test_the_calibration_is_absent_rather_than_wrong_without_a_comparison():
    """A board with no ESPN column gets no calibration key, not a table of nulls."""
    board = pd.DataFrame({"primaryPosition": ["WR"], "adp": [10.0],
                          "TRUE_Points": [100.0]})
    assert store.calibration_summary(board) == {}


def test_meta_carries_the_calibration_when_a_board_is_written(redirect_store):
    board = _calibration_board([("WR", float(i), 100.0, 90.0)
                                for i in range(1, 12)])
    store.write_league_store(2026, "knights_ffl", board=board)
    meta = json.loads(
        (redirect_store / "2026" / "knights_ffl" / "meta.json").read_text())
    assert meta["espn_calibration"]["WR"]["0-50"]["share_above"] == 0.0
