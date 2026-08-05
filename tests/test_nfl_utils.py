"""Season and week derivation from the schedule file."""

import polars as pl
import pytest

from Scripts import nfl_utils


def _schedule(rows):
    return pl.DataFrame(rows, schema={"season": pl.Int64, "week": pl.Int64,
                                      "away_score": pl.Utf8, "gameday": pl.Utf8})


@pytest.fixture
def fake_schedule(monkeypatch):
    """Swap in a synthetic schedule without touching the cached real one."""
    def _install(rows):
        df = _schedule(rows)
        monkeypatch.setattr(nfl_utils, "load_schedule", lambda refresh=False: df)
        return df
    return _install


def test_current_week_is_first_unplayed(fake_schedule):
    fake_schedule([
        {"season": 2026, "week": 1, "away_score": "20", "gameday": "2026-09-10"},
        {"season": 2026, "week": 2, "away_score": "17", "gameday": "2026-09-17"},
        {"season": 2026, "week": 3, "away_score": "NA", "gameday": "2026-09-24"},
        {"season": 2026, "week": 4, "away_score": "NA", "gameday": "2026-10-01"},
    ])
    assert nfl_utils.current_week() == 3


def test_current_week_before_the_season_opens(fake_schedule):
    fake_schedule([
        {"season": 2026, "week": 1, "away_score": "NA", "gameday": "2026-09-10"},
        {"season": 2026, "week": 2, "away_score": "NA", "gameday": "2026-09-17"},
    ])
    assert nfl_utils.current_week() == 1


def test_current_week_after_the_season_completes(fake_schedule):
    """Previously returned null once every game had a score, which crashed
    `range(1, WEEK + 1)` in the FantasyPros scraper."""
    fake_schedule([
        {"season": 2025, "week": 16, "away_score": "20", "gameday": "2025-12-21"},
        {"season": 2025, "week": 17, "away_score": "24", "gameday": "2025-12-28"},
    ])
    assert nfl_utils.current_week() == 17


def test_current_season(fake_schedule):
    fake_schedule([
        {"season": 2026, "week": 1, "away_score": "NA", "gameday": "2026-09-10"},
    ])
    assert nfl_utils.current_season() == 2026


def test_multi_season_schedule_is_rejected(fake_schedule):
    """Two seasons in one file would make every season-scoped path ambiguous."""
    fake_schedule([
        {"season": 2025, "week": 1, "away_score": "20", "gameday": "2025-09-04"},
        {"season": 2026, "week": 1, "away_score": "NA", "gameday": "2026-09-10"},
    ])
    with pytest.raises(ValueError, match="multiple seasons"):
        nfl_utils.current_season()


def _typed(rows):
    """Schedule frame including game_type, for the validation tests."""
    return pl.DataFrame(rows, schema={"season": pl.Int64, "week": pl.Int64,
                                      "away_score": pl.Utf8, "gameday": pl.Utf8,
                                      "game_type": pl.Utf8})


def test_regular_season_only_schedule_validates():
    assert nfl_utils._validate(_typed([
        {"season": 2026, "week": 1, "away_score": "NA",
         "gameday": "2026-09-10", "game_type": "REG"},
    ])).height == 1


@pytest.mark.parametrize("bad", ["POST", "WC", "DIV", "CON", "SB", "PRE"])
def test_non_regular_season_games_are_rejected(bad):
    """Fantasy scores the regular season only, and current_week()/date_week()
    assume every row is a regular-season game -- a playoff or preseason row shifts
    week detection and mis-assigns Pinnacle props rather than failing."""
    with pytest.raises(ValueError, match="non-regular-season"):
        nfl_utils._validate(_typed([
            {"season": 2026, "week": 1, "away_score": "20",
             "gameday": "2026-09-10", "game_type": "REG"},
            {"season": 2026, "week": 19, "away_score": "NA",
             "gameday": "2027-01-10", "game_type": bad},
        ]))


def test_validation_is_skipped_when_game_type_is_absent():
    """Older schedule files predate the column; they should still load."""
    assert nfl_utils._validate(_schedule([
        {"season": 2026, "week": 1, "away_score": "NA", "gameday": "2026-09-10"},
    ])).height == 1


def test_the_real_schedule_on_disk_is_regular_season_only():
    """Guards the actual committed file, not a synthetic one."""
    sched = nfl_utils.load_schedule(refresh=True)
    assert sched["game_type"].unique().to_list() == ["REG"]
    assert sched["week"].min() == 1 and sched["week"].max() == 18
    assert sched.height == 272, "a full NFL regular season is 272 games"


def test_legacy_module_attributes_still_resolve():
    """NFL_SCHEDULE / DATE_WEEK are lazy now (PEP 562) but callers still use them."""
    assert nfl_utils.NFL_SCHEDULE.height > 0
    assert set(nfl_utils.DATE_WEEK.columns) == {"gameday", "week"}


def test_unknown_attribute_still_raises():
    with pytest.raises(AttributeError):
        nfl_utils.NOT_A_REAL_ATTRIBUTE
