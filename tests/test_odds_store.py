"""The odds store: append only what moved, and refuse an empty pull.

The design is one idea -- a pull is compared against the newest stored state of each
line, and unchanged rows are not written -- which yields line history without a second
mechanism. The hazard it creates is the reason for the last test here: in an
append-only store, "nothing changed" and "nothing came back" are the same empty write,
so the difference has to be caught at the door or it is lost.
"""

import polars as pl
import pytest

from Scripts.books import store
from Scripts.books.schema import ODDS_SCHEMA


def _rows(price=-110, line=44.0, ts="2026-08-27T10:00:00", n=1):
    base = {
        "sportsbook": "Test", "bookType": "book", "season": 2026, "week": 1,
        "officialDate": "2026-09-13", "startTimeET": "2026-09-13T13:00:00-04:00",
        "rotNum": 101, "matchup": "A vs. B", "Home": "B", "Away": "A",
        "marketTitle": "Total", "gamePeriod": "GAME", "sideOf": None,
        "betSide": "over", "marketLine": line, "value": line, "price": float(price),
        "impProb": 0.52, "fairProb": 0.5, "isAlt": False, "propType": None,
        "snapshot_ts": ts,
    }
    return pl.DataFrame([base] * n).cast(dict(ODDS_SCHEMA))


def test_the_first_write_stores_everything(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "ODDS_DIR", tmp_path)
    stats = store.write_snapshot(_rows(), 2026, "Test")
    assert stats["appended"] == 1
    assert (tmp_path / "2026" / "Test" / "2026-09-13.parquet").is_file()


def test_an_unchanged_line_is_not_written_again(tmp_path, monkeypatch):
    """Books reprice far less often than they are polled, so this is the common case.
    Writing anyway would turn a 6-hourly pull into four copies a day of the same row."""
    monkeypatch.setattr(store, "ODDS_DIR", tmp_path)
    store.write_snapshot(_rows(ts="2026-08-27T10:00:00"), 2026, "Test")
    stats = store.write_snapshot(_rows(ts="2026-08-27T16:00:00"), 2026, "Test")
    assert stats["appended"] == 0
    assert stats["unchanged"] == 1


def test_a_repriced_line_is_appended(tmp_path, monkeypatch):
    """A book can hold the number and move the price. That is movement."""
    monkeypatch.setattr(store, "ODDS_DIR", tmp_path)
    store.write_snapshot(_rows(price=-110), 2026, "Test")
    stats = store.write_snapshot(_rows(price=-125, ts="2026-08-27T16:00:00"),
                                 2026, "Test")
    assert stats["appended"] == 1

    history = store.line_history(2026, "Test")
    assert history.height == 2
    assert history["price"].to_list() == [-110.0, -125.0]


def test_a_moved_number_is_appended(tmp_path, monkeypatch):
    """And it can move the number at the same price."""
    monkeypatch.setattr(store, "ODDS_DIR", tmp_path)
    store.write_snapshot(_rows(line=44.0), 2026, "Test")
    stats = store.write_snapshot(_rows(line=44.5, ts="2026-08-27T16:00:00"),
                                 2026, "Test")
    assert stats["appended"] == 1


def test_current_holds_the_newest_state_of_each_line(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "ODDS_DIR", tmp_path)
    store.write_snapshot(_rows(price=-110), 2026, "Test")
    store.write_snapshot(_rows(price=-125, ts="2026-08-27T16:00:00"), 2026, "Test")

    current = store.read_current(2026, "Test")
    assert current.height == 1
    assert current["price"][0] == -125.0


def test_reading_a_book_with_nothing_stored_does_not_create_it(tmp_path, monkeypatch):
    """Asking whether a book has data must not leave a directory saying it does --
    the mistake that once produced three `2999/` directories in the projections tree."""
    monkeypatch.setattr(store, "ODDS_DIR", tmp_path)
    assert store.read_current(2026, "Nobody").is_empty()
    assert not (tmp_path / "2026" / "Nobody").exists()


def test_an_empty_pull_is_refused_rather_than_stored(tmp_path, monkeypatch):
    """The whole hazard of an append-only store. A book that renamed a market writes
    the same nothing as a book whose lines simply did not move, and the store looks
    healthy either way."""
    monkeypatch.setattr(store, "ODDS_DIR", tmp_path)
    with pytest.raises(store.EmptyPullError):
        store.write_snapshot(pl.DataFrame(schema=ODDS_SCHEMA), 2026, "Test")


def test_each_game_date_is_its_own_partition(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "ODDS_DIR", tmp_path)
    two_dates = pl.concat([
        _rows(), _rows().with_columns(pl.lit("2026-09-14").alias("officialDate"))])
    store.write_snapshot(two_dates, 2026, "Test")
    written = {p.name for p in (tmp_path / "2026" / "Test").glob("*.parquet")}
    assert written == {"2026-09-13.parquet", "2026-09-14.parquet", "current.parquet"}
