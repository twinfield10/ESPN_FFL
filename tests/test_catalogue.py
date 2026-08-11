"""The data catalogue, which has to survive the states nobody tests by hand.

A catalogue is read on two occasions: when everything is fine, and on a fresh machine
where almost nothing is there yet. The second is the one that matters and the one
that never gets exercised in normal use, so it is what these pin — an empty tree, an
unreadable file, an unreachable bucket. A catalogue that raises on any of those tells
you strictly less than one that says "none".
"""

import pytest

from Scripts import catalogue, paths


@pytest.fixture
def empty_data(monkeypatch, tmp_path):
    """A Data/ tree with nothing in it -- a fresh checkout before any pull."""
    monkeypatch.setattr(paths, "DATA_DIR", tmp_path)
    monkeypatch.setattr(paths, "STORE_DIR", tmp_path / "Store")
    return tmp_path


# --- the empty case, which is the fresh-machine case ---------------------

def test_an_empty_tree_reports_rather_than_raises(empty_data, capsys):
    assert catalogue.main([]) == 0
    out = capsys.readouterr().out
    assert "(none" in out


def test_an_empty_store_names_the_command_that_builds_one(empty_data, capsys):
    """The repo's convention: an error state that says what to run."""
    catalogue.main([])
    assert "Scripts.refresh --all" in capsys.readouterr().out


def test_an_unreachable_bucket_is_reported_not_raised(empty_data, monkeypatch,
                                                      capsys):
    """No credentials on a new machine is the normal first run, not a crash."""
    from Scripts import s3_store

    def explode(*a, **kw):
        raise RuntimeError("Unable to locate credentials")
    monkeypatch.setattr(s3_store, "list_objects", explode)

    assert catalogue.main(["--s3"]) == 0
    assert "unreachable" in capsys.readouterr().out


def test_an_empty_bucket_names_the_command_that_fills_it(empty_data, monkeypatch,
                                                         capsys):
    from Scripts import s3_store
    monkeypatch.setattr(s3_store, "list_objects", lambda prefix: {})
    catalogue.main(["--s3"])
    assert "Scripts.sync --push" in capsys.readouterr().out


# --- reading files -------------------------------------------------------

def test_an_unreadable_file_does_not_abort_the_walk(empty_data):
    """One corrupt parquet must not cost you the whole inventory."""
    bad = empty_data / "NFL" / "2026" / "broken.parquet"
    bad.parent.mkdir(parents=True)
    bad.write_bytes(b"this is not parquet")
    assert catalogue.shape(bad) == (None, None)

    lines = catalogue.local_report()
    assert any("broken.parquet" in line for line in lines)


def test_shape_reads_parquet_without_loading_columns(empty_data):
    import polars as pl
    path = empty_data / "f.parquet"
    pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_parquet(path)
    assert catalogue.shape(path) == (3, 2)


def test_a_non_tabular_file_is_not_guessed_at(empty_data):
    path = empty_data / "meta.json"
    path.write_text('{"a": 1}')
    assert catalogue.shape(path) == (None, None)


# --- the weekly collapse -------------------------------------------------

def test_weekly_scrape_files_collapse_to_one_line(empty_data):
    """17 weeks x 2 sources x N seasons is noise. The useful facts are that the set
    exists, how many weeks landed, and how wide a row is."""
    import polars as pl
    directory = empty_data / "Projections" / "BetOnline" / "Season" / "2025"
    directory.mkdir(parents=True)
    for week in range(1, 18):
        pl.DataFrame({"player": ["x"], "line": [1.0]}).write_parquet(
            directory / f"BetOnline_AllProps_Week_{week}.parquet")

    lines = catalogue.local_report()
    collapsed = [line for line in lines if "Week_N" in line]
    assert len(collapsed) == 1
    assert "n=17" in collapsed[0]
    assert not any("Week_5.parquet" in line for line in lines)


def test_a_season_span_is_reported_as_a_range(empty_data):
    import polars as pl
    for season in (2016, 2020, 2025):
        directory = empty_data / "NFL" / str(season)
        directory.mkdir(parents=True)
        pl.DataFrame({"gsis_id": ["00-1"]}).write_parquet(directory / "routes.parquet")

    lines = catalogue.local_report()
    row = next(line for line in lines if "routes.parquet" in line)
    assert "2016-2025" in row and "n= 3" in row


# --- the store report ----------------------------------------------------

def test_the_store_report_names_artifacts_that_were_never_built(empty_data,
                                                                monkeypatch):
    """team_stats is opt-in and built for nobody. Saying so is more useful than
    omitting it, which reads as 'there is no such thing'."""
    import json
    import polars as pl
    from Scripts.store import ARTIFACTS, META_FILENAME

    directory = paths.store_dir(2026, "knights_ffl", create=True)
    pl.DataFrame({"player": ["a", "b"]}).write_parquet(directory / ARTIFACTS["board"])
    (directory / META_FILENAME).write_text(json.dumps({"season": 2026}))

    lines = catalogue._store_report(detail=False)
    assert any("board" in line and "1/1 leagues" in line for line in lines)
    assert any("not built" in line and "team_stats" in line for line in lines)
