"""The play-by-play archive that ``R/GetPBP.R`` writes.

What is pinned here is the one design decision that separates this from every other
pull in the repo: **the archive is unfiltered on disk and filtered on read.**

Every other writer -- ``R/GetUsage.R``, ``R/GetAdvanced.R`` -- narrows to regular
season weeks 1-18 before the parquet lands, because a fantasy pipeline never scores
weeks 19-22 and playoff games corrupt per-game denominators. That is right for a
feature table and wrong for an archive: a filter applied at write time cannot be
undone, and the whole reason this exists is that ``GetAdvanced.R`` already threw 370
columns of play-by-play away and every later question had to re-download a season.

So the file holds the post-season and :func:`Scripts.usage.nflverse.load_pbp`
defaults to hiding it. If that default ever slips, callers silently gain playoff
games in their denominators -- the same class of error as an absent source reading
as agreement, and just as invisible in aggregate.

Synthetic parquet in ``tmp_path``. No network.
"""

import polars as pl
import pytest

from Scripts import paths
from Scripts.usage import nflverse as nv


@pytest.fixture
def nfl_root(tmp_path, monkeypatch):
    """Redirect ``Data/NFL`` to ``tmp_path``."""
    monkeypatch.setattr(paths, "DATA_DIR", tmp_path / "Data")
    root = tmp_path / "Data" / "NFL"
    root.mkdir(parents=True, exist_ok=True)
    return root


def write_pbp(nfl_root, season, rows, name="pbp"):
    """Write one season of the archive."""
    directory = nfl_root / str(season)
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(directory / f"{name}.parquet")


def season_frame(season, weeks_reg=(1, 2), weeks_post=(19,), extra=None):
    """A tiny play-by-play season carrying both season types."""
    rows = {"season": [], "week": [], "play_id": [], "season_type": []}
    for week in weeks_reg:
        rows["season"].append(season); rows["week"].append(week)
        rows["play_id"].append(week * 100); rows["season_type"].append("REG")
    for week in weeks_post:
        rows["season"].append(season); rows["week"].append(week)
        rows["play_id"].append(week * 100); rows["season_type"].append("POST")
    if extra:
        rows.update({k: [v] * len(rows["season"]) for k, v in extra.items()})
    return rows


# --- the filtering contract ----------------------------------------------------

def test_the_post_season_is_on_disk_and_hidden_by_default(nfl_root):
    """Both halves of the design, in one test."""
    write_pbp(nfl_root, 2024, season_frame(2024))

    default = nv.load_pbp([2024])
    assert default["season_type"].unique().to_list() == ["REG"]
    assert default.height == 2

    everything = nv.load_pbp([2024], season_type=None, max_week=None)
    assert sorted(everything["season_type"].unique().to_list()) == ["POST", "REG"]
    assert everything.height == 3


def test_the_week_cap_is_separate_from_the_season_type(nfl_root):
    """A REG week 19 cannot happen, but the cap must not depend on that."""
    write_pbp(nfl_root, 2024, season_frame(2024, weeks_reg=(1, 19), weeks_post=()))
    assert nv.load_pbp([2024]).height == 1
    assert nv.load_pbp([2024], max_week=None).height == 2


def test_the_post_season_can_be_asked_for_on_its_own(nfl_root):
    write_pbp(nfl_root, 2024, season_frame(2024))
    out = nv.load_pbp([2024], season_type="POST", max_week=None)
    assert out["week"].to_list() == [19]


# --- reading across seasons that are not the same ------------------------------

def test_a_missing_season_is_skipped_not_raised(nfl_root):
    """The archive starts where nflverse does, and 1998 is a legitimate ask."""
    write_pbp(nfl_root, 2024, season_frame(2024))
    out = nv.load_pbp([1998, 2024])
    assert out["season"].unique().to_list() == [2024]


def test_no_seasons_at_all_returns_an_empty_frame_with_a_schema(nfl_root):
    out = nv.load_pbp([1998])
    assert out.height == 0
    assert {"season", "week"} <= set(out.columns)


def test_a_column_absent_from_an_old_season_does_not_fail_the_read(nfl_root):
    """`xpass` postdates 1999. Asking for it across both must not raise.

    A projection naming a column one file lacks raises mid-read, so the narrowing
    happens per season against that season's real schema.
    """
    write_pbp(nfl_root, 1999, season_frame(1999, weeks_post=()))
    write_pbp(nfl_root, 2024, season_frame(2024, weeks_post=(),
                                           extra={"xpass": 0.5}))
    out = nv.load_pbp([1999, 2024], columns=["play_id", "xpass"])
    assert out.height == 4
    assert out.filter(pl.col("season") == 1999)["xpass"].null_count() == 2


def test_the_keys_survive_a_column_projection(nfl_root):
    """Filtering needs season/week/season_type even when nobody asked for them."""
    write_pbp(nfl_root, 2024, season_frame(2024))
    out = nv.load_pbp([2024], columns=["play_id"])
    assert out.height == 2
    assert {"season", "week", "season_type"} <= set(out.columns)


# --- the annotation pulls ------------------------------------------------------

def test_annotations_skip_the_seasons_that_predate_them(nfl_root):
    """FTN starts in 2022; a read spanning the training window walks that edge."""
    write_pbp(nfl_root, 2024, {"season": [2024], "nflverse_play_id": [1]},
              name="ftn_charting")
    out = nv.load_pbp_annotation([2016, 2024], "ftn_charting")
    assert out.height == 1


def test_an_annotation_with_no_seasons_returns_an_empty_frame(nfl_root):
    assert nv.load_pbp_annotation([2016], "pfr_pass").height == 0


def test_play_by_play_is_refused_by_the_annotation_loader(nfl_root):
    """It filters season type and week; this does not, and silently differing
    contracts behind one name is how a caller gets playoff games it did not ask
    for."""
    with pytest.raises(ValueError, match="load_pbp"):
        nv.load_pbp_annotation([2024], "pbp")


def test_an_unknown_pull_name_is_a_keyerror(nfl_root):
    with pytest.raises(KeyError):
        nv.pbp_path(2024, "nonesuch")


# --- availability --------------------------------------------------------------

def test_availability_reports_only_what_is_on_disk(nfl_root):
    """~26 MB a season, so a caller wanting 1999-2025 should know before reading."""
    write_pbp(nfl_root, 2024, season_frame(2024))
    assert nv.pbp_seasons_available(range(1999, 2026)) == [2024]
    assert nv.pbp_seasons_available(range(1999, 2026), "ftn_charting") == []
