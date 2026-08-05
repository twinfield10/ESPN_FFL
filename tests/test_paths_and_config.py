"""Path resolution and config loading.

These are the pieces that broke the working-directory contract before: data
paths were bare relative strings, so the scrapers and the analysis modules
needed mutually incompatible working directories.
"""

import pathlib

import pytest

from Scripts import config_utils, paths


def test_repo_root_is_the_repo():
    assert (paths.REPO_ROOT / "populateGoogleSheet.py").exists()
    assert (paths.REPO_ROOT / "Scripts" / "__init__.py").exists()


def test_resolve_is_cwd_independent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert paths.resolve("Data/NFL_Schedules.csv") == paths.NFL_SCHEDULE_CSV
    assert paths.resolve("Data/x.csv").is_absolute()


def test_resolve_passes_absolute_through():
    p = pathlib.Path("/tmp/somewhere.csv")
    assert paths.resolve(p) == p


@pytest.mark.parametrize("builder", [paths.season_dir, paths.landing_dir])
def test_season_dirs_are_separated_by_year(builder):
    """The 2026 bug: without a season component, a new season's scrape merged
    into the previous season's files."""
    a = builder("BetOnline", 2025, "x.parquet")
    b = builder("BetOnline", 2026, "x.parquet")
    assert a != b
    assert "2025" in a.parts and "2026" in b.parts
    assert a.parent.exists() and b.parent.exists()


def test_config_loads_and_every_league_is_complete():
    cfg = config_utils.load_config()
    assert isinstance(cfg["season"], int)
    for key, league in cfg["leagues"].items():
        assert "display_name" in league, f"{key} is missing display_name"
        for field in ("id", "start", "end", "primary_owner"):
            assert field in league, f"{key} is missing {field}"


def test_display_names_are_unique():
    """display_name is the Google Sheet name; a collision would overwrite."""
    names = [v["display_name"] for v in config_utils.load_config()["leagues"].values()]
    assert len(names) == len(set(names))


def test_build_lg_vars_falls_back_to_shared_credentials():
    cfg = {
        "season": 2026,
        "credentials": {"espn_id": "SHARED_S2", "s_id": "SHARED_SWID"},
        "leagues": {
            "inherits": {
                "id": 1, "display_name": "Inherits", "start": 2020, "end": 2026,
                "primary_owner": "A",
            },
            "overrides": {
                "id": 2, "display_name": "Overrides", "start": 2020, "end": 2026,
                "primary_owner": "B", "espn_s2": "OWN_S2", "swid": "OWN_SWID",
            },
        },
    }
    lg = config_utils.build_lg_vars(cfg)
    assert lg["Inherits"]["ESPN_S2"] == "SHARED_S2"
    assert lg["Inherits"]["SWID"] == "SHARED_SWID"
    assert lg["Overrides"]["ESPN_S2"] == "OWN_S2"
    assert lg["Overrides"]["SWID"] == "OWN_SWID"


def test_build_lg_vars_keyed_by_display_name():
    """Previously a hardcoded snake_case->display dict with a bare subscript,
    so any league added to the YAML without editing two files raised KeyError."""
    cfg = {
        "season": 2026,
        "credentials": {"espn_id": "s2", "s_id": "swid"},
        "leagues": {
            "brand_new_league": {
                "id": 99, "display_name": "Brand New", "start": 2026,
                "end": 2026, "primary_owner": "C",
            }
        },
    }
    lg = config_utils.build_lg_vars(cfg)
    assert "Brand New" in lg
    assert lg["Brand New"]["key"] == "brand_new_league"


def test_missing_config_points_at_the_example(tmp_path):
    with pytest.raises(FileNotFoundError, match="config.example.yaml"):
        config_utils.load_config(tmp_path / "nope.yaml")
