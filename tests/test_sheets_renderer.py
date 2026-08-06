"""``populateGoogleSheet.py`` as a renderer over the store.

The property worth pinning is that it no longer ingests. ``run()`` used to hold a
line-for-line copy of ``equivalence.build_league_frame`` -- fetch, lineups by
matchup, free-agent market, concat/fillna/dedupe, blend -- and two copies of one
sequence is the shape that already cost this repo once (12 projection functions,
8 drifted). These tests fail if that copy ever comes back.

No network and no Google connection: ``write_to_google`` is replaced throughout.
"""

import ast

import pandas as pd
import pytest

from Scripts import paths, store
from Scripts.paths import REPO_ROOT

DRIVER = REPO_ROOT / "populateGoogleSheet.py"

#: The ten tabs a league's Sheet has always had.
EXPECTED_TABS = [
    "League_Projections", "Lineup", "FA_QBs", "FA_RBs", "FA_WRs", "FA_TEs",
    "FA_FLX", "FA_DST", "FA_KCK", "FA_IDP",
]

#: Names that only exist to pull data out of ESPN. If the renderer references any
#: of them again, it has grown a second ingest path.
INGEST_NAMES = {
    "fetch_league", "get_ply_stats_by_matchup", "build_fa_market", "clean_lineups",
    "clean_pinny", "clean_bol", "compute_weighted_stats", "proj_to_score",
}


@pytest.fixture
def driver(tmp_path, monkeypatch):
    """Import the driver with the store redirected and Google stubbed out."""
    monkeypatch.setattr(paths, "STORE_DIR", tmp_path / "Store")
    import populateGoogleSheet as p

    published = []
    monkeypatch.setattr(p, "write_to_google",
                        lambda **kw: published.append(kw["league_name"]))
    monkeypatch.setattr(p.time, "sleep", lambda _s: None)
    p.published = published
    return p


@pytest.fixture
def lineups():
    """A frame with the columns the ten table builders read."""
    rows = []
    for i, (name, pos, owner, slot) in enumerate([
        ("QB One", "QB", "Tommy Winfield", "QB"),
        ("RB One", "RB", "Tommy Winfield", "RB"),
        ("WR One", "WR", "Someone Else", "WR"),
        ("TE One", "TE", "Free Agent", "TE"),
        ("K One", "K", "Free Agent", "K"),
        ("DST One", "D/ST", "Tommy Winfield", "D/ST"),
    ]):
        rows.append({
            "week": 1, "team_owner": owner, "team_name": f"Team {owner}",
            "player_name": name, "primaryPosition": pos, "slotPosition": slot,
            "points": 0.0, "projPoints": 10.0 + i, "FP_Points": 11.0 + i,
            "PINNY_Points": 12.0 + i, "BOL_Points": 13.0 + i,
            "TRUE_Points": 14.0 + i, "PosRank": 1.0, "ESPN_PosRank": 1.0,
            "FP_PosRank": 1.0, "PINNY_PosRank": 1.0, "BOL_PosRank": 1.0,
            "TRUE_PosRank": 1.0,
        })
    return pd.DataFrame(rows)


# --- the renderer no longer ingests --------------------------------------

def test_driver_does_not_reference_any_ingest_function():
    """The regression guard. `run()` held a copy of build_league_frame's body."""
    tree = ast.parse(DRIVER.read_text())
    referenced = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    referenced |= {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    leaked = sorted(INGEST_NAMES & referenced)
    assert not leaked, f"the Sheets renderer is ingesting again: {leaked}"


def test_driver_reads_the_store():
    tree = ast.parse(DRIVER.read_text())
    imported = {a.name for n in ast.walk(tree)
                if isinstance(n, ast.ImportFrom) for a in n.names}
    assert "read_league_store" in imported
    assert "read_meta" in imported


# --- table building ------------------------------------------------------

def test_build_tables_produces_all_ten_tabs(driver, lineups):
    tables = driver.build_tables(lineups, curr_week=1,
                                 primary_own="Tommy Winfield")
    assert list(tables) == EXPECTED_TABS


def test_build_tables_is_indifferent_to_frame_origin(driver, lineups):
    """Store-read and in-memory frames must give identical tables -- this is what
    makes the migration off the inline ingest safe."""
    store.write_league_store(2026, "knights_ffl", lineups=lineups)
    from_store = store.read_league_store(2026, "knights_ffl", "lineups")

    a = driver.build_tables(lineups.copy(), 1, "Tommy Winfield")
    b = driver.build_tables(from_store.copy(), 1, "Tommy Winfield")
    for tab in EXPECTED_TABS:
        assert a[tab].shape == b[tab].shape, tab
        assert list(a[tab].columns) == list(b[tab].columns), tab


# --- run() ---------------------------------------------------------------

def test_run_publishes_from_the_store(driver, lineups):
    store.write_league_store(2026, "knights_ffl", lineups=lineups,
                             league=None, meta_extra={"current_week": 1})
    results = driver.run(["Knights_FFL"], season=2026)
    assert results == {"Knights_FFL": "ok"}
    assert driver.published == ["Knights_FFL"]


def test_run_skips_a_league_with_no_store(driver, lineups):
    """Publishing eight leagues must not be lost to one unbuilt store."""
    store.write_league_store(2026, "knights_ffl", lineups=lineups,
                             meta_extra={"current_week": 1})
    results = driver.run(["Knights_FFL", "GOP_Degenerates"], season=2026)

    assert results["Knights_FFL"] == "ok"
    assert "no store" in results["GOP_Degenerates"]
    assert driver.published == ["Knights_FFL"]


def test_run_takes_the_week_from_the_store(driver, lineups):
    """Not from a live fetch -- so the Sheet and the app report the same week from
    the same build."""
    captured = {}
    driver.build_tables = lambda lu, curr_week, primary_own: captured.setdefault(
        "week", curr_week) or {t: pd.DataFrame() for t in EXPECTED_TABS}

    store.write_league_store(2026, "knights_ffl", lineups=lineups,
                             meta_extra={"current_week": 9})
    driver.run(["Knights_FFL"], season=2026)
    assert captured["week"] == 9


def test_run_accepts_a_config_key(driver, lineups):
    store.write_league_store(2026, "knights_ffl", lineups=lineups,
                             meta_extra={"current_week": 1})
    assert driver.run(["knights_ffl"], season=2026) == {"knights_ffl": "ok"}
