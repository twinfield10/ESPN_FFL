"""Pooling nine league stores into one evaluation set, and the population flags.

Two things here have already been wrong once and are pinned as a result:

* **Pooling across leagues.** A stat line is league-independent, but the *columns*
  are not -- a non-PPR league carries no ``receivingReceptions`` at all -- so the
  collapse has to take the first non-null rather than the first row, and the
  cross-league agreement has to be measured rather than assumed.
* **The bye flag.** ``team_played`` has two different nulls in it: "no team known"
  and "the team did not play". Filling both the same way marked every bye week as
  played, which quietly credited the usage baseline for not knowing about byes.

Synthetic stores in ``tmp_path``. No network.
"""

import json

import polars as pl
import pytest

from Scripts import paths
from Scripts.usage import evalset
from Scripts.usage.baseline import USAGE_PREFIX

STATS = ("receivingYards", "receivingReceptions")


def write_store(root, season, league_key, frame):
    """Write a synthetic ``lineups`` artifact plus the ``meta.json`` that validates it.

    Args:
        root: Store root directory.
        season: Season year.
        league_key: League key.
        frame: The lineups frame.
    """
    directory = root / str(season) / league_key
    directory.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(directory / "lineups.parquet")
    (directory / "meta.json").write_text(json.dumps({"season": season}))


def lineups(rows, stats=STATS):
    """Build a frame shaped like the store's lineups, for the columns pooling reads.

    Args:
        rows: Iterable of dicts with ``week``, ``player_id`` and per-stat values.
        stats: Stats the league scores.

    Returns:
        pl.DataFrame: A lineups-shaped frame.
    """
    data = {
        "week": [r["week"] for r in rows],
        "player_id": [r["player_id"] for r in rows],
        "player_name": [r.get("player_name", "Somebody") for r in rows],
        "primaryPosition": [r.get("position", "WR") for r in rows],
        "points": [r.get("points", 0.0) for r in rows],
    }
    for stat in stats:
        data[stat] = [float(r.get(stat, 0.0)) for r in rows]
        data[f"ESPN_{stat}"] = [float(r.get(f"ESPN_{stat}", 0.0)) for r in rows]
        data[f"FP_{stat}"] = [float(r.get(f"FP_{stat}", 0.0)) for r in rows]
        data[f"FP_{stat}_is_imputed"] = [bool(r.get(f"FP_{stat}_is_imputed", False))
                                         for r in rows]
    return pl.DataFrame(data)


@pytest.fixture
def store_root(tmp_path, monkeypatch):
    """Redirect the store to ``tmp_path`` and stub the id crosswalk."""
    monkeypatch.setattr(paths, "STORE_DIR", tmp_path / "Store")
    monkeypatch.setattr(evalset, "id_map",
                        lambda *_: {"111": "00-0001", "222": "00-0002"})
    return tmp_path / "Store"


# --- pooling -------------------------------------------------------------

def test_pooling_collapses_to_one_row_per_player_week(store_root):
    """The same player-week in two leagues is one row, not two."""
    frame = lineups([{"week": 1, "player_id": "111", "receivingYards": 80.0}])
    write_store(store_root, 2025, "league_a", frame)
    write_store(store_root, 2025, "league_b", frame)

    pooled, report = build(2025)
    assert pooled.height == 1
    assert report["worst_cross_league_disagreement"] == 0.0
    assert set(report["leagues"]) == {"league_a", "league_b"}


def test_pooling_fills_a_stat_one_league_does_not_score(store_root):
    """A non-PPR league contributes nothing to receptions and blocks nothing."""
    ppr = lineups([{"week": 1, "player_id": "111", "receivingReceptions": 6.0,
                    "ESPN_receivingReceptions": 5.0}])
    standard = lineups([{"week": 1, "player_id": "111"}],
                       stats=("receivingYards",))
    write_store(store_root, 2025, "ppr_league", ppr)
    write_store(store_root, 2025, "standard_league", standard)

    pooled, report = build(2025)
    assert report["leagues"]["standard_league"]["unscored_stats"] == \
        ["receivingReceptions"]
    assert pooled["act_receivingReceptions"][0] == 6.0
    assert pooled["ESPN_receivingReceptions"][0] == 5.0


def test_cross_league_disagreement_is_reported_not_hidden(store_root):
    """Pooling assumes the leagues agree; the report is what checks it."""
    write_store(store_root, 2025, "league_a",
                lineups([{"week": 1, "player_id": "111", "receivingYards": 80.0}]))
    write_store(store_root, 2025, "league_b",
                lineups([{"week": 1, "player_id": "111", "receivingYards": 95.0}]))

    _, report = build(2025)
    assert report["worst_cross_league_disagreement"] == pytest.approx(15.0)


def test_gsis_id_is_attached_and_counted(store_root):
    """An unmatched player is kept, with a null id and an honest count."""
    write_store(store_root, 2025, "league_a", lineups([
        {"week": 1, "player_id": "111"},
        {"week": 1, "player_id": "999"},        # not in the crosswalk
    ]))
    pooled, report = build(2025)
    assert report["rows"] == 2
    assert report["with_gsis_id"] == 1
    assert pooled.filter(pl.col("player_id") == "999")["gsis_id"][0] is None


def test_no_store_names_the_command_that_builds_one(store_root):
    """A missing store is a setup step, not a mystery."""
    with pytest.raises(ValueError, match="Scripts.refresh"):
        build(2025)


def build(season):
    """Call ``build_eval_set`` for the stats these tests use."""
    return evalset.build_eval_set(season, stats=list(STATS))


# --- provenance ----------------------------------------------------------

def test_real_mask_reads_the_provenance_flags():
    """ESPN is never imputed; a null flag counts as imputed."""
    frame = pl.DataFrame({
        "ESPN_receivingYards": [10.0, None],
        "FP_receivingYards": [10.0, 10.0],
        "FP_receivingYards_is_imputed": [True, None],
    })
    assert frame.select(evalset.real_mask(frame, "ESPN", "receivingYards")) \
        .to_series().to_list() == [True, False]
    assert frame.select(evalset.real_mask(frame, "FP", "receivingYards")) \
        .to_series().to_list() == [False, False]
    # A source with no column at all is never real.
    assert frame.select(evalset.real_mask(frame, "PINNY", "receivingYards")
                        .alias("x")).to_series().to_list() == [False]


# --- population flags ----------------------------------------------------

@pytest.fixture
def usage_pull(tmp_path, monkeypatch):
    """A synthetic opportunity parquet: SEA plays weeks 1-2, is on bye in week 3."""
    monkeypatch.setattr(paths, "DATA_DIR", tmp_path / "Data")
    directory = tmp_path / "Data" / "NFL" / "2025"
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "season": [2025, 2025, 2025],
        "week": [1, 2, 3],
        "gsis_id": ["00-0001", "00-0001", "00-0002"],
        "full_name": ["A", "A", "B"],
        "position": ["WR", "WR", "WR"],
        "posteam": ["SEA", "SEA", "LAR"],
        "rec_yards_gained": [50.0, 60.0, 70.0],
        "rec_yards_gained_exp": [55.0, 65.0, 75.0],
        "receptions": [4.0, 5.0, 6.0],
        "receptions_exp": [4.5, 5.5, 6.5],
    }, schema_overrides={"season": pl.Int32, "week": pl.Int32}).write_parquet(
        directory / "opportunity.parquet")
    return directory


def test_bye_week_is_not_counted_as_played(store_root, usage_pull):
    """A team with no snaps that week is on a bye, and must be flagged as such."""
    write_store(store_root, 2025, "league_a", lineups([
        {"week": 3, "player_id": "111"},     # SEA: on bye in week 3
        {"week": 3, "player_id": "222"},     # LAR: playing, and played
    ]))
    pooled, _ = build(2025)
    predictions = pl.DataFrame({
        "week": [3, 3],
        "gsis_id": ["00-0001", "00-0002"],
        "last_posteam": ["SEA", "LAR"],
        "weeks_of_history": [2, 0],
        f"{USAGE_PREFIX}receivingYards": [58.0, 70.0],
    }, schema_overrides={"week": pl.Int32})

    attached = evalset.attach_usage(pooled, predictions, season=2025)
    by_player = {row["gsis_id"]: row for row in attached.iter_rows(named=True)}
    assert by_player["00-0001"]["team_played"] is False
    assert by_player["00-0001"]["played"] is False
    assert by_player["00-0002"]["team_played"] is True
    assert by_player["00-0002"]["played"] is True


def test_a_player_with_no_history_stays_in_the_population(store_root, usage_pull):
    """No known team is not a bye, and dropping him would change the population."""
    write_store(store_root, 2025, "league_a",
                lineups([{"week": 3, "player_id": "999"}]))
    pooled, _ = build(2025)
    empty = pl.DataFrame(
        {"week": [], "gsis_id": [], "last_posteam": [], "weeks_of_history": [],
         f"{USAGE_PREFIX}receivingYards": []},
        schema={"week": pl.Int32, "gsis_id": pl.Utf8, "last_posteam": pl.Utf8,
                "weeks_of_history": pl.Int64,
                f"{USAGE_PREFIX}receivingYards": pl.Float64},
    )
    attached = evalset.attach_usage(pooled, empty, season=2025)
    assert attached["team_played"].to_list() == [True]
    assert attached["played"].to_list() == [False]


def test_usage_grid_drops_rows_with_no_play_by_play_id(store_root):
    """The grid is what the model is asked about, so an unjoinable row is not on it."""
    write_store(store_root, 2025, "league_a", lineups([
        {"week": 1, "player_id": "111"},
        {"week": 1, "player_id": "999"},
    ]))
    pooled, _ = build(2025)
    grid = evalset.usage_grid(pooled)
    assert grid.height == 1
    assert grid["gsis_id"].to_list() == ["00-0001"]
