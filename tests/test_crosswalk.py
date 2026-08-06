"""The cross-provider player identity table.

Logic is tested against a synthetic crosswalk written to tmp_path, so these do not
depend on what upstream published today. Two tests do check the committed file,
because a crosswalk that is absent or implausible should fail loudly rather than
silently degrade every join built on it.

No network.
"""

import pandas as pd
import pytest

from Scripts import crosswalk, paths


@pytest.fixture
def synthetic(tmp_path, monkeypatch):
    """A crosswalk with the shapes that matter: clean pairs, a collision, a gap.

    The collision mirrors a real upstream data error -- Nate Jones (WR) and Nathan
    Jones (CB) share both ``espn_id`` 5730 and ``gsis_id`` 00-0022828.
    """
    table = pd.DataFrame([
        # Ordinary, fully-identified players.
        {"name": "Jahmyr Gibbs", "position": "RB", "team": "DET",
         "gsis_id": "00-0039139", "espn_id": "4429795", "fantasypros_id": "22968"},
        {"name": "Puka Nacua", "position": "WR", "team": "LAR",
         "gsis_id": "00-0039075", "espn_id": "4426515", "fantasypros_id": "23180"},
        # A shared name resolved by distinct ids -- the case the crosswalk exists for.
        {"name": "Lamar Jackson", "position": "QB", "team": "BAL",
         "gsis_id": "00-0034796", "espn_id": "3916387", "fantasypros_id": "17282"},
        {"name": "Lamar Jackson", "position": "CB", "team": None,
         "gsis_id": "00-0036152", "espn_id": "4034849", "fantasypros_id": None},
        # An upstream collision: two players, one id. Must never be joined on.
        {"name": "Nate Jones", "position": "WR", "team": None,
         "gsis_id": "00-0022828", "espn_id": "5730", "fantasypros_id": None},
        {"name": "Nathan Jones", "position": "CB", "team": None,
         "gsis_id": "00-0022828", "espn_id": "5730", "fantasypros_id": None},
        # Known to nflverse but with no ESPN id.
        {"name": "Obscure Rookie", "position": "WR", "team": "SEA",
         "gsis_id": "00-0041999", "espn_id": None, "fantasypros_id": None},
        # Upstream writes empty strings as well as nulls.
        {"name": "Blank Id Player", "position": "TE", "team": "KC",
         "gsis_id": "", "espn_id": "9999999", "fantasypros_id": "NA"},
    ])
    path = tmp_path / "player_ids.parquet"
    table.to_parquet(path)
    monkeypatch.setattr(paths, "PLAYER_IDS_PARQUET", path)
    monkeypatch.setattr(crosswalk, "PLAYER_IDS_PARQUET", path)
    crosswalk.reset_cache()
    crosswalk._mapping.cache_clear()
    yield path
    crosswalk.reset_cache()
    crosswalk._mapping.cache_clear()


@pytest.fixture
def board():
    """A frame shaped like a draft board, ESPN-keyed."""
    return pd.DataFrame([
        {"player_id": 4429795, "player_name": "Jahmyr Gibbs", "primaryPosition": "RB"},
        {"player_id": 3916387, "player_name": "Lamar Jackson", "primaryPosition": "QB"},
        {"player_id": 4034849, "player_name": "Lamar Jackson", "primaryPosition": "CB"},
        {"player_id": 5730, "player_name": "Nate Jones", "primaryPosition": "WR"},
        {"player_id": -1, "player_name": "Ravens D/ST", "primaryPosition": "D/ST"},
    ])


# --- loading -------------------------------------------------------------

def test_missing_crosswalk_names_the_command(tmp_path, monkeypatch):
    monkeypatch.setattr(crosswalk, "PLAYER_IDS_PARQUET", tmp_path / "nope.parquet")
    crosswalk.reset_cache()
    with pytest.raises(FileNotFoundError, match="GetPlayerIDs.R"):
        crosswalk.load_crosswalk()


def test_blank_ids_read_as_missing(synthetic):
    table = crosswalk.load_crosswalk()
    blank = table[table["name"] == "Blank Id Player"].iloc[0]
    assert pd.isna(blank["gsis_id"])         # was ""
    assert pd.isna(blank["fantasypros_id"])  # was "NA"


# --- the collision, which is the point -----------------------------------

def test_shared_names_resolve_to_distinct_ids(synthetic, board):
    """Two different Lamar Jacksons. A name join cannot tell them apart; this can."""
    out = crosswalk.attach_gsis_id(board, warn_below=None)
    jacksons = out[out["player_name"] == "Lamar Jackson"]
    assert jacksons["gsis_id"].tolist() == ["00-0034796", "00-0036152"]
    assert jacksons["gsis_id"].nunique() == 2


def test_ambiguous_ids_are_excluded_from_the_map(synthetic):
    """Upstream really does give two players one id. Joining on it fans out rows."""
    assert crosswalk.ambiguous_ids("espn_id") == ["5730"]
    assert crosswalk.ambiguous_ids("gsis_id") == ["00-0022828"]

    mapping = crosswalk.id_map("espn_id", "gsis_id")
    assert "5730" not in mapping
    assert "4429795" in mapping


def test_attaching_never_adds_rows(synthetic, board):
    """The reason this uses Series.map rather than a merge. A board that silently
    gains a duplicated player shifts every positional rank below it."""
    out = crosswalk.attach_gsis_id(board, warn_below=None)
    assert len(out) == len(board)
    # And the player holding the ambiguous id simply goes unmatched.
    assert pd.isna(out.loc[out["player_name"] == "Nate Jones", "gsis_id"].iloc[0])


# --- direction and coverage ---------------------------------------------

def test_attach_espn_id_is_the_reverse(synthetic):
    stats = pd.DataFrame({"gsis_id": ["00-0039139", "00-0041999"],
                          "player_name": ["Jahmyr Gibbs", "Obscure Rookie"]})
    out = crosswalk.attach_espn_id(stats, warn_below=None)
    assert out.loc[0, "espn_id"] == "4429795"
    # Known to nflverse, no ESPN id -- must be NA, not an error.
    assert pd.isna(out.loc[1, "espn_id"])


def test_coverage_counts_and_samples_the_misses(synthetic, board):
    out = crosswalk.attach_gsis_id(board, warn_below=None)
    result = crosswalk.coverage(out, "gsis_id")
    assert result.total == 5
    assert result.matched == 3          # Gibbs + both Jacksons
    assert "Ravens D/ST" in result.unmatched_sample
    assert "matched" in str(result)


def test_low_coverage_warns(synthetic):
    """A silent collapse in match rate is the failure this replaces, so it must not
    become a new silent failure."""
    unknown = pd.DataFrame({"player_id": [111, 222, 333],
                            "player_name": ["A", "B", "C"]})
    with pytest.warns(crosswalk.CrosswalkWarning, match="matched only"):
        crosswalk.attach_gsis_id(unknown, warn_below=70.0)


def test_missing_source_column_is_an_error(synthetic, board):
    with pytest.raises(KeyError, match="nope"):
        crosswalk.attach_gsis_id(board, espn_id_column="nope")


def test_unknown_id_column_is_an_error(synthetic):
    with pytest.raises(KeyError, match="not in the crosswalk"):
        crosswalk.id_map("espn_id", "not_an_id_column")


# --- the committed file --------------------------------------------------

def test_the_real_crosswalk_is_present_and_plausible():
    """It is committed, like the scoring registry, so it should be here. A crosswalk
    that quietly shrank would degrade every join built on it."""
    table = crosswalk.load_crosswalk()
    assert len(table) > 10_000
    pairs = table[["gsis_id", "espn_id"]].dropna()
    assert len(pairs) > 5_000, f"only {len(pairs)} gsis<->espn pairs"


def test_the_real_crosswalk_has_few_ambiguous_ids():
    """A handful of upstream data errors is expected; a surge would mean the file
    changed shape and the id columns no longer mean what this module assumes."""
    for column in ("gsis_id", "espn_id"):
        assert len(crosswalk.ambiguous_ids(column)) < 50, column
