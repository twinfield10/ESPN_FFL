"""The availability layer: denominators, id joins, and the pre-season path.

Availability is the first feature family because plan 16's step 0 measured the
whole G1 deficit sitting there. What is pinned here is the arithmetic underneath
it, which had two bugs on its first pass and both were the silent kind: an
unsigned counter that wrapped to 4.29 billion on a subtraction, and a denominator
that counted calendar weeks so a player who never missed a game came out 106%
available.

Synthetic parquet in ``tmp_path``. No network, and no dependency on the gitignored
pulls.
"""

import json

import polars as pl
import pytest

from Scripts import paths
from Scripts.usage import context as ctx


@pytest.fixture
def nfl_root(tmp_path, monkeypatch):
    """Redirect ``Data/NFL`` to ``tmp_path``."""
    monkeypatch.setattr(paths, "DATA_DIR", tmp_path / "Data")
    return tmp_path / "Data" / "NFL"


def write_rosters(nfl_root, season, rows):
    """Write a rosters_weekly parquet from ``(gsis_id, week, team, status)``."""
    directory = nfl_root / str(season)
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "season": [season] * len(rows),
        "week": [r[1] for r in rows],
        "gsis_id": [r[0] for r in rows],
        "team": [r[2] for r in rows],
        "status": [r[3] for r in rows],
        "position": [r[4] if len(r) > 4 else "RB" for r in rows],
        "full_name": [f"Player {r[0]}" for r in rows],
        "espn_id": ["1"] * len(rows),
        "pfr_id": [f"P{r[0]}" for r in rows],
        "years_exp": [3] * len(rows),
        "depth_chart_position": ["RB"] * len(rows),
    }).write_parquet(directory / "rosters_weekly.parquet")


def player_weeks(rows, season=2025):
    """A player_weeks frame from ``(gsis_id, week, team)``."""
    return pl.DataFrame({
        "season": pl.Series([season] * len(rows), dtype=pl.Int32),
        "week": pl.Series([r[1] for r in rows], dtype=pl.Int32),
        "gsis_id": [r[0] for r in rows],
        "team": [r[2] for r in rows],
    })


# --- the denominator -----------------------------------------------------

def test_team_games_counts_games_not_calendar_weeks():
    """Rosters carry a row in the bye week; player_weeks do not.

    Counting calendar weeks is what produced an availability of 17/16.
    """
    frame = player_weeks([("a", w, "SEA") for w in (1, 2, 4)]      # bye in week 3
                         + [("b", w, "LA") for w in (1, 2, 3, 4)])
    games = ctx.team_games(frame).sort("team")
    assert games["team_games"].to_list() == [4, 3]  # LA 4, SEA 3


def test_team_games_refuses_a_frame_with_no_team_column():
    """A frame selected down to (season, week, gsis_id) looks usable and is not."""
    frame = player_weeks([("a", 1, "SEA")]).drop("team")
    with pytest.raises(KeyError, match="team_games needs a `team` column"):
        ctx.team_games(frame)


# --- availability --------------------------------------------------------

def test_a_player_who_played_every_game_is_fully_available(nfl_root):
    write_rosters(nfl_root, 2025, [("a", w, "SEA", "ACT") for w in range(1, 5)])
    frame = player_weeks([("a", w, "SEA") for w in range(1, 5)])
    out = ctx.season_availability([2025], frame)
    row = out.row(0, named=True)
    assert row["games_played"] == 4
    assert row["games_available"] == 4
    assert row["games_missed"] == 0
    assert row["availability"] == pytest.approx(1.0)


def test_games_missed_never_wraps_around(nfl_root):
    """The counts are Int32, not the UInt32 `len` returns.

    An unsigned subtraction that should go negative wraps to 4,294,967,295, which
    is what the first version of this did -- and it survived a describe() because
    only one row in a season hit it.
    """
    write_rosters(nfl_root, 2025, [("a", w, "SEA", "ACT") for w in (1, 2)])
    # Three appearances against a two-game team: impossible in real data, but the
    # arithmetic must not produce a nine-digit answer.
    frame = player_weeks([("a", 1, "SEA"), ("a", 2, "SEA"), ("a", 3, "LA")])
    out = ctx.season_availability([2025], frame)
    row = out.row(0, named=True)
    assert row["games_missed"] == 0
    assert row["availability"] <= 1.0


def test_a_player_on_reserve_is_counted_as_absent_not_healthy(nfl_root):
    """The caveat plan 16 recorded against its own injury table: the weekly report
    drops a player once he lands on IR, so appearances alone read him as available.
    """
    write_rosters(nfl_root, 2025,
                  [("a", 1, "SEA", "ACT"), ("a", 2, "SEA", "ACT"),
                   ("a", 3, "SEA", "RES"), ("a", 4, "SEA", "RES")])
    frame = player_weeks([("a", 1, "SEA"), ("a", 2, "SEA"),
                          ("b", 3, "SEA"), ("b", 4, "SEA")])
    out = ctx.season_availability([2025], frame).filter(pl.col("gsis_id") == "a")
    row = out.row(0, named=True)
    assert row["weeks_active"] == 2
    assert row["weeks_on_reserve"] == 2
    assert row["games_played"] == 2
    assert row["games_missed"] == 2


def test_a_traded_player_gets_one_seasons_worth_of_games(nfl_root):
    """The larger of his teams' slates, not their sum -- he had one season to play."""
    write_rosters(nfl_root, 2025,
                  [("a", 1, "SEA", "ACT"), ("a", 2, "SEA", "ACT"),
                   ("a", 3, "LA", "ACT"), ("a", 4, "LA", "ACT")])
    frame = player_weeks([("a", w, "SEA") for w in (1, 2)]
                         + [("a", w, "LA") for w in (3, 4)]
                         + [("z", w, "SEA") for w in (1, 2, 3, 4)])
    out = ctx.season_availability([2025], frame).filter(pl.col("gsis_id") == "a")
    assert out["games_available"][0] == 4          # not 4 + 2
    assert out["availability"][0] == pytest.approx(1.0)


def test_a_rostered_player_who_never_appeared_is_zero_not_missing(nfl_root):
    write_rosters(nfl_root, 2025, [("bench", w, "SEA", "ACT") for w in range(1, 5)])
    frame = player_weeks([("other", w, "SEA") for w in range(1, 5)])
    out = ctx.season_availability([2025], frame).filter(pl.col("gsis_id") == "bench")
    assert out["games_played"][0] == 0
    assert out["availability"][0] == pytest.approx(0.0)


def test_an_unknown_roster_team_falls_back_to_the_seasons_slate(nfl_root):
    """A practice-squad code that played no games must not null the denominator."""
    write_rosters(nfl_root, 2025, [("a", w, "XXX", "ACT") for w in range(1, 5)])
    frame = player_weeks([("z", w, "SEA") for w in range(1, 5)])
    out = ctx.season_availability([2025], frame).filter(pl.col("gsis_id") == "a")
    assert out["games_available"][0] == 4
    assert out["games_missed"][0] == 4


# --- injuries ------------------------------------------------------------

def write_injuries(nfl_root, season, rows):
    """From ``(gsis_id, week, report_status, practice_status)``."""
    directory = nfl_root / str(season)
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "season": [season] * len(rows),
        "week": [r[1] for r in rows],
        "gsis_id": [r[0] for r in rows],
        "position": ["RB"] * len(rows),
        "full_name": ["N"] * len(rows),
        "report_status": [r[2] for r in rows],
        "practice_status": [r[3] for r in rows],
        "report_primary_injury": ["Knee"] * len(rows),
        "practice_primary_injury": ["Knee"] * len(rows),
    }).write_parquet(directory / "injuries.parquet")


def test_injury_severity_ranks_worst_highest(nfl_root):
    write_injuries(nfl_root, 2025, [
        ("a", 1, "Out", "Did Not Participate In Practice"),
        ("b", 1, "Questionable", "Full Participation in Practice"),
        ("c", 1, "Doubtful", "Limited Participation in Practice"),
        ("d", 1, None, None),
    ])
    out = ctx.load_injuries([2025]).sort("gsis_id")
    assert out["report_rank"].to_list() == [3, 1, 2, 0]
    assert out["practice_rank"].to_list() == [3, 1, 2, 0]


def test_an_unrecognised_designation_ranks_zero_rather_than_raising(nfl_root):
    """Upstream added a `Note` status; 6 rows carry it across ten seasons."""
    write_injuries(nfl_root, 2025, [("a", 1, "Note", "Something New")])
    out = ctx.load_injuries([2025])
    assert out["report_rank"][0] == 0
    assert out["practice_rank"][0] == 0


# --- snap counts ---------------------------------------------------------

def write_snaps(nfl_root, season, rows):
    """From ``(pfr_player_id, week, offense_pct)``."""
    directory = nfl_root / str(season)
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "season": [season] * len(rows),
        "week": [r[1] for r in rows],
        "pfr_player_id": [r[0] for r in rows],
        "player": ["N"] * len(rows),
        "position": ["RB"] * len(rows),
        "team": ["SEA"] * len(rows),
        "offense_snaps": [40.0] * len(rows),
        "offense_pct": [float(r[2]) for r in rows],
    }).write_parquet(directory / "snap_counts.parquet")


def test_snaps_resolve_to_gsis_through_the_crosswalk(nfl_root):
    write_snaps(nfl_root, 2025, [("PfrA", 1, 0.8), ("PfrB", 1, 0.4)])
    crosswalk = pl.DataFrame({"pfr_id": ["PfrA", "PfrB"],
                              "gsis_id": ["00-1", "00-2"]})
    out = ctx.load_snap_counts([2025], crosswalk=crosswalk)
    assert sorted(out["gsis_id"].to_list()) == ["00-1", "00-2"]


def test_an_unresolvable_snap_row_is_dropped(nfl_root):
    """A snap count with no id joins to nothing; keeping it only inflates counts."""
    write_snaps(nfl_root, 2025, [("PfrA", 1, 0.8), ("Unknown", 1, 0.4)])
    crosswalk = pl.DataFrame({"pfr_id": ["PfrA"], "gsis_id": ["00-1"]})
    out = ctx.load_snap_counts([2025], crosswalk=crosswalk)
    assert out.height == 1


def test_an_ambiguous_pfr_id_is_refused_not_joined_on(nfl_root):
    """A key covering two players fans out rows and shifts every rank below it --
    the same reasoning Scripts/crosswalk.py applies to its 13 ambiguous espn_ids."""
    write_snaps(nfl_root, 2025, [("PfrA", 1, 0.8)])
    crosswalk = pl.DataFrame({"pfr_id": ["PfrA", "PfrA"],
                              "gsis_id": ["00-1", "00-2"]})
    out = ctx.load_snap_counts([2025], crosswalk=crosswalk)
    assert out.is_empty()


# --- the pre-season path -------------------------------------------------

def test_a_missing_in_season_pull_says_it_is_expected_pre_season(nfl_root):
    """Three of the four cannot be pulled before week 1, so telling someone to
    re-run the script would be wrong. Verified live 2026-08-07: nflreadr refuses
    injuries/snap_counts/depth_charts while most_recent_season() is behind 2026.
    """
    with pytest.raises(FileNotFoundError, match="once the season is under way"):
        ctx.load_injuries([2026])


def test_a_missing_roster_pull_names_the_command_that_fixes_it(nfl_root):
    with pytest.raises(FileNotFoundError, match="Rscript R/GetContext.R"):
        ctx.load_rosters([2026])


def test_meta_is_none_rather_than_raising_when_absent(nfl_root):
    assert ctx.load_meta(2026) is None


def test_meta_round_trips(nfl_root):
    directory = nfl_root / "2026"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "context_meta.json").write_text(
        json.dumps({"season": 2026, "has_current_injury_report": False}))
    assert ctx.load_meta(2026)["has_current_injury_report"] is False


def test_an_unknown_artifact_name_is_a_keyerror(nfl_root):
    with pytest.raises(KeyError, match="Unknown context artifact"):
        ctx.artifact_path(2026, "practice_squad")
