"""Season-level features: the as-of boundary, shrinkage, and the current context.

The boundary is the thing worth pinning. Plan 16 names leakage as the risk that
makes a backtest excellent and the live model useless, and at season level it has
two doors rather than one: a lagged join that quietly reaches the predicted season,
and a positional baseline fitted over seasons that include it. The second is the
dangerous one -- it is small, it flatters every efficiency feature at once, and
nothing about the output looks wrong.

Synthetic parquet in ``tmp_path``. No network.
"""

import polars as pl
import pytest

from Scripts import paths
from Scripts.usage import features as ft


@pytest.fixture
def nfl_root(tmp_path, monkeypatch):
    """Redirect ``Data/NFL`` to ``tmp_path``."""
    monkeypatch.setattr(paths, "DATA_DIR", tmp_path / "Data")
    return tmp_path / "Data" / "NFL"


def write_player_weeks(nfl_root, season, rows):
    """From ``(gsis_id, week, position, targets, receptions, receiving_yards)``."""
    directory = nfl_root / str(season)
    directory.mkdir(parents=True, exist_ok=True)
    n = len(rows)
    pl.DataFrame({
        "season": [season] * n,
        "week": [r[1] for r in rows],
        "gsis_id": [r[0] for r in rows],
        "position": [r[2] for r in rows],
        "team": ["SEA"] * n,
        "targets": [float(r[3]) for r in rows],
        "receptions": [float(r[4]) for r in rows],
        "receiving_yards": [float(r[5]) for r in rows],
        "receiving_tds": [0.0] * n,
        "receiving_air_yards": [float(r[3]) * 8 for r in rows],
        "carries": [0.0] * n,
        "rushing_yards": [0.0] * n,
        "rushing_tds": [0.0] * n,
        "attempts": [0.0] * n,
        "passing_yards": [0.0] * n,
        "passing_tds": [0.0] * n,
        "passing_interceptions": [0.0] * n,
        "target_share": [0.2] * n,
        "air_yards_share": [0.25] * n,
        "wopr": [0.5] * n,
    }).write_parquet(directory / "player_weeks.parquet")


def write_opportunity(nfl_root, season, rows):
    """From ``(gsis_id, week, rec_yards, rec_yards_exp)``."""
    directory = nfl_root / str(season)
    directory.mkdir(parents=True, exist_ok=True)
    n = len(rows)
    frame = {
        "season": [season] * n,
        "week": [r[1] for r in rows],
        "gsis_id": [r[0] for r in rows],
        "full_name": ["N"] * n,
        "position": ["WR"] * n,
        "posteam": ["SEA"] * n,
    }
    for stat, (actual, expected) in ft.USAGE_STATS.items():
        if stat == "receivingYards":
            frame[actual] = [float(r[2]) for r in rows]
            frame[expected] = [float(r[3]) for r in rows]
        else:
            frame[actual] = [0.0] * n
            frame[expected] = [0.0] * n
    pl.DataFrame(frame).write_parquet(directory / "opportunity.parquet")


def write_rosters(nfl_root, season, rows):
    """From ``(gsis_id, week, team, position, entry_year)``."""
    directory = nfl_root / str(season)
    directory.mkdir(parents=True, exist_ok=True)
    n = len(rows)
    pl.DataFrame({
        "season": [season] * n,
        "week": [r[1] for r in rows],
        "gsis_id": [r[0] for r in rows],
        "team": [r[2] for r in rows],
        "position": [r[3] for r in rows],
        "status": ["ACT"] * n,
        "full_name": [f"Player {r[0]}" for r in rows],
        "espn_id": ["1"] * n,
        "pfr_id": [f"P{r[0]}" for r in rows],
        "years_exp": [0 if r[4] == season else 3 for r in rows],
        "entry_year": [r[4] for r in rows],
        "depth_chart_position": [r[3] for r in rows],
    }).write_parquet(directory / "rosters_weekly.parquet")


def full_season(nfl_root, season, players, weeks=4, targets=8):
    """Write all three artifacts for one season with uniform usage."""
    rows = [(p, w, "WR", targets, targets / 2, targets * 10)
            for p in players for w in range(1, weeks + 1)]
    write_player_weeks(nfl_root, season, rows)
    write_opportunity(nfl_root, season,
                      [(p, w, targets * 10, targets * 9)
                       for p in players for w in range(1, weeks + 1)])
    write_rosters(nfl_root, season,
                  [(p, w, "SEA", "WR", season - 3)
                   for p in players for w in range(1, weeks + 1)])


# --- the as-of boundary --------------------------------------------------

def test_history_may_not_include_the_predicted_season(nfl_root):
    """An error rather than a silent filter: the boundary is the whole point."""
    with pytest.raises(ValueError, match="not allowed to see"):
        ft.prior_season_frame(2026, [2024, 2025, 2026])


def test_history_may_not_reach_past_the_predicted_season(nfl_root):
    with pytest.raises(ValueError, match=r"\[2026, 2027\]"):
        ft.prior_season_frame(2025, [2024, 2026, 2027])


def test_every_feature_column_is_lagged_or_current_context(nfl_root):
    """A bare stat name means an unlagged join slipped in."""
    full_season(nfl_root, 2024, ["a", "b"])
    full_season(nfl_root, 2025, ["a", "b"])
    write_rosters(nfl_root, 2026,
                  [(p, 1, "SEA", "WR", 2021) for p in ("a", "b")])
    out = ft.season_features(2026, [2024, 2025])
    assert ft.leakage_columns(out, 2026) == []


def test_leakage_columns_names_an_unlagged_column(nfl_root):
    """The check has to be able to fail, or it is decoration."""
    frame = pl.DataFrame({"gsis_id": ["a"], "season": [2026],
                          "p1_targets_pg": [5.0], "targets_pg": [9.0]})
    assert ft.leakage_columns(frame, 2026) == ["targets_pg"]


def test_lag_one_and_lag_two_stay_distinguishable(nfl_root):
    """A back coming off one injured season looks nothing like one off two."""
    full_season(nfl_root, 2024, ["a"], weeks=4, targets=4)
    full_season(nfl_root, 2025, ["a"], weeks=4, targets=10)
    write_rosters(nfl_root, 2026, [("a", 1, "SEA", "WR", 2021)])
    out = ft.season_features(2026, [2024, 2025])
    row = out.row(0, named=True)
    assert row["p1_targets_pg"] == pytest.approx(10.0)
    assert row["p2_targets_pg"] == pytest.approx(4.0)


# --- totals and per-game rates -------------------------------------------

def test_per_game_rates_divide_by_appearances_not_the_schedule(nfl_root):
    """A player who played 4 of 17 weeks averaged over 4, not 17."""
    write_player_weeks(nfl_root, 2025,
                       [("a", w, "WR", 10, 5, 100) for w in (1, 2, 3, 4)])
    totals = ft.season_totals(ft.load_player_weeks([2025]))
    row = totals.row(0, named=True)
    assert row["games"] == 4
    assert row["targets_pg"] == pytest.approx(10.0)


def test_shares_are_season_means_not_divided_again(nfl_root):
    """target_share arrives already normalised; dividing by games would halve it."""
    write_player_weeks(nfl_root, 2025,
                       [("a", w, "WR", 10, 5, 100) for w in (1, 2)])
    totals = ft.season_totals(ft.load_player_weeks([2025]))
    assert totals["target_share"][0] == pytest.approx(0.2)


# --- efficiency and shrinkage -------------------------------------------

def test_baselines_pool_totals_rather_than_averaging_player_rates(nfl_root):
    """Otherwise a position's baseline is the mean of its part-timers' noise."""
    write_player_weeks(nfl_root, 2025, [
        ("heavy", 1, "WR", 100, 50, 1000),   # 10.0 yards per target
        ("light", 1, "WR", 1, 1, 30),        # 30.0, on one target
    ])
    totals = ft.season_totals(ft.load_player_weeks([2025]))
    base = ft.positional_baselines(totals)
    # Pooled: 1030 yards / 101 targets = 10.2. Averaged: (10 + 30) / 2 = 20.
    assert base["yards_per_target"][0] == pytest.approx(1030 / 101)


def test_a_low_volume_rate_is_pulled_toward_the_baseline(nfl_root):
    write_player_weeks(nfl_root, 2025, [
        ("bulk", 1, "WR", 400, 200, 4000),   # baseline-setting, 10.0
        ("tiny", 1, "WR", 2, 2, 60),         # 30.0 on two targets
    ])
    totals = ft.season_totals(ft.load_player_weeks([2025]))
    out = ft.attach_efficiency(totals, ft.positional_baselines(totals))
    tiny = out.filter(pl.col("gsis_id") == "tiny").row(0, named=True)
    assert tiny["yards_per_target_raw"] == pytest.approx(30.0)
    # k = 40 against n = 2, so nearly all baseline.
    assert tiny["yards_per_target"] < 12.0


def test_a_high_volume_rate_keeps_most_of_itself(nfl_root):
    write_player_weeks(nfl_root, 2025, [
        ("star", 1, "WR", 160, 100, 2400),   # 15.0 on heavy volume
        ("rest", 1, "WR", 400, 200, 3200),   # 8.0, sets the baseline low
    ])
    totals = ft.season_totals(ft.load_player_weeks([2025]))
    out = ft.attach_efficiency(totals, ft.positional_baselines(totals))
    star = out.filter(pl.col("gsis_id") == "star").row(0, named=True)
    assert star["yards_per_target"] > 13.0


def test_a_player_with_no_opportunity_gets_the_baseline_not_a_null(nfl_root):
    """Dropping him would make coverage depend on last season's usage, and a
    drafter still has to price him."""
    write_player_weeks(nfl_root, 2025, [
        ("bulk", 1, "WR", 400, 200, 4000),
        ("none", 1, "WR", 0, 0, 0),
    ])
    totals = ft.season_totals(ft.load_player_weeks([2025]))
    out = ft.attach_efficiency(totals, ft.positional_baselines(totals))
    none = out.filter(pl.col("gsis_id") == "none").row(0, named=True)
    assert none["yards_per_target_raw"] is None
    assert none["yards_per_target"] == pytest.approx(10.0)


def test_touchdown_rates_are_shrunk_harder_than_yardage_rates():
    """Plan 16 measured TD rate at +0.234 year over year against 0.86-0.92 for
    opportunity, so a player's own TD rate is mostly noise."""
    assert ft.SHRINKAGE_K["rec_td_per_target"] > ft.SHRINKAGE_K["yards_per_target"]
    assert ft.SHRINKAGE_K["rush_td_per_carry"] > ft.SHRINKAGE_K["yards_per_carry"]


def test_baselines_are_fitted_only_on_permitted_seasons(nfl_root):
    """The subtle leak: a baseline including the predicted season flatters every
    efficiency feature at once and nothing about the output looks wrong."""
    full_season(nfl_root, 2024, ["a"], targets=8)
    full_season(nfl_root, 2025, ["a"], targets=8)
    # 2026 exists on disk with wildly different efficiency. It must not be read.
    write_player_weeks(nfl_root, 2026,
                       [("a", w, "WR", 8, 8, 8000) for w in range(1, 5)])
    write_rosters(nfl_root, 2026, [("a", 1, "SEA", "WR", 2021)])

    prior = ft.prior_season_frame(2026, [2024, 2025])
    assert prior["season"].unique().sort().to_list() == [2024, 2025]
    base = ft.positional_baselines(ft.season_totals(ft.load_player_weeks([2024, 2025])))
    # 80 yards on 8 targets a game, not the 1000 the 2026 file would imply.
    assert base["yards_per_target"][0] == pytest.approx(10.0)


# --- current-season context ---------------------------------------------

def test_a_change_of_team_is_flagged(nfl_root):
    """Usage is sticky for a player in a stable situation; plan 18 wants the ones
    whose situation moved identified so the interval can widen."""
    full_season(nfl_root, 2025, ["stay", "move"])
    write_rosters(nfl_root, 2026, [("stay", 1, "SEA", "WR", 2021),
                                   ("move", 1, "LA", "WR", 2021)])
    out = ft.season_features(2026, [2025]).sort("gsis_id")
    by_id = dict(zip(out["gsis_id"], out["team_changed"]))
    assert by_id["move"] is True
    assert by_id["stay"] is False


def test_a_player_with_no_prior_team_has_an_unknown_move_not_a_false_one(nfl_root):
    """A rookie did not "stay" anywhere. False would be a claim; null is the truth."""
    full_season(nfl_root, 2025, ["veteran"])
    write_rosters(nfl_root, 2026, [("rook", 1, "SEA", "WR", 2026)])
    out = ft.season_features(2026, [2025])
    assert out["team_changed"][0] is None


def test_rookies_are_identified_from_entry_year(nfl_root):
    full_season(nfl_root, 2025, ["vet"])
    write_rosters(nfl_root, 2026, [("vet", 1, "SEA", "WR", 2021),
                                   ("rook", 1, "SEA", "WR", 2026)])
    out = ft.season_features(2026, [2025]).sort("gsis_id")
    by_id = dict(zip(out["gsis_id"], out["is_rookie"]))
    assert by_id["rook"] is True and by_id["vet"] is False


def test_only_the_pre_season_snapshot_is_read(nfl_root):
    """In-season the roster has many weeks, and this function answers the
    pre-season question regardless of which."""
    full_season(nfl_root, 2025, ["a"])
    write_rosters(nfl_root, 2026, [("a", 1, "SEA", "WR", 2021),
                                   ("a", 8, "LA", "WR", 2021)])
    out = ft.season_features(2026, [2025])
    assert out.height == 1
    assert out["team"][0] == "SEA"


def test_unmodelled_positions_are_excluded(nfl_root):
    """Kickers and team defences have no usage features, and plan 18 fixes
    abstention rather than a positional default."""
    full_season(nfl_root, 2025, ["wr"])
    write_rosters(nfl_root, 2026, [("wr", 1, "SEA", "WR", 2021),
                                   ("k", 1, "SEA", "K", 2021)])
    out = ft.season_features(2026, [2025])
    assert out["position"].to_list() == ["WR"]
    assert "K" not in ft.MODELLED_POSITIONS


def test_a_player_with_no_history_is_kept_with_null_features(nfl_root):
    """The board has to price him, so he cannot be dropped."""
    full_season(nfl_root, 2025, ["vet"])
    write_rosters(nfl_root, 2026, [("new", 1, "SEA", "WR", 2026)])
    out = ft.season_features(2026, [2025])
    assert out.height == 1
    assert out["p1_games"][0] is None


# --- expected production -------------------------------------------------

def test_expected_production_is_per_game(nfl_root):
    write_opportunity(nfl_root, 2025, [("a", w, 100.0, 90.0) for w in (1, 2, 3)])
    out = ft.expected_production([2025])
    row = out.row(0, named=True)
    assert row["opportunity_games"] == 3
    assert row["exp_receivingYards_pg"] == pytest.approx(90.0)
    assert row["act_receivingYards_pg"] == pytest.approx(100.0)


def test_a_missing_pull_names_the_command_that_fixes_it(nfl_root):
    with pytest.raises(FileNotFoundError, match="Rscript R/GetUsage.R"):
        ft.load_player_weeks([2026])


# --- snap share ----------------------------------------------------------

def test_snap_share_is_a_games_regressor_not_a_volume_denominator():
    """Where it measured, and where it did not.

    Predicting next season's games played it is worth +0.027 R-squared on the
    model's weakest arm. Used the other way -- to convert per-appearance rates into
    per-full-game-equivalent rates -- it was measured much *worse*: R-squared fell
    from 0.62 to 0.26 predicting next-season totals, because a part-time player's low
    per-appearance rate is his role rather than a distortion of it, and dividing it
    out removes the signal.
    """
    from Scripts.usage import season as sn
    assert "p1_snap_share" in sn.GAMES_REGRESSORS
    assert not any("snap" in r for r in sn.VOLUME_REGRESSORS)


def test_snap_share_is_absent_rather_than_fatal():
    """Snap counts are an in-season pull, so a pre-season build must survive without
    them -- the same way the blend survives a missing sportsbook."""
    empty = ft.snap_share([])
    assert empty.height == 0
    assert set(["season", "gsis_id", "snap_share"]).issubset(empty.columns)


def test_a_missing_snap_record_falls_back_to_availability():
    """A player with no snap record is not a player with no role. Dropping those rows
    would shrink the fit to the seasons the pfr crosswalk happens to cover well --
    measured at 66% overall, and only 99.8% once restricted to QB/RB/WR/TE."""
    from Scripts.usage import season as sn
    frame = pl.DataFrame({
        "position": ["WR", "WR"],
        "y_games": [10.0, 12.0],
        "y_games_available": [17.0, 17.0],
        f"{ft.LAG1_PREFIX}availability": [0.8, 0.9],
        f"{ft.LAG1_PREFIX}weeks_on_reserve": [0.0, 0.0],
        f"{ft.LAG1_PREFIX}snap_share": [None, 0.7],
        "team_changed": [False, False],
    })
    model = sn.SeasonUsageModel(
        volume={},
        games={"WR": sn.VolumeFit(
            position="WR", target="games", intercept=0.0,
            coefficients={"p1_availability": 0.0, "p1_weeks_on_reserve": 0.0,
                          "p1_snap_share": 1.0, "team_changed": 0.0},
            n=100, r2=0.2)},
        games_by_position={"WR": 14.0})
    # The null row falls back to its own availability of 0.8 rather than to zero.
    got = frame.select(model.expected_games(frame, target_slate=17.0)).to_series()
    assert got[0] == pytest.approx(0.8 * 17.0)
    assert got[1] == pytest.approx(0.7 * 17.0)


# --- age -----------------------------------------------------------------

def test_age_is_measured_against_the_opener_not_by_subtracting_years():
    """A January and a December birthday are most of a year apart, and the
    running-back decline is steep enough for that to matter."""
    from datetime import date
    import polars as pl
    january = date(2026, 9, 1) - date(2000, 1, 15)
    december = date(2026, 9, 1) - date(2000, 12, 15)
    assert (january.days - december.days) / ft.DAYS_IN_YEAR > 0.9
    assert january.days / ft.DAYS_IN_YEAR == pytest.approx(26.6, abs=0.1)


def test_age_is_a_regressor_on_both_heads():
    """It was measured to pay on each separately: volume WR 0.5462 -> 0.5645, and
    availability 0.1982 -> 0.2100 on top of snap share."""
    from Scripts.usage import season as sn
    assert "age" in sn.VOLUME_REGRESSORS
    assert "age" in sn.GAMES_REGRESSORS


def test_a_missing_age_column_disables_the_term_rather_than_emptying_the_fit():
    """The fits drop null regressor rows, so a null age would empty an entire volume
    fit instead of merely not using age -- which is what a frame built before this
    feature existed would produce."""
    import polars as pl
    from Scripts.usage import season as sn
    frame = pl.DataFrame({"position": ["WR", "WR"]})
    got = frame.select(sn.age_expr(frame).alias("age"))["age"].to_list()
    assert got == [sn.DEFAULT_AGE, sn.DEFAULT_AGE]


def test_a_missing_birth_date_falls_back_to_the_position_median():
    """Coverage is 98.6%, and the 1.4% are not a random sample -- they are the
    players nflverse knows least about, which skews to the fringe roster spots a
    board is least sure of. Dropping them would be a quiet selection effect."""
    import polars as pl
    from Scripts.usage import season as sn
    frame = pl.DataFrame({"position": ["RB", "RB", "RB"],
                          "age": [24.0, 28.0, None]})
    got = frame.select(sn.age_expr(frame).alias("age"))["age"].to_list()
    assert got[2] == pytest.approx(26.0)


def test_age_never_falls_back_to_zero():
    """Zero is not a neutral age the way a zero team-change flag is neutral -- it
    would put every unknown player at the far end of the decline curve."""
    import polars as pl
    from Scripts.usage import season as sn
    frame = pl.DataFrame({"position": ["RB"], "age": [None]}).with_columns(
        pl.col("age").cast(pl.Float64))
    assert frame.select(sn.age_expr(frame).alias("age"))["age"][0] > 20.0
