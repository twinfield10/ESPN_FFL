"""The season usage head: decomposition, abstention, and the backtest's metrics.

Two classes of thing are pinned here. The model's own guarantees -- that it emits
stat lines rather than points, that it says nothing where it knows nothing, that
volume and efficiency stay separable -- and the *evaluation's* guarantees, which
matter just as much: a backtest metric that is subtly wrong is worse than no metric,
because it will be believed. Both bugs that shipped in the first draft of this were
in the evaluation, not the model.

Synthetic frames throughout. No network, no parquet.
"""

import numpy as np
import polars as pl
import pytest

from Scripts.usage import backtest as bt
from Scripts.usage import features as ft
from Scripts.usage import season as sn


def feature_rows(rows):
    """A feature frame in the shape ``season_features`` returns.

    Args:
        rows: dicts of overrides, merged onto a plausible receiver's season.
    """
    default = {
        "gsis_id": "a", "position": "WR", "team": "SEA", "team_changed": False,
        "is_rookie": False, "season": 2026,
        "p1_games": 16, "p1_weeks_on_reserve": 0, "p1_availability": 1.0,
        "p1_targets_pg": 8.0, "p2_targets_pg": 7.0,
        "p1_carries_pg": 0.0, "p2_carries_pg": 0.0,
        "p1_pass_attempts_pg": 0.0, "p2_pass_attempts_pg": 0.0,
        "p1_yards_per_target": 8.5, "p1_catch_rate": 0.65,
        "p1_rec_td_per_target": 0.06, "p1_yards_per_carry": 4.2,
        "p1_rush_td_per_carry": 0.02, "p1_yards_per_attempt": 7.0,
        "p1_pass_td_per_attempt": 0.05, "p1_int_per_attempt": 0.02,
    }
    return pl.DataFrame([{**default, **row} for row in rows])


def trained_model(**overrides):
    """A hand-specified model, so predictions are checkable by arithmetic."""
    volume = {
        ("WR", "targets_pg"): sn.VolumeFit(
            position="WR", target="targets_pg", intercept=0.0,
            coefficients={"p1_volume": 1.0, "p2_volume": 0.0,
                          "p1_games": 0.0, "team_changed": 0.0},
            n=100, r2=0.5),
    }
    games = {
        "WR": sn.VolumeFit(
            position="WR", target="games", intercept=0.0,
            coefficients={"p1_availability": 1.0, "p1_weeks_on_reserve": 0.0,
                          "team_changed": 0.0},
            n=100, r2=0.2),
    }
    kwargs = {"volume": volume, "games": games,
              "games_by_position": {"WR": 14.0, "QB": 15.0},
              "train_seasons": (2024, 2025)}
    kwargs.update(overrides)
    return sn.SeasonUsageModel(**kwargs)


# --- the decomposition ---------------------------------------------------

def test_every_modelled_stat_is_a_volume_times_a_rate():
    """The design: no stat is predicted in one step."""
    for stat, (volume, rate) in sn.STAT_TERMS.items():
        assert volume in ft.VOLUME_STATS.values() or volume.endswith("_pg")
        assert rate in dict((n, d) for n, _, d in
                            [(n, num, den) for n, num, den in ft.EFFICIENCY_RATES])


def test_a_prediction_is_games_times_volume_times_rate():
    """Expected games is a share of the slate: availability 1.0 x 17 games."""
    frame = feature_rows([{"p1_availability": 1.0, "p1_targets_pg": 8.0,
                           "p1_yards_per_target": 8.5}])
    out = trained_model().predict(frame)
    assert out["expected_games"][0] == pytest.approx(17.0)
    assert out["pred_targets_pg"][0] == pytest.approx(8.0)
    assert out["USG_receivingYards"][0] == pytest.approx(17 * 8.0 * 8.5)


def test_receptions_and_yards_share_one_volume_term():
    """Both come off targets, which is what makes the two internally consistent."""
    frame = feature_rows([{}])
    out = trained_model().predict(frame)
    ratio = out["USG_receivingReceptions"][0] / out["USG_receivingYards"][0]
    assert ratio == pytest.approx(0.65 / 8.5)


def test_it_emits_stat_lines_not_points():
    """A points model could not serve nine leagues with different scoring."""
    out = trained_model().predict(feature_rows([{}]))
    assert any(c.startswith(sn.USAGE_PREFIX) for c in out.columns)
    assert not any(c.endswith("_Points") for c in out.columns)


# --- abstention ----------------------------------------------------------

def test_a_player_with_no_prior_season_gets_nothing():
    """Every rookie, which is plan 18's stated v1: the draft-capital arm ships only
    if it beats abstention on the walk-forward."""
    frame = feature_rows([{"p1_games": None, "is_rookie": True}])
    out = trained_model().predict(frame)
    for stat in sn.STAT_TERMS:
        assert out[f"{sn.USAGE_PREFIX}{stat}"][0] is None


def test_a_receiver_gets_no_passing_line():
    """Step 0's baseline projected 38 passing yards for every wide receiver in the
    league, off an intercept applied to players with no attempts."""
    out = trained_model().predict(feature_rows([{"p1_pass_attempts_pg": 0.0}]))
    assert out["USG_passingYards"][0] is None
    assert out["USG_receivingYards"][0] is not None


def test_a_position_with_no_fit_abstains_on_that_volume():
    out = trained_model().predict(feature_rows([{"position": "QB"}]))
    assert out["pred_targets_pg"][0] is None
    assert out["USG_receivingYards"][0] is None


def test_a_negative_prediction_is_clipped_not_emitted():
    """The blend would price a negative stat line."""
    model = trained_model(volume={
        ("WR", "targets_pg"): sn.VolumeFit(
            position="WR", target="targets_pg", intercept=-50.0,
            coefficients={"p1_volume": 0.0, "p2_volume": 0.0,
                          "p1_games": 0.0, "team_changed": 0.0},
            n=100, r2=0.1)})
    out = model.predict(feature_rows([{}]))
    assert out["pred_targets_pg"][0] == pytest.approx(0.0)


def test_kickers_and_defences_are_outside_the_modelled_positions():
    assert "K" not in ft.MODELLED_POSITIONS
    assert "D/ST" not in ft.MODELLED_POSITIONS


# --- expected games ------------------------------------------------------

def test_expected_games_cannot_exceed_the_slate():
    """The share is clipped to 1.0 before it is multiplied up, so the bound holds
    whatever slate is passed rather than only at the hardcoded 18."""
    model = trained_model(games={
        "WR": sn.VolumeFit(position="WR", target="games", intercept=40.0,
                           coefficients={"p1_availability": 0.0,
                                         "p1_weeks_on_reserve": 0.0,
                                         "team_changed": 0.0},
                           n=100, r2=0.1)})
    out = model.predict(feature_rows([{}]))
    assert out["expected_games"][0] == pytest.approx(sn.DEFAULT_TARGET_SLATE)
    out16 = model.predict(feature_rows([{}]), target_slate=16.0)
    assert out16["expected_games"][0] == pytest.approx(16.0)


def test_expected_games_cannot_be_negative():
    model = trained_model(games={
        "WR": sn.VolumeFit(position="WR", target="games", intercept=-5.0,
                           coefficients={"p1_availability": 0.0,
                                         "p1_weeks_on_reserve": 0.0,
                                         "team_changed": 0.0},
                           n=100, r2=0.1)})
    out = model.predict(feature_rows([{}]))
    assert out["expected_games"][0] == pytest.approx(0.0)


def test_a_position_with_no_games_fit_falls_back_to_its_mean():
    out = trained_model().predict(feature_rows([{"position": "QB",
                                                 "p1_pass_attempts_pg": 30.0}]))
    assert out["expected_games"][0] == pytest.approx(15.0)


def test_availability_and_raw_games_are_never_both_regressors():
    """Availability is games_played / games_available, so the two are collinear.
    Fitting both gave RB +1.056 on games against -10.160 on availability --
    offsetting coefficients that cannot be read and will not transfer.

    v1.1.0 swapped which one is used rather than adding the second: the head works
    in share of slate, so `p1_availability` is in and `p1_games` is out. The
    invariant that mattered was never "use games", it was "never use both".
    """
    assert "p1_availability" in sn.GAMES_REGRESSORS
    assert "p1_games" not in sn.GAMES_REGRESSORS
    assert "p1_weeks_on_reserve" in sn.GAMES_REGRESSORS


def test_the_games_head_is_scale_free():
    """The point of the share form: a 16-game season and a 17-game season describe
    the same player identically. Fitted in raw games they did not, and 45% of the
    training range predates the 17-game slate."""
    full16 = feature_rows([{"p1_games": 16, "p1_availability": 1.0}])
    full17 = feature_rows([{"p1_games": 17, "p1_availability": 1.0}])
    model = trained_model()
    assert (model.predict(full16)["expected_games"][0]
            == pytest.approx(model.predict(full17)["expected_games"][0]))


def test_the_projected_slate_scales_the_answer():
    frame = feature_rows([{"p1_availability": 1.0}])
    model = trained_model()
    at17 = model.predict(frame, target_slate=17.0)["expected_games"][0]
    at16 = model.predict(frame, target_slate=16.0)["expected_games"][0]
    assert at17 / at16 == pytest.approx(17.0 / 16.0)


# --- fitting -------------------------------------------------------------

def training_rows(n=200, seed=0):
    """Rows where targets carry forward with noise, so a fit should find them.

    ``p2`` is independent of ``p1`` here on purpose. In the real data they correlate
    strongly, which makes the individual coefficients partly arbitrary while the
    prediction stays sound -- the fitted WR target model reports p1 0.709 and p2
    0.051, a pair that has to be read together. A fixture with ``p2 = 0.9 * p1``
    reproduces that and cannot identify either slope, which is how this test first
    "failed" at 0.44.
    """
    rng = np.random.default_rng(seed)
    p1 = rng.uniform(1.0, 10.0, n)
    return pl.DataFrame({
        "gsis_id": [f"p{i}" for i in range(n)],
        "position": ["WR"] * n,
        "team_changed": [False] * n,
        "p1_targets_pg": p1,
        "p2_targets_pg": rng.uniform(1.0, 10.0, n),
        "p1_carries_pg": np.zeros(n),
        "p2_carries_pg": np.zeros(n),
        "p1_pass_attempts_pg": np.zeros(n),
        "p2_pass_attempts_pg": np.zeros(n),
        "p1_games": rng.integers(4, 17, n),
        "p1_weeks_on_reserve": np.zeros(n),
        "y_targets_pg": 0.8 * p1 + 1.0 + rng.normal(0, 0.3, n),
        "y_games": rng.integers(4, 17, n).astype(float),
    })


def test_a_volume_fit_recovers_a_known_slope():
    fit = sn._fit_volume(training_rows(), "WR", "targets_pg")
    assert fit is not None
    assert fit.coefficients["p1_volume"] == pytest.approx(0.8, abs=0.15)
    assert fit.r2 > 0.8


def test_structural_zeros_are_excluded_from_the_fit():
    """Every quarterback's target line is a perfectly predicted zero. Left in, they
    dominate the fit, flatter the R-squared and pull the slope toward zero.
    """
    rows = training_rows(n=60)
    zeros = rows.with_columns(
        pl.lit(0.0).alias("p1_targets_pg"), pl.lit(0.0).alias("y_targets_pg"),
        pl.Series("gsis_id", [f"z{i}" for i in range(rows.height)]))
    fit = sn._fit_volume(pl.concat([rows, zeros]), "WR", "targets_pg")
    assert fit.n == 60          # the zero rows were not fitted on
    assert fit.coefficients["p1_volume"] == pytest.approx(0.8, abs=0.2)


def test_too_few_rows_yields_no_fit_rather_than_a_bad_one():
    assert sn._fit_volume(training_rows(n=5), "WR", "targets_pg") is None


def test_a_fitted_model_records_its_training_range_and_version():
    """`CLAUDE.md`: save models with metadata -- version, date, training range."""
    model = sn.fit(training_rows(), [2023, 2024], fitted_at="2026-08-07T12:00:00")
    payload = model.to_dict()
    assert payload["train_seasons"] == [2023, 2024]
    assert payload["version"] == sn.MODEL_VERSION
    assert payload["fitted_at"] == "2026-08-07T12:00:00"
    assert payload["volume_fits"]


def test_a_model_round_trips_to_json(tmp_path):
    model = sn.fit(training_rows(), [2024])
    path = model.save(tmp_path / "m.json")
    assert path.is_file()
    import json
    assert json.loads(path.read_text())["train_seasons"] == [2024]


# --- the backtest's own correctness -------------------------------------

def scored_rows(season, ids, predicted, actual):
    return pl.DataFrame({
        "test_season": [season] * len(ids),
        "gsis_id": list(ids),
        "position": ["QB"] * len(ids),
        "usg_points": [float(p) for p in predicted],
        "actual_points": [float(a) for a in actual],
    })


def test_top_n_is_computed_per_season_not_over_the_pool():
    """Pooling first compares a 2019 quarterback against a 2024 one, so the number
    moves with the scoring era rather than with the model. Constructed so pooling
    and per-season disagree: each season is ranked perfectly, but season 2's points
    are all below season 1's.
    """
    frame = pl.concat([
        scored_rows(2019, ["a", "b"], [100, 90], [100, 90]),
        scored_rows(2020, ["c", "d"], [10, 5], [10, 5]),
    ])
    # Per season, the top-1 is correct in both -> 1.0. Pooled, the top-1 of four is
    # 'a' either way, which also scores 1.0 -- so use n=2 where they diverge.
    per_season = bt.top_n_hit_rate(frame, "usg_points", "actual_points", 2)
    assert per_season == pytest.approx(1.0)
    # 'c' and 'd' can never enter a pooled top 2, so a pooled metric would be blind
    # to how season 2 was ranked at all.
    assert frame.filter(pl.col("test_season") == 2020).height == 2


def test_top_n_skips_a_season_without_enough_players():
    frame = scored_rows(2019, ["a"], [1], [1])
    assert bt.top_n_hit_rate(frame, "usg_points", "actual_points", 12) is None


def test_points_treats_an_absent_stat_as_zero_not_null():
    """A receiver has no passing line, and his receiving points still have to add."""
    frame = pl.DataFrame({"a": [10.0], "b": [None]}, schema={"a": pl.Float64,
                                                            "b": pl.Float64})
    out = frame.select(bt.points(frame, {"receivingYards": "a",
                                         "passingYards": "b"},
                                {"receivingYards": 0.1, "passingYards": 0.04}))
    assert out.item() == pytest.approx(1.0)


def test_spearman_declines_to_answer_on_too_few_rows():
    frame = pl.DataFrame({"a": [1.0, 2.0], "b": [1.0, 2.0]})
    assert bt.spearman(frame, "a", "b") is None


def test_spearman_is_rank_based_not_level_based():
    frame = pl.DataFrame({"a": [float(i) for i in range(20)],
                          "b": [float(i) ** 3 for i in range(20)]})
    assert bt.spearman(frame, "a", "b") == pytest.approx(1.0)


def test_the_scoring_league_has_a_full_registry_history():
    """Every other league was only recorded from 2023 or 2024, so picking one of
    those would silently shorten the walk-forward."""
    from Scripts.paths import DATA_DIR
    path = DATA_DIR / "Scoring" / "scoring.csv"
    if not path.is_file():
        pytest.skip("no scoring registry on disk")
    registry = pl.read_csv(path)
    seasons = set(
        registry.filter(pl.col("league_key") == bt.SCORING_LEAGUE)["season"]
        .unique().to_list())
    assert set(bt.DEFAULT_TEST_SEASONS) <= seasons


# --- the rookie arm ------------------------------------------------------

def rookie_rows(n=120, seed=1, position="WR"):
    """Rookie training rows where volume decays with log(pick).

    Two thirds undrafted, matching the real split (1,338 of 2,008 rookie
    player-seasons 2017-2025), because that population is most of what the arm has
    to get right.
    """
    rng = np.random.default_rng(seed)
    drafted = n // 3
    picks = list(rng.integers(1, 257, drafted)) + [None] * (n - drafted)
    volume = [max(0.0, 12.0 - 2.0 * np.log(p) + rng.normal(0, 0.5))
              if p is not None else max(0.0, rng.normal(0.2, 0.2))
              for p in picks]
    return pl.DataFrame({
        "gsis_id": [f"r{i}" for i in range(n)],
        "position": [position] * n,
        "is_rookie": [True] * n,
        "team_changed": [None] * n,
        "draft_number": picks,
        "p1_games": [None] * n,
        "p1_targets_pg": [None] * n,
        "p2_targets_pg": [None] * n,
        "y_targets_pg": volume,
        "y_games": [12.0 if p is not None else 1.0 for p in picks],
        "y_games_available": [17.0] * n,
        "y_tot_receiving_yards": [v * 100 for v in volume],
        "y_tot_targets": [v * 14 for v in volume],
        "y_tot_receptions": [v * 9 for v in volume],
    }, schema_overrides={"draft_number": pl.Int32})


def test_the_rookie_arm_recovers_the_draft_position_slope():
    fit = sn._fit_rookie_volume(rookie_rows(), "WR", "targets_pg")
    assert fit is not None
    assert fit.coefficients["log_pick"] < 0     # later pick, less volume
    assert fit.r2 > 0.5


def test_the_rookie_arm_fits_undrafted_players_too():
    """Two of three rookies go undrafted and 79% of those never take a snap. A fit
    that excluded them would project every undrafted free agent as a contributor.
    """
    fit = sn._fit_rookie_volume(rookie_rows(n=120), "WR", "targets_pg")
    assert fit.n == 120
    assert fit.coefficients["undrafted"] < 0


def test_undrafted_is_not_modelled_as_a_late_pick():
    """It is a different population, not pick 300 -- so it gets its own indicator
    with log_pick held at zero."""
    assert {"log_pick", "undrafted"} <= set(sn.ROOKIE_REGRESSORS)
    frame = pl.DataFrame({"draft_number": [None, 32]},
                         schema={"draft_number": pl.Int32})
    terms = sn.SeasonUsageModel._rookie_terms(frame)
    out = frame.select(terms["log_pick"].alias("lp"),
                       terms["undrafted"].alias("ud"))
    assert out["lp"].to_list() == [0.0, pytest.approx(np.log(32))]
    assert out["ud"].to_list() == [1.0, 0.0]


def test_a_rookie_row_is_projected_by_the_rookie_arm():
    train = rookie_rows()
    model = sn.fit(train, [2025])
    frame = feature_rows([{"is_rookie": True, "p1_games": None,
                           "p1_targets_pg": None, "draft_number": 10,
                           "p1_yards_per_target": None}])
    out = model.predict(frame)
    assert out["usg_arm"][0] == "rookie"
    assert out["USG_receivingYards"][0] is not None


def test_an_earlier_pick_is_projected_higher():
    model = sn.fit(rookie_rows(), [2025])
    frame = feature_rows([
        {"gsis_id": "early", "is_rookie": True, "p1_games": None,
         "draft_number": 5, "p1_yards_per_target": None},
        {"gsis_id": "late", "is_rookie": True, "p1_games": None,
         "draft_number": 220, "p1_yards_per_target": None},
    ])
    out = model.predict(frame).sort("gsis_id")
    values = dict(zip(out["gsis_id"], out["USG_receivingYards"]))
    assert values["early"] > values["late"]


def test_rookies_may_be_abstained_on_for_the_comparison():
    """Plan 18's test is arm against abstention, so abstention has to be runnable."""
    model = sn.fit(rookie_rows(), [2025])
    frame = feature_rows([{"is_rookie": True, "p1_games": None,
                           "draft_number": 10, "p1_yards_per_target": None}])
    out = model.predict(frame, rookies=False)
    assert out["usg_arm"][0] == "abstain"
    assert out["USG_receivingYards"][0] is None


def test_prior_history_wins_over_the_rookie_flag():
    """A "rookie" with a prior season is a data problem, not a rookie -- and both
    arms must not claim the same row."""
    model = trained_model(rookie_volume={
        ("WR", "targets_pg"): sn.VolumeFit(
            position="WR", target="targets_pg", intercept=99.0,
            coefficients={"log_pick": 0.0, "undrafted": 0.0}, n=100, r2=0.5)})
    frame = feature_rows([{"is_rookie": True, "p1_games": 16, "draft_number": 5}])
    out = model.predict(frame)
    assert out["usg_arm"][0] == "veteran"


def test_rookies_use_a_rookie_efficiency_baseline_not_the_pools():
    """A rookie is less efficient per opportunity than an established player."""
    rates = sn.rookie_efficiency(rookie_rows(), positions=["WR"])
    assert "yards_per_target" in rates["WR"]
    assert rates["WR"]["yards_per_target"] == pytest.approx(100 / 14, rel=0.01)


def test_a_rate_built_on_almost_no_opportunity_is_not_recorded():
    """One rookie back's single intercepted pass gave RB an int_per_attempt of
    0.200. Unreachable in practice, and still wrong to record."""
    # 60 rows at 0.5 targets each pools to 30, under the 50-opportunity floor.
    frame = rookie_rows(n=60).with_columns(pl.lit(0.5).alias("y_tot_targets"))
    rates = sn.rookie_efficiency(frame, positions=["WR"])
    assert "yards_per_target" not in rates.get("WR", {})
    assert "catch_rate" not in rates.get("WR", {})


def test_too_few_rookie_rows_yields_no_arm():
    assert sn._fit_rookie_volume(rookie_rows(n=10), "WR", "targets_pg") is None


# --- the relevance gate --------------------------------------------------

def volume_frame(position, target, mean):
    return pl.DataFrame({
        "position": [position] * 50,
        f"y_{target}": [mean] * 50,
    })


def test_a_position_that_barely_uses_a_volume_is_not_modelled_for_it():
    """WR pass attempts average 0.007 a game across the training seasons. A
    regression on 111 trick plays returned a positive intercept, so 315 of 389
    receivers were handed a passing line with a median of 2.12 yards."""
    assert not sn.models_volume(
        volume_frame("WR", "pass_attempts_pg", 0.007), "WR", "pass_attempts_pg")
    assert not sn.models_volume(
        volume_frame("TE", "carries_pg", 0.012), "TE", "carries_pg")


def test_a_volume_the_position_really_gets_is_modelled():
    assert sn.models_volume(
        volume_frame("QB", "carries_pg", 1.781), "QB", "carries_pg")
    assert sn.models_volume(
        volume_frame("RB", "targets_pg", 1.197), "RB", "targets_pg")


def test_the_gate_removes_the_cross_position_fits_from_a_real_fit():
    """Six real (position, volume) pairs survive, six junk ones do not."""
    rows = []
    means = {("QB", "targets_pg"): 0.015, ("QB", "carries_pg"): 1.781,
             ("QB", "pass_attempts_pg"): 14.3, ("RB", "targets_pg"): 1.197,
             ("RB", "carries_pg"): 4.026, ("RB", "pass_attempts_pg"): 0.002,
             ("WR", "targets_pg"): 2.102, ("WR", "carries_pg"): 0.083,
             ("WR", "pass_attempts_pg"): 0.007, ("TE", "targets_pg"): 1.513,
             ("TE", "carries_pg"): 0.012, ("TE", "pass_attempts_pg"): 0.001}
    for position in ("QB", "RB", "WR", "TE"):
        for _ in range(60):
            row = {"position": position}
            for target in sn.VOLUME_TARGETS:
                row[f"y_{target}"] = means[(position, target)]
            rows.append(row)
    frame = pl.DataFrame(rows)
    gated = {(p, t) for p in ("QB", "RB", "WR", "TE") for t in sn.VOLUME_TARGETS
             if sn.models_volume(frame, p, t)}
    assert gated == {("QB", "carries_pg"), ("QB", "pass_attempts_pg"),
                     ("RB", "targets_pg"), ("RB", "carries_pg"),
                     ("WR", "targets_pg"), ("TE", "targets_pg")}


# --- persistence ---------------------------------------------------------

def test_load_round_trips_save(tmp_path):
    """Coefficients persisted but nothing read them back, so every caller wanting a
    projection had to refit -- and the board and the backtest could silently end up
    built from different coefficients."""
    model = trained_model()
    path = model.save(tmp_path / "m.json")
    back = sn.SeasonUsageModel.load(path)

    assert back.version == model.version
    assert back.train_seasons == model.train_seasons
    assert back.games_by_position == model.games_by_position
    assert set(back.volume) == set(model.volume)
    fit, original = back.volume[("WR", "targets_pg")], model.volume[("WR", "targets_pg")]
    assert fit.intercept == original.intercept
    assert fit.coefficients == original.coefficients


def test_a_loaded_model_predicts_identically(tmp_path):
    """The point of loading is to skip the refit, so it has to give the same answer."""
    model = trained_model()
    back = sn.SeasonUsageModel.load(model.save(tmp_path / "m.json"))
    frame = feature_rows([{}])
    assert (back.predict(frame)["USG_receivingYards"][0]
            == pytest.approx(model.predict(frame)["USG_receivingYards"][0]))


# --- the declined position -----------------------------------------------

def test_no_position_is_declined_any_more():
    """Quarterback was, on measurement -- the model made QB ordering worse than the
    naive heuristic by 0.0155 Spearman. The deficit closed as the model improved and
    the depth chart closed it decisively (+0.0132), so the tuple is empty.

    The mechanism is kept, not deleted: it is how a position gets declined on
    evidence, and the next arm that measures worse should use it."""
    assert sn.ABSTAIN_POSITIONS == ()
    out = trained_model().predict(feature_rows([
        {"position": "QB", "p1_pass_attempts_pg": 30.0}]))
    assert out["usg_arm"][0] != "abstain"


def test_a_declined_position_can_still_be_measured():
    """`ABSTAIN_POSITIONS` exists because of a row in the backtest's table. If the
    backtest honoured it, the evidence for it would erase itself."""
    out = trained_model().predict(
        feature_rows([{"position": "QB", "p1_pass_attempts_pg": 30.0}]),
        abstain_positions=())
    assert out["usg_arm"][0] == "veteran"


def test_declining_a_position_leaves_the_others_alone():
    """The mechanism still works when asked, which is what keeps it usable."""
    out = trained_model().predict(
        feature_rows([{"position": "QB"}, {"position": "WR"}]),
        abstain_positions=("QB",))
    assert out["usg_arm"].to_list() == ["abstain", "veteran"]
    assert out["USG_receivingYards"][0] is None
    assert out["USG_receivingYards"][1] is not None


# --- the slate normalisation (v1.1.0) ------------------------------------

def test_a_rookie_who_never_played_counts_as_zero_not_as_missing():
    """Caught while fixing the era bias, and it is the trap that version of the
    change walked into.

    The bins are a mean over *every* rookie, and 78.8% of undrafted rookies never
    appear. Those players have no outcome row and therefore no measured slate, so a
    share fit that requires a denominator drops them -- which silently reweights the
    bin onto the minority who played. It took the undrafted bin from 1.1 games to
    5.8, enough to project a camp body as a third of a season, and nothing about the
    fitted table looked wrong."""
    # The never-played rows carry a *null* slate, which is how they really arrive:
    # no outcome row means no measured denominator either.
    played = {"y_games": 12.0, "y_games_available": 17.0}
    never = {"y_games": 0.0, "y_games_available": None}
    rows = pl.DataFrame([
        {"gsis_id": f"d{i}", "position": "WR", "is_rookie": True,
         "draft_number": 10, "team_changed": None, "p1_games": None, **played}
        for i in range(10)
    ] + [
        {"gsis_id": f"u{i}", "position": "WR", "is_rookie": True,
         "draft_number": None, "team_changed": None, "p1_games": None, **never}
        for i in range(20)
    ], schema_overrides={"draft_number": pl.Int32})

    bins = sn._fit_rookie_games(rows, "WR")
    undrafted = bins.get(sn.bin_label(None))
    assert undrafted is not None, "the undrafted bin must exist, not be dropped"
    assert undrafted == pytest.approx(0.0), (
        "a rookie who never played is a zero, not an absent observation")


def test_the_share_is_stored_not_the_games():
    """`rookie_games` and the games fits are on a 0-1 scale, so a reader who assumes
    games gets an obviously wrong number rather than a subtly wrong one."""
    rows = pl.DataFrame([
        {"gsis_id": f"d{i}", "position": "WR", "is_rookie": True,
         "draft_number": 10, "team_changed": None, "p1_games": None,
         "y_games": 17.0, "y_games_available": 17.0}
        for i in range(sn.MIN_ROOKIE_FIT_ROWS)
    ], schema_overrides={"draft_number": pl.Int32})
    bins = sn._fit_rookie_games(rows, "WR")
    assert all(0.0 <= v <= 1.0 for v in bins.values())
    assert bins[sn.bin_label((1, 32))] == pytest.approx(1.0)
