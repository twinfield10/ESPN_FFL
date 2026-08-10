"""The plan 22 feature layer: advanced pulls, fitted rate baselines, contracts.

Three things are worth pinning here, and they are the three that broke while this
was being built rather than three that seemed likely to.

**NaN is not null.** Polars treats them as different and ``is_not_null()`` is True
for a NaN, so an NGS column carrying one reaches numpy intact. The first run of the
efficiency experiment failed on ``SVD did not converge``, and the run after that
returned NaN for every receiving MAE -- which looked like a result rather than a
bug. This repo has now paid for that distinction three times.

**A fitted baseline may never withdraw coverage.** It is an optional better prior;
a player whose advanced features are missing has to come out exactly where he came
out before the feature existed, or the model's coverage starts depending on NGS's
qualifying threshold.

**The contract boundary.** ``year_signed <= target_season`` admits March free
agency, which is the point, and a mid-season extension, which is the documented
cost. What it must never admit is a deal signed the following year.

Synthetic parquet in ``tmp_path``. No network.
"""

import numpy as np
import polars as pl
import pytest

from Scripts import paths
from Scripts.usage import features as ft
from Scripts.usage import nflverse as nv


@pytest.fixture
def nfl_root(tmp_path, monkeypatch):
    """Redirect ``Data/NFL`` to ``tmp_path``, for both path helpers."""
    monkeypatch.setattr(paths, "DATA_DIR", tmp_path / "Data")
    monkeypatch.setattr(nv, "CONTRACTS_PARQUET",
                        tmp_path / "Data" / "NFL" / "contracts.parquet")
    root = tmp_path / "Data" / "NFL"
    root.mkdir(parents=True, exist_ok=True)
    return root


def write_advanced(nfl_root, season, name, frame: dict):
    """Write one of the advanced player-week parquets."""
    directory = nfl_root / str(season)
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(frame).write_parquet(directory / f"{name}.parquet")


def write_contracts(nfl_root, rows):
    """From ``(gsis_id, year_signed, apy_cap_pct, guaranteed, years)``."""
    pl.DataFrame({
        "gsis_id": [r[0] for r in rows],
        "player": [f"Player {r[0]}" for r in rows],
        "position": ["WR"] * len(rows),
        "team": ["SEA"] * len(rows),
        "year_signed": [r[1] for r in rows],
        "years": [float(r[4]) for r in rows],
        "value": [0.0] * len(rows),
        "apy": [0.0] * len(rows),
        "guaranteed": [float(r[3]) for r in rows],
        "apy_cap_pct": [float(r[2]) for r in rows],
    }).write_parquet(nfl_root / "contracts.parquet")


# --- absent pulls are not errors -----------------------------------------

def test_advanced_totals_is_empty_rather_than_raising(nfl_root):
    """The model has to stay runnable for anyone who has not run GetAdvanced.R."""
    out = ft.advanced_totals([2024, 2025])
    assert out.height == 0
    assert set(out.columns) == {"season", "gsis_id"}


def test_load_advanced_skips_seasons_it_does_not_have(nfl_root):
    write_advanced(nfl_root, 2025, "routes", {
        "season": [2025], "week": [1], "gsis_id": ["a"], "posteam": ["SEA"],
        "routes": [30], "team_dropbacks": [40], "route_share": [0.75]})
    out = nv.load_advanced([2024, 2025], "routes")
    assert out.height == 1 and out["season"].to_list() == [2025]


def test_load_advanced_rejects_an_unknown_pull(nfl_root):
    with pytest.raises(KeyError, match="Unknown advanced pull"):
        nv.load_advanced([2025], "not_a_pull")


# --- NaN is not null -----------------------------------------------------

def test_advanced_totals_converts_nan_to_null(nfl_root):
    """The trap that made an efficiency bug look like an efficiency result.

    NGS ships NaN for a player-week it could not measure. Polars' ``is_not_null()``
    is True for a NaN, so every guard written the obvious way lets one through and
    it propagates silently through the shrinkage arithmetic.
    """
    write_advanced(nfl_root, 2025, "ngs", {
        "season": [2025, 2025], "week": [1, 2], "gsis_id": ["a", "a"],
        "ngs_adot": [float("nan"), float("nan")],
        "ngs_separation": [2.5, 3.5]})
    out = ft.advanced_totals([2025])
    assert out["ngs_adot"].is_null().all()
    assert not out["ngs_adot"].is_nan().any()
    assert out["ngs_separation"].to_list() == [3.0]


def test_fit_rate_baselines_survives_a_degenerate_column(nfl_root):
    """A constant regressor is rank-deficient; skip the fit rather than record it."""
    totals = pl.DataFrame({
        "season": [2024] * 80, "gsis_id": [str(i) for i in range(80)],
        "position": ["WR"] * 80,
        "tot_receiving_yards": np.linspace(300, 1200, 80).tolist(),
        "tot_targets": np.linspace(40, 140, 80).tolist(),
        # Every regressor identical: nothing to fit.
        "ngs_adot": [9.0] * 80, "ngs_separation": [3.0] * 80,
        "ngs_cushion": [6.0] * 80,
    })
    baselines = pl.DataFrame({"position": ["WR"], "catch_rate": [0.62],
                              "yards_per_target": [8.0]})
    fits = ft.fit_rate_baselines(totals, baselines)
    assert ("WR", "yards_per_target") not in fits


# --- a fitted baseline may never withdraw coverage -----------------------

def test_missing_features_fall_back_to_the_positional_constant(nfl_root):
    """A player NGS never measured must land exactly where he did before."""
    totals = pl.DataFrame({
        "season": [2024, 2024], "gsis_id": ["covered", "absent"],
        "position": ["WR", "WR"],
        "tot_receiving_yards": [800.0, 800.0],
        "tot_receptions": [60.0, 60.0],
        "tot_targets": [100.0, 100.0],
        "ngs_adot": [12.0, None], "ngs_separation": [3.0, None],
        "ngs_cushion": [6.0, None], "ngs_yac_oe": [0.5, None],
        "ngs_air_yards_share": [25.0, None],
    })
    baselines = pl.DataFrame({"position": ["WR"], "yards_per_target": [7.5],
                              "catch_rate": [0.62]})
    fit = {("WR", "yards_per_target"): {
        "intercept": 4.0, "n": 100.0, "r2": 0.2,
        "ngs_adot": 0.15, "ngs_separation": 0.6, "ngs_yac_oe": 0.4,
        "ngs_air_yards_share": 0.04}}

    plain = ft.attach_efficiency(totals, baselines)
    fitted = ft.attach_efficiency(totals, baselines, rate_baselines=fit)

    rows = {r["gsis_id"]: r for r in fitted.to_dicts()}
    plain_rows = {r["gsis_id"]: r for r in plain.to_dicts()}

    assert rows["absent"]["yards_per_target"] == pytest.approx(
        plain_rows["absent"]["yards_per_target"])
    assert rows["absent"]["yards_per_target_prior"] == pytest.approx(7.5)
    # And the covered player did move, or the test proves nothing.
    assert rows["covered"]["yards_per_target"] != pytest.approx(
        plain_rows["covered"]["yards_per_target"])


def test_the_fitted_prior_is_clipped_into_a_sane_range(nfl_root):
    """A linear fit on a bounded quantity will extrapolate absurdly eventually."""
    totals = pl.DataFrame({
        "season": [2024], "gsis_id": ["a"], "position": ["WR"],
        "tot_receptions": [1.0], "tot_targets": [1.0],
        "ngs_adot": [-500.0], "ngs_separation": [0.0], "ngs_cushion": [0.0],
    })
    baselines = pl.DataFrame({"position": ["WR"], "catch_rate": [0.62]})
    fit = {("WR", "catch_rate"): {"intercept": 0.7, "n": 100.0, "r2": 0.2,
                                  "ngs_adot": 0.01, "ngs_separation": 0.0,
                                  "ngs_cushion": 0.0}}
    out = ft.attach_efficiency(totals, baselines, rate_baselines=fit)
    assert out["catch_rate_prior"][0] >= 0.0
    assert out["catch_rate"][0] >= 0.0


# --- the contract boundary -----------------------------------------------

def test_contract_context_takes_the_latest_deal_signed_by_the_season(nfl_root):
    write_contracts(nfl_root, [
        ("a", 2022, 0.04, 10_000_000.0, 4),
        ("a", 2026, 0.14, 69_000_000.0, 4),
    ])
    out = ft.contract_context(2026)
    row = out.filter(pl.col("gsis_id") == "a").to_dicts()[0]
    assert row["contract_apy_pct"] == pytest.approx(0.14)
    assert row["contract_is_new"] is True
    assert row["contract_age"] == pytest.approx(0.0)


def test_contract_context_never_sees_a_later_signing(nfl_root):
    """March free agency is knowable in August. Next March's is not."""
    write_contracts(nfl_root, [
        ("a", 2024, 0.04, 10_000_000.0, 4),
        ("a", 2027, 0.20, 90_000_000.0, 5),
    ])
    row = ft.contract_context(2026).filter(pl.col("gsis_id") == "a").to_dicts()[0]
    assert row["contract_apy_pct"] == pytest.approx(0.04)
    assert row["contract_age"] == pytest.approx(2.0)


def test_contract_context_is_empty_without_the_pull(nfl_root):
    assert ft.contract_context(2026).height == 0


# --- the leakage guard ---------------------------------------------------

def test_leakage_guard_covers_age():
    """`age` was legitimate by luck rather than by declaration until plan 22.

    The fixtures build rosters without a ``birth_date``, so ``roster_context``
    never created the column and the guard never had to judge it. On real data it
    reported ``age`` as leakage -- a false positive that would have been read as a
    real one.
    """
    frame = pl.DataFrame({"gsis_id": ["a"], "season": [2026], "age": [27.4],
                          "p1_targets_pg": [5.0]})
    assert ft.leakage_columns(frame, 2026) == []


def test_leakage_guard_allows_the_contract_columns():
    frame = pl.DataFrame({
        "gsis_id": ["a"], "season": [2026],
        "contract_apy_pct": [0.1], "contract_guaranteed": [17.0],
        "contract_years": [4.0], "contract_age": [1.0],
        "contract_is_new": [False], "p1_targets_pg": [5.0]})
    assert ft.leakage_columns(frame, 2026) == []


def test_leakage_guard_still_catches_an_unlagged_advanced_column():
    """The new columns must be caught when they arrive without a lag prefix."""
    frame = pl.DataFrame({"gsis_id": ["a"], "season": [2026],
                          "route_share": [0.8], "p1_route_share": [0.7]})
    assert ft.leakage_columns(frame, 2026) == ["route_share"]
