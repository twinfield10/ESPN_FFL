"""Do plan 22's negative results transfer to the weekly head? Two of them invert.

Everything in :mod:`Scripts.lab.run` was measured on the **season** head, and the
obvious question is whether it generalises. It does not, and the reason is not
subtle: two of the three failure mechanisms were arguments about *sample size*, and
the weekly horizon changes the sample size by an order of magnitude in one direction
and the shrinkage weight by a factor of two in the other.

**Routes.** Seasonally, route count and target count are two measurements of the
same role over seventeen games, and the second one is already the model's strongest
regressor. Over a trailing three-appearance window they are not comparable at all:
the median receiver has 12 trailing targets and 74 trailing routes. Routes carry
roughly six times the sample per unit time, so they stabilise about
:math:`\\sqrt{6}` times faster, and early-season target counts are mostly noise.

**The efficiency prior.** The season finding was that a fitted prior reaches the
players who give it almost no weight, because NGS's qualifying threshold is a
volume threshold and credibility weight falls with volume. Weekly the denominator is
a three-game window rather than a season, so the prior carries **more than double**
the weight — and it crosses 50%, which makes it the dominant term rather than a
correction. The anti-correlation that made the season result inevitable is broken.

Neither of these is a weekly *model* — plan 19 is not started. They are feature-level
measurements that say what plan 19 should carry.

Usage:
    python -m Scripts.lab.weekly
"""

import argparse
from typing import Dict, List, Optional, Sequence

import numpy as np
import polars as pl

from Scripts.lab.run import RESULTS_PATH, load_results, save_results
from Scripts.usage import features as ft
from Scripts.usage import nflverse as nv

#: Trailing window, in appearances, matching :data:`Scripts.usage.baseline.TRAILING_WINDOW`.
WINDOW = 3

#: Seasons to pool. 2018 rather than 2016 only to keep the panel and the season-side
#: comparison on the same footing; routes go back to 2016.
SEASONS = tuple(range(2018, 2026))

#: The fold boundary. Train on everything before, test on this.
TEST_SEASON = 2025


def panel(seasons: Sequence[int] = SEASONS) -> pl.DataFrame:
    """Player-weeks with trailing features, as of each week.

    Trailing terms are shifted by one so a row never sees its own outcome, and are
    counted over appearances rather than calendar weeks so a bye shortens the
    history instead of blanking it -- the same choice
    :mod:`Scripts.usage.baseline` makes.

    Args:
        seasons: Seasons to pool.

    Returns:
        pl.DataFrame: One row per player-week with ``t3_`` features and the
        realised outcome.
    """
    weekly = ft.load_player_weeks(list(seasons)).select(
        ["season", "week", "gsis_id", "position", "targets", "carries",
         "receiving_yards", "target_share"])
    routes = nv.load_advanced(list(seasons), "routes").select(
        ["season", "week", "gsis_id", "routes", "route_share"])

    group = ["gsis_id", "season"]
    return (
        weekly.join(routes, on=["season", "week", "gsis_id"], how="left")
        .sort(["gsis_id", "season", "week"])
        .with_columns([
            pl.col("targets").rolling_mean(WINDOW).over(group).shift(1)
              .alias("t3_targets"),
            pl.col("target_share").rolling_mean(WINDOW).over(group).shift(1)
              .alias("t3_target_share"),
            pl.col("route_share").rolling_mean(WINDOW).over(group).shift(1)
              .alias("t3_route_share"),
            pl.col("routes").rolling_mean(WINDOW).over(group).shift(1)
              .alias("t3_routes"),
            pl.col("targets").rolling_sum(WINDOW).over(group).shift(1)
              .alias("t3_target_n"),
            pl.col("routes").rolling_sum(WINDOW).over(group).shift(1)
              .alias("t3_route_n"),
        ])
    )


def _holdout_r2(train: pl.DataFrame, test: pl.DataFrame,
                regressors: Sequence[str], outcome: str) -> float:
    """Out-of-sample R-squared for an OLS fit, intercept included."""
    def design(frame):
        return (np.column_stack([np.ones(frame.height)]
                                + [frame[c].to_numpy() for c in regressors]),
                frame[outcome].to_numpy())

    train_x, train_y = design(train)
    beta, *_ = np.linalg.lstsq(train_x, train_y, rcond=None)
    test_x, test_y = design(test)
    residual = test_y - test_x @ beta
    variance = float(((test_y - test_y.mean()) ** 2).sum())
    return 1.0 - float((residual ** 2).sum()) / variance if variance else 0.0


def routes_transfer(frame: pl.DataFrame) -> List[Dict]:
    """Does trailing route share add to trailing targets, predicting next week?

    The direct weekly analogue of ``routes_volume``, which was rejected on the
    season head at +0.0000 mean Spearman.

    Args:
        frame: :func:`panel` output.

    Returns:
        list: One entry per position with the holdout R-squared either way and the
        trailing sample sizes that explain the difference.
    """
    usable = frame.drop_nulls(["t3_targets", "t3_route_share", "t3_routes",
                               "targets"])
    out = []
    for position in ("WR", "TE", "RB"):
        rows = usable.filter(pl.col("position") == position)
        train = rows.filter(pl.col("season") < TEST_SEASON)
        test = rows.filter(pl.col("season") == TEST_SEASON)
        if train.height < 500 or test.height < 100:
            continue
        base = _holdout_r2(train, test, ["t3_targets"], "targets")
        with_routes = _holdout_r2(
            train, test, ["t3_targets", "t3_route_share", "t3_routes"], "targets")
        out.append({
            "position": position,
            "n_test": test.height,
            "base_r2": base,
            "with_routes_r2": with_routes,
            "delta": with_routes - base,
            "median_trailing_targets":
                float(test.select(pl.col("t3_target_n").median()).item()),
            "median_trailing_routes":
                float(test.select(pl.col("t3_route_n").median()).item()),
        })
    return out


def shrinkage_transfer(seasons: Sequence[int] = SEASONS) -> List[Dict]:
    """How much weight the efficiency prior carries, season against weekly.

    ``k / (n + k)`` at each horizon. Reported at the 95th percentile of volume as
    well as the median, because the median rostered player is a special-teamer and
    the question is about the players a lineup actually contains.

    Args:
        seasons: Seasons to pool.

    Returns:
        list: One entry per rate.
    """
    weekly = ft.load_player_weeks(list(seasons))
    totals = ft.season_totals(weekly)
    trailing = weekly.sort(["gsis_id", "season", "week"]).with_columns([
        pl.col("targets").rolling_sum(WINDOW).over(["gsis_id", "season"]).shift(1)
          .alias("t3_targets"),
        pl.col("carries").rolling_sum(WINDOW).over(["gsis_id", "season"]).shift(1)
          .alias("t3_carries"),
    ])

    pairs = [("yards_per_target", "tot_targets", "t3_targets"),
             ("catch_rate", "tot_targets", "t3_targets"),
             ("rec_td_per_target", "tot_targets", "t3_targets"),
             ("yards_per_carry", "tot_carries", "t3_carries"),
             ("rush_td_per_carry", "tot_carries", "t3_carries")]

    out = []
    for rate, season_column, weekly_column in pairs:
        k = ft.SHRINKAGE_K[rate]
        season_n = totals.filter(pl.col(season_column) > 0).select(
            pl.col(season_column).quantile(0.95)).item()
        weekly_n = trailing.filter(pl.col(weekly_column) > 0).select(
            pl.col(weekly_column).quantile(0.95)).item()
        out.append({
            "rate": rate,
            "k": k,
            "season_n_p95": float(season_n),
            "season_prior_weight": k / (season_n + k),
            "weekly_n_p95": float(weekly_n),
            "weekly_prior_weight": k / (weekly_n + k),
        })
    return out


def run() -> Dict:
    """Both transfer tests."""
    frame = panel()
    return {"routes": routes_transfer(frame), "shrinkage": shrinkage_transfer()}


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.lab.weekly",
        description="Test whether plan 22's season findings transfer to weekly.")
    parser.parse_args(argv)

    results = run()

    print("Routes, weekly: trailing route share on top of trailing targets, "
          f"predicting next-week targets (test {TEST_SEASON})\n")
    print(f"  {'pos':<5}{'n':>7}{'base R2':>10}{'+routes':>10}{'delta':>10}"
          f"{'t3 targets':>13}{'t3 routes':>12}")
    for entry in results["routes"]:
        print(f"  {entry['position']:<5}{entry['n_test']:>7}"
              f"{entry['base_r2']:>10.4f}{entry['with_routes_r2']:>10.4f}"
              f"{entry['delta']:>+10.4f}"
              f"{entry['median_trailing_targets']:>13.0f}"
              f"{entry['median_trailing_routes']:>12.0f}")

    print("\nEfficiency prior: weight k/(n+k) at the 95th percentile of volume\n")
    print(f"  {'rate':<22}{'k':>5}{'season n':>10}{'weight':>9}"
          f"{'weekly n':>10}{'weight':>9}")
    for entry in results["shrinkage"]:
        print(f"  {entry['rate']:<22}{entry['k']:>5.0f}"
              f"{entry['season_n_p95']:>10.0f}{entry['season_prior_weight']:>8.1%}"
              f"{entry['weekly_n_p95']:>10.0f}{entry['weekly_prior_weight']:>8.1%}")

    ledger = load_results()
    ledger["weekly_transfer"] = results
    save_results(ledger)
    print(f"\nwrote {RESULTS_PATH.relative_to(RESULTS_PATH.parents[2])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
