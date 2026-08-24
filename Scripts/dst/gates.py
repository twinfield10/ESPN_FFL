"""G-DST2, run against the baseline that can actually be run.

The gate as ``docs/plans/30-dst-model.md`` pre-registers it has two baselines: (a)
prior-season D/ST points, and (b) ``ESPN_projected_total``. **Only (a) is runnable, and
this module says so rather than quietly reporting half a gate as a whole one.**

Baseline (b) needs ESPN's *pre-season* D/ST projection for a season whose result is
known. The store holds no board before 2026 -- ``Data/Store/2025/*/`` carries
``lineups.parquet`` and nothing else -- and re-requesting 2025 from ``kona_player_info``
today returns ESPN's *final* projection, which has seen the season it is being scored
against. There is no honest way to reconstruct it, so (b) is deferred to 2027, when the
2026 board now in the store becomes the historical record it needs. That is the same
shape of answer plan 18's G2 reached, and for the same reason.

Baseline (a) is the more informative half anyway. Total D/ST points persist at
r = 0.220-0.267 across the nine leagues, so "last year's defence" is a weak predictor --
but it is exactly what a drafter uses when no model is on the board, and it is what the
model has to beat to be worth its weight.

**Walk-forward, not in-sample.** For each test season the model is refit on the seasons
strictly before it, so the fit never sees the year it is scored on. That costs a minute
and is the difference between a gate and a decoration.

Usage::

    python -m Scripts.dst.gates
    python -m Scripts.dst.gates --seasons 2023 2024 2025
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import polars as pl

from Scripts import paths  # noqa: F401  -- import side effects match the package
from Scripts.config_utils import load_config
from Scripts.dst import model as dm
from Scripts.projection_utils import _apply_scoring
from Scripts.scoring import SLOT_DST, get_scoring_table

#: The bar plan 30 pre-registers for baseline (a): the model must cut mean absolute
#: error on season D/ST points by at least this much, **in every league**.
BAR_VS_PRIOR: float = 0.10


def realised(seasons: Sequence[int]) -> pl.DataFrame:
    """Actual D/ST component vectors per team-season, shaped like :func:`dm.project`.

    The tier components are counted from weekly actuals rather than derived from a
    season mean -- which is the whole point of plan 30's ``E[f(X)]`` finding, and would
    invalidate the comparison if done the other way here.

    Args:
        seasons: Seasons to summarise.

    Returns:
        pl.DataFrame: ``season``, ``team`` and ``DST_<stat>`` columns.
    """
    tw = dm.team_weeks(list(seasons))

    aggs = [pl.len().alias("games")]
    for col, espn in dm.RATES.items():
        aggs.append(pl.col(col).fill_null(0).sum().alias(f"DST_{espn}"))
    aggs.append(pl.col("def_tds").fill_null(0).sum().alias("_def_tds"))
    aggs.append(pl.col("points_allowed").fill_null(0).sum()
                .alias("DST_defensivePointsAllowed"))
    aggs.append(pl.col("yards_allowed").fill_null(0).sum()
                .alias("DST_defensiveYardsAllowed"))

    # Games in each tier, counted one week at a time.
    for name, ladder, src in (("pa", dm.PA_TIERS, "points_allowed"),
                              ("yd", dm.YD_TIERS, "yards_allowed")):
        for tier, lo, hi in ladder:
            aggs.append(((pl.col(src) >= lo) & (pl.col(src) <= hi))
                        .sum().cast(pl.Float64).alias(f"DST_defensive{tier}"))

    out = tw.group_by(["season", "team"]).agg(aggs)
    share = dm.INT_TD_SHARE
    return out.with_columns(
        (pl.col("_def_tds") * share).alias("DST_interceptionReturnTouchdowns"),
        (pl.col("_def_tds") * (1 - share)).alias("DST_fumbleReturnTouchdowns"),
        (pl.col("_def_tds") * (1 - share)).alias("DST_fumbleRecoveredForTD"),
        pl.col("_def_tds").cast(pl.Float64).alias("DST_defensiveTouchdowns"),
    ).drop("_def_tds")


def score(frame: pl.DataFrame, league_key: str, season: int) -> pd.DataFrame:
    """Score a ``DST_``-prefixed component frame under one league's slot-16 rules.

    Args:
        frame: Output of :func:`realised` or :func:`dm.project`.
        league_key: Config key, e.g. ``"gop_degenerates"``.
        season: Season whose scoring table applies.

    Returns:
        pd.DataFrame: ``team`` and ``DST_Points``.
    """
    pdf = frame.to_pandas()
    pdf["primaryPosition"] = "D/ST"
    table = get_scoring_table(league_key=league_key, season=season,
                              verify=False, slot=SLOT_DST)
    _apply_scoring(pdf, table, ["DST"])
    return pdf[["team", "DST_Points"]].copy()


def _walk_forward(test_seasons: Sequence[int],
                  first: int = 2016) -> Dict[int, pl.DataFrame]:
    """Refit on everything before each test season, then project it."""
    out: Dict[int, pl.DataFrame] = {}
    for s in test_seasons:
        train = list(range(first, s))
        # `holdout` carves the shrinkage/diagnostic split out of `train` only, so the
        # test season stays entirely unseen.
        model = dm.fit(seasons=train, holdout=2)
        out[s] = dm.project(s, model)
    return out


def run(test_seasons: Optional[Sequence[int]] = None,
        leagues: Optional[Sequence[str]] = None) -> Tuple[pd.DataFrame, bool]:
    """Run G-DST2 baseline (a) across every configured league.

    Args:
        test_seasons: Seasons to score. Defaults to 2024 and 2025.
        leagues: Config keys. Defaults to every configured league.

    Returns:
        tuple: A per-league result frame, and whether the gate passed everywhere.
    """
    test_seasons = [2024, 2025] if test_seasons is None else list(test_seasons)
    if leagues is None:
        leagues = list(load_config()["leagues"].keys())

    need = sorted(set(test_seasons) | {s - 1 for s in test_seasons})
    truth = realised(need)
    preds = _walk_forward(test_seasons)

    rows = []
    for key in leagues:
        model_err, prior_err = [], []
        for s in test_seasons:
            actual = score(truth.filter(pl.col("season") == s), key, s)
            before = score(truth.filter(pl.col("season") == s - 1), key, s)
            pred = score(preds[s], key, s)

            a = actual.rename(columns={"DST_Points": "actual"})
            m = a.merge(pred.rename(columns={"DST_Points": "model"}), on="team")
            m = m.merge(before.rename(columns={"DST_Points": "prior"}), on="team")
            m = m.dropna(subset=["actual", "model", "prior"])
            model_err.append((m["model"] - m["actual"]).abs())
            prior_err.append((m["prior"] - m["actual"]).abs())

        mm = float(pd.concat(model_err).mean())
        pm = float(pd.concat(prior_err).mean())
        gain = 1 - mm / pm if pm else 0.0
        rows.append({"league": key, "model_mae": mm, "prior_mae": pm,
                     "gain": gain, "pass": gain >= BAR_VS_PRIOR,
                     "n": int(sum(len(e) for e in model_err))})

    out = pd.DataFrame(rows)
    return out, bool(out["pass"].all())


def report(test_seasons: Optional[Sequence[int]] = None) -> str:
    """Human-readable G-DST2(a) result."""
    test_seasons = [2024, 2025] if test_seasons is None else list(test_seasons)
    df, ok = run(test_seasons)
    lines = [
        f"===== G-DST2 baseline (a): model vs prior-season points "
        f"({', '.join(str(s) for s in test_seasons)}, walk-forward) =====",
        f"  bar: cut MAE by >= {BAR_VS_PRIOR:.0%} in every league",
        "",
        f"  {'league':<28}{'n':>5}{'model MAE':>11}{'prior MAE':>11}"
        f"{'gain':>9}  verdict",
        "  " + "-" * 72,
    ]
    for r in df.sort_values("gain", ascending=False).to_dict("records"):
        lines.append(f"  {r['league']:<28}{r['n']:>5}{r['model_mae']:>11.1f}"
                     f"{r['prior_mae']:>11.1f}{r['gain']:>8.1%}  "
                     f"{'PASS' if r['pass'] else 'FAIL'}")
    lines += ["", f"  G-DST2(a): {'PASS' if ok else 'FAIL'} "
                  f"({int(df['pass'].sum())} of {len(df)} leagues)",
              "",
              "  G-DST2(b) vs ESPN is NOT run: no pre-season ESPN D/ST projection",
              "  survives for any season whose result is known. Deferred to 2027,",
              "  when the 2026 board in the store becomes that record."]
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--seasons", type=int, nargs="+", default=None)
    a = ap.parse_args(argv)
    print(report(a.seasons))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
