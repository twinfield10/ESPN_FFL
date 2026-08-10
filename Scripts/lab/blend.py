"""Can the blend weights be fitted rather than chosen? Measured: no.

``WEIGHTS`` in :mod:`Scripts.projection_utils` is ESPN, FantasyPros and the usage
head at a third each, set by hand on 2026-08-07. Fitting them against realised
outcomes is the obvious improvement and plan 22 tried it.

The answer is that this data does not identify them, which is a different and more
useful result than "the current weights are fine". Three reasons, all measured by
:func:`fit_weights` and :func:`split_half`:

1. **The sample collapses.** The blend's own imputation machinery means most rows
   carry a filled-in value for at least one source. Requiring every source to be
   *real* takes the 2025 evaluation set from 5,257 player-weeks to 265-401 -- and
   the survivors are the heavily-covered stars, not the population the weights are
   applied to.
2. **The sources are collinear.** Plan 16's G0 measured FantasyPros' residuals at
   **+0.988** against ESPN's. Non-negative least squares on two near-copies splits
   their weight arbitrarily, which is exactly what the split-half test shows.
3. **The season question is not this question.** These are weekly rows. The open
   question is the *season* blend's usage weight, and there is no historical season
   blend to fit against -- plan 18 records that as a permanent limitation of the
   data rather than a gap in the work.

Usage:
    python -m Scripts.lab.blend
"""

import argparse
import json
from typing import Dict, List, Optional, Sequence

import numpy as np
import polars as pl

from Scripts.lab.run import RESULTS_PATH, load_results, save_results
from Scripts.projection_utils import WEIGHTS
from Scripts.usage import evalset

#: Sources the evaluation set carries a column for.
SOURCES = ("ESPN", "FP", "PINNY", "BOL")

#: Fewest usable rows before a stat is worth fitting at all.
MIN_ROWS = 150


def real_only(frame: pl.DataFrame, stat: str,
              sources: Sequence[str] = SOURCES) -> pl.DataFrame:
    """Rows where every listed source produced a real number for ``stat``.

    An imputed value is a copy of another source by construction, so fitting on one
    would measure the imputation rule rather than the source.

    Args:
        frame: The evaluation set.
        stat: ESPN stat name.
        sources: Source prefixes to require.

    Returns:
        pl.DataFrame: ``week``, the actual, and one column per available source.
    """
    columns = [f"{s}_{stat}" for s in sources if f"{s}_{stat}" in frame.columns]
    if not columns or f"act_{stat}" not in frame.columns:
        return pl.DataFrame()

    out = frame.select(["week", f"act_{stat}"] + columns)
    for column in columns:
        flag = f"{column}_is_imputed"
        if flag in frame.columns:
            out = out.with_columns(
                pl.when(frame[flag]).then(None)
                .otherwise(pl.col(column)).alias(column))
    return out.drop_nulls()


def _nnls(design: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Non-negative least squares, normalised to sum to one.

    Non-negative because a negative weight on a forecaster is not a blend, it is a
    bet against them, and this pipeline has no way to act on that. Normalised
    because :func:`Scripts.projection_utils.compute_weighted_stats` renormalises
    anyway when a source abstains.
    """
    from scipy.optimize import nnls
    weights, _ = nnls(design, y)
    total = weights.sum()
    return weights / total if total > 0 else weights


def current_weights(columns: Sequence[str]) -> np.ndarray:
    """The shipped weights for these columns, renormalised over them."""
    weights = np.array([WEIGHTS["default"].get(c.split("_")[0], 0.0)
                        for c in columns])
    total = weights.sum()
    return weights / total if total > 0 else weights


def split_half(frame: pl.DataFrame, stat: str) -> Optional[Dict]:
    """Fit on odd weeks, score on even weeks, and report both fits.

    The stability test rather than the accuracy test, and it is the one that
    matters. Two fits of the same quantity on two halves of one season should agree;
    when they do not, an improvement measured in-sample is a description of the
    sample.

    Args:
        frame: The evaluation set.
        stat: ESPN stat name.

    Returns:
        dict | None: Both weight vectors, the out-of-sample MAE change against the
        shipped weights, and the largest disagreement between the halves. None when
        either half is too small.
    """
    usable = real_only(frame, stat)
    if usable.is_empty():
        return None
    columns = [c for c in usable.columns if c not in ("week", f"act_{stat}")]

    odd = usable.filter(pl.col("week") % 2 == 1)
    even = usable.filter(pl.col("week") % 2 == 0)
    if odd.height < MIN_ROWS // 2 or even.height < MIN_ROWS // 2:
        return None

    def design(part):
        return (np.column_stack([part[c].to_numpy() for c in columns]),
                part[f"act_{stat}"].to_numpy())

    odd_x, odd_y = design(odd)
    even_x, even_y = design(even)
    odd_w = _nnls(odd_x, odd_y)
    even_w = _nnls(even_x, even_y)
    shipped = current_weights(columns)

    fitted_mae = float(np.abs(even_x @ odd_w - even_y).mean())
    shipped_mae = float(np.abs(even_x @ shipped - even_y).mean())

    names = [c.split("_")[0] for c in columns]
    return {
        "stat": stat,
        "n_odd": odd.height,
        "n_even": even.height,
        "n_total_evalset": frame.height,
        "odd_weights": dict(zip(names, odd_w.round(3).tolist())),
        "even_weights": dict(zip(names, even_w.round(3).tolist())),
        "shipped_weights": dict(zip(names, shipped.round(3).tolist())),
        "out_of_sample_mae_change_pct":
            100 * (fitted_mae - shipped_mae) / shipped_mae if shipped_mae else None,
        "max_half_disagreement": float(np.abs(odd_w - even_w).max()),
    }


def run(season: int = 2025) -> List[Dict]:
    """Every stat the evaluation set can speak for.

    Args:
        season: Season whose stores the evaluation set pools.

    Returns:
        list: One :func:`split_half` result per stat with enough rows.
    """
    frame = evalset.build_eval_set(season)[0]
    stats = sorted({c.split("_", 1)[1] for c in frame.columns
                    if c.startswith("act_")})
    return [result for result in (split_half(frame, stat) for stat in stats)
            if result is not None]


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.lab.blend",
        description="Test whether the blend weights can be fitted from outcomes.")
    parser.add_argument("--season", type=int, default=2025)
    args = parser.parse_args(argv)

    results = run(args.season)
    if not results:
        print("No stat had enough rows with every source real.")
        return 0

    print(f"Split-half stability, {args.season} evaluation set "
          f"({results[0]['n_total_evalset']:,} player-weeks before filtering)\n")
    for entry in results:
        show = lambda w: " ".join(f"{k}={v:.2f}" for k, v in w.items())
        print(f"  {entry['stat']}  (odd n={entry['n_odd']}, "
              f"even n={entry['n_even']})")
        print(f"    odd weeks   {show(entry['odd_weights'])}")
        print(f"    even weeks  {show(entry['even_weights'])}")
        print(f"    largest disagreement between halves "
              f"{entry['max_half_disagreement']:.2f}")
        print(f"    out-of-sample MAE vs shipped weights "
              f"{entry['out_of_sample_mae_change_pct']:+.1f}%\n")

    ledger = load_results()
    ledger["blend_weights"] = {"season": args.season, "results": results}
    save_results(ledger)
    print(f"wrote {RESULTS_PATH.relative_to(RESULTS_PATH.parents[2])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
