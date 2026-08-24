"""Walk-forward evaluation of the recovery curve and the hazard, against pre-set gates.

The thresholds live in :mod:`Scripts.lab.registry` and were written before this ran. That
is the point of putting them there: a bar chosen after seeing the numbers is not a bar.

**Folds are on episodes, not weeks.** For each season S the model trains on episodes whose
*return* falls in seasons up to S-1 and is scored on episodes returning in S. Six
appearances of one injury are one correlated observation, so splitting them across folds
would leak the answer into the question.

**Three things are re-derived inside every fold**, each of which is leakage if it is not:

*The control cohort.* Computing the placebo curve once over all seasons and then "walking
forward" leaks, and it is the subtle one because a nuisance parameter does not feel like a
parameter.

*The shrinkage strength.* Picking ``k`` once on the pooled data is the fishing
``registry.py`` exists to prevent.

*The ESPN ramp.* Only 2025 has stored weekly lineups, so it cannot in fact be
walk-forwarded, and the honest handling is to leave it out of the scored comparison rather
than to let a 2025 measurement inform a 2019 fold.

**The largest leakage risk is duration itself.** ``weeks_out`` is endogenous: a mild injury
both returns quickly *and* performs well on return, which is what makes it a useful
predictor and is not a causal claim. But at apply time duration is *predicted*, not
observed. So every metric is computed twice -- once conditioning on realised duration
(**oracle**) and once with the body part alone (**blind**) -- and only the blind figure is
compared against a gate. The gap between them is the value of better severity information,
which is also the quantified case for the daily ESPN archive.

Usage::

    python -m Scripts.injury.backtest
    python -m Scripts.injury.backtest --seasons 2019-2025
"""

from __future__ import annotations

import datetime
import json
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts.injury import episodes as ep
from Scripts.injury import model as im
from Scripts.lab import registry as reg

#: Seasons scored. 2019 is the first with three prior seasons behind it.
DEFAULT_FOLDS = tuple(range(2019, 2026))

#: The hypothesis this plan started from, scored explicitly so the disagreement with the
#: measurement is a number rather than an assertion.
HYPOTHESISED_LADDER = (0.75, 0.75, 0.85, 0.92, 1.0, 1.0)

#: Bootstrap resamples inside a fold.
#:
#: Lower than the shipped fit, and it is not only for speed: the standard errors drive the
#: abstention rule, so they have to be computed per fold rather than borrowed, and 60 draws
#: is enough to place a shortfall inside or outside two of them.
FOLD_DRAWS = 60

#: Deciles the calibration slope is measured over.
CALIBRATION_BINS = 5


def _expected(rows: pl.DataFrame, control: im.ControlCurve) -> np.ndarray:
    """What a comparable healthy player would have scored, per row."""
    return np.array([
        r["base_pts"] * control.expected(r["position"], r["base_pts"],
                                         r["appearance_back"])
        for r in rows.iter_rows(named=True)])


def _multipliers(rows: pl.DataFrame, model: im.InjuryModel,
                 blind: bool) -> np.ndarray:
    """The model's multiplier per row.

    Args:
        rows: Post-return appearances.
        model: The fitted model.
        blind: True to withhold realised duration -- what the live system has.
    """
    return np.array([
        model.multiplier(r["body_part"], r["appearance_back"],
                         None if blind else r["duration_bucket"])
        for r in rows.iter_rows(named=True)])


def _mae(actual: np.ndarray, predicted: np.ndarray) -> float:
    return float(np.mean(np.abs(actual - predicted))) if actual.size else float("nan")


def _rmse(actual: np.ndarray, predicted: np.ndarray) -> float:
    """Root mean squared error, reported beside MAE because they disagree here.

    **They disagree for a structural reason and it decides how to read the result.** The
    prediction is a conditional *mean* -- a ratio of sums -- and weekly fantasy scoring is
    strongly right-skewed, so the conditional median sits well below it. MAE is minimised by
    the median. That makes MAE reward *any* downward bias whether or not it is about
    injuries, and it shows up as a signature in the results: the ranking of candidates comes
    out in order of how hard each one discounts, with the hypothesised 0.75 ladder -- the
    most aggressive -- scoring best, and healthy comparables "improving" when discounted too.

    RMSE is minimised by the conditional mean, which is what the model estimates, so it is
    the metric that can actually distinguish a correct multiplier from a merely smaller one.
    Both are reported; the pre-committed gate reads MAE, and that is left alone rather than
    swapped after the fact.
    """
    if not actual.size:
        return float("nan")
    return float(np.sqrt(np.mean((actual - predicted) ** 2)))


def _calibration_slope(actual: np.ndarray, expected: np.ndarray,
                       multiplier: np.ndarray) -> float:
    """Regress realised shortfall on predicted shortfall, binned.

    Binned rather than per row because a single appearance is far too noisy to regress --
    the slope of realised-on-predicted at row level is an attenuation artefact. Bins are
    equal-count on the prediction, and the slope is the ordinary least-squares fit through
    the bin means, weighted by expectation mass.

    Returns:
        float: 1.0 means a cell predicted to lose 20% loses 20%. Near 0 means the ordering
        may be right while the magnitude is noise.
    """
    usable = (expected > 0) & np.isfinite(actual) & np.isfinite(multiplier)
    if usable.sum() < 3 * CALIBRATION_BINS:
        return float("nan")
    predicted = multiplier[usable]
    realised = actual[usable] / expected[usable]
    mass = expected[usable]

    if np.allclose(predicted, predicted[0]):
        return float("nan")
    order = np.argsort(predicted)
    chunks = np.array_split(order, CALIBRATION_BINS)
    xs, ys, ws = [], [], []
    for chunk in chunks:
        if chunk.size == 0:
            continue
        weight = mass[chunk].sum()
        xs.append(float(np.average(predicted[chunk], weights=mass[chunk])))
        # Ratio of sums inside the bin, matching the estimator the curve was fitted with.
        ys.append(float((realised[chunk] * mass[chunk]).sum() / weight))
        ws.append(weight)
    if len(xs) < 2 or np.allclose(xs, xs[0]):
        return float("nan")
    slope = np.polyfit(np.array(xs), np.array(ys), 1, w=np.sqrt(np.array(ws)))[0]
    return float(slope)


def _score(rows: pl.DataFrame, control: im.ControlCurve,
           multiplier: np.ndarray) -> Dict[str, float]:
    """MAE of predicted points against actual, for one multiplier vector."""
    actual = rows["fantasy_points_ppr"].to_numpy()
    expected = _expected(rows, control)
    return {"mae": _mae(actual, expected * multiplier),
            "rows": int(rows.height),
            "mass": float(expected.sum())}


def _pick_k(train: pl.DataFrame, controls: pl.DataFrame, episodes: pl.DataFrame,
            inner_season: int) -> Tuple[float, Dict[str, float]]:
    """Choose the shrinkage strength on a fold *inside* the training seasons.

    Reports the whole sweep, not just the winner. ``registry.py``'s discipline verbatim: a
    gain that holds across a wide range is a real bias-variance trade; a gain that spikes at
    one value is the test set being fitted -- and here the "test set" would be the inner
    validation season.
    """
    inner_train = train.filter(pl.col("season") < inner_season)
    inner_test = train.filter(pl.col("season") == inner_season)
    if inner_train.is_empty() or inner_test.is_empty():
        return 20.0, {}

    inner_controls = controls.filter(pl.col("season") < inner_season)
    inner_episodes = episodes.filter(pl.col("season") < inner_season)
    scores: Dict[str, float] = {}
    for k in im.SHRINKAGE_GRID:
        model = im.fit(inner_train, inner_controls, inner_episodes, k=float(k),
                       draws=FOLD_DRAWS, train_seasons=[inner_season - 1])
        usable = im._fittable(inner_test)
        if usable.is_empty():
            continue
        scores[str(k)] = _score(usable, model.control,
                                _multipliers(usable, model, blind=True))["mae"]
    if not scores:
        return 20.0, {}
    best = min(scores, key=scores.get)
    return float(best), scores


def run(folds: Sequence[int] = DEFAULT_FOLDS) -> Dict:
    """Walk forward across seasons and score every baseline against the model.

    Args:
        folds: Seasons to score. Each trains on everything strictly before it.

    Returns:
        dict: Per-fold and pooled metrics, plus the verdicts.
    """
    post = ep.load_post_return()
    controls = ep.load_controls()
    episodes = ep.load_episodes()

    per_fold: List[Dict] = []
    pooled: Dict[str, List[Tuple[float, float]]] = {}
    slope_parts: List[Tuple[np.ndarray, np.ndarray, np.ndarray]] = []
    chosen_k: Dict[str, float] = {}
    control_rmse_changes: List[float] = []

    for season in folds:
        train = post.filter(pl.col("season") < season)
        test = im._fittable(post.filter(pl.col("season") == season))
        if train.is_empty() or test.is_empty():
            continue

        k, sweep = _pick_k(train, controls, episodes, season - 1)
        chosen_k[str(season)] = k
        model = im.fit(train,
                       controls.filter(pl.col("season") < season),
                       episodes.filter(pl.col("season") < season),
                       k=k, draws=FOLD_DRAWS, train_seasons=range(2016, season))

        actual = test["fantasy_points_ppr"].to_numpy()
        expected = _expected(test, model.control)
        ones = np.ones(test.height)
        blind = _multipliers(test, model, blind=True)
        oracle = _multipliers(test, model, blind=False)
        ladder = np.array([HYPOTHESISED_LADDER[min(r - 1, len(HYPOTHESISED_LADDER) - 1)]
                           for r in test["appearance_back"].to_list()])
        global_only = np.array([
            model.multiplier(im.GLOBAL, r) for r in test["appearance_back"].to_list()])

        candidates = {"do_nothing": ones, "blind": blind, "oracle": oracle,
                      "hypothesised_ladder": ladder, "global_only": global_only}
        maes = {name: _mae(actual, expected * m) for name, m in candidates.items()}
        rmses = {name: _rmse(actual, expected * m) for name, m in candidates.items()}

        # Control rows for the same season, discounted as if they were injured -- the
        # false-positive test. They are not hurt, so this should not help.
        ctl = controls.filter(pl.col("season") == season)
        control_change = float("nan")
        control_change_rmse = float("nan")
        if not ctl.is_empty():
            ctl_actual = ctl["fantasy_points_ppr"].to_numpy()
            ctl_expected = np.array([
                r["base_pts"] * model.control.expected(r["position"], r["base_pts"],
                                                       r["appearance_back"])
                for r in ctl.iter_rows(named=True)])
            ctl_mult = np.array([model.multiplier(im.GLOBAL, r)
                                 for r in ctl["appearance_back"].to_list()])
            plain = _mae(ctl_actual, ctl_expected)
            with_model = _mae(ctl_actual, ctl_expected * ctl_mult)
            control_change = 100.0 * (with_model - plain) / plain if plain else float("nan")
            plain_r = _rmse(ctl_actual, ctl_expected)
            with_r = _rmse(ctl_actual, ctl_expected * ctl_mult)
            control_change_rmse = (100.0 * (with_r - plain_r) / plain_r if plain_r
                                   else float("nan"))

        slope_parts.append((actual, expected, blind))
        for name, value in maes.items():
            pooled.setdefault(name, []).append((value, float(test.height)))
        for name, value in rmses.items():
            pooled.setdefault(f"rmse:{name}", []).append((value, float(test.height)))
        if np.isfinite(control_change_rmse):
            control_rmse_changes.append(control_change_rmse)

        per_fold.append({
            "season": int(season), "test_rows": int(test.height),
            "test_episodes": im._episode_count(test),
            "k": k, "k_sweep": sweep,
            "mae": {n: round(v, 4) for n, v in maes.items()},
            "rmse": {n: round(v, 4) for n, v in rmses.items()},
            "gain_pct": {
                n: round(100.0 * (maes["do_nothing"] - v) / maes["do_nothing"], 3)
                for n, v in maes.items() if maes["do_nothing"]},
            "rmse_gain_pct": {
                n: round(100.0 * (rmses["do_nothing"] - v) / rmses["do_nothing"], 3)
                for n, v in rmses.items() if rmses["do_nothing"]},
            "control_mae_change_pct": (round(control_change, 3)
                                       if np.isfinite(control_change) else None),
        })

    if not per_fold:
        return {"folds": [], "error": "no fold had both training and test rows."}

    def weighted(name: str) -> float:
        pairs = pooled.get(name, [])
        total = sum(w for _, w in pairs)
        return sum(v * w for v, w in pairs) / total if total else float("nan")

    do_nothing = weighted("do_nothing")
    gains = {name: 100.0 * (do_nothing - weighted(name)) / do_nothing
             for name in pooled if do_nothing and not name.startswith("rmse:")}
    rmse_base = weighted("rmse:do_nothing")
    rmse_gains = {name[len("rmse:"):]:
                  100.0 * (rmse_base - weighted(name)) / rmse_base
                  for name in pooled if rmse_base and name.startswith("rmse:")}

    actual = np.concatenate([a for a, _, _ in slope_parts])
    expected = np.concatenate([e for _, e, _ in slope_parts])
    blind = np.concatenate([m for _, _, m in slope_parts])
    slope = _calibration_slope(actual, expected, blind)

    control_changes = [f["control_mae_change_pct"] for f in per_fold
                       if f["control_mae_change_pct"] is not None]
    control_change = float(np.mean(control_changes)) if control_changes else None

    shipped = im.InjuryModel.load() if im.InjuryModel.default_path().is_file() else None
    hazard_ratio = None
    hamstring = None
    if shipped and shipped.hazard.base_brier:
        hazard_ratio = shipped.hazard.brier / shipped.hazard.base_brier
        hamstring = shipped.reinjury_probability("hamstring", 3.0)

    metrics = {
        "post_return_mae_gain_pct": round(gains.get("blind", float("nan")), 3),
        "oracle_mae_gain_pct": round(gains.get("oracle", float("nan")), 3),
        "ladder_mae_gain_pct": round(gains.get("hypothesised_ladder", float("nan")), 3),
        "global_only_mae_gain_pct": round(gains.get("global_only", float("nan")), 3),
        "control_mae_change_pct": (round(control_change, 3)
                                   if control_change is not None else None),
        "calibration_slope": round(slope, 4) if np.isfinite(slope) else None,
        "hazard_brier_ratio": round(hazard_ratio, 4) if hazard_ratio else None,
        "hamstring_recurrence": round(hamstring, 4) if hamstring else None,
        "do_nothing_mae": round(do_nothing, 4),
        "k_by_fold": chosen_k,
        "rmse_gain_pct": {n: round(v, 3) for n, v in rmse_gains.items()},
        "control_rmse_change_pct": (round(float(np.mean(control_rmse_changes)), 3)
                                    if control_rmse_changes else None),
    }
    curve_verdict, curve_reason = reg.injury_verdict(metrics)
    hazard_call, hazard_reason = reg.hazard_verdict(metrics)

    return {
        "ran_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "folds": per_fold,
        "metrics": metrics,
        "verdict": {"curve": curve_verdict, "curve_reason": curve_reason,
                    "hazard": hazard_call, "hazard_reason": hazard_reason},
        "gates": {
            "MIN_POST_RETURN_MAE_GAIN_PCT": reg.MIN_POST_RETURN_MAE_GAIN_PCT,
            "MAX_CONTROL_MAE_INCREASE_PCT": reg.MAX_CONTROL_MAE_INCREASE_PCT,
            "MIN_CALIBRATION_SLOPE": reg.MIN_CALIBRATION_SLOPE,
            "MAX_HAZARD_BRIER_RATIO": reg.MAX_HAZARD_BRIER_RATIO,
            "HAMSTRING_RECURRENCE_RANGE": list(reg.HAMSTRING_RECURRENCE_RANGE),
        },
    }


def report(result: Dict) -> str:
    """A printable account of the walk-forward."""
    if not result.get("folds"):
        return f"  {result.get('error', 'nothing to report.')}"

    lines = [f"  {'season':>7}{'eps':>6}{'rows':>6}{'k':>5}"
             f"{'do-nothing':>12}{'blind':>9}{'oracle':>9}{'ladder':>9}{'global':>9}"]
    for fold in result["folds"]:
        gain = fold["gain_pct"]
        lines.append(
            f"  {fold['season']:>7}{fold['test_episodes']:>6}{fold['test_rows']:>6}"
            f"{fold['k']:>5.0f}{fold['mae']['do_nothing']:>12.3f}"
            f"{gain.get('blind', 0):>8.2f}%{gain.get('oracle', 0):>8.2f}%"
            f"{gain.get('hypothesised_ladder', 0):>8.2f}%"
            f"{gain.get('global_only', 0):>8.2f}%")

    m = result["metrics"]
    lines.append("")
    lines.append("  Pooled, weighted by rows. Gain is MAE improvement over doing nothing.")
    lines.append(f"    blind (body part only, what the live system has): "
                 f"{m['post_return_mae_gain_pct']:+.2f}%")
    lines.append(f"    oracle (conditioned on realised duration):        "
                 f"{m['oracle_mae_gain_pct']:+.2f}%")
    lines.append(f"    the hypothesised 0.75/0.75/0.85/0.92 ladder:      "
                 f"{m['ladder_mae_gain_pct']:+.2f}%")
    lines.append(f"    a single global curve, no cell structure:         "
                 f"{m['global_only_mae_gain_pct']:+.2f}%")
    lines.append(f"    healthy comparables, discounted as if injured:    "
                 f"{m['control_mae_change_pct']:+.3f}%  "
                 f"(0 is correct; negative means mean reversion)")
    lines.append(f"    calibration slope, realised on predicted:         "
                 f"{m['calibration_slope']}")

    r = m.get("rmse_gain_pct") or {}
    if r:
        lines.append("")
        lines.append("  The same, by RMSE -- the metric the prediction is actually a "
                     "point estimate for.")
        lines.append(f"    blind:                                            "
                     f"{r.get('blind', float('nan')):+.2f}%")
        lines.append(f"    oracle:                                           "
                     f"{r.get('oracle', float('nan')):+.2f}%")
        lines.append(f"    the hypothesised ladder:                          "
                     f"{r.get('hypothesised_ladder', float('nan')):+.2f}%")
        lines.append(f"    a single global curve:                            "
                     f"{r.get('global_only', float('nan')):+.2f}%")
        if m.get("control_rmse_change_pct") is not None:
            lines.append(f"    healthy comparables:                              "
                         f"{m['control_rmse_change_pct']:+.3f}%")
    if m.get("hazard_brier_ratio"):
        lines.append(f"    hazard Brier / constant base rate:               "
                     f"{m['hazard_brier_ratio']:.4f}")
    if m.get("hamstring_recurrence"):
        low, high = result["gates"]["HAMSTRING_RECURRENCE_RANGE"]
        inside = low <= m["hamstring_recurrence"] <= high
        lines.append(f"    hamstring recurrence vs published 11.9%:         "
                     f"{m['hamstring_recurrence']:.3f}  "
                     f"{'inside' if inside else 'OUTSIDE'} [{low}, {high}]")

    lines.append("")
    lines.append(f"  VERDICT curve:  {result['verdict']['curve'].upper()} -- "
                 f"{result['verdict']['curve_reason']}")
    lines.append(f"  VERDICT hazard: {result['verdict']['hazard'].upper()} -- "
                 f"{result['verdict']['hazard_reason']}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    import argparse

    from Scripts import paths

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seasons", default=None, help="Fold range, e.g. 2019-2025.")
    parser.add_argument("--write", action="store_true",
                        help="Record the result in Scripts/lab/results.json.")
    args = parser.parse_args(argv)

    if args.seasons:
        first, _, last = args.seasons.partition("-")
        folds = range(int(first), int(last or first) + 1)
    else:
        folds = DEFAULT_FOLDS

    print(f"\n===== Injury model walk-forward: {min(folds)}-{max(folds)} =====")
    result = run(folds)
    print(report(result))

    if args.write:
        path = paths.REPO_ROOT / "Scripts" / "lab" / "results.json"
        payload = json.loads(path.read_text()) if path.is_file() else {}
        payload["injury"] = result
        path.write_text(json.dumps(payload, indent=2, sort_keys=True))
        print(f"\n  wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
