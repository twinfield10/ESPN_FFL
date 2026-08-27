"""Can the blend weights be fitted rather than chosen? Measured twice now, still no.

``WEIGHTS`` in :mod:`Scripts.projection_utils` states a **rule** rather than a table:
one equal vote per source that has an opinion, made literal by renormalisation.
Plan 03 step 3 asked for that rule to be replaced by weights fitted against 2025
actuals. This module is the measurement that answers it, and the answer is no.

**This is the second attempt, and the first one's reasons no longer hold.** The
original run concluded "the data does not identify them" on three grounds: the
all-sources-real sample collapsed to 265-401 rows, the sources were collinear, and
the season question was not the weekly question. The first ground is now obsolete --
a free FantasyPros account lifted that source from 13% real to 90% on the key stats
(``docs/STATE_OF_THE_REPO.md``), so the fittable sample is 4x larger. Re-run with
that data and a corrected method, the verdict survives, but the *reasons* are
different and worth stating precisely, because each one kills a different candidate:

1. **Re-weighting the four universal sources is not identified.** Out of sample the
   per-stat fits run -1.4% to +2.4% and disagree between folds by up to **1.00** on
   a simplex whose equal point is 0.25. More data did not help, because the cause is
   collinearity rather than sample size: plan 16's G0 measured FantasyPros'
   residuals at +0.988 against ESPN's, and no procedure can split weight between two
   near-copies.
2. **The one large, stable signal is about a source that casts no weekly vote.**
   Dropping ``USG`` from the weekly blend is worth -1.9% to -23.5%, which dwarfs
   everything else here -- and it is unactionable twice over. TOMCAT has no weekly
   head (``docs/plans/19-weekly-usage-model.md`` is not started), so ``USG_Points``
   was null 3,602 of 3,602 times on Knights_FFL 2025 and the weight already
   renormalises away. And the ``USG_`` column measured here is the deliberately
   crude trailing baseline in :mod:`Scripts.usage.baseline`, not the shipped season
   head. What the number *is* good for is a pre-registered warning for plan 19: most
   of it is availability, not accuracy -- it shrinks to -0.8% to -1.2% once the
   population is players who actually took a snap.
3. **The one stable per-stat signal was an artifact of the estimator's own
   fallback, and it is the reason :func:`degenerate_rows` exists.** Zeroing
   BetOnline on the touchdown stats looked like a genuine, identified, fold-stable
   -5.3% -- until the segment was walked finely. At ``t=0.999`` the same weight
   vector is worth -1.43%; at ``t=1.0`` it is worth -5.31%. The difference is not
   weighting. An **exact** zero collapses the renormalised denominator on 979 rows,
   and :func:`Scripts.projection_utils.compute_weighted_stats` then falls back to
   the face-value sum -- so the "gain" is the blend switching itself off on a rare
   count, which MAE rewards. Production never reaches that branch, because ESPN is
   never imputed and always carries weight; only a fitting harness can.

**And the weights that would matter most cannot be scored here at all.** ``USG`` and
``DST`` vote only on the season board, and there is no historical season blend to
score against -- plan 18 records that as a permanent limitation of the data.

Two methodological points, both of which changed the answer and neither of which was
in the first attempt:

* **Fit the estimator that ships.** The original fitted a plain linear combination
  on rows where every source was real, which is 1.9-29.7% of rows and selects the
  covered stars. :func:`predict` reproduces ``compute_weighted_stats`` cell for cell
  instead -- renormalising over whatever is real on each row, with its face-value
  fallback -- so a source is identified from every coverage pattern it appears in,
  over the 78-93% of rows carrying at least two real sources.
* **Score against a store rebuilt by current code.** The 2025 store as it stood was
  built 2026-08-24; plan 34's touchdown split and plan 35's de-vig landed on
  2026-08-27. Measured on the stale store, BetOnline over-projected touchdowns by
  21% and "drop BetOnline" looked like a finding. Rebuilt, that column moved to 0.88
  of consensus -- the bias reversed sign. A weight fitted to the old store would
  have been fitted to a defect the pipeline had already fixed, pointing the wrong
  way. Plan 35's de-vig **cannot** be replayed on 2025 at all: the archives hold
  post-conversion ``proj_*`` and the raw prices are gone.

Usage::

    python -m Scripts.lab.blend
    python -m Scripts.lab.blend --season 2025 --population played
    python -m Scripts.lab.blend --no-save
"""

import argparse
from typing import Callable, Dict, List, NamedTuple, Optional, Sequence, Tuple

import numpy as np
import polars as pl
from scipy.optimize import minimize

from Scripts.lab import registry as reg
from Scripts.lab.run import RESULTS_PATH, load_results, save_results
from Scripts.paths import REPO_ROOT
from Scripts.projection_utils import IMPUTED_SUFFIX, WEIGHTS
from Scripts.usage import evalset as es

#: The four sources that project every position, and the ones a weekly re-weight
#: could actually move.
UNIVERSAL: Tuple[str, ...] = ("ESPN", "FP", "PINNY", "BOL")

#: The internal model's prefix. Carried so its equal vote can be *measured*, not
#: because a weekly one exists -- see reason 2 in the module docstring.
USAGE: str = "USG"

#: Every source this module scores.
SOURCES: Tuple[str, ...] = UNIVERSAL + (USAGE,)

#: Fewest rows in a fold before a stat is worth fitting.
MIN_FOLD_ROWS: int = 200

#: How far two folds' fitted weights may disagree before the fit is describing the
#: half it was fitted on rather than the sources.
#:
#: Pre-registered at 0.10 before the sweep ran, and sized off the quantity itself:
#: on a simplex over four sources the equal-vote point is 0.25, so a fold-to-fold
#: swing of 0.10 is 40% of a source's entire nominal weight. A fit that moves a
#: source by that much between two halves of one season has not measured the
#: source. Measured, the free fits run to 1.00 -- the whole simplex -- so this
#: threshold is not the binding constraint, which is worth knowing: the failure is
#: not marginal.
MAX_FOLD_DISAGREEMENT: float = 0.10

#: Points on the segment from the equal-vote rule to the free fit.
#:
#: The 0.999 point is not padding. It is the control that separates a weighting
#: result from the degenerate fallback: it is numerically indistinguishable from the
#: free fit as a set of *ratios*, but no weight is exactly zero, so the renormalised
#: denominator never collapses. Where the two disagree, the gain is
#: :func:`degenerate_rows`, not the weights.
SEGMENT: Tuple[float, ...] = (0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9,
                              0.99, 0.999, 1.0)

#: Populations, in the order the tables print. ``all`` includes byes and inactives,
#: which is the population a projection is published over; ``played`` is the one
#: that isolates accuracy from availability. Both are reported because for the USG
#: arm the gap between them *is* the finding.
POPULATIONS: Tuple[str, ...] = ("all", "team_played", "played")

#: Fold definitions. Two, because they fail differently and a candidate has to
#: survive both: ``odd/even`` interleaves so both halves see the same weeks of the
#: season, and ``early/late`` is time-ordered, which is the honest shape for a
#: forecast and the one the free fits fail hardest.
SPLITS: Dict[str, Callable[[np.ndarray], Tuple[np.ndarray, np.ndarray]]] = {
    "odd/even": lambda week: (week % 2 == 1, week % 2 == 0),
    "early/late": lambda week: (week <= 9, week >= 10),
}


class Design(NamedTuple):
    """One stat's fitting problem.

    Attributes:
        sources: Source prefixes, in column order.
        values: ``(n, k)`` projections, **including imputed cells** -- production's
            face-value fallback reads them, so dropping them would not reproduce it.
        real: ``(n, k)`` float 0/1 provenance mask.
        actual: ``(n,)`` realised stat line.
        week: ``(n,)`` week number, for splitting.
    """

    sources: List[str]
    values: np.ndarray
    real: np.ndarray
    actual: np.ndarray
    week: np.ndarray


def design(frame: pl.DataFrame, stat: str,
           sources: Sequence[str] = SOURCES) -> Design:
    """Assemble one stat's fitting problem from an evaluation frame.

    Args:
        frame: Frame from :func:`build`.
        stat: ESPN stat name.
        sources: Source prefixes to include. A source with no column is skipped,
            which is the same thing ``compute_weighted_stats`` does.

    Returns:
        Design: With one column per source that has one.
    """
    names, values, real = [], [], []
    for source in sources:
        column = f"{source}_{stat}"
        if column not in frame.columns:
            continue
        names.append(source)
        values.append(frame[column].cast(pl.Float64).fill_null(0.0).to_numpy())
        real.append(frame.select(es.real_mask(frame, source, stat).alias("m"))["m"]
                    .fill_null(False).to_numpy())
    return Design(
        sources=names,
        values=np.column_stack(values),
        real=np.column_stack(real).astype(float),
        actual=frame[f"act_{stat}"].cast(pl.Float64).fill_null(0.0).to_numpy(),
        week=frame["week"].to_numpy(),
    )


def take(problem: Design, mask: np.ndarray) -> Design:
    """One fold of a design."""
    return Design(problem.sources, problem.values[mask], problem.real[mask],
                  problem.actual[mask], problem.week[mask])


def predict(weights: np.ndarray, problem: Design) -> np.ndarray:
    """Production's estimator, cell for cell.

    Mirrors :func:`Scripts.projection_utils.compute_weighted_stats`: renormalise
    over the sources that are real on this row, and where that denominator is zero
    fall back to the face-value sum over **all** columns, imputed ones included.

    That fallback is not a detail. It is the branch a fitted weight vector can
    exploit -- see :func:`degenerate_rows` -- and reproducing it is the difference
    between measuring the blend and measuring an estimator nothing ships.
    """
    numerator = (problem.values * problem.real) @ weights
    denominator = problem.real @ weights
    live = denominator > 1e-12
    return np.where(live,
                    np.divide(numerator, np.where(live, denominator, 1.0)),
                    problem.values @ weights)


def mae(weights: np.ndarray, problem: Design) -> float:
    """Mean absolute error of ``weights`` on ``problem``."""
    return float(np.abs(predict(weights, problem) - problem.actual).mean())


def degenerate_rows(weights: np.ndarray, problem: Design) -> int:
    """Rows where ``weights`` collapses the renormalised denominator to zero.

    The guard that caught this module's most convincing false positive. A weight
    vector with an **exact** zero on the only sources that are real for a row leaves
    no denominator, so :func:`predict` takes production's face-value branch and the
    blend stops blending. On a rare count -- touchdowns, interceptions -- that
    lowers MAE, so an optimiser walks straight into it and reports a large, stable,
    fold-consistent "improvement" that is nothing of the kind.

    Production cannot reach this: ESPN carries a non-zero weight and is never
    imputed, so the denominator is always at least its share. Only a fit that is
    free to zero sources can, which is why this is measured here and not there.

    Returns:
        int: Row count. Compare against the equal-vote vector's count, which is 0.
    """
    return int(((problem.real @ weights) <= 1e-12).sum())


def equal_weights(problem: Design) -> np.ndarray:
    """The shipped rule as a vector: one equal vote per source with a column.

    Equality is the assertion, not the value -- ``compute_weighted_stats``
    renormalises, so it only ever sees ratios. ``1/k`` is therefore the shipped
    ``WEIGHTS`` restricted to this design, whenever those weights are equal across
    the sources present, which :func:`shipped_is_equal` checks.
    """
    return np.full(len(problem.sources), 1.0 / len(problem.sources))


def shipped_is_equal(sources: Sequence[str]) -> bool:
    """Whether ``WEIGHTS`` really is equal over these sources.

    Guards the baseline. Every number this module reports is a change *against the
    equal-vote rule*, so if someone sets a non-equal weight the comparison silently
    stops being the one described. Then this returns False and :func:`run` says so.
    """
    weights = [WEIGHTS["default"].get(source, 0.0) for source in sources]
    return bool(weights) and max(weights) - min(weights) < 1e-12


def _simplex(theta: np.ndarray) -> np.ndarray:
    """Softmax onto the simplex, stabilised.

    The subtraction is load-bearing rather than tidy. Without it the optimiser
    pushes a coordinate far enough that ``exp`` overflows to ``inf``, ``inf/inf``
    gives ``nan``, and the search returns a weight vector of ``nan`` that scores as
    a finite improvement further up the call stack.
    """
    z = np.concatenate([[0.0], np.clip(theta, -30.0, 30.0)])
    z = z - z.max()
    e = np.exp(z)
    return e / e.sum()


def fit(problem: Design, prior: Optional[np.ndarray] = None, lam: float = 0.0,
        restarts: int = 6, seed: int = 0) -> np.ndarray:
    """Minimise MAE over the simplex.

    Non-negative and summing to one because a negative weight on a forecaster is a
    bet against them rather than a blend, and because renormalisation means only
    ratios reach the output anyway.

    Args:
        problem: The design to fit.
        prior: Shrinkage target. Defaults to the equal-vote vector.
        lam: Shrinkage strength. 0 fits freely.
        restarts: Random restarts, on top of the equal-weight start. MAE on a
            renormalised ratio is not convex, so a single start finds a local
            minimum and reports it as the fit.
        seed: Restart seed, so a reported number reproduces.

    Returns:
        np.ndarray: Weights, summing to 1.
    """
    k = len(problem.sources)
    target = equal_weights(problem) if prior is None else np.asarray(prior, float)

    def objective(theta: np.ndarray) -> float:
        weights = _simplex(theta)
        value = mae(weights, problem)
        return value + lam * float(np.sum((weights - target) ** 2)) if lam else value

    rng = np.random.default_rng(seed)
    starts = [np.zeros(k - 1)] + [rng.normal(0, 1.5, k - 1)
                                  for _ in range(restarts - 1)]
    best, best_value = _simplex(np.zeros(k - 1)), np.inf
    for start in starts:
        for method, extra in (("Nelder-Mead", {"xatol": 1e-8, "fatol": 1e-10}),
                              ("Powell", {"xtol": 1e-8, "ftol": 1e-10})):
            result = minimize(objective, start, method=method,
                              options={"maxiter": 8000, **extra})
            if np.isfinite(result.fun) and result.fun < best_value:
                weights = _simplex(result.x)
                if np.all(np.isfinite(weights)):
                    best, best_value = weights, float(result.fun)
    return best


def two_fold(problem: Design, split: str,
             sources: Sequence[str] = SOURCES) -> Optional[Dict]:
    """Fit each half, score the other, and walk the segment between rule and fit.

    Symmetric on purpose: fitting on one half and scoring the other twice, then
    averaging, uses every row for both jobs and removes the arbitrary choice of
    which half is training.

    Args:
        problem: The design to evaluate.
        split: A key of :data:`SPLITS`.
        sources: Unused; kept so callers can read the signature as the design's.

    Returns:
        dict | None: Fitted weights per fold, their disagreement, the segment's
        out-of-sample MAE change against the equal-vote rule, and the degeneracy
        count at each end. None when either fold is too small.
    """
    left, right = SPLITS[split](problem.week)
    if left.sum() < MIN_FOLD_ROWS or right.sum() < MIN_FOLD_ROWS:
        return None

    folds = [(take(problem, left), take(problem, right)),
             (take(problem, right), take(problem, left))]
    fitted = [fit(train) for train, _ in folds]
    equal = equal_weights(problem)

    segment: Dict[str, float] = {}
    degenerate: Dict[str, int] = {}
    for t in SEGMENT:
        changes, worst = [], 0
        for weights, (_, score) in zip(fitted, folds):
            candidate = (1 - t) * equal + t * weights
            base = mae(equal, score)
            changes.append(100 * (mae(candidate, score) - base) / base
                           if base else 0.0)
            worst = max(worst, degenerate_rows(candidate, score))
        segment[str(t)] = float(np.mean(changes))
        degenerate[str(t)] = worst

    return {
        "sources": list(problem.sources),
        "n": [int(left.sum()), int(right.sum())],
        "fold_weights": [dict(zip(problem.sources, np.round(w, 3).tolist()))
                         for w in fitted],
        "fold_disagreement": float(np.abs(fitted[0] - fitted[1]).max()),
        "segment_mae_change_pct": segment,
        "segment_degenerate_rows": degenerate,
        "free_fit_mae_change_pct": segment[str(SEGMENT[-1])],
        "near_free_mae_change_pct": segment["0.999"],
    }


def usage_vote_cost(problem: Design, split: str) -> Optional[Dict]:
    """What the internal model's equal vote costs, holding everything else equal.

    The contrast that turned out to carry almost all of this module's measured
    movement, and the one it cannot act on. Compares the equal-vote rule over the
    universal sources against the equal-vote rule over those *plus* ``USG``, on one
    fixed population, so nothing but the vote changes.

    Returns:
        dict | None: MAE change from adding the usage vote, negative meaning the
        vote helps. None when ``USG`` has no column.
    """
    if USAGE not in problem.sources:
        return None
    keep = [i for i, source in enumerate(problem.sources) if source != USAGE]
    without = Design([problem.sources[i] for i in keep], problem.values[:, keep],
                     problem.real[:, keep], problem.actual, problem.week)
    with_usage = mae(equal_weights(problem), problem)
    without_usage = mae(equal_weights(without), without)
    return {
        "mae_with_usage_vote": with_usage,
        "mae_without_usage_vote": without_usage,
        "adding_usage_mae_change_pct":
            100 * (with_usage - without_usage) / without_usage
            if without_usage else None,
    }


def build(season: int, stats: Optional[Sequence[str]] = None) -> pl.DataFrame:
    """The evaluation frame: every source's line, the outcome, population flags.

    Built the same way :mod:`Scripts.usage.gates` builds it, including asking the
    usage baseline about the evaluation set's own player-weeks rather than the weeks
    it happens to have rows for -- otherwise the model is only scored on players who
    turned out to play.

    Args:
        season: Season to score.
        stats: ESPN stat names. Defaults to the usage model's stats, which are the
            ones every source has a column for.

    Returns:
        pl.DataFrame: Pooled evaluation frame with ``USG_`` and population flags.
    """
    from Scripts.usage.baseline import fit_seasons, predict_season
    from Scripts.usage.nflverse import USAGE_STATS, seasons_available

    stats = list(USAGE_STATS) if stats is None else list(stats)
    frame, _ = es.build_eval_set(season, stats=stats)
    train = list(seasons_available(range(2016, season)))
    baseline = fit_seasons(train, stats=stats)
    predictions = predict_season(baseline, season, grid=es.usage_grid(frame))
    return es.attach_usage(frame, predictions, season=season)


def _population(frame: pl.DataFrame, name: str) -> pl.DataFrame:
    """Restrict to one population."""
    if name == "all":
        return frame
    if name == "team_played":
        return frame.filter(pl.col("team_played"))
    if name == "played":
        return frame.filter(pl.col("played"))
    raise ValueError(f"unknown population {name!r}; expected one of {POPULATIONS}")


def verdict(entries: Sequence[Dict]) -> Dict:
    """Apply the decision rule to a population's results, mechanically.

    Four clauses, and a candidate has to clear all of them. The first three are the
    lab's existing thresholds, reused rather than re-chosen. The fourth is this
    module's own, and it is the one that killed the most convincing candidate.

    **The accuracy clauses are scored at ``t=0.999``, not at the free fit.** That is
    the correction that makes this rule coherent, and it was written after watching
    the first version pass its own mean-gain clause at -1.11% while the degeneracy
    clause failed at 912 rows -- which is to say, it rewarded the artifact it had
    just detected. The honest weighting gain is the one measured where no weight is
    exactly zero, so that is what the bar applies to; the free fit is reported
    beside it, and the difference between them is the size of the illusion.

    Args:
        entries: :func:`two_fold` results for one population and split.

    Returns:
        dict: Each clause's measured value, whether it passed, and the overall
        verdict.
    """
    if not entries:
        return {"pass": False, "why": "no stat had two large enough folds"}

    free = [e["free_fit_mae_change_pct"] for e in entries]
    near = [e["near_free_mae_change_pct"] for e in entries]
    disagreement = max(e["fold_disagreement"] for e in entries)
    degenerate = max(e["segment_degenerate_rows"][str(SEGMENT[-1])]
                     for e in entries)

    clauses = {
        "mean_gain": {
            "value": float(np.mean(near)),
            "bar": -reg.MAX_MEAN_MAE_INCREASE_PCT,
            "pass": float(np.mean(near)) <= -reg.MAX_MEAN_MAE_INCREASE_PCT,
            "what": "mean per-stat out-of-sample MAE change at t=0.999, must beat "
                    f"the rule by {reg.MAX_MEAN_MAE_INCREASE_PCT}%",
        },
        "worst_stat": {
            "value": float(np.max(near)),
            "bar": reg.MAX_STAT_MAE_INCREASE_PCT,
            "pass": float(np.max(near)) <= reg.MAX_STAT_MAE_INCREASE_PCT,
            "what": "no single stat may worsen by more than "
                    f"{reg.MAX_STAT_MAE_INCREASE_PCT}%",
        },
        "stability": {
            "value": disagreement,
            "bar": MAX_FOLD_DISAGREEMENT,
            "pass": disagreement <= MAX_FOLD_DISAGREEMENT,
            "what": "two folds of one season must agree on the weights within "
                    f"{MAX_FOLD_DISAGREEMENT}",
        },
        "not_degenerate": {
            "value": degenerate,
            "bar": 0,
            "pass": degenerate == 0,
            "what": "the fit may not win by collapsing the renormalised "
                    "denominator; compare the free fit against t=0.999",
        },
    }
    return {
        "clauses": clauses,
        "pass": all(c["pass"] for c in clauses.values()),
        "free_fit_mean_pct": float(np.mean(free)),
        "near_free_mean_pct": float(np.mean(near)),
        "degeneracy_gap_pct": float(np.mean(free)) - float(np.mean(near)),
    }


def run(season: int = 2025) -> Dict:
    """Every population, every split, every stat the evaluation set can speak for.

    Args:
        season: Season whose stores the evaluation set pools.

    Returns:
        dict: The ledger entry, keyed by population then split.
    """
    frame = build(season)
    stats = sorted({column.split("_", 1)[1] for column in frame.columns
                    if column.startswith("act_")})

    results: Dict = {"season": season, "stats": stats, "populations": {},
                     "shipped_rule_is_equal": None, "pooled_rows": frame.height}

    for population in POPULATIONS:
        subset = _population(frame, population)

        # The usage contrast is a property of the population, not of the fold split:
        # it compares two fixed weight vectors and fits nothing. Computed once here
        # rather than inside the split loop, which reported it identically twice.
        usage: Dict = {}
        for stat in stats:
            problem = design(subset, stat)
            if results["shipped_rule_is_equal"] is None:
                results["shipped_rule_is_equal"] = shipped_is_equal(problem.sources)
            cost = usage_vote_cost(problem, "n/a")
            if cost:
                usage[stat] = cost

        per_split: Dict = {}
        for split in SPLITS:
            entries = []
            for stat in stats:
                entry = two_fold(design(subset, stat, UNIVERSAL), split)
                if entry is None:
                    continue
                entry["stat"] = stat
                entries.append(entry)
            per_split[split] = {"stats": entries, "verdict": verdict(entries)}

        results["populations"][population] = {
            "rows": subset.height, "splits": per_split, "usage_vote": usage}
    return results


def render(results: Dict, population: str) -> str:
    """One population's tables, as text."""
    block = results["populations"][population]
    out = [f"\n=== {population}  ({block['rows']:,} rows) " + "=" * 24]
    for split, payload in block["splits"].items():
        entries = payload["stats"]
        if not entries:
            out.append(f"\n  split {split}: no stat had two large enough folds")
            continue
        sources = entries[0]["sources"]
        out.append(f"\n  split {split} -- re-weighting {'/'.join(sources)}, "
                   f"out of sample against the equal-vote rule\n")
        out.append(f"  {'stat':<22}{'t=0.999':>9}{'t=1.0':>8}{'gap':>7}"
                   f"{'fold gap':>10}{'degen':>7}   fitted weights")
        for entry in entries:
            free = entry["free_fit_mae_change_pct"]
            near = entry["near_free_mae_change_pct"]
            weights = " ".join(
                f"{k}={v:.2f}" for k, v in entry["fold_weights"][0].items())
            out.append(
                f"  {entry['stat']:<22}{near:>+8.2f}%{free:>+7.2f}%"
                f"{free - near:>+7.2f}{entry['fold_disagreement']:>10.2f}"
                f"{entry['segment_degenerate_rows'][str(SEGMENT[-1])]:>7,}"
                f"   {weights}")

        decision = payload["verdict"]
        out.append(f"\n  verdict: {'PASS' if decision['pass'] else 'FAIL'}")
        for name, clause in decision["clauses"].items():
            mark = "pass" if clause["pass"] else "FAIL"
            out.append(f"    {mark}  {name:<16}{clause['value']:>10.2f} "
                       f"(bar {clause['bar']})  -- {clause['what']}")
        out.append(f"    the free fit reads {decision['free_fit_mean_pct']:+.2f}% "
                   f"and t=0.999 reads {decision['near_free_mean_pct']:+.2f}%; "
                   f"{decision['degeneracy_gap_pct']:+.2f} of it is the "
                   f"collapsed denominator, not the weights")

    usage = block.get("usage_vote") or {}
    changes = [v["adding_usage_mae_change_pct"] for v in usage.values()
               if v["adding_usage_mae_change_pct"] is not None]
    if changes:
        out.append(
            f"\n  adding the USG equal vote moves MAE {min(changes):+.2f}% to "
            f"{max(changes):+.2f}% (mean {float(np.mean(changes)):+.2f}%) -- "
            f"unactionable twice\n  over: TOMCAT has no weekly head, so the weight "
            f"already renormalises away, and this\n  column is the crude trailing "
            f"baseline rather than the shipped season head. Read it\n  against the "
            f"same line on the 'played' population: what shrinks between them is "
            f"availability,\n  not accuracy.")
    return "\n".join(out)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.lab.blend",
        description="Test whether the blend weights can be fitted from outcomes.")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--population", choices=POPULATIONS + ("every",),
                        default="every")
    parser.add_argument("--no-save", action="store_true",
                        help="print without writing the ledger")
    args = parser.parse_args(argv)

    results = run(args.season)
    print(f"Blend-weight identification, {args.season} evaluation set "
          f"({results['pooled_rows']:,} player-weeks pooled)")
    if not results["shipped_rule_is_equal"]:
        print("\n  WARNING: WEIGHTS is no longer equal over the universal sources, "
              "so the\n  baseline below is not the rule this module was written to "
              "test.")
    for population in POPULATIONS:
        if args.population in (population, "every"):
            print(render(results, population))

    if args.no_save:
        return 0
    ledger = load_results()
    ledger["blend_weights"] = results
    save_results(ledger)
    print(f"\nwrote {RESULTS_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
