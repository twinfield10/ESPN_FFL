"""Run an experiment through the real walk-forward and record what happened.

Deliberately thin. Every metric here comes out of
:mod:`Scripts.usage.backtest` -- the same folds, the same scoring registry, the
same veteran-rows-only restriction that plan 18 argued for. A lab that computes
its own version of the model's headline number will eventually disagree with the
model about it, and then nobody knows which one is right.

What this module adds is only: swap the configuration, run the folds, reduce the
frames to a JSON-shaped dict, and append it to ``Scripts/lab/results.json``.

Usage:
    python -m Scripts.lab.run --all
    python -m Scripts.lab.run --only contracts_x_moved
    python -m Scripts.lab.run --all --first 2022     # a faster, shallower sweep
"""

import argparse
import contextlib
import json
from datetime import datetime
from typing import Dict, List, Optional, Sequence

import polars as pl

from Scripts.lab import registry as reg
from Scripts.paths import REPO_ROOT
from Scripts.usage import backtest as bt
from Scripts.usage import features as ft
from Scripts.usage import season as sn

#: Where the ledger lives. Committed: it is the record, not a cache.
RESULTS_PATH = REPO_ROOT / "Scripts" / "lab" / "results.json"

#: Top-N per position, matching :func:`Scripts.usage.backtest.report` exactly so a
#: number in the HTML can be checked against a number in the console.
TOP_N = {"QB": 12, "RB": 24, "WR": 36, "TE": 12}


@contextlib.contextmanager
def configured(experiment: reg.Experiment):
    """Apply an experiment's regressor overrides for the duration of a block.

    The model reads :data:`Scripts.usage.season.VOLUME_REGRESSORS` at fit time and
    then carries the chosen names inside each ``VolumeFit``, which
    :meth:`SeasonUsageModel.predict_volume` iterates. So swapping the module
    constant is genuinely sufficient: predict follows the fit rather than the
    constant, and a model fitted under one setting cannot be predicted under
    another by accident.

    Restored in a ``finally`` because several experiments run in one process and a
    leaked override would silently contaminate every experiment after it.

    Args:
        experiment: The experiment whose overrides to apply.
    """
    saved_volume = sn.VOLUME_REGRESSORS
    saved_games = sn.GAMES_REGRESSORS
    saved_rates = ft.RATE_BASELINE_FEATURES
    saved_ridge = sn.RIDGE_ALPHA
    try:
        if experiment.ridge_alpha is not None:
            sn.RIDGE_ALPHA = experiment.ridge_alpha
        if experiment.volume_regressors is not None:
            sn.VOLUME_REGRESSORS = experiment.volume_regressors
        if experiment.games_regressors is not None:
            sn.GAMES_REGRESSORS = experiment.games_regressors
        if experiment.rate_baseline_features is not None:
            ft.RATE_BASELINE_FEATURES = experiment.rate_baseline_features
        yield
    finally:
        sn.VOLUME_REGRESSORS = saved_volume
        sn.GAMES_REGRESSORS = saved_games
        ft.RATE_BASELINE_FEATURES = saved_rates
        sn.RIDGE_ALPHA = saved_ridge


def metrics(frames: Sequence[pl.DataFrame]) -> Dict:
    """Reduce walk-forward frames to the numbers the ledger keeps.

    Veteran rows only for Spearman, MAE and top-N, for the reason
    :func:`Scripts.usage.backtest.report` gives at length: the naive baseline is
    last season's production carried forward, which is 0 for every rookie by
    construction, so pooling rookies in credits the model for covering them rather
    than for projecting anyone better.

    Args:
        frames: One scored frame per test season.

    Returns:
        dict: ``spearman`` and ``top_n`` per position, ``mae`` per stat with the
        naive comparison, plus coverage counts.
    """
    pooled = pl.concat(frames, how="diagonal")
    veterans = (pooled.filter(pl.col("usg_arm") == "veteran")
                if "usg_arm" in pooled.columns else pooled)

    spearman, naive_spearman, counts = {}, {}, {}
    for position in ft.MODELLED_POSITIONS:
        rows = veterans.filter(
            (pl.col("position") == position)
            & pl.col("usg_points").is_not_null()
            & pl.col("actual_points").is_not_null())
        usg = bt.spearman(rows, "usg_points", "actual_points")
        naive = bt.spearman(rows, "naive_points", "actual_points")
        if usg is None or naive is None:
            continue
        spearman[position] = usg
        naive_spearman[position] = naive
        counts[position] = rows.height

    top_n, naive_top_n = {}, {}
    for position, n in TOP_N.items():
        rows = veterans.filter(pl.col("position") == position)
        usg = bt.top_n_hit_rate(rows, "usg_points", "actual_points", n)
        naive = bt.top_n_hit_rate(rows, "naive_points", "actual_points", n)
        if usg is not None:
            top_n[position] = usg
        if naive is not None:
            naive_top_n[position] = naive

    mae = {}
    for stat, outcome in bt.OUTCOME_COLUMNS.items():
        predicted = f"{sn.USAGE_PREFIX}{stat}"
        naive_column = f"{ft.LAG1_PREFIX}act_{stat}_pg"
        if predicted not in pooled.columns or outcome not in pooled.columns:
            continue
        rows = veterans.filter(pl.col(predicted).is_not_null()
                               & pl.col(outcome).is_not_null())
        if rows.height < 10:
            continue
        entry = {
            "n": rows.height,
            "usg": rows.select(
                (pl.col(predicted) - pl.col(outcome)).abs().mean()).item(),
        }
        if naive_column in rows.columns:
            entry["naive"] = rows.select(
                ((pl.col(naive_column) * pl.col("expected_games"))
                 - pl.col(outcome)).abs().mean()).item()
        mae[stat] = entry

    return {
        "spearman": spearman,
        "naive_spearman": naive_spearman,
        "spearman_n": counts,
        "top_n": top_n,
        "naive_top_n": naive_top_n,
        "mae": mae,
        "subpopulations": subpopulation_spearman(veterans),
        "rostered": pooled.height,
        "projected": pooled.filter(pl.col("usg_points").is_not_null()).height,
        "arms": {row["usg_arm"]: row["len"] for row in
                 pooled.group_by("usg_arm").len().to_dicts()}
        if "usg_arm" in pooled.columns else {},
    }


#: Subpopulations a feature might help without moving the pooled number.
#:
#: Pooled Spearman is the right gate and the wrong diagnostic. A feature aimed at
#: players who changed teams is being asked about roughly a sixth of the rows; if it
#: helps them a lot and everyone else not at all, the pooled figure shows almost
#: nothing and "reject" is a true answer to a question nobody asked. These slices
#: are the questions the hypotheses were actually about.
#:
#: The first two are plan 18's two surviving thin-evidence flags, measured there at
#: +32% and +42% median rank error -- the populations the model is already known to
#: be worst on, and so the ones where a new feature has room to work.
SUBPOPULATIONS: Dict[str, pl.Expr] = {
    "changed_teams": pl.col("team_changed").fill_null(False),
    "thin_prior_season": pl.col(f"{ft.LAG1_PREFIX}games").fill_null(0) < 8,
    "settled": ~pl.col("team_changed").fill_null(False),
}


def subpopulation_spearman(veterans: pl.DataFrame) -> Dict[str, Dict]:
    """Within-position Spearman on each slice of :data:`SUBPOPULATIONS`.

    Pooled across positions rather than reported per position, because the slices
    are small -- roughly a sixth of rows change teams -- and a per-position split
    would put tight ends below the ten-row floor :func:`backtest.spearman` enforces.
    Ranking is still done within position first, so a slice's number means the same
    thing as the headline one.

    Args:
        veterans: Pooled veteran rows across folds.

    Returns:
        dict: Slice name to ``{"n": ..., "usg": ..., "naive": ...}``, omitting any
        slice too small to correlate.
    """
    out: Dict[str, Dict] = {}
    for name, condition in SUBPOPULATIONS.items():
        if "team_changed" in str(condition) and "team_changed" not in veterans.columns:
            continue
        rows = veterans.filter(
            condition
            & pl.col("usg_points").is_not_null()
            & pl.col("actual_points").is_not_null())
        if rows.height < 30:
            continue
        # Rank within position, then correlate the ranks pooled. Comparing a
        # quarterback's points to a receiver's directly would measure positional
        # scoring differences rather than the model.
        ranked = rows.with_columns(
            pl.col("usg_points").rank().over("position").alias("_ru"),
            pl.col("naive_points").rank().over("position").alias("_rn"),
            pl.col("actual_points").rank().over("position").alias("_ra"),
        )
        out[name] = {
            "n": rows.height,
            "usg": float(ranked.select(pl.corr("_ru", "_ra")).item() or 0.0),
            "naive": float(ranked.select(pl.corr("_rn", "_ra")).item() or 0.0),
        }
    return out


def run_experiment(experiment: reg.Experiment, seasons: Sequence[int],
                   league_key: str = bt.SCORING_LEAGUE) -> Dict:
    """Execute one experiment across the walk-forward.

    Args:
        experiment: What to run.
        seasons: Test seasons, each trained on everything before it.
        league_key: League whose scoring prices the comparison.

    Returns:
        dict: The ledger entry, including the experiment's own description so the
        report never has to join two files to render a row.
    """
    frames = []
    with configured(experiment):
        for season in seasons:
            scored, _ = bt.run_season(
                season, league_key=league_key,
                feature_kwargs=experiment.feature_kwargs or None)
            frames.append(scored)

    entry = {
        "name": experiment.name,
        "hypothesis": experiment.hypothesis,
        "source": experiment.source,
        "note": experiment.note,
        "feature_kwargs": dict(experiment.feature_kwargs),
        "volume_regressors": list(experiment.volume_regressors or reg.BASE_VOLUME),
        "games_regressors": list(experiment.games_regressors or reg.BASE_GAMES),
        "seasons": list(seasons),
        "ran_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    entry.update(metrics(frames))
    return entry


def load_results() -> Dict:
    """Read the ledger, or an empty one."""
    if not RESULTS_PATH.is_file():
        return {"experiments": {}, "rejected_before_plan_22": []}
    with RESULTS_PATH.open() as handle:
        return json.load(handle)


def prune_orphans(results: Dict) -> List[str]:
    """Drop ledger entries whose experiment no longer exists in the registry.

    The ledger is the registry's output, so an entry with no experiment behind it is
    stale by definition -- it was produced by a version of the question that is no
    longer being asked. Left in place they accumulate silently and the report starts
    showing results nothing can reproduce, which is worse than showing nothing.

    Args:
        results: The whole ledger. Mutated in place.

    Returns:
        list: The names removed.
    """
    known = {experiment.name for experiment in reg.EXPERIMENTS}
    orphans = [name for name in results.get("experiments", {}) if name not in known]
    for name in orphans:
        del results["experiments"][name]
    return orphans


def save_results(results: Dict) -> None:
    """Write the ledger, sorted and indented so its diffs are readable."""
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_PATH.open("w") as handle:
        json.dump(results, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.lab.run",
        description="Run feature experiments through the season walk-forward.")
    parser.add_argument("--all", action="store_true", help="run every experiment")
    parser.add_argument("--only", action="append", default=[],
                        help="run one experiment by name; repeatable")
    parser.add_argument("--first", type=int, default=bt.DEFAULT_TEST_SEASONS[0])
    parser.add_argument("--last", type=int, default=bt.DEFAULT_TEST_SEASONS[-1])
    parser.add_argument("--league", default=bt.SCORING_LEAGUE)
    parser.add_argument("--reverdict", action="store_true",
                        help="re-apply the decision rule to the stored ledger "
                             "without re-running anything")
    args = parser.parse_args(argv)

    if args.reverdict:
        results = load_results()
        for name in prune_orphans(results):
            print(f"  dropped {name} — no longer in the registry")
        base = results["experiments"]["baseline"]
        for name, entry in sorted(results["experiments"].items()):
            if name == "baseline":
                continue
            call, reason = reg.verdict(base, entry)
            entry["verdict"], entry["verdict_reason"] = call, reason
            print(f"  {name} ... {call.upper()}: {reason}")
        save_results(results)
        print(f"\nwrote {RESULTS_PATH.relative_to(REPO_ROOT)}")
        return 0

    if not args.all and not args.only:
        parser.error("pass --all or --only NAME")

    seasons = list(range(args.first, args.last + 1))
    queue = ([e for e in reg.EXPERIMENTS if e.name != "everything_that_passed"]
             if args.all else [reg.by_name(name) for name in args.only])

    results = load_results()
    results.setdefault("experiments", {})
    for name in prune_orphans(results):
        print(f"  dropped {name} — no longer in the registry")

    # The baseline has to run first and in the same process: it is the comparison,
    # and comparing against a stored one from a different data pull would silently
    # attribute a data refresh to a feature.
    if args.all and queue and queue[0].name != "baseline":
        queue.insert(0, reg.by_name("baseline"))

    print(f"Walk-forward {seasons[0]}..{seasons[-1]}, scored with "
          f"{args.league}'s rules.\n")
    for experiment in queue:
        print(f"  {experiment.name} ...", end="", flush=True)
        entry = run_experiment(experiment, seasons, args.league)
        results["experiments"][experiment.name] = entry

        base = results["experiments"].get("baseline")
        if base and experiment.name != "baseline":
            call, reason = reg.verdict(base, entry)
            entry["verdict"], entry["verdict_reason"] = call, reason
            print(f" {call.upper()}: {reason}")
        else:
            print(" recorded")

    save_results(results)
    print(f"\nwrote {RESULTS_PATH.relative_to(REPO_ROOT)}")
    print("render the ledger with: python -m Scripts.lab.report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
