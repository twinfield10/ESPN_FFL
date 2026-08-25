"""Walk-forward scoring of the season-points distribution, against pre-committed gates.

For each season S: train on everything before S, predict S, and score the *distribution*
against what actually happened. No season ever sees itself, and every nuisance parameter
-- the conditional dispersion, the stat correlation, the games elasticity, the vacancy
shares -- is refitted inside the fold. The vacancy rule especially: it is fitted from
completed seasons and would otherwise be fitted on the fold it is scored against.

**What this cannot measure, said before what it can.** There is no historical draft board
(``docs/plans/25-results-backfill.md``): FantasyPros serves no season parameter, so no
pre-season blend survives for any season nobody archived one in. So G-D0's incumbent
width is the **live** board and the realised spread is history, and the projection under
test is the usage model's own line rather than the ``TRUE_`` blend -- the model's error
sits inside the measured spread, and a better projection would narrow it. Both
substitutions inflate the gap this gate reports, and neither can plausibly close it.

Usage::

    python -m Scripts.outcomes.backtest --seasons 2019-2025
    python -m Scripts.outcomes.backtest --write        # into Scripts/lab/results.json

See ``docs/plans/28-outcome-distributions.md``.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts.lab import registry as reg
from Scripts.outcomes import distribution as dist
from Scripts.outcomes import evidence as ev
from Scripts.outcomes import simulate as sim
from Scripts.outcomes import vacancy as vac
from Scripts.usage import backtest as ubt
from Scripts.usage import context as ctx
from Scripts.usage import role as rl
from Scripts.usage import season as sn

#: Seasons predicted, each from a model blind to it.
#:
#: **Starts at 2021, where :mod:`Scripts.usage.backtest` starts at 2019, and the two extra
#: folds are not available rather than not wanted.** The dispersions this scores are fitted
#: on held-out residuals: :func:`Scripts.usage.season._holdout_residuals` sets aside the
#: most recent :data:`Scripts.usage.season.HOLDOUT_SEASONS` training seasons and returns
#: nothing at all below four, so a 2019 fold trains on 2017-2018 and its model carries no
#: distribution to score. Running those folds would report a model with no interval as a
#: model whose interval failed.
DEFAULT_FOLDS: Tuple[int, ...] = tuple(range(2021, 2026))

#: Draws per fold.
#:
#: Lower than :data:`Scripts.outcomes.distribution.DEFAULT_SIMS` because a coverage
#: estimate is a proportion over hundreds of players, so the Monte Carlo error is
#: negligible beside the sampling error of the fold itself, and this runs seven folds
#: twice over.
FOLD_SIMS: int = 2000

#: Bins the calibration slope is measured over.
#:
#: Five, matching :data:`Scripts.injury.backtest.CALIBRATION_BINS`, so the two calibration
#: numbers in this repo mean the same thing.
CALIBRATION_BINS: int = 5

#: Depth rank at or below which a player is the room's incumbent.
#:
#: The control group for the false-positive clause: an entrenched starter has no vacancy
#: to inherit, so a redistribution rule should do nothing for him. If it improves his
#: coverage as much as a backup's, it has found variance in general.
STARTER_RANK: int = 1


def _fold_frame(season: int, league_key: str) -> Tuple[pl.DataFrame, sn.SeasonUsageModel]:
    """One fold's predictions, on the basis the dispersions were fitted on.

    :func:`Scripts.usage.backtest.run_season` emits ``USG_<stat>`` with ``expected_games``
    already inside it, which is exactly what the simulation wants: an opportunity
    multiplier of 1.0 then means "the season the model projected".
    :func:`Scripts.usage.project.to_full_slate` is deliberately **not** applied.

    Args:
        season: Season to predict.
        league_key: League whose scoring prices the comparison.

    Returns:
        tuple: The scored frame and the fold's model.
    """
    frame, model = ubt.run_season(season, league_key=league_key)
    frame = frame.filter(pl.col("usg_points").is_not_null())
    # The cohort plan 33's calibration is keyed on. Derived here rather than read,
    # because `attach_confidence` writes it onto the *artifact* and a backtest fold never
    # goes through that path.
    if {"is_rookie", "team_changed"}.issubset(frame.columns):
        frame = frame.with_columns(
            rl.cohort_expression().alias("usg_role_cohort"))
    return frame, model


def _depth_rank(frame: pl.DataFrame, season: int) -> np.ndarray:
    """Pre-season depth rank per row, for splitting backups from incumbents.

    Args:
        frame: The fold frame, carrying ``gsis_id``.
        season: Season whose pre-season chart to read.

    Returns:
        np.ndarray: Ranks, with :data:`Scripts.outcomes.simulate.UNLISTED_RANK` where the
        chart does not list the player -- buried, not a starter.
    """
    try:
        chart = (ctx.preseason_snapshot(ctx.load_depth_charts([season]), season)
                 .filter(pl.col("position").is_in(list(sim.ROOM_POSITIONS)))
                 .sort(["gsis_id", "depth_rank"])
                 .unique(subset=["gsis_id"], keep="first"))
    except FileNotFoundError:
        return np.full(frame.height, float(sim.UNLISTED_RANK))
    lookup = dict(chart.select("gsis_id", "depth_rank").rows())
    return np.array([float(lookup.get(p, sim.UNLISTED_RANK))
                     for p in frame["gsis_id"].to_list()])


def _calibration_slope(actual: np.ndarray, low: np.ndarray, high: np.ndarray,
                       centre: np.ndarray, bins: int = CALIBRATION_BINS) -> float:
    """Regress realised spread on predicted spread, by bin.

    Coverage says the interval is the right width **on average**. This says the width is
    right *where the model claims it is*: bin players by their own predicted spread, and
    in each bin compare the realised spread of ``actual`` against the predicted one. A
    slope near 1 means a player the model calls twice as uncertain really is.

    Args:
        actual: Realised season points.
        low: Predicted p10.
        high: Predicted p90.
        centre: Predicted p50.
        bins: Quantile bins.

    Returns:
        float: The slope, or NaN when there is too little to regress.
    """
    predicted = high - low
    keep = (np.isfinite(predicted) & np.isfinite(actual) & np.isfinite(centre)
            & (predicted > 0))
    if keep.sum() < bins * 10:
        return float("nan")
    predicted, realised = predicted[keep], actual[keep] - centre[keep]

    edges = np.quantile(predicted, np.linspace(0, 1, bins + 1))
    x, y = [], []
    for i in range(bins):
        block = (predicted >= edges[i]) & (predicted <= edges[i + 1])
        if block.sum() < 10:
            continue
        x.append(predicted[block].mean())
        # The realised p10-p90 width in this bin, against the width it was promised.
        y.append(float(np.percentile(realised[block], 90)
                       - np.percentile(realised[block], 10)))
    if len(x) < 3:
        return float("nan")
    return float(np.polyfit(x, y, 1)[0])


def _coverage(actual: np.ndarray, low: np.ndarray, high: np.ndarray) -> Tuple[float, int]:
    """Share of realised outcomes inside the predicted band, and how many rows."""
    keep = np.isfinite(actual) & np.isfinite(low) & np.isfinite(high)
    if not keep.any():
        return float("nan"), 0
    inside = (actual[keep] >= low[keep]) & (actual[keep] <= high[keep])
    return float(inside.mean()), int(keep.sum())


def run_fold(season: int, league_key: str = ubt.SCORING_LEAGUE,
             n_sims: int = FOLD_SIMS,
             weeks: Optional[pl.DataFrame] = None) -> Dict:
    """Score one held-out season, joint against independent.

    Args:
        season: Season to predict.
        league_key: League whose scoring prices the comparison.
        n_sims: Draws.
        weeks: Player-weeks for fitting the vacancy rule. None loads them.

    Returns:
        dict: Per-arm coverage and calibration, plus the backup and starter splits.
    """
    frame, model = _fold_frame(season, league_key)
    weeks = ev.load_weeks() if weeks is None else weeks

    # Fitted on completed seasons only. Without the filter the transfer rule would be
    # fitted on the fold it is about to be scored against.
    prior = weeks.filter(pl.col("season") < season)
    shares = vac.applied_rule(vac.fit(prior, positions=vac.TRANSFER_POSITIONS))

    baseline = sim.baseline_opportunity(frame)
    rooms = sim.room_order(frame, season, baseline=baseline)
    weights = ubt.scoring_weights(season, league_key)
    specs = {True: dist.player_spec(frame, model, conditional=True, use_cohort=True),
             False: dist.player_spec(frame, model, conditional=True, use_cohort=False)}
    spec = specs[True]
    slate = int(round(float(frame["y_games_available"].cast(pl.Float64).max()
                            or sn.DEFAULT_TARGET_SLATE)))

    actual = frame["actual_points"].cast(pl.Float64).to_numpy()
    # Cohorts come from the rooms themselves rather than from a ``depth_rank`` label,
    # and the distinction is not cosmetic: the 2016-2024 chart lists two or three
    # rank-1 backs in a third of rooms, so a ``depth_rank <= 1`` control group contains
    # players who *receive* transfers. That puts the treatment into the control and
    # biases the false-positive clause toward rejecting. `room_order` has already
    # resolved who leads; use its answer.
    lead = np.zeros(frame.height, dtype=bool)
    backup = np.zeros(frame.height, dtype=bool)
    for room in rooms:
        lead[list(room.players[:STARTER_RANK])] = True
        backup[list(room.players[STARTER_RANK:])] = True
    cohorts = {"all": np.ones(frame.height, dtype=bool),
               "backup": backup, "starter": lead}
    # Plan 33's own axis. The pooled coverage cannot answer whether the interval varies
    # along the *right* axis -- only the per-cohort split can, and it is the whole
    # question G-R2 asks.
    if "usg_role_cohort" in frame.columns:
        listed = np.array(frame["usg_role_cohort"].to_list())
        for name in ("settled", "mover", "rookie"):
            cohorts[name] = listed == name

    probabilities = rl.rank_probabilities()

    out: Dict[str, object] = {"season": season, "n": frame.height,
                              "slate": slate, "shares": shares,
                              "role_cells": len(probabilities)}
    # Three arms, differing in exactly one thing each. `independent` draws availability
    # per player and redistributes nothing. `joint` adds the room transfer with the depth
    # chart treated as fact. `role` adds plan 33's draw on top, so who leads the room is
    # itself uncertain -- a listed rookie rank-1 really leads only 35.6% of the time.
    out["cohort_share"] = spec.cohort_share
    for arm, transfer, drawn, cohort in (("joint", True, False, False),
                                         ("independent", False, False, False),
                                         ("cohort", True, False, True),
                                         ("role", True, True, True)):
        rng = np.random.default_rng(dist.DEFAULT_SEED + season)
        order = (sim.draw_role_order(rng, frame, rooms, n_sims, probabilities)
                 if drawn else None)
        modulation = sim.opportunity_multiplier(
            rng, frame, rooms, shares, model, slate, n_sims, baseline,
            transfer=transfer, role_order=order)
        sample = dist.sample_stats(specs[cohort], rng, n_sims=n_sims,
                                   mu_scale=modulation)
        points = dist.season_points(sample, weights)
        summary = dist.summarise(points, specs[cohort].positions,
                                 specs[cohort].has_projection)

        low = summary["pts_p10"].to_numpy()
        mid = summary["pts_p50"].to_numpy()
        high = summary["pts_p90"].to_numpy()
        for name, mask in cohorts.items():
            coverage, n = _coverage(actual[mask], low[mask], high[mask])
            out[f"{arm}_{name}_coverage"] = coverage
            out[f"{arm}_{name}_n"] = n
        out[f"{arm}_slope"] = _calibration_slope(actual, low, high, mid)
        out[f"{arm}_mean_ratio"] = float(
            np.nanmedian(mid[cohorts["all"]] / np.where(actual > 0, actual, np.nan)))
    return out


def board_interval_width(season: int, league_key: str) -> Dict:
    """G-D0's incumbent: how wide the board's own floor-to-ceiling actually is.

    Restated in code because the original measurement's script was never committed --
    ``git show --stat 21b302c`` touches two markdown files and nothing else -- so the
    17.5x has lived as a claim in a document rather than as something anyone can re-run.

    **Measured on the board's own subpopulation.** ``attach_source_spread`` leaves
    ``floor``/``ceiling`` null wherever fewer than two sources really priced a player, so
    the width exists for 522 of 2,504 rows on the 2026 GOP board. Comparing it against a
    simulated interval that exists for far more rows would be comparing two different
    populations.

    Args:
        season: Season whose board to read.
        league_key: League.

    Returns:
        dict: ``width`` (median ``(ceiling - floor) / TRUE_Points``), ``n``, and
        ``contained`` where measurable. Empty when no board is stored.
    """
    from Scripts import store

    try:
        board = pl.from_pandas(store.read_league_store(season, league_key, "board"))
    except (FileNotFoundError, KeyError):
        return {}
    if not {"floor", "ceiling", "TRUE_Points"}.issubset(board.columns):
        return {}

    scoped = board.filter(
        pl.col("floor").is_not_null() & pl.col("ceiling").is_not_null()
        & (pl.col("TRUE_Points") > 0))
    if "primaryPosition" in scoped.columns:
        scoped = scoped.filter(pl.col("primaryPosition").is_in(list(sim.ROOM_POSITIONS)))
    if not scoped.height:
        return {}
    width = ((scoped["ceiling"] - scoped["floor"]) / scoped["TRUE_Points"])
    return {"width": float(width.median()), "n": int(scoped.height)}


def ordering_change(season: int, league_key: str,
                    pool: int = 200) -> Dict:
    """G-D3: does ordering by ``p_top12`` disagree with ordering by mean points?

    **Measured within position, and that is not a detail.** ``p_top12`` is the chance a
    player finishes in his *position's* top N, so ranking the whole board by it ranks
    quarterbacks against running backs on two different scales -- the first attempt at
    this gate scored 85% of the pool as "moved" and was measuring nothing but that
    mismatch. A drafter comparing across positions has ``vor_rank`` for it; the question
    this gate asks is whether, among running backs, the distribution puts them in a
    different order than the mean does.

    Args:
        season: Season whose board to read.
        league_key: League.
        pool: How deep into ``vor_rank`` counts as draftable.

    Returns:
        dict: ``moved_share``, ``n``, and the per-position split. Empty when no board
        carries the columns.
    """
    from Scripts import store

    try:
        board = pl.from_pandas(store.read_league_store(season, league_key, "board"))
    except (FileNotFoundError, KeyError):
        return {}
    needed = {"pts_p50", "p_top12", "TRUE_Points", "vor_rank", "primaryPosition"}
    if not needed.issubset(board.columns):
        return {}

    scoped = board.filter(
        pl.col("p_top12").is_not_null() & pl.col("TRUE_Points").is_not_null()
        & (pl.col("vor_rank") <= pool))

    by_position, total, movers = {}, 0, 0
    for position in ("QB", "RB", "WR", "TE"):
        block = scoped.filter(pl.col("primaryPosition") == position)
        if block.height < 10:
            continue
        joined = (block.sort("TRUE_Points", descending=True).with_row_index("r_mean")
                  .select("player_id", "r_mean")
                  .join(block.sort("p_top12", descending=True).with_row_index("r_top")
                        .select("player_id", "r_top"), on="player_id")
                  # Cast before subtracting: the row index is unsigned, so a player who
                  # moves *up* underflows to four billion rather than going negative.
                  .with_columns((pl.col("r_top").cast(pl.Int64)
                                 - pl.col("r_mean").cast(pl.Int64)).alias("move")))
        moved = joined.filter(pl.col("move").abs() >= reg.MIN_PICK_MOVE).height
        by_position[position] = {"n": joined.height, "moved": moved}
        total += joined.height
        movers += moved

    if not total:
        return {}
    return {"moved_share": movers / total, "n": total, "moved": movers,
            "by_position": by_position}


def run(folds: Sequence[int] = DEFAULT_FOLDS,
        league_key: str = ubt.SCORING_LEAGUE,
        n_sims: int = FOLD_SIMS) -> Dict:
    """The walk-forward, and the verdicts it implies.

    Args:
        folds: Seasons to predict.
        league_key: League whose scoring prices the comparison.
        n_sims: Draws per fold.

    Returns:
        dict: ``ran_at``, ``folds``, ``metrics``, ``verdict`` and ``gates`` -- the
        thresholds echoed in, so a stored result reads without the code that produced it.
    """
    weeks = ev.load_weeks()
    rows = [run_fold(season, league_key, n_sims, weeks) for season in folds]

    def pooled(key: str) -> float:
        """Row-count-weighted mean, so a thin fold does not count as a fat one."""
        values = [(r.get(key), r.get(key.rsplit("_", 1)[0] + "_n", r.get("n", 0)))
                  for r in rows]
        pairs = [(v, w) for v, w in values
                 if v is not None and np.isfinite(v) and w]
        if not pairs:
            return float("nan")
        return float(sum(v * w for v, w in pairs) / sum(w for _, w in pairs))

    def gain(cohort: str) -> float:
        """How much closer to nominal 0.80 the joint arm gets, in percentage points."""
        joint = pooled(f"joint_{cohort}_coverage")
        independent = pooled(f"independent_{cohort}_coverage")
        if not (np.isfinite(joint) and np.isfinite(independent)):
            return float("nan")
        return 100.0 * (abs(independent - 0.80) - abs(joint - 0.80))

    metrics = {
        "coverage": pooled("joint_all_coverage"),
        "calibration_slope": float(np.nanmean([r["joint_slope"] for r in rows])),
        "independent_coverage": pooled("independent_all_coverage"),
        "backup_coverage_joint": pooled("joint_backup_coverage"),
        "backup_coverage_independent": pooled("independent_backup_coverage"),
        "starter_coverage_joint": pooled("joint_starter_coverage"),
        "starter_coverage_independent": pooled("independent_starter_coverage"),
        "backup_coverage_gain_pp": gain("backup"),
        "starter_coverage_gain_pp": gain("starter"),
        # Plan 33 phase 3's two candidate mechanisms, and the cohorts they were supposed
        # to help. Both are measured here rather than in a separate harness because they
        # are variants of this same distribution and share every other input.
        "role_coverage": pooled("role_all_coverage"),
        "cohort_coverage": pooled("cohort_all_coverage"),
        "role_backup_gain_pp": gain("backup"),
    }
    for name in ("settled", "mover", "rookie"):
        for arm in ("joint", "cohort", "role"):
            metrics[f"{arm}_{name}_coverage"] = pooled(f"{arm}_{name}_coverage")

    board = board_interval_width(max(folds) + 1, league_key)
    if board:
        realised = pooled("joint_all_coverage")
        metrics["board_interval_width"] = board["width"]
        metrics["board_interval_n"] = board["n"]
        del realised

    ordering = ordering_change(max(folds) + 1, league_key)
    if ordering:
        metrics["moved_share"] = ordering["moved_share"]
        metrics["moved_n"] = ordering["n"]

    call, reason = reg.outcome_verdict(metrics)
    joint_call, joint_reason = reg.joint_verdict(metrics)
    relevance_call, relevance_reason = reg.relevance_verdict(metrics)
    role_call, role_reason = reg.role_verdict(metrics)
    return {
        "ordering": ordering,
        "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "league": league_key,
        "n_sims": n_sims,
        "folds": rows,
        "metrics": metrics,
        "verdict": {"distribution": call, "distribution_reason": reason,
                    "joint": joint_call, "joint_reason": joint_reason,
                    "relevance": relevance_call,
                    "relevance_reason": relevance_reason,
                    "role": role_call, "role_reason": role_reason},
        "gates": {
            "OUTCOME_COVERAGE_RANGE": list(reg.OUTCOME_COVERAGE_RANGE),
            "OUTCOME_SLOPE_RANGE": list(reg.OUTCOME_SLOPE_RANGE),
            "MIN_JOINT_COVERAGE_GAIN_PP": reg.MIN_JOINT_COVERAGE_GAIN_PP,
            "MIN_VACANCY_SPECIFICITY_PP": reg.MIN_VACANCY_SPECIFICITY_PP,
            "MIN_MOVED_SHARE": reg.MIN_MOVED_SHARE,
            "MIN_PICK_MOVE": reg.MIN_PICK_MOVE,
            "MAX_ROLE_COVERAGE_ERROR": reg.MAX_ROLE_COVERAGE_ERROR,
        },
    }


def report(result: Dict) -> str:
    """Format a walk-forward result.

    Args:
        result: :func:`run` output.

    Returns:
        str: The printable report.
    """
    metrics = result["metrics"]
    lines = [
        "=== plan 28: season-points distribution, walk-forward ===",
        f"  league {result['league']}, {result['n_sims']} draws per fold",
        "",
        f"  {'season':<9}{'n':>6}{'cov joint':>11}{'cov indep':>11}{'slope':>8}",
    ]
    for row in result["folds"]:
        lines.append(
            f"  {row['season']:<9}{row['n']:>6}"
            f"{row['joint_all_coverage']:>11.3f}{row['independent_all_coverage']:>11.3f}"
            f"{row['joint_slope']:>8.2f}")

    lines += ["", "  --- G-D1: is the distribution publishable? ---",
              f"  80% interval coverage      {metrics['coverage']:.3f}   "
              f"(bar {reg.OUTCOME_COVERAGE_RANGE[0]:.2f}-"
              f"{reg.OUTCOME_COVERAGE_RANGE[1]:.2f})",
              f"  calibration slope          {metrics['calibration_slope']:.3f}   "
              f"(bar {reg.OUTCOME_SLOPE_RANGE[0]:.2f}-{reg.OUTCOME_SLOPE_RANGE[1]:.2f})",
              "",
              "  --- the false-positive clause, reported first ---",
              f"  {'cohort':<28}{'joint':>9}{'indep':>9}{'gain pp':>10}",
              f"  {'depth-rank >=2 RB/TE':<28}"
              f"{metrics['backup_coverage_joint']:>9.3f}"
              f"{metrics['backup_coverage_independent']:>9.3f}"
              f"{metrics['backup_coverage_gain_pp']:>10.1f}",
              f"  {'depth-rank 1 RB/TE':<28}"
              f"{metrics['starter_coverage_joint']:>9.3f}"
              f"{metrics['starter_coverage_independent']:>9.3f}"
              f"{metrics['starter_coverage_gain_pp']:>10.1f}",
              ]
    if "board_interval_width" in metrics:
        lines += ["", "  --- G-D0: the incumbent, restated ---",
                  f"  board (ceiling-floor)/TRUE_Points   "
                  f"{metrics['board_interval_width']:.3f}  "
                  f"(n={metrics['board_interval_n']})"]

    lines += ["", "  --- G-R2 (plan 33 phase 3): does role uncertainty help? ---",
              f"  {'cohort':<12}{'joint':>9}{'by cohort':>11}{'role draw':>11}"]
    for name in ("settled", "mover", "rookie"):
        if f"joint_{name}_coverage" not in metrics:
            continue
        lines.append(f"  {name:<12}{metrics[f'joint_{name}_coverage']:>9.3f}"
                     f"{metrics[f'cohort_{name}_coverage']:>11.3f}"
                     f"{metrics[f'role_{name}_coverage']:>11.3f}")
    lines.append(f"  {'all':<12}{metrics['coverage']:>9.3f}"
                 f"{metrics['cohort_coverage']:>11.3f}{metrics['role_coverage']:>11.3f}")

    ordering = result.get("ordering") or {}
    if ordering:
        lines += ["", "  --- G-D3: does the distribution reorder anything? ---",
                  f"  {'position':<10}{'draftable':>11}{'moved 12+':>11}{'share':>9}"]
        for position, block in ordering["by_position"].items():
            lines.append(f"  {position:<10}{block['n']:>11}{block['moved']:>11}"
                         f"{block['moved'] / max(block['n'], 1):>9.1%}")
        lines.append(f"  {'pooled':<10}{ordering['n']:>11}{ordering['moved']:>11}"
                     f"{ordering['moved_share']:>9.1%}")

    lines += ["",
              f"  VERDICT distribution: {result['verdict']['distribution'].upper()} -- "
              f"{result['verdict']['distribution_reason']}",
              f"  VERDICT joint:        {result['verdict']['joint'].upper()} -- "
              f"{result['verdict']['joint_reason']}",
              f"  VERDICT relevance:    {result['verdict']['relevance'].upper()} -- "
              f"{result['verdict']['relevance_reason']}",
              f"  VERDICT role (G-R2):  {result['verdict']['role'].upper()} -- "
              f"{result['verdict']['role_reason']}"]
    return "\n".join(lines)


def _write(result: Dict) -> str:
    """Persist under ``outcome_distribution`` in the committed lab ledger."""
    from Scripts.lab.run import RESULTS_PATH

    ledger = {}
    if RESULTS_PATH.exists():
        ledger = json.loads(RESULTS_PATH.read_text())
    ledger["outcome_distribution"] = result
    RESULTS_PATH.write_text(json.dumps(ledger, indent=2, sort_keys=True))
    return str(RESULTS_PATH)


def main(argv: Optional[List[str]] = None) -> int:
    """Run the walk-forward and print the report."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seasons", default=None,
                        help="inclusive range, e.g. 2019-2025")
    parser.add_argument("--league", default=ubt.SCORING_LEAGUE)
    parser.add_argument("--sims", type=int, default=FOLD_SIMS)
    parser.add_argument("--write", action="store_true",
                        help="persist into Scripts/lab/results.json")
    args = parser.parse_args(argv)

    folds = DEFAULT_FOLDS
    if args.seasons:
        first, last = (int(part) for part in args.seasons.split("-"))
        folds = tuple(range(first, last + 1))

    result = run(folds, league_key=args.league, n_sims=args.sims)
    print(report(result))
    if args.write:
        print(f"\n  wrote {_write(result)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
