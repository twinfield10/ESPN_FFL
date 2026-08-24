"""The go/no-go gates for the usage models. Run this before building features.

``docs/plans/16-usage-data-layer.md`` step 0. Three questions, in the order that
decides whether the next two plans are worth writing:

* **G0 -- independence.** Are usage residuals materially less correlated with
  ESPN's than ESPN's are with FantasyPros'? If a usage model is re-deriving what
  ESPN already knows, a blend gets heavier rather than better and the whole
  workstream is a rounding error. The same matrix was taken to answer
  ``docs/plans/20-consensus-sources.md``'s question, because the marginal value of
  source *k+1* is roughly one minus its correlation with the rest.

  **That second use was withdrawn on 2026-08-24 and the matrix re-run the same day.**
  It scores only non-imputed cells, and FantasyPros had been real for 60 players -- ten
  per position, everything it served without an account -- so its column was sampled
  from the band where every source agrees. At 80.1% coverage FantasyPros' marginal value
  roughly doubles (+0.027 to +0.058; +0.109 to +0.180 partialled) and it is no longer
  the least independent source -- Pinnacle is. **G0's own verdict strengthened**, from
  +0.318 to +0.371 partialled, because the old FantasyPros column was largely ESPN
  imputed through the mean and was therefore counting ESPN twice against the model.
  Plan 20, which rested on the old ranking, is retired.
* **G1 -- accuracy.** Does adding ``USG_`` reduce blended per-stat MAE?
* **G2 -- draft.** Does within-position ranking against realised season points
  improve? That is the board's objective and it is not the same test as G1.

Usage::

    python -m Scripts.usage.gates                            # 2025 holdout
    python -m Scripts.usage.gates --season 2025 --population played

Read-only, no network, 2.5s. It does need
``python -m Scripts.refresh --all --season <season>`` and
``Rscript R/GetUsage.R 2016 <season>`` to have run.

The 2026-08-06 result is written into the plan: **G0 passed, G1 and G2 failed on
the crude baseline, and the failure is almost entirely availability.** Re-run this
after each feature family -- it is the regression test for whether the feature
moved anything, and nothing goes into ``WEIGHTS`` until G1 turns.
"""

import argparse
import itertools
import sys
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts.projection_utils import IMPUTED_SUFFIX, WEIGHTS, compute_weighted_stats
from Scripts.usage import evalset
from Scripts.usage.baseline import USAGE_PREFIX, fit_seasons, predict_season
from Scripts.usage.evalset import SOURCES, attach_usage, build_eval_set, real_mask
from Scripts.usage.nflverse import ACTUAL_PREFIX, USAGE_STATS, load_meta

#: Every source the gates compare, the usage baseline last.
ALL_SOURCES: Tuple[str, ...] = SOURCES + (USAGE_PREFIX.rstrip("_"),)

#: Stats broken out in the printed tables. The same five
#: ``Scripts.projection_utils.print_coverage_report`` uses, so the coverage report
#: and the gates cannot disagree about which stats matter.
KEY_STATS: Tuple[str, ...] = (
    "passingYards", "passingTouchdowns", "rushingYards",
    "receivingYards", "receivingReceptions",
)

#: Weights to try for the fifth source in G1. The four existing weights sum to
#: 1.0, so ``0.2`` gives the usage model a one-sixth share where every source is
#: real -- see the renormalisation note in
#: :func:`Scripts.projection_utils.compute_weighted_stats`. Note the *effective*
#: share is larger than the nominal weight wherever the books have no line, since
#: renormalisation divides by the weight of the sources that are real: on a row
#: where only ESPN and the usage model are real, a nominal 0.1 is a third of the
#: blend.
USAGE_WEIGHTS: Tuple[float, ...] = (0.05, 0.1, 0.2, 0.3, 0.5)

#: Positions the draft gate ranks within. Kickers and D/ST are excluded because a
#: usage model emits nothing for them -- see the positional-coverage risk in the
#: plan.
DRAFT_POSITIONS: Tuple[str, ...] = ("QB", "RB", "WR", "TE")

#: Minimum weeks a player must appear in before his season totals are ranked.
#: Below this a season sum is mostly noise about when he was rostered.
MIN_DRAFT_WEEKS = 8


def _rule(title: str) -> None:
    """Print a section heading."""
    print(f"\n=== {title} ===")


def pearson(x: np.ndarray, y: np.ndarray) -> float:
    """Correlation of two vectors, tolerating degenerate input.

    Args:
        x: First vector.
        y: Second vector.

    Returns:
        float: Pearson r, or NaN when either vector is constant or too short.
    """
    if len(x) < 3 or np.std(x) == 0 or np.std(y) == 0:
        return float("nan")
    return float(np.corrcoef(x, y)[0, 1])


def partial_pearson(x: np.ndarray, y: np.ndarray, given: np.ndarray) -> float:
    """Correlation of ``x`` and ``y`` with ``given``'s linear effect removed.

    Every source's residual contains minus the actual outcome, and the outcome
    varies far more than any projection of it does, so *all* pairs of residuals
    correlate above 0.9 by construction. The plain correlation is still the right
    quantity for the gate -- the inflation is identical for every pair, so the
    comparison between pairs is sound -- but the levels are unreadable. Partialling
    the outcome out leaves the question "conditional on what actually happened, do
    these two still say the same thing", which is what "a genuinely different
    opinion" means.

    Args:
        x: First vector.
        y: Second vector.
        given: Vector to partial out.

    Returns:
        float: Partial correlation, or NaN on degenerate input.
    """
    if len(x) < 4:
        return float("nan")
    design = np.column_stack([np.ones(len(given)), given])
    def strip(v: np.ndarray) -> np.ndarray:
        coefficients, *_ = np.linalg.lstsq(design, v, rcond=None)
        return v - design @ coefficients
    return pearson(strip(x), strip(y))


def spearman(x: np.ndarray, y: np.ndarray) -> float:
    """Rank correlation, computed as Pearson on ranks.

    Written out rather than taken from scipy: scipy is installed here but is not
    in ``requirements.txt``, and this is four lines.

    Args:
        x: First vector.
        y: Second vector.

    Returns:
        float: Spearman rho, or NaN on degenerate input.
    """
    if len(x) < 3:
        return float("nan")
    rank = lambda v: pl.Series(v).rank("average").to_numpy()  # noqa: E731
    return pearson(rank(x), rank(y))


# --- G0: residual independence -------------------------------------------

def residuals(frame: pl.DataFrame, source: str, stat: str) -> pl.DataFrame:
    """A source's signed error on one stat, where it had a real line.

    Args:
        frame: Evaluation frame.
        source: Source prefix, e.g. ``"PINNY"``.
        stat: ESPN stat name.

    Returns:
        pl.DataFrame: ``week``, ``player_id``, ``resid`` and ``actual`` for the
        rows the source really covered.
    """
    actual = f"{ACTUAL_PREFIX}{stat}"
    return (
        frame.filter(real_mask(frame, source, stat) & pl.col(actual).is_not_null())
        .select(["week", "player_id",
                 (pl.col(f"{source}_{stat}") - pl.col(actual)).alias("resid"),
                 pl.col(actual).alias("actual")])
    )


def residual_correlations(frame: pl.DataFrame,
                          stats: Sequence[str],
                          sources: Sequence[str] = ALL_SOURCES
                          ) -> pl.DataFrame:
    """Pairwise residual correlation for every source pair and stat.

    Each pair is measured on the rows where **both** sources had a real line --
    pairwise rather than complete-case, because a complete-case matrix would be
    restricted to the ~8% of rows Pinnacle covers and would answer a different
    question.

    Args:
        frame: Evaluation frame with usage attached.
        stats: ESPN stat names.
        sources: Source prefixes.

    Returns:
        pl.DataFrame: ``stat``, ``a``, ``b``, ``n``, ``r`` and ``r_partial`` --
        the latter with the actual outcome partialled out, see
        :func:`partial_pearson`.
    """
    rows = []
    for stat in stats:
        for a, b in itertools.combinations(sources, 2):
            left = residuals(frame, a, stat).rename({"resid": "resid_a"})
            right = (residuals(frame, b, stat).rename({"resid": "resid_b"})
                     .drop("actual"))
            both = left.join(right, on=["week", "player_id"], how="inner")
            x = both["resid_a"].to_numpy()
            y = both["resid_b"].to_numpy()
            rows.append({
                "stat": stat, "a": a, "b": b, "n": both.height,
                "r": pearson(x, y),
                "r_partial": partial_pearson(x, y, both["actual"].to_numpy()),
            })
    return pl.DataFrame(rows)


def weighted_mean_r(pairs: pl.DataFrame, column: str = "r") -> Dict:
    """Average each pair's correlation across stats, weighting by rows measured.

    Weighting by ``n`` rather than taking a flat mean keeps a stat that only
    Pinnacle's 2.5% interception coverage supports from counting as much as
    receiving yards.

    Args:
        pairs: Output of :func:`residual_correlations`.
        column: ``"r"`` or ``"r_partial"``.

    Returns:
        dict: ``{(a, b): (mean r, total n)}``.
    """
    grouped = (
        pairs.filter(pl.col(column).is_not_nan() & pl.col(column).is_not_null())
        .group_by(["a", "b"])
        .agg([
            pl.col("n").sum().alias("n"),
            ((pl.col(column) * pl.col("n")).sum() / pl.col("n").sum()).alias("r"),
        ])
    )
    return {(row["a"], row["b"]): (row["r"], row["n"])
            for row in grouped.iter_rows(named=True)}


def print_pair_table(pairs: pl.DataFrame, stats: Sequence[str],
                     column: str = "r") -> None:
    """Print pairwise residual correlations, one row per source pair.

    Args:
        pairs: Output of :func:`residual_correlations`.
        stats: Stats to show as columns, in order.
        column: ``"r"`` or ``"r_partial"``.
    """
    lookup = {(r["a"], r["b"], r["stat"]): (r[column], r["n"])
              for r in pairs.iter_rows(named=True)}
    means = weighted_mean_r(pairs, column)

    header = (f"  {'pair':<14}" + "".join(f"{s[:11]:>12}" for s in stats)
              + f"{'mean':>10}{'n':>9}")
    print(header)
    print("  " + "-" * (len(header) - 2))
    for a, b in itertools.combinations(ALL_SOURCES, 2):
        cells = []
        for stat in stats:
            value = lookup.get((a, b, stat))
            cells.append(f"{value[0]:>+12.3f}" if value and value[0] == value[0]
                         else f"{'-':>12}")
        mean_r, total_n = means.get((a, b), (float("nan"), 0))
        print(f"  {a + '/' + b:<14}" + "".join(cells)
              + f"{mean_r:>+10.3f}{total_n:>9}")


def print_matrix(pairs: pl.DataFrame, column: str = "r") -> None:
    """Print the n-weighted mean residual-correlation matrix.

    This is the artifact step 0 asks to be written into the plan: it answers the
    independence gate and the consensus-sources question at once.

    Args:
        pairs: Output of :func:`residual_correlations`.
        column: ``"r"`` or ``"r_partial"``.
    """
    means = {key: value[0] for key, value in weighted_mean_r(pairs, column).items()}
    print(f"  {'':<8}" + "".join(f"{s:>9}" for s in ALL_SOURCES))
    for a in ALL_SOURCES:
        cells = []
        for b in ALL_SOURCES:
            if a == b:
                cells.append(f"{'1.000':>9}")
                continue
            value = means.get((a, b), means.get((b, a)))
            cells.append(f"{value:>+9.3f}" if value is not None else f"{'-':>9}")
        print(f"  {a:<8}" + "".join(cells))

    print("\n  independence, as 1 - mean r with the other four:")
    for source in ALL_SOURCES:
        others = [means.get((source, b), means.get((b, source)))
                  for b in ALL_SOURCES if b != source]
        others = [v for v in others if v is not None]
        if others:
            print(f"    {source:<8}{1 - float(np.mean(others)):>+8.3f}")


# --- G1: blended accuracy -------------------------------------------------

def standalone_mae(frame: pl.DataFrame, stats: Sequence[str],
                   sources: Sequence[str] = ALL_SOURCES) -> pl.DataFrame:
    """Each source's own MAE, restricted to the rows the usage model speaks about.

    Context for G1: a blend gets worse when a member is *much* less accurate than
    the rest, however independent its errors are. Without this table a negative G1
    is ambiguous between "the usage signal is redundant" and "this particular
    two-term regression is bad", and those have opposite implications.

    Each source is still measured only on its own real cells, so the ``n`` columns
    differ -- Pinnacle covers 8% of passing rows and ESPN 100%. That is a property
    of the data, not of the comparison, and hiding it would be worse.

    Args:
        frame: Evaluation frame with usage attached.
        stats: ESPN stat names.
        sources: Source prefixes.

    Returns:
        pl.DataFrame: ``stat`` then ``mae_<source>`` and ``n_<source>`` per source.
    """
    rows = []
    for stat in stats:
        actual = f"{ACTUAL_PREFIX}{stat}"
        usage_column = f"{USAGE_PREFIX}{stat}"
        if usage_column not in frame.columns:
            continue
        base = frame.filter(pl.col(actual).is_not_null()
                            & pl.col(usage_column).is_not_null())
        row: Dict[str, object] = {"stat": stat}
        for source in sources:
            column = f"{source}_{stat}"
            if column not in base.columns:
                row[f"mae_{source}"] = None
                row[f"n_{source}"] = 0
                continue
            subset = base.filter(real_mask(base, source, stat))
            row[f"mae_{source}"] = (
                float((subset[column] - subset[actual]).abs().mean())
                if subset.height else None
            )
            row[f"n_{source}"] = subset.height
        rows.append(row)
    return pl.DataFrame(rows)


def print_standalone_mae(table: pl.DataFrame,
                         sources: Sequence[str] = ALL_SOURCES) -> None:
    """Print the per-source MAE table.

    Args:
        table: Output of :func:`standalone_mae`.
        sources: Source prefixes, in order.
    """
    header = f"  {'stat':<22}" + "".join(f"{s:>10}{'n':>7}" for s in sources)
    print(header)
    print("  " + "-" * (len(header) - 2))
    for row in table.iter_rows(named=True):
        cells = ""
        for source in sources:
            value = row[f"mae_{source}"]
            cells += (f"{value:>10.3f}" if value is not None else f"{'-':>10}")
            cells += f"{row[f'n_{source}']:>7}"
        print(f"  {row['stat']:<22}{cells}")


def blend_mae(frame: pl.DataFrame, stats: Sequence[str],
              usage_weights: Sequence[float] = USAGE_WEIGHTS) -> pl.DataFrame:
    """Per-stat MAE of the blend, with and without the usage model.

    Runs the real blend rather than a reimplementation of it:
    :func:`Scripts.projection_utils.compute_weighted_stats` is the production
    path, including the renormalisation that drops an imputed source's weight. A
    fifth source is exactly the case the plan flags as changing that arithmetic,
    so it is the code under test here.

    Args:
        frame: Evaluation frame with usage attached.
        stats: ESPN stat names.
        usage_weights: Nominal weights to try for ``USG``.

    Returns:
        pl.DataFrame: ``stat``, ``n``, ``mae_4`` and one ``mae_w<weight>`` column
        per weight tried, plus ``best_weight`` and ``pct_change`` at that weight.
    """
    pandas_frame = frame.to_pandas()

    # Provenance for the fifth source: it abstains by returning null, which is the
    # same statement the other three make with a flag column.
    for stat in stats:
        column = f"{USAGE_PREFIX}{stat}"
        if column in pandas_frame.columns:
            pandas_frame[column + IMPUTED_SUFFIX] = pandas_frame[column].isna()

    four = compute_weighted_stats(pandas_frame.copy(), list(stats), WEIGHTS)
    blends = {}
    for weight in usage_weights:
        weights = {}
        for key, entry in WEIGHTS.items():
            weights[key] = dict(entry)
            weights[key]["USG"] = weight
        blends[weight] = compute_weighted_stats(pandas_frame.copy(), list(stats),
                                                weights)

    rows = []
    for stat in stats:
        actual = pandas_frame[f"{ACTUAL_PREFIX}{stat}"]
        # Evaluated where the usage model actually has an opinion. Elsewhere the
        # two blends are identical by construction, so including those rows only
        # dilutes the comparison toward zero.
        mask = actual.notna() & pandas_frame[f"{USAGE_PREFIX}{stat}"].notna()
        if not mask.any():
            continue
        row: Dict[str, object] = {"stat": stat, "n": int(mask.sum())}
        row["mae_4"] = float((four.loc[mask, f"TRUE_{stat}"] - actual[mask]).abs().mean())
        for weight, blended in blends.items():
            row[f"mae_w{weight}"] = float(
                (blended.loc[mask, f"TRUE_{stat}"] - actual[mask]).abs().mean())
        best = min(usage_weights, key=lambda w: row[f"mae_w{w}"])
        row["best_weight"] = best
        row["pct_change"] = round(
            100.0 * (row[f"mae_w{best}"] - row["mae_4"]) / row["mae_4"], 2)
        rows.append(row)
    return pl.DataFrame(rows)


def print_mae(table: pl.DataFrame, usage_weights: Sequence[float]) -> None:
    """Print the G1 table.

    Args:
        table: Output of :func:`blend_mae`.
        usage_weights: The weights it was built with.
    """
    header = (f"  {'stat':<22}{'n':>7}{'MAE 4-src':>11}"
              + "".join(f"{'w=' + str(w):>10}" for w in usage_weights)
              + f"{'best':>7}{'change':>9}")
    print(header)
    print("  " + "-" * (len(header) - 2))
    for row in table.iter_rows(named=True):
        cells = "".join(f"{row[f'mae_w{w}']:>10.3f}" for w in usage_weights)
        print(f"  {row['stat']:<22}{row['n']:>7}{row['mae_4']:>11.3f}{cells}"
              f"{row['best_weight']:>7}{row['pct_change']:>+8.2f}%")


# --- G2: the draft objective ---------------------------------------------

def draft_spearman(season: int, league_key: str, predictions: pl.DataFrame,
                   stats: Sequence[str], usage_weight: float = 0.2
                   ) -> List[Dict]:
    """Within-position rank correlation against realised season points.

    Sums each player's weekly blended projection over the season and ranks it
    against what he actually scored under that league's rules, with and without
    the usage model.

    **This is a proxy for the gate as written, not the gate itself.** The real
    draft test ranks *pre-season* season-long projections, and 2025's cannot be
    reconstructed: FantasyPros' URLs take no season parameter, and only BetOnline's
    season-long archive survives. Summed weekly projections use in-season
    information no drafter had. What it does measure honestly is whether usage
    improves *within-position ordering* over a season rather than week-level
    error, which is the part of G2 that differs from G1.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        predictions: Frame from :func:`Scripts.usage.baseline.predict_season`.
        stats: ESPN stat names the usage model emits.
        usage_weight: Nominal weight for the fifth source.

    Returns:
        list: One dict per position with ``n``, ``rho_4``, ``rho_5`` and ``delta``.
    """
    # Deferred: SLOT_BASE lives in the ESPN ingest module, which is several
    # seconds of imports that G0 and G1 have no use for.
    from Scripts import store as store_module
    from Scripts.crosswalk import id_map
    from Scripts.scoring import get_scoring_table
    from Scripts.scrape_player_stats import SLOT_BASE

    # From the registry, so this needs no network and cannot be told a different
    # story than the store was built with. SLOT_BASE because every stat a usage
    # model emits is an offensive one, which proj_to_score prices from the base
    # table -- the D/ST override applies to a slot no usage row can occupy.
    scoring = get_scoring_table(league_key=league_key, season=season,
                                slot=SLOT_BASE, verify=False)
    points_per_stat = {
        row["colName"]: float(row["points"])
        for _, row in scoring.iterrows()
        if isinstance(row["colName"], str)
    }

    frame = pl.read_parquet(
        store_module.require_artifact(season, league_key, "lineups"))
    usable = [s for s in stats
              if s in frame.columns and s in points_per_stat]
    if not usable:
        return []

    columns = ["week", "player_id", "player_name", "primaryPosition", "points",
               "TRUE_Points"]
    for stat in usable:
        for source in SOURCES:
            for candidate in (f"{source}_{stat}",
                              f"{source}_{stat}{IMPUTED_SUFFIX}"):
                if candidate in frame.columns:
                    columns.append(candidate)
    slim = frame.select([c for c in columns if c in frame.columns])

    mapping = id_map("espn_id", "gsis_id")
    slim = slim.with_columns(
        pl.col("player_id").cast(pl.Utf8)
        .replace_strict(mapping, default=None, return_dtype=pl.Utf8)
        .alias("gsis_id")
    )
    usage_columns = [f"{USAGE_PREFIX}{s}" for s in usable]
    slim = slim.join(predictions.select(["week", "gsis_id"] + usage_columns),
                     on=["week", "gsis_id"], how="left")

    pandas_frame = slim.to_pandas()
    for stat in usable:
        column = f"{USAGE_PREFIX}{stat}"
        pandas_frame[column + IMPUTED_SUFFIX] = pandas_frame[column].isna()

    five = {}
    for key, entry in WEIGHTS.items():
        five[key] = dict(entry)
        five[key]["USG"] = usage_weight

    four_blend = compute_weighted_stats(pandas_frame.copy(), usable, WEIGHTS)
    five_blend = compute_weighted_stats(pandas_frame.copy(), usable, five)

    # Only the delta matters, so the D/ST slot override never comes into it: every
    # stat here is offensive, which proj_to_score prices from the base table, and
    # a usage model emits nothing for a D/ST unit anyway.
    delta = sum(
        (five_blend[f"TRUE_{stat}"].fillna(0.0)
         - four_blend[f"TRUE_{stat}"].fillna(0.0)) * points_per_stat[stat]
        for stat in usable
    )
    pandas_frame["proj_4"] = pandas_frame["TRUE_Points"]
    pandas_frame["proj_5"] = pandas_frame["TRUE_Points"] + delta

    seasonal = (
        pl.from_pandas(pandas_frame)
        .group_by(["player_id", "player_name", "primaryPosition"])
        .agg([pl.len().alias("weeks"), pl.col("points").sum().alias("actual"),
              pl.col("proj_4").sum(), pl.col("proj_5").sum()])
        .filter(pl.col("weeks") >= MIN_DRAFT_WEEKS)
    )

    out = []
    for position in DRAFT_POSITIONS:
        group = seasonal.filter(pl.col("primaryPosition") == position)
        if group.height < 10:
            continue
        actual = group["actual"].to_numpy()
        rho_4 = spearman(group["proj_4"].to_numpy(), actual)
        rho_5 = spearman(group["proj_5"].to_numpy(), actual)
        out.append({"league": league_key, "position": position, "n": group.height,
                    "rho_4": rho_4, "rho_5": rho_5, "delta": rho_5 - rho_4})
    return out


# --- report ---------------------------------------------------------------

#: The populations the gates can measure on, and what each one asks.
POPULATIONS = {
    "all": "every pooled player-week, byes and inactives included",
    "team": "the player's team played that week -- excludes byes, which are public "
            "information the crude baseline has not been given",
    "played": "the player took offensive snaps -- also excludes inactives, which "
              "is the availability problem plan 19 picks up",
}


def _population(frame: pl.DataFrame, name: str) -> pl.DataFrame:
    """Restrict the evaluation frame to one population.

    Args:
        frame: Evaluation frame with usage attached.
        name: A key of :data:`POPULATIONS`.

    Returns:
        pl.DataFrame: The subset.

    Raises:
        KeyError: On an unknown population name.
    """
    if name not in POPULATIONS:
        raise KeyError(f"Unknown population {name!r}. Known: {sorted(POPULATIONS)}.")
    if name == "all":
        return frame
    if name == "team":
        return frame.filter(pl.col("team_played"))
    return frame.filter(pl.col("played"))


def run(season: int = 2025,
        train_seasons: Optional[Sequence[int]] = None,
        stats: Optional[Sequence[str]] = None,
        population: str = "team",
        usage_weight: float = 0.2) -> Dict:
    """Build the eval set, fit the baseline, and print all three gates.

    Args:
        season: Season to evaluate.
        train_seasons: Seasons to fit the baseline on. Defaults to every pulled
            season before ``season``.
        stats: ESPN stat names. Defaults to :data:`USAGE_STATS`.
        population: A key of :data:`POPULATIONS`. G0's headline is reported for
            all three regardless; this one carries the full tables.
        usage_weight: Nominal ``USG`` weight for G2.

    Returns:
        dict: The frames and tables produced, for a caller that wants the numbers
        rather than the printout.
    """
    stats = list(USAGE_STATS) if stats is None else list(stats)
    if train_seasons is None:
        from Scripts.usage.nflverse import seasons_available
        train_seasons = list(seasons_available(range(2016, season)))
    if not train_seasons:
        raise ValueError(
            f"No training seasons available before {season}. Pull them with "
            f"`Rscript R/GetUsage.R 2016 {season - 1}`."
        )

    print(f"Usage gates: {season} holdout, trained on "
          f"{min(train_seasons)}-{max(train_seasons)}")
    meta = load_meta(season)
    if meta:
        print(f"  {season} expected-production release "
              f"{meta['opportunity']['nflverse_timestamp']}, "
              f"pulled {meta['pulled_at']}")

    # --- evaluation set ---------------------------------------------------
    eval_set, report = build_eval_set(season, stats=stats)
    baseline = fit_seasons(train_seasons, stats=stats)
    # The grid is the eval set's own player-weeks, not the weeks the usage data
    # happens to have rows for -- otherwise the model is only asked about players
    # who turned out to play. See evalset.usage_grid.
    predictions = predict_season(baseline, season,
                                 grid=evalset.usage_grid(eval_set))
    eval_set = attach_usage(eval_set, predictions, season=season)

    _rule("1. Evaluation set")
    print(f"  {report['rows']} player-weeks pooled from "
          f"{len(report['leagues'])} league stores, "
          f"{report['with_gsis_id']} ({100 * report['with_gsis_id'] / report['rows']:.1f}%) "
          f"carry a gsis_id")
    print(f"  worst cross-league disagreement on a shared cell: "
          f"{report['worst_cross_league_disagreement']:.6f}")
    unscored = {k: v["unscored_stats"] for k, v in report["leagues"].items()
                if v["unscored_stats"]}
    if unscored:
        print(f"  stats some leagues do not score: {unscored}")
    for name in POPULATIONS:
        subset = _population(eval_set, name)
        print(f"  {name:<8}{subset.height:>6} rows  -- {POPULATIONS[name]}")

    measured = _population(eval_set, population)
    print(f"\n  full tables below are on the {population!r} population "
          f"({measured.height} rows)")

    print("\n  real (non-imputed) coverage, % of measured rows:")
    print("  " + str(evalset.coverage(measured, stats)).replace("\n", "\n  "))

    _rule("2. Sanity: ESPN's box-score actuals vs nflverse's")
    print("  mean absolute difference per player-week on played rows. Two "
          "independent\n  stat feeds, so a large gap here would invalidate every "
          "number below.\n")
    nflverse_actuals = (
        load_opportunity_actuals(season, stats)
        .rename({f"{ACTUAL_PREFIX}{s}": f"nfl_{s}" for s in stats})
    )
    checked = measured.join(nflverse_actuals, on=["week", "gsis_id"], how="inner")
    for stat in stats:
        difference = (checked[f"{ACTUAL_PREFIX}{stat}"] - checked[f"nfl_{stat}"]).abs()
        print(f"    {stat:<22}n={checked.height:>6}  mean |diff| = "
              f"{float(difference.mean()):.4f}  max = {float(difference.max()):.1f}")

    _rule("3. The usage baseline")
    print(baseline.summary())

    _rule("G0. Pairwise residual correlation, non-imputed cells only")
    pairs = residual_correlations(measured, stats)
    print_pair_table(pairs, KEY_STATS)
    print(f"\n  n-weighted mean over all {len(stats)} stats:\n")
    print_matrix(pairs)

    print("\n  the same, with the actual outcome partialled out -- every residual\n"
          "  contains minus the outcome, which is what puts all ten pairs above\n"
          "  0.85. See partial_pearson.\n")
    print_matrix(pairs, "r_partial")

    print("\n  the gate, across all three populations:\n")
    print(f"    {'population':<10}{'rows':>7}{'ESPN/USG':>11}{'ESPN/FP':>10}"
          f"{'ESPN/USG partial':>19}{'ESPN/FP partial':>18}")
    by_population = {}
    for name in POPULATIONS:
        subset = _population(eval_set, name)
        subset_pairs = (pairs if name == population
                        else residual_correlations(subset, stats))
        plain = weighted_mean_r(subset_pairs)
        partial = weighted_mean_r(subset_pairs, "r_partial")
        by_population[name] = {"pairs": subset_pairs, "plain": plain,
                               "partial": partial}
        print(f"    {name:<10}{subset.height:>7}"
              f"{plain[('ESPN', 'USG')][0]:>+11.3f}{plain[('ESPN', 'FP')][0]:>+10.3f}"
              f"{partial[('ESPN', 'USG')][0]:>+19.3f}"
              f"{partial[('ESPN', 'FP')][0]:>+18.3f}")

    _rule("G1. Accuracy")
    print("  a) each source alone, on the rows where the usage model has an "
          "opinion:\n")
    solo = standalone_mae(measured, stats)
    print_standalone_mae(solo)

    print(f"\n  b) the blend, four sources vs five ({population!r} population):\n")
    mae = blend_mae(measured, stats)
    print_mae(mae, USAGE_WEIGHTS)

    played_mae = None
    if population != "played":
        # Splits the two reasons a fifth source can hurt: being wrong about the
        # stat line, and being wrong about whether the player is on the field at
        # all. The crude baseline has no availability input whatsoever.
        played = _population(eval_set, "played")
        played_mae = blend_mae(played, stats)
        print("\n  c) the same on rows where the player took snaps -- availability "
              "removed:\n")
        print_mae(played_mae, USAGE_WEIGHTS)
        print("\n     and each source alone on those rows. ESPN and USG share a "
              "population\n     here, so that column pair is the one direct "
              "comparison:\n")
        print_standalone_mae(standalone_mae(played, stats))

    _rule("G2. Within-position Spearman against realised season points")
    print("  A proxy: summed weekly projections, not the pre-season board 2025 "
          "cannot\n  reconstruct. See draft_spearman's docstring.\n")
    draft_rows: List[Dict] = []
    for league_key in report["leagues"]:
        draft_rows += draft_spearman(season, league_key, predictions, stats,
                                     usage_weight=usage_weight)
    if draft_rows:
        table = pl.DataFrame(draft_rows)
        summary = (table.group_by("position")
                   .agg([pl.col("n").sum(), pl.col("rho_4").mean(),
                         pl.col("rho_5").mean(), pl.col("delta").mean()])
                   .sort("position"))
        print(f"  averaged over {table['league'].n_unique()} leagues, "
              f"USG weight {usage_weight}:\n")
        print(f"  {'pos':<6}{'n':>7}{'rho 4-src':>12}{'rho 5-src':>12}{'delta':>10}")
        for row in summary.iter_rows(named=True):
            print(f"  {row['position']:<6}{row['n']:>7}{row['rho_4']:>12.4f}"
                  f"{row['rho_5']:>12.4f}{row['delta']:>+10.4f}")

    return {"eval_set": eval_set, "baseline": baseline, "pairs": pairs,
            "by_population": by_population, "standalone_mae": solo, "mae": mae,
            "played_mae": played_mae, "draft": draft_rows, "report": report}


def load_opportunity_actuals(season: int, stats: Sequence[str]) -> pl.DataFrame:
    """nflverse's own actuals, for the cross-feed sanity check.

    Args:
        season: Season year.
        stats: ESPN stat names.

    Returns:
        pl.DataFrame: ``week``, ``gsis_id`` and ``act_<stat>`` per stat.
    """
    from Scripts.usage.nflverse import load_opportunity

    return load_opportunity([season], stats=stats).select(
        ["week", "gsis_id"] + [f"{ACTUAL_PREFIX}{s}" for s in stats])


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point. See ``python -m Scripts.usage.gates --help``."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.usage.gates",
        description="Measure whether a usage model would add anything to the "
                    "existing four-source blend.",
    )
    parser.add_argument("--season", type=int, default=2025,
                        help="holdout season (default: 2025)")
    parser.add_argument("--population", default="team", choices=sorted(POPULATIONS),
                        help="which player-weeks carry the full tables; G0's "
                             "headline is reported for all three either way "
                             "(default: team)")
    parser.add_argument("--usage-weight", type=float, default=0.2,
                        help="nominal USG weight for G2 (default: 0.2)")
    args = parser.parse_args(argv)

    run(season=args.season, population=args.population,
        usage_weight=args.usage_weight)
    return 0


if __name__ == "__main__":
    sys.exit(main())
