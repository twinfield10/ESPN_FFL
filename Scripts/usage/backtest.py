"""Walk-forward backtest for the season usage head.

    python -m Scripts.usage.backtest

For each season *S* in the test range: train on everything before *S*, predict *S*,
score against realised *S*. No exceptions, and no season ever sees itself -- the
positional baselines the efficiency features are shrunk toward are refitted per
season for the same reason the coefficients are.

**What this can and cannot measure.** Plan 18 asks for the blend scored with and
without ``USG_``, against ESPN's own season projection and the four-source
``TRUE_`` blend. Neither is reconstructable for a past season: FantasyPros' URLs
take no season parameter and only BetOnline's season-long archive survives, so
there are no historical pre-season projections to compare against. What is available
is the naive draft heuristic -- last season's production -- which is the baseline
that actually matters, because it is what a drafter does by default. The
against-the-blend comparison is G2 and belongs to the 2026 board.

Metrics, in plan 18's priority order:

- **within-position Spearman** against realised season points, which is what a board
  consumes -- ordering, not level
- **per-stat MAE**, which says which half of volume x efficiency was wrong
- **top-N hit rate**, because draft value concentrates at the top of each position

Fantasy points are scored offline from the committed scoring registry, so this runs
with no network.
"""

import argparse
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts.scoring import SLOT_BASE, get_scoring_table
from Scripts.usage import features as ft
from Scripts.usage import season as sn

#: Seasons the walk-forward tests. 2019 is the first with three prior seasons to
#: learn from, which is the shortest history the lag-2 features can use.
DEFAULT_TEST_SEASONS = tuple(range(2019, 2026))

#: Earliest season the features may look back to.
HISTORY_START = 2016

#: League whose scoring prices the backtest. One league, named, rather than an
#: invented "standard" scoring: this repo's whole architecture is that stat lines
#: are priced per league, and the registry has the real settings for every season.
#: Per-stat MAE below is scoring-agnostic and is the metric to read for the model
#: itself; the Spearman is what a board consumes and needs *a* scoring.
#:
#: Winfield Football because it is the **only** league with a registry entry for
#: every season 2016-2026 -- the others were only recorded from 2023 or 2024, since
#: the registry was built this cycle from live settings and ESPN does not serve a
#: 2019 scoring table for a league you did not have then. Picking any other league
#: would silently shorten the walk-forward.
SCORING_LEAGUE = "winfield_football"

#: Realised-total column per ESPN stat, from ``training_frame``.
OUTCOME_COLUMNS: Dict[str, str] = {
    "receivingYards": "y_tot_receiving_yards",
    "receivingReceptions": "y_tot_receptions",
    "receivingTouchdowns": "y_tot_receiving_tds",
    "rushingYards": "y_tot_rushing_yards",
    "rushingTouchdowns": "y_tot_rushing_tds",
    "passingYards": "y_tot_passing_yards",
    "passingTouchdowns": "y_tot_passing_tds",
    "passingInterceptions": "y_tot_passing_interceptions",
}


def scoring_weights(season: int,
                    league_key: str = SCORING_LEAGUE) -> Dict[str, float]:
    """Points per unit for each modelled stat, from the registry.

    Args:
        season: Season year, so a mid-history rule change is honoured.
        league_key: Config key of the league to price with.

    Returns:
        dict: ESPN stat name to points per unit, for the stats this model emits.
    """
    table = get_scoring_table(league_key=league_key, season=season,
                              slot=SLOT_BASE, verify=False)
    weights = {}
    for row in table.itertuples():
        name = getattr(row, "colName", None)
        if isinstance(name, str) and name in OUTCOME_COLUMNS:
            weights[name] = float(row.points)
    return weights


def points(frame: pl.DataFrame, columns: Dict[str, str],
           weights: Dict[str, float]) -> pl.Expr:
    """Dot a stat line with a scoring table.

    Args:
        frame: Frame holding the stat columns.
        columns: ESPN stat name to the column carrying it.
        weights: ESPN stat name to points per unit.

    Returns:
        pl.Expr: Fantasy points, treating an absent stat as zero.
    """
    terms = [pl.col(column).cast(pl.Float64).fill_null(0.0) * weights[stat]
             for stat, column in columns.items()
             if stat in weights and column in frame.columns]
    if not terms:
        return pl.lit(None, dtype=pl.Float64)
    expression = terms[0]
    for term in terms[1:]:
        expression = expression + term
    return expression


def spearman(frame: pl.DataFrame, a: str, b: str) -> Optional[float]:
    """Rank correlation, or None when there is too little to correlate.

    Args:
        frame: Source frame.
        a: First column.
        b: Second column.

    Returns:
        float | None: Spearman's rho, or None below 10 usable rows.
    """
    sub = frame.select(a, b).drop_nulls()
    if sub.height < 10:
        return None
    ranked = sub.select(pl.col(a).rank().alias("ra"), pl.col(b).rank().alias("rb"))
    value = ranked.select(pl.corr("ra", "rb")).item()
    return None if value is None else float(value)


def top_n_hit_rate(frame: pl.DataFrame, predicted: str, actual: str,
                   n: int) -> Optional[float]:
    """Share of the realised top ``n`` that the projection also put in its top ``n``.

    Computed **per season and then averaged**, not over the pooled frame. Pooling
    first and taking one top-12 compares a 2019 quarterback against a 2024 one and
    answers a question nobody asked -- the first version of this did exactly that,
    and its numbers moved with the scoring era rather than with the model.

    Args:
        frame: One position's rows, across seasons, carrying ``test_season``.
        predicted: Projection column.
        actual: Realised column.
        n: Cutoff, applied within each season.

    Returns:
        float | None: Mean hit rate in 0-1, or None when no season has ``n`` scored
        players.
    """
    rates = []
    for (season,), rows in frame.group_by(["test_season"], maintain_order=True):
        sub = rows.select("gsis_id", predicted, actual).drop_nulls()
        if sub.height < n:
            continue
        predicted_top = set(sub.sort(predicted, descending=True).head(n)["gsis_id"])
        actual_top = set(sub.sort(actual, descending=True).head(n)["gsis_id"])
        rates.append(len(predicted_top & actual_top) / n)
    return float(np.mean(rates)) if rates else None


def run_season(test_season: int, history_start: int = HISTORY_START,
               league_key: str = SCORING_LEAGUE,
               feature_kwargs: Optional[Dict[str, object]] = None,
               ) -> Tuple[pl.DataFrame, sn.SeasonUsageModel]:
    """Train on everything before ``test_season`` and predict it.

    Args:
        test_season: Season to predict.
        history_start: Earliest season the features may see.
        league_key: League whose scoring prices the comparison.
        feature_kwargs: Passed to :func:`Scripts.usage.season.training_frame`, for
            both the training and the test fold. Passing them to only one would fit
            on one feature set and predict with another, which is a bug rather than
            an experiment.

    Returns:
        tuple: The scored prediction frame, and the fitted model.

    Raises:
        FileNotFoundError: When a required pull is missing.
    """
    train_seasons = [s for s in range(history_start + 1, test_season)]
    train = sn.training_frame(train_seasons, history_start,
                              feature_kwargs=feature_kwargs)
    # Stamped here rather than inside `fit`, which takes it as an argument so a fit
    # is reproducible from its inputs alone.
    model = sn.fit(train, train_seasons,
                   fitted_at=datetime.now().astimezone().isoformat(timespec="seconds"))

    test = sn.training_frame([test_season], history_start,
                             feature_kwargs=feature_kwargs)

    # The slate the *test* season really offered, not the one 2026 will. The games
    # heads predict a share, so scoring a 2019 fold against 17 games would inflate
    # every prediction by a game -- the mirror image of the bias the share
    # normalisation removes. Measured from the fold's own data rather than assumed
    # from the year, which keeps 2022's two 16-game teams correct.
    slate = test.select(
        pl.col("y_games_available").cast(pl.Float64).max()).item()
    if not slate:
        slate = sn.DEFAULT_TARGET_SLATE

    # Every position, including the ones shipped code declines. The backtest is what
    # decides whether an arm is worth having, so it has to keep measuring the arms
    # that lost -- `ABSTAIN_POSITIONS` exists because of the quarterback row in this
    # table, and honouring the default here would erase the evidence for it.
    predicted = model.predict(test, abstain_positions=(), target_slate=slate)
    predicted = model.games_interval(predicted, target_slate=slate)
    predicted = model.stat_intervals(predicted)

    weights = scoring_weights(test_season, league_key)

    # An uninformative rookie projection: the positional rookie mean, carrying no
    # draft information at all. This is what the arm has to beat -- plan 18 puts the
    # burden of proof on draft capital, and "better than nothing" is only meaningful
    # against a guess that uses nothing.
    rookie_means = (
        train.filter(pl.col("is_rookie").fill_null(False)
                     & pl.col(f"{ft.LAG1_PREFIX}games").is_null())
        .group_by("position")
        .agg([pl.col(column).cast(pl.Float64).fill_null(0.0).mean()
              .alias(f"mean_{column}")
              for column in OUTCOME_COLUMNS.values() if column in train.columns])
    )
    predicted = predicted.join(rookie_means, on="position", how="left")

    # The naive draft heuristic: last season's production, unadjusted. It is what a
    # drafter does by default and the only historical baseline that survives -- see
    # the module docstring on why ESPN's and FantasyPros' past projections do not.
    prior_columns = {
        stat: f"{ft.LAG1_PREFIX}{'act_' + stat}_pg"
        for stat in OUTCOME_COLUMNS
    }
    prior_totals = {
        stat: column for stat, column in prior_columns.items()
        if column in predicted.columns
    }

    # `points` treats an absent stat as zero, which is right for a partial line -- a
    # receiver has no passing yards -- and wrong for no line at all. Without this
    # guard an abstention scores 0.0 rather than null, and the backtest reported
    # 100% coverage including every rookie the model deliberately said nothing
    # about.
    spoke = pl.any_horizontal(
        [pl.col(f"{sn.USAGE_PREFIX}{stat}").is_not_null()
         for stat in OUTCOME_COLUMNS
         if f"{sn.USAGE_PREFIX}{stat}" in predicted.columns]
    )

    return predicted.with_columns(
        pl.when(spoke).then(
            points(predicted,
                   {s: f"{sn.USAGE_PREFIX}{s}" for s in OUTCOME_COLUMNS}, weights)
        ).otherwise(None).alias("usg_points"),
        points(predicted, OUTCOME_COLUMNS, weights).alias("actual_points"),
        points(predicted,
               {stat: f"mean_{column}" for stat, column in OUTCOME_COLUMNS.items()},
               weights).alias("rookie_mean_points"),
        # Prior-season per-game production x this season's games, so the naive
        # baseline gets the same availability information the model has. Without
        # that it would be beaten on games played rather than on production, which
        # would flatter the model for the wrong reason.
        (points(predicted, prior_totals, weights)
         * pl.col("expected_games")).alias("naive_points"),
        pl.lit(test_season).cast(pl.Int32).alias("test_season"),
    ), model


def report(frames: Sequence[pl.DataFrame], positions=ft.MODELLED_POSITIONS) -> str:
    """Format the walk-forward results.

    Args:
        frames: One scored frame per test season.
        positions: Positions to report.

    Returns:
        str: The printable report.
    """
    pooled = pl.concat(frames, how="diagonal")
    lines = []

    # The USG-vs-naive tables run on **veteran rows only**, and the restriction is
    # load-bearing. The naive baseline is last season's production carried forward,
    # so for a rookie it is 0 by construction -- on all 1,497 rookie rows. Pooling
    # them in credits the model for *covering* rookies rather than for projecting
    # anyone more accurately, and it inflated every figure in this table: RB Spearman
    # read +0.149 pooled against +0.022 on the population where both can actually
    # speak. The rookie arm gets its own comparison below, against a baseline that
    # can answer.
    veterans = (pooled.filter(pl.col("usg_arm") == "veteran")
                if "usg_arm" in pooled.columns else pooled)

    lines.append("=== within-position Spearman vs realised season points ===")
    lines.append("  veteran rows only — the naive baseline cannot project a rookie")
    lines.append(f"  {'pos':<6}{'n':>6}{'USG':>10}{'naive':>10}{'delta':>10}")
    for position in positions:
        rows = veterans.filter((pl.col("position") == position)
                             & pl.col("usg_points").is_not_null()
                             & pl.col("actual_points").is_not_null())
        usg = spearman(rows, "usg_points", "actual_points")
        naive = spearman(rows, "naive_points", "actual_points")
        if usg is None or naive is None:
            lines.append(f"  {position:<6}{rows.height:>6}   too few rows")
            continue
        lines.append(f"  {position:<6}{rows.height:>6}{usg:>10.4f}{naive:>10.4f}"
                     f"{usg - naive:>+10.4f}")

    lines.append("")
    lines.append("=== per-stat MAE, veteran rows the model speaks for ===")
    lines.append(f"  {'stat':<24}{'n':>7}{'USG':>10}{'naive':>10}{'delta %':>10}")
    for stat, outcome in OUTCOME_COLUMNS.items():
        predicted_column = f"{sn.USAGE_PREFIX}{stat}"
        naive_column = f"{ft.LAG1_PREFIX}act_{stat}_pg"
        if predicted_column not in pooled.columns or outcome not in pooled.columns:
            continue
        rows = veterans.filter(pl.col(predicted_column).is_not_null()
                            & pl.col(outcome).is_not_null())
        if rows.height < 10:
            continue
        usg_mae = rows.select(
            (pl.col(predicted_column) - pl.col(outcome)).abs().mean()).item()
        if naive_column in rows.columns:
            naive_mae = rows.select(
                ((pl.col(naive_column) * pl.col("expected_games"))
                 - pl.col(outcome)).abs().mean()).item()
        else:
            naive_mae = None
        if naive_mae:
            delta = 100 * (usg_mae - naive_mae) / naive_mae
            lines.append(f"  {stat:<24}{rows.height:>7}{usg_mae:>10.2f}"
                         f"{naive_mae:>10.2f}{delta:>+10.1f}")
        else:
            lines.append(f"  {stat:<24}{rows.height:>7}{usg_mae:>10.2f}"
                         f"{'—':>10}{'—':>10}")

    lines.append("")
    lines.append("=== top-N hit rate vs realised, per season then averaged ===")
    lines.append("  veteran rows only, for the same reason as above")
    lines.append(f"  {'pos':<6}{'N':>4}{'USG':>10}{'naive':>10}")
    for position, n in (("QB", 12), ("RB", 24), ("WR", 36), ("TE", 12)):
        rows = veterans.filter(pl.col("position") == position)
        usg = top_n_hit_rate(rows, "usg_points", "actual_points", n)
        naive = top_n_hit_rate(rows, "naive_points", "actual_points", n)
        if usg is None:
            continue
        naive_text = f"{naive:>10.3f}" if naive is not None else f"{'—':>10}"
        lines.append(f"  {position:<6}{n:>4}{usg:>10.3f}{naive_text}")

    lines.append("")
    lines.append("=== coverage, by arm ===")
    total = pooled.height
    spoke = pooled.filter(pl.col("usg_points").is_not_null()).height
    lines.append(f"  {spoke} of {total} rostered player-seasons "
                 f"({100 * spoke / total:.1f}%) got a projection.")
    if "usg_arm" in pooled.columns:
        for (arm,), rows in pooled.group_by(["usg_arm"], maintain_order=False):
            lines.append(f"  {arm:<10}{rows.height:>6}")

    lines.append("")
    lines.append(report_games_interval(pooled))

    lines.append("")
    lines.append(report_stat_intervals(pooled))

    lines.append("")
    lines.append(report_rookie_arm(pooled, positions))
    return "\n".join(lines)


def report_stat_intervals(pooled: pl.DataFrame) -> str:
    """Coverage of each stat's predictive interval, against realised totals.

    Args:
        pooled: Walk-forward rows carrying ``USG_<stat>_low``/``_high`` and the
            ``y_tot_`` outcomes.

    Returns:
        str: A printable block.
    """
    lines = ["=== stat lines: predictive interval coverage (nominal 80%) ===",
             f"  {'stat':<24}{'n':>7}{'covered':>10}{'below':>9}{'above':>9}"]
    any_row = False

    for stat, outcome in sn.STAT_OUTCOMES.items():
        low, high = f"{sn.USAGE_PREFIX}{stat}_low", f"{sn.USAGE_PREFIX}{stat}_high"
        if any(c not in pooled.columns for c in (low, high, outcome)):
            continue
        rows = pooled.filter(pl.col(low).is_not_null()
                             & pl.col(outcome).is_not_null())
        if rows.is_empty():
            continue
        any_row = True
        n = rows.height
        inside = rows.filter((pl.col(outcome) >= pl.col(low))
                             & (pl.col(outcome) <= pl.col(high))).height
        below = rows.filter(pl.col(outcome) < pl.col(low)).height
        above = rows.filter(pl.col(outcome) > pl.col(high)).height
        lines.append(f"  {stat:<24}{n:>7}{100 * inside / n:>9.1f}%"
                     f"{100 * below / n:>8.1f}%{100 * above / n:>8.1f}%")

    if not any_row:
        return "=== stat lines: predictive interval coverage ===\n  not computed"

    lines.append("  counts read high for the same discreteness reason as games.")
    if not sn.ABSTAIN_POSITIONS:
        lines.append("  NOTE: `season.ABSTAIN_POSITIONS` is empty, so every row above "
                     "reaches a board.")
        lines.append("  `passingYards` covers 58.9% against a nominal 80% and misses "
                     "asymmetrically")
        lines.append("  (24.5% below p10, 16.6% above p90): too narrow *and* biased "
                     "high at quarterback.")
        lines.append("  This line used to read 'the passing rows are the arm shipped "
                     "code abstains on'.")
        lines.append("  That was true until 2026-08-07, when the depth chart entered "
                     "the veteran arm and")
        lines.append("  the abstention was lifted -- and the excuse outlived it in "
                     "this function and in")
        lines.append("  docs/STATE_OF_THE_REPO.md. See docs/plans/34-stat-first-audit.md "
                     "F5.")
    return "\n".join(lines)


def report_games_interval(pooled: pl.DataFrame) -> str:
    """Whether the games-played interval covers what it claims to.

    The only check that decides whether a predictive interval is worth printing. It
    reports realised coverage against the model's **own** implied coverage rather
    than against the nominal 80%, because on a discrete support those are not the
    same thing: ``games_low`` is the smallest integer whose cumulative probability
    reaches 0.10, and with eighteen attainable values each step carries several
    percent of mass, so the chosen cut points always exclude less than asked. Judging
    the model against 80% would report a well-calibrated distribution as badly
    over-wide.

    Args:
        pooled: Walk-forward rows carrying ``y_games``, ``games_low``, ``games_high``
            and ``games_implied_coverage``.

    Returns:
        str: A printable block, or a note when the columns are absent.
    """
    needed = ("y_games", "games_low", "games_high")
    if any(c not in pooled.columns for c in needed):
        return "=== games interval ===\n  not computed"

    rows = pooled.filter(
        pl.col("y_games").is_not_null() & pl.col("games_low").is_not_null())
    if rows.is_empty():
        return "=== games interval ===\n  no rows"

    inside = rows.filter((pl.col("y_games") >= pl.col("games_low"))
                         & (pl.col("y_games") <= pl.col("games_high")))
    below = rows.filter(pl.col("y_games") < pl.col("games_low"))
    above = rows.filter(pl.col("y_games") > pl.col("games_high"))

    n = rows.height
    lines = ["=== games played: predictive interval (Beta-Binomial, closed form) ==="]
    lines.append(f"  n={n}   realised coverage {100 * inside.height / n:.1f}%"
                 f"   below {100 * below.height / n:.1f}%"
                 f"   above {100 * above.height / n:.1f}%")
    if "games_implied_coverage" in rows.columns:
        implied = rows["games_implied_coverage"].mean()
        if implied is not None:
            lines.append(f"  the model's own claim for these cut points is "
                         f"{100 * implied:.1f}% -- that, not the nominal 80%, is "
                         f"what it should be judged against")
    lines.append("  (a discrete support cannot hit 10% exactly, so an integer "
                 "p10/p90 always excludes less than asked)")
    return "\n".join(lines)


def report_rookie_arm(pooled: pl.DataFrame,
                      positions=ft.MODELLED_POSITIONS) -> str:
    """Does the draft-capital arm beat saying nothing?

    Plan 18 puts the burden of proof here, so the comparison is against a projection
    that carries **no** draft information: the positional rookie mean. If the arm
    cannot beat that, it has no information a board can use, and abstaining is
    strictly better than adding an uninformative fifth source to ``WEIGHTS``.

    Args:
        pooled: Scored frames from every test season, concatenated.
        positions: Positions to report.

    Returns:
        str: The printable verdict.
    """
    rookies = pooled.filter(pl.col("usg_arm") == "rookie") \
        if "usg_arm" in pooled.columns else pooled.head(0)
    if rookies.is_empty():
        return "=== rookie arm ===\n  no rookie rows projected."

    lines = ["=== rookie arm: draft capital vs an uninformative guess ===",
             f"  {'pos':<6}{'n':>6}{'rho arm':>10}{'rho mean':>10}{'Δ':>9}"
             f"{'MAE arm':>10}{'MAE mean':>10}"]
    for position in positions:
        rows = rookies.filter((pl.col("position") == position)
                             & pl.col("actual_points").is_not_null())
        if rows.height < 10:
            lines.append(f"  {position:<6}{rows.height:>6}   too few rows")
            continue
        arm_rho = spearman(rows, "usg_points", "actual_points")
        mean_rho = spearman(rows, "rookie_mean_points", "actual_points")
        arm_mae = rows.select(
            (pl.col("usg_points") - pl.col("actual_points")).abs().mean()).item()
        mean_mae = rows.select(
            (pl.col("rookie_mean_points") - pl.col("actual_points"))
            .abs().mean()).item()
        # The positional mean is one number for everyone, so its rank correlation is
        # undefined -- every player ties. Reported as 0.0, which is what "no ordering
        # information" means, rather than hidden.
        mean_rho = 0.0 if mean_rho is None else mean_rho
        lines.append(f"  {position:<6}{rows.height:>6}{arm_rho:>10.4f}"
                     f"{mean_rho:>10.4f}{arm_rho - mean_rho:>+9.4f}"
                     f"{arm_mae:>10.2f}{mean_mae:>10.2f}")

    drafted = rookies.filter(pl.col("draft_number").is_not_null())
    undrafted = rookies.filter(pl.col("draft_number").is_null())
    lines.append("")
    lines.append(f"  drafted rookies   {drafted.height:>5}, "
                 f"mean realised {drafted['actual_points'].mean():.1f} pts, "
                 f"mean projected {drafted['usg_points'].mean():.1f}")
    lines.append(f"  undrafted rookies {undrafted.height:>5}, "
                 f"mean realised {undrafted['actual_points'].mean():.1f} pts, "
                 f"mean projected {undrafted['usg_points'].mean():.1f}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.usage.backtest",
        description="Walk-forward backtest of the season usage head.")
    parser.add_argument("--first", type=int, default=DEFAULT_TEST_SEASONS[0])
    parser.add_argument("--last", type=int, default=DEFAULT_TEST_SEASONS[-1])
    parser.add_argument("--league", default=SCORING_LEAGUE,
                        help="league key whose scoring prices the comparison")
    parser.add_argument("--save", action="store_true",
                        help="persist the final season's fitted coefficients")
    args = parser.parse_args(argv)

    seasons = list(range(args.first, args.last + 1))
    print(f"Walk-forward: train on {HISTORY_START}..S-1, predict S, "
          f"for S in {seasons[0]}..{seasons[-1]}")
    print(f"Scoring: {args.league}'s own rules, per season, from the registry.\n")

    frames = []
    model = None
    for season in seasons:
        scored, model = run_season(season, league_key=args.league)
        spoke = scored.filter(pl.col("usg_points").is_not_null()).height
        print(f"  {season}: {scored.height:>4} rostered, {spoke:>4} projected, "
              f"{len(model.volume)} volume fits")
        frames.append(scored)

    print()
    print(report(frames))

    if model is not None:
        print()
        print("=== final fitted model ===")
        print(model.summary())
        if args.save:
            print(f"\nwrote {model.save()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
