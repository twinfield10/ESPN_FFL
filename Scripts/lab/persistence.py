"""What actually persists year over year, and whether the model shrinks accordingly.

**Why this exists.** :mod:`Scripts.lab.accuracy` finds one defect in the shipping
blend and it is a touchdown stat. A four-source average is only an improvement on
its inputs where the inputs carry independent signal; where a quantity is close to
unforecastable, four sources' extrapolations average into a confident wrong number
and the honest estimate is a shrunken one. So the question underneath the defect is
not "which source should carry more weight on rushing touchdowns" -- it is "how much
of a touchdown rate is forecastable at all".

That question is answerable directly, on ten seasons of per-player-per-week box
scores that are already on disk, and it has never been asked here as its own
measurement. Plan 16 quoted three volume figures in passing (carries/game +0.915,
air-yards share +0.903, target share +0.858) and
:data:`Scripts.usage.features.SHRINKAGE_K`'s docstring quotes one rate figure
(+0.234). This module measures the whole set, per position, and puts the shipped
constants beside the numbers.

**How a persistence figure relates to a shrinkage constant, and the direction this
module had backwards until 2026-08-27.** The model shrinks a player's own rate
toward his position's baseline with credibility weight ``n / (n + k)`` in
denominator units. Write year *i*'s observed rate as ``y_i = theta_i + e_i``, with
``Var(theta) = tau^2``, ``Cov(theta_1, theta_2) = rho * tau^2`` and
``Var(e) = sigma^2 / n``. Then

    corr(y_1, y_2)  =  rho * tau^2 / (tau^2 + sigma^2/n)  =  rho * n / (n + k_opt)

where ``k_opt = sigma^2 / tau^2`` is the constant that actually minimises error.
Solving ``n / (n + k) = r`` for the *observed* ``r`` gives

    k_implied  =  n * (1 - r) / r   >=   k_opt

with equality only when ``rho = 1``. So ``k_implied`` is a **ceiling, not a floor**:
genuine year-to-year drift in a player's true rate depresses ``r``, which *inflates*
``k_implied``. A shipped constant sitting below it is exactly where a
correctly-calibrated one belongs, and the gap measures drift rather than
under-shrinking.

**This module said "floor" and drew the opposite conclusion -- that all eight
shipped constants were 1.4x to 4.6x too small.** Three experiments put it through
the walk-forward and the pre-committed rule rejected all three: every rate at
``k_implied`` costs +0.48% to +1.23% on yardage MAE and -0.0018 mean within-position
Spearman; touchdown rates alone still cost -0.0009, worst at quarterback (-0.0027);
and a 2x midpoint costs -0.0012, so the damage is monotone in the amount of
shrinkage. See ``Scripts/lab/results.json`` under ``shrinkage_at_floor``,
``shrinkage_touchdowns_at_floor`` and ``shrinkage_double``. The measurements below
are unchanged and worth having; the *inference* from them was wrong.

**What the table is for, then: ranking.** Touchdown rates persist at +0.189 to
+0.276 against +0.895 for carries per game, and that ordering is a fact about what
is forecastable whatever constant prices it. It is why the blend loses on
``rushingTouchdowns`` -- a fourth extrapolation of a three-quarters-noise quantity
does not help -- and why volume is the half worth predicting.

**Volume is reported the same way and read differently.** Volume is what the model
predicts (``targets_pg``, ``carries_pg``, ``pass_attempts_pg`` -- see
:data:`Scripts.usage.season.VOLUME_TARGETS`), not what it shrinks, so its
persistence is a statement about how much of next season is knowable from last,
which is the ceiling any veteran arm is working against.

**Leakage is not a concern and that is worth saying once.** Nothing here is fitted
and nothing here is predicted. It is a description of ten seasons of history,
computed over every consecutive pair at once, and it feeds decisions rather than
models.

Usage::

    python -m Scripts.lab.persistence
    python -m Scripts.lab.persistence --first 2016 --last 2025 --min-denominator 50
    python -m Scripts.lab.persistence --no-save
"""

import argparse
import math
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import polars as pl

from Scripts.lab.run import RESULTS_PATH, load_results, save_results
from Scripts.paths import REPO_ROOT
from Scripts.usage import features as ft

#: Seasons on disk with both ``player_weeks`` and the advanced pulls.
DEFAULT_FIRST, DEFAULT_LAST = 2016, 2025

#: Volume quantities, as ``(reported name, column in season_totals)``.
#:
#: These are the model's actual prediction targets, named as
#: :data:`Scripts.usage.season.VOLUME_TARGETS` names them.
VOLUME: Tuple[Tuple[str, str], ...] = (
    ("pass_attempts_pg", "pass_attempts_pg"),
    ("carries_pg", "carries_pg"),
    ("targets_pg", "targets_pg"),
    ("receptions_pg", "receptions_pg"),
)

#: Positions each rate is a real question for, so a quarterback's catch rate on
#: two career targets does not enter the receiving numbers.
RATE_POSITIONS: Dict[str, Tuple[str, ...]] = {
    "yards_per_target": ("RB", "WR", "TE"),
    "catch_rate": ("RB", "WR", "TE"),
    "rec_td_per_target": ("RB", "WR", "TE"),
    "yards_per_carry": ("QB", "RB", "WR"),
    "rush_td_per_carry": ("QB", "RB", "WR"),
    "yards_per_attempt": ("QB",),
    "pass_td_per_attempt": ("QB",),
    "int_per_attempt": ("QB",),
}

#: Same, for volume.
VOLUME_POSITIONS: Dict[str, Tuple[str, ...]] = {
    "pass_attempts_pg": ("QB",),
    "carries_pg": ("QB", "RB", "WR"),
    "targets_pg": ("RB", "WR", "TE"),
    "receptions_pg": ("RB", "WR", "TE"),
}

#: Fewest opportunities in *both* seasons before a pair counts toward a rate.
#:
#: A rate off three targets is not an observation of that player's efficiency, and
#: pooling those rows in drives every correlation toward zero for a reason that has
#: nothing to do with forecastability. Matches
#: :data:`Scripts.usage.features.RATE_FIT_MIN_DENOMINATOR` in spirit and sits above
#: it, because this needs the denominator on both sides of a pair rather than one.
MIN_DENOMINATOR: float = 25.0

#: Fewest games in both seasons before a pair counts toward a volume figure.
MIN_GAMES: int = 4

#: Fewest pairs before a figure is reported at all.
MIN_PAIRS: int = 40

#: Volume terciles, for the stratification.
#:
#: Plan 22 measured that the credibility architecture reaches the players who give
#: it almost no weight -- 0.4% of thin-volume rows got a fitted prior against 95.6%
#: of the heaviest. So a pooled persistence figure can be true and useless: it is
#: dominated by whichever tercile has the rows.
STRATA: Tuple[str, ...] = ("low", "mid", "high")


def totals(first: int = DEFAULT_FIRST,
           last: int = DEFAULT_LAST) -> pl.DataFrame:
    """Season totals per player, reusing the model's own aggregation.

    Deliberately :func:`Scripts.usage.features.season_totals` rather than a local
    ``group_by``. A persistence study that aggregates differently from the model
    would eventually disagree with it about what a season is, and then the
    comparison against :data:`Scripts.usage.features.SHRINKAGE_K` means nothing.

    Args:
        first: First season to read.
        last: Last season to read.

    Returns:
        pl.DataFrame: One row per ``(season, gsis_id)``, with volume per game,
        the raw totals, and one column per
        :data:`Scripts.usage.features.EFFICIENCY_RATES` name.
    """
    seasons = list(range(first, last + 1))
    frame = ft.season_totals(ft.load_player_weeks(seasons))
    rates = []
    for name, numerator, denominator in ft.EFFICIENCY_RATES:
        num, den = f"tot_{numerator}", f"tot_{denominator}"
        if num not in frame.columns or den not in frame.columns:
            continue
        rates.append(
            pl.when(pl.col(den) > 0).then(pl.col(num) / pl.col(den))
            .otherwise(None).alias(name))
    return frame.with_columns(rates) if rates else frame


def pairs(frame: pl.DataFrame) -> pl.DataFrame:
    """Consecutive season pairs for the same player, as ``_now`` and ``_next``.

    Joined on ``season + 1`` rather than shifted within a player, so a player who
    misses a whole season contributes no pair across the gap -- his 2019 and 2021
    are not adjacent, and treating them as adjacent would measure two years of
    drift as one.

    Args:
        frame: :func:`totals` output.

    Returns:
        pl.DataFrame: One row per ``(gsis_id, season)`` that has a following season,
        carrying every value column twice.
    """
    value_columns = [c for c in frame.columns if c not in ("gsis_id", "season")]
    later = frame.select(
        ["gsis_id", "season"] + value_columns
    ).with_columns((pl.col("season") - 1).alias("season")).rename(
        {c: f"{c}_next" for c in value_columns})
    return frame.join(later, on=["gsis_id", "season"], how="inner")


def _correlations(rows: pl.DataFrame, column: str) -> Tuple[float, float]:
    """Pearson and Spearman between a column and its ``_next`` counterpart."""
    ranked = rows.with_columns(pl.col(column).rank().alias("_a"),
                               pl.col(f"{column}_next").rank().alias("_b"))
    pearson = ranked.select(pl.corr(column, f"{column}_next")).item()
    spearman = ranked.select(pl.corr("_a", "_b")).item()
    return (float(pearson) if pearson is not None else float("nan"),
            float(spearman) if spearman is not None else float("nan"))


def rate_persistence(paired: pl.DataFrame, name: str, denominator: str,
                     position: Optional[str] = None,
                     min_denominator: float = MIN_DENOMINATOR) -> Optional[Dict]:
    """One efficiency rate's year-over-year persistence, and the ``k`` it implies.

    Args:
        paired: :func:`pairs` output.
        name: Rate name, e.g. ``"rush_td_per_carry"``.
        denominator: The rate's denominator column in season totals, e.g.
            ``"tot_carries"``.
        position: Restrict to one position. None pools
            :data:`RATE_POSITIONS` for the rate.
        min_denominator: Opportunities required in both seasons.

    Returns:
        dict or None: ``n``, ``pearson``, ``spearman``, ``median_denominator``,
        ``implied_k`` and ``shipped_k``. None when too few pairs qualify.
    """
    if name not in paired.columns or denominator not in paired.columns:
        return None
    positions = (position,) if position else RATE_POSITIONS.get(name, ())
    rows = paired.filter(
        pl.col("position").is_in(list(positions))
        & (pl.col(denominator) >= min_denominator)
        & (pl.col(f"{denominator}_next") >= min_denominator)
        & pl.col(name).is_not_null() & pl.col(f"{name}_next").is_not_null()
    )
    if rows.height < MIN_PAIRS:
        return None

    pearson, spearman = _correlations(rows, name)
    median = float(rows.select(pl.col(denominator).median()).item())
    implied = median * (1.0 - pearson) / pearson if pearson > 0 else float("inf")
    return {
        "n": rows.height,
        "pearson": pearson,
        "spearman": spearman,
        "median_denominator": median,
        "implied_k": implied,
        "shipped_k": ft.SHRINKAGE_K.get(name),
    }


def volume_persistence(paired: pl.DataFrame, name: str, column: str,
                       position: Optional[str] = None) -> Optional[Dict]:
    """One volume quantity's year-over-year persistence, pooled and by tercile.

    Args:
        paired: :func:`pairs` output.
        name: Reported name, e.g. ``"carries_pg"``.
        column: Column in season totals holding it.
        position: Restrict to one position. None pools
            :data:`VOLUME_POSITIONS` for the quantity.

    Returns:
        dict or None: ``n``, ``pearson``, ``spearman`` and a ``strata`` entry per
        :data:`STRATA`. None when too few pairs qualify.
    """
    if column not in paired.columns:
        return None
    positions = (position,) if position else VOLUME_POSITIONS.get(name, ())
    rows = paired.filter(
        pl.col("position").is_in(list(positions))
        & (pl.col("games") >= MIN_GAMES) & (pl.col("games_next") >= MIN_GAMES)
        & pl.col(column).is_not_null() & pl.col(f"{column}_next").is_not_null()
    )
    if rows.height < MIN_PAIRS:
        return None

    pearson, spearman = _correlations(rows, column)
    entry = {"n": rows.height, "pearson": pearson, "spearman": spearman,
             "strata": {}}

    # Terciles of the *prior* season, which is the information a projection has.
    cuts = rows.select(pl.col(column).quantile(1 / 3).alias("lo"),
                       pl.col(column).quantile(2 / 3).alias("hi")).row(0)
    conditions = {
        "low": pl.col(column) <= cuts[0],
        "mid": (pl.col(column) > cuts[0]) & (pl.col(column) <= cuts[1]),
        "high": pl.col(column) > cuts[1],
    }
    for stratum in STRATA:
        slice_ = rows.filter(conditions[stratum])
        if slice_.height < MIN_PAIRS:
            continue
        stratum_pearson, stratum_spearman = _correlations(slice_, column)
        entry["strata"][stratum] = {
            "n": slice_.height, "pearson": stratum_pearson,
            "spearman": stratum_spearman,
        }
    return entry


def run(first: int = DEFAULT_FIRST, last: int = DEFAULT_LAST,
        min_denominator: float = MIN_DENOMINATOR) -> Dict:
    """Measure every rate and every volume quantity, pooled and per position.

    Args:
        first: First season.
        last: Last season.
        min_denominator: Opportunities required in both seasons of a rate pair.

    Returns:
        dict: The ledger entry written under ``persistence``.
    """
    paired = pairs(totals(first, last))
    entry: Dict = {
        "seasons": [first, last],
        "pairs": paired.height,
        "min_denominator": min_denominator,
        "min_games": MIN_GAMES,
        "rates": {},
        "volume": {},
        "ran_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }

    denominators = {name: f"tot_{denominator}"
                    for name, _, denominator in ft.EFFICIENCY_RATES}
    for name, _, _ in ft.EFFICIENCY_RATES:
        pooled = rate_persistence(paired, name, denominators[name],
                                  min_denominator=min_denominator)
        if not pooled:
            continue
        pooled["by_position"] = {
            position: result
            for position in RATE_POSITIONS.get(name, ())
            if (result := rate_persistence(paired, name, denominators[name],
                                           position=position,
                                           min_denominator=min_denominator))
        }
        entry["rates"][name] = pooled

    for name, column in VOLUME:
        pooled = volume_persistence(paired, name, column)
        if not pooled:
            continue
        pooled["by_position"] = {
            position: result
            for position in VOLUME_POSITIONS.get(name, ())
            if (result := volume_persistence(paired, name, column,
                                             position=position))
        }
        entry["volume"][name] = pooled
    return entry


def render(entry: Dict) -> str:
    """The two tables, as text."""
    lines = [
        f"\nVolume -- what the model predicts. Pairs {entry['seasons'][0]}"
        f"-{entry['seasons'][1]}, {MIN_GAMES}+ games both seasons.",
        f"{'quantity':20s} {'n':>6s} {'pearson':>8s} {'spearman':>9s} "
        f"{'low':>8s} {'mid':>8s} {'high':>8s}",
    ]
    for name, _ in VOLUME:
        result = entry["volume"].get(name)
        if not result:
            continue
        strata = result["strata"]
        cells = "".join(
            f"{strata[s]['pearson']:8.3f}" if s in strata else f"{'--':>8s}"
            for s in STRATA)
        lines.append(f"{name:20s} {result['n']:6d} {result['pearson']:8.3f} "
                     f"{result['spearman']:9.3f} {cells}")

    lines += [
        f"\nEfficiency -- what the model shrinks. {int(entry['min_denominator'])}+ "
        "opportunities both seasons.",
        "implied_k is a CEILING: it assumes a perfectly stable true rate, so real "
        "drift inflates it, and",
        "below it is where a calibrated constant belongs. Moving every rate to it "
        "was measured and",
        "rejected -- results.json: shrinkage_at_floor, -0.0018 mean Spearman and "
        "+0.48-1.23% yardage MAE.",
        f"{'rate':22s} {'n':>6s} {'pearson':>8s} {'spearman':>9s} "
        f"{'med n':>7s} {'ceiling k':>10s} {'shipped k':>10s} {'ratio':>17s}",
    ]
    for name, _, _ in ft.EFFICIENCY_RATES:
        result = entry["rates"].get(name)
        if not result:
            continue
        shipped = result["shipped_k"]
        implied = result["implied_k"]
        # A ratio rather than a verdict. `implied_k` is a *ceiling* -- see the module
        # docstring -- so below it is the expected place for a calibrated constant,
        # and the walk-forward rejected moving any rate up to it.
        if shipped is None or not math.isfinite(implied) or implied <= 0:
            verdict = ""
        else:
            verdict = f"{shipped / implied:.2f} of ceiling"
        lines.append(
            f"{name:22s} {result['n']:6d} {result['pearson']:8.3f} "
            f"{result['spearman']:9.3f} {result['median_denominator']:7.0f} "
            f"{implied:10.0f} "
            f"{('--' if shipped is None else f'{shipped:.0f}'):>10s} "
            f"{verdict:>17s}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.lab.persistence",
        description="Year-over-year persistence of volume and efficiency, and "
                    "the shrinkage it implies.")
    parser.add_argument("--first", type=int, default=DEFAULT_FIRST)
    parser.add_argument("--last", type=int, default=DEFAULT_LAST)
    parser.add_argument("--min-denominator", type=float, default=MIN_DENOMINATOR)
    parser.add_argument("--by-position", action="store_true",
                        help="also print the per-position split")
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args(argv)

    entry = run(args.first, args.last, args.min_denominator)
    print(f"Persistence over {entry['pairs']} consecutive player-season pairs, "
          f"{entry['seasons'][0]}-{entry['seasons'][1]}.")
    print(render(entry))

    if args.by_position:
        print("\nBy position:")
        for group, label in (("volume", "volume"), ("rates", "rate")):
            for name, result in entry[group].items():
                for position, split in result.get("by_position", {}).items():
                    print(f"  {label:6s} {name:22s} {position:3s} "
                          f"n={split['n']:5d} r={split['pearson']:+.3f}")

    if not args.no_save:
        results = load_results()
        results["persistence"] = entry
        save_results(results)
        print(f"\nwrote {RESULTS_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
