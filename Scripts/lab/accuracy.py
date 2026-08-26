"""Score the shipping blend against realised stat lines, one stat at a time.

**The gap this fills.** Every scored evaluation in this repo judges either TOMCAT
(:mod:`Scripts.usage.backtest`, :mod:`Scripts.usage.gates`,
:mod:`Scripts.usage.g1_season`, :mod:`Scripts.lab.run`) or a calibration curve
(:mod:`Scripts.outcomes.backtest`, :mod:`Scripts.injury.backtest`). The
four-source blend that actually reaches the app, the Sheets and the draft board
had never been scored against a realised stat line at all -- even though
``Data/Store/<season>/<league>/lineups.parquet`` has carried the projected line
and the realised one side by side, for nine leagues, since 2025.

**Why per stat and not per point.** A fantasy-point MAE is a variance-weighted
sum, and yardage carries most of the variance. Measured on Knights_FFL 2025 the
blend beats ESPN by 2.2% on points -- and inside that number it is winning on
yardage and receptions by 2.8-4.1% while *losing* on rushing touchdowns by 2.0%,
which is the stat with the largest points weight per unit. One number cannot say
that, and the pipeline is built to be answerable per stat: sources are reduced to
stat lines, blended per stat, and priced only at the very end
(:func:`Scripts.projection_utils.compute_weighted_stats`,
:func:`Scripts.projection_utils._apply_scoring`). The evaluation should match.

**The comparison is paired, and that is not a detail.** ``TRUE_`` is dense --
:func:`compute_weighted_stats` 0-fills, so every row has a blend value, including
rows where no source said anything. Every other source is sparse: FantasyPros is
real for 13% of cells, BetOnline 12%, Pinnacle 8%. Pooling all of them onto one
population would compare the blend's number on 100% of rows against FantasyPros'
on 13%, which measures coverage and calls it accuracy -- the exact mistake
``docs/plans/20-consensus-sources.md`` was retired for. So each source is scored
against the blend **on the rows that source was real for**, using
:func:`Scripts.usage.evalset.real_mask`, and the two numbers in a row are always
computed over the same cells.

**The decision rule is the one already in the lab.** A stat where the blend is
worse than one of its own inputs by more than
:data:`Scripts.lab.registry.MAX_STAT_MAE_INCREASE_PCT` is a defect, not noise --
the same threshold, and for the same stated reason: ``USG_`` and ``TRUE_`` are
stat lines the pipeline averages and prices, not ranks it sorts.

**What this cannot do.** Weekly projections-vs-actuals exist for **2025 only**.
``Data/Store/2019-2024/`` holds ``results.parquet`` for one league and no
``lineups.parquet``, and ``docs/plans/25-results-backfill.md`` explains why the
earlier ones can never be built: the FantasyPros, Pinnacle and BetOnline files
for those weeks are gone and FantasyPros' URLs take no season parameter. This is
therefore a one-season scoreboard that gains a season every year, not a
walk-forward, and nothing here should be read as one.

Usage::

    python -m Scripts.lab.accuracy
    python -m Scripts.lab.accuracy --season 2025 --population played
    python -m Scripts.lab.accuracy --no-save
"""

import argparse
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import polars as pl

from Scripts.lab import registry as reg
from Scripts.lab.run import RESULTS_PATH, load_results, save_results
from Scripts.paths import REPO_ROOT
from Scripts.usage import evalset as es
from Scripts.usage.nflverse import ACTUAL_PREFIX, USAGE_STATS, load_opportunity

#: The blend column prefix, and the thing under test.
BLEND = "TRUE"

#: Sources the blend is compared against, in pipeline order.
SOURCES: Tuple[str, ...] = es.SOURCES

#: Stats, in a reading order that groups the three passing stats together.
STAT_ORDER: Tuple[str, ...] = (
    "passingYards", "passingTouchdowns", "passingInterceptions",
    "rushingYards", "rushingTouchdowns",
    "receivingYards", "receivingReceptions", "receivingTouchdowns",
)

#: Positions a stat is a real question for.
#:
#: Not cosmetic. A quarterback carries a ``receivingYards`` row that every source
#: projects at 0 and that realises 0, and there are more quarterback-weeks than
#: tight-end-weeks. Pooling them in halves every receiving MAE and makes every
#: source look identical, because on those rows they are.
STAT_POSITIONS: Dict[str, Tuple[str, ...]] = {
    "passingYards": ("QB",),
    "passingTouchdowns": ("QB",),
    "passingInterceptions": ("QB",),
    "rushingYards": ("QB", "RB", "WR"),
    "rushingTouchdowns": ("QB", "RB", "WR"),
    "receivingYards": ("RB", "WR", "TE"),
    "receivingReceptions": ("RB", "WR", "TE"),
    "receivingTouchdowns": ("RB", "WR", "TE"),
}

#: How a population is expressed as a filter.
#:
#: ``all`` is every rostered player-week, which is the population a projection is
#: actually made over -- a source that says 18 points for a player who is inactive
#: is wrong, and should be scored as wrong. ``team_played`` removes byes, which no
#: source should get credit or blame for. ``played`` keeps only player-weeks with
#: offensive snaps, which isolates "the stat line was wrong" from "he was not on
#: the field" -- the decomposition :mod:`Scripts.usage.gates` uses to locate where
#: a deficit lives.
POPULATIONS: Tuple[str, ...] = ("all", "team_played", "played")

#: Minimum paired cells before a comparison is reported at all.
MIN_PAIRED_ROWS: int = 30


def population_flags(frame: pl.DataFrame, season: int) -> pl.DataFrame:
    """Attach ``played`` and ``team_played`` without fitting anything.

    :func:`Scripts.usage.evalset.attach_usage` attaches the same two flags, but it
    needs a fitted baseline's predictions to supply ``last_posteam``. Nothing here
    is being asked about a model, so the team is derived directly: a player's
    ``posteam`` is carried from the weeks he appeared, filled forward and then
    backward within the player.

    Filling backward as well as forward is deliberate. A player's week-1 team is
    known before the season starts, so using the first team observed in the season
    is a statement about the schedule, not about the outcome -- and the
    alternative, leaving it null, would mark his early weeks ``team_played`` by the
    fallback below and quietly readmit the byes this flag exists to remove.

    A player with no appearance all season has no team at all. He is marked
    ``team_played`` rather than dropped, matching
    :func:`Scripts.usage.evalset.attach_usage`: keeping him is what makes this the
    same population every source is measured on.

    Args:
        frame: Frame from :func:`Scripts.usage.evalset.build_eval_set`.
        season: Season year, for the opportunity and schedule pulls.

    Returns:
        pl.DataFrame: ``frame`` plus boolean ``played`` and ``team_played``.
    """
    from Scripts.usage.nflverse import teams_by_week

    appearances = (
        load_opportunity([season], stats=[])
        .select(["week", "gsis_id", "posteam"])
        .unique(subset=["week", "gsis_id"])
    )
    out = frame.join(
        appearances.with_columns(pl.lit(True).alias("played")),
        on=["week", "gsis_id"], how="left",
    ).sort(["gsis_id", "week"])

    out = out.with_columns(
        pl.when(pl.col("gsis_id").is_null()).then(None)
        .otherwise(pl.col("posteam").forward_fill().backward_fill()
                   .over("gsis_id"))
        .alias("player_team"),
        pl.col("played").fill_null(False),
    )

    playing = teams_by_week(season).with_columns(pl.lit(True).alias("_played"))
    return (
        out.join(playing, left_on=["week", "player_team"],
                 right_on=["week", "posteam"], how="left")
        .with_columns(
            pl.when(pl.col("player_team").is_null()).then(True)
            .otherwise(pl.col("_played").fill_null(False))
            .alias("team_played"))
        .drop("_played")
    )


def build(season: int,
          league_keys: Optional[Sequence[str]] = None) -> Tuple[pl.DataFrame, Dict]:
    """The scoreboard's input frame: every source's line, the blend, the outcome.

    Args:
        season: Season year.
        league_keys: Leagues to pool. Defaults to every league with a store.

    Returns:
        tuple: ``(frame, report)``. The report is
        :func:`Scripts.usage.evalset.build_eval_set`'s, which carries the
        cross-league disagreement checks pooling rests on.
    """
    frame, report = es.build_eval_set(season, league_keys=league_keys,
                                      stats=list(USAGE_STATS))
    return population_flags(frame, season), report


def _population_filter(name: str) -> pl.Expr:
    """Turn a population name into a filter expression."""
    if name == "all":
        return pl.lit(True)
    if name in ("played", "team_played"):
        return pl.col(name)
    raise ValueError(f"unknown population {name!r}; expected one of {POPULATIONS}")


def paired(frame: pl.DataFrame, stat: str, source: str,
           position: Optional[str] = None,
           population: str = "all") -> Optional[Dict]:
    """Blend against one source, on the cells that source was real for.

    Args:
        frame: Frame from :func:`build`.
        stat: ESPN stat name.
        source: Source prefix without the underscore, e.g. ``"FP"``.
        position: Restrict to one ``primaryPosition``. None pools the positions
            :data:`STAT_POSITIONS` lists for the stat.
        population: One of :data:`POPULATIONS`.

    Returns:
        dict or None: ``n``, ``source_mae``, ``blend_mae``, ``source_rmse``,
        ``blend_rmse`` and ``delta_pct`` (positive = the blend is worse). None when
        the columns are absent or fewer than :data:`MIN_PAIRED_ROWS` cells qualify.
    """
    actual = f"{ACTUAL_PREFIX}{stat}"
    source_column, blend_column = f"{source}_{stat}", f"{BLEND}_{stat}"
    if not {actual, source_column, blend_column}.issubset(frame.columns):
        return None

    positions = (position,) if position else STAT_POSITIONS.get(stat, ())
    rows = frame.filter(
        _population_filter(population)
        & pl.col("primaryPosition").is_in(list(positions))
        & pl.col(actual).is_not_null()
        & pl.col(blend_column).is_not_null()
        & es.real_mask(frame, source, stat)
    )
    if rows.height < MIN_PAIRED_ROWS:
        return None

    def _error(column: str) -> Tuple[float, float]:
        residual = pl.col(column) - pl.col(actual)
        summary = rows.select(residual.abs().mean().alias("mae"),
                              (residual ** 2).mean().sqrt().alias("rmse"))
        return float(summary["mae"][0]), float(summary["rmse"][0])

    source_mae, source_rmse = _error(source_column)
    blend_mae, blend_rmse = _error(blend_column)
    return {
        "n": rows.height,
        "source_mae": source_mae,
        "blend_mae": blend_mae,
        "source_rmse": source_rmse,
        "blend_rmse": blend_rmse,
        "delta_pct": (100.0 * (blend_mae - source_mae) / source_mae
                      if source_mae else 0.0),
    }


def scoreboard(frame: pl.DataFrame,
               population: str = "all",
               by_position: bool = False) -> Dict[str, Dict[str, Dict]]:
    """Every (stat, source) pair, optionally split by position.

    Args:
        frame: Frame from :func:`build`.
        population: One of :data:`POPULATIONS`.
        by_position: Split each stat across the positions
            :data:`STAT_POSITIONS` lists for it.

    Returns:
        dict: ``{stat: {source: paired(...)}}``, or with position-suffixed source
        keys when ``by_position``.
    """
    out: Dict[str, Dict[str, Dict]] = {}
    for stat in STAT_ORDER:
        entries: Dict[str, Dict] = {}
        for source in SOURCES:
            if by_position:
                for position in STAT_POSITIONS.get(stat, ()):
                    result = paired(frame, stat, source, position=position,
                                    population=population)
                    if result:
                        entries[f"{source}@{position}"] = result
            else:
                result = paired(frame, stat, source, population=population)
                if result:
                    entries[source] = result
        if entries:
            out[stat] = entries
    return out


def defects(board: Dict[str, Dict[str, Dict]],
            threshold: float = reg.MAX_STAT_MAE_INCREASE_PCT) -> List[Dict]:
    """Stats where the blend is worse than one of its own inputs, past the bar.

    The threshold is :data:`Scripts.lab.registry.MAX_STAT_MAE_INCREASE_PCT`, not a
    number chosen here, and it is applied mechanically to whatever comes back --
    the same discipline the experiment registry states in its own docstring.

    Args:
        board: Output of :func:`scoreboard`.
        threshold: Percent MAE the blend may be worse by before it counts.

    Returns:
        list: ``{stat, source, n, delta_pct}``, worst first.
    """
    found = [
        {"stat": stat, "source": source, "n": entry["n"],
         "delta_pct": entry["delta_pct"]}
        for stat, entries in board.items()
        for source, entry in entries.items()
        if entry["delta_pct"] > threshold
    ]
    return sorted(found, key=lambda row: -row["delta_pct"])


def points_accuracy(season: int, league_key: str,
                    population_frame: Optional[pl.DataFrame] = None) -> Dict:
    """Fantasy-point MAE per source, for the league whose rules price it.

    Reported **beside** the per-stat table and never instead of it. Points are
    league-specific -- that is why :mod:`Scripts.usage.evalset` leaves them out of
    the pooled frame -- so this reads one league's store directly.

    **Read this row with the caveat, not without it.** Unlike the per-stat table
    above it is *not* paired on provenance -- ``<prefix>_Points`` is dense on the
    weekly path because ``clean_lineups`` imputes and 0-fills every source before
    scoring, so Pinnacle's points column is largely the ESPN/FantasyPros mean
    wearing a Pinnacle badge. Its total can therefore beat the blend's while
    Pinnacle itself covers 8% of cells. Per-prefix ``n`` is returned so the shape
    of that is visible rather than implied.

    Args:
        season: Season year.
        league_key: League whose ``lineups`` to read.
        population_frame: Unused; present so callers can pass the pooled frame
            without special-casing. Points cannot be pooled.

    Returns:
        dict: ``{"league": ..., "n": ..., "mae": {prefix: value},
        "counts": {prefix: rows}}``.
    """
    from Scripts import store

    frame = pl.read_parquet(store.require_artifact(season, league_key, "lineups"))
    rows = frame.filter(pl.col("points").is_not_null())
    mae, counts = {}, {}
    for prefix in (BLEND,) + SOURCES:
        column = f"{prefix}_Points"
        if column not in rows.columns:
            continue
        scored = rows.filter(pl.col(column).is_not_null())
        value = scored.select(
            (pl.col(column) - pl.col("points")).abs().mean()).item()
        if value is not None:
            mae[prefix] = float(value)
            counts[prefix] = scored.height
    return {"league": league_key, "n": rows.height, "mae": mae, "counts": counts}


def render(board: Dict[str, Dict[str, Dict]], population: str) -> str:
    """One population's table, as text."""
    lines = [f"\n=== {population} " + "=" * (58 - len(population)),
             f"{'stat':22s} {'vs':6s} {'n':>6s} {'source':>9s} "
             f"{'blend':>9s} {'blend vs source':>16s}"]
    for stat in STAT_ORDER:
        entries = board.get(stat, {})
        for source in SOURCES:
            entry = entries.get(source)
            if not entry:
                continue
            flag = " <-- worse" if entry["delta_pct"] > reg.MAX_STAT_MAE_INCREASE_PCT else ""
            lines.append(
                f"{stat:22s} {source:6s} {entry['n']:6d} "
                f"{entry['source_mae']:9.3f} {entry['blend_mae']:9.3f} "
                f"{entry['delta_pct']:+15.1f}%{flag}")
    return "\n".join(lines)


def run(season: int, league_keys: Optional[Sequence[str]] = None,
        points_league: str = "winfield_football") -> Dict:
    """Build the frame, score every population, and assemble the ledger entry.

    Args:
        season: Season year.
        league_keys: Leagues to pool. Defaults to every league with a store.
        points_league: League whose rules price the secondary points comparison.

    Returns:
        dict: The ledger entry written under ``blend_accuracy``.
    """
    frame, build_report = build(season, league_keys=league_keys)
    entry: Dict = {
        "season": season,
        "rows": frame.height,
        "leagues": sorted(build_report["leagues"]),
        "worst_cross_league_disagreement":
            build_report.get("worst_cross_league_disagreement"),
        "worst_blend_disagreement": build_report.get("worst_blend_disagreement"),
        "threshold_pct": reg.MAX_STAT_MAE_INCREASE_PCT,
        "populations": {},
        "ran_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    for population in POPULATIONS:
        board = scoreboard(frame, population=population)
        entry["populations"][population] = {
            "stats": board,
            "defects": defects(board),
        }
        # The per-position split is kept for one population, not three. It is the
        # useful one -- `played` isolates "the stat line was wrong" from "he was
        # not on the field" -- and carrying it for all three tripled the committed
        # ledger for a detail nothing renders. Ask for the others with
        # `scoreboard(frame, population=..., by_position=True)`.
        if population == "played":
            entry["populations"][population]["by_position"] = scoreboard(
                frame, population=population, by_position=True)
    entry["points"] = points_accuracy(season, points_league)
    return entry


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.lab.accuracy",
        description="Score the shipping blend against realised stat lines.")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--league", action="append", default=[],
                        help="restrict the pool to one league; repeatable")
    parser.add_argument("--points-league", default="winfield_football",
                        help="league whose rules price the secondary points row")
    parser.add_argument("--population", choices=POPULATIONS, default=None,
                        help="print only this population")
    parser.add_argument("--no-save", action="store_true",
                        help="print without touching the ledger")
    args = parser.parse_args(argv)

    entry = run(args.season, league_keys=args.league or None,
                points_league=args.points_league)

    print(f"Blend accuracy, {entry['season']}, {len(entry['leagues'])} league(s) "
          f"pooled, {entry['rows']} player-weeks.")
    print(f"Cross-league disagreement: actual/ESPN "
          f"{entry['worst_cross_league_disagreement']:.4f}, blend "
          f"{entry['worst_blend_disagreement']:.4f}.")

    shown = [args.population] if args.population else list(POPULATIONS)
    for population in shown:
        print(render(entry["populations"][population]["stats"], population))

    points = entry["points"]
    print(f"\nFantasy points, {points['league']}, n={points['n']} "
          "(the secondary number, and not paired on provenance -- an imputed")
    print("  source's points column is the ESPN/FP mean, so its total can flatter it):")
    print("  " + "  ".join(
        f"{k} {v:.3f} (n={points['counts'].get(k, 0)})"
        for k, v in sorted(points["mae"].items())))

    print(f"\nDefects (blend worse than an input by more than "
          f"{entry['threshold_pct']}%):")
    any_defect = False
    for population in POPULATIONS:
        for row in entry["populations"][population]["defects"]:
            any_defect = True
            print(f"  [{population}] {row['stat']} vs {row['source']}: "
                  f"{row['delta_pct']:+.1f}% on n={row['n']}")
    if not any_defect:
        print("  none")

    if not args.no_save:
        results = load_results()
        results.setdefault("blend_accuracy", {})[str(args.season)] = entry
        save_results(results)
        print(f"\nwrote {RESULTS_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
