"""What the first three games say about the pre-season depth chart.

`depth_rank` is the one feature that has ever moved this model -- +0.048 R-squared on
veteran carries, plan 21 -- and it enters both arms as a hard number: rank 1, 2 or 3.
**Nothing anywhere records how often that number turns out to be right.**

This module answers the question a chart cannot answer about itself. If a player is
starting in week 1, the depth chart should have had him at rank 1; so reconstruct the
chart the season actually revealed and score the pre-season one against it.

The answer, measured over 2018-2025, is that a listed starter really is one **57%** of
the time if he is a settled veteran, **44%** if he changed teams and **35%** if he is a
rookie. The chart degrades precisely on the players whose role a drafter cannot work
out for himself.

Leak-free, and that is the whole design
---------------------------------------

Early-season data appears here **only as a training label from seasons that are already
over**. Calibrating on 2018-2024 and applying the result to 2026 needs nothing from 2026
except its pre-season chart, so none of this is reachable-only-after-week-1 the way a
naive "use the first few games" feature would be.

What this is not for
--------------------

**Not a feature.** Plan 33 measured that and the result is negative: a two-stage role
model beats the best single signal only at quarterback movers (0.450 against 0.386,
n = 39) and is a wash elsewhere, because prior volume already carries role for anyone
who has one. For rookies, draft capital *alone* beats draft capital plus the chart
(0.478 against 0.458 at running back), which confirms the shipped
:data:`Scripts.usage.season.ROOKIE_REGRESSORS` rather than arguing against it.

What it is for is the **variance**. Realised season volume conditioned on listed rank
runs from p90/p50 = 1.16 for a settled QB1 to 2.24 for a mover TE2, against a board
``floor``/``ceiling`` that is 9.0% wide and varies by *position* rather than by cohort.
See plan 33 phase 3, which belongs scoped with plan 28.

Usage::

    python -m Scripts.usage.role --seasons 2018 2025
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import polars as pl

from Scripts.usage import context as ctx
from Scripts.usage import features as ft

#: Games the derived chart reads. Three rather than one because a single game is a
#: game script -- a team down 21 at half throws to everybody -- and rather than five
#: because a chart that takes five weeks to reveal itself has stopped describing the
#: pre-season question this scores.
DERIVED_WEEKS = 3

#: Fewest of those games a player must appear in to be ranked at all.
#:
#: The guard against reading an injury as a demotion. A starter hurt in week 1 has one
#: game of near-zero opportunity and would otherwise rank behind his own backup, which
#: would score the pre-season chart as wrong about a player it had exactly right.
MIN_DERIVED_GAMES = 2

#: Position to the per-game opportunity that defines its depth chart.
#:
#: Opportunity rather than production, for the same reason the model decomposes volume
#: from efficiency: a receiver who was targeted eight times and dropped six of them
#: held the role. Yards would say he did not.
ROLE_STAT: Dict[str, str] = {
    "QB": "attempts", "RB": "carries", "WR": "targets", "TE": "targets",
}

#: Cohorts the calibration splits on, in the order they are reported.
#:
#: The split is the finding. Pooled, the chart looks like a 52%-accurate signal and
#: that number describes nobody -- it is a settled veteran's 57% and a rookie's 35%
#: averaged into a figure neither of them has.
COHORTS = ("settled", "mover", "rookie")


def cohort_expression() -> pl.Expr:
    """Which cohort a feature row belongs to.

    Rookie takes precedence over mover: a rookie has no prior team, so
    ``team_changed`` is not meaningful for him and
    :func:`Scripts.usage.features.roster_context` fills it False anyway.

    Returns:
        pl.Expr: ``"settled"``, ``"mover"`` or ``"rookie"``.
    """
    return (pl.when(pl.col("is_rookie")).then(pl.lit("rookie"))
              .when(pl.col("team_changed")).then(pl.lit("mover"))
              .otherwise(pl.lit("settled")).alias("cohort"))


def derived_chart(season: int, weeks: int = DERIVED_WEEKS,
                  min_games: int = MIN_DERIVED_GAMES) -> pl.DataFrame:
    """The depth chart as the season's first games actually revealed it.

    Args:
        season: Completed season to read.
        weeks: Games to read. See :data:`DERIVED_WEEKS`.
        min_games: Games a player must appear in. See :data:`MIN_DERIVED_GAMES`.

    Returns:
        pl.DataFrame: ``gsis_id``, ``season``, ``position``, ``true_rank`` clipped to
        :data:`Scripts.usage.context.MAX_DEPTH_RANK`, ``early_volume`` (per game) and
        ``early_games``. One row per player who cleared ``min_games``.

    Raises:
        FileNotFoundError: When the season has not been pulled.
    """
    weekly = ft.load_player_weeks([season]).filter(pl.col("week") <= weeks)
    ranked = []
    for position, stat in ROLE_STAT.items():
        if stat not in weekly.columns:
            continue
        rows = weekly.filter(pl.col("position") == position)
        if not rows.height:
            continue
        totals = rows.group_by("gsis_id", "team", "position").agg(
            (pl.col(stat).sum() / pl.len()).alias("early_volume"),
            pl.len().alias("early_games"))
        totals = totals.filter(pl.col("early_games") >= min_games)
        # Ranked within the team, which is what a depth chart is. `min` ties rather
        # than `ordinal`, so two backs splitting a room evenly are both rank 1 --
        # asserting an order the games did not reveal would score the pre-season
        # chart against a coin flip.
        ranked.append(totals.with_columns(
            pl.col("early_volume").rank("min", descending=True)
              .over("team").cast(pl.Int32)
              .clip(upper_bound=ctx.MAX_DEPTH_RANK).alias("true_rank")))

    if not ranked:
        return pl.DataFrame(schema={"gsis_id": pl.String, "season": pl.Int32,
                                    "position": pl.String, "true_rank": pl.Int32,
                                    "early_volume": pl.Float64,
                                    "early_games": pl.UInt32})
    return (pl.concat(ranked, how="diagonal")
              .with_columns(pl.lit(season).cast(pl.Int32).alias("season"))
              .select("gsis_id", "season", "position", "true_rank",
                      "early_volume", "early_games")
              .sort("season", "position", "true_rank"))


def scored_seasons(seasons: Sequence[int],
                   history_start: int = 2016) -> pl.DataFrame:
    """Pre-season chart beside the derived one, for several seasons.

    Args:
        seasons: Completed seasons to score.
        history_start: Earliest season the features may look back to.

    Returns:
        pl.DataFrame: One row per scored player -- ``season``, ``gsis_id``,
        ``position``, ``cohort``, ``depth_rank`` as the chart listed him, and
        ``true_rank`` as the games revealed him.
    """
    frames = []
    for season in sorted(set(seasons)):
        history = [s for s in range(history_start, season)]
        if not history:
            continue
        features = ft.season_features(season, history)
        derived = derived_chart(season).drop("position", "season")
        joined = features.join(derived, on="gsis_id", how="inner")
        if not joined.height:
            continue
        frames.append(joined.with_columns(cohort_expression()).select(
            "season", "gsis_id", "position", "cohort", "depth_rank", "true_rank",
            "early_volume", "early_games"))

    if not frames:
        return pl.DataFrame(schema={"season": pl.Int32, "gsis_id": pl.String,
                                    "position": pl.String, "cohort": pl.String,
                                    "depth_rank": pl.Int32, "true_rank": pl.Int32})
    return pl.concat(frames, how="diagonal").drop_nulls(["depth_rank", "true_rank"])


def calibration(scored: pl.DataFrame, by_position: bool = False) -> pl.DataFrame:
    """``P(true rank | listed rank, cohort)``.

    The table plan 33's phases 2 and 3 both read: phase 2 prints the diagonal beside a
    projection so a drafter can see whether he is being sold a fact or a 35% chance,
    and phase 3 draws from the whole row.

    Args:
        scored: :func:`scored_seasons` output.
        by_position: Split by position as well. Off by default -- the per-position
            cells run to 30 rows and the cohort split is where the signal is.

    Returns:
        pl.DataFrame: ``cohort``, ``depth_rank``, optional ``position``, one
        ``p_true_<n>`` column per rank, ``accuracy`` (the diagonal) and ``n``.
    """
    if not scored.height:
        return pl.DataFrame(schema={"cohort": pl.String, "depth_rank": pl.Int32,
                                    "accuracy": pl.Float64, "n": pl.UInt32})

    keys = ["cohort", "depth_rank"] + (["position"] if by_position else [])
    ranks = sorted(r for r in scored["true_rank"].unique().to_list() if r is not None)
    out = scored.group_by(keys).agg(
        [(pl.col("true_rank") == rank).mean().alias(f"p_true_{rank}")
         for rank in ranks]
        + [pl.len().alias("n")])

    # The diagonal: how often the chart was simply right. Built by comparing each
    # row's listed rank against its own probability column rather than with a
    # coalesce over hard-coded names, so a fourth rank cannot silently drop out.
    accuracy = pl.lit(None, dtype=pl.Float64)
    for rank in ranks:
        accuracy = (pl.when(pl.col("depth_rank") == rank)
                      .then(pl.col(f"p_true_{rank}")).otherwise(accuracy))
    return out.with_columns(accuracy.alias("accuracy")).sort(keys)


#: Version of the persisted calibration, bumped when its *shape* changes.
CALIBRATION_VERSION = "1.0.0"


def calibration_path(version: Optional[str] = None):
    """Where the fitted calibration lives. Not created.

    Args:
        version: Override :data:`CALIBRATION_VERSION`.

    Returns:
        Path: ``Data/NFL/models/role_calibration_<version>.json``.
    """
    from Scripts.paths import DATA_DIR
    directory = DATA_DIR / "NFL" / "models"
    return directory / f"role_calibration_{version or CALIBRATION_VERSION}.json"


def save_calibration(table: pl.DataFrame, seasons: Sequence[int],
                     path=None):
    """Persist a calibration table.

    Fitted from history and read at projection time, like the season model, because
    the alternative is rebuilding eight seasons of features on every board build to
    arrive at the same nine numbers.

    Args:
        table: :func:`calibration` output.
        seasons: Seasons it was fitted on, recorded so a stale table is visible.
        path: Override :func:`calibration_path`.

    Returns:
        Path: Where it was written.
    """
    import json
    from datetime import datetime

    path = calibration_path() if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": CALIBRATION_VERSION,
        "fitted_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "seasons": sorted(seasons),
        "derived_weeks": DERIVED_WEEKS,
        "min_derived_games": MIN_DERIVED_GAMES,
        "rows": table.to_dicts(),
    }
    path.write_text(json.dumps(payload, indent=2))
    return path


def load_calibration(path=None) -> Optional[Dict]:
    """The persisted calibration, or None when it has not been fitted.

    None rather than an exception: this is a diagnostic column, and a board that has
    never run ``python -m Scripts.usage.role --fit`` must still build.

    Args:
        path: Override :func:`calibration_path`.

    Returns:
        dict | None: The payload, with ``rows`` as a list of records.
    """
    import json

    path = calibration_path() if path is None else path
    if not path.is_file():
        return None
    return json.loads(path.read_text())


def rank_probabilities(payload: Optional[Dict] = None
                       ) -> Dict[Tuple[str, int], List[float]]:
    """``P(true rank | listed rank, cohort)`` as a lookup, for drawing a role.

    :func:`attach_confidence` reads the *diagonal* of this table -- how often the chart
    was simply right -- because a drafter wants one number beside a projection. Phase 3
    reads the whole row: a listed rookie RB1 is the lead only 35.6% of the time, and the
    other 64.4% is not "unknown", it is 22.6% rank 2 and 41.8% rank 3. That distribution
    is the input :mod:`Scripts.outcomes.simulate` draws a role from.

    Args:
        payload: :func:`load_calibration` output. None loads it.

    Returns:
        dict: ``(cohort, listed_rank)`` to a probability vector indexed from rank 1, each
        renormalised to sum to 1. Empty when the calibration has not been fitted -- the
        caller then treats the chart as certain, which is the behaviour before this
        existed rather than an invented distribution.
    """
    payload = load_calibration() if payload is None else payload
    rows = (payload or {}).get("rows") or []

    ranks = sorted({int(key.rsplit("_", 1)[1])
                    for row in rows for key in row if key.startswith("p_true_")})
    out: Dict[Tuple[str, int], List[float]] = {}
    for row in rows:
        vector = [float(row.get(f"p_true_{rank}") or 0.0) for rank in ranks]
        total = sum(vector)
        if total <= 0:
            continue
        # Renormalised because the stored row is a set of independently computed means
        # and need not sum to exactly one; a sampler needs it to.
        out[(str(row["cohort"]), int(row["depth_rank"]))] = [v / total for v in vector]
    return out


def attach_confidence(frame: pl.DataFrame,
                      payload: Optional[Dict] = None) -> pl.DataFrame:
    """Attach ``usg_role_cohort`` and ``usg_role_confidence`` to a feature frame.

    **No projection moves.** This is the number beside the projection, not inside it:
    a rookie listed WR2 is not a WR2, he is a 32% WR2, and a drafter reading a line
    should be able to see which of those he is being sold.

    Args:
        frame: Feature frame carrying ``depth_rank``, ``is_rookie`` and
            ``team_changed``.
        payload: :func:`load_calibration` output. None loads it.

    Returns:
        pl.DataFrame: ``frame`` with the two columns. Confidence is null wherever the
        calibration has no cell -- an unfitted table, or a rank it never saw -- which
        the board renders as blank rather than as a confident zero.
    """
    needed = {"depth_rank", "is_rookie", "team_changed"}
    if not needed.issubset(frame.columns):
        return frame

    out = frame.with_columns(cohort_expression().alias("usg_role_cohort"))
    payload = load_calibration() if payload is None else payload
    rows = (payload or {}).get("rows") or []

    lookup = pl.DataFrame(
        [{"usg_role_cohort": r["cohort"], "depth_rank": r["depth_rank"],
          "usg_role_confidence": r.get("accuracy")}
         for r in rows if r.get("accuracy") is not None],
        schema={"usg_role_cohort": pl.String, "depth_rank": pl.Int32,
                "usg_role_confidence": pl.Float64})
    if not lookup.height:
        return out.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("usg_role_confidence"))

    return out.with_columns(pl.col("depth_rank").cast(pl.Int32)).join(
        lookup, on=["usg_role_cohort", "depth_rank"], how="left")

def report(seasons: Sequence[int]) -> str:
    """Human-readable calibration, and the spread it implies.

    Args:
        seasons: Completed seasons to score.

    Returns:
        str: The table plan 33 records, reproduced from live data.
    """
    scored = scored_seasons(seasons)
    if not scored.height:
        return "No scored seasons -- has R/GetUsage.R been run?"

    table = calibration(scored)
    lines = [
        f"===== Depth-chart calibration, {min(seasons)}-{max(seasons)} =====",
        f"  {scored.height} players scored over "
        f"{scored['season'].n_unique()} season(s); "
        f"role read from the first {DERIVED_WEEKS} games, "
        f"{MIN_DERIVED_GAMES}+ played",
        "",
        f"  {'cohort':<10}{'listed':>7}{'-> true 1':>11}{'true 2':>9}"
        f"{'true 3':>9}{'right':>8}{'n':>7}",
    ]
    for cohort in COHORTS:
        rows = table.filter(pl.col("cohort") == cohort)
        for row in rows.iter_rows(named=True):
            if row["n"] < 20:
                continue
            shares = "".join(
                f"{100 * (row.get(f'p_true_{r}') or 0.0):>{11 if r == 1 else 9}.0f}%"
                for r in (1, 2, 3))
            lines.append(f"  {cohort:<10}{row['depth_rank']:>7}{shares}"
                         f"{100 * (row['accuracy'] or 0.0):>7.0f}%{row['n']:>7}")
        lines.append("")

    starters = table.filter(pl.col("depth_rank") == 1)
    if starters.height:
        best = starters.sort("accuracy", descending=True).row(0, named=True)
        worst = starters.sort("accuracy").row(0, named=True)
        lines.append(
            f"  A listed starter really is one {100 * best['accuracy']:.0f}% of the "
            f"time for a {best['cohort']} player and "
            f"{100 * worst['accuracy']:.0f}% for a {worst['cohort']} one.")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m Scripts.usage.role",
        description="Score the pre-season depth chart against the one the season "
                    "revealed.")
    parser.add_argument("--seasons", type=int, nargs=2, default=(2018, 2025),
                        metavar=("FIRST", "LAST"))
    parser.add_argument("--fit", action="store_true",
                        help="persist the calibration for the board to read")
    args = parser.parse_args(argv)
    seasons = list(range(args.seasons[0], args.seasons[1] + 1))
    print(report(seasons))
    if args.fit:
        table = calibration(scored_seasons(seasons))
        path = save_calibration(table, seasons)
        print(f"\n  wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
