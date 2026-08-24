"""G1 for the head that actually ships.

``Scripts/usage/gates.py`` runs G1 against :mod:`Scripts.usage.baseline` -- "the crudest
usage model there is: two trailing terms per stat", by its own docstring, and deliberately
so: the gates needed residuals to measure before any effort went into features. It failed
there, and located the deficit in not knowing who plays.

**The shipped season head has never been through G1.** It is a different model -- volume x
efficiency x expected games, with a depth chart, a rookie arm and an availability
estimate, all of which arrived *after* that failure and were the answer to it. Its G0 has
been re-run twice; its accuracy against the blend has been asserted and never measured.

This closes that. The question is the one plan 18 asks: **does adding TOMCAT to the blend
make the blend better?**

Method
------

Season level, because a season head predicts a season. For each player:

* **The external sources** come from the stored weekly lineups, pro-rated to a season:
  a source's real (non-imputed) weeks are summed and scaled by the player's week count.
  A source with no real week for a stat has no opinion about it and is dropped, which is
  the same rule the shipped blend applies per row.
* **TOMCAT** comes from :func:`Scripts.usage.backtest.run_season`, walk-forward -- trained
  on every season before the test year and never shown it.
* **The outcome** is the realised season total from the same frame's ``y_tot_*`` columns.

Both blends are then scored through a real league's rules and compared on the two things
a board consumes: within-position ordering, and per-stat error.

Two bases, and only one of them is usable
-----------------------------------------

``--basis preseason`` (the default) uses **genuine pre-season projections**: FantasyPros'
season-long table for the test year, reachable since ``year=`` turned out to work, and
BetOnline's archived season props. Neither has seen a snap of the season it projects,
which is the whole requirement. ESPN and Pinnacle are absent -- ESPN serves only its
current projection and no 2025 board survives -- so the baseline is weaker than a real
board. It is honest, which the alternative is not.

``--basis summed-weekly`` sums each source's *weekly* projections into a season line. It
was built first, it looks stronger, and **it does not work.** A weekly projection is
reissued every week knowing who got hurt, so summing one produces a "projection" that has
read most of the answer. The tell is quantitative rather than a matter of taste, and
:func:`hindsight_report` prints it: on 2025, summed-weekly ESPN correlates **+0.327** with
the games a player actually went on to play, and FantasyPros **+0.324**, where TOMCAT --
a real pre-season projection -- manages **+0.067**. A pre-season projection cannot know
games played; those two clearly do.

The consequence is that the summed-weekly baseline reaches a within-position Spearman of
0.91 at quarterback and 0.93 at running back against realised season points. No
projection is that good. It is not a strong baseline, it is a partly-revealed answer, and
anything measured against it is measuring the reveal. The mode is kept, loudly labelled,
so the next person to have this idea can see it fail in one command instead of a day.

Usage::

    python -m Scripts.usage.g1_season
    python -m Scripts.usage.g1_season --season 2025 --weights 0.1 0.25 0.5
    python -m Scripts.usage.g1_season --basis summed-weekly   # see above; don't trust it
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts.crosswalk import id_map
from Scripts.projection_utils import IMPUTED_SUFFIX
from Scripts.usage import backtest as bt
from Scripts.usage import evalset

#: External sources, in the order the tables print them.
SOURCES: Tuple[str, ...] = ("ESPN", "FP", "PINNY", "BOL")

#: Candidate TOMCAT weights. 0.25 is what ships; the rest bracket it so the printed
#: curve says whether the shipped number is near the optimum or merely inside it.
WEIGHTS: Tuple[float, ...] = (0.05, 0.1, 0.25, 0.4, 0.5)

#: Fewest real weeks before a source is credited with a season-level opinion.
#:
#: One week is not a season projection -- pro-rating a single week by seventeen turns
#: one good afternoon into a projection. Three is still thin and is the point at which
#: the scaling stops being dominated by which week happened to be real.
MIN_REAL_WEEKS: int = 3


def _league_keys() -> List[str]:
    from Scripts.config_utils import load_config
    return list(load_config()["leagues"].keys())


def external_season(season: int, stats: Sequence[str]) -> pl.DataFrame:
    """Pro-rate each external source's weekly projections to a season line.

    Args:
        season: Season to build.
        stats: ESPN stat names.

    Returns:
        pl.DataFrame: ``gsis_id`` and ``<SOURCE>_<stat>`` season totals, null where the
        source had fewer than :data:`MIN_REAL_WEEKS` real weeks for that stat.
    """
    frame, _ = evalset.build_eval_set(season, stats=list(stats))

    mapping = id_map("espn_id", "gsis_id")
    frame = frame.with_columns(
        pl.col("player_id").cast(pl.Utf8)
        .replace_strict(mapping, default=None, return_dtype=pl.Utf8).alias("gsis_id")
    ).drop_nulls("gsis_id")

    weeks = (frame.group_by("gsis_id").agg(pl.col("week").n_unique().alias("n_weeks")))

    out = weeks
    for source in SOURCES:
        for stat in stats:
            column = f"{source}_{stat}"
            if column not in frame.columns:
                continue
            flag = column + IMPUTED_SUFFIX
            real = (pl.col(column).is_not_null() & ~pl.col(flag).fill_null(True)
                    if flag in frame.columns else pl.col(column).is_not_null())
            agg = (frame.group_by("gsis_id").agg(
                pl.when(real).then(pl.col(column)).otherwise(None)
                  .sum().alias("_sum"),
                real.sum().alias("_n")))
            agg = agg.join(weeks, on="gsis_id", how="left").with_columns(
                pl.when(pl.col("_n") >= MIN_REAL_WEEKS)
                  .then(pl.col("_sum") * pl.col("n_weeks") / pl.col("_n"))
                  .otherwise(None).alias(column)
            ).select("gsis_id", column)
            out = out.join(agg, on="gsis_id", how="left")
    return out


#: BetOnline's archived season-prop column names, mapped to ESPN stat names.
BOL_SEASON: Dict[str, str] = {
    "YDS_PASS": "passingYards", "TD_PASS": "passingTouchdowns",
    "INT_PASS": "passingInterceptions", "YDS_RUSH": "rushingYards",
    "TD_RUSH": "rushingTouchdowns", "YDS_REC": "receivingYards",
    "TD_REC": "receivingTouchdowns", "REC_REC": "receivingReceptions",
}

#: FantasyPros' season-long column names, mapped to ESPN stat names.
FP_SEASON: Dict[str, str] = {f"proj_{s}": s for s in BOL_SEASON.values()}


def preseason_season(season: int, stats: Sequence[str],
                     names: pl.DataFrame) -> pl.DataFrame:
    """Genuine pre-season projections: FantasyPros season-long, and BetOnline props.

    Neither source has seen a snap of the season it projects, which is the whole point
    of this basis -- see the module docstring on why summing weekly projections does not
    substitute.

    Args:
        season: Season to load.
        stats: ESPN stat names to keep.
        names: ``gsis_id`` and ``name_key`` for the population being evaluated.

    Returns:
        pl.DataFrame: ``gsis_id`` with ``FP_<stat>`` and ``BOL_<stat>`` columns.
    """
    from Scripts.paths import season_dir
    from Scripts.season_projections import normalise_name

    out = names.select("gsis_id")

    fp_path = season_dir("FantasyPros", season,
                         "FantasyPros_Projections_Season.parquet", create=False)
    if fp_path.is_file():
        fp = pl.read_parquet(fp_path)
        keep = {c: f"FP_{v}" for c, v in FP_SEASON.items() if c in fp.columns}
        fp = (fp.with_columns(
                  pl.col("player_name")
                    .map_elements(normalise_name, return_dtype=pl.Utf8)
                    .alias("name_key"))
                .select(["name_key"] + list(keep))
                .rename(keep))
        fp = fp.filter(pl.col("name_key").is_not_null()).unique("name_key", keep="none")
        out = out.join(names.join(fp, on="name_key", how="inner").drop("name_key"),
                       on="gsis_id", how="left")

    bol_path = (season_dir("BetOnline", season, create=False)
                / "BetOnline_SeasonProps_Offense.csv")
    if bol_path.is_file():
        bol = pl.read_csv(bol_path)
        keep = {c: f"BOL_{v}" for c, v in BOL_SEASON.items() if c in bol.columns}
        bol = (bol.with_columns(
                   pl.col("player").map_elements(normalise_name, return_dtype=pl.Utf8)
                     .alias("name_key"))
                 .select(["name_key"] + list(keep))
                 .rename(keep))
        bol = bol.filter(pl.col("name_key").is_not_null()).unique("name_key", keep="none")
        out = out.join(names.join(bol, on="name_key", how="inner").drop("name_key"),
                       on="gsis_id", how="left")
    return out


def hindsight_report(season: int = 2025) -> str:
    """Why the summed-weekly basis is unusable, as a number rather than an opinion.

    A pre-season projection cannot correlate with games a player has not yet played.
    A summed weekly one can, and does.
    """
    model_frame, _ = bt.run_season(season)
    games = model_frame.select("gsis_id", pl.col("y_games").alias("actual_games"),
                               "y_tot_receiving_yards", "USG_receivingYards")
    joined = games.join(external_season(season, list(bt.OUTCOME_COLUMNS)),
                        on="gsis_id", how="inner")

    lines = [f"  Correlation with ACTUAL games played — {season}", ""]
    played = joined.filter(pl.col("y_tot_receiving_yards") > 0)
    for column, label in (("ESPN_receivingYards", "summed-weekly ESPN"),
                          ("FP_receivingYards", "summed-weekly FantasyPros"),
                          ("USG_receivingYards", "TOMCAT (pre-season)")):
        sub = played.drop_nulls([column, "actual_games"])
        if sub.height < 20:
            continue
        r = float(np.corrcoef(sub[column].to_numpy(),
                              sub["actual_games"].to_numpy())[0, 1])
        lines.append(f"    {label:<28}{r:+.3f}   n={sub.height}")
    lines += ["", "  A pre-season projection cannot know games played."]
    return "\n".join(lines)


def blend(frame: pl.DataFrame, stats: Sequence[str],
          usage_weight: float) -> pl.DataFrame:
    """Equal-vote blend over whichever sources have an opinion.

    The shipped rule: every source that is real carries the same weight, and the
    survivors renormalise. ``usage_weight`` is expressed relative to an external
    source's 1.0, so 0.25 means TOMCAT votes at a quarter of one of theirs -- matching
    ``WEIGHTS['default']`` where every universal source carries 0.25 and TOMCAT is one
    of them, which renormalises to the same ratio.

    Args:
        frame: Output of :func:`external_season` joined to TOMCAT's ``USG_`` columns.
        stats: ESPN stat names.
        usage_weight: TOMCAT's weight, or 0.0 to leave it out entirely.

    Returns:
        pl.DataFrame: ``frame`` with a ``BLEND_<stat>`` column per stat.
    """
    out = frame
    for stat in stats:
        # Whichever external columns this basis actually produced. `preseason` has no
        # ESPN or Pinnacle, so hard-coding SOURCES here would silently count them as
        # absent-with-an-opinion rather than not present at all.
        columns = [f"{s}_{stat}" for s in SOURCES if f"{s}_{stat}" in frame.columns]
        usage = f"USG_{stat}"
        numerator, denominator = None, None
        for column in columns:
            present = pl.col(column).is_not_null()
            term = pl.when(present).then(pl.col(column)).otherwise(0.0)
            unit = pl.when(present).then(1.0).otherwise(0.0)
            numerator = term if numerator is None else numerator + term
            denominator = unit if denominator is None else denominator + unit
        if usage_weight > 0 and usage in frame.columns:
            present = pl.col(usage).is_not_null()
            numerator = numerator + pl.when(present).then(
                pl.col(usage) * usage_weight).otherwise(0.0)
            denominator = denominator + pl.when(present).then(usage_weight).otherwise(0.0)
        out = out.with_columns(
            pl.when(denominator > 0).then(numerator / denominator)
              .otherwise(None).alias(f"BLEND_{stat}"))
    return out


def run(season: int = 2025,
        weights: Optional[Sequence[float]] = None,
        league_key: str = bt.SCORING_LEAGUE,
        basis: str = "preseason") -> Dict:
    """Measure the blend with and without the shipped season head.

    Args:
        season: Test season. Walk-forward: TOMCAT is trained on everything before it.
        weights: TOMCAT weights to sweep. Defaults to :data:`WEIGHTS`.
        league_key: League whose scoring prices the comparison.
        basis: ``"preseason"`` for genuine pre-season sources, or ``"summed-weekly"``
            for the contaminated one -- see the module docstring, and do not report a
            result from it.

    Returns:
        dict: ``frame`` (the joined evaluation frame) and ``rows`` (one record per
        weight, carrying MAE and per-position Spearman).
    """
    weights = list(WEIGHTS) if weights is None else list(weights)
    stats = list(bt.OUTCOME_COLUMNS)

    from Scripts.season_projections import normalise_name

    model_frame, _ = bt.run_season(season)
    keep = (["gsis_id", "position", "full_name"]
            + [f"USG_{s}" for s in stats if f"USG_{s}" in model_frame.columns]
            + [c for c in bt.OUTCOME_COLUMNS.values() if c in model_frame.columns])
    model_frame = model_frame.select([c for c in keep if c in model_frame.columns])

    if basis == "summed-weekly":
        external = external_season(season, stats)
    elif basis == "preseason":
        names = model_frame.select(
            "gsis_id",
            pl.col("full_name").map_elements(normalise_name, return_dtype=pl.Utf8)
              .alias("name_key")).drop_nulls("name_key")
        names = names.unique("name_key", keep="none")
        external = preseason_season(season, stats, names)
    else:
        raise ValueError(f"basis must be 'preseason' or 'summed-weekly', got {basis!r}")

    joined = model_frame.join(external, on="gsis_id", how="inner")

    scoring = bt.scoring_weights(season, league_key)
    actual_columns = {s: c for s, c in bt.OUTCOME_COLUMNS.items()
                      if c in joined.columns}
    joined = joined.with_columns(
        bt.points(joined, actual_columns, scoring).alias("actual_points"))

    # Restrict to players at least one external source has an opinion about.
    #
    # Without this the comparison is not between two blends, it is between "has an
    # opinion" and "does not". Measured on 2025: 393 of 949 rows carry no external
    # projection at all, `bt.points` scores an absent stat as zero rather than null, and
    # those players really did score close to nothing -- so the baseline collects free
    # credit for a 0 it never issued, while TOMCAT projects them and takes the error.
    # That artefact alone moved the headline by roughly eight points of MAE, in TOMCAT's
    # disfavour, and it is not a fact about either model.
    has_external = None
    for source in SOURCES:
        for stat in stats:
            column = f"{source}_{stat}"
            if column in joined.columns:
                present = pl.col(column).is_not_null()
                has_external = present if has_external is None else (has_external | present)
    if has_external is not None:
        before = joined.height
        joined = joined.filter(has_external)
        dropped = before - joined.height
    else:
        dropped = 0

    rows = []
    for weight in [0.0] + [w for w in weights if w > 0]:
        scored = blend(joined, stats, weight)
        blend_columns = {s: f"BLEND_{s}" for s in stats
                         if f"BLEND_{s}" in scored.columns}
        scored = scored.with_columns(
            bt.points(scored, blend_columns, scoring).alias("blend_points"))
        usable = scored.drop_nulls(["blend_points", "actual_points"])

        record: Dict[str, object] = {
            "weight": weight,
            "n": usable.height,
            "mae": float((usable["blend_points"] - usable["actual_points"])
                         .abs().mean()),
        }
        for position in ("QB", "RB", "WR", "TE"):
            sub = usable.filter(pl.col("position") == position)
            record[position] = bt.spearman(sub, "blend_points", "actual_points")
        rows.append(record)
    return {"frame": joined, "rows": rows, "dropped_no_external": dropped}


def report(season: int = 2025, weights: Optional[Sequence[float]] = None,
           league_key: str = bt.SCORING_LEAGUE, basis: str = "preseason") -> str:
    """Human-readable G1 for the shipped head."""
    result = run(season, weights, league_key, basis)
    rows = result["rows"]
    base = rows[0]
    used = sorted({c.split("_")[0] for c in result["frame"].columns
                   if c.split("_")[0] in SOURCES})

    lines = [
        f"===== G1, shipped season head — {season} walk-forward, priced by {league_key} =====",
        f"  basis: {basis}   baseline sources: {', '.join(used) or 'none'}",
        f"  {result['dropped_no_external']} players dropped: no external source has an "
        f"opinion about them, so there is no blend to compare against",
        "",
    ]
    if basis == "summed-weekly":
        lines += [
            "  *** DO NOT REPORT THIS AS A RESULT. ***",
            "  Summed weekly projections have seen most of the season they project.",
            "  Run `hindsight_report()` for the number. This mode exists to fail visibly.",
            "",
        ]
    lines += [
        "  Does adding TOMCAT to the blend make the blend better?",
        "  Baseline (weight 0.00) is the external sources alone.",
        "",
        f"  {'TOMCAT wt':>10}{'n':>6}{'MAE':>10}{'vs base':>10}"
        f"{'QB':>9}{'RB':>9}{'WR':>9}{'TE':>9}",
        "  " + "-" * 72,
    ]
    for row in rows:
        gain = (1 - row["mae"] / base["mae"]) if base["mae"] else 0.0
        mark = "  <- ships" if abs(row["weight"] - 0.25) < 1e-9 else ""
        cells = "".join(
            f"{row[p]:>9.3f}" if row.get(p) is not None else f"{'—':>9}"
            for p in ("QB", "RB", "WR", "TE"))
        label = "none" if row["weight"] == 0 else f"{row['weight']:.2f}"
        lines.append(f"  {label:>10}{row['n']:>6}{row['mae']:>10.2f}"
                     f"{gain:>9.1%}{cells}{mark}")

    best = min(rows, key=lambda r: r["mae"])
    lines += [
        "",
        f"  Lowest MAE at weight {best['weight']:.2f}"
        f" ({1 - best['mae'] / base['mae']:+.1%} against the four-source baseline).",
        "",
    ]
    if basis == "preseason":
        lines += [
            "  Every source here is a genuine pre-season projection — none has seen a",
            "  snap of the season it projects. ESPN and Pinnacle are absent: ESPN serves",
            "  only its current figure and no 2025 board survives, so this baseline is",
            "  weaker than a real board and the comparison is correspondingly kinder to",
            "  TOMCAT than a live one would be.",
        ]
    else:
        lines += [
            "  Contaminated basis — see the warning above. Not a result.",
        ]
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--weights", type=float, nargs="+", default=None)
    parser.add_argument("--league", default=bt.SCORING_LEAGUE)
    parser.add_argument("--basis", default="preseason",
                        choices=["preseason", "summed-weekly"])
    parser.add_argument("--hindsight", action="store_true",
                        help="print the contamination diagnostic and exit")
    args = parser.parse_args(argv)
    if args.hindsight:
        print(hindsight_report(args.season))
        return 0
    print(report(args.season, args.weights, args.league, args.basis))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
