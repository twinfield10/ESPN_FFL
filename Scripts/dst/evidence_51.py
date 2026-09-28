"""The step-0 evidence behind ``docs/plans/51-dst-from-opponent-markets.md``, reproduced.

Two questions, both answered from local data:

1. **Which definition of points and yards allowed does ESPN score?** Each 2025 defence-week
   whose ESPN tier is recorded in any league store is compared with candidate definitions
   built from play-by-play. The winners reproduce every tier (431/431, 357/357) and are
   what :mod:`Scripts.dst.allowed` implements.
2. **Do opponent props beat game lines on the D/ST components?** BetOnline's 2025 weekly
   projections against plan 30's two line inputs, OLS with each week held out in turn.
   They do not, on any component.

Usage::

    python -m Scripts.dst.evidence_51
"""

from __future__ import annotations

import glob
from typing import Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import polars as pl

from Scripts import paths, vegas
from Scripts.dst import allowed
from Scripts.dst.model import PA_TIERS, YD_TIERS

#: ESPN's abbreviations where they differ from nflverse's.
ESPN_TO_NFLVERSE: Dict[str, str] = {"WSH": "WAS", "LAR": "LA"}


def _tier(value: float, ladder) -> Optional[str]:
    for name, lo, hi in ladder:
        if lo <= value <= hi:
            return name
    return ladder[0][0] if value < 0 else None


def espn_tiers(season: int) -> Dict[Tuple[int, str, str], Set[str]]:
    """The ESPN tier each D/ST landed in, per week, read from every league store.

    A league stores only the tiers it prices, so a row with no tier set means the tier is
    one that league leaves out. Each store therefore contributes a *candidate set*, and
    the stores are intersected.

    Args:
        season: Season of the stores to read.

    Returns:
        dict: ``(week, nflverse team, "pa" | "ya")`` to the set of possible tier names.
    """
    obs: Dict[Tuple[int, str, str], Set[str]] = {}
    for f in glob.glob(str(paths.DATA_DIR / "Store" / str(season) / "*" / "lineups.parquet")):
        d = pl.read_parquet(f).filter(pl.col("primaryPosition") == "D/ST")
        for ladder, key in ((PA_TIERS, "pa"), (YD_TIERS, "ya")):
            present = [n for n, _, _ in ladder if f"defensive{n}" in d.columns]
            if not present:
                continue
            cols = [f"defensive{n}" for n in present]
            for r in d.select("week", "pro_team", *cols).iter_rows(named=True):
                k = (r["week"], ESPN_TO_NFLVERSE.get(r["pro_team"], r["pro_team"]), key)
                hit = {n for n in present if (r[f"defensive{n}"] or 0) >= 1}
                cand = hit or {n for n, _, _ in ladder if n not in present}
                obs[k] = obs.get(k, cand) & cand
    return obs


def definitions(season: int = 2025) -> pl.DataFrame:
    """Tier match counts for each candidate definition of points and yards allowed.

    Args:
        season: Season to test.

    Returns:
        pl.DataFrame: ``quantity``, ``definition``, ``n``, ``matches``, ``rate``.
    """
    obs = espn_tiers(season)
    a = allowed.load([season])
    sc = (pl.scan_parquet(paths.DATA_DIR / "NFL" / str(season) / "pbp.parquet")
          .filter(pl.col("season_type") == "REG",
                  pl.col("play_type").is_in(["pass", "run"]),
                  pl.col("special_teams_play").fill_null(0) == 0)
          .group_by("game_id", pl.col("defteam").alias("team"))
          .agg(pl.col("yards_gained").fill_null(0).sum().alias("net_no_kneels")).collect())
    a = (a.join(sc, on=["game_id", "team"], how="left")
         .with_columns((pl.col("points_allowed")
                        + allowed.SAFETY_CREDIT * pl.col("safeties_vs_off"))
                       .alias("minus_6_per_return_td")))

    candidates = (
        ("points allowed", "opponent final score", "points_allowed_all", PA_TIERS, "pa"),
        ("points allowed", "- 6 per return TD vs offence", "minus_6_per_return_td", PA_TIERS, "pa"),
        ("points allowed", "- 6 per return TD, - 2 per safety (ESPN)", "points_allowed", PA_TIERS, "pa"),
        ("yards allowed", "gross (player pass + rush yards)", "yards_allowed_gross", YD_TIERS, "ya"),
        ("yards allowed", "net, pass + run", "net_no_kneels", YD_TIERS, "ya"),
        ("yards allowed", "net, incl. kneels and spikes (ESPN)", "yards_allowed", YD_TIERS, "ya"),
    )
    rows = []
    for quantity, label, col, ladder, key in candidates:
        n = hits = 0
        for r in a.select("week", "team", col).iter_rows(named=True):
            cand = obs.get((r["week"], r["team"], key))
            if not cand:
                continue
            n += 1
            hits += _tier(r[col], ladder) in cand
        rows.append({"quantity": quantity, "definition": label, "n": n, "matches": hits,
                     "rate": hits / n if n else float("nan")})
    return pl.DataFrame(rows)


def _props(season: int) -> pl.DataFrame:
    """BetOnline's weekly projections, rolled up to the offence each defence faced."""
    pr = (pl.read_parquet(paths.PROJECTIONS_DIR / "BetOnline" / "Season" / str(season)
                          / "BetOnline_AllProps.parquet")
          .with_columns(pl.col("team").replace(ESPN_TO_NFLVERSE)))
    # The quarterback is the one priced for the most attempts, so a listed backup is not
    # added on top of the starter.
    qb = (pr.filter(pl.col("proj_passingAttempts").is_not_null())
          .sort("proj_passingAttempts", descending=True).group_by("week", "team").first()
          .select("week", "team", pl.col("proj_passingYards").alias("p_pass"),
                  pl.col("proj_passingAttempts").alias("p_att"),
                  pl.col("proj_passingInterceptions").alias("p_int")))
    ru = pr.group_by("week", "team").agg(
        pl.col("proj_rushingYards").fill_null(0).sum().alias("p_rush"),
        pl.col("proj_rushingAttempts").fill_null(0).sum().alias("p_rush_att"))
    sk = (pr.filter(pl.col("proj_defensiveSacks").is_not_null())
          .group_by("week", "team").agg(pl.col("proj_defensiveSacks").sum().alias("p_sacks_priced")))
    return qb.join(ru, on=["week", "team"], how="full", coalesce=True), sk


def backtest_frame(season: int = 2025) -> pl.DataFrame:
    """One row per defence-game: the opposing offence's actual line, lines and props."""
    off = (pl.scan_parquet(paths.DATA_DIR / "NFL" / str(season) / "pbp.parquet")
           .filter(pl.col("season_type") == "REG",
                   pl.col("play_type").is_in(list(allowed.SCRIMMAGE)),
                   pl.col("special_teams_play").fill_null(0) == 0)
           .group_by("week", "posteam", "defteam")
           .agg(pl.col("yards_gained").fill_null(0).sum().alias("yds_allowed"),
                pl.col("sack").fill_null(0).sum().alias("sacks"),
                pl.col("interception").fill_null(0).sum().alias("ints"),
                pl.col("pass_attempt").fill_null(0).sum().alias("dropbacks"),
                pl.col("fumble_lost").fill_null(0).sum().alias("fum_lost"))
           .collect()
           .with_columns(pl.col("week").cast(pl.Int32)))
    offence, sacks = _props(season)
    lines = vegas.team_games([season], use_book_quotes=False).select(
        "week", "team", "implied_allowed", "margin")
    return (off.rename({"defteam": "team", "posteam": "opp"})
            .join(lines.with_columns(pl.col("week").cast(pl.Int32)), on=["week", "team"])
            .join(offence.rename({"team": "opp"}).with_columns(pl.col("week").cast(pl.Int32)),
                  on=["week", "opp"], how="left")
            .join(sacks.with_columns(pl.col("week").cast(pl.Int32)), on=["week", "team"], how="left")
            .with_columns((pl.col("p_pass") + pl.col("p_rush")).alias("p_yds")))


def _loo(df: pl.DataFrame, y: str, feats: Sequence[str]) -> Tuple[float, float]:
    """Leave-one-week-out OLS: RMSE and the correlation of predictions with the truth."""
    w = df["week"].to_numpy()
    X = np.column_stack([np.ones(df.height)]
                        + [df[f].to_numpy().astype(float) for f in feats])
    Y = df[y].to_numpy().astype(float)
    pred = np.empty(df.height)
    for k in np.unique(w):
        tr, te = w != k, w == k
        pred[te] = X[te] @ np.linalg.lstsq(X[tr], Y[tr], rcond=None)[0]
    r = float(np.corrcoef(Y, pred)[0, 1]) if feats else float("nan")
    return float(np.sqrt(np.mean((Y - pred) ** 2))), r


#: Target, the prop features that should carry it, and a label.
BACKTESTS: Tuple[Tuple[str, Tuple[str, ...], str], ...] = (
    ("yds_allowed", ("p_yds",), "yards allowed <- QB pass + rush props"),
    ("ints", ("p_int",), "interceptions <- QB INT prop"),
    ("sacks", ("p_att",), "sacks <- opponent attempts prop"),
    ("sacks", ("p_sacks_priced",), "sacks <- summed defender props"),
    ("dropbacks", ("p_att",), "dropbacks <- opponent attempts prop"),
    ("fum_lost", ("p_att", "p_rush_att"), "fumbles lost <- attempts props"),
)


def backtest(season: int = 2025) -> pl.DataFrame:
    """CV RMSE for constant, lines, props, and lines + props on each component.

    Args:
        season: Season whose props and results to use.

    Returns:
        pl.DataFrame: One row per component with RMSEs and correlations.
    """
    t = backtest_frame(season)
    line = ["implied_allowed", "margin"]
    rows = []
    for y, feats, label in BACKTESTS:
        d = t.drop_nulls([*feats, *line, y])
        const, _ = _loo(d, y, [])
        lr, lc = _loo(d, y, line)
        pr, pc = _loo(d, y, list(feats))
        br, _ = _loo(d, y, line + list(feats))
        rows.append({"component": label, "n": d.height, "sd": float(d[y].std()),
                     "constant": const, "lines": lr, "lines_r": lc,
                     "props": pr, "props_r": pc, "lines_props": br})
    return pl.DataFrame(rows)


def report(season: int = 2025) -> str:
    out: List[str] = [f"===== ESPN definitions, {season} =====",
                      f"  {'quantity':16s}{'definition':44s}{'matches':>12s}"]
    for r in definitions(season).iter_rows(named=True):
        out.append(f"  {r['quantity']:16s}{r['definition']:44s}"
                   f"{r['matches']:>6d}/{r['n']:<5d}")
    out += ["", f"===== opponent props vs game lines, {season}, leave-one-week-out CV RMSE =====",
            f"  {'component':40s}{'n':>5s}{'const':>8s}{'lines':>8s}{'props':>8s}"
            f"{'both':>8s}{'r lines':>9s}{'r props':>9s}"]
    for r in backtest(season).iter_rows(named=True):
        out.append(f"  {r['component']:40s}{r['n']:>5d}{r['constant']:>8.3f}{r['lines']:>8.3f}"
                   f"{r['props']:>8.3f}{r['lines_props']:>8.3f}{r['lines_r']:>+9.3f}"
                   f"{r['props_r']:>+9.3f}")
    return "\n".join(out)


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--season", type=int, default=2025)
    print(report(ap.parse_args(argv).season))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
