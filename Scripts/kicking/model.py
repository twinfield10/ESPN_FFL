"""Fit and project kicker stat lines from team offence.

Everything a league scores is derived from four quantities: extra-point attempts,
field-goal attempts, how those attempts split by distance, and positional conversion
rates. Nothing here is per-kicker except the distance mix, and that is carried at
**team** level on purpose -- it is as much a coach's willingness to send someone out from
52 as it is a leg, and a team key needs no crosswalk join.

Output is a **stat line, not points.** ``proj_to_score`` applies each league's own ladder,
which matters more at this position than anywhere else: two leagues score field-goal
*yardage* (ESPN stat id 214) and one of them has no distance-tier rules at all.
"""

from __future__ import annotations

import json
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts import paths, vegas

#: Distance buckets, mapped from nflverse's finer ones to ESPN's scoring buckets.
BUCKETS: Dict[str, Tuple[str, ...]] = {
    "Under40": ("0_19", "20_29", "30_39"),
    "From40To49": ("40_49",),
    "From50Plus": ("50_59", "60_"),
}

#: Games in a modern season, the slate every projection is expressed over.
SLATE: int = 17

#: Minimum games for a team-season to enter the fit.
MIN_GAMES: int = 14

#: Shrinkage weight on a team's own prior-season red-zone figures. The interaction's
#: input is barely forecastable -- team red-zone conversion persists at r = 0.095 -- so
#: the prior is pulled hard toward the league mean. Fitted, not chosen: see
#: :func:`fit`, which solves for it on held-out seasons.
RZ_SHRINK_GRID: Tuple[float, ...] = (0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 1.0)

#: Where the fitted model lives.
MODEL_PATH = paths.DATA_DIR / "NFL" / "models" / "kicking_1.0.0.json"


def _team_season(seasons: Sequence[int]) -> pl.DataFrame:
    """Team-season kicking volume, red-zone context and line environment."""
    kf, rf = [], []
    for s in seasons:
        d = pl.read_parquet(paths.DATA_DIR / "NFL" / str(s) / "player_weeks.parquet")
        cols = (["season", "week", "team", "position", "fg_att", "fg_made", "fg_missed",
                 "pat_att", "pat_made", "fg_made_distance",
                 "rushing_tds", "receiving_tds"]
                + [f"fg_made_{b}" for bs in BUCKETS.values() for b in bs]
                + [f"fg_missed_{b}" for bs in BUCKETS.values() for b in bs])
        kf.append(d.select([c for c in cols if c in d.columns])
                  .with_columns(pl.col("season").cast(pl.Int32),
                                pl.col("week").cast(pl.Int32)))
        r = pl.read_parquet(paths.DATA_DIR / "NFL" / str(s) / "red_zone.parquet")
        rf.append(r.select("season", "week", "posteam",
                           "team_rz20_carries", "team_rz20_targets")
                  .unique()
                  .with_columns(pl.col("season").cast(pl.Int32),
                                pl.col("week").cast(pl.Int32)))
    pw = pl.concat(kf, how="diagonal_relaxed")
    rz = (pl.concat(rf, how="diagonal_relaxed")
          .group_by(["season", "week", "posteam"])
          .agg((pl.col("team_rz20_carries").fill_null(0)
                + pl.col("team_rz20_targets").fill_null(0)).sum().alias("rz20"))
          .rename({"posteam": "team"}))

    made_cols = [f"fg_made_{b}" for bs in BUCKETS.values() for b in bs]
    miss_cols = [f"fg_missed_{b}" for b in
                 (b for bs in BUCKETS.values() for b in bs)
                 if f"fg_missed_{b}" in pw.columns]
    kick = (pw.filter(pl.col("position") == "K")
            .group_by(["season", "team"])
            .agg(pl.len().alias("kg"),
                 pl.col("fg_att").fill_null(0).sum().alias("fga"),
                 pl.col("fg_made").fill_null(0).sum().alias("fgm"),
                 pl.col("pat_att").fill_null(0).sum().alias("pat_att"),
                 pl.col("pat_made").fill_null(0).sum().alias("pat_made"),
                 pl.col("fg_made_distance").fill_null(0).sum().alias("fg_yards"),
                 *[pl.col(c).fill_null(0).sum().alias(c) for c in made_cols],
                 *[pl.col(c).fill_null(0).sum().alias(c) for c in miss_cols]))
    offence = (pw.group_by(["season", "team"])
               .agg(pl.len().alias("rows"),
                    (pl.col("rushing_tds").fill_null(0)
                     + pl.col("receiving_tds").fill_null(0)).sum().alias("off_tds")))
    games = (pw.select("season", "week", "team").unique()
             .group_by(["season", "team"]).agg(pl.len().alias("games")))
    rzs = rz.group_by(["season", "team"]).agg(pl.col("rz20").sum())

    out = (games.join(kick, on=["season", "team"], how="left")
           .join(offence, on=["season", "team"], how="left")
           .join(rzs, on=["season", "team"], how="left")
           .filter(pl.col("games") >= MIN_GAMES))
    for name, bs in BUCKETS.items():
        out = out.with_columns(
            sum(pl.col(f"fg_made_{b}") for b in bs).alias(f"made_{name}"))
        present = [b for b in bs if f"fg_missed_{b}" in out.columns]
        out = out.with_columns(
            (sum(pl.col(f"fg_missed_{b}") for b in present) if present
             else pl.lit(0.0)).alias(f"missed_{name}"))
    return out.with_columns(
        (pl.col("fga") / pl.col("games")).alias("fga_pg"),
        (pl.col("pat_att") / pl.col("games")).alias("pat_pg"),
        (pl.col("rz20") / pl.col("games")).alias("rz20_pg"),
        (pl.col("off_tds") / pl.col("rz20").replace(0, None)).alias("rz_td_rate"))


def _constants(ts: pl.DataFrame) -> Dict[str, float]:
    """Positional conversion rates, attempt shares and mean made distances."""
    c: Dict[str, float] = {
        "pat_make_rate": float(ts["pat_made"].sum() / max(ts["pat_att"].sum(), 1)),
        "fg_make_rate": float(ts["fgm"].sum() / max(ts["fga"].sum(), 1)),
        "yards_per_make": float(ts["fg_yards"].sum() / max(ts["fgm"].sum(), 1)),
    }
    total_made = sum(ts[f"made_{n}"].sum() for n in BUCKETS)
    for n in BUCKETS:
        c[f"made_share_{n}"] = float(ts[f"made_{n}"].sum() / max(total_made, 1))

    # Misses get their **own** distance shares, measured rather than borrowed.
    #
    # They used to be allocated on `made_share_*`, which is wrong in the one direction
    # that matters: makes concentrate where kicks are easy and misses concentrate where
    # they are hard. Pooled over 2016-2025 -- 8,667 makes against 1,393 misses -- a kick
    # inside 40 is 57.8% of makes and only 15.6% of misses, while a kick from 50+ is
    # 14.7% of makes and 42.4% of misses. So the old allocation over-stated short misses
    # by 3.7x and under-stated long ones by 2.9x. On the 2026 board that put
    # `missedFieldGoalsFromUnder40` at 2.95 a season against ESPN's 0.60, and short
    # misses are a penalty in the leagues that score them.
    #
    # This stays a positional constant, which is the plan's central finding: a *distance
    # band's* make rate is a property of the distance (95.8% / 80.3% / 68.4%), not of the
    # kicker, whose own conversion has a year-over-year r of 0.009.
    total_missed = sum(ts[f"missed_{n}"].sum() for n in BUCKETS)
    for n in BUCKETS:
        c[f"miss_share_{n}"] = (float(ts[f"missed_{n}"].sum() / total_missed)
                                if total_missed else c[f"made_share_{n}"])
        att = float(ts[f"made_{n}"].sum() + ts[f"missed_{n}"].sum())
        c[f"make_rate_{n}"] = float(ts[f"made_{n}"].sum() / att) if att else c["fg_make_rate"]
    return c


def _ols(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Least squares with an intercept column prepended."""
    a = np.column_stack([np.ones(len(x))] + [x[:, i] for i in range(x.shape[1])])
    return np.linalg.lstsq(a, y, rcond=None)[0]


def _design(df: pl.DataFrame, rz_mean_vol: float, rz_mean_conv: float,
            shrink: float) -> np.ndarray:
    """Channel F's regressors: shrunk red-zone volume, conversion and their product."""
    vol = rz_mean_vol + shrink * (df["prior_rz20_pg"].to_numpy() - rz_mean_vol)
    conv = rz_mean_conv + shrink * (df["prior_rz_td_rate"].to_numpy() - rz_mean_conv)
    return np.column_stack([vol, conv, vol * conv, df["implied_own"].to_numpy()])


def fit(seasons: Optional[Sequence[int]] = None,
        holdout: int = 2) -> Dict:
    """Fit both channels and the positional constants.

    Channel P is a one-regressor line on the implied team total. Channel F is the
    red-zone interaction plus the implied total, with the shrinkage on the prior-season
    red-zone terms **selected on held-out seasons** rather than assumed -- the plan
    pre-registers that a heavy shrink is likely to win, because team red-zone conversion
    persists at only r = 0.095.

    Args:
        seasons: Seasons to fit on. 2016-2025 when None.
        holdout: Most recent seasons reserved for choosing the shrinkage.

    Returns:
        dict: Coefficients, constants and diagnostics, ready for :func:`project`.
    """
    seasons = list(range(2016, 2026)) if seasons is None else list(seasons)
    ts = _team_season(seasons)
    lines = (vegas.team_games(seasons).filter(pl.col("priced"))
             .group_by(["season", "team"])
             .agg(pl.col("implied_own").mean(), pl.len().alias("n_priced"))
             .filter(pl.col("n_priced") >= 8))
    prior = ts.select((pl.col("season") + 1).alias("season"), "team",
                      pl.col("rz20_pg").alias("prior_rz20_pg"),
                      pl.col("rz_td_rate").alias("prior_rz_td_rate"))
    d = (ts.join(lines, on=["season", "team"], how="inner")
         .join(prior, on=["season", "team"], how="inner")
         .drop_nulls(["fga_pg", "pat_pg", "implied_own",
                      "prior_rz20_pg", "prior_rz_td_rate"]))

    consts = _constants(ts)
    rz_mean_vol = float(d["prior_rz20_pg"].mean())
    rz_mean_conv = float(d["prior_rz_td_rate"].mean())

    train = d.filter(pl.col("season") < max(seasons) - holdout + 1)
    test = d.filter(pl.col("season") >= max(seasons) - holdout + 1)

    # Channel P: PAT attempts from the implied team total.
    pat_beta = _ols(train.select("implied_own").to_numpy(),
                    train["pat_pg"].to_numpy())
    pat_pred = (pat_beta[0] + pat_beta[1] * test["implied_own"].to_numpy())
    pat_mae = float(np.abs(pat_pred - test["pat_pg"].to_numpy()).mean())
    pat_base = float(np.abs(train["pat_pg"].mean() - test["pat_pg"].to_numpy()).mean())

    # Channel F: pick the shrinkage that generalises, then refit on everything.
    best = None
    for sh in RZ_SHRINK_GRID:
        b = _ols(_design(train, rz_mean_vol, rz_mean_conv, sh),
                 train["fga_pg"].to_numpy())
        p = np.column_stack([np.ones(test.height),
                             _design(test, rz_mean_vol, rz_mean_conv, sh)]) @ b
        mae = float(np.abs(p - test["fga_pg"].to_numpy()).mean())
        if best is None or mae < best[1]:
            best = (sh, mae, b)
    shrink, fga_mae, _ = best
    fga_beta = _ols(_design(d, rz_mean_vol, rz_mean_conv, shrink),
                    d["fga_pg"].to_numpy())
    fga_base = float(np.abs(train["fga_pg"].mean() - test["fga_pg"].to_numpy()).mean())

    return {
        "version": "1.0.0",
        "train_seasons": [int(min(seasons)), int(max(seasons))],
        "n_team_seasons": d.height,
        "constants": consts,
        "rz_mean_vol": rz_mean_vol, "rz_mean_conv": rz_mean_conv,
        "rz_shrink": float(shrink),
        "pat_beta": [float(x) for x in pat_beta],
        "fga_beta": [float(x) for x in fga_beta],
        "diagnostics": {
            "pat_mae": pat_mae, "pat_mae_baseline": pat_base,
            "pat_gain_pct": 100 * (1 - pat_mae / pat_base) if pat_base else 0.0,
            "fga_mae": fga_mae, "fga_mae_baseline": fga_base,
            "fga_gain_pct": 100 * (1 - fga_mae / fga_base) if fga_base else 0.0,
            "holdout_seasons": holdout, "n_test": test.height,
        },
    }


def project(season: int, model: Optional[Dict] = None) -> pl.DataFrame:
    """A full-slate kicking stat line per team.

    Args:
        season: Season to project.
        model: Output of :func:`fit`. Loaded from :data:`MODEL_PATH` when None.

    Returns:
        pl.DataFrame: ``season``, ``team``, ``KIK_<stat>`` columns over a
        :data:`SLATE`-game season, plus ``kik_n_priced`` and ``kik_evidence``.
    """
    model = load() if model is None else model
    c = model["constants"]
    prior = _team_season([season - 1]).select(
        "team", pl.col("rz20_pg").alias("prior_rz20_pg"),
        pl.col("rz_td_rate").alias("prior_rz_td_rate"))
    strength = vegas.team_strength(season).select("season", "team", "implied_own",
                                                  "n_priced")
    d = strength.join(prior, on="team", how="left").with_columns(
        pl.col("prior_rz20_pg").fill_null(model["rz_mean_vol"]),
        pl.col("prior_rz_td_rate").fill_null(model["rz_mean_conv"]))

    pat_pg = (model["pat_beta"][0] + model["pat_beta"][1] * d["implied_own"].to_numpy())
    x = _design(d, model["rz_mean_vol"], model["rz_mean_conv"], model["rz_shrink"])
    fga_pg = np.column_stack([np.ones(d.height), x]) @ np.array(model["fga_beta"])
    # A projection below zero is arithmetic, not football.
    pat_att = np.clip(pat_pg, 0.0, None) * SLATE
    fg_att = np.clip(fga_pg, 0.0, None) * SLATE

    made_total = fg_att * c["fg_make_rate"]
    out = d.select("season", "team", pl.col("n_priced").alias("kik_n_priced")).with_columns(
        pl.Series("KIK_attemptedExtraPoints", pat_att),
        pl.Series("KIK_madeExtraPoints", pat_att * c["pat_make_rate"]),
        pl.Series("KIK_missedExtraPoints", pat_att * (1 - c["pat_make_rate"])),
        pl.Series("KIK_attemptedFieldGoals", fg_att),
        pl.Series("KIK_madeFieldGoals", made_total),
        pl.Series("KIK_missedFieldGoals", fg_att - made_total),
        pl.Series("KIK_214", made_total * c["yards_per_make"]),
    )
    for name in BUCKETS:
        share = c[f"made_share_{name}"]
        col = ("KIK_madeFieldGoalsFromUnder40" if name == "Under40"
               else f"KIK_madeFieldGoalsFrom{name}")
        out = out.with_columns(pl.Series(col, made_total * share))
    # Missed-by-bucket, on the miss distribution rather than the make distribution.
    # See `_constants`: borrowing `made_share_*` here over-stated short misses 3.7x.
    # Shares are normalised so the buckets sum back to `missed` exactly, which keeps the
    # per-bucket columns reconciling with `KIK_missedFieldGoals` however the pooled
    # counts drift.
    missed = fg_att - made_total
    miss_norm = sum(c[f"miss_share_{n}"] for n in BUCKETS) or 1.0
    for name in BUCKETS:
        col = ("KIK_missedFieldGoalsFromUnder40" if name == "Under40"
               else f"KIK_missedFieldGoalsFrom{name}")
        out = out.with_columns(
            pl.Series(col, missed * (c[f"miss_share_{name}"] / miss_norm)))
    return out.with_columns(
        pl.when(pl.col("kik_n_priced") == 0)
        .then(pl.lit("no line; league-average environment"))
        .otherwise(pl.lit("")).alias("kik_evidence"))


def save(model: Dict) -> None:
    """Write the fitted model beside the other model artifacts."""
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    MODEL_PATH.write_text(json.dumps(model, indent=2))


def load() -> Dict:
    """Read the fitted model.

    Raises:
        FileNotFoundError: When it has not been fitted yet.
    """
    if not MODEL_PATH.is_file():
        raise FileNotFoundError(
            f"No kicking model at {MODEL_PATH}. Run "
            "`python -m Scripts.kicking.model --fit --write`.")
    return json.loads(MODEL_PATH.read_text())


def projection_path(season: int, create: bool = False):
    """Where the per-team kicking projection is written."""
    p = paths.PROJECTIONS_DIR / "Kicking" / "Season" / str(season)
    if create:
        p.mkdir(parents=True, exist_ok=True)
    return p / "Kicking_SeasonProjections.parquet"


def report(model: Dict) -> str:
    """The fit and its held-out diagnostics, as text."""
    d, c = model["diagnostics"], model["constants"]
    return "\n".join([
        f"=== kicking model {model['version']} "
        f"({model['train_seasons'][0]}-{model['train_seasons'][1]}, "
        f"n={model['n_team_seasons']} team-seasons) ===",
        f"  channel P  PAT/gm = {model['pat_beta'][0]:+.3f} "
        f"{model['pat_beta'][1]:+.4f} x implied_own",
        f"             held-out MAE {d['pat_mae']:.4f} against a constant's "
        f"{d['pat_mae_baseline']:.4f}  ({d['pat_gain_pct']:+.1f}%)",
        f"  channel F  FGA/gm intercept {model['fga_beta'][0]:+.3f}, "
        f"vol {model['fga_beta'][1]:+.4f}, conv {model['fga_beta'][2]:+.3f}, "
        f"vol x conv {model['fga_beta'][3]:+.3f}, implied {model['fga_beta'][4]:+.4f}",
        f"             red-zone shrink selected = {model['rz_shrink']:.2f}"
        f"   (1.0 = trust the prior season, 0.0 = league mean)",
        f"             held-out MAE {d['fga_mae']:.4f} against a constant's "
        f"{d['fga_mae_baseline']:.4f}  ({d['fga_gain_pct']:+.1f}%)",
        f"  constants  PAT make {c['pat_make_rate']:.4f}, FG make {c['fg_make_rate']:.4f}, "
        f"yards/make {c['yards_per_make']:.2f}",
        f"             made shares  <40 {c['made_share_Under40']:.3f}  "
        f"40-49 {c['made_share_From40To49']:.3f}  "
        f"50+ {c['made_share_From50Plus']:.3f}",
    ])


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--fit", action="store_true")
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args(argv)

    model = fit() if (a.fit or not MODEL_PATH.is_file()) else load()
    print(report(model))
    if a.write:
        save(model)
        pred = project(a.season, model)
        path = projection_path(a.season, create=True)
        pred.write_parquet(path)
        print(f"\n  wrote {pred.height} teams -> {path}")
        top = pred.sort("KIK_madeFieldGoals", descending=True).head(5)
        print("\n  most field goals projected:")
        for r in top.iter_rows(named=True):
            print(f"    {r['team']:4s} FGA {r['KIK_attemptedFieldGoals']:5.1f}  "
                  f"FGM {r['KIK_madeFieldGoals']:5.1f}  "
                  f"PAT {r['KIK_madeExtraPoints']:5.1f}  "
                  f"FG yards {r['KIK_214']:6.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
