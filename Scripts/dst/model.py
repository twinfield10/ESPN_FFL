"""Fit and project the D/ST component vector from game lines.

Two kinds of output, and the second is the reason this module is not a one-liner:

**Rate components** -- sacks, interceptions, fumble recoveries, safeties, defensive
touchdowns, tackles for loss -- are linear in implied points allowed and the spread.

**Tiered components** -- points allowed and yards allowed -- are *step functions of a
weekly quantity*, so a season projection has to carry the weekly distribution and
integrate the ladder over it. The output is therefore expected **games in each tier**,
summing to the slate, exactly the shape ESPN publishes. The residual distribution is
empirical rather than Gaussian: weekly points allowed is right-skewed and bounded below
at zero, and a normal approximation misprices the shutout tier that pays the most.
"""

from __future__ import annotations

import json
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts import paths, vegas

SLATE: int = 17
MIN_GAMES: int = 14

#: Points-allowed ladder, as ESPN names the buckets.
PA_TIERS: Tuple[Tuple[str, int, int], ...] = (
    ("0PointsAllowed", 0, 0), ("1To6PointsAllowed", 1, 6),
    ("7To13PointsAllowed", 7, 13), ("14To17PointsAllowed", 14, 17),
    ("18To21PointsAllowed", 18, 21), ("22To27PointsAllowed", 22, 27),
    ("28To34PointsAllowed", 28, 34), ("35To45PointsAllowed", 35, 45),
    ("45PlusPointsAllowed", 46, 10_000),
)
#: Yards-allowed ladder. ``0To99`` is included for completeness and is essentially never
#: reached -- no team-game in 2016-2025 comes close -- which is worth seeing as a zero
#: rather than as a missing column.
YD_TIERS: Tuple[Tuple[str, int, int], ...] = (
    ("0To99YardsAllowed", 0, 99), ("100To199YardsAllowed", 100, 199),
    ("200To299YardsAllowed", 200, 299), ("300To349YardsAllowed", 300, 349),
    ("350To399YardsAllowed", 350, 399), ("400To449YardsAllowed", 400, 449),
    ("450To499YardsAllowed", 450, 499), ("500To549YardsAllowed", 500, 549),
    ("550PlusYardsAllowed", 550, 100_000),
)

#: Rate components, mapped from the nflverse column to the ESPN scoring name.
RATES: Dict[str, str] = {
    "def_sacks": "defensiveSacks",
    "def_interceptions": "defensiveInterceptions",
    "fumble_recovery_opp": "defensiveFumbles",
    "def_safeties": "defensiveSafeties",
    "def_fumbles_forced": "defensiveForcedFumbles",
    "def_tackles_for_loss": "defensiveStuffs",
    "def_pass_defended": "defensivePassesDefensed",
    "def_tackles_solo": "defensiveSoloTackles",
    "def_tackle_assists": "defensiveAssistedTackles",
}

#: How a defensive touchdown splits between an interception return and a fumble return.
#: ``player_weeks`` carries only the combined ``def_tds``, and the two are priced
#: separately (though at the same 5.0 in every configured league, which is why a constant
#: split is harmless here and would not be if a league ever priced them differently).
INT_TD_SHARE: float = 0.60

MODEL_PATH = paths.DATA_DIR / "NFL" / "models" / "dst_1.0.0.json"


def team_weeks(seasons: Sequence[int]) -> pl.DataFrame:
    """Team-week defence: components, points and yards allowed, and the game's line."""
    frames = []
    for s in seasons:
        d = pl.read_parquet(paths.DATA_DIR / "NFL" / str(s) / "player_weeks.parquet")
        keep = (["season", "week", "team", "opponent_team", "passing_yards",
                 "rushing_yards", "def_tds"] + [c for c in RATES if c in d.columns])
        frames.append(d.select([c for c in keep if c in d.columns])
                      .with_columns(pl.col("season").cast(pl.Int32),
                                    pl.col("week").cast(pl.Int32)))
    pw = pl.concat(frames, how="diagonal_relaxed")
    have = [c for c in RATES if c in pw.columns]
    dfn = (pw.group_by(["season", "week", "team"])
           .agg(*[pl.col(c).fill_null(0).sum().alias(c) for c in have],
                pl.col("def_tds").fill_null(0).sum().alias("def_tds")))
    allowed = (pw.group_by(["season", "week", "opponent_team"])
               .agg((pl.col("passing_yards").fill_null(0)
                     + pl.col("rushing_yards").fill_null(0)).sum().alias("yards_allowed"))
               .rename({"opponent_team": "team"}))
    lines = vegas.team_games(seasons).select(
        "season", "week", "team", "implied_allowed", "margin", "total_line",
        "points_allowed", "priced")
    return (dfn.join(allowed, on=["season", "week", "team"], how="left")
            .join(lines, on=["season", "week", "team"], how="inner")
            .with_columns(pl.col("yards_allowed").fill_null(0)))


def _ols(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    a = np.column_stack([np.ones(len(x))] + [x[:, i] for i in range(x.shape[1])])
    return np.linalg.lstsq(a, y, rcond=None)[0]


def _tier_probs(mean: np.ndarray, residuals: np.ndarray,
                tiers: Sequence[Tuple[str, int, int]]) -> np.ndarray:
    """Integrate a step ladder over an empirical residual distribution.

    ``E[f(X)]``, not ``f(E[X])``. For each team the weekly quantity is modelled as its
    predicted mean plus the pooled empirical residual, and the share of that shifted
    distribution landing in each tier is the expected fraction of games there. Using the
    empirical residuals rather than a Gaussian matters at the ends: weekly points allowed
    is right-skewed and floored at zero, and a normal would invent shutouts.

    Args:
        mean: One predicted weekly mean per team, shape ``(n_teams,)``.
        residuals: Pooled ``observed - team mean`` values, shape ``(n_resid,)``.
        tiers: ``(name, low, high)`` triples, inclusive.

    Returns:
        np.ndarray: Shape ``(n_teams, n_tiers)``, each row summing to 1.
    """
    # Round and floor at zero, because both quantities are non-negative integers and the
    # ladders are stated over integers. Without the rounding the `0 points allowed` tier
    # is `(0, 0)` against a continuous draw and collects probability zero -- it produced
    # 0.00 projected shutouts for every defence against ESPN's 0.31, which is the kind of
    # wrong that a summing-to-17 check does not catch. Flooring is not a fudge either: a
    # residual implying -3 points allowed is a game the defence pitched a shutout in.
    draws = np.clip(np.round(mean[:, None] + residuals[None, :]), 0.0, None)
    out = np.empty((len(mean), len(tiers)))
    for j, (_, lo, hi) in enumerate(tiers):
        out[:, j] = ((draws >= lo) & (draws <= hi)).mean(axis=1)
    total = out.sum(axis=1, keepdims=True)
    return np.divide(out, total, out=np.zeros_like(out), where=total > 0)


def fit(seasons: Optional[Sequence[int]] = None, holdout: int = 2) -> Dict:
    """Fit the rate components and the two tiered distributions.

    Rates regress on implied points allowed **and** the spread, which
    ``docs/plans/30-dst-model.md`` measured as a second channel rather than a proxy. Each
    fitted coefficient is reported beside the improvement it buys on held-out seasons, and
    a component whose fit does not beat its own mean is **shrunk to that mean** -- the
    correct answer for a coin flip, and the plan pre-registers fumble recoveries and
    safeties as the likely cases.

    Args:
        seasons: Seasons to fit on. 2016-2025 when None.
        holdout: Most recent seasons held out for the comparison.

    Returns:
        dict: Coefficients, residual pools and diagnostics.
    """
    seasons = list(range(2016, 2026)) if seasons is None else list(seasons)
    tw = team_weeks(seasons).filter(pl.col("priced"))
    games = (tw.group_by(["season", "team"]).agg(pl.len().alias("g"))
             .filter(pl.col("g") >= MIN_GAMES).select("season", "team"))
    tw = tw.join(games, on=["season", "team"])

    have = [c for c in RATES if c in tw.columns]
    ts = (tw.group_by(["season", "team"])
          .agg(pl.len().alias("g"), pl.col("implied_allowed").mean(),
               pl.col("margin").mean(), pl.col("total_line").mean(),
               pl.col("points_allowed").mean().alias("pa_pg"),
               pl.col("yards_allowed").mean().alias("yd_pg"),
               pl.col("def_tds").sum(),
               *[pl.col(c).sum() for c in have])
          .with_columns(*[(pl.col(c) / pl.col("g")).alias(f"{c}_pg") for c in have],
                        (pl.col("def_tds") / pl.col("g")).alias("def_tds_pg")))

    cut = max(seasons) - holdout + 1
    train, test = ts.filter(pl.col("season") < cut), ts.filter(pl.col("season") >= cut)
    X = lambda f: np.column_stack([f["implied_allowed"].to_numpy(), f["margin"].to_numpy()])

    rates: Dict[str, Dict] = {}
    for c in have + ["def_tds"]:
        col = f"{c}_pg"
        beta = _ols(X(train), train[col].to_numpy())
        pred = np.column_stack([np.ones(test.height), X(test)]) @ beta
        mae = float(np.abs(pred - test[col].to_numpy()).mean())
        base = float(np.abs(train[col].mean() - test[col].to_numpy()).mean())
        gain = 100 * (1 - mae / base) if base else 0.0
        # A component the market cannot beat its own mean on is shrunk to that mean.
        use = gain > 0.0
        rates[c] = {"beta": [float(b) for b in _ols(X(ts), ts[col].to_numpy())] if use
                    else [float(ts[col].mean()), 0.0, 0.0],
                    "mae": mae, "mae_baseline": base, "gain_pct": gain,
                    "shrunk_to_mean": not use, "mean": float(ts[col].mean())}

    tiers = {}
    for name, mean_col, tier_col, ladder in (
            ("points_allowed", "pa_pg", "points_allowed", PA_TIERS),
            ("yards_allowed", "yd_pg", "yards_allowed", YD_TIERS)):
        beta = _ols(X(train), train[mean_col].to_numpy())
        pred = np.column_stack([np.ones(test.height), X(test)]) @ beta
        mae = float(np.abs(pred - test[mean_col].to_numpy()).mean())
        base = float(np.abs(train[mean_col].mean() - test[mean_col].to_numpy()).mean())
        # Pooled within-team residuals: the weekly spread a season mean has to be
        # smeared over before a step ladder is applied to it.
        j = tw.join(ts.select("season", "team", pl.col(mean_col).alias("mu")),
                    on=["season", "team"])
        resid = (j[tier_col].to_numpy() - j["mu"].to_numpy())
        tiers[name] = {
            "beta": [float(b) for b in _ols(X(ts), ts[mean_col].to_numpy())],
            "mae": mae, "mae_baseline": base,
            "gain_pct": 100 * (1 - mae / base) if base else 0.0,
            "residuals": [float(v) for v in np.round(resid, 3)],
            "resid_sd": float(resid.std()),
        }

    return {"version": "1.0.0",
            "train_seasons": [int(min(seasons)), int(max(seasons))],
            "n_team_seasons": ts.height, "holdout_seasons": holdout,
            "rates": rates, "tiers": tiers,
            "int_td_share": INT_TD_SHARE}


def project(season: int, model: Optional[Dict] = None) -> pl.DataFrame:
    """A full-slate D/ST component vector per team.

    Args:
        season: Season to project.
        model: Output of :func:`fit`. Loaded from :data:`MODEL_PATH` when None.

    Returns:
        pl.DataFrame: ``season``, ``team``, ``DST_<stat>`` over a :data:`SLATE`-game
        season -- rate components, both tier vectors as expected games, and the two
        season totals -- plus ``dst_n_priced`` and ``dst_evidence``.
    """
    model = load() if model is None else model
    st = vegas.team_strength(season)
    x = np.column_stack([np.ones(st.height), st["implied_allowed"].to_numpy(),
                         st["margin"].to_numpy()])

    out = st.select("season", "team", pl.col("n_priced").alias("dst_n_priced"))
    for c, spec in model["rates"].items():
        per_game = np.clip(x @ np.array(spec["beta"]), 0.0, None)
        if c == "def_tds":
            share = model["int_td_share"]
            out = out.with_columns(
                pl.Series("DST_interceptionReturnTouchdowns", per_game * SLATE * share),
                pl.Series("DST_fumbleReturnTouchdowns", per_game * SLATE * (1 - share)),
                pl.Series("DST_fumbleRecoveredForTD", per_game * SLATE * (1 - share)),
                pl.Series("DST_defensiveTouchdowns", per_game * SLATE))
        else:
            out = out.with_columns(pl.Series(f"DST_{RATES[c]}", per_game * SLATE))

    for name, ladder in (("points_allowed", PA_TIERS), ("yards_allowed", YD_TIERS)):
        spec = model["tiers"][name]
        mean = x @ np.array(spec["beta"])
        probs = _tier_probs(mean, np.array(spec["residuals"]), ladder)
        for j, (tier, _, _) in enumerate(ladder):
            out = out.with_columns(
                pl.Series(f"DST_defensive{tier}", probs[:, j] * SLATE))
        total = "DST_defensivePointsAllowed" if name == "points_allowed" \
            else "DST_defensiveYardsAllowed"
        out = out.with_columns(pl.Series(total, np.clip(mean, 0.0, None) * SLATE))

    return out.with_columns(
        pl.when(pl.col("dst_n_priced") == 0)
        .then(pl.lit("no line; league-average environment"))
        .otherwise(pl.lit("")).alias("dst_evidence"))


def save(model: Dict) -> None:
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    MODEL_PATH.write_text(json.dumps(model))


def load() -> Dict:
    if not MODEL_PATH.is_file():
        raise FileNotFoundError(
            f"No D/ST model at {MODEL_PATH}. Run "
            "`python -m Scripts.dst.model --fit --write`.")
    return json.loads(MODEL_PATH.read_text())


def projection_path(season: int, create: bool = False):
    p = paths.PROJECTIONS_DIR / "DST" / "Season" / str(season)
    if create:
        p.mkdir(parents=True, exist_ok=True)
    return p / "DST_SeasonProjections.parquet"


def report(model: Dict) -> str:
    lines = [f"=== D/ST model {model['version']} "
             f"({model['train_seasons'][0]}-{model['train_seasons'][1]}, "
             f"n={model['n_team_seasons']} team-seasons, "
             f"{model['holdout_seasons']} held out) ===",
             "  rate components  (gain = held-out MAE against the component's own mean)",
             f"    {'component':24s} {'intercept':>10s} {'impAllw':>9s} {'margin':>8s} "
             f"{'gain':>7s}  verdict"]
    for c, s in sorted(model["rates"].items(), key=lambda kv: -kv[1]["gain_pct"]):
        v = "shrunk to mean" if s["shrunk_to_mean"] else "market"
        lines.append(f"    {c:24s} {s['beta'][0]:10.4f} {s['beta'][1]:9.4f} "
                     f"{s['beta'][2]:8.4f} {s['gain_pct']:6.1f}%  {v}")
    lines.append("  tiered components")
    for n, s in model["tiers"].items():
        lines.append(f"    {n:24s} mean = {s['beta'][0]:.3f} "
                     f"{s['beta'][1]:+.4f} x impAllw {s['beta'][2]:+.4f} x margin"
                     f"   held-out gain {s['gain_pct']:+.1f}%"
                     f"   weekly resid SD {s['resid_sd']:.2f}")
    return "\n".join(lines)


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
        pa = [f"DST_defensive{t}" for t, _, _ in PA_TIERS]
        chk = pred.with_columns(sum(pl.col(c) for c in pa).alias("tier_sum"))
        print(f"  points-allowed tier vectors sum to "
              f"[{chk['tier_sum'].min():.3f}, {chk['tier_sum'].max():.3f}] "
              f"(must be {SLATE})")
        top = pred.sort("DST_defensiveSacks", descending=True).head(5)
        print("\n  most sacks projected:")
        for r in top.iter_rows(named=True):
            print(f"    {r['team']:4s} sacks {r['DST_defensiveSacks']:5.1f}  "
                  f"INT {r['DST_defensiveInterceptions']:4.1f}  "
                  f"pts allowed {r['DST_defensivePointsAllowed']:6.1f}  "
                  f"shutouts {r['DST_defensive0PointsAllowed']:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
