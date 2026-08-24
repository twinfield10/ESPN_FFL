"""Game lines, and the two team-level quantities every special-teams model needs.

A closing total and spread imply each team's expected points and each team's expected
points *allowed*. Those two numbers are the best available predictor of almost everything
a kicker or a defence does -- measured, on this repo's own 2016-2025 data:

===========================  ============  =================
quantity                     r vs Vegas    r vs prior season
===========================  ============  =================
PAT attempts / game               0.844                0.399
team offensive TDs / game         0.848                0.360
points allowed / game             0.816                0.277
yards allowed / game              0.702                0.260
sacks / game                     -0.464                0.203
interceptions / game             -0.357                0.113
fumble recoveries / game         -0.193                0.015
FG attempts / game                0.117                0.058
===========================  ============  =================

**The sign convention is asserted, not commented.** ``spread_line`` is the *home* team's
margin and positive means the home team is favoured. Deriving it the other way round is
silent: every implied total inverts, the model ranks good offences as bad ones, and the
result looks clean and publishable. It happened while measuring
``docs/plans/29-kicker-model.md`` -- an inverted ``r(implied, PAT attempts)`` of -0.272 in
place of the true +0.844. So :func:`assert_sign_convention` runs on every load and raises
rather than warns.

**Only part of a season is priced before it starts, and that changes the design.** On the
2026 pull, 52 of 272 games carry a line -- weeks 1 to 4, giving 3 to 4 priced games per
team and all 32 teams covered. A season model therefore cannot average seventeen lines; it
averages three and shrinks. Measured over 320 historical team-seasons, an estimate built
from weeks 1-4 alone predicts the full-season figure at r = 0.845 for implied own total,
0.810 for spread and 0.727 for implied points allowed -- usable, but with a standard
deviation 17% wider than the quantity it estimates, so :func:`team_strength` shrinks
toward the league mean by a factor fitted from history rather than chosen.

See ``docs/plans/29-kicker-model.md`` and ``docs/plans/30-dst-model.md``.
"""

from __future__ import annotations

from typing import Dict, Optional, Sequence, Tuple

import polars as pl

from Scripts import paths

#: Smallest acceptable correlation between ``spread_line`` and the realised home margin.
#: Well below the observed +0.446 -- this is a sign-and-sanity gate, not a fit test.
MIN_SIGN_R: float = 0.20

#: Weeks a pre-season pull can be expected to have priced. Used only for reporting how
#: much of a season's line coverage is present; nothing filters on it.
EARLY_WEEKS: int = 4


def load_schedules(seasons: Optional[Sequence[int]] = None) -> pl.DataFrame:
    """Regular-season games with their closing lines.

    Args:
        seasons: Season years to keep. All seasons when None.

    Returns:
        pl.DataFrame: ``game_id``, ``season``, ``week``, ``home_team``, ``away_team``,
        ``home_score``, ``away_score``, ``result``, ``total_line``, ``spread_line``.

    Raises:
        FileNotFoundError: When the schedule pull is missing.
    """
    path = paths.DATA_DIR / "NFL" / "schedules.parquet"
    if not path.is_file():
        raise FileNotFoundError(
            f"No schedule pull at {path}. Run `Rscript R/GetNFL.R <season>`.")
    df = (pl.read_parquet(path)
          .filter(pl.col("game_type") == "REG")
          .with_columns(pl.col("season").cast(pl.Int32), pl.col("week").cast(pl.Int32)))
    if seasons is not None:
        df = df.filter(pl.col("season").is_in(sorted(set(seasons))))
    return df


def assert_sign_convention(schedules: Optional[pl.DataFrame] = None) -> float:
    """Prove that a positive ``spread_line`` means the home team is favoured.

    The gate ``docs/plans/29-kicker-model.md`` registers as G-K0 and
    ``docs/plans/30-dst-model.md`` as G-DST0. It is an assertion rather than a comment
    because the failure it guards against produces a clean, plausible, entirely inverted
    model instead of an error.

    Args:
        schedules: Frame from :func:`load_schedules`. Loaded for all seasons when None.

    Returns:
        float: The measured correlation between ``spread_line`` and the realised home
        margin. Observed at +0.446 over 2016-2025.

    Raises:
        ValueError: When the correlation is below :data:`MIN_SIGN_R`, which means either
            the convention flipped upstream or the wrong column is being read.
    """
    df = load_schedules() if schedules is None else schedules
    played = df.filter(pl.col("result").is_not_null()
                       & pl.col("spread_line").is_not_null())
    if played.height < 100:
        raise ValueError(
            f"Only {played.height} played games with a line; cannot verify the "
            "spread_line sign convention. Refusing to derive implied totals blind.")
    r = played.select(pl.corr("spread_line", "result")).item()
    if r is None or r < MIN_SIGN_R:
        raise ValueError(
            f"spread_line correlates {r} with the realised home margin, below the "
            f"{MIN_SIGN_R} this module requires. A positive spread_line must mean the "
            "home team is favoured; if that changed upstream, every implied total "
            "derived here is inverted. See Scripts/vegas.py.")
    return float(r)


def team_games(seasons: Optional[Sequence[int]] = None,
               verify: bool = True) -> pl.DataFrame:
    """One row per team per game, with that team's own line and implied totals.

    Args:
        seasons: Season years to keep. All seasons when None.
        verify: Run :func:`assert_sign_convention` first. Only turn this off in a test
            that is deliberately feeding a synthetic frame.

    Returns:
        pl.DataFrame: ``season``, ``week``, ``team``, ``opponent``, ``is_home``,
        ``total_line``, ``margin`` (this team's spread, positive = favoured),
        ``implied_own``, ``implied_allowed``, ``points_scored``, ``points_allowed``,
        ``actual_margin``, and ``priced`` (whether a line exists at all).

    Raises:
        ValueError: From :func:`assert_sign_convention`.
    """
    df = load_schedules(seasons)
    if verify:
        assert_sign_convention()
    home = df.select(
        "season", "week", "game_id",
        pl.col("home_team").alias("team"), pl.col("away_team").alias("opponent"),
        pl.lit(True).alias("is_home"), "total_line",
        pl.col("spread_line").alias("margin"),
        pl.col("home_score").alias("points_scored"),
        pl.col("away_score").alias("points_allowed"),
        pl.col("result").alias("actual_margin"))
    away = df.select(
        "season", "week", "game_id",
        pl.col("away_team").alias("team"), pl.col("home_team").alias("opponent"),
        pl.lit(False).alias("is_home"), "total_line",
        (-pl.col("spread_line")).alias("margin"),
        pl.col("away_score").alias("points_scored"),
        pl.col("home_score").alias("points_allowed"),
        (-pl.col("result")).alias("actual_margin"))
    return (pl.concat([home, away])
            .with_columns(
                # The identity: a team's implied points is half the total plus half its
                # own margin, and its implied points allowed is half the total minus it.
                (pl.col("total_line") / 2 + pl.col("margin") / 2).alias("implied_own"),
                (pl.col("total_line") / 2 - pl.col("margin") / 2).alias("implied_allowed"),
                (pl.col("total_line").is_not_null()
                 & pl.col("margin").is_not_null()).alias("priced"))
            .sort(["season", "week", "team"]))


#: Quantities :func:`team_strength` averages and shrinks.
STRENGTH_COLUMNS: Tuple[str, ...] = ("margin", "total_line", "implied_own",
                                     "implied_allowed")


def fit_shrinkage(seasons: Optional[Sequence[int]] = None,
                  max_week: int = EARLY_WEEKS,
                  min_full_games: int = 14) -> Dict[str, Dict[str, float]]:
    """How much to trust an early-weeks average as a full-season estimate.

    A pre-season pull prices only the first few weeks, so a season model averages three
    or four lines rather than seventeen. That average is both **noisier** than the
    quantity it estimates and **regressive** toward it, so the optimal linear predictor
    is ``mean + slope x (early - mean)`` with ``slope = r x sd_full / sd_early`` -- the
    textbook shrinkage, fitted here rather than assumed.

    Measured 2016-2025: r is 0.845 for implied own total, 0.810 for spread and 0.727 for
    implied points allowed, with the early estimate's standard deviation about 17% wider.

    Args:
        seasons: Seasons to fit on. Excludes any season with fewer than
            ``min_full_games`` priced games per team, which is how the in-progress
            season excludes itself.
        max_week: Last week the early estimate may see.
        min_full_games: Priced games a team-season needs to serve as ground truth.

    Returns:
        dict: ``{column: {"mean", "slope", "r", "sd_early", "sd_full", "n"}}``.
    """
    tg = team_games(seasons).filter(pl.col("priced"))
    early = (tg.filter(pl.col("week") <= max_week)
             .group_by(["season", "team"])
             .agg(*[pl.col(c).mean().alias(f"{c}_early") for c in STRENGTH_COLUMNS],
                  pl.len().alias("n_early")))
    full = (tg.group_by(["season", "team"])
            .agg(*[pl.col(c).mean().alias(f"{c}_full") for c in STRENGTH_COLUMNS],
                 pl.len().alias("n_full"))
            .filter(pl.col("n_full") >= min_full_games))
    j = early.join(full, on=["season", "team"])
    out: Dict[str, Dict[str, float]] = {}
    for c in STRENGTH_COLUMNS:
        sub = j.filter(pl.col(f"{c}_early").is_not_null()
                       & pl.col(f"{c}_full").is_not_null())
        if sub.height < 30:
            continue
        r = sub.select(pl.corr(f"{c}_early", f"{c}_full")).item()
        sd_e = sub[f"{c}_early"].std()
        sd_f = sub[f"{c}_full"].std()
        out[c] = {
            "mean": float(sub[f"{c}_full"].mean()),
            "slope": float(r * sd_f / sd_e) if r is not None and sd_e else 0.0,
            "r": float(r) if r is not None else 0.0,
            "sd_early": float(sd_e), "sd_full": float(sd_f), "n": sub.height,
        }
    return out


def team_strength(season: int,
                  shrinkage: Optional[Dict[str, Dict[str, float]]] = None,
                  fit_seasons: Optional[Sequence[int]] = None) -> pl.DataFrame:
    """Each team's season-long line environment, shrunk for partial pricing.

    Args:
        season: Season to estimate.
        shrinkage: Output of :func:`fit_shrinkage`. Fitted on ``fit_seasons`` when None.
        fit_seasons: Seasons to fit the shrinkage on. Every season before ``season``
            when None, which keeps it leakage-free.

    Returns:
        pl.DataFrame: ``season``, ``team``, ``n_priced``, ``games``, and for each of
        :data:`STRENGTH_COLUMNS` a raw ``<col>_raw`` and a shrunk ``<col>`` estimate.
        Teams with no priced game at all get the league mean and ``n_priced`` of 0,
        which is visible rather than silent.
    """
    if shrinkage is None:
        fit = fit_seasons if fit_seasons is not None else range(2016, season)
        shrinkage = fit_shrinkage(list(fit))
    tg = team_games([season])
    games = tg.group_by(["season", "team"]).agg(pl.len().alias("games"))
    priced = (tg.filter(pl.col("priced"))
              .group_by(["season", "team"])
              .agg(*[pl.col(c).mean().alias(f"{c}_raw") for c in STRENGTH_COLUMNS],
                   pl.len().alias("n_priced")))
    out = games.join(priced, on=["season", "team"], how="left").with_columns(
        pl.col("n_priced").fill_null(0))
    for c in STRENGTH_COLUMNS:
        p = shrinkage.get(c)
        if p is None:
            out = out.with_columns(pl.col(f"{c}_raw").alias(c))
            continue
        out = out.with_columns(
            pl.when(pl.col(f"{c}_raw").is_null())
            .then(pl.lit(p["mean"]))
            .otherwise(pl.lit(p["mean"])
                       + pl.lit(p["slope"]) * (pl.col(f"{c}_raw") - pl.lit(p["mean"])))
            .alias(c))
    return out.sort("team")


def report(season: int) -> str:
    """Line coverage and the fitted shrinkage, as text.

    Args:
        season: Season to describe.

    Returns:
        str: The rendered report.
    """
    r = assert_sign_convention()
    tg = team_games([season])
    n, np_ = tg.height // 2, tg.filter(pl.col("priced")).height // 2
    weeks = sorted(tg.filter(pl.col("priced"))["week"].unique().to_list())
    shrink = fit_shrinkage(list(range(2016, season)))
    st = team_strength(season, shrinkage=shrink)
    lines = [
        f"=== Vegas lines, {season} ===",
        f"  sign convention verified: r(spread_line, home margin) = +{r:.3f}",
        f"  games priced: {np_} of {n}" + (f"  weeks {weeks[0]}-{weeks[-1]}" if weeks else ""),
        f"  priced games per team: min {st['n_priced'].min()}, "
        f"mean {st['n_priced'].mean():.2f}, max {st['n_priced'].max()}",
        "",
        "  shrinkage fitted on prior seasons (slope 1.0 = trust the early average):",
        f"    {'quantity':20s} {'r':>6s} {'slope':>7s} {'mean':>7s} {'n':>5s}",
    ]
    for c, p in shrink.items():
        lines.append(f"    {c:20s} {p['r']:6.3f} {p['slope']:7.3f} "
                     f"{p['mean']:7.2f} {p['n']:5d}")
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--season", type=int, default=2026)
    print(report(ap.parse_args().season))
