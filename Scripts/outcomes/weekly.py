"""How wrong a weekly projection is, fitted -- and therefore what a matchup is worth.

    python -m Scripts.outcomes.weekly --fit
    python -m Scripts.outcomes.weekly --report

:mod:`Scripts.outcomes.distribution` prices a *season*. This prices a *week*, which is
a different question with a different answer, and the Matchup tab's win probability
rests entirely on it: a projected margin is not a decision until you know how wide the
distribution around it is.

**The form is borrowed; the parameters are measured.** Plan 28 established
``Var = φ·μ + μ²/k`` as the mean-variance function for season points -- a Poisson-like
term plus a proportional one, so a smaller projection gets a proportionally wider
interval. Refitted here on weekly residuals, per position, over
``Data/Store/2025/*/lineups.parquet``: nine leagues, seventeen weeks, ~16,000
started-and-active player-weeks carrying both an actual and a ``TRUE_Points``.

**What the fit says, and the one surprise.** Skill positions want both terms; kickers
want only the proportional one; team defences and tight ends want only the linear one.
Per *player*, the normal 80% interval over-covers -- 0.82 at receiver up to 0.88 at
tight end -- because weekly fantasy residuals are right-skewed with a floor at zero,
so a symmetric interval on a skewed distribution catches more than it claims.

That does not survive aggregation, and aggregation is what a matchup is. Summed over a
starting lineup the central limit theorem does its work and **team-level coverage lands
at 0.802 against a nominal 0.800**, with a predicted team sd of 23.22 against a
realised 22.89. So the per-player over-coverage is a fact about the marginals and not a
defect in the totals the win probability is read off.

**Independence is measured, not assumed.** The obvious objection to
``sd_team = sqrt(Σ sd_i²)`` is correlation: a quarterback and his own receivers rise
together, so a real total should be *wider* than the independent sum. Measured, it is
not -- predicted 23.22 against realised 22.89, if anything a shade wide. A fantasy
starting lineup is mostly nine players on nine different NFL teams, and the stacking
that would drive correlation is the exception. The residual correlation is small enough
that modelling it would move no decision, and that is a finding rather than a
convenience: see :data:`GATES`.

**No Monte Carlo.** An earlier plan for this used 10,000 draws per matchup. Once the
normal approximation is calibrated to within 0.2pp there is nothing left for sampling
to add except its own noise, so the win probability is the closed form
``Φ((μ_a - μ_b) / sqrt(sd_a² + sd_b²))``. Exact, instant, and no seed to remember.

See ``docs/plans/28-outcome-distributions.md`` and
``docs/plans/42-weekly-matchup-odds.md``.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts import paths
from Scripts.scrape_player_stats import FREE_AGENT_OWNER

#: Bump when the fitted form changes meaning. Readers refuse a version they do not
#: understand rather than mis-read its coefficients.
MODEL_VERSION = "1.0.0"

MODEL_PATH = paths.DATA_DIR / "NFL" / "models" / f"weekly_dispersion_{MODEL_VERSION}.json"

#: Seasons the fit trains on.
#:
#: One season, and that is a real limit rather than a choice. ``lineups.parquet``
#: **cannot be rebuilt for a past season** -- it carries ``FP_``/``PINNY_``/``BOL_``
#: columns and FantasyPros serves no season parameter, so the blend inputs for 2024
#: no longer exist anywhere. 2025 is the first season the store was kept for, so it is
#: the whole training set that will ever be available for it. Each season from here
#: adds one.
TRAIN_SEASONS: Tuple[int, ...] = (2025,)

#: Slots whose points do not count.
BENCH_SLOTS = ("BE", "IR")

#: Fewest player-weeks a position needs for its own coefficients.
#:
#: Below this it falls back to the pooled fit. IDP positions are why: safeties turn up
#: 24 times in 2025 and cornerbacks five, and a two-parameter mean-variance function
#: fitted on 24 rows is noise with a decimal point on it.
MIN_ROWS = 300

#: z for an 80% interval, matching the p10/p90 plan 28 publishes.
Z_P90 = 1.2816

#: Pre-committed gates. Recorded here before the fit was run, and reported by
#: ``--report`` so the answer is checkable rather than asserted.
#:
#: The point of writing them down is that a win probability is the one number on this
#: app a reader cannot sanity-check by eye. A confident-looking 63% that is really a
#: coin flip is worse than showing no number at all, so the percentage ships only if
#: these hold -- otherwise the Matchup tab shows the projected margin and says the
#: probability did not calibrate.
GATES: Dict[str, str] = {
    "G-W1": "Team-level 80% interval coverage within [0.77, 0.83] of nominal 0.800.",
    "G-W2": "Worst predicted-probability decile within 5pp of the realised win rate.",
    "G-W3": "Brier score beats an uninformative 0.500 on every matchup.",
    "G-W4": "Predicted team sd within 10% of realised, which is the independence "
            "assumption's own test.",
}


def _log(message: str) -> None:
    """Print unbuffered, so a long fit is watchable.

    Args:
        message: The line.
    """
    print(message, flush=True)


def residuals(seasons: Sequence[int] = TRAIN_SEASONS) -> pl.DataFrame:
    """Every started, active player-week with both an actual and a projection.

    Restricted to **started** players on purpose. The bench is not a sample of the
    same thing: it is where managers park players they expect to do nothing, so its
    residuals describe a selection rather than a projection. Restricted to **active**
    players for the sharper version of the same reason -- a player on bye scores
    exactly zero and would teach the model that a 14-point projection has a 14-point
    error.

    Args:
        seasons: Seasons to read.

    Returns:
        pl.DataFrame: ``lg``, ``season``, ``week``, ``team_owner``, ``player_id``,
        ``player_position``, ``mu``, ``actual``, ``resid``.
    """
    wanted = ["week", "team_owner", "player_id", "slotPosition", "player_position",
              "points", "TRUE_Points", "player_active_status"]
    frames: List[pl.DataFrame] = []
    for season in seasons:
        root = paths.STORE_DIR / str(season)
        if not root.is_dir():
            continue
        for path in sorted(root.glob("*/lineups.parquet")):
            frame = pl.read_parquet(path, columns=wanted)
            frames.append(frame.with_columns(
                pl.lit(path.parent.name).alias("lg"),
                pl.lit(season).alias("season")))
    if not frames:
        raise FileNotFoundError(
            f"No lineups.parquet under {paths.STORE_DIR} for seasons {list(seasons)}. "
            f"Run `python -m Scripts.sync --pull` or `Scripts.refresh`."
        )

    return (pl.concat(frames)
            .filter(~pl.col("slotPosition").is_in(list(BENCH_SLOTS)))
            .filter(pl.col("team_owner") != FREE_AGENT_OWNER)
            .filter(pl.col("player_active_status") == "active")
            .filter(pl.col("TRUE_Points").is_not_null())
            .filter(pl.col("points").is_not_null())
            .rename({"TRUE_Points": "mu", "points": "actual"})
            .with_columns((pl.col("actual") - pl.col("mu")).alias("resid"))
            .drop(["slotPosition", "player_active_status"]))


def _fit_one(mu: np.ndarray, resid: np.ndarray) -> Tuple[float, float]:
    """Fit ``Var = φ·μ + μ²/k`` by least squares on the squared residuals.

    ``E[r²] = Var``, so regressing ``r²`` on ``(μ, μ²)`` with no intercept estimates
    the two coefficients directly, using every row rather than binned variances.
    Both are clamped at zero: a negative variance coefficient is not a model, and for
    several positions one term genuinely wants to be absent.

    Args:
        mu: Projections.
        resid: ``actual - mu``.

    Returns:
        tuple: ``(phi, inv_k)`` where ``inv_k`` is ``1/k``, stored inverted so a
        position that wants no proportional term stores a plain 0.0 rather than an
        infinity.
    """
    design = np.column_stack([mu, mu ** 2])
    coefficients, *_ = np.linalg.lstsq(design, resid ** 2, rcond=None)
    return float(max(coefficients[0], 0.0)), float(max(coefficients[1], 0.0))


def fit(seasons: Sequence[int] = TRAIN_SEASONS,
        frame: Optional[pl.DataFrame] = None) -> dict:
    """Fit the per-position dispersion.

    Args:
        seasons: Seasons to train on.
        frame: Pre-loaded :func:`residuals` output, for tests.

    Returns:
        dict: The model, ready for :func:`write`.
    """
    frame = residuals(seasons) if frame is None else frame

    pooled_phi, pooled_inv_k = _fit_one(frame["mu"].to_numpy(),
                                        frame["resid"].to_numpy())
    model = {
        "version": MODEL_VERSION,
        "train_seasons": list(seasons),
        "n_rows": frame.height,
        "pooled": {"phi": pooled_phi, "inv_k": pooled_inv_k},
        "positions": {},
    }

    for (position,), group in frame.group_by(["player_position"], maintain_order=True):
        if position is None:
            continue
        mu = group["mu"].to_numpy()
        resid = group["resid"].to_numpy()
        entry = {
            "n": int(group.height),
            "mean_mu": float(mu.mean()),
            "bias": float(resid.mean()),
            "sd": float(resid.std()),
        }
        if group.height >= MIN_ROWS:
            phi, inv_k = _fit_one(mu, resid)
            entry.update({"phi": phi, "inv_k": inv_k, "source": "fitted"})
        else:
            # Named rather than silently pooled, so the report can say which
            # positions are riding on the pooled coefficients.
            entry.update({"phi": pooled_phi, "inv_k": pooled_inv_k,
                          "source": "pooled"})
        model["positions"][position] = entry

    return model


def write(model: dict) -> None:
    """Persist a fitted model.

    Args:
        model: From :func:`fit`.
    """
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    MODEL_PATH.write_text(json.dumps(model, indent=2, sort_keys=True))


def load() -> dict:
    """Read the fitted model.

    Returns:
        dict: The model.

    Raises:
        FileNotFoundError: When it has not been fitted.
        ValueError: When it was fitted by a different version of this module.
    """
    if not MODEL_PATH.is_file():
        raise FileNotFoundError(
            f"No weekly dispersion model at {MODEL_PATH}. Run "
            f"`python -m Scripts.outcomes.weekly --fit`."
        )
    model = json.loads(MODEL_PATH.read_text())
    if model.get("version") != MODEL_VERSION:
        raise ValueError(
            f"{MODEL_PATH} was fitted by version {model.get('version')}, this is "
            f"{MODEL_VERSION}. Refit rather than reinterpreting its coefficients."
        )
    return model


def player_sd(model: dict, position: Optional[str], mu: Optional[float]) -> float:
    """Standard deviation of one player's weekly points.

    Args:
        model: From :func:`load`.
        position: The player's position. Unknown positions take the pooled fit.
        mu: His projection. Negative or None is treated as zero.

    Returns:
        float: Standard deviation, never negative.
    """
    if mu is None or mu <= 0:
        return 0.0
    entry = (model.get("positions") or {}).get(position) or model["pooled"]
    variance = entry["phi"] * mu + entry["inv_k"] * mu * mu
    return math.sqrt(max(variance, 0.0))


def team_sd(model: dict, players: Sequence[Tuple[Optional[str], Optional[float]]]
            ) -> float:
    """Standard deviation of a starting lineup's weekly total.

    The independent sum, which is measured to be right -- see the module docstring
    and gate G-W4.

    Args:
        model: From :func:`load`.
        players: ``(position, mu)`` per starter.

    Returns:
        float: ``sqrt(Σ sd_i²)``.
    """
    return math.sqrt(sum(player_sd(model, position, mu) ** 2
                         for position, mu in players))


def normal_cdf(x: float) -> float:
    """Standard normal CDF, without pulling in scipy.

    Args:
        x: Value.

    Returns:
        float: ``P(Z <= x)``.
    """
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def win_probability(mu_a: float, sd_a: float, mu_b: float, sd_b: float) -> float:
    """Probability team A outscores team B.

    Closed form rather than simulated. The margin of two independent normals is
    normal, and the normal approximation is calibrated to within 0.2pp at team level
    -- so a Monte Carlo would contribute nothing but its own sampling noise.

    Args:
        mu_a: A's projected total.
        sd_a: A's standard deviation.
        mu_b: B's projected total.
        sd_b: B's standard deviation.

    Returns:
        float: 0.0 to 1.0. Exactly 0.5 when neither side has any spread and the
        projections tie, which is the only honest answer to that.
    """
    spread = math.sqrt(sd_a ** 2 + sd_b ** 2)
    if spread <= 0:
        return 0.5 if mu_a == mu_b else (1.0 if mu_a > mu_b else 0.0)
    return normal_cdf((mu_a - mu_b) / spread)


def team_totals(model: dict, frame: Optional[pl.DataFrame] = None,
                seasons: Sequence[int] = TRAIN_SEASONS) -> pl.DataFrame:
    """Projected total, sd and actual per team-week, for the back-test.

    Args:
        model: From :func:`load`.
        frame: Pre-loaded :func:`residuals` output, for tests.
        seasons: Seasons to read when ``frame`` is None.

    Returns:
        pl.DataFrame: ``lg``, ``season``, ``week``, ``team_owner``, ``mu``, ``sd``,
        ``actual``, ``starters``.
    """
    frame = residuals(seasons) if frame is None else frame
    frame = frame.with_columns(
        pl.struct(["player_position", "mu"]).map_elements(
            lambda row: player_sd(model, row["player_position"], row["mu"]),
            return_dtype=pl.Float64).alias("sd"))
    return frame.group_by(["lg", "season", "week", "team_owner"]).agg(
        pl.col("mu").sum(),
        (pl.col("sd") ** 2).sum().sqrt().alias("sd"),
        pl.col("actual").sum(),
        pl.len().alias("starters"))


def backtest(model: dict, frame: Optional[pl.DataFrame] = None,
             seasons: Sequence[int] = TRAIN_SEASONS) -> dict:
    """Score the model against :data:`GATES`.

    **Evaluated on every within-league-week pair, in both directions, not on the real
    schedule.** The real pairing would be the obvious choice and is the wrong one twice
    over: the ``team_stats`` artifact that records who played whom has never been
    built, and the question "is this probability calibrated" does not depend on the
    fixture list. All pairs is a larger, unbiased evaluation set -- ~20,000 half-
    matchups against the ~900 the schedule would have offered.

    Both directions because the calibration curve is otherwise a function of row
    order; see the comment on the pairing loop.

    Args:
        model: From :func:`load`.
        frame: Pre-loaded :func:`residuals` output, for tests.
        seasons: Seasons to evaluate.

    Returns:
        dict: Metrics, plus a ``gates`` block of pass/fail.
    """
    totals = team_totals(model, frame=frame, seasons=seasons)

    resid = (totals["actual"] - totals["mu"]).to_numpy()
    predicted_sd = float(totals["sd"].mean())
    realised_sd = float(resid.std())
    inside = np.abs(resid) <= Z_P90 * totals["sd"].to_numpy()
    coverage = float(inside.mean())

    # **Both directions of every pair**, and that is a correctness fix rather than a
    # doubling of the sample. Emitting each pair once makes the *composition of the
    # deciles* depend on which team happened to be listed first -- a matchup emitted
    # as (A, B) contributes p, and as (B, A) contributes 1 - p, which lands in a
    # different bucket. Brier and accuracy are symmetric under that flip and never
    # noticed; the calibration curve is not, and G-W2 swung between 0.046 and 0.078 on
    # identical inputs purely on Polars' group iteration order. Emitting both makes the
    # curve symmetric by construction and the gate deterministic.
    probabilities, outcomes = [], []
    for _, group in totals.group_by(["lg", "season", "week"], maintain_order=True):
        rows = group.to_dicts()
        for i in range(len(rows)):
            for j in range(i + 1, len(rows)):
                a, b = rows[i], rows[j]
                if a["actual"] == b["actual"]:
                    continue                    # a real tie decides nothing
                for home, away in ((a, b), (b, a)):
                    probabilities.append(
                        win_probability(home["mu"], home["sd"],
                                        away["mu"], away["sd"]))
                    outcomes.append(
                        1.0 if home["actual"] > away["actual"] else 0.0)

    probabilities = np.asarray(probabilities)
    outcomes = np.asarray(outcomes)
    brier = float(np.mean((probabilities - outcomes) ** 2))

    deciles, worst = [], 0.0
    edges = np.quantile(probabilities, np.linspace(0, 1, 11))
    for i in range(10):
        mask = (probabilities >= edges[i]) & (probabilities <= edges[i + 1])
        if mask.sum() < 30:
            continue
        predicted = float(probabilities[mask].mean())
        realised = float(outcomes[mask].mean())
        worst = max(worst, abs(predicted - realised))
        deciles.append({"low": float(edges[i]), "high": float(edges[i + 1]),
                        "n": int(mask.sum()), "predicted": predicted,
                        "realised": realised})

    sd_error = abs(predicted_sd - realised_sd) / realised_sd
    report = {
        "team_weeks": totals.height,
        "matchups": int(len(probabilities)),
        "coverage": coverage,
        "predicted_sd": predicted_sd,
        "realised_sd": realised_sd,
        "sd_error": sd_error,
        "brier": brier,
        "brier_uninformative": float(np.mean((0.5 - outcomes) ** 2)),
        "accuracy": float(np.mean((probabilities > 0.5) == (outcomes > 0.5))),
        "worst_decile": worst,
        "deciles": deciles,
    }
    report["gates"] = {
        "G-W1": 0.77 <= coverage <= 0.83,
        "G-W2": worst <= 0.05,
        "G-W3": brier < report["brier_uninformative"],
        "G-W4": sd_error <= 0.10,
    }
    report["calibrated"] = all(report["gates"].values())
    return report


def print_report(report: dict) -> None:
    """Print a back-test the way the gates are written.

    Args:
        report: From :func:`backtest`.
    """
    _log(f"\n{report['team_weeks']} team-weeks, {report['matchups']} matchups "
         f"(every within-league-week pair)\n")
    _log(f"  team 80% coverage   {report['coverage']:.3f}   (nominal 0.800)")
    _log(f"  predicted team sd   {report['predicted_sd']:.2f}")
    _log(f"  realised  team sd   {report['realised_sd']:.2f}   "
         f"({report['sd_error'] * 100:.1f}% error)")
    _log(f"  Brier               {report['brier']:.4f}   "
         f"(uninformative {report['brier_uninformative']:.4f})")
    _log(f"  accuracy            {report['accuracy']:.4f}")
    _log(f"  worst decile        {report['worst_decile']:.3f}\n")

    _log(f"  {'predicted':>16s} {'n':>7s} {'pred':>7s} {'actual':>7s} {'diff':>7s}")
    for row in report["deciles"]:
        _log(f"  {row['low']:.3f}-{row['high']:.3f}  {row['n']:7d} "
             f"{row['predicted']:7.3f} {row['realised']:7.3f} "
             f"{row['predicted'] - row['realised']:+7.3f}")

    _log("")
    for gate, description in GATES.items():
        mark = "PASS" if report["gates"][gate] else "FAIL"
        _log(f"  {gate}  {mark}  {description}")
    _log(f"\n  {'CALIBRATED' if report['calibrated'] else 'NOT CALIBRATED'} — "
         f"the Matchup tab "
         f"{'shows the win probability' if report['calibrated'] else 'must show the projected margin only'}.")


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point.

    Args:
        argv: Argument list, for tests.

    Returns:
        int: 0 on success, 1 when the gates fail.
    """
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.outcomes.weekly",
        description=__doc__.split("\n\n")[0],
    )
    parser.add_argument("--fit", action="store_true",
                        help="refit from the store and write the model")
    parser.add_argument("--report", action="store_true",
                        help="back-test against the pre-committed gates")
    parser.add_argument("--season", type=int, action="append", default=None,
                        help="season to train on; repeatable "
                             f"(default: {list(TRAIN_SEASONS)})")
    args = parser.parse_args(argv)

    seasons = tuple(args.season) if args.season else TRAIN_SEASONS

    if not args.fit and not args.report:
        parser.error("nothing to do: pass --fit, --report, or both")

    frame = residuals(seasons)
    _log(f"{frame.height} started-and-active player-weeks from "
         f"{frame['lg'].n_unique()} leagues, seasons {list(seasons)}")

    if args.fit or not MODEL_PATH.is_file():
        model = fit(seasons, frame=frame)
        write(model)
        _log(f"\nwrote {MODEL_PATH}")
        _log(f"  {'pos':6s} {'n':>6s} {'phi':>8s} {'1/k':>8s} {'bias':>7s} "
             f"{'sd':>7s}  source")
        for position, entry in sorted(model["positions"].items(),
                                      key=lambda kv: -kv[1]["n"]):
            _log(f"  {position:6s} {entry['n']:6d} {entry['phi']:8.3f} "
                 f"{entry['inv_k']:8.4f} {entry['bias']:+7.2f} {entry['sd']:7.2f}"
                 f"  {entry['source']}")
    else:
        model = load()

    if args.report:
        report = backtest(model, frame=frame)
        print_report(report)
        return 0 if report["calibrated"] else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
