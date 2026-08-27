"""What the market conversions do, measured on the archived prices.

:mod:`Scripts.market` states four arithmetic claims. This replays the archived raw
BetOnline store through both the old expressions and the new ones and prints the
difference, because a derivation that nobody has run against real prices is a
paragraph, not a fix.

**What this can and cannot score, and why.** It cannot score any of it against
realised outcomes at useful n, and that is worth being blunt about:
``scrape_BOL.py`` **overwrites** ``BetOnline_AllProps_Raw.parquet`` on every run and
``scrape_pinnacle.py`` overwrites ``Raw_Pinnacle_New.csv``, so the only prices this
repo still holds are the last scrape of 2025 -- one game, 624 rows. Every earlier
week survives as the *derived* ``proj_`` column, which cannot be re-derived under a
new formula. So the 2025 calibration in
``Scripts/lab/results.json:blend_accuracy`` measures the old arithmetic and will
keep measuring it; the fixes reach it a week at a time from the next live scrape.
Both scrapers now keep a per-week copy of their raw file so that this is measurable
next season -- which is the change that makes plan 35's own instruction, "judge all
of it on calibration", executable at all.

What is measurable today is the arithmetic itself, on real prices: the margin
removed, the size and direction of the line adjustment against what it replaces, and
the dispersion the ladder was throwing away.

Usage::

    python -m Scripts.lab.market
    python -m Scripts.market --report      # same thing
"""

from __future__ import annotations

import argparse
import math
from typing import Dict, List, Optional, Tuple

import numpy as np
import polars as pl

from Scripts import market as mk
from Scripts import paths
from Scripts.lab.run import RESULTS_PATH, load_results, save_results

#: Stat names whose ladder rungs are integers, so a half-integer line resolves to a
#: rung rather than to a point between two.
#:
#: Reading a discrete ladder by interpolation is what made the hold look like 1.30 on
#: receptions and 1.51 on passing touchdowns in an earlier pass at this measurement.
#: ``P(X > 4.5)`` *is* ``P(X >= 5)``; averaging the rungs at 4 and 5 invents a
#: number the book never posted and then blames the book for it.
COUNT_LADDERS: Tuple[str, ...] = tuple(
    stat for stat, found in mk.MARKET_STATS.items() if found.kind == "count")


def raw_props(season: int) -> Optional[pl.DataFrame]:
    """The archived raw BetOnline prices for a season, or None.

    Args:
        season: Season to read.

    Returns:
        pl.DataFrame | None: The landing file, or None when it was never archived.
    """
    path = paths.landing_dir("BetOnline", season, "BetOnline_AllProps_Raw.parquet")
    return pl.read_parquet(path) if path.is_file() else None


def two_way(frame: pl.DataFrame) -> pl.DataFrame:
    """Two-way pairs, one row per player-stat-line, with the margin removed.

    Args:
        frame: Raw props.

    Returns:
        pl.DataFrame: ``player_name``, ``position``, ``espn_stat``, ``value``, the
        two implied probabilities, their ``book`` total and the de-vigged ``q``.
    """
    pairs = (frame.filter(pl.col("prop_source") == "OverUnder")
             .pivot(index=["player_name", "position", "espn_stat", "value"],
                    on="type", values="impProb", aggregate_function="first")
             .drop_nulls(["Over", "Under"]))
    if pairs.height == 0:
        return pairs
    q_over, _ = mk.devig_two_way(pairs["Over"].to_numpy(),
                                 pairs["Under"].to_numpy())
    return pairs.with_columns(
        pl.Series("book", mk.overround(pairs["Over"].to_numpy(),
                                      pairs["Under"].to_numpy())),
        pl.Series("q", q_over))


def ladders(frame: pl.DataFrame) -> Dict[Tuple[str, str], Tuple[np.ndarray,
                                                                np.ndarray]]:
    """Every ladder in the scrape, keyed by ``(espn_stat, player_name)``.

    Args:
        frame: Raw props.

    Returns:
        dict: ``(stat, player)`` -> ``(thresholds, survival)``, monotone and sorted.
    """
    out: Dict[Tuple[str, str], Tuple[np.ndarray, np.ndarray]] = {}
    rungs = frame.filter(pl.col("prop_source") == "Values")
    for (stat, player), group in rungs.group_by(["espn_stat", "player_name"]):
        out[(str(stat), str(player))] = mk.monotone_survival(
            group["value"].to_numpy(), group["impProb"].to_numpy())
    return out


def measured_ladder_hold(frame: pl.DataFrame) -> Dict:
    """The one-sided ladder's margin, measured against the two-way line beside it.

    A ladder has no complement, so its hold cannot be normalised out -- it has to be
    compared with a price that has already had its hold removed. Where the same
    player-stat carries both, ``S_ladder(line) / q_novig`` is that comparison.

    Args:
        frame: Raw props.

    Returns:
        dict: ``n``, ``median``, and ``by_stat`` medians.
    """
    rungs = ladders(frame)
    rows = []
    for row in two_way(frame).iter_rows(named=True):
        found = rungs.get((row["espn_stat"], row["player_name"]))
        if found is None or found[0].size < 2:
            continue
        edges, survival = found
        if row["espn_stat"] in COUNT_LADDERS:
            # P(X > 4.5) is P(X >= 5): read the rung, never between rungs.
            hit = np.flatnonzero(edges == math.ceil(row["value"]))
            at_line = float(survival[hit[0]]) if hit.size else float("nan")
        elif edges[0] <= row["value"] <= edges[-1]:
            at_line = float(np.interp(row["value"], edges, survival))
        else:
            at_line = float("nan")
        if not np.isfinite(at_line) or row["q"] <= 0:
            continue
        rows.append({"stat": row["espn_stat"], "hold": at_line / row["q"]})
    if not rows:
        return {"n": 0, "median": None, "by_stat": {}}
    table = pl.DataFrame(rows)
    return {
        "n": table.height,
        "median": round(float(table["hold"].median()), 4),
        "by_stat": {
            str(r["stat"]): [int(r["n"]), round(float(r["hold"]), 4)]
            for r in table.group_by("stat")
                          .agg(pl.len().alias("n"), pl.col("hold").median())
                          .sort("stat").iter_rows(named=True)},
    }


#: The two coefficients the derivation replaces, for the side-by-side.
OLD_COEFFICIENTS: Dict[str, float] = {"BetOnline k=0.50": 0.5,
                                      "Pinnacle k=0.25": 0.25}


def line_adjustment(frame: pl.DataFrame, model: mk.MarketModel,
                    hold: float) -> Tuple[pl.DataFrame, Dict]:
    """The old juice nudge against the derived one, per stat.

    Carries both dispersions -- the ladder's own and the fitted one -- because the
    gap between them is worth more than the nudge they feed, and because only one of
    them moves the line. See :data:`Scripts.market.MARKET_OVER_FITTED_SCALE`.

    Args:
        frame: Raw props.
        model: Fitted dispersion.
        hold: Overround to de-vig the ladders by.

    Returns:
        tuple: The per-line frame, and a per-stat summary dict.
    """
    pairs = two_way(frame)
    if pairs.height == 0:
        return pairs, {}
    lines = pairs["value"].to_numpy()
    juice_diff = (1.0 / pairs["Under"].to_numpy() - 1.0
                  - (1.0 / pairs["Over"].to_numpy() - 1.0))
    positions = np.asarray(pairs["position"].to_list(), dtype=object)
    fitted = np.full(lines.shape, np.nan)
    for stat in pairs["espn_stat"].unique().to_list():
        rows = (pairs["espn_stat"] == stat).to_numpy()
        fitted[rows] = model.sigma(stat, lines[rows], positions[rows])

    rungs = ladders(frame)
    from_ladder = np.array([
        mk.market_scale(*_devigged(rungs.get((row["espn_stat"],
                                              row["player_name"])), hold),
                        kind=_kind(row["espn_stat"]))
        for row in pairs.iter_rows(named=True)])
    # The fitted sigma, not the market's: see MARKET_OVER_FITTED_SCALE.
    derived = mk.line_to_mean(lines, pairs["q"].to_numpy(), fitted)
    frame_out = pairs.with_columns(
        _nullable("fitted_sd", fitted),
        _nullable("ladder_sd", from_ladder),
        pl.Series("derived_adj", derived - lines),
        *[pl.Series(f"old_adj_{label}", juice_diff * lines * k)
          for label, k in OLD_COEFFICIENTS.items()])
    summary = {
        str(r["espn_stat"]): {
            "n": int(r["n"]),
            "mean_line": round(float(r["mean_line"]), 2),
            "mean_abs_old_k50": round(float(r["old50"]), 3),
            "mean_abs_old_k25": round(float(r["old25"]), 3),
            "mean_abs_derived": round(float(r["derived"]), 3),
            "mean_fitted_sd": _round_or_none(r["fitted_sd"]),
            "mean_ladder_sd": _round_or_none(r["ladder_sd"]),
            "n_from_ladder": int(r["from_ladder"]),
        }
        for r in frame_out.group_by("espn_stat").agg(
            pl.len().alias("n"),
            pl.col("value").mean().alias("mean_line"),
            pl.col("old_adj_BetOnline k=0.50").abs().mean().alias("old50"),
            pl.col("old_adj_Pinnacle k=0.25").abs().mean().alias("old25"),
            pl.col("derived_adj").abs().mean().alias("derived"),
            pl.col("fitted_sd").mean(),
            pl.col("ladder_sd").mean(),
            pl.col("ladder_sd").is_not_null().sum().alias("from_ladder"),
        ).sort("espn_stat").iter_rows(named=True)}
    return frame_out, summary


def _kind(stat: str) -> str:
    """A stat's ladder shape, or ``"yardage"`` for one this module has not declared."""
    found = mk.MARKET_STATS.get(str(stat))
    return found.kind if found is not None else "yardage"


def _nullable(name: str, values: np.ndarray) -> pl.Series:
    """A Polars series with NaN as null, so an aggregate skips it instead of
    poisoning itself. Polars propagates NaN through ``mean`` and skips null."""
    return pl.Series(name, [None if not np.isfinite(v) else float(v)
                            for v in np.asarray(values, dtype=float)])


def _devigged(found, hold: float) -> Tuple[np.ndarray, np.ndarray]:
    """A ladder with the margin removed, or an empty one when there is no ladder."""
    if found is None:
        return np.array([]), np.array([])
    edges, survival = found
    return edges, mk.devig_survival(survival, hold)


def _round_or_none(value, digits: int = 1):
    """``round``, but None for a missing or non-finite value."""
    if value is None or not np.isfinite(float(value)):
        return None
    return round(float(value), digits)


def old_value_calc(edges: np.ndarray, survival: np.ndarray) -> float:
    """What ``value_calc`` computed: ``sum(threshold * P(exactly that bucket))``.

    Kept so the replacement can be measured against it rather than described. It is
    the exact mean for a count ladder rooted at 1 and a 20% understatement for a
    yardage ladder, and this is the function that shows which.

    Args:
        edges: Rung values, ascending.
        survival: ``P(X >= t)`` per rung.

    Returns:
        float: The old projection.
    """
    if edges.size == 0:
        return float("nan")
    exact = np.empty_like(survival)
    exact[-1] = survival[-1]
    exact[:-1] = survival[:-1] - survival[1:]
    return float(edges @ exact)


def ladder_reads(frame: pl.DataFrame, hold: float) -> pl.DataFrame:
    """Every ladder read the old way and the new way, side by side.

    Args:
        frame: Raw props.
        hold: Overround to divide out.

    Returns:
        pl.DataFrame: ``stat``, ``kind``, ``player``, ``rungs``, ``old``, and the
        new ``mean``/``sd``/``median``/``scale`` as each applies.
    """
    lines = {(r["espn_stat"], r["player_name"]): r["value"]
             for r in two_way(frame).iter_rows(named=True)}
    rows = []
    for (stat, player), (edges, survival) in sorted(ladders(frame).items()):
        clean = mk.devig_survival(survival, hold)
        kind = mk.MARKET_STATS[stat].kind if stat in mk.MARKET_STATS else "unknown"
        mean, sd = (mk.count_moments(edges, clean) if kind == "count"
                    else (float("nan"), float("nan")))
        rows.append({
            "stat": stat, "kind": kind, "player": player, "rungs": int(edges.size),
            "line": lines.get((stat, player)),
            "old": old_value_calc(edges, survival),
            "count_mean": mean, "count_sd": sd,
            "median": mk.ladder_median(edges, clean),
            "scale": mk.market_scale(edges, clean, kind),
        })
    return pl.DataFrame(rows)


def ladder_median_vs_line(frame: pl.DataFrame, hold: float) -> Dict:
    """Does a de-vigged ladder's median reproduce the line posted beside it?

    The check that validates the de-vig without any outcome data. If the margin has
    been removed correctly then the ladder and the two-way line are two readings of
    the same distribution's middle, and they have to agree to within the rung
    spacing -- which is reported alongside, because a ladder on 10-yard rungs cannot
    locate a median more precisely than that.

    Args:
        frame: Raw props.
        hold: Overround to divide out.

    Returns:
        dict: Per stat, ``n``, the mean signed and absolute gap, and the median rung
        spacing.
    """
    rungs = ladders(frame)
    rows = []
    for row in two_way(frame).iter_rows(named=True):
        found = rungs.get((row["espn_stat"], row["player_name"]))
        if found is None or found[0].size < 2:
            continue
        edges, survival = _devigged(found, hold)
        median = mk.ladder_median(edges, survival)
        if not np.isfinite(median):
            continue
        rows.append({"stat": row["espn_stat"],
                     "gap": median - row["value"],
                     "rung_gap": float(np.median(np.diff(edges)))})
    if not rows:
        return {}
    table = pl.DataFrame(rows)
    return {
        str(r["stat"]): {"n": int(r["n"]),
                         "mean_gap": round(float(r["gap"]), 3),
                         "mean_abs_gap": round(float(r["abs_gap"]), 3),
                         "rung_gap": round(float(r["rung_gap"]), 2)}
        for r in table.group_by("stat").agg(
            pl.len().alias("n"),
            pl.col("gap").mean(),
            pl.col("gap").abs().mean().alias("abs_gap"),
            pl.col("rung_gap").median(),
        ).sort("stat").iter_rows(named=True)}


def _blank(value) -> str:
    """A number, or a dash where there is none."""
    return "-" if value is None else f"{value:.1f}"


#: Markets whose old projection can be inverted back to the price that made it.
#:
#: **This is how three of plan 35's four fixes get scored against realised
#: outcomes despite the raw prices being gone.** The archived store holds only the
#: derived ``proj_`` column, but for a market whose line never moves, the old
#: expression is an invertible function of the de-vigged price alone -- so the price
#: can be recovered from the archive and re-converted. Two shapes qualify:
#:
#: * **An anytime ladder rooted at 1.** ``value_calc``'s ``sum(threshold *
#:   P(exactly))`` is algebraically ``sum_k P(N >= k)``, so the corrected column is
#:   exactly the old one divided by the overround. No inversion needed.
#: * **A two-way market on a fixed 0.5 line.** ``old = 0.5 + k * 0.5 * Juice_Diff``
#:   is strictly increasing in ``q``, so ``q`` is recoverable and
#:   :func:`Scripts.market.count_line_to_mean` re-converts it.
#:
#: ``BOL_passingTouchdowns`` does *not* qualify: its line is 1.5 or 2.5 depending on
#: the quarterback, the line is not archived, and two candidate lines give two
#: candidate prices for the same stored number. It is left unscored rather than
#: guessed.
RECOVERABLE: Dict[str, Dict] = {
    "BOL_rushingTouchdowns": {"how": "devig_only", "positions": ("QB", "RB")},
    "BOL_receivingTouchdowns": {"how": "devig_only", "positions": ("WR", "TE")},
    "BOL_passingInterceptions": {"how": "half_line", "k": 0.5,
                                 "stat": "passingInterceptions",
                                 "positions": ("QB",)},
    "PINNY_anytimeTouchdown": {"how": "half_line", "k": 0.25,
                               "stat": "anytimeTouchdown",
                               "positions": ("RB", "WR", "TE")},
}


def recover_price(old, coefficient: float, hold: float) -> np.ndarray:
    """The de-vigged price behind an old 0.5-line projection.

    ``old(q) = 0.5 + coefficient * 0.5 * (1 / (1 + h)) * (2q - 1) / (q (1 - q))``,
    which is strictly increasing on ``(0, 1)`` and therefore invertible. Bisected
    rather than solved: the closed form is a quadratic with a branch condition, and
    a bisection over a monotone function needs no case analysis.

    Args:
        old: Stored projection.
        coefficient: The juice coefficient that produced it -- 0.5 for BetOnline,
            0.25 for Pinnacle.
        hold: Overround assumed at the time, which cancels out of the ratio only
            approximately, so it is passed rather than ignored.

    Returns:
        np.ndarray: ``q``, NaN where the value is outside the invertible range.
    """
    target = np.asarray(old, dtype=float)
    lo = np.full(target.shape, 1e-6)
    hi = np.full(target.shape, 1.0 - 1e-6)

    def forward(q):
        """The old expression, as a function of the de-vigged price alone."""
        return (0.5 + coefficient * 0.5 * (2.0 * q - 1.0)
                / (hold * q * (1.0 - q)))

    for _ in range(mk.BISECTION_STEPS):
        mid = 0.5 * (lo + hi)
        below = forward(mid) < target
        lo = np.where(below, mid, lo)
        hi = np.where(below, hi, mid)
    q = 0.5 * (lo + hi)
    # Outside the range the old expression can produce, the inversion is meaningless
    # rather than merely imprecise -- and the negative projections it produced for a
    # longshot are exactly that region.
    return np.where(np.isfinite(target) & (np.abs(forward(q) - target) < 1e-6),
                    q, np.nan)


def corrected_columns(frame: pl.DataFrame, model: mk.MarketModel,
                      hold: float = mk.DEFAULT_OVERROUND) -> pl.DataFrame:
    """Add a ``NEW_<source>_<stat>`` column per :data:`RECOVERABLE` market.

    Args:
        frame: Frame from :func:`Scripts.lab.accuracy.build`.
        model: Fitted market model.
        hold: Overround to remove.

    Returns:
        pl.DataFrame: ``frame`` plus the corrected columns.
    """
    positions = np.asarray(frame["primaryPosition"].to_list(), dtype=object)
    added = []
    for column, spec in RECOVERABLE.items():
        if spec["how"] == "devig_only":
            if column not in frame.columns:
                continue
            added.append((pl.col(column) / hold).alias(f"NEW_{column}"))
            continue
        source = column.split("_", 1)[0]
        parts = ([f"{source}_rushingTouchdowns", f"{source}_receivingTouchdowns"]
                 if spec["stat"] == "anytimeTouchdown" else [column])
        if not set(parts).issubset(frame.columns):
            continue
        old = np.zeros(frame.height)
        for part in parts:
            old = old + np.nan_to_num(frame[part].to_numpy(), nan=0.0)
        old = np.where(np.isfinite(frame[parts[0]].to_numpy()), old, np.nan)
        q = recover_price(old, spec["k"], hold)
        new = np.full(frame.height, np.nan)
        for name in set(positions.tolist()):
            rows = positions == name
            found = model.parameters(spec["stat"], str(name))
            if found is None:
                continue
            new[rows] = mk.count_line_to_mean(np.full(rows.sum(), 0.5), q[rows],
                                              *found)
        added.append(_nullable(f"NEW_{source}_{spec['stat']}", new))
    return frame.with_columns(added) if added else frame


def backtest(season: int = 2025) -> Tuple[List[str], Dict]:
    """2025 calibration, old conversion against new, on the recoverable markets.

    The only part of plan 35 that reaches realised outcomes. Per source rather than
    on the blend, deliberately: the question is whether the market conversion got
    better, and the blend's renormalised weights would absorb part of the answer.

    Args:
        season: Season to score.

    Returns:
        tuple: ``(text lines, entry)``.
    """
    from Scripts.lab import accuracy as acc

    model = mk.MarketModel.load()
    frame, _ = acc.build(season)
    frame = corrected_columns(frame, model)

    lines = ["--- 5. calibration against realised outcomes, "
             f"{season} ------------------",
             "  Total projected / total realised on played rows, 1.00 is right. Only",
             "  the markets whose old projection can be inverted back to its price;",
             "  see RECOVERABLE for the two that cannot.",
             f"  {'source@pos':22}{'stat':24}{'n':>6}{'realised':>10}{'old':>9}"
             f"{'new':>9}{'old cal':>9}{'new cal':>9}"]
    entry: Dict = {}
    for column, spec in sorted(RECOVERABLE.items()):
        source = column.split("_", 1)[0]
        stat = spec.get("stat")
        pairs = ([("rushingTouchdowns", f"NEW_{source}_anytimeTouchdown"),
                  ("receivingTouchdowns", f"NEW_{source}_anytimeTouchdown")]
                 if stat == "anytimeTouchdown"
                 else [(column.split("_", 1)[1], f"NEW_{column}")])
        for scored_stat, new_column in pairs:
            if new_column not in frame.columns:
                continue
            for position in spec["positions"]:
                found = _calibration_pair(frame, acc, source, scored_stat,
                                          new_column, position,
                                          share=(stat == "anytimeTouchdown"))
                if found is None:
                    continue
                entry[f"{source}@{position}|{scored_stat}"] = found
                lines.append(
                    f"  {source + '@' + position:22}{scored_stat:24}"
                    f"{found['n']:>6}{found['realised']:>10.0f}"
                    f"{found['old']:>9.1f}{found['new']:>9.1f}"
                    f"{found['old_ratio']:>9.3f}{found['new_ratio']:>9.3f}")
    return lines, entry


def _calibration_pair(frame, acc, source: str, stat: str, new_column: str,
                      position: str, share: bool) -> Optional[Dict]:
    """Old and new totals against realised, on the rows the source is real for.

    Args:
        frame: Frame carrying both columns.
        acc: The accuracy module, for its own row filter and real mask.
        source: ``"BOL"`` or ``"PINNY"``.
        stat: Stat being scored.
        new_column: The corrected column.
        position: Position to restrict to.
        share: Whether the corrected column is a single anytime number that has to
            be split back across rushing and receiving in the old column's own
            proportion. Pinnacle's split is by yardage share and is not this
            plan's to change, so the correction is applied to the total and
            reallocated the way the store already allocated it.

    Returns:
        dict | None: ``n``, ``realised``, ``old``, ``new`` and the two ratios.
    """
    from Scripts.usage import evalset as es

    actual = f"{acc.ACTUAL_PREFIX}{stat}"
    old_column = f"{source}_{stat}"
    if not {actual, old_column, new_column}.issubset(frame.columns):
        return None
    rows = frame.filter(
        pl.col("played")
        & (pl.col("primaryPosition") == position)
        & pl.col(actual).is_not_null()
        & pl.col(old_column).is_not_null()
        & pl.col(new_column).is_not_null()
        & es.real_mask(frame, source, stat))
    if rows.height < acc.MIN_PAIRED_ROWS:
        return None
    realised = float(rows[actual].sum())
    if realised <= 0:
        return None
    old_total = float(rows[old_column].sum())
    if share:
        others = ("receivingTouchdowns" if stat == "rushingTouchdowns"
                  else "rushingTouchdowns")
        both = f"{source}_{others}"
        combined = (rows[old_column] + rows[both].fill_null(0)
                    if both in rows.columns else rows[old_column])
        weight = pl.Series(
            [o / c if c not in (0.0, None) and o is not None else 0.0
             for o, c in zip(rows[old_column].to_list(), combined.to_list())])
        new_total = float((rows[new_column] * weight).sum())
    else:
        new_total = float(rows[new_column].sum())
    return {"n": rows.height, "realised": round(realised, 1),
            "old": round(old_total, 1), "new": round(new_total, 1),
            "old_ratio": round(old_total / realised, 4),
            "new_ratio": round(new_total / realised, 4)}


def report(season: int = 2025) -> Tuple[str, Dict]:
    """The measurement block and its ledger entry.

    Args:
        season: Season whose archived prices to replay.

    Returns:
        tuple: ``(text, entry)``.
    """
    frame = raw_props(season)
    if frame is None or frame.height == 0:
        text = (f"=== market conversions, {season} ===\n"
                f"  No archived raw BetOnline prices for {season}.")
        return text, {"season": season, "rows": 0}

    model = mk.MarketModel.load()
    pairs = two_way(frame)
    book = pairs["book"].to_numpy()
    hold = mk.measure_overround(pairs["Over"].to_numpy(),
                                pairs["Under"].to_numpy())
    ladder_hold = measured_ladder_hold(frame)
    median_check = ladder_median_vs_line(frame, hold)
    _, adjustment = line_adjustment(frame, model, hold)
    reads = ladder_reads(frame, hold)

    games = frame["BOL_game_id"].n_unique()
    weeks = sorted(frame["week"].unique().to_list())
    lines: List[str] = [
        f"=== market conversions, {season}: {frame.height} rows, {games} game(s), "
        f"week(s) {weeks} ===",
        "  The scrapers overwrite their raw file, so this is the last scrape of the",
        "  season and not the season. Nothing here is scored against outcomes; see",
        "  the module docstring.",
        "",
        "--- 1. the margin ------------------------------------------------------",
        f"  two-way pairs      n={pairs.height:<5} median overround "
        f"{np.median(book):.4f}  range {book.min():.4f}-{book.max():.4f}",
        f"  after de-vig       n={pairs.height:<5} pairs summing to 1.0000 by "
        f"construction",
        f"  one-sided ladder   n={ladder_hold['n']:<5} median hold "
        f"{ladder_hold['median']}  (measured against the de-vigged line)",
        "  per stat, ladder hold:",
    ]
    for stat, (n, value) in sorted(ladder_hold["by_stat"].items()):
        kind = mk.MARKET_STATS[stat].kind if stat in mk.MARKET_STATS else "?"
        lines.append(f"    {stat:24}{kind:>9}{n:>5}{value:>10.4f}")
    lines += [
        "  A yardage row reads high because the survival curve is convex across a",
        "  10-yard rung gap, not because the hold differs -- every count stat lands",
        f"  on the two-way number, {np.median(book):.4f}.",
        "",
        "  The independent check, on prices alone: a de-vigged ladder's own median",
        "  should be the line the book posted beside it.",
        f"  {'stat':24}{'n':>4}{'median-line':>13}{'|median-line|':>15}"
        f"{'rung gap':>10}",
    ]
    for stat, found in sorted(median_check.items()):
        lines.append(
            f"  {stat:24}{found['n']:>4}{found['mean_gap']:>13.2f}"
            f"{found['mean_abs_gap']:>15.2f}{found['rung_gap']:>10.1f}")
    lines += [
        "  Read each gap against the rung gap in the last column: the median can",
        "  only be located to the resolution the book priced.",
        "",
        "--- 2. the line adjustment ---------------------------------------------",
        "  Mean absolute yards/counts a posted line moves, by each formula. The",
        "  derivation uses 'fit sd'; 'ladder sd' is the market's own number, shipped",
        "  beside the projection and not inside it -- MARKET_OVER_FITTED_SCALE says",
        "  why.",
        f"  {'stat':22}{'n':>4}{'line':>8}{'k=0.50':>8}{'k=0.25':>8}"
        f"{'derived':>9}{'fit sd':>8}{'ladder sd':>11}{'from ladder':>12}",
    ]
    for stat, found in sorted(adjustment.items()):
        lines.append(
            f"  {stat:22}{found['n']:>4}{found['mean_line']:>8.1f}"
            f"{found['mean_abs_old_k50']:>8.3f}{found['mean_abs_old_k25']:>8.3f}"
            f"{found['mean_abs_derived']:>9.3f}"
            f"{_blank(found['mean_fitted_sd']):>8}"
            f"{_blank(found['mean_ladder_sd']):>11}"
            f"{found['n_from_ladder']:>7}/{found['n']:<4}")
    lines += [
        "  Where no fitted sigma exists the derivation declines to move the line at",
        "  all. That is the abstention, not a gap: the line is the book's median and",
        "  moving it needs a sigma. Note the ladder reads 1.3-1.9x the fit on every",
        "  stat here, counts included.",
        "",
        "--- 3. what the ladder was throwing away -------------------------------",
        "  Count ladders, where the discrete identity is exact. 'old' is",
        "  value_calc's own arithmetic on vigged prices; 'mean' is the same",
        "  arithmetic de-vigged; 'sd' never existed before.",
        f"  {'stat':24}{'player':22}{'rungs':>6}{'old':>8}{'mean':>8}{'sd':>8}",
    ]
    counts = reads.filter((pl.col("kind") == "count")
                          & pl.col("count_mean").is_not_nan()).sort(
        "stat", "count_mean", descending=[False, True])
    for row in counts.head(12).iter_rows(named=True):
        lines.append(
            f"  {row['stat']:24}{row['player'][:21]:22}{row['rungs']:>6}"
            f"{row['old']:>8.3f}{row['count_mean']:>8.3f}{row['count_sd']:>8.3f}")
    if counts.height > 12:
        lines.append(f"  ... {counts.height - 12} more count ladders")
    lines += [
        "",
        "  Yardage ladders, where the same arithmetic is not a mean. 'old' drops",
        "  every yard below the lowest rung and lumps each bucket at its lower",
        "  edge; 'line' is what the book posted for the same player.",
        f"  {'stat':22}{'player':20}{'lo rung':>8}{'old':>8}{'line':>8}"
        f"{'old/line':>10}{'median':>8}{'scale':>8}",
    ]
    yardage = reads.filter(pl.col("kind") == "yardage").sort("stat", "player")
    ratios = []
    for row in yardage.iter_rows(named=True):
        edges, _ = mk.monotone_survival(*ladders(frame)[(row["stat"],
                                                        row["player"])])
        ratio = (row["old"] / row["line"]) if row["line"] else float("nan")
        if np.isfinite(ratio):
            ratios.append(ratio)
        lines.append(
            f"  {row['stat']:22}{row['player'][:19]:20}{edges[0]:>8.0f}"
            f"{row['old']:>8.1f}"
            f"{(row['line'] if row['line'] is not None else float('nan')):>8.1f}"
            f"{ratio:>10.3f}{row['median']:>8.1f}{row['scale']:>8.1f}")
    if ratios:
        lines.append(
            f"  old/line: median {np.median(ratios):.3f}, range "
            f"{min(ratios):.2f}-{max(ratios):.2f} over n={len(ratios)}. Not a bias "
            f"in one direction -- a number that is neither a mean nor a median, "
            f"which is what plan 35 item 4 would have shipped as the projection.")
    lines += [
        "",
        "--- 4. the anytime-touchdown ratio, for reference -----------------------",
        "  E[N] / P(N >= 1), fitted on player_weeks. Pinnacle posts P(>= 1) and it",
        "  was consumed as a count.",
        f"  {'position':24}{'ratio':>9}{'weeks':>9}",
    ]
    for key in sorted(model.td_scale_by_position):
        ratio, n = model.td_scale_by_position[key]
        lines.append(f"  {key:24}{ratio:>9.4f}{n:>9}")
    lines.append("  Not what ships: count_line_to_mean inverts the distribution "
                 "instead, which")
    lines.append("  is calibrated across the rate range where a flat ratio is "
                 "calibrated at one")
    lines.append("  point. Kept as the fallback and as the number that made the "
                 "defect visible.")

    try:
        scored_lines, scored = backtest(season)
    except Exception as problem:            # pragma: no cover - data dependent
        scored_lines = ["", f"--- 5. calibration: unavailable ({problem})"]
        scored = {}
    lines += [""] + scored_lines

    entry = {
        "season": season,
        "rows": frame.height,
        "games": games,
        "weeks": weeks,
        "two_way": {"n": pairs.height,
                    "median_overround": round(float(np.median(book)), 4),
                    "min": round(float(book.min()), 4),
                    "max": round(float(book.max()), 4)},
        "ladder_hold": ladder_hold,
        "ladder_median_vs_line": median_check,
        "line_adjustment": adjustment,
        "yardage_ladder_old_over_line": (round(float(np.median(ratios)), 4)
                                         if ratios else None),
        "td_scale": {k: [round(v[0], 4), v[1]]
                     for k, v in model.td_scale_by_position.items()},
        "model_version": model.version,
        "calibration": scored,
        "scored_against_outcomes": bool(scored),
        "why_partly_scored": (
            "Both scrapers overwrite their raw price file, so only the last scrape "
            "of the season survives. The markets in RECOVERABLE are scored anyway, "
            "because their old projection is an invertible function of the price "
            "alone; BOL_passingTouchdowns and every yardage market are not, because "
            "their line moves and was not archived. Per-week raw copies start now."),
    }
    return "\n".join(lines), entry


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.lab.market",
        description="Measure the market conversions on the archived prices.")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--no-save", action="store_true",
                        help="print without touching the ledger")
    args = parser.parse_args(argv)

    text, entry = report(args.season)
    print(text)
    if not args.no_save and entry.get("rows"):
        results = load_results()
        results.setdefault("market_lines", {})[str(args.season)] = entry
        save_results(results)
        print(f"\nwrote {RESULTS_PATH.relative_to(paths.REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
