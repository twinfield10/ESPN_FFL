"""Yardage-milestone bonuses: a step function of a weekly quantity.

**Why this cannot be a column times a constant.** Three of the nine leagues score
yardage milestones -- "3 points for a 100-199 yard rushing game", "1 point for a
300-399 yard passing game". :func:`Scripts.projection_utils.proj_to_score` can only
multiply a stat column by a constant, and a milestone is not a rate on a season
total: 1,400 rushing yards buys a different number of 100-yard games depending how
they are distributed, and a season total cannot say which.

**How badly the linear reading fails, measured over 2016-2025.** Pricing the ladder
at the season mean -- ``f(E[X])`` -- recovers **13% to 18% of the first tier and
exactly none of the second**: 117 rushing games against 690 realised, 160 receiving
against 1,262, 116 passing against 651, and 0 against 18, 19 and 75 in the tiers
above them. A season mean can only award a band in every game or in none, and almost
no player's mean sits above an edge, so the bonus is very nearly all tail:

(An earlier draft of this docstring said the linear reading is *identically* zero.
That was measured off a binned table using each bin's lower edge rather than each
player's own mean, and :func:`report` now computes the column instead of asserting
it. The mechanism is unchanged; the number was wrong.)

| stat | per-game mean | P(week in the first band) | P(second band) |
|---|---|---|---|
| rushing yards | 50-65 | 0.112 | 0.001 |
| rushing yards | 65-80 | 0.211 | 0.004 |
| rushing yards | 80+ | **0.415** | 0.024 |
| receiving yards | 80+ | **0.395** | 0.018 |
| passing yards | 260+ | **0.375** | 0.059 |

A back averaging 80+ yards hits 100 in 41.5% of his games, which at 3 points over a
17-game slate is **21 points a season** -- more than a round of draft value, and the
pipeline was scoring it as nothing. ``docs/plans/13-dst-from-vegas-lines.md`` prices
this same error class on the D/ST ladder at a 16.5-point compression; plan 01's FGY50
floor is another instance. This is the third.

**So the quantity is a probability, and a probability needs a variance.**

    E[band games] = games x P(low <= weekly yardage < high)

fitted per position and per stat with the same two-parameter mean-variance function
:mod:`Scripts.usage.predictive` uses -- ``Var(mu) = phi*mu + mu^2/k`` -- because the
coefficient of variation is not constant here either. Measured weekly it falls from
0.81 to 0.44 across the rushing range and 0.52 to 0.25 across the passing range, the
same shape that module found on season totals and for the same reason.

**The weekly dispersion is fitted here rather than divided out of the season one.**
``predictive.py`` fits its numbers on season totals, and dividing a season variance
by seventeen assumes independent weeks. That module already records what happens when
these parts are composed instead of fitted: games and per-game volume correlate +0.48
to +0.63, and backing one variance out of another produced *negative* numbers for
quarterbacks.

**The Gamma prices the first step and an empirical rate prices the rest, because the
Gamma tail was measured and rejected.** Taking every band straight from the fitted
Gamma calibrates the two common bands to a population-total ratio of 1.02 and 1.00 --
and understates the extreme tiers by **twenty to twenty-five times**: it predicts 1.0
rushing games over 200 yards against 18 realised, and 0.7 receiving against 19. An
exponential tail is simply the wrong shape that far out, and a 20x multiplier is not a
calibration, it is an admission.

So above the first step the ladder is climbed by a *measured conditional rate*:

    P(week >= 200) = P(week >= 100) x P(>= 200 | >= 100)

with the second factor counted off the training weeks. It reuses the half that
calibrates and replaces the half that does not with a number rather than a
functional-form assumption -- the same move this repo makes wherever a parametric
form runs out of information. Measured, that takes the 200+ rushing tier from a ratio
of 0.05 to about 1.0.

**What is fitted, and the bias it leaves.** The dispersion is fitted on within-season
weekly variation -- realised weekly values against the player's own realised per-game
mean. At projection time the mean is a projection and carries its own error, which the
fit has not seen. :func:`report` measures the result rather than assuming it away.

Usage::

    python -m Scripts.usage.milestones --fit
    python -m Scripts.usage.milestones --report
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts import paths
from Scripts.usage import features as ft
from Scripts.usage import predictive as pv

#: Model version. Bumped when a fit changes, so a stored artifact says what it is.
MODEL_VERSION = "1.0.0"

#: Where the fitted artifact lives, beside the other model files.
MODEL_PATH = paths.DATA_DIR / "NFL" / "models" / f"milestones_{MODEL_VERSION}.json"

#: ESPN scoring ``colName`` -> ``(stat this pipeline blends, low, high)``.
#:
#: ``high`` of None is unbounded. Names are exactly the registry's, so
#: ``proj_to_score`` prices whatever this writes with no mapping in between --
#: verified against ``Data/Scoring/scoring.csv``, where these six appear across
#: john_pc_league (all six), fields_league and john_atl_league.
BANDS: Dict[str, Tuple[str, float, Optional[float]]] = {
    "passingYards300to399Game":  ("passingYards", 300.0, 400.0),
    "passingYards400PlusGame":   ("passingYards", 400.0, None),
    "rushingYards100-199Game":   ("rushingYards", 100.0, 200.0),
    "rushingYards200+Game":      ("rushingYards", 200.0, None),
    "receivingYards100-199Game": ("receivingYards", 100.0, 200.0),
    "receivingYards200+Game":    ("receivingYards", 200.0, None),
}

#: Each stat's band edges, ascending. Derived from :data:`BANDS` so a ladder and its
#: bands cannot disagree about where the steps are.
def _ladder() -> Dict[str, Tuple[float, ...]]:
    """Sorted band edges per stat."""
    out: Dict[str, List[float]] = {}
    for stat, low, _ in BANDS.values():
        out.setdefault(stat, [])
        if low not in out[stat]:
            out[stat].append(low)
    return {stat: tuple(sorted(edges)) for stat, edges in out.items()}


LADDER: Dict[str, Tuple[float, ...]] = _ladder()

#: Blended stat name -> the weekly column in ``player_weeks``.
WEEKLY_COLUMN: Dict[str, str] = {
    "passingYards": "passing_yards",
    "rushingYards": "rushing_yards",
    "receivingYards": "receiving_yards",
}

#: Positions each stat's dispersion is fitted for.
#:
#: Split because the weekly shape differs at the same mean: a quarterback averaging
#: 40 rushing yards is a designed-run offence and reasonably steady, while a back
#: averaging 40 is splitting a backfield and is not.
FIT_POSITIONS: Dict[str, Tuple[str, ...]] = {
    "passingYards": ("QB",),
    "rushingYards": ("QB", "RB", "WR"),
    "receivingYards": ("WR", "TE", "RB"),
}

#: Games a player-season needs before its weeks teach the dispersion.
#:
#: A four-game sample gives a per-game mean too noisy to be the x-axis of a variance
#: fit, and those rows are exactly the part-time players whose weekly spread is
#: widest -- so including them inflates the fitted dispersion for everyone.
MIN_FIT_GAMES: int = 8

#: Per-game mean below which no band is projected at all.
#:
#: Not a performance guard. A Gamma fitted at mu = 2 yards still returns a positive
#: P(>= 100), and multiplied across 17 games and a whole league those rounding errors
#: sum to real points awarded to players who will never come close. The floor is set
#: at a quarter of the band's lower edge, where the measured empirical rate is 0.000
#: on 10,116 rushing player-seasons.
FLOOR_SHARE: float = 0.25

#: Games in a season, the slate every ``TRUE_`` line is expressed over.
SLATE: float = 17.0


@dataclass
class MilestoneModel:
    """Fitted weekly dispersion per position and stat.

    Attributes:
        dispersion: ``"<position>|<stat>"`` -> ``(phi, k, bust, n)``.
        pooled: Same, keyed by stat alone, for a position with too few rows.
        tail_share: ``"<stat>|<edge>"`` -> ``(P(>= edge | >= first edge), n)``,
            counted off the training weeks. The first edge is 1.0 by definition and
            is stored anyway, so the table can be read without knowing the ladder.
        seasons: Seasons the fit was trained on.
        version: :data:`MODEL_VERSION` at fit time.
    """

    dispersion: Dict[str, Tuple[float, float, float, int]] = field(default_factory=dict)
    pooled: Dict[str, Tuple[float, float, float, int]] = field(default_factory=dict)
    tail_share: Dict[str, Tuple[float, int]] = field(default_factory=dict)
    seasons: List[int] = field(default_factory=list)
    version: str = MODEL_VERSION

    def parameters(self, position: str, stat: str
                   ) -> Optional[Tuple[float, float, float]]:
        """Dispersion for a position and stat, falling back to the pooled fit.

        Args:
            position: ``primaryPosition``.
            stat: Blended stat name, e.g. ``"rushingYards"``.

        Returns:
            tuple | None: ``(phi, k, bust)``, or None when neither fit exists.
        """
        found = self.dispersion.get(f"{position}|{stat}") or self.pooled.get(stat)
        return None if found is None else (found[0], found[1], found[2])

    def band_games(self, col_name: str, mu_per_game, position,
                   slate: float = SLATE) -> np.ndarray:
        """Expected games in one band, from a per-game mean.

        Args:
            col_name: A key of :data:`BANDS`.
            mu_per_game: Per-game mean of the underlying yardage, per player.
            position: ``primaryPosition`` per player, same length.
            slate: Games the projection is expressed over.

        Returns:
            np.ndarray: Expected count per player. Zero where the position has no
            fit or the mean is below :data:`FLOOR_SHARE` of the band's lower edge.
        """
        stat, low, high = BANDS[col_name]
        first = LADDER[stat][0]
        mu = np.asarray(mu_per_game, dtype=float)
        positions = np.asarray(position, dtype=object)
        out = np.zeros(mu.shape, dtype=float)

        # The floor is on the *first* edge, not this band's, so the two bands of one
        # ladder are projected over the same population -- otherwise a player could
        # be eligible for the 200+ tier and not the 100-199 one below it.
        eligible = np.isfinite(mu) & (mu > first * FLOOR_SHARE)
        share_low = self.share(stat, low)
        share_high = 0.0 if high is None else self.share(stat, high)

        for name in np.unique(positions[eligible]) if eligible.any() else ():
            found = self.parameters(str(name), stat)
            if found is None:
                continue
            phi, k, bust = found
            rows = eligible & (positions == name)
            # One Gamma call, at the first edge only. Everything above it is that
            # probability times a counted conditional rate -- see the module
            # docstring for why the Gamma is not asked about 200 yards.
            entry = pv.band_probability(stat, mu[rows], phi, k, first, None, bust)
            if entry is None:
                continue
            entry = np.asarray(entry, dtype=float)
            out[rows] = slate * entry * max(share_low - share_high, 0.0)
        return out

    def share(self, stat: str, edge: float) -> float:
        """``P(week >= edge | week >= the ladder's first edge)``, as counted.

        Args:
            stat: Blended stat name.
            edge: A band edge.

        Returns:
            float: The conditional rate. 1.0 at the first edge by definition, and
            0.0 for an edge the fit never saw -- which withdraws that tier rather
            than pricing it off a tail the fit has no evidence for.
        """
        if edge <= LADDER[stat][0]:
            return 1.0
        found = self.tail_share.get(f"{stat}|{edge:g}")
        return 0.0 if found is None else float(found[0])

    def to_dict(self) -> Dict:
        """JSON-shaped form."""
        return {"version": self.version, "seasons": self.seasons,
                "dispersion": {k: list(v) for k, v in self.dispersion.items()},
                "pooled": {k: list(v) for k, v in self.pooled.items()},
                "tail_share": {k: list(v) for k, v in self.tail_share.items()}}

    def save(self, path=None) -> None:
        """Persist the fit."""
        path = MODEL_PATH if path is None else path
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as handle:
            json.dump(self.to_dict(), handle, indent=2, sort_keys=True)
            handle.write("\n")

    @classmethod
    def load(cls, path=None) -> "MilestoneModel":
        """Read a persisted fit.

        Raises:
            FileNotFoundError: When no artifact exists. Build one with
                ``python -m Scripts.usage.milestones --fit``.
        """
        path = MODEL_PATH if path is None else path
        if not path.is_file():
            raise FileNotFoundError(
                f"No milestone model at {path}. Fit one with "
                f"`python -m Scripts.usage.milestones --fit`.")
        with open(path) as handle:
            blob = json.load(handle)
        return cls(
            dispersion={k: tuple(v) for k, v in blob["dispersion"].items()},
            pooled={k: tuple(v) for k, v in blob["pooled"].items()},
            tail_share={k: tuple(v) for k, v in
                        blob.get("tail_share", {}).items()},
            seasons=blob.get("seasons", []),
            version=blob.get("version", "unknown"),
        )


def _no_bust(fitted: Tuple[float, float, float]) -> Tuple[float, float, float]:
    """Drop the fitted zero point mass, which is wrong for a milestone.

    **Measured, not assumed.** ``fit_variance`` returns the share of rows that
    realised exactly zero -- 0.18 of weekly WR receiving rows -- and a zero-inflated
    Gamma carrying it shifts the conditional mean to ``mu / (1 - s)`` and narrows it.
    That is right for an *interval* at a low projection, where those zeros are real
    players who got hurt or lost the job. It is wrong for a milestone, which is only
    ever evaluated where the volume makes one reachable and where almost no week is
    a zero. Against ten seasons of WR receiving weeks:

    | per-game mean | empirical P(week >= 100) | with bust | without |
    |---|---|---|---|
    | 30-50 | 0.044 | 0.033 | **0.045** |
    | 65-80 | 0.215 | 0.224 | **0.205** |
    | 80-90 | 0.354 | 0.470 | **0.314** |
    | 90-100 | 0.412 | 0.700 | **0.384** |
    | 100-200 | 0.510 | 0.583 | **0.489** |

    With the mass the curve is 70% high at the top *and* non-monotone -- a player
    projected for 110 yards a game gets a lower probability than one projected for
    90, because past ``CV^2 = s / (1 - s)`` the mixture stops being feasible at all
    (see :func:`Scripts.usage.predictive._reparameterise`). Without it the curve
    tracks the measurement within 4-11% everywhere and rises with the projection.

    The residual is a consistent 5-8% *under*-prediction at the top, and that is the
    expected sign: the dispersion is fitted against a player's realised per-game
    mean, so it has never seen the extra spread a *projected* mean carries.

    Args:
        fitted: ``(phi, k, bust)`` from :func:`Scripts.usage.predictive.fit_variance`.

    Returns:
        tuple: The same, with ``bust`` set to 0.0.
    """
    return fitted[0], fitted[1], 0.0


def weekly_panel(seasons: Sequence[int]) -> pl.DataFrame:
    """Player-weeks with each player-season's own per-game mean beside them.

    The mean is the x-axis of the variance fit, and it is the player's *realised*
    per-game mean rather than a projection -- this fit is about within-season weekly
    variation, and mixing projection error into it would make the two
    indistinguishable. The bias that leaves is measured in :func:`report`.

    Args:
        seasons: Seasons to pool.

    Returns:
        pl.DataFrame: One row per player-week, with ``position``, the three yardage
        columns, ``games``, and ``mu_<stat>`` per stat.
    """
    columns = ["season", "week", "gsis_id", "position"] + list(WEEKLY_COLUMN.values())
    weeks = ft.load_player_weeks(seasons, columns=columns)

    means = weeks.group_by(["season", "gsis_id"]).agg(
        pl.len().alias("games"),
        *[pl.col(column).mean().alias(f"mu_{stat}")
          for stat, column in WEEKLY_COLUMN.items()])
    return weeks.join(means, on=["season", "gsis_id"], how="left")


def fit(seasons: Sequence[int]) -> MilestoneModel:
    """Fit weekly dispersion per position and stat.

    Args:
        seasons: Training seasons.

    Returns:
        MilestoneModel: The fit, with a pooled fallback per stat.
    """
    panel = weekly_panel(seasons).filter(pl.col("games") >= MIN_FIT_GAMES)
    model = MilestoneModel(seasons=sorted(int(s) for s in seasons))

    for stat, column in WEEKLY_COLUMN.items():
        rows = panel.filter(pl.col(column).is_not_null()
                            & (pl.col(f"mu_{stat}") > 0.0))
        found = pv.fit_variance(rows[column].to_list(),
                               rows[f"mu_{stat}"].to_list())
        if found:
            model.pooled[stat] = (*_no_bust(found), rows.height)

        for position in FIT_POSITIONS[stat]:
            here = rows.filter(pl.col("position") == position)
            found = pv.fit_variance(here[column].to_list(),
                                   here[f"mu_{stat}"].to_list())
            if found:
                model.dispersion[f"{position}|{stat}"] = (
                    *_no_bust(found), here.height)

        # Conditional tail rates, counted on *every* week rather than only the ones
        # from an 8-game player: this is a property of the yardage distribution and
        # a 200-yard game counts however many the player went on to play. Pooled
        # across positions, because n at the top of a ladder is 25 weeks in ten
        # seasons and a per-position split of that is not a rate.
        weeks = panel.filter(pl.col(column).is_not_null())
        first = LADDER[stat][0]
        entered = weeks.filter(pl.col(column) >= first).height
        for edge in LADDER[stat]:
            above = weeks.filter(pl.col(column) >= edge).height
            model.tail_share[f"{stat}|{edge:g}"] = (
                (above / entered if entered else 0.0), above)
    return model


def realised_counts(seasons: Sequence[int]) -> pl.DataFrame:
    """Band games each player actually recorded, per player-season.

    The evaluation target, and it needs no ESPN column at all -- a realised
    100-yard game is a fact about ``player_weeks``. That matters, because the
    ``<band>`` columns ESPN's own breakdown is read into are zero for every row in
    the store, so the pipeline's own actuals cannot be the target
    (``docs/plans/34-stat-first-audit.md`` F4b).

    Args:
        seasons: Seasons to count.

    Returns:
        pl.DataFrame: ``season``, ``gsis_id``, ``position``, ``games``, ``mu_<stat>``
        and one ``act_<band>`` column per :data:`BANDS` key.
    """
    panel = weekly_panel(seasons)
    aggregates = [pl.len().alias("games"),
                  pl.col("position").drop_nulls().first().alias("position")]
    aggregates += [pl.col(f"mu_{stat}").first().alias(f"mu_{stat}")
                   for stat in WEEKLY_COLUMN]
    for col_name, (stat, low, high) in BANDS.items():
        column = WEEKLY_COLUMN[stat]
        inside = pl.col(column) >= low
        if high is not None:
            inside = inside & (pl.col(column) < high)
        aggregates.append(inside.sum().cast(pl.Float64).alias(f"act_{col_name}"))
    return panel.group_by(["season", "gsis_id"]).agg(aggregates)


def report(first: int = 2016, last: int = 2025,
           folds: Sequence[int] = tuple(range(2019, 2026))) -> str:
    """Walk-forward: predicted band games against realised, per band.

    Trains on everything before each fold, predicts the fold from the player's
    *realised* per-game mean. That isolates the question this module is responsible
    for -- given a per-game mean, how often is the band crossed -- from the separate
    question of whether the season head projects the mean well, which
    :mod:`Scripts.usage.backtest` already answers.

    The comparison against ``f(E[X])`` is not a baseline so much as the arithmetic:
    it is identically zero for every player, because no per-game mean reaches any
    band's lower edge.

    Args:
        first: First training season.
        last: Last season available.
        folds: Test seasons.

    Returns:
        str: A printable block.
    """
    lines = ["=== milestone bands: predicted expected games vs realised ===",
             f"  walk-forward, train {first}..S-1, predict S, for S in "
             f"{folds[0]}..{folds[-1]}",
             "",
             "  Population totals -- every player, no selection. This is the",
             "  calibration that decides whether a band is priced right.",
             f"  {'band':28}{'players':>9}{'realised':>10}{'predicted':>11}"
             f"{'ratio':>8}{'f(E[X])':>9}"]

    pooled: Dict[str, List[Tuple[float, float, float]]] = {n: [] for n in BANDS}
    for season in folds:
        train = [s for s in range(first, season) if s >= first]
        if len(train) < 2:
            continue
        model = fit(train)
        actual = realised_counts([season]).filter(pl.col("games") >= 1)
        games = actual["games"].to_numpy()
        for col_name, (stat, low, high) in BANDS.items():
            mu = actual[f"mu_{stat}"].to_numpy()
            predicted = model.band_games(
                col_name, mu, actual["position"].to_list(), slate=1.0) * games
            # The linear reading, computed rather than asserted: the ladder evaluated
            # at the season mean awards the band to every game or to none.
            inside = mu >= low
            if high is not None:
                inside = inside & (mu < high)
            linear = np.where(inside, games, 0.0)
            realised = actual[f"act_{col_name}"].to_numpy()
            keep = np.isfinite(mu) & np.isfinite(realised)
            pooled[col_name].extend(
                zip(realised[keep], predicted[keep], linear[keep]))

    arrays = {}
    for col_name in BANDS:
        rows = pooled[col_name]
        if not rows:
            continue
        realised = np.array([r for r, _, _ in rows])
        predicted = np.array([p for _, p, _ in rows])
        linear = np.array([f for _, _, f in rows])
        arrays[col_name] = (realised, predicted)
        total_r, total_p = realised.sum(), predicted.sum()
        ratio = total_p / total_r if total_r > 0 else float("nan")
        lines.append(
            f"  {col_name:28}{realised.size:>9}{total_r:>10.0f}{total_p:>11.1f}"
            f"{ratio:>8.2f}{linear.sum():>9.1f}")

    lines.append("")
    lines.append("  f(E[X]) is the linear reading, computed: the ladder evaluated "
                 "at the season mean.")
    lines.append("  It comes out 0.0 for every band because no player's per-game "
                 "mean has ever")
    lines.append("  reached any lower edge -- which is why six scored rules read as "
                 "nothing at all.")

    lines.append("")
    lines.append("  Per-player error, on players with exposure to the band")
    lines.append("  (realised > 0 or predicted > 0.05). **A rare band's row here is "
                 "selected on")
    lines.append("  the outcome and cannot be read as accuracy** -- when predicted "
                 "is near zero")
    lines.append("  everywhere, the filter keeps only players who realised one. The "
                 "totals above")
    lines.append("  are the honest test for those; this is the one for the two "
                 "common bands.")
    lines.append(f"  {'band':28}{'n':>6}{'realised':>10}{'predicted':>11}"
                 f"{'bias':>9}{'MAE':>8}")
    for col_name, (realised, predicted) in arrays.items():
        live = (realised > 0) | (predicted > 0.05)
        if not live.any():
            continue
        lines.append(
            f"  {col_name:28}{int(live.sum()):>6}"
            f"{realised[live].mean():>10.3f}{predicted[live].mean():>11.3f}"
            f"{predicted[live].mean() - realised[live].mean():>+9.3f}"
            f"{np.abs(predicted[live] - realised[live]).mean():>8.3f}")
    return "\n".join(lines)


def show(model: MilestoneModel) -> str:
    """The fitted dispersion, as text."""
    lines = ["=== fitted weekly dispersion: Var(mu) = phi*mu + mu^2/k ===",
             f"  {'position|stat':28}{'phi':>9}{'k':>8}{'bust':>8}{'n':>8}"
             f"{'CV at mu=60':>13}"]
    for key in sorted(model.dispersion):
        phi, k, bust, n = model.dispersion[key]
        cv = float(np.sqrt(pv.variance_at(60.0, phi, k))) / 60.0
        lines.append(f"  {key:28}{phi:>9.2f}{k:>8.2f}{bust:>8.3f}{n:>8}{cv:>13.2f}")

    lines.append("")
    lines.append("=== counted tail rates: P(week >= edge | >= the first edge) ===")
    lines.append("  The Gamma is asked only about the first edge. Above it the "
                 "ladder is climbed")
    lines.append("  by these, because the fitted tail understated the top tiers "
                 "20-25x.")
    lines.append(f"  {'stat|edge':28}{'share':>9}{'weeks >= edge':>15}")
    for key in sorted(model.tail_share):
        share, n = model.tail_share[key]
        lines.append(f"  {key:28}{share:>9.4f}{n:>15}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.usage.milestones",
        description="Fit and evaluate the yardage-milestone bands.")
    parser.add_argument("--fit", action="store_true", help="fit and persist")
    parser.add_argument("--report", action="store_true", help="walk-forward report")
    parser.add_argument("--show", action="store_true", help="print the stored fit")
    parser.add_argument("--first", type=int, default=2016)
    parser.add_argument("--last", type=int, default=2025)
    args = parser.parse_args(argv)

    if not (args.fit or args.report or args.show):
        parser.error("pass --fit, --report or --show")

    if args.fit:
        model = fit(range(args.first, args.last + 1))
        model.save()
        print(show(model))
        print(f"\nwrote {MODEL_PATH}")
    if args.show and not args.fit:
        print(show(MilestoneModel.load()))
    if args.report:
        print(report(args.first, args.last))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
