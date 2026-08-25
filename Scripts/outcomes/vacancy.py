"""Where a missing starter's work goes, as a rule a simulation can apply.

:mod:`Scripts.outcomes.evidence` measured the transfer and printed it. This fits it:
what share of a vacated lead's opportunity reappears on the man behind him, per position,
with a standard error beside it so the number cannot be read as more precise than it is.

**The rule exists for two position groups and not for the third, and that is the finding
rather than a simplification.** A backfield is very nearly zero-sum -- a lead back's 17.42
opportunities a game go 81% to the next three backs and the room keeps 93% of its volume
-- while a receiver room is not: 45% of a lead receiver's targets reappear within the
room, his direct understudy gains **0.59 of 7.72**, and the offence simply throws 1.25
fewer times. A rule that handed a WR1's targets to the WR2 would be inventing 2.8 targets
a game from nothing, so WR rooms get no transfer rule at all and their vacancy is modelled
as group shrinkage with no beneficiary.

**The applied rule is coarser than the fitted one, and it has to be.** The shares are
fitted to ranks 2, 3 and 4 of the room's *realised* season order, which is available in
history and not in a projection. What a projection has is
:func:`Scripts.usage.context.load_depth_charts`, whose rank is clipped to
``MAX_DEPTH_RANK = 3`` because the 2016-2024 upstream schema carries nothing finer -- and
every backtest fold is on that schema. So the rule is also published in the form that
survives the clip: a share to rank 2, and a share to *everyone below rank 2* distributed
in proportion to their own baseline opportunity. That is self-normalising, indifferent to
room size, and means the same thing under both schemas.

Usage::

    python -m Scripts.outcomes.vacancy --report      # print the table
    python -m Scripts.outcomes.vacancy --write       # fit and persist

See ``docs/plans/28-outcome-distributions.md``.
"""

from __future__ import annotations

import argparse
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts import paths
from Scripts.outcomes import evidence as ev

#: Positions a transfer rule is fitted and applied for.
#:
#: Not WR, and see the module docstring: the measured within-room recapture is 45% and
#: lands mostly on ranks 3 and 4 while the offence itself contracts.
TRANSFER_POSITIONS: Tuple[str, ...] = ("RB", "TE")

#: Positions the closure table is *measured* for, including the one that gets no rule.
#:
#: WR is measured and published precisely so the decision not to build a rule for it is
#: visible as a number rather than as an assertion in a docstring.
MEASURED_POSITIONS: Tuple[str, ...] = ("RB", "WR", "TE")

#: Deepest rank the transfer is fitted to.
#:
#: Four, because that is where the recapture has flattened -- rank 4 takes 0.15 of an RB1's
#: vacated work against rank 2's 0.41 -- and because a fifth back is rostered by too few
#: teams for the cell to mean anything.
MAX_TRANSFER_RANK: int = 4

#: Resamples behind the standard error.
#:
#: Two hundred is enough for a standard error, which is all this is used for; it is not
#: enough for a percentile interval and none is published.
BOOTSTRAP_DRAWS: int = 200

#: Seed for the bootstrap, fixed so the artifact is a function of its inputs.
BOOTSTRAP_SEED: int = 28

#: Where the fitted rule lives. Not season-scoped and not in ``store.ARTIFACTS``.
#:
#: Pooling across seasons is the point, and a vacated backfield is the same vacated
#: backfield in all nine leagues -- there is nothing league-specific or season-specific in
#: it to scope by. ``s3_store.MIRROR_TIERS["NFL"]`` publishes it as
#: ``nfl/vacancy_transfer.parquet`` with no new plumbing.
VACANCY_PARQUET = paths.DATA_DIR / "NFL" / "vacancy_transfer.parquet"


def _cells(weeks: pl.DataFrame, position: str,
           min_in: int = 3, min_out: int = 2) -> pl.DataFrame:
    """Per team-season, rank and lead state: total opportunity and how many weeks.

    The intermediate the point estimate and the bootstrap are both computed from, so the
    two cannot disagree about which team-seasons are in the sample. Restricted, as
    :func:`Scripts.outcomes.evidence.closure` is, to team-seasons that experienced **both**
    states, which is what makes the comparison within-team rather than between-team.

    Args:
        weeks: :func:`Scripts.outcomes.evidence.load_weeks` output.
        position: Position group.
        min_in: Weeks the lead must have played.
        min_out: Weeks he must have missed.

    Returns:
        pl.DataFrame: ``season``, ``team``, ``rk``, ``lead_played``, ``opp``, ``weeks``.
    """
    ranked = ev._ranked(weeks, position)
    state = ev._lead_state(weeks, ranked)
    both = (state.group_by(["season", "team"])
            .agg(pl.col("lead_played").sum().alias("n_in"),
                 (~pl.col("lead_played")).sum().alias("n_out"))
            .filter((pl.col("n_in") >= min_in) & (pl.col("n_out") >= min_out))
            .select("season", "team"))
    return (ranked.filter(pl.col("rk") <= MAX_TRANSFER_RANK)
            .group_by(["season", "team", "week", "rk"])
            .agg(pl.col("opp").sum())
            .join(both, on=["season", "team"])
            .join(state, on=["season", "team", "week"], how="left")
            .group_by(["season", "team", "rk", "lead_played"])
            .agg(pl.col("opp").sum().alias("opp"), pl.len().alias("weeks"))
            .sort(["season", "team", "rk", "lead_played"]))


def _shares(cells: pl.DataFrame,
            keys: Optional[Sequence[Tuple[int, str]]] = None
            ) -> Dict[int, float]:
    """Vacated share reaching each rank, from a set of team-seasons.

    The estimator, isolated so the bootstrap re-runs exactly it. Each rank's mean
    opportunity is a weeks-weighted mean over the selected team-seasons, and the share is
    ``(mean when the lead is out - mean when he is in) / (the lead's own mean)``.

    Args:
        cells: :func:`_cells` output.
        keys: ``(season, team)`` pairs to include, with repeats honoured so a bootstrap
            resample is weighted correctly. None uses every team-season once.

    Returns:
        dict: Rank to share. Empty when the lead's own opportunity cannot be measured.
    """
    frame = cells
    if keys is not None:
        picked = pl.DataFrame({"season": [int(s) for s, _ in keys],
                               "team": [t for _, t in keys]},
                              schema={"season": pl.Int32, "team": pl.String})
        # An inner join with repeats duplicates the matching rows, which is what a
        # bootstrap weight is.
        frame = cells.join(picked, on=["season", "team"], how="inner")

    pooled = (frame.group_by(["rk", "lead_played"])
              .agg((pl.col("opp").sum() / pl.col("weeks").sum()).alias("per_week")))
    lookup = {(int(r), bool(p)): float(v) for r, p, v in pooled.rows()}

    vacated = lookup.get((1, True))
    if not vacated:
        return {}
    return {rank: (lookup.get((rank, False), 0.0) - lookup.get((rank, True), 0.0)) / vacated
            for rank in range(2, MAX_TRANSFER_RANK + 1)}


def fit(weeks: Optional[pl.DataFrame] = None,
        positions: Sequence[str] = MEASURED_POSITIONS) -> pl.DataFrame:
    """Fit the redistribution rule, with a bootstrap standard error.

    Args:
        weeks: :func:`Scripts.outcomes.evidence.load_weeks` output. None loads it.
        positions: Position groups to measure.

    Returns:
        pl.DataFrame: One row per (position, rank) with ``share``, ``share_se``,
        ``vacated`` (the lead's own per-week opportunity), ``recapture`` (the position's
        total across ranks 2-4), ``applied`` (whether a rule is used for this position at
        all) and ``team_seasons``.
    """
    weeks = ev.load_weeks() if weeks is None else weeks
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    rows: List[Dict[str, object]] = []

    for position in positions:
        cells = _cells(weeks, position)
        point = _shares(cells)
        if not point:
            continue

        universe = [(int(s), t) for s, t in
                    cells.select("season", "team").unique().sort(["season", "team"]).rows()]
        draws: Dict[int, List[float]] = {rank: [] for rank in point}
        for _ in range(BOOTSTRAP_DRAWS):
            picked = [universe[i] for i in
                      rng.integers(0, len(universe), size=len(universe))]
            for rank, value in _shares(cells, picked).items():
                draws[rank].append(value)

        lead = (cells.filter((pl.col("rk") == 1) & pl.col("lead_played"))
                .select((pl.col("opp").sum() / pl.col("weeks").sum())).item())
        recapture = float(sum(point.values()))
        for rank, share in sorted(point.items()):
            rows.append({
                "position": position,
                "rank": rank,
                "share": float(share),
                "share_se": float(np.std(draws[rank])) if draws[rank] else float("nan"),
                "vacated": float(lead),
                "recapture": recapture,
                "applied": position in TRANSFER_POSITIONS,
                "team_seasons": len(universe),
            })
    return pl.DataFrame(rows)


def applied_rule(table: pl.DataFrame) -> Dict[str, Dict[str, float]]:
    """Collapse the fitted rule onto the scale a depth chart can express.

    ``depth_rank`` is clipped to 3 and its rank 3 means "everyone else", so a rule keyed
    to ranks 2/3/4 cannot be applied as fitted. Ranks 3 and above are pooled into one
    share, which the simulation splits among whoever is actually there in proportion to
    their own baseline opportunity -- a division the fitted rank ordering cannot supply
    and the projection can.

    Args:
        table: :func:`fit` output.

    Returns:
        dict: Position to ``{"rank_2": ..., "rank_rest": ...}``, for the positions a rule
        applies to. Positions with no rule are absent, so a caller iterating this cannot
        accidentally give a receiver room a transfer.
    """
    out: Dict[str, Dict[str, float]] = {}
    for position in table["position"].unique().to_list():
        block = table.filter((pl.col("position") == position) & pl.col("applied"))
        if not block.height:
            continue
        by_rank = dict(block.select("rank", "share").rows())
        out[position] = {
            "rank_2": float(by_rank.get(2, 0.0)),
            "rank_rest": float(sum(v for r, v in by_rank.items() if r >= 3)),
        }
    return out


def write(table: pl.DataFrame, path=None):
    """Persist the fitted rule.

    Args:
        table: :func:`fit` output.
        path: Destination. Defaults to :data:`VACANCY_PARQUET`.

    Returns:
        Path: Where it was written.
    """
    path = VACANCY_PARQUET if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    table.write_parquet(path)
    return path


def load(path=None) -> pl.DataFrame:
    """Read the fitted rule back.

    Args:
        path: Source. Defaults to :data:`VACANCY_PARQUET`.

    Returns:
        pl.DataFrame: :func:`fit` output.

    Raises:
        FileNotFoundError: Naming the command that builds it, rather than the path alone.
    """
    path = VACANCY_PARQUET if path is None else path
    if not path.exists():
        raise FileNotFoundError(
            f"{path} does not exist. Build it with "
            f"`python -m Scripts.outcomes.vacancy --write`.")
    return pl.read_parquet(path)


def report(table: Optional[pl.DataFrame] = None) -> str:
    """The fitted rule, printable.

    Args:
        table: :func:`fit` output. None fits it.

    Returns:
        str: The report.
    """
    table = fit() if table is None else table
    lines = ["=== vacancy transfer: share of a vacated lead's opportunity ===", ""]
    for position in MEASURED_POSITIONS:
        block = table.filter(pl.col("position") == position)
        if not block.height:
            continue
        first = block.row(0, named=True)
        rule = "applied" if first["applied"] else "MEASURED, NOT APPLIED"
        lines.append(f"  {position}  ({first['team_seasons']} team-seasons, "
                     f"lead vacates {first['vacated']:.2f} opp/gm)  [{rule}]")
        lines.append(f"    {'rank':<8}{'share':>9}{'se':>8}")
        for row in block.sort("rank").iter_rows(named=True):
            lines.append(f"    {row['rank']:<8}{row['share']:>9.3f}"
                         f"{row['share_se']:>8.3f}")
        lines.append(f"    {'2-4':<8}{first['recapture']:>9.3f}")
        lines.append("")

    lines += ["  applied form, on the scale a depth chart can express:", ""]
    for position, shares in sorted(applied_rule(table).items()):
        lines.append(f"    {position}  rank 2: {shares['rank_2']:.3f}   "
                     f"ranks 3+: {shares['rank_rest']:.3f}")
    lines += ["",
              "  WR is measured and deliberately not applied: the understudy gains 0.59",
              "  of 7.72 targets and the offence throws 1.25 fewer times. See the module",
              "  docstring and docs/plans/28-outcome-distributions.md."]
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Fit, report and optionally persist."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", action="store_true",
                        help="persist to Data/NFL/vacancy_transfer.parquet")
    parser.add_argument("--report", action="store_true",
                        help="print the fitted rule (the default)")
    args = parser.parse_args(argv)

    table = fit()
    print(report(table))
    if args.write:
        print(f"\n  wrote {write(table)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
