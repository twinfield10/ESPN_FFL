"""Snapshot the projection pipeline's output, so a refactor can be proven safe.

Plans 05, 06, 07, 10 and 11 all say "verify with the equivalence harness", but no
such harness existed -- Phase 0 did it ad hoc and nothing was committed. This is
that harness.

The pipeline is ~23s per league and almost entirely ESPN round-trips, so the
workflow is snapshot-then-compare rather than run-twice-in-one-process:

    python -m Scripts.equivalence snapshot --label before --season 2025
    ... make the change ...
    python -m Scripts.equivalence snapshot --label after  --season 2025
    python -m Scripts.equivalence compare before after

``compare`` reports per-column differences with a float tolerance, so noise from
reordering does not read as a regression. Snapshots live under ``Data/Equivalence``
and are gitignored -- they are large and regenerable.
"""

import argparse
import sys
from typing import Dict, Iterable, List, Optional

import pandas as pd

from Scripts.config_utils import build_lg_vars, get_season
from Scripts.paths import DATA_DIR

EQUIV_DIR = DATA_DIR / "Equivalence"

#: Columns that identify a row. Snapshots are sorted and indexed on these so two
#: runs line up even if the pipeline's row order changes.
KEY_COLUMNS = ["league_id", "year", "week", "player_name", "slotPosition"]

#: Absolute tolerance for float comparison. Tighter than any real behaviour change
#: this pipeline can produce, looser than float re-association noise.
TOLERANCE = 1e-9


def snapshot_dir(label: str):
    """Directory for one labelled snapshot.

    Args:
        label: Snapshot name, e.g. ``"before"``.

    Returns:
        Path: The directory, created if absent.
    """
    p = EQUIV_DIR / label
    p.mkdir(parents=True, exist_ok=True)
    return p


def build_league_frame(league_key_or_name: str, season: int) -> pd.DataFrame:
    """Run the full projection pipeline for one league-season.

    Mirrors what ``populateGoogleSheet.run()`` does before it writes to Sheets:
    fetch, lineups by matchup, free-agent market, then the blend.

    Args:
        league_key_or_name: Display name or config key.
        season: Season year.

    Returns:
        pd.DataFrame: ``clean_lineups`` output.
    """
    # Deferred so `compare` does not pay for ESPN imports.
    from Scripts.fetch_utils import fetch_league
    from Scripts.projection_utils import clean_lineups
    from Scripts.scrape_player_stats import build_fa_market, get_ply_stats_by_matchup

    lg_vars = build_lg_vars()
    by_key = {c["key"]: c for c in lg_vars.values()}
    cfg = lg_vars.get(league_key_or_name) or by_key.get(league_key_or_name)
    if cfg is None:
        raise ValueError(
            f"Unknown league {league_key_or_name!r}. Configured: "
            f"{sorted(lg_vars)} or {sorted(by_key)}."
        )

    league = fetch_league(
        league_id=cfg["ID"], year=season, swid=cfg["SWID"], espn_s2=cfg["ESPN_S2"],
    )
    lineups = get_ply_stats_by_matchup(
        league_id=cfg["ID"], year=season, swid=cfg["SWID"], espn_s2=cfg["ESPN_S2"],
    )
    free_agents = build_fa_market(league=league)

    df = pd.concat([lineups, free_agents])
    df.fillna(0, inplace=True)
    df = df.drop_duplicates(subset=["week", "player_name"])

    return clean_lineups(df=df, lg=league)


def take_snapshot(
    label: str, leagues: Optional[Iterable[str]] = None, season: Optional[int] = None
) -> Dict[str, str]:
    """Run the pipeline for each league and write one parquet per league.

    Args:
        label: Snapshot name.
        leagues: Display names or config keys. Defaults to every configured league.
        season: Season year. Defaults to the configured season.

    Returns:
        dict: ``{league_key: status}``, status being ``"ok"`` or an error string.
    """
    season = get_season() if season is None else season
    lg_vars = build_lg_vars()
    targets = list(lg_vars) if leagues is None else list(leagues)
    out = snapshot_dir(label)
    results = {}

    for name in targets:
        try:
            df = build_league_frame(name, season)
            safe = name.replace(" ", "_").replace("/", "_")
            df.to_parquet(out / f"{safe}_{season}.parquet")
            results[name] = "ok"
            print(f"  {name:<28} {df.shape[0]:>5} rows x {df.shape[1]:>3} cols")
        except Exception as e:                      # noqa: BLE001 - reported, not hidden
            results[name] = f"{type(e).__name__}: {e}"
            print(f"  {name:<28} FAILED  {type(e).__name__}: {e}")

    print(f"\nSnapshot '{label}' written to {out.relative_to(DATA_DIR.parent)}")
    return results


def _load(label: str) -> Dict[str, pd.DataFrame]:
    """Read every parquet in a snapshot, keyed by filename stem."""
    d = EQUIV_DIR / label
    if not d.exists():
        raise FileNotFoundError(
            f"No snapshot named {label!r} under {EQUIV_DIR}. Take one with "
            f"`python -m Scripts.equivalence snapshot --label {label}`."
        )
    return {p.stem: pd.read_parquet(p) for p in sorted(d.glob("*.parquet"))}


def _align(a: pd.DataFrame, b: pd.DataFrame):
    """Sort and index both frames on KEY_COLUMNS so rows correspond.

    Returns:
        tuple: ``(a, b, only_a, only_b)`` -- the aligned frames restricted to
        shared keys, plus the keys unique to each side.
    """
    keys = [c for c in KEY_COLUMNS if c in a.columns and c in b.columns]
    a = a.sort_values(keys).set_index(keys)
    b = b.sort_values(keys).set_index(keys)
    # Duplicate keys would make reindexing ambiguous.
    a = a[~a.index.duplicated()]
    b = b[~b.index.duplicated()]
    shared = a.index.intersection(b.index)
    return a.loc[shared], b.loc[shared], a.index.difference(b.index), b.index.difference(a.index)


def compare(label_a: str, label_b: str, top: int = 25) -> pd.DataFrame:
    """Diff two snapshots column by column.

    Args:
        label_a: Baseline snapshot.
        label_b: Snapshot to compare against the baseline.
        top: How many changed columns to print per league.

    Returns:
        pd.DataFrame: One row per (league, column) that differs, with the number
        of differing cells and the largest absolute difference. Empty when the
        two snapshots are equivalent.
    """
    A, B = _load(label_a), _load(label_b)

    missing = set(A) ^ set(B)
    if missing:
        print(f"WARNING leagues present in only one snapshot: {sorted(missing)}\n")

    rows = []
    for key in sorted(set(A) & set(B)):
        a, b, only_a, only_b = _align(A[key], B[key])
        print(f"\n=== {key} ===")
        print(f"  rows: {len(a)} shared, {len(only_a)} only in {label_a}, "
              f"{len(only_b)} only in {label_b}")

        added = sorted(set(b.columns) - set(a.columns))
        dropped = sorted(set(a.columns) - set(b.columns))
        if added:
            print(f"  columns added ({len(added)}): {added[:8]}"
                  f"{' ...' if len(added) > 8 else ''}")
        if dropped:
            print(f"  columns dropped ({len(dropped)}): {dropped[:8]}"
                  f"{' ...' if len(dropped) > 8 else ''}")

        changed = []
        for col in sorted(set(a.columns) & set(b.columns)):
            sa, sb = a[col], b[col]
            both_bool = (pd.api.types.is_bool_dtype(sa)
                         and pd.api.types.is_bool_dtype(sb))
            if both_bool:
                # is_numeric_dtype() is True for bool, which sent the *_is_imputed
                # provenance flags down the arithmetic path below -- and numpy
                # refuses `-` on booleans, so comparing any snapshot taken after
                # plan 03 raised TypeError. Count disagreements instead.
                neq = sa.fillna(False).ne(sb.fillna(False))
                n, worst = int(neq.sum()), float("nan")
            elif (pd.api.types.is_numeric_dtype(sa)
                    and pd.api.types.is_numeric_dtype(sb)):
                diff = (sa.fillna(0) - sb.fillna(0)).abs()
                n = int((diff > TOLERANCE).sum())
                worst = float(diff.max()) if len(diff) else 0.0
            else:
                neq = sa.astype(str).ne(sb.astype(str)) & ~(sa.isna() & sb.isna())
                n, worst = int(neq.sum()), float("nan")
            if n:
                changed.append((col, n, worst))
                rows.append({"league": key, "column": col,
                             "cells_changed": n, "max_abs_diff": worst})

        if not changed:
            print("  no differences")
            continue
        changed.sort(key=lambda t: (-t[1], t[0]))
        print(f"  {len(changed)} column(s) differ:")
        for col, n, worst in changed[:top]:
            w = "n/a" if worst != worst else f"{worst:.6g}"
            print(f"    {col:<44} {n:>6} cells  max|diff|={w}")
        if len(changed) > top:
            print(f"    ... and {len(changed) - top} more")

    result = pd.DataFrame(rows)
    print()
    if result.empty:
        print(f"EQUIVALENT: '{label_a}' and '{label_b}' match within {TOLERANCE}.")
    else:
        print(f"DIFFERENT: {len(result)} (league, column) pairs differ. "
              f"Inspect above and confirm each change is intended.")
    return result


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point. See ``python -m Scripts.equivalence --help``."""
    p = argparse.ArgumentParser(
        prog="python -m Scripts.equivalence",
        description="Snapshot and diff the projection pipeline's output.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    snap = sub.add_parser("snapshot", help="run the pipeline and save its output")
    snap.add_argument("--label", required=True, help="snapshot name, e.g. 'before'")
    snap.add_argument("--league", action="append", dest="leagues", metavar="NAME",
                      help="repeatable; defaults to every configured league")
    snap.add_argument("--season", type=int, help="defaults to config.yaml season")

    cmp_ = sub.add_parser("compare", help="diff two snapshots")
    cmp_.add_argument("label_a")
    cmp_.add_argument("label_b")
    cmp_.add_argument("--top", type=int, default=25,
                      help="changed columns to print per league (default 25)")

    args = p.parse_args(argv)

    if args.cmd == "snapshot":
        results = take_snapshot(args.label, args.leagues, args.season)
        return 0 if all(v == "ok" for v in results.values()) else 1

    result = compare(args.label_a, args.label_b, top=args.top)
    return 0 if result.empty else 1


if __name__ == "__main__":
    sys.exit(main())
