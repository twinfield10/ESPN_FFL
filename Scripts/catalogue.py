"""What data do we actually have? Walk every tier and say so.

    python -m Scripts.catalogue              # local disk
    python -m Scripts.catalogue --s3         # the bucket
    python -m Scripts.catalogue --both       # both, side by side

``docs/DATA_CATALOGUE.md`` is the written version of this: what each dataset *is*,
what produces it, and how the tiers join. This is the live counterpart, and it exists
because that document will drift. ``Data/`` is no longer tracked in git, so nothing
about a stale row count would show up in a diff -- the number in the doc could be a
year old and look exactly as authoritative as a correct one.

Reading is deliberately cheap: parquet row counts come from the file's metadata via
``scan_parquet``, not by loading columns, so this walks 350 files in a couple of
seconds. CSVs have to be read, which is why the two loose ones are the slow part.
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from Scripts import paths

#: Weekly scrape outputs are one file per week and there are 17 of them per source
#: per season. Listing each is noise: the useful facts are that the set exists, how
#: many weeks landed, and how wide a row is. Collapsed to a single ``Week_N`` line.
_WEEKLY = re.compile(r"_(Week|week)_\d+")

#: Tier to the directories under ``Data/`` it covers, in report order. The store is
#: listed separately because it is per league-season rather than a flat tree.
TIERS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("nflverse", ("NFL",)),
    ("projections", ("Projections",)),
    ("scoring", ("Scoring",)),
    ("injuries", ("Injuries",)),
    ("archive (G2)", ("G2",)),
)

DATA_SUFFIXES = {".parquet", ".csv", ".json"}


def _human(n: float) -> str:
    """Bytes as a short human string."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}GB"


def shape(path: Path) -> Tuple[Optional[int], Optional[int]]:
    """Rows and columns of a data file, without loading it where possible.

    Args:
        path: A ``.parquet`` or ``.csv``.

    Returns:
        tuple: ``(rows, cols)``, or ``(None, None)`` when the file cannot be read or
        is not tabular. Unreadable is reported rather than raised -- a catalogue that
        aborts on one bad file tells you less than one that flags it.
    """
    import polars as pl

    try:
        if path.suffix == ".parquet":
            lazy = pl.scan_parquet(path)
            return (lazy.select(pl.len()).collect().item(),
                    len(lazy.collect_schema().names()))
        if path.suffix == ".csv":
            frame = pl.read_csv(path, infer_schema_length=200)
            return frame.height, frame.width
    except Exception:                                       # noqa: BLE001
        return None, None
    return None, None


# --- local ----------------------------------------------------------------

def _store_report(detail: bool) -> List[str]:
    """The per-league-season store."""
    from Scripts import store

    lines = ["", "STORE  Data/Store/<season>/<league>/  ->  s3://.../store/season=/league=/"]
    seasons = store.list_seasons()
    if not seasons:
        return lines + ["  (none -- run `python -m Scripts.refresh --all`)"]

    for season in seasons:
        leagues = store.list_leagues(season)
        lines.append(f"  season {season}: {len(leagues)} leagues")
        for name, filename in store.ARTIFACTS.items():
            present, rows, cols, total = 0, [], 0, 0
            for league in leagues:
                path = paths.store_dir(season, league) / filename
                if not path.is_file():
                    continue
                present += 1
                total += path.stat().st_size
                r, c = shape(path)
                if r is not None:
                    rows.append(r)
                    cols = max(cols, c or 0)
            if not present:
                continue
            span = (f"{min(rows)}-{max(rows)}" if rows and min(rows) != max(rows)
                    else str(rows[0]) if rows else "?")
            lines.append(f"    {name:<12} {present}/{len(leagues)} leagues  "
                         f"rows {span:<12} x {cols:>4} cols  "
                         f"{sum(rows):>7} total  {_human(total):>8}")
        missing = [n for n in store.ARTIFACTS
                   if not any((paths.store_dir(season, lg) / store.ARTIFACTS[n]).is_file()
                              for lg in leagues)]
        if missing:
            lines.append(f"    not built:   {', '.join(missing)}")
    return lines


def _tree_report(label: str, directories: Tuple[str, ...], detail: bool) -> List[str]:
    """A flat mirrored tier, grouped by filename across seasons."""
    lines = [""]
    groups: Dict[str, List[Path]] = {}
    loose: List[Path] = []

    for directory in directories:
        root = paths.DATA_DIR / directory
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*")):
            if (not path.is_file() or path.suffix not in DATA_SUFFIXES
                    or path.name.startswith(".")):
                continue
            # A bare four-digit component means the file is season-scoped, and the
            # interesting fact is the span rather than each year's row count.
            season = next((p for p in path.relative_to(root).parts
                           if p.isdigit() and len(p) == 4), None)
            name = _WEEKLY.sub(r"_\1_N", path.name)
            (groups.setdefault(name, []) if season else loose).append(path)

    total_files = sum(len(v) for v in groups.values()) + len(loose)
    total_bytes = sum(p.stat().st_size
                      for v in list(groups.values()) + [loose] for p in v)
    lines.append(f"{label.upper()}  {', '.join('Data/' + d for d in directories)}"
                 f"   [{total_files} files, {_human(total_bytes)}]")
    if not total_files:
        return lines + ["  (none)"]

    for name, found in sorted(groups.items()):
        years = sorted({p_ for path in found
                        for p_ in path.parts if p_.isdigit() and len(p_) == 4})
        newest = max(found, key=lambda p: p.stat().st_mtime)
        rows, cols = shape(newest)
        span = f"{years[0]}-{years[-1]}" if len(years) > 1 else (years[0] if years else "")
        size = _human(sum(p.stat().st_size for p in found))
        shown = f"{rows:>7} x {cols:>4}" if rows is not None else "      -      "
        lines.append(f"  {name:<38} {span:<10} n={len(found):>2}  "
                     f"newest {shown} cols  {size:>8}")

    for path in sorted(loose):
        rows, cols = shape(path)
        shown = f"{rows:>7} x {cols:>4}" if rows is not None else "      -      "
        rel = path.relative_to(paths.DATA_DIR)
        lines.append(f"  {str(rel):<38} {'':<10} {'':>5}  "
                     f"       {shown} cols  {_human(path.stat().st_size):>8}")
    return lines


def local_report(detail: bool = False) -> List[str]:
    """Everything on local disk, tier by tier."""
    lines = [f"LOCAL  {paths.DATA_DIR}"]
    lines += _store_report(detail)
    for label, directories in TIERS:
        lines += _tree_report(label, directories, detail)

    loose = [paths.DATA_DIR / n for n in
             ("NFL_Schedules.csv", "NFL_Tackles_By_Position.csv")]
    present = [p for p in loose if p.is_file()]
    if present:
        lines += ["", "LOOSE  Data/*.csv"]
        for path in present:
            rows, cols = shape(path)
            lines.append(f"  {path.name:<38} {'':<10} {'':>5}  "
                         f"       {rows:>7} x {cols:>4} cols  "
                         f"{_human(path.stat().st_size):>8}")
    return lines


# --- S3 -------------------------------------------------------------------

def s3_report() -> List[str]:
    """Everything in the bucket, by top-level prefix.

    Deliberately does not download anything: the point of this half is "what is in
    the record", and row counts would mean pulling 77 MB to learn what the local
    half already reports.
    """
    from Scripts import s3_store

    lines = [f"S3  s3://{s3_store.BUCKET} ({s3_store.REGION})"]
    try:
        objects = s3_store.list_objects("")
    except Exception as e:                                  # noqa: BLE001
        return lines + [f"  unreachable: {type(e).__name__}: {e}"]

    if not objects:
        return lines + ["  (empty -- run `python -m Scripts.sync --push`)"]

    tiers: Dict[str, List[int]] = {}
    for key, meta in objects.items():
        tiers.setdefault(key.split("/")[0], []).append(int(meta["size"]))
    for tier, sizes in sorted(tiers.items(), key=lambda kv: -len(kv[1])):
        lines.append(f"  {tier:<14} {len(sizes):>4} objects  {_human(sum(sizes)):>9}")
    lines.append(f"  {'TOTAL':<14} {len(objects):>4} objects  "
                 f"{_human(sum(sum(s) for s in tiers.values())):>9}")

    # Snapshots are a time series, so the interesting fact is which dates exist.
    dates = sorted({part.split("=", 1)[1] for key in objects
                    for part in key.split("/") if part.startswith("date=")})
    if dates:
        span = f"{dates[0]} to {dates[-1]}" if len(dates) > 1 else dates[0]
        lines.append(f"\n  board snapshots: {len(dates)} date(s), {span}")
    return lines


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point. See ``python -m Scripts.catalogue --help``."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.catalogue",
        description="Inventory the data: what exists, how much of it, and where.")
    where = parser.add_mutually_exclusive_group()
    where.add_argument("--s3", action="store_true", help="the bucket instead of disk")
    where.add_argument("--both", action="store_true", help="disk and bucket")
    parser.add_argument("--detail", action="store_true",
                        help="reserved for per-season breakdowns")
    args = parser.parse_args(argv)

    lines: List[str] = []
    if not args.s3:
        lines += local_report(args.detail)
    if args.s3 or args.both:
        if lines:
            lines.append("")
        lines += s3_report()

    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
