"""Date-partitioned odds storage, where line history is a by-product.

The design is one idea: **append only what moved.** Each pull is stamped with a
snapshot time and compared against the newest snapshot already stored for the same
line; rows whose number or price is unchanged are not written at all. What
accumulates is therefore every *distinct* state a line has been in, which is line
history without a second mechanism to maintain. Ported from
``Rebirtha/python/sportsbooks/odds_upload.py``.

Layout, one file per game date::

    Data/Odds/<season>/<book>/<officialDate>.parquet   every snapshot that moved
    Data/Odds/<season>/<book>/current.parquet          the newest state of each line

Partitioned on the *game* date rather than the scrape date, so one file holds a
game's whole history and reading a slate is one file per day rather than one per pull.

The hazard this creates is named in plan 36 and is worth repeating here, because the
mechanism makes it worse rather than better: **in an append-only store, "nothing
changed" and "nothing came back" are the same empty write.** A book that renames a
market does not error, its rows simply stop arriving, and the store looks healthy.
That is why :func:`write_snapshot` refuses an empty frame rather than treating it as
a quiet no-op, and why the puller asserts market coverage rather than row counts.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import polars as pl

from Scripts.books.schema import LINE_KEYS, MOVEMENT_COLUMNS, ODDS_SCHEMA
from Scripts.paths import DATA_DIR

#: Root of the odds store. Not under ``Data/Projections`` -- a game line is not a
#: projection of a player, and nothing in the projection tree should join to it by
#: accident.
ODDS_DIR = DATA_DIR / "Odds"


class EmptyPullError(RuntimeError):
    """A book returned no rows. Refused rather than stored as "no change"."""


def book_dir(season: int, book: str, create: bool = True) -> Path:
    """Directory for one book's snapshots in one season.

    Args:
        season: Season year.
        book: Book name, e.g. ``"Pinnacle"``.
        create: False to merely compute the path. Readers must pass False -- asking
            whether a book has data must not create a directory saying it does.

    Returns:
        Path: ``Data/Odds/<season>/<book>``.
    """
    path = ODDS_DIR / str(season) / book
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


#: Stands in for a null inside a composite key. Any string works; it only has to be
#: something no real value equals.
_NULL = "\x00"


def _key_expr(columns, alias: str) -> pl.Expr:
    """One string column standing for several, with nulls made comparable.

    A single composite key rather than a multi-column join, and not for tidiness:
    **a null never equals a null in a join.** ``sideOf`` is null on every game market
    and ``propType`` on every game line, so joining on the key columns directly
    matched nothing at all -- and because an unmatched row reads as "new line", then
    fell out of the final inner join, the store reported *nothing changed* for every
    pull. Both readings were wrong, and both looked exactly like a quiet book.
    """
    return pl.concat_str(
        [pl.col(c).cast(pl.Utf8).fill_null(_NULL) for c in columns],
        separator="|").alias(alias)


def _fingerprint(df: pl.DataFrame, keys, columns) -> pl.DataFrame:
    """One row per line: its identity, and a digest of what counts as movement."""
    present = [c for c in columns if c in df.columns]
    return df.select([_key_expr(keys, "line_id"),
                      _key_expr(present, "fingerprint")])


def detect_changes(existing: Optional[pl.DataFrame], new: pl.DataFrame,
                   keys=LINE_KEYS, columns=MOVEMENT_COLUMNS) -> pl.DataFrame:
    """The rows of *new* that are new lines or moved ones.

    Args:
        existing: Everything stored for this partition, all snapshots. None on first
            write.
        new: This pull.
        keys: What identifies a line.
        columns: What counts as the line having moved.

    Returns:
        pl.DataFrame: A subset of *new*. Empty when nothing moved, which is a normal
        and frequent outcome -- books reprice far less often than they are polled.
    """
    if existing is None or existing.is_empty():
        return new

    keys = [k for k in keys if k in new.columns and k in existing.columns]
    latest = (existing.with_columns(_key_expr(keys, "line_id"))
                      .sort("snapshot_ts", descending=True)
                      .unique(subset=["line_id"], keep="first"))

    moved = (
        _fingerprint(new, keys, columns)
        .join(_fingerprint(latest, keys, columns), on="line_id", how="left",
              suffix="_prev")
        .filter(pl.col("fingerprint_prev").is_null()
                | (pl.col("fingerprint") != pl.col("fingerprint_prev")))
        .select("line_id")
    )
    return (new.with_columns(_key_expr(keys, "line_id"))
               .join(moved, on="line_id", how="inner")
               .drop("line_id"))


def write_snapshot(df: pl.DataFrame, season: int, book: str) -> Dict[str, int]:
    """Append this pull's moved rows, one partition per game date.

    Args:
        df: Standard-schema rows from one adapter.
        season: Season year.
        book: Book name.

    Returns:
        Dict[str, int]: ``appended`` and ``unchanged`` row counts, per pull.

    Raises:
        EmptyPullError: If *df* has no rows. An empty pull and a pull where nothing
            moved produce the identical empty write, so the difference has to be
            caught here or it is lost forever.
    """
    if df.is_empty():
        raise EmptyPullError(
            f"{book} returned no rows for {season}. Refusing to store: in an "
            f"append-only store this is indistinguishable from a quiet run, and an "
            f"absent source reading as agreement is this repo's recurring failure.")

    out = book_dir(season, book)
    appended = unchanged = 0

    for (date,), partition in df.group_by(["officialDate"], maintain_order=True):
        path = out / f"{date}.parquet"
        existing = pl.read_parquet(path) if path.is_file() else None
        moved = detect_changes(existing, partition)

        if moved.is_empty():
            unchanged += partition.height
            continue
        combined = (pl.concat([existing, moved], how="diagonal_relaxed")
                    if existing is not None else moved)
        combined.write_parquet(path)
        appended += moved.height

    _write_current(season, book)
    return {"appended": appended, "unchanged": unchanged}


def _write_current(season: int, book: str) -> None:
    """Collapse every partition to the newest state of each line."""
    out = book_dir(season, book)
    parts = [p for p in sorted(out.glob("*.parquet")) if p.name != "current.parquet"]
    if not parts:
        return
    (pl.concat([pl.read_parquet(p) for p in parts], how="diagonal_relaxed")
       .with_columns(_key_expr(list(LINE_KEYS), "line_id"))
       .sort("snapshot_ts", descending=True)
       .unique(subset=["line_id"], keep="first")
       .drop("line_id")
       .write_parquet(out / "current.parquet"))


def read_current(season: int, book: str = None) -> pl.DataFrame:
    """The newest state of every line, for one book or all of them.

    Args:
        season: Season year.
        book: One book, or None for every book stored.

    Returns:
        pl.DataFrame: Standard-schema rows. Empty if nothing is stored.
    """
    root = ODDS_DIR / str(season)
    if not root.is_dir():
        return pl.DataFrame(schema=ODDS_SCHEMA)
    books = [root / book] if book else [d for d in sorted(root.iterdir()) if d.is_dir()]
    frames = [pl.read_parquet(d / "current.parquet")
              for d in books if (d / "current.parquet").is_file()]
    if not frames:
        return pl.DataFrame(schema=ODDS_SCHEMA)
    return pl.concat(frames, how="diagonal_relaxed")


def line_history(season: int, book: str, matchup: str = None,
                 market: str = None) -> pl.DataFrame:
    """Every stored state of the matching lines, oldest first.

    Args:
        season: Season year.
        book: Book name.
        matchup: Substring filter on ``matchup``.
        market: Exact ``marketTitle``.

    Returns:
        pl.DataFrame: Snapshots in time order.
    """
    out = book_dir(season, book, create=False)
    parts = [p for p in sorted(out.glob("*.parquet"))
             if p.name != "current.parquet"] if out.is_dir() else []
    if not parts:
        return pl.DataFrame(schema=ODDS_SCHEMA)
    df = pl.concat([pl.read_parquet(p) for p in parts], how="diagonal_relaxed")
    if matchup:
        df = df.filter(pl.col("matchup").str.contains(matchup))
    if market:
        df = df.filter(pl.col("marketTitle") == market)
    return df.sort("snapshot_ts")
