"""Pull every configured book and store what moved.

Usage::

    python -m Scripts.books.pull
    python -m Scripts.books.pull --season 2026 --dry-run
    python -m Scripts.books.pull --history "Buffalo" --market Total

The coverage assertion is the part worth reading. A book renaming a market does not
error -- its rows stop arriving and everything downstream carries on with one fewer
market, which in an append-only store looks exactly like a quiet day. So a pull that
comes back missing a market family it is supposed to price fails the run, and a pull
that comes back with nothing at all fails harder.

What is deliberately *not* asserted is a game count. Pinnacle prices roughly the
upcoming week and sometimes posts lookahead lines, so the number of games moves for
ordinary reasons; gating on it would fail the run every Tuesday. Coverage is a
property of each game returned, which is the thing that actually breaks.
"""

import argparse
import sys
from typing import Dict, List

import polars as pl

from Scripts.books.pinnacle import PinnacleSportsbook
from Scripts.books.schema import MARKET_TITLES, PAIR_KEYS
from Scripts.books.store import EmptyPullError, line_history, write_snapshot
from Scripts.nfl_utils import current_season

#: The adapters this repo pulls, and the markets each is expected to price.
#:
#: Expectations per book rather than one global list: a book that does not post team
#: totals is not broken, it is a different book, and a shared list would either fail
#: on it or stop protecting the books that do.
BOOKS = {
    "Pinnacle": {
        "adapter": PinnacleSportsbook,
        "expect_markets": set(MARKET_TITLES),
    },
}


class CoverageError(RuntimeError):
    """A book answered, but not with the markets it is supposed to price."""


def assert_coverage(df: pl.DataFrame, book: str, expected: set) -> None:
    """Fail loudly when a market family stops arriving.

    Args:
        df: The book's standardised rows.
        book: Book name, for the message.
        expected: Market titles this book should price.

    Raises:
        CoverageError: If any expected market is absent.
    """
    found = set(df["marketTitle"].unique().to_list())
    missing = expected - found
    if missing:
        raise CoverageError(
            f"{book} returned {df.height} rows but no {sorted(missing)}. A book that "
            f"renames a market does not error -- its rows just stop arriving -- so "
            f"this is checked rather than assumed. Found: {sorted(found)}.")


def assert_devigged(df: pl.DataFrame, book: str) -> None:
    """Every two-sided market must de-vig to one.

    Cheap, and it has already caught two real defects: a team total whose four prices
    collapsed into one group, and a spread pairing against the wrong alternate.

    Raises:
        CoverageError: If a paired market's fair probabilities do not sum to 1.
    """
    paired = (df.group_by(list(PAIR_KEYS))
                .agg(pl.col("fairProb").sum().alias("s"), pl.len().alias("n"))
                .filter(pl.col("n") == 2))
    if paired.is_empty():
        return
    off = paired.filter((pl.col("s") - 1.0).abs() > 1e-6)
    if not off.is_empty():
        raise CoverageError(
            f"{book}: {off.height} market(s) whose de-vigged sides do not sum to 1. "
            f"That means two rows were paired that are not each other's opposite.")


def pull(season: int = None, write: bool = True,
         books: List[str] = None) -> Dict[str, pl.DataFrame]:
    """Pull each book, check it, and store what moved.

    Args:
        season: Season year. None derives it.
        write: False pulls and checks without touching disk.
        books: Subset of :data:`BOOKS` to pull. None does all.

    Returns:
        Dict[str, pl.DataFrame]: Each book's standardised rows.
    """
    season = current_season() if season is None else season
    wanted = books or list(BOOKS)
    results = {}

    for name in wanted:
        spec = BOOKS[name]
        adapter = spec["adapter"](season=season)
        views = adapter.get_df_dict()
        df = views["All_Bets"]

        if df.is_empty():
            raise EmptyPullError(
                f"{name} returned no rows ({adapter.last_failure.value}). Failing the "
                f"run rather than storing nothing, because a stored nothing is "
                f"indistinguishable from a quiet day.")

        assert_coverage(df, name, spec["expect_markets"])
        assert_devigged(df, name)
        results[name] = df

        by_market = (df.group_by("marketTitle").agg(pl.len().alias("n"))
                       .sort("marketTitle"))
        print(f"  {name}: " + ", ".join(
            f"{r['marketTitle']} {r['n']}" for r in by_market.iter_rows(named=True)))

        if write:
            stats = write_snapshot(df, season, name)
            print(f"  {name}: {stats['appended']} rows appended, "
                  f"{stats['unchanged']} unchanged since the last pull")
        else:
            print(f"  {name}: --dry-run, nothing written")

    return results


def main(argv=None) -> int:
    """Command-line entry point."""
    p = argparse.ArgumentParser(
        prog="python -m Scripts.books.pull",
        description="Pull sportsbook game lines and store what moved.")
    p.add_argument("--season", type=int, help="defaults to the schedule's season")
    p.add_argument("--book", action="append", dest="books",
                   choices=sorted(BOOKS), help="repeatable; defaults to all")
    p.add_argument("--dry-run", action="store_true", help="do not write")
    p.add_argument("--history", metavar="MATCHUP",
                   help="print stored line history for matchups containing this")
    p.add_argument("--market", help="with --history, restrict to one market")
    args = p.parse_args(argv)

    season = args.season or current_season()

    if args.history:
        for name in (args.books or list(BOOKS)):
            hist = line_history(season, name, matchup=args.history,
                                market=args.market)
            print(f"\n{name}: {hist.height} stored snapshots")
            if not hist.is_empty():
                with pl.Config(tbl_rows=60):
                    print(hist.select("snapshot_ts", "matchup", "marketTitle",
                                      "sideOf", "betSide", "marketLine", "price"))
        return 0

    try:
        pull(season=season, write=not args.dry_run, books=args.books)
    except (EmptyPullError, CoverageError) as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
