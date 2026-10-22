"""Weekly player props from the sportsbook engine, in this repo's raw shape (plan 54).

    python -m Scripts.books.engine                    # this week, as of now, into the shadow
    python -m Scripts.books.engine --week 7 --at 2026-10-21T16:06Z
    python -m Scripts.books.engine --compare          # shadow vs our own scrape, per book and stat
    python -m Scripts.books.engine --agreement        # each book's de-vigged prices vs Novig's

**What it replaces.** ``Scripts.books.weekly_props`` and ``Scripts.scrape_BOL`` fetch
each book and turn its payload into :data:`~Scripts.books.weekly_props.RAW_SCHEMA`
rows. ``~/GitRepos/SportsbookScrapers`` already collects the same books on the same
machine and publishes every price change to ``s3://tmw-sportsbook-data``
(``docs/consumer-contract.md`` there). This module produces the same raw rows from
that bucket, so :func:`~Scripts.books.weekly_props.project_book` and everything after
it run unchanged.

**Phase 1 is shadow only.** Output goes under ``Data/Engine/Shadow/``, which
nothing in the blend reads and ``Scripts.sync`` does not mirror. Plan 54's gates
(G1 coverage, G2 stat lines, G3 board) are measured from it with ``--compare``
before anything switches.

**Reading a changes-only table needs two things the contract's reader skips:**

* **An as-of join.** A row is written when a price *changes*, so the price at ``t``
  is each series' last row at or before ``t``, which may be days old and still
  current.
* **The listing spells.** Nothing closes a series when a book takes it down, so the
  as-of board alone resurrects every line that ever moved. On week 6's last
  pre-kickoff board, 7% of FanDuel's series and 12.5% of Novig's had been delisted,
  and 137 FanDuel player-stats showed two main lines instead of one. A series is on
  the board only if one of its spells covers ``t``. Closed spells are in
  ``spells/``, the open ones in ``state/<book>/nfl/spells.parquet``. **Neither path
  is in the consumer contract yet**, which is why they are named once, here.

**Two engine conventions are converted, nothing else:**

* Ladders: the engine stores "k+" as over ``k - 0.5``; this repo's ``value`` is ``k``
  (``floor(line) + 1``, exactly what the Novig and 4Casters adapters compute).
* Prices: American odds, vig or exchange commission included, become decimal
  ``odds`` and ``impProb = 1 / odds``.

**Which rows become the line and which the ladder** follows each adapter it
replaces. ``is_alternate`` is false on every engine prop row (2026-10-21), so the
main line is found from the prices: the most balanced two-sided rung, as
``novig_props.normalize`` does. Once delisted series are dropped, FanDuel has
exactly one two-sided rung per player-stat, so the rule reproduces its main line.
FanDuel's ladder is its one-sided rungs only, as its adapter reads alt markets
only. Every other book's ladder is the over side of every rung. The anytime
touchdown is always a ladder, never a line.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import polars as pl

from Scripts import market as mk
from Scripts.books.weekly_props import RAW_SCHEMA, project_book, raw_frame, reconcile
from Scripts.paths import DATA_DIR, season_dir

logger = logging.getLogger(__name__)

BUCKET = os.environ.get("SPORTSBOOK_BUCKET", "tmw-sportsbook-data")
REGION = os.environ.get("SPORTSBOOK_REGION", "us-east-2")

#: The contract major version this reader understands (``lines/_contract.json``).
CONTRACT_MAJOR = "1"

#: Local copy of the partitions read, keyed exactly as in the bucket.
CACHE_DIR = DATA_DIR / "Engine" / "cache"

#: Where the shadow writes. Read by ``--compare`` and nothing else.
SHADOW_DIR = DATA_DIR / "Engine" / "Shadow"
STATUS_PATH = SHADOW_DIR / "engine_shadow.json"

#: Days of transitions read before ``t``. A week's props are posted from the Monday
#: before, so ten days always reaches back past the first price of every series.
LOOKBACK_DAYS = 10

#: A book whose last poll is older than this at ``t`` is withheld, not read. A
#: changes-only table never goes empty, so a dead collector looks like a quiet market.
MAX_AGE_HOURS = 12.0

#: The engine's identity of a price series (contract: "A series is ...").
SERIES_KEY = ["book", "source_event_id", "market_uid", "side"]

#: Engine market -> ESPN stat. Only what ``weekly_props.OFFENSIVE_STATS`` projects.
STAT_MAP: Dict[str, str] = {
    "touchdowns": "anytimeTouchdown",
    "passing_yards": "passingYards",
    "passing_attempts": "passingAttempts",
    "completions": "passingCompletions",
    "passing_tds": "passingTouchdowns",
    "interceptions": "passingInterceptions",
    "rushing_yards": "rushingYards",
    "rushing_attempts": "rushingAttempts",
    "receiving_yards": "receivingYards",
    "receptions": "receivingReceptions",
}


@dataclass(frozen=True)
class Book:
    """One engine book, under the name, directory and prefix this repo uses for it.

    ``ladder`` is ``"one_sided"`` where the adapter it replaces read alt markets only
    (FanDuel), and ``"every_rung"`` where it read the over side of every rung.
    ``pooled`` is False for a book that is shadowed but not yet in any pool (plan 54,
    decision 3).
    """

    engine: str
    slug: str
    name: str
    dir: str
    prefix: str
    ladder: str = "every_rung"
    pooled: bool = True


BOOKS: List[Book] = [
    Book("FanDuel", "fanduel", "FanDuel", "FanDuel", "FD", ladder="one_sided"),
    Book("Novig", "novig", "Novig", "Novig", "NV"),
    Book("4Casters", "4casters", "4Casters", "FourCasters", "C4"),
    # BetOnline and LowVig are one book behind two skins; its props come from the same
    # DST widget Scripts/scrape_BOL.py walks (SportsbookScrapers docs/books/lowvig_dst.md).
    Book("LowVig", "lowvig", "BetOnline", "BetOnline", "BOL"),
    Book("Bovada", "bovada", "Bovada", "Bovada", "BOV", pooled=False),
    Book("Kalshi", "kalshi", "Kalshi", "Kalshi", "KAL", pooled=False),
    Book("Polymarket", "polymarket", "Polymarket", "Polymarket", "PM", pooled=False),
    # Shadowed 2026-10-22 as Pinnacle's replacement, so the square pool stops depending
    # on a Selenium scrape of a Chrome install Homebrew can quarantine. On that day's
    # week-7 board its derived stat lines matched our Pinnacle scrape to a median 0.02%
    # on every shared stat, 163 of 166 players in both. `--compare` measures it against
    # that scrape, not against a Bet105 scrape of ours -- there is none.
    Book("Bet105", "bet105", "Bet105", "Bet105", "B105", pooled=False),
]

#: The fair price every other book is measured against (plan 54, decision 3).
REFERENCE_BOOK = "Novig"


class EngineError(RuntimeError):
    """The bucket could not be read, or speaks a contract this reader does not."""


# --- the bucket ----------------------------------------------------------------------

def _client():
    """A boto3 S3 client for the engine's bucket."""
    import boto3
    from botocore.config import Config

    return boto3.client("s3", region_name=REGION,
                        config=Config(connect_timeout=10, read_timeout=60,
                                      retries={"max_attempts": 5, "mode": "standard"}))


def check_contract(client) -> str:
    """The contract version, after checking its major version is one this reader knows.

    Raises:
        EngineError: On a different major version, which the contract reserves for
            renames, removals and changed meanings.
    """
    body = client.get_object(Bucket=BUCKET, Key="lines/_contract.json")["Body"].read()
    version = str(json.loads(body)["version"])
    if version.split(".")[0] != CONTRACT_MAJOR:
        raise EngineError(f"lines/ contract is {version}; this reader understands "
                          f"{CONTRACT_MAJOR}.x -- see SportsbookScrapers' change log")
    return version


def sync_prefix(client, prefix: str, cache: Path = CACHE_DIR) -> List[Path]:
    """Mirror the parquet under ``prefix`` into ``cache``, downloading only what changed.

    A file is re-fetched when its size or modification time differs from the
    bucket's. Today's partition is rewritten by every hourly publish, so this is
    what keeps it current without re-reading last week's.
    """
    paths: List[Path] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            local = cache / obj["Key"]
            stamp = obj["LastModified"].timestamp()
            fresh = (local.exists() and local.stat().st_size == obj["Size"]
                     and abs(local.stat().st_mtime - stamp) < 1)
            if not fresh:
                local.parent.mkdir(parents=True, exist_ok=True)
                client.download_file(BUCKET, obj["Key"], str(local))
                os.utime(local, (stamp, stamp))
            paths.append(local)
    return paths


def _read(paths: Sequence[Path]) -> pl.DataFrame:
    frames = [pl.read_parquet(p) for p in paths]
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _days(t: datetime, lookback: int, through: Optional[date] = None) -> List[date]:
    last = through or t.date()
    first = t.date() - timedelta(days=lookback)
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def load_props(client, season: int, t: datetime, lookback: int = LOOKBACK_DAYS) -> pl.DataFrame:
    """Every NFL prop transition captured in the ``lookback`` days up to ``t``."""
    paths: List[Path] = []
    for day in _days(t, lookback):
        paths += sync_prefix(client, f"lines/NFL/props/season={season}/date={day:%Y-%m-%d}/")
    return _read(paths)


def load_spells(client, t: datetime, books: Sequence[Book] = BOOKS,
                lookback: int = LOOKBACK_DAYS) -> pl.DataFrame:
    """Every NFL listing spell that could cover ``t``: closed ones, then the open ones.

    Closed spells are filed under the date they closed, which can be after ``t``,
    so the read runs through today rather than stopping at ``t``.
    """
    today = datetime.now(timezone.utc).date()
    paths: List[Path] = []
    for book in books:
        for day in _days(t, lookback, through=max(today, t.date())):
            paths += sync_prefix(client, f"spells/book={book.slug}/sport=nfl/date={day:%Y-%m-%d}/")
        paths += sync_prefix(client, f"state/{book.slug}/nfl/spells.parquet")
    return _read(paths)


# --- the board -----------------------------------------------------------------------

def listed_board(props: pl.DataFrame, spells: pl.DataFrame, t: datetime) -> pl.DataFrame:
    """The pregame board at ``t``: each series' last price, if it was still listed.

    Args:
        props: Engine prop transitions (``lines/NFL/props``).
        spells: Listing spells, closed and open, for the same books.
        t: The moment to read, UTC.

    Returns:
        pl.DataFrame: One row per listed series, full-game markets of games not yet
        started, not in-play, with the covering spell's ``last_seen`` -- the last poll
        that actually saw the series, which :func:`series_age_hours` reads.
    """
    if props.is_empty():
        return props
    board = (props.filter(pl.col("captured_at") <= t, pl.col("start_time") > t,
                          pl.col("period") == "GAME", ~pl.col("in_play"))
                  .sort("captured_at")
                  .unique(subset=SERIES_KEY, keep="last", maintain_order=True))
    live = (spells.filter(pl.col("started_at") <= t,
                          pl.col("ended_at").is_null() | (pl.col("ended_at") > t))
                  .group_by(SERIES_KEY).agg(pl.col("last_seen").max()))
    return board.join(live, on=SERIES_KEY, how="inner")


def book_age_hours(spells: pl.DataFrame, t: datetime) -> Dict[str, float]:
    """Hours from each book's last poll to ``t``, read off its spells' ``last_seen``."""
    if spells.is_empty():
        return {}
    last = (spells.filter(pl.col("last_seen") <= t)
                  .group_by("book").agg(pl.col("last_seen").max()))
    return {b: (t - seen).total_seconds() / 3600.0 for b, seen in last.iter_rows()}


def series_age_hours(board: pl.DataFrame, t: datetime) -> Dict[str, Dict[str, float]]:
    """How stale each book's projected props are at ``t``, series by series.

    :func:`book_age_hours` answers "is the collector alive" and is the gate. It cannot
    answer "how old are the prices", because one book can have two collectors: LowVig's
    hourly sportsbook poll keeps its newest ``last_seen`` minutes old while its ladders
    come from the DST widget, walked four times a day, and both are ``player_prop``.
    Each listed series' own ``last_seen`` can: a ladder last seen at the 07:52 walk is
    four hours old at noon whatever else the book was polled for. A series seen after
    ``t`` (a backtest reading the past) counts as zero.

    Returns:
        dict: book -> ``{"p50": h, "p90": h, "max": h}`` over its listed series in
        :data:`STAT_MAP` markets.
    """
    rows = board.filter(pl.col("market").is_in(list(STAT_MAP)))
    if rows.is_empty() or "last_seen" not in rows.columns:
        return {}
    hours = ((pl.lit(t) - pl.min_horizontal(pl.col("last_seen"), pl.lit(t)))
             .dt.total_seconds() / 3600.0)
    ages = rows.group_by("book").agg(hours.quantile(0.5).alias("p50"),
                                     hours.quantile(0.9).alias("p90"),
                                     hours.max().alias("max"))
    return {b: {"p50": round(p50, 2), "p90": round(p90, 2), "max": round(mx, 2)}
            for b, p50, p90, mx in ages.iter_rows()}


def _decimal(price: pl.Expr) -> pl.Expr:
    """American odds to decimal, as :func:`~Scripts.books.weekly_props.american_to_decimal`."""
    return pl.when(price > 0).then(1.0 + price / 100.0).otherwise(1.0 + 100.0 / price.abs())


def to_raw(board: pl.DataFrame, book: Book) -> pl.DataFrame:
    """One book's listed board as :data:`~Scripts.books.weekly_props.RAW_SCHEMA` rows.

    ``week`` is left null for the caller; ``position`` is left null for
    :func:`~Scripts.books.weekly_props.assign_positions`, as every adapter but
    BetOnline's leaves it.
    """
    rows = board.filter(pl.col("book") == book.engine,
                        pl.col("market").is_in(list(STAT_MAP)),
                        pl.col("price").is_not_null(), pl.col("price") != 0,
                        pl.col("line").is_not_null())
    if rows.is_empty():
        return raw_frame([])
    rows = rows.with_columns(
        pl.col("market").replace_strict(STAT_MAP).alias("espn_stat"),
        _decimal(pl.col("price")).alias("odds"),
        pl.col("game_id").alias("NFL_game_id"),
        pl.col("player_team_id").alias("team"),
    ).with_columns((1.0 / pl.col("odds")).alias("impProb"))
    who = ["NFL_game_id", "player_key", "espn_stat"]
    # One spelling per player-game, so a label that varies between series cannot
    # split a player in two at the blend's name join.
    names = rows.group_by(who[:2]).agg(pl.col("player_label").first().alias("player_name"),
                                      pl.col("team").drop_nulls().first().alias("team"))
    rows = rows.drop("team").join(names, on=who[:2], how="left")

    overs = rows.filter(pl.col("side").is_in(["over", "yes"]))
    unders = rows.filter(pl.col("side").is_in(["under", "no"]))
    if book.ladder == "one_sided":
        overs = overs.join(unders.select(who + ["line"]), on=who + ["line"], how="anti")
    ladder = (overs.with_columns((pl.col("line").floor() + 1.0).alias("value"),
                                 pl.lit("Over").alias("type"),
                                 pl.lit("Values").alias("prop_source"))
                   .sort("side")  # "over" before "yes" where a book lists both
                   .unique(subset=who + ["value"], keep="first", maintain_order=True))

    paired = (rows.filter(pl.col("side") == "over", pl.col("espn_stat") != "anytimeTouchdown")
                  .join(rows.filter(pl.col("side") == "under")
                            .select(who + ["line", pl.col("odds").alias("odds_u"),
                                           pl.col("impProb").alias("imp_u")]),
                        on=who + ["line"], how="inner")
                  .with_columns((pl.col("impProb") / (pl.col("impProb") + pl.col("imp_u"))
                                 - 0.5).abs().alias("_tilt"))
                  .sort("_tilt")
                  .unique(subset=who, keep="first", maintain_order=True))
    line_over = paired.with_columns(pl.col("line").alias("value"), pl.lit("Over").alias("type"),
                                    pl.lit("OverUnder").alias("prop_source"))
    line_under = paired.with_columns(pl.col("line").alias("value"), pl.lit("Under").alias("type"),
                                     pl.col("odds_u").alias("odds"), pl.col("imp_u").alias("impProb"),
                                     pl.lit("OverUnder").alias("prop_source"))

    out = [f.with_columns(pl.lit(book.name).alias("book"),
                          pl.lit(None, dtype=pl.Int64).alias("week"),
                          pl.lit(None, dtype=pl.Utf8).alias("position"))
            .select(list(RAW_SCHEMA)).cast(RAW_SCHEMA)
           for f in (ladder, line_over, line_under) if not f.is_empty()]
    return pl.concat(out) if out else raw_frame([])


def agreement(board: pl.DataFrame, reference: str = REFERENCE_BOOK) -> pl.DataFrame:
    """How closely each book's de-vigged two-way prices track the reference's, per stat.

    Plan 54's decision 3: the new books join a pool by measurement, not by type. On
    every rung both the book and the reference price on both sides, each pair is
    de-vigged proportionally and the over probabilities are compared.

    Returns:
        pl.DataFrame: ``book``, ``espn_stat``, ``shared`` (rungs), ``mae`` (mean
        absolute gap in fair over-probability) and ``bias`` (book minus reference).
    """
    rows = board.filter(pl.col("market").is_in(list(STAT_MAP)), pl.col("market") != "touchdowns",
                        pl.col("price").is_not_null(), pl.col("price") != 0)
    if rows.is_empty():
        return pl.DataFrame(schema={"book": pl.Utf8, "espn_stat": pl.Utf8, "shared": pl.UInt32,
                                    "mae": pl.Float64, "bias": pl.Float64})
    rows = rows.with_columns((1.0 / _decimal(pl.col("price"))).alias("p"))
    key = ["book", "game_id", "player_key", "market", "line"]
    fair = (rows.filter(pl.col("side") == "over").select(key + [pl.col("p").alias("po")])
                .join(rows.filter(pl.col("side") == "under").select(key + [pl.col("p").alias("pu")]),
                      on=key, how="inner")
                .with_columns((pl.col("po") / (pl.col("po") + pl.col("pu"))).alias("fair")))
    ref = fair.filter(pl.col("book") == reference).select(key[1:] + [pl.col("fair").alias("ref")])
    gap = (fair.filter(pl.col("book") != reference).join(ref, on=key[1:], how="inner")
               .with_columns((pl.col("fair") - pl.col("ref")).alias("d"),
                             pl.col("market").replace_strict(STAT_MAP).alias("espn_stat")))
    return (gap.group_by("book", "espn_stat")
               .agg(pl.len().alias("shared"), pl.col("d").abs().mean().alias("mae"),
                    pl.col("d").mean().alias("bias"))
               .sort("book", "espn_stat"))


# --- the shadow ------------------------------------------------------------------------

def shadow_file(book: Book, season: int) -> Path:
    """The shadow's season file for ``book``, the twin of ``weekly_props.season_file``."""
    return SHADOW_DIR / book.dir / "Season" / str(season) / f"{book.dir}_AllProps.parquet"


def _stamp(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def run(season: int, week: int, t: Optional[datetime] = None, *, write: bool = True,
        model=None, client=None, props: Optional[pl.DataFrame] = None,
        spells: Optional[pl.DataFrame] = None, sched: Optional[pl.DataFrame] = None) -> Dict:
    """Read the engine's board at ``t`` and project every book into the shadow.

    Args:
        season: Season year.
        week: NFL week.
        t: The moment to read, UTC. Defaults to now.
        write: False derives without touching disk.
        model: Loaded market model; None loads it.
        client, props, spells, sched: Injected for tests; read from the bucket and
            the schedule when None.

    Returns:
        dict: The run's status -- per book prices, players, age and outcome, plus the
        agreement table -- as written to :data:`STATUS_PATH`.
    """
    t = (t or datetime.now(timezone.utc)).astimezone(timezone.utc)
    contract = None
    if props is None or spells is None:
        client = client or _client()
        contract = check_contract(client)
        props = load_props(client, season, t) if props is None else props
        spells = load_spells(client, t) if spells is None else spells
    if sched is None:
        from Scripts.scrape_BOL import slim_schedule
        sched = slim_schedule().filter(pl.col("week") == week)
    model = mk.load_model() if model is None else model

    board = listed_board(props, spells, t)
    if not board.is_empty():
        board = board.filter(pl.col("game_id").is_in(sched["NFL_game_id"].to_list()))
    ages = book_age_hours(spells, t)
    series_ages = series_age_hours(board, t)
    status: Dict = {"at": t.isoformat(timespec="seconds"), "season": season, "week": week,
                    "contract": contract, "board_series": board.height, "books": {}}

    for book in BOOKS:
        age = ages.get(book.engine)
        entry = {"age_hours": None if age is None else round(age, 2),
                 "series_age_hours": series_ages.get(book.engine), "pooled": book.pooled}
        status["books"][book.name] = entry
        if age is None or age > MAX_AGE_HOURS:
            entry["outcome"] = "withheld"
            print(f"  {book.name:<11} withheld: last poll "
                  f"{'never' if age is None else f'{age:.1f}h'} before {t:%m-%d %H:%M}Z")
            continue
        raw = to_raw(board, book).with_columns(pl.lit(week, dtype=pl.Int64).alias("week"))
        entry.update(prices=raw.height,
                     players=raw["player_name"].n_unique() if raw.height else 0)
        if raw.is_empty():
            entry["outcome"] = "empty"
            print(f"  {book.name:<11} nothing listed for week {week}")
            continue
        projected = project_book(raw, sched, model=model, td_hold=mk.TD_OVERROUND.get(book.name))
        entry["outcome"] = "projected"
        sa = entry["series_age_hours"] or {}
        print(f"  {book.name:<11} {raw.height:>6} prices, {entry['players']:>4} players, "
              f"last poll {age:.1f}h before t, prices {sa.get('p50', 0):.1f}h old "
              f"(p90 {sa.get('p90', 0):.1f}h)")
        if write:
            path = shadow_file(book, season)
            raw_path = path.parent / "Raw" / f"{book.dir}_Raw_Week_{week}_{_stamp(t)}.parquet"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw.write_parquet(raw_path)
            reconcile(book.dir, projected, season, path=path)

    status["agreement"] = agreement(board).to_dicts()
    if write:
        _write_status(status)
    return status


def _write_status(status: Dict, path: Path = STATUS_PATH, keep: int = 60) -> None:
    """Keep the last ``keep`` runs; the schedule discussion needs the ages as a series."""
    path.parent.mkdir(parents=True, exist_ok=True)
    runs = json.loads(path.read_text())["runs"] if path.exists() else []
    path.write_text(json.dumps({"runs": (runs + [status])[-keep:]}, indent=1, default=str) + "\n")


# --- the comparison (plan 54's G1 and G2) -------------------------------------------

def _name_key(col: str) -> pl.Expr:
    return (pl.col(col).str.to_lowercase().str.replace_all(r"[^a-z]", "")
              .str.replace(r"(jr|sr|ii|iii|iv|v)$", ""))


def compare(season: int, week: int, books: Sequence[Book] = BOOKS) -> pl.DataFrame:
    """The shadow against our own scrape, per book and projected stat, for one week.

    ``coverage`` is the share of our scrape's player-stats the shadow also has (G1).
    ``rel_gap`` is the median of ``|shadow - ours|`` over the mean of ours, on the
    player-stats both have (G2, before the 100-point population filter is applied).
    """
    from Scripts.books.weekly_props import season_file
    from Scripts.projection_utils import betonline_parquet

    out = []
    for book in books:
        if book.name == "BetOnline":
            ours_path = Path(betonline_parquet(season))
        elif book.name == "Bet105":
            # The scrape it replaces, which writes under its own file name.
            ours_path = season_dir("Pinnacle", season, "Pinnacle_Props_Week_All.parquet",
                                   create=False)
        else:
            ours_path = season_file(book.dir, season, create=False)
        theirs_path = shadow_file(book, season)
        if not ours_path.exists() or not theirs_path.exists():
            continue
        ours = pl.read_parquet(ours_path).filter(pl.col("week") == week)
        theirs = pl.read_parquet(theirs_path).filter(pl.col("week") == week)
        stats = sorted({c for c in ours.columns if c.startswith("proj_") and not c.endswith("_sd")}
                       & {c for c in theirs.columns if c.startswith("proj_") and not c.endswith("_sd")})
        joined = (ours.with_columns(_name_key("player_name").alias("k"))
                      .select(["k"] + stats)
                      .unique("k", keep="first")
                      .join(theirs.with_columns(_name_key("player_name").alias("k"))
                                  .select(["k"] + stats).unique("k", keep="first"),
                            on="k", how="full", suffix="_eng", coalesce=True))
        for stat in stats:
            a, b = pl.col(stat), pl.col(f"{stat}_eng")
            both = joined.filter(a.is_not_null() & (a != 0) & b.is_not_null())
            n_ours = joined.filter(a.is_not_null() & (a != 0)).height
            mean = both[stat].mean() if both.height else None
            out.append({"book": book.name, "stat": stat.removeprefix("proj_"),
                        "ours": n_ours,
                        "engine": joined.filter(b.is_not_null() & (b != 0)).height,
                        "both": both.height,
                        "coverage": both.height / n_ours if n_ours else None,
                        "rel_gap": ((both[stat] - both[f"{stat}_eng"]).abs().median() / mean)
                        if both.height and mean else None})
    return pl.DataFrame(out)


# --- the command line ------------------------------------------------------------------

def _parse_at(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def main(argv=None) -> int:
    """Command-line entry point. See ``python -m Scripts.books.engine --help``."""
    from Scripts.nfl_utils import current_season, current_week

    p = argparse.ArgumentParser(prog="python -m Scripts.books.engine",
                                description="Weekly props from the sportsbook engine (plan 54, shadow).")
    p.add_argument("--season", type=int)
    p.add_argument("--week", type=int)
    p.add_argument("--at", help="read the board as of this UTC time (default: now)")
    p.add_argument("--dry-run", action="store_true", help="derive without writing")
    p.add_argument("--compare", action="store_true", help="shadow vs our scrape for the week")
    p.add_argument("--agreement", action="store_true",
                   help="print the last run's agreement with Novig")
    args = p.parse_args(argv)

    season = current_season() if args.season is None else args.season
    week = current_week() if args.week is None else args.week
    pl.Config.set_tbl_rows(100)

    if args.compare:
        print(compare(season, week))
        return 0
    if args.agreement:
        runs = json.loads(STATUS_PATH.read_text())["runs"] if STATUS_PATH.exists() else []
        if not runs:
            print("no shadow run recorded yet")
            return 1
        print(f"as of {runs[-1]['at']}, against {REFERENCE_BOOK}:")
        print(pl.DataFrame(runs[-1]["agreement"]))
        return 0

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(f"===== engine props: {season} week {week} =====")
    try:
        status = run(season, week, _parse_at(args.at), write=not args.dry_run)
    except EngineError as exc:
        print(f"FAILED: {exc}")
        return 1
    projected = [b for b, e in status["books"].items() if e.get("outcome") == "projected"]
    print(f"done: {len(projected)} of {len(BOOKS)} books projected ({', '.join(projected)})")
    return 0 if projected else 1


if __name__ == "__main__":
    raise SystemExit(main())
