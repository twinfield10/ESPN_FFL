"""Weekly player props from any book, through one derivation, into one file shape.

BetOnline and Pinnacle each grew their own path from a scrape to a ``proj_<stat>``
file. Four more books arrived on 2026-10-13 -- FanDuel, DraftKings, Novig and
4Casters, ported from ``NHL-Data/src/nhl/sources`` -- and a fifth and sixth copy of
that path is exactly what ``Scripts/market.py`` was written to end. So this module
is the path, and a book adapter's whole job is to produce :data:`RAW_SCHEMA` rows.

**The raw row** is one posted price, the shape ``Scripts/scrape_BOL.py`` already
archives:

* ``prop_source == "OverUnder"`` -- one side of a two-way market. ``value`` is the
  line, ``type`` is ``"Over"`` or ``"Under"``.
* ``prop_source == "Values"`` -- one rung of a one-sided ladder. ``value`` is ``k``
  and the price is ``P(X >= k)``: an anytime touchdown is ``value == 1``, "2+
  receptions" is 2, "150+ yards" is 150. **Not** ``k - 0.5``, which is how NHL-Data
  stores the same thing; the ladder arithmetic here reads thresholds as integers.

``impProb`` is the price as a probability, vig included where the book has one. An
exchange whose adapter has already removed the margin sets the two sides of a pair
to sum to 1, and then the de-vig below is a no-op rather than a special case.

**The derivation** is :func:`project_stat`, lifted out of
``Scripts.scrape_BOL.get_x_stat`` unchanged so that BetOnline and the new books
share it: two-way pair first, de-vigged and converted by
:meth:`Scripts.market.MarketModel.mean_from_line`; a ladder's exact count moments or
de-vigged median where there is no pair; a dispersion from the ladder wherever there
is one.

**The file** is one per book per season, ``Data/Projections/<Book>/Season/<season>/
<Book>_AllProps.parquet``, one row per player-week, latest scrape winning -- the
shape ``Scripts.projection_utils.clean_bol`` reads, so a new book reaches the blend
with a loader rather than a new pipeline.

Usage::

    python -m Scripts.books.weekly_props --book fanduel
    python -m Scripts.books.weekly_props --book all --week 6
    python -m Scripts.books.weekly_props --compare --week 6
"""

import importlib
import logging
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Sequence

import numpy as np
import polars as pl

from Scripts import market as mk
from Scripts.paths import DATA_DIR, season_dir

logger = logging.getLogger(__name__)

#: One posted price. See the module docstring for the two ``prop_source`` shapes.
RAW_SCHEMA: Dict[str, pl.DataType] = {
    "book": pl.Utf8,
    "NFL_game_id": pl.Utf8,
    "week": pl.Int64,
    "player_name": pl.Utf8,
    "team": pl.Utf8,
    "position": pl.Utf8,
    "espn_stat": pl.Utf8,
    "value": pl.Float64,
    "type": pl.Utf8,
    "odds": pl.Float64,
    "impProb": pl.Float64,
    "prop_source": pl.Utf8,
}

#: Player-week identity for every book but BetOnline, whose keys carry its own game
#: id. Position is in the key because the conversion is indexed by it; team is not,
#: because three of the four books do not say which side a player is on.
KEYS: List[str] = ["NFL_game_id", "week", "player_name", "position"]

#: Stats a weekly prop can project. The defensive markets are BetOnline's alone.
OFFENSIVE_STATS: tuple = (
    "anytimeTouchdown", "passingYards", "passingAttempts", "passingCompletions",
    "passingTouchdowns", "passingInterceptions", "rushingYards", "rushingAttempts",
    "receivingYards", "receivingReceptions",
)

#: Adapter module per book. Each exposes ``BOOK``, ``DIR``, ``PREFIX``,
#: ``fetch(sched) -> payload`` and ``normalize(payload, sched) -> RAW_SCHEMA rows``,
#: and optionally ``FALLBACK_HOLD``.
ADAPTERS: Dict[str, str] = {
    "fanduel": "Scripts.books.fanduel_props",
    "draftkings": "Scripts.books.draftkings_props",
    "novig": "Scripts.books.novig_props",
    "fourcasters": "Scripts.books.fourcasters_props",
}


# --- shared helpers adapters use -------------------------------------------------

def american_to_decimal(american: float) -> float:
    """American odds to decimal. ``+150 -> 2.5``, ``-200 -> 1.5``."""
    american = float(american)
    return 1.0 + (american / 100.0 if american > 0 else 100.0 / abs(american))


def team_resolver() -> Callable[[Optional[str]], Optional[str]]:
    """A function from any spelling of a team to the schedule's abbreviation.

    Every book names a game by full team names, ESPN by its own abbreviations, and
    the schedule by nflverse's -- ``LA`` for the Rams, ``WAS`` for Washington. One
    lookup built from ``Data/NFL/team_names.parquet`` covers all three, keyed on the
    upper-cased name, the nickname, the abbreviation and a jersey-logo slug.
    """
    names = pl.read_parquet(DATA_DIR / "NFL" / "team_names.parquet")
    lookup: Dict[str, str] = {}
    for abbr, full, nick in names.select(["team_abbr", "team_name", "team_nick"]).iter_rows():
        lookup[abbr.upper()] = abbr
        lookup[full.upper()] = abbr
        lookup[full.upper().replace(" ", "_")] = abbr
    # Current franchises win any collision with a relocated one ("Rams", "Raiders").
    current = {"LA": "Los Angeles Rams", "LV": "Las Vegas Raiders", "LAC": "Los Angeles Chargers"}
    for abbr, full in current.items():
        lookup[full.upper()] = abbr
    lookup.update({"WSH": "WAS", "LAR": "LA", "JAC": "JAX", "LVR": "LV", "OAK": "LV"})

    def resolve(text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        return lookup.get(str(text).strip().upper())

    return resolve


def game_lookup(sched: pl.DataFrame) -> Callable[[Optional[str], Optional[str]], Optional[str]]:
    """A function from two team spellings to this week's ``NFL_game_id``, or None.

    Order-insensitive, because a neutral-site game can be listed either way round
    and a book's "away" is not always the schedule's.
    """
    resolve = team_resolver()
    by_pair = {frozenset((a, h)): gid for gid, a, h
               in sched.select(["NFL_game_id", "Away", "Home"]).iter_rows()}

    def find(team_a: Optional[str], team_b: Optional[str]) -> Optional[str]:
        a, b = resolve(team_a), resolve(team_b)
        if not a or not b:
            return None
        return by_pair.get(frozenset((a, b)))

    return find


def raw_frame(rows: Sequence[dict]) -> pl.DataFrame:
    """Rows to :data:`RAW_SCHEMA`, filling ``impProb`` from ``odds`` where unset."""
    if not rows:
        return pl.DataFrame(schema=RAW_SCHEMA)
    frame = pl.DataFrame([{k: r.get(k) for k in RAW_SCHEMA} for r in rows], schema=RAW_SCHEMA)
    return frame.with_columns(
        pl.coalesce(pl.col("impProb"), 1.0 / pl.col("odds")).alias("impProb"))


def assign_positions(raw: pl.DataFrame) -> pl.DataFrame:
    """Fill a missing position from the markets the book posted for the player.

    The same inference Pinnacle uses (:func:`Scripts.market.position_from_markets`):
    a passing line means a quarterback, a rushing line without one a back, anything
    else the pooled receiver key ``REC``. A back priced only on an anytime touchdown
    therefore reads ``REC`` -- which is why the touchdown split below is redone on
    the blended frame by ``reallocate_book_touchdowns`` wherever ESPN and FantasyPros
    state a ratio.
    """
    if raw.is_empty():
        return raw
    inferred = (raw.group_by(["NFL_game_id", "player_name"])
                   .agg(pl.col("espn_stat").unique().alias("_stats"))
                   .with_columns(pl.col("_stats").map_elements(
                       mk.position_from_markets, return_dtype=pl.Utf8).alias("_pos"))
                   .drop("_stats"))
    return (raw.join(inferred, on=["NFL_game_id", "player_name"], how="left")
               .with_columns(pl.coalesce("position", "_pos").alias("position"))
               .drop("_pos"))


def book_overround(raw: pl.DataFrame, default: float = mk.DEFAULT_OVERROUND) -> float:
    """The book's margin across every two-way pair it posted, any stat.

    The fallback for a ladder-only stat -- an anytime touchdown, usually -- when the
    book posts no two-way market on that stat to measure its hold from. BetOnline
    falls back to :data:`Scripts.market.DEFAULT_OVERROUND`, measured on BetOnline;
    applying BetOnline's 6.4% to FanDuel or to an exchange would be a guess, and the
    book's own margin on its other props is a measurement.
    """
    ou = raw.filter(pl.col("prop_source") == "OverUnder")
    if ou.is_empty():
        return float(default)
    sides = ou.pivot(index=["NFL_game_id", "player_name", "espn_stat", "value"],
                     on="type", values="impProb", aggregate_function="first")
    if not {"Over", "Under"}.issubset(sides.columns):
        return float(default)
    return mk.measure_overround(sides["Over"].to_numpy(), sides["Under"].to_numpy(),
                                default=default)


# --- the derivation --------------------------------------------------------------

def _finite(value):
    """A float, or None where it is not finite -- Polars skips null and propagates
    NaN, and every consumer of a dispersion column wants the former."""
    return None if value is None or not np.isfinite(value) else float(value)


def project_stat(df: pl.DataFrame, stat: str, model, keys: Sequence[str],
                 fallback_hold: float = mk.DEFAULT_OVERROUND) -> pl.DataFrame:
    """One stat's projection, and its market-implied dispersion, per player-week.

    Lifted from ``Scripts.scrape_BOL.get_x_stat``, which now calls it. **Every piece
    of arithmetic here lives in :mod:`Scripts.market`**; this is the plumbing. The
    hierarchy, cheapest information first:

    1. A two-way pair, de-vigged, converted by
       :meth:`Scripts.market.MarketModel.mean_from_line`.
    2. A count ladder rooted at 1, read by the exact discrete identity.
    3. A yardage ladder's own de-vigged median -- never ``sum(threshold * P)``.

    Args:
        df: Raw price rows for any stats; filtered to ``stat`` here.
        stat: ESPN stat name.
        model: Loaded :class:`Scripts.market.MarketModel`, or None to convert a line
            to itself.
        keys: Player-week identity columns, which must include ``position``.
        fallback_hold: Overround applied to the ladder when this stat has no two-way
            pair to measure one from.

    Returns:
        pl.DataFrame: ``keys`` plus ``proj_<stat>`` and ``proj_<stat>_sd``.
    """
    keys = list(keys)
    df = df.filter(pl.col("espn_stat") == stat)
    df_ou = df.filter(pl.col("prop_source") == "OverUnder")
    df_val = df.filter(pl.col("prop_source") == "Values")

    # The book's own margin, measured on this scrape's two-way pairs and applied to
    # the one-sided ladders as well.
    hold = fallback_hold
    if df_ou.height > 0:
        sides = df_ou.pivot(index=keys + ["value"], on="type", values="impProb",
                            aggregate_function="first")
        if {"Over", "Under"}.issubset(sides.columns):
            hold = mk.measure_overround(sides["Over"].to_numpy(),
                                        sides["Under"].to_numpy(),
                                        default=fallback_hold)

    def ou_calc(frame: pl.DataFrame) -> pl.DataFrame:
        """Two-way pairs into an expectation: de-vig, then convert by stat shape."""
        wide = frame.pivot(
            index=keys + ["espn_stat", "value"],
            on="type", values=["odds", "impProb"], aggregate_function="first",
        )
        if not {"impProb_Over", "impProb_Under"}.issubset(wide.columns):
            wide = wide.head(0)
        else:
            wide = wide.drop_nulls(["impProb_Over", "impProb_Under"])
        if wide.height == 0:
            return frame.select(keys).head(0).with_columns(
                pl.lit(None, dtype=pl.Float64).alias(f"from_line_{stat}"))

        q_over, _ = mk.devig_two_way(wide["impProb_Over"].to_numpy(),
                                     wide["impProb_Under"].to_numpy())
        lines = wide["value"].to_numpy()
        converted = (model.mean_from_line(stat, lines, q_over,
                                          wide["position"].to_list())
                     if model is not None else lines)
        # One row per player-stat, in case the book posts alternate lines: the join
        # below is on the keys alone and a duplicate there multiplies every stat.
        return (wide
                .with_columns(pl.Series(f"from_line_{stat}", converted))
                .group_by(keys)
                .agg(pl.col(f"from_line_{stat}").mean()))

    def ladder_calc(frame: pl.DataFrame) -> pl.DataFrame:
        """A ladder into a projection and a dispersion, by its shape."""
        kind = mk.MARKET_STATS[stat].kind if stat in mk.MARKET_STATS else "yardage"
        rows = []
        for group in frame.partition_by(keys, maintain_order=True):
            edges, survival = mk.monotone_survival(group["value"].to_numpy(),
                                                   group["impProb"].to_numpy())
            fair = mk.devig_survival(survival, hold)
            mean = (mk.count_moments(edges, fair)[0] if kind == "count"
                    else mk.ladder_median(edges, fair))
            rows.append({
                **{key: group[key][0] for key in keys},
                f"from_ladder_{stat}": None if not np.isfinite(mean) else mean,
                f"proj_{stat}_sd": _finite(mk.market_scale(edges, fair, kind)),
            })
        if not rows:
            return frame.select(keys).head(0).with_columns(
                pl.lit(None, dtype=pl.Float64).alias(f"from_ladder_{stat}"),
                pl.lit(None, dtype=pl.Float64).alias(f"proj_{stat}_sd"))
        return pl.DataFrame(rows, schema_overrides={
            **{key: frame.schema[key] for key in keys},
            f"from_ladder_{stat}": pl.Float64, f"proj_{stat}_sd": pl.Float64})

    # A full join, not an inner one: a laddered player with no line still states a
    # variance, and a lined player with no ladder still states a mean.
    if df_ou.height > 0 and df_val.height > 0:
        final_df = ou_calc(df_ou).join(ladder_calc(df_val), on=keys, how="full",
                                       coalesce=True)
    elif df_ou.height > 0:
        final_df = ou_calc(df_ou).with_columns(
            pl.lit(None, dtype=pl.Float64).alias(f"from_ladder_{stat}"),
            pl.lit(None, dtype=pl.Float64).alias(f"proj_{stat}_sd"))
    else:
        final_df = ladder_calc(df_val).with_columns(
            pl.lit(None, dtype=pl.Float64).alias(f"from_line_{stat}"))

    return (final_df
            .with_columns(pl.coalesce(pl.col(f"from_line_{stat}"),
                                      pl.col(f"from_ladder_{stat}"))
                            .alias(f"proj_{stat}"))
            .drop(f"from_line_{stat}", f"from_ladder_{stat}")
            .select(keys + [f"proj_{stat}", f"proj_{stat}_sd"])
            .sort(by=[f"proj_{stat}"], nulls_last=True))


def project_book(raw: pl.DataFrame, sched: pl.DataFrame, model=None,
                 fallback_hold: Optional[float] = None,
                 td_hold: Optional[float] = None,
                 stats: Iterable[str] = OFFENSIVE_STATS) -> pl.DataFrame:
    """Every stat a book priced, one row per player-week, in BetOnline's file shape.

    Args:
        raw: :data:`RAW_SCHEMA` rows for one book and one week.
        sched: That week's rows of ``Scripts.scrape_BOL.slim_schedule()``.
        model: Loaded market model; None loads it.
        fallback_hold: Ladder overround where a stat has no two-way pair. None
            measures it across the book's own pairs (:func:`book_overround`).
        td_hold: The anytime-touchdown ladder's own margin, where it has been
            measured (:data:`Scripts.market.TD_OVERROUND`); None uses ``fallback_hold``.
        stats: Stats to build.

    Returns:
        pl.DataFrame: :data:`KEYS`, ``proj_<stat>``/``proj_<stat>_sd`` per stat
        present, the touchdown split, ``officialDate`` and ``BetTimeStamp``.
    """
    if raw.is_empty():
        return pl.DataFrame(schema={k: RAW_SCHEMA[k] for k in KEYS})
    model = mk.load_model() if model is None else model
    raw = assign_positions(raw)
    hold = book_overround(raw) if fallback_hold is None else fallback_hold

    present = [s for s in stats if s in set(raw["espn_stat"].unique().to_list())]
    result = None
    for stat in present:
        stat_hold = td_hold if (stat == "anytimeTouchdown" and td_hold) else hold
        frame = project_stat(raw, stat, model, KEYS, fallback_hold=stat_hold)
        result = frame if result is None else result.join(frame, on=KEYS, how="full",
                                                          coalesce=True)

    # The anytime market, split by position the way BetOnline's is, and re-split on
    # the blended frame by `reallocate_book_touchdowns` wherever a consensus exists.
    # `REC` -- the inferred receiver key -- is receiving, as WR and TE are.
    if "proj_anytimeTouchdown" in result.columns:
        rushing = pl.col("position").is_in(["QB", "RB"])
        result = (result.with_columns([
            pl.when(rushing).then(pl.col("proj_anytimeTouchdown"))
              .otherwise(pl.lit(0.0)).alias("proj_rushingTouchdowns"),
            pl.when(~rushing).then(pl.col("proj_anytimeTouchdown"))
              .otherwise(pl.lit(0.0)).alias("proj_receivingTouchdowns"),
            pl.when(rushing).then(pl.col("proj_anytimeTouchdown_sd"))
              .alias("proj_rushingTouchdowns_sd"),
            pl.when(~rushing).then(pl.col("proj_anytimeTouchdown_sd"))
              .alias("proj_receivingTouchdowns_sd"),
        ]).drop("proj_anytimeTouchdown", "proj_anytimeTouchdown_sd"))

    return (result
            .join(sched.select(["NFL_game_id", "officialDate"]), on="NFL_game_id",
                  how="left")
            .with_columns(pl.lit(datetime.now()).alias("BetTimeStamp"))
            .sort(["NFL_game_id", "player_name"]))


# --- storage -----------------------------------------------------------------------

def season_file(book_dir: str, season: int, create: bool = True):
    """The season's accumulated per-player-week file for one book."""
    return season_dir(book_dir, season, f"{book_dir}_AllProps.parquet", create=create)


def archive_raw(raw: pl.DataFrame, book_dir: str, season: int, week: int) -> None:
    """Keep every scrape's raw prices, stamped, because they cannot be re-derived.

    BetOnline keeps one file per week and overwrites it. These keep every pull: a
    few hundred kilobytes a week, and the only way plan 35's de-vig and conversion
    can ever be scored on these books against realised stat lines -- which is the
    backtest this comparison could not run.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = season_dir(book_dir, season, "Raw", f"{book_dir}_Raw_Week_{week}_{stamp}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    raw.write_parquet(path)


def reconcile(book_dir: str, projected: pl.DataFrame, season: int,
              path=None) -> pl.DataFrame:
    """Merge a fresh pull into the season file, the latest pull winning per player-week.

    Keyed on ``(week, player_name)`` alone -- not on position as BetOnline's is --
    because an inferred position can change between pulls as a book posts more of a
    player's markets, and keying on it would keep both rows and double the player at
    the blend's merge.

    ``path`` overrides the season file, which is how ``Scripts.books.engine`` writes
    its shadow copy (plan 54) without the blend ever reading it.
    """
    path = season_file(book_dir, season) if path is None else path
    combined = projected
    if path.exists():
        existing = pl.read_parquet(path)
        combined = pl.concat([existing, projected], how="diagonal_relaxed")
    combined = (combined.sort("BetTimeStamp", descending=True)
                        .unique(subset=["week", "player_name"], keep="first",
                                maintain_order=True)
                        .sort(["week", "NFL_game_id", "player_name"]))
    combined.write_parquet(path)
    return combined


# --- the driver ----------------------------------------------------------------------

def load_adapter(book: str):
    """The adapter module for ``book`` (a key of :data:`ADAPTERS`)."""
    return importlib.import_module(ADAPTERS[book])


def scrape(book: str, season: Optional[int] = None, week: Optional[int] = None,
           write: bool = True, model=None) -> pl.DataFrame:
    """Pull one book's props for a week, derive projections, and reconcile them.

    Args:
        book: A key of :data:`ADAPTERS`.
        season: None derives it from the schedule.
        week: None uses the first week with unplayed games.
        write: False pulls and derives without touching disk.
        model: Loaded market model; None loads it.

    Returns:
        pl.DataFrame: This pull's projections, empty if the book posted nothing.
    """
    from Scripts.nfl_utils import current_season, current_week
    from Scripts.scrape_BOL import slim_schedule

    adapter = load_adapter(book)
    season = current_season() if season is None else season
    week = current_week() if week is None else week
    sched = slim_schedule().filter(pl.col("week") == week)

    payload = adapter.fetch(sched)
    raw = adapter.normalize(payload, sched).with_columns(pl.lit(week).cast(pl.Int64).alias("week"))
    print(f"{adapter.BOOK} week {week}: {raw.height} prices, "
          f"{raw['player_name'].n_unique() if raw.height else 0} players, "
          f"{raw['NFL_game_id'].n_unique() if raw.height else 0} games")
    if raw.is_empty():
        return pl.DataFrame()

    projected = project_book(raw, sched, model=model,
                             fallback_hold=getattr(adapter, "FALLBACK_HOLD", None),
                             td_hold=mk.TD_OVERROUND.get(adapter.BOOK))
    if write:
        archive_raw(raw, adapter.DIR, season, week)
        total = reconcile(adapter.DIR, projected, season)
        print(f"  wrote {projected.height} player rows; season file now {total.height}")
    return projected


def main(argv=None) -> int:
    """Command-line entry point. Exit 1 if any requested book returned nothing."""
    import argparse

    p = argparse.ArgumentParser(prog="python -m Scripts.books.weekly_props",
                                description="Scrape weekly NFL player props from more books.")
    p.add_argument("--book", default="all", choices=["all", *ADAPTERS])
    p.add_argument("--season", type=int)
    p.add_argument("--week", type=int)
    p.add_argument("--dry-run", action="store_true", help="do not write files")
    p.add_argument("--compare", action="store_true",
                   help="print the coverage comparison instead of scraping")
    args = p.parse_args(argv)

    if args.compare:
        from Scripts.books.compare_props import main as compare_main
        return compare_main(["--week", str(args.week)] if args.week else [])

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    model = mk.load_model()
    books = list(ADAPTERS) if args.book == "all" else [args.book]
    failed = []
    for book in books:
        try:
            out = scrape(book, season=args.season, week=args.week,
                         write=not args.dry_run, model=model)
            if out.is_empty():
                failed.append(book)
        except Exception as exc:  # one book down must not stop the others
            print(f"{book}: FAILED ({type(exc).__name__}: {exc})")
            failed.append(book)
    if failed:
        print(f"no props from: {', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
