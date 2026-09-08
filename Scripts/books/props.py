"""Player props as :data:`ODDS_SCHEMA` rows, so their movement is stored.

The weekly prop scrapers produce a flat, stat-shaped frame that the blend consumes
directly, and that shape is deliberately left alone -- see
``docs/plans/45-props-in-the-odds-store.md``. What it cannot do is remember: the
landing file is overwritten every run, so a line's history is a week of snapshots at
best and usually nothing.

This module is the other half of a **dual write**. The same scrape that feeds the
blend also lands here, converted to the standard odds row and appended to the store
that already holds game lines. Nothing downstream of the blend changes; what is
gained is that ``Scripts/books/store.py`` diffs each pull against the last and keeps
only what moved, which is prop line history as a by-product of running the nightly.

Props and game lines share a book's directory on purpose. ``ODDS_SCHEMA`` was built
for both -- ``propType`` is the slot, ``sideOf`` "names the entity a price is about --
the team on a team total, the player on a prop" -- and ``store._key_expr`` already
handles the nulls that appear on one kind and not the other.

Two mappings are worth stating, because both are choices rather than transcription:

* **A ladder rung is an alternate.** ``marketsBySs`` prices "2 or more touchdowns" as
  its own one-sided market, which is what ``isAlt=True`` means here. Only the two-way
  ``marketsByOu`` rows pair up, so only they get a de-vigged ``fairProb``; a rung keeps
  its raw ``impProb``, which :func:`add_fair_probability` documents as the honest
  outcome when there is nothing to de-vig against.
* **``propType`` carries the repo's stat name**, not the book's market title, so a
  query spans books once a second one arrives.
"""

from datetime import datetime, timezone
from typing import Optional

import numpy as np
import polars as pl

from Scripts.books.schema import BOOK, ODDS_SCHEMA, add_fair_probability
from Scripts.paths import DATA_DIR

#: What a player prop is called in ``marketTitle``. One value rather than one per stat:
#: the stat lives in ``propType``, and keeping the market families short is what lets
#: ``Scripts/vegas.py`` and friends filter game lines with an equality test.
PROP_MARKET = "PlayerProp"

#: Matches the game-line rows already stored; props are full-game markets.
GAME_PERIOD = "GAME"


def _team_names() -> pl.DataFrame:
    """Abbreviation to full team name, as the stored ``matchup`` strings spell it."""
    path = DATA_DIR / "NFL" / "team_names.parquet"
    if not path.is_file():
        return pl.DataFrame(schema={"team_abbr": pl.Utf8, "team_name": pl.Utf8})
    return pl.read_parquet(path).select(["team_abbr", "team_name"])


def _american(decimal: pl.Expr) -> pl.Expr:
    """Decimal odds to American, the units :data:`ODDS_SCHEMA` stores.

    DST quotes decimal. The store is American because every other adapter is, and
    ``impProb`` is carried alongside anyway -- so this is presentation, not arithmetic
    the de-vig depends on.
    """
    return (pl.when(decimal >= 2.0)
              .then((decimal - 1.0) * 100.0)
              .otherwise(-100.0 / (decimal - 1.0))
              .round(0))


def props_to_odds_rows(raw: pl.DataFrame, season: int, sched: pl.DataFrame,
                       sportsbook: str = "BetOnline",
                       snapshot_ts: Optional[str] = None) -> pl.DataFrame:
    """Convert a flat weekly-prop frame to standard odds rows.

    Args:
        raw: The scraper's raw frame -- ``FULL_DF_SCHEMA`` from
            ``Scripts.scrape_BOL``: one row per posted price, with ``team``,
            ``player_name``, ``espn_stat``, ``value``, ``odds`` (decimal), ``type``
            (``Over``/``Under`` on a two-way market) and ``prop_source``
            (``OverUnder`` or ``Values``).
        season: Season year.
        sched: Slim schedule carrying ``week``, ``officialDate``, ``Away``, ``Home``
            as team abbreviations -- ``Scripts.scrape_BOL.slim_schedule()``.
        sportsbook: Book name, stored verbatim and used as the store's directory.
        snapshot_ts: Overrides the stamp, for tests.

    Returns:
        pl.DataFrame: :data:`ODDS_SCHEMA` rows, empty-but-shaped if *raw* is empty.
    """
    if raw.is_empty():
        return pl.DataFrame(schema=ODDS_SCHEMA)

    stamp = snapshot_ts or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    names = _team_names()

    # A prop names one team; the fixture names two. Listing each game under both of
    # its teams gives the player's row its matchup regardless of which side it is on.
    games = sched.select(["week", "officialDate", "Away", "Home"])
    lookup = pl.concat([
        games.with_columns(pl.col("Away").alias("team")),
        games.with_columns(pl.col("Home").alias("team")),
    ]).unique(subset=["week", "team"])

    out = raw.join(lookup, on=["week", "team"], how="inner")
    if out.is_empty():
        return pl.DataFrame(schema=ODDS_SCHEMA)

    out = (
        out
        .join(names.rename({"team_abbr": "Away", "team_name": "_away_name"}),
              on="Away", how="left")
        .join(names.rename({"team_abbr": "Home", "team_name": "_home_name"}),
              on="Home", how="left")
        .with_columns([
            pl.lit(sportsbook).alias("sportsbook"),
            pl.lit(BOOK).alias("bookType"),
            pl.lit(season).cast(pl.Int32).alias("season"),
            pl.col("week").cast(pl.Int32).alias("week"),
            pl.col("officialDate").cast(pl.Utf8).alias("officialDate"),
            pl.lit(None, dtype=pl.Utf8).alias("startTimeET"),
            pl.lit(None, dtype=pl.Int64).alias("rotNum"),
            # Same "Away vs. Home" spelling the game-line rows use, so one matchup
            # string reaches a game's spread and its players' props alike.
            (pl.coalesce([pl.col("_away_name"), pl.col("Away")])
             + pl.lit(" vs. ")
             + pl.coalesce([pl.col("_home_name"), pl.col("Home")])).alias("matchup"),
            pl.coalesce([pl.col("_home_name"), pl.col("Home")]).alias("Home"),
            pl.coalesce([pl.col("_away_name"), pl.col("Away")]).alias("Away"),
            pl.lit(PROP_MARKET).alias("marketTitle"),
            pl.lit(GAME_PERIOD).alias("gamePeriod"),
            # A ladder rung is "N or more", which is an over with no under posted.
            pl.when(pl.col("type") == "Under").then(pl.lit("under"))
              .otherwise(pl.lit("over")).alias("betSide"),
            pl.col("player_name").alias("sideOf"),
            pl.col("value").cast(pl.Float64).alias("marketLine"),
            pl.col("value").cast(pl.Float64).alias("value"),
            _american(pl.col("odds").cast(pl.Float64)).alias("price"),
            pl.col("impProb").cast(pl.Float64).alias("impProb"),
            (pl.col("prop_source") == "Values").alias("isAlt"),
            pl.col("espn_stat").alias("propType"),
            pl.lit(stamp).alias("snapshot_ts"),
        ])
    )

    out = add_fair_probability(out)
    return out.select(list(ODDS_SCHEMA)).cast(ODDS_SCHEMA)
