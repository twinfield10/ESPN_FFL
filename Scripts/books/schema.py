"""The standard row every book normalises to.

One row per posted price. That is the whole design decision, and it is what makes a
game line and a player prop the same artifact: a spread, a total, a moneyline, a team
total and "Ja'Marr Chase over 1099.5 receiving yards" are all a side, a number and a
price attached to a matchup. Adding game lines is therefore not a new artifact type,
it is rows this schema already has a place for.

Ported from ``Rebirtha/python/sportsbooks/config.py``'s ``ODDS_SCHEMA``, with three
additions that repo learned to want and never added:

* ``bookType`` -- an exchange price is a different quantity from a book price, and the
  code should be able to say so rather than leaving it in a comment. 4Casters keeps
  quoting after kickoff and has vig *added* rather than removed, which is why the
  other repo excludes it from closing-line reference entirely. A discriminator added
  later means rewriting every stored row, so it goes in now.
* ``fairProb`` -- ``impProb`` carries the vig. See :func:`add_fair_probability`.
* ``snapshot_ts`` -- stamped at write time upstream, declared here so an empty frame
  still has the shape a reader expects.
"""

from typing import Dict

import numpy as np
import polars as pl

#: A sportsbook: takes the other side, prices with a hold, stops at kickoff.
BOOK = "book"

#: An exchange: matches bettors, keeps quoting in-game, and its "current" price can be
#: a live number rather than a pregame one. Never a closing-line reference.
EXCHANGE = "exchange"

#: The market families this repo reasons about.
#:
#: ``TeamTotal`` is the one worth naming. ``Scripts/vegas.py`` derives each team's
#: implied points as ``total_line/2 +/- margin/2``, an identity that is exact only if
#: the market's team totals are symmetric about the game total. Measured against
#: Pinnacle's own quotes on 2026-08-27 they are not: the two sides sum to 0.25 points
#: *under* the game total on average, and the quoted number differs from the derived
#: one by a mean 0.734 points. A team-total market replaces that assumption with a
#: quote, which is the argument for scraping game lines at all.
MARKET_TITLES = ("Spread", "Total", "Moneyline", "TeamTotal")

#: The standard row. Polars dtypes rather than Python types, so an empty frame built
#: from this schema concatenates cleanly with a full one.
ODDS_SCHEMA: Dict[str, pl.DataType] = {
    "sportsbook":   pl.Utf8,
    "bookType":     pl.Utf8,
    "season":       pl.Int32,
    "week":         pl.Int32,
    "officialDate": pl.Utf8,
    "startTimeET":  pl.Utf8,
    "rotNum":       pl.Int64,
    "matchup":      pl.Utf8,
    "Home":         pl.Utf8,
    "Away":         pl.Utf8,
    "marketTitle":  pl.Utf8,
    "gamePeriod":   pl.Utf8,
    "betSide":      pl.Utf8,
    "sideOf":       pl.Utf8,
    "marketLine":   pl.Float64,
    "value":        pl.Float64,
    "price":        pl.Float64,
    "impProb":      pl.Float64,
    "fairProb":     pl.Float64,
    "isAlt":        pl.Boolean,
    "propType":     pl.Utf8,
    "snapshot_ts":  pl.Utf8,
}

#: Columns identifying one *line*, for change detection. Includes ``value`` because
#: each alternate is its own line: a -3.5 and a -6.5 spread on the same game are two
#: rows that move independently, and a grain without ``value`` would collapse them.
LINE_KEYS = ("sportsbook", "officialDate", "matchup", "marketTitle", "gamePeriod",
             "sideOf", "betSide", "marketLine", "propType")

#: What makes two rows *the same market*, and therefore each other's de-vig partner:
#: everything identifying the line except which side of it you took.
#:
#: Two columns here exist only because pairing is harder than it looks, and both were
#: found by the de-vig coming out wrong rather than by reasoning.
#:
#: ``sideOf`` names the entity a price is about -- the team on a team total, the player
#: on a prop. A game total has one over and one under, but a *team* total has two of
#: each, one pair per team, and when both teams are priced at the same number, as on
#: any pick-em, the four rows are otherwise indistinguishable. That is not a rare edge;
#: it was true of the first game looked at.
#:
#: ``marketLine`` is the number identifying the *market*, as against ``value``, which
#: is the number for *this side*. On a total they agree. On a spread they do not: the
#: book posts one market at -1.0 carrying a home price at -1.0 and an away price at
#: +1.0. Pairing on ``value`` therefore matched the away side of the -1.0 line with the
#: home side of the +1.0 line -- two different markets, silently de-vigged against each
#: other, on an alternate ladder where both exist.
PAIR_KEYS = tuple(k for k in LINE_KEYS if k != "betSide")

#: What "the line moved" means. Price and value both, because a book can hold a number
#: and reprice it, or move the number at the same price.
MOVEMENT_COLUMNS = ("value", "price", "impProb")


def empty_frame() -> pl.DataFrame:
    """An empty frame with the standard shape.

    A book that returns nothing must still return *something* shaped like odds, or
    every downstream concat and filter needs its own absence branch.

    Returns:
        pl.DataFrame: Zero rows, :data:`ODDS_SCHEMA` columns.
    """
    return pl.DataFrame(schema=ODDS_SCHEMA)


def american_to_probability(price) -> np.ndarray:
    """Implied probability of an American price, vig included.

    Args:
        price: American odds, e.g. -110 or +128. Array-like or scalar.

    Returns:
        np.ndarray: Implied probability in (0, 1). NaN where the price is missing.
    """
    p = np.asarray(price, dtype=float)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(p < 0, -p / (-p + 100.0), 100.0 / (p + 100.0))


def add_fair_probability(df: pl.DataFrame) -> pl.DataFrame:
    """Fill ``fairProb`` by de-vigging each two-sided market against its own pair.

    ``impProb`` is what the book posted and it sums to more than one across a market's
    two sides -- that surplus is the hold. Proportional de-vig, ``q = p / (p + p')``,
    is :func:`Scripts.market.devig_two_way`, and routing through it is the point: this
    repo derives de-vig in exactly one place, and a book that invents its own is the
    defect plan 35 exists to prevent.

    A market with only one side posted keeps ``impProb`` as its ``fairProb`` and is
    left slightly rich. That is honest -- there is nothing to de-vig against -- and it
    is visible, because the two columns being equal is the signal.

    Args:
        df: Frame carrying at least :data:`LINE_KEYS` minus ``betSide``, plus
            ``impProb``.

    Returns:
        pl.DataFrame: The same rows with ``fairProb`` populated.
    """
    from Scripts import market as mk

    if df.is_empty():
        return df.with_columns(pl.lit(None, dtype=pl.Float64).alias("fairProb"))

    out = df.with_columns(pl.col("impProb").alias("fairProb"))
    pair_keys = [k for k in PAIR_KEYS if k in out.columns]

    frames = []
    for _, group in out.group_by(pair_keys, maintain_order=True):
        if group.height == 2 and group["impProb"].null_count() == 0:
            a, b = group["impProb"].to_list()
            qa, qb = mk.devig_two_way(np.array([a]), np.array([b]))
            group = group.with_columns(
                pl.Series("fairProb", [float(qa[0]), float(qb[0])]))
        frames.append(group)
    return pl.concat(frames).select(out.columns)
