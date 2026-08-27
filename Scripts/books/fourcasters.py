"""4Casters: an exchange, and deliberately not treated like a book.

The third source, and the one the schema's ``bookType`` column exists for. Plan 36
asked that the exchange caveat live in the schema rather than in a comment, because an
exchange price is a different quantity from a book price in three ways that all matter:

* **It keeps quoting after kickoff.** A "current" price can be a live in-game number,
  which is not a pregame opinion about anything. Live games are dropped here, and the
  upstream repo goes further and excludes this source from closing-line reference
  entirely (``CLV_REFERENCE_BOOKS = ["Pinnacle", "LowVig"]``).
* **There is no single price.** Each market is an order book -- a list of standing
  offers at different odds with different amounts behind them. What this adapter
  stores is the best price with real money behind it, and the size that was available,
  because "best price" at five dollars is not a market view.
* **Its overround is a bid-ask spread, not a hold**, and measured 2026-08-27 it is
  the *wider* of the two: 1.078 against Pinnacle's 1.040 over the same 16 games,
  never below 1.0. Worth stating because the intuition runs the other way -- an
  exchange takes no position, so its prices ought to be tighter. They are not, once
  you take both sides at the offered price, because the two best offers are separated
  by a spread and crossing it twice costs more than Pinnacle's hold. Proportional
  normalisation still applies and needs no special case, but the number it produces
  means something different from a book's, and ``bookType`` is how a consumer tells.

Credentials come from the environment, never from the repo::

    export CAST4_USER=...
    export CAST4_PASS=...
    export CAST4_AUTH_TOKEN=...     # optional; skips the login round-trip
"""

import os
from datetime import datetime, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import polars as pl

from Scripts.books.base import BaseSportsbook, BookFetchError, FetchFailure
from Scripts.books.schema import EXCHANGE, ODDS_SCHEMA, american_to_probability

BASE_URL = "https://api.4casters.io"

#: Smallest offer, in the exchange's own units, that counts as a price rather than
#: dust. An order book always has a "best" price; without a floor it is whoever left a
#: five-dollar limit order at a silly number, and that is not a market view.
MIN_LIQUIDITY = 25.0

ET = ZoneInfo("America/New_York")


class FourCastersSportsbook(BaseSportsbook):
    """4Casters' NFL order book, reduced to one best price per market side.

    Returns the same view names as the other adapters. There are no team totals and no
    alternate flag -- every spread and total the exchange lists is a real market that
    someone is offering, so the alternate/main distinction a book makes does not apply.
    """

    book_type = EXCHANGE

    def __init__(self, sport: str = "football", league: str = "nfl",
                 season: Optional[int] = None):
        super().__init__(sport=sport, league=league)
        self.season = season
        self._token: Optional[str] = None

    # --- auth --------------------------------------------------------------

    def _authenticate(self) -> str:
        """Reuse a supplied token if it still validates, otherwise log in.

        Raises:
            BookFetchError: If no credentials are configured, or login is refused.
        """
        token = os.getenv("CAST4_AUTH_TOKEN")
        if token:
            probe = self.fetch_json(f"{BASE_URL}/user/getme", method="POST",
                                    headers={"Authorization": token,
                                             "Content-Type": "application/json"})
            if probe:
                return token

        user, password = os.getenv("CAST4_USER"), os.getenv("CAST4_PASS")
        if not user or not password:
            raise BookFetchError(
                "4Casters needs CAST4_USER and CAST4_PASS in the environment "
                "(CAST4_AUTH_TOKEN optionally skips the login).",
                FetchFailure.OTHER)

        body = self.fetch_json(f"{BASE_URL}/user/login", method="POST",
                               headers={"Content-Type": "application/json"},
                               body={"username": user, "password": password})
        token = ((body or {}).get("data") or {}).get("user", {}).get("auth")
        if not token:
            raise BookFetchError("4Casters refused the login.", self.last_failure)
        return token

    # --- fetching ----------------------------------------------------------

    def fetch_odds(self) -> Dict[str, pl.DataFrame]:
        """Pull the NFL order book."""
        self._token = self._authenticate()
        body = self.fetch_json(
            f"{BASE_URL}/exchange/getOrderbook", method="POST",
            headers={"Authorization": self._token,
                     "Content-Type": "application/json"},
            body={"leagueRequested": "NFL"})

        games = ((body or {}).get("data") or {}).get("games", []) or []
        # A live game's prices are an in-game number, not a pregame opinion. A
        # user-created or specials market is not a game line at all.
        pregame = [g for g in games
                   if not g.get("live") and not g.get("userCreated")
                   and not g.get("isSpecials") and not g.get("ended")]
        print(f"  4Casters NFL: {len(pregame)} pregame games "
              f"({len(games) - len(pregame)} live/other dropped)")

        final = self.transform_to_standard(self._flatten(pregame))
        return {
            "Pinnacle": final,
            "Pinnacle_Alts": final.head(0),
            "Games": pl.DataFrame(),
            "All_Bets": final,
        }

    @staticmethod
    def _best(offers: List[dict]) -> Optional[dict]:
        """The best-priced offer with real money behind it.

        "Best" is the highest American odds, which is the most the taker is paid.
        """
        real = [o for o in (offers or [])
                if o.get("odds") is not None
                and float(o.get("sumUntaken") or 0) >= MIN_LIQUIDITY]
        return max(real, key=lambda o: float(o["odds"])) if real else None

    def _flatten(self, games: List[dict]) -> pl.DataFrame:
        """One row per market side, at its best available price."""
        rows = []
        for game in games:
            parts = {p.get("homeAway"): p for p in game.get("participants", [])}
            home, away = parts.get("home"), parts.get("away")
            if not home or not away:
                continue

            start = game.get("start") or game.get("dateGame")
            start_et = (datetime.fromisoformat(start.replace("Z", "+00:00"))
                        .astimezone(ET)) if start else None
            common = {
                "season": self.season,
                "officialDate": start_et.date().isoformat() if start_et else None,
                "startTimeET": start_et.isoformat() if start_et else None,
                "rotNum": int(home.get("rotationNumber") or 0) or None,
                "Home": home.get("longName"), "Away": away.get("longName"),
                "gamePeriod": "GAME", "isAlt": False, "propType": None,
                "sideOf": None,
            }

            for side, key in (("home", "homeMoneylines"), ("away", "awayMoneylines")):
                best = self._best(game.get(key))
                if best:
                    rows.append({**common, "marketTitle": "Moneyline",
                                 "betSide": side, "marketLine": 0.0, "value": 0.0,
                                 "price": float(best["odds"])})

            # Spreads and totals arrive as {line: [offers]}. The market line is the
            # home team's number, matching the other two adapters, so the away
            # book's keys are negated rather than reported as posted.
            for side, key, negate in (("home", "homeSpreads", False),
                                      ("away", "awaySpreads", True)):
                for line, offers in (game.get(key) or {}).items():
                    best = self._best(offers)
                    if not best:
                        continue
                    point = float(line)
                    rows.append({**common, "marketTitle": "Spread", "betSide": side,
                                 "marketLine": -point if negate else point,
                                 "value": point, "price": float(best["odds"])})

            for side, key in (("over", "over"), ("under", "under")):
                for line, offers in (game.get(key) or {}).items():
                    best = self._best(offers)
                    if not best:
                        continue
                    point = float(line)
                    rows.append({**common, "marketTitle": "Total", "betSide": side,
                                 "marketLine": point, "value": point,
                                 "price": float(best["odds"])})

        if not rows:
            return pl.DataFrame(schema=ODDS_SCHEMA)
        return pl.DataFrame(rows)

    def transform_to_standard(self, raw: pl.DataFrame) -> pl.DataFrame:
        """Add matchup and implied probability, then hand to the base."""
        if raw.is_empty():
            return pl.DataFrame(schema=ODDS_SCHEMA)
        raw = raw.with_columns([
            (pl.col("Away") + pl.lit(" vs. ") + pl.col("Home")).alias("matchup"),
            pl.Series("impProb", american_to_probability(raw["price"].to_list())),
        ])
        return self.standardise(raw)
