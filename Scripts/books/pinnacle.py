"""Pinnacle game lines over the public guest API.

The first adapter on the contract, and the one that answers plan 36's central
question: this repo reasons hard about implied team strength and has never scraped a
game line. Every spread and total it has comes from nflverse's schedule file, whose
whole inventory is ``spread_line``, ``total_line`` and ``total`` -- no moneyline, no
team total, no alternate, no price and no history.

**On the geo-block.** Plan 36 named it the one hard blocker: Pinnacle's league routes
403 with ``reason: "location"`` from US addresses, discovery lives on the blocked
route, and NFL's league id was recorded as unknown. Measured 2026-08-27 from a US
residential address, none of that held -- ``/sports``, ``/sports/15/matchups`` and
``/leagues/889/matchups`` all returned 200 -- and the league id was already sitting in
``Scripts/scrape_pinnacle_season.py``, which has used these routes all along.

The workaround is kept anyway, as the fallback rather than the plan. The block is
real, it was rolled out progressively from 2026-07-30 in the repo this is ported from,
and the sport-level feed carries the same games. So: try the league route, fall back
to the sport feed filtered by league id, and classify a location 403 as its own thing
so that if it does arrive it does not read as a transport failure.

**On slate size.** Pinnacle prices about the upcoming week -- 16 games when this was
written, occasionally more when it posts lookahead lines. nflverse had 52 for the same
season. So this is not a replacement for nflverse's breadth; it is depth on the games
that are priced, plus a refresh and a price. Nothing here asserts a game count.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import polars as pl

from Scripts.books.base import BaseSportsbook, FetchFailure
from Scripts.books.schema import ODDS_SCHEMA, american_to_probability

GUEST_API = "https://guest.api.arcadia.pinnacle.com/0.1"

#: NFL in Pinnacle's taxonomy. Not unknown, and never was -- see the module docstring.
NFL_LEAGUE_ID = 889

#: Football. Only needed for the fallback, where the league route is refused and the
#: sport-level feed has to be filtered client-side.
FOOTBALL_SPORT_ID = 15

#: The site's own public guest key, sent by pinnacle.com on every page load. Not a
#: credential; it grants nothing beyond the public odds feed. Same value as
#: ``Scripts/scrape_pinnacle_season.py`` uses.
GUEST_API_KEY = "CmX2KcMrXuFmNg6YFbmTxE0y9CIrOi0R"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept": "application/json",
    "X-API-Key": GUEST_API_KEY,
}

#: Pinnacle's market key prefixes, mapped onto :data:`schema.MARKET_TITLES`.
#:
#: A key is ``<unit>;<period>;<type>[;<line>[;<side>]]`` -- ``s;0;ou;44.5`` is a
#: full-game total, ``s;0;tt;20.5;home`` is the home team's total. The period digit is
#: what makes ``gamePeriod`` meaningful; 0 is the full game.
MARKET_TYPES = {
    "m":  "Moneyline",
    "s":  "Spread",
    "ou": "Total",
    "tt": "TeamTotal",
}

#: Pinnacle's period digit. Only the full game matters to this repo today, but the
#: halves are in the same feed and cost nothing to label correctly.
PERIODS = {"0": "GAME", "1": "H1", "2": "H2"}

ET = ZoneInfo("America/New_York")


class PinnacleSportsbook(BaseSportsbook):
    """Pinnacle's NFL game lines: spread, total, moneyline, team totals, alternates.

    Returns four views, and returns all four keys even when the pull is empty:

    * ``Pinnacle`` -- the main line of each market, one row per side.
    * ``Pinnacle_Alts`` -- alternate spreads and totals.
    * ``Games`` -- one row per priced game, for joining.
    * ``All_Bets`` -- everything, which is what gets stored.
    """

    def __init__(self, sport: str = "football", league: str = "nfl",
                 season: Optional[int] = None):
        super().__init__(sport=sport, league=league)
        self.season = season
        self.used_fallback = False

    # --- fetching ----------------------------------------------------------

    def _league_route(self):
        """The direct route. Geo-blocked in the other repo, reachable here."""
        matchups = self.fetch_json(f"{GUEST_API}/leagues/{NFL_LEAGUE_ID}/matchups",
                                   headers=HEADERS)
        markets = self.fetch_json(f"{GUEST_API}/leagues/{NFL_LEAGUE_ID}/markets/straight",
                                  headers=HEADERS)
        return matchups, markets

    def _sport_route(self):
        """The fallback: same games, filtered client-side rather than server-side.

        The sport feed carries every football league -- NCAA, the CFL, the FCS -- so
        the league filter that the blocked route applied for us has to be applied
        here instead.
        """
        matchups = self.fetch_json(
            f"{GUEST_API}/sports/{FOOTBALL_SPORT_ID}/matchups?brandId=0",
            headers=HEADERS)
        markets = self.fetch_json(
            f"{GUEST_API}/sports/{FOOTBALL_SPORT_ID}/markets/straight",
            headers=HEADERS)
        keep = {m["id"] for m in matchups
                if (m.get("league") or {}).get("id") == NFL_LEAGUE_ID}
        matchups = [m for m in matchups if m["id"] in keep]
        markets = [r for r in markets if r.get("matchupId") in keep]
        return matchups, markets

    def fetch_odds(self) -> Dict[str, pl.DataFrame]:
        """Pull the league's games and their straight markets.

        Returns:
            Dict[str, pl.DataFrame]: The four views named in the class docstring.
        """
        matchups, markets = self._league_route()
        if not matchups and self.last_failure in (FetchFailure.GEO_BLOCK,
                                                  FetchFailure.IP_BLOCK):
            # Only a *block* is worth a second route, which is the whole reason the
            # fallback exists -- the sport-level feed carries the same games and is
            # not geo-restricted. Falling back on a transport failure or a 5xx just
            # spends the same broken request twice: when DNS is dead for the league
            # route it is dead for the sport route too, and the retries make that
            # twelve wasted seconds rather than one. An empty feed with no failure at
            # all means the league has no games, which a second route cannot fix
            # either.
            print(f"  league route refused ({self.last_failure.value}); "
                  f"falling back to the sport-level feed")
            self.used_fallback = True
            matchups, markets = self._sport_route()

        games = [m for m in matchups if m.get("type") == "matchup"]
        print(f"  Pinnacle NFL: {len(games)} priced games, {len(markets)} market rows"
              + (" (via sport-level fallback)" if self.used_fallback else ""))

        raw = self._flatten(games, markets)
        final = self.transform_to_standard(raw)
        return self._views(final, games)

    # --- shaping -----------------------------------------------------------

    def _flatten(self, games: List[dict], markets: List[dict]) -> pl.DataFrame:
        """One row per posted price, joined to its game."""
        by_id = {g["id"]: g for g in games}
        rows = []
        for market in markets:
            game = by_id.get(market.get("matchupId"))
            if game is None:
                continue
            parsed = self._parse_key(market.get("key", ""))
            if parsed is None:
                continue
            period, market_title, market_line, key_side = parsed

            participants = {p.get("alignment"): p.get("name")
                            for p in game.get("participants", [])}
            start = game.get("startTime")
            start_et = (datetime.fromisoformat(start.replace("Z", "+00:00"))
                        .astimezone(ET)) if start else None

            for price in market.get("prices", []):
                designation = price.get("designation")
                # A team total names its team in the *key*; the designation is only
                # over/under. That goes in ``sideOf`` rather than being mashed into
                # ``betSide``, because it is what pairs a price with its de-vig
                # partner -- and when both teams are priced at the same number, which
                # is any pick-em, nothing else distinguishes the four rows.
                rows.append({
                    "season": self.season,
                    "officialDate": start_et.date().isoformat() if start_et else None,
                    "startTimeET": start_et.isoformat() if start_et else None,
                    "rotNum": game.get("rotation"),
                    "Home": participants.get("home"),
                    "Away": participants.get("away"),
                    "marketTitle": market_title,
                    "gamePeriod": period,
                    "betSide": designation,
                    "sideOf": key_side if market_title == "TeamTotal" else None,
                    "marketLine": market_line,
                    "value": price.get("points"),
                    "price": price.get("price"),
                    "isAlt": bool(market.get("isAlternate", False)),
                    "propType": None,
                })
        if not rows:
            return pl.DataFrame(schema=ODDS_SCHEMA)
        return pl.DataFrame(rows)

    @staticmethod
    def _parse_key(key: str):
        """``s;0;tt;20.5;home`` -> ``("GAME", "TeamTotal", 20.5, "home")``.

        The line comes out of the *key*, not out of a price's ``points``. On a spread
        those differ by sign between the two sides, and it is the key's number that
        says which market they belong to.

        Returns:
            tuple | None: ``(period, marketTitle, marketLine, sideOf)``, or None for a
            market family this repo does not model -- which is most of them, and
            skipping quietly is correct because Pinnacle posts far more than four.
        """
        parts = key.split(";")
        if len(parts) < 3:
            return None
        period = PERIODS.get(parts[1])
        title = MARKET_TYPES.get(parts[2])
        if period is None or title is None:
            return None
        try:
            line = float(parts[3]) if len(parts) > 3 else 0.0
        except ValueError:
            return None
        side = parts[4] if len(parts) > 4 else None
        return period, title, line, side

    def transform_to_standard(self, raw: pl.DataFrame) -> pl.DataFrame:
        """Add the derived columns, then hand to the base for identity and de-vig."""
        if raw.is_empty():
            return pl.DataFrame(schema=ODDS_SCHEMA)

        raw = raw.with_columns([
            (pl.col("Away") + pl.lit(" vs. ") + pl.col("Home")).alias("matchup"),
            pl.Series("impProb", american_to_probability(raw["price"].to_list())),
        ])
        # A moneyline has no number; 0.0 rather than null keeps ``marketLine`` in the
        # change-detection grain without a null-comparison special case.
        raw = raw.with_columns(
            pl.when(pl.col("marketTitle") == "Moneyline")
              .then(pl.lit(0.0))
              .otherwise(pl.col("value")).alias("value"))
        return self.standardise(raw)

    def _views(self, final: pl.DataFrame, games: List[dict]) -> Dict[str, pl.DataFrame]:
        """Split one fetch into the four named frames."""
        game_rows = pl.DataFrame([{
            "matchupId": g["id"],
            "rotNum": g.get("rotation"),
            "startTime": g.get("startTime"),
            "Home": next((p["name"] for p in g.get("participants", [])
                          if p.get("alignment") == "home"), None),
            "Away": next((p["name"] for p in g.get("participants", [])
                          if p.get("alignment") == "away"), None),
        } for g in games]) if games else pl.DataFrame()

        return {
            "Pinnacle": final.filter(~pl.col("isAlt")),
            "Pinnacle_Alts": final.filter(pl.col("isAlt")),
            "Games": game_rows,
            "All_Bets": final,
        }
