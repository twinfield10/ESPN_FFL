"""BetOnline game lines: spread, total, moneyline and team totals.

The second adapter, and the one that closes plan 36's step 5 for this book. BetOnline
props were already ingested -- by ``Scripts/scrape_BOL.py`` weekly and
``R/GetSeasonProps.R`` for the season -- while its *game lines* went untouched, which
is the same gap as Pinnacle's and for the same reason: nothing in the repo had a place
to put a game line.

Two things about this feed differ from Pinnacle's and both matter:

* **Zero means absent, not zero.** Every market is present in the payload whether or
  not it is posted, with ``Line`` and ``Point`` at 0. A preseason game carries a
  spread and a total with team totals sitting at 0, and a game with no market at all
  still returns the full nested shape. Reading those as real would put a stream of
  0-point, 0-price rows into the store, all of them de-vigging to nonsense. A market
  counts as posted only when its price is non-zero -- the price, not the point, since
  a pick-em spread of exactly 0 is a real market.
* **No alternates.** This endpoint carries one line per market, so every row here is
  ``isAlt = False``. Pinnacle's alternate ladder has no counterpart.

BetOnline is also fussy about the User-Agent: a shortened one returns 200 with an
empty body rather than an error, which reads as "no games" if you are not looking.
"""

from datetime import datetime
from typing import Dict, List, Optional

import polars as pl

from Scripts.books.base import BaseSportsbook
from Scripts.books.schema import ODDS_SCHEMA, american_to_probability

OFFERING_URL = ("https://api-offering.betonline.ag/api/offering/Sports/"
                "offering-by-league")

#: Sent verbatim. See the module docstring on the User-Agent -- a truncated one is
#: answered with an empty 200 body rather than a refusal.
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "gsetting": "bolsassite",
    "Origin": "https://www.betonline.ag",
    "Referer": "https://www.betonline.ag/sportsbook/football/nfl",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/18.6 Safari/605.1.15"
    ),
    "utc-offset": "240",
}


def _posted(quote: Optional[dict]) -> bool:
    """Whether a market is actually offered.

    Keyed on the *price*, never the point: a spread of exactly 0 is a pick-em and a
    real market, but a price of 0 is the feed's way of saying nothing is posted.
    """
    return bool(quote) and bool(quote.get("Line"))


class BetOnlineSportsbook(BaseSportsbook):
    """BetOnline's NFL game lines.

    Returns the same view names as the Pinnacle adapter, so the puller and the store
    need no per-book branches. ``Pinnacle_Alts``' counterpart is always empty here.
    """

    def __init__(self, sport: str = "football", league: str = "nfl",
                 season: Optional[int] = None):
        super().__init__(sport=sport, league=league)
        self.season = season

    def fetch_odds(self) -> Dict[str, pl.DataFrame]:
        """Pull every posted NFL game and its straight markets."""
        payload = {"Sport": "football", "League": "nfl",
                   "ScheduleText": None, "filterTime": 0}
        body = self.fetch_json(OFFERING_URL, headers=HEADERS, method="POST",
                               body=payload)

        groups = (((body or {}).get("GameOffering") or {})
                  .get("GamesDescription") or [])
        games = [(g.get("GameDate"), g.get("Game")) for g in groups if g.get("Game")]
        print(f"  BetOnline NFL: {len(games)} games offered")

        final = self.transform_to_standard(self._flatten(games))
        return {
            "Pinnacle": final.filter(~pl.col("isAlt")),
            "Pinnacle_Alts": final.head(0),
            "Games": pl.DataFrame(),
            "All_Bets": final,
        }

    def _flatten(self, games: List[tuple]) -> pl.DataFrame:
        """One row per posted price."""
        rows = []
        for game_date, game in games:
            home, away = game.get("HomeTeam"), game.get("AwayTeam")
            if not home or not away:
                continue
            cutoff = game.get("WagerCutOff")
            date = self._date(game_date)

            common = {
                "season": self.season,
                "officialDate": date,
                "startTimeET": cutoff,
                "rotNum": game.get("HomeRotation"),
                "Home": home, "Away": away,
                "gamePeriod": "GAME", "isAlt": False, "propType": None,
            }

            for side, block in (("home", game.get("HomeLine") or {}),
                                ("away", game.get("AwayLine") or {})):
                spread = (block.get("SpreadLine") or {})
                if _posted(spread):
                    # Both sides of one spread must carry the same ``marketLine`` or
                    # they are not each other's de-vig partner. The convention is the
                    # home team's number, matching how Pinnacle keys the market
                    # (``s;0;s;-2.5`` is the home line), so the away row negates its
                    # own point rather than reporting it.
                    point = float(spread["Point"])
                    rows.append({**common, "marketTitle": "Spread",
                                 "sideOf": None, "betSide": side,
                                 "marketLine": point if side == "home" else -point,
                                 "value": point,
                                 "price": float(spread["Line"])})
                money = (block.get("MoneyLine") or {})
                if _posted(money):
                    rows.append({**common, "marketTitle": "Moneyline",
                                 "sideOf": None, "betSide": side,
                                 "marketLine": 0.0, "value": 0.0,
                                 "price": float(money["Line"])})

                team_total = (block.get("TeamTotalLine") or {})
                rows.extend(self._over_under(common, team_total, "TeamTotal",
                                             side_of=side))

            total_block = ((game.get("TotalLine") or {}).get("TotalLine") or {})
            rows.extend(self._over_under(common, total_block, "Total", side_of=None))

        if not rows:
            return pl.DataFrame(schema=ODDS_SCHEMA)
        return pl.DataFrame(rows)

    @staticmethod
    def _over_under(common: dict, block: dict, title: str, side_of) -> List[dict]:
        """The two sides of an over/under, when both are posted."""
        out = []
        point = block.get("Point")
        for designation in ("Over", "Under"):
            quote = block.get(designation) or {}
            if not _posted(quote):
                continue
            out.append({**common, "marketTitle": title, "sideOf": side_of,
                        "betSide": designation.lower(),
                        "marketLine": float(point), "value": float(point),
                        "price": float(quote["Line"])})
        return out

    @staticmethod
    def _date(game_date: Optional[str]) -> Optional[str]:
        """``08/27/2026`` -> ``2026-08-27``, matching the schedule's ``gameday``."""
        if not game_date:
            return None
        try:
            return datetime.strptime(game_date, "%m/%d/%Y").date().isoformat()
        except ValueError:
            return None

    def transform_to_standard(self, raw: pl.DataFrame) -> pl.DataFrame:
        """Add matchup and implied probability, then hand to the base."""
        if raw.is_empty():
            return pl.DataFrame(schema=ODDS_SCHEMA)
        raw = raw.with_columns([
            (pl.col("Away") + pl.lit(" vs. ") + pl.col("Home")).alias("matchup"),
            pl.Series("impProb", american_to_probability(raw["price"].to_list())),
        ])
        return self.standardise(raw)
