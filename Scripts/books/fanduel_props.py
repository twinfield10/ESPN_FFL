"""FanDuel weekly NFL player props: the sportsbook's public JSON, plain HTTP, no login.

Ported from ``NHL-Data/src/nhl/sources/fanduel.py``; the endpoints and app key are
the same, the market vocabulary is football's. Two routes on
``sbapi.va.sportsbook.fanduel.com/api``:

* ``content-managed-page?page=CUSTOM&customPageId=nfl`` lists the board. Games are the
  ``attachments.events`` named ``"Away Team @ Home Team"``.
* ``event-page?eventId=...&tab=...`` returns one tab of a game's markets. Player props
  sit on :data:`TABS`, and a game shows those tabs **only once FanDuel has posted its
  props** -- probed Tuesday 2026-10-13 of week 6, 7 of 14 games had them.

Market shapes (probed 2026-10-13), **one market per player**, unlike hockey:

* Two-way: ``PLAYER_X_<STAT>_<TIER>`` named ``"AJ Barner - Total Receptions"``,
  runners ``"AJ Barner Over"``/``"... Under"`` with the line in ``handicap``. The tier
  suffix (``LOW``/``MEDIUM``/``HIGH``) is FanDuel's own grouping and is ignored.
* Ladder: ``PLAYER_X_ALT_<STAT>_<TIER>``, runners ``"Sam Darnold 175+ Yards"``,
  ``"AJ Barner 3+ Receptions"`` -- one-sided, ``P(X >= k)``.
* Touchdowns: ``ANY_TIME_TOUCHDOWN_SCORER`` and ``TO_SCORE_{2,3,4}+_TOUCHDOWNS``, one
  runner per player -- the anytime ladder rooted at 1.

Period scorers, "either player", longest-reception and game specials are skipped.
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional

import polars as pl
import requests

from Scripts.books.weekly_props import (american_to_decimal, game_lookup, raw_frame,
                                        team_resolver)

logger = logging.getLogger(__name__)

BOOK = "FanDuel"
DIR = "FanDuel"
PREFIX = "FD"
API_BASE = "https://sbapi.va.sportsbook.fanduel.com/api"
#: The public app key the FanDuel website sends with every request.
APP_KEY = "FhMFpcPWXMeyZxOx"
TABS = ("passing-props", "receiving-props", "rushing-props", "td-scorer-props")
#: Seconds between requests: four tabs per game, so ~25 s for a 14-game slate.
PAUSE = 0.4
TIMEOUT = (5, 20)
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/129.0 Safari/537.36")

#: The stat words inside ``PLAYER_X_[ALT_]<STAT>_<TIER>`` -> ESPN stat.
STAT_WORDS: Dict[str, str] = {
    "PASSING_YARDS": "passingYards",
    "PASSING_TOUCHDOWNS": "passingTouchdowns",
    "PASSING_ATTEMPTS": "passingAttempts",
    "PASS_ATTEMPTS": "passingAttempts",
    "PASSING_COMPLETIONS": "passingCompletions",
    "PASS_COMPLETIONS": "passingCompletions",
    "INTERCEPTIONS": "passingInterceptions",
    "PASSING_INTERCEPTIONS": "passingInterceptions",
    "RUSHING_YARDS": "rushingYards",
    "RUSHING_ATTEMPTS": "rushingAttempts",
    "RUSH_ATTEMPTS": "rushingAttempts",
    "RECEIVING_YARDS": "receivingYards",
    "RECEPTIONS": "receivingReceptions",
}
PLAYER_MARKET = re.compile(r"^PLAYER_X_(?P<alt>ALT_)?(?P<stat>[A-Z_]+?)(?:_(?:LOW|MEDIUM|HIGH))?$")
TD_LADDER: Dict[str, int] = {"ANY_TIME_TOUCHDOWN_SCORER": 1, "TO_SCORE_2+_TOUCHDOWNS": 2,
                             "TO_SCORE_3+_TOUCHDOWNS": 3, "TO_SCORE_4+_TOUCHDOWNS": 4}
TWO_WAY_RUNNER = re.compile(r"^(?P<name>.+?) (?P<side>Over|Under)$")
RUNG = re.compile(r"(?P<k>\d+)\+")
LOGO_TEAM = re.compile(r"/nfl/(?P<team>[a-z0-9_]+?)(?:_jersey)?\.png$")
#: FanDuel disambiguates namesakes with a team suffix: "Josh Allen (BUF)".
NAME_SUFFIX = re.compile(r"\s*\([A-Z]{2,3}\)$")


def _get(session: requests.Session, path: str, params: dict) -> dict:
    response = session.get(f"{API_BASE}/{path}", params={"_ak": APP_KEY, **params},
                           timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def board_events(board: dict) -> List[dict]:
    """NFL games on the board: ``{event_id, away, home, start}``."""
    out = []
    for event in ((board.get("attachments") or {}).get("events") or {}).values():
        away, sep, home = str(event.get("name") or "").partition(" @ ")
        if sep:
            out.append({"event_id": str(event.get("eventId")), "away": away.strip(),
                        "home": home.strip(), "start": event.get("openDate")})
    return out


def fetch(sched: pl.DataFrame) -> dict:
    """The board, then every prop tab of each of this week's games.

    Raises:
        requests.HTTPError: The board itself was refused. A refused tab is logged and
            skipped, because one missing tab is a thinner pull rather than no pull.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept": "application/json"})
    board = _get(session, "content-managed-page",
                 {"page": "CUSTOM", "customPageId": "nfl", "timezone": "America/New_York"})
    find = game_lookup(sched)
    events: Dict[str, Dict[str, Any]] = {}
    for event in board_events(board):
        if find(event["away"], event["home"]) is None:
            continue  # another week's game
        tabs = {}
        for tab in TABS:
            time.sleep(PAUSE)
            try:
                tabs[tab] = _get(session, "event-page", {"eventId": event["event_id"], "tab": tab})
            except requests.RequestException as exc:
                logger.warning("[fanduel] %s %s skipped: %s", event["event_id"], tab, exc)
        events[event["event_id"]] = tabs
    return {"board": board, "events": events}


def _price(runner: dict) -> Optional[float]:
    american = ((runner.get("winRunnerOdds") or {}).get("americanDisplayOdds") or {}).get("americanOddsInt")
    return american_to_decimal(american) if american not in (None, 0) else None


def market_rows(market: dict, base: dict, team_of) -> List[dict]:
    """One market's raw rows; empty for markets outside the vocabulary. Pure."""
    kind = str(market.get("marketType") or "")
    if market.get("marketStatus", "OPEN") != "OPEN" or market.get("inPlay"):
        return []
    runners = [r for r in market.get("runners") or [] if r.get("runnerStatus", "ACTIVE") == "ACTIVE"]
    rows: List[dict] = []

    def add(name: str, stat: str, value: float, side: str, source: str, runner: dict) -> None:
        odds = _price(runner)
        if odds is not None and name:
            rows.append({**base, "player_name": NAME_SUFFIX.sub("", name).strip(),
                         "team": team_of(runner), "espn_stat": stat, "value": float(value),
                         "type": side, "odds": odds, "prop_source": source})

    if kind in TD_LADDER:
        for r in runners:
            add(str(r.get("runnerName") or ""), "anytimeTouchdown", TD_LADDER[kind], "Over", "Values", r)
        return rows

    match = PLAYER_MARKET.match(kind)
    stat = STAT_WORDS.get(match.group("stat")) if match else None
    if stat is None:
        return rows
    player = str(market.get("marketName") or "").split(" - ")[0].strip()
    if match.group("alt"):
        for r in runners:
            rung = RUNG.search(str(r.get("runnerName") or ""))
            if rung:
                add(player, stat, int(rung.group("k")), "Over", "Values", r)
    else:
        for r in runners:
            side = TWO_WAY_RUNNER.match(str(r.get("runnerName") or ""))
            if side and r.get("handicap") is not None:
                add(player or side.group("name"), stat, float(r["handicap"]),
                    side.group("side"), "OverUnder", r)
    return rows


def normalize(payload: dict, sched: pl.DataFrame) -> pl.DataFrame:
    """One pull to :data:`~Scripts.books.weekly_props.RAW_SCHEMA` rows. No network."""
    find = game_lookup(sched)
    resolve = team_resolver()
    games = {e["event_id"]: e for e in board_events(payload.get("board") or {})}

    def team_of(runner: dict) -> Optional[str]:
        logo = LOGO_TEAM.search(str(runner.get("logo") or ""))
        return resolve(logo.group("team").replace("_", " ")) if logo else None

    rows: List[dict] = []
    for event_id, tabs in (payload.get("events") or {}).items():
        game = games.get(str(event_id))
        game_id = find(game["away"], game["home"]) if game else None
        if game_id is None:
            continue
        # The same market can sit on two tabs; keep one copy by market id.
        markets: Dict[str, dict] = {}
        for tab in tabs.values():
            markets.update(((tab or {}).get("attachments") or {}).get("markets") or {})
        base = {"book": BOOK, "NFL_game_id": game_id}
        for market in markets.values():
            rows.extend(market_rows(market, base, team_of))
    return raw_frame(rows)
