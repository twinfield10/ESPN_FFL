"""DraftKings weekly NFL player props, read through ESPN -- **lines only, no prices**.

Ported from ``NHL-Data/src/nhl/sources/espn_odds.py``, which reads DraftKings' props
from the ``propBets`` collection ESPN links on DraftKings' odds item. On hockey each
item carries ``odds.american``. **On football it does not**: probed 2026-10-13, every
one of 1,000 week-6 items held ``current.target`` (the line or rung) and nothing else
-- no price field at any depth. DraftKings' own API answers this machine with an
Akamai "Access Denied" on every state host, so the price is not reachable without a
browser session.

What survives is the **line**, and that is most of the information for the stats
whose line moves: ``Scripts/market.py`` measured ``|q - 0.5|`` at a median of 0.02 on
the archive, because a book posts the line at the median and the price only says
which side of it. So each two-way line is stored as a pair at even money and goes
through the same conversion as every other book -- exact for yardage, an inversion of
``P(N >= k) = 0.5`` for a count.

Three things are **deliberately left out** because the line carries no opinion there:

* Ladders ("Receptions Milestones", ...) -- a rung without a price is just a number.
* Touchdowns -- the anytime market has no line at all, only a price.
* Passing touchdowns and interceptions -- the line is 1.5 and 0.5 for nearly every
  quarterback, so the opinion lives entirely in the price ESPN drops.

Athlete names and positions come from ESPN's own rosters, so unlike the other three
books this one needs no position inference and no name matching.
"""

import logging
import re
import time
from typing import Dict, List, Optional

import polars as pl
import requests

from Scripts.books.weekly_props import game_lookup, raw_frame, team_resolver

logger = logging.getLogger(__name__)

BOOK = "DraftKings"
DIR = "DraftKings"
PREFIX = "DK"
SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
ROSTER = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/roster"
ODDS = ("https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{id}"
        "/competitions/{id}/odds")
PROVIDER = "DraftKings"
PAUSE = 0.3
TIMEOUT = (5, 30)

#: ESPN prop type -> ESPN stat, for the two-way markets whose line moves.
TWO_WAY: Dict[str, str] = {
    "Total Passing Yards (incl. overtime)": "passingYards",
    "Total Passing Attempts (incl. overtime)": "passingAttempts",
    "Total Pass Completions (incl. overtime)": "passingCompletions",
    "Total Rushing Yards (incl. overtime)": "rushingYards",
    "Total Carries (incl. overtime)": "rushingAttempts",
    "Total Receiving Yards (incl. overtime)": "receivingYards",
    "Total Receptions (incl. overtime)": "receivingReceptions",
}
#: ESPN positions -> the keys the market model is fitted on.
POSITIONS = {"QB": "QB", "RB": "RB", "FB": "RB", "WR": "WR", "TE": "TE"}
#: Even money, as decimal odds. See the module docstring for why.
EVEN = 2.0


def _get(session: requests.Session, url: str, **params) -> dict:
    response = session.get(url, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def _paged(session: requests.Session, url: str) -> List[dict]:
    first = _get(session, url, limit=1000)
    items = list(first.get("items") or [])
    for page in range(2, int(first.get("pageCount") or 1) + 1):
        time.sleep(PAUSE)
        items.extend(_get(session, url, limit=1000, page=page).get("items") or [])
    return items


def fetch(sched: pl.DataFrame) -> dict:
    """ESPN's week scoreboard, DraftKings' ``propBets`` per game, and both rosters."""
    session = requests.Session()
    week = int(sched["week"][0])
    season = int(str(sched["NFL_game_id"][0])[:4])
    board = _get(session, SCOREBOARD, week=week, seasontype=2, dates=season)
    games, rosters = [], {}
    for event in board.get("events") or []:
        comp = (event.get("competitions") or [{}])[0]
        teams = {c.get("homeAway"): c.get("team") or {} for c in comp.get("competitors") or []}
        odds = _get(session, ODDS.format(id=event["id"]), limit=100)
        props: List[dict] = []
        for item in odds.get("items") or []:
            if (item.get("provider") or {}).get("name") == PROVIDER and item.get("propBets"):
                props = _paged(session, item["propBets"]["$ref"].replace("http://", "https://", 1))
        for team in teams.values():
            if team.get("id") and team["id"] not in rosters:
                time.sleep(PAUSE)
                rosters[team["id"]] = _get(session, ROSTER.format(team_id=team["id"]))
        games.append({"event_id": event["id"],
                      "away": (teams.get("away") or {}).get("displayName"),
                      "home": (teams.get("home") or {}).get("displayName"),
                      "props": props})
        time.sleep(PAUSE)
    return {"games": games, "rosters": rosters}


def roster_athletes(rosters: Dict[str, dict]) -> Dict[str, tuple]:
    """``athlete_id -> (name, team abbreviation, position)`` across every roster."""
    out = {}
    for roster in rosters.values():
        team = (roster.get("team") or {}).get("abbreviation")
        for group in roster.get("athletes") or []:
            for athlete in group.get("items") or []:
                pos = ((athlete.get("position") or {}).get("abbreviation"))
                out[str(athlete.get("id"))] = (athlete.get("fullName") or athlete.get("displayName"),
                                               team, POSITIONS.get(pos))
    return out


def _athlete_id(item: dict) -> Optional[str]:
    match = re.search(r"/athletes/(\d+)", (item.get("athlete") or {}).get("$ref", ""))
    return match.group(1) if match else None


def normalize(payload: dict, sched: pl.DataFrame) -> pl.DataFrame:
    """One pull to :data:`~Scripts.books.weekly_props.RAW_SCHEMA` rows. No network."""
    find = game_lookup(sched)
    resolve = team_resolver()
    athletes = roster_athletes(payload.get("rosters") or {})
    rows: List[dict] = []
    for game in payload.get("games") or []:
        game_id = find(game.get("away"), game.get("home"))
        if game_id is None:
            continue
        seen = set()
        for item in game.get("props") or []:
            stat = TWO_WAY.get((item.get("type") or {}).get("name"))
            athlete = _athlete_id(item)
            target = (((item.get("current") or {}).get("target")
                       or (item.get("open") or {}).get("target") or {}).get("value"))
            if stat is None or athlete is None or target is None or (athlete, stat) in seen:
                continue
            name, team, position = athletes.get(athlete, (None, None, None))
            if not name:
                continue
            # The first item of a pair is the line; its twin repeats it.
            seen.add((athlete, stat))
            for side in ("Over", "Under"):
                rows.append({"book": BOOK, "NFL_game_id": game_id, "player_name": name,
                             "team": resolve(team), "position": position, "espn_stat": stat,
                             "value": float(target), "type": side, "odds": EVEN,
                             "impProb": 1.0 / EVEN, "prop_source": "OverUnder"})
    return raw_frame(rows)
