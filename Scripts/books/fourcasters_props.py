"""4Casters weekly NFL player props: the exchange's ``NFL-PROPS`` order book.

Ported from the props half of ``NHL-Data/src/nhl/sources/fourcasters.py``; the login
is :class:`Scripts.books.fourcasters.FourCastersSportsbook`'s, so the credentials
are the same ``CAST4_USER`` / ``CAST4_PASS`` the game-line pull already reads.

``POST /exchange/getOrderbook {"leagueRequested": "NFL-PROPS"}`` returns one
"special" game per player market -- ``eventName`` ``"JAYLEN WADDLE (TOTAL RECEIVING
YARDS)"``, ``participants`` the two teams -- with ``over``/``under`` order books keyed
by line, like a game total, and ``mainTotal`` naming the main rung. Probed
2026-10-13: 64 player markets, almost all on Thursday's game. It is the thinnest of
the four by a distance and is here because it costs one request.

**A price is only real at a size.** Each side is walked to :data:`PROP_UNIT_SIZE`
of exposure (:func:`vwap`) and the 1% commission is added back, so the stored price
is what a taker pays; a side that cannot fill is omitted. The main rung becomes the
two-way line; the over side of every rung a ladder step at ``floor(line) + 1``.
``TOTAL TOUCHDOWNS`` is the anytime ladder.
"""

import logging
import math
import re
from typing import Dict, Iterable, List, Optional, Tuple

import polars as pl

from Scripts.books.fourcasters import BASE_URL, FourCastersSportsbook
from Scripts.books.weekly_props import game_lookup, raw_frame

logger = logging.getLogger(__name__)

BOOK = "4Casters"
DIR = "FourCasters"
PREFIX = "C4"
PROPS_LEAGUE = "NFL-PROPS"
#: Exposure a prop price must fill; prop books rest $25-$850 a side (NHL-Data).
PROP_UNIT_SIZE = 50.0
#: Taker commission, added back as ``q + c * (1 - q)`` (NHL-Data, from 2026-10-12).
COMMISSION = 0.01

#: ``eventName`` suffix -> ESPN stat.
PROP_TYPES: Dict[str, str] = {
    "TOTAL TOUCHDOWNS": "anytimeTouchdown",
    "TOTAL RECEIVING YARDS": "receivingYards",
    "TOTAL RECEPTIONS": "receivingReceptions",
    "TOTAL RUSHING YARDS": "rushingYards",
    "TOTAL RUSHING ATTEMPTS": "rushingAttempts",
    "TOTAL PASSING YARDS": "passingYards",
    "TOTAL TOUCHDOWN PASSES": "passingTouchdowns",
    "TOTAL PASSING TOUCHDOWNS": "passingTouchdowns",
    "TOTAL PASS ATTEMPTS": "passingAttempts",
    "TOTAL PASSING ATTEMPTS": "passingAttempts",
    "TOTAL COMPLETIONS": "passingCompletions",
    "TOTAL PASS COMPLETIONS": "passingCompletions",
    "TOTAL INTERCEPTIONS": "passingInterceptions",
    "TOTAL INTERCEPTIONS THROWN": "passingInterceptions",
}
PROP_EVENT = re.compile(r"^(?P<name>.+?)\s*\((?P<kind>[^()]+)\)\s*$")


def fetch(sched: pl.DataFrame) -> dict:
    """The NFL props order book. Raises if the login is refused or unconfigured."""
    book = FourCastersSportsbook()
    token = book._authenticate()
    return book.fetch_json(f"{BASE_URL}/exchange/getOrderbook", method="POST",
                           headers={"Authorization": token, "Content-Type": "application/json"},
                           body={"leagueRequested": PROPS_LEAGUE}) or {}


def _implied(american: float) -> float:
    american = float(american)
    return 100.0 / (american + 100.0) if american > 0 else -american / (-american + 100.0)


def vwap(levels: Iterable[Tuple[float, float]], unit_size: float = PROP_UNIT_SIZE) -> Optional[float]:
    """The probability actually available for ``unit_size`` of exposure, commission in.

    Best-first is American odds descending on both signs. An underdog risks the
    unit; a favourite bets to win it. None when the book cannot fill all of it.
    """
    book = sorted(((float(o), float(a)) for o, a in levels if a and float(a) > 0), reverse=True)
    if not book:
        return None
    best = book[0][0]
    target = unit_size if best > 0 else unit_size * abs(best) / 100.0
    filled = weighted = 0.0
    for odds, available in book:
        take = min(available, target - filled)
        if take <= 0:
            break
        weighted += take * _implied(odds)
        filled += take
    if filled < target - 1e-6:
        return None
    q = weighted / filled
    return min(q + COMMISSION * (1.0 - q), 0.999)


def _quote(orders) -> Optional[float]:
    return vwap((o["odds"], o.get("sumUntaken") or 0.0) for o in orders or []
                if o and o.get("odds") is not None)


def normalize(payload: dict, sched: pl.DataFrame) -> pl.DataFrame:
    """One pull to :data:`~Scripts.books.weekly_props.RAW_SCHEMA` rows. No network."""
    find = game_lookup(sched)
    rows: List[dict] = []
    for game in ((payload or {}).get("data") or {}).get("games") or []:
        if game.get("live") or game.get("ended"):
            continue
        match = PROP_EVENT.match(str(game.get("eventName") or ""))
        stat = PROP_TYPES.get(match.group("kind").strip().upper()) if match else None
        teams = [p.get("longName") for p in game.get("participants") or []]
        game_id = find(*teams[:2]) if stat and len(teams) >= 2 else None
        if game_id is None:
            continue
        base = {"book": BOOK, "NFL_game_id": game_id, "player_name": match.group("name").title(),
                "espn_stat": stat}
        main = float(game.get("mainTotal") or 0) or None
        for key, orders in (game.get("over") or {}).items():
            line = float(key)
            p_over = _quote(orders)
            if p_over is None:
                continue
            rows.append({**base, "value": float(math.floor(line) + 1), "type": "Over",
                         "odds": 1.0 / p_over, "impProb": p_over, "prop_source": "Values"})
            p_under = _quote((game.get("under") or {}).get(key))
            if stat != "anytimeTouchdown" and line == main and p_under is not None:
                for side, p in (("Over", p_over), ("Under", p_under)):
                    rows.append({**base, "value": line, "type": side, "odds": 1.0 / p,
                                 "impProb": p, "prop_source": "OverUnder"})
    return raw_frame(rows)
