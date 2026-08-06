"""The draft market: ADP, auction values, and ESPN's season projection.

All of it comes from **one** ``view=kona_player_info`` request. Measured on 2026
pre-season: 1,000 players in 0.24s, every one with a real ADP. That single
response carries

* ``ownership.averageDraftPosition`` -- live market ADP, fractional (``1.83``)
* ``ownership.auctionValueAverage`` -- live market auction dollars (``63.52``)
* ``draftRanksByRankType.{PPR,STANDARD}`` -- ESPN's own board and auction value
* ``ownership.percentOwned``
* the full 45-stat **season projection**, plus the prior season's actual total
* ``player.id`` -- a real join key, which the name-based pipeline lacks

``espn_api`` cannot be used for this directly: its ``Player`` class parses
``percent_owned`` but drops ``draftRanksByRankType`` and
``ownership.averageDraftPosition`` entirely (``football/player.py``). It *is* used
for the stat translation, though -- constructing a ``Player`` from each entry
yields ``stats[0]['projected_breakdown']`` keyed by ``colName`` rather than by
numeric stat id, which is how the rest of this repo reads projections.

Why this is also the player universe: ``espn_season_projections`` builds its
universe from team rosters plus ``league.free_agents(size=60, position=...)``
across 11 positions -- 329 players, at 4.8s per ``free_agents`` call. This request
reaches 1,000 in a fifth of a second, and a 14-team league drafts 210.

ADP is a global ESPN number, so the response is cached per season and shared
across leagues. Any configured league's credentials will serve it.
"""

import json
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from espn_api.football import Player

from Scripts.season_projections import (
    DEFAULT_GAMES,
    GAMES_PLAYED_KEY,
    PER_GAME_IN_SEASON_ROW,
    normalise_name,
)

#: How many players to request. High enough to exhaust the pool rather than
#: truncate it: an offence-only league returns ~1,000 and stops, but an IDP league's
#: pool is 2,503 and a limit of 1,000 silently starves the *offence* -- measured on
#: GOP Degenerates, a 1,000 cap returned only 134 WRs and 50 QBs because individual
#: defenders crowded them out. Asking for more than exists just returns what exists.
DEFAULT_LIMIT = 3000

#: Which of ESPN's two published draft boards to read. Our leagues are mostly PPR
#: or PPR-ish, and the two agree closely at the top anyway. ``ownership``-derived
#: ADP is not split by rank type at all -- it is one market-wide number.
DEFAULT_RANK_TYPE = "PPR"

#: Player statuses to include. Pre-draft every player is a free agent, but asking
#: for rostered players too means this works mid-season and in keeper leagues.
PLAYER_STATUSES = ("FREEAGENT", "WAIVERS", "ONTEAM")

#: ``statSourceId``: 0 is what happened, 1 is what ESPN projects.
STAT_SOURCE_ACTUAL = 0
STAT_SOURCE_PROJECTED = 1

#: ``statSplitTypeId``: 0 is the season row. 1 is per-game, 2 is a split we ignore.
STAT_SPLIT_SEASON = 0

#: Cache of ``(season, league_id, rank_type, limit) -> DataFrame``.
#:
#: The league id is in the key, and it was not at first. ADP itself *is*
#: league-independent -- ``averageDraftPosition`` is one market-wide number -- so
#: the first version cached on season alone and boasted about serving nine leagues
#: from one request. But the **pool** the endpoint returns is league-dependent: it
#: reflects that league's roster slots. GOP Degenerates has IDP slots and its own
#: response carries 129 defensive ends and 125 linebackers; every other league's
#: does not. Sharing the cache across leagues gave the one IDP league a board with
#: no individual defenders on it. Nine requests cost about two seconds. Correctness
#: is worth two seconds.
_MARKET_CACHE: Dict[tuple, pd.DataFrame] = {}


def reset_cache() -> None:
    """Drop the cached market response. Mainly for tests and long sessions."""
    _MARKET_CACHE.clear()


def _request_player_pool(
    league,
    limit: int = DEFAULT_LIMIT,
    rank_type: str = DEFAULT_RANK_TYPE,
) -> List[Dict[str, Any]]:
    """Issue the ``kona_player_info`` request and return the raw entries.

    Args:
        league: A fetched ESPN ``League`` -- supplies ``endpoint`` and ``cookies``,
            both set by :func:`Scripts.fetch_utils.fetch_league`.
        limit: Maximum players to return.
        rank_type: ``"PPR"`` or ``"STANDARD"``, used to sort the response.

    Returns:
        list: One dict per player, each with a ``player`` key.

    Raises:
        requests.HTTPError: On a non-2xx response.
        ValueError: When the response carries no ``players`` array, which is what
            an auth failure looks like here rather than a 401.
    """
    filters = {
        "players": {
            "filterStatus": {"value": list(PLAYER_STATUSES)},
            "limit": limit,
            "sortDraftRanks": {
                "sortPriority": 1, "sortAsc": True, "value": rank_type,
            },
        }
    }
    response = requests.get(
        league.endpoint,
        params={"view": "kona_player_info"},
        cookies=league.cookies,
        headers={"x-fantasy-filter": json.dumps(filters)},
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    if isinstance(payload, list):
        payload = payload[0]
    if "players" not in payload:
        raise ValueError(
            f"kona_player_info returned no 'players' array for league "
            f"{league.league_id} {league.year}; keys were {sorted(payload)}. "
            f"Expired cookies usually surface this way rather than as a 401."
        )
    return payload["players"]


def _season_stat_row(entry: Dict[str, Any], season: int, source: int) -> Dict[str, Any]:
    """Find one season-level stat row in a raw player entry.

    Args:
        entry: A raw ``players[]`` element.
        season: Season year to match.
        source: :data:`STAT_SOURCE_ACTUAL` or :data:`STAT_SOURCE_PROJECTED`.

    Returns:
        dict: The stat row, or ``{}`` when absent.
    """
    for row in entry.get("player", {}).get("stats") or []:
        if (row.get("seasonId") == season
                and row.get("statSourceId") == source
                and row.get("statSplitTypeId") == STAT_SPLIT_SEASON):
            return row
    return {}


def _parse_entry(entry: Dict[str, Any], season: int, rank_type: str) -> Optional[Dict[str, Any]]:
    """Turn one raw entry into a board row.

    The projection is read through ``espn_api``'s ``Player`` so its 45 stats arrive
    keyed by ``colName``; the market fields are read from the raw payload because
    ``Player`` discards them.

    Args:
        entry: A raw ``players[]`` element.
        season: Season year.
        rank_type: Which published board to read ranks from.

    Returns:
        dict | None: A row, or None if the entry has no usable name.
    """
    # pro_team_schedule is deliberately omitted: it costs a second request and the
    # board does not show opponents. Bye weeks would come from there if wanted.
    player = Player(entry, season)
    if not player.name:
        return None

    raw = entry.get("player", {}) or {}
    ownership = raw.get("ownership") or {}
    ranks = (raw.get("draftRanksByRankType") or {}).get(rank_type, {}) or {}

    projected = player.stats.get(0, {}) or {}
    breakdown = projected.get("projected_breakdown") or {}
    games = float(breakdown.get(GAMES_PLAYED_KEY) or DEFAULT_GAMES) or DEFAULT_GAMES

    row: Dict[str, Any] = {
        "player_id": int(player.playerId),
        "player_name": _scalar_or_none(player.name),
        "name_key": normalise_name(player.name),
        "primaryPosition": _scalar_or_none(player.position),
        "pro_team": _scalar_or_none(player.proTeam),
        "eligible_slots": [str(s) for s in player.eligibleSlots],
        "injury_status": _scalar_or_none(player.injuryStatus),
        "injured": bool(raw.get("injured", False)),
        # Which fantasy team holds them, 0 for none. Note ESPN pre-fills rosters
        # before a draft, so pre-draft this is not evidence of being drafted --
        # `league.draft` is. Useful for keeper leagues and mid-season boards.
        "on_team_id": int(entry.get("onTeamId") or 0),

        # --- the market -------------------------------------------------
        # averageDraftPosition is the actual average pick across ESPN leagues, so
        # it is fractional and is the right number for "is he falling?".
        # draftRanksByRankType.rank is ESPN's editorial board -- an integer, and a
        # different thing. Both are kept; the board sorts on the former.
        "adp": _positive_or_none(ownership.get("averageDraftPosition")),
        "auction_value": _positive_or_none(ownership.get("auctionValueAverage")),
        "espn_draft_rank": ranks.get("rank"),
        "espn_auction_value": ranks.get("auctionValue"),
        "percent_owned": _positive_or_none(ownership.get("percentOwned")),
        "adp_change": ownership.get("averageDraftPositionPercentChange"),

        # League-specific rather than market-wide: what this league would pay, and
        # what a keeper costs. Present even pre-draft.
        "league_auction_value": entry.get("draftAuctionValue"),
        "keeper_value": entry.get("keeperValue"),

        # --- projection -------------------------------------------------
        "games": games,
        "ESPN_projected_total": float(projected.get("projected_points") or 0.0),
        "prior_season_points": float(
            _season_stat_row(entry, season - 1, STAT_SOURCE_ACTUAL).get("appliedTotal")
            or 0.0
        ),
    }

    for stat, value in breakdown.items():
        if not isinstance(value, (int, float)):
            continue
        # ESPN stores these three per-game inside the season row. Same conversion
        # espn_season_projections applies -- see that module's docstring.
        if stat in PER_GAME_IN_SEASON_ROW:
            value = value * games
        row[f"ESPN_{stat}"] = float(value)

    return row


def _scalar_or_none(value):
    """Normalise an ``espn_api`` field that may come back as an empty list.

    ``espn_api.football.utils.json_parsing`` ends with
    ``return results[0] if results else results`` -- so a key that is simply absent
    yields the empty **list**, not None. A healthy player has no ``injuryStatus``
    key, so ``player.injuryStatus`` is ``[]`` for most of the pool, which makes the
    column mixed-type and fails the parquet write with
    ``ArrowTypeError: Expected bytes, got a 'list' object``.

    Args:
        value: A possibly-empty-list scalar from ``json_parsing``.

    Returns:
        The value as a string, or None when it is absent.
    """
    if value is None or (isinstance(value, (list, tuple, dict)) and not value):
        return None
    if isinstance(value, (list, tuple)):
        return str(value[0])
    return str(value)


def _positive_or_none(value):
    """ESPN uses 0 and -1 for "no data" on ownership fields.

    Args:
        value: A raw ownership number.

    Returns:
        float | None: The value, or None when it is missing or a sentinel.
    """
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def fetch_draft_market(
    league,
    season: Optional[int] = None,
    *,
    limit: int = DEFAULT_LIMIT,
    rank_type: str = DEFAULT_RANK_TYPE,
    refresh: bool = False,
) -> pd.DataFrame:
    """The draftable player universe with market prices and ESPN projections.

    Cached per ``(season, league_id, rank_type, limit)`` -- see
    :data:`_MARKET_CACHE` for why the league id belongs in the key even though ADP
    does not vary by league.

    Args:
        league: A fetched ESPN ``League``, for its endpoint and cookies.
        season: Season year. Defaults to ``league.year``.
        limit: Maximum players to request.
        rank_type: ``"PPR"`` or ``"STANDARD"``.
        refresh: Bypass the cache and re-request.

    Returns:
        pd.DataFrame: One row per player -- identity, ``adp``, ``auction_value``,
        ``percent_owned``, ``injury_status``, ``prior_season_points``,
        ``ESPN_projected_total`` and ``ESPN_<stat>`` columns. Sorted by ADP, with
        players who have no ADP last.

    Raises:
        requests.HTTPError: On a non-2xx response.
        ValueError: When the response carries no player array.
    """
    season = int(league.year if season is None else season)
    key = (season, int(league.league_id), rank_type, limit)
    if not refresh and key in _MARKET_CACHE:
        return _MARKET_CACHE[key].copy()

    entries = _request_player_pool(league, limit=limit, rank_type=rank_type)
    rows = [r for r in (_parse_entry(e, season, rank_type) for e in entries)
            if r is not None]
    market = pd.DataFrame(rows)

    if not market.empty:
        market = market.drop_duplicates(subset=["player_id"])
        # ADP-less players sort last rather than first: they are the deep end of
        # the pool, not the top of the board.
        market = market.sort_values(
            "adp", ascending=True, na_position="last"
        ).reset_index(drop=True)

    _MARKET_CACHE[key] = market
    return market.copy()


def market_summary(market: pd.DataFrame) -> str:
    """A one-line description of a market pull, for refresh output.

    Args:
        market: A :func:`fetch_draft_market` frame.

    Returns:
        str: e.g. ``"1000 players, 1000 with ADP (WR 359, RB 236, ...)"``.
    """
    if market.empty:
        return "0 players"
    with_adp = int(market["adp"].notna().sum())
    by_pos = market["primaryPosition"].value_counts()
    breakdown = ", ".join(f"{pos} {n}" for pos, n in by_pos.items())
    return f"{len(market)} players, {with_adp} with ADP ({breakdown})"
