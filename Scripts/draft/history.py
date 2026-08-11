"""Draft pick history: who took whom, in which round, for how much.

One ``view=mDraftDetail`` request per league-season returns every pick ever made
in that league, back to its first year -- 5,748 picks across the nine configured
leagues, 2016-2025, with no gaps and no failed seasons. It is the cheapest
historical data this repo has: ESPN keeps it forever and it never changes once a
draft is over.

The pick payload is thin on purpose -- ``playerId``, ``roundId``,
``overallPickNumber``, ``bidAmount``, ``keeper``, ``autoDraftTypeId`` and a
``teamId``. Two joins make it readable:

* **Players.** ``seasons/{year}/players?view=players_wl`` returns that season's
  whole universe with ``fullName``, ``defaultPositionId`` and ``proTeamId``. It is
  league-independent, so one request per *season* serves all nine leagues, and it
  resolves 100% of drafted ids including team defences.
* **Owners.** Not from the pick. ``memberId`` on a pick looks like the drafter and
  is not: in six of the older league-seasons every pick in the draft carries the
  same member GUID, which would have credited one manager with all 96 picks of the
  2016 draft. The ``teamId`` is right in every season, so the owner is resolved
  through ``teams[].primaryOwner`` and named from the league's ``members`` block,
  pooled across seasons because ESPN drops the name from old payloads while
  keeping the GUID stable.

Rookie status is the one field ESPN cannot supply. Its player universe carries a
``Rookie`` eligible slot, but only from about 2019 and never for the seasons this
repo has the most history in -- 0 rookies in 2016-2018. ``years_exp`` from
nflverse is complete but has its own hole: every 2023 rookie has a null
``espn_id`` in that season's roster file. Pooling ``entry_year`` over *all*
seasons by ``espn_id`` closes both -- a 2023 rookie is resolved through his 2024
roster row -- and matches 100% of skill-position picks.

See ``docs/plans/23-owner-tendencies.md``.
"""

import json
from typing import Dict, Iterable, List, Optional, Sequence

import polars as pl
import requests
from espn_api.football.constant import PRO_TEAM_MAP

#: ESPN's ``defaultPositionId``, which is **not** the lineup-slot map in
#: ``espn_api.football.constant.POSITION_MAP`` -- there 2 is RB and 4 is WR, here
#: 2 is RB and 4 is TE. Verified against the 2025 universe by cross-checking each
#: id's ``eligibleSlots``; these fourteen cover every one of the 5,748 picks.
DEFAULT_POSITION_MAP: Dict[int, str] = {
    1: "QB", 2: "RB", 3: "WR", 4: "TE", 5: "K", 7: "P", 9: "DT", 10: "DE",
    11: "LB", 12: "CB", 13: "S", 14: "HC", 15: "TQB", 16: "D/ST",
}

#: Pure (non-flex) lineup slots, for the fallback when ``defaultPositionId`` is
#: one this map does not know. Ordered, so the first hit wins.
FALLBACK_SLOT_POSITIONS: List[tuple] = [
    (0, "QB"), (2, "RB"), (4, "WR"), (6, "TE"), (17, "K"), (16, "D/ST"),
    (8, "DT"), (9, "DE"), (10, "LB"), (12, "CB"), (13, "S"), (18, "P"),
]

FANTASY_HOST = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl"

#: Cache of ``season -> {player_id: metadata}``. The universe is league-
#: independent, so a nine-league refresh pays for each season once rather than
#: nine times.
_UNIVERSE_CACHE: Dict[int, Dict[int, dict]] = {}

#: Cache of ``player_id -> entry_year``, pooled across every pulled season.
_ENTRY_YEAR_CACHE: Optional[Dict[int, int]] = None


def reset_cache() -> None:
    """Drop the player-universe and rookie caches. Mainly for tests."""
    _UNIVERSE_CACHE.clear()
    global _ENTRY_YEAR_CACHE
    _ENTRY_YEAR_CACHE = None


def season_endpoint(league_id: int, season: int, current_season: int) -> str:
    """The right ESPN base URL for one league-season.

    A finished season lives under ``leagueHistory``; the season in progress lives
    under ``seasons/{year}``. Same split as
    :func:`Scripts.fetch_utils.set_league_endpoint`, restated here because this
    module never builds a ``League`` -- one JSON request per season is the whole
    point.

    Args:
        league_id: ESPN league id.
        season: Season year to read.
        current_season: The season the pipeline is operating on.

    Returns:
        str: A base URL ending in ``?`` or ``&``, ready for ``view=`` params.
    """
    if season >= current_season:
        return f"{FANTASY_HOST}/seasons/{season}/segments/0/leagues/{league_id}?"
    return f"{FANTASY_HOST}/leagueHistory/{league_id}?seasonId={season}&"


def _get_json(url: str, cookies: Dict[str, str],
              headers: Optional[Dict[str, str]] = None, timeout: int = 60,
              unwrap: bool = True):
    """GET and decode, optionally unwrapping ``leagueHistory``'s one-element list.

    The unwrap is a flag rather than automatic because both shapes are real here:
    ``leagueHistory`` returns the league inside a single-element list, and the
    players endpoint returns a list that *is* the answer. Unwrapping both gave one
    player's dict where the universe should have been.

    Args:
        url: Fully-formed URL.
        cookies: ESPN ``swid`` / ``espn_s2``.
        headers: Extra headers, e.g. ``x-fantasy-filter``.
        timeout: Seconds.
        unwrap: Take the first element when ESPN wraps the payload in a list.

    Returns:
        The decoded payload.

    Raises:
        requests.HTTPError: On a non-2xx response.
    """
    response = requests.get(url, cookies=cookies, headers=headers or {},
                            timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if unwrap and isinstance(payload, list):
        payload = payload[0] if payload else {}
    return payload


def player_universe(season: int, cookies: Dict[str, str]) -> Dict[int, dict]:
    """That season's whole player pool, keyed by ESPN player id.

    Cached per season because it is league-independent: the nine leagues drafted
    the same 2025 players.

    Args:
        season: Season year.
        cookies: Any configured league's ESPN cookies.

    Returns:
        dict: ``player_id -> {"name", "position", "pro_team"}``.

    Raises:
        requests.HTTPError: On a non-2xx response.
    """
    if season in _UNIVERSE_CACHE:
        return _UNIVERSE_CACHE[season]

    entries = _get_json(
        f"{FANTASY_HOST}/seasons/{season}/players?scoringPeriodId=0&view=players_wl",
        cookies=cookies,
        # Without the filter ESPN returns the whole historical universe rather
        # than the players who existed that season.
        headers={"x-fantasy-filter": json.dumps({"filterActive": {"value": True}})},
        unwrap=False,
    )
    universe = {
        int(entry["id"]): {
            "name": entry.get("fullName") or "",
            "position": _position_of(entry),
            "pro_team": PRO_TEAM_MAP.get(entry.get("proTeamId")) or "",
        }
        for entry in (entries or [])
    }
    _UNIVERSE_CACHE[season] = universe
    return universe


def _position_of(entry: dict) -> str:
    """Resolve a player entry's position, preferring ``defaultPositionId``.

    Args:
        entry: One player from the season universe.

    Returns:
        str: ``"RB"``, ``"D/ST"``, and so on. Empty when neither the id nor the
        eligible slots resolve.
    """
    position = DEFAULT_POSITION_MAP.get(entry.get("defaultPositionId"))
    if position:
        return position
    eligible = set(entry.get("eligibleSlots") or [])
    for slot, name in FALLBACK_SLOT_POSITIONS:
        if slot in eligible:
            return name
    return ""


def entry_years(seasons: Sequence[int]) -> Dict[int, int]:
    """ESPN player id to the season the player entered the league.

    Pooled across every season available locally, then keyed by ``espn_id``.
    Pooling is not an optimisation -- it is the fix for nflverse's 2023 roster
    file, in which all 8,263 rookie rows carry a null ``espn_id``. Those players
    are resolved through their 2024 rows instead.

    Args:
        seasons: Seasons to read rosters for. Missing ones are skipped.

    Returns:
        dict: ``player_id -> entry_year``. Empty when no season is pulled, which
        leaves ``is_rookie`` null rather than wrongly False.
    """
    global _ENTRY_YEAR_CACHE
    if _ENTRY_YEAR_CACHE is not None:
        return _ENTRY_YEAR_CACHE

    from Scripts.usage.context import load_rosters

    frames = []
    for season in sorted(set(seasons)):
        try:
            frames.append(load_rosters([season]).select("espn_id", "entry_year"))
        except (FileNotFoundError, OSError):
            # A season nobody has pulled. The rookie flag degrades for picks that
            # only that season could have resolved; nothing else is affected.
            continue
    if not frames:
        _ENTRY_YEAR_CACHE = {}
        return _ENTRY_YEAR_CACHE

    pooled = (
        pl.concat(frames, how="diagonal")
        .filter(pl.col("espn_id").is_not_null() & pl.col("entry_year").is_not_null())
        # espn_id is a string of a float in some vintages ("3139477.0").
        .with_columns(pl.col("espn_id").cast(pl.Float64, strict=False)
                      .cast(pl.Int64, strict=False).alias("player_id"))
        .drop_nulls("player_id")
        .group_by("player_id")
        .agg(pl.col("entry_year").min())
    )
    _ENTRY_YEAR_CACHE = dict(zip(pooled["player_id"].to_list(),
                                 pooled["entry_year"].to_list()))
    return _ENTRY_YEAR_CACHE


def _member_names(payloads: Iterable[dict]) -> Dict[str, str]:
    """Member GUID to display name, pooled over a league's seasons.

    Pooled because ESPN drops ``firstName``/``lastName`` from some old payloads
    while keeping the GUID -- three Winfield Football seasons and two Weenieless
    seasons name nobody. A later season names them, and the GUID is stable, so a
    single map over the league fills every year.

    Args:
        payloads: One league's season payloads, oldest first.

    Returns:
        dict: ``member_id -> name``. Later (newer) payloads win, so a manager who
        fixed their profile is shown under the name they use now.
    """
    names: Dict[str, str] = {}
    for payload in payloads:
        for member in payload.get("members") or []:
            first = str(member.get("firstName") or "").strip()
            last = str(member.get("lastName") or "").strip()
            name = " ".join(part for part in (first, last) if part)
            name = name or str(member.get("displayName") or "").strip()
            if name:
                names[member["id"]] = name
    return names


def _team_label(team: dict) -> str:
    """A team's name, from whichever fields that season's payload used.

    Args:
        team: One entry from the payload's ``teams``.

    Returns:
        str: e.g. ``"Coconut Crushers"``. Empty when the payload names it nowhere.
    """
    joined = " ".join(
        str(team.get(key) or "").strip() for key in ("location", "nickname")
    ).strip()
    return joined or str(team.get("name") or "").strip()


#: Columns of a pick-history frame, in order. Named here so the empty frame and
#: the built one cannot disagree about the schema.
PICK_SCHEMA: Dict[str, pl.DataType] = {
    "season": pl.Int64, "draft_type": pl.Utf8, "n_teams": pl.Int64,
    "rounds": pl.Int64, "n_picks": pl.Int64, "overall_pick": pl.Int64,
    "round": pl.Int64, "round_pick": pl.Int64, "pick_pct": pl.Float64,
    "team_id": pl.Int64, "team_name": pl.Utf8, "owner_id": pl.Utf8,
    "owner": pl.Utf8, "player_id": pl.Int64, "player_name": pl.Utf8,
    "position": pl.Utf8, "pro_team": pl.Utf8, "bid": pl.Float64,
    "keeper": pl.Boolean, "auto_drafted": pl.Boolean, "is_rookie": pl.Boolean,
}


def empty_history() -> pl.DataFrame:
    """A correctly-typed, empty pick-history frame.

    Returns:
        pl.DataFrame: Zero rows with :data:`PICK_SCHEMA`'s columns.
    """
    return pl.DataFrame(schema=PICK_SCHEMA)


def fetch_draft_history(
    league_id: int,
    seasons: Sequence[int],
    swid: str,
    espn_s2: str,
    *,
    current_season: Optional[int] = None,
) -> pl.DataFrame:
    """Every pick one league has ever made, across ``seasons``.

    Args:
        league_id: ESPN league id.
        seasons: Season years to read. A season whose draft has not happened
            contributes no rows rather than failing.
        swid: ESPN SWID cookie.
        espn_s2: ESPN espn_s2 cookie.
        current_season: The season the pipeline operates on, deciding which
            endpoint each year uses. Read from ``config.yaml`` when omitted.

    Returns:
        pl.DataFrame: One row per pick, :data:`PICK_SCHEMA`. ``pick_pct`` is the
        pick's position through the draft (0-1), which is the only cross-league
        comparable ordering when leagues run 14 to 17 rounds.

    Raises:
        requests.HTTPError: On a non-2xx response -- typically expired cookies.
    """
    if current_season is None:
        from Scripts.config_utils import get_season
        current_season = get_season()

    cookies = {"swid": swid, "espn_s2": espn_s2}
    payloads: Dict[int, dict] = {}
    for season in sorted(seasons):
        url = season_endpoint(league_id, season, current_season)
        payload = _get_json(url + "view=mDraftDetail&view=mTeam&view=mSettings",
                            cookies=cookies)
        detail = payload.get("draftDetail") or {}
        # A season whose draft has not happened still returns a full set of picks
        # -- ESPN pre-creates the slots with ``playerId: -1``. Measured on
        # Winfield Football on 2026-08-10: 96 empty picks, one per slot, which
        # read as a completed eleventh draft in which every manager took nothing.
        # Every position would have been censored to one round past the end for
        # all six managers, moving every timing baseline in the league.
        if detail.get("picks") and detail.get("drafted"):
            payloads[season] = payload

    if not payloads:
        return empty_history()

    names = _member_names(payloads[season] for season in sorted(payloads))
    rows: List[dict] = []
    for season in sorted(payloads):
        payload = payloads[season]
        universe = player_universe(season, cookies)
        teams = {team["id"]: team for team in payload.get("teams") or []}
        draft_settings = (payload.get("settings") or {}).get("draftSettings", {})
        # Second guard, for a draft ESPN calls complete with unfilled slots in it:
        # keep only picks whose player resolves. The test is membership in the
        # season's universe, *not* the sign of the id -- team defences are
        # negative (-16027 is Tampa Bay), so dropping ``playerId <= 0`` would
        # silently delete every D/ST and with it the whole D/ST timing tendency.
        picks = [pick for pick in payload["draftDetail"]["picks"]
                 if int(pick["playerId"]) in universe]
        if not picks:
            continue
        n_picks = len(picks)
        rounds = max(int(pick["roundId"]) for pick in picks)

        for pick in picks:
            team = teams.get(pick["teamId"], {})
            owner_id = (team.get("primaryOwner")
                        or next(iter(team.get("owners") or []), None)
                        or pick.get("memberId") or "")
            player = universe.get(int(pick["playerId"]), {})
            rows.append({
                "season": int(season),
                "draft_type": str(draft_settings.get("type") or "SNAKE"),
                "n_teams": len(teams),
                "rounds": rounds,
                "n_picks": n_picks,
                "overall_pick": int(pick["overallPickNumber"]),
                "round": int(pick["roundId"]),
                "round_pick": int(pick["roundPickNumber"]),
                "pick_pct": int(pick["overallPickNumber"]) / n_picks,
                "team_id": int(pick["teamId"]),
                "team_name": _team_label(team),
                "owner_id": str(owner_id),
                "owner": names.get(owner_id) or _team_label(team) or "Unknown",
                "player_id": int(pick["playerId"]),
                "player_name": player.get("name", ""),
                "position": player.get("position", ""),
                "pro_team": player.get("pro_team", ""),
                "bid": float(pick.get("bidAmount") or 0),
                "keeper": bool(pick.get("keeper")),
                "auto_drafted": bool(pick.get("autoDraftTypeId") or 0),
            })

    history = pl.DataFrame(rows, schema_overrides={
        key: dtype for key, dtype in PICK_SCHEMA.items() if key != "is_rookie"
    })

    rookie_years = entry_years(sorted(payloads))
    if rookie_years:
        lookup = pl.DataFrame(
            {"player_id": list(rookie_years), "entry_year": list(rookie_years.values())},
            schema={"player_id": pl.Int64, "entry_year": pl.Int64},
        )
        history = (history.join(lookup, on="player_id", how="left")
                   .with_columns((pl.col("entry_year") == pl.col("season"))
                                 .alias("is_rookie"))
                   .drop("entry_year"))
    else:
        history = history.with_columns(pl.lit(None, dtype=pl.Boolean).alias("is_rookie"))

    return history.select(list(PICK_SCHEMA)).sort("season", "overall_pick")


def history_summary(history: pl.DataFrame) -> str:
    """A one-line description of a pick history, for refresh output.

    Args:
        history: A :func:`fetch_draft_history` frame.

    Returns:
        str: e.g. ``"960 picks, 2016-2025 (10 drafts), 6 managers"``.
    """
    if history.is_empty():
        return "no drafts found"
    seasons = history["season"]
    formats = sorted(set(history["draft_type"].to_list()))
    return (f"{history.height} picks, {seasons.min()}-{seasons.max()} "
            f"({seasons.n_unique()} drafts), {history['owner'].n_unique()} managers, "
            f"{'/'.join(fmt.lower() for fmt in formats)}")
