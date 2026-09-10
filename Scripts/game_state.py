"""What state every NFL game is in, so a zero can be told from a blank.

The pipeline collects actual points already -- ``lineups.parquet`` has carried
``points`` and the raw stat line from ESPN's box scores since it existed. What it
has never carried is **why a number is zero**. A player who has not kicked off and
a player who played and caught nothing are the same row, and
``build_league_frame`` runs ``df.fillna(0)``, so a null cannot carry the
distinction either.

Neither of the two signals already on the frame can answer it:

* ``player_active_status`` is set in ``espn_api``'s ``Player.__init__`` by looping
  every scoring period, so it means "has stats somewhere this season", not "played
  this week". On 2026 week 1, **102 of 242 rows read ``active`` when 13 players had
  played**.
* ``BoxPlayer.game_played`` is ``100 if now > kickoff + 3h else 0`` -- a clock
  heuristic that reads 0 for the whole of a live game and 100 in the middle of an
  overtime.

So the state comes from ESPN's own scoreboard, which says it directly::

    "status": {"period": 4, "displayClock": "0:00",
               "type": {"state": "post", "completed": true, "detail": "Final"}}

**Why this endpoint.** Same host as :mod:`Scripts.scrape_espn_injuries`, which
carries the access reasoning: ``site.api.espn.com`` publishes no robots.txt (403 on
the file, which RFC 9309 classes as "unavailable" and permits), and unlike
Pro-Football-Reference or BetOnline's weekly endpoint there is no anti-bot control
here to circumvent. It needs no cookies, and one request covers all sixteen games
for every league in the config.

**And no name reconciliation on this path.** Verified 2026-09-10: the scoreboard's
32 ``team.abbreviation`` values match ``espn_api.football.constant.PRO_TEAM_MAP``'s
values exactly, in both directions, so the join to the pipeline's ``pro_team`` is
direct. Only nflverse disagrees -- ``LA``/``WAS`` against ``LAR``/``WSH`` -- which
is why :func:`from_schedule`, the offline fallback, is the one function here that
aliases anything.

**A bye is an absence, and this reads it as one.** A team that does not appear in
the week's events is on bye. That is derived from the payload rather than from the
schedule's missing-week arithmetic, so it needs neither the schedule nor the
aliasing, and it is right in a week ESPN has and the local schedule CSV does not.

Usage::

    python -m Scripts.game_state                  # the current week's board
    python -m Scripts.game_state --week 1
"""

from __future__ import annotations

import datetime
import warnings
from typing import Any, Dict, List, Optional

import polars as pl
import requests

from Scripts.paths import nfl_season_dir

#: The JSON behind ``espn.com/nfl/scoreboard``.
ENDPOINT = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"

#: ESPN's ``seasontype`` for the regular season. The pipeline is regular-season
#: only throughout -- ``R/GetNFL.R`` filters ``game_type == 'REG'`` and
#: ``nflfastR::calculate_stats`` defaults to it -- so this is a constant, not an
#: argument.
REGULAR_SEASON = 2

#: Not kicked off. The projection stands.
PRE = "pre"

#: Under way. Actual so far, plus what is left of the projection.
IN = "in"

#: Over. The actual is the answer.
POST = "post"

#: No game this week. Zero, and it is not a projection failure.
BYE = "bye"

#: Every state, in the order a week moves through them.
STATES = (PRE, IN, POST, BYE)

#: Minutes in a quarter and in regulation, for the elapsed fraction.
QUARTER_MINUTES = 15.0
REGULATION_MINUTES = 60.0

#: ESPN status names that are *not* what their ``state`` claims.
#:
#: A postponed game reports ``state: "post"`` with ``completed: false``, which read
#: literally would freeze every player in it at zero **as a final score** -- and the
#: game is going to be replayed. It is a ``pre`` game with an unknown kickoff. A
#: cancelled game genuinely never happens, so zero is final and ``post`` is right.
POSTPONED_STATUSES = ("STATUS_POSTPONED", "STATUS_DELAYED")
CANCELLED_STATUSES = ("STATUS_CANCELED", "STATUS_CANCELLED", "STATUS_FORFEIT")

#: How long a cached board is worth reading. Matches ``app/store.CACHE_TTL``, so the
#: app and the state behind it cannot be more than one interval apart.
STALE_AFTER_SECONDS = 300


class GameStateWarning(UserWarning):
    """The scoreboard could not be read, or said something unrecognised."""


def _warn(message: str) -> None:
    """Warn past ``fetch_utils``' module-scope ``filterwarnings("ignore")``.

    That call silences every warning in the process, so a plain ``warnings.warn``
    here is invisible in any run that has imported the ESPN fetch layer -- which is
    every run that would want this. Same local override
    ``Scripts.projection_utils`` and ``Scripts.scoring`` use.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("always", GameStateWarning)
        warnings.warn(message, GameStateWarning, stacklevel=3)


def nfl_teams() -> frozenset:
    """The 32 team abbreviations, in ESPN's spelling.

    Read from ``espn_api``'s map rather than listed here, so a relocation or a
    rebrand arrives with the library instead of silently marking a team as
    permanently on bye.

    Returns:
        frozenset: Abbreviations. Excludes the ``0`` and free-agent sentinels.
    """
    from espn_api.football.constant import PRO_TEAM_MAP

    return frozenset(v for v in PRO_TEAM_MAP.values() if v and v not in ("None", "FA"))


def game_state_path(season: int, create: bool = False):
    """Where the cached board lives.

    Args:
        season: Season year.
        create: Create the directory. Writers set this.

    Returns:
        Path: ``Data/NFL/<season>/game_state.parquet``.
    """
    return nfl_season_dir(season, create=create) / "game_state.parquet"


def clock_minutes(display_clock: Optional[str]) -> float:
    """Minutes left in the current period, from ESPN's ``displayClock``.

    Args:
        display_clock: ``"12:34"``, or ``"0:00"`` between periods.

    Returns:
        float: Minutes remaining. ``0.0`` for anything unparseable, which is the
        conservative direction -- it reads the period as finished rather than as
        untouched.
    """
    if not display_clock:
        return 0.0
    text = str(display_clock).strip()
    parts = text.split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) + int(parts[1]) / 60.0
        return float(text) / 60.0
    except (TypeError, ValueError):
        _warn(f"unparseable displayClock {display_clock!r}; treating the period "
              f"as elapsed.")
        return 0.0


def elapsed_fraction(state: str, period: Optional[int],
                     display_clock: Optional[str]) -> float:
    """How much of a game has been played, as a fraction of regulation.

    This is what scales the remaining projection: a player mid-game is worth what
    he has banked plus ``(1 - elapsed)`` of what he was projected for. It is
    deliberately a **clock** fraction and not a drive or possession count -- the
    scoreboard gives the clock for free, and the alternative wants play-by-play
    that does not exist until after the game.

    Overtime returns ``1.0`` rather than something over 1: the remaining projection
    should be zero by then, and a negative remainder would subtract from a total.

    Args:
        state: One of :data:`STATES`.
        period: Quarter number, 1-4, 5+ for overtime.
        display_clock: ESPN's ``displayClock``.

    Returns:
        float: In ``[0.0, 1.0]``.
    """
    if state in (POST, BYE):
        return 1.0
    if state == PRE:
        return 0.0
    quarter = int(period or 0)
    if quarter <= 0:
        return 0.0
    if quarter > 4:
        return 1.0
    played = (quarter - 1) * QUARTER_MINUTES + (
        QUARTER_MINUTES - clock_minutes(display_clock))
    return max(0.0, min(1.0, played / REGULATION_MINUTES))


def fetch(season: int, week: int, timeout: int = 30) -> Dict[str, Any]:
    """Request one week's scoreboard.

    Args:
        season: Season year.
        week: Week number.
        timeout: Seconds to wait.

    Returns:
        dict: The decoded payload.

    Raises:
        requests.HTTPError: On a non-2xx response.
        ValueError: When the payload carries no ``events`` array. ESPN answers a
            bad season or week with a 200 and an empty body rather than an error,
            so this is the check that a nonsense week is not read as a week in
            which nobody plays.
    """
    response = requests.get(
        ENDPOINT,
        params={"dates": str(season), "seasontype": REGULAR_SEASON, "week": int(week)},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if "events" not in payload:
        raise ValueError(
            f"{ENDPOINT} returned no 'events' array for {season} week {week}; "
            f"keys were {sorted(payload)}."
        )
    return payload


def _resolve_state(status: Dict[str, Any]) -> str:
    """ESPN's status block, reduced to one of :data:`STATES`.

    Args:
        status: A ``competitions[0].status`` dict.

    Returns:
        str: :data:`PRE`, :data:`IN` or :data:`POST`.
    """
    kind = status.get("type") or {}
    name = str(kind.get("name") or "")
    state = str(kind.get("state") or "").lower()

    if name in POSTPONED_STATUSES:
        return PRE
    if name in CANCELLED_STATUSES:
        return POST
    if state in (PRE, IN, POST):
        # A `post` that is not `completed` and is not one of the named exceptions is
        # a suspended game. Reported as `in` at full elapsed: it neither claims a
        # final score nor hands the player his projection back.
        if state == POST and not kind.get("completed", False):
            _warn(f"status {name or state!r} is 'post' but not completed; treating "
                  f"the game as in progress.")
            return IN
        return state

    _warn(f"unrecognised scoreboard state {state!r} (status {name!r}); treating the "
          f"game as in progress so it neither locks nor projects.")
    return IN


#: The frame's schema, declared rather than inferred.
#:
#: A week in which every game is still ``pre`` carries no scores at all, and polars
#: would infer those columns as ``Null`` -- which then fails to concatenate or
#: compare against a later pull that has them.
_SCHEMA = {
    "season": pl.Int64,
    "week": pl.Int64,
    "team": pl.String,
    "opponent": pl.String,
    "state": pl.String,
    "completed": pl.Boolean,
    "period": pl.Int64,
    "clock": pl.String,
    "elapsed": pl.Float64,
    "kickoff_utc": pl.String,
    "team_score": pl.Float64,
    "opp_score": pl.Float64,
    "detail": pl.String,
    "fetched_at": pl.String,
}


def parse(payload: Dict[str, Any], season: int, week: int,
          fetched_at: Optional[datetime.datetime] = None) -> pl.DataFrame:
    """Flatten a scoreboard payload into one row per team.

    Two rows per game, because the consumer joins on a player's ``pro_team`` and
    does not care who was at home. Teams with no game in the payload are added as
    :data:`BYE`.

    Args:
        payload: :func:`fetch` output.
        season: Season year, stamped on every row.
        week: Week number, stamped on every row.
        fetched_at: When the pull happened. Defaults to now, UTC.

    Returns:
        pl.DataFrame: ``season``, ``week``, ``team``, ``opponent``, ``state``,
        ``completed``, ``period``, ``clock``, ``elapsed``, ``kickoff_utc``,
        ``team_score``, ``opp_score``, ``detail`` and ``fetched_at``.
    """
    if fetched_at is None:
        fetched_at = datetime.datetime.now(datetime.timezone.utc)
    stamp = fetched_at.isoformat()

    rows: List[Dict[str, Any]] = []
    for event in payload.get("events", []) or []:
        competitions = event.get("competitions") or []
        if not competitions:
            continue
        competition = competitions[0]
        status = competition.get("status") or {}
        kind = status.get("type") or {}
        state = _resolve_state(status)
        period = status.get("period")
        clock = status.get("displayClock")
        elapsed = elapsed_fraction(state, period, clock)

        competitors = competition.get("competitors") or []
        sides = []
        for competitor in competitors:
            team = (competitor.get("team") or {}).get("abbreviation")
            if not team:
                continue
            score = competitor.get("score")
            try:
                score = float(score) if score is not None else None
            except (TypeError, ValueError):
                score = None
            sides.append((team, score))
        if len(sides) != 2:
            _warn(f"event {event.get('shortName')!r} has {len(sides)} identifiable "
                  f"teams, not 2; skipped.")
            continue

        for index, (team, score) in enumerate(sides):
            opponent, opp_score = sides[1 - index]
            rows.append({
                "season": int(season),
                "week": int(week),
                "team": team,
                "opponent": opponent,
                "state": state,
                "completed": bool(kind.get("completed", False)),
                "period": int(period) if period is not None else 0,
                "clock": str(clock) if clock is not None else None,
                "elapsed": float(elapsed),
                "kickoff_utc": event.get("date"),
                "team_score": score,
                "opp_score": opp_score,
                "detail": kind.get("detail"),
                "fetched_at": stamp,
            })

    playing = {row["team"] for row in rows}
    for team in sorted(nfl_teams() - playing):
        rows.append({
            "season": int(season),
            "week": int(week),
            "team": team,
            "opponent": None,
            "state": BYE,
            "completed": True,
            "period": 0,
            "clock": None,
            "elapsed": 1.0,
            "kickoff_utc": None,
            "team_score": None,
            "opp_score": None,
            "detail": "Bye",
            "fetched_at": stamp,
        })

    return pl.DataFrame(rows, schema=_SCHEMA).sort("team")


def from_schedule(season: int, week: int,
                  now: Optional[datetime.datetime] = None) -> pl.DataFrame:
    """The same board, derived from the local schedule CSV. The offline fallback.

    Strictly worse than the scoreboard and used only when that cannot be reached:
    ``R/GetNFL.R`` runs at 06:00, so a game that ended at 16:20 on Sunday reads as
    unplayed here until Monday morning. It exists so that a network failure degrades
    to "the projection stands" rather than to an exception in the middle of a
    refresh.

    ``elapsed`` is a clock estimate from ``gameday`` + ``gametime``, which nflverse
    publishes in US/Eastern, against a three-hour game length.

    Args:
        season: Season year.
        week: Week number.
        now: Current time, UTC. Injected for testing.

    Returns:
        pl.DataFrame: Same schema as :func:`parse`.

    Raises:
        FileNotFoundError: When the schedule CSV has not been generated.
        ValueError: When the CSV holds a different season.
    """
    from Scripts.nfl_utils import SCHEDULE_TEAM_ALIASES, load_schedule

    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)
    stamp = now.isoformat()

    schedule = load_schedule()
    seasons = schedule["season"].unique().to_list()
    if seasons != [int(season)]:
        raise ValueError(
            f"Data/NFL_Schedules.csv covers {seasons}, not {season}. Re-run "
            f"`Rscript R/GetNFL.R {season}`."
        )

    this_week = schedule.filter(pl.col("week") == int(week))
    rows: List[Dict[str, Any]] = []
    for game in this_week.iter_rows(named=True):
        kickoff = _schedule_kickoff(game.get("gameday"), game.get("gametime"))
        scored = str(game.get("away_score")) not in ("NA", "None", "")
        if scored:
            state, elapsed = POST, 1.0
        elif kickoff is None or now < kickoff:
            state, elapsed = PRE, 0.0
        else:
            played = (now - kickoff).total_seconds() / 60.0
            state = POST if played >= 3 * REGULATION_MINUTES else IN
            elapsed = 1.0 if state == POST else max(
                0.0, min(1.0, played / (3 * REGULATION_MINUTES)))

        for home in (True, False):
            team = game["home_team"] if home else game["away_team"]
            opponent = game["away_team"] if home else game["home_team"]
            rows.append({
                "season": int(season),
                "week": int(week),
                # The one place this module aliases anything: nflverse says LA and
                # WAS where ESPN says LAR and WSH, and the consumer joins on ESPN's
                # spelling.
                "team": SCHEDULE_TEAM_ALIASES.get(team, team),
                "opponent": SCHEDULE_TEAM_ALIASES.get(opponent, opponent),
                "state": state,
                "completed": state == POST,
                "period": 0,
                "clock": None,
                "elapsed": float(elapsed),
                "kickoff_utc": kickoff.isoformat() if kickoff else None,
                "team_score": _numeric(
                    game.get("home_score") if home else game.get("away_score")),
                "opp_score": _numeric(
                    game.get("away_score") if home else game.get("home_score")),
                "detail": "from schedule",
                "fetched_at": stamp,
            })

    playing = {row["team"] for row in rows}
    for team in sorted(nfl_teams() - playing):
        rows.append({
            "season": int(season), "week": int(week), "team": team,
            "opponent": None, "state": BYE, "completed": True, "period": 0,
            "clock": None, "elapsed": 1.0, "kickoff_utc": None,
            "team_score": None, "opp_score": None, "detail": "Bye",
            "fetched_at": stamp,
        })

    return pl.DataFrame(rows, schema=_SCHEMA).sort("team")


def _numeric(value) -> Optional[float]:
    """A schedule cell as a float, or None for its ``"NA"`` sentinel."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _schedule_kickoff(gameday: Optional[str],
                      gametime: Optional[str]) -> Optional[datetime.datetime]:
    """Kickoff as an aware UTC datetime, from the schedule's two string columns.

    nflverse publishes ``gametime`` in US/Eastern. ``zoneinfo`` handles the daylight
    boundary, which matters: the season crosses it in early November, and a fixed
    offset would put every game after it an hour wrong.

    Args:
        gameday: ``"2026-09-13"``.
        gametime: ``"13:00"``.

    Returns:
        datetime | None: None when either part is missing or malformed.
    """
    from zoneinfo import ZoneInfo

    if not gameday or not gametime or "NA" in (str(gameday), str(gametime)):
        return None
    try:
        naive = datetime.datetime.strptime(
            f"{gameday} {gametime}", "%Y-%m-%d %H:%M")
    except ValueError:
        return None
    return naive.replace(tzinfo=ZoneInfo("America/New_York")).astimezone(
        datetime.timezone.utc)


def states(season: int, week: int, *, refresh: bool = False,
           timeout: int = 30, allow_fallback: bool = True) -> pl.DataFrame:
    """One week's board, from the scoreboard, the cache, or the schedule.

    The order is deliberate. A fresh scoreboard read is best; a cached one within
    :data:`STALE_AFTER_SECONDS` is as good for anything but a live game; the
    schedule is a floor that keeps a refresh running when the network is gone.

    Args:
        season: Season year.
        week: Week number.
        refresh: Skip the cache and re-read the scoreboard.
        timeout: Seconds to wait on the request.
        allow_fallback: Fall back to :func:`from_schedule` on a failed fetch.
            Off in tests that need the failure to be visible.

    Returns:
        pl.DataFrame: Same schema as :func:`parse`.

    Raises:
        Exception: The fetch error, when ``allow_fallback`` is False or the
            fallback also fails.
    """
    path = game_state_path(season)
    if not refresh:
        cached = _read_cache(path, week)
        if cached is not None:
            return cached

    try:
        frame = parse(fetch(season, week, timeout=timeout), season, week)
    except Exception as error:                    # noqa: BLE001 - reported, not hidden
        if not allow_fallback:
            raise
        _warn(f"could not read the scoreboard for {season} week {week} "
              f"({type(error).__name__}: {error}); falling back to the schedule, "
              f"which is only as fresh as the last `Rscript R/GetNFL.R`.")
        stale = _read_cache(path, week, ignore_age=True)
        if stale is not None:
            return stale
        return from_schedule(season, week)

    _write_cache(frame, week)
    return frame


def _read_cache(path, week: int, ignore_age: bool = False) -> Optional[pl.DataFrame]:
    """The cached board for one week, when it is present and fresh enough."""
    if not path.exists():
        return None
    try:
        frame = pl.read_parquet(path).filter(pl.col("week") == int(week))
    except Exception:                             # noqa: BLE001 - a corrupt cache is a miss
        return None
    if frame.is_empty():
        return None
    if ignore_age or _settled(frame):
        return frame
    stamps = frame["fetched_at"].drop_nulls().to_list()
    if not stamps:
        return None
    try:
        age = (datetime.datetime.now(datetime.timezone.utc)
               - datetime.datetime.fromisoformat(max(stamps))).total_seconds()
    except ValueError:
        return None
    return frame if age <= STALE_AFTER_SECONDS else None


def _settled(frame: pl.DataFrame) -> bool:
    """Whether every game in a cached week is over.

    A finished week cannot change, so it is exempt from :data:`STALE_AFTER_SECONDS`.
    Without this the nightly re-fetches every elapsed week for every league --
    ``clean_lineups`` sees weeks 1..current and runs nine times -- to re-learn
    sixteen final scores that were final in September.

    Args:
        frame: One week's cached rows.

    Returns:
        bool: True when no row is :data:`PRE` or :data:`IN`.
    """
    return not frame.filter(pl.col("state").is_in([PRE, IN])).height


def board(season: int, weeks, *, refresh_current: Optional[int] = None,
          timeout: int = 30) -> pl.DataFrame:
    """The board for several weeks at once, which is what a lineups frame needs.

    ``lineups.parquet`` holds weeks 1..current, so a join needs all of them. Settled
    weeks come from the cache for free (see :func:`_settled`); only a week still in
    progress costs a request.

    Args:
        season: Season year.
        weeks: Week numbers.
        refresh_current: Force a fresh read of this week, bypassing the cache. The
            live refresh passes the current week; the nightly does not need to.
        timeout: Seconds to wait on each request.

    Returns:
        pl.DataFrame: Same schema as :func:`parse`, for every requested week.
    """
    frames = [states(season, int(week), timeout=timeout,
                     refresh=(refresh_current is not None
                              and int(week) == int(refresh_current)))
              for week in sorted({int(w) for w in weeks})]
    if not frames:
        return pl.DataFrame(schema=_SCHEMA)
    return pl.concat(frames, how="vertical_relaxed")


def _write_cache(frame: pl.DataFrame, week: int) -> None:
    """Replace this week's rows in the cache, leaving other weeks alone.

    Other weeks are preserved because a season accumulates them and a refresh of
    week 6 must not erase week 5's final states, which is what the accuracy pass
    reads.
    """
    path = game_state_path(frame["season"][0], create=True)
    combined = frame
    if path.exists():
        try:
            existing = pl.read_parquet(path).filter(pl.col("week") != int(week))
            combined = pl.concat([existing, frame], how="vertical_relaxed")
        except Exception:                          # noqa: BLE001 - overwrite a bad cache
            combined = frame
    tmp = path.with_suffix(".parquet.tmp")
    combined.write_parquet(tmp)
    tmp.replace(path)


def anything_live(frame: pl.DataFrame) -> bool:
    """Whether any game in the board is under way.

    What the cron wrapper branches on, so a quiet Tuesday costs one request rather
    than nine league refreshes.

    Args:
        frame: :func:`states` output.

    Returns:
        bool: True when at least one row is :data:`IN`.
    """
    return bool(frame.filter(pl.col("state") == IN).height)


#: How long after kickoff a game still counts as worth polling.
#:
#: The live refresh must run at least once *after* the last game of the day is final,
#: or the store keeps the mid-game hybrid as its permanent record of that week.
#: Gating on "anything in progress" alone cannot do that: the moment a game ends,
#: nothing is in progress. Six hours comfortably covers a three-hour game plus
#: overtime and a delay, and the cost of an unnecessary run is one HTTP request plus a
#: few seconds per league.
POLL_WINDOW_HOURS = 6.0


def in_window(frame: pl.DataFrame, *, hours: float = POLL_WINDOW_HOURS,
              now: Optional[datetime.datetime] = None) -> bool:
    """Whether this week is worth refreshing right now.

    True while a game is in progress, and for :data:`POLL_WINDOW_HOURS` after each
    kickoff -- so the run that captures the final score is guaranteed to happen. On a
    Tuesday it is False and the cron wrapper exits having made one request.

    Args:
        frame: :func:`states` output.
        hours: The window after kickoff.
        now: Current time, UTC. Injected for testing.

    Returns:
        bool: True when the live refresh should run.
    """
    if frame.is_empty():
        return False
    if anything_live(frame):
        return True
    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)
    cutoff = now - datetime.timedelta(hours=hours)
    for stamp in frame.filter(pl.col("state") == POST)["kickoff_utc"].drop_nulls():
        try:
            kickoff = datetime.datetime.fromisoformat(
                str(stamp).replace("Z", "+00:00"))
        except ValueError:
            continue
        if cutoff <= kickoff <= now:
            return True
    return False


def summary(frame: pl.DataFrame) -> str:
    """A printable description of the week's board."""
    counts = dict(frame.group_by("state").len().rows())
    # Two rows per game, one per team, so a game count halves -- except bye, which
    # is a team-level fact and is already one row per team.
    per_state = {state: counts.get(state, 0) // 2 for state in (PRE, IN, POST)}
    per_state[BYE] = counts.get(BYE, 0)
    games = sum(per_state[state] for state in (PRE, IN, POST))
    lines = [f"  {games} games ("
             + ", ".join(f"{state}={per_state[state]}" for state in (PRE, IN, POST))
             + f"), {per_state[BYE]} teams on bye"]
    for row in frame.filter(pl.col("state") != BYE).sort(
            ["kickoff_utc", "team"]).iter_rows(named=True):
        # One line per game, not per team.
        if row["team"] > (row["opponent"] or ""):
            continue
        lines.append(
            f"    {row['team']:>3} vs {row['opponent'] or '---':<3} "
            f"{row['state']:<4} elapsed={row['elapsed']:.2f}  {row['detail'] or ''}")
    byes = frame.filter(pl.col("state") == BYE)["team"].to_list()
    if byes:
        lines.append(f"    bye: {', '.join(byes)}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    import argparse

    from Scripts.nfl_utils import current_season, current_week

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--season", type=int, default=None,
                        help="Season year. Defaults to the schedule's.")
    parser.add_argument("--week", type=int, default=None,
                        help="Week number. Defaults to the current week.")
    parser.add_argument("--cached", action="store_true",
                        help="Read the cache if it is fresh, instead of ESPN.")
    parser.add_argument("--if-live", action="store_true",
                        help="Print nothing and exit 0 only when this week is worth "
                             "a live refresh; 1 otherwise. What the cron wrapper "
                             "branches on, so a quiet day costs one request.")
    args = parser.parse_args(argv)

    season = args.season if args.season is not None else current_season()
    week = args.week if args.week is not None else current_week()

    if args.if_live:
        return 0 if in_window(states(season, week, refresh=not args.cached)) else 1

    print(f"\n===== NFL game state: {season} week {week} =====")
    frame = states(season, week, refresh=not args.cached)
    print(summary(frame))
    print(f"\n  in progress now: {anything_live(frame)}")
    print(f"  worth a live refresh: {in_window(frame)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
