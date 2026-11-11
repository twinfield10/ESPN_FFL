"""Snapshot everything ESPN will give us about one league-season into a zip.

    python -m Scripts.backup_league --league Jeffs_League
    python -m Scripts.backup_league --league jeffs_league --season 2025 --out ~/Desktop

**What goes in.** The raw JSON for every league-level ESPN view (settings, teams,
rosters, schedule and scores, box scores, standings, draft, transactions), a
per-week snapshot of the views that change week to week (lineups as they stood,
player-level box scores, that week's transactions), the player pool, the league
activity feed and message board, and -- when this machine has them -- the
pipeline's own ``Data/Store/<season>/<key>/`` files and the league's scoring rules.

**Why raw JSON and not parquet.** The point of a backup is that it does not
depend on what this pipeline happened to parse. ESPN can wipe or lock a league;
a field we never read today may be the one someone asks about next year.

**Where it lands.** The working copy is written under ``Data/Backups/`` -- which
is inside the gitignored ``Data/`` tree, so backup data is never committed -- and
the zip goes to ``--out`` (``~/Downloads`` by default). Nothing is pushed to S3.

**Why it exits non-zero.** Same rule as ``Scripts.sync``: a failed call is
counted, named, and turned into an exit code. The only failures that do not
count are calls marked optional (the activity feed, which ESPN does not serve
for past seasons).
"""

import argparse
import csv
import datetime
import json
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from Scripts import paths
from Scripts.config_utils import get_season, load_config, resolve_league

API = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl"
BACKUP_DIR = paths.DATA_DIR / "Backups"
SCORING_CSV = paths.DATA_DIR / "Scoring" / "scoring.csv"

#: League-wide views, each fetched on its own so one refusal does not hide the rest.
LEAGUE_VIEWS = [
    "mSettings", "mTeam", "mRoster", "mMatchup", "mMatchupScore", "mBoxscore",
    "mScoreboard", "mSchedule", "mStandings", "mStatus", "mNav", "mDraftDetail",
    "mPendingTransactions", "mTransactions2", "mPositionalRatings",
    "mLiveScoring", "proTeamSchedules_wl", "kona_game_state", "kona_playercard",
]

#: Views whose answer depends on ``scoringPeriodId`` -- the week-by-week history.
WEEK_VIEWS = ["mMatchup", "mMatchupScore", "mBoxscore", "mRoster", "mTransactions2", "mScoreboard"]

#: First season ESPN serves under ``/seasons/<y>/...``. Earlier seasons only exist under
#: ``/leagueHistory``, which drops per-player box scores -- so that endpoint is a last
#: resort, not the "past season" endpoint. (Checked 2026-11-11: Winfield_Football 2023
#: returns 16-man box-score rosters under /seasons and none under /leagueHistory.)
FIRST_SEASONS_ENDPOINT_YEAR = 2019

#: Pause between calls. ~100 calls per league, so this costs well under a minute.
CALL_DELAY_S = 0.4


@dataclass
class CallResult:
    """One ESPN request and whether it produced usable JSON."""

    file: str
    ok: bool
    optional: bool = False
    error: Optional[Dict[str, Any]] = None


@dataclass
class Fetcher:
    """Issues ESPN requests for one league-season and writes each response to disk.

    Attributes:
        league_id: ESPN league id.
        season: Season year.
        current: Whether ``season`` is the live one. Only the live season has an
            activity feed; ESPN deletes a season's communication group after it ends.
        out: Directory the JSON files are written under.
        session: ``requests`` session carrying the ``espn_s2`` / ``SWID`` cookies.
    """

    league_id: int
    season: int
    current: bool
    out: Path
    session: requests.Session
    log: List[CallResult] = field(default_factory=list)

    @property
    def history(self) -> bool:
        """Whether this season is only reachable through ``/leagueHistory``."""
        return self.season < FIRST_SEASONS_ENDPOINT_YEAR

    def league_url(self, sub: str = "") -> str:
        """Base URL for league requests."""
        if not self.history:
            return f"{API}/seasons/{self.season}/segments/0/leagues/{self.league_id}{sub}"
        return f"{API}/leagueHistory/{self.league_id}{sub}"

    def base_params(self) -> Dict[str, Any]:
        """Query parameters every league request needs (the season, for history)."""
        return {"seasonId": self.season} if self.history else {}

    def grab(
        self,
        rel: str,
        params: Optional[Dict[str, Any]] = None,
        filt: Optional[Dict[str, Any]] = None,
        sub: str = "",
        url: Optional[str] = None,
        optional: bool = False,
    ) -> Any:
        """Fetch one request, write it to ``<out>/<rel>.json``, and log the outcome.

        Args:
            rel: Output path relative to ``out``, without extension.
            params: Query parameters (merged over :meth:`base_params`).
            filt: Body of the ``x-fantasy-filter`` header, if any.
            sub: Path appended to the league URL (e.g. ``/communication/``).
            url: Full URL, replacing the league URL entirely.
            optional: A failure here is recorded but does not fail the run.

        Returns:
            The decoded JSON. Historical responses arrive as a one-element list
            and are unwrapped. ``None`` on failure.
        """
        headers = {"x-fantasy-filter": json.dumps(filt)} if filt else None
        query = {**(self.base_params() if url is None else {}), **(params or {})}
        data, err = fetch_json(self.session, url or self.league_url(sub), query, headers)
        if self.history and url is None and isinstance(data, list):
            data = data[0] if data else None
            err = err or (None if data else {"error": "empty leagueHistory response"})

        target = self.out / f"{rel}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(data if err is None else err, indent=1))
        self.log.append(CallResult(f"{rel}.json", err is None, optional, err))
        print(f"{'ok ' if err is None else 'ERR'} {rel}", flush=True)
        time.sleep(CALL_DELAY_S)
        return data if err is None else None


def fetch_json(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    headers: Optional[Dict[str, str]],
    attempts: int = 3,
):
    """GET ``url`` with retries.

    Returns:
        ``(data, None)`` on a 200 with a JSON body, else ``(None, error_dict)``
        describing the last failure.
    """
    err: Dict[str, Any] = {}
    for attempt in range(attempts):
        try:
            r = session.get(url, params=params, headers=headers, timeout=60)
            if r.status_code == 200:
                return r.json(), None
            err = {"status": r.status_code, "body": r.text[:500]}
            if r.status_code in (401, 403, 404):  # retrying will not change these
                break
        except (requests.RequestException, ValueError) as e:
            err = {"error": repr(e)}
        time.sleep(1.5 * (attempt + 1))
    return None, err


def last_scoring_period(status: Optional[Dict[str, Any]]) -> int:
    """How many weeks to snapshot, from an ``mStatus``+``mSettings`` response.

    A live season stops at the latest period ESPN has opened; a finished one at
    its final period. With no usable response, fall back to an 18-week season --
    asking for a week that has not happened returns an empty schedule, not an error.
    """
    if not status:
        return 18
    st = status.get("status", {})
    latest = max(st.get("latestScoringPeriod") or 0, status.get("scoringPeriodId") or 0)
    final = st.get("finalScoringPeriod") or 0
    if latest and final:
        return min(latest, final)
    return latest or final or 18


def fetch_espn(fx: Fetcher) -> int:
    """Pull every ESPN view into ``fx.out``. Returns the last week snapshotted."""
    fx.grab("league/_base")
    for view in LEAGUE_VIEWS:
        fx.grab(f"league/{view}", {"view": view})

    status = fx.grab("league/_status", {"view": ["mStatus", "mSettings"]})
    last = last_scoring_period(status)

    for wk in range(1, last + 1):
        for view in WEEK_VIEWS:
            fx.grab(f"weeks/week_{wk:02d}/{view}", {"view": view, "scoringPeriodId": wk})

    y = fx.season
    fx.grab(
        "players/kona_player_info",
        {"view": "kona_player_info", "scoringPeriodId": last},
        filt={"players": {
            "limit": 3000,
            "sortPercOwned": {"sortPriority": 1, "sortAsc": False},
            "filterStatsForTopScoringPeriodIds": {
                "value": last, "additionalValue": [f"00{y}", f"10{y}", f"11{y}", f"02{y}"]},
        }},
    )
    fx.grab(
        "players/players_wl",
        {"view": "players_wl", "scoringPeriodId": 0},
        filt={"filterActive": {"value": True}},
        url=f"{API}/seasons/{y}/players",
    )

    newest_first = {"sortMessageDate": {"sortPriority": 1, "sortAsc": False}}
    fx.grab(
        "activity/transactions_feed",
        {"view": "kona_league_communication"},
        filt={"topics": {
            "filterType": {"value": ["ACTIVITY_TRANSACTIONS"]}, "limit": 2000, "offset": 0,
            "limitPerMessageSet": {"value": 25}, **newest_first}},
        sub="/communication/",
        optional=not fx.current,
    )
    fx.grab(
        "activity/all_topics",
        {"view": "kona_league_communication"},
        filt={"topics": {"limit": 2000, "offset": 0, **newest_first}},
        sub="/communication/",
        optional=not fx.current,
    )
    return last


def copy_local(key: str, season: int, dest: Path) -> List[str]:
    """Copy the pipeline's derived files for this league, if this machine has them.

    Returns:
        The file names copied; empty when the store has never been built here.
    """
    copied: List[str] = []
    store = paths.STORE_DIR / str(season) / key
    if store.is_dir():
        dest.mkdir(parents=True, exist_ok=True)
        for f in sorted(store.iterdir()):
            if f.is_file():
                shutil.copy2(f, dest / f.name)
                copied.append(f.name)

    if SCORING_CSV.exists():
        with SCORING_CSV.open(newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            rows = [r for r in reader if len(r) > 2 and r[0] == str(season) and r[1] == key]
        if header and rows:
            dest.mkdir(parents=True, exist_ok=True)
            with (dest / "scoring_rules.csv").open("w", newline="") as fh:
                csv.writer(fh).writerows([header, *rows])
            copied.append("scoring_rules.csv")
    return copied


def write_readme(root: Path, display: str, league_id: int, season: int, last: int,
                 league_name: str, local: List[str]) -> None:
    """Describe the backup's layout, for whoever opens the zip without this repo."""
    local_line = (
        "local_store/         Derived files from the ESPN_FFL pipeline: " + ", ".join(local)
        if local else "local_store/         (none -- this league's store was not built on this machine)"
    )
    (root / "README.txt").write_text(
        f"""{display} ("{league_name}") backup - ESPN league {league_id}, season {season}
Taken {datetime.date.today().isoformat()}, through scoring period {last}.

espn_raw/            Raw JSON straight from ESPN's fantasy API
  league/            Every league-level view: settings, teams/members, rosters,
                     schedule + scores, box scores, standings, draft detail,
                     transactions, pending transactions, positional ratings, ...
  weeks/week_NN/     Point-in-time snapshot per week: rosters/lineups, matchups,
                     box scores (player-level points), scoreboard, transactions
  players/           kona_player_info (top players w/ stats & ownership; ESPN caps
                     it near 1000) and players_wl (every active player)
  activity/          League activity feed + message board topics
  _fetch_log.json    Every call made and whether it succeeded
{local_line}

Made by `python -m Scripts.backup_league` in the ESPN_FFL repo.
"""
    )


def run(league: str, season: Optional[int], out_dir: Path) -> int:
    """Back up one league-season. Returns the process exit code."""
    config = load_config()
    lg = resolve_league(league, config)
    live = get_season(config)
    season = season or live
    if not lg["start"] <= season <= max(lg["end"], live):
        print(f"{lg['display_name']} has no {season} season (configured {lg['start']}-{lg['end']}).")
        return 2

    stamp = datetime.date.today().isoformat()
    name = f"{lg['display_name'].replace(' ', '_')}_{season}_backup_{stamp}"
    root = BACKUP_DIR / name
    if root.exists():
        shutil.rmtree(root)
    raw = root / "espn_raw"

    session = requests.Session()
    session.cookies.update({"espn_s2": lg["ESPN_S2"], "SWID": lg["SWID"]})
    fx = Fetcher(lg["ID"], season, season >= live, raw, session)
    last = fetch_espn(fx)

    settings_file = raw / "league" / "mSettings.json"
    try:
        league_name = json.loads(settings_file.read_text())["settings"]["name"]
    except (OSError, ValueError, KeyError, TypeError):
        league_name = lg["display_name"]

    local = copy_local(lg["key"], season, root / "local_store")
    (raw / "_fetch_log.json").write_text(json.dumps({
        "league_id": lg["ID"], "season": season, "last_scoring_period": last,
        "fetched_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "calls": [vars(c) for c in fx.log],
    }, indent=1))
    write_readme(root, lg["display_name"], lg["ID"], season, last, league_name, local)

    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = Path(shutil.make_archive(str(out_dir / name), "zip", BACKUP_DIR, name))

    failed = [c for c in fx.log if not c.ok and not c.optional]
    skipped = [c for c in fx.log if not c.ok and c.optional]
    print(f"\n{len(fx.log) - len(failed) - len(skipped)}/{len(fx.log)} calls ok, "
          f"weeks 1-{last}, {len(local)} local files")
    for c in skipped:
        print(f"  optional, unavailable: {c.file}")
    for c in failed:
        print(f"  FAILED: {c.file} {c.error}")
    print(f"zip: {zip_path} ({zip_path.stat().st_size / 1e6:.1f} MB)")
    return 1 if failed else 0


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point."""
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--league", required=True, help="display name or config key")
    p.add_argument("--season", type=int, help="defaults to config.yaml season")
    p.add_argument("--out", type=Path, default=Path.home() / "Downloads",
                   help="where the zip goes (default ~/Downloads)")
    args = p.parse_args(argv)
    return run(args.league, args.season, args.out.expanduser())


if __name__ == "__main__":
    sys.exit(main())
