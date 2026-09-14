# Base
import json
import re
import time
from datetime import datetime
import requests
import polars as pl
import pandas as pd
from io import StringIO

# Scrape
from bs4 import BeautifulSoup

# Get Schedule and Active Week
from Scripts.nfl_utils import current_season, current_week
from Scripts.paths import season_dir

WEEK = current_week()
SEASON = current_season()

#: robots.txt asks for five seconds between requests. Honour it.
#:
#: `https://www.fantasypros.com/robots.txt` sets `Crawl-delay: 5` and disallows `/api/`,
#: `/json/`, `/ajax/` and `/xml/`. This scraper reads `/nfl/projections/`, which is
#: allowed -- but it used to fire six requests per week back to back with no pause at
#: all, and a measured 10.2s time-to-first-byte on one request during testing suggests
#: that was being noticed. Whatever endpoint might serve this more conveniently under
#: `/ajax/` is off-limits, the same call this repo made for BetOnline and
#: Pro-Football-Reference.
CRAWL_DELAY_SECONDS: float = 5.0

#: Browser UA. The default `python-requests/x.y` gets a different page.
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def _session_cookie():
    """The logged-in FantasyPros cookie from ``config.yaml``, or None.

    **This is what decides whether the scrape is worth running.** Anonymously,
    FantasyPros serves exactly ten rows per position behind a registration fence
    ("Create a free account to unlock"), so the whole scrape returns 60 players --
    which is what every board built before 2026-08-24 was blended on. A *free*
    account (tier `basic`) lifts it: measured the same day, 592 rows against 60, and
    D/ST goes from ten teams to all thirty-two.

    Optional on purpose. Without it the scrape still runs and still returns the top
    ten per position, and :func:`get_fp` says so loudly rather than silently
    producing a tenth of the data -- this repo's recurring failure mode is an absent
    source reading as agreement.

    Returns:
        Optional[str]: The ``Cookie`` header value, or None when unconfigured.
    """
    try:
        from Scripts.config_utils import load_config
        return (load_config().get("fantasypros") or {}).get("cookie") or None
    except Exception:
        return None

# FantasyPros serves full-season ("draft") projections under this sentinel in
# place of a week number. Used to build the pre-season draft board.
DRAFT_WEEK = "draft"

pos_list = ['qb', 'rb', 'wr', 'te', 'k', 'dst']

team_map = {'Kansas City Chiefs': 'KC',
           'Tampa Bay Buccaneers': 'TB',
           'Seattle Seahawks': 'SEA',
           'New Orleans Saints': 'NO',
           'Chicago Bears': 'CHI',
           'Cincinnati Bengals': 'CIN',
           'Buffalo Bills': 'BUF',
           'San Francisco 49ers': 'SF',
           'Dallas Cowboys': 'DAL',
           'Atlanta Falcons': 'ATL',
           'New York Giants': 'NYG',
           'Los Angeles Chargers': 'LAC',
           'Houston Texans': 'HOU',
           'Miami Dolphins':'MIA',
           'Cleveland Browns':'CLE',
           'Green Bay Packers':'GB',
           'Tennessee Titans': 'TEN',
           'Pittsburgh Steelers':'PIT',
           'Las Vegas Raiders':'LV',
           'Washington Commanders': 'WAS',
           'Indianapolis Colts':'IND',
           'Baltimore Ravens':'BAL',
           'Denver Broncos':'DEN',
           'Detroit Lions':'DET',
           'New York Jets':'NYJ',
           'Philadelphia Eagles':'PHI',
           'Jacksonville Jaguars':'JAX',
           'New England Patriots':'NE',
           'Arizona Cardinals':'ARI',
           'Los Angeles Rams': 'LAR',
           'Carolina Panthers': 'CAR',
           'Minnesota Vikings': 'MIN'
           }
# NOT `dst_map = team_map = {...}`, which is what this said until 2026-08-24 and
# which rebound `team_map` to this dict -- so D/ST rows stored a `playerTeam` of
# "Texans D/ST" instead of "HOU", and the abbreviation map was unreachable
# afterwards. Latent rather than fatal only because the join runs on name.
dst_map = {'Kansas City Chiefs': 'Chiefs D/ST',
           'Tampa Bay Buccaneers': 'Buccaneers D/ST',
           'Seattle Seahawks': 'Seahawks D/ST',
           'New Orleans Saints': 'Saints D/ST',
           'Chicago Bears': 'Bears D/ST',
           'Cincinnati Bengals': 'Bengals D/ST',
           'Buffalo Bills': 'Bills D/ST',
           'San Francisco 49ers': '49ers D/ST',
           'Dallas Cowboys': 'Cowboys D/ST',
           'Atlanta Falcons': 'Falcons D/ST',
           'New York Giants': 'Giants D/ST',
           'Los Angeles Chargers': 'Chargers D/ST',
           'Houston Texans': 'Texans D/ST',
           'Miami Dolphins':'Dolphins D/ST',
           'Cleveland Browns':'Browns D/ST',
           'Green Bay Packers':'Packers D/ST',
           'Tennessee Titans': 'Titans D/ST',
           'Pittsburgh Steelers':'Steelers D/ST',
           'Las Vegas Raiders':'Raiders D/ST',
           'Washington Commanders': 'Commanders D/ST',
           'Indianapolis Colts':'Colts D/ST',
           'Baltimore Ravens':'Ravens D/ST',
           'Denver Broncos':'Broncos D/ST',
           'Detroit Lions':'Lions D/ST',
           'New York Jets':'Jets D/ST',
           'Philadelphia Eagles':'Eagles D/ST',
           'Jacksonville Jaguars':'Jaguars D/ST',
           'New England Patriots':'Patriots D/ST',
           'Arizona Cardinals':'Cardinals D/ST',
           'Los Angeles Rams': 'Rams D/ST',
           'Carolina Panthers': 'Panthers D/ST',
           'Minnesota Vikings': 'Vikings D/ST'
           }

def get_fp(wk, year=None):
    """Scrape FantasyPros projections for one week.

    Args:
        wk: Week number, or ``DRAFT_WEEK`` (``"draft"``) for full-season
            projections. FantasyPros accepts the literal string ``draft`` in
            place of a week number; the response layout is identical, so the
            same parser handles both.
        year: Season to fetch. **This works, and this repo believed for a year
            that it did not.** `docs/plans/03` and `STATE_OF_THE_REPO.md` both
            recorded that "FantasyPros URLs take no season parameter, so the 2025
            CSV cannot be reproduced by re-scraping" -- the parameter is `year`,
            not `season`, and `season=` really is ignored, which is presumably how
            the wrong conclusion was reached. Verified 2026-08-24 against the
            archived 2025 week 1: all ten shared running backs matched to the
            decimal, and the live pull returned 161 of them against the archive's
            27. None when the current season is wanted.

    Returns:
        pd.DataFrame: One row per player with ``proj_``-prefixed stat columns.
        The ``week`` column carries ``wk`` verbatim, so season-long rows are
        labelled ``"draft"`` rather than a week number.
    """
    proj_dfs = []

    cookie = _session_cookie()
    headers = {"User-Agent": USER_AGENT}
    if cookie:
        headers["Cookie"] = cookie
    else:
        print("  WARNING: no FantasyPros session in config.yaml -- the registration "
              "fence caps every position at 10 rows, so this returns ~60 players "
              "rather than ~590. See _session_cookie().")

    for i, pos in enumerate(pos_list):
        # Build and Get URL
        url = (f"https://www.fantasypros.com/nfl/projections/{pos}.php"
               f"?max-yes=false&min-yes=false&scoring=STD&week={wk}")
        if year is not None:
            url += f"&year={int(year)}"
        if i:
            time.sleep(CRAWL_DELAY_SECONDS)
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        if 'id="registration-fence"' in response.text or "registration-fence" in response.text:
            print(f"  NOTE: {pos} still served the registration fence -- the session "
                  "cookie is missing or expired, and this position is capped at 10.")
        soup = BeautifulSoup(response.content, "lxml")

        # Find the table and extract HTML
        table_html = str(soup.find('table', id='data'))

        # Read the table into a pandas DataFrame
        df = pd.read_html(StringIO(table_html))[0]

        final_cols = ['week', 'player_name', 'playerTeam',
                      'proj_passingAttempts', 'proj_passingCompletions', 'proj_passingYards', 'proj_passingTouchdowns', 'proj_passingInterceptions',
                      'proj_rushingAttempts', 'proj_rushingYards', 'proj_rushingTouchdowns',
                      'proj_receivingReceptions', 'proj_receivingYards', 'proj_receivingTouchdowns',
                      'proj_lostFumbles',
                      'proj_defensiveSacks', 'proj_defensiveInterceptions', 'proj_defensiveFumbles', 'proj_defensiveTouchdowns', 'proj_defensiveSafeties', 'proj_defensivePointsAllowed', 'proj_defensiveYardsAllowed',
                      'STD_FantasyPoints'
                      ]

        # Clean Column Names
        if pos == 'qb':
            df.columns = ['player_name',
                        'proj_passingAttempts', 'proj_passingCompletions', 'proj_passingYards', 'proj_passingTouchdowns', 'proj_passingInterceptions',
                        'proj_rushingAttempts', 'proj_rushingYards', 'proj_rushingTouchdowns',
                        'proj_lostFumbles',
                        'STD_FantasyPoints']

        if pos == 'rb':
            df.columns = ['player_name',
                        'proj_rushingAttempts', 'proj_rushingYards', 'proj_rushingTouchdowns',
                        'proj_receivingReceptions', 'proj_receivingYards', 'proj_receivingTouchdowns',
                        'proj_lostFumbles',
                        'STD_FantasyPoints']

        if pos == 'wr':
            df.columns = ['player_name',
                          'proj_receivingReceptions', 'proj_receivingYards', 'proj_receivingTouchdowns',
                          'proj_rushingAttempts', 'proj_rushingYards', 'proj_rushingTouchdowns',
                          'proj_lostFumbles',
                          'STD_FantasyPoints']

        if pos == 'te':
            df.columns = ['player_name',
                          'proj_receivingReceptions', 'proj_receivingYards', 'proj_receivingTouchdowns',
                          'proj_lostFumbles',
                          'STD_FantasyPoints']
        
        if pos == 'k':
            df = df[['Player', 'FPTS']]
            df.columns = ['player_name', 'STD_FantasyPoints']

        if pos == 'dst':
            df = df[['Player', 'SACK', 'INT', 'FR', 'TD', 'SAFETY', 'PA', 'YDS AGN', 'FPTS']]
            df.columns = ['player_name',
                          'proj_defensiveSacks', 'proj_defensiveInterceptions', 'proj_defensiveFumbles', 'proj_defensiveTouchdowns', 'proj_defensiveSafeties', 'proj_defensivePointsAllowed', 'proj_defensiveYardsAllowed',
                          'STD_FantasyPoints']

        # Split Name and Team
        if pos == 'dst':
             df['playerTeam'] = df['player_name']
             df.replace({"playerTeam": team_map}, inplace=True)
             df.replace({'player_name': dst_map}, inplace=True)
        else:
            df['playerTeam'] = df['player_name'].str.split().str[-1]
            df['player_name'] = df['player_name'].str.rsplit(' ', n=1).str[0]

        # Add missing columns with 0 value
        for col in final_cols:
            if col not in df.columns:
                df[col] = 0

        # Add Constants
        df['week'] = wk
        

        df = df[final_cols]

        df['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Map Player Names
        player_map = {"Patrick Mahomes II": "Patrick Mahomes", "Gardner Minshew II": "Gardner Minshew"}
        df.replace({"player_name": player_map}, inplace=True)

        proj_dfs.append(df)

    out = pd.concat(proj_dfs, ignore_index=True)

    # Fill the *stat* columns only. `.fillna(0)` across the whole frame was the previous
    # line, and it turned a missing player name into the integer 0 -- which then failed
    # the parquet write with "Expected bytes, got a 'int' object" rather than producing a
    # visibly wrong row, so it only surfaced when the 2025 season-long table happened to
    # carry one blank. A nameless row is not a player and cannot be joined to anything, so
    # it is dropped rather than zero-filled; the count is printed because silently losing
    # rows is how a scraper stops being trustworthy.
    identity = ["week", "player_name", "playerTeam", "TimeStamp"]
    numeric = [c for c in out.columns if c not in identity]
    out[numeric] = out[numeric].fillna(0)

    named = out["player_name"].apply(lambda v: isinstance(v, str) and v.strip() != "")
    if not named.all():
        print(f"  dropped {int((~named).sum())} row(s) with no player name")
    return out[named].reset_index(drop=True)


#: Join key of the weekly file, and what a merge de-duplicates on.
WEEKLY_KEYS = ["week", "player_name"]


def parse_weeks(spec):
    """``"1-3"`` or ``"1,4,7"`` to a list of week numbers.

    Args:
        spec: The ``--weeks`` argument, or None.

    Returns:
        list | None: Weeks, in ascending order without duplicates, or None when
        ``spec`` is empty so the caller falls back to the current week.

    Raises:
        ValueError: On anything that is not a number or an ascending range.
    """
    if not spec:
        return None
    weeks = set()
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            low, _, high = part.partition("-")
            low, high = int(low), int(high)
            if high < low:
                raise ValueError(f"week range {part!r} runs backwards")
            weeks.update(range(low, high + 1))
        else:
            weeks.add(int(part))
    return sorted(weeks)


def scrape_weekly(season=None, week=None, weeks=None, merge=True):
    """Scrape the current week's projections and merge them into the season file.

    **The current week alone, and merged rather than rewritten.** This used to fetch
    ``range(1, week + 1)`` on every call, which is wrong in two directions once a
    season is under way.

    Too slow: six requests a week at the 5s crawl delay ``robots.txt`` asks for is
    30s at week 1 and **nine minutes at week 18**, on a nightly job.

    Worse, it re-requests a *projections* page for weeks already played. What
    FantasyPros serves for a completed week is not necessarily the number it served
    before kickoff, so re-scraping quietly rewrites history -- and the whole point of
    this file is to be the pre-game opinion the blend voted with. Merging with
    ``keep="first"`` on the existing rows freezes each week at first capture, the
    same rule ``Scripts.freeze`` applies to the draft board.

    The file **must stay cumulative** whichever way it is written:
    :func:`Scripts.projection_utils.clean_lineups` re-merges it onto every week in
    the lineup frame on ``["week", "player_name"]``, and that frame gains a week
    every Tuesday. A current-week-only file would blank FantasyPros for every prior
    week and turn stored history into an ESPN-only board retroactively.

    Args:
        season: Season for the output path. Defaults to the schedule's season.
        week: The week to fetch. Defaults to the schedule's current week, resolved
            **at call time** -- ``WEEK`` is bound at import, and with a stale
            schedule that is week 1 for the rest of the season.
        weeks: Explicit weeks to fetch, for a backfill. Overrides ``week``.
        merge: Combine with whatever the file already holds, existing rows winning.
            False rewrites it, which is how to replace a bad capture on purpose.

    Returns:
        pd.DataFrame: The whole file as written -- every week it now holds, not just
        the ones this call fetched, so a caller can row-count what shipped.
    """
    season = SEASON if season is None else season
    if weeks is None:
        weeks = [current_week() if week is None else week]
    weeks = [int(w) for w in weeks]

    # `season` used to name the output directory and nothing else, so asking for 2025
    # wrote the *current* season's numbers into `Data/Projections/FantasyPros/2025/`
    # under 2025's name. It never fired because nobody could backfill -- the season
    # parameter was believed not to exist. It does (`year`), so the argument now
    # reaches the request as well as the path.
    year = None if int(season) == int(SEASON) else int(season)
    fetched = pd.concat([get_fp(wk=w, year=year) for w in weeks], ignore_index=True)

    parquet = season_dir("FantasyPros", season,
                         "FantasyPros_Projections_Week_All.parquet")
    df = fetched
    if merge and parquet.is_file():
        existing = pd.read_parquet(parquet)
        # Existing first, so `keep="first"` preserves a week already captured.
        df = pd.concat([existing, fetched], ignore_index=True)
        df = df.drop_duplicates(subset=WEEKLY_KEYS, keep="first")
        df = df.sort_values(WEEKLY_KEYS).reset_index(drop=True)

    df.to_parquet(parquet)
    df.to_csv(parquet.with_suffix(".csv"), index=False)

    scraped = int(fetched["player_name"].nunique())
    print(f"FantasyPros weekly {season}: fetched week(s) "
          f"{', '.join(str(w) for w in weeks)} -- {len(fetched)} rows, "
          f"{scraped} players; file now holds {len(df)} rows over weeks "
          f"{sorted(pd.unique(df['week']))}")
    if scraped <= 60:
        print("  NOTE: FantasyPros caps its public tables at 10 rows per position. "
              "This is a top-10 teaser, not full coverage -- check the "
              "`fantasypros.cookie` session in config.yaml (docs/plans/03).")
    return df


def scrape_season_long(season=None):
    """Scrape full-season projections -- the draft-board input.

    ``DRAFT_WEEK`` was defined but never called: the module-level loop only ever
    ran ``range(1, WEEK + 1)``, so pre-season that is week 1 alone and the
    season-long table was never fetched despite being one argument away.

    Args:
        season: Season for the output path. Defaults to the schedule's season.

    Returns:
        pd.DataFrame: One row per player, ``week`` set to ``"draft"``.
    """
    season = SEASON if season is None else season

    year = None if int(season) == int(SEASON) else int(season)
    df = get_fp(wk=DRAFT_WEEK, year=year)
    out = season_dir("FantasyPros", season, "FantasyPros_Projections_Season.parquet")
    df.to_parquet(out)
    df.to_csv(out.with_suffix(".csv"), index=False)
    print(f"FantasyPros season-long {season}: {len(df)} rows, "
          f"{df['player_name'].nunique()} players")
    if len(df) <= 60:
        print("  NOTE: FantasyPros caps its public tables at 10 rows per position. "
              "This is a top-10 teaser, not full coverage -- the blend renormalises "
              "around it (docs/plans/03).")
    return df


# --- rest of season -------------------------------------------------------
#
# A different page from the projections above, and it has to be: on
# `/nfl/projections/`, `week=ros` silently returns **week 1**, a future `week=N`
# silently returns **last season's** week N, and `week=draft` is capped at 10 rows
# per position even with the session cookie. All three were probed on 2026-09-10.
# None of them errors -- each returns a plausible table of the wrong thing, which
# is the worst failure shape a source can have.
#
# `/nfl/rankings/ros-<pos>.php` is allowed by robots.txt (which disallows only
# `/ajax/`, `/api/`, `/json/`, `/xml/` and `/nfl/ranker/`) and honours the same
# `Crawl-delay: 5` the projections scrape already does.

#: The rest-of-season rankings page, one per position.
#:
#: The bare slug is the STD scoring variant for every position -- verified, not
#: assumed -- which matches what :func:`get_fp` already pulls, so the two
#: FantasyPros artifacts are denominated the same way. PPR and half-PPR variants
#: exist at `ros-ppr-<pos>` and `ros-half-point-ppr-<pos>` and are deliberately not
#: used: the repo scores every league from stat lines in its own rules, so picking
#: a scoring flavour here would be choosing one league's rules for all of them.
ROS_URL = "https://www.fantasypros.com/nfl/rankings/ros-{pos}.php"

#: The same six positions :data:`pos_list` covers.
#:
#: **There is no IDP rest-of-season page.** FantasyPros does not publish one, which
#: on GOP_Degenerates means 150 of 284 free-agent rows can never carry an ROS
#: number. That is a gap in the source and must read as an abstention downstream,
#: never as a projection of zero.
ROS_POSITIONS = tuple(pos_list)

#: The table is rendered by JavaScript, so the rows are read from the JSON the page
#: ships them in rather than from the DOM. `re.S` because the blob is multi-line;
#: non-greedy up to the terminating `};` because several other `var` blocks follow.
_ECR_DATA = re.compile(r"var\s+ecrData\s*=\s*(\{.*?\});\s*\n", re.S)

#: Fields taken verbatim off each `ecrData["players"]` row.
#:
#: `rank_min`/`rank_max`/`rank_std` come along because **this consensus is thin** --
#: `total_experts` is 2-3 on these pages against 100+ on the in-season rankings --
#: and a spread over three opinions has to travel with the number it describes.
#: `r2p_pts` is kept and is *not* to be divided by our own games-remaining: it is a
#: total over the games FantasyPros thinks the player will play, so mixing their
#: numerator with our denominator discounts a known absence twice.
ROS_PLAYER_FIELDS = (
    "player_name", "player_team_id", "player_positions", "player_bye_week",
    "player_owned_avg", "rank_ecr", "rank_min", "rank_max", "rank_ave",
    "rank_std", "pos_rank", "r2p_pts", "player_ecr_delta",
)

#: What a row is keyed by once captured. The capture date is part of the key
#: because this file **accumulates**: a rest-of-season opinion is only meaningful
#: as of a date, and overwriting it would make the source permanently
#: unfalsifiable -- there is no archive of what the consensus said in week 5.
ROS_KEYS = ["captured_date", "position", "player_name"]

#: FantasyPros' team abbreviations, where they differ from ESPN's.
#:
#: Only two of thirty-two, and D/ST joins on this key rather than on the name, so
#: each one is a defence that silently never matches. Caught by gate G-R0 on the
#: first run: every league was missing exactly its Jaguars and its Commanders.
#: FantasyPros writes `LAR` like ESPN, so the nflverse map in
#: `Scripts/nfl_utils.ESPN_TEAM_ALIASES` is the wrong one to reach for -- this is a
#: fact about FantasyPros, and it lives here for the same reason that one lives
#: beside the schedule.
ROS_TEAM_ALIASES = {"JAC": "JAX", "WAS": "WSH"}


def parse_ecr_data(html):
    """The ``ecrData`` blob a rankings page embeds.

    Args:
        html: The page source.

    Returns:
        dict: The decoded payload, with ``players`` among its keys.

    Raises:
        ValueError: When the blob is absent or will not decode -- which means
            FantasyPros has restructured the page, and is a build failure rather
            than an empty result. A rankings scrape that silently returns nothing
            looks exactly like a week in which nobody was ranked.
    """
    found = _ECR_DATA.search(html or "")
    if not found:
        raise ValueError(
            "no `var ecrData` on the page. FantasyPros has changed the rankings "
            "template, or the response was an interstitial rather than the page."
        )
    try:
        return json.loads(found.group(1))
    except json.JSONDecodeError as exc:
        raise ValueError(f"ecrData did not decode as JSON: {exc}") from exc


def get_ros(pos, year=None, *, html=None):
    """Rest-of-season consensus for one position.

    Args:
        pos: One of :data:`ROS_POSITIONS`.
        year: Season for the ``year`` query parameter. None for the current one.
        html: Pre-fetched page source, for tests. Skips the request entirely.

    Returns:
        pd.DataFrame: One row per ranked player, with the envelope's
        ``total_experts``, ``scoring`` and ``last_updated`` stamped on every row so
        a stored snapshot can say how thin the consensus behind it was.
    """
    if html is None:
        url = ROS_URL.format(pos=pos)
        if year is not None:
            url += f"?year={int(year)}"
        headers = {"User-Agent": USER_AGENT}
        cookie = _session_cookie()
        if cookie:
            headers["Cookie"] = cookie
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        html = response.text

    payload = parse_ecr_data(html)
    players = payload.get("players") or []
    rows = [{field: player.get(field) for field in ROS_PLAYER_FIELDS}
            for player in players]
    df = pd.DataFrame(rows, columns=list(ROS_PLAYER_FIELDS))

    df["position"] = pos.upper()
    df["total_experts"] = payload.get("total_experts")
    df["scoring"] = payload.get("scoring")
    df["ranking_type"] = payload.get("type")
    df["last_updated"] = payload.get("last_updated")
    df["source_year"] = payload.get("year")

    # `pro_team` is the join key the store uses, and for D/ST it is the *only* one
    # that works: FantasyPros names a defence "Houston Texans" where the store says
    # "Texans D/ST", so 0 of 14 match by name. The abbreviation matches directly.
    df["pro_team"] = df["player_team_id"].replace(ROS_TEAM_ALIASES)
    if pos == "dst":
        df["player_name"] = df["player_name"].replace(dst_map)

    # The same two the weekly scrape rewrites, for the same reason.
    df["player_name"] = df["player_name"].replace(
        {"Patrick Mahomes II": "Patrick Mahomes",
         "Gardner Minshew II": "Gardner Minshew"})

    for column in ("r2p_pts", "rank_min", "rank_max", "rank_ave", "rank_std",
                   "player_owned_avg", "player_ecr_delta"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def scrape_ros(season=None, week=None, merge=True):
    """Scrape every position's rest-of-season consensus and append it to the file.

    **Append-only, keyed by capture date, and that is the point.** There is no
    archive anywhere of what a rest-of-season consensus said in a past week --
    FantasyPros serves only today's, and the `year` parameter that rescues the
    weekly projections does not rescue this. Overwriting would leave the source
    permanently unmeasurable: you could never ask whether week 5's ranking ordered
    week 6-18 correctly. At ~485 rows a night it is about 58k rows by January.

    Args:
        season: Season for the output path. Defaults to the schedule's season.
        week: Week to stamp the capture with. Defaults to the current week.
        merge: Append to the stored file. False rewrites it, for a repair.

    Returns:
        pd.DataFrame: The whole accumulated file, not just this capture.
    """
    season = SEASON if season is None else season
    week = WEEK if week is None else week
    year = None if int(season) == int(SEASON) else int(season)

    frames = []
    for i, pos in enumerate(ROS_POSITIONS):
        if i:
            time.sleep(CRAWL_DELAY_SECONDS)
        frames.append(get_ros(pos, year=year))

    fresh = pd.concat(frames, ignore_index=True)
    stamp = datetime.now()
    fresh["captured_at"] = stamp.strftime("%Y-%m-%d %H:%M:%S")
    fresh["captured_date"] = stamp.strftime("%Y-%m-%d")
    fresh["captured_week"] = week
    fresh["season"] = int(season)

    out = season_dir("FantasyPros", season, "FantasyPros_ROS_Ranks.parquet")
    combined = fresh
    if merge and out.exists():
        previous = pd.read_parquet(out)
        # `keep="last"` so a same-day re-run refreshes rather than duplicates, which
        # is the opposite of the weekly file's rule and for the opposite reason:
        # there the point is to freeze a pre-game opinion, here it is to hold the
        # most recent read of a forecast that is *meant* to move.
        combined = (pd.concat([previous, fresh], ignore_index=True)
                    .drop_duplicates(subset=ROS_KEYS, keep="last")
                    .reset_index(drop=True))

    combined.to_parquet(out)
    combined.to_csv(out.with_suffix(".csv"), index=False)

    experts = fresh.groupby("position")["total_experts"].max().to_dict()
    priced = int(fresh["r2p_pts"].notna().sum())
    print(f"FantasyPros ROS {season} week {week}: {len(fresh)} ranked, "
          f"{priced} with points; file now {len(combined)} rows over "
          f"{combined['captured_date'].nunique()} captures")
    print(f"  experts per position: {experts}")
    if len(fresh) <= 60:
        print("  NOTE: 60 rows or fewer -- the registration fence is back. See "
              "_session_cookie().")
    return combined


def main(argv=None):
    """Command-line entry point.

    The scrape used to run at import time with no ``__main__`` guard, so simply
    importing this module fired a live scrape and overwrote the season's file.
    """
    import argparse

    p = argparse.ArgumentParser(
        prog="python -m Scripts.scrape_FP",
        description="Scrape FantasyPros projections.",
    )
    p.add_argument("--season", type=int, help="defaults to the schedule's season")
    p.add_argument("--week", type=int,
                   help="week for the weekly scrape; defaults to the current week")
    p.add_argument("--weeks",
                   help="explicit weeks to fetch, e.g. 1-3 or 1,4,7 (a backfill)")
    p.add_argument("--no-merge", action="store_true",
                   help="rewrite the weekly file instead of merging into it")
    # `both` still means weekly+season. `ros` is a third thing rather than a
    # widening of the default: the nightly names its stages explicitly, and
    # silently adding six requests to a default a human invokes by hand is how a
    # crawl delay turns into a rate limit.
    p.add_argument("--what", choices=["weekly", "season", "both", "ros"],
                   default="both")
    args = p.parse_args(argv)

    if args.what in ("weekly", "both"):
        scrape_weekly(season=args.season, week=args.week,
                      weeks=parse_weeks(args.weeks), merge=not args.no_merge)
    if args.what in ("season", "both"):
        scrape_season_long(season=args.season)
    if args.what == "ros":
        scrape_ros(season=args.season, week=args.week,
                   merge=not args.no_merge)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())