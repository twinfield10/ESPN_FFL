# Base
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

    return pd.concat(proj_dfs, ignore_index=True).fillna(0)


def scrape_weekly(season=None, week=None):
    """Scrape week-by-week projections and write the season file.

    Args:
        season: Season for the output path. Defaults to the schedule's season.
        week: Highest week to fetch. Defaults to the schedule's current week.

    Returns:
        pd.DataFrame: One row per player-week.
    """
    season = SEASON if season is None else season
    week = WEEK if week is None else week

    # `season` used to name the output directory and nothing else, so asking for 2025
    # wrote the *current* season's numbers into `Data/Projections/FantasyPros/2025/`
    # under 2025's name. It never fired because nobody could backfill -- the season
    # parameter was believed not to exist. It does (`year`), so the argument now
    # reaches the request as well as the path.
    year = None if int(season) == int(SEASON) else int(season)
    proj_list = [get_fp(wk=w, year=year) for w in range(1, week + 1)]
    df = pd.concat(proj_list, ignore_index=True)

    df.to_csv(season_dir("FantasyPros", season, "FantasyPros_Projections_Week_All.csv"))
    df.to_parquet(season_dir("FantasyPros", season,
                             "FantasyPros_Projections_Week_All.parquet"))
    print(f"FantasyPros weekly {season}: {len(df)} rows, weeks 1-{week}, "
          f"{df['player_name'].nunique()} players")
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
    p.add_argument("--week", type=int, help="highest week for the weekly scrape")
    p.add_argument("--what", choices=["weekly", "season", "both"], default="both")
    args = p.parse_args(argv)

    if args.what in ("weekly", "both"):
        scrape_weekly(season=args.season, week=args.week)
    if args.what in ("season", "both"):
        scrape_season_long(season=args.season)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())