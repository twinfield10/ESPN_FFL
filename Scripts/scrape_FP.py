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

# FantasyPros serves full-season ("draft") projections under this sentinel in
# place of a week number. Used to build the pre-season draft board.
DRAFT_WEEK = "draft"

pos_list = ['qb', 'rb', 'wr', 'te', 'k', 'dst']

team_map = {'Kansas City Chiefs': 'KC',
           'Tampa Bay Buccaneers': 'TB',
           'Seattle Seahawks': 'SEA',
           'New Orleans Saints': 'NO',
           'Chicago Beaars': 'CHI',
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
dst_map = team_map = {'Kansas City Chiefs': 'Chiefs D/ST',
           'Tampa Bay Buccaneers': 'Buccaneers D/ST',
           'Seattle Seahawks': 'Seahawks D/ST',
           'New Orleans Saints': 'Saints D/ST',
           'Chicago Beaars': 'Bears D/ST',
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

def get_fp(wk):
    """Scrape FantasyPros projections for one week.

    Args:
        wk: Week number, or ``DRAFT_WEEK`` (``"draft"``) for full-season
            projections. FantasyPros accepts the literal string ``draft`` in
            place of a week number; the response layout is identical, so the
            same parser handles both.

    Returns:
        pd.DataFrame: One row per player with ``proj_``-prefixed stat columns.
        The ``week`` column carries ``wk`` verbatim, so season-long rows are
        labelled ``"draft"`` rather than a week number.
    """
    proj_dfs = []

    for pos in pos_list:
        # Build and Get URL
        url = f"https://www.fantasypros.com/nfl/projections/{pos}.php?max-yes=false&min-yes=false&scoring=STD&week={wk}"
        response = requests.get(url)
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

    proj_list = [get_fp(wk=w) for w in range(1, week + 1)]
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

    df = get_fp(wk=DRAFT_WEEK)
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