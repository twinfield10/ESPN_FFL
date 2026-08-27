import time
from datetime import datetime
from pathlib import Path

import numpy as np
import requests
import polars as pl
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from Scripts import market as mk
from Scripts.nfl_utils import current_season, date_week
from Scripts.paths import landing_dir, season_dir

def chrome_options():
    """Options for the scrape's browser. A function, not a module constant: building
    it at import is harmless in itself, but it sat directly above the block that
    launched a browser and scraped, and the whole point is that this module now does
    nothing until asked."""
    opts = webdriver.ChromeOptions()
    opts.add_argument('--ignore-certificate-errors')
    opts.add_argument('--ignore-ssl-errors')
    return opts

# Constants
prop_to_stat={
    "Touchdowns": 'rushingTouchdowns',
    "Rushing Yards": "rushingYards",
    "Rush Attempts": 'rushingAttempts',
    "Receiving Yards": "receivingYards",
    "Receptions": "receivingReceptions",
    "Touchdown Passes": "passingTouchdowns",
    "Pass Completions": "passingCompletions",
    "Pass Attempts": "passingAttempts",
    "Passing Yards": "passingYards",
    "Interceptions": "passingInterceptions"
}


def get_links_soup(driver, start_time):
    # Get the page's HTML and parse it with Beautiful Soup
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    # List to store the links
    links_list = []
    matchups = soup.select('a:has(div[class*="matchupMetadata"])')

    for e in matchups:
        link = e.get('href')
        if link is not None and "games" not in link:
            live_text = e.select_one('div[class*="matchupDate"] span').text if e.select_one('div[class*="matchupDate"] span') else None
            if live_text != 'Live Now':
                link = f"https://www.pinnacle.com{link}"
                links_list.append(link)

    # Print Stats
    link_end_time = time.time()
    link_elap_time = round((link_end_time - start_time)/60, 2)

    print(f"Found {len(links_list)} Games in {link_elap_time} Minutes")
    print("")
    return links_list



def get_raw_pinny_soup(driver, links_list, season=None):
    season = current_season() if season is None else season
    type_list = ["#all"]
    df_list = []

    for i in type_list:
        all_links = [link + i for link in links_list]
        for url in all_links:

            try:
                # Load URL and interact with dynamic elements using Selenium
                driver.get(url)
                print(url)
                time.sleep(1.5)

                # Get Game Date
                try:
                    full_date = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="startTime"] span'))
                    ).text
                    date = datetime.strptime(full_date, "%A, %B %d, %Y at %H:%M").strftime("%Y-%m-%d")
                except TimeoutException:
                    print(f"Failed to retrieve date for {url}")
                    continue

                # Show All Bets and Expand Markets
                try:
                    show_all_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[class*="showAllButton"]'))
                    )
                    if show_all_button.text == "Show All":
                        show_all_button.click()
                        time.sleep(0.1)

                    # Expand 'See more' markets
                    for btn in driver.find_elements(By.CSS_SELECTOR, 'button[class*="toggleMarkets"]'):
                        if btn.text == 'See more':
                            btn.click()
                            time.sleep(0.2)

                except TimeoutException:
                    print(f"Show All button or markets not found for {url}")

                # Get the page HTML after interacting with elements
                page_source = driver.page_source

                # Use BeautifulSoup for faster and efficient parsing
                soup = BeautifulSoup(page_source, 'html.parser')

                # Scrape Market Elements
                market_elements = soup.select('div[class*="primary"]')
                for melem in market_elements:
                    # Get Bet Title
                    bet_titles = [m.text for m in melem.select('span[class*="titleText"]') if m.text]
                    # Get Bet Labels and Prices
                    lab_list = [lab.text for lab in melem.select('span[class*="label"]') if lab.text]
                    price_list = [price.text for price in melem.select('span[class*="price"]') if price.text]

                    if bet_titles and lab_list and price_list:
                        # Create Data Frame    
                        bet_data = {'title': [bet_titles[0]] * len(price_list),
                                    'label': lab_list[0:len(price_list)],
                                    'Price': price_list}

                        bet_df = pl.DataFrame(bet_data).with_columns(
                            pl.lit(date).str.strptime(pl.Date, "%Y-%m-%d").alias('officialDate'),
                            pl.lit(date).alias('gameday'),
                            pl.lit(url).alias('url'),
                            pl.lit(datetime.now()).alias('BetTimeStamp')
                        )

                        # Append to df_list
                        df_list.append(bet_df)

            except Exception as e:
                print(f"An error occurred with {url}: {e}")

    # Quit Driver

    # Small Clean and Join To NFL Schedule
    df = pl.concat(df_list)
    df = df.join(date_week(), left_on='gameday', right_on='gameday', how='left').drop('gameday')
    df = df.with_columns(pl.col("title").str.replace("Josh Allen \\(BUF\\)", "Josh Allen").alias("title"))

    df.write_csv(landing_dir("Pinnacle", season, "Raw_Pinnacle_New.csv"))
    archive_raw(df, season)

    return df


def archive_raw(raw_df: pl.DataFrame, season: int) -> None:
    """Keep each week's raw prices, because the landing file is overwritten.

    **The reason plan 35 could mostly not be scored.** ``Raw_Pinnacle_New.csv`` is
    rewritten on every run, so by the time anyone asked whether the de-vig and the
    line conversion were right, the only prices left in the repo were the last
    scrape of 2025. Every earlier week survives only as the derived ``proj_``
    column, which cannot be re-derived under a new formula.

    One parquet per week fixes that going forward. ``Scripts/scrape_BOL.py`` does
    the same thing for the same reason.

    Args:
        raw_df: The raw scrape, already joined to the week.
        season: Season being scraped.

    Returns:
        None. Writes ``Raw/<season>/Pinnacle_Raw_Week_<week>.parquet`` per week
        present, so a scrape spanning two weeks does not overwrite either.
    """
    if 'week' not in raw_df.columns:
        print("Pinnacle raw archive skipped: no week column on the scrape")
        return
    for week_num in raw_df['week'].drop_nulls().unique().to_list():
        week_df = raw_df.filter(pl.col('week') == week_num)
        path = season_dir("Pinnacle", season, "Raw",
                          f"Pinnacle_Raw_Week_{week_num}.parquet")
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        week_df.write_parquet(path)
        print(f"Archived {week_df.height} raw Pinnacle prices for week {week_num}")


def clean_raw_pinny(df):

    # Get Rid of Unnecessary Bets
    filt_bets = ['Correct Score', 'Exact', 'Winning Margin', 'Winner/Total', 'Range', 'Odd/Even', 'Alternate Lines']

    print(df.filter(pl.col('title').str.contains_any(['Touchdowns', 'Anytime', 'Interceptions', 'Reception', 'Yards', 'Receptions', 'Kicking', 'Completion', 'Attempts', 'Passes'])))

    # Base Clean All Data
    final_df = df.filter(~pl.col('title').str.contains_any(filt_bets)) \
            .with_columns(pl.when(pl.col('title').str.contains_any(['Game', 'Alternate Lines'])).then(pl.lit('Game'))
                            .when(pl.col('title').str.contains('1st Half')).then(pl.lit('1H'))
                            .when(pl.col('title').str.contains('1st Quarter')).then(pl.lit('1Q'))
                            .when(pl.col('title').str.contains_any(['Touchdowns', 'Anytime', 'Interceptions', 'Reception', 'Yards', 'Receptions', 'Kicking', 'Completion', 'Attempts', 'Passes'])).then(pl.lit('PlayerProp'))
                            .otherwise(pl.lit('Other')).alias('Period')) \
            .with_columns(pl.col("url").str.extract(r'nfl\/(.*?)\/\d+\/#all').alias('AllTeams')) \
            .with_columns(pl.col("AllTeams").str.split('-vs-').alias('split_teams')) \
            .with_columns(pl.col('split_teams').list.first().str.strip_chars().str.replace_all("-", " ").str.to_titlecase().alias('Away')) \
            .with_columns(pl.col('split_teams').list.last().str.strip_chars().str.replace_all("-", " ").str.to_titlecase().alias('Home')) \
            .drop('split_teams', 'AllTeams', 'url')

    print(final_df.filter(pl.col('title').str.contains_any(['Touchdowns', 'Anytime', 'Interceptions', 'Reception', 'Yards', 'Receptions', 'Kicking', 'Completion', 'Attempts', 'Passes'])).head())
    return final_df

def clean_props(df, season=None):
    season = current_season() if season is None else season
    ## Build + Save Prop DF
    prop_df = df.filter(pl.col('title').str.contains_any(['Touchdowns', 'Anytime', 'Interceptions', 'Reception', 'Yards', 'Receptions', 'Kicking', 'Completion', 'Attempts', 'Passes']))
    print(prop_df)
    prop_df = prop_df \
                .with_columns(pl.col('title').str.replace('(BUF)', '').alias('Title')) \
                .with_columns(pl.col('Title').str.extract(r'Total\s+(.*)').alias('PropType')) \
                .with_columns(pl.col('Title').str.replace(r'\(.*?\)', '').alias('Title')) \
                .with_columns(pl.col('Title').str.extract(r'^(.*?)\s+Total').alias('Player')) \
                .with_columns(pl.col('label').str.extract(r'(Over|Under)').alias('OverUnder')) \
                .with_columns(pl.col('label').str.extract(r'(\d+\.\d+|\d+)').alias('Value')) \
                .with_columns((1/(pl.col('Price').cast(pl.Float32))).alias('Implied')) \
                .with_columns((pl.col('Implied') / (pl.col('Implied').sum().over(['officialDate', 'Away', 'Home', 'Player', 'PropType']))).alias('ImpNoVig'))\
                .with_columns(pl.lit(1).alias('start'))\
                .select(
                        pl.all().exclude("start"),
                        pl.col("start").cum_sum().over(['officialDate', 'week', 'Away', 'Home', 'Player', 'PropType']).flatten().alias("num_bets")
                        ) \
                .with_columns(
                             pl.when((pl.col('PropType') == 'Anytime TD') & (pl.col('num_bets') % 2 == 1)).then(pl.lit('Over'))
                               .when((pl.col('PropType') == 'Anytime TD') & (pl.col('num_bets') % 2 != 1)).then(pl.lit('Under'))
                               .otherwise(pl.col('OverUnder')).alias('OverUnder')
                        ) \
                .select('officialDate', 'week', 'Away', 'Home', 'Player', 'PropType', 'OverUnder', 'Value', 'Price', 'Implied', 'ImpNoVig', 'BetTimeStamp') #\
                #.filter(~(pl.col('PropType').is_in(['1st TD Scorer', 'Last TD Scorer'])))

    # Begin Secondary Clean To Match ESPN
    prop_df = prop_df.with_columns([
        pl.col('PropType').replace_strict(prop_to_stat, default=pl.col('PropType')),
        pl.col('Player').replace_strict(prop_to_stat, default=pl.col('Player')),
        # `Value` was filled from `ImpNoVig` here, which put a probability in a
        # column of thresholds for any market with no posted line. It fired on no
        # row of the archived store -- Pinnacle posts the touchdown market as
        # "Over/Under 0.5" and every other market carries a real line -- so it was
        # a defect waiting for a market shape rather than a live one. A row with no
        # line now stays null, converts to null, and drops out of the pivot below
        # rather than shipping a probability as a threshold. See
        # docs/plans/35-market-lines-and-vig.md V4.
        pl.col('BetTimeStamp').max().over(['week', 'Player', 'Home', 'Away', 'officialDate'])
    ])\
    .select([
        pl.col('officialDate'),
        pl.col('week'),
        pl.col('Away'),
        pl.col('Home'),
        pl.col('Player'),
        pl.col('PropType'),
        pl.col('Value').cast(pl.Float64),
        pl.col('BetTimeStamp'),
        pl.col('OverUnder'),
        pl.col('Price').cast(pl.Float64),
        pl.col('Implied').cast(pl.Float64),
        pl.col('ImpNoVig').cast(pl.Float64),
    ])

    def adjust_value_polars(df, model=None):
        """Turn each posted line and price into an expectation.

        **What this replaces.** ``AdjValue = Value + Juice_Diff * Value * 0.25``,
        where ``Juice_Diff`` is a difference of decimal-odds-minus-one and ``Value``
        is the level of the line. ``Scripts/scrape_BOL.py`` had the same expression
        with 0.5, and ``docs/STATE_OF_THE_REPO.md`` records that this one changed
        from 0.5 to 0.25 mid-2025 with no explanation. Neither coefficient had
        evidence, and scaling by the level assumes a constant coefficient of
        variation, which is measurably false. See :mod:`Scripts.market`.

        **The touchdown market needed more than a new coefficient.** Pinnacle posts
        it as ``Over/Under 0.5 Touchdowns``, so the old expression evaluated
        ``0.5 + Juice_Diff * 0.125`` -- a number that lands near the de-vigged
        ``P(at least one touchdown)`` by numerical accident in the middle of the
        range, and goes **negative** at the longshot end. Measured on the archived
        2025 store: the combined touchdown column ran -0.698 to 1.124 with a mean of
        0.417, and 14 of 421 player-weeks projected a negative number of
        touchdowns. A count cannot be negative, and 0.417 is a probability where the
        rows in question realised 0.637 touchdowns a week.

        So that market is converted properly. Its 0.5 line makes it a statement of
        ``P(at least one) = q``, and
        :meth:`Scripts.market.MarketModel.mean_from_line` inverts that into a mean
        under the fitted count family -- which needs no special case, because every
        count market here is the same shape at a different threshold. Yardage gets
        ``line + Phi^-1(q) * sigma(line)``.

        **Measured on 2025.** Recovering the price behind each stored projection and
        re-converting it takes Pinnacle's running-back touchdown calibration from
        **0.679 to 0.996** on rushing and **0.591 to 0.894** on receiving, over 389
        player-weeks. See :func:`Scripts.lab.market.backtest`.

        Args:
            df: Long prop frame with ``Value``, ``Implied`` and ``ImpNoVig``.
            model: Loaded :class:`Scripts.market.MarketModel`. None loads it.

        Returns:
            pl.DataFrame: One row per player-stat with ``AdjValue``.
        """
        model = mk.load_model() if model is None else model
        pivoted_df = df.pivot(
            index=['officialDate', 'week', 'Away', 'Home', 'Player', 'PropType', 'Value', 'BetTimeStamp'],
            on='OverUnder',
            values=['Price', 'Implied', 'ImpNoVig'],
            aggregate_function='first'
        ).drop_nulls(['Implied_Over', 'Implied_Under'])
        if pivoted_df.height == 0:
            return pivoted_df.with_columns(pl.lit(None, dtype=pl.Float64)
                                             .alias('AdjValue'))

        # Pinnacle's own no-vig column is the proportional rule and was already
        # right; recomputed here so both books de-vig through one function.
        q_over, _ = mk.devig_two_way(pivoted_df['Implied_Over'].to_numpy(),
                                     pivoted_df['Implied_Under'].to_numpy())

        # Position from the markets the book posted for the player, because the
        # feed carries none and the touchdown multiplier is indexed by one.
        posted = (df.group_by('Player')
                    .agg(pl.col('PropType').unique().alias('markets')))
        by_player = {row['Player']: mk.position_from_markets(row['markets'])
                     for row in posted.iter_rows(named=True)}
        positions = np.array([by_player.get(name, 'REC')
                              for name in pivoted_df['Player'].to_list()],
                             dtype=object)

        lines = pivoted_df['Value'].to_numpy()
        stats_posted = np.asarray(pivoted_df['PropType'].to_list(), dtype=object)
        adjusted = lines.copy()
        for stat in set(stats_posted.tolist()):
            rows = stats_posted == stat
            # `mean_from_line` picks the conversion from the stat's own shape -- a
            # normal shift for yardage, an inversion of P(N >= k) = q for a count --
            # and resolves the column name to the market it came from, because this
            # frame calls the touchdown market `rushingTouchdowns` while the fit is
            # keyed `anytimeTouchdown`. So the 0.5 line needs no special case: it
            # *is* the count conversion at k = 1.
            adjusted[rows] = (
                model.mean_from_line(str(stat), lines[rows], q_over[rows],
                                     positions[rows])
                if model is not None else lines[rows])

        return pivoted_df.with_columns(pl.Series('AdjValue', adjusted))

    # Get Adjusted Value
    pivoted_df = adjust_value_polars(prop_df)

    clean = pivoted_df.select([
        pl.col('week'),
        pl.col('officialDate'),
        pl.col('Away'),
        pl.col('Home'),
        pl.col('Player').alias('player_name'),
        pl.col('PropType').alias('statType'),
        pl.col('AdjValue').alias('statValue'),
        pl.col('BetTimeStamp')
    ])

    # Pivot again
    clean = clean.pivot(
        index=['week', 'officialDate', 'Away', 'Home', 'player_name', 'BetTimeStamp'],
        on='statType',
        values='statValue',
        aggregate_function='mean'
    )

    if 'rushingTouchdowns' in clean.columns:
        clean = clean.with_columns([
            (pl.col('rushingTouchdowns') * 
             (pl.col('receivingYards') / (pl.col('receivingYards') + pl.col('rushingYards')))
            ).alias('receivingTouchdowns')
        ]).with_columns([
            (pl.col('rushingTouchdowns') - pl.col('receivingTouchdowns')).alias('rushingTouchdowns')
        ])

    stat_columns = [col for col in clean.columns if col not in ['week', 'officialDate', 'Away', 'Home', 'player_name', 'BetTimeStamp']]
    rename_mapping = {col: f'proj_{col}' for col in stat_columns}
    
    clean = clean.rename(rename_mapping)
    
    prop_path = landing_dir("Pinnacle", season, "Pinnacle_Props_Week_New.csv")
    clean.write_csv(prop_path)
    clean.write_parquet(landing_dir("Pinnacle", season, "Pinnacle_Props_Week_New.parquet"))
    print(clean.head())
    
    return clean

def reconcile_props(prop_df: pl.DataFrame, season: int = None):
    """Merge a fresh Pinnacle scrape into the season's accumulated prop file.

    Args:
        prop_df: Newly scraped props.
        season: Season to reconcile into. Defaults to the schedule's season.

    Returns:
        None. Writes the combined file plus one parquet per week.
    """
    season = current_season() if season is None else season

    # Load Previous
    all_path = season_dir("Pinnacle", season, "Pinnacle_Props_Week_All.parquet")
    all_df = (
        pl.read_parquet(all_path) if all_path.exists()
        else prop_df.clear()  # first scrape of a new season: start empty
    )

    old_df_rows = all_df.height
    old_df_games = all_df['officialDate', 'week', 'Away', 'Home'].n_unique()

    # Clean for Join
    prop_df = prop_df\
        .with_columns(
            pl.col("^proj_.*$").cast(pl.Float64),
            pl.col('player_name').replace_strict(prop_to_stat, default=pl.col('player_name'))
        )
    
    all_df = all_df\
         .with_columns(
            pl.col("^proj_.*$").cast(pl.Float64),
            pl.col('player_name').replace_strict(prop_to_stat, default=pl.col('player_name'))
        )

    # Perform Join
    all_df_cols = set(all_df.columns) - {'BetTimeStamp'}
    prop_df_cols = set(prop_df.columns)

    for col in all_df_cols:
        if col not in prop_df_cols:
            prop_df = prop_df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
            print(f"{col} In All DF - Not in Prop DF")

    all_df_cols = set(all_df.columns) - {'BetTimeStamp'}
    prop_df_cols = set(prop_df.columns)
    join_cols = list(all_df_cols.intersection(prop_df_cols))
    full_df = all_df.join(prop_df, on=join_cols, how='full', suffix='_new')
    

    coalesce_cols = [
        pl.coalesce([pl.col(col), pl.col(f"{col}_new")]).alias(col)
        for col in join_cols + ["BetTimeStamp"]
    ]
    final_df = full_df.select(coalesce_cols)

    df_filtered = (
        final_df.sort("BetTimeStamp", descending=True)
          .group_by(['officialDate', 'week', 'Away', 'Home', 'player_name'])
          .agg(pl.all().first())
        )
    
    # Sort + Index
    df_filtered = df_filtered.sort(by=['week', 'officialDate', 'Away', 'Home', 'player_name'])

    # Metrics
    new_df_rows = df_filtered.height
    new_df_games = df_filtered['officialDate', 'week', 'Away', 'Home'].n_unique()

    add_rows = new_df_rows - old_df_rows
    add_games = new_df_games - old_df_games

    # Save All
    df_filtered.write_parquet(all_path)
    print(f"All Pinnacle Player Prop File Contains {df_filtered.height} Rows")
    print(f"{add_rows} Rows Added to Pinnacle Player Prop File ({add_games} New Games)")
    print("")

    # Save - Split Into Weeks:
    weeks_list = df_filtered['week'].unique().to_list()
    for w in weeks_list:
        week_df = df_filtered.filter(pl.col('week') == w)
        n_games = week_df['officialDate', 'week', 'Away', 'Home'].n_unique()
        week_path = season_dir("Pinnacle", season, f"Pinnacle_Props_Week_{w}.parquet")
        week_df.write_parquet(week_path)
        print(f"WEEK {w} Pinnacle Player Prop File Contains {week_df.height} Rows ({n_games} Games)")

def clean_base(df, season=None):
    season = current_season() if season is None else season
    # Build
    clean_df = df.filter(pl.col('Period') != 'PlayerProp')\
                       .with_columns(pl.when(~pl.col('title').str.contains(' –')).then(pl.col('title'))
                                     .otherwise(pl.col('title').str.split(' –').map_elements(lambda x: x[0], return_dtype=pl.Utf8)).alias('BetType'),
                                     pl.lit(1).alias("start")
                                    ) \
                        .select(
                                pl.all().exclude("start"),
                                pl.col("start").cum_sum().over(['officialDate', 'week', 'Home', 'Away', 'title']).flatten().alias("num_bets")
                                ) \
                        .with_columns(
                             pl.when((pl.col('BetType') == 'Team Total') & (pl.col('num_bets') % 2 == 1)).then(pl.lit('TeamTotal_Away'))
                               .when((pl.col('BetType') == 'Team Total') & (pl.col('num_bets') % 2 != 1)).then(pl.lit('TeamTotal_Home'))
                               .otherwise(pl.col('BetType')).alias('BetType')
                        ) \
                        .with_columns(
                            pl.when(pl.col('BetType').is_in(['Handicap']) & (pl.col('num_bets') % 2 == 1)).then(pl.col('Away'))
                              .when(pl.col('BetType').is_in(['Handicap']) & (pl.col('num_bets') % 2 != 1)).then(pl.col('Home'))
                              .when(pl.col('BetType').is_in(['Money Line', 'Team To Score 1st Run'])).then(pl.col('label'))
                              .when(pl.col('BetType').is_in(['Total', 'TeamTotal_Away', 'TeamTotal_Home']) & (pl.col('num_bets') % 2 == 1)).then(pl.lit('Over'))
                              .when(pl.col('BetType').is_in(['Total', 'TeamTotal_Away', 'TeamTotal_Home']) & (pl.col('num_bets') % 2 != 1)).then(pl.lit('Under'))
                              .otherwise(pl.lit('Other')).alias('BetSide'),
                        ) \
                        .with_columns(
                            pl.when(pl.col('BetSide').is_in(['Over', 'Under'])).then(pl.col('label').str.extract(r'(\d+\.\d+|\d+)').cast(pl.Float32))
                                        .when(pl.col('BetType').is_in(['Handicap'])).then(pl.col('label').str.extract(r'(-?\d+\.\d+|-?\d+)').cast(pl.Float32))
                                        .otherwise(pl.lit(0)).alias('BetValue'),
                            pl.col('Price').cast(pl.Float32)
                        ) \
                        .with_columns((1 / pl.col('Price')).alias('BetImpProb'),
                                      pl.col('BetValue').abs().alias('abs_bet_value')
                                      ) \
                        .with_columns(
                            (pl.col("BetImpProb").max().over(['officialDate', 'week', 'Home', 'Away', 'Period', 'BetType', 'abs_bet_value'])-
                            pl.col("BetImpProb").min().over(['officialDate', 'week', 'Home', 'Away', 'Period', 'BetType', 'abs_bet_value'])
                            ).alias("imp_prob_diff")) \
                        .with_columns(pl.when(
                            (pl.col('imp_prob_diff') == pl.col('imp_prob_diff').min().over(['officialDate', 'week', 'Home', 'Away', 'Period', 'BetType'])) &
                            (pl.col('num_bets').max().over(['officialDate', 'week', 'Home', 'Away', 'Period', 'BetType']) > 3)).then(pl.lit(1))
                            .when(pl.col('num_bets').max().over(['officialDate', 'week', 'Home', 'Away', 'Period', 'BetType']) <= 3).then(pl.lit(1))
                            .otherwise(pl.lit(0)).alias("IsPrimary")
                         ) \
                        .select('officialDate', 'week', 'Home', 'Away', 'Period', 'BetType', 'BetSide', 'BetValue', 'Price', 'IsPrimary', 'BetImpProb', 'BetTimeStamp')

    print(clean_df.head())
    clean_path = landing_dir("Pinnacle", season, "Pinnacle_Base_New.csv")
    clean_df.write_csv(clean_path)

    return clean_df

MATCHUPS_URL = 'https://www.pinnacle.com/en/football/nfl/matchups/#period:0'


def scrape_weekly_props(season: int = None, write: bool = True) -> pl.DataFrame:
    """Drive Pinnacle's matchups page and reconcile this week's player props.

    All of this used to run at module scope, so importing this module launched Chrome
    and scraped -- which is why ``tests/test_market.py`` had to read this file as text
    to reach ``prop_to_stat``. See ``docs/plans/36-sportsbook-scrapes.md``.

    The driver's lifetime is a ``try/finally`` here rather than a ``driver.quit()``
    buried inside the parsing helper. A failure between launching the browser and
    reaching that line used to leave a Chrome process alive.

    Args:
        season: Season to scrape. None derives it from the schedule file.
        write: Unused today -- every helper below writes as it goes. Present so the
            signature matches the other scrapers, and so the flag exists once the
            guest-API path replaces the Selenium one.

    Returns:
        pl.DataFrame: The cleaned player props, empty if the page yielded no games.
    """
    season = current_season() if season is None else season
    start_time = time.time()

    driver = webdriver.Chrome(options=chrome_options())
    try:
        driver.get(MATCHUPS_URL)
        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located(
                (By.CSS_SELECTOR, 'div[class*="matchupMetadata"]')))
        print("Link Element Located")

        links = get_links_soup(driver, start_time)
        raw_pinny = get_raw_pinny_soup(driver, links_list=links, season=season)
    finally:
        driver.quit()

    base_clean = clean_raw_pinny(df=raw_pinny)
    props_df = clean_props(df=base_clean, season=season)
    clean_base(df=base_clean, season=season)

    reconcile_props(prop_df=props_df, season=season)

    elap_time = round((time.time() - start_time) / 60, 2)
    print(f"{base_clean.height} Rows in Base Bets Table")
    print(f"{props_df.height} Rows in Player Props Table")
    print(f"Scraper Elapsed Time: {elap_time} Minutes")
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return props_df


def main(argv=None) -> int:
    """Command-line entry point.

    Usage::

        python -m Scripts.scrape_pinnacle
        python -m Scripts.scrape_pinnacle --season 2026
    """
    import argparse

    p = argparse.ArgumentParser(
        prog="python -m Scripts.scrape_pinnacle",
        description="Scrape Pinnacle weekly NFL player props.",
    )
    p.add_argument("--season", type=int, help="defaults to the schedule's season")
    p.add_argument("--dry-run", action="store_true", help="do not write files")
    args = p.parse_args(argv)

    df = scrape_weekly_props(season=args.season, write=not args.dry_run)
    # An empty scrape is a failure, matching scrape_pinnacle_season.
    return 0 if not df.is_empty() else 1


if __name__ == "__main__":
    raise SystemExit(main())
