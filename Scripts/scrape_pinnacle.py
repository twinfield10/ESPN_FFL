import time
from datetime import datetime
import requests
import polars as pl
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from nfl_utils import DATE_WEEK

chrome_options  = webdriver.ChromeOptions()
chrome_options .add_argument('--ignore-certificate-errors')
chrome_options .add_argument('--ignore-ssl-errors')

def get_links_soup():
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
    return links_list

def get_links():
    # Loop
    links_list = []
    matchups = driver.find_elements(By.CSS_SELECTOR, 'a:has(div[class*="matchupMetadata"])')
    for e in matchups:
        link = e.get_attribute('href')
        if link is not None and "games" not in link:
            for f in e.find_elements(By.CSS_SELECTOR, 'div[class*="matchupDate"] span'):
                live_text = f.text if f.text else None
                if live_text != 'Live Now':
                    links_list.append(link)
    return links_list

def get_raw_pinny(links_list):
    type_list = ["#all"]
    df_list = []

    for i in type_list:
        all_links = [link + i for link in links_list]
        for url in all_links:

            try:
                # Load URL
                driver.get(url)
                print(url)

                # Get Game Date
                try:
                    full_date = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="startTime"] span'))
                    ).text
                    date = datetime.strptime(full_date, "%A, %B %d, %Y at %H:%M").strftime("%Y-%m-%d")
                except TimeoutException:
                    print(f"Failed to retrieve date for {url}")
                    continue

                # Show All Bets
                try:
                    show_all_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[class*="showAllButton"]'))
                    )
                    if show_all_button.text == "Show All":
                        show_all_button.click()
                        time.sleep(1)
                except TimeoutException:
                    print(f"Show All button not found for {url}")

                # Expand More Markets
                for btns in driver.find_elements(By.CSS_SELECTOR, 'button[class*="toggleMarkets"]'):
                    if btns.text == 'See more':
                        btns.click()
                        time.sleep(1)

                # Market Elements
                market_elements_list = driver.find_elements(By.CSS_SELECTOR, 'div[class*="primary"]')
                for melem in market_elements_list:
                    # Get Bet Title
                    bet_titles = [m.text for m in melem.find_elements(By.CSS_SELECTOR, 'span[class*="titleText"]') if m.text]
                    # Get Bet Labels (teams, over/under, etc.)
                    lab_list = [lab.text for lab in melem.find_elements(By.CSS_SELECTOR, 'span[class*="label"]') if lab.text]
                    # Get Bet Prices (as Decimal Format)
                    price_list = [price.text for price in melem.find_elements(By.CSS_SELECTOR, 'span[class*="price"]') if price.text]
                    if bet_titles and lab_list and price_list:

                        # Create Data Frame    
                        bet_data = {'title': [bet_titles[0]] * len(price_list),
                                    'label': lab_list,
                                    'Price': price_list}

                        bet_df = pl.DataFrame(bet_data).with_columns(
                            pl.lit(date).alias('officialDate'),
                            pl.lit(url).alias('url'),
                            pl.lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).alias('BetTimeStamp'),
                        )

                        # Append to Large List
                        df_list.append(bet_df)

            except Exception as e:
                print(f"An error occurred with {url}: {e}")

    # Quit Driver
    driver.quit()

    # Create DataFrame
    df = pl.concat(df_list)
    df = df.join(DATE_WEEK, left_on='officialDate', right_on='gameday', how = 'left')
    df = df.with_columns(pl.col("title").str.replace("Josh Allen \\(BUF\\)", "Josh Allen").alias("title"))

    df.write_csv('Data/Projections/Pinnacle/Raw_Pinnacle.csv')

    return df

def get_raw_pinny_soup(links_list):
    type_list = ["#all"]
    df_list = []

    for i in type_list:
        all_links = [link + i for link in links_list]
        for url in all_links:

            try:
                # Load URL and interact with dynamic elements using Selenium
                driver.get(url)
                print(url)

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
                                    'label': lab_list,
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
    driver.quit()

    # Small Clean and Join To NFL Schedule
    df = pl.concat(df_list)
    df = df.join(DATE_WEEK, left_on='gameday', right_on='gameday', how='left').drop('gameday')
    df = df.with_columns(pl.col("title").str.replace("Josh Allen \\(BUF\\)", "Josh Allen").alias("title"))

    df.write_csv('Data/Projections/Pinnacle/Raw_Pinnacle_New.csv')

    return df

def clean_raw_pinny(df):

    # Get Rid of Unnecessary Bets
    filt_bets = ['Correct Score', 'Exact', 'Winning Margin', 'Winner/Total', 'Range', 'Odd/Even', 'Alternate Lines']

    # Base Clean All Data
    final_df = df.filter(~pl.col('title').str.contains_any(filt_bets)) \
            .with_columns(pl.when(pl.col('title').str.contains_any(['Game', 'Alternate Lines'])).then(pl.lit('Game'))
                            .when(pl.col('title').str.contains('1st Half')).then(pl.lit('1H'))
                            .when(pl.col('title').str.contains('1st Quarter')).then(pl.lit('1Q'))
                            .when(pl.col('title').str.contains_any(['TD Scorer', 'Anytime', 'Interceptions', 'Reception', 'Yards', 'Receptions', 'Kicking', 'Completion', 'Attempts', 'Passes'])).then(pl.lit('PlayerProp'))
                            .otherwise(pl.lit('Other')).alias('Period')) \
            .with_columns(pl.col("url").str.extract(r'nfl\/(.*?)\/\d+\/#all').alias('AllTeams')) \
            .with_columns(pl.col("AllTeams").str.split('-vs-').alias('split_teams')) \
            .with_columns(pl.col('split_teams').list.first().str.strip_chars().str.replace_all("-", " ").str.to_titlecase().alias('Away')) \
            .with_columns(pl.col('split_teams').list.last().str.strip_chars().str.replace_all("-", " ").str.to_titlecase().alias('Home')) \
            .drop('split_teams', 'AllTeams', 'url')

    print(final_df.head())
    return final_df

def clean_props(df):
    ## Build + Save Prop DF
    prop_df = df.filter((pl.col('Period') == 'PlayerProp')) \
                .with_columns(pl.col('title').str.replace('(BUF)', '').alias('Title')) \
                .with_columns(pl.col('Title').str.extract(r'\((.*?)\)').alias('PropType')) \
                .with_columns(pl.col('Title').str.replace(r'\(.*?\)', '').alias('Title')) \
                .with_columns(pl.col('Title').str.replace(r'\(\)', '').str.strip_chars().alias('Player')) \
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
                .select('officialDate', 'week', 'Away', 'Home', 'Player', 'PropType', 'OverUnder', 'Value', 'Price', 'Implied', 'ImpNoVig', 'BetTimeStamp') \
                .filter(~pl.col('PropType').is_in(['1st TD Scorer', 'Last TD Scorer']))
    
    #prop_path = 'Data/Projections/Pinnacle/Pinnacle_Props_New.csv'
    #prop_df.write_csv(prop_path)
    #print(prop_df.head())
    
    return prop_df

def reconcile_props(prop_df: pl.DataFrame, base_path = "Data/Projections/Pinnacle/Props/Pinnacle_Props_Week_"):
    
    # Load Previous
    all_path = f"{base_path}All.parquet"
    all_df = pl.read_parquet(all_path)

    # Clean for Join
    prop_df = prop_df\
        .with_columns([
            pl.col('Value').cast(pl.Float64),
            pl.col('Price').cast(pl.Float64),
            pl.col('Implied').cast(pl.Float64),
            pl.col('ImpNoVig').cast(pl.Float64)
        ])

    # Perform Join
    join_cols = [col for col in all_df.columns if col not in 'BetTimeStamp']
    full_df = all_df.join(prop_df, on=join_cols, how='full', suffix='_new')

    coalesce_cols = [
        pl.coalesce([pl.col(col), pl.col(f"{col}_new")]).alias(col)
        for col in join_cols + ["BetTimeStamp"]
    ]
    final_df = full_df.select(coalesce_cols)

    df_filtered = (
        final_df.sort("BetTimeStamp", descending=True)
          .group_by(['officialDate', 'week', 'Away', 'Home', 'Player', 'PropType', 'OverUnder'])
          .agg(pl.all().first())
        )
    
    # Sort + Index
    df_filtered = df_filtered.sort(by=['week', 'officialDate', 'Away', 'Player', 'PropType', 'OverUnder'])

    # Save All
    df_filtered.write_parquet(all_path)
    print(f"All Pinnacle Player Prop File Updated with {df_filtered.height} Rows")

    # Save - Split Into Weeks:
    weeks_list = df_filtered['week'].unique().to_list()
    for w in weeks_list:
        week_df = df_filtered.filter(pl.col('week') == w)
        week_df.write_parquet(f"{base_path}{w}.parquet")
        print(f"Pinnacle Week {w} Player Prop File Updated with {week_df.height} Rows")

def clean_base(df):
    # Build + Save 
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
    clean_path = 'Data/Projections/Pinnacle/Pinnacle_Base_New.csv'
    clean_df.write_csv(clean_path)

    return clean_df

# Driver
start_time = time.time()
driver = webdriver.Chrome(options=chrome_options)

driver.get('https://www.pinnacle.com/en/football/nfl/matchups/#period:0')
WebDriverWait(driver, 2).until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div[class*="matchupMetadata"]')))
print("Link Element Located")

# Execute
links = get_links_soup()
raw_pinny = get_raw_pinny_soup(links_list=links)

base_clean = clean_raw_pinny(df = raw_pinny)
props_df = clean_props(df = base_clean)
base_df = clean_base(df = base_clean)

reconcile_props(prop_df = props_df)

end_time = time.time()
elap_time = round((end_time - start_time)/60, 2)

print(f"{base_clean.height} Rows in Base Bets Table")
print(f"{props_df.height} Rows in Player Props Table")

print(f"Scraper Elapsed Time: {elap_time} Minutes")
print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))