import time
from datetime import datetime
import numpy as np
import polars as pl
from pathlib import Path

from Scripts import market as mk
from Scripts.bol_widget import (
    STATS as WIDGET_STATS,
    BetOnlineWidgetError,
    DSTWidget,
    make_fetcher,
)
from Scripts.books.props import props_to_odds_rows
from Scripts.books.store import write_snapshot
from Scripts.nfl_utils import current_season, current_week, load_schedule
from Scripts.paths import landing_dir, season_dir


## Constants

def slim_schedule() -> pl.DataFrame:
    """The schedule columns this scraper joins on, one row per game.

    Built on demand rather than at import. It used to be a module constant reading
    ``NFL_SCHEDULE``, which resolves the schedule CSV eagerly through
    ``nfl_utils.__getattr__`` -- so merely importing this module read a file, and
    everything below it then ran a live scrape and overwrote the archive.

    Returns:
        pl.DataFrame: ``NFL_game_id``, ``week``, ``officialDate``, ``Away``, ``Home``.
    """
    return (
        pl.DataFrame(load_schedule())
        .select([
            pl.col('game_id').alias('NFL_game_id')
            ,pl.col('week').cast(pl.Int64)
            ,pl.col('gameday').str.strptime(pl.Date, format="%Y-%m-%d").alias('officialDate')
            ,pl.col('away_team').alias('Away')
            ,pl.col('home_team').alias('Home')
        ])
    )

class BetOnlineAccessError(RuntimeError):
    """Raised when BetOnline's markets can not be reached.

    Kept as its own type, and still raised, because the failure this module has
    actually suffered is a silent one: an empty scrape that looks like "the book
    posted nothing this week" when in fact the transport is broken.
    """


# Statistic Mapping
#
# Derived from the widget's own market table rather than restated here, so the
# scraper and the transport cannot drift apart. ``defensiveInterceptions`` used
# to be in this map and is gone: DST posts no such market. See Scripts/bol_widget.py.
stats = {spec.espn_stat: spec.statistic for spec in WIDGET_STATS}

## Functions

# Create BOL Keys
def get_week_ids(cache: dict, week_num: int) -> dict:
    """Return ``{week: [BOL game ids]}`` from a harvested widget cache.

    This used to guess. BetOnline game ids are consecutive integers, there was no
    listing endpoint reachable from a plain client, and so the scraper probed
    outward from a hand-maintained ``LAST_KNOWN_ID`` constant that had to be reset
    every season. The widget's own game list makes all of that unnecessary: the
    ids are simply the ones it fetched markets for.
    """
    return {week_num: sorted({game_id for _, game_id, _ in cache})}


def build_BOL_dim(ids:list, sched_df: pl.DataFrame, fetch):
    raw = pl.DataFrame()
    for i in ids:
        data = fetch('marketsBySs', i, 'Touchdowns')
        if data:
            players_data = data[0]['players']

            # Build DF
            rows = []
            for player in players_data:
                for market in player['markets']:
                    rows.append({
                        'team': player['team'],
                        'BOL_game_id': market['game1Id']
                    })
            raw = raw.vstack(pl.DataFrame(rows))
        else:
            print(f"No BetOnline Touchdowns markets for game id {i}")

        dim = raw.unique()

    # Join To NFL Schedule:
    dim = (
        dim
        .join(sched_df, left_on=['team'], right_on=['Away'], how='left')
        .filter(~pl.col('NFL_game_id').is_null())
        .select([
             pl.col('NFL_game_id')
            ,pl.col('team').alias('Away')
            ,pl.col('Home')
            ,pl.col('officialDate')
            ,pl.col('week')
            ,pl.col('BOL_game_id')
        ])
        .unique()
    )

    return dim

# GET BOL json
def get_BOL_data(ids: list, link_stat: str, espn_stat: str, week: int,
                 fetch) -> pl.DataFrame:
    # Initialize Polars DF
    raw = pl.DataFrame()
    missing = []
    for i in ids:
        # Build URL + Game Label
        data = fetch('marketsBySs', i, link_stat)
        if data:

            # Check Data Loaded
            try:
                players_data = data[0]['players']

                # Build DF
                rows = []
                for player in players_data:
                    for market in player['markets']:
                        rows.append({
                            'BOL_game_id': market['game1Id'],
                            'week': week,
                            'player_name': player['name'],
                            'player_id': player['id'],
                            'team': player['team'],
                            'position': player['position']['title'],
                            'market_id': market['id'],
                            'condition': market['condition'],
                            'is_active': market['isActive'],
                            'is_actual': market['isActual'],
                            'type': str(market['type']),
                            'odds': market['odds'],
                            'value': market['value'],
                            'statistic': market['statistic']['title'],
                            'espn_stat': espn_stat
                        })
                raw = raw.vstack(pl.DataFrame(rows).with_columns([(1/pl.col('odds')).alias('impProb'), pl.col('value').cast(pl.Float64)]))
            except:
                print(f"Data Retreived with Error for BOL Game ID: {i} | Stat: {link_stat}")
        else:
            missing.append(i)

    if missing:
        print(f"  {link_stat}: no market in {len(missing)}/{len(ids)} games")

    team_map = {
        'LVR': 'LV',
        'NOS': 'NO',
        'LAR': 'LA' 
    }

    if raw.height > 0:
        raw = raw.with_columns(
                pl.col("team").str.replace_many(
                    list(team_map.keys()),
                    list(team_map.values())
                )
                .alias('team'),
                pl.lit('Values').alias('prop_source')
            )

        return raw
    else:
        return None
def get_BOL_data_OU(ids: list, link_stat: str, espn_stat: str, week: int,
                    fetch) -> pl.DataFrame:
    # Initialize Polars DF
    raw = pl.DataFrame()
    missing = []
    for i in ids:
        # Build URL + Game Label
        data = fetch('marketsByOu', i, link_stat)
        if data:

            # Check Data Loaded
            try:
                players_data = data[0]['players']

                # Build DF
                rows = []
                for player in players_data:
                    for market in player['markets']:
                        rows.append({
                            'BOL_game_id': market['game1Id'],
                            'week': week,
                            'player_name': player['name'],
                            'player_id': player['id'],
                            'team': player['team'],
                            'position': player['position']['title'],
                            'market_id': market['id'],
                            'condition': market['condition'],
                            'is_active': market['isActive'],
                            'is_actual': market['isActual'],
                            'type': 'Over' if market['type'] == 18 else 'Under' if market['type'] == 19 else str(market['type']),
                            'odds': market['odds'],
                            'value': market['value'],
                            'statistic': market['statistic']['title'],
                            'espn_stat': espn_stat
                        })
                raw = raw.vstack(
                    pl.DataFrame(rows)\
                        .with_columns([
                            (1/pl.col('odds')).alias('impProb')
                            ])
                    )
            except:
                print(f"Data Retreived with Error for BOL Game ID: {i} | Stat: {link_stat}")
        else:
            missing.append(i)

    if missing:
        print(f"  {link_stat}: no market in {len(missing)}/{len(ids)} games")

    team_map = {
        'LVR': 'LV',
        'NOS': 'NO',
        'LAR': 'LA' 
    }

    if raw.height > 0:
        raw = raw.with_columns(
                pl.col("team").str.replace_many(
                    list(team_map.keys()),
                    list(team_map.values())
                )
                .alias('team'),
                pl.lit('OverUnder').alias('prop_source')
            )

        return raw
    else:
        return None

def archive_raw(prop_df: pl.DataFrame, season: int, week_num: int) -> None:
    """Keep this week's raw prices, because the landing file is overwritten.

    **The reason plan 35 could not be scored.** ``BetOnline_AllProps_Raw.parquet``
    is rewritten on every run, so by the time anyone asked whether the de-vig and
    the line conversion were right, the only prices left in the repo were the last
    scrape of 2025 -- one game. Every earlier week survives only as the *derived*
    ``proj_`` column, which cannot be re-derived under a new formula, so
    ``Scripts/lab/accuracy.py``'s 2025 calibration measures arithmetic that has
    since been replaced and will keep measuring it.

    One parquet per week fixes that going forward: prices and outcomes both
    archived means every conversion in :mod:`Scripts.market` becomes answerable
    against realised stat lines rather than against its own derivation.

    Args:
        prop_df: The raw scrape.
        season: Season being scraped.
        week_num: Week being scraped.

    Returns:
        None. Writes ``Raw/<season>/BetOnline_Raw_Week_<week>.parquet``.
    """
    path = season_dir("BetOnline", season, "Raw",
                      f"BetOnline_Raw_Week_{week_num}.parquet")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    prop_df.write_parquet(path)
    print(f"Archived {prop_df.height} raw BetOnline prices for week {week_num}")


# Reconcile Full File
def reconcile_BOL(prop_df: pl.DataFrame, season: int = None):
    """Merge a fresh scrape into the season's accumulated prop file.

    Args:
        prop_df: Newly scraped props.
        season: Season to reconcile into. Defaults to the schedule's season.

    Returns:
        None. Writes the combined file plus one parquet per week.
    """
    season = current_season() if season is None else season

    # Outline Pathway + Load
    all_path = season_dir("BetOnline", season, "BetOnline_AllProps.parquet")
    all_df = (
        pl.read_parquet(all_path) if all_path.exists()
        else prop_df.clear()  # first scrape of a new season: start empty
    )
    old_df_rows = all_df.height
    old_df_games = all_df['BOL_game_id'].n_unique()

    # Current Data
    prop_df = prop_df.with_columns(pl.col('week').cast(pl.Int32))
    join_cols = [col for col in all_df.columns if col not in 'BetTimeStamp']
    full_df = all_df.join(prop_df, on=join_cols, how='full', suffix='_new')
 
    coalesce_cols = [
         pl.coalesce([pl.col(col), pl.col(f"{col}_new")]).alias(col)
         for col in join_cols + ["BetTimeStamp"]
     ]
    
    final_df = full_df.select(coalesce_cols)
 
    df_filtered = (
         final_df.sort("BetTimeStamp", descending=True)
           .group_by(['week', 'player_name', 'position', 'team'])
           .agg(pl.all().first())
         )
    
    df_filtered = df_filtered.sort(by=['week', 'player_name', 'position', 'team'])

    # Metrics
    new_df_rows = df_filtered.height
    new_df_games = df_filtered['BOL_game_id'].n_unique()

    add_rows = new_df_rows - old_df_rows
    add_games = new_df_games - old_df_games

    # Save All
    df_filtered.write_parquet(all_path)
    print(f"All BetOnline Player Prop File Contains {new_df_rows} Rows")
    print(f"{add_rows} Rows Added to BetOnline Player Prop File ({add_games} New Games)")
    print("")

    # Save - Split Into Weeks:
    weeks_list = df_filtered['week'].unique().to_list()
    for w in weeks_list:
        # Get Week DataFrame
        week_df = df_filtered.filter(pl.col('week') == w)
        n_games = week_df['BOL_game_id'].n_unique()

        # Identify Week Path + Create Folder If Not Exists
        week_path = season_dir(
            "BetOnline", season, f"Week {w}", f"BetOnline_AllProps_Week_{w}.parquet"
        )
        Path(week_path).parent.mkdir(parents=True, exist_ok=True)

        # Save as Parquet
        week_df.write_parquet(week_path)
        print(f"WEEK {w} Bet Online Player Prop File Contains {week_df.height} Rows ({n_games} Games)")

# Get Stat by Name
def get_x_stat(stat: str = 'anytimeTouchdown', model=None, season: int = None) -> pl.DataFrame:
    """One stat's projection, and its market-implied dispersion, per player-week.

    **Every piece of arithmetic here lives in :mod:`Scripts.market`** -- the de-vig,
    the line-to-mean conversion, and the two ways to read a ladder. This function
    is the plumbing: pivot, join, name the columns. It used to hold an undocumented
    coefficient, a conditional that was always False, and an inner join that could
    drop a player; ``docs/plans/35-market-lines-and-vig.md`` records each one.

    The hierarchy for the projection, cheapest information first:

    1. A two-way pair, de-vigged, converted by
       :meth:`Scripts.market.MarketModel.mean_from_line` -- ``line + Phi^-1(q) *
       sigma(line)`` for yardage, an inversion of ``P(N >= k) = q`` for a count.
       This is every stat BetOnline posts an over/under for.
    2. A count ladder rooted at 1, read by the exact discrete identity. This is the
       anytime-touchdown, sack and interception markets, which have no two-way line.
    3. A yardage ladder's own de-vigged median, which is the quantity a line *is*.
       Never ``sum(threshold * P(bucket))``: measured against the line it posts
       beside, that ran 0.77 to 1.81.

    The dispersion is separate and comes from the ladder wherever there is one --
    exactly for a count rooted at 1, from the ladder's upper quantiles otherwise.

    Args:
        stat: ESPN stat name, a key of :data:`stats`.
        model: Loaded :class:`Scripts.market.MarketModel`. None loads it.

    Returns:
        pl.DataFrame: ``BOL_game_id``, ``week``, ``player_name``, ``position``,
        ``team``, ``proj_<stat>`` and ``proj_<stat>_sd``.
    """
    model = mk.load_model() if model is None else model
    season = current_season() if season is None else season
    df = pl.read_parquet(landing_dir("BetOnline", season, "BetOnline_AllProps_Raw.parquet"))\
           .filter(pl.col('espn_stat') == stat)\
           .drop('market_id', 'condition', 'is_active', 'is_actual')

    keys = ['BOL_game_id', 'week', 'player_name', 'position', 'team']
    df_ou = df.filter(pl.col('prop_source') == 'OverUnder')
    df_val = df.filter(pl.col('prop_source') == 'Values')

    # The book's own margin, measured on this scrape's two-way pairs and applied to
    # the one-sided ladders as well -- they carry the same 6.4%, measured. Falls
    # back to the archived constant when a stat has no two-way market at all.
    hold = mk.DEFAULT_OVERROUND
    if df_ou.height > 0:
        sides = df_ou.pivot(index=['BOL_game_id', 'player_name', 'value'],
                            on='type', values='impProb', aggregate_function='first')
        if {'Over', 'Under'}.issubset(sides.columns):
            hold = mk.measure_overround(sides['Over'].to_numpy(),
                                        sides['Under'].to_numpy())

    def ou_calc(frame: pl.DataFrame) -> pl.DataFrame:
        """Two-way pairs into an expectation: de-vig, then convert by stat shape."""
        wide = frame.pivot(
            index=keys + ['player_id', 'statistic', 'espn_stat', 'value'],
            on='type', values=['odds', 'impProb'],
        ).drop_nulls(['impProb_Over', 'impProb_Under'])
        if wide.height == 0:
            return wide.select(keys).with_columns(
                pl.lit(None, dtype=pl.Float64).alias(f'from_line_{stat}'))

        q_over, _ = mk.devig_two_way(wide['impProb_Over'].to_numpy(),
                                     wide['impProb_Under'].to_numpy())
        lines = wide['value'].to_numpy()
        converted = (model.mean_from_line(stat, lines, q_over,
                                          wide['position'].to_list())
                     if model is not None else lines)
        # One row per player-stat, in case the book posts alternate lines: the join
        # below is on the keys alone and a duplicate there multiplies every stat.
        return (wide
                .with_columns(pl.Series(f'from_line_{stat}', converted))
                .group_by(keys)
                .agg(pl.col(f'from_line_{stat}').mean()))

    def ladder_calc(frame: pl.DataFrame) -> pl.DataFrame:
        """A ladder into a projection and a dispersion, by its shape."""
        kind = mk.MARKET_STATS[stat].kind if stat in mk.MARKET_STATS else 'yardage'
        rows = []
        for group in frame.partition_by(keys, maintain_order=True):
            edges, survival = mk.monotone_survival(group['value'].to_numpy(),
                                                   group['impProb'].to_numpy())
            fair = mk.devig_survival(survival, hold)
            mean = (mk.count_moments(edges, fair)[0] if kind == 'count'
                    else mk.ladder_median(edges, fair))
            rows.append({
                **{key: group[key][0] for key in keys},
                f'from_ladder_{stat}': None if not np.isfinite(mean) else mean,
                f'proj_{stat}_sd': _finite(mk.market_scale(edges, fair, kind)),
            })
        if not rows:
            return frame.select(keys).with_columns(
                pl.lit(None, dtype=pl.Float64).alias(f'from_ladder_{stat}'),
                pl.lit(None, dtype=pl.Float64).alias(f'proj_{stat}_sd'))
        return pl.DataFrame(rows, schema_overrides={
            f'from_ladder_{stat}': pl.Float64, f'proj_{stat}_sd': pl.Float64})

    # A full join, not an inner one. The old inner join dropped any player with a
    # ladder and no two-way line for that stat -- measured at 0 rows on the archived
    # week, so a latent risk rather than a live bug, and the dispersion column makes
    # it a live one: a laddered player with no line still states a variance.
    if df_ou.height > 0 and df_val.height > 0:
        final_df = ou_calc(df_ou).join(ladder_calc(df_val), on=keys, how='full',
                                       coalesce=True)
    elif df_ou.height > 0:
        final_df = ou_calc(df_ou).with_columns(
            pl.lit(None, dtype=pl.Float64).alias(f'from_ladder_{stat}'),
            pl.lit(None, dtype=pl.Float64).alias(f'proj_{stat}_sd'))
    else:
        final_df = ladder_calc(df_val).with_columns(
            pl.lit(None, dtype=pl.Float64).alias(f'from_line_{stat}'))

    return (final_df
            .with_columns(pl.coalesce(pl.col(f'from_line_{stat}'),
                                      pl.col(f'from_ladder_{stat}'))
                            .alias(f'proj_{stat}'))
            .drop(f'from_line_{stat}', f'from_ladder_{stat}')
            .select(keys + [f'proj_{stat}', f'proj_{stat}_sd'])
            .sort(by=[f'proj_{stat}'], nulls_last=True))


def _finite(value):
    """A float, or None where it is not finite -- Polars skips null and propagates
    NaN, and every consumer of a dispersion column wants the former."""
    return None if value is None or not np.isfinite(value) else float(value)


# Clean Final Dataframe
def clean_bol(stats_list=None, current_sched: pl.DataFrame = None,
              season: int = None) -> pl.DataFrame:
    """Every stat's projection and dispersion, one row per player-week.

    Args:
        stats_list: ESPN stat names to build. None does all of :data:`stats`.
        current_sched: This week's rows from :func:`slim_schedule`, used for the
            team-to-game join. Was a module global set only inside the old
            import-time execute block, so calling this after a plain import raised
            ``NameError``.
        season: Season to read the landing file for. None derives it.

    Returns:
        pl.DataFrame: ``proj_<stat>`` and ``proj_<stat>_sd`` per stat, plus the
        schedule join.
    """
    stats_list = list(stats.keys()) if stats_list is None else stats_list
    keys = ['BOL_game_id', 'week', 'player_name', 'position', 'team']
    model = mk.load_model()
    season = current_season() if season is None else season

    final_result = None
    for stat in stats_list:
        result = get_x_stat(stat, model=model, season=season)
        if final_result is None:
            final_result = result
            continue
        final_result = final_result.join(result, on=keys, how='full',
                                        coalesce=True)

    # The anytime-touchdown market, allocated to a rushing or a receiving column.
    #
    # **This allocation is wrong and is not plan 35's to fix.** All of a back's
    # anytime market goes to rushing and all of a receiver's to receiving, which is
    # why `Scripts/lab/accuracy.py` reports BOL@RB receivingTouchdowns at a ratio of
    # 0.0 on 910 player-weeks. Reallocating it is docs/plans/34-stat-first-audit.md
    # F2's open item, measured there as worth 0.597 -> 0.891 on RB receiving
    # calibration. What changed here is only that the dispersion follows its own
    # mean instead of being dropped.
    rushing = pl.col('position').is_in(['QB', 'RB'])
    receiving = pl.col('position').is_in(['WR', 'TE'])
    final_result = final_result.with_columns([
        pl.when(rushing).then(pl.col('proj_anytimeTouchdown'))
          .otherwise(pl.lit(0.0)).alias('proj_rushingTouchdowns'),
        pl.when(receiving).then(pl.col('proj_anytimeTouchdown'))
          .otherwise(pl.lit(0.0)).alias('proj_receivingTouchdowns'),
        pl.when(rushing).then(pl.col('proj_anytimeTouchdown_sd'))
          .alias('proj_rushingTouchdowns_sd'),
        pl.when(receiving).then(pl.col('proj_anytimeTouchdown_sd'))
          .alias('proj_receivingTouchdowns_sd'),
    ])

    Long_Sched = current_sched.unpivot(
        index=["NFL_game_id", "week", "officialDate"],
        on=["Home", "Away"],
        variable_name="Location",
        value_name="team"
    )

    final_result = (
        final_result
        .with_columns(pl.lit(datetime.now()).alias('BetTimeStamp'))
        .drop('proj_anytimeTouchdown', 'proj_anytimeTouchdown_sd')
        .join(Long_Sched, on=['team', 'week'], how="left")
    )

    return final_result


#: Columns every raw price row carries, so an empty scrape still has a shape.
FULL_DF_SCHEMA = {
    "BOL_game_id": pl.Int64,
    "week": pl.Int64,
    "player_name": pl.Utf8,
    "player_id": pl.Int64,
    "team": pl.Utf8,
    "position": pl.Utf8,
    "market_id": pl.Int64,
    "condition": pl.Int64,
    "is_active": pl.Boolean,
    "is_actual": pl.Boolean,
    "type": pl.Utf8,
    "odds": pl.Float64,
    "value": pl.Float64,
    "statistic": pl.Utf8,
    "espn_stat": pl.Utf8,
    "impProb": pl.Float64,
    "prop_source": pl.Utf8
}


def _store_line_history(raw: pl.DataFrame, season: int, sched: pl.DataFrame) -> None:
    """Append these prices to the odds store, where their movement accumulates.

    The second half of a dual write. The blend reads the flat file written above;
    this lands the same prices as standard odds rows next to the game lines, and
    ``write_snapshot`` keeps only what moved -- so prop line history costs a
    conversion rather than a mechanism. See
    ``docs/plans/45-props-in-the-odds-store.md``.

    Never fatal. The projection copy is already on disk by the time this runs, and
    history is not worth losing a scrape over. It reports rather than raising,
    because the failure that matters here is a silent one.
    """
    try:
        rows = props_to_odds_rows(raw, season, sched)
        if rows.is_empty():
            print("  odds store: nothing mapped to a scheduled game, skipped")
            return
        counts = write_snapshot(rows, season, "BetOnline")
        print(f"  odds store: {counts['appended']} appended, "
              f"{counts['unchanged']} unchanged")
    except Exception as exc:
        print(f"  odds store: line history not written "
              f"({type(exc).__name__}: {exc})")


def scrape_week(season: int = None, week: int = None, write: bool = True) -> pl.DataFrame:
    """Pull one week of BetOnline player props and reconcile them into the season file.

    Everything below used to run at module scope, so importing this module performed a
    live scrape *and overwrote the archived parquet and CSV* -- strictly worse than
    merely spending a scrape, and the reason ``tests/test_market.py`` reads its sibling
    as text rather than importing it. See ``docs/plans/36-sportsbook-scrapes.md``.

    Args:
        season: Season to scrape. None derives it from the schedule file.
        week: Week to scrape. None uses the first week with unplayed games.
        write: False does the whole pull and returns the frame without touching disk.

    Returns:
        pl.DataFrame: The cleaned per-player projections, empty if the book returned
        nothing.

    Raises:
        BetOnlineAccessError: If the widget yielded no markets at all.
    """
    season = current_season() if season is None else season
    sched = slim_schedule()
    week = current_week() if week is None else week
    print(f"Now Loading NFL Week {week}:")

    # One browser session for the whole week. Every /api/dfm/* route is signed by
    # the widget's own client -- see Scripts/bol_widget.py for why nothing simpler
    # works -- so the markets are harvested up front and then read out of a cache.
    try:
        with DSTWidget() as widget:
            cache = widget.harvest()
    except BetOnlineWidgetError as exc:
        raise BetOnlineAccessError(str(exc)) from exc
    if not cache:
        raise BetOnlineAccessError(
            "The BetOnline props widget returned no markets at all. That is a "
            "transport failure, not an empty board -- see Scripts/bol_widget.py."
        )
    fetch = make_fetcher(cache)

    BOL_IDs = get_week_ids(cache=cache, week_num=week)
    print(f"  {len(BOL_IDs[week])} games, {len(cache)} market responses")
    current_sched = sched.filter(pl.col('week') == week)

    full_df = pl.DataFrame(schema=FULL_DF_SCHEMA)

    for espn, bol in stats.items():
        df = get_BOL_data(ids=BOL_IDs[week], link_stat=bol, espn_stat=espn,
                          week=week, fetch=fetch)
        if df is not None:
            full_df = full_df.vstack(df)

    # Touchdowns and Sacks are ladder-only markets: the widget lists no two-way
    # tile for them, so asking would only add empty round-trips.
    for espn, bol in stats.items():
        if bol not in ['Sacks', 'Interceptions', 'Touchdowns']:
            df = get_BOL_data_OU(ids=BOL_IDs[week], link_stat=bol, espn_stat=espn,
                                 week=week, fetch=fetch)
            if df is not None:
                full_df = full_df.vstack(df)

    print(f"BetOnline week {week}: {full_df.height} raw prices")
    if full_df.is_empty():
        return full_df

    # The raw landing file is a hard prerequisite of clean_bol(), not a convenience:
    # get_x_stat() re-reads it rather than taking the frame. So --dry-run cannot
    # simply skip this write, and instead returns before the clean step.
    if not write:
        print("  --dry-run: not writing, so the clean step is skipped "
              "(clean_bol reads the raw file back off disk)")
        return full_df

    full_df.write_parquet(landing_dir("BetOnline", season, "BetOnline_AllProps_Raw.parquet"))
    archive_raw(full_df, season, week)
    _store_line_history(full_df, season, sched)

    BOL_STATS = clean_bol(current_sched=current_sched, season=season)
    BOL_STATS.write_parquet(landing_dir("BetOnline", season, "BetOnline_AllProps_Clean.parquet"))
    BOL_STATS.write_csv(landing_dir("BetOnline", season, "BetOnline_AllProps_Clean.csv"))

    reconcile_BOL(prop_df=BOL_STATS, season=season)
    print(f"  wrote {BOL_STATS.height} cleaned rows")
    return BOL_STATS


def main(argv=None) -> int:
    """Command-line entry point.

    Usage::

        python -m Scripts.scrape_BOL
        python -m Scripts.scrape_BOL --week 3 --dry-run
    """
    import argparse

    p = argparse.ArgumentParser(
        prog="python -m Scripts.scrape_BOL",
        description="Scrape BetOnline weekly NFL player props.",
    )
    p.add_argument("--season", type=int, help="defaults to the schedule's season")
    p.add_argument("--week", type=int, help="defaults to the first unplayed week")
    p.add_argument("--dry-run", action="store_true", help="do not write files")
    args = p.parse_args(argv)

    df = scrape_week(season=args.season, week=args.week, write=not args.dry_run)
    # An empty scrape is a failure, not a success -- matching scrape_pinnacle_season.
    # The nightly's `|| fail` is built on this.
    return 0 if not df.is_empty() else 1


if __name__ == "__main__":
    raise SystemExit(main())
