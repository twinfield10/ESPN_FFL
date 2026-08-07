"""Multi-source projection blending, shared by the weekly pipeline and the notebook.

These functions previously existed as two hand-maintained copies -- one in
``populateGoogleSheet.py`` and one pasted into ``FF Analysis Notebook.ipynb`` --
which had drifted apart on 8 of 12 functions. The notebook you used to *decide*
a lineup and the script that *published* it were computing different numbers.
This module is the single copy; both callers import from it.

The script versions were kept as the base throughout, since they were the ones
that produced the 2025 season's published data and were uniformly the newer,
league-aware variants. Three functions took new parameters where the script
copies had been reading module-level globals (``curr_week``, ``LINEUPS``,
``lg_vars``/``select_league``); those are called out in their docstrings.

The pipeline order is:

    ESPN stats -> FantasyPros -> MEAN -> Pinnacle -> BetOnline -> TRUE -> points

where ``TRUE_*`` is the weighted blend and ``*_Points`` applies the league's own
ESPN scoring settings via :func:`proj_to_score`.
"""

import warnings
from typing import Dict, Optional

import pandas as pd

from Scripts.paths import NFL_TACKLES_CSV, resolve, season_dir
from Scripts.scoring import get_scoring_table
from Scripts.scrape_player_stats import SLOT_BASE, SLOT_DST


# These three are read-only lookups -- the scrapers write through season_dir()
# directly. create=False so that asking whether a season has a file cannot create
# an empty directory for it.

def fantasypros_parquet(season: int):
    """Season's FantasyPros projections file."""
    return season_dir("FantasyPros", season,
                      "FantasyPros_Projections_Week_All.parquet", create=False)


def pinnacle_parquet(season: int):
    """Season's accumulated Pinnacle props file."""
    return season_dir("Pinnacle", season, "Pinnacle_Props_Week_All.parquet",
                      create=False)


def betonline_parquet(season: int):
    """Season's accumulated BetOnline props file."""
    return season_dir("BetOnline", season, "BetOnline_AllProps.parquet",
                      create=False)


#: The keys every weekly projection source is merged onto. A source with no file
#: for the season returns an empty frame carrying only these -- see
#: :func:`absent_weekly_source`.
SOURCE_JOIN_KEYS = ["week", "player_name"]


class MissingProjectionSourceWarning(UserWarning):
    """A weekly projection source has no file for the requested season."""


def _warn_missing(msg: str) -> None:
    """Warn about an absent source in a way the global filter cannot swallow.

    ``Scripts/fetch_utils.py`` calls ``warnings.filterwarnings("ignore")`` at
    module scope, so a plain ``warnings.warn`` here would be silenced -- which is
    the exact failure mode this warning exists to prevent. Mirrors
    ``Scripts.scoring._warn``; see ``docs/plans/06-performance.md`` for the
    global filter.

    Args:
        msg: The warning text.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("always", MissingProjectionSourceWarning)
        warnings.warn(msg, MissingProjectionSourceWarning, stacklevel=3)


def absent_weekly_source(label: str, path) -> pd.DataFrame:
    """The empty frame that stands in for a weekly source with no file.

    Returning this rather than raising lets the rest of the blend treat the
    source as wholly absent: :func:`impute_columns` creates its columns from
    ``MEAN_`` and flags every cell imputed, and :func:`compute_weighted_stats`
    drops the imputed weight and renormalises over the sources that are real. The
    result is an honest ESPN/FantasyPros blend rather than a crash.

    This is the pre-season state every year -- weekly props do not exist until
    the season starts -- and it is what the store in
    ``docs/plans/07-frontend-foundation.md`` has to be buildable in.

    Args:
        label: Human-readable source name, e.g. ``"Pinnacle"``.
        path: The file that was looked for, quoted in the warning.

    Returns:
        pd.DataFrame: Empty, with :data:`SOURCE_JOIN_KEYS` as its columns. Dtypes
        are set explicitly rather than left as ``object``, because pandas
        validates dtype compatibility on a merge key even when one side is empty.
    """
    _warn_missing(
        f"{label} has no weekly props for this season ({path} does not exist). "
        f"Its columns will be imputed from the ESPN/FantasyPros mean and dropped "
        f"from the renormalised blend, so TRUE_* is an ESPN/FP number for every "
        f"row. Check the coverage report below."
    )
    return pd.DataFrame({
        "week": pd.Series(dtype="int64"),
        "player_name": pd.Series(dtype="object"),
    })


def weekly_sources_present(season: int) -> Dict[str, bool]:
    """Which weekly projection sources have a file for ``season``.

    Recorded in the store's ``meta.json`` so the app can show a degraded source
    rather than rendering an ESPN-only number that looks like a four-source
    blend.

    Args:
        season: Season year.

    Returns:
        dict: ``{"fantasypros": bool, "pinnacle": bool, "betonline": bool}``.
    """
    return {
        "fantasypros": fantasypros_parquet(season).exists(),
        "pinnacle": pinnacle_parquet(season).exists(),
        "betonline": betonline_parquet(season).exists(),
    }


_TACKLE_DIM: Optional[pd.DataFrame] = None


def get_tackle_dim() -> pd.DataFrame:
    """Solo/assist tackle ratios by defensive position, loaded once and cached.

    Used to split a projected total-tackle line into the solo and assisted
    components that IDP scoring settings price separately. Loaded lazily rather
    than at import time so that importing this module never touches the disk.

    Returns:
        pd.DataFrame: Columns ``pos`` and ``tackle_ratio``.
    """
    global _TACKLE_DIM
    if _TACKLE_DIM is None:
        _TACKLE_DIM = pd.read_csv(NFL_TACKLES_CSV)
    return _TACKLE_DIM


def change_col_prefix(df, old_pfix, new_pfix):

    df = df
    df.columns = df.columns.str.replace(f'{old_pfix}', new_pfix, regex=False)
    return df


#: Suffix marking a filled-in cell. ``PINNY_receivingYards_is_imputed`` is True
#: where that projection came from another source rather than from Pinnacle.
IMPUTED_SUFFIX = "_is_imputed"

#: Blend weights per stat, with a ``default`` fallback. Applied only across the
#: sources that have *real* data for a given row -- see :func:`compute_weighted_stats`.
#: Lifted to module scope so the weights can be inspected and tested without running
#: the whole pipeline. Only meaningful in proportion, since renormalisation divides by
#: whatever subset is real, so a set summing to 1.0 is conventional rather than
#: required.
#:
#: **Set 2026-08-07 to an equal three-way split of ESPN, FantasyPros and the usage
#: model, with Pinnacle and BetOnline at zero.** This is an owner decision, not a
#: fitted result, and the two halves of it deserve separate notes.
#:
#: **On ``USG`` moving off 0.0.** Plan 18 gated a non-zero weight on G2 -- the blend
#: with and without the model, scored against realised results -- which cannot be run
#: on any past season, because FantasyPros' URLs take no season parameter and no
#: historical pre-season blend survives. That remains true and G2 remains unanswered.
#: What changed is the evidence around it: the model now beats the naive draft
#: heuristic on **every metric at every position**, out of sample, in 26 of 28
#: season-position cells across a seven-fold walk-forward, and the folds never used for
#: feature selection score as well as or better than the ones that were. Weighting it
#: in is still an assertion rather than a measurement; it is now an assertion with a
#: lot behind it, made deliberately rather than inherited.
#:
#: ``USG`` enters on an **if-healthy basis** -- :func:`Scripts.usage.project.to_full_slate`
#: rescales the model's expected-value line to a full 17-game slate before it reaches
#: the blend, so all three sources describe the same quantity. Without that the blend
#: mixed an availability-discounted source with two undiscounted ones, and did so
#: unevenly: the usage model covers QB/RB/WR/TE and not K or D/ST, so skill positions
#: came out at 0.887-0.900 of their ESPN/FantasyPros level while kickers and defences
#: sat at exactly 1.000. That is 11% of cross-position distortion in a blend whose job
#: is to be comparable across positions. Rescaled, the same ratios are 0.974-1.012.
#:
#: **On Pinnacle and BetOnline going to zero.** Note this drops the better-covered of
#: the two market sources: BetOnline's season endpoint works and resolves **273
#: players with 13 stat columns including IDP tackles and sacks**, against
#: FantasyPros' 60 players. Only the *weekly* BetOnline endpoint is blocked, and that
#: is a different host which never fed this path. Reinstating it is one number here.
#:
#: The previous hand-tuned table, for the record and for plan 03 step 3's re-tune:
#:
#:     passingYards        ESPN 0.1  FP 0.7  PINNY 0.1   BOL 0.1
#:     passingTouchdowns   ESPN 0.1  FP 0.1  PINNY 0.4   BOL 0.4
#:     rushingYards        ESPN 0.2  FP 0.3  PINNY 0.25  BOL 0.25
#:     receivingYards      ESPN 0.2  FP 0.3  PINNY 0.25  BOL 0.25
#:     default             ESPN 0.2  FP 0.3  PINNY 0.25  BOL 0.25
#:
#: The per-stat keys are gone with it. They existed to hold per-stat differences and
#: there are none now; a row of identical dicts is a thing that drifts out of sync
#: rather than a structure. :func:`compute_weighted_stats` reads ``default`` for any
#: stat without its own entry, so re-adding one is a single line.
WEIGHTS = {
    'default': {'ESPN': 1 / 3, 'FP': 1 / 3, 'PINNY': 0.0, 'BOL': 0.0, 'USG': 1 / 3},
}


def impute_columns(df, target_prefix, source_prefix, track=True):
    """Fill missing ``target_prefix`` columns from ``source_prefix``, recording which.

    The provenance flags are the point. Without them, a filled cell is
    indistinguishable from a real one, so ``compute_weighted_stats`` weights an
    imputed value as though it were an independent opinion -- and since the
    sportsbook columns are imputed from ``MEAN_`` (the ESPN/FantasyPros average),
    that counts ESPN and FantasyPros two or three times over. Measured on
    Knights_FFL week 17 2025, 60%+ of Pinnacle and BetOnline receiving-yard cells
    were imputed while still carrying a full 25% weight each.

    Flags accumulate across calls: this function runs more than once per source
    (once on the merged frame, once on ``base`` to catch rows that did not join at
    all), and a cell imputed by any call stays flagged.

    Args:
        df: Frame to fill in place.
        target_prefix: Prefix being filled, e.g. ``"PINNY_"``.
        source_prefix: Prefix to fill from, e.g. ``"MEAN_"``.
        track: Write ``*_is_imputed`` companion columns. Off only for callers
            that want the historical behaviour.

    Returns:
        pd.DataFrame: ``df``, with target columns filled and, when ``track``,
        one boolean ``<target><IMPUTED_SUFFIX>`` column per filled column.
    """
    source_cols = [
        col for col in df.columns
        if col.startswith(source_prefix) and not col.endswith(IMPUTED_SUFFIX)
    ]

    flags = {}
    for source_col in source_cols:
        # Define the corresponding target column name
        target_col = target_prefix + source_col[len(source_prefix):]
        flag_col = target_col + IMPUTED_SUFFIX

        # If the target column does not exist, create it by copying the values from the source column
        if target_col not in df.columns:
            if track:
                flags[flag_col] = pd.Series(True, index=df.index)
            df[target_col] = df[source_col]

        # If the target column exists, impute missing values from the source column
        elif source_col in df.columns:
            if track:
                was_missing = df[target_col].isna()
                if flag_col in df.columns:
                    # A cell imputed by an earlier call stays imputed. NaN here
                    # means the row did not join, which is itself a miss.
                    was_missing = was_missing | df[flag_col].fillna(True).astype(bool)
                flags[flag_col] = was_missing
            df[target_col] = df[target_col].fillna(df[source_col])

    if flags:
        # Built as a block rather than inserted one at a time: this runs over ~45
        # stats x 4 sources and column-at-a-time insertion is what produces the
        # PerformanceWarning storm in plan 06.
        new = {k: v for k, v in flags.items() if k not in df.columns}
        existing = {k: v for k, v in flags.items() if k in df.columns}
        for k, v in existing.items():
            df[k] = v
        if new:
            df = pd.concat([df, pd.DataFrame(new, index=df.index)], axis=1)

    return df


def imputed_flag_columns(df):
    """The provenance columns present in ``df``.

    Args:
        df: Any frame produced by :func:`impute_columns`.

    Returns:
        list: Column names ending in :data:`IMPUTED_SUFFIX`.
    """
    return [c for c in df.columns if c.endswith(IMPUTED_SUFFIX)]


def coverage_report(df, sources=('ESPN', 'FP', 'PINNY', 'BOL', 'USG'), stats=None):
    """Per-source share of cells that are real rather than imputed.

    Plan 03 step 4: a source quietly degrading should be visible, not absorbed by
    imputation. ``ESPN`` is the root source and is never imputed, so it reports
    100% wherever it has a column.

    Args:
        df: Blended frame carrying ``*_is_imputed`` columns.
        sources: Source prefixes to report on.
        stats: Restrict to these stat names. Defaults to every stat found.

    Returns:
        pd.DataFrame: Columns ``source``, ``stat``, ``n``, ``real``, ``real_pct``,
        sorted worst-covered first.
    """
    rows = []
    for source in sources:
        prefix = f"{source}_"
        cols = [
            c for c in df.columns
            if c.startswith(prefix) and not c.endswith(IMPUTED_SUFFIX)
        ]
        for col in cols:
            stat = col[len(prefix):]
            if stats is not None and stat not in stats:
                continue
            flag = col + IMPUTED_SUFFIX
            n = len(df)
            if flag in df.columns:
                real = int((~df[flag].fillna(True).astype(bool)).sum())
            else:
                real = int(df[col].notna().sum())
            rows.append({"source": source, "stat": stat, "n": n, "real": real,
                         "real_pct": round(100.0 * real / n, 1) if n else 0.0})
    if not rows:
        # A frame with no source-prefixed columns has an empty report, not a
        # broken one. Without the declared columns, sort_values below raises
        # KeyError: 'real_pct' -- which is how a store write could be taken down
        # by the metadata it was only annotating.
        return pd.DataFrame(columns=["source", "stat", "n", "real", "real_pct"])
    out = pd.DataFrame(rows)
    return out.sort_values(["real_pct", "source", "stat"]).reset_index(drop=True)


def print_coverage_report(df, weights_dict=None, key_stats=(
    'passingYards', 'passingTouchdowns', 'rushingYards',
    'receivingYards', 'receivingReceptions',
)):
    """Print per-source real coverage, so a degrading source is visible.

    Args:
        df: Blended frame carrying provenance flags.
        weights_dict: Weights, used only to show the nominal weight alongside the
            coverage it is actually backed by.
        key_stats: Stats to break out individually. The overall average covers all.
    """
    rep = coverage_report(df)
    if rep.empty:
        return

    print("")
    print("========== Projection Source Coverage (% real, not imputed) ==========")
    overall = rep.groupby("source")["real_pct"].mean().round(1)
    sources = [s for s in ("ESPN", "FP", "PINNY", "BOL", "USG") if s in overall.index]

    header = f"  {'stat':<24}" + "".join(f"{s:>12}" for s in sources)
    print(header)
    print("  " + "-" * (len(header) - 2))

    by_stat = rep.set_index(["stat", "source"])["real_pct"]
    for stat in key_stats:
        cells = []
        for s in sources:
            try:
                pct = by_stat.loc[(stat, s)]
                w = (weights_dict or {}).get(stat, (weights_dict or {}).get("default", {})).get(s)
                cells.append(f"{pct:>6.1f}% w{w:<4.2f}" if w is not None
                             else f"{pct:>11.1f}%")
            except KeyError:
                cells.append(f"{'-':>12}")
        print(f"  {stat:<24}" + "".join(f"{c:>12}" for c in cells))

    print("  " + "-" * (len(header) - 2))
    print(f"  {'ALL STATS (mean)':<24}" + "".join(f"{overall[s]:>11.1f}%" for s in sources))
    print("")


def create_mean_cols(df, target_prefix, source_prefix, mean_prefix='MEAN_'):
    target_cols = [col for col in df.columns if col.startswith(target_prefix)]
    source_cols = [col for col in df.columns if col.startswith(source_prefix)]

    for source_col in source_cols:
        target_col = target_prefix + source_col[len(source_prefix):]
        mean_col = mean_prefix + source_col[len(source_prefix):]

        if target_col in df.columns and source_col in df.columns:
            df[mean_col] = df[[target_col, source_col]].mean(axis=1)

    df = df[['week', 'player_name', 'primaryPosition','player_active_status']  + list(df.filter(like='MEAN_').columns)]

    return df


def clean_pinny(pinny_path=None, season=None):
    """Load the Pinnacle season props file.

    Args:
        pinny_path: Explicit parquet location. Relative paths resolve against
            the repo root. Takes precedence over ``season``.
        season: Season to load. Required unless ``pinny_path`` is given.

    Returns:
        pd.DataFrame: Raw Pinnacle props, or the empty frame from
        :func:`absent_weekly_source` when ``season`` has no props file yet.

    Raises:
        FileNotFoundError: When an explicit ``pinny_path`` does not exist. A
            named file that is missing is a typo, not an absent season.

    Note:
        Most of this function's body is commented out upstream -- the pivot,
        TD-split, no-vig adjustment and scoring call are all inert, so it
        currently returns essentially raw data. Preserved as-is during the
        module extraction; see docs/STATE_OF_THE_REPO.md.
    """
    if pinny_path is not None:
        pinny_path = resolve(pinny_path)
    elif season is not None:
        pinny_path = pinnacle_parquet(season)
        if not pinny_path.exists():
            return absent_weekly_source("Pinnacle", pinny_path)
    else:
        raise ValueError("clean_pinny requires either pinny_path or season")

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

    name_changes={
        # Pinny -> ESPN #
        "Tre Harris": "Tre' Harris",
        "Marvin Mims": "Marvin Mims Jr.",
        "Travis Etienne": "Travis Etienne Jr.",
        "Aaron Jones": "Aaron Jones Sr.",
        "Kyle Pitts": "Kyle Pitts Sr.",
        "Calvin Austin": "Calvin Austin III",
        "Ollie Gordon":"Ollie Gordon II",
        "Marvin Harrison": "Marvin Harrison Jr.",
        "Kyle Pitts": "Kyle Pitts Sr.",
        "Marvin Mims": "Marvin Mims Jr.",
        "Travis Etienne": "Travis Etienne Jr.",
        "Aaron Jones": "Aaron Jones Sr.",
        "Zonovan Knight": "Bam Knight"
    }

    # Load
    raw=pd.read_parquet(pinny_path)

    # Get Stats
    #raw.replace({"PropType": prop_to_stat}, inplace=True)

    # Filter
    #raw = raw[raw['PropType'].isin(list(prop_to_stat.values()))]

    # Clean Names
    raw.replace({"player_name": name_changes}, inplace=True)

    # Replace NaN in Value with ImpNoVig
    #raw['Value'] = raw['Value'].fillna(raw['ImpNoVig'])

    def adjust_value(df):
        pivoted_df = df.pivot_table(
            index=['officialDate', 'week', 'Away', 'Home', 'Player', 'PropType', 'Value', 'BetTimeStamp'],
            columns='OverUnder',
            values=['Price', 'Implied', 'ImpNoVig'],
            aggfunc='first'  # Use 'first' in case there are duplicates
        ).reset_index()

        # Flatten the column names and add OverUnder values as suffixes
        pivoted_df.columns.name = None
        new_columns = []

        for col in pivoted_df.columns:
            if col[0] in ['Price', 'Implied', 'ImpNoVig']:
                new_columns.append(f"{col[0]}_{col[1]}")
            else:
                new_columns.append(col[0])

        pivoted_df.columns = new_columns

        # Create Adjusted Values From Juice
        pivoted_df["Juice"] = pivoted_df['Implied_Over'] + pivoted_df['Implied_Under']
        pivoted_df["Over_Juice"] = (1 / pivoted_df['Implied_Over'] - 1)
        pivoted_df["Under_Juice"] = (1 / pivoted_df['Implied_Under'] - 1)
        pivoted_df["Juice_Diff"] = pivoted_df["Under_Juice"] - pivoted_df["Over_Juice"]
        pivoted_df["AdjValue"] = pivoted_df["Value"] + (pivoted_df["Juice_Diff"] * pivoted_df["Value"] * 0.5)

        return pivoted_df
    
    #adjusted = adjust_value(raw)


    #adjusted = adjusted[['week', 'Player', 'PropType', 'AdjValue']]
    #adjusted.columns = ['week', 'player_name', 'statType', 'statValue']

    # Pivot
    #clean = adjusted.pivot_table(index=['week','player_name'], columns='statType', values='statValue', aggfunc='mean').reset_index()

    

    # Split Touchdowns by Usage (Rushing/Receiving)
    #if 'rushingTouchdowns' in clean.columns:
    #    clean['receivingTouchdowns'] = clean['rushingTouchdowns'] * (clean['receivingYards'] / (clean['receivingYards'] + clean['rushingYards']))
    #    clean['rushingTouchdowns'] = clean['rushingTouchdowns'] - clean['receivingTouchdowns']

    #clean.columns = ['week', 'player_name'] + ['proj_' + str(col) for col in clean.columns[2:]]
    #clean = clean[['week', 'player_name'] + list(clean.columns[2:])]

    # Flatten If Needed
    #clean.columns.name = None
    #clean.columns = [col if col is not None else 'StatValue' for col in clean.columns]

    #final = proj_to_score(proj_df=clean, col_pfix="PINNY")

    return raw


def clean_bol(bol_path=None, season=None, tackle_dim=None):
    """Load BetOnline props and normalise player names to ESPN spellings.

    Args:
        bol_path: Explicit parquet location. Relative paths resolve against the
            repo root. Takes precedence over ``season``.
        season: Season to load. Required unless ``bol_path`` is given.
        tackle_dim: Solo/assist tackle ratios by position. ``None`` loads the
            cached default via :func:`get_tackle_dim`. Only consulted when the
            input carries ``proj_defensiveTotalTackles`` (IDP leagues).

    Returns:
        pd.DataFrame: BetOnline projections with ESPN-compatible player names, or
        the empty frame from :func:`absent_weekly_source` when ``season`` has no
        props file yet.

    Raises:
        FileNotFoundError: When an explicit ``bol_path`` does not exist. A named
            file that is missing is a typo, not an absent season.
    """
    # Load
    if bol_path is not None:
        bol_path = resolve(bol_path)
    elif season is not None:
        bol_path = betonline_parquet(season)
        if not bol_path.exists():
            return absent_weekly_source("BetOnline", bol_path)
    else:
        raise ValueError("clean_bol requires either bol_path or season")
    raw = pd.read_parquet(bol_path).drop(columns=['team'])

    name_changes={
        # BOL -> ESPN #
        "Tre Harris": "Tre' Harris",
        "Kyle Pitts": "Kyle Pitts Sr.",
        "Deebo Samuel Sr.":"Deebo Samuel",
        "Cameron Ward":"Cam Ward",
        "Marquise Brown":"Hollywood Brown",
        "Ray-Ray McCloud": "Ray-Ray McCloud III",
        "Chris Godwin": "Chris Godwin Jr.",
        "Anthony Richardson": "Anthony Richardson Sr.",
        "Oronde Gadsden": "Oronde Gadsden II",
        "James Cook": "James Cook III",
        "Zonovan Knight": "Bam Knight",
        "Calvin Austin": "Calvin Austin III",
        "Ollie Gordon": "Ollie Gordon II"
    }

    if 'proj_defensiveTotalTackles' in raw.columns:
        tkls = get_tackle_dim() if tackle_dim is None else tackle_dim
        raw = raw.merge(tkls, left_on="position", right_on="pos", how="left")
        raw['proj_defensiveAssistedTackles'] = raw['proj_defensiveTotalTackles'] / (raw['tackle_ratio'] + 0.5)
        raw['proj_defensiveSoloTackles'] = raw['tackle_ratio'] * raw['proj_defensiveAssistedTackles']

    # Join Tackle DataFrame
    raw = raw.drop(columns=['position', 'pos'])

    raw.replace({"player_name": name_changes}, inplace=True)

    return raw


def get_match_details(df1, df2, keys, check_col2, tbl_lab, min_wk):
    """Report how many of ``df1``'s players failed to join to ``df2``.

    Args:
        df1: Left frame, expected to carry ``week``, ``primaryPosition``,
            ``player_active_status`` and ``MEAN_*`` columns.
        df2: Right frame -- one projection source.
        keys: Join keys.
        check_col2: A column only ``df2`` supplies; its nullity after a left join
            is what identifies an unmatched row.
        tbl_lab: Label for the printed report.
        min_wk: Restrict the check to this week.
    """
    if check_col2 not in df2.columns:
        # The source has no file for this season, so there is nothing to match
        # against. Indexing check_col2 below would raise KeyError.
        print(f"{tbl_lab}: no data for this season, skipping the match check")
        print(" ")
        return

    # Only Check Weeks that Exist in Data w/ Projected Stats
    df1 = df1[((df1['week'] == min_wk) & (~df1['primaryPosition'].isin(['D/ST', 'K', 'DL', 'DE', 'LB', 'NT', 'CB', 'S', 'DT', 'DB', 'OLB'])))]
    df1 = df1[df1.filter(like='MEAN_').sum(axis=1) > 0]
    df1 = df1[df1['player_active_status'] == 'active']

    # Option 1 - Count unmatched values
    merged_df = pd.merge(df1, df2, on=keys, how='left')
    unmatched_from_df2 = merged_df[check_col2].isnull().sum()
    
    # If > 0, print more information
    if unmatched_from_df2 > 0:
        print(f'Unmatched from {tbl_lab}: {unmatched_from_df2}')

        merged_df = pd.merge(df1, df2, on=keys, how='left', indicator=True)
        unmatched_rows = merged_df[merged_df['_merge'] != 'both']
        unmatched_count = unmatched_rows.shape[0]
        print(unmatched_rows[['week', 'player_name']])

        # Option 3 - Grouping To get Count
        unmatched_count = merged_df['_merge'].value_counts()
        print(unmatched_count)
        print(" ")
    elif unmatched_from_df2 == 0:
        print(f"All Rows in {tbl_lab} Match")
        print(" ")


def compute_weighted_stats(df, stats_list, weights_dict, renormalise=True):
    """Blend each source's projection into a ``TRUE_`` column.

    When a source's cell is flagged imputed (see :func:`impute_columns`), its
    weight is **removed and the remainder renormalised**, rather than letting a
    filled-in value absorb a full share as though it were an independent opinion.

    Concretely, with the default weights and a player Pinnacle has no line for:

    ``PINNY_`` is imputed from ``MEAN_`` = avg(ESPN, FP), and ``FP_`` is itself
    imputed from ESPN for most players. Weighting all four at face value made
    ``TRUE_passingTouchdowns`` (PINNY 0.4 + BOL 0.4) essentially pure ESPN wearing
    a four-source badge. Renormalising gives a player with real book lines a
    genuine four-source blend, and a player without one an honest ESPN/FP blend.

    Args:
        df: Frame with ``<SOURCE>_<stat>`` columns, optionally with
            ``<SOURCE>_<stat>_is_imputed`` companions.
        stats_list: Stat names to blend.
        weights_dict: ``{stat: {source: weight}}`` plus a ``'default'`` entry.
        renormalise: Set False for the historical face-value behaviour.

    Returns:
        pd.DataFrame: ``df`` with one ``TRUE_<stat>`` column per stat.

    Note:
        A source with no provenance column counts as real. That keeps this
        function correct on frames built without imputation tracking, and is why
        the pre-existing unit tests still describe the behaviour accurately.

        Where *every* source for a stat is imputed the renormalised denominator is
        zero, and the face-value sum is used instead. That can only happen when
        the root source is absent, and falling back keeps the result identical to
        the historical output rather than substituting a zero.
    """
    new_cols = {}

    for stat in stats_list:
        weights = weights_dict.get(stat, weights_dict['default'])

        numerator = pd.Series(0.0, index=df.index)
        denominator = pd.Series(0.0, index=df.index)
        face_value = pd.Series(0.0, index=df.index)

        for source, weight in weights.items():
            col_name = f"{source}_{stat}"
            if col_name not in df.columns:
                continue

            values = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0)
            face_value = face_value + values * weight

            flag_col = col_name + IMPUTED_SUFFIX
            if flag_col in df.columns:
                # NaN flag means the row never joined, so treat it as imputed.
                is_real = ~df[flag_col].fillna(True).astype(bool)
            else:
                is_real = pd.Series(True, index=df.index)

            numerator = numerator + values * weight * is_real
            denominator = denominator + weight * is_real

        if renormalise:
            blended = numerator.divide(denominator.where(denominator > 0))
            new_cols[f'TRUE_{stat}'] = blended.fillna(face_value)
        else:
            new_cols[f'TRUE_{stat}'] = face_value

    # One concat rather than ~45 individual inserts into a 350-column frame.
    for name, series in new_cols.items():
        if name in df.columns:
            df[name] = series
    fresh = {k: v for k, v in new_cols.items() if k not in df.columns}
    if fresh:
        df = pd.concat([df, pd.DataFrame(fresh, index=df.index)], axis=1)

    return df


#: Positions ESPN fills from individual defensive players rather than a team
#: D/ST unit. Listed for clarity about what an IDP league rosters; the scoring
#: split below keys on :data:`DST_POSITIONS` instead, because ESPN's override map
#: singles out the D/ST slot rather than singling out IDP slots.
IDP_POSITIONS = ['DL', 'DE', 'LB', 'NT', 'CB', 'S', 'DT', 'DB', 'OLB']

#: The only position whose scoring comes from a ``pointsOverrides`` entry. Every
#: other slot -- offence, kicker and every individual defensive slot -- scores the
#: rule's base value, because ESPN sets no override for those slots.
DST_POSITIONS = ['D/ST']


def _apply_scoring(df, s_df, col_pfix_list):
    """Sum each prefix's stat columns into a ``<prefix>_Points`` column.

    **A stat the source did not project scores 0, but a source that projected
    nothing at all scores NaN.** The distinction matters and the first version of
    this function did not make it: it summed straight through, so a single NaN
    cell made the whole total NaN. The weekly path never noticed because
    ``clean_lineups`` imputes and 0-fills every source before scoring. The season
    path is sparse -- a running back has no ``ESPN_passingYards`` -- and passing
    yards is a scored rule in all nine leagues, so **every per-source
    ``*_Points`` column on every stored draft board was NaN for every row**,
    1026 of 1026. Only ``TRUE_Points`` survived, because the blend is dense.

    The NaN for a wholly absent source is deliberate rather than 0.0: this repo's
    recurring failure mode is an absent source reading as agreement (see
    ``docs/plans/03-projection-source-coverage.md``), and a book with no line is
    not a book projecting zero points.

    A NaN ``points`` value still poisons the total, which is intended -- an
    unrecognised scoring rule should be loud, not silently worth nothing. See
    ``docs/plans/01-scoring-coverage.md``.

    Args:
        df: Projection frame. Modified in place.
        s_df: Scoring table with ``colName`` and ``points``.
        col_pfix_list: Projection-source prefixes to score.

    Returns:
        The same frame, for chaining.
    """
    for col_pfix in col_pfix_list:
        # Accumulate into one Series and assign once, rather than += onto the
        # frame per rule, which fragments a 350-column block.
        total = pd.Series(0.0, index=df.index)
        scored_any = pd.Series(False, index=df.index)
        for _, score_row in s_df.iterrows():
            col_name = f"{col_pfix}_{score_row['colName']}"
            if col_name not in df.columns:
                continue
            values = df[col_name]
            total = total + values.fillna(0) * score_row['points']
            scored_any = scored_any | values.notna()
        df[f'{col_pfix}_Points'] = total.where(scored_any)
    return df


def proj_to_score(proj_df, s_league, col_pfix_list=['ESPN', 'FP', 'MEAN', 'PINNY',
                                                    'BOL', 'USG', 'TRUE']):
    """Score projected stat lines with a league's rules, per lineup slot.

    ESPN prices the same rule differently depending on the slot a player occupies
    -- a sack is worth one thing to a D/ST unit and another to an individual
    defensive player. It expresses this as a ``pointsOverrides`` map keyed by slot
    id, and slot 16 (D/ST) is the only key any of the configured leagues sets. So
    a D/ST unit is scored from the override and **everything else** -- offence,
    kicker, and every individual defensive slot -- from the rule's base value.

    Scoring offence from the override, as an earlier revision of this did, is
    wrong in the same direction for both league types: it prices an offensive
    player's stray imputed defensive stats at the D/ST rate.

    This used to be a block keyed on the literal league id ``1727104`` that
    patched in hardcoded constants. Six of its seven IDP values were wrong
    against live settings, inflating that league's IDP projections roughly 2-3x;
    it also overwrote three rules ``espn_api`` already reported correctly, and
    missed the two it existed to fix. See ``docs/plans/11-per-slot-scoring.md``.

    Args:
        proj_df: Projection frame carrying ``<prefix>_<stat>`` columns and
            ``primaryPosition``.
        s_league: League whose scoring applies, passed to
            :func:`Scripts.scoring.get_scoring_table`.
        col_pfix_list: Projection-source prefixes to score.

    Returns:
        pd.DataFrame: ``proj_df`` with a ``<prefix>_Points`` column per prefix.
    """
    is_dst = proj_df['primaryPosition'].isin(DST_POSITIONS)

    # A frame with no D/ST unit has nothing the override applies to.
    if not is_dst.any():
        return _apply_scoring(
            proj_df, get_scoring_table(s_league, slot=SLOT_BASE), col_pfix_list)

    dst_df = proj_df[is_dst].copy()
    rest_df = proj_df[~is_dst].copy()

    _apply_scoring(dst_df, get_scoring_table(s_league, slot=SLOT_DST), col_pfix_list)
    _apply_scoring(rest_df, get_scoring_table(s_league, slot=SLOT_BASE), col_pfix_list)

    # TRUE_Points comes from the TRUE_* columns like every other prefix. It used
    # to be hardcoded for IDP rows as (ESPN_Points + BOL_Points) / 2, which
    # bypassed the renormalised blend -- see
    # docs/plans/03-projection-source-coverage.md.
    return pd.concat([rest_df, dst_df])


def clean_lineups(df, lg, season=None):
    """Blend ESPN, FantasyPros, Pinnacle and BetOnline into league-scored points.

    Args:
        df: Lineup frame from ``get_ply_stats_by_matchup`` plus free agents.
        lg: ESPN ``League``. Supplies the scoring settings, current week, and
            (by default) the season whose projection files are read.
        season: Override the projection season. Defaults to ``lg.year``, so the
            projections always come from the same season as the league.

    Returns:
        pd.DataFrame: One row per player-week with ``ESPN_``/``FP_``/``MEAN_``/
        ``PINNY_``/``BOL_``/``TRUE_`` stat columns and matching ``*_Points``.
    """
    season = lg.year if season is None else season

    # Get Base of Projections (player_name, week, team, etc.)
    base_cols = ['league_id','year','week', 'team_owner', 'team_name', 'team_division', 'player_name', 'player_id', 'slotPosition', 'primaryPosition', 'eligiblePositions', 'pro_team', 'current_team_id' ,'player_position' ,'player_active_status', 'points', 'projPoints']
    scores_df = get_scoring_table(lg)
    actual_scoring_cols = scores_df['colName'].to_list()
    base = df[base_cols + actual_scoring_cols]

    curr_week = lg.current_week

    # Constants

    ## Defensive Positions
    d_pos = ['DL', 'DE', 'LB', 'NT', 'CB', 'S', 'DT', 'DB', 'OLB']

    ## Duplicate Error
    fix_list = ['ESPN_rushingYards', 'ESPN_receivingYards', 'ESPN_passingYards']

    # 1) Combine ESPN and Fantasy Pros Data

    ## a) Build ESPN From Raw Data
    espn_proj = df[['week', 'player_name', 'primaryPosition', 'player_active_status']  + list(df.filter(like='proj_').columns)]
    espn_proj = change_col_prefix(df=espn_proj, old_pfix="proj", new_pfix="ESPN")

    ## b) Build Fantasy Pros From Scrape
    fp_proj = pd.read_parquet(fantasypros_parquet(season)).drop(columns=['STD_FantasyPoints', 'TimeStamp'])
    fp_proj = change_col_prefix(df=fp_proj, old_pfix="proj", new_pfix="FP")

    ## c) Combine ESPN and FP
    trans1_df = espn_proj.merge(fp_proj, how='left', on=['week', 'player_name'])
    get_match_details(df1=espn_proj, df2=fp_proj, keys=["week", "player_name"], check_col2="FP_rushingTouchdowns", min_wk=curr_week, tbl_lab="FantasyPros Table")

    ## d) Correct Error Where ESPN Duplicates Yards
    print("=============================== Correcting ESPN Doubled Yard Projections ===============================")
    print(" ")
    for column in fix_list:
        FP_Col = column.replace("ESPN_", "FP_")
        # Apply with a conditional print statement
        trans1_df[column] = trans1_df.apply(
            lambda row: (
                # Check if the condition is true
                print(f"Player: {row['player_name']}, Week: {row['week']}, "
                      f"Original {column}: {row[column]}, "
                      f"New {column}: {row[column] / 2}") or row[column] / 2
                if (row[column] > (row[FP_Col] * 1.75)) and (row[column] > 40) else row[column]
            ),
            axis=1
        )

    print(" ")
    print("======================================== End Doubled Correction ========================================")
    print(" ")

    ## e) Impute FP with ESPN + Create Means
    trans1_df = impute_columns(trans1_df, target_prefix='FP_', source_prefix='ESPN_')
    mean_df = create_mean_cols(trans1_df, target_prefix='FP_', source_prefix='ESPN_')

    ## f) Retain New ESPN Values For Join With Books + Add To Base
    base = base.merge(trans1_df, on=['week', 'player_name', 'primaryPosition','player_active_status'], how='left')

    ## f) Create Dataframe of Means For Imputing Sportsbook Data
    mean_df = create_mean_cols(trans1_df, target_prefix='FP_', source_prefix='ESPN_')
    

    # 2) Combine Pinnacle Data With ESPN and Impute
    ## a) Clean Pinnacle Data
    pinny_proj = clean_pinny(season=season)
    pinny_proj = change_col_prefix(df=pinny_proj, old_pfix="proj", new_pfix="PINNY")

    ## b) Impute Missing Data From ESPN
    trans2_df = mean_df.merge(pinny_proj, on=["week", "player_name"], how='left')
    get_match_details(df1=mean_df, df2=pinny_proj, keys=["week", "player_name"], check_col2="PINNY_receivingYards", min_wk=curr_week, tbl_lab="Pinnacle Sportsbook Table")
    trans2_df = impute_columns(trans2_df, target_prefix='PINNY_', source_prefix="MEAN_")
    

    ## c) Slim Columns To Only Pinnacle Data
    trans2_df = trans2_df[['week', 'player_name', 'primaryPosition','player_active_status'] + list(trans2_df.filter(like='PINNY').columns)]
    ## d) Join Slim Transformation Back To Base
    base = base.merge(trans2_df, on=['week', 'player_name', 'primaryPosition','player_active_status'], how='left')


    # 3) Combine BetOnline Data With ESPN and Impute
    bol_proj = clean_bol(season=season)
    bol_proj = change_col_prefix(df=bol_proj, old_pfix="proj", new_pfix="BOL")

    ## b) Impute Missing Data From ESPN
    trans3_df = mean_df.merge(bol_proj, on=["week", "player_name"], how='left')
    get_match_details(df1=mean_df, df2=bol_proj, keys=["week", "player_name"], check_col2="NFL_game_id", min_wk=curr_week, tbl_lab="BetOnline Sportsbook Table")
    trans3_df = impute_columns(trans3_df, target_prefix='BOL_', source_prefix="MEAN_")

    ## c) Slim Columns To Only BOL Data
    trans3_df = trans3_df[['week', 'player_name', 'primaryPosition','player_active_status'] + list(trans3_df.filter(like='BOL_').columns)]

    ## d) Join Slim Transformation Back To Base
    base = base.merge(trans3_df, on=['week', 'player_name', 'primaryPosition','player_active_status'], how='left')

    ## Clean Missing COlumns
    base = impute_columns(base, target_prefix='PINNY_', source_prefix='MEAN_')
    base = impute_columns(base, target_prefix='BOL_', source_prefix='MEAN_')


    ## 4b) Report how much of each source is real rather than imputed.
    ##
    ## Measured on Knights_FFL 2025, averaged over all 45 stats: ESPN 100%,
    ## FantasyPros 13%, BetOnline 12%, Pinnacle 8%. Weighting imputed cells at
    ## face value made the nominal four-source blend roughly 90% ESPN. The weights
    ## below are renormalised over whichever sources are real per row, so this
    ## report is the thing to watch when a source degrades.
    print_coverage_report(base, weights_dict=WEIGHTS)

    ## 5) Create Aggregate Columns For Each Projection Type (Manual Weights)
    final = compute_weighted_stats(df=base, stats_list=actual_scoring_cols,
                                   weights_dict=WEIGHTS)

    ## 6) Build Score Column
    final = proj_to_score(proj_df=final, s_league=lg)

    # a) Adjust For Extra ESPN Stats
    final['adjustment'] = final['projPoints'] - final['ESPN_Points']
    for i in ['ESPN', 'FP', 'MEAN', 'PINNY', 'BOL', 'TRUE']:
        final[f'{i}_Points'] = final['adjustment'] + final[f'{i}_Points']


    ## 7) Build Position Rank Columns
    for i in ['ESPN', 'FP', 'MEAN', 'PINNY', 'BOL', 'TRUE']:
        final[f"{i}_PosRank"] = final.groupby(['week', 'primaryPosition'])[f'{i}_Points'].rank(ascending=False, method='dense')

    # Actual
    final['PosRank'] = final.groupby(['week', 'primaryPosition'])['points'].rank(ascending=False, method='dense')

    return final


def check_week(lu, week, own, curr_week=None):
    """Build the per-team lineup table for a single week.

    Args:
        lu: Combined lineup frame from :func:`clean_lineups`.
        week: Week to report on.
        own: Team owner name to filter to.
        curr_week: The league's current week. When ``week`` matches it, the
            actual-points columns are dropped because they aren't final yet.
            Defaults to ``week`` itself, which reproduces the weekly pipeline's
            behaviour (it only ever called this for the current week). Pass it
            explicitly when reporting on a completed week.

    Returns:
        pd.DataFrame: Lineup table with per-source projected points.
    """
    if curr_week is None:
        curr_week = week

    lu['trueDiff'] = lu['TRUE_Points'] - lu['projPoints']

    # Get My Team
    df = lu[(lu['week'] == week) & (lu['team_owner'] == own)][['week', 'team_name', 'player_name', 'slotPosition', 'primaryPosition',
                                                               'points', 'projPoints', 'FP_Points', 'PINNY_Points', 'BOL_Points', 'TRUE_Points', 'trueDiff',
                                                               'PosRank', 'ESPN_PosRank', 'FP_PosRank', 'PINNY_PosRank', 'BOL_PosRank', 'TRUE_PosRank']]
    
    df = df.rename(columns={
        'team_name': 'team',
        'player_name': 'player',
        'slotPosition': 'rosPos',
        'primaryPosition': 'primPos',
        'points': 'Actual_PTS',
        'projPoints': 'ESPN_PTS',
        'FP_Points': 'FP_PTS',
        'PINNY_Points': 'PINNY_PTS',
        'BOL_Points': 'BOL_PTS',
        'TRUE_Points': 'TRUE_PTS',
        'trueDiff': 'DIFF_PTS',
        'PosRank': 'Actual',
        'ESPN_PosRank': 'ESPN',
        'FP_PosRank': 'FP',
        'PINNY_PosRank': 'PINNY',
        'BOL_PosRank': 'BOL',
        'TRUE_PosRank': 'TRUE',
    })

    # Order
    bench = pd.DataFrame([{
        'week': week,
        'team': df['team'].iloc[0],
        'player': 'Total',
        'rosPos': 'Starting Lineup',
        'primPos': 'Starting Lineup',
        'Actual_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['Actual_PTS'].sum(),
        'ESPN_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['ESPN_PTS'].sum(),
        'FP_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['FP_PTS'].sum(),
        'PINNY_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['PINNY_PTS'].sum(),
        'BOL_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['BOL_PTS'].sum(),
        'TRUE_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['TRUE_PTS'].sum(),
        'DIFF_PTS': df[~df['rosPos'].isin(['BE', 'IR'])]['DIFF_PTS'].sum(),
        'Actual': '',
        'ESPN': '',
        'FP': '',
        'PINNY': '',
        'BOL': '',
        'TRUE': ''
        
    },
    {
        'week': week,
        'team': df['team'].iloc[0],
        'player': 'Total',
        'rosPos': 'Bench',
        'primPos': 'Bench',
        'Actual_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['Actual_PTS'].sum(),
        'ESPN_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['ESPN_PTS'].sum(),
        'FP_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['FP_PTS'].sum(),
        'PINNY_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['PINNY_PTS'].sum(),
        'BOL_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['BOL_PTS'].sum(),
        'TRUE_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['TRUE_PTS'].sum(),
        'DIFF_PTS': df[df['rosPos'].isin(['BE', 'IR'])]['DIFF_PTS'].sum(),
        'Actual': '',
        'ESPN': '',
        'FP': '',
        'PINNY': '',
        'BOL': '',
        'TRUE': ''
        
    }])

    # Append the summary row to the DataFrame
    df = pd.concat([df, bench], ignore_index=True)

    pos_order = ['QB', 'RB', 'WR', 'TE', 'RB/WR/TE', 'OP', 'DP', 'D/ST', 'K', 'Starting Lineup', 'BE', 'Bench', 'IR']
    order_mapping = {val: idx for idx, val in enumerate(pos_order)}
    df = df.sort_values(by=['rosPos', 'TRUE_PTS'], key=lambda x: x.map(order_mapping))

    # Round
    df = df.round(decimals={'FP_PTS': 2, 'PINNY_PTS': 2, 'BOL_PTS': 2, 'TRUE_PTS': 2, 'DIFF_PTS': 3})

    # Drop Actual if Current Week
    if week == curr_week:
        df.drop(['Actual_PTS', 'Actual'], axis=1, inplace=True)

    return df


def get_league_projections(week, lu):
    df = lu[(lu['week'] == week) & (lu['team_owner'] != 'Free Agent') & (~lu['slotPosition'].isin(['BE', 'IR']))][['week', 'team_owner', 'team_name',
             'points', 'projPoints', 'FP_Points', 'BOL_Points', 'PINNY_Points', 'TRUE_Points']]
    
    df['TRUE_Points'] = df['TRUE_Points'].fillna(df['projPoints'])

    result = df.groupby(['week', 'team_owner', 'team_name'], as_index=False).agg({
        'points': 'sum',
        'projPoints': 'sum',
        'FP_Points': 'sum',
        'BOL_Points': 'sum',
        'PINNY_Points': 'sum',
        'TRUE_Points': 'sum'
        })
    
    result['point_diff'] = result['TRUE_Points'] - result['projPoints']

    return result.sort_values(by='TRUE_Points', ascending=False)


def get_rankings(pos, week, lu, primary_owner=None, visualize=False, check_fa=False):
    """Rank players at the given positions for a week, by blended projection.

    Args:
        pos: Positions to include, e.g. ``['RB', 'WR', 'TE']``.
        week: Week to rank.
        lu: Combined lineup frame from :func:`clean_lineups`. Previously read
            from a module-level ``LINEUPS`` global.
        primary_owner: Team owner to keep alongside free agents when
            ``check_fa`` is set. Previously read from
            ``lg_vars[select_league]['primary_own']``.
        visualize: Retained for call compatibility; the upstream function has
            no visualisation branch and returns ``None`` when this is ``True``.
        check_fa: Restrict the result to ``primary_owner`` plus free agents.

    Returns:
        pd.DataFrame: Ranked players, or ``None`` when ``visualize`` is ``True``.
    """
    df = lu[(lu['primaryPosition'].isin(pos)) & (lu['week'] == week)]
    df = df[['week', 'primaryPosition','player_name', 'team_owner', 'team_name',
                  'points', 'projPoints', 'FP_Points', 'BOL_Points', 'PINNY_Points', 'TRUE_Points',
                  'PosRank', 'ESPN_PosRank', 'FP_PosRank', 'BOL_PosRank', 'PINNY_PosRank', 'TRUE_PosRank']]
    df = df.drop(columns=['points', 'PosRank']).sort_values(by=['TRUE_Points'], ascending=False)

    if visualize == False:
        if check_fa == True:
            return df[df['team_owner'].isin([primary_owner, 'Free Agent'])]
        else:
            return df
