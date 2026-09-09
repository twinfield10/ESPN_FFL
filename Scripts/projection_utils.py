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

import time
import warnings
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from Scripts import market as mk
from Scripts.paths import NFL_TACKLES_CSV, resolve, season_dir
from Scripts.scoring import get_scoring_table
from Scripts.scrape_player_stats import (
    DERIVED_STATS,
    FREE_AGENT_OWNER,
    SLOT_BASE,
    SLOT_DST,
    VOLUME_STATS,
)


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


def usage_weekly_parquet(season: int):
    """Season's weekly TOMCAT projections file.

    Nothing writes this yet. ``docs/plans/19-weekly-usage-model.md`` is the plan for
    the head that would, and the path is named here so that when it lands the wiring
    below is already in place -- see :func:`clean_usage_weekly`.
    """
    return season_dir("Usage", season, "Usage_WeeklyProjections.parquet",
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


#: How old a projection source's file may be before the blend says so. The nightly
#: writes daily, so one missed run is not an emergency and two is. Deliberately not
#: an error: a stale source is still a real line, and dropping it would renormalise
#: the remaining books upward, which is worse than using yesterday's number.
STALE_AFTER_HOURS = 48.0


class StaleProjectionSourceWarning(UserWarning):
    """A projection source's file is older than :data:`STALE_AFTER_HOURS`."""


def _warn_stale(msg: str) -> None:
    """Warn about a stale source, past the global filter. See :func:`_warn_missing`."""
    with warnings.catch_warnings():
        warnings.simplefilter("always", StaleProjectionSourceWarning)
        warnings.warn(msg, StaleProjectionSourceWarning, stacklevel=3)


def source_age_hours(path) -> Optional[float]:
    """How long ago a source file was written.

    Args:
        path: Path to the source file.

    Returns:
        float | None: Age in hours, or None when the file does not exist.
    """
    path = Path(path)
    if not path.is_file():
        return None
    return (time.time() - path.stat().st_mtime) / 3600.0


def check_source_freshness(label: str, path, fix_hint: str,
                           max_age_hours: float = STALE_AFTER_HOURS) -> Optional[float]:
    """Warn if a source's file is stale, and say how to refresh it.

    The loaders only ever checked whether a file *existed*, so a thirteen-day-old
    book read exactly like one written this morning -- which is how both books sat
    stale on the 2026 draft board while every other source refreshed nightly. An
    equal-vote blend makes that a stale opinion rather than a missing column, and a
    stale opinion is invisible by construction. See ``docs/plans/36-sportsbook-scrapes.md``.

    Args:
        label: Human-readable source name, used in the warning.
        path: Path to the source file.
        fix_hint: The command that refreshes it.
        max_age_hours: Age past which to warn.

    Returns:
        float | None: The file's age in hours, or None when it does not exist.
    """
    age = source_age_hours(path)
    if age is not None and age > max_age_hours:
        _warn_stale(
            f"{label} is {age / 24:.1f} days old ({Path(path).name}); "
            f"the nightly refresh writes daily. Run `{fix_hint}`."
        )
    return age


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


#: The weekly sources, and where each one's file lives.
WEEKLY_SOURCE_FILES = {
    "fantasypros": fantasypros_parquet,
    "pinnacle": pinnacle_parquet,
    "betonline": betonline_parquet,
    # TOMCAT is deliberately absent. This dict drives the app's "no weekly props
    # this season for X" caption, and TOMCAT is not a props feed that went quiet --
    # it is a head nobody has built (`docs/plans/19-weekly-usage-model.md`). Listing
    # it would put a model in a sentence about sportsbooks and make the key set
    # unstable for every consumer. It joins here when it ships; see
    # `clean_usage_weekly`.
}


def weekly_source_status(season: int, week=None) -> Dict[str, Dict]:
    """Per weekly source: whether there is a file, how old it is, and what it holds.

    The detail behind :func:`weekly_sources_present`, kept separate so a caller that
    wants to say *why* a source is absent can, without the boolean growing a shape
    every reader has to handle.

    Args:
        season: Season year.
        week: Week to count rows for. None counts the whole file.

    Returns:
        dict: source name to ``{exists, age_hours, stale, rows, usable}``.
    """
    out: Dict[str, Dict] = {}
    for name, resolver in WEEKLY_SOURCE_FILES.items():
        path = resolver(season)
        age = source_age_hours(path)
        entry = {"exists": age is not None, "age_hours": age,
                 "stale": age is not None and age > STALE_AFTER_HOURS,
                 "rows": None}
        if age is not None:
            try:
                frame = pd.read_parquet(path, columns=["week"])
                if week is None:
                    entry["rows"] = int(len(frame))
                else:
                    matched = frame["week"].astype(str) == str(week)
                    entry["rows"] = int(matched.sum())
            except Exception:                                  # noqa: BLE001
                # A file that cannot be read is not a source. Reporting it as
                # unusable is the honest answer and is what the caller acts on;
                # raising here would take a store write down over metadata.
                entry["rows"] = 0
        entry["usable"] = bool(entry["exists"] and not entry["stale"]
                               and (entry["rows"] or 0) > 0)
        out[name] = entry
    return out


def weekly_sources_present(season: int, week=None) -> Dict[str, bool]:
    """Which weekly projection sources have a *usable* file.

    Recorded in the store's ``meta.json`` so the app can show a degraded source
    rather than rendering an ESPN-only number that looks like a four-source
    blend.

    Present means all three of: the file exists, it is younger than
    :data:`STALE_AFTER_HOURS`, and it carries at least one row for ``week``.

    **It used to mean existence alone, and that reported a dead source as live.** On
    2026-09-08 this returned ``fantasypros: True`` off a file holding **60 rows for
    week 1, written 2026-08-03** -- the anonymous ten-per-position teaser, 25 days
    stale, scraped three weeks before the account that lifts the fence existed, for a
    week that had not been played. Every consumer of this dict -- the weekly table's
    column list, the sidebar's absent-source caption, ``real_sources`` -- was told
    FantasyPros was fine. An absent source reading as agreement is this repo's oldest
    failure mode (``docs/plans/03-projection-source-coverage.md``) and this function
    was one of the guards against it.

    Args:
        season: Season year.
        week: Week the store is being built for. None means "any week", which
            preserves the old meaning for a caller that does not know the week --
            ``Scripts.refresh`` passes None on a run with no live league.

    Returns:
        dict: ``{"fantasypros": bool, "pinnacle": bool, "betonline": bool}``.
    """
    return {name: entry["usable"]
            for name, entry in weekly_source_status(season, week=week).items()}


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

#: Columns that carry a source's prefix but are not one of its lines.
#:
#: ``<SOURCE>_Points`` and ``<SOURCE>_PosRank`` are computed *from* a stat line by
#: :func:`proj_to_score`, so they have no ``MEAN_`` counterpart to be imputed from
#: and therefore no ``_is_imputed`` companion -- which drops them into
#: :func:`coverage_report`'s ``notna()`` branch, where a derived number that is
#: always populated reads as 100% real.
#:
#: Measured on the 2026 weekly stores on 2026-09-08: the sidebar reported
#: **Pinnacle 4.1% and BetOnline 4.1% for two sources with no weekly line at all**,
#: and 4.1% is exactly 2/49 columns -- these two. Every genuine PINNY and BOL stat
#: column was 0.0%. The panel that exists to stop an absent source reading as
#: agreement was doing precisely that, which is this repo's oldest failure mode
#: (``docs/plans/03-projection-source-coverage.md``).
#:
#: :func:`present_prefixes` had excluded the same two names by literal since plan 34.
#: They are named once here so the two lists cannot drift apart.
DERIVED_SOURCE_COLUMNS: Tuple[str, ...] = ("Points", "PosRank")

#: Working name for ESPN's own published point total inside ``clean_lineups``.
#:
#: It exists because :func:`change_col_prefix` replaces the *substring* ``proj``
#: rather than a prefix, so ``projPoints`` would become ``ESPNPoints`` on the way
#: past. Any replacement name therefore has to contain no ``proj`` of its own --
#: ``espn_projected_points`` would be mangled to ``espn_ESPNected_points``.
ESPN_PUBLISHED_POINTS = "espn_published_points"

#: Blend weights per stat, with a ``default`` fallback. Applied only across the
#: sources that have *real* data for a given row -- see :func:`compute_weighted_stats`.
#: Lifted to module scope so the weights can be inspected and tested without running
#: the whole pipeline. Only meaningful in proportion, since renormalisation divides by
#: whatever subset is real, so a set summing to 1.0 is conventional rather than
#: required.
#:
#: **An equal split across the four external sources that can speak** -- ESPN,
#: FantasyPros, Pinnacle and BetOnline, plus The Athletic. Set three-way on
#: 2026-08-07, widened to include the usage model on 2026-08-17, and **narrowed again
#: on 2026-09-07 when TOMCAT was withdrawn from the season-long blend.** This is an
#: owner decision, not a fitted result, and its parts deserve separate notes.
#:
#: **On ``USG`` being removed, 2026-09-07.** It carried an equal vote for three weeks
#: on the strength of its own backtest -- it beats the naive draft heuristic on every
#: metric at every position, out of sample, in 26 of 28 season-position cells. That
#: result stands and the model is not being retired; what it does not establish is
#: that the model's *level* is fit to blend, and a measurement against the board says
#: it is not.
#:
#: The disqualifying number is a team total. An NFL team ran the ball a median 454,
#: 450 and 465 times in 2023, 2024 and 2025. For 2026 ESPN projects 462 per team, The
#: Athletic 452 and the ESPN/FP/ATH mean 466; **TOMCAT projects 384**, or 417 once its
#: abstentions are filled from the field. On the players it and the field both price it
#: runs 0.903 of the field's carries and 0.891 of its pass attempts, while receiving
#: targets come in at 0.999. A team's carry count is not a matter of opinion, so this
#: is a level error rather than a disagreement -- and it survives to the board because
#: :data:`Scripts.usage.coherence.IDENTITIES` holds three passing/receiving pairs and
#: **no rushing identity**, so nothing constrains a team's carries the way the
#: passing side is constrained.
#:
#: What that cost the blend: on the draftable pool the model read 0.836 of ESPN at ADP
#: 1-50 and 1.089 at 150+, stable across all nine leagues, so it discounted precisely
#: the picks the board exists to get right. Rebuilt with and without it, including it
#: moved skill-position levels to 0.975-0.990 and **D/ST to 0.879** -- 12.5% of
#: cross-position distortion, nearly all of it the defence arm, whose ordering
#: correlates with the field at a Spearman of only 0.168.
#:
#: **The weekly path is a separate question and is not foreclosed.** ``USG`` has never
#: been in :data:`WEEKLY_PREFIXES` -- TOMCAT has no weekly head -- so nothing about
#: this removal touches it, and a weekly arm fitted on in-season usage would be judged
#: on its own evidence. See ``docs/plans/43-tomcat-out-of-season-blend.md``.
#:
#: **The model keeps running and its columns stay in the store.** ``USG_`` stat lines
#: are still merged onto the board so the source atlas can keep measuring them and so
#: the decision is reversible; they simply carry no weight, are no longer scored into
#: ``USG_Points``, and no longer appear on the draft board. The lower-case ``usg_*``
#: diagnostics -- ``usg_depth_rank``, ``usg_role_cohort`` -- are depth-chart facts
#: rather than projections and remain load-bearing for the injury vacancy transfer and
#: :func:`Scripts.season_projections._withdraw_usage_on_role`.
#:
#: **On BetOnline coming back off zero, 2026-08-17.** It went to zero alongside
#: Pinnacle and that was the wrong half to drop. BetOnline's season endpoint works
#: and resolves **273 players with 13 stat columns including IDP tackles and sacks**,
#: against FantasyPros' 60 -- only the *weekly* endpoint is blocked, on a different
#: host which never fed this path. Meanwhile the nominal split is never the realised
#: one: FantasyPros is **5.8% real** on the 2026 board, so with TOMCAT withdrawn
#: ``TRUE_`` is ``ESPN`` alone on most rows and ``(ESPN + BOL)/2`` or
#: ``(ESPN + ATH)/2`` where a second source actually has a line.
#:
#: That widening was **additive rather than a re-tune**, which is what made it safe to
#: ship beside a change to the usage basis. Because :func:`compute_weighted_stats`
#: renormalises over the sources that are *real*, a player with no BetOnline line kept
#: exactly the split he had before -- byte-identical output. The weight could only bite
#: on the 273 players BetOnline actually covers. **Removing ``USG`` is not additive and
#: was never going to be**: it moves every row the model spoke for, which is the point.
#:
#: **Pinnacle was at zero until 2026-08-24 and is now an equal quarter like the rest.**
#: The reasoning for zeroing it -- 76 props, offence only, 94-100% imputed on the board --
#: described its *coverage*, and coverage is already handled one layer down: an imputed cell
#: is flagged and its weight dropped, so a source with no line for a player cannot vote on
#: him whatever its nominal weight says. Zeroing it on top of that was the same objection
#: applied twice, and it silenced Pinnacle on the 21-30 players a week where it does have a
#: real line.
#:
#: `USG` is **TOMCAT** -- Touches, Opportunity, Market, Context, Availability, Tiers --
#: this repo's own model, and `KIK`/`DST` are two of its three arms rather than separate
#: sources. The prefix stayed `USG_` when the name landed on 2026-08-24; see
#: `Scripts/usage/__init__.py`. **It is absent from this table as of 2026-09-07** -- not
#: at 0.0, but absent, so that a reader counting entries counts the sources that vote.
#: Kickers and team defences lose their only second opinion with it and fall back to
#: ESPN plus whatever FantasyPros has; that is a real cost of the removal and it is
#: recorded rather than smoothed over.
#:
#: The rule these nominal weights encode is **one equal vote per source that actually has
#: an opinion**. Because every universal source carries the same 0.25, renormalisation makes
#: that literal: four real sources weight 0.25 each, three weight 0.333, two weight 0.5.
#: Measured on the 2026 board's draftable receiving lines before TOMCAT was withdrawn --
#: 29 rows carried five real sources, 67 carried four, 160 carried three, 102 carried
#: two. The nominal number is therefore almost never the realised one, and that is the
#: design rather than a defect.
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

#: Prefixes the weekly path may score, in the order the pipeline builds them.
#:
#: This list and ``proj_to_score``'s default are now the same set of external
#: sources plus ``MEAN`` and ``TRUE``, and they arrived there from opposite
#: directions. ``USG`` was struck from the *weekly* list because TOMCAT has no
#: weekly head (``docs/plans/19-weekly-usage-model.md`` is not started) and the
#: kicking and defence arms are season-long, so scoring it weekly wrote a column
#: that was null for every row of every store -- null 3,602 of 3,602 times on
#: Knights_FFL 2025. It was struck from the *season* list on 2026-09-07 for an
#: unrelated reason: the model was withdrawn from the blend, so pricing its line
#: would publish a ``USG_Points`` column nothing consumes. See :data:`WEIGHTS`.
#:
#: The weekly exclusion was never cosmetic and still is not. ``_apply_scoring``
#: writes ``NaN`` for a source that projected nothing *deliberately*, to
#: distinguish it from a source projecting zero -- so an all-NaN column is
#: indistinguishable from a source that had an opinion and could not be reached,
#: and every coverage count built on ``notna()`` reads it the same way.
#:
#: **``USG`` joined this tuple on 2026-09-08 without joining the blend**, which is
#: only safe because ``present_prefixes`` exists. It carries no weekly stat columns
#: until ``docs/plans/19-weekly-usage-model.md`` ships a head that writes some, and
#: until then the prefix is filtered out before ``proj_to_score`` runs, so no
#: all-null ``USG_Points`` is published. It is listed rather than omitted so the
#: registration is one line of data rather than a change to this file, and it is
#: **not** in :data:`WEIGHTS`: that dict is shared with the season path, where TOMCAT
#: was withdrawn on 2026-09-07, so an entry there would re-admit it to the draft
#: board as a side effect. See :func:`clean_usage_weekly`.
WEEKLY_PREFIXES: Tuple[str, ...] = ("ESPN", "FP", "MEAN", "PINNY", "BOL", "USG",
                                    "TRUE")


def present_prefixes(df, candidates=WEEKLY_PREFIXES) -> list:
    """The candidates that actually have a stat column on this frame.

    Args:
        df: Frame to inspect.
        candidates: Prefixes to consider, without their underscores.

    Returns:
        list: Candidates with at least one ``<prefix>_<stat>`` column, in the
        order given. A prefix whose only column is ``<prefix>_Points`` does not
        count -- that is the output, not a line.
    """
    out = []
    for prefix in candidates:
        start = f"{prefix}_"
        if any(c.startswith(start) and not c.endswith(IMPUTED_SUFFIX)
               and c[len(start):] not in DERIVED_SOURCE_COLUMNS
               for c in df.columns):
            out.append(prefix)
    return out



WEIGHTS = {
    # `TOMCAT` was one source with three backends -- the usage arm, kicking and team
    # defence, all writing `USG_` and carrying one vote between them. **It was
    # withdrawn from the season-long blend on 2026-09-07** and there is no entry for
    # it here. The measurement that removed it is in the module docstring above and
    # the decision is written up in
    # docs/plans/43-tomcat-out-of-season-blend.md.
    #
    # Two things it took with it, recorded because they are costs rather than
    # tidy-ups. **Kickers and team defences lose their only second opinion**: the
    # kicking arm was deliberately switched on over an unpassed G-K2 gate precisely
    # because the alternative was a starting slot in nine leagues at 100% ESPN, and
    # that alternative is now what we have (docs/plans/29-kicker-model.md). And
    # **`USG_receivingTargets` was the blend's third opinion on volume** -- FantasyPros
    # publishes no target column and neither book prices one, so targets are back to
    # ESPN and The Athletic alone.
    #
    # **Position scoping used to fall out of the flags rather than the weights**, and
    # that mechanism is unchanged for the sources that remain: a player a source has
    # no line for arrives null and flagged, `compute_weighted_stats` drops the weight
    # and the rest renormalise.
    #
    # `ATH` -- The Athletic (Jake Ciely's workbook) -- registered at 0.25 on
    # 2026-09-01, as an equal vote. It covers 434 offensive players with a raw
    # stat line and abstains on kickers and defences, so on those rows the weight is
    # dropped and the rest renormalise as usual.
    #
    # Registered straight to 0.25 rather than shipping dark at 0.0 first, which is
    # what the TOMCAT arms each did. Two things worth writing down about that:
    # it moves `TRUE_Points` for every player the source covers, and it was merged
    # five days before the GOP auction, which is the week docs/DRAFT_READINESS.md
    # asks to be left alone. The out-of-sample MAE measurement that plan 20 asks for
    # is therefore owed *after* the fact rather than before -- see
    # docs/plans/38-the-athletic.md.
    'default': {'ESPN': 0.25, 'FP': 0.25, 'PINNY': 0.25, 'BOL': 0.25, 'ATH': 0.25},
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


def clean_usage_weekly(usage_path=None, season=None):
    """Load TOMCAT's weekly stat lines, if a weekly head has ever written any.

    **Today this always returns the empty frame, and that is the point.** It is the
    seam ``docs/plans/19-weekly-usage-model.md`` step 5 asks for -- "loader +
    ``WEIGHTS`` entry + ``proj_to_score`` prefix, following the ``clean_pinny`` /
    ``clean_bol`` pattern including its absent-source path" -- built now, while the
    surrounding code is being touched anyway, so that shipping a weekly arm is a data
    change rather than a plumbing change.

    Nothing null reaches the store while it is empty. With no file there are no
    ``USG_`` columns on the frame, so :func:`present_prefixes` drops the prefix
    before :func:`proj_to_score` ever sees it. That function exists for exactly this:
    plan 34 added it after ``USG_Points`` was written null for all 3,602 rows of
    every 2025 weekly store, because a column shaped like a source that never has an
    opinion reads as a source that agreed.

    **``USG`` is deliberately not in :data:`WEIGHTS`, and adding it is not this
    function's job.** That dict is shared by both grains, so a weekly entry would
    also re-admit TOMCAT to the season board it was withdrawn from on 2026-09-07 for
    a measured level error (see this module's ``WEIGHTS`` docstring and
    ``docs/plans/43-tomcat-out-of-season-blend.md``). Turning the weekly arm on is one
    deliberate line, taken on in-season evidence, against plan 03's pre-registered
    bar: a crude weekly head at an equal vote cost **+16.91%** MAE over all rostered
    player-weeks and **+1.55%** on players who actually took a snap, so roughly nine
    tenths of the harm was availability rather than accuracy -- which is why plan 19
    builds the availability head first.

    Args:
        usage_path: Explicit parquet location. Takes precedence over ``season``.
        season: Season to load. Required unless ``usage_path`` is given.

    Returns:
        pd.DataFrame: Weekly ``proj_<stat>`` lines keyed on
        :data:`SOURCE_JOIN_KEYS`, or the empty frame from
        :func:`absent_weekly_source` when no weekly head has written one.

    Raises:
        FileNotFoundError: When an explicit ``usage_path`` does not exist. A named
            file that is missing is a typo, not an absent season.
    """
    if usage_path is not None:
        usage_path = resolve(usage_path)
    elif season is not None:
        usage_path = usage_weekly_parquet(season)
        if not usage_path.exists():
            return absent_weekly_source("TOMCAT weekly", usage_path)
        check_source_freshness("TOMCAT weekly projections", usage_path,
                               "python -m Scripts.usage.weekly")
    else:
        raise ValueError("clean_usage_weekly requires either usage_path or season")

    return pd.read_parquet(usage_path)


def coverage_report(df, sources=('ESPN', 'FP', 'PINNY', 'BOL', 'ATH'),
                    stats=None):
    """Per-source share of cells that are real rather than imputed.

    Plan 03 step 4: a source quietly degrading should be visible, not absorbed by
    imputation. ``ESPN`` is the root source and is never imputed, so it reports
    100% wherever it has a column.

    Args:
        df: Blended frame carrying ``*_is_imputed`` columns.
        sources: Source prefixes to report on.
        stats: Restrict to these stat names. Defaults to every stat found. Two
            kinds of column are never a stat and are excluded whatever ``stats``
            says. A market-implied dispersion column (``<stat>_sd``) has no
            ``MEAN_`` counterpart to be imputed from, so counting it would drag a
            source's coverage average toward whatever share of its columns happen
            to carry one. :data:`DERIVED_SOURCE_COLUMNS` -- ``_Points`` and
            ``_PosRank`` -- are worse: they are always populated *and* have no
            provenance flag, so they read 100% and were the entire reported
            coverage of two sources with no weekly line at all.

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
            and not c.endswith(mk.SD_SUFFIX)
            and c[len(prefix):] not in DERIVED_SOURCE_COLUMNS
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


def source_contributed(df, prefix, stats, *, points_fallback=True,
                       missing_flag_is_imputed=True, zero_is_real=False):
    """Per row: did ``prefix`` supply at least one real cell to a scored stat?

    Extracted from :func:`Scripts.season_projections.attach_source_spread` so the
    store, the board and the app answer this one question the same way. Two clauses
    carry the whole meaning, and both were learned from a defect:

    **A zero does not count.** These frames are dense with structural zeros -- a
    kicker's ``FP_passingYards`` is 0.0 and unflagged, because nobody imputed it and
    nobody asserted it either. Counting those made FantasyPros a real source for
    Cameron Dicker on the strength of twelve zeros, and his floor and ceiling came
    back exactly equal to ESPN's total: a spread of zero, reported as measured
    agreement.

    **An imputed cell does not count.** That is what the flags are for. ESPN carries
    no flags at all -- it is the source every other one is imputed *from* -- so it
    counts wherever it has a non-zero number.

    Args:
        df: Frame carrying ``<prefix>_<stat>`` columns and their ``_is_imputed``
            companions.
        prefix: Source prefix, without the underscore.
        stats: Stat names to consider. Scored stats for a points question; the
            blended stats for a coverage question.
        points_fallback: When ``prefix`` has no stat column on the frame at all, fall
            back to ``<prefix>_Points`` being non-null. This is what
            ``app.lineup.real_sources`` already does at league level, and it is what
            keeps this usable on the points-only frames the tests and the Sheets
            renderer pass around -- ``tests/test_lineup.py``'s ``one_real_source``
            and ``two_real_sources`` fixtures carry no stat columns whatever.
        zero_is_real: Whether a published **0.0** counts as a line. False -- the
            default and what every existing caller wants -- is the Cameron Dicker
            rule above. True is for the root source only, and the distinction is
            not fussiness either:

            ESPN publishes ``0.0`` for a player who is inactive or on a bye, and
            that is an assertion, not an absence. Counting it as absence made the
            sidebar read **ESPN 92-99.5%** across the ten 2026 stores when ESPN is
            the frame every row came from -- 11 of GOP Degenerates' 311 scoped
            players, every one of them ``player_active_status`` ``bye`` or
            ``inactive``. :func:`coverage_report` had always answered 100% for
            ESPN, so the two coverage views disagreed about the one source that
            cannot be missing. A structural zero and a published zero are different
            facts, and only the root source can tell you which it has.
        missing_flag_is_imputed: What a **NaN** provenance flag means. True -- a row
            that never joined counts as imputed -- matches
            :func:`compute_weighted_stats` and :func:`coverage_report`.
            ``attach_source_spread`` has always read it the other way and passes
            False to keep its published output identical. The disagreement is latent
            rather than live: measured 2026-09-08, **zero** flag cells are NaN across
            all ten leagues' ``lineups.parquet`` and ``board.parquet``, so no shipped
            number depends on which way it is read.

    Returns:
        pd.Series: Boolean, indexed like ``df``.
    """
    contributed = pd.Series(False, index=df.index)
    seen_stat_column = False

    for stat in stats:
        stat_col = f"{prefix}_{stat}"
        if stat_col not in df.columns:
            continue
        seen_stat_column = True
        values = pd.to_numeric(df[stat_col], errors="coerce")
        has_value = values.notna() if zero_is_real else (values.notna()
                                                         & (values != 0))
        flag_col = stat_col + IMPUTED_SUFFIX
        if flag_col in df.columns:
            imputed = df[flag_col].fillna(missing_flag_is_imputed).astype(bool)
            has_value &= ~imputed
        contributed |= has_value

    if not seen_stat_column and points_fallback:
        points_col = f"{prefix}_Points"
        if points_col in df.columns:
            return df[points_col].notna()

    return contributed


def player_coverage(df, sources=('ESPN', 'FP', 'PINNY', 'BOL', 'ATH'), stats=None,
                    root='ESPN'):
    """Per source: the share of *players* it really has a line for.

    The companion to :func:`coverage_report`, which asks the same question of cells
    and answers it in a way that does not survive being read off a sidebar. Averaging
    ``real_pct`` over every column a source carries divides by 45-odd stats, most of
    which that source structurally never publishes -- so on the 2026 weekly stores
    FantasyPros read **12.4%** when it had a real line for **21.8%** of the players
    the owner can actually start, and the two numbers are not measuring the same
    thing. A denominator of players wants a numerator of players.

    Args:
        df: Frame with one row per player, or per player-week.
        sources: Source prefixes to report on.
        stats: Stat names a source can contribute to. Defaults to every stat with a
            ``TRUE_<stat>`` column -- the blend's own definition of a stat that
            matters, which needs no scoring table -- falling back to the union of
            the requested sources' own stat columns on a frame that carries no
            blend yet. Without that fallback a frame holding ``ESPN_rushingYards``
            and no ``TRUE_rushingYards`` reported 0% for every source, which is the
            shape ``tests/test_store.py``'s fixture has.
            :data:`DERIVED_SOURCE_COLUMNS` are removed either way: leaving them in
            is what made a first draft of this function report 100% for all four
            sources, for exactly the reason those two names exist.
        root: The source every other one is imputed *from*, measured with
            ``zero_is_real=True``. **It is the frame's own author, so it cannot be
            missing a player, and reporting that it was is the panel lying about
            the one row a reader uses as the baseline.** ESPN publishes ``0.0`` for
            an inactive or bye player, the non-zero rule read that as absence, and
            the sidebar showed ESPN at 92-99.5% across the ten 2026 stores on
            2026-09-09 -- 96.5% on GOP Degenerates, whose 11 uncounted players were
            four on a bye and seven inactive. Pass ``None`` to measure every source
            the same way.

    Returns:
        pd.DataFrame: Columns ``source``, ``players``, ``real``, ``real_pct``, sorted
        worst-covered first. Empty with those columns when there is nothing to
        measure, matching :func:`coverage_report` rather than raising.

    Note:
        The root's number is a constant 100% by construction, which is the honest
        answer rather than an uninformative one: it is what the other sources are
        read against, and a panel built so that "a dead source cannot hide" needs a
        baseline that cannot move for reasons unrelated to a source dying.
    """
    columns = ["source", "players", "real", "real_pct"]
    if df is None or not len(df):
        return pd.DataFrame(columns=columns)

    if stats is None:
        stats = [c[len("TRUE_"):] for c in df.columns if c.startswith("TRUE_")]
        stats = [s for s in stats if s not in DERIVED_SOURCE_COLUMNS]
        if not stats:
            found = []
            for source in sources:
                prefix = f"{source}_"
                found += [c[len(prefix):] for c in df.columns
                          if c.startswith(prefix)
                          and not c.endswith(IMPUTED_SUFFIX)
                          and not c.endswith(mk.SD_SUFFIX)]
            stats = list(dict.fromkeys(found))
    stats = [s for s in stats if s not in DERIVED_SOURCE_COLUMNS]

    rows = []
    total = len(df)
    for source in sources:
        prefix = f"{source}_"
        if not any(c.startswith(prefix) for c in df.columns):
            continue
        real = int(source_contributed(df, source, stats,
                                      zero_is_real=(source == root)).sum())
        rows.append({"source": source, "players": total, "real": real,
                     "real_pct": round(100.0 * real / total, 1) if total else 0.0})

    if not rows:
        return pd.DataFrame(columns=columns)
    return (pd.DataFrame(rows)
            .sort_values(["real_pct", "source"])
            .reset_index(drop=True))


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
    sources = [s for s in ("ESPN", "FP", "PINNY", "BOL", "ATH")
               if s in overall.index]

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


def align_to_espn_names(source, espn_names, label, keys=SOURCE_JOIN_KEYS):
    """Rewrite a weekly source's ``player_name`` to ESPN's spelling of that player.

    **The weekly path joins on the raw name string, and this is what makes that
    safe.** ``clean_lineups`` merges every source ``on=['week', 'player_name']``,
    so ``James Cook`` against ESPN's ``James Cook III`` is not a near miss, it is a
    miss: the player abstains, ``impute_columns`` fills his line from the ESPN/FP
    mean, and the board shows a book agreeing with ESPN about a player it never
    priced. The season path has never had this problem because it keys on
    :func:`Scripts.season_projections.normalise_name`.

    The fix that had accreted instead was a hand-maintained
    ``name_changes`` dict inside ``clean_pinny`` and ``clean_bol``, one per source,
    each mapping that source's spellings to ESPN's. Audited on 2026-09-09 by
    ``python -m Scripts.name_audit --maps``, **20 of the 22 entries were pure
    suffix or punctuation differences that ``normalise_name`` already collapses**,
    the remaining two are now in :data:`Scripts.season_projections.NAME_ALIASES`,
    and **two were pointing at spellings ESPN had stopped using** -- so they
    created the miss they were written to fix. ``Deebo Samuel`` (9.1 projected
    points that week) and ``Oronde Gadsden`` (4.8) were both being renamed *away*
    from ESPN's current name. FantasyPros had no map at all and lost Gadsden too.

    One rule over three sources instead, derived from the ESPN frame at build time,
    so a suffix ESPN adds mid-season cannot leave a map stale.

    Args:
        source: A weekly source frame carrying ``player_name``. Returned unchanged
            when it is empty or has no such column -- an absent source is not an
            error here, see :func:`absent_weekly_source`.
        espn_names: The ESPN spellings to align onto. Any iterable of names; the
            league's own lineup frame in practice.
        label: Source name, for the messages.
        keys: The columns the caller will merge on. Duplicates across these are
            dropped after aligning, because that is what alignment can create --
            FantasyPros' weekly file carries ``Mitch Tinsley`` *and* ``Mitchell
            Tinsley`` as separate rows, and left alone they would each match the
            one ESPN row and double it.

    Returns:
        pd.DataFrame: A copy with ``player_name`` rewritten where a match was found.

    Note:
        **An ambiguous key is left alone rather than guessed.** Stripping suffixes
        collapses five pairs of genuinely different 2026 players onto one key --
        ``Byron Murphy II`` the tackle and ``Byron Murphy Jr.`` the cornerback,
        ``Michael Carter`` and ``Michael Carter II``, and three more. Picking either
        would attach a real line to the wrong player, which is worse than the
        abstention this function exists to remove.
    """
    from Scripts.season_projections import normalise_name

    if source is None or "player_name" not in getattr(source, "columns", []):
        return source
    if not len(source):
        return source

    candidates = {}
    for name in espn_names:
        key = normalise_name(name)
        if key:
            candidates.setdefault(key, set()).add(name)
    lookup = {k: next(iter(v)) for k, v in candidates.items() if len(v) == 1}
    ambiguous = {k for k, v in candidates.items() if len(v) > 1}

    out = source.copy()
    espn_set = set(lookup.values())

    def _align(name):
        if not isinstance(name, str) or name in espn_set:
            return name
        return lookup.get(normalise_name(name), name)

    before = out["player_name"]
    out["player_name"] = before.map(_align)
    renamed = int((out["player_name"] != before).sum())

    hit_ambiguous = sorted({n for n in before
                            if isinstance(n, str) and n not in espn_set
                            and normalise_name(n) in ambiguous})

    merge_keys = [c for c in keys if c in out.columns]
    dropped = 0
    if merge_keys:
        rows = len(out)
        out = out.drop_duplicates(subset=merge_keys, keep="first")
        dropped = rows - len(out)

    if renamed or dropped or hit_ambiguous:
        detail = [f"{label}: aligned {renamed} name(s) to ESPN spellings"]
        if dropped:
            detail.append(f"dropped {dropped} row(s) that collided after aligning")
        if hit_ambiguous:
            detail.append("left ambiguous: " + ", ".join(hit_ambiguous[:4]))
        print("  " + "; ".join(detail) + ".")
    return out.reset_index(drop=True)


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
        This function loads and renames; it does not derive. The pivot, TD-split,
        no-vig adjustment and scoring call all happen in
        :mod:`Scripts.scrape_pinnacle`, which writes the wide frame this reads. A
        commented-out copy of that chain used to sit in the body carrying its own
        juice coefficient; it was deleted with
        ``docs/plans/35-market-lines-and-vig.md``.
    """
    if pinny_path is not None:
        pinny_path = resolve(pinny_path)
    elif season is not None:
        pinny_path = pinnacle_parquet(season)
        if not pinny_path.exists():
            return absent_weekly_source("Pinnacle", pinny_path)
        check_source_freshness("Pinnacle weekly props", pinny_path,
                               "python -m Scripts.scrape_pinnacle")
    else:
        raise ValueError("clean_pinny requires either pinny_path or season")

    # Load
    raw=pd.read_parquet(pinny_path)

    # A nine-entry `name_changes` dict used to sit here, mapping Pinnacle's
    # spellings to ESPN's. `align_to_espn_names` replaces it and every other copy:
    # eight of the nine were suffix differences `normalise_name` already collapses,
    # four of those were duplicate keys in the same literal, the ninth
    # (`Zonovan Knight` -> `Bam Knight`) is now in `NAME_ALIASES`, and five of the
    # nine keys no longer appeared in the source file at all. What the map did not
    # carry was `James Cook`, `Luther Burden`, `Kenneth Gainwell` and
    # `Brian Thomas` -- four players Pinnacle priced in week 1 and this loader
    # dropped. See `python -m Scripts.name_audit --maps`.

    # A commented-out copy of the pivot, no-vig, touchdown-split and scoring chain
    # used to sit here, carrying a *third* instance of the juice coefficient at 0.5
    # against the scraper's 0.25. It was inert -- the Pinnacle scraper does this work
    # and writes the wide frame, so this function only reads and renames -- and an
    # inert copy of a formula is worse than no copy, because a reader has to decide
    # which one ships. Deleted with docs/plans/35-market-lines-and-vig.md, which
    # moved the live arithmetic into `Scripts/market.py`.

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
        # This used to name a reason rather than a command, because the scraper could
        # not succeed. It can again: since 2026-09-08 it drives BetOnline's own props
        # widget in a headless browser rather than calling the signed API directly.
        check_source_freshness(
            "BetOnline weekly props", bol_path,
            "python -m Scripts.scrape_BOL --week <week>")
    else:
        raise ValueError("clean_bol requires either bol_path or season")
    raw = pd.read_parquet(bol_path).drop(columns=['team'])

    # A thirteen-entry `name_changes` dict used to sit here. `align_to_espn_names`
    # replaces it, and two of its entries were live defects rather than dead
    # weight: `Deebo Samuel Sr. -> Deebo Samuel` and
    # `Oronde Gadsden -> Oronde Gadsden II` both renamed BetOnline's name *away*
    # from what ESPN now calls the player, so the loader was manufacturing the miss
    # it was written to prevent. `Cameron Ward` and `Zonovan Knight` -- the only two
    # entries doing real work -- are now in `NAME_ALIASES`, where the season path
    # gets them too; it had been missing Cam Ward's Pinnacle line all along.
    # See `python -m Scripts.name_audit --maps`.

    if 'proj_defensiveTotalTackles' in raw.columns:
        tkls = get_tackle_dim() if tackle_dim is None else tackle_dim
        raw = raw.merge(tkls, left_on="position", right_on="pos", how="left")
        raw['proj_defensiveAssistedTackles'] = raw['proj_defensiveTotalTackles'] / (raw['tackle_ratio'] + 0.5)
        raw['proj_defensiveSoloTackles'] = raw['tackle_ratio'] * raw['proj_defensiveAssistedTackles']

    # Join Tackle DataFrame
    raw = raw.drop(columns=['position', 'pos'])

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


#: Yardage stats ESPN sometimes reports at twice their real value.
#:
#: The bug is upstream and long-standing -- see ``docs/STATE_OF_THE_REPO.md`` --
#: and it is a clean factor of two on yardage rather than a general inflation,
#: which is what makes it detectable at all.
DOUBLED_YARDAGE = ("passingYards", "rushingYards", "receivingYards")

#: How far over ESPN's own points total a line must sit before halving is tried.
DOUBLING_MARGIN: float = 1.0

#: How much closer halving must bring it before the halving is kept.
DOUBLING_IMPROVEMENT: float = 0.5


def espn_line_points(df, scoring_df, prefix="ESPN_", halve=()) -> "pd.Series":
    """Score one prefix's stat line, optionally halving some columns.

    A local scorer rather than :func:`_apply_scoring` because it has to price a
    *hypothetical* line -- the one where the yardage is halved -- without writing
    anything to the frame.

    Args:
        df: Frame carrying ``<prefix><stat>`` columns.
        scoring_df: Scoring table with ``colName`` and ``points``.
        prefix: Source prefix, with its underscore.
        halve: Stat names to halve before scoring.

    Returns:
        pd.Series: Points per row, absent columns scoring zero.
    """
    total = pd.Series(0.0, index=df.index)
    halve = set(halve)
    for _, rule in scoring_df.iterrows():
        stat = rule["colName"]
        column = f"{prefix}{stat}" if isinstance(stat, str) else None
        if column is None or column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
        if stat in halve:
            values = values / 2.0
        total = total + values * rule["points"]
    return total


def halve_doubled_espn_yardage(df, scoring_df, projected_points="projPoints"):
    """Reconcile ESPN's stat line to ESPN's own points total, at the stat level.

    **This replaces a test that could not fire for most players.** The previous
    rule was ``ESPN > FantasyPros * 1.75 and ESPN > 40``, evaluated *before* the
    FantasyPros imputation -- so for any player FantasyPros had no line for, the
    comparison was against NaN, came out False, and the doubled value went through
    untouched. FantasyPros served sixty players in 2025 behind a registration
    fence, so the correction was inert for nearly the whole board, and Deebo
    Samuel's week 6 reached the store at 136.3 receiving yards against a
    ``projPoints`` consistent with 68.

    The test here needs no second source. ESPN publishes a stat line **and** a
    point total for the same player-week, so when they disagree the disagreement
    is ESPN's own, and the point total is the authoritative half -- it is the
    number ESPN shows its users. A line is halved only when doing so brings the
    two materially closer, which is what makes this a correction for a known
    factor-of-two rather than a general fit to ESPN's total.

    It fires on 3-8 rows per league-season out of 1,800-4,500, which is the honest
    size of the bug: rare, and previously invisible because the points-space patch
    downstream absorbed it.

    Args:
        df: Frame with ``ESPN_<stat>`` columns and ESPN's own projected points.
            Modified in place.
        scoring_df: The league's scoring table.
        projected_points: Column holding ESPN's own projection.

    Returns:
        tuple: ``(df, rows halved)``. Unchanged with a count of 0 when the
        projected-points column is absent, which is how a frame built without it
        degrades rather than raising.
    """
    if projected_points not in df.columns:
        return df, 0

    published = pd.to_numeric(df[projected_points], errors="coerce")
    current = espn_line_points(df, scoring_df)
    halved = espn_line_points(df, scoring_df, halve=DOUBLED_YARDAGE)

    take = (
        published.notna()
        & (current > published + DOUBLING_MARGIN)
        & ((halved - published).abs() < (current - published).abs()
           - DOUBLING_IMPROVEMENT)
    )
    for stat in DOUBLED_YARDAGE:
        column = f"ESPN_{stat}"
        if column in df.columns:
            df.loc[take, column] = pd.to_numeric(
                df.loc[take, column], errors="coerce") / 2.0
    return df, int(take.sum())


def report_silent_zero_stats(df, scoring_df, prefix="ESPN_"):
    """Scored stats whose column is zero for every row, which is not a projection.

    Plan 01 warns about a scoring rule with no ``colName``. This is the case that
    slips past it: the rule *is* mapped, the column *does* exist, and it is
    uniformly zero all season -- so nothing reports a gap and the points are
    quietly short.

    Found in john_pc_league 2025, which scores six yardage-milestone bonuses
    (``rushingYards100-199Game`` and five siblings, worth 1 to 5 points each). All
    six are zero for all 3,095 player-weeks, in the *actuals* as well as the
    projections. It cost that league a median 0.48 points a row, and the
    points-space patch downstream hid every one of them.

    **The two halves need different things, and only the actuals are a naming
    problem.** A realised 100-yard game is a fact, so a zero there means the key
    read here is not the key ESPN's breakdown uses. The *projection* cannot be
    fixed that way at all: a milestone bonus is a **non-linear** function of the
    stat line -- 1,400 rushing yards buys a different number of 100-yard games
    depending how they are distributed -- and :func:`proj_to_score` can only
    multiply a stat column by a constant. What it wants is a per-game distribution
    counted across the threshold -- ``E[bonus games] = sum over the slate of
    P(that week's yardage lands in the band)`` -- emitted as an expected *count*
    that can then be priced linearly. So it is a **variance** problem before it is
    a mean one, and the weekly dispersion cannot be divided out of the season one
    that :mod:`Scripts.usage.predictive` fits: that module already records
    composing variances failing here, at correlations of +0.48 to +0.63 and
    negative fitted variances for quarterbacks. :mod:`Scripts.dst.model` already
    integrates exactly this shape for ``PA_TIERS``, and
    ``docs/plans/13-dst-from-vegas-lines.md`` prices the error of not doing so at a
    16.5-point compression. Recorded, deliberately not built -- see
    ``docs/plans/34-stat-first-audit.md``.

    Args:
        df: The merged frame.
        scoring_df: The league's scoring table.
        prefix: Source prefix to inspect.

    Returns:
        list: Stat names that are scored, present, and identically zero.
    """
    silent = []
    for _, rule in scoring_df.iterrows():
        stat = rule["colName"]
        if not isinstance(stat, str) or rule["points"] == 0:
            continue
        column = f"{prefix}{stat}"
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
        if len(values) and (values == 0).all():
            silent.append(stat)
    return silent


def blended_stats(scoring_cols) -> list:
    """The stats to blend: everything a league scores, plus volume.

    One definition shared by the weekly path (:func:`clean_lineups`) and the
    season path (:func:`Scripts.season_projections.build_season_projections`),
    because the two computing different stat lists is how the grains drift apart.

    Order is preserved and duplicates are dropped: ``passingCompletions`` and
    ``rushingAttempts`` are scoring rules in some leagues and volume everywhere,
    and blending a stat twice would double its ``TRUE_`` column.

    :data:`DERIVED_STATS` are excluded. A yardage milestone is a non-linear
    function of a line rather than a projection any source makes, so averaging four
    sources' readings of it is meaningless -- and ESPN's own columns for the six are
    identically zero, which would have dragged every blended band to a fraction of
    itself. They are computed *after* the blend instead, by
    :func:`Scripts.season_projections.attach_milestone_bands`.

    Args:
        scoring_cols: ``colName`` values from the league's scoring table. NaN
            entries -- plan 01's unrecognised rules -- are dropped rather than
            becoming a ``TRUE_nan`` column.

    Returns:
        list: Stat names, scoring rules first.
    """
    derived = set(DERIVED_STATS)
    seen, out = set(), []
    for stat in list(scoring_cols) + list(VOLUME_STATS):
        if not isinstance(stat, str) or stat in seen or stat in derived:
            continue
        seen.add(stat)
        out.append(stat)
    return out


def reallocate_book_touchdowns(df, books=mk.TD_ALLOCATION_BOOKS,
                               consensus=mk.TD_CONSENSUS):
    """Re-split each sportsbook's anytime-touchdown total by the consensus ratio.

    A book prices *any* scrimmage touchdown; this pipeline carries a rushing column
    and a receiving one, and until this existed each book invented the split.
    :func:`Scripts.market.allocate_touchdowns` holds the argument and the
    measurements; this is the frame plumbing, and it runs on the blended frame
    rather than in either scraper because that is the first point where ESPN and
    FantasyPros are both present.

    **Worth zero points directly.** All nine leagues score both touchdown types at
    6, so no ``*_Points`` column moves by a cent. It is worth a projection that is
    right about *which* stat, which matters to every consumer that reads a stat
    line rather than a total -- the coherence checks, the outcome simulator, and
    anyone reading a player's row.

    **And it makes per-stat MAE slightly worse**, by design: rushing -0.9%,
    receiving +2.1%, net zero. A weekly receiving-touchdown count is 0 about 95% of
    the time, so MAE is minimised by projecting zero and BetOnline was doing exactly
    that. Judge it on calibration -- see
    :data:`Scripts.lab.accuracy.CALIBRATION_STATS`.

    Args:
        df: Blended frame with ``<source>_rushingTouchdowns`` and
            ``<source>_receivingTouchdowns`` columns. Modified in place.
        books: Sources whose touchdown columns came from one anytime market.
        consensus: Sources that project the two types separately.

    Returns:
        pd.DataFrame: ``df``, with each book's two touchdown columns reallocated
        wherever the consensus states a ratio, and left alone where it does not.
    """
    def _totals(prefixes, stat):
        found = pd.Series(0.0, index=df.index)
        seen = False
        for prefix in prefixes:
            column = f"{prefix}_{stat}"
            if column in df.columns:
                found = found + pd.to_numeric(df[column], errors="coerce").fillna(0.0)
                seen = True
        return found if seen else None

    rushing = _totals(consensus, "rushingTouchdowns")
    receiving = _totals(consensus, "receivingTouchdowns")
    if rushing is None or receiving is None:
        # No consensus source on this frame, so there is no ratio to split by and
        # inventing one would be worse than each book's own guess.
        return df

    for book in books:
        rush_col = f"{book}_rushingTouchdowns"
        rec_col = f"{book}_receivingTouchdowns"
        if rush_col not in df.columns or rec_col not in df.columns:
            continue
        total = (pd.to_numeric(df[rush_col], errors="coerce").fillna(0.0)
                 + pd.to_numeric(df[rec_col], errors="coerce").fillna(0.0))
        split_rush, split_rec = mk.allocate_touchdowns(total, rushing, receiving)
        # Written positionally with `np.where` rather than through `.loc`, because
        # `allocate_touchdowns` returns plain arrays and this frame's index is
        # whatever a chain of merges left behind. NaN marks a row the consensus had
        # no opinion on, and there the book's own split survives.
        keep = np.isfinite(split_rush)
        df[rush_col] = np.where(keep, split_rush, df[rush_col].to_numpy())
        df[rec_col] = np.where(keep, split_rec, df[rec_col].to_numpy())
    return df


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

#: :data:`Scripts.scrape_player_stats.FREE_AGENT_OWNER` is re-exported through this
#: module's import above, because this is where most readers of it look:
#: :func:`coverage_population` needs it, and ``app.session`` and ``app.draft_view``
#: both import it from here.

#: Free agents kept per position when scoping a coverage denominator.
#:
#: :func:`Scripts.scrape_player_stats.build_fa_market` pulls 20-30 per position plus
#: every D/ST, so on a six-team league the pool is 146 rows against 96 rostered ones
#: -- 60% of the frame is players nobody has. Twenty is the owner's number and it is
#: about the depth a waiver claim ever reaches.
COVERAGE_FREE_AGENTS_PER_POSITION = 20


def coverage_population(df, *,
                        free_agents_per_position=COVERAGE_FREE_AGENTS_PER_POSITION,
                        exclude_positions=tuple(IDP_POSITIONS),
                        owner_col="team_owner", position_col="primaryPosition",
                        rank_col="ESPN_Points", group_cols=("week",)):
    """The players a coverage number should be measured over.

    Every rostered player, plus the best ``free_agents_per_position`` free agents at
    each position, minus ``exclude_positions``. The point is that a coverage
    percentage is only meaningful over a population somebody would actually start:
    the free-agent pool is 12-100% of ``lineups.parquet`` depending on the league,
    and measuring FantasyPros against the 30th-best available tight end says nothing
    about whether the projections you read are backed by more than ESPN.

    **IDP positions are excluded unconditionally, and that is not a shortcut.**
    Measured on the 2026 stores, exactly one of ten leagues rosters individual
    defenders -- ``league.free_agents(position='DT')`` returns nothing for the other
    nine, so they carry zero IDP rows and dropping the positions is arithmetically a
    no-op there. In the league that does, the 195 IDP rows have **no** real cell from
    FantasyPros, Pinnacle or BetOnline, so including them measured those sources
    against players they do not publish. The alternative -- probing
    ``meta["starting_slots"]`` for a defensive slot -- would read the one field
    ``app.lineup.slot_counts`` documents as disagreeing with the lineups *for exactly
    that league*, and would not be available here at all: this runs inside
    ``store.build_meta``, where the live league is often ``None``.

    Args:
        df: Any frame with one row per player, or per player-week.
        free_agents_per_position: How many free agents to keep at each position.
        exclude_positions: Positions dropped entirely, before the ranking, so the
            top-N is taken from what survives.
        owner_col: Column separating rostered players from the pool. **When it is
            absent the frame is returned unchanged** -- that is what makes this safe
            to call on a frame that has no notion of ownership.
        position_col: Column holding the player's position.
        rank_col: Column ranked descending to pick the best free agents. Falls back
            to ``projPoints`` then ``TRUE_Points`` then arrival order.
        group_cols: Additional columns the ranking is taken within. ``("week",)``
            because ``lineups.parquet`` gains a week every Tuesday, and a global
            top-20 would apply week 1's twenty to every week after it. Names absent
            from the frame are dropped, which is what lets one function serve both
            grains.

    Returns:
        pd.DataFrame: The scoped rows, index reset. Never raises -- a coverage
        annotation must not be able to take a store write down, the same reason
        :func:`coverage_report` returns an empty typed frame rather than failing.
    """
    if df is None or not len(df):
        return df

    scoped = df
    if position_col in scoped.columns and exclude_positions:
        scoped = scoped[~scoped[position_col].isin(list(exclude_positions))]

    if owner_col not in scoped.columns:
        return scoped.reset_index(drop=True)

    is_fa = scoped[owner_col] == FREE_AGENT_OWNER
    rostered = scoped[~is_fa]
    pool = scoped[is_fa]
    if pool.empty:
        return rostered.reset_index(drop=True)

    keys = [c for c in group_cols if c in pool.columns]
    if position_col in pool.columns:
        keys = keys + [position_col]
    if not keys:
        kept = pool.head(free_agents_per_position)
    else:
        ranked = pool
        for candidate in (rank_col, "projPoints", "TRUE_Points"):
            if candidate and candidate in pool.columns:
                ranked = pool.sort_values(candidate, ascending=False,
                                          na_position="last")
                break
        kept = ranked.groupby(keys, dropna=False, sort=False).head(
            free_agents_per_position)

    return pd.concat([rostered, kept]).reset_index(drop=True)


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
                                                    'BOL', 'ATH', 'TRUE']):
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

    # ESPN's spellings, and the only naming authority on this path: every source
    # below is aligned onto these before it is merged, because the merges key on
    # the raw `player_name` string. See `align_to_espn_names`.
    espn_universe = df['player_name'].dropna().unique()

    # Get Base of Projections (player_name, week, team, etc.)
    base_cols = ['league_id','year','week', 'team_owner', 'team_name', 'team_division', 'player_name', 'player_id', 'slotPosition', 'primaryPosition', 'eligiblePositions', 'pro_team', 'current_team_id' ,'player_position' ,'player_active_status', 'points', 'projPoints']
    scores_df = get_scoring_table(lg)
    actual_scoring_cols = scores_df['colName'].to_list()
    # Volume is blended even where the league does not score it -- see
    # VOLUME_STATS for why. `blend_cols` is what compute_weighted_stats averages;
    # `proj_to_score` still iterates the scoring table, so an unscored TRUE_
    # column cannot reach any *_Points total.
    blend_cols = blended_stats(actual_scoring_cols)
    volume_actuals = [c for c in VOLUME_STATS
                      if c in df.columns and c not in actual_scoring_cols]
    base = df[base_cols + actual_scoring_cols + volume_actuals]

    curr_week = lg.current_week

    # Constants

    ## Defensive Positions
    d_pos = ['DL', 'DE', 'LB', 'NT', 'CB', 'S', 'DT', 'DB', 'OLB']

    # 1) Combine ESPN and Fantasy Pros Data

    ## a) Build ESPN From Raw Data
    # `projPoints` travels with the stat line because `halve_doubled_espn_yardage`
    # reconciles the two against each other -- and it has to be renamed *first*.
    # `change_col_prefix` is a plain substring replace, not a prefix match, so it
    # turns `projPoints` into `ESPNPoints` (no underscore) rather than leaving it
    # alone, and the correction would then look for a column that no longer exists
    # and silently never fire. The replacement name deliberately contains no "proj"
    # substring for the same reason.
    espn_proj = df[['week', 'player_name', 'primaryPosition', 'player_active_status',
                    'projPoints'] + list(df.filter(like='proj_').columns)]
    espn_proj = espn_proj.rename(columns={'projPoints': ESPN_PUBLISHED_POINTS})
    espn_proj = change_col_prefix(df=espn_proj, old_pfix="proj", new_pfix="ESPN")

    ## b) Build Fantasy Pros From Scrape
    ##
    ## Guarded and freshness-checked like the two books below, which it was not.
    ## This was a bare `pd.read_parquet`, so a season with no weekly FantasyPros file
    ## raised `FileNotFoundError` where an absent book degrades cleanly -- and,
    ## worse, no staleness check ran anywhere on the weekly path, so the 25-day-old
    ## 60-row registration teaser found on 2026-09-08 read exactly like a file
    ## written this morning. `check_source_freshness` had five call sites and all
    ## five were in the season path.
    fp_path = fantasypros_parquet(season)
    if not fp_path.exists():
        fp_proj = absent_weekly_source("FantasyPros", fp_path)
    else:
        check_source_freshness("FantasyPros weekly projections", fp_path,
                               "python -m Scripts.scrape_FP --what weekly")
        fp_proj = pd.read_parquet(fp_path).drop(
            columns=['STD_FantasyPoints', 'TimeStamp'], errors='ignore')
    fp_proj = align_to_espn_names(fp_proj, espn_universe, "FantasyPros")
    fp_proj = change_col_prefix(df=fp_proj, old_pfix="proj", new_pfix="FP")

    ## c) Combine ESPN and FP
    trans1_df = espn_proj.merge(fp_proj, how='left', on=['week', 'player_name'])
    get_match_details(df1=espn_proj, df2=fp_proj, keys=["week", "player_name"], check_col2="FP_rushingTouchdowns", min_wk=curr_week, tbl_lab="FantasyPros Table")

    ## d) Reconcile ESPN's stat line to ESPN's own points total.
    ##
    ## Against `projPoints` rather than against FantasyPros. The old rule compared
    ## ESPN to FP *before* the FP imputation ran, so for any player FantasyPros had
    ## no line for it compared against NaN and never fired -- and FantasyPros served
    ## sixty players in 2025. See `halve_doubled_espn_yardage`.
    ## The base-slot table, not `scores_df`. `get_scoring_table` defaults to the
    ## D/ST slot, whose override map prices some rules differently; `projPoints`
    ## for the offensive players this correction is about comes from the base
    ## values, so the two sides of the comparison have to agree on which.
    base_scores = get_scoring_table(lg, slot=SLOT_BASE)
    trans1_df, halved = halve_doubled_espn_yardage(
        trans1_df, base_scores, projected_points=ESPN_PUBLISHED_POINTS)
    print(f"  ESPN line: halved doubled yardage on {halved} player-week(s).")

    silent = report_silent_zero_stats(trans1_df, base_scores)
    if silent:
        _warn_missing(
            "Scored stats that are identically zero for every player-week, so "
            f"they contribute nothing to any total: {', '.join(silent)}. The rule "
            "is mapped and the column exists, which is why nothing else reports "
            "this -- see report_silent_zero_stats.")

    ## e) Impute FP with ESPN + Create Means
    ##
    ## The reconciliation reference is dropped first. `base` already carries
    ## `projPoints` from `base_cols`, and leaving a second copy here would ship the
    ## same number under two names and give `impute_columns` a non-stat column to
    ## reason about.
    trans1_df = trans1_df.drop(columns=[ESPN_PUBLISHED_POINTS], errors='ignore')
    trans1_df = impute_columns(trans1_df, target_prefix='FP_', source_prefix='ESPN_')
    mean_df = create_mean_cols(trans1_df, target_prefix='FP_', source_prefix='ESPN_')

    ## f) Retain New ESPN Values For Join With Books + Add To Base
    base = base.merge(trans1_df, on=['week', 'player_name', 'primaryPosition','player_active_status'], how='left')

    ## f) Create Dataframe of Means For Imputing Sportsbook Data
    mean_df = create_mean_cols(trans1_df, target_prefix='FP_', source_prefix='ESPN_')
    

    # 2) Combine Pinnacle Data With ESPN and Impute
    ## a) Clean Pinnacle Data
    pinny_proj = clean_pinny(season=season)
    pinny_proj = align_to_espn_names(pinny_proj, espn_universe, "Pinnacle")
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
    bol_proj = align_to_espn_names(bol_proj, espn_universe, "BetOnline")
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

    # 3b) TOMCAT's weekly lines, if a weekly head has written any.
    #
    # No head has (`docs/plans/19-weekly-usage-model.md`), so `clean_usage_weekly`
    # returns the empty frame and this whole block is skipped -- today's output is
    # byte-identical without it. It is here so that shipping the arm is a data
    # change, which is what plan 19 step 5 asks for.
    #
    # **Deliberately outside the impute chain above.** Every other source is filled
    # from `MEAN_` = avg(ESPN, FantasyPros) when it has no line, and TOMCAT must not
    # be: it is the one source in the register that is not derived from the others
    # (G0 measured its residual independence at +0.832 against FantasyPros' +0.988),
    # and filling it from an average of two of them would count those two a third
    # time -- the double-count plan 03 exists to have measured. So its cells are
    # flagged imputed **where they are null**, which makes `compute_weighted_stats`
    # drop the weight and renormalise rather than substitute a number TOMCAT never
    # said. Same treatment `Scripts.season_projections._merge_usage` gives it.
    usg_proj = clean_usage_weekly(season=season)
    if not usg_proj.empty:
        usg_proj = change_col_prefix(df=usg_proj, old_pfix="proj", new_pfix="USG")
        get_match_details(df1=base, df2=usg_proj, keys=SOURCE_JOIN_KEYS,
                          check_col2=next((c for c in usg_proj.columns
                                           if c.startswith("USG_")), None),
                          min_wk=curr_week, tbl_lab="TOMCAT Weekly Table")
        base = base.merge(usg_proj, on=SOURCE_JOIN_KEYS, how='left')
        usg_cols = [c for c in base.columns
                    if c.startswith("USG_") and not c.endswith(IMPUTED_SUFFIX)]
        flags = {c + IMPUTED_SUFFIX: base[c].isna() for c in usg_cols}
        if flags:
            base = pd.concat([base, pd.DataFrame(flags, index=base.index)], axis=1)

    ## 4a) Re-split each book's anytime-touchdown market by the ESPN/FantasyPros
    ## ratio. Runs here because this is the first point all four sources are on one
    ## frame, and before the blend because the blend must see the corrected columns.
    base = reallocate_book_touchdowns(base)


    ## 4b) Report how much of each source is real rather than imputed.
    ##
    ## Measured on Knights_FFL 2025, averaged over all 45 stats: ESPN 100%,
    ## FantasyPros 13%, BetOnline 12%, Pinnacle 8%. Weighting imputed cells at
    ## face value made the nominal four-source blend roughly 90% ESPN. The weights
    ## below are renormalised over whichever sources are real per row, so this
    ## report is the thing to watch when a source degrades.
    print_coverage_report(base, weights_dict=WEIGHTS)

    ## 5) Create Aggregate Columns For Each Projection Type (Manual Weights)
    final = compute_weighted_stats(df=base, stats_list=blend_cols,
                                   weights_dict=WEIGHTS)

    ## 5b) The milestone bands, derived from each source's own line.
    ##
    ## A weekly line is already a one-game mean, so the slate is 1.0 and the band
    ## value *is* the probability of crossing it this week -- which is the quantity a
    ## start/sit decision wants anyway. Deferred import: the blend layer must not
    ## drag the usage feature stack into the Sheets renderer's process.
    from Scripts.season_projections import attach_milestone_bands
    final = attach_milestone_bands(final, actual_scoring_cols,
                                   WEEKLY_PREFIXES, slate=1.0)

    ## 6) Build Score Column
    ##
    ## The prefix list is derived from what the frame actually carries rather than
    ## assumed. It used to differ from `proj_to_score`'s default because that default
    ## included the TOMCAT arms, which have no weekly stat columns -- there is no
    ## weekly model (`docs/plans/19-weekly-usage-model.md` is not started) -- so
    ## scoring them wrote a `USG_Points` that was null for all 3,602 rows of every
    ## 2025 store. A column shaped like a source that never has an opinion reads as a
    ## source that agreed, which is this repo's oldest failure mode. The two lists
    ## agree again since 2026-09-07; `present_prefixes` stays because it is the check
    ## that catches the next divergence rather than a workaround for that one.
    prefixes = present_prefixes(final, WEEKLY_PREFIXES)
    final = proj_to_score(proj_df=final, s_league=lg, col_pfix_list=prefixes)

    ## 6a) What ESPN prices that this pipeline cannot, as a named column.
    ##
    ## This used to be added to every source's points total, `TRUE_Points`
    ## included, under the name `adjustment`. It is not added to anything now.
    ##
    ## The reason is the architecture rather than the arithmetic: points are
    ## defined here as the league's scoring rules applied to a blended stat line,
    ## and `TRUE_Points == score(TRUE_ stat line)` is the identity that lets one
    ## pipeline serve nine leagues. A scalar with no stat behind it breaks that
    ## identity, and because it was ESPN's residual it also injected an unweighted,
    ## full-strength sixth ESPN vote *after* `compute_weighted_stats` had carefully
    ## renormalised the imputed cells out.
    ##
    ## Worst of all it was load-bearing camouflage. Two genuine stat-level defects
    ## were hiding underneath it and neither was visible until it came off: ESPN's
    ## doubled yardage escaping a correction that could not fire (see
    ## `halve_doubled_espn_yardage`), and six milestone-bonus rules in
    ## john_pc_league that are mapped, present and identically zero (see
    ## `report_silent_zero_stats`).
    ##
    ## Removing it costs nothing measurable: against realised 2025 points the patch
    ## made `TRUE_Points` MAE *worse* in six of nine leagues and better by under
    ## 0.2% in the other three. But that is a footnote. The residual stays on the
    ## frame as `espn_unpriced` so the gap has a number to close, and closing it
    ## belongs at the stat level -- a milestone bonus is a function of the yardage
    ## each source already projects.
    final['espn_unpriced'] = final['projPoints'] - final['ESPN_Points']

    ## 7) Build Position Rank Columns
    for i in prefixes:
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
    df = lu[(lu['week'] == week) & (lu['team_owner'] != FREE_AGENT_OWNER) & (~lu['slotPosition'].isin(['BE', 'IR']))][['week', 'team_owner', 'team_name',
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
            return df[df['team_owner'].isin([primary_owner, FREE_AGENT_OWNER])]
        else:
            return df
