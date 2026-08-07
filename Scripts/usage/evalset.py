"""The evaluation set: one row per player-week, every source's line, the outcome.

Step 0 of ``docs/plans/16-usage-data-layer.md``. Accuracy has to be measured on
cells where a source *really* had a line, otherwise ESPN is being scored against
its own imputed copy: measured on Knights_FFL 2025 and averaged over 45 stats,
real coverage is ESPN 100%, FantasyPros 13%, BetOnline 12%, Pinnacle 8%. So the
``*_is_imputed`` provenance flags travel with the values, and every measurement
downstream filters on them.

Built from the store rather than from a fresh ingest, deliberately -- the store is
what the app and Sheets render, so a gate measured on it is measured on the
numbers that were actually published.

**Pooled across leagues.** A stat line is a league-independent fact: ESPN's
projected receiving yards for a player-week do not depend on whose league is
asking, and neither does what he actually gained. What *is* league-specific is
which stats a league scores at all -- and therefore which columns its store
carries, 389 to 549 across the nine -- so pooling the nine stores covers more
player-weeks and more stats than any one of them. Points are the exception and are
left out: those are league-specific, and :mod:`Scripts.usage.gates` reads them
per league where it needs them.
"""

from typing import Dict, List, Optional, Sequence, Tuple

import polars as pl

from Scripts import store
from Scripts.crosswalk import id_map
from Scripts.projection_utils import IMPUTED_SUFFIX
from Scripts.usage.baseline import USAGE_PREFIX
from Scripts.usage.nflverse import (
    ACTUAL_PREFIX,
    USAGE_STATS,
    load_opportunity,
    teams_by_week,
)

#: The four weekly projection sources, in the order the pipeline builds them.
#: ``MEAN`` and ``TRUE`` are derived from these and are not independent opinions,
#: so they are not evaluated as sources.
SOURCES: Tuple[str, ...] = ("ESPN", "FP", "PINNY", "BOL")

#: Identity columns carried through pooling.
ID_COLUMNS = ("week", "player_id", "player_name", "primaryPosition")


def _league_columns(available: Sequence[str],
                    stats: Sequence[str]) -> Tuple[List[str], List[str]]:
    """Split the columns this league's store can supply into wanted and missing.

    Args:
        available: The store frame's column names.
        stats: ESPN stat names being evaluated.

    Returns:
        tuple: ``(columns to read, stats this league does not score)``.
    """
    have = set(available)
    columns = [c for c in ID_COLUMNS if c in have]
    missing_stats = []
    for stat in stats:
        # The actual outcome is the un-prefixed column, and it only exists when
        # the league scores that stat -- a non-PPR league carries no
        # receivingReceptions column at all.
        if stat not in have:
            missing_stats.append(stat)
            continue
        columns.append(stat)
        for source in SOURCES:
            for candidate in (f"{source}_{stat}", f"{source}_{stat}{IMPUTED_SUFFIX}"):
                if candidate in have:
                    columns.append(candidate)
    return columns, missing_stats


def league_eval_frame(season: int, league_key: str,
                      stats: Sequence[str]) -> pl.DataFrame:
    """One league's contribution to the evaluation set.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        stats: ESPN stat names to carry.

    Returns:
        pl.DataFrame: Identity columns, ``act_<stat>``, and each source's line and
        provenance flag for the stats this league scores.

    Raises:
        FileNotFoundError: When the league has no ``lineups`` in its store.
    """
    path = store.require_artifact(season, league_key, "lineups")
    frame = pl.read_parquet(path)
    columns, _ = _league_columns(frame.columns, stats)
    out = frame.select(columns)
    return out.rename({s: f"{ACTUAL_PREFIX}{s}" for s in stats
                       if s in out.columns})


def build_eval_set(season: int,
                   league_keys: Optional[Sequence[str]] = None,
                   stats: Optional[Sequence[str]] = None
                   ) -> Tuple[pl.DataFrame, Dict]:
    """Pool every league's store into one player-week evaluation set.

    Duplicate ``(week, player_id)`` rows across leagues are collapsed by taking
    the first non-null value of each column. That is safe because the pooled
    columns are league-independent facts, and the returned report carries the
    largest disagreement found so that the assumption is checked rather than
    asserted.

    Args:
        season: Season year.
        league_keys: Leagues to pool. Defaults to every league with a store.
        stats: ESPN stat names. Defaults to every key of
            :data:`Scripts.usage.nflverse.USAGE_STATS`, since those are the stats
            a usage model can emit.

    Returns:
        tuple: ``(frame, report)``. The report holds per-league row counts, the
        stats each league does not score, and the worst cross-league
        disagreement.

    Raises:
        ValueError: When no league has a store for ``season``.
    """
    stats = list(USAGE_STATS) if stats is None else list(stats)
    keys = list(store.list_leagues(season)) if league_keys is None else list(league_keys)
    if not keys:
        raise ValueError(
            f"No league store for {season}. Build one with "
            f"`python -m Scripts.refresh --all --season {season}`."
        )

    frames = []
    report: Dict = {"season": season, "leagues": {}, "stats": stats}
    for league_key in keys:
        frame = pl.read_parquet(store.require_artifact(season, league_key, "lineups"))
        columns, missing = _league_columns(frame.columns, stats)
        selected = frame.select(columns).rename(
            {s: f"{ACTUAL_PREFIX}{s}" for s in stats if s in columns}
        )
        report["leagues"][league_key] = {
            "rows": selected.height,
            "unscored_stats": missing,
        }
        frames.append(selected)

    pooled = pl.concat(frames, how="diagonal")

    value_columns = [c for c in pooled.columns if c not in ID_COLUMNS]
    # Disagreement is measured before collapsing, on the actual outcome and on
    # ESPN's line -- the two columns every league carries for a scored stat. If
    # these ever differ for one player-week, pooling is unsound and the number
    # below says so instead of the pooling quietly picking a winner.
    checked = [c for c in value_columns
               if c.startswith(ACTUAL_PREFIX) or c.startswith("ESPN_")]
    worst = 0.0
    if checked:
        spread = (
            pooled.group_by(["week", "player_id"])
            .agg([(pl.col(c).max() - pl.col(c).min()).alias(c) for c in checked])
            .select(pl.max_horizontal([pl.col(c).abs().max() for c in checked])
                    .alias("worst"))
        )
        if spread.height and spread["worst"][0] is not None:
            worst = float(spread["worst"][0])
    report["worst_cross_league_disagreement"] = worst

    collapsed = (
        pooled.group_by(["week", "player_id"])
        .agg([pl.col(c).drop_nulls().first().alias(c)
              for c in pooled.columns if c not in ("week", "player_id")])
        .sort(["week", "player_id"])
    )

    mapping = id_map("espn_id", "gsis_id")
    collapsed = collapsed.with_columns(
        pl.col("player_id").cast(pl.Utf8).replace_strict(
            mapping, default=None, return_dtype=pl.Utf8).alias("gsis_id"),
        pl.lit(season).cast(pl.Int32).alias("season"),
    )
    report["rows"] = collapsed.height
    report["with_gsis_id"] = int(collapsed["gsis_id"].is_not_null().sum())
    return collapsed, report


def usage_grid(eval_set: pl.DataFrame) -> pl.DataFrame:
    """The player-weeks a usage model must project to be comparable.

    Every other source projects a player-week whether or not the player ends up
    taking a snap, and eats the error when a projected starter is inactive. Asking
    the usage model only about the weeks its own data has a row for would let it
    skip exactly those player-weeks.

    Args:
        eval_set: Frame from :func:`build_eval_set`.

    Returns:
        pl.DataFrame: Unique ``gsis_id`` and ``week`` for rows that resolved to a
        play-by-play id.
    """
    return (eval_set.filter(pl.col("gsis_id").is_not_null())
            .select(["gsis_id", "week"]).unique())


def attach_usage(eval_set: pl.DataFrame, predictions: pl.DataFrame,
                 season: Optional[int] = None) -> pl.DataFrame:
    """Join the usage model's stat lines onto the evaluation set.

    Attaches two population flags alongside them, because "the model was wrong"
    and "the player was not on the field" are different failures and the gates
    have to be able to separate them:

    * ``team_played`` -- his team played that week. Its negation is a bye, which is
      public information the other four sources all use and the crude baseline
      simply has not been given. Judging it on bye weeks measures missing
      plumbing.
    * ``played`` -- he has a usage row, so he took offensive snaps. Its negation
      inside ``team_played`` is the genuinely hard availability problem that
      ``docs/plans/19-weekly-usage-model.md`` picks up with the injury report.

    Args:
        eval_set: Frame from :func:`build_eval_set`.
        predictions: Frame from :func:`Scripts.usage.baseline.predict_season`.
        season: Season for the lookups. Defaults to the eval set's.

    Returns:
        pl.DataFrame: ``eval_set`` plus ``USG_<stat>`` columns,
        ``weeks_of_history``, ``team_played`` and ``played``.
    """
    season = int(eval_set["season"][0]) if season is None else int(season)
    carried = [c for c in predictions.columns
               if c.startswith(USAGE_PREFIX)
               or c in ("weeks_of_history", "last_posteam")]

    joined = eval_set.join(
        predictions.select(["week", "gsis_id"] + carried),
        on=["week", "gsis_id"], how="left",
    )

    appearances = (
        load_opportunity([season], stats=[])
        .select(["week", "gsis_id"])
        .with_columns(pl.lit(True).alias("played"))
    )
    playing_teams = teams_by_week(season).with_columns(
        pl.lit(True).alias("team_played")
    )
    return (
        joined.join(appearances, on=["week", "gsis_id"], how="left")
        .join(playing_teams, left_on=["week", "last_posteam"],
              right_on=["week", "posteam"], how="left")
        .with_columns(
            pl.col("played").fill_null(False),
            # Two different nulls here, and conflating them silently marks every
            # bye week as played. A null `last_posteam` means the player has no
            # usage history at all, so no team is known -- he carries no usage
            # projection either, and keeping him in the population is what makes it
            # the same population the other four sources are measured on. A null
            # `team_played` with a known team means the team genuinely did not
            # play: a bye.
            pl.when(pl.col("last_posteam").is_null()).then(True)
            .otherwise(pl.col("team_played").fill_null(False))
            .alias("team_played"),
        )
    )


def real_mask(frame: pl.DataFrame, source: str, stat: str) -> pl.Expr:
    """Whether ``source``'s cell for ``stat`` is a real line rather than a fill-in.

    ESPN is the root source and is never imputed, so it counts as real wherever it
    has a value. The usage model has no flag column either -- it abstains by
    returning null, which is the same statement.

    Args:
        frame: An evaluation frame.
        source: Source prefix without the underscore, e.g. ``"PINNY"``.
        stat: ESPN stat name.

    Returns:
        pl.Expr: Boolean expression, all-false when the column is absent.
    """
    column = f"{source}_{stat}"
    if column not in frame.columns:
        return pl.lit(False)
    flag = f"{column}{IMPUTED_SUFFIX}"
    if flag in frame.columns:
        # A null flag means the row never joined to that source, which is itself
        # a miss -- the same reading compute_weighted_stats takes.
        return pl.col(column).is_not_null() & ~pl.col(flag).fill_null(True)
    return pl.col(column).is_not_null()


def coverage(frame: pl.DataFrame, stats: Sequence[str],
             sources: Sequence[str] = SOURCES + (USAGE_PREFIX.rstrip("_"),)
             ) -> pl.DataFrame:
    """Per-source share of rows carrying a real line, by stat.

    Args:
        frame: An evaluation frame with usage attached.
        stats: ESPN stat names.
        sources: Source prefixes to report.

    Returns:
        pl.DataFrame: Columns ``stat`` then one percentage column per source.
    """
    rows = []
    for stat in stats:
        row: Dict[str, object] = {"stat": stat}
        for source in sources:
            column = f"{source}_{stat}"
            if column not in frame.columns:
                row[source] = None
                continue
            row[source] = round(
                100.0 * float(frame.select(real_mask(frame, source, stat)
                                          .mean())[0, 0] or 0.0), 1)
        rows.append(row)
    return pl.DataFrame(rows)
