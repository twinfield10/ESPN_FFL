"""What an offence does with each position, and which coach's offence it is.

``docs/plans/21-coaching-and-scheme.md``. The rookie arm in
:mod:`Scripts.usage.season` predicts volume from draft capital alone: it knows what a
team *invested* in a player and nothing about the offence he is walking into. A
third-round back on a team that gives its backs 60 carries a week is not the same
asset as the same pick on a team that throws.

**The measurement that justifies keying this on the coach.** Over 288 consecutive
team-season pairs, year-over-year persistence of team usage roughly halves when the
head coach changes -- RB carry share 0.693 against 0.405, TE target share 0.530
against 0.176, and the same pattern on all six metrics. Usage is a property of the
coaching staff rather than of the franchise. Coach mean RB target share spans 0.132
(Sean McVay) to 0.263 (Anthony Lynn) across 48 coaches with three or more seasons,
and the between-coach spread is 70% of the spread across all team-seasons.

**Two priors, because they disagree exactly when it matters.** A team's own trailing
profile is the better estimate while the staff is stable, and actively misleading the
year a new coach arrives -- which is the year a drafter most needs help. The coach's
own prior seasons cover that case. Both are emitted; the model decides.

**What this deliberately does not claim.** Nobody publishes who calls the plays, so
none of this is attributed to a play-caller. What it measures is *coaching-staff
continuity*, and the columns are named for that. Plan 21 sizes the residual: with the
same head coach, RB target share still moves more than a league standard deviation in
30% of team-seasons.

Everything reads ``player_weeks``, already on disk from ``R/GetUsage.R``, plus the
committed coaching table. No new pull.
"""

from typing import Dict, List, Optional, Sequence

import polars as pl

from Scripts.paths import DATA_DIR

#: The committed coaching table, from ``python -m Scripts.coaches``.
COACHING_STAFF_PARQUET = DATA_DIR / "NFL" / "coaching_staff.parquet"

#: Team-season profile metrics. Shares and per-game rates, never totals -- a team
#: that ran more plays should not look like a team that favoured a position.
PROFILE_METRICS: List[str] = [
    "plays_pg",
    "pass_rate",
    "rb_carry_share",
    "qb_carry_share",
    "rb_target_share",
    "wr_target_share",
    "te_target_share",
    # Derived group volumes: the pool a player at that position is competing for,
    # which is what a projection actually needs. plays x run/pass split x share.
    "team_rb_carries_pg",
    "team_rb_targets_pg",
    "team_wr_targets_pg",
    "team_te_targets_pg",
    "team_pass_attempts_pg",
]

#: Which profile metrics inform a player at each position. Passed to the model so a
#: receiver is not handed the team's carry distribution.
POSITION_METRICS: Dict[str, List[str]] = {
    "QB": ["pass_rate", "qb_carry_share", "team_pass_attempts_pg", "plays_pg"],
    "RB": ["rb_carry_share", "rb_target_share", "team_rb_carries_pg",
           "team_rb_targets_pg", "plays_pg"],
    "WR": ["wr_target_share", "team_wr_targets_pg", "pass_rate", "plays_pg"],
    "TE": ["te_target_share", "team_te_targets_pg", "pass_rate", "plays_pg"],
}

#: Prior seasons at which a coach's own mean is weighted equally with the league
#: mean. Three because that is roughly where the between-coach signal was measured
#: to be stable -- the 48 coaches with 3+ seasons are the ones whose means separate.
COACH_SHRINKAGE = 3.0

#: Prefix for a coach-prior column.
COACH_PREFIX = "coach_"

#: Prefix for a team's own trailing-profile column.
TEAM_PREFIX = "team_prior_"


def load_staff(path=None) -> pl.DataFrame:
    """The committed coaching table.

    Args:
        path: Override the location, for tests.

    Returns:
        pl.DataFrame: ``season``, ``team``, ``head_coach`` and the rest.

    Raises:
        FileNotFoundError: When it has not been built.
    """
    path = COACHING_STAFF_PARQUET if path is None else path
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} is missing. Build it with `Rscript R/GetCoaches.R` then "
            f"`python -m Scripts.coaches`."
        )
    return pl.read_parquet(path).with_columns(pl.col("season").cast(pl.Int32))


def team_profile(player_weeks: pl.DataFrame) -> pl.DataFrame:
    """One row per team-season describing what the offence did.

    Args:
        player_weeks: :func:`Scripts.usage.features.load_player_weeks` output, with
            ``team``, ``position``, ``carries``, ``attempts`` and ``targets``.

    Returns:
        pl.DataFrame: ``season``, ``team``, ``team_games`` and every
        :data:`PROFILE_METRICS` column.

    Raises:
        KeyError: When a required column is absent.
    """
    required = {"season", "week", "team", "position", "carries", "attempts",
                "targets"}
    missing = sorted(required - set(player_weeks.columns))
    if missing:
        raise KeyError(
            f"team_profile needs {missing}; got "
            f"{sorted(player_weeks.columns)[:12]}. Read player_weeks.parquet "
            f"without selecting those away."
        )

    scoped = player_weeks.with_columns(pl.col("season").cast(pl.Int32))

    # Team games from distinct (team, week), the same derivation
    # `Scripts.usage.context.team_games` uses and for the same reason: it reproduces
    # the 2022 exception where two teams played 16.
    games = (scoped.select("season", "week", "team").unique()
             .group_by(["season", "team"])
             .agg(pl.len().cast(pl.Int32).alias("team_games")))

    totals = (scoped.group_by(["season", "team"])
              .agg(pl.col("carries").sum().alias("carries"),
                   pl.col("attempts").sum().alias("attempts"),
                   pl.col("targets").sum().alias("targets")))

    by_position = (
        scoped.filter(pl.col("position").is_in(list(POSITION_METRICS)))
        .group_by(["season", "team", "position"])
        .agg(pl.col("targets").sum().alias("t"),
             pl.col("carries").sum().alias("c"))
    )
    wide = by_position.pivot(on="position", index=["season", "team"],
                             values=["t", "c"], aggregate_function="sum")
    # A team with no rows at some position -- never in practice, but a filtered or
    # partial frame in a test -- must read as zero rather than null, or every share
    # built from it goes null and the join silently drops the team.
    for position in POSITION_METRICS:
        for prefix in ("t", "c"):
            column = f"{prefix}_{position}"
            if column not in wide.columns:
                wide = wide.with_columns(pl.lit(0.0).alias(column))
    wide = wide.with_columns(
        [pl.col(c).fill_null(0.0) for c in wide.columns
         if c not in ("season", "team")])

    def share(numerator: str, denominator: str) -> pl.Expr:
        """A share, null rather than a divide-by-zero when the denominator is empty."""
        return (pl.when(pl.col(denominator) > 0)
                .then(pl.col(numerator) / pl.col(denominator))
                .otherwise(None))

    profile = (
        totals.join(games, on=["season", "team"], how="left")
        .join(wide, on=["season", "team"], how="left")
        .with_columns(
            ((pl.col("attempts") + pl.col("carries")) / pl.col("team_games"))
            .alias("plays_pg"),
        )
        .with_columns(
            (pl.col("attempts")
             / (pl.col("attempts") + pl.col("carries"))).alias("pass_rate"),
            share("c_RB", "carries").alias("rb_carry_share"),
            share("c_QB", "carries").alias("qb_carry_share"),
            share("t_RB", "targets").alias("rb_target_share"),
            share("t_WR", "targets").alias("wr_target_share"),
            share("t_TE", "targets").alias("te_target_share"),
            (pl.col("c_RB") / pl.col("team_games")).alias("team_rb_carries_pg"),
            (pl.col("t_RB") / pl.col("team_games")).alias("team_rb_targets_pg"),
            (pl.col("t_WR") / pl.col("team_games")).alias("team_wr_targets_pg"),
            (pl.col("t_TE") / pl.col("team_games")).alias("team_te_targets_pg"),
            (pl.col("attempts") / pl.col("team_games"))
            .alias("team_pass_attempts_pg"),
        )
    )
    return profile.select(["season", "team", "team_games"] + PROFILE_METRICS) \
        .sort(["season", "team"])


def league_means(profile: pl.DataFrame,
                 seasons: Optional[Sequence[int]] = None) -> Dict[str, float]:
    """The league-average profile, for shrinking a thin coach mean toward.

    Args:
        profile: :func:`team_profile` output.
        seasons: Seasons to average over. None uses all of ``profile``.

    Returns:
        dict: Metric to mean, omitting metrics with no data.
    """
    scoped = (profile if seasons is None
              else profile.filter(pl.col("season").is_in(list(seasons))))
    if scoped.is_empty():
        return {}
    row = scoped.select([pl.col(m).mean().alias(m) for m in PROFILE_METRICS
                         if m in scoped.columns]).row(0, named=True)
    return {metric: float(value) for metric, value in row.items()
            if value is not None}


def coach_prior(profile: pl.DataFrame, staff: pl.DataFrame, target_season: int,
                shrinkage: float = COACH_SHRINKAGE) -> pl.DataFrame:
    """Each coach's expected profile, from his seasons before ``target_season``.

    ``(n * coach_mean + k * league_mean) / (n + k)`` in seasons observed, the same
    credibility weighting the efficiency features use. A coach in his first year has
    ``n = 0`` and receives the league mean, flagged by ``coach_seasons``.

    **Only seasons before ``target_season``.** A coach prior that includes the season
    being predicted leaks the outcome into the feature, and it would leak the same way
    for every player on that team at once.

    Args:
        profile: :func:`team_profile` output.
        staff: :func:`load_staff` output.
        target_season: The season being predicted.
        shrinkage: Prior seasons at which coach and league weigh equally.

    Returns:
        pl.DataFrame: ``head_coach``, ``coach_seasons`` and one
        ``coach_<metric>`` column per :data:`PROFILE_METRICS`.
    """
    history = profile.filter(pl.col("season") < target_season)
    means = league_means(history)
    if history.is_empty() or not means:
        return pl.DataFrame(schema={"head_coach": pl.String,
                                    "coach_seasons": pl.Int32})

    joined = history.join(
        staff.select("season", "team", "head_coach"),
        on=["season", "team"], how="inner").filter(pl.col("head_coach").is_not_null())

    metrics = [m for m in PROFILE_METRICS if m in joined.columns]
    aggregated = (joined.group_by("head_coach")
                  .agg(pl.len().cast(pl.Int32).alias("coach_seasons"),
                       *[pl.col(m).mean().alias(m) for m in metrics]))

    shrunk = [
        ((pl.col("coach_seasons").cast(pl.Float64) * pl.col(metric).fill_null(
            means.get(metric, 0.0)) + shrinkage * means.get(metric, 0.0))
         / (pl.col("coach_seasons").cast(pl.Float64) + shrinkage))
        .alias(f"{COACH_PREFIX}{metric}")
        for metric in metrics if metric in means
    ]
    return (aggregated.with_columns(shrunk)
            .select(["head_coach", "coach_seasons"]
                    + [f"{COACH_PREFIX}{m}" for m in metrics if m in means])
            .sort("head_coach"))


def team_prior(profile: pl.DataFrame, target_season: int,
               window: int = 1) -> pl.DataFrame:
    """Each team's own recent profile, before ``target_season``.

    The better estimate while the staff is stable, and the misleading one the year a
    new coach arrives -- which is why :func:`coach_prior` exists beside it.

    Args:
        profile: :func:`team_profile` output.
        target_season: The season being predicted.
        window: Seasons to average, most recent first.

    Returns:
        pl.DataFrame: ``team`` plus one ``team_prior_<metric>`` column, and
        ``team_prior_seasons``.
    """
    seasons = [target_season - offset for offset in range(1, window + 1)]
    history = profile.filter(pl.col("season").is_in(seasons))
    metrics = [m for m in PROFILE_METRICS if m in history.columns]
    if history.is_empty():
        return pl.DataFrame(schema={"team": pl.String,
                                    "team_prior_seasons": pl.Int32})
    return (history.group_by("team")
            .agg(pl.len().cast(pl.Int32).alias("team_prior_seasons"),
                 *[pl.col(m).mean().alias(f"{TEAM_PREFIX}{m}") for m in metrics])
            .sort("team"))


def attach(features: pl.DataFrame, profile: pl.DataFrame, staff: pl.DataFrame,
           target_season: int, shrinkage: float = COACH_SHRINKAGE) -> pl.DataFrame:
    """Join the coach prior, the team prior, and the continuity flags onto features.

    Args:
        features: One row per player, with ``team`` and ``position``. The team is the
            *current* one from the pre-season roster, which is the point -- a player
            who moved gets his new team's context and his new coach's prior.
        profile: :func:`team_profile` output.
        staff: :func:`load_staff` output.
        target_season: The season being predicted.
        shrinkage: Passed to :func:`coach_prior`.

    Returns:
        pl.DataFrame: ``features`` plus ``head_coach``, ``coach_seasons``,
        ``coach_is_new`` (no prior seasons at all), ``staff_continuity`` (the coach
        also coached this team last season), and the prior columns.
    """
    current = staff.filter(pl.col("season") == target_season) \
        .select("team", "head_coach")
    previous = staff.filter(pl.col("season") == target_season - 1) \
        .select("team", pl.col("head_coach").alias("prior_head_coach"))

    priors = coach_prior(profile, staff, target_season, shrinkage)
    out = (features
           .join(current, on="team", how="left")
           .join(previous, on="team", how="left")
           .join(priors, on="head_coach", how="left")
           .join(team_prior(profile, target_season), on="team", how="left"))

    # A first-year head coach has no row in `priors` at all, so the join leaves his
    # players null. Six of 32 teams were in that position for 2026 -- a fifth of the
    # league -- so filling with the league mean and flagging it is more useful than a
    # column of nulls the model can only abstain on. `coach_is_new` is what says the
    # number is a stand-in.
    means = league_means(profile.filter(pl.col("season") < target_season))
    filled = [
        pl.col(f"{COACH_PREFIX}{metric}").fill_null(value)
        for metric, value in means.items()
        if f"{COACH_PREFIX}{metric}" in out.columns
    ]
    out = out.with_columns(filled) if filled else out

    return out.with_columns(
        pl.col("coach_seasons").fill_null(0).alias("coach_seasons"),
        (pl.col("coach_seasons").fill_null(0) == 0).alias("coach_is_new"),
        # Named for what it measures. The head coach staying is not the play-caller
        # staying, and no free source resolves the difference -- see the module
        # docstring.
        (pl.col("head_coach").is_not_null()
         & (pl.col("head_coach") == pl.col("prior_head_coach")))
        .alias("staff_continuity"),
    )


def position_columns(position: str, prefix: str = COACH_PREFIX) -> List[str]:
    """The prior columns that inform a player at one position.

    Args:
        position: A :data:`POSITION_METRICS` key.
        prefix: :data:`COACH_PREFIX` or :data:`TEAM_PREFIX`.

    Returns:
        list: Column names, empty for an unmodelled position.
    """
    return [f"{prefix}{metric}" for metric in POSITION_METRICS.get(position, [])]
