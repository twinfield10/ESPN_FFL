"""Points and yards allowed, defined the way ESPN scores them.

Both definitions were reproduced exactly against every 2025 defence-week whose ESPN tier
is recorded in the league stores (see ``docs/plans/51-dst-from-opponent-markets.md``):

**Yards allowed is net scrimmage yards, kneels included** -- every pass, run, kneel and
spike the opponent ran, with sacks counting negative. **357 of 357** tiers match. The
gross figure (player passing yards + rushing yards, which ignores sack yardage) is what
plan 30 used; it runs 15.1 yards a game high and lands 26.1% of team-games in the wrong
tier.

**Points allowed is the opponent's score less what they scored against the offence** --
6 for each interception or fumble returned for a touchdown on a scrimmage play (the
conversion after it *is* charged) and 2 for each safety. Punt, kickoff and blocked-kick
return touchdowns are charged in full: those are the D/ST's own unit. **431 of 431**
tiers match. The opponent's final score, which is what a market's team total prices,
matches 401 and runs 0.54 points a game high.
"""

from __future__ import annotations

from typing import Sequence

import polars as pl

from Scripts import paths

#: Scrimmage play types that count toward yards allowed.
SCRIMMAGE: tuple = ("pass", "run", "qb_kneel", "qb_spike")

#: What ESPN takes off the opponent's score for each score conceded by the offence.
RETURN_TD_CREDIT: int = 6
SAFETY_CREDIT: int = 2

PBP_COLUMNS: tuple = (
    "season", "season_type", "game_id", "play_id", "week", "home_team", "away_team",
    "posteam", "defteam", "play_type", "special_teams_play", "yards_gained", "sack",
    "passing_yards", "rushing_yards", "touchdown", "td_team", "safety",
    "total_home_score", "total_away_score",
)


def allowed_from_pbp(pbp: pl.DataFrame) -> pl.DataFrame:
    """ESPN-definition points and yards allowed per defence-game.

    Args:
        pbp: nflverse play-by-play carrying :data:`PBP_COLUMNS`. Regular-season rows
            only are used.

    Returns:
        pl.DataFrame: One row per defence per game -- ``season``, ``week``, ``game_id``,
        ``team`` (the defence), ``opponent``, ``points_allowed`` (ESPN),
        ``points_allowed_all`` (the opponent's final score), ``ret_td_vs_off``,
        ``safeties_vs_off``, ``yards_allowed`` (ESPN, net), ``yards_allowed_gross``.
    """
    p = (pbp.filter(pl.col("season_type") == "REG").sort("game_id", "play_id")
         .with_columns(pl.col("special_teams_play").fill_null(0),
                       pl.col("sack").fill_null(0)))

    games = (p.group_by("season", "week", "game_id", "home_team", "away_team")
             .agg(pl.col("total_home_score").max().alias("home_pts"),
                  pl.col("total_away_score").max().alias("away_pts")))
    sides = pl.concat([
        games.select("season", "week", "game_id", pl.col("home_team").alias("team"),
                     pl.col("away_team").alias("opponent"),
                     pl.col("away_pts").alias("points_allowed_all")),
        games.select("season", "week", "game_id", pl.col("away_team").alias("team"),
                     pl.col("home_team").alias("opponent"),
                     pl.col("home_pts").alias("points_allowed_all")),
    ])

    # A touchdown by the side without the ball on a scrimmage play: pick-six or scoop-and-
    # score against this team's offence. `posteam` is the offence that gave it up.
    ret = (p.filter(pl.col("touchdown") == 1, pl.col("td_team").is_not_null(),
                    pl.col("td_team") != pl.col("posteam"),
                    pl.col("play_type").is_in(["pass", "run"]),
                    pl.col("special_teams_play") == 0)
           .group_by("game_id", pl.col("posteam").alias("team"))
           .agg(pl.len().cast(pl.Int64).alias("ret_td_vs_off")))

    # A safety goes to whichever side's score moved by two on the play, which is not
    # always the defence -- an interception downed in its own end zone concedes one
    # to the offence. Reading the score change keeps that case on the right side.
    sc = p.with_columns(
        (pl.col("total_home_score") - pl.col("total_home_score").shift(1).over("game_id")
         .fill_null(0)).alias("_dh"),
        (pl.col("total_away_score") - pl.col("total_away_score").shift(1).over("game_id")
         .fill_null(0)).alias("_da"))
    saf = (sc.filter(pl.col("safety") == 1)
           .with_columns(pl.when(pl.col("_dh") == 2).then(pl.col("away_team"))
                         .when(pl.col("_da") == 2).then(pl.col("home_team"))
                         .otherwise(pl.col("posteam")).alias("team"))
           .group_by("game_id", "team")
           .agg(pl.len().cast(pl.Int64).alias("safeties_vs_off")))

    scrim = p.filter(pl.col("play_type").is_in(list(SCRIMMAGE)),
                     pl.col("special_teams_play") == 0)
    yds = (scrim.group_by("game_id", pl.col("defteam").alias("team"))
           .agg(pl.col("yards_gained").fill_null(0).sum().alias("yards_allowed"),
                (pl.col("passing_yards").filter(pl.col("sack") == 0).fill_null(0).sum()
                 + pl.col("rushing_yards").fill_null(0).sum())
                .alias("yards_allowed_gross")))

    return (sides.join(ret, on=["game_id", "team"], how="left")
            .join(saf, on=["game_id", "team"], how="left")
            .join(yds, on=["game_id", "team"], how="left")
            .with_columns(pl.col("ret_td_vs_off").fill_null(0),
                          pl.col("safeties_vs_off").fill_null(0),
                          pl.col("yards_allowed").fill_null(0),
                          pl.col("yards_allowed_gross").fill_null(0))
            .with_columns((pl.col("points_allowed_all")
                           - RETURN_TD_CREDIT * pl.col("ret_td_vs_off")
                           - SAFETY_CREDIT * pl.col("safeties_vs_off"))
                          .alias("points_allowed"))
            .with_columns(pl.col("season").cast(pl.Int32), pl.col("week").cast(pl.Int32)))


def load(seasons: Sequence[int]) -> pl.DataFrame:
    """:func:`allowed_from_pbp` over each season's local play-by-play.

    Args:
        seasons: Seasons to read from ``Data/NFL/<season>/pbp.parquet``.

    Returns:
        pl.DataFrame: As :func:`allowed_from_pbp`.
    """
    frames = []
    for s in seasons:
        lf = pl.scan_parquet(paths.DATA_DIR / "NFL" / str(s) / "pbp.parquet")
        have = [c for c in PBP_COLUMNS if c in lf.collect_schema().names()]
        frames.append(allowed_from_pbp(lf.select(have).collect()))
    return pl.concat(frames, how="diagonal_relaxed")
