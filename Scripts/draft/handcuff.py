"""Backfield rank and handcuff value, from Vegas team strength.

A good team leads, runs to bleed clock, and feeds its backfield; a bad team trails and
throws. Plan 21 measured that narrative across 5,198 team-games and found it true and
large **after the fact** -- rush attempts run 20.7 in a blowout loss against 32.9 in a
blowout win -- but mostly unforecastable, and worth nothing at all as a season-model
feature (+0.0015 R-squared over prior-season volume).

One piece of it does survive into something a drafter can use, and it is the piece
that is invisible in a projection: **the extra volume on a strong team goes to the
backup, not the starter.**

| team strength | RB1 carries | RB2 carries | RB2 share |
|---|---|---|---|
| weak | 203.9 | 84.0 | 0.244 |
| average | 213.5 | 96.6 | 0.264 |
| strong | 213.2 | **103.0** | **0.280** |

RB1 is flat across the range. RB2 gains 19 carries. So a strong team's handcuff is
worth materially more than a weak team's, and nothing on the board said so.

**The effect is small and this module says so in its own output.** Fitted over 315
team-seasons, ``RB2 carries = 94.5 + 1.65 x strength`` at **R-squared 0.030** with a
residual standard deviation of 36 carries. Across the realistic spread range that is
about +-13 carries, roughly 55 rushing yards, against 36 carries of noise. It is a
tiebreaker between two similar backups, not a reason to move anyone up a round, and
``handcuff_r2`` travels to the board so the number cannot be read as more than it is.

Team strength is the mean Vegas spread across a team's games, from the point of view
of that team -- positive means favoured. Every 2026 game is already priced, all 272 of
them, so this needs no forecast of its own.

``Data/NFL/schedules.parquet`` holds 2016-2026 from ``nflverse/nfldata``
(``data/games.csv``), which is what ``nflreadr::load_schedules`` reads.
"""

from __future__ import annotations

from typing import Dict, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts.paths import DATA_DIR

#: Committed schedule table: one row per game, with the closing spread.
SCHEDULES_PARQUET = DATA_DIR / "NFL" / "schedules.parquet"

#: Schedule abbreviation -> the abbreviation ESPN uses on the board.
#:
#: Only two differ across all 32 teams, and both are silent failures rather than
#: errors: an unmapped team simply gets a null strength and drops out of the join.
TEAM_ALIASES: Dict[str, str] = {"LA": "LAR", "WAS": "WSH"}

#: Seasons the RB2 relationship is fitted over.
FIT_SEASONS: Tuple[int, ...] = tuple(range(2016, 2026))

#: Backfield rank that counts as the handcuff.
HANDCUFF_RANK: int = 2

#: Team-seasons required before the fit is trusted.
MIN_FIT_ROWS: int = 100


def load_schedules() -> pl.DataFrame:
    """The committed schedule table.

    Returns:
        pl.DataFrame: One row per game.

    Raises:
        FileNotFoundError: When the pull is missing.
    """
    if not SCHEDULES_PARQUET.is_file():
        raise FileNotFoundError(
            f"No schedules at {SCHEDULES_PARQUET}. It is committed; restore it, or "
            f"re-pull from nflverse/nfldata data/games.csv."
        )
    return pl.read_parquet(SCHEDULES_PARQUET)


def team_strength(season: int,
                  schedules: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Mean Vegas spread per team, from that team's own point of view.

    The schedule stores one spread per game, signed for the home team, so each game
    contributes twice with opposite signs. Positive means favoured.

    Args:
        season: Season to summarise.
        schedules: Override the table, for tests.

    Returns:
        pl.DataFrame: ``pro_team`` and ``team_strength``. Empty when the season has
        no priced games.
    """
    frame = load_schedules() if schedules is None else schedules
    games = frame.filter((pl.col("season") == season)
                         & (pl.col("game_type") == "REG")
                         & pl.col("spread_line").is_not_null())
    if games.is_empty():
        return pl.DataFrame(schema={"pro_team": pl.String,
                                    "team_strength": pl.Float64})

    home = games.select(pl.col("home_team").alias("team"),
                        pl.col("spread_line").cast(pl.Float64).alias("spread"))
    away = games.select(pl.col("away_team").alias("team"),
                        (-pl.col("spread_line").cast(pl.Float64)).alias("spread"))
    return (pl.concat([home, away])
            .group_by("team")
            .agg(pl.col("spread").mean().alias("team_strength"))
            .with_columns(pl.col("team")
                          .replace(TEAM_ALIASES)
                          .alias("pro_team"))
            .select("pro_team", "team_strength"))


def fit_rb2_carries(seasons: Sequence[int] = FIT_SEASONS
                    ) -> Optional[Tuple[float, float, float, float]]:
    """Regress the number-two back's season carries on his team's strength.

    Fitted rather than hardcoded so the number on the board can be re-derived, and so
    it moves if the league does. RB2 is defined by realised carries, which is what a
    drafter is trying to anticipate -- it therefore includes the games the starter
    missed, and that is deliberate: the question a handcuff answers is "what does the
    backup on this team actually end up with", not "what does he get while the
    starter is healthy".

    Args:
        seasons: Completed seasons to fit over.

    Returns:
        tuple | None: ``(intercept, slope, r_squared, residual_sd)``, or None when the
        inputs are missing or too thin. None means the board ships without these
        columns rather than with invented ones.
    """
    from Scripts.usage.features import load_player_weeks

    try:
        schedules = load_schedules()
    except FileNotFoundError:
        return None

    rows = []
    for season in sorted(set(seasons)):
        try:
            weekly = load_player_weeks([season])
        except FileNotFoundError:
            continue
        backs = weekly.filter((pl.col("position") == "RB") & (pl.col("carries") > 0))
        if backs.is_empty():
            continue
        totals = (backs.group_by(["team", "gsis_id"])
                  .agg(pl.col("carries").sum().alias("carries")))
        ranked = totals.with_columns(
            pl.col("carries").rank("ordinal", descending=True)
            .over("team").alias("backfield_rank"))
        second = ranked.filter(pl.col("backfield_rank") == HANDCUFF_RANK)
        strength = team_strength(season, schedules)
        rows.append(
            second.with_columns(pl.col("team").replace(TEAM_ALIASES).alias("pro_team"))
            .join(strength, on="pro_team", how="inner")
            .select("team_strength", pl.col("carries").cast(pl.Float64)))

    if not rows:
        return None
    fitted = pl.concat(rows).drop_nulls()
    if fitted.height < MIN_FIT_ROWS:
        return None

    x = fitted["team_strength"].to_numpy()
    y = fitted["carries"].to_numpy()
    slope, intercept = np.polyfit(x, y, 1)
    predicted = intercept + slope * x
    total = ((y - y.mean()) ** 2).sum()
    r_squared = 1.0 - ((y - predicted) ** 2).sum() / total if total else 0.0
    return (float(intercept), float(slope), float(r_squared),
            float(np.std(y - predicted)))


def attach_handcuff(board: pl.DataFrame, season: int,
                    points_column: str = "TRUE_Points") -> pl.DataFrame:
    """Add team strength, backfield rank and handcuff value to a board.

    Backfield rank comes from the board's **own** projection rather than from a depth
    chart, so it is consistent with everything else on the page: the back this league
    projects second is the back this league would be handcuffing.

    Args:
        board: A built board, with ``pro_team``, ``primaryPosition`` and
            ``points_column``.
        season: Season being drafted.
        points_column: Column that orders the backfield.

    Returns:
        pl.DataFrame: ``board`` plus ``team_strength``, ``backfield_rank``,
        ``handcuff_carries``, ``handcuff_premium`` and ``handcuff_r2``. Returned
        unchanged when the schedule or the fit is unavailable.
    """
    if any(c not in board.columns
           for c in ("pro_team", "primaryPosition", points_column)):
        return board

    try:
        strength = team_strength(season)
    except FileNotFoundError:
        return board
    if strength.is_empty():
        return board

    fitted = fit_rb2_carries()
    if fitted is None:
        return board
    intercept, slope, r_squared, _ = fitted

    out = board.join(strength, on="pro_team", how="left")

    # Rank only the backs, and only those the board actually projects. A null-points
    # back ranking above a projected one would name the wrong handcuff.
    #
    # Free agents are excluded rather than ranked. ESPN gives an unrostered player a
    # `pro_team` of the literal string "None", which groups all of them into a
    # thirty-third phantom backfield with its own RB1 and RB2 -- and a "handcuff" to
    # a team that does not exist is worse than no column at all. It resolved to a null
    # strength anyway, so this changes the rank rather than the value; the point is
    # that the rank should not have been there either.
    on_a_team = pl.col("pro_team").is_not_null() & (pl.col("pro_team") != "None")
    is_back = ((pl.col("primaryPosition") == "RB")
               & pl.col(points_column).is_not_null()
               & on_a_team)
    out = out.with_columns(
        pl.when(is_back)
        .then(pl.col(points_column).rank("ordinal", descending=True)
              .over(["pro_team", "primaryPosition"]))
        .otherwise(None)
        .cast(pl.Int32)
        .alias("backfield_rank"))

    # The fitted expectation for *this* team's number-two back, and how far that sits
    # from the league-average one. The premium is the whole point: the level is
    # dominated by how many carries a backfield has, the premium is what strength
    # buys.
    average = intercept + slope * float(
        strength["team_strength"].mean() or 0.0)
    expected = intercept + slope * pl.col("team_strength")

    return out.with_columns(
        pl.when(pl.col("backfield_rank") == HANDCUFF_RANK)
        .then(expected).otherwise(None).alias("handcuff_carries"),
        pl.when(pl.col("backfield_rank") == HANDCUFF_RANK)
        .then(expected - average).otherwise(None).alias("handcuff_premium"),
        # Carried so the column cannot be read as more than it is: strength explains
        # 3% of RB2 carry variance, against a residual standard deviation of 36
        # carries. This is a tiebreaker between similar backups.
        pl.lit(r_squared).alias("handcuff_r2"),
    )
