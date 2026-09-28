"""Points and yards allowed on ESPN's definitions (plan 51).

Two groups.

**The rules, on a synthetic game.** A pick-six costs the defence the conversion but not the
six; a punt-return touchdown is charged in full, since the D/ST is its own unit; a safety
conceded by the offence is not charged; sacks count negative in yards allowed and so do
kneels. Each of these is a line in the definition and each was wrong somewhere before.

**The reproduction, on local data.** ESPN's own 2025 tiers are reproduced exactly -- 431 of
431 and 357 of 357. That is a stronger check than any synthetic case, and it is the one
that would catch nflverse changing a column underneath us. Skipped where the local store
is absent.
"""

import polars as pl
import pytest

from Scripts import paths
from Scripts.dst import allowed

COLS = ["play_id", "posteam", "defteam", "play_type", "special_teams_play", "yards_gained",
        "sack", "passing_yards", "rushing_yards", "touchdown", "td_team", "safety",
        "total_home_score", "total_away_score"]

#: KC at home to DEN. Scores are *after* each play.
PLAYS = [
    (1, "KC", "DEN", "pass", 0, 20, 0, 20, None, 0, None, 0, 0, 0),
    (2, "KC", "DEN", "pass", 0, -8, 1, None, None, 0, None, 0, 0, 0),       # sack
    (3, "KC", "DEN", "pass", 0, 0, 0, None, None, 1, "DEN", 0, 0, 6),       # pick-six
    (4, "DEN", "KC", "extra_point", 1, 0, 0, None, None, 0, None, 0, 0, 7),
    (5, "DEN", "KC", "run", 0, 10, 0, None, 10, 0, None, 0, 0, 7),
    (6, "DEN", "KC", "pass", 0, -7, 1, None, None, 0, None, 0, 0, 7),       # sack
    (7, "DEN", "KC", "qb_kneel", 0, -1, 0, None, -1, 0, None, 0, 0, 7),
    (8, "KC", "DEN", "punt", 1, 0, 0, None, None, 1, "DEN", 0, 0, 13),      # punt return TD
    (9, "DEN", "KC", "extra_point", 1, 0, 0, None, None, 0, None, 0, 0, 14),
    (10, "KC", "DEN", "pass", 0, -5, 1, None, None, 0, None, 1, 0, 16),     # safety
]


def game() -> pl.DataFrame:
    df = pl.DataFrame(PLAYS, schema=COLS, orient="row")
    return df.with_columns(pl.lit(2025).alias("season"), pl.lit("REG").alias("season_type"),
                           pl.lit("g1").alias("game_id"), pl.lit(1).alias("week"),
                           pl.lit("KC").alias("home_team"), pl.lit("DEN").alias("away_team"))


def side(team: str) -> dict:
    return allowed.allowed_from_pbp(game()).filter(pl.col("team") == team).row(0, named=True)


def test_points_allowed_drops_the_six_of_a_pick_six_and_the_safety_but_not_the_return():
    kc = side("KC")
    assert kc["points_allowed_all"] == 16
    assert kc["ret_td_vs_off"] == 1, "the punt return is special teams, not the offence"
    assert kc["safeties_vs_off"] == 1
    # 16 - 6 (pick-six; its extra point stays charged) - 2 (safety) = 8. The punt return
    # touchdown and its conversion are charged in full.
    assert kc["points_allowed"] == 8


def test_yards_allowed_is_net_of_sacks_and_counts_kneels():
    assert side("KC")["yards_allowed"] == 10 - 7 - 1
    assert side("KC")["yards_allowed_gross"] == 10 - 1
    # KC's own offence: 20 gained, sacked for -8 and -5. The pick-six and punt are not
    # yardage the defence allowed.
    assert side("DEN")["yards_allowed"] == 20 - 8 - 5
    assert side("DEN")["yards_allowed_gross"] == 20


def test_the_side_that_scored_nothing_allowed_nothing():
    den = side("DEN")
    assert den["points_allowed"] == den["points_allowed_all"] == 0


def test_a_safety_is_charged_to_the_side_whose_opponent_scored_it():
    """Read from the score change, not from possession: the rare end-zone interception
    downed by the defence concedes two to the team that had the ball."""
    df = game().with_columns(
        pl.when(pl.col("play_id") == 10).then(pl.lit("DEN")).otherwise(pl.col("posteam"))
        .alias("posteam"))
    out = allowed.allowed_from_pbp(df)
    assert out.filter(pl.col("team") == "KC")["safeties_vs_off"].item() == 1
    assert out.filter(pl.col("team") == "DEN")["safeties_vs_off"].item() == 0


LOCAL = (paths.DATA_DIR / "NFL" / "2025" / "pbp.parquet").is_file() and any(
    (paths.DATA_DIR / "Store" / "2025").glob("*/lineups.parquet"))


@pytest.mark.skipif(not LOCAL, reason="needs 2025 play-by-play and league stores")
def test_espn_2025_tiers_are_reproduced_exactly():
    from Scripts.dst import evidence_51

    d = evidence_51.definitions(2025)
    espn = d.filter(pl.col("definition").str.ends_with("(ESPN)"))
    assert espn.height == 2
    for r in espn.iter_rows(named=True):
        assert r["n"] > 300, f"too few ESPN tiers to mean anything: {r}"
        assert r["matches"] == r["n"], f"{r['quantity']}: {r['matches']}/{r['n']}"
    # And the definitions plan 30 used do not, which is the point of the change.
    old = d.filter(pl.col("definition").is_in(
        ["opponent final score", "gross (player pass + rush yards)"]))
    assert (old["rate"] < 0.95).all()
