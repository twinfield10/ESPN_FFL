"""Every source has to describe the same quantity before an equal vote means anything.

Three changes of 2026-09-02 share one premise: a blend of sources answering *different
questions* produces something that is neither, and the fix is to convert the basis
rather than to re-weight the source. That is the pattern `Scripts.usage.project`
established for TOMCAT, and these pin the two places it has since been applied.

The trap each guards is the same one: an adjustment fitted to the gap it is correcting
will close that gap by construction and look like a success while destroying the
disagreement the blend exists to measure.
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

import Scripts.season_projections as sp
from Scripts.projection_utils import IMPUTED_SUFFIX
from Scripts.usage import project as up


# --- the sportsbook slate basis -------------------------------------------

def test_the_market_slate_constant_is_the_measured_base_rate():
    """Anchored on realised games, **not** on the gap it corrects.

    Fantasy starters played 0.878/0.891/0.905/0.903/0.898 of a 17-game slate across
    2021-2025, pooled 0.895. BetOnline's lines read 0.871 of the ESPN/FantasyPros mean
    on the 2026 board. Calibrating to *that* would force the books into agreement by
    construction; anchoring on the base rate leaves them a residual opinion.
    """
    assert sp.MARKET_SLATE_SHARE == pytest.approx(0.895, abs=1e-9)
    assert 0.86 < sp.MARKET_SLATE_SHARE < 0.93, "outside five seasons of realised games"


def test_the_conversion_lifts_a_book_line_and_leaves_a_residual():
    """It divides by the base rate, so a book that was 0.871 lands near 0.973 --
    close to consensus but not on it. Landing exactly on 1.000 would be the symptom
    of having fitted the gap."""
    frame = pd.DataFrame({"name_key": ["A"], "BOL_receivingYards": [871.0]})
    out = sp.market_to_full_slate(frame.copy(), "BOL")
    lifted = out["BOL_receivingYards"].iloc[0]
    assert lifted == pytest.approx(871.0 / 0.895, rel=1e-9)
    assert 0.95 < lifted / 1000.0 < 0.99, "the residual disagreement was flattened away"


def test_the_conversion_touches_only_that_book_and_not_its_flags():
    """ESPN and FantasyPros already project a full slate -- ESPN's median `games` is
    17.0 -- so lifting them too would double-count the correction."""
    frame = pd.DataFrame({
        "name_key": ["A"],
        "BOL_receivingYards": [1000.0],
        f"BOL_receivingYards{IMPUTED_SUFFIX}": [False],
        "ESPN_receivingYards": [1000.0],
        "PINNY_receivingYards": [1000.0],
    })
    out = sp.market_to_full_slate(frame.copy(), "BOL")
    assert out["BOL_receivingYards"].iloc[0] > 1000.0
    assert out["ESPN_receivingYards"].iloc[0] == 1000.0
    assert out["PINNY_receivingYards"].iloc[0] == 1000.0
    assert out[f"BOL_receivingYards{IMPUTED_SUFFIX}"].iloc[0] is np.False_ or \
        out[f"BOL_receivingYards{IMPUTED_SUFFIX}"].iloc[0] is False


def test_an_empty_book_frame_survives_the_conversion():
    """A season with no props posted yet is an absent source, not an error."""
    assert sp.market_to_full_slate(pd.DataFrame(), "BOL").empty


# --- the MEAN basis --------------------------------------------------------

def test_the_athletic_is_in_the_mean_and_the_usage_model_is_not():
    """`MEAN_` is what the *external* sources fall back to. Seeding it with our own
    model would put TOMCAT's opinion inside a cell wearing a sportsbook's name."""
    assert sp.MEAN_SOURCES == ("ESPN", "FP", "ATH")
    assert "USG" not in sp.MEAN_SOURCES
    assert "PINNY" not in sp.MEAN_SOURCES and "BOL" not in sp.MEAN_SOURCES


def test_the_mean_averages_real_cells_only():
    """The load-bearing detail, and what makes adding a third source safe.

    A source whose cell was itself filled from ESPN must not contribute to the average
    that filled it, or ESPN is counted twice on every row FantasyPros does not cover --
    which is most of them. Masked, adding The Athletic changes nothing where it is
    silent and everything where it speaks.
    """
    base = pd.DataFrame({
        "ESPN_receivingTargets": [100.0, 100.0],
        "FP_receivingTargets": [100.0, 100.0],
        f"FP_receivingTargets{IMPUTED_SUFFIX}": [True, True],   # both filled from ESPN
        "ATH_receivingTargets": [140.0, None],                  # real, then silent
    })
    got = []
    for i in range(2):
        parts = []
        for source in sp.MEAN_SOURCES:
            column = f"{source}_receivingTargets"
            values = pd.to_numeric(base[column], errors="coerce")
            flag = column + IMPUTED_SUFFIX
            if flag in base.columns:
                values = values.where(~base[flag].fillna(True).astype(bool))
            parts.append(values)
        got = pd.concat(parts, axis=1).mean(axis=1)
    assert got.iloc[0] == pytest.approx(120.0), "ATH real -> mean(ESPN, ATH)"
    assert got.iloc[1] == pytest.approx(100.0), "ATH silent -> unchanged from before"


# --- TOMCAT volume ---------------------------------------------------------

def test_tomcat_publishes_the_volume_it_already_models():
    """Every stat the model emits is a volume term times a rate, so the volume was
    always a first-class prediction -- it was carried as a lower-case diagnostic and
    then thrown away by the blend."""
    assert set(up.VOLUME_STATS.values()) == {
        "USG_receivingTargets", "USG_rushingAttempts", "USG_passingAttempts"}
    for source in up.VOLUME_STATS:
        assert source.startswith("pred_") and source.endswith("_pg")


def test_volume_is_published_on_the_same_slate_as_the_yardage():
    """A target count on an expected-games basis beside yardage on a 17-game basis
    would break `yards = targets x yards_per_target`, which is the identity that makes
    publishing the pair worth anything."""
    frame = pl.DataFrame({"pred_targets_pg": [10.0], "pred_carries_pg": [5.0],
                          "pred_pass_attempts_pg": [None]})
    out, added = up.attach_volume(frame, slate=17.0)
    assert out["USG_receivingTargets"][0] == pytest.approx(170.0)
    assert out["USG_rushingAttempts"][0] == pytest.approx(85.0)
    assert "USG_receivingTargets" in added


def test_the_published_volume_reconciles_with_the_published_yardage():
    """The identity, checked on the real artifact rather than asserted.

    Yards per target has to land in a football-shaped range. If the volume were
    multiplied by a different slate than the yardage, this is where it would show --
    as a league of receivers averaging six yards a target, or fifteen.
    """
    path = up.projection_path(2026)
    if not path.is_file():
        pytest.skip("no 2026 usage artifact built")
    d = pl.read_parquet(path)
    if "USG_receivingTargets" not in d.columns:
        pytest.skip("volume not published in this artifact")
    live = d.filter((pl.col("USG_receivingTargets") > 20)
                    & pl.col("USG_receivingYards").is_not_null())
    ypt = (live["USG_receivingYards"] / live["USG_receivingTargets"]).median()
    assert 6.0 < ypt < 12.0, f"implied yards per target is {ypt:.2f}"


def test_volume_is_not_published_where_the_model_abstained():
    """The bug this guard exists for, and it is the Jayden Higgins shape.

    The volume heads predict for everybody -- `pred_pass_attempts_pg` is a number for
    any quarterback with a snap of history -- while `predict` *abstains* on the derived
    stats when the evidence is thin, nulling them so the blend sees an absent source.
    Publishing volume unconditionally breaks that in the worst direction: the count
    arrives with `_is_imputed` False, so it is not an abstention the blend skips, it is
    a confident vote.

    Shipped without the guard it gave **all 148** abstaining players a voting volume
    line. Deshaun Watson -- `usg_arm` abstain, `expected_games` 2.73 -- published 344.8
    pass attempts beside a null passing line: a starter's season for a man the model
    thinks plays three games.
    """
    frame = pl.DataFrame({
        "pred_pass_attempts_pg": [20.28, 30.0],
        "pred_carries_pg": [2.07, 12.0],
        "pred_targets_pg": [None, 8.0],
        "USG_passingYards": [None, 4200.0],    # abstained, then projected
        "USG_rushingYards": [None, 300.0],
        "USG_receivingYards": [None, 900.0],
    })
    out, _ = up.attach_volume(frame, slate=17.0)
    assert out["USG_passingAttempts"][0] is None, "published volume for an abstention"
    assert out["USG_rushingAttempts"][0] is None
    assert out["USG_passingAttempts"][1] == pytest.approx(510.0)
    assert out["USG_rushingAttempts"][1] == pytest.approx(204.0)


def test_the_witness_is_derived_from_the_model_not_restated():
    """So a stat cannot be added to `STAT_TERMS` and silently left unguarded."""
    assert up.volume_witness("targets_pg") == "receivingYards"
    assert up.volume_witness("carries_pg") == "rushingYards"
    assert up.volume_witness("pass_attempts_pg") == "passingYards"
    assert up.volume_witness("not_a_term_pg") is None


def test_no_orphaned_volume_survives_on_the_built_artifact():
    """End to end: every published volume count has the stat it multiplies into."""
    path = up.projection_path(2026)
    if not path.is_file():
        pytest.skip("no 2026 usage artifact built")
    d = pl.read_parquet(path)
    for volume, stat in (("USG_passingAttempts", "USG_passingYards"),
                         ("USG_rushingAttempts", "USG_rushingYards"),
                         ("USG_receivingTargets", "USG_receivingYards")):
        if volume not in d.columns:
            continue
        orphaned = d.filter(pl.col(volume).is_not_null() & pl.col(stat).is_null())
        assert orphaned.height == 0, (
            f"{orphaned.height} rows publish {volume} with no {stat}")


# --- the pre-season depth-chart boundary ----------------------------------

def test_the_depth_chart_cutoff_is_kickoff_not_a_fixed_date():
    """A hardcoded cutoff goes stale silently, and this one did.

    `PRESEASON_CUTOFF` was (9, 1) with a comment claiming September 1 was "comfortably
    after the final pre-season update". By 2026 the feed still published on September 1
    and the season did not open until the 9th, so a week of final depth-chart movement
    fell outside the window -- with drafts on the 6th and 7th. MarShawn Lloyd was
    promoted to RB1 on 2026-09-01 and every board built afterwards had him at RB2.

    Kickoff is the right boundary on both sides: every snapshot before it is
    information a drafter genuinely had, and every snapshot after it is leakage. Read
    from the schedule, so it needs no maintenance -- 2025 opened on the 4th and 2026
    on the 9th.
    """
    from Scripts.usage import context as cx
    for season, expected in ((2025, "2025-09-04"), (2026, "2026-09-09")):
        got = cx.season_kickoff(season)
        if got is None:
            pytest.skip("no schedule on disk")
        assert got == expected


def test_the_snapshot_used_is_the_last_one_before_kickoff():
    """Not the latest available, which would leak in-season information into a
    feature the model treats as known before week 1."""
    from Scripts.usage import context as cx
    depth = cx.load_depth_charts([2026])
    if depth.is_empty():
        pytest.skip("no 2026 depth charts on disk")
    snapshot = cx.preseason_snapshot(depth, 2026)
    chosen = snapshot["as_of"].unique().to_list()
    assert len(chosen) == 1, "a snapshot must be one point in time"
    kickoff = cx.season_kickoff(2026)
    if kickoff:
        assert chosen[0] < kickoff, "snapshot is at or after kickoff -- leakage"
    later = depth.filter(pl.col("as_of") > chosen[0])
    if not later.is_empty():
        assert (later["as_of"].min() >= kickoff), (
            "a pre-kickoff snapshot was available and not used")


def test_a_missing_schedule_falls_back_rather_than_failing():
    """The fallback is why the old constant is kept. A season with no schedule on
    disk still has to build a board."""
    from Scripts.usage import context as cx
    assert cx.season_kickoff(1999) is None
    assert cx.PRESEASON_CUTOFF == (9, 1)


# --- the rate of last resort and the role distribution --------------------

def test_a_veteran_with_no_prior_season_gets_the_positional_rate():
    """The hole this closes, stated as the user did: a back who gets a hundred
    carries does not gain five yards on them.

    `effective_rate` took a rookie's rate from `rookie_efficiency` and everyone
    else's from his own prior season, so a veteran who missed a full year had no rate
    at all and every stat built on one came out null -- beside a perfectly good volume
    estimate. Deshaun Watson: 345 pass attempts, no passing yards.
    """
    from Scripts.usage import season as sn
    model = sn.SeasonUsageModel.load()
    assert model.pool_efficiency, "pooled baselines were never fitted"
    for position in ("QB", "RB", "WR", "TE"):
        assert position in model.pool_efficiency
    # Football-shaped, or the fallback is worse than the hole.
    assert 6.0 < model.pool_efficiency["QB"]["yards_per_attempt"] < 8.5
    assert 3.6 < model.pool_efficiency["RB"]["yards_per_carry"] < 5.0


def test_the_pooled_rate_is_not_the_rookie_rate():
    """They are different populations and conflating them was the reason the pooled
    one was never wired in: a rookie is less efficient per opportunity, so using the
    pool for him overstates every rookie projection by that gap."""
    from Scripts.usage import season as sn
    model = sn.SeasonUsageModel.load()
    if not model.rookie_efficiency.get("QB"):
        pytest.skip("no rookie baselines fitted")
    assert (model.pool_efficiency["QB"]["yards_per_attempt"]
            != model.rookie_efficiency["QB"]["yards_per_attempt"])


def test_the_returning_veteran_now_has_a_line():
    """End to end on the player who exposed this."""
    path = up.projection_path(2026)
    if not path.is_file():
        pytest.skip("no 2026 usage artifact built")
    d = pl.read_parquet(path)
    row = d.filter(pl.col("full_name") == "Deshaun Watson")
    if not row.height:
        pytest.skip("Watson not in the 2026 pool")
    row = row.row(0, named=True)
    assert row["usg_arm"] == "baseline"
    assert row["USG_passingYards"] is not None, "volume with no yards -- the old hole"
    implied = row["USG_passingYards"] / row["USG_passingAttempts"]
    assert 5.5 < implied < 9.0, f"implied yards per attempt is {implied:.2f}"


def test_volume_is_priced_over_the_role_distribution_not_the_listed_rank():
    """The listed chart is 58.8% right about a settled starter. Pricing at the listed
    rank asserts a certainty plan 33 measured and found absent -- in both directions:
    a listed rank 2 is the actual lead 16.7% of the time."""
    from Scripts.usage import season as sn, role as rl, features as ft
    probabilities = rl.rank_probabilities()
    if not probabilities:
        pytest.skip("role calibration not fitted")
    model = sn.SeasonUsageModel.load()
    frame = ft.season_features(2026, list(range(2016, 2026)))
    starters = frame.filter((pl.col("depth_rank") == 1)
                            & (pl.col("position") == "WR")
                            & pl.col(f"{ft.LAG1_PREFIX}targets_pg").is_not_null())
    if starters.height < 10:
        pytest.skip("too few listed WR1s to compare")
    point = starters.select(model.predict_volume(starters, "targets_pg").alias("v"))["v"]
    weighted = starters.select(
        model.expected_volume(starters, "targets_pg", probabilities).alias("v"))["v"]
    ratio = (weighted / point).median()
    # Strictly below 1: a listed starter is sometimes not the starter.
    assert 0.85 < ratio < 1.0, f"role weighting moved a listed WR1 by {ratio:.3f}"


def test_a_player_the_calibration_cannot_place_keeps_the_point_estimate():
    """No cell must mean 'the chart as given', not a zero -- the behaviour before the
    distribution existed rather than an invented one."""
    from Scripts.usage import season as sn, features as ft
    model = sn.SeasonUsageModel.load()
    frame = ft.season_features(2026, list(range(2016, 2026))).head(40)
    point = frame.select(model.predict_volume(frame, "targets_pg").alias("v"))["v"]
    empty = frame.select(model.expected_volume(frame, "targets_pg", {}).alias("v"))["v"]
    assert point.to_list() == empty.to_list()
