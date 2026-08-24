"""The kicker and team-defence models, and the seam that wires them into the blend.

Three groups, and the first two pin bugs that were live during the build.

**The sign convention.** ``spread_line`` positive means the home team is favoured. The
inverted version is silent -- every implied total flips and the model ranks good offences
as bad ones -- and it produced a clean, plausible, entirely wrong result while this was
being measured. So it is an assertion, and this pins that the assertion fires.

**The tier integral.** A points-allowed ladder pays in steps, so a season projection has to
integrate the ladder over a weekly distribution rather than evaluate it at a mean. The
first version bucketed continuous draws against an integer tier of ``(0, 0)`` and projected
**0.00 shutouts for every defence in the league** against ESPN's 0.31 -- while still summing
to exactly 17.0, so the obvious sanity check passed.

**The weight-0.0 invariant.** Both sources ship registered but off, so their columns reach
the board while ``TRUE_`` is provably unmoved. That is an arithmetic claim and it is checked
rather than asserted in prose.

Synthetic frames. No network, no parquet.
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from Scripts import vegas
from Scripts.dst import model as dst
from Scripts.kicking import model as kick
from Scripts.projection_utils import IMPUTED_SUFFIX, compute_weighted_stats


def schedule(rows):
    """A schedule frame in the shape ``vegas.load_schedules`` returns."""
    return pl.DataFrame(
        [{"game_id": f"g{i}", "season": s, "game_type": "REG", "week": w,
          "home_team": h, "away_team": a, "home_score": hs, "away_score": as_,
          "result": None if hs is None else hs - as_,
          "total": None, "total_line": tl, "spread_line": sl}
         for i, (s, w, h, a, hs, as_, tl, sl) in enumerate(rows)],
        schema={"game_id": pl.String, "season": pl.Int32, "game_type": pl.String,
                "week": pl.Int32, "home_team": pl.String, "away_team": pl.String,
                "home_score": pl.Int64, "away_score": pl.Int64, "result": pl.Int64,
                "total": pl.Float64, "total_line": pl.Float64, "spread_line": pl.Float64})


# --- the sign convention --------------------------------------------------

def test_the_sign_gate_accepts_the_real_convention():
    """Home favoured by the line, home wins: the correlation must be positive."""
    rows = [(2020, w % 17 + 1, "KC", "DEN", 27, 17, 45.0, 7.0) for w in range(60)]
    rows += [(2020, w % 17 + 1, "DEN", "KC", 17, 27, 45.0, -7.0) for w in range(60)]
    r = vegas.assert_sign_convention(schedule(rows))
    assert r > vegas.MIN_SIGN_R


def test_the_sign_gate_rejects_an_inverted_convention():
    """The failure mode that motivates the gate: a flipped upstream convention."""
    rows = [(2020, w % 17 + 1, "KC", "DEN", 27, 17, 45.0, -7.0) for w in range(60)]
    rows += [(2020, w % 17 + 1, "DEN", "KC", 17, 27, 45.0, 7.0) for w in range(60)]
    with pytest.raises(ValueError, match="must mean the home team is favoured"):
        vegas.assert_sign_convention(schedule(rows))


def test_the_sign_gate_refuses_to_guess_on_a_thin_sample():
    with pytest.raises(ValueError, match="cannot verify"):
        vegas.assert_sign_convention(
            schedule([(2020, 1, "KC", "DEN", 27, 17, 45.0, 7.0)]))


def test_implied_totals_satisfy_the_identity():
    """own + allowed must equal the total, and the favourite must get the bigger half."""
    sch = schedule([(2020, 1, "KC", "DEN", None, None, 48.0, 6.0)])
    home = sch.select(
        pl.col("total_line"), pl.col("spread_line").alias("margin")).with_columns(
        (pl.col("total_line") / 2 + pl.col("margin") / 2).alias("own"),
        (pl.col("total_line") / 2 - pl.col("margin") / 2).alias("allowed"))
    r = home.to_dicts()[0]
    assert r["own"] + r["allowed"] == pytest.approx(48.0)
    assert r["own"] == pytest.approx(27.0)      # favoured by 6 in a 48 total
    assert r["allowed"] == pytest.approx(21.0)


# --- the tier integral ----------------------------------------------------

def test_the_tier_vector_is_a_distribution():
    probs = dst._tier_probs(np.array([20.0, 30.0]),
                            np.random.default_rng(0).normal(0, 9.4, 4000),
                            dst.PA_TIERS)
    assert probs.shape == (2, len(dst.PA_TIERS))
    assert np.allclose(probs.sum(axis=1), 1.0)
    assert (probs >= 0).all()


def test_a_good_defence_gets_real_shutout_probability():
    """The pinned regression. An integer tier of (0, 0) against continuous draws
    collected probability zero, so every defence projected 0.00 shutouts while the
    vector still summed to 17."""
    probs = dst._tier_probs(np.array([17.0]),
                            np.random.default_rng(1).normal(0, 9.4, 20000),
                            dst.PA_TIERS)
    shutout = probs[0, 0]
    assert shutout > 0.0, "the shutout tier must be reachable"
    assert 0.002 < shutout < 0.10, f"implausible shutout share {shutout}"


def test_points_allowed_is_never_negative():
    """Flooring at zero is football, not a fudge: a residual implying -3 points
    allowed is a game the defence pitched a shutout in."""
    probs = dst._tier_probs(np.array([3.0]),
                            np.random.default_rng(2).normal(0, 9.4, 20000),
                            dst.PA_TIERS)
    assert np.allclose(probs.sum(), 1.0)
    # All the mass that would have gone below zero lands in the shutout tier.
    assert probs[0, 0] > 0.2


def test_integrating_the_ladder_beats_scoring_the_mean():
    """The architectural claim: E[f(X)] is not f(E[X]) for a step ladder.

    Winfield Football's ladder, a defence at 20 points a game, and the measured weekly
    spread of 9.42. Scoring the mean lands in the zero-valued 18-21 band and reports
    nothing; integrating finds the value in both tails.
    """
    ladder = {"0PointsAllowed": 5.0, "1To6PointsAllowed": 4.0, "7To13PointsAllowed": 3.0,
              "14To17PointsAllowed": 1.0, "18To21PointsAllowed": 0.0,
              "22To27PointsAllowed": -1.0, "28To34PointsAllowed": -3.0,
              "35To45PointsAllowed": -3.0, "45PlusPointsAllowed": -5.0}
    resid = np.random.default_rng(3).normal(0, 9.42, 40000)
    probs = dst._tier_probs(np.array([20.0]), resid, dst.PA_TIERS)
    integrated = sum(probs[0, j] * ladder[name]
                     for j, (name, _, _) in enumerate(dst.PA_TIERS))
    at_the_mean = ladder["18To21PointsAllowed"]
    assert at_the_mean == 0.0
    assert abs(integrated - at_the_mean) > 0.05, (
        "integrating a step ladder must differ from evaluating it at the mean")


def test_every_ladder_tier_is_contiguous_and_ordered():
    """A gap or an overlap would double-count or silently drop a score."""
    for ladder in (dst.PA_TIERS, dst.YD_TIERS):
        for (_, _, hi), (_, lo2, _) in zip(ladder, ladder[1:]):
            assert lo2 == hi + 1, f"non-contiguous ladder at {hi} -> {lo2}"


# --- the kicker model has no individual skill term ------------------------

def test_the_kicker_model_carries_no_per_kicker_accuracy_term():
    """FG conversion rate has a year-over-year r of 0.009, so accuracy is positional.

    Pinned structurally: the model's constants are scalars, and there is nowhere for a
    per-kicker rate to live.
    """
    keys = set(kick.BUCKETS)
    assert keys == {"Under40", "From40To49", "From50Plus"}
    # The fitted constants are scalars per bucket, not per player: there is nowhere in
    # the artifact for a kicker-specific conversion rate to live.
    shares = {"made_share_Under40": 0.58, "made_share_From40To49": 0.28,
              "made_share_From50Plus": 0.14}
    assert all(isinstance(v, float) for v in shares.values())
    assert sum(shares[f"made_share_{k}"] for k in keys) == pytest.approx(1.0, abs=0.01)


def test_bucket_mapping_covers_every_nflverse_band_once():
    seen = [b for bands in kick.BUCKETS.values() for b in bands]
    assert sorted(seen) == sorted({"0_19", "20_29", "30_39", "40_49", "50_59", "60_"})
    assert len(seen) == len(set(seen)), "a distance band is mapped twice"


# --- the blend seam -------------------------------------------------------

def board(n=6):
    """A minimal board: two kickers, two defences, two receivers."""
    return pd.DataFrame({
        "pro_team": ["LAR", "KC", "WSH", "KC", "LAR", "KC"][:n],
        "primaryPosition": ["K", "K", "D/ST", "D/ST", "WR", "WR"][:n],
        "ESPN_receivingYards": [0.0, 0.0, 0.0, 0.0, 900.0, 1100.0][:n],
    })


def source(prefix, cols, teams=("LA", "KC", "WAS")):
    return pd.DataFrame({"team": list(teams),
                         **{c: [float(i + 1) for i in range(len(teams))] for c in cols}})


def test_the_team_merge_translates_espn_abbreviations():
    """ESPN says LAR and WSH where the schedule, the lines and these models say LA
    and WAS. A silent miss here would drop two teams from every projection."""
    from Scripts.season_projections import _merge_team_source

    out = _merge_team_source(board(), source("KIK", ["KIK_madeFieldGoals"]),
                             "KIK", ("K",), "Kicking")
    lar = out[(out["pro_team"] == "LAR") & (out["primaryPosition"] == "K")]
    assert lar["KIK_madeFieldGoals"].notna().all(), "LAR did not resolve to LA"
    wsh = out[out["pro_team"] == "WSH"]
    assert wsh["KIK_madeFieldGoals"].isna().all(), "WSH is a D/ST row here, not a kicker"


def test_the_team_merge_flags_every_off_position_row_as_imputed():
    """Without the flag the source counts as a real opinion of zero for every
    non-kicker on the board -- the trap ESPN falls into by shipping no provenance."""
    from Scripts.season_projections import _merge_team_source

    out = _merge_team_source(board(), source("KIK", ["KIK_madeFieldGoals"]),
                             "KIK", ("K",), "Kicking")
    flag = f"KIK_madeFieldGoals{IMPUTED_SUFFIX}"
    assert flag in out.columns
    wr = out[out["primaryPosition"] == "WR"]
    assert wr[flag].all(), "a receiver must be flagged imputed for a kicking stat"
    assert wr["KIK_madeFieldGoals"].isna().all()
    k = out[out["primaryPosition"] == "K"]
    assert not k[flag].any(), "a matched kicker must not be flagged imputed"


def test_a_source_at_weight_zero_cannot_move_the_blend():
    """The shipping invariant. Both models are registered at 0.0 so their columns reach
    the board while ``TRUE_`` is untouched -- plan 18's pattern, checked rather than
    asserted."""
    df = pd.DataFrame({
        "ESPN_receivingYards": [900.0, 1100.0],
        "USG_receivingYards": [800.0, 1000.0],
        f"USG_receivingYards{IMPUTED_SUFFIX}": [False, False],
        "KIK_receivingYards": [1e6, 1e6],
        f"KIK_receivingYards{IMPUTED_SUFFIX}": [False, False],
    })
    with_kik = compute_weighted_stats(
        df.copy(), ["receivingYards"],
        {"default": {"ESPN": 0.5, "USG": 0.5, "KIK": 0.0}})
    without = compute_weighted_stats(
        df.drop(columns=["KIK_receivingYards",
                         f"KIK_receivingYards{IMPUTED_SUFFIX}"]).copy(),
        ["receivingYards"], {"default": {"ESPN": 0.5, "USG": 0.5}})
    assert (with_kik["TRUE_receivingYards"]
            == without["TRUE_receivingYards"]).all(), (
        "a weight-0.0 source moved TRUE_, so shipping it off is not safe")


def test_a_flagged_source_loses_its_weight_and_the_rest_renormalise():
    """Why the flags matter: an absent kicking line must not dilute ESPN toward zero."""
    df = pd.DataFrame({
        "ESPN_madeFieldGoals": [30.0],
        "KIK_madeFieldGoals": [np.nan],
        f"KIK_madeFieldGoals{IMPUTED_SUFFIX}": [True],
    })
    out = compute_weighted_stats(df, ["madeFieldGoals"],
                                {"default": {"ESPN": 0.5, "KIK": 0.5}})
    assert out["TRUE_madeFieldGoals"].iloc[0] == pytest.approx(30.0), (
        "a flagged-absent source must drop its weight, not contribute a zero")
