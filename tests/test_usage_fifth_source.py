"""The usage model as the blend's fifth source.

The model itself is pinned in ``test_usage_season.py``. What is pinned here is the
*plumbing* -- registration, the id-first join, and above all the handling of
abstention, which is where this wiring can go wrong quietly.

The recurring failure mode in this repo is an absent source reading as agreement: a
``0.0`` that means "nothing here" is indistinguishable from one that means "zero",
and every count built on ``notna()`` reads the first as the second. It has cost a
draft board once already (``docs/STATE_OF_THE_REPO.md``). This source abstains on
roughly a quarter of the players it sees, so it walks straight into that trap unless
the provenance flags are right -- and a test that only checks "the column exists"
would not notice.

Synthetic frames throughout. No network, no parquet.
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from Scripts import projection_utils as pu
from Scripts import season_projections as sp
from Scripts.usage import project as pj
from Scripts.usage import season as sn

STAT = "receivingYards"


def blend_frame(usg_value, usg_imputed, espn=100.0, fp=120.0):
    """One player, ESPN and FantasyPros real, USG as specified."""
    return pd.DataFrame({
        f"ESPN_{STAT}": [espn],
        f"FP_{STAT}": [fp],
        f"FP_{STAT}{pu.IMPUTED_SUFFIX}": [False],
        f"USG_{STAT}": [usg_value],
        f"USG_{STAT}{pu.IMPUTED_SUFFIX}": [usg_imputed],
    })


def weights(usg):
    return {"default": {"ESPN": 0.5, "FP": 0.5, "USG": usg}}


# --- registration --------------------------------------------------------

def test_usg_is_registered_in_the_blend_weights():
    for stat, entry in pu.WEIGHTS.items():
        assert "USG" in entry, f"{stat} has no USG entry"


def test_the_blend_is_an_equal_three_way_split():
    """Set deliberately on 2026-08-07, replacing the hand-tuned four-source table.

    ESPN, FantasyPros and the usage model at a third each; Pinnacle and BetOnline at
    zero. G2 is still unanswered -- it needs the blend scored with and without the
    model against realised results, and no historical pre-season blend survives -- so
    this is an assertion rather than a measurement. It is an assertion with a
    seven-fold walk-forward behind it, and it is recorded as a decision rather than
    inherited as a default."""
    entry = pu.WEIGHTS["default"]
    assert entry["ESPN"] == entry["FP"] == entry["USG"] == pytest.approx(1 / 3)
    assert entry["PINNY"] == 0.0
    assert entry["BOL"] == 0.0


def test_every_source_still_has_an_entry():
    """A source dropped from the dict is invisible; a source at 0.0 is a decision.
    BetOnline in particular resolves 273 players against FantasyPros' 60, so its zero
    should stay legible rather than vanish."""
    for source in ("ESPN", "FP", "PINNY", "BOL", "USG"):
        assert source in pu.WEIGHTS["default"], source


def test_usg_is_scored_like_every_other_source():
    import inspect
    default = inspect.signature(pu.proj_to_score).parameters["col_pfix_list"].default
    assert "USG" in default


def test_usg_stays_out_of_the_floor_ceiling_spread():
    """Independence is not the property this spread needs -- a shared question is.

    G0 measured USG as the most independent source in the set, which is why it was
    briefly added here, and that was the wrong test. `USG_Points` is an expected
    value and the other four project a healthy season, so it sat below all of them
    for 51.7% of the players it covered and widened the median interval from 8.5% to
    24.0%. Disagreement between forecasters and uncertainty within one forecast are
    different quantities; this column holds the first."""
    assert "USG" not in sp.OPINION_PREFIXES
    assert "MEAN" not in sp.OPINION_PREFIXES
    assert set(sp.OPINION_PREFIXES) == {"ESPN", "FP", "PINNY", "BOL"}


def test_the_models_dissent_is_carried_scale_free():
    """Removing USG from the spread must not lose its opinion -- the rank delta is
    the vehicle, and being a rank it cannot be contaminated by the level mismatch."""
    import inspect
    body = inspect.getsource(sp.build_season_projections)
    assert "USG_PosRank" in body
    assert "USG_PosRankDelta" in body


# --- the zero weight -----------------------------------------------------

def test_a_zero_weight_source_cannot_move_the_blend():
    """Verified on the real board too: all 45 TRUE_ columns bit-identical over
    1,026 rows. This pins it so a later weight re-tune cannot change it by
    accident."""
    frame = blend_frame(999.0, False)
    with_usg = pu.compute_weighted_stats(frame.copy(), [STAT], weights(0.0))
    without = pu.compute_weighted_stats(
        frame.copy(), [STAT], {"default": {"ESPN": 0.5, "FP": 0.5}})
    assert with_usg[f"TRUE_{STAT}"][0] == pytest.approx(without[f"TRUE_{STAT}"][0])
    assert with_usg[f"TRUE_{STAT}"][0] == pytest.approx(110.0)


def test_turning_the_weight_on_is_one_number():
    """The point of shipping at 0.0 rather than not shipping: G2 is answerable by
    changing a constant, not by building the source again."""
    frame = blend_frame(200.0, False)
    out = pu.compute_weighted_stats(frame, [STAT], weights(1.0))
    # (100*.5 + 120*.5 + 200*1.0) / 2.0
    assert out[f"TRUE_{STAT}"][0] == pytest.approx(155.0)


# --- abstention ----------------------------------------------------------

def test_a_flagged_abstention_is_dropped_from_the_blend():
    frame = blend_frame(None, True)
    out = pu.compute_weighted_stats(frame, [STAT], weights(1.0))
    assert out[f"TRUE_{STAT}"][0] == pytest.approx(110.0)


def test_an_unflagged_abstention_would_poison_the_blend():
    """Why the flags are not optional, stated as an executable fact.

    `compute_weighted_stats` treats a source with no provenance column as real, and
    fills a null with 0.0. So an abstention that arrived unflagged enters as a
    confident projection of zero and drags the player toward it. This is the
    behaviour `Scripts.usage.project.build` exists to prevent."""
    frame = blend_frame(None, True).drop(columns=[f"USG_{STAT}{pu.IMPUTED_SUFFIX}"])
    out = pu.compute_weighted_stats(frame, [STAT], weights(1.0))
    assert out[f"TRUE_{STAT}"][0] == pytest.approx(55.0)
    assert out[f"TRUE_{STAT}"][0] < 110.0


def test_the_builder_flags_every_abstention():
    """The flag is derived from the null, so the two cannot drift apart."""
    frame = pd.DataFrame({
        "USG_receivingYards": [None, 500.0],
        "USG_receivingYards_is_imputed": [True, False],
    })
    assert frame["USG_receivingYards"].isna().tolist() == \
        frame["USG_receivingYards_is_imputed"].tolist()


def test_usg_is_not_imputed_from_the_mean():
    """Filling this source's gaps from MEAN_ -- the ESPN/FantasyPros average --
    would make the one source that is not somebody else's projection into a copy of
    two that are. That is the double-counting plan 03 measured for Pinnacle."""
    import inspect
    body = inspect.getsource(sp.build_season_projections)
    imputed = [line for line in body.splitlines()
               if "impute_columns" in line and "target_prefix" in line]
    assert imputed, "expected the imputation chain to still exist"
    assert not any("USG_" in line for line in imputed)


def test_a_wholly_abstaining_source_scores_null_not_zero():
    """`USG_Points` of 0.0 would read on the board as 'the model projects zero',
    which is the opposite of 'the model said nothing'."""
    df = pd.DataFrame({"USG_receivingYards": [np.nan], "USG_rushingYards": [np.nan]})
    scoring = pd.DataFrame({"colName": ["receivingYards", "rushingYards"],
                            "points": [0.1, 0.1]})
    out = pu._apply_scoring(df, scoring, ["USG"])
    assert pd.isna(out["USG_Points"][0])


def test_a_partial_line_still_scores():
    """A receiver has no rushing line and that is not an abstention."""
    df = pd.DataFrame({"USG_receivingYards": [1000.0], "USG_rushingYards": [np.nan]})
    scoring = pd.DataFrame({"colName": ["receivingYards", "rushingYards"],
                            "points": [0.1, 0.1]})
    out = pu._apply_scoring(df, scoring, ["USG"])
    assert out["USG_Points"][0] == pytest.approx(100.0)


# --- the id-first join ---------------------------------------------------

def base_frame():
    return pd.DataFrame({
        "player_id": [1, 2, 3],
        "join_key": ["alpha", "bravo", "charlie"],
        "ESPN_receivingYards": [10.0, 20.0, 30.0],
    })


def usage_frame(rows):
    return pd.DataFrame(rows)


def test_the_join_prefers_the_espn_id():
    """Every other season source joins on a name, which is why
    `_disambiguate_name_keys` has to exist: a wide IDP pool holds two Lamar Jacksons
    and two Justin Jeffersons. This source can avoid that."""
    source = usage_frame([{"player_id": 3.0, "name_key": "alpha",
                           "USG_receivingYards": 300.0}])
    out = sp._merge_usage(base_frame(), source)
    # Resolved by id to player 3, not by its (misleading) name to player 1.
    assert out.loc[out["player_id"] == 3, "USG_receivingYards"].iloc[0] == 300.0
    assert pd.isna(out.loc[out["player_id"] == 1, "USG_receivingYards"].iloc[0])


def test_the_join_falls_back_to_the_name_when_the_id_is_missing():
    """The crosswalk carries no 2026 rookies -- 95 unresolved, every one a rookie,
    and rookies are the arm's one clearly measured win."""
    source = usage_frame([{"player_id": np.nan, "name_key": "bravo",
                           "USG_receivingYards": 200.0}])
    out = sp._merge_usage(base_frame(), source)
    assert out.loc[out["player_id"] == 2, "USG_receivingYards"].iloc[0] == 200.0


def test_the_name_fallback_inherits_the_collision_protection():
    """It matches on `join_key`, which `_disambiguate_name_keys` has already set to
    a sentinel for the non-primary holder of a shared name. So the fallback either
    finds the player the book sources found, or nobody."""
    base = base_frame()
    base.loc[1, "join_key"] = None          # the shadowed Lamar Jackson
    source = usage_frame([{"player_id": np.nan, "name_key": "bravo",
                           "USG_receivingYards": 200.0}])
    out = sp._merge_usage(base, source)
    assert pd.isna(out.loc[out["player_id"] == 2, "USG_receivingYards"].iloc[0])


def test_the_merge_cannot_fan_the_frame_out():
    """The crosswalk records 13 duplicated espn_id, and a name fallback can collide
    with an already-resolved row. Either would multiply rows on merge."""
    source = usage_frame([
        {"player_id": 1.0, "name_key": "alpha", "USG_receivingYards": 100.0},
        {"player_id": 1.0, "name_key": "delta", "USG_receivingYards": 111.0},
    ])
    out = sp._merge_usage(base_frame(), source)
    assert len(out) == 3


def test_an_unresolvable_row_is_dropped_rather_than_guessed():
    source = usage_frame([{"player_id": np.nan, "name_key": "nobody",
                           "USG_receivingYards": 900.0}])
    out = sp._merge_usage(base_frame(), source)
    assert len(out) == 3
    assert out["USG_receivingYards"].isna().all()


def test_the_merge_does_not_leak_the_name_key():
    source = usage_frame([{"player_id": 1.0, "name_key": "alpha",
                           "USG_receivingYards": 100.0}])
    out = sp._merge_usage(base_frame(), source)
    assert "name_key" not in out.columns


# --- model freshness -----------------------------------------------------

def test_a_model_trained_through_last_season_is_current():
    model = sn.SeasonUsageModel(volume={}, train_seasons=(2017, 2025))
    assert not pj.is_stale(model, 2026)


def test_a_model_missing_a_season_it_could_have_had_is_stale():
    """Caught a real case: the persisted artifact trained on 2017-2024, because it
    was written by a walk-forward whose last fold predicted 2025."""
    model = sn.SeasonUsageModel(volume={}, train_seasons=(2017, 2024))
    assert pj.is_stale(model, 2026)


def test_a_model_with_no_training_range_is_stale():
    assert pj.is_stale(sn.SeasonUsageModel(volume={}, train_seasons=()), 2026)


def test_coverage_and_disagreement_use_different_source_lists():
    """They answer different questions and merging them is a live bug.

    The floor/ceiling spread needs sources measuring the *same quantity*, so it
    excludes USG -- an expected value against four if-healthy projections. Coverage
    needs every source that moves `TRUE_Points`, so it includes it. With USG weighted
    into the blend but missing from the coverage list, a player only the usage model
    projects gets a real TRUE_Points and `projection_missing = True`: the board would
    hide, as unprojected, exactly the players the model exists to differentiate.

    Measured on the 2026 board when this was wrong: 523 players counted as projected
    against the correct 699.
    """
    assert "USG" in sp.PROJECTION_PREFIXES
    assert "USG" not in sp.OPINION_PREFIXES
    assert set(sp.OPINION_PREFIXES) < set(sp.PROJECTION_PREFIXES)


def test_the_blend_receives_an_if_healthy_line():
    """The model predicts expected value; the blend needs the same quantity its other
    sources carry. Mixing them distorted cross-position comparison by ~11%, because
    the usage model covers QB/RB/WR/TE and not K or D/ST -- so skill positions took an
    availability discount that kickers and defences did not."""
    frame = pl.DataFrame({
        "expected_games": [13.6, 17.0, None],
        "USG_receivingYards": [1000.0, 1000.0, None],
    })
    out = pj.to_full_slate(frame, ["USG_receivingYards"], slate=17.0)
    got = out["USG_receivingYards"].to_list()
    assert got[0] == pytest.approx(1000.0 * 17.0 / 13.6)
    assert got[1] == pytest.approx(1000.0)          # already a full slate
    assert got[2] is None                            # an abstention stays absent


def test_rescaling_cannot_divide_by_no_games():
    """A zero expected-games would divide a projection by nothing. Those rows are
    abstentions and carry a null line already, but the guard is cheaper than the
    infinity it prevents."""
    frame = pl.DataFrame({"expected_games": [0.0],
                          "USG_receivingYards": [1000.0]})
    out = pj.to_full_slate(frame, ["USG_receivingYards"], slate=17.0)
    assert out["USG_receivingYards"][0] is None


# --- current injuries -----------------------------------------------------

def injury_frame(statuses):
    frame = blend_frame(500.0, False)
    frame = pd.concat([frame] * len(statuses), ignore_index=True)
    frame["injury_status"] = list(statuses)
    return frame


def test_the_model_withdraws_for_players_espn_lists_out():
    """It cannot see a current injury and the other sources can.

    nflreadr refuses 2026 injuries outright, so `expected_games` is built from
    prior-season availability -- statistics about a player who was healthy last
    August. ESPN knows today that Ricky Pearsall is on IR for the season.

    Left alone the model overrode that in the worst direction: across the 22 players
    ESPN listed OUT or IR on the 2026 board, adding USG lifted the blend by a mean of
    +15.7 points while lowering active draftable players by 2.7. ESPN and FantasyPros
    both projected Pearsall at 0.0 and the model pulled the blend to 72.4.
    """
    out = sp._abstain_on_injury(injury_frame(["OUT", "INJURY_RESERVE", "ACTIVE"]))
    values = out[f"USG_{STAT}"].tolist()
    assert pd.isna(values[0]) and pd.isna(values[1])
    assert values[2] == 500.0


def test_the_withdrawal_is_flagged_not_merely_nulled():
    """A null without its flag enters the blend as a confident zero -- the trap this
    whole source is threaded to avoid. Flagged, the weight is dropped instead."""
    out = sp._abstain_on_injury(injury_frame(["OUT"]))
    assert bool(out[f"USG_{STAT}{pu.IMPUTED_SUFFIX}"].iloc[0]) is True
    blended = pu.compute_weighted_stats(out, [STAT], weights(1.0))
    assert blended[f"TRUE_{STAT}"][0] == pytest.approx(110.0)


def test_questionable_is_not_withdrawn_on():
    """Pre-season it is week-to-week noise on 64 players, and abstaining would
    discard the model across a large slice of the pool on a signal that says little
    about the season."""
    assert "QUESTIONABLE" not in sp.INJURY_ABSTAIN_STATUSES
    out = sp._abstain_on_injury(injury_frame(["QUESTIONABLE"]))
    assert out[f"USG_{STAT}"].iloc[0] == 500.0


def test_a_frame_without_injury_status_is_untouched():
    frame = blend_frame(500.0, False)
    assert sp._abstain_on_injury(frame)[f"USG_{STAT}"].iloc[0] == 500.0
