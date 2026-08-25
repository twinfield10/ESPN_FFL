"""What the room-level draw must not get wrong.

The accounting is the thing to pin: work vacated by an absent lead must reach the players
who are actually there, in the fitted proportions, and none of it may be invented. Two
subtler properties get their own tests because both were real bugs found while building
this -- a transfer that quietly *raised* every backup's projection, and a control cohort
that was receiving the treatment.

Synthetic frames and hand-built rooms. No network, no parquet, no depth chart on disk.
"""

import numpy as np
import polars as pl
import pytest

from Scripts.outcomes import simulate as sim
from Scripts.usage import predictive as pv
from Scripts.usage import season as sn


# --- fixtures ------------------------------------------------------------

def model(kappa=1000.0, elasticity=1.0):
    """A model whose availability is nearly deterministic, so draws are readable."""
    return sn.SeasonUsageModel(
        volume={},
        games_dispersion={"RB": kappa, "TE": kappa},
        games_elasticity={pv.key("RB", s): elasticity for s in sn.STAT_OUTCOMES},
        version="1.2.0")


def frame(expected):
    """One RB room, ``expected`` games each."""
    return pl.DataFrame(
        {"gsis_id": [f"00-{i}" for i in range(len(expected))],
         "position": ["RB"] * len(expected),
         "expected_games": [float(e) for e in expected],
         "pred_carries_pg": [10.0] * len(expected),
         "pred_targets_pg": [0.0] * len(expected)})


def room(n):
    return [sim.Room(team="AAA", position="RB",
                     players=tuple(range(n)), rank=tuple(range(1, n + 1)))]


SHARES = {"RB": {"rank_2": 0.40, "rank_rest": 0.40}}


def modulate(expected, shares=SHARES, n_sims=400, transfer=True, centre=True,
             rooms=None, kappa=1000.0):
    f = frame(expected)
    return sim.opportunity_multiplier(
        np.random.default_rng(0), f, rooms if rooms is not None else room(len(expected)),
        shares, model(kappa), 17, n_sims, sim.baseline_opportunity(f),
        transfer=transfer, centre=centre)


# --- availability --------------------------------------------------------

def test_a_player_expected_to_play_every_week_plays_every_week():
    """The multiplier is weeks played over weeks expected, so a fully available player
    sits at 1.0 -- 'the season the model projected'."""
    got = modulate([17.0, 17.0], transfer=False)
    assert got.availability.mean() == pytest.approx(1.0, abs=0.01)


def test_the_number_of_weeks_drawn_matches_the_games_drawn():
    """The count comes from the Beta-Binomial and the *identity* of the missed weeks is
    an exchangeable subset. If those two ever disagree, a player is available for a
    number of weeks he was not drawn to play."""
    rng = np.random.default_rng(1)
    available = sim.draw_weeks(rng, np.array([8.0, 17.0]), np.array([2.5, 2.5]), 17, 300)
    assert available.shape == (300, 2, 17)
    assert available[:, 1, :].all()                      # 17 expected, kappa 2.5
    assert 0 < available[:, 0, :].sum(axis=1).mean() < 17


# --- the transfer --------------------------------------------------------

def test_a_vacancy_reaches_the_understudy_in_the_fitted_proportion():
    """The lead is out all season, so the men behind him inherit in the fitted shares.

    Three in the room, identical baselines: the understudy takes ``rank_2`` and the
    third back takes all of ``rank_rest``, since he is the only one it can be split
    among.
    """
    got = modulate([0.001, 17.0, 17.0],
                   rooms=[sim.Room("AAA", "RB", (0, 1, 2), (1, 2, 3))])
    assert got.gain[:, 1].mean() == pytest.approx(0.40, abs=0.02)
    assert got.gain[:, 2].mean() == pytest.approx(0.40, abs=0.02)


def test_the_only_man_behind_the_lead_takes_every_share_there_is():
    """``rank_rest`` is split among whoever is available, so in a two-man room that is
    the understudy as well. The room recaptures what the rule says it recaptures,
    regardless of how many bodies are there to do it."""
    got = modulate([0.001, 17.0])
    assert got.gain[:, 1].mean() == pytest.approx(0.80, abs=0.02)


def test_a_lead_who_never_misses_a_game_transfers_nothing():
    """No vacancy, no transfer. The obvious case, and the one that would hide a rule
    firing on the wrong condition."""
    got = modulate([17.0, 17.0])
    assert got.gain.max() == pytest.approx(0.0, abs=1e-9)


def test_an_absent_understudy_cascades_his_share_to_the_rest():
    """If the man who should inherit is himself out, his share does not evaporate and it
    does not sit with the absent player -- it goes to whoever is actually there."""
    both_out = modulate([0.001, 0.001, 17.0])
    assert both_out.gain[:, 1].max() == pytest.approx(0.0, abs=1e-9)
    # rank_rest (0.40) plus the cascaded rank_2 (0.40), all of it to the only man left.
    assert both_out.gain[:, 2].mean() == pytest.approx(0.80, abs=0.03)


def test_work_nobody_is_available_to_take_leaves_the_room():
    """The measured group shrinkage -- an RB room keeps 93% of its volume and a TE room
    68%. A rule that conserved everything would be inventing the difference."""
    got = modulate([0.001, 0.001])
    assert got.gain.max() == pytest.approx(0.0, abs=1e-9)


def test_the_pooled_share_splits_by_projected_opportunity():
    """Ranks 3 and up are pooled because ``depth_rank`` cannot tell them apart, so the
    split has to come from somewhere the projection knows. A bigger projected role takes
    a bigger piece.

    Four in the room so the understudy -- who is excluded from the pooled split -- is not
    one of the two players being compared.
    """
    f = pl.DataFrame({"gsis_id": ["a", "b", "c", "d"], "position": ["RB"] * 4,
                      "expected_games": [0.001, 17.0, 17.0, 17.0],
                      "pred_carries_pg": [10.0, 5.0, 9.0, 3.0],
                      "pred_targets_pg": [0.0] * 4})
    got = sim.opportunity_multiplier(
        np.random.default_rng(0), f,
        [sim.Room("AAA", "RB", (0, 1, 2, 3), (1, 2, 3, 3))],
        {"RB": {"rank_2": 0.0, "rank_rest": 1.0}}, model(), 17, 400,
        sim.baseline_opportunity(f))
    assert got.gain[:, 1].max() == pytest.approx(0.0, abs=1e-9)   # took rank_2, = 0.0
    # 9 against 3, so three-quarters of the pooled share -- then each man's gain is
    # expressed against his own projected season, so divide by his own baseline.
    assert got.gain[:, 2].mean() / got.gain[:, 3].mean() == pytest.approx(
        (0.75 / 9.0) / (0.25 / 3.0), rel=0.05)


def test_the_understudy_is_not_paid_twice_out_of_one_vacancy():
    """He takes ``rank_2``; the pooled ranks 3+ share goes to the others. Letting him
    into that split as well would recapture more than the closure table measured."""
    got = modulate([0.001, 17.0, 17.0],
                   rooms=[sim.Room("AAA", "RB", (0, 1, 2), (1, 2, 3))])
    total = got.gain[:, 1].mean() + got.gain[:, 2].mean()
    assert total == pytest.approx(SHARES["RB"]["rank_2"] + SHARES["RB"]["rank_rest"],
                                  abs=0.03)


def test_turning_the_transfer_off_leaves_the_identical_availability_draw():
    """The G-D2 control has to differ from the treatment in exactly one thing."""
    with_transfer = modulate([0.001, 17.0], transfer=True)
    without = modulate([0.001, 17.0], transfer=False)
    assert np.array_equal(with_transfer.availability, without.availability)
    assert without.gain.max() == 0.0


# --- the double-count, which was a real bug ------------------------------

def test_the_transfer_moves_no_mean():
    """**A backup's projection already contains his expected inheritance.**

    Measured: an RB2 averages 9.86 opportunities a game across a season, against 5.09 in
    the weeks his lead plays and 12.93 in the weeks he does not. A model fitted on season
    totals therefore predicts the blend, and adding the full transfer on top counts it
    twice -- on the 2026 board that lifted the median backup from 123 points to 156,
    which is a different projection, not a wider one.
    """
    got = modulate([8.0, 17.0])
    factor = got.scale("RB", "rushingYards")
    without = modulate([8.0, 17.0], transfer=False).scale("RB", "rushingYards")
    assert factor[:, 1].mean() == pytest.approx(without[:, 1].mean(), rel=0.01)


def test_the_transfer_still_widens_the_spread_it_leaves_the_mean_alone():
    """The counterpart: mean-preserving must not mean effect-erasing. The whole point is
    that the same expected season is split between the world where the man ahead plays
    and the world where he does not."""
    got = modulate([8.0, 17.0])
    factor = got.scale("RB", "rushingYards")
    without = modulate([8.0, 17.0], transfer=False).scale("RB", "rushingYards")
    assert factor[:, 1].std() > without[:, 1].std() * 1.2


def test_the_uncentred_form_is_available_and_does_inflate():
    """Kept only so the backtest can measure what the correction is worth. If this ever
    stops inflating, the centring above is not doing anything."""
    got = modulate([8.0, 17.0], centre=False)
    assert (got.scale("RB", "rushingYards")[:, 1].mean()
            > modulate([8.0, 17.0], transfer=False)
            .scale("RB", "rushingYards")[:, 1].mean() * 1.05)


# --- the elasticity ------------------------------------------------------

def test_availability_is_damped_by_the_fitted_elasticity_and_the_transfer_is_not():
    """``expected_games`` carries role as well as health, so a player who plays twice the
    games the model expected does not produce twice the output -- the fitted exponent is
    0.32-0.49. Inherited work is not that, and damping it would shrink the one effect
    this module exists to price.
    """
    f = frame([17.0, 17.0])
    plain = sn.SeasonUsageModel(volume={}, games_dispersion={"RB": 1000.0},
                                games_elasticity={pv.key("RB", "rushingYards"): 0.5},
                                version="1.2.0")
    got = sim.Modulation(availability=np.full((2, 2), 4.0),
                         gain=np.full((2, 2), 1.0),
                         elasticity=plain.games_elasticity, centre=False)
    del f
    # 4 ** 0.5 == 2 for the availability half, and the gain passes through untouched.
    assert got.scale("RB", "rushingYards")[0, 0] == pytest.approx(3.0)
    # A stat with no fitted elasticity falls back to the pooled value, never to 1.0 --
    # 1.0 is precisely the assumption the fit exists to reject.
    assert got.scale("RB", "receivingYards")[0, 0] == pytest.approx(
        4.0 ** sn.DEFAULT_GAMES_ELASTICITY + 1.0)


# --- the room ordering ---------------------------------------------------

def test_a_tied_rank_is_broken_by_projected_opportunity():
    """The 2016-2024 depth chart lists two or three rank-1 backs in a third of rooms,
    because its rank 1 means 'a starter' rather than 'the best one'. Picking the lead by
    row order would make a third of every backtest fold arbitrary and would put genuine
    co-starters into the group that receives transfers."""
    f = pl.DataFrame({"gsis_id": ["aaa", "bbb"], "position": ["RB", "RB"]})
    chart = pl.DataFrame({"gsis_id": ["aaa", "bbb"], "team": ["AAA", "AAA"],
                          "position": ["RB", "RB"], "depth_rank": [1, 1]})

    import Scripts.usage.context as ctx
    original = ctx.preseason_snapshot
    ctx.preseason_snapshot = lambda depth, season: chart
    try:
        # "bbb" is the smaller opportunity, so despite sorting later by id he must not
        # be the lead when "aaa" projects for less.
        rooms = sim.room_order(f, 2024, baseline=np.array([2.0, 9.0]))
        assert rooms[0].players[0] == 1
        rooms = sim.room_order(f, 2024, baseline=np.array([9.0, 2.0]))
        assert rooms[0].players[0] == 0
    finally:
        ctx.preseason_snapshot = original


def test_a_room_of_one_is_not_a_room():
    """Nobody to transfer to, so it is left out rather than special-cased in the loop."""
    got = modulate([0.001, 17.0], rooms=[])
    assert got.gain.max() == 0.0


# --- determinism ---------------------------------------------------------

def test_the_same_seed_gives_the_same_draw():
    """Not `np.random.seed`. This runs once per league across nine leagues, which is
    exactly the situation where a global stream stops being reproducible."""
    assert np.array_equal(modulate([8.0, 17.0]).gain, modulate([8.0, 17.0]).gain)
