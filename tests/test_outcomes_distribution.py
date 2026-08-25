"""What the season-points sampler must not get wrong.

Three properties, and each is pinned because getting it wrong is invisible in the
output. A sampler whose marginals drift from the published quantiles produces a
plausible-looking distribution that disagrees with the interval printed beside it. A
sampler that is not reproducible turns a board into a draw. And a player the model
declined to project must come out null rather than zero, which is the bug
``Scripts/usage/season.py``:722 records putting measured coverage at 6%.

Synthetic frames throughout. No network, no parquet, no fitted model on disk.
"""

import numpy as np
import polars as pl
import pytest

from Scripts.outcomes import distribution as dist
from Scripts.usage import predictive as pv
from Scripts.usage import season as sn


# --- fixtures ------------------------------------------------------------

def model(correlation=None, conditional=True):
    """A minimal fitted model carrying only what the sampler reads."""
    dispersion = {
        pv.key("WR", "receivingYards"): {"phi": 119.8, "k": 1000.0, "bust": 0.07},
        pv.key("WR", "receivingReceptions"): {"phi": 9.1, "k": 1000.0, "bust": 0.0},
        pv.key("RB", "rushingYards"): {"phi": 218.3, "k": 1000.0, "bust": 0.09},
    }
    return sn.SeasonUsageModel(
        volume={},
        stat_dispersion=dispersion,
        stat_dispersion_conditional=dispersion if conditional else {},
        stat_correlation=correlation or {},
        version="1.2.0",
    )


def frame(rows):
    """``(gsis_id, position, receivingYards, receivingReceptions, rushingYards)``."""
    return pl.DataFrame(
        rows,
        schema={"gsis_id": pl.String, "position": pl.String,
                "USG_receivingYards": pl.Float64,
                "USG_receivingReceptions": pl.Float64,
                "USG_rushingYards": pl.Float64},
        orient="row")


WRS = [("00-1", "WR", 900.0, 70.0, None),
       ("00-2", "WR", 400.0, 40.0, None),
       ("00-3", "WR", 120.0, 14.0, None)]


# --- the marginals must be the ones the model publishes ------------------

@pytest.mark.parametrize("stat,column,bust", [
    ("receivingYards", "USG_receivingYards", 0.07),
    ("receivingReceptions", "USG_receivingReceptions", 0.0),
])
def test_the_sampled_marginal_matches_the_published_quantile(stat, column, bust):
    """A sampled decile and a printed decile are the same number.

    The whole reason the sampler pushes uniforms through
    :func:`Scripts.usage.predictive.quantile` rather than reimplementing the Negative
    Binomial and zero-inflated Gamma is that a reimplementation can drift from the
    ``USG_<stat>_low``/``_high`` columns without anything failing. If it ever does, this
    is what says so.
    """
    rows = [(f"00-{i}", "WR", 900.0, 70.0, None) for i in range(40)]
    spec = dist.player_spec(frame(rows), model(), conditional=True)
    sample = dist.sample_stats(spec, np.random.default_rng(0), n_sims=4000)

    index = dist.STAT_ORDER.index(stat)
    drawn = sample[:, :, index].ravel()
    mu = frame(rows)[column].to_numpy()[:1]
    coefficients = model().stat_dispersion[pv.key("WR", stat)]

    for q in (0.1, 0.5, 0.9):
        closed = float(pv.quantile(stat, mu, coefficients["phi"], coefficients["k"],
                                   q, bust=bust)[0])
        assert np.percentile(drawn, 100 * q) == pytest.approx(closed, rel=0.04, abs=1.0)


def test_the_copula_moves_stats_together_without_moving_either_marginal():
    """Correlation is imposed on the coupling only.

    The failure this guards against is a dependence structure that quietly shifts a
    marginal -- which would make the points total right for the wrong reason and the
    per-stat intervals wrong.
    """
    paired = {"WR": {"stats": ["receivingYards", "receivingReceptions"],
                     "matrix": [[1.0, 0.9], [0.9, 1.0]], "n": 400}}
    rows = [(f"00-{i}", "WR", 900.0, 70.0, None) for i in range(20)]

    independent = dist.sample_stats(
        dist.player_spec(frame(rows), model(), conditional=True),
        np.random.default_rng(3), n_sims=3000)
    correlated = dist.sample_stats(
        dist.player_spec(frame(rows), model(paired), conditional=True),
        np.random.default_rng(3), n_sims=3000)

    a, b = dist.STAT_ORDER.index("receivingYards"), dist.STAT_ORDER.index("receivingReceptions")
    rho_independent = np.corrcoef(independent[:, 0, a], independent[:, 0, b])[0, 1]
    rho_correlated = np.corrcoef(correlated[:, 0, a], correlated[:, 0, b])[0, 1]
    assert abs(rho_independent) < 0.1
    assert rho_correlated > 0.8

    # ... and the marginal is untouched by the coupling.
    assert (np.median(correlated[:, :, a])
            == pytest.approx(np.median(independent[:, :, a]), rel=0.05))


def test_correlation_widens_the_points_total_and_that_is_the_point():
    """Summing independent marginals understates a correlated sum by the covariance.

    This is the arithmetic the copula exists for, so it gets an assertion rather than a
    comment: the same marginals, scored the same way, must produce a wider points
    distribution once they are allowed to move together.
    """
    paired = {"WR": {"stats": ["receivingYards", "receivingReceptions"],
                     "matrix": [[1.0, 0.9], [0.9, 1.0]], "n": 400}}
    rows = [(f"00-{i}", "WR", 900.0, 70.0, None) for i in range(20)]
    weights = {"receivingYards": 0.1, "receivingReceptions": 1.0}

    def spread(correlation):
        spec = dist.player_spec(frame(rows), model(correlation), conditional=True)
        sample = dist.sample_stats(spec, np.random.default_rng(5), n_sims=3000)
        return dist.season_points(sample, weights)[:, 0].std()

    assert spread(paired) > spread(None) * 1.1


# --- reproducibility -----------------------------------------------------

def test_the_same_seed_gives_the_same_board():
    """A board that changed between two builds of identical inputs would be
    indistinguishable from one that changed because the market moved."""
    spec = dist.player_spec(frame(WRS), model(), conditional=True)
    weights = {"receivingYards": 0.1, "receivingReceptions": 1.0}

    def once():
        sample = dist.sample_stats(spec, np.random.default_rng(dist.DEFAULT_SEED),
                                   n_sims=500)
        return dist.summarise(dist.season_points(sample, weights), spec.positions)

    assert once().equals(once())


def test_a_different_seed_gives_a_different_draw():
    """The counterpart, so the test above cannot pass by the sampler being constant."""
    spec = dist.player_spec(frame(WRS), model(), conditional=True)
    a = dist.sample_stats(spec, np.random.default_rng(1), n_sims=200)
    b = dist.sample_stats(spec, np.random.default_rng(2), n_sims=200)
    assert not np.allclose(np.nan_to_num(a), np.nan_to_num(b))


# --- abstention ----------------------------------------------------------

def test_a_player_the_model_declined_to_project_is_null_not_zero():
    """``Scripts/usage/backtest.py``'s ``spoke`` guard, one level down.

    Without it an abstention scores 0.0 rather than null, and every rookie the model
    deliberately said nothing about counts as a covered prediction.
    """
    rows = WRS + [("00-9", "WR", None, None, None)]
    spec = dist.player_spec(frame(rows), model(), conditional=True)
    assert spec.has_projection.tolist() == [True, True, True, False]

    sample = dist.sample_stats(spec, np.random.default_rng(0), n_sims=200)
    summary = dist.summarise(dist.season_points(sample, {"receivingYards": 0.1}),
                             spec.positions, has_projection=spec.has_projection)
    assert summary["pts_p50"].to_list()[-1] is None
    assert summary["pts_p50"].null_count() == 1
    assert not summary["pts_p50"].is_nan().any()


def test_a_position_with_no_fitted_dispersion_gets_no_draw():
    """Partial coverage is visible; an invented number is not -- the rule
    ``stat_intervals`` already follows."""
    rows = [("00-1", "TE", 500.0, 50.0, None)]
    spec = dist.player_spec(frame(rows), model(), conditional=True)
    assert not spec.has_projection.any()


# --- the basis, which must never be chosen by accident -------------------

def test_asking_for_a_conditional_basis_a_model_lacks_is_an_error():
    """The silent-failure clause.

    A 1.1.0 model has no conditional block. Falling back to the unconditional one would
    let :mod:`Scripts.outcomes.simulate` draw availability on top of a dispersion that
    already contains it -- counting availability twice, and then passing a coverage gate
    by being too wide rather than by being right. It is not recoverable at the call site,
    so it raises here.
    """
    with pytest.raises(ValueError, match="stat_dispersion_conditional"):
        dist.player_spec(frame(WRS), model(conditional=False), conditional=True)

    # The unconditional basis is still available from the same model.
    spec = dist.player_spec(frame(WRS), model(conditional=False), conditional=False)
    assert spec.conditional is False


# --- summarise -----------------------------------------------------------

def test_p_top12_is_a_rank_within_each_drawn_season():
    """Not a rank of the summary quantiles.

    Ranking medians would answer whether his median beats their medians, which is what
    the mean already says. The useful question is how often he finishes there.
    """
    points = np.tile(np.array([300.0, 200.0, 100.0]), (400, 1))
    summary = dist.summarise(points, ["WR", "WR", "WR"], top_n={"WR": 2})
    assert summary["p_top12"].to_list() == [1.0, 1.0, 0.0]


def test_p_bust_is_measured_against_the_simulated_mean():
    """So it cannot be moved by a rescaling elsewhere in the pipeline."""
    points = np.concatenate([np.full(300, 200.0), np.full(100, 10.0)])[:, None]
    summary = dist.summarise(points, ["RB"])
    assert summary["p_bust"].to_list()[0] == pytest.approx(0.25)


# --- the board attachment ------------------------------------------------

def test_the_board_columns_and_the_glossary_cannot_drift_apart():
    """Every column plan 28 writes must have a spec in the app, and vice versa.

    ``display_frame`` drops a spec whose source column is absent **silently**, which is
    what lets one table serve nine leagues -- and is also what would let a renamed column
    disappear from the board with nothing failing.
    """
    import sys
    from pathlib import Path

    # `app/` is not a package -- Streamlit runs the page scripts directly with app/ on
    # sys.path, so the modules import each other by bare name. Same shim
    # `tests/test_draft_view.py` uses.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
    import draft_view as dv

    shown = {c.source for c in dv.COLUMNS}
    # `pts_p50`, `pts_mean` and `pts_sd` are written for the store and the backtest
    # rather than for the table; the rest must be on screen. The two probabilities are
    # read through their rescaled `_pct` twins -- see `with_percent_columns`.
    assert {"pts_p10", "pts_p90", "outcome_evidence",
            "p_top12_pct", "p_bust_pct"} <= shown


def test_the_outcome_evidence_strings_are_pinned_across_the_import_boundary():
    """``app`` cannot import the board builder -- it would pull the ESPN and scoring
    stack into a process that only reads parquet -- so the marker strings are duplicated.
    The same pinning ``ROLE_WITHDRAWN_EVIDENCE`` already has."""
    from Scripts.season_projections import OUTCOME_EVIDENCE

    assert set(OUTCOME_EVIDENCE) == {"no_model", "no_dispersion", "unpriced", "simulated"}
    assert OUTCOME_EVIDENCE["simulated"] == "simulated"


def test_the_unpriced_note_names_rules_the_simulation_cannot_see():
    """A distribution over most of a player's points must say so.

    Two leagues price per-game yardage bonuses, which a season-total simulation cannot
    express at all, and one scores rushing attempts and completions. Silence there would
    read as completeness.
    """
    from Scripts.season_projections import _unpriced_note

    assert _unpriced_note(["receivingYards", "rushingYards"]) == ""
    assert "1 unpriced" in _unpriced_note(["receivingYards", "rushing2PtConversions"])
    assert "2 unpriced" in _unpriced_note(
        ["receivingYards", "rushingYards100-199Game", "passingYards300to399Game"])


def test_a_probability_formatted_as_a_percent_is_rescaled_first():
    """``column_config`` formats are printf, so ``"%.0f%%"`` on 0.90 prints ``1%``.

    No error, no blank cell -- just a column of zeroes that reads as "this player never
    busts" when it means the opposite. ``usg_role_confidence`` shipped that way with plan
    33 phase 2 and showed ``Role %`` as 0% for all 671 players who had one.
    """
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
    import draft_view as dv

    board = pl.DataFrame({"p_top12": [0.9, 0.05], "p_bust": [0.1, 0.62],
                          "usg_role_confidence": [0.588, None]})
    got = dv.with_percent_columns(board)
    assert got["p_top12_pct"].to_list() == pytest.approx([90.0, 5.0])
    assert got["p_bust_pct"].to_list() == pytest.approx([10.0, 62.0])
    assert got["usg_role_confidence_pct"][0] == pytest.approx(58.8)

    # The stored units are untouched -- the rescale is a derived column, following
    # `inj_reinjury_pct`, because changing units in the render layer is how a reader
    # ends up unsure which one he is looking at.
    assert got["p_top12"].to_list() == pytest.approx([0.9, 0.05])

    # And every spec formatted as a percent must read the rescaled column, or it is
    # the bug this function exists to prevent.
    for spec in dv.COLUMNS:
        if spec.fmt and spec.fmt.endswith("%%"):
            assert spec.source.endswith("_pct"), spec.source


def test_a_board_without_the_probability_columns_passes_through():
    """A board built before plan 28 has none of them, and must still render."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
    import draft_view as dv

    board = pl.DataFrame({"player_name": ["a"], "TRUE_Points": [1.0]})
    assert dv.with_percent_columns(board).equals(board)


# --- plan 33 phase 3, measured and rejected ------------------------------

def test_the_cohort_split_is_off_and_the_pooled_cell_is_what_ships():
    """G-R2 measured the split at -0.3pp, so it does not bind by default.

    The cells stay fitted and persisted so the measurement is reproducible; a caller
    has to ask for them. If this ever flips silently, every published interval changes
    for a reason nobody chose.
    """
    dispersion = {
        pv.key("WR", "receivingYards"): {"phi": 100.0, "k": 1000.0, "bust": 0.0},
        pv.key("WR", "receivingYards", "rookie"): {"phi": 400.0, "k": 1000.0,
                                                   "bust": 0.0},
    }
    m = sn.SeasonUsageModel(volume={}, stat_dispersion=dispersion,
                            stat_dispersion_conditional=dispersion, version="1.2.0")
    rows = pl.DataFrame({"gsis_id": ["a"], "position": ["WR"],
                         "USG_receivingYards": [500.0],
                         "usg_role_cohort": ["rookie"]})

    assert dist.COHORT_DISPERSION is False
    index = dist.STAT_ORDER.index("receivingYards")
    assert dist.player_spec(rows, m, conditional=True).phi[0, index] == 100.0
    # ... and the split is still reachable, or the rejection could not be re-measured.
    forced = dist.player_spec(rows, m, conditional=True, use_cohort=True)
    assert forced.phi[0, index] == 400.0
    assert forced.cohort_share == pytest.approx(1.0)


def test_a_cohort_with_no_fitted_cell_falls_back_to_pooled():
    """A thin cohort must not lose its interval entirely -- partial coverage is
    visible, a missing one reads as the model declining to speak."""
    dispersion = {
        pv.key("WR", "receivingYards"): {"phi": 100.0, "k": 1000.0, "bust": 0.0},
        pv.key("WR", "receivingYards", "settled"): {"phi": 60.0, "k": 1000.0,
                                                    "bust": 0.0},
    }
    m = sn.SeasonUsageModel(volume={}, stat_dispersion=dispersion,
                            stat_dispersion_conditional=dispersion, version="1.2.0")
    rows = pl.DataFrame({"gsis_id": ["a", "b"], "position": ["WR", "WR"],
                         "USG_receivingYards": [500.0, 500.0],
                         "usg_role_cohort": ["settled", "mover"]})
    spec = dist.player_spec(rows, m, conditional=True, use_cohort=True)
    index = dist.STAT_ORDER.index("receivingYards")
    assert spec.phi[0, index] == 60.0     # has its own cell
    assert spec.phi[1, index] == 100.0    # falls back to pooled
    assert spec.cohort_share == pytest.approx(0.5)


def test_coverage_is_scored_on_players_worth_projecting():
    """The artefact that passed G-D1 and then inverted plan 33's result.

    A third of the scored sample projects near zero and realises exactly zero, so its
    interval contains the outcome by construction and it counts as covered. Pooling that
    with real projections pulled reported coverage from 0.687 to 0.730 and across the
    gate's lower bound. Coverage has to be measured where a forecast means something.
    """
    from Scripts.lab import registry as reg

    assert reg.MIN_SCORED_PROJECTION > 0

    # The gate reads the draftable figure when one is present, and only falls back to
    # the whole-pool one when it is not -- so a harness that forgets to compute it
    # cannot silently restore the flattering number.
    passing = {"coverage": 0.730, "coverage_draftable": 0.80,
               "calibration_slope": 1.0}
    failing = {"coverage": 0.730, "coverage_draftable": 0.687,
               "calibration_slope": 1.0}
    assert reg.outcome_verdict(passing)[0] == "merge"
    call, reason = reg.outcome_verdict(failing)
    assert call == "reject"
    assert "25 points" in reason and "too narrow" in reason

    # The threshold itself is untouched from its pre-commitment. Only the population
    # moved, and it moved in the direction that makes the gate harder.
    assert reg.OUTCOME_COVERAGE_RANGE == (0.72, 0.88)


def test_the_gate_scores_the_arm_the_board_actually_publishes():
    """G-D1 asks whether the *published* distribution is fit to publish.

    The board runs without the room transfer, because G-D2 rejected it. The first version
    of the harness pooled its headline coverage from the joint arm instead -- both land at
    0.68 and both fail, so no verdict moved, but the number being reported was not the
    number on the board. These two constants have to agree.
    """
    from Scripts.outcomes import backtest as obt
    from Scripts.season_projections import BOARD_USES_JOINT_DRAW

    expected = "joint" if BOARD_USES_JOINT_DRAW else "independent"
    assert obt.SHIPPED_ARM == expected
