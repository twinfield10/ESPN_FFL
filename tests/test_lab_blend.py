"""Fitting the blend weights, and the three ways the fit lied before these tests.

Every case here is a defect this harness actually produced, in the order it produced
them. They are pinned rather than described because each one reported a *large,
plausible, fold-stable* improvement that was not one -- which is the only kind of
error that survives a reviewer's sniff test.

* **The estimator has to be the shipped one.** A fit of a plain linear combination
  measures an estimator nothing publishes. :func:`Scripts.lab.blend.predict` must
  agree with :func:`Scripts.projection_utils.compute_weighted_stats` cell for cell,
  including its face-value fallback -- so this file's anchor test drives the real
  production function and compares.
* **An exact zero switches the blend off.** Concentrating weight on a sparse source
  leaves no renormalised denominator on the rows it does not cover, production falls
  back to face value, and MAE on a rare count rewards it. That read as a stable
  -5.31% on rushing touchdowns; at ``t=0.999`` the same ratios are worth -1.43%.
* **A softmax overflows.** Unstabilised, the optimiser pushed a coordinate until
  ``exp`` returned ``inf``, ``inf/inf`` gave ``nan``, and a ``nan`` weight vector
  was scored as a finite improvement.

Synthetic frames. No store, no network.
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from Scripts.lab import blend
from Scripts.lab import registry as reg
from Scripts import projection_utils as pu

STAT = "rushingTouchdowns"


def frame(rows, stat=STAT):
    """Build an eval-shaped frame for one stat.

    Args:
        rows: Dicts with ``actual``, ``week`` and one entry per source, where a
            value of ``None`` means the source is imputed on that row. An imputed
            cell still carries a number -- the ESPN/FantasyPros mean -- because
            production's fallback reads it, so ``imputed_value`` supplies it.
        stat: Stat name to build columns for.

    Returns:
        pl.DataFrame: With ``<SOURCE>_<stat>``, its ``_is_imputed`` companion,
        ``act_<stat>``, ``week`` and both population flags.
    """
    data = {"week": [r["week"] for r in rows],
            f"act_{stat}": [r["actual"] for r in rows],
            "team_played": [r.get("team_played", True) for r in rows],
            "played": [r.get("played", True) for r in rows]}
    for source in blend.SOURCES:
        key = source.lower()
        if not any(key in r for r in rows):
            continue
        values, flags = [], []
        for row in rows:
            given = row.get(key, None)
            if given is None:
                values.append(row.get("imputed_value", 0.0))
                flags.append(True)
            else:
                values.append(float(given))
                flags.append(False)
        data[f"{source}_{stat}"] = values
        data[f"{source}_{stat}{pu.IMPUTED_SUFFIX}"] = flags
    return pl.DataFrame(data)


# --- the anchor: the harness must fit what the pipeline ships ---------------

@pytest.mark.parametrize("weights", [
    (0.25, 0.25, 0.25, 0.25),
    (0.0, 0.0, 1.0, 0.0),
    (0.5, 0.2, 0.2, 0.1),
])
def test_predict_reproduces_compute_weighted_stats(weights):
    """The whole measurement rests on this, so it drives the real function.

    Includes the all-imputed row on purpose: that is the branch where production
    abandons renormalisation for the face-value sum, and a harness that returns
    zero there instead reports a fictional improvement on every sparse count.
    """
    rows = [
        {"week": 1, "actual": 1.0, "espn": 0.5, "fp": 0.4, "pinny": 0.9, "bol": 0.7},
        {"week": 2, "actual": 0.0, "espn": 0.2, "fp": None, "pinny": None,
         "bol": 0.3, "imputed_value": 0.25},
        # every source imputed: the face-value branch
        {"week": 3, "actual": 2.0, "espn": None, "fp": None, "pinny": None,
         "bol": None, "imputed_value": 0.6},
    ]
    built = frame(rows)
    problem = blend.design(built, STAT, blend.UNIVERSAL)
    mine = blend.predict(np.array(weights), problem)

    pandas_frame = pd.DataFrame(
        {f"{s}_{STAT}": built[f"{s}_{STAT}"].to_numpy()
         for s in problem.sources}
        | {f"{s}_{STAT}{pu.IMPUTED_SUFFIX}":
           built[f"{s}_{STAT}{pu.IMPUTED_SUFFIX}"].to_numpy()
           for s in problem.sources})
    theirs = pu.compute_weighted_stats(
        pandas_frame, [STAT],
        {"default": dict(zip(problem.sources, weights))})[f"TRUE_{STAT}"].to_numpy()

    assert np.allclose(mine, theirs, atol=1e-12), (mine, theirs)


def test_an_imputed_cell_does_not_vote_but_is_still_read_by_the_fallback():
    """Both halves of the provenance rule, which are easy to conflate.

    An imputed cell must not contribute to the renormalised blend, and must still
    be available to the face-value branch. A design that zeroed imputed values
    would satisfy the first and silently break the second.
    """
    built = frame([{"week": 1, "actual": 1.0, "espn": 10.0, "fp": None,
                    "imputed_value": 99.0}])
    problem = blend.design(built, STAT, ("ESPN", "FP"))

    # ESPN real, FP imputed -> ESPN alone, the 99.0 ignored.
    assert blend.predict(np.array([0.5, 0.5]), problem)[0] == pytest.approx(10.0)
    # All weight on the imputed source -> no denominator -> face value reads it.
    assert blend.predict(np.array([0.0, 1.0]), problem)[0] == pytest.approx(99.0)


# --- the degeneracy trap ---------------------------------------------------

def test_equal_votes_never_collapse_the_denominator():
    """Production's own configuration cannot reach the fallback.

    ESPN carries a non-zero weight and is never imputed, so the denominator is
    always at least its share. This is why the trap is a harness problem rather
    than a live bug.
    """
    built = frame([{"week": 1, "actual": 1.0, "espn": 0.4, "fp": None,
                    "pinny": None, "bol": None, "imputed_value": 0.4},
                   {"week": 2, "actual": 0.0, "espn": 0.1, "fp": 0.2,
                    "pinny": None, "bol": None, "imputed_value": 0.15}])
    problem = blend.design(built, STAT, blend.UNIVERSAL)
    assert blend.degenerate_rows(blend.equal_weights(problem), problem) == 0


def test_zeroing_all_but_a_sparse_source_collapses_the_denominator():
    """The exact shape of the false positive: 'PINNY=1.00' on a sparse count."""
    built = frame([{"week": w, "actual": 0.0, "espn": 0.2, "fp": 0.2,
                    "pinny": None, "bol": 0.3, "imputed_value": 0.2}
                   for w in range(1, 6)])
    problem = blend.design(built, STAT, blend.UNIVERSAL)
    only_pinny = np.array([0.0, 0.0, 1.0, 0.0])
    assert blend.degenerate_rows(only_pinny, problem) == 5


def test_a_hair_off_zero_does_not_collapse_it():
    """``t=0.999`` is the control, and this is why it works.

    The ratios are indistinguishable from the free fit, but every source keeps a
    sliver of weight, so the denominator survives and the estimator keeps
    blending. Where the two disagree, the difference is the fallback.
    """
    built = frame([{"week": 1, "actual": 0.0, "espn": 0.2, "fp": 0.2,
                    "pinny": None, "bol": 0.3, "imputed_value": 0.2}])
    problem = blend.design(built, STAT, blend.UNIVERSAL)
    equal = blend.equal_weights(problem)
    free = np.array([0.0, 0.0, 1.0, 0.0])
    near = 0.001 * equal + 0.999 * free

    assert blend.degenerate_rows(free, problem) == 1
    assert blend.degenerate_rows(near, problem) == 0
    # and the two give materially different answers on that row
    assert blend.predict(free, problem)[0] != pytest.approx(
        blend.predict(near, problem)[0])


def test_the_segment_carries_the_control_point():
    """A segment without 0.999 cannot separate weighting from the fallback."""
    assert 0.999 in blend.SEGMENT
    assert blend.SEGMENT[0] == 0.0 and blend.SEGMENT[-1] == 1.0


# --- the decision rule -----------------------------------------------------

def _entry(near, free, disagreement=0.0, degenerate=0, stat=STAT):
    """A minimal `two_fold`-shaped result."""
    segment = {str(t): 0.0 for t in blend.SEGMENT}
    segment["0.999"] = near
    segment[str(blend.SEGMENT[-1])] = free
    return {
        "stat": stat, "sources": list(blend.UNIVERSAL), "n": [500, 500],
        "fold_weights": [{}, {}], "fold_disagreement": disagreement,
        "segment_mae_change_pct": segment,
        "segment_degenerate_rows": {str(t): 0 for t in blend.SEGMENT}
        | {str(blend.SEGMENT[-1]): degenerate},
        "free_fit_mae_change_pct": free, "near_free_mae_change_pct": near,
    }


def test_the_accuracy_clauses_are_scored_without_the_fallback():
    """The correction that made this rule coherent.

    The first version judged the free fit, so a candidate could pass mean_gain on
    a number the degeneracy clause was simultaneously failing -- rewarding the
    artifact it had just detected. Here the free fit looks excellent (-5%) and the
    honest number does not (-0.1%); mean_gain must read the honest one.
    """
    decision = blend.verdict([_entry(near=-0.1, free=-5.0, degenerate=900)])
    assert decision["clauses"]["mean_gain"]["value"] == pytest.approx(-0.1)
    assert not decision["clauses"]["mean_gain"]["pass"]
    assert not decision["clauses"]["not_degenerate"]["pass"]
    assert not decision["pass"]


def test_a_genuine_gain_passes_every_clause():
    """The rule is not unfalsifiable: a stable, non-degenerate, large gain passes."""
    decision = blend.verdict([
        _entry(near=-1.5, free=-1.5, disagreement=0.02, stat="a"),
        _entry(near=-0.8, free=-0.8, disagreement=0.05, stat="b"),
    ])
    assert decision["pass"], decision["clauses"]


def test_one_bad_stat_fails_the_rule_even_when_the_mean_is_good():
    """A mean can hide a stat, and a board is read one stat at a time."""
    decision = blend.verdict([
        _entry(near=-4.0, free=-4.0, stat="helped"),
        _entry(near=+reg.MAX_STAT_MAE_INCREASE_PCT + 0.5, free=+2.5, stat="hurt"),
    ])
    assert decision["clauses"]["mean_gain"]["pass"]
    assert not decision["clauses"]["worst_stat"]["pass"]
    assert not decision["pass"]


def test_unstable_weights_fail_however_good_the_error():
    """Two halves of one season disagreeing means the fit described a half."""
    decision = blend.verdict([_entry(near=-9.0, free=-9.0, disagreement=0.9)])
    assert decision["clauses"]["mean_gain"]["pass"]
    assert not decision["clauses"]["stability"]["pass"]
    assert not decision["pass"]


def test_the_stability_bar_is_a_fraction_of_an_equal_vote():
    """Pins the threshold's stated justification, so a silent loosening shows up."""
    assert blend.MAX_FOLD_DISAGREEMENT == 0.10
    assert blend.MAX_FOLD_DISAGREEMENT < 1.0 / len(blend.UNIVERSAL)


# --- numerical safety ------------------------------------------------------

@pytest.mark.parametrize("theta", [
    np.array([0.0, 0.0, 0.0]),
    np.array([1e3, -1e3, 1e3]),
    np.array([np.inf, 0.0, 0.0]),
    np.array([-np.inf, -np.inf, -np.inf]),
])
def test_the_simplex_map_never_returns_nan(theta):
    """The overflow regression. `inf/inf` gave `nan`, which scored as finite."""
    weights = blend._simplex(theta)
    assert np.all(np.isfinite(weights)), weights
    assert weights.sum() == pytest.approx(1.0)
    assert np.all(weights >= 0.0)


def test_fit_returns_a_point_on_the_simplex():
    """Whatever the data, the fit is a blend rather than a bet against a source."""
    built = frame([{"week": w, "actual": float(w % 3), "espn": 0.3, "fp": 0.5,
                    "pinny": 0.9, "bol": 0.1} for w in range(1, 40)])
    problem = blend.design(built, STAT, blend.UNIVERSAL)
    weights = blend.fit(problem, restarts=2)
    assert weights.sum() == pytest.approx(1.0)
    assert np.all(weights >= -1e-12)
    assert np.all(np.isfinite(weights))


def test_fit_is_reproducible():
    """A reported number has to come back the same, or it cannot be checked."""
    built = frame([{"week": w, "actual": float(w % 4), "espn": 0.3, "fp": 0.5,
                    "pinny": 0.9, "bol": 0.1} for w in range(1, 40)])
    problem = blend.design(built, STAT, blend.UNIVERSAL)
    assert np.allclose(blend.fit(problem, restarts=3, seed=7),
                       blend.fit(problem, restarts=3, seed=7))


# --- guards on what is being compared -------------------------------------

def test_the_baseline_is_checked_against_the_shipped_weights():
    """Every number here is a change *against the equal-vote rule*.

    If someone sets a non-equal weight, that comparison quietly stops being the
    one the module documents, so `run` reports it rather than printing a baseline
    that no longer exists.
    """
    assert blend.shipped_is_equal(blend.UNIVERSAL)
    assert not blend.shipped_is_equal(("ESPN", "KIK"))


def test_the_shipped_weights_really_are_equal_over_the_universal_sources():
    """Ties this file to `WEIGHTS`, so turning a source down breaks a test here."""
    weights = [pu.WEIGHTS["default"][s] for s in blend.UNIVERSAL]
    assert len(set(weights)) == 1, dict(zip(blend.UNIVERSAL, weights))


def test_a_fold_too_small_returns_nothing_rather_than_a_number():
    """Two hundred rows is the floor; below it a fit is a description of noise."""
    built = frame([{"week": w, "actual": 1.0, "espn": 0.3, "fp": 0.5,
                    "pinny": 0.9, "bol": 0.1} for w in range(1, 12)])
    problem = blend.design(built, STAT, blend.UNIVERSAL)
    assert blend.two_fold(problem, "odd/even") is None


def test_usage_vote_cost_changes_only_the_vote():
    """The contrast has to hold the population fixed, or it measures coverage.

    Both sides are scored on the same rows; the only difference is whether USG
    casts a vote. Here USG is wrong on every row, so adding it must worsen MAE.
    """
    rows = [{"week": w, "actual": 1.0, "espn": 1.5, "fp": 1.5, "pinny": 1.5,
             "bol": 1.5, "usg": 5.0} for w in range(1, 6)]
    problem = blend.design(frame(rows), STAT, blend.SOURCES)
    cost = blend.usage_vote_cost(problem, "odd/even")

    # Four sources at 1.5 against an actual of 1.0 leaves a base error of 0.5, so
    # the percentage is genuinely exercised rather than short-circuited by a zero
    # divisor. The fifth vote pulls the blend to (1.5*4 + 5)/5 = 2.2.
    assert cost["mae_without_usage_vote"] == pytest.approx(0.5)
    assert cost["mae_with_usage_vote"] == pytest.approx(1.2)
    assert cost["adding_usage_mae_change_pct"] == pytest.approx(140.0)


def test_usage_vote_cost_is_absent_when_the_source_is():
    """The weekly path has no USG column at all, and that must not raise."""
    rows = [{"week": 1, "actual": 1.0, "espn": 1.0, "fp": 1.0}]
    problem = blend.design(frame(rows), STAT, blend.UNIVERSAL)
    assert blend.usage_vote_cost(problem, "odd/even") is None
