"""G1 for the shipped season head: the blend rule, and the two traps it fell into.

Network-free. The blend arithmetic and the population rule are pure functions of a
frame, which is where both bugs lived.
"""

import polars as pl
import pytest

from Scripts.usage import g1_season as g1


STATS = ["receivingYards", "rushingYards"]


def frame(rows):
    return pl.DataFrame(rows)


# --- the blend rule -------------------------------------------------------

def test_one_equal_vote_among_the_sources_that_have_an_opinion():
    """Two real sources weight 0.5 each, matching the shipped blend."""
    out = g1.blend(frame([{"FP_receivingYards": 100.0, "BOL_receivingYards": 200.0,
                           "USG_receivingYards": None}]), ["receivingYards"], 0.0)
    assert out["BLEND_receivingYards"][0] == pytest.approx(150.0)


def test_an_absent_source_is_dropped_rather_than_counted_as_zero():
    """The failure this whole module exists to avoid: absence read as an opinion of 0.

    If a missing BetOnline line were treated as a projection of zero, a player only
    FantasyPros covers would be halved.
    """
    out = g1.blend(frame([{"FP_receivingYards": 100.0, "BOL_receivingYards": None,
                           "USG_receivingYards": None}]), ["receivingYards"], 0.0)
    assert out["BLEND_receivingYards"][0] == pytest.approx(100.0)


def test_tomcat_enters_at_its_weight_and_renormalises():
    """One external at 1.0 and TOMCAT at 0.25 gives (100 + 0.25*200) / 1.25."""
    out = g1.blend(frame([{"FP_receivingYards": 100.0, "BOL_receivingYards": None,
                           "USG_receivingYards": 200.0}]), ["receivingYards"], 0.25)
    assert out["BLEND_receivingYards"][0] == pytest.approx((100 + 0.25 * 200) / 1.25)


def test_weight_zero_leaves_tomcat_out_entirely():
    """The baseline arm must not be moved by a column it is supposed to ignore."""
    with_model = frame([{"FP_receivingYards": 100.0, "BOL_receivingYards": None,
                         "USG_receivingYards": 9999.0}])
    out = g1.blend(with_model, ["receivingYards"], 0.0)
    assert out["BLEND_receivingYards"][0] == pytest.approx(100.0)


def test_a_row_no_source_prices_blends_to_null_not_zero():
    out = g1.blend(frame([{"FP_receivingYards": None, "BOL_receivingYards": None,
                           "USG_receivingYards": None}]), ["receivingYards"], 0.25)
    assert out["BLEND_receivingYards"][0] is None


# --- the population rule --------------------------------------------------

def test_the_two_arms_are_scored_on_the_same_players():
    """Regression for the bug that produced a false -8% against TOMCAT.

    393 of 949 players on the 2025 fold carry no external projection at all.
    `backtest.points` scores an absent stat as **zero**, not null, so those rows stayed
    in the baseline as a confident projection of nought -- and since a player nobody
    projects generally does score close to nought, the baseline collected free credit
    for an opinion it never had, while TOMCAT projected them and took the error.

    The comparison is between two blends, so it has to run on the players both arms can
    blend. Pinned as a property of `run`'s contract rather than a number: `n` must be
    identical at every weight, and `dropped_no_external` must be reported rather than
    absorbed silently.
    """
    import inspect

    source = inspect.getsource(g1.run)
    assert "has_external" in source, "the population restriction has been removed"
    assert "dropped_no_external" in source, "the drop count must be reported"


def test_the_drop_count_is_surfaced_in_the_report():
    """A measurement that silently discards a third of its rows is not a measurement."""
    import inspect

    assert "dropped_no_external" in inspect.getsource(g1.report)


# --- the contaminated basis stays labelled --------------------------------

def test_the_summed_weekly_basis_is_refused_as_a_result():
    """It correlates +0.33 with games actually played. It must never read as a finding."""
    import inspect

    text = inspect.getsource(g1.report)
    assert "DO NOT REPORT THIS AS A RESULT" in text
    doc = g1.__doc__
    assert "+0.327" in doc and "+0.067" in doc, (
        "the contamination numbers belong in the docstring, not in someone's memory")


def test_an_unknown_basis_raises_rather_than_defaulting():
    with pytest.raises(ValueError, match="basis"):
        g1.run(2025, weights=[0.25], basis="whatever")


# --- the shipped weight, which this module marked wrongly for its whole life ------

def test_shipped_weight_is_derived_from_production_not_restated():
    """``SHIPPED_WEIGHT`` must equal TOMCAT's real ratio to one external source.

    The bug this pins: ``WEIGHTS['default']`` gives TOMCAT 0.25 -- but so does every
    external source, so the *ratio* is 1.0. This module hard-coded 0.25 as "what
    ships", put the ``<- ships`` marker on the wrong row, and swept a range that never
    reached production. The curve then looked like it fell monotonically to its own
    right-hand edge, which read as "TOMCAT is under-weighted" and was acted on twice.
    """
    from Scripts.projection_utils import WEIGHTS as PROD

    default = PROD["default"]
    assert g1.SHIPPED_WEIGHT == pytest.approx(default["USG"] / default["ESPN"])
    assert g1.SHIPPED_WEIGHT == pytest.approx(1.0)


def test_the_swept_range_brackets_production_on_both_sides():
    """An optimum at the edge of the sweep is not an optimum, it is a missing column."""
    assert min(g1.WEIGHTS) < g1.SHIPPED_WEIGHT < max(g1.WEIGHTS)
    assert any(abs(w - g1.SHIPPED_WEIGHT) < 1e-9 for w in g1.WEIGHTS), \
        "production's own weight must be one of the swept points, or the report " \
        "cannot mark it"


def test_tomcat_at_the_shipped_weight_is_a_co_equal_vote():
    """At ``SHIPPED_WEIGHT`` TOMCAT counts exactly as much as one external source."""
    row = {"FP_receivingYards": 100.0, "BOL_receivingYards": 100.0,
           "USG_receivingYards": 400.0}
    out = g1.blend(frame([row]), ["receivingYards"], g1.SHIPPED_WEIGHT)
    # three co-equal voters: (100 + 100 + 400) / 3
    assert out["BLEND_receivingYards"][0] == pytest.approx(200.0)


def test_unequal_external_weights_refuse_to_collapse_to_one_ratio():
    """The ratio is only meaningful under the equal-vote rule; say so rather than lie."""
    import Scripts.projection_utils as pu

    original = pu.WEIGHTS["default"]
    pu.WEIGHTS["default"] = dict(original, ESPN=0.5)
    try:
        with pytest.raises(ValueError, match="unequal weights"):
            g1._shipped_weight()
    finally:
        pu.WEIGHTS["default"] = original
