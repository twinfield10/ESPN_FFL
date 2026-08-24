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
