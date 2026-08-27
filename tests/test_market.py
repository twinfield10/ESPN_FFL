"""Market lines and vig: the four identities, and the six bugs found building them.

Every function in :mod:`Scripts.market` is arithmetic on prices, so each one has a
property that has to hold whatever the data. These pin those, plus one regression per
defect the build turned up -- each of which was live in the code that shipped before
it:

* **A two-way pair sums to 1 after de-vig.** The old code never removed a measured
  6.4% and every expectation built on those prices carried it.
* **A count ladder's mean and variance are exact identities.** The old code computed
  the mean by the same arithmetic under another name and discarded the variance.
* **A yardage ladder is not a mean.** ``sum(threshold * P(bucket))`` drops every yard
  below the lowest rung; against the posted line it ran 0.77 to 1.81.
* **A count market at a 0.5 line states a probability.** Shifting it by
  ``Phi^-1(q) * sigma`` sends a 0.5 interception line to 0.35 where the answer is
  0.53 -- and the whole point of the fix is that this is the biggest single error in
  the file.
* **Only volume markets identify a position.** Pinnacle's touchdown market is named
  ``rushingTouchdowns``, so "starts with rushing" made Drake London a running back.
* **A market has to be looked up under the name it is fitted as.** Pinnacle's
  touchdown column is ``rushingTouchdowns`` and the fit is ``anytimeTouchdown``; the
  mismatch returned the line unconverted, which is a flat 0.5 touchdowns per player.

No network and no data pull: models are constructed directly and ladders are written
out by hand.
"""

import numpy as np
import pytest

from Scripts import market as mk


# --- fixtures ------------------------------------------------------------

#: Kyle Pitts' anytime market, archived 2025 week 17, exactly as the book posted it.
#:
#: The worked example in ``docs/plans/35-market-lines-and-vig.md`` V6, kept as the
#: fixture so the numbers in that document and the numbers this module produces
#: cannot drift.
PITTS_RUNGS = (1.0, 2.0, 3.0)
PITTS_SURVIVAL = (1 / 3.0, 1 / 13.0, 1 / 85.0)

#: Bijan Robinson's receiving ladder, same scrape. Ten rungs, 10 yards apart,
#: spanning S = 0.699 down to 0.061 -- which is why it brackets the median and the
#: 85th percentile but not the first quartile.
BIJAN_RUNGS = (34.0, 44.0, 54.0, 64.0, 74.0, 84.0, 94.0, 104.0, 114.0, 124.0)
BIJAN_SURVIVAL = (0.699301, 0.549451, 0.458716, 0.375940, 0.303030,
                  0.232558, 0.173913, 0.125000, 0.086957, 0.060606)


def model(**overrides) -> mk.MarketModel:
    """A model with dispersion roughly as fitted on 2016-2025."""
    built = mk.MarketModel(
        dispersion={
            "WR|receivingYards": (20.48, 55.56, 20462),
            "RB|rushingYards": (19.93, 1000.0, 12649),
            "QB|passingYards": (21.89, 1000.0, 4857),
            "QB|passingInterceptions": (0.972, 31.13, 4838),
            "QB|passingTouchdowns": (0.734, 1000.0, 4837),
            "RB|anytimeTouchdown": (0.902, 1000.0, 11055),
            "WR|receivingReceptions": (1.103, 1000.0, 20502),
        },
        pooled={"receivingYards": (17.73, 19.98, 44898),
                "anytimeTouchdown": (0.884, 1000.0, 40738)},
        td_scale_by_position={"RB": (1.2650, 14883), "WR": (1.1515, 23609),
                              "TE": (1.1187, 11778), "QB": (1.1385, 6282),
                              "REC": (1.1418, 35387)},
        seasons=[2016, 2025],
    )
    for key, value in overrides.items():
        setattr(built, key, value)
    return built


# --- the margin ----------------------------------------------------------

def test_devig_two_way_sums_to_one():
    """The property the old code never enforced. A pair implying 1.064 leaves 6.4%
    of the book's margin inside every projection built on it."""
    over, under = np.array([0.532, 0.60]), np.array([0.532, 0.464])
    q_over, q_under = mk.devig_two_way(over, under)
    assert np.allclose(q_over + q_under, 1.0)
    assert q_over[0] == pytest.approx(0.5)


def test_overround_matches_the_archived_measurement():
    """A -110/-110 pair is the 1.0476 textbook case; the archived BetOnline pairs
    ran 1.0621 to 1.0658, and the constant is the median of those."""
    assert mk.overround(0.5238, 0.5238) == pytest.approx(1.0476, abs=1e-4)
    assert mk.DEFAULT_OVERROUND == pytest.approx(1.0640)


def test_devig_two_way_is_nan_where_a_side_is_missing():
    """A missing side is not a fair price. Filling it would invent one."""
    q_over, _ = mk.devig_two_way(np.array([0.5, np.nan]), np.array([0.5, 0.5]))
    assert np.isnan(q_over[1])


@pytest.mark.parametrize("over, under, expected", [
    ([0.510, 0.515], [0.520, 0.520], "measured"),
    ([], [], "fallback"),
    ([0.9, 0.9], [0.9, 0.9], "fallback"),      # 1.80 -- a malformed pivot
])
def test_measure_overround_refuses_an_impossible_margin(over, under, expected):
    """A guard on the *measurement*, not the book. A wild overround is a broken
    pivot, and dividing a week's survival probabilities by it would gut every
    projection silently."""
    found = mk.measure_overround(np.array(over), np.array(under))
    if expected == "fallback":
        assert found == mk.DEFAULT_OVERROUND
    else:
        assert mk.OVERROUND_BOUNDS[0] <= found <= mk.OVERROUND_BOUNDS[1]
        assert found != mk.DEFAULT_OVERROUND


def test_devig_survival_scales_and_clips():
    """A one-sided ladder has no complement, so its hold is divided out rather than
    normalised away -- measured at the same 6.4% as the two-way market."""
    fair = mk.devig_survival(np.array([1.20, 0.532, 0.106]), 1.064)
    assert fair[0] == 1.0                       # clipped, not left above 1
    assert fair[1] == pytest.approx(0.5, abs=1e-3)
    assert fair[2] == pytest.approx(0.106 / 1.064)


# --- ladder shape --------------------------------------------------------

def test_monotone_survival_sorts_and_repairs():
    """P(X >= t) cannot rise with t. A feed that says so is stale on one rung, and
    every consumer below differences or inverts this function."""
    edges, values = mk.monotone_survival([3.0, 1.0, 2.0], [0.1, 0.3, 0.4])
    assert list(edges) == [1.0, 2.0, 3.0]
    assert list(values) == [0.3, 0.3, 0.1]      # the 0.4 at rung 2 is impossible


def test_monotone_survival_drops_non_finite_rungs():
    edges, values = mk.monotone_survival([1.0, np.nan, 2.0], [0.3, 0.2, 0.1])
    assert list(edges) == [1.0, 2.0]
    assert list(values) == [0.3, 0.1]


# --- the count identity --------------------------------------------------

def test_count_moments_reproduces_the_plan_worked_example():
    """``E[N] = sum P(N >= k)`` and ``E[N^2] = sum (2k-1) P(N >= k)``, both exact.

    These are the numbers plan 35 V6 quotes for Kyle Pitts. The mean was already
    being computed -- by ``sum(threshold * P(exactly))``, which is the same
    arithmetic -- and the standard deviation was thrown away."""
    mean, sd = mk.count_moments(PITTS_RUNGS, PITTS_SURVIVAL)
    assert mean == pytest.approx(0.4220, abs=5e-4)
    assert sd == pytest.approx(0.6670, abs=5e-4)


def test_count_moments_equals_the_old_arithmetic():
    """The identity and ``sum(threshold * P(exactly))`` agree for a ladder rooted at
    1, which is why the mean did not need fixing and the variance did."""
    survival = np.asarray(PITTS_SURVIVAL)
    exact = np.append(survival[:-1] - survival[1:], survival[-1])
    assert mk.count_moments(PITTS_RUNGS, PITTS_SURVIVAL)[0] == pytest.approx(
        float(np.asarray(PITTS_RUNGS) @ exact))


def test_count_moments_abstains_when_the_ladder_does_not_start_at_one():
    """A receptions ladder starts at 4. ``E[N]`` needs the rungs below it and
    inventing them would be worse than returning nothing."""
    mean, sd = mk.count_moments([4.0, 5.0, 6.0], [0.8, 0.6, 0.45])
    assert np.isnan(mean) and np.isnan(sd)


def test_count_moments_variance_is_never_negative():
    """Floating point on a nearly-degenerate ladder used to be able to produce a
    negative variance, and sqrt of that is not a standard deviation."""
    _, sd = mk.count_moments([1.0], [1.0])
    assert sd == 0.0


# --- reading a yardage ladder --------------------------------------------

def test_ladder_median_interpolates_between_the_bracketing_rungs():
    edges, survival = BIJAN_RUNGS, BIJAN_SURVIVAL
    median = mk.ladder_median(edges, survival)
    assert 44.0 < median < 54.0


def test_devigged_ladder_median_recovers_the_posted_line():
    """The check that validates the de-vig on prices alone, with no outcome data.

    BetOnline posted Bijan Robinson's receiving line at 43.5 and this ladder beside
    it. De-vigged, the ladder's own median has to land on that line to within the
    rung spacing -- 10 yards here, because that is the resolution the book priced."""
    fair = mk.devig_survival(BIJAN_SURVIVAL, mk.DEFAULT_OVERROUND)
    assert mk.ladder_median(BIJAN_RUNGS, fair) == pytest.approx(43.5, abs=10.0)


def test_ladder_median_abstains_when_the_median_is_not_bracketed():
    """A book that priced only the tail has not stated a median."""
    assert np.isnan(mk.ladder_median([100.0, 110.0], [0.20, 0.10]))


def test_ladder_scale_is_insensitive_to_the_span():
    """Measured on the archive: 0.50-0.80, 0.50-0.85 and 0.50-0.90 give this ladder
    49.2, 49.2 and 49.1 yards. If the read moved with the span it would be measuring
    the choice of span."""
    fair = mk.devig_survival(BIJAN_SURVIVAL, mk.DEFAULT_OVERROUND)
    baseline = mk.ladder_scale(BIJAN_RUNGS, fair)
    assert baseline == pytest.approx(49.2, abs=1.0)


def test_ladder_scale_abstains_below_the_span():
    """Eight of seventeen archived yardage ladders start at or below the median, so
    a symmetric read is not available and an extrapolated one is not a measurement."""
    fair = mk.devig_survival([0.45, 0.30, 0.20], mk.DEFAULT_OVERROUND)
    assert np.isnan(mk.ladder_scale([50.0, 60.0, 70.0], fair))


def test_market_scale_prefers_the_exact_identity_for_a_rooted_count():
    """A count ladder rooted at 1 has an exact standard deviation; a quantile read
    of the same rungs is an approximation of it."""
    fair = mk.devig_survival(PITTS_SURVIVAL, mk.DEFAULT_OVERROUND)
    exact = mk.count_moments(PITTS_RUNGS, fair)[1]
    assert mk.market_scale(PITTS_RUNGS, fair, "count") == pytest.approx(exact)


def test_market_scale_falls_through_for_a_count_ladder_rooted_higher():
    """Receptions from 4, carries from 7: the identity cannot help and the quantile
    read can."""
    fair = mk.devig_survival([0.82, 0.60, 0.46, 0.34, 0.24, 0.15, 0.10],
                             mk.DEFAULT_OVERROUND)
    found = mk.market_scale([4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0], fair, "count")
    assert np.isfinite(found) and found > 0


# --- converting a yardage line -------------------------------------------

def test_line_to_mean_is_exact_at_an_even_price():
    """A line the book has priced at 50/50 is its own median and its own mean under
    a symmetric read. Anything else would move a number on no information."""
    assert mk.line_to_mean(np.array([65.5]), np.array([0.5]),
                           np.array([37.0]))[0] == pytest.approx(65.5)


def test_line_to_mean_is_monotone_in_the_price():
    """The property the old form also had, and the only one it had."""
    lines = np.full(5, 65.5)
    prices = np.array([0.35, 0.45, 0.5, 0.55, 0.65])
    means = mk.line_to_mean(lines, prices, np.full(5, 37.0))
    assert np.all(np.diff(means) > 0)


def test_line_to_mean_scales_with_dispersion_not_with_the_level():
    """The whole of plan 35 V2. The old form multiplied the tilt by the *level* of
    the line, which assumes a constant coefficient of variation; measured, CV falls
    0.81 to 0.44 across the rushing range. Two lines at the same price and the same
    sigma must move by the same amount whatever their level."""
    small = mk.line_to_mean(np.array([20.0]), np.array([0.6]), np.array([30.0]))
    large = mk.line_to_mean(np.array([200.0]), np.array([0.6]), np.array([30.0]))
    assert (small[0] - 20.0) == pytest.approx(large[0] - 200.0)


def test_line_to_mean_leaves_the_line_alone_without_a_dispersion():
    """The abstention. A stat with no fitted sigma gets the book's median, not a
    number invented from a coefficient."""
    assert mk.line_to_mean(np.array([5.5]), np.array([0.62]),
                           np.array([np.nan]))[0] == 5.5


# --- converting a count line ---------------------------------------------

@pytest.mark.parametrize("line", [0.5, 1.5, 5.5, 16.5, 33.5])
@pytest.mark.parametrize("phi, k", [(0.9, 1000.0), (2.25, 1000.0), (1.11, 218.0)])
def test_count_line_to_mean_inverts_its_own_survival(line, phi, k):
    """The defining property: the mean it returns is the mean that produces the
    price it was given."""
    means = np.array([0.3, 1.0, 3.0, 10.0, 35.0])
    edge = np.floor(line) + 1.0
    prices = mk._count_survival(np.full(means.shape, edge), means, phi, k)
    usable = (prices > 1e-6) & (prices < 1.0 - 1e-6)
    back = mk.count_line_to_mean(np.full(means.shape, line), prices, phi, k)
    assert np.allclose(back[usable], means[usable], rtol=1e-6)


def test_count_line_to_mean_beats_a_gaussian_shift_on_a_half_line():
    """**The largest single error in the file this replaces.**

    A 0.5 interception line priced at ``q = 0.412`` is the statement
    ``P(at least one) = 0.412``, so the mean is about ``-ln(1 - q) = 0.531``. The
    old expression gave 0.415 and a Gaussian shift with the fitted sigma of 0.70
    gives 0.345, against a population that realises 0.663 a week. Measured on 2025,
    fixing this took BetOnline's quarterback interception calibration from **0.712
    to 1.011** on 431 player-weeks."""
    phi, k = model().parameters("passingInterceptions", "QB")
    inverted = mk.count_line_to_mean(np.array([0.5]), np.array([0.412]), phi, k)[0]
    gaussian = mk.line_to_mean(np.array([0.5]), np.array([0.412]),
                               np.array([0.70]))[0]
    assert inverted == pytest.approx(0.53, abs=0.02)
    assert gaussian < 0.40 < inverted


def test_count_line_to_mean_is_monotone_in_the_price():
    prices = np.array([0.1, 0.3, 0.5, 0.7, 0.9])
    means = mk.count_line_to_mean(np.full(5, 0.5), prices, 0.9, 1000.0)
    assert np.all(np.diff(means) > 0)


def test_count_line_to_mean_abstains_on_a_degenerate_price():
    found = mk.count_line_to_mean(np.array([0.5, 0.5]), np.array([0.0, 1.0]),
                                 0.9, 1000.0)
    assert np.all(np.isnan(found))


def test_count_line_to_mean_reads_over_as_the_next_integer():
    """"Over 0.5" and "over 1.5" are P(N >= 1) and P(N >= 2). Rounding the wrong way
    prices one market as another."""
    at_half = mk.count_line_to_mean(np.array([0.5]), np.array([0.5]), 0.9, 1000.0)
    at_one_half = mk.count_line_to_mean(np.array([1.5]), np.array([0.5]), 0.9, 1000.0)
    assert at_half[0] == pytest.approx(np.log(2.0), abs=0.02)   # Poisson P(N>=1)=.5
    assert at_one_half[0] > at_half[0]


# --- position from the market set ----------------------------------------

@pytest.mark.parametrize("posted, expected", [
    (["passingYards", "passingAttempts", "rushingTouchdowns"], "QB"),
    (["rushingYards", "rushingAttempts", "receivingYards"], "RB"),
    (["receivingYards", "receivingReceptions"], "REC"),
    ([], "REC"),
])
def test_position_from_markets(posted, expected):
    assert mk.position_from_markets(posted) == expected


def test_a_receiver_with_a_touchdown_market_is_not_a_back():
    """Regression. Pinnacle's touchdown market maps to ``rushingTouchdowns`` for
    column-naming reasons, so a bare "starts with rushing" test made Drake London a
    running back and sent his touchdown price through the wrong conversion."""
    assert mk.position_from_markets(
        ["receivingYards", "receivingReceptions", "rushingTouchdowns"]) == "REC"


# --- the model -----------------------------------------------------------

def test_mean_from_line_dispatches_on_the_stat_shape():
    """A yardage market gets the normal shift and a count market the inversion. The
    two answers differ by more than rounding at a low line, which is why the
    dispatch is not cosmetic."""
    built = model()
    yardage = built.mean_from_line("receivingYards", np.array([65.5]),
                                  np.array([0.5]), np.array(["WR"], dtype=object))
    count = built.mean_from_line("passingInterceptions", np.array([0.5]),
                                 np.array([0.5]), np.array(["QB"], dtype=object))
    assert yardage[0] == pytest.approx(65.5)
    assert count[0] == pytest.approx(np.log(2.0), abs=0.05)


def test_mean_from_line_returns_the_line_for_an_unfitted_market():
    """Regression, and the one that would have been silent: Pinnacle's touchdown
    column is named ``rushingTouchdowns`` while the fit is ``anytimeTouchdown``. A
    lookup under the column name found nothing, and the projection became a flat 0.5
    touchdowns for every player the book priced. :data:`Scripts.market.MARKET_ALIASES`
    is the fix; the abstention itself stays, for a market with no entry at all."""
    built = model()
    assert built.parameters("rushingTouchdowns") is None
    # The alias is what stops the abstention: looked up under the column name the
    # frame carries, the conversion resolves to the market it came from.
    assert mk.resolve_stat("rushingTouchdowns") == "anytimeTouchdown"
    for name in ("rushingTouchdowns", "anytimeTouchdown"):
        converted = built.mean_from_line(name, np.array([0.5]), np.array([0.62]),
                                         np.array(["RB"], dtype=object))[0]
        assert converted > 0.9      # -ln(1 - 0.62) is about 0.97
    # A market with no entry and no alias still abstains, which is the safe default.
    assert built.mean_from_line("fieldGoalsMade", np.array([1.5]),
                                np.array([0.62]))[0] == 1.5


def test_sigma_falls_back_to_the_pooled_fit():
    """Pinnacle reports no position, so the pooled fit is what it gets for any stat
    whose position cannot be inferred."""
    built = model()
    pooled = built.sigma("receivingYards", np.array([65.5]))
    per_position = built.sigma("receivingYards", np.array([65.5]),
                               np.array(["WR"], dtype=object))
    assert np.isfinite(pooled[0]) and np.isfinite(per_position[0])
    assert pooled[0] != per_position[0]


def test_sigma_is_nan_for_an_unfitted_stat():
    assert np.isnan(model().sigma("defensiveSacks", np.array([1.5]))[0])


def test_td_scale_pools_receivers_and_matches_the_measurement():
    """The fitted ratios from 174,374 player-weeks. Kept as the fallback and as the
    number that made the defect visible; :func:`Scripts.market.count_line_to_mean`
    is what ships, because a flat ratio is calibrated at one rate and the inversion
    is calibrated across the range."""
    built = model()
    assert built.td_scale(np.array(["RB"], dtype=object))[0] == pytest.approx(1.2650)
    assert built.td_scale(np.array(["REC"], dtype=object))[0] == pytest.approx(1.1418)
    # An unfitted position gets the pooled receiver ratio, not 1.0 and not a guess.
    assert built.td_scale(np.array(["FB"], dtype=object))[0] == pytest.approx(1.1418)


def test_model_round_trips_through_json(tmp_path):
    path = tmp_path / "market.json"
    built = model()
    built.save(path)
    read = mk.MarketModel.load(path)
    assert read.dispersion == built.dispersion
    assert read.pooled == built.pooled
    assert read.td_scale_by_position == built.td_scale_by_position
    assert read.version == mk.MODEL_VERSION


def test_load_model_returns_none_rather_than_raising(tmp_path):
    """A missing artifact must not stop a Sunday-morning scrape. Without it lines go
    out at the book's median, which is a visible degradation rather than a crash."""
    assert mk.load_model(tmp_path / "absent.json") is None


#: Every stat name the two scrapers can hand the conversion.
#:
#: Hardcoded rather than imported: ``Scripts.scrape_pinnacle`` starts a browser at
#: import time, so a test cannot read its ``prop_to_stat`` without scraping the web.
#: The duplication is the point of the test below -- it fails if a scraper grows a
#: market the market table has no entry or alias for.
SCRAPED_STATS = (
    "passingYards", "passingAttempts", "passingCompletions", "passingTouchdowns",
    "passingInterceptions", "rushingYards", "rushingAttempts", "rushingTouchdowns",
    "receivingYards", "receivingReceptions", "receivingTouchdowns",
    "defensiveTotalTackles", "defensiveSacks", "defensiveInterceptions",
    "anytimeTouchdown",
)


def test_every_scraped_stat_resolves_to_a_market():
    """The scrapers look every posted market up in this table by column name. A name
    that resolves to nothing does not raise -- it abstains and ships the raw line,
    which is what made the Pinnacle touchdown bug invisible."""
    for stat in SCRAPED_STATS:
        assert mk.resolve_stat(stat) in mk.MARKET_STATS, stat


def test_scraped_stats_matches_the_pinnacle_mapping():
    """The hardcoded list above against the scraper's own, read from source rather
    than imported -- importing that module opens a browser."""
    import re

    source = (mk.paths.REPO_ROOT / "Scripts" / "scrape_pinnacle.py").read_text()
    block = source[source.index("prop_to_stat={"):source.index("name_changes={")]
    mapped = set(re.findall(r": *[\'\"]([A-Za-z]+)[\'\"]", block))
    assert mapped <= set(SCRAPED_STATS), mapped - set(SCRAPED_STATS)


def test_panel_columns_cover_every_market():
    """``weekly_panel`` reads a fixed column list and then evaluates each market's
    expression against it. A market whose column is not in the list raises at fit
    time, which is the right moment, but only if the two are kept in step."""
    for stat, found in mk.MARKET_STATS.items():
        for name in found.weekly.meta.root_names():
            assert name in mk.PANEL_COLUMNS, f"{stat} needs {name}"
