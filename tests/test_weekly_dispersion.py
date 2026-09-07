"""The weekly dispersion fit, and the gates the win probability ships on.

Covers ``Scripts/outcomes/weekly.py``. No store on disk -- residual frames are
synthesised with a known variance structure, so the fit can be checked against the
parameters it was generated from.

The gates are the point of the module. A win probability is the one number in this app
a reader cannot sanity-check by eye, so a confident-looking 63% that is really a coin
flip is worse than no number at all. ``GATES`` is pre-committed and ``backtest``
reports pass/fail against it; the tests below check that a good model passes and that
a deliberately broken one is caught.
"""

import numpy as np
import polars as pl
import pytest

from Scripts.outcomes import weekly as wk


def synthetic(n_leagues=4, n_weeks=17, n_teams=10, starters=9, phi=4.0, seed=28):
    """Residuals generated with a known ``Var = phi * mu`` structure.

    Deliberately generated from the *fitted form*, so the recovered coefficient can
    be compared to the one used. Positions are QB/RB/WR/TE only, which is the set
    plan 28's distribution actually covers.
    """
    rng = np.random.default_rng(seed)
    positions = ["QB", "RB", "RB", "WR", "WR", "WR", "TE", "RB", "WR"][:starters]
    rows = []
    for league in range(n_leagues):
        for week in range(1, n_weeks + 1):
            for team in range(n_teams):
                for slot, position in enumerate(positions):
                    mu = float(rng.uniform(5.0, 25.0))
                    actual = mu + rng.normal(0.0, np.sqrt(phi * mu))
                    rows.append({
                        "lg": f"lg{league}", "season": 2025, "week": week,
                        "team_owner": f"t{team}", "player_id": slot,
                        "player_position": position, "mu": mu,
                        "actual": actual, "resid": actual - mu,
                    })
    return pl.DataFrame(rows)


# --- the fit -------------------------------------------------------------

def test_the_fit_recovers_the_variance_it_was_generated_from():
    """Checked on the implied variance, not on the two coefficients separately.

    ``mu`` and ``mu**2`` are collinear over any realistic projection range, so phi
    and 1/k trade off against each other: a fit on data generated with phi=4 and
    1/k=0 comes back near phi=3.4, 1/k=0.031 and describes the same curve. The
    identified quantity is ``Var(mu)``, which is also the only thing any caller
    reads -- so that is what the test pins.
    """
    frame = synthetic(phi=4.0)
    model = wk.fit(frame=frame)
    for position in ("QB", "RB", "WR", "TE"):
        for mu in (5.0, 15.0, 25.0):
            implied = wk.player_sd(model, position, mu) ** 2
            assert implied == pytest.approx(4.0 * mu, rel=0.15)


def test_a_position_with_too_few_rows_takes_the_pooled_fit():
    """Safeties turn up 24 times in a season. A two-parameter fit on that is noise
    with a decimal point on it."""
    frame = synthetic()
    thin = frame.head(10).with_columns(pl.lit("S").alias("player_position"))
    model = wk.fit(frame=pl.concat([frame, thin]))
    assert model["positions"]["S"]["source"] == "pooled"
    assert model["positions"]["S"]["phi"] == model["pooled"]["phi"]
    assert model["positions"]["RB"]["source"] == "fitted"


def test_coefficients_are_never_negative():
    """A negative variance coefficient is not a model."""
    model = wk.fit(frame=synthetic())
    for entry in model["positions"].values():
        assert entry["phi"] >= 0.0 and entry["inv_k"] >= 0.0


def test_the_model_records_what_it_trained_on():
    model = wk.fit(frame=synthetic())
    assert model["version"] == wk.MODEL_VERSION
    assert model["n_rows"] > 0


# --- per-player sd -------------------------------------------------------

def test_sd_grows_with_the_projection():
    model = {"pooled": {"phi": 1.0, "inv_k": 0.0}, "positions": {}}
    assert wk.player_sd(model, "RB", 100.0) > wk.player_sd(model, "RB", 25.0)


def test_a_proportional_term_widens_a_small_projection_relatively():
    """The reason plan 28 uses two terms: `mu^2/k` keeps the *ratio* from collapsing."""
    linear = {"pooled": {"phi": 4.0, "inv_k": 0.0}, "positions": {}}
    both = {"pooled": {"phi": 4.0, "inv_k": 0.25}, "positions": {}}
    assert (wk.player_sd(both, "RB", 20.0) / 20.0
            > wk.player_sd(linear, "RB", 20.0) / 20.0)


def test_a_zero_or_missing_projection_has_no_spread():
    model = {"pooled": {"phi": 4.0, "inv_k": 0.0}, "positions": {}}
    assert wk.player_sd(model, "RB", 0.0) == 0.0
    assert wk.player_sd(model, "RB", None) == 0.0
    assert wk.player_sd(model, "RB", -5.0) == 0.0


def test_an_unknown_position_uses_the_pooled_fit():
    model = {"pooled": {"phi": 9.0, "inv_k": 0.0}, "positions": {}}
    assert wk.player_sd(model, "Punter", 1.0) == pytest.approx(3.0)


def test_team_sd_is_the_independent_sum():
    model = {"pooled": {"phi": 1.0, "inv_k": 0.0}, "positions": {}}
    assert wk.team_sd(model, [("RB", 9.0), ("RB", 16.0)]) == pytest.approx(5.0)


# --- the probability -----------------------------------------------------

def test_equal_projections_and_spreads_are_a_coin_flip():
    assert wk.win_probability(100.0, 20.0, 100.0, 20.0) == pytest.approx(0.5)


def test_a_lead_of_one_combined_sd_is_about_84_percent():
    """Φ(1) = 0.841. The closed form, checked against the constant."""
    spread = (20.0 ** 2 + 20.0 ** 2) ** 0.5
    assert wk.win_probability(100.0 + spread, 20.0, 100.0, 20.0) == pytest.approx(
        0.8413, abs=1e-3)


def test_with_no_spread_the_higher_projection_wins_outright():
    assert wk.win_probability(101.0, 0.0, 100.0, 0.0) == 1.0
    assert wk.win_probability(99.0, 0.0, 100.0, 0.0) == 0.0


def test_a_tie_with_no_spread_is_the_only_honest_fifty():
    assert wk.win_probability(100.0, 0.0, 100.0, 0.0) == 0.5


def test_the_normal_cdf_matches_known_values():
    assert wk.normal_cdf(0.0) == pytest.approx(0.5)
    assert wk.normal_cdf(1.6449) == pytest.approx(0.95, abs=1e-4)


# --- the gates -----------------------------------------------------------

def test_a_well_specified_model_passes_every_gate():
    frame = synthetic(phi=4.0)
    report = wk.backtest(wk.fit(frame=frame), frame=frame)
    assert report["calibrated"]
    assert all(report["gates"].values())
    assert 0.77 <= report["coverage"] <= 0.83
    assert report["brier"] < report["brier_uninformative"]


def test_the_backtest_evaluates_every_within_week_pair():
    """Not the real schedule: `team_stats` records the fixtures and had never been
    built, and calibration does not depend on the fixture list. All pairs is a
    larger, unbiased set -- 9,940 matchups against the ~900 a schedule offers."""
    frame = synthetic(n_leagues=1, n_weeks=2, n_teams=4)
    report = wk.backtest(wk.fit(frame=frame), frame=frame)
    assert report["team_weeks"] == 8            # 2 weeks x 4 teams
    # C(4,2) = 6 pairs per week, each emitted in both directions.
    assert report["matchups"] <= 2 * 6 * 2


def test_an_understated_spread_fails_the_coverage_gate():
    """The failure mode the gates exist to catch: intervals too narrow, so a
    close matchup reads as a decided one."""
    frame = synthetic(phi=4.0)
    honest = wk.fit(frame=frame)
    narrow = {
        **honest,
        "pooled": {"phi": honest["pooled"]["phi"] / 25, "inv_k": 0.0},
        "positions": {p: {**e, "phi": e["phi"] / 25, "inv_k": 0.0}
                      for p, e in honest["positions"].items()},
    }
    report = wk.backtest(narrow, frame=frame)
    assert not report["calibrated"]
    assert not report["gates"]["G-W1"]
    assert not report["gates"]["G-W4"]


def test_an_overstated_spread_also_fails():
    frame = synthetic(phi=4.0)
    honest = wk.fit(frame=frame)
    wide = {
        **honest,
        "pooled": {"phi": honest["pooled"]["phi"] * 25, "inv_k": 0.0},
        "positions": {p: {**e, "phi": e["phi"] * 25, "inv_k": 0.0}
                      for p, e in honest["positions"].items()},
    }
    report = wk.backtest(wide, frame=frame)
    assert not report["gates"]["G-W1"]


def test_the_backtest_is_deterministic():
    """The same inputs must give the same gate result.

    They did not. The first version emitted each pair in one direction only, so which
    team was listed first decided which decile a matchup landed in -- (A, B)
    contributes ``p`` and (B, A) contributes ``1 - p``. G-W2 came back 0.046, 0.050 and
    0.078 on identical inputs, purely on Polars' group-iteration order, and Brier and
    accuracy were symmetric under the flip so neither noticed. A gate that is a
    function of iteration order is not a gate.
    """
    frame = synthetic(n_leagues=2, n_weeks=6, n_teams=8)
    model = wk.fit(frame=frame)
    reports = [wk.backtest(model, frame=frame) for _ in range(3)]
    assert len({r["matchups"] for r in reports}) == 1
    # Exact to float summation order, which is all that is left: the residual spread
    # is ~1e-16, against the ~0.03 the row-order dependence was worth.
    first = reports[0]["worst_decile"]
    for report in reports[1:]:
        assert report["worst_decile"] == pytest.approx(first, abs=1e-12)


def test_the_calibration_curve_is_symmetric():
    """Both directions of every pair, so the curve mirrors around 0.5.

    Not decoration: it is what makes :func:`backtest` independent of row order.
    """
    frame = synthetic(n_leagues=2, n_weeks=6, n_teams=8)
    report = wk.backtest(wk.fit(frame=frame), frame=frame)
    assert report["matchups"] % 2 == 0
    # Mean predicted probability over both directions of every pair is exactly 0.5.
    weighted = sum(d["predicted"] * d["n"] for d in report["deciles"])
    assert weighted / sum(d["n"] for d in report["deciles"]) == pytest.approx(0.5,
                                                                             abs=0.01)


def test_every_gate_has_a_description():
    """A gate nobody wrote down is not pre-committed."""
    frame = synthetic(n_leagues=1, n_weeks=4, n_teams=6)
    report = wk.backtest(wk.fit(frame=frame), frame=frame)
    assert set(report["gates"]) == set(wk.GATES)


# --- persistence ---------------------------------------------------------

def test_a_model_round_trips(tmp_path, monkeypatch):
    monkeypatch.setattr(wk, "MODEL_PATH", tmp_path / "weekly.json")
    model = wk.fit(frame=synthetic(n_leagues=1, n_weeks=4, n_teams=6))
    wk.write(model)
    assert wk.load()["positions"]["RB"]["phi"] == pytest.approx(
        model["positions"]["RB"]["phi"])


def test_a_missing_model_says_how_to_fit_it(tmp_path, monkeypatch):
    monkeypatch.setattr(wk, "MODEL_PATH", tmp_path / "absent.json")
    with pytest.raises(FileNotFoundError, match="--fit"):
        wk.load()


def test_a_model_from_another_version_is_refused_rather_than_reinterpreted(
        tmp_path, monkeypatch):
    """Its coefficients would still parse. That is exactly the hazard."""
    path = tmp_path / "weekly.json"
    monkeypatch.setattr(wk, "MODEL_PATH", path)
    import json
    path.write_text(json.dumps({"version": "0.0.1", "pooled": {},
                                "positions": {}}))
    with pytest.raises(ValueError, match="Refit"):
        wk.load()
