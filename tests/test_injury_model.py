"""The fitted recovery curve, the hazard, and the guards that keep them honest.

The most important test in this file is :func:`test_a_null_effect_fits_to_no_effect`. The
whole design is arranged around one measurement -- that healthy players score below their
own four-game baseline too -- and the failure mode is a curve that attributes that skew to
injury and haircuts every returning player for reasons that have nothing to do with his
ankle. So there is a test that feeds in injured and control cohorts which are statistically
identical and requires the fit to find nothing.

The rest pin the properties the model claims: monotone, asymptotic to 1.0, bounded ``tau``,
reproducible, shrinking toward the parent when thin, and abstaining rather than shipping a
number inside its own error bars.

Synthetic frames, plus a handful of assertions against the shipped artifact where the point
is that the *published* numbers hold.
"""

import math

import numpy as np
import polars as pl
import pytest

from Scripts.injury import episodes as ep
from Scripts.injury import lexicon
from Scripts.injury import model as im


# --- synthetic cohorts ----------------------------------------------------

def cohort(effect, episodes=120, base=15.0, seed=1, season=2024):
    """Post-return rows whose net multiplier is ``effect(appearance)`` by construction.

    Deterministic magnitudes with a fixed jitter, so a test asserting "the fit recovers
    what was put in" is testing the fit and not the random number generator.
    """
    rng = np.random.default_rng(seed)
    rows = []
    for episode in range(episodes):
        for appearance in range(1, ep.POST_RETURN_WINDOW + 1):
            rows.append({
                "gsis_id": f"P{episode:04d}", "season": season, "run": 1,
                "week": 4 + appearance, "position": "WR",
                "body_part": "ankle", "body_part_unknown": False,
                "weeks_out": 2, "duration_bucket": "2", "strong_share": 1.0,
                "base_pts": base, "base_snap": 0.8, "baseline_n": 4,
                "appearance_back": appearance,
                "fantasy_points_ppr": max(
                    base * effect(appearance) + rng.normal(0, 2.0), 0.0),
                "offense_pct": 0.8, "return_week": 5,
            })
    frame = pl.DataFrame(rows)
    return frame.with_columns(
        (pl.col("fantasy_points_ppr") / pl.col("base_pts")).alias("pts_ratio"),
        (pl.col("offense_pct") / pl.col("base_snap")).alias("snap_ratio"))


def controls(effect=lambda w: 1.0, rows=4000, base=15.0, seed=2, season=2024):
    rng = np.random.default_rng(seed)
    built = []
    for i in range(rows):
        appearance = 1 + i % ep.POST_RETURN_WINDOW
        built.append({
            "gsis_id": f"C{i:05d}", "season": season, "position": "WR",
            "anchor": 5, "appearance_back": appearance,
            "fantasy_points_ppr": max(base * effect(appearance) + rng.normal(0, 2.0),
                                      0.0),
            "base_pts": base, "base_snap": 0.8,
        })
    frame = pl.DataFrame(built)
    return frame.with_columns(
        (pl.col("fantasy_points_ppr") / pl.col("base_pts")).alias("pts_ratio"),
        pl.lit(1.0).alias("snap_ratio"))


def episode_table(episodes=120, season=2024, body_part="ankle", recurred=False):
    return pl.DataFrame([{
        "gsis_id": f"P{i:04d}", "season": season, "run": 1, "outcome": "returned",
        "body_part": body_part, "body_part_unknown": False, "weeks_out": 2,
        "return_week": 5, "last_out_week": 4, "first_out_week": 3,
        "weeks_to_recurrence": 2 if (recurred and i % 10 == 0) else None,
        "recurred": bool(recurred and i % 10 == 0),
    } for i in range(episodes)])


# --- the placebo guard ---------------------------------------------------

def test_a_null_effect_fits_to_no_effect():
    """**The test this module exists for.**

    Injured and control cohorts drawn from the same distribution. A curve fitted against
    1.0 instead of against the control would read the shared right-skew as injury; fitted
    against the control it has to find nothing, and the abstention rule has to catch what
    little noise survives."""
    post = cohort(lambda w: 1.0, episodes=120, seed=11)
    ctl = controls(lambda w: 1.0, rows=4000, seed=12)
    model = im.fit(post, ctl, episode_table(), draws=40)

    for appearance in range(1, ep.POST_RETURN_WINDOW + 1):
        assert model.multiplier("ankle", appearance) == 1.0


def test_a_shared_downward_skew_is_not_read_as_injury():
    """Both cohorts sit 15% below their own baselines -- which is what a selected four-game
    mean does to a right-skewed target. None of it is injury."""
    post = cohort(lambda w: 0.85, episodes=120, seed=13)
    ctl = controls(lambda w: 0.85, rows=4000, seed=14)
    model = im.fit(post, ctl, episode_table(), draws=40)

    assert model.multiplier("ankle", 1) == 1.0


def test_a_real_effect_on_top_of_the_skew_is_recovered():
    """The other half of the guard: the correction must not remove a genuine effect."""
    post = cohort(lambda w: 0.85 * (1 - 0.25 * math.exp(-(w - 1) / 1.0)), episodes=140,
                  seed=15)
    ctl = controls(lambda w: 0.85, rows=4000, seed=16)
    model = im.fit(post, ctl, episode_table(episodes=140), draws=40)

    cell = model.cells["ankle|*"]
    assert not cell.abstained
    assert cell.a == pytest.approx(0.25, abs=0.10)
    assert model.multiplier("ankle", 1) < 0.90
    assert model.multiplier("ankle", 6) > 0.95


# --- shape ---------------------------------------------------------------

def test_the_curve_is_monotone_and_asymptotes_to_one():
    values = [float(im.curve(0.3, 1.5, w)) for w in range(1, 20)]
    assert values == sorted(values)
    assert values[0] == pytest.approx(0.7)
    assert values[-1] == pytest.approx(1.0, abs=1e-3)


def test_every_shipped_cell_is_monotone():
    """Over the artifact, not over a synthetic fit -- the point is that the *published*
    numbers cannot say a player gets worse as he heals."""
    model = im.InjuryModel.load()
    for cell in model.cells.values():
        ramp = model.ladder(cell.body_part, cell.duration_bucket)
        assert ramp == sorted(ramp), f"{cell.key} is not monotone: {ramp}"
        assert ramp[-1] <= 1.0


def test_the_weekly_residual_is_monotone_too():
    """It was not, before ESPN's flat ramp was carried past its measured window: a knee came
    out at 1.00 in the third appearance and 0.95 in the fourth, purely because the
    denominator vanished."""
    model = im.InjuryModel.load()
    for part in ("__global__", "knee", "foot_toe", "shoulder", "hamstring"):
        ramp = [model.multiplier(part, w, net_of_espn=True)
                for w in range(1, ep.POST_RETURN_WINDOW + 1)]
        assert ramp == sorted(ramp), f"{part} residual is not monotone: {ramp}"


def test_tau_cannot_exceed_the_observation_window():
    """An identifiability bound. Unbounded, the ankle cell came back at ``tau = 112`` -- the
    optimiser's way of writing a flat line -- which asserts a permanent talent reduction
    from a sprain and triples what the season multiplier charges."""
    model = im.InjuryModel.load()
    for cell in model.cells.values():
        assert cell.tau <= im.MAX_TAU + 1e-9


def test_the_multiplier_is_never_null_and_never_below_the_floor():
    model = im.InjuryModel.load()
    for part in list(lexicon.GROUPS) + ["nonsense"]:
        for appearance in range(0, 12):
            value = model.multiplier(part, appearance)
            assert value == value                      # not nan
            assert im.MULTIPLIER_FLOOR <= value <= 1.0


def test_outside_the_window_the_multiplier_is_one():
    """Extrapolating a two-parameter exponential past its data is how a model invents a
    tail."""
    model = im.InjuryModel.load()
    assert model.multiplier("knee", ep.POST_RETURN_WINDOW + 1) == 1.0
    assert model.multiplier("knee", 0) == 1.0


def test_a_group_with_no_recovery_mechanism_is_never_discounted():
    """An illness costs availability and nothing else -- there is no tissue healing on a
    timetable, so a post-return efficiency ramp has no mechanism behind it."""
    model = im.InjuryModel.load()
    for part in lexicon.RECOVERY_EXCLUDED_GROUPS:
        assert model.ladder(part) == [1.0] * ep.POST_RETURN_WINDOW


# --- shrinkage and abstention --------------------------------------------

def test_a_thin_cell_takes_its_parents_numbers_and_says_so():
    thin = (cohort(lambda w: 0.5, episodes=4, seed=22)
            .with_columns(pl.lit("knee").alias("body_part"),
                          pl.lit("5+").alias("duration_bucket")))
    post = pl.concat([cohort(lambda w: 0.8, episodes=100, seed=21), thin])
    model = im.fit(post, controls(rows=3000, seed=23),
                   episode_table(episodes=104), draws=30)
    cell = model.cells["knee|5+"]
    assert cell.episodes < im.MIN_CELL_EPISODES
    assert cell.shrunk_from in ("parent", "combined")


def test_shrinkage_moves_a_cell_further_toward_its_parent_the_thinner_it_is():
    own, parent = (0.40, 2.0), (0.10, 1.0)
    distances = [abs(im._shrink(own, parent, n, 20.0)[0] - parent[0])
                 for n in (5, 20, 100, 500)]
    assert distances == sorted(distances)


def test_shrinkage_cannot_leave_the_valid_region():
    a, tau = im._shrink((0.999, 5.9), (0.001, 0.01), 3, 80.0)
    assert 0.0 < a < 1.0
    assert 0.0 < tau <= im.MAX_TAU


def test_a_shortfall_inside_two_standard_errors_abstains():
    """A number the model cannot stand behind is worse than no number, because it is
    applied by arithmetic and nothing downstream can tell it was a guess."""
    post = cohort(lambda w: 1.0 - 0.02 * math.exp(-(w - 1)), episodes=60, seed=31)
    model = im.fit(post, controls(rows=3000, seed=32), episode_table(episodes=60),
                   draws=60)
    cell = model.cells["ankle|*"]
    assert cell.abstained
    assert "standard error" in cell.reason
    assert model.multiplier("ankle", 1) == 1.0


def test_the_shipped_model_abstains_where_the_evidence_said_it_should():
    """Concussions and lower-body soft tissue showed no lasting efficiency cost in the
    measurement, and the fit has to reach the same conclusion on its own."""
    model = im.InjuryModel.load()
    assert model.cells["concussion|*"].abstained
    assert model.cells["soft_tissue_lower|*"].abstained


# --- the combined cell --------------------------------------------------

def test_a_joint_cell_inherits_from_both_of_its_parents():
    """Body part and duration are each well powered and their joint cells are not -- the
    largest holds 45 episodes. Combining them uses both signals; picking one discards the
    other."""
    model = im.InjuryModel.load()
    combined = [c for c in model.cells.values() if c.shrunk_from == "combined"]
    assert combined, "no cell was combined, so the fallback is untested"
    for cell in combined:
        assert "+" in (cell.parent or "")


def test_duration_moves_a_body_parts_ladder():
    """A one-game knee and a six-game knee must not get the same curve."""
    model = im.InjuryModel.load()
    short = model.ladder("knee", "1")
    long = model.ladder("knee", "5+")
    assert long[0] < short[0]
    assert long[-1] < short[-1]


def test_a_joint_cell_abstains_only_when_both_parents_do():
    """"No measurable effect for concussions in general" and "three-to-four-game absences
    cost 19%" are different claims, and a concussion that cost four games is not the average
    concussion."""
    part = im.RecoveryCell("concussion", None, 0.05, 0.5, 0.10, 0.2, 70, 200,
                           "__global__|*", "own", True, "")
    bucket = im.RecoveryCell("__global__", "3-4", 0.19, 1.5, 0.05, 0.3, 130, 400,
                             "__global__|*", "own", False, "")
    root = im.RecoveryCell("__global__", None, 0.16, 1.1, 0.03, 0.2, 450, 2000,
                           None, "own", False, "")
    combined = im._combine(part, bucket, root, "concussion", "3-4", 8, 30)
    assert not combined.abstained


# --- the hazard ----------------------------------------------------------

def test_hazard_probabilities_are_probabilities():
    model = im.InjuryModel.load()
    for part in model.hazard.by_body_part:
        for week in range(1, im.HAZARD_WINDOW + 1):
            value = model.hazard.weekly(part, 3.0, week)
            assert 0.0 < value < 1.0


def test_the_cumulative_risk_compounds_rather_than_sums():
    """Summing weekly probabilities double-counts and can exceed 1.0."""
    model = im.InjuryModel.load()
    weekly = [model.hazard.weekly("hamstring", 3.0, w) for w in range(1, 7)]
    cumulative = model.hazard.cumulative("hamstring", 3.0, 6)
    assert cumulative < sum(weekly)
    assert 0.0 < cumulative < 1.0


def test_the_cumulative_risk_grows_with_the_horizon():
    model = im.InjuryModel.load()
    values = [model.hazard.cumulative("hamstring", 3.0, w) for w in range(1, 7)]
    assert values == sorted(values)


def test_the_shipped_hamstring_recurrence_matches_the_published_figure():
    """External validation, and the reason to believe the episode construction at all.
    Jenkins et al. put same-season hamstring reinjury at 11.9%; this repo's own logic, from a
    different source with a different definition, lands at 9.9%. Drift outside the interval
    means the episode boundaries have moved."""
    from Scripts.lab import registry as reg

    model = im.InjuryModel.load()
    low, high = reg.HAMSTRING_RECURRENCE_RANGE
    assert low <= model.reinjury_probability("hamstring", 3.0) <= high


def test_a_group_excluded_from_the_curve_carries_no_recurrence_risk_either():
    model = im.InjuryModel.load()
    for part in lexicon.RECOVERY_EXCLUDED_GROUPS:
        assert model.reinjury_probability(part, 3.0) == 0.0


def test_recurrence_feeds_games_lost_not_the_efficiency_ramp():
    """A recurrence is a new absence, so it costs games. It must never be folded into the
    multiplier, which prices being on the field and off form."""
    model = im.InjuryModel.load()
    lost = model.expected_games_lost_to_reinjury("hamstring", 4.0)
    assert lost == pytest.approx(model.reinjury_probability("hamstring", 4.0) * 4.0)


def test_person_periods_stop_at_the_recurrence():
    """A recurrence in week 3 is two clean weeks and one event, not one row saying so."""
    periods = im.person_periods(episode_table(episodes=1, recurred=True))
    assert periods.height == 2
    assert periods["event"].sum() == 1
    assert periods["event"].to_list() == [0, 1]


def test_an_unknown_body_part_contributes_no_person_periods():
    table = episode_table(episodes=4).with_columns(
        pl.lit(True).alias("body_part_unknown"))
    assert im.person_periods(table).is_empty()


# --- the ESPN residual --------------------------------------------------

def test_the_residual_only_ever_reduces_the_haircut():
    """If ESPN happened to price more than the fitted drop at some appearance, the residual
    is nothing -- not a bonus."""
    model = im.InjuryModel.load()
    model.espn = im.EspnRamp(by_appearance={"1": 0.50}, episodes=10, seasons=[2025])
    assert model.multiplier("knee", 1, net_of_espn=True) == 1.0


def test_espn_pricing_is_carried_forward_not_reset():
    ramp = im.EspnRamp(by_appearance={"1": 0.92, "2": 0.93, "3": 0.91}, episodes=108,
                       seasons=[2025])
    assert ramp.priced(3) == 0.91
    assert ramp.priced(6) == 0.91          # carried, not 1.0
    assert ramp.priced(1) == 0.92


def test_with_no_measurement_the_residual_is_the_raw_curve():
    """The season path, where ESPN publishes before anyone is recently returned and there is
    no ramp to net against."""
    model = im.InjuryModel.load()
    model.espn = im.EspnRamp()
    for w in range(1, ep.POST_RETURN_WINDOW + 1):
        assert (model.multiplier("knee", w, net_of_espn=True)
                == model.multiplier("knee", w))


# --- the season translation ---------------------------------------------

def test_the_season_multiplier_shrinks_as_more_games_remain():
    """The ramp costs a fixed quantity of partial games, so its share of a season falls the
    earlier the player is back."""
    model = im.InjuryModel.load()
    early = model.season_multiplier("knee", 14.0)
    late = model.season_multiplier("knee", 4.0)
    assert late < early < 1.0


def test_no_games_remaining_costs_nothing():
    model = im.InjuryModel.load()
    assert model.season_multiplier("knee", 0.0) == 1.0


# --- the artifact -------------------------------------------------------

def test_save_and_load_round_trip_every_multiplier(tmp_path):
    model = im.InjuryModel.load()
    reloaded = im.InjuryModel.load(model.save(tmp_path / "m.json"))
    for cell in model.cells.values():
        for w in range(1, ep.POST_RETURN_WINDOW + 1):
            assert (reloaded.multiplier(cell.body_part, w, cell.duration_bucket)
                    == model.multiplier(cell.body_part, w, cell.duration_bucket))


def test_the_artifacts_lexicon_matches_the_code_that_produced_it():
    """The group a body part lands in *is* the cell whose coefficients were fitted. If the
    mapping lives only in code, editing it silently repoints a body part at coefficients
    never fitted for it. Same pinning as ``ROLE_WITHDRAWN_EVIDENCE``."""
    assert im.InjuryModel.load().body_part_map == lexicon.as_dict()


def test_a_missing_artifact_names_the_command_that_builds_it(tmp_path):
    with pytest.raises(FileNotFoundError, match="Scripts.injury.model --fit"):
        im.InjuryModel.load(tmp_path / "absent.json")


def test_the_artifact_records_what_it_was_trained_on():
    model = im.InjuryModel.load()
    assert model.train_seasons
    assert model.fitted_at
    assert model.is_stale(max(model.train_seasons) + 1)
    assert not model.is_stale(max(model.train_seasons))


def test_two_fits_of_the_same_data_agree():
    """``unique()`` is unordered, so a seeded RNG indexing into it drew a different bootstrap
    sample every run -- which moved the standard errors, which moved the abstention
    decisions, which moved the walk-forward's chosen shrinkage. A fitted artifact has to be
    reproducible or its provenance means nothing."""
    post = cohort(lambda w: 0.85, episodes=60, seed=41)
    ctl = controls(rows=2000, seed=42)
    table = episode_table(episodes=60)
    first = im.fit(post, ctl, table, draws=30)
    second = im.fit(post, ctl, table, draws=30)
    assert ([c.a_sd for c in first.cells.values()]
            == [c.a_sd for c in second.cells.values()])
