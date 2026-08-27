"""What an experiment is, which ones exist, and what counts as passing.

The decision rule at the bottom of this module is written **before** the
experiments run and is applied mechanically. That is the whole point of putting it
in code: a threshold chosen after seeing the numbers is not a threshold, and a
model built by keeping whatever happened to look good on one walk-forward is how
you acquire six features that each add 0.003 and collectively add nothing.

It mirrors the criterion plan 21 already applied by hand -- "a feature keeps its
place only if it moves the numbers" -- and makes the three ways a feature can fail
explicit: it can fail to help on average, it can help on average while hurting a
position, and it can improve ordering while making the point estimates worse.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from Scripts.usage import features as ft
from Scripts.usage import season as sn

#: Mean within-position Spearman gain a feature must clear to be merged.
#:
#: 0.005 is roughly a tenth of what the depth chart was worth (+0.048 R-squared on
#: veteran carries, +0.05 to +0.07 Spearman across positions) and comfortably above
#: what the rejected coach priors managed (at most 0.0012 in either direction). It
#: is a low bar deliberately: the point of the rule is to exclude noise and
#: everything that quietly costs a position, not to demand another depth chart.
MIN_MEAN_SPEARMAN_GAIN: float = 0.005

#: How much a single position may lose even when the mean improves.
#:
#: Without this a feature can post a good average by helping wide receivers, where
#: n is largest, while making tight ends worse -- and a draft board is read one
#: position at a time.
MAX_POSITION_SPEARMAN_LOSS: float = 0.005

#: How many positions may lose top-N hit rate. One is tolerated as noise; two is a
#: pattern. Top-N is the metric closest to what a draft actually needs, and it is
#: also the noisiest, which is why it constrains rather than decides.
MAX_TOP_N_REGRESSIONS: int = 1

#: How much mean per-stat MAE may worsen, in percent, and how much any single stat may.
#:
#: **This clause was specified in the plan and missing from the implementation until
#: the ridge sweep found the gap.** The first three clauses all judge *ordering*, and
#: for a feature that is nearly the whole story. For a change to the estimator it is
#: not: heavy ridge improved within-position Spearman monotonically all the way to
#: alpha 300 while making every per-stat MAE monotonically worse -- +20.6% on passing
#: yards at the end of that curve -- and two settings passed a rule that never looked.
#:
#: MAE has to be a gate here because of what this model *is* in the pipeline. `USG_`
#: is not a rank the board consumes; it is a **stat line** that
#: :func:`Scripts.projection_utils.compute_weighted_stats` averages with ESPN's and
#: FantasyPros' before anything is priced. A source whose ordering improved while its
#: yardage got 8% worse makes the blend worse and the board's own ranking better only
#: incidentally. Plan 18 already names the failure this protects against: a model
#: that quietly emits a positional average looks like full coverage and drags the
#: blend toward the mean for exactly the players a board must differentiate.
MAX_MEAN_MAE_INCREASE_PCT: float = 0.5
MAX_STAT_MAE_INCREASE_PCT: float = 2.0


# --- the injury model, plan 27 --------------------------------------------
#
# A separate block because it judges a different kind of thing. The clauses above score a
# *source* -- ordering and per-stat accuracy for a stat line the blend averages. An injury
# multiplier is not a source: it moves an already-blended number, on a small subpopulation,
# and the question is whether the movement is real per player rather than only in the mean.
#
# Written before the walk-forward ran, and the numbers are sized from the measurement in
# ``docs/plans/27-injury-model.md``, not chosen to be clearable.

#: Post-return MAE improvement, in percent, that the multiplier must deliver.
#:
#: Sized from the effect and the noise. The deconfounded first-appearance drop is ~14% and
#: the weekly residual standard deviation is ~6.4 fantasy points (plan 19's table), so a 14%
#: haircut on a 12-point player is 1.7 points against a 6.4-point sigma. Two percent is
#: about the smallest MAE movement that is distinguishable from noise on a few hundred
#: post-return appearances, and anything under it is not evidence of anything.
#:
#: **Scored on post-return appearances only.** Pooled over every player-week the layer
#: touches a few hundred rows out of tens of thousands and any real effect disappears into
#: the denominator -- which would let a useless multiplier pass by dilution.
MIN_POST_RETURN_MAE_GAIN_PCT: float = 2.0

#: How much the multiplier may worsen MAE on *matched healthy* appearances.
#:
#: The false-positive clause, and the one that catches the failure this whole design is
#: arranged against. Applied to control rows the multiplier should do nothing useful: they
#: are not injured. If discounting healthy comparables *also* improves their MAE, the curve
#: is not measuring injury at all -- it is measuring regression toward the mean off a
#: selected four-game baseline, which the placebo correction exists to remove. Half a percent
#: is tight because the correct answer here is zero.
MAX_CONTROL_MAE_INCREASE_PCT: float = 0.5

#: Slope of realised outcome regressed on predicted multiplier, by decile.
#:
#: The number to weight most heavily, because it is the one that separates "real in the
#: mean" from "usable per player". A slope near 1 means a cell predicted to lose 20% loses
#: about 20%. A slope of 0.2 means the ordering is right and the magnitude is mostly noise --
#: directionally informative, and not something to multiply a projection by.
MIN_CALIBRATION_SLOPE: float = 0.4

#: How far any position x ADP band's median TRUE/ESPN ratio may move, as a fraction.
#:
#: Cross-position neutrality, the invariant commit 3a403bb exists to protect: a layer that
#: touches only skill positions introduced 11%+ band distortion once already.
#: ``store.calibration_summary()`` is the persisted regression test. The layer bites only on
#: currently-injured players, so a whole-band move above one percent means it is biting
#: somewhere it should not.
MAX_BAND_RATIO_DRIFT: float = 0.01

#: Ratio of the fitted hazard's Brier score to a constant base rate's.
#:
#: A miscalibrated hazard corrupts channel A downstream, since expected games lost is the
#: recurrence probability times a duration. Beating a constant by 2% is a low bar and it is
#: deliberately low: the event is rare (~1% a week), so Brier is dominated by the base rate
#: and a large ratio improvement is not available at any skill level. If even this cannot be
#: cleared, the weekly hazard is not a weekly predictor and only the pooled per-body-part
#: rate should ship.
MAX_HAZARD_BRIER_RATIO: float = 0.98

#: Episodes a per-body-part recurrence rate needs before it is quoted.
MIN_RECURRENCE_EPISODES: int = 40

#: Where the published NFL same-season hamstring reinjury rate must land.
#:
#: External validation, and the reason to believe the episode construction at all. Jenkins
#: et al. put same-season hamstring reinjury at 11.9%; this repo's own episode logic, built
#: from a different source with a different definition, produces 9.9%. A drift outside this
#: interval means the episode boundaries have moved, not that the league has changed.
HAMSTRING_RECURRENCE_RANGE: Tuple[float, float] = (0.09, 0.15)


# --- outcome distributions, plan 28 ---------------------------------------
#
# A fourth block, judging a third kind of thing. The first block scores a *source* -- is
# its stat line better. The second scores a *multiplier* on an already-blended number.
# These score a **distribution**, where the failure mode is different again: a
# distribution can have a perfect mean and still be useless, and an uncalibrated one is
# worse than a point estimate because it invites acting on its tails.
#
# Written before the walk-forward ran. The numbers are transcribed from
# ``docs/plans/28-outcome-distributions.md``, which pre-committed them before any of this
# was built.

#: Where realised 80% interval coverage must land.
#:
#: The gate that decides whether any of this is publishable. A p10-p90 band should contain
#: 80% of outcomes; much more and it is uselessly wide, much less and it is lying. The
#: window is +-8 points rather than tighter because the walk-forward is seven folds of a
#: few hundred players and the binomial standard error on a coverage estimate at n = 400
#: is about 2 points -- a window inside three of those would reject on noise.
#:
#: **Unchanged since it was pre-committed. What was corrected on 2026-08-25 is the
#: population it is measured over**, and the correction makes the gate harder rather than
#: easier, which is the only direction a post-hoc change to a gate can honestly move.
#: See :data:`MIN_SCORED_PROJECTION`.
OUTCOME_COVERAGE_RANGE: Tuple[float, float] = (0.72, 0.88)

#: Projected season points below which a row does not count toward coverage.
#:
#: **The gate was passing on players nobody drafts.** Measured over 2021-2025, **32% of
#: the scored sample projects under 10 points, realises a median of exactly 0.0, and is
#: "covered" at 0.825** -- its interval contains the zero it was always going to produce.
#: Pooled with everyone else that pulled reported coverage to 0.732 and inside the window.
#: Strip them and it is **0.688**, outside it.
#:
#: Twenty-five points is about 1.5 a game, below which nobody is rostered in any of the
#: nine leagues. **The exact cut is not load-bearing and that is checkable**: coverage is
#: 0.687 at a 10-point floor, 0.688 at 25, 0.701 at 50 and 0.722 at 100. Every cut in that
#: range says the same thing and only the unfiltered population disagrees, which is what
#: identifies the unfiltered population as the artefact rather than the finding.
MIN_SCORED_PROJECTION: float = 25.0

#: Where the calibration slope must land.
#:
#: Coverage alone can be bought by a wide interval that is wide in the wrong places. The
#: slope of realised on predicted, by bin, is what says the *ordering* of uncertainty is
#: right: a player the model says is uncertain must actually be more uncertain. Centred on
#: 1.0 because unlike plan 27's multiplier -- where 0.4 was accepted as "directionally
#: informative" -- a distribution's whole claim is the magnitude.
OUTCOME_SLOPE_RANGE: Tuple[float, float] = (0.85, 1.15)

#: How much closer to nominal the joint draw must be than independent marginals.
#:
#: Five percentage points, on depth-rank >= 2 RBs and TEs. The room-level machinery is the
#: expensive half of plan 28 and this is what makes it earn its place: if independent
#: marginals already cover the backups, phase 1 ships alone. Sized against the ~2-point
#: standard error above, so it is a real gap rather than a fold-to-fold wobble.
MIN_JOINT_COVERAGE_GAIN_PP: float = 5.0

#: How much of that gain must be specific to backups rather than general.
#:
#: **The false-positive clause, and it is applied before the accuracy clause** -- the same
#: ordering, and for the same reason, as ``injury_verdict``'s control arm. A model that
#: widens every interval improves coverage everywhere, including for healthy entrenched
#: starters who have no vacancy to inherit. That is not the vacancy effect, it is a wider
#: interval, and reporting the coverage gain first would bury it.
#:
#: Three of the five points the gate requires, so a majority of the improvement has to be
#: where the mechanism claims it is.
MIN_VACANCY_SPECIFICITY_PP: float = 3.0

#: How wide the board's own floor-to-ceiling must be beaten to justify building at all.
#:
#: G-D0, measured 2026-08-24 and passed by 17.5x. Kept in code so the claim is
#: reproducible rather than a number in a document -- the original measurement's script
#: was never committed.
MIN_INTERVAL_WIDTH_RATIO: float = 1.5

#: How much of the draftable pool must move, and by how many picks, for the board's sort
#: to change.
#:
#: G-D3. If ordering by ``p_top12`` agrees with ordering by mean points, the columns ship
#: as diagnostics and the sort is left alone -- the plan-27 outcome, named in advance
#: again. Twelve picks is a round in a twelve-team league, which is the smallest move a
#: drafter would act on.
MIN_MOVED_SHARE: float = 0.05
MIN_PICK_MOVE: int = 12

#: How far ``TRUE_Points`` may move when the outcome columns are attached.
#:
#: Zero, to the byte. Plan 33's G-R3 set the precedent and passed it twice; the columns
#: are diagnostics until G-D3 says otherwise, and a diagnostic that moves a projection is
#: not a diagnostic.
MAX_TRUE_POINTS_DRIFT: float = 0.0


# --- role uncertainty as a variance channel, plan 33 phase 3 ---------------
#
# Transcribed from ``docs/plans/33-role-resolution.md``, which pre-committed it before
# any of this was built.

#: How far from nominal 80% the role-conditional interval may land.
#:
#: G-R2, and it is a *coverage* bar rather than a width bar because a wide interval is
#: trivially achievable and useless. Five points, which is tighter than plan 28's G-D1
#: window on the same quantity -- deliberately, because this is asked of a mechanism whose
#: entire claim is that it makes an existing interval better.
MAX_ROLE_COVERAGE_ERROR: float = 0.05


def role_verdict(metrics: Dict) -> Tuple[str, str]:
    """Whether role uncertainty earns a place in the interval. Plan 33's G-R2.

    Args:
        metrics: Carrying ``role_coverage`` and, optionally, ``incumbent_coverage`` --
            the share of realised outcomes inside the board's source-disagreement band.

    Returns:
        tuple: ``("merge", reason)`` or ``("reject", reason)``. A rejection leaves plan
        33 phases 1 and 2 standing, which that plan names in advance.
    """
    coverage = metrics.get("role_coverage")
    if coverage is None:
        return "reject", "no role-conditional coverage was measured."

    error = abs(coverage - 0.80)
    if error > MAX_ROLE_COVERAGE_ERROR:
        return "reject", (
            f"role-conditional coverage is {coverage:.3f}, {error * 100:.1f} points from "
            f"nominal against the {MAX_ROLE_COVERAGE_ERROR * 100:.0f} the rule allows -- "
            f"so the interval is not fit to replace anything, whatever it beats.")

    incumbent = metrics.get("incumbent_coverage")
    if incumbent is not None and abs(incumbent - 0.80) <= error:
        return "reject", (
            f"role-conditional coverage {coverage:.3f} is no closer to nominal than "
            f"source disagreement's {incumbent:.3f}.")

    return "merge", (
        f"role-conditional coverage {coverage:.3f} is within "
        f"{MAX_ROLE_COVERAGE_ERROR * 100:.0f} points of nominal.")


@dataclass(frozen=True)
class Experiment:
    """One thing to try, and everything needed to try it.

    Attributes:
        name: Stable identifier. Used as the results key, so renaming one orphans
            its history rather than updating it.
        hypothesis: What is expected to happen, in a sentence, written before the
            run. A hypothesis recorded afterwards is a description.
        source: Where the data comes from, for the ledger's feasibility column.
        feature_kwargs: Passed through to
            :func:`Scripts.usage.features.season_features`.
        volume_regressors: Replaces :data:`Scripts.usage.season.VOLUME_REGRESSORS`
            for the run. None leaves it alone.
        games_regressors: Replaces :data:`Scripts.usage.season.GAMES_REGRESSORS`.
        shrinkage: Replaces :data:`Scripts.usage.features.SHRINKAGE_K`, per rate.
            Partial: keys given override, the rest keep the shipped value. Added
            2026-08-27, because :mod:`Scripts.lab.persistence` measured every shipped
            constant to sit **below** its credibility floor -- by 1.4x for catch rate
            to 4.6x for yards per attempt -- and there was no way to test a different
            one through the walk-forward. A floor is an argument, not a result; this
            is what turns it into one.
        note: Anything the ledger should carry that the numbers do not say.
    """

    name: str
    hypothesis: str
    source: str
    feature_kwargs: Dict[str, object] = field(default_factory=dict)
    volume_regressors: Optional[Tuple[str, ...]] = None
    games_regressors: Optional[Tuple[str, ...]] = None
    rate_baseline_features: Optional[Dict[str, Tuple[str, ...]]] = None
    ridge_alpha: Optional[float] = None
    shrinkage: Optional[Dict[str, float]] = None
    note: str = ""


#: The current shipped regressors, for building variants against.
BASE_VOLUME = sn.VOLUME_REGRESSORS
BASE_GAMES = sn.GAMES_REGRESSORS

#: The experiment queue, in the order plan 22 ranked them.
#:
#: Each varies **one** thing against the baseline. That is slower than testing a
#: combined feature set and it is the only way to know which part did the work --
#: plan 21 credited the rookie arm's gain to the depth chart and the coach prior
#: together, and separating them showed only the first earned it.
EXPERIMENTS: Tuple[Experiment, ...] = (
    Experiment(
        name="baseline",
        hypothesis="The shipped model, as the comparison every other row is read "
                   "against.",
        source="—",
    ),
    Experiment(
        name="efficiency_fitted_baseline",
        hypothesis="The efficiency head has never had a feature: it shrinks every "
                   "player toward his position's pooled rate. Replacing that "
                   "constant with a rate fitted on route depth, separation and "
                   "goal-line role should improve the rates most for the players "
                   "with least opportunity of their own, who are exactly the ones "
                   "the constant serves worst.",
        source="NGS receiving + rushing, red-zone play-by-play (2016-2025)",
        feature_kwargs={"rate_baselines": True},
        note="Probed before building: WR catch rate holdout R-squared 0.083 to "
             "0.136, TE yards-per-target 0.015 to 0.113. NGS covers only 35-75% "
             "of the relevant population, so the fallback to the positional "
             "constant carries a lot of rows.",
    ),
    Experiment(
        name="efficiency_no_ngs_gate",
        hypothesis="The first efficiency run was damped by its own coverage, and "
                   "the diagnostic says so precisely: the fitted prior reached "
                   "95.6% of the players who give a prior 6.8% weight and 0.4% of "
                   "those who give it 88.9%, because NGS's qualifying threshold is "
                   "a volume threshold and credibility weight is inversely "
                   "proportional to volume. The touchdown-rate baselines did not "
                   "have to inherit that: they are built on play-by-play field "
                   "position, which has no threshold, and only the stray ngs_adot "
                   "term gated them. Dropping it should let those two rates reach "
                   "the low-volume players where a prior actually decides the "
                   "number.",
        source="red-zone play-by-play only (79-96% coverage), NGS for the "
               "yardage rates as before",
        feature_kwargs={"rate_baselines": True},
        rate_baseline_features={
            **{k: v for k, v in ft.RATE_BASELINE_FEATURES.items()
               if k not in ("rec_td_per_target", "rush_td_per_carry")},
            "rec_td_per_target": ("rz10_target_share", "ez_targets_pg"),
            "rush_td_per_carry": ("rz5_carry_share", "rz10_carry_share"),
        },
        note="ngs_adot's coefficients in the touchdown fits were +0.0016 and "
             "+0.0010 -- near nothing, bought at the cost of most of the coverage. "
             "A cheap term that gates an expensive one.",
    ),
    Experiment(
        name="contracts_x_moved",
        hypothesis="A settled veteran's prior volume already encodes what his team "
                   "thinks of him, but a mover's describes a job he no longer has. "
                   "Contract value should help on changed-teams rows and nowhere "
                   "else, which is why it enters interacted rather than as a main "
                   "effect.",
        source="OverTheCap via nflreadr::load_contracts (98.1% of rostered players)",
        feature_kwargs={"contracts": True},
        volume_regressors=BASE_VOLUME + ("moved_contract_apy",
                                         "moved_contract_gtd",
                                         "moved_contract_new"),
        note="Changed teams is plan 18's worst thin-evidence flag at +32% median "
             "rank error. Probed positive at all four positions on that "
             "subpopulation and a wash or worse on the full one.",
    ),
    Experiment(
        name="red_zone_role_volume",
        hypothesis="Goal-line work is role rather than skill, and role is the only "
                   "thing that has ever moved this model. Prior-season red-zone "
                   "share should predict next-season volume beyond raw volume.",
        source="play-by-play by field position (79-96% coverage)",
        volume_regressors=BASE_VOLUME + ("p1_rz10_carry_share",
                                         "p1_rz5_carry_share",
                                         "p1_rz10_target_share"),
    ),
    Experiment(
        name="routes_volume",
        hypothesis="Route participation is opportunity stripped of whether the "
                   "quarterback liked him. Expected to fail: probed at +0.004 to "
                   "+0.014 holdout R-squared and no movement in ordering at all, "
                   "because routes correlate 0.88-0.91 with targets.",
        source="participation x play-by-play dropbacks (98-100% coverage)",
        volume_regressors=BASE_VOLUME + ("p1_route_share", "p1_routes_pg"),
        note="Run despite the negative probe, because the probe omitted depth_rank "
             "and this is the test that includes it.",
    ),
    *[
        Experiment(
            name=f"ridge_alpha_{int(alpha)}",
            hypothesis="Not a feature but the functional form. If the heads are "
                       "overfitting their seven regressors, regularising them "
                       "should help. Predicted beforehand to do nothing -- n runs "
                       "450 to 1,500 per position-target against seven terms, "
                       "which is comfortable for OLS -- and that prediction was "
                       "wrong: alpha 10 was the single largest effect measured in "
                       "this plan. The sweep exists because one good point is not "
                       "a result. A gain that holds across a wide range of alpha "
                       "is a real bias-variance trade; a gain that spikes at one "
                       "value is the test set being fitted.",
            source="—  (np.linalg.lstsq to standardised ridge)",
            ridge_alpha=float(alpha),
            note="Standardised, intercept unpenalised. Alpha is NOT tuned on these "
                 "folds -- the whole curve is reported and the shape is the "
                 "evidence. Selecting the best point here would be exactly the "
                 "fishing the decision rule exists to prevent.",
        )
        for alpha in (1, 3, 10, 30, 100, 300)
    ],
    # --- shrinkage, plan 34 -----------------------------------------------
    #
    # `Scripts.lab.persistence` inverted the credibility identity `n/(n+k) = r` at
    # each rate's median denominator, over 13,288 consecutive player-season pairs,
    # and every shipped constant came out **below** the implied floor:
    #
    #   catch_rate           54 vs   40      yards_per_carry       155 vs   60
    #   yards_per_target    103 vs   40      yards_per_attempt     618 vs  150
    #   rec_td_per_target   292 vs  120      pass_td_per_attempt  1054 vs  300
    #   rush_td_per_carry   290 vs  150      int_per_attempt      1394 vs  300
    #
    # The floor assumes a perfectly stable underlying rate, so real drift only raises
    # it -- which makes it an argument for shrinking harder, and an argument is not a
    # result. These put it through the same walk-forward and the same pre-committed
    # rule as everything else. The numbers below are the measured floors, written
    # here before the run rather than tuned on it.
    Experiment(
        name="shrinkage_at_floor",
        hypothesis="Every efficiency rate shrunk to its measured credibility "
                   "floor. If the shipped constants are too small, the model is "
                   "keeping noise it should be discarding, and per-stat MAE should "
                   "fall furthest on the rates that persist least.",
        source="Data/NFL/<season>/player_weeks.parquet, 2016-2025",
        shrinkage={
            "catch_rate": 54.0, "yards_per_target": 103.0,
            "rec_td_per_target": 292.0, "yards_per_carry": 155.0,
            "rush_td_per_carry": 290.0, "yards_per_attempt": 618.0,
            "pass_td_per_attempt": 1054.0, "int_per_attempt": 1394.0,
        },
        note="A floor, not an estimate: it assumes the true rate is perfectly "
             "stable, so drift raises it. Passing here would mean the shipped "
             "constants are too small; failing would mean the floor's assumption "
             "does more damage than the extra shrinkage buys.",
    ),
    Experiment(
        name="shrinkage_touchdowns_at_floor",
        hypothesis="Only the touchdown and interception rates. They are the least "
                   "forecastable quantities measured (+0.189 to +0.276 year over "
                   "year against +0.895 for carries per game), and the blend's one "
                   "per-stat defect is a touchdown stat. If shrinkage is the answer "
                   "it should show here without moving yardage at all.",
        source="Data/NFL/<season>/player_weeks.parquet, 2016-2025",
        shrinkage={
            "rec_td_per_target": 292.0, "rush_td_per_carry": 290.0,
            "pass_td_per_attempt": 1054.0, "int_per_attempt": 1394.0,
        },
        note="Scoped deliberately. `shrinkage_at_floor` moves eight rates at once "
             "and a pooled gain there would not say which did the work -- the same "
             "reason every other experiment here varies one thing.",
    ),
    Experiment(
        name="shrinkage_double",
        hypothesis="Every rate at twice its shipped constant -- a point between the "
                   "shipped value and the floor. Included so the result is a curve "
                   "rather than one point: a gain that holds from 2x to the floor is "
                   "a real bias-variance trade, and one that appears only at the "
                   "floor is the folds being fitted.",
        source="—",
        shrinkage={
            "catch_rate": 80.0, "yards_per_target": 80.0,
            "rec_td_per_target": 240.0, "yards_per_carry": 120.0,
            "rush_td_per_carry": 300.0, "yards_per_attempt": 300.0,
            "pass_td_per_attempt": 600.0, "int_per_attempt": 600.0,
        },
        note="Same reasoning as the ridge sweep: the shape is the evidence, and "
             "picking the best point would be the fishing the rule exists to stop.",
    ),
    Experiment(
        name="everything_that_passed",
        hypothesis="Whatever survives individually, combined. Features that each "
                   "help can still overlap, and the combination is what would "
                   "actually ship.",
        source="—",
        note="Composed at run time from the experiments that passed.",
    ),
)


#: Features measured and rejected before this plan, transcribed from plans 18 and 21.
#:
#: Here so the HTML ledger is the single place to look. A negative result that lives
#: only in a commit message gets rediscovered, and the cost of rediscovering one is
#: a day of somebody's time plus the chance they conclude the opposite from a
#: smaller sample. Each entry names where the full working is.
PRIOR_NEGATIVES: Tuple[Dict[str, str], ...] = (
    {
        "name": "Vacated opportunity share",
        "measured": "Next-season total targets, train 2020-2023 / test 2024-2025: "
                    "0.6578 to 0.6578 R-squared (+0.0000) on all players; +0.0018 "
                    "on changed-teams rows alone.",
        "why": "The feature is real and has spread — Green Bay retained 100% of its "
               "2024 target volume, Pittsburgh lost 51.8%. It predicts nothing, and "
               "not because the depth chart already carries it: corr(vacated, "
               "depth_rank) is −0.009.",
        "where": "docs/plans/21-coaching-and-scheme.md",
    },
    {
        "name": "Coach and coordinator priors",
        "measured": "Rookie-arm mean Spearman: depth chart only 0.6403, + OC prior "
                    "0.6367, + offensive-lead 0.6366, + head coach 0.6353. On the "
                    "veteran arm, at most 0.0012 in either direction and top-N hit "
                    "rate worse at three of four positions.",
        "why": "The whole of the rookie arm's improvement is the depth chart. A "
               "veteran's own prior volume already encodes his situation.",
        "where": "Scripts/usage/season.py: SITUATIONAL_PREFIX, "
                 "VETERAN_SITUATIONAL_REJECTED",
    },
    {
        "name": "Team strength and game script",
        "measured": "+0.064 R-squared on team rush attempts, +0.0015 on next-season "
                    "player carries.",
        "why": "Real after the fact — corr(pass rate, realised margin) −0.494 — but "
               "only −0.117 against the pregame spread, which is all a drafter has. "
               "Parked for the weekly head, where the margin is observed.",
        "where": "docs/plans/21-coaching-and-scheme.md, docs/plans/19-*.md",
    },
    {
        "name": "Finer depth rank (1-6 instead of clipped 1-3)",
        "measured": "R-squared on 2025 targets: clipped 0.4175, fine 0.2537.",
        "why": "The clip was adopted as a schema workaround and turns out to be the "
               "better functional form.",
        "where": "docs/plans/21-coaching-and-scheme.md",
    },
    {
        "name": "Team-then-allocate architecture",
        "measured": "R-squared 0.5488 against the direct model's 0.5633. Oracle "
                    "rows: perfect team volume buys +0.006, perfect player share "
                    "buys +0.42.",
        "why": "The generalisation this repo now plans by — team-level context does "
               "not survive to player level, because role variance dominates it.",
        "where": "docs/plans/18-season-usage-model.md",
    },
    {
        "name": "Partial-game snap correction",
        "measured": "R-squared 0.693 to 0.684 narrow, 0.693 to 0.295 general.",
        "why": "Correcting per-game rates for partial appearances made prediction "
               "worse, substantially so in the general form.",
        "where": "docs/plans/18-season-usage-model.md",
    },
    {
        "name": "USG in the floor/ceiling spread",
        "measured": "Median interval width 8.5% to 24.0%.",
        "why": "A category error rather than a bad number. Disagreement between "
               "forecasters and uncertainty within one forecast are different "
               "quantities.",
        "where": "docs/plans/18-season-usage-model.md",
    },
    {
        "name": "Thin-evidence flags: 'no second prior season', 'rookie arm'",
        "measured": "Median rank error against a 0.096 baseline: 0.089 (−7%) and "
                    "0.083 (−14%).",
        "why": "Both were rejected. The intuitive version of the flag would have "
               "marked the model's strongest arm as its weakest.",
        "where": "Scripts/usage/project.py",
    },
)


def by_name(name: str) -> Experiment:
    """Look an experiment up. Raises KeyError with the known names on a miss."""
    for experiment in EXPERIMENTS:
        if experiment.name == name:
            return experiment
    raise KeyError(f"Unknown experiment {name!r}. "
                   f"Known: {[e.name for e in EXPERIMENTS]}.")


def injury_verdict(metrics: Dict) -> Tuple[str, str]:
    """Apply the injury-model decision rule to a walk-forward's metrics.

    Clause order is deliberate: the false-positive test comes before the accuracy test,
    because a multiplier that improves injured *and* healthy predictions alike has not
    found an injury effect however good its MAE looks, and reporting the MAE first would
    bury that.

    Args:
        metrics: A dict from :mod:`Scripts.injury.backtest`, carrying
            ``post_return_mae_gain_pct``, ``control_mae_change_pct``,
            ``calibration_slope`` and optionally ``hazard_brier_ratio`` and
            ``band_ratio_drift``.

    Returns:
        tuple: ``("merge", reason)`` or ``("reject", reason)``. "merge" here means *the
        multiplier may be applied*; a rejection still leaves the columns worth shipping as
        diagnostics, which is the outcome ``docs/plans/27-injury-model.md`` names in
        advance.
    """
    control = metrics.get("control_mae_change_pct")
    if control is not None and control < -MAX_CONTROL_MAE_INCREASE_PCT:
        return "reject", (
            f"discounting healthy comparables improves their MAE by "
            f"{-control:.2f}%, so the curve is fitting regression to the mean rather "
            f"than injury.")
    if control is not None and control > MAX_CONTROL_MAE_INCREASE_PCT:
        return "reject", (
            f"MAE on matched healthy appearances worsens by {control:.2f}%, more than "
            f"the {MAX_CONTROL_MAE_INCREASE_PCT:.2f}% the rule allows.")

    gain = metrics.get("post_return_mae_gain_pct")
    if gain is None:
        return "reject", "no post-return MAE was measured."
    if gain < MIN_POST_RETURN_MAE_GAIN_PCT:
        return "reject", (
            f"post-return MAE gain {gain:+.2f}% is below the "
            f"{MIN_POST_RETURN_MAE_GAIN_PCT:+.2f}% the rule requires.")

    slope = metrics.get("calibration_slope")
    if slope is None or slope < MIN_CALIBRATION_SLOPE:
        return "reject", (
            f"calibration slope {slope if slope is None else f'{slope:.2f}'} is below "
            f"{MIN_CALIBRATION_SLOPE:.2f}: the effect is real in the mean but the "
            f"per-player magnitude is noise.")

    drift = metrics.get("band_ratio_drift")
    if drift is not None and drift > MAX_BAND_RATIO_DRIFT:
        return "reject", (
            f"a position x ADP band's median TRUE/ESPN ratio moves {drift:.3f}, more "
            f"than the {MAX_BAND_RATIO_DRIFT:.3f} cross-position neutrality allows.")

    return "merge", (
        f"post-return MAE improves {gain:+.2f}% with calibration slope {slope:.2f} and "
        f"no more than {MAX_CONTROL_MAE_INCREASE_PCT:.2f}% cost to healthy comparables.")


def hazard_verdict(metrics: Dict) -> Tuple[str, str]:
    """Whether the fitted weekly hazard may feed channel A.

    Judged apart from the recovery curve because the two can fail independently, and
    because the *pooled per-body-part rate* is a separate and simpler quantity: it has its
    own external check against the published 11.9% hamstring figure and can be quoted on a
    board whether or not the weekly model beats a constant.

    Args:
        metrics: Carries ``hazard_brier_ratio``.

    Returns:
        tuple: ``("merge", reason)`` or ``("reject", reason)``.
    """
    ratio = metrics.get("hazard_brier_ratio")
    if ratio is None:
        return "reject", "no hazard Brier score was measured."
    if ratio > MAX_HAZARD_BRIER_RATIO:
        return "reject", (
            f"weekly Brier is {ratio:.4f} of the constant base rate's, above the "
            f"{MAX_HAZARD_BRIER_RATIO:.4f} the rule requires -- the hazard is not a "
            f"weekly predictor. The pooled per-body-part rate is unaffected and may "
            f"still be quoted.")
    return "merge", f"weekly Brier is {ratio:.4f} of the constant base rate's."


def outcome_verdict(metrics: Dict) -> Tuple[str, str]:
    """Whether the season-points distribution is fit to publish. Plan 28's G-D1.

    Args:
        metrics: A dict from :mod:`Scripts.outcomes.backtest`, carrying ``coverage`` and
            ``calibration_slope``.

    Returns:
        tuple: ``("merge", reason)`` or ``("reject", reason)``. "merge" means the
        distribution may be published as ``pts_p10``/``pts_p50``/``pts_p90``; a rejection
        does not touch the interval fix, which is a separate correction to a column that
        already existed.
    """
    # The draftable population, not the whole pool -- see `MIN_SCORED_PROJECTION` for the
    # 32% of rows that were being counted as covered for producing the zero their
    # interval already contained.
    coverage = metrics.get("coverage_draftable", metrics.get("coverage"))
    if coverage is None:
        return "reject", "no interval coverage was measured."
    low, high = OUTCOME_COVERAGE_RANGE
    if not low <= coverage <= high:
        direction = "too narrow -- it is lying" if coverage < low else "uselessly wide"
        return "reject", (
            f"80% interval coverage is {coverage:.3f} on players projected above "
            f"{MIN_SCORED_PROJECTION:.0f} points, outside [{low:.2f}, {high:.2f}]: "
            f"{direction}.")

    slope = metrics.get("calibration_slope")
    if slope is None:
        return "reject", "no calibration slope was measured."
    low, high = OUTCOME_SLOPE_RANGE
    if not low <= slope <= high:
        return "reject", (
            f"calibration slope {slope:.3f} is outside [{low:.2f}, {high:.2f}]: coverage "
            f"is right on average while the per-player spread is not.")

    return "merge", (
        f"80% coverage {coverage:.3f} on the draftable pool with calibration slope "
        f"{slope:.3f}, both inside the pre-committed windows.")


def joint_verdict(metrics: Dict) -> Tuple[str, str]:
    """Whether the room-level joint draw earns its complexity. Plan 28's G-D2.

    **Clause order is deliberate and matches ``injury_verdict``'s.** The specificity test
    runs before the coverage test, because a model that has simply widened every interval
    improves coverage for backups and entrenched starters alike -- and it would pass a
    coverage-gain bar while having found nothing about vacancy at all.

    Args:
        metrics: Carrying ``backup_coverage_gain_pp`` and ``starter_coverage_gain_pp``,
            both as the improvement in distance to nominal, in percentage points.

    Returns:
        tuple: ``("merge", reason)`` or ``("reject", reason)``. A rejection means phase 1
        ships alone, which is the outcome the plan names in advance.
    """
    backup = metrics.get("backup_coverage_gain_pp")
    starter = metrics.get("starter_coverage_gain_pp")
    if backup is None:
        return "reject", "no backup coverage gain was measured."

    # The clause only has something to catch when the control group actually moved. Where
    # entrenched starters gain nothing, there is no false positive to find and this must
    # fall through to the magnitude bar -- otherwise a perfectly vacancy-specific effect
    # gets rejected with the words "it has found variance in general", which is the
    # opposite of what was measured. The first run of this gate did exactly that: backups
    # +2.1pp, starters +0.0pp, rejected for non-specificity.
    if (starter is not None and starter > 0
            and (backup - starter) < MIN_VACANCY_SPECIFICITY_PP):
        return "reject", (
            f"the joint draw improves backups by {backup:+.1f}pp and entrenched starters "
            f"by {starter:+.1f}pp -- a {backup - starter:+.1f}pp difference against the "
            f"{MIN_VACANCY_SPECIFICITY_PP:.1f}pp the rule requires. It has found variance "
            f"in general rather than vacancy in particular, and the redistribution rule "
            f"is decoration.")

    if backup < MIN_JOINT_COVERAGE_GAIN_PP:
        specific = ("" if not starter else
                    f" The gain is entirely vacancy-specific -- entrenched starters move "
                    f"{starter:+.1f}pp -- so the mechanism is real and the magnitude is "
                    f"not.")
        if starter is not None and starter <= 0:
            specific = (f" Entrenched starters move {starter:+.1f}pp, so the effect is "
                        f"exactly as specific as the mechanism claims; it is the size "
                        f"that fails, not the direction.")
        return "reject", (
            f"the joint draw is {backup:+.1f}pp closer to nominal for depth-rank >= 2 RBs "
            f"and TEs, below the {MIN_JOINT_COVERAGE_GAIN_PP:.1f}pp the rule requires, so "
            f"the room-level machinery does not earn its complexity and phase 1 ships "
            f"alone.{specific}")

    return "merge", (
        f"the joint draw is {backup:+.1f}pp closer to nominal for backups against "
        f"{starter:+.1f}pp for entrenched starters, so the gain is vacancy-specific.")


def relevance_verdict(metrics: Dict) -> Tuple[str, str]:
    """Whether the distribution may change what the board is sorted by. Plan 28's G-D3.

    Args:
        metrics: Carrying ``moved_share`` -- the fraction of draftable players whose
            ordering by ``p_top12`` differs from their ordering by mean points by at least
            :data:`MIN_PICK_MOVE` picks.

    Returns:
        tuple: ``("merge", reason)`` or ``("reject", reason)``. "merge" means the sort may
        change; "reject" means ship the columns as diagnostics and leave the sort alone,
        which is not a failure -- it is one of the two outcomes the gate was written to
        distinguish.
    """
    moved = metrics.get("moved_share")
    if moved is None:
        return "reject", "no ordering comparison was measured."
    if moved < MIN_MOVED_SHARE:
        return "reject", (
            f"only {moved:.1%} of draftable players move by {MIN_PICK_MOVE}+ picks under "
            f"p_top12, below the {MIN_MOVED_SHARE:.0%} the rule requires -- the two "
            f"orderings agree, so ship the columns as diagnostics and do not touch the "
            f"board's sort.")
    return "merge", (
        f"{moved:.1%} of draftable players move by {MIN_PICK_MOVE}+ picks under p_top12, "
        f"above the {MIN_MOVED_SHARE:.0%} the rule requires.")


def verdict(baseline: Dict, candidate: Dict) -> Tuple[str, str]:
    """Apply the decision rule to a candidate's metrics.

    Args:
        baseline: The baseline run's metric dict, from :mod:`Scripts.lab.run`.
        candidate: The candidate's, same shape.

    Returns:
        tuple: ``(verdict, reason)`` where verdict is ``"merge"`` or ``"reject"``
        and reason is one sentence naming which clause decided it.
    """
    positions = [p for p in ft.MODELLED_POSITIONS
                 if p in baseline["spearman"] and p in candidate["spearman"]]
    if not positions:
        return "reject", "no position produced a comparable Spearman."

    deltas = {p: candidate["spearman"][p] - baseline["spearman"][p]
              for p in positions}
    mean_gain = sum(deltas.values()) / len(deltas)

    worst_position = min(deltas, key=deltas.get)
    worst = deltas[worst_position]

    regressions = [
        p for p in positions
        if p in baseline["top_n"] and p in candidate["top_n"]
        and candidate["top_n"][p] < baseline["top_n"][p]
    ]

    if mean_gain < MIN_MEAN_SPEARMAN_GAIN:
        return "reject", (
            f"mean within-position Spearman gain {mean_gain:+.4f} is below the "
            f"{MIN_MEAN_SPEARMAN_GAIN:+.4f} the rule requires.")
    if worst < -MAX_POSITION_SPEARMAN_LOSS:
        return "reject", (
            f"mean gain {mean_gain:+.4f} clears the bar but {worst_position} loses "
            f"{worst:+.4f}, more than the {-MAX_POSITION_SPEARMAN_LOSS:+.4f} a "
            f"single position may give up.")
    if len(regressions) > MAX_TOP_N_REGRESSIONS:
        return "reject", (
            f"top-N hit rate falls at {len(regressions)} positions "
            f"({', '.join(regressions)}), more than the {MAX_TOP_N_REGRESSIONS} "
            f"the rule tolerates.")

    changes = {
        stat: 100 * (candidate["mae"][stat]["usg"] - entry["usg"]) / entry["usg"]
        for stat, entry in baseline["mae"].items()
        if stat in candidate["mae"] and entry.get("usg")
    }
    if changes:
        mean_change = sum(changes.values()) / len(changes)
        worst_stat = max(changes, key=changes.get)
        if mean_change > MAX_MEAN_MAE_INCREASE_PCT:
            return "reject", (
                f"ordering improves {mean_gain:+.4f} but mean per-stat MAE worsens "
                f"{mean_change:+.2f}%, past the {MAX_MEAN_MAE_INCREASE_PCT}% the "
                f"rule allows — this source feeds the blend as a stat line, not as "
                f"a rank.")
        if changes[worst_stat] > MAX_STAT_MAE_INCREASE_PCT:
            return "reject", (
                f"ordering improves {mean_gain:+.4f} but {worst_stat} MAE worsens "
                f"{changes[worst_stat]:+.2f}%, past the "
                f"{MAX_STAT_MAE_INCREASE_PCT}% any single stat may give up.")

    mae_note = (f", mean per-stat MAE {sum(changes.values()) / len(changes):+.2f}%"
                if changes else "")
    return "merge", (
        f"mean within-position Spearman {mean_gain:+.4f}, worst position "
        f"{worst_position} {worst:+.4f}, top-N regressions "
        f"{len(regressions)}{mae_note}.")
