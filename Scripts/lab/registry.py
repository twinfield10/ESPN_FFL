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
