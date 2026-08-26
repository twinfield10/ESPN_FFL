# 22 — Feature research for the season head

**Status:** COMPLETE

**Where it stands:** measured. Nothing merged. The lab that produced it is shipped, and so is
the data layer under it.

## Problem

Plan 18 built the season head and plan 21 measured five situational features out of
it. The question this plan asks is the obvious next one: *is there anything else to
feed it?*

Plan 21 ended on a generalisation that had to be tested rather than trusted:

> Team-level context does not survive to player level, because role variance
> dominates it. The only feature that has ever moved this model is the one that
> resolves *role*: the depth chart, worth +0.048 R² on veteran carries.

If that is right, the search should be for more ways to measure role directly. So
this plan pulled four new data sources, built the features they support, and ran
each through the same walk-forward that judged everything before it.

## What was pulled

`R/GetAdvanced.R`, new, patterned on `R/GetUsage.R`. All coverage figures checked
live against nflreadr 1.4.1 on 2026-08-08.

| Source | Seasons | What it gives | Coverage |
|---|---|---|---|
| `load_participation` × `load_pbp` | 2016–2025 | **routes run** — there is no routes column in public data; `offense_players` on dropback plays is the standard derivation | 98–100% of players with a prior season |
| `load_nextgen_stats` | 2016–2025 | aDOT, separation, cushion, YAC over expected, air-yards share; RYOE, stacked-box rate, time to line | **35–75%** |
| play-by-play by field position | 2016–2025 | carries and targets inside the 20/10/5, end-zone targets, with team totals | 79–96% |
| `load_contracts` | signings 1992–2026 | `apy_cap_pct`, guarantees, signing year | 98.1% of rostered players |

Sanity checks on the derivation: median route share came out WR 0.578 / TE 0.320 /
RB 0.292, which is right. Every fitted efficiency coefficient carries the
mechanically correct sign — aDOT lowers catch rate, separation raises it, RYOE
raises yards per carry, a stacked box lowers it.

Two sources were considered and not pulled. `load_pfr_advstats` starts in 2018, two
seasons short of the training window, and largely duplicates NGS.
`load_ftn_charting` starts in 2022. Both are coverage decisions, not judgements
about the data.

## What was tried

Six experiments, each varying **one** thing against the baseline, each run over the
full 2019–2025 walk-forward. The decision rule was written into
`Scripts/lab/registry.py` before any of them ran: mean within-position Spearman gain
≥ +0.005, no position worse by more than 0.005, top-N hit rate down at no more than
one position.

| Experiment | Mean Spearman Δ | Verdict |
|---|---|---|
| `ridge_alpha_*` (functional form, 6 settings) | up to +0.0127 | reject — see the sweep below |
| `contracts_x_moved` | +0.0007 | reject |
| `efficiency_no_ngs_gate` | +0.0004 | reject |
| `efficiency_fitted_baseline` | +0.0003 | reject |
| `routes_volume` | +0.0000 | reject |
| `red_zone_role_volume` | −0.0005 | reject |

For scale, the depth chart was worth +0.05 to +0.07 Spearman across positions. The
best feature here is an order of magnitude short of the threshold, which is itself
an order of magnitude below the depth chart.

The full ledger, with per-position and per-stat detail, is `docs/model_lab.html`,
generated from `Scripts/lab/results.json`.

## Why they failed, which is the useful part

Three distinct mechanisms, and none of them is "the data was bad".

### 1. Collinear with what the model already has

Routes correlate **0.88 to 0.91** with targets. A player's route count and his
target count are two measurements of the same underlying role, and `p1_targets_pg`
is already the highest-weighted regressor in the volume head. Red-zone share fails
the same way against `depth_rank` and prior volume together.

This was measured before the plan was written, on a simpler model, and it predicted
the outcome: +0.004 to +0.014 holdout R² and **no movement in ordering at all**.

### 2. The credibility architecture damps the efficiency prior exactly where it would matter

The efficiency head had never had a feature — it shrinks every player toward his
position's pooled rate. Replacing that constant with a *fitted* one (route depth,
separation, goal-line role) is the natural improvement, and it does move the rates:
2.8% at the median for yards per target, 15.3% for rushing touchdown rate, on the
rows it reaches.

It reaches the wrong rows. Shrinkage weight on the prior is `k / (n + k)`, so it
falls as opportunity rises — and NGS's qualifying threshold **is** an opportunity
threshold:

| prior-season targets | rows | got a fitted prior | weight the prior carries |
|---|---|---|---|
| 0–10 | 13,367 | **0.4%** | 88.9% |
| 10–30 | 1,151 | 34.4% | 66.7% |
| 30–60 | 864 | 65.5% | 47.1% |
| 60–100 | 608 | 83.7% | 33.3% |
| 100+ | 362 | **95.6%** | **6.8%** |

The fitted prior reaches 95.6% of the players who give it 6.8% weight and 0.4% of
those who give it 88.9%. The two are anti-correlated by construction.

That diagnosis suggested a fix, and the fix was tried. The touchdown-rate baselines
are built on play-by-play field position, which has no qualifying threshold; only a
stray `ngs_adot` term — coefficients +0.0016 and +0.0010, near nothing — was gating
them. Dropping it took `rec_td_per_target` coverage in the 10–30 target band from
34% to **92%**, and improved receiving-touchdown MAE from −0.09% to −0.42%.

Ordering still did not move (+0.0003 to +0.0004). So the ceiling is not coverage
either: low-volume players sit at the bottom of their position regardless, and
making their efficiency prior better does not reorder anything a drafter reads.

### 3. The estimator can trade accuracy for ordering, which is not the same as being better

This was predicted to be a null result and it was not — the one prediction in this
plan that was wrong, and the most interesting thing in it.

Regularising the volume and games heads improves within-position ordering
**monotonically**, at every position, out to α = 300. It also makes every per-stat
error monotonically worse.

| α | mean Spearman Δ | mean MAE Δ | thin-prior-season Δ |
|---|---|---|---|
| 1 | +0.0005 | +0.04% | +0.0012 |
| 3 | +0.0012 | +0.11% | +0.0026 |
| 10 | +0.0034 | +0.30% | +0.0081 |
| 30 | +0.0072 | +0.87% | +0.0198 |
| 100 | +0.0119 | +3.13% | +0.0374 |
| 300 | +0.0127 | +9.71% | +0.0583 |

+0.0119 at α = 100 is more than double the decision rule's threshold and larger
than every feature in this plan put together.

The charitable reading is that shrinkage helps most where data is thinnest, which
the last column supports: at α = 100 the thin-prior-season slice gains +0.0374
against the settled slice's +0.0051.

The decisive reading is that **there is no interior optimum.** A real bias-variance
sweet spot has a peak — error falls, bottoms out, rises. This curve only rises, and
the accuracy cost rises with it without bound. That is not a better-fitted model; it
is progressive reversion toward the positional mean, which orders players slightly
better because the mean is a decent ranker and prices them worse because it is a
terrible estimate. Prediction spread at quarterback falls from 91.1 to 82.3 against
a realised 118.6 — the model was already under-dispersed and this makes it more so.

Plan 18 already named the failure: *a model that quietly emits a positional average
looks like full coverage and drags the blend toward the mean for exactly the players
a board must differentiate.* `USG_` is a stat line that gets averaged with ESPN's
and FantasyPros' before anything is priced, so an 8% worse yardage number is a cost
paid by every player in the blend in exchange for an ordering gain inside one of
three sources.

**This also found a bug in the decision rule.** α = 30 and α = 100 both passed it as
first implemented — because `verdict()` checked ordering three ways and never looked
at accuracy, while the plan had specified an accuracy clause. The clause is now in
`registry.py` as `MAX_MEAN_MAE_INCREASE_PCT` and `MAX_STAT_MAE_INCREASE_PCT`. Worth
recording that the gap was found by a result that looked like good news.

`RIDGE_ALPHA` ships at 0.0. It is left in place because the curve is the evidence
for a claim worth being able to re-check.

## The near-miss worth recording

`contracts_x_moved` was the one hypothesis about a *subpopulation*, and the
subpopulation metric was added to the lab because of it — testing a
changed-teams hypothesis on the pooled population answers a different question.

On changed-teams rows it is **+0.0027**, positive, in the predicted direction, on
the exact population plan 18 flagged as carrying +32% median rank error. It is also
five times too small to merge, and it costs tight ends −0.0012 on the full
population.

The earlier probe suggested much more (+0.026 to +0.100 R² on movers at four
positions). The difference is `depth_rank`: the probe omitted it, and the depth
chart already says which movers got a job. This is the same pattern plan 21 found
when it separated the coach prior from the depth chart, and it is why the lab runs
every candidate against the full regressor set.

## What this adds to plan 21's finding

Plan 21: team-level context does not survive to player level.

Plan 22 extends it: **player-level context that is a function of past usage does not
survive either, because past usage is already the model's strongest regressor.**
Routes, red-zone share, aDOT and separation are all things a player did last season.
So is `p1_targets_pg`, and it got there first.

What actually moved this model was the depth chart, which is none of those things.
It is **current-season, player-level, and not derivable from past production**. That
is now a fairly sharp specification of where any remaining edge has to come from,
and it says what to try next:

* **NGS passing** — a straightforward gap rather than a decision. Completion
  percentage over expected is the obvious regressor for the quarterback rates,
  which are the only rates with no fitted baseline at all. `R/GetAdvanced.R` does
  not pull it. Untested.
* **Pre-season depth-chart movement.** The 2026 depth-chart feed is a timestamped
  snapshot log — 406k rows. A player's *trajectory* through camp is current-season,
  player-level, and not a function of last year. The strongest untested candidate.
* **The weekly head (plan 19).** In-season, information arrives every week that no
  pre-season model can have. This plan is more evidence that the pre-season head is
  close to what its inputs support — and the transfer section below shows that two
  of the features rejected here are worth real money weekly.

Explicitly not next: anything else derived from prior-season production.

## Does any of this transfer to the weekly head? Two of them invert

Everything above was measured on the **season** head. Two of the three failure
mechanisms were arguments about *sample size*, and the in-season horizon changes the
sample size by an order of magnitude. Measured, not assumed —
`python -m Scripts.lab.weekly`.

**Routes, rejected seasonally at +0.0000 mean Spearman, are the largest weekly
effect in this plan.** Trailing route share and route count on top of trailing
targets, predicting next-week targets, train ≤2024 / test 2025:

| pos | n | trailing targets only | + routes | delta | median t3 targets | median t3 routes |
|---|---|---|---|---|---|---|
| WR | 1,789 | 0.3995 | 0.4108 | **+0.0113** | 12 | 74 |
| TE | 976 | 0.3701 | 0.4247 | **+0.0546** | 9 | 56 |
| RB | 1,083 | 0.2846 | 0.3118 | **+0.0271** | 6 | 44 |

The mechanism is the last two columns. Over a trailing three-appearance window the
median receiver has 12 targets and 74 routes — routes carry roughly six times the
sample per unit time and stabilise about two and a half times faster. Over a full
season both are large (250+ targets, 900+ routes) and the extra precision buys
nothing, which is exactly what the season experiment found.

Plan 19 independently measured usage shares at +0.0103 WR and +0.0114 TE and said to
carry them for those two positions. The WR figure lands in the same place; tight end
is far better, because route share is a sharper instrument than the target share
plan 19 had available. **Plan 19 should carry routes, and `routes.parquet` now
exists for it.**

**The efficiency prior's anti-correlation breaks.** The season finding was that a
fitted prior reaches the players who give it almost no weight. Weekly the
denominator is a three-game window, so at the 95th percentile of volume:

| rate | k | season n | prior weight | weekly t3 n | prior weight |
|---|---|---|---|---|---|
| yards_per_target | 40 | 113 | 26.1% | 27 | **59.7%** |
| catch_rate | 40 | 113 | 26.1% | 27 | **59.7%** |
| rec_td_per_target | 120 | 113 | 51.5% | 27 | **81.6%** |
| yards_per_carry | 60 | 204 | 22.7% | 51 | **54.1%** |
| rush_td_per_carry | 150 | 204 | 42.4% | 51 | **74.6%** |

The prior more than doubles its weight and crosses 50% — it becomes the dominant
term rather than a correction. And NGS coverage is best for exactly these
high-volume players, so weekly the fitted prior would reach the rows where it
decides the number. `fit_rate_baselines` is built and tested; plan 19 should use it.

**What should still hold weekly.** Contracts, as a pre-season signal dominated once
three weeks of usage are observable — with the caveat that weeks 1–2 have no
trailing window at all. The ridge critique, because reversion to the mean is an
argument about shrinkage rather than horizon, and weekly there are 42,796
player-weeks against ~6,600 player-seasons so estimator variance binds even less.
Red-zone role is genuinely unknown and worth testing rather than reasoning about: a
three-game window holds only about ten red-zone touches, so the sample argument that
rescues routes cuts the other way.

## Blend weights: not identified by the data

The other lever this plan was asked to pull. `WEIGHTS` is ESPN, FantasyPros and the
usage head at a third each, set by hand; fitting them against realised outcomes is
the obvious improvement.

Fitted per stat on the 2025 evaluation set by non-negative least squares, then
split-half tested — odd weeks against even weeks, same season, same quantity:

| stat | n odd / even | odd weeks | even weeks | out-of-sample MAE |
|---|---|---|---|---|
| receivingReceptions | 231 / 160 | PINNY 0.63, BOL 0.16, ESPN 0.11, FP 0.10 | FP 1.00 | +0.2% |
| receivingYards | 236 / 165 | BOL 0.56, PINNY 0.29, FP 0.15 | FP 0.78, BOL 0.22 | −2.1% |
| rushingYards | 155 / 110 | FP 0.73, PINNY 0.27 | FP 1.00 | −1.2% |

The halves do not agree — receiving receptions goes from `PINNY 0.63` to
`PINNY 0.00, FP 1.00`. Out of sample the fitted weights beat the shipped ones on one
stat and lose on another. Three reasons, none fixable by fitting harder:

* **The sample collapses.** Requiring every source to be real rather than imputed
  takes 5,257 player-weeks to 110–236 per half, and the survivors are the
  heavily-covered stars rather than the population the weights are applied to.
* **The sources are collinear.** G0 measured FantasyPros' residuals at **+0.988**
  against ESPN's. Nothing can say how to split weight between two near-copies; NNLS
  responds by giving one of them everything, and which one depends on the half.
* **The season question is not this question.** These are weekly rows, and the open
  question is the *season* blend's usage weight. There is no historical season blend
  to fit against — plan 18 records that as permanent.

`python -m Scripts.lab.blend` reproduces it. The weights stay where plan 03 will
consider them with better evidence.

## Rejected, and why not to retry it

`ESPN ADP and auction values` are already on disk from `fetch_draft_market`, and are
a genuinely strong current-season role signal — they would probably clear the bar.
They are rejected on **independence**, not accuracy. Usage residuals already
correlate +0.832 with ESPN's, and G0 exists to keep the fifth source independent
enough to be worth a third of the blend. A `USG_` that reads ADP is a slower way of
computing `ESPN_`.

## What shipped

Nothing changed the model. `VOLUME_REGRESSORS`, `GAMES_REGRESSORS` and the
efficiency shrinkage are as plan 18 left them, and `RIDGE_ALPHA` is 0.0, which is
ordinary least squares.

What did ship is the apparatus, because the next person to ask this question should
not re-pull four data sources to answer it:

* `R/GetAdvanced.R` and the loaders in `Scripts/usage/nflverse.py`
* `Scripts/usage/features.py` — `advanced_totals`, `fit_rate_baselines`,
  `contract_context`, and `attach_efficiency`'s optional fitted prior. All off by
  default.
* `Scripts/lab/` — the experiment registry, the runner, and the HTML report
* `Scripts/usage/season.py` — `RIDGE_ALPHA`, `_ridge`, and the plan 22 candidate
  terms in `_veteran_terms`, all inert at their defaults
* `feature_kwargs` pass-through on `training_frame` and `run_season`

## Two bugs found on the way

**`age` was leaking past the leakage guard's allowlist.** Not leaking data — it is a
legitimate current-season fact — but it was legitimate by luck: the test fixtures
build rosters without a `birth_date`, so `roster_context` never created the column
and the guard never had to judge it. On real data it reported `age` as leakage, a
false positive that would have been read as a real one. Now declared, with
`test_leakage_guard_covers_age` to keep it declared.

**NaN is not null, for the third time in this repo.** NGS ships NaN for a
player-week it could not measure. Polars' `is_not_null()` is True for a NaN, so the
guard on the fitted prior let one through; the first run died on `SVD did not
converge` and the second returned NaN for every receiving MAE — which looked like a
result rather than a bug. Fixed at the source in `advanced_totals`, so no consumer
has to remember, and pinned by `test_advanced_totals_converts_nan_to_null`.

## Effort

Four data sources pulled and validated, eleven experiments, a blend-weight
identification test, 13 new tests and one lab. The walk-forward is about 90 seconds
per experiment; the cold data backfill is a few minutes and is cached after that.

## Reproducing it

```bash
Rscript R/GetAdvanced.R 2016 2025          # cold: a few minutes, then cached
python -m Scripts.lab.run --all            # ~20 min for eleven experiments
python -m Scripts.lab.run --reverdict      # re-apply the rule without re-running
python -m Scripts.lab.blend                # the blend-weight identification test
python -m Scripts.lab.report               # regenerate docs/model_lab.html
```
