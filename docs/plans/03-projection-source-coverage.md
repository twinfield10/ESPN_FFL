# 03 — Blend weights assume coverage the sources don't have

**Status:** COMPLETE

**Priority:** High · **Effort:** Medium · **Where it stands:** Steps 1, 2, 4 built ·
**step 3 measured 2026-08-27 and the answer is no** · **step 3b measured 2026-09-01:
the season weight is right, and the gate had been marking the wrong row as shipping**
· step 5 moved to
[36](36-sportsbook-scrapes.md), where it turned out to be two scrapers rather than one

> **The problem was bigger than this plan estimated.** Measured provenance shows
> FantasyPros was only **25% real** even in 2025, Pinnacle **8%**, BetOnline
> **12%** — so the nominal four-source blend averaged roughly **90% ESPN**.
> Renormalisation is implemented and proven confined to imputed cells. See
> [Measured coverage](#measured-coverage-2026-08-03).

> **Step 3's answer is no, and the weights do not move.** This plan asked for the
> hand-set weights to be replaced by weights fitted against 2025 actuals. Built and
> measured, that fit **fails all four clauses of its own pre-registered rule in all
> six population × split cells** — and the two candidates that looked like findings
> turned out to be, respectively, a source that casts no weekly vote and an artifact
> of the estimator's own fallback. The equal-vote rule stays, now measured rather
> than asserted. See [Step 3: the weight re-tune, measured](#step-3-the-weight-re-tune-measured-2026-08-27).

> **The season weight is now measured too, and it does not move either — but the gate
> that said otherwise was misreading its own curve.** `g1_season` hard-coded 0.25 as
> TOMCAT's shipped weight and swept a range topping out at 0.5. Production is **1.0**:
> `WEIGHTS['default']` gives TOMCAT the same 0.25 as every external source, so the
> ratio the gate sweeps is 1.0, not 0.25. The curve therefore ran off its own edge and
> read as "TOMCAT is under-weighted" — a conclusion derived and acted on twice.
> Bracketed properly the minimum is **interior and sits on production**, and the
> long-standing "the baseline is missing ESPN, so this flatters TOMCAT" caveat is
> measured and false: a synthetic ESPN at ESPN's real 0.985 residual correlation with
> FantasyPros moves the baseline 34.83 → 34.85. See
> [Step 3b](#step-3b-the-season-weight-and-the-mislabel-that-hid-it-2026-09-01).

## Problem

The blend treats four sources as peers, weighting each per stat (Pinnacle and
BetOnline get 25% each on most stats, 40% each on passing TDs). But the sources
cover very different numbers of players, and gaps are filled by
`impute_columns(target_prefix='PINNY_', source_prefix='MEAN_')` — where `MEAN_`
is the average of ESPN and FantasyPros.

So when Pinnacle has no line for a player, `PINNY_*` silently becomes the ESPN/FP
mean, and the blend then weights that as though it were an independent
sportsbook opinion. **ESPN and FantasyPros get counted twice.** The apparent
four-source consensus is, for a large share of players, a two-source blend with
extra confidence.

This is invisible downstream: `PINNY_Points` looks populated and reasonable.

## Evidence

Season 2025, week 17:

| Source | Distinct players, wk 17 | All weeks | Stat columns |
|---|---|---|---|
| FantasyPros | 575 | 706 | offense |
| BetOnline | 598 | 1051 | offense + **defensive/IDP** |
| **Pinnacle** | **213** | 385 | offense only (11 stats) |

Against 338 rostered + FA players in Knights_FFL that week, `PINNY_Points` was
non-zero for 265 — but Pinnacle only had lines for 213 players league-wide, and
not all of those are rostered. A large fraction of that 265 is imputed.

Pinnacle also has **no defensive stats at all**, so for IDP leagues its 25%
weight is entirely ESPN/FP passthrough.

For the record, an earlier note in `STATE_OF_THE_REPO.md` suggested Pinnacle
might not be contributing at all because `clean_pinny()` is ~27% commented out.
That was too strong — measured, Pinnacle *is* contributing real lines for the
players it covers. The problem is coverage and imputation, not absence.

## Fix

**1. Track provenance.** Add a per-source `*_is_imputed` boolean during
`impute_columns`, so it's possible to tell a real line from a filled one. Cheap,
and everything below depends on it.

**2. Re-weight per row, not globally.** Renormalise weights over the sources
that actually have data for that player-stat, instead of letting an imputed
value absorb a full 25%. Roughly:

```python
# in compute_weighted_stats: skip imputed contributions, renormalise the rest
active = {src: w for src, w in weights.items() if not row[f"{src}_{stat}_is_imputed"]}
total = sum(active.values()) or 1.0
value = sum(row[f"{src}_{stat}"] * w / total for src, w in active.items())
```

This is the substantive change: a player with a real Pinnacle line gets a
genuine four-source blend; a player without one gets an honest ESPN/FP blend
rather than a fake consensus.

**3. Re-tune the weights against 2025 actuals.** They are currently hand-set. The
notebook already contains two unproductionised learned-weight models (cells 11
and 14 — OLS per stat, and a `LinearRegression` combo). With a full season of
actuals plus provenance flags, fit them properly and replace the guesses. Do
this *after* the BetOnline decision in plan 02, since the source set may change.

> **This prescription was wrong, and measuring it is how that was established.**
> "Fit them properly and replace the guesses" assumes the weights are identified.
> They are not, and the notebook models this step points at (cells 11 and 14, OLS
> per stat and a `LinearRegression` combo) are both the *wrong estimator* — they fit
> a plain linear combination, which is not what the pipeline publishes. See
> [Step 3: the weight re-tune, measured](#step-3-the-weight-re-tune-measured-2026-08-27).

**4. Report coverage each run.** `get_match_details()` already prints unmatched
players. Extend it to print per-source coverage as a percentage, so a source
quietly degrading is visible rather than absorbed by imputation.

**5. ~~Fix `scrape_pinnacle.py`'s import-time scrape.~~ Moved to
[36](36-sportsbook-scrapes.md) on 2026-08-27.** Found 2026-08-03 while verifying the
season rollover: the module's tail is bare top-level statements — `driver.get()`,
`WebDriverWait(...).until()`, `reconcile_props()` — with no `if __name__ ==
"__main__":` guard, so merely *importing* it launches Chrome and scrapes Pinnacle.

It moved because it is not a coverage problem and it is not one file. Scoping it
found that **`scrape_BOL.py` has the same defect with a worse blast radius** — its
import-time statements *write* the archived parquet and CSV — and that the
consequence is shared: neither book is in the nightly, so both were **thirteen days
stale** on a board eleven days from a draft. That is a plan about how this repo talks
to sportsbooks, which is what 36 is. The odds work the owner actually wants next —
game lines, totals, team totals, 4Casters, Pinnacle over its API instead of
Selenium — lives there too.

Two details this plan had slightly wrong, corrected in 36: the lines are 645–666
rather than 538–559 (the file moved under the citation), and the guard is missing from
two scrapers rather than one. Also still true and recorded there: `SEASON =
current_season()` is bound at import, before any caller can override it, which makes
the module unusable for backfilling another season.

## Measured coverage (2026-08-03)

`coverage_report()` on Knights_FFL 2025, share of cells that are **real** rather
than imputed, with the nominal weight beside it:

```
  stat                            ESPN          FP       PINNY         BOL
  ------------------------------------------------------------------------
  passingYards              100.0% w0.1   25.2% w0.7    8.7% w0.1    9.3% w0.1
  passingTouchdowns         100.0% w0.1   25.2% w0.1    8.7% w0.4    9.3% w0.4
  rushingYards              100.0% w0.2   25.2% w0.3   25.4% w0.25   28.4% w0.25
  receivingYards            100.0% w0.2   25.2% w0.3   43.8% w0.25   50.2% w0.25
  receivingReceptions       100.0% w0.2   25.2% w0.3   42.7% w0.25   48.9% w0.25
  ------------------------------------------------------------------------
  ALL STATS (mean)              100.0%        9.6%        4.1%        8.1%
```

Read the worst line: `passingTouchdowns` gave Pinnacle and BetOnline 0.4 each — 80%
of the weight — while both were real under 10% of the time. Since they impute from
`MEAN_` = avg(ESPN, FP), and FP is itself ESPN for 75% of cells, that 80% was
recycled ESPN. **FantasyPros at 25% is not a 2026 regression** — it was already
mostly imputed in 2025, which this plan did not catch by counting distinct players
in the source file rather than matched cells in the blend.

### 2026 pre-season source state

| Source | Weekly | Season-long | Coverage | IDP |
|---|---|---|---|---|
| ESPN | works | works | all rosters + FA | yes |
| FantasyPros | 10/position | 10/position | 60 players | no |
| BetOnline | 403, dead | **works (R only)** | 546 props / 273 players | **yes** |
| Pinnacle | game lines only | **works (guest API)** | 76 props / 76 players | no |

FantasyPros is now gated: the page is 289 KB but `table#data` holds exactly 10
rows, there are 10 `.player-name` links, and the HTML advertises
`upgrade` / `premium` / `sign in`. Same cap on `week=draft`, so it is not
pre-season sparsity. Kept in the blend — renormalisation means it contributes for
the 60 players it genuinely covers and nothing elsewhere.

## What landed

**Steps 1, 2 and 4 are done.** `impute_columns` writes
`<SOURCE>_<stat>_is_imputed` flags that accumulate across calls;
`compute_weighted_stats(renormalise=True)` drops imputed sources and renormalises
the remainder, with `renormalise=False` retained to A/B against history;
`print_coverage_report` runs inside `clean_lineups`. Weights moved to a module-level
`WEIGHTS` constant so they can be inspected without running the pipeline.

A third bug surfaced doing it: a source whose **column was absent entirely** was
skipped from the sum but not the divisor, so a 10-yard projection came out as 5
purely because Pinnacle was not in the frame. A pre-existing test asserted that
behaviour; it now asserts the corrected one.

**Verified with the new equivalence harness** (`Scripts/equivalence.py` — five
plans referenced one that did not exist). Across Knights_FFL, GOP_Degenerates and
Winfield_Football for 2025: only `TRUE_*` columns changed, and for every
(row, stat) where all weighted sources were real the drift was **0**, bar
`passingYards` at 5.7e-14 of float re-association noise. `TRUE_Points` moved for
~73% of rows, max 11.3 points.

**Step 3 (re-tune the weights) was measured on 2026-08-27 and rejected.** It had been
deferred twice — once for the plan 02 source-mix decision, once for plan 34's
condition that the stat lines be right first. Both conditions were met, so it was
built. Full account below.

**Step 5 (the import-time scrape) is not open here any more — it is
[36](36-sportsbook-scrapes.md) step 1.** `scrape_pinnacle_season.py` is still the
pattern to follow (everything behind `main()`, guarded by `__main__`, with a test
asserting no bare module-level calls), and 36 generalises that test across every
`Scripts/scrape_*.py` rather than fixing the two known offenders — which is what
scoping it found: `scrape_BOL.py` is the second one, and it writes files on import.

## Step 3: the weight re-tune, measured (2026-08-27)

**Verdict: the weights do not move.** `python -m Scripts.lab.blend` reproduces every
number here.

This was the second attempt. The first concluded "the data does not identify them"
on three grounds, and the useful part of this run is that **the first attempt's main
objection is now obsolete and the verdict survives anyway**. That objection was
sample collapse: requiring every source to be real left 265–401 rows. A free
FantasyPros account has since taken that source from 13% real to **90%** on the key
stats, and fitting the estimator that actually ships — which renormalises over
whatever is real per row, rather than requiring the intersection — uses the **78–93%**
of rows carrying at least two real sources instead of the 1.9–29.7% carrying five.
Four times the data, same answer.

### The decision rule, pre-registered

Four clauses. The first two reuse the lab's existing thresholds
(`Scripts/lab/registry.py`) rather than choosing new ones; the third and fourth are
this measurement's own.

| Clause | Bar | Protects against |
|---|---|---|
| `mean_gain` | mean per-stat OOS MAE must beat the rule by **0.5%** | paying complexity for noise |
| `worst_stat` | no stat worse than **+2.0%** | a good mean hiding a stat, on a board read one stat at a time |
| `stability` | folds must agree within **0.10** on the simplex | a fit describing the half it was fitted on |
| `not_degenerate` | **0** rows may lose their renormalised denominator | the fit winning by switching the blend off |

Scored two ways that fail differently — `odd/even` (both halves see the whole season)
and `early/late` (time-ordered, the honest shape for a forecast) — over three
populations, fitting each half and scoring the other, symmetrically.

### Result: fails everywhere, and not marginally

All six population × split cells **FAIL**:

| | mean gain (bar −0.5%) | worst stat (bar 2.0%) | stability (bar 0.10) | degenerate rows (bar 0) |
|---|---|---|---|---|
| all, odd/even | −0.43% | +0.51% | **1.00** | **1,042** |
| all, early/late | −0.09% | **+2.17%** | **0.74** | **1,110** |
| team_played, odd/even | −0.43% | +0.51% | **1.00** | **912** |
| team_played, early/late | −0.09% | **+2.17%** | **0.74** | **995** |
| played, odd/even | −0.45% | +0.44% | **0.88** | **105** |
| played, early/late | −0.24% | +1.45% | **0.92** | **96** |

The stability clause misses by **7–10×**. On a simplex whose equal-vote point is
0.25, two halves of one season disagree by up to the entire simplex. The cause is
collinearity rather than sample size — plan 16's G0 measured FantasyPros' residuals
at **+0.988** against ESPN's — and no amount of data splits weight between two
near-copies.

### The two candidates that looked like findings

**1. `USG`'s equal vote costs the weekly blend +1.96% to +30.87%, and cannot be acted
on.** It is by far the largest effect measured — quoted the way the tool prints it,
as the cost of *adding* the vote — and it is unactionable twice over. TOMCAT has **no
weekly head** (plan 19 is not started), so `USG_Points` was null 3,602 of 3,602 times
on Knights_FFL 2025 and the weight already renormalises away; and the column measured
is the deliberately crude trailing baseline in `Scripts/usage/baseline.py`, not the
shipped season head. What it *is* good for is a **pre-registered warning for plan
19**, because the cost decomposes cleanly by population: mean **+16.91%** over all
rostered player-weeks, **+11.24%** once byes are excluded, and **+1.55%** on players
who took a snap. Roughly nine tenths of the damage is availability rather than
accuracy — a weekly head that confidently projects a player who did not suit up is
the problem to solve, and it is the problem plan 19 already names.

**2. Zeroing BetOnline on the touchdown stats looked like a stable −5.34%. It is an
artifact, and it is the reason `degenerate_rows` now exists.** A free fit puts
`PINNY=1.00` on `rushingTouchdowns`, with the two folds agreeing to **0.00** — the
identification this plan said was impossible, apparently found. It is not. Pinnacle
covers 7.7% of those rows; an **exact** zero on the others leaves no renormalised
denominator, `compute_weighted_stats` falls back to its face-value sum, and MAE on a
rare count rewards projecting nothing. Hold every weight a hair off zero and the same
ratios are worth **−1.49%**:

| | t = 0.999 | t = 1.0 | the gap |
|---|---|---|---|
| `rushingTouchdowns` | −1.49% | **−4.45%** | −2.96 |
| `receivingTouchdowns` | −0.88% | **−2.33%** | −1.45 |

Across all stats the collapsed denominator accounts for **−0.58 to −0.84 percentage
points** of the apparent gain. Production cannot reach that branch — ESPN carries
weight and is never imputed — so this is a hazard for any fitting harness rather than
a live bug, and it is now pinned by a test.

### Two methodological findings worth more than the verdict

**Fit the estimator that ships.** The first attempt fitted a plain linear combination
on the all-real intersection, which is an estimator nothing publishes and a
population of covered stars. `Scripts.lab.blend.predict` now reproduces
`compute_weighted_stats` cell for cell — including the face-value fallback — verified
to **1.1e-13** against the production function on three weight vectors and every
stat, and pinned by a test that drives the real function.

**A measurement is only as fresh as the store under it.** The 2025 store was built
2026-08-24; plan 34's touchdown split and plan 35's de-vig landed 2026-08-27.
Measured on the store as it stood, BetOnline over-projected touchdowns by **21%** and
"drop BetOnline" looked like a genuine finding with a mechanism behind it. Rebuilt by
current code, that column sits at **0.88** of consensus — the bias did not shrink, it
**reversed sign**. A weight fitted to the stale store would have been fitted to a
defect the pipeline had already fixed, pointing the wrong way. Plan 35's de-vig
**cannot** be replayed on 2025 at all: the archives hold post-conversion `proj_*` and
the raw prices are gone, so that correction is permanently unscoreable on this
season.

### What the season weights still cannot answer

`USG` and `DST` vote only on the season board, which is where the draft is read from
— and there is no historical season blend to score against. Plan 18 records that as a
permanent limitation of the data rather than a gap in the work. Everything above is
the **weekly** blend. The season weights remain a judgement call, and this
measurement does not change that.

> **Partly answered 2026-09-01, and the answer is still that the weights do not
> move.** `Scripts.usage.g1_season` *does* score a season blend — genuine pre-season
> FantasyPros and BetOnline against realised 2025 season totals, with TOMCAT
> walk-forward — so `USG`'s weight is not the pure judgement call this paragraph
> describes. It is measured, it sweeps to an interior optimum, and production sits on
> it. See [Step 3b](#step-3b-the-season-weight-and-the-mislabel-that-hid-it-2026-09-01).
> `DST`'s weight is still unmeasurable for the reason plan 30 gives: G-DST2(b) cannot
> be run.

## Step 3b: the season weight, and the mislabel that hid it (2026-09-01)

**Verdict: the season weight does not move either — and the gate that suggested
otherwise was reading its own curve at the wrong point.**

### The mislabel

`WEIGHTS['default']` gives TOMCAT **0.25 — the same as ESPN, FantasyPros, Pinnacle and
BetOnline.** Its *ratio* to any single external source is therefore **1.0**, and on a
row where all five are real it takes one fifth of the blend exactly as ESPN does.
Verified on the live 2026 board: where all five sources are real and unimputed,
`TRUE_` is their equal five-way mean, up to the `reconcile_team_totals` pass that runs
after blending.

`Scripts/usage/g1_season.py` sweeps that ratio. It hard-coded **0.25** as "what
ships", put its `<- ships` marker on that row, and swept `(0.05, 0.1, 0.25, 0.4, 0.5)`
— **a range whose maximum is half of production.** The curve therefore fell
monotonically to its own right-hand edge, which reads as *"TOMCAT is under-weighted,
the optimum is 0.5 or beyond"*. That reading was derived and acted on twice before
anyone checked what the number meant.

Fixed: `SHIPPED_WEIGHT` is now derived from `Scripts.projection_utils.WEIGHTS` rather
than restated, raises if the externals ever carry unequal weights (the ratio is only
defined under the equal-vote rule), and the sweep brackets it on both sides.
`tests/test_g1_season.py` pins all three.

### With the sweep bracketing production, the minimum is interior and lands on it

| TOMCAT ratio | 0.0 | 0.25 | 0.50 | 0.75 | **1.00 (ships)** | 1.25 | 1.50 | 2.00 | 3.00 |
|---|---|---|---|---|---|---|---|---|---|
| season-points MAE | 34.83 | 33.94 | 33.61 | 33.50 | **33.50** | 33.59 | 33.71 | 33.91 | 34.27 |

### The missing-ESPN caveat was wrong, and it is now measured

G1's basis carries no ESPN and no Pinnacle, and the module said so as a caveat: the
baseline is "weaker than a real board", so the comparison is "kinder to TOMCAT than a
live one would be". **Measured, it is not.**

ESPN's relationship to FantasyPros, on 4,080 paired 2025 player-weeks where both are
real and unimputed: residual correlation **0.985** (reproducing plan 16's G0 at
+0.988) and residual-sd ratio **1.01** — equally accurate, and very nearly the same
opinion. Injecting a synthetic ESPN with exactly those properties moves the baseline
from **34.83 to 34.85**. A 0.985-correlated near-duplicate of a source already in the
blend adds nothing, which is the same collinearity that sank step 3's simplex fit.

Swept over the correlation, 25 seeds per cell:

| synthetic ESPN's residual correlation with FP | 0.985 (measured) | 0.90 | 0.50 | 0.00 |
|---|---|---|---|---|
| optimal TOMCAT ratio | 2.00 | 1.50 | 0.50 | 0.00 |
| gain at the shipped 1.0 | +3.9% | +3.2% | +0.5% | −5.1% |

The optimum only falls below production once the missing source is *far* more
independent than ESPN actually is. At the measured correlation the optimum is a flat
basin over roughly 1.0–2.0 and production sits inside it, **0.5%** off the floor —
which is the lab's own `mean_gain` threshold for refusing to pay complexity for noise.

Reproduce with `python -m Scripts.usage.g1_season --season 2025`.

### What the sweep did surface: the residual is per position, not a scalar

Slope of realised on projected, 2025, season level. `>1` means the projection is too
**narrow**, `<1` too **wide**:

| pos · stat | base (FP+BOL) | **ships (1.0)** | TOMCAT alone |
|---|---|---|---|
| RB rushing yards | 0.904 | **0.993** | 1.037 |
| TE receptions | 0.854 | **0.982** | 1.263 |
| TE receiving yards | 0.898 | **1.022** | 1.243 |
| QB rushing TDs | 0.724 | **0.979** | 1.431 |
| WR receiving yards | 0.726 | **0.870** | 1.152 |
| RB receiving yards | 1.063 | **1.250** | 1.309 |
| QB passing yards | 0.704 | **0.747** | 0.819 |

Four cells land on 1.00 at the shipped weight, and the rest miss **in both
directions** — WR receiving is still over-spread, RB receiving is over-corrected.
A single scalar cannot fix both, which is why the MAE basin is flat: raising the
weight repairs WR and breaks RB. Any further gain has to be **per position**, and that
is a fitted object needing its own walk-forward gate rather than a number to nudge.

### Step 3c: the per-position weight, measured and rejected (2026-09-01)

Step 3b ends by locating the remaining gain "per position, not a scalar". Built and
measured the same day: **it does not survive out of sample, and the pre-registered rule
rejects it.**

**In sample it looks strong.** Sweeping a per-position TOMCAT weight on the
reconstructed five-source board, both criteria agree inside each position and point in
*opposite directions across* positions -- which is exactly why the scalar basin is flat:

| position | n | best w by MAE | MAE gain vs shipped | best w by calibration |
|---|---|---|---|---|
| WR | 186 | 3.0 | **+2.7%** | 6.0 |
| RB | 145 | 0.0 | **+2.2%** | 0.0 |
| TE | 133 | 2.0 | +0.6% | 1.5 |
| QB | 92 | 1.5 | +0.3% | 6.0 |

**Out of sample it collapses.** Forty random half-splits by player, fitted on one half
and scored on the other, both directions -- the same shape step 3 used, and for the
same reason:

| | mean_gain (bar +0.5%) | worst position (bar −2.0%) | stability (IQR ≤ 0.5) |
|---|---|---|---|
| reconstructed board | **−0.92%** | **−3.54%** (QB) | **FAIL** |
| real basis (FP+BOL) | **−1.69%** | **−4.07%** (QB) | **FAIL** |

**WR's signal is not robust to the reconstruction.** It is +1.0% with synthetic ESPN
and Pinnacle in the baseline and **−1.1%** without, with the fitted weight moving 3.0 →
1.5. That is a property of the modelling assumption, not of the data, and it is the
reason the reconstruction is reported here rather than relied on.

**One cell survives, and is still not worth taking.** Running back is stable
(+0.7% out of sample on *both* bases, fitted weight 0.0, IQR [0.0, 0.1]) -- TOMCAT's
vote is not earning its place at RB, which is consistent with G1's own Spearman barely
moving there. But 0.7% of a 36-point MAE is **a quarter of a point per player**, and
collecting it needs position-keyed weights in `compute_weighted_stats`, which is keyed
by stat today and sits on every board's critical path. That is new machinery in the
blend for a quarter of a point.

**Recalibrating the blend output directly is far worse.** Fitting `E[y|x] = a + b·x`
per (position, stat) on half the players and applying it to the other half loses
**−14.4%** MAE on average and **−36.9%** at quarterback, and makes calibration worse
too. The slopes are real in aggregate and cannot be estimated per cell from half a
season -- 46 quarterbacks per fold is not a fit.

**The blocker is the sample, not the model.** The five-season table below shows the
bias is real and stable; every attempt to *correct* it fails because the external
pre-season lines exist for one season only. That changes: the nightly has been writing
`snapshots/board/season=2026/league=*/date=*/` since 2026-08-11, all ten leagues, every
source's stat line including ESPN's, and the bucket lifecycle does not touch
`snapshots/`. **In 2027 this is a two-season question with a real four-source baseline**,
which is when it should be asked again. Nothing to build before then.

### TOMCAT's own calibration, which nothing recorded before

Walk-forward, one fit per season, TOMCAT against realised season totals. The bias is
stable in sign and size across every fold:

| pos · stat | 2021 | 2022 | 2023 | 2024 | 2025 | mean |
|---|---|---|---|---|---|---|
| RB receptions | 1.298 | 1.367 | 1.392 | 1.256 | 1.263 | **1.315** |
| RB receiving yards | 1.256 | 1.349 | 1.346 | 1.313 | 1.260 | **1.305** |
| WR receptions | 1.278 | 1.337 | 1.415 | 1.323 | 1.127 | **1.296** |
| WR receiving yards | 1.256 | 1.333 | 1.494 | 1.270 | 1.158 | **1.302** |
| TE receiving yards | 1.500 | 1.259 | 1.444 | 1.405 | 1.240 | **1.370** |
| QB rushing TDs | 1.094 | 1.574 | 1.733 | 1.721 | 1.430 | **1.510** |
| RB rushing yards | 1.019 | 1.138 | 0.971 | 1.164 | 1.046 | 1.068 |
| QB passing yards | 1.014 | 0.837 | 1.054 | 0.904 | 0.815 | 0.925 |

TOMCAT is calibrated on rushing and passing volume and **under-spread on receiving
volume and QB rushing**. It is also the best-calibrated single source in the blend:
FantasyPros runs 0.65–0.95 at season level and BetOnline 0.21–0.81, both **over**-spread.

**Correcting TOMCAT's under-spread in isolation would make the board worse**, and the
table above is why: the two errors point in opposite directions and partly cancel in
the blend. Widening TOMCAT removes the counterweight and pushes `TRUE_` further
over-spread. That is the trap this section exists to stop someone walking into — the
per-position work is the way in, not a scalar correction to either side.

## Verification

- Coverage report prints per-source real percentages inside every
  `clean_lineups` run.
- All-real rows are bit-identical before and after renormalisation (0 violations,
  three leagues, 45 stats).
- For a player a book does not cover, `TRUE_` equals the renormalised blend of the
  sources that do cover them.
- `Scripts.lab.blend.predict` agrees with `compute_weighted_stats` to 1.1e-13, and
  `tests/test_lab_blend.py` asserts it against the production function.
- The step 3 verdict reproduces from `python -m Scripts.lab.blend`, and is rendered
  into `docs/model_lab.html`.
- `TRUE_Points` is unchanged by step 3, because step 3 changed nothing — which is the
  point. The per-stat scoreboard that step 3's original wording asked for already
  exists as `python -m Scripts.lab.accuracy` (plan 34).
