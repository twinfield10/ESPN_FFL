# 03 — Blend weights assume coverage the sources don't have

**Status:** IN PROGRESS

**Priority:** High · **Effort:** Medium · **Where it stands:** Steps 1, 2, 4 done ·
**step 3 measured 2026-08-27 and the answer is no** · step 5 open

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

**5. Fix `scrape_pinnacle.py`'s import-time scrape.** Found 2026-08-03 while
verifying the season rollover. Lines 538-559 are module-level statements —
`driver.get()`, `WebDriverWait(...).until()`, `reconcile_props()` — with no
`if __name__ == "__main__":` guard. So merely *importing* the module launches
Chrome and scrapes Pinnacle, which means no tool or test can touch it without
triggering a live scrape, and a scrape failure surfaces as an `ImportError`.

It currently times out on `div[class*="matchupMetadata"]`, so Pinnacle is
effectively a second dead source alongside BetOnline (plan 02) and needs the same
kind of decision. Wrap the driver work in a `main()` behind a `__main__` guard
before diagnosing whether the selector or the pre-season page is the cause —
otherwise the two failure modes are indistinguishable.

Note `SEASON = current_season()` at line 18 is also import-time, so it is bound
before any caller can override it. Correct now that the schedule file says 2026,
but it makes the module unusable for backfilling another season.

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

**Step 5 (Pinnacle's import-time scrape) is still open.** `scrape_pinnacle.py`
lines 538-559 remain module-level. `Scripts/scrape_pinnacle_season.py` is the
pattern to follow: everything behind `main()`, guarded by `__main__`, with a test
asserting no bare module-level calls.

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
