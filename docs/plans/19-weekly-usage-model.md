# 19 — The weekly usage model (in-season head)

**Priority:** High (quality) · **Effort:** M–L · **Status:** Not started
**Depends on:** [16](16-usage-data-layer.md) — Step 0 gates and the feature layer ·
[18](18-season-usage-model.md) — shares the availability head, but does not block
**Feeds:** [08 (weekly views)](08-frontend-weekly-views.md) ·
[03 (weight re-tune)](03-projection-source-coverage.md)

## Why this head, and not the draft one, is where the edge is

[Plan 16 §Measurements](16-usage-data-layer.md#measurements-that-decide-the-design)
measured it. Next-week PPR, RB/WR/TE, 42,796 player-weeks over 2016–2025:

| Model | r | R² | resid sd |
|---|---|---|---|
| trailing-3 **actual** fantasy points | +0.520 | 0.2702 | 6.56 |
| trailing-3 **expected** fantasy points | +0.539 | **0.2907** | 6.46 |
| trailing-6 expected | +0.552 | — | — |
| both trailing-3 terms | — | 0.2978 | 6.43 |
| + trailing-6 expected | — | **0.3124** | 6.36 |
| + usage shares and over-expectation | — | 0.3153 | 6.35 |

Fitted jointly, the two-term model puts **0.537 on expected production and 0.234
on actual** — it leans on what a player *earned* more than twice as hard as on
what he *scored*. No current source exposes expected production at all.

That is the whole case for this plan, and unlike the season head
([plan 18](18-season-usage-model.md), where expected PPG does *not* beat actual
PPG) it is unambiguous.

## What the measurement says to build, and what not to

**Build:** trailing expected production, at two window lengths. It is the single
largest term and the longer window adds real signal (0.2978 → 0.3124).

**Build, but scope it:** usage shares. Pooled they are worth 0.0029 on top of
expected production — because `ff_opportunity` already encodes them. By position:

| Pos | n | both | +t6_exp | +usage | shares are worth |
|---|---|---|---|---|---|
| QB | 5,492 | .2334 | .2424 | .2425 | +0.0001 |
| RB | 12,703 | .3269 | .3374 | .3389 | +0.0015 |
| WR | 19,996 | .2707 | .2877 | **.2980** | **+0.0103** |
| TE | 10,097 | .2429 | .2586 | **.2700** | **+0.0114** |

So carry route share, target share and WOPR for **WR and TE**, where they pay,
and do not spend effort engineering them for QB and RB.

**Shrink to near zero:** points over expected. r = +0.090 weekly, +0.193 year over
year. It is the thing that regresses, not the thing that predicts. Carry it as a
regressor with heavy shrinkage, never as a level adjustment.

**Do not build:** anything that re-derives expected production from raw plays.
`load_ff_opportunity` is maintained, keyed on `gsis_id`, and covers 2016–2025.

## The shape

```
E[stat]  =  P(active)  ×  E[stat | active]
            ─────────     ────────────────
            availability   opportunity × efficiency
            head           heads
```

Separating them is the design decision that matters most in-season, and plan 16's
injury measurement is why.

### The availability head

`load_injuries` — 55,556 rows 2016–2025, `gsis_id` 100% populated, free, and
published Wednesday to Friday so using week *N*'s report for week *N* is not
leakage.

Measured over a 65,640 player-week grid:

| Week-N designation | n | % missed the game | % of baseline when they played |
|---|---|---|---|
| Out | 2,379 | **100.0** | 0 |
| Doubtful | 393 | **99.2** | 1 |
| Questionable | 3,510 | 35.0 | 62 |
| not on report | 59,356 | 23.0 | 82 |

Out and Doubtful are effectively deterministic — that alone is worth having,
because the current pipeline will happily project 18 points for a player who is
ruled out. Questionable is a coin-flip that the practice column resolves:

| Questionable, by practice participation | n | % missed | % of baseline |
|---|---|---|---|
| Did not participate | 542 | **57.2** | 38 |
| Limited | 2,272 | 33.5 | 63 |
| Full | 655 | 22.3 | 79 |

A 57% vs 22% spread inside one designation is a real edge. **ESPN gives a status
string; none of the four projection sources model availability as a number.**

Requirements on this head:

- **Calibrated, not just accurate.** It multiplies a production estimate, so a
  systematically overconfident P(active) biases every downstream projection.
  Brier score and a reliability curve are deliverables, not niceties.
- **Two outputs, not one** — P(active) and, conditional on playing, an expected
  workload haircut. The table above shows these differ: a Questionable player who
  practised in full misses 22% of the time *and* returns 79% of baseline when he
  plays. Both effects are real and they compound.
- **`load_rosters_weekly.status` is required, not optional.** The 23% miss rate
  for players not on the report is inflated because the injury report drops a
  player once he lands on IR. Without roster status the head cannot separate
  "hurt" from "not on the roster" and will read IR as healthy.

### Teammate absence

The second thing injury data buys, and it needs the availability head to exist
first. A WR2's target share when the WR1 is Out is a large, systematic, weekly
shift that no projection source prices quickly. Computable from injuries +
depth charts and from nothing else in the free stack.

Implement it as a team-level redistribution: when a teammate's P(active) is low,
reallocate their expected opportunity share across the remaining depth chart.
Keep it explicit and auditable rather than folding it into a feature vector.

### The production heads

Volume × efficiency, per [plan 16 §Design](16-usage-data-layer.md#volume--efficiency-not-yards-in-one-step).
Feature emphasis set by the measurements above. Emits `USG_<stat>` per
player-week, which `proj_to_score` prices for each of the nine leagues.

Model family: something interpretable first. The value here is a new **input**,
not a clever estimator, and a regularised linear opportunity model with explicit
shrinkage on efficiency is a real baseline rather than a placeholder. `sklearn`
and `xgboost` are both already installed; reach for the second only once the
first is beaten honestly on the walk-forward.

## Leakage discipline

Inherited from [plan 16](16-usage-data-layer.md#leakage-discipline) and repeated
here because this is where it will actually bite. Every feature for week *N* is
built from weeks strictly before *N*; the week-*N* injury report is the single
exception and is legitimate because it predates kickoff.

The two tests in `tests/test_usage_features.py` are the guard:

- a feature frame for week *N* is unchanged when week *N*'s stat rows are altered
- a feature frame built at week *N* equals the same slice of one built at *N*+5

## Abstention

There is no current-season data until games are played, and a trailing-3 window
needs three of them.

- **Weeks 1–2: emit nothing.** Not a prior-season fallback, not a positional
  default — nothing. That is [plan 18](18-season-usage-model.md)'s job and it is
  already in the blend.
- **Weeks 3–4: partial.** Emit only for players whose trailing window has sample.
  Per-row abstention, not all-or-nothing.
- **Week 5 onward: full.**

Already handled end to end: plan 07 made a wholly-absent source degrade correctly
— `impute_columns` fills from `MEAN_`, flags every cell, and
`compute_weighted_stats` renormalises it out of `TRUE_*`. The app's coverage panel
shows `USG` at 0% in week 1 and rising, which is the honest display.

## Backtest

Two nested loops, because there are two kinds of leakage to avoid.

- **Across seasons:** for *S* in 2019…2025, train on ≤ *S*−1, predict *S*.
- **Within season:** expanding window. Features for (*S*, *W*) use only
  (*S*, < *W*) plus prior seasons.

**Baselines it must beat:**

1. trailing-3 actual points per game — the cheap heuristic, R² 0.2702
2. ESPN's own weekly projection alone
3. the current four-source `TRUE_` blend

**Metrics:**

| Metric | Why |
|---|---|
| per-stat MAE / RMSE | diagnoses which half of volume × efficiency is wrong |
| per-league fantasy-point MAE via `proj_to_score` | nine leagues price the same line differently |
| **start/sit accuracy** | the weekly decision is a ranking within a slot, not a point estimate |
| availability head: Brier score + reliability curve | scored separately; a miscalibrated multiplier corrupts everything downstream |

Report all of them for the blend **with and without** `USG_`. Measure on
non-imputed cells only — the `*_is_imputed` flags exist for exactly this, and
scoring ESPN against its own imputed copy is how the current weights got their
reputation.

## Ship criteria

- **G0** — usage residuals materially less correlated with ESPN's than ESPN's are
  with FantasyPros'. Gated in [plan 16 Step 0](16-usage-data-layer.md#step-0--the-gates),
  before any of this is built.
- **G1** — adding `USG_` reduces blended per-stat MAE on the 2025 holdout.
- **G3** — the availability head is calibrated: reliability curve within a stated
  tolerance of the diagonal on held-out seasons.

G3 has teeth of its own. Even if G1 fails, a calibrated P(active) is worth
shipping on its own as a lineup-safety flag in
[plan 08](08-frontend-weekly-views.md) — "this projection assumes a 43% chance he
plays" is useful whether or not the production model beats ESPN.

If G1 fails, do not wire the production side in at a token weight. Record the
numbers here.

## Steps

1. Plan 16 Step 0 and its feature layer. Blocking.
2. `Scripts/usage/availability.py` — P(active) and the workload haircut, with
   calibration diagnostics. Independently useful; build it first.
3. `Scripts/usage/weekly.py` — opportunity and efficiency heads, `USG_<stat>`
   per player-week, per-row abstention.
4. Teammate-absence redistribution.
5. Loader + `WEIGHTS` entry + `proj_to_score` prefix, following the
   `clean_pinny` / `clean_bol` pattern in `Scripts/projection_utils.py` including
   its absent-source path.
6. Walk-forward backtest; write the tables into this document.
7. Hand the enlarged source set to
   [plan 03](03-projection-source-coverage.md)'s weight re-tune.

## Risks

- **Positional coverage.** Rich for RB/WR/TE, thinner for QB, absent for K and
  D/ST. Emit nothing where the features do not exist. D/ST is
  [plan 13](13-dst-from-vegas-lines.md)'s job.
- **Upstream cadence.** `load_ff_opportunity` is `ffverse/ffopportunity` release
  data, not nflverse core. If it stops refreshing mid-season the head loses its
  best feature *silently*. Record the release timestamp with each pull and warn
  when it is more than a week stale — the same discipline
  `Scripts/crosswalk.py` uses for the player-id file.
- **The injury report is a moving target within a week.** The archived file is the
  final pre-game state; a Thursday run of the pipeline sees a less complete
  report than the file implies. The backtest will therefore be slightly
  optimistic about availability. Quantify it rather than ignoring it: re-score
  the head using only Wednesday-status rows and report both numbers.
- **`E[stat] = P(active) × E[stat|active]` understates variance.** The product of
  two estimates is not a distribution. Floor/ceiling for lineup decisions needs
  the simulation path, not this point estimate.
