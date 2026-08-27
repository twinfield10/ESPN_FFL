# 35 — What the books are actually telling us, and what we do with it

**Status:** COMPLETE

**Priority:** High (quality) · **Effort:** M · **Where it stands:** Evaluated 2026-08-27,
built 2026-08-27. All four fixes shipped in one place, `Scripts/market.py`, plus two
defects and two bugs the build found that the evaluation had not. **Measured against
realised 2025 outcomes on the markets that can be scored, every calibration ratio moves
toward 1.00 and three move a long way: BetOnline quarterback interceptions 0.712 →
1.011, Pinnacle running-back rushing touchdowns 0.679 → 0.996, receiving 0.591 → 0.894.**
**Depends on:** nothing · **Feeds:** [03](03-projection-source-coverage.md) ·
[19](19-weekly-usage-model.md) · [28](28-outcome-distributions.md) ·
[34](34-stat-first-audit.md)

---

## Problem

Two of the five sources are sportsbooks, and a posted market is not a projection — it is
a line plus a price. Turning one into a number requires two decisions: remove the book's
margin, and convert a threshold into an expectation. Both were made in two files,
differently, and neither had ever been measured.

---

## Evidence

Every number below was re-measured during the build. Where a claim did not survive, the
correction is inline and marked.

### V1 · One formula, two coefficients, and no evidence for either

Both scrapers nudged the line by the same expression:

```
Juice_Diff = (1/impProb_Under - 1) - (1/impProb_Over - 1)
AdjValue   = value + Juice_Diff * value * k
```

`k = 0.5` in `Scripts/scrape_BOL.py`. `k = 0.25` in `Scripts/scrape_pinnacle.py`.
`docs/STATE_OF_THE_REPO.md` records that the Pinnacle coefficient changed from 0.5 to
0.25 mid-2025 in commit `c3b4d16` with no explanation.

**Correction: there was a third copy.** `Scripts/projection_utils.py:clean_pinny` carried
a commented-out `adjust_value` with `k = 0.5`, inert but present, so a reader had to
decide which of three coefficients shipped. Deleted.

### V2 · The adjustment is scaled by the wrong quantity

`Juice_Diff` is a difference of decimal-odds-minus-one, and it was multiplied by `value`
— the **level** of the line. A line's sensitivity to an odds tilt scales with its
**standard deviation**, not its level, so scaling by level silently assumes a constant
coefficient of variation. Measured weekly, CV falls **0.81 → 0.44** across the rushing
range and **0.52 → 0.25** across the passing range ([plan 34](34-stat-first-audit.md)
F3).

Measured on the archived scrape, the over-adjustment is large: Kyle Pitts, receiving
yards, line 54.5 — `k = 0.5` moved it **+6.27 yards**; the derivation moves it **+2.41**.
Mean absolute movement per stat, `k=0.5` against the derivation: tackles 0.76 → 0.23,
pass attempts 1.59 → 0.28, receptions 0.84 → 0.34, receiving yards 0.98 → 0.43.

The conversion needed no new machinery: `mean = line + Phi^-1(p_novig_over) * sigma(line)`.
It is exact at `q = 0.5`, and the corrections are small — measured, the de-vigged price
has median exactly **0.5000** and `|q - 0.5|` a median of **0.0199**, maximum 0.1388.

**Correction: `sigma(line)` did not already exist for most of these markets.**
`Scripts/usage/milestones.py` fits it for three yardage stats. The books post ten. The
same two-parameter function (`Scripts.usage.predictive.fit_variance`) is now fitted for
every market, per position, in `Data/NFL/models/market_1.0.0.json` — and it reproduces
the milestone numbers exactly where they overlap, because it is the same fit on the same
panel.

### V3 · BetOnline never removes the vig — confirmed exactly

Two-way implied probabilities sum to a median of **1.0640** (n=51, range 1.0621–1.0658)
and nothing normalised them.

**And the one-sided ladder carries the same margin, which the plan said could not be
measured.** Plan 35 proposed normalising "the anytime ladder to its own total". That is a
**no-op by construction**: differencing a survival function gives exact probabilities
summing to `P(X >= t_1)`, and the residual `P(X < t_1)` makes the total 1 whatever the
prices are. So the ladder's hold was measured against the two-way line beside it instead —
interpolate for yardage, read the matching rung for a count — and it comes out at
**1.0649** (n=48), 1.063–1.065 on *every* count stat. One measured number de-vigs both
shapes.

*(An earlier pass at this measurement read a discrete ladder by interpolation and got
1.30 on receptions and 1.51 on passing touchdowns. `P(X > 4.5)` **is** `P(X >= 5)`;
averaging the rungs at 4 and 5 invents a number the book never posted. The 1.065 is the
one with the arithmetic right.)*

### V4 · Pinnacle uses a probability as a count — right conclusion, wrong mechanism

The plan named `Scripts/scrape_pinnacle.py:313`, `pl.col('Value').fill_null(pl.col('ImpNoVig'))`,
and said the anytime market has no line. **It has one.** Pinnacle posts it as
`Over/Under 0.5 Touchdowns`, so `Value` is 0.5 and that `fill_null` fired on **no row of
the archived store**. It was a defect waiting for a market shape, not a live one, and it
is gone.

The live mechanism was worse. `AdjValue = 0.5 + Juice_Diff * 0.125` lands near the
de-vigged `P(at least one touchdown)` by numerical accident in the middle of the range,
and it has no floor: measured on the archived 2025 store, the combined touchdown column
ran **−0.698 to 1.124** with a mean of **0.417**, and **14 of 421 player-weeks projected a
negative number of touchdowns**. The rows in question realised 0.637 a week.

**Correction: the same defect was live on a stat the plan did not look at.** BetOnline's
`passingInterceptions` line is 0.5 in every archived row, so its projection was also a
probability wearing a count's name — calibrated at **0.712**, the worst BetOnline number
in the store.

The multi-touchdown share the plan measured is real, over 174,374 player-weeks 2016–2025:

| pos | P(>=1 TD) | E[TDs] | E[N] / P(>=1) | share of TD weeks with 2+ |
|---|---|---|---|---|
| RB | 0.2403 | 0.3040 | **1.2650** | 22.5% |
| WR | 0.1871 | 0.2154 | 1.1515 | 13.9% |
| TE | 0.1560 | 0.1745 | 1.1187 | 10.9% |
| QB | 0.1299 | 0.1479 | 1.1385 | 13.1% |

Restricted to weeks a book would price — three or more scoring weeks that season — the RB
factor is **1.2953**. Every figure reproduced to four decimals.

**Correction: the fitted multiplier the plan proposed is not the best available
conversion, and it is not what shipped.** A flat ratio is correct at one rate. The true
ratio runs from 1.0 at a rate of zero to 1.6 at a touchdown a game, so 1.2650 applied to
a back priced at 0.08 is 22% high. Inverting the count distribution instead — solve
`P(N >= k) = q` under the Negative Binomial this repo already fits for counts, degenerating
to Poisson where the fit is underdispersed — is calibrated across the range. Binned by
realised per-game rate, 2016–2025:

| market | k | rate range | inversion / realised |
|---|---|---|---|
| anytime touchdown | 1 | 0.08 – 0.67 | 1.02 – 1.05 |
| passing interceptions | 1 | 0.31 – 1.13 | 0.96 – 1.05 |
| passing touchdowns | 2 | 0.74 – 2.30 | 1.02 – 1.13 |
| receptions | 5 | 3.2 – 5.9 | 0.97 – 1.01 |

The multiplier is kept in the artifact as the fallback where a stat has no fitted
dispersion, and as the number that made the defect visible.

### V5 · The two errors are large and point in opposite directions — confirmed

Total projected touchdowns against total realised, 2025, on the rows each book really
priced:

| source | pos | n | realised | projected | ratio |
|---|---|---|---|---|---|
| BetOnline | RB | 915 | 426 | 504.5 | **1.184** |
| BetOnline | WR | 1,100 | 353 | 447.7 | **1.268** |
| BetOnline | TE | 433 | 147 | 161.8 | 1.100 |
| Pinnacle | RB | 389 | 248 | 163.0 | **0.657** |
| *ESPN, same rows as BOL* | RB | 915 | 426 | 419.1 | *0.984* |
| *ESPN, same rows as PINNY* | RB | 389 | 248 | 225.9 | *0.911* |

All six reproduced from `Scripts/lab/results.json`. They partly cancel in the blend,
which is an accident and not a design.

### V6 · The books give a distribution per player-week and we keep only its mean

BetOnline posts a **ladder**, not a line: 2 to 17 thresholds per player-stat-week. For a
count rooted at 1 the moments are exact identities, `E[N] = sum_k P(N >= k)` and
`E[N^2] = sum_k (2k-1) P(N >= k)`, so the ladder gives a mean *and* a variance with no
family assumed. Kyle Pitts' anytime market reproduces the plan's worked example to four
decimals: `E[N] = 0.4220`, `sd = 0.6670`. `value_calc` computed the mean by the same
arithmetic under another name and discarded the variance.

**Correction, and it changes what item 4 of the Fix could safely do.** The plan asked to
stop preferring a single line over a ladder. Applied to a *yardage* ladder,
`sum(threshold * P(bucket))` is not a mean: it assigns each bucket's mass to its lower
edge and drops everything below the lowest rung, which is nowhere near zero — Matthew
Stafford's passing ladder starts at 253. Measured against the line posted beside it, that
expression runs **0.77 to 1.81** over 17 ladders. Not a bias in one direction; a number
that is neither a mean nor a median. Shipping it as the projection would have been worse
than the single line it replaced.

So a yardage ladder is now read for what it states directly: its **median** and its
**scale**. The median is the check that validates the whole de-vig on prices alone —
de-vigged, the ladder's own median reproduces the posted line to within the rung spacing
on every stat (0.49 yards on passing against 10-yard rungs, 1.23 on receiving, 0.12–0.77
on the integer-rung counts). No outcome data involved.

**A finding worth its own line: the market's dispersion reads 1.64x the fitted one.**
Measured across the archived yardage ladders at the same line. It is emitted as
`<stat>_sd` and is *not* substituted into the mean, because the gap has three components
pointing two ways and the archive cannot separate them — a fit whose x-axis is a hindsight
season mean (which argues the fit is too narrow, and matches plan 34 F3's 49–57% coverage
against a nominal 80%), a book shading its long shots (against a Gamma matched to the
fitted sigma and the ladder's median, the de-vigged survival runs 0.99 of it below the
median, **1.42** at q75–90), and the one-sided read meeting a skewed shape (~6%).
Separating them needs realised outcomes against archived prices. See
`Scripts.market.MARKET_OVER_FITTED_SCALE`.

### V7 · Two smaller things found on the way — both confirmed

`value_calc`'s first-row condition was `pl.col('impProb') == pl.col('value').max()`,
comparing a probability to a threshold and therefore always False. The `pl.coalesce` two
lines later produced the correct answer anyway. Gone.

The OverUnder/Values join was an **inner** join, so a player with a ladder and no two-way
line for that stat was dropped. It drops **0** pairs on the archived week among the stats
where the join actually runs — the three markets with no over/under at all take a
different branch — so it was a latent risk. It is now a full join, and the risk became
live the moment a laddered player with no line started carrying a variance.

---

## What was built

One module, `Scripts/market.py`, holding every price decision, with a fitted artifact
(`Data/NFL/models/market_1.0.0.json`: weekly dispersion per position and market, plus the
touchdown ratios). Both scrapers call it and neither derives anything.

1. **De-vig, one function, both shapes.** Proportional for a two-way pair, and the same
   measured overround divided out of a one-sided ladder. Measured per scrape rather than
   hardcoded, with a bounds guard so a malformed pivot cannot gut a week.
2. **`line + Phi^-1(q) * sigma(line)`** for yardage, replacing two coefficients with one
   derivation, and a dispersion fitted for all ten markets rather than three.
3. **An inversion of `P(N >= k) = q`** for every count market, which is the fix for V4 and
   for the interception market V4 did not know about. It subsumes the proposed multiplier.
4. **`proj_<stat>_sd` beside `proj_<stat>`** from the ladder — exact for a count rooted at
   1, a quantile read otherwise — and the inner join made full.
5. **Per-week raw price archives** in both scrapers. This is the change that makes the
   plan's own instruction executable: the landing file was **overwritten every run**, so
   the only prices the repo still held were the last scrape of 2025, one game. That is why
   most of this could not be scored end to end, and why it can be next season.

`Scripts/lab/market.py` measures all of it and writes `market_lines` to the ledger.

## What it is worth, measured

2025 calibration, total projected over total realised on played rows, on the markets whose
old projection is an invertible function of the price that produced it:

| source | stat | n | before | after |
|---|---|---|---|---|
| BetOnline QB | passingInterceptions | 431 | 0.712 | **1.011** |
| Pinnacle RB | rushingTouchdowns | 389 | 0.679 | **0.996** |
| Pinnacle RB | receivingTouchdowns | 389 | 0.591 | **0.894** |
| BetOnline RB | rushingTouchdowns | 910 | 1.490 | 1.401 |
| BetOnline WR | receivingTouchdowns | 1,098 | 1.308 | 1.229 |
| BetOnline TE | receivingTouchdowns | 431 | 1.127 | 1.059 |
| BetOnline QB | rushingTouchdowns | 440 | 1.123 | 1.055 |

**Judged on calibration, not MAE**, for the reason [plan 34](34-stat-first-audit.md) F3
records: a weekly touchdown count is zero about 95% of the time, MAE is minimised by the
median, and a source projecting a flat zero beats one projecting the truth.

**Two markets cannot be scored this way and are not claimed.** `BOL_passingTouchdowns`'
line is 1.5 or 2.5 depending on the quarterback and the line was not archived, so one
stored number has two candidate prices. Every yardage market has a moving line, same
problem. Their arithmetic is verified on the one archived game and by unit test; their
calibration waits for archived prices.

## What is left, and where it belongs

* **BetOnline's touchdown allocation.** All of a back's anytime market goes to the rushing
  column and none to receiving, so BOL@RB receiving touchdowns calibrates at **0.0** on
  910 player-weeks and rushing stays 40% high after de-vigging. That is
  [plan 34](34-stat-first-audit.md) F2's open item, measured there as worth 0.597 → 0.891.
  Deliberately untouched here.
* **Pinnacle's touchdown split needs both yardage columns**, so a receiver with no rushing
  line gets no touchdown projection at all — Drake London and Kyle Pitts get none in the
  archived week. Same allocation question, same owner.
* **The season/draft path is untouched, on purpose.** `Scripts/scrape_pinnacle_season.py`
  computes `no_vig_over_prob` and `Scripts/season_projections.py:_pivot_props` then
  projects the raw `line`, discarding it. That is the same class of defect on the draft
  board, and mid-camp is the wrong week to move a draft board. Its own plan.
* **The 1.64x dispersion gap** between the market and the fit, above.

## Reproducing

```bash
python -m Scripts.market --fit         # the artifact
python -m Scripts.market --show        # the fitted numbers
python -m Scripts.lab.market           # every measurement above, and the calibration
python -m Scripts.lab.accuracy         # V5, the calibration block
python -m Scripts.lab.persistence      # V2, the CV range
pytest tests/test_market.py
```
`Scripts/lab/accuracy.py` still scores the archived store, which holds the *old*
arithmetic — it cannot move until new weeks are scraped, and `Scripts/lab/market.py`'s
section 5 is the number that says what the change is worth.
