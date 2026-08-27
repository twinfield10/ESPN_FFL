# 35 — What the books are actually telling us, and what we do with it

**Status:** TO DO

**Priority:** High (quality) · **Effort:** M · **Where it stands:** Evaluated 2026-08-27,
nothing built. Four defects measured, one of them worth 18–34% on the affected stat, plus
a per-player-week market-implied **variance** the pipeline computes and throws away.
**Depends on:** nothing · **Feeds:** [03](03-projection-source-coverage.md) ·
[19](19-weekly-usage-model.md) · [28](28-outcome-distributions.md) ·
[34](34-stat-first-audit.md)

---

## Problem

Two of the five sources are sportsbooks, and a posted market is not a projection — it is
a line plus a price. Turning one into a number requires two decisions: remove the book's
margin, and convert a threshold into an expectation. Both are made here, in two files,
differently, and neither has ever been measured.

---

## Evidence

### V1 · One formula, two coefficients, and no evidence for either

Both scrapers nudge the line by the same expression:

```
Juice_Diff = (1/impProb_Under - 1) - (1/impProb_Over - 1)
AdjValue   = value + Juice_Diff * value * k
```

`k = 0.5` in `Scripts/scrape_BOL.py:454`. `k = 0.25` in `Scripts/scrape_pinnacle.py:348`.
`docs/STATE_OF_THE_REPO.md` already records that the Pinnacle coefficient changed from
0.5 to 0.25 mid-2025 in commit `c3b4d16` with no explanation. The two have silently
drifted to a factor of two apart on identical arithmetic.

### V2 · The adjustment is scaled by the wrong quantity

`Juice_Diff` is a difference of decimal-odds-minus-one, and it is multiplied by
`value` — the **level** of the line. A line's sensitivity to an odds tilt scales with its
**standard deviation**, not its level, so scaling by level silently assumes a constant
coefficient of variation. Measured weekly, CV is nothing like constant: it falls
**0.81 → 0.44** across the rushing range and **0.52 → 0.25** across the passing range
([plan 34](34-stat-first-audit.md) F3, and `Scripts/usage/milestones.py`'s own fit). So
the current form over-adjusts large lines and under-adjusts small ones.

The textbook conversion needs no new machinery:

```
mean = line + Phi^-1(p_novig_over) * sigma(line)
```

and `sigma(line)` already exists — `Scripts/usage/milestones.py` fits
`Var(mu) = phi*mu + mu^2/k` on weekly residuals per position and stat, which is exactly
this quantity. It was built for the yardage milestones and applies here unchanged.

### V3 · BetOnline never removes the vig

Measured on `BetOnline_AllProps_Raw.parquet`: two-way implied probabilities sum to a
median of **1.0640** — a 6.4% overround — and nothing normalises them. `ou_calc` reads
the raw probabilities only to compute the tilt, and `value_calc` sums
`value * exactProb` straight from them, so the expectation inherits the hold.

### V4 · Pinnacle removes the vig and then uses a probability as a count

`Scripts/scrape_pinnacle.py:313` is `pl.col('Value').fill_null(pl.col('ImpNoVig'))`. The
anytime-TD market has no line, so its projection becomes the de-vigged **P(at least one
touchdown)** — and it is then consumed as an expected touchdown *count*. Those differ by
exactly the multi-touchdown games. Measured over 174,374 player-weeks, 2016–2025:

| pos | P(>=1 TD) | E[TDs] | E[N] / P(>=1) | share of TD weeks with 2+ |
|---|---|---|---|---|
| RB | 0.2403 | 0.3040 | **1.265** | 22.5% |
| WR | 0.1871 | 0.2154 | 1.151 | 13.9% |
| TE | 0.1560 | 0.1745 | 1.119 | 10.9% |
| QB | 0.1299 | 0.1479 | 1.138 | 13.1% |

Restricted to weeks a book would actually price — a player with three or more scoring
weeks that season — the RB factor is **1.295**.

### V5 · The two errors are large and point in opposite directions

Total projected touchdowns against total realised, 2025, on the rows each book really
priced (non-imputed cells only):

| source | pos | n | realised | projected | ratio |
|---|---|---|---|---|---|
| BetOnline | RB | 915 | 426 | 504.5 | **1.184** |
| BetOnline | WR | 1,100 | 353 | 447.7 | **1.268** |
| BetOnline | TE | 433 | 147 | 161.8 | 1.100 |
| Pinnacle | RB | 389 | 248 | 163.0 | **0.657** |
| *ESPN, same rows as BOL* | RB | 915 | 426 | 419.1 | *0.984* |
| *ESPN, same rows as PINNY* | RB | 389 | 248 | 225.9 | *0.911* |

One book 18–27% high, the other 34% low, while ESPN sits within 2–9% on the same
players. They partly cancel in the blend, which is an accident and not a design — and it
is why [plan 34](34-stat-first-audit.md) F2 found the blend's touchdown columns
mis-calibrated at 0.60 (RB receiving) and 1.10 (RB rushing).

### V6 · The books give a distribution per player-week and we keep only its mean

This is the largest item here, and it is not a defect so much as unclaimed information.

BetOnline posts a **ladder**, not a line. Every stat has a `Values` path with 2 to 17
thresholds per player-stat-week, and `value_calc` already differences the cumulative
implied probabilities into `P(exactly k)` correctly. For Kyle Pitts' anytime market:

```
P(>=1)=0.333  P(>=2)=0.077  P(>=3)=0.012
P(exactly k) = [0.2564, 0.0652, 0.0118]
E[N] = 0.4220    Var[N] = 0.4448    sd = 0.6670
```

The function returns `E[N]` and discards `Var[N]`. And where a two-way line also exists,
the coalesce at `scrape_BOL.py:493` **prefers the single juice-nudged line over the
ladder** — for `receivingYards` that is 102 ladder rows discarded in favour of 22
two-way ones.

Nothing else in this repo has a market-implied dispersion at player-week grain. Both
places that need one currently fit it from history:
[plan 28](28-outcome-distributions.md)'s outcome distributions and plan 34's milestone
bands. And [plan 34](34-stat-first-audit.md) F5 withdrew the quarterback passing
interval precisely because a fitted Gamma has the skew inverted — a ladder carries the
shape directly and would not need a family assumed for it.

### V7 · Two smaller things found on the way

`value_calc`'s first-row condition is `pl.col('impProb') == pl.col('value').max()`, which
compares a probability to a threshold and is therefore **always False**. The
`pl.coalesce` two lines later happens to produce the correct answer anyway, so the
arithmetic is right and the expression is dead. Worth deleting rather than leaving as a
thing a reader has to verify.

The OverUnder/Values join at `scrape_BOL.py:490` is an **inner** join, so a player with a
ladder and no two-way line for that stat is dropped. Measured on the archived week it
drops **0** pairs — every laddered player also has a two-way line — so this is a latent
risk rather than a live bug, and it is written down so it is not rediscovered as one.

---

## Fix

Cheapest first. Each is independently shippable and each moves `TRUE_`, so all of it is
post-draft work.

1. **De-vig BetOnline.** Normalise two-way pairs to sum to 1 and the anytime ladder to
   its own total. Removes a measured 6.4%+ inflation. One function, shared with Pinnacle,
   which already does this correctly.
2. **Stop using a probability as a count.** Pinnacle posts one anytime threshold, so the
   correction is a fitted positional multiplier — 1.15 to 1.30, measured in V4. Prefer
   BetOnline's ladder where both exist, since it needs no multiplier at all.
3. **Unify and re-derive the juice coefficient** as `Phi^-1(p) * sigma(line)`, reusing
   `Scripts/usage/milestones.py`'s dispersion fit. This replaces two undocumented
   constants with one derivation, and it can be scored the same way everything else here
   is: per-stat calibration on the archived 2025 store.
4. **Capture the ladder's variance.** Emit `BOL_<stat>_sd` beside `BOL_<stat>` and stop
   preferring a single line over a ladder. Feeds plans 19, 28 and 34's open items.

**Judge all of it on calibration, not MAE.** [Plan 34](34-stat-first-audit.md) F3 records
why: a weekly touchdown count is zero about 95% of the time, MAE is minimised by the
median, and a source projecting a flat zero therefore beats one projecting the truth.
`Scripts/lab/accuracy.py` reports both.

## Reproducing the evidence

```bash
python -m Scripts.lab.accuracy          # V5, the calibration block
python -m Scripts.lab.persistence       # V2, the CV range
```
V3, V4 and V6 are measured directly off
`Data/Projections/BetOnline/Landing/<season>/BetOnline_AllProps_Raw.parquet` and
`Data/NFL/<season>/player_weeks.parquet`.
