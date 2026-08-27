# 34 — Stat-first, audited: where points leaked back into a stat-line pipeline

**Status:** COMPLETE

**Priority:** High · **Effort:** M · **Where it stands:** **Phases 1–6 done; the three
owed items closed 2026-08-27, and two of them closed by rejecting what the audit
proposed.** The milestone model shipped and calibrates 0.90–1.26. The shrinkage
argument was **wrong in its direction** — `implied_k` is a ceiling, not a floor — and
three walk-forward experiments rejected the change. The QB interval was **withdrawn
rather than refitted**, because the Gamma has the skew inverted and no dispersion fixes
that. Building the milestone model also found a bug in shipped code: 81 of 3,508 board
interval cells ship p10 == p90. Previous status: **Phases 1–3 built 2026-08-26.**
The per-stat scoreboard and the persistence study exist and both found something; volume
is in the blend at max |Δ| **0.0** on every `<prefix>_Points` column across all nine 2026
boards; the weekly points patch is gone and the two defects it was hiding are named. Source
bias adjustment and any weight movement are **deliberately deferred** — the owner's
direction is to get individual stat-line projections right first and re-compute after.
**Depends on:** nothing · **Feeds:** [19](19-weekly-usage-model.md) ·
[03](03-projection-source-coverage.md) · [01](01-scoring-coverage.md) ·
[31](31-team-coherent-tomcat.md)

---

## Problem

The premise of this repo is that a projection is a **stat line**, and fantasy points are
what you get when you apply a league's own rules to one. That ordering is why one pipeline
serves nine leagues, and the code says so in several places
(`Scripts/usage/season.py:20-24`, `Scripts/kicking/model.py:9-12`).

**It is mostly true.** The blend runs per stat column and collapses to points exactly once,
at `Scripts/projection_utils.py:_apply_scoring`. TOMCAT emits `USG_<stat>` from volume ×
efficiency × games. The kicker and defence arms emit stat vectors. The outcome simulator
draws an eight-stat tensor and dot-products it. The lab already gates experiments on
per-stat MAE, and says why.

**Seven places it was not**, found by audit and each measured rather than asserted.

---

## Evidence

### F1 · The blend threw away every volume stat

The blend list was the league's *scored* columns, in both grains
(`projection_utils.py:927`, `season_projections.py:1902`). So a stat only got a `TRUE_`
column if it paid points. On the 2026 board:

| prefix | stat columns | volume columns |
|---|---|---|
| `ESPN_` | 138 | `passingAttempts, passingCompletions, rushingAttempts, receivingTargets` |
| `FP_` / `PINNY_` | 138 | same |
| `BOL_` | 140 | same |
| **`TRUE_`** | **45** | **none** |

Four sources published volume — three of them as real market lines — and the blend
discarded all of it. What that cost:

- **No coherence check was possible.** 1,584 passing yards on 116 attempts and 2,091 on
  441 look equally reasonable until you divide, and there was no attempts column to
  divide by.
- **The team attempt budget could not be expressed.**
  [Plan 31](31-team-coherent-tomcat.md) locates TOMCAT's team-identity drift in a missing
  team snap budget. A snap budget is a statement about attempts.
- **Volume is the forecastable half** (F3 below) and it was the half being dropped.
- `BOL_rushingPlusReceivingYards` and `BOL_rushingPlusReceivingTouchdowns` are scraped,
  mapped (`season_projections.py:82-83`) and reach nothing, having no scoring rule.

### F2 · The blend had never been scored per stat, and per stat it loses on rushing touchdowns

Every scored evaluation in the repo judges TOMCAT (`usage/backtest.py`, `usage/gates.py`,
`usage/g1_season.py`, `lab/run.py`) or a calibration curve (`outcomes/backtest.py`,
`injury/backtest.py`). The four-source blend that actually ships had never been scored
against a realised stat line — although `Data/Store/2025/<league>/lineups.parquet` has
carried the projected line and the realised one side by side, for nine leagues, all along.

`python -m Scripts.lab.accuracy`, 5,257 player-weeks pooled across nine leagues, each
source paired against the blend **on the cells that source was real for**:

| stat | vs ESPN | vs FP | vs PINNY | vs BOL |
|---|---|---|---|---|
| passingYards | −1.6% | +1.5% | −2.8% | −3.3% |
| passingTouchdowns | −2.7% | −0.6% | −1.8% | −2.8% |
| passingInterceptions | −0.6% | −0.3% | +1.6% | −0.2% |
| rushingYards | −3.4% | −0.5% | +0.3% | +0.2% |
| **rushingTouchdowns** | **+2.4%** | **+5.1%** | **+6.0%** | −9.4% |
| receivingYards | −2.1% | −1.2% | +0.5% | +0.2% |
| receivingReceptions | −1.9% | −0.4% | −1.3% | −2.7% |
| receivingTouchdowns | +1.4% | +0.7% | −3.3% | −1.9% |

Negative is the blend winning. **`rushingTouchdowns` is the only defect**, it clears the
lab's pre-registered 2.0% bar against three of the four sources, and it survives all three
populations (`all`, `team_played`, `played`). Note the fourth column: the blend is 9.4%
*better* than BetOnline on the same stat, which points at BetOnline's crude
`anytimeTouchdown` split — 100% to rushing for QB/RB — already a known issue and now
priced.

The fantasy-point MAE for the same league-season is **−2.2%**, a clean win. It cannot see
any of the above, because yardage carries most of the variance. That is the whole argument
for measuring per stat.

**Amended 2026-08-27: the metric was wrong for the stat it flagged.**

MAE is minimised by the *median*, and an RB's weekly receiving-touchdown count is 0 in
about 95% of player-weeks — so a source projecting a flat 0.0 scores **better** on MAE
than one projecting the true expectation. BetOnline does exactly that: 988 of 995 RB
player-weeks carry `BOL_receivingTouchdowns == 0`, because `scrape_BOL.py:520-523` sends
100% of the anytime-TD market to rushing for every QB and RB. Pinnacle splits the same
market by *yardage* share (`scrape_pinnacle.py:373-381`), which errs the other way —
Christian McCaffrey at 56/44 against a realised ~79/21. The two cancel by accident, not
by design.

So `Scripts/lab/accuracy.py` now reports **calibration** — total projected over total
realised — beside MAE for the four sparse counts. It tells a different and more useful
story:

| stat | QB | RB | WR | TE |
|---|---|---|---|---|
| rushingTouchdowns | 0.98 | **1.10** | 0.39 | — |
| receivingTouchdowns | — | **0.60** | 1.16 | 0.93 |
| passingTouchdowns | 0.94 | — | — | — |
| passingInterceptions | 0.92 | — | — | — |

**`receivingTouchdowns` at RB is 0.60 — the largest error in the table, and MAE scored
it as +0.7% against ESPN, i.e. fine.** The `rushingTouchdowns` defect this plan led with
is real but is the *smaller* half of one mis-split market.

**And the fix is measured but not landed**, because it is source construction and the
owner's sequencing puts bias adjustment after the stat lines. Splitting each book's
anytime total by the **ESPN+FantasyPros TD ratio** — both project the two types
separately, so the columns are already on the frame — cuts RB share MAE from 0.202 to
**0.113** and moves calibration from 0.597 to **0.891** on receiving and 1.124 to
**1.046** on rushing. Red-zone opportunity share is comparable (0.125 over ten seasons)
and needs a new join plus a fitted shrinkage, so the consensus ratio is the cheaper of
the two.

Three things to know before landing it. It is **weekly-only** — BetOnline's *season*
endpoint publishes both TD types separately (Jahmyr Gibbs 12.5 rushing / 3.74
receiving), so no draft board is affected. It is worth **zero points directly**: all
nine leagues score both types at 6. And it makes weekly per-stat *MAE slightly worse*
(rushing −0.9%, receiving +2.3%, net zero) for exactly the reason above, so it must be
judged on calibration or it will look like a regression.

### F3 · What persists, and what the model shrinks, disagree everywhere

`python -m Scripts.lab.persistence`, 13,288 consecutive player-season pairs, 2016–2025.

**Volume persists. Efficiency barely does.**

| quantity | n | Pearson | low tercile | mid | high |
|---|---|---|---|---|---|
| carries/g | 2,407 | **+0.895** | 0.084 | 0.610 | 0.759 |
| targets/g | 2,747 | +0.820 | 0.360 | 0.288 | 0.652 |
| receptions/g | 2,747 | +0.787 | 0.358 | 0.277 | 0.619 |
| pass attempts/g | 348 | +0.540 | 0.266 | 0.373 | **0.093** |

| rate | n | Pearson | med. denom | ceiling *k* | shipped *k* |
|---|---|---|---|---|---|
| catch_rate | 1,383 | +0.559 | 68 | 54 | 40 |
| yards_per_target | 1,383 | +0.399 | 68 | 103 | 40 |
| yards_per_carry | 743 | +0.397 | 102 | 155 | 60 |
| yards_per_attempt | 377 | +0.394 | 401 | 618 | 150 |
| pass_td_per_attempt | 377 | +0.276 | 401 | 1054 | 300 |
| rush_td_per_carry | 743 | +0.260 | 102 | 290 | 150 |
| int_per_attempt | 377 | +0.223 | 401 | 1394 | 300 |
| rec_td_per_target | 1,383 | +0.189 | 68 | 292 | 120 |

`ceiling_k` inverts the credibility identity `n/(n+k) = r` at each rate's median
denominator.

**I first called it a floor and drew the opposite conclusion from it. That was wrong,
and the experiment that tested it is what says so.** Writing year *i*'s observed rate as
`y_i = theta_i + e_i`, the observed correlation is `rho * n/(n + k_opt)` where `k_opt` is
the constant that actually minimises error. Solving `n/(n+k) = r` therefore gives
`k_implied >= k_opt`, with equality only if the true rate is perfectly stable. So it is a
**ceiling**: real drift depresses `r` and *inflates* the number, and a shipped constant
sitting below it is where a calibrated one belongs.

Three experiments put it through the walk-forward under the lab's pre-committed rule,
and **all three were rejected**:

| experiment | mean Spearman | yardage MAE | TD/INT MAE |
|---|---|---|---|
| `shrinkage_at_floor` — every rate at the ceiling | **−0.0018** | +0.48% to +1.23% | −0.15% to −3.41% |
| `shrinkage_touchdowns_at_floor` — TD/INT only | **−0.0009** | 0.00% | −0.15% to −3.41% |
| `shrinkage_double` — 2× shipped, a midpoint | **−0.0012** | +0.35% to +0.55% | −0.22% to −1.93% |

The damage is monotone in the amount of shrinkage and worst at quarterback (−0.0027 even
in the touchdowns-only variant). Interception MAE does improve by 3.4% — the one place
the argument had force — and it does not pay for the ordering.

**So nothing shipped, and the table's value is ranking rather than calibration.**
Touchdown rates persist at +0.189 to +0.276 against +0.895 for carries per game whatever
constant prices them, and that is the fact behind F2.

Two readings worth keeping:

- **The touchdown rates are the least forecastable things measured** (+0.189 to +0.276).
  That is the mechanism behind F2: four sources extrapolating a quantity that is three
  quarters noise, averaged, is a confident wrong number. Averaging helps where sources
  carry independent signal, and there is very little signal here to be independent about.
- **A quarterback's attempt rate is nearly unpredictable among established starters**
  — +0.093 in the top tercile against +0.540 pooled. The pooled figure is mostly
  separating starters from backups, exactly as plan 18 found for games played (+0.663
  pooled, +0.343 among players who managed 8+).

### F4 · The weekly path patched points with a scalar that had no stat behind it

```python
final['adjustment'] = final['projPoints'] - final['ESPN_Points']
for i in ['ESPN', 'FP', 'MEAN', 'PINNY', 'BOL', 'TRUE']:
    final[f'{i}_Points'] = final['adjustment'] + final[f'{i}_Points']
```

ESPN's unpriced residual, added to **every** source's total including `TRUE_Points`. It
broke `TRUE_Points == score(TRUE_ stat line)`, and it injected an unweighted, full-strength
sixth ESPN vote *after* `compute_weighted_stats` had renormalised the imputed cells out.

Size, over the nine 2025 stores: mean |adjustment| 0.08–0.47 points a row, max 8.25, and
in `john_pc_league` **11.7% of rows over one point**.

**It was camouflage.** Two genuine stat-level defects were underneath it and neither was
visible until it came off:

**(a) ESPN's doubled yardage was escaping its correction.** The rule was
`ESPN > FP * 1.75 and ESPN > 40`, evaluated *before* the FantasyPros imputation — so for
any player FantasyPros had no line for it compared against NaN, returned False, and the
doubled value went to the store. FantasyPros served sixty players in 2025 behind a
registration fence. Deebo Samuel's week 6 reached the store at **136.3 receiving yards**
against an ESPN `projPoints` consistent with 68, and `TRUE_receivingYards` came out 83.2.

**(b) Six scored rules in `john_pc_league` are mapped, present and identically zero.**
`passingYards300to399Game`, `passingYards400PlusGame`, `rushingYards100-199Game`,
`rushingYards200+Game`, `receivingYards100-199Game`, `receivingYards200+Game` — worth 1 to
5 points each, and **0.0 for all 3,095 player-weeks, in the actuals as well as the
projections**. The name this pipeline reads is not the name ESPN's breakdown uses.
[Plan 01](01-scoring-coverage.md) catches an *unmapped* rule; this one is mapped, so
nothing reported it. It explains the position split in that league's flagged rows — RB
179, WR 108, QB 74 — precisely the three the yardage bonuses apply to.

### F5 · A QB interval excused by an abstention that no longer exists

`docs/STATE_OF_THE_REPO.md:420-423` records `passingYards` predictive coverage at **60.8%
against a nominal 80%** and excuses it: *"which is the quarterback arm the model already
abstains on, so it never reaches a board."*

`Scripts/usage/season.py:409` is `ABSTAIN_POSITIONS = ()`. The abstention was lifted on
2026-08-07 when the depth chart entered the veteran arm — the same document says so
seventy lines further down. Verified on `Data/Store/2026/winfield_football/board.parquet`:
Josh Allen, Jayden Daniels, Drake Maye and every other top quarterback carry `pts_p10`,
`pts_p50` and `pts_p90` today.

**Resolved 2026-08-27: withdrawn, not widened, because the family has the skew
inverted.** Re-measured on a fresh walk-forward it is **58.9%** against a nominal 80%,
missing asymmetrically -- 24.5% below p10 against 16.6% above p90.

The question the note left open was refit or withdraw. Measured, refitting cannot work.
Realised quarterback season passing yards, as a ratio of a 3,000+ prior season over 183
pairs:

| p5 | p10 | p25 | p50 | p75 | p90 | p95 |
|---|---|---|---|---|---|---|
| 0.23 | 0.43 | 0.70 | 0.90 | 1.03 | 1.17 | 1.23 |

A long left tail -- 13.1% below half, 6.6% below a quarter, which is losing the job or
getting hurt -- against a compressed upper end. `(p90-p50)/(p50-p10)` is **0.57**, i.e.
left-skewed. A Gamma matched to that p10 gives **1.57**, right-skewed: its p90 comes out
1.68 against 1.17 (**44% too high**), and matched to the p90 instead its p10 comes out
0.84 against 0.43. **There is no dispersion that makes a right-skewed family fit a
left-skewed outcome**, so widening it would buy coverage by making the ceiling absurd.

So `predictive.UNCALIBRATED` withdraws the published `USG_passingYards_low`/`_high` at
quarterback, on the principle `stat_intervals` already applies to a stat with no fitted
dispersion: partial coverage is visible, an invented number is not.

**The points interval cannot be withdrawn the same way and is flagged instead.**
`pts_p10`/`pts_p90` come from a copula over all eight stats, and passing yards are most
of a quarterback's points -- dropping the marginal would leave quarterbacks with no
interval at all. `outcome_evidence` now reads *"simulated; QB floor not calibrated"* for
them, and the board's column help says to read `p10` at quarterback as indicative. The
asymmetry is the useful part: the miss is on the **floor**, not the ceiling.

**What would fix it** is a family that can hold a compressed upper tail and a long lower
one, or the room-level draw in `Scripts/outcomes/simulate.py` -- since the left tail *is*
the quarterback losing the job, which is a thing that machinery already models and
[plan 28](28-outcome-distributions.md) has off by default. Not a wider Gamma.

### F6 · The weekly grain has no model, and carried a null column shaped like one

`Scripts/usage/weekly.py` does not exist; [plan 19](19-weekly-usage-model.md) is not
started. `USG_Points` was nonetheless on the 2025 weekly store, **null for 3,602 of 3,602
rows** — `proj_to_score`'s default prefix list is the *season* one, so `_apply_scoring`
found no `USG_` stat columns, `scored_any` was False everywhere, and it wrote an all-NaN
column. A source-shaped column that never has an opinion reads as a source that agreed.

### F7 · One weight table for forty-five stats

`WEIGHTS` is a single `'default'` dict. Defensible as a starting position and the
docstring's reasoning is sound. F2 and F3 together are why it cannot stay one — but see
*Deferred* below: this is a bias question, and bias comes after the stat lines.

---

## Fix

### Built

**1 · `Scripts/lab/accuracy.py` — the per-stat scoreboard.** Scores every source and the
blend against realised stat lines, per stat × position × population, on non-imputed cells,
with the fantasy-point MAE reported beside as the secondary number it is. Reuses
`Scripts/usage/evalset.py`, extended to carry `TRUE_` (`DERIVED`) so the blend is scorable
at all. Comparisons are **paired** — each source against the blend on the rows that source
was real for — because the blend is dense and its inputs are 8–13% covered, and pooling
would measure coverage and report it as accuracy. The decision rule is the lab's own
`MAX_STAT_MAE_INCREASE_PCT`, applied mechanically.

Pooling was checked, not assumed: actuals and ESPN agree to **0.0000** across all nine
leagues, and `TRUE_` disagrees on 13 player-weeks by up to 3.11 receiving yards — reported
as `worst_blend_disagreement`. The cause is upstream, in `clean_lineups` merging on
`(week, player_name, primaryPosition, player_active_status)`.

**2 · `Scripts/lab/persistence.py` — what is forecastable.** Reuses
`features.season_totals` rather than aggregating its own seasons, so the comparison against
`SHRINKAGE_K` means something. Pairs are joined on `season + 1`, never shifted, so a missed
season contributes no pair across the gap.

**3 · Volume in the blend.** `VOLUME_STATS` in `Scripts/scrape_player_stats.py` (the module
that owns ESPN's stat vocabulary), appended to the weekly extraction so ESPN is the root
source for volume the way it is for everything else — without that the imputation chain has
nothing to fill `FP_passingAttempts` from, and a NaN with no provenance flag becomes a
0.0 vote. `blended_stats()` in `projection_utils.py` is the one definition both grains use,
and it also drops plan 01's NaN `colName` rather than blending a `TRUE_nan` column.

**Verified additive.** Replaying blend-and-score over all nine stored 2026 boards, with and
without the volume columns: **max |Δ| = 0.0 on every `<prefix>_Points` column, over 10,710
rows.** `_apply_scoring` iterates the scoring table, not the stat list, so an unscored
`TRUE_` column cannot reach a total — pinned in `tests/test_per_slot_scoring.py`.

**4 · `report_line_coherence` on the board build.** Print-only. Measured on the 2026
boards: yards per attempt median 7.04 with 3 of 35 quarterbacks outside 6.0–8.5 (Kirk
Cousins at 13.62 on 116 attempts is the clearest), yards per carry median 4.11 with 0 of 67
outside, catch rate median 0.68 with 5 of 165 outside, and **team plays per game 59.1–65.0
across all 32 teams**. The team budget is a check that currently *passes*, which is itself
worth knowing: the drift plan 31 measured is in the allocation between teammates, not in
the total.

Two things it got wrong on the first run and now pins: a scrambling quarterback is not an
incoherent running back (QB is out of the carry band), and `pro_team` is the **string**
`"None"` for 212 unrostered players, so a `notna()` filter leaves a 33rd team at 0.5 plays
a game.

**5 · The weekly points patch removed.** The residual stays on the frame as
`espn_unpriced`, added to nothing. `TRUE_Points == score(TRUE_ stat line)` again.

**6 · The doubling correction rewritten to need no second source.** ESPN publishes a stat
line *and* a point total; when they disagree the disagreement is ESPN's own and the total
is the authoritative half. `halve_doubled_espn_yardage` halves only where doing so brings
the two materially closer. It fires on 3–8 rows per league-season — the honest size of the
bug, previously invisible.

**7 · `report_silent_zero_stats`.** A scored, mapped, present stat that is identically zero
all season is now a loud warning instead of nothing.

**8 · The weekly prefix list is explicit.** `WEEKLY_PREFIXES` plus `present_prefixes`, so
`USG_Points` is not written null and the `_Points` / `_PosRank` loops agree with the scorer
about which sources exist.

### Deferred, deliberately

**Source bias adjustment and any weight movement.** The owner's direction: get individual
stat-line projections right first, then adjust sources for bias and re-compute. So the
rushing-touchdown defect is **recorded, not patched** — no TD shrinkage, no per-stat
weight, and no re-tune. F3 gives whoever picks it up the number that decides it, and
BetOnline's `anytimeTouchdown` split is the first place to look.

### Owed

- **The six milestone bonuses need a model, not a column name.** Recorded 2026-08-26 and
  **not to be acted on** — this is a note, not a queued task.

  The first reading of F4(b) was that the projection side is a naming problem: find the
  key ESPN's `projected_breakdown` uses and the number arrives. That is wrong about the
  half that matters. **A milestone bonus is a non-linear function of the stat line.**
  "3 points for a 100-199 yard rushing game" is not a rate on rushing yards, so it cannot
  be expressed as a column times a constant — which is precisely the constraint
  `proj_to_score` operates under, and `REPL_SCORING`'s own comment in
  `Scripts/scrape_player_stats.py` states it: *"proj_to_score can only multiply a stat
  column by a constant."* 1,400 rushing yards over 17 games buys a different number of
  100-yard games depending on how they are distributed, and a season total cannot say.

  So the projection side wants a **separate model over the player's per-game
  distribution**, counting how often he crosses the threshold: expected bonus games
  = Σ over the slate of P(stat in the band that week). The output is an expected count,
  which `proj_to_score` can then price linearly.

  **The repo already has this shape twice.** `Scripts/dst/model.py` calls it out
  explicitly — points allowed and yards allowed are *"step functions of a weekly
  quantity, so a season projection has to carry the weekly distribution and integrate
  the ladder over it"* — and emits expected **games in each tier**, summing to the slate.
  `PA_TIERS` and `YD_TIERS` are the same object as these six bonuses. The kicker model
  does the same with distance buckets. A yardage-milestone arm is that pattern applied to
  a third ladder, and its natural home is a **per-game** distribution, which is
  [plan 19](19-weekly-usage-model.md)'s grain rather than the season head's.

  **The mechanism, as the owner put it: variance and the probability of hitting the
  milestone each week.** Not a yardage total scaled by anything --

      E[bonus games] = sum over the slate of P(week's yardage lands in the band)

  and a probability needs a distribution, so this is a variance problem before it is a
  mean problem. Four constraints, every one of which this repo has already paid for
  somewhere else:

  1. **It is a ladder, not a threshold.** `rushingYards100-199Game` is worth 3 and
     `rushingYards200+Game` worth 5, so the quantity is `P(100 <= Y < 200)` and
     `P(Y >= 200)` -- expected *counts per band*, summing over the slate. Structurally
     identical to `PA_TIERS`.
  2. **The weekly variance cannot be derived from the season one.**
     `Scripts/usage/predictive.py` fits dispersion on **season totals**, and dividing by
     17 assumes independent weeks. That module already records what happens when parts
     are composed rather than fitted end to end: games and per-game volume correlate
     **+0.48 to +0.63**, and backing one variance out of another *produced negative
     numbers for quarterbacks*. A weekly arm needs its own fit on weekly residuals.
  3. **The tail shape is most of the answer.** `P(Y >= 100)` is dominated by the right
     tail, not by the variance alone, so the family matters more here than it does for an
     80% interval. `predictive.py` chose a Gamma for yardage on the skew of the
     *per-opportunity rate*; weekly yardage is a different and far more skewed object,
     and it has real mass at exactly zero -- the same fact that already forced a fix
     there, since a Gamma has none.
  4. **Availability compounds it.** The sum runs over games *played*, so it needs the
     Beta-Binomial games distribution beside the yardage one. Both already exist.

  **The size of getting this wrong is already measured, in this repo, on this pattern.**
  [Plan 13](13-dst-from-vegas-lines.md) evaluated a scoring ladder at the season mean
  instead of integrating it and found a **16.5-point compression** of the range the
  component exists to create; `E[f(X)]` beat `f(E[X])` on bias *and* RMSE, per league, on
  held-out data. Plan 01's FGY50 floor is named there as the same error class. A
  milestone bonus priced off a season mean would be the third instance.

  **BUILT 2026-08-27.** `Scripts/usage/milestones.py`, and the notes above turned out
  to be right about the mechanism and wrong about one number.

  `python -m Scripts.usage.milestones --fit --report`. Walk-forward 2019-2025,
  population totals with no selection:

  | band | realised | predicted | ratio | `f(E[X])` |
  |---|---|---|---|---|
  | passingYards300to399Game | 651 | 584.3 | 0.90 | 116 |
  | passingYards400PlusGame | 75 | 80.7 | 1.08 | 0 |
  | rushingYards100-199Game | 690 | 657.0 | 0.95 | 117 |
  | rushingYards200+Game | 18 | 16.9 | 0.94 | 0 |
  | receivingYards100-199Game | 1,262 | 1,192.4 | 0.94 | 160 |
  | receivingYards200+Game | 19 | 24.0 | 1.26 | 0 |

  **The number that was wrong.** The note above said the linear reading awards
  *exactly zero, always*. It recovers **13-18% of the first tier and none of the
  second**. The original figure came from a binned table read at each bin's lower
  edge rather than each player's own mean; `report` now computes the column instead
  of asserting it, and the docstrings that repeated the claim are corrected. The
  conclusion is unaffected -- 117 against 690 is still most of a scoring rule going
  unpriced -- but it was an assertion dressed as a measurement.

  **Two things had to be measured out of the way, and a pooled metric hid both.**

  1. **The Gamma tail understates the top tiers by 20-25x** -- 1.0 predicted rushing
     games over 200 yards against 18 realised. An exponential tail is the wrong shape
     that far out, and a 20x multiplier is not a calibration. Above the first step the
     ladder is climbed by a *counted* conditional rate instead:
     `P(>= 200) = P(>= 100) x P(>= 200 | >= 100)`, the second factor counted off the
     training weeks (0.0257 for rushing, 0.0175 for receiving, 0.1090 for passing).
     That took the 200+ tier from a ratio of 0.05 to 0.94.
  2. **The fitted zero point mass is misspecified, and the population total could not
     see it.** With `bust` carried through, the totals calibrated at 1.02 and 1.00
     while the *top of the range* -- the only place bonus points exist -- over-predicted
     by **1.7x**, because the errors cancel against ten thousand players for whom every
     method gives zero. Per per-game-mean bin against ten seasons of WR receiving:

     | per-game mean | empirical | with bust | without |
     |---|---|---|---|
     | 65-80 | 0.215 | 0.224 | **0.205** |
     | 80-90 | 0.354 | 0.470 | **0.314** |
     | 90-100 | 0.412 | 0.700 | **0.384** |
     | 100-200 | 0.510 | 0.583 | **0.489** |

     With the mass the curve is also **non-monotone** -- a receiver projected for 110
     yards a game got a *lower* probability than one projected for 90. Shipped, the
     model tracks the measurement at 0.87-1.07 across the whole range for both stats,
     and monotonically.

  **That non-monotonicity is a bug in shipped code, not only in this model.** Past
  `CV^2 = s/(1-s)` the zero-inflated mixture in
  `Scripts.usage.predictive._reparameterise` has a **negative** conditional variance;
  it was clipped to epsilon, which collapsed the Gamma to a point mass. So
  `P(>= 100 | mu = 110)` read 0.820 where the plain Gamma gives 0.522 and ten seasons
  of football give 0.510. The guard now drops the zero mass where the mixture is
  infeasible -- the limiting case of the same family, needing no tuning constant.
  Measured on the shipped 2026 boards, **81 of 3,508 projected yardage cells (2.31%)
  carry a zero-width interval today**: Puka Nacua's receiving yards go from a p10 and
  p90 both equal to 1,631 to a 1,128-yard interval around it. This is the residual
  [plan 28](28-outcome-distributions.md) could not reach when it took the same symptom
  from 14.0% to 0.5%, because the cause is the mixture's feasibility rather than the
  basis that plan corrected.

  **How it is wired.** Bands are **derived, not blended** -- `DERIVED_STATS` in
  `Scripts/scrape_player_stats.py`, excluded by `blended_stats`, computed after
  reconciliation by `attach_milestone_bands`. A non-linear function of a line has
  nothing to average, and ESPN's own six columns are identically zero, so blending
  them would have halved every band. Derived for **every prefix** from that prefix's
  own yardage rather than for `TRUE_` alone, so `points_delta` against ESPN does not
  show a fabricated six-to-twenty-point disagreement.

  Effect, replaying blend-and-score over the stored 2026 boards: `john_pc_league`
  +0.73 points mean and **+17.6 max**, with within-position rank moving for 17% of
  top-40 players and the largest move 2 places; `john_atl_league` +0.22 mean, +5.9
  max, 5% moved. The seven leagues that score no milestones are **bit-identical**,
  max delta 0.0000.

  Residual, stated: a consistent 5-8% under-prediction at the very top, which is the
  expected sign -- the dispersion is fitted against a player's *realised* per-game
  mean, so it has never seen the extra spread a projected mean carries. And
  `passingYards300to399Game` sits at 0.90; the 400+ tier above it is 1.08, so the
  understatement is in the Gamma's P(>= 300) for quarterbacks rather than in the
  ladder.

  Two halves, and only one of them is modelling. The **actual** columns are also zero for
  all 3,095 player-weeks, and a realised 100-yard game is a fact rather than an
  expectation — so that half probably *is* a naming problem, and it is what a backtest of
  any such model would be scored against. Until either is done that league's points are
  short by a median 0.48 a row, and now say so out loud.
- **The efficiency shrinkage constants** are all below their measured floor (F3). Refitting
  them moves `USG_` and therefore `TRUE_`.
- **`Scripts/usage/weekly.py`** — [plan 19](19-weekly-usage-model.md), unchanged in scope
  and now with a per-stat baseline to beat.

---

## Reproducing it

```bash
python -m Scripts.lab.accuracy         # F2, the per-stat scoreboard
python -m Scripts.lab.persistence      # F3, what is forecastable
python -m Scripts.refresh --all --what board   # F1 + the coherence diagnostic
pytest tests/test_lab_accuracy.py tests/test_lab_persistence.py \
       tests/test_line_coherence.py tests/test_weekly_points_identity.py
```
