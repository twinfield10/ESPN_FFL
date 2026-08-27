# 34 — Stat-first, audited: where points leaked back into a stat-line pipeline

**Status:** IN PROGRESS

**Priority:** High · **Effort:** M · **Where it stands:** **Phases 1–3 built 2026-08-26.**
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

### F3 · What persists, and what the model shrinks, disagree everywhere

`python -m Scripts.lab.persistence`, 13,288 consecutive player-season pairs, 2016–2025.

**Volume persists. Efficiency barely does.**

| quantity | n | Pearson | low tercile | mid | high |
|---|---|---|---|---|---|
| carries/g | 2,407 | **+0.895** | 0.084 | 0.610 | 0.759 |
| targets/g | 2,747 | +0.820 | 0.360 | 0.288 | 0.652 |
| receptions/g | 2,747 | +0.787 | 0.358 | 0.277 | 0.619 |
| pass attempts/g | 348 | +0.540 | 0.266 | 0.373 | **0.093** |

| rate | n | Pearson | med. denom | implied *k* | shipped *k* |
|---|---|---|---|---|---|
| catch_rate | 1,383 | +0.559 | 68 | 54 | 40 |
| yards_per_target | 1,383 | +0.399 | 68 | 103 | 40 |
| yards_per_carry | 743 | +0.397 | 102 | 155 | 60 |
| yards_per_attempt | 377 | +0.394 | 401 | 618 | 150 |
| pass_td_per_attempt | 377 | +0.276 | 401 | 1054 | 300 |
| rush_td_per_carry | 743 | +0.260 | 102 | 290 | 150 |
| int_per_attempt | 377 | +0.223 | 401 | 1394 | 300 |
| rec_td_per_target | 1,383 | +0.189 | 68 | 292 | 120 |

`implied_k` inverts the credibility identity `n/(n+k) = r` at the median denominator, and
is a **floor**: it assumes a perfectly stable underlying rate, so genuine drift only raises
it. **Every shipped constant sits below its floor**, by 1.4× (catch rate) to 4.6× (yards
per attempt, interceptions per attempt) — the model shrinks efficiency too little, across
the board.

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
