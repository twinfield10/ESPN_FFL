# 27 — Injuries as duration, recovery and recurrence

**Priority:** High (seasonal, and it compounds) · **Effort:** L · **Status:**
**Phases 1–4 done 2026-08-18. The curve and the hazard are fitted, walk-forwarded, and
both REJECTED by their own pre-committed gates.** So the outcome is the one this plan named
in advance: **ship the columns, multiply nothing.** The daily ESPN injury archive is running and
`Scripts/injury/{lexicon,episodes}.py` build the episode table — 3,056 episodes across
2016–2025, 1,555 ending in a return, **580 of them absences of four games or more where
the injury report alone finds 99**. Same-body-part recurrence for hamstrings comes out at
**9.9% against a published 11.9%**, which is the external check that the episode logic is
sound. Phase 3 resolves a live severity through a six-rung precedence ladder and puts **Body
Part** and **Wks Out** on all nine boards as diagnostics — `TRUE_` and every source column
are untouched, and a test pins that. The fitted curve is well calibrated — a cell predicted to lose 20%
loses 20%, slope **1.05** — and its accuracy gain is about **1%**, half the pre-committed
2% bar, on either metric. Nothing multiplies a projection.

**Depends on:** [16](16-usage-data-layer.md) — the context readers and the leakage
discipline · [18](18-season-usage-model.md) — `to_full_slate`'s availability seam
**Feeds:** [19](19-weekly-usage-model.md) — weekly channel A is that plan's job, not
this one's

## Problem

The pipeline models an injury as one thing. `_apply_injury_adjustment`
(`Scripts/season_projections.py`:758) multiplies `USG_` by
`games_available(return_date, week_one) / 17` and nulls it for a season-ender.
`clean_lineups` (`Scripts/projection_utils.py`:849) has no injury layer at all. And
`ctx.load_injuries` — ten seasons of weekly designations, body parts and practice
participation, already on disk — **had no caller outside its own tests.**

Three different questions are collapsed into that one number:

| | Channel | Quantity | Exists? |
|---|---|---|---|
| A | Availability | expected games missed | partly — `USG_` only |
| B | Return to form | efficiency once he is back | **no** |
| C | Recurrence | probability he goes again | **no** |

The motivating case: Jeremiyah Love, a high ankle sprain that ESPN's feed reports as
status `Active` with no return date and no injury type, the severity visible only as
"(ankle)" in a news comment.

## Evidence

Measured on this repo's own data, 2016–2025, skill positions. Episodes are built from the
union described under *Fix*; each player's own mean over his last four appearances before
the injury is the baseline; appearances 1–6 after the return are the ratios.

### The channel is efficiency, not volume

Median snap-share ratio to baseline is **0.99–1.02 in every appearance after a return.**
Teams put a returning player straight back on the field. So the loss is per-snap
efficiency, and the volume and opportunity layers structurally cannot be pricing it —
which is the whole architectural argument for a `TRUE_`-level multiplier not double
counting anything.

### The placebo is not 1.0, and this is the finding that matters most

Healthy players passing the **identical** baseline filter score:

| appearance | 1 | 2 | 3 | 4 | 5 | 6 |
|---|---|---|---|---|---|---|
| control mean ratio | 0.980 | 0.969 | 0.970 | 0.969 | 0.962 | 0.955 |

Flat, and below 1.0. A four-game mean is a selected high point and weekly scoring is
right-skewed, so "below your own recent average" is the *normal* condition of a healthy
player. n = 42,660 control appearances.

Fitting against 1.0 rather than against this would attribute all of it to injury and
haircut every returning player by ~16% for reasons that have nothing to do with his
ankle. Every net figure below is a ratio to the matched control.

**The materiality floor is part of this.** Before `MIN_BASELINE_POINTS` went in, a
baseline of 0.02 points produced a ratio in the trillions and a handful of deep-bench
player-weeks dominated every mean in the table — the control appearance-1 mean came out
at 1.5e13. The floor is applied to both cohorts through one expression called twice,
because the injured curve only means anything divided by the control curve and that
division is only valid if both sides were selected the same way.

### The deconfounded effect, by body part

Net multiplier = injured mean / control mean at the same appearance index. Returned
episodes only.

| body part | n | mean wks out | recurrence | a1 | a2 | a3 | a4 | a5 | a6 |
|---|---|---|---|---|---|---|---|---|---|
| other (undisclosed) | 243 | 6.64 | n/a | 0.89 | 0.84 | 1.12 | 0.90 | 0.88 | 1.05 |
| hamstring | 223 | 3.02 | **9.9%** | 0.87 | 1.02 | 1.03 | 1.07 | 1.02 | 1.39 |
| knee | 207 | 3.71 | 7.2% | 0.92 | 0.84 | 0.81 | 0.99 | 0.88 | 1.01 |
| concussion | 197 | 1.75 | 4.1% | 0.93 | 1.03 | 1.33 | 1.23 | 0.96 | 1.02 |
| ankle | 184 | 2.87 | 8.2% | 0.91 | 0.91 | 1.00 | 0.94 | 0.89 | 0.96 |
| soft tissue lower | 157 | 3.31 | 5.7% | 0.95 | 1.07 | 1.00 | 1.17 | 1.06 | 0.92 |
| shoulder | 88 | 2.97 | 4.5% | 0.77 | 0.84 | 1.03 | 0.99 | 1.26 | 1.00 |
| foot / toe | 73 | 3.29 | 6.8% | **0.63** | 0.88 | 1.03 | 0.99 | 0.69 | 0.87 |
| back / core | 60 | 3.52 | 0.0% | 0.93 | 1.03 | 1.46 | 0.92 | 1.00 | 1.16 |
| hand / wrist / arm | 49 | 3.86 | 4.1% | **0.63** | 0.82 | 1.20 | 0.89 | 0.82 | 1.19 |
| ribs / chest | 47 | 2.98 | 0.0% | 0.95 | 1.20 | 0.76 | 1.54 | 1.46 | 1.03 |
| illness | 27 | 4.04 | 0.0% | 1.39 | 0.62 | 0.60 | 0.45 | 1.54 | 1.45 |

Pooled across body parts: **a1 0.876, a2 0.947, a3 1.04.** Roughly −12% in the first game
back, −5% in the second, gone by the third.

**Three readings, and the third is a design constraint.**

*The hypothesised ladder is too steep and too long.* A 0.75 / 0.75 / 0.85 / 0.92 ramp is
about three times the measured depth and twice the measured duration.

*There are two cost channels and they are not the same injuries.* Hamstrings and
concussions show no lasting efficiency cost at all — a hamstring is back to 1.02 by the
second game — but hamstrings recur at 9.9%. Feet and hands cost 35%+ in the first game
back. A model priced on the ramp alone calls a hamstring cheap; one priced on
availability alone calls both free.

*The later columns are noise, and the fit must not chase them.* Hamstring a6 = 1.39,
illness a4 = 0.45, ribs a4 = 1.54, all on cells of 16–58. Per-cell empirical means
cannot be shipped. The curve has to be monotone and parametric with the cell count
shrinking it toward its parent, or the model will confidently predict that hamstrings
overperform in the sixth game back.

### Duration stands in for severity, because severity does not exist

`report_primary_injury` says `"Ankle"` and never `"high ankle"` — the field is a body
part, for all ten seasons. ESPN's structured feed does carry real diagnoses
(`"Knee - ACL"`, `"Knee - ACL + MCL"`, `injury_detail` of `"Surgery"`), but for **114 of
800 records** on the 2026-08-18 pull, `"Undisclosed"` is common, and Love is not one of
the 114. Observed duration is the only severity signal available historically, and it
works: appearance-1 multipliers split 0.95 / 0.75 / 0.66 across one, two and
three-or-more weeks missed.

**And until 2026-08-18 the one real severity source was being destroyed nightly.**
`scrape_espn_injuries.write()` overwrote a single file per season, so ESPN's vocabulary
survived exactly as long as it took the next pull to replace it. There is no backfill.
That is why the archive shipped first: it is the only step in this plan whose value
strictly decreases with delay.

### The injury report truncates long absences, and reserve status recovers them

The report goes quiet once a player lands on reserve. On its own it yields **99** returned
absences of four games or more across ten seasons, which would suggest long injuries
barely happen. Adding roster reserve status brings that to **580**.

### The reserve allowlist is derived, and deriving it caught a real contaminant

Every generic "is he missing?" rule admits `RES`/`R59` and `RES`/`R62`: both are 100%
absent, both look exactly like injured reserve on every behavioural column. **`R59`
appears only in 2020 and 2021, and `R62` only in 2020** — they are the COVID-19 reserve
lists. Left in, they would have contributed ~456 fabricated episodes concentrated in two
seasons.

`R48` is the other one worth naming. It is 100% absent like `R01`, but **32.3% of its
player-weeks also carry an injury-report row** against `R01`'s 3.8%, and only 54% of
those say `Out` against `R01`'s 90.3%. It is designated-for-return: the player is
practising again. Pooling it with `R01` would mix the shelved with the nearly-recovered,
which is precisely the distinction a recovery curve is trying to measure. It is admitted
as an absence and flagged separately.

The full cross-tab — every `(status, code)` with its absence rate, report corroboration,
seasons present, verdict and the reason — is persisted in `Data/NFL/injury_meta.json`, so
the allowlist travels with its evidence rather than as a literal in the source.

## Fix

### Three absence signals, unioned, because each alone is wrong

1. `report_status == "Out"` on the weekly injury report;
2. an allowlisted roster reserve code;
3. absent from the box score while on a roster whose team played.

Signal 3 is **never** an episode opener — it catches healthy scratches and buried backups
as readily as injuries — so it counts only inside a run that signal 1 or 2 has vouched
for. `strong_share` records how thinly a run is corroborated and is reported rather than
acted on: one early `Out` vouching for eight quiet weeks may be an injury that moved to
reserve, or one bad week and a benching, and the honest move is to say which and let the
fit decide. 152 of 1,555 returned episodes fall below 0.5.

### Byes bridge, and the clock counts appearances

The grid is built from the weeks a team **actually played** — the same primitive
`ctx.team_games` counts, because rosters' distinct weeks include the bye and counting it
produced an availability of 17/16. A run of absence is consecutive over the player's own
sequence of gamedays, so a bye neither ends an absence nor inflates `weeks_out`, and a
mid-season trade does not split one. Post-return rows are indexed over games played, so a
bye after a return shortens the history instead of reading as a week of recovery that
never happened.

### Censoring has three kinds and they must not be conflated

| kind | n | treatment |
|---|---|---|
| `returned` | 1,555 | the only episodes the recovery curve can see |
| `season_end` | 1,437 | a genuine lower bound on duration; excluded from the curve |
| `off_roster` | 64 | not an observation of anything; excluded from both |

Treating the second as the first is how you conclude that knee injuries end careers.

### Artifacts

`Data/NFL/injury_{episodes,post_return,controls}.parquet` and `injury_meta.json`.
Deliberately **not** season-scoped — pooling across seasons is the entire point, since a
body part yields 40–220 episodes over ten years and single digits within one. Deliberately
**not** in `store.ARTIFACTS` — the store is league-scoped and `artifact_path` takes a
`league_key`, and an injury to a running back is the same injury in all nine leagues.
`s3_store.MIRROR_TIERS["NFL"]` already publishes them at `nfl/injury_*.parquet`, and
`mirror_key` maps the injury snapshots to
`injuries/season=2026/snapshots/date=<stamp>/` with no new plumbing, because `_hive`
converts bare four-digit years and leaves `date=` alone.

`injury_meta.json` carries the reserve-code evidence, per-season episode counts, unmapped
body parts and a **single-season concentration warning** at >25% of a body part's
episodes. It fired on the first build: illness, 2020, 25.9% — which is the COVID era
showing up in the one place the code allowlist could not remove it. A body part whose
episodes pile up in one season is not a body part, it is a season.

### What is left, and what should be cut

| Phase | What | Status |
|---|---|---|
| 1 | dated ESPN snapshots + `pulled_at`; `status_description_abbr`; pin scipy | **Done** |
| 2 | `episodes.py`, the artifacts, the descriptive report | **Done** |
| 3 | `severity.py` + the override file + board columns | **Done** |
| 4 | **Done, and REJECTED.** Curve fitted (global a=0.163, tau=1.14) and walk-forwarded; calibration slope 1.05, accuracy gain ~1% against a 2% bar. Hazard Brier 0.9898 against a 0.98 bar |
| 5 | channel B weekly — **columns only**, per the rejection | Diagnostics shippable |
| 6 | channel B on the season `TRUE_` | **Do not build** — gate rejected |
| 7 | channel C into channel A | **Do not build** — hazard gate rejected |

**Position × body-part multipliers are cut.** 4–152 episodes per cell, most under 40.
Position ships as a reported split with `n` visible; a shrunk cell equal to its parent is
false precision wearing a column header.

**Expect phase 6 to fail its own materiality gate.** The ramp costs ≈0.20 of a game in
total, so over a ten-game remainder the season multiplier is ≈0.98 — 3–6 points on a
300-point season, inside the 8.5% median source spread the board already displays. Build
the code path so the number exists, then decide on the number. The week-to-week surface
is where a 12% first-game haircut is a real start/sit change.

**G-B0 must be answered before any curve is fitted.** Using the archived boards, test
whether `ESPN_Points` for a player in his first appearance back is *already* depressed
against his own pre-injury figure. If ESPN already ramps, channel B is double counting and
belongs on `USG_` only. "No source prices this" is a claim, not a fact.

**Weekly channel A is plan 19's job.** That plan has already specified and measured the
availability head — Out 100% deterministic, Doubtful 99.2%, Questionable splitting
57%/22% by practice status — and requires a calibrated `P(active)`. A second zeroing rule
here would be a duplicate implementation of a designed thing. **27 does channels B and C
weekly; 19 does channel A.**

### G-B0 answered: ESPN part-prices the ramp, flat, and under-prices the tail

The gate that had to clear before fitting anything. Using 2025's stored weekly lineups --
which carry ESPN's own `projPoints` alongside actual `points` -- for players in their first
three appearances back from an episode, against a healthy control passing the identical
baseline filter:

| appearance | ESPN projection, net of control | actual, net of control | ESPN prices | real drop |
|---|---|---|---|---|
| 1 | 0.920 | 0.860 | 8.0% | 14.0% |
| 2 | 0.926 | 0.802 | 7.4% | 19.8% |
| 3 | 0.911 | 0.788 | 8.9% | 21.2% |

**Three readings.**

*ESPN does mark returning players down* -- so "no source prices this" was wrong, and had the
gate not been run the multiplier would have double counted roughly half the effect.

*But it marks them down by a constant.* 0.920, 0.926, 0.911 is not a recovery curve, it is a
flat ~8% haircut on anyone recently injured. ESPN is not modelling recovery; it is applying a
penalty. That is why it under-prices appearances 2 and 3, where the real drop is 20%+.

*So channel B is viable on `TRUE_`, as a residual, and the residual is not flat.*
`actual / ESPN` gives **0.935, 0.866, 0.865** -- shallower than the raw net effect at
appearance 1 and roughly double it by appearance 3. Fitting the raw curve and applying it
over the top would be wrong in both directions.

**And the answer differs by surface.** This measurement is of ESPN's *weekly* projection. Its
*season* projection is published pre-season, when nobody is "recently returned", so there is
no ramp in it to net against: on the season path channel B is fully additive, on the weekly
path it must be a residual. One more reason the two paths do not share a multiplier.

**Caveat on sample size, stated because the table above overstates it.** The `n` in that
measurement counts league-player-weeks, and the same player appears in up to nine league
lineups. The effective sample is **108 distinct episodes across 88 players in one season**.
The direction is consistent across all three appearances and the flatness is unmistakable,
but the 8% is a point estimate on 108 episodes and the walk-forward has to re-derive it per
fold rather than treat it as a constant.

### Phase 4: the fit, and both gates rejecting it

`Scripts/injury/{model,backtest}.py`, gates in `Scripts/lab/registry.py`, 36 tests.

#### What was fitted

`m(w) = 1 - a·exp(-(w-1)/tau)` — two parameters, monotone and asymptotic to 1.0 by
construction. The global fit lands at **a = 0.163, tau = 1.14**, which is almost exactly
what this plan predicted before any code existed (`a ≈ 0.15, tau ≈ 1.0`).

**The estimator is a ratio of sums, not a mean of ratios.** 125 of 2,121 post-return
appearances score *exactly zero* — a player is on the field and does nothing — so no mean of
log-ratios exists, and dropping those rows would discard precisely the worst outcomes and
bias every curve upward. Summed points over summed expectation handles them, is the correct
estimator for a multiplicative factor, and puts the placebo correction in the denominator
where it belongs.

| cell | eps | a | a sd | tau | multiplier by appearance back |
|---|---|---|---|---|---|
| global | 452 | 0.163 | 0.032 | 1.14 | 0.84 0.93 0.97 0.99 1.00 1.00 |
| global · 1 game missed | 157 | 0.112 | 0.052 | 0.71 | 0.89 0.97 0.99 1.00 1.00 1.00 |
| global · 2 | 115 | 0.195 | 0.065 | 0.77 | 0.81 0.95 0.99 1.00 1.00 1.00 |
| global · 3–4 | 132 | 0.186 | 0.061 | 1.45 | 0.81 0.91 0.95 0.98 0.99 0.99 |
| global · 5+ | 48 | 0.169 | 0.067 | 3.68 | 0.83 0.87 0.90 0.93 0.94 0.96 |
| foot / toe | 31 | 0.245 | 0.071 | 1.91 | 0.76 0.86 0.91 0.95 0.97 0.98 |
| shoulder | 47 | 0.238 | 0.083 | 1.67 | 0.76 0.87 0.93 0.96 0.98 0.99 |
| knee | 56 | 0.188 | 0.075 | 3.87 | 0.81 0.85 0.89 0.91 0.93 0.95 |
| hamstring | 77 | 0.154 | 0.062 | 0.73 | 0.85 0.96 0.99 1.00 1.00 1.00 |
| ankle | 72 | 0.108 | 0.062 | 4.18 | **abstains** |
| concussion | 72 | 0.114 | 0.070 | 0.42 | **abstains** |
| soft tissue lower | 46 | 0.068 | 0.078 | 0.05 | **abstains** |

`tau` is where the two-parameter form earns its keep. Hamstring 0.73 against knee 3.87:
"deep then fast" against "shallow then slow", which one parameter cannot express and which
is the actual difference between those injuries. The duration ladder shows the same thing —
`tau` climbs 0.71 → 0.77 → 1.45 → 3.68 as the absence lengthens. A longer absence takes
longer to shake off.

**Three cells abstain, and they are the right three.** Concussion and lower-body soft tissue
are exactly where the raw measurement showed no lasting efficiency cost, and the fit reaches
that on its own: the shortfall sits inside two bootstrap standard errors, so the model
returns 1.0 and records why. Ankle is a knife-edge case (0.108 against 0.062) — an effect
that flips on the bootstrap has not been measured.

**Body part × duration is combined, not fitted.** Each dimension is well powered alone
(19–77 episodes per body part, 48–157 per bucket) and their joint cells are not — the
largest holds 45 and most hold 6–25. Fitting them directly meant every one fell back to a
single parent and one of the two signals was thrown away. So the joint cell is
`theta(part, bucket) = theta(part) + theta(global, bucket) - theta(global)`: the
interaction-free reading, which assumes no interaction because 25 episodes cannot test one.

#### The walk-forward, and why both gates reject

Folds on episodes (six appearances of one injury are one observation), with the control
cohort, the shrinkage strength and the abstention decisions all re-derived inside each fold.
Every metric is computed twice — **oracle** (conditioned on realised duration) and **blind**
(body part only, which is what the live system has) — and only the blind figure faces a gate,
because at apply time duration is predicted rather than observed.

| candidate | MAE gain | RMSE gain |
|---|---|---|
| the hypothesised 0.75/0.75/0.85/0.92 ladder | **+3.17%** | **+0.70%** |
| a single global curve, no cell structure | +2.29% | +1.21% |
| oracle (realised duration) | +2.02% | +1.17% |
| blind (body part only) | +1.58% | +1.11% |
| healthy comparables, discounted as if injured | **−0.53%** | **+0.61%** |

**The two metrics reverse the ordering, and that is the finding.** Under MAE the ranking is
monotone in *how hard each candidate discounts* — the hypothesised ladder, the most
aggressive, wins outright — and discounting healthy comparables *improves* their MAE.
Under RMSE the ladder is the **worst** candidate and healthy comparables correctly get
worse.

The reason is structural. The prediction is a conditional **mean** (a ratio of sums) and
weekly fantasy scoring is strongly right-skewed, so the conditional median sits well below
it. MAE is minimised by the median. So MAE rewards *any* downward bias whether or not it has
anything to do with injuries. RMSE is minimised by the mean, which is what the model
estimates, so it is the metric that can distinguish a correct multiplier from a merely
smaller one.

**The gate reads MAE, and it has been left alone rather than swapped after the fact.** It
rejects on the false-positive clause — the clause written precisely to catch "this helps
healthy players too". It fires for a slightly different reason than anticipated (a metric
artefact rather than mean reversion) but it fires on the right evidence.

**And re-specifying it would not change the outcome.** Under RMSE the fitted curve gains
**1.11%** against a 2% bar. The curve is directionally right and well calibrated —
**slope 1.05**, meaning a cell predicted to lose 20% loses 20%, comfortably above the 0.4
floor — but the accuracy improvement is half what was pre-committed as the minimum worth
acting on. That is a clean rejection, not a near miss dressed up.

**The hazard rejects too**, at a Brier ratio of **0.9898** against a 0.98 bar. The weekly
recurrence event is rare (~1.07% a week), so Brier is dominated by the base rate and there
is little room to beat a constant at any skill level. The *pooled per-body-part rate* is a
different and simpler quantity, it is judged separately, and it passes its own external
check: **hamstring 9.8% against a published 11.9%**, inside the pre-set [0.09, 0.15].

#### The recommendation

Ship the columns. The body part, the expected absence, the fitted ladder and the recurrence
probability are all useful next to a projection without touching `TRUE_`, and that is what
this plan said the failure case looks like. **Do not proceed with phases 6 and 7 as
multipliers.**

Two things would change the answer, and both are data rather than modelling. The archive
started on 2026-08-18: a season of daily ESPN snapshots gives real severity — "Knee - ACL"
against "Knee" — and the oracle-versus-blind gap (2.02% against 1.58% by MAE, 1.17% against
1.11% by RMSE) is the measured value of knowing severity better. And re-specifying the
accuracy gate on RMSE **before** the next run, with the reasoning above, is the right
correction to make in advance rather than in arrears.

### Resolving a live severity — the ladder, and what live data did to it

Six rungs, most trusted first: **override → ESPN structured diagnosis → ESPN return date →
news text → weekly report body part → abstain.** On Winfield Football's 2026 board that
resolves 132 of 1,027 players: 1 override, 29 return dates, 102 from free text, and **zero
abstentions**.

The ladder as designed was wrong in four ways, and every one of them was found by running
it against the actual feed rather than by reasoning about it.

**A diagnosis names an injury; a return date times it.** Structured diagnosis outranked the
return date at first, which is intuitive and wrong. ESPN had **Malik Nabers** at
`injury_type="Knee - ACL"`, `injury_detail="Surgery"`, status `Questionable` — and a
`returnDate` of 2026-08-15, four weeks *before* the opener, with a comment describing
11-on-11 reps in a non-contact jersey. The ACL is real and it is last season's.
Diagnosis-first put him **46 weeks out at ADP 36**; date-first has him available in week 1
with an ACL in his history. The label still comes from the diagnosis, so nothing is lost.

**Text is undated; ESPN's status is not.** The group priors are means over episodes that
cost at least one game, so they answer *given he is out with this body part, how long?* —
not *given he is mentioned with it*. **Puka Nacua** read as 3.5 expected weeks missed, at
ADP 4.4, from a note saying he had practised. Where ESPN still lists a player Active the
status now sets the duration (~0.5 games) and the text still supplies the label. Isiah
Pacheco's blurb — "dealing with a sprained MCL, but head coach Dan Campbell believes he
will be ready for the season opener" — is the same shape: a real diagnosis describing a
player who will not miss a game. Season-ending language in the text still overrides a stale
Active status, because that is newer information than a status field that has not caught up.

**A beat report often describes somebody else's injury.** **Tyler Allgeier**'s comment
reads: "Allgeier could open the regular season as the Cardinals' primary running back, as
Adam Schefter reports that *Jeremiyah Love* sustained a high-ankle sprain." That tagged
**Allgeier** with Love's high ankle sprain — on the same board where Love carries it
correctly from an override. A mention now has to be attributable: anchored on the player's
own surname through the parenthetical convention ("Metcalf (undisclosed) will be…"), or
within 60 characters behind his surname. Beyond that the extractor abstains rather than
guessing whose knee it is. This is the single largest precision risk in text extraction and
it is invisible in aggregate — the counts look identical either way.

**Say the word the writer used.** Two label bugs reached board cells: the raw regex
`high[\s-]*ankle` (the detail was the *pattern*, not the match), and "multi week" in a
column headed Body Part — Emeka Egbuka's "(toe) is day-to-day, week-to-week". And the
catch-all group rendered as "other" where the writer had written "leg" or "undisclosed",
which tells a drafter strictly less.

### The override file

`config/injuries/<season>.yaml`, hand-edited and **tracked in git** — the only injury
artifact that is. It started under `Data/` and moved, because everything there is untracked
on the principle that S3 is the system of record for *data*, and this is not data. It is a
set of judgements someone made from a beat report, each stamped with an `as_of` and a
`source`, and the git history is the useful part: what was believed, and when. A severity
written in August that looks wrong in November is a question `git log` should answer. One
file per season, so a rollover leaves last year's reasoning readable rather than overwriting
it.

`.gitignore` ignores `config*.yaml` so `config.yaml`'s league ids never leave the machine.
That pattern matches on **basename**, so `config/injuries/2026.yaml` is unaffected — but
incidentally, so an explicit `!config/injuries/` sits beside it. A future tightening of the
secrets pattern must not swallow these files silently.

### How often it needs updating

Measured on this repo's own episode table rather than estimated. Among players who clear the
materiality floor — the ones a projection adjustment could change a decision about:

| | per week |
|---|---|
| new injuries | **2.7** (1.6–2.2 in 2016–18, rising to 2.6–3.4 in 2023–25) |
| costing 3+ games | **1.2** |
| costing 5+ games | **0.35** |

An override is only worth writing where the automatic ladder is materially wrong, and that is
essentially the 1.2-a-week tail — a one-game knock resolves to a ~0.5-game absence on its own
and the human adds nothing. So:

- **Once before the draft.** 22 of the 145 players inside ADP 150 on the 2026 board sit on
  the `news text` rung; scan them and correct the few the beat reports say are worse than a
  camp tweak. On this board that was one — Jeremiyah Love, and it moved him a whole duration
  bucket.
- **Roughly one entry a week in season**, when a rostered player picks up something real.
- **Delete entries as they resolve.** Loading warns at 28 days, which is about the point a
  severity taken from a single report stops describing the present.

That is a few minutes a week, and it is the highest-value-per-minute input in the whole
package: the fitted curve was rejected as a multiplier, while a hand-written severity moves a
player's duration bucket outright.

Validation raises on an unknown body part and warns on an entry matching no player or older
than 28 days, because an override exists precisely where a human noticed something the
feeds do not carry — so one that silently does nothing is worse than no file at all.

Love end to end: without the override he resolves to a generic ankle at ~0.5 games (Active,
no severity in the text); with it, 4–6 weeks, bucket `5+`, confidence high. Different
duration buckets, therefore different cells of the fitted curve — and no automatic channel
can close that distance, because the severity is in a sentence nobody has written into a
field. `multiplier_ladder` is supported so the hypothesised 0.75/0.75/0.85/0.92 ramp can be
applied and scored against the fitted one, with `inj_severity_source = "override"` making a
hand number impossible to mistake for a fitted one.

### On the board

**Body Part** and **Wks Out**, in the `Notes` group beside `Exp G`. The provenance is *in*
the label — `ankle high (override)`, `knee (ESPN date)`, `soft tissue lower (news text)` —
because six channels can answer and they are not equally trustworthy, and on this board the
weakest rung is carrying 102 of 132 answers. A reader deciding whether to spend an
eighteenth pick needs to know which one he is looking at. Both columns carry the caveat that
nothing on the board is discounted by them.

### Keeping it fresh

The episode table and the fitted model are **not** in the nightly job, and deliberately:
they change only when a new season of games lands, and rebuilding ten seasons every morning
to get the same answer is waste. So they are refreshed by hand after a season completes:

```bash
Rscript R/GetUsage.R <season> <season>     # the box score the episodes are built from
Rscript R/GetContext.R <season> <season>   # rosters and the weekly injury report
python -m Scripts.injury.episodes --rebuild
python -m Scripts.injury.model --fit
python -m Scripts.injury.backtest --write  # re-score against the gates
python -m Scripts.lab.report               # re-render docs/model_lab.html
python -m Scripts.sync --push
```

An artifact nobody knows to refresh goes stale in silence, so
`attach_model_diagnostics` compares the model's own `train_seasons` against the season being
projected and names those commands when it is behind. That check existed as a method with no
caller for a while, which is the same as not having it — its sibling in
`Scripts/usage/project.py` caught a real model trained a season short.

**The daily ESPN snapshot is different and *is* in the nightly job**, because it has no
backfill. See phase 1.

## Effort

Phases 1–4: ~1 day, done, **204 tests** (49 episodes, 66 severity, 14 apply, 36 model, plus 13
new archive tests). Phases 5–7 as multipliers are **not to be built** on this evidence;
surfacing the fitted ladder and the recurrence probability as diagnostics is ~0.5 day.

**Phase 2 is where most of the standalone value is.** The table under *Evidence* is a
draft-room and Tuesday-lineup artifact with no fitted model in it, built on our own data
with our own episode logic and validated against a published figure. It does not depend
on the multiplier surviving its gate.

## Postscript — what building the data layer turned up

- **`ctx.load_injuries` had no caller.** Ten seasons of backfilled designations, a
  `report_rank` and a `practice_rank` computed on every read, and nothing downstream ever
  looked at them. The measurement in plan 16 that justified pulling the data was never
  wired to anything.
- **`player_weeks` is not a `context` artifact.** It comes from `GetUsage.R`, so
  `ctx._require(season, "player_weeks")` raises `KeyError` naming the four artifacts
  `GetContext.R` writes. The two readers are separate on purpose, and borrowing the wrong
  one puts the wrong R script in the error message.
- **"Appeared" needs both the box score and the snap count.** 15.7% of player-weeks with
  offensive snaps record no statistic at all — a blocking tight end, a decoy receiver —
  and 3.3% of box-score rows carry no offensive snaps. Either definition alone opens an
  episode on a quiet week.
- **232 episodes have no body part at all**, averaging 6.7 weeks out: the player went
  straight to reserve and the report never said a word. They are real injuries of unknown
  kind. `map_elements` skips nulls, so without an explicit fill they became a null
  `body_part` that every downstream group-by silently dropped.
- **Polars will not nest one window expression inside another.** The run-id construction
  (`shift().over()` then `cum_sum().over()`) has to be two passes, and the error —
  `window expression not allowed in aggregation` — does not say so.
- **`pd.Series(pd.NA, index=..., dtype="float64")` raises.** pandas tries `float(pd.NA)` to
  fill the numpy array: `TypeError: float() argument must be a string or a real number, not
  'NAType'`. It has to be a real nan; only the nullable extension dtypes take `pd.NA`. This
  took all nine boards down and no test caught it, because the tests covered the severity
  resolution and not the attach.
- **ESPN's fantasy status and its own injury feed disagree, usefully.** Nabers is
  `Questionable` in the fantasy API and carries a surgical ACL with a past return date in
  the site API. Neither field alone tells you he is fine.
- **A seeded bootstrap was not reproducible, and it moved the conclusions.** Polars'
  `unique()` is unordered and parallelised, so a fixed-seed RNG indexing into it drew a
  different sample every run. That moved the fitted standard errors, which moved the
  abstention decisions, which moved the walk-forward's chosen shrinkage — two runs of the
  same code on the same data disagreed and neither was wrong. A fitted artifact has to be
  reproducible or its provenance means nothing.
- **`tau` was unidentifiable above the observation window.** With six points the likelihood
  is flat past `tau ≈ 6`, and the ankle cell came back at **112** — the optimiser's way of
  writing a flat line, there being no other way to express one in this parameterisation. Left
  in, it asserts a *permanent* 9% talent reduction from a sprain and triples what the season
  multiplier charges.
- **MAE and RMSE disagreed about the ranking, and only one of them was answering the
  question.** See phase 4 above. The general lesson: a metric has to match the functional the
  predictor estimates, and "improvement in MAE" on a skewed target is a measure of downward
  bias as much as of accuracy.
- **`cut` raises on duplicate quantile boundaries.** A narrow baseline distribution collapses
  every decile edge onto one value and `fit_control` died with `breaks are not unique`. The
  right response is fewer strata, not an exception.
- **The four label and precedence bugs above were all invisible in aggregate.** Every one of
  them produced a plausible-looking count — 132 resolved either way — and only reading the
  actual rows against the actual comments surfaced them. The lesson is the same one plan 16
  records about measurement: the summary statistic cannot tell you the rows are right.
