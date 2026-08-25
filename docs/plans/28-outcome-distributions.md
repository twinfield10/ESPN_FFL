# 28 — Outcome distributions, and where the variance actually lives

**Priority:** High (draft-relevant, and it is the first thing on the board that would
be *new information* rather than a better mean) · **Effort:** L · **Status:** **Phases
1-3 built 2026-08-24. G-D0, G-D1 and G-D3 pass; G-D2 and G-D5 fail.**

The board now carries `pts_p10`, `pts_p90`, `p_top12`, `p_bust` and `outcome_evidence`
on all nine leagues, from a Monte Carlo over the usage model's own fitted per-stat
distributions, rescaled onto `TRUE_Points`. **No projection moved** -- `TRUE_Points` is
identical to the byte on a rebuilt board against the same inputs.

**G-D1 passed at 0.730 on 2026-08-24 and fails at 0.687 on re-measurement**: a third of
the gated sample was players projected near zero who realise zero and sit inside their own
interval. The threshold is untouched; the population was corrected, which made the gate
harder. **G-D2 failed**: the room-level joint draw is only **+2.1pp** closer to nominal for backups
against a 5pp bar, so phase 1 ships alone and the room machinery is off by default --
though entrenched starters move **+0.0pp**, so the effect is exactly as vacancy-specific
as claimed and it is the size that fails, not the direction. **G-D3 passed** at 13.5%
within position, driven by quarterback and receiver.

**Two defects found in shipped code on the way, and the postscript has both.** The
published `USG_<stat>_low`/`_high` were the realised-season spread rescaled onto an
if-healthy centre, which on **14.0% of projected cells was a p10 equal to its p90** -- now
0.5%, and fixed without moving a mean. And **half of `expected_games` is role rather than
availability**: the proportional rescale everyone reaches for over-projects a realised
total by up to 27%, and the fitted exponent is 0.32-0.49. **Evidence below measured 2026-08-18, and it already kills one of the three
things this plan was asked for.** The vacancy transfer is real and large in a backfield
— the lead back's 17.42 opportunities a game go 81% to the next three backs and the room
keeps 93% of its volume — **and it is absent in a receiver room**, where the WR2 gains
0.59 targets and the offence just throws less. Long absences predict the *following*
season on a clean monotone ladder (next-season ppg ratio 0.837 / 0.726 / 0.641 across
1–3, 4–7 and 8+ weeks out, against a healthy control at 0.906). But **a fragile
incumbent is not, on this evidence, a tradeable edge for his backup**: RB2 season points
behind a clean incumbent are 111.4 and behind one who missed 3–5 games are **92.4**,
backwards, and the incumbent's own prior absence barely predicts his next one once you
stop conditioning on the outcome. That last table is **confounded by team strength in a
direction plan 21 already measured** — `Scripts/draft/handcuff.py` puts a weak team's RB2
19 carries behind a strong team's — so the premise is unproven rather than refuted, and
phase 5 is the cheap stratified test that settles it. Either way the deliverable is a
**distribution**, not a bump: the transfer is worth ≈46 points over an eight-game absence
against the existing handcuff column's entire ±13-carry range.

**Depends on:** [16](16-usage-data-layer.md) — the context readers and the leakage
discipline · [18](18-season-usage-model.md) — the per-stat marginals this aggregates,
and `availability.py`'s Beta-Binomial · [10](10-scoring-registry.md) — the per-league
linear map from stats to points · [27](27-injury-model.md) — the episode table, and its
duration-as-severity finding
**Feeds:** [09](09-frontend-draft-views.md) — the columns · [13](13-dst-from-vegas-lines.md)
— shares its `E[f(X)]`-over-a-distribution machinery · [19](19-weekly-usage-model.md) —
the weekly surface, which is not this plan's job

## Problem

The board publishes a mean and an interval, and the interval is not uncertainty.
`attach_source_spread` (`Scripts/season_projections.py`:1258) sets `floor`/`ceiling` from
how much the sources *disagree*, and that file says so deliberately at line 1245: "do the
forecasters disagree, and by how much" is a different question from "how uncertain is
this forecast". `USG_` was briefly added to the spread by mistake and removed, because a
level mismatch is not a disagreement.

Meanwhile the uncertainty **does** exist upstream and never arrives:

| piece | where | reaches the board? |
|---|---|---|
| games played, Beta-Binomial | `Scripts/usage/availability.py` | as `usg_games_low/high` |
| per-stat, NegBin / Gamma | `Scripts/usage/predictive.py` | as `USG_<stat>_low/high` |
| **season points** | **nowhere** | **no** |

`grep -rn "points_low\|Points_sd\|pts_sd" Scripts app` returns nothing. Every stat has a
distribution; the one number a drafter reads has a point estimate and a disagreement
band.

And there is a question the per-player marginals **structurally cannot answer**. A
backup running back's season is bimodal. On the measured per-game figures below he is a
**71-point** flier if the man ahead of him plays all seventeen games (4.15 x 17) and a
**116-point** starter if that man misses eight (4.15 x 9 + 9.86 x 8) -- a 64% swing on a
variable that is not his own. A marginal interval fitted on his own residuals reports a
single smear over both worlds and calls the middle of it his projection. The information that separates the two lives on
*another player's* row.

## Evidence

All figures 2016–2025, this repo's own `player_weeks`, `rosters_weekly` and
`injury_episodes`. Scripts in the postscript.

### Almost all of the reducible uncertainty is opportunity, not efficiency

Not measured here — measured already, by plan 18, and it is the finding that says where
this plan should spend its effort. From `Scripts/usage/predictive.py`'s own docstring:
conditional on the opportunity count, bounded rates are **1.08×–1.79×** overdispersed
against Binomial, where games played is 5.6–8.1× and volume is 13×–99×. Once you know
how many targets a player gets, what he does with them is close to sampling noise.

So an "efficiency distribution" is the cheap half. The expensive half — and the half
worth modelling jointly — is *how much work he gets*, which is availability and depth
chart. Which is what was asked for, and it happens to be right.

### The vacancy transfer, and where the accounting closes

Team-week level, restricted to team-seasons that experienced both states (lead present
for ≥3 games, absent for ≥2), so the comparison is within-team. "Rank" is by the
position group's own season opportunity total.

| | RB room (carries+targets) | WR room (targets) | TE room (targets) |
|---|---|---|---|
| team-seasons | 133 | 80 | 159 |
| group volume, lead in → out | 27.84 → 25.88 | 18.93 → 16.53 | 7.13 → 4.86 |
| **group retains** | **93%** | 87% | **68%** |
| rank 1 | −17.42 | −7.72 | −4.89 |
| rank 2 | **+7.14** | **+0.59** | +1.29 |
| rank 3 | +4.44 | +1.23 | +0.64 |
| rank 4 | +2.59 | +1.68 | +0.38 |
| **recaptured by ranks 2–4** | **81%** | **45%** | 47% |

**And the measurement is conservative, in a direction worth naming.** "Rank" is the
season's own opportunity total, so a team-season where the absence ran long enough to hand
the backup the season lead is counted with the roles *swapped* — the deposed starter shows
up as rank 2. There is no leakage-free pre-season rank available inside a descriptive
measurement, so the extreme cases are understated rather than overstated, and any share
fitted from this table inherits that direction. `tests/test_outcomes_evidence.py` pins it.

**A backfield is very nearly zero-sum and a receiver room is not.** That is the whole
design constraint. 81% of a lead back's vacated work reappears on the next three backs
and the room barely shrinks; 45% of a lead receiver's targets reappear on the next three
receivers, and his direct understudy gets 0.59 of 7.72.

Where the rest of a missing WR1's work goes, same frame:

| | lead WR in | lead WR out |
|---|---|---|
| WR targets / gm | 19.53 | 16.76 |
| RB targets / gm | 6.13 | **6.65** |
| TE targets / gm | 7.02 | **7.85** |
| team pass attempts / gm | 34.12 | **32.87** |

The offence throws 1.25 fewer times and redistributes across position groups rather than
within one. A rule that hands a WR1's targets to the WR2 would be inventing 2.8 targets
a game from nothing.

### The player-level effect, paired within player-season

Same mechanism, seen from the recipient's row rather than the room's. Restricted to
player-seasons with ≥3 games in each state, so each player is his own control.

| | n | opportunity / gm | points / gm |
|---|---|---|---|
| RB2 | 87 | 5.09 → 12.93 (**2.54×**) | 4.15 → 9.86 (**+5.72**) |
| RB3 | 87 | 2.71 → 6.68 (2.46×) | 2.05 → 5.11 (+3.06) |
| TE2 | 104 | 1.27 → 2.76 (2.16×) | 2.32 → 4.87 (+2.55) |
| TE3 | 103 | 0.64 → 1.29 (2.00×) | 1.14 → 2.19 (+1.04) |
| WR2 | 45 | 4.37 → 4.68 (1.07×) | 7.88 → 7.93 (**+0.05**) |
| WR3 | 45 | 3.02 → 3.67 (1.22×) | 5.23 → 5.90 (+0.67) |

An RB2 is a different player when the RB1 sits. A WR2 is the same player. **+5.72 points
a game over an eight-game absence is 46 points**, sitting inside a variable the board does
not carry -- and for scale, that is roughly 3.5x the entire range of the one column that
does try to price a backup, `Scripts/draft/handcuff.py`'s +-13 carries.

### Duration predicts the following season; the diagnosis is unavailable

The plan was asked for "the chance a player returns from an ACL at full strength". That
question cannot be asked of this data, and the reason is the same truncation plan 27
found: **of 992 episodes of 8+ weeks, 73.1% have no body part at all** (730 labelled
`other`), because the injury report goes quiet once a player lands on reserve. Knee at
8+ weeks is n=59, and only 10 survive the filter needing both a usable prior season and
a following one. There is no ACL cohort here.

Duration substitutes, and it works. Season S+1 outcome for players with an episode in S,
requiring ≥6 games and >3.0 ppg in S−1 so the baseline is real:

| last season | n | games in S+1 | ppg ratio to S−1, median | P(≥90% of prior ppg) | P(≥14 games) |
|---|---|---|---|---|---|
| healthy control | 967 | 12.9 / 17 | **0.906** | **0.51** | **0.59** |
| 1–3 weeks out | 431 | 11.8 | 0.837 | 0.43 | 0.46 |
| 4–7 weeks out | 218 | 11.3 | 0.726 | 0.32 | 0.44 |
| **8+ weeks out** | **169** | **9.8** | **0.641** | **0.26** | **0.31** |

Monotone in all four columns. An 8+ week absence roughly **halves** the chance of a full
following slate (0.31 against 0.59) and costs ~29% of prior per-game production against
the control's ~9%. That is the closest available answer to the ACL question: about **a
quarter** of players return to within 90% of their prior form, against about half of
comparable healthy players.

**Caveat stated against ourselves.** The control is selected on a healthy season S
(≥14 games, no episode of 2+ weeks), which selects for durability, so it is flattered.
Both cohorts are indexed to S−1 so the *ratio* is matched, but the control's level is an
upper bound rather than a neutral placebo. The 27-style fix — a control passing the
identical filter — needs the filter rebuilt on a season-to-season frame and is phase 4's
first job, not a result to lean on now.

### The fragility premium does not survive as stated, and the reason is instructive

The specific hypothesis: a lead back who is *constantly injured* raises the value of the
back behind him. Two things have to be true, and only one is.

**Is fragility persistent?** Across all skill players with a real prior role (≥8 games),
yes, and strongly — `corr(missed_S, missed_S+1) = 0.31`, and on two seasons of history:

| prior two seasons | n | P(miss ≥3 next) | mean games missed |
|---|---|---|---|
| both clean (0, 0) | 131 | 0.405 | 3.50 |
| one bad (≥3) | 597 | 0.558 | 4.75 |
| both bad (≥3, ≥3) | 377 | **0.785** | **7.16** |

**But that is role, not body.** Re-run on incumbent lead backs identified *pre-season* —
led his team in opportunity in S−1, on a week-1 roster in S, so role loss is mostly
stripped out (n=217):

| incumbent's prior season | n | games played | games missed | P(miss ≥3) |
|---|---|---|---|---|
| missed 0 | 72 | 13.2 | 3.29 | 0.40 |
| missed 1–2 | 85 | 12.6 | 4.00 | 0.46 |
| missed 3–5 | 48 | 12.2 | 4.40 | 0.50 |
| missed 6+ | **12** | 13.2 | **3.42** | **0.33** |

A gradient from clean to any-history of about +1 game, then nothing, and the tail
reverses on n=12. Two-season split (n=35 / 70 / 23): both clean 2.66 games missed, one bad
4.40, both bad 4.04 — non-monotone too, and the pattern version of "constantly injured"
is the intuitive form of the hypothesis, so it gets its own row rather than an
extrapolation. This is consistent with plan 18's own finding at
`Scripts/usage/features.py`:721, where prior *snap share* beats prior *games* at
predicting next season's availability and is documented as reading "role security rather
than durability". The population-level persistence is largely players losing jobs.

**And the payoff is absent or negative.** The RB2 behind each of those incumbents:

| incumbent's prior season | n | RB2 season opportunity | RB2 season points | P(RB2 > 150) |
|---|---|---|---|---|
| missed 0 | 72 | 136.5 | **111.4** | 0.15 |
| missed 1–2 | 85 | 128.4 | 109.4 | 0.25 |
| missed 3–5 | 48 | 114.1 | **92.4** | 0.06 |
| missed 6+ | 12 | 120.2 | 104.5 | 0.33 |

Backwards, and mechanically explicable: a team carrying a fragile lead back tends
already to be running a committee, so its "RB2" is a lower-ceiling player on a worse
offence. **Do not build a fragility-conditional bump for backups on this evidence** — and note
that "this evidence" is doing real work in that sentence, because the payoff table is
confounded by team strength. See the next section. What is not in doubt: the transfer is
real conditional on the absence, and the absence is not forecastable enough to price into
a mean, which is precisely the argument for pricing it into a *variance*.

### The board already has a handcuff column, and it prices a different variable

`Scripts/draft/handcuff.py` exists, from plan 21, and it must be read before any of the
above is called new. It prices an RB2 by **team strength** — a strong team's number-two
back gets 103.0 carries against a weak team's 84.0 while RB1 stays flat at ~213 — fitted
over 315 team-seasons at `RB2 carries = 94.5 + 1.65 × strength`, R² **0.030**, residual SD
36 carries. Its own docstring calls it a tiebreaker between two similar backups rather
than a reason to move anyone a round, and ships `handcuff_r2` so it cannot be read as
more.

**Two consequences, and the second is a correction to this plan's own measurement.**

*The magnitudes are not comparable, which is the case for building this.* The handcuff
column's entire realistic range is ±13 carries, about 55 rushing yards. The vacancy
transfer is +5.72 points a game, ≈46 points over an eight-game absence — roughly **3.5×
the whole span of the existing column**, and it lives in a quantity the column cannot
express, because a mean tiebreaker and a bimodal spread are different objects.

*And the fragility-premium table above is confounded, in a direction plan 21 already
measured.* A team carrying a fragile lead back is disproportionately a weak team, and a
weak team's RB2 gets 19 fewer carries **for reasons that have nothing to do with the
starter's health**. So the finding that RB2 points fall from 111.4 to 92.4 behind a more
fragile incumbent is not evidence that fragility is worthless — it is a measurement with a
known omitted variable whose sign is known. The honest statement is narrower than the one
this plan first wrote: **on this evidence a fragility bump is not buildable, and the test
that would settle it is the same measurement stratified by Vegas team strength**, which
`handcuff.py` already computes and which is cheap. It is listed as phase 5 rather than
cut, and it is the one place this plan expects to revise itself.

## Fix

### One architectural claim, and it is the whole plan

**The unit of simulation is the position room, not the player.** Draw each room's
availability once, redistribute the vacated opportunity inside the room, score, repeat.
A backup's bimodality is then an emergent property of the shared draw rather than
something anyone has to assert.

Why marginals cannot substitute: the RB2's two worlds differ by +5.72 points a game, and
which world he is in is a function of the RB1's row. Independent per-player intervals
have no channel to carry that, and `predictive.py` already found that composing
independent factors understates spread even *within* one player (games vs per-game volume
correlate +0.48 to +0.63; backing a variance out of the identity produced negative
numbers for quarterbacks). The correlation is not a nuisance here — it is the product.

### Points, not stats, and it has to be per league

A season points distribution is a league-specific linear combination of correlated stat
distributions, so it cannot be a property of the `USG_` artifact. It is computed at
`proj_to_score` time against `Scripts/scoring.py`'s registered rules, per league, the
same way `TRUE_Points` already is. Nine leagues, nine distributions, one model.

Simulation rather than closed form, and deliberately, despite `predictive.py` having gone
out of its way to stay closed-form: the aggregation is over a **correlated** vector under
a **shared discrete** availability draw with a **non-linear** redistribution rule.
There is no incomplete-beta for that. `Scripts/simulation_utils.py` already runs a Monte
Carlo at the league-standings level, so the pattern exists — **but do not copy its
seeding.** It calls `np.random.seed(42)` and then `np.random.normal`
(`simulation_utils.py`:65, :462), which is reproducible only as long as nothing else in
the process touches the global stream. A per-player simulation invoked once per league
across nine leagues is exactly the situation that breaks, so this uses an explicit
`np.random.default_rng(seed)` Generator threaded through the call.

### The redistribution rule, only where the accounting closes

Fitted per position group from the closure table, and **applied only to RB and TE
rooms**. Shares of vacated opportunity to ranks 2/3/4: RB **0.41 / 0.26 / 0.15** (81%
recaptured, 7% group shrinkage absorbed), TE **0.26 / 0.13 / 0.08**. WR rooms get **no
transfer rule at all** — the measured within-room recapture is 45% and lands mostly on
ranks 3 and 4, while the offence itself contracts. A WR room's vacancy is modelled as
group shrinkage with no beneficiary, which is what the data says it is.

Depth rank comes from `ctx.depth_features` / `preseason_snapshot` (plan 21's daily 2026
snapshot), not from a season total the projection cannot know.

### Severity feeds the availability prior, not the mean

Phase 4, and this is where plan 27's rejected work becomes useful. The duration ladder
above is a *next-season* signal, and `availability.py` has exactly the parameter for it:
the prior absence bucket shifts `mu`, the Beta-Binomial's mean share, rather than
multiplying anyone's points. A 8+ week absence last season moves a player from the
control's 0.59 chance of a full slate toward 0.31. Plan 27's ~1% accuracy gain failed as
a multiplier on a mean because the effect was small relative to the mean; the same effect
as a **shift in a distribution's location and a widening of its spread** is being asked
to do something it can actually do.

### Artifacts

- `Data/NFL/vacancy_transfer.parquet` — the fitted redistribution shares by position and
  rank, with `n` and the recapture rate visible. **Not** season-scoped and **not** in
  `store.ARTIFACTS`, for plan 27's reason: pooling across seasons is the point, and a
  vacated backfield is the same vacated backfield in all nine leagues.
  `s3_store.MIRROR_TIERS["NFL"]` publishes it with no new plumbing.
- `outcomes` in `store.ARTIFACTS` — per league, per player: `pts_p10`, `pts_p50`,
  `pts_p90`, `pts_mean`, `p_top12` (position-relative), `p_bust`, and `outcome_evidence`
  naming why a row is missing. League-scoped because points are.

### Phases

| Phase | What | Gate |
|---|---|---|
| — | `Scripts/outcomes/evidence.py` — the measurements above, reproducible | **Done 2026-08-18** |
| 0 | **G-D0 first.** Does the existing `floor`/`ceiling` already span it? | **G-D0 passed 2026-08-24** |
| 1 | `Scripts/outcomes/distribution.py` — points marginal from the per-stat marginals, per league scoring, correlation from residuals | **G-D1 FAILS on re-measurement 2026-08-25** — 0.687 on the draftable pool. Columns ship, labelled |
| 2 | `Scripts/outcomes/vacancy.py` + `simulate.py` — the room-level joint draw | **G-D2 FAILED 2026-08-24 — built, measured, off by default** |
| 3 | Board columns and `app/draft_view.py` | **G-D3 passed 2026-08-24** |
| 4 | Duration → `availability.py`'s `mu`, with a 27-style matched control rebuilt on the season frame | G-D4 |
| ~~5~~ | ~~the fragility premium, stratified by Vegas team strength~~ | **G-D5 FAILED 2026-08-24 — do not build** |
| — | fragility-conditional bump for backups, unstratified | **Do not build** — measured, reversed, and confounded |
| — | fragility-conditional bump for backups, **stratified** | **Do not build** — G-D5 ran it and it is non-monotone inside strata, with the strong tertile running 142.1 → 94.3 the *wrong* way |
| — | diagnosis-level (ACL) severity | **Do not build** — 73.1% of long absences carry no body part. Revisit after a season of plan 27's ESPN archive |
| — | WR-room transfer rule | **Do not build** — 45% recapture, +0.59 targets to the WR2 |

### Gates, pre-committed

Registered in `Scripts/lab/registry.py` beside `injury_verdict` and `hazard_verdict`,
scored by `Scripts/outcomes/backtest.py`, walk-forward over held-out seasons.

**G-D0 — materiality, and it runs before anything is built.** Plan 27's G-B0 exists
because "no source prices this" turned out to be false. Same discipline: on the archived
boards, compare the existing `floor`–`ceiling` width for depth-rank ≥2 RBs and TEs
against the simulated p10–p90 width. **Bar: the simulated interval must be at least 1.5×
wider at the median.** If source disagreement already spans the bimodal range, the board
displays this information today and the rest of the plan is machinery for a column that
already exists.

> ✅ **PASSED 2026-08-24, by 17.5× against a 1.5× bar.** For depth-rank ≥2 RBs and TEs
> the board's `(ceiling − floor) / TRUE_Points` is **16.0%** (n = 104); the realised
> spread of `actual / projected` season points over 2020–2025 is **280.5%** (n = 626).
>
> **It is not the availability tail.** Stripping players who barely appeared:
>
> | cut | width | vs board |
> |---|---|---|
> | all | 280.5% | 17.5× |
> | played 1+ games | 268.2% | 16.7× |
> | played 8+ games | 204.7% | 12.8× |
> | played 14+ games | 185.8% | **11.6×** |
>
> The most legible form: **the board's own floor→ceiling contains 4.6% of realised
> outcomes** — 10.1% among players who managed 8+ games — against the ~80% a thing
> called a floor and a ceiling implies.
>
> Two substitutions, both forced and both stated. No historical board survives (plan
> 25), so the incumbent width is the live 2026 board and the spread is 2020–2025. And
> the projection is TOMCAT's expected-value line rather than the `TRUE_` blend, so its
> own error is inside that spread — a better projection would narrow it. Neither can
> plausibly close a 11.6× gap, which is why the gate is reported as passed rather than
> as passed-with-conditions.

**G-D1 — calibration, and it is not optional.** An uncalibrated distribution is worse
than a point estimate, because it invites acting on its tails. **Bar: 80% interval
coverage inside [0.72, 0.88] and calibration slope inside [0.85, 1.15]** on realised
season points, walk-forward. `availability.py::calibration` and
`Scripts/usage/backtest.py`'s existing coverage report are the pattern.

> ❌ **PASSED 2026-08-24 AND FAILS ON RE-MEASUREMENT 2026-08-25.** The pass was real
> arithmetic on the wrong population.
>
> Reported coverage was **0.730** over 2021–2025, inside the window. But **32% of the
> scored sample projects under 10 points, realises a median of exactly 0.0, and is
> "covered" at 0.825** — its interval contains the zero it was always going to produce.
> Those rows are not a forecast anybody reads. On players projected above 25 points,
> coverage is **0.687**, outside [0.72, 0.88], and the verdict is *too narrow — it is
> lying*.
>
> **The cut is not what decides it, and that is checkable.** Coverage by projection floor:
>
> | floor | ≥0 | ≥10 | ≥25 | ≥50 | ≥100 |
> |---|---|---|---|---|---|
> | coverage | 0.730 | 0.685 | 0.687 | 0.699 | 0.724 |
>
> Every floor from 10 points up says the same thing. Only the unfiltered population
> disagrees, which is what identifies it as the artefact rather than the finding.
>
> **The threshold was not touched.** `OUTCOME_COVERAGE_RANGE` is exactly as
> pre-committed; what changed is `MIN_SCORED_PROJECTION`, the population it is measured
> over — a correction that makes the gate *harder*, which is the only direction a
> post-hoc change to a gate can honestly move. Found on 2026-08-25 while checking a
> suspiciously good rookie cell in [33](33-role-resolution.md) phase 3, where the same
> artefact had inverted that plan's result.
>
> Calibration slope **1.072** is unaffected and still inside its window: it measures
> whether the spread is right *where the model claims it is*, which is a within-sample
> comparison the degenerate rows do not distort.
>
> **What this means for the shipped columns.** They are still far better than what they
> sit beside — the board's own floor-to-ceiling contains **4.6%** of realised outcomes
> against this interval's 68.7%. But a `p90` measured at 0.687 coverage is closer to a
> 1-in-6 than a 1-in-10, and `p10` understates downside, which is the worse of the two
> directions. The columns keep their place and the board now says so; widening the
> interval is a modelling change and the lead is in the postscript below.
>
> **The folds start at 2021 and that is a constraint rather than a choice.** The
> dispersions are fitted on held-out residuals, and `_holdout_residuals` sets aside the
> two most recent training seasons and returns nothing below four — so a 2019 fold trains
> on 2017–2018 and its model carries no distribution to score at all. Running it would
> have reported a model with no interval as a model whose interval failed.
>
> The direction of the miss is named in the postscript — the decomposition drops ~10% of
> the unconditional spread because it draws games and stats independently, while
> `predictive.py` measures those correlating +0.48 to +0.63. That is the first thing to
> try against the gap.

**G-D2 — the joint structure must earn its complexity.** Coverage for depth-rank ≥2
players in RB and TE rooms, joint draw against independent marginals. **Bar: the joint
must be at least 5 percentage points closer to nominal.** If independent marginals
already cover the backups, the room-level machinery is unjustified and phase 1 ships
alone.

> ❌ **FAILED 2026-08-24 — +2.1pp against a 5pp bar.** Backup coverage goes 0.695 →
> 0.715; nominal is 0.80. So phase 1 ships alone, `BOARD_USES_JOINT_DRAW` is False, and
> the room machinery stays in the tree measured and rejected — plan 27's outcome for its
> recovery curve, reached the same way.
>
> **How it failed is the part worth keeping.** Entrenched starters move **+0.0pp**. The
> false-positive clause — which exists because a model that simply widens every interval
> improves coverage everywhere — found *nothing*. The effect is exactly as
> vacancy-specific as the mechanism claims; it is the magnitude that fails, not the
> direction. The shape change is real and large: on the 2026 board the joint draw takes a
> backup RB's p90/p50 from ~1.70 to 1.82–2.16, moving mass out of the middle into both
> tails at constant mean. It just does not buy enough coverage to justify carrying it.
>
> **Two bugs found on the way, both of which would have made this gate pass wrongly.**
> The first draft of the transfer *added* opportunity rather than redistributing it, which
> lifted the median backup on the 2026 board from 123 points to 156 — a different
> projection, not a wider one. It is a double-count: an RB2 averages 9.86 opportunities a
> game across a season against 5.09 with his lead present and 12.93 without, so a model
> fitted on season totals already prices the expected inheritance. And the control cohort
> was contaminated: the 2016–2024 depth chart lists **two or three rank-1 backs in 19–23
> of 64 rooms**, because its rank 1 means "a starter" rather than "the best one", so a
> `depth_rank <= 1` control group contained players receiving the treatment. Ties inside a
> rank are now broken by projected opportunity.

**G-D3 — decision relevance.** Ordering by `p_top12` against ordering by mean points.
**Bar: ≥5% of draftable players move by ≥12 picks.** If the two orderings agree, ship
the columns as diagnostics and do not touch the board's sort — the plan-27 outcome, named
in advance again.

> ✅ **PASSED 2026-08-24 at 13.5%** — 18 of 133 draftable players inside `vor_rank` 200
> on the 2026 Winfield Football board, the league the walk-forward scores. By position:
> QB **23.1%** (6 of 26), WR **22.9%** (11 of 48), TE 3.8% (1 of 26), RB **0.0%** (0 of
> 33). GOP's board gives 13.3% on the same measure, so it is not a one-league artefact.
>
> **Measured within position, and the first attempt was not.** `p_top12` is the chance of
> finishing in a player's *own* position's starter tier, so ranking the whole board by it
> ranks quarterbacks against running backs on two different scales — that version scored
> 85% and was measuring only the mismatch. Within position it is a real question and the
> answer is 13.3%.
>
> **Running back moves nobody, and that follows from G-D2.** The joint draw is off, so an
> RB's distribution is his own marginal, and a marginal's `p_top12` ordering tracks its
> mean closely. The movement is at quarterback and receiver, where the pools are deepest
> relative to their starter tiers and the tails cross most.
>
> It is worth saying which way the causation runs: the position this plan was *written*
> about is the one its shipped columns reorder least, because the mechanism that would
> have reordered it is the one G-D2 turned off.
>
> Passing means the ordering information is materially different — **not** that the
> board's sort should become `p_top12`, which is position-relative and cannot compare
> across positions. It ships as a sortable column beside `VOR`.

**G-D4 — the severity shift.** Out-of-sample R² on games played, prior-absence bucket
added to the availability head. **Bar: +0.02**, the bar prior snap share actually cleared
(0.203 → 0.230 at `features.py`:721). Anything smaller is not worth a feature.

**G-D5 — the fragility premium, given the confound.** Re-run the incumbent→backup
measurement within tertiles of `handcuff.py`'s team-strength variable. **Bar: a monotone
gradient in the backup's season points across the incumbent's absence buckets, inside at
least two of three strength tertiles.** Non-monotone inside strata means the premise is
dead rather than merely confounded, and this plan stops claiming otherwise.

> ❌ **FAILED 2026-08-24 — monotone in 1 of 3 tertiles.** Backup's season PPR points by
> the incumbent's prior-season games missed, 203 incumbent/backup pairs with a Vegas
> strength value, tertile cuts at spread −1.21 and 2.18:
>
> | tertile | missed 0 | missed 1–2 | missed 3–5 | missed 6+ | monotone |
> |---|---|---|---|---|---|
> | weak | 93.6 (25) | 93.0 (26) | 85.5 (13) | — (4) | yes |
> | middle | 106.4 (24) | 107.5 (23) | 97.4 (18) | — (3) | no |
> | **strong** | **142.1 (21)** | **125.8 (27)** | **94.3 (14)** | 106.3 (5) | no |
>
> **And where there is a gradient it runs the wrong way.** A fragile incumbent's backup
> scores *fewer* points, not more — most sharply on strong teams, 142.1 down to 94.3.
> The premise was that a fragile starter makes his backup more valuable; no tertile
> supports it and the best-powered one contradicts it.
>
> So this is the outcome the gate named in advance: **the premise is dead rather than
> merely confounded**, and phase 5 moves to *Do not build*. Thin, and stated: 203 pairs,
> and the "missed 6+" bucket falls below five in two tertiles, so the test rests on
> three buckets there. The middle tertile's non-monotonicity is a hair (107.5 against
> 106.4) and reads as flat; the strong tertile's is not.

**And one false-positive clause, in G-D1's spirit and ordered before it**, borrowed from
`injury_verdict`: if widening the interval improves coverage for healthy, entrenched
starters *as much as* for backups, the model has found variance in general rather than
vacancy in particular, and the redistribution rule is decoration. Report that first.

## Effort

**Spent: phases 1–3 in a day**, against an estimate of ~3 days. The estimate was not
wrong about the work; it was wrong about where the work would be. Almost none of it went
on the Monte Carlo, which reuses `predictive.quantile` as its own inverse CDF and was
right the first time. It went on three things the plan had not anticipated:

1. **The plan's stated conditional fit was biased and had to be replaced.** `USG × games /
   expected_games` over-projects by up to 27%; the exponent had to be fitted. Half a day,
   and it produced the most transferable finding here.
2. **Two bugs that would each have made a gate pass wrongly** — the transfer adding rather
   than redistributing, and the control cohort containing treated players. Both were found
   by looking at a number that seemed slightly off, not by a test.
3. **Attributing a 21.97-point `TRUE_Points` drift** that turned out to be live ESPN data
   moving between the 06:00 nightly and the rebuild, not the change under test. Settled by
   stashing the entire branch and reproducing the drift on clean `main`.

The general lesson, which is the one worth carrying: on this plan the *simulation* was
cheap and the *basis* was expensive. Every hard question was some version of "which random
variable is this, exactly".

L, now **M** — phase 5 is gone. Phase 0 was an afternoon and did not reject the plan;
phase 5 was also an afternoon and **did** reject its own premise, which is exactly why
it was scheduled early. Both ran 2026-08-24. Phases 1–2 were estimated at ~2 days
including the correlation estimation and the seeding discipline a reproducible Monte Carlo
needs. Phase 3 ~0.5 day. Phase 4 ~0.5 day, and it is the piece most likely to survive on
its own merits, because it is a feature on an existing head with an existing bar rather
than a new artifact.

**Most of the standalone value is in the closure table.** The RB/TE/WR transfer split is a
draft-room fact with no fitted model in it — "an RB2 is a different player when the RB1
sits and a WR2 is not" changes how you value a bench, whether or not any of the
simulation machinery survives its gates.

## Postscript — what measuring this turned up

- **The RB2 hypothesis reversed under a leakage fix.** Defining the lead back by *this*
  season's opportunity total gave prior-fragility → no effect (1.52 vs 1.69 games
  missed), because conditioning on "led his team in touches" selects out exactly the
  players who got hurt. Re-identifying the incumbent from S−1 plus a week-1 roster
  changed the base rates by more than a factor of two (3.29 games missed against 1.52).
  The original framing was measuring survivorship.
- **`games_missed` is two quantities.** It counts injury and benching alike, and the gap
  between the population persistence (r = 0.31) and the incumbent-only persistence
  (~none) is the size of the benching component. Any future use of it should say which
  one it means.
- **Half of `expected_games` is role, not availability, and it is now a fitted number.**
  The obvious way to ask "what would he do over G games" is to divide `expected_games`
  out of `USG_<stat>` and multiply the games back in. Measured on held-out residuals that
  **over-projects the realised total by +8.8% to +26.7%** and drops the regression slope
  of realised on projected from ~1.00 to 0.32–0.70, while the unconditional projection on
  the same rows is unbiased with a slope of 0.92–1.10 — so it is a real degradation, not
  the regression dilution a slope alone would suggest. The cause is the thing
  `DATA_CATALOGUE.md` already asserts qualitatively: a low `expected_games` on a backup
  means *buried*, not *fragile*, and his per-game line is a buried player's line.
  Fitting the exponent instead — `USG_<stat> × (games / expected_games) ** e` — puts `e`
  at **0.32 to 0.49** across every position and stat, bias inside ±6% and slope back to
  0.91–1.04. Read plainly: **a player who plays twice the games the model expected
  produces about the square root of twice the output, not twice.** Quarterback is lowest
  at 0.32, which is where [31](31-team-coherent-tomcat.md) would predict it.

- **The shipped `USG_<stat>_low`/`_high` were a rescaled version of the wrong
  distribution, and on 14% of cells they were not an interval at all.**
  `stat_intervals` fits its quantiles around `USG_<stat>`, which carries
  `expected_games` inside it, so its spread is the spread of the season a player
  *realises*. `to_full_slate` then multiplied the mean **and those quantiles** by
  `slate / expected_games`. Multiplying a quantile by a constant rescales a random
  variable correctly — but the if-healthy line is a *different* random variable, with the
  availability variance taken out, so the published band carried realised-season spread
  around an if-healthy centre. Worst exactly where the multiplier is largest: a TE
  projected for 22 receiving yards had his interval evaluated at a mean of **1.01 yards**,
  where the fitted Gamma is nearly a spike at zero, and then scaled up — giving
  **p10 = p90 = 0**. Across the 2026 artifact **398 of 2,839 projected cells (14.0%) had a
  p10 equal to their p90**; it is now 15 (0.5%). On cells that were not degenerate, every
  stat narrows: −5% at receiving yards to −34% at passing touchdowns. Fixed by
  `project.healthy_intervals`, from a games-conditional dispersion fitted in the same
  place as the unconditional one. **No mean moves** — `TRUE_Points` is identical to the
  byte on a rebuilt board.

- **The if-healthy *mean* has the same problem and is not fixed.** `to_full_slate` rescales
  proportionally, which is the `e = 1` case the elasticity above rejects. On healthy
  players it lands +15% high at WR, −0.3% at RB and **+41%** at QB. That column is in the
  blend at weight 0.25, so correcting it moves `TRUE_Points` — it is named here and left
  alone, deliberately, days before a draft.

- **Two of nine leagues score per-game bonuses that currently score zero.** The registry's
  `colName` is `rushingYards100-199Game`; ESPN's `projected_breakdown` key, which becomes
  the frame column, is `rushing100To199YardGame`. On `john_pc_league`'s 2026 board
  `ESPN_rushing100To199YardGame` has 242 non-zero rows and `TRUE_rushingYards100-199Game`
  is 0.0 on every one. `coverage_gaps()` cannot catch it — the `colName` is non-null, just
  wrong. Belongs to [01](01-scoring-coverage.md); it is recorded here because this plan
  found it and because a season-total simulation could not price those bonuses anyway.

- **2025's `depth_charts.parquet` is not in nflverse shape.** It carries `dt`, `team`,
  `player_name`, `espn_id` — plan 21's ESPN snapshot — so a `pl.concat` across 2016–2025
  raising `ColumnNotFoundError: week` is the schemas being genuinely different, not a bad
  read. `ctx.load_depth_charts` is the only safe reader.

- **A third of historical rooms have more than one "starter".** The 2016–2024 depth chart's
  rank 1 means *a* starter, not *the* best one — `STARTERS_BY_POSITION` exists for exactly
  this — so **19 to 23 of 64 RB and TE rooms list two or three rank-1 players**, up to
  three. The 2025-onward schema is a strict ordering and has none. Any code that picks a
  room's lead from `depth_rank` alone is picking arbitrarily on a third of the sample, and
  any cohort defined as `depth_rank <= 1` contains players who are in fact understudies.
  Ties are now broken by the model's own projected per-game opportunity, which is the
  quantity that separates a lead back from a committee-mate and is available pre-season.

- **The room ranking was not deterministic, and the numbers moved between runs.**
  `rank("ordinal")` breaks ties in row order, and row order out of a multi-threaded
  `group_by` is not stable — so deep reserves tied at zero or one target swapped places
  and a TE room's volume read 7.145 on one run and 7.136 on the next, with a cohort going
  104 → 103. Sorting on `gsis_id` after the volume makes the ordering total.
  `Scripts/outcomes/evidence.py` now returns byte-identical output across runs, which is
  the difference between a measurement and a draw. **Any future room-rank code should
  copy the tie-break rather than rediscover this.**
- **A skill-position filter silently deleted the players the question was about.**
  Joining availability through `player_seasons` — which filters to `SKILL` — dropped the
  **8 of 217** incumbent lead backs who appeared in *no* game that season, because a
  player with no appearance has no position recorded. Those eight are the entire severe
  tail. It moved the "missed 1–2" bucket from 4.00 games missed to 3.22 and biased every
  bucket toward the conclusion that fragility does not matter. The cohort size was
  unchanged at 217 either way, so nothing about the output looked wrong.

Every figure in this document is reproduced by:

```bash
python -m Scripts.outcomes.evidence                      # the evidence layer
python -m Scripts.outcomes.vacancy --report              # the fitted transfer rule
python -m Scripts.outcomes.backtest --seasons 2021-2025  # G-D0, G-D1, G-D2, G-D3
```

That module is the plan's evidence layer and exists before its model layer on purpose —
it is what phase 0's gate will be argued from, and it stands on its own if the rest of
the plan is rejected.
