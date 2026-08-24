# 32 — Movers: what a player does on a new team

**Priority:** Medium · **Effort:** S–M · **Status:** Evidence measured 2026-08-24, not started
**Depends on:** [18](18-season-usage-model.md) (the season head) ·
[22](22-feature-research.md) (the contract pull this reuses)
**Feeds:** [19](19-weekly-usage-model.md) · [31](31-team-coherent-tomcat.md)

---

## Problem

**TOMCAT has no team-specific opinion about a veteran who changed teams.** For a
2026 WR the veteran arm's regressors are `p1_volume, p2_volume, p1_games,
team_changed, age, depth_rank, is_first_string`. Exactly two of those know anything
about the destination: a **flat** `team_changed` coefficient — −0.399 targets/game,
applied identically to all 49 veteran WRs who moved, wherever they went — and his
depth-chart slot on the new team.

The new team's pass volume, its target pool, its quarterback and its scheme are all
joined onto the frame (`team_prior_*`, `coach_*`, `lead_*`) and then deliberately
excluded from the veteran arm; see `VETERAN_SITUATIONAL_REJECTED` in
`Scripts/usage/season.py`. That exclusion was measured, but it was measured **pooled
over all veterans**, where movers are a minority. Whether it survives on movers alone
was an open question. This plan closes it, and finds the win somewhere else entirely.

DJ Moore is the worked example. Chicago → Buffalo at 29, and his projection is
`p1_volume` 5.00 carried forward almost unchanged:

| term | value | contribution |
|---|---|---|
| `p1_volume` | 5.00 | **+3.301** |
| `age` | 29.4 | −3.109 |
| intercept | | +3.041 |
| `is_first_string` | 1 | +1.039 |
| `p2_volume` | 8.24 | +0.628 |
| `p1_games` | 17 | +0.474 |
| **`team_changed`** | 1 | **−0.399** |
| `depth_rank` | 1 | +0.025 |

→ **5.00 targets/game**, 679 receiving yards on a full slate.

## Evidence

Panel: `Scripts.usage.season.training_frame(2017…2025)`, 8,552 player-seasons.
Movers with a prior season and 4+ games played: **335 WR, 222 RB, 170 TE, 113 QB**.
Every model number below is walk-forward — trained on seasons strictly before the
test year, scored on that year's movers, pooled over test seasons 2020–2025.

### The mover penalty is real, and it is mostly a quarterback effect

Median change in the position's volume stat from the prior season:

| position | movers | stayers Δ | movers Δ | gap |
|---|---|---|---|---|
| **QB** (attempts/g) | 113 | +0.31 | **−1.92** | **−2.24** |
| WR (targets/g) | 335 | −0.05 | −0.50 | −0.45 |
| RB (carries/g) | 222 | −0.04 | −0.30 | −0.26 |
| TE (targets/g) | 170 | +0.00 | −0.14 | −0.14 |

A quarterback who changes teams is usually losing a job rather than taking one, and
the effect is five times the size of the receiver version. At WR it also **widens
with age** — the gap is +0.05 at 25–26, −0.54 at 29–30 and −0.84 at 31+ — against a
flat coefficient that is roughly the pooled average of all of them.

### The destination's target pool: a real bias that cannot be fixed with it

The model **over-projects a WR moving into a smaller receiving pool**, walk-forward
mean signed error:

| group | n | bias | se |
|---|---|---|---|
| moved into a **smaller** WR pool | 91 | **+0.53** | 0.18 |
| moved into a **bigger** WR pool | 68 | −0.27 | 0.20 |
| all movers | 224 | +0.20 | 0.11 |

Positive in **5 of 6 seasons**, so the bias is real. An 0.80 targets/game spread
between the ends, and the model corrects none of it.

**And it does not respond to the obvious feature.** Adding `moved × pool_delta`
leaves the bias at +0.49, and its fitted coefficient **flips sign across folds**
(−0.025, −0.027, +0.012, +0.019, +0.025, +0.032). A mover-only arm — fitted
exclusively on movers, with the delta available — is **worse** than the pooled fit
and still leaves the bias at +0.47.

The cause is that **a team's receiving pool is barely persistent**:

* `rho(team WR pool, same team next season) = **+0.285**` (n = 253)
* cross-team sd **5.18** targets/g; year-to-year change sd **6.18**

The noise is bigger than the signal, so last season's pool is a broken instrument
for this one. Confirmed by giving the model an oracle on the destination's *actual*
pool that season:

| | movers MAE | vs current | rho |
|---|---|---|---|
| current model | 1.3254 | — | 0.671 |
| + last year's pool (knowable) | 1.3264 | −0.1% | 0.670 |
| **+ this year's pool (oracle)** | **1.2911** | **+2.6%** | **0.691** |

So the signal exists, it is worth ~2.6% with **perfect** knowledge, and it sits behind
a forecasting problem — projecting a team's passing distribution — harder than the one
it would solve. This is also why the human sources beat TOMCAT here: ESPN and
FantasyPros read camp reports in August. It is not a gap a regressor closes.

### The win is a longer lookback, and it is at quarterback

The model looks back exactly two seasons. Adding a **3- and 5-year peak and mean**,
built from the panel's own history:

| feature set | QB movers MAE | vs current | rho | all-QB MAE |
|---|---|---|---|---|
| current model | 7.8371 | — | 0.456 | 6.1344 |
| **+ 3yr peak & mean** | **7.4807** | **+4.5%** | **0.531** | **6.0784** |
| + 5yr peak | 7.4542 | +4.9% | 0.490 | 6.0788 |
| + moved × 3yr peak | 7.4296 | +5.2% | 0.487 | 6.0404 |

Better in **5 of 6 folds** (the sixth is a 0.01 tie), and the coefficient is stable in
sign and size across folds — `p3_peak` +0.298 / +0.181 / +0.221, against `pool_delta`
flipping. Most striking: **the three-year peak outweighs last season.** On the 2025
fold `p3_peak` is +0.221 where `p1` is +0.061 and `p2` +0.144.

The mechanism is that quarterback usage is close to binary. Last season's attempts
confound "lost the job for eight weeks" with "is not a starter", and a multi-year peak
separates them. **It is not a mover feature** — it improves all quarterbacks by ~1%,
and the mover gain is a consequence.

### Contract value is a mover-specific signal at receiver

`Data/NFL/contracts.parquet` (OverTheCap, already pulled by plan 22) covers **86% of
WR movers** and carries `apy_cap_pct` — what the new team paid, which is the market's
own forecast of role, and knowable in August.

| position | feature | movers MAE | vs current | all-position MAE |
|---|---|---|---|---|
| WR | + `apy_cap_pct` | 1.2942 | **+2.4%** | 1.2204 → **1.2390** (worse) |
| WR | + `moved × apy` | 1.3139 | +0.9% | 1.2380 |
| RB | + `apy_cap_pct` | 2.6912 | +0.9% | 2.4307 |
| TE | + `apy_cap_pct` | 0.9392 | −0.2% | 0.9054 |

It helps movers and **hurts everyone else**, which is the signature of a feature that
belongs behind an interaction rather than in the main effect. The clean form is not
yet identified: the plain term wins on movers, the interaction wins overall.

### What is not measurable today — **resolved, see the note**

The destination-quality idea — is he going somewhere better? — wants pressure rate and
an offensive-line grade. When this plan was written the repo had neither:
`Data/NFL/<season>/player_weeks.parquet` carried `sacks_suffered` and `def_qb_hits`,
and `ADVANCED_FILES` was `routes`, `ngs`, `red_zone`. No pressures, no hurries.

> **Resolved 2026-08-24 by `R/GetPBP.R`.** `Data/NFL/<season>/pfr_pass.parquet` now
> carries `times_pressured`, `times_pressured_pct`, `times_blitzed`, `times_hurried`,
> `times_hit` and `times_sacked` per quarterback per game, and `pfr_rush` carries
> yards before and after contact. Phase 3 is unblocked.
>
> **Two constraints came with it.** The feeds start late — PFR in **2018**, FTN in
> **2022** — against a walk-forward that trains from 2016, so a model using them sees
> pressure data for part of its window and not the rest. That is the same coverage
> objection `GetAdvanced.R`'s header raised when it declined to pull them, and it is
> now a gate's problem rather than a reason not to hold the data. And they key on
> **PFR's** player id: `Scripts.crosswalk` maps `gsis_id` to ESPN and stops there, so
> joining them to anything here needs a crosswalk hop that does not exist yet.
>
> Full play-by-play is archived for **1999-2025** besides, so a team sack rate per
> dropback — the O-line proxy this section called for — is a `group_by` rather than a
> pull.

## Fix

Three phases, each independently shippable and gated.

### Phase 1 — a longer lookback for the volume heads

Add `p3_peak`, `p3_mean`, `p5_peak` to the feature layer as first-class lagged terms,
and to `VOLUME_REGRESSORS` **at quarterback only** unless the other positions clear
their gate. Half a day: the panel already proves it out, and this is a features change
rather than a new arm. Note it is not a mover fix — it is a quarterback fix that shows
up largest on movers.

### Phase 2 — contract value, gated to movers

Add `apy_cap_pct` behind a `team_changed` interaction, at WR first. A day, most of it
resolving the join: contracts key on `gsis_id` + `year_signed`, and a player who
re-signed with his own team must not read as a mover.

### Phase 3 — destination quality (research, not yet a build)

The pull has landed — `R/GetPBP.R`, 2026-08-24 — so this is now a research spike
rather than a data task. Build a team sack-rate-per-dropback proxy from the
play-by-play archive, bring in `pfr_pass`'s pressure columns behind a `pfr_player_id`
crosswalk, and test whether *quarterback* and *line* quality at the destination
predict a receiver's usage where the target pool did not. Gate it on its own G-M5
before any of it enters a fitted arm — the pool result says destination features are where
this kind of idea goes to die, and the oracle bound says the whole category is worth at
most a few percent.

### Do not build

| | Why |
|---|---|
| Destination target pool as a linear regressor | Measured dead six ways. The coefficient flips sign across folds and the pool is `rho = +0.285` autocorrelated — last year's pool does not predict this year's |
| A dedicated mover-only arm | Built and measured: **−0.7%** against the pooled fit, and it does not remove the bias it exists to remove. 113–335 movers per position is not enough to re-fit seven coefficients |
| An age × `team_changed` interaction | The age gradient is real descriptively (+0.05 at 25–26, −0.84 at 31+) and **−0.4% out of sample** |

## Gates, pre-committed

**G-M1 — the lookback must pay at quarterback.** Walk-forward 2020–2025, QB movers.
**Bar: ≥ +3% MAE and no fall in all-QB MAE.** Measured at +4.5%; the bar exists so a
re-measurement on the real feature layer, rather than the panel reconstruction, has to
reproduce it.

**G-M2 — it must not cost the other positions.** Same sweep at RB, TE, WR. **Bar: no
regression.** Measured: RB −0.6%, TE +0.2%, WR −0.1% — so on current evidence the
lookback ships at **quarterback only**, and G-M2 is what keeps it there.

**G-M3 — contract must survive the interaction.** **Bar: +2% on WR movers *and* no
regression on all WRs.** The plain term fails the second half today (1.2204 → 1.2390),
which is exactly why this is gated rather than shipped.

**G-M5 — destination quality must clear the coverage objection, not dodge it.**
PFR starts in 2018 and FTN in 2022, so any feature built on them is measurable on at
most 8 of the 10 training seasons. **Bar: the walk-forward gain must survive being
scored only on folds where the feature is actually present**, rather than on a window
where its absence is filled and the fill carries the result. This is the gate the data
being collected turned from an assertion into a question.

**G-M4 — nothing here may move the blend.** Re-run `python -m Scripts.usage.g1_season`.
**Bar: no regression on TOMCAT's contribution at weight 0.25.**

**And a scope clause:** the bias in *§the destination's target pool* is real and stays
unfixed after this plan. That is deliberate. Three separate attempts to correct it made
the model worse, and the honest read-around until someone forecasts team pass
distributions is **discount a TOMCAT mover heading to a low-volume passing offence, and
note that the model already flags him** — `usg_thin_evidence` is True with
`usg_evidence = "changed teams"`, and his interval is wider (1.90 relative width against
1.68 for stayers).

## Effort

S–M. Phase 1 is half a day and carries the measured win. Phase 2 is a day, mostly join
work. Phase 3 is a data pull plus a research spike and should not be started until
phases 1 and 2 have cleared their gates.

Not urgent and **not before the 2026 drafts** — every phase moves `USG_` and therefore
`TRUE_`, and the board is frozen; see [`DRAFT_READINESS.md`](../DRAFT_READINESS.md).
