# 21 — Depth charts, scheme, and the play-caller problem

**Priority:** High (feeds [18](18-season-usage-model.md) before the draft) ·
**Effort:** M · **Status:** **Done 2026-08-07**, all five steps. `R/GetCoaches.R` +
`Scripts/coaches.py` build the committed coaching table including a 17-season
coordinator crawl; `Scripts/usage/scheme.py` builds team profiles and three coach
priors; depth-chart features are in the feature layer.

**The verdict is narrower than the plan expected.** The **depth chart** earns its place
in the rookie arm and delivers the whole of that arm's improvement. Every coach prior —
head coach, coordinator, or coordinator-else-head-coach — was **measured out of both
arms**, because the depth chart already carries their signal. §Step 5 has the numbers.
The coaching table stays built: it is cheap, it is worth showing on a board, and
[plan 19](19-weekly-usage-model.md) has not been measured against it.
**Feeds:** [18](18-season-usage-model.md) (rookie and veteran volume) ·
[19](19-weekly-usage-model.md) · [09](09-frontend-draft-views.md)

## The question

The rookie arm in [plan 18](18-season-usage-model.md#rookies) predicts a rookie's
volume from draft capital alone. Draft capital says how much a team *invested*; it
says nothing about the situation the rookie is walking into. Two things would:

- **Where he sits on the depth chart** — a third-round back behind an entrenched
  starter is a different projection from the same pick as the presumptive week-1 back.
- **What the offence does with his position** — a back joining a coach who throws to
  backs 26% of the time is not the same asset as one joining a coach who throws to
  them 13% of the time.

The second one has a crux: **usage is a property of the play-caller, and nobody
publishes who the play-caller is.**

## What was found, verified live 2026-08-07

### Depth charts: solved, and better than expected

`load_depth_charts(2026)` refuses with `seasons <= most_recent_season() is not TRUE`
because `most_recent_season()` tracks the *game* season, which stays 2025 until week
1. **The refusal is the guard, not the data.** The release asset holds:

| | rows | snapshots | range |
|---|---|---|---|
| 2026 | 410,431 | 140 | 2026-03-22 → **2026-08-07 08:11** |

Daily snapshots, updated the morning this was checked, with `pos_rank`, `gsis_id`
and `espn_id`. It already lists the rookies — **Jeremiyah Love is RB1 for Arizona,
ahead of Tyler Allgeier and James Conner**, which is exactly the signal the rookie
arm is missing and independently corroborates its high projection for him.

`R/GetContext.R` now reads past the guard via `load_release_asset` for depth charts
only; injuries and snap counts genuinely do not exist before games are played, and
their assets come back empty.

**Two schemas, already recorded.** 2016–2024 are week-keyed (~35k rows a season,
`depth_position`); 2025 onward is a timestamped snapshot log (`dt`, `pos_rank`, no
week). `context_meta.json` carries `depth_charts_shape` so a feature built on the
wrong one fails loudly rather than at predict time.

### Scheme: available as behaviour, never as a label

There is **no scheme, system or offensive-identity field anywhere in nflverse.** What
exists is what the offence did, which is better anyway — a label like "Shanahan
system" is an argument, a personnel rate is a measurement.

| Source | Seasons | What it gives |
|---|---|---|
| `load_pbp` | 2016–2025 | `pass_oe` (pass rate over expected), `xpass`, `shotgun`, `no_huddle`, `qb_dropback` |
| `load_participation` | 2016–2025 | `offense_formation`, `offense_personnel` — the 11/12/21 personnel groupings |
| `load_ftn_charting` | **2022–2025 only** | `is_motion`, `is_play_action`, `is_screen_pass`, `is_rpo`, `n_offense_backfield` |

None of it exists for 2026, because no games have been played. So scheme is a
**trailing** feature pre-season, the same constraint availability has.

### Coaches: the head coach is free and complete; the play-caller is not

| Level | Source | Coverage |
|---|---|---|
| **Head coach** | `load_schedules` | **Complete.** 2010–2026, 384 team-seasons 2015–2026, 97 distinct coaches, and 2026 is populated |
| **Offensive coordinator** | Wikipedia `<year>_<Team>_season` infobox | **Patchy.** 2025 Arizona's article carries no `off_coach` at all; 2026 Arizona's carries all three |
| **Play-caller** | — | **Does not exist as a structured free source** |

Two practical notes on the Wikipedia route. It is **rate-limited** — 40 rapid
requests returned `HTTP 429`, so a 32-team × 17-season crawl needs ~1s spacing and
backoff, roughly ten polite minutes. And the two sources **disagree on at least one
2026 head coach**: `load_schedules` says Arizona's is Jonathan Gannon, Wikipedia's
2026 article says Mike LaFleur. That needs resolving before either is trusted as the
key for a coach-level aggregate.

## The measurement that says this is worth building

288 consecutive team-season pairs, 2016–2025 (216 with the same head coach, 67 after
a change). Year-over-year correlation of team usage:

| Metric | all | coach stayed | coach changed | gap |
|---|---|---|---|---|
| QB carry share | 0.661 | **0.719** | 0.363 | +0.356 |
| TE target share | 0.466 | **0.530** | 0.176 | +0.354 |
| RB carry share | 0.641 | **0.693** | 0.405 | +0.287 |
| WR target share | 0.410 | **0.451** | 0.170 | +0.281 |
| RB target share | 0.425 | **0.468** | 0.210 | +0.258 |
| Pass rate | 0.417 | **0.451** | 0.255 | +0.196 |

**Persistence roughly halves when the head coach changes, on every metric.** That is
the hypothesis — usage is a coach property, not a franchise property — and it holds
without needing coordinator data to see it.

The coach-level spread is large enough to matter on a draft board. Over 48 coaches
with 3+ team-seasons, mean RB target share runs **0.132 (Sean McVay) to 0.263
(Anthony Lynn)** — a factor of two. And the between-coach standard deviation (0.030)
is 70% of the standard deviation across all team-seasons (0.043), so most of the
variation in how much a team throws to its backs is *between* coaches rather than
within them. Face validity holds: McVay lowest, Belichick and Sean Payton highest,
which is what those offences are known for.

### How big is the play-caller-shaped hole?

The head coach staying is not the play-caller staying. Measuring how much RB target
share still moves when the head coach *did* stay:

| | n | mean absolute shift | shifted > 1 league sd |
|---|---|---|---|
| coach stayed | 216 | 0.0344 | 64 (**30%**) |
| coach changed | 67 | 0.0367 | 24 (36%) |

Two things fall out, and the second is the more interesting.

**A real residual remains.** Three in ten team-seasons move more than a league
standard deviation with the same head coach, so head-coach identity alone will be
confidently wrong about a meaningful minority — and that is the population
coordinator data would catch.

**But a coaching change does not make usage more volatile; it makes it
unpredictable.** The mean absolute shift is nearly identical either way (0.0344
against 0.0367) while the correlation collapses (0.468 → 0.210). A coaching change
scrambles the *ordering* rather than widening the *spread*. That argues for using a
coach change as a signal to fall back to a positional prior, rather than to widen an
interval around a point estimate that has become meaningless.

## Proposed design

**Do not try to source the play-caller.** It is not available, and the two things it
would be used for are separately obtainable:

1. *Attributing tendency to a person so it travels when they move* — head coach does
   most of this, measurably, and is free and complete.
2. *Knowing when the tendency broke* — derivable from the behaviour itself, without
   a name.

### Step 1 — `R/GetCoaches.R`

Head coaches per team-season from `load_schedules`, 2010 onward, to
`Data/NFL/coaching_staff.parquet`. **Committed**, not gitignored: small,
hand-auditable, and the same argument that keeps `player_ids.parquet` in git.
Resolve the Gannon/LaFleur disagreement before trusting 2026.

### Step 2 — `Scripts/usage/scheme.py`

Team-season usage profile from `player_weeks` (already on disk, no new pull): pass
rate, and RB/WR/TE target and carry shares. Then a **coach-level prior**: the mean
profile over that coach's prior seasons, shrunk toward the league mean by seasons
observed — the same credibility weighting the efficiency features use. Strictly
prior seasons, so the as-of guarantee holds.

### Step 3 — depth-chart features

From the pulled snapshots, as of the most recent snapshot before the season starts:
`pos_rank` within position and team, and the count of players ahead. For a rookie
this is the single most informative situational fact available, and plan 18's rookie
arm currently cannot see it.

### Step 4 — feed plan 18 and measure

**Done, and the answer was asymmetric.**

**The rookie arm keeps them.** Adding `is_first_string` and the coach's positional
volume prior, on the same walk-forward:

| Pos | ρ before | ρ after | MAE before | MAE after |
|---|---|---|---|---|
| QB | 0.6024 | **0.6593** | 26.73 | **23.84** |
| RB | 0.6175 | **0.6450** | 21.58 | **20.58** |
| WR | 0.6141 | **0.6331** | 14.06 | **13.51** |
| TE | 0.6186 | 0.6037 | 9.81 | **9.64** |

MAE improves at all four positions and ordering at three. That is what a rookie
projection was missing: draft capital says what a team invested, the depth chart and
the coach prior say what he is walking into.

**The veteran arm rejects them.** The same two features moved within-position Spearman
by at most 0.0012 in either direction, traded −0.3% MAE on receiving for +0.2% on
rushing and passing, and made **top-N hit rate worse at three of four positions** —
QB 0.607 → 0.595, WR 0.671 → 0.663, TE 0.512 → 0.488. Reverted.

The reason is the obvious one in hindsight: a veteran's own prior volume already
encodes his situation, so the coach prior is redundant with it and only adds
parameters. A rookie has no prior volume for it to be redundant with. Recorded as
`VETERAN_SITUATIONAL_REJECTED` in `Scripts/usage/season.py` so the negative result is
not re-discovered.

## Results

Two schema traps had to be fixed before any of this measured anything, and both would
have degraded the 2026 prediction while looking fine in training.

**The two depth-chart schemas do not share a rank scale.** 2016–2024 carry
`depth_team`, which is only ever 1, 2 or 3; 2025 onward carry `pos_rank`, which runs
to 15. Ranks are clipped to the coarse scale, which loses granularity and is the only
version that transfers.

**They do not share the *meaning* of rank 1 either, which is worse.** The old feed
marks every starter in a three-receiver set as first string — measured, an average of
**3.0 rank-1 receivers per team** in 2024 against 0.97 quarterbacks. The new feed is a
strict ordering: exactly 1.0 at every position. A model trained on the former and
applied to the latter demotes every WR2 and WR3 in the league. The new schema is
shifted onto the old one's meaning via a per-position starter count, after which the
two agree (QB 0.97/1.0, TE 1.16/1.0, WR 3.0/3.0).

A third bug was in my own code: deduplicating the snapshot on `gsis_id` before
filtering to fantasy positions dropped backs and receivers who are also listed as kick
returners, because `KR` sorts before `RB`.

### Step 5 — the coordinator crawl. Done, and it did not earn a place in the model

Crawled all 17 seasons: 544 team-seasons, 32 articles found every year, one batched
request per 20 titles. **Coverage is 49%** — 266 of 544 team-seasons carry an
`off_coach`, consistently 11 to 18 of 32 a year.

**It does fill the gap it was done for.** Of the six 2026 first-year head coaches,
four have offensive-coordinator history:

| Team | Head coach | OC history |
|---|---|---|
| ARI | Mike LaFleur | Rams 2023, 2024, 2025 |
| CLE | Todd Monken | Baltimore 2023, 2024, 2025 |
| LV | Klint Kubiak | New Orleans 2024, Seattle 2025 |
| BUF | Joe Brady | Buffalo 2024 |
| BAL | Jesse Minter | none — he is a *defensive* coordinator |
| MIA | Jeff Hafley | none — likewise |

The two misses are both defensive coaches, so having no OC record is correct rather
than a coverage failure.

**And coordinators separate more sharply than head coaches.** Over the 3+-season
population, mean RB target share spans 0.112–0.251 for coordinators with a standard
deviation of 0.0388, against 0.132–0.263 and 0.0300 for head coaches, on an
all-team-season deviation of 0.0427. The coordinator is closer to the thing that moves
usage — as you would expect, and it is nice to see it measured.

**None of which improved the model.** All three priors were tried on the rookie arm
against a version carrying only the depth-chart feature. Mean within-position Spearman
across QB/RB/WR/TE:

| Rookie arm | mean ρ |
|---|---|
| **depth chart only** | **0.6403** |
| + offensive-coordinator prior | 0.6367 |
| + offensive-lead prior (coordinator else head coach) | 0.6366 |
| + head-coach prior | 0.6353 |
| *draft capital alone, for reference* | *0.6132* |

**So the whole of the rookie arm's improvement is the depth chart**, and every coach
prior is a small net loss on top of it — worst at tight end, where the head-coach
prior costs 0.026. This corrects §Step 4 above, which credited the gain to the depth
chart and the coach prior together; separating them showed only the first earns it.
Recorded as `COACH_PRIOR_REJECTED` in `Scripts/usage/season.py`.

Why it fails is worth stating, because it is not that the coach signal is absent —
§The measurement shows it plainly. It is that **the depth chart already contains it.**
ESPN's editors set a depth chart knowing the scheme; a back listed first on a
run-heavy team is exactly the player the coach prior would have flagged. The prior is
redundant with a more direct measurement of the same thing.

The coordinator data stays built and committed regardless: it costs nothing to carry,
it is worth showing on a board beside a projection, and [plan 19](19-weekly-usage-model.md)
has not been measured against it.

**Two free by-products of the crawl.** Wikipedia's head coach agrees with nflverse's
on **98.0%** of played team-seasons (502 of 512), and after one parsing fix *every*
remaining disagreement is a season with `coach_changed_midseason` — nflverse reports
the replacement, the article names the starter. Two correct answers to different
questions, and a cheap validation of the whole table. The parsing fix was itself found
this way: stripping HTML before splitting on `<br>` removes the separator and welds
two names into one, which is how "John FoxJack Del Rio" became a head coach.

### Game script and team strength — measured 2026-08-07, and it goes the same way as the coach prior

The narrative: good teams lead, run to bleed clock, and feed the backfield; bad teams
trail and throw. Tested against 5,198 team-games 2016–2025, joined to Vegas lines from
`nflverse/nfldata`.

**The narrative is true, and large — after the fact.**

| realised margin | pass rate | rush attempts |
|---|---|---|
| lost by 14+ | 0.632 | 20.7 |
| lost 4–14 | 0.615 | 23.0 |
| within 3 | 0.563 | 26.8 |
| won 4–14 | 0.503 | 30.6 |
| won by 14+ | 0.477 | 32.9 |

A **12-carry swing** between blowout loss and blowout win; `corr(pass rate, realised
margin)` = **−0.494**.

**The forecastable slice is much smaller.** Against the *pregame* spread the same
correlation is **−0.117**, because game outcomes are mostly not knowable in advance.
By spread bucket, team rush attempts run 24.6 (underdog 7+) to 28.9 (favourite 7+) —
a 4.3-attempt spread, versus the 12 available with hindsight.

**At season level it aggregates into something real.** `corr(season rush attempts,
mean pregame spread)` = **+0.332**, monotone across quintiles: 411.7 rush attempts for
the weakest teams against 465.0 for the strongest, a 53-carry difference.

**And the RB2 hypothesis is right.** The extra volume on a good team goes to the
backup, not the starter:

| team strength | RB1 carries | RB2 carries | RB2 share |
|---|---|---|---|
| weak | 203.9 | 84.0 | 0.244 |
| average | 213.5 | 96.6 | 0.264 |
| strong | 213.2 | **103.0** | **0.280** |

RB1 is flat at ~213; RB2 gains **+19 carries**. A strong team's handcuff is worth
materially more than a weak team's, which is a real draft-board fact.

**It still does not belong in the season head.** The gate is the same one the coach
prior failed — does it add anything over prior-season volume?

| predicting next-season RB carries (n=622) | R² |
|---|---|
| prior carries/game + games | **0.5012** |
| + next-season team strength | 0.5012 |
| + change in team strength | 0.5021 |
| + both | 0.5027 |

**+0.0015.** Two reasons, and the second is the interesting one:

1. Team strength is **61.6% persistent** year over year, so next season's strength is
   largely already encoded in last season's volume.
2. **The signal survives at team level and dies at player level.** Predicting a
   *team's* rush attempts, adding strength moves R² from 0.2012 to **0.2649** — a 32%
   relative gain, clearly real. Predicting a *player's* carries it vanishes, because
   player-level variance is dominated by role — who starts, who is hurt, who was
   drafted over — and a ±30-carry team effect is noise against a 0-to-300 role range.

That second point suggests the one structure where it might pay, untested: **predict
team volume with strength, then allocate by depth chart**, rather than predicting
player volume directly from player history. That is a different architecture, not a
feature, and it should be measured before being believed.

**Where it should go instead is [plan 19](19-weekly-usage-model.md).** Weekly, the
spread for that specific game is known, the 4.3-attempt swing applies to one game
rather than being averaged away, and there is no prior-season volume already carrying
it. The user's instinct that it matters more week to week is what the numbers say.

Worth recording separately: **every 2026 game already has a spread**, all 272 of them,
in `Data/NFL_Schedules.csv`. Team strength for the season being drafted needs no
win-totals scrape.

#### The one part that shipped: handcuff value on the board

`Scripts/draft/handcuff.py`. Since the RB2 effect is real and invisible in a
projection, it is surfaced on the board rather than fed to the model. Every board now
carries `team_strength`, `backfield_rank`, `handcuff_carries`, `handcuff_premium` and
`handcuff_r2`.

Backfield rank comes from **the board's own projection**, not from a depth chart, so
the back a given league projects second is the back that league would be handcuffing —
and it stays consistent with everything else on the page across all nine leagues.

The relationship is fitted at build time over 315 team-seasons rather than hardcoded:
`RB2 carries = 94.5 + 1.65 × strength`. `Data/NFL/schedules.parquet` (2016–2026, 41 KB,
from `nflverse/nfldata`) makes that reproducible.

**`handcuff_r2` ships on every row on purpose.** It is 0.030, against a residual
standard deviation of 36 carries, so the premium spans roughly ±13 carries — about 55
rushing yards. A reader who sees "+10 carries" without that number will price it as a
projection. It is a tiebreaker between two similar backups, not a reason to move
anyone up a round.

2026's extremes: Jordan James (SF, +6.2 strength) at **+10.0 carries**, against Tyler
Allgeier (ARI, −11.0) at **−18.3**.

Two things the column had to be stopped from doing, both pinned by tests: attaching
itself to starters or non-backs, and inventing a thirty-third backfield out of free
agents — ESPN gives an unrostered player a `pro_team` of the literal string `"None"`,
which otherwise gets its own RB1 and RB2 and a handcuff to a team that does not
exist.

### Vacated opportunity share — measured 2026-08-08, and it makes three

Built to answer a specific critique: the model leans on last year's volume, and for a
player who changed teams that volume was earned in a different offence. `team_changed`
is one blunt coefficient. The proposed fix was a **vacated share** — the fraction of a
team's prior-season targets or carries whose owner is no longer on the roster, which is
the opportunity a new arrival walks into.

The feature is real and has spread. On 2025: Green Bay retained **100%** of its 2024
target volume, Pittsburgh lost **51.8%**, median 26.6%.

It predicts nothing. Train 2020–2023, test 2024–2025, next-season total targets:

| | n | base | + vacated | Δ |
|---|---|---|---|---|
| all players | 599 | 0.6578 | 0.6578 | **+0.0000** |
| changed teams only | 122 | 0.4996 | 0.5014 | +0.0018 |

And **not because the depth chart already carries it**, which was the obvious
explanation and is wrong: adding it to a base *without* the depth chart gains +0.0004,
and `corr(vacated, depth_rank)` is **−0.009**. The two are unrelated. The feature
simply has no player-level signal.

**That is the third team-level context feature to fail the same way**, and the pattern
is now the finding:

| feature | team level | player level |
|---|---|---|
| coach prior | large between-coach spread (RB target share 0.132–0.263) | ~0.001 Spearman |
| team strength | **+0.064 R²** on team rush attempts | +0.0015 |
| vacated share | 0 to 51.8% spread across teams | **+0.0000** |

**Team-level context does not survive to player level, because role variance dominates
it.** Who starts, who is hurt, who was drafted over — that range is 0 to 300 carries,
against team effects worth tens. The only feature that has ever moved this model is the
one that resolves *role*: the depth chart, worth +0.048 R² on veteran carries. Three
independent attempts to add situational context have now confirmed the same boundary.

#### A finer depth rank does not help either

Checked at the same time, since role resolution is the thing that works. `depth_rank`
is clipped to 3 because the two upstream schemas disagree, and 589 of 909 players on
the 2026 chart sit in that bucket — an obvious place to look for granularity.

2025 has both a fine pre-season chart and a realised outcome, so it is testable.
Explaining 2025 targets from the 2025 pre-season chart alone:

| | R² |
|---|---|
| clipped rank 1–3 (what the model uses) | **0.4175** |
| fine rank 1–6 | 0.2537 |

The fine scale is *worse*, and the mean targets by fine rank say why — 61.9, 24.0,
16.2, 10.5. The drop from first to second string is enormous and everything below
flattens, so a linear term on the fine scale fits that curve badly while the clip
approximates it well. The clip was adopted as a schema workaround and happens to be the
better functional form. Anything finer would need bins or a log, not a longer scale.

### Deferred, with reasons
- **`load_ftn_charting`** (motion, play action, RPO, backfield count) is the richest
  scheme data and starts only in 2022. Four seasons is thin for a walk-forward that
  trains from 2016. Revisit once it has more history.

## Risks

- **Coach as a key is unstable.** Mid-season firings mean a team-season can have two
  head coaches; `load_schedules` gives one per game, so the aggregate has to pick
  (modal coach) and record that it did.
- **Attributing team usage to the head coach over-credits him.** The measured gap is
  real but it bundles the coordinator change that usually accompanies a head-coach
  change. The honest claim is "coaching-staff continuity", not "the head coach's
  preference", and the feature should be named for what it measures.
- **Depth charts pre-season are a projection, not a fact.** They are ESPN's editorial
  view, they move daily, and camp changes them. The 140 snapshots make that visible —
  a player whose rank has moved recently is less certain than one who has held it
  since March, which is itself a usable feature.
- **Survivorship in coach means.** A coach with 8 seasons kept his job; his profile
  is not a random draw from coaches. Fine for a prior on *his* next season, wrong for
  inferring what a first-year coach will do.
