# 21 — Depth charts, scheme, and the play-caller problem

**Priority:** High (feeds [18](18-season-usage-model.md) before the draft) ·
**Effort:** M · **Status:** Sourcing settled 2026-08-07; depth charts already pulled,
the rest not built
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

Add to the rookie arm: depth-chart rank, and the coach prior for the rookie's
position. Add to the veteran arm: the coach prior and a coach-change flag. Then
re-run `python -m Scripts.usage.backtest` — it is the regression test, and the arm
only keeps a feature that moves the numbers.

### Deferred, with reasons

- **Wikipedia coordinator crawl.** Worth doing, but it is a ten-minute rate-limited
  crawl with patchy coverage, and step 2 gets most of the signal without it. Do it
  when the head-coach features have been measured, so its marginal value can be
  measured rather than assumed.
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
