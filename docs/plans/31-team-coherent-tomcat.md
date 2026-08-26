# 31 — A team plays seventeen games: making TOMCAT team-coherent

**Status:** IN PROGRESS

**Priority:** High · **Effort:** M · **Where it stands:** **Phases 1–2 built on `feat/team-coherent-tomcat`** — phase 1 shipped 2026-08-24 and missed G-T2; phase 2 (2026-08-26) clears it at +0.0305 on a +0.02 bar and takes G-T0's QB-games half from 1 of 32 to 32 of 32, though its separation from phase 1 is inside the noise at n=92. G-T3 is owed and phase 3 is open.
**Depends on:** [18](18-season-usage-model.md) (the season head) ·
[28](28-outcome-distributions.md) (the redistribution evidence this needs)
**Feeds:** [19](19-weekly-usage-model.md) · [03](03-projection-source-coverage.md)

---

## Problem

TOMCAT projects each player independently. Nothing in the model knows that the players
share a roster, so the team-level accounting identities it implies are wrong — often
badly.

**A team cannot throw for 5,652 yards and catch 3,718.** On the 2026 board:

| source | `Σ receivingYards / Σ passingYards` | teams off by >5% |
|---|---|---|
| ESPN | 1.000 – 1.000 | **0 / 32** |
| FantasyPros | 0.979 – 1.206 | 22 / 32 |
| **TOMCAT** | **0.658 – 1.704** | **23 / 32** |
| `TRUE_` (the blend) | 1.000 – 1.000 | 0 / 32 |

`reconcile_team_totals` already repairs the blend, deliberately touching `TRUE_` only so
that `points_delta` against ESPN stays a real comparison. The board is therefore
consistent and this is not a live scoring bug. It is three other things:

1. **TOMCAT's own column is unreconciled**, so reading its dissent from the market is
   partly reading arithmetic — see *What it costs* below.
2. **The blend's repair is doing far heavier lifting than it was designed for.** It was
   built to close a 0.80–1.23 spread; it is absorbing 0.66–1.70.
3. **The error is in the model, so every downstream use inherits it** — G1, G2, the
   weekly head, and any future use of `USG_` on its own.

## Evidence

### The root cause is a missing snap budget, not a missing identity

The identity is the symptom. The cause is that **expected games are estimated per player
with no constraint that a team plays seventeen of them and fields one quarterback at a
time.** Summing TOMCAT's `usg_expected_games` across each team's quarterbacks:

| | teams | mean QB-games projected |
|---|---|---|
| Two QBs projected | 2 | **21.0** |
| One QB projected | 30 | **12.5** |
| Over 17 | 2 | — |
| Under 13 | 13 | — |

`rho(team QB-games, team passing yards) = **+0.739**`. A team's projected passing volume
is largely decided by how many quarterback-games TOMCAT happens to have projected, which
is an artefact of who cleared the coverage threshold rather than a fact about football.

Both failure directions are the same bug:

* **Atlanta** — Tua 3,634 + Penix 2,404 = **6,037** passing yards, from 21.9 QB-games.
  ESPN splits the same room 2,171 + 1,884 = 4,055. TOMCAT projects each quarterback as
  though he were most of a starter and adds them.
* **Cleveland** — Shedeur Sanders alone at **1,938** passing yards. No NFL team has
  thrown for under 2,800 in a 17-game season.

> **Corrected 2026-08-24, in implementation.** This plan attributed Cleveland to
> Sanders' 10.2 expected games, with the other 6.8 games' volume leaving the team.
> **That is the mechanism in the backtest and not on the board.**
> `Scripts.usage.project.to_full_slate` divides each player's own `expected_games`
> back out before the parquet is written, so every shipped `USG_` line already
> describes seventeen games. Cleveland's number is 16.7 attempts a game at 6.8 yards
> an attempt over a full slate — the model shrinking an unproven rookie toward a
> positional baseline. No games term is involved.
>
> The consequence is not cosmetic: phase 1 as written below **multiplies a line that
> is already on a full slate**, so it does not lift a short room to seventeen games,
> it pushes it past them. Miami — whose only projected passer is Malik Willis at 3.7
> expected games, the starter being unprojected — went to **8,268 projected passing
> yards** against an all-time record of 5,477. See *What shipped*.

Atlanta's failure is the one that survives intact, and it is the full-slate double
count: **every projected quarterback is priced for seventeen games and a team fields
one at a time.** The two teams with two quarterbacks projected are exactly the two
teams over the line.

### Roster incompleteness is not the explanation

The obvious alternative — TOMCAT just does not project enough pass-catchers — does not
survive. `rho(roster coverage, identity ratio) = +0.317, p = 0.077`, and the extremes
contradict it outright: **Las Vegas is at 0.66 with 100% of its pass-catchers priced**,
while **Cleveland is at 1.70 while missing two of eight**. Incompleteness cannot produce
a ratio *above* one at all.

### What it costs

The identity has two ends, so a team whose receivers are over-projected has a
quarterback correspondingly under-projected. Position-adjusted TOMCAT-vs-market
disagreement, correlated against the player's own team ratio:

| position | rho | n |
|---|---|---|
| **QB** | **−0.502** | 34 |
| RB | +0.409 | 75 |
| WR | +0.217 | 121 |
| TE | −0.001 | 60 |

At quarterback that is half of the model's apparent opinion. Joe Burrow is the clean
case: Cincinnati sits at 1.195 and TOMCAT drops him from the market's QB4 to QB20.
Carnell Tate and Wan'Dale Robinson both ride Tennessee's 1.270 the other way.

## Fix

Three phases, cheapest first, each gated. **Phase 1 is the null hypothesis and may be the
whole answer** — see the false-positive clause.

### Phase 1 — reconcile inside the model — **shipped**

Apply `reconcile_team_totals`' midpoint scaling to `USG_` before it reaches the blend,
and normalise each team's quarterback-games to the slate. Half a day. It makes the column
honest and nothing more: the shares stay wherever the model put them.

**As built** — `Scripts/usage/coherence.py`, called from `_make_usage_coherent` in
`Scripts.season_projections` and from `Scripts.usage.g1_season` so the gate measures
what ships. Two passes, room first, because a midpoint taken against a double-counted
room is still too high (Atlanta 5,112).

The room factor is **capped at one**: `min(1, slate / room games)`. Uncapped it
fabricates volume, per the correction above. The four candidates, scored as MAE
against ESPN's 2026 team passing totals:

| room rule | raw MAE | midpoint MAE | team span after midpoint |
|---|---|---|---|
| none (status quo) | 431 | 309 | 2,620 – 5,112 |
| `eg_i / room` (shares to 1) | 384 | 286 | 2,620 – 4,606 |
| `17 / room` (this plan as written) | 1,442 | 722 | 3,268 – **8,268** |
| **`min(1, 17 / room)`** (shipped) | **361** | **274** | 2,620 – 4,606 |

The cap is a **no-op on thirty of the thirty-two teams**. Only Atlanta and Las Vegas
have a room to trim.

Two things phase 1 deliberately does not do. `usg_expected_games` is **not** rewritten
— normalising it would print 17.0 beside a quarterback the model believes plays four
games, and the board shows that column and plan 27 reads it; the room's arithmetic
travels beside it as `usg_team_qb_games` and `usg_qb_room_scale`. And a **short** room
is left short: the missing volume belongs to a passer the model does not project, and
putting it on the projected starter's row would be wrong at the player level to buy
tidiness at the team level. That is phase 3, and it needs a replacement row to move it
to.

### Phase 2 — a team snap budget — **shipped**

Constrain expected games so a team's quarterback-games sum to the slate, and its
skill-position snaps to a plausible team total, rather than each player being estimated
alone. ~~This is where the Cleveland and Atlanta failures actually live.~~

**As built** — `allocate_qb_starts` in `Scripts/usage/coherence.py`, the *replacement*
for phase 1's `normalise_qb_room` rather than an addition to it. `allocate=False` keeps
the phase 1 form so the two are read against each other rather than asserted.

> **Three of this plan's premises did not survive re-measurement, and the struck
> sentence above is the first.** Cleveland was already corrected under phase 1 —
> shrinkage toward a positional baseline, not availability — and Atlanta was the
> double-count the cap removed. Neither is what phase 2 turned out to be about.

**Correction 1: "a no-op on thirty of the thirty-two teams" described the board, not
the model.** The board carries ESPN-draftable players, roughly one quarterback a team.
In the model's own universe — which is what the gate measures — it is the inverse:

| universe | projected QBs | median room games | rooms over the slate |
|---|---|---|---|
| board (phase 1's table) | ~1 per team | 12.5 | 2 of 32 |
| model, 2026 | 96 | 22.0 | 30 of 32 |
| model, 2025 (the gate's frame) | 129 | 25.9 | 30 of 32 |

**Correction 2: the shares are wrong, not just the total** — so "constrain the room to
the slate", read literally, makes the starter worse. Against realised 2025:

| rank in room | model | realised | proportional rescale to the slate |
|---|---|---|---|
| QB1 | 12.46 | **14.03** | 9.6 — *further from the truth* |
| QB2 | 6.58 | 2.24 | 5.0 |
| room | 25.9 | 20.0 | 17.0 |

**Correction 3: the currency is starts, not appearances.** `expected_games` predicts
appearances, and appearances do not sum to the slate — a starter who leaves injured and
his replacement both count, so a realised room sums to a median **20**. A start, the
passer with the most attempts in a team-week, sums to seventeen by construction.

The allocation is measured, not assumed: 644 quarterback player-seasons 2018–2025, the
same leak-free construction as [plan 33](33-role-resolution.md)'s calibration and the
same cohort split in a different currency.

| cohort | QB1 | QB2 | QB3 |
|---|---|---|---|
| settled | **13.88** | 2.73 | 2.00 |
| mover | 10.11 | 2.21 | 1.02 |
| rookie | 9.06 | 1.58 | 0.47 |

**Why this is the phase that could move G-T2, when phase 1 structurally could not.**
A per-team constant multiplier cannot reorder inside a room, and every team's factor is
similar, so it barely reorders across them: phase 1's league-wide quarterback Spearman
against its own input is **0.956**. Allocation gives each passer his own factor and
moves the starter and his backup in opposite directions.

It also **divides rather than lifts**, which is what stops it re-creating the Miami
blow-up: a lone projected passer's share is one, not `slate / expected_games`.

### Phase 3 — redistribute vacated volume

The piece that makes phase 2 correct rather than merely tidy: when a starter's expected
games fall short of the slate, the remaining games belong to **somebody**, and their
production should appear on that player's row.

**[Plan 28](28-outcome-distributions.md) has already measured the redistribution**, which
is why this plan is cheaper than it looks: a backfield is near zero-sum — the lead back's
17.42 opportunities a game go **81%** to the next three backs and the room keeps 93% of
its volume — while a receiver room is not: a lead receiver's understudy gains **0.59 of
7.72** targets and the offence throws 1.25 fewer times. Those two numbers are the
redistribution rule, and they say it must be applied **per position group, not uniformly**.

### Do not build

| | Why |
|---|---|
| A full top-down team model (project the team, allocate shares) | It is the structurally elegant answer and a rewrite of the season head. Phases 1–3 reach team coherence without it; revisit only if G-T2 says shares are the problem |
| Reconciling `passingCompletions` / `receivingReceptions` | `USG_` projects no completions column, so there is nothing to reconcile against. Two of the three identities is the ceiling until it does |

## Gates, pre-committed

**G-T0 — the identity must close.** After the fix, `Σ receivingYards / Σ passingYards`
must fall within **0.98–1.02 on all 32 teams**, and each team's quarterback-games within
**16–18**. This one is arithmetic and will pass; it is here so a partial fix cannot be
reported as a whole one.

**G-T1 — it must not cost accuracy.** Re-run `python -m Scripts.usage.g1_season`. TOMCAT
currently improves the pre-season blend by **+1.3% MAE at weight 0.25**. **Bar: no
regression** — the reconciled model must be at least as good. Tidiness that costs
accuracy is not an improvement, and it is entirely possible that the drift is carrying
information about which side of a roster the model trusts.

**G-T2 — it should improve standalone ordering.** Within-position Spearman of `USG_`
alone against realised 2025, walk-forward. **Bar: +0.02 at quarterback**, which is where
the damage is concentrated (rho −0.502). Ordering elsewhere must not fall.

**G-T3 — the blend must barely move.** `TRUE_` is already reconciled, so a correctly
reconciled `USG_` should change the blended output very little. **Bar: median
`TRUE_Points` shift under 2% at every position.** A large move means the two
reconciliations disagree, and that is a bug rather than a result.

**G-T4 — redistribution must conserve.** After phase 3, a team's total projected
opportunities must not rise when a starter's expected games fall. Vacated volume moves;
it is not created.

**And a false-positive clause, ordered before all of them:** if **phase 1 alone** clears
G-T1 and G-T2, then the shares were already right and only the level was wrong. **Ship
phase 1 and stop.** Phases 2 and 3 are a week of work justified only by a measurement
that phase 1 leaves on the table, and this repo's own history — eleven rejected
experiments in [plan 22](22-feature-research.md), a rejected injury multiplier in
[27](27-injury-model.md) — says the cheap version wins more often than it feels like it
should.

## What shipped, and what the gates said

### Phase 2, measured 2026-08-26

| gate | bar | phase 1 | phase 2 | |
|---|---|---|---|---|
| **G-T0** identity | 0.98–1.02 on 32 teams | 1.000 on 32 | **1.000 on 32** | **pass** |
| **G-T0** QB-games | 16–18 on every team | **1 of 32** | **32 of 32** | **pass** |
| **G-T1** accuracy | no regression | +2.14% | **+2.30%** | **pass** |
| **G-T2** standalone QB ordering | +0.02 Spearman | +0.0054 | **+0.0305** | **pass** |
| **G-T2** ordering elsewhere | must not fall | RB −0.0015, WR +0.0023, TE +0.0012 | RB −0.0013, WR +0.0017, TE −0.0018 | **pass** |
| **G-T3** blend stability | median shift < 2% every position | worst 0.45% | **worst 0.00%** | **pass** |
| **G-T4** conservation | — | n/a | phase 3 | n/a |

**G-T0's second half is the one phase 1 recorded as "not reachable".** Lifting a short
room to seventeen games means projecting a quarterback with no row on the board.
Allocation reaches it from the other side — it never lifts, it divides — so the gate is
met without inventing a player.

Team passing totals land at **3,711–5,249** against phase 1's 4,604–6,368. The all-time
record is 5,477, so phase 1's top team was still projecting past it and phase 2's is not.

**G-T3 passes, and the shape of the pass is the interesting part.** Measured on a real
`gop_degenerates` board built twice in process, writing nothing: the median
`TRUE_Points` shift is **0.00% at every position**, because **18 of 749 rows move at
all**. TOMCAT is one of five sources and is withdrawn wherever ESPN prices a player out,
so a reallocation inside a quarterback room reaches the blend on a handful of rows and
nowhere else. Where it does land it is not small — 4 of 74 quarterbacks move, by a
median of 15.7% and a maximum of 19.2%.

That is the gate behaving as designed rather than a null result. Atlanta is the worked
example: the chart's QB1 keeps 12.91 of the seventeen starts while the QB2, whom the
model had at 2,404 passing yards on a full slate, drops to 493.

| player | rank | cohort | allocated starts | `USG_passingYards` |
|---|---|---|---|---|
| Tua Tagovailoa | 1 | mover | 12.91 | 3,634 → 2,760 |
| Michael Penix Jr. | 2 | settled | 3.49 | 2,404 → **493** |
| Jack Strand | 3 | rookie | 0.60 | 200 → 7 |

The shipping path closes the identity on the same build: 0.612–1.704 → 1.000–1.000
across 32 teams.

**The honest limit, and it is the number to argue with.** Bootstrapped over the
evaluation frame, phase 2's gain *over phase 1* at quarterback is **+0.0252 with a 95%
interval of [−0.0035, +0.0536]** — it includes zero. The other three positions'
intervals include zero too, which is why the TE −0.0018 is not read as a fall.

`n` is 92 quarterbacks and **cannot be raised**: `external_season` needs `lineups`, and
the store holds those for 2025 alone because FantasyPros serves no season parameter
(see [plan 25](25-results-backfill.md)). The gate is structurally single-season. So
phase 2 clears a bar that was pre-committed against the *unreconciled* baseline, on
exactly the `n` at which phase 1's +0.0054 was read as a miss, and the separation
between the two phases is not itself significant. Both statements are true and the
second does not cancel the first.

The independent evidence does not depend on that `n`: the allocation is fitted on 644
player-seasons, G-T0's QB-games half goes 1 of 32 to 32 of 32, and the league's team
passing totals move inside their historical range.

### Phase 1, measured 2026-08-24

| gate | bar | result | |
|---|---|---|---|
| **G-T0** identity | 0.98–1.02 on 32 teams | **1.000–1.000 on 32** | **pass** |
| **G-T0** QB-games | 16–18 on every team | 3.7–17.0 | **not reachable in phase 1** |
| **G-T1** accuracy | no regression on the +1.3% at weight 0.25 | **+1.3% → +2.1%** | **pass** |
| **G-T2** standalone QB ordering | +0.02 Spearman | **+0.005** | **miss** |
| **G-T2** ordering elsewhere | must not fall | RB −0.001, WR +0.002, TE +0.001 | **pass** |
| **G-T3** blend stability | median `TRUE_Points` shift < 2% at every position | **worst 0.45%** (QB 0.43, TE 0.45, WR 0.31, RB 0.09) | **pass** |
| **G-T4** conservation | — | phase 3 | n/a |

**G-T0's second half cannot be met by phase 1 and the gate was mis-specified, not
missed.** Bringing a short room up to seventeen games means projecting a quarterback
who is not on the board — Miami's starter is not in the model's universe at all. There
is no row to put the games on until phase 3 makes one. The identity half, which is the
half that reads on the board, closes exactly.

**G-T1 came in better than "no regression".** The full sweep, 2025 walk-forward,
pre-season basis, priced by `winfield_football`:

| TOMCAT weight | MAE before | MAE after | vs baseline before | after |
|---|---|---|---|---|
| 0.10 | 34.52 | 34.39 | +0.9% | **+1.3%** |
| **0.25** (ships) | 34.40 | **34.09** | +1.3% | **+2.1%** |
| 0.40 | 34.36 | 33.91 | +1.4% | **+2.7%** |
| 0.50 | 34.36 | 33.86 | +1.4% | **+2.8%** |

Blended within-position Spearman at quarterback rises 0.804 → 0.815 at the shipping
weight. The lowest-MAE weight moves from 0.40 to 0.50, which is worth a separate look:
a reconciled TOMCAT is a source the blend wants **more** of, not less.

**G-T2 is the result that matters, and it is a miss.** `USG_` alone at quarterback
moves 0.713 → 0.718 against a pre-committed bar of +0.02. The plan predicted the fix
would recover much of the −0.502 correlation between a quarterback's TOMCAT-vs-market
disagreement and his team's identity ratio; it recovered a quarter of the predicted
amount.

So **the false-positive clause does not fire.** Phase 1 fixed the level and left the
ordering roughly where it was, which is the measurement the plan pre-committed to as
the justification for phases 2 and 3 — the disagreement at quarterback is mostly not
arithmetic. That is now the open question rather than an assumption, and the honest
reading is the narrower one: reconciling puts TOMCAT's column on a footing where its
quarterback opinion can be *read*, and says nothing yet about whether that opinion is
right.

The two paths group by different columns — the gate by the model's own `team`, the
shipping path by ESPN's `pro_team` — and that is a plumbing detail rather than a
difference of opinion. Both are the current pre-season roster team; a player who
moved is reconciled inside his **new** huddle, which is the same team whose offensive
profile and coaching prior the model projected him in (`Scripts.usage.scheme.attach`).
`project.build` simply does not carry `team` into the parquet, and `pro_team` is
already on the board frame.

## Effort

M. Phase 1 is half a day and may be the whole plan. Phase 2 is a day. Phase 3 is two to
three days and is the only part that needs new modelling, most of it already measured by
plan 28.

**Not before the 2026 drafts, and phase 1 is on a branch for exactly that reason.**
Every phase moves `USG_` and therefore `TRUE_`, and the board is frozen — see [`DRAFT_READINESS.md`](../DRAFT_READINESS.md). The evidence above
is reproducible today and the fix is not urgent: the blend is already reconciled, so
nothing anyone drafts from is wrong. What is wrong is the column beside it, and the
read-around until this ships is **compare TOMCAT by rank within position, and discount a
quarterback whose team ratio is far from 1.0**.
