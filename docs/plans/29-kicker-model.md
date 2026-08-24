# 29 — Kickers: the leg is noise, the offence is the projection

**Priority:** **Low for the draft board, Medium-high for the weekly surface** — and that
split is a measurement, not a hedge. Season-average environment separates the best kicker
quintile from the worst by **27.7 points, a ratio of 1.24×**, so at draft time kickers are
nearly undifferentiable; per game the range is **2.4 points**, which is a real start/sit
decision. **Build the weekly path first.** · **Effort:** M ·
**Status: BUILT and shipped at weight 0.0, 2026-08-18. Channel P passes its gate at
+45.9%; channel F FAILS at +1.2% against a 5% bar and the shrinkage selection chose
0.00 — i.e. held-out selection said to ignore prior-season red-zone figures entirely
and use the league mean.** That is the outcome the plan pre-registered, and the artifact
shows it: projected FG attempts run 1.89-1.98 a game with a cross-team SD of **0.03**
against an empirical between-team SD of 0.36, so channel F is a constant in all but name.
Every point of differentiation is channel P — PAT made ranges 1.46 to 2.74 a game, SD 0.38.
Against ESPN on the stats we model, rank **rho +0.91** and a level gap of 0.2 points: this
model agrees with ESPN almost completely, which was the expected result and is why it ships
at 0.0. **Evidence
below measured 2026-08-18 and it settles the question the plan was opened to explore.**
Of the two candidate drivers — individual skill, or the team's failure to finish drives —
**the first does not exist as a predictable quantity and the second does.** A kicker's
field-goal conversion rate has a year-over-year correlation of **0.009** across 222
kicker-season pairs; his field-goal attempts per game correlate **−0.006** with his own
previous season. What *is* sticky is the offence around him: extra-point attempts per game
at **0.346** for the kicker and **0.399** for the team, and Vegas's implied team total
predicts extra-point volume at **r = 0.844**. And the red-zone hypothesis is confirmed and
monotone: a team in the top third for red-zone volume and the bottom third for red-zone
conversion gives its kicker **2.24 field-goal attempts a game** against **1.74** for a
low-volume, high-conversion offence — **8.4 extra attempts over a season.**

**Game script was tested too, and it is the largest environment signal in the plan — but the
proposed mechanism is backwards.** Heavy favourites do score more (8.89 a game against a heavy
underdog's 6.12) and **all of the gain is extra points**: PAT attempts go 1.41 → 3.31 while
field-goal attempts *peak at a modest favourite* (2.09) and fall for double-digit ones (1.97).
Favourites do not settle for field goals — they settle **less**, taking 0.185 attempts per
red-zone play against a heavy underdog's **0.232**, because they reach the red zone 42% more
often and punch it in. The field-goal-richest script is a **close win**, not a blowout (0.239
attempts per red-zone play, and 2.27 attempts a game). And net of the implied team total the
spread is **flat** — 8.81 / 8.75 / 8.81 across its whole range in the top band — so it is
expected scoring under another name and stays out of the model. Best pre-game cell: a solid
favourite in a moderately high total, **8.95 a game against 6.56**, worth **+40.7 over a
season**.

**Depends on:** [10](10-scoring-registry.md) — the per-league kicker rules, which differ
more than any other position's · [16](16-usage-data-layer.md) — red-zone and team context
**Feeds:** [09](09-frontend-draft-views.md) — the columns · [28](28-outcome-distributions.md)
— a kicker's season is a count process and belongs in that framework
**Shares evidence with:** [30](30-dst-model.md) — the same Vegas implied-total machinery,
derived once

> **Correction, 2026-08-18 — only part of a season is priced pre-season.** This plan
> said Vegas needs "no forecast of its own" because "all 272 of 2026's games are already
> priced". **They are not: 52 of 272 carry a line**, weeks 1 to 4, giving 3 to 4 priced
> games per team with all 32 teams covered. The claim was inherited from
> `Scripts/draft/handcuff.py`'s docstring, which says the same thing and is also wrong —
> that module averages whatever lines exist, so its 2026 team-strength input rests on
> ~3 games per team rather than seventeen.
>
> It does not sink the approach, and that was measured rather than hoped: over 320
> historical team-seasons a weeks-1-to-4 average predicts the full-season figure at
> **r = 0.845** for implied own total, **0.810** for spread and **0.727** for implied
> points allowed. But the early average is ~17% wider than the quantity it estimates, so
> `Scripts/vegas.py` shrinks it toward the league mean by a fitted slope — 0.829, 0.772
> and 0.624 respectively — rather than trusting it.


## Problem

Kickers are **100% ESPN** and nothing else. Of the four weighted sources, FantasyPros,
Pinnacle and BetOnline arrive imputed from `MEAN_` for every kicking stat, and the usage
model abstains outright — `usg_arm` is null for all **58** kickers on every board. So a
starting slot in nine leagues is filled by one source's opinion with no second opinion, no
model, and no uncertainty.

That would be defensible if kickers were unpredictable noise. The measurements below say
something more useful: kicker scoring is **highly predictable and almost none of the
predictability belongs to the kicker.**

**What the position is worth.** On Winfield Football the top projected kicker sits at 171.7
points against a positional median of 123.7 — a 48-point spread, comparable to the
RB2 vacancy swing plan 28 was written for. On GOP Degenerates, whose rules differ, the
top kicker projects at **251.3**.

## Evidence

2016–2025, `player_weeks` and `red_zone` from this repo's own pulls, 322 kicker-seasons of
8+ games and 320 team-seasons of 14+ games. Reproduce with
`python -m Scripts.kicking.evidence`.

### Fantasy scoring used for the measurement

A neutral ladder — 3 points under 40 yards, 4 from 40–49, 5 from 50+, 1 per extra point,
−1 per miss — chosen to sit near the middle of the nine leagues' actual rules so the
stickiness figures are not an artifact of one league's ladder. **The model itself will not
use this**; it projects stat lines and `proj_to_score` applies each league's own rules, the
same as every other position.

### The kicker's own skill does not persist. At all.

Same kicker, consecutive seasons, n=222:

| component | r | what it is |
|---|---|---|
| **FG conversion rate** | **0.009** | the thing everyone calls kicker skill |
| FG attempts / game | −0.006 | his own workload |
| fantasy points / game | 0.159 | the outcome |
| share of makes from 50+ | **0.335** | leg strength, or coach trust in it |
| **PAT attempts / game** | **0.346** | *the offence scoring touchdowns* |

Two readings, and both are design constraints.

*Field-goal percentage is not a projectable quantity.* An `r` of 0.009 on 222 pairs is
indistinguishable from zero. Any model that carries a per-kicker accuracy term is fitting
last year's luck. **The correct treatment is a positional constant** — every kicker gets
the league-wide make rate for the distance bucket, and the only thing that varies between
kickers is how many attempts and from where.

*The two stickiest things on the list are both properties of the team*, and the fifth row
is the loudest: extra-point attempts are a pure function of touchdowns scored, and they
persist better than anything the kicker does himself.

### The kicker changes team, and the stickiness stays behind

The cleanest available separation of leg from offence. `r` between a kicker's season and
his next, split by whether he stayed:

| | moved (n=39) | stayed (n=183) |
|---|---|---|
| PAT attempts / game | **−0.040** | **0.386** |
| FG attempts / game | −0.267 | 0.050 |
| fantasy points / game | −0.047 | 0.189 |
| FG conversion rate | 0.196 | −0.019 |

Extra-point volume travels **not at all**: 0.386 becomes −0.040 when the jersey changes.
It is the offence's property, held by whoever happens to be kicking. The conversion-rate
row reverses sign between the groups, which on n=39 is what noise looks like and should not
be read as movers kicking more consistently.

### Team level, which is where the signal lives

320 team-seasons, 288 consecutive pairs:

| team quantity | r YoY | reading |
|---|---|---|
| PAT attempts / game | **0.399** | the most persistent kicking-relevant quantity in football |
| offensive TDs / game | 0.360 | its cause |
| red-zone plays / game | 0.337 | the offence gets close |
| TDs per red-zone play | 0.095 | **red-zone *efficiency* barely persists** |
| FG attempts / game | 0.058 | the kicker's actual workload |

**The row that matters most is the fourth.** Red-zone conversion is close to a coin-flip
year over year, which is the same finding the wider analytics literature reports and it has
a sharp consequence here: *the mechanism that drives field-goal volume is real but its input
is not forecastable from last season.* Field-goal attempts inherit that — 0.058 — which is
why the position looks unpredictable when measured naively.

### The red-zone hypothesis, confirmed and interacted

Within season, team level:

| pair | r |
|---|---|
| red-zone plays/gm → FG attempts/gm | **+0.257** |
| **TDs per red-zone play → FG attempts/gm** | **−0.393** |
| offensive TDs/gm → FG attempts/gm | −0.089 |
| offensive TDs/gm → PAT attempts/gm | +0.955 *(sanity)* |

Red-zone conversion is the **strongest single correlate of field-goal volume** found
anywhere in this measurement, and its sign is the hypothesised one: the worse a team
finishes, the more it kicks. Field-goal attempts per game, by tertile of both:

| | poor conversion | mid | good conversion |
|---|---|---|---|
| low red-zone volume | 1.90 (n=32) | 1.82 (n=33) | 1.74 (n=42) |
| mid red-zone volume | 2.10 (n=47) | 2.03 (n=33) | 1.80 (n=27) |
| **high red-zone volume** | **2.24 (n=28)** | 2.10 (n=40) | 1.78 (n=38) |

Monotone decreasing across every row and increasing down the poor-conversion column.
Conversion tertile means ascend 0.220 / 0.264 / 0.322 TDs per red-zone play, so the
labelling is the right way round.

**And it is an interaction, not two additive effects.** Down the *good*-conversion column
the volume effect vanishes — 1.74 / 1.80 / 1.78 — because an offence that finishes does not
kick regardless of how often it gets close. Volume only converts into field goals when the
conversion is poor. Best cell against worst is **2.24 against 1.74**, which over 17 games is
**+8.4 attempts**, roughly 7 extra makes and ~25 fantasy points on the neutral ladder.

### Game script: favourites do score more, and not for the reason anyone assumes

5,183 team-games with a closing line. Kicker points on the neutral ladder, by the kicker's
own team's pre-game spread:

| own line | n | K pts | FGA | PAT | FG% | 50+ share |
|---|---|---|---|---|---|---|
| dog 10.5+ | 259 | 6.12 | 1.65 | 1.41 | 84.1% | 15.6% |
| dog 6.5–10.5 | 601 | 6.92 | 1.83 | 1.83 | 83.3% | 16.6% |
| dog 3.5–6.5 | 758 | 7.55 | 1.93 | 2.01 | 84.8% | 15.5% |
| dog 0.5–3.5 | 959 | 7.52 | 1.91 | 2.18 | 83.7% | 17.0% |
| pick'em | 1238 | 8.00 | 2.02 | 2.39 | 84.1% | 14.3% |
| fav 3.5–6.5 | 621 | 8.62 | **2.09** | 2.69 | 85.6% | 13.2% |
| fav 6.5–10.5 | 543 | 8.78 | 2.02 | 3.01 | 86.6% | 12.7% |
| **fav 10.5+** | 204 | **8.89** | 1.97 | **3.31** | 86.5% | 11.8% |

Points rise monotonically, +2.77 a game from heaviest dog to heaviest favourite. **But the
gain is extra points, not field goals.** PAT attempts go 1.41 → 3.31, a factor of 2.3. Field
goal attempts are **hump-shaped and peak at a modest favourite** — 2.09 at fav 3.5–6.5,
falling to 1.97 for double-digit favourites. And the 50+ share *falls* with favouritism,
15.6% → 11.8%: a team in control does not need 52-yarders.

### "Settling for a field goal" is real behaviour and it belongs to underdogs

The hypothesis was that a team about to win big gets conservative and takes the three. Tested
directly — field-goal attempts per red-zone play, and the share of red-zone conversions that
end in a kick rather than a touchdown:

| own line | n | FGA | off TDs | RZ plays | **FGA per RZ play** | kick share |
|---|---|---|---|---|---|---|
| dog 6.5+ | 860 | 1.78 | 1.83 | 7.66 | **0.232** | **49.3%** |
| dog 0.5–6.5 | 1717 | 1.92 | 2.24 | 8.64 | 0.222 | 46.1% |
| fav 0.5–6.5 | 1859 | 2.04 | 2.57 | 9.28 | 0.220 | 44.3% |
| **fav 6.5+** | 747 | 2.01 | 3.11 | 10.85 | **0.185** | **39.2%** |

**Monotonically decreasing.** Heavy favourites settle *least* — they reach the red zone 42%
more often than heavy dogs (10.85 against 7.66 plays) and convert a larger share of it into
touchdowns. The conservative-favourite intuition describes a real thing that happens in
individual games, but across ten seasons it is dominated by the fact that good offences
finish. Settling is what a bad offence does, because a bad offence's red-zone trips are worse.

By realised margin, where the "sitting on a lead" story should be strongest if it exists:

| realised margin | n | FGA | off TDs | RZ plays | **FGA per RZ play** | kick share |
|---|---|---|---|---|---|---|
| lost by 8+ | 1318 | 1.42 | 1.44 | 6.75 | 0.211 | 49.7% |
| lost by 1–7 | 1253 | 1.99 | 2.21 | 8.72 | 0.228 | 47.3% |
| **won by 1–7** | 1271 | **2.27** | 2.57 | 9.49 | **0.239** | 46.8% |
| won by 8+ | 1341 | 2.14 | 3.41 | 11.11 | **0.193** | **38.6%** |

On a finer split, kicker points a game run 4.23 (lost by 15+), 5.80, 7.34, 9.19, 9.21 and
**10.27** (won by 15+), while field-goal attempts over the same six bands go 1.24, 1.67, 1.99,
**2.27**, 2.21, 2.09 and extra points go 1.02, 1.51, 2.01, 2.45, 2.96, **3.99**.

**The field-goal-richest script is a close win, not a blowout.** FGA and FGA-per-red-zone-play
both peak in the "won by 1–7" band and fall in blowout wins, where the offence is punching it
in (3.41 touchdowns) and the kicker's 10.27 points are 3.99 extra points plus 2.09 attempts.
So there are two different good scripts and they pay through different channels: *win
comfortably* maximises total points, *win narrowly* maximises field goals.

**One caveat on the FG% column, because it is reverse causality.** 78.6% in blowout losses
against 90.0% in blowout wins is not evidence that winning makes kickers accurate. Missed
kicks cause losses, and trailing teams attempt desperation long ones. It must not enter the
model as a feature.

### And net of the implied team total, the spread tells you nothing

The spread and the implied team total are algebraically entangled — `implied = total/2 +
line/2` — so the table above may be measuring expected scoring wearing a spread's clothes.
Holding the implied total fixed, kicker points a game:

| implied team total | dog 6.5+ | dog 0.5–6.5 | fav 0.5–6.5 | fav 6.5+ |
|---|---|---|---|---|
| ≤ 21.5 | 6.57 (n=796) | 7.45 (n=1064) | 7.63 (n=247) | — |
| 21.5–25.5 | 8.02 (n=64) | 7.57 (n=605) | 8.08 (n=1085) | 8.80 (n=177) |
| **> 25.5** | — | **8.81 (n=48)** | **8.75 (n=527)** | **8.81 (n=570)** |

**The top band is flat** — 8.81 / 8.75 / 8.81 across the whole spread range. FG attempts are
flat within every band too (1.75–2.19, no pattern). So favouritism is a **proxy** for expected
scoring and adds essentially nothing once the implied total is known. Channel P should be
keyed on the implied team total and the spread should not appear in the model at all.

### The best pre-game script, named

Total × spread, kicker points a game, cells with n ≥ 150:

| | dog 6.5+ | dog 0.5–6.5 | fav 0.5–6.5 | fav 6.5+ |
|---|---|---|---|---|
| total ≤ 43.5 | 6.56 (n=326) | 7.24 (n=667) | 7.90 (n=721) | 8.77 (n=285) |
| total 43.5–47.5 | 6.56 (n=319) | 7.72 (n=564) | 8.25 (n=621) | **8.95 (n=272)** |
| total > 47.5 | 7.03 (n=215) | 7.72 (n=486) | 8.59 (n=517) | 8.65 (n=190) |

**Best: a solid favourite in a moderately high-total game — 8.95 points a game.** Worst: a
heavy underdog, 6.56. The spread is **+2.39 a game, +40.7 over a season** — larger than the
red-zone effect's ~25 points and the biggest single kicker-environment signal in this plan.

**And the optimum is interior, which is the interesting part.** The highest-total column is
*worse* for a heavy favourite (8.65) than the middle column (8.95), because field-goal
attempts collapse to 1.82 in shootouts — at some point the offence simply scores touchdowns
instead. The ideal kicker environment is not the most extreme version of anything: it is an
offence good enough to reach the red zone constantly, in a game competitive enough that field
goals still matter.

### Vegas already knows the extra-point half, and nothing about the other half

Implied team total
against realised, 315 team-seasons:

| | r |
|---|---|
| implied team total → offensive TDs / gm | **0.848** |
| implied team total → PAT attempts / gm | **0.844** |
| implied team total → **FG attempts / gm** | **0.117** |

**This is the cleanest decomposition in the plan.** The extra-point channel is essentially
solved by a number already on disk. The field-goal channel is where all the remaining work
is, and Vegas cannot see it — because the market prices *how many points a team scores*, not
*how it fails to score them*.

**A trap, recorded because it bit during this measurement.** `schedules.parquet`'s
`spread_line` is the **home** team's margin and **positive means home favoured** — verified
against realised results at `r(spread_line, home_score − away_score) = +0.446`, mean margin
+5.68 when positive and −4.46 when negative. The first version of the implied total used the
opposite sign and reported `r(implied, PAT) = −0.272`: a strong, clean, entirely inverted
result that looked like a finding rather than a bug. Any consumer of these lines —
[plan 30](30-dst-model.md) included — must derive the sign the same way and assert it.

### What is already correct, and should not be "fixed"

Two things looked like bugs and are not.

*The `214` colName is ugly and functional.* Two leagues score kickers on field-goal
**yardage** (`FGY`, ESPN stat id 214) — GOP at 0.064/yard and Twelve Dudes at **0.1/yard
with no distance-tier rules at all**. The registry records `colName` as the literal string
`"214"` because no name mapping exists, and the projection sources expose `ESPN_214`
identically, so the rule blends and scores correctly. Verified on Twelve Dudes' board:
`1371.49 × 0.1 + 46.10 × 1.0 − 4.16 × 1.0 = 179.09`, matching `TRUE_Points` of 179.085. It
is fragile — the two sides agree only because both fell back to the same raw id — and worth
a name, but it is not the dropped rule plan 01 describes.

*Kicker `TRUE_Points` equalling `ESPN_projected_total` is not a passthrough.* They match to
two decimals on Winfield Football because ESPN is the only real source for kicking stats, so
the four-way blend collapses to ESPN, and ESPN's own total is already scored in that
league's rules. On GOP the two diverge (251.3 against 163.5) exactly as different rules
should make them.

### Weekly or seasonal? The variance says weekly, the inputs say both

The game-script tables above are per-game. Whether any of it reaches a *draft board* depends
on how much survives averaging over seventeen games, so: variance decomposition of weekly
values into between-team-season and within-team-season, on the 5,183 games where both
positions are present. `ICC` is the share of variance that is a team property.

| quantity | SD total | between team-season | within | **ICC** |
|---|---|---|---|---|
| kicker points | 4.54 | 1.47 | 4.30 | **0.105** |
| kicker FG attempts | 1.29 | 0.36 | 1.24 | **0.077** |
| D/ST points | 6.11 | 2.10 | 5.74 | **0.118** |
| sacks | 1.74 | 0.50 | 1.67 | 0.084 |
| points allowed | 9.98 | 3.32 | 9.42 | 0.111 |
| | | | | |
| implied own total | 3.87 | 2.82 | 2.65 | **0.530** |
| game total | 4.42 | 2.84 | 3.39 | 0.413 |
| spread | 6.34 | 3.88 | 5.02 | 0.374 |
| implied points allowed | 3.86 | 1.90 | 3.36 | 0.242 |

**Roughly 89% of both positions' weekly variance is week-to-week, not team.** These are
streaming positions in the statistical sense, not just the colloquial one — which is why they
look unpredictable when measured as season totals and why the honest season projection has a
wide interval.

**But the market inputs are far more team-persistent than the outcomes they predict** — ICC
0.53 on the implied own total against 0.105 on kicker points. That asymmetry is the whole case
for a season model: the *forecastable* component of the signal is precisely the team-level
component, and the within-team week-to-week variance is mostly irreducible noise that averages
out. A season projection is allowed to capture the 0.53 and forget the rest.

**How much differentiation is actually available at draft time.** No team plays a season as a
7-point favourite in a low total; season averages compress hard:

| season-average input | mean | SD | p10 | p90 | p10–p90 range |
|---|---|---|---|---|---|
| implied own total | 22.61 | 2.82 | 18.81 | 26.27 | 7.45 |
| implied points allowed | 22.58 | 1.90 | 20.15 | 25.13 | 4.99 |
| spread | 0.03 | 3.88 | −5.16 | 4.78 | 9.94 |
| game total | 45.19 | 2.84 | 41.66 | 49.38 | 7.72 |

**The kicker ceiling, by quintile of season-average implied own total:**

| quintile | input | n | pts/gm | over 17 |
|---|---|---|---|---|
| q1 | 18.72 | 64 | 6.76 | **115.0** |
| q2 | 20.95 | 62 | 7.69 | 130.7 |
| q3 | 22.64 | 62 | 7.91 | 134.5 |
| q4 | 24.16 | 63 | 8.32 | 141.5 |
| q5 | 26.59 | 63 | 8.40 | **142.7** |

**27.7 points from worst quintile to best — a ratio of 1.24×**, against the +40.7 the best and
worst *per-game* cells implied. So about two thirds of the game-script edge survives
seasonalisation, and the top is flat: q3 → q5 buys only 8.2 points while q1 → q2 buys 15.7.
**At draft time kickers are nearly undifferentiable** — one round of ADP is worth more than the
entire league-wide spread in kicker environment — and that is the finding, not a failure of the
model. The value of channel P is concentrated in *avoiding q1*, and in the weekly surface where
the per-game range is 2.4 points a game.

## Fix

### Two channels, because the evidence says they are two problems

**Channel P — extra points.** `PAT attempts = f(implied team total)`, times a positional
constant make rate. `r = 0.844` from lines already on disk, shrunk for partial coverage (see the correction above). There is
no kicker-specific term because there is no kicker-specific signal: PAT conversion is a
league-wide ~0.96 and does not persist per kicker.

**The spread is deliberately excluded**, and that is a measurement rather than a
simplification: within the top implied-total band, kicker points are 8.81 / 8.75 / 8.81
across the full spread range. Favouritism is expected scoring under another name, and adding
it would let the same information in twice.

**Channel F — field goals.** `FG attempts = f(red-zone volume, red-zone conversion,
implied team total)` with the **interaction** term, since the 3×3 table says volume only
matters when conversion is poor. Then attempts are allocated across distance buckets by a
**team-and-kicker** distance mix, and converted at **positional constants** per bucket.

**The distance mix is where the only real kicker term lives.** Share of makes from 50+
persists at `r = 0.335` — the single stickiest kicker-specific quantity measured — and it
matters because the leagues pay 5 points for a 50-yarder against 3 under 40. It is a
*usage* signal as much as a leg signal (a coach's willingness to send him out), which is
why it belongs on the attempt-allocation step rather than on the conversion step.

### The forecasting problem this creates, stated plainly

Channel F's strongest input — red-zone conversion — has a YoY `r` of **0.095**. The
mechanism is confirmed but its driver is nearly unforecastable from history. So channel F
must be built as a **shrunk** estimate: a team's prior red-zone conversion pulled hard
toward the league mean, with the shrinkage fitted rather than chosen. Expect the honest
answer to be that the field-goal channel lands close to a league-average attempt rate
modulated mostly by red-zone *volume* (`r = 0.337`, the forecastable half) and only
slightly by conversion.

**This is pre-registered as the likely outcome**, so that a model which reproduces
league-average field-goal volume is read as the correct answer rather than a failure. The
value delivered is then not a better kicker ranking but an honest one — plus the extra-point
channel, which is genuinely predictable and currently unmodelled.

### Artifacts and code

- `Scripts/kicking/evidence.py` — the measurements above, reproducible, deterministic.
- `Scripts/kicking/model.py` — the two channels; writes `USG_`-style `KIK_<stat>` columns
  on the same footing as every other source, per-stat, so `proj_to_score` applies each
  league's ladder rather than the model knowing anything about scoring.
- `Data/NFL/team_kicking_context.parquet` — team-season red-zone volume, conversion and
  implied total. **Not** league-scoped: a red-zone failure is the same failure in all nine.
- The Vegas implied-total derivation moves to `Scripts/vegas.py`, shared with plan 30, with
  the sign assertion as a test rather than a comment.

### Phases

| Phase | What | Gate |
|---|---|---|
| 1 | `Scripts/vegas.py` — implied totals, sign asserted, shared with plan 30 | G-K0 |
| 2 | `Scripts/kicking/evidence.py` — the tables above | — |
| 3 | Channel P: PAT attempts from the implied total | G-K1 |
| 4 | Channel F: FG attempts from red-zone volume × conversion, shrunk | G-K2 |
| 5 | Distance mix and positional conversion constants | G-K3 |
| 6 | `KIK_` as a fifth source for kickers only, at weight 0.0 first | G-K4 |
| 7 | **the weekly surface — where the measured value is** ([19](19-weekly-usage-model.md)'s home) | G-K1/G-K2 re-run per week |
| — | a per-kicker accuracy term | **Do not build** — `r = 0.009` |
| — | a per-kicker FG-volume term | **Do not build** — `r = −0.006`, and it does not travel |
| — | game spread as a feature | **Do not build** — flat at 8.81/8.75/8.81 net of the implied total |
| — | a game-script FG% adjustment | **Do not build** — the 78.6%-vs-90.0% split is reverse causality |

### Gates, pre-committed

**G-K0 — the sign.** `r(spread_line, home_score − away_score)` must be **positive** on the
historical schedule before any implied total is derived, asserted in code rather than
checked by eye. This gate exists because the error it catches produced a clean, plausible,
inverted result during this plan's own measurement.

**G-K1 — the extra-point channel must beat ESPN.** Out-of-sample MAE on season PAT
attempts, walk-forward, against `ESPN_attemptedExtraPoints`. **Bar: 10% better.** With
`r = 0.844` available from a line ESPN also has, anything less means ESPN already prices it
and channel P is redundant.

**G-K2 — the field-goal channel must beat a league-average constant.** Out-of-sample MAE on
season FG attempts against "every team gets the league mean". **Bar: 5%.** Deliberately
lower than G-K1 because the input's YoY `r` is 0.095 and the pre-registered expectation is
that this gate is the one that fails.

**G-K3 — no accuracy term smuggled in.** Fit the model, then refit with a per-kicker
conversion offset. **Bar: the offset must not improve out-of-sample MAE by more than 1%.**
If it does, the 0.009 finding is wrong and this plan's central claim needs revisiting; if it
does not, the constant stays and the result is recorded.

**G-K5 — the spread must stay out, pre-registered as a null.** Refit channel P with the
game spread beside the implied total. **Bar: it must not improve out-of-sample MAE on PAT
attempts by more than 1%.** The measurement says it will not; the gate is here so a future
reader who adds it back has to defeat a number rather than an opinion.

**G-K4 — cross-position neutrality, borrowed from plan 27's `injury_verdict`.** Adding
`KIK_` must not move any *non-kicker* position's median `TRUE_`/`ESPN_` ratio by more than
0.02. A kicker model that shifts running backs has renormalised something it should not
have touched.

**And a false-positive clause ordered before all of them:** if the model's improvement on
kickers is matched by an equal improvement when the red-zone terms are **replaced with
random team assignments**, it has found team-level scoring volume in general rather than
red-zone failure in particular, and channel F is a relabelled implied total. Report that
first.

## Effort

M. Phase 1 is shared with plan 30 and is an hour. Phases 2–3 are a day and are where the
measured value is. Phases 4–5 are a day and are where the plan expects to be humbled.
Phase 6 is the same wiring plan 18 already established, at weight 0.0 so turning it on is
one number.

**Order matters more than usual here.** The variance decomposition says a draft-day kicker
column is worth about one round of ADP across the entire league, while a weekly one is worth
2.4 points a game. If only one gets built, build the weekly one — which means this plan is
really a contribution to [plan 19](19-weekly-usage-model.md) wearing a season plan's clothes.

**Most of the standalone value is in the evidence.** "A kicker's field-goal percentage has
no year-over-year signal, and the best kicker environment is a high-volume offence that
cannot finish" is a draft-room fact with no fitted model in it, and it changes how the last
round is spent whether or not any of the above survives its gates.

## Postscript — traps found while measuring this

- **The `spread_line` sign.** See above. It cost an inverted Vegas result that looked
  entirely publishable.
- **Scoring rules must be read at the right slot.** Reading `Data/Scoring/scoring.csv`
  without filtering `slot == "16"` and letting a dict collapse duplicate `colName` keys
  returns the **base** value for every defensive rule, which is `0.0` — an offensive player
  scores nothing for a defensive stat. It produced a points-allowed ladder of all zeros and
  a divide-by-zero. This is exactly what [plan 11](11-per-slot-scoring.md) exists for, and
  any new consumer of the registry has to honour the slot dimension.
- **`qcut` labels and pivot column order.** `pl.DataFrame.pivot` does not guarantee column
  order, so the same 3×3 table printed with its conversion tertiles in a different order on
  consecutive runs and appeared to reverse the finding. Print cross-tabs by explicit
  row/column keys, never by pivot order — the same discipline
  [plan 28](28-outcome-distributions.md) had to adopt for its room ranks.
