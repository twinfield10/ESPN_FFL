# 30 — D/ST: nine ladders, one defence, and 71% of the score is noise

**Status:** COMPLETE

**Priority:** **High for both surfaces**, and unlike [29](29-kicker-model.md) the draft-board
case is measured rather than assumed: season-average environment separates the best D/ST
quintile from the worst by **80.2 points, a ratio of 3.1×**, monotone across all five
quintiles. A starting slot in **eight** of the nine leagues -- Twelve Dudes rosters no D/ST, which
this plan assumed it did -- with **zero** market coverage. · **Effort:** L ·
**Where it stands: BUILT and shipped at weight 0.0, 2026-08-18.** Points-allowed and yards-allowed
tier means clear their gates at **+36.8%** and **+32.9%** held-out; tackles for loss,
interceptions, solo tackles, passes defensed and sacks beat their own means by 4-7%; and
**fumbles forced, safeties, fumble recoveries and defensive touchdowns are shrunk to the
positional mean** because the market cannot beat a constant on them -- exactly the four the
plan named in advance. Against ESPN on the components we model the level matches (80.8
against 80.0 on Winfield) but rank **rho is only +0.50 to +0.56**, so this is a genuinely
independent opinion rather than a restatement. **One known weakness:** our cross-team SD of
projected points allowed is **1.87** against ESPN's 2.26 and an empirical between-team-season
SD of **3.32** -- both projections are over-shrunk and ours more so, which is the first thing
to fix before any weight is turned on. **Evidence below measured 2026-08-18
and it reverses the plan's own framing twice.**

*The nine ladders are a smaller problem than they look.* Scoring every team-defence season
under each league's actual rules gives a **median pairwise rank correlation of 0.968** on
2025 (lowest pair 0.918). All nine leagues rank Houston first; eight of nine rank Seattle
second. **One model, scored per league at the end, is the right architecture** — the same
one the rest of the repo already uses.

*But the components carrying the score are the unpredictable ones.* In six of nine leagues
**sacks + interceptions + fumble recoveries are 71% of the D/ST score**, and their
year-over-year correlations are **0.203, 0.113 and 0.015**. The two components that do
persist — points allowed at 0.277 and yards allowed at 0.260 — carry **22.3%**. That is why
total D/ST points stick at only **r = 0.22–0.27** in every league.

*And the way out is the market, more broadly than expected.* Vegas's implied points
**allowed** beats the prior season on **seven of eight** components — sacks **0.464 against
0.203**, interceptions **0.357 against 0.113**, fumble recoveries **0.193 against 0.015**,
points allowed **0.816 against 0.277**. A weak opponent offence produces sacks and
turnovers, and the market prices opponent offences.

**And game script is the largest environment effect in the repo, running opposite to the
kicker's.** A heavy favourite's defence scores **9.18** a game against a heavy underdog's
**1.06** in the default six — a factor of nine. The mechanism is realised rather than
predicted: opponent pass share goes **47.8% in a blowout loss to 63.2% in a blowout win**
while rush attempts collapse 32.9 → 20.6, taking sacks ×2.2, interceptions ×3.6 and defensive
touchdowns **×7**. Garbage time costs nothing — points allowed in a 15+ win is 11.49, the
lowest band. Best pre-game cell is a **heavy favourite in a low total, 8.55 against 0.88,
+130.3 over a season** — and the total runs the *opposite* way from
[plan 29](29-kicker-model.md)'s kicker, so the two positions want opposite games. Critically,
**the spread survives the implied points allowed here** (+2.20 to +2.75 a game within band)
where for a kicker it was the same number renamed: the implied total prices the opponent's
offence, the spread prices who will be ahead, and only the second one makes anybody throw.

**Depends on:** [10](10-scoring-registry.md) — the per-league ladders, at the right slot ·
[11](11-per-slot-scoring.md) — the slot dimension, without which every defensive rule reads
as 0.0 · [21](21-coaching-and-scheme.md) — the coordinator table
**Supersedes the D/ST half of:** [13](13-dst-from-vegas-lines.md) — that plan's
`E[f(X)]`-over-tiers instinct is correct and is quantified here; its scope was too narrow
**Shares:** [29](29-kicker-model.md)'s `Scripts/vegas.py`, sign assertion included
**Feeds:** [28](28-outcome-distributions.md) — a tiered score is a distribution problem by
construction

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

**32 defences on every board, projected by one source.** `usg_arm` is null for all of them —
the usage model abstains — and D/ST is the position [`STATE_OF_THE_REPO.md`](../STATE_OF_THE_REPO.md)
calls "the only position with zero market coverage". FantasyPros, Pinnacle and BetOnline
arrive imputed. So the entire position is ESPN's opinion, unchallenged, in nine leagues that
score it nine different ways.

And unlike every other position, **the scoring is non-linear**. A points-allowed ladder pays
in steps, so a defence's fantasy value is not a function of its mean — it is an integral over
its weekly distribution. Nothing in the pipeline currently does that arithmetic.

## Evidence

2016–2025. Team-defence built from `player_weeks`' `def_*` columns aggregated by the
defending team, points allowed from `schedules.parquet`, yards allowed from the opponent's
offensive production. 315 team-defence seasons of 14+ games, 283 consecutive pairs.
Reproduce with `python -m Scripts.dst.evidence`.

### Every component, ranked by whether it is a real quantity

| component | r YoY | per game | verdict |
|---|---|---|---|
| tackles for loss | **0.389** | 4.22 | **signal** |
| QB hits | 0.322 | 5.65 | signal-ish |
| **points allowed** | **0.277** | 22.75 | weak |
| **yards allowed** | **0.260** | 357.83 | weak |
| **sacks** | **0.203** | 2.37 | weak |
| interceptions | 0.113 | 0.78 | **noise** |
| passes defensed | 0.109 | 4.33 | noise |
| fumbles forced | 0.101 | 0.80 | noise |
| **defensive TDs** | **−0.052** | 0.07 | **noise** |
| safeties | −0.015 | 0.03 | noise |
| **fumble recoveries** | **0.015** | 0.52 | **noise** |

**The cruel structure of the position, in two rows.** The two most *valuable* events — a
defensive touchdown at 5–6 points and a safety at ~1.2 — have **negative** year-over-year
correlation. And the two components that persist best, tackles for loss and QB hits, are
**scored in one league out of nine** (`defensiveStuffs` at 0.75, in the IDP league only).
The stickiest thing about a defence is a thing almost nobody pays for.

### What each league actually pays for

Share of absolute scoring weight, 2016–2025, each league's own slot-16 rules:

| league | pts allowed | yards allowed | sacks | INTs | fumb rec | ret TDs | safeties |
|---|---|---|---|---|---|---|---|
| winfield_football | 7.6% | 14.7% | 33.8% | 22.3% | 14.9% | 5.9% | 0.8% |
| knights_ffl | 7.6% | 14.7% | 33.8% | 22.3% | 14.9% | 5.9% | 0.8% |
| twelve_dudes_one_cup | 7.6% | 14.7% | 33.8% | 22.3% | 14.9% | 5.9% | 0.8% |
| fields_league | 7.6% | 14.7% | 33.8% | 22.3% | 14.9% | 5.9% | 0.8% |
| john_atl_league | 10.1% | 11.4% | 34.1% | 22.5% | 15.1% | 6.0% | 0.8% |
| john_pc_league | 9.8% | 11.5% | 34.2% | 22.5% | 15.1% | 6.0% | 0.8% |
| big_red_fantasy_football | 8.9% | **none** | 39.6% | 26.1% | 17.5% | 6.9% | 1.0% |
| weenieless_wanderers | **20.8%** | **none** | 34.5% | 22.7% | 15.2% | 6.0% | 0.8% |
| **gop_degenerates** | **40.7%** | **none** | 25.8% | 17.0% | 11.4% | 4.5% | 0.6% |

**Three groups, not nine.** Six leagues are the ESPN default and are a bet on sacks and
turnovers — 71.0% of the score on components with `r` of 0.203, 0.113 and 0.015. GOP is a
different game entirely: **40.7%** of its D/ST score is the points-allowed ladder, the
single most forecastable component, so GOP is the league where a model can help most.
Weenieless sits between. Three of nine leagues have **no yards-allowed tiers at all.**

### And yet the nine leagues agree on who is good

2025, each league's own rules, ranking all 32 defences:

- **median pairwise rank correlation 0.968**
- lowest pair `gop_degenerates` vs `john_pc_league` = **0.918**
- highest pair `twelve_dudes_one_cup` vs `winfield_football` = **1.000**

| league | 2025 top five |
|---|---|
| winfield_football | HOU, SEA, CLE, DEN, MIN |
| knights_ffl | HOU, SEA, CLE, DEN, MIN |
| twelve_dudes_one_cup | HOU, SEA, CLE, DEN, MIN |
| john_pc_league | HOU, SEA, CLE, DEN, MIN |
| fields_league | HOU, SEA, CLE, DEN, MIN |
| john_atl_league | HOU, SEA, CLE, MIN, DEN |
| gop_degenerates | HOU, SEA, DEN, JAX, CHI |
| weenieless_wanderers | HOU, SEA, DEN, JAX, LA |
| big_red_fantasy_football | HOU, DEN, SEA, JAX, CLE |

**This is the finding that sets the architecture.** The ladders differ, the component mix
differs by more than 5× on points allowed, and the *ordering* is nearly identical — because
a good defence is good at all of it at once. So the plan does **not** build nine models. It
builds one projection of the component vector, then lets `proj_to_score` apply each ladder,
exactly as the offence already works. The 0.918 pair is the check that this is an
approximation with a known size, not an identity.

### Vegas reaches the noisy components, which was not expected

Implied points **allowed** = `total_line / 2 − own margin / 2`, contemporaneous, against the
prior season's own value for the same component:

| component | r vs Vegas | r YoY | best via |
|---|---|---|---|
| points allowed | **0.816** | 0.277 | **Vegas** |
| yards allowed | **0.702** | 0.260 | **Vegas** |
| **sacks** | **−0.464** | 0.203 | **Vegas** |
| **interceptions** | **−0.357** | 0.113 | **Vegas** |
| tackles for loss | −0.289 | **0.389** | prior season |
| **fumble recoveries** | **−0.193** | 0.015 | **Vegas** |
| defensive TDs | −0.185 | −0.052 | **Vegas** |
| safeties | −0.083 | −0.015 | **Vegas** |

Negative signs are the right direction: a *lower* implied points allowed means a better
defence or a weaker opponent, and that produces *more* sacks and turnovers.

**This is the reframing.** [Plan 13](13-dst-from-vegas-lines.md) scopes Vegas to the
points-allowed tier, which is 7.6% of the score in six leagues — a small prize. But Vegas is
also the **best available predictor of sacks and interceptions**, which are 56% of the score
in those same leagues, and it is **13× better than history** at fumble recoveries. The
market's value here is not the tier; it is that opponent offensive quality drives every
defensive event, and the market prices opponent offences.

Tackles for loss is the sole exception and the sole component Vegas does not own — and it is
scored in one league.

### Game script, and it is the mirror image of the kicker's

5,198 team-defence games with a closing line, scored under two structurally different
ladders: Winfield Football (the default six — sacks + INTs = 56% of the score) and GOP
(points allowed 40.7%). By the defence's own team's pre-game spread:

| own line | n | **default six** | **GOP** | sacks | INTs | fum rec | def TD | pts allw | yds allw | opp pass att |
|---|---|---|---|---|---|---|---|---|---|---|
| dog 10.5+ | 261 | **1.06** | 6.26 | 1.77 | 0.59 | 0.41 | 0.054 | 29.35 | 396.5 | 34.1 |
| dog 6.5–10.5 | 602 | 2.22 | 7.05 | 1.96 | 0.64 | 0.48 | 0.060 | 27.50 | 385.9 | 33.6 |
| dog 3.5–6.5 | 760 | 3.08 | 7.95 | 2.21 | 0.71 | 0.48 | 0.047 | 25.40 | 380.0 | 34.3 |
| dog 0.5–3.5 | 961 | 4.45 | 8.94 | 2.27 | 0.75 | 0.51 | 0.059 | 22.95 | 357.6 | 33.9 |
| pick'em | 1242 | 5.45 | 9.86 | 2.50 | 0.86 | 0.52 | 0.078 | 21.44 | 351.6 | 34.3 |
| fav 3.5–6.5 | 623 | 5.97 | 10.23 | 2.59 | 0.79 | 0.61 | 0.079 | 20.39 | 344.6 | 34.8 |
| fav 6.5–10.5 | 543 | 7.13 | 11.13 | 2.83 | 0.90 | 0.60 | 0.077 | 18.85 | 329.4 | 33.5 |
| **fav 10.5+** | 206 | **9.18** | **12.91** | 2.80 | 1.06 | 0.57 | 0.141 | 14.99 | 294.2 | **32.6** |

**A +8.12 point swing a game in the default six** — from 1.06 to 9.18, a factor of nine, and
the largest game-environment effect measured anywhere in this repo. Every component improves
together, which is the same "good defences are good at all of it" fact that made one model
serve nine leagues.

**But note the last column.** Opponent pass attempts do **not** rise with favouritism — 34.1
for a heavy underdog against 32.6 for a heavy favourite, slightly *down*. So the pre-game
gain is not the game-script story at all: it is simply that favourites face worse offences
(396.5 yards allowed down to 294.2). The passing story is real, and it only appears once the
game is actually played.

### The game-script channel, and it is entirely a realised-margin effect

| realised | n | **default six** | **GOP** | sacks | INTs | fum rec | def TD | pts allw | opp pass | opp rush | **pass share** |
|---|---|---|---|---|---|---|---|---|---|---|---|
| lost by 15+ | 758 | **−1.09** | 3.96 | 1.52 | 0.37 | 0.26 | 0.025 | 34.49 | 30.1 | 32.9 | **47.8%** |
| lost by 8–14 | 568 | 1.62 | 5.77 | 1.75 | 0.42 | 0.38 | 0.030 | 27.66 | 30.3 | 31.4 | 49.1% |
| lost by 1–7 | 1257 | 2.73 | 7.21 | 2.06 | 0.57 | 0.42 | 0.036 | 25.00 | 33.4 | 28.8 | 53.6% |
| won by 1–7 | 1272 | 5.41 | 10.10 | 2.57 | 0.87 | 0.59 | 0.061 | 20.89 | 36.4 | 24.5 | 59.8% |
| won by 8–14 | 570 | 8.62 | 13.17 | 3.09 | 1.19 | 0.74 | 0.114 | 16.65 | **37.4** | 21.8 | 63.2% |
| **won by 15+** | 773 | **11.86** | **15.70** | **3.32** | **1.35** | **0.78** | **0.176** | **11.49** | 35.5 | **20.6** | **63.2%** |

**A 12.95-point swing**, and the mechanism is explicit: opponent pass share goes **47.8% →
63.2%** while rush attempts collapse from 32.9 to 20.6. A trailing offence throws, and
throwing is what produces sacks (1.52 → 3.32, ×2.2), interceptions (0.37 → 1.35, ×3.6) and
defensive touchdowns (0.025 → 0.176, **×7**).

**Two details worth keeping.** Opponent pass *attempts* peak in the won-by-8–14 band (37.4)
rather than in the blowout — at 15+ the winning team's own offence is also killing clock, so
there are fewer total plays. And **garbage time does not cost anything**: points allowed in a
15+ win is 11.49, the lowest band on the table. The "they'll score late once we're up three
scores" worry does not survive contact with ten seasons.

### Best pre-game script: a low total, and a big favourite

Total × spread. Default six:

| | dog 6.5+ | dog 0.5–6.5 | fav 0.5–6.5 | fav 6.5+ |
|---|---|---|---|---|
| **total ≤ 43.5** | 2.77 (n=328) | 5.21 (n=669) | 6.90 (n=721) | **8.55 (n=287)** |
| total 43.5–47.5 | 1.61 (n=320) | 3.67 (n=566) | 5.58 (n=624) | 7.82 (n=272) |
| total > 47.5 | **0.88 (n=215)** | 2.17 (n=486) | 3.91 (n=520) | 6.23 (n=190) |

GOP, same cells: 7.18 / 9.35 / 10.94 / **12.07** · 6.72 / 8.43 / 9.88 / 11.74 · **6.37** /
7.42 / 8.79 / 10.77.

**Best: a heavy favourite in a low-total game — 8.55 a game, against 0.88 for a heavy
underdog in a shootout. +7.66 a game, +130.3 over a season.** For GOP the same cells give
+5.70 a game and +96.9, compressed because its points-allowed ladder is bounded while sack
and interception counts are not.

**And the total runs the opposite way from the kicker's.** [Plan 29](29-kicker-model.md) finds
a kicker's best environment at a *moderately high* total; a defence's is monotonically better
as the total *falls*, in every spread column, and sacks follow the same gradient (2.92 at a
low total against 2.68 at a high one for the same heavy favourite). A low total is the
market saying "bad offences", and a defence is the one position that wants to face one. The
two positions want opposite games, which is worth knowing before stacking a kicker and a
defence from the same slate.

### The spread survives the implied points allowed — unlike for kickers

The test that mattered most in plan 29, where the spread turned out to be the implied total
renamed. Here it does **not**. Default six D/ST points, holding implied points allowed fixed:

| implied pts allowed | dog 6.5+ | dog 0.5–6.5 | fav 0.5–6.5 | fav 6.5+ |
|---|---|---|---|---|
| ≤ 19.5 | — | 8.05 (n=42) | 7.18 (n=605) | 8.21 (n=586) |
| 19.5–23.5 | 3.70 (n=53) | 5.09 (n=669) | 5.30 (n=1025) | **5.90 (n=155)** |
| > 23.5 | 1.75 (n=810) | 2.85 (n=1010) | 3.03 (n=235) | 4.50 (n=8) |

Sacks, same grid: — / 3.24 / 2.67 / 2.85 · 2.53 / 2.42 / 2.52 / 2.72 · 1.86 / 2.09 / 2.21 /
2.50.

In the two well-populated bands the spread still adds **+2.20** and **+2.75** points a game
from heavy dog to favourite, and sacks still climb within band. **This is a real second
input, and the reason is structural:** implied points allowed prices the *opponent's offence*,
while the spread prices *who will be ahead* — and being ahead is what makes the opponent
throw. For a kicker those two collapse into one quantity; for a defence they are two channels
with different targets.

So the D/ST model carries **both**: implied points allowed drives the points- and
yards-allowed tiers, and the spread drives the sack, interception and defensive-touchdown
components. That is the opposite of plan 29's specification, and it is a measurement rather
than a preference.

### Total D/ST points barely persist, in every league

Per-game D/ST fantasy points, own rules, YoY:

| league | r | league | r |
|---|---|---|---|
| john_pc_league | 0.267 | john_atl_league | 0.260 |
| fields_league | 0.265 | weenieless_wanderers | 0.256 |
| knights_ffl | 0.265 | gop_degenerates | 0.244 |
| twelve_dudes_one_cup | 0.265 | big_red_fantasy_football | 0.220 |
| winfield_football | 0.265 | | |

A tight band, 0.220–0.267, and it is exactly what the component table predicts: a weighted
average dominated by quantities that do not persist. **No amount of modelling makes last
season's defence a good predictor of this season's.** The only route is a signal from
outside the defence's own history, which is the previous section.

### Tiered scoring: `E[f(X)]` is not `f(E[X])`, and the gap is 16 points wide

Winfield Football's points-allowed ladder, at slot 16: `0 → +5`, `1–6 → +4`, `7–13 → +3`,
`14–17 → +1`, `18–27 → 0` (no rule), `28–34 → −1`, `35–45 → −3`, `46+ → −5`.

Mean weekly points allowed across the 315 team-seasons is 22.75 with a **weekly SD of 9.57**
— far wider than the 4-to-7-point tiers, so the convexity bites hard:

| | f(E[X]) — score the season mean | E[f(X)] — score every week | bias |
|---|---|---|---|
| all defences | −0.78 | +3.28 | **+4.06** |
| best third | 1.40 | **13.64** | **+12.24** |
| middle third | 0.00 | 4.19 | +4.19 |
| worst third | −3.73 | −7.99 | −4.26 |

Mean absolute per-team error **7.21 points a season**, worst cases −13.0 and +23.0.

**Scoring the mean compresses the position.** It understates the best defences by 12.24
points and overstates the worst by 4.26 — a **16.5-point distortion across the range**, on
the one component whose entire job is to separate defences. Any projection that assigns a
defence a mean points-allowed figure and reads a tier off it is wrong by more than the tier
values themselves. The model must carry a weekly **distribution** and integrate the ladder
over it, which is the same machinery [plan 28](28-outcome-distributions.md) is building and
the reason these two plans are siblings.

### Coaching and personnel: measured, and both currently confounded

**Coordinator changes**, from `coaching_staff.parquet`'s `defensive_coordinator`, n=95 pairs:

| group | n | next-season pts allowed/gm | change vs prior | r(prior, next) |
|---|---|---|---|---|
| DC stayed | 64 | 21.30 | +0.29 | 0.157 |
| DC changed | 31 | 21.91 | **−1.62** | 0.134 |
| DC and HC both changed | 9 | 23.35 | **−2.94** | 0.363 |

Defences that change coordinators **improve** by 1.62 points a game, and by 2.94 when the
head coach goes too. **This is almost certainly mean reversion, not coaching.** Teams fire
coordinators because the defence was bad, and a bad defence regresses toward the mean
whoever is calling it. The `r(prior, next)` column is the tell: it barely moves (0.157 →
0.134), so a coordinator change is not resetting the defence's identity — it is a marker for
*having been bad*, which the prior-season figure already carries.

**Pre-registered as a likely null, with the test that would settle it:** stratify by prior
points allowed, then compare change-vs-stay *within* stratum. Only a difference that
survives that is a coaching effect. Plan 21 already measured coach priors **out** of both
usage arms because the depth chart carried their signal, so the prior here should be
skeptical.

**Personnel continuity**, share of a season's defensive snaps taken by players on the same
team the previous year, mean 0.667:

| continuity tertile | n | mean continuity | r(prior, next) pts allowed |
|---|---|---|---|
| low | 95 | 0.511 | 0.145 |
| mid | 94 | 0.680 | 0.142 |
| high | 94 | 0.812 | **0.215** |

Monotone in the expected direction — a defence that keeps its players is more like itself
next year — but **all three are low**, and 0.145 → 0.215 does not rescue a position whose
best history-based signal is 0.277. Continuity is a **modifier on how much to trust the
prior season**, not a predictor in its own right, and that is how it should enter: as a
weight on the history term against the Vegas term, not as a feature beside them.

### Components no available data can score

Scored in **all nine** leagues and absent from this measurement: `blockedKick`,
`defensiveBlockedKickTD`, `kickoffReturnTouchdowns`, `puntReturnTouchdowns`. GOP
additionally scores `defensiveAssistedTackles`, `kickoffReturnYards` and `puntReturnYards`.

By the return-TD weight in the component table these are ~6% of the score, and by the
`r = −0.052` on defensive touchdowns they are near-certainly noise — but that is an
inference from a neighbouring quantity, not a measurement, and the plan should say so rather
than assume it. Special-teams return data is available from nflverse play-by-play; pulling
it is phase 1's job so the claim can be made properly.

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

**The D/ST ceiling, by quintile of season-average implied points allowed:**

| quintile | input | n | pts/gm | over 17 |
|---|---|---|---|---|
| q1 (best) | 20.02 | 64 | 6.95 | **118.2** |
| q2 | 21.57 | 63 | 5.54 | 94.1 |
| q3 | 22.50 | 61 | 4.71 | 80.1 |
| q4 | 23.50 | 63 | 4.08 | 69.4 |
| q5 (worst) | 25.35 | 63 | 2.24 | **38.0** |

**80.2 points from worst quintile to best, a ratio of 3.1×** — against the kicker's 1.24×, and
monotone across all five. So the two positions land in opposite places on the draft board even
though both are ~89% weekly: **D/ST is genuinely draftable and a kicker essentially is not.**
A defence in the top environment quintile is worth three of one in the bottom, and the market
tells you which is which before a snap is played.

**One honest caveat on what that quintile is.** Season-average implied points allowed *is* the
market's assessment of the defence, so this table is not "script separate from quality" — for a
season projection those are the same quantity, and that is exactly why it works. The
script-versus-quality distinction only becomes separable weekly, where the spread carries the
game-script channel independently (see above).

## Fix

### One model of the component vector, nine ladders applied at the end

Project the **vector** — sacks, interceptions, fumble recoveries, defensive TDs, safeties,
points allowed, yards allowed, return TDs, blocked kicks — per team-week, then let
`proj_to_score` apply each league's slot-16 rules. Justified by the 0.968 rank agreement:
the leagues disagree about prices, not about defences.

### Weekly, not seasonal, because the ladder is non-linear

The unit of projection is the **team-week distribution**, not the team-season mean. Each
week gets a predicted points-allowed distribution from that week's own line, the ladder is
integrated over it, and the season is the sum of weekly expectations. This is the only way
to avoid the 16.5-point compression measured above, and it has a free benefit: a per-week
D/ST projection is what [plan 08](08-frontend-weekly-views.md)'s streaming decisions need,
so the season number falls out of the weekly one rather than the reverse.

### The blend, and what carries what

- **Points and yards allowed** → the game's own implied points allowed (`r` 0.816 / 0.702).
  This channel prices the *opponent's offence*.
- **Sacks, interceptions, fumble recoveries, defensive TDs, safeties** → implied points
  allowed (`r` 0.464 / 0.357 / 0.193 / 0.185 / 0.083) **and the spread**, which survives it:
  within an implied-points-allowed band the spread still adds +2.20 to +2.75 D/ST points a
  game and moves sacks 1.86 → 2.50. This channel prices *who will be ahead*, and being ahead
  is what makes an opponent throw — opponent pass share runs 47.8% in a blowout loss against
  63.2% in a blowout win. Shrink each component toward the positional mean in proportion to
  how weak its correlation is; fumble recoveries and safeties should end up **almost entirely
  the positional mean**, which is the correct answer for a coin-flip and must not be dressed
  up as a projection.
- **The total, with a negative sign.** A defence's environment improves monotonically as the
  game total *falls* — 8.55 points a game at ≤43.5 against 6.23 at >47.5 for the same heavy
  favourite — because a low total is the market naming bad offences. It is not redundant with
  implied points allowed, since `implied allowed = total/2 − line/2` leaves the total and the
  spread independently identified.
- **Tackles for loss and QB hits** → prior season, continuity-weighted. The one place
  history wins, and it only matters in the IDP league.
- **Continuity** → the weight between the history term and the market term, per the tertile
  table.

### Artifacts and code

- `Scripts/vegas.py` — shared with [plan 29](29-kicker-model.md). Implied totals both ways,
  with the sign asserted in a test.
- `Scripts/dst/evidence.py` — every table above, deterministic.
- `Scripts/dst/tiers.py` — parse a ladder out of the registry at slot 16 and integrate it
  over a distribution. Reusable: the yards-allowed ladder is the same shape.
- `Scripts/dst/model.py` — the weekly component projection, writing `DST_<stat>`.
- `Data/NFL/team_defence.parquet` — the component table. Not league-scoped.

### Phases

| Phase | What | Gate |
|---|---|---|
| 1 | `Scripts/vegas.py` + special-teams return pull, closing the six data gaps | G-DST0 |
| 2 | `Scripts/dst/evidence.py` — the tables above | — |
| 3 | `Scripts/dst/tiers.py` — ladder parsing and `E[f(X)]` integration | G-DST1 |
| 4 | Weekly component projection from the market | G-DST2 |
| 5 | Continuity as the history/market weight | G-DST3 |
| 6 | `DST_` as a source for D/ST only, weight 0.0 first | G-DST4 |
| 7 | Coordinator change, **stratified by prior points allowed** | G-DST5 |
| 4b | The spread as a second channel on sacks / INTs / def TDs | G-DST6 |
| — | nine separate models | **Do not build** — rank agreement 0.968 |
| — | a per-defence fumble-recovery or safety rate | **Do not build** — `r` 0.015 and −0.015 |

### Gates, pre-committed

**G-DST0 — the sign, and the gap.** As [plan 29](29-kicker-model.md)'s G-K0:
`r(spread_line, home_score − away_score)` asserted positive. Plus: the special-teams pull
must reduce unscored rules from six to zero, or the component shares above are reported with
a named residual rather than silently normalised.

**G-DST1 — the tiered integral must beat scoring the mean.** On held-out seasons, predict each
team's D/ST tier points two ways. **Bar: `E[f(X)]` must cut MAE by ≥15% against `f(E[X])`.**
The measured compression is 7.21 points of mean absolute error against a positional spread
of ~20, so anything under 15% means the weekly distribution was estimated too poorly to
realise a bias that is definitely there.

**G-DST2 — the market must beat both baselines.** Out-of-sample MAE on season D/ST points, per
league, against (a) prior-season D/ST points and (b) `ESPN_projected_total`. **Bar: 10%
better than (a) in all nine leagues, and better than (b) in at least six.** Beating history
is the easy half given `r = 0.22–0.27`; ESPN is the real competitor and may already price
the market.

**G-DST3 — continuity must earn its place as a weight, not a feature.** Fit with a fixed
history/market weight, then with the continuity-varying weight. **Bar: +0.02 out-of-sample
R² on points allowed**, the bar prior snap share cleared in
`Scripts/usage/features.py`:721. If it fails, ship a fixed weight.

**G-DST6 — the spread must earn its place, pre-registered as the opposite of plan 29's
null.** Fit the sack/interception/defensive-TD components on implied points allowed alone,
then add the spread. **Bar: ≥5% out-of-sample MAE improvement on those components.** The
measurement says the spread carries +2.20 to +2.75 points a game net of the implied number,
so unlike [plan 29](29-kicker-model.md)'s G-K5 this gate is expected to **pass** — and the two
gates being pre-registered in opposite directions on the same variable is the point. If it
fails here, the asymmetry between the two positions is not real and both models simplify.

**G-DST4 — cross-position neutrality.** Adding `DST_` must not move any non-D/ST position's
median `TRUE_`/`ESPN_` ratio by more than 0.02.

**G-DST5 — the coordinator effect, stratified.** Within tertiles of prior-season points
allowed, DC-changed against DC-stayed. **Bar: a consistent-sign difference of ≥1.0 points
allowed per game in at least two of three strata.** The unstratified −1.62 is expected not
to survive; recording that it did not is the deliverable, in the manner plan 21 recorded the
coach priors measuring out.

**And a false-positive clause ordered before all of them:** if replacing each game's implied
points allowed with the *season-average* implied points allowed loses less than 20% of the
model's gain, then the weekly machinery is not doing the work and the model is a
season-level market read wearing a weekly costume. Report that first, because it is the
cheapest way this plan fools itself.

## Effort

L. Phase 1 shares `vegas.py` with plan 29 and adds a special-teams pull, ~half a day. Phase
3's ladder integration is the piece with no precedent in the repo and is where the care goes,
~1 day. Phases 4–5 ~1.5 days. Phases 6–7 ~0.5 day each.

**Both surfaces are worth building, and the season one is not the consolation prize.** 89% of
weekly D/ST variance is week-to-week, so the streaming edge is larger — but the season-average
environment still spreads the position 118.2 to 38.0 points, which is a genuine draft signal
and three times the kicker's. The weekly model produces the season one by summation anyway, so
there is one build here, not two.

**The standalone value is unusually high here**, because three of the findings are decisions
rather than models: one model serves nine leagues; GOP's D/ST is a different position from
everyone else's and rewards the forecastable component 5× more; and last season's defence is
worth almost nothing, so the draft-room instinct to buy last year's top unit is priced on an
`r` of 0.24.

## Postscript — what measuring this turned up

- **The registry must be read at slot 16.** Collapsing base and slot-16 rules into one dict
  gives every defensive rule the base value of `0.0`, because an offensive player scores
  nothing for a defensive stat. It produced a points-allowed ladder of all zeros and a
  divide-by-zero before it produced anything wrong-but-plausible, which is the good failure
  mode. [Plan 11](11-per-slot-scoring.md) is the reason the dimension exists.
- **Winfield's ladder has a hole, and it is real.** There is no rule for 18–27 points
  allowed, so that band scores 0.0 — not a missing pull but how the league is configured.
  A tier integrator must treat "no rule" as zero rather than interpolating between
  neighbours, or it will invent points ESPN never awards.
- **The Vegas sign.** Documented in plan 29's postscript; it bit the shared derivation and
  is the reason `Scripts/vegas.py` exists as one module with one test rather than two
  inlined formulas.
- **`def_tds` cannot be split into interception and fumble return touchdowns** in
  `player_weeks`, and the leagues price them separately (both ~5.0, so the mean is used).
  Where a league prices them differently the component share is approximate; play-by-play
  can separate them if it ever matters.
