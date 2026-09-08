# 43 — TOMCAT out of the season-long blend

**Status:** COMPLETE

**Priority:** High · **Effort:** M · **Where it stands:** **Shipped 2026-09-07 — an owner
decision, taken on a measurement.** TOMCAT carried an equal vote in `TRUE_` from 2026-08-17 and was withdrawn
from the season-long blend on 2026-09-07, three weeks later and one day before the last
draft. The model is not retired: it still runs, its `USG_` stat lines are still merged
onto the board, and the weekly head remains open work.
**Depends on:** [18](18-season-usage-model.md) · [31](31-team-coherent-tomcat.md) ·
[39](39-source-basis.md) · **Feeds:** [19](19-weekly-usage-model.md) ·
[29](29-kicker-model.md) · [30](30-dst-model.md)

---

## Problem

TOMCAT's projected NFL does not run the ball enough, and nothing in the pipeline
notices.

The trigger was a single board cell: **Jahmyr Gibbs at 303.7 points against a field
where no source is below 353.** He is the first pick in the league. Every source agrees
on what kind of player he is; only ours prices him like a committee back.

## Evidence

### The gap is volume, and only volume

Gibbs' 60-point shortfall against the ESPN/FantasyPros/Athletic mean decomposes as
−29.8 rushing yards, −22.6 rushing touchdowns, −5.6 receptions, −5.0 receiving yards,
−2.2 receiving touchdowns. **80% of it is rushing.**

Across the top 50 ADP skill players, the rates agree and the opportunity does not:

| | TOMCAT | field | ratio |
|---|---|---|---|
| yards per carry | 4.510 | 4.479 | **1.007** |
| rush TD per carry | 0.0332 | 0.0333 | **0.997** |
| yards per target | 7.306 | 7.777 | 0.939 |
| catch rate | 0.652 | 0.687 | 0.948 |
| **carries** | | | **0.857** |
| **targets** | | | **0.890** |

The model agrees with everybody about how good these players are per touch. It gives
them ~14% fewer touches.

### It is not a slate error, and `to_full_slate` is not at fault

Gibbs' line is `pred_carries_pg` 13.037 × 17 = 221.6 exactly — the number comes straight
out of the per-game head. And the shortfall is **flat across how much time a back missed
last year** (0.832 / 0.848 / 0.849 for 8–12, 13–15, 16–17 games played), so no residual
availability term is leaking through.

What it is: TOMCAT sits below each back's **own realised 2025 per-game-played rate** at
every volume level, while the field sits on top of it.

| 2025 actual | n | TOMCAT / his own rate | field / his own rate |
|---|---|---|---|
| 5–9 car/gm | 11 | 0.870 | 1.035 |
| 9–13 | 13 | 0.818 | 0.956 |
| 13–16 | 15 | 0.848 | 0.959 |
| 16+ | 7 | 0.828 | 1.001 |

Gibbs ran 14.29/gm in 2025 and 14.71 in 2024, never missing a game. TOMCAT projects
13.04.

### The disqualifying number is a team total

A team's carry count is not a matter of opinion, which is what makes this a level error
rather than a disagreement — plan 39's rule, applied to our own source.

| | carries per team |
|---|---|
| realised 2023 / 2024 / 2025 | **454 / 450 / 465** |
| ESPN 2026 | 462 |
| The Athletic 2026 | 452 |
| ESPN/FP/ATH mean 2026 | 466 |
| **TOMCAT 2026** | **384** |
| TOMCAT, abstentions filled from the field | 417 |

On the players TOMCAT and the field both price, matched row for row:

| | ratio |
|---|---|
| `rushingAttempts` | **0.903** (QB 0.906, RB 0.903) |
| `passingAttempts` | **0.891** |
| `receivingTargets` | **0.999** (RB 1.02, TE 1.03, WR 0.98) |

### Why targets escape and carries do not

`Scripts/usage/coherence.py` holds three accounting identities:

```
("passingYards",       "receivingYards")
("passingTouchdowns",  "receivingTouchdowns")
("passingCompletions", "receivingReceptions")
```

All three are passing↔receiving. **There is no rushing identity.** The receiving side
gets pulled onto the passing side and lands at 0.999 in aggregate; the rushing head's
level error travels to the board untouched, because nothing constrains a team's carries
the way the passing side is constrained.

### What it cost the blend

Rebuilt with and without TOMCAT through the real pipeline — `compute_weighted_stats` →
`reconcile_team_totals` → vacancy transfer → reconcile → `proj_to_score`. The
with-TOMCAT rebuild reproduces the shipped board to **max error 0.0000 over 1,036 rows**,
so the counterfactual is exact rather than approximate.

| position | level with ÷ without | mean positional-rank move |
|---|---|---|
| D/ST | **0.879** | 1.5 |
| WR | 0.975 | 1.6 |
| K | 0.977 | 2.1 |
| TE | 0.978 | 1.3 |
| RB | 0.981 | 1.5 |
| QB | 0.990 | 1.1 |

**12.5% of cross-position distortion, and nearly all of it is the defence arm** — 1.5%
across the skill positions alone. The D/ST arm's ordering correlates with the field at a
Spearman of **0.168** and compresses the position's spread to 0.66×, knocking the top
defences down 15–22 points and lifting the bottom ones.

And it discounted precisely the picks the board exists to get right. Median
`USG_Points / ESPN_Points` by ADP band, stable across all nine leagues:

| ADP | knights | range across 9 leagues |
|---|---|---|
| 1–50 | **0.836** | 0.813–0.836 |
| 50–100 | 0.873 | 0.848–0.895 |
| 100–150 | 0.901 | 0.852–0.920 |
| 150+ | 1.089 | 0.986–1.154 |

### The role reads were fine — it really is the pie

Worth recording because it was the first hypothesis and it was wrong. TOMCAT gives Gibbs
**0.598** of Detroit's carries; the field gives him 0.625; he had 0.550 in 2025. Both
move him up after David Montgomery's departure to Houston. Across all 71 projected
backfields the median share gap is **0.035** and **68% agree within 0.05**.

Holding TOMCAT's own room shares and putting each team on the field's carry budget moves
the top-45 RBs from **0.847 to 0.970** of consensus, and Gibbs from 303.7 to 347.7. The
slice is right; the pie is 10% too small.

## The decision

**Withdraw `USG` from `WEIGHTS`, and remove it rather than zero it**, so a reader
counting entries counts the sources that vote. Removed from `PROJECTION_PREFIXES` and
from `proj_to_score`, so no `USG_Points` is written and no `USG_PosRank` /
`USG_PosRankDelta` derived from it. Off the draft board along with `Exp G`, `Role %` and
`Model Evidence`, and the Sheet tab's availability lens with them.

**What is deliberately kept.** The model runs, and its `USG_` stat lines are still merged
onto the board — unweighted and unpriced — so `Scripts.lab.sources` keeps measuring them
against the field, the outcome distributions in plan 28 keep the decomposition they are
built on, and reversing this is one line. The lower-case `usg_*` diagnostics
(`usg_depth_rank`, `usg_role_cohort`) are depth-chart facts rather than projections and
stay load-bearing for the injury vacancy transfer and `_withdraw_usage_on_role`.

## What this costs, stated rather than smoothed over

* **Kickers and team defences lose their only second opinion.** The kicking arm was
  switched on over an unpassed G-K2 gate precisely because the alternative was a starting
  slot in nine leagues at 100% ESPN (plan 29). That alternative is now what we have.
* **`receivingTargets` drops from three real voters to two.** FantasyPros publishes no
  target column and neither book prices one, so it is back to ESPN and The Athletic — the
  thinnest coverage on the board sitting on its most forecastable quantity. TOMCAT's
  target head was the one volume head that was well calibrated (0.999); it goes anyway,
  because a source votes as a source.
* **No source on the board prices availability.** A known absence is still docked by ESPN
  and FantasyPros directly, but there is no longer a per-player estimate of the games a
  healthy man misses anyway.

## What this does not decide

**The weekly arm.** `USG` was never in `WEEKLY_PREFIXES` — TOMCAT has no weekly head and
plan 19 is unstarted — so nothing here touches it. A weekly arm would be fitted on
in-season usage and judged on its own evidence. It would, however, inherit this problem:
a team-total identity is not a season-long concern.

**G2.** `Data/G2/2026/` still holds the pre-season board blended both ways, frozen
2026-08-09, and it can still be scored against realised 2026 after the season. This
withdrawal was decided on a level error measured against team carry totals, not on G2.
A "no" from G2 would confirm the removal by a second route; a "yes" would say the level
error was worth fixing rather than routing around. `Scripts.lab.g2.VARIANTS` is now
pinned literally rather than read from `WEIGHTS`, because a live read would make both
variants identical and a re-archive would overwrite the one artifact in this repo that
cannot be rebuilt.

**Whether the model is good.** It beats the naive draft heuristic out of sample on every
metric at every position, in 26 of 28 season-position cells across a seven-fold
walk-forward, and it is the most independent source ever registered here (+0.113 residual
independence against the best external's +0.068). None of that is retracted. A source can
be informative and still be on the wrong level, and the level is what a blend adds up.

## Owed

1. **A team rushing identity in `Scripts/usage/coherence.py`**, anchored on realised team
   carries — 450–465 over three seasons — and never on the gap to ESPN, per plan 39. This
   is the single change most likely to make the model blendable again, and the
   counterfactual above sizes it: 0.847 → 0.970 on the top-45 backs.
2. **Re-measure and re-decide after it lands.** Not a re-admission by default: the same
   with/without rebuild, the same team totals, the same ADP bands.
3. **The D/ST arm needs its own answer**, separately. A Spearman of 0.168 against the
   field is not a level problem and a rushing identity will not touch it.
4. **A room-share disagreement flag** is cheap and independently useful — built on share
   of a team's projected carries rather than on level, so a level error cannot flag every
   back. On the 2026 board it surfaces Jonathon Brooks (0.144 against the field's 0.442),
   MarShawn Lloyd (0.150 / 0.413), Josh Jacobs (0.529 / 0.331) and Chuba Hubbard
   (0.606 / 0.424) — the last two being cases where TOMCAT is *higher*, having not priced
   a returning teammate.
5. **A vacated-volume feature** was measured while diagnosing this and is worth about what
   the model already gets from availability: on realised seasons 2021→2025, carries
   vacated by a room predict a healthy incumbent's carry change at **r = +0.327**
   (+0.205 unconditional). Targets read **+0.041** and are not worth it. It must be net
   of the incoming replacement — rooms losing 250+ carries show a median change of −7,
   because teams sign someone, which is exactly what Detroit did.

## Numbers in here, and which kind they are

Per this directory's README: the team totals, the realised per-game rates and the
vacated-volume correlations are **fitted-artifact numbers** off nflverse seasons and
should reproduce exactly. The ADP-band ratios, the with/without level table and the
backfield shares are **live-board numbers**, measured against the 2026 boards as built
**2026-09-07**, and will not reproduce once rosters move.
