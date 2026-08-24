# 33 — Role resolution: what the first three games say about the depth chart

**Priority:** Medium · **Effort:** S–M · **Status:** **Phases 1–2 built 2026-08-24** — G-R0, G-R1 and G-R3 pass, and G-R3 passing means these ship during the draft freeze. Phase 3 not started, and belongs scoped with [28](28-outcome-distributions.md), whose **G-D0 passed by 17.5×** on 2026-08-24
**Depends on:** [21](21-coaching-and-scheme.md) (the depth chart this measures) ·
[18](18-season-usage-model.md) (the arms that consume it)
**Feeds:** [28](28-outcome-distributions.md) (the floor/ceiling this is really for) ·
[32](32-movers.md) · [19](19-weekly-usage-model.md)

---

## Problem

**The board treats a pre-season depth chart entry as a fact, and it is a guess about
a third of the time.**

`depth_rank` is the one feature that has ever moved this model (+0.048 R² on veteran
carries, plan 21) and it enters both arms as a hard number: rank 1, 2 or 3. Nothing
anywhere records how often that number turns out to be right, or that it is far less
right for a rookie than for a settled starter — which is exactly backwards from where
a drafter needs it.

The question this plan answers is one a chart cannot answer about itself: **if a
player is starting in week 1, the depth chart should have had him at rank 1.** So
reconstruct the chart the season actually revealed, and score the pre-season one
against it.

## Evidence

### The derived chart

For each completed season, rank every player within `(team, position)` by per-game
opportunity over **weeks 1–3** — targets for WR/TE, carries for RB, attempts for QB —
and clip to the same 1/2/3 scale `MAX_DEPTH_RANK` uses. That is the depth chart as
the season played it.

**It is leak-free for a draft board, and that is the whole point.** The early-season
data is used only as a *training label from seasons that are already over*. Fitting
"pre-season signals → the role the season revealed" on 2018–2024 and applying it to
2026 needs nothing from 2026 except its pre-season chart. Requires 2+ games played, so
a week-1 injury does not read as a demotion.

### Calibration: the chart is least reliable exactly where it is most needed

`P(true rank | listed rank)`, 2018–2025:

| cohort | listed 1 → true 1 | listed 2 → true 2 | listed 3 → true 3 | n |
|---|---|---|---|---|
| **settled** | **59%** | 47% | 59% | 1,525 |
| **mover** | 45% | 43% | 57% | 408 |
| **rookie** | **36%** | 32% | 51% | 293 |

A rookie listed as his position's starter is the real starter **36%** of the time,
against 59% for a settled veteran.

> Reproduced by `python -m Scripts.usage.role`, which is the authority on these
> numbers. They sit a point or two above the first measurement because the shipped
> version drops a player short of :data:`MIN_DERIVED_GAMES` **before** ranking rather
> than after: a man who played one game should not occupy a rank that pushes a
> team-mate down one. The chart degrades precisely on the players whose
role a drafter cannot work out for himself — which is unsurprising once stated (a
rookie's line is the most speculative entry on any chart) and is not recorded anywhere
today.

### As a *feature* the payoff is small, and that is measured

A two-stage role model — predict the derived role from pre-season signals, feed the
prediction forward — against the best single signal available, Spearman on realised
early volume:

| pos | cohort | role model | `depth_rank` alone | best single signal |
|---|---|---|---|---|
| **QB** | **mover** | **0.450** | 0.236 | 0.386 |
| RB | mover | 0.744 | 0.242 | 0.722 |
| TE | mover | 0.714 | 0.462 | 0.702 |
| WR | mover | 0.706 | 0.393 | 0.713 |
| QB | settled | 0.513 | 0.201 | 0.506 |
| RB | settled | 0.830 | 0.529 | 0.830 |

**Quarterback movers is the only real gain (+0.064), and n = 39.** Everywhere else the
combination is a wash, because prior volume already dominates — plan 22's finding
again, and plan 32's.

**For rookies, draft capital already beats the chart.** Fitted on rookies alone (a
pooled fit tunes its coefficients for veterans and reads as a catastrophe here, which
is an artefact rather than a result):

| pos | rookie-only fit | `depth_rank` alone | **draft capital alone** | n |
|---|---|---|---|---|
| RB | 0.458 | 0.180 | **0.478** | 57 |
| WR | 0.582 | 0.416 | **0.600** | 102 |

The chart carries real information for rookies — 0.18 and 0.42 against nothing else in
existence — but adding it to draft capital does not improve on draft capital.
**That independently confirms the shipped `ROOKIE_REGRESSORS`** and is a reason not to
build a role feature, not a reason to build one.

### The finding worth building on: role uncertainty is a *variance* channel

Realised **season** per-game volume, conditioned on the listed rank:

| cohort | pos | listed | p10 | p50 | p90 | **p90/p50** | n |
|---|---|---|---|---|---|---|---|
| settled | QB | 1 | 26.86 | 32.88 | 38.12 | **1.16** | 152 |
| mover | QB | 1 | 26.91 | 32.06 | 35.35 | **1.10** | 28 |
| settled | RB | 1 | 2.33 | 12.53 | 17.76 | 1.42 | 220 |
| settled | WR | 1 | 2.00 | 6.00 | 9.33 | 1.56 | 453 |
| mover | RB | 2 | 2.53 | 6.11 | 11.43 | 1.87 | 42 |
| rookie | WR | 2 | 1.08 | 2.90 | 5.75 | **1.98** | 43 |
| rookie | RB | 2 | 1.79 | 6.44 | 13.12 | **2.04** | 35 |
| mover | TE | 2 | 0.62 | 1.42 | 3.18 | **2.24** | 30 |

**The board's floor/ceiling does not know any of this.** Measured on the 2026 GOP
board over 437 players above 20 points, the median `(ceiling − floor) / TRUE_Points`
is **9.0%**, and the ceiling sits at **1.042×** the projection. Role uncertainty
implies 1.10× to 2.24×.

The current column is **source disagreement** — how far apart ESPN, FantasyPros,
BetOnline and TOMCAT are — which plan 28 already names as the wrong quantity. This
measurement says something sharper than "it is the wrong quantity": it varies along
the wrong axis. Today's width is set by position (QB 7.4%, WR 9.7%, RB 12.1%, TE
16.3%) and **not at all by cohort**, when cohort is what actually decides whether a
projection is knowable. A settled QB1 is nearly certain (1.16) and a mover TE2 is a
coin-flip between two seasons (2.24), and the board shows both at about 9%.

## Fix

### Phase 1 — ship the derived chart and the calibration table — **built**

`Scripts/usage/role.py`: build the derived chart for every completed season, and the
`P(true | listed, cohort)` table from it. Half a day. It is a diagnostic before it is
anything else — the 35% is worth knowing on its own, and every later phase reads this
table.

### Phase 2 — carry the calibration onto the board — **built**

Surface it where `usg_evidence` already lives: a rookie listed WR2 is not a WR2, he is
a 32% WR2, and a drafter reading a projection should see which of those he is being
sold. No projection moves — this is a confidence column, and it is the cheapest thing
here that a human actually uses.

### Phase 3 — role uncertainty as the floor/ceiling

The one that matters. Replace source disagreement with a **role-conditional
distribution**: draw a role from `P(true | listed, cohort)`, take the volume
distribution for that role, and report the resulting quantiles. That is a real
forecast spread, it is bimodal where the role is genuinely in doubt, and it is exactly
the shape [plan 28](28-outcome-distributions.md) argues a mean tiebreaker cannot hold.

Plan 28 owns the outcome-distribution machinery and this owns one of its inputs; the
two should land together rather than build two Monte Carlos.

### Do not build

| | Why |
|---|---|
| A role feature for the volume heads | Measured: a wash at every position except QB movers, where n = 39. Prior volume already carries role for anyone who has one |
| A role feature for the rookie arm | Draft capital alone beats draft capital plus the chart (0.478 vs 0.458 at RB, 0.600 vs 0.582 at WR). The shipped `ROOKIE_REGRESSORS` is already right |
| Chasing a better pre-season depth-chart *source* | The chart is the weaker signal even where it works — `depth_rank` 0.20–0.53 against prior volume's 0.51–0.83. A better chart competes with prior usage, which is where plan 22's eleven experiments died |

## Gates, pre-committed

**G-R0 — the derived chart must agree with the pre-season one more often than chance.**
Three ranks, so chance is 33%. **Bar: settled rank-1 accuracy above 50%.** ✅ Measured
at **59%**; this is here so a bug that scrambles the join cannot be reported as a
finding.

**G-R1 — the calibration must separate the cohorts.** **Bar: rookie rank-1 accuracy at
least 15 points below settled.** ✅ Measured **36% against 59%**, a 23-point gap. If a
re-run does not reproduce it, phases 2 and 3 have nothing to carry.

**G-R2 — the spread must beat the incumbent at being a spread.** Score both the
role-conditional interval and the current source-disagreement interval by **empirical
coverage**: what fraction of players land inside their own p10–p90. **Bar: the
role-conditional interval within 5 points of nominal 80% coverage, and closer to it
than source disagreement is.** Coverage rather than width, because a wide interval is
trivially achievable and useless.

**G-R3 — no projection may move in phases 1 and 2.** They are diagnostics. **Bar:
`TRUE_Points` identical to the byte.** ✅ **Passed twice.** On a rebuilt GOP board,
2,504 players, **0 rows moved and max |change| is exactly 0.0**; and at the source, a
rebuilt `Usage_SeasonProjections.parquet` is identical on all **55** shared columns
with only `usg_role_cohort` and `usg_role_confidence` added. Phase 3 changes
`floor`/`ceiling` only.

**Consequence: phases 1 and 2 are safe to merge during the draft freeze**, unlike
plan 31 phase 1 and plan 32 phase 1. Nothing they touch is a projection.

**And a scope clause.** The feature evidence above is negative and the plan is being
written anyway, because the *calibration* and the *spread* are the deliverables and
they need no feature to land. If phase 3 fails G-R2, phases 1 and 2 still stand on
their own — a number a drafter can read beside a projection he already reads.

## Effort

S–M. Phase 1 is half a day and reproduces every figure above. Phase 2 is a day, mostly
board plumbing. Phase 3 is the real work and should be scoped with plan 28 rather than
separately.

Two limits on the evidence, both worth knowing before leaning on it. The mover and
rookie cells are **thin** — QB movers n = 39, mover TE2 n = 30 — so the calibration
table is firmer than the per-cell spreads. And "weeks 1–3 opportunity" is itself a
noisy label: it requires 2+ games, which handles a week-1 absence, but a starter who
plays hurt through three games still reads as a backup.

**Not before the 2026 drafts** for phase 3, which moves a board column. Phases 1 and 2
move nothing and could ship any time — see [`DRAFT_READINESS.md`](../DRAFT_READINESS.md).
