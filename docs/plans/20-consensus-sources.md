# 20 — Consensus sources — **retired, on evidence that turned out to be wrong**

**Status:** Retired 2026-08-24. Kept as a stub so existing links resolve, and
because *why* it was wrong is the useful part.
**Superseded by:** nothing. Its question is reopened, not answered — see
*What is now owed* below.

This plan asked whether to add more free projection feeds, and answered **no** on a
measured independence argument. That argument was measured on a sample that could not
have produced any other answer.

---

## What it concluded, and why that is dead

It rested on one table — pairwise residual correlation over 2025, non-imputed cells
only, from [plan 16 step 0](16-usage-data-layer.md#step-0--the-gates-measured-2026-08-06):

| Source | 1 − mean residual r with the rest |
|---|---|
| ESPN | +0.068 |
| **FantasyPros** | **+0.027** |
| Pinnacle | +0.035 |
| BetOnline | +0.043 |
| usage baseline | +0.090 |

From which: FantasyPros is the least independent thing in the blend at a third of
ESPN's marginal value, so a sixth expert aggregator is the worst available use of
effort.

**The restriction to non-imputed cells is what breaks it.** FantasyPros was real for
60 players — ten per position, everything it serves without an account — so "non-imputed
FantasyPros cells" meant *the top ten at each position and nobody else*. The measurement
was taken entirely inside the band where every source agrees, because everybody knows
who the best running back is.

Measured 2026-08-24 on the 2026 board, once the registration fence was lifted and
FantasyPros reached 592 players:

| ADP band | n | mean \|ESPN − FP\| |
|---|---|---|
| 1–50 | 67 | **6.0%** |
| 51–100 | 63 | **5.3%** |
| 101–150 | 65 | 8.6% |
| 151–250 | 372 | **31.6%** |

Disagreement outside the top 150 is **five to six times** what it is inside it. Plan 20
could only ever see the first two rows of that table. Its +0.027 is a real number about
a sample roughly five times more agreeable than the population it was generalised to.

Note the correlation stays ~0.99 across every band, which is why this hid: correlation
on a quantity spanning 200 to 1,400 yards is dominated by scale and stays high while the
per-player disagreement quintuples. Percentage disagreement is the metric that moves.

**This does not prove a sixth aggregator is worth adding.** It removes the evidence that
said it was not. The honest state is *unmeasured*, which is where this plan started
before it acquired a number it could not support.

## What was actually true, and where it went

Three things in the original survive and are worth not re-deriving:

- **Sleeper's projections endpoint does not return projections.**
  `GET /projections/nfl/2025/1` answered 200 with 743 rows whose `stats` payload held
  only `adp_dd_ppr`, and a null `player`. Probed 2026-08-06. Do not spec it as a
  projection source on the strength of blog posts.
- **Sleeper's own cross-provider ids are patchy** — Puka Nacua's record carried
  `fantasy_data_id` and `sportradar_id` but null `espn_id`, `gsis_id` and `yahoo_id`.
  Join through `Scripts/crosswalk.py`'s `sleeper_id`, never Sleeper's own fields.
- **Crosswalk coverage**, 2026-08-06, 12,470 rows: `pfr_id` 9,610 · `espn_id` 8,139 ·
  `gsis_id` 7,985 · `sleeper_id` 6,358 · `yahoo_id` 5,488 · `fantasypros_id` **4,784**.
  That last one is low enough to measure with `crosswalk.coverage()` before relying on it.

Its two recommended *uses* for Sleeper have both been largely overtaken:

| Proposed use | Now |
|---|---|
| A second injury feed | [Plan 27](27-injury-model.md) built the injury layer on ESPN's `site.api` feed plus nflverse, with a daily archive and an episode table. A third feed is a cross-check, not a gap |
| `depth_chart_order` | [Plan 21](21-coaching-and-scheme.md) pulls 2026 depth charts daily and they are the one feature that moved the season model |

## What is now owed

Not scheduled here, because this plan is retired and its question belongs to the plans
that own the blend:

1. **Re-run plan 16 step 0's independence matrix** on FantasyPros at 592 players rather
   than 60. The whole table is suspect, not just FantasyPros' row: every other source's
   "residual correlation with the rest" was computed against a *rest* that was mostly
   ESPN wearing a FantasyPros badge. → [16](16-usage-data-layer.md)
2. **Then** let [plan 03](03-projection-source-coverage.md)'s weight re-tune fit the
   weights, rather than the hand-tuned quarters standing on this plan's reasoning.

Both are **frozen until after the 2026 drafts** (7–8 September) — they move
`TRUE_Points`, and the [readiness doc](../DRAFT_READINESS.md) freezes projection maths
in draft week.

## The one rule here worth keeping

From the original ship criterion, and it survives everything above:

> If a feed does not reduce blended per-stat MAE, **record the measurement and drop the
> feed. Do not wire it in at a token weight.** Six extra names in `WEIGHTS` that each
> contribute nothing make the blend harder to reason about and give every future
> debugging session six more places to look.

That is how `KIK` is handled today — built, measured, left at 0.0 with the failing gate
written down ([29](29-kicker-model.md)) — and it is the right pattern.
