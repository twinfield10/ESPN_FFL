# 39 — Convert the basis, do not calibrate the source

**Status:** COMPLETE

**Priority:** Medium · **Effort:** S · **Where it stands:** **Written 2026-09-03 to
name a rule three shipped changes were each deriving separately.** It records no new
work: `to_full_slate` (plan 18), `market_to_full_slate` and the pooled rate fallback
were built on the same argument in three different docstrings, and plan 03 step 3c
already measured what happens when the rule is broken. The contribution is the
**diagnostic** — how to tell a basis mismatch from a disagreement — because the two look
identical in a ratio table and the correct response to them is opposite.
**Depends on:** [18](18-season-usage-model.md) · [03](03-projection-source-coverage.md) ·
[38](38-the-athletic.md) · **Feeds:** [19](19-weekly-usage-model.md) ·
[34](34-stat-first-audit.md)

---

## Problem

An equal-vote blend assumes its sources answer the same question. They do not, and the
failure is quiet: a source on a different basis produces numbers in the right units and
the right ballpark, so nothing raises, nothing is null, and the blend returns something
that is neither quantity.

The tempting fix is to measure how far the odd source sits from the others and multiply
it back. That is a calibration, and it is wrong for a reason that is easy to talk
yourself out of: **it closes the gap by construction.** A source fitted to agree with
the consensus agrees with the consensus, so the fit always looks like a success, and
what it has removed is the disagreement the blend exists to measure.

## Evidence

**Three sources have sat on the wrong basis, and each was found separately.**

* **TOMCAT** predicted an expected value — per-game production × ~13.6 expected games —
  while ESPN and FantasyPros project a healthy 17-game slate. Blended raw it dragged
  skill positions to **0.887–0.900** of their ESPN level while kickers and defences,
  which the model does not cover, sat at exactly **1.000**: about **11%** of
  cross-position distortion in a blend whose whole job is to be comparable across
  positions. Fixed by `Scripts.usage.project.to_full_slate`, which divides the games
  term back out.

* **Both sportsbooks** price a season-long prop, which settles on what a player
  accumulates, so the line must carry the games he misses. ESPN projects a median of
  **17.0** games and applies no availability discount at all. BetOnline sat at
  **0.871** of the ESPN/FantasyPros mean. Fixed by
  `Scripts.season_projections.market_to_full_slate`.

* **A veteran with no prior season** had no efficiency rate, so every stat built on one
  came out null beside a perfectly good volume estimate — 345 pass attempts and no
  passing yards. The missing quantity was a *rate on a different basis* (his own, which
  did not exist) where a positional one was available and already fitted for rookies.

**Plan 03 step 3c measured the alternative and it fails.** Direct recalibration
`E[y|x] = a + b·x` per (position, stat) loses **−14.4%** MAE and **−36.9%** at
quarterback. The fitted weight re-tune fails all four of its pre-registered clauses in
all six population × split cells. Calibrating a source against the sources you want it
to match is a measured dead end in this repo, not an untried idea.

## The rule

> When a source answers a different question, **convert its basis**. Anchor the
> conversion on something measured *outside* the comparison. Never fit the constant to
> the gap you are trying to close.

The sportsbook conversion is the worked example. The observed gap is 0.871; the constant
used is **0.895** — the realised share of a 17-game slate a fantasy starter plays,
measured over 2021–25 at 0.878 / 0.891 / 0.905 / 0.903 / 0.898. Anchoring externally
leaves the books about **2.7%** below consensus after conversion, and that residual is
the point: it is a real opinion, and fitting to 0.871 would have deleted it.

## The diagnostic, and why it is the useful half

A level ratio in a bias table does not tell you which of the two you are looking at.
Four checks separate them.

| Check | Basis mismatch | Genuine disagreement |
|---|---|---|
| Flat across a player's stats? | yes | no |
| Flat across players? | yes | no |
| Matches an external anchor? | yes | no |
| Survives aggregating to what the source models? | yes | **no** |

BetOnline passes all four: within-player standard deviation of the ratio across five
stats is **0.084**, across-player standard deviation is **0.064** — *smaller*, so it is
a base rate applied to everyone rather than a per-player judgement — and it matches the
realised 0.895.

**The Athletic is the counter-example, and it is why the fourth row exists.** It reads
**0.77** on `rushingAttempts` against the gated ESPN/FantasyPros mean, which looks
exactly like a source that is bearish on running backs. Summed to *team* rushing
budgets it sits at **0.970** — the two sources agree almost exactly on how often a team
runs. What they disagree about is who gets the ball: the lead back's share of his own
backfield is **0.675** for ESPN and **0.645** for The Athletic, so on backs ESPN gives
150+ carries it reads **0.923** and on backs ESPN gives under 60 it reads **1.495**. It
flattens backfields.

Correcting that with a multiplier would be precisely wrong. It would restore the
starters and leave the committee view — the actual opinion — uncorrected and now
double-counted.

**The measurement gate makes this worse, not better.** The 100-point gate exists so that
two sources eight and thirteen points apart on a third-string quarterback do not read as
47% disagreement about nobody. But it keeps the top of every room and drops the bench, so
**any source that redistributes *within* a room shows up as though it were low on the
whole stat.** Read a rushing row next to the team-budget check, never on its own.

## What this does not license

* It is not permission to rescale a source because it sits below the others. Three of the
  four checks failing means the source disagrees, and disagreement is what it is for.
* It does not apply to rates, only to counts on a different accounting period. A
  yards-per-carry that differs is an opinion about a player.
* The anchor has to exist independently. If the only available number is the gap itself,
  there is no conversion to make — record the difference and leave it.

## Owed

The four checks are prose here and in `market_to_full_slate`'s docstring, applied by
hand. They belong in `Scripts.lab.sources` beside the bias tables it already renders, so
a new source gets screened rather than argued about. Not built.
