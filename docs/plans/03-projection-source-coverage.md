# 03 — Blend weights assume coverage the sources don't have

**Status:** IN PROGRESS

**Priority:** High · **Effort:** Medium · **Where it stands:** Steps 1, 2, 4 done · step 3 deferred · step 5 open

> **The problem was bigger than this plan estimated.** Measured provenance shows
> FantasyPros was only **25% real** even in 2025, Pinnacle **8%**, BetOnline
> **12%** — so the nominal four-source blend averaged roughly **90% ESPN**.
> Renormalisation is implemented and proven confined to imputed cells. See
> [Measured coverage](#measured-coverage-2026-08-03).

## Problem

The blend treats four sources as peers, weighting each per stat (Pinnacle and
BetOnline get 25% each on most stats, 40% each on passing TDs). But the sources
cover very different numbers of players, and gaps are filled by
`impute_columns(target_prefix='PINNY_', source_prefix='MEAN_')` — where `MEAN_`
is the average of ESPN and FantasyPros.

So when Pinnacle has no line for a player, `PINNY_*` silently becomes the ESPN/FP
mean, and the blend then weights that as though it were an independent
sportsbook opinion. **ESPN and FantasyPros get counted twice.** The apparent
four-source consensus is, for a large share of players, a two-source blend with
extra confidence.

This is invisible downstream: `PINNY_Points` looks populated and reasonable.

## Evidence

Season 2025, week 17:

| Source | Distinct players, wk 17 | All weeks | Stat columns |
|---|---|---|---|
| FantasyPros | 575 | 706 | offense |
| BetOnline | 598 | 1051 | offense + **defensive/IDP** |
| **Pinnacle** | **213** | 385 | offense only (11 stats) |

Against 338 rostered + FA players in Knights_FFL that week, `PINNY_Points` was
non-zero for 265 — but Pinnacle only had lines for 213 players league-wide, and
not all of those are rostered. A large fraction of that 265 is imputed.

Pinnacle also has **no defensive stats at all**, so for IDP leagues its 25%
weight is entirely ESPN/FP passthrough.

For the record, an earlier note in `STATE_OF_THE_REPO.md` suggested Pinnacle
might not be contributing at all because `clean_pinny()` is ~27% commented out.
That was too strong — measured, Pinnacle *is* contributing real lines for the
players it covers. The problem is coverage and imputation, not absence.

## Fix

**1. Track provenance.** Add a per-source `*_is_imputed` boolean during
`impute_columns`, so it's possible to tell a real line from a filled one. Cheap,
and everything below depends on it.

**2. Re-weight per row, not globally.** Renormalise weights over the sources
that actually have data for that player-stat, instead of letting an imputed
value absorb a full 25%. Roughly:

```python
# in compute_weighted_stats: skip imputed contributions, renormalise the rest
active = {src: w for src, w in weights.items() if not row[f"{src}_{stat}_is_imputed"]}
total = sum(active.values()) or 1.0
value = sum(row[f"{src}_{stat}"] * w / total for src, w in active.items())
```

This is the substantive change: a player with a real Pinnacle line gets a
genuine four-source blend; a player without one gets an honest ESPN/FP blend
rather than a fake consensus.

**3. Re-tune the weights against 2025 actuals.** They are currently hand-set. The
notebook already contains two unproductionised learned-weight models (cells 11
and 14 — OLS per stat, and a `LinearRegression` combo). With a full season of
actuals plus provenance flags, fit them properly and replace the guesses. Do
this *after* the BetOnline decision in plan 02, since the source set may change.

**4. Report coverage each run.** `get_match_details()` already prints unmatched
players. Extend it to print per-source coverage as a percentage, so a source
quietly degrading is visible rather than absorbed by imputation.

**5. Fix `scrape_pinnacle.py`'s import-time scrape.** Found 2026-08-03 while
verifying the season rollover. Lines 538-559 are module-level statements —
`driver.get()`, `WebDriverWait(...).until()`, `reconcile_props()` — with no
`if __name__ == "__main__":` guard. So merely *importing* the module launches
Chrome and scrapes Pinnacle, which means no tool or test can touch it without
triggering a live scrape, and a scrape failure surfaces as an `ImportError`.

It currently times out on `div[class*="matchupMetadata"]`, so Pinnacle is
effectively a second dead source alongside BetOnline (plan 02) and needs the same
kind of decision. Wrap the driver work in a `main()` behind a `__main__` guard
before diagnosing whether the selector or the pre-season page is the cause —
otherwise the two failure modes are indistinguishable.

Note `SEASON = current_season()` at line 18 is also import-time, so it is bound
before any caller can override it. Correct now that the schedule file says 2026,
but it makes the module unusable for backfilling another season.

## Measured coverage (2026-08-03)

`coverage_report()` on Knights_FFL 2025, share of cells that are **real** rather
than imputed, with the nominal weight beside it:

```
  stat                            ESPN          FP       PINNY         BOL
  ------------------------------------------------------------------------
  passingYards              100.0% w0.1   25.2% w0.7    8.7% w0.1    9.3% w0.1
  passingTouchdowns         100.0% w0.1   25.2% w0.1    8.7% w0.4    9.3% w0.4
  rushingYards              100.0% w0.2   25.2% w0.3   25.4% w0.25   28.4% w0.25
  receivingYards            100.0% w0.2   25.2% w0.3   43.8% w0.25   50.2% w0.25
  receivingReceptions       100.0% w0.2   25.2% w0.3   42.7% w0.25   48.9% w0.25
  ------------------------------------------------------------------------
  ALL STATS (mean)              100.0%        9.6%        4.1%        8.1%
```

Read the worst line: `passingTouchdowns` gave Pinnacle and BetOnline 0.4 each — 80%
of the weight — while both were real under 10% of the time. Since they impute from
`MEAN_` = avg(ESPN, FP), and FP is itself ESPN for 75% of cells, that 80% was
recycled ESPN. **FantasyPros at 25% is not a 2026 regression** — it was already
mostly imputed in 2025, which this plan did not catch by counting distinct players
in the source file rather than matched cells in the blend.

### 2026 pre-season source state

| Source | Weekly | Season-long | Coverage | IDP |
|---|---|---|---|---|
| ESPN | works | works | all rosters + FA | yes |
| FantasyPros | 10/position | 10/position | 60 players | no |
| BetOnline | 403, dead | **works (R only)** | 546 props / 273 players | **yes** |
| Pinnacle | game lines only | **works (guest API)** | 76 props / 76 players | no |

FantasyPros is now gated: the page is 289 KB but `table#data` holds exactly 10
rows, there are 10 `.player-name` links, and the HTML advertises
`upgrade` / `premium` / `sign in`. Same cap on `week=draft`, so it is not
pre-season sparsity. Kept in the blend — renormalisation means it contributes for
the 60 players it genuinely covers and nothing elsewhere.

## What landed

**Steps 1, 2 and 4 are done.** `impute_columns` writes
`<SOURCE>_<stat>_is_imputed` flags that accumulate across calls;
`compute_weighted_stats(renormalise=True)` drops imputed sources and renormalises
the remainder, with `renormalise=False` retained to A/B against history;
`print_coverage_report` runs inside `clean_lineups`. Weights moved to a module-level
`WEIGHTS` constant so they can be inspected without running the pipeline.

A third bug surfaced doing it: a source whose **column was absent entirely** was
skipped from the sum but not the divisor, so a 10-yard projection came out as 5
purely because Pinnacle was not in the frame. A pre-existing test asserted that
behaviour; it now asserts the corrected one.

**Verified with the new equivalence harness** (`Scripts/equivalence.py` — five
plans referenced one that did not exist). Across Knights_FFL, GOP_Degenerates and
Winfield_Football for 2025: only `TRUE_*` columns changed, and for every
(row, stat) where all weighted sources were real the drift was **0**, bar
`passingYards` at 5.7e-14 of float re-association noise. `TRUE_Points` moved for
~73% of rows, max 11.3 points.

**Step 3 (re-tune the weights) is deliberately deferred.** This plan says to do it
after the plan 02 decision "since the source set may change" — and it just did:
BetOnline weekly is gone, FantasyPros is at 10/position, two season-props sources
are new. Fitting now would fit to a source mix that no longer exists. It also needs
a real backtest, not a one-off regression.

**Step 5 (Pinnacle's import-time scrape) is still open.** `scrape_pinnacle.py`
lines 538-559 remain module-level. `Scripts/scrape_pinnacle_season.py` is the
pattern to follow: everything behind `main()`, guarded by `__main__`, with a test
asserting no bare module-level calls.

## Verification

- Coverage report prints per-source real percentages inside every
  `clean_lineups` run.
- All-real rows are bit-identical before and after renormalisation (0 violations,
  three leagues, 45 stats).
- For a player a book does not cover, `TRUE_` equals the renormalised blend of the
  sources that do cover them.
- Still outstanding: backtest `TRUE_Points` correlation against 2025 actuals, as
  part of step 3.
