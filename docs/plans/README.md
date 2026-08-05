# Upgrade plans — 2026 season

Small, self-contained plans from the pre-season scan on 2026-08-01. Each one is
Problem / Evidence / Fix / Effort, so it can be picked up independently.

Phase 0 (2026 rollover, pipeline de-duplication, season-scoped paths, docs,
tests) is already done — see [`../STATE_OF_THE_REPO.md`](../STATE_OF_THE_REPO.md).
These are what the scan turned up *beyond* that.

| # | Plan | Priority | Effort | Why now |
|---|---|---|---|---|
| 01 | [Scoring coverage gaps](01-scoring-coverage.md) | ~~High~~ **Done** | S | ~~Two GOP scoring rules are silently ignored, and nothing detects it~~ |
| 02 | [BetOnline access](02-betonline-access.md) | ~~High~~ **Done** | M | ~~One of four projection sources is dead~~ Weekly dead, season props wired up with IDP |
| 03 | [Projection source coverage](03-projection-source-coverage.md) | **Partly done** | M | Renormalisation + provenance landed; weight re-tune deferred, Pinnacle import-time scrape open |
| 04 | [Matchup-period handling](04-matchup-periods.md) | Medium | S | Winfield_Football silently loses a week of data |
| 05 | [Dependency upgrades](05-dependency-upgrades.md) | Medium | M | `espn-api` has a traded-player fix we want before drafting |
| 06 | [Performance](06-performance.md) | Low | S | Backfills are slow enough to discourage re-running them |
| 10 | [Scoring registry](10-scoring-registry.md) | ~~High~~ **Done** | S | ~~Scoring is re-derived from a mutable live object 4× per league, and never recorded~~ |
| 11 | [Per-slot scoring](11-per-slot-scoring.md) | ~~High~~ **Done** | M | ~~`espn_api` collapses per-slot scoring to one value~~ Registry now has a `slot` dimension; GOP's D/ST was inflated ~16% |
| 12 | [Season projections](12-season-projections.md) | ~~High~~ **Done** | M | ~~Draft board has no book data~~ Phase 2 done: season props blended and scored per league |
| 13 | [D/ST from Vegas lines](13-dst-from-vegas-lines.md) | Medium → High in-season | M | D/ST is the only position with zero market coverage; game lines imply points allowed |

### Local frontend

Replaces the notebook-plus-Google-Sheets workflow. Split into three because the
foundation blocks the other two and they have different dependencies.

| # | Plan | Priority | Effort | Depends on |
|---|---|---|---|---|
| 07 | [Frontend foundation & data store](07-frontend-foundation.md) | **High** | M | — |
| 08 | [Week-to-week views](08-frontend-weekly-views.md) | High | M | 07 |
| 09 | [Draft views](09-frontend-draft-views.md) | High (seasonal) | L | 07 + draft roadmap Phases 1-3 |

Plan 07 is the one to read first — a measured 2,700× gap between recomputing
(22.9s/league) and reading cached parquet (0.01s) forces a local data store, and
that store is a prerequisite for both view plans. It also fixes an existing gap:
ESPN data is currently never persisted anywhere.

## Suggested order

**Before the draft:** ~~01~~ → ~~10~~ → ~~11~~ → 05 (espn-api only) → ~~02~~,
then 07 → 09. The first few affect the numbers the draft board is built on, so
they come first.

Note on 11, now done: it turned out to matter beyond GOP. The per-slot fix
also stopped offensive players in *every* league being credited for imputed
kick-return yards and points-allowed tiers at D/ST rates.

Note for 05: plan 01 worked around an `espn_api` 0.45.1 bug that shares scoring
dicts across `League` objects. Check whether the upgrade fixes it upstream; if it
does, `fetch_utils.isolate_scoring_format()` becomes redundant, though its live
test is worth keeping either way.

**Before week 1:** 03 → 04 → 08 → 13. Plan 13 needs posted game lines, so it
cannot be finished in August — 51 of 272 games had lines on 2026-08-03. Its
distributional-scoring piece (`E[f(X)]` over tiers) can be built and tested
against 2025 now, independent of 2026 lines.

**Whenever:** 06, and the rest of 05.

~~Independent of all of these, run `Rscript R/GetNFL.R 2026`~~ — **done
2026-08-03.** `current_season()` is 2026 and `current_week()` is 1. Two bugs in
`GetNFL.R` surfaced doing it and are fixed: the schedule was filtered on
`!is.na(total_line)`, which kept only 51 of 272 games pre-season (betting totals
are posted a few weeks ahead), and the player-stats step aborted the whole script
for a season with no play-by-play yet, before the tackle ratios were written.

Still outstanding from that: `Data/NFL/2026/NFL_Stats.csv` and refreshed
`NFL_Tackles_By_Position.csv` need a re-run of `Rscript R/GetNFL.R 2026` once week
1 has been played.

Separately, `Scripts/scrape_pinnacle.py` runs a live Selenium scrape at **module
import time** (lines 538-559, no `if __name__ == "__main__"` guard), so importing
it launches Chrome, and it currently times out on Pinnacle's page. Folded into
plan 03.
