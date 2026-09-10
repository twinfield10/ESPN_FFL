# 47 — The Athletic as the fifth weekly source

**Status:** COMPLETE

**Where it stands:** Shipped 2026-09-09 at **0.25**, a full equal weekly vote, live on
every league's weekly board from the first `--what lineups` run after import.

**What it is:** The Athletic began publishing a weekly slate alongside the season
workbook [plan 38](38-the-athletic.md) already ingests. It arrives as a second
hand-downloaded `.xlsx` — one sheet, four side-by-side position blocks, 232 offensive
players, nine raw stats. Parsed by `Scripts/load_athletic.py --what weekly`, written to
`Data/Projections/TheAthletic/Season/<season>/TheAthletic_Projections_Week_All.parquet`,
and blended as `ATH_` by `clean_lineups` beside ESPN, FantasyPros, Pinnacle and
BetOnline.

---

## The thing this unblocks

[Plan 38](38-the-athletic.md) shipped The Athletic at an equal season vote **before** the
per-stat MAE measurement [plan 20](20-consensus-sources.md) pre-registered, and recorded
that measurement as *owed rather than skipped*. It then explained why it could not be
paid:

> `Scripts.usage.evalset.SOURCES` drives `Scripts.lab.accuracy`, `Scripts.usage.gates`
> and `Scripts.usage.g1_season`, all of which score **player-week** rows out of
> `lineups.parquet`. `ATH` is season-only and has no weekly line, so it has no column
> there — running `python -m Scripts.lab.accuracy` in January and reading a clean table
> would look like The Athletic had been judged when it had not. The measurement has to
> come from the **season** artifact … That harness does not exist and is the concrete
> piece of work owed.

It has a weekly line now. `"ATH"` joined `evalset.SOURCES` in this change, so the
measurement is runnable on the harness that already exists rather than the one plan 38
owes. **That does not retire the season harness** — a weekly MAE answers whether the
source is fit to blend *weekly*, and the draft-board question is still open. But the
cheaper half is now a command rather than a project.

Per plan 38's own terms and `docs/DRAFT_READINESS.md`'s gate: score it on players
projected over **100 points**, and report the ungated number beside it rather than
instead of it.

## What was measured before shipping

**The parse, against the 2026 week-1 download.** 232 players — QB 32 / RB 75 / WR 95 /
TE 30 — and **232 of 232 names resolve through the crosswalk**, which is the best
first-import rate of any source here. Two players ESPN rosters are absent from the
workbook (Kenyon Sadiq, Travis Hunter); they abstain.

**The blend is additive where the source is silent, and that was checked rather than
assumed.** Knights_FFL week 1, the same ESPN pull blended twice with only
`clean_ath_weekly` swapped:

| | rows | `TRUE_Points` moved | max \|Δ\| |
|---|---|---|---|
| The Athletic is real | 219 | 219 | 0.873 |
| The Athletic is silent | 114 | **0** | **0.000000** |

Byte-identical on every row it does not speak for, which is what the provenance flags
exist to guarantee: kickers, defences and the deep bench are untouched. The A/B was run
in both orderings and gave the same answer.

**Realised coverage on that league** — the number to watch, since the nominal 0.25 is
almost never the realised weight:

| stat | ESPN | FP | PINNY | BOL | **ATH** |
|---|---|---|---|---|---|
| rushingYards | 100% | 97% | 22.5% | 20.7% | **57.1%** |
| receivingYards | 100% | 97% | 44.4% | 44.1% | **56.2%** |
| receivingReceptions | 100% | 97% | 44.1% | 42.6% | **56.2%** |
| passingYards | 100% | 97% | 9.6% | 9.6% | **9.6%** |

Second only to FantasyPros on the volume stats, and roughly 2.5× either book. On passing
it is exactly a book's coverage, because 32 quarterbacks is 32 quarterbacks.

## Three decisions, and the one that is a departure

**1. One module, `--what season|weekly`.** Following `Scripts/scrape_FP.py`. The two
workbooks share a provider, a prefix and `POSITION_STATS` and almost nothing else, but
they share the rules that matter — points are never read, the mask is always applied —
and those are worth stating once. `--what` defaults to `season`, so every existing
invocation, including the fix hint `refresh_status` prints, still works.

**2. The newest download wins for the week it names.** This is a **departure** from
`scrape_FP.scrape_weekly`, which merges `keep="first"` so a re-scrape cannot rewrite the
pre-game opinion the blend actually voted with — the same rule `Scripts/freeze.py`
applies to the board ([plan 41](41-projection-freeze.md)).

The argument for departing: this file is downloaded by hand, and an updated copy on
Sunday morning is the normal case rather than an accident. The owner asked for the fresh
numbers. **The cost is real and is not being smoothed over** — a re-import after kickoff
does rewrite what the blend voted with, and stored history then disagrees with the board
that was published. Three things hold the blast radius down:

- Only the imported week's rows are replaced. Every other week is untouched, and there
  is a test for it.
- The write **prints how many rows it replaced**, so a rewrite is in the log rather than
  silent.
- Every download is kept verbatim under `Landing/<season>/` under its own datestamp, so
  the superseded numbers are recoverable.

If this turns out to matter, the fix is `keep="first"` plus an explicit `--replace`, and
it is a one-line change to `build_weekly`.

**3. Live at an equal vote on the first run, not dark at 0.0.** `WEIGHTS['default']`
already carried `ATH: 0.25` from the season registration and **that dict is shared by
both grains**, so adding `ATH` to `WEEKLY_PREFIXES` was the whole switch — shipping dark
would have required a second, weekly-only weights dict. Registered live rather than
built and measured first, on the same reasoning as plan 38 and with the same honesty
about it: it moves `TRUE_Points` for 219 of 333 rows on day one, and the measurement is
owed after the fact.

**The shared dict is worth writing down as a constraint, not a convenience.** There is
no way to weight this source differently by grain without splitting `WEIGHTS`, so a
re-tune of the weekly blend is a re-tune of the draft board. That is fine today (both
are one equal vote) and is the thing to notice first if either ever needs to move.

## Three things this plan learned from the file rather than assumed

1. **The blocks do not share a column set.** The receiver block has no `Rush Att` and the
   tight-end block has no rushing columns at all. Blocks are therefore bounded
   *banner-to-banner* and their headers read through `_header_map`, never by position —
   for the season tabs that function is merely free, and here it is load-bearing.
2. **The two Athletic workbooks disagree with each other about team abbreviations.**
   `load_athletic.TEAM_TABS` asserts the provider is "already on ESPN's abbreviations …
   so no alias map is needed here". True of the season book. The weekly sheet is a
   *third* set: `JAC` (neither ESPN's `JAX` nor nflverse's) and `WAS` (nflverse's,
   against ESPN's `WSH`), while `LAR` follows ESPN against nflverse's `LA`. Hence
   `WEEKLY_TEAM_ALIASES`. Caught by the bye-week check, which reported Jacksonville
   absent from a week it played.
3. **`FPS` is derived, not an opinion.** Joe Burrow week 1: 19.3 published against 19.66
   recomputed from the nine columns beside it. It was never going to be read — points are
   what a league's rules do to a stat line — but it is worth knowing that reading it
   would have shipped somebody else's rounding rather than a second view.

## What is not in this change

- **`MEAN_` stays ESPN/FantasyPros on the weekly path.** The season path added `ATH` to
  `MEAN_SOURCES` for exactly one reason — FantasyPros publishes no `receivingTargets`, so
  `MEAN_receivingTargets` was `mean(ESPN, ESPN)` wearing a consensus badge
  ([plan 39](39-source-basis.md)). **This workbook publishes no targets either**, so the
  reason does not carry over and `create_mean_cols` stays a two-source function. The
  weekly thinnest-covered stat is still ESPN alone; that is unchanged, not newly broken.
- **No nightly stage.** There is nothing to call. `run_daily_refresh.sh` is untouched and
  the import is a documented weekly hand step in `docs/SEASON_ROLLOVER.md`, watched by
  `refresh_status` on an eight-day clock.
- **No K or D/ST.** The workbook has neither, so those rows are untouched — and with
  TOMCAT withdrawn ([plan 43](43-tomcat-out-of-season-blend.md)) they still have no
  second opinion. This change does not help them.

## Adjacent, and deliberately left alone

- **`Scripts/lab/blend.py:88` and `Scripts/lab/report.py:728`** both hold
  `("ESPN","FP","PINNY","BOL")` and have been missing `ATH` on the *season* side since
  2026-09-01. Not touched here — it is plan 38's debt, not this one's, and fixing it
  quietly inside a different change is how a stale list becomes two stale lists.
- **The season entry in `WEEKLY_PROJECTION_SOURCES`' sibling, `PROJECTION_SOURCES`, is
  red and will stay red.** The season workbook is also hand-dropped and is now three days
  old against a 25-hour window; it will read STALE every day for the rest of the season.
  The five-element form added here (`max_age_hours` per source) is exactly the mechanism
  that would fix it, but how the season book should be watched now the draft is over is
  an owner decision rather than a mechanical one.
