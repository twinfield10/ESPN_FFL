# 36 — Sportsbook scrapes: one contract, and game lines as a first-class source

**Status:** TO DO

**Priority:** Medium · **Effort:** Large · **Where it stands:** Not started. Absorbs
[03](03-projection-source-coverage.md)'s step 5, which is closed there and owned here.

> **Two books feed the draft board and neither is refreshed.** Pinnacle's 2026 props
> and BetOnline's 2026 season props were both last written **2026-08-14**, thirteen
> days before this was scanned, while ESPN, FantasyPros and the usage model refresh
> at 06:00 daily. The cause is not neglect: `scrape_pinnacle.py` and `scrape_BOL.py`
> both **scrape at import time**, so neither can be invoked as a module and neither is
> in the nightly. That is one defect wearing two costumes, and it is the reason to do
> this as one plan rather than five fixes.

> **And the odds this repo actually reasons about are not scraped at all.** Every game
> line comes from nflverse's schedule file — `spread_line` and `total_line`, nothing
> else. For 2026 that is **52 of 272 games**. There is no moneyline, no team total, no
> alternate, and no line history anywhere in the repo, so `Scripts/vegas.py` derives
> each team's implied points by halving a total and adding half a spread. A book
> quotes team totals directly.

## Problem

Five things are wrong and they share one root cause: **there is no contract for what
"a book" is in this repo.** Each scraper is a script that happens to write a file, so
every book gets its own answer to authentication, retries, storage, staleness,
scheduling and market coverage — and two of them answer "at import".

1. **Two of four scrapers run on import.** No `__main__` guard, so importing the
   module performs a live scrape. `scrape_BOL.py`'s import-time statements *write
   files*, which is strictly worse than Pinnacle's.
2. **Consequently neither book is scheduled.** `run_daily_refresh.sh` runs six
   stages and no book among them. Nothing in the script says why, so the omission
   reads as a decision rather than a blocker.
3. **Only player props are ingested.** Game lines, totals, team totals, moneylines
   and alternates are absent, and the models that most want them — the kicker
   ([29](29-kicker-model.md)) and D/ST ([30](30-dst-model.md)) heads, which are
   *built on* implied team strength — read a thin nflverse column instead.
4. **Pinnacle has two mechanisms for one book.** The weekly path drives Selenium
   against a page that currently times out; the season path uses the guest JSON API
   and works. The working one is not used weekly.
5. **Books we can reach and don't.** 4Casters has a documented API and is already
   integrated elsewhere. BetOnline's game lines are untouched even though its props
   are ingested.

## Evidence

All measured 2026-08-27 on `main`.

### Import-time scrapes, and which files they touch

`ast.parse` over each scraper's module body, counting top-level calls:

| Scraper | `__main__` guard | Top-level calls | Worst of them |
|---|---|---|---|
| `Scripts/scrape_pinnacle.py` | **no** | 10 | `driver.get(...)`, `WebDriverWait(...).until(...)`, `reconcile_props(...)` at lines 645–666 |
| `Scripts/scrape_BOL.py` | **no** | 7 | `write_parquet(...BetOnline_AllProps_Raw.parquet)`, `archive_raw(...)`, `write_csv(...Clean.csv)`, `reconcile_BOL(...)` at lines 695–706 |
| `Scripts/scrape_FP.py` | yes | 0 | — |
| `Scripts/scrape_pinnacle_season.py` | yes | 0 | — |

Two corrections to [03](03-projection-source-coverage.md)'s step 5, which is where
this was first recorded. It named **only** `scrape_pinnacle.py`; `scrape_BOL.py` has
the same defect and a worse blast radius, because importing it overwrites the
archived parquet and CSV rather than merely spending a scrape. And it cited lines
538–559, which have since moved to 645–666 — a reminder that a line number in a plan
is a snapshot.

`scrape_pinnacle_season.py` is the pattern to copy, and it already exists. That is
worth stating plainly: this step is not a design problem, it is 900 lines of script
that never got the treatment its sibling got.

### Staleness on the live draft board

`Data/Projections/*/Season/2026/`, by modification time, against a first draft on
**2026-09-07**:

| Source | Last written | Refreshed nightly? |
|---|---|---|
| FantasyPros | **2026-08-27 06:00** | yes |
| Usage (TOMCAT) | **2026-08-27 06:00** | yes |
| Kicking | 2026-08-24 10:01 | on demand |
| DST | 2026-08-18 15:24 | on demand |
| **Pinnacle** | **2026-08-14 13:05** | **no** |
| **BetOnline** | **2026-08-14 13:05** | **no** |

Thirteen days covers most of training camp. The blend gives each book an **equal
vote** on every row it has a real line for ([03](03-projection-source-coverage.md)),
so a stale book is not a stale column — it is a stale opinion carrying a quarter to a
fifth of the projection for those players.

This is the exact failure mode `run_daily_refresh.sh`'s own header warns about: *"an
absent source reading as agreement: something upstream stops answering, the pipeline
carries on with stale or imputed data, and the output looks entirely normal."* It is
happening to the two sources the script does not run.

### The game lines this repo does not have

`Data/NFL/schedules.parquet`, from `R/GetNFL.R`:

| Season | Games | Carry `spread_line` / `total_line` | Weeks priced |
|---|---|---|---|
| 2025 (complete) | 285 | 285 | all |
| **2026 (pre-season)** | **272** | **52** | **1–4 only** |

Columns available: `spread_line`, `total_line`, `total`. That is the whole
inventory — **no moneyline, no team total, no alternate line, no price, no
timestamp, no history.**

Two consequences already visible in the code:

* `Scripts/vegas.py` computes `implied_own = total_line/2 + margin/2` and
  `implied_allowed = total_line/2 − margin/2`. That identity is exact only if the
  market's team totals are symmetric about the game total, which is an assumption a
  team-total market would replace with a quote.
* The same module documents having to shrink a season estimate built from three or
  four priced games, because that is all nflverse offers pre-season: *"a season model
  therefore cannot average seventeen lines; it averages three and shrinks."* A live
  book prices the full slate much earlier, and prices it repeatedly.

`vegas.py`'s own table is the argument for why this matters — its measured
correlations against realised outcomes are 0.844 for PAT attempts, 0.848 for team
offensive TDs and 0.816 for points allowed, against 0.399/0.360/0.277 for the prior
season. Implied team strength is the strongest single predictor in the repo for
everything special teams, and it is sourced from the thinnest feed in the repo.

### The precedent, inventoried

`~/GitRepos/Rebirtha/python/sportsbooks/` — a working multi-book pipeline, ~15k lines,
already solving most of this. What is directly reusable:

| Piece | What it gives us |
|---|---|
| `base.py` (109 lines) | `BaseSportsbook` ABC: `fetch_odds() -> Dict[bet_type, DataFrame]`, `transform_to_standard()`, plus implied-probability and team-name helpers |
| `sources/pinnacle.py` (557) | The guest API adapter, **including the geo-block workaround** |
| `sources/fourcasters.py` (629) | 4Casters exchange adapter, `https://api.4casters.io`, token auth |
| `sources/bol_season_props.py` (424) | BetOnline season props, Python rather than this repo's R |
| `pipeline.py` (801) | Parallel fetch, then Bronze/Silver/Gold |
| `odds_upload.py` (1166) | Date-partitioned writes **with change detection** — only rows whose price/value/implied probability moved are appended, which is line history as a by-product |
| `line_history.py`, `closing.py` | Movement and closing-line handling |
| `combine_odds.py` (736) | Best-line-per-market across books |

The standard schema it normalises to: `sportsbook`, `matchup`, `marketGrouping`,
`marketTitle` (Spread / Total / Moneyline), `betSide`, `points`, `price`, `impProb`,
`gamePeriod`, `officialDate`, `startTimeET`, `rotNum`, `isAlt`, `propType`. One row
per price, which covers props and game lines in the same shape — so *game lines are
not a new artifact type*, they are rows this schema already has a place for.

Pinnacle's adapter returns four views from one fetch: `Pinnacle` (main lines),
`Prop` (player props), `Pinnacle_Alts` (alternate spreads and totals), `All_Bets`.
Everything this plan wants from Pinnacle is already in that return value.

### The one hard blocker, named up front

Pinnacle **geo-blocks US IPs on its league routes**. From the adapter's own comments:
`/leagues/{id}` and `/leagues/{id}/matchups` return 403 with `reason: "location"`, and
rotating the IP cannot clear it because every address in the region is in the same
country. The workaround in production is to **pin league IDs in config** and use the
sport-level feed instead:

```
/sports/{sport_id}/matchups?brandId=0          # not geo-blocked
/sports/{sport_id}/markets/straight            # not geo-blocked
/matchups/{matchup_id}/markets/related/straight
```

`PINNACLE_LEAGUE_IDS` currently pins `{"mlb": [246]}`. **NFL's league ID is not
known** and discovering it is a prerequisite, not a detail — the discovery route is
the blocked one.

Two more inherited cautions worth carrying over verbatim, because both were learned
the expensive way in the other repo:

* `requests` defaults to **no timeout**, and a dead socket wedged a run for 30+
  minutes holding a shared lock. Use explicit `(connect, read)` timeouts.
* **4Casters is an exchange, not a book.** It keeps quoting after kickoff, so its
  "current" price can be a live in-game number, and its de-vigged probabilities swing
  far enough that the other repo excludes it from closing-line reference entirely
  (`CLV_REFERENCE_BOOKS = ["Pinnacle", "LowVig"]`). It also has vig *added* rather
  than removed (`_VIG = 0.0075`). Treat it as a distinct market type.

## Fix

Ordered so each step ships and is useful alone. Steps 1 and 2 are worth doing even if
the rest is never built.

**1. Guard the two scrapers, and put them in the nightly.** Absorbs
[03](03-projection-source-coverage.md) step 5. Move each module's top-level work into
`main()` behind `if __name__ == "__main__":`, following
`scrape_pinnacle_season.py`. Add a test asserting **no bare top-level calls in any
`Scripts/scrape_*.py`** — the AST check in the Evidence section above generalises to
four lines of pytest, and it is what stops this recurring. Then add the two to
`run_daily_refresh.sh` with the same `|| fail` treatment every other stage gets.

This is the step that fixes the staleness, and it is small. Do not let it wait for
the architecture.

**2. Adopt one adapter contract.** Port `BaseSportsbook` into
`Scripts/books/base.py`: `fetch_odds()` returning a dict of frames,
`transform_to_standard()` returning the schema above, shared implied-probability and
team-name helpers. Then make the existing scrapers implement it rather than rewriting
them — the goal is one shape, not one file.

Note what this makes possible that nothing currently can: `Scripts/market.py` already
holds this repo's *single* derivation of de-vig and threshold-to-expectation
([35](35-market-lines-and-vig.md)). A shared adapter means a new book gets that
treatment by construction instead of inventing its own, which is precisely the defect
plan 35 was created to fix — three different juice coefficients in three files.

**3. Pinnacle weekly over the guest API; retire the Selenium path.** Discover and pin
the NFL league ID, add the sport-level fallback, port the geo-block classification so
a location 403 does not look like a transport failure. Success criterion: the weekly
Pinnacle props that `clean_pinny()` consumes arrive from JSON, and
`scrape_pinnacle.py`'s Selenium code is **deleted rather than left dormant**. One book,
one mechanism.

**4. Game lines as a first-class artifact.** The new capability, and the reason the
rest is worth doing. Ingest spread, total, **team totals**, moneyline and alternates
per game, with prices and a timestamp, from every book that quotes them. Persist
date-partitioned with change detection, so line *movement* accrues for free.

Then rewire `Scripts/vegas.py` to prefer a quoted team total over
`total_line/2 ± margin/2`, and to read the full priced slate rather than nflverse's 52
games — keeping nflverse as the historical backstop, since it is the only source for
completed seasons and its `assert_sign_convention` guard must survive the change
unchanged.

**5. Add 4Casters, and BetOnline's game lines.** Both are adapters once step 2 exists.
Carry the exchange caveat into the schema rather than into a comment: an exchange
price is a different quantity from a book price, and the code should be able to say so.

**6. Schedule at six hours.** Four runs a day, locally, not on EC2 — the other repo's
EIP-rotation machinery is an EC2 answer to an IP-block problem and should not be
ported. Reuse this repo's own conventions: `run_daily_refresh.sh`'s branch guard and
`|| fail` discipline, and `Scripts/refresh_status.py` so a silently dead book is
visible rather than absorbed.

**7. Decide what plan 02 owns.** [02](02-betonline-access.md) is still IN PROGRESS on
one question — whether BetOnline's weekly props API (403
`invalid_security_headers`) is worth another attempt. That question is a member of
this plan's set, and keeping it in 02 splits book-scraping across two plans. **Fold it
in when this is picked up**, or close 02 explicitly as "season-only, permanently".
Flagged rather than done, because it is a scope call rather than a finding.

## Risks

- **The geo-block may not have a local answer.** The pinned-league workaround is
  documented as working from EC2. Whether a US residential IP can reach even the
  sport-level feed is **unverified** and is the first thing to test — before any
  design work, because a negative answer reshapes steps 3 and 4. Cheapest possible
  probe: one `curl` against `/sports`.
- **Terms of service and rate limits.** Four runs a day across three or four books is
  modest, and the existing scrapers already poll. Still: use explicit timeouts, back
  off, cache, and do not parallelise beyond what the other repo already runs.
- **Schema drift is silent.** A book renaming a market makes rows vanish, not error.
  The change-detection layer makes this worse, not better, because "no change" and "no
  data" look identical in an append-only store. Assert expected market coverage per
  run.
- **Scope.** This is the largest plan in the set by surface area, and only step 1
  is draft-relevant. The staleness fix should not be held hostage to the
  architecture.
- **Do not port the EC2 machinery.** EIP rotation, shared cron locks and the
  seed-data fallbacks are answers to problems this repo does not have. Port the
  adapter contract and the storage layer; leave the infrastructure.

## Verification

- No `Scripts/scrape_*.py` module performs work on import — asserted by test over
  every scraper, not just the two known offenders.
- Both books appear in `run_daily_refresh.sh` with `|| fail`, and their 2026 archive
  timestamps track the nightly rather than a two-week-old manual run.
- `python -c "import Scripts.scrape_BOL"` writes no files and launches no browser.
- Pinnacle weekly props arrive without Selenium, and the Selenium code is gone.
- A quoted team total is preferred over the halved-total derivation where one exists,
  with `vegas.py`'s sign-convention assertion still passing.
- Line movement is queryable for at least one market across at least two books.
- A book returning zero rows fails the run loudly rather than renormalising away.

## Prior art in this repo

- [02](02-betonline-access.md) — the BetOnline weekly 403, and the season-props
  fallback that is now the draft source.
- [03](03-projection-source-coverage.md) — step 5 originated here; also the
  provenance and equal-vote machinery that makes a stale book consequential.
- [13](13-dst-from-vegas-lines.md), [29](29-kicker-model.md),
  [30](30-dst-model.md) — the consumers of implied team strength, and the reason
  game lines are worth scraping properly.
- [35](35-market-lines-and-vig.md) — the one place de-vig and
  threshold-to-expectation are derived. Every new book must route through it.
