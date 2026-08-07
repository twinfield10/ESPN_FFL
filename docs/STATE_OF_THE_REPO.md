# State of the Repo

**Last updated:** 2026-08-07, preparing for the 2026 season. Plans 07 (local data
store + app), 14 (Sheets reads the store) and 15 (draft boards) landed on the 5th;
plan 16's data layer and its go/no-go gates on the 6th; plan 09's draft board page
on the 7th.

A standing assessment of what works, what is broken, and what to do next. Update
it as things change — particularly the *Known issues* table, which is the part
worth keeping honest.

Actionable items have small plans in **[`plans/`](plans/)**, each with evidence
and a proposed fix.

---

## Executive summary

The weekly in-season pipeline is mature and ran all of 2025. It has strong,
genuinely reusable bones: a league-aware scoring engine, a four-source
projection blend, a Monte Carlo season simulator, a polished Sheets renderer,
and now a local data store with a Streamlit app reading it.

Two things were true at the start of this cycle, and one of them is now fixed:

1. ~~**Nothing was set up for 2026.**~~ Addressed — see *2026 readiness* below.
2. ~~**There is no draft capability.**~~ Largely addressed. The only draft file was
   dead code copied from another project; it is deleted, `Scripts/draft/`
   builds a league-aware board (roadmap Phase 3,
   [plan 15](plans/15-draft-board.md)), and **the draft board page is now built on
   top of it** ([plan 09](plans/09-frontend-draft-views.md)). There is a usable
   draft-day artifact today. What remains is the **live draft page** and **draft
   history** (Phase 1's backfill).

One new problem surfaced during the rollover: **BetOnline's weekly props API now
blocks the scraper.** That removes one of four projection sources. Details
below — it needs a decision.

---

## 2026 readiness — done

All nine leagues were verified live against ESPN for 2026: credentials valid,
teams rolled over, roster settings populated, drafts empty as expected.

| Team counts | Notable slots |
|---|---|
| 6, 10, 12, 12, 12, 12, 12, 14, 16 | `OP` superflex (Weenieless Wanderers), `DP` IDP (GOP Degenerates), no D/ST (12 Dudes one Cup) |

Fixed this cycle:

- **Deduplicated the projection pipeline.** 12 functions existed as two copies —
  one in `populateGoogleSheet.py`, one pasted into the notebook — and 8 had
  drifted apart, so the notebook you used to *decide* a lineup and the script
  that *published* it computed different numbers. Both now import
  `Scripts/projection_utils.py`. Verified behaviour-preserving: old and new
  produce cell-identical frames for a standard league and the IDP league
  (3,602 × 350 and 4,508 × 349, plus all downstream tables).
- **Season-scoped every data path.** Output paths carried no season component,
  and `reconcile_BOL`/`reconcile_props` deduped on
  `['week','player_name','position','team']` with no season key — so 2026 week 1
  would have merged into 2025 week 1 and picked winners nondeterministically.
  Paths are now `Data/Projections/<source>/{Season,Landing}/<season>/`, and
  existing 2025 data was migrated into place.
- **`fetch_league` fails loudly.** It wrapped everything in a bare `except:`
  that fell back to a 2024 league and then returned a possibly-unbound local,
  raising `UnboundLocalError` and hiding the real cause. Now propagates.
- **Killed the year mismatch.** `populateGoogleSheet.py` fetched league metadata
  with the configured year but player stats with a hardcoded `year=2025`, five
  lines apart — so the two could silently disagree.
- **Removed the weekly manual step.** `id_var` in `scrape_BOL.py` was a
  BetOnline game-ID seed hand-edited before nearly every weekly run
  (`259322 → … → 259563` across 2025), documented nowhere. Replaced with
  auto-discovery plus a `BOL_FIRST_GAME_ID` override.
- **Unified imports.** Three conventions coexisted (`from Scripts.x`, `from .x`,
  bare `from nfl_utils`), and the scrapers required a working directory that was
  mutually incompatible with the one their data paths needed. Everything is
  `Scripts.*` and runs from the repo root.
- **`nfl_utils` no longer does I/O at import,** and `current_week()` returns the
  final week once a season completes instead of a null that crashed
  `range(1, WEEK + 1)`.
- Added `requirements.txt`, `config.example.yaml`, `Scripts/__init__.py`, and a
  `tests/` suite (30 tests, no network); broadened `.gitignore` from literal
  filenames to patterns; untracked 11 `.pyc` files; removed dead files and stale
  data directories.
- **Upgraded `espn-api` 0.45.1 → 0.46.0** for the traded-player `proTeam` fix
  ([plan 05](plans/05-dependency-upgrades.md)). The equivalence harness caught a
  silent breaking change in the process: 0.46.0 repurposed `points_breakdown`
  from raw stats to applied points, so every stat column became a point value
  until the call sites moved to `['breakdown']`. 2025 ran on 0.45.1; 2026 runs on
  0.46.0.
- **ESPN data is now persisted.** It used to be fetched, blended in memory,
  pushed to Sheets and discarded, so re-examining last week meant re-fetching it.
  `python -m Scripts.refresh` writes `Data/Store/<season>/<league_key>/`, and
  `streamlit run app/main.py` reads it — 11ms against ~8s to rebuild pre-season
  and ~23s in season ([plan 07](plans/07-frontend-foundation.md)). Nothing in the
  app's render path touches ESPN.
- **The blend survives a missing source.** `clean_pinny`/`clean_bol` read the
  season's weekly props unconditionally, so `clean_lineups` raised
  `FileNotFoundError` every pre-season, when those files do not exist yet. They
  now degrade to an absent source: the columns are imputed from the ESPN/FP mean,
  flagged, and renormalised out of `TRUE_*`, with coverage recorded in the store
  and shown in the app.
- **League-aware draft boards exist.** `refresh --all --what board` writes
  `board.parquet` per league: replacement level from each league's real starting
  slots, VOR, 1-D KMeans tiers, and value against ADP. The same player ranks
  differently across the nine leagues for the right reason — Josh Allen is VOR rank
  9 in the 10-team superflex and 21 in 14-team Knights, because the superflex `OP`
  slot pushes QB replacement to QB20 ([plan 15](plans/15-draft-board.md)).
- **Fixed the season path never using plan 11's per-slot scoring.**
  `build_season_projections` scored through a local loop over one scoring table,
  with a comment saying it could not do per-slot values — a comment that predated
  plan 11. So GOP Degenerates' individual defenders were priced with the D/ST-slot
  override of **0.0 for tackles**, and linebackers, whose points are almost all
  tackles, projected near zero: LB replacement came out at LB1. It now calls
  `proj_to_score`, and the top DP options are real tackle leaders.
- **Google Sheets is now a renderer over the store, not a second pipeline.**
  `run()` held a line-for-line duplicate of `equivalence.build_league_frame()` —
  the same two-drifting-copies shape that already cost this repo once with the 12
  projection functions. It reads `Data/Store` now, so Sheets and the app cannot
  disagree, and it takes `current_week` from the store's metadata rather than a
  live fetch. Verified: all ten tabs identical from the store versus a fresh
  ingest, and `run()` completes with every outbound socket blocked. Sheets is
  **kept** — it is a published artifact readable from a phone with the laptop
  shut, which the app cannot be ([plan 14](plans/14-thin-google-sheets.md)).
- **Fixed a silent free-agent-tab regression.** `write_to_google` read a
  `select_league` global that commit `304ba39` (2026-08-05) turned into a local,
  so all eight `FA_*` Sheet tabs raised `NameError` — swallowed by a bare
  `except` that reported it as "Position Does Not Exist in League". Introduced
  and caught the same day, so no published Sheet was affected; the point is that
  nothing would have told you. No bare `except` blocks remain in that file.

- **The usage-model gates are measured, and one of them failed.**
  `Rscript R/GetUsage.R 2016 2025` pulls ten seasons of nflverse expected
  production and observed usage; `python -m Scripts.usage.gates` builds a 5,257
  player-week 2025 evaluation set out of all nine league stores, fits the crudest
  possible usage model on 2016–2024, and prints the pairwise residual-correlation
  matrix. **G0 passed decisively** — usage residuals correlate +0.832 with ESPN's
  where FantasyPros' correlate +0.988, and FantasyPros turns out to be the *least*
  independent source in the blend. **G1 failed**: adding it raises per-stat MAE at
  every weight tried, so nothing is wired into `WEIGHTS`. The useful part is the
  decomposition — on rows where the player actually took snaps the effect is
  −0.16% to +0.35%, so essentially the whole deficit is **not knowing who plays**.
  That reorders the work: availability features first
  ([plan 16](plans/16-usage-data-layer.md#step-0--the-gates-measured-2026-08-06)).

- **The draft board has a page, and building it fixed three things underneath it.**
  `app/pages/draft_board.py` reads `board.parquet` and nothing else: the scarcity
  curve out to 1.6× replacement level with each position's replacement rank drawn
  in, a tier-runway chart answering "how many are left in tier 2", value-on-the-board,
  and the full table sorted by **value rather than rank**. Renders for all nine
  leagues, verified headless. Josh Allen is VOR rank **9** in the 10-team superflex
  against **21** in 14-team Knights, because the `OP` slot pushes quarterback
  replacement to QB20 ([plan 09](plans/09-frontend-draft-views.md)).

  The three defects it surfaced, none of them visible from the builder's own output:
  **(1)** `_apply_scoring` propagated NaN, so `ESPN_Points`/`FP_Points`/
  `PINNY_Points`/`BOL_Points` were NaN **1,026 of 1,026 rows on every board in every
  league** — a running back has no `ESPN_passingYards` and passing yards is a scored
  rule everywhere. The weekly path was unaffected because it 0-fills first, proved by
  recomputing every prefix over all nine 2025 `lineups.parquet` at max difference
  0.0. **(2)** `projection_missing` was `TRUE_Points.isna()`, which the 0-filling
  blend never trips: False for all 1,026 including 503 players projected a literal
  0.0, and `board_summary` had been claiming "1026 projected" where the honest number
  is 523. **(3)** A structural zero counted as a source opinion, so FantasyPros
  registered as a real source for a kicker on twelve non-imputed `0.0` cells and
  reported floor == ceiling as measured agreement.

  All three are the same underlying thing, and it is worth naming: **a `0.0` that
  means "nothing here" is indistinguishable from one that means "zero", and any
  count built on `notna()` reads the first as the second.**

**Credentials have never been committed** — verified across all of history.
`config.yaml` and `gs4creds.json` are gitignored and remain plaintext on disk,
which is acceptable for a single-user repo but is the obvious hardening target.

### Still to do before the season

- [ ] Run `Rscript R/GetNFL.R 2026` to generate the 2026 schedule. **Everything
      keys off this** — until it exists, the scrapers still report season 2025.
- [ ] Decide what to do about BetOnline weekly props (below).
- [ ] Re-run the full weekly pipeline end-to-end once against 2026 and confirm
      the Sheets render.

---

## Known issues

### Blocking

| Issue | Location |
|---|---|
| **BetOnline weekly props API returns `403 invalid_security_headers`.** `bv2-us.digitalsportstech.com` now requires a signed request header the scraper does not send. Browser-like UA/Origin/Referer/`gsetting` headers do not satisfy it. This is an anti-bot control and should not be circumvented. BOL contributes 10–40% of the blend weight depending on the stat, so its absence shifts projections. **Options:** drop BOL and re-weight the other three; replace it with another book; or drive it through a real browser session as Pinnacle already does via Selenium. → [plan 02](plans/02-betonline-access.md) | `Scripts/scrape_BOL.py` |

Note the **season-long** BetOnline endpoint (`api-offering.betonline.ag`) is a
different host and **works** — verified serving 2026 data. That is the
draft-relevant one, so the draft board is unaffected.

**Pro-Football-Reference is gated the same way**, found 2026-08-06 while looking
for a free coaching-staff table. PFR sits behind a Cloudflare managed challenge:
even `/robots.txt` returns the JS-challenge interstitial, and
`/teams/nwe/2025.htm` is a **403** to a normal request. Same decision as
BetOnline — not circumvented. Wikipedia's MediaWiki API serves the same data and
is meant to be used this way: every `<year>_<Team>_season` article carries
`coach`, `off_coach`, `def_coach` in its infobox. →
[plan 16](plans/16-usage-data-layer.md#coaching-context-pfr-is-unavailable-wikipedia-is-not)

### Correctness / data quality

| Issue | Location |
|---|---|
| **Blend weights assume coverage the sources don't have.** Pinnacle covered 213 players in 2025 wk 17 vs FantasyPros' 575 and BetOnline's 598, and has no defensive stats at all — but carries a full 25% weight. Gaps are imputed from the ESPN/FP mean, so for most players `PINNY_*` *is* ESPN/FP, double-counted. → [plan 03](plans/03-projection-source-coverage.md) | `projection_utils.py` |
| **Unrecognised scoring rules are silently dropped.** `build_scoring_table()` emits a NaN `colName`, which `proj_to_score` then skips without error. Two GOP Degenerates kicker rules are affected in 2026. → [plan 01](plans/01-scoring-coverage.md) | `scrape_player_stats.py` |
| `clean_pinny()` is ~27% commented out — the pivot, TD-split and no-vig `adjust_value()` are inert, so it returns near-raw data. Measured, Pinnacle *does* still contribute real lines for the players it covers; the problem is coverage (plan 03), not absence. An earlier note here overstated this. | `projection_utils.py` |
| Player matching is still `(week, player_name)` string equality in the **projection sources**, patched by hardcoded rename dicts (~140 entries). The ESPN side is fixed: `Scripts/crosswalk.py` gives an ID join at 98.5-99% coverage, and boards carry `gsis_id`. Pointing FantasyPros/Pinnacle/BetOnline at `fantasypros_id` is the remaining work — 89% of offensive players resolve, the misses being 2026 rookies. → [plan 20](plans/20-consensus-sources.md) | `projection_utils.py`, `scrape_pinnacle.py:33-46` |
| ESPN sometimes doubles yardage projections; worked around by halving when `ESPN > FP*1.75 and > 40`. A heuristic, not a fix. | `projection_utils.py` |
| The 2025 Pinnacle juice formula changed mid-season in commit `c3b4d16` (sign flipped, coefficient halved 0.5 → 0.25) with no explanation in the message or the code. Unclear which is correct. | `scrape_pinnacle.py` |
| BOL splits `anytimeTouchdown` 100% to rushing for QB/RB and 100% to receiving for WR/TE. Crude for pass-catching backs. | `scrape_BOL.py` |
| Blend weights are hand-tuned ([plan 03](plans/03-projection-source-coverage.md)). Two learned-weight models (OLS per stat, and a `LinearRegression` combo) were built in the notebook and never productionised — with 2025 actuals in hand these could replace the guesses. | notebook cells 11, 14 |
| A matchup-period hack hardcoded by league id for `521152` weeks 15/17, tied to 2025's playoff structure. The IDP-scoring branch keyed on `1727104` is **gone** — replaced by the registry's `slot` dimension ([plan 11](plans/11-per-slot-scoring.md)). | `scrape_player_stats.py:204` |
| `get_free_agent_stats()` is dead — wrong arity, references a non-existent `league.currentMatchupPeriod`. | `scrape_player_stats.py:242-259` |
| FantasyPros URLs take no season parameter, so the 2025 CSV cannot be reproduced by re-scraping. Backtests must use archived data. | `scrape_FP.py` |

### Maintainability

| Issue | Location |
|---|---|
| Test coverage is thin in the places that matter most. `tests/` covers paths, config, season/week derivation, the scoring registry, per-slot scoring, the blend primitives, the store, the usage layer's leakage guarantee and the draft board page's derivations (346 tests, no network), including a guard that the notebook never re-defines the shared projection functions. Nothing covers the scrapers, the Sheets renderer, `analytic_utils`, `luck_index`, or `simulation_utils`. | `tests/` |
| No retry/backoff on any HTTP call. Four bare `except:` blocks remain — `populateGoogleSheet.py`'s is gone. A global `warnings.filterwarnings("ignore")` in `fetch_utils.py:16` silences every warning process-wide; `Scripts.scoring` and `Scripts.projection_utils` each force their own filter past it, which is a workaround rather than a fix. → [plan 06](plans/06-performance.md) | repo-wide |
| `build_league_frame` calls `fetch_league`, then `get_ply_stats_by_matchup` calls it again — ~1s of duplicated ESPN round-trip per league, ~12% of a pre-season refresh. Fixing it means changing that function's signature from ids to a `League`. → [plan 06](plans/06-performance.md) | `equivalence.py`, `scrape_player_stats.py:463` |
| `oauth2client==4.1.3` is end-of-life upstream and is only needed for Sheets auth. A Google auth change would mean migrating to `google-auth` mid-season, so it is worth doing before the season. → [plan 14](plans/14-thin-google-sheets.md) step 2.3 | `populateGoogleSheet.py`, `requirements.txt` |
| A Sheets publish spends ~9.3 min in `time.sleep` for rate limits (5s per sheet × 10 tabs, plus 20s per league). Now that ingest is a store read, that is essentially the entire runtime. Cutting tabs nobody opens is the cheapest fix. → [plan 14](plans/14-thin-google-sheets.md) step 2.1 | `populateGoogleSheet.py` |
| ESPN ingest uses `pd.concat` inside row loops and `df.loc[i, col]` cell assignment — quadratic. Also 54 pandas fragmentation warnings per blend run. → [plan 06](plans/06-performance.md) | `scrape_player_stats.py:162`, `scrape_team_stats.py:204-343` |
| Pandas/Polars split: sportsbook scrapers and the app are Polars, ESPN ingest and the blend layer are Pandas. The store is the conversion point — `Scripts.store` reads pandas, `app.store` reads Polars from the same parquet. New code should be Polars. | — |
| The notebook is ~938 KB, of which the large majority is committed cell output; history holds 8+ full copies. Consider `pip install nbstripout && nbstripout --install` so future commits strip outputs. Not wired up here because a declared-but-missing git filter breaks `git checkout` for anyone without the package. | `FF Analysis Notebook.ipynb` |
| `tidbit_utils.py` functions are still named `django_*` with ~56 lines of commented-out Django ORM code, inherited from the upstream project. There is no Django app here. | `tidbit_utils.py:5,20-75` |
| `R/GetSeasonProps.R` is 566 lines of which ~215 are **MLB** code copy-pasted from a separate betting project, and it calls an undefined `teamabbr_build()`. Only lines 245-338 are NFL-relevant. | `R/GetSeasonProps.R` |
| Commit messages are data-snapshot labels ("Week 11 Update" = 43 files, 21.7k insertions, ~6 lines of real code). Model-changing fixes ship undocumented inside them. | git history |

---

## Roadmap — draft strategy

The 2026 focus. Phases are ordered so each is independently useful if the
calendar tightens. Full design in `docs/DRAFT_SYSTEM.md` (written alongside the
build).

| Phase | Deliverable | Depends on |
|---|---|---|
| **1. Draft history** | Backfill 2016-2025 drafts to `Data/Draft/`. Points-over-expectation per manager, positional tendency by round, reach/steal distribution. Also captures `bid_amount` and `keeper_status`, which the ESPN wrapper already parses but the old code ignored. | — |
| **2. Season projections** | Season-long stat lines from FantasyPros (`week=draft`), BetOnline season props (port `R/GetSeasonProps.R` to Python, emitting raw stats not PPR), and ESPN. Blended and scored per league. Plus a real player crosswalk. | — |
| **3. ADP + VOR + board** | ~~**Done**~~ — [plan 15](plans/15-draft-board.md). `python -m Scripts.refresh --all --what board` builds nine league-aware boards in ~16s. One `kona_player_info` request per league returns ADP, auction values **and** a 45-stat season projection, carrying `player.id` so the ESPN join is exact. Replacement level from each league's real starting slots; 1-D KMeans tiers; value vs ADP. | 2 |
| **4. Draft simulation** | Monte Carlo mock drafts. Opponent models calibrated from Phase 1 tendencies. Tests Zero-RB / Hero-RB / BPA from your actual slot. | 1, 3 |
| **5. Live assistant** | Terminal app polling the draft: best available by VOR, tier breaks, roster needs, positional-run alerts, value vs ADP. | 3 |

Two findings make this cheaper than it looks:

- **FantasyPros accepts `week=draft`** for full-season projections — a one-line
  change, already wired in as `DRAFT_WEEK` in `scrape_FP.py`.
- **`R/GetSeasonProps.R` already does the hard part** — scraping BetOnline
  season futures and removing the vig. It just needs porting to Python and
  rescoring per league instead of hardcoded PPR. Its 2025 output is preserved at
  `Data/Projections/BetOnline/Season/2025/BetOnline_SeasonProps_Offense.csv`.

If time runs short, **Phase 2 → 3** still gets a real draft board. Phases 1, 4
and 5 are upside.
