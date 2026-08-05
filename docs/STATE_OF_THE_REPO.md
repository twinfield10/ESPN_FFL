# State of the Repo

**Last updated:** 2026-08-05, preparing for the 2026 season.

A standing assessment of what works, what is broken, and what to do next. Update
it as things change — particularly the *Known issues* table, which is the part
worth keeping honest.

Actionable items have small plans in **[`plans/`](plans/)**, each with evidence
and a proposed fix.

---

## Executive summary

The weekly in-season pipeline is mature and ran all of 2025. It has strong,
genuinely reusable bones: a league-aware scoring engine, a four-source
projection blend, a Monte Carlo season simulator, and a polished Sheets
renderer.

Two things were true at the start of this cycle, and one of them is now fixed:

1. ~~**Nothing was set up for 2026.**~~ Addressed — see *2026 readiness* below.
2. **There is still no draft capability.** The only draft file was dead code
   copied from another project. This is the current focus; see *Roadmap*.

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
| `Scripts/draft_utils.py` is dead — never imported, and reads `./src/doritostats/pick_value.csv`, a path from the upstream project this was copied from. Also carries another league's owner map. Being rewritten as `Scripts/draft/history.py`. | `draft_utils.py:56,68` |

Note the **season-long** BetOnline endpoint (`api-offering.betonline.ag`) is a
different host and **works** — verified serving 2026 data. That is the
draft-relevant one, so the draft board is unaffected.

### Correctness / data quality

| Issue | Location |
|---|---|
| **Blend weights assume coverage the sources don't have.** Pinnacle covered 213 players in 2025 wk 17 vs FantasyPros' 575 and BetOnline's 598, and has no defensive stats at all — but carries a full 25% weight. Gaps are imputed from the ESPN/FP mean, so for most players `PINNY_*` *is* ESPN/FP, double-counted. → [plan 03](plans/03-projection-source-coverage.md) | `projection_utils.py` |
| **Unrecognised scoring rules are silently dropped.** `build_scoring_table()` emits a NaN `colName`, which `proj_to_score` then skips without error. Two GOP Degenerates kicker rules are affected in 2026. → [plan 01](plans/01-scoring-coverage.md) | `scrape_player_stats.py` |
| `clean_pinny()` is ~27% commented out — the pivot, TD-split and no-vig `adjust_value()` are inert, so it returns near-raw data. Measured, Pinnacle *does* still contribute real lines for the players it covers; the problem is coverage (plan 03), not absence. An earlier note here overstated this. | `projection_utils.py` |
| Player matching is `(week, player_name)` string equality, patched by hardcoded rename dicts (~140 entries) that need annual curation. No ID-based crosswalk. A miss silently drops a player. | `projection_utils.py`, `scrape_pinnacle.py:33-46` |
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
| Test coverage is thin. `tests/` now covers paths, config, season/week derivation and the blend primitives (30 tests, no network), including a guard that the notebook never re-defines the shared projection functions. Nothing covers the scrapers, the Sheets renderer, `analytic_utils`, `luck_index`, or `simulation_utils`. | `tests/` |
| No retry/backoff on any HTTP call. Five bare `except:` blocks remain. A global `warnings.filterwarnings("ignore")` in `fetch_utils.py:16` silences every warning process-wide. → [plan 06](plans/06-performance.md) | repo-wide |
| ESPN ingest uses `pd.concat` inside row loops and `df.loc[i, col]` cell assignment — quadratic. Also 54 pandas fragmentation warnings per blend run. → [plan 06](plans/06-performance.md) | `scrape_player_stats.py:162`, `scrape_team_stats.py:204-343` |
| Pandas/Polars split: sportsbook scrapers are Polars, ESPN ingest and the blend layer are Pandas. New code should be Polars. | — |
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
| **3. ADP + VOR + board** | ESPN ADP and auction values via `view=kona_player_info` (the wrapper drops this payload). Replacement level from each league's actual roster slots. Tiers by 1-D clustering. Published as a `Draft_Board` Sheet tab. **Minimum viable draft-day artifact.** | 2 |
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
