# State of the Repo

**Last updated:** 2026-08-14, preparing for the 2026 season. Plans 07 (local data
store + app), 14 (Sheets reads the store) and 15 (draft boards) landed on the 5th;
plan 16's data layer and its go/no-go gates on the 6th; plan 09's draft board page,
plan 16's availability and feature layers, plan 18's season usage model with its
rookie arm, and plan 21's depth-chart and coaching context on the 7th — and, later
the same day, plan 18 steps 3 and 6: the usage model wired in as the blend's **fifth
source** at weight 0.0, abstaining at quarterback. Plan 23's owner tendencies landed
on the 10th, and **plan 24 on the 11th moved the data itself to S3** — the largest
structural change here since plan 07, and the one that changes how you set the repo
up on a new machine.

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
and a data store with a Streamlit app reading it.

**That store now lives in S3 rather than on this laptop**, which is the one change
here that alters how you set the repo up: `Data/` is no longer tracked in git and no
longer authoritative, and the app needs AWS credentials by default. See *The data
lives in S3* below.

Both things that were true at the start of this cycle are now addressed:

1. ~~**Nothing was set up for 2026.**~~ See *2026 readiness* below.
2. ~~**There is no draft capability.**~~ The only draft file was dead code copied
   from another project. It is deleted; `Scripts/draft/` builds a league-aware board
   ([plan 15](plans/15-draft-board.md)), `app/pages/draft_board.py` renders it
   ([plan 09](plans/09-frontend-draft-views.md)), and there is a **usable draft-day
   artifact today**, and `docs/plans/23-owner-tendencies.md` puts owner tendencies
   on it. What remains is the live draft page.

**There is also a projection model now, and it is honest about its limits.**
`Scripts/usage/` holds a season head that predicts 80.4% of rostered players from
observed usage rather than from other people's projections. Against the naive draft
heuristic it improves ordering at RB/WR/TE and cuts MAE 10–13% on the noisy rate
stats; it does not improve yardage and is slightly worse at quarterback. Its **rookie
arm is the clear win** — draft capital plus depth-chart position orders rookies at
ρ ≈ 0.64 where a projection carrying no such information manages ~0, on 1,497
player-seasons the model previously said nothing about.

**The board says when the model's evidence is thin.** `usg_evidence` names the
conditions under which a projection orders players worse, chosen by measurement: a
prior season under 8 games (+42% rank error), a team change (+32%) and bottom-quartile
prior volume (+23%). Two obvious candidates were rejected by the same measurement --
having only one prior season is no worse than two (-7%), and a **rookie orders 14%
better** than the pool, so the intuitive version of this flag would have marked the
model's strongest arm as its weakest. 27 of 162 draftable players carry a flag; DJ
Moore, Wan'Dale Robinson and Kenneth Walker III -- three of the largest disagreements
with ESPN -- are all team changes, while Justin Jefferson at ADP 12 is unflagged.

**The usage model is adjusted by ESPN's estimated return date.**
`Scripts/scrape_espn_injuries.py` pulls `site.api.espn.com`'s injury report, which
carries a `returnDate` the fantasy API does not -- 152 of 152 non-active records have
one, against a free-text outlook present for only 9 of 22 there. The model is scaled by
`games_available / 17`, so "misses the first five weeks" is expressible; a
season-ending sentinel (2027-02-15) withdraws it outright, and a player the report does
not know falls back to the fantasy status.

Only the model is scaled. ESPN and FantasyPros already price a known absence, so
discounting the whole blend would count the same injury twice.

It replaced a status-only abstention that was wrong for 9 of 22 players -- Alec Pierce
(ADP 96) returns 13 August, Zach Charbonnet (ADP 149) on 9 September, the day before
week 1. Withdrawals fell 22 to 13.

On access: `www.espn.com/robots.txt` does not disallow `/nfl/injuries` for general
agents, though it blocks ten named AI crawlers site-wide including `anthropic-ai`. The
API host publishes no robots.txt (403 on the file), which RFC 9309 classes as
"unavailable" and permits. Unlike Pro-Football-Reference and BetOnline's weekly
endpoint, there is no anti-bot control here to circumvent.

**Superseded -- the status-only rule.** It cannot see a
current injury and the other sources can -- nflreadr refuses 2026 injuries, so
`expected_games` is built from prior-season statistics about a player who was healthy
last August. Left alone the model inflated exactly the players it knew nothing about:
across the 22 players ESPN listed OUT or IR, adding it lifted the blend by a mean of
**+15.7 points** while lowering active draftable players by 2.7. ESPN and FantasyPros
both projected Ricky Pearsall at 0.0 -- they know he is on IR for the season -- and the
model pulled the blend to 72.4. It now abstains, flagged, so the weight is dropped and
the sources that know carry the player.

ESPN gives no structured return date. Probed live: `injuryStatus`, `injured`,
`lastNewsDate` and a free-text `seasonOutlook` present for only 9 of 22 injured
players. The prose often carries a timeline in words, but parsing it would be a
fragile answer to a question ESPN's own projection already encodes.

**`USG` blends on an if-healthy basis.** The model predicts an expected value
(per-game production x ~13.6 expected games); ESPN and FantasyPros project a healthy
17-game season. Blending them mixed two quantities, and unevenly -- the usage model
covers QB/RB/WR/TE and not K or D/ST, so skill positions came out at 0.887-0.900 of
their ESPN/FantasyPros level while kickers and defences sat at exactly 1.000. That is
11% of cross-position distortion, which VOR inherits directly. `to_full_slate`
rescales before blending and the residual is now 1-3%. The model itself is unchanged
and the backtest is untouched -- only the artifact the blend consumes is rescaled, and
the availability estimate travels beside it as `usg_expected_games`.

**The blend is an equal quarter each to ESPN, FantasyPros, BetOnline and the usage
model**, with **Pinnacle** at zero — `projection_utils.WEIGHTS` is
`{'ESPN': 0.25, 'FP': 0.25, 'PINNY': 0.0, 'BOL': 0.25, 'USG': 0.25}`. This paragraph
said "an equal three-way split of ESPN, FantasyPros and the usage model, with Pinnacle
**and BetOnline** at zero" until 2026-08-18, and was wrong: BetOnline carries a full
quarter, and the *Known issues* table below has always said so ("BOL contributes 10–40%
of the blend weight"), so this file contradicted itself. **Which of the two is the
intended decision is still open** — the three-way split may be what was meant and never
landed in the code. The boards in the store were all built with BOL at 0.25.

**And the nominal weights are not the realised ones.** `compute_weighted_stats` drops a
source's weight where its cell is flagged imputed and renormalises the rest, so the
effective weight is per-row. Measured on Winfield Football's 2026 board over players with
a positive `TRUE_Points`:

| stat | ESPN | FP | BOL | USG |
|---|---|---|---|---|
| receivingYards | 0.69 | 0.04 | 0.06 | **0.21** |
| receivingReceptions | 0.70 | 0.04 | 0.03 | **0.22** |
| rushingYards | 0.85 | 0.05 | 0.02 | **0.08** |
| passingYards | 0.91 | 0.05 | 0.01 | **0.02** |

ESPN is the **only** real source for 45.5% of receiving-yard rows and 84% of passing-yard
rows, and it carries 0.69–0.91 rather than 0.25. Two causes, and the first is structural:
**ESPN has no `_is_imputed` columns at all** (FP, PINNY and BOL have 136 each, USG has 8),
so `compute_weighted_stats`'s "a source with no provenance column counts as real" branch
means ESPN's weight is *never* dropped — including on the 668 of 1,027 rows where
`ESPN_receivingYards` is null and `.fillna(0.0)` turns its silence into an opinion of
zero. That is contained rather than harmless: `sources_real` is 0 and
`projection_missing` is True for 501 of those rows, so the board can tell them apart, but
`TRUE_receivingYards` is `0.0` and not null for all 668.

Recorded as an owner decision rather than inherited. G2 is still unanswered and still unanswerable on history;
what changed is the evidence around it, with the model now beating the naive draft
heuristic on every metric at every position in 26 of 28 out-of-sample season-position
cells. Note the decision drops the better-covered market source: **BetOnline's season
endpoint works and resolves 273 players with 13 stat columns including IDP tackles and
sacks, against FantasyPros' 60.** Only the *weekly* BetOnline endpoint is blocked, on a
different host that never fed this path.

**It was previously the blend's fifth source at weight 0.0.** `python -m
Scripts.usage.project` writes it, `Scripts/season_projections.py` loads it, and
`USG_Points` / `USG_PosRank` / `USG_PosRankDelta` / `usg_expected_games` / `usg_arm`
are on all nine boards. It contributes nothing to `TRUE_Points` and that was verified
rather than assumed: rebuilding a league with and without the weight entry gives all
45 `TRUE_` columns bit-identical over 1,026 rows.

The comparison that would justify a real weight — the blend with and without `USG_`,
scored against realised results — cannot be run on any past season, because no
historical pre-season blend survives. The 2026 board is the first chance, and that
means after the season is played. What shipping at 0.0 buys is that answering it is
then one number rather than a build.

It is also, unexpectedly, the **best-covered source in the pre-season blend**: 23.1%
real cells against ESPN's 13.1%, FantasyPros' 0.8%, Pinnacle's 0.1% and BetOnline's
0.3%, and 51.9% on receiving yards against ESPN's 34.9%. Players with any real
projection went 523 → 675.

It is deliberately **not** in the floor/ceiling spread, having been briefly added
there by mistake. G0's independence result made it look like what a disagreement
interval was missing, but independence is about information content and that interval
needs the sources to be answering the same question. `USG_Points` is an expected
value where the other four project a healthy season, so it sat below all of them for
51.7% of the players it covered and widened the median interval from 8.5% to 24.0%.
Disagreement between forecasters and uncertainty within one forecast are different
quantities; the spread holds the first, and the model's dissent is carried by
`USG_PosRankDelta`, which being a rank cannot be contaminated by the level mismatch.
→ [plan 18](plans/18-season-usage-model.md)

One problem from the rollover is still open: **BetOnline's weekly props API blocks
the scraper**, removing one of four projection sources. Details below — it needs a
decision.

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
  app's render path touches ESPN. *(Superseded in part by plan 24: the writer still
  writes that path, but the app now reads the S3 copy of it by default.)*
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
  `app/pages/draft_board.py` reads `board.parquet` and nothing else, across three
  tabs: **Board** (search, position/team/bye filters, an auction budget, the table
  sorted by VOR), **Values** (where the room and our valuation disagree) and
  **League** (the scarcity curve out to 1.6× replacement level with each position's
  replacement rank drawn in, the tier-runway chart answering "how many are left in
  tier 2", and owner tendencies). Renders for all nine leagues, verified headless.
  Josh Allen is VOR rank **9** in the 10-team superflex against **21** in 14-team
  Knights, because the `OP` slot pushes quarterback replacement to QB20
  ([plan 09](plans/09-frontend-draft-views.md)).

  The `$` column had been showing ESPN's own $200 auction in leagues that play for
  $250. It is now a share of a budget, rescaled to whatever the Board tab is set to.

- **The app shows the viewer's leagues, not all nine.** `app/auth.py` is the seam a
  login lands in — one function, called from one component, so identity does not
  have to be retrofitted into every page later. There is no authentication yet and
  it is **not** a security boundary; `ESPN_FFL_ALL_LEAGUES=1` drops the scope for
  when another owner's Sheet needs explaining ([plan 26](plans/26-user-accounts.md)).

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

- **There is a season usage model, and it is measured.**
  `Rscript R/GetContext.R` pulls availability and role data (2016–2025 backfilled,
  2026 as far as upstream allows); `Scripts/usage/features.py` builds season features
  with an as-of guarantee; `Scripts/usage/season.py` fits volume × efficiency ×
  expected games and emits `USG_<stat>` lines for `proj_to_score` to price nine ways;
  `python -m Scripts.usage.backtest` runs a 2019–2025 walk-forward
  ([plan 18](plans/18-season-usage-model.md)).

  Against the naive draft heuristic it improves within-position ordering at RB
  (+0.0218 Spearman), TE (+0.0295) and WR (+0.0106), and cuts MAE 10-13% on receiving
  touchdowns and interceptions — the noisy rates where shrinking to a positional
  baseline is most of the edge. It does **not** improve yardage, it makes QB ordering
  slightly worse, and top-N hit rate is a wash.

  **The rookie arm ships.** Draft capital is a far stronger signal than plan 18
  assumed: 87.9% of drafted rookies play against 21.2% of undrafted, and pick number
  correlates −0.57 to −0.60 with the volume that matters per position. On the
  walk-forward it orders rookies within position at ρ ≈ 0.61 where a projection
  carrying no draft information manages ~0, and roughly halves MAE. Coverage went
  57.8% → 80.4%. It needed no new data pull — `rosters_weekly.draft_number` was
  already in the feature frame, and `load_draft_picks` would have been the wrong
  source anyway: for **2026 it carries no real `gsis_id` at all**, so the join would
  have fitted on history and returned nothing for the season that needs it.

  **It is deliberately not in `WEIGHTS`.** The comparison that would justify wiring
  it in — the four-source blend with and without it — cannot be run on any past
  season, because FantasyPros' URLs take no season parameter and no historical
  pre-season blend survives. The 2026 board is the first chance to answer it.

  **The expected-games heads work in share of the slate, not games** (v1.1.0). The
  NFL went 16 games to 17 in 2021 and 45% of the training range predates it, so a fit
  in raw games learned a blend of two eras — players who managed 16+ the prior year
  average 13.06 next season in the 16-game era against 13.64 in the 17-game era. On
  the veteran arm the correction is small (+0.066 games), because the old fit had the
  slate on both sides and largely self-corrected; on the **rookie bins**, which are a
  plain mean with no predictor to compensate, it lands properly — rookie ordering
  improved from ρ ≈ 0.61 to ρ ≈ 0.64 and rookie MAE fell 4–8%. No metric regressed.

  Fixing it nearly introduced a worse bug: a share needs a denominator, and a rookie
  who never played has no outcome row and so no measured slate. Filtering those rows
  out took the undrafted bin from **1.1 games to 5.8** — projecting a camp body as a
  third of a season — while every printed table still looked reasonable. A missing
  slate is now filled, never filtered. Same lesson as the `0.0` one below: **a player
  who never appeared is a zero, not an absent observation.**

  **Games played is reported as a distribution, not a number.**
  `Scripts/usage/availability.py` fits a Beta-Binomial — chosen by measurement, not
  taste: the variance of the games share is 5.6x to 8.1x what a Binomial permits over
  3,942 player-seasons. Mean, variance, PMF and exact quantiles are all closed form,
  so no simulation is involved. The board carries `usg_games_sd`, `usg_games_low` and
  `usg_games_high`; fitted concentration is 2.1-3.2 by position, i.e. heavy
  overdispersion.

  Held-out calibration over 4,211 player-seasons: realised coverage 87.5% against the
  model's own claim of 89.4%. Against the *nominal* 80% that looks badly over-wide,
  and is not — an integer p10/p90 on an 18-value support always excludes less than
  asked, so the claim is what it must be judged against. Judging it against 80% would
  have condemned a calibrated distribution for a property of the support.

  **The stat lines carry intervals too.** `Scripts/usage/predictive.py` fits a
  Negative Binomial for counts and a Gamma for yardage, closed form throughout. The
  natural design -- model games, volume and rate separately and multiply -- was
  measured and rejected: games against per-game volume correlate **+0.48 to +0.63**,
  so a product of independent factors understates the spread, and backing one
  variance out of another produced negative numbers. Each stat's dispersion is fitted
  end-to-end on held-out residuals instead.

  Getting them to calibrate took four fixes, all found by measuring coverage: NaN is
  not null in Polars (`is_not_null()` is True for NaN, which put coverage at 6%);
  in-sample dispersion is too narrow; the coefficient of variation is **not
  constant**, falling 1.90 to 0.48 across the projection range, so the variance
  function needs two parameters; and a Gamma has no mass at zero while 10.5% of rows
  realise exactly zero. Final coverage is 74.6-91.1% against a nominal 80%, except
  `passingYards` at 60.8% -- which is the quarterback arm the model already abstains
  on, so it never reaches a board.

  One finding worth carrying: conditional on the opportunity count the bounded rates
  are barely overdispersed (1.08-1.79x Binomial) against 5.6-8.1x for games and
  13-99x for volume. **Nearly all the reducible uncertainty is how much work a player
  gets, not what he does with it** -- an argument for spending effort on the depth
  chart rather than on efficiency modelling.

  **Snap share is an availability regressor, and deliberately not a rate
  denominator.** Both `snap_counts` and `injuries` were already on disk for 2016-2025,
  unused. Prior-season snap share is the largest single gain available to the model's
  weakest arm -- predicting next season's games, R-squared goes 0.203 to 0.230, and in
  the fitted head RB 0.187 to 0.224, WR 0.188 to 0.215. It reads as role security: 85%
  of snaps is entrenched, 25% is one depth-chart move from inactive.

  Using it the other way -- to stop a player who left on the first drive from being
  docked a full game -- was measured and **rejected**. The distortion is real (those
  games average 1.86 targets against 5.21, and excluding them lifts affected players
  +8.2%), but correcting it made prediction worse: R-squared 0.693 to 0.684 for the
  narrow version, and 0.693 to 0.295 for the general one. A part-time player's low
  per-appearance rate *is* his role, and the model already discounts injury once in
  `expected_games`, so cleaning the rate applies it twice. Injury-report features add
  +0.003 on top of snap share and are held for plan 19, where the live report is a
  primary signal.

  Downstream every backtest metric improved and one flipped: rushing yardage MAE went
  from +0.4% (worse than the naive baseline) to -1.9% (better), RB top-24 hit rate now
  beats naive 0.637 to 0.625, and the games interval calibrates at 90.0% against its
  own claim of 89.9%.

  **The depth chart joined the veteran volume arm, and every backtest metric now
  beats the naive draft heuristic** -- which was not true of any earlier version. This
  came out of testing a team-then-allocate architecture, which *failed*: predicting
  team volume then allocating by role scored R-squared 0.5488 against the direct
  model's 0.5633. The two oracle rows are what mattered. Knowing every team's rushing
  volume perfectly buys +0.006 R-squared; knowing every player's *share* perfectly
  buys +0.42. The bottleneck was never team volume, and share is what a depth chart
  describes.

  A comment in the code had claimed the depth chart was measured out of the veteran
  arm. It was not -- the experiment it described varied only the coach prior, and the
  depth chart was swept into the sentence. Tested properly it moves RB carries from
  R-squared 0.5584 to 0.6066, and QB pass attempts from 0.353 to 0.455.

  Spearman against the naive heuristic went QB -0.0115 to **+0.0132**, RB +0.0196 to
  **+0.0623**, WR +0.0126 to **+0.0531**, TE +0.0300 to **+0.0658**; receiving yardage
  MAE -6.3% to -12.3%, rushing -2.9% to -9.0%, and passing yardage from +1.8% (worse
  than naive) to **-8.2%**.

  **The quarterback abstention is lifted** as a result. QB was declined because the
  model measured worse there; the deficit closed as the model improved (-0.0155 ->
  -0.0153 -> -0.0119 -> -0.0115) and the depth chart flipped it positive. Coverage goes
  73.2% to **83.7%**, and the only draftable gaps left are positions the model has
  never modelled: 17 D/ST, 14 K and four skill players.

  A scheduling fact worth knowing for the draft: **nflreadr will not serve 2026
  injuries, snap counts or depth charts at all** while `most_recent_season()` is
  2025, though it does serve the 2026 roster and updates it daily. So a pre-season
  availability estimate has to come from trailing games played plus roster status,
  and that estimate is weak by nature — prior-season games predict next season at
  r = +0.663 over the whole pool but only +0.343 among players who managed 8+ games,
  so most of the apparent signal is separating reserves from starters rather than
  durable players from fragile ones.

- **The data lives in S3.** `s3://espn-ffl-data` (`us-east-2`, versioning already
  enabled) is the system of record; `Data/` is a writer's scratch pad plus a read
  cache and is **no longer tracked in git at all**. `Scripts/s3_store.py` is the
  boundary, `python -m Scripts.sync --push/--pull/--verify` moves bytes, and step 6
  of `run_daily_refresh.sh` pushes on a clean run only, so S3 never receives stale
  data wearing a fresh timestamp ([plan 24](plans/24-s3-data-flow.md)).

  The durability problem it solves was specific, not general: the one artifact in
  this repo that **cannot be rebuilt at any price** — the G2 counterfactual, which
  exists because FantasyPros serves no season parameter, so a board is gone the
  moment it stops being current — was durable only in the sense that it was
  committed to git. It now sits under `archive/`, exempt from version expiry
  forever, while `store/` and `nfl/` expire noncurrent versions after 90 days
  because both regenerate.

  **The unplanned win is the dated board snapshot.** Each night's push writes
  `snapshots/board/season=/league=/date=/`, so nine boards a night accumulate
  instead of being overwritten. That retires the entire class of problem `Data/G2/`
  was hand-built to work around, and makes **ADP drift through camp** measurable at
  daily resolution across nine leagues — something that was never available before
  and improves with every night that passes.

  Two consequences to know. **The app is network-dependent by default:** a cold S3
  render is ~3× a local read and still sub-second (94 ms local, 231–342 ms cold S3,
  58 ms on a Streamlit cache hit, since the parse is cached too), and
  `ESPN_FFL_STORE_SOURCE=local` is the draft-morning escape hatch precisely because
  a render path that needs a network can fail at the worst possible moment.
  **Set-level atomicity is gone** — each PUT is atomic and `meta.json` still uploads
  last as the sentinel, so a reader sees the old complete store or the new one, but
  the five objects of one league's store are no longer written as a unit. That is
  inherent to S3 and is written down rather than papered over.

  Verified rather than assumed, re-checked 2026-08-14: `--verify` reports **249
  current-state files SHA-256 identical** local against S3, the lifecycle rules are
  live on the bucket, and the nightly push has run unattended every night since —
  `snapshots/` holds a dated board for the 11th, 12th and 13th, which is the first
  evidence that this works without anyone watching it.

**Credentials have never been committed** — verified across all of history.
`config.yaml` and `gs4creds.json` are gitignored and remain plaintext on disk,
which is acceptable for a single-user repo but is the obvious hardening target.
**AWS credentials are now load-bearing** and come from the standard boto3 chain
(`~/.aws/credentials`, `AWS_PROFILE`, or the environment) — nothing project-specific,
and nothing read from `config.yaml`.

### Still to do before the season

- [x] ~~Run `Rscript R/GetNFL.R 2026` to generate the 2026 schedule.~~ Done
      2026-08-03. Needs **one more run after week 1** for
      `Data/NFL/2026/NFL_Stats.csv` and a refreshed `NFL_Tackles_By_Position.csv`,
      which need play-by-play that does not exist yet.
- [ ] Decide what to do about BetOnline weekly props (below).
- [ ] Re-run the full weekly pipeline end-to-end once against 2026 and confirm
      the Sheets render.
- [x] ~~Surface `USG_` on the draft board **without** blending it.~~ Done
      2026-08-07 — wired as the fifth source at weight 0.0, on all nine boards.
      → [plan 18](plans/18-season-usage-model.md#the-fifth-source-wired--2026-08-07)
- [x] ~~Abstain for QB in the season head.~~ Added and then removed on 2026-08-07.
      It took coverage 80.4% → 73.2%, and was lifted once the depth chart entered
      the veteran arm and quarterback ordering went positive (+0.0132 against the
      naive baseline). `season.ABSTAIN_POSITIONS` is `()`; coverage is **83.7%**.
- [x] ~~Render the new `USG_` columns on the board page.~~ Done 2026-08-14. Four
      columns after the market block on every table: `USG`, `Δrk`, `Exp G` and a
      **Model evidence** column that resolves what an empty `USG` means, because it
      meant three different things — the model does not cover the position (K, D/ST),
      it declined a player whose expected games were too low, or the injury report
      withdrew a price it had already made. All three rendered as the same blank
      before, which read as agreement. Sorting by the model's dissent is offered in
      both directions. Verified headless across all nine leagues.
      → [plan 09](plans/09-frontend-draft-views.md)

The ordered list of everything outstanding lives in
**[`plans/README.md` §What is left](plans/README.md#what-is-left)**.

The subset of it that a **draft in the next two weeks** depends on — with the dates
ESPN actually holds, and a day-by-day countdown — is
**[`DRAFT_READINESS.md`](DRAFT_READINESS.md)**, assessed 2026-08-24. Short version:
nothing blocks a draft, the boards are built and 1,174 tests pass, and the one real
risk is that ~12,300 lines of plans 27-30 are uncommitted on `main`.

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
| Player matching is still `(week, player_name)` string equality in the **projection sources**, patched by hardcoded rename dicts (~140 entries). The ESPN side is fixed: `Scripts/crosswalk.py` gives an ID join at 98.5-99% coverage, and boards carry `gsis_id`. **The usage source is the first to join on an id** (`gsis_id` → `player_id`, 769 of 778 for 2026); pointing FantasyPros/Pinnacle/BetOnline at `fantasypros_id` is the remaining work — 89% of offensive players resolve, the misses being 2026 rookies. → [plan 20](plans/20-consensus-sources.md) | `projection_utils.py`, `scrape_pinnacle.py:33-46` |
| **The crosswalk carries no 2026 rookies.** 95 of the usage model's predictions resolve to no ESPN id, all of them rookies — the population its strongest arm exists to project. Worked around with a `join_key` name fallback in `_merge_usage`, which inherits the shared-name protection but is still a name join. A refreshed `load_ff_playerids()` pull closer to the season should shrink it. | `Scripts/crosswalk.py`, `season_projections.py` |
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
| Test coverage is thin in the places that matter most. `tests/` covers paths, config, season/week derivation, the scoring registry, per-slot scoring, the blend primitives, the store, the usage layer's leakage guarantee, the draft board page's derivations, the season usage head with both its arms, the fifth source's registration/join/abstention plumbing, the coaching table's Wikipedia parsing, the team-profile as-of boundary, and the S3 layer — key mapping, checksummed upload, the `meta.json`-last *sequence*, sync's push/pull/verify and the app's three read modes, all against a stub so it still needs no network or credentials, the board page's model block including the three ways an empty `USG` has to be told apart, the board's four include-filters, the auction-budget rescale, the keeper-pending test and the cash lens's budget conservation, and the viewer scoping that decides which leagues the app may offer, and the whole injury layer -- episode construction with its three kinds of censoring, the severity ladder including a pinned regression for a comment that describes a *teammate's* injury, and the fitted curve's placebo guard, which requires a null effect to fit to no effect (1,125 tests), including a guard that the notebook never re-defines the shared projection functions. Nothing covers the scrapers, the Sheets renderer, `analytic_utils`, `luck_index`, or `simulation_utils`. | `tests/` |
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
| **1. Draft history** | ~~**Mostly done**~~ — [plan 23](plans/23-owner-tendencies.md). `python -m Scripts.refresh --all --what draft` pulls 5,748 picks across 36 league-seasons in 10s, into the store rather than `Data/Draft/`. Positional tendency by round, NFL-team lean, player loyalty, rookie appetite, autodraft rate, and auction budget shape — every one leave-one-out against the room and season-matched. `bid` and `keeper` are captured. **Points-over-expectation is not done**: it needs every past season scored in each league's own rules, which the store does not hold. | — |
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

If time runs short, **Phase 2 → 3** still gets a real draft board. Phases 4 and 5
are upside; Phase 1 landed.
