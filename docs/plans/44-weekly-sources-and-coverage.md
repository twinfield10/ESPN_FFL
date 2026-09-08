# 44 — The weekly blend had one live source, and the panel that should have said so

**Status:** COMPLETE

**Priority:** High · **Effort:** M · **Where it stands:** Steps 0-6 built and measured
2026-09-08, the day before week 1. The weekly blend went from **ESPN plus 47 players**
to three real sources over 157 of 334 · step 6 is a seam with no consumer yet, by
design
**Depends on:** [03](03-projection-source-coverage.md) — provenance flags and
renormalisation · [36](36-sportsbook-scrapes.md) — the freshness discipline this
extends to the weekly path
**Feeds:** [08](08-frontend-weekly-views.md) · [19](19-weekly-usage-model.md) — step 6
is that plan's step 5, pre-built

> **The blend was never the problem.** `clean_lineups` already calls
> `compute_weighted_stats` against the same `WEIGHTS` the draft board uses, with the
> same equal-vote-per-real-source renormalisation. "Make the weekly blend the same as
> the draft" was already true. What was not true is that the weekly *sources* were
> alive, that anything rebuilt the weekly artifact on a schedule, or that the sidebar
> coverage panel measured what its label claimed.

## What was actually broken

Eight things, all measured on the ten 2026 stores on 2026-09-08.

| | Finding |
|---|---|
| 1 | **The schedule was frozen and week detection was stuck at 1.** `Data/NFL_Schedules.csv` was dated 08-14: 272 rows, **0 with a score**. Only `R/GetNFL.R` writes it and the nightly never called it. `current_week()` is "the first week with an unplayed game", so a scoreless file returns **1 forever** — and `Scripts/scrape_FP.py` binds `WEEK = current_week()` at *import*. |
| 2 | **…and that would have taken the nightly down.** `SEASON_STARTED` is derived from the same column, so it stayed `0`, so `book_stage` and `book_rows_guard` stayed **fatal**. The first night a book retired its season-long market — which the script's own comment says it will — `fail()` fires: no board rebuild, no S3 push, a desktop notification instead of data. |
| 3 | **FantasyPros weekly was the anonymous registration teaser.** 60 rows, week 1 only, `TimeStamp 2026-08-03` — scraped three weeks before the free account that lifts the fence landed on 08-24, and never re-run, because the nightly only ever ran `--what season`. |
| 4 | **`weekly_sources_present` reported that file as `True`.** It was `Path.exists()`. A 25-day-old 60-row teaser for an unplayed week was indistinguishable from a live feed, and every consumer — the weekly table's columns, `real_sources`, the sidebar's absent-source caption — was told FantasyPros was fine. |
| 5 | **`check_source_freshness` never ran on the weekly path.** All four call sites were in `season_projections.py`. The weekly FantasyPros read was a bare, unguarded `pd.read_parquet` — the only one of the three weekly loaders with neither an absence path nor a staleness check. |
| 6 | **Nothing rebuilt `lineups.parquet` on a schedule.** The nightly ran `--what board` only. `DEFAULT_WHAT` is `("lineups",)`, but only a human ever invoked it: boards were 06:01 and lineups were from whenever someone last typed the command. |
| 7 | **The sidebar showed Pinnacle 4.1% and BetOnline 4.1% for sources with no weekly line at all.** `coverage_report` excluded `_is_imputed` and `_sd` columns but not `<SRC>_Points` and `<SRC>_PosRank` — which have no provenance flag and so fell into the `notna()` branch at 100%. **4.1% is exactly 2/49 columns: those two.** Every genuine PINNY and BOL stat column was 0.0%. The panel built to stop an absent source reading as agreement was doing precisely that. |
| 8 | **The app's own per-player source count was dead code.** `with_source_spread` cut at `_imputed_share < 0.5`. FantasyPros carries **47** flag columns on a Knights frame and fills at most **15** of them for any player — the other 32 are kicker bands, D/ST bands, two-point conversions and targets it structurally never publishes — so the minimum imputed share over 334 rows was **0.681** and **0 rows** cleared the cut. `sources_real` was uniformly 1 and `source_spread` null for every player, all season. The Roster tab's "Single-Source Starters" counted every starter. |

## The requested change, and why the denominator was the smaller half

The ask was to narrow the coverage denominator from all players to **rostered players
plus the top 20 free agents at each position**, excluding IDP. Built, and measured — on
its own it moves almost nothing:

| league | rows before | rows after | FP cell coverage before → after |
|---|---|---|---|
| winfield_football | 242 | 216 | 12.4% → **13.1%** |
| knights_ffl | 334 | 314 | 10.1% → 10.5% |
| gop_degenerates | 521 | 306 | 7.8% → 8.5% → **10.7%** dropping IDP |

**The dilution was in stat-space, not player-space.** A cell average divides by 45-odd
stats, most of which a given source never publishes, so it answered a question about
cells while the label claimed a question about players. A denominator of players wants
a numerator of players, so `meta["coverage"]["players"]` is now what the sidebar reads
and `overall` is kept beside it for a store built before this.

`player_coverage` reuses `attach_source_spread`'s rule verbatim, extracted to
`projection_utils.source_contributed`: a source counts for a player when it supplied at
least one **non-null, non-zero, non-imputed** cell to a blended stat. The non-zero
clause is not fussiness — it is the Cameron Dicker case that function was written for,
where FantasyPros read real "on the strength of twelve zeros".

### IDP is excluded unconditionally, and that is not a shortcut

Exactly one league of ten rosters individual defenders. `league.free_agents(position='DT')`
returns nothing for the other nine, so they carry **zero** IDP rows and dropping the
positions is arithmetically a no-op there (216 → 216 on Winfield, 314 → 314 on Knights).
In the league that does, the 195 IDP rows have **no** real cell from FantasyPros,
Pinnacle *or* BetOnline, so including them graded those sources against players they do
not publish.

The alternative — probing `meta["starting_slots"]` for a defensive slot — would read the
one field `app.lineup.slot_counts` documents as disagreeing with the lineups **for
exactly that league**: its metadata says `DP: 1` while its lineups hold `CB`, `DE`,
`DT`, `LB` and `S`. And it would not be available anyway, because `coverage_summary`
runs inside `store.build_meta`, where the live league is often `None`.

The cost, stated: in that league 45 rostered players stop being counted. GOP's
FantasyPros number reads 16.0% with IDP out against 10.9% with it in, and the
difference is entirely players no source but ESPN covers.

## What landed

**Step 0 — `R/GetNFL.R` in the nightly, first.** Before the `SEASON_STARTED` probe that
depends on it. `GetNFL.R` refuses to write a schedule under 250 rows, so a truncated
upstream response fails loudly rather than producing a file that breaks week detection.
It exits 0 pre-season having skipped the play-by-play-dependent files, so it does not
trip `|| fail`. The nightly also now exports `WEEK` for the stages below.

**Step 1 — `DERIVED_SOURCE_COLUMNS`.** `("Points", "PosRank")`, named once and used by
both `coverage_report` and `present_prefixes`, which had been excluding the same two
names by separate literals. Pinnacle and BetOnline dropped from a fictitious 4.1% to
**0.0%**.

**Step 2 — `coverage_population`.** Rostered plus the best *n* free agents per position
per week, IDP dropped first so the top-20 comes from what survives. Ranked by
`ESPN_Points` with `projPoints` then `TRUE_Points` as fallbacks. **Returns the frame
unchanged when there is no ownership column** — the property that keeps it safe on the
season path, where 940 of a 1,036-row board are free agents and the coverage question
genuinely is about the whole market. Applied in `store.coverage_summary` only; both
`print_coverage_report` call sites still see the full frame.

**Step 3 — one definition of "real".** `source_contributed` extracted from
`attach_source_spread`, which now calls it. Verified identical: every league × prefix
agrees cell for cell, and all nine boards with a scoring registry reproduce their
stored `sources_real` distribution and `floor` sum exactly. `app.lineup._contributed`
is the polars twin, pinned against the pandas one by a test, and replaces the 0.5 cut.

**Step 4 — the sources.** `scrape_weekly` fetches the current week alone and **merges**
with `keep="first"`, so each week freezes at first capture (the rule `Scripts.freeze`
uses) and the nightly stays a flat ~30s instead of nine minutes by week 18. The week
resolves at call time, not import. `weekly_sources_present` now means *usable* — exists,
younger than 48h, and carrying rows for this week. `check_source_freshness` runs on all
three weekly loaders, and the FantasyPros read is guarded like its siblings.
`refresh_status` gained a separate `WEEKLY_PROJECTION_SOURCES` manifest, advisory for
the two dead books so the exit code is not red every night.

**Step 5 — `--what lineups` in the nightly**, after the board stage so a failure in the
longest path in the repo costs the S3 push rather than the boards.

**Step 6 — the weekly TOMCAT seam**, which is [19](19-weekly-usage-model.md) step 5
pre-built. See below.

## The two source findings

**FantasyPros weekly lifts the fence too, and nobody had checked.** Authenticated, the
same page returns **597 rows over 595 players** against the teaser's 60. On Knights that
took FantasyPros from **17.7% to 96.4%** real on the key stats.

**Pinnacle's weekly props are live, and [36](36-sportsbook-scrapes.md) step 3's
deferral no longer holds.** That step was deferred because Pinnacle had posted **zero**
weekly player props across all sixteen week-1 games. Re-probed 2026-09-08 the Selenium
path did not time out: **165 props over 165 players and all 16 games, in 35 seconds.**
Wired in as a **non-fatal** nightly stage — `scrape_pinnacle` exits 1 on an empty
result, which is the right contract for a human and the wrong one for cron, since a
book's weekly board is genuinely empty some of the time.

BetOnline weekly remains 403 `invalid_security_headers` and is not circumvented
([02](02-betonline-access.md)). Its freshness hint names the reason rather than a
command that cannot succeed.

### What the weekly blend is now, measured on Knights week 1

| | before | after |
|---|---|---|
| ESPN | 100% (of cells) | **96.5%** of players |
| FantasyPros | 15.6% of players | **86.3%** |
| Pinnacle | a fictitious 4.1% | **50.3%** |
| BetOnline | a fictitious 4.1% | **0.0%**, honestly |
| players with ≥2 real sources | 48 | **284** |
| players with 3 real sources | 0 | **157** |
| `source_spread` populated | **0** | 284 |

ESPN below 100% is honest rather than a regression: the rows it does not cover are
bye-week or inactive players whose entire line is zeros — rows nobody has an opinion on.
The old cell metric could not express that.

Across all ten leagues, after the rebuild:

| league | denominator (was) | ESPN | FantasyPros | Pinnacle | BetOnline |
|---|---|---|---|---|---|
| winfield_football | 216 (242) | 99.5 | 87.5 | 63.0 | 0.0 |
| knights_ffl | 314 (334) | 96.5 | 86.3 | 50.3 | 0.0 |
| gop_degenerates | 306 (521) | 96.4 | 85.6 | 51.3 | 0.0 |
| weenieless_wanderers | 206 (206) | 92.2 | 93.7 | 61.2 | 0.0 |
| jeffs_league | 177 (201) | 98.3 | 98.3 | 70.6 | 0.0 |
| twelve_dudes_one_cup | 196 (196) | 93.4 | 83.7 | 66.3 | 0.0 |
| big_red_fantasy_football | 234 (234) | 93.6 | 85.0 | 56.4 | 0.0 |
| john_pc_league | 288 (308) | 97.9 | 86.8 | 53.8 | 0.0 |
| john_atl_league | 214 (214) | 92.5 | 83.6 | 57.0 | 0.0 |
| fields_league | 270 (290) | 97.4 | 97.0 | 58.1 | 0.0 |

The denominator does not move at all in four of the ten, because a 12-team league with
deep rosters has a free-agent pool under twenty at most positions already. The leagues
it moves are the ones with the shallowest rosters and the widest pools — which is where
the old number was least meaningful.

## Step 6: the TOMCAT seam, and the constraint that shaped it

`USG` is in `WEEKLY_PREFIXES` and **not** in `WEIGHTS`, and both halves are deliberate.

`WEIGHTS` is shared by both grains. A weekly entry would also re-admit TOMCAT to the
draft board it was withdrawn from on 2026-09-07 on a measured level error
([43](43-tomcat-out-of-season-blend.md)), and a second weekly-only weights dict would
break the property this plan started from — that the weekly blend *is* the draft blend.
So turning the arm on stays one deliberate line, taken on in-season evidence against
plan 03's pre-registered bar: **+16.91%** MAE over all rostered player-weeks and
**+1.55%** on players who took a snap, so roughly nine tenths of the harm was
availability rather than accuracy.

Listing the prefix is safe only because `present_prefixes` exists. Plan 34 added it
after `USG_Points` was written null for all 3,602 rows of every 2025 weekly store,
because a column shaped like a source that never has an opinion reads as a source that
agreed. With no weekly file there are no `USG_` columns, the prefix is filtered out
before `proj_to_score`, and a rebuild is byte-identical.

`tests/test_weekly_points_identity.py`'s guard was rewritten from *"`USG` is not in
`WEEKLY_PREFIXES`"* to the property that assertion stood in for: a registered prefix
with no weekly stat line gets **no** `_Points` column, not a null one — and its mirror,
that a prefix *with* a stat line is scored, so the seam is not dead text.

`clean_usage_weekly` merges outside the impute chain. Every other source is filled from
`MEAN_` = avg(ESPN, FantasyPros) when it has no line; TOMCAT must not be, because it is
the one source in the register not derived from the others (G0: residual independence
**+0.832** against FantasyPros' **+0.988**), and filling it from an average of two of
them would count those two a third time — the double-count plan 03 exists to have
measured. Its cells are flagged imputed where null, so the weight is dropped and the
rest renormalise.

## The Athletic will publish weekly projections, and it registers like anything else

Noted here and on the `ATH` card in `Scripts/lab/sources.py` rather than built. When the
weekly workbook appears it needs a loader, a `WEEKLY_PREFIXES` entry and provenance
flags, the same as every other source.

**Do not synthesise one by dividing the season workbook by games.** A season total over
17 is not an opinion about *this* week's matchup: it cannot disagree with itself week to
week, so it would add a vote that is a constant, and renormalisation would give that
constant a full share on every row it covers. The same argument [39](39-source-basis.md)
makes about basis, one grain down.

## Not fixed here, and recorded so they are not re-derived

- **The TOMCAT withdrawal is unmerged, so every board published since 2026-09-07 still
  weights it.** [43](43-tomcat-out-of-season-blend.md) is marked COMPLETE and
  `STATE_OF_THE_REPO.md` says it shipped, but the code sits uncommitted: `HEAD` still
  reads `'USG': 0.25`. The nightly refuses to run off anything but `origin/main`, so it
  has been rebuilding and pushing TOMCAT-weighted boards every morning since.
  Proof rather than inference — reblending a stored board gives max `TRUE_receivingYards`
  drift of **101.32** under the working tree's weights and **0.000000** under HEAD's,
  on both leagues. This is what
  `test_lab_g2.py::test_reblend_reproduces_the_shipped_board` is failing on; it clears
  the moment the withdrawal merges and the nightly rebuilds.
- ~~**`john_pc_league`'s `lineups.parquet` has zero rostered players.**~~ **Transient,
  and it cleared on rebuild.** The store held 153 rows, every one a free agent, with no
  `BE` or `IR` in its slot set — `get_ply_stats_by_matchup` had returned nothing for it.
  Rebuilt the same afternoon it reads **168 rostered** and 120 pool. Recorded because
  the failure is silent: the league builds, the app renders, and the only visible sign
  is a coverage denominator made entirely of free agents. Worth a glance at the
  `rostered` count in `meta["coverage"]["population"]` if a league's numbers look
  unusually good.
- **`jeffs_league` is absent from `Data/Scoring/scoring.csv` for every season.** It was
  added 2026-09-01 and `python -m Scripts.scoring --all` has not run since, so it builds
  boards on a cold registry — `get_scoring_table` derives the table from the live league
  and warns. It works, but the committed scoring history that `git log -p` is supposed
  to provide does not exist for that league. One command, not a code change.
- **The blend can project a bye-week player off FantasyPros alone.** Knights: Josh
  Jacobs, status `bye`, `projPoints 0.0`, `ESPN_Points 0.0`, `FP_Points 16.4`,
  `TRUE_Points` **8.2**. ESPN carries no provenance flags, so its all-zero line counts
  as a real vote of zero and the two average. One row before this plan; it scales with
  the FantasyPros coverage this plan multiplied, and the fix is a provenance question
  about ESPN rather than anything here.
- **A NaN provenance flag means opposite things in two places.** `compute_weighted_stats`
  and `coverage_report` read it as imputed; `attach_source_spread` reads it as real.
  Latent, not live: **zero** flag cells are NaN across all twenty stored artifacts, so
  nothing published depends on it. `source_contributed` takes it as an explicit argument
  so the difference is visible rather than accidental.

## Verification

- `Rscript R/GetNFL.R 2026` writes 272 games; `current_week()` is 1 today and the
  nightly's `SEASON_STARTED` snippet flips on the Tuesday after week 1.
- `python -m Scripts.scrape_FP --what weekly --week 1` returns 597 rows / 595 players.
  Re-running preserves the capture; `--no-merge` replaces it.
- `python -m Scripts.scrape_pinnacle --dry-run` returns 165 props over 16 games.
- `coverage_report` on all ten stores reports Pinnacle and BetOnline at 0.0 and emits no
  `Points` or `PosRank` row.
- `attach_source_spread` reproduces all nine boards' `sources_real` and `floor` exactly;
  `source_contributed` agrees with the old inline logic on every league × prefix, and
  the pandas and polars implementations agree on every league × prefix.
- `python -m Scripts.refresh_status` prints a `-- weekly blend --` block, and only
  FantasyPros weekly moves the exit code.
- A rebuild with no weekly TOMCAT file is byte-identical: 334 rows × 506 cols.
- `pytest`: 2,020 passed, the two `test_lab_g2` failures above being the unmerged
  withdrawal.
