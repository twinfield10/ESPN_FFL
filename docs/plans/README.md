# Upgrade plans — 2026 season

Small, self-contained plans from the pre-season scan on 2026-08-01. Each one is
Problem / Evidence / Fix / Effort, so it can be picked up independently.

Phase 0 (2026 rollover, pipeline de-duplication, season-scoped paths, docs,
tests) is already done — see [`../STATE_OF_THE_REPO.md`](../STATE_OF_THE_REPO.md).
These are what the scan turned up *beyond* that.

| # | Plan | Status | Why it mattered / what is left |
|---|---|---|---|
| 01 | [Scoring coverage gaps](01-scoring-coverage.md) | **Done** | Two GOP kicker rules were silently dropped and nothing detected it |
| 02 | [BetOnline access](02-betonline-access.md) | **Partly resolved** | Season props wired up with IDP; the **weekly** props API still 403s and needs a decision |
| 03 | [Projection source coverage](03-projection-source-coverage.md) | **Partly done** | Renormalisation and provenance landed. Left: the weight re-tune (now unblocked by 16) and `scrape_pinnacle.py`'s import-time Selenium scrape |
| 04 | [Matchup-period handling](04-matchup-periods.md) | Not started | Winfield_Football silently loses a week of data |
| 05 | [Dependency upgrades](05-dependency-upgrades.md) | **espn-api done** | 0.46.0 also silently swapped stats for points, caught by the equivalence harness. Rest of the upgrades open |
| 06 | [Performance](06-performance.md) | Not started | Quadratic `pd.concat` in row loops; a duplicated `fetch_league` round-trip; a process-wide warnings filter |
| 07 | [Frontend foundation & data store](07-frontend-foundation.md) | **Done** | 11ms to read a league back from parquet against ~8s to rebuild it |
| 08 | [Week-to-week views](08-frontend-weekly-views.md) | Not started | Unblocked since 07 |
| 09 | [Draft views](09-frontend-draft-views.md) | **Board done** | Board page renders for all nine leagues. Left: **Live Draft**, and **Draft History** (blocked on roadmap Phase 1) |
| 10 | [Scoring registry](10-scoring-registry.md) | **Done** | Scoring was re-derived from a mutable live object 4× per league and never recorded |
| 11 | [Per-slot scoring](11-per-slot-scoring.md) | **Done** | GOP's D/ST was inflated ~16%, and every league credited offensive players for imputed defensive stats at D/ST rates |
| 12 | [Season projections](12-season-projections.md) | **Done** | Season props blended and scored per league — the draft board's input |
| 13 | [D/ST from Vegas lines](13-dst-from-vegas-lines.md) | Not started | The only position with zero market coverage. Its `E[f(X)]`-over-tiers piece can be built now; the rest waits on posted 2026 lines |
| 14 | [Thin Google Sheets](14-thin-google-sheets.md) | **Step 1 done** | Sheets is a renderer over the store. **Kept**, not retired — readable on a phone with the laptop shut. Left: step 2.3, `oauth2client` → `google-auth` |
| 15 | [Draft board: ADP, VOR, tiers](15-draft-board.md) | **Done** | Nine league-aware boards in 16s. Also fixed the season path never using plan 11's per-slot scoring |
| 16 | [Usage data layer](16-usage-data-layer.md) | **Done** | The shared extraction and feature layer, the ID crosswalk, and the gates. **G0 passed** (+0.832 residual correlation with ESPN against FantasyPros' +0.988); **G1 failed** on the crude baseline and located the deficit in not knowing who plays |
| 17 | [Draft usage model](17-draft-usage-model.md) | **Superseded** | Split into 16 / 18 / 19 / 20. Two of its claims were measured and did not survive; the stub records which |
| 18 | [Season usage model](18-season-usage-model.md) | **Shipped as source 5, weight 0.0** | The draft head, now on all nine boards. **Best-covered source in the pre-season blend** — 23.1% real against ESPN's 13.1%. Abstains at QB, where it measured worse. **Rookie arm is the win** — ρ ≈ 0.61 against ~0. Weight stays 0.0 until a played season answers G2; turning it on is one number |
| 19 | [Weekly usage model](19-weekly-usage-model.md) | Not started | **Where the larger edge is.** Trailing expected production beats trailing actual at predicting next week (R² 0.2907 vs 0.2702), and it is the only head that gets the live injury report |
| 20 | [Consensus sources](20-consensus-sources.md) | **Deprioritised on evidence** | FantasyPros' marginal value is +0.027 against ESPN's +0.068, so a sixth expert aggregator is the worst available use of the effort |
| 21 | [Depth charts, scheme, play-caller](21-coaching-and-scheme.md) | **Done** | 2026 depth charts pulled past nflreadr's season guard — the daily snapshot that made the rookie arm work. Coach and coordinator priors built and **measured out** of both arms: the depth chart already carries their signal |
| 22 | [Feature research for the season head](22-feature-research.md) | **Measured, nothing merged** | Routes, Next Gen Stats, red-zone role and contracts pulled and tested; ridge swept. All eleven experiments rejected. The finding: player-level context that is a *function of past usage* does not survive either, because past usage is already the strongest regressor. Ships the data layer, the lab and `docs/model_lab.html` |

### Local frontend

Plans 07, 08 and 09 replace the notebook-plus-Google-Sheets workflow, split because
the foundation blocked the other two.

**07 is done**: the store lives at `Data/Store/<season>/<league_key>/`,
`python -m Scripts.refresh` builds it and `streamlit run app/main.py` reads it — 11ms
against ~8s to rebuild a league pre-season and ~23s in season. **09's board page is
done** and renders for all nine leagues. **08 is unblocked and not started.**

Both plans have postscripts worth reading before touching that code, because building
each turned up bugs nothing would otherwise have reported — a same-day regression that
silently failed all eight `FA_*` Sheet tabs, and every per-source point column being
NaN on every stored draft board.

Sheets is **kept**, not retired: it is a published artifact readable on a phone with
the laptop shut, and five of the eight published leagues belong to other owners.
See [plan 14](14-thin-google-sheets.md).

## What is left

Ordered by when it matters, not by plan number. Everything not listed here is done;
each plan doc carries its own evidence and postscript.

### Before the draft

| | Plan | Why it is next |
|---|---|---|
| ~~1~~ | ~~**Show `USG_` on the board without blending it** — [18](18-season-usage-model.md) step 3~~ | **Done 2026-08-07.** Wired as the fifth source at weight 0.0 — scored per league, on all nine boards, and verified not to move `TRUE_Points` (max difference 0.0 over 1,026 rows) |
| ~~2~~ | ~~**Abstain for QB in the season head**~~ | **Done 2026-08-07 and then undone the same day.** The abstention was lifted once the depth chart landed in the veteran arm and quarterback ordering went positive; `ABSTAIN_POSITIONS` is now `()`. The backtest overrides it either way so the evidence stays reproducible |
| 1 | **[09](09-frontend-draft-views.md) Live Draft page** | The last draft-critical UI piece. Slips gracefully — the board on a second monitor works without it |
| 2 | **Render the new `USG_` columns** — [09](09-frontend-draft-views.md) | The board page predates them. `USG_PosRankDelta`, `usg_expected_games` and `usg_arm` are on `board.parquet` and nothing shows them yet. Note the level caveat: `USG_Points` is injury-adjusted and `TRUE_Points` is not, so compare ranks and not points |

### Before week 1

- **[03](03-projection-source-coverage.md)** — the weight re-tune, now unblocked by
  plan 16. Also holds `scrape_pinnacle.py`'s Selenium scrape at **module import
  time** (no `__main__` guard), which launches Chrome on import and currently times
  out.
- **[04](04-matchup-periods.md)** — Winfield_Football silently loses a week.
- **[08](08-frontend-weekly-views.md)** — the weekly views. Unblocked since plan 07.
- **[13](13-dst-from-vegas-lines.md)** — its `E[f(X)]`-over-tiers piece can be built
  and tested against 2025 now; the rest waits on posted 2026 game lines.
- **[14](14-thin-google-sheets.md) step 2.3** — `oauth2client` → `google-auth`.
  End-of-life upstream, and better done before the season than during it.

### In season

- **[19](19-weekly-usage-model.md)**, the weekly head, comes online around week 3 and
  is where the larger edge was measured: trailing *expected* production beats trailing
  actual at predicting next week (R² 0.2907 against 0.2702). It is also the head that
  gets the live injury report — the season head cannot, because nflreadr does not
  serve one before week 1.
- **[18](18-season-usage-model.md) G2** — build the 2026 board with and without
  `USG_` in `WEIGHTS` and score both against realised 2026. This is the only way the
  comparison can be made; no historical pre-season blend survives.
- **[21](21-coaching-and-scheme.md)** — measure the coach and coordinator priors
  against plan 19, which has not been tried. They were rejected as regressors for the
  season head because the depth chart already contained their signal.

### Whenever

- **[06](06-performance.md)** — quadratic `pd.concat` in row loops, the duplicated
  `fetch_league` round-trip, the process-wide `warnings.filterwarnings("ignore")`.
- **[05](05-dependency-upgrades.md)** — the rest of the dependency upgrades.
- **[20](20-consensus-sources.md)** — deprioritised on evidence. Plan 16 step 0 put
  FantasyPros' marginal value at +0.027 against ESPN's +0.068, so a sixth expert
  aggregator is the worst available use of the effort. Sleeper's case now rests on the
  ID join and coverage, not independence.
- **Roadmap Phases 1, 4, 5** — draft history backfill, the Monte Carlo mock-draft
  simulator, and the live terminal assistant. Phase 1 blocks
  [09](09-frontend-draft-views.md)'s Draft History page.

### Blocked or waiting on the calendar

- **BetOnline weekly props** return `403 invalid_security_headers` and need a
  decision: drop and re-weight, replace the book, or drive it through a real browser
  as Pinnacle already is. → [02](02-betonline-access.md). The season-long endpoint is
  a different host and works, so the draft board is unaffected.
- **`Rscript R/GetNFL.R 2026`** needs a re-run once week 1 is played, for
  `Data/NFL/2026/NFL_Stats.csv` and a refreshed `NFL_Tackles_By_Position.csv`.
- **[09](09-frontend-draft-views.md) Draft History** waits on roadmap Phase 1.
- **Plan 18's rookie draft-capital arm** shipped; its remaining question — whether a
  *combine* arm adds anything — is unmeasured and low priority.

## Where the modelling stands

[Plan 16](16-usage-data-layer.md) is the shared data and feature layer plus the
independence gate; [18](18-season-usage-model.md) and
[19](19-weekly-usage-model.md) are the two model heads on it;
[21](21-coaching-and-scheme.md) is the situational context underneath both.
[Plan 17](17-draft-usage-model.md) is a superseded stub.

The season head predicts **83.7%** of rostered players on the 2026 pull (765 of 914)
and 80.3% across the walk-forward. The quarterback abstention that briefly took this
to 73.2% was lifted on 2026-08-07 once the depth chart landed in the veteran arm.
Against the naive draft heuristic it improves ordering at **all four** positions
(+0.013 QB, +0.062 RB, +0.053 WR, +0.066 TE) and cuts MAE 7–17% on every stat. Its
rookie arm is the clear win — draft capital plus depth-chart position orders rookies
at ρ ≈ 0.63 where a guess carrying no such information manages ~0.

[Plan 22](22-feature-research.md) then tried eleven things on top of it and merged
none of them. The running record is [`docs/model_lab.html`](../model_lab.html).

**It is the fifth source, at weight 0.0.** As of 2026-08-07 `USG_` is registered in
`WEIGHTS`, scored by `proj_to_score`, counted as an independent opinion in the
floor/ceiling spread, and present on all nine boards — while contributing exactly
nothing to `TRUE_Points`, verified at max difference 0.0 across all 45 `TRUE_`
columns over 1,026 rows.

That is not timidity, it is the gate: G0 (independence) passed at +0.832 against
FantasyPros' +0.988; G1 failed on the crude baseline and located the whole deficit in
not knowing who plays; **G2 cannot be measured on history at all**, because no
historical pre-season blend survives to compare against. The 2026 board is the first
chance, and that means after the season is played. Turning it on is one number in
`Scripts/projection_utils.py`.

Worth knowing before reading the column: `USG_Points` is an expected value (× ~13.5
expected games) where ESPN and FantasyPros project a healthy 17, so it sits ~20% low
for everyone. Compare `USG_PosRank`, not points. `python -m Scripts.usage.backtest`
reproduces every number above; `python -m Scripts.usage.project` builds the artifact.

Three things were measured and rejected rather than assumed, and are recorded in code
so they are not rediscovered: the coach prior in the veteran arm, the coach prior in
the rookie arm, and a sixth consensus source.
