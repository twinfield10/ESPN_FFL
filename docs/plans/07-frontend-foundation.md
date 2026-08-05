# 07 — Local frontend: foundation and data store

**Priority:** High · **Effort:** Medium · **Status:** Not started
**Blocks:** [08 (weekly views)](08-frontend-weekly-views.md), [09 (draft views)](09-frontend-draft-views.md)

## Goal

A local app that replaces the notebook-plus-Google-Sheets workflow with
something you can actually use during a draft and on a Sunday morning. This plan
covers the architecture and skeleton only; the views live in plans 08 and 09.

## The constraint that decides the design

Measured on Knights_FFL, 2025:

| Step | Time |
|---|---|
| `fetch_league` | 1.0s |
| `build_fa_market` | 4.1s |
| `get_ply_stats_by_matchup` | **17.6s** |
| `clean_lineups` | 0.2s |
| **Total, one league** | **22.9s** |
| **All nine leagues** | **3.4 min** |
| Read the same result from parquet | **0.01s** |

That's a **~2,700× gap**. Any UI that recomputes on interaction is unusable —
changing a week dropdown would cost 23 seconds. Note the blend itself is fast
(0.2s); essentially all the cost is ESPN round-trips.

So the app must **never call ESPN in the render path**. Ingest and presentation
get separated by a file-backed store, and refresh becomes an explicit action.

This also closes an existing gap: ESPN data is currently never persisted at all.
It's fetched, blended in memory, pushed to Sheets, and discarded — which is why
re-examining last week means re-fetching it.

## Architecture

```
  ESPN / FantasyPros / Pinnacle / books
                 │
                 ▼
      Scripts/refresh.py            <- explicit, slow, writes parquet
                 │
                 ▼
      Data/Store/<season>/<league>/ <- the boundary
                 │
                 ▼
      app/  (Streamlit)             <- read-only, fast, never touches ESPN
```

### The store

```
Data/Store/<season>/<league_key>/
    lineups.parquet      # clean_lineups output (~3.6k x 350)
    team_stats.parquet   # scrape_team_stats history
    draft.parquet        # draft history + ADP (plan 09)
    board.parquet        # computed draft board (plan 09)
    meta.json            # built_at, current_week, source coverage, versions
```

Scoring is deliberately *not* in here — it is an input to ingest, not an output,
since `extract_player_stats` needs the `colName` list before it can pull any
stats. It lives in `Data/Scoring/scoring.csv`; read it with
`Scripts.scoring.get_scoring_table()`. See [plan 10](10-scoring-registry.md).

`meta.json` is what makes staleness visible — the app shows "built 14 min ago,
week 3" in the header rather than silently rendering old numbers. Include
per-source coverage counts from plan 03 so a degraded source is visible in the
UI instead of hidden by imputation.

Add `Data/Store/` to `.gitignore` — it's regenerable and large.

### Refresh

`Scripts/refresh.py`, a CLI that reuses the existing pipeline:

```bash
python -m Scripts.refresh --league Knights_FFL          # ~23s
python -m Scripts.refresh --all                          # ~3.4 min
python -m Scripts.refresh --all --what team_stats,draft
```

The app gets a refresh button that shells out to this and streams progress. It
stays a subprocess deliberately: a 23-second call inside a Streamlit rerun
blocks the whole session.

`populateGoogleSheet.py` should become a *consumer of the store* rather than a
parallel pipeline, so Sheets and the app can never disagree. Its `run()` already
takes a league list, so this is a small change — swap the inline fetch for a
store read.

## Tech choice: Streamlit

Recommended. Reasons specific to this repo:

- Renders `pandas`/`polars` frames and the existing `plotly` figures directly —
  the notebook's charts port with minimal rewriting.
- `st.dataframe` with `column_config` gives sortable, filterable, conditionally
  formatted tables natively. That's most of what the Sheets output was for, and
  it's exactly what a draft board needs.
- `st.fragment(run_every="5s")` polls a live draft without re-running the rest
  of the app — the one hard requirement in plan 09.
- `st.cache_data` on parquet reads makes navigation instant.
- Pure Python, no build step, no JS toolchain to maintain for a single-user
  local tool.

**Alternatives considered.** Dash gives more layout control for materially more
boilerplate. Marimo is a nice reactive-notebook fit but less mature, and the
notebook is being retired anyway. A FastAPI + React split is the most flexible
and matches the FastAPI preference in `CLAUDE.md` — but that preference is about
building *APIs*, and this is a local single-user analytics UI where a JS build
step is cost without benefit. If the app ever needs to be shared with
leaguemates over the network, revisit; the store boundary above means the
frontend can be swapped without touching ingest.

## Layout

```
app/
    main.py              # entry: st.navigation, league/season/week selector
    store.py             # cached readers over Data/Store + staleness checks
    components/
        tables.py        # styled dataframe helpers (tiers, diff colouring)
        charts.py        # shared plotly builders
        header.py        # league picker, freshness badge, refresh button
    pages/
        ...              # plans 08 and 09
Scripts/refresh.py       # store builder
```

Run with `streamlit run app/main.py`.

### Shared state

League, season and week are global selectors in the sidebar, held in
`st.session_state` so they persist across pages. Every page reads the same
`store.load_lineups(season, league)`.

### `store.py` contract

```python
@st.cache_data(ttl=300)
def load_lineups(season: int, league_key: str) -> pd.DataFrame: ...

@st.cache_data(ttl=300)
def load_meta(season: int, league_key: str) -> dict: ...

def is_stale(meta: dict, max_age_min: int = 60) -> bool: ...
```

Cache keyed on `(season, league_key)` plus the store file's mtime, so a refresh
invalidates it without a manual cache clear.

## Steps

1. `Scripts/store.py` — path helpers extending `Scripts/paths.py`, plus
   `write_league_store()` / `read_league_store()` and the `meta.json` schema.
2. `Scripts/refresh.py` — CLI wrapping the existing fetch → `clean_lineups`
   path, writing the store. Reuses `build_lg_vars()` and `fetch_league()`.
3. `app/main.py` + `app/store.py` + header component — league picker and
   freshness badge, one placeholder page. Ship this before building views.
4. Point `populateGoogleSheet.py` at the store.
5. Add `Data/Store/` to `.gitignore`; document `streamlit run` in the README and
   the weekly runbook.

## Verification

- `python -m Scripts.refresh --league Knights_FFL` writes a store in ~23s, and
  `lineups.parquet` matches `clean_lineups` output exactly (reuse the Phase 0
  equivalence approach).
- Page navigation and week switching are visibly instant (<100ms).
- Stopping your network and reloading the app still renders — proving nothing in
  the render path touches ESPN.
- The freshness badge correctly reports a store built an hour ago as stale.
