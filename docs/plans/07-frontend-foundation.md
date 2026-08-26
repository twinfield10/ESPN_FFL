# 07 — Local frontend: foundation and data store

**Status:** COMPLETE

**Priority:** High · **Effort:** Medium · **Where it stands:** **Done** (2026-08-05)
**Blocks:** [08 (weekly views)](08-frontend-weekly-views.md), [09 (draft views)](09-frontend-draft-views.md)

## Goal

A local app that replaces the notebook-plus-Google-Sheets workflow with
something you can actually use during a draft and on a Sunday morning. This plan
covers the architecture and skeleton only; the views live in plans 08 and 09.

## The constraint that decides the design

| Step | 2025, week 17 | 2026, week 1 |
|---|---|---|
| `fetch_league` | 1.0s | 0.96s |
| `build_fa_market` | 4.1s | 4.82s |
| `get_ply_stats_by_matchup` | **17.6s** | **2.02s** |
| `clean_lineups` | 0.2s | 0.16s |
| **Total, one league** | **22.9s** | **~8s** |
| **All nine leagues** | **3.4 min** | ~1 min |
| Read the same result from parquet | 0.01s | **0.011s** |

Both columns are Knights_FFL. The left is a completed season; the right was
measured on 2026 pre-season while implementing this plan, and is the honest
figure for August. Almost all of the difference is
`get_ply_stats_by_matchup`, which loops over elapsed weeks — so the cost climbs
back toward 23s as the season runs.

Re-confirmed on 2026-08-05: `python -m Scripts.refresh --league Knights_FFL
--season 2025` builds a 3,602 × 479 frame in **23.7s** end to end, against 7.4s
for the same league's 235 × 469 2026 frame. The original 22.9s stands.

The gap is **~800× pre-season and ~2,700× by December**. Either way, a UI that
recomputes on interaction is unusable: changing a week dropdown would cost
seconds. Note the blend itself is fast (0.16s); essentially all the cost is ESPN
round-trips.

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
    lineups.parquet      # clean_lineups output (235 x 469 pre-season)
    team_stats.parquet   # scrape_team_stats history (opt-in)
    draft.parquet        # draft history + ADP (plan 09)
    board.parquet        # computed draft board (plan 09)
    meta.json            # built_at, current_week, source coverage, versions
```

Scoring is deliberately *not* in here — it is an input to ingest, not an output,
since `extract_player_stats` needs the `colName` list before it can pull any
stats. It lives in `Data/Scoring/scoring.csv`; read it with
`Scripts.scoring.get_scoring_table()`. See [plan 10](10-scoring-registry.md).

`meta.json` is what makes staleness visible — the app shows "built 14 min ago ·
week 3" in the sidebar rather than silently rendering old numbers. It carries the
per-source coverage counts from plan 03, so a degraded source is visible in the
UI instead of hidden by imputation.

Two invariants, both added during implementation because the app reads while
refresh writes:

- **Artifacts are written atomically** — to a `.tmp` sibling, then `os.replace`.
  A half-written parquet read from the app would otherwise raise in the render
  path.
- **`meta.json` is written last**, and its presence is what `has_store()` keys
  on. A directory with a parquet and no meta is a build in progress, not a store,
  and must not be selectable.

`Data/Store/` is in `.gitignore` — it's regenerable and large.

### Refresh

`Scripts/refresh.py`, a CLI that reuses the existing pipeline:

```bash
python -m Scripts.refresh --league Knights_FFL          # ~8s now, ~23s in season
python -m Scripts.refresh --all                          # ~1 min now, ~3.4 min in season
python -m Scripts.refresh --all --what lineups,team_stats
```

It calls `Scripts.equivalence.build_league_frame()` rather than re-implementing
ingest, so the store cannot drift from what the equivalence harness snapshots.
That function gained a `return_league=True` option to hand back the `League`
object `meta.json` needs.

`--what` defaults to `lineups`. **`team_stats` is opt-in**, not part of a default
refresh: it re-derives a league's entire history (2016-2026 for
Winfield_Football) and nothing about this week changes 2019. It also has to be
run over the full year range — `scrape_team_stats` normalises each season's
scores against the median of `end_year - 1`, so a single-season call has nothing
to divide by; `refresh` skips it with a message when the season is the league's
first.

One league failing does not abort the rest, and a failed league keeps its
previous store — so the badge shows an older build time rather than nothing,
which is the honest outcome. The CLI exits non-zero and names the failures.

The app gets a refresh button that shells out to this and streams progress. It
stays a subprocess deliberately: a multi-second call inside a Streamlit rerun
blocks the whole session.

`populateGoogleSheet.py` should become a *consumer of the store* rather than a
parallel pipeline, so Sheets and the app can never disagree. Its `run()` already
takes a league list, so this is a small change — swap the inline fetch for a
store read. **Done**, same day, once the duplication was measured: `run()` held a
line-for-line copy of `build_league_frame`'s body. See
[plan 14](14-thin-google-sheets.md).

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
    _bootstrap.py        # puts the repo root on sys.path
    main.py              # entry: st.navigation
    store.py             # cached Polars readers over Data/Store + staleness
    components/
        header.py        # league picker, freshness badge, refresh button
        tables.py        # styled dataframe helpers (plan 08)
        charts.py        # shared plotly builders (plan 08)
    pages/
        overview.py      # store overview (this plan)
        ...              # plans 08 and 09
Scripts/refresh.py       # store builder
Scripts/store.py         # store read/write contract
```

Run with `streamlit run app/main.py` from the repo root.

`_bootstrap.py` exists because `streamlit run app/main.py` sets `sys.path[0]` to
`app/`, not the repo root, so every `from Scripts.x import ...` would raise
`ModuleNotFoundError`. Import it first from any module under `app/`, including
page scripts — Streamlit executes those on their own.

### Shared state

League, season and week are global selectors in the sidebar, held in
`st.session_state` so they persist across pages. Every page reads the same
`store.load_lineups(season, league_key)`.

The three selectors are *dependent* — the league list comes from the season and
the week list from the league — so `st.session_state` is managed explicitly
rather than handed to the widgets via `key=`. A widget key holding a value that is
no longer in its options (week 14 after switching to a league that only has week
1) is exactly the case that misbehaves.

### `store.py` contract

```python
def load_lineups(season: int, league_key: str) -> pl.DataFrame: ...
def load_meta(season: int, league_key: str) -> dict: ...
def is_stale(meta: dict, max_age_min: int = 60) -> bool: ...
```

Cache keyed on `(season, league_key)` plus the store's newest mtime, so a refresh
invalidates it without a manual cache clear. Streamlit hashes every argument whose
name does not start with an underscore, so the mtime has to be a **real
parameter** — the public functions above pass it through to a private
`@st.cache_data` implementation.

Frames come back as Polars, converting at the store boundary per `CLAUDE.md`, even
though `clean_lineups` upstream is Pandas.

`is_stale` returns True when `built_at` is missing or unparseable. An unreadable
build time is a reason to warn, not a reason to claim freshness.

## Steps

1. **Graceful missing-source degradation** in `Scripts/projection_utils.py`.
   Not in the original plan; it turned out to be a prerequisite — see the
   postscript.
2. `Scripts/paths.py` + `Scripts/store.py` — `store_dir()`, atomic
   `write_league_store()` / `read_league_store()`, and the `meta.json` schema.
3. `Scripts/refresh.py` — CLI over `equivalence.build_league_frame()`. Plus
   `config_utils.resolve_league()`, replacing the display-name-or-key lookup that
   had been written inline in `equivalence.py` and `season_projections.py`.
4. `app/main.py` + `app/store.py` + header component — league picker and
   freshness badge, one placeholder page.
5. Fix the `FA_*` regression in `populateGoogleSheet.py`, then point it at the
   store — `run()` turned out to hold a duplicate of `build_league_frame`, so
   this stopped being deferrable. See [plan 14](14-thin-google-sheets.md).
6. `Data/Store/` in `.gitignore`, `streamlit` in `requirements.txt`, and
   `streamlit run` documented in the README and the weekly runbook.
7. Tests: `tests/test_store.py`, `tests/test_refresh.py`, plus additions to
   `test_projection_utils.py` and `test_paths_and_config.py`.

## Verification

- `python -m Scripts.refresh --league Knights_FFL` writes a store in ~8s
  pre-season, and `lineups.parquet` matches `clean_lineups` output exactly.
- Page navigation and week switching are visibly instant (<100ms).
- Stopping your network and reloading the app still renders — proving nothing in
  the render path touches ESPN.
- The freshness badge correctly reports a store built an hour ago as stale.

---

## Postscript — what implementation turned up

Written after the fact, 2026-08-05. Five things the plan did not anticipate.

### 1. The store could not be built at all (prerequisite, not polish)

`clean_lineups` hard-failed pre-season:

```
FileNotFoundError: Data/Projections/Pinnacle/Season/2026/Pinnacle_Props_Week_All.parquet
```

`clean_pinny` and `clean_bol` read the season's weekly props unconditionally, and
those files do not exist until the season starts. August, pre-draft, is exactly
the condition the app launches in — so this blocked everything.

The fix was small because the surrounding machinery already handles an absent
source correctly: `impute_columns` **creates** a target column from `MEAN_` when
it does not exist and flags every cell imputed; `compute_weighted_stats` drops
imputed weight and renormalises; `_apply_scoring` writes `0.0` for a prefix with
no columns. Only the unguarded `read_parquet` was missing. Both loaders now return
an empty join-key frame and warn (`MissingProjectionSourceWarning`, forced past
the process-wide `filterwarnings("ignore")` the way `Scripts.scoring` does), and
`get_match_details` returns early rather than `KeyError`-ing on the absent check
column.

Measured result on Knights_FFL 2026: a 235 × 469 frame where `TRUE_Points` is an
honest ESPN/FantasyPros blend, with Pinnacle and BetOnline reporting 0% real
coverage. Behaviour is unchanged when the files exist.

An explicit `pinny_path`/`bol_path` that is missing still raises — a named file
that is not there is a typo, not an absent season.

### 2. Parquet does not round-trip the frame exactly

Two distinct problems, both found by asserting `DataFrame.equals` rather than
eyeballing shapes:

- **`eligiblePositions`** is a Python list per row. pyarrow writes it as a parquet
  list and reads it back as a `numpy.ndarray` — value-identical, not
  `equals`-identical, and it breaks any caller expecting a list.
  `read_league_store` converts them back.
- **A phantom column.** `clean_lineups` builds its output with `pd.concat`, so the
  index is a duplicated integer index, and pandas serialises a non-default index
  as `__index_level_0__`. Polars then read it as a real column — the app reported
  **470 columns for a 469-column frame** until the index was dropped on write.

With both fixed, `df.equals(read_league_store(...))` is True for the real frame.

### 3. `season_dir()` created directories as a side effect of a *lookup*

It calls `mkdir(parents=True)` unconditionally, which is right for the scrapers
that write through it and wrong for the read-only path helpers. Asking whether
Pinnacle had a file for a season created an empty directory for that season —
three `2999/` directories appeared under `Data/Projections` from a test that only
called `.exists()`. `season_dir` now takes `create=`, and the six read-only call
sites in `projection_utils.py` and `season_projections.py` pass `False`.

### 4. `coverage_report()` raised on a frame with no source columns

`rows = []` → `pd.DataFrame([])` has no columns → `sort_values(["real_pct", ...])`
raises `KeyError: 'real_pct'`. Real `clean_lineups` output always has source
columns so this never fired in the weekly pipeline, but it meant a store write
could be taken down by the metadata it was only annotating. It now returns an
empty frame with the declared columns.

### 5. All eight `FA_*` Sheet tabs were failing silently

`write_to_google` read `lg_vars[select_league]['primary_own']`, where
`select_league` was a module-level global assigned by a top-level loop. Commit
`304ba39` moved that loop into `run()`, making it a local — so every `FA_*` tab
raised `NameError`, which the bare `except` reported as *"Position Does Not Exist
in League"*. Verified against the pre-refactor file: `select_league = l` was at
module scope. The owner is now a parameter, and the `except` names the real cause.

`304ba39` is dated 2026-08-05, the same day this was found, so **no published
Sheet was affected** — the 2025 season ran on the pre-refactor code. The finding
is not that Sheets was broken all year; it is that a total failure of eight of
ten tabs was indistinguishable from "this league has no kicker", and the weekly
run would have exited 0.

Reading that file also turned up the duplicate ingest in `run()`, which is why
step 4 stopped being deferrable — see [plan 14](14-thin-google-sheets.md).

Pointing `populateGoogleSheet.py` at the store is still outstanding, deliberately:
Sheets keeps working untouched while the app proves itself. Doing it would make
the weekly Sheets run depend on a store having been refreshed first.
