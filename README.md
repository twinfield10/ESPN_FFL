# ESPN Fantasy Football Analytics

Pulls league data from the ESPN Fantasy API for nine leagues, blends four
independent projection sources into each league's **own** scoring settings, and
publishes weekly lineup and free-agent boards to a local app and to Google
Sheets.

Everything is league-agnostic: scoring rules and roster slots are read from each
league via the API, so the same code handles a 6-team standard league, a 16-team
IDP league, and a superflex league without special-casing.

- **[docs/STATE_OF_THE_REPO.md](docs/STATE_OF_THE_REPO.md)** — what works, what
  is broken, and the prioritised backlog. Start here.
- **[docs/SEASON_ROLLOVER.md](docs/SEASON_ROLLOVER.md)** — the weekly and annual
  runbooks.
- **[docs/plans/](docs/plans/)** — small, self-contained upgrade plans.

---

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp config.example.yaml config.yaml   # then fill in your ESPN cookies
```

`config.yaml` holds live ESPN session cookies and is gitignored — never commit
it. `config.example.yaml` documents how to obtain each value.

Google Sheets output additionally needs a GCP service-account key at
`gs4creds.json` (also gitignored), with the target spreadsheets shared to the
service account's email. Skip it if you only want the local app — the
`gspread`/`oauth2client` dependencies are then optional too.

The R scripts need `tidyverse`, `nflfastR`, and `nflreadr`.

**Run everything from the repo root.** Modules import as `Scripts.<name>`, so
scrapers are invoked with `-m`:

```bash
python -m Scripts.scrape_FP        # not: python Scripts/scrape_FP.py
```

Tests need no network or credentials:

```bash
pytest
```

---

## Layout

```
config.yaml                  # leagues + credentials (gitignored)
populateGoogleSheet.py       # weekly entry point -> Google Sheets
FF Analysis Notebook.ipynb   # interactive analysis
Scripts/
  paths.py                   # repo-root-relative, season-scoped path helpers
  config_utils.py            # config.yaml loader -> lg_vars, resolve_league()
  nfl_utils.py               # schedule; current season + week
  fetch_utils.py             # fetch_league(): the single ESPN entry point
  scrape_player_stats.py     # ESPN player stats + build_scoring_table()
  scrape_team_stats.py       # ESPN team/matchup history
  scrape_FP.py               # FantasyPros projections
  scrape_pinnacle.py         # Pinnacle props (Selenium)
  scrape_BOL.py              # BetOnline weekly props  [BROKEN - see docs]
  projection_utils.py        # the projection blend pipeline
  scoring.py                 # the scoring registry (one source of truth)
  equivalence.py             # build_league_frame(): the single ingest path
  refresh.py                 # builds the store  <- the app's only writer
  store.py                   # the store's read/write contract
  crosswalk.py               # gsis_id <-> espn_id <-> fantasypros_id
  draft/adp.py               # ADP, auction values, ESPN season projections
  draft/board.py             # replacement level, VOR, tiers, value
  analytic_utils.py          # lineup efficiency, records, SOS
  luck_index.py              # 7-factor weekly luck index
  simulation_utils.py        # Monte Carlo season sim + playoff odds
  tidbit_utils.py            # report formatters
app/                         # local Streamlit app; reads the store, never ESPN
  main.py                    # entry: st.navigation
  store.py                   # cached, Polars-native reads + staleness
  components/header.py       # league/season/week picker, freshness, refresh
  pages/                     # one file per view
R/
  GetNFL.R                   # schedule + season stats via nflfastR
  GetPlayerIDs.R             # the cross-provider player id table
  GetSeasonProps.R           # BetOnline season-long futures
Data/
  NFL/<season>/              # season player stats
  NFL_Schedules.csv          # current season schedule (drives season + week)
  Projections/<source>/Season/<season>/
  Projections/<source>/Landing/<season>/
  Scoring/scoring.csv        # the scoring registry (committed)
  NFL/player_ids.parquet     # the id crosswalk (committed)
  Store/<season>/<league>/   # app data store (gitignored, regenerable)
                             #   lineups.parquet, board.parquet, meta.json
```

Data paths are **season-scoped**. Before 2026 they were not, so a new season's
scrape merged into the previous season's files.

---

## Local app

The app reads `Data/Store` and nothing else — no page talks to ESPN. That
separation is not stylistic: rebuilding one league's blended frame is ~8s
pre-season and rises toward ~23s with a full season of box scores, against 11ms
to read the same frame back from parquet. A UI that recomputed on a dropdown
change would be unusable, so refresh is an explicit step.

```bash
python -m Scripts.refresh --all      # build the store (slow, hits ESPN)
streamlit run app/main.py            # read it (fast, offline-safe)
```

Refresh options:

```bash
python -m Scripts.refresh --league Knights_FFL
python -m Scripts.refresh --league Knights_FFL --season 2025
python -m Scripts.refresh --all --what lineups,team_stats
```

`--what` defaults to `lineups`. `team_stats` is opt-in because it re-derives a
league's entire history — 2016-2026 for Winfield_Football — and nothing about
this week changes 2019. `board` is opt-in for the mirror-image reason: it is the
pre-season draft board, and nothing about week 9 changes your draft.

```bash
python -m Scripts.refresh --all --what board      # nine draft boards, ~16s
```

The board is **league-aware**, which is the whole point of building one: replacement
level comes from each league's real starting slots, so the same player is ranked
differently across your nine. Josh Allen is VOR rank 9 in the 10-team superflex and
21 in 14-team Knights_FFL, because a superflex `OP` slot pushes QB replacement from
QB14 to QB20.

The sidebar shows when the store was built, turns red past an hour, and lists
per-source projection coverage so a dead source is visible rather than absorbed
by imputation. Its refresh button shells out to the same CLI; a league that fails
keeps its previous store, so the badge shows an older time rather than nothing.

Pre-season the weekly Pinnacle and BetOnline props do not exist yet. The blend
falls back to the ESPN/FantasyPros mean for those columns, flags them imputed,
and renormalises them out of `TRUE_*` — so the numbers stay honest and the app
says so in the sidebar.

---

## How the projection blend works

```
ESPN stats ─┐
FantasyPros ─┼─► impute gaps ─► MEAN ─► weighted blend ─► TRUE_* stat line
Pinnacle ───┤                                                    │
BetOnline ──┘                                                    ▼
                                              proj_to_score(league)
                                                  │
                                   build_scoring_table() turns that
                                   league's ESPN scoring settings into
                                   per-stat point values
                                                  ▼
                                            TRUE_Points
```

Sources are blended per stat, not per player — `passingTouchdowns` leans on the
sportsbooks, `passingYards` leans on FantasyPros. Weights live in
`clean_lineups()` in `Scripts/projection_utils.py`.

The key idea is that projections are produced as **stat lines**, then scored
through each league's own rules. That is what makes one pipeline serve nine
leagues with different scoring.

---

## Weekly run

See [docs/SEASON_ROLLOVER.md](docs/SEASON_ROLLOVER.md) for the full runbook.

```bash
Rscript R/GetNFL.R              # refresh schedule + stats
python -m Scripts.scrape_FP     # FantasyPros
python -m Scripts.scrape_pinnacle
python -m Scripts.refresh --all # build the store, once
python populateGoogleSheet.py   # render it to Sheets
```

`refresh` must come first: `populateGoogleSheet.py` reads the store rather than
ESPN, so the two outputs cannot disagree.

**Why both outputs exist.** The app is a service — fast, rich, and alive only
while your laptop is. The Sheet is a published artifact: readable from a phone,
away from home, with the laptop shut. That is a real capability the app cannot
have, and five of the eight published leagues belong to other owners for whom the
Sheet is their only access. See
[plan 14](docs/plans/14-thin-google-sheets.md).

## Leagues

Nine leagues across five owners, ranging from 6 to 16 teams, including one IDP
league (GOP Degenerates) and one superflex (Weenieless Wanderers). Configured in
`config.yaml`; see `display_name` for the key used throughout the pipeline,
which must match the Google Sheet name exactly.
