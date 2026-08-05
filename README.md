# ESPN Fantasy Football Analytics

Pulls league data from the ESPN Fantasy API for nine leagues, blends four
independent projection sources into each league's **own** scoring settings, and
publishes weekly lineup and free-agent boards to Google Sheets.

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
service account's email.

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
  config_utils.py            # config.yaml loader -> lg_vars
  nfl_utils.py               # schedule; current season + week
  fetch_utils.py             # fetch_league(): the single ESPN entry point
  scrape_player_stats.py     # ESPN player stats + build_scoring_table()
  scrape_team_stats.py       # ESPN team/matchup history
  scrape_FP.py               # FantasyPros projections
  scrape_pinnacle.py         # Pinnacle props (Selenium)
  scrape_BOL.py              # BetOnline weekly props  [BROKEN - see docs]
  projection_utils.py        # the projection blend pipeline
  analytic_utils.py          # lineup efficiency, records, SOS
  luck_index.py              # 7-factor weekly luck index
  simulation_utils.py        # Monte Carlo season sim + playoff odds
  tidbit_utils.py            # report formatters
  draft_utils.py             # [DEAD - being rewritten, see docs]
R/
  GetNFL.R                   # schedule + season stats via nflfastR
  GetSeasonProps.R           # BetOnline season-long futures
Data/
  NFL/<season>/              # season player stats
  NFL_Schedules.csv          # current season schedule (drives season + week)
  Projections/<source>/Season/<season>/
  Projections/<source>/Landing/<season>/
```

Data paths are **season-scoped**. Before 2026 they were not, so a new season's
scrape merged into the previous season's files.

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
python populateGoogleSheet.py   # blend + publish
```

## Leagues

Nine leagues across five owners, ranging from 6 to 16 teams, including one IDP
league (GOP Degenerates) and one superflex (Weenieless Wanderers). Configured in
`config.yaml`; see `display_name` for the key used throughout the pipeline,
which must match the Google Sheet name exactly.
