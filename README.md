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
- **[docs/projection_pipeline.html](docs/projection_pipeline.html)** — every source,
  how they blend into `TRUE_Points`, and what the app shows. A living document;
  open it in a browser, no build step.
- **[docs/DRAFT_READINESS.md](docs/DRAFT_READINESS.md)** — the 2026 draft countdown:
  the dates ESPN has, what is verified working, and what to do on which day.
  Temporary — retire it after the last draft.
- **[docs/DATA_CATALOGUE.md](docs/DATA_CATALOGUE.md)** — what every dataset *is*: the
  grain of a row, what the columns mean, and **how the tiers join**. For how much of
  it there is right now, `python -m Scripts.catalogue`.
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
aws configure                        # then check: aws sts get-caller-identity
```

`config.yaml` holds live ESPN session cookies and is gitignored — never commit
it. `config.example.yaml` documents how to obtain each value.

**AWS credentials are not optional for the app.** S3 is the system of record and
the app reads it *by default*, so a fresh clone with no `~/.aws/credentials`
fails at the first store read with `Unable to locate credentials`. Any of the
standard boto3 sources work — `aws configure`, an `AWS_PROFILE`, or
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` in the environment; there is no
project-specific variable and nothing is read from `config.yaml`. The bucket is
`espn-ffl-data` in `us-east-2`, and the principal needs read plus
`s3:PutObject` under it to run the nightly push.

To work without AWS at all, run `ESPN_FFL_STORE_SOURCE=local` against a `Data/`
you already have — but note a fresh clone's `Data/` is *empty*, since nothing
under it is tracked in git, and populating it is itself a `--pull` from S3. So
local-only is an offline escape hatch for a machine that has synced once, not a
way to skip the setup.

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
  store.py                   # the local store's read/write contract
  s3_store.py                # the S3 boundary: keys, checksums, ETag-cached reads
  sync.py                    # --push / --pull / --verify between disk and S3
  catalogue.py               # what data do we have? -> docs/DATA_CATALOGUE.md
  crosswalk.py               # gsis_id <-> espn_id <-> fantasypros_id
  draft/adp.py               # ADP, auction values, ESPN season projections
  draft/board.py             # replacement level, VOR, tiers, value
  draft/history.py           # every pick the league has ever made
  draft/tendencies.py        # what each manager does that the room does not
  analytic_utils.py          # lineup efficiency, records, SOS
  luck_index.py              # 7-factor weekly luck index
  simulation_utils.py        # Monte Carlo season sim + playoff odds
  tidbit_utils.py            # report formatters
app/                         # local Streamlit app; reads the store, never ESPN
  main.py                    # entry: st.navigation
  store.py                   # cached, Polars-native reads + staleness
  auth.py                    # who is looking, and which leagues they may open
  draft_view.py              # the draft board's derivations, testable without Streamlit
  components/header.py       # league/season/week picker, freshness, refresh
  pages/                     # one file per view
R/
  GetNFL.R                   # schedule + season stats via nflfastR
  GetPlayerIDs.R             # the cross-provider player id table
  GetSeasonProps.R           # BetOnline season-long futures
Data/                        # local: a writer's scratch pad + a read cache.
  NFL/<season>/              #   Not tracked in git -- S3 is the record.
  NFL_Schedules.csv          # current season schedule (drives season + week)
  Projections/<source>/Season/<season>/
  Projections/<source>/Landing/<season>/
  Scoring/scoring.csv        # the scoring registry
  NFL/player_ids.parquet     # the id crosswalk
  G2/<season>/               # the one thing that cannot be rebuilt
  Store/<season>/<league>/   #   lineups.parquet, board.parquet, meta.json
  .s3cache/                  # downloaded objects, keyed by ETag; safe to delete
```

Data paths are **season-scoped**. Before 2026 they were not, so a new season's
scrape merged into the previous season's files.

### The data lives in S3

`s3://espn-ffl-data` (`us-east-2`, versioned) is the system of record; `Data/` is
local scratch and is **not tracked in git**. Keys are Hive-partitioned so a query
engine can prune on them:

```
store/season=2026/league=knights_ffl/board.parquet        # what the app reads
snapshots/board/season=2026/league=knights_ffl/date=.../  # one board per night
archive/g2/season=2026/                                   # irreproducible
nfl/season=2026/  projections/  scoring/  injuries/       # inputs and cache
```

```bash
python -m Scripts.sync --push       # after any refresh; the nightly job does it
python -m Scripts.sync --verify     # SHA-256 both sides, exits 1 on a difference
python -m Scripts.sync --pull       # rebuild Data/ on a fresh machine
python -m Scripts.catalogue --both  # what is actually there, disk and bucket
```

The app reads S3 by default. `ESPN_FFL_STORE_SOURCE=local` reads disk instead
(offline, and the draft-morning escape hatch); `auto` prefers S3 and falls back.
See [plan 24](docs/plans/24-s3-data-flow.md).

---

## Local app

The app reads the store and nothing else — no page talks to ESPN. That separation is
not stylistic: rebuilding one league's blended frame is ~8s pre-season and rises
toward ~23s with a full season of box scores, against 11ms to read the same frame
back from parquet. A UI that recomputed on a dropdown change would be unusable, so
refresh is an explicit step.

```bash
python -m Scripts.refresh --all      # build the store (slow, hits ESPN)
python -m Scripts.sync --push        # publish it -- the app reads S3
streamlit run app/main.py            # read it (one list call, then cached)
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
python -m Scripts.refresh --all --what draft      # pick history + owner tendencies, ~10s
```

The board is **league-aware**, which is the whole point of building one: replacement
level comes from each league's real starting slots, so the same player is ranked
differently across your nine. Josh Allen is VOR rank 9 in the 10-team superflex and
21 in 14-team Knights_FFL, because a superflex `OP` slot pushes QB replacement from
QB14 to QB20.

There are **two pages over the same board**, because reading a board and drafting off
one are different jobs.

**Draft Board** is four tabs and 45 columns, and it is where you go *before* a draft to
decide whether you believe the numbers. **Board** is the working surface — player
search, filters for position, NFL team and bye week, an auction budget, and the table,
sorted by VOR. **Values** is where the room and our valuation disagree. **League** is
what does not change during a draft: the positional cliff, the tier runway, and who you
are drafting against. **Calibration** is where *we* disagree with ESPN, and whether
that disagreement is a player or the model.

**The Sheet** is the on-the-clock view: four position panels in a 2x2 grid, banded by
tier, seven columns each — `Tier · Player · TM/BYE · PTS · VALUE · PS · ADP`. Click a
row to cross a player off; click again to put him back. Its organisation is lifted from
`DraftSheets_2026.xlsx`, the BeerSheets replacement, which is a good draft-day interface
over a weak engine — so the layout is taken and the numbers are ours. See
[plan 37](docs/plans/37-draft-sheet.md), including the two column-drift bugs in the
workbook that made this a reimplementation rather than a port.

`PS` is the column worth learning. **Positional scarcity** is how much of that
position's value over replacement is still sitting *below* a player and undrafted —
high means plenty behind him and no urgency, low means the cliff is here. It decays as
you cross names off: RB1 on Knights runs from 90% to 25% as the twelve backs below him
go. Nothing else on either page answers "if I pass on him, what is actually left".

The **Availability** toggle discounts every projection by the games the model expects a
player to miss. It is **off by default on purpose**: the discount is real money (Puka
Nacua 339 → 275) and it reorders within position, but the availability head is the
weakest arm of the model that produces it — prior-season games predict next season at
r = +0.343. Look at it every time; do not assume it is the better number.

The auction budget matters more than it looks. ESPN publishes its market auction values
against **its own $200 budget**, so the `$` column was denominated in somebody else's
money — and until 2026-08-28 the fix was a straight `budget/200` rescale that never saw
**team count**, which put GOP's market total at $2,702 against the $4,000 actually on
the table while six-team Winfield read $2,083 against $1,200. Both sides of the cash
lens now go through one allocation — every roster spot reserves its $1, and what is
left is split in proportion to value — so our dollars and the market's sum to the same
pool and the difference between them means something. The budget is read from ESPN,
because it varies: GOP Degenerates plays for $250 and the other eight for $200. It is
set on the Draft Board page and The Sheet reads it, so the two cannot drift.

**Keeper leagues.** ESPN carries last season's rosters into a keeper league before
anyone declares, so GOP's board arrives with 252 players held against a keeper limit
of 2. A roster bigger than the limit cannot be a list of keepers, so the board treats
everyone as available and says so, and turns the filter back on by itself once
rosters shrink. `Keeper $` is what it costs the current holder to keep a player —
measured, not assumed: 130 of GOP's keeper prices are exactly their 2025 auction
bid, and a player claimed off waivers has no winning bid to record, so he keeps for
the $1 minimum. Being on a roster is what confers a price; only a free agent has none.

Every column on the board has a **Glossary** entry under the table — its source and
a one-line derivation — generated from the same list that builds the table, so the
two cannot drift.

**Two ways to measure value.** `ADP` is our VOR rank against the market's draft
position, which is the right comparison when a pick is a place in a queue. `Cash` is
our dollar valuation against ESPN's average auction price, which is the right one
when there is no queue, only a price. Auction leagues open on Cash.

### Who the app is for

The picker offers **your** leagues, not all nine. `config.yaml` holds nine across
five owners and the app scopes them through `app/auth.py`, which defaults to
Winfield_Football. There is **no login yet** — that module is the seam one lands in,
so identity arrives in one function rather than in every page. It is not a security
boundary; see [plan 26](docs/plans/26-user-accounts.md) for what the real thing
needs.

```bash
ESPN_FFL_ALL_LEAGUES=1 streamlit run app/main.py   # every configured league
```

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

Four external sources — **ESPN**, **FantasyPros**, **BetOnline** and **Pinnacle** —
plus **TOMCAT**, our own model, which covers every position through three backends: a
usage arm for QB/RB/WR/TE, a defence arm for D/ST, and a kicking arm for K.

**TOMCAT** is **T**ouches · **O**pportunity · **M**arket · **C**ontext ·
**A**vailability · **T**iers — its six feature families. The columns it writes are
still prefixed `USG_`; `Scripts/usage/__init__.py` says why the prefix did not move
with the name.

Each source is reduced to a **stat line**, never to points. The stat lines are blended,
and only then scored through each league's own rules. That ordering is what lets one
pipeline serve nine leagues with different scoring.

The weighting rule is **one equal vote per source that has an opinion**. Every source
carries the same nominal weight, a source with no real line for a player is flagged and
drops out, and the survivors renormalise — so four real sources weight 0.25 each, three
weight 0.333, two weight 0.5. Weights live in `WEIGHTS` in
`Scripts/projection_utils.py`.

**Full detail, with current coverage figures and the board's column map:
[`docs/projection_pipeline.html`](docs/projection_pipeline.html)** — open it in a
browser, no build step. This paragraph is the summary; that document is the reference,
and it is kept current.

---

## Weekly run

See [docs/SEASON_ROLLOVER.md](docs/SEASON_ROLLOVER.md) for the full runbook.

```bash
Rscript R/GetNFL.R                     # refresh schedule + stats
python -m Scripts.scrape_FP            # FantasyPros
python -m Scripts.scrape_pinnacle
python -m Scripts.scrape_espn_injuries # injury report + a dated snapshot
python -m Scripts.injury.review        # who needs a hand-written severity
python -m Scripts.refresh --all        # build the store, once
python populateGoogleSheet.py          # render it to Sheets
```

`Scripts.injury.review` is the one step that can ask something of you: it names the
players whose injury severity came off a news sentence rather than a published
diagnosis, and any correction goes in `config/injuries/<season>.yaml` **before**
`refresh`. Most weeks it names nobody worth writing down. Five minutes, and the
runbook has the decision rule.

`refresh` must come first: `populateGoogleSheet.py` reads the store rather than
ESPN, so the two outputs cannot disagree.

**Two jobs run themselves, so they are not in that list.** `run_daily_refresh.sh` at
06:00 pulls the season's projection sources and rebuilds the boards;
`run_odds_refresh_nfl.sh` at 07:00 pulls NFL sportsbook game lines into `Data/Odds/`
and stores only what moved, so line history accumulates for free.

`python -m Scripts.refresh_status` reports both, plus every projection source by name
and how old it is. That last part exists because it did not before: both books once
sat **thirteen days stale** on a live draft board while this reported everything
healthy — truthfully, in its own terms, since the nightly it watched was fine and
simply never ran them. A source is only as visible as something that names it.

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
