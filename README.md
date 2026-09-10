# ESPN Fantasy Football Analytics

Pulls league data from the ESPN Fantasy API for nine leagues, blends six
independent projection sources into each league's **own** scoring settings, and
publishes weekly lineup and free-agent boards to a local app and to Google
Sheets.

Everything is league-agnostic: scoring rules and roster slots are read from each
league via the API, so the same code handles a 6-team standard league, a 16-team
IDP league, and a superflex league without special-casing.

- **[docs/STATE_OF_THE_REPO.md](docs/STATE_OF_THE_REPO.md)** — what works, what
  is broken, and the prioritised backlog. Start here.
- **[docs/projection_pipeline.html](docs/projection_pipeline.html)** — how the blend
  works: stat lines in, one equal vote per source with an opinion, `TRUE_Points` out.
  The narrative. Hand-written, and its figures are stamped 2026-08-24.
- **[docs/projection_sources.html](docs/projection_sources.html)** — what each source
  *is*: how it is fetched, whether it is available at the draft or week to week, what
  it depends on, and which way it is biased by stat and position. Generated from the
  live board by `python -m Scripts.lab.sources`; open either in a browser, no build
  step.
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
  routes/                    # one file per tab, registered with st.Page
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
python -m Scripts.refresh --all --what board      # ten draft boards, ~16s
python -m Scripts.refresh --all --what draft      # pick history + owner tendencies, ~10s
```

The board is **league-aware**, which is the whole point of building one: replacement
level comes from each league's real starting slots, so the same player is ranked
differently across your ten. Josh Allen is VOR rank 9 in the 10-team superflex and
21 in 14-team Knights_FFL, because a superflex `OP` slot pushes QB replacement from
QB14 to QB20.

### Five tabs

**Home**, **Roster**, **Matchup**, **Free Agents**, **Draft** — Home first because it
is the question you actually arrive with, then a single week in the order you work it,
then Draft, which for all but one weekend of the year is history. The **League** and
**Week** selectors sit in the sidebar, under the identity block, and govern the other
four; they are *drawn from the entrypoint* rather than from the pages, which is what
removes a Streamlit behaviour that had twice rendered the wrong league silently. The
season is **pinned, not selected**: every tab answers a question about the season in
progress, and a control nobody moves is one that eventually gets moved by accident. See
[plan 40](docs/plans/40-frontend-restructure.md).

**Home** is the landing page, and the only tab that is not about the selected league.
It draws one card per league you are in — record and rank, who you play, the win
probability, and the decisions each team needs — ordered worst first, so the page reads
in the order you should act. A card's highlighted button opens the tab that can act on
it *and* points the League selector at that league, which is the whole ergonomic gain:
finding the league with a ruled-out starter in it used to mean opening all five. Every
number on a card is computed by the same function the deep tab computes it with, so a
card cannot disagree with the page it sends you to. Underneath, one standings table per
league carries the record over **weeks that were actually played** — ESPN reports an
unplayed fixture as a 0-0 tie, and believing it had every team reading 0-0-1 before the
season started — beside what this week has scored and what it projects. Ranking is win
percentage, then points for, then the projection, which breaks a tie in the first two
and before week 1 is the only thing separating anybody.

The sidebar's freshness line says **whether the nightly build ran**, not whether you
should refresh. It is a plain caption inside 25 hours and an error past it, because the
store is rebuilt once a night: the old one-hour threshold painted a red badge over a
6am build for the rest of the day, every day, with nothing wrong — and an alarm that
fires daily is one nobody reads. The refresh button is directly beneath it for the
case the short threshold was really about.

**Draft** holds six sub-tabs over one board read — the two you drive a draft from first:

**Board** is the working surface and 45 columns, and it is where you go *before* a
draft to decide whether you believe the numbers. **Board** is the working surface — player
search, filters for position, NFL team and bye week, an auction budget, and the table,
sorted by VOR. **Values** is where the room and our valuation disagree. **League** is
what does not change during a draft: the positional cliff, the tier runway, and who you
are drafting against. **Calibration** is where *we* disagree with ESPN, and whether
that disagreement is a player or the model.

**Sheet** is the on-the-clock view: four position panels in a 2x2 grid, banded by
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
because it varies: GOP Degenerates plays for $250 and the others for $200. It is set on
the Board sub-tab and the Sheet reads the same frame, so the two cannot drift.

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

**Rundown** appears once a league has drafted, and grades every roster in it under
**three independent bases side by side** — ESPN's projections, The Athletic's, and ours
— rather than averaging them into a verdict. The headline is the best *legal starting
lineup*, because a season is scored out of one and four good quarterbacks are one good
quarterback. A letter grade sits beside each rank as a pure rescaling of the percentile,
labelled as such: it adds no evidence and cannot disagree with the rank it comes from.
The row worth reading is the widest disagreement between the three, because a roster
ESPN likes and The Athletic does not is a roster whose value rests on a claim you can go
and check.

The board a rundown grades against is **frozen** once the drafts finish
(`python -m Scripts.freeze --all`), because `board.parquet` rebuilds every morning and
grading a September draft against a December board measures who got lucky. The tab says
which basis it used, every time. See [plan 41](docs/plans/41-projection-freeze.md).

### Roster, Free Agents, Matchup

**Roster** is the Sunday-morning tab: the lineup ESPN has set beside the best one
available, and **the specific swaps between them**. Knowing you left 19 points on the
table does not tell you to start Trevor Lawrence over Philip Rivers, so the swaps are
the deliverable and the efficiency score is not. The gains sum to the difference between
the two lineups exactly. Players on bye or ruled out are excluded from the optimum and
flagged loudly if one is in your starting lineup.

Slots are read from **what is actually being started** rather than from
`meta["starting_slots"]`, which is written when a board is built and can be a month out
of step with the lineups it would be applied to — GOP's metadata says its only
defensive slot is `DP`, while its lineups hold players at `CB`, `DE`, `DT`, `LB` and `S`.

**Free Agents** is the eight `FA_*` Sheets tabs collapsed into one filter. The pool is
not a separate artifact: `lineups.parquet` carries every unrostered player as extra rows
on a synthetic team called `Free Agent`, which is why this tab and Roster agree about
what a player projects. **Add/drop is scored as one decision** — the difference between
two optimal lineups — because a receiver who out-projects your worst bench player by
four points is worth nothing if he still would not start. The common answer is "none of
these would improve your lineup", and it is the answer worth trusting.

**Matchup** puts a **calibrated win probability** on the week. A projected margin is not
a decision: six points on a lineup whose weekly standard deviation is 23 is barely an
edge. The dispersion is fitted per position on 15,989 started player-weeks from 2025 and
**back-tested against four pre-committed gates** before the number was allowed on
screen — team-level interval coverage 0.802 against a nominal 0.800, every predicted
decile within 2.8pp of its realised win rate, Brier 0.2277 against 0.2500. If those
fail, the tab shows the margin and says the probability did not calibrate; a
confident-looking 63% that is really a coin flip is worse than no number.

The payoff is reading a lineup change in probability rather than in points. On
Weenieless week 1 — measured before that league was disconnected — fixing the lineup is
+19.6 projected points and **+21.8pp** of win probability, 53% to 75%. On Knights the same tab reports +1.6 points and +1.4pp. Same
week, two very different reasons to care. See
[plan 42](docs/plans/42-weekly-matchup-odds.md).

Under the headline the whole fixture is **one mirrored table**. Both sides carry the
same columns — every source's number, our blend, and `Δ` against ESPN's own
projection — and the away half draws them in reverse, so the two lineups read outward
from a shared `SLOT` column in the middle. Between them sit the two `ADV` columns:
that side's points at that slot minus the other's, filled continuously from red to
green, summing to the projected margin. Rows are a **max** over both sides and the
league's own slot definition, so a slot one manager left empty leaves a gap rather
than shifting his RB2 opposite the opponent's flex.

Both tables are hand-emitted HTML rather than `st.dataframe`, because Streamlit's grid
merges no cells and stacks no third header row — and because sorting a matchup table
by one column would leave a quarterback opposite a kicker.

**Roster is the same table without the mirroring, and it is a decision table rather
than a comparison**, so it differs in three deliberate ways. `TRUE` and `Δ` come
*first*, beside the player's name, because the blend is the number the lineup is
chosen on. The changes are marked in the lineup rather than listed beside it: **green
`IN`** for a player to start who is currently benched, **red `OUT`** for one currently
starting who the best lineup drops, drawn directly under the man taking his place so a
swap reads as one decision. And the bench sits **below** the `TOTAL` row, because the
total is the boundary — everything above it counts, everything under it does not.

A starter dropped with *nobody* to replace him — a kicker on bye with no cover, which
is five of the 114 real 2026 team-weeks — is marked too. There is no arrival to pair
him with, so `swaps` has nothing to say about him, and he is the change you most want
flagged; `lineup.changed_ids` reports him anyway. Every mark is **printed as well as
coloured**, the same rule the draft board's `Δ` columns follow.

The Roster table also carries `Sources` and `Spread` — how many sources really had an
opinion, and how far apart they were. The matchup table does not: that answers a
question about your own bench, and across a fixture it says nothing about whether your
receiver beats theirs. Its `TOTAL` row **does not sum the two of them**: `Sources` is
averaged, since ten starters on one source each is not a well-corroborated lineup, and
`Spread` is composed as `√Σspread²`, which is what independent disagreements add up to.

Matchup needs the `team_stats` artifact for the fixture list, which is opt-in:

```bash
python -m Scripts.refresh --all --what lineups,team_stats
```

### Who the app is for

The picker offers **your** leagues, not all nine. `config.yaml` holds nine across
five owners and the app scopes them through `app/auth.py`, which defaults to
Winfield_Football. Adding a league takes **two** edits, not one — `config.yaml` *and*
`DEFAULT_VIEWER.leagues` in that module; `jeffs_league` was configured, refreshed and
published on 2026-09-01 and stayed invisible until the tuple changed. **Removing one is
the same two edits**, and deliberately not a third: `weenieless_wanderers` came out on
2026-09-09 with its parquet left in S3, and because `store.list_leagues` reads store
prefixes rather than the config, that data is still reachable — but only under
`ESPN_FFL_ALL_LEAGUES=1`. There is **no login yet** — that module is the seam one lands in,
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

**ESPN reads 100% there by construction** — it is the frame every row came from,
and the other sources are read against it. It used to read 92–99.5% depending on
the league, because ESPN publishes `0.0` for an inactive or bye player and the
"real line" rule counted that as absence. Its zero is an assertion; a book's zero
on a stat it never posts is not, so only the root source counts one.

Pre-season the weekly Pinnacle and BetOnline props do not exist yet. The blend
falls back to the ESPN/FantasyPros mean for those columns, flags them imputed,
and renormalises them out of `TRUE_*` — so the numbers stay honest and the app
says so in the sidebar.

---

## How the projection blend works

Five external sources — **ESPN**, **FantasyPros**, **The Athletic**, **BetOnline** and
**Pinnacle** — on **both** grains since 2026-09-09, when The Athletic began publishing a
weekly slate and joined the weekly blend it had been absent from
(`docs/plans/47-athletic-weekly.md`). Before that the draft board had five votes and the
weekly board four.

**TOMCAT**, our own model, was the sixth from 2026-08-17 until **2026-09-07, when it was
withdrawn from the season-long blend.** It projects every position through three
backends — a usage arm for QB/RB/WR/TE, a defence arm for D/ST, a kicking arm for K —
and it was not removed for being uninformative: it beats the naive draft heuristic out
of sample at every position and is the most independent source ever registered here. It
was removed on a **level** error. Its projected league runs the ball **384 times per
team against a realised 450–465**, so every runner carried roughly a 10% haircut that
had nothing to do with the player, and nothing in the pipeline corrected it — the team
accounting identities are all passing↔receiving and there is no rushing pair. The full
argument, and what it costs, is `docs/plans/43-tomcat-out-of-season-blend.md`.

The model still runs and its `USG_` stat lines are still written to the board,
unweighted and unpriced, so it can keep being measured and the decision is one line to
reverse. Nothing about that withdrawal touches the weekly path: TOMCAT has never had a
weekly head, and the weekly blend's fifth vote is The Athletic's rather than the
model's.

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
`Scripts/projection_utils.py`, where **a source at 0.0 is dormant and a source that is
absent has been withdrawn** — TOMCAT is absent.

**A source that cannot be right is withdrawn before the vote.** Equal votes have one
failure mode: when four sources correctly abstain on a player nobody can start, the
fifth becomes 100% of the projection. Jayden Higgins went on injured reserve for the
season, ESPN priced him at 0.0, FantasyPros and Pinnacle dropped him and TOMCAT (then
still voting) was withdrawn — and he still read 36.3 points, because BetOnline was still
posting a 575-yard
season prop and a book does not take its market down. So three gates in
`_withdraw_sources_on_availability` pull every non-ESPN source where the player is out
for the season, or ESPN prices him at zero and he is out, or ESPN prices him at zero and
only one source still had a line. Two sources agreeing against an ESPN zero are left
alone — that is a disagreement, and a board that cannot disagree with ESPN is not worth
building. The board's **Withdrawn** column says which gate fired.

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

`python -m Scripts.name_audit` answers the neighbouring question: a source that is
fresh, present, and joined to *nobody*. Every source is matched to ESPN by name, and
a name that fails to match does not fail loudly — the player abstains, the blend
renormalises, and the number still looks complete. The audit classifies each miss so
the handful that are spelling differences separate from the hundreds that are simply
players outside the league. It exits non-zero when anything needs a fix, reads only
the built stores, and needs no ESPN connection. Run it after a source's first scrape
of the season and whenever a book's file changes shape.

**Why both outputs exist.** The app is a service — fast, rich, and alive only
while your laptop is. The Sheet is a published artifact: readable from a phone,
away from home, with the laptop shut. That is a real capability the app cannot
have, and five of the eight published leagues belong to other owners for whom the
Sheet is their only access. See
[plan 14](docs/plans/14-thin-google-sheets.md).

## Leagues

Nine leagues across five owners, ranging from 6 to 16 teams, including one IDP
league (GOP Degenerates) and one superflex (Jeffs_League).
Configured in `config.yaml`; see `display_name` for the key used throughout the
pipeline, which must match the Google Sheet name exactly. Seven are published to
Google Sheets and four are the app viewer's own — the three counts differ on purpose,
and `populateGoogleSheet.py` and `app/auth.py` are where the other two live.

A tenth, `Weenieless_Wanderers`, was **disconnected on 2026-09-09**: out of
`config.yaml`, out of `DEFAULT_VIEWER.leagues`, and off both Google Sheet cohorts, with
its 2025 and 2026 parquet deliberately left in S3. The pipeline no longer fetches it and
the app no longer offers it; its existing Sheet tabs stop being refreshed but are not
deleted. `ESPN_FFL_ALL_LEAGUES=1` is the only way back to the kept data.
