# 54 — Props and lines from the sportsbook engine, not our own scrapes

**Status:** IN PROGRESS

**Where it stands:** **Phase 1 built 2026-10-21.** `Scripts/books/engine.py` reads the
engine into a shadow every night (nightly stage 2g, non-fatal), and `--compare` scores it.
Measured the same day against the mini's own scrapes, through the same `project_book`:
**FanDuel's projections are identical** (median gap 0.0 on every stat, weeks 6 and 7),
Novig and 4Casters are within 0.01-0.4%, and **BetOnline fails G1**, because the
engine's widget walk captured yardage props for only 4 of week 7's 14 games (see
[Phase 1 findings](#phase-1-findings-2026-10-21)). **Bet105 went live in Pinnacle's
place on 2026-10-23**, and the Selenium Pinnacle scrape retired (see
[the Bet105 swap](#the-bet105-swap-2026-10-23)). **Phase 2 went live the same day for
FanDuel, Novig and 4Casters** ([phase 2, as built](#phase-2-as-built-2026-10-23)). It
also fixed a Novig rung-choice defect that our own scrape had been shipping. BetOnline
waits for phase 2b. The engine
(`~/GitRepos/SportsbookScrapers`, publishing to `s3://tmw-sportsbook-data`) already carries
every prop this repo scrapes for FanDuel, Novig, 4Casters and BetOnline, rung for rung and
usually wider. Its owner mapped this repo on 2026-10-16 in
`SportsbookScrapers/docs/transition/espn-ffl.md`. This plan checks that map against the
live data and turns it into the order of work here.

**What it is:** The nightly stops calling sportsbooks. A new adapter,
`Scripts/books/engine.py`, reads the engine's tables as of the nightly's start and writes
each book's raw frame in this repo's existing `RAW_SCHEMA` and `ODDS_SCHEMA`. Everything
downstream (`project_book`, `clean_lineups`, the `SHARP_`/`SQUARE_` pools,
`vegas.book_game_lines`, the D/ST and kicking models) keeps reading the shapes it reads
today. **The engine replaces the inputs to the pipeline, not the pipeline.**

---

## Why

- **The same books are being scraped twice on the same machine.** The engine runs on the
  Mac mini every minute (`sbs tick`, plus a DST walk four times a day). This repo's nightly
  then scrapes FanDuel, Novig, 4Casters and the BetOnline widget again at 06:00, and
  `run_odds_refresh_nfl.sh` scrapes game lines at 06:45. That's two copies of the
  credentials (`Rebirtha/.env`, `NHL-Data/.env`), two Playwright walks of the same widget,
  and two sets of parsers to keep up with every book's changes.
- **The engine is wider.** Week 7, Wednesday 16:06 UTC: FanDuel has 197 receivers in
  the bucket against 87 in our own scrape, and Novig 92 rushers against 37. It also has
  books we don't: Bovada, Kalshi and Polymarket.
- **It keeps every price change.** Plan 52's owed backtest needs archived prices that
  meet outcomes. The engine's changes-only history is that archive, from 2026-10-16 on,
  without this repo storing anything.

---

## What was measured, 2026-10-21

The bucket's NFL props for 2026-10-16 to 2026-10-21 (304,317 rows, 7 books) and game
lines (417,955 rows) were compared with the mini's own raw archives
(`Data/Projections/<Book>/Season/2026/Raw/`). The bucket was rebuilt as an **as-of
board**: the last row of each `(source_event_id, market_uid, player_key, side)` series
at or before the moment our scrape ran, `period = GAME`, `in_play` false. Rungs were
matched on player, stat, side and value.

**Week 6, the last pre-kickoff scrape (Sun 10/18 ~10:10 UTC):**

| Book | Our players, by stat | In the bucket too | Our rungs with no bucket match | Same price (two-way / ladder) |
|---|---|---|---|---|
| FanDuel | TD 382, rec 159, rush 80, pass 26 | every one | 23 of 5,116 | 99.7% / 90.8% |
| Novig | TD 363, rec yds 162, rush 81, pass 26 | all but 1 | 25 of 4,487 | 77% / 67% (median gap 0.004 decimal) |
| 4Casters | TD 100, rec 150, rush 75, pass 26 | all but 1–2 | 10 of 1,420 | 76% / 73% (median gap 0.002) |

**The ladder convention is exact.** For every matched ladder rung, the engine's `line`
plus 0.5 equals our `value` (100% for every book). That's the one-line adapter rule
SportsbookScrapers predicted (contract: "ladders over k − 0.5; ESPN_FFL stores
`value = k`").

**The bucket has more than we do.** Novig's passing attempts, completions, interceptions
and rushing attempts weren't in our week-6 Novig scrape at all, and the bucket has them for
all 26 QBs. 4Casters anytime TD covers 283 players in the bucket against our 100.

**BetOnline is in the bucket as `LowVig`,** with the two-way props from the LowVig
sportsbook and the ladders from the same DST widget this repo scrapes
(`docs/books/lowvig_dst.md`). Week 7: 4,072 of our 4,507 BetOnline rungs match, again
100% via k − 0.5. Prices agree less (50% identical, median gap 0.02) because of **timing,
not source**. The engine's widget walk runs at 01:52, 07:52, 13:52 and 19:52 ET, and our
copy was taken at 12:05. The 1,270 unmatched rungs are the same effect: rungs that
appeared after the 07:52 walk.

**Game lines are complete.** Weeks 6 and 7: spread, total and moneyline for every game at
8 books (4Casters, Bovada, DraftKings, FanDuel, Kalshi, LowVig, Novig, Polymarket), and
team totals at 6–7 of them. Week 6 shows 13 of 14 games because the Thursday game came
before the bucket's first day.

**`game_id` is never null.** It's the nflverse id, so the week join is the schedule
this repo already loads. Player names still need `align_to_espn_names`, but every row also
carries `player_id` (gsis), null on 0–2% of rows by book.

Reproduce: the scripts are in the 2026-10-21 session's scratchpad. Phase 1's shadow
report will replace them with `python -m Scripts.books.engine --compare`.

---

## What the engine doesn't have

| Missing | Used here for | Plan |
|---|---|---|
| **Pinnacle**, props and game lines | `PINNY_` in the `SQUARE_` pool, `TD_ALLOCATION_BOOKS`, D/ST book lines (`dst/books.py`), 0.25 of the season blend | Removed from the engine on purpose (owner, 2026-10-16). The open and close role goes to the engine's de-vigged market line. **Decision 1.** |
| **DraftKings props** | `DK_` in `SQUARE_`, even-money lines only, no TDs or ladders | DraftKings game lines are in the bucket, its props aren't. **Decision 2.** |
| **Season-long player props** (draft board) | `R/GetSeasonProps.R`, `scrape_pinnacle_season.py` | The engine's futures tables have no player-stat markets. These stay here, and they only run while the board is warm, so nothing changes until next August. |
| **Raw history before 2026-10-16** | `compare_props`, `Scripts/lab/market.py`, plan 52's backtest | Our `Raw/` archives stay readable. History is ours before 10/16 and the engine's after. |

**Contract mismatches found, filed 2026-10-21:** [#5](https://github.com/twinfield10/SportsbookScrapers/issues/5) DST coverage, [#6](https://github.com/twinfield10/SportsbookScrapers/issues/6)
`is_alternate`, [#7](https://github.com/twinfield10/SportsbookScrapers/issues/7) spells and `board()`, [#8](https://github.com/twinfield10/SportsbookScrapers/issues/8) reference key case.
- The contract says `reference/{sport}/...` with sport `NFL`, but the bucket has
  `reference/nfl/...` (lowercase). `reference/NFL/` is empty.
- Our BetOnline raw rows store ladder rungs with `type = "1"` (the widget's condition
  code), not `"Over"`. That's harmless today, but the adapter must write `"Over"`, and
  `project_stat` should be checked for depending on `"1"`.
- `is_alternate` is **false on every NFL prop row at every book**. The contract says
  it marks every rung but the book's main line.
- The contract says "was the market still up?" comes from the listing spells, but
  neither their location (`spells/`, `state/<book>/<sport>/spells.parquet`) nor their
  schema is in it, and `sportsbook.read.board()` doesn't apply them. Every consumer of
  `board()` gets the delisted series described under Phase 1 findings.

---

## The design

```
s3://tmw-sportsbook-data/lines/NFL/props/season=S/date=D/   (changes only, UTC)
s3://tmw-sportsbook-data/lines/NFL/odds/season=S/date=D/
        │  scan: season = S, date ≥ the week's first capture; check _contract.json major = 1
        ▼
as-of board at T (the nightly's start): last row per series, GAME, not in-play,
        │  and only series a listing spell covers at T (spells/ closed + state/ open)
        │
        ├─▶ props → RAW_SCHEMA per book  → Data/Projections/<Dir>/Season/S/Raw/<Dir>_Raw_Week_W_<T>.parquet
        │     book: LowVig→BetOnline, 4Casters→FourCasters, FanDuel, Novig (+ Bovada, Kalshi, Polymarket)
        │     market→espn_stat (passing_yards→passingYards, touchdowns→anytimeTouchdown, …)
        │     line→value (+0.5 on ladder rungs), American→decimal odds, impProb, prop_source
        │     game_id→NFL_game_id, week from the schedule
        │     → project_book (unchanged) → <Dir>_AllProps.parquet → clean_lineups (unchanged)
        │
        └─▶ odds → ODDS_SCHEMA → what read_current() returns today → vegas.* (unchanged)
```

- **One adapter, two outputs.** The props half writes the raw archive files
  `weekly_props.scrape` writes now, so `project_book`, `reconcile` and every reader after
  them run unmodified. The odds half sits behind `Scripts/books/store.read_current` with a
  `source="engine"` switch, so `vegas.py` doesn't learn a new schema.
- **`T` is a parameter.** The nightly passes its own start time. Phase 4's game-day
  re-blend passes "now". Backtests pass any moment since 2026-10-16. As-of is the only
  correct read of a changes-only table: the absence of a row means the price didn't move,
  not that the market closed.
- **Novig rung choice.** The engine prices every rung, and our adapter picks the most
  balanced rung (`novig_props.py:356-360`). The adapter keeps that rule so `NV_` means the
  same thing. Phase 1 confirms it.
- **Book lists come from the data** (the engine's transition rule 1). The new books are
  written to their own `Dir`s and prefixes, and their pool membership is a constant set by
  measurement (Decision 3), not by which scrapers exist.
- **Freshness gate.** If a book's last poll (the newest `last_seen` among its spells)
  is over 12 hours older than `T`, it's withheld for that run and reported, the way plan
  52 withdraws a dark book. This matters because a changes-only table never goes
  "empty": a dead collector looks like a quiet market.

---

## Phases and gates

**Phase 1: build the adapter, shadow only.** The nightly keeps scraping. After stage 2f, it
also runs the adapter into `Data/Engine/Shadow/<Dir>/Season/<s>/`, with the same file
names as the real books, and appends the run to `Data/Engine/Shadow/engine_shadow.json`:
each book's age at `T`, outcome, and the agreement table. Nothing reads the shadow, and
it's outside `Data/Projections/`, so `Scripts.sync` doesn't mirror it.
- **G1, coverage.** On every night of a full week, including one Sunday, the adapter's
  `(player, stat)` set covers ≥ 99% of the scrape's for FanDuel, Novig and 4Casters, and
  ≥ 95% for BetOnline. The BetOnline bar is lower because of the widget-walk timing.
- **G2, the stat lines.** Compare per book and per stat, before any points:
  `|proj_<stat>(engine) − proj_<stat>(scrape)|` median ≤ 2% of the stat's mean, on
  players projected ≥ 100 season points. The book under test isn't used to build the
  gate.
- **G3, the board.** `SHARP_` and `SQUARE_` per stat, then `TRUE_Points`, for the same
  population. Any player moving > 1 point is explained, by timing, a rung or a name,
  before the switch.

### Phase 1 findings (2026-10-21)

**The spell filter isn't optional.** On week 6's last pre-kickoff board, 511 of FanDuel's
7,262 series (7%) and 1,164 of Novig's 9,276 (12.5%) had been delisted. Unfiltered,
137 FanDuel player-stats show two main lines, because the line moved and nothing closed
the old series. Filtered, that number is **0**.

**`project_book` on the engine's raw vs our own raw, same moment** (median `|gap|` / mean,
players both have):

| Book | Week 6 (Sun 06:07 ET) | Week 7 (Wed 12:06 ET) | Coverage of our player-stats |
|---|---|---|---|
| FanDuel | 0.0 on every stat | 0.0 on every stat | 100% (wk 6), 93-100% (wk 7) |
| Novig | 0.2-0.8% | 0.01-0.2% | 97.5-100% |
| 4Casters | 0.01-0.6% | — | 93-99% |
| BetOnline | — | 0.05-1.1% where present | **39-62%** for yards and attempts, 100% for TDs |

**BetOnline's gap is the engine's DST walk, not the adapter.** Week 7's listed board has
LowVig receiving-yard props for 4 games (GB-DET, KC-SEA, NE-CHI, PIT-NO), while our walk
had players from 22 teams. Whole games are missing, so it's tile coverage in
SportsbookScrapers' `lowvig_dst` fetch, filed as [SportsbookScrapers#5](https://github.com/twinfield10/SportsbookScrapers/issues/5). BetOnline
stays our own scrape until G1 passes.

### The Bet105 swap (2026-10-23)

Pulled ahead of phase 2 for one book, because the Pinnacle weekly scrape was the only
Selenium/Chrome dependency left in the nightly, and Chrome is what hung it on 2026-10-21.
Pinnacle's game lines (`Scripts/books/pinnacle.py`) and season props
(`scrape_pinnacle_season.py`) use the guest API with no browser, so they stay.

**Measured 2026-10-22, week 7**, live board against that afternoon's Pinnacle scrape:

| Stat | Pinnacle players | Also in Bet105 | Median gap |
|---|---|---|---|
| Passing yds / att / cmp / TD | 28 | 27 | 0 to 0.018% |
| Receiving yds / receptions | 125 | 124 / 123 | 0 to 0.008% |
| Rushing yds / att | 60 / 5 | 59 / 5 | 0 to 0.007% |

The owed depth check came back the other way round: **Pinnacle posted no anytime
touchdown or interception that week**, and Bet105 had 18 and 2 players. So Bet105 is
held to Pinnacle's eight stats (`Book.stats`), and the swap is like for like. Widen
it when there's enough depth to measure a margin on.

**What changed:**
- `engine.Book` gained `live` and `stats`. A live book is written to
  `Data/Projections/<dir>/` (and its `Raw/`), where the blend reads it, not into the
  shadow. Bet105 is the only live book. Stage 2g is still non-fatal: a failed read
  costs the square pool one of five books for a night, as an empty Pinnacle scrape did.
- `B105` is in `NEW_WEEKLY_BOOKS`, `SQUARE_BOOKS`, `WEEKLY_PREFIXES` and the
  coverage lists. The app's SQUARE pool and `weekly_sources_present` answer to
  `bet105` rather than `pinnacle`.
- **`PINNY` stays in `SQUARE_BOOKS` (owner, 2026-10-22).** It carries its D/ST rows,
  priced off Pinnacle's guest-API game lines, until phase 3 moves game lines to the
  engine, and its prop lines for weeks 1-6. `drop_weeks_bet105_covers` drops
  Pinnacle's lines for any week Bet105 has, so the switch week isn't voted twice.
- `PINNY` stays in `TD_ALLOCATION_BOOKS` for the weeks it has. Bet105 isn't added,
  because it carries no touchdowns.
- Nightly stage 2b''' is removed, along with `Scripts/scrape_pinnacle.py`, its
  `prop_to_stat` test and `selenium` in `requirements.txt`. `refresh_status` watches
  `B105 weekly`, advisory as Pinnacle was.
- `NAME_ALIASES`: `KEVIN CONCEPCION -> KC CONCEPCION`.

**Gate, `winfield_football`, week 7** (2026-10-22 inputs pulled from the mirror,
`build_league_frame` run twice: A without the Bet105 file, so Pinnacle votes; B with
it). Of 243 week-7 players, **4 moved by more than 0.01 `TRUE_Points`, and the largest
move was 0.02** (George Pickens, whose Bet105 receiving line sits 1 yard off the
Pinnacle scrape, taken a few hours earlier). Weeks 1-6 are identical, because Pinnacle
still votes there. The `SQUARE_` medians moved a median 0.0003-0.07 per stat.

**Also fixed:** `engine._write_status` bound `STATUS_PATH` as a default argument, so
the test fixture's monkeypatch never reached it, and every pytest run appended fixture
runs to the real `Data/Engine/Shadow/engine_shadow.json` (22 on the MacBook). Clear
them on the mini as well: they're the runs stamped `2026-10-18T10:00:00+00:00`.

**Phase 2: switch the weekly props.** Stage 2e (`scrape_BOL`) and 2f
(`weekly_props --book all`) become `python -m Scripts.books.engine --week W` plus
`weekly_props --book draftkings` (Decision 2). `PINNY_` leaves the pools (Decision 1).
Non-fatal, like today's stages. Weekly freshness in `refresh_status` is read from the
engine's newest `captured_at` per book, which also fixes the existing gap where FanDuel,
DraftKings, Novig and 4Casters had no freshness check at all. *(As planned. What was
built differs; see below.)*

### Phase 2, as built (2026-10-23)

**FanDuel, Novig and 4Casters are live. BetOnline isn't (phase 2b).** For the three,
the engine's output has this repo's schema exactly, lands on the same
`Data/Projections/<Dir>/Season/<s>/<Dir>_AllProps.parquet` the scrapes wrote, and comes
from collectors that poll every minute. So decision 4 doesn't bind them: at 06:00 their
prices are minutes old. BetOnline differs on all three points (below), and decision 4
was really about its four-times-a-day widget walk.

**What changed:**
- `engine.BOOKS`: FanDuel, Novig and 4Casters are `live=True`, so `reconcile` merges
  each night's engine pull into the season file the blend has always read. Weeks 1-7
  keep their scraped rows; from here on the newest pull wins per player-week.
- Stage 2f is DraftKings only (decision 2), and the cross-repo `.env` reads for
  4Casters and Novig went with it. Stage 2e (BetOnline) is unchanged.
- `refresh_status` watches `FD weekly` and `NV weekly` (stale-making, because the two
  are the whole sharp pool) and `C4 weekly` (advisory). It reads file age rather than
  the engine's `captured_at`: a withheld book writes nothing, so a file that stops
  moving is exactly what a dead collector looks like from here.
- `clean_book_props` names the engine as the fix for a stale FanDuel, Novig or
  4Casters file.

**What a failed night costs:** less than a failed scrape did. A withheld book (no poll
for 12 hours) or a failed read writes nothing, so the season file keeps that week's last
pull and the blend votes yesterday's lines. Only a new week with no successful read is
empty, and then `SHARP_` abstains.

**A defect found on the way, in production since plan 52:** Novig is an exchange, and
stale resting orders leave crossed two-way quotes at far-off rungs. On 2026-10-22, 564 of
Novig's 1,518 two-way rungs had both sides favourites (Tucker Kraft at 19.5 receiving
yards: over -1567, under -2400, summing to 1.90). The most-balanced-rung rule rescales a
pair to sum to one before judging balance, so a crossed pair reads as perfect and wins.
**32 of 471 Novig lines** came out at about a third of FanDuel's. Our scrape
(`novig_props.py:356-360`) has the same rule and was shipping it on different players:
McMillan 24 receiving yards against FanDuel's 69.5, McCaffrey 14 against 44.5, every QB's
passing TDs at about half of FanDuel's and 4Casters'. `engine.to_raw` now chooses a main
line only from pairs inside `mk.OVERROUND_BOUNDS` (1.0-1.25). Healthy Novig pairs never
summed below 1.004. FanDuel, BetOnline, Bovada and Bet105 had no crossed pairs, 4Casters
had one (at 1.26), and Kalshi and Polymarket have the same exchange problem (20 and 13);
both are still shadow-only.

**Gate (G3), weeks 1-7, `winfield_football` and `knights_ffl`.** The engine was read
**as of 2026-10-22 18:05Z, the moment that day's scrapes ran**, so line movement is
excluded. `build_league_frame` ran with the scraped files (A) and with the engine's
reconciled in (B), and the files were restored afterwards.

| | winfield (243 wk-7 players) | knights (342) |
|---|---|---|
| `TRUE_Points` move > 0.1 / > 0.5 / > 1 | 16 / 2 / 0 | 22 / 2 / 0 |
| Largest move | 0.675 | 0.675 |
| Weeks 1-6 | identical | identical |

Every move over 0.3 is the engine correcting a crossed Novig line in the scrape:
McMillan +0.67, Coker +0.65, McCaffrey +0.47 and Sutton +0.42 (receiving yards), and Geno
Smith, McKee, Brissett, Mahomes and Watson +0.35 to +0.49 (passing TDs). The rest are
FanDuel identical, 4Casters a ladder rung's difference, and Novig's wider coverage.

**Not met: G1's full week.** The coverage gate asks for every night of a week including
a Sunday. This was built on one day's measurement, on the owner's call (2026-10-22). The
shadow log on the mini records every night's ages and agreement, so a regression shows
there.

**Phase 2b: BetOnline.** Three things to build before it goes live:
1. **IDP.** The scrape carries `defensiveTotalTackles` and `defensiveSacks`, which the
   IDP leagues use. The engine's LowVig feed has both (`tackles` 73 players, `sacks` 78
   on 2026-10-22, against the scrape's 8 and 7 that week), but `STAT_MAP` doesn't map
   them, and `project_book` builds offensive stats only.
2. **Positions.** `clean_bol` splits tackles into solo and assisted by position (LB,
   DB, DL), which the widget supplies and the engine doesn't. `assign_positions` would
   call every defender `REC`. The engine's gsis `player_id` plus the crosswalk is the
   likely source.
3. **`clean_bol` assumes the scrape's columns.** It drops `team`, and `pos` after the
   tackle merge, unconditionally, so it would raise on engine output. It also needs
   decision 4, though that's narrower than it reads: only the **ladders** come from the
   widget walk (01:52, 07:52, 13:52 and 19:52 ET), so the 06:00 nightly would read
   ladders four hours old. LowVig's **two-way lines** come from the sportsbook
   collector and change through the day (2026-10-22: latest change 21:34Z, with the
   last ladder change 17:52Z, the 13:52 ET walk). For [plan 56](56-intraday-reblend.md)'s
   intraday re-blend, that means fresh BetOnline main lines once 2b ships.

**Phase 3: switch the game lines.** `read_current(source="engine")` becomes the default.
`run_odds_refresh_nfl.sh` and its 06:45 cron line are retired. `dst/books.py` swaps
`PINNY` for books the engine has (Decision 1).

**Phase 4: retire (category H).**
- Delete `Scripts/books/{pinnacle,betonline,fourcasters,fanduel_props,novig_props,fourcasters_props}.py`
  (the three props adapters stopped running nightly in phase 2),
  `pull.py`, `scrape_BOL.py` and `bol_widget.py`. (`scrape_pinnacle.py` went
  early, with the Bet105 swap.)
  `draftkings_props.py` and the `weekly_props` driver stay until the engine carries
  DraftKings props (Decision 2).
- Remove the `PROPS_ENV_FILES` cross-repo `.env` reads from `run_daily_refresh.sh`.
- Remove or cut down their tests: `test_books`, `test_books_props`, `test_odds_store`,
  `test_scrape_BOL` and `test_dst_books`. `test_weekly_props` keeps its DraftKings cases.
- Keep `Scripts/market.py` (the de-vig and projection maths), `project_stat`, and the
  season-prop scrapers.
- Playwright stays, for `fetch_athletic`.

**Phase 5: what the engine makes cheap (later, separate gates).**
- **The end goal (owner, 2026-10-22): the blend re-read through the day, not only at
  06:00**, so the lineups and scoring follow the market as it moves. Phase 2 is what
  makes this cheap. Reading the board is a bucket sync and seconds of arithmetic, with
  no book scraped, so a re-blend costs the lineups rebuild and nothing more. The first
  version is a game-day re-blend: read the board at 11:30 ET on Sunday so the blend
  sees the inactives-driven moves. Its own plan settles the cadence, which stages
  re-run, and how it sits with the kickoff freeze.
- The engine's market line for team totals in `vegas.py`.
- gsis `player_id` in place of `align_to_espn_names`.
- Plan 52's backtest, run from the engine's history.

---

## Decisions (owner, 2026-10-21)

1. **Pinnacle is dropped.** At the props switch (phase 2), `PINNY_` leaves the `SQUARE_`
   pool and `TD_ALLOCATION_BOOKS`. At the lines switch (phase 3), it leaves D/ST book
   lines, where BetOnline stays and the engine's other books join. `scrape_pinnacle.py`
   is retired in phase 4. The shadow measures the blend **without** Pinnacle, so G3's
   board comparison includes the cost of dropping it, not only the adapter.

   **Amended 2026-10-22: Bet105 takes Pinnacle's place.** The engine carries Bet105
   (`state/bet105/`, first seen that day), and on that day's week-7 board its derived
   stat lines matched our Pinnacle scrape to a median **0.02%** on every shared stat,
   with 163 of 166 players in both. Against Novig it sits exactly where Pinnacle does,
   including Pinnacle's +9–10% lean on passing TDs and receiving yards. So the switch
   replaces `PINNY_` with Bet105 in the `SQUARE_` pool rather than dropping it, and the
   blend keeps the vote it has today. It's shadowed (unpooled) from 2026-10-22, and
   `--compare` measures it against the Pinnacle scrape until that scrape retires.
   Owed before it votes: an alias for "Kevin Concepcion" (Bet105's spelling of KC
   Concepcion) in `NAME_ALIASES`, and a check of anytime-TD and interception depth.
   Bet105 had 36 and 4 rows on the first day. **Both done, and it votes from
   2026-10-23**. See [the Bet105 swap](#the-bet105-swap-2026-10-23). The trigger was practical: on
   2026-10-21 Homebrew re-quarantined the Chrome install `scrape_pinnacle.py` drives, and
   the next nightly hung on the Gatekeeper prompt until it was approved by hand.
2. **DraftKings props stay, as our own scrape, for now.** The owner is scoping DraftKings
   props for the engine. Until they land in the bucket, `weekly_props --book draftkings`
   (ESPN's lines, even money) keeps running in stage 2f and is the one book this repo
   still fetches. When the engine has them, `DK_` moves to the adapter like the others,
   and `draftkings_props.py` is retired then, not in phase 4.
3. **The new books are shadowed first, and pooled by measurement.** Bovada, Kalshi and
   Polymarket are written to their own `Dir`s and prefixes and enter **no** pool during
   the shadow. Each is scored by how closely its de-vigged prices agree with Novig's fair
   price, per stat, on the rungs both list. That's the measurement plan 52 used for the TD
   margins. A book that tracks Novig as closely as FanDuel does joins `SHARP_`, one that
   tracks as loosely as the `SQUARE_` books joins `SQUARE_`, and one with too few shared
   rungs to tell stays out. The thresholds are set from FanDuel's and BetOnline's own
   agreement with Novig, measured in the same run, so the bar is relative rather than
   picked.
4. **Freshness is an open schedule discussion.** The nightly at 06:00 would read the
   engine's 01:52 widget walk, four hours older than the scrape it replaces. **This is the
   one decision that changes SportsbookScrapers rather than this repo.** When the nightly
   (and any game-day re-blend, phase 5) reads the board determines when the engine's
   collectors have to have run: the DST walk, and FanDuel's poll tier. It's settled
   together with that repo's cadence (`schedule/tiers.toml`, `docs/scheduling.md`) before
   phase 2. Phase 1 records each book's age at `T` every night, so the discussion has
   numbers.

---

## What changes on ship

- `docs/SEASON_ROLLOVER.md`: the props stages and the odds cron come out of the runbook,
  and "the engine is down" goes into Troubleshooting, along with the freshness gate's
  message.
- `docs/DATA_CATALOGUE.md`: book outputs are written by the adapter, and `Data/Odds` is
  retired.
- `README.md`: the source section names the engine.
- `SportsbookScrapers/docs/transition/espn-ffl.md`: marked switched, with the date, by
  that repo's owner.
