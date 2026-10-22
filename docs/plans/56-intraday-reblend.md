# 56 — The blend, re-read through the day

**Status:** TO DO

**Where it stands:** Scoped and measured 2026-10-22, decisions taken the same day,
nothing built yet. The two numbers
that shape it were measured that day on the MacBook (the mini is owed the same
measurement):
- **A full lineups rebuild is 149 seconds for nine leagues, and 93% of it is ESPN.**
- **A current-week-only blend matches the full rebuild exactly**, in 0.7 seconds.

So a re-blend that fetches only the current week should cost about a minute and stay
flat all season. The full rebuild grows every week.

**What it is:** the owner's end goal for [plan 54](54-props-from-the-sportsbook-engine.md)
(phase 5): lineups and scoring that follow the market through the day, not only the
06:00 nightly. A new `Scripts.refresh --what reblend` rebuilds **this week's
projections only**, from the engine's board as of now and ESPN's current week, and
splices them into the stored lineups. A cron wrapper runs it through the day,
self-gating like the live job, and it pushes only leagues whose numbers moved.

---

## Why now

- **Plan 54's phase 2 made the market cheap to re-read.** FanDuel, Novig, 4Casters and
  Bet105 come from the engine, which polls every minute. Reading the board takes 6-9
  seconds (2026-10-22, MacBook) and scrapes no book.
- **The board goes stale in the ways that matter most.** Friday injury reports,
  Sunday's inactives (90 minutes before each kickoff) and the line moves that follow
  them all land after 06:00. The nightly sees them the next morning, after the games.
- **The other half already exists.** `ops/espn_ffl_live.sh` runs every ten minutes on
  game days and patches actual scores and rosters onto the stored lineups. By design it
  never moves a projection column ([plan 48](48-live-scoring.md)). This plan is the
  projection half.

---

## What was measured, 2026-10-22 (MacBook, week 7)

**The full lineups rebuild** (`python -m Scripts.refresh --all --what lineups`):
149.3 seconds wall clock for nine leagues, 13.5-19.4 seconds each, with 67 seconds of
CPU. Split for two leagues:

| League | `fetch_league` | matchup stats | free agents | blend | total |
|---|---|---|---|---|---|
| winfield_football | 1.1 s | 7.3 s | 4.5 s | 0.9 s | 13.9 s |
| gop_degenerates | 1.8 s | 11.5 s | 5.9 s | 1.0 s | 20.1 s |

**The blend is about 1 second of every league's 14-20.** The matchup stats are
`get_ply_stats_by_matchup` re-fetching every week from 1 to the current one, so that
cost grows each week. By week 17 the full rebuild should be well over twice what it is
now.

**The current week alone is enough.** For winfield_football, `clean_lineups` was run
on the full ESPN frame and on that frame filtered to week 7. **All 1,327 numeric
columns for week 7 are identical** (max difference 0.0, `TRUE_Points` included), and
the one-week blend took 0.7 seconds. No week-7 projection depends on earlier weeks'
rows.

**Other inputs:**
- Engine read: 6-9 seconds, every book.
- DraftKings props (`weekly_props --book draftkings`, the one book still scraped): 19.5
  seconds, all of it network.
- S3: `store/` already holds **1,257 non-current versions of `lineups.parquet`,
  2.95 GB**, mostly from the live job's pushes. Versioning keeps every PUT for 90 days.
  A re-blend that pushed nine leagues every 30 minutes would add about 1 GB a day.

---

## The design

```
cron (every 30 min, 08:00-23:30 ET)
  └─ ops/espn_ffl_reblend.sh
       ├─ gate: Scripts.game_state --if-pending     (any game this week still "pre"? else exit)
       ├─ python -m Scripts.books.engine             (FanDuel, Novig, 4Casters, Bet105 as of now)
       ├─ python -m Scripts.parse_athletic           (imports a slate the 17:45 / Sun 12:00 fetch landed)
       └─ python -m Scripts.refresh --all --what reblend --push
            per league:
              ESPN: fetch_league + this week's box scores (live.week_box_scores) + free agents
              clean_lineups on this week's rows only                (identical to a full rebuild)
              splice into the stored lineups: other weeks untouched
              kickoff_freeze.apply                                   (started games keep their columns)
              materiality: write and push only if a pre-game player moved ≥ 0.1 TRUE_Points,
                           or a roster/status changed
```

- **Projection columns for unplayed games only.** The kickoff freeze already holds every
  column for a player whose game is `in` or `post`, so a re-blend can't rewrite a game
  in progress, and the live job owns those rows. Past weeks are never rebuilt.
- **ESPN's current week is re-read, not cached.** Caching the ESPN frame would cut the
  run to the blend's second a league, but it would miss what intraday is for: ESPN
  zeroing an inactive player's projection
  ([the in/out signal](54-props-from-the-sportsbook-engine.md) the blend withdraws books
  on), and status changes from injury reports.
- **One lock with the live job.** Both rewrite `lineups.parquet` as read-modify-write.
  `_write_parquet_atomic` makes each write atomic but not ordered, so they share
  `/tmp/espn_ffl_live.lock`, and whichever finds it held skips that firing.
- **Pushes are scoped and gated.** `--push` already sends only the artifact a run built,
  and `push_league_store` skips bytes S3 already holds. A re-blend will almost always
  change a few decimals, though, so the materiality check decides whether a league is
  written at all.
- **FantasyPros stays nightly.** Its weekly file freezes at first capture by design
  (`docs/SEASON_ROLLOVER.md`), so there's nothing to re-read intraday.

---

## Steps

1. **A one-week ESPN frame.** Use `live.week_box_scores` (or a `weeks=` argument on
   `get_ply_stats_by_matchup`) to build this week's lineups rows. Check they carry
   every column `clean_lineups` reads, projections included, not just the actuals the
   live path patches.
2. **`--what reblend` in `Scripts/refresh.py`.** Splice, freeze, apply the materiality
   check, write. The meta records `reblended_at` beside `built_at`, so the app can say
   "projections as of 14:30" truthfully.
3. **`Scripts.game_state --if-pending`**: exit 0 while any game this week is still
   `pre`, 3 when none is (Tuesday morning after Monday night), anything else when
   broken. Same three-code contract as `--if-live`.
4. **`ops/espn_ffl_reblend.sh`**, modelled on `espn_ffl_live.sh`: rotating log,
   heartbeat on the closed-gate exit, the shared lock with staleness expiry, no
   `git reset`. It runs from the `ESPN_FFL-nightly` checkout the 06:00 job pins.
5. **Runbook:** `docs/SEASON_ROLLOVER.md` gets the crontab line, the one-command check,
   and a troubleshooting row. `README.md` gets the schedule.

---

## Gates

- **G1, identity.** For every league, `--what reblend` at a fixed moment equals a full
  `--what lineups` rebuild at the same moment on the current week's rows (all numeric
  columns within 1e-9), and leaves every other week byte-identical. The week-7
  winfield check above is the first instance. The gate is all nine leagues, on the
  real one-week fetch from step 1.
- **G2, cost.** At most 90 seconds for nine leagues on the mini, and flat from week to
  week: measured at week 8 and again at week 12, while the full rebuild grows.
- **G3, S3 growth.** Over one full game week, non-current bytes added under `store/`
  stay within twice what the live job alone adds. The materiality gate is the lever.
- **G4, safety.** Over a Sunday: no started game's projection columns change after
  kickoff, the live columns (`LIVE_Points`, actuals) are never lost to a re-blend, and
  no firing overlaps a live run (from the lock lines in both logs).

---

## Decisions (owner, 2026-10-22)

1. **Cadence: every 30 minutes, 08:00-23:30 ET, gated on a pending game.**
   Inactives land 90 minutes before kickoff, so a 30-minute cadence picks them up with
   an hour to spare. The materiality gate makes quiet firings nearly free. Fixed times
   (hourly, plus T-75 and T-20 minutes before each kickoff window) were the
   alternative: more precise, and more to maintain.
2. **DraftKings stays nightly.** It's 19.5 seconds a firing and the only scrape
   left, and the square pool has four other books. It
   moves to the engine when the engine carries DraftKings props (plan 54, decision 2).
3. **BetOnline (LowVig in the engine) moves at the nightly's pace until plan 54's
   phase 2b**, because until then it's still our own scrape and the re-blend reads
   whatever its season file last holds. Measured 2026-10-22 22:06Z, this is not a
   limit of the engine's LowVig feed. The collector polls continuously (last seen
   22:04Z), and its **two-way lines** change through the day (latest change 21:34Z). Only
   the **ladders** (alternate rungs, anytime TDs, sacks, tackles) come from the
   four-times-a-day widget walk (01:52, 07:52, 13:52, 19:52 ET; latest change 17:52Z).
   The main line is what a projection centres on, so phase 2b would give BetOnline
   intraday main lines with ladders up to six hours behind. That makes 2b worth more
   to this plan than plan 54 assumed.

---

## What changes on ship

- `docs/SEASON_ROLLOVER.md`: the crontab line, `python -m Scripts.game_state` for the
  gate, and "projections didn't move during the day" in Troubleshooting.
- `docs/DATA_CATALOGUE.md`: `lineups.parquet` is rewritten intraday for the current
  week, and `meta.json` gains `reblended_at`.
- `README.md`: the schedule names three jobs: the 06:00 nightly, the live job and the
  re-blend.
