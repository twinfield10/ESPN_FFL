# Deployment

Plans and reports written in preparation for deploying the app beyond this laptop.

Separate from [`docs/plans/`](../plans/), which is the record of how the *pipeline*
was built and why. This directory is about what has to be true before other people
can point a browser at it: what data ships, what refreshes it, what it costs, and
which of the laptop's assumptions stop holding.

## Reports

| | | |
|---|---|---|
| [01](reports/01-data-and-refresh-sweep.html) | **The Refresh Ledger** | Every byte stored, every job that moves one, and the missing kickoff guard on the projection blend. Measured 2026-09-16. |

Reports are self-contained HTML, following the convention of `docs/projection_pipeline.html`
and `docs/projection_sources.html` — open them with `open <path>`.

## Plans

| | | |
|---|---|---|
| [01](plans/01-writing-to-s3.md) | **How anything writes to S3** | Canonical prefix, content dedup, scoped push, no staging layer — and why "temp storage for 24 hours" would have solved the wrong problem. |

Numbered like `docs/plans/`, one file per decision, written when a report turns up
something that needs building rather than knowing.

## Open questions this directory exists to close

Carried out of report 01, in the order they block a deploy.

**Closed 2026-09-16:**

1. ~~**The sidebar refresh button** wrote the local store and never pushed.~~ It now
   runs `Scripts.refresh --league X --push`, publishes only the artifacts it built,
   skips bytes S3 already holds, and greys out for five minutes after a successful
   run. See [plan 01](plans/01-writing-to-s3.md).
2. ~~**The kickoff freeze.**~~ `Scripts/kickoff_freeze.py` holds every projection
   column for a player whose game is `in` or `post`, applied at the store boundary in
   `Scripts.refresh`; `live.FROZEN_AFTER_KICKOFF` stops the ten-minute loop rewriting
   `projPoints` on those rows; `Scripts.scrape_FP` freezes per game rather than per
   week. **Partial:** the BetOnline, Pinnacle and Athletic *source files* are still
   ungated — see (8), now the kickoff-gate item.
3. ~~**Draft boards rebuilt nightly.**~~ Out of `run_daily_refresh.sh` — 97s of a 539s
   run. `percent_owned` and `injury_status` moved to `pool.parquet` so the waiver wire
   did not go stale with it. Now **gated rather than removed**: `Scripts.freeze.board_is_cold`
   keeps it warm pre-season and while a league that has drafted is unfrozen, so it
   returns next August without anyone remembering to.
4. ~~**Two orphan leagues.**~~ Purged 2026-09-16 — **2,583 object versions, 1.26 GB**
   across `store/` and `snapshots/`, plus 8.8 MB local. Their `archive/g2/` parquet is
   deliberately kept: it is the irreproducible counterfactual behind the published fit
   in `Scripts/lab/results.json`. The hole that kept them alive was
   `Scripts.sync._league_seasons` fanning out over store prefixes rather than
   `config.yaml`; the config is now the authority for writes.
5. ~~**Lifecycle gaps.**~~ `ops/s3-lifecycle.json` applied 2026-09-16, covering **all
   seven** prefixes. `store/` tightened from 90 days to *newest 3 non-current, 7 days*
   — it held 6.2 GB of derived state the nightly rebuilds. `archive/` gets an explicit
   rule that expires nothing, so it reads as deliberate rather than forgotten.
   `Data/.s3cache/` now evicts to the newest two copies per key (`s3_store.CACHE_KEEP`);
   it was 311 MB against the 58 MB store it cached.

**Still open:**

6. **The button is still an ESPN round-trip from a render path.** A five-minute
   session-scoped cooldown is right for one operator on one laptop and is not a rate
   limit. Before anyone else gets a URL, this needs server-side enforcement — and the
   same applies to (7).
7. **Auth is a seam, not a boundary.** `app/auth.py` says so itself. Enforcement has to
   move to the store read.
8. **Three source scrapes have no kickoff gate.** BetOnline, Pinnacle and The
   Athletic overwrite a week's props on the newest capture, protected only by books
   retiring a market once its game starts. Real today, not enforced. Generalise
   4Casters' `if not g["live"]` into a shared helper.
9. **A cold-storage gate that only the code knows.** `Scripts.freeze.board_is_cold`
   keeps the board stage out of the nightly in-season and puts it back pre-season, but
   three of the eight leagues carry **no 2026 draft at all** — `drafted_picks` reads 0
   for `john_atl_league`, `john_pc_league` and `twelve_dudes_one_cup`, so their Draft
   page has nothing to show and they cannot be frozen. `draft` is in no schedule;
   nothing rebuilds it. Decide whether those leagues want a draft artifact.
