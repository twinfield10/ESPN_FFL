# 01 — How anything writes to S3

**Status:** BUILT 2026-09-16

**Priority:** High (blocks deploy) · **Effort:** S · **Where it stands:** Done. The
dedup and the scoped push landed first; the lifecycle rules, the orphan-league purge
and the cache eviction landed later the same day, once they had a decision behind
them.
**Answers:** report [01 — The Refresh Ledger](../reports/01-data-and-refresh-sweep.html) §05

---

## The question

The sidebar's refresh button had to start publishing, because under the default
`ESPN_FFL_STORE_SOURCE=s3` it was writing `Data/Store` that nobody reads. The
question that came with it was the real one:

> How do we want the user writing to S3? Temp storage for 24 hours or something so
> things don't get bloated?

## The answer: write to the canonical prefix, and never write bytes S3 already has

**No temp prefix. No TTL. No overlay.** The instinct behind the question is right —
there *is* a bloat problem — but it is not where it looks, and a staging layer would
miss it while adding a failure mode this repo already knows the shape of.

### Why not temp storage

1. **The store is derived state with a deterministic rebuild path.** It is not user
   content. It has no history worth preserving — that is what `snapshots/` is for —
   and a bad write is repaired by the next nightly, at worst 24 hours later.
2. **An overlay doubles every read.** The app resolves a league in one
   `ListObjectsV2` for the prefix fingerprint. An overlay means two listings and a
   per-artifact merge rule in a render path.
3. **A TTL creates the failure this repo keeps hitting.** A stale overlay silently
   shadows a good nightly until it expires, and the page looks entirely normal
   throughout. Same shape as the ten-row FantasyPros teaser and the thirteen-day-old
   book: something stops being right and nothing says so.

### Where the bloat actually was

The bucket has **versioning enabled**. Measured 2026-09-16:

| prefix | current | non-current | total | expiry rule |
|---|---|---|---|---|
| `store/` | 58 MB · 105 obj | **7,699 MB · 12,928 obj** | 7.76 GB | 90 days |
| `snapshots/` | 651 MB | 724 MB | 1.38 GB | **none** |
| `nfl/` | 570 MB | 764 MB | 1.33 GB | 90 days |
| `projections/` | 7 MB | 47 MB | 54 MB | **none** |
| `archive/` | 1.6 MB | 28 MB | 30 MB | **none** |
| `injuries/` | 1.4 MB | 5 MB | 6 MB | **none** |

So the bucket bills at **~10.6 GB**, not the 1.29 GB a plain `s3 ls --summarize`
reports — that command counts current versions only.

An identical PUT does not overwrite. It mints a retained version. And
`push_league_store` uploaded **all eight artifacts unconditionally**, from the nightly
*and* from the live loop's 74-object push every ten minutes through a slate. One
league, one season:

| artifact | versions | MB | distinct contents | redundant |
|---|---|---|---|---|
| `board.parquet` | 207 | 442 | 59 | 148 |
| `board_frozen.parquet` | 156 | 344 | **1** | **155** |
| `lineups.parquet` | 207 | 118 | 99 | 108 |
| `team_stats.parquet` | 162 | 15 | 5 | 157 |
| `draft.parquet` | 207 | 5 | 2 | 205 |
| `tendencies.parquet` | 207 | 3 | 4 | 203 |

`board_frozen.parquet` is the clearest case: it is frozen by definition, it has had
exactly one content in its life, and it was re-uploaded 156 times. ~743 MB of the
7.7 GB is one league's redundant copies; ten leagues account for essentially all of
it. **About 95% of `store/` is bytes S3 already had.**

`Scripts.sync` has skipped unchanged *mirror* files since the play-by-play archive
made it obvious. The store tier — which pushes orders of magnitude more often — was
never given the same rule.

## The convention

1. **Write to the canonical prefix.** `store/season=/league=/artifact.parquet`, the
   keys the app reads. Same for a nightly, a live patch, and a button press.
2. **Never PUT bytes that are already there.** `push_league_store` lists the prefix
   once and compares the object's ETag against the local MD5. Single-part PUTs make
   the ETag the content MD5, verified against the live bucket; the listing is the same
   call the fingerprint already pays for, so the check costs one request per league
   rather than one per artifact.
3. **Scope the push to what the run built.** `--what lineups` publishes `lineups` and
   `meta`, not eight objects. `PUSH_ARTIFACTS` maps `--what` to artifacts.
4. **Any doubt uploads.** A failed listing yields no checksums, everything compares
   unequal, and the push happens. A "nothing to do" that really means "could not
   tell" is the failure mode this repo keeps rediscovering.
5. **A manual refresh never writes a dated snapshot, and the nightly only writes one
   when the board moved.** `snapshots/` means "the market as it stood on date D" and
   is the one prefix with an unbounded slope. A second dated copy of bytes already
   stored under an earlier date is not a data point — and that became the *normal*
   case the moment the board stage left the nightly, since `sync --push` still runs
   every night against a board nothing rebuilds. Unchecked that is ~4 GB by January,
   all of it one distinct board.
6. **`meta.json` last, always.** Unchanged. It is the completeness sentinel.
7. **Cool down the button, and be honest about what that is.**
   `REFRESH_COOLDOWN_SECONDS = 300`, held in `st.session_state`. It stops the honest
   double-press on a run with no visible progress. It is **not** a rate limit — it is
   per browser session and a reload clears it. Real enforcement belongs server-side
   next to the auth boundary `app/auth.py` says it is not.

### Measured effect

```
# before: every push, every time
store  winfield_football  uploaded 8 objects

# after, on a real change
store  winfield_football  uploaded 2 objects

# after, pressing again with nothing changed
store  winfield_football  uploaded 0 objects  (2 unchanged)
```

A full `sync --push --what store` against the current bucket now uploads **0 of 24**
objects for the three leagues checked, because they were already in sync.

## What the lifecycle and the purge actually did (2026-09-16, later the same day)

The section below was written as a decision to be taken rather than a commit. It was
taken. `ops/s3-lifecycle.json` exists now and is the **complete** rule set, which is
the one thing worth knowing before editing it: `put-bucket-lifecycle-configuration`
replaces the whole configuration rather than merging, so omitting the `nfl/` rule from
that file deletes it.

| Prefix | Rule | Why |
|---|---|---|
| `store/` | newest **3** non-current, expire at **7 days** | Derived state with a deterministic nightly rebuild. It held 6.2 GB. |
| `nfl/` | 90 days, unchanged | Play-by-play is minutes to re-pull, not seconds. |
| `snapshots/`, `projections/`, `injuries/`, `scoring/` | 30 days | Regenerable. |
| `archive/` | **nothing expires, ever** | The one irreproducible tier, and now explicitly so rather than by omission. |

`NewerNoncurrentVersions` and `NoncurrentDays` compose as **AND**: a version goes only
when it is both outside the newest three *and* seven days old. So `store/` does not
floor at its current 47 MB, it floors at current plus three supersessions per key —
which is small now that `push_league_store` skips unchanged bytes, and would not have
been before.

**`snapshots/` gets a non-current rule and must never get an `Expiration.Days`.** A
dated key is written once, so its *current* version is the board for that date and the
whole ADP time series this plan calls the unplanned win. Only same-day rewrites are
non-current. The two are one JSON key apart and one of them is unrecoverable.

**The two orphan leagues were purged rather than adopted**, closing the oldest open
question in `docs/deploy/README.md`: 2,583 object versions and 1.26 GB across `store/`
and `snapshots/`, plus 8.8 MB local. Three things made it safe rather than merely
final:

- **`aws s3 rm` would have reclaimed nothing.** On a versioned bucket it writes a
  delete marker, so the objects vanish from `ListObjectsV2` -- and therefore from
  `Scripts.catalogue --s3` -- while every byte stays billed. The purge deleted by
  `VersionId`, in batches of 1,000, checking `.Errors` on each response because
  `delete-objects` exits 0 on partial failure.
- **`meta.json` versions went first.** `push_league_store` uploads it last because it
  is the completeness sentinel; deleting it first is the same invariant run backwards.
  The moment it was gone both leagues disappeared from `s3_store.list_leagues`, so an
  interrupted purge could leave orphan bytes but never a half-visible league.
- **Their `archive/g2/` parquet was kept**, with every non-current version, and so
  were the local `Data/G2/` copies -- those are the only thing `sync --verify --what
  archive` can compare the kept objects against, so deleting them would have made the
  preserved tier unverifiable by any command in this repo.

The hole that kept those leagues alive for six weeks was not the data, it was
`Scripts.sync._league_seasons` fanning out over store prefixes rather than
`config.yaml`. **The config is now the authority for writes and deliberately not for
reads** -- `app/auth.py` scopes the app by filtering a prefix scan, and filtering the
scan itself would have broken that and hidden data from `--verify`.

`Data/.s3cache/` evicts to the newest two copies per key. It was 311 MB across 496
files against the 58 MB store it caches, with 108 cached boards for one league,
because the ETag is in the filename and nothing ever asks for an old one again.

And `Scripts.catalogue` now reports non-current versions beside current ones. It had
been answering **1.2 GB for a bucket that billed 10.6 GB** -- `ListObjectsV2` returns
current versions only -- which is worse than not answering, since `DATA_CATALOGUE.md`
points at it for the live number.

## What is left, and is deliberately not done here

Nothing from this plan. Both items that stood here — the lifecycle gaps and the
`.s3cache` eviction — were applied on 2026-09-16 and are recorded in the section
above.

What the measurements turned up and this plan does **not** address:

- **Compression is not a lever on the board.** `board.parquet` is 2.009 MB and zstd-9
  gives 1.944 MB, because 0.898 MB of it is incompressible parquet footer for 1,662
  columns. Writing it less often was the only thing that was ever going to work.
- **A slim board snapshot would be 25x smaller** — 26 draft-relevant columns measure
  80.9 KB against 2,057 KB — but it would break comparability with the 35 full
  snapshots already stored, and `snapshot_board`'s dedup already drives the tier to
  roughly zero bytes a night while the board is frozen. Worth revisiting before next
  pre-season, not during one.
- **The mirror push still does a `HeadObject` per file**, ~370 a night, which is most
  of the 30-second push stage. Hashing all 542 MB locally takes 0.3s, so the cost is
  entirely round-trips: one `list_objects` per tier prefix plus the `md5_hex`
  comparison `push_league_store` already uses would collapse it.
