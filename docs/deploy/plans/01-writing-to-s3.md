# 01 — How anything writes to S3

**Status:** BUILT 2026-09-16

**Priority:** High (blocks deploy) · **Effort:** S · **Where it stands:** The dedup and
the scoped push are in. The lifecycle gaps in §5 are **not** applied — they are a
bucket-level change and want a decision, not a commit.
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

## What is left, and is deliberately not done here

**Four prefixes have no non-current expiry rule at all** — `snapshots/`,
`projections/`, `injuries/`, `archive/` — holding 804 MB of versions retained
forever. Changing bucket lifecycle is an infrastructure decision with no undo for
what it deletes, so it wants a call rather than a commit. The shape, if wanted:

```bash
# adds NoncurrentVersionExpiration to the four unruled prefixes.
# READ THIS FIRST: it permanently deletes non-current versions older than N days.
aws s3api put-bucket-lifecycle-configuration --bucket espn-ffl-data \
  --lifecycle-configuration file://ops/s3-lifecycle.json
```

Two judgement calls inside it:

- **`snapshots/` non-current versions are safe to expire** — a dated board key is
  written once per date, so a second version only exists where a date was rewritten.
  The *current* versions are the archive and must never expire.
- **`archive/g2/` should arguably keep everything.** It is the one tier the module
  docstring calls irreproducible.

Separately, and cheaper: **`Data/.s3cache/` has no eviction** (311 MB, 496 files, 108
versions of one board). `_cache_path` keys on ETag and says a stale entry "is simply a
file nobody asks for" — which is true and also means nothing ever deletes it. Keeping
the newest two per key reclaims ~290 MB with no behaviour change.
