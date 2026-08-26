# 24 — S3 as the system of record

**Status:** COMPLETE

**Priority:** **High** · **Effort:** M · **Where it stands:** **Done** (2026-08-11)
**Depends on:** [07 (store)](07-frontend-foundation.md) — done
**Unblocks:** a cloud runner for the nightly refresh; a query layer over history

`Data/` was 152 MB on one laptop, and the durability story for the one artifact in
this repo that cannot be rebuilt — the [G2 counterfactual](18-season-usage-model.md)
— was "it is committed to git." The nightly job runs from cron at 6am, so a shut lid
is a skipped night, and nothing about the layout would have survived moving that job
anywhere else.

## Goal

Make S3 the system of record for the data flow: the nightly refresh pushes to it, the
app reads from it, and local disk becomes a writer's scratch pad plus a read cache.

Four things were wanted from it, and all four are now true: durability for the
irreproducible, the bytes off the laptop, nothing in the design that assumes local
disk, and a partition layout a query engine can read.

## What the bucket already was

`espn-ffl-data` existed and was **empty** — created for
`Scripts/aws_utils.py`, a dormant module whose only caller had its S3 write commented
out and which had no live callers at all. It is deleted; `Scripts/s3_store.py`
replaces it.

Two things about the bucket were already right and were kept: it is in `us-east-2`,
the same region as the default profile, so there is no cross-region egress; and
**versioning was already enabled**.

## The key schema

Hive-style `key=value` for the two tiers a query engine will read, a plain mirror for
the rest. The rule for the mirror is uniform rather than per-directory: **any path
component that is a bare four-digit year becomes `season=YYYY`.**

```
store/season=2026/league=knights_ffl/board.parquet
store/season=2026/league=knights_ffl/meta.json          <- uploaded last

snapshots/board/season=2026/league=knights_ffl/date=2026-08-11/board.parquet

archive/g2/season=2026/knights_ffl.parquet
nfl/season=2026/depth_charts.parquet
projections/Usage/Season/season=2026/Usage_SeasonProjections.parquet
```

`Data/Equivalence/` is **not** uploaded. At 74 MB it is half the total, and it is
before/after debug snapshots from the espn-api 0.46.0 migration — evidence about a
bug that is fixed, not live data. `Data/refresh_status.json` is not uploaded either:
it is a fact about one laptop rather than about football.

## The dated board snapshots are the unplanned win

`Data/G2/` exists because a past board cannot be reconstructed. FantasyPros serves no
season parameter, so a board is gone the moment it stops being current, and
[plan 18](18-season-usage-model.md) records G2 as unmeasurable on history for exactly
that reason. It had to be built by hand, before week 1, or the evidence was lost for
a year.

One dated key per nightly push retires that whole class of problem. It also makes
something newly measurable that never was: **ADP drift through camp**, at daily
resolution, across nine leagues. That was not the reason for doing this and it is
probably the most useful thing to come out of it.

## What carried over, and what could not

**`meta.json` last, and it is still the sentinel.** On disk, artifacts are written to
a `.tmp` sibling and `os.replace`d, and `meta.json` is written last so `has_store()`
can key on it. S3 has no rename, so the mechanism does not port — but the invariant
does. Each PUT is atomic, read-after-write is strongly consistent, and
`push_league_store` uploads `meta.json` last. A reader keying on it sees the previous
complete store or the new one, exactly as before.

What is genuinely lost is atomicity across the *set* of five objects. That is
inherent to S3, and it is written down here rather than papered over.

**The cache key.** `store_mtime()` was the app's Streamlit cache key — a `stat`.
Against S3 it is `prefix_fingerprint()`, a digest over every ETag under the league's
prefix, costing **one** `ListObjectsV2` rather than a `HeadObject` per artifact. The
architecture is unchanged: the version is a real function argument, so a refresh
still invalidates the cache with no explicit clear anywhere.

**Checksums.** Every upload uses `put_object`, never boto3's `upload_file`. That is
deliberate: `upload_file` switches to multipart above a threshold, and a multipart
object's `ChecksumSHA256` is a hash *of the part hashes*, not of the file — so
verification would silently stop comparing like with like the first time an artifact
grew past 8 MB. `put_object` is single-part to 5 GB; the largest artifact is 2.5 MB.

## What shipped

| | |
|---|---|
| `Scripts/s3_store.py` | The boundary. Key mapping, checksummed upload, ETag-cached reads, fingerprints. Replaces `aws_utils.py` |
| `Scripts/sync.py` | `--push` / `--pull` / `--verify`, `--dry-run`, `--what store,archive,nfl` |
| `app/store.py` | Reads S3 by default, behind `ESPN_FFL_STORE_SOURCE` |
| `run_daily_refresh.sh` | Step 6: push, on a clean run only |
| Bucket lifecycle | Noncurrent versions expire after 90d under `store/` and `nfl/` |

**At the first push (2026-08-11):** 258 objects, 77.5 MB — 133 nfl, 59 projections,
45 store, 10 archive, 9 snapshots, 2 scoring/injuries.

That is a measurement of one morning, not a description of the bucket, and it is dated
because it **cannot stay true**: the snapshot tier gains nine objects a night by
design, so the total is supposed to climb. For the live answer,
`python -m Scripts.catalogue --s3`, for the same reason
[the catalogue carries no counts](../DATA_CATALOGUE.md) — a number in a doc about
untracked data goes stale in no diff and reads exactly as authoritative as a right one.

**Verified:** 249 current-state files SHA-256 identical local against S3, by
`python -m Scripts.sync --verify`. That count *is* stable, and the arithmetic against
the total is the invariant worth keeping rather than the totals themselves: everything
S3 holds beyond those 249 is a dated board snapshot, which has no current-state
counterpart to compare against.

## Reading it

The app defaults to S3. Three modes, read at call time so they can be changed without
restarting Streamlit:

```bash
streamlit run app/main.py                            # s3 (default)
ESPN_FFL_STORE_SOURCE=local streamlit run app/main.py   # disk, offline
ESPN_FFL_STORE_SOURCE=auto  streamlit run app/main.py   # S3, falling back to disk
```

`local` is the draft-morning escape hatch, and it exists because a render path that
depends on a network is one that can fail at the worst possible moment. An S3 error
in `s3` mode names the variable in its message.

## Lifecycle, and what is deliberately exempt

Versioning was on with no rules, so noncurrent versions would have accumulated
forever. Noncurrent versions now expire after 90 days under `store/` and `nfl/` —
both regenerable.

**`archive/`, `projections/`, `scoring/`, `injuries/` and `snapshots/` have no expiry
and keep every version forever.** For `archive/` that is the entire point: it holds
the data that cannot be regenerated at any price. Applied once, as bucket
configuration:

```python
s3.put_bucket_lifecycle_configuration(Bucket="espn-ffl-data", LifecycleConfiguration={
    "Rules": [
        {"ID": "expire-noncurrent-store", "Filter": {"Prefix": "store/"},
         "Status": "Enabled", "NoncurrentVersionExpiration": {"NoncurrentDays": 90}},
        {"ID": "expire-noncurrent-nfl", "Filter": {"Prefix": "nfl/"},
         "Status": "Enabled", "NoncurrentVersionExpiration": {"NoncurrentDays": 90}},
    ]})
```

## Cost

77.5 MB at rest is about **$0.002/month**. The snapshots are the only tier that
grows: nine boards at ~1.7 MB nightly is ~5.7 GB/year, about **$0.13/month** after a
full year. Requests are negligible and the first 100 GB/month of egress is free. Cost
is not a design constraint at this volume and should not be treated as one.

## Consequences, accepted knowingly

- **The app is network-dependent by default.** Measured on the real 1,026 × 1,236
  Knights board:

  | | |
  |---|---|
  | `local` | **94 ms** |
  | `s3`, cold (list + download + parse) | **231–342 ms** |
  | `s3`, Streamlit cache hit | **58 ms** |

  So a cold S3 render is roughly 3× local and still comfortably sub-second, and the
  cache hit is *faster* than a local read because the parse is cached too. Offline use
  now means a warm `Data/.s3cache/` or `ESPN_FFL_STORE_SOURCE=local`.
- **Local and S3 can skew** between a refresh and its push. The window is seconds,
  and `--verify` closes it on demand.
- **Set-level atomicity is gone**, as above.
- **`--pull` deliberately bypasses the ETag cache.** It writes each object to its real
  home, so caching a second copy doubled the footprint of every pull — 20 MB of store
  became 40 MB. Found while measuring, fixed, and pinned by a test.
- **Checking out a commit from before this one, and coming back, empties `Data/`.**
  Found the hard way on 2026-08-14, merging this branch. Those files were tracked at
  `ab477f9` and are untracked now, so checking out the older commit materialises them
  and returning to a commit that deletes them from the index deletes them from the
  working tree as well. Git is right; it reads as data loss. `--pull` restores
  everything, which is the whole point, and this is the first time the design was
  tested by needing it rather than by measuring it. The tell is `test_crosswalk`,
  `test_lab_g2` and `test_nfl_utils` failing together — the guards that read real data
  instead of fixtures. In the [runbook](../SEASON_ROLLOVER.md#troubleshooting) too,
  because the symptom is alarming and the fix is one command.

## What is left

- **A cloud runner.** Deliberately not built here — the design stops assuming local
  disk, which was the prerequisite. Moving the nightly job needs ESPN cookies in a
  secret store, an R runtime for `R/GetContext.R`, and a notification path that is
  not `osascript`.
- **The query layer.** The partitions are laid out for Athena or DuckDB-over-S3 and
  nothing reads them that way yet. The first question worth asking is the ADP drift
  the snapshots now make answerable.
- **`populateGoogleSheet.py` still reads local**, on purpose: it runs in the same
  chain as the writer, moments after it, so a local read is both correct and faster.
  If the refresh ever moves off this laptop, that assumption moves with it.
