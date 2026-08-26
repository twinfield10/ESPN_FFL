# 06 — Performance and warning hygiene

**Status:** TO DO

**Priority:** Low · **Effort:** Small · **Where it stands:** Not started

Not urgent, but the two are related: the code is slow in a specific, fixable
way, and the warning that says so is globally muted.

## Problem 1 — a global warning filter hides everything

`Scripts/fetch_utils.py:16`:

```python
warnings.filterwarnings("ignore")
```

This is module-level and unscoped, so **any** process that imports `fetch_utils`
— which is every entry point — silences every warning from every library for
the rest of its life. Deprecations, pandas `FutureWarning`s that would flag the
3.0 migration, and the fragmentation warnings below all disappear.

**Fix:** delete it, or narrow it to the specific noisy warning it was added for
and scope it with a context manager. Anything genuinely noisy afterwards can be
suppressed individually.

Plan 01 no longer depends on this: its `ScoringCoverageWarning` is emitted inside
a `catch_warnings()` block that force-enables its own category, so the global
filter cannot eat it. Every *other* warning is still silenced, so this is still
worth doing — and doing it will surface the `PerformanceWarning`s below.

## Problem 2 — DataFrame fragmentation

With warnings unmuted, one `clean_lineups` run emits **54** of these:

```
PerformanceWarning: DataFrame is highly fragmented. This is usually the result
of calling `frame.insert` many times, which has poor performance.
```

The cause is column-at-a-time insertion in loops. `compute_weighted_stats`
creates one `TRUE_*` column per stat inside a Python loop, and `proj_to_score`
accumulates `*_Points` the same way — across ~45 scoring columns × 6 source
prefixes, that's a lot of individual inserts into a 350-column frame.

**Fix:** build the columns into a dict and concatenate once:

```python
new_cols = {}
for stat in stats_list:
    ...
    new_cols[f"TRUE_{stat}"] = series
df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)
```

Mechanical, and the equivalence harness confirms it's safe.

## Problem 3 — quadratic ingest

Two patterns in the ESPN ingest make backfills slow enough to avoid running:

- `pd.concat` inside a row loop — `scrape_player_stats.py:162`
- `df.loc[i, col] = ...` cell-at-a-time assignment —
  `scrape_team_stats.py:204-343`

Both are the standard accumulate-then-build-once fix: append dicts to a list,
construct the DataFrame at the end. `scrape_team_stats` writes ~40 columns per
row this way, so it's the bigger win.

This matters for the draft work — Phase 1 backfills ten seasons of draft
history, and `draft_utils.get_draft_details` uses the same `df.loc[i, col]`
pattern with a per-pick API call (its own docstring notes ~1.5 min). Worth
fixing there as it's rewritten rather than carrying the pattern forward.

## Problem 4 — remaining bare excepts

Five left, all swallowing the exception entirely:

```
Scripts/scrape_BOL.py:260, 326
Scripts/scrape_player_stats.py:148, 153, 301, 306
populateGoogleSheet.py:618
```

The `scrape_player_stats` ones sit in the per-player stat extraction, so a
malformed player silently produces zeros rather than an error — indistinguishable
from a genuine zero. At minimum catch a specific type and log the player.

`populateGoogleSheet.py:618` already prints a message, so it only needs
narrowing.

## Verification

- `clean_lineups` runs with zero `PerformanceWarning`s.
- Equivalence harness still matches after the concat refactor.
- Timing before/after on a full `scrape_team_stats` backfill.
