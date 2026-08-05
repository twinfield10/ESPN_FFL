# 10 — Scoring registry

**Priority:** High · **Effort:** Small · **Status:** Done
**Grew out of:** [01 (scoring coverage)](01-scoring-coverage.md)

## Problem

Scoring was re-derived from a live ESPN `League` object on every call — four
times per league per pipeline run:

```
fetch_league(A)                                ← 1 ESPN round-trip
get_ply_stats_by_matchup() → fetch_league(B)   ← 2nd round-trip, same league
  └─ build_scoring_table(B)
build_fa_market(A)      └─ build_scoring_table(A)
clean_lineups(A)        └─ build_scoring_table(A)
  └─ proj_to_score(A)   └─ build_scoring_table(A)
```

Three consequences:

1. **No source of truth.** Scoring was whatever the last ESPN fetch said, read
   from an object `espn_api` mutates behind your back (plan 01's
   `isolate_scoring_format` patch).
2. **No history.** Nothing recorded what a league's rules *were*, so answering
   "what changed this season" meant a live scan of every league-season — about
   90 seconds of ESPN calls during the plan 01 investigation.
3. **No drift detection.** A commissioner changing scoring mid-season was
   invisible; the pipeline would keep scoring with the old rules and produce
   normal-looking, wrong numbers. The same failure mode plan 01 fixed, one level
   up.

## Design

One tidy registry, all leagues × all seasons, written explicitly and read by
everything:

```
Data/Scoring/scoring.csv
season, league_key, league_name, source_id, id, abbr, label, points, colName, recorded_at
```

1,860 rows across 45 league-seasons (2016-2026), 214 KB.

**Committed to git**, which makes `git log -p Data/Scoring/scoring.csv` a
readable history of every scoring change. Keyed on `league_key` rather than the
numeric ESPN league id, so committing it publishes nothing `.gitignore` currently
withholds — display names are already in `populateGoogleSheet.py`.

`build_scoring_table()` stays exactly where it is as the pure ESPN→table
derivation. `Scripts/scoring.py` wraps it.

### `source_id` vs `id`

`REPL_SCORING` rewrites "every N yards" rules onto the stat they count, so `id`
is not the id the commissioner configured. `source_id` preserves that.

This is not cosmetic. Keyed on `id`, the GOP 2025→2026 kicker change reads as
`214 repriced 0.1 → 0.064` — conflating a rule the commissioner *deleted* with a
modelling rate *we* chose. Keyed on `source_id` it reads correctly:

```
214  FGY    FG Made Yards            0.1    NaN   removed
221  FGY    FG Yards                 NaN    0.064 added
79   FGM40  FG Missed (40-49 yards)  NaN   -1.0   added
```

### Resolution order

`get_scoring_table(league)`:

1. Registry hit for `(league_key, season)` → use it.
2. Miss → derive from the live league and **warn** that the registry is cold.
   With no league to fall back to, raise.
3. Both a hit and a live league → **compare, and warn on disagreement.** This is
   what catches a mid-season scoring change. It costs nothing, because the league
   has already been fetched.

### The trap that shaped the API

`proj_to_score` **mutates** the table it is given — 13 `s_df.loc[...] = ...`
writes at `projection_utils.py:362-390`, re-scoring sacks and tackles for the IDP
league. So `get_scoring_table` returns a **fresh copy every call** and the memo
holds a private original.

A cache without that defensive copy would recreate the exact `espn_api` bug this
registry exists to escape — and worse, since it would corrupt a league's own
rules mid-run rather than another league's. Pinned by
`test_get_scoring_table_returns_a_fresh_copy_each_call`.

### Stable timestamps

`recorded_at` means "first seen with these values", not "last checked". Unchanged
rules keep their original stamp, so a refresh that finds nothing new produces a
**byte-identical file**. Without this, every refresh would restamp all 1,860 rows
and bury real changes in full-file diff noise — which is most of the reason to
commit the file at all.

## Usage

```bash
python -m Scripts.scoring --all                        # every league, every season
python -m Scripts.scoring --league gop_degenerates     # one league, current season
python -m Scripts.scoring --diff gop_degenerates 2025 2026
python -m Scripts.scoring --gaps                       # scored but unmodelled rules
```

Re-run `--all` whenever a league's settings might have changed — cheap, and the
diff tells you if anything did.

## What it caught immediately

**GOP_Degenerates changed far more than its kicker rules for 2026.** Plan 01's
evidence said "GOP Degenerates reworked its kicker rules"; the registry shows 16
repriced rules, mostly defensive:

| rule | 2025 | 2026 |
|---|---|---|
| Each Sack | 12.0 | 1.0 |
| Each Interception | 1.0 | 2.0 |
| 0 points allowed | 30.0 | 10.0 |
| 1-6 points allowed | 26.0 | 10.0 |
| 18-21 points allowed | 14.0 | 3.0 |
| Solo Tackles | 2.0 | 1.5 |

**Winfield_Football's 2019→2020 gap is a rule migration**, not a settings change:
`74` (FG Made 50+) removed, `198` (FG Made 50-59) added. Independent confirmation
of the plan 01 finding, from the registry rather than a live scan.

## Known limitation: scoring is per-lineup-slot, and this table is not

Surfaced while building this, not yet fixed. ESPN scores a stat differently by
lineup slot via `pointsOverrides`. `espn_api` collapses that to one value:

```python
points_override = scoring_item.get('pointsOverrides', {}).get('16')
scoring_type['points'] = points_override or scoring_item.get('points', 0)
```

Two problems.

**The falsy-or.** An override of exactly `0.0` falls through to the base value.
GOP 2025 stat 99 (sack) is `base=12.0, override{16: 0.0}` — the D/ST value is
zero, but `espn_api` reports 12.0. So `build_scoring_table` returns a *mix*: the
D/ST value where the override is non-zero, the base (IDP) value where it is zero.
Neither one thing nor the other.

**Scope.** All nine leagues use slot-16 overrides, but only GOP has any set to
`0.0` — 5 rules in 2026 — because it is the only league with individual defensive
players. So eight leagues are unaffected in practice.

**The existing workaround is stale.** `proj_to_score` hardcodes GOP's IDP values
(`projection_utils.py:362-390`) and they match neither season's actual settings:
it sets DP sack to 10 where the 2025 base is 12 and the 2026 base is 5; DP
interception to 12 where the bases are 9 and 6.

**Fix:** give the registry a `slot` dimension — one row per (rule, slot class) —
read `pointsOverrides` directly rather than through `espn_api`, and delete the
hardcoded block. Sized as its own plan; it changes GOP's IDP projections, so it
wants the equivalence harness.

## Verification

```bash
pytest tests/                 # 68 pass, 4 live deselected
pytest tests/ -m live         # 4 pass
```

- Registry-sourced tables are **identical** to live derivation for all nine
  leagues, including `colName`.
- A `proj_to_score`-style mutation of a returned table does not leak into the
  next read.
- Two consecutive `--all` runs produce a byte-identical file.
