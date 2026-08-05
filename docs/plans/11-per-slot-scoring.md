# 11 — Scoring is per-lineup-slot, and the pipeline treats it as flat

**Priority:** High · **Effort:** Medium · **Status:** Not started
**Found by:** [10 (scoring registry)](10-scoring-registry.md)
**Affects:** GOP_Degenerates only, but materially

## Problem

ESPN lets a league score the same stat differently depending on the lineup slot
the player occupies — a sack is worth one thing to a D/ST unit and another to an
individual defensive player. It expresses this as `pointsOverrides` keyed by slot
id, alongside a base `points`.

`espn_api` 0.45.1 collapses that to a single number, and does it with a falsy-or:

```python
# espn_api/football/settings.py
points_override = scoring_item.get('pointsOverrides', {}).get('16')
scoring_type['points'] = points_override or scoring_item.get('points', 0)
```

Two distinct bugs.

**1. Only slot 16 (D/ST) is read.** Every other slot's override is discarded, so
individual-defensive-player scoring is simply unavailable.

**2. An override of exactly `0.0` falls through to the base.** `0.0 or 12.0`
evaluates to `12.0`. So a rule the commissioner explicitly zeroed for D/ST comes
back carrying the IDP value.

The result is that `build_scoring_table` returns a **mix**: the D/ST value where
the override is non-zero, and the base (IDP) value where the override is zero.
Neither the D/ST table nor the IDP table.

## Evidence

Raw ESPN settings for GOP_Degenerates, versus what `espn_api` reports:

| season | stat | base | override{16} | espn_api says | truth for D/ST |
|---|---|---|---|---|---|
| 2025 | 99 Sack | 12.0 | **0.0** | 12.0 | **0.0** |
| 2025 | 95 Interception | 9.0 | 1.0 | 1.0 | 1.0 |
| 2025 | 108 Solo Tackles | 2.0 | **0.0** | 2.0 | **0.0** |
| 2026 | 99 Sack | 5.0 | 1.0 | 1.0 | 1.0 |
| 2026 | 95 Interception | 6.0 | 2.0 | 2.0 | 2.0 |
| 2026 | 108 Solo Tackles | 1.5 | **0.0** | 1.5 | **0.0** |

### Scope is contained

Across all nine leagues in 2026:

```
league                        rules  w/ovr  ovr=0  verdict
Winfield_Football                46     27      0  slot scoring
Weenieless_Wanderers             39     20      0  slot scoring
GOP_Degenerates                  48     26      5  MISREAD as base pts
Knights_FFL                      46     27      0  slot scoring
12 Dudes one Cup                 43     27      0  slot scoring
Big Red Fantasy Football         37     19      0  slot scoring
John_PC_League                   53     28      0  slot scoring
John_ATL_League                  51     28      0  slot scoring
Washed_Up_Fijians                44     25      0  slot scoring
```

All nine use slot-16 overrides, so all nine depend on that code path — but only
GOP has any set to `0.0`, because it is the only league with IDP roster slots.
The other eight are correct in practice.

### The existing workaround is stale

`proj_to_score` already knows about this and hardcodes GOP's values
(`projection_utils.py:358-407`), splitting the frame into IDP and non-IDP and
re-scoring each. Those constants match **neither** season's actual settings:

| rule | hardcoded (DP) | 2025 base | 2026 base |
|---|---|---|---|
| 99 Sack | 10 | 12.0 | 5.0 |
| 95 Interception | 12 | 9.0 | 6.0 |
| 108 Solo Tackles | 2 | 2.0 | 1.5 |

It is also keyed on a bare league id (`if 1727104 in ...`), so it silently does
nothing if GOP's id ever changes, and cannot generalise to a second IDP league.

## Fix

**1. Read `pointsOverrides` directly** rather than through
`league.settings.scoring_format`. `build_scoring_table` already has
`league.cookies` and `league.endpoint` available; the `mSettings` view carries
`scoringSettings.scoringItems` with the full override map. Use `is None` rather
than a falsy check.

**2. Give the registry a `slot` dimension.** One row per (rule, slot class), with
`slot='base'` for the configured base value and `slot='<id>'` per override. That
keeps the file tidy and makes the IDP/D/ST split data rather than code.

**3. Have `proj_to_score` select by slot** instead of hardcoding, and delete the
`1727104` block. The frame is already split on `primaryPosition`; the change is
to look up the slot-appropriate rows rather than patch constants.

**4. Keep it general.** Nothing should reference a specific league id.

## Risk

This changes GOP's IDP projections, and probably by a lot — the hardcoded sack
value is 10 against a 2026 base of 5. Run the Phase 0 equivalence harness and
diff GOP's `TRUE_Points` before and after, per position, rather than trusting the
tests alone.

Worth doing before the draft if GOP is being drafted, since IDP values feed
position scarcity. The other eight leagues are unaffected, so it can be sequenced
independently of them.

## Verification

- Raw ESPN `pointsOverrides` for every league-season round-trips through the
  registry unchanged, including zeros.
- The eight non-IDP leagues produce byte-identical scoring tables before and
  after — this must be a no-op for them.
- GOP's D/ST sack value is `0.0` for 2025 and `1.0` for 2026, matching the raw
  settings rather than the base.
- `grep -rn 1727104 Scripts/` returns nothing.
