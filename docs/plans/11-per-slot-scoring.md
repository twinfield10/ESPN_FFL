# 11 — Scoring is per-lineup-slot, and the pipeline treats it as flat

**Priority:** High · **Effort:** Medium · **Status:** Not started · **Claims
verified live 2026-08-05**
**Found by:** [10 (scoring registry)](10-scoring-registry.md)
**Affects:** GOP_Degenerates only, but materially — IDP projections inflated ~2-3x

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

Two candidate bugs. **Only the second one actually bites** — see the correction
below.

**1. Only slot 16 (D/ST) is read.** Every other slot's override is discarded.

> **Corrected 2026-08-05, verified live.** This has **no practical impact here.**
> Slot `16` is the *only* key present in `pointsOverrides` across all nine
> leagues for 2026 — there are no other slots' overrides to discard. The earlier
> claim that "individual-defensive-player scoring is simply unavailable" was
> wrong: IDP scoring is carried by the base `points` value, which `espn_api`
> already exposes. So the fix does not need a general slot-map reader, only a
> correct two-way split between `base` (IDP) and `override{16}` (D/ST).

**2. An override of exactly `0.0` falls through to the base.** `0.0 or 12.0`
evaluates to `12.0`. So a rule the commissioner explicitly zeroed for D/ST comes
back carrying the IDP value. **This is the real bug**, and it affects 5 of
GOP's 48 rules.

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

Re-verified live on 2026-08-05: the table above reproduces exactly, and slot `16`
is the only override key in any of the nine.

### The existing workaround is stale

`proj_to_score` already knows about this and hardcodes GOP's values
(`projection_utils.py:568-634`, inside `proj_to_score` at line 561), splitting
the frame into IDP and non-IDP and re-scoring each. Those constants match
**neither** season's actual settings:

| rule | hardcoded (DP) | 2025 base | 2026 base |
|---|---|---|---|
| 99 Sack | 10 | 12.0 | 5.0 |
| 95 Interception | 12 | 9.0 | 6.0 |
| 108 Solo Tackles | 2 | 2.0 | 1.5 |

**Measured 2026-08-05 — the workaround is worse than "stale".** Full comparison
of the hardcoded constants against live 2026 settings, where truth for an IDP
slot is `base` and truth for D/ST is `override{16}`:

| id | base (IDP truth) | ovr16 (D/ST truth) | hardcoded DP | hardcoded D/ST |
|---|---|---|---|---|
| 95 Interception | 6.0 | 2.0 | **12** ✗ | **1** ✗ |
| 97 Fumble rec. | 4.0 | 2.0 | **2** ✗ | **1** ✗ |
| 99 Sack | 5.0 | 1.0 | **10** ✗ | 1 ✓ |
| 107 Assist tackle | 0.5 | 0.0 | 0.5 ✓ | *unset* ✗ |
| 108 Solo tackle | 1.5 | 0.0 | **2** ✗ | *unset* ✗ |
| 112 | 1.5 | 0.0 | **5** ✗ | 0 ✓ |
| 113 | 1.5 | 0.0 | **5** ✗ | 0 ✓ |
| 109 | *not in this league* | — | — | 0 (dead write) |

Six of seven DP constants are wrong, most by 2-3.3x (112 and 113 at 5 against a
true 1.5). GOP's IDP projections are therefore inflated roughly **2-3x**, which
badly distorts position scarcity at the draft.

Two further problems this surfaced, neither in the original plan:

- The block **overwrites rules `espn_api` already had right.** Ids 95, 97 and 99
  have non-zero overrides, so `espn_api` returns the correct D/ST value — and the
  hardcoded D/ST pass then replaces 2.0 with 1. It corrupts working data.
- Ids 107 and 108 are the two rules whose D/ST override *is* `0.0`, i.e. exactly
  the ones bug 2 breaks — and the D/ST pass never sets them, so they keep the
  wrong base value. The workaround misses the only case it needed to catch.
- The IDP branch also ends with
  `dp_df['TRUE_Points'] = (dp_df['ESPN_Points'] + dp_df['BOL_Points']) / 2`,
  a hardcoded two-source average that bypasses the renormalised blend from
  [plan 03](03-projection-source-coverage.md) entirely.

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
keeps the file tidy and makes the IDP/D/ST split data rather than code. In
practice `<id>` is only ever `16` today, so this is a two-value dimension — but
storing it generally costs nothing and survives ESPN adding slots.

**3. Have `proj_to_score` select by slot** instead of hardcoding, and delete the
`1727104` block. The frame is already split on `primaryPosition`; the change is
to look up the slot-appropriate rows rather than patch constants. Resolution rule:
a player in a D/ST slot takes `override{16}` where present and `base` otherwise;
every other slot takes `base`.

**4. Keep it general.** Nothing should reference a specific league id. Derive
"does this league have IDP slots" from `league.settings.roster_slots`, not from
an id list.

**5. Restore the blend for IDP rows.** Delete the hardcoded
`TRUE_Points = (ESPN_Points + BOL_Points) / 2` in the DP branch and let the
renormalised blend from [plan 03](03-projection-source-coverage.md) apply, as it
does for every other position. Note BetOnline is the only source with defensive
stats, so renormalisation should collapse to roughly ESPN+BOL on its own — the
point is that it does so by measured provenance rather than assumption, and stops
silently ignoring a third source if one ever appears.

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
