# 01 — Scoring rules are silently dropped

**Status:** COMPLETE

**Priority:** High · **Effort:** Small · **Where it stands:** Done

> **Implemented, with two corrections to the fix below.** The `221` rate is
> **0.064/yd, not 0.1** — ESPN applies the rule per game on a floor, so 0.1
> overstates a GOP kicker by ~2.4 pts/week. And the historical gap turned out to
> be id `74`, which was mappable. See [Outcome](#outcome).

## Problem

`build_scoring_table()` translates a league's ESPN scoring settings into
stat→points rows. Any stat id it doesn't recognise gets a **NaN** `colName`
rather than an error. That NaN then flows into `proj_to_score()`, which builds
column names as `f"{prefix}_{colName}"` → `"TRUE_nan"`, finds no such column,
and skips it.

So an unrecognised scoring rule is **silently ignored**. Projections come out
looking perfectly normal, just wrong. Nothing logs, nothing raises.

This matters more than it sounds because league scoring changes between seasons,
which is exactly when nobody is looking closely.

## Evidence

Diffing every league's 2025 vs 2026 settings, only one league changed scoring —
**GOP Degenerates** reworked its kicker rules:

| id | abbr | label | 2025 | 2026 |
|---|---|---|---|---|
| 214 | FGY | FG yards | 0.1/yd | *removed* |
| 221 | FGY50 | Every 50 FG Made yards | — | **5.0** |
| 79 | FGM40 | FG Missed (40-49 yards) | — | **-1.0** |

Both new rules are unmapped and produce NaN `colName` rows:

```
Rows build_scoring_table emits with a NaN colName:
   GOP_Degenerates   id=79    FGM40   pts=-1.0
   GOP_Degenerates   id=221   FGY50   pts=5.0
```

Every other league is clean *for 2026* once you account for the two mechanisms
that legitimately handle ids outside `score_to_lab_dict`:

- `repl_scoring` remaps "every N yards" rules `{8, 27, 28, 47, 48}` to per-yard
  decimals.
- ids `{201, 206, 209}` (FG 60+, 2-pt return, 1-pt safety) are filtered on
  purpose.

### This is not new to 2026

Running `build_scoring_table` across every league × every configured season
(60+ combinations) shows unmapped rules in the historical seasons too:

```
Winfield_Football     2016:1nan 2017:1nan 2018:1nan 2019:1nan  2020-2026: clean
Weenieless_Wanderers  2017:1nan 2018:1nan 2019:1nan            2020-2026: clean
GOP_Degenerates       2023-2025: clean                         2026:2nan
```

So any backtest or historical analysis touching those seasons has been scored
with a rule missing. Worth knowing before Phase 1 of the draft work uses
2016-2025 draft history to derive pick value — those `total_points` figures
inherit the same gap.

### It fails silently, it does not raise

Worth stating explicitly, because "the scoring table is broken" naturally reads
as *it errors*. It does not. Across all 60+ league-season combinations it
returned a table every time and raised nothing. The failure is entirely silent:
a NaN column name that downstream code skips. That is what makes it worth a
guard rather than a bug fix.

## Fix

**1. Map the two new ids** in `Scripts/scrape_player_stats.py`.

`221` is an "every N yards" rule and belongs in `repl_scoring`, not
`score_to_lab_dict`.

> **Correction.** The claim that "5.0 per 50 FG yards is exactly the 0.1/yd it
> replaced" is wrong, and using 0.1 would have introduced a second silent error
> while fixing the first. ESPN awards this rule **per game, on the floor of the
> yardage** — stat `221 == floor(stat 214 / 50)` held on 14/14 sampled
> player-weeks — so the sub-50 remainder is discarded every game. Measured across
> the 21 kickers with ≥300 FG made yards in 2025, the realised rate is
> **0.0642 pts/yd**. Will Reichard's 1,369 FG yards paid 20 × 5 = **100** points;
> 0.1/yd would have claimed 137.

```python
REPL_SCORING = {
    ...
    221: {'abbr': 'FGY', 'label': 'FG Yards', 'id': 214, 'points': 0.064},
}
```

A single linear rate cannot capture a floor exactly — high-volume kickers waste
proportionally less remainder (0.073/yd) than low-volume ones (0.050/yd) — but
it is unbiased at the observed yardage distribution, and it is the only shape
`proj_to_score` can express, since that function only multiplies a stat column
by a constant.

`79` is a genuinely new penalty and needs a missed-FG stat column. It resolves:
ESPN exposes `missedFieldGoalsFrom40To49` in both `points_breakdown` and
`projected_breakdown`, so `79` maps directly in `score_to_lab_dict`.

**2. Stop failing silently.** This is the part that matters long-term. Make
`build_scoring_table()` warn (or raise under a strict flag) when it produces a
row with a non-zero `points` and no `colName`:

```python
unmapped = league_scoring[league_scoring['colName'].isna() & (league_scoring['points'] != 0)]
if not unmapped.empty:
    warnings.warn(
        f"{league.name}: {len(unmapped)} scoring rule(s) not modelled and "
        f"silently excluded from projections: "
        f"{unmapped[['id','abbr','points']].to_dict('records')}"
    )
```

Note `Scripts/fetch_utils.py:16` calls `warnings.filterwarnings("ignore")`
globally, which would swallow this.

> **Resolved without touching that call.** The warning is emitted inside a
> `warnings.catch_warnings()` block that installs
> `simplefilter("always", ScoringCoverageWarning)`, which takes precedence over
> the global filter. This keeps plan 01 independent of both plan 06 and module
> import order, and avoids dragging plan 06's 54 `PerformanceWarning`s into this
> change. Plan 06 should still remove the global filter properly.

**3. Add a test** asserting no league produces unmapped non-zero scoring rules.
That turns "someone changed their scoring" into a red test instead of quietly
wrong projections. Landed as `tests/test_scoring_coverage.py` rather than in
`tests/test_projection_utils.py`, which is a pure-unit file — the all-league
check needs credentials and network, so it is marked `@pytest.mark.live` and
deselected by default.

## Outcome

**The historical gap was id `74`** (`FG50P`, "FG Made (50+ yards)", 5.0 pts) —
ESPN's older id for the stat `198` now covers. It was mappable, so the 2016-2019
Winfield_Football and 2017-2019 Weenieless_Wanderers seasons are now scored
correctly too. No league-season scores both `74` and `198`, so mapping both to
`madeFieldGoalsFrom50Plus` cannot double-count; a test documents that assumption.

All 60+ league-season combinations now report zero unmapped non-zero rules, and
no new duplicate `colName`s were introduced.

**GOP_Degenerates 2026 kicker projections**, per week:

| | before fix | after fix | plan's 0.1/yd |
|---|---|---|---|
| mean K projection | 7.65 | **11.72** | 14.07 |

The fix recovers **4.07 pts/week** that were being silently dropped. The plan's
0.1/yd would have overshot by 2.35 pts/week — about **40 points per kicker per
season** — so the correction matters at roughly half the scale of the original
bug.

### Also fixed: cross-league scoring contamination

Found while verifying the above, and the same class of silent failure. In
`espn_api` 0.45.1, `Settings.__init__` builds each scoring row from
`SETTINGS_SCORING_FORMAT_MAP.get(stat_id, ...)` and then writes `id` and `points`
onto the dict it got back — which is the *module-level* dict, not a copy. Every
`League` in a process therefore shares one set of scoring rows, and each new
fetch retroactively overwrites the points of every league fetched before it.

Fetching Weenieless_Wanderers after GOP_Degenerates silently changed GOP's
passing TD from **6.0 to 4.0**, plus six more rows. Anything holding two `League`
objects at once was scoring with another league's rules.

`fetch_utils.isolate_scoring_format()` now deep-copies the scoring format at
fetch time, pinning each league's values before another fetch can clobber them.
A `@pytest.mark.live` test guards it.

## Verification

```bash
pytest tests/                 # 43 pass, 2 live deselected
pytest tests/ -m live         # 2 pass — all-league coverage + no contamination
```
