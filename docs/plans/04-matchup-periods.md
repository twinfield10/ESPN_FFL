# 04 — Multi-week matchup periods lose a week of data

**Priority:** Medium · **Effort:** Small · **Status:** Not started

## Problem

`get_ply_stats_by_matchup()` iterates `range(current_matchup_period)` and treats
each iteration as a week. That's fine while matchup periods map 1:1 to weeks —
but leagues with **two-week playoff matchups** break the assumption, because the
period number falls behind the week number.

There's a hardcoded workaround for exactly one league and exactly two weeks:

```python
# Scripts/scrape_player_stats.py:203
current_matchup_period = league.settings.week_to_matchup_period[league.current_week]
if league.league_id == 521152 and league.current_week in [15, 17]:
    current_matchup_period = league.current_week
```

Two problems. It's keyed to a literal league id, so any other league that adopts
two-week playoffs silently loses data. And **it misses week 16** even for the
league it targets.

## Evidence

Winfield_Football (521152) uses two-week playoff periods, identically in 2025
and 2026 — so this is still live:

```
matchup_periods: {'14': [14, 15], '15': [16, 17]}
week -> period:   14->14   15->14   16->15   17->15
```

Walking through the loop bound:

| Current week | period | loop covers | outcome |
|---|---|---|---|
| 15 | 14 | weeks 1-14 | would miss wk 15 → **hack forces 15** ✓ |
| 16 | 15 | weeks 1-15 | **misses wk 16** — not covered by the hack ✗ |
| 17 | 15 | weeks 1-15 | would miss 16, 17 → **hack forces 17** ✓ |

Every other league has 1:1 periods and is unaffected:

```
Knights_FFL:  week -> period   14->14  15->15  16->16  17->17
```

Winfield_Football is currently commented out of the `all` cohort in
`populateGoogleSheet.py`, which may well be a symptom of this.

## Fix

Derive the bound from the data instead of hardcoding. The loop wants *weeks*, so
iterate weeks and translate to periods where the API needs one:

```python
# every week that has been played, regardless of how periods group them
for week in range(1, league.current_week + 1):
    league.load_roster_week(week)
    period = league.settings.week_to_matchup_period[week]
    box_scores = league.box_scores(period)
    ...
```

Two things to watch:

- `league.box_scores` is memoised in `fetch_utils.fetch_league`, so two weeks in
  the same period return the same cached object. That's correct for a two-week
  matchup (the box score *is* the period), but any per-week attribution has to
  come from `load_roster_week`, not from the box score.
- Confirm whether scores in a two-week period are cumulative across both weeks
  or reported per week — that determines whether week 15 should be summed with
  week 14 or treated separately. Check a known Winfield_Football playoff result
  against the ESPN UI before trusting the output.

Then delete the `521152` special case.

## Verification

- Winfield_Football at week 16 returns 16 weeks of player rows, not 15.
- Knights_FFL output is unchanged (regression check — it has 1:1 periods).
- A known 2025 Winfield_Football playoff matchup matches the ESPN UI.
- Re-enable `Winfield_Football` in the `all` cohort and confirm a clean run.
