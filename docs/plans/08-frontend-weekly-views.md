# 08 — Local frontend: week-to-week views

**Priority:** High · **Effort:** Medium · **Status:** Not started
**Depends on:** [07 (foundation)](07-frontend-foundation.md)

## Goal

Port what the notebook and the Google Sheet already show into pages, and fix the
parts that were awkward in both. The notebook is the reference for *content*;
this is not a literal port of its layout.

Every page reads the store from plan 07. No page calls ESPN.

## What exists today, and where it lives

| Notebook | Sheets tab | Becomes |
|---|---|---|
| cell 15 `check_week` | `Lineup` | **My Matchup** |
| cell 16 `get_league_projections` | `League_Projections` | **League Slate** |
| cell 17 `get_rankings` | 8 × `FA_*` tabs | **Free Agents** |
| cell 13 `peek_proj_stats`, cell 12 spanned table | — | **Player Explorer** |
| cells 14, 34-36 (R², positional variance) | — | **Projection Accuracy** |
| cell 22 `django_simulation` | — | **Playoff Odds** |
| cells 39-40 power rankings, luck index | — | **Standings** |
| cells 24-28 history, `h2h_build` | — | **History** |

## Pages, in build order

### 1. My Matchup — the Sunday-morning page

Backed by `check_week()`. Your starters and bench with actual, ESPN, FP,
Pinnacle, BetOnline and blended points side by side, plus the diff column.

Beyond what the Sheet did:

- **Start/sit deltas.** `analytic_utils.get_best_lineup()` and
  `get_best_proj_lineup()` already compute the optimal lineup. Surface the
  specific swaps and the points they'd gain — that's the actionable bit, and
  today it's buried in an efficiency score.
- **Opponent side by side**, from `get_opp_lineup()`.
- **Win probability**, reusing the `simulate_matchup` machinery in
  `simulation_utils`.
- Flag players whose sources **disagree sharply** — a wide ESPN/FP/book spread
  is a risk signal the single blended number hides. Plan 03's provenance flags
  make this honest by distinguishing real disagreement from imputed agreement.

### 2. League Slate

`get_league_projections()` — every team's projected total for the week, sorted,
with `point_diff` against ESPN's own projection. Add each matchup paired up so
it reads as a slate rather than a leaderboard, with projected margin.

### 3. Free Agents

Replaces the eight `FA_*` Sheet tabs with one page: position filter, a search
box, and the same per-source columns. `get_rankings()` already takes a position
list, so the tabs collapse into a multiselect.

Add:
- **Rest-of-season** value, not just this week, once plan 09's season
  projections exist.
- **Drop candidates** — your worst rostered player at that position alongside
  the best available, which is the actual waiver decision.
- Filter to genuinely rostered-available players (`percent_owned` is already on
  the ESPN `Player` object).

### 4. Player Explorer

The most useful notebook cell (13, `peek_proj_stats`) and the least accessible.
Pick a player, see every source's projected stat line side by side, plus their
weekly actual-vs-projected history.

`create_plotly_spanned_table` (cell 12) groups columns by source prefix with
colour bands. `st.dataframe` `column_config` covers most of this natively; keep
the plotly version only if the spanned header genuinely reads better.

### 5. Projection Accuracy

Cells 14 and 34-36, which currently have to be re-run by hand. R² per source,
overall and by position, plus the positional variance the markdown header at
cell 37 promises and never delivers (there's no code under it).

This is the page that tells you **which source to trust**, so it should directly
inform the blend weights in plan 03 rather than living as a separate curiosity.
Show measured per-source accuracy next to the configured weight — a visible
mismatch is the prompt to re-tune.

### 6. Playoff Odds

`simulation_utils.simulate_season()` plus `get_playoff_odds_df`,
`get_rank_distribution_df`, `get_seeding_outcomes_df` — all built, all currently
reachable only by running cell 22.

Simulation is slow, so **cache results into the store** during refresh rather
than running on page load. `playoff_odds_swing()` (what this week's result does
to your odds) is the compelling view and the most expensive; compute it in
`refresh.py`.

### 7. Standings

`league.power_rankings()` and `luck_index.get_season_luck_indices()`. The luck
index is a genuinely distinctive piece of work that is currently invisible —
show the seven factor contributions, not just the composite.

Note `luck_index.py` carries seven TODOs calling its own scaling "crude and
trash"; worth revisiting before giving it prominent screen space.

### 8. History

Cells 24-28. Efficiency vs projection-adherence scatter (the `adjust_text`
labelling in cell 28 is broken — its import is commented out at cell 0), plus
`h2h_build` for head-to-head records. Backed by `team_stats.parquet`.

## Cross-cutting

- **Shared table component.** Diff colouring, tier bands and per-source column
  grouping recur on most pages. Build once in `components/tables.py`; the
  `scale_dict` conditional-formatting logic in `write_to_google` is the
  reference for the colour scales.
- **Empty and pre-season states.** Pre-draft there are no lineups and
  `current_week` is 0 (clamped to 1). Every page needs a sensible empty state —
  this is the condition the app will actually launch in.
- **Polars.** New app code should be Polars per `CLAUDE.md`. The store is
  parquet, so the frontend can be Polars-native even though `clean_lineups` is
  still Pandas. Convert at the store boundary.

## Retiring the notebook

Once pages 1-8 exist, the notebook keeps only genuine exploration. The two
unproductionised models in cells 11 and 14 should move into
`Scripts/` as part of plan 03 rather than being ported to the UI.

Do not delete it — it's the scratchpad. But it should stop being load-bearing,
which it currently is for anything the Sheet doesn't cover.

## Verification

- Every page renders for all nine leagues, including the IDP and superflex ones
  and the league with no D/ST.
- `My Matchup` totals reconcile with the `Lineup` Sheet tab for the same week.
- `League Slate` totals match `get_league_projections` exactly.
- Pages render pre-season with no data and no traceback.
- Week switching stays under 100ms.
