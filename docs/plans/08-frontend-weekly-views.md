# 08 — Local frontend: week-to-week views

**Status:** IN PROGRESS

**Priority:** High · **Effort:** Medium · **Where it stands:** **Three of the eight
pages shipped 2026-09-07 as tabs** rather than as pages, via
[40](40-frontend-restructure.md): **My Matchup** became the *Roster* tab and the
*Matchup* tab between them, **League Slate** folded into Matchup, and **Free Agents**
is its own tab. The remaining five are sub-tabs of those four rather than pages of
their own, which is the structural point of the restructure — see
[What is left](#what-is-left-2026-09-07).
**Depends on:** [07 (foundation)](07-frontend-foundation.md) ·
**Feeds:** [40 (frontend restructure)](40-frontend-restructure.md) ·
[42 (weekly matchup odds)](42-weekly-matchup-odds.md)

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

  **The colour-scale half shipped 2026-09-10**, in `app/lineup_table.py`
  (`points_scales`, `points_fill`, `cell_fill`, `PointsScale`) rather than in a new
  `components/tables.py` — it is vocabulary both weekly renderers already speak, and
  `views/weekly.py` imports that module anyway. `LIVE` and `TRUE` are filled on
  Roster, Matchup and Free Agents, on one ruler per position group computed from the
  **unfiltered** league-week: Sheets' `0 → rostered non-zero median → league max`,
  with its white midpoint expressed as alpha 0 so one pair of colours works in both
  themes. Two things carried over exactly because they are load-bearing — the pivot
  excludes free agents (including them moves the RB pivot 25%) and the ceiling
  includes them.

  Three findings worth not re-deriving:

  1. **`LIVE` must be pooled into the same ruler as `TRUE`.** A ruler built from
     projections alone puts 15–50% of realised scores above its own ceiling — half of
     all kicker and D/ST weeks — because actuals are 2–3× more dispersed than
     projections. Pooling both makes the ceiling ≥ every value either column holds,
     so nothing clips; measured 0.0% at every position. Since `LIVE == TRUE` at
     `elapsed = 0`, the ruler needs no notion of what day it is.
  2. **The fill is blue/red, not the green/red used everywhere else here.**
     `advantage_fill` earns green/red because `ADV` prints a `%+` sign in every cell;
     a *level* has no sign, so the pair stands on hue alone. Measured through the
     dataviz validator, composited over both themes: green/red tops out at CVD ΔE 4.7
     and never reaches the 6–8 floor band, and below ~alpha 0.40 it fails the
     *normal-vision* floor too. Blue/red clears outright (12.2/18.7 light,
     14.2/21.2 dark). `DELTA_FILLS`'s note suggesting the **red** arm be swapped for
     blue is the worse swap — green/blue are both cool and never clear in light mode.
  3. **`TEAM` takes a real minimum where a position takes zero**, which is Sheets'
     own asymmetry and not an oversight: a lineup total is never near zero, so
     anchoring its red arm there spends the whole arm on a range no team occupies.

  The `TRUE` column was also unpainted on all ten **Sheets** tabs from 2026-09-09,
  when The Athletic was inserted and the three hardcoded `gradientRule` ranges were
  not updated alongside the `df.columns` assignments. `points_span` had been written
  for exactly that hazard and wired only to `numberFormat`; the gradients now use it
  too, pinned by `tests/test_sheets_renderer.py`.

  **Still open here:** the draft board's tier bands and per-source column grouping,
  which is what a real `components/tables.py` would be for.
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

---

## What is left, 2026-09-07

Three of the eight are built. What each of the other five now needs, and where it
lands:

| # | Page | Becomes | Blocked on |
|---|---|---|---|
| 4 | Player Explorer | a **Roster** sub-tab | nothing |
| 5 | Projection Accuracy | a **Roster** sub-tab, beside the blend weights it should inform | nothing |
| 6 | Playoff Odds | a **Matchup** sub-tab | porting `simulate_season` off the live `League` object, and caching it into the store during refresh rather than running it on page load |
| 7 | Standings | a **Matchup** sub-tab | `luck_index.py` carries seven TODOs calling its own scaling "crude and trash" — worth revisiting before it gets screen space |
| 8 | History | a **Matchup** sub-tab | **unblocked**: `team_stats.parquet` now exists for nine of ten leagues, which it did not when this plan was written |

Corrections to this plan, from building it:

- **`get_best_lineup` / `get_best_proj_lineup` could not be reused.** §1 proposed them
  for the start/sit deltas. Both take a live `espn_api.League`, which the app promises
  never to have in a render path, and both return a *float* — the optimal total — when
  the actionable half is which player to bench for which. `app/lineup.py` is the
  replacement, and it returns the swaps.
- **`simulate_matchup` could not serve the win probability** either, for a third
  reason beyond those two: it draws from the last six weeks of *actual* team scores, so
  it says nothing at all in week 1. [Plan 42](42-weekly-matchup-odds.md) fits a weekly
  dispersion instead.
- **A shared table component did land**, as `app/views/weekly.py` rather than
  `components/tables.py`. It carries the labels, the tooltips and — the part that
  earns it — the rule that a source the store marked absent is **dropped rather than
  shown**, because the blend imputes it from the ESPN/FantasyPros mean and an absent
  book would otherwise read as unanimous agreement.
- **`current_week` is 1 pre-season, and the empty states this plan asked for are the
  condition the app launched in.** Every tab has one and they are checked across all
  ten leagues.
- **The `weeks_present` week-switching this plan assumed does not exist.** That key is
  absent from every 2026 `meta.json`, so the Week control offered `[1]` and always
  would have. `session.available_weeks` reads the artifact instead.
