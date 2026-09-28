# 51 — D/ST from opponent markets: the props add nothing, and two definitions were wrong

**Status:** IN PROGRESS

**Priority:** Medium · **Effort:** M · **Where it stands:** **Phases 0–2 done 2026-09-28.**
Step 0 refuted the plan's main hypothesis, phase 1 made the evidence reproducible
(`python -m Scripts.dst.evidence_51`), and phase 2 moved the model onto ESPN's definitions as
**D/ST 1.1.0**, and **each book's weekly D/ST line is now the D/ST model priced off that
book's own game lines** (phase 2c), so `PINNY_` and `BOL_` stop abstaining on every
defence. Phases 2b and 3–5 are owed.

The idea was to project each D/ST component
from the opponent's props: yards allowed from the opposing QB's passing line plus the
rushers' lines, interceptions from the QB's INT prop, sacks from the defenders' sack props
and the opponent's pass attempts. On a full season of 2025 BetOnline props (494 team-games,
leave-one-week-out), **props beat plain game lines on no D/ST component**, and adding them to
the lines improves nothing. What step 0 did find are two definition errors in what we score
*against*. Both are now reproduced exactly, 431/431 and 357/357, and the model now uses them.
Moving to the correct stat improves the yards tier sharply and exposes an over-projection
in the turnover rates that the old yards error had been hiding (see Phase 2 result).
**Depends on:** [30](30-dst-model.md) — the model this corrects ·
[36](36-sportsbook-scrapes.md) / [45](45-props-in-the-odds-store.md) — the odds store
**Feeds:** [28](28-outcome-distributions.md) · [42](42-weekly-matchup-odds.md) — a weekly
D/ST distribution

---

## The hypothesis

Plan 30 projects D/ST from two numbers per game: implied points allowed and the spread.
The books publish much more than that about the opposing offence, so the proposal was to
build each component out of the opponent's markets:

| Component | Proposed market input |
|---|---|
| Points allowed | Opponent team total at both books, plus Pinnacle's alternate spread and total ladders, turned into a points distribution (opp = (total − margin) / 2) |
| Yards allowed | Opposing QB passing-yards line + rushers' yards lines + a measured gap |
| Interceptions | Opposing QB's INT prop, read as a ladder mean |
| Sacks | Opponent pass attempts × sack rate; cross-checked against the sum of defenders' sack props |
| Fumble recoveries, defensive TDs | Opponent play volume from the attempts props; defensive TDs built from turnover expectations |

## What step 0 measured

### 1. Props do not beat game lines on any component (2025, out of sample)

`Data/Projections/BetOnline/Season/2025/BetOnline_AllProps.parquet` carries BetOnline's
weekly projections for all of 2025, weeks 1–17, with team and `NFL_game_id`. That is the
history the odds store (which starts 2026-08-27) does not have. Each defence-game row is the
opposing offence's actual line from 2025 play-by-play, predicted by OLS refit with each week
held out in turn. "Lines" is plan 30's pair: implied points allowed and the spread, both from
`vegas.team_games(use_book_quotes=False)`.

| Target (defence view) | n | sd | Constant | Lines | Props | Lines + props |
|---|---|---|---|---|---|---|
| Yards allowed | 494 | 84.3 | 84.33 | **76.79** (r 0.410) | 79.01 (r 0.346) | 76.90 |
| Interceptions | 492 | 0.85 | 0.855 | **0.850** (r 0.096) | 0.852 (r 0.078) | 0.852 |
| Sacks, from opp. attempts prop | 496 | 1.77 | 1.774 | **1.760** (r 0.121) | 1.772 (r 0.013) | 1.760 |
| Sacks, from summed defender props | 440 | 1.73 | 1.733 | **1.727** (r 0.088) | 1.732 (r 0.024) | 1.728 |
| Opponent dropbacks | 496 | 8.28 | 8.289 | 8.222 (r 0.119) | **8.022** (r 0.246) | 8.044 |
| Opponent fumbles lost | 496 | 0.66 | 0.663 | 0.664 | 0.663 | 0.663 |

(CV RMSE, lower is better.)

**Reading it:**

- **Yards: the props are a noisier copy of the line.** The sum of the QB and rushing props
  correlates 0.346 with yards allowed, against 0.410 for the lines, and adds nothing on top of
  them. The identity holds up as bookkeeping: actual net yards = QB passing prop + all
  rushing props + **11.7**, with a residual SD of 78.9. It just doesn't forecast.
- **INTs and sacks are nearly unforecastable weekly by anything.** The best model takes
  0.5% off a constant's RMSE for INTs and 0.8% for sacks. At a weekly SD of 1.77 sacks and
  0.85 INTs, game noise swamps any pre-game signal. Plan 30's season-level r of 0.464 for
  sacks is an average over 17 of these games, and that average is what the market prices.
  A weekly prop adds no information on top of it.
- **Dropbacks is the one place props win, and it doesn't carry through.** The attempts
  prop predicts dropbacks better than the lines (0.246 against 0.119), and dropbacks is the
  mechanism plan 30 named for game script. But sacks per dropback is noisy enough that the
  gain disappears one step later.
- **Two calibration problems in the 2025 prop projections, which don't change the verdict:**
  `proj_passingInterceptions` averages 0.492 against 0.699 actual, and summed
  `proj_defensiveSacks` covers 0.94 of 2.38 team sacks, with 3.6 priced defenders per team.
  Both would need rescaling, but the comparisons above are correlations and refit
  intercepts, so the scale doesn't matter to them.

**The 2026 weeks 1–3 check (91 team-games, odds store, main lines) agrees:** props sum
against net yards r = 0.233, residual SD 79.8, against 76.5 for an in-sample OLS on
BetOnline's team total. INT prop P(over 0.5) against actual INTs r = 0.160. Summed defender
sack ladders against actual sacks r = 0.003. Coverage is fine: priced QBs threw **96.6%** of
team passing yards and priced rushers (2.6 per team) ran **87.8%** of team rushing. The props
aren't missing the offence. They just can't predict one game of it better than the line
does.

### 2. ESPN's yards allowed is net scrimmage yards including kneels, and the model uses gross

For every 2025 defence-week where an ESPN yards-allowed tier is recorded in any of the six
stores that roster D/ST, the tier was compared with three candidate definitions from
play-by-play (`evidence_51.definitions`):

| Definition | Tier matches |
|---|---|
| **Net yards, pass + run + kneels + spikes (sacks negative)** | **357 / 357** |
| Net yards, pass + run only | 351 / 357 |
| Gross (player passing yards + rushing yards, no sack yards) | 264 / 357 (73.9%) |

`Scripts/dst/model.py:86` builds `yards_allowed` as the sum of player `passing_yards` +
`rushing_yards`, which is the **gross** row. It runs **15.1 yards a game high** on average
(342.0 against 326.8), and 26.1% of real team-games land in a different ESPN tier under
it. So the yards-allowed ladder has been fitted and scored against the wrong quantity since
plan 30.

### 3. ESPN's points allowed leaves out 6 per return TD and 2 per safety scored against the offence

| Definition | Tier matches | Of the 60 games with non-offensive opponent points |
|---|---|---|
| Opponent final score | 401 / 431 | 30 / 60 |
| − full return TD (7) against the offence | 414 / 431 | 43 / 60 |
| − defence and special-teams return TDs | 398 / 431 | 27 / 60 |
| − **6** per INT/fumble return TD against the offence | 428 / 431 | 57 / 60 |

(The two middle rows were scratch variants and are not in `evidence_51`, which keeps the
first, fourth and fifth.)
| **− 6 per return TD against the offence, − 2 per safety conceded by the offence** | **431 / 431** | **60 / 60** |

So ESPN charges the defence the conversion after a pick-six but not the six, charges
punt/kick/blocked-kick return TDs in full (those are the D/ST's own unit), and leaves out
safeties. On average the market's number, all points, runs **0.54 above** ESPN's.
`vegas.team_games` sets `points_allowed` to the opponent's final score, so plan 30's ladder
is fitted to a quantity that is slightly high and, in the tiers, wrong at 30 of 431
defence-weeks. The model then integrates the ESPN ladder over that wrongly defined
distribution.

## Phase 2 result — D/ST 1.1.0

`Scripts/dst/allowed.py` implements both definitions from play-by-play, and
`model.team_weeks` reads points and yards allowed from it instead of from the schedule's
final score and player gross yards. The model is refit as **1.1.0**
(`Data/NFL/models/dst_1.1.0.json`; 1.0.0 is kept beside it), and
`DST_SeasonProjections.parquet` for 2026 is rewritten.

**On identical 2026 lines, 1.1.0 against 1.0.0:** −17.4 yards allowed a game and −0.64
points allowed a game. Those are the measured definition gaps (15.1 and 0.54) passed through
the fitted slopes. The shutout tier moves +0.013 games a season.

**G-DST2(a) still passes, 8 of 8 scoreable leagues, 38.4–44.4% below prior-season MAE.**
The truth side of that gate is built from the same `team_weeks`, so it is now scored on
ESPN's definitions too. `jeffs_league` is new in 2026 and has no 2024 registry, which
crashed the gate CLI on `main`. `gates.scoreable_leagues` now skips it and the report names
it.

**The honest comparison, and the reason it is still the right change.** Scoring both
versions against the *same*, ESPN-correct truth (walk-forward 2024–2025):

| | 1.0.0 definitions | 1.1.0 definitions |
|---|---|---|
| Yards-tier points, `winfield_football` (MAE / bias per team-season) | 12.66 / −11.15 | **8.52 / −3.99** |
| Points-tier points | 6.00 / −0.88 | 6.11 / +1.37 |
| Total D/ST points MAE, per league | 22.49–23.66 | 23.14–24.61 |

The corrected yards tier is much better and points-allowed is flat, **but total D/ST points
MAE is 0.5–1.2 worse in every league**. That is within one standard error in seven of eight
leagues (paired, 64 team-seasons; `richardson_invitational` +1.02 ± 0.55). New is closer on
26–30 of 64 team-seasons.

The worse total is a **removed compensating error**. The rate components, which are
identical in both versions, over-project 2024–2025 by **+8.97 points per team-season**
(INTs +3.30, fumble recoveries +2.16, return TDs +3.0 across the three columns). The old
yards definition under-scored the yards tier by 11.15, which hid most of that. So 1.1.0
fixes the stat and exposes the next problem rather than creating one.

### Owed, found in phase 2

- **The turnover rates are fitted on a decade that no longer looks like the present.**
  INTs and fumble recoveries over-project the two most recent seasons. Candidates are a
  season trend term or recency weighting in `fit`. That's a new phase between 2 and 3, gated
  on walk-forward 2024–2025 rate bias, and it should come before any weight change.
- **Nothing rebuilds `DST_SeasonProjections.parquet` on a schedule.** The copy this replaced
  was built on older lines. The old model on today's lines differed from it by hundreds of
  yards a season. Same shape as [44](44-weekly-sources-and-coverage.md)'s finding 6.

## Phase 2c — each book's D/ST line, from that book's own lines

Neither book posts a D/ST market, so on the weekly board `PINNY_` and `BOL_` were imputed
from `MEAN_` and flagged on every defence, and the D/ST blend was ESPN plus whatever
FantasyPros had. But the D/ST model's only inputs are a spread and a total, which both
books post. So:

- `vegas.book_game_lines(season, book)` reads one book's main spread and main total per
  team-game (latest snapshot, full game), plus its own team total where it quotes one.
- `dst.model.project_games` prices those lines one game at a time, and `dst.books.weekly`
  wraps the two together for `"PINNY"` and `"BOL"`.
- `projection_utils.book_dst_rows` attaches the result to each book's weekly frame under
  ESPN's D/ST names, keeping only stats the blend has a `MEAN_` column for. Stats the model
  doesn't produce (blocked kicks, kick and punt return TDs) stay null, so the book abstains
  on those alone.

**It is one model read through two sets of lines**, which is the requested weighting and
worth stating: Pinnacle and BetOnline agree within **0.27** points on spreads and **0.38**
on totals over 2026 weeks 1–4, so on a D/ST row the model now carries two of the (up to)
four real votes.

Verified end-to-end on `winfield_football`: only the two D/ST rows not yet kicked off
changed (Eagles TRUE 7.41 → 6.83, Bears 3.75 → 4.49). All 416 locked rows held under the
freeze, and non-D/ST `TRUE_Points` moved by exactly 0.0.

### Four things building it found

1. **The season coefficients cannot price a single game.** Points allowed runs **1.347**
   per point of implied points allowed across team-seasons but **0.996** across games
   (2016–2025). Pushing one game through the season fit put Minnesota at 11.4 points
   allowed on a line implying 14.75. `fit` now also writes a per-game block
   (`model["games"]`) with its own coefficients and tier residuals measured around each
   game's own prediction. Held-out gain on 2024–25 games: points allowed +8.7%, yards
   +9.8%, against a constant.
2. **`book_team_totals` was averaging Pinnacle's first-half team totals into the full-game
   ones**, and every value a line had ever carried. Arizona read 15.0 and the Giants 15.5
   on a 43.5 total. It now filters to `gamePeriod == "GAME"` and takes the latest snapshot
   per book. `team_games(use_book_quotes=True)` feeds `team_strength`, so **every
   book-quoted implied total on the season path was about a third too low**. Seattle's
   season D/ST points allowed moves from 260.6 to 306.6 and its projected shutouts from
   0.84 to 0.37. The kicker's season projection reads the same function and will change on
   its next rebuild. It has not been rebuilt here.
3. **Fumble touchdowns were projected twice.** ESPN books every D/ST fumble TD under
   `fumbleReturnTouchdowns` (15 in 2025), with `fumbleRecoveredForTD` zero on every D/ST
   row, both actual and ESPN-projected. Eight of the nine leagues price both at the D/ST
   slot, and the model filled both with the same expectation, about +3 points a season per
   defence. It now writes zero there, and so does the gate's truth frame. G-DST2(a) MAE on
   `winfield_football` fell 23.1 → 22.0.
4. **nflverse's 2026 schedule lines disagree with both books by about 1.8 points on the
   spread** (Washington in week 4: both books −3, nflverse +1.5), while the books agree
   with each other within 0.27. They look like stale lookahead lines. `team_games` uses
   them wherever no book quote exists, and the season D/ST projection is built on them.
   Owed: find where `R/GetNFL.R` gets its lines and whether they refresh.

## What to build instead

The hypothesis is set aside rather than half-built. The weekly D/ST projection stays on
**game lines**, with the fixes the measurements point to:

1. **Fix the two definitions in `Scripts/dst/model.py`.** Yards allowed = net scrimmage yards
   including kneels, from play-by-play. Points allowed = opponent points − 6 × return TDs
   against the offence − 2 × safeties against the offence. Refit, and re-run plan 30's gates.
   Both definitions get a test pinned to the 2025 reproduction, so a regression shows up as a
   tier mismatch count rather than a vague drift.
2. **The points-allowed distribution is still worth improving.** Points allowed is the one
   channel where the market is strong (plan 30: r 0.816 season-level), and the tier edges
   (13/14, 17/18, 21/22, 27/28) sit on NFL key numbers. Replace the pooled empirical residual
   with a **score PMF conditional on the implied total**, fitted on nflverse closing lines
   2006–2025. It can be backtested on history today. Only once that clears its gate, tilt it
   toward de-vigged quoted team totals and Pinnacle's alternate ladders. Those exist only for
   2026, so that step is gated on accrued weeks, not history.
3. **Collect the opponent props anyway, but don't model from them.** They're already in the
   store (plan 45), which is all it costs. Re-run this backtest at the end of 2026 with ladder
   means from the store instead of 2025's `proj_` columns. That test would be fairer to the
   props, since ladder means are sharper than a single line, so this verdict stays open until
   it's run.

### Columns: what the weekly D/ST input frame needs

One row per defence per game, `Data/NFL/<season>/dst_inputs.parquet`, keyed
`season, week, game_id, def_team, opp_team`:

- **Lines:** `spread`, `total`, `implied_allowed_derived`, `opp_team_total_quoted`,
  `opp_team_total_book`, `implied_source`, `snapshot_ts`, `is_close`
- **Distribution (step 2):** `p_pa_le_{0,6,13,17,21,27,34,45}` on the ESPN definition, plus
  `pa_mean` and `pa_sd`
- **ESPN-definition actuals (training and scoring):** `pa_espn`, `pa_all`, `ret_td_vs_off`,
  `safeties_vs_off`, `yds_net_espn`, `yds_gross`, `sacks`, `ints`, `fum_rec`, `def_td`
- **Carried for the end-of-season re-test (not model inputs):** `opp_qb_pass_yds_ladder_mean`,
  `opp_qb_att_ladder_mean`, `opp_qb_int_ladder_mean`, `opp_rush_yds_priced_sum`,
  `n_rushers_priced`, `def_sack_ladder_sum`, `n_sackers_priced`

## Phases

| Phase | What | Gate |
|---|---|---|
| 0 | Measurements above | **Done 2026-09-28** |
| 1 | `Scripts/dst/evidence_51.py`: the step-0 tables, deterministic, from local data | **Done 2026-09-28**: reproduces 357/357, 431/431 and the backtest table |
| 2 | Fix both definitions in `model.py`, refit, re-run plan 30's G-DST gates | **G-51a: PASS 2026-09-28**, 100% tier match on both ladders and G-DST2(a) 8/8 |
| 2c | Each book's D/ST line from its own game lines | **Done 2026-09-28** — see Phase 2c |
| 2b | Recency or trend in the turnover rates | Walk-forward 2024–2025 rate-component bias within ±2 points per team-season, without losing G-DST2(a) |
| 3 | Key-number score PMF conditional on the implied total, replacing the pooled residual | **G-51b:** held-out-season log-loss on the ESPN PA tiers beats the pooled residual, 2016–2025 rolling |
| 4 | Tilt the PMF toward quoted team totals and Pinnacle ladders | **G-51c:** needs about 8 weeks of 2026; tier log-loss beats phase 3 on 2026 held-out weeks |
| 5 | End-of-2026 re-test of the props, from store ladder means | Pre-registered: props or lines + props must beat lines by ≥ 1% CV RMSE on yards, sacks or INTs, or the columns stop being carried |

## Reproducing step 0

`python -m Scripts.dst.evidence_51` prints the definition tables and the 2025 backtest.
`tests/test_dst_allowed.py` pins both definitions on a synthetic game and on ESPN's 2025
tiers. The 2026 weeks 1–3 check (name-matched odds-store props against play-by-play) was a
one-off and is not in the script, since the end-of-season re-test in phase 5 supersedes it.
