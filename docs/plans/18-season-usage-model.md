# 18 — The season usage model (pre-season / draft head)

**Priority:** High (seasonal) · **Effort:** M · **Status:** **Built and backtested
2026-08-07**, not yet in `WEIGHTS`. `Scripts/usage/{features,season,backtest}.py`,
45 tests. Walk-forward 2019–2025 beats the naive draft heuristic on ordering at
RB/WR/TE and on top-N hit rate at all four positions; it does not improve yardage
and slightly hurts QB ordering. §Backtest results has the numbers. **G2 as written
still cannot be measured historically** — it needs the four-source blend, which
does not exist for a past season — so the 2026 board is the gate.
**Depends on:** [16](16-usage-data-layer.md) — Step 0 gates and the feature layer ·
[15 (draft board)](15-draft-board.md) — done
**Supersedes:** [17](17-draft-usage-model.md)
**Feeds:** [09 (draft views)](09-frontend-draft-views.md) ·
[03 (weight re-tune)](03-projection-source-coverage.md)

## What it is for

The draft board values players on `TRUE_Points`, a blend of four projection
sources that are all somebody else's model output and demonstrably correlated —
plan 03 measured real, non-imputed coverage at ESPN 100%, FantasyPros 13%,
BetOnline 12%, Pinnacle 8%.

This head adds observed usage to that blend for the pre-season universe, and
gives the board a **usage column set worth showing**: target share, route share,
snap share are what a drafter actually wants next to a projection, and no current
source has them.

A draft board is also forgiving in a way a weekly lineup is not: it wants
relative ordering over ~200 players, not a precise point estimate for one.

## Set expectations honestly first

[Plan 16 §Measurements](16-usage-data-layer.md#measurements-that-decide-the-design)
measured the season-level case, and it is weaker than the earlier plan 17
assumed. Over 2,252 player-season pairs, predicting next season's points per game:

| Predictor | r with next-season PPG |
|---|---|
| this season's actual PPG | **+0.792** |
| this season's expected PPG | +0.779 |

**Expected production does not beat actual production at season level.** What it
does do is hold up better as a *metric*: expected PPG is more stable year over
year (+0.816) than actual PPG (+0.792), and its inputs are far more stable still
— carries/game +0.915, air-yards share +0.903, WOPR +0.885, target share +0.858,
against TD rate +0.234.

So the realistic contribution of this head is **variance reduction and better
within-position ordering**, not a step change in point estimates. That is worth
having on a board, and it is why the effort is scoped **M rather than L** and why
[plan 19](19-weekly-usage-model.md) — where trailing expected production *does*
beat trailing actual production — is where the larger edge lives.

Plan 17 claimed the reverse. It was written before the measurement existed.

## Four decisions, fixed

**1. It emits stat lines, not points.** Non-negotiable, and it falls out of the
architecture rather than being a preference: projecting stat lines and scoring
them per league is what lets one pipeline serve a 6-team standard league, a
16-team IDP league and a superflex. So the model produces `USG_rushingYards`,
`USG_receivingReceptions`, `USG_passingTouchdowns`, which `proj_to_score` prices
nine different ways. A points model would need refitting per league.

**2. Volume × efficiency, not yards in one step.** Justified by the stickiness
table in plan 16: opportunity sits at r ≈ 0.86–0.92 year over year, TD rate at
0.234. Modelling them jointly lets the noisy half contaminate the predictable
half and leaves you unable to tell which half was wrong when a projection misses.

**3. It enters the blend as a fifth source, not a replacement.** A `USG` entry in
`WEIGHTS`, a loader beside `load_fantasypros_season` / `load_betonline_season` /
`load_pinnacle_season` in `Scripts/season_projections.py`, and `USG` added to
`proj_to_score`'s `col_pfix_list`. It competes on measured accuracy rather than
being asserted to be better.

**4. It abstains where it cannot speak.** Kickers and team defences have no usage
features at all. For those it emits nothing rather than a positional default —
plan 07 made a wholly-absent source degrade correctly (imputed from `MEAN_`,
flagged, renormalised out of `TRUE_*`), so an abstaining `USG_` behaves exactly
like pre-season Pinnacle does today.

Point 4 is the one most likely to be tempting to skip. A model that quietly emits
a positional average will look like it has full coverage and will drag the blend
toward the mean for exactly the players a draft board most needs to differentiate.

## The shape

```
Data/NFL/<season>/{player_weeks,opportunity,routes,injuries,snap_counts}.parquet
        │  (gsis_id)
        │  ── Scripts/crosswalk.py ──►  espn_id
        ▼
  Scripts/usage/features.py    prior-season aggregates, plan 16 §Feature layer
        │
        ├── availability head ──►  expected games played
        ├── opportunity model ──►  expected targets / carries / routes / snaps
        └── efficiency model  ──►  yards per opportunity, catch rate, TD rate
                    │                (shrunk to positional baselines)
                    ▼
        per-game stat line × expected games  ──►  USG_<stat> season line
                    │
                    ▼
  build_season_projections  ──►  blended with ESPN/FP/PINNY/BOL  ──►  TRUE_Points
                    │
                    ▼
              build_board  ──►  VOR, tiers, value
```

`Scripts/usage/season.py` holds the model. Fitted coefficients persist with
metadata — version, date, training range, metrics — per `CLAUDE.md`.

### Expected games played

New relative to plan 17, and it comes straight out of plan 16's injury
measurement. Season projections are per-game production × games, and games is
not 17 for everyone. The availability head (shared with
[plan 19](19-weekly-usage-model.md)) supplies a prior-season durability estimate;
pre-season it has no current injury report to read — **confirmed live 2026-08-07:
nflreadr refuses `load_injuries(2026)` entirely** — so it falls back to trailing
games-missed and roster status.

Keep it visibly separate from production. "18 points per game × 14.2 games" is
auditable; a single 256-point number is not.

#### Measured 2026-08-07, and it is weaker than this plan assumed

Predicting next season's games played, over 6,599 QB/RB/WR/TE player-season pairs
from 2016–2025 (`Scripts.usage.context.season_availability`):

| Predictor | r | MAE (games) |
|---|---|---|
| everyone plays a full slate | — | 10.284 |
| everyone plays the pool mean (6.72) | — | 5.749 |
| **this season's games played** | **+0.663** | **3.543** |

That looks like a strong result and it is mostly an artefact of the population.
Restricted to players who managed 8+ games — **the population a draft board
actually ranks** — it collapses:

| Predictor, 8+ games (n=3,216) | r | MAE (games) |
|---|---|---|
| everyone plays a full slate | — | 6.411 |
| everyone plays the group mean (10.59) | — | 4.767 |
| this season's games played | **+0.343** | **4.382** |

**So the full-pool correlation of +0.663 is largely a role signal, not a durability
signal** — it is separating deep reserves who played twice from starters who played
sixteen times. Once you condition on being a rotation player, a player's own prior
games-played beats simply assuming the group mean by 8% of MAE, on r = 0.343.

Three consequences, and they are design decisions rather than caveats:

1. **Shrink hard toward the role mean.** An expected-games head that trusts a
   player's own prior season will be confidently wrong. The prior should be the
   positional/role mean with the player's own history moving it a little.
2. **Do not let expected games carry the model.** Its honest contribution is
   trimming the tail — the player who missed nine games last year — not
   differentiating the top of a position, where everyone's estimate is ~15.5.
3. **`weeks_on_reserve` is worth carrying** (r = −0.462 to next-season games
   played), because it separates "hurt" from "healthy and benched" in a way
   appearance counts alone cannot. That is the caveat plan 16 recorded against its
   own injury table, and roster status is the fix.

Two arithmetic traps found building this, both silent: counting a team's *calendar*
weeks as its games (rosters carry a bye-week row, so a player who never missed a
game came out 106% available), and leaving the counters as the unsigned integers
`len` returns (one subtraction that should have gone negative produced 4,294,967,295
games missed, in a frame whose `describe()` looked ordinary). The denominator now
comes from `player_weeks`' distinct (team, week), which reproduces the real
exception: 16 games for exactly two teams in 2022, Buffalo and Cincinnati.

### Rookies

Plan 17 left this open. Decide it by measurement rather than argument: fit both
arms on the 2016–2025 walk-forward and keep whichever wins.

- **Draft capital** — `load_draft_picks(2010:2026)`, 4,350 × 36. Draft position is
  the standard rookie usage proxy. `load_combine` (5,710 × 18) is weaker but free.
- **Abstain** — let the other four sources carry rookies, which is honest and is
  probably the right v1.

Rookies are a large share of draft-day uncertainty, so a wrong confident answer
here is costly. Abstention is the safe default and the burden of proof is on the
draft-capital arm.

### Team context changes

Usage is sticky *for a player in a stable situation*. A new offensive
coordinator, a departed target-hog, a change of team — the model will be
confidently wrong about exactly the players whose value moved most.

Mitigations, all from plan 16's context family:

- `coaching_staff.parquet` supplies head-coach and OC change flags. Note the
  limitation recorded there: **OC ≠ play-caller**, and no free source resolves it.
- Team-level scheme proxies — pass rate over expected, pace, personnel and
  formation rates — measure what the play-caller does rather than who they are.
- Departed-teammate target share is computable from the prior season's roster.

Where a player changed team, the honest move is to widen the interval rather than
adjust the point estimate, and to surface that on the board.

## How the board uses it

**No change to `build_board`.** It already values whatever `points_column` it is
given, so the model reaches it entirely through `TRUE_Points`. What the board
gains:

- **Better ordering**, if the model earns its weight (G2 below).
- **A usage column set worth showing** — target share, route share, snap share.
  [Plan 09](09-frontend-draft-views.md) renders them.
- **A basis for floor/ceiling.** Source disagreement plus prior-season variance,
  which the market pull already carries as `prior_season_points`.

## Backtest

Walk-forward, no exceptions. For each season *S* in 2019…2025: train on
2016…*S*−1, predict *S*, score against realised *S*.

**Baselines it must beat.** A model is only worth its complexity against the
cheap thing it replaces:

1. prior-season points per game (the naive draft heuristic)
2. ESPN's own season projection alone
3. the current four-source `TRUE_` blend

**Metrics, in priority order:**

| Metric | Why |
|---|---|
| **within-position Spearman** vs realised season points | what a board actually consumes — ordering, not level |
| per-stat MAE / RMSE | diagnoses which half of volume × efficiency is wrong |
| per-league fantasy-point MAE via `proj_to_score` | the nine leagues price the same stat line differently |
| top-N hit rate (RB1/WR1/TE1 tiers) | draft value concentrates in the top of each position |

Report all of them for the blend **with and without** `USG_`, not for `USG_`
alone. The question is never "is the usage model good" but "does adding it help".

## Backtest results — measured 2026-08-07

`python -m Scripts.usage.backtest`. Train on 2016…*S*−1, predict *S*, for *S* in
2019…2025. Priced with Winfield Football's own scoring per season, which is the
**only** league with a registry entry for every season — the others were recorded
from 2023 or 2024, so picking one would have silently shortened the walk-forward.

**Baseline 2 and 3 are not measurable.** ESPN's own past season projections and the
four-source `TRUE_` blend cannot be reconstructed: FantasyPros' URLs take no season
parameter and only BetOnline's season-long archive survives. So the comparison here
is against **baseline 1, the naive draft heuristic** — last season's per-game
production carried forward — which is what a drafter does by default and is the one
that matters. The naive arm is given the *same* expected-games estimate as the model,
so it cannot lose on availability rather than on production.

**Within-position Spearman** against realised season points, pooled:

| Pos | n | USG | naive | Δ |
|---|---|---|---|---|
| QB | 513 | 0.6877 | 0.7038 | **−0.0161** |
| RB | 1,021 | 0.7121 | 0.6902 | **+0.0218** |
| WR | 1,494 | 0.7526 | 0.7399 | **+0.0127** |
| TE | 801 | 0.7259 | 0.6976 | **+0.0284** |

**Per-stat MAE**, on rows the model speaks for:

| Stat | n | USG | naive | Δ |
|---|---|---|---|---|
| receivingTouchdowns | 2,732 | 1.27 | 1.45 | **−12.4%** |
| passingInterceptions | 1,301 | 1.19 | 1.35 | **−12.0%** |
| rushingTouchdowns | 2,393 | 0.96 | 1.00 | −4.2% |
| receivingYards | 2,732 | 141.30 | 146.61 | −3.6% |
| receivingReceptions | 2,732 | 12.46 | 12.92 | −3.5% |
| rushingYards | 2,393 | 89.26 | 89.00 | +0.3% |
| passingTouchdowns | 1,301 | 2.15 | 2.13 | +0.7% |
| passingYards | 1,301 | 302.03 | 297.92 | +1.4% |

**Top-N hit rate**, computed per season and averaged:

| Pos | N | USG | naive |
|---|---|---|---|
| QB | 12 | 0.607 | 0.595 |
| RB | 24 | 0.619 | 0.595 |
| WR | 36 | 0.683 | 0.627 |
| TE | 12 | 0.512 | 0.452 |

### Reading it honestly

**The gains are where the shrinkage is, and that is exactly what plan 16 predicted.**
Touchdown rates and interception rates improve 12%, because a player's own rate is
mostly noise (year-over-year stickiness +0.234) and pulling it to the positional
baseline is most of the available edge. Volume-driven yardage does *not* improve —
prior-season volume carried forward is already close to the best cheap estimate, and
the fitted regression matches rather than beats it.

**Top-N improves at all four positions**, which is the metric closest to what a
board is for, and by more than the Spearman moved: WR +0.056, TE +0.060.

**QB ordering gets slightly worse.** The quarterback arm is the weakest — 119
rostered quarterbacks, `pass_attempts_pg` R² 0.35, and the passing stats are the
three where MAE regressed. A defensible v1.1 would abstain for QB.

**Coverage is 57.8%** — 3,829 of 6,620 rostered player-seasons. The 42% it says
nothing about are overwhelmingly players with no prior season, including all 1,497
rookie rows. That is the designed behaviour, and the blend's absent-source path
handles it.

### The fitted expected-games head, and what it says

| Pos | n | const | p1_games | reserve | team change | R² |
|---|---|---|---|---|---|---|
| QB | 468 | 3.269 | 0.643 | 0.235 | −2.272 | 0.437 |
| RB | 958 | 4.087 | 0.555 | 0.198 | −1.150 | 0.191 |
| TE | 731 | 4.593 | 0.534 | 0.151 | −0.649 | 0.224 |
| WR | 1,333 | 4.828 | 0.535 | 0.161 | −1.691 | 0.189 |

**The slope on prior games comes out at 0.53–0.64, not 1.0** — the model estimates
about 45% shrinkage rather than having it asserted, which is the same conclusion the
r = +0.663 / +0.343 split argued for. Changing team costs 0.6 to 2.3 games. R² of
0.19 for a skill position is the honest ceiling on this: **availability is mostly not
predictable**, and the head's job is trimming the tail rather than differentiating
the top, where every estimate lands within a game of 14.

Two things went wrong building this and both are worth recording:

- **Shrinking toward a positional constant was wrong**, and wrong in a way that
  looked fine. The mean games of every rostered player is QB 8.2 / RB 9.8 / TE 9.3 /
  WR 10.2, dragged down by the majority who barely play, so a genuine starter was
  projected for eleven games. Conditioning the mean on having been on a roster the
  prior season, and then regressing rather than shrinking, fixes it.
- **`availability` and `games` cannot both be regressors.** Availability *is*
  games ÷ slate. Fitting both produced RB +1.056 on games against −10.160 on
  availability, and QB +0.052 against +8.620 — offsetting coefficients that cannot
  be read and would not transfer. `weeks_on_reserve` replaced it and is not
  mechanically tied to appearances.

Read the volume coefficients as pairs, not individually: `p1` and `p2` volume
correlate strongly, so the split between them is partly arbitrary while the
prediction is sound (WR targets: p1 0.709, p2 0.051, R² 0.628).

## Ship criteria

Inherited from
[plan 16 §Step 0](16-usage-data-layer.md#step-0--the-gates-measured-2026-08-06),
plus this head's own:

- ~~**G0** — usage residuals materially less correlated with ESPN's than ESPN's are
  with FantasyPros'.~~ **Passed 2026-08-06**, +0.832 against +0.988.
- **G2** — within-position Spearman against realised season points improves over
  `TRUE_` alone, on the walk-forward. **This head owns the real test.** Plan 16's
  step 0 could only run a proxy — summed weekly projections, which failed at −0.009
  to −0.063 by position — because 2025's *pre-season* season-long projections cannot
  be reconstructed: FantasyPros' URLs take no season parameter and only BetOnline's
  season-long archive survives. The 2026 board is the first clean measurement.

  **Still open, and now known to be unmeasurable on history.** §Backtest results
  compares against the naive heuristic because there *is* no historical blend to
  compare against — a permanent limitation of the data, not a gap in the work. G2
  gets its answer from the 2026 board: build it with and without `USG_` in `WEIGHTS`
  and score both against realised 2026. Until then `USG_` stays out.
- **Rookie arm** — ships only if draft capital beats abstention on the same
  walk-forward. **Not attempted.** v1 abstains, which the backtest confirms is 1,497
  of 6,620 rows saying nothing rather than guessing.
- **A QB arm** — new, and the backtest is the reason. Quarterback ordering is
  slightly *worse* with the model (−0.0161 Spearman) and the three passing stats are
  the only ones whose MAE regressed. Abstaining for QB is the defensible v1.1.

If G2 fails, **do not wire it in at a token weight.** Record the numbers in this
document and stop. A source that does not improve ordering but does add a fifth
name to `WEIGHTS` makes the blend harder to reason about for nothing.

## Steps

1. ~~Plan 16 Step 0 and its feature layer.~~ **Done** — step 0 on 2026-08-06,
   `R/GetContext.R` + `Scripts/usage/context.py` and
   `Scripts/usage/features.py` on 2026-08-07.
2. ~~`Scripts/usage/season.py` — prior-season aggregation, opportunity and
   efficiency heads, expected games, `USG_<stat>` season line.~~ **Done.**
   Coefficients persist to `Data/NFL/models/season_usage_<version>.json` with
   version, fit timestamp and training range.
3. Loader + `WEIGHTS` entry + `proj_to_score` prefix, following the
   `load_pinnacle_season` pattern in `Scripts/season_projections.py`. **Blocked on
   G2** — deliberately. The model exists and is measured; wiring it into the blend
   before the 2026 board answers G2 would be asserting the thing that is meant to be
   tested.
4. ~~Walk-forward backtest; write the table into this document.~~ **Done** —
   `python -m Scripts.usage.backtest`, §Backtest results.
5. Rookie arm, measured against abstention.
6. A QB arm, or abstention there — see §Ship criteria.
7. Hand the enlarged source set to
   [plan 03](03-projection-source-coverage.md)'s weight re-tune.

## Risks

- **The measured season-level case is modest.** Stated up front so the result is
  not a surprise. If G2 comes back flat, that is a real outcome and the weekly
  head is still worth building.
- **Nine leagues, one model.** Emitting stat lines keeps that true. Any temptation
  to tune against one league's points breaks it.
- **Pre-season has no current-season data at all**, so this head is entirely
  prior-season extrapolation. That is legitimate — the stickiness table says so —
  but it means the model is weakest exactly where situations changed.
