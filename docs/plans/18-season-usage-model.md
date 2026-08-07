# 18 — The season usage model (pre-season / draft head)

**Priority:** High (seasonal) · **Effort:** M · **Status:** **Built, backtested and
wired in as the fifth source 2026-08-07**, at weight **0.0**.
`Scripts/usage/{features,season,project,backtest}.py`, 84 tests. Walk-forward
2019–2025 beats the naive draft heuristic on ordering at RB/WR/TE; it does not improve
yardage, and **it now abstains for QB**, where it measured worse. **The rookie
draft-capital arm passed its gate decisively.** `USG_` reaches all nine boards and is
the best-covered source in the pre-season blend (23.1% real against ESPN's 13.1%);
adding it does not move `TRUE_Points` by design, verified at max difference 0.0 over
1,026 rows. §The fifth source, wired has the details. **G2 as written still cannot be
measured historically** — it needs the four-source blend, which does not exist for a
past season — so the 2026 board is the gate, and turning the weight on is one number.
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

#### Built and measured 2026-08-07 — the arm wins, and not narrowly

**No new pull was needed.** `load_draft_picks` was the obvious source and is the
wrong one: for **2026 it carries no real `gsis_id` at all** — 0 of 257 picks in
`00-00…` format against ~99% in every season 2010–2025 — so the join would have
fitted beautifully on history and returned nothing for the season that needs it. It
also uses PFR team codes (`LVR`, `KAN`, `GNB`). `rosters_weekly.draft_number` is
already in the feature frame, is the overall pick, and needs no join: verified
against the picks file on Fernando Mendoza (1) and Jeremiyah Love (3).

**Draft capital carries far more signal than this plan assumed.** Over 2,008 rookie
player-seasons 2017–2025:

| | n | % who played | mean games |
|---|---|---|---|
| drafted | 670 | **87.9** | 9.17 |
| undrafted | 1,338 | **21.2** | 1.08 |

and monotone by round — round 1: 13.2 games, 3.58 targets/g, 404 receiving yards;
round 7: 5.4 games, 0.90, 54; undrafted: 1.1 games, 0.24, 10. Within drafted
rookies, pick number against the volume that matters correlates **−0.59 (RB
carries), −0.60 (WR targets), −0.57 (TE targets), −0.57 (QB attempts)** — as strong
as the veteran arm's own predictors.

**The verdict, on the walk-forward, against a projection carrying no draft
information (the positional rookie mean):**

| Pos | n | ρ arm | ρ mean | Δ | MAE arm | MAE mean |
|---|---|---|---|---|---|---|
| QB | 150 | **0.5986** | −0.0115 | +0.610 | 26.02 | 55.03 |
| RB | 368 | **0.6156** | +0.0112 | +0.604 | 21.53 | 39.72 |
| WR | 676 | **0.6121** | −0.0345 | +0.647 | 14.28 | 29.51 |
| TE | 303 | **0.6175** | +0.0451 | +0.573 | 10.15 | 20.01 |

The arm orders rookies within position at ρ ≈ 0.61 where the uninformative guess
carries none, and roughly halves MAE. Calibration is slightly conservative, which is
the right direction: drafted rookies averaged 60.8 realised points against 51.0
projected, undrafted 3.0 against 0.7. **The arm ships.** Coverage goes from 57.8% to
80.4% of rostered players.

`log(pick)` for volume, measured: it beats linear at every position (R² 0.374 vs
0.352 RB carries, 0.417 vs 0.360 WR targets) and `1/pick` is worst. Undrafted is a
separate indicator rather than "pick 300", because it is a different population and
two of three rookies are in it.

#### Two things this arm got wrong first, both caught by looking at the output

**Games played is not log-linear in draft position.** It is flat across the early
rounds and then declines — RB means by round 13.2, 12.6, 10.6, 12.6, 10.0, 7.9, 6.9
— so a log fit extrapolated **21.7 games at pick 1**, clipped to 18. Searching a
shift parameter did not rescue it: the best shift ranged 0 to 60 by position, bought
at most 0.01 R², and still put pick 1 at 15.9. Replaced with a bin mean over
draft-capital groups, which cannot extrapolate past what rookies actually did:

Stored as a share of the slate since v1.1.0, shown here at 17 games:

| Pos | 1–32 | 33–64 | 65–128 | 129–262 | undrafted |
|---|---|---|---|---|---|
| QB | 11.8 | — | 2.4 | 2.8 | 0.6 |
| RB | 13.8 | 12.5 | 12.2 | 8.0 | 1.8 |
| TE | 15.3 | 12.3 | 9.1 | 5.7 | 0.8 |
| WR | 14.3 | 13.3 | 10.8 | 7.4 | 1.0 |

Volume keeps the log fit, where the relationship really is monotone.

**A rookie needs a rookie efficiency baseline.** A rookie is less efficient per
opportunity than an established player, so the pool's baseline overstates every
rookie projection. Pooled rookie rates per position replaced it — and those needed
their own floor: without a minimum pooled denominator the table carried an
`int_per_attempt` of 0.200 for running backs, which is one rookie's single
intercepted pass.

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
| QB | 513 | 0.6883 | 0.7038 | **−0.0155** |
| RB | 1,021 | 0.7121 | 0.6902 | **+0.0218** |
| WR | 1,492 | 0.7500 | 0.7394 | **+0.0106** |
| TE | 801 | 0.7271 | 0.6976 | **+0.0295** |

**Veteran rows only, and the restriction is load-bearing.** The naive baseline is
last season carried forward, so for a rookie it is 0 by construction — on all 1,497
rookie rows. Pooling them in credits the model for *covering* rookies rather than for
projecting anyone better, and it inflated this table badly: RB read +0.149 pooled
against +0.022 where both arms can actually speak. The rookie arm gets its own
comparison in §Rookies, against a baseline that can answer.

**Per-stat MAE**, on rows the model speaks for:

| Stat | n | USG | naive | Δ |
|---|---|---|---|---|
| receivingTouchdowns | 2,702 | 1.28 | 1.47 | **−12.5%** |
| passingInterceptions | 429 | 3.54 | 3.95 | **−10.4%** |
| rushingTouchdowns | 1,265 | 1.68 | 1.75 | −4.2% |
| receivingYards | 2,702 | 142.66 | 148.09 | −3.7% |
| receivingReceptions | 2,702 | 12.58 | 13.05 | −3.6% |
| rushingYards | 1,265 | 157.63 | 156.19 | +0.9% |
| passingTouchdowns | 429 | 6.32 | 6.27 | +0.9% |
| passingYards | 429 | 904.99 | 879.84 | +2.9% |

Row counts fell for the rushing and passing stats once the relevance gate landed —
they are now computed only on positions that really accumulate them, so the absolute
values rise and the comparison means something.

**Top-N hit rate**, computed per season and averaged:

| Pos | N | USG | naive |
|---|---|---|---|
| QB | 12 | 0.607 | 0.619 |
| RB | 24 | 0.619 | 0.631 |
| WR | 36 | 0.671 | 0.687 |
| TE | 12 | 0.512 | 0.500 |

### Reading it honestly

**The gains are where the shrinkage is, and that is exactly what plan 16 predicted.**
Touchdown rates and interception rates improve 12%, because a player's own rate is
mostly noise (year-over-year stickiness +0.234) and pulling it to the positional
baseline is most of the available edge. Volume-driven yardage does *not* improve —
prior-season volume carried forward is already close to the best cheap estimate, and
the fitted regression matches rather than beats it.

**Top-N is a wash** — better at TE, marginally worse at the other three. An earlier
run showed it improving everywhere, and that was an artefact: rookies and abstentions
were being ranked in the naive arm (as zeros) while excluded from the model's,
diluting the baseline. Both comparisons here run on veteran rows only.

**QB ordering gets slightly worse.** The quarterback arm is the weakest — 119
rostered quarterbacks, `pass_attempts_pg` R² 0.35, and the passing stats are the
three where MAE regressed. A defensible v1.1 would abstain for QB.

**Coverage is 80.4%** — 5,324 of 6,620 rostered player-seasons: 3,834 veteran,
1,497 rookie, 1,289 abstained. Before the rookie arm it was 57.8%. What remains
unprojected is players with neither a prior season nor rookie status — a second-year
player who never appeared, mostly — and the blend's absent-source path handles them.

### The 16-to-17-game bias, found and removed — 2026-08-07 (v1.1.0)

The head used to be fitted in raw games. The NFL slate went from 16 to 17 in 2021 and
**45% of the training rows predate that**, so the fit learned a blend of two eras.
Measured on players who had managed 16+ the prior year, next-season games average
**13.06** in the 16-game era against **13.64** in the 17-game era.

Both heads now predict a **share of the slate** — target `y_games / y_games_available`,
regressor `p1_availability` — and multiply back up by the season being projected.
A 16-game and a 17-game season then describe the same player identically. The
backtest passes each fold's *measured* slate, so a 2019 fold is still scored against
16 games.

This also settles an old note. `GAMES_REGRESSORS` recorded `p1_availability` as
"deliberately absent", because fitting it *alongside* `p1_games` gave offsetting
nonsense (RB +1.056 on games against −10.160 on availability). That objection was
about collinearity between the two and still holds — availability is used **instead
of** raw games, never with it. The same note also observed the two were collinear
"up to the 16-to-17-game schedule change", which named this bias exactly.

**The effect is not where it was expected to be.** On the veteran arm it is small —
2026 expected games move **+0.066 on average, +0.023 for fully-available players** —
because the old fit had the slate on *both* sides and largely self-corrected. The
rookie bins are a plain mean of games with no predictor to compensate, so the era mix
passed straight through, and that is where the correction lands: every bin rises
about 3%, and rookie ordering improves materially.

| | before | after |
|---|---|---|
| rookie ρ, QB / RB / WR / TE | 0.599 / 0.616 / 0.612 / 0.618 | **0.655 / 0.643 / 0.634 / 0.626** |
| rookie MAE, QB / RB / WR / TE | 26.0 / 21.5 / 14.3 / 10.2 | **24.0 / 20.5 / 13.5 / 9.5** |
| receivingYards MAE vs naive | −3.7% | **−4.1%** |
| receivingTouchdowns MAE | −12.5% | **−13.0%** |
| rushingYards MAE | +0.9% | **+0.4%** |
| TE top-12 hit rate | 0.500 | **0.512** |

Veteran Spearman is unchanged to three decimals. No metric regressed.

#### The trap inside the fix, which nearly shipped

A share needs a denominator, and the first version filtered out rows that had none:
`y_games_available > 0`. **A rookie who never played has no outcome row and therefore
no measured slate**, so that filter silently dropped him — and 78.8% of undrafted
rookies are in exactly that group. The undrafted bin went from **1.1 games to 5.8**,
which would have projected a camp body as a third of a season, and every printed
table still looked reasonable.

A missing slate is now *filled*, never filtered — in `training_frame` where the season
is known, and again defensively in `_fit_rookie_games` from the rows' own maximum. The
distinction is the same one this repo keeps paying for: **a player who never appeared
is a zero, not an absent observation.**

### Games played is a distribution, not a number — 2026-08-07

`Scripts/usage/availability.py`. The point estimate is the weakest thing this model
reports — R² 0.19, and prior games predict next season at r = +0.343 among players
who managed 8+ — so reporting it alone invites it being read as a forecast. The board
now carries `usg_games_sd`, `usg_games_low` and `usg_games_high` beside
`usg_expected_games`.

**Beta-Binomial, and the family is chosen by measurement rather than taste.** Games
is a count out of a known slate, so the natural first guess is Binomial, and the data
rejects that decisively — over 3,942 player-seasons the variance of the games share
is **5.6× to 8.1×** what a Binomial permits:

| Pos | mean share | observed Var | Binomial Var | ratio |
|---|---|---|---|---|
| QB | 0.546 | 0.1203 | 0.0149 | 8.1× |
| RB | 0.634 | 0.0918 | 0.0140 | 6.5× |
| WR | 0.669 | 0.0876 | 0.0134 | 6.6× |
| TE | 0.624 | 0.0786 | 0.0141 | 5.6× |

Beta-Binomial is the Binomial with its success probability given a Beta prior, which
is the structure here: each player has his own durability, drawn from a positional
distribution, never observed directly. Everything is **closed form** — mean, variance,
PMF and exact quantiles over the 18-value support. No simulation.

**Parameterised by mean and concentration, fitted in that order.** The mean stays the
existing regression, which is measured and works; this adds only the second moment, by
method of moments on the residuals. A joint likelihood would let the dispersion pull
the mean around and silently move every number in this document. Fitted κ:

| | QB | RB | TE | WR |
|---|---|---|---|---|
| κ | 2.24 | 2.14 | 3.17 | 2.13 |

Small κ means heavy overdispersion — far closer to "some players are durable and some
are not" than to "every player is a coin flip each week".

The shape matches what the data actually does. For a fully available receiver
(μ = 0.80, 17-game slate): mean **13.6**, median **15**, p10 **7**, p75 and p90 both
**17**. Compare the empirical distribution for players who managed 16+ the prior year
— mean 13.4, median 15, mode 16. The left skew is the whole point, and a normal
approximation would put the mass symmetrically and miss the tail a drafter cares
about.

#### Calibration, and the thing that would have looked like a failure

Walk-forward 2020–2025, 4,211 held-out player-seasons:

```
realised coverage 87.5%   below 6.9%   above 5.6%
the model's own claim for these cut points is 89.4%
```

Against a nominal 80% that reads as badly over-wide. It is not. `games_low` is the
smallest integer whose cumulative probability reaches 0.10, and with eighteen
attainable values each step carries several percent of mass, so **an integer p10/p90
always excludes less than asked**. The model knows this: it claims 89.4% for the cut
points it picked, and reality delivered 87.5%. That is a well-calibrated
distribution, marginally overconfident, and judging it against 80% would have
condemned it for a property of the support rather than a defect of the fit.

`report_games_interval` prints both numbers for that reason.

#### What it says about the players the model fades

The disagreement list from §How the board uses it now has an interval on it, and the
interval is the answer to "is this a 20% bust risk or a 5% one":

| | expected games | sd | p10 | p90 |
|---|---|---|---|---|
| Malik Nabers | 9.3 | 5.1 | 2 | 16 |
| Garrett Wilson | 10.3 | 5.0 | 3 | 17 |
| Omarion Hampton | 10.9 | 4.9 | 3 | 17 |
| DJ Moore | 12.3 | 4.6 | 5 | 17 |
| Puka Nacua | 13.6 | 4.1 | 7 | 17 |

The honest reading is that these bands overlap heavily. Nabers' p90 is 16 and Nacua's
p10 is 7 — the availability head separates them in expectation and barely at all in
outcome, which is exactly what R² 0.19 means and why a fade on availability grounds
should be held loosely.

### The stat lines get intervals too — 2026-08-07

`Scripts/usage/predictive.py`. Every `USG_<stat>` now carries `_sd`, `_low` and
`_high` on the board. Closed form throughout — Negative Binomial and Gamma quantiles
are the regularised incomplete beta and gamma inverses.

**The clean design does not work, and the measurement is the interesting part.** The
obvious move is to model the three factors separately and multiply, since the model
already decomposes into games × volume × rate and the product of *independent*
variables has an exact CV identity. Independence fails:

| pair | correlation |
|---|---|
| games vs per-game volume | **+0.48 to +0.63** |
| total opportunities vs per-opportunity rate | +0.17 to +0.37 |

The first is large and obvious in hindsight: a player who misses games also loses
touches in the games he does play, because those are the same loss of role. Backing a
per-game volume variance out of an opportunity variance gave **negative** numbers for
quarterbacks — the arithmetic refusing the assumption. Each stat's dispersion is
therefore fitted end-to-end on its own residuals, which absorbs every correlation
without needing any of it named.

**Three things had to be fixed before the intervals calibrated at all**, and each was
found by measuring coverage rather than by inspection:

| | symptom | cause | fix |
|---|---|---|---|
| 1 | coverage 6%, identical n for every stat | **NaN is not null in Polars** — `is_not_null()` is True for NaN, so declined rows survived every filter and then compared False against everything | `fill_nan(None)` |
| 2 | yardage 49–57% | dispersion fitted in-sample | fit on a two-season holdout |
| 3 | yardage still 55–67% | **CV is not constant** — it falls 1.90 → 0.48 across the projection range, and a moment fit weights by μ² so it lands on the top quartile | two-parameter `Var = φμ + μ²/k` |
| 4 | lower tail leaking 20.5% | **a Gamma has no mass at zero** and 10.5% of rows realise exactly 0; 59% of sub-p10 rows produced under 5% of their projection | a fitted bust point mass, mean-preserving |

Coverage after all four, walk-forward 2021–2025:

| stat | n | covered | below | above |
|---|---|---|---|---|
| receivingYards | 2,379 | 76.3% | 9.5% | 14.2% |
| receivingReceptions | 2,379 | 77.3% | 9.0% | 13.7% |
| receivingTouchdowns | 2,379 | 91.1% | 0.6% | 8.3% |
| rushingYards | 1,105 | 74.6% | 10.3% | 15.1% |
| rushingTouchdowns | 1,104 | 89.9% | 0.8% | 9.3% |
| passingYards | 367 | **60.8%** | 23.2% | 16.1% |
| passingTouchdowns | 367 | 76.8% | 10.1% | 13.1% |
| passingInterceptions | 367 | 75.5% | 8.7% | 15.8% |

The touchdown rows read high for the same discreteness reason as games — an integer
p10/p90 on a small count excludes less than asked. **`passingYards` at 60.8% is the
one genuinely poor row**, and it is on the quarterback arm, which
`ABSTAIN_POSITIONS` already declines, so it never reaches a board.

#### What the dispersion says about where uncertainty lives

Worth reading alongside the fitted values, because it points somewhere specific.
Conditional on the opportunity count, the bounded rates are barely overdispersed at
all — **1.08× to 1.79×** Binomial — against 5.6–8.1× for games and 13–99× for volume.

Once you know how many targets a player gets, his touchdown rate is close to pure
sampling noise. **Nearly all the reducible uncertainty in a season projection is how
much work a player gets, not what he does with it** — the same conclusion plan 16's
stickiness table reached from the other direction, and an argument for spending
future effort on the depth chart rather than on efficiency modelling.

### Snap share — added to availability, rejected as a rate denominator — 2026-08-07

**No new data source.** `snap_counts.parquet` and `injuries.parquet` were already
pulled by `R/GetContext.R` for 2016–2025 and sitting unused. The `pfr_id → gsis_id`
join is 66% overall but **99.8% restricted to QB/RB/WR/TE** — the misses are offensive
linemen. Injuries carry `gsis_id` natively at 100%.

**The partial-game problem is real.** Games played counts an appearance, so a starter
who leaves on the first drive is credited with a full game and his per-game rates are
dragged down. Measured on 2024 starters (median snap share ≥ 50%): 4.1% of their games
run below half their own normal snap share, and those games average **1.86 targets
against 5.21** in normal ones. 38% of starters have at least one, and excluding them
lifts their targets per game **+8.2%**.

**Correcting it makes prediction worse, twice over.** Two versions were tried:

| prior-season feature | R² predicting next-season targets/game | R² predicting next-season total |
|---|---|---|
| per appearance (what the model uses) | **0.693** | **0.608** |
| per snap-weighted game, all games | 0.295 | 0.259 |
| per appearance, injury-truncated games removed | 0.684 | 0.596 |

The global normalisation fails because a part-time player's low per-appearance rate
*is his role*, not a distortion of it — dividing it out inflates him three-fold and
predicts a job he does not have.

The narrow version — dropping only games anomalously low against the player's own norm
— fails for a subtler reason worth keeping. **The model already discounts injury once,
in `expected_games`.** Cleaning the rate applies it a second time. And truncation
recurs: a player truncated last season is likelier to be truncated again, so the
"contaminated" rate is the better forecast of a future that will also contain
truncations. It is the same distinction as if-healthy versus expected-value, one level
down.

**Where snap share does pay is the availability head**, and it is the largest single
gain available there. Over 1,605 player-season pairs predicting next season's games:

| features | R² |
|---|---|
| prior games alone | 0.203 |
| **+ prior snap share** | **0.230** |
| + injury weeks-out | 0.208 |
| + snap share + every injury feature | 0.233 |

It reads as role *security* rather than durability: 85% of snaps is entrenched, 25% is
one depth-chart move from inactive, and being inactive is most of what games played
measures once a player is on a roster. Fitted into the real head, R² rises at every
position — RB 0.187 → **0.224**, TE 0.218 → **0.247**, WR 0.188 → **0.215**, QB
0.441 → **0.456** — and the availability coefficient falls as snap share absorbs part
of it.

Downstream, every metric improved and one flipped sign:

| | before | after |
|---|---|---|
| receivingYards MAE vs naive | −4.1% | **−5.5%** |
| receivingTouchdowns MAE | −13.0% | **−13.7%** |
| **rushingYards MAE** | **+0.4%** (worse than naive) | **−1.9%** (better) |
| rushingTouchdowns MAE | −4.7% | **−6.6%** |
| passingInterceptions MAE | −10.6% | **−12.0%** |
| RB top-24 hit rate | 0.619 vs 0.631 naive | **0.637 vs 0.625** |
| QB Spearman delta | −0.0153 | −0.0119 |
| games interval coverage | 87.5% (claim 89.4%) | **90.0% (claim 89.9%)** |

Rushing yardage had never beaten the naive baseline before. The games interval is now
calibrated almost exactly, with symmetric 5.0%/5.0% tails.

**Injury reports are not used, and the measurement is why.** They add **+0.003** R² on
top of snap share, and that is generous — the test's baseline did not include
`p1_weeks_on_reserve`, which the model already carries and which overlaps with "was
hurt". Their real home is [plan 19](19-weekly-usage-model.md): the weekly head gets
the live report, where it is a primary signal rather than a marginal one. The pull is
already on disk for 2016–2025 when that is built.

### Age — added to both heads, 2026-08-07

**No new data source.** `birth_date` and `years_exp` were in `rosters_weekly.parquet`
for every season, already pulled, and nothing used them. Age is a *current-season*
fact rather than a lag — a birth date does not move, so 2026's age is knowable in
2026 — so it lands in `roster_context` beside `team_changed` and `is_rookie`.

Measured against the season opener rather than by subtracting birth years: a January
and a December birthday are most of a year apart, and the running-back decline is
steep enough for that to matter.

Measured over 8,763 player-season pairs before building anything:

| predicting next season | prior volume + games | + age |
|---|---|---|
| WR targets | 0.5462 | **0.5645** |
| RB carries | 0.5114 | **0.5211** |
| TE targets | 0.5554 | **0.5603** |
| games played (on top of snap share) | 0.1982 | **0.2100** |

**Linear, not a curve.** Adding `age²` moved every one of those by less than 0.0003.
The quadratic the football-analytics literature likes buys nothing over this
population and this horizon, and a parameter that does nothing is a parameter that
can overfit.

In the fitted heads, R² rose on every volume arm — RB carries 0.585 → **0.605**, WR
targets 0.628 → **0.650**, TE targets 0.606 → **0.622** — and the coefficients are
face-valid: **RB carries −0.236 per year of age**, the classic cliff, with older
quarterbacks running less (−0.073) and throwing more (+0.072). The availability gain
is smaller than it measured in isolation (TE +0.004, WR +0.003, QB/RB ≈ 0), because
snap share already absorbs part of it.

Downstream:

| | before | after |
|---|---|---|
| receivingYards MAE vs naive | −5.5% | **−6.3%** |
| receivingReceptions MAE | −5.2% | **−6.2%** |
| receivingTouchdowns MAE | −13.7% | **−14.0%** |
| rushingYards MAE | −1.9% | **−2.9%** |
| WR Spearman delta | +0.0097 | **+0.0126** |
| TE Spearman delta | +0.0291 | **+0.0300** |
| TE top-12 hit rate | 0.524 vs 0.512 | 0.524 vs 0.512 |

RB Spearman slipped slightly (+0.0222 → +0.0196) while RB MAE and top-24 both
improved, which is the usual ordering-versus-level trade and nets positive.

**It did not close the quarterback gap.** QB Spearman has been converging as the
model improved — **−0.0155 → −0.0153 (slate) → −0.0119 (snap share) → −0.0115
(age)** — but it is still negative, so `ABSTAIN_POSITIONS` stays as it is. That was
the stated reason for trying age second; the answer is no.

One bug worth recording: `age_expr` first returned a null when the column was absent,
and both fits `drop_nulls()` their regressor block, so a frame built before this
feature existed would have produced an **empty volume fit** rather than simply not
using age. It returns a constant now, which is collinear with the intercept and
therefore contributes nothing — the intended "this arm does not use age" behaviour.
Zero was not an option: unlike a team-change flag, a zero age is not neutral, it puts
every unknown player at the far end of the decline curve.

### The weight comes off 0.0 — 2026-08-07

`WEIGHTS` is now a single `default` entry: **ESPN, FantasyPros and `USG` at a third
each, Pinnacle and BetOnline at zero.** An owner decision, and worth separating from
the evidence.

G2 is unchanged and still unanswerable: it needs the blend scored with and without the
model against realised results, and no historical pre-season blend survives. What
changed is everything around it — the model now beats the naive draft heuristic on
every metric at every position, in **26 of 28** out-of-sample season-position cells,
with the five folds never used for feature selection scoring as well as or better than
the two that were. Weighting it in remains an assertion; it is now an assertion with a
seven-fold walk-forward behind it, made deliberately rather than inherited as a
default.

The per-stat keys are gone with the old table. They held per-stat differences and
there are none now; a row of identical dicts drifts out of sync rather than being a
structure. `compute_weighted_stats` falls back to `default`, so re-adding one is a
line.

**This drops the better-covered market source.** BetOnline's *season* endpoint works —
273 players, 13 stat columns, including the IDP tackles and sacks no other source has —
against FantasyPros' 60. Only the *weekly* endpoint returns 403, on a different host
that never fed this path. Reinstating it is one number.

#### It blends on an if-healthy basis, not on expected value

`Scripts.usage.project.to_full_slate` rescales the stat lines to a full 17-game slate
before they reach the blend. **The model still predicts expected value** — that is what
predicts a realised season and what the backtest scores, and its numbers are unchanged
by this. Only the artifact the blend consumes is rescaled.

The reason is a measured cross-position distortion, and it is exactly the failure that
kept `USG` out of the floor/ceiling spread, one layer up. `USG_Points` was an expected
value (per-game production × ~13.6 games); ESPN and FantasyPros project a healthy
17-game season with no availability discount at all. Blending them produced something
that was neither — and because the model covers QB/RB/WR/TE and not kickers or team
defences, it did so **unevenly**:

| position | before rescale | after |
|---|---|---|
| D/ST | 1.000 | 1.000 |
| K | 1.000 | 1.000 |
| QB | 0.896 | **1.012** |
| RB | 0.887 | **0.974** |
| TE | 0.900 | **0.991** |
| WR | 0.890 | **0.983** |

(Ratio of blended points to an ESPN/FantasyPros-only blend, draftable players.)

Roughly **11% of cross-position distortion** in a blend whose entire job is to be
comparable across positions, and VOR inherits it directly — VOR is a difference, so a
uniformly deflated position gets a uniformly deflated VOR while kickers keep theirs.
After the rescale the residual is 1–3%, which is genuine disagreement rather than a
units artifact, and quarterbacks come out slightly *above* consensus rather than 10%
below.

The availability estimate is not lost. It travels as `usg_expected_games` with its own
Beta-Binomial interval, which is where a per-player discount belongs — and applied
there it would discount the **whole** blend rather than one third of it, which is a
better use of the one thing this model has that no other source does.

**This also reopens the floor/ceiling question**, since the scale mismatch was the
stated reason for the exclusion. Re-measured on the rescaled lines, `USG` is now the
minimum for **30%** of draftable players and the maximum for **46%** — two-sided
disagreement rather than the systematic lowness that made the interval read as "the
model is bearish" (it was the minimum for 52% before). Median spread widens from 8.5%
to 14.2%, which is arguably more honest given how correlated plan 03 measured the
market sources to be. Not changed here; it is a separate decision and a visible board
column.

#### It withdraws for players ESPN lists as out

**The model cannot see a current injury and the other sources can**, which is a
one-directional failure worth naming. nflreadr refuses 2026 injuries outright, so
`expected_games` is built from prior-season availability, snap share and age —
statistics about a player who was healthy last August. ESPN knows *today* that Ricky
Pearsall is on injured reserve for the season and that Zach Charbonnet tore an ACL in
January, and both ESPN's and FantasyPros' projections reflect it.

Left alone the model overrode them in the worst possible direction. Measured on the
2026 board, mean effect of adding `USG` at a third:

| | n | mean effect on the blend |
|---|---|---|
| ESPN lists OUT / INJURY_RESERVE | 22 | **+15.7 points** |
| active, draftable | 169 | −2.7 points |

It inflated precisely the players it knew nothing about. Pearsall is the clearest
case: ESPN and FantasyPros both projected him at literally **0.0**, and the model
pulled the blend to **72.4**. Charbonnet went 133 → 164 with a torn ACL.

`INJURY_ABSTAIN_STATUSES` withdraws the source for those players — nulled *and*
flagged, so `compute_weighted_stats` drops the weight and renormalises rather than
blending a null as a zero. This is plan 18's decision 4 applied to a fact rather than
a position: say nothing where you know nothing, and let the sources that do know carry
the player. Pearsall is back to 0.0.

`QUESTIONABLE` is deliberately excluded. Pre-season it is week-to-week noise on 64
players, and abstaining on it would discard the model across a large slice of the pool
on a signal that says little about the season.

**What ESPN does and does not give.** Probed live on 2026-08-07 via
`view=kona_player_info`. The player object carries `injuryStatus` (ACTIVE /
QUESTIONABLE / DOUBTFUL / OUT / INJURY_RESERVE), `injured`, `lastNewsDate` and a
free-text `seasonOutlook`. **There is no structured estimated return date**, and the
prose is present for only 9 of 22 injured players. It does often contain a timeline in
words — *"Kittle could miss a few games"*, *"out for the 2026 season"* — but parsing it
would be a fragile answer to a question ESPN's own projection already encodes.

#### The change exposed a real bug in coverage counting

`projection_missing` and `sources_real` were computed from `OPINION_PREFIXES`, which
the floor/ceiling fix had deliberately narrowed to the four market sources. With `USG`
weighted into `TRUE_Points` but absent from that list, a player **only** the usage
model projects got a real blended score and `projection_missing = True` — the board
would have hidden, as unprojected, exactly the players the model exists to
differentiate. Measured: 523 counted as projected against the correct **699**.

The two lists now exist separately, because they answer different questions:

| | list | question |
|---|---|---|
| `OPINION_PREFIXES` | ESPN, FP, PINNY, BOL | do the forecasters disagree, and by how much? |
| `PROJECTION_PREFIXES` | + USG | does this player have a projection at all? |

The first needs sources measuring the same quantity, which is why `USG` — an expected
value against four if-healthy projections — stays out. The second needs every source
that moves the blend. Merging them is the bug.

### Team-then-allocate: tested, rejected, and it pointed at the real bottleneck — 2026-08-07

Plan 21's game-script measurement left one structural idea open. Team strength
improves a *team's* rush-attempt prediction by +0.064 R² and a *player's* by +0.0015,
which suggested predicting team volume first and allocating it by role, so the signal
would have somewhere to land.

**It does not work**, on RB carries, train 2020–2023 and test 2024–2025:

| | R² | MAE |
|---|---|---|
| **direct** — player carries from player history | **0.5633** | 48.3 |
| team-then-allocate | 0.5488 | 49.3 |
| *oracle* team carries × predicted share | 0.5695 | 48.4 |
| predicted team carries × *oracle* share | **0.9793** | **7.7** |

The two oracle rows are the finding. **Knowing every team's rushing volume perfectly
buys +0.006 R². Knowing every player's share perfectly buys +0.42.** The bottleneck
was never team volume — team volume is nearly a constant against the variation in
share. On the test seasons the two halves predict at R² 0.164 (team carries) and 0.548
(share), and it is the second number that governs.

So the answer is not a better team model. It is a better *share* model — and share is
exactly what a depth chart describes.

### The depth chart joins the veteran arm — and a wrong claim gets corrected

`VOLUME_REGRESSORS` now carries `depth_rank` and `is_first_string`.

**This contradicts a comment that had been in the code since plan 21.** It read "The
coach prior and depth chart are **not** here, and that is a measured decision rather
than an omission" — but the experiment it went on to describe varied only
`coach_volume` and `staff_continuity`, and the constant recording the result is
literally `VETERAN_SITUATIONAL_REJECTED = ("coach_volume", "staff_continuity")`. The
depth chart was swept into the sentence and never tested on veterans.

Tested properly, train 2020–2023 and test 2024–2025 on RB carries:

| | share of team carries | carries |
|---|---|---|
| prior + age (+ moved) | 0.5193 | 0.5584 |
| **+ depth_rank and is_first_string** | **0.5803** | **0.6066** |

In the fitted heads R² rises everywhere: RB carries 0.605 → **0.644**, TE targets
0.622 → **0.662**, WR targets 0.650 → **0.676**, and **QB pass attempts 0.353 →
0.455**. Read `depth_rank` and `is_first_string` as a pair rather than individually —
they are strongly collinear, the same caution the volume lags already carry.

**Every backtest metric now beats the naive draft heuristic.** That was not true of any
earlier version:

| | before | after |
|---|---|---|
| QB Spearman delta | −0.0115 | **+0.0132** |
| RB Spearman delta | +0.0196 | **+0.0623** |
| WR Spearman delta | +0.0126 | **+0.0531** |
| TE Spearman delta | +0.0300 | **+0.0658** |
| receivingYards MAE | −6.3% | **−12.3%** |
| rushingYards MAE | −2.9% | **−9.0%** |
| passingYards MAE | **+1.8%** (worse) | **−8.2%** |
| passingInterceptions MAE | −11.9% | **−17.0%** |
| top-N hit rate | RB/TE only | **all four positions** |

### The quarterback abstention is lifted

`ABSTAIN_POSITIONS` is now empty. QB was declined because the model measured worse
than the naive heuristic there; the deficit closed as the model improved, and the
depth chart closed it decisively:

| | QB Spearman delta |
|---|---|
| original | −0.0155 |
| share-of-slate games head | −0.0153 |
| + snap share | −0.0119 |
| + age | −0.0115 |
| **+ depth chart on veterans** | **+0.0132** |

Not one metric turning over: QB top-12 hit rate goes 0.607 → **0.631** against the
baseline's 0.619, and all three passing MAEs flip from losing to the naive heuristic to
beating it by 7–17%. In hindsight the mechanism is obvious — being the listed starter
is enormously predictive of pass attempts, and prior-season volume alone cannot see a
backup who has won the job.

Coverage goes **73.2% → 83.7%** of rostered players. On a draft board the only
remaining gaps are the positions the model has never modelled: 17 D/ST, 14 K, and four
skill players.

The constant is kept rather than deleted. It is how a position gets declined on
evidence, and the next arm that measures worse should use it.

### The fitted expected-games head, and what it says

Coefficients are in **share of the slate** as of v1.1.0; the last column converts a
fully-available player back to games at a 17-game season.

| Pos | n | const | p1_availability | reserve | team change | R² | @17 games |
|---|---|---|---|---|---|---|---|
| QB | 468 | 0.196 | 0.642 | 0.014 | −0.137 | 0.441 | 14.24 |
| RB | 958 | 0.259 | 0.543 | 0.011 | −0.071 | 0.187 | 13.63 |
| TE | 731 | 0.289 | 0.540 | 0.008 | −0.039 | 0.218 | 14.09 |
| WR | 1,333 | 0.304 | 0.540 | 0.009 | −0.103 | 0.188 | 14.34 |

**The slope on prior availability comes out at 0.54–0.64, not 1.0** — the model
estimates about 45% shrinkage rather than having it asserted, which is the same
conclusion the r = +0.663 / +0.343 split argued for. Changing team costs 0.7 to 2.3
games at a 17-game slate. R² of 0.19 for a skill position is the honest ceiling on
this: **availability is mostly not predictable**, and the head's job is trimming the
tail rather than differentiating the top, where every estimate lands within a game
of 14.

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

## The fifth source, wired — 2026-08-07

Steps 3 and 6. `Scripts/usage/project.py` turns the model into an artifact the blend
can consume, and `USG` is now a registered source everywhere the other four are.

```
python -m Scripts.usage.project --season 2026
  -> Data/Projections/Usage/Season/2026/Usage_SeasonProjections.parquet
python -m Scripts.refresh --all --what board
  -> USG_<stat>, USG_Points, USG_PosRank, USG_PosRankDelta,
     usg_expected_games, usg_arm  on all nine boards
```

**It ships at weight 0.0, and that is the whole point.** The source is built, joined,
scored per league and shown on the board; it does not move `TRUE_Points`. Verified
rather than argued: rebuilding Knights_FFL with and without the `USG` weight entry
gives **all 45 `TRUE_` columns bit-identical over 1,026 rows, max difference 0.0**.
G2 is now answerable by changing one number, which is what "wired but not weighted"
was supposed to buy.

**Coverage is the surprise, and it is the good kind.** The pre-season blend is far
thinner than the weekly one, and the usage model is the best-covered source in it:

| stat | ESPN | FP | PINNY | BOL | **USG** |
|---|---|---|---|---|---|
| receivingYards | 34.9% | 5.8% | 2.9% | 9.4% | **51.9%** |
| receivingReceptions | 34.9% | 5.8% | 0.0% | 5.2% | **51.9%** |
| rushingYards | 23.8% | 5.8% | 2.0% | 3.9% | **14.5%** |
| passingYards | 6.3% | 5.8% | 2.2% | 2.4% | **0.0%** — declined |
| **all stats (mean)** | 13.1% | 0.8% | 0.1% | 0.3% | **23.1%** |

It matched 709 of Knights' 1,026 players, against FantasyPros' 60, Pinnacle's 74 and
BetOnline's 156. Players with any real projection went **523 → 675**.

### Three implementation decisions worth recording

**It joins on an id.** The model keys on `gsis_id`; `Scripts/crosswalk.py` maps that
to ESPN's `player_id`. Every other season source joins on a normalised name, which is
why `_disambiguate_name_keys` exists — GOP Degenerates' pool holds 16 colliding names,
including two Lamar Jacksons and two Justin Jeffersons, and a name join hands the
receiver's line to the linebacker. `USG` is the first source to avoid that.

It cannot avoid it for everyone: the crosswalk file carries **no 2026 rookies**, so 95
predictions resolve to no ESPN id — and those are exactly the population the rookie
arm exists to project. Dropping them to keep the join pure would discard the model's
one clearly measured win, so the merge falls back to `join_key` where the id is
missing, inheriting the collision protection already built. Measured on Knights: 769
by id, 9 by name, the remaining 136 not in the ESPN universe at all.

**Abstention is flagged, and `USG_` is deliberately outside the imputation chain.**
`compute_weighted_stats` treats a source with no `_is_imputed` companion as real and
fills its nulls with 0.0 — so an unflagged abstention enters the blend as a confident
projection of zero. `test_an_unflagged_abstention_would_poison_the_blend` pins the
difference: 110.0 flagged against 55.0 unflagged, on the same inputs. This is the
`0.0`-means-two-things failure that already cost this repo a draft board.

Nor is it imputed from `MEAN_` the way the books are. Filling the one source that is
*not* somebody else's projection from the ESPN/FantasyPros average would turn it into
a copy of two sources already in the blend — the double-counting plan 03 measured for
Pinnacle.

**The model gained a `load()`.** Coefficients persisted and nothing read them back,
so every caller had to refit — and the board and the backtest could silently end up
built from different coefficients. `load_or_fit` also checks freshness, which caught a
real case immediately: the persisted artifact had trained on **2017–2024**, because it
was written by a walk-forward whose last fold predicted 2025. A 2026 projection now
refits through 2025 rather than quietly using a model a season out of date.

### It does not belong in the floor/ceiling spread — a defect, found and removed

Shipping it as an opinion source in `OPINION_PREFIXES` was wrong, and the reasoning
is worth keeping because it was *nearly* right. G0 measured `USG` as the most
independent source in the set (+0.832 residual correlation with ESPN against
FantasyPros' +0.988), so it looked like exactly what a disagreement interval was
missing. But independence is a claim about information content, and that interval
needs a different property: **that the sources are answering the same question.**

They are not — `USG_Points` is an expected value and the other four project a healthy
season — so it did not disagree with them so much as measure something else. It sat
below all four for **51.7% of the players it covered**, taking the median
floor-to-ceiling width on the draftable pool from 8.5% to **24.0%**. Every affected
player's floor was literally his `USG_Points`.

Rescaling to a common if-healthy basis was measured too, and is better but still
wrong:

| spread source | median relative width | USG is the floor |
|---|---|---|
| four market sources | **8.5%** | — |
| + USG as-is | 24.0% | 52% |
| + USG rescaled to per-17-games | 13.6% | 47% |

The residual is not a units problem. The model shrinks toward positional baselines
while the other sources extrapolate, and draftable players are by definition the top
of the pool, so it is genuinely lower for most of them. That is real disagreement,
but it makes the interval asymmetric — it reads as "the model is bearish" rather than
"here is the uncertainty".

**Disagreement between forecasters and uncertainty within one forecast are different
quantities, and one column can only hold one of them.** The spread holds the first.
The second is a predictive interval, which this model can supply properly because it
decomposes into volume × efficiency × games — see §Where the variance should come
from. Meanwhile the model's dissent is carried by `USG_PosRankDelta`, which is a rank
and therefore cannot be contaminated by the level mismatch at all.

### `USG_Points` is not on the same scale as `TRUE_Points`

Worth stating plainly, because side-by-side columns invite the wrong reading.
`USG_Points` is an **expected value** — per-game production × *expected games*, which
for a rostered starter is about 13.5. ESPN and FantasyPros project a **healthy
17-game season**. So `USG_Points` sits roughly 20% below `TRUE_Points` for nearly
everyone, and that gap is an injury discount, not bearishness.

Among the top 60 players a board actually ranks, `expected_games` is **13.2 ± 0.77** —
near-constant, so the deflation is close to uniform and the *ordering* is what carries
information. Hence `USG_PosRank` and `USG_PosRankDelta` on the board rather than a
points comparison. Against the consensus ordering on Knights' draftable pool: Spearman
**0.78 RB, 0.70 WR, 0.44 TE** — correlated, not redundant.

**The two tails decompose cleanly, and a drafter should know which is which:**

| | who | why |
|---|---|---|
| model **fades** | Nabers (9.2 expected games), Reed (9.2), Evans (9.0), G. Wilson (10.3), McLaurin (11.4), A.J. Brown (11.4) | almost entirely the **availability head** |
| model **buys** | Tate, Tyson, Lemon, Concepcion (all 14.0 expected games) | almost entirely the **rookie arm** |

The asymmetry matters. The rookie arm is the strong half (ρ ≈ 0.61 against ~0). The
availability half is the weak one — prior-season games predict next season at r =
**+0.343** among players who managed 8+ — so a fade is an injury-risk discount and not
a verdict on the player. Reading it as the latter would be trusting the weaker arm
because it happens to produce the more dramatic column.

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
- ~~**Rookie arm** — ships only if draft capital beats abstention on the same
  walk-forward.~~ **Passed, decisively, 2026-08-07.** ρ ≈ 0.61 within position
  against ~0 for a projection with no draft information, and MAE roughly halved. See
  §Rookies. Coverage went 57.8% → 80.4%.
- ~~**A QB arm** — new, and the backtest is the reason. Quarterback ordering is
  slightly *worse* with the model (−0.0161 Spearman) and the three passing stats are
  the only ones whose MAE regressed. Abstaining for QB is the defensible v1.1.~~
  **Resolved 2026-08-07: it abstains.** `season.ABSTAIN_POSITIONS = ("QB",)`, which
  routes quarterbacks down the same absent-source path as a missing book — weight
  dropped, remaining sources renormalised. Coverage falls 80.4% → 73.2% as a result,
  which is the correct trade: the 7% given up is the 7% the model was measured to be
  worse at. `Scripts.usage.backtest` passes `abstain_positions=()` so the evidence
  for the decision does not erase itself.

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
3. ~~Loader + `WEIGHTS` entry + `proj_to_score` prefix, following the
   `load_pinnacle_season` pattern in `Scripts/season_projections.py`.~~ **Done
   2026-08-07** — see §The fifth source, wired. The `WEIGHTS` entry ships at
   **0.0**: the source is built, scored and shown, and does not move `TRUE_Points`
   until G2 can answer for it.
4. ~~Walk-forward backtest; write the table into this document.~~ **Done** —
   `python -m Scripts.usage.backtest`, §Backtest results.
5. ~~Rookie arm, measured against abstention.~~ **Done** — it won; see §Rookies.
6. ~~A QB arm, or abstention there — see §Ship criteria.~~ **Abstention, done
   2026-08-07.** `season.ABSTAIN_POSITIONS = ("QB",)`. The backtest passes
   `abstain_positions=()` so the table that justifies it stays reproducible.
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
