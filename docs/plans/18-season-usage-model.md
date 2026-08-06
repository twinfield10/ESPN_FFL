# 18 — The season usage model (pre-season / draft head)

**Priority:** High (seasonal) · **Effort:** M · **Status:** Not started
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
pre-season it has no current injury report to read, so it falls back to trailing
games-missed and roster status.

Keep it visibly separate from production. "18 points per game × 14.2 games" is
auditable; a single 256-point number is not.

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

## Ship criteria

Inherited from [plan 16 §Go/no-go](16-usage-data-layer.md#go--no-go), plus this
head's own:

- **G0** — usage residuals materially less correlated with ESPN's than ESPN's are
  with FantasyPros'. Gated in plan 16, before any of this is built.
- **G2** — within-position Spearman against realised season points improves over
  `TRUE_` alone, on the walk-forward.
- **Rookie arm** — ships only if draft capital beats abstention on the same
  walk-forward.

If G2 fails, **do not wire it in at a token weight.** Record the numbers in this
document and stop. A source that does not improve ordering but does add a fifth
name to `WEIGHTS` makes the blend harder to reason about for nothing.

## Steps

1. Plan 16 Step 0 and its feature layer. Blocking.
2. `Scripts/usage/season.py` — prior-season aggregation, opportunity and
   efficiency heads, expected games, `USG_<stat>` season line.
3. Loader + `WEIGHTS` entry + `proj_to_score` prefix, following the
   `load_pinnacle_season` pattern in `Scripts/season_projections.py`.
4. Walk-forward backtest; write the table into this document.
5. Rookie arm, measured against abstention.
6. Hand the enlarged source set to
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
