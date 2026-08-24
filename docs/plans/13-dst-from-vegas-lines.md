# 13 — Model D/ST scoring from Vegas lines

**Priority:** Medium (High once weekly lines are posted) · **Effort:** Medium ·
**Status: SUPERSEDED by [30](30-dst-model.md) on 2026-08-18.**
**Depends on:** [11 (per-slot scoring)](11-per-slot-scoring.md) for the IDP league only
**Related:** [12 (season projections)](12-season-projections.md), [03 (source coverage)](03-projection-source-coverage.md)

> **Read [plan 30](30-dst-model.md) instead. Three corrections, measured 2026-08-18.**
>
> **1. The premise below about which components are biggest is wrong.** Scoring every
> team-defence season under each league's actual slot-16 rules puts points allowed at
> **7.6%** and yards allowed at **14.7%** of the D/ST score in six of the nine leagues —
> 22.3% together, against **sacks 33.8% + interceptions 22.3% = 56%**. Points allowed is the
> largest component in exactly one league, GOP Degenerates, where it is **40.7%**. So the
> plan below optimises the small half everywhere except one league.
>
> **2. But the market is still the answer — for a bigger reason than this plan gives.**
> Implied points allowed beats the prior season on **seven of eight** components, including
> the noisy ones this plan does not mention: sacks **0.464 against 0.203**, interceptions
> **0.357 against 0.113**, fumble recoveries **0.193 against 0.015**. Opponent offensive
> quality drives every defensive event, so a game line reaches the whole score sheet rather
> than one tier.
>
> **3. The `E[f(X)]`-over-tiers instinct is correct and now has a number.** Weekly points
> allowed has an SD of **9.57** against tiers 4–7 points wide, so scoring the season mean
> understates the best third of defences by **12.24 points** and overstates the worst third
> by 4.26 — a 16.5-point compression of the range the component exists to create.
>
> Also: "the rest waits on posted 2026 lines" is out of date — all **272** of 2026's games
> are priced in `Data/NFL/schedules.parquet`.

## Problem

D/ST is the one position with no market projection at all. BetOnline and Pinnacle
sell player props; neither sells "Bears defence fantasy points". So `BOL_*` and
`PINNY_*` for every defensive stat are 100% imputed from ESPN — measured 0.0% real
across all D/ST stats in plan 03's coverage report. The D/ST column of the board is
ESPN's opinion with a four-source badge on it.

But the two biggest D/ST scoring components — **points allowed** and **yards
allowed** — are close to what a game line already tells you. A total and a spread
imply each team's expected points, and a defence's points allowed *is* its
opponent's implied total.

## The data is already in the repo

`Data/NFL_Schedules.csv`, written by `R/GetNFL.R` from nflreadr, already carries:

```
spread_line, total_line, away_moneyline, home_moneyline,
away_spread_odds, home_spread_odds, over_odds, under_odds
```

`spread_line` is from the **home** team's perspective (positive = home favoured),
verified against results. So:

```python
home_implied = (total_line + spread_line) / 2
away_implied = (total_line - spread_line) / 2
# a defence's points allowed is its opponent's implied total
```

No new scraper. Coverage grows as the season approaches — as of 2026-08-03, 51 of
272 games have lines (weeks 1-4). Pinnacle's guest API and the `LOWVIG=1` path in
`R/GetSeasonProps.R` are alternative sources if a second opinion is wanted.

## What the data actually supports

Validated on 2025: 480 defence-games with both lines and final scores.

### Points allowed — real but modest signal

| | |
|---|---|
| correlation, implied vs actual | **0.412** |
| RMSE | **8.99** points |
| RMSE of predicting the league mean | 9.83 points |
| improvement over that baseline | **8%** |
| bias | −0.76 points |
| residual sd | 8.97 points |

Worth being clear-eyed: a single game's points allowed is mostly noise, and Vegas
cuts the error by only 8%. This is not a magic input. Its value is that it is
**unbiased and matchup-aware**, where ESPN's D/ST projection is neither obviously.

### The distributional correction is the bigger win

Points allowed is scored as a **step function** over tiers, so plugging the
expected points allowed into the tier lookup is wrong — the same
`E[f(X)] ≠ f(E[X])` error as the FGY50 floor in [plan 01](01-scoring-coverage.md).

Using GOP's 2026 tiers and a Normal(implied, 8.97) distribution:

| method | mean pts/game | RMSE | season bias |
|---|---|---|---|
| actual | 3.51 | — | — |
| `f(E[X])` — plug in the mean | 2.94 | 2.84 | **−10 pts** |
| `E[f(X)]` — integrate the buckets | **3.55** | **2.52** | **+1 pt** |

Integrating is essentially unbiased and cuts RMSE 11%. Plugging in the mean
understates a defence by ~10 points a season. **This is the part of the plan that
matters most** — it is a correctness fix, not a signal improvement, and it applies
to every tiered stat, including yards allowed.

## Design

**1. Implied points allowed per defence-game.** A small module,
`Scripts/dst_model.py`, joining `NFL_Schedules.csv` to produce
`(season, week, team, implied_points_allowed, has_line)`. Flag `has_line` so a
missing line is visible rather than silently defaulted.

**2. Tier expectation, not tier lookup.** Given a mean and a spread, integrate over
the league's own points-allowed buckets, read from the scoring registry rather than
hardcoded — the tiers differ by league and GOP already changed theirs for 2026.

```python
def expected_tier_points(mu, sd, tiers):
    return sum(p_bucket(lo, hi, mu, sd) * pts for (lo, hi), pts in tiers.items())
```

Start with a Normal; a discrete distribution fit to actual NFL scoring would be
better and is a cheap upgrade, since football scores cluster on 3s and 7s. Fit the
sd from residuals rather than assuming, and refit per season.

**3. Yards allowed.** Not implied by a game line, so this needs a regression:
`yards_allowed ~ implied_points_allowed + total_line + spread_line`, fit on
historical team-game data. Source is `nflfastR::calculate_stats(season, "week",
"team")`, which `R/GetNFL.R` already calls for players — extend it to teams. Then
integrate over the yards-allowed tiers the same way.

**4. Turnovers, sacks, return TDs.** Weakly related to game environment and mostly
team-quality driven. Use a team prior blended with the game line rather than
pretending the line predicts them.

**5. Blend as a source, with provenance.** Add the model as a distinct prefix
(`VEGAS_`) so plan 03's renormalisation treats it as a real source where a line
exists and drops it where one does not. That is exactly what the provenance flags
are for, and it means the D/ST column stops being silently ESPN-only.

## Timing — two different problems

**In-season weekly** is the straightforward case: lines exist a few weeks out, so
week-by-week D/ST projections work from week 1. This is where the model earns its
keep, and it is what the user is asking for.

**Pre-season and the draft board** cannot use game lines — they do not exist for
week 12 in August. For a season-long D/ST projection use season-long markets
instead: Pinnacle's guest API already returns 32 `Regular Season Wins` and 32
`Team to Make Playoffs` specials for 2026. A team's win total implies its strength,
which implies season points allowed. Lower resolution than a game line, but
available now and better than nothing.

Note the asymmetry: this makes weekly D/ST decisions materially better while
improving the draft board only a little. Draft-day D/ST is a late-round decision
anyway, so that ordering is fine.

## Dependency note

The bucket integration needs a normal CDF. `scipy` is installed locally (1.17.1)
but is **not in `requirements.txt`** — add it when this lands, or use
`statsmodels` (already a dependency) / `math.erf` to avoid a new one. A three-line
`erf`-based CDF is enough and keeps the dependency footprint unchanged.

## Risks

- **Overfitting the residual sd.** Fit on multiple seasons, not just 2025.
- **Line movement.** nflreadr's `total_line` is a closing or near-closing number.
  Projecting mid-week uses a line that may still move; record when it was read.
- **Correlated errors.** A defence's points allowed and its own offence's output
  are not independent (garbage-time, script). Do not blend a Vegas-derived D/ST
  projection with an offence-derived one as though the errors are independent.
- **GOP.** Its D/ST and IDP values are misread by `espn_api` today
  ([plan 11](11-per-slot-scoring.md)), so validate the tier values from the
  registry against raw `pointsOverrides` before trusting GOP output.

## Verification

- Implied points allowed reproduces the 0.412 correlation / 8.99 RMSE above on a
  held-out season.
- `E[f(X)]` beats `f(E[X])` on both bias and RMSE, per league, on held-out data.
- The tier definitions come from the scoring registry, and GOP's 2026 tiers differ
  from its 2025 ones in the output.
- A week with no posted lines produces `has_line = False` and the D/ST projection
  falls back to ESPN with the provenance flag set — not a silent zero.
- Backtest: `TRUE_Points` for D/ST correlates better with actuals than ESPN alone
  across 2025.
