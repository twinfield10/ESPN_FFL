# 42 — What a weekly matchup is actually worth

**Status:** COMPLETE

**Priority:** High · **Effort:** M · **Where it stands:** **Fitted, gated and shipped
2026-09-07.** All four pre-committed gates pass: team-level interval coverage **0.802**
against a nominal 0.800, worst predicted decile within **2.8pp** of its realised win
rate, Brier **0.2277** against 0.2500 uninformative, predicted team sd within **1.5%**
of realised. The Matchup tab shows the probability because it earned the right to.
**Depends on:** [28 (outcome distributions)](28-outcome-distributions.md) ·
[40 (frontend restructure)](40-frontend-restructure.md) ·
**Feeds:** [19 (weekly usage model)](19-weekly-usage-model.md)

---

## Problem

"You are projected to win by 6" is not a decision. A six-point edge on a lineup whose
weekly standard deviation is 23 is barely an edge at all, and the same six points mean
something completely different in a blowout. The projected margin is the input; the
probability is the number you act on.

[Plan 28](28-outcome-distributions.md) prices a **season** — `pts_p10`, `pts_p90`,
`p_top12`, `p_bust`, all on the board. A week is a different question and nothing
answered it.

**The existing simulator could not.** `simulation_utils.simulate_matchup` draws each
team's score from `N(mean of last 6 weeks of actual scores, 2 × season sd)` off a live
`espn_api.Team`. Three things wrong with that here: it needs played games, so it says
nothing in week 1; it needs ESPN in the render path, which the app promises never to
have; and its own comment calls the `×2` an artificial inflation. It is a
*season-standings* simulator and it is fine at that job.

## What was fitted

`Scripts/outcomes/weekly.py`. The **form is borrowed and the parameters are measured**:
plan 28 established `Var = φ·μ + μ²/k` — a Poisson-like term plus a proportional one,
so a smaller projection gets a proportionally wider interval — and this refits it on
weekly residuals.

**Training set: 2025, and that is a hard ceiling rather than a choice.**
`lineups.parquet` cannot be rebuilt for a past season — it carries `FP_`/`PINNY_`/`BOL_`
columns and FantasyPros serves no season parameter, so 2024's blend inputs no longer
exist anywhere. 2025 is the first season the store was kept for. 15,989
started-and-active player-weeks across nine leagues and seventeen weeks, each carrying
both an actual and a `TRUE_Points`. Each season from here adds one.

Restricted to **started** players, because the bench is not a sample of the same
thing — it is where managers park players they expect to do nothing, so its residuals
describe a selection. Restricted to **active** players for the sharper version of the
same reason: a player on bye scores exactly zero and would teach the model that a
14-point projection has a 14-point error.

| pos | n | φ | 1/k | bias | sd |
|---|---|---|---|---|---|
| WR | 4,762 | 3.379 | 0.0734 | −0.64 | 7.63 |
| RB | 4,101 | 3.193 | 0.0788 | +0.14 | 7.97 |
| QB | 1,873 | 4.086 | 0 | −0.05 | 8.08 |
| TE | 1,837 | 5.153 | 0 | +0.27 | 6.79 |
| K | 1,588 | 0 | 0.4492 | +0.56 | 5.56 |
| D/ST | 1,565 | 7.192 | 0 | +0.86 | 6.50 |
| LB, DE, S, DT, CB | 5–153 | *pooled* | | | |

Skill positions want both terms; kickers want only the proportional one; tight ends and
defences only the linear one. IDP positions fall back to a pooled fit — safeties turn up
24 times in 2025 and cornerbacks five, and a two-parameter mean-variance function fitted
on 24 rows is noise with a decimal point on it. The fallback is recorded per position
(`"source": "pooled"`) rather than applied silently.

**The blend is unbiased in the mean on started players** — overall residual +0.013 — so
this is purely a variance exercise. Worth stating because it is the precondition for
the whole thing: a calibrated interval around a biased centre is still wrong.

## The surprise, and why it does not matter

**Per player the normal 80% interval over-covers**: 0.816 at receiver, 0.826 at running
back, up to 0.884 at tight end. Weekly fantasy residuals are right-skewed with a floor
at zero, so a symmetric interval on a skewed distribution catches more than it claims.

That does not survive aggregation, and aggregation is what a matchup is. Summed over a
starting lineup the central limit theorem does its work:

| | |
|---|---|
| team-level 80% coverage | **0.802** (nominal 0.800) |
| predicted team sd | 23.22 |
| realised team sd | 22.88 |

So the per-player over-coverage is a fact about the marginals, not a defect in the
totals the probability is read off.

## Independence is measured, not assumed

The obvious objection to `sd_team = sqrt(Σ sd_i²)` is correlation: a quarterback and his
own receivers rise together, so a real total should be *wider* than the independent sum.
Measured, it is not — predicted 23.22 against realised 22.88, if anything a shade wide.

A fantasy starting lineup is mostly nine players on nine different NFL teams, and the
stacking that would drive correlation is the exception rather than the rule. Gate G-W4
is that claim's own test, and it passes at 1.5% error. `distribution.correlation_matrices`
remains the seam if a future measurement disagrees, but modelling it today would move no
decision.

## No Monte Carlo

The plan for this was 10,000 draws per matchup. Once the normal approximation is
calibrated to within 0.2pp there is nothing left for sampling to add except its own
noise and a seed to remember, so the probability is the closed form:

```
P(A beats B) = Φ((μ_a − μ_b) / sqrt(sd_a² + sd_b²))
```

Exact, instant, deterministic. The margin between two independent normals is normal.

## The gates, pre-committed

Written down before the fit was run, because a win probability is the one number in this
app a reader cannot sanity-check by eye. **A confident-looking 63% that is really a coin
flip is worse than showing no number at all** — so `app/matchup_sim.gate_note` returns a
"missing" state and the tab falls back to the projected margin if the model is absent,
and `python -m Scripts.outcomes.weekly --report` exits 1 if the gates fail.

| Gate | Bar | Result | |
|---|---|---|---|
| G-W1 | team 80% coverage within [0.77, 0.83] | 0.802 | **PASS** |
| G-W2 | worst predicted decile within 5pp of realised | 0.028 | **PASS** |
| G-W3 | Brier beats uninformative 0.500 | 0.2277 vs 0.2500 | **PASS** |
| G-W4 | predicted team sd within 10% of realised | 1.5% | **PASS** |

Calibration, by predicted decile:

| predicted | n | pred | actual | diff |
|---|---|---|---|---|
| 0.000–0.310 | 1,988 | 0.233 | 0.231 | +0.002 |
| 0.310–0.379 | 1,988 | 0.347 | 0.341 | +0.006 |
| 0.379–0.426 | 1,988 | 0.404 | 0.410 | −0.006 |
| 0.426–0.465 | 1,988 | 0.446 | 0.474 | −0.028 |
| 0.465–0.500 | 1,988 | 0.482 | 0.487 | −0.005 |
| 0.500–0.535 | 1,988 | 0.518 | 0.513 | +0.005 |
| 0.535–0.574 | 1,988 | 0.554 | 0.526 | +0.028 |
| 0.574–0.621 | 1,988 | 0.596 | 0.590 | +0.006 |
| 0.621–0.690 | 1,988 | 0.653 | 0.659 | −0.006 |
| 0.690–1.000 | 1,988 | 0.767 | 0.769 | −0.002 |

**Evaluated on every within-league-week pair, not on the real schedule.** The real
pairing was the obvious choice and is the wrong one twice over: the `team_stats`
artifact that records who played whom had never been built, and the question "is this
probability calibrated" does not depend on the fixture list. All pairs is a larger,
unbiased evaluation set — 9,940 pairs against the ~900 the schedule would have offered.

**Both directions of each pair, and finding out why is the one thing here that changed
a result.** The first version emitted each pair once, and G-W2 came back 0.046, 0.050
and 0.078 on *identical inputs* — a flaky test on a fixed seed, which is a symptom
worth chasing rather than re-running. The cause: which team a pair is emitted as
decides which decile it lands in, because (A, B) contributes `p` and (B, A) contributes
`1 − p`. Brier and accuracy are symmetric under that flip and never noticed; the
calibration curve is not, so the gate was a function of Polars' group-iteration order.
Emitting both directions makes the curve symmetric by construction — which is why the
table above mirrors around 0.5 — and the gate deterministic. The headline numbers did
not move; the confidence in them did.

## What it buys on the page

The Matchup tab shows the probability, both 80% score bands with their σ, and — the
part that makes the start/sit list worth acting on — **what a change is worth in
probability rather than in points**. On Weenieless week 1, fixing the lineup adds 19.6
projected points and moves the win probability by **+21.8pp**, from 53% to 75%. On
Knights the same tab reports +1.6 points and +1.4pp. Same tab, same week, two very
different reasons to care.

## What is left, and the two honest caveats

- **Kickers and defences carry no fitted spread** in the *player* sense that plan 28
  covers — its predictive distribution is fitted on the usage model, which covers
  QB/RB/WR/TE. They are fitted here (`K` and `D/ST` have their own coefficients above),
  so the weekly path is actually better off than the season path. What remains
  unfitted is IDP, on the pooled fallback.
- **Independence is a stated assumption with a passing test, not a proof.** One season
  of nine leagues. If a future season disagrees, G-W4 is what will say so.
- **The training set grows by one season a year and cannot grow backwards.** Re-run
  `--fit --report` each off-season; the gates are the acceptance criteria and are
  already written down.
- **No playoff-odds swing.** "What this result does to your seeding" is
  `simulation_utils.playoff_odds_swing`, which still needs porting off the live
  `League` object and caching into the store during refresh — [plan 40](40-frontend-restructure.md)'s
  owed list.
