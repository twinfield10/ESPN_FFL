# Upgrade plans — 2026 season

Small, self-contained plans from the pre-season scan on 2026-08-01. Each one is
Problem / Evidence / Fix / Effort, so it can be picked up independently.

Phase 0 (2026 rollover, pipeline de-duplication, season-scoped paths, docs,
tests) is already done — see [`../STATE_OF_THE_REPO.md`](../STATE_OF_THE_REPO.md).
These are what the scan turned up *beyond* that.

## Which numbers in here drift

A number in a plan is one of two kinds, and confusing them wastes a re-measurement.

**Fitted-artifact numbers are stable and should reproduce exactly.** They come off a
persisted fit — `Data/NFL/vacancy_transfer.parquet`, `injury_episodes.parquet`, the model
files under `Data/NFL/models/` — and they only move when something is deliberately
refitted. Audited 2026-08-26, these reproduce to the digit: plan 27's 3,056 episodes,
1,555 returns, 580 long absences and hamstring recurrence at 9.9% on n=223; plan 28's
recapture shares of 0.814 / 0.452 / 0.471; plan 33's 59% / 45% / 36%. If one of these
does not reproduce, something is wrong.

**Live-board numbers are a snapshot and will not reproduce.** They are measured against
whatever the board held that morning, and rosters, depth charts, injuries and ADP all
move daily through camp. Plan 31's TOMCAT identity range read 0.658–1.704 on 2026-08-24
and 0.612–1.704 two days later, from the same code. Quote these with an *as at* date and
read the shape rather than the endpoints.

**A third kind is neither, and it is the one to watch: a number that never reproduced.**
Plan 32's phase 1 claimed +1.8% at quarterback and +4.4% on quarterback movers. Measured
at its own original commit in a detached worktree, before anything else had landed, it
comes back +0.34% and +0.03% — identical fold for fold. It was wrong when written rather
than overtaken, and the way that was established was to check out the commit that made
the claim and re-run it there. That is the test worth applying before building on any
measurement whose code has since been deleted.

Every plan carries one of three statuses, recorded in **two** places that must agree:
a `**Status:**` line under the plan's own title, and the section it sits in below.
**IN PROGRESS** means partly built with work owed; **TO DO** means not started;
**COMPLETE** means nothing left to build — including a plan whose measured answer was
*no*, and one superseded by a later plan. A plan's richer narrative status lives in its
`**Where it stands:**` line.

The two copies are checked against each other by `tests/test_plan_status.py`, so changing one and not the other fails the suite rather than going quiet — which is how 31 and 32 came to read *"not started"* while phase 1 of each sat built in an open PR.

## IN PROGRESS (10)

Partly built, with work still owed. The **Status** column says what landed; the last column says what is left.

| # | Plan | Status | Why it mattered / what is left |
|---|---|---|---|
| 02 | [BetOnline access](02-betonline-access.md) | **Partly resolved** | Season props wired up with IDP; the **weekly** props API still 403s and needs a decision |
| 03 | [Projection source coverage](03-projection-source-coverage.md) | **Partly done** | Renormalisation and provenance landed. Left: the weight re-tune (now unblocked by 16) and `scrape_pinnacle.py`'s import-time Selenium scrape |
| 05 | [Dependency upgrades](05-dependency-upgrades.md) | **espn-api done** | 0.46.0 also silently swapped stats for points, caught by the equivalence harness. Rest of the upgrades open |
| 09 | [Draft views](09-frontend-draft-views.md) | **Board + tendencies done** | Board page renders for all nine leagues, with owner tendencies on it ([23](23-owner-tendencies.md)). Split into **Board / Values / League** tabs on 2026-08-14, with search, team and bye filters. 2026-08-17 started reading ESPN's `draftSettings`, which nothing had: **GOP is a 2-keeper $250 auction** and its board was hiding 252 carried-over players as "unavailable" before a single keeper had been declared, while every league's `$` column was denominated in ESPN's $200 rather than its own. Adds the keeper price (measured to be *what the current holder paid*), and a **Cash** value lens for auction leagues — under ADP, GOP's best values were three negative-VOR backup tight ends. Turned up that every *second* league change in the sidebar was being silently discarded. Left: **Live Draft** |
| 14 | [Thin Google Sheets](14-thin-google-sheets.md) | **Step 1 done** | Sheets is a renderer over the store. **Kept**, not retired — readable on a phone with the laptop shut. Left: step 2.3, `oauth2client` → `google-auth` |
| 25 | [`results`, the artifact that reaches back](25-results-backfill.md) | **Done, one league** | The store held one played season while `draft.parquet` held ten, so every retrospective question was half-answerable. `lineups` **cannot** be built for a past season — it carries FantasyPros columns and FantasyPros serves no season parameter — so `results` holds what was scored and nothing else. Winfield_Football backfilled 2019–2025; `espn_api` serves no box scores before 2019. Turned up that ESPN's three team identities are each stable over a different span: join within a season on `team_name`, group across seasons on `owner_id` |
| 26 | [User accounts](26-user-accounts.md) | **Seam built, login not started** | Nine configured leagues narrow to the viewer's four in exactly one place (`app/auth.py`), so a login lands in one function rather than in every page that ever called `store.list_leagues`. Deliberately **not** a security boundary yet — the enforcement step belongs at the store read, and building it against an audience of one would be enforcement nobody could test |
| 28 | [Outcome distributions](28-outcome-distributions.md) | **Phases 1-3 built 2026-08-24 — G-D0/G-D1/G-D3 pass, G-D2 and G-D5 fail** | **The board now carries a real forecast interval on all nine leagues** — `pts_p10`, `pts_p90`, `p_top12`, `p_bust` — from a Monte Carlo over the usage model's fitted per-stat distributions, coupled by a Gaussian copula on their probability transforms and rescaled onto `TRUE_Points`. **No projection moved**: `TRUE_Points` byte-identical on a rebuilt board. **G-D1 passes** at coverage 0.730 and slope 1.072 walk-forward 2021-2025; **G-D3 passes** at 13.5% within position — QB 23.1%, WR 22.9%, TE 3.8%, RB **0.0%**. **G-D2 fails** — the room-level joint draw is +2.1pp closer to nominal for backups against a 5pp bar, so phase 1 ships alone and the room machinery is off by default. But entrenched starters move **+0.0pp**, so the vacancy effect is exactly as specific as claimed and it is the magnitude that fails, not the direction; the shape change is real (a backup RB's p90/p50 goes 1.70 to 1.82-2.16 at constant mean). **Two defects found in shipped code.** The published `USG_<stat>_low`/`_high` were the realised-season spread rescaled onto an if-healthy centre — on **14.0% of projected cells a p10 equal to its p90**, now 0.5%, fixed without moving a mean. And **half of `expected_games` is role rather than availability**: the proportional rescale over-projects a realised total by **+8.8% to +26.7%** and the fitted exponent is **0.32-0.49**, so a player who plays twice his expected games produces about the square root of twice the output. That same bias sits in `to_full_slate`'s **mean**, which is in the blend at 0.25 — named, and deliberately not touched before a draft. Also caught a **double-count** (the transfer added rather than redistributed, lifting the median backup 123→156) and a **contaminated control** (the 2016-2024 chart lists 2-3 rank-1 backs in a third of rooms, so `depth_rank <= 1` held players receiving the treatment). **The materiality gate cleared by 17.5x against a 1.5x bar and the fragility gate failed on its own pre-committed terms**, so phases 1-3 are justified and phase 5 is struck. For depth-rank >=2 RBs and TEs the board shows a **16.0%** floor-ceiling width against a realised spread of **280.5%**, and it is not the availability tail -- restricted to players who managed 14+ games it is still 185.8%, **11.6x**. Put legibly: **the board's own floor-to-ceiling contains 4.6% of realised outcomes** (10.1% among 8+ game players) against the ~80% a floor and a ceiling implies. G-D5 re-ran the fragility premium inside Vegas team-strength tertiles and found it monotone in **1 of 3**, with the best-powered tertile running 142.1 -> 94.3 the **wrong way**: a fragile incumbent's backup scores *fewer* points, not more. The gate named that outcome in advance -- premise dead rather than merely confounded. The board's `floor`/`ceiling` is source *disagreement*; nothing anywhere publishes forecast uncertainty in **points** — every stat has a distribution and the one number a drafter reads does not. Measured first, and it already cut two of the three things asked for. **A backfield is near zero-sum and a receiver room is not**: the lead back's 17.42 opportunities a game go **81%** to the next three backs and the room keeps 93% of its volume, while a lead receiver's understudy gains **0.59 of 7.72** targets and the offence throws 1.25 fewer times. Paired within player-season, an RB2 goes 4.15 → 9.86 points a game when the lead sits (**+5.72**, ≈46 points over an eight-game absence) and a WR2 goes **+0.07**. The **ACL question is unaskable** — 73.1% of 992 long absences carry no body part, because the report goes quiet on reserve — but duration substitutes cleanly: next-season ppg ratio 0.837 / 0.726 / **0.641** across 1–3, 4–7 and 8+ weeks out against a healthy control's 0.906, and P(≥14 games) **0.31 against 0.59**. So ~a quarter of players return to within 90% of prior form where half of healthy comparables do. **The fragile-RB1 premise does not survive as stated**: RB2 season points are 111.4 behind a clean incumbent and **92.4** behind one who missed 3–5, and the incumbent's own prior absence stops predicting once you identify him pre-season instead of by this season's touches — which reversed the whole result and was pure survivorship. That payoff table is **confounded by team strength**, and plan 21's `Scripts/draft/handcuff.py` already measured the confound's sign (a weak team's RB2 is 19 carries behind a strong team's), so phase 5 is a cheap stratified re-test rather than a cut. The case for building anyway is magnitude: the transfer is ≈46 points over an eight-game absence against that column's entire ±13-carry range, and a bimodal spread is not a quantity a mean tiebreaker can hold. Population-level persistence is real (r = 0.31, both-bad seasons → 78.5%) but it is **role, not body**, matching plan 18's snap-share finding. Deliverable is therefore a **distribution, not a bump**: a room-level Monte Carlo over a shared availability draw, since the RB2's two worlds live on the RB1's row and no per-player marginal has a channel for that. Four pre-committed gates, and G-D0 can reject the plan in an afternoon. Ships its evidence layer ahead of its model layer — `python -m Scripts.outcomes.evidence` reproduces every figure, and building it caught a **nondeterministic room rank** (tied reserves swapping places moved a TE room's volume between runs) and a **skill-position filter that deleted the 8 of 217 incumbents who missed an entire season** — the severe tail, dropped while the cohort size stayed 217, biasing the result toward the answer that fragility does not matter |
| 32 | [Movers](32-movers.md) | **Phase 1 built, held as draft [PR #24](https://github.com/twinfield10/ESPN_FFL/pull/24) until after the drafts — G-M1/G-M2/G-M4 all pass; `MODEL_VERSION` 1.3.0 forces a refit** | TOMCAT has **no team-specific opinion about a veteran who changed teams**: the only two regressors that know anything about the destination are a *flat* `team_changed` coefficient -- −0.399 targets/game, applied identically to all 49 veteran WRs who moved for 2026, wherever they went -- and his depth-chart slot. `VETERAN_SITUATIONAL_REJECTED` measured the team context out **pooled over all veterans**, where movers are a minority; this closes the open question of whether it survives on movers alone. It does. The mover penalty is real and is mostly a **quarterback** effect (median Δ vs stayers: QB **−2.24** attempts/g, WR −0.45, RB −0.26, TE −0.14), and at WR it widens with age (+0.05 at 25–26 to −0.84 at 31+) against a flat coefficient. There **is** a real bias -- the model over-projects a WR moving into a smaller receiving pool by **+0.53** targets/g, positive in 5 of 6 seasons, an 0.80 spread against the bigger-pool end -- and **it cannot be fixed with the pool.** `moved × pool_delta` flips sign across folds; a dedicated mover-only arm is **−0.7%** and leaves the bias at +0.47. Cause: a team's receiving pool is `rho = +0.285` autocorrelated, with a year-to-year change sd (6.18) **larger** than the cross-team sd (5.18), so last year's pool is a broken instrument for this year's. An **oracle** on the actual pool is worth only +2.6%, which bounds the whole category. **The win is elsewhere:** the model looks back exactly two seasons, and adding a 3/5-year peak is **+4.5% MAE and ρ 0.456 → 0.531 on QB movers plus ~1% on all QBs**, stable in 5 of 6 folds -- and `p3_peak` (+0.221) **outweighs last season** (`p1` +0.061), because a two-year window confounds "lost the job" with "is not a starter". Contract `apy_cap_pct` (already pulled by plan 22, 86% mover coverage) is worth **+2.4% on WR movers and hurts every other WR**, so it belongs behind an interaction. Pressure rate and O-line grades are **not in the repo** -- only `sacks_suffered` -- so destination quality needs a `load_pfr_advstats` pull before it can even be tested |
| 34 | [Stat-first audit](34-stat-first-audit.md) | **Phases 1-3 built 2026-08-26** | The premise is that a projection is a **stat line** and points are what a league's rules do to one. Mostly true, and an audit found **seven places it was not**. Two new measurements, both of which found something. **The shipping blend had never been scored per stat** -- every scored evaluation in the repo judges TOMCAT or a calibration curve -- and per stat it *loses* on `rushingTouchdowns`, by +2.4% against ESPN, +5.1% against FantasyPros and +6.0% against Pinnacle, on all three populations, while the fantasy-point MAE for the same league-season reads a clean **-2.2% win**. Points MAE cannot see it because yardage carries the variance. **And volume persists where efficiency does not**: carries/game +0.895 year over year against +0.260 for the rate it is multiplied by, and every one of the eight shipped `SHRINKAGE_K` constants sits **below** its measured credibility floor, by 1.4x to 4.6x. **The blend was discarding volume entirely** -- four sources publish attempts, completions and targets and `TRUE_` had none of them, so no coherence check was possible and plan 31's team attempt budget could not be expressed. Now blended, verified additive at max |delta| **0.0** on every `<prefix>_Points` column across all nine 2026 boards. The team budget turns out to **pass** at 59.1-65.0 plays a game over 32 teams. **The weekly path patched points with a scalar that had no stat behind it** -- ESPN's unpriced residual added to every source's total, `TRUE_Points` included -- and it was camouflage: underneath it, ESPN's doubled yardage was escaping a correction that compared it against a FantasyPros line that was usually not there yet (Deebo Samuel week 6 stored at **136.3** receiving yards against an ESPN total consistent with 68), and six scored milestone-bonus rules in one league are mapped, present and **identically zero for all 3,095 player-weeks**. Also **a QB interval excused by an abstention that no longer exists**: `passingYards` covers **58.9%** against a nominal 80% and the excuse in two places said the model abstains at quarterback, which stopped being true on 2026-08-07. Left: source bias adjustment and any weight movement, **deferred on purpose** until the individual stat lines are right |

## TO DO (4)

Not started — nothing built, and no evidence gathered beyond the original scan.

| # | Plan | Status | Why it mattered / what is left |
|---|---|---|---|
| 04 | [Matchup-period handling](04-matchup-periods.md) | Not started | Winfield_Football silently loses a week of data |
| 06 | [Performance](06-performance.md) | Not started | Quadratic `pd.concat` in row loops; a duplicated `fetch_league` round-trip; a process-wide warnings filter |
| 08 | [Week-to-week views](08-frontend-weekly-views.md) | Not started | Unblocked since 07 |
| 19 | [Weekly usage model](19-weekly-usage-model.md) | Not started | **Where the larger edge is.** Trailing expected production beats trailing actual at predicting next week (R² 0.2907 vs 0.2702), and it is the only head that gets the live injury report |

## COMPLETE (20)

Nothing left to build. This includes plans whose answer turned out to be **no**: a candidate built, measured and rejected by its own pre-committed gate is a finished plan, not an abandoned one. It also includes plans superseded or retired by a later one.

| # | Plan | Status | Why it mattered / what is left |
|---|---|---|---|
| 01 | [Scoring coverage gaps](01-scoring-coverage.md) | **Done** | Two GOP kicker rules were silently dropped and nothing detected it |
| 07 | [Frontend foundation & data store](07-frontend-foundation.md) | **Done** | 11ms to read a league back from parquet against ~8s to rebuild it |
| 10 | [Scoring registry](10-scoring-registry.md) | **Done** | Scoring was re-derived from a mutable live object 4× per league and never recorded |
| 11 | [Per-slot scoring](11-per-slot-scoring.md) | **Done** | GOP's D/ST was inflated ~16%, and every league credited offensive players for imputed defensive stats at D/ST rates |
| 12 | [Season projections](12-season-projections.md) | **Done** | Season props blended and scored per league — the draft board's input |
| 13 | [D/ST from Vegas lines](13-dst-from-vegas-lines.md) | **Superseded by [30](30-dst-model.md)** | The only position with zero market coverage. Its `E[f(X)]`-over-tiers instinct was right and is now measured — `f(E[X])` understates the best third of defences by **12.24** points and overstates the worst by 4.26 — but its **scope was too narrow**: Vegas also beats prior season on sacks (0.464 vs 0.203) and interceptions (0.357 vs 0.113), which carry 56% of the score in six leagues, so the market is not just a points-allowed input. Also wrong that this "waits on posted 2026 lines": all 272 games are already priced. Read 30 instead |
| 15 | [Draft board: ADP, VOR, tiers](15-draft-board.md) | **Done** | Nine league-aware boards in 16s. Also fixed the season path never using plan 11's per-slot scoring |
| 16 | [Usage data layer](16-usage-data-layer.md) | **Done** | The shared extraction and feature layer, the ID crosswalk, and the gates. **G0 passed** (+0.832 residual correlation with ESPN against FantasyPros' +0.988); **G1 failed** on the crude baseline and located the deficit in not knowing who plays |
| 17 | [Draft usage model](17-draft-usage-model.md) | **Superseded** | Split into 16 / 18 / 19 / 20. Two of its claims were measured and did not survive; the stub records which |
| 18 | [Season usage model](18-season-usage-model.md) | **Shipped, and carrying real weight** | The draft head, on all nine boards. **Best-covered source in the pre-season blend** — 23.1% real against ESPN's 13.1%. **Rookie arm is the win** — ρ ≈ 0.61 against ~0. Wired at 0.0 so turning it on would be one number, then turned on the same day: `WEIGHTS` is an equal **quarter** each to ESPN, FantasyPros, **BetOnline** and `USG` with **Pinnacle** at zero, and the QB abstention was lifted once the depth chart entered the veteran arm. This row said "weight 0.0" until 2026-08-14 and was wrong for a week, then said "three-way split ... BOL at zero" until 2026-08-18 and was wrong again. **Two measurements from 2026-08-18 qualify the coverage claim:** after renormalisation the model's *effective* weight in `TRUE_` is 0.21 on receiving stats, 0.08 on rushing and **0.02** on passing — ESPN has no `_is_imputed` columns at all, so its weight is never dropped and it carries 0.69–0.91 — and in **all nine leagues zero players** have a real `USG` receiving line where ESPN has none, because `PRICED_OUT_SHARE` withdraws the model precisely where ESPN says a player will not play. It is a second opinion on ESPN-covered players, not coverage |
| 20 | [Consensus sources](20-consensus-sources.md) | **Retired 2026-08-24 — its evidence was wrong** | Deprioritised on FantasyPros' +0.027 marginal value against ESPN's +0.068. That was measured on **non-imputed cells only**, and FantasyPros was real for 60 players -- ten per position -- so the sample was *the top ten at each position and nobody else*, which is exactly where every source agrees. With the registration fence lifted and FantasyPros at 592, mean ESPN-FP disagreement is **6.0% inside ADP 50 and 31.6% outside 150**: five to six times larger in the region the measurement could not see. Correlation stays ~0.99 in every band, which is why it hid -- on a quantity spanning 200 to 1,400 yards, correlation is dominated by scale. This does **not** prove a sixth aggregator is worth adding; it removes the evidence that said it was not. **Plan 16 step 0's whole matrix is suspect**, not just FantasyPros' row: every other source's independence was scored against a *rest* that was largely ESPN wearing a FantasyPros badge. Re-measuring is owed and frozen until after the drafts |
| 21 | [Depth charts, scheme, play-caller](21-coaching-and-scheme.md) | **Done** | 2026 depth charts pulled past nflreadr's season guard — the daily snapshot that made the rookie arm work. Coach and coordinator priors built and **measured out** of both arms: the depth chart already carries their signal |
| 22 | [Feature research for the season head](22-feature-research.md) | **Measured, nothing merged** | Routes, Next Gen Stats, red-zone role and contracts pulled and tested; ridge swept. All eleven experiments rejected. The finding: player-level context that is a *function of past usage* does not survive either, because past usage is already the strongest regressor. Ships the data layer, the lab and `docs/model_lab.html` |
| 23 | [Owner tendencies](23-owner-tendencies.md) | **Done** | 5,748 picks across 36 league-seasons, all of it one request per season. Every measurement is leave-one-out against the room and season-matched. **112 managers, 103 with a measured tendency.** Caught ESPN pre-creating a full set of picks for a draft that has not happened |
| 24 | [S3 as the system of record](24-s3-data-flow.md) | **Done** | `s3://espn-ffl-data` holds every tier, Hive-partitioned so a query engine can prune, and `--verify` proves 249 current-state files SHA-256 identical against disk. `Data/` is untracked now; the app reads S3 by default. The nightly push also writes a **dated board snapshot**, which retires the problem `Data/G2/` exists to work around and makes ADP drift through camp measurable for the first time |
| 27 | [Injuries as duration, recovery and recurrence](27-injury-model.md) | **Phases 1-4 done; multiplier REJECTED** | The one severity source in the repo was being overwritten nightly with no backfill, so the daily ESPN archive shipped first. `ctx.load_injuries` had ten seasons behind it and **no caller outside its own tests**. Episode table built: 3,056 episodes, 1,555 returned, and **580 absences of 4+ games where the injury report alone finds 99** -- reserve status is what recovers them. Deriving the reserve allowlist caught `R59`/`R62` as **COVID-19 reserve, not injury** (~456 fabricated episodes in two seasons). Hamstring recurrence **9.9% against a published 11.9%**. The measurements disagree with the ask in two useful ways: the post-return dip is **three times shallower and half as long** as hypothesised once the placebo is netted out, and hamstrings/concussions cost nothing in efficiency -- they cost recurrence. Phase 3 puts **Body Part** and **Wks Out** on all nine boards as diagnostics, resolved through a six-rung ladder -- and running it on live data broke the design four times: a **past** ACL with a return date read as 46 weeks out at ADP 36 (Nabers), a body-part average put a player who had just practised at 3.5 weeks out at ADP 4 (Nacua), and a comment about a **teammate** tagged Allgeier with Love's high ankle sprain. All four were invisible in aggregate. **Gate G-B0 answered:** ESPN *does* already mark returning players down -- but by a flat ~8% that is not a recovery curve at all, against a real drop of 14% rising to 21% by the third game back. So the multiplier must be a **residual** (0.94/0.87/0.87), and the raw curve applied over the top would have double-counted half the effect. **The curve is fitted, walk-forwarded, and rejected by its own pre-committed gates** -- so the outcome is the one the plan named in advance: ship the columns, multiply nothing. Global fit `a=0.163, tau=1.14`, almost exactly the pre-registered prediction; `tau` 0.73 for a hamstring against 3.87 for a knee is the "deep then fast vs shallow then slow" contrast one parameter cannot express. Three cells **abstain on their own evidence** (concussion, lower-body soft tissue, ankle). Calibration slope **1.05** -- a cell predicted to lose 20% loses 20% -- but the accuracy gain is ~**1%** against a 2% bar. MAE and RMSE **reverse the candidate ordering**, because MAE on a right-skewed target rewards any downward bias and the prediction is a conditional mean: the hypothesised 0.75 ladder scores best on MAE and worst on RMSE. Hazard rejects at Brier 0.9898 vs 0.98, though the pooled hamstring rate passes its external check (9.8% vs a published 11.9%). The hand-written severity file lives **tracked** at `config/injuries/<season>.yaml` -- it is judgement, not data, and its git history is the point. Measured cadence: ~2.7 relevant injuries a week, ~1.2 of them costing 3+ games, so a pre-draft scan plus about one entry a week |
| 29 | [Kickers](29-kicker-model.md) | **BUILT, and STAYS at weight 0.0 — channel P passes, channel F REJECTED at G-K2** | Kickers are **100% ESPN** — FP/PINNY/BOL all imputed, `usg_arm` null for all 58 — a starting slot in nine leagues with no model and no second opinion, worth a 48-point spread on Winfield and **251.3** for GOP's top kicker. Opened to explore skill vs red-zone failure; the evidence settles both. **Individual skill does not exist as a projectable quantity:** FG conversion rate YoY `r` = **0.009** (n=222), and his own FG attempts/gm `r` = **−0.006**. What sticks is the offence — PAT attempts/gm 0.346 for the kicker, 0.399 for the team — and it **does not travel**: a kicker changing team takes his PAT `r` from 0.386 to **−0.040**, proving the volume is the team's. **The red-zone hypothesis is confirmed and is an interaction:** high red-zone volume + poor conversion gives **2.24 FGA/gm** against **1.74** for low-volume/good-conversion, monotone across every row, **+8.4 attempts** a season — and in the good-conversion column volume stops mattering entirely (1.74/1.80/1.78), because an offence that finishes does not kick. Vegas settles the other half: implied team total → PAT attempts **r = 0.844** and TDs 0.848, but FG attempts only **0.117** — the market prices scoring, not failing to score. So: two channels, a positional constant for accuracy, and the only kicker-specific term is share of makes from 50+ (`r` = 0.335). **Game script is the biggest environment signal and its usual story is backwards:** heavy favourites score more (8.89/gm vs a heavy dog's 6.12) but **entirely via PATs** (1.41 → 3.31), while FG attempts *peak at a modest favourite* (2.09) and fall for double-digit ones. Favourites **settle less**, not more — 0.185 FG attempts per red-zone play against a heavy dog's **0.232**, kick share 39.2% vs 49.3% — because they reach the red zone 42% more often and punch it in; settling is what bad offences do. The FG-richest script is a **close win** (0.239/RZ play, 2.27 FGA), not a blowout. And **net of the implied team total the spread is flat** (8.81/8.75/8.81 in the top band), so it is expected scoring renamed and is excluded, with G-K5 pre-registering that null. Best pre-game cell is interior: a solid favourite in a *moderately* high total, **8.95/gm vs 6.56 = +40.7 a season** — shootouts are worse for a favourite because FGA collapses to 1.82. Pre-registers its own likely humbling — red-zone *conversion* has YoY `r` of just 0.095, so channel F is expected to land near league-average. Also clears two false alarms: the `214` FG-yardage colName blends and scores correctly (`1371.49 × 0.1 + 46.10 − 4.16 = 179.09` = `TRUE_Points`), and kicker `TRUE_Points` matching `ESPN_projected_total` is source collapse, not a passthrough. **Week vs season, measured:** ICC of weekly kicker points is **0.105** — 89% of the variance is week-to-week — while the market inputs are far more team-persistent (implied own total ICC **0.530**), which is what makes a season model possible at all. But season-average environment spreads the position only **115.0 → 142.7 points, a 1.24× ratio**, and flattens at the top (q3→q5 buys 8.2 points). **At draft time kickers are near-undifferentiable — one round of ADP beats the entire league-wide spread — so build the weekly path first**, where the range is 2.4 pts/gm. **Left at 0.0 on 2026-08-24 and that is the decision, not a delay:** channel P is +45.9% held out but **channel F fails G-K2 at +1.2% against a 5% bar**, and blending the position carries the failed channel in with the good one. Building the gate turned up a **real bug** -- per-bucket misses were allocated on `made_share_*`, but makes concentrate short and misses concentrate long (a kick inside 40 is 57.8% of makes and **15.6%** of misses), so short misses were over-stated **3.7x** and 50+ misses under-stated 2.9x. On the board that was 2.95 short misses a season against ESPN's 0.60, now 0.80. Short misses are a scored penalty in the leagues that price them, so the error had a sign |
| 30 | [D/ST](30-dst-model.md) | **BUILT and TURNED ON at 0.25 on 2026-08-24 — G-DST2(a) passes 34-46% in all nine leagues; tiers pass, four components shrink to the mean** | 32 defences on every board from one source, `usg_arm` null for all, and the only position with **zero market coverage**. Reverses its own framing twice. **Nine ladders are a smaller problem than they look** — scoring every team-defence season under each league's slot-16 rules gives a median pairwise rank correlation of **0.968** (lowest 0.918); all nine rank HOU first, eight rank SEA second. So **one model, nine ladders applied at the end**, not nine models. **But 71% of the score sits on noise:** in six leagues sacks + INTs + fumble recoveries are 33.8% + 22.3% + 14.9%, with YoY `r` of **0.203, 0.113 and 0.015**, while the two forecastable components (points allowed 0.277, yards allowed 0.260) carry 22.3% — which is why total D/ST points stick at **0.220–0.267 in every league**. The most *valuable* events have **negative** persistence (defensive TDs −0.052, safeties −0.015), and the two stickiest components (tackles for loss 0.389, QB hits 0.322) are scored in **one league of nine**. **GOP is a different position** — 40.7% of its D/ST score is the points-allowed ladder against 7.6% for the default six, and three leagues have no yards-allowed tiers at all. **Vegas is the way out, far more broadly than [13](13-dst-from-vegas-lines.md) scopes it:** implied points allowed beats prior season on **seven of eight** components — sacks 0.464 vs 0.203, INTs 0.357 vs 0.113, fumble recoveries 0.193 vs **0.015**, points allowed 0.816 vs 0.277 — because opponent offences drive defensive events and the market prices all 272 of 2026's games. **And the tiered ladder must be integrated, not evaluated at the mean:** weekly SD of points allowed is 9.57 against 4–7-point tiers, so `f(E[X])` understates the best third by **12.24** points and overstates the worst by 4.26 — a **16.5-point compression** of the exact range the component exists to separate. **Game script is the largest environment effect in the repo and mirrors the kicker's:** a heavy favourite's defence scores **9.18/gm against a heavy dog's 1.06** in the default six, and the channel is realised rather than predicted — opponent pass share **47.8% in a blowout loss to 63.2% in a blowout win**, rush attempts 32.9 → 20.6, taking sacks ×2.2, INTs ×3.6, def TDs **×7**. Garbage time costs nothing (11.49 pts allowed in a 15+ win, the lowest band). Best cell is a **heavy favourite in a LOW total — 8.55 vs 0.88, +130.3 a season** — the total running *opposite* to [29](29-kicker-model.md)'s kicker, so the two positions want opposite games. And unlike the kicker, **the spread survives the implied points allowed** (+2.20 to +2.75/gm within band, sacks 1.86 → 2.50): the implied number prices the opponent's offence, the spread prices who will be ahead, and only the second makes anyone throw — so G-DST6 and G-K5 are pre-registered in opposite directions on the same variable. Coordinator changes look like a −1.62 pts/gm improvement and are pre-registered as mean reversion (`r(prior,next)` barely moves, 0.157 → 0.134); continuity enters as a weight on history, not a feature (0.145 → 0.215 across tertiles). **Week vs season, measured:** ICC of weekly D/ST points is **0.118**, so like the kicker it is ~89% a weekly quantity — but season-average environment still spreads it **118.2 → 38.0 points, a 3.1× ratio**, monotone across all five quintiles against the kicker's 1.24×. **So D/ST is genuinely draftable and a kicker essentially is not**, and since the weekly model sums to the season one there is one build here rather than two. **Turned on at 0.25 on 2026-08-24.** `python -m Scripts.dst.gates` runs **G-DST2 baseline (a) walk-forward and it passes in all nine leagues at 34-46%** below prior-season points (model MAE 20-24 against 31-43), against a 10% bar. **G-DST4 passes at exactly 0.0000** -- QB/RB/WR/TE median `TRUE_`/`ESPN_` does not move at all. **G-DST2(b), against ESPN, is not run and the module says so:** no pre-season ESPN D/ST projection survives for a season whose result is known, and re-requesting one today returns a projection that has seen its own season; the 2026 board in the store becomes that record in 2027, the same answer plan 18's G2 reached. So 0.25 is a **co-equal** weight with ESPN, not a claim to beat it. And the level gap that looked like a blocker was **ESPN's error, not the model's**: actual league-wide points allowed is **22.74** per team-game over 2016-2025 and 23.01 in 2025, the model says 22.89, ESPN says **22.00** -- every defence above average. D/ST ordering now differs from ESPN at rho 0.863 where it was 1.000 |
| 31 | [Team-coherent TOMCAT](31-team-coherent-tomcat.md) | **All three phases shipped 2026-08-26, seven gates passing** | TOMCAT projected each player alone, so the team identities it implied were wrong: `sum(receivingYards)/sum(passingYards)` ran **0.658–1.704** across the 32 teams against ESPN's flat 1.000. Phase 1 closed the identity and **missed G-T2** — a per-team constant multiplier cannot reorder inside a room, and its league-wide QB Spearman against its own input is **0.956**, so ordering had nowhere to go. Phase 2 allocates a team's seventeen starts by depth rank and cohort, fitted on **644 QB player-seasons** — a listed QB1 is 13.88 starts settled, 10.11 a mover, 9.06 a rookie — and clears G-T2 at **+0.0305** on a +0.02 bar while taking G-T0's QB-games half from **1 of 32 to 32 of 32**, the half phase 1 recorded as *not reachable*. **Three of the plan's premises did not survive re-measurement**, including *"a no-op on thirty of the thirty-two teams"*, which described the board's ESPN-draftable subset rather than the model's universe, where it is the inverse. Phase 3's premise failed the same way — nothing is vacated on a board whose lines are already on a full slate — and what was actually owed was a **defect in phase 1**: `reconcile_identities` scaled identity *pairs* independently, so `receivingReceptions`, whose counterpart the model never projects, was never scaled while the yards beside it were. That rewrote implied yards-per-reception on **all 665 pass-catchers** by a median 18.4%, and left team receptions at 365–640 against a real 300–450 in nine leagues that score a reception at ten times a yard. Correcting by volume family fixes it with **no trade**: both identities still close, rate drift goes 18.40% → **0.00%**, receptions to 337–473. Live on the board since 2026-08-26 |
| 33 | [Role resolution](33-role-resolution.md) | **Phases 1-2 built 2026-08-24; phase 3 built and REJECTED at G-R2 2026-08-25** | **Role uncertainty is real, and it is already in the interval.** Phase 3 built both candidate mechanisms on top of [28](28-outcome-distributions.md)'s simulation -- splitting the fitted dispersion by cohort, and drawing the true rank per simulation from the calibration table -- and both are worth **-0.3pp** of coverage walk-forward. **The premise has its sign backwards:** rookies cover at **0.801** against a nominal 0.800 while *settled* players sit at 0.701, so the cohort phase 3 was built to widen is the only one already right. The residuals do agree cohort matters (a rookie's CV is **1.6x-2.3x** a settled player's) -- but `Var = phi*mu + mu^2/k` already widens the interval at a smaller projection, and a rookie's projection *is* smaller (182 rushing yards against 382), so most of the cohort effect was a level effect the two-parameter form had already absorbed. Plan 22's generalisation from a new direction. G-R2 fails on its first clause anyway: 0.727 coverage against a bar of within 5 points of nominal. Both mechanisms kept, tested and off by default. Phases 1-2 stand, as the plan's own scope clause said they would. **Phases 1-2, unchanged:** The board treats a pre-season depth-chart entry as a fact and it is **a guess about a third of the time**. Reconstructs the chart the season actually revealed -- rank within (team, position) by per-game opportunity over weeks 1-3 -- and scores the pre-season one against it. Leak-free for a draft board because the early-season data is only ever a *training label from seasons already over*. **The chart is least reliable exactly where it is most needed:** a player listed as his position's starter really is one 59% of the time if settled, 45% if he moved and **36% if he is a rookie**. As a *feature* the payoff is small and measured -- a two-stage role model beats the best single signal only at quarterback movers (0.450 against 0.386, n=39) and is a wash everywhere else, and for rookies **draft capital alone beats draft capital plus the chart** (0.478 vs 0.458 at RB, 0.600 vs 0.582 at WR), which independently confirms the shipped `ROOKIE_REGRESSORS`. **The finding worth building on is variance.** Realised season volume conditioned on listed rank runs from p90/p50 = **1.16** for a settled QB1 to **2.24** for a mover TE2 -- while the board's own floor/ceiling is **9.0% wide** with the ceiling at 1.042x the projection, and varies by *position* (QB 7.4%, TE 16.3%) and **not at all by cohort**, when cohort is what decides whether a projection is knowable at all. So today's spread is not merely the wrong quantity (which [28](28-outcome-distributions.md) already said) -- it varies along the wrong axis. Phases 1-2 are diagnostics that move no projection; phase 3 replaces source disagreement with a role-conditional distribution and belongs scoped with plan 28 rather than separately |

### Local frontend

Plans 07, 08 and 09 replace the notebook-plus-Google-Sheets workflow, split because
the foundation blocked the other two. [26](26-user-accounts.md) is the seam that
decides which leagues any of them may show.

**07 is done**: the store lives at `Data/Store/<season>/<league_key>/`,
`python -m Scripts.refresh` builds it and `streamlit run app/main.py` reads it — 11ms
against ~8s to rebuild a league pre-season and ~23s in season. **09's board page is
done**, split into Board / Values / League tabs, and renders for all nine leagues —
though the picker now offers the viewer's four, per 26. **08 is unblocked and not
started.**

Every title, header and column label across the app is **Title Case**; captions and
explanatory paragraphs stay sentence case, because they are sentences.

Both plans have postscripts worth reading before touching that code, because building
each turned up bugs nothing would otherwise have reported — a same-day regression that
silently failed all eight `FA_*` Sheet tabs, and every per-source point column being
NaN on every stored draft board.

Sheets is **kept**, not retired: it is a published artifact readable on a phone with
the laptop shut, and five of the eight published leagues belong to other owners.
See [plan 14](14-thin-google-sheets.md).

## What is left

Ordered by when it matters, not by plan number. Everything not listed here is done;
each plan doc carries its own evidence and postscript.

### Before the draft

| | Plan | Why it is next |
|---|---|---|
| ~~1~~ | ~~**Show `USG_` on the board without blending it** — [18](18-season-usage-model.md) step 3~~ | **Done 2026-08-07.** Wired as the fifth source at weight 0.0 — scored per league, on all nine boards, and verified not to move `TRUE_Points` (max difference 0.0 over 1,026 rows) |
| ~~2~~ | ~~**Abstain for QB in the season head**~~ | **Done 2026-08-07 and then undone the same day.** The abstention was lifted once the depth chart landed in the veteran arm and quarterback ordering went positive; `ABSTAIN_POSITIONS` is now `()`. The backtest overrides it either way so the evidence stays reproducible |
| 1 | **[09](09-frontend-draft-views.md) Live Draft page** | The last draft-critical UI piece. Slips gracefully — the board on a second monitor works without it |
| ~~2~~ | ~~**Render the new `USG_` columns** — [09](09-frontend-draft-views.md)~~ | **Done 2026-08-14.** `USG`, `Δrk`, `Exp G` and **Model evidence** on every table, with the level caveat in the `USG` tooltip and spelled out in the page's own "what is missing" panel: `USG_Points` is injury-adjusted and `TRUE_Points` is not, so `Δrk` is the comparison that survives. The evidence column exists because an empty `USG` meant three different things — not modelled, withdrawn on availability, withdrawn on injury — and all three looked like agreement |

### Before week 1

- **[03](03-projection-source-coverage.md)** — the weight re-tune, now unblocked by
  plan 16. Also holds `scrape_pinnacle.py`'s Selenium scrape at **module import
  time** (no `__main__` guard), which launches Chrome on import and currently times
  out.
- **[04](04-matchup-periods.md)** — Winfield_Football silently loses a week.
- **[08](08-frontend-weekly-views.md)** — the weekly views. Unblocked since plan 07.
- **[13](13-dst-from-vegas-lines.md)** — its `E[f(X)]`-over-tiers piece can be built
  and tested against 2025 now; the rest waits on posted 2026 game lines.
- **[14](14-thin-google-sheets.md) step 2.3** — `oauth2client` → `google-auth`.
  End-of-life upstream, and better done before the season than during it.
- ~~**[29](29-kicker-model.md)** and **[30](30-dst-model.md)** — the two unmodelled
  positions.~~ **Both built; 30 turned on at 0.25 and 29 deliberately left at 0.0, on
  2026-08-24.** The gates separated them: G-DST2(a) passes in all nine leagues at 34-46%
  against prior-season points, while G-K2 fails at +1.2% against a 5% bar. What is left
  is the **kicker's weekly path**, which is where plan 29 measured the value, and
  **G-DST2(b) against ESPN**, which cannot be run until 2027. Original note follows.

- **[29](29-kicker-model.md)** and **[30](30-dst-model.md)** — the two unmodelled
  positions. Both share `Scripts/vegas.py`, and **G-K0 / G-DST0 assert the `spread_line`
  sign before anything is derived from it**, because getting it backwards produced a clean
  inverted result during the measurement. 30 **supersedes the D/ST half of
  [13](13-dst-from-vegas-lines.md)**, whose `E[f(X)]`-over-tiers instinct is right and now
  quantified at a 16.5-point compression, but whose scope missed that Vegas also predicts
  the sack and interception components carrying 56% of the score.
- **[28](28-outcome-distributions.md)** — the outcome distributions. **G-D0 runs
  first and can reject the plan in an afternoon**: if the existing `floor`/`ceiling`
  already spans a backup's bimodal range, the board shows this today. Its measured
  evidence is usable at the draft **without any of the build** — an RB2 is +5.72
  points a game when the lead back sits and a WR2 is +0.07, and a player who missed
  8+ weeks last season has a 0.31 chance of a full slate against a healthy
  comparable's 0.59.

### Before week 1

- **[34](34-stat-first-audit.md)'s owed items.** Three, in order of how cheap they are.
  **The six milestone-bonus column names** need one live probe of ESPN's
  `projected_breakdown` for a player with a 100-yard game; until then `john_pc_league`'s
  points are short by a median 0.48 a row and now say so out loud. **The eight efficiency
  shrinkage constants** are all below their measured credibility floor and refitting them
  moves `USG_` and so `TRUE_`. **The quarterback passing interval** covers 58.9% against a
  nominal 80% and misses asymmetrically — either refit the QB dispersion in
  `Scripts/usage/predictive.py` or withdraw the QB interval from the board.

### In season

- **[19](19-weekly-usage-model.md)**, the weekly head, comes online around week 3 and
  is where the larger edge was measured: trailing *expected* production beats trailing
  actual at predicting next week (R² 0.2907 against 0.2702). It is also the head that
  gets the live injury report — the season head cannot, because nflreadr does not
  serve one before week 1.
- **[18](18-season-usage-model.md) G2** — build the 2026 board with and without
  `USG_` in `WEIGHTS` and score both against realised 2026. This is the only way the
  comparison can be made; no historical pre-season blend survives.
- **[21](21-coaching-and-scheme.md)** — measure the coach and coordinator priors
  against plan 19, which has not been tried. They were rejected as regressors for the
  season head because the depth chart already contained their signal.

### Whenever

- **[06](06-performance.md)** — quadratic `pd.concat` in row loops, the duplicated
  `fetch_league` round-trip, the process-wide `warnings.filterwarnings("ignore")`.
- **[05](05-dependency-upgrades.md)** — the rest of the dependency upgrades. `boto3`
  moved out of "low priority" with plan 24: it is the app's read path now.
- **[24](24-s3-data-flow.md)'s two deliberate omissions.** The **query layer** —
  the partitions are laid out for Athena or DuckDB-over-S3 and nothing reads them
  that way yet; the first question worth asking is the **ADP drift through camp**
  the nightly snapshots now make answerable, and that one only improves with
  waiting, since every night adds a row. The **cloud runner** is the larger piece
  and was scoped out on purpose: the design no longer assumes local disk, which was
  the prerequisite, but moving the 6am job still needs ESPN cookies in a secret
  store, an R runtime for `R/GetContext.R`, and a notification path that is not
  `osascript`. Until then a shut lid is still a skipped night.
- ~~**[20](20-consensus-sources.md)** — deprioritised on evidence.~~ **Retired
  2026-08-24: the evidence was measured on a sample that could not have produced any
  other answer.** FantasyPros' +0.027 came from non-imputed cells, and FantasyPros was
  real for 60 players — the top ten per position, where every source agrees. At 592
  players its disagreement with ESPN runs 6.0% inside ADP 50 and **31.6%** outside 150.
  What is owed is a re-run of **plan 16 step 0's independence matrix — the whole table,
  not one row** — and then [03](03-projection-source-coverage.md)'s weight re-tune. Both
  move `TRUE_Points` and are frozen until after the drafts.
- **Roadmap Phases 4, 5** — the Monte Carlo mock-draft simulator and the live
  terminal assistant. Phase 1 (draft history) landed as
  [23](23-owner-tendencies.md); what is still missing from it is *outcomes*,
  points-over-expectation per manager, which needs past seasons scored in each
  league's own rules.

### Blocked or waiting on the calendar

- **BetOnline weekly props** return `403 invalid_security_headers` and need a
  decision: drop and re-weight, replace the book, or drive it through a real browser
  as Pinnacle already is. → [02](02-betonline-access.md). The season-long endpoint is
  a different host and works, so the draft board is unaffected.
- **`Rscript R/GetNFL.R 2026`** needs a re-run once week 1 is played, for
  `Data/NFL/2026/NFL_Stats.csv` and a refreshed `NFL_Tackles_By_Position.csv`.
- **[09](09-frontend-draft-views.md) Live Draft** is the last piece of that plan; draft history landed as [23](23-owner-tendencies.md).
- **Plan 18's rookie draft-capital arm** shipped; its remaining question — whether a
  *combine* arm adds anything — is unmeasured and low priority.

## Where the modelling stands

[Plan 16](16-usage-data-layer.md) is the shared data and feature layer plus the
independence gate; [18](18-season-usage-model.md) and
[19](19-weekly-usage-model.md) are the two model heads on it;
[21](21-coaching-and-scheme.md) is the situational context underneath both.
[Plan 17](17-draft-usage-model.md) is a superseded stub.

The season head predicts **83.7%** of rostered players on the 2026 pull (765 of 914)
and 80.3% across the walk-forward. The quarterback abstention that briefly took this
to 73.2% was lifted on 2026-08-07 once the depth chart landed in the veteran arm.
Against the naive draft heuristic it improves ordering at **all four** positions
(+0.013 QB, +0.062 RB, +0.053 WR, +0.066 TE) and cuts MAE 7–17% on every stat. Its
rookie arm is the clear win — draft capital plus depth-chart position orders rookies
at ρ ≈ 0.63 where a guess carrying no such information manages ~0.

[Plan 22](22-feature-research.md) then tried eleven things on top of it and merged
none of them. The running record is [`docs/model_lab.html`](../model_lab.html).

**It is the fifth source, at weight 0.25.** Registered in `WEIGHTS` at 0.0 on
2026-08-07 and turned on the same day; `WEIGHTS` is now an equal quarter each to
`ESPN`, `FP`, `BOL` and `USG`. It is scored by `proj_to_score`, present on all nine
boards, and deliberately *excluded* from the floor/ceiling spread — that column needs
sources measuring the same quantity and the model still sits below all four for 47% of
draftable players.

Turning it on was an assertion, not a measurement, and remains one: G0 (independence)
passed at +0.832 against FantasyPros' +0.988; G1 failed on the crude baseline and
located the whole deficit in not knowing who plays; **G2 cannot be measured on history
at all**, because no historical pre-season blend survives to compare against. The 2026
board is the first chance, and that means after the season is played — the
counterfactual is frozen in `Data/G2/2026/`.

Worth knowing before reading the column: `USG_Points` is on the **same footing** as
`TRUE_Points` and `ESPN_Points` — a full healthy 17-game slate, with each player's own
expected games divided back out, so it carries no availability discount.
`usg_expected_games` travels beside it rather than inside it, and carries depth-chart
role as well as health — which is why the board build withdraws the model's line for
backups ESPN has priced out rather than trying to scale it down. Compare
`USG_PosRank` rather than points anyway: the model shrinks toward positional baselines
where the other sources extrapolate. `python -m Scripts.usage.backtest` reproduces
every number above; `python -m Scripts.usage.project` builds the artifact.

Three things were measured and rejected rather than assumed, and are recorded in code
so they are not rediscovered: the coach prior in the veteran arm, the coach prior in
the rookie arm, and a sixth consensus source.
