# 49 — Add/drop with a horizon: legality, the wire, and rest-of-season value

**Status:** IN PROGRESS

**Priority:** High · **Effort:** L, and most of it is measurement
**Where it stands:** Stages 0–1 shipped 2026-09-10; the ROS source and its two
gates shipped 2026-09-11, **G-R0 and G-R1 both pass**. The decision layer is not
started.
**Depends on:** [48](48-live-scoring.md) — kickoff is what decides "available" ·
[28](28-outcome-distributions.md) — the fitted dispersion the confidence column needs ·
[33](33-role-resolution.md) — how much a depth chart can be trusted ·
[27](27-injury-model.md) — the hazard the drop-side premium is priced from
**Feeds:** [08](08-frontend-weekly-views.md) §Free Agents ·
[40](40-frontend-restructure.md)'s owed-work table, two rows of which this closes

---

## Problem

The Free Agents tab scores an add/drop as the difference between two optimal
lineups, which is the right metric and stays. Three things around it were wrong,
all confirmed against the live 2026 stores on 2026-09-10.

**It suggested moves ESPN would not allow.** `weakest_starter_candidates` ranks
drop candidates by projection ascending, and a player in an IR slot projects `0.0`,
so he sorted **first**. Brian Barrett's top suggested drop in `gop_degenerates`
week 1 was Jordyn Tyson at slot `IR`; Tank Dell was second on Ryan Bonifay's. An IR
slot sits *outside* the roster count, so dropping the man in it frees an IR slot
rather than a bench place — the move only works if the incoming player carries an
`INJURY_RESERVE` or `OUT` designation himself. 19 IR rows across the nine leagues
were being offered as the best available drop.

**It suggested moves after the games had been played.** Ryan Bonifay's top drop was
Nick Emmanwori, already `post` at 0.0 — a banked zero no swap can recover, and a
number that is no longer a projection at all. `pool_playable` already gated the
*add* side on `game_locked` (plan 48); nothing gated the drop side, the `over` side
of the upgrade flags, or the pool table, where 55 rows across eight leagues were
still counted as "Available".

**It only ever spoke about this week, and said so.** The route disclaimed
rest-of-season value outright, pointing at [plan 19](19-weekly-usage-model.md),
which is unstarted. Honest, but it makes the tab bad at its own job: a +0.5 weekly
edge is not a reason to spend a roster spot, and a player who does nothing this week
can still be the right claim. Jonah Coleman — DEN RB3, `TRUE_Points` 3.73, on Tommy
Winfield's GOP bench — was the tab's **number one recommended drop**.

And one bug found while deriving the threshold: in `big_red_fantasy_football` ESPN
serves no owner for two teams, `set_owner_names` called both `"Unknown Owner"`, and
`slot_counts` — grouping on the owner — read the merged 32-row roster as one manager
starting two quarterbacks and **doubled every starting slot in the league**. Its
optimiser was solving a lineup twice the real size, so every add/drop number that
league produced was wrong.

---

## What shipped, 2026-09-10

### Stage 0 — `pool.parquet`, because the nightly destroys the wire

**Nothing in this repo had ever recorded what the waiver wire looked like in a past
week, in any season.** `build_fa_market` calls `league.free_agents()`, which ESPN
serves only as of *now*, and `extract_fa_stats` stamps `league.current_week`.
`build_league_frame` concatenates that single snapshot onto a full roster history
and `clean_lineups` rebuilds the frame, so the 06:00 refresh is destructive by
design: the 2025 stores carry free-agent rows on **week 17 only**, for all nine
leagues and all of 2025.

The consequence is that no claim about a waiver suggestion is falsifiable, and no
backfill can fix it — the same sentence `scrape_espn_injuries` had to write about
injury severity, for the same reason.

`Scripts/pool.py` + a new store artifact. One row per pool player-week — 27 columns
rather than 625, enough to replay a decision: identity, `eligiblePositions` (slot
comparison is what the suggestions are built on), the five per-source `*_Points`,
the blend, `LIVE_Points`, game state, and `percent_owned` / `injury_status` joined
from the board. Captured by `--what pool` in a new nightly stage 4c, reading the
store rather than ESPN, so it adds no round-trip and cannot fail upstream.

Two properties carry the design, and both are tested by name:

- **Replace-this-week, not append.** The nightly runs daily and `--what live` every ten minutes; a true append would hold ninety copies of week 4 by Friday. The most recent capture of a week wins, which is also right on a Tuesday when the wire has moved since Monday.
- **An empty write is refused.** In an accumulating artifact "nobody was available" and "the fetch broke" produce the same file, and the history it would truncate is not rebuildable. The rule `Scripts/books/store.py` already follows.

Measured: 284 rows for `gop_degenerates` week 1 in 0.04s, idempotent across re-runs.
`Scripts/sync.py` and `Scripts/catalogue.py` both iterate `ARTIFACTS`, so
registration alone gets it to S3 and into the catalogue.

### Stage 1 — only suggest moves that can be made

- `weakest_starter_candidates` grew `exclude_locked=True`; `upgrades` filters its `mine` list the same way, which fixes the Home tab too since `home.waiver_action` reads the same output. Verified: upgrades whose `over` has already played went 20 → **0** on GOP.
- `lineup.droppable_for` / `ir_eligible` / `IR_ELIGIBLE_STATUSES`. IR players are held **out of the four-drop cap** rather than counted against it — they sort first and are refused for all but an IR/OUT add, so letting them occupy two of four places would quietly halve the candidates a healthy add is weighed against.
- `lineup.playable_pool` narrows the pool table, with the count saying what went: `Available · 118 of 268 · 16 already kicked off`.
- The filter cell moved to sit directly above the table it scopes. It never applied to the Add/Drop panel, which reads the whole pool on purpose.
- `slot_counts` groups by owner **and team name**; `set_owner_names` qualifies its fallback as `Unknown Owner (Team 5)`. All ten leagues now return exactly their declared `starting_slots`.

**No pipeline change was needed for the IR rule.** `injury_status` and
`percent_owned` are on `board.parquet` and join 1:1 — measured across all ten 2026
stores: no board has a duplicate `player_id`, `percent_owned` reaches 100% of every
pool, and all 19 IR-slot rows carry a status. The 32 nulls per league are D/ST,
which has no designation by nature. The board is rebuilt by the 06:00 nightly and
the live patch does not touch it, so a designation can be a day old — right for an
IR placement, wrong for anything needing the hour.

That also closes plan 40's `percent_owned` owed item: the pool can finally be
narrowed to players who are *gettable* rather than merely unrostered.

---

## What is next, and what would falsify it

### The base rate the criteria must be tuned to

Across all nine 2025 leagues: 562 in-season adds that stuck four weeks or more, of
which **156 reached the median week-1 starter's season pace at their position — 85
skill, 64 K/DST, 7 IDP**, about 17 per league-season. So roughly **9 skill-position
hits per league-season, one every other week**. A table that fires once a month is
tuned too tight; one that fires weekly is noise.

K and D/ST are 41% of the successful adds but each is nearly costless and
reversible. They get their own quiet "best streamer this week" line rather than
competing in the main table.

### Rest-of-season, and the one honest way to use FantasyPros

Verified live 2026-09-10: `https://www.fantasypros.com/nfl/rankings/ros-{pos}.php`
for `qb, rb, wr, te, k, dst` returns `scoring=STD`, `type=ROS`, embedded in
`var ecrData`. robots.txt allows `/nfl/rankings/` at `Crawl-delay: 5`. 486 rows, 480
with `r2p_pts`, plus `rank_min/max/std`, `pos_rank` and `player_owned_avg`.

Coverage against GOP's week-1 pool: RB 30/30, TE 20/20, WR 28/30, QB 19/20, K 17/20.
**IDP 0 of 150** — FantasyPros publishes none, so GOP's `DP` slot abstains, visibly,
and never at zero. D/ST joins on `player_team_id` ↔ `pro_team`, never on name
("Houston Texans" against "Texans D/ST" matches 0 of 14).

Three dead ends, recorded so they are not rediscovered. On `/nfl/projections/`:
`week=ros` silently returns **week 1**; a future `week=N` silently returns **2025**
week N; `week=draft` is capped at **10 rows per position** even authenticated.

**Use the ordering, not the points.** A per-position STD→league conversion factor
`k = median(FP_Points / STD_FantasyPoints)` was measured and is tight within a
position (CV 0.018–0.025 at QB, 0.05–0.12 at the skill positions, 0.08–0.22 at K and
D/ST) and wild across leagues, as a scoring conversion must be. But `r2p_pts` is a
total over *FantasyPros'* own assumed games, so dividing it by *our* games-remaining
mixes their numerator with our denominator and discounts a known absence twice —
[plan 43](43-tomcat-out-of-season-blend.md)'s level error with different column
names. And `total_experts` is **2–3**, so `rank_std` is a spread over three
opinions.

So the decision logic is expressed as **rank inequalities against this league's own
`replacement_rank`** (`Scripts/draft/board.py:114`), which needs no scoring bridge
at all; a converted `ros_ppg` is a display column and a tiebreak. If the bridge
turns out not to earn its place, the column goes and the decision logic is
untouched. Never name it `ROS_Points` — a `*_Points` suffix is picked up by
`present_prefixes` and `points_columns` as if it were a sixth blend voter.

### The threshold, and why it is the existing metric's own two halves

`optimal_lineup` maximises over a transversal matroid, so the optimum as a function
of an inserted player's projection is `max(base, base − w + x)` — one kink, slope 0
then 1. So `w`, the displaced starter's value, is recoverable in **two solves,
exactly**. That makes `add_drop_gain == add_value − drop_cost` an identity rather
than a second metric bolted alongside the first, and it is what makes `Lineup Gain`
explainable. It returns `None` — not `0.0` — for a player this league cannot start
anywhere, which is the honest answer for an IDP in a league with no `DP` slot.

Two levels are wanted and they answer different questions: what it takes to crack
*your* lineup, and what it takes to be a startable player in this league at all
(`replacement_ranks`, transposed from a season to a week).

### Confidence, which is what actually tightens the criteria

`UPGRADE_MIN_MARGIN = 0.5` is sized from *between-source* disagreement (median sd
0.43) — model uncertainty. What decides whether a suggestion was right is *outcome*
uncertainty, and it is already fitted: `Data/NFL/models/weekly_dispersion_1.0.0.json`,
n = 15,989 started player-weeks, per-position residual sd QB 8.08, RB 7.97, WR 7.63,
TE 6.79, D/ST 6.50, K 5.56. A swap has a realised-difference sd near **11.0**, so a
`+0.5` critical alert is **0.045 sd — a 52% coin flip wearing a red badge**.

The fix is to report a probability rather than a margin, and the machinery exists.
This is also why rest-of-season matters mathematically: gain accumulates linearly in
weeks while noise accumulates as √weeks.

### Upside, without a new model

`Scripts/outcomes/vacancy.py` already fitted it: **81% of a vacated lead back's
opportunity reappears on the next three backs**, and the room keeps 93%. That is the
Jonah Coleman mechanism, with a standard error. The same module rules out the
receiver version — 45% reappears and the offence simply throws 1.25 fewer times — so
a path-to-role feature at WR is **measured dead before it is written**.
`Scripts/usage/role.py` supplies the discount: a rookie listed as his position's
starter really is the starter **36%** of the time, against 59% for a settled
veteran.

Coleman's board row already carries `usg_depth_rank` 3.0, `pts_p10/p50/p90` of
12.1 / 61.9 / 131.3 and `p_top12` 5.9% — a very wide band, which is what upside *is*.
Present on only 76 of 284 pool rows, so it abstains often.

## The source, as built — 2026-09-11

`--what ros` on `Scripts/scrape_FP`, six requests at the 5s crawl delay, into
`FantasyPros_ROS_Ranks.parquet`. **Append-only, keyed by capture date**, because
nothing anywhere archives what a rest-of-season consensus said in a past week:
overwrite it and the source can never be measured. 485 rows a capture, ~58k by
January. Nightly stage 2b''' beside the other two FantasyPros pulls, with the same
60-row teaser guard counting **today's capture only** — a cumulative file would
otherwise report health off one good night weeks after the cookie died. Registered
in `Scripts/name_audit.source_readers` and `Scripts/refresh_status`.

### G-R0 — the source joins. **PASS**, after it caught a real defect.

Free-agent pool coverage, all ten 2026 stores:

| position | coverage | note |
|---|---|---|
| RB | **30/30 in every league** | |
| TE | **20/20 in every league** | |
| QB | 20/20, except 19/20 on GOP | the miss is Philip Rivers, retired — an ESPN pool artifact |
| WR | 28–30/30 | Jayden Higgins and Ricky Pearsall are outside FantasyPros' 149 |
| K | 17–20/20 | Justin Tucker and Younghoe Koo are unsigned, and outside its 37 |
| **D/ST** | **100% in every league** | was 8/9, 13/14, 19/20 … before the fix below |

**The gate earned its place on the first run.** Every league was missing exactly its
Jaguars and its Commanders: FantasyPros writes `JAC` and `WAS` where ESPN writes
`JAX` and `WSH`, and D/ST joins on that key rather than on the name. Two of
thirty-two, each one a defence that would have silently never matched.
`ROS_TEAM_ALIASES` fixes it — and note it is *not*
`Scripts/nfl_utils.ESPN_TEAM_ALIASES`, which maps `LAR → LA` for nflverse;
FantasyPros writes `LAR` like ESPN, so reaching for the existing map would have
fixed one team and broken another.

**The remaining misses are the source, not the join**, and that was checked rather
than assumed: no surname variant of Higgins, Pearsall, Tucker or Koo appears
anywhere in the file, and `python -m Scripts.name_audit --season 2026` reports
**FantasyPros ROS: 0 to fix, 0 to review**. The reason code is therefore
`not_ranked` rather than `unmatched_name` — the older label asserted a defect where
the common case is a gap, and the two want opposite responses.

### G-R1 — the conversion holds together. **PASS.**

Within-position coefficient of variation of
`k = median(FP_Points / STD_FantasyPoints)`, all ten leagues, against a 0.15 bar at
QB/RB/WR/TE:

| position | CV range | factor range across leagues |
|---|---|---|
| QB | 0.011–0.025 | 0.94 – 1.32 |
| RB | 0.040–0.080 | 1.10 – 1.35 |
| WR | 0.035–0.124 | 1.26 – 1.58 |
| TE | 0.041–0.078 | 1.31 – 1.66 |
| K | 0.064–0.198 | 1.12 – 1.63 |
| D/ST | 0.068–**0.221** | 0.88 – **2.66** |

No gated position exceeds 0.124. The spread *between* leagues is the conversion
doing its job. **K and D/ST are the two loosest by a distance**, which is a second,
independent reason for the separate streaming channel — arrived at from the
scoring side rather than from the 2025 hit-rate study, and agreeing with it.

`ros_ppg` is published from this and is **display only**. Nothing in the decision
path reads it, and deleting it would leave the gates untouched — which is the point
of expressing them as ranks.

### Pre-registered gates

- **G-R0 — the panel accumulates and the cap is honest.** By week 8: ≥6 decision-week snapshots for ≥8 leagues, zero empty partitions. And of players actually added in week `w+1`, **≥85%** appeared in week `w`'s captured pool — below that, `build_fa_market`'s 20/30/30/20/20 cap is a blind spot.
- **G-R1 — the source joins.** ≥95% of pool rows at QB/RB/WR/TE carry a real `r2p_pts`; D/ST 32 of 32.
- **G-R2 — ROS beats what is already on the page.** From week 8, on `pool.parquet`: Spearman of the ROS ordering against *realised* rest-of-season ppg, wire population only, per position, against season-to-date ppg as the baseline. **If it does not beat it, ROS ships as a display column and comes out of the sort key.** Three experts is a thin reed and the honest posture is to plan for this to fail.
- **G-R3 — confidence is calibrated.** Reliability diagram: within 5pp in every decile, the bar `Scripts/outcomes/weekly.py`'s G-W2 already uses.
- **G-R4 — the rate is right.** The main table fires at roughly 9 skill suggestions per league-season.

### Two hazards named so nobody trips on them

**No board points column is a rest-of-season number.** `games` reads 17 for
essentially every player and `games_available` discounts only for an injury return
date, never for weeks elapsed. `Scripts/store.py`'s comment that the nightly board
"is a rest-of-season instrument" is prose about staleness for draft grading and is
one careless reading away from becoming plan 43 again.

**The 2025 FantasyPros archive may be hindsight.**
`FantasyPros_Projections_Week_All.parquet` for 2025 was pulled 2026-08-24 and the
2025 stores were rebuilt 2026-08-27 — *after* it. So `FP_Points` on a 2025 store is
a retroactive pull with no as-made copy to diff against. Before quoting any 2025
number that treats FP as the decision-time projection, score its relative MAE
against 2026 weeks 1–n, which are unambiguously as-made; a gap materially larger
than 10% disqualifies it.
