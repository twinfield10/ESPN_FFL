# 15 — Draft board: ADP, VOR and tiers

**Priority:** **High, date-driven** · **Effort:** M · **Status:** **Done** (2026-08-05)
**Depends on:** [07 (store)](07-frontend-foundation.md) — done ·
[12 (season projections)](12-season-projections.md) — done
**Blocks:** [09 (draft views)](09-frontend-draft-views.md)

This is roadmap **Phase 3** from
[`../STATE_OF_THE_REPO.md`](../STATE_OF_THE_REPO.md#roadmap--draft-strategy),
written up as a plan because it is the next thing to build and the only one with a
deadline. It produces `board.parquet`; [plan 09](09-frontend-draft-views.md)
renders it.

## Goal

For each league, one table that answers "who should I take next" in that league's
own scoring and roster shape. The minimum viable draft-day artifact.

## What the investigation changed

Three findings from probing the live 2026 API. The first two make this much
cheaper than the roadmap assumed; the third is the constraint to design around.

### 1. One request gives almost everything

`view=kona_player_info` with an `x-fantasy-filter` header returns, per player, in
a **single 0.24s request for 1,000 players**:

| Field | Value seen |
|---|---|
| `player.id` | ESPN player id — a real join key |
| `ownership.averageDraftPosition` | `1.83` — live market ADP, fractional |
| `ownership.auctionValueAverage` | `63.52` — live market auction dollars |
| `draftRanksByRankType.{PPR,STANDARD}` | `{rank: 1, auctionValue: 57}` — ESPN's own board |
| `ownership.percentOwned` | `99.8` |
| `stats[]` → `statSourceId=1, statSplitTypeId=0, seasonId=2026` | `appliedTotal: 365.49` **plus all 45 raw stats** |
| `stats[]` → `statSourceId=0, seasonId=2025` | `appliedTotal: 366.9` — prior-season actual |
| `injuryStatus`, `eligibleSlots`, `defaultPositionId`, `proTeamId`, `seasonOutlook` | — |

So ADP, auction value, **and** ESPN's full season projection as raw stats all
arrive together. `espn_api`'s `Player` class parses `percent_owned` but drops
`draftRanksByRankType` and `ownership.averageDraftPosition` entirely — confirmed
by reading `football/player.py`. That is the only reason this needed new code.

Universe returned: WR 359, RB 236, TE 196, QB 126, K 51, D/ST 32.

### 2. It triples the player universe and removes the slow path

`espn_season_projections()` currently builds its universe from team rosters plus
`league.free_agents(size=60, position=...)` across 11 positions — **329 players**,
and `free_agents` measured 4.8s per call in `build_fa_market`. A 14-team league
drafts 210 players, so 329 is uncomfortably thin for a board.

The ADP request covers 1,000 players in 0.24s and carries the same projection
data. Sourcing the universe from it is both wider and faster.

### 3. There is no ID join today, and that is the real risk

`espn_season_projections()` returns `name_key, player_name, primaryPosition,
pro_team, team_owner, games` — **no `player_id`**. The repo's standing complaint
that "player matching is `(week, player_name)` string equality, patched by
hardcoded rename dicts (~140 entries)" applies here, and a draft board that
silently drops a player because of a suffix is worse than no board.

The ADP payload has `player.id`, and the `Player` objects behind
`espn_season_projections` have `playerId`. Threading it through makes the
ESPN↔ADP join exact. The book sources (FantasyPros, Pinnacle, BetOnline) still
join on `name_key` — that is plan 03's crosswalk problem and is **not** solved
here, but it must be *reported* rather than hidden.

## Design

```
kona_player_info (1 request, 0.24s, league-independent)
        │
        ├── ADP, auction, %owned, injury        ── market
        └── ESPN 45-stat season projection      ── the wide universe
                     │
                     ▼
   Scripts.season_projections.build_season_projections()   ← already built (plan 12)
   blends FantasyPros / Pinnacle / BetOnline, scores per league → TRUE_Points
                     │
                     ▼
   replacement level from THIS league's starting slots × team count
                     │
                     ▼
   VOR → tiers (1-D clustering) → value vs ADP
                     │
                     ▼
   Data/Store/<season>/<league_key>/board.parquet
```

**The market half is league-independent; the valuation half is not.** ADP is one
global ESPN number, so fetch it **once** and reuse it for all nine leagues, then
compute replacement level, VOR and tiers nine times. That is what keeps a
nine-league board build near the cost of one.

> **Wrong, corrected during implementation.** ADP is league-independent but the
> *pool the endpoint returns* is not — it reflects the league's roster slots, so
> only the IDP league's response carries individual defenders. It is one request
> **per league**. See postscript §4.

### Replacement level

The differentiator, and the reason a generic board cannot do this. Already in the
store's `meta.json` as `roster_slots`:

| League | Starters |
|---|---|
| Knights_FFL | QB 1, RB 2, WR 2, TE 1, RB/WR/TE 1, K 1, D/ST 1 |
| Weenieless_Wanderers | QB 1, RB 2, WR 2, TE 1, RB/WR/TE 2, **OP 1**, D/ST 1 — no K |
| GOP_Degenerates | … + **DP 1** (IDP) |
| 12 Dudes one Cup | … **no D/ST** |

Replacement rank per position = `teams × (dedicated slots + expected share of
flex slots)`. Flex allocation is the only judgement call: `RB/WR/TE` and `OP` get
split across their eligible positions by how the market actually fills them —
approximate from ADP rather than assuming an even split, and make the assumption
visible in the output.

> **Wrong, corrected during implementation.** Allocating from ADP imports
> single-QB-league bias and left the superflex `OP` slot filled with running backs,
> making Josh Allen *less* valuable in a superflex league. Allocated from this
> league's own projected points instead. See postscript §3.

**`meta.json` does not currently record team count.** Add it in `build_meta` —
one line, and without it replacement level cannot be computed from the store
alone.

### Tiers

1-D KMeans within position on `TRUE_Points` (scikit-learn 1.7.2 is installed and
confirmed importable). Pick `k` per position by the largest gap in sorted points
rather than a fixed number, so a position with a genuine cliff gets a break there.
Tier breaks drive draft decisions far more than one-spot rank differences, so this
is the primary visual in plan 09.

### Value

`value = ADP_rank − VOR_rank`. Positive means the room is letting them fall past
where your projections say they belong. Sorting by this rather than by rank is the
board's actual job.

## Steps

1. **`Scripts/draft/__init__.py` + `Scripts/draft/adp.py`** — one function
   issuing the `kona_player_info` request and returning a tidy frame:
   `player_id, player_name, primaryPosition, pro_team, adp, auction_value,
   espn_draft_rank, espn_auction_value, percent_owned, injury_status,
   prior_season_points`, plus the ESPN season projection. Cache it per season so
   nine leagues share one request. Reuse `espn_api`'s `Player` construction so the
   45 raw stats arrive keyed by `colName` rather than by stat id — that is how
   `extract_player_stats` already reads them, and it avoids a second stat-id map.
2. **Thread `player_id` through `espn_season_projections()`** and widen its
   universe to the ADP pull. Report `name_key` join misses for the book sources
   explicitly — a silent drop is the failure mode that matters.
3. **`Scripts/draft/board.py`** — `build_board(league, season)`: replacement level
   from `roster_slots` × team count, VOR, tiers, value. Returns one row per
   player.
4. **`meta.json` gains `team_count`**; `refresh.py` grows `--what board` (already
   a reserved value) writing `board.parquet` through `write_league_store`.
5. **Delete `Scripts/draft_utils.py`.** It is dead — never imported, reads
   `./src/doritostats/pick_value.csv` from the upstream project this was copied
   from, and carries another league's owner map.
6. **Tests** — `tests/test_draft_board.py`, no network: replacement rank for a
   standard 12-team league, for the superflex `OP` league, and for the no-D/ST
   league; tiering on a synthetic cliff; `value` sign convention; and a fixture of
   the `kona_player_info` payload so the parser is tested without ESPN.

## Not in this plan

- **Draft history** (roadmap Phase 1) — independent, and only feeds the Phase 4
  simulator's opponent models. Cheap once wanted, not needed to draft.
- **Floor/ceiling.** The payload carries prior-season actuals, so source
  disagreement plus prior variance is buildable — but it is a refinement on top of
  a working board, not part of it.
- **The UI.** [Plan 09](09-frontend-draft-views.md).
- **IDP defensive scoring in the board.** `build_season_projections` still warns
  that IDP point totals come from a table `espn_api` collapses across slots
  ([plan 11](11-per-slot-scoring.md) fixed the weekly path, not this one). GOP's
  `DP` slot is affected. Surface the warning in the board rather than quietly
  ranking IDP wrong.

## Verification

- Replacement ranks hand-check: a 12-team league with one flex lands RB ≈ RB30,
  and the superflex and IDP leagues differ in the right direction.
- **The same player ranks differently across the nine leagues, for the right
  reason** — trace one player's VOR through two leagues by hand.
- Board builds for all nine leagues, including the no-D/ST and IDP ones.
- Universe is ~1,000 players, not 329, and every row with an ADP either has a
  projection or is flagged as missing one.
- Nine-league board build issues **one** `kona_player_info` request, not nine.
- Spot-check ADP against ESPN's own site for five players — this is market data,
  so it is externally checkable, and worth doing once before trusting it.

---

## Postscript — what building it turned up

Written after the fact, 2026-08-05. `python -m Scripts.refresh --all --what board`
builds nine boards in ~16s. Six findings, in rough order of how wrong they were.

### 1. Naive VOR says draft the Steelers defence in round 8

The first working board's eight best "values" in the league were all team
defences. Not a coding error — a modelling one. VOR asks "how many more points
than the last startable player at this position", which assumes you hold one
player all season. For kickers and D/ST you do not: you start the good matchup and
drop them. Their real replacement level is close to the best available in any given
week, not to the season total of the 14th-best unit.

`value` is now NaN for `STREAMED_POSITIONS` (K, D/ST). `vor` is still computed, so
nothing is hidden — but the board's headline signal no longer tells you to reach
for a defence.

### 2. ESPN's ADP saturates, and comparing against the plateau is nonsense

**758 of 1,000 players share an ADP of exactly 170.0.** Only 159 have a genuine
one. ESPN reports an `averageDraftPosition` for everyone it knows about but only
*prices* the players the market drafts; the rest sit on a filler value.

Ranking inside that plateau is noise, and differencing it against a projection
produced the first board's other embarrassment: backup kickers at ADP 170.4 with a
VOR rank of 150, scoring +819 "value". `adp_plateau()` now detects the pile-up
empirically (it moves year to year), `adp_is_priced` marks the real prices, and
both ranks in the `value` difference are computed over the same population.

A player with no market price but high VOR is now NaN rather than scored — which
is its own signal, and one plan 09 should surface rather than rank.

### 3. Allocating flex slots by ADP broke the superflex league

Replacement level for a `RB/WR/TE` or `OP` slot depends on which position fills it.
The plan said to read that off the market rather than assume an even split. That
was wrong, and wrong in a way that inverted the answer.

Global ADP comes overwhelmingly from single-QB leagues. Pooling by ADP filled
Weenieless Wanderers' superflex `OP` slot almost entirely with running backs and
left QB replacement at QB10 — identical to a one-QB league. **Josh Allen came out
*less* valuable in the superflex league than in a 14-team single-QB league**, which
is backwards.

Allocated by this league's own projected points instead, the `OP` slot goes to
quarterbacks, QB replacement moves to **QB20**, and Josh Allen's VOR goes from 65.4
to 102.2 — overall VOR rank 28 → 9. That is the superflex effect, and it is now
league-scoring-aware rather than importing another format's bias.

### 4. One request per league, not one per run

The plan claimed nine boards from one `kona_player_info` request, because ADP is a
market-wide number. ADP is — but the **pool the endpoint returns is
league-dependent**: it reflects that league's roster slots. GOP Degenerates' own
response carries 129 defensive ends and 125 linebackers; nobody else's does. A
season-keyed cache gave the one IDP league a board with no individual defenders.

Cache key now includes the league id. Nine requests cost about two seconds.

Relatedly, a limit of 1,000 truncates an IDP league's pool at the *offence*'s
expense — GOP returned only 134 WRs and 50 QBs because defenders crowded them out.
The pool exhausts at 2,503, so the limit is 3,000.

### 5. The season path was never using plan 11's per-slot scoring

GOP's board put LB replacement at **LB1** — implying the best linebacker in
football is already replacement level. The cause: `build_season_projections` scored
through a local loop over a single scoring table, with a comment explaining that it
could not do per-slot scoring. That comment predated plan 11, which gave the
registry a `slot` dimension and taught `proj_to_score` to read it.

So GOP's individual defenders were priced with the **D/ST-slot override of 0.0 for
tackles** — and linebacker points are almost entirely tackles. Swapping the local
loop for `proj_to_score` fixed it with code that already existed and was already
tested: LBs now project 190-225, replacement is LB15, and the top DP options are
Blake Cashman, Jordyn Brooks and Fred Warner. The stale `LIMITATION` comment and
its warning are gone.

### 6. Two NFL players really do share a name

GOP's 2,503-player pool has **16 colliding names**: Lamar Jackson the Ravens
quarterback (ADP 40) alongside Lamar Jackson a cornerback (ADP 170); Justin
Jefferson the Vikings receiver alongside Justin Jefferson a Browns linebacker.

The board joins on `player_id`, so it was fine. The **book sources join on
`name_key`** and carry one row per name — so a left merge attached the receiver's
projected receiving line to the linebacker as well, inflating him into the league's
top-projected IDP on somebody else's numbers. `_disambiguate_name_keys` now keeps
the book join for the highest-projected holder of a shared name and gives the
others a sentinel key, dropping them onto the absent-source path plan 03 already
handles.

This is the concrete demonstration of why the repo's name-based joining is a
standing risk, and it is worth remembering that it only became visible because the
IDP pool widened the universe.

## Verified

- `refresh --all --what board`: nine boards, ~16s, all ok.
- Replacement arithmetic exact on Knights_FFL: QB 14 = 14×1; RB 31 and WR 39 and
  TE 14 allocate 3+11+0 = **14** flex openings = 14 teams × 1 slot.
- Superflex QB20 vs single-QB QB14/QB12; 12 Dudes has no startable D/ST; GOP's
  `DP` slot splits LB15 + CB1 = 16 openings.
- Same player ranks differently across leagues, and for the right reason.
- Top of the ADP board (Gibbs 1.83, Bijan 2.62, Nacua 3.68) matches ESPN's own
  board with monotonically descending auction values — externally checkable.
- 26 new tests in `tests/test_draft_board.py`, no network, plus name-collision
  coverage in `tests/test_season_projections.py`. Full suite 267 passed.

## Still open

- **Floor/ceiling.** The payload carries prior-season actuals
  (`prior_season_points`), so source-disagreement plus prior variance is buildable.
  Not attempted.
- **Bye weeks.** Would need `_get_all_pro_schedule()`, a second request. Skipped to
  keep the board to one call per league; cheap to add if plan 09 wants it.
- **Draft history** (roadmap Phase 1), and the UI ([plan 09](09-frontend-draft-views.md)).
