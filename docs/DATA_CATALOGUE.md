# Data catalogue

Everything this repo holds: what it is, how much of it there is, where it lives, what
produces it, and — the part that actually matters — how the pieces join.

**Measured 2026-08-11.** Numbers drift; the doc does not. For the live answer:

```bash
python -m Scripts.catalogue          # local disk, ~0.4s
python -m Scripts.catalogue --s3     # the bucket
python -m Scripts.catalogue --both
```

## Where it lives

`s3://espn-ffl-data` (`us-east-2`, versioned) is the system of record — **258 objects,
77.5 MB**. `Data/` locally is a writer's scratch pad plus a read cache and is not
tracked in git. `python -m Scripts.sync --pull` rebuilds it. See
[plan 24](plans/24-s3-data-flow.md).

| Tier | S3 prefix | Objects | Size |
|---|---|---:|---:|
| nflverse | `nfl/` | 133 | 38.8 MB |
| projections | `projections/` | 59 | 1.6 MB |
| the store | `store/` | 45 | 19.7 MB |
| G2 archive | `archive/g2/` | 10 | 1.6 MB |
| board snapshots | `snapshots/board/` | 9 | 15.4 MB |
| injuries, scoring | `injuries/`, `scoring/` | 2 | 0.5 MB |

`Data/Equivalence/` (74 MB) is **not** in S3 and not in the table above — it is
before/after debug snapshots from the espn-api 0.46.0 migration, evidence about a bug
that is fixed rather than live data.

---

## 1. The store — what you actually query

`Data/Store/<season>/<league>/` → `store/season=<season>/league=<league>/`. Nine
leagues. Built by `python -m Scripts.refresh`, read by the app and by
`populateGoogleSheet.py`. Every artifact is scored in **that league's own rules**.

### 2026 — 9 leagues

| Artifact | Rows/league | Cols | Total rows | What one row is |
|---|---|---:|---:|---|
| `board.parquet` | 969–2,503 | 1,243 | 10,622 | A draftable player, with VOR, tier, ADP and value |
| `lineups.parquet` | 168–530 | 539 | 2,213 | A player-week |
| `draft.parquet` | 336–1,470 | 21 | 5,748 | A pick, across **every season the league has existed** |
| `tendencies.parquet` | 6–19 | 21 | 112 | A manager, and what they do that the room does not |
| `meta.json` | — | 16 keys | — | Build time, current week, coverage, package versions, git sha |

`team_stats.parquet` is **not built** for any league. It is opt-in because it
re-derives a league's entire history — 2016–2026 for Winfield Football.

### 2025 — 9 leagues, `lineups` only

1,784–4,508 rows per league, **28,975 total**, 549 columns. This is the played-season
data: every player-week of a completed season, which is what any backtest reads.

### The wide artifacts, by column family

`board.parquet` is 1,243 columns because it carries every source's full stat line
side by side rather than only the blend. Measured on Knights FFL:

| Prefix | Cols | What it is |
|---|---:|---|
| `BOL_` | 275 | BetOnline |
| `FP_` | 273 | FantasyPros |
| `PINNY_` | 273 | Pinnacle |
| `ESPN_` | 137 | ESPN |
| `MEAN_` | 137 | The cross-source mean |
| `TRUE_` | 45 | **The blend** — what the board ranks on |
| `USG_` | 43 | The usage model, source 5, at weight 0.0 |
| `usg_` | 7 | Usage diagnostics: `usg_arm`, `usg_expected_games`, `usg_evidence` |
| other | 46 | Identity, ADP, VOR, tiers, keeper and auction values |

Every source also carries `*_is_imputed` flags, which is how a degraded source shows
up as a number rather than being silently absorbed by the blend.

Identity and value columns: `player_id`, `gsis_id`, `name_key`, `player_name`,
`primaryPosition`, `pro_team`, `adp`, `auction_value`, `vor`, `pos_rank`,
`replacement_rank`, `floor`, `ceiling`, `sources_real`, `injury_status`,
`keeper_value`, `league_auction_value`, `prior_season_points`.

`lineups.parquet` follows the same shape at 539 columns: `FP_` 95, `BOL_` 95,
`PINNY_` 91, `ESPN_` 45, `MEAN_` 45, `TRUE_` 45, plus 63 identity and actual-score
columns (`week`, `slotPosition`, `team_owner`, `points`, `projPoints`, and the raw
stat columns).

---

## 2. nflverse — 131 files, 38.7 MB, 2016–2026

`Data/NFL/` → `nfl/`. Pulled by the `R/Get*.R` scripts. Row counts below are the
**newest season**; every file exists per season across the span.

| File | Seasons | n | Newest rows × cols | Produced by |
|---|---|---:|---|---|
| `depth_charts.parquet` | 2016–2026 | 11 | 418,377 × 13 | `GetContext.R` |
| `rosters_weekly.parquet` | 2016–2026 | 11 | 2,930 × 36 | `GetContext.R` |
| `injuries.parquet` | 2016–2025 | 10 | 5,783 × 16 | `GetContext.R` |
| `snap_counts.parquet` | 2016–2025 | 10 | 25,395 × 16 | `GetContext.R` |
| `player_weeks.parquet` | 2016–2025 | 10 | 18,539 × 114 | `GetUsage.R` |
| `opportunity.parquet` | 2016–2025 | 10 | 5,373 × 159 | `GetUsage.R` |
| `routes.parquet` | 2016–2025 | 10 | 9,552 × 7 | `GetAdvanced.R` |
| `red_zone.parquet` | 2016–2025 | 10 | 2,639 × 18 | `GetAdvanced.R` |
| `ngs.parquet` | 2016–2025 | 10 | 1,782 × 14 | `GetAdvanced.R` |
| `NFL_Stats.csv` | 2025 only | 1 | 1,964 × 113 | `GetNFL.R` |

**`depth_charts` is the outlier and the important one.** 418k rows for 2026 against
~35k a season through 2024, because upstream switched the feed to a timestamped
snapshot log. It is the only current-season input nflreadr serves before week 1, it
changes daily through camp, and it is the only feature that has ever moved the season
model. It is why `run_daily_refresh.sh` exists.

**2026 has only depth charts and rosters.** Injuries, snap counts, and everything
from `GetUsage.R`/`GetAdvanced.R` need played games. That is expected pre-season, not
a gap.

### Not season-scoped

| File | Rows × cols | What it is |
|---|---|---|
| `player_ids.parquet` | 12,470 × 13 | **The crosswalk.** See §5 |
| `contracts.parquet` | 46,719 × 15 | Contract history, measured and rejected by [plan 22](plans/22-feature-research.md) |
| `coaches_by_game.parquet` | 9,270 × 7 | Coach per team-game |
| `coaching_staff.parquet` | 544 × 10 | Head coach and coordinators per team-season, merged with Wikipedia |
| `schedules.parquet` | 3,033 × 13 | Every game, 2016– |
| `team_names.parquet` | 36 × 3 | Abbreviation ↔ name |
| `models/season_usage_1.1.0.json` | 10.3 KB | **The fitted season model.** Coefficients, dispersions, rookie bins, `fitted_at` |

---

## 3. Projections — 59 files, 1.6 MB

`Data/Projections/<source>/{Season,Landing}/<season>/` → `projections/`.

| Source | Notable files | Rows × cols | State |
|---|---|---|---|
| **Usage** | `Season/2026/Usage_SeasonProjections.parquet` | 915 × 55 | The model's own output — the blend's fifth source |
| **FantasyPros** | `Season/2026/..._Season.parquet` | 60 × 24 | Works. `week=draft` gives season lines |
| **BetOnline** | `Season/2026/..._SeasonProps_All.csv` | 546 × 11 | Season props work; **weekly is broken** (403, signed header) |
| **BetOnline** | `Season/2025/..._Week_N.parquet` ×17 | ~600 × 23 each | Last season's weekly props, preserved |
| **Pinnacle** | `Season/2026/Pinnacle_SeasonProps.parquet` | 76 × 11 | Season props |
| **Pinnacle** | `Season/2025/..._Week_N.parquet` ×17 | ~214 × 17 each | Last season's weekly |

ESPN projections are not a file — they arrive with the league fetch and land directly
in the store.

---

## 4. The rest

| What | Where | Shape | Notes |
|---|---|---|---|
| Scoring registry | `Data/Scoring/scoring.csv` | 3,720 × 11 | One source of truth for every league's rules |
| ESPN injuries | `Data/Injuries/2026/espn_injuries.parquet` | 800 × 11 | The feed carrying `returnDate`, refreshed nightly |
| **G2 archive** | `Data/G2/2026/` | 2,052–5,006 × 62–78 | 9 leagues + `manifest.json` |
| Board snapshots | `snapshots/board/` (S3 only) | 9 objects, 1 date | One board per league per night |

**G2 is the one thing here that cannot be rebuilt.** It is the pre-season board
blended with and without `USG_`, archived so that "does the usage head earn its
weight?" can be answered against real 2026 outcomes. FantasyPros serves no season
parameter, so a past board is gone the moment it stops being current. Column counts
differ per league (62–78) because each is scored in its own rules. `archive/` is
exempt from the bucket's version-expiry rules.

The board snapshots are the going-forward replacement for hand-archiving: they
accumulate one board per league per night, which makes **ADP drift through camp**
measurable. Currently one date — the series starts here.

---

## 5. How it joins

This is the part worth knowing. There are **two id worlds** and one bridge.

```
   ESPN world                    bridge                   nflverse world
   player_id  ─────────►  player_ids.parquet  ◄─────────  gsis_id
   (board, lineups,        espn_id ↔ gsis_id              (player_weeks,
    draft)                 ↔ fantasypros_id                opportunity,
                           ↔ sleeper/yahoo/pfr/            depth_charts,
                             sportradar                    contracts)
                                  │
                            name_key / merge_name
                        (normalised-name fallback)
```

| Dataset | ids it carries |
|---|---|
| `store/board` | `player_id`, **`gsis_id`**, `name_key`, `player_name` |
| `store/lineups` | `player_id`, `player_name` |
| `store/draft` | `player_id`, `player_name` |
| `nfl/player_weeks` | `gsis_id`, `player_name` |
| `nfl/opportunity` | `gsis_id`, `full_name` |
| `nfl/depth_charts` | `gsis_id`, **`espn_id`**, `player_name` |
| `nfl/contracts` | `gsis_id` |
| `projections/Usage` | `gsis_id`, `player_id`, `name_key`, `full_name` |
| `injuries/espn` | `name_key`, `full_name` — **no id at all** |

**`board.parquet` is the join hub**: it is the only store artifact carrying both
`player_id` and `gsis_id`, so it is the natural place to attach nflverse data to
league-scored data. `depth_charts` is the other bridge, carrying both ids natively —
which is exactly why the rookie arm works.

### Crosswalk coverage

`player_ids.parquet` spans all 12,470 players it knows of, including long-retired
ones, so these percentages are the floor rather than what you get on a live roster
(`python -m Scripts.crosswalk` reports ~99% against a built board):

| Column | Non-null | |
|---|---:|---:|
| `name`, `merge_name`, `position`, `team` | 12,470 | 100% |
| `draft_year` | 12,369 | 99% |
| `birthdate` | 11,203 | 90% |
| `pfr_id` | 9,610 | 77% |
| `espn_id` | 8,139 | 65% |
| `gsis_id` | 7,985 | 64% |
| `sportradar_id` | 7,440 | 60% |
| `sleeper_id` | 6,358 | 51% |
| `yahoo_id` | 5,498 | 44% |
| `fantasypros_id` | 4,784 | 38% |

Team D/ST units never match any id and join on name alone.

---

## 6. Gaps worth knowing

- **ESPN injuries carry no id.** `espn_injuries.parquet` joins on `name_key` only, so
  it is the most fragile join in the repo — and it feeds the usage model's return-date
  adjustment. Suffix changes (Jr./III) are the usual failure.
- **`team_stats` is built for nobody.** Points-over-expectation per manager needs
  past seasons scored in each league's rules, and that is the missing input.
- **BetOnline weekly props are broken** — 403, their API now wants a signed header.
  Season props still work.
- **2026 has no played-game data**, by definition. Every `GetUsage.R`/`GetAdvanced.R`
  file stops at 2025 until week 1 is in the books, and `NFL_Stats.csv` exists for 2025
  only.
- **`fantasypros_id` at 38%** is the weakest crosswalk column, which is part of why
  [plan 20](plans/20-consensus-sources.md) deprioritised adding a sixth aggregator.
