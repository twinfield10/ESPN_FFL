# Data catalogue

What each dataset **is**: the grain of a row, what the columns mean, what produces it,
what reads it, and how the pieces join.

Deliberately no row counts, file counts or sizes — those change nightly and belong to
the tool that can be right about them:

```bash
python -m Scripts.catalogue          # local disk
python -m Scripts.catalogue --s3     # the bucket
python -m Scripts.catalogue --both
```

Read this for meaning, run that for volume.

## Where it lives

`s3://espn-ffl-data` (`us-east-2`, versioned) is the system of record. `Data/`
locally is a writer's scratch pad plus a read cache, is not tracked in git, and is
safe to delete — `python -m Scripts.sync --pull` rebuilds it. See
[plan 24](plans/24-s3-data-flow.md).

| Tier | Local | S3 prefix |
|---|---|---|
| The store | `Data/Store/<season>/<league>/` | `store/season=/league=/` |
| Board history | — | `snapshots/board/season=/league=/date=/` |
| G2 archive | `Data/G2/<season>/` | `archive/g2/season=/` |
| nflverse | `Data/NFL/` | `nfl/` |
| Projections | `Data/Projections/<source>/` | `projections/` |
| Scoring, injuries | `Data/Scoring/`, `Data/Injuries/` | `scoring/`, `injuries/` |

`Data/Equivalence/` is in neither git nor S3. It is before/after debug snapshots from
the espn-api 0.46.0 migration — evidence about a bug that is fixed, not live data.

---

## 1. The store — league-scored data, and what the app reads

One directory per league-season, nine leagues. Written by `python -m Scripts.refresh`,
read by the app and by `populateGoogleSheet.py`. The defining property: **every number
in here is already scored in that league's own rules**, so a point in the IDP league
and a point in the superflex league mean what they say. Nothing downstream re-scores.

| Artifact | One row is | Built by |
|---|---|---|
| `board.parquet` | A **draftable player** — the draft board | `--what board` |
| `lineups.parquet` | A **player-week** — the weekly blend | default |
| `draft.parquet` | A **pick**, across every season the league has existed | `--what draft` |
| `tendencies.parquet` | A **manager**, and what they reliably do that the room does not | `--what draft` |
| `team_stats.parquet` | A **team-week** of matchup history | `--what team_stats`, opt-in |
| `board_frozen.parquet` | A **draftable player**, as the board stood when the drafts finished | `python -m Scripts.freeze`, once a season |
| `meta.json` | Build time, current week, roster slots, source coverage, versions, git sha | always, **last** |

`board_frozen.parquet` is the only artifact **not** written by `Scripts.refresh`, and
that is the point of it. It is a copy of `board.parquet` taken once the drafts are over
and never rebuilt, because `board.parquet` refreshes every morning and therefore stops
being able to say what a roster looked like the day it was assembled — which is the only
basis on which a draft can be graded. Same schema as `board.parquet`; `meta.json` gains
`frozen_at`, `frozen_git_sha` and `frozen_picks`. See
[plan 41](plans/41-projection-freeze.md).

`meta.json` is written last on purpose: its presence is what distinguishes a finished
store from one mid-build, and both `Scripts.store` and `Scripts.s3_store` key on it.

### How `board` and `lineups` are shaped

Both are wide — hundreds of columns — because they keep **every source's full stat
line side by side** rather than only the blended answer. Columns are prefixed by
source:

| Prefix | Source |
|---|---|
| `ESPN_` | ESPN's own projection, which arrives with the league fetch |
| `FP_` | FantasyPros |
| `PINNY_` | Pinnacle |
| `BOL_` | BetOnline |
| `ATH_` | The Athletic — Jake Ciely's workbooks, offence only, at weight 0.25. **Two files, two grains**: the season book (~430 players, twelve stats) is on the board, and a weekly slate (232 players, nine stats — no `passingAttempts`, `passingCompletions` or `receivingTargets`) is on the lineups from 2026-09-09. Both hand-downloaded; see [plan 47](plans/47-athletic-weekly.md) |
| `USG_` | **TOMCAT — our own model. Withdrawn from the blend 2026-09-07, at weight *nothing*.** The columns are still written and still real; they carry no weight, are not scored into a `USG_Points`, and are off the draft board. One source, three backends: the usage arm (QB/RB/WR/TE), the defence arm (D/ST, from the betting market) and the kicking arm (K, per team). They were `USG_`, `DST_` and `KIK_` until 2026-09-02; the stat sets are disjoint, so one namespace holds all three and the model cast one vote rather than three. The lower-case `kik_*` and `dst_*` diagnostics keep their own names, because they answer *which arm spoke for this row*. Why it went: [plan 43](plans/43-tomcat-out-of-season-blend.md) |
| `MEAN_` | The unweighted cross-source mean |
| `TRUE_` | **The blend.** This is what the board ranks on and what lineups score |

`WEIGHTS` is an equal quarter each to `ESPN`, `FP`, `PINNY`, `BOL` and `ATH` — five
equal votes since TOMCAT was withdrawn on 2026-09-07 — and the nominal figures are never
normalised to 1 because `compute_weighted_stats` divides by whichever of them turned out
to be real. **A source at 0.0 is dormant; a source with no entry has been withdrawn.**
`USG` has no entry.

Read the weights alongside the coverage numbers rather than on their own: FantasyPros
publishes 60 season projections and is **5.8% real** on a board, so for most players the
blend renormalises down to ESPN alone, and to `(ESPN + BOL)/2` or `(ESPN + ATH)/2` where
a second source genuinely has a line. That thinning is the direct cost of the
withdrawal.

Every source also carries `*_is_imputed` flags per stat. That is the mechanism by
which a broken source shows up as a measured number rather than being quietly
absorbed — the repo's recurring failure mode is an absent source reading as
agreement, and these flags are the defence against it.

`board` adds draft-specific columns on top: `adp`, `auction_value`, `vor`, `pos_rank`,
`replacement_rank`, tier, `keeper_value`, `floor`/`ceiling`, `sources_real`. Its
`usg_*` (lower-case) columns are usage-model diagnostics rather than projections —
`usg_arm` says which arm produced the estimate, `usg_expected_games` its availability
term, `usg_evidence` how much history it rested on.

The `ath_*` (lower-case) columns are the other lower-case family, and lower-case for
the same reason: **they are outside the blend and cannot enter it.** They carry Jake
Ciely's hand ranking, which comes off the same workbook as `ATH_` but is an ordering
rather than a stat line, so it votes on nothing and only ever gets read.

| Column | What it is |
|---|---|
| `ath_pos_rank` | His rank within position, in whichever of his three lists — non-PPR, half, full — matches this league's points per reception |
| `ath_override` | That rank minus the rank his own `ATH_Points` implies. Positive means he ranks the player above his own numbers |
| `ath_rank_delta` | That rank minus our `pos_rank`. Positive means he is higher on the player than we are |

Both deltas re-rank each side over the 290 players he actually ranked. He ranks 85
backs and a board carries half as many again, so comparing his 60th against a
`pos_rank` of 60 drawn from the deeper pool would report bench depth as disagreement.
Blank means unranked, not last — and he ranks no kicker, defence or individual
defender at all.

It also carries **ESPN's own opinion beside ours**, so the two can be differenced:
`espn_draft_rank` is ESPN's published draft ranking (dense 1..N, no ties, every row),
`espn_pos_rank` is that ranking re-ranked within position, and `points_delta`,
`rank_delta` and `pos_rank_delta` are the differences against `TRUE_Points`,
`vor_rank` and `pos_rank`. **Every one is oriented so positive means we rate the
player higher than ESPN does** — points ours-minus-theirs, ranks theirs-minus-ours —
which is the convention `value` already set and what lets the page colour all of them
on one scale.

`injury_return_date` and `injury_note` come off ESPN's injury report
(`Data/Injuries/<season>/espn_injuries.parquet`) rather than `kona_player_info`. The
note is ESPN's own one-line account of the injury and is the only player news this
repo holds; both are null for the ~two thirds of the pool the report does not list, and
the date is null for most of the rest, because ESPN publishes an estimate only where it
has one. A date past `SEASON_ENDING_AFTER` is its season-ending sentinel, not an
estimate.

`lineups` adds the week's actuals: `points`, `projPoints`, the raw stat columns,
`slotPosition`, `team_owner`.

**And, since 2026-09-10, the resolution between the two halves.** A projection and an
actual side by side is not usable on its own, because until game state existed nothing
on the row could say *which one applies* — `points` is `0.0` both for a player who took
the field and caught nothing and for one who has not kicked off, and
`build_league_frame`'s `fillna(0)` means a null cannot carry the difference either.
Eight columns close that:

| Column | What it is |
|---|---|
| `game_state` | `pre` \| `in` \| `post` \| `bye`, from ESPN's scoreboard. A **string**, because a numeric or nullable state would be silently zeroed by `fillna(0)` |
| `game_elapsed` | Fraction of regulation played, 0.0–1.0, from `period` and `displayClock` |
| `game_locked` | `game_state != "pre"`. The one flag consumers branch on: it is what makes a lineup change impossible and a free agent unstartable |
| **`LIVE_Points`** | **The number every weekly surface reads.** `banked + TRUE_Points × (1 − game_elapsed)`, so it is exactly `TRUE_Points` before kickoff and exactly ESPN's `points` once the game is final |
| `LIVE_remaining_points` | `TRUE_Points × (1 − game_elapsed)`. What the win probability's spread is computed from — banked points have no uncertainty left |
| `ACT_Points` | This league's rules applied to the *actual* stat line. The audit half, not the answer |
| `actual_unpriced` | `points − ACT_Points` on a started game: what ESPN paid that this pipeline cannot price. Non-zero means a scoring rule is unmapped — see [plan 34](plans/34-stat-first-audit.md) |
| `live_only` | The row was added by a live refresh because the box score had a player the stored frame did not. It carries actuals and **no projection** |

`LIVE_Points` follows ESPN rather than `ACT_Points` for a finished game, and the two
are not interchangeable: measured on 2026 week 1 they agreed to float noise across
every played row in eight leagues and differed by 5.00 points on one player in the
ninth. ESPN's number is the league's official score.

**`LIVE` is not a source.** It is absent from `projection_utils.WEEKLY_PREFIXES` on
purpose — that tuple is the register of things that have an *opinion*, and
`Scripts.lab.sources` reads it to decide what a source is. `LIVE_Points` is what the
sources and the box score resolve to.

**Column width varies by league** and that is correct, not drift: the IDP league's
board carries individual defenders and nobody else's does, because ESPN returns a pool
shaped by each league's own roster slots.

### Seasons

The current season is built for every artifact. Completed seasons hold `lineups` —
the played-season record any backtest reads. A season's `board` is a pre-season
artifact and is not rebuilt once games start; nothing about week 9 changes your draft.

---

## 2. Board snapshots — the market as a time series

`snapshots/board/season=/league=/date=/`, S3 only. One board per league per night,
written by the nightly push.

This exists because a board cannot be reconstructed after the fact: FantasyPros serves
no season parameter, so the moment a board stops being current it is gone. Every
nightly build is now kept, which makes **ADP drift through camp** answerable —
position battles resolving, a rookie's price moving, the market reacting to news — at
daily resolution across all nine leagues.

---

## 3. The G2 archive — the one thing that cannot be rebuilt

`Data/G2/<season>/`: the pre-season board for each league, blended **with and without**
`USG_`, plus a `manifest.json` recording the question, the git sha and the archive
time.

It answers one question that cannot be answered any other way: does the usage head
earn its share of the blend weight, measured against what actually happened? Plan 18
records G2 as unmeasurable on history precisely because the inputs are gone once the
season starts, so the archive has to be taken **before week 1 or not at all**.

Column counts differ per league because each arm is scored in its own rules. `archive/`
is exempt from the bucket's version-expiry rules; everything else regenerates, this
does not.

---

## 4. nflverse — the football-truth layer

`Data/NFL/`, pulled by the `R/Get*.R` scripts. Seasons run from 2016. This is
league-agnostic: no scoring rules touch it.

| File | One row is | Producer |
|---|---|---|
| `player_weeks.parquet` | A player's **realised production** in one week | `GetUsage.R` |
| `opportunity.parquet` | A player-week's **opportunity**: targets, carries, air yards, shares | `GetUsage.R` |
| `depth_charts.parquet` | A **dated snapshot** of one team's depth chart | `GetContext.R` |
| `rosters_weekly.parquet` | A player's roster status in one week | `GetContext.R` |
| `injuries.parquet` | A player-week's official injury designation | `GetContext.R` |
| `snap_counts.parquet` | A player's snap share in one game | `GetContext.R` |
| `routes.parquet` | Routes run against team dropbacks, per player-week | `GetAdvanced.R` |
| `red_zone.parquet` | Carries and targets inside the 20 and the 10 | `GetAdvanced.R` |
| `ngs.parquet` | Next Gen Stats: cushion, separation, aDOT | `GetAdvanced.R` |
| `NFL_Stats.csv` | A season's player stat totals | `GetNFL.R` |

**`depth_charts` is the one to know about.** It is the only current-season input
nflreadr serves before week 1, it changes daily through camp, and it is the only
feature that has ever moved the season model. Upstream switched it to a *timestamped
snapshot log*, so it is far larger than the others and its grain is a snapshot rather
than a week. It is the reason `run_daily_refresh.sh` exists.

**Pre-season, most of this stops at the last completed season.** Everything from
`GetUsage.R` and `GetAdvanced.R` needs play-by-play, which does not exist until games
are played. Only depth charts and rosters are served for a season in progress. That is
expected, not a gap — but it looks like one until you know.

`routes` and `red_zone` are the expensive pulls: both need full play-by-play, so a
cold backfill is minutes rather than seconds. That is why the tier is mirrored to S3
at all rather than simply regenerated.

### Not season-scoped

| File | One row is |
|---|---|
| `player_ids.parquet` | A player, and his id in every provider's world. **The crosswalk** — see §7 |
| `contracts.parquet` | A contract. Measured and rejected as a feature by [plan 22](plans/22-feature-research.md) |
| `coaching_staff.parquet` | A team-season's head coach and coordinators, merged with Wikipedia |
| `coaches_by_game.parquet` | Who coached a given team in a given game |
| `schedules.parquet` | A game |
| `team_names.parquet` | Abbreviation ↔ full name |
| `models/season_usage_<version>.json` | **The fitted season model** — coefficients, dispersions, rookie bins, `fitted_at`. Not data; a model |

`coaching_staff` is committed-in-spirit rather than regenerable: it merges Wikipedia
data that will not come back identically once an article is edited.

---

## 5. Projections — the sources before they are blended

`Data/Projections/<source>/`, split into `Landing/` (raw scrape, as received) and
`Season/` (cleaned, ready to blend). Season sources land one file per season; weekly
sources land one **cumulative** `*_Week_All` file per season, carrying a `week` column
— `clean_lineups` re-merges it onto every week in the lineup frame, so a
current-week-only file would blank that source for prior weeks retroactively.

| Source | What it gives | State |
|---|---|---|
| **Usage** | The season model's own stat lines | Current |
| **FantasyPros** | Weekly and season projections (`week=draft` gives season lines) | Working |
| **The Athletic** | Season stat lines for ~430 offensive players plus his hand ranking of 290 of them, and — since 2026-09-09 — a weekly slate of 232 offensive players in nine stats | **Manual, twice** — two paid `.xlsx` downloads with no API, imported by `python -m Scripts.load_athletic --what season\|weekly`. Nothing refreshes either; `Scripts.refresh_status` reports both ages, the weekly one on an eight-day clock because a weekly workbook is correctly days old. One season import writes both season files, so the ranking cannot go stale beside fresh projections. The weekly file is cumulative over every week imported and the newest download wins for the week it names ([plan 47](plans/47-athletic-weekly.md)) |
| **Pinnacle** | Sportsbook props, weekly and season | Working, Selenium |
| **BetOnline** | Sportsbook props | **Weekly is broken** — 403, their API now wants a signed header. Season props still work |

ESPN is the source that is *not* here: its projections arrive with the league fetch
and go straight into the store.

Raw props are prices, so the season pipeline removes the vig before anything downstream
treats them as projections.

---

## 6. Scoring and injuries

**`Data/Scoring/scoring.csv`** — the scoring registry, and the one source of truth for
every league's rules. One row per **league-season-slot-stat**: `season`, `league_key`,
`slot`, `id`/`abbr`/`label`, `points`, the `colName` it maps to, and `recorded_at`.

The `slot` in that grain is load-bearing. A stat can be worth different amounts
depending on the roster slot scoring it, and collapsing that dimension inflated GOP's
D/ST by ~16% and credited offensive players for imputed defensive stats at D/ST rates
— see [plan 11](plans/11-per-slot-scoring.md). Aggregate over `slot` at your peril.

It is an **input to ingest, not an output**: stats cannot be extracted from ESPN until
the `colName` list is known, which is why it lives outside the store rather than in
it.

**`Data/Injuries/<season>/espn_injuries.parquet`** — the ESPN injury report, refreshed
nightly. A separate feed from nflverse's, and the reason it exists is `returnDate`:
the usage model scales a player's expected games by when he is expected back, rather
than zeroing him or ignoring the injury.

---

## 7. How it joins

There are **two id worlds** and one bridge.

```
   ESPN world                    bridge                   nflverse world
   player_id  ─────────►  player_ids.parquet  ◄─────────  gsis_id
   (board, lineups,        espn_id ↔ gsis_id              (player_weeks,
    draft)                 ↔ fantasypros_id                opportunity,
                           ↔ sleeper / yahoo /             depth_charts,
                             pfr / sportradar              contracts)
                                  │
                            name_key / merge_name
                        (normalised-name fallback)
```

| Dataset | ids it carries |
|---|---|
| `store/board` | `player_id`, **`gsis_id`**, `name_key`, `player_name` |
| `store/lineups`, `store/draft` | `player_id`, `player_name` |
| `nfl/depth_charts` | `gsis_id`, **`espn_id`**, `player_name` |
| `nfl/player_weeks`, `opportunity`, `contracts` | `gsis_id` |
| `projections/Usage` | `gsis_id`, `player_id`, `name_key`, `full_name` |
| `injuries/espn` | `name_key`, `full_name` — **no id at all** |

**`board.parquet` is the join hub.** It is the only store artifact carrying both
`player_id` and `gsis_id`, so it is where nflverse data attaches to league-scored data.
`depth_charts` is the other native bridge, carrying both ids on the row — which is
exactly why the rookie arm works.

The crosswalk covers every player it has ever seen, including the long retired, so its
per-column fill rates look low in aggregate and are much higher on a live roster.
`python -m Scripts.crosswalk` reports coverage against a built board. Ranked by how
much you can rely on them: `pfr_id` and `espn_id` are the strongest, `gsis_id` close
behind, `fantasypros_id` the weakest by a distance.

Team D/ST units match no id in any provider and join on name alone.

---

## 8. Gotchas worth knowing before you query

- **Never compare points across leagues.** Everything in the store is scored in its
  own league's rules. Ranks compare; points do not.
- **There is no `USG_Points` column any more.** TOMCAT left the blend on 2026-09-07 and
  is no longer priced, so nothing scores its stat line. The `USG_<stat>` columns are
  still there and still on a full 17-game slate — `to_full_slate` divides each player's
  own `expected_games` back out, and `usg_expected_games` travels beside the line rather
  than inside it. To compare the model against the blend, score the stat lines yourself
  or read `python -m Scripts.lab.sources`, which still measures them.
- **The model's carry lines run about 10% light**, which is why it was withdrawn: 384
  projected carries per team against a realised 450–465. Anything you build on
  `USG_rushingAttempts` or `USG_passingAttempts` inherits that; `USG_receivingTargets`
  is unaffected and sits at 0.999 of the field. See
  [plan 43](plans/43-tomcat-out-of-season-blend.md).
- **`TRUE_` is reconciled to team totals; the source columns are not.** A completed
  pass is one team's passing yard and one of its receivers' receiving yards, so those
  two sums must match — and player-by-player projections have nothing holding them
  together. Before `reconcile_team_totals`, `receiving/passing` ran 0.80 to 1.23 across
  the league. ESPN holds all three identities at exactly 1.000, which is how we knew
  they reconcile and we did not. Each side is scaled to the **midpoint** of the two, so
  ranks within a team are unchanged and only the level moves.
- **`usg_expected_games` carries role as well as health.** It is fitted from prior
  availability, snap share, reserve status and age, so a low number on a backup means
  buried, not fragile. That is why the board build withdraws the model's line where
  the depth chart says backup *and* ESPN has priced the player out, rather than
  trusting a starter's slate for him.
- **ESPN injuries carry no id**, so they join on normalised name only. It is the most
  fragile join in the repo, and suffixes (Jr./Sr./II/III) are the usual failure.
- **Check `*_is_imputed` before trusting a source column.** A source can be present in
  the schema and 0% real — pre-season that is the normal state for the sportsbooks.
- **Column width varies by league**, legitimately. Do not treat it as corruption.
- **`team_stats` is opt-in and generally not built.** Points-over-expectation per
  manager needs past seasons scored in each league's rules, which is exactly what it
  would supply — that is the missing input in [plan 23](plans/23-owner-tendencies.md).
- **A season in progress has no play-by-play-derived data**, so most of the nflverse
  tier lags by one season until week 1 is played.
