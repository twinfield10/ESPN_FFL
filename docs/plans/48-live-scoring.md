# 48 — Live scoring: what actually happened, everywhere

**Status:** COMPLETE

**Priority:** High · **Effort:** M · **Where it stands:** shipped 2026-09-10, all nine leagues
**Depends on:** [42 (weekly matchup odds)](42-weekly-matchup-odds.md) — the dispersion
this narrows
**Unblocks:** the projection-accuracy surface [plan 08](08-frontend-weekly-views.md) §5
reserves; measuring the blend against 2026 while the season is on

## Problem

Every number in the app was a projection, all week long. Week 1 opened on Wednesday
2026-09-09 with NE at SEA and the pipeline went on showing 18.9 projected points for
a receiver who had already caught 8 for 122.

**The actuals were never the missing part.** `extract_player_stats` has written
`points`, `projPoints` and the raw actual stat line into every player-week since the
artifact existed, and `clean_lineups` carried them through `base_cols`. What was
missing was a way to tell **why a number is zero**:

| Candidate signal | Why it cannot answer |
|---|---|
| `points == 0` | Identical for "played and scored nothing" and "has not kicked off". `build_league_frame` runs `df.fillna(0)`, so a null cannot carry the difference either |
| `player_active_status` | Set in `espn_api`'s `Player.__init__` by looping **every** scoring period, so it means "has stats somewhere this season". On 2026 week 1: **102 of 242 rows read `active` when 13 players had played** |
| `BoxPlayer.game_played` | `100 if now > kickoff + 3h else 0` — a clock heuristic, `0` for the whole of a live game and `100` in the middle of an overtime |
| `NFL_Schedules.csv` | Has final scores, but `R/GetNFL.R` runs at 06:00 — a game that ends at 16:20 Sunday reads as unplayed until Monday |

Nothing in the repo had ever called ESPN's scoreboard; `mScoreboard` appeared in no
code.

## What shipped

### `Scripts/game_state.py` — the missing signal

One request per week to `site.api.espn.com/.../scoreboard` (the host
`scrape_espn_injuries` already uses, and whose access reasoning it already carries).
Returns one row per team: `state` ∈ `pre` | `in` | `post` | `bye`, plus `elapsed`,
the fraction of regulation played, from `period` and `displayClock`.

**No name reconciliation on this path.** Verified: the scoreboard's 32
`team.abbreviation` values match `espn_api`'s `PRO_TEAM_MAP` values exactly, both
directions, so the join to `pro_team` is direct. Only nflverse disagrees (`LA`/`WAS`
against `LAR`/`WSH`), which is why `from_schedule` — the offline fallback — is the one
function that aliases anything. `ESPN_TEAM_ALIASES` moved to `Scripts/nfl_utils.py`,
where it is a fact about the schedule file rather than about the draft board;
`Scripts.draft.board` re-exports it.

**A bye is read as an absence**, from the payload: a team with no game that week. So
it is right in a week ESPN has and the local CSV does not, and it needs no
missing-week arithmetic.

Three status readings are named rather than trusted, because ESPN's `state` and
`completed` disagree exactly where it matters:

- **postponed** reports `state: "post"` with `completed: false`. Read literally, every
  player in it is finalised at zero for a game that will be replayed → treated as
  `pre`.
- **cancelled** genuinely never happens → `post`, and zero is final.
- **suspended** (`post`, not completed, not named) → `in`, which neither locks a score
  nor hands the projection back.

A settled week is exempt from the cache TTL: `clean_lineups` sees weeks 1..current and
runs nine times a night, and without that the nightly re-fetched sixteen scores that
were final in September.

### `Scripts/live.py` — one number per player-week

```
banked      = points, where the game has started, else 0
remaining   = TRUE_Points x (1 - game_elapsed)
LIVE_Points = banked + remaining
```

One expression, not four branches, because scoring is linear. **At `elapsed = 0` it is
exactly `TRUE_Points`** — so this was inert until the first kickoff, which is what made
it shippable in week 1 rather than next August.
`tests/test_live_points.py::test_before_any_kickoff_live_is_exactly_the_blend` pins it.

Eight columns land on `lineups.parquet`: `game_state`, `game_elapsed`, `game_locked`,
`LIVE_Points`, `LIVE_remaining_points`, `ACT_Points`, `actual_unpriced`,
`LIVE_PosRank`. Verified **purely additive**: rebuilt against the store the old code
wrote the same morning, all 614 shared columns identical, nothing removed.

**A finished game uses ESPN's number, not our scoring of the actual line** — the one
place the `X_Points == score(X_ stat line)` identity is deliberately not extended, and
the reason is measured rather than assumed:

| League | ESPN `points` | `score(actual line)` | |
|---|---|---|---|
| `winfield_football`, all 13 players who had played | — | — | agree to float noise, kickers and both D/ST units included |
| `john_pc_league`, Jaxon Smith-Njigba | **31.20** | 26.20 | **Δ 5.00** |

That league prices five long-touchdown bonuses (`PTD50`, `RTD40/50`, `RETD40/50`) that
`score_to_lab_dict` does not map. ESPN's `points` *is* the league's official score; an
app that disagrees with the box score about a finished game is wrong whatever the
architecture prefers.

So the scored value stays beside it as `ACT_Points` and the gap gets a name:
**`actual_unpriced = points - ACT_Points`**, non-null only on a started game, warned
per league and per player. It is the same shape as `espn_unpriced`, and the stronger of
the two: `report_silent_zero_stats` can only say "this rule is always zero" about a
projection, while this says "ESPN paid 5.00 points for a real week that we cannot
price". That is [plan 34](34-stat-first-audit.md)'s outstanding item, now measured
against realised data rather than owed.

**No `LIVE_<stat>` hybrid stat line.** It would be ~45 columns nothing reads, and an
unconsumed column shaped like a source is this repo's oldest failure mode. `LIVE` is
also deliberately **absent from `WEEKLY_PREFIXES`**, which is the register of
projection *sources* — `Scripts.lab.sources` reads that tuple to decide what a source
is, and a resolved output listed there would be reported as a sixth opinion.

### `--what live` — the ten-minute path

| | |
|---|---|
| `Scripts/refresh.py` | `"live"` in `WHAT_CHOICES`, not in `DEFAULT_WHAT`. Skipped when `lineups` is also requested |
| `Scripts/live.py` | `week_box_scores`, `patch`, `refresh_live` |
| `ops/espn_ffl_live.sh` | cron wrapper, `*/10 * * * *` |

Re-reads the current week's box scores **and the free-agent pool**, patches the actuals
*and the roster* onto the frame already in the store, and touches no projection column
and no source file, and is verified to produce a frame numerically identical to a
full rebuild.

**Measured in week 1: 7.2s against 9.8s on `knights_ffl`, so about a quarter
faster -- and that understates it in the direction that matters.** The ~25s the store
docs quote for `lineups` is a *full-season* figure: `get_ply_stats_by_matchup` loops
weeks 1..current, so its cost grows every week while this reads one week and stays
flat. The structural difference is the larger point either way -- it reads **no source
parquet at all**, so it cannot raise a staleness warning, cannot be affected by a
source going dark mid-week, and moves no projection column, which is what makes it
safe to run 144 times a day.

The roster half is not incidental: a manager who promotes a bench player before kickoff
changes `slotPosition`, and a refresh that patched only `points` would show live scores
against last night's lineup. The free-agent pool is not optional either — the Free
Agents tab has an Actual column and `pool_playable` decides whether a player can still
be started, and half-live is a worse failure than not-live because nothing about it
looks broken. The pool costs ~4s of the ~7s.

It **refuses rather than guesses** when the stored frame holds a different week:
appending one it does not have would add a second, projection-free copy and halve
every team total, and the result would look like an ordinary store. A player in the box
score but not in the frame — added mid-week — arrives flagged `live_only` with actuals
and no projection, and is counted loudly, because such a row understates his team
before his kickoff.

The cron wrapper self-gates on `Scripts.game_state --if-live`, which is true while a
game is in progress **and for six hours after each kickoff**. The tail is the load-
bearing part: the run that captures the final score has to happen *after* the last game
ends, and at that moment nothing is "in progress" — so a gate of "anything live" alone
would leave a mid-game hybrid as the store's permanent record of the week. On a Tuesday
the job is one HTTP request and an exit 0.

### Consumers

| Surface | Change |
|---|---|
| `app/lineup.py` | `optimal_lineup(..., respect_locks=True)`: a locked starter keeps his slot **whatever `player_active_status` says** — a player ruled out an hour before kickoff scored zero and is still occupying it — and a locked bench player can no longer be promoted. So the suggested lineup is one you are allowed to set. `hindsight_lineup` and `points_left_on_bench` answer the after-the-fact question |
| `app/lineup.pool_playable` | Was `ESPN_Points > 0`, a measured proxy that was right about byes and blind to kickoff. Game state answers both at once, and correctly excludes a free agent whose game has already ended. The proxy stays as the fallback for an older store |
| `app/matchup_sim.side` | **The spread now comes from `LIVE_remaining_points`, not the total.** Banked points have no uncertainty left, and `Var = phi*mu + mu^2/k` at the remaining projection returns exactly zero for a finished player. Read off the total, a team with every game final and 140 points banked would still have been quoted near 70%. No refit: at `elapsed = 0` the remaining projection *is* the projection, so plan 42's coverage of 0.802 still describes it |
| `app/matchup_sim.Side.modelled` | `sd == 0` stopped being one thing. It used to mean only "no fitted model, no probability"; it now also means "every game is final, so the outcome is certain" — opposite readings from the same number. `outcome` was discarding the second, quoting `None` for a settled matchup |
| `app/views/weekly.py` | Live and Game columns; `Actual` is shown once **any game has started**, replacing `frame["points"].sum() == 0` — a proxy that flipped the instant one player scored and would have hidden the column through a week in which everybody genuinely scored zero |
| `app/lineup_table.py` | `LIVE` leads the points block; `ACT` when something is locked; a `GM` state column beside the team |
| `app/home.py` | `banked_points` gives `This Week` off the ten-minute refresh rather than off `team_stats`, which is a weekly artifact and hours stale mid-slate |
| `Scripts/outcomes/weekly.residuals` | **Finished games only.** The live refresh rewrites `lineups.parquet` every ten minutes, so a refit during the slate would read a half-played week as a set of enormous negative residuals and widen every interval in the app. Measured before the guard: 951 contaminating rows |
| `Scripts/lab/accuracy.py` | `--week N`: each source's weekly points MAE and bias over finished games, gated at a **leave-one-out** projection of 5 points — built from the other sources, never from the one under test, which would let a source excuse itself from the rows it was unsure about |

## Two bugs found on the way

**`residuals()` could not read 2026 at all.** `pl.concat` with the default strategy
raised `SchemaError: type Int64 is incompatible with expected type Float64`, because
`points` is written from whatever ESPN returned and `weenieless_wanderers` had no
fractional total yet — so one league's column was `Int64` and the other eight were
`Float64`. Latent until a 2026 store existed to read. Now `vertical_relaxed`.

**A mid-week waiver claim landed unnamed.** `player_name` was not in `PATCH_COLUMNS`,
and an added row is built from those columns and nothing else, so it arrived as a row
with no name — invisible in every table that renders one.

## The one thing left owed

`current_week()` is "the first week with an unplayed game", and it does not roll over
until `R/GetNFL.R` writes Monday night's score at 06:00 Tuesday. So `--what live`
polls week N until Tuesday morning, which is harmless (every game is `post`, the
patch is a no-op) but means Tuesday's early runs do no useful work. Worth folding the
scoreboard into `current_week` rather than leaving two sources of truth about which
week it is.

## Verification

```bash
python -m Scripts.game_state                                    # the week's board
python -m Scripts.refresh --league winfield_football --what live
python -m Scripts.lab.accuracy --season 2026 --week 1
pytest tests/test_game_state.py tests/test_live_points.py
```

On 2026-09-10 the board read 16 games, NE/SEA `post`, the rest `pre`, 0 byes; the
live refresh patched 242 rows in 6.8s; and week 1's accuracy table came back on the
three finished-game starters `winfield_football` had.
