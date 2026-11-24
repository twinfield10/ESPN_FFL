# 57 — Playoff bracket

**Status:** IN PROGRESS

**Where it stands:** Phase 1 (if the season ended today) shipped in PR #118. Phase 2
(odds) is built and dry-run on all nine leagues. **Owed:** the first nightly run on the
mini after merge, which records `playoff_settings`/`fixtures` and then builds the odds.

**What it is:** a **Playoffs** tab between Free Agents and Draft. It shows every
league's seeds and the fixed bracket they produce, using that league's own ESPN rules.
For Jeffs_League it uses the stitched season ([prior_league](../../Scripts/prior_league.py)),
because ESPN's records and seeds for the recreated league only start at week 10.

---

## The rules come from ESPN

`mSettings.scheduleSettings` carries `playoffTeamCount`, `playoffSeedingRule`,
`playoffReseed`, `matchupPeriodCount` and the matchup period → scoring week map.
`fetch_utils.get_roster_settings` reads them from the payload it already fetches, and
`store.build_meta` writes them to `meta.json` as `playoff_settings`, so the first
nightly after merge adds them to all nine leagues. `simulation_utils.sort_standings`
assumed total points because "the API doesn't seem to get this data correctly"; it
does.

| League | Teams | Playoff teams | Byes | Divisions | Tiebreaker | Regular season | Rounds |
|---|---|---|---|---|---|---|---|
| Winfield_Football | 6 | 4 | 0 | 2 | Total points | 13 | Weeks 14–15, 16–17 |
| Jeffs_League | 8 | 4 | 0 | – | Head-to-head | 13 | 14, 15 |
| GOP_Degenerates | 16 | 7 | 1 | 4 | Head-to-head | 14 | 15, 16, 17 |
| Knights_FFL | 14 | 8 | 0 | – | Total points | 14 | 15, 16, 17 |
| 12 Dudes, Richardson | 12 | 6 | 2 | – | Total points | 14 | 15, 16, 17 |
| John_PC, John_ATL, Washed_Up_Fijians | 12 | 6 | 2 | 2 | Total points | 14 | 15, 16, 17 |

## Seeding, as ESPN actually does it

Rules found by matching ESPN's `playoffSeed` for every team in **36 league-seasons**
(all leagues, 2019–2026, minus Jeffs 2026). The final rules match **all 36**:

1. **Division winners are seeded first**, then wildcards. ESPN has no setting for this.
   GOP 2026 seeds two 6-5 division leaders above a 7-3-1 wildcard.
2. Within each group, teams are ordered by win percentage, with a tie counting as half a win.
3. Ties are broken by the league's rule. **Head-to-head goes one seed at a time:**
   - If every pair of tied teams has played, the best record in the games among them
     takes the seed. If they are all level, points for decides.
   - If any pair has not played, the top points-for team takes the seed.
   - That team is removed, and the rest of the group is re-evaluated from scratch.

   This is the rule because two simpler versions each missed real cases. GOP 2025 has
   a three-way circle that points break; the pair left over then goes by their own
   game, not by points. GOP 2024 had a team that never met the others, so the top two
   seeds went on points, then 8 over 1 on head-to-head.

`tests/test_playoffs.py` pins each of these cases on a few synthetic teams. The scratch
validation (the live seeds plus `/leagueHistory` for 2019–2025) is not in the suite,
because it needs ESPN.

**Jeffs 2026 after week 11:** Jason Walker, Jeff Wilhelm, Sam Giammalva, Tommy
Winfield. Three teams finished 7-4, and Tommy, the league's top scorer, is fourth: 0-3
against the other two.

## Phase 1: if the season ended today (built)

- `Scripts/playoffs.py`: `seed`, `bracket`, `byes`, `round_weeks`, plus readers for
  `team_stats`. Pure polars; no ESPN calls and no store reads.
- `app/views/playoffs_tab.py` + `app/routes/playoffs.py`:
  - a seeds table (division ★, Bye/In/Out, a note on how each tie was broken, the
    viewer's row banded, the playoff line ruled);
  - one bracket column per round, with its weeks.
- Only completed weeks count (`current_matchup_period - 1`, capped at the regular
  season). The week selector does not apply here.

## Phase 2: odds (built)

`Scripts/playoff_odds.py`, run nightly as `python -m Scripts.refresh --all --what
playoff_odds` after `team_stats` (non-fatal, about a second a league, nothing fetched).
It simulates the rest of the regular season and the bracket 10,000 times, seeds each
simulated season with `playoffs.order`, and counts. Output per team (artifact
`playoff_odds.parquet`):

- P(seed = k) for each playoff seed, P(playoffs), P(bye), P(division) (division leagues);
- P(final), P(title), expected W-L;
- **likeliest first opponent**: the most common opponent in the team's first playoff
  game (round 2 for a bye team, against whoever wins the game feeding its slot), with
  how often, among the simulations where it makes the playoffs;
- projected points per week over the weeks left.

### The model, as decided with the owner

1. **Who starts is decided by ROS rank.** Each simulated manager starts the best legal
   lineup by FantasyPros' rest-of-season ranking. FantasyPros ranks each position on
   its own page, with no overall rank, so the rank can't settle a flex spot on its
   own. Instead it becomes a **rank-implied value**: within a position, the league's
   rostered players sorted by ROS rank take that position's values sorted high to low.
   - The order within a position is FantasyPros'; the scale across positions is the
     league's own scoring.
   - Players FantasyPros doesn't rank (IDP, the fringe) keep their own value.
   - The optimiser is the Roster tab's, moved verbatim to `Scripts/lineup_solver.py`
     and re-exported from `app/lineup.py`.
2. **What a starter scores is this week's blended projection.** The owner proposed
   mixing the projection with the season average, and the mix was measured rather than
   guessed. Over 2026 weeks 2–11, all nine leagues, 27,671 player-games in each
   league's own scoring, predicting 0–3 weeks ahead:

   | Weeks ahead | n | MAE projection | MAE season avg | Best weight on projection |
   |---|---|---|---|---|
   | 0 | 10,057 | 5.63 | 6.28 | 1.00 |
   | 1 | 7,142 | 5.84 | 6.57 | 1.00 |
   | 2 | 5,806 | 5.98 | 6.60 | 1.00 |
   | 3 | 4,666 | 6.10 | 6.70 | 1.00 |

   RB and D/ST preferred 0.80–0.85, but that improved MAE by less than 0.03. So the
   season average is only a fallback, for a player with no projection this week (most
   recent projection first, then the average).
3. **Spread** comes from the Matchup tab's weekly dispersion model
   ([42](42-weekly-matchup-odds.md)). A player whose game this week is over counts his
   actual with no spread.
4. **Availability:**
   - a bye that week (`board.bye_weeks`) rules a player out;
   - this week, ESPN's status decides;
   - a player out this week is back next week, unless he's inactive on IR, in which
     case he's out for the rest of the season.

**Not modelled:** waivers and trades (rosters are frozen), commissioner adjustments in
future weeks, and correlation between players.

### Inputs the nightly now records

`team_stats` stops at the current week, so the weeks still to play are recorded from
the live league into `meta.json` as `fixtures`. They match `team_stats` exactly for
weeks 1–12 in eight leagues. In Jeffs_League, weeks 1–9 differ, because the recreated
league scheduled its offline weeks differently from the old one; weeks 10+ match, and
only weeks after the last completed one are ever read.

### Dry run, 2026-11-24 (through week 11, mini's ROS capture)

All nine leagues ran in 0.4–1.4 s each, and projected points per week land within the
range of each league's real per-game scoring (Jeffs 101–143, Winfield 117–144). Two
defects were caught by sanity-checking magnitudes before anything shipped:

- **Free-agent rows reached `slot_counts`.** They read as 20 QB slots, and lineups
  projected 260 a week.
- **The ROS rank turned out to be per-position.** It was first used as if it were an
  overall rank.

**The odds move with the ROS capture.** The same Jeffs inputs with a month-old capture
moved one team's title odds from 37% to 28%. That's expected, since lineup choice is
the point of the model, but it means a stale `FantasyPros_ROS_Ranks.parquet` gives
stale odds.
