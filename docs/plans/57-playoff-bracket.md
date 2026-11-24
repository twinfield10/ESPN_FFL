# 57 — Playoff bracket

**Status:** IN PROGRESS

**Where it stands:** Phase 1 (if the season ended today) is built. Phase 2 (odds) is next.

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

## Phase 2: odds (next)

Simulate the remaining regular-season weeks many times and run `seed` on each
simulated season. Everything below is a tally over the same simulations:

- **Seed odds**: P(seed = k) for every team and k, shown as a team × seed table,
  plus P(playoffs) and P(bye).
- **Division odds**: P(wins division).
- **Likeliest first opponent**: the most common opponent in the team's first playoff
  game (round 2 for a bye team), with its probability.

**Open:** the score model for future weeks. The Matchup tab's calibrated win
probability ([42](42-weekly-matchup-odds.md)) covers the current week. Weeks after it
need a per-team mean and spread from rest-of-season lineup projections. The remaining
schedule is in `team_stats` as unplayed 0-0 rows. Jeffs' weeks 12–13 come from the
new league, so the stitch already covers them.
