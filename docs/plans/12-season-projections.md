# 12 — Season projections (draft roadmap Phase 2)

**Status:** COMPLETE

**Priority:** High · **Effort:** Medium · **Where it stands:** Done (2026-08-03)
**Unblocks:** [09 (draft views)](09-frontend-draft-views.md) · roadmap Phase 3 (ADP + VOR + board)
**Blocked by, for defence only:** [11 (per-slot scoring)](11-per-slot-scoring.md)

## Why this exists

Pre-season there are no weekly markets, so `clean_lineups` has nothing to blend —
it raises `FileNotFoundError` for 2026 because no weekly Pinnacle or BetOnline
parquet exists. But season-long data exists from every source, and season-long is
what a draft board wants anyway.

`Scripts/season_projections.py` is the season-long counterpart to `clean_lineups`:
same provenance-and-renormalisation machinery from [plan 03](03-projection-source-coverage.md),
same per-league scoring from [plan 10](10-scoring-registry.md), one row per player
instead of per player-week.

```bash
python -m Scripts.season_projections --league Knights_FFL --season 2026
```

## Sources, and the trap in each

| Source | Players | Notes |
|---|---|---|
| ESPN | 329 | full universe; **mixed units**, see below |
| BetOnline | 273 (146 matched) | 546 props, **127 IDP** — the only IDP source |
| Pinnacle | 76 (74 matched) | offence only, clean |
| FantasyPros | 60 (60 matched) | gated to 10/position |

### ESPN mixes units inside one breakdown

The find that matters most. In `player.stats[0]['projected_breakdown']`, counts
(receptions, attempts, touchdowns) are **season totals**, but *offensive* yardage is
a **per-game average**. Each entry was verified against an independent cross-check
rather than assumed:

| Player | raw | cross-check A | cross-check B | verdict |
|---|---|---|---|---|
| Puka Nacua `receivingYards` | 93.5 | receptions × yds/rec = 1590 | every-5-yds × 5 = 1585 | per-game (×17 = 1590) |
| Jahmyr Gibbs `rushingYards` | 80.83 | 283.19 att × 4.85 = 1373 | every-5-yds × 5 = 1370 | per-game (×17 = 1374) |
| Josh Allen `passingYards` | 232.1 | — | every-25-yds × 25 = 3925 | per-game (×17 = 3945) |

**Return and defensive yardage are season totals and must not be scaled.** A D/ST
unit's `puntReturnYards` reads 302.98 against `puntsReturned` of 30 — 10.1 yards
per return, the league average, so that is already the season figure. I initially
scaled them too, which turned a 422-point D/ST projection into **2294**. Caught
only because the number was absurd; there is now a test pinning the exact
membership of `PER_GAME_IN_SEASON_ROW`.

Blending unscaled ESPN yardage against a season-long book line would have put ESPN
**17× low** and collapsed the blend onto whichever source remained.

**End-to-end check on the unit handling:** scoring ESPN's own season stat lines
with the league's own rules reproduces ESPN's own `projected_total_points` —
median ratio **0.994**, correlation **0.9995** across 273 players. That is a
`@pytest.mark.live` test.

### BetOnline descriptions are hand-typed

14% of props (74 of 546) failed the R script's parse. Three distinct causes, all
handled in `normalise_bol_props`:

- **Typos:** `Receiving Yrads`, `Receiving Yrards`, `Passing IND's` (D for T),
  `Passing INT's`, `Receiving TD;s`, `Rushing TD"s`, singular `Reception` and
  `Tackles & Assist`.
- **Combined markets:** `Receiving & Rushing Yards`. These cannot be apportioned
  between the two stats, so they get their own `rushingPlusReceivingYards` column
  rather than being guessed apart. 24 props.
- **Failed player/stat split:** irregular separators (`PHI-`, `KC -`, `-Total`)
  left `player == 'UNKNOWN'` with the name inside the stat text, e.g.
  `"De'Von Achane MIA -Total Receiving & Rushing Yards"`. Recovered by locating
  where the stat vocabulary starts.

Now **100% of props map** (546/546), with a live test that fails the build if
BetOnline changes a wording rather than silently dropping props.

### Names need a crosswalk, not string equality

BetOnline is uppercase, ESPN and Pinnacle are title case, and BetOnline contains
genuine misspellings (`Dalton Kinciad`). `normalise_name` strips accents,
suffixes and punctuation to a single key.

One subtlety worth keeping: punctuation is **dropped**, not replaced with a space,
so `A.J.` matches `AJ` and `De'Von` matches `DeVon`. Replacing with a space
splits them and silently fails to join exactly the punctuated names — A.J. Brown,
Ja'Marr Chase, De'Von Achane. My first version had this bug; two tests now cover it.

## Known limitation: defence in the IDP league

Point totals for IDP and D/ST rows in GOP_Degenerates are **not trustworthy yet**.
`proj_to_score` patches per-slot values for that league with hardcoded constants
that match neither season's real settings; rather than copy a known-wrong
workaround, this module scores from the unpatched table and prints a warning. The
warning keys on whether the league actually has IDP roster slots, so the eight
D/ST-only leagues are unaffected and stay quiet.

Fix is [plan 11](11-per-slot-scoring.md). Offensive projections are correct now.

## Verification

```bash
pytest tests/                  # 144 pass
pytest tests/ -m live          # 6 pass
python -m Scripts.season_projections --league Knights_FFL --season 2026
```

- `ESPN_Points` reproduces ESPN's `projected_total_points` (median 0.994, r 0.9995).
- Every BetOnline prop maps to an ESPN stat name.
- Board head for a standard league is sensible: Gibbs 351, Allen 346, Robinson 338,
  Nacua 333.
- No IDP warning for leagues without IDP slots.

## Next

Roadmap Phase 3 — ADP and auction values via `view=kona_player_info`, replacement
level from each league's real starting slots, tiers by 1-D clustering. `TRUE_Points`
and `TRUE_PosRank` from here are its input.
