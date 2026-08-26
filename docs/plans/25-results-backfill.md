# 25 — `results`, the artifact that reaches back

**Status:** IN PROGRESS

**Priority:** Medium · **Effort:** S · **Where it stands:** **Done** (2026-08-14), Winfield_Football only
**Depends on:** [23 (draft history)](23-owner-tendencies.md) — the picks it joins against
**Unblocks:** any question about how a past season actually went; roadmap Phase 1's
points-over-expectation

## Problem

The store held **one** played season. Every retrospective question — did the draft
work, does a manager live off the wire, what did a pick actually return — could
only be asked of 2025, while `draft.parquet` already held ten seasons of picks
going back to 2016. Half of every such question was available and the other half
was not.

The obvious fix is to build `lineups.parquet` for past seasons. **It cannot be
done, and not for want of effort.** `lineups` is the *blended* frame: it carries
`FP_`, `PINNY_` and `BOL_` columns, so building it needs that season's projection
files. FantasyPros serves no season parameter — the same fact that forces
[plan 18](18-season-usage-model.md)'s G2 to be unmeasurable on history and that
[`Data/G2/`](24-s3-data-flow.md) exists to work around — so the inputs for 2024
are simply gone. `--what lineups --season 2024` fails on a missing FantasyPros
file and always will.

## What shipped

A new store artifact holding **what was scored and nothing else**:

```
Data/Store/<season>/<league>/results.parquet
store/season=<season>/league=<league>/results.parquet
```

Eight columns: `week`, `team_owner`, `team_name`, `player_id`, `player_name`,
`slotPosition`, `primaryPosition`, `points`. No projections anywhere near it,
which is exactly why it can be built for a finished season — it needs only ESPN
box scores.

`slotPosition` is load-bearing rather than descriptive. **`BE` and `IR` points
counted for nobody**, so dropping the column would silently answer a different
question than the one asked.

| | |
|---|---|
| `Scripts/store.py` | `results` registered in `ARTIFACTS`, so `sync` picks it up with no change — the store push iterates that dict |
| `Scripts/refresh.py` | `--what results`, over `get_ply_stats_by_matchup` |
| `app/store.py` | `load_results` |
| `app/draft_view.py` | `drafted_versus_added`, `acquisition_history`, `acquisition_averages` |

## The 2019 floor

`espn_api` refuses box scores before 2019 — *"Cant use box score before 2019"* —
so **2016, 2017 and 2018 are unreachable**, for this league and any other. Those
three seasons have drafts and will never have results. The limit is upstream, not
ours, and the season dropdown says so rather than leaving a reader to wonder.

Backfilled for Winfield_Football: **2019–2025, seven seasons**, ~20s each, 1,632
rows per season. 2025 was rebuilt as `results` too, so the reader is uniform
rather than branching on which artifact a season happens to have.

## Three identities, and which one to join on

The point of the exercise, and the thing that cost the most time. ESPN offers
three ways to identify a team and **each is stable over a different span**:

| Identity | Stable within a season? | Stable across seasons? |
|---|---|---|
| owner name | **no** | no |
| `team_name` | **yes** | no |
| `owner_id` (GUID) | yes | **yes** |

So the join *within* a season is on `team_name`, and the grouping *across* seasons
is on `owner_id`. Neither can be swapped for the other.

The owner name fails in four distinct ways, each real, each in a different league,
all in 2025 — and each one costs a manager their entire draft:

| `results` / `lineups` | `draft.parquet` | Cause |
|---|---|---|
| `Hank Winfield` | `hank Winfield` | `str.title` in `set_owner_names` |
| `Zach Imel` | `Zachary Imel` | a nickname |
| `Logan Tola` | `Matt Logan Tola` | an extra given name |
| `Alex Holton` | `Michael Beal` | the team changed hands |

The first version keyed on a case-folded owner name — which fixes only row one —
and reported three managers as having drafted **nobody**, Zach Imel's entire
2,592.95 points among them. That reads as a perfectly plausible story about a
manager who churned his roster, which is what makes it dangerous. Row four cannot
be fixed by any string rule and is what settles it: the question is what a *roster*
got from draft day, and a roster survives a handover even though the name against
it does not.

`team_name` was checked before being trusted, across all nine leagues for 2025: no
team renamed itself mid-season, no two teams shared a name, all 108 matched a
drafting team.

Across seasons the names drift again — Jack's team went `Cococnut Crushers` →
`Coconut Crushers` in 2023, Tommy renamed his four times, and ESPN recorded Jack
as `J W` for one season, which split him into a separate manager with one season to
his name. `owner_id` has been identical for all six managers every year since 2019.

## Where it refuses to answer

Both cases are the same lesson, and this repo has paid for it repeatedly — an
absent source reading as a real answer:

- **A team absent from that season's draft gets `null`, not `0`**, and is named on
  the page as unmatched. Zero would claim they drafted nobody who scored.
- **A season with no draft data returns empty.** With no picks every point
  classifies as "added", so the alternative is reporting a whole league that built
  itself off waivers.

## Measured: Winfield_Football, 2019–2025

Per-season averages, points from a starting slot:

| Manager | Points | From the draft | From the wire | % drafted | Moves | Pts / move |
|---|---:|---:|---:|---:|---:|---:|
| Chaille Winfield | 1855 | 1709 | 147 | **92.2%** | 3.9 | 36.0 |
| Jack Winfield | 1915 | 1613 | 303 | 83.8% | 14.4 | 29.5 |
| Frankie W | 1938 | 1606 | 332 | 82.8% | 9.7 | 35.3 |
| Will Winfield | 2127 | 1669 | 458 | 78.2% | 12.9 | **38.9** |
| Hank Winfield | 1890 | 1386 | 505 | 71.9% | 22.1 | 23.5 |
| Tommy Winfield | 2047 | 1370 | 677 | **66.6%** | 28.3 | 25.6 |

Two things worth noticing. **Wire volume and wire quality are not the same
skill** — Tommy makes seven times Chaille's moves for 25.6 points each against her
36.0, and Will makes fewer moves than either of the two most active managers while
returning the most per move. And **the highest scorer is not the best drafter**:
Will tops the points column on the third-highest drafted total.

A move is a distinct player brought in, bench included, because claiming a player
is a move whether or not you start them. It is a **floor** on real transactions: a
player added, dropped and re-added counts once. The true count needs ESPN's
transaction log, which this does not read.

## What is left

- **The other eight leagues.** Deliberately not backfilled — the ask was this one.
  `--what results --season <y>` per league-season is all it takes, ~20s each, and
  the page already falls back to `lineups` so nobody lost the 2025 answer.
- **Points-over-expectation**, roadmap Phase 1's remaining piece. This artifact is
  the input it was missing, but it needs each season *scored in that league's own
  rules*, and `points` here is what ESPN recorded rather than something re-derived.
- **Real transaction counts**, if points-per-move is worth sharpening.
