# 23 — Owner tendencies from draft history

**Priority:** **High, date-driven** · **Effort:** M · **Status:** **Done** (2026-08-10)
**Depends on:** [07 (store)](07-frontend-foundation.md) — done ·
[15 (draft board)](15-draft-board.md) — done
**Unblocks:** [09 (draft views)](09-frontend-draft-views.md)'s **Draft History**,
which was blocked on roadmap Phase 1's backfill.

The board says who to take. It said nothing about the eleven other people in the
room. This adds the second half: for each manager, what they reliably do that the
rest of their league does not.

## Goal

One short, checkable description per manager — the kind of thing you would say to
a friend before a draft. "Takes a kicker in round 5." "Cowboys homer, and he will
take Zeke again." "Autodrafts half his picks."

## What the investigation found

### 1. The whole history is one request per league-season, and it is complete

`view=mDraftDetail` on `leagueHistory` returns every pick a league has ever made.
Probed across all nine configured leagues, every season, 2016-2025:

| | |
|---|---|
| League-seasons | 36, **all 200 OK**, none missing |
| Picks | 5,748 |
| Formats | 7 snake, 2 auction (GOP Degenerates, Washed Up Fijians) |
| Player metadata resolved | **5,748 of 5,748** |
| Deepest history | Winfield Football, 10 drafts; Weenieless Wanderers, 9 |

It is the cheapest historical data in this repo: ESPN keeps it forever and a
finished draft never changes. All nine leagues build in **10 seconds**.

### 2. Three joins that each looked trivial and were not

**Players.** `seasons/{year}/players?view=players_wl` returns that season's whole
universe. It is league-independent, so one request per *season* serves all nine
leagues. It resolves 100% of drafted ids, D/ST included.

**Owners are not on the pick.** `memberId` looks like the drafter. It is not: in
six older league-seasons *every pick in the draft* carries one member GUID, which
credited a single manager with all 96 picks of the 2016 Winfield Football draft.
`teamId` is right in every season, so the owner is resolved through
`teams[].primaryOwner`. The *name* then has its own gap — ESPN drops
`firstName`/`lastName` from some old payloads while keeping the GUID stable — so
names are pooled across the league's seasons, newest winning. That fills all 614
otherwise-anonymous picks.

**Rookies are the one field ESPN cannot supply.** Its universe carries a `Rookie`
eligible slot, but not before about 2019 and never for the seasons with the most
history: 0 rookies in 2016, 2017 and 2018. nflverse's `years_exp` is complete but
has a hole of its own — **every one of the 8,263 rookie rows in the 2023 roster
file has a null `espn_id`**. Pooling `entry_year` across *all* seasons by
`espn_id` closes both: a 2023 rookie resolves through his 2024 row. Match rate on
skill-position picks: **100%**.

### 3. An undrafted season looks exactly like a drafted one

ESPN pre-creates the full set of pick slots for a draft that has not happened,
with `playerId: -1`. Measured live on 2026-08-10: Winfield Football returned 96
picks for 2026 and the first build read them as an eleventh completed draft in
which all six managers took nothing — censoring every position one round past the
end for everybody, which moves every timing baseline in the league. The
`draftDetail.drafted` flag is the gate; unresolvable player ids are the second.

**The obvious second guard would have been a bug.** Filtering `playerId <= 0`
looks right and would have silently deleted every team defence — ESPN's D/ST ids
are negative (`-16027` is Tampa Bay), and the D/ST timing tendency is one of the
strongest signals in these leagues. The test is membership in the season's
universe, not the sign of the id.

## The design decision: the room is the baseline

Every measurement is against **the room that manager was actually sitting in**,
with that manager left out of it. This does the work of a dozen special cases:

- No external ADP is needed, and none exists for 2016.
- It is immune to league size, scoring and format — these leagues run 6 to 16
  teams and 14 to 17 rounds.
- "Took a quarterback in round 4" means nothing. "Took a quarterback in round 4
  when the other five averaged 4.9" is a fact about the manager.

**Leave-one-out, not the plain mean.** In a six-team league a manager is a sixth
of the room, so including them shrinks their own deviation by 17% — in exactly the
leagues with the longest history. Chaille Winfield's kicker tendency reads -6.9
rounds against the pooled mean and **-8.2** against the room that excludes him.

**Nothing is pooled across eras.** Rookie appetite, positional shares and NFL-team
leans are each computed within a season and only then averaged over the manager's
own seasons. These leagues drafted 5 rookies in 2016 and 168 in 2025; pooled,
every long-serving manager would read as rookie-averse and every recent arrival as
rookie-hungry, purely from when they showed up.

## What is measured

Two families chosen by format, because a nomination order is not a valuation:

| Family | Snake | Auction |
|---|---|---|
| Timing | first round each position comes off their board | — |
| Shape | first-three-rounds positional mix | share of budget by position; top-3 concentration |

And four that apply to both: NFL-team lean (binomial against the league's own
drafted pool), player loyalty (times drafted over the drafts the player was
*available* in), rookie appetite, and autodraft rate.

A trait needs an effect size **and** consistency — two thirds of a manager's
drafts on the same side — **and** two drafts. Managers with one are named and left
alone rather than described.

## What this does not do

**No outcome is measured.** Points-over-expectation per manager needs every past
season scored in that league's own rules, and the store holds one season. Whether
the round-5 kicker was a mistake is a different question from whether it is
coming. Predictable is what a board can use.

## Result

`--what draft` writes two artifacts per league: `draft.parquet` (the picks, the
evidence) and `tendencies.parquet` (the reading of it). Split because every
threshold in the reading is a judgement call that will be revised, and re-pulling
ten seasons to move one constant would be absurd.

Across the nine leagues: **112 managers, 103 with a measured tendency, 6 too new**
— the other 3 have the drafts and simply match their room, which is itself worth
knowing.
The descriptions the deepest league produces:

> **Chaille Winfield** — Takes K early, round 5.3 against the room's 13.5. Has
> drafted Justin Tucker in 8 of the 9 drafts he was available in. Takes D/ST
> early, round 7.6 against the room's 12.3.
>
> **Hank Winfield** — Has drafted Ezekiel Elliott in 7 of the 7 drafts he was
> available in. Takes TE early, round 3.2 against the room's 6.3. Leans DAL, 15
> Dallas picks against 6.5 expected.

Rendered on the draft board page as cards, a dot plot of positional timing against
the room, and a table of the measurements underneath.
