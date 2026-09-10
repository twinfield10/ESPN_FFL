# 38 — The Athletic as a sixth blended source

**Status:** COMPLETE

**Where it stands:** Built and turned on at **0.25** on 2026-09-01 — a full equal
sixth vote, live on all ten boards. The per-stat MAE measurement
[20](20-consensus-sources.md) asks for is **owed rather than skipped**; see
*What is owed* below.

**What it is:** Jake Ciely's season projection workbook for The Athletic, ingested as
`ATH_` and blended at an equal sixth vote. 434 offensive players, raw stat lines,
scored through each league's own rules like every other source.

---

## The thing to be honest about first

[Plan 20](20-consensus-sources.md) asked whether to add another projection feed and
answered **no**. It then retired its own evidence as unmeasurable — the independence
table was computed on non-imputed cells only, when FantasyPros was real for sixty
players, so the sample was the top ten at each position and nobody else. What survived
retirement was a criterion, not a conclusion:

> If a feed does not reduce blended per-stat MAE, record the measurement and drop the
> feed. Do not wire it in at a token weight.

**This source was wired in at 0.25 before that measurement, deliberately, five days
before the GOP auction.** `docs/DRAFT_READINESS.md` asks for projection-moving merges
to be left alone in draft week and this is one. It is recorded here rather than
justified: the owner made the call with the alternative (register at 0.0, byte-identical,
measure in-season) on the table.

So the measurement is **owed, not skipped**. The 2026 boards in the store are the
record it will be scored against, the same way [plan 18](18-season-usage-model.md)'s G2
is frozen in `Data/G2/2026/`. What can be said now is what was measured now, below.

---

## What was measured before shipping

**Coverage is real, and better than both books.** Percentage of rows where the source
has a genuine line rather than an imputed one, on GOP's board:

| Stat | ESPN | FP | PINNY | BOL | **ATH** | USG |
|---|---|---|---|---|---|---|
| `rushingYards` | 9.6% | 20.1% | 0.8% | 1.4% | **12.8%** | 4.3% |
| `receivingYards` | 14.0% | 20.1% | 1.2% | 3.4% | **13.3%** | 10.2% |
| `passingYards` | 2.6% | 20.1% | 0.9% | 1.0% | **2.7%** | 1.4% |

It is the third-best-covered source in the blend and roughly ten times either book on
rushing. This is a coverage argument, not an accuracy one — coverage is what decides
how often the vote is cast, and accuracy is what plan 20's gate is for.

**The blend move, measured against the 06:00 build it replaced.** Joined on
`player_id`, not name — GOP is an IDP league carrying eleven duplicate player names
(a CB Lamar Jackson beside the QB, an LB Justin Jefferson beside the WR), and a
name-join fabricates a −408-point regression out of nothing:

| League | Rows | Moved > 0.05 pts | median \|Δ\| | p90 | max | Rank moved 15+ |
|---|---|---|---|---|---|---|
| GOP_Degenerates | 2,514 | 415 (17%) | 3.65 | 10.67 | 47.4 | 238 |
| Knights_FFL | 1,036 | 417 (40%) | 3.50 | 9.66 | 42.8 | 87 |
| Winfield_Football | 1,036 | 417 (40%) | 3.50 | 9.66 | 42.8 | 82 |
| Weenieless_Wanderers | 978 | 417 (43%) | 3.50 | 9.66 | 42.8 | 425 |

Weenieless' 425 is not a bigger move, it is a flatter board: **343 of those 425 had no
points change at all** and only 15 sit inside `vor_rank` 150. Deep tie-break churn
among players on equal points, which any sixth opinion would reshuffle.

**The largest genuine mover is the source disagreeing on purpose.** Travis Hunter falls
65 ranks: ESPN 199.5, FantasyPros 193.8, BetOnline 199.4 — and The Athletic 132.9,
because the workbook's Jacksonville tab gives him **25 targets** behind Parker
Washington's 133. Our own usage model independently says **132.4**. Two sources built
from unrelated inputs now agree the consensus is high on a two-way player, which is
the specific thing a sixth vote is for.

---

## Why the raw stat lines and nothing else

The workbook carries far more than projections, and the rest is not usable.

- **`VORP` is contaminated.** `OVR & VORP Ranks!F2` computes
  `(QB_pts − replacement) + (RB_VORP_from_the_same_row × 0.45)` — 45% of the
  *row-aligned* running back's value added to each quarterback's. Josh Allen shows
  112.1: 34.3 earned, 77.8 borrowed from Jahmyr Gibbs for sharing a row number. The
  QB replacement rank also resolves to **2** in a one-QB league, so only the QB1 gets
  a non-trivial number.
- **`AUC$` is sound and we already do it better.** It is
  `max(0, VORP / Σ positive VORP) × budget × teams`, allocating exactly $2,400 across
  149 players — a legitimate VORP-share model, but it prices 149 of ~192 roster slots
  and reserves no $1 minimums. `app/draft_view.py allocate_dollars()` reserves
  `spots × min_bid` first and splits the remainder.
- **The `DST` tab cannot be scored.** `Settings` defines all seven points-allowed
  tiers; `DST!L:R` is null for all 32 teams. Its own defence values therefore omit
  that component silently. [Plan 30](30-dst-model.md)'s model is blended at 0.25 and
  integrates the ladder over a weekly distribution rather than evaluating it at the
  mean.
- **`Jake's Ranks` is a real signal and is not ingested.** A human overlay that
  deliberately disagrees with his own projections: **28 of 85 running backs move 5+
  spots**, while quarterbacks barely move — the sensible shape, since RB committees
  are where a projection is least trustworthy. A rank is not a stat line and has
  nowhere to go in a blend that works in stat space. Worth revisiting after the drafts
  as its own column, not as a source. **Done on 2026-09-02 — see *The hand ranking*
  below. Still not a source; three lower-case display columns.**

---

## The traps, and where they are handled

All three in `Scripts/load_athletic.py`, with the numbers as executable tests in
`tests/test_athletic_source.py`.

1. **Position stat bleed.** The team tabs are a team-budget × usage-share model
   (`PASS ATT = team_pass_attempts × player_pass_share`), and on the New Orleans tab
   some target share lands on a **quarterback**: Spencer Rattler carries 32.2 targets,
   23.7 receptions, 258.7 receiving yards and 2.38 receiving touchdowns. The
   workbook's own `QB` tab has no receiving columns so it never sees them and scores
   him 7.8; read straight he scores **59.8**. `POSITION_STATS` masks each position to
   what it can hold, and the importer names every row it masked rather than dropping
   them silently. Exactly one player affected in the 2026-08-31 workbook, and no
   skill-position player carries passing stats — but the share model is what produced
   it, so the next download can produce it elsewhere.
2. **Four name mismatches**, added to `NAME_ALIASES`: two nicknames
   (`Chig Okonkwo` → `Chigoziem Okonkwo`, `Hollywood Brown` → `Marquise Brown`) and
   two of the workbook's typos (`Dermarcus Robinson`, `Braxton Barrios`). A nickname
   is the more dangerous kind, because it looks correct in both files. 432 of 434
   players now resolve; the two that do not are undrafted receivers absent from the
   crosswalk entirely.
3. **It is a file somebody saved.** No API, no scraper, no nightly stage — so it goes
   stale because nobody downloaded a new one, which nothing in the repo would otherwise
   notice. Named in `Scripts.refresh_status.PROJECTION_SOURCES` for exactly that
   reason: an unwatched source carrying a sixth of every projection it covers is the
   failure [plan 36](36-sportsbook-scrapes.md) found the hard way, when both books sat
   thirteen days stale on a draft board while everything reported healthy.

It projects no fumbles, so `ATH_` has no `lostFumbles` column and renormalisation
handles the gap.

---

## Measured 2026-09-01, after shipping

The accuracy gate cannot run (see *What is owed*). These are the questions that
**can** be answered before a ball is snapped, and they are the ones plan 20's
retirement turned on.

### The measurement basis: projected over 100 points

**Every number below is computed on players projected over 100 points by ESPN or
FantasyPros**, and that gate is not cosmetic — it changes the conclusion.

Percentage disagreement divides by the projection, so a third-string quarterback
projected for 8 points and 13 points by two sources reads as a **47% disagreement**
about nothing anybody will draft. Pooling those in measures roster depth and calls it
independence. It is the same class of error plan 20 was retired for — that one
measured on a sample where every source agreed, this one on a sample where the
arithmetic is unstable — and it is worth more than it looks:

| | all players | **>100 pts** | shift |
|---|---|---|---|
| mean disagreement involving `ATH` | 26.3% | **18.9%** | −7.4 |
| mean among the pre-existing five | 16.2% | **14.1%** | −2.2 |

The gate costs `ATH` more than three times what it costs the others, and the reason is
coverage: `ATH` and FantasyPros both project deep benches, so they share 838 cells of
which most are irrelevant, and FP-vs-`ATH` falls **34.7% → 18.0%**. The two books do
not move **at all** (`+0.0`) — a book only prices players worth pricing, so its rows
were already gated.

The gate is built from ESPN and FantasyPros only, never from `ATH`, so it cannot be
accused of selecting the rows that flatter the new source. 242 of Knights_FFL's 1,036
rows clear it.

### It is a genuinely independent opinion — but not the most independent

Mean pairwise disagreement, `mean |a−b| / mean(a,b)`, on cells where **both** sources
are real and unimputed and the player clears 100 points:

| Pair | Disagreement |
|---|---|
| **ATH** vs USG | **22.2%** |
| BOL vs **ATH** | 19.5% |
| ESPN vs BOL | 19.4% |
| FP vs BOL | 18.7% |
| ESPN vs **ATH** | 18.7% |
| FP vs **ATH** | 18.0% |
| ESPN vs USG | 17.7% |
| FP vs USG | 15.7% |
| PINNY vs **ATH** | 16.3% |
| ESPN vs FP | **9.9%** |
| PINNY vs BOL | 5.9% |

`ATH` sits at 18–22% against everything, against **9.9%** for ESPN-vs-FantasyPros —
so it is roughly twice as far from the two highest-coverage sources as they are from
each other, and about as far from them as a sportsbook is. It is not a re-badged
consensus, which is the specific failure plan 20 feared from a sixth expert feed.

**On the unfiltered sample it looked like the most independent source in the blend at
26.3%. It is not** — `ATH`-vs-USG is the largest cell but ESPN-vs-USG is in the same
band, and the ranking is not stable once the backups come out. Recorded because the
unfiltered version was written into this file first and is wrong.

**Disagreement is not accuracy.** Being different is not being right; that is what the
MAE gate is for, and it cannot run yet.

### The disagreement is shaped like a useful one

`ATH` against the ESPN/FantasyPros mean, priced players:

| Band | Disagreement | n |
|---|---|---|
| inside ADP 50 | **9.6%** | 112 |
| ADP 50–150 | 15.4% | 210 |
| outside ADP 150 | 23.9% | 58 |

Unchanged by the 100-point gate in the first two bands, because a player priced inside
ADP 150 already clears it — which is a check on the gate as much as on the source.
The same shape plan 20 found for ESPN-vs-FP once the registration fence lifted: tight
where the board is checkable, widening with ADP.

By position, and this is where the gate matters most:

| Position | all players | **>100 pts** |
|---|---|---|
| QB | 31.4% | **9.8%** |
| TE | 28.8% | **14.5%** |
| RB | 26.3% | **18.0%** |
| WR | 31.8% | **20.0%** |

Ungated, every position looked equally chaotic at 26–32%. Gated, the picture inverts
into something readable: **The Athletic essentially agrees with consensus on startable
quarterbacks (9.8%) and disagrees most at receiver (20.0%) and back (18.0%)** — the
two positions where projections are least reliable and where a draft is actually won.
The ungated QB number was backup quarterbacks and nothing else.

### It moves the part of the board that decides picks

VOR rank change against the 06:00 build, priced players inside ADP 150:

| League | Priced ≤ ADP 150 | Moved 5+ | Moved 10+ |
|---|---|---|---|
| GOP_Degenerates | 165 | 70 (42%) | 50 |
| Knights_FFL | 147 | 63 (43%) | 32 |
| Winfield_Football | 147 | 52 (35%) | 24 |
| Weenieless_Wanderers | 139 | 59 (42%) | 32 |

**The largest single move is one coherent call, checked by hand.** Josh Jacobs falls
63 ranks (−20.6 points) and MarShawn Lloyd rises 29 (+13.5) because the workbook's
Green Bay tab gives **Lloyd 234.8 carries against Jacobs' 99.1** — the share model
conserves the backfield, so the two moves are the same opinion stated twice. Not a
parse artifact, and adjudicable on draft night. Worth noting that **TOMCAT is the most
bullish source on Jacobs** (207.6 against ESPN's 162.2 and `ATH`'s 86.8), so `ATH` is
not merely tracking our own model — the two agreed on Travis Hunter and disagree
sharply here.

---

## What is owed

**The accuracy gate cannot be run yet, and this is the same wall
[30](30-dst-model.md) hit at G-DST2(b) and [18](18-season-usage-model.md) at G2.** Two
independent reasons: 2026 has not been played, and **there is no historical Athletic
file** — the workbook is a 2026 download, so a walk-forward against 2025 is not merely
unrun but unbuildable, exactly as `docs/plans/25-results-backfill.md` describes for
the other sources.

- **The per-stat MAE measurement plan 20 asks for**, once 2026 is played. If it does
  not reduce blended per-stat MAE, the weight goes to 0.0 and the number is recorded
  here. Shipping first does not retire the criterion.
- **Score it on players projected over 100 points**, the same gate as *Measured*
  above, and report the ungated number beside it rather than instead of it. An MAE is
  an absolute error so it does not blow up on small projections the way a percentage
  does — but it does get *dominated* by them: a board is roughly three-quarters
  players nobody drafts, so an ungated mean is mostly a measurement of who is better
  at projecting fourth-string tight ends. Both numbers are informative; only one of
  them answers whether the source should carry a vote on draft day.
- ~~**It will not be scored by the existing harness, and that gap is now documented in
  code.**~~ **Half of this closed on 2026-09-09**, when The Athletic began publishing
  weekly slates and `ATH` joined `Scripts.usage.evalset.SOURCES` — see
  [plan 47](47-athletic-weekly.md). What the bullet said was true when written:

  > `Scripts.usage.evalset.SOURCES` drives `Scripts.lab.accuracy`,
  > `Scripts.usage.gates` and `Scripts.usage.g1_season`, all of which score
  > **player-week** rows out of `lineups.parquet`. `ATH` is season-only and has no
  > weekly line, so it has no column there — running `python -m Scripts.lab.accuracy`
  > in January and reading a clean table would look like The Athletic had been judged
  > when it had not.

  It has a weekly line now, so `Scripts.lab.accuracy` scores it and that table means
  what it reads. **The season half of the debt stands**: a weekly MAE answers whether
  the source is fit to blend *weekly*, and the question this plan owes is whether it
  earned a vote on **draft day**. That still has to come from `board.parquet`'s
  `ATH_<stat>` scored against realised season totals from `Scripts.usage.nflverse`,
  and that harness still does not exist. The cheap half became a command; the owed
  half did not move.
- **`Scripts/usage/g1_season.py` now describes a five-source world.**
  `_shipped_weight()` still correctly reads 1.0 — it divides TOMCAT's weight by one
  external's, and both are 0.25 — but `SOURCES` there does not name `ATH`, so the
  ratio's *meaning* changed even though its value did not: TOMCAT is one vote of six.
- ~~**`Jake's Ranks` as an expert-rank overlay**, if the drafts suggest it is
  wanted.~~ **Shipped 2026-09-02**, below. What it owes in turn is one question, and
  it is the cheaper half of the gate above: *does his hand rank beat the order his own
  projections imply?* Same 290 players, same 100-point gate, scored on rank
  correlation against realised finish. It needs the season harness this plan already
  owes and adds no second one.

---

## The hand ranking, shipped 2026-09-02

The last owed bullet, done. **Three lower-case display columns and nothing else** —
`TRUE_Points` and `vor_rank` are byte-identical with and without it, measured on
Knights_FFL's 1,036 rows before and after: **zero rows changed on either**. That is
what made it mergeable in the same week `docs/DRAFT_READINESS.md` closes the door on
the 0.25 merge above.

### It carries no projections, and that was checked rather than assumed

`Jake's Ranks` looks like a second opinion and is not one. Every stat cell on it is
`=VLOOKUP(<player>,QB!B:O,4,FALSE)` — the same team-tab projections already ingested
as `ATH_`. Checked against the built parquet: all 85 running backs match to the float.
The ordering is the only thing on that tab we did not already have.

### Three things this plan had wrong or did not know

1. **The source is the `Rankings` tab, not `Jake's Ranks`.** The latter is a rendering
   of the former — `=VLOOKUP(<rank>,Rankings!A:T,3,FALSE)`.
2. **There are three lists, not one.** Non-PPR, half and full, side by side, and the
   workbook says which is which rather than leaving it to inference: `Jake PPR` reads
   column 23 for its backs, `Jake Non` reads column 38. Our leagues split 0.5 and 1.0,
   so each board reads the list built for its own scoring — the same principle that
   scores every source through the league's rules instead of the workbook's.
3. **All 290 ranked names appear verbatim in the 434-row projections file**, so the
   join needed no new aliases. There are also three *overall* cross-position lists;
   they are **not** ingested, because they encode the workbook's own `Settings`
   (12 teams, 1QB, 2RB/3WR/1TE/1FLEX), which is not GOP's IDP league.

### The override has the shape that justifies showing it

His hand rank against the order his own projections imply, on his half-PPR list and
his own scoring:

| Position | n | moved 5+ | mean \|Δ\| | max |
|---|---|---|---|---|
| QB | 40 | **0** | 1.0 | 3 |
| TE | 45 | 4 | 2.1 | 11 |
| RB | 85 | 26 | 3.8 | 19 |
| WR | 120 | 55 | 4.9 | 25 |

**He leaves alone the position a projection handles best and reworks the two it
handles worst** — the same inversion the 100-point gate found for the stat lines
(QB 9.8%, WR 20.0%). On Knights_FFL's full-PPR board, scored in that league's rules,
it reproduces: QB 0, TE 6, RB 28, WR 50.

The largest overrides are a different set of players from the largest stat-line
disagreements above, which is the check that the column carries something `ATH_` does
not: Dontayvion Wicks (his projection's WR84, his hand WR59), Xavier Hutchinson
(WR94 → WR117), Adonai Mitchell, Jameson Williams. Travis Hunter and Josh Jacobs — the
flagship stat-line calls — barely move here. Those were projection arguments; these
are him distrusting the projection.

### A join miss this surfaced, not yet fixed

The rank join reported 289 of 290 matched on Knights_FFL. The miss is **Kenny
Gainwell**, whom the workbook spells *Kenneth* — and it is not a rank problem. His
`ATH_rushingYards_is_imputed` is `True` on the shipped board: **the stat line of a
165-point running back has been silently replaced by the ESPN/FantasyPros mean since
the 0.25 merge went in.** Exactly the failure `NAME_ALIASES` warns about, in its own
words: a nickname "looks correct in both files".

`_report_join_misses` did its job — it named him first in The Athletic's miss list.
He was camouflaged by the eleven kickers behind him, which The Athletic legitimately
does not project, so the one real name miss read like the noise around it. Worth
knowing about that diagnostic independently of this fix.

The fix is one line in `NAME_ALIASES`, `"KENNETH GAINWELL": "KENNY GAINWELL"`. It is
**not applied here**, because it moves `TRUE_Points` and this merge's whole claim is
that it does not. It is a draft-week judgement call of the same kind as the 0.25 merge
at the top of this file, and it should be made deliberately rather than as a side
effect of a display change.
