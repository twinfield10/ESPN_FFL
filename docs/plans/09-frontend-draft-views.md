# 09 — Local frontend: draft views

**Priority:** High (seasonal) · **Effort:** Large · **Status:** **Board done
2026-08-07, model columns added 2026-08-14, split into three tabs 2026-08-14**;
Live not started; History unblocked
**Depends on:** [07 (foundation)](07-frontend-foundation.md), and the draft
phases in [`../STATE_OF_THE_REPO.md`](../STATE_OF_THE_REPO.md#roadmap--draft-strategy)

## Status

**Page 1 of 3 is built** — `app/pages/draft_board.py` over `app/draft_view.py`,
registered in `app/main.py`. It renders for all nine leagues with no exception, and
Josh Allen comes out VOR rank **9** in the 10-team superflex against **21** in
14-team Knights, which is the league-awareness this plan exists for.

Building it turned up three defects in the artifact underneath, all now fixed —
see the postscript. The short version: **every per-source point column on every
stored board was NaN for every row**, and `projection_missing` was False for all
1,026 including the 503 players projected a literal 0.0.

What is still open:

- **Live Draft** (§2) — not started. It is the one page that talks to ESPN in the
  render path, and the plan's own build order puts it last because it is the most
  likely to slip.
- **Draft History** (§3) — **no longer blocked, as of plan 23.** This said
  `draft.parquet` was unwritten and roadmap Phase 1 had not backfilled it; both are
  now false. `Scripts.refresh --what draft` writes it, the artifact holds 826 picks
  for Knights 2026, and the board page already reads it for the owner-tendency cards.
  What is missing from it is *outcomes* — points-over-expectation per manager, which
  needs every past season scored in each league's own rules.
- **Floor/ceiling is half-built.** Source disagreement is in; prior-season variance
  is not, because it needs per-week 2025 actuals joined per player out of
  `Data/Store/2025/*/lineups.parquet` — a different data path from anything the
  board builder touches today. Note the model is deliberately **not** a fourth input
  to this interval: it projects an expected value where the other sources project a
  healthy season, and mixing the two widened the median interval from 8.5% to 24.0%.

## The three tabs, 2026-08-14

The page had become one scroll holding a filterable table, two charts, six manager
cards and a decade of acquisition history. That is three jobs stacked vertically,
and the one you do under time pressure — find a player, read his number, decide —
was the one you had to scroll past two charts to reach.

| Tab | What it is for | What is on it |
|---|---|---|
| **Board** (default) | the working surface | search, the four filters, the auction budget, the table, and the column caveats |
| **Values** | where the room is wrong | *Falling past their price*, with its own position filter and a depth slider |
| **League** | what does not change during a draft | toplines, the positional cliff, the tier runway, owner tendencies, acquisition history |

**Board now sorts by VOR, not by value.** Value-first was right when the page was
one surface — it is the thing a board can tell you that the room cannot, and the
argument for it is still in this plan's §1. It is wrong once *Falling past their
price* has a tab of its own: value against ADP is a second opinion about the room,
and the surface you actually draft from should answer "who is worth the most" first.
Value is still one radio click away and still the whole subject of the Values tab.

**Each tab carries its own position filter, deliberately.** The Board's is "what am
I looking at right now" and gets narrowed constantly; the League tab's is "which
curves am I comparing" and should not be dragged around by it. One shared control
would have meant a chart quietly reshaping itself because you typed a name into a
search box on another tab.

### Four filters, not one

| Filter | Semantics |
|---|---|
| **Search** | literal substring, case-insensitive |
| **Positions** | keep the ones named |
| **NFL Teams** | keep the ones named |
| **Bye Weeks** | keep the ones named, *including* dropping players with no recorded bye |

Every one is an **include** list: empty keeps everything, non-empty keeps only what
it names. That single rule is what makes four controls composable without a legend
explaining each, and it is what settles the bye filter's awkward case — "keep only
weeks 5 and 10" cannot honestly keep a player nobody knows the bye of.

The search is matched **literally**, which is a fix rather than a preference. It
compiled as a regex before, and the names on a board are full of regex syntax:
`T.J.` matched any three characters between two dots, `Amon-Ra St. Brown` the same,
and a name typed with an unclosed bracket raised out of the page instead of finding
nothing.

### The auction budget

The `$` column was denominated in somebody else's money. ESPN publishes
`auctionValueAverage` against **its own $200 auction** — the 2026 pool agrees: the
338 players it has priced sum to $1,871 of the $2,000 a ten-team $200 auction puts
on the table, with the rest spread across a bench it prices in pennies — and the
board stored those dollars as published.

`draft_view.at_budget` now carries two columns instead of one: `auction_share` is
the fraction of a team's budget the market puts on a player, and `auction_dollars`
is that share at the budget set on the Board tab. The share is what makes the
dollars portable; the dollars are what gets shown. Default **$250**.

It is a straight proportion, and the help text says so. A real auction's minimum bid
does not scale with the budget — the last roster spots cost $1 whatever you are
playing for — so raising the budget adds slightly more to the top of the board than
a flat multiple suggests. The distortion is small next to the disagreement between
any two sources' valuations, and correcting it needs a roster size the function is
not given.

Verified headless through `AppTest` across all four of the viewer's leagues: Puka
Nacua prices at $57.76 on ESPN's budget, $72.20 at 250 and $144.40 at 500.

## The model columns, added 2026-08-14

The page predated the usage model and showed none of it, which by 2026-08-14 meant a
third of `TRUE_Points` was invisible on the artifact built to explain `TRUE_Points`.
Four columns now sit between the market block and the status columns:

| | |
|---|---|
| `USG` | The model's own projection. The one number on the table not comparable to `Proj` — see below |
| `Δrk` | `TRUE_PosRank − USG_PosRank`. Positive where the model likes a player more than ESPN and FantasyPros do |
| `Exp G` | Games out of 17 the model expects the player available for, which `USG` is already scaled by |
| `Model evidence` | Why the model's evidence is thin, or why it produced nothing |

**The level mismatch is handled by showing ranks alongside the points, not by hiding
the points.** `USG` is an expected value over `Exp G` games; `Proj` assumes a healthy
17. Subtracting them means nothing, so the `USG` tooltip says so and the page's "what
is missing" panel says it again at length. `Δrk`, being a rank, is immune — the same
property that keeps the model out of `Floor`/`Ceil`.

**`Model evidence` exists because an empty `USG` meant three different things** and
all three rendered as the same blank cell, which reads as agreement:

| State | Rows, Knights 2026 |
|---|---|
| `not modelled` — K and D/ST, never modelled, plus anyone with no usage history | 316 |
| `withdrawn (availability)` — the model declined a player whose expected games were too low to price | 90 |
| `withdrawn (injury)` — the report withdrew a price the model had already made | 12 |
| the flag itself — `changed teams`, `thin prior season`, `low prior volume`, or a combination | 290 |
| `—`, priced with nothing flagged | 318 |

A withdrawal is reported ahead of the evidence text where a row carries both, which
7 rows do: that there is no number is more useful than why the absent number would
have been shaky. Sorting by `Δrk` is offered in both directions, because the
interesting disagreements are at both ends.

Verified headless through `AppTest` across all nine leagues, four of four columns
present on each. The board's default position filter shows only QB/RB/TE/WR, so
`not modelled` is mostly reached by widening that filter — which is pre-existing
behaviour, not something these columns changed.

## Goal

The draft-day surface. Three pages with different technical characters:

| Page | When | Character |
|---|---|---|
| **Draft Board** | pre-draft prep | static, sortable, exploratory |
| **Live Draft** | during the draft | polling, stateful, glanceable |
| **Draft History** | offseason | analytical |

Draft data pipelines are Phases 1-3 of the roadmap; this plan is the UI on top.
Board data comes from the store (`board.parquet`, `draft.parquet`), computed
during refresh — the board must not be calculated on page load.

## 1. Draft Board

The pre-draft artifact. One table, heavily filterable:

| Column | Source |
|---|---|
| Player, position, NFL team, bye | ESPN |
| Projected season points | **done** — `Scripts.season_projections.build_season_projections()` gives `TRUE_Points` and `TRUE_PosRank`, blended across ESPN/FantasyPros/BetOnline/Pinnacle season lines and scored per league. See [plan 12](12-season-projections.md). Defence in the IDP league is pending [plan 11](11-per-slot-scoring.md). |
| **VOR** | Phase 3, from this league's actual roster slots |
| **Tier** | Phase 3, 1-D clustering within position |
| ESPN ADP, auction value | Phase 3, `view=kona_player_info` |
| **Value** = ADP − your VOR rank | the target/avoid signal |
| Floor / ceiling | source disagreement + prior-season variance |
| Injury flag | ESPN |

All of these are on the page. Two carry caveats the page states rather than hides:

- **`Value` is blank for 829 of 1,026** — 84% of the pool the market has not priced,
  plus K and D/ST, where a season-total VOR does not describe a position you stream.
- **`Floor`/`Ceil` are measured for 163 of 1,026.** They are the range across the
  sources that *really* have a line, and a source imputed from the ESPN/FantasyPros
  mean does not count. Mean width is 11% of the projection. Fewer than two real
  sources means no spread rather than a spread of zero — otherwise the players
  nobody has priced would report the *narrowest* range, which is backwards.

Interactions that matter:

- **Sort by value, not by rank.** The board's job is surfacing where your
  projections disagree with the room. **Done** — it is the default sort, with VOR,
  projected points, ADP and auction value as alternatives.
- **Tier bands as the primary visual.** Tier breaks drive draft decisions far
  more than one-spot rank differences. ~~Colour by tier~~, and show where each tier
  runs out. **Done as a tier-runway chart** — available players per tier per
  position, so "three left in tier 2" is legible at a glance. **Not coloured by
  tier**, and that is deliberate: tier is an ordinal blue ramp, and eight tiers
  cannot be stepped down one hue with separable lightness. Squeezing eight steps
  into the range whose light end still clears the surface leaves adjacent lightness
  differences of 0.047, which fails the palette validator and the eye alike. Tier
  went on an axis and position kept the colour, which also means one colour means
  one thing across both charts on the page.
- Position filter, and a **roster-need** toggle that filters to slots you
  haven't filled. **Done.** Dedicated slots fill before flex ones, so a third
  running back fills the flex rather than displacing a starter. Pre-draft it is a
  no-op, since every slot is open — the toggle's help text says so.
- **Scarcity curve** — projected points by position rank, which makes the
  positional cliff visible. This is the single most useful chart on the page.
  **Done**, running to 1.6× replacement level with each position's replacement rank
  drawn as a dashed rule. Past that every curve is flat near zero and the cliff —
  the whole point — is squeezed into the left edge.

**League-awareness is the differentiator.** Replacement level comes from each
league's real starting slots, so the same player is legitimately ranked
differently across your nine leagues. The scan found real variety here — 6 to 16
teams, a superflex `OP` slot in Weenieless Wanderers, IDP `DP` in GOP
Degenerates, no D/ST in 12 Dudes. A generic board from any website cannot do
this, and it's the whole reason to build one.

Confirmed on the built page. Josh Allen, one player across nine boards:

| League | Teams | QB replacement | VOR | VOR rank |
|---|---|---|---|---|
| Weenieless Wanderers (superflex) | 10 | **QB20** | 102.2 | **9** |
| GOP Degenerates | 16 | QB16 | 87.3 | 20 |
| Knights FFL | 14 | QB14 | 78.7 | 21 |
| Winfield Football | 6 | QB6 | 51.3 | 11 |

The superflex `OP` slot pushes quarterback replacement to QB20 and Allen up twelve
places against Knights. The 6-team league is the other direction and shows why
team count alone does not explain it: replacement is QB6, so the gap to the next
quarterback is small and his VOR is the lowest of the nine even though he ranks
11th there.

## 2. Live Draft

The hard page. Requirements are different from everything else in the app: it
updates without interaction, holds state across reruns, and gets read at a
glance under time pressure.

### Polling

`st.fragment(run_every="5s")` re-runs only the draft-state fragment, leaving the
rest of the page alone:

```python
@st.fragment(run_every="5s")
def draft_state():
    picks = poll_draft(league)          # league.refresh_draft(), then league.draft
    ...
```

Note `refresh_draft(refresh__teams=True)` is broken upstream — `league.py:93`
references an undefined `data`, and the kwarg is misspelled with a double
underscore. Call it with defaults only.

This is the one place the app talks to ESPN in the render path, which is a
deliberate exception: the draft endpoint is small and fast, unlike the
17.6s `get_ply_stats_by_matchup`. Load the board from parquet at mount; only
poll picks.

### What it shows

- **Best available**, by VOR, filtered to your roster needs.
- **Tier breaks remaining** per position — "3 RBs left in tier 2" is the
  information that actually drives a pick.
- **Positional run detection** over the last N picks. A run on TEs is when you
  either jump or deliberately fade.
- **Value on the board** — players falling well past their ADP.
- **Your roster** against starting slots, with what's still unfilled.
- **Picks until your turn**, and who picks between now and then.

### Resilience

Draft day is the wrong time to debug. Cache the board to parquet at startup so a
crash resumes instantly. Degrade visibly rather than silently: if polling fails,
show a stale-data banner with the last successful poll time and keep rendering
the board. A manual "mark player drafted" fallback covers total API failure —
without it, one bad request ends your draft.

Dry-run it by replaying a completed 2025 draft pick by pick before trusting it.

## 3. Draft History

Roadmap Phase 1 output — 2016-2025 for the two leagues with real depth
(Winfield_Football, Weenieless_Wanderers).

- Points-over-expectation per manager per year: who actually drafts well.
- **Positional tendency by round** per manager. This is what feeds the Phase 4
  simulator's opponent models, and it's directly useful on draft day — knowing a
  leaguemate reliably reaches for a QB two rounds early changes when you take
  yours.
- Reach/steal distribution against that year's ADP.
- Empirical pick-value curve from your own leagues, replacing the missing
  `pick_value.csv` the dead `draft_utils.py` referenced.

**Caveat to surface in the UI:** the scan found unmapped scoring rules in
Winfield_Football 2016-2019 and Weenieless_Wanderers 2017-2019
([plan 01](01-scoring-coverage.md)). Historical `total_points` inherit that gap,
so points-over-expectation for those years is slightly off. Fix plan 01 before
trusting the early-year numbers, and note the caveat on the page until then.

## Build order

Board → History → Live. The board is the minimum viable draft-day artifact and
is useful the moment it exists. History is cheap once Phase 1 lands and it
informs draft strategy directly. Live is the most work and the most likely to
slip; if it does, the board on a second monitor still works.

## Verification

- ~~Replacement ranks hand-check for a standard 12-team league (RB ≈ RB30 with
  flex) and differ correctly for the superflex and IDP leagues.~~ **Done.** The
  three 12-team leagues come out **RB31**/WR29 with the flex. The superflex is
  QB20; the IDP league is LB15 with CB1, and the 6-team league RB17.
- ~~The same player ranks differently across leagues, for the right reason.~~
  **Done** — the Josh Allen table in §1.
- ~~Board renders for all nine leagues, including the one with no D/ST.~~ **Done**,
  driven headless through `streamlit.testing.v1.AppTest`, nine of nine with no
  exception. 12 Dudes one Cup's replacement list correctly has no D/ST entry, and
  its 32 team defences are flagged unstartable and hidden by default.
- Screenshotted in both themes and the rendered stroke colours read back out of the
  DOM: `#2a78d6/#eb6834/#1baf7a/#eda100` light, `#3987e5/#d95926/#199e70/#c98500`
  dark. Both palettes pass the colour-blind-separation and lightness-band checks;
  the light one's sub-3:1 slots are relieved by the direct labels and the table.
- Live page dry-run against a replayed 2025 draft keeps its available-player set
  in sync throughout. — **open, with Live**
- Kill the network mid-poll: stale banner appears, board still renders, manual
  mark-drafted still works. — **open, with Live**

## Postscript: three defects in the artifact, found by building the page

None of these were visible from the board builder's own output. Each surfaced from
asking the page a question the summary line never had to answer.

**1. Every per-source point column was NaN, on every board, for every row.**
`_apply_scoring` summed a prefix's scored stat columns straight through, so one NaN
cell made the total NaN. The weekly path never noticed because `clean_lineups`
imputes and 0-fills before scoring; the season path is sparse — a running back has
no `ESPN_passingYards`, and passing yards is a scored rule in all nine leagues. So
`ESPN_Points`, `FP_Points`, `PINNY_Points` and `BOL_Points` were NaN 1,026 of 1,026
in every league, and `season_projections.main` had been printing three empty columns.
A missing stat now scores 0 while a source with no scored cell at all stays NaN,
which is the distinction plan 03 cares about: a book with no line is not a book
projecting zero. Verified behaviour-preserving on the weekly path by recomputing
every prefix over all nine leagues' stored 2025 `lineups.parquet` — max absolute
difference **0.0**.

**2. `projection_missing` never fired.** It was `board[points_column].isna()`, and
the blend 0-fills, so it was False for all 1,026 rows in every league — including
**503 players whose projection is a literal 0.0**, two of whom the market has even
priced. It now means "no source produced a scored line", which only became
computable once (1) was fixed. `board_summary` had the same blind spot and had been
reporting "1026 projected"; it now says 523.

**3. A structural zero counted as a source opinion.** First cut of the floor/ceiling
spread let FantasyPros count as real for Cameron Dicker on the strength of twelve
non-imputed `0.0` cells — a kicker has no passing yards, and nobody imputed that or
asserted it. His floor and ceiling came back exactly equal to ESPN's total: a spread
of zero, reported as measured agreement. Zeros no longer count, and kickers dropped
out of the spread population entirely, which is the honest answer for a position no
source prices.

The pattern in all three is the same one this repo keeps meeting: **a 0.0 that means
"nothing here" is indistinguishable from a 0.0 that means "zero", and every summary
count built on `notna()` reads the first as the second.**
