# 37 — The Sheet: a DraftSheets-style board over our own engine

**Status:** COMPLETE

**Priority:** **High, date-driven** · **Effort:** M · **Where it stands:** **Done
(2026-08-28)** — 10 days before Knights_FFL drafts and 11 before GOP's auction
**Depends on:** [15 (draft board)](15-draft-board.md) — done ·
[09 (draft views)](09-frontend-draft-views.md) — board tab done
**Closes:** [09](09-frontend-draft-views.md) §2's manual half, and the auction
team-count question 09 deferred

## Goal

A board you can drive a draft off in ninety seconds: four position panels on one
screen, banded by tier, with a cross-off column and a live positional-scarcity read.

`DraftSheets_2026.xlsx` — the BeerSheets replacement — is a genuinely good draft-day
*interface* wearing a weak projection engine. This takes the interface and keeps the
engine we already have.

## Why not just use the Draft Board page

The Board page is 45 columns across eight spanner groups, and that is the right shape
for the hour *before* a draft: it is where you decide whether you believe the numbers,
and where the disagreement with ESPN and with the room lives. It is the wrong shape for
the pick clock. Those are two tables, not one table with a filter — so this is a second
page over the same `board.parquet`, not a mode on the first.

## What the workbook actually does

Read out of the file rather than off the Reddit post, and recorded because **in two
places the intent is not what the formulas do**.

| Piece | The workbook | Ours |
|---|---|---|
| Projections | 3 hand-entered stat lines per player (mean/high/low), scored through its `Scoring` tab | Four blended sources scored per league, plus `pts_p10/p50/p90` from a 5,000-season sim |
| Availability | every projection × `(16 − missed)/17`, off a **positional-rank prior** (`RISK` tab: QB1 loses 1.80 games *because he is QB1*) | `usg_expected_games`, per player — now applied by the `Availability` toggle |
| Market anchor | 4th term in the average: the projection of whoever sits at this player's market positional rank — a 25% shrink toward consensus | **Deliberately not adopted.** See below |
| Baseline rank | `starters × teams × (1.17–1.30 + bench multiplier)`, flex split by 10-year PPG constants (RB .25 / WR .65 / TE .10) off its `FLEX EST` tab | `replacement_ranks()` — exact on dedicated slots, flex split by *this league's own projected points*. Handles superflex `OP` and IDP `DP`, which the workbook cannot express |
| VBD | `Zscore Projection − baseline points` | `vor`, the same quantity |
| Auction $ | `$/VOR = (teams×budget − spots×$1) / Σ positive VBD`; price = `VBD × rate + 1`, floored at $1 | `our_dollars` — the same construction, Σ restricted to the top `spots` players |
| Tier | 5 equal-width bands across `0 … 2nd-largest VBD` | 1-D KMeans, so the breaks land where the gaps are |
| **PS** | `Σ(VBD of undrafted players below him) / Σ(all positive positional VBD)` — decays live as you cross players off | **This was the gap.** Built here |
| Layout | 4 panels; grey band on **even tiers**; black fill on drafted; colour scale on `VALUE` and `PS` | Built here |

### The two bugs, and why this is a reimplementation

- **QB.** `Aggregate!I3 = AVERAGE(F3,G3,H3,BP3)` — and column `BP` belongs to the **TE
  block**. Josh Allen's projection is averaged with `66.19` (the top tight end's VBD),
  Caleb Williams' with `27.64`. Confirmed against the workbook's own cached values:
  Allen's LOW/AVG/HIGH of 295.7 / 311.0 / 331.6 average to **251.1**, not ~312. It
  partly self-cancels because the QB14 baseline is dragged down too — but the drag is
  *stepped by TE tier*, so it systematically inflates the top two quarterbacks against
  QB5 and below.
- **RB.** The market-anchor rank (`BR`) reads `BQ`, the **TE tier counter**, rather than
  the back's own ADP positional rank. WR (`BS`) and TE (`BU`) do it correctly.

The `BF:BQ` tier-walk block is otherwise vestigial — the live `Tier` column uses the
5-band CEILING formula instead — which is how the drift went unnoticed. Two of four
positions are wrong in a spreadsheet thousands of people draft off.

### The market anchor is the one piece deliberately rejected

Blending 25% of a market-rank-implied projection into every player would flow straight
into `value` and `cash_delta` and quietly erase the disagreement those columns exist to
measure. The board's entire argument is that it is *not* the market; anchoring to the
market and then reporting surprise at the market is circular. Not adopted, and this is
the note saying it was a choice.

## What was built

### 1. One dollar allocation, shared by both sides of the cash lens

`draft_view.allocate_dollars()` — extracted from `with_cash_value` — is now the single
implementation of `min_bid + (teams×budget − spots×min_bid) × wᵢ / Σw(top spots)`.
`with_cash_value` passes `vor`; `at_budget` passes `auction_value_filled`.

**This fixed a real bug that had a sign.** `at_budget` scaled ESPN's `$200` values by
`budget/200` and never saw team count, so the market total landed wrong in both
directions — and `cash_delta` had been differencing our correctly-pooled dollars
against it:

| League | Money on the table | Market total, before | After |
|---|---|---|---|
| GOP Degenerates, 16 × $250 | $4,000 | $2,702 | **$3,866** |
| Knights_FFL, 14 × $200 | $2,800 | $2,083 | **$2,698** |
| Winfield_Football, 6 × $200 | $1,200 | $2,083 | **$1,142** |

Both sides now total identically, so the difference means something. `our_dollars` is
unchanged to 1e-6 — the market was the side that was wrong. The residual gap to
`teams × budget` is `(spots − |pool|) × $1`, which is correct rather than a rounding
error: a spot the pool was too small to fill still costs its dollar.

Three parameters answer three different questions, and conflating them was the first
attempt's bug: `weight` is what each player is worth, `pool` is who sets the rate, and
`price_outside_pool` is who gets a number printed. The valuation side wants all three
aligned — pricing the 121st-best player in a 120-spot league invents money. The market
side wants them apart: ESPN prices ~313 players in a 240-spot league, and what the room
pays for a defence is worth showing even though a streamed position has no business
setting the rate.

`at_budget(board, budget, meta=None)` without `meta` falls back to the proportional
rescale **and warns**, since it cannot see team count. It no longer blames missing
`meta` for a board ESPN simply never priced — that would be a lie in the one place the
reader looks to find out what degraded.

### 2. Positional scarcity

`draft_view.positional_scarcity(board, drafted)` adds `ps`. Per position: a **fixed**
denominator of all positive `vor`, over a numerator of the still-available `vor`
strictly below this player. The fixed denominator is what makes it decay, and it is the
workbook's choice rather than an oversight — normalising by the value *remaining* would
be scale-free and would never fall, which reads better and says less.

Measured on Knights: RB1's `PS` runs **90.3% → 25.3%** as the twelve backs below him go,
converging on RB13's, because everything between them is gone. Crossing off a player
moves the rows *above* him and provably not the rows below. Streamed positions come back
null, for the same reason `value` is NaN for them in `board.py`.

Keyed on `player_id`, not name — `15`'s postscript found 16 colliding names in the IDP
pool, and crossing off Lamar Jackson the quarterback must not take Lamar Jackson the
cornerback.

### 3. The availability lens

`draft_view.with_availability_points()` adds `avail_points = TRUE_Points ×
usg_expected_games / 17`, plus `avail_pos_rank` and `avail_evidence`.

**It does not double-count, and settling that corrected a doc.**
`docs/DRAFT_READINESS.md` said "`USG_Points` is injury-adjusted and `TRUE_Points` is
not". That is backwards: `Scripts.usage.project.to_full_slate` divides each player's
expected games back out precisely so the availability term can be "applied deliberately
and to the *whole* blend rather than to one quarter of it" — its own words. Both columns
are if-healthy 17-game lines. The readiness doc is fixed.

**Off by default, and that is the finding rather than caution.** The availability head is
the weakest arm of the model that produces it — [18](18-season-usage-model.md) measures
prior-season games against next season at r = **+0.343**. The discount is real money
(Puka Nacua 339.4 → 274.8, Jahmyr Gibbs 342.9 → 296.8 on the 2026 Knights board) and it
reorders within position: Christian McCaffrey passes both on `avail_pos_rank`. Worth
looking at every time; not worth silently repricing four leagues ten days out.

The ~1 player in 7 with no estimate (80 of 590 priced-and-projected on Knights) takes a
factor of 1.0 and is named in `avail_evidence`. Sinking them would be a filter disguised
as a projection; passing them through silently would read as durability.

### 4. The page

`app/pages/draft_sheet.py` over `app/sheet_view.py`, registered in `app/main.py` as
**The Sheet**. Seven columns per panel — `Tier · Player · TM/BYE · PTS · VALUE · PS ·
ADP` — plus the cross-off mark.

- `VALUE` is `our_dollars` in an auction and `value` in a snake, on `is_auction(meta)`,
  which is the same switch the workbook's `Scoring` tab makes.
- Panels run to **twice replacement**, clamped to 12–80 rows. The workbook prints QB40 /
  RB80 / WR80 / TE37 against baselines of 14 / 35 / 42 / 17 — one rule, not four numbers.
- Ordered by `vor`, not by the points column, even with the availability toggle on: VOR
  is what makes a quarterback comparable to a back, and it is the order `PS` is defined
  against.
- Crossed-off players **stay in place**, dimmed rather than blacked out. Watching the
  board empty where the players were is most of what a paper sheet is for, and a drafted
  player is still information — who went, and at what.
- Bands follow the **tier**, not the row — `ISEVEN($B5)`, kept exactly, because it makes
  the bands the cliff at no cost.
- Two prices, one input: the budget is read from the Board page's per-league
  `budget_key` rather than given a second widget, so the two pages cannot drift.

**No ESPN client in the render path.** `app/main.py` states the app is read-only by
construction and this keeps it: crossing a player off is a click, exactly as the
workbook has you type an `x`. That is [09](09-frontend-draft-views.md) §2's own named
fallback, built first because it is the half that cannot break on the night.

## Verification

- **1,705 tests pass**, 49 of them new in `tests/test_draft_sheet.py`. No network, no
  store on disk.
- Rendered headless through `AppTest` on all four of the viewer's leagues. League
  awareness shows through on the panels themselves: **Weenieless Wanderers' superflex
  carries 19 quarterbacks above replacement against Knights' 13**, and its TE panel has
  9 where Knights has 13.
- Every control driven: the availability toggle (Gibbs 375 → 325 in GOP's scoring), the
  depth slider (2.0 → 3.0 takes WR to the 80-row cap), the K/D·ST toggle (4 panels → 6),
  the search box (narrows the panels and provably **not** the counts above them), a
  cross-off click, and Clear.
- `our_dollars` unchanged across GOP, Knights and Winfield after the refactor;
  market totals corrected as tabulated above.
- Both draft types render: `is_auction` false → `VALUE` is a signed rank difference;
  true → dollars.

## Still open

- **ESPN draft polling** — [09](09-frontend-draft-views.md) §2's `st.fragment(run_every=
  "5s")`. Deliberately unbuilt: it is the half most likely to break under time pressure,
  and it would put an ESPN client in a render path the app promises is a parquet read.
- **A Google Sheets mirror** for phone access. The gspread + `batch_update` conditional-
  formatting machinery in `populateGoogleSheet.py` is what it would reuse; the blocker is
  that cross-off only drives the math where something reruns.
- **`our_dollars` has no cap at one team's budget.** Carried over from
  [09](09-frontend-draft-views.md): it never binds on real data — the top of GOP's $250
  board is $123 — but the construction permits it.
