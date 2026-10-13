# 52 — Four more weekly prop books, pooled into one market source

**Status:** IN PROGRESS

**Priority:** High · **Effort:** M · **Where it stands:** **Shipped to the weekly blend
2026-10-13 (week 6).** Owner decision on the weighting; the backtest is owed once archived
prices meet realised stat lines.

## Problem

The weekly blend's only market inputs were BetOnline and Pinnacle, and on a Tuesday —
when waiver claims are made — they cover almost nothing. Measured 2026-10-13 ~15:30 ET,
of 139 skill players ESPN projects at ≥5 points in week 6: BetOnline had a yardage or
reception line for **10**, Pinnacle for **27**. (BetOnline's anytime-touchdown market
covers 117, so touchdowns were never the gap.)

## What was added

Four adapters ported from `NHL-Data/src/nhl/sources`, all producing one raw-price row
shape and all going through one derivation (`Scripts/books/weekly_props.py`):

| Book | Access | Notes |
|---|---|---|
| FanDuel | public JSON | two-way lines, alt ladders, anytime/2+/3+ TD |
| DraftKings | via ESPN `propBets` | **lines only** — ESPN's football feed carries no prices and DraftKings' own API is Akamai-blocked. Two-way lines are read at even money; ladders, TDs, pass TDs and INTs are skipped because their information is in the price |
| Novig | public, or signed websocket | every rung is a two-way exchange market; taker price at $50 |
| 4Casters | login (`CAST4_*`) | `NFL-PROPS` order book; VWAP at $50 plus 1% commission; thinnest by far |

BetOnline's `get_x_stat` now calls the shared `project_stat` — the arithmetic was lifted
unchanged, verified byte-identical on the week-6 landing file (322 rows × 34 columns).

## Evidence (week-6 Tuesday snapshot, `python -m Scripts.books.compare_props --week 6`)

* **Coverage roughly doubles on stat lines:** 27 → 56 of 139 (receiving yards 19 → 37,
  rushing 9 → 19, passing 6 → 13). Touchdowns unchanged at 117. Every book had posted
  the same ~7 games; the other 7 fill in later in the week.
* **The books are one opinion.** Where three or more price a player, FanDuel, Novig,
  Pinnacle and BetOnline sit a median 0.0–0.3 receiving yards from the books' median.
  ESPN sits +3.0 receiving, +2.9 rushing and +10.4 passing yards above it (partly a
  mean-versus-median difference — `Scripts/market.py` item 4).
* 35 free-agent rows across the owner's four leagues gained a stat line they had none of.

## Decision

Six books as six equal votes would count one opinion six times against ESPN's one. So:

* `MKT_` = per-stat **median** of every book with a *real* (non-imputed) line —
  `build_market_consensus`. Books stay on the board as `FD_`/`DK_`/`NV_`/`C4_`/`BOL_`/
  `PINNY_` columns but carry no weight of their own on the weekly path.
* `WEEKLY_WEIGHTS`: **MKT 0.50 fixed**, then ESPN 0.10 / FP 0.20 / ATH 0.20 renormalised
  over whichever are real. Fixed means the market is exactly half of `TRUE_` wherever it
  is real, rather than drifting to two-thirds when FantasyPros is missing.
* Weekly only. `WEIGHTS` (shared with the season board) is unchanged.

Measured effect on Winfield Football week 6, old code against new on the same inputs:
125 of 242 rows move, median 0.25 points, largest 1.56; kickers unmoved.

## Revised the same day: sharp and square, and touchdown margins

**Two pools instead of one.** FanDuel and Novig are the sharp prop books (owner's call —
Pinnacle is the classic sharp book but has not been for props in a year or two), and on
the snapshot they sat closest to the consensus. So `SHARP_` = median of FD/NV, held at a
**fixed 30%**, and `SQUARE_` = median of BOL/PINNY/DK/C4, held at a **fixed 20%**; ESPN 10 /
FP 20 / ATH 20 renormalise into the rest. Each pool abstains where none of its books
prices the stat, so a player priced only by BetOnline — most anytime touchdowns — is 20%
market, not 50%. Measured before the change: using the sharp books' median wherever
either has a line moved the pooled number by a median 0.23 rec yds (max 1.6) and 0.01 TD.
The app's weekly tables, sidebar and the Sheets show `SHARP`/`SQUARE` where they showed
`PINNY`/`BOL`; the draft board is the season path and keeps the individual books.

**Each book's anytime-touchdown margin is now its own.** A touchdown ladder is one-sided
with no two-way market to measure a hold from, so it borrowed the book's general props
margin. Against Novig's fair price (the exchange's two taker prices at the 0.5 strike,
de-vigged): **BetOnline 1.1375** (n=42), not the 1.064 it was de-vigged by — its touchdown
projections were ~7% high, the lean visible on every player compared; **FanDuel 1.0242**
(n=41), not 1.065 — ~4% low. Stored as `Scripts.market.TD_OVERROUND`; after the fix BOL's
median deviation from the book consensus on touchdowns went 0.044 → 0.003. Re-measure with
`python -m Scripts.books.compare_props --td-margin`.

## Owed

* **A backtest.** Every pull's raw prices are archived
  (`Data/Projections/<Book>/Season/<season>/Raw/`), so after a few weeks the plan-20
  comparison of `MKT_` against realised lines — and of the 50% share — becomes possible.
* DraftKings' prices, if a browser session (as BetOnline's widget uses) is worth it.
* The odds-store dual write BetOnline does (`props_to_odds_rows`) for line history.
