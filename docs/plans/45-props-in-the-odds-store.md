# 45 — Props into the odds store, and the migration that waits for a second producer

**Status:** IN PROGRESS

**Priority:** Medium · **Effort:** S now, M later · **Where it stands:** **Step 1 (dual
write) complete 2026-09-08**; step 2 (the consumer migration) deliberately deferred
behind a named trigger rather than a date. BetOnline's weekly props now land in
`Data/Odds/<season>/BetOnline/` beside the game lines — 7,470 prices, 597 two-way pairs
de-vigging to 1.000000000, and 659 lines already carrying two recorded states within
the first day. The blend still reads the flat file and is untouched
**Depends on:** [35](35-market-lines-and-vig.md) — the single de-vig ·
[36](36-sportsbook-scrapes.md) — `ODDS_SCHEMA` and the append-only store ·
[02](02-betonline-access.md) — the transport that made weekly props available again
**Blocks:** nothing. That is the point — step 2 is a choice, not a debt with interest

> **The schema question was asked and answered "not yet."** `Scripts/books/schema.py`
> says a prop and a game line are the same artifact and reserves `propType` for it, so
> the obvious move on restoring BetOnline weekly props was to emit them as
> `ODDS_SCHEMA` rows. Two measurements argued against doing it now, and a third showed
> the one real benefit could be had separately and cheaply. That split is this plan.

## Context

Weekly player props were restored 2026-09-08 (plan [02](02-betonline-access.md)) after
a month dead. They land in the flat `FULL_DF_SCHEMA` shape the blend has always read.
The open question was whether to migrate them onto `ODDS_SCHEMA` and
`BaseSportsbook`, the contract the game lines use.

## Why the migration is not worth doing yet

**1. The main benefit has already been paid, at a different layer.** The expected prize
was de-vig via `Scripts.market` by construction. It is already there: `scrape_BOL`
calls `mk.devig_two_way`, `mk.devig_survival`, `mk.measure_overround`,
`mk.ladder_median` and `mk.count_moments` directly. Plan 35 unified de-vig at the
*math* layer, not the schema layer, so migrating buys nothing on the thing that
mattered most. This is worth stating plainly because "get de-vig for free" is the
argument that will be re-made next time someone reads `schema.py`'s docstring.

**2. Migrating one of two producers increases divergence.** There are already two
weekly-prop shapes. BetOnline's uses `espn_stat` / `prop_source` / decimal `odds`;
Pinnacle's (`Scripts/scrape_pinnacle.py`) uses `PropType` / `Player` / `Title` /
`Period`. Moving BetOnline to `ODDS_SCHEMA` makes three shapes, and leaves
`get_x_stat` and `Scripts/lab/market.py` straddling all of them. Unification that
unifies nothing is churn.

**3. The consumers are real work.** `projection_utils.clean_bol`,
`scrape_BOL.get_x_stat` (which re-reads the landing parquet off disk and splits on
`prop_source`), `reconcile_BOL`, `archive_raw` and `Scripts/lab/market.py` all expect
the flat shape.

## What was worth doing: the dual write — DONE

The one benefit migration genuinely offered is **line history**. The flat landing file
is overwritten every run, so prop movement was not recorded anywhere. Meanwhile
`Scripts/books/store.py` already diffs each pull and appends only what moved.

That is separable from the consumer migration, so it was taken separately.
`Scripts/books/props.py` converts the scraper's raw frame to `ODDS_SCHEMA` rows and
`scrape_BOL._store_line_history` appends them next to the game lines. Nothing
downstream of the blend changed.

Measured on week 1, 2026:

| | |
|---|---|
| prop rows stored | 7,470, beside 340 BetOnline game lines |
| two-way pairs | 597, `fairProb` summing to 1.000000000 |
| ladder rungs | 6,276, keeping `impProb` as `fairProb` — nothing to de-vig against |
| re-running an unchanged scrape | 0 appended, 7,470 unchanged |
| one price moved | 1 appended, `current.parquet` reflecting the new price |

Three decisions inside it, recorded because they are choices:

- **`marketTitle = "PlayerProp"`, one value rather than one per stat.** The stat lives
  in `propType`. Keeping market families short is what lets `Scripts/vegas.py` keep
  filtering game lines with an equality test — verified: `book_team_totals` still
  returns its 32 rows with props in the store.
- **A ladder rung is `isAlt=True`.** `marketsBySs` prices "2 or more touchdowns" as its
  own one-sided market. Only two-way rows pair, so only they get a de-vigged
  `fairProb`.
- **`propType` carries the repo's stat name** (`passingYards`), not the book's market
  title, so a query spans books the moment a second one arrives.

Props and game lines share a book directory on purpose. `store._key_expr`'s docstring
already anticipated it: *"`sideOf` is null on every game market and `propType` on every
game line."* `tests/test_books_props.py` asserts the two stay distinct and that neither
shadows the other.

**One hazard, named rather than fixed.** `write_snapshot` appends to
`Data/Odds/<season>/<book>/<date>.parquet`, and two processes now write there:
`run_daily_refresh.sh` at 06:00 (props) and `run_odds_refresh_nfl.sh` at 07:00 (game
lines). They do not overlap today — the nightly takes ~9 minutes — but they are not
locked against each other, and a concurrent write would be last-writer-wins on a
partition. If the nightly ever grows past an hour, that is the thing that breaks.

## Step 2 — the migration, DEFERRED

**The trigger is a second live prop producer, and Pinnacle weekly is the candidate.**
Plan [36](36-sportsbook-scrapes.md) step 3 recorded Pinnacle weekly props as returning
zero rows across all sixteen week-1 games. On 2026-09-08 the nightly logged `Pinnacle
weekly props: ok` and the blend showed real PINNY coverage (9.9% passing yards, 41.7%
receiving yards). Whether that is a fixed Selenium path or a one-off has not been
established — establishing it is the first task of step 2, not an assumption of it.

When there are two live producers, migrate **both together**:

1. Confirm Pinnacle weekly is reliably live, not a single good night.
2. Emit both books' props as `ODDS_SCHEMA` rows through `BaseSportsbook`, so
   `assert_coverage` and `assert_devigged` apply to props as they do to game lines.
3. Port `get_x_stat` to read the standard shape — `betSide`/`isAlt` in place of
   `prop_source`, `propType` in place of `espn_stat`. This is the bulk of the work.
4. Point `Scripts/lab/market.py` at the same.
5. Retire the flat `FULL_DF_SCHEMA` path and the dual write, which by then is
   writing the only copy rather than a second one.

Do **not** migrate on the strength of `schema.py`'s docstring alone. It is right about
the destination and silent about the timing, and the timing is the whole question.

## Verification

```bash
python -m Scripts.scrape_BOL --season 2026 --week <w>   # dual write reports its counts
python -m pytest tests/test_books_props.py              # 21 tests, no network
```

```python
from Scripts.books.store import line_history
line_history(2026, "BetOnline", market="PlayerProp")    # every stored state, oldest first
line_history(2026, "BetOnline", market="Total")         # game lines, unaffected
```

History only accrues across runs, so the check that matters is the second night: a
re-run with nothing moved must report `0 appended`, and a repriced line must add one
row rather than replacing one.
