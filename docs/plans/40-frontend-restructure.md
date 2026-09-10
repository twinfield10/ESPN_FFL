# 40 — One league picker, five tabs

**Status:** IN PROGRESS

**Priority:** High · **Effort:** Large · **Where it stands:** **Shell and four tabs
built 2026-09-07**, the afternoon Knights drafted; **Home added and the chrome
reworked 2026-09-09**. Draft carries six sub-tabs including the post-draft Rundown;
Home is the landing page and carries standings. Every tab renders for all ten leagues.
What is owed is listed under [What is left](#what-is-left) — mostly plan 08's remaining
views, which now have a place to go rather than a page each.
**Depends on:** [07 (foundation)](07-frontend-foundation.md) ·
[08 (weekly views)](08-frontend-weekly-views.md) ·
[09 (draft views)](09-frontend-draft-views.md) ·
**Feeds:** [41 (projection freeze)](41-projection-freeze.md) ·
[42 (weekly matchup odds)](42-weekly-matchup-odds.md)

---

## Problem

The app was three sidebar-nav pages — Store Overview, Draft Board, The Sheet — with
Season, League and Week selectors in the sidebar. That was the right shape while the
only job was drafting. Two things made it wrong:

**It had no surface for the decisions the season is actually made of.** Who to start,
who to add, whether you win on Sunday. All of that lived in a notebook cell or in a
Google Sheets tab, and plan 08 had been the "to do" that would fix it since the
foundation shipped.

**The control that governs everything sat beside it rather than above it.** A league
picker in the sidebar is a picker you have to go and find, on the opposite side of the
screen from the thing it changes. Worse, it was structurally exposed to a Streamlit
behaviour that had already cost two silent wrong-league renders: *widget state is
discarded when you navigate to a page that has not yet rendered that widget.*
`components/header.sticky_selectbox` carries the full account of both bugs.

## The shape

```
┌───────────────────┬──────────────────────────────────────────┐
│ Fantasy Football  │  Home │ Roster │ Matchup │ Free Agents │ Draft
│ Signed in as …    ├──────────────────────────────────────────┤
│                   │                                          │
│ League [Knights ▾]│  (tab body)                              │
│ Week   [1       ▾]│                                          │
│                   │                                          │
│ Store built 5.7 h │                                          │
│  ago, refreshed   │                                          │
│  nightly at 6am   │                                          │
│ [Refresh League]  │                                          │
│ source coverage   │                                          │
│ unbuilt leagues   │                                          │
└───────────────────┴──────────────────────────────────────────┘
      ↑ header.render_identity() → session.render_context() → header.render_sidebar_health()
        all three called from main.py, in that order, before st.navigation
```

Five tabs. **Home leads and is the default**, because the question you actually arrive
with is "which of my five teams needs me before kickoff". The other four then run in
the order you work a single week: set the lineup, read the fixture, work the wire — and
Draft last, because for all but one weekend of the year it is history. **No icons**: the
label already says what the tab is, and a row of five emoji reads as five different
kinds of thing rather than five peers.

**The selectors moved into the sidebar** on 2026-09-09, under the identity block. The
original shape put them in the body, above the tab body, on the argument that a control
governing every tab should sit above the thing it governs. In practice it made the
page's own title the second thing on it, and it cost Home — whose cards are sixteen
columns wide — a row it needed. What was load-bearing was never *where they land*, it
was **what draws them**: `main.py` still calls `session.render_context()`, so the
invariant below is untouched.

**Drawing the selectors in the entrypoint removes that bug class rather than defending
against it.** Streamlit executes the entrypoint on every rerun — that is what makes it
a router — so there is no longer any page that has not rendered the league selector.
The unconditional-write pattern is kept anyway; it costs one line and its test is the
record of the two bugs.

`position="top"` is available in the pinned `streamlit==1.61.1`. Widgets cannot be
placed inside the nav chrome, so League and Week are the first row of the body. One
control row above every tab, which was the point.

## Season is pinned, not selected

`session.current_season()`. Every tab here answers a question about the season in
progress and none of them mean anything for a finished one, so a season picker is a
control nobody moves — and a control nobody moves is one that eventually gets moved by
accident.

Resolved from `Data/NFL_Schedules.csv` rather than hardcoded, so the annual rollover
stays one R script (`Rscript R/GetNFL.R 2027`) instead of an edit in the app. That file
is already the declared source of truth for both season and week
([SEASON_ROLLOVER](../SEASON_ROLLOVER.md) step 3), so the app cannot disagree with the
pipeline about what year it is. Falls back to what is actually in the store, which is
also the offline case: the CSV lives under `Data/`, which a machine reading the store
from S3 need not have.

Views that need history read it as an explicit lookback instead — the weekly dispersion
fit reads 2025 by name, because it is fitting on last year's residuals rather than
showing you last year.

## Two defects the restructure found

**The Week dropdown offered `[1]` and always would have.** `header.py` read
`meta.get("weeks_present") or [current_week]`, and **`weeks_present` is absent from
every 2026 `meta.json` on disk** — so the `or` branch always fired and the control was
decorative. `session.available_weeks` falls back to the distinct `week` values in
`lineups.parquet`, which is a cached parquet read costing about 11ms. The fallback is
load-bearing, not defensive.

**`meta["starting_slots"]` is not the league being played.** It is written when a
*board* is built; the lineups it would be applied to are written by a different
command on a different day. GOP Degenerates' Sep-7 metadata says its only defensive
slot is `DP: 1` while its Aug-14 lineups hold players in `CB`, `DE`, `DT`, `LB` and
`S`. `lineup.slot_counts` infers the slots from what is actually being started — the
widest any one team filled, unioned with the metadata as a floor — so the optimiser is
always solving the league that is actually being played.

## The five tabs

### Home — which of five teams needs you

The landing page, and the only tab that is not about the selected league. Roster, Free
Agents and Matchup each answer deeply about *one* league; finding the league with a
ruled-out starter in it meant opening all five in turn. Home is that sweep, done once.

One card per league, **ordered worst first**, so the page reads in the order you should
act on it. Each card carries the record and rank, the fixture with both projections and
the win probability, and up to two callouts:

| Callout | Severity | When |
| --- | --- | --- |
| *n starters cannot play* | critical | someone ruled out or on bye is in the set lineup |
| *+n points from n lineup changes* | warning | `lineup.swaps` has something, and everyone can play |
| *n starting slots beaten by the wire* | critical | `lineup.UPGRADE_CRITICAL` |
| *n bench slots beaten by the wire* | warning | `lineup.UPGRADE_DEPTH` only |

A start/sit worth points is a decision; a ruled-out starter is an error. Keeping them
apart is what stops five cards shouting every week — most weeks produce the first and
almost none produce the second.

Every card carries all three tab links in the same place, and the one an action points
at is painted **primary**. Pressing it sets the League and Week selectors *and* opens
the tab, so you arrive where the card was talking about. Only a critical action earns
the primary paint: three highlighted buttons is a page with no emphasis on it.

**Nothing here is re-derived.** `home.summarise` calls `lineup.swaps`,
`lineup.upgrades`, `matchup_sim.side` and `matchup_sim.outcome` — the same functions
the deep tabs call. A landing page that disagreed with the page it sends you to would
be worse than no landing page. `app/home.py` holds the decisions and is Streamlit-free
and tested (`tests/test_home.py`, 31 cases); `views/home_tab.py` is layout and owns the
store reads, so a summary can be cached on the same `store.version` fingerprint the
artifact readers use.

**Standings, and the trap in them.** One table per league, as tabs, in the viewer's own
league order — the cards above reorder by urgency, but a tab bar is a *control*, and
this app has paid twice for a control that moved under the pointer.

The record counts **weeks that were actually played**, recomputed rather than read off
`season_wins`/`season_losses`/`season_ties`. ESPN reports an unplayed fixture as a 0-0
result, `scrape_team_stats` faithfully records it as a **tie**, and the cumulative
column faithfully accumulates it — so on 2026-09-09, with no game played, every team in
all ten leagues read **0-0-1**. `box_score_available` is `true` on those rows and is no
help. The test is that *nobody* scored: a real fantasy team cannot score nothing in a
game that happened, because ESPN will not accept an empty lineup.

Ranked on win percentage, then points for, then this week's **projection** — which
breaks a tie in the first two, and before week 1 is the only thing separating anybody.
That is the difference between landing on a real table in September and landing on six
rows of zeroes in alphabetical order. `This Week` (scored) and `Projected` sit side by
side because a week in progress is described honestly by neither alone.

A league with no `team_stats` — Jeff's, in 2026 — keeps its card and loses the fixture,
the probability and the table, with a caption saying which and why.

### Draft — six sub-tabs over one board read

`Board · Sheet · Values · League · Calibration · Rundown`

Board and Sheet lead, because they are the two surfaces you drive a draft from.

**This was a re-parenting, not a rewrite,** and deliberately so: it happened the
afternoon Knights drafted, with two more drafts the next night. The four analytic tab
bodies moved out of `pages/draft_board.py` into `views/draft_tabs.py` as functions
whose first lines name the pieces of `BoardContext` they use; everything after that
preamble is the code that was already there. The Sheet moved the same way.
**`app/draft_view.py` (3,281 lines) and `app/sheet_view.py` (580) were not edited at
all** — they are pure transforms and they hold all the actual draft logic.

The board is read, rescaled to this league's budget and enriched **once**, by
`draft_tabs.prepare`. That matters beyond the 11ms: the Board owns the auction-budget
widget and the Sheet reads the same value, and sharing one frame makes them price at
the same number by construction rather than by both sides remembering to read the same
session key.

### Rundown — how the draft graded, three ways

Shown only once a league has picks for the season; seven of ten were still waiting the
day it shipped. Logic in `app/rundown.py`, Streamlit-free and tested.

Every roster in the league is scored under **three independent bases**, side by side
rather than averaged into a verdict:

| Basis | Column | What it is |
|---|---|---|
| ESPN | `ESPN_projected_total` | the market's own view, and what most of the room drafted off |
| The Athletic | `ATH_projected_total` | a sixth independent vote ([38](38-the-athletic.md)); flattens backfields |
| Ours | `TRUE_Points` | the blend, in this league's own scoring |

**The headline is the best legal starting lineup, not the sum of the roster.** A season
is scored out of a starting lineup, so four good quarterbacks are one good quarterback.
`fill_starters` is greedy descending by points into the scarcest eligible slot, which
is *exact* rather than heuristic — fantasy eligibility is a transversal matroid and
greedy by weight is optimal on a matroid. Scarcity is measured from the roster rather
than by splitting the slot name, which is what makes `D/ST` (a slash that is not a
flex) and `OP` (superflex) work without a lookup table.

Also per team: rank under each basis, a **Consensus** column, `Σ vor` over starters,
`Σ value` over the whole roster, an 80% band, and the widest disagreement between the
bases named out loud — a roster ESPN likes and The Athletic does not is a roster whose
value rests on a claim you can go and check.

**On the word "grade".** The repo does not hand out letters, and inventing one would be
a *fourth* opinion wearing the clothes of a summary of the other three. So the primary
render is rank-of-N, points, and the gap to the league median. A letter chip sits beside
it as a **pure rescaling of the within-league percentile** — no new evidence, and it
cannot disagree with the rank it is derived from, because it is computed from it.

Two arithmetic corrections the build forced:

* **`Σ pts_p10` is not a floor.** It prices the world where every starter busts *at
  once*. On one real six-team roster it read 1,018 against a projection of 2,183 — a
  floor 53% below the mean that nothing in the model supports. The band is built from
  the per-player standard deviations instead, `total ± 1.2816·sqrt(Σ sd²)`, with the
  two caveats stated on the page: independence understates the width, and kickers and
  defences carry no fitted spread (plan 28 covers QB/RB/WR/TE — 299 of 1,036 board
  rows), so they add their means and no variance.
* **`vor` belongs to starters, `value` to the roster.** VOR is points above
  *replacement*, and replacement level is defined by the starting slots, so summed over
  a full roster it reads a deep bench as a penalty — an eight-team superflex roster came
  out at **−528**. `value` is rank-against-ADP, and a bench player taken below his price
  is real value; that one is summed over everybody.

### Roster — the Sunday-morning tab

`app/pages/roster.py` over `lineup.py`. The lineup ESPN has set beside the best one
available, and **the specific swaps between them** — which is the deliverable. Knowing
you left 19 points on the table does not tell you to start Trevor Lawrence over Philip
Rivers. `analytic_utils.get_best_proj_lineup` could not be reused: it needs a live
`espn_api.League`, which this app promises never to put in a render path, and it returns
a float rather than the swaps.

**Σ swap gains equals the lineup's total gain, exactly**, and getting that wrong was the
build's sharpest bug. Pairing changes by *slot* double-counts a player who merely
moves: Pickens shifting `RB/WR/TE → WR` while Williams shifts the other way is a
permutation worth nothing, and slot-pairing read it as two swaps gaining 26 points on a
lineup worth 1.5 more. A start/sit list whose numbers do not add up to the headline is
worse than no list. Computed on the symmetric difference of the two lineups instead,
then paired position-compatibly so a flex addition is not reported against a benched
quarterback. Verified against all 114 rosters in all ten leagues: zero mismatches.

Also here: players who cannot play are excluded from the optimum and flagged loudly if
one is in the starting lineup, and a **single-source starters** count — a projection
resting on one source that really had an opinion is not wrong, but nothing is
corroborating it.

### Free Agents

`team_owner == "Free Agent"`. The pool is **not a separate artifact**:
`lineups.parquet` carries every unrostered player as extra rows on a synthetic team,
which is why this is a filter rather than a second ingest, and why it agrees with the
Roster tab about what a player projects.

**Add/drop is scored as one decision.** A receiver who out-projects your worst bench
player by four points is worth nothing if he still would not start, so every suggestion
is the difference between two optimal lineups (`lineup.add_drop_gain`). The common
answer is "none of these would improve your lineup", and that is the answer worth
trusting — a waiver claim that does not change your Sunday is a roster spot spent on
nothing.

### Matchup

`app/pages/matchup.py`. Two artifacts, because neither has both halves:
`lineups.parquet` knows every projection and whose roster it is on and nothing about
the fixture; `team_stats.parquet` knows the fixture and nothing about players.

The win probability, its fitted dispersion and its pre-committed gates are
[plan 42](42-weekly-matchup-odds.md).

## Three bugs fixed to get here

All three were live and none were introduced by this work.

1. **`team_stats` could not be built for the current season at all.** An unplayed
   season has a median score of 0, and the cross-season score normalisation divided by
   it: `ZeroDivisionError: float division by zero`, on every call before week 1
   finishes — precisely when the Matchup tab wants the artifact. Both degenerate cases
   (unplayed season, absent baseline) now resolve to "do not adjust". Played seasons
   reproduce their previous numbers exactly.
2. **`analytic_utils.get_best_trio` indexed past an empty position.**
   `get_top_players(lineup, "TE", 1)[0]` raises `IndexError` on any lineup with no
   tight end — a league *having* a slot does not mean a roster has anybody who can
   fill it. One such week in Winfield_Football's history took the league's entire
   multi-season build down. Now `analytic_utils.top_points`.
3. **One unservable week cost a league its whole history.** ESPN does not always serve
   `rosterForCurrentScoringPeriod` for an old week and `espn_api` indexes it
   unconditionally. Weenieless_Wanderers 2019–2021 are simply not available; they are
   now reported and skipped rather than fatal.

And one identity problem that is ESPN's rather than a bug: **the fixture list and the
rosters do not always describe a team the same way.** On Weenieless week 1 the
box-score view served no owner for one team — `"Unknown Owner"` playing as `"Team 11"`
— while the roster view had it as Stephen Touchstone's `"Sandusky Shower Pals"`. That
team was Tommy's week 1 opponent, so matching on owner alone left the tab empty in the
league it mattered in. GOP Degenerates is the mirror image, with the roster side
missing the name. `matchup_sim.opponent_map` resolves in three passes — owner, then
team name (plan 25's documented within-season key), then the last one standing when
exactly one is unmatched on each side — and says in the UI which pass it used.

## A test destroyed the real store, and that is worth recording

`test_freeze.py`'s fixture redirected `paths.DATA_DIR` and not `paths.STORE_DIR`. **
`STORE_DIR` is computed from `DATA_DIR` at import**, so the patch moved nothing, and
`write_league_store` wrote three-row test frames over three leagues' real
`board.parquet` (2.2 MB each), `draft.parquet` and `meta.json` — on the afternoon of
two live drafts.

Everything came back from S3, which is exactly what a system of record is for
([24](24-s3-data-flow.md)). Two things are worth keeping from it:

* **Nothing objected at the time.** The only symptom was two unrelated tests in
  `test_lab_g2.py` failing a run later, on the wreckage. `--verify` then named the
  divergence precisely, and the fix was a surgical `get_bytes` per artifact rather than
  a `--pull`, because local now held the only copy of a freshly built `team_stats`.
* **`tests/conftest.py` now makes it impossible.** An autouse fixture fails any test
  that calls `write_league_store` while the store root is still the real one. Redirecting
  the store is still the test's own job; forgetting now fails loudly instead of
  succeeding silently against real data. Autouse, because the tests that needed
  protecting were exactly the ones that did not know they did.

## What is left

Plan 08's remaining views are **not a fifth tab.** Each is a sub-tab of whichever of
the four it answers a question for, which is the structural payoff of the restructure:

| Owed | Where it goes | Blocked on |
|---|---|---|
| Player Explorer (cell 13, `peek_proj_stats`) | Roster | nothing |
| Projection Accuracy (cells 14, 34–36) | Roster, beside the blend weights it should inform | nothing |
| Playoff Odds (`simulate_season`) | Matchup | needs porting off the live `League` object, and caching into the store during refresh |
| Standings, power rankings, luck index | Matchup | `luck_index.py` carries seven TODOs calling its own scaling "crude and trash" |
| History, `h2h_build` | Matchup | `team_stats` now exists, so unblocked |
| ~~Rest-of-season value on Free Agents~~ | Free Agents | **Re-scoped 2026-09-10 to [49](49-rest-of-season-waivers.md)**, which does not wait for 19: FantasyPros publishes a rest-of-season consensus, and the decision logic uses its *rank* against this league's own `replacement_rank` rather than its points |
| Points-over-expectation per manager | Rundown | scoring past seasons in each league's own rules (roadmap phase 1) |
| ~~`percent_owned` in the weekly store~~ | Free Agents | **Done 2026-09-10** — and it needed no `refresh` change at all. `board.parquet` joins to the weekly frame on `player_id` 1:1, measured across all ten 2026 stores: no board carries a duplicate id, and `percent_owned` reaches **100% of every league's pool**. The Free Agents tab joins it in the route and gained a `Max Owned %` filter. `injury_status` came along the same way, which is what made the IR drop rule possible. See [49](49-rest-of-season-waivers.md) |
| Live draft polling | nowhere, on purpose | still refused: it would put an ESPN client in a render path the app promises is an 11ms parquet read |

Smaller things, named so they are not rediscovered:

- **The Board sub-tab's filters carry no widget keys.** Verified to collide with
  nothing — every other sub-tab is prefixed `value_*`, `league_*`, `cal_*`, `sheet_*` —
  so they were deliberately left alone on draft day rather than given keys that would
  change filter-persistence semantics on the surface being drafted from.
- ~~**A multi-league view has no seam.**~~ **Done 2026-09-09** — it did not need a
  second `Selection`. `views.home_tab` reads the viewer's leagues from
  `auth.visible_leagues` and calls `home.summarise` per league, cached on
  `store.version` so the sidebar's refresh button invalidates the landing page along
  with every deep tab. Five leagues cost ~1.7s cold and nothing warm. `Selection`
  stays singular, and Home is the one tab that does not read its `league_key`.
- **Adding a league still takes two edits.** `config.yaml` *and*
  `auth.DEFAULT_VIEWER.leagues`. `auth.py` documents this and it remains true —
  `jeffs_league` was configured, refreshed and published on 2026-09-01 and stayed
  invisible until the tuple changed.
- **[Plan 04](04-matchup-periods.md) is still open**, and Roster and Matchup inherit
  it: Winfield_Football silently loses a week to multi-week matchup periods, with a
  hardcoded hack at `scrape_player_stats.py:568`.
- **`Store Overview` was deleted.** Its content is diagnostics and now lives in the
  sidebar, which is where diagnostics belong. It did not deserve a tab.
- **Standings landed on Home rather than becoming a tab of its own**, which is where
  plan 08 had it. A table per league under the cards is the same information in the
  place you were already looking, and it shares the per-league read the cards need.

## Verification

```bash
python -m pytest                          # 2,000+ tests
streamlit run app/main.py
```

Done at build time, and worth repeating after any change to the shell:

- **All five tabs render for all ten leagues** — 50 combinations through
  `AppTest`, zero exceptions. Includes GOP (16-team IDP, $250 keeper auction),
  Weenieless (superflex), Winfield (6-team), and Jeff's (2026-only, so no draft
  history and no `team_stats`).
- **The league survives a tab change**, checked in both directions. This is the bug
  `sticky_selectbox` exists for, and the restructure is what makes it structurally
  impossible.
- **The Sheet's 2x2 layout still holds on a real screen.** Plan 37 found that defect by
  Playwright screenshot at 1900×1400, and `AppTest` cannot catch it — "it reports the
  frame a page rendered, never the width it rendered into."
- **Swap gains reconcile.** `Σ gain == optimal − current` for every roster in every
  league.
- **Empty states render with no traceback** for a league missing any artifact, and
  pre-game where every `points` is zero.
- `ESPN_FFL_STORE_SOURCE=local streamlit run app/main.py` still works, and nothing in
  the render path imports an ESPN client.
- **A Home card's button lands on the right league.** Clicking GOP's *Free Agents*
  moved both the page and the League selector in one rerun, checked through `AppTest`.
  The two-step is required: `st.switch_page` **cannot** be called from an `on_click`
  callback, because callbacks run before the script body and therefore before
  `st.navigation` has declared the pages — it fails with `Could not find page:
  routes/free_agents.py`, the legacy-`pages/` error, which is a misleading way to learn
  it. So the callback writes `session_state` and the page body switches.
- **The stale badge fires at the right hour.** 0.5h, 4.1h and 23h read as a plain
  caption; 26h and 72h read as an error.
