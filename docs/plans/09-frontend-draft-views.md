# 09 — Local frontend: draft views

**Priority:** High (seasonal) · **Effort:** Large · **Status:** Not started
**Depends on:** [07 (foundation)](07-frontend-foundation.md), and the draft
phases in [`../STATE_OF_THE_REPO.md`](../STATE_OF_THE_REPO.md#roadmap--draft-strategy)

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

Interactions that matter:

- **Sort by value, not by rank.** The board's job is surfacing where your
  projections disagree with the room.
- **Tier bands as the primary visual.** Tier breaks drive draft decisions far
  more than one-spot rank differences. Colour by tier, and show where each tier
  runs out.
- Position filter, and a **roster-need** toggle that filters to slots you
  haven't filled.
- **Scarcity curve** — projected points by position rank, which makes the
  positional cliff visible. This is the single most useful chart on the page.

**League-awareness is the differentiator.** Replacement level comes from each
league's real starting slots, so the same player is legitimately ranked
differently across your nine leagues. The scan found real variety here — 6 to 16
teams, a superflex `OP` slot in Weenieless Wanderers, IDP `DP` in GOP
Degenerates, no D/ST in 12 Dudes. A generic board from any website cannot do
this, and it's the whole reason to build one.

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

- Replacement ranks hand-check for a standard 12-team league (RB ≈ RB30 with
  flex) and differ correctly for the superflex and IDP leagues.
- The same player ranks differently across leagues, for the right reason.
- Board renders for all nine leagues, including the one with no D/ST.
- Live page dry-run against a replayed 2025 draft keeps its available-player set
  in sync throughout.
- Kill the network mid-poll: stale banner appears, board still renders, manual
  mark-drafted still works.
