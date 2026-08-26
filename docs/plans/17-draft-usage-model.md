# 17 — The draft usage model — **superseded**

**Status:** COMPLETE

**Where it stands:** Superseded on 2026-08-06 by [18](18-season-usage-model.md).
Kept as a stub so existing links resolve.

This was an outline of a pre-season usage model, written alongside a survey of
what `nflreadr` exposes. It has been split, because it overlapped heavily with
the then-plan 16 — same data layer, same features, same gates, written twice:

| Where it went | What |
|---|---|
| [16 — usage data layer](16-usage-data-layer.md) | The data inventory, the feature layer, leakage discipline, and the go/no-go gates. All shared, so it lives in one place |
| [18 — season usage model](18-season-usage-model.md) | The pre-season / draft head this plan outlined |
| [19 — weekly usage model](19-weekly-usage-model.md) | The in-season head |
| [20 — consensus sources](20-consensus-sources.md) | Adding more free projection feeds, which this plan explicitly ruled out of scope |

**Two of this plan's claims were measured and did not survive**, which is the
main reason it should not be followed as written. Both are corrected in plan 16
§Measurements, and reproducible with `Rscript R/UsageEvidence.R`:

- It said `load_ff_opportunity`'s expected production reshapes the season model
  because expectation is what persists. Measured over 2,252 player-season pairs,
  expected points per game predicts next season's PPG at r = +0.779 against
  actual PPG's +0.792 — **it does not beat actual production at season level.**
  It is more *stable* (+0.816 vs +0.792), which is a smaller and different claim.
  The weekly result runs the other way and is where the edge actually is.
- It deferred snap counts ("needs its own crosswalk hop") and route
  participation ("needs per-play list unnesting, so v2"). Both are v1:
  `load_rosters_weekly` carries `gsis_id` and `pfr_id` together, and
  participation joins 100% to play-by-play on `(game_id, play_id)`.

Its data inventory was good and is preserved, verified and extended, in
[plan 16](16-usage-data-layer.md#what-is-reachable--verified-2026-08-06).
