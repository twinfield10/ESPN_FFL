# 20 — Consensus: more free projection feeds, gated on measured independence

**Priority:** Medium · **Effort:** M · **Status:** Not started — and the
independence evidence now argues **against** candidate 1
**Depends on:** ~~[16 Step 0](16-usage-data-layer.md#step-0--the-gates-measured-2026-08-06)~~
— **measured 2026-08-06**, see the table below
**Feeds:** [03 (weight re-tune)](03-projection-source-coverage.md) ·
[02 (BetOnline access)](02-betonline-access.md) — a replacement for the dead
weekly feed

## The argument, stated once

Consensus helps when sources are **independent**. It does not help when they
agree because they are looking at each other.

This repo already has evidence on which side it sits. Plan 03 measured real,
non-imputed coverage on Knights_FFL 2025 at ESPN 100%, FantasyPros 13%,
BetOnline 12%, Pinnacle 8% — the nominal four-source blend is roughly **90%
ESPN**. And FantasyPros is *itself* an expert consensus, so adding CBS,
NumberFire, FFToday and NFL.com partly re-adds what is already there wearing
different names.

The marginal value of source *k*+1 is roughly **(1 − its correlation with the
rest)**. That is a number, not an argument, and
[plan 16 Step 0](16-usage-data-layer.md#step-0--the-gates-measured-2026-08-06) now
**has** it — a pairwise residual-correlation matrix over 2025, on non-imputed
cells only, measured 2026-08-06:

| Source | 1 − mean residual r with the rest | with the outcome partialled out |
|---|---|---|
| ESPN | +0.068 | +0.199 |
| **FantasyPros** | **+0.027** | **+0.109** |
| Pinnacle | +0.035 | +0.169 |
| BetOnline | +0.043 | +0.167 |
| usage baseline | +0.090 | +0.318 |

**This is the argument above, confirmed and pointed at a specific source.**
FantasyPros — itself an expert consensus — is the least independent thing in the
blend, at a third of ESPN's marginal value. A sixth aggregator of the same kind is
the *worst* available use of the effort, and `ffanalytics` wraps ten of them
including FantasyPros itself. Meanwhile the sportsbooks, which have real money on
the line, are the second and third most independent, and observed usage is by some
distance the first.

**So this plan is gated, and the gate has now half-fired.** Do not add six
scrapers and then ask whether they helped. Candidate 1 below (`ffanalytics`) is
the one this evidence argues *against*; candidate 2's value rests on the
ID-join and coverage arguments rather than on independence.

That said, two things make it worth speccing now rather than deferring:

1. **BetOnline's weekly feed is dead** (`403 invalid_security_headers`, see
   [plan 02](02-betonline-access.md)), so the blend is down to three weekly
   sources and one of them is Pinnacle at 8% real coverage. Breadth has a
   defensive value here independent of the correlation question.
2. **Both candidate sources join by ID**, which is the concrete route to
   retiring the ~140 hand-curated rename entries that `STATE_OF_THE_REPO.md`
   lists as an open issue.

## Candidate 1 — `ffanalytics` (R)

Free, and it wraps ten sources behind one interface.

- **Season:** CBS, ESPN, FantasyPros, FantasySharks, FFToday, NumberFire,
  FantasyFootballNerd, NFL, RTSports, WalterFootball
- **Weekly:** the same, minus RTSports/WalterFootball, plus FleaFlicker

**It is maintained**, which was the main risk and is worth recording because the
last tagged release is 3.0 from 2022 and the package looks abandoned at a glance.
Checked 2026-08-06 via the GitHub API: last push **2026-07-16**, with commits
`prep for 2026, additional caching` and `transitioning css selector to xpath`.
186 stars, 5 open issues, the oldest from 2020, none reporting a dead scraper.

```r
remotes::install_github("FantasyFootballAnalytics/ffanalytics")
```

Design notes:

- **Scrape raw stats, not points.** `ffanalytics` will happily compute projected
  points for a scoring system; this repo must not let it. Stat lines go through
  `proj_to_score` so nine leagues price them nine ways. Take the stat columns and
  discard the package's points.
- **One source directory per feed**, following the existing convention:
  `Data/Projections/<Source>/Season/<year>/`. They are separate opinions, and
  collapsing them to an average before storage throws away the covariance
  structure that plan 03's re-tune needs.
- **Every new feed gets the absent-source path** that
  `clean_pinny` / `clean_bol` use — imputed from `MEAN_`, flagged, renormalised
  out of `TRUE_*`. A feed whose scraper breaks mid-season must degrade, not
  raise.
- **Expect breakage.** Ten HTML scrapers against ten sites is ten things that
  change without notice. This is the real cost of this plan and it is recurring,
  not one-off.

## Candidate 2 — Sleeper's public API

Free, unauthenticated, no key, ~1,000 req/min, and notably more stable than an
HTML scraper. Verified live 2026-08-06.

**What works, measured:**

| Endpoint | Result |
|---|---|
| `GET /v1/players/nfl` | HTTP 200, 14.6 MB, **12,211 players** |

Per player it carries `injury_status`, `injury_body_part`, `status`,
`depth_chart_order`, `years_exp`, `search_rank`, and provider ids. 520 players
currently carry a non-null `injury_status`.

**What does not work as advertised:** the projections endpoint is undocumented.
`GET /projections/nfl/2025/1?season_type=regular&position[]=RB` returns HTTP 200
with 743 rows, but in the probe the `stats` payload held only `adp_dd_ppr` and
`player` was null. **Do not spec Sleeper as a projection source** on the strength
of blog posts; if it is wanted, someone has to work out the real parameters
first and record them here.

**What Sleeper is genuinely good for**, and this is the recommendation:

- **A second injury feed**, refreshed daily, to cross-check
  `nflreadr::load_injuries` in-season. Plan 19's availability head runs on the
  nflverse table, which is the authoritative weekly report; Sleeper is the fast
  one. Disagreement between them is itself a signal that a status just changed.
- **`depth_chart_order`**, which feeds plan 19's teammate-absence redistribution.

Note the id caveat found in the probe: Sleeper's own cross-provider id fields are
patchy — Puka Nacua's record carried `fantasy_data_id` and `sportradar_id` but
null `espn_id`, `gsis_id` and `yahoo_id`. **Join through
`Scripts/crosswalk.py`'s `sleeper_id`, not through Sleeper's own id fields.**

## Joining: by ID, not by name

This is the part that pays for itself regardless of whether any new feed improves
accuracy. Crosswalk coverage on the 2026-08-06 file, 12,470 rows:

| Column | Non-null | Ambiguous |
|---|---|---|
| `pfr_id` | 9,610 | 16 |
| `espn_id` | 8,139 | 13 |
| `gsis_id` | 7,985 | 10 |
| `sleeper_id` | 6,358 | 6 |
| `yahoo_id` | 5,488 | 5 |
| `fantasypros_id` | 4,784 | 2 |

`Scripts/crosswalk.py` already exposes `attach_fantasypros_id`, refuses to build
a lookup from an ambiguous key, and reports coverage rather than dropping
silently. New feeds join through it.

`fantasypros_id` coverage is genuinely lower than the others (4,784 of 12,470),
so measure with `crosswalk.coverage()` before relying on it — the docstring
already says so and sets no default warning threshold for that reason.

## Ship criterion

**Each feed, individually:** does adding it reduce blended per-stat MAE on the
2025 holdout, measured on non-imputed cells?

- Yes → keep it, and let [plan 03](03-projection-source-coverage.md) fit its
  weight rather than hand-tuning one.
- No → **record the measurement in this document and drop the feed.** Do not wire
  it in at a token weight. Six extra names in `WEIGHTS` that each contribute
  nothing make the blend harder to reason about and give every future debugging
  session six more places to look.

The evaluation set is the same one plan 16 Step 0 builds:
`python -m Scripts.refresh --all --season 2025`, one row per player-week with the
actual outcome, all sources' stat lines, and the `*_is_imputed` flags.

## Steps

1. **Plan 16 Step 0.** Blocking. If the matrix shows the four existing sources
   are already near-collinear, expect the new feeds to be too, and consider
   stopping here — that is a legitimate outcome and cheaper than ten scrapers.
2. **Pilot with two feeds, not ten.** Pick the two least likely to be
   FantasyPros-derived (NumberFire and CBS are the usual answer) and measure.
   Ten scrapers is a lot of surface to maintain for an unmeasured hypothesis.
3. `R/GetFFAnalytics.R` — season and weekly, stat lines only, one directory per
   source, under the existing path convention.
4. Loaders in `Scripts/projection_utils.py` / `Scripts/season_projections.py`
   following `clean_pinny` / `load_pinnacle_season`, with the absent-source path.
5. `Scripts/sleeper.py` — the players endpoint, for injury cross-check and
   `depth_chart_order`. Joined via `sleeper_id`.
6. Measure. Keep what earns its place; write down what did not.
7. Hand the surviving source set to
   [plan 03](03-projection-source-coverage.md)'s weight re-tune.

## Risks

- **Ten scrapers is ten maintenance liabilities.** Hence the two-feed pilot.
- **Correlated by construction.** Several of these sites are known to publish
  numbers derived from the same underlying models. If the matrix says so, the
  honest outcome is to add none of them and put the effort into
  [plan 19](19-weekly-usage-model.md) instead.
- **`ffanalytics` computes points.** Letting it do so would break the
  scoring-agnostic architecture that lets one pipeline serve nine leagues.
- **The Sleeper projections endpoint is undocumented and did not return stats in
  the probe.** Anything built on it can break without notice and without a
  changelog.
