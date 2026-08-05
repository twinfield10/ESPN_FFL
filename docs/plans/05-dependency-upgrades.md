# 05 — Dependency upgrades

**Priority:** Medium · **Effort:** Medium · **Status:** Not started

Everything is pinned in `requirements.txt` to what the 2025 season ran on. Most
of it is safely behind; two items need real thought.

| Package | Pinned | Latest | Action |
|---|---|---|---|
| **espn-api** | 0.45.1 | **0.46.0** | **Upgrade before the draft** |
| **pandas** | 2.3.3 | **3.0.5** | Defer — major version |
| **oauth2client** | 4.1.3 | 4.1.3 (dead) | Replace with `google-auth` |
| pyarrow | 18.1.0 | 25.0.0 | Upgrade |
| polars | 1.19.0 | 1.43.2 | Upgrade |
| selenium | 4.20.0 | 4.46.0 | Upgrade with the Pinnacle scraper |
| boto3 | 1.35.88 | 1.43.62 | Low priority (S3 path is dormant) |
| numpy / scikit-learn / requests / bs4 / statsmodels / gspread | — | minor | Routine |

## espn-api 0.45.1 → 0.46.0 — do this first

Released 2026-03-23. Directly relevant to the draft work:

- **"Fix proTeam for traded players + multiple bug fixes."** `proTeam` feeds the
  `pro_team` column and is part of how players are matched to projection
  sources. Wrong teams for traded players means bad joins on exactly the players
  whose situation changed — which is exactly who you're re-evaluating at draft
  time. This alone justifies the upgrade.
- **"Add/expose Point Breakdowns on Player/BoxPlayer."** The pipeline currently
  fetches `points_breakdown` / `projected_breakdown` by hand via raw requests in
  `scrape_player_stats.py`. If the library exposes these directly, a chunk of
  that code can go.
- **"Throw ESPNAccessDenied when no swid or espn_s2 provided."** Better failure
  mode; complements the `fetch_league` error handling fixed in Phase 0.
- **"Add INTRA_DIVISION_RECORD tiebreaker."** Affects standings and therefore
  `simulation_utils` playoff odds.
- Adds a `jersey` attribute (cosmetic).

**Risk:** low. The wrapper API is stable across this bump. The pipeline's
integration surface is small — `fetch_utils.fetch_league` plus attribute reads.

**Verify:** re-run the Phase 0 equivalence check against 2025 (old vs new should
still match except where the traded-player fix legitimately changes `pro_team`),
then confirm all nine leagues still fetch for 2026.

## pandas 2.3 → 3.0 — defer, but it's not scary

Major version, so it needs a deliberate window rather than a casual bump. The
good news is the codebase is closer to ready than expected.

Scanned for the idioms 3.0 removes — `.append()` on DataFrames, `applymap`,
`fillna(method=)`, `iteritems`, `is_categorical_dtype`, `delim_whitespace`,
`errors='ignore'`: **zero hits**. The 52 `.append(` matches are all list
appends. The 15 `inplace=True` uses are on real DataFrames, not chained slices —
discouraged, but they work.

Running the full `clean_lineups` pipeline with warnings unmuted produced **54
warnings, all one kind**: `PerformanceWarning: DataFrame is highly fragmented`.
No `FutureWarning`, no `DeprecationWarning`. See plan 06 for the fragmentation.

The real 3.0 risk is copy-on-write becoming default, which changes behaviour
where code mutates a slice and expects it to propagate. Static scanning won't
catch that — the equivalence harness will. Do the upgrade behind that check.

## oauth2client → google-auth

`oauth2client` is at its final version and has been deprecated by Google since
2018. It handles the Google Sheets service-account auth in `write_to_google()`:

```python
from oauth2client.service_account import ServiceAccountCredentials
creds = ServiceAccountCredentials.from_json_keyfile_name('gs4creds.json', scope)
```

Replacement is contained — `gspread` supports it natively:

```python
import gspread
gc = gspread.service_account(filename='gs4creds.json')
```

One function, same credentials file. Worth doing before it breaks on its own.

## Suggested sequencing

1. `espn-api` alone, verified against the equivalence harness. Before the draft.
2. `oauth2client` → `google-auth`. Small and isolated.
3. `pyarrow`, `polars`, and the routine minors together.
4. `pandas` 3.0 in its own window, after the season or during a quiet week.

Bump the pins in `requirements.txt` as each lands, and note in
`STATE_OF_THE_REPO.md` which season each version actually ran.
