# 05 — Dependency upgrades

**Priority:** Medium · **Effort:** Medium · **Status:** espn-api **done
(2026-08-05)** · everything else open

Everything is pinned in `requirements.txt` to what the 2025 season ran on. Most
of it is safely behind; two items need real thought.

| Package | Pinned | Latest | Action |
|---|---|---|---|
| **espn-api** | ~~0.45.1~~ **0.46.0** | 0.46.0 | ~~Upgrade before the draft~~ **Done** |
| **pandas** | 2.3.3 | **3.0.5** | Defer — major version |
| **oauth2client** | 4.1.3 | 4.1.3 (dead) | Replace with `google-auth` |
| pyarrow | 18.1.0 | 25.0.0 | Upgrade |
| polars | 1.19.0 | 1.43.2 | Upgrade |
| selenium | 4.20.0 | 4.46.0 | Upgrade with the Pinnacle scraper |
| boto3 | 1.35.88 | 1.43.62 | **No longer low priority** — since [plan 24](24-s3-data-flow.md) S3 is the system of record and the app's read path, so this is load-bearing. `s3fs==2024.12.0` is pinned alongside it and was installed but unpinned until then |
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

### Done 2026-08-05 — and "risk: low" was wrong

The upgrade landed, but only after the equivalence harness caught a **silent
breaking change** this plan did not anticipate. Upgrading and changing nothing
else produced **332 differing (league, column) pairs**, including actual stat
columns: `receivingYards` differed in 1,007 of 1,779 Winfield rows, by up to
202.5.

The cause. Puka Nacua's week 16 went from `225.0` to `22.5` — the same number at
0.1 pts/yd. `points_breakdown` had stopped being yards and started being points:

| version | `breakdown` | `points_breakdown` |
|---|---|---|
| 0.45.1 | *not set on BoxPlayer* | **raw stats** (`box_player.py:33`, `stats.get('breakdown', 0)`) |
| 0.46.0 | **raw stats** | **applied points** |

0.46.0 corrected a misnomer — the attribute called `points_breakdown` had never
held points — and the pipeline was reading the old name. Every stat column
silently became a point value, which then got multiplied by the scoring rate
again. Fix is one line at each of two call sites in `scrape_player_stats.py`:
read `['breakdown']`, not `['points_breakdown']`.

**Nothing about this raised an error, and no test caught it.** It would have
shipped as quietly wrong projections. `tests/test_espn_api_contract.py` now pins
the semantics: the version floor, that `breakdown` carries the raw stat and
`points_breakdown` the applied points, and that the call site reads the former.

After the fix, the 2025 equivalence diff is exactly what this plan predicted —
two non-numeric columns per league, every stat and projection identical:

| column | cells (Knights 2025) | what changed |
|---|---|---|
| `pro_team` | 64 | The traded-player fix. `David Montgomery HOU → DET`, `Jakobi Meyers JAX → LV`, `Tyreek Hill None → MIA`. All corrections. |
| `player_active_status` | 11 | D/ST units on a bye now report `bye` rather than `active`. Also a fix. |

**Determinism was verified before attributing anything to the upgrade.** Two
snapshots taken on the same version are byte-equivalent, so the diff is caused by
the version change and not by run-to-run variation in the live fetch. Worth
repeating for any future bump: without that control the 332-column diff is
uninterpretable.

Also fixed en route: `Scripts/equivalence.compare` crashed with
`TypeError: numpy boolean subtract` on any snapshot containing the `*_is_imputed`
provenance flags from [plan 03](03-projection-source-coverage.md), because
`pd.api.types.is_numeric_dtype` is `True` for `bool`. The harness could not
compare a post-plan-03 snapshot at all.

**What 0.46.0 does *not* fix.** Both `espn_api` bugs the pipeline works around are
still present, so neither workaround can be retired:

- `Settings.__init__` still writes `points` onto the module-level
  `SETTINGS_SCORING_FORMAT_MAP` dict, so `fetch_utils.isolate_scoring_format()`
  stays. This answers the open question in [`README.md`](README.md).
- It still reads only slot `'16'` and still uses a falsy-or, so an override of
  exactly `0.0` falls through to the base — hence
  [plan 11](11-per-slot-scoring.md) reads `mSettings` directly.

The changelog's "Add/expose Point Breakdowns" is genuinely useful: the applied
points are now available without the hand-rolled requests, which is worth
revisiting when trimming `scrape_player_stats.py`.

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

1. ~~`espn-api` alone, verified against the equivalence harness. Before the
   draft.~~ **Done 2026-08-05** — see above.
2. `oauth2client` → `google-auth`. Small and isolated.
3. `pyarrow`, `polars`, and the routine minors together.
4. `pandas` 3.0 in its own window, after the season or during a quiet week.

Bump the pins in `requirements.txt` as each lands, and note in
`STATE_OF_THE_REPO.md` which season each version actually ran.
