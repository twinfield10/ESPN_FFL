# 14 — Thin the Google Sheets output (and why it isn't being retired)

**Priority:** Low · **Effort:** S
**Status:** Investigated 2026-08-05. **Step 1 done** — Sheets is now a renderer
over the store. Sheets is **kept**, not deprecated.
**Depends on:** [07 (store)](07-frontend-foundation.md) — done

## Why this came up, and what changed

[Plan 07](07-frontend-foundation.md) put a local app in front of the same data
Sheets publishes, so the question was whether Sheets still earns its place. This
started as "retire Google Sheets". It isn't, for one reason:

> **The Sheet gets read on a phone, away from home.**

That is a capability the app structurally cannot have. Streamlit binds `0.0.0.0`
and prints a Network URL, so a phone on the same wifi works — but the app is a
*service*, alive only while the laptop is awake and on the same network. A Google
Sheet is a *published artifact*: it works from anywhere, on a phone, with the
laptop shut. Remote-access workarounds (Tailscale, a tunnel, Community Cloud) all
still need the laptop running, so none of them close the gap.

So this plan is now: **keep Sheets, make it cheap to keep.** Do not delete it.

## The second reason it stays

`populateGoogleSheet.py` publishes to eight leagues owned by **five different
people**:

| Owner | Leagues published |
|---|---|
| Tommy Winfield | Knights_FFL, GOP_Degenerates, Weenieless_Wanderers |
| John Baizer | John_PC_League, John_ATL_League |
| Will Winfield | 12 Dudes one Cup |
| Robert Cooleen | Big Red Fantasy Football |
| Fields Pierce | Washed_Up_Fijians |

Each `display_name` must match a Google Spreadsheet name exactly, and those
spreadsheets are shared to the service account — so they are somebody's
spreadsheets, opened by somebody. **Five of the eight leagues belong to people who
are not you**, and for them the Sheet is the only access there is. Removing it is
a conversation with them, not a refactor.

(`Winfield_Football` is commented out of the `all` cohort, so the league with the
deepest history is already app-only. That works fine.)

## Step 1 — done: Sheets reads the store

`run()` held a **line-for-line duplicate** of `Scripts.equivalence.build_league_frame()`:
fetch, lineups by matchup, free-agent market, concat/`fillna`/dedupe, blend.
`build_league_frame`'s own docstring said it "mirrors what
`populateGoogleSheet.run()` does" — the mirror was the problem. It is the same
defect the repo already paid to remove once: 12 projection functions existed here
and in the notebook, 8 had drifted, so the notebook used to *decide* a lineup and
this script computed different numbers. Two copies instead of twelve, same shape.

`run()` now reads the store:

```python
LINEUPS = read_league_store(year, cfg['key'], "lineups")
meta = read_meta(year, cfg['key'])
curr_week = meta["current_week"]
```

Consequences:

- **One ingest path.** Sheets and the app cannot disagree, because the numbers come
  from the same parquet. `curr_week` comes from the store's metadata rather than a
  live fetch, so both report the same week from the same build.
- **The file is a renderer.** It imports no ESPN client and no blend primitive —
  the explicit import list dropped from 12 projection functions to 3 view
  builders. `tests/test_sheets_renderer.py` fails if any ingest name reappears.
- **A missing store skips that league**, with the command that would build it,
  rather than aborting the run. Publishing eight leagues should not be lost to one.
- **`run()` gained a `season=` argument**, so a past season can be republished from
  the store without re-fetching it.

Verified: all ten tabs are identical built from the store versus built from a
fresh ESPN ingest, for Knights_FFL 2026 — same shapes, same columns, numerics
within 1e-9. `run()` also completes with every outbound socket blocked, which is
the proof it no longer ingests.

New requirement: `python -m Scripts.refresh` must run first. That was already the
documented order in the weekly runbook.

## Step 2 — the remaining cost, and what to do about it

What is left is not the pipeline, it is the rendering:

| Cost | Detail |
|---|---|
| **~9.3 min of `time.sleep`** | 10 sheets × 5s per league, plus 20s between leagues. With ingest now a store read, this is essentially the *entire* runtime of a Sheets run. |
| **588 lines** | `write_to_google` is 588 of the file's 760 lines — hand-built `repeatCell` and `addConditionalFormatRule` gradient specs. |
| **3 dependencies** | `gspread`, `gspread-dataframe`, and `oauth2client==4.1.3`, which is **end-of-life upstream**. A Google auth change means migrating to `google-auth` mid-season. |
| **1 plaintext secret** | `gs4creds.json`, a GCP service-account private key in the repo root. Gitignored, never committed — verified across all history. |

The cheapest wins, in order:

1. **Publish fewer tabs.** You read the Sheet on a phone; you are not scrolling
   `FA_IDP` on a phone. Cut to the two or three tabs that actually get opened —
   `Lineup` and `League_Projections` are the plausible ones. Each tab dropped is 5s
   per league, so 10 tabs → 3 takes the run from ~12 min to ~4.
2. **Split the cadence.** `run()` takes a league list, so `p.run(p.tommy)` and
   `p.run(p.john + p.will + p.cooleen + p.fields)` are already available with no
   code change. Yours can be published more often than theirs, or vice versa.
3. **Migrate `oauth2client` → `google-auth`.** Small, and removes the one
   dependency here that is a standing liability rather than a maintained package.
   Worth doing before the season rather than during it.
4. **Only if the formatting stops earning its keep:** replace the hand-built
   gradient specs with a smaller helper. 588 lines is a lot of code for colour
   bands — but it works, and it is what makes the Sheet readable at a glance on a
   phone, which is the whole reason Sheets survives. Do not touch it for tidiness.

Explicitly **not** recommended: deleting `write_to_google`, dropping the
dependencies, or revoking `gs4creds.json`. The phone requirement and the four
other owners both point the other way.

## Verification

- Ten tabs identical from the store versus a fresh ingest — **done**, above.
- `run()` completes with sockets blocked — **done**.
- After step 2.1, the surviving tabs still render with their conditional
  formatting, and the run time drops in proportion to the tabs removed.
- After step 2.3, a full publish succeeds with `google-auth` installed and
  `oauth2client` absent.
