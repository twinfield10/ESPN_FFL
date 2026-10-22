# 55 — Splitting The Athletic's fetch from its parse

**Status:** IN PROGRESS

**Where it stands:** Built 2026-10-22. `Scripts.fetch_athletic` only lands the raw
`.xlsx`, `Scripts.parse_athletic` imports it from the nightly (stage 2h), and
`Scripts.load_athletic.check_weekly_format` refuses a slate whose stat columns were
renamed or came through blank. All nine 2026 weekly downloads pass the check. **Owed:**
the first real landed-then-parsed week on the Mac mini, which is week 8 (the slate
usually lands Tuesday or Wednesday, so 2026-10-27/28). Until then nothing here has run
end to end against a file that wasn't already imported.

**What it is:** [Plan 53](53-athletic-weekly-fetch.md)'s fetch downloaded the slate into
a temporary folder and passed it straight to `load_athletic.build_weekly`. Three
problems came out of a review of that path on 2026-10-22:

1. **A renamed stat column imported silently.** The parser finds columns by header.
   A header it doesn't find reads as None for every player, and the blend then drops
   The Athletic from that stat with no warning. No check covered this.
2. **A failed parse threw away the raw file.** `build_weekly` copies the workbook into
   the landing folder only after parsing it, so the one file that showed what changed
   was deleted with the temporary folder.
3. **A file replaced at the same link was never fetched.** An unchanged href skipped the
   download. Re-uploads have so far come with a new timestamp folder, but nothing
   guaranteed that.

The download is the hop that gets bot-blocked, and the parse is the one that can be
rerun. So they're now separate steps, each with its own status file.

---

## What the format has and hasn't done

Measured on the nine weekly files in `Data/Projections/TheAthletic/Landing/2026/`
(weeks 1–7, two copies each of weeks 2 and 6):

| Changed | Unchanged |
|---|---|
| `FPS` became `Proj` (week 2) and moved from last column to second | Every position block's stat headers, exactly |
| `Flex`, `Superflex` and `DST` blocks added (week 4) | The sheet name pattern `NFL_<season>_Week_<n>_<flavor>_Weekly` |
| A `K` block in one week-6 copy and not the other | The four position banners `QB`/`RB`/`WR`/`TE` |

The stat headers per block were the same in all nine files:

| Block | Stat headers |
|---|---|
| QB | Pass YD, Pass TD, INT, Rush Att, Rush YD, Rush TD |
| RB | Rush Att, Rush YD, Rush TD, REC, REC YD, REC TD |
| WR | Rush YD, Rush TD, REC, REC YD, REC TD |
| TE | REC, REC YD, REC TD |

Every stat column in every block was **at least 99% filled** in every file. Zeros are
written as zeros, not left blank, so a receiver's rushing touchdowns are 96–100% zero
and 100% filled.

## Design

**Fetch** (`Scripts.fetch_athletic`, 05:45 LaunchAgent, unchanged schedule):
sitemap → article → link → download, every run. The download is compared by SHA-256
with the last file landed for the week; new bytes land as
`<upload name>_<first 8 hex of sha>.xlsx`, so a corrected re-upload under the same
name can't overwrite the raw file the last import came from. Outcome is `fetched`
(was `imported`). `--push` sends the archive tier, which now carries the raw file
rather than a parsed one. The fetch never calls the parser, and its test fixture fails
if it does.

**Parse** (`Scripts.parse_athletic`, nightly stage 2h, after the weekly sources and
before the usage model and lineups): every week in the fetch status whose `sha256`
differs from the last one imported goes through `build_weekly`. Its own status file,
`parse_status.json`, because the fetch and the nightly are separate processes and
today's 08:45 fetch showed they can overlap.

**The check** (`load_athletic.check_weekly_format`, called by `build_weekly` before
anything is written, so the manual `--what weekly --file` path gets it too). It raises
`WeeklyFormatError` naming every problem at once:

- a `QB`/`RB`/`WR`/`TE` block is missing;
- a block lacks one of its stat headers from the table above. The check is per block,
  not per sheet: `REC YD` renamed in the TE block alone would still be found in the WR
  block, and every tight end would import with no receiving yards;
- a block's stat column is under **90%** filled. That's far below the 99% floor in
  real files, so it means the column went empty, not a thin week.

A new column in a position block is printed as a `NOTE` and not refused. Nothing reads
it, so it can't put a wrong number on the board. `FPS`, `Proj` and whole extra blocks
are ignored silently, because all three have already come and gone in real files.

**On a refusal:** the weekly parquet keeps the last good import, the nightly logs a
`WARNING` and posts a notification on the mini, and the board still builds. The week
stays pending, so the warning repeats every night until the parser is fixed. After the
fix, the next nightly imports the file landed days earlier with no re-download.
`python -m Scripts.refresh_status` prints `ATH parse REFUSED <week>` and exits 1. The
`ATH weekly` age line can't see this, because a refusal leaves the file at its old age,
which reads ok for eight days.

**Not fatal to the nightly, on purpose.** A format change in one of the board's weekly
sources shouldn't hold back ESPN, injuries and the other four sources. This is the same
treatment as the Pinnacle and engine-shadow stages.

## Migration

The mini's `fetch_status.json` holds week 7 as the old code wrote it: `imported_at`,
and `file: Week7_Proj_1021.xlsx` with no hash suffix. Both steps read it as is. The
fetch compares by its `sha256` and reads `unchanged`. The parse finds no
`parse_status.json`, imports week 7 once from that file (the same bytes, so the same
rows), and is caught up. Weeks 1–6 were hand-imported, are not in the fetch status,
and are not touched.

## Gates

- **G1 (built):** a renamed header in one block, a blank column, a missing block, and
  several problems at once are each refused by name. A refused slate writes nothing.
  Same link with new bytes lands a second file. All in `tests/test_athletic_source.py`,
  `tests/test_fetch_athletic.py` and `tests/test_parse_athletic.py`.
- **G2 (built):** all nine real 2026 slates pass the check
  (`test_every_landed_2026_slate_passes_the_format_check`).
- **G3 (owed):** week 8 on the mini: `fetch_athletic --status` shows `fetched`, the
  next nightly's log shows `Athletic weekly parse: ok`, and `parse_athletic --status`
  records the week with the same `sha256`.

## Where the old behavior is described

`docs/SEASON_ROLLOVER.md` (weekly run and the mini section), `README.md`,
`docs/DATA_CATALOGUE.md` and plan 53. All updated in the same change.
