# Runbooks

Reconstructed from git history and verified against the code. Neither of these
was written down before; the 2024→2025 rollover was a single 30-file commit
(`b6576e0`) and reading that diff was the only way to know what it involved.

---

## Annual rollover

Do this once, before the season starts. Roughly 30 minutes.

> **Two steps here have a deadline rather than an order.** Step 0 must happen
> **before week 1** or the evidence is gone for a year. Step 9 must happen
> **after** the season it refers to. Everything else can be done whenever.

### 0. Archive the G2 counterfactual — before week 1, or not at all

```bash
python -m Scripts.lab.g2 --archive              # writes Data/G2/<season>/
python -m Scripts.sync --push --what archive     # to s3://espn-ffl-data
python -m Scripts.sync --verify --what archive    # confirm it landed, exit 0
```

The pre-season board blended with and without `USG_`, so the question "does the
usage head earn its third of the weight?" can finally be answered against real
outcomes. It cannot be rebuilt: FantasyPros serves no season parameter, so once the
board stops being current it is gone. Plan 18 records G2 as unmeasurable on history
for exactly this reason.

**Verify the push rather than assuming it.** This is the one artifact in the repo
with no second chance, and since [plan 24](plans/24-s3-data-flow.md) S3 is where it
lives — `Data/` is no longer tracked in git. `archive/` is exempt from the bucket's
version-expiry rules for the same reason.

Run it after `python -m Scripts.refresh --all --what board` and before week 1.
It takes seconds; only the push needs network.

### 9. Score last season's G2 archive — after the season

```bash
Rscript R/GetUsage.R <last_season> <last_season>   # realised production
python -m Scripts.lab.g2 --score --season <last_season>
```

If the `with_usg` arm wins, the weight is earned. If it does not, plan 18 is
explicit about the response: **do not keep it at a token weight** — record the
numbers and take it out.

### 1. Confirm ESPN has rolled your leagues over

Do this **first** — if a league is not available yet, nothing else matters.

```bash
python -c "
from Scripts.config_utils import build_lg_vars
from Scripts.fetch_utils import fetch_league
for name, v in build_lg_vars().items():
    l = fetch_league(v['ID'], v['end'], v['SWID'], v['ESPN_S2'])
    print(f'{name}: {len(l.teams)} teams, draft={len(l.draft)} picks')
"
```

Expect every league to return teams with `draft=0` picks pre-draft. Failures:

- `ESPNAccessDenied` → cookies expired. Refresh them per `config.example.yaml`.
- `ESPNInvalidLeague` → the league has not been recreated for the new season, or
  the id changed. Some hosts create a brand-new league rather than renewing.

### 2. Bump the season in `config.yaml`

```yaml
season: 2027          # top level
...
    end: 2027         # per league
```

Leagues that folded should have `end` left at their final season.

### 3. Regenerate NFL reference data

```bash
Rscript R/GetNFL.R 2027
```

Writes `Data/NFL_Schedules.csv` — **the source of truth for both current season
and current week** across every Python scraper. Until this runs, the scrapers
will report the previous season and write into its directories.

Expect a full regular season: **272 games, weeks 1-18, game_type REG**. The script
validates this and refuses to write anything else, because a truncated or polluted
schedule produces silently wrong weeks rather than an error — `DATE_WEEK` is
left-joined in `scrape_pinnacle.py` to assign a week to each prop, so missing weeks
become null weeks. `Scripts.nfl_utils.load_schedule()` re-checks on read, in case
the file on disk is stale or hand-edited.

On preseason: `nflreadr::load_schedules()` does not return preseason games at all
(verified 2023-2026 — game types are only REG/WC/DIV/CON/SB), so the `game_type ==
'REG'` filter is really excluding the 13 **postseason** games. Player stats are
regular-season only too, since `nflfastR::calculate_stats()` defaults
`season_type` to `"REG"`. The validation is what catches it if any of that ever
changes upstream.

**Pre-season, the player-stats and tackle-ratio steps are skipped**, and that is
normal: `nflfastR::calculate_stats()` needs play-by-play, which does not exist
until games are played. The script says so and exits 0.
`NFL_Tackles_By_Position.csv` keeps the previous season's values, which is fine —
they are league-wide ratios and stable year to year. Re-run once week 1 is in the
books to refresh them and to write `Data/NFL/<season>/NFL_Stats.csv`.

### 4. Verify the season flipped

```bash
python -c "
from Scripts.nfl_utils import current_season, current_week
print('season', current_season(), '| week', current_week())"
```

### 5. Re-check the player rename maps

Rookies and name changes (suffixes especially: Jr./Sr./II/III) break the
name-based joins each year. The hardcoded maps in `Scripts/projection_utils.py`
and `Scripts/scrape_pinnacle.py` need curating. `get_match_details()` prints
unmatched players during a run — use it to find what needs adding.

### 6. Refresh the player id crosswalk

```bash
Rscript R/GetPlayerIDs.R
```

Writes `Data/NFL/player_ids.parquet` — the `gsis_id`/`espn_id`/`fantasypros_id`
table that joins nflverse data to ESPN's. Cheap, and worth re-running through the
off-season as rookies are assigned ids. `python -m Scripts.crosswalk` prints
coverage against every built board; expect ~99% of individual players, with team
D/ST units never matching.

### 7. Build the draft boards

```bash
python -m Scripts.refresh --all --what board
```

~16s for nine leagues. Re-run it in the days before each draft — ADP moves, and the
board is a snapshot of the market at build time, which the app's freshness badge
reports. See [plan 15](plans/15-draft-board.md).

### 8. Dry-run one league

```bash
python -c "
import populateGoogleSheet as p
p.run(['Knights_FFL'])"
```

---

## Weekly run

```bash
Rscript R/GetNFL.R                     # refresh schedule (scores drive current week)
python -m Scripts.scrape_FP            # FantasyPros -- needs the session cookie, see below
python -m Scripts.scrape_pinnacle      # Pinnacle (launches Chrome via Selenium)
python -m Scripts.scrape_BOL           # BetOnline  -- SEE WARNING
python -m Scripts.scrape_espn_injuries # today's injury report + a dated snapshot
python -m Scripts.injury.review        # who needs a hand-written severity  <-- read this
#   ... edit config/injuries/<season>.yaml if it named anyone ...
python -m Scripts.refresh --all        # build the store, once
python -m Scripts.sync --push          # publish it to S3 -- the app reads from there
python populateGoogleSheet.py          # render the store to Sheets
```

Run from the repo root. Scrapers use `-m` because modules import as
`Scripts.<name>`.

**FantasyPros needs a logged-in session or it returns a tenth of the data.** Anonymously
it serves ten rows per position behind a registration fence -- 60 players, which is what
every board built before 2026-08-24 was blended on. A *free* account lifts it to 592 and
takes D/ST from ten teams to all thirty-two. The cookie lives in `config.yaml` under
`fantasypros.cookie`; see `config.example.yaml` for how to get it.

It expires -- the 2026 session runs to **22 November**, mid-season. An expired cookie
does not error, it silently returns the teaser, so `run_daily_refresh.sh` fails the run
if the scrape comes back at 60 rows or fewer. If that fires, log in again and replace
the cookie.

`Scripts.refresh` is **required before** Sheets and comes **after** the projection
scrapers. Both outputs read the same store, so they cannot disagree — but that
means the order is no longer optional: refresh before the scrapers and you bake
last week's lines in; skip refresh and Sheets has nothing to publish.

`Scripts.sync --push` goes **after** `refresh` and is what the app actually reads —
skip it and the Streamlit board shows the last thing that *was* pushed, with no
error, because a stale store in S3 is a perfectly valid store. `populateGoogleSheet.py`
reads local disk rather than S3, deliberately: it runs moments after the writer on
the same machine. The nightly `run_daily_refresh.sh` does the push for you.

Then read it:

```bash
streamlit run app/main.py
```

The sidebar shows the build time and turns red past an hour. A league that fails
to refresh keeps its previous store, so check the badge rather than assuming the
run succeeded — `refresh` also exits non-zero and names the failures.

> **BetOnline is currently broken.** `scrape_BOL.py` fails with
> `BetOnlineAccessError` — their API now requires a signed security header.
> Skip it; the pipeline still runs on ESPN + FantasyPros + Pinnacle. See
> `docs/STATE_OF_THE_REPO.md`.

`populateGoogleSheet.py` **reads the store**, so `Scripts.refresh` must have run
first. A league with no store is skipped with the command that would build it,
rather than aborting the run.

It sleeps 5s per sheet and 20s between leagues for Sheets rate limits — ~9.3
minutes of sleeping, which is now essentially the whole runtime. Cutting unused
tabs is the cheapest fix; see [plan 14](plans/14-thin-google-sheets.md).

To publish a subset:

```python
import populateGoogleSheet as p
p.run(['Knights_FFL', 'GOP_Degenerates'])        # or p.run(p.tommy)
p.run(p.john + p.will + p.cooleen + p.fields)    # everyone but you (~7 min)
```

Cohorts defined in the script: `all`, `tommy`, `john`, `will`, `cooleen`,
`fields`.

### Injuries: the weekly five minutes

**Order matters.** `scrape_espn_injuries` first, so the review reads today's report;
the file edit before `refresh`, because `refresh` is what bakes it into the boards.
Edit after refreshing and nothing happens until next week.

```bash
python -m Scripts.scrape_espn_injuries   # the nightly job does this; do it again if stale
python -m Scripts.injury.review          # prints the three lists below
```

The review prints three groups and you act on them in order:

| group | what to do |
|---|---|
| **Needs a severity** | Players inside ADP 150 whose reading came off the `news text` rung — a regex over one sentence, resolving to a body-part average. Rows marked `<-- worth a look` expect **more than one game** missed; the rest are camp knocks and need nothing. |
| **Stale** | Older than 28 days. Re-read the beat report, or delete. |
| **Expired** | The entry's own window has elapsed — "4–6 weeks from 18 August" is spent by early October. Confirm he is back and delete. |

Most weeks the answer is **nothing**. On the 2026-08-18 board all 22 flagged players were
half-game knocks, so the correct action was to close the file. That is the review working,
not the review failing.

To add one, append to `config/injuries/<season>.yaml`:

```yaml
  - espn_id: 4870808            # from the board's player_id -- preferred, it is stable
    name_key: JEREMIYAH LOVE    # fallback; joins on a normalised name, the fragile one
    player: Jeremiyah Love      # for humans, not used to join
    body_part: ankle_high       # must map to a Scripts.injury.lexicon group or load RAISES
    weeks_out: [4, 6]           # a range, because a range is what a report supports
    as_of: 2026-08-18           # required
    source: "beat report"       # required
    note: >-
      High ankle sprain; ESPN lists him Active with no returnDate.
```

Then:

```bash
python -m Scripts.refresh --all --what board   # in season, plain --all
python -m Scripts.injury.review                # confirm the row moved to the strong rung
```

**Why bother.** The fitted recovery curve was *rejected* as a multiplier — it gains ~1%
accuracy against a 2% bar — so nothing here discounts a projection automatically. A
hand-written severity does something the model cannot: it moves the player's **duration
bucket**, which is the strongest severity signal in the data. Love went from a 0.5-game camp
knock to a 4–6 week absence on one line.

**How often.** Measured on this repo's own episode table: about **2.7 new injuries a week**
among players who clear the materiality floor, of which **~1.2** cost three or more games.
That tail is the whole job — a pre-draft scan, then roughly one entry a week. See
[plan 27](plans/27-injury-model.md).

**What this file is not.** It is not availability. `Exp G` and the `USG_` scaling already
read ESPN's return date; this supplies *severity* where no feed carries it, and the
`Body Part`, `Wks Out`, `Form Cost` and `Re-inj` columns are what it feeds.

### Refitting after the season

The episode table and the fitted model are deliberately **not** in any recurring job —
they change only when a season of games lands, and rebuilding ten seasons every morning for
the same answer is waste. Once, after the season:

```bash
Rscript R/GetUsage.R <season> <season>
Rscript R/GetContext.R <season> <season>
Rscript R/GetAdvanced.R <season> <season>
python -m Scripts.injury.episodes --rebuild
python -m Scripts.injury.model --fit
python -m Scripts.injury.backtest --write
python -m Scripts.lab.report
python -m Scripts.sync --push
```

You do not have to remember: the board build compares the model's own `train_seasons`
against the season being projected and prints those commands when it is behind.

### The play-by-play archive

`R/GetPBP.R` runs **nightly for the current season** as part of
`run_daily_refresh.sh`, so there is nothing recurring to remember here. What is
worth knowing is when it does *not* cover you.

It writes six files per season under `Data/NFL/<season>/` — `pbp.parquet` plus
`participation`, `ftn_charting` and three `pfr_*` tables — and the last four start
later than play-by-play does:

| pull | from | why it matters |
|---|---|---|
| `pbp` | **1999** | every play, all 372 columns |
| `participation` | 2016 | who was on the field; upstream's own start |
| `pfr_pass` / `pfr_rush` / `pfr_rec` | 2018 | pressures, blitzes, yards before contact |
| `ftn_charting` | 2022 | manual charting |

A model trained from 2016 therefore sees pressure data for part of its window and
not the rest. That is a coverage fact to gate on, not a bug to fix.

Check what is on disk with:

```bash
python -c "
from Scripts.usage import nflverse as nv
for n in ('pbp','participation','ftn_charting','pfr_pass','pfr_rush','pfr_rec'):
    s = nv.pbp_seasons_available(range(1999, 2027), n)
    print(f'{n:<14} {len(s):>2} seasons  {min(s) if s else 0}-{max(s) if s else 0}')"
```

**Backfilling is a one-off**, and only needed on a fresh clone or a new machine —
completed seasons do not change:

```bash
Rscript R/GetPBP.R 1999 2025     # ~540 MB, ~20 minutes cold
```

The archive is unfiltered on disk — post-season included — and
`Scripts.usage.nflverse.load_pbp` filters to regular-season weeks 1–18 by default,
so every existing caller sees what it saw before. Pass `season_type=None` for the
rest.

Two consequences worth knowing. `R/GetAdvanced.R` now **reads this archive** instead
of re-downloading play-by-play, so run `GetPBP.R` first if you are running both. And
`Data/NFL` went from ~40 MB to ~540 MB, which is why `Scripts.sync --push` now skips
mirror files whose S3 object already has the same SHA-256 — without it the nightly
would upload half a gigabyte to arrive at identical objects.

### What used to be manual

`scrape_BOL.py` needed its `id_var` game-ID seed hand-edited before nearly every
run. That is now auto-discovered. To pin it:

```bash
BOL_FIRST_GAME_ID=259563 python -m Scripts.scrape_BOL
```

### Committing

**`Data/` is no longer tracked.** Since [plan 24](plans/24-s3-data-flow.md) the data
lives in S3, so a weekly commit is now code and docs only — which means a commit that
changes model behaviour is no longer buried in a diff of regenerated parquet. Say so
in the message anyway: several 2025 model changes are undiscoverable without reading
diffs, and that is the habit worth keeping rather than the tracked bytes.

Data reaches durability through `python -m Scripts.sync --push`, not through git. If
you want to know whether it got there, `--verify` answers it and exits non-zero if
not.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `FileNotFoundError: .../NFL_Schedules.csv` | Run `Rscript R/GetNFL.R` first. |
| `ModuleNotFoundError: No module named 'Scripts'` | Run from the repo root, and use `python -m Scripts.x` rather than `python Scripts/x.py`. |
| `ESPNAccessDenied` | Cookies expired. Refresh per `config.example.yaml`. |
| `BetOnlineAccessError` | Known, unresolved. Skip that scraper. |
| Scraper writes into last season's directory | `Data/NFL_Schedules.csv` is stale — regenerate for the new season. |
| Projections missing for a player | Name-join miss. Check `get_match_details()` output and add to the rename map. |
| `ValueError: covers multiple seasons` | The schedule CSV has more than one season; regenerate it for a single year. |
| App says "No store yet" | Nothing in S3 for that season. Run `python -m Scripts.refresh --all` then `python -m Scripts.sync --push`. To read the local copy instead, `ESPN_FFL_STORE_SOURCE=local`. |
| App errors naming `ESPN_FFL_STORE_SOURCE` | S3 is unreachable — no credentials, no network, or the bucket is denying you. Set `ESPN_FFL_STORE_SOURCE=local` to carry on off disk, or `auto` to fall back automatically. |
| Board in the app is older than the one you just built | `refresh` writes local; the app reads S3. Run `python -m Scripts.sync --push`. |
| `Unable to locate credentials` | No `~/.aws/credentials`. The bucket is `espn-ffl-data` in `us-east-2`; see [plan 24](plans/24-s3-data-flow.md). |
| `--verify` reports files that DIFFER | Local moved on since the last push. `python -m Scripts.sync --push` reconciles it. |
| **`Data/` emptied itself after a `git checkout`, and tests that read real data now fail** | You checked out a commit from before `42bd2c4` and came back. Those files were *tracked* then and are untracked now, so returning to a commit that deletes them from the index deletes them from the working tree too. Git is behaving correctly; it looks exactly like data loss. **`python -m Scripts.sync --pull` restores all of it** — this is the case S3-as-record exists for. Watch for `test_crosswalk`, `test_lab_g2` and `test_nfl_utils` failing together, which is the signature: they are the guards that read real data rather than fixtures. |
| A league is missing from the app's picker | Only leagues with a complete store are selectable; the sidebar lists the rest. Its last refresh failed, or it was never refreshed. |
| Freshness badge says stale | The store is over an hour old. Refresh from the sidebar or the CLI. It also reads stale when `built_at` is unparseable, which is deliberate — an unreadable build time is not evidence of freshness. |
| `MissingProjectionSourceWarning` during a refresh | That source has no weekly props file for the season. Expected pre-season and whenever a scraper is broken; the blend imputes those columns from the ESPN/FP mean and renormalises them out. The sidebar shows it. |
| Sidebar coverage shows a source at 0% | Same cause as above, measured rather than inferred. It is the number to watch when re-tuning blend weights. |
| Team coherence looks like phase 1 after merging plan 31 | Expected until the projections parquet is rebuilt. Phase 2's allocation needs `depth_rank`, which only reaches the parquet once `python -m Scripts.usage.project --season <y>` re-runs; until then `make_coherent` falls back to the phase 1 cap by design. `MODEL_VERSION` is unchanged, so that re-run **loads** the fitted model rather than refitting — it is cheap, and it does not move the model's own lines. Confirm with `usg_qb_allocated_starts` appearing on the board frame. |
| Nightly refresh refused, or the board is a day stale | Cron runs `~/bin/espn_ffl_nightly.sh`, which drives a **second checkout** at `~/GitRepos/ESPN_FFL-nightly` pinned to `origin/main` — so what you have checked out never affects it, and the boards rebuild nightly whatever branch you are on. A refusal names its cause: a missing `Data`/`config.yaml` symlink, an unreachable origin, or a checkout that does not match `origin/main`. Recreate the layout with the commands in `ops/espn_ffl_nightly.sh`. `ALLOW_ANY_BRANCH=1` overrides the revision check for a deliberate run. |
