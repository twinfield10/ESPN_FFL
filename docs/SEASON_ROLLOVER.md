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
Rscript R/GetNFL.R                  # refresh schedule (scores drive current week)
python -m Scripts.scrape_FP         # FantasyPros
python -m Scripts.scrape_pinnacle   # Pinnacle (launches Chrome via Selenium)
python -m Scripts.scrape_BOL        # BetOnline  -- SEE WARNING
python -m Scripts.refresh --all     # build the store, once
python -m Scripts.sync --push       # publish it to S3 -- the app reads from there
python populateGoogleSheet.py       # render the store to Sheets
```

Run from the repo root. Scrapers use `-m` because modules import as
`Scripts.<name>`.

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
