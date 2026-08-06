# Runbooks

Reconstructed from git history and verified against the code. Neither of these
was written down before; the 2024→2025 rollover was a single 30-file commit
(`b6576e0`) and reading that diff was the only way to know what it involved.

---

## Annual rollover

Do this once, before the season starts. Roughly 30 minutes.

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
python populateGoogleSheet.py       # render the store to Sheets
```

Run from the repo root. Scrapers use `-m` because modules import as
`Scripts.<name>`.

`Scripts.refresh` is **required before** Sheets and comes **after** the projection
scrapers. Both outputs read the same store, so they cannot disagree — but that
means the order is no longer optional: refresh before the scrapers and you bake
last week's lines in; skip refresh and Sheets has nothing to publish.

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

Weekly commits are mostly regenerated data. When a commit also changes model
behaviour — blend weights, no-vig formulas, scoring — say so in the message.
Several 2025 model changes are now undiscoverable without reading diffs.

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
| App says "No store yet" | Nothing in `Data/Store`. Run `python -m Scripts.refresh --all`. |
| A league is missing from the app's picker | Only leagues with a complete store are selectable; the sidebar lists the rest. Its last refresh failed, or it was never refreshed. |
| Freshness badge says stale | The store is over an hour old. Refresh from the sidebar or the CLI. It also reads stale when `built_at` is unparseable, which is deliberate — an unreadable build time is not evidence of freshness. |
| `MissingProjectionSourceWarning` during a refresh | That source has no weekly props file for the season. Expected pre-season and whenever a scraper is broken; the blend imputes those columns from the ESPN/FP mean and renormalises them out. The sidebar shows it. |
| Sidebar coverage shows a source at 0% | Same cause as above, measured rather than inferred. It is the number to watch when re-tuning blend weights. |
