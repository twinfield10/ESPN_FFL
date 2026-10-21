# 53 — Fetching The Athletic's weekly slate instead of downloading it

**Status:** IN PROGRESS

**Where it stands:** `Scripts/fetch_athletic.py` is built and tested (2026-10-21). The
sitemap hop, the download and the import all work live. **The browser hop is
unreliable.** The article loaded in a visible window once, then returned 403 to every
kind of client after about six automated loads in an hour (see
[What happened on the first live run](#what-happened-on-the-first-live-run)). So
`--href` is the supported path today: copy the link and the rest is automatic. Whether
the browser hop works at one load a day from the Mac mini is G0, still unanswered. The LaunchAgent and wrapper are in `ops/`, and the install steps are in `docs/SEASON_ROLLOVER.md` under "The Athletic fetch, on the Mac mini". `--status` keeps the last 60 run outcomes, so the week of G0 runs can be read with one command.

**What it is:** [Plan 47](47-athletic-weekly.md) made The Athletic the fifth weekly vote,
but the slate still comes from a hand download. `docs/SEASON_ROLLOVER.md` describes it as
"a paid .xlsx with no API behind it, so nothing can fetch it for you." That turns out to
be only half true. There is no API, but the file itself is public, and the only thing
that needs a browser is finding the link to it. `Scripts/fetch_athletic.py` would find
the article, read the link, download the file and hand it to the existing
`load_athletic --what weekly`. The parse, crosswalk and blend don't change.

---

## What was measured before designing

**The file is public.** Week 7's link is
`https://static01.nyt.com/athletic/uploads/wp/2026/10/21103756/Week7_Proj_1021.xlsx`.
A plain `curl` with no cookies returns it (200, 30,663 bytes, xlsx content type), and its
SHA-256 matches the copy downloaded by hand and imported that morning
(`0fb8fa0a…d920c8`).

**The URL cannot be guessed.** `21103756` is an upload timestamp (day 21, 10:37:56).
Nothing lists the uploads publicly either: the sitemap index holds only `sitemap-pt-post-*`
article maps plus `misc`, `externals` and `authors`, there are no `.xlsx` entries in the
October map, and `/athletic/wp-json/wp/v2/media` redirects instead of answering. The
article page is the only place the link appears.

**Finding the article doesn't need a browser.** The monthly article sitemap
`https://www.nytimes.com/athletic/sitemap-pt-post-YYYY-MM.xml` is public and returns
200 to plain HTTP. The slug isn't a fixed template:

| Week | Slug |
|---|---|
| 1 | `fantasy-football-rankings-week-1-sleepers-starts-projections` |
| 2 | `fantasy-football-week-2-rankings-start-sit-drake-london-romeo-doubs` |
| 3–6 | `nfl-week-N-fantasy-football-rankings-starts-projections-…` |
| 7 | `fantasy-football-week-7-rankings-michael-wilson-colston-loveland` |

All seven do contain `fantasy-football`, `rankings` and `week-N`. That rule, **excluding
slugs that contain `matchup`**, matches exactly one article per week for weeks 1–7 in the
September and October maps. The excluded ones are the `team-matchup-rankings-week-N` and
`schedule-strength-matchup-rankings-week-N` articles.

**The sitemap date is a last-modified date, not a publish date.** Week 4's article is
dated 10/04, but it was downloaded on 10/01. So match on the week in the slug and ignore
the date. A week's article can also show up in the following month's map, so read both
the current month and the previous one.

**The article page blocks scripts, but not a visible browser.** Fetched logged out:

| Client | Result |
|---|---|
| `curl` | 403, 771 bytes |
| Playwright Chromium, headless | 403, empty body |
| Playwright Chromium, visible window | 200. The paywall shows, but the link is in the page as text `Projections Download Link` → the `static01` URL above |

**So no subscription login is needed.** That removes the saved profile, session expiry
and the "sign in again over Screen Sharing" step. What it adds is that **the browser must
open a real window**. This is ordinary browser use and not detection evasion. The design
doesn't spoof the user agent, use stealth plugins, or tweak the headless mode to get past
the block. If a visible window stops working, the fallback is the hand download.

### What happened on the first live run

The table above was a single reading. The first `--dry-run`s a few minutes later, for
weeks 6 and 3, got **403 in a visible window**, and so did a rerun of the exact probe
that had passed. Counting the curl and headless attempts, that was about six automated
article loads from one IP in under an hour. The likely reading is that NYT's bot
protection flags a client by behaviour, so one clean load doesn't prove much about the
next.

**The design doesn't escalate.** No stealth plugin, no real-Chrome profile to look more
human, no IP rotation, no retry loop. The site has said no to automated readers, and
working around that is detection evasion. A 403 is recorded and reported, and the run
stops.

Two other machine-readable channels were checked and neither carries the link:

- **The author RSS feed** (`/athletic/rss/author/jake-ciely/`, public, 200) lists the
  week-7 article, but each item is a 118-character teaser with no body.
- **Newsletter email.** The Gmail account Claude can reach is the work one and has no
  Athletic mail. Whether the subscription's own newsletter includes the link is
  unknown, and would be a clean source if it does.

So **hop 2 has a manual alternative**: `--href <link>` skips the sitemap and the
browser, holds the link to the same `static01.nyt.com` + `.xlsx` rule, and runs hops 3
onward unchanged. Verified live on week 7: it downloaded, imported 178 rows (replacing
the same 178), recorded the link, and skipped on the second run.

The schedule-derived week depends on the machine. The MacBook's
`NFL_Schedules.csv` is from 10/17, so it resolves week 6 on 10/21. The mini refreshes
its copy every night. Pass `--week` on anything that isn't the mini.

The author's own note on the article says the slate is "updated Wednesday and before
kickoff on game days." The date shown next to the link ("Oct. 21") isn't part of the
link text, so it isn't used. A re-upload gets a new timestamp folder, so **a changed URL
is the update signal**.

---

## The design

```
sitemap (plain HTTP) ─▶ article URL for week N
        │
        ▼
Chromium, visible window, logged out ─▶ read the href of the "Projections Download Link" link
        │                              (nothing is clicked)
        ▼
static01 URL (plain HTTP) ─▶ Landing/<season>/<filename> ─▶ load_athletic --what weekly
```

1. **Week.** `Scripts.nfl_utils.current_week()`, overridable with `--week`.
2. **Article.** Read the current and previous month's post sitemaps and apply the slug
   rule above. Zero matches means it's not published yet: exit 0 with a status of
   `not_published`. Two or more matches means the rule has broken: exit non-zero and
   name every match. Never guess.
3. **Link.** Open the article in Playwright's bundled Chromium with `headless=False`,
   wait for the page to load, and select the `a` element whose text matches
   `/Projections Download Link/i` and whose href is on `static01.nyt.com` and ends in
   `.xlsx`. Exactly one is expected. Anything else is a failure and is logged with the
   page title, so a 403 can be told apart from a change in the article's layout.
4. **Skip when nothing changed.** The last imported URL for each week is kept in
   `Data/Projections/TheAthletic/fetch_status.json`. If the URL is the same, skip the
   download. Checking several times a day is then one sitemap read and one page load.
5. **Download.** Plain `requests.get` of the `static01` URL into a new, empty temporary
   directory. Check the content type and the zip/xlsx signature, then move it to
   `Landing/<season>/` under its own filename (`Week7_Proj_1021.xlsx`), which is
   already the naming convention there.
6. **Import.** Call `load_athletic.build_weekly(path, season, week)` in-process, the
   same call `--what weekly --file` makes. The sheet name already confirms the week.
   Newest-wins replacement is plan 47's rule and is unchanged.
7. **Status.** Write `fetch_status.json` with `{week, article, href, sha256,
   imported_at, outcome}`. `refresh_status`'s 8-day `ATH weekly` clock stays as the
   backstop alert. If the fetch fails quietly, that clock still goes red.

The browser step is the only part that's hard to test, so it stays small. Steps 2, 4, 5
and 7 are pure functions with fixture sitemaps and fixture HTML under `tests/`, and
`fetch_athletic --dry-run` prints the article and href without downloading anything.

---

## Running on the Mac mini

The nightly runs on the mini. Interactive work happens on the MacBook. Everything in this
section is done **on the mini, over SSH** (`ssh mini.local`).

**A visible browser window needs a logged-in desktop session.** A job started by `cron`
runs outside the desktop session, and a visible Chromium window launched from it isn't
guaranteed to open. A **LaunchAgent** in the `gui/<uid>` domain runs inside the session
and is the reliable choice. It also runs a missed time slot when the machine wakes,
which cron doesn't. Because of this, the fetch gets its own LaunchAgent, scheduled ahead
of the 06:00 cron nightly. The nightly itself is left alone. Moving it to launchd is a
separate decision.

The mini needs:

- **The user logged in at the desktop.** FileVault is on, so automatic login isn't
  available, and it isn't needed either: after a reboot nothing runs until the
  FileVault password is typed, and typing it logs the user in. See the runbook.
- **Playwright's Chromium** for `/usr/local/bin/python3.11`, the interpreter
  `run_daily_refresh.sh` pins. It was already installed on 2026-10-21.
- `ops/com.espn_ffl.athletic_fetch.plist`, copied to `~/Library/LaunchAgents/` and
  bootstrapped into `gui/501`, running `ops/espn_ffl_athletic_fetch.sh` from `~/bin`
  inside the `ESPN_FFL-nightly` worktree. Installed over SSH on 2026-10-21. The
  launchd mechanics worked: the job ran in the session, resolved week 7 from the mini's
  current schedule, and the browser opened with the display asleep. The page returned
  403, but that was about 30 minutes after the MacBook's burst from the same public
  IP, so it isn't counted. G0's first run that counts is 2026-10-22 05:45.

**The schedule (open, see below):** daily at 05:45, ahead of the nightly, which picks up
Wednesday's upload on Thursday morning before the Thursday night game. Game-day pulls
come on top of that.

---

## Gates

- **G0 — headed fetch works on the mini, as scheduled, at one load a day.**
  The scheduled 05:45 run, with the display asleep and nothing else loading the site
  from the home IP that day. Each run must read the week's href, so `--status` shows
  `unchanged` or `imported` and never `failed`. For week 7 the file's SHA-256 is
  `0fb8fa0a…d920c8`. It must pass from the LaunchAgent, not from a Terminal window, and
  keep passing for a week of daily runs. Given the 403s on 2026-10-21, a single pass
  doesn't count. **If it fails, the browser hop is retired** and `--href` is the
  routine: the LaunchAgent and the browser code are deleted rather than left to fail
  nightly.
- **G1 — three weeks with no hand download.** Weeks 8–10 are imported by the fetch
  alone, and each week's `Landing/` file matches what a hand download of the same link
  would have been. One week is checked by actually downloading it by hand.
- **G2 — a mid-week update is caught.** At least one week where the href changes after
  the Wednesday import, and the later fetch imports it and logs the replaced row count.

---

## Open decisions

1. **What a game-day pull triggers.** A new file only reaches the board after
   `refresh --what lineups`, `sync --push` and `populateGoogleSheet.py`. A Sunday 11:00
   fetch without them changes nothing anyone sees. The options are a shortened
   "lineups-only" run after a successful import, or no game-day pulls at all. This
   depends on how long the lineups-only half of the nightly takes on the mini, which
   hasn't been measured.
2. **Where `--push` runs.** The push sends the whole `archive` tier
   (`Projections`, `Scoring`, `Injuries`, `G2`). From any copy of `Data/` other than
   the mini's, that would overwrite the mini's newer files with stale ones. So `--push`
   is opt-in and only the mini passes it. The nightly's own `sync --push` already covers
   `Projections/`. The fetch's push adds durability for the raw download and covers
   game-day runs outside the nightly.
3. **Failure notification.** The nightly already posts a macOS notification through
   `osascript` when it fails. The fetch could do the same on the mini. That only helps
   if someone is looking at the mini, so the app-side `ATH weekly` clock is the alert
   that actually gets seen.

---

## What changes on ship

- `docs/SEASON_ROLLOVER.md`: the weekly-routine line becomes "the fetch did it;
  `python -m Scripts.fetch_athletic --status` shows the last import." The hand-download
  command stays as the fallback, and the "nothing can fetch it for you" wording is
  corrected.
- `README.md`: one line under the sources section.
- `requirements.txt`: `playwright` is already a dependency (Pinnacle, BetOnline). No
  new packages.
