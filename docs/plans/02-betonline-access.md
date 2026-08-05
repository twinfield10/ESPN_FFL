# 02 — BetOnline weekly props are blocked

**Priority:** High · **Effort:** Medium · **Status:** Resolved (2026-08-03)

> **Outcome: option D, plus a correction.** The weekly block is real and confirmed
> from two HTTP clients. The season-long endpoint works and carries the IDP props,
> so it is now the pre-season/draft source: 546 props over 273 players and 32
> teams, 123 of them defensive. See [Resolution](#resolution).

## Problem

`Scripts/scrape_BOL.py` can no longer reach BetOnline's weekly markets API. It
returns `403 invalid_security_headers` on every request.

This removes one of four projection sources from the weekly pipeline. BetOnline
carries 10–40% of the blend weight depending on the stat, and it is the **only**
source with defensive/IDP stats — so GOP Degenerates is hit hardest.

## Evidence

```
GET https://bv2-us.digitalsportstech.com/api/dfm/marketsBySs?sb=betonline&gameId=...
→ 403 {"statusCode":403,"message":"invalid_security_headers","error":"Forbidden"}
```

Tested and still 403 with: bare request; browser User-Agent; adding
`Origin`/`Referer`; adding the `gsetting: bolsassite` and `utc-offset` headers
that the R scraper uses successfully elsewhere. The endpoint wants a signed
header the scraper doesn't produce.

This is an anti-bot control. **Don't try to forge it** — that's both fragile and
the wrong side of their terms.

### What still works

The **season-long** props endpoint is a different host and is fine:

```
POST https://api-offering.betonline.ag/api/offering/Sports/get-contests-by-contest-type2
     header: gsetting: bolsassite
→ 200, and already serving 2026 data ("NFL 2026 Regular Season", 09/13/2026)
```

That's the one `R/GetSeasonProps.R` uses, and the one the **draft board** needs.
So the draft work is unblocked; only the weekly in-season blend is affected.

The scraper now fails with `BetOnlineAccessError` carrying this explanation
rather than a bare 403 traceback.

## Options

**A. Drop BetOnline and re-weight (lowest effort).**
Redistribute BOL's weight across ESPN/FantasyPros/Pinnacle. Honest, immediate.
Costs the IDP stats entirely — GOP Degenerates would lose defensive projections
unless another source is found. Weights live in `clean_lineups()` in
`Scripts/projection_utils.py`.

**B. Drive it through a real browser (medium effort).**
Pinnacle already works this way via Selenium. A browser session obtains whatever
the API wants naturally. Slower and more brittle, but reuses a pattern already
in the repo and preserves IDP coverage.

**C. Substitute another book (medium effort).**
Any book with player props would do. Worth checking whether Pinnacle exposes the
defensive props BOL was providing — if so, extending the existing Pinnacle
scraper is cheaper than a new integration.

**D. Use season props as a weekly fallback (low effort, low fidelity).**
Divide season-long projections by remaining games. Crude, ignores matchup and
injury, but better than nothing for IDP and it reuses a source already working.

## Recommendation

**A now, C to investigate.** Get the weekly pipeline honest before week 1 by
dropping BOL and re-weighting; in parallel check whether Pinnacle can cover the
defensive props. B is the fallback if IDP coverage turns out to matter more than
expected.

Whichever way it goes, re-tune the remaining weights against 2025 actuals rather
than just rescaling the current hand-set numbers — see plan 03.

## Resolution

### The weekly block is real

Re-tested 2026-08-03 from **two** HTTP clients, since a 403 can be a client
fingerprint rather than a policy. `bv2-us.digitalsportstech.com` returns
`403 invalid_security_headers` from Python *and* from R/libcurl, across bare
request / UA only / UA+`gsetting` / full browser-ish headers. It genuinely wants a
signed header. Not forging it.

### The season endpoint works — and my first test of it was wrong

Worth recording because it nearly cost a working source. Python `requests` gets a
403 from `api-offering.betonline.ag` with a 544 KB WAF error page. R `httr` with
the same headers and body gets **200 and 60 KB of 2026 props**. The endpoint is
client-sensitive — most likely TLS fingerprinting — so:

**The BetOnline season fetch must stay in R.** Porting it to Python `requests`
will silently look like the source is dead. If it ever has to move, it needs a
TLS-impersonating client rather than `requests`.

### The existing R script threw the IDP props away

`R/GetSeasonProps.R` parsed `SK_DEF` / `INT_DEF` / `TKL_DEF` correctly and then
dropped them: its wide pivot selects only `ends_with("_PASS"|"_RUSH"|"_REC")`, and
the caller appended `filter(pos != 'DEF')`. Its `PPR_PTS` formula also scores
sacks, interceptions and tackles at 0. So the one source with IDP coverage was
discarding exactly that.

Three further problems in the same script, all of which meant it could never have
run headlessly:

- `write_csv` was called without `library(readr)` — the `tidyverse` import is
  commented out, so under `Rscript` it failed with "could not find function".
  It only ever worked inside an interactive session that had loaded tidyverse.
- Output was `write_csv(bol_clean, '2025_BetOnlineProps_Offense.csv')` — season
  hardcoded, and a bare relative path, so it landed wherever the interpreter was.
- Two stray top-level expressions left from an interactive session
  (`get_bol_raw(team='pittsburgh-steelers')`, which passes the wrong argument
  name, and a bare `pmap_dfr()`) sat *after* the write and errored, so a
  successful scrape still exited non-zero.

Now: `Rscript R/GetSeasonProps.R 2026` takes a season argument, resolves paths
against the repo root, writes season-scoped output, and exits 0.

```
Data/Projections/BetOnline/Season/2026/
    BetOnline_SeasonProps_All.csv       # long, every prop, offence + defence
    BetOnline_SeasonProps_Offense.csv   # wide, unchanged shape for existing readers
```

The long file is the one to read: a new stat type arrives as extra rows rather
than a schema change, and nothing is dropped in transit.

The unrelated LowVig game-line scrape at the tail is now behind `LOWVIG=1`. It ran
unconditionally, so a LowVig outage failed the run after the props were written.

### Pinnacle needed no browser

Checked while resolving this. Pinnacle's guest API serves 76 season-long player
props as plain JSON, so `Scripts/scrape_pinnacle_season.py` fetches them in two
requests with no Selenium. The DOM scraper in `Scripts/scrape_pinnacle.py` is
timing out on a selector to obtain data that is already available as JSON — see
plan 03 step 5.

## Verification

- `Rscript R/GetSeasonProps.R 2026` → 546 props, 273 players, 32 teams, 123
  defensive; exit 0.
- `python -m Scripts.scrape_pinnacle_season --season 2026` → 76 props, every stat
  wording mapped, no-vig probabilities summing to 1.
- Weekly `scrape_BOL` still fails with `BetOnlineAccessError` and its explanation.
- Blend weights no longer need to sum to 1.0: renormalisation divides by whichever
  subset of sources is real, so they are meaningful in proportion only (plan 03).
