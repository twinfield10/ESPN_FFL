# 46 — Every source joins by name, and three maps disagreed about how

**Status:** COMPLETE

**Priority:** High · **Effort:** S · **Where it stands:** Built and measured
2026-09-09. **24 live misses closed, the largest a 302-point starting quarterback**
· both hand rename maps deleted · `python -m Scripts.name_audit` is the standing
check
**Depends on:** [03](03-projection-source-coverage.md) — provenance flags and
renormalisation, and the failure mode this is another face of ·
[44](44-weekly-sources-and-coverage.md) — the coverage panel this corrects one row of
**Feeds:** the rollover runbook's step 5, which was "curate the hardcoded maps" and
is now a command

> **A name that fails to join does not fail loudly.** The player abstains,
> `impute_columns` fills his line from the ESPN/FantasyPros mean, `compute_weighted_stats`
> renormalises over the sources that did match, and the board shows a book agreeing
> with ESPN about a player it never priced. Plan 03's oldest failure mode — an absent
> source reading as agreement — reached in through the join key rather than the file.

## The question that started it

*"For GOP_Degenerates, I see ESPN has 96% coverage. Why is that? It should be 100%."*

It should, and the 96.5% was a defect in the panel rather than in ESPN — but the
sweep that question prompted found a much larger one underneath it.

## What was actually broken

Measured on the ten 2026 stores and every source file on 2026-09-09.

| | Finding |
|---|---|
| 1 | **Three alias maps, no shared authority, two grains that join differently.** The season loaders key on `normalise_name` — suffixes, punctuation and accents stripped. `clean_lineups` merges every weekly source `on=['week', 'player_name']`, the **raw string**. So `James Cook` against ESPN's `James Cook III` is free on one path and a silent abstention on the other, and the fix that had accreted was a `name_changes` dict inside `clean_pinny` (9 entries) and another inside `clean_bol` (13). FantasyPros had none at all. |
| 2 | **20 of those 22 entries were no-ops.** Pure suffix or punctuation differences `normalise_name` already collapses. Four of `clean_pinny`'s nine were *duplicate keys in the same dict literal*, and five of its nine keys no longer appeared in the source file. |
| 3 | **Two of the 22 were live defects — they created the miss they were written to fix.** `Deebo Samuel Sr. -> Deebo Samuel` and `Oronde Gadsden -> Oronde Gadsden II` both renamed BetOnline's name *away* from what ESPN now calls the player. 9.1 and 4.8 projected points that week, both reading as ESPN/FP agreement. A map curated by hand goes stale in whichever direction the upstream moved. |
| 4 | **The two entries that did real work were in the wrong place.** `Cameron Ward -> Cam Ward` sat in `clean_bol` and nothing propagated it to the season path — so **Pinnacle's season line for a 302-point starting quarterback came from the ESPN/FantasyPros mean on all nine draft boards**, from the day the file landed. `Zonovan Knight -> Bam Knight` the same. This is the argument for one audit over both grains rather than a map per loader. |
| 5 | **Pinnacle's weekly loader was missing four players BetOnline's had.** `James Cook` (16.8 projected points), `Luther Burden` (12.3), `Kenneth Gainwell` (11.5), `Brian Thomas` (10.3) — all four with `PINNY_*_is_imputed` True and `BOL_*` real on the same row, which is what makes the diagnosis unambiguous. |
| 6 | **BetOnline's season file loses part of the name and `_TEAM_TAIL` only ran on the other branch.** `_recover_player` strips a trailing team abbreviation, but it is only reached when `stat_short` is *unrecognised* — and `YDS_REC` is recognised, so `AJ BARNER SEA` went through with the tail on and a 116-point tight end's receiving-yards line was discarded. Five more are the scrape truncating the name outright: `DAL PRESCOTT` (Dak, 395 points, two props), `CHRISTIAN` (McCaffrey's receptions), `AKHEEM`, `KELDRIC`. |
| 7 | **And the ESPN coverage row.** `source_contributed`'s non-zero clause is right for the Cameron Dicker case it was written for — a kicker's `FP_passingYards` is 0.0 and unflagged because nobody imputed it and nobody asserted it either. It is wrong for the root source: **ESPN publishes `0.0` for an inactive or bye player, and that is an assertion.** The sidebar read **ESPN 92–99.5%** across the ten leagues, and every uncounted row in every league was `player_active_status` `bye` or `inactive`. `coverage_report` had always answered 100% for ESPN, so the panel's two views disagreed about the one source that cannot go dark. |

## The fix, and why it is one function rather than eleven aliases

The obvious reading of findings 1–5 is "add the missing entries." That is what the
last four rounds did, and it is why there were 22 entries of which two were wrong.

`align_to_espn_names` instead rewrites each weekly source's `player_name` to ESPN's
spelling of that player, using **the ESPN frame from the build in progress** as the
authority. The join stays a raw-string merge, so nothing downstream changes shape;
what changes is that a suffix ESPN adds mid-season cannot leave a map stale, because
there is no map. Both `name_changes` dicts are deleted, and
`tests/test_name_audit.py::test_the_hand_rename_maps_are_gone` fails if one comes back.

Two details carry the correctness:

**An ambiguous key is left alone rather than guessed.** Stripping suffixes collapses
five pairs of genuinely different 2026 players onto one key — `Byron Murphy II` the
tackle and `Byron Murphy Jr.` the cornerback, `Michael Carter` and `Michael Carter II`,
and three more. Attaching a real line to the wrong player is worse than the abstention
this exists to remove.

**Alignment can create a duplicate, so it de-duplicates after.** FantasyPros' weekly
file carries `Mitch Tinsley` *and* `Mitchell Tinsley` as separate rows; aligned and
left alone they would each match the one ESPN row and double it. This is the reason
switching the weekly merge to `name_key` outright is not the smaller change it looks
like.

The residue `normalise_name` cannot bridge — nicknames, typos, mangled scrapes — goes
in `NAME_ALIASES`, which is now the repo's only player-name alias map. Sixteen entries
added, all measured, the largest being `CAMERON WARD`.

## `Scripts/name_audit.py`

The standing check, and the part worth keeping. It resolves every source's names
against the ESPN universe from the built stores — no live connection, so it is safe in
the nightly — and **classifies each miss** rather than listing it. That is the whole
value: the raw miss counts are 258 for FantasyPros weekly and 184 for BetOnline, and
almost all of them are simply players outside the league.

| verdict | meaning |
|---|---|
| `alias:suffix` / `alias:nickname` / `alias:team-tail` / `truncated` | **to fix.** Each rests on a mechanical rule — a suffix, a first name that is a prefix or one typo away, a team abbreviation, a first name unique on its team — so the target can go straight into an alias map |
| `review:surname` / `review:ambiguous` | **to read first.** A named candidate, not an assertion |
| `upstream:scrape` | The scrape mangled the name, so no alias can fix it. Reported with its cost and the file that owns it, and **excluded from the exit code** — a check that cannot go green is a check nobody keeps |
| `unrostered` | ESPN has this player, no league's weekly frame does. A source with a wider slate. Not a defect |
| `absent` | Not in the ESPN universe at all — practice-squad and camp bodies a book priced |

**A shared surname is not evidence, and that is the finding the review bucket exists
for.** An earlier cut reported those as aliases and **got eight of nine wrong**: `Tru
Edwards` is a real receiver and not T.J. Edwards; `Cash Jones`, `Alex Bullock`, `EJ
Smith` and `Josh Kelly` are all real players who merely share a surname with somebody
ESPN lists. No similarity threshold separates them from `DAL PRESCOTT` → `DAK
PRESCOTT`, because a short first name beside a long shared surname scores high either
way. Nor does a ratio work on first names: `JOSH`/`JOHN` and `CHIQ`/`CHIG` both score
0.75 on `SequenceMatcher`, and one is two people. So the confident rule is *one
substituted character or one adjacent transposition* — what a typo actually is — and
everything else is a suggestion.

Reviewed rejections go in `CONFIRMED_DISTINCT` with the date and the reason, so the
bucket empties. A recurring check that reports the same nine rows forever is a check
that stops being read, which is the failure mode the module's own docstring warns
about.

## What landed

* `Scripts/name_audit.py` — the audit, `python -m Scripts.name_audit`, non-zero exit
  when anything needs a fix. `--all` lists the non-actionable verdicts; `--maps` is the
  regression guard on hand rename maps.
* `projection_utils.align_to_espn_names`, called on all three weekly sources before
  their merges. Both `name_changes` dicts deleted.
* 16 entries in `NAME_ALIASES`, which is now the only player-name alias map in the
  repo.
* `season_projections._drop_team_tail`, applied on the recognised-`stat_short` branch
  of `normalise_bol_props` too.
* `source_contributed(..., zero_is_real=)` and `player_coverage(..., root=)`. ESPN
  reads **100.0% in all ten leagues**, by construction and as the baseline the other
  rows are read against. The other sources are unchanged by it.
* 31 tests in `tests/test_name_audit.py`, two in `tests/test_projection_utils.py`.
* Rollover runbook step 5, which was "curate the hardcoded maps" and is now a command.

## Measured

Names reaching an exact ESPN join, before and after alignment, against the union of
all ten leagues' weekly frames:

| source | before | after |
|---|---|---|
| FantasyPros weekly | 339 | **340** |
| Pinnacle weekly | 164 | **171** |
| BetOnline weekly | 264 | **275** |

No source lost a name it already joined, and no frame gained or lost a row. The audit
now reports **0 to fix** on all three weekly sources, on Pinnacle season, on
FantasyPros season and on both Athletic files.

## What is left, and it is upstream

BetOnline's season scrape truncates a player to a bare first name — `CHRISTIAN`,
`AKHEEM`, `KELDRIC` on the 2026 file — and that is a defect in
`R/GetSeasonProps.R`, not a spelling. Two of the three resolve uniquely by team and
the audit reports them; `CHRISTIAN` does not, because the 2026 boards put Christian
Kirk on San Francisco alongside Christian McCaffrey. **`NAME_ALIASES` deliberately
carries no entry for any of them:** a key of `CHRISTIAN` would match whoever the
scrape truncated next week, which is the one way an alias map can do real damage.

The stored `PINNY`/`BOL`/`FP` coverage figures in `meta.json` predate this and will
rise on the next `Scripts.refresh` — the ESPN row is the only one that moves from the
code change alone.
