# 41 — Freeze the season projections when the drafts finish

**Status:** IN PROGRESS

**Priority:** High (seasonal) · **Effort:** S · **Where it stands:** **Built
2026-09-07.** `python -m Scripts.freeze` writes a `board_frozen` artifact and stamps
`frozen_at`; the Rundown reads it where it exists and says which basis it used.
**What is owed is running it** — after the last draft (Tue 2026-09-08, ~22:00) and
before Wednesday's first game. Nothing is frozen yet.
**Depends on:** [15 (draft board)](15-draft-board.md) ·
[24 (S3 as system of record)](24-s3-data-flow.md) ·
**Feeds:** [40 (frontend restructure)](40-frontend-restructure.md)

---

## Problem

`board.parquet` is rebuilt every morning at 06:00 by `run_daily_refresh.sh`, and that
is correct: ADP moves, injuries land, and a board you draft off has to be current.

But it means the artifact stops being able to answer the one question a draft grade
*is*: **what did this roster look like on the day it was assembled?** By Thursday the
board is a rest-of-season instrument. Grading a September draft against a December
board measures who got lucky, not who drafted well — and it would keep silently
re-grading it every morning, so the same roster would carry a different verdict each
day with nothing on screen to say why.

## The deadline, and the trap

**After the last draft and before the first game.** For 2026:

| | |
|---|---|
| Last draft | Tue 2026-09-08, John_ATL 20:30 |
| First game | Wed 2026-09-09 |
| **Freeze** | **Tuesday night, after the last draft finishes** |

The trap is the nightly. Freezing "Wednesday morning" freezes a board that already
rebuilt at 06:00 with Wednesday's news in it — so the window is the night the last
draft ends, not the morning after. `--refreeze` exists for having missed it, not as
the normal path.

This has the same character as [SEASON_ROLLOVER](../SEASON_ROLLOVER.md) step 0, the G2
archive: a deadline rather than an ordering, and one you cannot go back for.

## What was built

**`python -m Scripts.freeze --all`** (also `--league`). Copies each league's
`board.parquet` to `board_frozen.parquet` and stamps `frozen_at`, `frozen_git_sha` and
`frozen_picks` into `meta.json`.

**Registered as a real artifact.** `"board_frozen"` is in `Scripts.store.ARTIFACTS`, so
`sync --push` carries it and `sync --verify` checks it with no new code, and it inherits
the store's invariants: parquet written atomically via `.tmp` + `os.replace`,
`meta.json` written last.

**Idempotent by refusal.** A league already frozen is skipped unless `--refreeze`.
Freezing twice would overwrite the very board the first freeze existed to protect.

**It refuses a league that has not drafted** unless `--allow-undrafted`. A pre-draft
board is the one thing there is no point freezing: it projects a roster nobody owns
yet. Seven of ten leagues were in exactly that state the day this shipped, so the
refusal is the common path for `--all` rather than an error case — which is why skips
are reported per league and the exit code treats "already frozen" as success.

**`app/store.draft_basis`** returns `(frame, "frozen" | "live")`. The label is returned
rather than inferred by the caller so a page cannot show one basis and read the other,
and the Rundown prints which one it used **every time** — including the live case,
where it says the numbers will drift and names the command that stops them.

## Why not the S3 snapshot

`Scripts.sync` already publishes a dated board snapshot every night
([plan 24](24-s3-data-flow.md) calls it the unplanned win), and reading the right date
back out of it was the obvious alternative. Two reasons against:

- The app must keep working under `ESPN_FFL_STORE_SOURCE=local`, and a snapshot-backed
  rundown would not.
- A page that has to know a snapshot key layout is a page coupled to the bucket. A
  store artifact is read by `app/store` like any other.

The snapshot is still the backstop if a freeze is missed entirely: the board as it
stood on any given date is in the bucket, and `--refreeze` from a restored copy would
reconstruct it.

## Run it like this

```bash
# Tuesday night, once John_ATL is done
python -m Scripts.freeze --all --season 2026
python -m Scripts.sync --push --what store
python -m Scripts.sync --verify --what store
```

Verify the push rather than assuming it, for the reason step 0 of the rollover gives
about the G2 archive: this is an artifact with no second chance once the nightly has
run.

## What is left

- **Run it.** Nothing is frozen. `Scripts.freeze --all` will currently freeze the three
  leagues with recorded 2026 picks and skip the rest, so it wants running *after* the
  drafts and after a `--what draft` pull for the leagues that have just finished — a
  finished draft is not part of the nightly, because a finished draft never changes.
- **Add it to the rollover runbook** as a dated step beside step 0 and step 9.
- **`frozen_at` is not surfaced outside the Rundown.** The sidebar says how fresh the
  *live* store is; it says nothing about whether the frozen basis exists. A league
  frozen at the wrong moment is currently only visible by reading the caption.
- **Nothing re-freezes on a corrected board.** If a projection bug is found and boards
  are rebuilt, the frozen copy is deliberately stale — which is right — but there is no
  record linking a frozen board to the `git_sha` that would let you decide whether to
  redo it. `frozen_git_sha` is stamped for exactly this and is not yet read by anything.
