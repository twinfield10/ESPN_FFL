# Draft readiness — 2026

**Assessed 2026-08-24.** A countdown, not a status report: what is ready, what is
missing, and what to do on which day. Retire this file after the last draft.

The standing assessment lives in [`STATE_OF_THE_REPO.md`](STATE_OF_THE_REPO.md) and
the ordered backlog in [`plans/README.md`](plans/README.md). This is the subset of
both that a draft in the next two weeks actually depends on.

---

## The calendar

Read from ESPN's `draftSettings` on the 2026-08-24 06:00 build — the same field the
board page reads, so it is what the app believes too.

| League | Yours | Type | Draft date | Days |
|---|---|---|---|---|
| Knights_FFL | **yes** | Snake | Mon 2026-09-07 20:00 | 14 |
| GOP_Degenerates | **yes** | Auction, 2 keepers, $250 | Tue 2026-09-08 20:00 | 15 |
| 12 Dudes one Cup | no | Snake | Tue 2026-09-08 20:00 | 15 |
| John_ATL_League | no | Snake | Tue 2026-09-08 20:30 | 15 |
| John_PC_League | no | Snake | Tue 2026-09-08 20:45 | 15 |
| Winfield_Football | **yes** | Snake | **not set** | ? |
| Weenieless_Wanderers | **yes** | Snake | **not set** | ? |
| Big Red Fantasy Football | no | Snake | not set | ? |
| Washed_Up_Fijians | no | Auction | not set | ? |

**No ESPN-dated draft falls next week.** The earliest is 14 days out. But **two of
your own four leagues carry no date at all**, and those are the two that could be
next week without this repo knowing. Confirming those two dates is the first item
below, because everything else here is scheduled against them.

Re-read the table any time with:

```bash
python -c "
import json, glob, datetime
for p in sorted(glob.glob('Data/Store/2026/*/meta.json')):
    m = json.load(open(p)); d = (m.get('draft_settings') or {}).get('date')
    when = datetime.datetime.fromtimestamp(d/1000).strftime('%a %Y-%m-%d %H:%M') if d else 'not set'
    print(f\"{m['display_name']:<28}{when}\")"
```

---

## Verdict

**You could draft tomorrow in any of the nine leagues and the board would hold up.**
The draft-critical path — ingest, blend, score, board, app — is built, tested and
running unattended. Nothing on the list below blocks a draft; the items are upside,
one operational risk, and two decisions.

Verified today, not taken from a doc:

| Check | Result |
|---|---|
| Nightly refresh | `ok`, 3.7 h ago, all six stages |
| Boards | 9 of 9 built 06:00, 2,504 rows × 1,355 cols (GOP) |
| S3 publish | 274 objects, plus the dated board snapshot |
| Test suite | **1,174 passing**, 0 failures |
| Usage model coverage | 766/915 rostered players (83.7%) |
| Pre-draft injury scan | 2 players inside ADP 150 on a weak rung, **both under one game** |
| Kicker / D/ST models | priced and on every board (3–4 sources each) |

The injury result is the one worth noticing: 31 players inside ADP 150 already carry
a real severity off an automatic rung, and the manual queue is two camp knocks that
need nothing. That is the pre-draft scan
[plan 27](plans/27-injury-model.md) asked for, and it is effectively already done.

---

## The one thing that is actually risky

**~12,300 lines of work are uncommitted, unpushed, and on `main`.**

```
10,837 lines  untracked  (Scripts/injury, Scripts/kicking, Scripts/dst,
                          Scripts/outcomes, Scripts/vegas.py, config/injuries,
                          plans 27–30, 7 test files)
 1,438 lines  modified   (21 tracked files)
```

That is plans 27, 28, 29 and 30 — the entire injury layer, both special-teams models
and the outcome-distribution evidence. No branch, no PR, nothing at `origin`.

Two reasons this matters more than it usually would:

1. **The nightly job depends on it.** `run_daily_refresh.sh` calls code that exists
   only in this working tree. A `git checkout` of a clean `main` would leave a
   pipeline that no longer builds the boards you are about to draft off.
2. **A draft is a deadline.** The one week you cannot afford to reconstruct a week of
   work from memory is this one.

**Do this first, before anything else on the list.** It is an hour, most of it
writing commit messages, and it is the only item here whose cost goes up if you wait.

---

## Two open decisions

### 1. Leave `KIK_` and `DST_` at weight 0.0 — recommended

Both models are built, priced and on all nine boards at weight 0.0, exactly as `USG_`
was on 2026-08-07. Turning either on is one number. **Do not turn them on before the
draft**, on today's numbers:

| | Level vs ESPN | Rank agreement with ESPN |
|---|---|---|
| `KIK_Points` | **0.689** | ρ 0.627 |
| `DST_Points` | **0.589** | ρ 0.455 |

The ordering disagreement is the interesting half — the D/ST model's top defence is
the **Chiefs at ADP 164** where ESPN has Seattle and Baltimore — and it is exactly
what [plan 30](plans/30-dst-model.md) predicted from Vegas beating prior season on
seven of eight components. But the *level* is 31–41% low, and blending a low level
at any non-zero weight walks both positions down the VOR board in every league,
which is precisely what gates G-K4 and G-DST4 pre-register against (bar: 0.02 on a
position's median `TRUE_`/`ESPN_` ratio).

So the value is available **as a second opinion you read**, not as a blended number.
On draft night, sort D/ST by `DST_Points` and treat a large positive gap against ADP
as the model saying the market has not priced the schedule. Fixing the level is a
post-draft job; the ranking is usable today.

### 2. BetOnline weekly props — not a draft decision

The **season-long** endpoint works and is what the board uses. The weekly 403 is a
week-1 problem. → [plan 02](plans/02-betonline-access.md).

---

## What is missing, and whether it matters

| Missing | Draft impact | Verdict |
|---|---|---|
| **Live Draft page** ([09](plans/09-frontend-draft-views.md)) | Pick tracking during the draft. The board on a second monitor works without it — that is how 2025 ran | The only item worth building if time allows |
| **Outcome distributions** ([28](plans/28-outcome-distributions.md)) | The model is not built, but **its evidence is usable at the draft without it** — see below | Do not build. Read the numbers |
| **Weekly views** ([08](plans/08-frontend-weekly-views.md)) | None | After week 1 |
| **Weekly usage head** ([19](plans/19-weekly-usage-model.md)) | None — comes online ~week 3 | After the draft |
| **Blend weight re-tune** ([03](plans/03-projection-source-coverage.md)) | Would move `TRUE_Points` days before a draft on an untested change | **Do not touch before the draft** |

### Plan 28's evidence, which you can use on draft night with no build

Four numbers from [28](plans/28-outcome-distributions.md), already measured:

- An **RB2 gains +5.72 points a game** when the lead back sits — ≈46 points over an
  eight-game absence. A **WR2 gains +0.07.** Handcuff running backs; do not handcuff
  receivers.
- A player who missed **8+ weeks last season** has a **0.31** chance of a full slate
  against a healthy comparable's **0.59**, and returns to 0.641 of prior points per
  game against 0.906.
- **The fragile-RB1 premise did not survive.** Once you identify the incumbent
  pre-season rather than by this season's touches, his prior absence stops predicting
  his backup's value. Do not pay up for a handcuff on the "he's injury-prone" story.

---

## The countdown

Ordered by when it has to happen, not by size.

### This week — by Fri 2026-08-28

- [ ] **Confirm the draft dates for Winfield_Football and Weenieless_Wanderers.** Two
      of your four leagues, no date in ESPN. Everything below is scheduled against
      these. If either is genuinely next week, stop and re-read this file — the
      answer does not change, but the ordering does.
- [ ] **Commit and push the ~12,300 uncommitted lines.** Branch, PR, merge. See
      *The one thing that is actually risky*. `gh auth switch --user twinfield10`
      in the same shell as the `gh` call.
- [ ] Confirm the nightly job is still firing daily — `python -m Scripts.refresh_status`
      should read under 24 h every morning. A shut lid is a skipped night, and camp
      is when the depth chart moves most.

### Next week — by Fri 2026-09-04

- [ ] **Re-run the pre-draft injury scan.** Measured cadence is ~2.7 relevant injuries
      a week, ~1.2 of them costing 3+ games, so expect 3–4 new names by then.
      ```bash
      python -m Scripts.injury.review
      #   ... edit config/injuries/2026.yaml for any the beat reports say are worse ...
      python -m Scripts.refresh --all --what board
      ```
- [ ] **Decide on the Live Draft page.** It is the last piece of plan 09 and the only
      build worth starting this late. If it is not started by 2026-09-04, do not start
      it — draft off the board page, which is tested across all nine leagues.
- [ ] **Open the app and actually use it for an hour.** Not a smoke test — a dry run
      of draft night on GOP's auction board, which is the one with keepers, a $250
      budget and the Cash lens. The failure you want to find is a workflow one, and
      no test finds those.
- [ ] Skim [23](plans/23-owner-tendencies.md)'s tendencies for your four rooms. 103 of
      112 managers carry a measured tendency; knowing who reaches for a quarterback is
      free information you already paid for.

### Draft week

- [ ] **Rebuild the board the morning of each draft.** ADP moves through the final
      week and the board is a snapshot of the market at build time.
      ```bash
      python -m Scripts.refresh --all --what board && python -m Scripts.sync --push
      ```
- [ ] Re-run `python -m Scripts.injury.review` the morning of. A Friday injury to a
      third-round pick is exactly the case the override file exists for.
- [ ] **Freeze the code.** No blend-weight changes, no turning `KIK_`/`DST_` on, no
      dependency upgrades. Everything in this repo's own *Known issues* about absent
      sources reading as agreement applies double to a change made the day of a draft.

### Draft night

- [ ] `python -m Scripts.refresh_status` — confirm the board is hours old, not days.
- [ ] Board page on the second monitor, on the right league. The picker offers your
      four; GOP and Knights draft on consecutive nights and are one click apart.
- [ ] Read `Δrk` rather than `USG` points — `USG_Points` is injury-adjusted and
      `TRUE_Points` is not, so the rank comparison is the one that survives.
- [ ] For K and D/ST, read `KIK_Points` / `DST_Points` as an opinion, not a total.

---

## One-command check

```bash
python -m Scripts.refresh_status   # refresh + boards, ages and league count
python -m Scripts.injury.review    # who needs a hand-written severity
python -m pytest -q                # 1,174 tests, ~1 min
```

All three green and the board built today is the whole readiness bar.
