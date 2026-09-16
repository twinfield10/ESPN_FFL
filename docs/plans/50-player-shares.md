# 50 — Player shares: who to root for, across every league at once

**Status:** COMPLETE

**Priority:** Medium · **Effort:** S · **Where it stands:** **Shipped 2026-09-16.**
The Player Shares tab differentiates [plan 42](42-weekly-matchup-odds.md)'s win
probability with respect to one player and sums it over every league the viewer is
in. No new model, no new artifact, no pipeline change.
**Depends on:** [42 (weekly matchup odds)](42-weekly-matchup-odds.md) ·
[48 (live scoring)](48-live-scoring.md) — the remaining-points spread this reads ·
[40 (frontend restructure)](40-frontend-restructure.md) · [26 (user accounts)](26-user-accounts.md)

---

## Problem

Four leagues, eight starting lineups, one Sunday. Every tab in the app answers a
question about **one** league, and the question you actually have on the couch is
not one of them: *when Ja'Marr Chase catches a touchdown, is that good for me?*

It is not rhetorical. On week 2 of 2026, **eighteen players were started in more
than one of the viewer's four leagues, and eight of those were on both sides** —
owned in one league, faced in another. Davante Adams, Kyren Williams, Josh Allen,
Christian McCaffrey, Jaxon Smith-Njigba, Omarion Hampton, Mike Evans, the Seahawks
defence. Nothing anywhere said so, and no amount of opening the Matchup tab four
times says it either, because the answer is a *sum* and each tab holds one term.

The second half is weighting, and it is the half that needs a model. Owning a
player in a coin flip is not the same as owning him in a week already won. Plan 42
priced that — it just never differentiated it.

## What it computes

**The currency is expected wins, and that choice is load-bearing rather than
cosmetic.** `docs/DATA_CATALOGUE.md` §8 is blunt: *never compare points across
leagues; ranks compare, points do not.* That is not an abstraction here — measured
on the same week, **Josh Allen projects 23.27 in three of this viewer's leagues and
31.00 in `gop_degenerates`**, which pays six for a passing touchdown. Adding those
would be adding two different units.

So nothing is added in points. Each league's points become a probability, inside
that league's own scoring, and probabilities are unitless:

```
E[W] = Σ_L p_L                        # 2.35 of 4, week 2
```

For player *i* started in league *L*: take him out of his side's mean **and** its
spread, put him back at each end of his own interval, and read the viewer's
probability.

```
mu_rest = mu_side - mu_i
sd_rest = sqrt(sd_side² - sd_i²)
stake_L = p(mu_rest + mu_i + z·sd_i) - p(mu_rest + max(0, mu_i - z·sd_i))
Δ Wins  = Σ_L stake_L
```

**The sign is not applied by hand anywhere.** When the player is on the opponent's
roster the perturbation lands on the opponent's side and the viewer's probability
falls out negative on its own. That is what makes a player who is owned twice and
faced once one number rather than three.

**Exact, not a derivative.** `matchup_sim.swing` moves the mean and leaves the
spread alone — correct for its own question, *what is a lineup change worth* — and
wrong for this one, where removing a superflex quarterback takes a fifth of the
lineup's variance with him. Hence a new function rather than a call into `swing`.

Beside it, the rate: `phi(d) / sd_margin`, what one point is worth. It is the
number that answers the original question about weighting — on week 2 a point in
`knights_ffl` (52.7%) was worth **1.20pp** and a point in `winfield_football`
(70.7%) **0.99pp**, so a player owned in both carried 2.19.

**No Monte Carlo, for plan 42's reason.** The normal approximation is calibrated to
within 0.2pp at team level; sampling would add its own noise and a seed to remember.

## The two things measurement changed

**A commissioner adjustment moved a whole league.** The first cut of this was a
scratch prototype that summed lineups directly, and it put `jeffs_league` at 72.4%.
The shipped path routes through `matchup_sim.side` and reads **54.8%**, because
Logan Walker carries a **+16.0** point adjustment that ESPN folds into the score it
publishes. Expected wins for the week: 2.35, not 2.53. The prototype was not
wrong about the arithmetic; it was wrong about what a team's score is. Reusing
`matchup_sim.adjustments` rather than re-summing is what made this tab agree with
the Matchup tab and with the box score.

**The hedge threshold came off the distribution, not off intuition.** An ordinary
starter in a single league carries `|stake|` of **0.22**, and the smallest of the 45
of them is 0.14. So `HEDGE_EPSILON = 0.02` is about a tenth of the quietest real
rooting interest on the page, and the two players it catches are exactly the ones it
should — Josh Allen at 0.007 and Omarion Hampton at 0.011, each owned in one league
and faced in another at leverages that almost exactly matched. An earlier 0.005 left
both reading *root for him*, which is the table saying something it does not mean.

## What week 2 actually said

| | Δ Wins | | |
|---|---|---|---|
| Ja'Marr Chase | **+0.531** | +2.19 %/pt | yours in Winfield and Knights |
| James Cook III | **−0.527** | −2.38 | faced in Knights and Jeffs |
| Trey McBride | +0.493 | +2.17 | yours in Winfield and Jeffs |
| Kyren Williams | +0.483 | +2.38 | yours in Knights and Jeffs |
| Joe Burrow | −0.462 | −2.17 | faced in Winfield and Jeffs |

And the part that exists nowhere else — the five players on both sides:

| | | |
|---|---|---|
| Seahawks D/ST | **−0.181** | for in Jeffs, against in Knights *and* Winfield |
| Jaxon Smith-Njigba | +0.048 | for in Jeffs, against in Winfield |
| Christian McCaffrey | +0.030 | for in GOP, against in Winfield |
| Omarion Hampton | +0.011 | *a wash* |
| Josh Allen | +0.007 | *a wash* |

Accounting checks: 72 started player-rows across four fixtures, 58 distinct players,
72 interests. Nothing dropped, nothing double-counted.

## What is on the page, and what it refuses to say

**Two charts and a table.** The page answers two questions -- who do I want the
ball to go to, and who do I want it kept from -- so it draws them as two horizontal
bar charts of ten, side by side. Splitting by direction makes each chart a *single
series*, which means no legend: its title carries the identity. Both plot
**magnitude**, so the bars can be compared by length across the pair; the signed
value lives in the tooltip. Below them the Conflicts **table** nets the both-sides
players and carries the reading in words, so a hedge says *a wash* rather than
looking like a very small opinion. The full ranking is one expander down -- a chart
showing ten of fifty-eight is a summary, and the reader has to be able to get past
it -- and a second expander holds the per-league arithmetic behind any player's
total, which is the one place a **rate** is unambiguous.

**The colours were measured, not chosen.** Green-against-red is the textbook
colour-vision trap, so the pair was run through the validator rather than eyeballed:
on the light surface it separates at **ΔE 7.2** (protan), inside the 6-8 band that
is legal *only* alongside a second encoding, and clears 3:1 contrast; on dark it
passes outright at **ΔE 8.6**. Three second encodings are present -- separate
charts with their own titles and axes, a number printed on every bar, and the table
below -- so nothing asks a reader to tell the directions apart by hue. A
clean-passing blue/red was available at ΔE 21.6 and **was not taken**: it would have
made this the one chart in the app where green does not mean good. The slots come
from `draft_view.SERIES_COLORS`, the palette the draft charts already use, so a
theme switch moves every chart together.

Three captions, and the middle one is the point of the design:

- `matchup_sim.gate_note`, already written, on where the probability comes from.
- **The summed rate is an index, not a quantity**, and the page says so *from the
  data* rather than in the abstract: `scoring_divergence` finds the players whose
  own leagues disagree about what they project and quotes the worst by name. Δ Wins
  is immune, which is the entire reason it is the headline and the sort key.
- Bench players are absent by construction; lineups are taken as they stand; players
  in the same NFL game are treated as independent.

With no fitted dispersion the tab falls back to net projected points and says so —
plan 42's rule that a confident-looking number nobody can check by eye is worse than
no number.

## Files

| | |
|---|---|
| `app/player_shares.py` | The arithmetic. Streamlit-free, takes loaded frames, 19 tests |
| `app/views/shares_tab.py` | Layout and the IO, cached on `store.version` |
| `app/routes/shares.py` | The route, after Matchup |
| `Scripts/outcomes/weekly.py` | `normal_pdf`, beside the `normal_cdf` it differentiates |
| `app/auth.py` | `Viewer.owner_names` and `owner_for` — see below |
| `app/main.py` | Six tabs |

**`Viewer.owner_names` is a new seam, not a convenience.** The tab has to answer
"which team in this league is mine" for four leagues at once, and
`Selection.my_owner` answers it for one. The obvious shortcut — reuse
`display_name` — conflates *how to greet somebody* with *a join key*, which works
today and breaks on the first viewer ESPN spells differently from their own
greeting. It is a tuple because the same person is not always one name:
`knights_ffl`'s draft history carries both `"andrew blair"` and `"Andrew Blair"`.

**The join is on the name string, and that is a decision.** `owner_id` exists, on
`draft` and `tendencies` only, and is not the shortcut it looks like — **this viewer
has two ESPN SWIDs**, `{DA9F7430-…}` in `winfield_football` and `{796FF49A-…}` in
the other three, so an id join needs the union before it beats a name that was
verified identical in all four stores.

## What is left

- **An owner picker.** `owner_names` is the seam; John Baizer, Will Winfield and
  Fields Pierce need the cross-league alias work [plan 26](26-user-accounts.md)
  describes before a picker is safe.
- **Grouping by NFL kickoff** — a Sunday watch guide. Cheap arithmetic:
  `Data/NFL_Schedules.csv` carries `gameday`/`gametime`/`away_team`/`home_team` and
  `nfl_utils.ESPN_TEAM_ALIASES` already reconciles nflverse's `LA`/`WAS` against
  ESPN's `LAR`/`WSH`. The obstacle is that the app reads the store from **S3** and
  that CSV is local-only, so it needs a fallback path.
- **Correlation within an NFL game.** Rooting for a quarterback while facing his own
  defence is two independent interests here. Plan 42 measured independence at *team*
  level and `distribution.correlation_matrices` is the named seam; at this level it
  is untested, and worth stating as such.
- **Nothing for another team's result.** What a third team does to seeding is
  playoff-odds swing, still owed from plan 42.

## Verification

```bash
pytest tests/test_player_shares.py tests/test_app_routing.py tests/test_matchup_sim.py
ESPN_FFL_STORE_SOURCE=local streamlit run app/main.py
```

No `AppTest` test, deliberately: the suite must run with no store on disk — `Data/`
is untracked — which is the same reason `tests/test_header_selection.py` stubs
Streamlit rather than driving the app. The render was checked by hand through
`AppTest.from_file("app/main.py")`, which is the only entry point that works; a
route run directly never gets `session.render_context()` and raises.
