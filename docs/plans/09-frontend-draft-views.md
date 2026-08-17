# 09 — Local frontend: draft views

**Priority:** High (seasonal) · **Effort:** Large · **Status:** **Board done
2026-08-07, model columns added 2026-08-14, split into three tabs 2026-08-14,
keeper handling and the cash lens 2026-08-17, spanners and the ESPN comparison
2026-08-17, Calibration tab 2026-08-17**; Live not started; History unblocked
**Depends on:** [07 (foundation)](07-frontend-foundation.md), and the draft
phases in [`../STATE_OF_THE_REPO.md`](../STATE_OF_THE_REPO.md#roadmap--draft-strategy)

## Status

**Page 1 of 3 is built** — `app/pages/draft_board.py` over `app/draft_view.py`,
registered in `app/main.py`. It renders for all nine leagues with no exception, and
Josh Allen comes out VOR rank **9** in the 10-team superflex against **21** in
14-team Knights, which is the league-awareness this plan exists for.

Building it turned up three defects in the artifact underneath, all now fixed —
see the postscript. The short version: **every per-source point column on every
stored board was NaN for every row**, and `projection_missing` was False for all
1,026 including the 503 players projected a literal 0.0.

What is still open:

- **Live Draft** (§2) — not started. It is the one page that talks to ESPN in the
  render path, and the plan's own build order puts it last because it is the most
  likely to slip.
- **Draft History** (§3) — **no longer blocked, as of plan 23.** This said
  `draft.parquet` was unwritten and roadmap Phase 1 had not backfilled it; both are
  now false. `Scripts.refresh --what draft` writes it, the artifact holds 826 picks
  for Knights 2026, and the board page already reads it for the owner-tendency cards.
  What is missing from it is *outcomes* — points-over-expectation per manager, which
  needs every past season scored in each league's own rules.
- **Floor/ceiling is half-built.** Source disagreement is in; prior-season variance
  is not, because it needs per-week 2025 actuals joined per player out of
  `Data/Store/2025/*/lineups.parquet` — a different data path from anything the
  board builder touches today. Note the model is deliberately **not** a fourth input
  to this interval: it projects an expected value where the other sources project a
  healthy season, and mixing the two widened the median interval from 8.5% to 24.0%.
- **The auction price level does not account for team count, and wants a plan of its
  own.** Deliberately left alone on 2026-08-17 — it is a question about what a
  valuation *means* rather than about how the table reads, and it kept dragging the
  frontend work sideways. `at_budget` scales ESPN's published `auctionValueAverage`
  by the budget ratio (`budget / 200`) and nothing else, so the market total lands
  wrong in both directions:

  | league | money on the table | ESPN's rescaled total | |
  |---|---|---|---|
  | GOP Degenerates, 16 × \$250 | \$4,000 | \$2,544 | understated 1.57× |
  | Winfield Football, 6 × \$200 | \$1,200 | \$1,902 | overstated 0.63× |

  The [auction budget](#the-auction-budget) section above reasons explicitly about
  "the \$2,000 a ten-team \$200 auction puts on the table" and then never uses the
  count. The obvious fix is to scale ESPN's values so they sum to `teams × budget`,
  which touches the side that is actually wrong and leaves `Draft Metric | Us`
  budget-relative — but it moves that column and its Δ in all nine leagues, so it
  belongs with a decision about the whole cash lens rather than in a table-layout
  change. Two things to settle when it is picked up: a real auction leaves a few
  dollars unspent (ESPN's own values sum to ~94% of its assumed pool), and
  `our_dollars` has no cap at one team's budget — it never binds on real data, where
  the top of a \$250 board is \$145, but the construction permits it.

## The three tabs, 2026-08-14

The page had become one scroll holding a filterable table, two charts, six manager
cards and a decade of acquisition history. That is three jobs stacked vertically,
and the one you do under time pressure — find a player, read his number, decide —
was the one you had to scroll past two charts to reach.

| Tab | What it is for | What is on it |
|---|---|---|
| **Board** (default) | the working surface | search, the four filters, the auction budget, the table, and the column caveats |
| **Values** | where the room is wrong | *Falling past their price*, with its own position filter and a depth slider |
| **League** | what does not change during a draft | toplines, the positional cliff, the tier runway, owner tendencies, acquisition history |
| **Calibration** (added 2026-08-17) | whether the numbers the other three stand on are believable | our projection against ESPN's, faceted by position, with the biggest disagreements named |

**Board now sorts by VOR, not by value.** Value-first was right when the page was
one surface — it is the thing a board can tell you that the room cannot, and the
argument for it is still in this plan's §1. It is wrong once *Falling past their
price* has a tab of its own: value against ADP is a second opinion about the room,
and the surface you actually draft from should answer "who is worth the most" first.
Value is still one radio click away and still the whole subject of the Values tab.

**Each tab carries its own position filter, deliberately.** The Board's is "what am
I looking at right now" and gets narrowed constantly; the League tab's is "which
curves am I comparing" and should not be dragged around by it. One shared control
would have meant a chart quietly reshaping itself because you typed a name into a
search box on another tab.

### Four filters, not one

| Filter | Semantics |
|---|---|
| **Search** | literal substring, case-insensitive |
| **Positions** | keep the ones named |
| **NFL Teams** | keep the ones named |
| **Bye Weeks** | keep the ones named, *including* dropping players with no recorded bye |

Every one is an **include** list: empty keeps everything, non-empty keeps only what
it names. That single rule is what makes four controls composable without a legend
explaining each, and it is what settles the bye filter's awkward case — "keep only
weeks 5 and 10" cannot honestly keep a player nobody knows the bye of.

The search is matched **literally**, which is a fix rather than a preference. It
compiled as a regex before, and the names on a board are full of regex syntax:
`T.J.` matched any three characters between two dots, `Amon-Ra St. Brown` the same,
and a name typed with an unclosed bracket raised out of the page instead of finding
nothing.

### The auction budget

The `$` column was denominated in somebody else's money. ESPN publishes
`auctionValueAverage` against **its own $200 auction** — the 2026 pool agrees: the
338 players it has priced sum to $1,871 of the $2,000 a ten-team $200 auction puts
on the table, with the rest spread across a bench it prices in pennies — and the
board stored those dollars as published.

`draft_view.at_budget` now carries two columns instead of one: `auction_share` is
the fraction of a team's budget the market puts on a player, and `auction_dollars`
is that share at the budget set on the Board tab. The share is what makes the
dollars portable; the dollars are what gets shown. Default **$250**.

It is a straight proportion, and the help text says so. A real auction's minimum bid
does not scale with the budget — the last roster spots cost $1 whatever you are
playing for — so raising the budget adds slightly more to the top of the board than
a flat multiple suggests. The distortion is small next to the disagreement between
any two sources' valuations, and correcting it needs a roster size the function is
not given.

Verified headless through `AppTest` across all four of the viewer's leagues: Puka
Nacua prices at $57.76 on ESPN's budget, $72.20 at 250 and $144.40 at 500.

## Keepers and the cash lens, 2026-08-17

Both came out of the same discovery: **ESPN publishes a league's draft settings and
we were not reading them.** `view=mSettings` — a response `get_roster_settings`
already fetches and parses — carries `draftSettings`, and three of its fields
change what a board means:

| | GOP Degenerates | The other eight |
|---|---|---|
| `type` | `AUCTION` | `SNAKE` (plus Washed_Up_Fijians, auction) |
| `keeperCount` | **2** | 0 |
| `auctionBudget` | **250** | 200 |

They now land in `meta.json` as `draft_settings`, at no extra round-trip.

### Keepers: everyone is available until they are not

GOP's 2026 board arrives with **252 players held across 16 teams** — 15 to 17
each — against a keeper limit of **2**. ESPN carries last season's rosters into a
keeper league before anyone declares. The `Available Only` filter was reading
`on_team_id` as "unavailable" and hiding all 252, which pre-draft is a tenth of the
pool and most of the league's best players.

`adp.py` had known this since it was written — *"ESPN pre-fills rosters before a
draft, so pre-draft this is not evidence of being drafted"* — but nothing
downstream could act on it, because telling a carried-over roster from a declared
keeper needs the keeper count.

**The test is arithmetic, not a flag.** A roster holding more players than the
league allows keepers cannot be a list of keepers. So `keepers_pending` is true
while any team is over the limit, and:

- `Available Only` defaults **off**, with a caption saying why. It stays offered —
  "who was on a roster last year" is a fair question, just not "who can I draft".
- the tier runway, the scarcity curve and the value table all count the full pool,
  because "three left in tier 1" is a lie if it excluded eleven nobody has kept
- roster-needs treats your team as empty, since 15 carried players otherwise report
  every slot filled and turn the toggle into a no-op

It resolves itself. The day rosters shrink to the limit, this returns false and the
board filters again with no flag anyone has to remember to flip. It also fails in
the safe direction: a false positive shows a few players who are gone, a false
negative hides players who are available, on the morning you are drafting them.

### The keeper price was already in the store

`keeper_value` has been pulled and stored since the board was built, and never
shown. What it *is* was established by measurement rather than read off the field
name: of GOP's 187 priced keepers, **130 carry exactly their 2025 auction bid** —
CeeDee Lamb $90, Gibbs $87, Chase $84, to the dollar — and where a player changed
hands the price follows the *current* holder: Jayden Daniels went for $46 in the
auction and keeps for $1 for the manager who later claimed him.

So: **what it costs the manager holding him to keep him.** Shown as `Keeper $`,
with `Keeper +/-` against the market price and a `Keeper Bargain` sort, and only in
a league whose keeper count is non-zero — every board carries the column, and in
the eight redraft leagues it is a small number ESPN publishes for nobody's benefit.

#### A zero is a waiver claim, not the absence of a price

The first version of this read `keeper_value == 0` as "no keeper price" and left
the cell blank. That blanked **65 of GOP's 252 held players**, Malik Nabers among
them — and a blank in that column says *nobody can keep him*, which is the opposite
of true. ESPN's own UI shows Nabers at $1.

Probing the raw entry settles it: ESPN returns `keeperValue: 0` with
`status: ONTEAM`, and `keeperValueFuture` is 0 for everybody including Lamb, so the
number is not hiding in another field. A player claimed off waivers or free agency
simply has **no winning bid to record**, and the keeper cost falls to the $1 minimum.
The data agrees: only 18 of those 65 appear in the 2025 draft at all, so they are
overwhelmingly in-season acquisitions, exactly as a zero implies.

**Being on a roster is what confers a keeper price**, and the figure is the
acquisition cost floored at the minimum bid. Only a genuine free agent has none.
That the two signals agree was checked rather than assumed: no free agent on any of
the nine boards carries a non-zero `keeper_value`. All 252 of GOP's held players are
now priced, and Nabers comes out the league's **second-largest keeper bargain** —
$1 against a $29 market price — which was invisible while the cell was empty.

### Cash: the auction answer to a question ADP cannot ask

`value` compares our VOR *rank* to the market's ADP *rank*. That is the right
comparison in a snake draft, where a pick is a place in a queue. In an auction
there is no queue — there is a price — and being four places underrated does not
tell you whether to bid $41 or $46.

The Values tab now carries a **Measure Value By: ADP / Cash** switch, defaulting to
Cash in an auction league. Cash converts our own valuation into dollars the standard
way:

```
discretionary = teams x budget  -  draftable spots x $1
our $         = $1 + discretionary x (this VOR / all positive VOR in the money)
```

with K and D/ST excluded for the same reason they are excluded from `value`, and IR
excluded from the spot count because you do not draft into it. A test asserts the
whole budget is allocated and no more: 120 priced players summing to exactly $2,000
for a ten-team $200 league.

**It fixes the complaint that prompted it.** Under the ADP lens GOP's "best values"
were Brenton Strange, Dalton Schultz and Juwan Johnson — backup tight ends with
*negative* VOR, scoring high because the market ranks them even lower than we do.
Under Cash the same board leads with Jeremiah Love (+$71), Bijan Robinson (+$67)
and Jahmyr Gibbs (+$65).

### Spanners, and ESPN's opinion beside ours, 2026-08-17

Twenty-four flat columns, several of them the same quantity computed by different
people — `Proj` was the blend, `USG` the model, `ADP` and `$` the market — with
nothing on screen grouping them, so the comparison the table exists for had to be
done in the reader's head. The table now has **seven spanner groups**: identity,
then four comparisons of our number against ESPN's (points, overall rank, positional
rank, and the draft's own currency), then what it costs to keep a player, then what
is wrong with him.

**ESPN's half of three of those four comparisons was already on every stored board
and shown nowhere.** `espn_draft_rank` — ESPN's published draft ranking, dense 1..N
with no ties and populated for every row — was parsed by `adp.py` and referenced
nowhere else in the repo. `ESPN_Points`, ESPN's line scored in *this league's* rules,
was computed for the coverage report and never displayed. Only the positional rank
had to be derived, and it is a rank of a rank: `espn_draft_rank` re-ranked within
position, so it is ESPN's own ordering of the position rather than a re-ranking of
its projections. The two ESPN quantities stay in their own lanes deliberately — points
against points, a draft ranking against our draft ranking — because a difference that
moves when either the projection *or* the ranking method changes gives no way to see
which moved.

Four decisions worth recording:

- **One record per column, replacing three parallel structures.** `DISPLAY_COLUMNS`
  here, `COLUMN_GLOSSARY` there and `column_config` in the page had to be kept in step
  by a test; `dv.COLUMNS` makes that true by construction. The page's copy could not
  be tested at all — Streamlit ignores config for a column a frame does not carry, so
  a stale label stopped formatting in silence.
- **`column_config` is keyed by integer position, not by name.** The leaf headers
  repeat by design: `ESPN`, `Us` and `Δ` each appear in several groups. A name-keyed
  config applies one format to all of them.
- **Every difference is oriented the same way** — positive means we are higher on the
  player than ESPN is; points ours-minus-theirs, ranks theirs-minus-ours. That is what
  lets one colour scale serve all six difference columns, and it is the convention
  `value` had already set.
- **`vor_rank` appears twice in a snake league**, under `Ranks` and under
  `Draft Metric`. Both comparisons want the same our-number and each group is meant to
  read standalone. Two aliases of one source is legal; the alternative was a
  cross-reference.

**Colour, on the differences only.** The six difference columns diverge around zero --
zero means we and ESPN agree -- with two fill steps per arm and bold. Thresholds are
scaled per column to its own 90th percentile of |Δ|, because the units are not shared:
points differences run in tens, rank differences in hundreds, cash in single dollars.
Measured on the *unfiltered* board, so a colour does not move when a filter does.

The raw points, ranks and prices were shaded too for about an hour, against the pool
rather than against zero, and it was removed on sight. Seventeen shaded columns read as
a heatmap: the columns carrying an opinion stopped being the ones that caught your eye,
which is the entire job of the fill. The levels are the context you read a difference
against, and context does not need paint. The machinery went with the decision rather
than staying as an unused mode.

### The news mark and the keeper comparison, same day

Two follow-ups from reading the built table.

**`Notes` became `News`, a one-icon column you click to read.** The note is
a sentence; a column wide enough for one costs ~400px of 1,440 and truncated mid-clause
anyway. So the cell carries a mark and the sentence renders under the table, with the
player's status and estimated return beside it.

*Clicking*, not hovering, and that is a platform limit rather than a choice.
**Streamlit's grid has no per-cell tooltip** — `help=` on a column config is the
*header* tooltip, and hovering a truncated cell produces nothing. Checked in the
browser, because the obvious design here is an icon you hover and it does not exist.

**The mark is a `st.column_config.ButtonColumn`, and the first attempt was row
selection.** Row selection works, and it is worse in exactly the way it sounds:
enabling `on_select` adds a checkbox column at the far left of a 26-column table, and
the grid does not select on a plain cell click — so the thing you press ends up nowhere
near the thing you are asking about. A button column makes the cell value the button's
label, so the mark itself is the target. `key=` is what enables the click at all; a
button column without one renders as inert buttons.

Two things the click has to survive. The row number it reports is a position in the
*sorted, filtered* frame, so it is resolved to a `player_id` immediately — held as a row
index it would name a different player the moment the sort changed, silently. And
Streamlit clears the click value on the *next* rerun, so a note read straight off it
would vanish the first time anything else on the page moved. `remember_note_click`
resolves and remembers; clicking the open player's mark again closes the panel, which is
the only dismissal available without a second control. Scoped per league and per table:
two widgets cannot share a key, and a note carried across a league change would point at
a player the new board may not hold.

**`Keepers | Value` now measures against our valuation, not ESPN's** -- what decides
whether to keep a player is whether *we* rate him that highly; the room's price is a
fact about other people's money. Both sides are in this league's real dollars: the
keeper price is what the holder actually paid, ours is what we would spend of the same
budget.

**A normalisation was attempted here and reverted, which is worth recording because the
reasoning was wrong in an instructive way.** Our dollars sit above ESPN's for most
players inside the money -- we allocate the whole budget across the ~106 worth rostering
where the market spreads it over the ~313 it prices -- so `cash_delta` came out positive
for 89% of the priced pool and the keeper surplus for 68% of held players. Scaling our
side onto the market's price level fixed that (56% and 42%) and it was the wrong fix: it
destroyed the only property that makes the column actionable. **`Draft Metric | Us` is a
bid you read off and make**, denominated in the budget you actually have; the top of a
$250 board is $145 of $250. A tidier difference is not worth a valuation you cannot act
on.

The residual is not an artefact either. It is our valuation talking: if you believe the
model, the players inside the money genuinely are underpriced by a room that spends a
third of its money on depth. So the difference is read as an *ordering* -- who the room
is most wrong about -- and the glossary says so rather than implying each row is a
verdict.

### Five bugs this turned up

**`column_config`'s numerical positions count the hidden index.** Streamlit numbers
every column it was handed, index first, and matches `_pos:N` against that;
`hide_index=True` hides the index without renumbering what follows. Off by one is not
a crash — every column silently wears its neighbour's format. It showed as `Tier`
rendering `1.0`, `Ranks | Us` rendering `+1`, `VOR` losing its decimal, and the
identity block splitting across the frozen boundary because the fifth pin landed on
the sixth column. Only found by screenshotting the running app.

**Streamlit prints the word "None" for a missing cell, and it takes two fixes.** The
default placeholder is the literal string, so `Exp Return` asserted "None" on 998 of
1,026 rows and every unpriced player's `Δ` did the same — columns whose blank means
*nobody published a number*. The two column kinds need different halves, which is why
fixing one looked like fixing it:

- **Text**: `st.dataframe(placeholder="")` covers it, but a Styler also ships display
  strings and Streamlit prefers those where `column_config` has no format of its own —
  which is every text column. Pandas renders a missing value with `str` by default, so
  `Styler.format(na_rep="")` is required as well.
- **Number**: its Styler display string is deliberately *ignored* in favour of
  `column_config`'s format, so only `placeholder=""` reaches it.

The frame keeps its nulls either way. Filling them would make a free agent's keeper
price a real `0`, and the blank there means nobody holds him.

**The glossary described the auction league's `Δ` twice.** The two `Draft Metric`
variants share their three headers, so scoping the glossary on `(group, label)` alone
matched both — a dollar difference and an ADP rank difference under one heading, with
no way to tell which the column above was. The lens is now passed, not inferred.

**An all-null `injury_status` took the page down.** `replace_strict` against a Null
dtype column tries to cast the map's string values into it and raises. A board where
ESPN returned no status at all is not hypothetical — it is what an artifact built
before the field existed looks like.

**Not colour-coded by tier, still.** Eight ordinal steps cannot be given separable
lightness; that decision from the original build stands and is now recorded in the
`Tier` glossary entry rather than only in this document.

### The glossary, 2026-08-17

Twenty-four columns, several of which are computed three different ways from three
different places, and the only account of any of them was a `help=` tooltip you had
to know to hover. **Glossary — Where Every Column Comes From** sits under both
tables as an expander, and gives each column a source and a one-line derivation.

*Superseded the same day by the section above: it now carries a fourth column, `What
It Does Not Say`, and is split by spanner group. The separate "What Is Missing From
These Columns, and Why" expander is gone — its per-column caveats moved into that
fourth cell, and only what is not about a column (the shape of the pool, what the
artifact as a whole cannot answer) stayed as prose beneath the table. Two accounts of
the same twenty-odd columns was one too many, and a caveat is only useful next to the
column it is about.*

Two decisions worth recording:

- **It is generated from `DISPLAY_COLUMNS`, and a test asserts the two agree in
  both directions.** A column added to the table and not the glossary is a silent
  gap; a glossary entry for a column that has been removed is documentation of
  something that is not there, and nobody would ever notice either. Neither
  survives the test.
- **It is scoped to the columns actually rendered.** A redraft league is not told
  about keeper prices it does not have — Winfield_Football gets 22 rows,
  GOP_Degenerates 24.

*Source* means where the number originates, not where it is stored: everything is
read out of one parquet file, which is not the useful answer. The vocabulary is
`ESPN`, `NFL schedule`, `Blend`, `Usage model`, `Board build` (computed once by
`refresh --what board`) and `Derived here` (computed by the page, because it depends
on the budget you set).

Writing it turned up that the `Δ Rk` tooltip had the subtraction **backwards** — it
said "the model's rank minus the blend's" where the code computes `TRUE_PosRank −
USG_PosRank`. The conclusion it drew was right and the formula was inverted, which
is the sort of thing only writing the derivation down catches.

### Three bugs this turned up

**`_sticky_selectbox` was broken in two opposite ways, and fixing one exposed the
other.** Both were silent: the app rendered a real league's real board, just not
the one you asked for.

*Every second league change was eaten.* It passed no `key=` and steered the widget
with `index=`; a keyless widget's identity is derived from its arguments, `index`
among them. Switching leagues changed the remembered value, which changed `index`
on the next run, which minted a new widget id — and the selection just made was
recorded against the old one and discarded. Winfield → GOP worked, GOP → Knights
silently did not. Pre-existing.

*Then navigation moved the league.* Giving the widget its key fixed the above and
broke something the keyless version had got right, which is presumably why it was
written that way: **Streamlit discards a widget's state when you open a page that
has not rendered it.** Correcting the key only when the remembered value was
*invalid* therefore fixed nothing — on navigation the value is perfectly valid,
nothing touches the key, and the widget falls back to its first option. Opening the
Draft Board from the Store Overview moved you from Winfield_Football to
GOP_Degenerates, and it took instrumenting the function to see it: session state
read `winfield_football` on the line above the widget that returned
`gop_degenerates`.

The fix is both halves — the widget owns its key, *and* that key is re-written on
every run whether or not it needed correcting, which is Streamlit's documented
"keep" pattern. `tests/test_header_selection.py` pins both against a stubbed
Streamlit, because the suite has to run with no store on disk.

**Streamlit reads paired `$` as LaTeX.** The cash caption rendered as green
monospace maths. Every dollar amount in a caption, markdown block or `help=` string
is escaped `\$`; the `format="$%.0f"` column specs are printf and are left alone.

## Calibration, 2026-08-17

A tab for the question none of the other three ask: **where do we disagree with
ESPN, and is that disagreement a player or the model?** `Points | ESPN` and
`Points | Us` have sat next to each other on the Board since the spanners landed,
but a 2,500-row table is not how you notice that a whole position is offset.

Both are scored through this league's own rules, so they are directly comparable.
Everything the tab computes lives in `dv.agreement_frame`, `agreement_outliers`,
`agreement_summary`, `with_outlier_flag` and `with_label_slots`.

### The scatter had to be faceted, and the palette is why

The obvious form is one scatter coloured by position. It is not available. The
repo's categorical palette was validated for **adjacent** pairs, which is all a
line chart or a grouped bar needs — their series sit in a fixed order, so only
neighbours are ever compared. A scatter is an **all-pairs** form: any two dots can
land next to each other. Re-running the validator in that mode fails it:

| Pairing | ΔE | Verdict |
|---|---|---|
| `#eda100` ↔ `#eb6834` (TE/RB) | 13.7 normal vision | below the floor of 15 |
| `#008300` ↔ `#eb6834` (D/ST/RB) | 3.2 protanopia | far below 8 |

So: one panel per position, one colour in each. The panel header carries the
identity and the colour is redundant with it, which is what keeps identity off
colour alone. No legend — there is one series per panel.

That redundancy also settles what to do when the palette runs out. `POSITION_HUES`
stops at eight slots and GOP Degenerates starts cornerbacks, so **a position with no
hue is drawn in muted ink** rather than given a generated ninth colour — which would
be indistinguishable from an existing one under CVD. Nothing is lost: inside a panel
the colour distinguishes the position from nothing. The alternative shipped first and
was wrong — the chart filtered to hued positions while the tables below still scored
and ranked every position, so 165 startable cornerbacks were in GOP's outlier
population with no panel to appear in.

The facets turned out to be the honest form anyway, because **the gap is strongly
position-dependent** and one pooled cloud is six calibration regimes drawn on top of
each other. On the 2026 Winfield board:

| Pos | n | Mean Δ | Spread |
|---|---|---|---|
| QB | 65 | **+27.3** | 29.1 |
| TE | 98 | +10.1 | 12.9 |
| WR | 187 | +8.8 | 16.1 |
| RB | 111 | +5.9 | 17.8 |
| D/ST | 32 | +2.1 | 3.3 |
| K | 32 | 0.0 | **0.0** |

Pooled, the quarterbacks read as the model's headline problem and hide every other
one.

### Two views, because r = 0.982

The scatter that was asked for — ESPN on x, us on y, a 45° line — is the default and
is the right entry point. It is also nearly useless for spotting outliers on its own:
the two columns correlate at **0.982** over a 0–360 range with a residual spread of
~18, so every disagreement is squeezed onto the diagonal. *Disagreement* plots the
gap itself against the midpoint of the two, with a solid rule at zero and a dashed
one at the position's own mean. The distance between those two rules is the
systematic half of the disagreement; the scatter around the dashed rule is the
per-player half. Which of the two dominates is the whole question.

### Scored within position, over what is currently shown

`agreement_z` is a z-score of the gap **within position**, measured over the frame
the filters produced rather than over the whole board. Both halves are load-bearing,
and the second is the one that looks wrong until you measure it. Narrowed to the
200 players the market actually prices, the direction of the bias **flips**:

| Pos | Mean Δ, whole pool | Mean Δ, priced only |
|---|---|---|
| WR | +8.8 | **−3.4** |
| RB | +5.9 | **−4.9** |
| TE | +10.1 | **−2.1** |

"We project above ESPN" is almost entirely deep players nobody drafts. Scored
against the full board, every priced receiver would read as a negative outlier for
the sole reason that it belongs to the priced half — so the *Market-Priced Only*
toggle changes the answer, not the view, and its help text says so.

### Two floors, and the kickers that forced the first one

A position gets no score at all when its disagreement has under `AGREEMENT_MIN_SD`
(0.5) of spread, or fewer than `AGREEMENT_MIN_PLAYERS` (8) rows left after the
filters. The first floor is not defensive coding. **No source but ESPN projects a
kicker**, so `TRUE_Points` for a K *is* `ESPN_Points`: every delta is float dust
around 1e-14 and the standard deviation is 7e-15. Dividing by it ranked Wil Lutz
and Andre Szmyt as the board's two biggest disagreements on a delta of
−0.00000000000003. They are now reported as unscorable, which is the true answer,
and the summary table says `Scored: no` rather than leaving a blank that would read
as agreement.

The floor turned out to cover more than kickers: **no source but ESPN projects any
IDP position either**, so GOP's 165 cornerbacks and 124 linebackers are float dust
on the same scale and are reported the same way.

### Labels: staggered by the mark's vertical order, not by rank

Vega-Lite has no collision solver for point labels, and three flagged marks in one
panel are routinely on top of each other — the three flagged quarterbacks are deep
backups within 15 points of each other. One text layer per slot at a fixed vertical
offset fixes it, but **the slot has to be the mark's own y-order, not its rank by
|z|**. By rank the offsets ran against the marks' spread — the lowest mark's label
pushed up, the highest one's pushed down — and the three tight ends still landed on
one line. Ordered by the y field, the marks' spread adds to the offsets instead.
Since the two views put different columns on y, the slot cannot be decided when the
flag is, which is `with_label_slots`' whole reason to exist separately.

One consequence worth knowing: Vega-Lite will not facet a layered spec whose layers
carry different data, so the highlighted marks are a `transform_filter` over one
flagged dataset rather than a second frame drawn on top.

### What the tab does not claim

- **These are not two independent forecasts.** ESPN is one of the three equal thirds
  inside `Us`. The gap is damped by construction and reads as *how far the blend
  moved off ESPN*.
- **Nothing here is scored against outcomes.** A big gap says the sources disagree,
  not who is right. Grading needs finished seasons projected in advance, which the
  store does not hold — see [25 (results backfill)](25-results-backfill.md).
- **`USG_Points` is deliberately absent.** It prices availability where the other two
  assume a healthy 17 games; subtracting mixes two quantities, which is the same
  reason it has no Δ on the Board tab.

## The model columns, added 2026-08-14

The page predated the usage model and showed none of it, which by 2026-08-14 meant a
third of `TRUE_Points` was invisible on the artifact built to explain `TRUE_Points`.
Four columns now sit between the market block and the status columns:

| | |
|---|---|
| `USG` | The model's own projection. The one number on the table not comparable to `Proj` — see below |
| `Δrk` | `TRUE_PosRank − USG_PosRank`. Positive where the model likes a player more than ESPN and FantasyPros do |
| `Exp G` | Games out of 17 the model expects the player available for, which `USG` is already scaled by |
| `Model evidence` | Why the model's evidence is thin, or why it produced nothing |

**The level mismatch is handled by showing ranks alongside the points, not by hiding
the points.** `USG` is an expected value over `Exp G` games; `Proj` assumes a healthy
17. Subtracting them means nothing, so the `USG` tooltip says so and the page's "what
is missing" panel says it again at length. `Δrk`, being a rank, is immune — the same
property that keeps the model out of `Floor`/`Ceil`.

**`Model evidence` exists because an empty `USG` meant three different things** and
all three rendered as the same blank cell, which reads as agreement:

| State | Rows, Knights 2026 |
|---|---|
| `not modelled` — K and D/ST, never modelled, plus anyone with no usage history | 316 |
| `withdrawn (availability)` — the model declined a player whose expected games were too low to price | 90 |
| `withdrawn (injury)` — the report withdrew a price the model had already made | 12 |
| the flag itself — `changed teams`, `thin prior season`, `low prior volume`, or a combination | 290 |
| `—`, priced with nothing flagged | 318 |

A withdrawal is reported ahead of the evidence text where a row carries both, which
7 rows do: that there is no number is more useful than why the absent number would
have been shaky. Sorting by `Δrk` is offered in both directions, because the
interesting disagreements are at both ends.

Verified headless through `AppTest` across all nine leagues, four of four columns
present on each. The board's default position filter shows only QB/RB/TE/WR, so
`not modelled` is mostly reached by widening that filter — which is pre-existing
behaviour, not something these columns changed.

## Goal

The draft-day surface. Three pages with different technical characters:

| Page | When | Character |
|---|---|---|
| **Draft Board** | pre-draft prep | static, sortable, exploratory |
| **Live Draft** | during the draft | polling, stateful, glanceable |
| **Draft History** | offseason | analytical |

Draft data pipelines are Phases 1-3 of the roadmap; this plan is the UI on top.
Board data comes from the store (`board.parquet`, `draft.parquet`), computed
during refresh — the board must not be calculated on page load.

## 1. Draft Board

The pre-draft artifact. One table, heavily filterable:

| Column | Source |
|---|---|
| Player, position, NFL team, bye | ESPN |
| Projected season points | **done** — `Scripts.season_projections.build_season_projections()` gives `TRUE_Points` and `TRUE_PosRank`, blended across ESPN/FantasyPros/BetOnline/Pinnacle season lines and scored per league. See [plan 12](12-season-projections.md). Defence in the IDP league is pending [plan 11](11-per-slot-scoring.md). |
| **VOR** | Phase 3, from this league's actual roster slots |
| **Tier** | Phase 3, 1-D clustering within position |
| ESPN ADP, auction value | Phase 3, `view=kona_player_info` |
| **Value** = ADP − your VOR rank | the target/avoid signal |
| Floor / ceiling | source disagreement + prior-season variance |
| Injury flag | ESPN |

All of these are on the page. Two carry caveats the page states rather than hides:

- **`Value` is blank for 829 of 1,026** — 84% of the pool the market has not priced,
  plus K and D/ST, where a season-total VOR does not describe a position you stream.
- **`Floor`/`Ceil` are measured for 163 of 1,026.** They are the range across the
  sources that *really* have a line, and a source imputed from the ESPN/FantasyPros
  mean does not count. Mean width is 11% of the projection. Fewer than two real
  sources means no spread rather than a spread of zero — otherwise the players
  nobody has priced would report the *narrowest* range, which is backwards.

Interactions that matter:

- **Sort by value, not by rank.** The board's job is surfacing where your
  projections disagree with the room. **Done** — it is the default sort, with VOR,
  projected points, ADP and auction value as alternatives.
- **Tier bands as the primary visual.** Tier breaks drive draft decisions far
  more than one-spot rank differences. ~~Colour by tier~~, and show where each tier
  runs out. **Done as a tier-runway chart** — available players per tier per
  position, so "three left in tier 2" is legible at a glance. **Not coloured by
  tier**, and that is deliberate: tier is an ordinal blue ramp, and eight tiers
  cannot be stepped down one hue with separable lightness. Squeezing eight steps
  into the range whose light end still clears the surface leaves adjacent lightness
  differences of 0.047, which fails the palette validator and the eye alike. Tier
  went on an axis and position kept the colour, which also means one colour means
  one thing across both charts on the page.
- Position filter, and a **roster-need** toggle that filters to slots you
  haven't filled. **Done.** Dedicated slots fill before flex ones, so a third
  running back fills the flex rather than displacing a starter. Pre-draft it is a
  no-op, since every slot is open — the toggle's help text says so.
- **Scarcity curve** — projected points by position rank, which makes the
  positional cliff visible. This is the single most useful chart on the page.
  **Done**, running to 1.6× replacement level with each position's replacement rank
  drawn as a dashed rule. Past that every curve is flat near zero and the cliff —
  the whole point — is squeezed into the left edge.

**League-awareness is the differentiator.** Replacement level comes from each
league's real starting slots, so the same player is legitimately ranked
differently across your nine leagues. The scan found real variety here — 6 to 16
teams, a superflex `OP` slot in Weenieless Wanderers, IDP `DP` in GOP
Degenerates, no D/ST in 12 Dudes. A generic board from any website cannot do
this, and it's the whole reason to build one.

Confirmed on the built page. Josh Allen, one player across nine boards:

| League | Teams | QB replacement | VOR | VOR rank |
|---|---|---|---|---|
| Weenieless Wanderers (superflex) | 10 | **QB20** | 102.2 | **9** |
| GOP Degenerates | 16 | QB16 | 87.3 | 20 |
| Knights FFL | 14 | QB14 | 78.7 | 21 |
| Winfield Football | 6 | QB6 | 51.3 | 11 |

The superflex `OP` slot pushes quarterback replacement to QB20 and Allen up twelve
places against Knights. The 6-team league is the other direction and shows why
team count alone does not explain it: replacement is QB6, so the gap to the next
quarterback is small and his VOR is the lowest of the nine even though he ranks
11th there.

## 2. Live Draft

The hard page. Requirements are different from everything else in the app: it
updates without interaction, holds state across reruns, and gets read at a
glance under time pressure.

### Polling

`st.fragment(run_every="5s")` re-runs only the draft-state fragment, leaving the
rest of the page alone:

```python
@st.fragment(run_every="5s")
def draft_state():
    picks = poll_draft(league)          # league.refresh_draft(), then league.draft
    ...
```

Note `refresh_draft(refresh__teams=True)` is broken upstream — `league.py:93`
references an undefined `data`, and the kwarg is misspelled with a double
underscore. Call it with defaults only.

This is the one place the app talks to ESPN in the render path, which is a
deliberate exception: the draft endpoint is small and fast, unlike the
17.6s `get_ply_stats_by_matchup`. Load the board from parquet at mount; only
poll picks.

### What it shows

- **Best available**, by VOR, filtered to your roster needs.
- **Tier breaks remaining** per position — "3 RBs left in tier 2" is the
  information that actually drives a pick.
- **Positional run detection** over the last N picks. A run on TEs is when you
  either jump or deliberately fade.
- **Value on the board** — players falling well past their ADP.
- **Your roster** against starting slots, with what's still unfilled.
- **Picks until your turn**, and who picks between now and then.

### Resilience

Draft day is the wrong time to debug. Cache the board to parquet at startup so a
crash resumes instantly. Degrade visibly rather than silently: if polling fails,
show a stale-data banner with the last successful poll time and keep rendering
the board. A manual "mark player drafted" fallback covers total API failure —
without it, one bad request ends your draft.

Dry-run it by replaying a completed 2025 draft pick by pick before trusting it.

## 3. Draft History

Roadmap Phase 1 output — 2016-2025 for the two leagues with real depth
(Winfield_Football, Weenieless_Wanderers).

- Points-over-expectation per manager per year: who actually drafts well.
- **Positional tendency by round** per manager. This is what feeds the Phase 4
  simulator's opponent models, and it's directly useful on draft day — knowing a
  leaguemate reliably reaches for a QB two rounds early changes when you take
  yours.
- Reach/steal distribution against that year's ADP.
- Empirical pick-value curve from your own leagues, replacing the missing
  `pick_value.csv` the dead `draft_utils.py` referenced.

**Caveat to surface in the UI:** the scan found unmapped scoring rules in
Winfield_Football 2016-2019 and Weenieless_Wanderers 2017-2019
([plan 01](01-scoring-coverage.md)). Historical `total_points` inherit that gap,
so points-over-expectation for those years is slightly off. Fix plan 01 before
trusting the early-year numbers, and note the caveat on the page until then.

## Build order

Board → History → Live. The board is the minimum viable draft-day artifact and
is useful the moment it exists. History is cheap once Phase 1 lands and it
informs draft strategy directly. Live is the most work and the most likely to
slip; if it does, the board on a second monitor still works.

## Verification

- ~~Replacement ranks hand-check for a standard 12-team league (RB ≈ RB30 with
  flex) and differ correctly for the superflex and IDP leagues.~~ **Done.** The
  three 12-team leagues come out **RB31**/WR29 with the flex. The superflex is
  QB20; the IDP league is LB15 with CB1, and the 6-team league RB17.
- ~~The same player ranks differently across leagues, for the right reason.~~
  **Done** — the Josh Allen table in §1.
- ~~Board renders for all nine leagues, including the one with no D/ST.~~ **Done**,
  driven headless through `streamlit.testing.v1.AppTest`, nine of nine with no
  exception. 12 Dudes one Cup's replacement list correctly has no D/ST entry, and
  its 32 team defences are flagged unstartable and hidden by default.
- Screenshotted in both themes and the rendered stroke colours read back out of the
  DOM: `#2a78d6/#eb6834/#1baf7a/#eda100` light, `#3987e5/#d95926/#199e70/#c98500`
  dark. Both palettes pass the colour-blind-separation and lightness-band checks;
  the light one's sub-3:1 slots are relieved by the direct labels and the table.
- Live page dry-run against a replayed 2025 draft keeps its available-player set
  in sync throughout. — **open, with Live**
- Kill the network mid-poll: stale banner appears, board still renders, manual
  mark-drafted still works. — **open, with Live**

## Postscript: three defects in the artifact, found by building the page

None of these were visible from the board builder's own output. Each surfaced from
asking the page a question the summary line never had to answer.

**1. Every per-source point column was NaN, on every board, for every row.**
`_apply_scoring` summed a prefix's scored stat columns straight through, so one NaN
cell made the total NaN. The weekly path never noticed because `clean_lineups`
imputes and 0-fills before scoring; the season path is sparse — a running back has
no `ESPN_passingYards`, and passing yards is a scored rule in all nine leagues. So
`ESPN_Points`, `FP_Points`, `PINNY_Points` and `BOL_Points` were NaN 1,026 of 1,026
in every league, and `season_projections.main` had been printing three empty columns.
A missing stat now scores 0 while a source with no scored cell at all stays NaN,
which is the distinction plan 03 cares about: a book with no line is not a book
projecting zero. Verified behaviour-preserving on the weekly path by recomputing
every prefix over all nine leagues' stored 2025 `lineups.parquet` — max absolute
difference **0.0**.

**2. `projection_missing` never fired.** It was `board[points_column].isna()`, and
the blend 0-fills, so it was False for all 1,026 rows in every league — including
**503 players whose projection is a literal 0.0**, two of whom the market has even
priced. It now means "no source produced a scored line", which only became
computable once (1) was fixed. `board_summary` had the same blind spot and had been
reporting "1026 projected"; it now says 523.

**3. A structural zero counted as a source opinion.** First cut of the floor/ceiling
spread let FantasyPros count as real for Cameron Dicker on the strength of twelve
non-imputed `0.0` cells — a kicker has no passing yards, and nobody imputed that or
asserted it. His floor and ceiling came back exactly equal to ESPN's total: a spread
of zero, reported as measured agreement. Zeros no longer count, and kickers dropped
out of the spread population entirely, which is the honest answer for a position no
source prices.

The pattern in all three is the same one this repo keeps meeting: **a 0.0 that means
"nothing here" is indistinguishable from a 0.0 that means "zero", and every summary
count built on `notna()` reads the first as the second.**
