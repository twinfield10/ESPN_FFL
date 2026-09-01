"""Derivations behind the draft board page: filters, roster needs, chart data.

Kept out of the page script so it is testable without a Streamlit runtime -- the
page is layout, this is the logic. Everything is Polars over the stored
``board.parquet``; nothing here reads a file or talks to ESPN.

The flex-slot definitions come from ``Scripts.draft.board`` rather than being
restated. Two copies of a positional-eligibility map is exactly the shape that has
already cost this repo twice (12 projection functions, then
``build_league_frame``), and a board that disagreed with its own page about what an
``OP`` slot accepts would be very hard to see.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import warnings
from typing import Dict, List, Mapping, NamedTuple, Optional, Sequence, Tuple

import pandas as pd
import polars as pl

from Scripts.draft.board import FLEX_SLOTS, NON_STARTING_SLOTS


class DraftViewWarning(UserWarning):
    """A derivation degraded rather than failed.

    Its own class for the same reason ``Scripts.draft.board.DraftBoardWarning`` is:
    a page that silently prices an auction in the wrong money is the failure mode
    worth naming, and a caller that wants to promote these to errors should not have
    to promote every ``UserWarning`` in the process.
    """


def _warn(msg: str) -> None:
    """Warn with :class:`DraftViewWarning`, from the caller's frame.

    Args:
        msg: What degraded, and what the reader gets instead.
    """
    warnings.warn(msg, DraftViewWarning, stacklevel=3)

#: Position to categorical slot, fixed. Colour follows the position, not its rank
#: in the current filter -- deselecting kickers must not repaint running backs.
#:
#: Eight positions get a hue, which is the palette's limit; a ninth would have to
#: be a generated colour, and generated hues are what break colour-blind safety.
#: The IDP league has more than eight positions in its pool, so the chart caps the
#: series it draws and says so rather than inventing colours.
POSITION_HUES: Dict[str, int] = {
    "QB": 1, "RB": 2, "WR": 3, "TE": 4, "K": 5, "D/ST": 6, "LB": 7, "DL": 8,
}

#: The validated categorical palette, keyed by slot, per theme.
SERIES_COLORS: Dict[str, Dict[int, str]] = {
    "light": {1: "#2a78d6", 2: "#eb6834", 3: "#1baf7a", 4: "#eda100",
              5: "#e87ba4", 6: "#008300", 7: "#4a3aa7", 8: "#e34948"},
    "dark": {1: "#3987e5", 2: "#d95926", 3: "#199e70", 4: "#c98500",
             5: "#d55181", 6: "#008300", 7: "#9085e9", 8: "#e66767"},
}

#: Chart ink, per theme: (gridline, axis/label muted, primary text, surface).
#: ``surface`` is the page background a mark sits on, used as the stroke on
#: overlapping marks so two dots at the same coordinate read as two.
CHART_INK: Dict[str, Dict[str, str]] = {
    "light": {"grid": "#e1e0d9", "muted": "#898781", "text": "#0b0b0b",
              "surface": "#ffffff"},
    "dark": {"grid": "#2c2c2a", "muted": "#898781", "text": "#ffffff",
             "surface": "#0e1117"},
}

# Tier is deliberately *not* colour-encoded anywhere. It reads as an ordinal blue
# ramp, and eight tiers cannot be stepped down one hue with visible gaps between
# them -- squeezing eight steps into the range whose light end still clears the
# surface leaves adjacent lightness differences of 0.047, which the validator fails
# and the eye cannot separate either. So the tier runway puts tier on an axis and
# colours by position instead, which reuses the categorical palette that does pass
# and keeps one meaning per colour across both charts on the page.

#: How deep the scarcity curve runs past replacement level. Two positions' curves
#: only compare where both still have startable players; far past that every
#: position is flat near zero and the cliff -- the thing the chart exists to show
#: -- is squeezed into the left edge.
SCARCITY_DEPTH = 1.6

#: What ``model_evidence`` says when the model priced a player and flagged nothing.
#: A visible mark rather than an empty cell, because a blank in this column would be
#: indistinguishable from the model having no opinion at all -- which is the whole
#: distinction the column exists to draw.
EVIDENCE_CLEAR = "—"

#: What it says when the model produced no projection. Two different reasons, kept
#: apart on purpose: ``availability`` is the model declining a player it could see
#: (its expected-games estimate was too low to price), ``injury`` is the report
#: withdrawing one it had already priced. Both read as an empty ``USG``, and telling
#: them apart is the difference between "no data" and "actively withheld".
EVIDENCE_WITHDRAWN_AVAILABILITY = "withdrawn (availability)"
EVIDENCE_WITHDRAWN_INJURY = "withdrawn (injury)"

#: And a third: the depth chart says he is a backup and ESPN has priced him out, so
#: the board build withdrew a line that was only high because the model puts everyone
#: on a starter's slate. Distinct from the injury withdrawal because the remedy is
#: different -- an injured starter comes back, a backup needs someone ahead of him to
#: get hurt.
EVIDENCE_WITHDRAWN_ROLE = "withdrawn (backup)"

#: The marker ``Scripts.season_projections.ROLE_WITHDRAWN_EVIDENCE`` writes. Duplicated
#: rather than imported: this module is loaded by a process that only reads parquet,
#: and importing the board builder would drag in the ESPN and scoring stack with it.
#: ``test_draft_view`` pins the two equal.
EVIDENCE_ROLE_MARKER = "withdrawn: backup"

#: And when the model does not cover the position at all -- K and D/ST, which it has
#: never modelled, plus anyone it had no usage history for.
EVIDENCE_NOT_MODELLED = "not modelled"

#: ESPN's fantasy injury status, abbreviated to fit a column you scan rather than read.
#:
#: Five of these are observed in the 2026 pool (``ACTIVE`` 2,204, ``QUESTIONABLE`` 162,
#: ``OUT`` 57, ``INJURY_RESERVE`` 22, ``SUSPENSION`` 1); the other three are ESPN's
#: documented enum and cost nothing to carry. A status **not** in this map is rendered
#: in full rather than shortened -- see :func:`with_injury_code`.
INJURY_CODES: Dict[str, str] = {
    "ACTIVE": "A",
    "PROBABLE": "P",
    "QUESTIONABLE": "Q",
    "DOUBTFUL": "D",
    "OUT": "O",
    "INJURY_RESERVE": "IR",
    "SUSPENSION": "SUS",
    "DAY_TO_DAY": "DTD",
}

#: How each rung of the severity ladder reads on the board.
#:
#: Short because the column is narrow, and ordered by trust: an ``override`` is a human
#: who read the beat report, ``ESPN dx`` is a published diagnosis, ``ESPN date`` is
#: ESPN's own return estimate, ``news text`` is a regex over a sentence, and ``report``
#: is a body part with no severity attached at all. Spelling the rung out is the point --
#: see :func:`with_injury_severity`.
INJURY_SEVERITY_SOURCES = {
    "override": "override",
    "espn_structured": "ESPN dx",
    "return_date": "ESPN date",
    "comment": "news text",
    "report": "report",
    "none": "unresolved",
}

#: The mark that says "this player has an injury note", in place of the note itself.
#:
#: The note is a sentence and a sentence needs 400px, which on a 26-column table is a
#: quarter of the width spent on a cell that truncated mid-clause anyway. So the column
#: carries a mark you click.
#:
#: It is the value of a ``st.column_config.ButtonColumn`` cell, which is what makes the
#: mark itself the target -- the button's label *is* the cell value. Row selection was
#: tried first and is worse in exactly the way it sounds: enabling it adds a checkbox
#: column at the far left of the table, and the thing you click is nowhere near the
#: thing you are asking about.
#:
#: Click, not hover, and not by choice: Streamlit's grid has no per-cell tooltip.
#: ``help=`` on a column config is the *header* tooltip, and hovering a truncated cell
#: produces nothing -- checked in the browser rather than assumed, because the obvious
#: design here is an icon you hover and it does not exist.
NOTE_MARK = ":material/sticky_note_2:"

#: Session-state key the news-mark button writes its click into.
#:
#: Streamlit puts ``{"row": n, "label": s}`` here on the rerun a click triggers, and
#: clears it on the next one -- so it is read once and resolved to something durable.
NOTE_CLICK_KEY = "note_click"

#: The Values tab's own, because two widgets cannot share a key. Its table renders the
#: same `News` column, and a table where the mark did nothing would be worse than no
#: mark -- the alternative was special-casing the column out of that frame.
VALUES_NOTE_CLICK_KEY = "values_note_click"

#: Prefix for where the *resolved* player is remembered. Per-league, for the reason
#: :func:`budget_key` is: a note carried across a league change would point at a player
#: the new board may not hold.
NOTE_HELD_KEY = "note_open"

#: What ``Exp Return`` says where ESPN's estimate is past the end of the season.
#:
#: The report encodes a season-ending injury as a return date in February, so the raw
#: value is a real date that means something other than a date. Rendering it as a word
#: is the difference between a reader knowing that and having to.
RETURN_SEASON_ENDING = "Season"

#: The diverging fill for the difference columns: green where we are higher on a
#: player than ESPN, red where we are lower, nothing in the middle.
#:
#: Two steps per arm, so magnitude reads as well as sign. Keyed by signed step, and
#: the neutral middle is ``None`` rather than a grey -- a table where two thirds of
#: the cells are painted has no emphasis left to spend.
#:
#: **Green/red is the requested pairing and it is CVD-marginal**, which is worth
#: recording rather than discovering later. Measured with the dataviz validator, the
#: two arms separate at ΔE 6.9 under deuteranopia (the 6-8 floor band, legal only with
#: a second channel) against 31.3 under normal vision. The second channel here is not
#: decoration: every one of these columns is formatted ``%+`` so the sign is printed
#: in the cell, and a reader who cannot see the hue reads the number. Swapping the red
#: arm for the palette's blue would clear the check outright and is a change to this
#: dict and nothing else.
#:
#: The soft steps are deliberately not near-white tints. The first attempt used
#: ``#d7f0e5``/``#fadbda``, which the validator caught at ΔE 6.4 under *normal*
#: vision -- a "soft green" and "soft red" nobody could tell apart, which is worse
#: than no fill at all because it looks like information.
DELTA_FILLS: Dict[str, Dict[int, Optional[str]]] = {
    "light": {-2: "#e34948", -1: "#f4a3a2", 0: None, 1: "#9ddcc2", 2: "#1baf7a"},
    "dark":  {-2: "#b62b2b", -1: "#8a3a38", 0: None, 1: "#1f6b4f", 2: "#0f7a55"},
}

#: Ink for a filled cell. One colour per theme across all four fills, so a column
#: does not switch text colour partway down. Every pairing clears 4.5:1 -- the
#: tightest is light strong red at 4.98.
DELTA_INK: Dict[str, str] = {"light": "#0b0b0b", "dark": "#ffffff"}

#: Where a difference column's fill steps up, as a fraction of that column's own
#: 90th-percentile magnitude.
#:
#: Scaled per column because the difference columns are not in the same units --
#: points differences run in tens, rank differences in hundreds, cash in single
#: dollars. A shared absolute threshold would paint every rank cell and no cash cell.
#: The 90th percentile rather than the maximum because these columns have long tails:
#: one player 1,600 ranks off ESPN would otherwise flatten everybody else to neutral.
DELTA_STRONG_AT = 0.5
DELTA_SOFT_AT = 0.15

#: How to measure "the room is wrong about this player".
#:
#: Two different questions, and which one is right is a property of the draft
#: rather than a preference. In a snake draft a pick is a place in a queue, so the
#: comparison is rank against rank. In an auction there is no queue -- there is a
#: price -- and being four places underrated tells you nothing about whether to bid
#: $41 or $46.
#:
#: Declared above :data:`COLUMNS` because the ``Draft Metric`` block is gated on it:
#: the same three headers read ADP in one lens and dollars in the other.
VALUE_LENS_ADP = "ADP"
VALUE_LENS_CASH = "Cash"


class Column(NamedTuple):
    """One column of the board table: how to find it, format it and explain it.

    One record per column rather than three parallel structures keyed on the label.
    The previous shape was a ``(source, label)`` list here, a glossary dict there and
    a ``column_config`` dict in the page, and keeping three keyed collections in step
    needed a test to enforce what a single record makes true by construction. The
    page's copy could not even be tested: Streamlit ignores config for a column the
    frame does not carry, so a stale label there stopped formatting in silence.

    Attributes:
        source: Board column to read. Two specs may share one source -- ``vor_rank``
            is both our overall rank and our implied pick order, and each comparison
            reads better with it than with a cross-reference.
        group: Spanner header. Rendered as the upper level of the frame's column
            MultiIndex, which Streamlit draws as a group above the leaf headers.
        label: Leaf header. **Not unique** -- ``ESPN``, ``Us`` and ``Δ`` each repeat
            across groups, which is the whole point of the spanners and the reason
            ``column_config`` is keyed by position rather than by name.
        kind: ``text``, ``number``, or ``button`` for a cell that is itself the
            control -- see :data:`NOTE_MARK`.
        fmt: printf format for a number column. Left as printf rather than escaped,
            because ``column_config`` formats are not markdown.
        source_of: Glossary: where the number originates.
        how: Glossary: how it is computed.
        caveat: Glossary: what it does not say. Empty where there is nothing to warn
            about, which is most of the identity block.
        pinned: Freeze the column against horizontal scrolling.
        emphasis: Bold. Set on the difference columns, which are the judgements.
        shade: ``delta`` to fill the cell on the diverging scale, ``""`` not to.
            Only the difference columns are filled -- see :class:`Shading`.
        lens: ``""`` to always render, else the :data:`VALUE_LENS_ADP` or
            :data:`VALUE_LENS_CASH` this spec belongs to.
        positions: ``()`` to always render, else the positions this column can hold a
            value for. A source that models **one** position -- the D/ST model, the
            kicker model -- is null on every other row, and a column of blanks is not
            a neutral thing to show: it reads as missing data about the player rather
            than as a question that was never asked of him. So it is dropped entirely
            unless the frame on screen actually contains one of these positions.
            Presence in the frame is not enough on its own, because the board carries
            the column for all 2,504 rows and only 32 of them are defences.
        width: Streamlit width hint, for the one column holding a sentence.
    """

    source: str
    group: str
    label: str
    kind: str
    source_of: str
    how: str
    fmt: Optional[str] = None
    caveat: str = ""
    pinned: bool = False
    emphasis: bool = False
    shade: str = ""
    lens: str = ""
    positions: Tuple[str, ...] = ()
    width: Optional[str] = None


#: Every column the board can show, in render order.
#:
#: The table reads left to right as: who they are, then four comparisons of *our*
#: number against *ESPN's* -- points, overall rank, positional rank, and the draft's
#: own currency -- then what it costs to keep them, then what is wrong with them.
#:
#: **The comparison groups all use the same three headers, and the difference in each
#: is oriented the same way.** ``Δ`` is positive wherever we are higher on a player
#: than ESPN is: points differenced ours-minus-theirs, ranks theirs-minus-ours. That
#: single rule is what lets one colour scale serve every difference column, and it is
#: the convention ``value`` already set before any of this was grouped.
#:
#: A spec whose source the board does not carry drops out silently -- see
#: :func:`display_frame`. That is what makes one table serve a redraft league with no
#: keeper prices and a board built before the usage model existed.
COLUMNS: List[Column] = [
    # --- who they are ---------------------------------------------------
    Column("player_name", "Player Info", "Player", "text", pinned=True,
           source_of="ESPN",
           how="The name as ESPN spells it, from `kona_player_info`."),
    Column("primaryPosition", "Player Info", "Pos", "text", pinned=True,
           source_of="ESPN",
           how="Primary position. Replacement level and tiers are both computed "
               "*within* position, so this drives most of the table."),
    Column("pro_team", "Player Info", "NFL", "text", pinned=True,
           source_of="ESPN", how="Pro team abbreviation."),
    Column("bye_week", "Player Info", "Bye", "number", fmt="%.0f", pinned=True,
           source_of="NFL schedule",
           how="Derived rather than read — a bye is an absence, so it is the week in "
               "which the team appears in neither the home nor the away column.",
           caveat="Blank when the schedule is missing or covers another season."),
    Column("tier", "Player Info", "Tier", "number", fmt="%.0f", pinned=True,
           source_of="Board build",
           how="1-D KMeans on projected points within position, so the breaks land "
               "where the gaps actually are rather than every N players. 1 is best.",
           caveat="Not colour-coded, deliberately: eight ordinal steps cannot be "
                  "given separable lightness, so the ramp would read as decoration."),

    # --- points ---------------------------------------------------------
    Column("ESPN_Points", "Points", "ESPN", "number", fmt="%.1f",
           source_of="ESPN",
           how="ESPN's own projected stat line, scored through **this league's** "
               "rules rather than ESPN's — so it is comparable to the column beside "
               "it, and differs from what ESPN's site shows you.",
           caveat="Blank for the players ESPN projects no line for at all — about "
                  "one in eight of the pool that has a projection."),
    Column("FP_Points", "Points", "FP", "number", fmt="%.1f",
           source_of="FantasyPros",
           how="FantasyPros' consensus season projection, scored through **this "
               "league's** rules like every column beside it. It is an aggregate of "
               "many analysts rather than one house view, which is why it is the "
               "closest thing here to a second opinion on ESPN.",
           caveat="Covered 60 players until 2026-08-24 — ten per position, all "
                  "FantasyPros serves without an account — and was imputed from the "
                  "ESPN/FP mean for everyone else. It now covers 592, so a blank here "
                  "means genuinely unprojected rather than merely unseen."),
    Column("BOL_Points", "Points", "BOL", "number", fmt="%.1f",
           source_of="BetOnline",
           how="Season-long props from BetOnline, de-vigged and converted to a stat "
               "line. The only source here priced by people with money at stake "
               "rather than by analysts, which is why it earns a quarter of the "
               "blend on the players it covers.",
           caveat="Thin by construction — a book prices the players it can take "
                  "action on, so it reaches 96 players on receiving yards and 25 on "
                  "passing, and is real for 73 of the top 150 by ADP. Where it is "
                  "blank the weight is dropped and the other sources renormalise, so "
                  "a gap here costs nothing; it simply is not an opinion."),
    Column("PINNY_Points", "Points", "PIN", "number", fmt="%.1f",
           source_of="Pinnacle",
           how="Pinnacle season props, de-vigged into a stat line. A second sportsbook "
               "beside BetOnline, and the sharper of the two — Pinnacle's whole business "
               "is taking the bet rather than shading the line.",
           caveat="The thinnest source here: real for roughly 21–30 players a stat and "
                  "none at all on passing touchdowns or receptions. Weighted an equal "
                  "quarter since 2026-08-24 — it had been zeroed for thin coverage, which "
                  "double-counted an objection the imputation flags already handle."),
    Column("TRUE_Points", "Points", "Us", "number", fmt="%.1f",
           source_of="Blend",
           how="ESPN, FantasyPros, BetOnline and the usage model in equal quarters, "
               "plus the D/ST model at a quarter on team defences only — blended as a "
               "**stat line**, then scored through this league's own rules, which is "
               "what lets one pipeline serve nine leagues. Pinnacle and the kicker "
               "model are weighted zero.",
           caveat="A source with no line for a player is dropped and the rest "
                  "reweighted. That used to make most players an ESPN/usage blend, "
                  "because FantasyPros reached only 60; since 2026-08-24 it reaches "
                  "592 and carries roughly 0.45 of the realised weight where a stat "
                  "is live. Every rank, tier and VOR on this table is built from "
                  "this column."),
    Column("USG_Points", "Points", "TOM", "number", fmt="%.1f",
           source_of="TOMCAT",
           how="**TOMCAT** — Touches, Opportunity, Market, Context, Availability, Tiers — "
               "is our own model, and the only source here built from observed usage "
               "rather than from somebody else's projection. This is its usage arm — "
               "QB, RB, WR and TE — quoted over a full healthy 17 games, so it means "
               "the same thing as the columns beside it: the availability estimate is "
               "divided back out rather than baked in.",
           caveat="Runs a few percent below `ESPN` at the top of the board because the "
                  "model shrinks toward positional baselines while ESPN extrapolates. "
                  "That is disagreement about players, not a scale difference — but "
                  "`Position Ranks | Δ TOM` is still the cleaner read, since a rank "
                  "cannot be moved by it at all. The column is still named `USG_` "
                  "underneath: renaming it would orphan the frozen G2 archive."),
    Column("DST_Points", "Points", "DST", "number", fmt="%.1f",
           positions=("D/ST",),
           source_of="TOMCAT · defence arm",
           how="TOMCAT's defence arm — the same model as `TOM`, a different backend. "
               "Team defence projected from the **betting market** rather than from "
               "last season: implied points allowed beats prior season on seven of "
               "eight components, because opponent offences drive defensive events "
               "and the market prices opponent offences. The points-allowed and "
               "yards-allowed ladders are integrated over a weekly distribution "
               "rather than scored at the season mean.",
           caveat="Team defences only — blank at every other position, which is not a "
                  "gap. It carries TOMCAT's vote on a D/ST row the way `TOM` carries it "
                  "on a receiver's. Blended at a quarter since 2026-08-24, so `Us` already "
                  "carries it; this column is what the model says on its own. It "
                  "cleared its gate against prior-season points by 34–46% in all nine "
                  "leagues, but the gate against **ESPN** cannot be run until 2027, so "
                  "read it as a co-equal second opinion rather than the better number."),
    Column("points_delta", "Points", "Δ", "number", fmt="%+.1f", emphasis=True,
           shade="delta",
           source_of="Board build",
           how="`Us − ESPN`. Positive means we project more points than ESPN does.",
           caveat="Damped, because ESPN is one of the quarters inside `Us` — and on a "
                  "player only ESPN and the model price, a full half of it. It reads "
                  "as *how far the blend moved off ESPN*, not as two independent "
                  "opinions. Blank wherever `ESPN` is."),
    Column("vor", "Points", "VOR", "number", fmt="%.1f",
           source_of="Board build",
           how="Projected points minus the projected points of the last startable "
               "player at that position. Replacement rank is this league's own "
               "starting slots × teams, which is why the same player is worth "
               "different amounts in different leagues.",
           caveat="The table's default sort, and the one number here that already "
                  "accounts for positional scarcity."),

    # --- overall rank ---------------------------------------------------
    Column("espn_draft_rank", "Ranks", "ESPN", "number", fmt="%.0f",
           source_of="ESPN",
           how="`draftRanksByRankType` — ESPN's published draft ranking. Dense and "
               "complete: 1 to N, no ties, every player.",
           caveat="ESPN's editorial opinion, not the room's. What the room actually "
                  "does is `Draft Metric`, and the two disagree often."),
    Column("vor_rank", "Ranks", "Us", "number", fmt="%.0f",
           source_of="Board build", how="Rank of VOR across the whole pool. 1 is best."),
    Column("rank_delta", "Ranks", "Δ", "number", fmt="%+.0f", emphasis=True,
           shade="delta",
           source_of="Board build",
           how="`ESPN − Us`. Positive means ESPN ranks him later than we do.",
           caveat="Ranked over the whole pool, so the tails are dominated by deep "
                  "players whose ranks nobody has thought hard about."),

    # --- positional rank ------------------------------------------------
    Column("espn_pos_rank", "Position Ranks", "ESPN", "number", fmt="%.0f",
           source_of="Board build",
           how="ESPN's draft ranking ranked again within position — so it is ESPN's "
               "own ordering of the position, not a re-ranking of its projections."),
    Column("pos_rank", "Position Ranks", "Us", "number", fmt="%.0f",
           source_of="Board build", how="Rank of projected points within position."),
    Column("pos_rank_delta", "Position Ranks", "Δ", "number", fmt="%+.0f",
           emphasis=True, shade="delta", source_of="Board build",
           how="`ESPN − Us`. Positive means ESPN ranks him later within his position "
               "than we do.",
           caveat="More useful than the overall Δ beside it, because a positional "
                  "rank is what you are actually choosing between on the clock."),
    Column("USG_PosRankDelta", "Position Ranks", "Δ TOM", "number", fmt="%+.0f",
           emphasis=True, shade="delta", source_of="Usage model",
           how="`Us − USG` within position — our rank minus the usage model's. "
               "Positive means the model likes him more than ESPN and FantasyPros do.",
           caveat="Not an outside opinion: the model is one of the three voices "
                  "already inside `Us`, shown separately so you can see it pull. "
                  "Being a rank, it survives the level mismatch that denies `USG` a Δ."),

    # --- the draft's own currency ---------------------------------------
    Column("adp", "Draft Metric", "ESPN", "number", fmt="%.1f", lens=VALUE_LENS_ADP,
           source_of="ESPN",
           how="`averageDraftPosition` — the average pick across real ESPN drafts, so "
               "it is fractional. This is the room, not ESPN's editors.",
           caveat="Everyone the market has no opinion on is parked on a single "
                  "plateau value — 758 of 1,000 players shared 170.0 in 2026 — and "
                  "ranking inside that plateau is noise."),
    Column("vor_rank", "Draft Metric", "Us", "number", fmt="%.0f", lens=VALUE_LENS_ADP,
           source_of="Board build",
           how="Our VOR rank again, as an implied pick order. Repeated from `Ranks` "
               "on purpose, so this comparison reads without cross-referencing."),
    Column("value", "Draft Metric", "Δ", "number", fmt="%+.0f", emphasis=True,
           shade="delta", lens=VALUE_LENS_ADP, source_of="Board build",
           how="ADP rank minus VOR rank, **both re-ranked over the players the market "
               "has actually priced**, excluding the streamed positions. Positive "
               "means the room is letting him fall.",
           caveat="Because of that re-ranking it will not equal `ESPN − Us` "
                  "arithmetically. Blank wherever the market set no price, and for K "
                  "and D/ST — season-total VOR assumes you hold one all season, and "
                  "you stream them. The first version of this board scored eight team "
                  "defences as the league's best values on exactly that mistake."),
    Column("auction_dollars", "Draft Metric", "ESPN", "number", fmt="$%.0f",
           lens=VALUE_LENS_CASH, source_of="ESPN, rescaled",
           how="`auctionValueAverage`, the average winning bid across ESPN auctions, "
               "falling back to ESPN's own suggested value. Rescaled from the $200 "
               "budget ESPN publishes against to the budget set above.",
           caveat="A rescale, not a valuation. A real auction's minimum bid does not "
                  "scale with the budget, so the top of the board is worth slightly "
                  "more than the multiple says — and nothing here knows what your "
                  "room actually pays for a quarterback."),
    Column("our_dollars", "Draft Metric", "Us", "number", fmt="$%.0f",
           lens=VALUE_LENS_CASH, source_of="Derived here",
           how="Our own valuation in dollars, **out of the budget you actually have**: "
               "every roster spot the room fills costs at least $1, and what is left "
               "splits in proportion to points above replacement. Read it off and bid "
               "it — the top of a $250 board comes out around $145.",
           caveat="Aggressive by construction: it spends the whole budget on the ~106 "
                  "players worth rostering, where the market spreads the same money "
                  "over ~313. So it sits above ESPN's price for most players inside "
                  "the money, and the Δ beside it is best read as an ordering — who "
                  "the room is *most* wrong about — rather than a verdict on each row. "
                  "Blank outside the money, and for K and D/ST."),
    Column("cash_delta", "Draft Metric", "Δ", "number", fmt="%+.0f", emphasis=True,
           shade="delta", lens=VALUE_LENS_CASH, source_of="Derived here",
           how="`Us − ESPN`. Positive means the room underpays. The auction answer to "
               "the question `value` asks of a snake draft."),

    # --- what it costs to keep them -------------------------------------
    Column("keeper_price", "Keepers", "Price", "number", fmt="$%.0f",
           source_of="ESPN",
           how="`keeperValue` — what the manager holding him paid to acquire him, "
               "floored at the $1 minimum for a player claimed off waivers, who has "
               "no winning bid to record.",
           caveat="Blank means nobody holds him, which is not the same as free."),
    Column("keeper_surplus", "Keepers", "Value", "number", fmt="%+.0f", emphasis=True,
           shade="delta", source_of="Derived here",
           how="**Our** dollar valuation minus the keeper price. Positive means he is "
               "worth more to us than keeping him costs.",
           caveat="Against our valuation rather than the market's, because what "
                  "decides whether to keep a player is whether *we* rate him that "
                  "highly — the room's price is a fact about other people's money. "
                  "Both sides are in this league's real dollars: the keeper price is "
                  "what the holder actually paid, ours is what we would spend of the "
                  "same budget. Blank outside the money and for K and D/ST, where we "
                  "publish no dollar valuation at all."),
    Column("team_owner", "Keepers", "Owner", "text", source_of="ESPN",
           how="The fantasy team holding him.",
           caveat="Before a keeper league's keepers are declared this is **last "
                  "season's** roster, not this year's. In a redraft league it is the "
                  "only column in this group."),

    # --- what is wrong with them ----------------------------------------
    Column("injury_code", "Notes", "Injury", "text", source_of="ESPN injury report",
           how="ESPN's fantasy status, abbreviated: **A**ctive, **P**robable, "
               "**Q**uestionable, **D**oubtful, **O**ut, **IR** injured reserve, "
               "**SUS**pension.",
           caveat="A status ESPN has not used before is shown in full rather than "
                  "abbreviated, so a new one is visible instead of silently mangled."),
    Column("injury_return_date", "Notes", "Exp Return", "text",
           source_of="ESPN injury report",
           how="ESPN's estimated return date. `Season` where the estimate is past the "
               "season's end, which is how the report encodes a season-ending injury.",
           caveat="Blank means no estimate published, not no injury — ESPN dates only "
                  "about one in seven of the players it lists. `IR` alone does not "
                  "mean out for the year; this column is what answers that."),
    Column("usg_role_confidence_pct", "Notes", "Role %", "number", fmt="%.0f%%",
           source_of="Usage model",
           how="How often the pre-season depth chart turns out to be right for a "
               "player in this one's situation — measured by rebuilding the chart "
               "each season from who actually got the ball in the first three games "
               "(plan 33). A listed starter really is one 59% of the time if he is "
               "settled, 45% if he changed teams and 36% if he is a rookie.",
           caveat="**This does not scale the projection and is not a grade on the "
                  "player.** It is how much of the projection rests on a depth-chart "
                  "entry that may not hold. A low number beside a high projection is "
                  "the combination worth a second look: the model is confident about "
                  "a role nobody should be confident about. **Compare within a depth "
                  "rank, not across one** — being right that a man is third string is "
                  "easier than picking the starter, so a rookie listed third scores "
                  "above a starter who changed teams without being better known. "
                  "Blank where the calibration has not been fitted, or for a position "
                  "group it has never seen."),
    # --- what the season could be, not just its mean (plan 28) ----------
    #
    # A separate group from Points, because these answer a different question. The
    # Points block is a level -- how much. This is a *range*, and the board's own
    # floor/ceiling is not one: `attach_source_spread` measures how far the forecasters
    # disagree, which is not how uncertain the forecast is. Measured, the two are 17.5x
    # apart, and the board's floor-to-ceiling contains 4.6% of realised outcomes against
    # the ~80% those words imply.
    Column("pts_p10", "Range", "p10", "number", fmt="%.0f",
           source_of="Outcome simulation",
           how="A bad but not disastrous season: he beats this in 9 years out of 10. "
               "Simulated from the usage model's own fitted per-stat distributions, "
               "then rescaled onto `Us` so it brackets the number you are reading.",
           caveat="Forecast uncertainty, **not** source disagreement — a different "
                  "quantity from `Floor`, and much wider. **Measured too narrow:** "
                  "walk-forward over 2021–2025 the p10–p90 band contains **68%** of "
                  "realised seasons for players projected above 25 points, against the "
                  "80% it is built for. So treat `p90` as a good season rather than a "
                  "ceiling, and `p10` as optimistic about the downside. It is still far "
                  "closer than `Floor`–`Ceiling`, which contains 4.6%."),
    Column("pts_p90", "Range", "p90", "number", fmt="%.0f",
           source_of="Outcome simulation",
           how="The season that makes your year — nominally one year in ten, and "
               "measured closer to one in six.",
           caveat="Same simulation as `p10`, and the same measured shortfall: 68% "
                  "realised coverage against a nominal 80%. Read the pair, not either "
                  "alone."),
    Column("p_top12_pct", "Range", "Top", "number", fmt="%.0f%%",
           source_of="Outcome simulation",
           how="How often he finishes in his **position's** starter tier across the "
               "simulated seasons — top 12 at QB and TE, top 24 at RB, top 36 at WR. "
               "A within-simulation rank, so it asks how often he *gets there*, which "
               "is not what his mean already tells you.",
           caveat="Position-relative, so it does **not** compare across positions — "
                  "`VOR` is the column for that. Measured against the mean ordering it "
                  "moves 13% of draftable players by 12+ places, and almost all of that "
                  "is at receiver."),
    Column("p_bust_pct", "Range", "Bust", "number", fmt="%.0f%%",
           source_of="Outcome simulation",
           how="How often he finishes below half his own projection — injury, a lost "
               "job, or simply the low end of his range.",
           caveat="Relative to his own number, so a 10% bust rate means something "
                  "different for a first-rounder and a bench flier."),
    Column("outcome_evidence", "Notes", "Range Evidence", "text",
           source_of="Derived here",
           how="Whether the range was simulated, and which of that league's scored "
               "rules the simulation cannot price.",
           caveat="The simulation covers the eight stats the usage model projects. "
                  "Two-point conversions are in no source at all, and GOP scores "
                  "rushing attempts and completions. The two leagues that price "
                  "per-game yardage bonuses now have them in the **projection** — "
                  "`Scripts/usage/milestones.py` integrates a weekly distribution "
                  "over the ladder — but not yet in this **range**, which is drawn "
                  "over season totals. And a quarterback's floor carries a marginal "
                  "measured at 58.9% coverage against a nominal 80%: read `p10` at "
                  "quarterback as indicative, not as a tenth."),
    Column("usg_expected_games", "Notes", "Exp G", "number", fmt="%.1f",
           source_of="Usage model",
           how="Games out of 17 the model expects him to play — its own estimate, "
               "fitted from prior availability, snap share and age.",
           caveat="**`USG` is not scaled by this**, and deliberately so: the column to "
                  "its left is a healthy-slate line, on ESPN's footing. This is the "
                  "availability view, kept separate so you can apply it yourself. It "
                  "carries role as well as health, so a low number on a backup means "
                  "*buried*, not *fragile*. Only present for players the model "
                  "covers."),
    Column("usg_evidence_label", "Notes", "Model Evidence", "text",
           source_of="Derived here",
           how="Why the model's number is thin, or which of the four ways it "
               "produced none — it does not cover the position, it declined to price "
               "him, the injury report withdrew a price it had made, or he is a "
               "backup ESPN has priced out.",
           caveat="Exists because an empty `USG` meant three different things and all "
                  "three rendered as the same blank cell, which reads as agreement."),
    Column("inj_severity_label", "Notes", "Body Part", "text",
           source_of="Injury model",
           how="What is wrong with him and which channel said so, resolved through a "
               "precedence ladder: a hand-written override, then ESPN's structured "
               "diagnosis, then its estimated return date, then the news text, then the "
               "weekly report's body part.",
           caveat="**The provenance in brackets is the important half.** On this board "
                  "most of these came from `news text` — a regex over one sentence — and "
                  "those are a body part with a group-average duration attached, not a "
                  "diagnosis. `ESPN dx` and `override` are worth acting on; `news text` "
                  "is worth checking."),
    Column("inj_expected_absence_weeks", "Notes", "Wks Out", "number", fmt="%.1f",
           source_of="Injury model",
           how="Games he is expected to miss from week 1, the midpoint of a range. From "
               "ESPN's return date where it published one, otherwise the measured "
               "average for that body part over 2016–2025.",
           caveat="**Nothing on this board is discounted by it.** `TRUE` and every "
                  "source column are healthy-season lines; this is the availability "
                  "view, kept beside them the way `Exp G` is. A body part average is a "
                  "wide distribution — an ankle spans one week to five — so read it as "
                  "an order of magnitude, not a date."),
    Column("inj_recovery_cost", "Notes", "Form Cost", "number", fmt="%.2f",
           source_of="Injury model",
           how="Games-equivalent of production the return-to-form ramp is expected to "
               "cost, on top of any games missed — the fitted shortfall summed over the "
               "six appearances after he is back. A knee costs about 0.36 of a game, a "
               "hamstring 0.12, a concussion nothing.",
           caveat="**Nothing is discounted by it, and that is a measured decision rather "
                  "than an omission.** The curve behind it is well calibrated — a cell "
                  "predicted to lose 20% loses 20% — but its accuracy gain in a "
                  "walk-forward was ~1% against a 2% bar pre-committed in "
                  "`Scripts/lab/registry.py`, so it is shown and not applied. Zero means "
                  "the model abstained: for concussions and lower-body soft tissue the "
                  "measured post-return shortfall sits inside its own error bars."),
    Column("inj_reinjury_pct", "Notes", "Re-inj", "number", fmt="%.0f%%",
           source_of="Injury model",
           how="Chance the same body part goes again within six weeks of his return, from "
               "a discrete-time hazard fitted over 2016–2025.",
           caveat="This is the **second** cost channel and for some injuries it is the "
                  "only one: a hamstring shows almost no lasting production loss once he "
                  "is back and the highest recurrence rate of any body part, so `Form "
                  "Cost` alone makes it look cheap. The pooled rate validates externally "
                  "(hamstring 9.8% against a published 11.9%); the *weekly* hazard behind "
                  "it failed its own Brier gate, so read this as a body-part rate rather "
                  "than a personal risk."),
    Column("note_mark", "Notes", "News", "button", width="small",
           source_of="ESPN injury report",
           how="A mark where ESPN's injury report carries a note about him. **Click it "
               "to read the note** — the note is a sentence, and a column wide enough "
               "for one costs a quarter of the table and truncated it anyway.",
           caveat="Click rather than hover because Streamlit's grid has no per-cell "
                  "tooltip. Injury news only — this repo fetches no general player "
                  "news, and ESPN's free-text `seasonOutlook` was rejected as "
                  "unparseable. So a blank means he is not on the injury report, not "
                  "that nothing has happened to him."),
]

#: Group order, derived so it cannot disagree with :data:`COLUMNS`.
GROUPS: List[str] = list(dict.fromkeys(column.group for column in COLUMNS))


# --- the auction budget ---------------------------------------------------

#: The budget ESPN's published auction values are denominated in.
#:
#: ESPN's auction default is $200, and the 2026 pool agrees: the 338 players it has
#: actually priced sum to $1,871 of the $2,000 a ten-team $200 auction puts on the
#: table, with the remaining ~$129 spread across the deep bench it prices at pennies.
#: The board stores those dollars as published, so a league playing for $250 was
#: reading a column denominated in somebody else's money.
BASE_AUCTION_BUDGET = 200.0

#: What the budget input starts at.
DEFAULT_AUCTION_BUDGET = 250

#: Prefix for the ``st.session_state`` key the budget input owns. See
#: :func:`budget_key` -- the key is per-league, which is the whole point.
AUCTION_BUDGET_KEY = "auction_budget"


def budget_key(league_key: str) -> str:
    """The session-state key the budget input owns, for one league.

    **Per-league, and that is load-bearing.** A single shared key is remembered
    across a league change, and because a keyed widget ignores its ``value=``
    once the key exists, GOP Degenerates' $250 auction rendered at Winfield's
    $200 -- every price on the board 20% light, with nothing on screen saying so.
    Clearing the key on a league change was tried first and is the fragile
    version of this: it depends on the pop landing before the widget registers,
    and it throws away a budget you deliberately set.

    Scoping the key instead fixes the bug and is better behaviour besides: each
    league remembers its own number, so overriding GOP to $300 and coming back to
    it still says $300.

    Named here rather than in the page so the page can read the current budget
    *before* the widget that sets it is drawn -- Streamlit reruns top to bottom
    with session state already updated, so reading the key is what makes the
    ``Draft Metric | ESPN`` column right on the same run in which it changed, and
    on the tabs that render before the input.

    Args:
        league_key: ``config.yaml`` league key.

    Returns:
        str: The session-state key.
    """
    return f"{AUCTION_BUDGET_KEY}::{league_key}"


def at_budget(board: pl.DataFrame, budget: float,
              base: float = BASE_AUCTION_BUDGET,
              meta: Optional[Mapping] = None) -> pl.DataFrame:
    """Re-price ESPN's auction values into the budget this league actually plays for.

    The stored value is a market average in ESPN's own $200 auction. Held as a
    **share of a budget** it is portable, so this carries both: ``auction_share``
    is the fraction of one team's money the market puts on the player, and
    ``auction_dollars`` is that share at ``budget``. The table shows the dollars;
    the share is what makes them meaningful.

    **With ``meta`` it allocates rather than rescales, and that is the fix for a
    real bug.** A straight proportion never sees team count, so the market total
    landed wrong in both directions -- GOP Degenerates' 16 x $250 came out $2,544
    against $4,000 of actual money, 1.57x light, while six-team Winfield ran 0.63x
    heavy. ``cash_delta`` was therefore differencing our correctly-pooled dollars
    against a mis-scaled market, and the sign of that error changed with league
    size. Given ``meta`` the market side goes through
    :func:`allocate_dollars` -- the same construction ``our_dollars`` uses, over the
    same eligible pool -- so both columns sum to ``teams x budget`` and their
    difference means something. ``docs/plans/09-frontend-draft-views.md`` named this
    fix and deferred it; The Sheet is the decision about the cash lens it was
    waiting for.

    Without ``meta`` it falls back to the proportional rescale and warns, which is
    the same degrade-visibly contract every attacher in ``build_board`` has. The
    fallback is honest to a point and no further: a real auction's minimum bid does
    not scale -- the last roster spots cost $1 whatever the budget is -- so raising
    the budget adds slightly more to the top of the board than a flat multiple
    suggests.

    ``auction_share`` is unchanged either way. It is the fraction of one team's
    money the market puts on a player and it is what makes the stored values
    portable at all, so it stays denominated in ``base``.

    Args:
        board: A stored draft board.
        budget: This league's per-team auction budget.
        base: Budget the stored values are denominated in. Overridable so the
            assumption is visible rather than buried in a literal.
        meta: The store's ``meta.json``, for team count and roster shape. Without
            it the dollars are a proportional rescale and a warning says so.

    Returns:
        pl.DataFrame: The board with ``auction_share`` and ``auction_dollars``
        added, or unchanged when it carries no auction column -- the ``Draft Metric``
        dollars then drop out of :func:`display_frame` exactly as any other absent
        column does.
    """
    if "auction_value_filled" not in board.columns or not base:
        return board
    share = pl.col("auction_value_filled") / float(base)
    out = board.with_columns(share.alias("auction_share"))

    spots = draftable_spots(meta) if meta else 0
    teams = int((meta or {}).get("team_count") or 0)
    if spots and teams:
        # Priced to everyone the market prices, including the streamed positions --
        # what the room pays for a defence is worth knowing. Normalised over the
        # *eligible* pool only, so the total matches `our_dollars`' total and the
        # two are differenceable. See `allocate_dollars`.
        weight = (pl.when(pl.col("auction_value_filled").is_not_null()
                          & (pl.col("auction_value_filled") > 0))
                  .then(pl.col("auction_value_filled")).otherwise(None))
        allocated = allocate_dollars(out, weight, spots, teams, budget,
                                     "auction_dollars", pool=_cash_eligible(out),
                                     price_outside_pool=True)
        if allocated is not None:
            return allocated
        # `meta` was fine; ESPN priced nobody on this board. There is nothing to
        # allocate and nothing to warn about -- the proportional rescale of an empty
        # column is equally empty, and saying "no meta" here would be a lie.
        return out.with_columns((share * float(budget)).alias("auction_dollars"))

    _warn(
        "at_budget got no meta, so the market's auction dollars are a proportional "
        "rescale of ESPN's $200 values and do not sum to this league's money. "
        "`cash_delta` against them is off by the team-count ratio -- 1.57x light for a "
        "16-team $250 auction. Pass meta to allocate instead."
    )
    return out.with_columns((share * float(budget)).alias("auction_dollars"))


# --- the glossary ---------------------------------------------------------


def escape_dollars(text: str) -> str:
    """Stop Streamlit reading a pair of dollar signs as LaTeX.

    Markdown rendered by Streamlit treats ``$...$`` as maths, and a glossary about
    auction values is full of dollar amounts -- unescaped, the middle of this table
    renders as italic equations.

    Public because the page needs it too: a ``help=`` tooltip is markdown and is
    built from the same prose as the glossary. The ``format=`` specs are printf and
    must **not** go through this.

    Args:
        text: Markdown source.

    Returns:
        str: The same text with ``$`` escaped.
    """
    return text.replace("$", r"\$")


def glossary_markdown(columns: Optional[Sequence[tuple]] = None,
                      lens: str = VALUE_LENS_ADP) -> str:
    """The glossary as one markdown table per spanner, for the columns on screen.

    Markdown rather than a dataframe on purpose: these are sentences, and
    ``st.dataframe`` truncates a long cell to an ellipsis, which makes the reader
    widen a column to find out what a number means.

    Split by group with a subheading each, so the glossary reads in the same order
    and the same shape as the table it sits under. Twenty-eight rows in one
    undifferentiated list is a worse reference than seven short ones, and the group
    is half the answer anyway -- ``ESPN`` means a different number under ``Points``
    than under ``Ranks``.

    *Source* is where the number originates, not where it is stored: everything here
    is read out of one parquet file, which is not the useful answer.

    **The lens has to be passed, not inferred from the columns.** The two Draft Metric
    variants share their three headers, so selecting on ``(group, label)`` alone
    matched both and an auction league's glossary described its ``Δ`` twice -- once as
    a dollar difference and once as an ADP rank difference, with no way to tell which
    the column above was.

    Args:
        columns: ``(group, label)`` pairs to include -- pass a rendered frame's
            ``.columns`` directly. None includes every column this lens renders.
        lens: Which of the two ``Draft Metric`` variants is on screen.

    Returns:
        str: Markdown: a bolded group heading and a four-column table per group,
        in :data:`COLUMNS` order. Groups with no column on screen are omitted
        entirely rather than left as an empty heading.
    """
    wanted = None if columns is None else {tuple(column) for column in columns}

    lensed, seen = [], set()
    for column in COLUMNS:
        key = (column.group, column.label)
        if column.lens in ("", lens) and key not in seen:
            seen.add(key)
            lensed.append(column)

    blocks = []
    for group in GROUPS:
        rows = [column for column in lensed
                if column.group == group
                and (wanted is None or (column.group, column.label) in wanted)]
        if not rows:
            continue
        lines = [f"**{escape_dollars(group)}**", "",
                 "| Column | Source | How It Is Calculated | What It Does Not Say |",
                 "|---|---|---|---|"]
        lines += [
            f"| **{escape_dollars(column.label)}** "
            f"| {escape_dollars(column.source_of)} "
            f"| {escape_dollars(column.how)} "
            f"| {escape_dollars(column.caveat) if column.caveat else '—'} |"
            for column in rows
        ]
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def available_only(board: pl.DataFrame) -> pl.DataFrame:
    """Players nobody has drafted yet.

    ``on_team_id`` is 0 for a free agent and the fantasy team's id once someone
    holds them, so it works pre-draft (where everyone is free) and mid-draft
    alike -- **except in a keeper league before keepers are declared**, where the
    column says who was on the roster last season and nothing about who is
    available. :func:`keepers_pending` is how the page tells those apart; this
    function is not the place, because "who does the store say holds this player"
    is a different question from "should I believe it".

    Args:
        board: A stored draft board.

    Returns:
        pl.DataFrame: The undrafted rows, or ``board`` unchanged when the column
        is absent.
    """
    if "on_team_id" not in board.columns:
        return board
    return board.filter(pl.col("on_team_id").fill_null(0) == 0)


def keeper_count(meta: Mapping) -> Optional[int]:
    """How many players this league lets a manager keep.

    Args:
        meta: The store's ``meta.json``.

    Returns:
        int | None: The count, 0 for a redraft league, or None when the store
        predates ``draft_settings`` being recorded. None is not 0 -- see
        :func:`keepers_pending`, where the two lead to opposite answers.
    """
    settings = meta.get("draft_settings") or {}
    value = settings.get("keeper_count")
    return None if value is None else int(value)


def league_auction_budget(meta: Mapping,
                          default: Optional[int] = None) -> int:
    """What one team has to spend in this league's auction.

    Read from ESPN rather than assumed, because it genuinely varies: GOP
    Degenerates plays for $250 and the other eight leagues for $200, while the
    market values on every board are denominated in ESPN's own $200 default
    regardless. Defaulting all nine to one number silently mispriced eight of
    them by 25%.

    Args:
        meta: The store's ``meta.json``.
        default: What to use when the store does not record one. Defaults to
            :data:`DEFAULT_AUCTION_BUDGET`.

    Returns:
        int: The budget.
    """
    fallback = DEFAULT_AUCTION_BUDGET if default is None else default
    settings = meta.get("draft_settings") or {}
    budget = settings.get("auction_budget")
    try:
        return int(budget) if budget else fallback
    except (TypeError, ValueError):
        return fallback


def rostered_counts(board: pl.DataFrame) -> Dict[int, int]:
    """How many players each fantasy team currently holds on this board.

    Args:
        board: A stored draft board.

    Returns:
        dict: ``{on_team_id: players held}``, free agents excluded. Empty when
        the board carries no ``on_team_id``.
    """
    if "on_team_id" not in board.columns:
        return {}
    held = (board.filter(pl.col("on_team_id").fill_null(0) != 0)
            .group_by("on_team_id").agg(pl.len().alias("n")))
    return {int(row["on_team_id"]): int(row["n"])
            for row in held.iter_rows(named=True)}


#: Slots you cannot draft into. ``BE`` is drafted (a bench player costs at least a
#: dollar); ``IR`` is not, so counting it would invent money the room never spends.
UNDRAFTABLE_SLOTS = {"IR", "", " "}

#: The floor on any winning bid in an ESPN auction.
MIN_BID = 1


def draft_type(meta: Mapping) -> str:
    """``"AUCTION"`` or ``"SNAKE"``, from ESPN.

    Args:
        meta: The store's ``meta.json``.

    Returns:
        str: The draft type, or ``""`` when the store predates draft settings.
    """
    return str((meta.get("draft_settings") or {}).get("type") or "")


def is_auction(meta: Mapping) -> bool:
    """Whether this league drafts by auction.

    Args:
        meta: The store's ``meta.json``.

    Returns:
        bool: True for an auction league.
    """
    return draft_type(meta).upper() == "AUCTION"


def draftable_spots(meta: Mapping) -> int:
    """Roster spots the room will actually buy: teams times draftable slots.

    Args:
        meta: The store's ``meta.json``.

    Returns:
        int: Total spots, 0 when the store records no roster shape.
    """
    slots = meta.get("roster_slots") or {}
    per_team = sum(int(n) for slot, n in slots.items()
                   if slot not in UNDRAFTABLE_SLOTS)
    return per_team * int(meta.get("team_count") or 0)


def _cash_eligible(board: pl.DataFrame) -> pl.Expr:
    """Who can absorb auction money, as a boolean expression.

    Four conditions, each of which cost something to learn: worth more than
    replacement, held rather than streamed, startable in this league at all, and
    actually projected by somebody. Shared by :func:`with_cash_value` and
    :func:`at_budget` so our dollars and the market's are normalised over the
    **same** population -- which is the only thing that makes ``cash_delta`` a
    difference rather than a coincidence.

    Every clause is guarded on the column existing, because a board built before a
    given attacher landed is still a board.

    Args:
        board: A stored draft board.

    Returns:
        pl.Expr: A boolean expression over ``board``.
    """
    eligible = pl.lit(True)
    if "vor" in board.columns:
        eligible = pl.col("vor").is_not_null() & (pl.col("vor") > 0)
    if "is_streamed" in board.columns:
        eligible = eligible & ~pl.col("is_streamed").fill_null(False)
    if "startable" in board.columns:
        eligible = eligible & pl.col("startable").fill_null(True)
    if "projection_missing" in board.columns:
        eligible = eligible & ~pl.col("projection_missing").fill_null(False)
    return eligible


def allocate_dollars(frame: pl.DataFrame, weight: pl.Expr, spots: int, teams: int,
                     budget: float, out: str, pool: Optional[pl.Expr] = None,
                     min_bid: int = MIN_BID,
                     price_outside_pool: bool = False) -> Optional[pl.DataFrame]:
    """Split a league's auction money across players in proportion to a weight.

    The standard auction conversion, extracted so both sides of the cash lens use
    it. Every spot the room fills costs at least ``min_bid``, so that money is
    committed before anyone bids on anyone; what remains is discretionary and is
    split in proportion to ``weight``::

        discretionary = teams x budget  -  spots x min_bid
        price         = min_bid + discretionary x wi / sum(w over the pool)

    Only as many players as there are spots to fill share the pool -- money the
    room does not have cannot be allocated -- so the sum over the pool lands on
    ``teams x budget``, less ``min_bid`` for any spot the pool was too small to
    fill. That shortfall is correct rather than a rounding error: an unfilled spot
    still costs a dollar.

    **``weight``, ``pool`` and ``price_outside_pool`` answer three different
    questions**: what each player is worth, who sets the rate, and who gets a number
    printed. The valuation side wants all three aligned -- only players the room can
    actually afford to roster get a price, because money the room does not have cannot
    be allocated, and pricing the 121st-best player in a 120-spot league would invent
    some. The market side wants them apart: ESPN prices ~313 players in a 240-spot
    league and what the room pays for a team defence is worth showing, even though a
    streamed position has no business setting the rate for a season-total valuation.

    Args:
        frame: The board.
        weight: Positive numeric expression for anyone who should receive a price,
            null for everyone else.
        spots: Roster spots the room will buy -- :func:`draftable_spots`.
        teams: Teams in the league.
        budget: Per-team budget.
        out: Name for the new dollars column.
        pool: Boolean expression for who counts toward the rate. Defaults to
            everyone ``weight`` prices.
        min_bid: The floor on a winning bid.
        price_outside_pool: Print a price for weighted players the pool's top ``spots``
            excludes, extrapolating the same rate. False -- the valuation default --
            leaves them null.

    Returns:
        pl.DataFrame: ``frame`` plus ``out``, or **None** when it cannot be computed
        -- no spots, no teams, or no positive weight anywhere. Returning None rather
        than an unchanged frame lets each caller degrade in its own way, which they
        do differently.
    """
    if not spots or not teams:
        return None

    discretionary = max(teams * float(budget) - spots * min_bid, 0.0)

    ranked = frame.with_columns(weight.alias("_alloc_w"))
    counts = pl.col("_alloc_w") if pool is None else (
        pl.when(pool).then(pl.col("_alloc_w")).otherwise(None))
    ranked = ranked.with_columns(counts.alias("_alloc_pool"))
    # Ranked within the population that sets the rate, so "as many as there are
    # spots" counts the players who could actually absorb the money rather than the
    # whole pool.
    ranked = ranked.with_columns(
        pl.col("_alloc_pool").rank("min", descending=True).alias("_alloc_rank"))
    inside = pl.col("_alloc_rank").is_not_null() & (pl.col("_alloc_rank") <= spots)

    total = (ranked.filter(inside)["_alloc_pool"].sum() or 0.0)
    scratch = ["_alloc_w", "_alloc_pool", "_alloc_rank"]
    if total <= 0:
        return None

    payable = pl.col("_alloc_w").is_not_null() if price_outside_pool else inside
    priced = (pl.when(payable)
              .then(min_bid + discretionary * pl.col("_alloc_w") / total)
              .otherwise(None))
    return ranked.with_columns(priced.alias(out)).drop(scratch)


def with_cash_value(board: pl.DataFrame, meta: Mapping, budget: float,
                    min_bid: int = MIN_BID) -> pl.DataFrame:
    """Price our own valuation in dollars, to set against what the room pays.

    **Why this exists.** ``value`` compares our VOR *rank* to the market's ADP
    *rank*, which is the right comparison in a snake draft, where a pick is a
    position in a queue. In an auction there is no queue — there is a price, and
    the only question is whether a player costs less than he is worth. Ranks
    cannot answer that: being four places underrated tells you nothing about
    whether to bid $41 or $46.

    **The conversion is the standard auction one.** Every spot the room fills
    costs at least ``min_bid``, so that money is committed before anyone bids on
    anyone; what remains is discretionary and is split across players in
    proportion to the points they return above replacement. A player worth no
    more than replacement is worth exactly the minimum, which is the correct
    answer rather than a rounding of one.

        discretionary = teams x budget  -  spots x min_bid
        our $         = min_bid + discretionary x (this VOR / all positive VOR)

    Only players above replacement share the discretionary pool, and only as many
    of them as there are spots to fill — money the room does not have cannot be
    allocated. K and D/ST are excluded for the reason they are excluded from
    ``value``: a season-total VOR does not describe a position you stream, and
    including them once had eight team defences priced as the league's best buys.

    **It stays denominated in your budget, and that is the whole point of the column.**
    The number is what to spend out of the money you actually have -- read it off and
    bid it. Scaling it onto some other price level was tried and reverted: it made the
    difference beside it tidier and destroyed the only property that makes the column
    actionable. Where the two sides need to be comparable, the fix belongs on the side
    that is wrong, which is :func:`at_budget` -- see the team-count note there.

    ``cash_delta`` is our price minus the market's, both in this league's dollars.
    Positive means the room is underpaying.

    It runs positive for most players inside the money, and that is our valuation
    talking rather than an artefact: we allocate the whole budget across the ~106
    players worth rostering while the market spreads it over the ~313 it prices, so
    against anyone in the money we will usually be higher. Read the column as an
    ordering -- who the room is *most* wrong about -- not as a verdict on each row.

    Args:
        board: A stored draft board, already through :func:`at_budget` so the
            market side is in league dollars rather than ESPN's $200 default.
        meta: The store's ``meta.json``, for team count and roster shape.
        budget: Per-team budget.
        min_bid: The floor on a winning bid.

    Returns:
        pl.DataFrame: The board with ``our_dollars`` and ``cash_delta`` added.
        Unchanged when it has no ``vor``, or when the store records no roster
        shape to derive the pool from -- :func:`display_frame` then drops both
        columns, as it does for anything else the artifact cannot support.
    """
    spots = draftable_spots(meta)
    teams = int(meta.get("team_count") or 0)
    if "vor" not in board.columns:
        return board

    weight = pl.when(_cash_eligible(board)).then(pl.col("vor")).otherwise(None)
    out = allocate_dollars(board, weight, spots, teams, budget, "our_dollars",
                           min_bid=min_bid)
    if out is None:
        return board

    if "auction_dollars" in out.columns:
        out = out.with_columns(
            (pl.col("our_dollars") - pl.col("auction_dollars")).alias("cash_delta"))
    return out


def with_keeper_price(board: pl.DataFrame, keepers: Optional[int],
                      min_bid: int = MIN_BID) -> pl.DataFrame:
    """Surface what it costs each holder to keep a player, and whether that is a bargain.

    **`keeper_value` is what the current holder paid to acquire the player**, and
    that was established by measurement rather than read off a field name. Of GOP
    Degenerates' 187 priced keepers, 130 carry *exactly* their 2025 auction bid --
    CeeDee Lamb $90, Gibbs $87, Chase $84, to the dollar. Of the rest, 29 were
    never drafted in 2025 and price at $1-$5, and 28 differ because the player
    changed hands: Jayden Daniels went for $46 in the auction and keeps for $1 for
    the manager who later claimed him. The holder's cost, not the draft's.

    **A rostered player with no cost keeps for the minimum bid, not for nothing.**
    ESPN reports ``keeperValue: 0`` for a player picked up off waivers or free
    agency -- there was no winning bid to record -- and its own UI shows those at
    $1. 65 of GOP's 252 held players are in that state, and only 18 of the 65
    appear in the 2025 draft at all, so they are overwhelmingly in-season
    acquisitions exactly as the zero implies. Reading the zero as "no keeper price"
    blanked a quarter of the league's keepers, Malik Nabers among them, and a blank
    there says "nobody can keep him", which is the opposite of true.

    So **being on a roster is what confers a keeper price**, and the dollar figure
    is the acquisition cost floored at ``min_bid``. Only a genuine free agent has
    none. That the two signals agree is checked rather than assumed: no free agent
    on any of the nine boards carries a non-zero ``keeper_value``.

    ``keeper_surplus`` is **our** valuation minus that cost -- positive means the
    player is worth more to us than keeping him costs, which is the only question a
    keeper price is actually asked.

    It was ESPN's market price minus the cost until 2026-08-17, and ours is the better
    comparison for the decision it informs. The market price says whether the *room*
    would pay more than the keeper price, which is a fact about other people's money;
    what decides whether to keep a player is whether *we* think he is worth it. The two
    disagree exactly where it matters -- on the players we rate differently from the
    market, which is the entire subject of this table.

    Both fall back honestly. ``our_dollars`` is null outside the money and for the
    streamed positions, so the surplus is blank there rather than claiming a player is
    a bargain we have no valuation for; the previous version could always produce a
    number because ESPN prices everybody. Where :func:`with_cash_value` has not run at
    all -- a board with no ``vor``, or a store with no roster shape -- there is no
    surplus column and :func:`display_frame` drops it.

    It needs :func:`at_budget` and :func:`with_cash_value` to have run, because both
    sides have to be in the same currency: the keeper price is in this league's real
    dollars, and ``our_dollars`` is denominated in the budget set on the page.

    **Only in a keeper league.** Every board carries ``keeper_value``, including
    the eight redraft leagues, where it is a small round number ESPN publishes for
    nobody's benefit -- 1 to 14, on free agents, in leagues whose keeper count is
    zero. Showing it there would invent a rule the league does not play by.

    Args:
        board: A stored draft board, ideally already through :func:`at_budget`.
        keepers: :func:`keeper_count`. 0 or None leaves the board untouched.
        min_bid: Floor on the price, for a player acquired without a bid.

    Returns:
        pl.DataFrame: The board with ``keeper_price`` and -- where the market has
        priced the player -- ``keeper_surplus``. Unchanged in a redraft league, so
        :func:`display_frame` drops both columns there.
    """
    if not keepers or "keeper_value" not in board.columns:
        return board

    cost = pl.col("keeper_value").fill_null(0).cast(pl.Float64)
    # Who can be kept: anyone on a roster. Falls back to the price itself where the
    # board predates `on_team_id`, which is the best available reading -- it loses
    # the waiver pickups rather than inventing keepers.
    if "on_team_id" in board.columns:
        rostered = pl.col("on_team_id").fill_null(0) != 0
    else:
        rostered = cost > 0

    price = (pl.when(rostered)
             .then(pl.max_horizontal(cost, pl.lit(float(min_bid))))
             .otherwise(None))
    out = board.with_columns(price.alias("keeper_price"))
    if "our_dollars" in out.columns:
        out = out.with_columns(
            (pl.col("our_dollars") - pl.col("keeper_price")).alias("keeper_surplus"))
    return out


def keepers_pending(board: pl.DataFrame, keepers: Optional[int]) -> bool:
    """Whether this league's rosters are last season's rather than this year's keepers.

    **The problem.** ESPN carries a keeper league's rosters into the new season
    before anyone has declared a keeper. GOP Degenerates' 2026 board arrives with
    252 players held across 16 teams -- 15 to 17 each -- against a
    ``keeper_count`` of **2**. None of those 252 has been kept; they were on that
    roster in 2025. Filtering them out as "unavailable" hides a tenth of the pool
    and every one of the league's best players, which is the opposite of what a
    pre-draft board is for.

    **The test.** A roster holding more players than the league allows keepers
    cannot be a list of keepers. That is a fact about arithmetic rather than a
    guess about ESPN's behaviour, and it resolves itself: the day rosters shrink
    to the keeper limit, this returns False and the board starts filtering again
    with no flag to remember to flip.

    It fails in the safe direction. A false positive shows a few players who are
    genuinely gone; a false negative hides players who are genuinely available,
    on the morning you are drafting them.

    Args:
        board: A stored draft board.
        keepers: :func:`keeper_count`. None -- a store built before draft settings
            were recorded -- returns False, preserving the behaviour that store was
            written under rather than silently changing what an old board means.

    Returns:
        bool: True when ``on_team_id`` should not be read as "unavailable".
    """
    if not keepers:
        return False
    counts = rostered_counts(board)
    return any(held > keepers for held in counts.values())


def roster_needs(starting_slots: Dict[str, int],
                 roster_positions: Sequence[str]) -> Dict[str, int]:
    """Starting slots still unfilled, after assigning the players already held.

    Dedicated slots are filled before flex ones. That is a convention rather than
    an optimisation -- an optimal assignment would need the projections -- but it
    is the one a drafter reasons with: a third running back fills the flex, it does
    not displace a starter.

    Args:
        starting_slots: ``meta.json``'s ``starting_slots``, e.g.
            ``{"QB": 1, "RB": 2, "RB/WR/TE": 1}``.
        roster_positions: ``primaryPosition`` of every player already rostered.

    Returns:
        dict: Slot name to openings remaining, omitting filled slots.
    """
    remaining = {slot: count for slot, count in starting_slots.items()
                 if slot not in NON_STARTING_SLOTS and count > 0}

    for position in roster_positions:
        if remaining.get(position, 0) > 0:
            remaining[position] -= 1
            continue
        for slot, eligible in FLEX_SLOTS.items():
            if position in eligible and remaining.get(slot, 0) > 0:
                remaining[slot] -= 1
                break

    return {slot: count for slot, count in remaining.items() if count > 0}


def positions_needed(starting_slots: Dict[str, int],
                     roster_positions: Sequence[str]) -> List[str]:
    """Positions that would fill one of the still-open starting slots.

    Args:
        starting_slots: ``meta.json``'s ``starting_slots``.
        roster_positions: ``primaryPosition`` of every player already rostered.

    Returns:
        list: Sorted positions. Empty when every starting slot is filled.
    """
    needs = roster_needs(starting_slots, roster_positions)
    positions = set()
    for slot in needs:
        positions.update(FLEX_SLOTS.get(slot, [slot]))
    return sorted(positions)


def board_positions(board: pl.DataFrame) -> List[str]:
    """Positions present in this league's pool, most-drafted first.

    Ordered by :data:`POSITION_HUES` so the familiar ones lead and the IDP
    positions follow, rather than alphabetically with cornerbacks first.

    Args:
        board: A stored draft board.

    Returns:
        list: Position names.
    """
    present = [p for p in board["primaryPosition"].drop_nulls().unique().to_list()]
    return sorted(present, key=lambda p: (POSITION_HUES.get(p, 99), p))


def board_teams(board: pl.DataFrame) -> List[str]:
    """NFL teams present in this league's pool, alphabetically.

    Args:
        board: A stored draft board.

    Returns:
        list: Team abbreviations. Empty when the board carries no ``pro_team``.
    """
    if "pro_team" not in board.columns:
        return []
    return sorted(board["pro_team"].drop_nulls().unique().to_list())


def board_byes(board: pl.DataFrame) -> List[int]:
    """Bye weeks present in this league's pool, in week order.

    Args:
        board: A stored draft board.

    Returns:
        list: Week numbers as ints. Empty when the board carries no ``bye_week``,
        which is what a board built before byes were attached looks like.
    """
    if "bye_week" not in board.columns:
        return []
    weeks = board["bye_week"].drop_nulls().unique().to_list()
    return sorted(int(week) for week in weeks)


def filter_board(
    board: pl.DataFrame,
    positions: Optional[Sequence[str]] = None,
    *,
    only_available: bool = True,
    hide_unstartable: bool = True,
    hide_unprojected: bool = True,
    max_tier: Optional[int] = None,
    search: str = "",
    teams: Optional[Sequence[str]] = None,
    byes: Optional[Sequence[int]] = None,
) -> pl.DataFrame:
    """Apply the page's filters.

    Every filter is an **include** list: an empty selection keeps everything, and a
    non-empty one keeps only what it names. That is the one rule that makes four
    controls composable without a legend explaining each -- and it is why ``byes``
    drops players whose bye is unknown, which "keep only weeks 5 and 10" has to
    mean if it is to mean anything.

    Args:
        board: A stored draft board.
        positions: Positions to keep. None or empty keeps all.
        only_available: Drop players somebody already holds.
        hide_unstartable: Drop positions this league has no starting slot for --
            the 32 team defences on the board of the league with no D/ST slot.
        hide_unprojected: Drop players no source has a line for. Half the pool,
            and their projection is a literal 0.0 rather than a null, so without
            this they rank as the worst players in the league rather than as
            unknowns.
        max_tier: Keep tiers at or above this one. None keeps all.
        search: Case-insensitive substring of the player name.
        teams: NFL teams to keep. None or empty keeps all.
        byes: Bye weeks to keep. None or empty keeps all; a non-empty selection
            also drops players with no recorded bye.

    Returns:
        pl.DataFrame: The filtered board, still in its stored ``vor`` order.
    """
    out = board
    if only_available:
        out = available_only(out)
    if hide_unstartable and "startable" in out.columns:
        out = out.filter(pl.col("startable").fill_null(True))
    if hide_unprojected and "projection_missing" in out.columns:
        out = out.filter(~pl.col("projection_missing").fill_null(False))
    if positions:
        out = out.filter(pl.col("primaryPosition").is_in(list(positions)))
    if teams and "pro_team" in out.columns:
        out = out.filter(pl.col("pro_team").is_in(list(teams)))
    if byes and "bye_week" in out.columns:
        out = out.filter(pl.col("bye_week").is_in([int(week) for week in byes]))
    if max_tier is not None and "tier" in out.columns:
        out = out.filter(pl.col("tier").is_null() | (pl.col("tier") <= max_tier))
    if search:
        # Matched literally, not as a regex. The names on a board are full of
        # characters a regex reads as syntax -- "T.J. Hockenson" matched any three
        # characters between the dots, "Amon-Ra St. Brown" the same, and a name
        # typed with an unclosed bracket raised out of the page instead of finding
        # nothing. Case folded on both sides rather than with an inline `(?i)`,
        # which a literal match does not honour.
        out = out.filter(pl.col("player_name").str.to_lowercase()
                         .str.contains(search.strip().lower(), literal=True))
    return out


def scarcity_curve(board: pl.DataFrame, positions: Sequence[str],
                   depth: float = SCARCITY_DEPTH) -> pl.DataFrame:
    """Projected points against positional rank -- the positional cliff.

    Runs to :data:`SCARCITY_DEPTH` times each position's replacement rank, so the
    visible range is the part of each curve a drafter can act on. Where a position
    has no replacement rank (this league does not start it) the whole curve is
    kept.

    Args:
        board: A stored draft board.
        positions: Positions to include.
        depth: Multiple of replacement rank to run to.

    Returns:
        pl.DataFrame: ``primaryPosition``, ``pos_rank``, ``TRUE_Points``,
        ``replacement_rank``, one row per player, projected players only.
    """
    curve = board.filter(
        pl.col("primaryPosition").is_in(list(positions))
        & pl.col("pos_rank").is_not_null()
        & pl.col("TRUE_Points").is_not_null()
    )
    if "projection_missing" in curve.columns:
        curve = curve.filter(~pl.col("projection_missing").fill_null(False))

    if "replacement_rank" in curve.columns:
        curve = curve.filter(
            pl.col("replacement_rank").is_null()
            | (pl.col("pos_rank") <= pl.col("replacement_rank") * depth)
        )

    keep = [c for c in ("primaryPosition", "player_name", "pos_rank",
                        "TRUE_Points", "replacement_rank", "tier")
            if c in curve.columns]
    return curve.select(keep).sort(["primaryPosition", "pos_rank"])


def tier_runway(board: pl.DataFrame, positions: Sequence[str],
                only_available: bool = True) -> pl.DataFrame:
    """How many players are left in each tier, per position.

    The question a board is actually read to answer: not "is this player one spot
    better than that one" but "how many of these are left before the drop". Counts
    available players only, so it empties as the draft runs.

    Args:
        board: A stored draft board.
        positions: Positions to include.
        only_available: Count only players nobody holds. Pass False in a keeper
            league before keepers are declared -- "three left in tier 1" is a lie
            if it excluded eleven players last season's rosters happen to list.

    Returns:
        pl.DataFrame: ``primaryPosition``, ``tier``, ``remaining``, ``best_points``,
        sorted by position then tier.
    """
    pool = filter_board(board, positions, only_available=only_available,
                        hide_unstartable=True, hide_unprojected=True)
    if pool.is_empty() or "tier" not in pool.columns:
        return pl.DataFrame(schema={"primaryPosition": pl.String, "tier": pl.Float64,
                                    "remaining": pl.UInt32, "best_points": pl.Float64})

    return (
        pool.filter(pl.col("tier").is_not_null())
        .group_by(["primaryPosition", "tier"])
        .agg(pl.len().alias("remaining"),
             pl.col("TRUE_Points").max().alias("best_points"))
        .sort(["primaryPosition", "tier"])
    )


#: A full slate, and the basis every projection on the board is quoted over.
#:
#: Pinned equal to ``Scripts.usage.season.DEFAULT_TARGET_SLATE`` by a test rather than
#: imported, for the reason :data:`OUTCOME_EVIDENCE` is duplicated: this module is read
#: by a process that only opens parquet, and importing the usage package to learn one
#: float would pull the model stack in behind it.
FULL_SLATE = 17.0

#: What ``avail_evidence`` says when the usage model has no availability estimate for a
#: player, and why it matters that it says something. Roughly one priced-and-projected
#: player in seven has no ``usg_expected_games`` -- 80 of 590 on the 2026 Knights board --
#: and those rows take a factor of 1.0. Silently, that reads as "durable"; named, it reads
#: as "not asked", which is the truth.
AVAIL_NO_ESTIMATE = "no availability estimate"


def _drafted_expr(board: pl.DataFrame, drafted: Sequence) -> pl.Expr:
    """Whether each row is one of the players already off the board.

    Keyed on ``player_id`` where the board has one, because names are not unique:
    ``Scripts.draft.board``'s postscript found **16 colliding names** in the IDP pool,
    Lamar Jackson the quarterback alongside Lamar Jackson the cornerback. Crossing off
    one and having the other vanish is a small bug with a very bad half-hour attached to
    it on draft night.

    Both sides are cast to string so a session-state value typed by a widget compares
    equal to an ``i64`` id read out of parquet.

    Args:
        board: A stored draft board.
        drafted: Player ids -- or names, on a board with no id column.

    Returns:
        pl.Expr: A boolean expression, constant False when ``drafted`` is empty.
    """
    if not len(drafted):
        return pl.lit(False)
    key = "player_id" if "player_id" in board.columns else "player_name"
    return pl.col(key).cast(pl.String).is_in([str(v) for v in drafted])


def positional_scarcity(board: pl.DataFrame, drafted: Sequence = (),
                        value_column: str = "vor") -> pl.DataFrame:
    """How much of each position's value is still sitting *below* each player.

    The DraftSheet's best single column, and the one thing this repo's board had no
    equivalent of. ``tier_runway`` counts players and ``scarcity_curve`` draws the
    cliff; neither answers "if I pass on him, what is actually left", which is the
    question a drafter asks about every pick.

    Per position: the denominator is the total value over replacement of everyone worth
    more than replacement, **fixed at build time**; the numerator is the same sum over
    the players strictly below this one who are still available. So it starts near 1.0
    at the top of a position and decays toward 0 as the draft empties it -- read a high
    number as "no urgency, plenty behind him" and a low one as "the cliff is here".

    **The fixed denominator is what makes it decay**, and it is the sheet's choice
    rather than an oversight. Normalising by the value *remaining* would make the column
    scale-free and it would never fall, which reads better and says less: the whole
    signal on draft night is that a position is being drained.

    Streamed positions come back null. A season-total value over replacement does not
    describe a position you stream -- the reason ``value`` is NaN for them in
    ``Scripts.draft.board`` -- so a scarcity share built on one would be null's more
    honest cousin.

    Computed on ``vor`` even when the page is sorted on availability-adjusted points.
    The two orders differ for a handful of players and reconciling them would mean
    recomputing replacement level, which is a board build rather than a page render.

    Args:
        board: A stored draft board.
        drafted: Players already off the board -- see :func:`_drafted_expr`.
        value_column: The value-over-replacement column.

    Returns:
        pl.DataFrame: ``board`` plus ``ps``, a 0-1 share. Null where the position is
        streamed or has no positive value at all. Row order is preserved.
    """
    if value_column not in board.columns or "primaryPosition" not in board.columns:
        return board.with_columns(pl.lit(None, dtype=pl.Float64).alias("ps"))

    counts = pl.col(value_column).is_not_null() & (pl.col(value_column) > 0)
    if "is_streamed" in board.columns:
        counts = counts & ~pl.col("is_streamed").fill_null(False)
    open_now = counts & ~_drafted_expr(board, drafted)

    scratch = ["_ps_order", "_ps_all", "_ps_open", "_ps_denom", "_ps_left", "_ps_cum"]
    return (
        board
        .with_row_index("_ps_order")
        .with_columns(
            pl.when(counts).then(pl.col(value_column)).otherwise(0.0).alias("_ps_all"),
            pl.when(open_now).then(pl.col(value_column)).otherwise(0.0).alias("_ps_open"),
        )
        # `cum_sum` reads the frame's row order, so the sort is load-bearing rather than
        # cosmetic: it is what makes "below" mean "worth less than him".
        .sort(["primaryPosition", value_column], descending=[False, True],
              nulls_last=True)
        .with_columns(
            pl.col("_ps_all").sum().over("primaryPosition").alias("_ps_denom"),
            pl.col("_ps_open").sum().over("primaryPosition").alias("_ps_left"),
            pl.col("_ps_open").cum_sum().over("primaryPosition").alias("_ps_cum"),
        )
        .with_columns(
            pl.when(pl.col("_ps_denom") > 0)
            .then((pl.col("_ps_left") - pl.col("_ps_cum")) / pl.col("_ps_denom"))
            .otherwise(None).alias("ps"))
        .sort("_ps_order")
        .drop(scratch)
    )


def with_availability_points(board: pl.DataFrame,
                             points_column: str = "TRUE_Points",
                             slate: float = FULL_SLATE) -> pl.DataFrame:
    """Discount every projection by the games the model expects the player to miss.

    The DraftSheet applies ``(16 - missed) / 17`` to everything it prints, off a table of
    **positional-rank priors** -- QB1 loses 1.80 games because he is QB1. We can do the
    same thing off a per-player estimate, which is what ``usg_expected_games`` is.

    **Neither ``TRUE_Points`` nor ``USG_Points`` is availability-adjusted**, so this does
    not double-count. ``Scripts.usage.project.to_full_slate`` divides each player's
    expected games back out of the usage line specifically so the availability term can
    be "applied deliberately and to the *whole* blend rather than to one quarter of it" --
    its own words. This is that application. (``docs/DRAFT_READINESS.md`` claimed the
    opposite until 2026-08-28; the claim was wrong and is corrected there.)

    **It is a parallel column and the board does not sort on it by default**, because the
    availability head is the weakest arm of the model that produces it: plan 18 measures
    prior-season games against next season at r = +0.343. On the 2026 Knights board the
    discount is real money -- Puka Nacua 339.4 to 274.8, Jahmyr Gibbs 342.9 to 296.8 --
    and it reorders the top of the board. That is worth *looking* at and worth being able
    to switch to; it is not worth silently repricing four leagues on ten days before a
    draft.

    A player with no estimate takes a factor of 1.0 and is marked in ``avail_evidence``.
    Sinking the un-estimated seventh of the pool to the bottom of a sort would be a
    filter disguised as a projection.

    Args:
        board: A stored draft board.
        points_column: The projection to discount.
        slate: Games in a full season, the basis the projection is quoted over.

    Returns:
        pl.DataFrame: ``board`` plus ``avail_points``, ``avail_pos_rank`` and
        ``avail_evidence``. Unchanged when either input column is absent -- the toggle
        then drops out of the page as any other unsupported column does.
    """
    if points_column not in board.columns or "usg_expected_games" not in board.columns:
        return board

    games = pl.col("usg_expected_games")
    factor = (games.clip(0.0, slate) / slate).fill_null(1.0)
    out = board.with_columns(
        (pl.col(points_column) * factor).alias("avail_points"),
        pl.when(games.is_null()).then(pl.lit(AVAIL_NO_ESTIMATE))
        .otherwise(pl.lit("")).alias("avail_evidence"),
    )
    return out.with_columns(
        pl.col("avail_points").rank("min", descending=True)
        .over("primaryPosition").alias("avail_pos_rank"))


# =========================================================================
# Calibration -- our projection against ESPN's
# =========================================================================

#: A position whose two projections never disagree has no outliers, and scoring one
#: divides by that non-disagreement. Kickers are the real case rather than a
#: hypothetical: no source but ESPN projects a kicker, so ``TRUE_Points`` for a K
#: *is* ``ESPN_Points``, every delta is float dust around 1e-14, and the standard
#: deviation is 7e-15. Dividing by it ranked two kickers as the board's two biggest
#: disagreements on a delta of -0.00000000000003. Below this floor a position is
#: reported as having no outliers, which is the true answer.
AGREEMENT_MIN_SD = 0.5

#: Fewest players a position needs before its own mean and spread mean anything. A
#: position left with four rows by a filter has a standard deviation that describes
#: those four players rather than the position.
AGREEMENT_MIN_PLAYERS = 8

#: The two ways to read the same two columns. Points plots them against each other
#: and asks where we sit off the diagonal; Disagreement plots the gap itself against
#: how big the player is, which is the only one of the two that separates anything
#: once the correlation is 0.98.
AGREEMENT_VIEW_POINTS = "Projected Points"
AGREEMENT_VIEW_DELTA = "Disagreement"

#: Schema for an agreement frame with nothing in it, so a caller can read
#: ``.columns`` off an empty result rather than special-casing it.
AGREEMENT_SCHEMA = {
    "primaryPosition": pl.String,
    "ESPN_Points": pl.Float64,
    "TRUE_Points": pl.Float64,
    "points_delta": pl.Float64,
    "agreement_mean": pl.Float64,
    "agreement_z": pl.Float64,
}


def agreement_frame(board: pl.DataFrame) -> pl.DataFrame:
    """Our projection against ESPN's, each disagreement scored within its position.

    Keeps only players who have both projections. ESPN projects no stat line at all
    for part of the pool, and a missing projection is not a disagreement -- on the
    2026 Winfield board that is 173 of the 698 startable, projected players.

    **The score is a z within position, measured over the frame you pass in.** Both
    halves of that are deliberate:

    - *Within position*, because the positions disagree by different amounts and in
      different directions. The mean gap is **+27.3** points at QB against **+5.9**
      at RB, so a 30-point gap is unremarkable for a quarterback and a real outlier
      for a back. A single pooled scale reports the quarterbacks as the model's
      problem and buries every other one.
    - *Over the frame passed in*, rather than over the whole board, because the
      scoping is the analysis. Narrowed to the 200 players the market actually
      prices, the mean gap at WR moves from +8.8 to **-3.4**: the "we project more
      than ESPN" bias is almost entirely deep players nobody drafts. Scored against
      the full board instead, every priced receiver would read as a negative outlier
      for the sole reason that it belongs to the priced half.

    **This is not two independent opinions.** ESPN is one of the three equal thirds
    inside ``TRUE_Points``, so the gap is damped by construction and reads as *how
    far the blend moved off ESPN*, not as a forecaster disagreeing with another. A
    player we and ESPN are far apart on is one FantasyPros and the usage model
    dragged, which is what makes the tails worth reading.

    Args:
        board: A stored draft board, filtered however the page is filtered.

    Returns:
        pl.DataFrame: One row per player carrying both projections, plus
        ``points_delta`` (``Us - ESPN``), ``agreement_mean`` (the midpoint of the
        two, which is what the gap is read against) and ``agreement_z``. The z is
        null wherever the position has too few players or too little disagreement
        to support one -- see :data:`AGREEMENT_MIN_PLAYERS` and
        :data:`AGREEMENT_MIN_SD` -- rather than being faked at zero, because "we
        never disagree here" and "we agree about this player" are different facts.
    """
    required = ("primaryPosition", "ESPN_Points", "TRUE_Points")
    if not all(column in board.columns for column in required):
        return pl.DataFrame(schema=AGREEMENT_SCHEMA)

    carried = [column for column in
               ("player_name", "pro_team", "bye_week", "tier", "adp",
                "adp_is_priced", "vor", "pos_rank", "team_owner")
               if column in board.columns]
    pool = board.filter(
        pl.col("ESPN_Points").is_not_null() & pl.col("TRUE_Points").is_not_null()
    ).select([*required, *carried]).with_columns(
        # Recomputed from the two columns actually plotted rather than read from the
        # board's stored `points_delta`, which is the same subtraction. A chart that
        # draws three numbers should not be able to disagree with itself about
        # whether the third is the difference of the other two.
        (pl.col("TRUE_Points") - pl.col("ESPN_Points")).alias("points_delta"),
        ((pl.col("TRUE_Points") + pl.col("ESPN_Points")) / 2).alias("agreement_mean"),
    )
    if pool.is_empty():
        return pool.with_columns(pl.lit(None, pl.Float64).alias("agreement_z"))

    # `fill_null(0.0)`: a position holding one row has a null standard deviation,
    # which would make the whole condition null and fall through to the `otherwise`
    # by accident rather than by the rule stated above it.
    spread = pl.col("points_delta").std().over("primaryPosition").fill_null(0.0)
    scorable = ((pl.len().over("primaryPosition") >= AGREEMENT_MIN_PLAYERS)
                & (spread >= AGREEMENT_MIN_SD))
    return pool.with_columns(
        pl.when(scorable)
        .then((pl.col("points_delta")
               - pl.col("points_delta").mean().over("primaryPosition")) / spread)
        .otherwise(None)
        .alias("agreement_z")
    ).sort(["primaryPosition", "ESPN_Points"], descending=[False, True])


#: Columns :func:`with_outlier_flag` writes: whether a player is one of the biggest
#: disagreements, and where he places among them.
AGREEMENT_FLAG = "agreement_flagged"
AGREEMENT_RANK = "agreement_rank"


def with_outlier_flag(frame: pl.DataFrame, limit: int = 3,
                      per_position: bool = False) -> pl.DataFrame:
    """Mark the biggest disagreements in place rather than splitting them out.

    A separate frame of outliers is the obvious shape and the wrong one for the
    chart: Vega-Lite will not facet a layered spec whose layers carry different
    data, so highlighting a subset has to be a filter over one dataset rather than
    a second dataset drawn on top. Flagging in place is what lets the cloud, the
    highlighted marks and their labels all be layers over the same rows.

    The rank comes back with it because two outliers in one panel are routinely on
    top of each other -- the three flagged quarterbacks on the 2026 Winfield board
    are all deep backups within 15 points of each other -- and a caller that knows
    which of the three a mark is can stagger its label by a fixed offset instead of
    printing three names in the same place. Vega-Lite has no label-collision solver
    for point marks, so the separation has to be built rather than asked for.

    Args:
        frame: An :func:`agreement_frame` result.
        limit: How many to flag. Applied within each position when
            ``per_position``, otherwise across the whole frame.
        per_position: Rank inside each position rather than across all of them.
            The chart labels want this -- a pooled top ten lands eight labels in
            one facet and none in the rest.

    Returns:
        pl.DataFrame: The frame plus :data:`AGREEMENT_FLAG` (boolean) and
        :data:`AGREEMENT_RANK` (1 is the biggest disagreement, null where not
        flagged). Players with no score are never flagged.
    """
    if frame.is_empty() or limit <= 0:
        return frame.with_columns(
            pl.lit(False, pl.Boolean).alias(AGREEMENT_FLAG),
            pl.lit(None, pl.UInt32).alias(AGREEMENT_RANK))

    # `fill_null(-1.0)` rather than letting the rank see nulls: an unscored player
    # must sort last, and a null magnitude ranking anywhere else would flag a
    # kicker whose disagreement is not measurable as one of the biggest.
    placed = pl.col("agreement_z").abs().fill_null(-1.0).rank(
        "ordinal", descending=True)
    if per_position:
        placed = placed.over("primaryPosition")

    flagged = pl.col("agreement_z").is_not_null() & (placed <= limit)
    return frame.with_columns(
        flagged.alias(AGREEMENT_FLAG),
        pl.when(flagged).then(placed.cast(pl.UInt32)).otherwise(None)
        .alias(AGREEMENT_RANK))


#: Column :func:`with_label_slots` writes.
AGREEMENT_SLOT = "label_slot"


def with_label_slots(frame: pl.DataFrame, y_field: str) -> pl.DataFrame:
    """Order each position's flagged marks bottom-to-top, for staggering labels.

    Vega-Lite has no collision solver for point labels, so a caller draws one text
    layer per slot at a fixed vertical offset. **The slot has to be the mark's own
    vertical order, not its rank by disagreement**, and getting that backwards is
    worse than not staggering at all: the three flagged tight ends sit within 30
    screen pixels of each other, and offsetting them by |z| rank pushed the lowest
    mark's label up and the highest mark's label down, collapsing three names onto
    one line. Ordered by ``y_field`` instead, the marks' own spread adds to the
    offsets rather than cancelling them.

    Args:
        frame: A :func:`with_outlier_flag` result.
        y_field: The column the chart puts on its y axis. It differs by view --
            ``TRUE_Points`` against ESPN's, ``points_delta`` against zero -- which
            is why the slot cannot be decided when the flag is.

    Returns:
        pl.DataFrame: The frame plus :data:`AGREEMENT_SLOT`, 1 for the lowest
        flagged mark in each position, null for everything unflagged.
    """
    if frame.is_empty() or AGREEMENT_FLAG not in frame.columns:
        return frame.with_columns(pl.lit(None, pl.UInt32).alias(AGREEMENT_SLOT))

    # Grouped by position *and* the flag, so the ranking happens inside each
    # position's flagged marks. The unflagged rows rank among themselves and the
    # `when` throws that away.
    return frame.with_columns(
        pl.when(pl.col(AGREEMENT_FLAG))
        .then(pl.col(y_field).rank("ordinal")
              .over(["primaryPosition", AGREEMENT_FLAG]).cast(pl.UInt32))
        .otherwise(None)
        .alias(AGREEMENT_SLOT))


def agreement_outliers(frame: pl.DataFrame, limit: int = 12,
                       per_position: bool = False) -> pl.DataFrame:
    """The biggest disagreements, furthest from its own position's first.

    The list form of :func:`with_outlier_flag`, and built on it so the table and
    the marks on the chart cannot pick different players.

    Args:
        frame: An :func:`agreement_frame` result.
        limit: How many to return. Applied per position when ``per_position``,
            otherwise across the whole frame.
        per_position: Rank inside each position rather than across all of them.

    Returns:
        pl.DataFrame: The frame's own columns, ordered by ``|agreement_z|``
        descending. Empty when nothing carries a score, which is not the same as
        everything agreeing.
    """
    flagged = with_outlier_flag(frame, limit=limit, per_position=per_position)
    return (flagged.filter(pl.col(AGREEMENT_FLAG))
            .drop(AGREEMENT_FLAG, AGREEMENT_RANK)
            .with_columns(pl.col("agreement_z").abs().alias("_magnitude"))
            .sort("_magnitude", descending=True).drop("_magnitude"))


def agreement_summary(frame: pl.DataFrame) -> pl.DataFrame:
    """Per position: how far the blend sits off ESPN, and how consistently.

    The table the outlier hunt is read against. A position whose mean gap is large
    and whose spread is small is a *systematic* offset -- something the blend does
    to every player at that position, which is a model question. A small mean with
    a wide spread is the opposite: we agree on the position and disagree about
    individuals, which is a player question.

    Args:
        frame: An :func:`agreement_frame` result.

    Returns:
        pl.DataFrame: ``primaryPosition``, ``players``, ``mean_delta``,
        ``sd_delta``, ``share_above`` (the fraction we project higher on, 0-100)
        and ``scored`` -- whether this position's players carry a z at all.
    """
    schema = {"primaryPosition": pl.String, "players": pl.UInt32,
              "mean_delta": pl.Float64, "sd_delta": pl.Float64,
              "share_above": pl.Float64, "scored": pl.Boolean}
    if frame.is_empty():
        return pl.DataFrame(schema=schema)

    return (
        frame.group_by("primaryPosition")
        .agg(pl.len().alias("players"),
             pl.col("points_delta").mean().alias("mean_delta"),
             pl.col("points_delta").std().alias("sd_delta"),
             (pl.col("points_delta") > 0).mean().mul(100).alias("share_above"),
             pl.col("agreement_z").is_not_null().any().alias("scored"))
        .sort("players", descending=True)
    )


#: Source column to display label for the outlier table, in render order. Its
#: vocabulary is the board's -- `ESPN`, `Us` and a Δ mean here exactly what the
#: `Points` spanner means by them -- so a reader arriving from the Board tab does
#: not have to learn the columns twice.
AGREEMENT_COLUMNS = (
    ("player_name", "Player"),
    ("primaryPosition", "Pos"),
    ("pro_team", "NFL"),
    ("ESPN_Points", "ESPN"),
    ("TRUE_Points", "Us"),
    ("points_delta", "Δ"),
    ("agreement_z", "σ vs Position"),
    ("adp", "ADP"),
    ("tier", "Tier"),
)


def agreement_table(frame: pl.DataFrame) -> pl.DataFrame:
    """Select and rename the columns the outlier table shows.

    Args:
        frame: An :func:`agreement_frame` or :func:`agreement_outliers` result.

    Returns:
        pl.DataFrame: Renamed display columns in :data:`AGREEMENT_COLUMNS` order,
        skipping any the frame does not carry.
    """
    present = [(source, label) for source, label in AGREEMENT_COLUMNS
               if source in frame.columns]
    return frame.select([pl.col(source).alias(label) for source, label in present])


#: Lens to the board column it sorts and filters on.
#:
#: The label half of this mapping was dropped when the table grew spanners: both
#: lenses now render under the same ``Draft Metric | Δ`` header, and which column is
#: behind it is :data:`COLUMNS`' business rather than this one's.
VALUE_LENS_COLUMNS = {
    VALUE_LENS_ADP: "value",
    VALUE_LENS_CASH: "cash_delta",
}


def default_value_lens(meta: Mapping) -> str:
    """Which lens this league should open on.

    Args:
        meta: The store's ``meta.json``.

    Returns:
        str: :data:`VALUE_LENS_CASH` for an auction league, :data:`VALUE_LENS_ADP`
        otherwise. Both remain selectable -- an auction manager still benefits from
        knowing the room drafts a player two rounds late.
    """
    return VALUE_LENS_CASH if is_auction(meta) else VALUE_LENS_ADP


def value_targets(board: pl.DataFrame, limit: int = 12,
                  only_available: bool = True,
                  lens: str = VALUE_LENS_ADP) -> pl.DataFrame:
    """The players the room is letting fall furthest past our valuation.

    Under :data:`VALUE_LENS_ADP`, ``value`` is NaN for the 84% of the pool the
    market has not priced and for the streamed positions, where a season-total VOR
    does not describe how the position is used. Under :data:`VALUE_LENS_CASH` it is
    ``cash_delta``, which is null wherever either side of the subtraction is --
    a player the market has not priced, or one outside the money. Either way the
    nulls are excluded rather than sorted to the bottom, because a "best values"
    list is worthless if most of its rows are blank.

    Args:
        board: A stored draft board. For the cash lens it must already have been
            through :func:`at_budget` and :func:`with_cash_value`.
        limit: Rows to return.
        only_available: Drop players somebody holds. Pass False in a keeper league
            before keepers are declared, where nobody is held yet.
        lens: A :data:`VALUE_LENS_COLUMNS` key.

    Returns:
        pl.DataFrame: The top ``limit`` by the lens's column, available players
        only. Empty when the board does not carry that column.
    """
    column = VALUE_LENS_COLUMNS.get(lens, VALUE_LENS_COLUMNS[VALUE_LENS_ADP])
    if column not in board.columns:
        return board.head(0)
    return (
        filter_board(board, None, only_available=only_available,
                     hide_unstartable=True, hide_unprojected=True)
        .filter(pl.col(column).is_not_null())
        .sort(column, descending=True)
        .head(limit)
    )


#: Column order for the owner-tendency detail table, and the label each gets.
TENDENCY_COLUMNS: List[tuple] = [
    ("owner_display", "Manager"),
    ("seasons", "Drafts"),
    ("headline", "Reads As"),
    ("earliest_position", "Earliest"),
    ("earliest_delta", "Rds Early"),
    ("latest_position", "Latest"),
    ("latest_delta", "Rds Late"),
    ("favourite_team", "NFL Lean"),
    ("favourite_team_excess", "Extra Picks"),
    ("favourite_player", "His Guy"),
    ("favourite_player_times", "Times"),
    ("rookie_rate", "Rookie %"),
    ("auto_rate", "Auto %"),
    ("top3_share", "Top-3 $"),
]


def timing_matrix(picks: pl.DataFrame, positions: Sequence[str],
                  min_seasons: int = 2) -> pl.DataFrame:
    """Rounds each manager takes each position, against the same room.

    Computed here rather than stored because it is a reshape of ``draft.parquet``
    -- 960 rows for the deepest league -- and because reusing
    ``Scripts.draft.tendencies.positional_timing`` is the only way the chart and
    the descriptions cannot disagree about what "two rounds early" means.

    Args:
        picks: A stored pick history.
        positions: Positions to keep, in the page's current filter.
        min_seasons: Managers with fewer drafts are dropped; one draft is not a
            tendency and plotting it as one is the whole failure mode here.

    Returns:
        pl.DataFrame: ``owner``, ``position``, ``own_round``, ``room_round``,
        ``delta``, ``seasons``. Empty for an auction league, which has no rounds
        to be early in.
    """
    from Scripts.draft.tendencies import positional_timing

    timing = positional_timing(picks)
    if timing.is_empty():
        return timing
    keep = [p for p in positions if p in TIMED_CHART_POSITIONS]
    return timing.filter(pl.col("position").is_in(keep)
                         & (pl.col("seasons") >= min_seasons))


#: Positions the timing chart draws. The same six the tendencies module times,
#: intersected with the palette -- every one has a fixed hue already.
TIMED_CHART_POSITIONS = ("QB", "RB", "WR", "TE", "K", "D/ST")


def owner_label(owner: str) -> str:
    """A manager's name as it should be shown.

    Re-exported from the tendencies module rather than reimplemented, so the
    chart's axis and the cards above it cannot spell a manager two ways.

    Args:
        owner: The name as ESPN stores it.

    Returns:
        str: e.g. ``"Hank Winfield"``.
    """
    from Scripts.draft.tendencies import display_name

    return display_name(owner)


#: What a manager's points are split into: the players they drafted themselves,
#: against everyone else they fielded — waiver claims, free agents, trade returns.
#: Labels rather than booleans because they are read off a chart legend.
SOURCE_DRAFTED = "Drafted"
SOURCE_ADDED = "Added in Season"

#: ``team_owner`` in a lineups frame is this for a player nobody rostered that
#: week. It is not a manager and must not become a bar.
FREE_AGENT_OWNER = "Free Agent"


def _franchise_key(column: str) -> pl.Expr:
    """A team name reduced to something two artifacts can be joined on.

    **The franchise is the join key, not the manager,** and that was arrived at by
    measurement rather than preference. ESPN does not spell a person consistently
    across its endpoints, so joining on the manager silently loses whole teams and
    reports them as having drafted nothing at all. Every row below is real, in
    2025, in a different league:

    | ``lineups.parquet`` | ``draft.parquet`` | Cause |
    |---|---|---|
    | ``Hank Winfield`` | ``hank Winfield`` | ``str.title`` in ``set_owner_names`` |
    | ``Zach Imel`` | ``Zachary Imel`` | a nickname |
    | ``Logan Tola`` | ``Matt Logan Tola`` | an extra given name |
    | ``Alex Holton`` | ``Michael Beal`` | the team changed hands |

    Case-folding fixes only the first. The last cannot be fixed by any string rule
    and is what settles the design: the question is what a *roster* got from draft
    day, and a roster survives a handover even though the name against it does
    not. Each of these cost a manager their entire draft — 2,592.95 points for
    Zach Imel, all of it reported as waiver pickups.

    ``team_name`` was checked before being trusted, across all nine leagues for
    2025: no team renamed itself mid-season, no two teams shared a name, and all
    108 matched a drafting team. The manager is still who gets displayed.

    Args:
        column: Name of the column holding the team name.

    Returns:
        pl.Expr: The name trimmed and lowercased.
    """
    return pl.col(column).str.strip_chars().str.to_lowercase()


def drafted_versus_added(lineups: pl.DataFrame, picks: pl.DataFrame,
                         starters_only: bool = True) -> pl.DataFrame:
    """How much of each manager's season came from their own draft.

    A player counts as *drafted* for a team only if that team drafted them. The
    same player is drafted for whoever took them and added for whoever picked them
    up later, which is the honest reading of a mid-season move and falls out of
    joining on the pair rather than on the player. Teams are matched on
    ``team_name`` — see :func:`_franchise_key` for why the manager's name does not
    work.

    Starters only by default. Bench points are real but never counted for anyone,
    so including them would answer a different question — how much talent did you
    hold — than the one asked, which is how much of what you *scored* came from
    draft day.

    A team whose name is absent from ``picks`` gets **null** rather than zero for
    ``drafted`` and ``added``. Zero would be a claim that they drafted nobody who
    scored; null is the truth, which is that this season's draft cannot be matched
    to them. The distinction is the one that has already cost this repo three
    separate bugs.

    Args:
        lineups: One season's weekly rows, with ``team_name``, ``team_owner``,
            ``player_id``, ``slotPosition`` and ``points``.
        picks: Draft picks for **that same season**, with ``team_name`` and
            ``player_id``. Filter by season before calling; this cannot tell one
            season's picks from another's and would count a player drafted in any
            season as drafted in this one.
        starters_only: Count only points scored from a starting slot.

    Returns:
        pl.DataFrame: One row per team — ``owner``, ``manager`` (rendered),
        ``drafted``, ``added``, ``total`` and ``share_drafted`` (0–100), ordered
        by total points descending. ``drafted``/``added``/``share_drafted`` are
        null where the draft could not be matched. Empty if either input is, or if
        ``picks`` is empty — with no draft every point would classify as added,
        and a league that looks like it built itself off waivers is exactly the
        kind of confident wrong answer this codebase keeps having to unlearn.
    """
    needed = {"team_name", "team_owner", "player_id", "points", "slotPosition"}
    empty = pl.DataFrame(schema={"owner": pl.Utf8, "manager": pl.Utf8,
                                 "drafted": pl.Float64, "added": pl.Float64,
                                 "total": pl.Float64,
                                 "share_drafted": pl.Float64,
                                 "moves": pl.UInt32,
                                 "points_per_move": pl.Float64})
    if (lineups.is_empty() or picks.is_empty()
            or not needed <= set(lineups.columns)
            or "team_name" not in picks.columns):
        return empty

    drafted = (picks.select(_franchise_key("team_name").alias("_team"), "player_id")
               .unique()
               .with_columns(pl.lit(True).alias("_drafted")))
    # ESPN's owner GUID, carried through so seasons can be grouped by *person*.
    # Necessary because neither of the other two identities survives a decade:
    # team names change (Jack's "Cococnut Crushers" became "Coconut Crushers" in
    # 2023, Tommy renamed his four times) and so do owner names (ESPN recorded
    # Jack as "J W" in one season, which split him into a separate manager with
    # one season to his name). The GUID has been the same for all six managers
    # every year since 2019.
    identities = (picks.select(_franchise_key("team_name").alias("_team"),
                               pl.col("owner_id").alias("owner_id"))
                  .unique(subset=["_team"])
                  if "owner_id" in picks.columns else None)
    drafting_teams = drafted.select("_team").unique()

    rostered = (lineups
                .filter(pl.col("team_owner").is_not_null()
                        & (pl.col("team_owner") != FREE_AGENT_OWNER))
                .with_columns(_franchise_key("team_name").alias("_team"))
                .join(drafted, on=["_team", "player_id"], how="left")
                .with_columns(pl.col("_drafted").fill_null(False))
                .join(drafting_teams.with_columns(pl.lit(True).alias("_known")),
                      on="_team", how="left")
                .with_columns(pl.col("_known").fill_null(False)))

    # Counted before the starter filter, and deliberately. A move is a player you
    # brought in; whether you ever started them is the *result* of the move, not
    # part of its definition. Counting only the ones who started would flatter a
    # manager who claimed twenty players and got three right, which is exactly
    # what points-per-move is supposed to expose.
    moves = (rostered.filter(pl.col("_drafted").not_())
             .group_by("_team")
             .agg(pl.col("player_id").n_unique().alias("moves")))

    rows = (rostered.filter(
                pl.col("slotPosition").is_in(list(NON_STARTING_SLOTS)).not_())
            if starters_only else rostered)

    if rows.is_empty():
        return empty

    summary = (rows.group_by("_team")
               .agg(pl.col("team_owner").mode().first().alias("owner"),
                    pl.col("_known").first().alias("_known"),
                    pl.col("points").filter(pl.col("_drafted")).sum().alias("_drafted_pts"),
                    pl.col("points").filter(pl.col("_drafted").not_()).sum().alias("_added_pts"),
                    pl.col("points").sum().alias("total"))
               .join(moves, on="_team", how="left")
               .with_columns(pl.col("moves").fill_null(0))
               .with_columns(
                   pl.when("_known").then(pl.col("_drafted_pts")).alias("drafted"),
                   pl.when("_known").then(pl.col("_added_pts")).alias("added"))
               .with_columns(
                   pl.when(pl.col("total") > 0)
                   .then(100 * pl.col("drafted") / pl.col("total"))
                   .otherwise(None).alias("share_drafted"),
                   # Null rather than zero when nobody was added: a manager who
                   # never touched the wire has no points-per-move, and 0.0 would
                   # rank them alongside one whose every pickup failed.
                   pl.when(pl.col("moves") > 0)
                   .then(pl.col("added") / pl.col("moves"))
                   .otherwise(None).alias("points_per_move"),
                   pl.col("owner").map_elements(owner_label, return_dtype=pl.Utf8)
                   .alias("manager")))

    if identities is None:
        summary = summary.with_columns(pl.lit(None, dtype=pl.Utf8).alias("owner_id"))
    else:
        summary = summary.join(identities, on="_team", how="left")

    return (summary.select("owner", "manager", "owner_id", "drafted", "added",
                           "total", "share_drafted", "moves", "points_per_move")
            .sort("total", descending=True))


def acquisition_history(picks: pl.DataFrame,
                        results_by_season: Mapping[int, pl.DataFrame]
                        ) -> pl.DataFrame:
    """Run :func:`drafted_versus_added` over several seasons and stack the answers.

    Each season is matched against *its own* draft. That is the whole reason this
    is a loop rather than one join over a concatenated frame: team names and owner
    names both drift between seasons, so a player drafted by "Coconut Crushers" in
    2023 must not be credited to "Cococnut Crushers" in 2022.

    Args:
        picks: Draft picks across every season, carrying a ``season`` column —
            i.e. ``draft.parquet`` as stored.
        results_by_season: Season year to that season's ``results`` frame.

    Returns:
        pl.DataFrame: :func:`drafted_versus_added` output for each season with a
        ``season`` column added, concatenated. Seasons that produce nothing are
        skipped. Empty when none of them produce anything.
    """
    frames = []
    for season, results in sorted(results_by_season.items()):
        one = drafted_versus_added(results,
                                   picks.filter(pl.col("season") == season))
        if not one.is_empty():
            frames.append(one.with_columns(
                pl.lit(season, dtype=pl.Int64).alias("season")))
    if not frames:
        return drafted_versus_added(pl.DataFrame(), pl.DataFrame()).with_columns(
            pl.lit(None, dtype=pl.Int64).alias("season"))
    return pl.concat(frames)


def acquisition_averages(history: pl.DataFrame) -> pl.DataFrame:
    """Average a manager's acquisition split across every season they played.

    The per-season numbers answer "what happened that year"; this answers whether
    it is a habit. A manager who leans on the wire once had a bad draft; one who
    does it every year is telling you something about how they play.

    Averaged per season rather than pooled, so a manager is not weighted by how
    many seasons they happened to be in the league. ``share_drafted`` is the mean
    of the seasonal shares for the same reason — a pooled ratio would let their
    highest-scoring season dominate the description of their habits.

    Args:
        history: :func:`drafted_versus_added` output for several seasons,
            concatenated, carrying a ``season`` column.

    Returns:
        pl.DataFrame: One row per manager — ``manager``, ``seasons``, and the
        per-season means of ``drafted``, ``added``, ``total``, ``share_drafted``,
        ``moves`` and ``points_per_move`` — ordered by mean share drafted
        descending, so the managers who lived off their own drafts come first.
        Empty if the input is.
    """
    if history.is_empty() or "season" not in history.columns:
        return pl.DataFrame(schema={"manager": pl.Utf8, "seasons": pl.UInt32,
                                    "drafted": pl.Float64, "added": pl.Float64,
                                    "total": pl.Float64,
                                    "share_drafted": pl.Float64,
                                    "moves": pl.Float64,
                                    "points_per_move": pl.Float64})

    # Group on the GUID where there is one. Grouping on the displayed name splits
    # a manager in two the moment ESPN spells them differently for one season --
    # Jack Winfield came out as six seasons plus a separate "J W" with one.
    identity = ("owner_id" if "owner_id" in history.columns
                and history["owner_id"].null_count() == 0 else "manager")

    return (history.sort("season")
            .group_by(identity)
            .agg(pl.col("season").n_unique().alias("seasons"),
                 # The name they go by *now*, not an arbitrary one from 2019.
                 pl.col("manager").last().alias("manager"),
                 pl.col("drafted").mean(),
                 pl.col("added").mean(),
                 pl.col("total").mean(),
                 pl.col("share_drafted").mean(),
                 pl.col("moves").mean(),
                 pl.col("points_per_move").mean())
            .select("manager", "seasons", "drafted", "added", "total",
                    "share_drafted", "moves", "points_per_move")
            .sort("share_drafted", descending=True, nulls_last=True))


#: Column order and labels for the acquisition tables, both the multi-season
#: averages and one season on its own.
ACQUISITION_COLUMNS: List[tuple] = [
    ("manager", "Manager"),
    ("seasons", "Seasons"),
    ("total", "Points"),
    ("drafted", "From the Draft"),
    ("added", "From the Wire"),
    ("share_drafted", "% Drafted"),
    ("moves", "Moves"),
    ("points_per_move", "Pts / Move"),
]


def acquisition_frame(summary: pl.DataFrame) -> pl.DataFrame:
    """Select and rename the columns the acquisition tables show.

    Args:
        summary: :func:`acquisition_averages` or :func:`drafted_versus_added`
            output. The latter carries no ``seasons`` column, which is skipped
            rather than faked.

    Returns:
        pl.DataFrame: Renamed display columns, in :data:`ACQUISITION_COLUMNS`
        order.
    """
    present = [(source, label) for source, label in ACQUISITION_COLUMNS
               if source in summary.columns]
    return summary.select([pl.col(source).alias(label)
                           for source, label in present])


def notes_for_board(tendencies: pl.DataFrame,
                    board: pl.DataFrame) -> pl.DataFrame:
    """Drop the player clause from a note when that player is not draftable.

    A manager's loyalty is measured over every draft on record, which is what
    makes it a tendency and also what makes it go stale: three of the six
    Winfield_Football notes cited Leonard Fournette, LeSean McCoy or T.Y. Hilton,
    none of whom are on a 2026 board. True, and useless at a 2026 draft — it
    spends a third of a note nobody has time to read on a player nobody can take.

    Only the loyalty clause is filtered. Timing, team lean and rookie appetite are
    statements about *how* a manager drafts and stay true whoever is available.

    The clause is removed rather than replaced. The sentences a note is built from
    are chosen under constraints this cannot see — at most two timing traits, one
    per family — so promoting the next trait in the list could produce a note the
    builder would never have written. A shorter note is the honest outcome.

    Args:
        tendencies: A stored tendencies frame, carrying ``traits``,
            ``description`` and ``favourite_player``.
        board: This season's board, for ``player_name``.

    Returns:
        pl.DataFrame: The same frame with ``description`` and ``traits`` rewritten
        where the cited player is absent from the board, plus
        ``favourite_player_draftable``. Unchanged when either input lacks the
        columns to decide.
    """
    from Scripts.draft.tendencies import sentence_case

    needed = {"traits", "description", "favourite_player", "seasons"}
    if (tendencies.is_empty() or not needed <= set(tendencies.columns)
            or "player_name" not in board.columns):
        return tendencies

    draftable = set(board["player_name"].to_list())

    def _rewrite(row: dict) -> dict:
        player = row["favourite_player"]
        if not player or player in draftable:
            return {"description": row["description"], "traits": row["traits"],
                    "favourite_player_draftable": bool(player)}

        # The traits that made it into the note, in the order they appear there.
        # Matching on the trait text rather than splitting the description on
        # sentences, because "T.Y. Hilton" is full of full stops.
        text = (row["description"] or "").lower()
        used = [trait for trait in (row["traits"] or [])
                if trait and trait.lower() in text]
        kept = [trait for trait in used if player.lower() not in trait.lower()]
        seasons = row["seasons"]
        if not kept:
            rewritten = (f"{len(used)} of this manager's tendencies are about "
                         f"players who are not on this year's board. "
                         f"({seasons} drafts)")
        else:
            rewritten = (" ".join(sentence_case(trait) for trait in kept)
                         + f" ({seasons} drafts)")
        return {"description": rewritten,
                "traits": [t for t in (row["traits"] or [])
                           if player.lower() not in t.lower()],
                "favourite_player_draftable": False}

    rewritten = [_rewrite(row) for row in tendencies.iter_rows(named=True)]
    return tendencies.with_columns(
        pl.Series("description", [r["description"] for r in rewritten]),
        pl.Series("traits", [r["traits"] for r in rewritten]),
        pl.Series("favourite_player_draftable",
                  [r["favourite_player_draftable"] for r in rewritten]),
    )


def tendency_frame(tendencies: pl.DataFrame) -> pl.DataFrame:
    """Select and rename the columns the tendency detail table shows.

    Args:
        tendencies: A stored tendencies frame.

    Returns:
        pl.DataFrame: Renamed display columns, in :data:`TENDENCY_COLUMNS` order.
    """
    present = [(source, label) for source, label in TENDENCY_COLUMNS
               if source in tendencies.columns]
    return tendencies.select([pl.col(source).alias(label)
                              for source, label in present])


def with_model_evidence(board: pl.DataFrame) -> pl.DataFrame:
    """Add ``usg_evidence_label``: why the model's number is thin, or missing.

    The board carries the model's self-assessment across four columns, and an empty
    ``USG`` cell can mean four different things that matter differently at a draft:
    the model does not cover the position, the model declined to price a player it
    could see, the injury report withdrew a price it had already made, or the board
    build withdrew one because the depth chart says he is a backup ESPN has priced
    out. Collapsing those into one blank throws away the distinction; this resolves
    them into one readable string instead.

    Order matters and is not arbitrary. A withdrawal is checked before the evidence
    text because a player can carry both -- the model flagged its evidence *and* then
    produced nothing -- and "there is no number here" is the more useful fact than
    why the number that does not exist would have been shaky. Within the withdrawals,
    role is checked before injury, because a role withdrawal nulls the same column and
    would otherwise report a healthy backup as hurt.

    ``usg_evidence`` arrives as an empty string when the model ran and flagged
    nothing, and as null when the model never ran. Those are different facts and both
    would render as an empty cell, which is why neither is passed through as-is.

    Args:
        board: A stored draft board. Boards written before the usage model landed
            carry none of the ``usg_*`` columns.

    Returns:
        pl.DataFrame: The board with ``usg_evidence_label`` added, or returned
        unchanged if it carries no ``usg_arm`` to reason about — in which case
        :func:`display_frame` drops the column along with the rest of the model
        block, exactly as it does for any other artifact that predates a feature.
    """
    if "usg_arm" not in board.columns:
        return board

    evidence = (pl.col("usg_evidence") if "usg_evidence" in board.columns
                else pl.lit(None, dtype=pl.String))
    points = (pl.col("USG_Points") if "USG_Points" in board.columns
              else pl.lit(None, dtype=pl.Float64))

    return board.with_columns(
        pl.when(pl.col("usg_arm").is_null())
        .then(pl.lit(EVIDENCE_NOT_MODELLED))
        .when(pl.col("usg_arm") == "abstain")
        .then(pl.lit(EVIDENCE_WITHDRAWN_AVAILABILITY))
        # Before the injury branch: a role withdrawal also nulls `USG_Points`, so
        # without this it would render as "withdrawn (injury)" and tell a drafter
        # the player is hurt when he is merely third on the depth chart.
        .when(points.is_null() & (evidence == EVIDENCE_ROLE_MARKER))
        .then(pl.lit(EVIDENCE_WITHDRAWN_ROLE))
        .when(points.is_null())
        .then(pl.lit(EVIDENCE_WITHDRAWN_INJURY))
        .when(evidence.fill_null("") != "")
        .then(evidence)
        .otherwise(pl.lit(EVIDENCE_CLEAR))
        .alias("usg_evidence_label")
    )


def with_injury_severity(board: pl.DataFrame) -> pl.DataFrame:
    """Add ``inj_severity_label``: what is wrong, how long for, and how we know.

    ``injury_code`` beside it answers *is he available*; this answers *what is it and how
    long*. They come from different places and disagree usefully: ESPN's fantasy status
    had Jeremiyah Love as ``ACTIVE`` at ADP 18 while a hand-written override put him four
    to six weeks out with a high ankle sprain.

    **The provenance is in the label, not in a tooltip.** Six channels can answer, and
    they are not equally trustworthy -- a hand-checked override and a body part guessed
    from a news sentence are both a body part and a number of weeks, and a reader deciding
    whether to spend an eighteenth pick on it needs to know which one he is looking at.
    Measured on Winfield Football's 2026 board: 102 of the 132 resolved severities came
    from free text, 16 from an ESPN diagnosis, 13 from a return date and 1 from an
    override. So the weak rung is carrying most of the column, and saying so is the
    difference between a useful column and a misleading one.

    Args:
        board: A stored draft board.

    Returns:
        pl.DataFrame: The board with ``inj_severity_label`` added where it carries
        ``inj_severity_source``. Unchanged otherwise, so an older artifact still renders.
    """
    if "inj_severity_source" not in board.columns:
        return board

    source = pl.col("inj_severity_source").cast(pl.String)
    detail = pl.col("inj_detail").cast(pl.String) if "inj_detail" in board.columns \
        else pl.lit(None, dtype=pl.String)
    part = pl.col("inj_body_part").cast(pl.String) if "inj_body_part" in board.columns \
        else pl.lit(None, dtype=pl.String)

    # Prefer the specific reading, but only when it is genuinely a refinement of the body
    # part. "ankle high" is what made the override worth writing and collapsing it back to
    # "ankle" throws that away; "multi week" is a duration, and a Body Part column that
    # says "multi week" is worse than one that says "foot toe".
    readable_part = part.str.replace_all("_", " ")
    readable_detail = detail.str.replace_all("_", " ")
    refines = (readable_detail.is_not_null()
               & (readable_detail != readable_part)
               & (readable_detail.str.contains(readable_part.str.split(" ").list.first())
                  | (part == "other")))
    named = pl.when(refines).then(readable_detail).otherwise(readable_part)

    board = board.with_columns(
        pl.when(source.is_null())
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(pl.concat_str([
            named.fill_null("unknown"),
            pl.lit(" ("),
            source.replace_strict(INJURY_SEVERITY_SOURCES, default=source),
            pl.lit(")"),
        ])).alias("inj_severity_label"))

    # Percentage points for display, in a column of its own rather than by rescaling the
    # stored one. The artifact keeps the probability as a fraction because that is what
    # arithmetic wants; quietly changing a column's units in the render layer is how a
    # reader ends up unsure which one he is looking at.
    if "inj_reinjury_prob" in board.columns:
        board = board.with_columns(
            (pl.col("inj_reinjury_prob").cast(pl.Float64) * 100.0)
            .alias("inj_reinjury_pct"))
    return board


def with_percent_columns(board: pl.DataFrame) -> pl.DataFrame:
    """Rescale the 0-1 probabilities into the 0-100 the ``%`` formats expect.

    **This exists because the alternative silently renders every one of them as 0%.**
    ``column_config`` formats are printf, so ``"%.0f%%"`` on a probability of 0.90 prints
    ``1%``, and on 0.32 it prints ``0%`` -- no error, no blank cell, just a column of
    zeroes that reads as "this player never busts" when it means the opposite.

    Derived columns rather than a rescale in place, following ``inj_reinjury_pct``: the
    stored artifact keeps the units arithmetic wants, and quietly changing a column's
    units in the render layer is how a reader ends up unsure which one he is looking at.

    ``usg_role_confidence`` is in here too. It shipped with plan 33 phase 2 against a
    ``%`` format and a 0-1 source, so every board built since has shown ``Role %`` as 0%
    for all 671 players who have one. Display only -- no projection moves.

    Args:
        board: The stored board.

    Returns:
        pl.DataFrame: ``board`` with a ``<column>_pct`` per probability it carries.
    """
    scaled = [(pl.col(column).cast(pl.Float64) * 100.0).alias(f"{column}_pct")
              for column in ("p_top12", "p_bust", "usg_role_confidence")
              if column in board.columns]
    return board.with_columns(scaled) if scaled else board


def with_injury_code(board: pl.DataFrame) -> pl.DataFrame:
    """Add ``injury_code``: ESPN's fantasy status, abbreviated, and a readable return.

    Two derivations rather than one because they answer the same question together and
    neither is much use alone. ``ACTIVE`` for 2,204 rows of 2,503 is a column of noise
    until the exceptions are short enough to scan, and ``IR`` does not say whether a
    player is back in November or gone for the year -- only the return date does.

    **An unmapped status passes through in full.** ESPN's enum is not published and
    this repo has observed five of it; abbreviating an unknown sixth to its first
    letter would collide with a real code, and blanking it would hide a player's
    status entirely. Showing ``DAY_TO_DAY`` in a narrow column is ugly and correct.

    The return date is rendered here rather than formatted in the page because the
    season-ending sentinel is a *value*, not a format: ESPN encodes "out for the year"
    as a date past the end of the season, and a reader shown ``2027-02-01`` would have
    to know that to read it. See ``SEASON_ENDING_AFTER`` in
    :mod:`Scripts.scrape_espn_injuries`, which is where the same convention is decoded
    for the availability scaling.

    Args:
        board: A stored draft board.

    Returns:
        pl.DataFrame: The board with ``injury_code`` added where it carries
        ``injury_status``, and ``injury_return_date`` rewritten as display text where
        it carries that. Unchanged for a board that has neither.
    """
    # Deferred so the render path does not import `requests` to read one date. The
    # sentinel is imported rather than restated because two copies of it would drift,
    # and the availability scaling in `season_projections` decodes the same convention.
    from Scripts.scrape_espn_injuries import SEASON_ENDING_AFTER

    if "injury_status" in board.columns:
        # Cast first: a board on which ESPN returned no status at all carries the
        # column as Null dtype, and `replace_strict` then tries to cast the map's
        # string values into it and raises.
        status = pl.col("injury_status").cast(pl.String)
        board = board.with_columns(
            status.replace_strict(INJURY_CODES, default=status).alias("injury_code")
        )

    if "injury_note" in board.columns:
        board = board.with_columns(
            pl.when(pl.col("injury_note").cast(pl.String).fill_null("") != "")
            .then(pl.lit(NOTE_MARK))
            .otherwise(pl.lit(None, dtype=pl.String))
            .alias("note_mark")
        )

    # Guarded on the dtype rather than on presence: this rewrites a date column into
    # text in place, so running twice on the same frame would re-parse its own output.
    dates = board.schema.get("injury_return_date")
    if dates in (pl.Date, pl.Datetime, pl.Datetime("ns"), pl.Datetime("us")):
        as_date = pl.col("injury_return_date").cast(pl.Date)
        board = board.with_columns(
            pl.when(as_date.is_null()).then(pl.lit(None, dtype=pl.String))
            .when(as_date > SEASON_ENDING_AFTER).then(pl.lit(RETURN_SEASON_ENDING))
            .otherwise(as_date.dt.to_string("%b %d"))
            .alias("injury_return_date")
        )
    return board


def player_note(table: pl.DataFrame, row: int) -> Optional[str]:
    """The selected player's injury note as markdown, or None if there is nothing.

    The other half of the ``News`` column: the table carries a mark, this renders the
    sentence behind it. Reached by selecting the row, because Streamlit's grid has no
    per-cell tooltip -- see :data:`NOTE_MARK`.

    Given the *sorted, filtered* frame the table was built from, because the row index
    a selection reports is a position in what is on screen and means nothing against
    the stored board.

    Args:
        table: The frame passed to :func:`display_frame`, in the same order.
        row: Zero-based row position from ``st.dataframe``'s selection.

    Returns:
        str | None: Markdown -- the player's name, position and team, whatever the
        report says about his status and return, then the note itself. None when the
        row is out of range or carries no note, so the caller renders nothing rather
        than an empty panel.
    """
    if row < 0 or row >= table.height:
        return None

    player = table.row(row, named=True)
    note = (player.get("injury_note") or "").strip()
    if not note:
        return None

    where = " ".join(str(player[key]) for key in ("primaryPosition", "pro_team")
                     if player.get(key))
    status = player.get("injury_status") or ""
    back = player.get("injury_return_date") or ""

    facts = [bit for bit in (where, status.title().replace("_", " ") if status else "",
                             f"back {back}" if back else "") if bit]
    heading = f"**{player.get('player_name', 'Unknown')}**"
    if facts:
        heading += " · " + " · ".join(facts)
    return f"{heading}\n\n{note}"


def player_note_for(board: pl.DataFrame, player_id) -> Optional[str]:
    """The note for one player, found by id rather than by position.

    The lookup half of :func:`remember_note_click`. Given the whole board, so a note
    stays readable after a filter that would have dropped the row it was clicked on.

    Args:
        board: Any frame carrying ``player_id``, before or after filtering.
        player_id: The id remembered from the click.

    Returns:
        str | None: Markdown from :func:`player_note`, or None where the player is not
        in this frame or carries no note.
    """
    if player_id is None or "player_id" not in board.columns:
        return None
    match = board.filter(pl.col("player_id") == player_id)
    return player_note(match, 0) if not match.is_empty() else None


def remember_note_click(state, table: pl.DataFrame, league_key: str = "",
                        click_key: str = NOTE_CLICK_KEY):
    """Resolve a news-mark click to a player and remember which, clicking again to close.

    **A click reports a row number, and a row number goes stale.** It is a position in
    the sorted, filtered frame on screen, so the moment the sort changes it names a
    different player -- silently, which is the worst way for it to be wrong. Streamlit
    also clears the click value on the *next* rerun, so a note read off it alone would
    vanish the first time anything else on the page moved. Resolving to a ``player_id``
    at click time and remembering that solves both.

    Toggles: clicking the mark of the player already open closes the panel, which is
    the only way to dismiss it without a second control.

    Args:
        state: ``st.session_state``, passed in so this stays testable with a plain
            dict and the module keeps its no-Streamlit-import rule.
        table: The frame the table was rendered from, in the same order.
        league_key: Scopes the memory per league, so switching leagues does not carry
            a note across to a board that may not even hold that player. Same reason
            :func:`budget_key` is scoped.
        click_key: Which button column's click to read. Each table needs its own --
            see :data:`VALUES_NOTE_CLICK_KEY`.

    Returns:
        The remembered ``player_id``, or None when nothing is open.
    """
    held_key = f"{NOTE_HELD_KEY}::{click_key}::{league_key}"
    click = state.get(click_key)

    row = None
    if click is not None:
        row = click.get("row") if hasattr(click, "get") else getattr(click, "row", None)

    if row is not None and 0 <= row < table.height and "player_id" in table.columns:
        clicked = table.row(row, named=True).get("player_id")
        state[held_key] = None if state.get(held_key) == clicked else clicked

    return state.get(held_key)


def shown_columns(board: pl.DataFrame, lens: str = VALUE_LENS_ADP) -> List[Column]:
    """The specs that will actually render, for this board and this lens.

    The single place the "which columns exist here" question is answered, so the
    frame, the column config and the glossary cannot disagree about it.

    Args:
        board: A board, at whatever stage of derivation the page has reached.
        lens: :data:`VALUE_LENS_ADP` or :data:`VALUE_LENS_CASH` -- which currency the
            ``Draft Metric`` group speaks.

            A spec carrying ``positions`` is dropped unless ``board`` holds one of
            them, so the ``DST`` column disappears on a running-back view rather than
            rendering 200 blanks. Unlike the lens this *is* an observation about the
            data: a team defence's projection is not missing from a receiver's row,
            it is a category error. A board with no ``primaryPosition`` column keeps
            every spec, since there is then nothing to filter on. Unlike every other group this one cannot
            be selected on column presence: ``adp`` and ``auction_dollars`` are both
            present in every league, so choosing between them is a decision about the
            draft rather than an observation about the data.

    Returns:
        List[Column]: In :data:`COLUMNS` order.
    """
    available = set()
    if "primaryPosition" in board.columns:
        available = set(board["primaryPosition"].drop_nulls().unique().to_list())

    return [column for column in COLUMNS
            if column.source in board.columns
            and column.lens in ("", lens)
            and (not column.positions
                 or not available
                 or bool(available & set(column.positions)))]


def display_frame(board: pl.DataFrame,
                  lens: str = VALUE_LENS_ADP) -> "pd.DataFrame":
    """Select and rename the columns the table shows, under spanner headers.

    Returns **pandas**, not Polars, and that is load-bearing rather than a
    preference. Two of the three things this table now does are pandas-only in
    Streamlit: grouped headers come from a ``MultiIndex`` on the columns, and cell
    colouring comes from a ``Styler``. Everything upstream of here stays Polars.

    A spec whose source column is absent drops out silently, which is what lets one
    spec list serve a redraft league with no keeper prices, a board built before the
    usage model existed, and an artifact written before the ESPN comparison columns
    were added.

    Args:
        board: A filtered board.
        lens: See :func:`shown_columns`.

    Returns:
        pd.DataFrame: One column per rendered spec, in :data:`COLUMNS` order, with a
        two-level column index of ``(group, label)``.
    """
    present = shown_columns(board, lens)
    # Selected under positional aliases, then renamed. Two specs may share a source
    # (`vor_rank` is both our overall rank and our implied pick order) and Polars will
    # not select one column twice under one name; the group and label cannot be joined
    # into the alias either, because Arrow field names may not contain a separator
    # exotic enough to be safe -- a NUL byte panics the conversion.
    frame = board.select(
        [pl.col(column.source).alias(f"_c{position}")
         for position, column in enumerate(present)]
    ).to_pandas()
    frame.columns = pd.MultiIndex.from_tuples(
        [(column.group, column.label) for column in present])

    return frame


class Shading(NamedTuple):
    """The threshold one difference column's fill is decided against.

    Only the differences are shaded. Colouring the raw points, ranks and prices too was
    built and then removed: at seventeen shaded columns the table read as a heatmap, and
    the columns carrying an opinion stopped being the ones that caught your eye. A level
    also has no midpoint to diverge around -- 340 projected points is not the opposite
    of anything -- so the fill had to be measured against the pool, which meant a second
    set of rules for half the table. The differences are the judgement; everything else
    is the context you read it against, and context does not need paint.

    Attributes:
        scale: The 90th percentile of the column's magnitude, which the two fill steps
            per arm are fractions of.
    """

    scale: float


def shade_scales(board: pl.DataFrame,
                 lens: str = VALUE_LENS_ADP) -> Dict[tuple, Shading]:
    """The fill threshold for every difference column, measured against this league.

    **Computed from the unfiltered board on purpose.** Scaling to whatever survives
    the current filters would repaint the table every time a position is deselected,
    so the same +14 would read as strong in one view and neutral in the next. A
    colour that moves when you filter is not encoding the number.

    Args:
        board: The league's whole board, before :func:`filter_board`.
        lens: See :func:`shown_columns`.

    Returns:
        Dict[tuple, Shading]: ``(group, label)`` to its threshold. A column with no
        measurable spread is omitted, which :func:`styled_frame` reads as "do not
        paint this one" -- a league where we and ESPN agree exactly has nothing to
        say, so it says nothing rather than painting a column of neutral grey.
    """
    scales = {}
    for column in shown_columns(board, lens):
        if column.shade != "delta":
            continue
        spread = board[column.source].abs().quantile(0.9)
        if spread:
            scales[(column.group, column.label)] = Shading(scale=float(spread))
    return scales


def shade_step(value: float, shading: Shading) -> int:
    """Which of the five diverging steps a difference falls in.

    Args:
        value: The difference.
        shading: That column's threshold, from :func:`shade_scales`.

    Returns:
        int: -2 to 2. Positive means we are higher on the player than ESPN is, which
        is the orientation every difference column is built to -- see :data:`COLUMNS`.
    """
    if value is None or pd.isna(value) or shading.scale <= 0:
        return 0
    share = abs(value) / shading.scale
    if share < DELTA_SOFT_AT:
        return 0
    step = 2 if share >= DELTA_STRONG_AT else 1
    return step if value > 0 else -step


def styled_frame(frame: "pd.DataFrame", scales: Mapping[tuple, Shading],
                 theme: str = "light"):
    """Fill the shaded columns, and bold the ones that carry a judgement.

    A ``Styler`` rather than ``column_config`` because Streamlit's grid takes cell
    colour only this way. It honours exactly three CSS properties -- ``color``,
    ``background-color`` and ``font-weight`` -- so this emits those and nothing else;
    borders, alignment and font-size in a Styler are silently dropped.

    **Only the difference columns are painted, and they are the only ones bolded.**
    Colouring the raw points, ranks and prices too was built and then removed: at
    seventeen shaded columns the table read as a heatmap and the columns carrying an
    opinion stopped being the ones that caught your eye.

    Args:
        frame: A :func:`display_frame` result.
        scales: From :func:`shade_scales`, computed on the *unfiltered* board.
        theme: ``light`` or ``dark``, from ``st.context.theme.type``. An unknown
            theme falls back to light rather than raising -- a new Streamlit theme
            name should not take the page down.

    Returns:
        pandas.io.formats.style.Styler: Ready to hand to ``st.dataframe``.
    """
    fills = DELTA_FILLS.get(theme, DELTA_FILLS["light"])
    ink = DELTA_INK.get(theme, DELTA_INK["light"])
    bold = {(column.group, column.label) for column in COLUMNS if column.emphasis}

    # `na_rep=""` and nothing else, and it covers exactly half the problem.
    #
    # A missing cell is drawn as the literal word "None" unless something says
    # otherwise, and for a *text* column the something has to be here: a Styler ships
    # display strings alongside the data, Streamlit prefers them wherever
    # ``column_config`` has no format of its own, and pandas renders a missing value
    # with ``str`` by default. A *number* column takes the other path -- its display
    # string is deliberately ignored in favour of ``column_config``'s format -- so the
    # blank there comes from ``st.dataframe(placeholder="")`` in the page.
    #
    # Both halves are load-bearing. Missing one of them put "None" on 998 of 1,026
    # rows of `Exp Return`, and on every unpriced player's `Δ`: columns whose blank
    # means "nobody published a number" instead asserting an answer.
    styler = frame.style.format(na_rep="")

    for key, shading in scales.items():
        if key not in frame.columns:
            continue

        def paint(value, shading=shading, weight="font-weight: 700"
                  if key in bold else ""):
            fill = fills[shade_step(value, shading)]
            # Bold regardless of fill: the emphasis says "this column is a
            # judgement", which is true of the rows we agree with ESPN on too.
            colour = f"background-color: {fill}; color: {ink}" if fill else ""
            return "; ".join(part for part in (colour, weight) if part)

        styler = styler.map(paint, subset=[key])
    return styler


#: Streamlit ``column_config`` kind to the spec fields the page needs to build it.
#:
#: Kept as data here, and turned into ``st.column_config`` objects by the page. This
#: module does not import Streamlit -- it is the half of the draft board that is
#: testable without a runtime, and the tests import it with no Streamlit at all.
def column_config_specs(frame: "pd.DataFrame",
                        lens: str = VALUE_LENS_ADP) -> Dict[int, Column]:
    """The spec behind each rendered column, keyed by its position in the frame.

    **Positional, and it has to be.** ``column_config`` accepts a column name or a
    numerical position, and the names here are not unique: ``ESPN``, ``Us`` and ``Δ``
    each repeat across groups, so a name-keyed config would apply the points format to
    the rank columns. Positions also close a silent failure the old hand-written dict
    had -- Streamlit ignores config for a column the frame does not carry, so a label
    that stopped existing stopped formatting without anybody being told.

    **The index counts as a column in that numbering, even hidden.** Streamlit's grid
    numbers every column it was handed, index first, and matches ``_pos:N`` against
    that; ``hide_index=True`` hides the index without renumbering what follows it. So
    the offset is the frame's index depth, and getting it wrong is not a crash -- every
    column silently wears its neighbour's format. It showed up as ``Tier`` rendering
    ``1.0``, ``Ranks | Us`` rendering ``+1``, and the identity block splitting across
    the frozen boundary because the fifth pin landed on the sixth column.

    Args:
        frame: A :func:`display_frame` result. Read rather than recomputed, so the
            positions cannot drift from the frame they describe.
        lens: See :func:`shown_columns`.

    Returns:
        Dict[int, Column]: Streamlit column position to its spec.
    """
    by_key = {}
    for column in COLUMNS:
        if column.lens in ("", lens):
            by_key.setdefault((column.group, column.label), column)
    offset = frame.index.nlevels
    return {position + offset: by_key[key]
            for position, key in enumerate(frame.columns)
            if key in by_key}
