"""One table for a matchup, and the same columns for a roster.

**Why this is HTML rather than a ``st.dataframe``.** The matchup table asks for four
things Streamlit's grid cannot do: three levels of header (team, then block, then
column), a merged cell where the ``TOTAL`` row replaces the identity columns with a
team name, a continuous fill on the advantage column rather than the draft board's
five discrete steps, and the same columns mirrored left-to-right so the two sides
meet in the middle. A ``Styler`` reaches exactly three CSS properties -- see
:func:`draft_view.styled_frame` -- and none of them merges cells.

The thing the grid gives up in exchange is sorting, and here that is a feature
rather than a cost: every row of a matchup table is a *slot*, and the pairing across
the middle is the whole point. Sorting one column would leave a quarterback opposite
a kicker.

Streamlit-free on purpose, like :mod:`lineup` and :mod:`draft_view`, so the pairing,
the totals and the emitted markup are all testable without a browser.
:mod:`views.weekly` hands the string to ``st.html``.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import html
import math
from typing import (Dict, Iterable, List, Mapping, NamedTuple, Optional,
                    Sequence, Tuple)

import lineup as lu
from Scripts.live import LIVE_POINTS, STATE_COLUMN

#: Source prefix to what it is, in the words the tooltip uses.
#:
#: Lives here rather than in :mod:`views.weekly` because it is vocabulary, not
#: layout, and both tables speak it -- this one and the ``st.dataframe`` the Free
#: Agents tab still draws. Two copies would eventually disagree about what a column
#: means.
SOURCE_HELP: Dict[str, str] = {
    "ESPN": "ESPN's own weekly projection. Present for every player, and the base "
            "every other source is imputed from when it has no opinion.",
    "FP": "FantasyPros' weekly consensus. Real for the players it publishes and "
          "imputed for the rest — check Sources against it.",
    "PINNY": "Pinnacle's weekly player props, converted to points.",
    "BOL": "BetOnline's weekly player props, converted to points.",
    "ATH": "The Athletic's weekly projection (Jake Ciely's workbook). Offence only "
           "— it never has an opinion about a kicker or a defence, and it projects "
           "who it thinks starts, so a player it omits is sometimes a judgement "
           "rather than a gap.",
    "TRUE": "The blend, in this league's own scoring. One equal vote per source "
            "that has an opinion about this player.",
}

#: How each per-source points column is headed. Short on purpose: twenty-five columns
#: only fit on a laptop if the numeric ones are four characters wide.
POINTS_LABELS: Dict[str, str] = {
    "ESPN": "ESPN", "FP": "FP", "PINNY": "PINNY", "BOL": "BOL", "ATH": "ATH",
    "TRUE": "TRUE",
}


class Col(NamedTuple):
    """One rendered column: where its number comes from and how it reads.

    Attributes:
        source: The frame column, or ``""`` for a column computed from others.
        label: The header.
        kind: ``text``, ``points``, ``delta``, ``count`` or ``spread``. Decides the
            format, the alignment, and how the ``TOTAL`` row aggregates it.
        help: Tooltip prose, shown as the header's ``title``.
    """

    source: str
    label: str
    kind: str
    help: str


#: The identity block, in reading order. Mirrored on the away side.
INFO_COLUMNS: Tuple[Col, ...] = (
    Col("player_name", "Player", "text", "The player, as ESPN spells him."),
    Col("player_position", "Pos", "text",
        "His own position — not the slot he is filling. A receiver in the flex "
        "still reads WR here."),
    Col("pro_team", "TM", "text", "His NFL team."),
)

#: Per-row format by :attr:`Col.kind`.
#:
#: Both differences carry a sign. A delta printed as ``2.4`` is ambiguous about which
#: way it points, and the whole reason the column exists is the direction.
FORMATS: Dict[str, str] = {
    "points": "{:.1f}", "delta": "{:+.1f}", "count": "{:.0f}", "spread": "{:.1f}",
}

#: Kinds whose sign is only claimed once the rounded number has one.
#:
#: ``{:+.1f}`` renders -0.04 as ``-0.0``, which asserts a direction the number does
#: not have -- and on the delta column, where the sign *is* the reading, a screenful
#: of ``-0.0`` says we are quietly below ESPN on players we agree with exactly.
SIGNED_KINDS = frozenset({"delta"})

#: Format for the same column on the ``TOTAL`` row.
#:
#: ``count`` differs because its total is a *mean* -- see :func:`totals` -- and a
#: mean of 1.6 sources printed as ``2`` would assert corroboration the lineup does
#: not have.
TOTAL_FORMATS: Dict[str, str] = {**FORMATS, "count": "{:.1f}"}

#: Where the advantage fill saturates, in points at one slot.
#:
#: A domain constant rather than a percentile of the table, and that is deliberate.
#: The draft board scales its fills to the board's own spread because rank
#: differences and dollar differences are not comparable units; here every cell is
#: fantasy points, so a fixed scale makes the colour mean the same thing in week 3 as
#: in week 14. Ten points at a single slot is most of a typical weekly margin, which
#: is why it is the point where the cell is as red or as green as it gets.
ADVANTAGE_FULL_AT = 10.0

#: Where the ``TOTAL`` row's advantage fill saturates, in points of margin.
#:
#: Larger than :data:`ADVANTAGE_FULL_AT` because a margin is the sum of nine or ten
#: slot advantages. On the per-slot scale every total above ten points would paint
#: identically, and "winning by 11" and "winning by 40" are not the same matchup.
MARGIN_FULL_AT = 25.0

#: The two arms of the advantage fill, as RGB.
#:
#: The draft board's endpoints, composited over the table's own background through an
#: alpha rather than picked per theme. One pair of colours therefore reads correctly
#: in light and in dark, and the module needs no theme argument -- unlike
#: :func:`draft_view.styled_frame`, which emits opaque fills and has to.
ADVANTAGE_RGB: Dict[str, Tuple[int, int, int]] = {
    "positive": (27, 175, 122), "negative": (227, 73, 72),
}

#: Alpha at full saturation. Below 1.0 so the number stays readable through the fill.
ADVANTAGE_MAX_ALPHA = 0.6

#: Fills fainter than this are dropped rather than drawn, so a one-point edge reads
#: as the neutral it effectively is instead of as a smudge.
ADVANTAGE_MIN_ALPHA = 0.05


#: The resolved live column's header and tooltip.
#:
#: Its own constant rather than an entry in :data:`POINTS_LABELS`, because that dict
#: is keyed by projection *source* and this is not one -- it is what the sources and
#: the box score resolve to. Same reason ``LIVE`` is absent from
#: ``projection_utils.WEEKLY_PREFIXES``.
LIVE_LABEL = "LIVE"
LIVE_HELP = ("What this player is worth right now: points already scored where his "
             "game is final, our blend where it has not kicked off, and the two "
             "blended by the game clock in between. Before the first kickoff it is "
             "exactly TRUE.")

#: The actual-points column, shown only once something has been played.
ACTUAL_LABEL = "ACT"
ACTUAL_HELP = ("Points actually scored, as ESPN's box score has them -- which is the "
               "league's official number, so this is what the standings will say.")

#: How each game state reads in the table. Four characters at most: this sits in the
#: identity block beside three-letter team abbreviations.
STATE_LABELS: Dict[str, str] = {
    "pre": "—", "in": "LIVE", "post": "FINAL", "bye": "BYE",
}

STATE_HELP = ("Where his NFL game is. Blank before kickoff, LIVE while it is being "
              "played, FINAL once it is over -- and a locked player cannot be moved "
              "out of, or into, a starting slot.")


def points_columns(columns: Iterable[str], meta: dict, *,
                   blend_first: bool = False,
                   corroboration: bool = True,
                   locked: bool = False) -> List[Col]:
    """The points block for a league, in reading order.

    The sources this league really has, then the blend, then what the blend is worth
    against ESPN, then how well corroborated it is. A source the store marked absent
    is dropped by :func:`lineup.real_sources` rather than shown -- the blend imputes
    it from the ESPN/FantasyPros mean, so rendering it would turn silence into
    unanimous agreement.

    Args:
        columns: The frame's column names.
        meta: The store's ``meta.json``.
        blend_first: Put ``TRUE`` and ``Δ`` ahead of the sources rather than after
            them. The Roster tab reads this way because it is a *decision* table --
            the blend is the number the lineup is chosen on, and it belongs beside
            the player's name rather than four columns downstream of it. The Matchup
            tab keeps the sources first, so that the blend and the delta land next to
            the ``ADV`` column they are being compared through.
        corroboration: Include ``Sources`` and ``Spread``. Off for the matchup table,
            where they are the two columns that answer a question nobody is asking
            across a fixture -- how well corroborated *my* receiver is says nothing
            about whether he beats theirs -- and where dropping them takes the table
            from 25 columns to 21.
        locked: Whether any player in this table has kicked off. Gates the ``ACT``
            column, which before the first game of the week is a column of zeros --
            and a column of zeros beside a column of projections invites exactly the
            wrong reading.

    Returns:
        list: :class:`Col` specs whose sources the frame carries.
    """
    have = set(columns)
    # First, because it is the number the table is read for. The sources behind it
    # follow in the order they always did.
    resolved: List[Col] = []
    if LIVE_POINTS in have:
        resolved.append(Col(LIVE_POINTS, LIVE_LABEL, "points", LIVE_HELP))
        if locked and "points" in have:
            resolved.append(Col("points", ACTUAL_LABEL, "points", ACTUAL_HELP))

    sources: List[Col] = []
    for prefix in lu.real_sources(meta):
        source = f"{prefix}_Points"
        if source in have:
            sources.append(Col(source, POINTS_LABELS.get(prefix, prefix), "points",
                               SOURCE_HELP.get(prefix, "")))

    blend: List[Col] = []
    if f"{lu.BLEND}_Points" in have:
        blend.append(Col(f"{lu.BLEND}_Points", POINTS_LABELS[lu.BLEND], "points",
                         SOURCE_HELP[lu.BLEND]))
        # Only where both halves exist. A delta against an absent ESPN column would
        # be the blend restated with a plus sign in front of it.
        if "ESPN_Points" in have:
            blend.append(Col("", "Δ", "delta",
                             "TRUE − ESPN. How far our blend is from the number "
                             "ESPN sets the league's expectations with. Positive "
                             "means we are higher on him."))

    specs = resolved + (blend + sources if blend_first else sources + blend)

    if not corroboration:
        return specs

    if "sources_real" in have:
        specs.append(Col("sources_real", "Sources", "count",
                         "How many sources really had an opinion about this player, "
                         "after dropping the ones imputed from the mean. 1 means "
                         "the projection beside it is a single source's view. The "
                         "TOTAL row averages this rather than summing it."))
    if "source_spread" in have:
        specs.append(Col("source_spread", "Spread", "spread",
                         "Standard deviation across those real sources. Blank below "
                         "two, because one number cannot disagree with itself. The "
                         "TOTAL row composes these as sqrt of the sum of squares, "
                         "which is what independent disagreements add up to."))
    return specs


def info_columns(columns: Iterable[str]) -> List[Col]:
    """The identity block, dropping anything the frame does not carry.

    ``player_position`` falls back to ``primaryPosition``: the lineups artifact holds
    both, and a frame assembled from the optimiser's dicts may hold only one.

    Args:
        columns: The frame's column names.

    Returns:
        list: :class:`Col` specs.
    """
    have = set(columns)
    specs = []
    for spec in INFO_COLUMNS:
        if spec.source in have:
            specs.append(spec)
        elif spec.source == "player_position" and "primaryPosition" in have:
            specs.append(spec._replace(source="primaryPosition"))
    if STATE_COLUMN in have:
        # Beside the team, because it is a fact about that team's game rather than
        # about the player.
        specs.append(Col(STATE_COLUMN, "GM", "text", STATE_HELP))
    return specs


def value(row: Optional[dict], column: Col):
    """One cell's value, before formatting.

    Args:
        row: A lineups row, or None for a slot this side did not fill.
        column: Its spec.

    Returns:
        The string for a text column, a float for a numeric one, or None when the
        cell has nothing to say.
    """
    if row is None:
        return None
    if column.kind == "delta":
        blend, espn = row.get(f"{lu.BLEND}_Points"), row.get("ESPN_Points")
        if blend is None or espn is None:
            return None
        return float(blend) - float(espn)
    held = row.get(column.source)
    if column.source == STATE_COLUMN:
        # `pre` maps to an em dash rather than the word: it is the default state of
        # every row for most of the week, and a column reading "pre" ten times is
        # noise that pushes the numbers off a laptop screen.
        return STATE_LABELS.get(str(held), None) if held is not None else None
    if column.kind == "text":
        # Absent rather than the word for it -- see :data:`ABSENT_TEXT`.
        return None if held is None or str(held).strip() in ABSENT_TEXT else held
    return None if held is None else float(held)


def totals(rows: Sequence[dict], columns: Sequence[Col]) -> List[Optional[float]]:
    """The ``TOTAL`` row, one aggregate per column, aligned to ``columns``.

    Points and deltas are summed, which is what a lineup total means. The other two
    are not points and are **not** summed:

    * ``Sources`` is averaged. Ten starters on one source each would otherwise total
      ten, which reads as a well-corroborated lineup and is the opposite of the
      truth.
    * ``Spread`` is composed as ``sqrt(Σ spread²)``, the same way
      :func:`lineup.team_total_sd` composes per-player dispersion. Summing standard
      deviations assumes every source disagrees about every player in the same
      direction at once; adding variances assumes the disagreements are independent,
      which is the weaker and more defensible claim.

    Args:
        rows: The rows to total -- for a roster table, the ones that actually count.
        columns: From :func:`points_columns`.

    Returns:
        list: One value per column, None where nothing was measurable.
    """
    out: List[Optional[float]] = []
    for column in columns:
        if column.kind == "text":
            out.append(None)
            continue
        held = [v for v in (value(row, column) for row in rows) if v is not None]
        if not held:
            out.append(None)
        elif column.kind == "count":
            out.append(sum(held) / len(held))
        elif column.kind == "spread":
            out.append(math.sqrt(sum(v * v for v in held)))
        else:
            out.append(float(sum(held)))
    return out


class SlotRow(NamedTuple):
    """One row of a matchup table: the same slot on both sides.

    Attributes:
        slot: The starting slot.
        home: The home side's player there, or None.
        away: The away side's player there, or None.
        advantage: Home points minus away points. An unfilled side counts as zero,
            because an empty slot really does score nothing.
    """

    slot: str
    home: Optional[dict]
    away: Optional[dict]
    advantage: float


def pair_by_slot(home: Sequence[dict], away: Sequence[dict],
                 slots: Dict[str, int], *, slot_column: str = "slot",
                 points_column: str = "TRUE_Points") -> List[SlotRow]:
    """Line two lineups up slot for slot, best against best.

    **Row count per slot is a max, not a count from either side.** A manager who left
    his second receiver empty would otherwise shorten the table and pair his RB2
    against the opponent's WR2 -- every row below the gap misaligned. So each slot
    gets as many rows as the widest of: what the league defines, what the home side
    filled, and what the away side filled.

    Within a slot both sides are ordered by projection descending, so RB1 meets RB1.
    Any other pairing gives the same totals and the same margin; this one is the only
    one that reads as a matchup.

    Args:
        home: The home side's starters, each carrying ``slot_column``.
        away: The away side's starters.
        slots: From :func:`lineup.slot_counts`.
        slot_column: ``"slot"`` for an optimiser assignment, ``"slotPosition"`` for
            the lineup as ESPN has it set.
        points_column: Which projection the advantage is measured in.

    Returns:
        list: Rows in ESPN's slot order. A slot neither side filled produces no row.
    """
    def grouped(side: Sequence[dict]) -> Dict[str, List[dict]]:
        out: Dict[str, List[dict]] = {}
        for row in side:
            out.setdefault(row.get(slot_column) or "", []).append(row)
        for players in out.values():
            players.sort(key=lambda r: float(r.get(points_column) or 0.0),
                         reverse=True)
        return out

    left, right = grouped(home), grouped(away)
    names = set(slots) | set(left) | set(right)
    ordered = sorted((n for n in names if n),
                     key=lambda slot: (lu.slot_rank(slot), slot))

    rows: List[SlotRow] = []
    for slot in ordered:
        mine, theirs = left.get(slot, []), right.get(slot, [])
        depth = max(int(slots.get(slot, 0)), len(mine), len(theirs))
        for index in range(depth):
            one = mine[index] if index < len(mine) else None
            two = theirs[index] if index < len(theirs) else None
            if one is None and two is None:
                # The league defines the slot and neither manager used it. A blank
                # row on both sides is furniture, not information.
                continue
            rows.append(SlotRow(
                slot=slot, home=one, away=two,
                advantage=float((one or {}).get(points_column) or 0.0)
                          - float((two or {}).get(points_column) or 0.0)))
    return rows


def advantage_fill(points: Optional[float],
                   scale: float = ADVANTAGE_FULL_AT) -> str:
    """The advantage cell's background, as a CSS colour or ``""`` for none.

    Continuous rather than stepped, and composited through an alpha so one pair of
    colours works in both themes -- see :data:`ADVANTAGE_RGB`.

    Args:
        points: The advantage. None and zero both paint nothing; zero is the
            midpoint, and a midpoint has no colour to be.
        scale: Where the fill saturates.

    Returns:
        str: An ``rgba(...)``, or ``""``.
    """
    if not points or scale <= 0:
        return ""
    share = min(abs(points) / scale, 1.0)
    alpha = share * ADVANTAGE_MAX_ALPHA
    if alpha < ADVANTAGE_MIN_ALPHA:
        return ""
    red, green, blue = ADVANTAGE_RGB["positive" if points > 0 else "negative"]
    return f"rgba({red}, {green}, {blue}, {alpha:.2f})"


# --- markup ---------------------------------------------------------------

#: The table's stylesheet, emitted with every table.
#:
#: Re-sent each time rather than injected once, because a Streamlit rerun rebuilds
#: the DOM and a stylesheet written on an earlier run is gone. Duplicating it is
#: idempotent and costs a couple of hundred bytes; a table that loses its borders on
#: the second rerun is a bug nobody would think to look for here.
#:
#: **Every colour is a grey or a fill alpha, never a literal ink or paper colour.**
#: Text inherits Streamlit's own, borders and header tints are ``rgba`` greys that
#: composite correctly over either theme's background, so the table follows a theme
#: switch without being told which one is on.
CSS = """<style>
.lt-scroll { overflow-x: auto; padding-bottom: 3px; }
.lt { border-collapse: collapse; width: max-content; min-width: 100%;
      font-size: 0.78rem; font-variant-numeric: tabular-nums;
      white-space: nowrap; }
.lt th, .lt td { padding: 3px 5px; text-align: right;
                 border-bottom: 1px solid rgba(128, 128, 128, 0.16); }
.lt th.lt-l, .lt td.lt-l { text-align: left; }
.lt thead th { font-weight: 600; }
.lt tr.lt-team th { text-align: center; font-size: 0.95rem; padding: 5px 6px;
                    background: rgba(128, 128, 128, 0.17);
                    border-bottom: 1px solid rgba(128, 128, 128, 0.3); }
.lt tr.lt-group th { text-align: center; font-size: 0.66rem; font-weight: 600;
                     text-transform: uppercase; letter-spacing: 0.07em;
                     opacity: 0.68; background: rgba(128, 128, 128, 0.07); }
.lt tr.lt-head th { border-bottom: 2px solid rgba(128, 128, 128, 0.42); }
.lt th.lt-edge, .lt td.lt-edge { border-left: 1px solid rgba(128, 128, 128, 0.3); }
.lt th.lt-slot, .lt td.lt-slot { text-align: center; font-weight: 600;
                                 background: rgba(128, 128, 128, 0.1); }
.lt td.lt-true { font-weight: 600; }
.lt td.lt-mute { opacity: 0.42; }
.lt tbody tr:hover td { background-color: rgba(128, 128, 128, 0.08); }
.lt tbody tr.lt-in td { background-color: rgba(27, 175, 122, 0.16); }
.lt tbody tr.lt-out td { background-color: rgba(227, 73, 72, 0.16); }
.lt tbody tr.lt-in:hover td { background-color: rgba(27, 175, 122, 0.26); }
.lt tbody tr.lt-out:hover td { background-color: rgba(227, 73, 72, 0.26); }
.lt .lt-tag { font-size: 0.66rem; font-weight: 700; letter-spacing: 0.06em;
              margin-left: 6px; opacity: 0.75; }
.lt tr.lt-total td { font-weight: 700; border-bottom: none;
                     border-top: 2px solid rgba(128, 128, 128, 0.42); }
.lt tr.lt-total.lt-split td { border-bottom: 2px solid rgba(128, 128, 128, 0.42); }
.lt tbody tr.lt-total:hover td { background-color: transparent; }
.lt tbody tr.lt-total.lt-split:hover td.lt-slot,
.lt tbody tr.lt-total:hover td.lt-slot { background: rgba(128, 128, 128, 0.1); }
</style>"""

#: What a marked row means, and the class its cells carry.
#:
#: ``in`` is a player the optimiser starts who is currently benched; ``out`` is one
#: currently starting who it would bench. They are the symmetric difference of the
#: two lineups -- :func:`lineup.changed_ids` -- so a row is never both.
MARK_CLASSES: Dict[str, str] = {"in": "lt-in", "out": "lt-out"}

#: The word printed in a marked row, beside the fill.
#:
#: **Printed as well as coloured, on purpose.** It is the draft board's rule -- there
#: the sign is written out beside the fill -- and it holds for the same reason: a
#: table that can only be read by telling green from red cannot be read by everyone,
#: and cannot be read at all in a screenshot that has lost its colour.
MARK_LABELS: Dict[str, str] = {"in": "IN", "out": "OUT"}

#: Text the store uses where it has no answer, which must never be printed.
#:
#: ``pro_team`` is a string column and ESPN gives it no value for a player on no NFL
#: roster, so the artifact carries the literal ``"None"`` -- six rows of Big Red's
#: week 1, one of them a kicker on bye who is in somebody's starting lineup. A cell
#: reading ``None`` asserts an answer where there is an absence, which is the same
#: failure the draft board's ``na_rep`` exists to prevent.
ABSENT_TEXT = frozenset({"", "None", "none", "nan", "NaN", "NA", "null"})

#: Extra classes a points column's cells carry, by :attr:`Col.label`.
#:
#: Only the blend. It is the number every other column on the row is context for, and
#: at eight numeric columns per side something has to be the one your eye lands on.
#: Nothing here is *coloured*: the draft board painted its levels as well as its
#: differences once, and at that density the table read as a heatmap and the columns
#: carrying a judgement stopped being the ones that caught the eye.
EMPHASIS: Dict[str, str] = {"TRUE": "lt-true"}


def _classes(*names: str) -> str:
    """A ``class="..."`` attribute, or nothing when there are no classes."""
    kept = [name for name in names if name]
    return f' class="{" ".join(kept)}"' if kept else ""


def _fmt(held, column: Col, *, total: bool = False) -> str:
    """One cell's text: formatted, escaped, or empty when there is nothing to say."""
    if held is None or held == "":
        return ""
    if column.kind == "text":
        return html.escape(str(held))
    pattern = (TOTAL_FORMATS if total else FORMATS).get(column.kind, "{:.1f}")
    if column.kind in SIGNED_KINDS and round(float(held), 1) == 0:
        return "0.0"
    return pattern.format(held)


def _run(row: Optional[dict], columns: Sequence[Col], *, align_left: bool,
         edge_first: bool = False, mark: str = "") -> str:
    """One side's cells for one row.

    Args:
        row: The player, or None for a slot this side left empty.
        columns: The specs, already in the order they are drawn.
        align_left: Whether text columns read left-aligned. The away side is
            right-aligned so the mirrored halves read outward from the middle.
        edge_first: Draw a block border before the first cell.
        mark: ``"in"``, ``"out"`` or ``""``. A marked row prints the word beside the
            player's name -- see :data:`MARK_LABELS`.

    Returns:
        str: ``<td>`` elements.
    """
    cells = []
    for index, column in enumerate(columns):
        edge = "lt-edge" if edge_first and index == 0 else ""
        text = _fmt(value(row, column), column)
        if not text and column.kind == "text" and column.label == "Player":
            # The one absence worth drawing. A slot the opponent filled and this side
            # did not is a real fact about the matchup, and a row of blanks reads as
            # a rendering fault rather than as an empty slot.
            cells.append(f"<td{_classes(edge, 'lt-mute', 'lt-l' if align_left else '')}"
                         f">—</td>")
            continue
        if column.label == "Player" and mark in MARK_LABELS:
            text += f'<span class="lt-tag">{MARK_LABELS[mark]}</span>'
        left = "lt-l" if align_left and column.kind == "text" else ""
        cells.append(
            f"<td{_classes(edge, left, EMPHASIS.get(column.label, ''))}>{text}</td>")
    return "".join(cells)


def _total_run(values: Sequence[Optional[float]], columns: Sequence[Col], *,
               edge_first: bool = False) -> str:
    """One side's ``TOTAL`` cells, from :func:`totals`."""
    cells = []
    for index, (held, column) in enumerate(zip(values, columns)):
        edge = "lt-edge" if edge_first and index == 0 else ""
        cells.append(f"<td{_classes(edge)}>{_fmt(held, column, total=True)}</td>")
    return "".join(cells)


def _labels(columns: Sequence[Col], *, align_left: bool,
            edge_first: bool = False) -> str:
    """One block's header cells, tooltipped with the column's own prose."""
    cells = []
    for index, column in enumerate(columns):
        edge = "lt-edge" if edge_first and index == 0 else ""
        left = "lt-l" if align_left and column.kind == "text" else ""
        title = f' title="{html.escape(column.help)}"' if column.help else ""
        cells.append(f"<th{_classes(edge, left)}{title}>"
                     f"{html.escape(column.label)}</th>")
    return "".join(cells)


def _advantage_cell(points: Optional[float], scale: float, *,
                    edge: bool = False) -> str:
    """One ADV cell, filled on the continuous scale."""
    fill = advantage_fill(points, scale)
    style = f' style="background-color: {fill}"' if fill else ""
    if points is None:
        text = ""
    else:
        text = "0.0" if round(points, 1) == 0 else f"{points:+.1f}"
    return f"<td{_classes('lt-edge' if edge else '')}{style}>{text}</td>"


#: The tooltip on both ADV columns, phrased from the reading side's point of view.
ADVANTAGE_HELP = ("This side's points at this slot minus the other side's. Red is "
                  "where the matchup is being lost, green where it is being won; "
                  "the fill saturates at "
                  f"{ADVANTAGE_FULL_AT:.0f} points at one slot. The column sums to "
                  "the projected margin.")

#: The tooltip on the middle column.
SLOT_HELP = ("The starting slot both rows are filling — the slot, not the position, "
             "so a receiver in the flex reads FLEX. Rows are paired best against "
             "best within a slot.")


def matchup_html(rows: Sequence[SlotRow], info: Sequence[Col],
                 points: Sequence[Col], *, home_label: str, away_label: str,
                 total_label: str = "TOTAL") -> str:
    """The whole matchup: one table, two mirrored halves, a total.

    The away half draws the same columns in reverse -- points then identity, and each
    block reversed within itself -- so the two lineups meet at the ``SLOT`` column
    and each side's player name sits at its own outside edge.

    Args:
        rows: From :func:`pair_by_slot`.
        info: From :func:`info_columns`.
        points: From :func:`points_columns`.
        home_label: What to call the left side, on the header and on the total.
        away_label: The right side.
        total_label: What the middle column says on the total row.

    Returns:
        str: A complete ``<style>`` plus ``<table>``, for ``st.html``.
    """
    away_points, away_info = list(reversed(points)), list(reversed(info))
    span = len(info) + len(points)

    head = (
        f'<tr class="lt-team">'
        f'<th colspan="{span}">{html.escape(home_label)}</th>'
        f'<th colspan="3" rowspan="2" class="lt-edge lt-slot" '
        f'title="{html.escape(SLOT_HELP)}">Matchup</th>'
        f'<th colspan="{span}" class="lt-edge">{html.escape(away_label)}</th></tr>'
        f'<tr class="lt-group">'
        f'<th colspan="{len(info)}">Player Info</th>'
        f'<th colspan="{len(points)}" class="lt-edge">Player Points</th>'
        f'<th colspan="{len(points)}" class="lt-edge">Player Points</th>'
        f'<th colspan="{len(info)}">Player Info</th></tr>'
        f'<tr class="lt-head">'
        + _labels(info, align_left=True)
        + _labels(points, align_left=True, edge_first=True)
        + f'<th class="lt-edge" title="{html.escape(ADVANTAGE_HELP)}">ADV</th>'
        + f'<th class="lt-slot" title="{html.escape(SLOT_HELP)}">SLOT</th>'
        + f'<th title="{html.escape(ADVANTAGE_HELP)}">ADV</th>'
        + _labels(away_points, align_left=False, edge_first=True)
        + _labels(away_info, align_left=False, edge_first=True)
        + '</tr>'
    )

    body = []
    for row in rows:
        body.append(
            "<tr>"
            + _run(row.home, info, align_left=True)
            + _run(row.home, points, align_left=True, edge_first=True)
            + _advantage_cell(row.advantage, ADVANTAGE_FULL_AT, edge=True)
            + f'<td class="lt-slot">{html.escape(row.slot)}</td>'
            + _advantage_cell(-row.advantage, ADVANTAGE_FULL_AT)
            + _run(row.away, away_points, align_left=False, edge_first=True)
            + _run(row.away, away_info, align_left=False, edge_first=True)
            + "</tr>")

    home_rows = [row.home for row in rows if row.home is not None]
    away_rows = [row.away for row in rows if row.away is not None]
    home_totals, away_totals = totals(home_rows, points), totals(away_rows, points)
    # Summed off the rows rather than differenced off the two totals, so the ADV
    # column adds up to the number on its own last row exactly. The two agree
    # arithmetically; only one of them still agrees if a row is ever filtered out.
    margin = sum(row.advantage for row in rows)

    foot = (
        '<tr class="lt-total">'
        f'<td colspan="{len(info)}" class="lt-l">{html.escape(home_label)}</td>'
        + _total_run(home_totals, points, edge_first=True)
        + _advantage_cell(margin, MARGIN_FULL_AT, edge=True)
        + f'<td class="lt-slot">{html.escape(total_label)}</td>'
        + _advantage_cell(-margin, MARGIN_FULL_AT)
        + _total_run(list(reversed(away_totals)), away_points, edge_first=True)
        + f'<td colspan="{len(info)}" class="lt-edge">{html.escape(away_label)}</td>'
        + "</tr>"
    )

    return (f'{CSS}<div class="lt-scroll"><table class="lt">'
            f'<thead>{head}</thead><tbody>{"".join(body)}</tbody>'
            f'<tfoot>{foot}</tfoot></table></div>')


def side_html(rows: Sequence[dict], info: Sequence[Col], points: Sequence[Col], *,
              label: str, slot_column: str = "slot",
              total_rows: Optional[Sequence[dict]] = None,
              total_label: str = "TOTAL",
              marks: Optional[Mapping[object, str]] = None,
              below: Optional[Sequence[dict]] = None) -> str:
    """One lineup, with the matchup table's columns and its total.

    The mirroring and the advantage columns are what a matchup adds; the header
    structure, the formats and the merged total are shared, which is the point of
    drawing the Roster tab through here too.

    Args:
        rows: Already ordered -- see :func:`lineup.sort_by_slot`.
        info: From :func:`info_columns`.
        points: From :func:`points_columns`.
        label: The team, shown across the total row's identity columns.
        slot_column: Which column holds the slot.
        total_rows: The rows the total is over, when that is not all of them. A table
            that shows a player it is not counting -- the man being benched, on the
            Roster tab -- must not add him in, or the total stops being a lineup
            anybody can field.
        total_label: What the slot column says on the total row.
        marks: ``player_id`` to ``"in"`` or ``"out"``, from
            :func:`lineup.changed_ids`. Keyed by identity rather than by row position
            so it cannot slide out of step with ``rows`` -- a green fill on the wrong
            player is worse than no fill.
        below: Rows to draw **after** the total -- the bench, on the Roster tab.
            Below rather than above because they are not in the lineup the total
            describes, and the total is the boundary between the two: everything
            above it counts, everything under it does not.

    Returns:
        str: A complete ``<style>`` plus ``<table>``, for ``st.html``.
    """
    counted = list(rows if total_rows is None else total_rows)
    trailing = list(below or [])
    marks = marks or {}

    head = (
        f'<tr class="lt-group">'
        f'<th rowspan="2" class="lt-slot" title="{html.escape(SLOT_HELP)}">SLOT</th>'
        f'<th colspan="{len(info)}">Player Info</th>'
        f'<th colspan="{len(points)}" class="lt-edge">Player Points</th></tr>'
        f'<tr class="lt-head">'
        + _labels(info, align_left=True)
        + _labels(points, align_left=True, edge_first=True)
        + '</tr>'
    )

    def drawn(those: Sequence[dict]) -> str:
        out = []
        for row in those:
            slot = row.get(slot_column) or ""
            mark = marks.get(row.get("player_id"), "")
            out.append(
                f'<tr{_classes(MARK_CLASSES.get(mark, ""))}>'
                f'<td class="lt-slot">{html.escape(str(slot))}</td>'
                + _run(row, info, align_left=True, mark=mark)
                + _run(row, points, align_left=True, edge_first=True)
                + "</tr>")
        return "".join(out)

    # One ``<tbody>`` rather than a ``<tfoot>``, because there are rows *after* the
    # total. A ``<tfoot>`` is rendered last whatever order it is written in, which
    # would put the bench above the total it is excluded from -- the one arrangement
    # that makes the number look wrong.
    total = (
        f'<tr class="{"lt-total lt-split" if trailing else "lt-total"}">'
        f'<td class="lt-slot">{html.escape(total_label)}</td>'
        f'<td colspan="{len(info)}" class="lt-l">{html.escape(label)}</td>'
        + _total_run(totals(counted, points), points, edge_first=True)
        + "</tr>"
    )

    return (f'{CSS}<div class="lt-scroll"><table class="lt">'
            f'<thead>{head}</thead>'
            f'<tbody>{drawn(rows)}{total}{drawn(trailing)}</tbody>'
            f'</table></div>')
