"""The matchup table: which columns exist, how rows pair, what the total means.

Covers ``app/lineup_table.py``. No store and no browser -- the module is
Streamlit-free precisely so the pairing, the totals and the emitted markup can all be
checked here.

Four properties, which are what the module's docstrings claim:

* **The ADV column sums to the projected margin.** Both halves of the table and the
  headline metric on the page are then the same number by construction rather than by
  coincidence. ``test_the_advantage_column_sums_to_the_margin`` pins it.
* **A slot one manager left empty does not shift the rows below it.** Row count per
  slot is a max over both sides and the league's own definition, so RB2 always meets
  RB2. ``test_an_empty_slot_does_not_shift_the_rows_below_it`` is that bug's
  regression test.
* **Every row of the emitted table is exactly as wide as the header**, rowspans and
  colspans included. A merged cell that is one column out silently shears the whole
  away half sideways, which no amount of reading the numbers would catch.
* **The total row does not sum what is not points.** Ten starters on one source each
  must not total ten sources.
* **The Roster tab marks exactly the players who are a change**, and its total counts
  only the rows that are in the lineup -- the man being benched is drawn in the table
  and must not be added into a score somebody is meant to be able to field.
"""

import re
import sys
from html.parser import HTMLParser
from pathlib import Path

import pytest

# `app/` is not a package: Streamlit runs the page scripts directly with app/ on
# sys.path, so the modules import each other by bare name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import lineup_table as lt  # noqa: E402


# --- fixtures ------------------------------------------------------------

#: A frame's worth of column names, with every weekly source present.
COLUMNS = (
    "player_name", "player_position", "primaryPosition", "pro_team", "slotPosition",
    "slot", "ESPN_Points", "FP_Points", "PINNY_Points", "BOL_Points", "TRUE_Points",
    "sources_real", "source_spread",
)

#: Every source present, which is what 2026 stores say.
ALL_PRESENT = {"weekly_sources_present":
               {"fantasypros": True, "pinnacle": True, "betonline": True}}


def player(name, position, points, *, slot="BE", espn=None, sources=4, spread=0.5):
    """One lineups row carrying the columns this module reads.

    ``player_id`` is derived from the name rather than counted, so a row built in one
    test is the same row in another -- the in/out marks are keyed by it.
    """
    return {
        "player_id": abs(hash(name)) % 10**6,
        "player_name": name,
        "player_position": position,
        "pro_team": "NE",
        "slot": slot,
        "slotPosition": slot,
        "ESPN_Points": points if espn is None else espn,
        "FP_Points": points,
        "PINNY_Points": points,
        "BOL_Points": points,
        "TRUE_Points": points,
        "sources_real": sources,
        "source_spread": spread,
    }


class Grid(HTMLParser):
    """Row widths of a rendered table, rowspans carried forward.

    A colspan-only count cannot check the header: the ``Matchup`` cell spans three
    columns *and* two rows, so the group row underneath is legitimately narrower by
    three and only reaches full width once the carry is added.
    """

    def __init__(self):
        super().__init__()
        self.rows = []
        self.cells = None
        self.section = None

    def handle_starttag(self, tag, attrs):
        held = dict(attrs)
        if tag in ("thead", "tbody", "tfoot"):
            self.section = tag
        elif tag == "tr":
            self.cells = []
        elif tag in ("td", "th") and self.cells is not None:
            self.cells.append((int(held.get("colspan", 1)),
                               int(held.get("rowspan", 1))))

    def handle_endtag(self, tag):
        if tag == "tr" and self.cells is not None:
            self.rows.append((self.section, self.cells))
            self.cells = None

    def widths(self):
        """One width per row, rowspan carry included."""
        out, carry = [], 0
        for _, cells in self.rows:
            out.append(sum(span for span, _ in cells) + carry)
            carry = sum(span for span, rows in cells if rows > 1)
        return out


def parsed(markup):
    """A :class:`Grid` fed one table."""
    grid = Grid()
    grid.feed(markup)
    return grid


# --- which columns exist -------------------------------------------------

def test_every_source_present_gives_the_full_points_block():
    labels = [c.label for c in lt.points_columns(COLUMNS, ALL_PRESENT)]
    assert labels == ["ESPN", "FP", "PINNY", "BOL", "TRUE", "Δ", "Sources", "Spread"]


def test_an_absent_book_is_dropped_rather_than_shown_as_agreement():
    """The reason :func:`lineup.real_sources` exists, restated at the table.

    The blend imputes an absent source from the ESPN/FantasyPros mean, so a book with
    no weekly props arrives as a column that agrees with the mean exactly. Rendering
    it turns silence into unanimity.
    """
    meta = {"weekly_sources_present": {"fantasypros": True, "pinnacle": False,
                                       "betonline": False}}
    labels = [c.label for c in lt.points_columns(COLUMNS, meta)]
    assert labels == ["ESPN", "FP", "TRUE", "Δ", "Sources", "Spread"]


def test_a_frame_without_espn_gets_no_delta_column():
    """A delta against an absent baseline is the blend with a sign in front of it."""
    columns = [c for c in COLUMNS if c != "ESPN_Points"]
    labels = [c.label for c in lt.points_columns(columns, ALL_PRESENT)]
    assert "Δ" not in labels
    assert "TRUE" in labels


def test_the_identity_block_falls_back_to_the_primary_position():
    columns = [c for c in COLUMNS if c != "player_position"]
    specs = lt.info_columns(columns)
    assert [c.label for c in specs] == ["Player", "Pos", "TM"]
    assert specs[1].source == "primaryPosition"


def test_an_identity_column_the_frame_lacks_is_dropped():
    specs = lt.info_columns(["player_name", "player_position"])
    assert [c.label for c in specs] == ["Player", "Pos"]


# --- pairing -------------------------------------------------------------

def test_rows_pair_best_against_best_within_a_slot():
    home = [player("Home RB1", "RB", 20.0, slot="RB"),
            player("Home RB2", "RB", 8.0, slot="RB")]
    away = [player("Away RB2", "RB", 6.0, slot="RB"),
            player("Away RB1", "RB", 18.0, slot="RB")]
    rows = lt.pair_by_slot(home, away, {"RB": 2})
    assert [(r.home["player_name"], r.away["player_name"]) for r in rows] == [
        ("Home RB1", "Away RB1"), ("Home RB2", "Away RB2")]


def test_an_empty_slot_does_not_shift_the_rows_below_it():
    """The bug a per-side row count would reintroduce.

    A manager who left his second receiver empty would shorten his half of the table,
    and every row under the gap would then pair his flex against the opponent's WR2.
    """
    home = [player("Home WR1", "WR", 20.0, slot="WR"),
            player("Home FLEX", "WR", 9.0, slot="RB/WR/TE")]
    away = [player("Away WR1", "WR", 18.0, slot="WR"),
            player("Away WR2", "WR", 12.0, slot="WR"),
            player("Away FLEX", "WR", 10.0, slot="RB/WR/TE")]
    rows = lt.pair_by_slot(home, away, {"WR": 2, "RB/WR/TE": 1})

    assert [r.slot for r in rows] == ["WR", "WR", "RB/WR/TE"]
    assert rows[1].home is None
    assert rows[1].away["player_name"] == "Away WR2"
    assert rows[2].home["player_name"] == "Home FLEX"
    assert rows[2].away["player_name"] == "Away FLEX"


def test_a_slot_neither_side_filled_produces_no_row():
    """A blank row on both sides is furniture, not information."""
    home = [player("Home QB", "QB", 20.0, slot="QB")]
    away = [player("Away QB", "QB", 18.0, slot="QB")]
    rows = lt.pair_by_slot(home, away, {"QB": 1, "K": 1})
    assert [r.slot for r in rows] == ["QB"]


def test_an_unfilled_side_scores_zero_rather_than_nothing():
    """An empty slot really does score nothing, so the advantage is the full gap."""
    away = [player("Away TE", "TE", 11.0, slot="TE")]
    rows = lt.pair_by_slot([], away, {"TE": 1})
    assert rows[0].home is None
    assert rows[0].advantage == pytest.approx(-11.0)


def test_rows_come_out_in_espns_slot_order():
    home = [player("K", "K", 9.0, slot="K"),
            player("QB", "QB", 20.0, slot="QB"),
            player("FLEX", "WR", 12.0, slot="RB/WR/TE"),
            player("TE", "TE", 11.0, slot="TE")]
    rows = lt.pair_by_slot(home, [], {})
    assert [r.slot for r in rows] == ["QB", "TE", "RB/WR/TE", "K"]


def test_the_advantage_column_sums_to_the_margin():
    home = [player("A", "QB", 21.5, slot="QB"), player("B", "RB", 14.25, slot="RB")]
    away = [player("C", "QB", 18.0, slot="QB"), player("D", "RB", 16.75, slot="RB")]
    rows = lt.pair_by_slot(home, away, {"QB": 1, "RB": 1})
    total_home = sum(r["TRUE_Points"] for r in home)
    total_away = sum(r["TRUE_Points"] for r in away)
    assert sum(r.advantage for r in rows) == pytest.approx(total_home - total_away)


def test_the_lineup_as_set_can_be_paired_off_slot_position():
    """The Matchup tab pairs on ``slot``; a raw roster carries ``slotPosition``."""
    home = [player("Home QB", "QB", 20.0, slot="QB")]
    rows = lt.pair_by_slot(home, [], {"QB": 1}, slot_column="slotPosition")
    assert rows[0].slot == "QB"


# --- the total row -------------------------------------------------------

def test_points_and_deltas_are_summed():
    rows = [player("A", "QB", 20.0, espn=18.0), player("B", "RB", 10.0, espn=11.0)]
    points = lt.points_columns(COLUMNS, ALL_PRESENT)
    by_label = dict(zip([c.label for c in points], lt.totals(rows, points)))
    assert by_label["TRUE"] == pytest.approx(30.0)
    assert by_label["ESPN"] == pytest.approx(29.0)
    assert by_label["Δ"] == pytest.approx(1.0)


def test_sources_are_averaged_rather_than_summed():
    """Ten starters on one source each is not a well-corroborated lineup."""
    rows = [player(f"P{i}", "WR", 10.0, sources=1) for i in range(10)]
    points = lt.points_columns(COLUMNS, ALL_PRESENT)
    by_label = dict(zip([c.label for c in points], lt.totals(rows, points)))
    assert by_label["Sources"] == pytest.approx(1.0)


def test_spread_is_composed_as_independent_variances():
    """Summing standard deviations assumes every source is wrong the same way."""
    rows = [player("A", "QB", 20.0, spread=3.0), player("B", "RB", 10.0, spread=4.0)]
    points = lt.points_columns(COLUMNS, ALL_PRESENT)
    by_label = dict(zip([c.label for c in points], lt.totals(rows, points)))
    assert by_label["Spread"] == pytest.approx(5.0)


def test_a_column_nobody_has_a_number_for_totals_to_nothing():
    rows = [dict(player("A", "K", 9.0), source_spread=None),
            dict(player("B", "K", 8.0), source_spread=None)]
    points = lt.points_columns(COLUMNS, ALL_PRESENT)
    by_label = dict(zip([c.label for c in points], lt.totals(rows, points)))
    assert by_label["Spread"] is None


def test_a_missing_cell_does_not_drag_the_total_down():
    """A player no source priced contributes nothing, not a zero to average in."""
    rows = [player("A", "QB", 20.0, sources=4), dict(player("B", "RB", 10.0),
                                                     sources_real=None)]
    points = lt.points_columns(COLUMNS, ALL_PRESENT)
    by_label = dict(zip([c.label for c in points], lt.totals(rows, points)))
    assert by_label["Sources"] == pytest.approx(4.0)


# --- the fill ------------------------------------------------------------

def test_a_dead_even_slot_is_not_painted():
    """Zero is the midpoint, and a midpoint has no colour to be."""
    assert lt.advantage_fill(0.0) == ""
    assert lt.advantage_fill(None) == ""


def test_the_fill_saturates_rather_than_overflowing():
    full = lt.advantage_fill(lt.ADVANTAGE_FULL_AT)
    assert lt.advantage_fill(lt.ADVANTAGE_FULL_AT * 4) == full
    assert f"{lt.ADVANTAGE_MAX_ALPHA:.2f}" in full


def test_the_two_arms_are_the_two_colours():
    red, green = lt.ADVANTAGE_RGB["negative"], lt.ADVANTAGE_RGB["positive"]
    assert f"{green[0]}, {green[1]}, {green[2]}" in lt.advantage_fill(6.0)
    assert f"{red[0]}, {red[1]}, {red[2]}" in lt.advantage_fill(-6.0)


def test_a_fill_too_faint_to_read_is_not_drawn():
    assert lt.advantage_fill(0.1) == ""


def test_the_fill_grows_with_the_advantage():
    def alpha(points):
        return float(lt.advantage_fill(points).rsplit(", ", 1)[1].rstrip(")"))

    assert alpha(2.0) < alpha(5.0) < alpha(9.0)


# --- the markup ----------------------------------------------------------

def matchup():
    """One rendered matchup: two slots, one of them empty on the home side."""
    home = [player("Home QB", "QB", 20.0, slot="QB")]
    away = [player("Away QB", "QB", 18.0, slot="QB"),
            player("Away K", "K", 9.0, slot="K")]
    rows = lt.pair_by_slot(home, away, {"QB": 1, "K": 1})
    return lt.matchup_html(
        rows, lt.info_columns(COLUMNS), lt.points_columns(COLUMNS, ALL_PRESENT),
        home_label="Home Owner", away_label="Away Owner")


def test_every_row_of_the_matchup_is_as_wide_as_the_header():
    widths = parsed(matchup()).widths()
    assert len(set(widths)) == 1, widths
    # Three identity columns and eight points columns a side, plus ADV | SLOT | ADV.
    assert widths[0] == 3 + 8 + 3 + 8 + 3


def test_the_away_half_mirrors_the_home_half():
    points = lt.points_columns(COLUMNS, ALL_PRESENT)
    header = re.search(r'<tr class="lt-head">(.*?)</tr>', matchup(), re.S).group(1)
    labels = re.findall(r">([^<>]+)</th>", header)
    home_side = [c.label for c in lt.info_columns(COLUMNS)] + \
                [c.label for c in points]
    assert labels == home_side + ["ADV", "SLOT", "ADV"] + list(reversed(home_side))


def test_the_total_row_merges_the_identity_columns_behind_the_team_name():
    foot = total_row_of(matchup())
    assert 'colspan="3"' in foot
    assert "Home Owner" in foot and "Away Owner" in foot
    assert ">TOTAL<" in foot


def test_the_total_rows_advantage_is_the_margin_from_both_sides():
    foot = total_row_of(matchup())
    # -7.0 for the home side: +2.0 at QB, and an unfilled K against 9.0.
    assert ">-7.0<" in foot and ">+7.0<" in foot


def test_a_slot_this_side_left_empty_reads_as_empty_rather_than_as_a_fault():
    body = re.search(r"<tbody>(.*?)</tbody>", matchup(), re.S).group(1)
    assert "lt-mute" in body and "—" in body


def test_a_players_name_is_escaped():
    """Names on a board are full of punctuation; one of them will eventually
    contain an ampersand, and it must not become markup."""
    rows = lt.pair_by_slot([player("A&W <script>", "QB", 20.0, slot="QB")], [],
                           {"QB": 1})
    markup = lt.matchup_html(rows, lt.info_columns(COLUMNS),
                             lt.points_columns(COLUMNS, ALL_PRESENT),
                             home_label="Home", away_label="Away")
    assert "A&amp;W &lt;script&gt;" in markup
    assert "<script>" not in markup


def test_a_delta_that_rounds_to_zero_claims_no_direction():
    """``{:+.1f}`` renders -0.04 as ``-0.0``, which on the delta column reads as
    being quietly below ESPN on a player we agree with exactly."""
    rows = lt.pair_by_slot([player("A", "QB", 20.0, espn=20.04, slot="QB")], [],
                           {"QB": 1})
    markup = lt.matchup_html(rows, lt.info_columns(COLUMNS),
                             lt.points_columns(COLUMNS, ALL_PRESENT),
                             home_label="Home", away_label="Away")
    assert ">-0.0<" not in markup
    assert ">0.0<" in markup


# --- the roster table ----------------------------------------------------

def roster(total_rows=None):
    """One rendered single-side table: a starter and a bench player."""
    rows = [player("Starter", "QB", 20.0, slot="QB"),
            player("Benched", "RB", 30.0, slot="BE")]
    return rows, lt.side_html(
        rows, lt.info_columns(COLUMNS), lt.points_columns(COLUMNS, ALL_PRESENT),
        label="Owner", total_rows=total_rows)


def test_every_row_of_a_roster_table_is_as_wide_as_the_header():
    widths = parsed(roster()[1]).widths()
    assert len(set(widths)) == 1, widths
    assert widths[0] == 1 + 3 + 8


def test_a_whole_roster_totals_only_the_rows_that_count():
    """A bench player's points do not count, so adding them in would report a
    score nobody can field."""
    rows, markup = roster(total_rows=None)
    assert ">50.0<" in markup

    _, starters_only = roster(total_rows=[rows[0]])
    assert ">20.0<" in starters_only
    assert ">50.0<" not in starters_only


def test_the_roster_total_row_carries_the_team_name():
    assert 'colspan="3"' in roster()[1]
    assert "Owner" in roster()[1]


def test_a_table_ships_its_own_stylesheet():
    """A Streamlit rerun rebuilds the DOM, so a stylesheet written on an earlier
    run is gone by the time the table is drawn again."""
    for markup in (matchup(), roster()[1]):
        assert markup.startswith("<style>")
        assert markup.count("<style>") == 1


# --- column order and the corroboration pair -----------------------------

def test_the_blend_can_lead_the_points_block():
    """The Roster tab reads this way: it is a decision table, and the number the
    lineup is chosen on belongs beside the player rather than downstream of four
    sources he is being compared against."""
    labels = [c.label for c in
              lt.points_columns(COLUMNS, ALL_PRESENT, blend_first=True)]
    assert labels == ["TRUE", "Δ", "ESPN", "FP", "PINNY", "BOL", "Sources", "Spread"]


def test_the_delta_stays_with_the_blend_when_it_leads():
    """`Δ` is TRUE − ESPN, so it reads as a property of the blend, not of ESPN."""
    labels = [c.label for c in
              lt.points_columns(COLUMNS, ALL_PRESENT, blend_first=True)]
    assert labels.index("Δ") == labels.index("TRUE") + 1


def test_the_corroboration_pair_can_be_dropped():
    """Off for the matchup: how well corroborated your receiver is says nothing
    about whether he beats theirs."""
    labels = [c.label for c in
              lt.points_columns(COLUMNS, ALL_PRESENT, corroboration=False)]
    assert labels == ["ESPN", "FP", "PINNY", "BOL", "TRUE", "Δ"]
    assert "Sources" not in labels and "Spread" not in labels


def test_dropping_the_pair_narrows_the_matchup_by_four_columns():
    """Two columns a side, and the table was one scrollbar wide before."""
    home = [player("Home QB", "QB", 20.0, slot="QB")]
    away = [player("Away QB", "QB", 18.0, slot="QB")]
    rows = lt.pair_by_slot(home, away, {"QB": 1})
    info = lt.info_columns(COLUMNS)
    wide = parsed(lt.matchup_html(
        rows, info, lt.points_columns(COLUMNS, ALL_PRESENT),
        home_label="H", away_label="A")).widths()[0]
    narrow = parsed(lt.matchup_html(
        rows, info, lt.points_columns(COLUMNS, ALL_PRESENT, corroboration=False),
        home_label="H", away_label="A")).widths()[0]
    assert wide == 25 and narrow == 21


def test_the_two_orders_hold_the_same_columns():
    """A reorder, not a different table -- otherwise a number would mean one thing
    on Roster and another on Matchup."""
    default = {c.label for c in lt.points_columns(COLUMNS, ALL_PRESENT)}
    led = {c.label for c in
           lt.points_columns(COLUMNS, ALL_PRESENT, blend_first=True)}
    assert default == led


# --- absent text ---------------------------------------------------------

def test_the_word_none_is_an_absence_rather_than_a_value():
    """``pro_team`` is a string column and ESPN leaves it unset for a player on no
    NFL roster, so the artifact carries the literal ``"None"``."""
    team = lt.INFO_COLUMNS[2]
    assert team.source == "pro_team"
    assert lt.value({"pro_team": "None"}, team) is None
    assert lt.value({"pro_team": "NE"}, team) == "NE"


def test_a_no_team_player_renders_a_blank_not_the_word():
    rows = [dict(player("Matt Prater", "K", 0.0, slot="K"), pro_team="None")]
    markup = lt.side_html(rows, lt.info_columns(COLUMNS),
                          lt.points_columns(COLUMNS, ALL_PRESENT), label="Owner")
    assert ">None<" not in markup
    assert "Matt Prater" in markup


# --- the in/out marks ----------------------------------------------------

def marked():
    """A lineup with one arrival, one departure, and one untouched starter."""
    rows = [player("Untouched", "QB", 20.0, slot="QB"),
            player("Arriving", "WR", 14.0, slot="WR"),
            player("Leaving", "WR", 9.0, slot="WR")]
    marks = {rows[1]["player_id"]: "in", rows[2]["player_id"]: "out"}
    return rows, marks, lt.side_html(
        rows, lt.info_columns(COLUMNS),
        lt.points_columns(COLUMNS, ALL_PRESENT, blend_first=True),
        label="Owner", total_rows=rows[:2], marks=marks)


def test_the_two_marks_paint_their_rows():
    _, _, markup = marked()
    assert markup.count('<tr class="lt-in">') == 1
    assert markup.count('<tr class="lt-out">') == 1


def test_a_mark_is_printed_as_well_as_painted():
    """The draft board's rule: a table that can only be read by telling green from
    red cannot be read by everyone, and cannot be read in a colourless screenshot."""
    _, _, markup = marked()
    assert ">IN</span>" in markup
    assert ">OUT</span>" in markup


def test_only_the_player_column_carries_the_mark():
    """The tag belongs on the name, not repeated down eight numeric cells."""
    _, _, markup = marked()
    assert markup.count("lt-tag") == 3  # the CSS rule, plus one per marked row


def rows_of(markup, totals=False):
    """The ``<tr ...>`` fragments of a rendered table's body, in order.

    The total row is skipped unless asked for: a single-side table keeps it in the
    body rather than a ``<tfoot>``, because a ``<tfoot>`` renders last however it is
    written and the bench has to come after it.
    """
    body = re.search(r"<tbody>(.*?)</tbody>", markup, re.S).group(1)
    found = re.findall(r"<tr.*?</tr>", body, re.S)
    return found if totals else [row for row in found if "lt-total" not in row]


def total_row_of(markup):
    """The one total row of a rendered table, from wherever it lives."""
    row, = [r for r in re.findall(r"<tr.*?</tr>", markup, re.S)
            if "lt-total" in r]
    return row


def test_an_unmarked_row_is_left_alone():
    _, _, markup = marked()
    untouched, = [row for row in rows_of(markup) if "Untouched" in row]
    assert "lt-in" not in untouched
    assert "lt-out" not in untouched
    assert "lt-tag" not in untouched


def test_the_outgoing_player_is_shown_but_not_counted():
    """The whole reason ``total_rows`` exists on this table. Adding the man being
    benched into the total would report a score nobody can field -- and it would
    stop matching the Best Available metric beside it."""
    rows, _, markup = marked()
    foot = total_row_of(markup)
    assert ">34.0<" in foot          # 20.0 + 14.0, the lineup
    assert ">43.0<" not in foot      # not the benched receiver as well
    assert "Leaving" in markup       # he is still on screen


def test_marks_are_keyed_by_player_rather_than_by_row():
    """A parallel list could slide out of step with the rows and paint the wrong
    player green, which is worse than painting nobody."""
    rows = [player("A", "QB", 20.0, slot="QB"), player("B", "WR", 10.0, slot="WR")]
    markup = lt.side_html(
        rows, lt.info_columns(COLUMNS), lt.points_columns(COLUMNS, ALL_PRESENT),
        label="Owner", marks={rows[1]["player_id"]: "in"})
    first, second = rows_of(markup)
    assert "lt-in" not in first
    assert "lt-in" in second


def test_an_unknown_mark_is_ignored_rather_than_raising():
    rows = [player("A", "QB", 20.0, slot="QB")]
    markup = lt.side_html(rows, lt.info_columns(COLUMNS),
                          lt.points_columns(COLUMNS, ALL_PRESENT),
                          label="Owner", marks={rows[0]["player_id"]: "sideways"})
    only, = rows_of(markup)
    assert only.startswith("<tr>")
    assert "lt-tag" not in only


# --- the bench, under the total ------------------------------------------

def benched():
    """A two-man lineup with two players left on the bench under it."""
    lineup = [player("Starter A", "QB", 20.0, slot="QB"),
              player("Starter B", "WR", 14.0, slot="WR")]
    bench = [player("Bench A", "WR", 9.0, slot="BE"),
             player("Bench B", "RB", 30.0, slot="BE")]
    return lineup, bench, lt.side_html(
        lineup, lt.info_columns(COLUMNS), lt.points_columns(COLUMNS, ALL_PRESENT),
        label="Owner", below=bench)


def test_the_bench_is_drawn_after_the_total():
    """A ``<tfoot>`` renders last whatever order it is written in, which would put
    the bench above the total it is excluded from -- the one arrangement that makes
    the number look wrong."""
    _, _, markup = benched()
    body = re.search(r"<tbody>(.*?)</tbody>", markup, re.S).group(1)
    assert body.index("Starter A") < body.index("TOTAL") < body.index("Bench A")
    assert "<tfoot>" not in markup


def test_the_bench_is_not_added_into_the_total():
    """The bench is shown, not counted. 34.0 is the lineup; 30.0 of bench receiver
    is on screen and must stay out of it."""
    _, _, markup = benched()
    foot = total_row_of(markup)
    assert ">34.0<" in foot
    assert ">64.0<" not in foot
    assert "Bench B" in markup


def test_the_total_row_splits_the_table_only_when_something_is_below_it():
    _, bench, with_bench = benched()
    assert "lt-split" in total_row_of(with_bench)

    without = lt.side_html(
        [player("Only", "QB", 20.0, slot="QB")], lt.info_columns(COLUMNS),
        lt.points_columns(COLUMNS, ALL_PRESENT), label="Owner")
    assert "lt-split" not in total_row_of(without)


def test_a_bench_row_is_an_ordinary_row():
    """No mark and no fill -- a player staying on the bench is not a change."""
    _, _, markup = benched()
    bench_row, = [row for row in rows_of(markup) if "Bench A" in row]
    assert bench_row.startswith("<tr>")
    assert "lt-tag" not in bench_row


def test_a_table_with_no_bench_is_unchanged():
    lineup = [player("Only", "QB", 20.0, slot="QB")]
    args = (lt.info_columns(COLUMNS), lt.points_columns(COLUMNS, ALL_PRESENT))
    assert (lt.side_html(lineup, *args, label="Owner", below=[])
            == lt.side_html(lineup, *args, label="Owner"))
