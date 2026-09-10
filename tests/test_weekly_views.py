"""The Free Agents grid: which columns it shows, and which two numbers it emphasises.

Streamlit-free. The grid is a different renderer from the Roster and Matchup tables --
those are hand-emitted HTML, this is a canvas-drawn Streamlit dataframe -- and the
consequence is the point of this file: **no stylesheet reaches a canvas cell**, so the
emphasis has to travel as a pandas ``Styler`` and the two renderers cannot share the
mechanism. They can and must share the *decision*, so the first test here holds them
in step.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import lineup_table as ltab  # noqa: E402
from views import weekly as wv  # noqa: E402

ALL_PRESENT = {"weekly_sources_present":
               {"fantasypros": True, "pinnacle": True, "betonline": True}}


def pool(**overrides):
    """A free-agent frame carrying the columns the grid reads."""
    base = {
        "player_name": ["A", "B"], "player_position": ["QB", "RB"],
        "pro_team": ["SEA", "NE"], "game_state": ["post", "pre"],
        "game_locked": [True, False],
        "LIVE_Points": [22.0, 14.0], "TRUE_Points": [18.0, 14.0],
        "ESPN_Points": [17.0, 13.0], "FP_Points": [19.0, 15.0],
        "points": [22.0, 0.0], "sources_real": [4, 2], "source_spread": [1.5, 0.5],
    }
    base.update(overrides)
    return pl.DataFrame(base)


# --- the two renderers must agree about what the table is about ----------

def test_the_grid_emphasises_the_same_columns_the_html_tables_do():
    """A reader moving between the Roster table and this grid should not have to work
    out which number each one is built around."""
    html_bold = {label for label, css in ltab.EMPHASIS.items() if css == "lt-em"}
    html_italic = {label for label, css in ltab.EMPHASIS.items() if css == "lt-delta"}
    # The grid renames as it renders: TRUE_Points reads "Us" and LIVE_Points "Live".
    assert html_bold == {"LIVE", "TRUE"}
    assert set(wv.BOLD_LABELS) == {"Live", "Us"}
    assert html_italic == set(wv.ITALIC_LABELS)


# --- which columns are shown ---------------------------------------------

def test_the_game_state_column_is_gone():
    assert "game_state" not in wv.display_columns(pool(), ALL_PRESENT)


def test_spread_is_gone():
    """Same call as the lineup tables: at this width the disagreement *between*
    sources was competing with the number the grid exists for."""
    assert "source_spread" not in wv.display_columns(pool(), ALL_PRESENT)


def test_sources_stayed():
    assert "sources_real" in wv.display_columns(pool(), ALL_PRESENT)


def test_the_live_column_leads_the_numbers():
    columns = wv.display_columns(pool(), ALL_PRESENT)
    assert columns.index("LIVE_Points") < columns.index("ESPN_Points")
    assert columns.index("LIVE_Points") < columns.index("TRUE_Points")


def test_actual_is_shown_once_any_game_has_started():
    assert "points" in wv.display_columns(pool(), ALL_PRESENT)


def test_actual_is_hidden_before_the_first_kickoff():
    """A column of zeros beside a column of projections invites the wrong reading."""
    frame = pool(game_state=["pre", "pre"], game_locked=[False, False],
                 points=[0.0, 0.0])
    assert "points" not in wv.display_columns(frame, ALL_PRESENT)


def test_a_store_without_game_state_falls_back_to_the_old_rule():
    """The sum-of-points proxy, which is what a 2025 store has to be read with."""
    frame = pool().drop(["game_state", "game_locked"])
    assert "points" in wv.display_columns(frame, ALL_PRESENT)
    quiet = frame.with_columns(pl.lit(0.0).alias("points"))
    assert "points" not in wv.display_columns(quiet, ALL_PRESENT)


# --- the emphasis itself -------------------------------------------------

def styled(frame, scales=None):
    """The frame through the grid's renaming, then the styler."""
    labels = {**wv.BASE_LABELS, **wv.SOURCE_LABELS}
    columns = wv.display_columns(frame, ALL_PRESENT)
    renamed = frame.select(columns).rename(
        {c: labels[c] for c in columns if c in labels})
    return wv._emphasised(renamed, wv._fills(frame, scales))


def css_for(frame, scales=None):
    """The CSS Streamlit will send to the frontend for this frame."""
    from streamlit.elements.lib.pandas_styler_utils import marshall_styler
    from streamlit.proto.ArrowData_pb2 import ArrowData

    proto = ArrowData()
    marshall_styler(proto, styled(frame, scales), "uuid")
    return proto.styler.styles


def test_the_styler_actually_emits_the_css():
    """Not a unit test of intent -- the real check, because a Styler that Streamlit
    silently drops would look identical from here."""
    css = css_for(pool())
    assert "font-weight: 700" in css


def test_a_frame_with_a_delta_gets_italics():
    """The grid does not currently show a delta -- ``display_columns`` emits none --
    so this goes at :func:`views.weekly._emphasised` directly. The rule is here so
    that adding the column later needs no styling work, and so that it cannot land
    formatted differently from the same column on the Roster table."""
    from streamlit.elements.lib.pandas_styler_utils import marshall_styler
    from streamlit.proto.ArrowData_pb2 import ArrowData

    frame = pl.DataFrame({"Player": ["A"], "Live": [22.0], "Us": [18.0],
                          "Δ": [4.0]})
    proto = ArrowData()
    marshall_styler(proto, wv._emphasised(frame), "uuid")
    assert "font-style: italic" in proto.styler.styles
    assert "font-weight: 700" in proto.styler.styles


def test_a_delta_carries_its_sign():
    values = wv._emphasised(
        pl.DataFrame({"Δ": [4.0, -1.5]}))._compute()._translate(True, True)
    cells = [str(c["display_value"]) for row in values["body"] for c in row]
    assert "+4.0" in cells and "-1.5" in cells


def test_the_numbers_are_formatted_rather_than_left_as_reprs():
    """A ``Styler`` sends its own display values, so the formats have to be applied
    here too -- left to pandas they would arrive as ``12.339999999999999`` beside a
    column claiming one decimal place."""
    frame = pool(LIVE_Points=[12.34, 5.0])
    values = styled(frame)._compute()._translate(True, True)
    cells = [c["display_value"] for row in values["body"] for c in row
             if c.get("display_value") not in (None, "")]
    assert "12.3" in cells
    assert not any("12.34" == str(c) for c in cells)


def test_a_null_renders_blank_not_the_word_nan():
    frame = pool(source_spread=[None, None], sources_real=[None, 2])
    values = styled(frame)._compute()._translate(True, True)
    cells = [str(c["display_value"]) for row in values["body"] for c in row]
    assert "nan" not in cells and "None" not in cells


def test_sources_is_a_whole_number():
    values = styled(pool())._compute()._translate(True, True)
    cells = [str(c["display_value"]) for row in values["body"] for c in row]
    assert "4" in cells and "4.0" not in cells


def test_a_styler_failure_falls_back_to_the_frame_rather_than_the_page():
    """Emphasis is cosmetic; this is the last thing between the data and the screen.
    An unstyled table is a loss, a raised exception takes the page."""
    class _Explodes:
        columns = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))

        def to_pandas(self):
            raise RuntimeError("boom")

    sentinel = _Explodes()
    assert wv._emphasised(sentinel) is sentinel


# --- the positional fill, on the grid ------------------------------------
#
# The mechanism differs from the HTML tables' -- inline ``style`` there, a pandas
# ``Styler`` here -- so it has to be proved separately. The *decision* is shared, and
# ``lineup_table`` owns it; these tests are about whether it survives the canvas.

def rulers(frame):
    """The rulers for this frame, as a route computes them."""
    return ltab.points_scales(
        frame.select([c for c in ltab.SCALE_INPUTS if c in frame.columns]).to_dicts(),
        frame.columns)


def wide_pool():
    """A pool with two positions and enough spread at each to build a ruler."""
    return pl.DataFrame({
        "player_name": ["Q1", "Q2", "Q3", "R1", "R2", "R3"],
        "player_position": ["QB", "QB", "QB", "RB", "RB", "RB"],
        "primaryPosition": ["QB", "QB", "QB", "RB", "RB", "RB"],
        "team_owner": ["A", "B", "C", "A", "B", "C"],
        "slotPosition": ["QB", "QB", "QB", "RB", "RB", "RB"],
        "pro_team": ["SEA"] * 6,
        "game_state": ["pre"] * 6,
        "game_locked": [False] * 6,
        "LIVE_Points": [14.0, 18.0, 26.0, 6.0, 12.0, 19.0],
        "TRUE_Points": [14.0, 18.0, 26.0, 6.0, 12.0, 19.0],
        "ESPN_Points": [13.0, 17.0, 25.0, 5.0, 11.0, 18.0],
        "FP_Points": [15.0, 19.0, 27.0, 7.0, 13.0, 20.0],
        "points": [0.0] * 6,
        "sources_real": [4] * 6,
        "source_spread": [1.0] * 6,
    })


def test_the_grid_really_ships_the_background_colour():
    """The check that matters. ``background-color`` is documented as one of the three
    properties Streamlit's grid honours, but a ``Styler`` property it silently drops
    would look identical from Python -- which is the whole reason this file marshals
    the proto instead of asserting on intent."""
    frame = wide_pool()
    css = css_for(frame, rulers(frame))
    assert "background-color" in css
    assert "rgba(" in css


def test_an_unscaled_grid_ships_no_fill_but_keeps_its_emphasis():
    """The pre-live fallback, and the failure mode worth being sure about: losing the
    colour must not cost the bold."""
    css = css_for(wide_pool(), None)
    assert "background-color" not in css
    assert "font-weight: 700" in css


def test_the_grid_paints_the_same_two_columns_the_html_tables_do():
    """Three names for two columns -- ``TRUE_Points`` is ``TRUE`` on a lineup table
    and ``Us`` here -- so the middle name in :data:`views.weekly.PAINTED` is what
    holds them in step."""
    assert {label for _, label, _ in wv.PAINTED} == set(ltab.PAINTED_LABELS)
    assert {shown for _, _, shown in wv.PAINTED} == set(wv.BOLD_LABELS)


def test_the_fills_are_one_dict_per_row_in_the_frames_own_order():
    """Row-wise rather than per-cell, because a row's colour depends on the player's
    own position -- and a fill list that slid out of step with the frame would colour
    the wrong player, which is worse than colouring nobody."""
    frame = wide_pool()
    fills = wv._fills(frame, rulers(frame))
    assert len(fills) == frame.height
    # Q3 is the best quarterback and R1 the worst back: opposite arms.
    blue, red = ltab.POINTS_RGB["positive"], ltab.POINTS_RGB["negative"]
    assert f"{blue[0]}, {blue[1]}, {blue[2]}" in fills[2]["Live"]
    assert f"{red[0]}, {red[1]}, {red[2]}" in fills[3]["Live"]


def test_the_same_number_at_two_positions_gets_two_colours():
    """The reason the fill is computed off the unrenamed frame: the rule reads the
    player's own position, which the ruler is keyed on."""
    frame = wide_pool().with_columns(
        pl.Series("LIVE_Points", [19.0, 18.0, 26.0, 19.0, 12.0, 6.0]),
        pl.Series("TRUE_Points", [19.0, 18.0, 26.0, 19.0, 12.0, 6.0]))
    fills = wv._fills(frame, rulers(frame))
    assert fills[0].get("Live") != fills[3].get("Live")


def test_a_frame_with_no_position_column_paints_nothing():
    """Rather than raising. ``display_columns`` always leads with the position, but
    the fill reads it off the frame and a caller could hand over a narrower one --
    and with nothing to group on there is no ruler a cell could be read against."""
    frame = wide_pool().drop("primaryPosition", "player_position")
    fills = wv._fills(frame, {"QB": ltab.PointsScale(mid=18.0, high=26.0)})
    assert not any(fills)
    assert "background-color" not in css_for(
        frame, {"QB": ltab.PointsScale(mid=18.0, high=26.0)})


def test_a_styler_failure_still_falls_back_to_the_frame():
    """Same guarantee as before the fill existed, re-checked because ``_emphasised``
    now does more work: a fill list of the wrong length must not take the page."""
    frame = wide_pool()
    out = wv._emphasised(frame.select("player_name"), [{"nope": "x"}] * 99)
    assert out is not None
