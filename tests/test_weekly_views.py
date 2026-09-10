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

def styled(frame):
    """The frame through the grid's renaming, then the styler."""
    labels = {**wv.BASE_LABELS, **wv.SOURCE_LABELS}
    columns = wv.display_columns(frame, ALL_PRESENT)
    renamed = frame.select(columns).rename(
        {c: labels[c] for c in columns if c in labels})
    return wv._emphasised(renamed)


def css_for(frame):
    """The CSS Streamlit will send to the frontend for this frame."""
    from streamlit.elements.lib.pandas_styler_utils import marshall_styler
    from streamlit.proto.ArrowData_pb2 import ArrowData

    proto = ArrowData()
    marshall_styler(proto, styled(frame), "uuid")
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
