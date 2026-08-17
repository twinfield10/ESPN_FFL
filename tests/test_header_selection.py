"""The sidebar's sticky selectors, which have broken in two opposite ways.

`_sticky_selectbox` has to satisfy two requirements that pull against each other,
and the obvious implementation of either one breaks the other. Both failures were
silent -- the app rendered a real league's real board, just not the one you asked
for -- so both are pinned here.

1. **A selection must survive being made twice.** The first version passed no
   ``key=`` and steered the widget with ``index=``. A keyless widget's identity is
   derived from its arguments, ``index`` among them, so a successful switch changed
   the index, minted a new widget id, and orphaned the *next* selection. Winfield →
   GOP worked; GOP → Knights did not.

2. **A selection must survive page navigation.** Streamlit discards a widget's
   state when you open a page that has not rendered it. Writing the key only when
   the remembered value was invalid therefore fixed nothing: on navigation the value
   is perfectly valid, nothing touches the key, and the widget falls back to its
   first option. Opening the Draft Board from the Store Overview moved you from
   Winfield_Football to GOP_Degenerates.

The fix for (1) is the widget owning its key; the fix for (2) is writing that key on
every run whether or not it needed correcting. These tests stub Streamlit rather
than drive a real app, because the repo's suite must run with no store on disk --
`Data/` is untracked, so a fresh clone has nothing to point `AppTest` at.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from components import header  # noqa: E402


class FakeStreamlit:
    """Enough of ``st`` for ``_sticky_selectbox``: session state and a selectbox.

    ``selectbox`` mimics the real contract for a keyed widget -- it reads its value
    out of session state and writes it back -- which is what makes the "did the key
    get written" question testable at all.
    """

    def __init__(self, session_state=None):
        self.session_state = dict(session_state or {})
        self.writes = []

    def selectbox(self, label, options, key=None, format_func=str, **kwargs):
        self.writes.append(key)
        if key is not None and key in self.session_state:
            return self.session_state[key]
        index = kwargs.get("index", 0)
        chosen = options[index]
        if key is not None:
            self.session_state[key] = chosen
        return chosen


@pytest.fixture
def fake_st(monkeypatch):
    fake = FakeStreamlit()
    monkeypatch.setattr(header, "st", fake)
    return fake


LEAGUES = ["gop_degenerates", "knights_ffl", "weenieless_wanderers",
           "winfield_football"]


# --- requirement 2: the selection survives navigation ---------------------

def test_the_key_is_written_even_when_it_did_not_need_correcting(fake_st):
    """The whole of the navigation fix. Streamlit drops a widget's state when you
    open a page that has not rendered it, and touching the key is what carries the
    selection over -- so a valid remembered value must still be re-written."""
    fake_st.session_state["league_key"] = "winfield_football"
    header._sticky_selectbox("League", "league_key", LEAGUES,
                             default="winfield_football")
    assert fake_st.session_state["league_key"] == "winfield_football"


def test_a_remembered_league_is_not_replaced_by_the_first_option(fake_st):
    """The bug as it was seen: Store Overview → Draft Board landed on GOP, because
    GOP sorts first and nothing had re-asserted the remembered value."""
    fake_st.session_state["league_key"] = "winfield_football"
    chosen = header._sticky_selectbox("League", "league_key", LEAGUES,
                                      default="winfield_football")
    assert chosen == "winfield_football" != LEAGUES[0]


# --- requirement 1: the widget owns its key -------------------------------

def test_the_widget_is_keyed_rather_than_steered_by_index(fake_st):
    """A keyless widget's identity changes with `index`, which is what ate every
    second league change."""
    header._sticky_selectbox("League", "league_key", LEAGUES)
    assert fake_st.writes == ["league_key"]


# --- the dependent-selector case the original version existed for ---------

def test_a_remembered_value_no_longer_offered_falls_back_to_the_default(fake_st):
    """Last week's week 14 after switching to a league that only has week 1."""
    fake_st.session_state["week"] = 14
    chosen = header._sticky_selectbox("Week", "week", [1, 2, 3], default=2)
    assert chosen == 2 and fake_st.session_state["week"] == 2


def test_a_default_that_is_also_unavailable_falls_back_to_the_first_option(fake_st):
    fake_st.session_state["league_key"] = "john_pc_league"
    chosen = header._sticky_selectbox("League", "league_key", LEAGUES,
                                      default="also_missing")
    assert chosen == LEAGUES[0]


def test_nothing_remembered_takes_the_default_not_the_first_option(fake_st):
    """The app must open on Winfield_Football, which sorts last of the four."""
    chosen = header._sticky_selectbox("League", "league_key", LEAGUES,
                                      default="winfield_football")
    assert chosen == "winfield_football"


def test_no_default_given_takes_the_first_option(fake_st):
    assert header._sticky_selectbox("Season", "season", [2026, 2025]) == 2026
