"""The sidebar's Owner selector: switching viewer without restarting the app.

Local convenience, and deliberately nothing more. `ESPN_FFL_VIEWER` already picked
the viewer at launch, which meant seeing what another owner sees cost a restart --
fine once, tedious while actually comparing two leagues side by side. The dropdown
is the same choice made at render time.

It is **on by default** and switched off with `ESPN_FFL_OWNER_PICKER=0`, which is
the way round that costs nothing locally; the gate's own tests live in
`test_app_auth.py`. It is not a security boundary, no more than the rest of
`auth` -- see that module's docstring, and plan 26 for the real login.

Streamlit is stubbed rather than driven through `AppTest`, for the reason
`test_header_selection.py` gives: the suite must run with no store on disk, and
`Data/` is untracked. The two facts that need pinning are that choosing an owner
reaches `auth.sign_in` -- so this dropdown and the eventual login share one seam --
and that the League selector below it lands on the new owner's own league rather
than holding one they cannot open.
"""

import ast
import pathlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import auth  # noqa: E402
from components import header  # noqa: E402

APP = pathlib.Path(__file__).resolve().parents[1] / "app"


class _Sidebar:
    """``with st.sidebar:`` -- a context manager, not a callable."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeStreamlit:
    """Enough of ``st`` for the identity block: session state, sidebar, widgets.

    ``selectbox`` mimics a keyed widget the same way
    ``test_header_selection.FakeStreamlit`` does -- it reads its value out of
    session state and writes it back -- because that contract is what makes "did
    the choice stick" answerable without a browser.
    """

    def __init__(self, session_state=None):
        self.session_state = dict(session_state or {})
        self.sidebar = _Sidebar()
        self.selectboxes = []
        self.captions = []
        self.markdowns = []

    def selectbox(self, label, options, key=None, format_func=str, **kwargs):
        self.selectboxes.append(
            {"label": label, "key": key, "options": list(options),
             "shown": [format_func(o) for o in options]})
        if key is not None and key in self.session_state:
            return self.session_state[key]
        chosen = options[kwargs.get("index", 0)]
        if key is not None:
            self.session_state[key] = chosen
        return chosen

    def caption(self, text):
        self.captions.append(text)

    def markdown(self, text):
        self.markdowns.append(text)


@pytest.fixture
def fake_st(monkeypatch):
    """One fake shared by both modules, so ``sign_in`` and the widget agree.

    ``auth`` and ``header`` each hold their own reference to ``streamlit``; patching
    only one would let the dropdown write a choice that ``current_viewer`` never
    sees, which is the bug this fixture exists to make impossible to miss.
    """
    fake = FakeStreamlit()
    monkeypatch.setattr(header, "st", fake)
    monkeypatch.setattr(auth, "st", fake)
    monkeypatch.delenv(auth.VIEWER_ENV, raising=False)
    monkeypatch.delenv(auth.ALL_LEAGUES_ENV, raising=False)
    monkeypatch.delenv(auth.OWNER_PICKER_ENV, raising=False)
    return fake


# --- what it offers -------------------------------------------------------

def test_it_offers_every_configured_viewer(fake_st):
    header.render_owner_picker()
    box = fake_st.selectboxes[0]
    assert box["label"] == "Owner"
    assert box["key"] == header.OWNER_KEY
    assert box["options"] == list(auth.VIEWERS)


def test_it_shows_names_rather_than_keys(fake_st):
    """The dropdown is read by a person; ``emma`` is a config key, not a name."""
    header.render_owner_picker()
    assert "Emma Richardson" in fake_st.selectboxes[0]["shown"]
    assert "emma" not in fake_st.selectboxes[0]["shown"]


# --- what choosing one does -----------------------------------------------

def test_choosing_an_owner_signs_them_in(fake_st):
    """The seam that matters: the dropdown hands its answer to the same function a
    real login will, rather than writing session state behind ``auth``'s back."""
    fake_st.session_state[header.OWNER_KEY] = "emma"
    header.render_owner_picker()
    assert auth.current_viewer() == auth.EMMA_VIEWER


def test_the_league_selector_lands_on_the_new_owners_league(fake_st):
    """Tommy's remembered league is not one Emma can open. ``sticky_selectbox``
    drops a remembered value that is no longer offered, so the correction needs no
    code in the picker -- but it is the behaviour a user would notice, so it is
    pinned here in the picker's terms rather than only in the primitive's."""
    fake_st.session_state[header.OWNER_KEY] = "emma"
    fake_st.session_state["league_key"] = "winfield_football"
    header.render_owner_picker()

    viewer = auth.current_viewer()
    hers = auth.visible_leagues(viewer, ["winfield_football", "richardson_invitational"])
    landed = header.sticky_selectbox("League", "league_key", hers,
                                     default=auth.default_league(viewer, hers))
    assert landed == "richardson_invitational"


def test_it_re_signs_in_on_every_run(fake_st):
    """Unconditional, like the key write in ``sticky_selectbox``. Streamlit drops
    widget state on navigation, and a conditional apply is how the sidebar ends up
    naming one owner while the pages render another."""
    fake_st.session_state[header.OWNER_KEY] = "emma"
    header.render_owner_picker()
    fake_st.session_state.pop(auth.SESSION_KEY)
    header.render_owner_picker()
    assert auth.current_viewer() == auth.EMMA_VIEWER


# --- the off switch -------------------------------------------------------

def test_the_off_switch_draws_nothing(fake_st, monkeypatch):
    monkeypatch.setenv(auth.OWNER_PICKER_ENV, "0")
    assert header.render_owner_picker() is False
    assert fake_st.selectboxes == []


def test_with_the_selector_off_the_identity_caption_is_what_names_the_owner(
        fake_st, monkeypatch):
    monkeypatch.setenv(auth.OWNER_PICKER_ENV, "0")
    header.render_identity()
    assert any("Tommy Winfield" in c for c in fake_st.captions)


def test_with_the_selector_on_the_caption_does_not_repeat_it(fake_st):
    """The dropdown already shows the name; a caption under it saying the same
    thing is furniture."""
    header.render_identity()
    assert fake_st.captions == []
    assert fake_st.selectboxes[0]["label"] == "Owner"


# --- where it sits --------------------------------------------------------

def test_the_owner_selector_is_drawn_above_the_league_selector():
    """Ordering is by call order in ``main.py``: Streamlit places sidebar elements
    in the order they are called, so Owner sits above League only for as long as
    ``render_identity`` is called before ``session.render_context``. Parsed rather
    than imported -- ``main.py`` is a script, and running it needs a store."""
    source = (APP / "main.py").read_text()
    # Sorted by line, because ``ast.walk`` is breadth-first and its order would
    # make this assertion pass or fail for reasons unrelated to the sidebar.
    found = sorted(
        ((node.lineno, node.func.attr)
         for node in ast.walk(ast.parse(source))
         if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
         and node.func.attr in {"render_identity", "render_context"}),
    )
    calls = [name for _, name in found]
    assert calls.index("render_identity") < calls.index("render_context")
