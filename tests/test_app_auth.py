"""The viewer boundary: which leagues the app is allowed to offer.

There is no login yet, so what is covered here is the *scoping* -- the part a login
will hand its answer to. The point of the module is that the leagues the store holds
narrow to one viewer's four in exactly one place, so these tests are the ones that
would fail if a page went back to reading ``store.list_leagues`` directly.

**The store list and ``config.yaml`` used to disagree by two, and no longer do.**
``weenieless_wanderers`` and ``big_red_fantasy_football`` were disconnected from the
config on 2026-09-09 and 2026-09-15 with their parquet deliberately kept, on the
reasoning that :func:`store.list_leagues` reads prefixes rather than the config so the
data stayed reachable through :data:`auth.ALL_LEAGUES_ENV`. That end state turned out
to be the expensive one: ``Scripts.sync`` fanned out over the same prefixes, so both
leagues kept being published and kept minting a dated board snapshot nightly from a
board nothing rebuilt. Their data was purged on 2026-09-16 and the config is now the
authority for what gets published.

So :data:`ALL_LEAGUES` below is eight, not ten, and the tests that pinned the
kept-but-unreachable contract are gone with it. **They would have gone on passing** --
the fixture is hard-coded, so nothing in them reads the store -- which is the reason
to delete them in the same change rather than later: a green test asserting a fiction
is this repo's stated worst failure mode wearing a tick.
"""

import sys
from pathlib import Path

import pytest

# `app/` is not a package: Streamlit runs the page scripts directly with app/ on
# sys.path, so the modules import each other by bare name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import auth  # noqa: E402

#: Every league **the store holds**, in the order it lists them (sorted).
#:
#: Now the same eight ``config.yaml`` has. See the module docstring for why it was
#: ten until 2026-09-16.
ALL_LEAGUES = [
    "fields_league", "gop_degenerates", "jeffs_league", "john_atl_league",
    "john_pc_league", "knights_ffl", "twelve_dudes_one_cup", "winfield_football",
]


@pytest.fixture(autouse=True)
def _no_escape_hatch(monkeypatch):
    """Run with ALL_LEAGUES unset, whatever the developer's shell has in it."""
    monkeypatch.delenv(auth.ALL_LEAGUES_ENV, raising=False)


# --- scoping --------------------------------------------------------------

def test_the_default_viewer_sees_only_the_leagues_he_plays_in():
    visible = auth.visible_leagues(auth.DEFAULT_VIEWER, ALL_LEAGUES)
    assert visible == ["gop_degenerates", "jeffs_league", "knights_ffl",
                       "winfield_football"]


def test_the_other_owners_leagues_are_not_offered():
    """Four of the eight configured leagues belong to other owners. The pipeline still
    builds them and the Sheet still publishes them -- they are just not this
    viewer's."""
    visible = auth.visible_leagues(auth.DEFAULT_VIEWER, ALL_LEAGUES)
    for key in ("fields_league", "john_atl_league",
                "john_pc_league", "twelve_dudes_one_cup"):
        assert key not in visible


def test_the_stores_order_is_kept_not_the_viewers():
    """The store's order is sorted and stable across seasons; the viewer's is a
    preference. Sorting the picker by preference would move the list under you as
    leagues get built."""
    reversed_store = list(reversed(ALL_LEAGUES))
    assert auth.visible_leagues(auth.DEFAULT_VIEWER, reversed_store) == [
        "winfield_football", "knights_ffl", "jeffs_league", "gop_degenerates"]


def test_an_empty_league_list_means_unrestricted():
    """The sentinel the escape hatch and any future admin role both use."""
    everyone = auth.DEFAULT_VIEWER._replace(leagues=())
    assert auth.visible_leagues(everyone, ALL_LEAGUES) == ALL_LEAGUES


def test_a_viewer_whose_leagues_are_not_built_gets_an_empty_list_not_an_error():
    """Not an error state -- it is a season the refresh has not reached yet, and the
    sidebar has its own message for it."""
    assert auth.visible_leagues(auth.DEFAULT_VIEWER,
                                ["john_pc_league", "fields_league"]) == []


# --- where the app lands --------------------------------------------------

def test_the_app_defaults_to_winfield_football():
    assert auth.DEFAULT_VIEWER.default_league == "winfield_football"
    assert auth.default_league(auth.DEFAULT_VIEWER, ALL_LEAGUES) == "winfield_football"


def test_the_default_falls_back_to_the_first_league_actually_available():
    """A season Winfield_Football has no store for still has to land somewhere."""
    assert auth.default_league(auth.DEFAULT_VIEWER,
                               ["gop_degenerates", "knights_ffl"]) == "gop_degenerates"


def test_nothing_available_lands_nowhere():
    assert auth.default_league(auth.DEFAULT_VIEWER, []) is None


# --- the escape hatch -----------------------------------------------------

def test_the_env_var_drops_the_scope(monkeypatch):
    """Five leagues belong to other owners who read their numbers off the Sheet.
    When one of those Sheets looks wrong, the app is where you find out why."""
    monkeypatch.setenv(auth.ALL_LEAGUES_ENV, "1")
    viewer = auth.current_viewer()
    assert viewer.leagues == ()
    assert auth.visible_leagues(viewer, ALL_LEAGUES) == ALL_LEAGUES


@pytest.mark.parametrize("value", ["", "0", "no", "false", "off"])
def test_anything_that_is_not_a_yes_leaves_the_scope_on(monkeypatch, value):
    monkeypatch.setenv(auth.ALL_LEAGUES_ENV, value)
    assert auth.current_viewer().leagues == auth.DEFAULT_VIEWER.leagues


def test_the_unrestricted_viewer_still_lands_on_winfield_football():
    assert auth.UNRESTRICTED_VIEWER.default_league == "winfield_football"


# --- the seam a login lands in -------------------------------------------

def test_there_is_a_viewer_before_anyone_has_signed_in():
    """An app with no viewer has nothing to render, so the fallback is a real
    account rather than an anonymous one."""
    assert auth.current_viewer() == auth.DEFAULT_VIEWER


def test_a_signed_in_viewer_replaces_the_default():
    """What the eventual login callback does, and the only state it has to set."""
    import streamlit as st

    guest = auth.Viewer(user_id="guest", display_name="Guest",
                        leagues=("knights_ffl",), default_league="knights_ffl")
    try:
        auth.sign_in(guest)
        assert auth.current_viewer() == guest
        assert auth.visible_leagues(auth.current_viewer(), ALL_LEAGUES) == ["knights_ffl"]
    finally:
        auth.sign_out()
        st.session_state.pop(auth.SESSION_KEY, None)

    assert auth.current_viewer() == auth.DEFAULT_VIEWER
