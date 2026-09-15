"""The viewer boundary: which leagues the app is allowed to offer.

There is no login yet, so what is covered here is the *scoping* -- the part a login
will hand its answer to. The point of the module is that the leagues the store holds
narrow to one viewer's four in exactly one place, so these tests are the ones that
would fail if a page went back to reading ``store.list_leagues`` directly.

**The store list and ``config.yaml`` are not the same list, and since 2026-09-15 they
disagree by two.** ``weenieless_wanderers`` and ``big_red_fantasy_football`` were
disconnected -- removed from the config, and from :data:`auth.DEFAULT_VIEWER` where they
were in it -- but their parquet was deliberately kept, and
:func:`store.list_leagues` reads store prefixes rather than the config. So both are still
in :data:`ALL_LEAGUES` below, which is the honest fixture, and
``test_a_disconnected_league_is_not_offered_even_though_its_data_remains`` is what
makes sure that data cannot come back through the picker.
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
#: Two more than ``config.yaml`` has: ``weenieless_wanderers`` (2026-09-09) and
#: ``big_red_fantasy_football`` (2026-09-15) were disconnected and their data left in
#: place. See the module docstring.
ALL_LEAGUES = [
    "big_red_fantasy_football", "fields_league", "gop_degenerates",
    "jeffs_league", "john_atl_league", "john_pc_league", "knights_ffl",
    "twelve_dudes_one_cup", "weenieless_wanderers", "winfield_football",
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


def test_a_disconnected_league_is_not_offered_even_though_its_data_remains():
    """The end state of removing a league without deleting its parquet.

    ``weenieless_wanderers`` (2026-09-09) and ``big_red_fantasy_football``
    (2026-09-15) are out of ``config.yaml``, so the pipeline does not fetch them and
    the app does not offer them -- but their store prefixes still exist, and
    :func:`store.list_leagues` reads prefixes rather than the config, so both are still
    in ``ALL_LEAGUES``. The viewer's tuple is the only thing standing between kept data
    and a picker entry, which is why this is worth a test of its own rather than being
    folded into the other-owners case above: these are not somebody else's leagues,
    they are leagues nobody is meant to open.

    Big Red needed only the one edit -- it was never in ``DEFAULT_VIEWER.leagues`` --
    which is exactly why the config edit alone cannot be trusted to have covered it.
    """
    for key in ("weenieless_wanderers", "big_red_fantasy_football"):
        assert key in ALL_LEAGUES, "fixture should keep the store's view"
        assert key not in auth.DEFAULT_VIEWER.leagues
        assert key not in auth.visible_leagues(auth.DEFAULT_VIEWER, ALL_LEAGUES)


def test_the_escape_hatch_still_reaches_a_disconnected_league():
    """Deliberately, and it is the only route left to one.

    Kept data you cannot look at is data you cannot check, and the reason the parquet
    was left in place was to keep 2025 and 2026 readable. So the unrestricted viewer
    must still see it -- otherwise "keep the data" and "delete the data" would be the
    same outcome from the app's point of view.
    """
    everyone = auth.DEFAULT_VIEWER._replace(leagues=())
    assert "weenieless_wanderers" in auth.visible_leagues(everyone, ALL_LEAGUES)


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
