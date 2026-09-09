"""The tab bar, and the one thing that must not quietly turn it into a v1 app.

Streamlit still carries the first version of its multi-page feature, and the entire
trigger for it is a directory named ``pages`` beside the entrypoint. While that mode
is on, the script runner executes **the requested page file alone** and never runs
``app/main.py`` -- so :func:`session.render_context` never runs, and every route dies
on ``session.current() before session.render_context()`` the moment someone opens
``/roster`` as the first request to a freshly started server. It is decided in
``PagesManager`` before any application code runs, which is why the directory's *name*
is the only thing that can prevent it.

The symptom was miles from the cause -- a traceback in ``routes/roster.py`` about
session state, on a page whose own code was fine -- and nothing in the suite could
see it, because the whole failure lives in how Streamlit chooses what to execute.
These tests are cheap and they pin the two facts that matter: the directory is not
called ``pages``, and every route ``main.py`` registers is really there.
"""

import ast
import pathlib

import pytest

APP = pathlib.Path(__file__).resolve().parents[1] / "app"


def page_calls():
    """The ``st.Page(...)`` calls in ``main.py``, in source order.

    Parsed rather than imported because importing the entrypoint would execute the
    whole app -- ``main.py`` is a script, not a module, and running it needs a
    Streamlit runtime and a store.

    Returns:
        list: ``(route, {keyword: value})`` per call, ``PAGES`` order preserved.
    """
    tree = ast.parse((APP / "main.py").read_text())
    return [
        (node.args[0].value,
         {kw.arg: getattr(kw.value, "value", None) for kw in node.keywords})
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "Page"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    ]


def registered_routes():
    """Just the route files, in ``PAGES`` order."""
    return [route for route, _ in page_calls()]


def test_there_is_no_pages_directory_beside_the_entrypoint():
    """The one condition that switches Streamlit into legacy multi-page mode.

    ``PagesManager.uses_pages_directory`` is set from ``main_script_parent /
    "pages"`` existing, as a process-wide flag, before there is any application code
    to intervene from. Only the public ``st.navigation`` clears it -- which is why
    the app looked fine whenever something landed on the default page first, and
    broke when a deep link got there before ``main.py`` did.
    """
    assert not (APP / "pages").exists(), (
        "app/pages/ switches Streamlit to v1 multi-page routing, which runs a route "
        "file without app/main.py and therefore without session.render_context()."
    )


def test_every_registered_route_exists():
    """Streamlit raises on a missing page at import, so a renamed directory that
    missed one of these would take the whole app down rather than one tab."""
    routes = registered_routes()
    assert routes, "no st.Page(...) calls found in app/main.py"
    for route in routes:
        assert (APP / route).is_file(), f"{route} is registered but not on disk"


def test_the_routes_directory_holds_nothing_unregistered():
    """A route file nobody registered is a page with no way to reach it."""
    registered = {pathlib.PurePath(route).name for route in registered_routes()}
    on_disk = {path.name for path in (APP / "routes").glob("*.py")
               if path.name != "__init__.py"}
    assert on_disk == registered


@pytest.mark.parametrize("tab", ["home", "draft", "roster", "free_agents", "matchup"])
def test_every_tab_is_still_a_tab(tab):
    """The URL path Streamlit derives comes from the *file* name, not the directory,
    so renaming ``pages/`` to ``routes/`` left every bookmark working -- and adding
    Home must not have moved any of the other four."""
    assert f"routes/{tab}.py" in registered_routes()


def test_the_tabs_are_in_the_order_a_season_is_worked():
    """Home first because it is the question you arrive with; then a single week in
    the order you work it; Draft last because for all but one weekend it is history.

    Pinned because the order *is* the design -- it is what the tab bar communicates
    before you have read a single label -- and it is one line in ``main.py`` that a
    later edit could reshuffle without anything else noticing.
    """
    assert registered_routes() == [
        "routes/home.py",
        "routes/roster.py",
        "routes/matchup.py",
        "routes/free_agents.py",
        "routes/draft.py",
    ]


def test_home_is_the_landing_page():
    """Exactly one default, and it is Home. Streamlit lands on the first page when
    none is marked, so a stray ``default=True`` elsewhere -- or none at all -- is a
    silent change of what the app opens on."""
    defaults = [route for route, kwargs in page_calls() if kwargs.get("default")]
    assert defaults == ["routes/home.py"]


def test_no_tab_carries_an_icon():
    """The tab bar is labels only.

    Five emoji across the top read as five different kinds of thing rather than five
    peers, and the label already says what the tab is. Icons still earn their place
    inside a page, where they mark a severity -- see ``views.weekly.UPGRADE_CALLOUTS``.
    """
    carrying = {route for route, kwargs in page_calls() if kwargs.get("icon")}
    assert not carrying, f"icons are back on {sorted(carrying)}"
