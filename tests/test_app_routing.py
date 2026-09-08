"""The app is a v2 multi-page app, and nothing may quietly make it a v1 one.

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


def registered_routes():
    """The ``st.Page`` arguments in ``main.py``, read without importing Streamlit.

    Parsed rather than imported because importing the entrypoint would execute the
    whole app -- ``main.py`` is a script, not a module, and running it needs a
    Streamlit runtime and a store.
    """
    tree = ast.parse((APP / "main.py").read_text())
    return [node.args[0].value
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "Page"
            and node.args
            and isinstance(node.args[0], ast.Constant)]


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


@pytest.mark.parametrize("tab", ["draft", "roster", "free_agents", "matchup"])
def test_the_four_tabs_are_still_the_four_tabs(tab):
    """The URL path Streamlit derives comes from the *file* name, not the directory,
    so renaming ``pages/`` to ``routes/`` left every bookmark working."""
    assert f"routes/{tab}.py" in registered_routes()
