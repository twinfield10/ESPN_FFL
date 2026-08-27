"""No scraper may do its work at import time.

`Scripts/scrape_pinnacle_season.py` already carried this check for itself, with the
reason in its docstring: importing a scraper used to fire a live scrape, which is how
a stray ``import Scripts.scrape_FP`` once wrote a projections file. This generalises
it to every scraper, because the guard that only watches the file that already has it
watches the wrong file -- plan 36 found the identical defect in ``scrape_BOL.py``,
where the import-time statements *write* the archived parquet and CSV rather than
merely spending a scrape.

Deliberately pure ``ast.parse`` with **no import**. A test that imported these to
check whether importing them is safe would answer its own question by causing the
damage, and ``tests/test_market.py`` already reads ``scrape_pinnacle.py`` as text for
exactly this reason.

Two rules, because one is not enough. A bare call is the obvious form
(``write_parquet(...)``, ``driver.get(...)``); the form that slipped through review is
``driver = webdriver.Chrome(...)``, which is an assignment and launches a browser.
"""

import ast

import pytest

from Scripts.paths import REPO_ROOT

SCRIPTS = REPO_ROOT / "Scripts"

#: Calls a scraper may make while defining its module constants.
#:
#: An allowlist rather than a denylist: the failure being prevented is someone adding
#: a *new* kind of import-time work, and a denylist cannot know about it. Everything
#: here reads the schedule CSV or config at worst -- no network, no browser, no write.
#: Adding to this list should be a deliberate decision, which is the point of the
#: failure it causes.
PURE_AT_IMPORT = frozenset({
    "compile",          # re.compile, for module-level patterns
    "current_season",
    "current_week",
    "date",             # datetime.date, for a default
    "date_week",
    "frozenset",
    "get_season",
    "load_config",
    "set",
    "sorted",
    "tuple",
})


def scraper_files():
    """Every scraper module, sorted. The glob is the point -- a new one is covered
    the moment it is added, without anyone remembering to list it here."""
    return sorted(SCRIPTS.glob("scrape_*.py"))


def _called_name(node):
    """The bare name of whatever a Call node calls, for allowlisting."""
    func = node.func
    return getattr(func, "id", None) or getattr(func, "attr", None) or "?"


def test_there_are_scrapers_to_check():
    """A glob that matches nothing passes every test under it silently."""
    assert len(scraper_files()) >= 4


@pytest.mark.parametrize("path", scraper_files(), ids=lambda p: p.name)
def test_scraper_makes_no_bare_call_at_import(path):
    """A statement that is nothing but a call is a call made for its side effect."""
    tree = ast.parse(path.read_text())
    bare = [
        f"line {n.lineno}: {_called_name(n.value)}(...)"
        for n in tree.body
        if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call)
    ]
    assert not bare, (
        f"{path.name} does work at import time -- {bare}. Move it into main() "
        f"behind `if __name__ == '__main__':`, following scrape_pinnacle_season.py."
    )


@pytest.mark.parametrize("path", scraper_files(), ids=lambda p: p.name)
def test_scraper_binds_no_module_constant_with_a_side_effect(path):
    """`driver = webdriver.Chrome(...)` is an assignment, and it opens a browser."""
    tree = ast.parse(path.read_text())
    offenders = []
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            continue
        value = node.value
        if value is None:
            continue
        for sub in ast.walk(value):
            if isinstance(sub, ast.Call):
                name = _called_name(sub)
                if name not in PURE_AT_IMPORT:
                    offenders.append(f"line {node.lineno}: {name}(...)")
                break
    assert not offenders, (
        f"{path.name} calls {offenders} while defining module constants. If it is "
        f"pure, add it to PURE_AT_IMPORT; otherwise move it into main()."
    )


@pytest.mark.parametrize("path", scraper_files(), ids=lambda p: p.name)
def test_a_scraper_with_a_main_can_be_invoked(path):
    """A ``main()`` with no guard is unreachable by ``python -m``.

    Conditional on defining ``main()`` on purpose. Two modules here carry the
    ``scrape_`` prefix but are pure libraries -- ``scrape_player_stats`` and
    ``scrape_team_stats`` are imported for their functions and have no entry point to
    guard. Demanding a guard from them would be cargo cult; the invariant that
    actually matters to them is the two rules above, which they already satisfy.
    """
    tree = ast.parse(path.read_text())
    if not any(isinstance(n, ast.FunctionDef) and n.name == "main" for n in tree.body):
        pytest.skip(f"{path.name} defines no main(); it is a library, not an entry point")
    guarded = any(
        isinstance(n, ast.If)
        and any(isinstance(sub, ast.Name) and sub.id == "__name__"
                for sub in ast.walk(n.test))
        for n in tree.body
    )
    assert guarded, (
        f"{path.name} defines main() but has no `if __name__ == '__main__':` guard, "
        f"so `python -m Scripts.{path.stem}` cannot reach it."
    )
