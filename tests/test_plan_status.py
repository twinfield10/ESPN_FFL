"""The plan status convention, which is only useful while both copies agree.

Every plan in ``docs/plans`` records its status twice: a ``**Status:**`` line under
its own title, and the README section it is listed in. Two copies is the deliberate
choice -- the doc has to state its own status for anyone who opens it directly, and
the README has to group them for anyone scanning the set -- and two copies of a fact
drift. They drifted before this convention existed: 31 and 32 both said "not started"
while phase 1 of each sat built in an open PR.

So these pin the drift rather than the format. A status changed in one place and not
the other, a plan added with no README row, a row left behind by a deleted plan, and
a section heading whose count no longer matches the rows under it -- each is silent
in review and each makes the classification worthless.
"""

import re

import pytest

from Scripts import paths

PLANS_DIR = paths.REPO_ROOT / "docs" / "plans"

#: The only three a plan may carry. TO DO means not started; IN PROGRESS means partly
#: built with work owed; COMPLETE means nothing left to build -- which includes a plan
#: whose measured answer was *no*, and one superseded by a later plan.
STATUSES = ("TO DO", "IN PROGRESS", "COMPLETE")

_STATUS_LINE = re.compile(r"^\*\*Status:\*\* (.+)$", re.M)
_ROW = re.compile(r"^\| (\d\d) \| \[[^\]]+\]\((\d\d-[a-z0-9-]+\.md)\)")
_SECTION = re.compile(r"^## (TO DO|IN PROGRESS|COMPLETE) \((\d+)\)$")


def plan_files():
    """Every numbered plan doc, sorted. Excludes the README itself."""
    return sorted(PLANS_DIR.glob("[0-9][0-9]-*.md"))


def declared_status(path):
    """The status a plan doc claims for itself.

    Args:
        path: Path to a plan doc.

    Returns:
        The single status string on its ``**Status:**`` line.

    Raises:
        AssertionError: If the doc carries no status line or more than one -- both
            mean the doc cannot be classified, which is the thing being prevented.
    """
    found = _STATUS_LINE.findall(path.read_text())
    assert len(found) == 1, f"{path.name}: expected 1 **Status:** line, found {len(found)}"
    return found[0].strip()


def readme_sections():
    """The README's grouping, read the way a reader sees it.

    Returns:
        A tuple of ``(by_plan, declared_counts)`` -- the status each plan is filed
        under, keyed by its two-digit number, and the count each section heading
        advertises.
    """
    by_plan, declared_counts, current = {}, {}, None
    for line in (PLANS_DIR / "README.md").read_text().split("\n"):
        section = _SECTION.match(line)
        if section:
            current = section.group(1)
            declared_counts[current] = int(section.group(2))
            continue
        row = _ROW.match(line)
        if row:
            assert current, f"plan {row.group(1)} is listed above any status heading"
            assert row.group(1) not in by_plan, f"plan {row.group(1)} is listed twice"
            by_plan[row.group(1)] = current
    return by_plan, declared_counts


# --- the drift these exist to catch --------------------------------------

@pytest.mark.parametrize("path", plan_files(), ids=lambda p: p.name)
def test_a_plan_declares_one_of_the_three_statuses(path):
    status = declared_status(path)
    assert status in STATUSES, (
        f"{path.name} declares {status!r}. The rich narrative belongs on the "
        f"**Where it stands:** line; **Status:** takes one of {STATUSES}.")


@pytest.mark.parametrize("path", plan_files(), ids=lambda p: p.name)
def test_a_plan_is_filed_under_the_status_it_declares(path):
    """The whole convention. Change one copy and this is what says so."""
    by_plan, _ = readme_sections()
    number = path.name[:2]
    assert number in by_plan, (
        f"{path.name} exists but has no README row -- it is invisible to anyone "
        f"scanning the set.")
    assert by_plan[number] == declared_status(path), (
        f"{path.name} says {declared_status(path)!r}; the README files it under "
        f"{by_plan[number]!r}. Update both.")


def test_every_readme_row_still_has_a_plan_behind_it():
    """A row outliving its file links to nothing and inflates a count."""
    by_plan, _ = readme_sections()
    on_disk = {p.name[:2] for p in plan_files()}
    assert not (set(by_plan) - on_disk), (
        f"README lists plans with no file: {sorted(set(by_plan) - on_disk)}")


def test_a_section_heading_counts_the_rows_under_it():
    """``## COMPLETE (19)`` is a claim, and it is the first thing anyone reads."""
    by_plan, declared_counts = readme_sections()
    for status, claimed in declared_counts.items():
        actual = sum(1 for s in by_plan.values() if s == status)
        assert claimed == actual, (
            f"heading says '## {status} ({claimed})' but {actual} plans are "
            f"listed under it")


def test_every_status_has_a_section_even_when_it_is_empty():
    """An emptied tier must not vanish -- a missing heading reads as "none of
    these exist" rather than "none right now", and the next plan to land in it
    has nowhere to go."""
    _, declared_counts = readme_sections()
    assert set(declared_counts) == set(STATUSES), (
        f"README is missing a section for {sorted(set(STATUSES) - set(declared_counts))}")
