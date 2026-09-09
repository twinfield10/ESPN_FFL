"""The staleness badge's threshold, which now means one thing rather than two.

It used to pivot on the season opener: 25 hours before week 1, one hour from then on.
The short number was calibrated for a real question -- "did you refresh before locking
this lineup", where injury news minutes before kickoff is what matters -- but it
answered it badly. The store is rebuilt once a night, so an hour after each 6am cron
the badge went red and stayed red for twenty-three hours, every day, with nothing
wrong. An alarm that fires daily teaches you to ignore it, and the button that acts on
it sits directly beneath it anyway.

So the badge answers the other question, the one it can answer honestly: **did the
nightly build run?** One threshold, a shade over a day, and crossing it means the cron
was missed or failed. These tests pin that it is one number, that the number outlives
one nightly run but not two, and that it still agrees with the command-line check --
which is the part that must not drift, because the badge's own caption tells you to run
it.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from components import header  # noqa: E402


def test_the_threshold_is_the_nightly_cadence():
    """A store built at 6am must still read fine when you open it that evening."""
    assert header.stale_after_minutes(2026) == header.STALE_AFTER_MIN
    # 14 hours -- cron at 6am, you look at 8pm.
    assert 14 * 60 < header.stale_after_minutes(2026)


def test_it_outlives_one_nightly_run_but_not_two():
    """25 hours: slack for a slow run, not tolerance for a skipped one.

    A missed 6am is exactly what the badge exists to surface, so the threshold has to
    sit above one day and below two.
    """
    assert 24 * 60 < header.STALE_AFTER_MIN < 48 * 60


@pytest.mark.parametrize("day", [
    date(2026, 3, 1),      # deep out of season
    date(2026, 8, 10),     # pre-season, nightly cron running
    date(2026, 9, 1),      # around the opener, where the pivot used to be
    date(2026, 11, 15),    # mid-season, the old one-hour window
    date(2026, 12, 28),    # playoffs
])
def test_the_same_number_all_year(day):
    """The pivot is gone, and its absence is the change worth pinning.

    Reintroducing a game-day threshold would restore a badge that is red all day on a
    store that is fine, which is the failure this replaced. If a second cadence is ever
    genuinely wanted, it belongs on a control the reader can see rather than on a date
    comparison they cannot.
    """
    assert header.stale_after_minutes(2026, day) == header.STALE_AFTER_MIN


def test_the_season_argument_no_longer_changes_the_answer():
    """The signature is kept as the seam every caller reads the threshold through --
    see :func:`components.header.stale_after_minutes` -- so it still accepts a season
    and a date. It must not quietly start using them again."""
    answers = {header.stale_after_minutes(season, day)
               for season in (2024, 2025, 2026, 2027)
               for day in (None, date(2026, 1, 1), date(2026, 9, 30))}
    assert answers == {header.STALE_AFTER_MIN}


def test_it_agrees_with_the_command_line_check():
    """Two surfaces answering the same question must not answer it differently.

    ``Scripts.refresh_status`` is what the badge's own caption tells you to run when
    it goes red. If they disagreed, one of them would be calling a store stale while
    the other called it fine -- and the badge would be sending you to a check that
    reports success.
    """
    from Scripts.refresh_status import DEFAULT_MAX_AGE_HOURS
    assert header.STALE_AFTER_MIN == pytest.approx(DEFAULT_MAX_AGE_HOURS * 60)
