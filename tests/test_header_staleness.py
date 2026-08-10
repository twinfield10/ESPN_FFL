"""The staleness badge's threshold, which has to mean two different things.

The app's job here is to stop an hour-old number being read as live. That is the
right instinct in season, where the question behind the badge is "did you refresh
before locking this lineup" and injury news an hour before kickoff is the whole
point.

Before week 1 it is the wrong instinct, and wrong in the way that quietly disarms an
alarm: the data refreshes once a night on cron, so a one-hour threshold paints the
badge red 23 hours out of 24 for a month. Nobody reads a badge that is always red,
and the cost lands in September when it finally means something.

So the threshold pivots on the season opener. These tests pin both sides of that and
the direction of the difference, which is the part that must not silently invert.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from components import header  # noqa: E402


def test_before_the_opener_the_threshold_is_the_nightly_cadence():
    """A board built at 6am must still read green when you open it that evening."""
    assert header.stale_after_minutes(2026, date(2026, 8, 10)) == \
        header.STALE_AFTER_MIN_PRE_SEASON
    # 14 hours -- cron at 6am, you look at 8pm.
    assert 14 * 60 < header.stale_after_minutes(2026, date(2026, 8, 10))


def test_on_and_after_the_opener_the_threshold_is_game_day():
    for day in (date(2026, 9, 1), date(2026, 11, 15), date(2026, 12, 28)):
        assert header.stale_after_minutes(2026, day) == \
            header.STALE_AFTER_MIN_IN_SEASON


def test_the_pre_season_threshold_is_the_looser_one():
    """The direction is the thing. Inverting these would hold a game-day alarm
    through August and a day-long tolerance through the playoffs -- both wrong, and
    neither obvious from reading a single constant."""
    assert header.STALE_AFTER_MIN_PRE_SEASON > header.STALE_AFTER_MIN_IN_SEASON


def test_the_pre_season_threshold_outlives_one_nightly_run_but_not_two():
    """25 hours: slack for a slow run, not tolerance for a skipped one.

    A missed 6am is exactly what the badge exists to surface pre-season, so the
    threshold has to sit above one day and below two.
    """
    assert 24 * 60 < header.STALE_AFTER_MIN_PRE_SEASON < 48 * 60


def test_it_agrees_with_the_command_line_check():
    """Two surfaces answering the same question must not answer it differently.

    ``Scripts.refresh_status`` is what the app's own caption tells you to run when
    the badge is red pre-season. If they disagreed, one of them would be calling a
    board stale while the other called it fine.
    """
    from Scripts.refresh_status import DEFAULT_MAX_AGE_HOURS
    assert header.STALE_AFTER_MIN_PRE_SEASON == pytest.approx(
        DEFAULT_MAX_AGE_HOURS * 60)


def test_the_boundary_uses_the_repo_s_existing_season_start():
    """Reused rather than redeclared, so there is one answer to 'when does the
    season start' rather than two that can drift."""
    from Scripts.usage.features import SEASON_START
    opener = date(2026, *SEASON_START)
    day_before = date.fromordinal(opener.toordinal() - 1)
    assert header.stale_after_minutes(2026, day_before) == \
        header.STALE_AFTER_MIN_PRE_SEASON
    assert header.stale_after_minutes(2026, opener) == \
        header.STALE_AFTER_MIN_IN_SEASON
