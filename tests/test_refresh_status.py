"""The staleness check, whose whole job is to fire when nothing happened.

A cron job cannot report its own absence, so this check is the only thing standing
between a laptop that was asleep at six and a board that silently ages for a week.
That makes the failing paths the ones worth pinning: a check that never fires is
indistinguishable from a system that never breaks, right up until it matters.

No network, no filesystem beyond what the fixtures write.
"""

from datetime import datetime, timedelta, timezone

import pytest

from Scripts import refresh_status as rs


# --- age arithmetic ------------------------------------------------------

def test_zulu_timestamps_parse():
    """The shell writes `date -u ...Z`, which fromisoformat rejects before 3.11."""
    stamp = (datetime.now(timezone.utc) - timedelta(hours=3)
             ).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert rs._age_hours(stamp) == pytest.approx(3.0, abs=0.05)


def test_offset_timestamps_parse():
    """The store writes an offset-aware local time, not Zulu."""
    stamp = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    assert rs._age_hours(stamp) == pytest.approx(2.0, abs=0.05)


def test_a_naive_timestamp_is_read_as_utc_not_local():
    """Reading it as local time would shift the age by the offset and could
    make a stale board look fresh, which is the one direction that must not happen."""
    stamp = (datetime.now(timezone.utc) - timedelta(hours=5)
             ).replace(tzinfo=None).isoformat()
    assert rs._age_hours(stamp) == pytest.approx(5.0, abs=0.05)


def test_garbage_is_unknown_rather_than_zero():
    """An unparseable stamp must not read as 'just refreshed'."""
    assert rs._age_hours("not a date") is None
    assert rs._age_hours("") is None
    assert rs._age_hours(None) is None


def test_unknown_age_is_reported_not_hidden():
    assert rs._fmt(None) == "unknown"


# --- the threshold -------------------------------------------------------

def test_the_threshold_is_longer_than_the_refresh_interval():
    """25h against a 6am daily job: enough slack for a slow run, not enough to
    swallow a skipped one. The app's 60-minute badge would read red 23 hours out of
    24 on this cadence, and a badge that is always red is one nobody reads."""
    assert 24 < rs.DEFAULT_MAX_AGE_HOURS < 36


# --- the failing paths, which are the point ------------------------------

@pytest.fixture
def status(tmp_path, monkeypatch):
    """Point the checker at a scratch status file."""
    path = tmp_path / "refresh_status.json"
    monkeypatch.setattr(rs, "STATUS_PATH", path)
    return path


def test_a_missing_status_file_is_stale(status, capsys):
    """Never run at all."""
    assert rs.main([]) == 1
    assert "NEVER RUN" in capsys.readouterr().out


def test_a_failed_run_is_stale_and_names_the_stage(status, capsys):
    """Ran, broke. The stage is what turns a log dive into a glance."""
    status.write_text('{"result": "failed", "at": "%s", "stage": "R/GetContext.R"}'
                      % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert rs.main([]) == 1
    out = capsys.readouterr().out
    assert "FAILED" in out and "R/GetContext.R" in out


def test_an_old_success_is_stale(status, capsys):
    """The case no notification can ever catch: the machine was asleep, so nothing
    failed and nothing ran."""
    old = (datetime.now(timezone.utc) - timedelta(hours=30)
           ).strftime("%Y-%m-%dT%H:%M:%SZ")
    status.write_text('{"result": "ok", "at": "%s", "stage": "complete"}' % old)
    assert rs.main([]) == 1
    assert "DID NOT RUN" in capsys.readouterr().out


def test_a_recent_success_is_not_stale_on_its_own(status, capsys):
    """The status half must pass cleanly; the board half is checked separately and
    may still fail on a fresh checkout with no store."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    status.write_text('{"result": "ok", "at": "%s", "stage": "complete"}' % now)
    rs.main([])
    assert "refresh    ok" in capsys.readouterr().out
