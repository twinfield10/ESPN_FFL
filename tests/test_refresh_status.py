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


def _iso_hours_ago(hours):
    """An ISO timestamp that many hours in the past."""
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


# --- per-source freshness and the odds pull -------------------------------
#
# This module knew whether the *job* ran and what the boards were built from, and
# nothing about the individual sources underneath. So when both books sat thirteen
# days stale on the 2026 draft board it reported everything healthy -- correctly, in
# its own terms: the nightly it was watching was healthy, it simply never ran them.
# A source is only as visible as something that names it.


def test_every_blended_source_is_named_in_the_manifest():
    """The manifest is the whole point. A source absent from it is a source this
    report cannot see, which is exactly how the books went missing.

    ``Injuries`` is in the set because the ESPN injury pull stopped being a diagnostic:
    ``_withdraw_sources_on_availability`` decides from it whether a player carries a
    projection at all, and it was left out of this manifest originally only because it
    is the one input that does not live under ``Data/Projections``."""
    named = {name for name, _, _ in rs.PROJECTION_SOURCES}
    assert {"FantasyPros", "Pinnacle", "BetOnline", "Usage",
            "Injuries"} <= named


def test_each_source_carries_a_runnable_fix_and_a_resolvable_path():
    """A report that says something is stale without saying how to fix it makes the
    reader go and find out, which is how a warning becomes background noise.

    The path is a resolver rather than a filename because not every input lives under
    ``Data/Projections/<source>/Season/<season>/`` -- the injury pull does not, and
    that technicality is why it went unwatched. Called here with ``create=False``
    semantics: merely asking where a source lives must not create a directory for it."""
    for name, resolve, fix in rs.PROJECTION_SOURCES:
        assert fix.startswith(("python -m", "Rscript")), name
        path = resolve(2999)
        assert path.suffix in (".parquet", ".csv"), name
        assert "2999" in str(path), name
        assert not path.parent.exists(), f"{name} created {path.parent}"


def test_a_missing_source_is_reported_and_makes_the_run_stale(tmp_path, monkeypatch,
                                                              capsys):
    monkeypatch.setattr(rs, "PROJECTION_SOURCES",
                        (("Nobody", lambda s: tmp_path / "nothing.parquet",
                          "python -m Scripts.nothing"),))
    assert rs._report_sources(2026, 25.0) is True
    out = capsys.readouterr().out
    assert "MISSING" in out and "Scripts.nothing" in out


def test_the_season_is_substituted_into_the_fix(tmp_path, monkeypatch, capsys):
    """`Rscript R/GetSeasonProps.R <season>` is not a command anyone can run."""
    monkeypatch.setattr(rs, "PROJECTION_SOURCES",
                        (("Nobody", lambda s: tmp_path / "nothing.parquet",
                          "Rscript R/Thing.R <season>"),))
    rs._report_sources(2026, 25.0)
    out = capsys.readouterr().out
    assert "Rscript R/Thing.R 2026" in out
    assert "<season>" not in out


def test_the_odds_pull_has_its_own_status_file():
    """One file for both jobs would let whichever ran last overwrite the other's
    verdict, and they have different fixes."""
    assert rs.ODDS_STATUS_PATH != rs.STATUS_PATH


def test_the_odds_threshold_matches_its_cadence():
    """Both jobs run daily, so both allow a day plus an hour of slack. If the odds
    pull ever goes six-hourly this has to come down with it, or a job that stopped
    firing would read healthy for a day."""
    assert rs.ODDS_MAX_AGE_HOURS == rs.DEFAULT_MAX_AGE_HOURS == 25.0


def test_an_odds_pull_that_never_ran_is_not_a_failure(tmp_path, monkeypatch, capsys):
    """Nothing on the draft board depends on it yet, so its absence is worth saying
    and not worth failing over."""
    monkeypatch.setattr(rs, "ODDS_STATUS_PATH", tmp_path / "absent.json")
    assert rs._report_odds(2026) is False
    assert "never run" in capsys.readouterr().out


def test_a_failed_odds_pull_is_stale(tmp_path, monkeypatch, capsys):
    import json
    path = tmp_path / "odds_status.json"
    path.write_text(json.dumps({"result": "failed", "at": _iso_hours_ago(1),
                                "stage": "Scripts.books.pull", "season": "2026"}))
    monkeypatch.setattr(rs, "ODDS_STATUS_PATH", path)
    assert rs._report_odds(2026) is True
    assert "FAILED" in capsys.readouterr().out


def test_an_overdue_odds_pull_is_stale(tmp_path, monkeypatch, capsys):
    """It runs daily, so a day and a half of silence is a missed run."""
    import json
    path = tmp_path / "odds_status.json"
    path.write_text(json.dumps({"result": "ok", "at": _iso_hours_ago(36),
                                "stage": "complete", "season": "2026"}))
    monkeypatch.setattr(rs, "ODDS_STATUS_PATH", path)
    assert rs._report_odds(2026) is True
    assert "DID NOT RUN" in capsys.readouterr().out
