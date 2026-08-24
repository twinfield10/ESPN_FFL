"""The weekly injury review: who it names, and who it does not.

This is the piece the runbook leans on, so what matters is its *precision*. A review that
names twenty players a week gets skipped by week 4, and a review that flags the entries it
was created to contradict trains you to ignore its own output — which the first draft did,
because it judged "resolved" by asking whether ESPN listed the player active. For Jeremiyah
Love the answer is yes, *while he carries a high ankle sprain*, and that disagreement is the
entire reason someone wrote him down.

Synthetic boards. No parquet, no network.
"""

import datetime

import polars as pl
import pytest
import yaml

from Scripts.injury import review as rv
from Scripts.injury import severity as sv

TODAY = datetime.date(2026, 8, 18)


def board(rows):
    """A stored board carrying the columns the review reads."""
    return pl.DataFrame([{
        "player_name": r.get("name", "A Player"),
        "player_id": r.get("espn_id", "1"),
        "name_key": r.get("name", "A Player").upper(),
        "primaryPosition": r.get("pos", "WR"),
        "adp": r.get("adp", 50.0),
        "injury_status": r.get("status", "ACTIVE"),
        "inj_body_part": r.get("part"),
        "inj_detail": r.get("detail"),
        "inj_expected_absence_weeks": r.get("weeks"),
        "inj_recovery_cost": r.get("cost", 0.0),
        "inj_reinjury_prob": r.get("risk", 0.05),
        "inj_severity_source": r.get("source"),
        "inj_evidence": r.get("evidence", ""),
    } for r in rows])


def write_overrides(tmp_path, monkeypatch, entries, season=2026):
    monkeypatch.setattr(sv.paths, "INJURY_OVERRIDES_DIR", tmp_path)
    path = sv.overrides_path(season, create=True)
    with open(path, "w") as handle:
        yaml.safe_dump({"players": entries}, handle)
    return path


def entry(**kwargs):
    base = {"espn_id": 4870808, "name_key": "JEREMIYAH LOVE",
            "player": "Jeremiyah Love", "body_part": "ankle_high",
            "weeks_out": [4, 6], "as_of": datetime.date(2026, 8, 18),
            "source": "beat report"}
    base.update(kwargs)
    return base


# --- who it names --------------------------------------------------------

def test_a_weak_rung_inside_the_cutoff_is_named():
    named = rv.needs_severity(board([
        {"name": "Weak", "adp": 40.0, "source": "comment", "part": "ankle"}]))
    assert named["player_name"].to_list() == ["Weak"]


def test_a_published_diagnosis_is_left_alone():
    """ESPN naming the ligament beats any prior a human would supply."""
    named = rv.needs_severity(board([
        {"name": "Diagnosed", "adp": 40.0, "source": "espn_structured",
         "part": "knee", "detail": "acl"}]))
    assert named.is_empty()


def test_an_espn_return_date_is_left_alone():
    named = rv.needs_severity(board([
        {"name": "Dated", "adp": 40.0, "source": "return_date", "part": "ankle"}]))
    assert named.is_empty()


def test_an_existing_override_is_not_asked_for_again():
    named = rv.needs_severity(board([
        {"name": "Done", "adp": 40.0, "source": "override", "part": "ankle"}]))
    assert named.is_empty()


def test_a_player_listed_but_unresolvable_is_named():
    """He is hurt and nothing could be read. The strongest signal a human should look."""
    named = rv.needs_severity(board([
        {"name": "Unknown", "adp": 40.0, "source": "none", "status": "OUT"}]))
    assert named["player_name"].to_list() == ["Unknown"]


def test_a_healthy_player_is_never_named():
    assert rv.needs_severity(board([{"name": "Fine", "adp": 5.0}])).is_empty()


def test_past_the_cutoff_nobody_is_named():
    """Nobody is starting an ADP 400 player, so the group average is good enough."""
    rows = board([{"name": "Deep", "adp": 400.0, "source": "comment", "part": "ankle"}])
    assert rv.needs_severity(rows, adp_cutoff=150.0).is_empty()
    assert not rv.needs_severity(rows, adp_cutoff=500.0).is_empty()


def test_a_player_with_no_adp_is_not_named():
    """An undrafted player has no ADP, and filling it with zero would put him first."""
    rows = board([{"name": "NoAdp", "source": "comment", "part": "ankle"}])
    rows = rows.with_columns(pl.lit(None, dtype=pl.Float64).alias("adp"))
    assert rv.needs_severity(rows).is_empty()


def test_the_named_are_ordered_by_how_much_they_matter():
    named = rv.needs_severity(board([
        {"name": "Late", "adp": 120.0, "source": "comment", "part": "ankle"},
        {"name": "Early", "adp": 4.0, "source": "comment", "part": "ankle"},
    ]))
    assert named["player_name"].to_list() == ["Early", "Late"]


def test_an_older_board_without_the_columns_returns_nothing_rather_than_raising():
    stored = pl.DataFrame({"player_name": ["A"], "adp": [1.0]})
    assert rv.needs_severity(stored).is_empty()


# --- what it says about them ---------------------------------------------

def test_a_camp_knock_is_named_without_the_marker():
    """Precision is the whole point. Most weeks every flagged player is a half-game knock,
    and the correct action is to close the file."""
    text = rv.report(2026, board([
        {"name": "Knock", "adp": 4.0, "source": "comment", "part": "hamstring",
         "weeks": 0.5}]), today=TODAY)
    assert "Knock" in text
    assert "worth a look" not in text


def test_something_costing_real_games_gets_the_marker():
    text = rv.report(2026, board([
        {"name": "Real", "adp": 4.0, "source": "comment", "part": "knee",
         "weeks": 4.0}]), today=TODAY)
    assert "worth a look" in text


def test_the_status_is_abbreviated_from_the_stored_column():
    """``injury_code`` is a view-layer derivation and is not on the artifact; selecting it
    here silently produced an empty column."""
    text = rv.report(2026, board([
        {"name": "Hurt", "adp": 4.0, "source": "none", "status": "INJURY_RESERVE"}]),
        today=TODAY)
    assert " IR " in text


def test_an_empty_review_says_so_plainly():
    text = rv.report(2026, board([{"name": "Fine", "adp": 5.0}]), today=TODAY)
    assert "Nothing needs a severity written" in text


def test_the_review_names_the_file_to_edit_and_the_command_to_run():
    text = rv.report(2026, board([{"name": "Fine", "adp": 5.0}]), today=TODAY)
    assert "config/injuries/2026.yaml" in text
    assert "Scripts.refresh" in text


# --- the health of existing overrides ------------------------------------

def test_a_fresh_override_is_neither_stale_nor_expired(tmp_path, monkeypatch):
    write_overrides(tmp_path, monkeypatch, [entry()])
    stale, expired = rv.override_health(2026, board([]), today=TODAY)
    assert stale == [] and expired == []


def test_an_override_past_its_own_window_is_expired(tmp_path, monkeypatch):
    """"Four to six weeks from 18 August" is spent by early October, and that is checkable
    from the entry alone without asking a feed that may still disagree."""
    write_overrides(tmp_path, monkeypatch, [entry()])
    _, expired = rv.override_health(2026, board([]),
                                    today=datetime.date(2026, 10, 20))
    assert [e["player"] for e in expired] == ["Jeremiyah Love"]


def test_an_active_status_does_not_expire_an_override(tmp_path, monkeypatch):
    """**The flaw the first draft had.** Love is listed Active *while* carrying a high ankle
    sprain — that disagreement is why the entry exists. A check that flags every entry it was
    created to contradict trains you to skip the section."""
    write_overrides(tmp_path, monkeypatch, [entry()])
    rows = board([{"name": "Jeremiyah Love", "espn_id": "4870808", "adp": 18.5,
                   "status": "ACTIVE", "source": "override", "part": "ankle",
                   "weeks": 5.0}])
    stale, expired = rv.override_health(2026, rows,
                                        today=TODAY + datetime.timedelta(days=3))
    assert expired == []
    assert stale == []


def test_an_override_stale_but_inside_its_window_is_reported_as_stale(tmp_path,
                                                                     monkeypatch):
    """A long injury written once, five weeks ago: not expired, but nobody has re-read the
    report."""
    write_overrides(tmp_path, monkeypatch, [entry(weeks_out=[16, 20])])
    stale, expired = rv.override_health(2026, board([]),
                                        today=TODAY + datetime.timedelta(days=35))
    assert [e["player"] for e in stale] == ["Jeremiyah Love"]
    assert expired == []


def test_an_entry_is_counted_once_however_many_keys_it_has(tmp_path, monkeypatch):
    """The loader indexes each entry under both its id and its name, and a review that
    reported it twice would look like two problems."""
    write_overrides(tmp_path, monkeypatch, [entry(weeks_out=[1, 1])])
    _, expired = rv.override_health(2026, board([]),
                                    today=TODAY + datetime.timedelta(days=30))
    assert len(expired) == 1


def test_a_malformed_override_file_is_reported_not_raised(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sv.paths, "INJURY_OVERRIDES_DIR", tmp_path)
    sv.overrides_path(2026, create=True).write_text(
        "players:\n  - espn_id: 1\n    body_part: anke\n    weeks_out: 4\n"
        "    as_of: 2026-08-18\n    source: x\n")
    stale, expired = rv.override_health(2026, board([]), today=TODAY)
    assert "unreadable" in capsys.readouterr().out
    assert stale == [] and expired == []


def test_no_override_file_is_not_a_problem(tmp_path, monkeypatch):
    monkeypatch.setattr(sv.paths, "INJURY_OVERRIDES_DIR", tmp_path)
    assert rv.override_health(2026, board([]), today=TODAY) == ([], [])


# --- reading a board off disk -------------------------------------------

def test_a_missing_board_names_the_command_that_builds_one(tmp_path, monkeypatch):
    from Scripts import paths

    monkeypatch.setattr(paths, "DATA_DIR", tmp_path)
    with pytest.raises(FileNotFoundError, match="Scripts.refresh"):
        rv.load_board(2026)


def test_the_weak_and_strong_rungs_between_them_cover_every_source():
    """A rung in neither list would be silently skipped by the review."""
    known = set(rv.WEAK_RUNGS) | set(rv.STRONG_RUNGS)
    assert known == {"override", "espn_structured", "return_date", "comment", "report",
                     "none"}
