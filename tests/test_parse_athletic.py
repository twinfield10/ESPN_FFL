"""The Athletic weekly parse: what counts as pending, and what a refusal leaves behind.

The parser itself is tested in ``tests/test_athletic_source.py``; here
``build_weekly`` is replaced, and what is under test is the bookkeeping between the
fetch's status file and this one (plan 55).
"""

import pytest

import Scripts.fetch_athletic as fa
import Scripts.load_athletic as la
import Scripts.parse_athletic as pa


def _fetched(**weeks):
    """A fetch status file's contents: ``week key -> (file, sha256)``."""
    return {"weeks": {k.replace("_", "-"): {"file": f, "sha256": s}
                      for k, (f, s) in weeks.items()}}


@pytest.fixture
def wired(monkeypatch, tmp_path):
    """Both status files and the landing folder in ``tmp_path``; imports logged."""
    landing = tmp_path / "Landing"
    landing.mkdir()
    calls = {"imports": [], "refuse": set(),
             "fetch": tmp_path / "fetch_status.json",
             "parse": tmp_path / "parse_status.json"}
    monkeypatch.setattr(pa, "landing_dir", lambda source, season, name: landing / name)

    def build_weekly(path, season, week):
        if path.name in calls["refuse"]:
            raise la.WeeklyFormatError(f"{path.name}: TE block has no REC YD column")
        calls["imports"].append((path.name, season, week))
    monkeypatch.setattr(la, "build_weekly", build_weekly)

    def land(key, name, sha):
        (landing / name).write_bytes(b"xlsx")
        status = fa.load_status(calls["fetch"])
        status.setdefault("weeks", {})[key] = {"file": name, "sha256": sha}
        fa.save_status(status, calls["fetch"])
    calls["land"] = land
    calls["run"] = lambda: pa.run(fetch_status_path=calls["fetch"],
                                  status_path=calls["parse"])
    return calls


def test_pending_is_every_week_whose_bytes_were_not_imported():
    fetched = _fetched(**{"2026_06": ("a.xlsx", "aaa"), "2026_07": ("b.xlsx", "bbb")})
    assert pa.pending(fetched, {}) == ["2026-06", "2026-07"]
    parsed = {"weeks": {"2026-06": {"sha256": "aaa"}, "2026-07": {"sha256": "old"}}}
    assert pa.pending(fetched, parsed) == ["2026-07"]


def test_a_landed_file_is_imported_once(wired):
    wired["land"]("2026-07", "Week7_Proj_1021_0fb8fa0a.xlsx", "0fb8")
    assert wired["run"]() == {"2026-07": "imported"}
    assert wired["imports"] == [("Week7_Proj_1021_0fb8fa0a.xlsx", 2026, 7)]
    assert wired["run"]() == {}
    assert len(wired["imports"]) == 1


def test_new_bytes_for_a_week_are_imported_again(wired):
    wired["land"]("2026-07", "Week7_a.xlsx", "aaa")
    wired["run"]()
    wired["land"]("2026-07", "Week7_b.xlsx", "bbb")
    assert wired["run"]() == {"2026-07": "imported"}
    assert [i[0] for i in wired["imports"]] == ["Week7_a.xlsx", "Week7_b.xlsx"]


def test_a_refusal_is_recorded_and_retried_until_it_imports(wired):
    """The week stays pending, so every nightly says so -- and once the parser is fixed
    the file landed days earlier imports with no re-download."""
    wired["land"]("2026-07", "Week7_a.xlsx", "aaa")
    wired["refuse"].add("Week7_a.xlsx")

    for _ in range(2):
        outcome = wired["run"]()["2026-07"]
        assert outcome.startswith("WeeklyFormatError") and "REC YD" in outcome
    status = fa.load_status(wired["parse"])
    assert "2026-07" in status["failures"] and "2026-07" not in status["weeks"]

    wired["refuse"].clear()                          # the parser is fixed
    assert wired["run"]() == {"2026-07": "imported"}
    status = fa.load_status(wired["parse"])
    assert status["failures"] == {} and status["weeks"]["2026-07"]["sha256"] == "aaa"


def test_one_refused_week_does_not_block_another(wired):
    wired["land"]("2026-06", "Week6.xlsx", "666")
    wired["land"]("2026-07", "Week7.xlsx", "777")
    wired["refuse"].add("Week6.xlsx")
    outcomes = wired["run"]()
    assert outcomes["2026-07"] == "imported" and outcomes["2026-06"] != "imported"


def test_a_file_missing_from_this_machine_is_a_failure_not_a_skip(wired):
    status = {"weeks": {"2026-07": {"file": "not_here.xlsx", "sha256": "aaa"}}}
    fa.save_status(status, wired["fetch"])
    assert wired["run"]()["2026-07"].startswith("FileNotFoundError")


def test_a_week_the_old_fetch_imported_is_reimported_once(wired):
    """The mini's fetch status already holds week 7 as the pre-split code wrote it; the
    first parse run imports that file (the same bytes, so the same rows) and is then
    caught up."""
    wired["land"]("2026-07", "Week7_Proj_1021.xlsx", "0fb8")
    status = fa.load_status(wired["fetch"])
    status["weeks"]["2026-07"]["imported_at"] = "2026-10-21T15:47:13+00:00"
    fa.save_status(status, wired["fetch"])
    assert wired["run"]() == {"2026-07": "imported"}
    assert wired["run"]() == {}


def test_main_exits_nonzero_on_a_refusal(wired, monkeypatch):
    monkeypatch.setattr(pa, "run", lambda: {"2026-07": "WeeklyFormatError: x"})
    assert pa.main([]) == 1
    monkeypatch.setattr(pa, "run", lambda: {"2026-07": "imported"})
    assert pa.main([]) == 0


def test_refresh_status_names_a_refused_week(wired, capsys):
    from Scripts import refresh_status as rs

    assert rs._report_athletic_parse(wired["parse"]) is False   # never run
    wired["land"]("2026-07", "Week7_a.xlsx", "aaa")
    wired["refuse"].add("Week7_a.xlsx")
    wired["run"]()
    capsys.readouterr()
    assert rs._report_athletic_parse(wired["parse"]) is True
    out = capsys.readouterr().out
    assert "REFUSED 2026-07" in out and "REC YD" in out
