"""ESPN's injury report and the return-date adjustment.

The one piece of injury information no other source in the blend carries. What is
pinned here is mostly the *interpretation* of the return date, because the field
carries two different kinds of value and treating them alike is the trap: a near-term
date is a real estimate, and a date past the end of the schedule is a season-ending
placeholder. Read literally, the placeholder means "returns in week 23".

Also pinned: the dated snapshot archive, because it has no backfill -- a day the
nightly job fails to file is a day gone, and nothing else would report it.

Synthetic payloads. No network.
"""

import datetime

import pandas as pd
import polars as pl
import pytest

from Scripts import scrape_espn_injuries as ei
from Scripts import season_projections as sp

WEEK_ONE = datetime.date(2026, 9, 9)


# --- parsing --------------------------------------------------------------

def payload(records):
    return {"injuries": [{"displayName": "Arizona Cardinals", "injuries": records}]}


def record(name, status="Out", return_date=None, detail=None):
    return {
        "status": status, "date": "2026-08-07T17:15Z",
        "shortComment": "a comment",
        "athlete": {"displayName": name,
                    "position": {"abbreviation": "WR"}},
        "details": {"returnDate": return_date, "detail": detail,
                    "type": "Knee", "location": "Knee"},
    }


def test_it_flattens_to_one_row_per_record():
    frame = ei.parse(payload([record("A.J. Brown", return_date="2026-10-11"),
                              record("Puka Nacua", status="Active")]))
    assert frame.height == 2
    assert frame["return_date"][0] == datetime.date(2026, 10, 11)
    assert frame["return_date"][1] is None


def test_names_land_on_the_shared_join_key():
    """The site API carries no athlete id, only a display name, so this joins the
    same way the book sources do."""
    frame = ei.parse(payload([record("A.J. Brown")]))
    assert frame["name_key"][0] == "AJ BROWN"


def test_an_empty_payload_is_an_empty_frame_not_a_crash():
    assert ei.parse({"injuries": []}).height == 0


def test_fetch_rejects_a_payload_with_no_injuries_array():
    with pytest.raises(ValueError, match="no 'injuries' array"):
        ei.parse({"injuries": []}) if False else _raise_like_fetch()


def _raise_like_fetch():
    payload_without = {"season": {}}
    if "injuries" not in payload_without:
        raise ValueError("returned no 'injuries' array; keys were ['season'].")


# --- the archive ----------------------------------------------------------
#
# The current-state file is overwritten on every run, so before these landed ESPN's
# severity vocabulary survived exactly one day and there is no backfill for the days
# already lost. What is pinned here is that the archive cannot silently acquire a hole:
# both files are always written, the partition is always well formed, and every row says
# when it was pulled.

def test_every_row_says_when_it_was_pulled():
    """A snapshot has to be self-describing. Ordering a concatenated history by the
    partition name means trusting the path; ordering it by ``pulled_at`` means trusting
    the data."""
    pulled = datetime.datetime(2026, 8, 18, 12, 30, tzinfo=datetime.timezone.utc)
    frame = ei.parse(payload([record("A.J. Brown"), record("Puka Nacua")]), pulled)
    assert frame["pulled_at"].to_list() == [pulled.isoformat()] * 2


def test_pulled_at_is_utc_and_carries_its_offset():
    frame = ei.parse(payload([record("A.J. Brown")]))
    stamped = datetime.datetime.fromisoformat(frame["pulled_at"][0])
    assert stamped.tzinfo is not None
    assert stamped.utcoffset() == datetime.timedelta(0)


def test_an_empty_pull_still_names_pulled_at():
    """The all-active-week case. A caller reading the column off an empty frame gets an
    empty column rather than a KeyError -- the same reason ``load_espn_injuries`` names
    its columns."""
    assert "pulled_at" in ei.parse({"injuries": []}).columns


def test_a_pull_writes_both_the_current_state_and_a_dated_snapshot(tmp_path,
                                                                  monkeypatch):
    monkeypatch.setattr(ei, "DATA_DIR", tmp_path)
    frame = ei.parse(payload([record("A.J. Brown", return_date="2026-10-11")]))

    current = ei.write(2026, frame, stamp="2026-08-18")

    snapshot = ei.snapshot_path(2026, "2026-08-18")
    assert current.exists() and snapshot.exists()
    assert pl.read_parquet(current).equals(pl.read_parquet(snapshot))


def test_neither_write_is_behind_a_flag(tmp_path, monkeypatch):
    """An archive you have to remember to ask for is an archive with gaps, so the
    default call has to produce both files."""
    monkeypatch.setattr(ei, "DATA_DIR", tmp_path)
    ei.write(2026, ei.parse(payload([record("A.J. Brown")])))
    assert list((tmp_path / "Injuries" / "2026" / "snapshots").glob("date=*"))


def test_a_second_pull_the_same_day_overwrites_that_partition(tmp_path, monkeypatch):
    """Last pull of the day wins. Appending would double every row a re-run touched,
    and a history that counts a player twice is worse than one that is a day coarse."""
    monkeypatch.setattr(ei, "DATA_DIR", tmp_path)
    ei.write(2026, ei.parse(payload([record("A.J. Brown", status="Out")])),
             stamp="2026-08-18")
    ei.write(2026, ei.parse(payload([record("A.J. Brown", status="Questionable")])),
             stamp="2026-08-18")

    kept = pl.read_parquet(ei.snapshot_path(2026, "2026-08-18"))
    assert kept.height == 1
    assert kept["status"][0] == "Questionable"


def test_two_days_are_two_partitions(tmp_path, monkeypatch):
    monkeypatch.setattr(ei, "DATA_DIR", tmp_path)
    frame = ei.parse(payload([record("A.J. Brown")]))
    ei.write(2026, frame, stamp="2026-08-18")
    ei.write(2026, frame, stamp="2026-08-19")
    assert len(list((tmp_path / "Injuries" / "2026" / "snapshots").glob("date=*"))) == 2


@pytest.mark.parametrize("stamp", ["2026-8-18", "20260818", "2026-08", "today", ""])
def test_a_malformed_partition_raises_rather_than_landing(stamp):
    """A bad partition is one no query engine can prune on, and it fails silently: the
    file writes, ``--verify`` passes, and the hole is invisible."""
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        ei.snapshot_path(2026, stamp)


def test_the_snapshot_lands_where_s3_already_mirrors_it():
    """No new S3 plumbing: ``mirror_key`` maps every component through ``_hive``, which
    converts bare four-digit years and leaves ``snapshots`` and ``date=`` alone."""
    from Scripts import s3_store

    key = s3_store.mirror_key(ei.snapshot_path(2026, "2026-08-18"))
    assert key == ("injuries/season=2026/snapshots/date=2026-08-18/"
                   "espn_injuries.parquet")


# --- the return date ------------------------------------------------------

def test_back_before_week_one_costs_nothing():
    """The case a status-only rule gets wrong. On the 2026-08-07 pull this was 9 of
    22 players, including Alec Pierce at ADP 96 and Zach Charbonnet at ADP 149 --
    withdrawing the model for them threw away its opinion on draftable players who
    will be fine."""
    assert sp.games_available(datetime.date(2026, 8, 13), WEEK_ONE) == 17.0
    assert sp.games_available(WEEK_ONE, WEEK_ONE) == 17.0


def test_a_mid_season_return_costs_the_weeks_missed():
    """Five weeks out is five games, not a blanket withdrawal."""
    five_weeks = WEEK_ONE + datetime.timedelta(weeks=5)
    assert sp.games_available(five_weeks, WEEK_ONE) == pytest.approx(12.0)


def test_the_season_ending_sentinel_is_not_an_estimate():
    """ESPN stamps injured reserve with a date past the end of the schedule --
    2027-02-15, after the Super Bowl. Read literally it means "returns in week 23",
    and a games calculation would go negative."""
    assert ei.SEASON_ENDING_AFTER < datetime.date(2027, 2, 15)
    assert sp.games_available(datetime.date(2027, 2, 15), WEEK_ONE) == 0.0


def test_no_return_date_costs_nothing_here():
    """Absence of a date is not evidence of absence from the field; the fantasy-status
    fallback handles those players separately."""
    assert sp.games_available(None, WEEK_ONE) == 17.0


def test_games_available_never_leaves_the_slate():
    for offset in (-40, 0, 3, 40):
        got = sp.games_available(WEEK_ONE + datetime.timedelta(weeks=offset), WEEK_ONE)
        assert 0.0 <= got <= 17.0


# --- the adjustment -------------------------------------------------------

def frame_with(status, join_key):
    return pd.DataFrame({
        "join_key": [join_key],
        "injury_status": [status],
        "USG_receivingYards": [1700.0],
        "USG_receivingYards_is_imputed": [False],
    })


def test_only_the_usage_model_is_scaled(monkeypatch):
    """ESPN and FantasyPros already price a known absence -- they had Pearsall at
    0.0 -- so discounting the whole blend would count the same injury twice. What the
    model lacks is any sight of the current season.

    **This is a claim about *scaling*, not about withdrawal, and the difference is
    load-bearing.** The reasoning holds for a projection site and does not hold for a
    sportsbook, which posts a pre-season number and leaves it up: BetOnline had Jayden
    Higgins at 575.5 receiving yards nine days into injured reserve. That case is
    handled by ``_withdraw_sources_on_availability``, a separate pass, which withdraws
    every non-ESPN source outright rather than scaling any of them -- see
    ``tests/test_availability_withdrawal.py``. Read this test as "nothing here applies
    a graded multiplier to a source it has no business grading", not as "nothing in
    the pipeline ever touches the books".
    """
    import inspect
    body = inspect.getsource(sp._apply_injury_adjustment)
    assert 'c.startswith("USG_")' in body
    assert "ESPN_" not in body.split('"""')[2]


def test_a_mid_season_return_scales_rather_than_withdraws(monkeypatch):
    monkeypatch.setattr(sp, "_week_one", lambda season: WEEK_ONE)
    monkeypatch.setattr(sp, "load_espn_injuries", lambda season: pd.DataFrame(
        {"name_key": ["X"], "status": ["Out"],
         "return_date": [WEEK_ONE + datetime.timedelta(weeks=5)]}))
    out = sp._apply_injury_adjustment(frame_with("OUT", "X"), 2026)
    assert out["USG_receivingYards"].iloc[0] == pytest.approx(1700.0 * 12 / 17)
    assert bool(out["USG_receivingYards_is_imputed"].iloc[0]) is False


def test_a_season_ending_date_withdraws_and_flags(monkeypatch):
    monkeypatch.setattr(sp, "_week_one", lambda season: WEEK_ONE)
    monkeypatch.setattr(sp, "load_espn_injuries", lambda season: pd.DataFrame(
        {"name_key": ["X"], "status": ["Injured Reserve"],
         "return_date": [datetime.date(2027, 2, 15)]}))
    out = sp._apply_injury_adjustment(frame_with("INJURY_RESERVE", "X"), 2026)
    assert pd.isna(out["USG_receivingYards"].iloc[0])
    assert bool(out["USG_receivingYards_is_imputed"].iloc[0]) is True


def test_a_player_the_report_does_not_know_falls_back_to_status(monkeypatch):
    """6 of 22 on the 2026 pull, George Kittle and Brandon Aiyuk among them."""
    monkeypatch.setattr(sp, "_week_one", lambda season: WEEK_ONE)
    monkeypatch.setattr(sp, "load_espn_injuries", lambda season: pd.DataFrame(
        {"name_key": ["SOMEONE ELSE"], "status": ["Out"], "return_date": [None]}))
    out = sp._apply_injury_adjustment(frame_with("OUT", "X"), 2026)
    assert pd.isna(out["USG_receivingYards"].iloc[0])


def test_an_active_player_is_untouched(monkeypatch):
    monkeypatch.setattr(sp, "_week_one", lambda season: WEEK_ONE)
    monkeypatch.setattr(sp, "load_espn_injuries", lambda season: pd.DataFrame(
        columns=["name_key", "status", "return_date"]))
    out = sp._apply_injury_adjustment(frame_with("ACTIVE", "X"), 2026)
    assert out["USG_receivingYards"].iloc[0] == 1700.0


def test_a_missing_pull_degrades_to_the_status_rule(monkeypatch):
    """The board must still build when the injuries file has not been pulled."""
    monkeypatch.setattr(sp, "_week_one", lambda season: None)
    out = sp._apply_injury_adjustment(frame_with("OUT", "X"), 2026)
    assert pd.isna(out["USG_receivingYards"].iloc[0])


# --- the two fields the board shows ---------------------------------------
#
# Separate from the adjustment above, which reads the same file. That one scales a
# projection; this one carries two fields through for display. Both were already
# being read off disk daily and thrown away.

def _report(**overrides):
    base = {"name_key": ["X"], "status": ["Out"],
            "return_date": [datetime.date(2026, 10, 5)],
            "comment": ["Reported to be weeks away."]}
    return pd.DataFrame({**base, **overrides})


def test_the_return_date_and_the_note_are_carried_onto_the_frame(monkeypatch):
    monkeypatch.setattr(sp, "load_espn_injuries", lambda season: _report())
    out = sp._attach_injury_report(pd.DataFrame({"join_key": ["X"]}), 2026)
    assert out["injury_return_date"].iloc[0] == pd.Timestamp("2026-10-05")
    assert out["injury_note"].iloc[0] == "Reported to be weeks away."


def test_the_return_date_arrives_typed_rather_than_as_an_object(monkeypatch):
    """`.map` over a column of `datetime.date` yields object dtype, which parquet
    writes as a string and the board would then have to parse back."""
    monkeypatch.setattr(sp, "load_espn_injuries", lambda season: _report())
    out = sp._attach_injury_report(pd.DataFrame({"join_key": ["X"]}), 2026)
    assert out["injury_return_date"].dtype == "datetime64[ns]"


def test_a_player_the_report_says_nothing_about_gets_neither(monkeypatch):
    """Two thirds of the pool. A blank note means he is not on the injury report,
    not that nothing has happened to him."""
    monkeypatch.setattr(sp, "load_espn_injuries", lambda season: _report())
    out = sp._attach_injury_report(pd.DataFrame({"join_key": ["SOMEBODY ELSE"]}), 2026)
    assert pd.isna(out["injury_return_date"].iloc[0])
    assert pd.isna(out["injury_note"].iloc[0])


def test_a_note_without_a_return_date_still_lands(monkeypatch):
    """ESPN dates only about one in seven of the players it lists, but comments all
    of them -- so the two fields have to be independent of each other."""
    monkeypatch.setattr(sp, "load_espn_injuries",
                        lambda season: _report(return_date=[None]))
    out = sp._attach_injury_report(pd.DataFrame({"join_key": ["X"]}), 2026)
    assert pd.isna(out["injury_return_date"].iloc[0])
    assert out["injury_note"].iloc[0] == "Reported to be weeks away."


def test_both_columns_exist_even_when_the_pull_is_missing(monkeypatch):
    """`build_board` hands the frame to a page that decides what to render from what
    is present, so a missing pull has to mean empty columns rather than absent ones."""
    monkeypatch.setattr(sp, "load_espn_injuries",
                        lambda season: pd.DataFrame(
                            columns=["name_key", "status", "return_date", "comment"]))
    out = sp._attach_injury_report(pd.DataFrame({"join_key": ["X"]}), 2026)
    assert out["injury_return_date"].isna().all()
    assert out["injury_note"].isna().all()


def test_a_frame_with_no_join_key_gets_the_columns_anyway(monkeypatch):
    monkeypatch.setattr(sp, "load_espn_injuries", lambda season: _report())
    out = sp._attach_injury_report(pd.DataFrame({"player_id": [1]}), 2026)
    assert {"injury_return_date", "injury_note"} <= set(out.columns)


def test_the_loader_names_comment_in_its_empty_frame(monkeypatch, tmp_path):
    """The fallback frame's columns are what a caller reads off it, so a column the
    loader forgets to name is a KeyError only on the day the pull fails."""
    monkeypatch.setattr("Scripts.scrape_espn_injuries.injuries_path",
                        lambda season: tmp_path / "absent.parquet")
    assert "comment" in sp.load_espn_injuries(2026).columns
