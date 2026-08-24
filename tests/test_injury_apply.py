"""Attaching the injury model to a projection frame.

Diagnostics only at this phase -- nothing here multiplies a projection, and the tests that
matter most are the ones pinning that it stays that way, plus the degradation paths. A
board that will not build is worse than a board with an empty column, so every missing
input has to end in blank columns rather than an exception.

The dtype test exists because a real board failed on it: ``pd.Series(pd.NA, dtype="float64")``
raises ``TypeError: float() argument must be a string or a real number, not 'NAType'``, and
nothing caught it until nine leagues would not build.

Synthetic frames. No parquet, no network.
"""

import datetime

import pandas as pd
import pytest

from Scripts.injury import apply as ia

WEEK_ONE = datetime.date(2026, 9, 10)


def board(rows=None):
    rows = rows or [("JEREMIYAH LOVE", "4870808", "RB"),
                    ("JAHMYR GIBBS", "4426502", "RB")]
    return pd.DataFrame({
        "join_key": [r[0] for r in rows],
        "player_id": [r[1] for r in rows],
        "primaryPosition": [r[2] for r in rows],
        "ESPN_receivingYards": [500.0] * len(rows),
        "TRUE_receivingYards": [500.0] * len(rows),
        "TRUE_Points": [200.0] * len(rows),
    })


def report(rows):
    return pd.DataFrame(rows)


# --- it does not move a projection ---------------------------------------

def test_no_projection_column_changes():
    """The whole phase-3 contract. A multiplier waits on the fitted curve clearing its
    gates, and until then this layer is allowed to add columns and nothing else."""
    frame = board()
    before = frame.copy()

    after = ia.attach_severity(frame, 2026, week_one=WEEK_ONE,
                               report=report([{"name_key": "JEREMIYAH LOVE",
                                               "status": "Out", "injury_type": "Ankle",
                                               "injury_detail": "Sprain",
                                               "return_date": None, "comment": None}]),
                               overrides={})

    for column in before.columns:
        pd.testing.assert_series_equal(after[column], before[column])


def test_it_adds_every_column_it_promises():
    after = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    assert set(ia.INJURY_COLUMNS) <= set(after.columns)


# --- dtypes --------------------------------------------------------------

def test_the_numeric_columns_are_real_floats_not_masked_nulls():
    """A real board failed here. ``pd.Series(pd.NA, index=..., dtype="float64")`` raises,
    because pandas tries ``float(pd.NA)`` to fill the numpy array -- it has to be a nan."""
    after = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    assert after["inj_expected_absence_weeks"].dtype == "float64"
    assert after["inj_season_ending"].dtype == "boolean"


def test_a_frame_of_healthy_players_still_types_correctly():
    after = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    assert after["inj_severity_source"].isna().all()
    assert (after["inj_evidence"] == ia.NO_INJURY_EVIDENCE).all()


# --- healthy is not the same as unknown ----------------------------------

def test_a_player_the_report_does_not_know_gets_no_severity_and_no_abstention():
    """Roughly 600 of a 1,000-row board are not on the injury report at all. Marking those
    "no severity evidence" would put an abstention on most of the table and make the flag
    meaningless."""
    after = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    assert after["inj_severity_source"].isna().all()


def test_an_active_player_with_unreadable_news_reads_as_healthy_not_unresolved():
    """686 of 800 records are Active and most of their comments are ordinary news. This is
    the same failure ``usg_evidence`` was created to fix -- one blank column standing for
    three different states, all of which looked like agreement."""
    after = ia.attach_severity(
        board(), 2026, week_one=WEEK_ONE,
        report=report([{"name_key": "JAHMYR GIBBS", "status": "Active",
                        "injury_type": None, "injury_detail": None,
                        "return_date": None,
                        "comment": "Gibbs signed a contract extension Tuesday."}]),
        overrides={})
    row = after.loc[after["join_key"] == "JAHMYR GIBBS"].iloc[0]
    assert pd.isna(row["inj_severity_source"])


def test_a_player_listed_out_with_no_readable_severity_does_abstain():
    """He is hurt and nothing here can say how badly. That is the row where the override
    file earns its keep, and it has to be visible."""
    after = ia.attach_severity(
        board(), 2026, week_one=WEEK_ONE,
        report=report([{"name_key": "JAHMYR GIBBS", "status": "Out",
                        "injury_type": "Undisclosed", "injury_detail": "Not Specified",
                        "return_date": None, "comment": None}]),
        overrides={})
    row = after.loc[after["join_key"] == "JAHMYR GIBBS"].iloc[0]
    assert row["inj_severity_source"] == "none"
    assert row["inj_evidence"].startswith("abstain:")


# --- degradation ---------------------------------------------------------

def test_a_frame_with_no_join_key_gets_the_columns_anyway():
    frame = board().drop(columns=["join_key"])
    after = ia.attach_severity(frame, 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    assert set(ia.INJURY_COLUMNS) <= set(after.columns)


def test_a_malformed_override_file_is_reported_and_skipped(tmp_path, monkeypatch,
                                                          capsys):
    """A human error worth surfacing loudly, but not one that should stop nine boards from
    building."""
    from Scripts.injury import severity as sv

    monkeypatch.setattr(sv.paths, "INJURY_OVERRIDES_DIR", tmp_path)
    path = sv.overrides_path(2026, create=True)
    path.write_text("players:\n  - espn_id: 1\n    body_part: anke\n"
                    "    weeks_out: 4\n    as_of: 2026-08-18\n    source: x\n")

    after = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame())
    assert "ignored" in capsys.readouterr().out
    assert set(ia.INJURY_COLUMNS) <= set(after.columns)


def test_an_empty_frame_does_not_raise():
    empty = board().iloc[0:0]
    after = ia.attach_severity(empty, 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    assert after.empty
    assert set(ia.INJURY_COLUMNS) <= set(after.columns)


# --- the override reaches the frame --------------------------------------

def test_an_override_is_joined_on_the_espn_id():
    """Preferred over the name because the ESPN name join is the most fragile in the repo,
    and an override is precisely where a silent miss is most expensive."""
    overrides = {"4870808": {"_group": "ankle", "_part": "ankle_high", "_low": 4.0,
                             "_high": 6.0, "_where": "test", "source": "beat report",
                             "note": "high ankle", "espn_id": 4870808}}
    after = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides=overrides)
    row = after.loc[after["join_key"] == "JEREMIYAH LOVE"].iloc[0]
    assert row["inj_severity_source"] == "override"
    assert row["inj_expected_absence_weeks"] == 5.0
    assert row["inj_duration_bucket"] == "5+"


def test_an_override_fires_for_a_player_the_report_calls_active():
    """The Love case end to end: no report row would otherwise reach him."""
    overrides = {"JEREMIYAH LOVE": {"_group": "ankle", "_part": "ankle_high",
                                    "_low": 4.0, "_high": 6.0, "_where": "test",
                                    "source": "beat report", "note": "high ankle",
                                    "name_key": "JEREMIYAH LOVE"}}
    after = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides=overrides)
    row = after.loc[after["join_key"] == "JEREMIYAH LOVE"].iloc[0]
    assert row["inj_severity_source"] == "override"


# --- the build log -------------------------------------------------------

def test_the_summary_counts_each_rung_separately():
    """Reported per rung because the interesting number is how often the *weak* rungs are
    carrying the answer. A board where most severities came from free text is a board where
    the override file is the highest-value thing to edit."""
    after = ia.attach_severity(
        board(), 2026, week_one=WEEK_ONE,
        report=report([{"name_key": "JEREMIYAH LOVE", "status": "Out",
                        "injury_type": "Knee - ACL", "injury_detail": "Surgery",
                        "return_date": None, "comment": None}]),
        overrides={})
    line = ia.summary(after)
    assert "espn_structured=1" in line
    assert "out for the season" in line


def test_the_summary_says_so_when_nothing_is_attached():
    assert "not attached" in ia.summary(board())


# --- the fitted model's diagnostics --------------------------------------
#
# Rejected as a multiplier, shipped as columns. These pin that the distinction holds.

def diagnosed(**kwargs):
    """A frame with severity already attached, then diagnostics on top."""
    from Scripts.injury import severity as sv

    overrides = {"4870808": {"_group": kwargs.get("part", "knee"),
                             "_part": kwargs.get("part", "knee"),
                             "_low": kwargs.get("weeks", 3.0),
                             "_high": kwargs.get("weeks", 3.0), "_where": "test",
                             "source": "t", "note": "t", "espn_id": 4870808}}
    frame = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides=overrides)
    assert sv.resolve({"espn_id": "4870808"}, overrides=overrides).source == "override"
    return ia.attach_model_diagnostics(frame)


def test_the_diagnostics_do_not_move_a_projection_either():
    """Compared over the projection columns specifically. The severity columns are compared
    in their own test above, and comparing an all-null object column against an all-null
    float one is a pandas dtype question rather than a statement about projections."""
    frame = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    projections = [c for c in frame.columns if not c.startswith("inj_")]
    before = frame[projections].copy()
    after = ia.attach_model_diagnostics(frame)
    pd.testing.assert_frame_equal(after[projections], before)


def test_a_hurt_player_gets_a_ladder_a_cost_and_a_recurrence_risk():
    after = diagnosed(part="knee", weeks=3.0)
    row = after.loc[after["join_key"] == "JEREMIYAH LOVE"].iloc[0]
    assert row["inj_recovery_ladder"]
    assert row["inj_recovery_cost"] > 0
    assert 0.0 < row["inj_reinjury_prob"] < 1.0


def test_a_healthy_player_gets_none_of_them():
    after = diagnosed()
    row = after.loc[after["join_key"] == "JAHMYR GIBBS"].iloc[0]
    assert row["inj_recovery_ladder"] is None
    assert pd.isna(row["inj_recovery_cost"])
    assert pd.isna(row["inj_reinjury_prob"])


def test_the_cost_is_the_ladder_summed_so_it_can_be_checked():
    """Tolerance is the rounding, not slack: the ladder is printed to two decimals so a
    reader can check the cost against it, and six values each rounded by up to 0.005 cannot
    reproduce the full-precision sum exactly."""
    after = diagnosed(part="foot_toe", weeks=3.0)
    row = after.loc[after["join_key"] == "JEREMIYAH LOVE"].iloc[0]
    ramp = [float(x) for x in row["inj_recovery_ladder"].split()]
    assert row["inj_recovery_cost"] == pytest.approx(sum(1.0 - x for x in ramp),
                                                    abs=0.005 * len(ramp))


def test_an_abstaining_body_part_costs_nothing_and_says_so_with_a_flat_ladder():
    """Concussion abstains on its own evidence, so the ladder is all ones and the cost is
    zero -- distinguishable from a missing reading, which is null."""
    after = diagnosed(part="concussion", weeks=1.0)
    row = after.loc[after["join_key"] == "JEREMIYAH LOVE"].iloc[0]
    assert row["inj_recovery_cost"] == pytest.approx(0.0)
    assert set(row["inj_recovery_ladder"].split()) == {"1.00"}


def test_a_worse_body_part_costs_more_form():
    knee = diagnosed(part="knee", weeks=3.0)
    hamstring = diagnosed(part="hamstring", weeks=3.0)
    cost = lambda f: f.loc[f["join_key"] == "JEREMIYAH LOVE", "inj_recovery_cost"].iloc[0]
    assert cost(knee) > cost(hamstring)


def test_a_missing_model_artifact_leaves_the_columns_blank_rather_than_failing(
        monkeypatch, capsys, tmp_path):
    """A board that will not build is worse than a board with an empty column."""
    from Scripts.injury import model as im

    monkeypatch.setattr(im.InjuryModel, "default_path",
                        classmethod(lambda cls, version=None: tmp_path / "absent.json"))
    frame = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    after = ia.attach_model_diagnostics(frame)
    assert "not fitted" in capsys.readouterr().out
    assert after["inj_recovery_cost"].isna().all()


def test_every_promised_column_survives_the_diagnostics_pass():
    frame = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    after = ia.attach_model_diagnostics(frame)
    assert set(ia.INJURY_COLUMNS) <= set(after.columns)


def test_a_stale_model_says_so(capsys):
    """``InjuryModel.is_stale`` existed with nothing calling it, which is the same as not
    having it. Its sibling in ``usage/project.py`` caught a real artifact trained a season
    short; an injury curve fitted before a season was played fails more quietly, because
    every number still renders."""
    from Scripts.injury import model as im

    model = im.InjuryModel.load()
    frame = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    ia.attach_model_diagnostics(frame, model=model, season=2030)
    assert "trained through" in capsys.readouterr().out


def test_a_current_model_stays_quiet(capsys):
    from Scripts.injury import model as im

    model = im.InjuryModel.load()
    frame = ia.attach_severity(board(), 2026, week_one=WEEK_ONE,
                               report=pd.DataFrame(), overrides={})
    ia.attach_model_diagnostics(frame, model=model,
                               season=max(model.train_seasons) + 1)
    assert "trained through" not in capsys.readouterr().out
