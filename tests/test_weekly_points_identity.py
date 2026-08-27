"""The weekly path's points identity, and the three defects that hid behind a patch.

``TRUE_Points == score(TRUE_ stat line)`` is the property that lets one pipeline
serve nine leagues with different scoring. The weekly path broke it for years with
a scalar named ``adjustment`` -- ESPN's ``projPoints`` minus our scoring of ESPN's
own stat line -- added to *every* source's total, ``TRUE_Points`` included.

Removing it exposed three things that had been invisible underneath, and each has
a test here:

* ESPN's doubled yardage escaping a correction that compared it against a
  FantasyPros line that was usually not there yet.
* Six scored milestone-bonus rules in one league that are mapped, present, and
  identically zero all season.
* ``USG_Points``, written null for all 3,602 rows of every 2025 store, because the
  scorer's default prefix list is the season one.
"""

import pandas as pd
import pytest

from Scripts import projection_utils as pu


def scoring(rules):
    """A scoring table.

    Args:
        rules: ``{colName: points}``.

    Returns:
        pd.DataFrame: ``colName`` and ``points``.
    """
    return pd.DataFrame({"colName": list(rules), "points": list(rules.values())})


PPR = scoring({"receivingYards": 0.1, "receivingReceptions": 1.0,
               "receivingTouchdowns": 6.0})


# --- the doubling correction ---------------------------------------------

def test_doubled_yardage_is_caught_with_no_fantasypros_line_at_all():
    """The case the old rule could not see, and the reason it was rewritten.

    The previous test was ``ESPN > FP * 1.75``, evaluated before the FantasyPros
    imputation ran. With no FantasyPros line the comparison was against NaN, came
    out False, and the doubled value went straight to the store.
    """
    frame = pd.DataFrame({
        "ESPN_receivingYards": [136.28],
        "ESPN_receivingReceptions": [5.49],
        "ESPN_receivingTouchdowns": [0.434],
        "projPoints": [11.7],          # consistent with ~68 yards, not 136
    })
    out, halved = pu.halve_doubled_espn_yardage(frame, PPR)
    assert halved == 1
    assert out["ESPN_receivingYards"][0] == pytest.approx(68.14)
    # Receptions and touchdowns are untouched: the bug is yardage, not the line.
    assert out["ESPN_receivingReceptions"][0] == pytest.approx(5.49)


def test_a_line_that_agrees_with_espns_own_total_is_left_alone():
    """Halving must not fire just because a projection is large."""
    frame = pd.DataFrame({
        "ESPN_receivingYards": [136.28],
        "ESPN_receivingReceptions": [5.49],
        "ESPN_receivingTouchdowns": [0.434],
        "projPoints": [21.7],
    })
    out, halved = pu.halve_doubled_espn_yardage(frame, PPR)
    assert halved == 0
    assert out["ESPN_receivingYards"][0] == pytest.approx(136.28)


def test_a_line_below_espns_own_total_is_never_halved():
    """Under-scoring is a different defect and halving would double it."""
    frame = pd.DataFrame({
        "ESPN_receivingYards": [60.0], "ESPN_receivingReceptions": [4.0],
        "ESPN_receivingTouchdowns": [0.3], "projPoints": [30.0],
    })
    _, halved = pu.halve_doubled_espn_yardage(frame, PPR)
    assert halved == 0


def test_a_frame_without_espns_own_points_degrades_rather_than_raising():
    """Not every caller has `projPoints`; a missing reference is not an error."""
    frame = pd.DataFrame({"ESPN_receivingYards": [136.28]})
    out, halved = pu.halve_doubled_espn_yardage(frame, PPR)
    assert halved == 0
    assert out["ESPN_receivingYards"][0] == pytest.approx(136.28)


# --- the silently-zero stats ---------------------------------------------

def test_a_mapped_scored_stat_that_is_always_zero_is_reported():
    """Plan 01 catches an unmapped rule. This is the case that slips past it.

    john_pc_league scores six yardage-milestone bonuses. All six are mapped, all
    six have a column, and all six are zero for all 3,095 player-weeks -- in the
    actuals as well as the projections -- so the name read here is not the name
    ESPN's breakdown uses.
    """
    table = scoring({"receivingYards": 0.1, "receivingYards100-199Game": 3.0})
    frame = pd.DataFrame({
        "ESPN_receivingYards": [60.0, 80.0],
        "ESPN_receivingYards100-199Game": [0.0, 0.0],
    })
    assert pu.report_silent_zero_stats(frame, table) == ["receivingYards100-199Game"]


def test_a_rule_worth_nothing_is_not_reported_as_silent():
    """A 0.0-point rule contributing zero is correct, not a gap."""
    table = scoring({"kickoffReturnYards": 0.0})
    frame = pd.DataFrame({"ESPN_kickoffReturnYards": [0.0, 0.0]})
    assert pu.report_silent_zero_stats(frame, table) == []


# --- the phantom source --------------------------------------------------

def test_a_prefix_with_only_a_points_column_is_not_a_source():
    """``USG_Points`` null for every row read as a source that agreed."""
    frame = pd.DataFrame({
        "ESPN_receivingYards": [60.0],
        "USG_Points": [None],
        "TRUE_receivingYards": [60.0],
    })
    assert pu.present_prefixes(frame, ("ESPN", "USG", "TRUE")) == ["ESPN", "TRUE"]


def test_the_weekly_prefix_list_excludes_the_season_only_sources():
    """TOMCAT, the kicker arm and the defence arm have no weekly stat line."""
    assert set(pu.WEEKLY_PREFIXES).isdisjoint({"USG", "KIK", "DST"})


# --- the identity --------------------------------------------------------

def test_points_are_the_stat_line_scored_and_nothing_else():
    """The property the removed patch broke, stated directly."""
    frame = pd.DataFrame({
        "primaryPosition": ["WR"],
        "ESPN_receivingYards": [80.0], "ESPN_receivingReceptions": [5.0],
        "ESPN_receivingTouchdowns": [0.5],
        "TRUE_receivingYards": [70.0], "TRUE_receivingReceptions": [4.5],
        "TRUE_receivingTouchdowns": [0.4],
        "projPoints": [99.0],       # a wildly different ESPN total changes nothing
    })
    pu._apply_scoring(frame, PPR, ["ESPN", "TRUE"])
    assert frame["TRUE_Points"][0] == pytest.approx(70.0 * 0.1 + 4.5 + 0.4 * 6.0)
    assert frame["ESPN_Points"][0] == pytest.approx(80.0 * 0.1 + 5.0 + 0.5 * 6.0)


# --- the column-name trap that silently disabled the correction ----------

def test_change_col_prefix_would_mangle_espns_published_points():
    """Why the reference column is renamed before the prefix swap, not after.

    ``change_col_prefix`` replaces the *substring* ``proj``, not a prefix, so
    ``projPoints`` comes out as ``ESPNPoints`` -- no underscore, and no longer the
    name the correction looks for. It would then find nothing, return zero rows
    halved, and report success. This caught exactly that during the change.
    """
    frame = pd.DataFrame({"proj_receivingYards": [1.0], "projPoints": [2.0]})
    swapped = pu.change_col_prefix(frame, "proj", "ESPN")
    assert list(swapped.columns) == ["ESPN_receivingYards", "ESPNPoints"]

    # And the replacement name is chosen to survive it.
    assert "proj" not in pu.ESPN_PUBLISHED_POINTS
    safe = pu.change_col_prefix(
        pd.DataFrame({"proj_receivingYards": [1.0], pu.ESPN_PUBLISHED_POINTS: [2.0]}),
        "proj", "ESPN")
    assert pu.ESPN_PUBLISHED_POINTS in safe.columns


def test_the_correction_fires_through_the_real_rename_sequence():
    """End to end over the naming `clean_lineups` actually performs."""
    raw = pd.DataFrame({
        "proj_receivingYards": [136.28],
        "proj_receivingReceptions": [5.49],
        "proj_receivingTouchdowns": [0.434],
        "projPoints": [11.7],
    })
    renamed = raw.rename(columns={"projPoints": pu.ESPN_PUBLISHED_POINTS})
    espn = pu.change_col_prefix(renamed, "proj", "ESPN")
    out, halved = pu.halve_doubled_espn_yardage(
        espn, PPR, projected_points=pu.ESPN_PUBLISHED_POINTS)
    assert halved == 1
    assert out["ESPN_receivingYards"][0] == pytest.approx(68.14)
