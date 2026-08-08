"""The thin-evidence flag.

``USG_PosRankDelta`` reads the same whether the model is standing on nine seasons of
stable usage or extrapolating from four games at a new team. This names which.

Every condition here was chosen by measurement, and two obvious ones were rejected by
it. Median within-position rank error as a share of the position pool, 2019-2025
walk-forward, baseline 0.096:

    thin prior season (<8 games)   +42%
    changed teams                  +32%
    low prior volume (bottom q)    +23%
    no second prior season          -7%   <- not flagged
    rookie arm                     -14%   <- not flagged

The last two are why this is measured rather than asserted. Both look like thin
evidence and neither is; flagging rookies would have marked the model's strongest arm
as its weakest.

Synthetic frames. No network.
"""

import polars as pl
import pytest

from Scripts.usage import features as ft
from Scripts.usage import project as pj

LAG = ft.LAG1_PREFIX


def rows(records):
    default = {
        "usg_arm": "veteran", "position": "WR", "team_changed": False,
        f"{LAG}games": 17, f"{LAG}targets_pg": 8.0,
        f"{LAG}carries_pg": 0.0, f"{LAG}pass_attempts_pg": 0.0,
    }
    return pl.DataFrame([{**default, **r} for r in records])


def evidence(frame):
    return pj.attach_evidence(frame)["usg_evidence"].to_list()


# --- the three that measured ---------------------------------------------

def test_a_thin_prior_season_is_flagged():
    """The largest single error inflator: +42% on within-position rank error."""
    assert evidence(rows([{f"{LAG}games": 4}])) == ["thin prior season"]


def test_a_team_change_is_flagged():
    """Disagreement with ESPN is 55% larger for movers, and the model's prior volume
    was earned in a different offence."""
    assert evidence(rows([{"team_changed": True}])) == ["changed teams"]


def test_low_prior_volume_is_flagged():
    busy = [{f"{LAG}targets_pg": v} for v in (8.0, 9.0, 10.0, 11.0)]
    assert "low prior volume" in evidence(rows(busy + [{f"{LAG}targets_pg": 0.2}]))[-1]


def test_reasons_accumulate():
    got = evidence(rows([{f"{LAG}games": 3, "team_changed": True}]))[0]
    assert "thin prior season" in got and "changed teams" in got


def test_a_full_evidence_player_is_unflagged():
    assert evidence(rows([{}])) == [""]


def test_the_label_has_no_dangling_separator():
    """An empty string for a reason that does not apply survives the concat and
    leaves "thin prior season; ; " behind."""
    for got in evidence(rows([{f"{LAG}games": 3}, {"team_changed": True}, {}])):
        assert not got.startswith(";")
        assert not got.endswith(";")
        assert "; ;" not in got


# --- the two that did not, and must stay unflagged ------------------------

def test_a_rookie_is_never_flagged():
    """Measured 14% *better* than the pool. The rookie arm projects from draft
    capital at rho ~ 0.64 against ~0 for a guess carrying no draft information, so
    flagging it would invert the meaning of the column."""
    rookie = rows([{"usg_arm": "rookie", f"{LAG}games": None,
                    "team_changed": True, f"{LAG}targets_pg": None}])
    assert evidence(rookie) == [""]


def test_an_abstention_is_never_flagged():
    """It has no projection to qualify."""
    assert evidence(rows([{"usg_arm": "abstain", f"{LAG}games": None}])) == [""]


def test_only_the_measured_conditions_are_used():
    assert set(pj.THIN_EVIDENCE_REASONS) == {
        "thin prior season", "changed teams", "low prior volume"}
    assert pj.THIN_PRIOR_GAMES == 8


# --- the quantile population ---------------------------------------------

def test_the_low_volume_cut_ignores_players_with_no_prior_season():
    """The bug this guards. Rookies and abstentions carry no prior volume, which
    fills to 0.0 -- and there are enough of them that they *are* the bottom quartile,
    putting the cut at 0.0 and making the comparison unsatisfiable. The flag was
    silently never set on any board."""
    frame = rows(
        [{"usg_arm": "rookie", f"{LAG}targets_pg": None} for _ in range(12)]
        + [{f"{LAG}targets_pg": v} for v in (1.0, 6.0, 7.0, 8.0)])
    got = pj.attach_evidence(frame)
    veterans = got.filter(pl.col("usg_arm") == "veteran")
    assert veterans.filter(pl.col("usg_thin_evidence")).height >= 1


def test_the_boolean_agrees_with_the_label():
    got = pj.attach_evidence(rows([{f"{LAG}games": 3}, {}]))
    for label, flag in zip(got["usg_evidence"], got["usg_thin_evidence"]):
        assert bool(label) == bool(flag)
