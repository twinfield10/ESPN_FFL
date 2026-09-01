"""The availability gates: when one surviving source stops being the projection.

The defect these pin, measured on the 2026 board the morning they were written. Jayden
Higgins was on injured reserve for the season. ``inj_season_ending`` said so, ESPN
projected him at **0.0**, FantasyPros and Pinnacle had both dropped him and the usage
model had already been withdrawn -- and he showed **36.3** points at WR126 against
ESPN's WR198, because BetOnline's season file still carried 575.5 receiving yards and an
equal-vote blend renormalises onto whoever is left.

What is pinned here is mostly the *restraint*. Three gates fire and it is easy to write
them so that they also take Zach Charbonnet, who is out until October and whom ESPN
projects at 134, or Troy Franklin, whom two books like and ESPN does not. A gate that
cannot be disagreed with is a gate that has replaced the board with ESPN's.

Synthetic frames. No network, no disk.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

from Scripts import season_projections as sp
from Scripts.injury import transfer as tr
from Scripts.projection_utils import IMPUTED_SUFFIX


def frame(rows):
    """A minimal board frame carrying one stat per source plus the gate inputs."""
    built = []
    for row in rows:
        entry = {
            "player_name": row["name"],
            "primaryPosition": row.get("position", "WR"),
            "ESPN_projected_total": row["espn"],
            "injury_status": row.get("status", "ACTIVE"),
            "inj_season_ending": row.get("season_ending", False),
            "FP_receivingYards": row.get("fp"),
            "PINNY_receivingYards": row.get("pinny"),
            "BOL_receivingYards": row.get("bol"),
            "USG_receivingYards": row.get("usg"),
            f"USG_receivingYards{IMPUTED_SUFFIX}": row.get("usg") is None,
        }
        built.append(entry)
    out = pd.DataFrame(built)
    out["inj_season_ending"] = out["inj_season_ending"].astype("boolean")
    return out


def run(rows):
    return sp._withdraw_sources_on_availability(frame(rows))


def withdrawn(out, name):
    """Whether every non-ESPN source is gone for one player."""
    row = out.loc[out["player_name"] == name].iloc[0]
    return all(pd.isna(row[c]) for c in
               ("FP_receivingYards", "PINNY_receivingYards",
                "BOL_receivingYards", "USG_receivingYards"))


def evidence(out, name):
    return out.loc[out["player_name"] == name, sp.AVAIL_EVIDENCE_COLUMN].iloc[0]


# --- gate A: the season-ender ---------------------------------------------

def test_a_season_ender_loses_every_source_not_just_the_model():
    """Higgins. The whole reason this module exists: four sources abstained
    correctly and the fifth, a sportsbook that had not taken its market down, became
    100% of the projection."""
    out = run([{"name": "Higgins", "espn": 0.0, "status": "INJURY_RESERVE",
                "season_ending": True, "bol": 575.5}])
    assert withdrawn(out, "Higgins")
    assert evidence(out, "Higgins") == sp.AVAIL_SEASON_ENDING


def test_the_season_ender_gate_sets_the_imputed_flag_rather_than_writing_a_zero():
    """Null-and-flag, the idiom `_withdraw_usage_on_role` already uses. A zero would
    enter the blend as an opinion and read as agreement; an abstention drops the
    weight and lets `sources_real` count honestly."""
    out = run([{"name": "Higgins", "espn": 0.0, "season_ending": True, "usg": 900.0}])
    assert bool(out[f"USG_receivingYards{IMPUTED_SUFFIX}"].iloc[0]) is True


def test_a_season_ender_espn_still_prices_is_named_and_still_withdrawn(capsys):
    """ESPN's two feeds contradicting each other -- the fantasy projection against the
    site API's return date. The date is the harder evidence, so the gate fires; but a
    human should learn it happened rather than have it resolved silently."""
    out = run([{"name": "Odd", "espn": 140.0, "season_ending": True, "bol": 500.0}])
    assert withdrawn(out, "Odd")
    assert "season-ender" in capsys.readouterr().out


# --- gate B: ESPN declined, and he is out ---------------------------------

def test_espn_pricing_zero_on_an_out_player_withdraws_the_rest():
    """James Conner: on injured reserve, ESPN at 0.0, and FantasyPros alone carrying
    44.2 points."""
    out = run([{"name": "Conner", "espn": 0.0, "status": "INJURY_RESERVE",
                "position": "RB", "fp": 700.0, "bol": 650.0}])
    assert withdrawn(out, "Conner")
    assert evidence(out, "Conner") == sp.AVAIL_ESPN_ZERO_AND_OUT


@pytest.mark.parametrize("name,espn", [("Charbonnet", 134.0), ("Tyson", 99.8),
                                       ("Savion", 41.9)])
def test_an_out_player_espn_still_projects_keeps_his_line(name, espn):
    """The three of 62 OUT/IR players ESPN priced above zero on the 2026 board. Keying
    the gate on the status alone would have zeroed all three -- Charbonnet is out until
    week 5 and worth a pick."""
    out = run([{"name": name, "espn": espn, "status": "OUT", "fp": 700.0}])
    assert not withdrawn(out, name)
    assert pd.isna(evidence(out, name))


def test_questionable_is_not_out():
    """Pre-season QUESTIONABLE is week-to-week noise on 115 players. Same exclusion
    `INJURY_ABSTAIN_STATUSES` already makes for the usage adjustment."""
    out = run([{"name": "Q", "espn": 0.0, "status": "QUESTIONABLE",
                "fp": 700.0, "bol": 650.0}])
    assert not withdrawn(out, "Q")


# --- gate C: ESPN declined, and one source is left ------------------------

def test_espn_zero_with_a_single_surviving_source_withdraws():
    """AJ Dillon: ACTIVE, ESPN 0.0, FantasyPros alone, 55.5 points. Same defect as
    Higgins with a role trigger instead of an injury one."""
    out = run([{"name": "Dillon", "espn": 0.0, "position": "RB", "fp": 700.0}])
    assert withdrawn(out, "Dillon")
    assert evidence(out, "Dillon") == sp.AVAIL_ESPN_ZERO_LONE_SOURCE


def test_two_surviving_sources_against_an_espn_zero_are_left_alone():
    """The restraint clause. Two independent sources agreeing against ESPN is a
    disagreement, and a board that cannot disagree with ESPN is not worth building."""
    out = run([{"name": "Franklin", "espn": 0.0, "fp": 700.0, "bol": 650.0}])
    assert not withdrawn(out, "Franklin")


def test_a_lone_source_is_kept_where_espn_prices_him():
    """Emari Demercado -- one source, but ESPN priced him at 24.6, so nothing here
    has any business overruling the blend."""
    out = run([{"name": "Demercado", "espn": 24.6, "position": "RB", "fp": 300.0}])
    assert not withdrawn(out, "Demercado")


def test_a_withdrawn_usage_line_does_not_count_as_a_surviving_source():
    """`_apply_injury_adjustment` nulls `USG_` and flags it. Counting the flag rather
    than only the value is what stops a player whose model was already withdrawn from
    reading as two sources and escaping gate C."""
    out = run([{"name": "One", "espn": 0.0, "fp": 700.0, "usg": None}])
    assert withdrawn(out, "One")


# --- the things it must not do --------------------------------------------

def test_espn_columns_are_never_touched():
    """ESPN carries no `_is_imputed` columns, so `compute_weighted_stats` always
    counts it as real and `fillna(0.0)` makes its missing stat a zero vote. That vote
    is what resolves the blend to zero once everyone else is gone -- withdraw it too
    and the denominator collapses to the face-value fallback."""
    before = frame([{"name": "Higgins", "espn": 0.0, "season_ending": True,
                     "bol": 575.5}])
    after = sp._withdraw_sources_on_availability(before.copy())
    pd.testing.assert_series_equal(after["ESPN_projected_total"],
                                   before["ESPN_projected_total"])


def test_a_healthy_well_covered_player_is_untouched():
    out = run([{"name": "Nacua", "espn": 352.0, "fp": 1500.0, "pinny": 1480.0,
                "bol": 1510.0, "usg": 1495.0}])
    assert not withdrawn(out, "Nacua")
    assert pd.isna(evidence(out, "Nacua"))


def test_a_frame_with_no_gate_columns_survives():
    """Same degrade-rather-than-fail contract every other attacher in the pipeline
    has: a board must still build when the injury pull has not run."""
    bare = pd.DataFrame({"player_name": ["X"], "FP_receivingYards": [700.0]})
    out = sp._withdraw_sources_on_availability(bare)
    assert out["FP_receivingYards"].iloc[0] == 700.0


# --- the vacancy transfer must not pay a man who cannot play --------------

def room(evidence_values):
    """An RB room: a hurt lead at rank 1 and two backups behind him."""
    return pd.DataFrame({
        "player_name": ["Lead", "Second", "Third"],
        "pro_team": ["ARI", "ARI", "ARI"],
        "primaryPosition": ["RB", "RB", "RB"],
        "usg_depth_rank": [1.0, 2.0, 3.0],
        "inj_expected_absence_weeks": [6.0, 0.0, 0.0],
        "usg_healthy_rushingYards": [1200.0, 0.0, 0.0],
        "TRUE_rushingYards": [700.0, 100.0, 50.0],
        sp.AVAIL_EVIDENCE_COLUMN: evidence_values,
    })


def test_an_injured_backup_does_not_inherit_the_vacancy():
    """James Conner inherited **118.69** points of his own room's vacated work while
    himself on injured reserve behind Jeremiyah Love."""
    out = tr.redistribute(room([None, sp.AVAIL_ESPN_ZERO_AND_OUT, None]))
    assert out.loc[1, tr.INHERITED_COLUMN] == 0
    assert out.loc[2, tr.INHERITED_COLUMN] > 0


def test_an_available_backup_still_inherits():
    out = tr.redistribute(room([None, None, None]))
    assert out.loc[1, tr.INHERITED_COLUMN] > 0


def test_a_board_without_the_gate_column_redistributes_as_before():
    """Boards built before the gates landed carry no `avail_evidence`, and the
    transfer must not start refusing to pay anyone because of it."""
    without = room([None, None, None]).drop(columns=[sp.AVAIL_EVIDENCE_COLUMN])
    out = tr.redistribute(without)
    assert out.loc[1, tr.INHERITED_COLUMN] > 0


# --- the app's copies of the marker strings -------------------------------

def test_the_app_renders_every_marker_the_pipeline_writes():
    """`app.draft_view` duplicates these literals rather than importing them -- it is
    loaded by a process that only reads parquet. Pinned equal here, the same way
    `EVIDENCE_ROLE_MARKER` is."""
    # `app/` is not a package -- Streamlit runs the page scripts directly with app/
    # on sys.path, so they import each other by bare name. Same shim as
    # `test_draft_view`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
    import draft_view as dv

    assert set(dv.AVAILABILITY_MARKERS) == {
        sp.AVAIL_SEASON_ENDING,
        sp.AVAIL_ESPN_ZERO_AND_OUT,
        sp.AVAIL_ESPN_ZERO_LONE_SOURCE,
    }
