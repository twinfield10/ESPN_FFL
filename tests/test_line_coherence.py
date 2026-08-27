"""Whether a blended line describes a football player, and a football team.

The check only became possible when volume entered the blend: before that
``TRUE_`` held yards and touchdowns and no attempts, and 1,584 passing yards on
116 attempts and 2,091 on 441 look equally reasonable until you divide.

Two things here are pinned because they were wrong on the first run and the
failure was invisible in the output:

* **A scrambling quarterback is not an incoherent running back.** Including QB in
  the yards-per-carry band flagged Mahomes, Mayfield and Murray every time, which
  teaches a reader to skip the line.
* **``pro_team`` is the string ``"None"`` for an unrostered player**, not a null.
  A ``notna()`` filter therefore leaves a 33rd team holding every free agent, and
  it lands at 0.5 plays a game.
"""

import pandas as pd
import pytest

from Scripts.season_projections import report_line_coherence


def qbs(rows):
    """A frame with the columns the passing band reads."""
    return pd.DataFrame({
        "player_name": [r["name"] for r in rows],
        "primaryPosition": ["QB"] * len(rows),
        "pro_team": [r.get("team", "KC") for r in rows],
        "TRUE_passingYards": [float(r["yards"]) for r in rows],
        "TRUE_passingAttempts": [float(r["attempts"]) for r in rows],
        "TRUE_rushingAttempts": [float(r.get("carries", 0.0)) for r in rows],
    })


def test_an_incoherent_line_is_named_and_a_plausible_one_is_not():
    """13.6 yards an attempt is two players' seasons stitched together."""
    text = report_line_coherence(qbs([
        {"name": "Plausible", "yards": 4200.0, "attempts": 600.0},
        {"name": "Stitched", "yards": 1584.0, "attempts": 116.0},
    ]))
    assert "1 outside 6.0-8.5" in text
    assert "Stitched 13.66" in text
    assert "Plausible" not in text


def test_a_thin_denominator_is_not_a_claim_about_efficiency():
    """A third-stringer's twelve projected attempts are noise, not incoherence."""
    text = report_line_coherence(qbs([
        {"name": "Starter", "yards": 4200.0, "attempts": 600.0},
        {"name": "Third string", "yards": 200.0, "attempts": 12.0},
    ]))
    assert "over 1 players" in text
    assert "Third string" not in text


def test_a_scrambling_quarterback_is_not_flagged_as_a_running_back():
    """QB is out of the yards-per-carry band on purpose; 6.3 a carry is correct for one."""
    from Scripts.season_projections import COHERENCE_BANDS

    carry_band = next(b for b in COHERENCE_BANDS if b[0] == "yards per carry")
    assert carry_band[3] == ("RB",)


def test_an_unrostered_player_is_not_a_thirty_third_team():
    """``pro_team`` of ``"None"`` is absence wearing a value, and pools free agents."""
    frame = qbs([
        {"name": "A", "yards": 4200.0, "attempts": 600.0, "carries": 400.0,
         "team": "KC"},
        {"name": "B", "yards": 40.0, "attempts": 6.0, "carries": 3.0,
         "team": "None"},
    ])
    text = report_line_coherence(frame)
    assert "over 1 teams" in text
    assert "0 outside" in text


def test_a_frame_with_no_volume_columns_returns_nothing_rather_than_raising():
    """A league store built before the change must not take a board down."""
    frame = pd.DataFrame({
        "player_name": ["A"], "primaryPosition": ["QB"], "pro_team": ["KC"],
        "TRUE_passingYards": [4200.0],
    })
    assert report_line_coherence(frame) == ""


def test_the_diagnostic_moves_no_number():
    """It reads and returns text; the frame it is handed comes back untouched."""
    frame = qbs([{"name": "A", "yards": 4200.0, "attempts": 600.0}])
    before = frame.copy(deep=True)
    report_line_coherence(frame)
    pd.testing.assert_frame_equal(frame, before)
