"""Guards on the parts of ``espn_api`` this pipeline reaches into directly.

The pipeline reads ``player.__dict__['breakdown']`` for raw per-week stats. That
is an upstream implementation detail, not a documented API, and it has already
changed meaning once:

* 0.45.1 assigned the **raw stat** breakdown to an attribute *named*
  ``points_breakdown`` (``box_player.py:33``, ``stats.get('breakdown', 0)``).
* 0.46.0 corrected the naming -- ``breakdown`` is the raw stats, and a new
  ``points_breakdown`` holds *applied points*.

Reading the old name under 0.46.0 silently substituted points for stats:
receiving yards of 225.0 became 22.5, the same figure at 0.1 pts/yd, across
~1,000 cells per league. Nothing raised. These tests make the next such rename
fail loudly instead.

See ``docs/plans/05-dependency-upgrades.md``.
"""

from importlib.metadata import version

import pytest

pytestmark = pytest.mark.filterwarnings("ignore")


def _tuple(v: str):
    return tuple(int(p) for p in v.split(".")[:3] if p.isdigit())


def test_espn_api_is_at_least_0_46_0():
    """``breakdown`` does not exist on BoxPlayer before 0.46.0."""
    assert _tuple(version("espn-api")) >= (0, 46, 0), (
        "Scripts/scrape_player_stats.py reads player.__dict__['breakdown'], "
        "which BoxPlayer only sets from 0.46.0 onward. On 0.45.1 the raw stats "
        "live under the misleading name 'points_breakdown'."
    )


def test_box_player_separates_raw_stats_from_applied_points():
    """The two attributes must stay distinct concepts, and keep these names.

    Built from a synthetic payload rather than a live fetch so this runs offline.
    ``stats`` is keyed by scoring period; ``breakdown`` carries stat ids mapped to
    names, ``points_breakdown`` the applied points for the same ids.
    """
    from espn_api.football.box_player import BoxPlayer

    data = {
        "playerPoolEntry": {
            "player": {
                "id": 1, "fullName": "Test Player", "proTeamId": 1,
                "defaultPositionId": 3, "eligibleSlots": [4],
                "injuryStatus": "ACTIVE", "injured": False,
                "stats": [{
                    # seasonId must match the year passed to BoxPlayer, or
                    # Player.__init__ skips the row entirely (player.py:46).
                    "seasonId": 2025,
                    "scoringPeriodId": 1,
                    "statSourceId": 0,
                    "statSplitTypeId": 1,
                    "appliedTotal": 22.5,
                    # 42 is receivingYards; 0.1 pts/yd gives the 22.5 above.
                    "stats": {"42": 225.0},
                    "appliedStats": {"42": 22.5},
                }],
            },
        },
        "lineupSlotId": 4,
    }
    bp = BoxPlayer(data, {}, [], 1, 2025)

    assert hasattr(bp, "breakdown"), "raw stat breakdown attribute was renamed"
    assert hasattr(bp, "points_breakdown"), "applied-points attribute was renamed"
    assert bp.breakdown.get("receivingYards") == 225.0, (
        "breakdown must carry the raw stat, not the applied points -- reading the "
        "wrong one silently swaps yards for points"
    )
    assert bp.points_breakdown.get("receivingYards") == 22.5


def test_the_pipeline_reads_the_raw_breakdown():
    """Pin the call site, so a well-meaning edit back to points_breakdown fails."""
    import inspect

    from Scripts import scrape_player_stats

    src = inspect.getsource(scrape_player_stats)
    assert "player.__dict__['breakdown']" in src
    assert "player.__dict__['points_breakdown']" not in src, (
        "points_breakdown holds applied points from espn-api 0.46.0 onward; the "
        "stat columns must come from ['breakdown']"
    )


def test_isolate_scoring_format_is_still_required():
    """0.46.0 did NOT fix the shared scoring dict, so the workaround must stay.

    ``Settings.__init__`` still does ``SETTINGS_SCORING_FORMAT_MAP.get(...)`` and
    then writes ``points`` onto the returned dict -- the module-level one, shared
    by every League in the process. If a future release fixes this, this test
    fails and ``fetch_utils.isolate_scoring_format`` can go.
    """
    import inspect

    import espn_api.football.settings as settings

    src = inspect.getsource(settings)
    assert "SETTINGS_SCORING_FORMAT_MAP.get(" in src and "scoring_type['points']" in src, (
        "espn_api no longer mutates the shared scoring-format dict. Re-check "
        "whether fetch_utils.isolate_scoring_format() is still needed."
    )


def test_the_per_slot_override_bug_is_still_upstream():
    """Documents why plan 11 reads mSettings directly rather than trusting the lib.

    0.46.0 still reads only slot '16' and still uses a falsy-or, so an override of
    exactly 0.0 falls through to the base value.
    """
    import inspect

    import espn_api.football.settings as settings

    src = inspect.getsource(settings)
    assert "pointsOverrides" in src and "points_override or " in src, (
        "espn_api's per-slot override handling changed. Re-check whether "
        "Scripts.scrape_player_stats.fetch_scoring_overrides is still needed."
    )
