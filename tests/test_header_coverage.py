"""Which coverage numbers the sidebar shows for the selected week."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from components import header  # noqa: E402


META = {"coverage": {
    "players": {"ESPN": 100.0, "SHARP": 8.6},
    "population": {"rows": 900, "rostered": 600, "free_agents_per_position": 20},
    "by_week": {"6": {"players": {"ESPN": 100.0, "SHARP": 81.0},
                      "rows": 240, "rostered": 100, "free_agents": 140}},
}}


def test_the_selected_week_gets_its_own_numbers():
    shown, population, week = header.coverage_for_week(META, 6)
    assert week == 6
    assert shown["SHARP"] == 81.0
    assert population["rows"] == 240
    assert population["free_agents_per_position"] == 20   # carried from season-wide


def test_a_week_the_store_does_not_hold_falls_back_to_the_season():
    shown, population, week = header.coverage_for_week(META, 3)
    assert week is None
    assert shown["SHARP"] == 8.6
    assert population["rows"] == 900


def test_a_store_built_before_by_week_falls_back_to_the_season():
    old = {"coverage": {k: v for k, v in META["coverage"].items() if k != "by_week"}}
    shown, _, week = header.coverage_for_week(old, 6)
    assert week is None
    assert shown["SHARP"] == 8.6
