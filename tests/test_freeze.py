"""Freezing the season projections: the guard, the stamp, and the refusal.

Covers ``Scripts/freeze.py`` and the ``board_frozen`` artifact it adds to the store.
No network -- boards are written to a scratch ``Data/`` root.

The thing worth protecting is idempotence-by-refusal. Freezing twice would overwrite
the very board the first freeze existed to preserve, so a second run is a no-op unless
it is asked for explicitly.
"""

import json

import pandas as pd
import pytest

from Scripts import freeze, paths, store


@pytest.fixture()
def scratch(monkeypatch, tmp_path):
    """A store root of our own, so nothing touches the real one.

    ``STORE_DIR`` is the knob, not ``DATA_DIR``: it is computed from ``DATA_DIR`` at
    import, so patching the parent leaves it pointing at the real ``Data/Store``.
    ``paths.store_root`` exists to make this patchable and says so.
    """
    monkeypatch.setattr(paths, "STORE_DIR", tmp_path / "Store")
    monkeypatch.setattr(paths, "DATA_DIR", tmp_path)
    return tmp_path


def _board(rows=3):
    return pd.DataFrame({"player_id": range(rows),
                         "TRUE_Points": [100.0] * rows})


def _write(season, league, *, board=True, picks=None):
    """Put a board and optionally a draft artifact into the scratch store."""
    written = {}
    if board:
        written["board"] = _board()
    if picks is not None:
        written["draft"] = pd.DataFrame({"season": picks,
                                         "player_id": range(len(picks))})
    store.write_league_store(season, league, **written)


# --- the artifact --------------------------------------------------------

def test_board_frozen_is_a_registered_artifact():
    """So `sync --push` carries it and `--verify` checks it, with no new code."""
    assert store.ARTIFACTS["board_frozen"] == "board_frozen.parquet"


# --- the guard -----------------------------------------------------------

def test_a_league_with_no_picks_is_refused(scratch):
    """A pre-draft board is the one thing there is no point freezing: it projects a
    roster nobody owns yet."""
    result = freeze.freeze_league(2026, "knights_ffl", allow_undrafted=False)
    assert "no 2026 picks" in result or "no board" in result


def test_a_league_with_no_picks_can_be_forced(scratch):
    _write(2026, "knights_ffl", picks=[2025, 2025])
    assert freeze.freeze_league(2026, "knights_ffl",
                                allow_undrafted=True).startswith("frozen")


def test_a_league_with_no_board_says_so(scratch):
    _write(2026, "knights_ffl", board=False, picks=[2026])
    assert "no board" in freeze.freeze_league(2026, "knights_ffl")


def test_picks_are_counted_for_this_season_only(scratch):
    """`draft.parquet` holds every season the league has ever drafted.

    Each season's store carries its own copy, so the count is read from the store
    for the season being asked about and then filtered inside it.
    """
    _write(2026, "knights_ffl", picks=[2024, 2025, 2026, 2026])
    _write(2025, "knights_ffl", picks=[2024, 2025])
    assert freeze.drafted_picks(2026, "knights_ffl") == 2
    assert freeze.drafted_picks(2025, "knights_ffl") == 1


def test_a_season_with_no_store_counts_as_no_picks(scratch):
    _write(2026, "knights_ffl", picks=[2026])
    assert freeze.drafted_picks(2019, "knights_ffl") == 0


def test_no_draft_artifact_counts_as_no_picks(scratch):
    _write(2026, "knights_ffl")
    assert freeze.drafted_picks(2026, "knights_ffl") == 0


# --- freezing ------------------------------------------------------------

def test_freezing_copies_the_board_and_stamps_the_metadata(scratch):
    _write(2026, "winfield_football", picks=[2026] * 96)
    assert freeze.freeze_league(2026, "winfield_football").startswith("frozen")

    frozen = paths.store_dir(2026, "winfield_football") / "board_frozen.parquet"
    assert frozen.is_file()

    meta = json.loads((paths.store_dir(2026, "winfield_football")
                       / "meta.json").read_text())
    assert meta["frozen_picks"] == 96
    assert meta["frozen_at"]
    assert "board_frozen" in meta["artifacts"]


def test_the_frozen_board_matches_the_board_it_was_taken_from(scratch):
    _write(2026, "winfield_football", picks=[2026])
    freeze.freeze_league(2026, "winfield_football")
    directory = paths.store_dir(2026, "winfield_football")
    assert pd.read_parquet(directory / "board_frozen.parquet").equals(
        pd.read_parquet(directory / "board.parquet"))


def test_freezing_does_not_erase_the_league_settings(scratch):
    """The plan-37 bug, one artifact over.

    `build_meta` fills `team_count`/`roster_slots`/`starting_slots` only when it has
    a live league, and the freeze has none. Without the carry-forward this call would
    silently strip the whole cash lens off an auction board.
    """
    store.write_league_store(2026, "gop_degenerates", board=_board(),
                             draft=pd.DataFrame({"season": [2026]}),
                             meta_extra={"team_count": 16,
                                         "starting_slots": {"QB": 1},
                                         "draft_settings": {"auction_budget": 250}})
    freeze.freeze_league(2026, "gop_degenerates")
    meta = json.loads((paths.store_dir(2026, "gop_degenerates")
                       / "meta.json").read_text())
    assert meta["team_count"] == 16
    assert meta["starting_slots"] == {"QB": 1}
    assert meta["draft_settings"]["auction_budget"] == 250


# --- refusing to refreeze -----------------------------------------------

def test_a_second_freeze_is_refused(scratch):
    """It would overwrite the board the first freeze existed to preserve."""
    _write(2026, "winfield_football", picks=[2026])
    freeze.freeze_league(2026, "winfield_football")
    assert "already frozen" in freeze.freeze_league(2026, "winfield_football")


def test_refreeze_replaces_it_when_asked(scratch):
    _write(2026, "winfield_football", picks=[2026])
    freeze.freeze_league(2026, "winfield_football")

    store.write_league_store(2026, "winfield_football",
                             board=pd.DataFrame({"player_id": [0],
                                                 "TRUE_Points": [999.0]}))
    freeze.freeze_league(2026, "winfield_football", refreeze=True)
    frozen = pd.read_parquet(paths.store_dir(2026, "winfield_football")
                             / "board_frozen.parquet")
    assert frozen["TRUE_Points"].tolist() == [999.0]


# --- the CLI ------------------------------------------------------------

def test_the_cli_needs_a_target():
    with pytest.raises(SystemExit):
        freeze.main([])


def test_the_cli_freezes_a_named_league(scratch, monkeypatch):
    _write(2026, "winfield_football", picks=[2026])
    monkeypatch.setattr(freeze, "resolve_league",
                        lambda name: {"key": "winfield_football",
                                      "display_name": "Winfield_Football"})
    assert freeze.main(["--league", "Winfield_Football", "--season", "2026"]) == 0


def test_the_cli_reports_failure_when_nothing_could_freeze(scratch, monkeypatch):
    _write(2026, "knights_ffl")
    monkeypatch.setattr(freeze, "resolve_league",
                        lambda name: {"key": "knights_ffl",
                                      "display_name": "Knights_FFL"})
    assert freeze.main(["--league", "Knights_FFL", "--season", "2026"]) == 1


def test_one_league_failing_does_not_stop_the_others(scratch, monkeypatch):
    """The same argument `Scripts.refresh` makes: a leaguemate's league going wrong
    should not cost you yours."""
    _write(2026, "winfield_football", picks=[2026])
    monkeypatch.setattr(freeze, "build_lg_vars", lambda: {"a": {}, "b": {}})

    def resolve(name):
        if name == "a":
            raise ValueError("no such league")
        return {"key": "winfield_football", "display_name": "Winfield_Football"}

    monkeypatch.setattr(freeze, "resolve_league", resolve)
    results = freeze.freeze(None, 2026)
    assert "ValueError" in results["a"]
    assert results["winfield_football"].startswith("frozen")


def test_the_commands_it_tells_you_to_run_are_actually_runnable(
        scratch, monkeypatch, capsys):
    """The publish hint is the one instruction printed at the one moment it matters,
    and it was wrong.

    `Scripts.freeze` used to print `sync --push --what board_frozen`. `--what` selects a
    *bucket prefix* -- store / archive / nfl -- not an artifact, so that exits 2 with
    `unknown --what value(s) ['board_frozen']`. `board_frozen` rides inside `store` like
    every other entry in `Scripts.store.ARTIFACTS`, which the module docstring says
    correctly; only the printed line disagreed. Found 2026-09-07 while freezing the
    first five leagues, with five more still to freeze before the season's first game.

    Checked against `sync.WHAT_CHOICES` rather than eyeballed, because the failure mode
    is that nobody runs the command until the deadline it exists to meet -- and by then
    the window it protects has closed.
    """
    import shlex
    from Scripts import sync

    _write(2026, "knights_ffl", picks=[2026])
    monkeypatch.setattr(freeze, "build_lg_vars", lambda: {"knights_ffl": {}})
    monkeypatch.setattr(freeze, "resolve_league",
                        lambda name: {"key": "knights_ffl",
                                      "display_name": "Knights_FFL"})
    freeze.freeze(None, 2026)

    printed = [line for line in capsys.readouterr().out.splitlines()
               if "Scripts.sync" in line]
    assert printed, "the publish hint stopped being printed"

    for line in printed:
        argv = shlex.split(line.split("python -m Scripts.sync", 1)[1])
        assert argv, line
        assert argv[0] in ("--push", "--pull", "--verify"), line
        if "--what" in argv:
            values = argv[argv.index("--what") + 1].split(",")
            unknown = [v for v in values if v not in sync.WHAT_CHOICES]
            assert not unknown, (
                f"{line!r} passes --what {unknown}, which sync rejects; "
                f"known values are {list(sync.WHAT_CHOICES)}")
