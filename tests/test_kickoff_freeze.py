"""A projection stops being writable when its own game kicks off.

Covers ``Scripts/kickoff_freeze.py`` and the two places it is wired in:
``Scripts.refresh``'s lineups branch and ``Scripts.scrape_FP.scrape_weekly``.

The cases that matter are the ones about *direction*. A freeze that is too eager
pins a season to whatever was stored the day the scoreboard broke; one that is too
shy rewrites a played game and says nothing. Each of those has a test here naming
which way it must fail.
"""

import pandas as pd
import polars as pl
import pytest

from Scripts import kickoff_freeze as kf


# --- fixtures -------------------------------------------------------------

def frame(states=("pre", "pre", "pre", "pre"), offset=0.0):
    """A four-row lineups frame: three week-1 players and one week-2 row."""
    return pd.DataFrame({
        "week": [1, 1, 1, 2],
        "player_id": [10, 11, 12, 10],
        "pro_team": ["NE", "SEA", "KC", "NE"],
        "game_state": list(states),
        "TRUE_Points": [10.0 + offset, 20.0 + offset, 30.0 + offset, 11.0 + offset],
        "ESPN_Points": [9.0 + offset, 19.0 + offset, 29.0 + offset, 10.0 + offset],
        "FP_receivingYards": [80.0 + offset] * 4,
        "projPoints": [9.5 + offset] * 4,
        "espn_unpriced": [0.5 + offset] * 4,
        # actuals, which must always flow
        "points": [0.0, 0.0, 0.0, 0.0],
        "passingYards": [0.0, 0.0, 0.0, 0.0],
        "LIVE_Points": [10.0 + offset] * 4,
        "ACT_Points": [0.0] * 4,
        # roster columns, which must always flow
        "slotPosition": ["QB", "WR", "RB", "QB"],
    })


# --- what freezes and what does not ---------------------------------------

def test_the_projection_families_are_taken_from_the_source_register():
    """`FROZEN_PREFIXES` is `WEEKLY_PREFIXES`, not a second hand-maintained list.

    A source added to the blend must be frozen the day it lands, not the day
    somebody remembers this module exists.
    """
    from Scripts.projection_utils import WEEKLY_PREFIXES

    assert kf.FROZEN_PREFIXES == tuple(WEEKLY_PREFIXES)


def test_projpoints_is_frozen_even_though_it_carries_no_prefix():
    """The one projection column with no source prefix, and the one the live loop
    overwrites every ten minutes through a slate."""
    assert "projPoints" in kf.frozen_columns(frame().columns)
    assert "espn_unpriced" in kf.frozen_columns(frame().columns)


def test_actuals_and_roster_columns_are_never_frozen():
    """`points` settles at the final whistle and `slotPosition` moves all week.
    Freezing either would be the opposite of the bug this fixes."""
    frozen = kf.frozen_columns(frame().columns)
    for column in ("points", "passingYards", "LIVE_Points", "ACT_Points",
                   "slotPosition", "game_state", "week", "player_id"):
        assert column not in frozen, column


def test_a_started_game_holds_its_projection_and_an_unstarted_one_rebuilds():
    stored = frame()
    fresh = frame(states=("post", "post", "pre", "pre"), offset=5.0)

    out, counts = kf.apply(fresh, stored, now="2026-09-10T06:00:00+00:00")

    assert counts["frozen"] == 2
    assert out.loc[0, "TRUE_Points"] == 10.0      # NE, played
    assert out.loc[1, "TRUE_Points"] == 20.0      # SEA, played
    assert out.loc[2, "TRUE_Points"] == 35.0      # KC, has not kicked off
    assert out.loc[3, "TRUE_Points"] == 16.0      # week 2, has not kicked off


def test_a_game_in_progress_freezes_too():
    """`in` is as unwritable as `post`. A projection revised at half time is not a
    pre-game opinion, and it is the state the sidebar button can most easily hit."""
    out, counts = kf.apply(frame(states=("in", "pre", "pre", "pre"), offset=5.0),
                           frame())
    assert counts["frozen"] == 1
    assert out.loc[0, "TRUE_Points"] == 10.0


def test_a_bye_does_not_freeze():
    """A bye is an absence of a game, not a game that has started. Its projection is
    zero either way, so a rule for it would protect nothing and be one more thing to
    explain."""
    out, counts = kf.apply(frame(states=("bye", "pre", "pre", "pre"), offset=5.0),
                           frame())
    assert counts["locked"] == 0
    assert out.loc[0, "TRUE_Points"] == 15.0


def test_actuals_flow_through_a_frozen_row():
    """The whole point: the projection is held and the score is not. A freeze that
    took the actuals with it would stop the week ever settling."""
    fresh = frame(states=("post", "pre", "pre", "pre"), offset=5.0)
    fresh.loc[0, "points"] = 26.2
    fresh.loc[0, "passingYards"] = 317.0
    fresh.loc[0, "LIVE_Points"] = 26.2

    out, _ = kf.apply(fresh, frame())

    assert out.loc[0, "TRUE_Points"] == 10.0
    assert out.loc[0, "points"] == 26.2
    assert out.loc[0, "passingYards"] == 317.0
    assert out.loc[0, "LIVE_Points"] == 26.2


# --- the failure directions -----------------------------------------------

def test_no_game_state_freezes_nothing():
    """An unknown clock must not freeze. The failure it would cause -- a season
    pinned to whatever was stored the day the scoreboard broke -- is far worse than
    the one it prevents, and it would be invisible."""
    fresh = frame(offset=5.0).drop(columns=["game_state"])
    out, counts = kf.apply(fresh, frame())
    assert counts["locked"] == 0
    assert out.loc[0, "TRUE_Points"] == 15.0


def test_a_first_build_freezes_nothing():
    """No stored frame means no earlier opinion to keep. Inventing one is worse than
    publishing the fresh number once."""
    out, counts = kf.apply(frame(states=("post",) * 4, offset=5.0), None)
    assert counts["frozen"] == 0
    assert out.loc[0, "TRUE_Points"] == 15.0


def test_a_locked_player_absent_from_the_stored_frame_keeps_his_fresh_values():
    """Somebody added mid-week, after the last build. The alternative is a row with
    no projection at all, which every downstream surface reads as zero."""
    stored = frame()
    out, counts = kf.apply(frame(states=("post", "pre", "pre", "pre"), offset=5.0),
                           stored[stored["player_id"] != 10])
    assert counts["unmatched"] == 1
    assert out.loc[0, "TRUE_Points"] == 15.0


def test_missing_join_keys_warn_rather_than_raise():
    """A refresh must not die because a frame lost a column. It publishes a rebuilt
    projection and says so -- the same direction `clean_lineups` takes for the live
    columns."""
    fresh = frame(states=("post",) * 4, offset=5.0).drop(columns=["player_id"])
    with pytest.warns(kf.KickoffFreezeWarning, match="cannot freeze"):
        out, counts = kf.apply(fresh, frame())
    assert counts["frozen"] == 0


# --- the stamp ------------------------------------------------------------

def test_the_stamp_records_when_a_row_was_first_frozen_and_never_moves_after():
    """`projection_frozen_at` is what separates "this number is pre-game" from "this
    number is pre-game as far as we know". A stamp that advanced on every run would
    answer neither."""
    first, _ = kf.apply(frame(states=("post", "pre", "pre", "pre"), offset=5.0),
                        frame(), now="2026-09-10T06:00:00+00:00")
    assert first.loc[0, kf.STAMP_COLUMN] == "2026-09-10T06:00:00+00:00"
    assert pd.isna(first.loc[1, kf.STAMP_COLUMN])

    second, counts = kf.apply(frame(states=("post", "post", "pre", "pre"), offset=9.0),
                              first, now="2026-09-11T06:00:00+00:00")
    assert second.loc[0, kf.STAMP_COLUMN] == "2026-09-10T06:00:00+00:00"
    assert second.loc[1, kf.STAMP_COLUMN] == "2026-09-11T06:00:00+00:00"
    assert counts["newly_frozen"] == 1


def test_a_stored_frame_from_before_the_stamp_existed_is_stamped_not_refused():
    """Every store written before this shipped has no stamp column. The first run
    after the change must stamp the already-locked rows, not raise."""
    stored = frame().drop(columns=[], errors="ignore")
    assert kf.STAMP_COLUMN not in stored.columns
    out, counts = kf.apply(frame(states=("post", "pre", "pre", "pre"), offset=5.0),
                           stored, now="2026-09-10T06:00:00+00:00")
    assert counts["newly_frozen"] == 1
    assert out.loc[0, kf.STAMP_COLUMN] == "2026-09-10T06:00:00+00:00"


def test_the_summary_says_nothing_rather_than_printing_zeros():
    """A line of zeros every night is a line people stop reading."""
    assert "no game has started" in kf.summary(
        {"locked": 0, "frozen": 0, "unmatched": 0, "columns": 0, "newly_frozen": 0})


# --- started_teams, and the FantasyPros merge it drives ---------------------

def test_started_teams_reads_in_and_post_and_skips_bye(monkeypatch):
    board = pl.DataFrame({
        "week": [1, 1, 1, 1],
        "team": ["NE", "SEA", "KC", "DEN"],
        "state": ["post", "in", "pre", "bye"],
    })
    monkeypatch.setattr("Scripts.game_state.board", lambda *a, **k: board)
    assert kf.started_teams(2026, [1]) == {(1, "NE"), (1, "SEA")}


def test_started_teams_is_empty_for_no_weeks():
    assert kf.started_teams(2026, []) == set()


def test_fantasypros_hold_mask_is_per_game(monkeypatch):
    """The fix for the old blanket `keep="first"`, which froze Sunday's slate on
    Tuesday and threw away every Saturday inactive. The merge itself is exercised
    end-to-end in `tests/test_scrape_fp.py`; this pins the decision."""
    from Scripts import scrape_FP

    fetched = pd.DataFrame({
        "week": [1, 1], "player_name": ["A", "B"], "playerTeam": ["NE", "KC"]})
    monkeypatch.setattr(kf, "started_teams", lambda season, weeks: {(1, "NE")})

    assert scrape_FP._hold_mask(2026, fetched, fetched.iloc[:0]).tolist() == [True, False]


def test_fantasypros_falls_back_to_what_the_file_already_has(monkeypatch, capsys):
    """The opposite direction from `apply`, and deliberately so. For a *cumulative
    source file* an unreadable scoreboard must mean "change nothing".

    But only for rows the file already holds -- a week it has never seen still lands,
    because `clean_lineups` re-merges this file onto every week in the lineup frame
    and a blanket hold would blank the new week instead of protecting the old one.
    """
    from Scripts import scrape_FP

    def explode(season, weeks):
        raise RuntimeError("scoreboard down")

    monkeypatch.setattr(kf, "started_teams", explode)
    fetched = pd.DataFrame({
        "week": [1, 2], "player_name": ["A", "A"], "playerTeam": ["NE", "KC"]})
    existing = pd.DataFrame({
        "week": [1], "player_name": ["A"], "playerTeam": ["NE"]})

    assert scrape_FP._hold_mask(2026, fetched, existing).tolist() == [True, False]
    assert "could not read the NFL scoreboard" in capsys.readouterr().out


def test_a_frame_with_no_team_column_falls_back_the_same_way(monkeypatch, capsys):
    """Hand-built frames and pre-2026-08-24 captures carry no `playerTeam`."""
    from Scripts import scrape_FP

    fetched = pd.DataFrame({"week": [1, 2], "player_name": ["A", "A"]})
    existing = pd.DataFrame({"week": [1], "player_name": ["A"]})

    assert scrape_FP._hold_mask(2026, fetched, existing).tolist() == [True, False]
    assert "cannot tell which" in capsys.readouterr().out


def test_a_total_join_failure_is_shouted_not_counted_as_churn():
    """Every locked row unmatched means the join broke -- most likely a dtype drift
    on `week` or `player_id` against a store written by an older build. Reported as
    "N added since the last build" it reads as an ordinary busy week, while in fact
    nothing was frozen at all."""
    message = kf.summary({"locked": 96, "frozen": 0, "unmatched": 96,
                          "columns": 0, "newly_frozen": 0})
    assert "NOTHING HELD" in message
    assert "dtypes" in message


def test_a_dtype_mismatch_on_the_join_keys_is_detectable(monkeypatch):
    """The failure the message above describes, reproduced."""
    stored = frame()
    fresh = frame(states=("post", "post", "pre", "pre"), offset=5.0)
    fresh["player_id"] = fresh["player_id"].astype(float)   # store had int64

    _, counts = kf.apply(fresh, stored)
    assert counts["locked"] == 2
    assert "NOTHING HELD" in kf.summary(counts) or counts["frozen"] == 2


# --- the live loop, which runs between the freezes --------------------------

def test_the_live_patch_holds_projpoints_once_the_game_has_started():
    """`projPoints` is ESPN's own weekly projection and it moves during a game.

    It is the one projection column `kickoff_freeze.apply` cannot protect, because the
    freeze runs on the nightly's build path and this runs 144 times a day between
    them. It was easy to miss for a structural reason: every other projection column
    carries a source prefix, so "touches no `ESPN_`/`FP_`/`TRUE_` cell" reads as
    "touches no projection" until you notice this one has no prefix.
    """
    from Scripts import live

    stored = pd.DataFrame({
        "week": [2, 2, 2], "player_id": [10, 11, 12],
        "player_name": ["A", "B", "C"],
        "game_state": ["post", "in", "pre"],
        "projPoints": [9.5, 14.0, 20.0],
        "points": [0.0, 0.0, 0.0], "passingYards": [0.0, 0.0, 0.0],
        "slotPosition": ["QB", "WR", "RB"], "team_owner": ["X", "X", "X"],
    })
    box = pd.DataFrame({
        "player_id": [10, 11, 12], "player_name": ["A", "B", "C"],
        "projPoints": [26.2, 18.0, 21.0],
        "points": [26.2, 7.0, 0.0], "passingYards": [317.0, 0.0, 0.0],
        "slotPosition": ["QB", "WR", "RB"], "team_owner": ["X", "X", "X"],
    })

    out, _ = live.patch(stored, box, 2, stats=["passingYards"])

    assert out.loc[0, "projPoints"] == 9.5, "post -- held"
    assert out.loc[1, "projPoints"] == 14.0, "in -- held"
    assert out.loc[2, "projPoints"] == 21.0, "pre -- ESPN's number is a real opinion"


def test_the_live_patch_still_writes_actuals_and_roster_on_a_locked_row():
    """The half that must not regress. A lineup is still legal to *inspect* after
    kickoff even when it is no longer legal to change, and the score is the point."""
    from Scripts import live

    stored = pd.DataFrame({
        "week": [2], "player_id": [10], "player_name": ["A"],
        "game_state": ["post"], "projPoints": [9.5], "points": [0.0],
        "passingYards": [0.0], "slotPosition": ["BE"], "team_owner": ["X"],
    })
    box = pd.DataFrame({
        "player_id": [10], "player_name": ["A"], "projPoints": [26.2],
        "points": [26.2], "passingYards": [317.0], "slotPosition": ["QB"],
        "team_owner": ["X"],
    })

    out, _ = live.patch(stored, box, 2, stats=["passingYards"])

    assert out.loc[0, "points"] == 26.2
    assert out.loc[0, "passingYards"] == 317.0
    assert out.loc[0, "slotPosition"] == "QB"


def test_a_store_with_no_game_state_patches_exactly_as_before():
    """Every 2025 store, and `winfield_football`'s early 2026 one, carry no
    `game_state`. An unknown clock must not start withholding columns."""
    from Scripts import live

    stored = pd.DataFrame({
        "week": [2], "player_id": [10], "player_name": ["A"],
        "projPoints": [9.5], "points": [0.0], "slotPosition": ["QB"],
    })
    box = pd.DataFrame({
        "player_id": [10], "player_name": ["A"], "projPoints": [26.2],
        "points": [26.2], "slotPosition": ["QB"],
    })

    out, _ = live.patch(stored, box, 2, stats=[])
    assert out.loc[0, "projPoints"] == 26.2


def test_the_two_frozen_column_lists_agree():
    """`live.FROZEN_AFTER_KICKOFF` is duplicated rather than imported, to avoid an
    import cycle. This is what stops the copies drifting apart."""
    from Scripts import live

    assert set(live.FROZEN_AFTER_KICKOFF) <= set(kf.FROZEN_COLUMNS)
    for column in live.FROZEN_AFTER_KICKOFF:
        assert column in live.PATCH_COLUMNS, (
            f"{column} is guarded but no longer patched -- drop it from "
            f"FROZEN_AFTER_KICKOFF")
