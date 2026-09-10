"""Resolving a projection against what has actually been played.

The property that matters most is the *negative* one, and it is the first test here:
before any game kicks off, ``LIVE_Points`` is ``TRUE_Points`` for every row. That is
what makes this change safe to ship mid-season -- if it were not true, every board in
the store would move the moment it landed, for no reason.

After that, the thing that is easy to get wrong: **a real zero.** A player who took
the field and scored nothing, and a player who has not kicked off, are the same row
in every column the pipeline had before game state existed. Several of the tests below
exist only to hold those two apart.

Synthetic frames and a fake League. No network.
"""

import warnings

import numpy as np
import pandas as pd
import polars as pl
import pytest

from Scripts import live
from Scripts.game_state import BYE, IN, POST, PRE


class _League:
    """The two things `proj_to_score` asks a League for."""

    def __init__(self, rules):
        self.rules = rules
        self.year = 2026
        self.league_id = 1

    # `get_scoring_table` is monkeypatched to read this.


PPR = {"receivingReceptions": 1.0, "receivingYards": 0.1,
       "receivingTouchdowns": 6.0}


@pytest.fixture(autouse=True)
def scoring(monkeypatch):
    """Price every slot the same, so these tests are about resolution not overrides.

    Patched on ``Scripts.projection_utils`` as well as on ``Scripts.scoring``.
    ``projection_utils`` does ``from Scripts.scoring import get_scoring_table``, so it
    binds its own reference at import -- patching only the source module worked when
    this file happened to run before anything had imported ``projection_utils`` and
    failed when it did not, which is the worst kind of green.
    """
    table = pd.DataFrame({"colName": list(PPR), "points": list(PPR.values())})
    fake = lambda league, slot=None: table          # noqa: E731 - one line, one use
    monkeypatch.setattr("Scripts.scoring.get_scoring_table", fake)
    monkeypatch.setattr("Scripts.projection_utils.get_scoring_table", fake)
    return table


def board(rows):
    """A game-state frame, as `Scripts.game_state.states` returns one."""
    return pl.DataFrame(rows, schema={"team": pl.String, "week": pl.Int64,
                                      "state": pl.String, "elapsed": pl.Float64})


def frame(rows):
    """A lineups-shaped frame."""
    return pd.DataFrame(rows)


def resolved(rows, states, *, expect_unpriced=False, **kwargs):
    """Run both halves, the way `clean_lineups` does.

    Most of these fixtures set ``points`` without a matching stat line, because the
    number under test is the resolution and not the scoring. That makes
    ``actual_unpriced`` non-zero by construction, so the warning is suppressed unless
    a test is specifically about it -- see
    :func:`test_an_unpriced_bonus_is_named_rather_than_absorbed`.
    """
    out = live.attach_game_state(frame(rows), board(states))
    if expect_unpriced:
        return live.resolve(out, _League(PPR), list(PPR), **kwargs)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", live.UnpricedActualWarning)
        return live.resolve(out, _League(PPR), list(PPR), **kwargs)


def player(name, team, *, points=0.0, true_points=10.0, receptions=None,
           yards=0.0, tds=0.0, week=1, position="WR"):
    """One lineups row.

    ``receptions`` defaults to ``points``, which under the PPR table above makes the
    stat line score to exactly ``points`` -- so ``actual_unpriced`` is zero and these
    fixtures do not trip the warning that exists to report real scoring gaps. A test
    about that gap sets the line and the total to disagree on purpose.
    """
    if receptions is None:
        receptions = points
    return {"week": week, "player_name": name, "player_id": abs(hash(name)) % 10**6,
            "pro_team": team, "primaryPosition": position, "points": points,
            "TRUE_Points": true_points, "receivingReceptions": receptions,
            "receivingYards": yards, "receivingTouchdowns": tds,
            "TRUE_receivingReceptions": 5.0, "TRUE_receivingYards": 60.0,
            "TRUE_receivingTouchdowns": 0.4}


# --- the inertness guard --------------------------------------------------

def test_before_any_kickoff_live_is_exactly_the_blend():
    """The reason this can ship in week 1 rather than next August."""
    out = resolved(
        [player("A", "SEA", true_points=12.34),
         player("B", "NE", true_points=0.0),
         player("C", "KC", true_points=21.5)],
        [{"team": t, "week": 1, "state": PRE, "elapsed": 0.0}
         for t in ("SEA", "NE", "KC")])
    assert out[live.LIVE_POINTS].tolist() == pytest.approx([12.34, 0.0, 21.5])
    assert out[live.LIVE_REMAINING].tolist() == pytest.approx([12.34, 0.0, 21.5])
    assert not out[live.LOCKED_COLUMN].any()


# --- a real zero ----------------------------------------------------------

def test_a_player_who_played_and_scored_nothing_stays_at_zero():
    """The case the pipeline could not express. He is not worth his projection and
    he is not missing data -- he is worth nothing, and it is final."""
    out = resolved([player("Ruled Out", "SEA", points=0.0, true_points=13.6)],
                   [{"team": "SEA", "week": 1, "state": POST, "elapsed": 1.0}])
    assert out[live.LIVE_POINTS][0] == 0.0
    assert out[live.LIVE_REMAINING][0] == 0.0
    assert out[live.LOCKED_COLUMN][0]


def test_the_same_row_before_kickoff_is_worth_his_projection():
    """Same numbers, different state, opposite answer -- which is the whole point."""
    out = resolved([player("Not Yet", "SEA", points=0.0, true_points=13.6)],
                   [{"team": "SEA", "week": 1, "state": PRE, "elapsed": 0.0}])
    assert out[live.LIVE_POINTS][0] == pytest.approx(13.6)


def test_a_bye_is_zero_and_not_a_leaked_blend():
    """`TRUE_Points` imputes an absent source from the ESPN/FantasyPros mean, so 24
    of 81 bye players carried up to 2.12 points -- see `app.lineup.pool_playable`.
    Game state settles it directly."""
    out = resolved([player("On Bye", "SEA", true_points=2.12)],
                   [{"team": "SEA", "week": 1, "state": BYE, "elapsed": 1.0}])
    assert out[live.LIVE_POINTS][0] == 0.0
    assert out[live.LIVE_REMAINING][0] == 0.0


# --- a finished game -----------------------------------------------------

def test_a_finished_game_uses_espns_number():
    out = resolved([player("Star", "SEA", points=26.2, true_points=18.56)],
                   [{"team": "SEA", "week": 1, "state": POST, "elapsed": 1.0}])
    assert out[live.LIVE_POINTS][0] == pytest.approx(26.2)


# --- a game in progress --------------------------------------------------

def test_mid_game_is_banked_plus_what_is_left():
    """Half a game played: everything scored so far, plus half the projection."""
    out = resolved([player("Half Way", "SEA", points=8.0, true_points=14.0)],
                   [{"team": "SEA", "week": 1, "state": IN, "elapsed": 0.5}])
    assert out[live.LIVE_POINTS][0] == pytest.approx(8.0 + 7.0)
    assert out[live.LIVE_REMAINING][0] == pytest.approx(7.0)


def test_the_remaining_projection_is_what_the_spread_is_computed_from():
    """`matchup_sim.side` reads this column, not the total. A player who has banked
    40 points must not be given the spread of a 40-point projection."""
    out = resolved([player("Banked", "SEA", points=40.0, true_points=12.0)],
                   [{"team": "SEA", "week": 1, "state": POST, "elapsed": 1.0}])
    assert out[live.LIVE_POINTS][0] == pytest.approx(40.0)
    assert out[live.LIVE_REMAINING][0] == 0.0


# --- nulls, which mean "nobody had an opinion" ---------------------------

def test_a_player_nobody_projected_stays_null_before_kickoff():
    """`_apply_scoring` writes NaN rather than 0 for a source with no opinion, and
    that has to survive -- an absent projection is not a projection of zero."""
    out = resolved([player("Unknown", "SEA", true_points=np.nan)],
                   [{"team": "SEA", "week": 1, "state": PRE, "elapsed": 0.0}])
    assert pd.isna(out[live.LIVE_POINTS][0])


def test_a_player_nobody_projected_still_gets_his_actual_once_played():
    """NaN * 0 is NaN in pandas, so without the branch on state a player nobody
    projected would finish a game he scored 14 points in with a null score."""
    out = resolved([player("Unknown", "SEA", points=14.0, true_points=np.nan)],
                   [{"team": "SEA", "week": 1, "state": POST, "elapsed": 1.0}])
    assert out[live.LIVE_POINTS][0] == pytest.approx(14.0)


# --- the audit half ------------------------------------------------------

def test_act_points_scores_the_actual_stat_line():
    out = resolved([player("Scorer", "SEA", points=26.2, receptions=8.0,
                           yards=122.0, tds=1.0)],
                   [{"team": "SEA", "week": 1, "state": POST, "elapsed": 1.0}])
    # 8 receptions + 12.2 yards + 6 for the touchdown.
    assert out[live.ACT_POINTS][0] == pytest.approx(26.2)
    assert out[live.ACTUAL_UNPRICED][0] == pytest.approx(0.0)


def test_an_unpriced_bonus_is_named_rather_than_absorbed():
    """The john_pc_league case, measured live on 2026-09-10: ESPN paid Jaxon
    Smith-Njigba 31.20 for a line this pipeline scores at 26.20, because that league
    prices five long-touchdown bonuses nothing here maps. `LIVE_Points` follows ESPN
    -- its number is the league's official score -- and the gap gets a name."""
    with pytest.warns(live.UnpricedActualWarning, match=r"\+5\.00"):
        out = resolved([player("Smith-Njigba", "SEA", points=31.2, receptions=8.0,
                               yards=122.0, tds=1.0)],
                       [{"team": "SEA", "week": 1, "state": POST, "elapsed": 1.0}],
                       expect_unpriced=True, label="john_pc_league")
    assert out[live.ACT_POINTS][0] == pytest.approx(26.2)
    assert out[live.ACTUAL_UNPRICED][0] == pytest.approx(5.0)
    assert out[live.LIVE_POINTS][0] == pytest.approx(31.2)


def test_the_gap_is_only_claimed_for_games_that_have_been_played():
    """Before kickoff `points` is zero and the actual stat line is empty, so the
    difference is not evidence of anything."""
    out = resolved([player("Not Yet", "SEA", points=0.0)],
                   [{"team": "SEA", "week": 1, "state": PRE, "elapsed": 0.0}])
    assert pd.isna(out[live.ACTUAL_UNPRICED][0])


# --- joining the board on ------------------------------------------------

def test_a_player_with_no_known_team_keeps_his_projection_and_is_reported():
    """An unsigned player carries `proTeam` of "None". Locking him would zero a
    projection nobody asked to zero, so he stays `pre` -- loudly."""
    with pytest.warns(live.MissingGameStateWarning, match="1 player-week"):
        out = resolved([player("Unsigned", "None", true_points=3.0)],
                       [{"team": "SEA", "week": 1, "state": POST, "elapsed": 1.0}])
    assert out[live.STATE_COLUMN][0] == PRE
    assert out[live.LIVE_POINTS][0] == pytest.approx(3.0)


def test_the_board_is_joined_on_week_as_well_as_team():
    """`lineups.parquet` always holds weeks 1..current, so a team-only join would
    give week 3's state to week 1's rows."""
    out = resolved(
        [player("Wk1", "SEA", points=20.0, week=1),
         player("Wk2", "SEA", points=0.0, true_points=15.0, week=2)],
        [{"team": "SEA", "week": 1, "state": POST, "elapsed": 1.0},
         {"team": "SEA", "week": 2, "state": PRE, "elapsed": 0.0}])
    by_week = dict(zip(out["week"], out[live.LIVE_POINTS]))
    assert by_week[1] == pytest.approx(20.0)
    assert by_week[2] == pytest.approx(15.0)


def test_resolving_without_the_state_columns_refuses():
    """It would return the projection for every row, which is indistinguishable from
    a live feature that is not working."""
    with pytest.raises(KeyError, match="attach_game_state"):
        live.resolve(frame([player("A", "SEA")]), _League(PPR), list(PPR))


# --- helpers the consumers read -----------------------------------------

def test_points_column_falls_back_for_a_store_built_before_this_landed():
    assert live.points_column(["TRUE_Points", live.LIVE_POINTS]) == live.LIVE_POINTS
    assert live.points_column(["TRUE_Points"]) == "TRUE_Points"


def test_state_counts_orders_the_week_as_it_is_played():
    out = resolved(
        [player("A", "SEA", points=1.0), player("B", "KC"), player("C", "NE")],
        [{"team": "SEA", "week": 1, "state": POST, "elapsed": 1.0},
         {"team": "KC", "week": 1, "state": IN, "elapsed": 0.3},
         {"team": "NE", "week": 1, "state": PRE, "elapsed": 0.0}])
    assert list(live.state_counts(out)) == [PRE, IN, POST]


def test_locked_ids_is_empty_without_game_state():
    """So the optimiser locks nobody on an older store."""
    assert live.locked_ids(frame([player("A", "SEA")])) == []


# --- the patch path ------------------------------------------------------

def test_patching_a_week_the_frame_does_not_hold_refuses():
    """It would append a second, projection-free copy of the week and halve every
    team total."""
    stored = frame([player("A", "SEA", week=1)])
    box = frame([player("A", "SEA", points=9.9, week=3)])
    with pytest.raises(live.LiveRefreshError, match="holds weeks"):
        live.patch(stored, box, 3, stats=list(PPR))


def test_a_patch_moves_the_actuals_and_leaves_the_projections_alone():
    stored = frame([player("A", "SEA", points=0.0, true_points=11.0)])
    box = frame([{**player("A", "SEA", points=17.4, receptions=6.0, yards=84.0),
                  "player_id": stored["player_id"][0]}])
    out, counts = live.patch(stored, box, 1, stats=list(PPR))
    assert counts == {"patched": 1, "added": 0, "uncovered": 0}
    assert out["points"][0] == pytest.approx(17.4)
    assert out["receivingYards"][0] == pytest.approx(84.0)
    assert out["TRUE_Points"][0] == pytest.approx(11.0)


def test_a_patch_moves_the_roster_too():
    """A manager who promotes a bench player before kickoff changes `slotPosition`.
    Patching only `points` would show live scores against last night's lineup."""
    stored = frame([{**player("A", "SEA"), "slotPosition": "BE",
                     "team_owner": "Old"}])
    box = frame([{**player("A", "SEA", points=5.0), "slotPosition": "WR",
                  "team_owner": "New", "player_id": stored["player_id"][0]}])
    out, _ = live.patch(stored, box, 1, stats=list(PPR))
    assert out["slotPosition"][0] == "WR"
    assert out["team_owner"][0] == "New"


def test_a_player_added_mid_week_arrives_flagged():
    """He has actuals and no projection, so before his kickoff he understates his
    team's total. Flagged and counted rather than absorbed."""
    stored = frame([player("Rostered", "SEA")])
    box = frame([{**player("Rostered", "SEA"),
                  "player_id": stored["player_id"][0]},
                 player("Just Added", "KC", points=12.0)])
    out, counts = live.patch(stored, box, 1, stats=list(PPR))
    assert counts["added"] == 1
    assert len(out) == 2
    added = out[out["player_name"] == "Just Added"]
    assert bool(added[live.LIVE_ONLY_COLUMN].iloc[0])
    assert not bool(out[out["player_name"] == "Rostered"][live.LIVE_ONLY_COLUMN].iloc[0])


def test_the_live_only_column_is_always_written():
    """So the artifact's schema does not depend on whether anybody made a claim."""
    stored = frame([player("A", "SEA")])
    box = frame([{**player("A", "SEA"), "player_id": stored["player_id"][0]}])
    out, _ = live.patch(stored, box, 1, stats=list(PPR))
    assert live.LIVE_ONLY_COLUMN in out.columns
    assert out[live.LIVE_ONLY_COLUMN].dtype == bool
