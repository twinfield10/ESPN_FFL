"""The waiver decision layer: the threshold, the scarcity price, and the verdict.

Network-free and store-free, like the rest of the engine's tests -- `Data/` is
untracked, so anything that needs a real store cannot run on a fresh clone.

What is pinned here is mostly **restraint**. The arithmetic was already right; what
it lacked was a reason to stay quiet. Each of these tests corresponds to a rule that
stopped the panel saying something it could not support, and three of them exist
because the obvious version of the rule was built, measured, and cut.
"""

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import lineup as lu  # noqa: E402
import waivers as wv  # noqa: E402

ELIGIBLE = {
    "QB": ["QB", "OP", "BE", "IR"],
    "RB": ["RB", "RB/WR", "RB/WR/TE", "OP", "BE", "IR"],
    "WR": ["WR", "RB/WR", "RB/WR/TE", "OP", "BE", "IR"],
    "TE": ["TE", "RB/WR/TE", "OP", "BE", "IR"],
    "K": ["K", "BE", "IR"],
    "D/ST": ["D/ST", "BE", "IR"],
    "LB": ["DP", "LB", "BE", "IR"],
}

FLEX_SLOTS = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1}


def player(name, position, points, *, slot="BE", status="active", pid=None, **extra):
    return {"player_id": pid if pid is not None else abs(hash(name)) % 10 ** 6,
            "player_name": name, "player_position": position,
            "primaryPosition": position, "slotPosition": slot,
            "eligiblePositions": ELIGIBLE[position],
            "player_active_status": status, "TRUE_Points": points, **extra}


def pool_row(name, position, points, **extra):
    return player(name, position, points, status=lu.POOL_STATUS,
                  ESPN_Points=points, **extra)


def full_roster():
    """A legal starting eleven plus a thin bench, on `FLEX_SLOTS`."""
    return [
        player("QB1", "QB", 20.0, slot="QB"),
        player("RB1", "RB", 16.0, slot="RB"),
        player("RB2", "RB", 12.0, slot="RB"),
        player("WR1", "WR", 15.0, slot="WR"),
        player("WR2", "WR", 11.0, slot="WR"),
        player("TE1", "TE", 9.0, slot="TE"),
        player("FLEX1", "WR", 10.0, slot="RB/WR/TE"),
        player("K1", "K", 8.0, slot="K"),
        player("DST1", "D/ST", 7.0, slot="D/ST"),
        player("Bench WR", "WR", 4.0),
    ]


# --- the threshold --------------------------------------------------------

def test_the_threshold_is_the_starter_you_displace():
    """One slot, one incumbent: the bar is exactly what he projects."""
    roster = [player("Only QB", "QB", 18.0, slot="QB")]
    assert lu.lineup_threshold(roster, {"QB": 1}, "TRUE_Points", ["QB"]) == pytest.approx(18.0)


def test_an_empty_slot_has_a_threshold_of_zero():
    """Nobody to displace, so any positive projection is an improvement."""
    assert lu.lineup_threshold([], {"QB": 1}, "TRUE_Points", ["QB"]) == pytest.approx(0.0)


def test_a_player_this_league_cannot_start_has_no_threshold():
    """**Not zero.** An individual defender in a league with no `DP` slot cannot be
    started at all, and a bar of zero would say the entire waiver wire improves
    you. This is the single most-repeated failure mode in the repo: an absent
    answer rendered as a number."""
    bar = lu.lineup_threshold(full_roster(), FLEX_SLOTS, "TRUE_Points", ["DP"])
    assert bar is None
    assert bar != 0.0


def test_a_slot_whose_starter_has_kicked_off_is_unreachable():
    """Different from the case above, and the difference is the hour rather than
    the league: the slot exists and you could normally fill it, but the man in it
    has played and no projection reaches him."""
    roster = [{**player("Played", "QB", 3.0, slot="QB"),
               lu.STATE_COLUMN: "post", lu.LOCKED_COLUMN: True}]
    assert lu.lineup_threshold(roster, {"QB": 1}, "TRUE_Points", ["QB"]) == math.inf


def test_the_bar_follows_the_flex_cascade_rather_than_the_position():
    """**The bar is what you would actually lose, which is not the man in the
    slot.** On this roster the starters are RB 16/12, WR 15/11, TE 9 and a flex
    filled by a 10-point receiver.

    A receiver takes `WR`, pushing the 11 down into the flex, which pushes the
    **10** out -- so a receiver costs 10, not 11. A back does the same: the 12
    slides to the flex and the 10 leaves again, so a back also costs 10. A tight
    end costs **9**, because the tight end he displaces is worse than the man in
    the flex and so simply leaves the lineup.

    Two linked slots therefore agree when the cascade reaches the same man and
    differ when it does not, and both are right. A per-position bar -- "beat my
    worst receiver" -- would have said 11, 12 and 9, overstating two of the three.
    """
    bars = lu.slot_thresholds(full_roster(), FLEX_SLOTS, "TRUE_Points")
    assert bars["WR"] == pytest.approx(10.0)
    assert bars["RB"] == pytest.approx(10.0)
    assert bars["TE"] == pytest.approx(9.0)


def test_the_gain_is_the_add_value_minus_the_drop_cost():
    """The identity that makes the threshold an explanation rather than a rival
    metric. Verified on live data at 448 of 448 pairings; pinned here on a roster
    small enough to read."""
    roster = full_roster()
    candidate = pool_row("Free WR", "WR", 13.0)
    drop_id = roster[-1]["player_id"]

    gain = lu.add_drop_gain(roster, FLEX_SLOTS, "TRUE_Points", candidate, drop_id)
    remaining = [r for r in roster if r["player_id"] != drop_id]
    halves = (lu.add_value(remaining, FLEX_SLOTS, "TRUE_Points",
                           lu.as_rostered(candidate))
              - lu.drop_cost(roster, FLEX_SLOTS, "TRUE_Points", drop_id))
    assert gain == pytest.approx(halves)


# --- scarcity, priced -----------------------------------------------------

def test_a_spare_at_a_thin_slot_is_worth_something_if_a_starter_sits():
    roster = full_roster()
    spare = roster[-1]["player_id"]
    assert lu.insurance_value(roster, FLEX_SLOTS, "TRUE_Points", spare) > 0


def test_a_player_who_covers_nobody_insures_nothing():
    """A kicker behind a kicker in a one-K league is not insurance against
    anything else on the roster."""
    roster = full_roster() + [player("K2", "K", 7.5)]
    k2 = roster[-1]["player_id"]
    # He covers only K1, so his insurance is what he is worth if K1 sits.
    assert lu.insurance_value(roster, FLEX_SLOTS, "TRUE_Points", k2) > 0
    # Whereas a man eligible nowhere the league starts insures nothing at all.
    orphan = player("Orphan", "LB", 30.0)
    roster.append(orphan)
    assert lu.insurance_value(roster, FLEX_SLOTS, "TRUE_Points",
                              orphan["player_id"]) == 0.0


# --- confidence -----------------------------------------------------------

def test_half_a_point_against_the_real_spread_is_a_coin_flip():
    """`UPGRADE_MIN_MARGIN` is 0.5, sized from how much the five sources disagree
    (median sd 0.43). What decides whether a move was right is how much the
    *outcome* varies, which is near 11 points for a skill-position swap. So the
    threshold that reads as a red alert is 0.045 sd -- 52%."""
    assert wv.confidence(0.5, 11.0) == pytest.approx(0.518, abs=0.005)


def test_a_real_edge_reads_as_one():
    assert wv.confidence(11.0, 11.0) == pytest.approx(0.841, abs=0.005)


def test_no_model_publishes_no_confidence_rather_than_a_guess():
    assert wv.swap_sd(None, player("A", "WR", 10.0), None, "TRUE_Points") is None
    assert wv.confidence(5.0, None) is None


# --- the rest-of-season gate ----------------------------------------------

def test_a_rank_inside_this_league_s_starting_depth_counts():
    row = {"player_position": "RB", "ros_pos_rank": "RB25", "ros_experts": 3}
    assert wv.ros_startable(row, {"RB": 31})
    assert not wv.ros_startable(row, {"RB": 20})


def test_one_expert_is_not_a_consensus():
    """With one there is no consensus, only a person. These pages carry two or
    three, so this floor is real rather than theoretical."""
    row = {"player_position": "RB", "ros_pos_rank": "RB5", "ros_experts": 1}
    assert not wv.ros_startable(row, {"RB": 31})


def test_a_position_the_league_never_starts_is_not_startable():
    """`replacement_ranks` omits a position that rounds to zero openings rather
    than flooring it at one, so an absent key means 'nobody starts here'."""
    row = {"player_position": "LB", "ros_pos_rank": "LB3", "ros_experts": 3}
    assert not wv.ros_startable(row, {"RB": 31})


def test_an_idp_with_no_ros_number_is_not_startable():
    row = {"player_position": "LB", "ros_pos_rank": None, "ros_experts": None}
    assert not wv.ros_startable(row, {"LB": 14})


# --- upside is context, not a gate ----------------------------------------

def test_the_ceiling_is_converted_to_a_per_game_figure():
    """`pts_p90` is a season total and a threshold is a weekly number. Compared
    raw, the upside rule fired on 324 of 369 rescues."""
    assert wv.upside_per_game({"pts_p90": 340.0, "games": 17.0}) == pytest.approx(20.0)


def test_the_ceiling_needs_a_real_games_denominator():
    """Not defaulted to 17 -- an assumed denominator is how the season and weekly
    grains got mixed in the first place."""
    assert wv.upside_per_game({"pts_p90": 340.0}) is None
    assert wv.upside_per_game({"pts_p90": 340.0, "games": 0}) is None


def test_a_wide_band_alone_never_recommends_anybody():
    """The rule was built and cut. Its own top of the pool refuted it -- Troy
    Franklin at p_top12 0.41 on a 3.3-point projection -- because a wide band on a
    player the model knows little about is ignorance, not upside. Only the
    rest-of-season rank, which is backed by people rather than by our own
    uncertainty, may rescue a move."""
    assert not hasattr(wv, "upside_clears")
    add = pool_row("Wide Band", "WR", 2.0, pts_p90=400.0, games=17.0,
                   p_top12=0.45, ros_pos_rank="WR90", ros_experts=3)
    verdict, _ = wv.judge(add, player("Bench", "WR", 1.0), week_gain=0.0,
                          add_val=0.0, cost=0.0, insurance=0.0, bar=5.0,
                          replacement={"WR": 33}, min_margin=0.5)
    assert verdict is None


# --- the verdicts ---------------------------------------------------------

def test_a_streamed_position_is_never_an_emergency():
    add = pool_row("Some D/ST", "D/ST", 12.0)
    verdict, _ = wv.judge(add, player("Old D/ST", "D/ST", 6.0), week_gain=6.0,
                          add_val=6.0, cost=0.0, insurance=0.0, bar=6.0,
                          replacement={}, min_margin=0.5)
    assert verdict == wv.VERDICT_STREAM
    assert wv.VERDICT_STYLE[verdict][1] == "info"


def test_only_a_lineup_move_is_rendered_as_an_error():
    assert wv.VERDICT_STYLE[wv.VERDICT_LINEUP][1] == "error"
    assert wv.VERDICT_STYLE[wv.VERDICT_ROS][1] != "error"


def test_giving_up_real_cover_is_a_warning_rather_than_a_recommendation():
    add = pool_row("Marginal", "WR", 12.0)
    verdict, reason = wv.judge(add, player("Only Spare RB", "RB", 9.0),
                               week_gain=1.0, add_val=1.0, cost=0.0,
                               insurance=8.0, bar=11.0, replacement={},
                               min_margin=0.5)
    assert verdict == wv.VERDICT_COSTLY
    assert "cover" in reason


def test_a_rest_of_season_rescue_must_be_cheap():
    """The drop is chosen to maximise *this* week, which on a thin roster can be a
    real starter -- Jeff's league proposed giving up Terry McLaurin, worth 11.9 as
    cover, for a player who does nothing now."""
    add = pool_row("Future Star", "WR", 3.0, ros_pos_rank="WR18", ros_experts=3)
    verdict, reason = wv.judge(add, player("Real Cover", "WR", 12.0),
                               week_gain=0.0, add_val=0.0, cost=0.0,
                               insurance=11.9, bar=14.0, replacement={"WR": 33},
                               min_margin=0.5)
    assert verdict is None
    assert "cover" in reason


def test_a_rest_of_season_rescue_may_not_cost_points_now():
    """A claim for December is still a claim you play Sunday with, and a rank has
    no units to buy back what it gave away."""
    add = pool_row("Future Star", "WR", 3.0, ros_pos_rank="WR18", ros_experts=3)
    verdict, _ = wv.judge(add, player("Spare", "WR", 4.0), week_gain=-3.0,
                          add_val=0.0, cost=3.0, insurance=0.0, bar=14.0,
                          replacement={"WR": 33}, min_margin=0.5)
    assert verdict is None


def test_a_cheap_rest_of_season_rescue_is_shown():
    add = pool_row("Future Star", "WR", 3.0, ros_pos_rank="WR18", ros_experts=3)
    verdict, reason = wv.judge(add, player("Nobody", "WR", 1.0), week_gain=0.0,
                               add_val=0.0, cost=0.0, insurance=0.0, bar=14.0,
                               replacement={"WR": 33}, min_margin=0.5)
    assert verdict == wv.VERDICT_ROS
    assert "WR18" in reason


def test_a_bench_only_move_is_not_shown():
    add = pool_row("Bench Filler", "WR", 6.0)
    verdict, reason = wv.judge(add, player("Worse Filler", "WR", 4.0),
                               week_gain=0.0, add_val=0.0, cost=0.0,
                               insurance=0.0, bar=14.0, replacement={},
                               min_margin=0.5)
    assert verdict is None
    assert "bench" in reason


# --- the streaming line ---------------------------------------------------

def test_the_streaming_line_is_silent_when_you_already_have_the_best_one():
    """It exists to be glanced at, and a block that is always there stops being
    glanced at."""
    roster = full_roster()
    pool = [pool_row("Worse D/ST", "D/ST", 3.0), pool_row("Worse K", "K", 2.0)]
    assert wv.best_streamers(pool, roster, FLEX_SLOTS, "TRUE_Points") == []


def test_the_streaming_line_names_a_real_upgrade():
    roster = full_roster()
    pool = [pool_row("Better D/ST", "D/ST", 12.0)]
    rows = wv.best_streamers(pool, roster, FLEX_SLOTS, "TRUE_Points")
    assert [r["slot"] for r in rows] == ["D/ST"]
    assert rows[0]["gain"] == pytest.approx(5.0)


def test_a_streamer_whose_game_has_started_is_not_offered():
    roster = full_roster()
    played = {**pool_row("Played D/ST", "D/ST", 30.0),
              lu.STATE_COLUMN: "post", lu.LOCKED_COLUMN: True}
    assert wv.best_streamers([played], roster, FLEX_SLOTS, "TRUE_Points") == []


# --- the table the panel leads with ---------------------------------------

def test_the_threshold_table_marks_the_slot_the_wire_can_improve():
    roster = full_roster()
    pool = [pool_row("Great TE", "TE", 30.0), pool_row("Poor QB", "QB", 2.0)]
    rows = {r["slot"]: r for r in
            wv.threshold_table(roster, pool, FLEX_SLOTS, "TRUE_Points")}
    assert rows["TE"]["beats"] is True
    assert rows["QB"]["beats"] is False


def test_the_threshold_table_covers_every_starting_slot():
    rows = wv.threshold_table(full_roster(), [], FLEX_SLOTS, "TRUE_Points")
    assert {r["slot"] for r in rows} == set(FLEX_SLOTS)


# --- roster composition ---------------------------------------------------
#
# ESPN publishes `positionLimits` and nothing here had ever read it. The engine
# could therefore propose swaps ESPN would refuse.

GOP_LIMITS = {"QB": 2, "RB": 3, "WR": 5, "TE": 2, "K": 2, "D/ST": 2, "LB": 2}


def test_a_player_on_ir_does_not_count_against_the_limit():
    """The same fact `droppable_for` turns on from the other side: IR sits outside
    the roster. It is why a team can hold six receivers against a limit of five."""
    roster = [player("WR1", "WR", 10.0, slot="WR"),
              player("Hurt", "WR", 0.0, slot="IR")]
    assert lu.position_counts(roster) == {"WR": 1}


def test_you_cannot_add_a_position_you_are_already_full_at():
    """Tommy Winfield's GOP roster, 2026 week 1: LB 2/2, RB 3/3, WR 5/5. The
    engine's best pairing for him was add Alex Singleton (LB) / drop Jonah Coleman
    (RB) at +0.45 -- a move ESPN would simply refuse."""
    roster = [player("LB1", "LB", 11.0, slot="DP"), player("LB2", "LB", 10.0),
              player("RB3", "RB", 3.7)]
    assert not lu.within_position_limits(
        roster, GOP_LIMITS, pool_row("New LB", "LB", 12.0), roster[2])


def test_dropping_the_same_position_does_make_room():
    roster = [player("LB1", "LB", 11.0, slot="DP"), player("LB2", "LB", 10.0)]
    assert lu.within_position_limits(
        roster, GOP_LIMITS, pool_row("New LB", "LB", 12.0), roster[1])


def test_dropping_an_ir_player_of_that_position_does_not_make_room():
    """He was never counted, so giving him up frees nothing at the cap."""
    roster = [player("LB1", "LB", 11.0, slot="DP"), player("LB2", "LB", 10.0),
              player("LB3", "LB", 0.0, slot="IR")]
    assert not lu.within_position_limits(
        roster, GOP_LIMITS, pool_row("New LB", "LB", 12.0), roster[2])


def test_an_unknown_limit_permits():
    """A store written before the limits were recorded must behave as it did."""
    roster = [player("LB1", "LB", 11.0), player("LB2", "LB", 10.0)]
    assert lu.within_position_limits(roster, {}, pool_row("New LB", "LB", 12.0),
                                     roster[0])


def test_a_negative_limit_means_no_limit():
    """ESPN writes -1 for unlimited, which every league but the tight one uses for
    individual defenders."""
    roster = [player("LB1", "LB", 11.0), player("LB2", "LB", 10.0)]
    assert lu.within_position_limits(roster, {"LB": -1},
                                     pool_row("New LB", "LB", 12.0), roster[0])


def test_the_engine_picks_a_legal_drop_rather_than_the_best_illegal_one():
    """**What the fix is actually worth.** It does not suppress the move, it
    corrects it: across the limit-bearing leagues it turned "add the Titans
    defence, drop TreVeyon Henderson" into "drop the Bills defence", which is the
    only kind of drop that makes room for a defence.

    The second defence here is worth more than the spare receiver, so the
    unconstrained engine reaches past it for the cheaper drop -- and that cheaper
    drop is the illegal one.
    """
    roster = full_roster() + [player("DST2", "D/ST", 6.0)]
    pool = [pool_row("Great D/ST", "D/ST", 20.0)]

    loose = wv.rank_moves(roster, pool, FLEX_SLOTS, "TRUE_Points", limits={})
    tight = wv.rank_moves(roster, pool, FLEX_SLOTS, "TRUE_Points",
                          limits={"D/ST": 2})

    assert [m.add["player_name"] for m in loose] == ["Great D/ST"]
    assert loose[0].drop["player_position"] == "WR", (
        "unconstrained, it gives up the cheapest man on the roster")

    assert [m.add["player_name"] for m in tight] == ["Great D/ST"]
    assert tight[0].drop["player_position"] == "D/ST", (
        "a defence can only be added by giving up a defence")


# --- insurance, past a single absence -------------------------------------

#: GOP Degenerates' shape: **one** dedicated RB slot, plus a flex a receiver can
#: fill. `FLEX_SLOTS` starts two backs, which is precisely the shape in which a
#: third back is ordinary depth rather than the last legal body.
GOP_SLOTS = {"QB": 1, "RB": 1, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1}


def two_elite_backs():
    """The shape that breaks a one-absence test: two good backs, one dedicated
    ``RB`` slot, a flex that a receiver can fill, and a third back behind them."""
    return [
        player("QB1", "QB", 20.0, slot="QB"),
        player("Elite RB1", "RB", 22.0, slot="RB"),
        player("Elite RB2", "RB", 24.0, slot="RB/WR/TE"),
        player("Spare RB", "RB", 3.7),
        player("WR1", "WR", 14.0, slot="WR"),
        player("WR2", "WR", 13.0, slot="WR"),
        player("WR3", "WR", 12.0),
        player("TE1", "TE", 9.0, slot="TE"),
        player("K1", "K", 8.0, slot="K"),
        player("DST1", "D/ST", 7.0, slot="D/ST"),
    ]


def test_a_third_back_is_worth_nothing_when_only_one_elite_sits():
    """The flex simply takes a receiver, so he never enters the lineup."""
    roster = two_elite_backs()
    spare = next(r for r in roster if r["player_name"] == "Spare RB")["player_id"]
    value, depth = lu.insurance_detail(roster, GOP_SLOTS, "TRUE_Points", spare,
                                       max_absences=1)
    assert value == pytest.approx(0.0)
    assert depth == 0


def test_a_third_back_is_the_only_legal_body_when_both_sit():
    """**The case a one-absence test cannot see**, and the reason the engine came
    to propose giving Jonah Coleman up for a linebacker worth +0.45. A receiver
    cannot fill a dedicated `RB` slot, so with both elites out he is all there is.
    Measured on the real roster: 0.00, 0.00, and +3.69."""
    roster = two_elite_backs()
    spare = next(r for r in roster if r["player_name"] == "Spare RB")
    value, depth = lu.insurance_detail(roster, GOP_SLOTS, "TRUE_Points",
                                       spare["player_id"])
    assert depth == 2
    assert value == pytest.approx(spare["TRUE_Points"])


def test_depth_is_capped_so_the_number_stays_a_number():
    """Each extra absence considers a rarer event with a larger value. Two is where
    a capped room's last man first becomes load-bearing; three would just be a
    bigger number about a less likely week."""
    assert lu.MAX_COVERED_ABSENCES == 2


def test_a_player_who_covers_nobody_still_insures_nothing():
    """A fifth receiver behind four better ones, in a league starting two plus a
    flex: even two absences leave somebody ahead of him, so he is free to drop.

    This is the control for the test above. Without it, "insurance is non-zero"
    would just mean "we now look two deep" rather than "this player is load-bearing".
    """
    roster = two_elite_backs() + [player("WR4", "WR", 11.0),
                                  player("WR5", "WR", 1.0)]
    wr5 = roster[-1]["player_id"]
    assert lu.insurance_value(roster, GOP_SLOTS, "TRUE_Points", wr5) == pytest.approx(0.0)
