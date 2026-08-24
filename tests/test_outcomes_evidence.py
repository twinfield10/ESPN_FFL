"""The vacancy measurements behind plan 28.

What is pinned here is mostly the two bugs that building this caught, because both were
invisible in the output: a **nondeterministic room rank** that moved the published figures
between runs, and a **skill-position filter** that deleted the players the question was
about while leaving the cohort size unchanged.

The rest pins the accounting identity the redistribution rule depends on -- that a
vacancy's opportunity is conserved between the lead's loss and the room's gain, since a
rule fitted on shares that do not sum is a rule inventing volume.

Synthetic frames. No network, no parquet.
"""

import polars as pl
import pytest

from Scripts.outcomes import evidence as ev


def weeks(rows):
    """A box-score frame in the shape :func:`Scripts.outcomes.evidence.load_weeks` returns."""
    return pl.DataFrame(
        [{"season": s, "week": w, "gsis_id": g, "player_display_name": g, "team": t,
          "position": p, "carries": c, "targets": tg, "attempts": 0,
          "fantasy_points_ppr": float(c + tg)}
         for s, w, g, t, p, c, tg in rows],
        schema={"season": pl.Int32, "week": pl.Int32, "gsis_id": pl.String,
                "player_display_name": pl.String, "team": pl.String,
                "position": pl.String, "carries": pl.Int64, "targets": pl.Int64,
                "attempts": pl.Int64, "fantasy_points_ppr": pl.Float64})


def backfield(lead_weeks, n_weeks=10, lead_opp=20, rb2_in=5, rb2_out=15, rb3=3):
    """One team-season: a lead back present for ``lead_weeks``, then absent."""
    rows = []
    for w in range(1, n_weeks + 1):
        lead_here = w <= lead_weeks
        if lead_here:
            rows.append((2020, w, "lead", "KC", "RB", lead_opp, 0))
        rows.append((2020, w, "rb2", "KC", "RB", rb2_in if lead_here else rb2_out, 0))
        rows.append((2020, w, "rb3", "KC", "RB", rb3, 0))
    return weeks(rows)


# --- the rank must be a measurement, not a draw ---------------------------

def test_tied_players_get_a_stable_rank():
    """Two backs with identical season volume must not swap ranks between calls.

    ``rank("ordinal")`` numbered ties in row order, and row order out of a threaded
    ``group_by`` is not stable, so this returned different figures on consecutive runs.
    """
    tied = weeks([(2020, w, g, "KC", "RB", 10, 0)
                  for w in range(1, 6) for g in ("aaa", "bbb", "ccc")])
    ranks = [ev._ranked(tied, "RB").select("gsis_id", "rk").unique().sort("gsis_id")
             for _ in range(6)]
    assert all(r.equals(ranks[0]) for r in ranks)
    # And the tie-break is the documented one: gsis_id ascending.
    assert ranks[0]["rk"].to_list() == [1, 2, 3]


def test_the_closure_report_is_identical_across_runs():
    """Both frames closure returns, repeated: same values and same row order."""
    frame = backfield(lead_weeks=6)
    group, by_rank = ev.closure(frame, "RB")
    for _ in range(5):
        again_group, again_rank = ev.closure(frame, "RB")
        assert again_group.equals(group)
        assert again_rank.equals(by_rank)


# --- the accounting the redistribution rule rests on ----------------------

def test_vacated_opportunity_is_conserved_when_the_room_absorbs_it():
    """The lead's loss equals the room's gain, when the synthetic room is zero-sum.

    Pins the identity a share is fitted against. ``rb2_out - rb2_in`` plus rb3's gain
    must equal the lead's volume, or a share fitted on it invents work.
    """
    frame = backfield(lead_weeks=8, n_weeks=10, lead_opp=20, rb2_in=5, rb2_out=25, rb3=3)
    _, by_rank = ev.closure(frame, "RB", min_in=3, min_out=2)
    got = {(r["rk"], r["lead_played"]): r["opp"] for r in by_rank.to_dicts()}
    assert got[(1, True)] == pytest.approx(20.0)
    assert got[(2, True)] == pytest.approx(5.0)
    assert got[(2, False)] == pytest.approx(25.0)
    # rank 2 absorbs the whole vacancy; rank 3 is flat.
    assert got[(2, False)] - got[(2, True)] == pytest.approx(20.0)
    assert got[(3, False)] == pytest.approx(got[(3, True)])


def test_a_week_the_team_played_with_no_back_is_still_a_week():
    """The grid comes from team-weeks, not from the position's own appearances.

    Building it from appearances would drop a week in which the whole room was inactive,
    which is silently the opposite of what a vacancy measurement wants to count.
    """
    rows = [(2020, w, "lead", "KC", "RB", 20, 0) for w in range(1, 6)]
    rows += [(2020, w, "rb2", "KC", "RB", 5, 0) for w in range(1, 6)]
    # Weeks 6-8: no back appears at all, but the team plays (a receiver does).
    rows += [(2020, w, "wr1", "KC", "WR", 0, 8) for w in range(1, 9)]
    frame = weeks(rows)
    state = ev._lead_state(frame, ev._ranked(frame, "RB"))
    assert state.height == 8
    assert state.filter(~pl.col("lead_played")).height == 3


def test_a_backup_gets_a_zero_rather_than_a_missing_row():
    """A backup who was inactive scored nothing; dropping the row would flatter him."""
    rows = [(2020, w, "lead", "KC", "RB", 20, 0) for w in range(1, 5)]
    rows += [(2020, w, "rb2", "KC", "RB", 6, 0) for w in range(1, 9)]
    # rb3 plays only while the lead is out, so his lead-in mean must be 0, not 4.
    rows += [(2020, w, "rb3", "KC", "RB", 4, 0) for w in range(5, 9)]
    got = ev.transfer(weeks(rows), "RB", min_each=4).to_dicts()
    rb3 = next(r for r in got if r["rk"] == 3)
    assert rb3["opp_in"] == pytest.approx(0.0)
    assert rb3["opp_out"] == pytest.approx(4.0)


# --- the filter that deleted the severe tail ------------------------------

def test_a_player_who_appeared_in_no_game_has_no_position_to_filter_on():
    """The premise behind ``fragility`` joining raw availability, not ``player_seasons``.

    Position is read off the box score, so a player who appeared in no game has none. A
    left join through a frame filtered to :data:`Scripts.outcomes.evidence.SKILL`
    therefore yields a *null outcome* rather than a dropped row -- and a null is invisible
    in a row count, which is how this deleted the 8 of 217 incumbents who missed an entire
    season while the cohort size stayed 217.
    """
    played = weeks([(2020, w, "rb", "KC", "RB", 10, 0) for w in range(1, 6)])
    positionless = played.head(1).with_columns(
        pl.lit("mystery").alias("gsis_id"),
        pl.lit(None, dtype=pl.String).alias("position"))
    positions = (pl.concat([played, positionless])
                 .group_by(["season", "gsis_id"])
                 .agg(pl.col("position").drop_nulls().first().alias("position")))
    assert positions.filter(pl.col("gsis_id") == "mystery")["position"].item() is None

    skill_only = positions.filter(pl.col("position").is_in(ev.SKILL))
    assert set(skill_only["gsis_id"]) == {"rb"}

    cohort = pl.DataFrame({"season": [2020, 2020], "gsis_id": ["rb", "mystery"]},
                          schema={"season": pl.Int32, "gsis_id": pl.String})
    through_filter = cohort.join(skill_only, on=["season", "gsis_id"], how="left")
    assert through_filter.height == cohort.height          # the row count says nothing
    assert through_filter["position"].null_count() == 1    # but the outcome is gone


# --- a caveat the measurement carries, pinned so it is not forgotten ------

def test_a_lead_who_misses_enough_games_stops_being_rank_one():
    """Rank is the season's own opportunity total, so the roles can invert.

    Not a bug -- there is no leakage-free pre-season rank inside a descriptive
    measurement -- but it makes :func:`closure` **conservative**: the team-seasons where
    an absence was long enough to hand the backup the season lead are counted with the
    roles swapped, so the measured transfer understates the extreme cases rather than
    overstating them. Any redistribution share fitted from it inherits that direction.
    """
    frame = backfield(lead_weeks=4, n_weeks=10, lead_opp=20, rb2_in=5, rb2_out=20, rb3=3)
    ranks = dict(ev._ranked(frame, "RB").select("gsis_id", "rk").unique().iter_rows())
    assert ranks["lead"] == 2      # 4 x 20 = 80
    assert ranks["rb2"] == 1       # 4 x 5 + 6 x 20 = 140


# --- spillover -----------------------------------------------------------

def test_spillover_reports_every_room_and_the_pass_volume():
    rows = []
    for w in range(1, 9):
        if w <= 5:
            rows.append((2020, w, "wr1", "KC", "WR", 0, 10))
        rows.append((2020, w, "wr2", "KC", "WR", 0, 5))
        rows.append((2020, w, "rb1", "KC", "RB", 12, 3))
        rows.append((2020, w, "te1", "KC", "TE", 0, 4))
    got = ev.spillover(weeks(rows), "WR").to_dicts()
    assert {r["lead_played"] for r in got} == {True, False}
    absent = next(r for r in got if not r["lead_played"])
    present = next(r for r in got if r["lead_played"])
    assert present["WR"] == pytest.approx(15.0)
    assert absent["WR"] == pytest.approx(5.0)
    # The other rooms are flat in this fixture, which is what makes a real shift readable.
    assert absent["TE"] == pytest.approx(present["TE"])
    assert absent["RB"] == pytest.approx(present["RB"])
