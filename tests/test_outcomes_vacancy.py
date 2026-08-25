"""What the redistribution rule must not get wrong.

The accounting identity is the thing worth pinning: opportunity vacated by a lead must
equal what the room recaptures plus what leaves it, and a rule that quietly invents or
loses work would still produce a plausible-looking share table. The rest guards the
decisions -- that a receiver room gets no rule, and that the depth-chart-scale collapse
conserves the total it collapses.

Synthetic frames. No network, no parquet.
"""

import polars as pl
import pytest

from Scripts.outcomes import evidence as ev
from Scripts.outcomes import vacancy as vac


# --- fixtures ------------------------------------------------------------

def backfield(lead_weeks: int, n_weeks: int = 12, lead_opp: int = 20,
              rb2_in: int = 5, rb2_out: int = 13, rb3: int = 3,
              position: str = "RB") -> pl.DataFrame:
    """One team-season: a lead present for ``lead_weeks``, then absent.

    The rank-2 back is handed exactly ``rb2_out - rb2_in`` of the lead's vacated work and
    the rank-3 back none, so every share in the result is a number the test chose.

    Opportunity goes into the column the position is actually ranked on
    (:data:`Scripts.outcomes.evidence.OPPORTUNITY`) -- a receiver room is ranked on
    targets and a backfield on carries plus targets, so putting a receiver's work in
    ``carries`` produces an empty room rather than a wrong answer.
    """
    def opp(value):
        return (value, 0) if "carries" in ev.OPPORTUNITY[position] else (0, value)

    rows = []
    for week in range(1, n_weeks + 1):
        present = week <= lead_weeks
        if present:
            rows.append((2024, week, "00-lead", "AAA", position, *opp(lead_opp), 0.0))
        rows.append((2024, week, "00-rb2", "AAA", position,
                     *opp(rb2_in if present else rb2_out), 0.0))
        rows.append((2024, week, "00-rb3", "AAA", position, *opp(rb3), 0.0))
    return pl.DataFrame(
        rows,
        schema={"season": pl.Int32, "week": pl.Int32, "gsis_id": pl.String,
                "team": pl.String, "position": pl.String, "carries": pl.Int64,
                "targets": pl.Int64, "fantasy_points_ppr": pl.Float64},
        orient="row").with_columns(pl.lit(0, dtype=pl.Int64).alias("attempts"),
                                   pl.col("gsis_id").alias("player_display_name"))


# --- the accounting ------------------------------------------------------

def test_the_share_is_the_gain_over_what_the_lead_vacated():
    """The estimator, on numbers chosen so the answer is arithmetic.

    The lead vacates 20 a game. The rank-2 back goes from 5 to 13, so he takes 8 of the
    20 -- 0.40. Rank 3 never moves, so he takes none. Anything else means the estimator
    is not measuring what its name says.
    """
    table = vac.fit(backfield(lead_weeks=8), positions=("RB",))
    shares = dict(table.select("rank", "share").rows())
    assert shares[2] == pytest.approx(0.40)
    assert shares[3] == pytest.approx(0.0)


def test_a_room_that_absorbs_everything_recaptures_all_of_it():
    """The conservation end of the identity: nothing leaves, so the shares sum to one."""
    table = vac.fit(backfield(lead_weeks=8, lead_opp=20, rb2_in=5, rb2_out=25),
                    positions=("RB",))
    assert table["recapture"][0] == pytest.approx(1.0)


def test_work_that_leaves_the_room_is_not_recaptured():
    """A room that shrinks must report a recapture below one rather than rescaling
    itself to hide the shrinkage -- the RB room keeps 93% of its volume and the TE room
    68%, and that difference is the reason this plan treats them differently."""
    table = vac.fit(backfield(lead_weeks=8, lead_opp=20, rb2_in=5, rb2_out=11),
                    positions=("RB",))
    assert table["recapture"][0] == pytest.approx(0.30)


def test_the_lead_who_vacates_is_measured_while_he_is_present():
    """``vacated`` is the lead's own per-week opportunity, not a room average."""
    table = vac.fit(backfield(lead_weeks=8, lead_opp=20), positions=("RB",))
    assert table["vacated"][0] == pytest.approx(20.0)


# --- the sample restriction ----------------------------------------------

def test_a_team_that_never_lost_its_lead_is_not_in_the_sample():
    """The comparison is within-team, so a team-season with only one state contributes
    nothing. Without the restriction the table would be comparing teams whose leads got
    hurt against teams whose leads did not, which is a different question."""
    assert vac.fit(backfield(lead_weeks=12), positions=("RB",)).is_empty()


def test_a_team_that_lost_its_lead_for_one_week_only_is_not_in_the_sample():
    """``min_out`` is 2. One week is a rest day, not a vacancy."""
    assert vac.fit(backfield(lead_weeks=11, n_weeks=12), positions=("RB",)).is_empty()


# --- the decisions -------------------------------------------------------

def test_a_receiver_room_is_measured_but_never_applied():
    """The do-not-build decision, in code rather than in a docstring.

    45% recapture, and the understudy gains 0.59 of 7.72 targets. A caller iterating
    ``applied_rule`` must not be able to give a receiver room a transfer by accident.
    """
    weeks = backfield(lead_weeks=8, position="WR")
    table = vac.fit(weeks, positions=("WR",))
    assert table.height and not table["applied"].any()
    assert "WR" not in vac.applied_rule(table)


def test_the_depth_chart_collapse_conserves_what_it_collapses():
    """Ranks 3 and 4 are pooled because ``depth_rank`` is clipped to 3 and cannot tell
    them apart. Pooling must move work between ranks, never create or destroy it."""
    table = vac.fit(backfield(lead_weeks=8, lead_opp=20, rb2_in=5, rb2_out=13, rb3=3),
                    positions=("RB",))
    shares = dict(table.select("rank", "share").rows())
    rule = vac.applied_rule(table)["RB"]
    assert rule["rank_2"] == pytest.approx(shares[2])
    assert rule["rank_2"] + rule["rank_rest"] == pytest.approx(table["recapture"][0])


# --- determinism ---------------------------------------------------------

def test_the_fit_is_identical_across_runs():
    """The bootstrap is seeded and the room rank is totally ordered, so a refit of the
    same data is the same artifact. ``evidence.py`` had to learn this the hard way when
    tied reserves swapped places and moved a TE room's volume between runs."""
    weeks = backfield(lead_weeks=8)
    assert vac.fit(weeks, positions=("RB",)).equals(vac.fit(weeks, positions=("RB",)))


def test_the_standard_error_is_reported_beside_every_share():
    """``handcuff.py`` ships ``handcuff_r2`` so its column cannot be over-read. Same
    rule here: a share with no error bar invites being treated as exact."""
    table = vac.fit(backfield(lead_weeks=8), positions=("RB",))
    assert "share_se" in table.columns
    assert table["share_se"].is_not_null().all()


def test_a_missing_artifact_names_the_command_that_builds_it(tmp_path):
    """The contract every optional artifact in this repo has."""
    with pytest.raises(FileNotFoundError, match="Scripts.outcomes.vacancy --write"):
        vac.load(tmp_path / "absent.parquet")
