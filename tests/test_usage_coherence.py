"""Team coherence for TOMCAT's own column -- phase 1 of plan 31.

What is pinned here is not "the ratio comes out at 1.0", which is arithmetic and
would pass on a broken implementation too. It is the three ways this operation can
be wrong quietly:

* **Fabricating volume.** The rule plan 31 asked for -- scale every room to the slate
  -- multiplies a line that is already on a full slate, and took Miami to 8,268
  projected passing yards off a 3.7-game backup. The cap is the fix and
  ``test_a_short_room_is_left_alone`` is what keeps it.
* **Reconciling the free-agent bucket.** ESPN files every unsigned player under the
  literal string ``"None"``, 522 of them on the 2026 board. Treated as a franchise,
  a dozen unsigned quarterbacks get scaled into one seventeen-game season.
* **Filling an abstention.** A null is the model declining, and it has to stay null
  through both passes -- a 0.0 that means "nothing here" reads to the blend as a
  confident zero.

Synthetic frames throughout. No network, no parquet.
"""

import polars as pl
import pytest

from Scripts.usage import coherence as ch

PASS, REC = "USG_passingYards", "USG_receivingYards"


def roster(rows):
    """A frame in the shape the coherence pass expects.

    Args:
        rows: ``(team, position, expected_games, passing, receiving)`` tuples.

    Returns:
        pl.DataFrame: One row per tuple.
    """
    return pl.DataFrame(
        {"pro_team": [r[0] for r in rows],
         "primaryPosition": [r[1] for r in rows],
         "usg_expected_games": [float(r[2]) for r in rows],
         PASS: [r[3] for r in rows],
         REC: [r[4] for r in rows]},
        schema_overrides={PASS: pl.Float64, REC: pl.Float64})


def team_total(frame, column, team="ATL"):
    """Sum one column over one team."""
    return frame.filter(pl.col("pro_team") == team)[column].sum()


# --- the quarterback room ------------------------------------------------------

def test_two_quarterbacks_do_not_each_play_a_season():
    """Atlanta's failure: two passers, both priced on a full slate, summed."""
    frame = roster([("ATL", "QB", 10.8, 3634.0, None),
                    ("ATL", "QB", 11.2, 2404.0, None),
                    ("ATL", "WR", 15.0, None, 1200.0)])
    out = ch.normalise_qb_room(frame)

    assert team_total(frame, PASS) == pytest.approx(6038.0)
    # 17 / 22.0 of the room, applied to both passers.
    assert team_total(out, PASS) == pytest.approx(6038.0 * 17.0 / 22.0)
    assert out[ch.ROOM_SCALE_COLUMN][0] == pytest.approx(17.0 / 22.0)


def test_a_short_room_is_left_alone():
    """The Miami case. An already-full-slate line must never be scaled *up*."""
    frame = roster([("MIA", "QB", 3.7, 2821.9, None),
                    ("MIA", "WR", 15.0, None, 900.0)])
    out = ch.normalise_qb_room(frame)

    assert team_total(out, PASS, team="MIA") == pytest.approx(2821.9)
    assert out[ch.ROOM_SCALE_COLUMN][0] == pytest.approx(1.0)


def test_the_room_diagnostic_records_what_was_there_before():
    """``usg_team_qb_games`` is the raw sum, not the normalised one."""
    frame = roster([("ATL", "QB", 10.8, 3634.0, None),
                    ("ATL", "QB", 11.2, 2404.0, None)])
    out = ch.normalise_qb_room(frame)
    assert out[ch.TEAM_GAMES_COLUMN][0] == pytest.approx(22.0)


def test_expected_games_is_not_rewritten():
    """The board's "Exp G" column is a player's availability and stays one.

    Plan 31 asked for the room's games to be normalised to the slate. Doing that to
    the per-player column would print "17.0" beside a quarterback the model believes
    plays four games, and plan 27's injury work reads that number.
    """
    frame = roster([("ATL", "QB", 10.8, 3634.0, None),
                    ("ATL", "QB", 11.2, 2404.0, None)])
    out = ch.normalise_qb_room(frame)
    assert out["usg_expected_games"].to_list() == [10.8, 11.2]


def test_an_abstaining_quarterback_does_not_count_toward_the_room():
    """No line means no projection to make room for.

    Counting his games would shrink the passers who *were* projected, in order to
    leave space for one that is not there.
    """
    frame = roster([("ATL", "QB", 10.8, 3634.0, None),
                    ("ATL", "QB", 11.2, None, None)])
    out = ch.normalise_qb_room(frame)
    assert out[ch.TEAM_GAMES_COLUMN][0] == pytest.approx(10.8)
    assert team_total(out, PASS) == pytest.approx(3634.0)


def test_free_agents_are_not_a_franchise():
    """ESPN's unsigned bucket must not be scaled into one shared season."""
    frame = roster([("None", "QB", 12.0, 3000.0, None),
                    ("None", "QB", 12.0, 2000.0, None)])
    out = ch.normalise_qb_room(frame)
    assert team_total(out, PASS, team="None") == pytest.approx(5000.0)


def test_a_quarterbacks_other_lines_move_with_him():
    """The factor is a share of the room's snaps, so it is not passing-only."""
    frame = pl.DataFrame({
        "pro_team": ["ATL", "ATL"], "primaryPosition": ["QB", "QB"],
        "usg_expected_games": [11.0, 11.0],
        PASS: [3000.0, 2000.0], "USG_rushingYards": [400.0, 100.0],
        REC: [None, None]}, schema_overrides={REC: pl.Float64})
    out = ch.normalise_qb_room(frame)
    assert out["USG_rushingYards"].sum() == pytest.approx(500.0 * 17.0 / 22.0)


# --- the identity --------------------------------------------------------------

def test_the_identity_closes_on_the_midpoint():
    """Both sides move by half the discrepancy; neither absorbs the whole of it."""
    frame = roster([("BUF", "QB", 15.0, 4000.0, None),
                    ("BUF", "WR", 15.0, None, 3000.0)])
    out = ch.reconcile_identities(frame)
    assert out[PASS][0] == pytest.approx(3500.0)
    assert out[REC][1] == pytest.approx(3500.0)


def test_a_team_with_one_empty_side_is_left_alone():
    """A missing side is no identity to enforce -- scaling it is a deletion."""
    frame = roster([("BUF", "QB", 15.0, 4000.0, None)])
    out = ch.reconcile_identities(frame)
    assert out[PASS][0] == pytest.approx(4000.0)


def test_receivers_share_the_correction_in_proportion():
    """The team total is what is fixed; the shares stay where the model put them."""
    frame = roster([("BUF", "QB", 15.0, 4000.0, None),
                    ("BUF", "WR", 15.0, None, 2000.0),
                    ("BUF", "TE", 15.0, None, 1000.0)])
    out = ch.reconcile_identities(frame)
    assert out[REC][1] / out[REC][2] == pytest.approx(2.0)
    assert out[REC].sum() == pytest.approx(3500.0)


def test_an_abstention_stays_absent_through_both_passes():
    """A null is the model declining and must not become a zero."""
    frame = roster([("BUF", "QB", 15.0, 4000.0, None),
                    ("BUF", "WR", 15.0, None, 3000.0),
                    ("BUF", "WR", 12.0, None, None)])
    out = ch.make_coherent(frame)
    assert out[REC][2] is None
    assert out[PASS][1] is None


def test_a_pair_the_model_does_not_project_is_skipped():
    """No completions column, so there is nothing to reconcile receptions against."""
    frame = roster([("BUF", "QB", 15.0, 4000.0, None),
                    ("BUF", "WR", 15.0, None, 3000.0)])
    out = ch.reconcile_identities(
        frame, identities=(("passingCompletions", "receivingReceptions"),))
    assert out[PASS][0] == pytest.approx(4000.0)


# --- both, in order ------------------------------------------------------------

def test_the_room_is_trimmed_before_the_midpoint_is_taken():
    """Order matters: a midpoint against a double-counted room is still too high."""
    frame = roster([("ATL", "QB", 11.0, 3634.0, None),
                    ("ATL", "QB", 11.0, 2404.0, None),
                    ("ATL", "WR", 15.0, None, 4186.0)])
    identity_only = ch.reconcile_identities(frame)
    both = ch.make_coherent(frame)

    assert team_total(identity_only, PASS) == pytest.approx(5112.0, rel=1e-3)
    assert team_total(both, PASS) < team_total(identity_only, PASS)
    assert team_total(both, PASS) == pytest.approx(team_total(both, REC))


def test_a_frame_with_no_team_column_is_returned_unchanged():
    """The shipping path guards on this; so does the operation itself."""
    frame = pl.DataFrame({"primaryPosition": ["QB"], PASS: [4000.0]})
    assert ch.make_coherent(frame).equals(frame)
