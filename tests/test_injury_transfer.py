"""The vacancy transfer, applied to a mean rather than to a simulation.

What is pinned here is not "the arithmetic adds up". It is the four ways this moves
volume it should not:

* **Onto a receiver room.** 45% of a lead receiver's targets reappear, his understudy
  gains 0.59 of 7.72, and the offence throws 1.25 fewer times -- so a WR room has no
  transfer rule, and a rule that grew one would invent 2.8 targets a game.
* **Out of nowhere.** A vacancy nobody can size must abstain, not fabricate. A
  season-ending starter has ``USG_`` withdrawn and ``TRUE_`` at zero, which is why the
  healthy line is stashed before the injury adjustment removes it.
* **Onto a healthy room.** No absence, no transfer -- the mechanism has to be as
  vacancy-specific as its evidence claims.
* **More than was vacated.** At most 81.4% of a back's line reappears; the rest leaves
  the offence and that is the measurement, not a rounding loss.

Synthetic frames throughout. No network, no parquet.
"""

import pandas as pd
import pytest

from Scripts.injury import transfer as tr

SHARES = {"RB": (0.410, 0.404), "TE": (0.263, 0.208)}


def room(rows):
    frame = pd.DataFrame(rows)
    for stat in tr.TRANSFER_STATS:
        column = f"TRUE_{stat}"
        if column not in frame.columns:
            frame[column] = 0.0
    return frame


def _lead(**kw):
    base = {"pro_team": "ARI", "primaryPosition": "RB", "usg_depth_rank": 1,
            "inj_expected_absence_weeks": 5.0, "TRUE_rushingYards": 1071.0,
            f"{tr.HEALTHY_PREFIX}rushingYards": 1434.0}
    base.update(kw)
    return base


def _backup(rank=2, yards=418.0, **kw):
    base = {"pro_team": "ARI", "primaryPosition": "RB", "usg_depth_rank": rank,
            "inj_expected_absence_weeks": float("nan"), "TRUE_rushingYards": yards}
    base.update(kw)
    return base


def test_the_backup_inherits_the_fitted_share_discounted_by_role():
    """Not the raw share. Plan 28 fitted it to a room's *realised* order and a
    projection only has a pre-season chart, so the payout is scaled by how often a
    listed backup really is the inheritor -- which is what takes the walk-forward gain
    from 1.72% in 5 folds of 6 to 2.14% in all six."""
    frame = room([_lead(), _backup()])
    out = tr.redistribute(frame, shares=SHARES)
    vacated = 1434.0 * (5.0 / tr.SLATE)
    expected = 418.0 + 0.410 * tr.DEFAULT_ROLE_HOLD * vacated
    assert out.loc[1, "TRUE_rushingYards"] == pytest.approx(expected)


def test_a_rookie_backup_inherits_less_than_a_settled_one():
    """Plan 33's cohort split, in the currency this spends: a listed second-stringer
    holds the job 47% of the time settled and 32% as a rookie."""
    def inherited(cohort):
        frame = room([_lead(), _backup(usg_role_cohort=cohort)])
        frame["usg_role_cohort"] = [None, cohort]
        return tr.redistribute(frame, shares=SHARES).loc[1, tr.INHERITED_COLUMN]
    assert inherited("settled") > inherited("mover") > inherited("rookie")


def test_the_vacating_starter_is_not_docked_again():
    """His absence is already priced -- ESPN had Pearsall at 0.0 and ``USG_`` is scaled
    on top. Docking him here would count the same injury twice."""
    frame = room([_lead(), _backup()])
    out = tr.redistribute(frame, shares=SHARES)
    assert out.loc[0, "TRUE_rushingYards"] == pytest.approx(1071.0)


def test_a_healthy_room_is_untouched():
    frame = room([_lead(inj_expected_absence_weeks=float("nan")), _backup()])
    out = tr.redistribute(frame, shares=SHARES)
    assert out.loc[1, "TRUE_rushingYards"] == pytest.approx(418.0)
    assert out[tr.INHERITED_COLUMN].sum() == pytest.approx(0.0)


def test_a_receiver_room_gets_nothing():
    """The finding, not an omission. See the module docstring."""
    frame = room([_lead(primaryPosition="WR"), _backup(primaryPosition="WR")])
    out = tr.redistribute(frame, shares=SHARES)
    assert out.loc[1, "TRUE_rushingYards"] == pytest.approx(418.0)


def test_no_more_than_the_fitted_share_ever_leaves_the_starter():
    """81.4% is the ceiling before the role discount, and the discount only lowers it.
    What must never happen is a room inheriting more than was vacated."""
    frame = room([_lead(), _backup(), _backup(rank=3, yards=76.0)])
    out = tr.redistribute(frame, shares=SHARES)
    vacated = 1434.0 * (5.0 / tr.SLATE)
    assert out[tr.INHERITED_COLUMN].sum() < (0.410 + 0.404) * vacated
    assert out[tr.INHERITED_COLUMN].sum() < vacated


def test_ranks_below_two_split_in_proportion_to_their_own_baseline():
    frame = room([_lead(), _backup(rank=3, yards=90.0), _backup(rank=3, yards=30.0)])
    out = tr.redistribute(frame, shares=SHARES)
    assert (out.loc[1, tr.INHERITED_COLUMN]
            == pytest.approx(3 * out.loc[2, tr.INHERITED_COLUMN]))


def test_a_room_with_no_baseline_at_all_splits_evenly_rather_than_dividing_by_zero():
    frame = room([_lead(), _backup(rank=3, yards=0.0), _backup(rank=3, yards=0.0)])
    out = tr.redistribute(frame, shares=SHARES)
    assert out.loc[1, tr.INHERITED_COLUMN] == pytest.approx(out.loc[2, tr.INHERITED_COLUMN])
    assert out[tr.INHERITED_COLUMN].sum() > 0


def test_a_season_ending_starter_still_sizes_his_vacancy():
    """`TRUE_` is zero and `USG_` was withdrawn, so without the stashed healthy line
    the largest vacancy on the board would silently transfer nothing."""
    frame = room([_lead(inj_expected_absence_weeks=18.0, TRUE_rushingYards=0.0),
                  _backup()])
    out = tr.redistribute(frame, shares=SHARES)
    assert out.loc[1, "TRUE_rushingYards"] > 418.0


def test_a_vacancy_that_cannot_be_sized_abstains():
    """No healthy line and nothing to gross up. Fabricating one is the failure this
    repo keeps finding: an absent source reading as agreement."""
    frame = room([_lead(inj_expected_absence_weeks=18.0, TRUE_rushingYards=0.0,
                        **{f"{tr.HEALTHY_PREFIX}rushingYards": float("nan")}),
                  _backup()])
    out = tr.redistribute(frame, shares=SHARES)
    assert out.loc[1, "TRUE_rushingYards"] == pytest.approx(418.0)


def test_a_frame_without_the_injury_columns_is_returned_unchanged():
    frame = room([_lead(), _backup()]).drop(columns=["inj_expected_absence_weeks"])
    out = tr.redistribute(frame, shares=SHARES)
    assert tr.INHERITED_COLUMN not in out.columns
