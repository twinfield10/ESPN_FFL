"""Injury episodes: what counts as an absence, when it ended, and what came after.

The episode table is the training data for every fitted number in
``Scripts/injury/``, so its edge cases are the ones that would silently bias a curve
rather than break a build. Four of them have teeth:

- a **bye** inside an absence must bridge it and must not count as a game missed;
- **censoring** comes in three kinds and the season running out is not the same event as
  a player leaving the league;
- a run of quiet weeks with **no corroboration** is a benching, not an injury;
- the **control cohort** must be selected by exactly the filter the injured cohort is,
  or the ratio between them means nothing.

Synthetic frames throughout. No parquet, no network.
"""

import polars as pl
import pytest

from Scripts.injury import episodes as ep
from Scripts.injury import lexicon


# --- synthetic frames -----------------------------------------------------

def grid(rows, season=2024, team="ARI", position="WR", player="P1"):
    """Build an absence grid from a compact spec.

    Args:
        rows: List of ``(week, appeared, strong_kind, points)`` where ``strong_kind`` is
            ``None``, ``"out"``, ``"reserve"`` or ``"designated"``.
    """
    built = []
    for slot, (week, appeared, kind, points) in enumerate(rows):
        built.append({
            "gsis_id": player, "season": season, "week": week, "team": team,
            "position": position, "full_name": player, "slot": slot,
            "appeared": appeared,
            "fantasy_points_ppr": points,
            "offense_pct": 0.8 if appeared else None,
            "status": "RES" if kind in ("reserve", "designated") else "ACT",
            "code": "R01" if kind == "reserve" else (
                "R48" if kind == "designated" else "A01"),
            "report_primary_injury": "Ankle" if kind == "out" else None,
            "report_status": "Out" if kind == "out" else None,
            "sig_out": kind == "out",
            "sig_reserve": kind in ("reserve", "designated"),
            "sig_designated": kind == "designated",
            "strong": kind in ("out", "reserve", "designated"),
        })
    return pl.DataFrame(built, schema_overrides={"season": pl.Int32,
                                                 "week": pl.Int32,
                                                 "slot": pl.Int64})


def gamedays(weeks, season=2024, team="ARI"):
    return pl.DataFrame({"season": [season] * len(weeks),
                         "team": [team] * len(weeks),
                         "week": list(weeks)},
                        schema_overrides={"season": pl.Int32, "week": pl.Int32})


def healthy(week, points=12.0):
    return (week, True, None, points)


def out(week):
    return (week, False, "out", 0.0)


def reserve(week):
    return (week, False, "reserve", 0.0)


def bench(week):
    """Absent, with nothing vouching for it."""
    return (week, False, None, 0.0)


# --- reserve codes --------------------------------------------------------

def evidence_row(status, code, n=200, absent=1.0, seasons=(2022, 2023, 2024),
                 on_report=0.04, report_out=0.9):
    return {"status": status, "code": code, "n": n, "absent_rate": absent,
            "on_report": on_report, "report_out": report_out,
            "seasons": list(seasons)}


def admit(**kwargs):
    frame = pl.DataFrame([evidence_row(**kwargs)],
                         schema_overrides={"seasons": pl.List(pl.Int32)})
    return ep.admit_reserve_codes(frame).row(0, named=True)


def test_injured_reserve_is_admitted():
    assert admit(status="RES", code="R01")["admitted"]


def test_a_cut_player_is_not_injured_however_absent_he_is():
    """``CUT`` and ``RET`` are 100% absent too. Absence alone cannot be the rule."""
    verdict = admit(status="CUT", code="(none)")
    assert not verdict["admitted"]
    assert "not a reserve status" in verdict["verdict"]


def test_a_code_confined_to_the_covid_seasons_is_rejected():
    """``R59`` (2020-2021) and ``R62`` (2020) are pandemic reserve. They are 100% absent
    and otherwise indistinguishable from injured reserve, and left in they would have
    contributed hundreds of fabricated episodes to two seasons."""
    verdict = admit(status="RES", code="R59", n=313, seasons=(2020, 2021))
    assert not verdict["admitted"]
    assert "COVID-19" in verdict["verdict"]


def test_a_reserve_code_spanning_normal_seasons_survives_the_covid_rule():
    assert admit(status="RES", code="R04", seasons=(2020, 2021, 2024))["admitted"]


def test_a_code_that_is_usually_present_is_not_an_absence():
    verdict = admit(status="RES", code="A01", absent=0.97)
    assert not verdict["admitted"]
    assert "absent only" in verdict["verdict"]


def test_a_tiny_code_is_reported_as_tiny_rather_than_as_covid():
    """Rule order matters because the verdict string is evidence. A five-row code that
    happens to fall in 2021 is too small to judge, not pandemic bookkeeping."""
    verdict = admit(status="RES", code="R33", n=5, seasons=(2021,))
    assert not verdict["admitted"]
    assert "only 5 player-weeks" in verdict["verdict"]


def test_the_unlabelled_reserve_code_is_admitted_on_its_status_alone():
    """Before 2020 ``status_description_abbr`` is null, so the bare ``RES`` is the only
    signal those seasons have. Holding it to the row minimum would discard 2016-2019."""
    assert admit(status="RES", code="(none)", n=10)["admitted"]


def test_designated_for_return_is_flagged_apart_from_injured_reserve():
    """Same absence, different situation: ``R48`` carries an injury-report row 32% of the
    time against ``R01``'s 3.8%, because the player is practising again."""
    assert admit(status="RES", code="R48")["designated_return"]
    assert not admit(status="RES", code="R01")["designated_return"]


# --- episode assembly ----------------------------------------------------

def build(rows, weeks=None, season=2024):
    g = grid(rows, season=season)
    played = weeks if weeks is not None else [r[0] for r in rows]
    return ep.build_episodes(g, gamedays(played, season=season))


def test_consecutive_out_weeks_are_one_episode():
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 out(5), out(6), healthy(7)])
    assert eps.height == 1
    assert eps["weeks_out"][0] == 2
    assert eps["outcome"][0] == "returned"


def test_a_bye_inside_an_absence_neither_ends_it_nor_counts_as_missed():
    """The grid holds only the weeks the team played, so week 6 -- the bye -- is simply
    not a row. Two games were missed across a three-week calendar gap."""
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 out(5), out(7), healthy(8)],
                weeks=[1, 2, 3, 4, 5, 7, 8])
    assert eps.height == 1
    assert eps["weeks_out"][0] == 2
    assert eps["first_out_week"][0] == 5 and eps["last_out_week"][0] == 7


def test_the_injury_report_and_reserve_are_one_episode_not_two():
    """The report goes quiet once a player lands on reserve. Treating that silence as the
    end of the injury is what truncates long absences -- the report alone finds 99
    returned absences of four games or more, and the union finds 580."""
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 out(5), out(6), reserve(7), reserve(8), reserve(9),
                 healthy(10)])
    assert eps.height == 1
    assert eps["weeks_out"][0] == 5
    assert eps["report_out_weeks"][0] == 2 and eps["reserve_weeks"][0] == 3


def test_the_body_part_carries_forward_across_reserve_weeks():
    """Reserve weeks carry no report row, so the body part is whatever the report said
    while it was still talking. Without this every long absence is an unknown injury."""
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 out(5), reserve(6), reserve(7), healthy(8)])
    assert eps["body_part"][0] == "ankle"
    assert not eps["body_part_unknown"][0]


def test_a_reserve_only_absence_has_an_unknown_body_part_not_a_null_one():
    """232 real episodes averaging 6.7 weeks out go straight to reserve and never appear
    on the report. They are injuries of unknown kind, and they must not silently become
    a null that a later group-by drops."""
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 reserve(5), reserve(6), healthy(7)])
    assert eps["body_part"][0] == "other"
    assert eps["body_part_unknown"][0]


def test_quiet_weeks_with_nothing_vouching_for_them_are_not_an_episode():
    """A healthy scratch and a buried backup look exactly like an injury in the box
    score. Absence alone cannot open an episode."""
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 bench(5), bench(6), healthy(7)])
    assert eps.is_empty()


def test_quiet_weeks_beside_a_vouched_one_are_part_of_the_episode():
    """A player hurt in week 5's game misses week 6 before the report catches up."""
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 bench(5), out(6), bench(7), healthy(8)])
    assert eps.height == 1
    assert eps["weeks_out"][0] == 3
    assert eps["strong_weeks"][0] == 1


def test_thin_corroboration_is_recorded_rather_than_acted_on():
    """One early ``Out`` vouching for eight quiet weeks may be an injury that moved to
    reserve or one bad week and a benching. The honest move is to say which runs are
    thinly evidenced and let the fit decide."""
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 out(5)] + [bench(w) for w in range(6, 13)] + [healthy(13)])
    assert eps.height == 1
    assert eps["strong_share"][0] == pytest.approx(1 / 8)


def test_two_separate_injuries_in_a_season_are_two_episodes():
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 out(5), healthy(6), healthy(7), healthy(8), healthy(9),
                 out(10), healthy(11)])
    assert eps.height == 2


# --- censoring -----------------------------------------------------------

def test_an_absence_running_to_the_end_of_the_schedule_is_season_end():
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 out(5), reserve(6), reserve(7)],
                weeks=[1, 2, 3, 4, 5, 6, 7])
    assert eps["outcome"][0] == "season_end"
    assert eps["return_week"][0] is None


def test_a_player_who_leaves_the_roster_mid_absence_is_off_roster_not_season_end():
    """The distinction the whole censoring scheme exists for. A season that ran out is a
    lower bound on duration; a player who left the league is not an observation of
    duration at all, and pooling the two is how you conclude that knees end careers."""
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4), out(5), out(6)],
                weeks=list(range(1, 18)))
    assert eps["outcome"][0] == "off_roster"


def test_only_returned_episodes_carry_a_return_week():
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4),
                 out(5), healthy(6)])
    assert eps["outcome"][0] == "returned"
    assert eps["return_week"][0] == 6


def test_weeks_out_is_a_signed_thirty_two_bit_count():
    """``context.py`` records the exact bug: a ``len()``-derived UInt32 wrapped to
    4,294,967,295 on a subtraction. Nothing downstream should be able to repeat it."""
    eps = build([healthy(1), healthy(2), healthy(3), healthy(4), out(5), healthy(6)])
    assert eps.schema["weeks_out"] == pl.Int32
    assert eps["weeks_out"].min() >= 1


# --- the recovery clock --------------------------------------------------

def post(rows, weeks=None, season=2024):
    g = grid(rows, season=season)
    played = weeks if weeks is not None else [r[0] for r in rows]
    eps = ep.build_episodes(g, gamedays(played, season=season))
    return ep.post_return(g, eps)


def test_the_clock_counts_appearances_not_calendar_weeks():
    """A bye after the return must shorten the history, not leave a hole in it. Indexed
    on weeks, a bye would read as a week of recovery that never happened."""
    frame = post([healthy(1, 20), healthy(2, 20), healthy(3, 20), healthy(4, 20),
                  out(5), healthy(7, 10), healthy(9, 20)],
                 weeks=[1, 2, 3, 4, 5, 7, 9])
    assert frame["appearance_back"].to_list() == [1, 2]
    assert frame["week"].to_list() == [7, 9]


def test_the_ratio_is_against_the_players_own_recent_form():
    frame = post([healthy(1, 20), healthy(2, 20), healthy(3, 20), healthy(4, 20),
                  out(5), healthy(6, 10)])
    assert frame["pts_ratio"][0] == pytest.approx(0.5)


def test_only_the_last_few_appearances_form_the_baseline():
    """A window of four, so a hot September does not set the bar for a November return."""
    frame = post([healthy(1, 100), healthy(2, 20), healthy(3, 20), healthy(4, 20),
                  healthy(5, 20), out(6), healthy(7, 20)])
    assert frame["pts_ratio"][0] == pytest.approx(1.0)


def test_a_baseline_below_the_materiality_floor_yields_no_rows():
    """Without a floor a baseline of 0.02 points produces a ratio in the trillions, and a
    handful of deep-bench weeks dominate every mean in the table -- measured at 1.5e13
    before the floor went in."""
    frame = post([healthy(1, 0.5), healthy(2, 0.5), healthy(3, 0.5), healthy(4, 0.5),
                  out(5), healthy(6, 30)])
    assert frame.is_empty()


def test_too_few_baseline_appearances_yields_no_rows():
    frame = post([healthy(1, 20), healthy(2, 20), out(3), healthy(4, 20)])
    assert frame.is_empty()


def test_the_window_stops_after_six_appearances():
    frame = post([healthy(w, 20) for w in range(1, 5)] + [out(5)]
                 + [healthy(w, 20) for w in range(6, 16)])
    assert frame["appearance_back"].max() == ep.POST_RETURN_WINDOW


@pytest.mark.parametrize("weeks_out,bucket", [(1, "1"), (2, "2"), (3, "3-4"),
                                              (4, "3-4"), (5, "5+"), (12, "5+")])
def test_duration_buckets_split_where_the_effect_does(weeks_out, bucket):
    """0.95, 0.75 and 0.66 at appearances one, two and three-or-more weeks missed. These
    are the cut points the data put there, not round numbers."""
    frame = pl.DataFrame({"weeks_out": [weeks_out]}).with_columns(
        ep.duration_bucket(pl.col("weeks_out")).alias("bucket"))
    assert frame["bucket"][0] == bucket


# --- recurrence ----------------------------------------------------------

def test_the_same_body_part_going_again_is_a_recurrence():
    g = grid([healthy(1, 20), healthy(2, 20), healthy(3, 20), healthy(4, 20),
              out(5), healthy(6, 20), healthy(7, 20), out(8), healthy(9, 20)])
    eps = ep.recurrence(ep.build_episodes(g, gamedays(range(1, 10))))
    first = eps.sort("first_out_week").row(0, named=True)
    assert first["recurred"]
    assert first["weeks_to_recurrence"] == 2


def test_a_different_body_part_is_a_second_injury_not_a_recurrence():
    g = grid([healthy(1, 20), healthy(2, 20), healthy(3, 20), healthy(4, 20),
              out(5), healthy(6, 20), reserve(7), healthy(8, 20)])
    eps = ep.recurrence(ep.build_episodes(g, gamedays(range(1, 9))))
    first = eps.sort("first_out_week").row(0, named=True)
    assert first["body_part"] == "ankle"
    assert not first["recurred"]


def test_an_unknown_body_part_cannot_recur():
    """Two reserve stints both land in ``other``, which is a bucket rather than a
    diagnosis. Calling that a recurrence would invent a rate out of ignorance."""
    g = grid([healthy(1, 20), healthy(2, 20), healthy(3, 20), healthy(4, 20),
              reserve(5), healthy(6, 20), reserve(7), healthy(8, 20)])
    eps = ep.recurrence(ep.build_episodes(g, gamedays(range(1, 9))))
    assert not eps.sort("first_out_week")["recurred"][0]


def test_a_relapse_beyond_the_window_is_not_counted():
    g = grid([healthy(1, 20), healthy(2, 20), healthy(3, 20), healthy(4, 20),
              out(5)] + [healthy(w, 20) for w in range(6, 14)] + [out(14),
                                                                  healthy(15, 20)])
    eps = ep.recurrence(ep.build_episodes(g, gamedays(range(1, 16))))
    assert not eps.sort("first_out_week")["recurred"][0]


# --- the control cohort --------------------------------------------------

def test_the_control_excludes_anyone_carrying_a_designation():
    """"Not in an episode" is not the same as healthy. A player listed Questionable all
    month is playing hurt, and putting him in the control drags the placebo curve down --
    which would then be subtracted from the injured curve as if it were skew."""
    rows = [healthy(w, 20) for w in range(1, 12)]
    g = grid(rows)
    g = g.with_columns(
        pl.when(pl.col("week") == 6).then(pl.lit("Questionable"))
        .otherwise(None).alias("report_status"))
    control = ep.control_cohort(g, ep.build_episodes(g, gamedays(range(1, 12))))
    assert control.is_empty()


def test_a_clean_player_lands_in_the_control():
    rows = [healthy(w, 20) for w in range(1, 12)]
    g = grid(rows)
    control = ep.control_cohort(g, ep.build_episodes(g, gamedays(range(1, 12))))
    assert not control.is_empty()
    assert control["pts_ratio"].max() == pytest.approx(1.0)


def test_the_control_keeps_clear_of_an_episodes_aftermath():
    """The window after a return is the thing being measured. A control drawn from it
    would be comparing injured players with injured players."""
    rows = ([healthy(w, 20) for w in range(1, 5)] + [out(5)]
            + [healthy(w, 20) for w in range(6, 14)])
    g = grid(rows)
    control = ep.control_cohort(g, ep.build_episodes(g, gamedays(range(1, 14))))
    assert control.is_empty()


def test_both_cohorts_pass_through_one_baseline_filter():
    """The injured curve only means anything divided by the control curve, and that
    division is only valid if both sides were selected identically. One expression, two
    call sites, so it cannot drift."""
    import inspect

    source = inspect.getsource(ep)
    assert source.count("_usable_baseline()") == 3  # definition plus two callers


# --- the lexicon ---------------------------------------------------------

def test_laterality_carries_no_diagnostic_content():
    assert lexicon.group("right Shoulder") == "shoulder"
    assert lexicon.group("left Shoulder") == "shoulder"


def test_singular_and_plural_land_together():
    assert lexicon.group("Rib") == lexicon.group("Ribs") == "ribs_chest"
    assert lexicon.group("Hamstring") == lexicon.group("hamstrings") == "hamstring"


def test_a_multi_part_report_takes_the_first_listed():
    """``"toe, pec, knee, hip"`` appears upstream. The report's own ordering is the
    closest thing to a primary diagnosis available."""
    assert lexicon.group("toe, pec, knee, hip") == "foot_toe"
    assert lexicon.group("foot/wrist/hip") == "foot_toe"


def test_roster_bookkeeping_is_not_an_injury():
    """A rested starter and a suspended one are both absent. Counting either would put a
    healthy player into the fit with a baseline drawn from the weeks he was fine."""
    for value in ("Not Injury Related", "Not injury related - resting player",
                  "Suspension", "coaching decision"):
        assert not lexicon.is_injury(value)


def test_an_absence_with_no_stated_reason_is_not_evidence_of_injury():
    assert not lexicon.is_injury(None)


def test_an_unrecognised_body_part_is_filed_rather_than_raised():
    """The upstream vocabulary grows without notice -- ``"Core Muscle"`` first appears in
    2023. A build that crashed over one new noun would be worse than one that files it
    and reports it."""
    assert lexicon.group("Spleen") == "other"
    assert lexicon.unmapped(["Spleen", "Spleen", "Ankle"]) == {"spleen": 2}


def test_every_mapped_group_is_a_declared_group():
    assert set(lexicon.as_dict().values()) <= set(lexicon.GROUPS)


def test_illness_and_unknown_are_kept_out_of_the_recovery_fit():
    """An illness costs availability and nothing else -- there is no tissue healing on a
    timetable, so a post-return efficiency ramp has no mechanism behind it."""
    assert "illness" in lexicon.RECOVERY_EXCLUDED_GROUPS
    assert "other" in lexicon.RECOVERY_EXCLUDED_GROUPS
