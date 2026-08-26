"""Resolving what is wrong with a player from whatever the feeds happen to carry.

The evidence here is genuinely thin and the module is built around admitting it. Measured
on the 2026-08-18 ESPN pull: 800 records, 114 with a structured ``injury_type``, and
exactly **one** comment anywhere containing "high ankle". So most of these tests are about
the two hard parts -- getting the *precedence* right when two channels disagree, and
abstaining cleanly when none of them knows anything.

Jeremiyah Love is pinned by name, because he is the case the module was written against
and the one that shows why the top rung has to be a human.

Synthetic records and a temporary override file. No parquet, no network.
"""

import datetime

import pytest
import yaml

from Scripts.injury import severity as sv

WEEK_ONE = datetime.date(2026, 9, 10)
TODAY = datetime.date(2026, 8, 18)

#: Love exactly as ESPN serves him: Active, no date, no type, severity only in prose.
LOVE = {
    "full_name": "Jeremiyah Love", "name_key": "JEREMIYAH LOVE", "espn_id": "4870808",
    "status": "Active", "return_date": None, "injury_type": None,
    "injury_detail": None,
    "comment": ('The Cardinals are "hopeful" that Love (ankle) will be ready for '
                'Week 1, Jeremy Fowler of ESPN.com reports.'),
}


def write_overrides(tmp_path, monkeypatch, entries, season=2026):
    monkeypatch.setattr(sv.paths, "INJURY_OVERRIDES_DIR", tmp_path)
    path = sv.overrides_path(season, create=True)
    with open(path, "w") as handle:
        yaml.safe_dump({"players": entries}, handle)
    return path


def entry(**kwargs):
    base = {"espn_id": 4870808, "name_key": "JEREMIYAH LOVE",
            "body_part": "ankle_high", "weeks_out": [4, 6],
            "as_of": datetime.date(2026, 8, 18), "source": "beat report"}
    base.update(kwargs)
    return base


# --- precedence ----------------------------------------------------------

def test_an_override_outranks_everything_else(tmp_path, monkeypatch):
    write_overrides(tmp_path, monkeypatch, [entry()])
    loaded = sv.load_overrides(2026)
    record = dict(LOVE, injury_type="Knee - ACL",
                  return_date=datetime.date(2026, 10, 11))

    found = sv.resolve(record, overrides=loaded, week_one=WEEK_ONE)
    assert found.source == "override"
    assert (found.weeks_low, found.weeks_high) == (4.0, 6.0)


def test_a_return_date_outranks_a_real_diagnosis_and_malik_nabers_is_why():
    """**A diagnosis names an injury; a return date times it.**

    This precedence was the other way round until real data broke it. ESPN had Malik
    Nabers at ``injury_type="Knee - ACL"``, ``injury_detail="Surgery"``, status
    ``Questionable`` -- and a ``returnDate`` four weeks *before* the opener, with a comment
    describing 11-on-11 reps in a non-contact jersey. The ACL is real and it is last
    season's. Diagnosis-first put him 46 weeks out at ADP 36; date-first has him available
    in week 1 with an ACL in his history, which is what is true.

    The label survives the reordering, which is the point: the body part still comes from
    the diagnosis."""
    found = sv.resolve({"injury_type": "Knee - ACL", "injury_detail": "Surgery",
                        "return_date": datetime.date(2026, 8, 15)},
                       week_one=WEEK_ONE)
    assert found.source == "return_date"
    assert found.body_part == "knee"
    assert found.detail == "acl"
    assert found.weeks_expected == 0.0
    assert not found.season_ending


def test_a_diagnosis_still_sets_the_duration_when_there_is_no_date():
    found = sv.resolve({"injury_type": "Knee - ACL", "injury_detail": "Surgery"},
                       week_one=WEEK_ONE)
    assert found.source == "espn_structured"
    assert found.season_ending


def test_a_bare_body_part_does_not_outrank_a_return_date():
    """72 of the 114 structured records are a joint with ``Not Specified`` detail, which
    carries no duration information at all. ESPN naming the joint and ESPN estimating when
    he is back are two different facts, and only one of them has a number in it."""
    found = sv.resolve({"injury_type": "Ankle", "injury_detail": "Not Specified",
                        "return_date": datetime.date(2026, 10, 11)},
                       week_one=WEEK_ONE)
    assert found.source == "return_date"
    assert found.body_part == "ankle"          # the label still survives the fall-through


def test_a_return_date_outranks_the_news_text():
    found = sv.resolve({"return_date": datetime.date(2026, 10, 11),
                        "comment": "Brown (ankle) is week-to-week."},
                       week_one=WEEK_ONE)
    assert found.source == "return_date"


def test_the_news_text_outranks_the_weekly_report():
    found = sv.resolve({"comment": "Brown suffered a high ankle sprain Sunday."},
                       report_body_part="Ankle")
    assert found.source == "comment"
    assert found.detail is not None


def test_the_weekly_report_is_the_last_rung_before_abstaining():
    found = sv.resolve({}, report_body_part="Hamstring")
    assert found.source == "report"
    assert found.body_part == "hamstring"
    assert found.confidence == "low"


# --- abstention ----------------------------------------------------------

def test_nothing_known_abstains_rather_than_guessing():
    found = sv.resolve({"status": "Active", "comment": "Jones was limited in practice."})
    assert found.abstained
    assert found.source == "none"


def test_an_abstention_still_carries_a_reason():
    """The flag *is* the evidence string. A multiplier cannot be null without poisoning
    the arithmetic, so "looked and found nothing" has to be distinguishable from "could
    not look" some other way."""
    found = sv.resolve({})
    assert found.evidence.startswith("abstain:")
    assert found.evidence != ""


def test_an_undisclosed_injury_type_is_not_severity():
    """``"Undisclosed"`` is the second most common value in the field."""
    found = sv.resolve({"injury_type": "Undisclosed",
                        "injury_detail": "Not Specified"})
    assert found.abstained


def test_a_suspension_is_not_an_injury():
    found = sv.resolve({"injury_type": "Suspension"})
    assert found.abstained


def test_an_unparseable_comment_abstains():
    found = sv.resolve({"comment": "Smith signed a contract extension on Tuesday."})
    assert found.abstained


# --- the Love case -------------------------------------------------------

def test_love_resolves_from_the_news_text_alone():
    """Every structured channel misses him. The parenthetical is all there is."""
    found = sv.resolve(LOVE, week_one=WEEK_ONE)
    assert found.source == "comment"
    assert found.body_part == "ankle"
    assert found.weeks_high > found.weeks_low        # a range, not false precision


def test_love_without_an_override_reads_as_a_camp_tweak():
    """The gap the override file exists to fill, and it is wider than it first looks.

    The comment says only "(ankle)" -- no severity -- and ESPN still lists him Active, so
    the honest automatic reading is a knock that costs about half a game. The override says
    four to six weeks. Those are different duration buckets and therefore different cells
    of the fitted curve, and no automatic channel can close the distance: the severity is
    in a beat reporter's sentence that nobody has written into a field."""
    found = sv.resolve(LOVE, week_one=WEEK_ONE)
    assert found.source == "comment"
    assert found.detail is None
    assert found.weeks_expected < 1.0
    assert found.duration_bucket == "1"


def test_love_with_an_override_reaches_the_right_bucket(tmp_path, monkeypatch):
    write_overrides(tmp_path, monkeypatch, [entry()])
    found = sv.resolve(LOVE, overrides=sv.load_overrides(2026), week_one=WEEK_ONE)
    assert found.source == "override"
    assert found.duration_bucket == "5+"
    assert found.confidence == "high"


def test_a_parenthesised_body_part_wins_over_one_mentioned_in_passing():
    """Beat reports put the injury in parentheses after the name. A stray mention of a
    teammate's knee later in the sentence must not win."""
    found = sv.resolve({"comment": "Love (ankle) practised while Benson nursed a knee."})
    assert found.body_part == "ankle"


def test_a_body_part_is_matched_on_a_word_boundary():
    """So "hand" does not fire on "handoff"."""
    found = sv.resolve({"comment": "He took a handoff on the opening drive."})
    assert found.abstained


# --- severity words ------------------------------------------------------

@pytest.mark.parametrize("text,group", [
    ("suffered a high ankle sprain", "ankle"),
    ("tore his ACL", "knee"),
    ("is dealing with an MCL sprain", "knee"),
    ("has a Lisfranc injury", "foot_toe"),
    ("in the concussion protocol", "concussion"),
])
def test_specific_injuries_are_read_from_the_text(text, group):
    found = sv.resolve({"comment": f"Smith {text}."})
    assert found.source == "comment"
    assert found.body_part == group


def test_a_high_ankle_sprain_is_longer_than_a_plain_ankle():
    """The distinction the whole exercise started from. Order is load-bearing in the
    phrase list -- "high ankle" has to be tested before "ankle"."""
    high = sv.resolve({"comment": "Smith has a high ankle sprain."})
    plain = sv.resolve({"comment": "Smith has an ankle injury."})
    assert high.weeks_expected > plain.weeks_expected


def test_season_ending_language_is_read_as_season_ending():
    found = sv.resolve({"comment": "Smith (knee) is out for the season."})
    assert found.season_ending


def test_minor_language_shortens_the_prior():
    sore = sv.resolve({"comment": "Smith (hamstring) is dealing with soreness."})
    plain = sv.resolve({"comment": "Smith (hamstring) did not practise."})
    assert sore.weeks_expected < plain.weeks_expected


def test_the_injured_reserve_sentinel_is_not_a_return_estimate():
    """ESPN stamps reserve with a date past the schedule. Read literally it means
    "returns in week 23", which a games-available calculation turns into a negative
    slate."""
    found = sv.resolve({"return_date": datetime.date(2027, 2, 15),
                        "injury_type": "Knee"}, week_one=WEEK_ONE)
    assert found.season_ending
    assert "past the schedule" in found.evidence


def test_a_return_date_with_no_week_one_is_skipped_rather_than_guessed():
    """Weeks missed is measured from the opener, and the opener moves several days a
    year. Without it there is no arithmetic to do."""
    found = sv.resolve({"return_date": datetime.date(2026, 10, 11)})
    assert found.source != "return_date"


# --- duration buckets ----------------------------------------------------

@pytest.mark.parametrize("weeks,bucket", [(0.5, "1"), (1, "1"), (2, "2"), (3, "3-4"),
                                          (4, "3-4"), (5, "5+"), (40, "5+")])
def test_a_resolved_absence_lands_in_the_right_bucket(weeks, bucket):
    override = {"4870808": {"_group": "ankle", "_part": "ankle", "_low": weeks,
                            "_high": weeks, "_where": "test", "source": "t",
                            "note": "t", "espn_id": 4870808}}
    found = sv.resolve(LOVE, overrides=override)
    assert found.source == "override"
    assert found.duration_bucket == bucket


def test_the_live_buckets_agree_with_the_historical_ones():
    """The invariant that makes the whole scheme work: a forward-looking lookup has to
    land in the cell the backward-looking fit populated. Two implementations of the cut
    points is how the fit and the application end up disagreeing about which curve a
    four-week ankle gets."""
    import polars as pl

    from Scripts.injury import episodes as ep

    weeks = list(range(0, 25))
    historical = (pl.DataFrame({"weeks_out": weeks})
                  .with_columns(ep.duration_bucket(pl.col("weeks_out")).alias("b"))
                  ["b"].to_list())
    live = [sv._bucket(float(w)) for w in weeks]
    assert live == historical


# --- the override file ---------------------------------------------------

def test_a_missing_override_file_is_not_an_error(tmp_path, monkeypatch):
    monkeypatch.setattr(sv.paths, "INJURY_OVERRIDES_DIR", tmp_path)
    assert sv.load_overrides(2026) == {}


def test_an_entry_is_reachable_by_either_key(tmp_path, monkeypatch):
    write_overrides(tmp_path, monkeypatch, [entry()])
    loaded = sv.load_overrides(2026)
    assert "4870808" in loaded and "JEREMIYAH LOVE" in loaded


def test_an_unknown_body_part_raises_and_lists_the_known_ones(tmp_path, monkeypatch):
    """A typo'd override that silently does nothing is worse than no file at all, because
    the reader believes the correction landed."""
    write_overrides(tmp_path, monkeypatch, [entry(body_part="anke")])
    with pytest.raises(ValueError, match="unknown body_part"):
        sv.load_overrides(2026)


def test_an_unknown_field_raises(tmp_path, monkeypatch):
    write_overrides(tmp_path, monkeypatch, [entry(weeks_ouy=4)])
    with pytest.raises(ValueError, match="unknown field"):
        sv.load_overrides(2026)


@pytest.mark.parametrize("missing", ["as_of", "source", "body_part", "weeks_out"])
def test_every_required_field_is_required(tmp_path, monkeypatch, missing):
    write_overrides(tmp_path, monkeypatch, [entry(**{missing: None})])
    with pytest.raises(ValueError):
        sv.load_overrides(2026)


def test_an_entry_with_no_join_key_raises(tmp_path, monkeypatch):
    write_overrides(tmp_path, monkeypatch,
                    [{k: v for k, v in entry().items()
                      if k not in ("espn_id", "name_key")}])
    with pytest.raises(ValueError, match="espn_id"):
        sv.load_overrides(2026)


def test_a_backwards_range_raises(tmp_path, monkeypatch):
    write_overrides(tmp_path, monkeypatch, [entry(weeks_out=[6, 4])])
    with pytest.raises(ValueError, match="exceeds"):
        sv.load_overrides(2026)


def test_a_single_number_is_a_degenerate_range(tmp_path, monkeypatch):
    write_overrides(tmp_path, monkeypatch, [entry(weeks_out=5)])
    found = sv.resolve(LOVE, overrides=sv.load_overrides(2026))
    assert (found.weeks_low, found.weeks_high) == (5.0, 5.0)


def test_an_override_matching_nobody_warns_and_names_him(tmp_path, monkeypatch):
    """The ESPN name join is the most fragile in the repo, and an override keyed on a
    misspelling simply never fires."""
    write_overrides(tmp_path, monkeypatch,
                    [entry(espn_id=None, name_key="JEREMIAH LOVE")])
    loaded = sv.load_overrides(2026)
    with pytest.warns(UserWarning, match="matches no player"):
        messages = sv.check_overrides(loaded, known_keys=["JEREMIYAH LOVE"],
                                      today=TODAY)
    assert any("JEREMIAH LOVE" in m for m in messages)


def test_a_stale_override_warns(tmp_path, monkeypatch):
    """A severity written in August is a description of August."""
    write_overrides(tmp_path, monkeypatch, [entry()])
    loaded = sv.load_overrides(2026)
    with pytest.warns(UserWarning, match="days old"):
        sv.check_overrides(loaded, known_keys=["4870808"],
                           today=TODAY + datetime.timedelta(days=60))


def test_a_fresh_override_does_not_warn(tmp_path, monkeypatch):
    write_overrides(tmp_path, monkeypatch, [entry()])
    assert sv.check_overrides(sv.load_overrides(2026), known_keys=["4870808"],
                              today=TODAY) == []


def test_a_hand_supplied_ladder_is_carried_verbatim(tmp_path, monkeypatch):
    """So a disagreement with the fitted curve becomes a number the backtest can score
    rather than an assertion -- and ``source="override"`` makes it impossible to mistake
    a hand number for a fitted one."""
    ladder = [0.75, 0.75, 0.85, 0.92, 1.0]
    write_overrides(tmp_path, monkeypatch, [entry(multiplier_ladder=ladder)])
    found = sv.resolve(LOVE, overrides=sv.load_overrides(2026))
    assert found.ladder == ladder
    assert found.source == "override"


def test_no_ladder_means_the_fitted_curve_is_used(tmp_path, monkeypatch):
    write_overrides(tmp_path, monkeypatch, [entry()])
    assert sv.resolve(LOVE, overrides=sv.load_overrides(2026)).ladder is None


# --- priors --------------------------------------------------------------

def test_every_group_has_a_prior():
    """A group with no prior would resolve to the ``other`` fallback silently."""
    from Scripts.injury import lexicon

    assert set(lexicon.GROUPS) <= set(sv.GROUP_PRIORS)


def test_the_committed_priors_are_checkable_against_the_data():
    """``GROUP_PRIORS`` is a literal so that resolving never depends on the episode table
    having been built. This is the seam that keeps the literal honest."""
    refreshed = sv.refresh_group_priors()
    assert set(refreshed) >= set(sv.GROUP_PRIORS)
    for low, high in refreshed.values():
        assert 0 < low <= high


# --- whose injury is it --------------------------------------------------
#
# Every test below exists because the extractor got it wrong on live data first.

def test_a_teammates_injury_is_not_attributed_to_this_player():
    """Tyler Allgeier's real blurb, which tagged **him** with Jeremiyah Love's high ankle
    sprain -- on the same board where Love carries it correctly from an override. A beat
    report describing one player's injury inside another player's note is routine, and the
    extractor has no idea whose body it is reading about."""
    found = sv.resolve({
        "name_key": "TYLER ALLGEIER", "full_name": "Tyler Allgeier", "status": "Active",
        "comment": ("Allgeier could open the regular season as the Cardinals' primary "
                    "running back, as Adam Schefter of ESPN reports that Jeremiyah Love "
                    "sustained a high-ankle sprain in practice."),
    })
    assert found.abstained


def test_the_parenthetical_convention_anchors_on_the_players_own_surname():
    """"Metcalf (undisclosed)" is about Metcalf even though Mike McCarthy is named first."""
    found = sv.resolve({
        "name_key": "DK METCALF", "full_name": "DK Metcalf", "status": "Active",
        "comment": ('Head coach Mike McCarthy said Saturday that Metcalf (undisclosed) '
                    'will be "hard pressed to work this week" in training camp.'),
    })
    assert found.source == "comment"
    assert found.detail == "undisclosed"


def test_a_diagnosis_close_behind_the_surname_is_his():
    found = sv.resolve({
        "name_key": "ISIAH PACHECO", "full_name": "Isiah Pacheco", "status": "Out",
        "comment": "Pacheco is dealing with a sprained MCL and will miss time.",
    })
    assert found.source == "comment"
    assert found.body_part == "knee"


def test_with_no_name_to_check_against_the_text_is_taken_at_face_value():
    """An unknown subject is not evidence of a misattribution, and there is no better
    option than the text the caller handed over."""
    found = sv.resolve({"comment": "Suffered a high ankle sprain Sunday."})
    assert found.source == "comment"


def test_a_generational_suffix_does_not_break_the_surname_match():
    found = sv.resolve({
        "name_key": "MICHAEL PITTMAN JR", "full_name": "Michael Pittman Jr.",
        "status": "Out", "comment": "Pittman (hamstring) did not practise Wednesday.",
    })
    assert found.body_part == "hamstring"


# --- is it current ------------------------------------------------------

def test_an_active_status_caps_the_duration_but_keeps_the_diagnosis():
    """Pacheco's real blurb: "dealing with a sprained MCL, but head coach Dan Campbell
    believes he will be ready for the season opener". A real diagnosis describing a player
    who will not miss a game. Text is undated; ESPN's status is not."""
    record = {"name_key": "ISIAH PACHECO", "full_name": "Isiah Pacheco",
              "status": "Active",
              "comment": ("Pacheco is dealing with a sprained MCL, but head coach Dan "
                          "Campbell believes he will be ready for the season opener.")}
    found = sv.resolve(record)
    assert found.body_part == "knee" and found.detail == "mcl"
    assert found.weeks_expected < 1.0
    assert "still lists him active" in found.evidence


def test_the_same_diagnosis_on_a_player_listed_out_costs_real_weeks():
    found = sv.resolve({"full_name": "Isiah Pacheco", "status": "Out",
                        "comment": "Pacheco is dealing with a sprained MCL."})
    assert found.weeks_expected > 3.0


def test_season_ending_text_beats_a_stale_active_status():
    """A report saying he is done for the year is newer information than a status field
    that has not caught up."""
    found = sv.resolve({"full_name": "Ricky Pearsall", "status": "Active",
                        "comment": "Pearsall (knee) is out for the season."})
    assert found.season_ending


def test_a_body_part_prior_is_not_applied_to_a_player_who_practised():
    """Puka Nacua at ADP 4.4 read as 3.5 expected weeks missed from a note saying he had
    practised. The group priors are means over episodes that cost at least one game, so
    they answer "given he is out", not "given he is mentioned"."""
    found = sv.resolve({"full_name": "Puka Nacua", "status": "Active",
                        "comment": "Nacua (quadriceps) returned to practice Monday."})
    assert found.weeks_expected <= 1.0


# --- what to call it ----------------------------------------------------

def test_the_detail_is_the_matched_text_not_the_pattern_that_matched():
    """``high[\\s-]*ankle`` reached a board cell before this was fixed."""
    found = sv.resolve({"full_name": "X Smith", "status": "Out",
                        "comment": "Smith suffered a high-ankle sprain."})
    assert found.detail == "high ankle"
    assert "[" not in found.detail and "\\\\" not in found.detail


def test_a_duration_word_is_labelled_with_the_body_part_beside_it():
    """Emeka Egbuka's "(toe) is day-to-day, week-to-week" rendered as "multi week" in a
    column headed Body Part."""
    found = sv.resolve({"full_name": "Emeka Egbuka", "status": "Out",
                        "comment": 'Egbuka (toe) is "day-to-day, week-to-week".'})
    assert found.body_part == "foot_toe"
    assert "toe" in found.detail


def test_the_catch_all_group_keeps_the_word_the_writer_used():
    """A cell reading "other" tells a drafter strictly less than "leg" does."""
    found = sv.resolve({"full_name": "Jaylen Waddle", "status": "Out",
                        "comment": "Waddle (leg) took part in Monday's practice."})
    assert found.body_part == "other"
    assert found.detail == "leg"


# --- whose body is it -----------------------------------------------------
#
# Plan 27 built ATTRIBUTION_WINDOW after a comment about a teammate tagged Tyler
# Allgeier with Jeremiyah Love's high ankle sprain, and noted that all four such
# cases "were invisible in aggregate". Two more were invisible in aggregate until the
# 2026 pre-draft scan: distance from the surname cannot tell that the sentence has
# changed subject, and "back" is a job as often as it is a body part.

def test_a_teammates_parenthetical_is_not_the_subjects_injury():
    """Measured on the 2026 archive. ESPN says "teammate" in so many words and the
    extractor read straight through it, putting Penix's knee on a projected starting
    quarterback."""
    found = sv.resolve({
        "full_name": "Tua Tagovailoa", "status": "Active",
        "comment": "Tagovailoa's teammate, Michael Penix (knee), is in line to return "
                   "to 11-on-11 drills in Monday's practice."})
    assert found.body_part != "knee"


def test_a_parenthetical_hanging_off_another_surname_is_his():
    """The convention that makes "Metcalf (knee)" readable makes "Chuba Hubbard
    (hamstring)" readable too -- and in the second the name is not the subject's,
    however close the bracket happens to sit."""
    found = sv.resolve({
        "full_name": "Jonathon Brooks", "status": "Active",
        "comment": "The Panthers will go with a committee Week 1 with Brooks and "
                   "Chuba Hubbard (hamstring), provided that Hubbard is healthy."})
    assert found.body_part != "hamstring"


def test_the_bare_word_rung_cannot_reopen_a_rejected_parenthetical():
    """The first fix rejected the bracket and the rung below found the same word
    inside it, which put the hamstring straight back on. Both rungs have to agree
    about whose bracket it is."""
    part, _ = sv._body_part_in_text(
        "A committee with Brooks and Chuba Hubbard (hamstring), provided he is healthy.",
        "brooks")
    assert part is None


@pytest.mark.parametrize("comment", [
    "Hill is in line to be the No.2 running back on the Ravens' depth chart.",
    "Skattebo is listed as the Giants' starting running back on the depth chart.",
    "Demercado is the No. 2 running back heading into the season.",
])
def test_a_running_back_is_not_a_back_injury(comment):
    """``back`` maps to ``back_core`` and a beat report says "running back" constantly.
    Four of the twenty-two backs carrying an automatic reading in 2026 got it from
    their own job title, and none of them was hurt."""
    part, _ = sv._body_part_in_text(comment, sv._surname(comment.split()[0]))
    assert part is None


def test_a_real_back_injury_still_reads():
    """The guard is on the position phrase, not on the word."""
    part, _ = sv._body_part_in_text(
        "Barkley tweaked his back in Sunday's win and is considered day-to-day.",
        "barkley")
    assert part == "back"


def test_a_parenthetical_with_no_owner_still_belongs_to_the_subject():
    """"is dealing with (knee) soreness" hangs off no name at all, so it belongs to
    whoever the sentence was already about. Rejecting it would trade two false
    positives for a pile of false negatives."""
    part, _ = sv._body_part_in_text(
        "Player is dealing with (knee) soreness after Sunday's game.", "player")
    assert part == "knee"
