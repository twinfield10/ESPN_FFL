"""The per-source name audit, and the name alignment it exists to police.

Two halves, tested differently.

The classifier is tested against a synthetic ESPN universe, so the cases are the
*shapes* that matter -- a suffix, a nickname, a single typo, a team abbreviation
left on the name, a first name on its own, a real player who merely shares a
surname -- and not whatever the boards happen to hold this morning. The shapes do
not move; the boards move daily.

Three tests do read the committed 2026 stores, because the defects this module
found on 2026-09-09 are worth a regression guard: Pinnacle's ``Cameron Ward``,
BetOnline's two backwards renames, and the ``AJ BARNER SEA`` team tail. They are
skipped when the stores are absent rather than failing, so a fresh clone is not
red.

No network.
"""

import pandas as pd
import pytest

from Scripts import name_audit as na
from Scripts import projection_utils as pu
from Scripts import season_projections as sp

SEASON = 2026


def _universe(rows):
    """An :class:`na.EspnUniverse` from ``(name, position, team, points)`` tuples."""
    frame = pd.DataFrame(rows, columns=["player_name", "primaryPosition",
                                        "pro_team", "espn_points"])
    return na.EspnUniverse(frame)


@pytest.fixture
def bare_aliases(monkeypatch):
    """Empty :data:`sp.NAME_ALIASES` for the duration of a classifier test.

    The classifier tests would otherwise pass for the wrong reason. ``Cameron
    Ward`` and ``STEFIN DIGGS`` are now *in* the alias map, so ``normalise_name``
    canonicalises them before the resolver sees them and every clause below reports
    a clean join. What is under test here is the rule that finds an alias nobody
    has written down yet, so the map has to be out of the way.
    """
    monkeypatch.setattr(sp, "NAME_ALIASES", {})


@pytest.fixture
def espn(bare_aliases):
    """A universe holding one of each shape the resolver has a clause for."""
    return _universe([
        ("James Cook III", "RB", "BUF", 300.0),      # suffix ESPN adds
        ("Cam Ward", "QB", "TEN", 302.0),            # short form
        ("Stefon Diggs", "WR", "NE", 175.0),         # one-character typo target
        ("Rueben Bain Jr.", "DE", "MIA", 85.0),      # transposed vowels
        ("AJ Barner", "TE", "SEA", 116.0),           # team tail target
        ("Dak Prescott", "QB", "DAL", 395.0),        # mangled first name
        ("Akheem Mesidor", "DE", "LAC", 69.0),       # first-name-only target
        ("Christian McCaffrey", "RB", "SF", 250.0),  # ambiguous first name,
        ("Christian Kirk", "WR", "SF", 130.0),       # and ambiguous on his team too
        ("Christian Watson", "WR", "GB", 120.0),
        ("T.J. Edwards", "LB", "CHI", 170.0),        # shares a surname, different man
        ("Cam Jones", "LB", "MIN", 20.0),
    ])


# --- The classifier --------------------------------------------------------

def test_a_suffix_difference_is_a_live_miss_on_the_raw_join(espn):
    """`James Cook` never reached `James Cook III` while the join was raw."""
    verdict, key, _ = na.resolve("James Cook", espn, join=na.JOIN_RAW)
    assert verdict == na.ALIAS_NORMALISED
    assert espn.describe(key)[0] == "James Cook III"


def test_the_same_suffix_difference_is_not_a_miss_once_normalised(espn):
    """Which is why the audit has to know which discipline each source uses."""
    verdict, _, _ = na.resolve("James Cook", espn, join=na.JOIN_NORMALISED)
    assert verdict == ""


def test_a_short_first_name_resolves_by_prefix(espn):
    """Pinnacle's `Cameron Ward` against ESPN's `Cam Ward`: 302 points."""
    verdict, key, _ = na.resolve("Cameron Ward", espn, join=na.JOIN_NORMALISED)
    assert verdict == na.ALIAS_NICKNAME
    assert espn.describe(key)[0] == "Cam Ward"


@pytest.mark.parametrize("source_name,expected", [
    ("Stefin Diggs", "Stefon Diggs"),       # one substitution
    ("Reuben Bain Jr", "Rueben Bain Jr."),  # one adjacent transposition
])
def test_a_single_typo_resolves(espn, source_name, expected):
    verdict, key, _ = na.resolve(source_name, espn, join=na.JOIN_NORMALISED)
    assert verdict == na.ALIAS_NICKNAME
    assert espn.describe(key)[0] == expected


def test_a_shared_surname_alone_is_not_an_alias(espn):
    """`Cash Jones` is a real back, not Cam Jones the linebacker.

    An earlier cut called this an alias and got eight of nine such rows wrong. It
    must never reach :data:`na.CONFIDENT`.
    """
    verdict, _, _ = na.resolve("Cash Jones", espn, join=na.JOIN_NORMALISED)
    assert verdict not in na.CONFIDENT


def test_josh_and_john_are_not_one_typo_apart():
    """The case a similarity ratio cannot separate from `CHIQ`/`CHIG`, both 0.75."""
    assert na._one_typo_apart("CHIQ", "CHIG")
    assert not na._one_typo_apart("JOSH", "JOHN")


def test_a_mangled_first_name_is_reported_not_applied(espn):
    """`DAL PRESCOTT` names Dak Prescott, and still only as a suggestion."""
    verdict, key, _ = na.resolve("DAL PRESCOTT", espn, join=na.JOIN_NORMALISED)
    assert verdict == na.REVIEW_SURNAME
    assert verdict in na.NEEDS_REVIEW
    assert espn.describe(key)[0] == "Dak Prescott"


def test_a_team_abbreviation_left_on_the_name_is_stripped(espn):
    verdict, key, _ = na.resolve("AJ BARNER SEA", espn, join=na.JOIN_NORMALISED)
    assert verdict == na.ALIAS_TEAM_TAIL
    assert espn.describe(key)[0] == "AJ Barner"


def test_a_team_tail_is_not_stripped_off_a_two_token_name(espn):
    """`DAL PRESCOTT` must not become `DAL`, which no rule could then resolve."""
    assert na._strip_team_tail("DAL PRESCOTT", espn.teams) is None


def test_a_first_name_only_row_resolves_by_team(espn):
    """BetOnline's season scrape loses surnames; the team is the only handle."""
    verdict, key, _ = na.resolve("AKHEEM", espn, join=na.JOIN_NORMALISED,
                                 team="LAC")
    assert verdict == na.TRUNCATED
    assert espn.describe(key)[0] == "Akheem Mesidor"


def test_an_ambiguous_first_name_names_its_candidates_and_stops(espn):
    """BetOnline's `CHRISTIAN` is Christian McCaffrey's receptions line, and the
    team narrows it no further: the 2026 boards put Christian Kirk on San
    Francisco too. So it stays a suggestion, and `NAME_ALIASES` deliberately
    carries no entry for it -- a key of `CHRISTIAN` would match whoever the scrape
    truncated next week."""
    verdict, key, note = na.resolve("CHRISTIAN", espn, join=na.JOIN_NORMALISED,
                                    team="SF")
    assert verdict == na.AMBIGUOUS
    assert key == ""
    assert "Christian McCaffrey" in note


def test_a_name_espn_has_nowhere_is_absent(espn):
    verdict, key, _ = na.resolve("Practice Squad Body", espn,
                                 join=na.JOIN_NORMALISED)
    assert verdict == na.ABSENT
    assert key == ""


def test_a_name_in_the_wider_universe_is_unrostered_not_an_alias():
    """A source with a wider slate than the league is not a defect.

    Checked before the surname rules on purpose: an exact match in the board must
    outrank a same-surname teammate in the weekly frame.
    """
    board = _universe([("Isiah Pacheco", "RB", "KC", 200.0)])
    weekly = na.EspnUniverse(
        pd.DataFrame([["Rashee Rice", "WR", "KC", 180.0]],
                     columns=["player_name", "primaryPosition", "pro_team",
                              "espn_points"]),
        wider=board)
    verdict, _, _ = na.resolve("Isiah Pacheco", weekly, join=na.JOIN_RAW)
    assert verdict == na.UNROSTERED
    assert verdict not in na.ACTIONABLE


def test_a_reviewed_pair_drops_out_of_the_review_bucket(espn):
    """Otherwise the bucket never empties and the report stops being read."""
    out = na.audit_source("TOMCAT", ["Cash Jones"], espn,
                          join=na.JOIN_NORMALISED)
    assert out.loc[0, "verdict"] == na.ABSENT
    assert "reviewed" in out.loc[0, "note"]
    assert "CASH JONES" in na.CONFIRMED_DISTINCT


def test_a_confident_verdict_is_never_silenced_by_review(espn):
    """`CONFIRMED_DISTINCT` only speaks to suggestions. A mechanical rule that
    lands on a reviewed name is a rule to look at, not noise to hide."""
    out = na.audit_source("Pinnacle", ["Cameron Ward"], espn,
                          join=na.JOIN_NORMALISED)
    assert out.loc[0, "verdict"] == na.ALIAS_NICKNAME


def test_an_empty_source_audits_to_an_empty_typed_frame(espn):
    out = na.audit_source("Nothing", [], espn, join=na.JOIN_NORMALISED)
    assert out.empty
    assert "verdict" in out.columns


def test_the_report_separates_what_to_fix_from_what_to_read(espn):
    audit = na.audit_source("BetOnline season", ["James Cook", "DAL PRESCOTT"],
                            espn, join=na.JOIN_RAW)
    audit["grain"] = "season"
    text = na.format_report(audit)
    assert "1 to fix, 1 to review" in text


# --- align_to_espn_names ---------------------------------------------------

@pytest.fixture
def espn_names():
    return ["James Cook III", "Deebo Samuel Sr.", "Oronde Gadsden",
            "Kenny Gainwell", "Byron Murphy II", "Byron Murphy Jr."]


def test_alignment_rewrites_a_source_name_to_espns_spelling(espn_names):
    source = pd.DataFrame({"week": [1, 1], "player_name": ["James Cook",
                                                           "Kenneth Gainwell"],
                           "proj_rushingYards": [80.0, 20.0]})
    out = pu.align_to_espn_names(source, espn_names, "Pinnacle")
    assert list(out["player_name"]) == ["James Cook III", "Kenny Gainwell"]


def test_alignment_fixes_the_direction_the_hand_map_had_backwards(espn_names):
    """`clean_bol` renamed these *away* from ESPN's current spelling.

    `Deebo Samuel Sr. -> Deebo Samuel` and `Oronde Gadsden -> Oronde Gadsden II`
    both manufactured the miss they were written to prevent -- 9.1 and 4.8
    projected points that week. Derived from the ESPN frame, the direction cannot
    be wrong.
    """
    source = pd.DataFrame({"week": [1, 1],
                           "player_name": ["Deebo Samuel", "Oronde Gadsden II"],
                           "proj_receivingYards": [39.0, 20.0]})
    out = pu.align_to_espn_names(source, espn_names, "BetOnline")
    assert list(out["player_name"]) == ["Deebo Samuel Sr.", "Oronde Gadsden"]


def test_alignment_leaves_an_ambiguous_key_alone(espn_names):
    """Suffix stripping collapses five pairs of different 2026 players onto one
    key. Attaching a real line to the wrong player is worse than abstaining."""
    source = pd.DataFrame({"week": [1], "player_name": ["Byron Murphy"],
                           "proj_defensiveSacks": [0.4]})
    out = pu.align_to_espn_names(source, espn_names, "BetOnline")
    assert list(out["player_name"]) == ["Byron Murphy"]


def test_alignment_drops_rows_that_collide_after_being_aligned():
    """FantasyPros' weekly file carries `Mitch Tinsley` *and* `Mitchell Tinsley`.

    Left alone they would each match the one ESPN row and double it, which is the
    hazard that makes this more than a rename.
    """
    source = pd.DataFrame({"week": [1, 1],
                           "player_name": ["Mitchell Tinsley", "Mitch Tinsley"],
                           "proj_receivingYards": [12.0, 9.0]})
    out = pu.align_to_espn_names(source, ["Mitchell Tinsley"], "FantasyPros")
    assert len(out) == 1
    assert out.loc[0, "proj_receivingYards"] == 12.0


def test_alignment_never_moves_a_name_that_already_matched(espn_names):
    source = pd.DataFrame({"week": [1], "player_name": ["Kenny Gainwell"],
                           "proj_rushingYards": [20.0]})
    out = pu.align_to_espn_names(source, espn_names, "Pinnacle")
    assert list(out["player_name"]) == ["Kenny Gainwell"]


def test_alignment_passes_an_absent_source_straight_through():
    """`absent_weekly_source` returns a frame with keys and no rows."""
    absent = pu.absent_weekly_source("Pinnacle", "/nonexistent.parquet")
    out = pu.align_to_espn_names(absent, ["James Cook III"], "Pinnacle")
    assert len(out) == 0


def test_every_weekly_source_is_aligned_before_it_is_merged():
    """The wiring, not the rule.

    Alignment is invisible when it is missing -- the merge still runs, the player
    just abstains and gets the ESPN/FantasyPros mean. So this asserts each of the
    three sources is aligned *before* `clean_lineups` merges it, the same way
    `test_reallocate_book_touchdowns_runs_inside_the_weekly_blend` does.
    """
    from Scripts.paths import REPO_ROOT

    source = (REPO_ROOT / "Scripts" / "projection_utils.py").read_text()
    body = source[source.index("def clean_lineups("):]
    for frame, merge in (("fp_proj", "espn_proj.merge(fp_proj"),
                         ("pinny_proj", "mean_df.merge(pinny_proj"),
                         ("bol_proj", "mean_df.merge(bol_proj")):
        aligned = body.index(f"align_to_espn_names({frame}")
        merged = body.index(merge)
        assert aligned < merged, f"{frame} is merged before it is aligned"


def test_the_hand_rename_maps_are_gone():
    """The regression guard for the whole change.

    Reintroducing a `name_changes` literal in either loader puts the pipeline back
    on a per-source map that goes stale silently -- which is how two entries came
    to point at spellings ESPN had stopped using.
    """
    assert na._rename_maps() == {}


# --- Against the committed stores -----------------------------------------

@pytest.fixture(scope="module")
def universes():
    board, weekly = na.espn_universes(SEASON)
    if not board.by_key:
        pytest.skip(f"no {SEASON} stores built")
    return board, weekly


def test_espn_spells_the_titans_quarterback_cam_ward(universes):
    """The alias's premise. If ESPN renames him, `CAMERON WARD` should be revisited."""
    board, _ = universes
    assert "CAM WARD" in board.by_key


def test_pinnacles_cameron_ward_now_joins(universes):
    """He did not, from the day the season file landed until 2026-09-09."""
    board, _ = universes
    assert sp.normalise_name("Cameron Ward") in board.by_key


def test_the_betonline_team_tail_is_stripped_before_the_join():
    """`AJ BARNER SEA` reached the blend with the tail on and lost 116 points of
    tight end. `_recover_player` strips it, but only on the branch this row does
    not take."""
    raw = pd.DataFrame([{"team": "SEATTLE SEAHAWKS", "player": "AJ BARNER SEA",
                         "stat_short": "YDS_REC", "stat_type": "Receiving Yards",
                         "line": 425.5, "True_Line": 425.5}])
    out = sp.normalise_bol_props(raw)
    assert out.loc[0, "name_key"] == "AJ BARNER"


def test_every_alias_target_is_a_name_espn_actually_uses(universes):
    """A canonicalisation onto a third spelling nobody uses is silently inert."""
    board, _ = universes
    unknown = [f"{k} -> {v}" for k, v in sp.NAME_ALIASES.items()
               if v not in board.by_key]
    assert not unknown, f"alias targets absent from every board: {unknown}"


def test_no_source_has_a_miss_a_rename_would_close(universes):
    """The audit's own bar, run against what ships.

    `CONFIDENT` only. BetOnline's season scrape still truncates first names, and no
    alias fixes a fragment -- those are `KNOWN_UPSTREAM`, reported with their cost
    and excluded here for the reason that constant names: a check that cannot go
    green is a check nobody keeps.
    """
    audit = na.audit_all(SEASON)
    outstanding = audit[audit["verdict"].isin(na.CONFIDENT)]
    assert outstanding.empty, outstanding[["source", "source_name", "espn_name",
                                           "verdict"]].to_string(index=False)


def test_a_known_upstream_defect_is_reported_but_does_not_fail_the_check(universes):
    """Visible, attributed, and not counted against the exit code."""
    audit = na.audit_all(SEASON)
    upstream = audit[audit["verdict"] == na.UPSTREAM]
    assert len(upstream) == len(na.KNOWN_UPSTREAM)
    assert all("GetSeasonProps" in n for n in upstream["note"])
    assert na.UPSTREAM not in na.CONFIDENT
    assert na.UPSTREAM in na.ACTIONABLE
