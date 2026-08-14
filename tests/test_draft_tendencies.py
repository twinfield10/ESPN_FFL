"""Owner tendencies: the room-relative measurements and the sentences built on them.

No network and no store. Every draft here is synthesised so that the *right*
answer is known by construction -- a manager who takes a quarterback in round 2
while five others take one in round 8 must come out at six rounds early, and if
the arithmetic ever stops saying so these tests fail rather than the descriptions
quietly going wrong.

The placeholder-draft test is the one that earns its keep. ESPN pre-creates a full
set of picks for a draft that has not happened, and that shape reached the store
once before it was caught.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from Scripts.draft import history as dh  # noqa: E402
from Scripts.draft import tendencies as dt  # noqa: E402


# --- fixtures ------------------------------------------------------------

def _picks(rows):
    """A pick-history frame with the columns the module reads, defaults filled."""
    defaults = {
        "season": 2024, "draft_type": "SNAKE", "n_teams": 6, "rounds": 10,
        "n_picks": 60, "overall_pick": 1, "round": 1, "round_pick": 1,
        "pick_pct": 0.1, "team_id": 1, "team_name": "Team", "owner_id": "id",
        "owner": "Owner", "player_id": 1, "player_name": "Player",
        "position": "RB", "pro_team": "DET", "bid": 0.0, "keeper": False,
        "auto_drafted": False, "is_rookie": False,
    }
    return pl.DataFrame([{**defaults, **row} for row in rows],
                        schema_overrides=dh.PICK_SCHEMA)


def _league(seasons, owners, *, early=None, position="QB", early_round=2,
            normal_round=8, rounds=10):
    """A synthetic league where one manager takes ``position`` early every year.

    Args:
        seasons: Season years to build.
        owners: Manager names.
        early: The manager who takes it early, or None for a uniform room.
        position: The position whose timing differs.
        early_round: The round the early manager takes it in.
        normal_round: The round everyone else takes it in.
        rounds: Rounds in the draft.

    Returns:
        pl.DataFrame: A pick history.
    """
    rows = []
    # Player ids are unique per manager-season, because a player can only be
    # drafted once per draft. Reusing one id across the room made every manager
    # look fiercely loyal to the same quarterback.
    player_id = iter(range(1000, 100000))
    for season in seasons:
        for owner in owners:
            taken = early_round if owner == early else normal_round
            rows.append({"season": season, "owner": owner, "owner_id": owner,
                         "position": position, "round": taken, "rounds": rounds,
                         "player_name": f"{owner} {position} {season}",
                         "player_id": next(player_id), "overall_pick": taken})
            # A running back every round, so the frame looks like a real draft
            # rather than one pick per manager.
            for other in range(1, rounds + 1):
                if other == taken:
                    continue
                rows.append({"season": season, "owner": owner, "owner_id": owner,
                             "position": "RB", "round": other, "rounds": rounds,
                             "player_name": f"{owner} RB{other} {season}",
                             "player_id": next(player_id), "overall_pick": other})
    return _picks(rows)


# --- positional timing ---------------------------------------------------

def test_the_room_baseline_leaves_the_manager_out():
    """Five managers at round 8 and one at round 2 is six rounds, not five."""
    picks = _league([2023, 2024], ["a", "b", "c", "d", "e", "f"], early="a")
    timing = dt.positional_timing(picks)
    early = timing.filter((pl.col("owner") == "a") & (pl.col("position") == "QB"))
    assert early["delta"][0] == pytest.approx(-6.0)
    assert early["room_round"][0] == pytest.approx(8.0)
    # And the five identical managers are each measured against a room that
    # includes the outlier, so they read as slightly late rather than average.
    other = timing.filter((pl.col("owner") == "b") & (pl.col("position") == "QB"))
    assert other["delta"][0] == pytest.approx(1.2)


def test_never_drafting_a_position_is_censored_not_dropped():
    """A manager who never takes a kicker is the latest, not the average."""
    picks = _league([2023, 2024], ["a", "b"], early="a", position="K",
                    early_round=3, normal_round=9, rounds=10)
    timing = dt.positional_timing(picks)
    # Nobody drafted a TE, so every manager is censored to rounds + 1 and no
    # manager can differ from the room.
    tight_end = timing.filter(pl.col("position") == "TE")
    assert set(tight_end["own_round"].to_list()) == {11.0}
    assert all(delta == pytest.approx(0.0) for delta in tight_end["delta"])


def test_one_loud_season_is_not_a_tendency():
    """A single wild year has the effect size but not the consistency."""
    picks = pl.concat([
        _league([2020, 2021, 2022], ["a", "b", "c"], early=None),
        _league([2023], ["a", "b", "c"], early="a", early_round=1),
    ])
    timing = dt.positional_timing(picks)
    row = timing.filter((pl.col("owner") == "a") & (pl.col("position") == "QB"))
    assert abs(row["delta"][0]) >= dt.TIMING_MIN_DELTA
    assert row["consistency"][0] < dt.TIMING_MIN_CONSISTENCY
    assert dt._timing_traits(row) == []


def test_auction_seasons_carry_no_timing():
    """Nomination order is not a valuation, so an auction has no early or late."""
    picks = _league([2023, 2024], ["a", "b"], early="a").with_columns(
        pl.lit("AUCTION").alias("draft_type"))
    assert dt.positional_timing(picks).is_empty()


# --- NFL-team lean -------------------------------------------------------

def test_a_team_lean_is_measured_against_what_the_league_drafted():
    picks = _picks(
        [{"owner": "homer", "pro_team": "PIT", "player_id": i} for i in range(10)]
        + [{"owner": "rest", "pro_team": "DET", "player_id": 50 + i}
           for i in range(30)]
    )
    lean = dt.team_lean(picks).filter(pl.col("owner") == "homer")
    top = lean.row(0, named=True)
    assert top["pro_team"] == "PIT"
    assert top["picks"] == 10
    assert top["expected"] == pytest.approx(2.5)
    assert top["z"] > dt.TEAM_MIN_Z


def test_a_manager_is_not_charged_for_seasons_they_missed():
    """A team that only became draftable later cannot be a lean away from it."""
    picks = _picks(
        [{"season": 2020, "owner": o, "pro_team": "DET", "player_id": i}
         for i, o in enumerate(["a", "b"] * 5)]
        + [{"season": 2024, "owner": "b", "pro_team": "PIT", "player_id": 60 + i}
           for i in range(10)]
    )
    lean = dt.team_lean(picks)
    # Manager a never saw 2024, so Pittsburgh is not part of their expectation.
    a_pit = lean.filter((pl.col("owner") == "a") & (pl.col("pro_team") == "PIT"))
    assert a_pit.is_empty() or a_pit["expected"][0] == pytest.approx(0.0)


# --- player loyalty ------------------------------------------------------

def test_loyalty_is_over_the_drafts_the_player_was_available_in():
    """Three of three beats three of ten, and the rate says so."""
    rows = []
    for season in range(2016, 2026):
        rows.append({"season": season, "owner": "a", "player_id": 7,
                     "player_name": "His Guy"} if season >= 2023 else
                    {"season": season, "owner": "a", "player_id": 99,
                     "player_name": "Someone"})
        rows.append({"season": season, "owner": "b", "player_id": 98,
                     "player_name": "Other"})
    loyalty = dt.player_loyalty(_picks(rows))
    guy = loyalty.filter((pl.col("owner") == "a")
                         & (pl.col("player_name") == "His Guy")).row(0, named=True)
    assert guy["times"] == 3
    # Available in three drafts, taken in all three -- not 3-of-10.
    assert guy["opportunities"] == 3
    assert guy["rate"] == pytest.approx(1.0)


# --- rookies -------------------------------------------------------------

def test_rookie_appetite_is_compared_within_a_season():
    """Rookie supply rose over the decade; a manager must not be scored for that.

    Manager ``old`` drafted only in 2016, when the league took one rookie in ten
    picks. Manager ``new`` drafted only in 2025, when it took five in ten. Both
    matched their own room exactly, so neither has a rookie tendency -- pooled,
    ``old`` would read as strongly rookie-averse.
    """
    rows = []
    for season, rookies in ((2016, 1), (2025, 5)):
        for owner in (("old", "peer") if season == 2016 else ("new", "peer")):
            for i in range(10):
                rows.append({"season": season, "owner": owner,
                             "player_id": season * 100 + i,
                             "is_rookie": i < rookies})
    habits = dt.habits(_picks(rows))
    for owner in ("old", "new"):
        row = habits.filter(pl.col("owner") == owner).row(0, named=True)
        assert row["rookie_delta"] == pytest.approx(0.0)


# --- descriptions --------------------------------------------------------

def test_a_single_draft_is_named_as_such_rather_than_described():
    picks = _league([2024], ["a", "b"], early="a")
    described = dt.build_tendencies(picks)
    row = described.filter(pl.col("owner") == "a").row(0, named=True)
    assert row["seasons"] == 1
    assert row["traits"] == []
    assert "One draft on record" in row["description"]


def test_a_manager_who_matches_the_room_is_told_so():
    picks = _league([2022, 2023, 2024], ["a", "b", "c"], early=None)
    row = (dt.build_tendencies(picks)
           .filter(pl.col("owner") == "a").row(0, named=True))
    assert row["headline"] == "Consensus"
    assert "near consensus" in row["description"]


def test_a_description_carries_at_most_three_clauses():
    picks = _league([2020, 2021, 2022, 2023], ["a", "b", "c"], early="a")
    row = (dt.build_tendencies(picks)
           .filter(pl.col("owner") == "a").row(0, named=True))
    assert row["description"].count(" — ") <= dt.MAX_TRAITS


def test_the_clause_keeps_its_own_capitals():
    """``str.capitalize`` lowercased player names and position codes."""
    assert dt.sentence_case("has drafted Justin Tucker at QB") == (
        "Has drafted Justin Tucker at QB.")


def test_display_name_fixes_lowercase_without_breaking_mixed_case():
    assert dt.display_name("hank Winfield") == "Hank Winfield"
    assert dt.display_name("Gates McGavick") == "Gates McGavick"


def test_empty_history_gives_an_empty_frame_not_a_crash():
    assert dt.build_tendencies(dh.empty_history()).is_empty()
    assert dt.tendencies_summary(dt.build_tendencies(dh.empty_history())) == (
        "no managers")


# --- the pick history's own guards ---------------------------------------

def test_an_undrafted_season_is_not_a_draft(monkeypatch):
    """ESPN pre-creates every pick slot with ``playerId: -1``.

    Left in, that reads as a completed draft in which every manager took nothing,
    and every position is censored one round past the end for everybody.
    """
    payloads = {
        2025: {"draftDetail": {"drafted": True, "picks": [
                   {"playerId": 11, "roundId": 1, "roundPickNumber": 1,
                    "overallPickNumber": 1, "teamId": 1}]},
               "teams": [{"id": 1, "primaryOwner": "g", "location": "T",
                          "nickname": "One"}],
               "members": [{"id": "g", "firstName": "Gia", "lastName": "N"}],
               "settings": {"draftSettings": {"type": "SNAKE"}}},
        2026: {"draftDetail": {"drafted": False, "picks": [
                   {"playerId": -1, "roundId": 1, "roundPickNumber": 1,
                    "overallPickNumber": 1, "teamId": 1}]},
               "teams": [{"id": 1, "primaryOwner": "g"}],
               "members": [], "settings": {"draftSettings": {"type": "SNAKE"}}},
    }
    monkeypatch.setattr(dh, "_get_json",
                        lambda url, **kw: payloads[2026 if "2026" in url else 2025])
    monkeypatch.setattr(dh, "player_universe",
                        lambda season, cookies: {11: {"name": "A Back",
                                                      "position": "RB",
                                                      "pro_team": "DET"}})
    monkeypatch.setattr(dh, "entry_years", lambda seasons: {})

    history = dh.fetch_draft_history(1, [2025, 2026], "swid", "s2",
                                     current_season=2026)
    assert history["season"].unique().to_list() == [2025]


def test_team_defences_survive_the_placeholder_filter(monkeypatch):
    """D/ST ids are negative; a ``playerId <= 0`` filter would delete them all."""
    payload = {"draftDetail": {"drafted": True, "picks": [
                   {"playerId": -16027, "roundId": 12, "roundPickNumber": 1,
                    "overallPickNumber": 12, "teamId": 1},
                   {"playerId": -1, "roundId": 13, "roundPickNumber": 1,
                    "overallPickNumber": 13, "teamId": 1}]},
               "teams": [{"id": 1, "primaryOwner": "g", "name": "T"}],
               "members": [{"id": "g", "firstName": "Gia", "lastName": "N"}],
               "settings": {"draftSettings": {"type": "SNAKE"}}}
    monkeypatch.setattr(dh, "_get_json", lambda url, **kw: payload)
    monkeypatch.setattr(dh, "player_universe",
                        lambda season, cookies: {-16027: {"name": "Buccaneers D/ST",
                                                          "position": "D/ST",
                                                          "pro_team": "TB"}})
    monkeypatch.setattr(dh, "entry_years", lambda seasons: {})

    history = dh.fetch_draft_history(1, [2025], "swid", "s2", current_season=2026)
    assert history["player_name"].to_list() == ["Buccaneers D/ST"]


def test_owners_come_from_the_team_not_the_pick(monkeypatch):
    """``memberId`` on a pick is not the drafter in six of the older seasons.

    Every pick in those drafts carries one member GUID, which credited a single
    manager with all 96 picks of the 2016 draft.
    """
    payload = {"draftDetail": {"drafted": True, "picks": [
                   {"playerId": 11, "roundId": 1, "roundPickNumber": 1,
                    "overallPickNumber": 1, "teamId": 1, "memberId": "commissioner"},
                   {"playerId": 12, "roundId": 1, "roundPickNumber": 2,
                    "overallPickNumber": 2, "teamId": 2, "memberId": "commissioner"}]},
               "teams": [{"id": 1, "primaryOwner": "g1", "name": "One"},
                         {"id": 2, "primaryOwner": "g2", "name": "Two"}],
               "members": [{"id": "g1", "firstName": "Ann", "lastName": "A"},
                           {"id": "g2", "firstName": "Bo", "lastName": "B"}],
               "settings": {"draftSettings": {"type": "SNAKE"}}}
    monkeypatch.setattr(dh, "_get_json", lambda url, **kw: payload)
    monkeypatch.setattr(dh, "player_universe", lambda season, cookies: {
        11: {"name": "P1", "position": "RB", "pro_team": "DET"},
        12: {"name": "P2", "position": "WR", "pro_team": "DET"}})
    monkeypatch.setattr(dh, "entry_years", lambda seasons: {})

    history = dh.fetch_draft_history(1, [2025], "swid", "s2", current_season=2026)
    assert history["owner"].to_list() == ["Ann A", "Bo B"]


def test_a_name_missing_from_an_old_payload_is_filled_from_a_newer_one():
    """ESPN drops firstName/lastName from old payloads but keeps the GUID."""
    old = {"members": [{"id": "g1", "displayName": "email@example.com"}]}
    new = {"members": [{"id": "g1", "firstName": "Ann", "lastName": "A"}]}
    assert dh._member_names([old, new]) == {"g1": "Ann A"}
