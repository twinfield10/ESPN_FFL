"""Which NFL game is where, and the fact that a zero is not a blank.

The whole live-scoring feature rests on one question the pipeline could not previously
answer: has this player's game been played? What is pinned here is the *reduction* of
ESPN's status block to that answer, because the block has two fields that disagree in
the cases that matter -- a postponed game reports ``state: "post"`` with
``completed: false``, and reading the first would freeze every player in it at zero as
a **final** score for a game that is going to be replayed.

Also pinned: the abbreviation contract. The scoreboard's spellings and
``espn_api``'s agree exactly, which is what lets the join to ``pro_team`` be direct
rather than aliased -- so if that ever stops being true, this fails rather than
silently marking teams as permanently on bye.

Synthetic payloads. No network.
"""

import datetime

import polars as pl
import pytest

from Scripts import game_state as gs


def status(state, *, name="", completed=None, period=0, clock="0:00"):
    """A ``competitions[0].status`` block."""
    return {"period": period, "displayClock": clock,
            "type": {"state": state, "name": name,
                     "completed": state == "post" if completed is None else completed,
                     "detail": name or state}}


def event(home, away, state, *, name="", completed=None, period=0, clock="0:00",
          home_score=None, away_score=None, date="2026-09-13T17:00Z"):
    """One scoreboard event."""
    return {"shortName": f"{away} @ {home}", "date": date,
            "competitions": [{
                "status": status(state, name=name, completed=completed,
                                 period=period, clock=clock),
                "competitors": [
                    {"team": {"abbreviation": home}, "score": home_score},
                    {"team": {"abbreviation": away}, "score": away_score},
                ]}]}


def payload(*events):
    return {"events": list(events)}


# --- the elapsed fraction -------------------------------------------------

def test_a_game_not_kicked_off_has_elapsed_nothing():
    """The load-bearing case: this is what makes the whole change inert pre-game."""
    assert gs.elapsed_fraction(gs.PRE, 1, "15:00") == 0.0


def test_a_finished_game_is_fully_elapsed():
    assert gs.elapsed_fraction(gs.POST, 4, "0:00") == 1.0


def test_a_bye_is_fully_elapsed():
    """So the remaining projection is zero rather than the whole of it."""
    assert gs.elapsed_fraction(gs.BYE, 0, None) == 1.0


@pytest.mark.parametrize("period, clock, expected", [
    (1, "15:00", 0.00),      # kickoff
    (1, "7:30", 0.125),
    (2, "0:00", 0.50),       # halftime
    (3, "15:00", 0.50),      # start of the second half
    (4, "0:00", 1.00),       # end of regulation
])
def test_elapsed_tracks_the_game_clock(period, clock, expected):
    assert gs.elapsed_fraction(gs.IN, period, clock) == pytest.approx(expected)


def test_overtime_does_not_exceed_a_full_game():
    """A fraction over 1.0 would make the remaining projection negative, and a
    negative remainder *subtracts* from a team total."""
    assert gs.elapsed_fraction(gs.IN, 5, "10:00") == 1.0


def test_an_unparseable_clock_reads_the_period_as_finished():
    """The conservative direction. Treating it as untouched would hand a player his
    whole projection back in the fourth quarter."""
    assert gs.clock_minutes("not a clock") == 0.0
    assert gs.clock_minutes(None) == 0.0


# --- reducing ESPN's status block -----------------------------------------

def test_the_three_ordinary_states_pass_through():
    frame = gs.parse(payload(
        event("SEA", "NE", "post", name="STATUS_FINAL", home_score="13",
              away_score="10"),
        event("LAR", "SF", "pre", name="STATUS_SCHEDULED"),
        event("KC", "DEN", "in", name="STATUS_IN_PROGRESS", period=2, clock="7:00"),
    ), 2026, 1)
    states = dict(zip(frame["team"], frame["state"]))
    assert states["SEA"] == states["NE"] == gs.POST
    assert states["LAR"] == states["SF"] == gs.PRE
    assert states["KC"] == states["DEN"] == gs.IN


def test_a_postponed_game_is_pre_not_post():
    """ESPN reports it as `post` with `completed: false`. Read literally, every
    player in it is finalised at zero for a game that has not been played."""
    frame = gs.parse(payload(
        event("BUF", "HOU", "post", name="STATUS_POSTPONED", completed=False),
    ), 2026, 1)
    row = frame.filter(pl.col("team") == "BUF").row(0, named=True)
    assert row["state"] == gs.PRE
    assert row["elapsed"] == 0.0


def test_a_cancelled_game_is_post():
    """It genuinely never happens, so zero is the final answer and the projection
    must not stand."""
    frame = gs.parse(payload(
        event("BUF", "HOU", "post", name="STATUS_CANCELED", completed=False),
    ), 2026, 1)
    assert frame.filter(pl.col("team") == "BUF")["state"][0] == gs.POST


def test_a_suspended_game_is_neither_final_nor_untouched():
    """`post` without `completed` and without a name we recognise. Reported as in
    progress, which neither locks in a score nor returns the projection."""
    frame = gs.parse(payload(
        event("BUF", "HOU", "post", name="STATUS_SUSPENDED", completed=False),
    ), 2026, 1)
    assert frame.filter(pl.col("team") == "BUF")["state"][0] == gs.IN


def test_an_unrecognised_state_does_not_silently_become_a_projection():
    frame = gs.parse(payload(event("BUF", "HOU", "quantum")), 2026, 1)
    assert frame.filter(pl.col("team") == "BUF")["state"][0] == gs.IN


# --- byes, which are an absence -------------------------------------------

def test_a_team_with_no_game_is_on_bye():
    frame = gs.parse(payload(
        event("SEA", "NE", "post", name="STATUS_FINAL", home_score="13",
              away_score="10")),
        2026, 6)
    assert frame.height == len(gs.nfl_teams())
    byes = frame.filter(pl.col("state") == gs.BYE)
    assert byes.height == len(gs.nfl_teams()) - 2
    assert "SEA" not in byes["team"].to_list()
    assert byes["elapsed"].unique().to_list() == [1.0]


def test_every_team_appears_exactly_once():
    """Two rows per game plus one per bye, and no duplicates -- the frame is joined
    onto a player-week on (team, week), so a duplicate would double his row."""
    frame = gs.parse(payload(
        event("SEA", "NE", "post", name="STATUS_FINAL"),
        event("KC", "DEN", "pre")), 2026, 1)
    assert frame["team"].n_unique() == frame.height == len(gs.nfl_teams())


# --- the abbreviation contract --------------------------------------------

def test_the_scoreboards_spellings_are_espn_apis_spellings():
    """Verified live 2026-09-10 and pinned here, because it is what lets the join to
    `pro_team` be direct. If ESPN renames a team on one host and not the other, this
    fails -- rather than the pipeline marking it as permanently on bye."""
    from espn_api.football.constant import PRO_TEAM_MAP

    teams = gs.nfl_teams()
    assert len(teams) == 32
    assert teams == frozenset(v for v in PRO_TEAM_MAP.values()
                              if v and v not in ("None", "FA"))


def test_the_schedules_two_disagreements_are_the_ones_we_alias():
    from Scripts.nfl_utils import ESPN_TEAM_ALIASES

    assert ESPN_TEAM_ALIASES == {"LAR": "LA", "WSH": "WAS"}
    assert set(ESPN_TEAM_ALIASES) <= gs.nfl_teams()


# --- fetch guards ---------------------------------------------------------

def test_a_payload_with_no_events_is_an_error_not_an_empty_week():
    """ESPN answers a nonsense season or week with 200 and an empty body. Read as
    data, that is a week in which nobody plays -- so every player is on bye and every
    projection is zeroed."""
    class _Response:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"leagues": []}

    original = gs.requests.get
    gs.requests.get = lambda *a, **k: _Response()
    try:
        with pytest.raises(ValueError, match="no 'events' array"):
            gs.fetch(2026, 1)
    finally:
        gs.requests.get = original


# --- the cache ------------------------------------------------------------

def test_a_finished_week_never_goes_stale():
    """`clean_lineups` sees weeks 1..current and runs once per league, so without
    this the nightly re-fetches every played week nine times over to re-learn
    scores that were final in September."""
    settled = gs.parse(payload(
        event("SEA", "NE", "post", name="STATUS_FINAL")), 2026, 1)
    assert gs._settled(settled)

    live = gs.parse(payload(
        event("SEA", "NE", "in", name="STATUS_IN_PROGRESS", period=1, clock="9:00")),
        2026, 1)
    assert not gs._settled(live)


def test_anything_live_is_what_the_cron_branches_on():
    quiet = gs.parse(payload(event("SEA", "NE", "pre")), 2026, 1)
    assert not gs.anything_live(quiet)
    busy = gs.parse(payload(
        event("SEA", "NE", "in", name="STATUS_IN_PROGRESS", period=3, clock="2:00")),
        2026, 1)
    assert gs.anything_live(busy)


# --- the offline fallback -------------------------------------------------

def test_the_schedule_fallback_aliases_onto_espns_spellings(monkeypatch):
    """nflverse says LA and WAS; the consumer joins on `pro_team`, which says LAR and
    WSH. This is the one function in the module that aliases anything."""
    schedule = pl.DataFrame({
        "season": [2026, 2026], "week": [1, 1],
        "gameday": ["2026-09-13", "2026-09-13"],
        "gametime": ["13:00", "16:25"],
        "home_team": ["LA", "WAS"], "away_team": ["SF", "PHI"],
        "home_score": ["NA", "NA"], "away_score": ["NA", "NA"],
    })
    monkeypatch.setattr("Scripts.nfl_utils.load_schedule", lambda *a, **k: schedule)
    frame = gs.from_schedule(
        2026, 1, now=datetime.datetime(2026, 9, 12, tzinfo=datetime.timezone.utc))
    playing = set(frame.filter(pl.col("state") != gs.BYE)["team"].to_list())
    assert playing == {"LAR", "SF", "WSH", "PHI"}
    assert "LA" not in playing and "WAS" not in playing


def test_the_fallback_reads_a_score_as_a_finished_game(monkeypatch):
    schedule = pl.DataFrame({
        "season": [2026], "week": [1], "gameday": ["2026-09-09"],
        "gametime": ["20:20"], "home_team": ["SEA"], "away_team": ["NE"],
        "home_score": ["13"], "away_score": ["10"],
    })
    monkeypatch.setattr("Scripts.nfl_utils.load_schedule", lambda *a, **k: schedule)
    frame = gs.from_schedule(
        2026, 1, now=datetime.datetime(2026, 9, 10, tzinfo=datetime.timezone.utc))
    row = frame.filter(pl.col("team") == "SEA").row(0, named=True)
    assert row["state"] == gs.POST
    assert row["team_score"] == 13.0 and row["opp_score"] == 10.0


def test_the_fallback_refuses_another_seasons_schedule(monkeypatch):
    """Silently returning another season's states would put week 1 of 2026 on 2025's
    fixtures, and every team's opponent would be wrong."""
    schedule = pl.DataFrame({
        "season": [2025], "week": [1], "gameday": ["2025-09-07"],
        "gametime": ["13:00"], "home_team": ["SEA"], "away_team": ["NE"],
        "home_score": ["NA"], "away_score": ["NA"],
    })
    monkeypatch.setattr("Scripts.nfl_utils.load_schedule", lambda *a, **k: schedule)
    with pytest.raises(ValueError, match="not 2026"):
        gs.from_schedule(2026, 1)


def test_kickoff_is_converted_from_eastern(monkeypatch):
    """nflverse publishes `gametime` in US/Eastern. A fixed offset would put every
    game after the November daylight boundary an hour wrong."""
    schedule = pl.DataFrame({
        "season": [2026, 2026], "week": [1, 10],
        "gameday": ["2026-09-13", "2026-11-15"],
        "gametime": ["13:00", "13:00"],
        "home_team": ["SEA", "SEA"], "away_team": ["NE", "NE"],
        "home_score": ["NA", "NA"], "away_score": ["NA", "NA"],
    })
    monkeypatch.setattr("Scripts.nfl_utils.load_schedule", lambda *a, **k: schedule)
    september = gs.from_schedule(
        2026, 1, now=datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc))
    november = gs.from_schedule(
        2026, 10, now=datetime.datetime(2026, 11, 1, tzinfo=datetime.timezone.utc))
    # EDT is UTC-4 in September, EST is UTC-5 in November.
    assert september.filter(pl.col("team") == "SEA")["kickoff_utc"][0].startswith(
        "2026-09-13T17:00")
    assert november.filter(pl.col("team") == "SEA")["kickoff_utc"][0].startswith(
        "2026-11-15T18:00")
