"""``Scripts.zero_starters``: starters who finished on zero or less.

The rules worth pinning are the ones a plain ``points <= 0`` filter gets wrong --
``0.0`` before kickoff, free-agent rows that carry a slot, bench and IR, week 1 --
and the one a plain rewrite of the tab gets wrong: ``COMPLETE`` is typed into the
Sheet by hand and must survive every publish.
"""

import pandas as pd
import pytest

from Scripts import paths, store
from Scripts.zero_starters import (
    COMPLETE,
    CURRENT_OPEN_COLOR,
    OVERDUE_OPEN_COLOR,
    LOG_COLUMNS,
    TALLY_COLUMNS,
    carry_marks,
    highlight_rules,
    owner_tally,
    zero_point_starters,
)


def _row(week, owner, name, slot, points, state="post", status="active"):
    return {
        "week": week, "team_owner": owner, "team_name": f"Team {owner}",
        "player_name": name, "slotPosition": slot, "points": points,
        "TRUE_Points": 10.0, "game_state": state, "player_active_status": status,
    }


@pytest.fixture
def lineups():
    return pd.DataFrame([
        _row(1, "Ann", "Week One Dud", "WR", 0.0),
        _row(2, "Ann", "Dud WR", "WR", 0.0, status="inactive"),
        _row(2, "Ann", "Bad DST", "D/ST", -4.0),
        _row(2, "Bob", "Good QB", "QB", 20.0),
        _row(2, "Bob", "Bye RB", "RB", 0.0, state="bye", status="bye"),
        _row(2, "Bob", "Benched Zero", "BE", 0.0),
        _row(2, "Bob", "Hurt TE", "IR", 0.0),
        _row(3, "Bob", "Not Yet", "WR", 0.0, state="pre"),
        _row(3, "Bob", "Playing", "TE", 0.0, state="in"),
        _row(2, "Free Agent", "FA K", "K", -1.0),
        _row(2, "Cal", "Fine", "K", 8.0),
    ])


def _tab(log, marks):
    """The tab as Google returns it: strings, header first, COMPLETE last."""
    rows = [LOG_COLUMNS + [COMPLETE]]
    for r in log.astype(str).itertuples(index=False):
        rows.append(list(r) + [marks.get(r.PLAYER, "")])
    return rows


def test_only_finished_starts_by_real_owners_from_week_two(lineups):
    log = zero_point_starters(lineups)
    assert list(log.columns) == LOG_COLUMNS
    assert set(log["PLAYER"]) == {"Dud WR", "Bad DST", "Bye RB"}


def test_status_separates_a_player_who_sat(lineups):
    log = zero_point_starters(lineups).set_index("PLAYER")
    assert log.loc["Dud WR", "STATUS"] == "inactive"
    assert log.loc["Bye RB", "STATUS"] == "bye"


def test_a_frame_without_game_state_reports_nothing(lineups):
    log = zero_point_starters(lineups.drop(columns="game_state"))
    assert log.empty and list(log.columns) == LOG_COLUMNS


# --- COMPLETE survives a publish -------------------------------------------

def test_marks_follow_their_row_even_when_the_order_changes(lineups):
    log = zero_point_starters(lineups)
    tab = _tab(log, {"Dud WR": "YES", "Bye RB": "N/A"})
    tab[1:] = tab[1:][::-1]  # a new week lands on top and pushes rows down

    carried, lost = carry_marks(log, tab)
    marks = dict(zip(carried["PLAYER"], carried[COMPLETE]))
    assert marks == {"Dud WR": "YES", "Bye RB": "N/A", "Bad DST": ""}
    assert lost.empty


def test_a_new_offence_arrives_unmarked(lineups):
    old = zero_point_starters(lineups[lineups["player_name"] != "Bad DST"])
    carried, _ = carry_marks(zero_point_starters(lineups),
                             _tab(old, {"Dud WR": "YES"}))
    marks = dict(zip(carried["PLAYER"], carried[COMPLETE]))
    assert marks["Bad DST"] == ""
    assert marks["Dud WR"] == "YES"


def test_first_publish_has_an_empty_complete_column(lineups):
    carried, lost = carry_marks(zero_point_starters(lineups), None)
    assert list(carried.columns) == LOG_COLUMNS + [COMPLETE]
    assert (carried[COMPLETE] == "").all() and lost.empty


def test_any_column_the_league_adds_is_kept(lineups):
    log = zero_point_starters(lineups)
    tab = _tab(log, {"Dud WR": "YES"})
    tab[0].append("NOTES")
    for row in tab[1:]:
        row.append("chug" if row[3] == "Dud WR" else "")

    carried, _ = carry_marks(log, tab)
    assert list(carried.columns) == LOG_COLUMNS + [COMPLETE, "NOTES"]
    assert carried.set_index("PLAYER").loc["Dud WR", "NOTES"] == "chug"


def test_retired_generated_columns_are_not_carried(lineups):
    """The bench columns left the log; an old tab still has them."""
    log = zero_point_starters(lineups)
    tab = _tab(log, {"Dud WR": "YES"})
    tab[0][-1:-1] = ["BEST_BENCH", "BENCH_PTS", "LOST"]
    for row in tab[1:]:
        row[-1:-1] = ["Someone", "12.5", "12.5"]

    carried, _ = carry_marks(log, tab)
    assert list(carried.columns) == LOG_COLUMNS + [COMPLETE]
    assert carried.set_index("PLAYER").loc["Dud WR", COMPLETE] == "YES"


def test_a_marked_row_that_stops_qualifying_is_reported(lineups):
    """A stat correction can lift a zero; its mark must not vanish unannounced."""
    old_log = zero_point_starters(lineups)
    corrected = lineups.copy()
    corrected.loc[corrected["player_name"] == "Dud WR", "points"] = 0.4

    carried, lost = carry_marks(zero_point_starters(corrected),
                                _tab(old_log, {"Dud WR": "YES"}))
    assert "Dud WR" not in set(carried["PLAYER"])
    assert list(lost["PLAYER"]) == ["Dud WR"]


def test_week_one_marks_drop_quietly(lineups):
    """Week 1 leaves the log by design, so its marks are not reported as lost."""
    week_one = zero_point_starters(lineups, start_week=1)
    _, lost = carry_marks(zero_point_starters(lineups),
                          _tab(week_one, {"Week One Dud": "N/A"}))
    assert lost.empty


# --- tally ------------------------------------------------------------------

def test_tally_counts_open_punishments(lineups):
    carried, _ = carry_marks(zero_point_starters(lineups),
                             _tab(zero_point_starters(lineups), {"Dud WR": "YES"}))
    tally = owner_tally(lineups, carried)
    assert list(tally.columns) == TALLY_COLUMNS
    t = tally.set_index("OWNER")
    assert set(t.index) == {"Ann", "Bob", "Cal"}
    assert (t.loc["Ann", "COUNT"], t.loc["Ann", "OPEN"]) == (2, 1)
    assert t.loc["Ann", "PTS"] == -4.0
    assert (t.loc["Cal", "COUNT"], t.loc["Cal", "OPEN"]) == (0, 0)


def test_tally_of_an_empty_log(lineups):
    empty, _ = carry_marks(zero_point_starters(lineups.drop(columns="game_state")), None)
    tally = owner_tally(lineups, empty)
    assert set(tally["OWNER"]) == {"Ann", "Bob", "Cal"}
    assert (tally[["COUNT", "OPEN"]] == 0).all().all()


# --- publishing: Knights only, and never without reading first ---------------

@pytest.fixture
def driver(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "STORE_DIR", tmp_path / "Store")
    import populateGoogleSheet as p

    published = {}
    monkeypatch.setattr(p, "build_tables", lambda *a, **k: {})
    monkeypatch.setattr(p, "write_to_google",
                        lambda **kw: published.__setitem__(kw["league_name"],
                                                           kw["df_dict"]))
    monkeypatch.setattr(p, "read_tab", lambda league, tab: None)
    monkeypatch.setattr(p.time, "sleep", lambda _s: None)
    p.published = published
    return p


def test_only_knights_gets_the_tabs(driver, lineups):
    for key in ("knights_ffl", "gop_degenerates"):
        store.write_league_store(2026, key, lineups=lineups,
                                 meta_extra={"current_week": 2})
    driver.run(["Knights_FFL", "GOP_Degenerates"], season=2026)
    assert list(driver.published["Knights_FFL"]) == [
        "Zero_Starters", "Zero_Starters_Owners"]
    assert driver.published["GOP_Degenerates"] == {}


def test_publish_carries_complete_from_the_sheet(driver, lineups, monkeypatch):
    log = zero_point_starters(lineups)
    monkeypatch.setattr(driver, "read_tab",
                        lambda league, tab: _tab(log, {"Bad DST": "YES"}))
    store.write_league_store(2026, "knights_ffl", lineups=lineups,
                             meta_extra={"current_week": 2})
    driver.run(["Knights_FFL"], season=2026)

    out = driver.published["Knights_FFL"]["Zero_Starters"].set_index("PLAYER")
    assert out.loc["Bad DST", COMPLETE] == "YES"


def test_a_failed_read_publishes_nothing(driver, lineups, monkeypatch):
    """If the marks cannot be read, the tab must not be rewritten without them."""
    def unreachable(league, tab):
        raise ConnectionError("Google is down")

    monkeypatch.setattr(driver, "read_tab", unreachable)
    store.write_league_store(2026, "knights_ffl", lineups=lineups,
                             meta_extra={"current_week": 2})
    results = driver.run(["Knights_FFL"], season=2026)
    assert "ConnectionError" in results["Knights_FFL"]
    assert driver.published == {}


# --- highlights ---------------------------------------------------------------

def _formulas(columns):
    rules = highlight_rules(7, columns)
    return [(r["addConditionalFormatRule"]["rule"]["booleanRule"]["condition"]
             ["values"][0]["userEnteredValue"],
             r["addConditionalFormatRule"]["rule"]["booleanRule"]["format"]
             ["backgroundColor"]) for r in rules]


def test_latest_week_open_is_yellow_and_older_open_is_red():
    (current, cur_color), (overdue, old_color) = _formulas(LOG_COLUMNS + [COMPLETE])
    assert current == '=AND($A2<>"", $I2<>"YES", $A2=MAX($A$2:$A))'
    assert overdue == '=AND($A2<>"", $I2<>"YES", $A2<MAX($A$2:$A))'
    assert cur_color == CURRENT_OPEN_COLOR and old_color == OVERDUE_OPEN_COLOR


def test_rules_follow_complete_wherever_it_sits():
    """A hand-added column before COMPLETE moves it; the formula must move too."""
    (current, _), _ = _formulas(LOG_COLUMNS + ["NOTES", COMPLETE])
    assert '$J2<>"YES"' in current


def test_rules_cover_every_column_from_the_first_data_row():
    rng = highlight_rules(7, LOG_COLUMNS + [COMPLETE])[0][
        "addConditionalFormatRule"]["rule"]["ranges"][0]
    assert rng == {"sheetId": 7, "startRowIndex": 1, "startColumnIndex": 0,
                   "endColumnIndex": len(LOG_COLUMNS) + 1}
