"""The FantasyPros scraper's session handling and its two long-standing map bugs.

Network-free: every test here works on module constants and the parsing helpers, so
it runs in CI, offline, and without a FantasyPros account.

The thing worth guarding is the **registration fence**. Anonymously FantasyPros serves
ten rows per position -- 60 players across the six -- behind a "Create a free account
to unlock" fence, and that is what every board built before 2026-08-24 was blended on.
A free account lifts it to 592. An expired cookie does not raise: it silently returns
the teaser, which is this repo's recurring failure mode of an absent source reading as
agreement. So the loud paths matter more than the happy one.
"""

import re

import pytest

from Scripts import scrape_FP as fp


# --- robots.txt compliance ------------------------------------------------

def test_the_crawl_delay_matches_what_robots_asks_for():
    """`https://www.fantasypros.com/robots.txt` sets `Crawl-delay: 5`.

    The scraper used to fire six requests back to back with no pause. A measured
    10.2s time-to-first-byte during testing on 2026-08-24 suggests that was noticed.
    """
    assert fp.CRAWL_DELAY_SECONDS >= 5.0


#: Paths robots.txt leaves open that this module is allowed to request.
#:
#: `/nfl/rankings/` joined the list when the rest-of-season scrape landed. It is
#: allowed for the same reason `/nfl/projections/` is -- robots.txt disallows only
#: `/ajax/`, `/api/`, `/json/`, `/xml/` and `/nfl/ranker/`, and `ranker` is not
#: `rankings`.
ALLOWED_PATHS = ("/nfl/projections/", "/nfl/rankings/")


def test_the_scraper_only_reads_a_robots_allowed_path():
    """`/api/`, `/json/`, `/ajax/`, `/xml/` and `/nfl/ranker/` are Disallow-ed.

    Scans the URL **constants** as well as the in-function assignments. The
    original version matched only `url = "https://..."` lines, so hoisting a URL to
    a module constant -- which is what `ROS_URL` did -- would have walked straight
    past the guard.
    """
    import inspect

    src = inspect.getsource(fp)
    # Prose mentioning robots.txt is not a fetch, so scan assignment lines only.
    fetched = [ln for ln in src.splitlines()
               if re.match(r'\s*[A-Za-z_]+\s*=\s*\(?\s*f?["\']https?://', ln)]
    assert len(fetched) >= 2, "expected the projections and rankings URLs"
    for ln in fetched:
        u = re.search(r'https://www\.fantasypros\.com(/[^"\'{\s?]*)', ln)
        assert u, f"unrecognised host in: {ln.strip()}"
        path = u.group(1)
        assert any(path.startswith(ok) for ok in ALLOWED_PATHS), \
            f"path outside the allowed set: {path}"
        for banned in ("/api/", "/json/", "/ajax/", "/xml/", "/nfl/ranker/"):
            assert banned not in ln


def test_the_rankings_path_is_rankings_and_not_ranker():
    """One letter apart, and `/nfl/ranker/` is the one robots.txt disallows."""
    assert "/nfl/rankings/" in fp.ROS_URL
    assert "/nfl/ranker/" not in fp.ROS_URL


# --- the session ----------------------------------------------------------

def test_a_missing_session_is_none_rather_than_an_exception(monkeypatch):
    """No account configured must degrade to the ten-row teaser, not crash. The
    scrape still has to run for someone who has not set a cookie up."""
    def boom():
        raise FileNotFoundError("no config.yaml here")

    monkeypatch.setattr("Scripts.config_utils.load_config", boom)
    assert fp._session_cookie() is None


def test_an_absent_fantasypros_block_is_none(monkeypatch):
    monkeypatch.setattr("Scripts.config_utils.load_config",
                        lambda: {"season": 2026, "leagues": {}})
    assert fp._session_cookie() is None


def test_an_empty_cookie_is_none_not_an_empty_header(monkeypatch):
    """An empty string would be sent as `Cookie: `, which is worse than sending
    nothing -- it looks configured and behaves anonymously."""
    monkeypatch.setattr("Scripts.config_utils.load_config",
                        lambda: {"fantasypros": {"cookie": ""}})
    assert fp._session_cookie() is None


def test_a_configured_cookie_is_returned(monkeypatch):
    monkeypatch.setattr("Scripts.config_utils.load_config",
                        lambda: {"fantasypros": {"cookie": "sessionid=abc; fptoken=def"}})
    assert fp._session_cookie() == "sessionid=abc; fptoken=def"


# --- the two map bugs, pinned --------------------------------------------

def test_team_map_still_maps_to_abbreviations():
    """Regression: line 58 read `dst_map = team_map = {...}` until 2026-08-24.

    That rebound `team_map` to the D/ST display map, so D/ST rows stored a
    `playerTeam` of "Texans D/ST" rather than "HOU" and the abbreviation map was
    unreachable for the rest of the module.
    """
    assert fp.team_map["Houston Texans"] == "HOU"
    assert fp.team_map["Kansas City Chiefs"] == "KC"
    assert fp.dst_map["Houston Texans"] == "Texans D/ST"
    assert fp.team_map is not fp.dst_map
    # Every value in team_map is an abbreviation, not a display name.
    assert all(len(v) <= 3 and "/" not in v for v in fp.team_map.values())


def test_both_maps_cover_all_thirty_two_teams_and_spell_chicago_correctly():
    """`'Chicago Beaars'` was a key in both dicts, so Chicago never mapped."""
    assert len(fp.team_map) == 32, f"team_map has {len(fp.team_map)} teams"
    assert len(fp.dst_map) == 32, f"dst_map has {len(fp.dst_map)} teams"
    assert fp.team_map["Chicago Bears"] == "CHI"
    assert fp.dst_map["Chicago Bears"] == "Bears D/ST"
    assert not any("Beaars" in k for k in fp.team_map)
    assert not any("Beaars" in k for k in fp.dst_map)
    # The two must describe the same league.
    assert set(fp.team_map) == set(fp.dst_map)


def test_every_position_is_scraped():
    assert fp.pos_list == ["qb", "rb", "wr", "te", "k", "dst"]
    assert fp.DRAFT_WEEK == "draft"


# --- the weekly scrape's cadence and its merge ---------------------------
#
# Added 2026-09-08. The weekly file on disk that day held 60 rows for week 1, stamped
# 2026-08-03 -- the anonymous teaser, scraped three weeks before the account that
# lifts the fence existed, and nothing had ever re-run it. Authenticated the same
# page returns 597 rows over 595 players.


def _recorder(monkeypatch, rows_per_week=3):
    """Swap `get_fp` for something that records the weeks asked for."""
    import pandas as pd
    asked = []

    def fake_get_fp(wk, year=None):
        asked.append(wk)
        return pd.DataFrame({"week": [wk] * rows_per_week,
                             "player_name": [f"w{wk}p{i}" for i in range(rows_per_week)],
                             "proj_rushingYards": [10.0] * rows_per_week})

    monkeypatch.setattr(fp, "get_fp", fake_get_fp)
    return asked


@pytest.fixture
def fp_season_dir(tmp_path, monkeypatch):
    """Redirect the scraper's output so no test writes the real projections."""
    def fake_season_dir(source, season, *parts, **kwargs):
        out = tmp_path / str(source) / str(season)
        out.mkdir(parents=True, exist_ok=True)
        return out.joinpath(*parts) if parts else out
    monkeypatch.setattr(fp, "season_dir", fake_season_dir)
    return tmp_path


def test_the_weekly_scrape_fetches_the_current_week_alone(monkeypatch, fp_season_dir):
    """It used to fetch `range(1, week + 1)` every time.

    Six requests a week at the 5s crawl delay is 30s at week 1 and **nine minutes at
    week 18**, on a nightly job -- and it re-requests a projections page for games
    already played, which is not necessarily the number the blend voted with.
    """
    asked = _recorder(monkeypatch)
    monkeypatch.setattr(fp, "current_week", lambda: 7)
    fp.scrape_weekly(season=2026)
    assert asked == [7]


def test_the_week_is_resolved_at_call_time_not_at_import_time(monkeypatch,
                                                              fp_season_dir):
    """`WEEK = current_week()` is bound at import, and a stale schedule pins it at 1.

    Found 2026-09-08: `Data/NFL_Schedules.csv` was frozen at 08-14 with no scores, so
    `current_week()` returned 1 -- and would have returned 1 for the rest of the
    season, scraping week 1 every night. The schedule is refreshed by the nightly
    now, which only helps if this reads it after that runs.
    """
    asked = _recorder(monkeypatch)
    monkeypatch.setattr(fp, "WEEK", 1)
    monkeypatch.setattr(fp, "current_week", lambda: 9)
    fp.scrape_weekly(season=2026)
    assert asked == [9]


def test_a_backfill_still_reaches_every_week(monkeypatch, fp_season_dir):
    asked = _recorder(monkeypatch)
    fp.scrape_weekly(season=2026, weeks=[1, 2, 3])
    assert asked == [1, 2, 3]


def test_merging_never_overwrites_a_game_already_played(monkeypatch, fp_season_dir):
    """A *played game* freezes at its last pre-kickoff capture.

    Re-scraping it would silently rewrite what the blend voted with, and the file is
    the only record of that.

    **Per game, not per week**, since 2026-09-16. The old rule froze the whole week at
    first capture, which protected Thursday's opener and also froze Sunday's slate
    four days early -- discarding every Friday practice report and Saturday inactive
    in between. Both halves are asserted here: NE has kicked off and holds, KC has not
    and takes the new number.
    """
    import pandas as pd
    _recorder(monkeypatch)
    monkeypatch.setattr(fp, "current_week", lambda: 1)
    monkeypatch.setattr("Scripts.kickoff_freeze.started_teams",
                        lambda season, weeks: {(1, "NE")})

    monkeypatch.setattr(fp, "get_fp", lambda wk, year=None: pd.DataFrame(
        {"week": [1, 1], "player_name": ["A", "B"], "playerTeam": ["NE", "KC"],
         "proj_rushingYards": [100.0, 50.0]}))
    fp.scrape_weekly(season=2026)

    monkeypatch.setattr(fp, "get_fp", lambda wk, year=None: pd.DataFrame(
        {"week": [1, 1], "player_name": ["A", "B"], "playerTeam": ["NE", "KC"],
         "proj_rushingYards": [999.0, 999.0]}))
    out = fp.scrape_weekly(season=2026).sort_values("player_name")
    assert out["proj_rushingYards"].tolist() == [100.0, 999.0]

    # And `--no-merge` is how to replace a bad capture on purpose -- which is what
    # the authenticated re-scrape of week 1 had to do on 2026-09-08. It bypasses the
    # kickoff rule entirely, which is the point: it is the deliberate override.
    out = fp.scrape_weekly(season=2026, merge=False).sort_values("player_name")
    assert out["proj_rushingYards"].tolist() == [999.0, 999.0]


def test_an_unreadable_scoreboard_holds_every_week_rather_than_rewriting_one(
        monkeypatch, fp_season_dir):
    """The failure direction, and it is the opposite of `kickoff_freeze.apply`'s.

    This file is the only record of what FantasyPros said before kickoff, so an
    unknown clock must mean "change nothing", not "take everything fresh".
    """
    import pandas as pd
    _recorder(monkeypatch)
    monkeypatch.setattr(fp, "current_week", lambda: 1)

    def explode(season, weeks):
        raise RuntimeError("scoreboard down")

    monkeypatch.setattr(fp, "get_fp", lambda wk, year=None: pd.DataFrame(
        {"week": [1], "player_name": ["A"], "playerTeam": ["KC"],
         "proj_rushingYards": [50.0]}))
    monkeypatch.setattr("Scripts.kickoff_freeze.started_teams",
                        lambda season, weeks: set())
    fp.scrape_weekly(season=2026)

    monkeypatch.setattr("Scripts.kickoff_freeze.started_teams", explode)
    monkeypatch.setattr(fp, "get_fp", lambda wk, year=None: pd.DataFrame(
        {"week": [1], "player_name": ["A"], "playerTeam": ["KC"],
         "proj_rushingYards": [999.0]}))
    out = fp.scrape_weekly(season=2026)
    assert out["proj_rushingYards"].tolist() == [50.0]


def test_merging_keeps_the_weeks_it_did_not_scrape(monkeypatch, fp_season_dir):
    """The file must stay cumulative.

    `clean_lineups` re-merges it onto every week in the lineup frame, and that frame
    gains a week every Tuesday -- so a current-week-only file would blank FantasyPros
    for every prior week and turn stored history into an ESPN-only board.
    """
    _recorder(monkeypatch)
    fp.scrape_weekly(season=2026, weeks=[1, 2])
    out = fp.scrape_weekly(season=2026, weeks=[3])
    assert sorted(out["week"].unique().tolist()) == [1, 2, 3]


# --- the --weeks spec -----------------------------------------------------

def test_parse_weeks_accepts_a_range_and_a_list():
    assert fp.parse_weeks("1-3") == [1, 2, 3]
    assert fp.parse_weeks("1,4,7") == [1, 4, 7]
    assert fp.parse_weeks("1-2,5") == [1, 2, 5]


def test_parse_weeks_is_empty_rather_than_zero_for_no_spec():
    """None means "fall back to the current week", and 0 would mean week zero."""
    assert fp.parse_weeks(None) is None
    assert fp.parse_weeks("") is None


def test_parse_weeks_refuses_a_backwards_range():
    with pytest.raises(ValueError, match="backwards"):
        fp.parse_weeks("5-2")


# --- an empty table from upstream -----------------------------------------
#
# 2026-09-15 06:01. FantasyPros served week 2's projection tables with headers and no
# rows -- it regenerates them at the Tuesday rollover, and the same call returned 595
# players by 11:59 that morning. Two separate defects turned one transient hour into
# a night with no boards at all, and both are pinned here.


def _empty_like_a_served_table():
    """What `get_fp` builds when every position's table parses to zero rows."""
    import pandas as pd
    return pd.DataFrame({"week": [], "player_name": [], "proj_rushingYards": []})


def test_an_empty_scrape_keeps_its_columns(monkeypatch):
    """The nameless-row guard must not delete the column it guards.

    `.apply()` over no rows returns float64, not bool, and pandas reads a
    non-boolean Series in `frame[...]` as a list of *column labels*. The guard
    therefore selected zero columns and returned 0x0, so an empty upstream table
    reached the caller as `KeyError: 'player_name'` rather than as no rows.
    """
    import pandas as pd
    out = _empty_like_a_served_table()

    named = out["player_name"].apply(
        lambda v: isinstance(v, str) and v.strip() != "").astype(bool)
    assert named.dtype == bool
    kept = out[named].reset_index(drop=True)
    assert list(kept.columns) == ["week", "player_name", "proj_rushingYards"]
    assert kept.empty

    # And the predicate still drops a non-string name, which is what keeps an
    # integer 0 out of the parquet write.
    rows = pd.DataFrame({"week": [1, 1], "player_name": ["A", 0],
                         "proj_rushingYards": [10.0, 20.0]})
    named = rows["player_name"].apply(
        lambda v: isinstance(v, str) and v.strip() != "").astype(bool)
    assert rows[named]["player_name"].tolist() == ["A"]


def test_an_empty_capture_is_never_written(monkeypatch, fp_season_dir):
    """A bad hour upstream must not blank a file that has to stay cumulative.

    The write used to happen before anything looked at what had been scraped, so
    `--no-merge` over an empty capture would rewrite the cumulative file as empty --
    and `clean_lineups` re-merges that onto every week, turning stored history into
    an ESPN-only board retroactively.
    """
    _recorder(monkeypatch)
    fp.scrape_weekly(season=2026, weeks=[1])

    monkeypatch.setattr(fp, "get_fp",
                        lambda wk, year=None: _empty_like_a_served_table())
    out = fp.scrape_weekly(season=2026, weeks=[2], merge=False)

    # Week 1 survived a *rewrite* asked for on top of an empty scrape.
    assert out["week"].unique().tolist() == [1]


def test_an_empty_capture_is_not_fatal(monkeypatch, fp_season_dir, capsys):
    """It returns and says so, rather than raising and taking the nightly with it."""
    monkeypatch.setattr(fp, "get_fp",
                        lambda wk, year=None: _empty_like_a_served_table())
    out = fp.scrape_weekly(season=2026, weeks=[2])
    assert out.empty
    assert "returned no rows" in capsys.readouterr().out


def test_an_empty_season_capture_leaves_the_snapshot_alone(monkeypatch,
                                                           fp_season_dir):
    """The season file is rewritten nightly with no merge to fall back on."""
    import pandas as pd
    monkeypatch.setattr(fp, "get_fp", lambda wk, year=None: pd.DataFrame(
        {"week": ["draft"], "player_name": ["A"], "proj_rushingYards": [100.0]}))
    fp.scrape_season_long(season=2026)
    path = fp.season_dir("FantasyPros", 2026,
                         "FantasyPros_Projections_Season.parquet")
    assert len(pd.read_parquet(path)) == 1

    monkeypatch.setattr(fp, "get_fp",
                        lambda wk, year=None: _empty_like_a_served_table())
    fp.scrape_season_long(season=2026)
    assert len(pd.read_parquet(path)) == 1
