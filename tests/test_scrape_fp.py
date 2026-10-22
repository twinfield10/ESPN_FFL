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

import json
import re

import pandas as pd
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


# ---------------------------------------------------------------------------
# The projections come out of the page's `FP.reportConfig` JSON, not a table. On
# 2026-10-22 FantasyPros stopped server-rendering `<table id="data">`; the 06:00
# nightly failed as "Missing optional dependency 'html5lib'" because the scraper
# handed pandas the string "None". The shapes below are trimmed from that morning's
# pages: the season-long (`week=draft`) fields carry `bye`, the weekly ones do not,
# and a D/ST row's `team` is blank.
# ---------------------------------------------------------------------------

def _page(fields, rows, *, prefix="<script>var FP = FP || {};\nFP.reportConfig = ",
          suffix=';\nFP.other = {"a": "b;c"};</script>'):
    """A projections page carrying one `FP.reportConfig` table."""
    config = {"type": "report", "title": "Projections",
              "table": {"fields": [{"key": "player", "label": "Player"},
                                   *({"key": k, "label": k.upper()} for k in fields)],
                        "rows": rows, "emptyMessage": "No data"}}
    return "<html><body>" + prefix + json.dumps(config) + suffix + "</body></html>"


_QB_FIELDS = ["pass_att", "pass_cmp", "pass_yds", "pass_tds", "pass_ints",
              "rush_att", "rush_yds", "rush_tds", "fumbles", "fpts"]


def _qb_row(**extra):
    return {"id": 17298, "player": {"id": 17298, "name": "Josh Allen", "team": "BUF"},
            "_filterGame": "", "pass_att": 504.5, "pass_cmp": 341.7, "pass_yds": 3889,
            "pass_tds": 26.8, "pass_ints": 11.7, "rush_att": 115.5, "rush_yds": 577.8,
            "rush_tds": 11.1, "fumbles": 4.1, "fpts": 367.14, **extra}


@pytest.mark.parametrize("with_bye", [True, False], ids=["season", "weekly"])
def test_a_qb_page_reads_the_same_with_or_without_a_bye_field(with_bye):
    fields = (["bye"] if with_bye else []) + _QB_FIELDS
    row = _qb_row(**({"bye": 7} if with_bye else {}))
    out = fp.parse_report_table(_page(fields, [row]), "qb")
    assert list(out.columns) == ["player_name", "playerTeam",
                                 *fp.FIELD_COLUMNS["qb"].values()]
    assert out.loc[0, "player_name"] == "Josh Allen"
    assert out.loc[0, "playerTeam"] == "BUF"
    # Rushing and passing attempts are separate keys, not two `ATT` headers.
    assert out.loc[0, "proj_passingAttempts"] == 504.5
    assert out.loc[0, "proj_rushingAttempts"] == 115.5
    assert out.loc[0, "STD_FantasyPoints"] == 367.14


def test_an_empty_table_still_has_its_columns():
    """The Tuesday-rollover hour: fields served, no rows."""
    out = fp.parse_report_table(_page(_QB_FIELDS, []), "qb")
    assert out.empty
    assert {"player_name", "playerTeam", "proj_passingYards"} <= set(out.columns)


def test_dst_ignores_fields_it_does_not_use():
    fields = ["bye", "def_sack", "def_int", "def_fr", "def_ff", "def_td",
              "def_safety", "def_pa", "def_tyda", "fpts"]
    row = {"id": 8120, "player": {"id": 8120, "name": "Houston Texans", "team": "",
                                  "shortName": "HOU DST"},
           "bye": 8, "def_sack": 49.6, "def_int": 14.4, "def_fr": 11.6, "def_ff": 18.3,
           "def_td": 2.8, "def_safety": 1, "def_pa": 322.2, "def_tyda": 5400.0,
           "fpts": 120.0}
    out = fp.parse_report_table(_page(fields, [row]), "dst")
    assert out.loc[0, "proj_defensiveFumbles"] == 11.6  # def_fr, not def_ff
    assert out.loc[0, "proj_defensiveYardsAllowed"] == 5400.0
    assert out.loc[0, "player_name"] == "Houston Texans"


def test_get_fp_names_a_defence_and_its_team_off_the_club_name(monkeypatch):
    """A D/ST row's `team` is blank; `get_fp` maps the club name, as it always has."""
    fields = ["def_sack", "def_int", "def_fr", "def_td", "def_safety", "def_pa",
              "def_tyda", "fpts"]
    dst = {"player": {"name": "Houston Texans", "team": ""}, "def_sack": 2.9,
           "def_int": 1.1, "def_fr": 0.6, "def_td": 0.2, "def_safety": 0,
           "def_pa": 19.2, "def_tyda": 311.7, "fpts": 7.4}
    pages = {"dst": _page(fields, [dst]), "qb": _page(_QB_FIELDS, [_qb_row()])}
    empty = {"rb": ["rush_att", "rush_yds", "rush_tds", "rec_att", "rec_yds",
                    "rec_tds", "fumbles", "fpts"],
             "wr": ["rec_att", "rec_yds", "rec_tds", "rush_att", "rush_yds",
                    "rush_tds", "fumbles", "fpts"],
             "te": ["rec_att", "rec_yds", "rec_tds", "fumbles", "fpts"],
             "k": ["fg", "fga", "xpt", "fpts"]}
    pages.update({pos: _page(f, []) for pos, f in empty.items()})

    class _Resp:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    monkeypatch.setattr(fp.requests, "get", lambda url, **k: _Resp(
        pages[url.split("/projections/")[1].split(".php")[0]]))
    monkeypatch.setattr(fp.time, "sleep", lambda s: None)
    monkeypatch.setattr(fp, "_session_cookie", lambda: "cookie")

    out = fp.get_fp(wk=7)
    row = out[out["player_name"] == "Texans D/ST"].iloc[0]
    assert row["playerTeam"] == "HOU" and row["proj_defensiveSacks"] == 2.9
    allen = out[out["player_name"] == "Josh Allen"].iloc[0]
    assert allen["playerTeam"] == "BUF" and allen["proj_receivingYards"] == 0


def test_a_renamed_field_fails_loudly_rather_than_publishing_zeros():
    fields = ["fumbles_lost" if f == "fumbles" else f for f in _QB_FIELDS]
    with pytest.raises(ValueError, match="fumbles"):
        fp.parse_report_table(_page(fields, [_qb_row()]), "qb")


def test_a_page_without_the_config_is_a_failure_not_an_empty_week():
    with pytest.raises(ValueError, match="FP.reportConfig"):
        fp.parse_report_table("<html><table id='data'></table></html>", "qb")


def test_semicolons_inside_the_json_do_not_end_it():
    """Decoded from the opening brace, not cut at the first `};`."""
    row = _qb_row()
    row["player"]["url"] = "/nfl/projections/josh-allen.php?a=1;b=2};"
    out = fp.parse_report_table(_page(_QB_FIELDS, [row]), "qb")
    assert out.loc[0, "proj_passingYards"] == 3889


def test_every_scraped_position_has_a_field_map():
    assert set(fp.FIELD_COLUMNS) >= set(fp.pos_list)
