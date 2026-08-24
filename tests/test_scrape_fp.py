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


def test_the_scraper_only_reads_a_robots_allowed_path():
    """`/api/`, `/json/`, `/ajax/` and `/xml/` are Disallow-ed. `/nfl/projections/`
    is not, and is the only path this module may touch -- the same call this repo
    made for BetOnline's weekly endpoint and Pro-Football-Reference."""
    import inspect

    # Only URLs the module actually *requests* -- prose mentioning robots.txt is not a
    # fetch, so scan assignment lines rather than the whole source.
    src = inspect.getsource(fp)
    fetched = [ln for ln in src.splitlines()
               if re.match(r'\s*url\s*=\s*\(?\s*f?["\']https?://', ln)]
    assert fetched, "expected a URL assignment in the module"
    for ln in fetched:
        u = re.search(r'https://www\.fantasypros\.com(/[^"\'{\s?]*)', ln)
        assert u, f"unrecognised host in: {ln.strip()}"
        assert u.group(1).startswith("/nfl/projections/"), f"non-projections path: {u.group(1)}"
        for banned in ("/api/", "/json/", "/ajax/", "/xml/"):
            assert banned not in ln


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
