"""The Athletic weekly fetch: finding the article, picking the link, and when to land.

The browser hop is not exercised here -- it needs a window and the live site, and that
is what G0 in ``docs/plans/53-athletic-weekly-fetch.md`` is for. Everything either side
of it is pure and tested on the real 2026 slugs.
"""

import datetime

import pytest

import Scripts.fetch_athletic as fa

BASE = "https://www.nytimes.com/athletic"

#: The weekly rankings article for weeks 1-7 of 2026, as the sitemaps list them.
WEEKLY = {
    1: f"{BASE}/7557051/2026/09/13/fantasy-football-rankings-week-1-sleepers-starts-projections/",
    2: f"{BASE}/7598426/2026/09/20/fantasy-football-week-2-rankings-start-sit-drake-london-romeo-doubs/",
    3: f"{BASE}/7621277/2026/09/27/nfl-week-3-fantasy-football-rankings-starts-projections-drake-maye-chuba-hubbard-and-more/",
    4: f"{BASE}/7643618/2026/10/04/nfl-week-4-fantasy-football-rankings-starts-projections-braelon-allen-darren-waller-and-more/",
    5: f"{BASE}/7666317/2026/10/11/nfl-week-5-fantasy-football-rankings-starts-projections-will-shipley-mike-gesicki-and-more/",
    6: f"{BASE}/7687544/2026/10/18/nfl-week-6-fantasy-football-rankings-starts-projections-marshawn-lloyd-marvin-harrison/",
    7: f"{BASE}/7708425/2026/10/21/fantasy-football-week-7-rankings-michael-wilson-colston-loveland/",
}

#: Neighbours that carry some or all of the words and must not match.
DECOYS = [
    f"{BASE}/7578125/2026/09/09/fantasy-football-schedule-strength-matchup-rankings-week-1/",
    f"{BASE}/7710208/2026/10/21/fantasy-football-team-matchup-rankings-week-7-strength-of-schedule/",
    f"{BASE}/7538205/2026/10/21/fantasy-football-start-sit-advice-lineup-tips-player-rankings/",
    f"{BASE}/7705508/2026/10/20/fantasy-football-waiver-wire-week-7-rookie-wide-receivers/",
    f"{BASE}/7458787/2026/09/04/fantasy-football-rankings-2026-top-300/",
    f"{BASE}/7595636/2026/09/15/fantasy-hockey-faces-new-places-rankings/",
]

HREF = ("https://static01.nyt.com/athletic/uploads/wp/2026/10/21103756/"
        "Week7_Proj_1021.xlsx")


def sitemap(*urls):
    """A sitemap document holding ``urls``."""
    body = "".join(f"<url><loc>{u}</loc><lastmod>2026-10-21</lastmod></url>"
                   for u in urls)
    return f'<?xml version="1.0"?><urlset>{body}</urlset>'


# --- finding the article ---------------------------------------------------

@pytest.mark.parametrize("week", sorted(WEEKLY))
def test_each_week_finds_exactly_its_own_article(week):
    docs = [sitemap(*WEEKLY.values(), *DECOYS)]
    assert fa.find_article(docs, week) == WEEKLY[week]


def test_week_one_does_not_match_week_ten():
    week_10 = f"{BASE}/1/2026/11/15/fantasy-football-week-10-rankings-x/"
    assert not fa.slug_matches(week_10, 1)
    assert fa.slug_matches(week_10, 10)


def test_not_published_is_none_not_an_error():
    assert fa.find_article([sitemap(*DECOYS)], 8) is None


def test_two_matches_refuse_to_guess():
    other = f"{BASE}/9/2026/10/22/fantasy-football-week-7-rankings-update/"
    with pytest.raises(fa.FetchError, match="refusing to guess"):
        fa.find_article([sitemap(WEEKLY[7]), sitemap(other)], 7)


def test_the_same_article_in_both_months_is_one_match():
    assert fa.find_article([sitemap(WEEKLY[4]), sitemap(WEEKLY[4])], 4) == WEEKLY[4]


def test_sitemap_months_reads_last_month_too():
    assert fa.sitemap_months(datetime.date(2026, 10, 1)) == [(2026, 10), (2026, 9)]
    assert fa.sitemap_months(datetime.date(2027, 1, 3)) == [(2027, 1), (2026, 12)]


# --- picking the link ------------------------------------------------------

def test_picks_the_download_link():
    links = [("Subscribe", "https://www.nytimes.com/subscription"),
             ("Projections Download Link", HREF),
             ("MY RANKS", f"{BASE}/rankings/")]
    assert fa.pick_download_link(links) == HREF


@pytest.mark.parametrize("links", [
    [],
    [("Projections Download Link", "https://example.com/Week7.xlsx")],
    [("Projections Download Link", "https://static01.nyt.com/x/Week7.pdf")],
    [("Download", HREF)],
    [("Projections Download Link", HREF),
     ("Projections Download Link", HREF.replace("21103756", "24090000"))],
])
def test_anything_but_one_qualifying_link_fails(links):
    with pytest.raises(fa.FetchError):
        fa.pick_download_link(links)


def test_a_misdirected_label_names_its_target():
    links = [("Projections Download Link", "https://www.nytimes.com/athletic/")]
    with pytest.raises(fa.FetchError, match="points elsewhere.*nytimes.com/athletic/"):
        fa.pick_download_link(links)


def test_a_missing_label_says_nothing_about_elsewhere():
    with pytest.raises(fa.FetchError) as exc:
        fa.pick_download_link([("Subscribe", "https://www.nytimes.com/subscription")])
    assert "elsewhere" not in str(exc.value)


@pytest.mark.parametrize("href", [
    "https://static01.nyt.com/a/..%2F..%2Fetc.xlsx",
    "https://static01.nyt.com/a/Week 7.xlsx",
    "https://static01.nyt.com/a/Week7.xlsm",
])
def test_unexpected_filenames_are_refused(href):
    with pytest.raises(fa.FetchError):
        fa.filename_from_href(href)


# --- when to land a file ---------------------------------------------------

@pytest.fixture
def wired(monkeypatch, tmp_path):
    """``run`` with every network hop replaced and the landing folder in ``tmp_path``.

    ``build_weekly`` is wired to fail: since plan 55 the fetch never parses, and a test
    that let it would not notice the coupling coming back.
    """
    calls = {"pushes": [], "href": HREF, "bytes": fa.XLSX_MAGIC + b"v1"}
    monkeypatch.setattr(fa, "fetch_sitemaps",
                        lambda today, session: [sitemap(*WEEKLY.values(), *DECOYS)])
    monkeypatch.setattr(fa, "read_download_link", lambda article: calls["href"])

    def fake_download(href, dest, session):
        path = dest / fa.filename_from_href(href)
        path.write_bytes(calls["bytes"])
        return path

    landing = tmp_path / "Landing"
    landing.mkdir()
    monkeypatch.setattr(fa, "download", fake_download)
    monkeypatch.setattr(fa, "landing_dir",
                        lambda source, season, name: landing / name)
    import Scripts.load_athletic as la

    def never_parse(*a, **k):
        raise AssertionError("the fetch parsed the slate")
    monkeypatch.setattr(la, "build_weekly", never_parse)
    monkeypatch.setattr(fa, "push_archive", lambda season: calls["pushes"].append(season))
    calls["status"] = tmp_path / "fetch_status.json"
    calls["landed"] = lambda: sorted(p.name for p in landing.iterdir())
    return calls


def _run(wired, week=7, **kw):
    return fa.run(2026, week, today=datetime.date(2026, 10, 21),
                  status_path=wired["status"], **kw)


def test_first_run_lands_the_raw_file_and_records_it(wired):
    assert _run(wired) == "fetched"
    digest = fa.hashlib.sha256(wired["bytes"]).hexdigest()
    name = f"Week7_Proj_1021_{digest[:8]}.xlsx"
    assert wired["landed"]() == [name]
    week = fa.load_status(wired["status"])["weeks"]["2026-07"]
    assert week["href"] == HREF and week["file"] == name and week["sha256"] == digest


def test_same_bytes_land_nothing(wired):
    _run(wired)
    assert _run(wired) == "unchanged"
    assert len(wired["landed"]()) == 1


def test_new_bytes_at_the_same_link_are_landed(wired):
    """The gap plan 55 closes: a file replaced in place. Comparing links skipped it."""
    _run(wired)
    wired["bytes"] = fa.XLSX_MAGIC + b"v2"
    assert _run(wired) == "fetched"
    assert len(wired["landed"]()) == 2, "the earlier raw file must not be overwritten"


def test_a_reupload_is_landed_beside_the_first(wired):
    _run(wired)
    wired["href"] = HREF.replace("21103756", "25091500")
    wired["bytes"] = fa.XLSX_MAGIC + b"v2"
    assert _run(wired) == "fetched"
    assert len(wired["landed"]()) == 2


def test_new_href_same_bytes_is_not_landed_again(wired):
    _run(wired)
    wired["href"] = HREF.replace("21103756", "25091500")
    assert _run(wired) == "unchanged"
    assert len(wired["landed"]()) == 1


def test_a_week_recorded_before_the_split_is_compared_by_its_hash(wired):
    """The mini's status file has week 7 as the old code wrote it: ``imported_at``,
    and a ``file`` with no hash in its name. The same bytes must still read unchanged."""
    digest = fa.hashlib.sha256(wired["bytes"]).hexdigest()
    fa.save_status({"weeks": {"2026-07": {
        "imported_at": "2026-10-21T15:47:13+00:00", "href": HREF,
        "file": "Week7_Proj_1021.xlsx", "sha256": digest}}}, wired["status"])
    assert _run(wired) == "unchanged"
    assert wired["landed"]() == []


def test_not_published_records_and_lands_nothing(wired):
    assert _run(wired, week=8) == "not_published"
    assert wired["landed"]() == []
    assert fa.load_status(wired["status"])["last_run"]["outcome"] == "not_published"


def test_dry_run_writes_nothing(wired):
    assert _run(wired, dry_run=True) == "dry_run"
    assert wired["landed"]() == [] and not wired["status"].exists()


def test_push_only_when_asked_and_only_after_a_new_file(wired):
    _run(wired)
    assert wired["pushes"] == []
    _run(wired, push=True)                      # unchanged: nothing to push
    assert wired["pushes"] == []
    wired["bytes"] = fa.XLSX_MAGIC + b"v2"
    _run(wired, push=True)
    assert wired["pushes"] == [2026]


def test_a_failure_is_recorded_and_raised(wired, monkeypatch):
    def blocked(article):
        raise fa.FetchError("article returned 403 ('nytimes.com')")
    monkeypatch.setattr(fa, "read_download_link", blocked)
    with pytest.raises(fa.FetchError):
        _run(wired)
    last = fa.load_status(wired["status"])["last_run"]
    assert last["outcome"] == "failed" and "403" in last["error"]
    assert "2026-07" not in fa.load_status(wired["status"])["weeks"]


# --- a link copied by hand -------------------------------------------------

def test_href_skips_the_sitemap_and_the_browser(wired, monkeypatch):
    def never(*a, **k):
        raise AssertionError("hop 1 or 2 ran")
    monkeypatch.setattr(fa, "fetch_sitemaps", never)
    monkeypatch.setattr(fa, "read_download_link", never)
    assert _run(wired, href=f"  {HREF}\n") == "fetched"
    assert len(wired["landed"]()) == 1


@pytest.mark.parametrize("href", [
    "http://static01.nyt.com/athletic/uploads/wp/2026/10/21103756/Week7_Proj_1021.xlsx",
    "https://static01.nyt.com.evil.example/Week7_Proj_1021.xlsx",
    f"{BASE}/7708425/2026/10/21/fantasy-football-week-7-rankings-michael-wilson-colston-loveland/",
])
def test_href_must_be_the_cdn_xlsx(wired, href):
    with pytest.raises(fa.FetchError):
        _run(wired, href=href)
    assert wired["landed"]() == []


def test_runs_keep_a_bounded_history(wired, monkeypatch):
    monkeypatch.setattr(fa, "RUN_HISTORY", 3)
    for _ in range(5):
        _run(wired)
    runs = fa.load_status(wired["status"])["runs"]
    assert len(runs) == 3
    assert all(r.endswith("week 7 unchanged") for r in runs)
