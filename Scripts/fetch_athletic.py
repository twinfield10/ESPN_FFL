"""Fetch The Athletic's weekly projection slate and keep the raw file.

    python -m Scripts.fetch_athletic                 # this week: find, fetch, land
    python -m Scripts.fetch_athletic --push          # ... then push the archive tier
    python -m Scripts.fetch_athletic --dry-run       # print the article and link only
    python -m Scripts.fetch_athletic --status        # what the last run recorded
    python -m Scripts.fetch_athletic --href <link>   # skip hop 2: a link you copied

**The file is public; finding it is not.** The slate lives on
``static01.nyt.com/athletic/uploads/wp/YYYY/MM/<upload timestamp>/<name>.xlsx`` and a
plain GET returns it, but the timestamp folder cannot be guessed and nothing lists the
uploads. The one place the link appears is the bottom of that week's rankings article,
under the text ``Projections Download Link``. See ``docs/plans/53-athletic-weekly-fetch.md``.

**Three hops, and only the middle one needs a browser:**

1. The monthly post sitemap, plain HTTP, gives the article (:func:`find_article`).
2. The article, in Playwright's Chromium **with a visible window** -- it returns 403 to
   ``curl`` and to headless Chromium, and 200 with the link in the DOM to a visible
   logged-out window -- **until it doesn't**: after about six automated loads in an hour
   on 2026-10-21 the visible window got a 403 too. No subscription login is used, and
   nothing here spoofs a user agent, dresses up headless mode or retries around a 403.
   ``--href`` is the way past a blocked hop 2: copy the link address off the article
   ("Copy Link Address") and the rest runs as usual.
3. The ``.xlsx``, plain HTTP, into the landing folder, unparsed.

**Fetching and parsing are separate steps** (plan 55). This module only lands the raw
workbook, under a name carrying its content hash, and records it in
:data:`STATUS_PATH`; ``Scripts.parse_athletic`` imports it from the nightly. The
download is the hop that gets blocked and the parse is the one that can be rerun, so a
format change the parser refuses must not cost the file that shows what changed.

**New bytes, not a new link, is what counts as new.** The file is downloaded on every
run and compared by SHA-256 with the last one landed for the week. A re-upload is
usually a new URL (the folder is the upload time), but a file replaced at the same URL
would be skipped forever by a link comparison. The file is ~30 KB.

**``--push`` is opt-in on purpose.** It pushes the whole ``archive`` tier, and a copy
of ``Data/`` that is not the nightly's -- the MacBook's, say -- would send its stale
``Scoring``/``Injuries`` files over the Mac mini's. The LaunchAgent on the mini passes
it; an interactive run does not.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests

from Scripts.load_athletic import SOURCE
from Scripts.paths import PROJECTIONS_DIR, landing_dir

#: Monthly article sitemap. Public, and answers plain HTTP.
SITEMAP_URL = "https://www.nytimes.com/athletic/sitemap-pt-post-{year:04d}-{month:02d}.xml"

#: Where the uploads live. A link anywhere else is not the slate.
CDN_HOST = "static01.nyt.com"

#: The link's text on the article page. The date beside it ("(Oct. 21)") sits outside
#: the anchor, so it is not part of the match.
LINK_TEXT = re.compile(r"projections download link", re.IGNORECASE)

#: Identifies us honestly. Both plain-HTTP hops answer it.
USER_AGENT = "ESPN_FFL athletic weekly fetch (personal use)"

#: Per-week record of what was last landed. ``Scripts.parse_athletic`` reads it.
STATUS_PATH = PROJECTIONS_DIR / SOURCE / "fetch_status.json"

#: The start of every .xlsx -- it is a zip archive.
XLSX_MAGIC = b"PK\x03\x04"

#: How many run outcomes the status file keeps.
RUN_HISTORY = 60

HTTP_TIMEOUT = 30
PAGE_TIMEOUT_MS = 45_000


class FetchError(RuntimeError):
    """A hop failed in a way a human needs to look at."""


def _log(msg: str) -> None:
    """Print a progress line, flushed -- the LaunchAgent sends this to a log."""
    print(msg, flush=True)


# --- hop 1: the article ----------------------------------------------------

def slug_matches(url: str, week: int) -> bool:
    """Whether an article URL is week ``week``'s rankings-and-projections piece.

    **The slug is not a template.** Weeks 1-7 of 2026 used three spellings --
    ``fantasy-football-rankings-week-1-...``, ``nfl-week-3-fantasy-football-rankings-...``
    and ``fantasy-football-week-7-rankings-...`` -- and all three carry
    ``fantasy-football``, ``rankings`` and ``week-N``. The team and schedule matchup
    rankings carry all three too, which is what the ``matchup`` exclusion is for.

    Args:
        url: An article URL from the sitemap.
        week: NFL week.

    Returns:
        bool: True for the weekly rankings article.
    """
    slug = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1].lower()
    return ("fantasy-football" in slug
            and "rankings" in slug
            and "matchup" not in slug
            # Bounded, so week-1 does not match week-10.
            and re.search(rf"(?:^|-)week-{int(week)}(?:-|$)", slug) is not None)


def sitemap_locs(xml: str) -> List[str]:
    """Every ``<loc>`` in a sitemap document, in order."""
    return re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml)


def find_article(sitemaps: Iterable[str], week: int) -> Optional[str]:
    """The one article for ``week`` across the given sitemap documents.

    Args:
        sitemaps: Raw sitemap XML, newest month first.
        week: NFL week.

    Returns:
        str | None: The article URL, or None when it is not published yet.

    Raises:
        FetchError: If more than one article matches -- the slug rule has broken and
            picking one would be a guess.
    """
    matches = sorted({loc for xml in sitemaps for loc in sitemap_locs(xml)
                      if slug_matches(loc, week)})
    if len(matches) > 1:
        raise FetchError(f"{len(matches)} articles match week {week}; refusing to "
                         f"guess:\n  " + "\n  ".join(matches))
    return matches[0] if matches else None


def sitemap_months(today: datetime.date) -> List[Tuple[int, int]]:
    """This month and last month, newest first.

    Two, because the sitemap dates an article by its last edit and a week's piece can
    land in the next month's map -- week 4 was downloaded 10/01 and is listed 10/04.
    """
    prev = today.replace(day=1) - datetime.timedelta(days=1)
    return [(today.year, today.month), (prev.year, prev.month)]


def fetch_sitemaps(today: datetime.date, session: requests.Session) -> List[str]:
    """Download the sitemap documents :func:`sitemap_months` names.

    A month that has no map yet (404) is skipped rather than fatal: on the 1st, this
    month's map may not exist.
    """
    docs = []
    for year, month in sitemap_months(today):
        url = SITEMAP_URL.format(year=year, month=month)
        resp = session.get(url, timeout=HTTP_TIMEOUT)
        if resp.status_code == 404:
            _log(f"  sitemap {year}-{month:02d}: not published yet, skipped")
            continue
        if resp.status_code != 200:
            raise FetchError(f"sitemap {url} returned {resp.status_code}")
        docs.append(resp.text)
    return docs


# --- hop 2: the link -------------------------------------------------------

def pick_download_link(links: Sequence[Tuple[str, str]]) -> str:
    """The slate's href out of the page's ``(text, href)`` anchors.

    Both conditions, not either: the text is the author's, the host and extension
    are the CDN's, and a page with a second spreadsheet on it should fail loudly
    rather than import whichever came first.

    Raises:
        FetchError: Unless exactly one anchor qualifies.
    """
    labelled = sorted({href for text, href in links if LINK_TEXT.search(text or "")})
    hits = [href for href in labelled
            if urlparse(href).hostname == CDN_HOST
            and urlparse(href).path.lower().endswith(".xlsx")]
    if len(hits) != 1:
        # A labelled anchor pointing elsewhere is the author mid-edit, not a missing
        # link -- name its target so the two read differently in the log.
        stray = sorted(set(labelled) - set(hits))
        detail = f"; the label points elsewhere: {stray}" if stray else ""
        raise FetchError(f"expected one '{LINK_TEXT.pattern}' link to a {CDN_HOST} "
                         f".xlsx, found {len(hits)}: {hits}{detail}")
    return hits[0]


def read_download_link(article: str) -> str:
    """Open ``article`` in a visible Chromium window and return the slate's href.

    Raises:
        FetchError: On a non-200 page or anything but one qualifying link. The page
            title is in the message, so a 403 reads differently from a moved link.
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        try:
            page = browser.new_page()
            resp = page.goto(article, wait_until="domcontentloaded",
                             timeout=PAGE_TIMEOUT_MS)
            status = resp.status if resp else None
            if status != 200:
                raise FetchError(f"article returned {status} ({page.title()!r})")
            try:
                page.wait_for_selector("a:has-text('Projections Download Link')",
                                       timeout=PAGE_TIMEOUT_MS)
            except Exception:
                pass  # pick_download_link reports the absence with context
            links = page.eval_on_selector_all(
                "a", "els => els.map(e => [e.textContent || '', e.href || ''])")
            try:
                return pick_download_link([tuple(x) for x in links])
            except FetchError as exc:
                raise FetchError(f"{exc} on {page.title()!r}") from None
        finally:
            browser.close()


# --- hop 3: the file -------------------------------------------------------

def check_href(href: str) -> str:
    """A hand-supplied href, held to the rule :func:`pick_download_link` applies.

    Raises:
        FetchError: If it is not a ``static01.nyt.com`` ``.xlsx`` link.
    """
    href = href.strip()
    parsed = urlparse(href)
    if parsed.scheme != "https" or parsed.hostname != CDN_HOST \
            or not parsed.path.lower().endswith(".xlsx"):
        raise FetchError(f"{href!r} is not an https://{CDN_HOST}/... .xlsx link")
    return href


def filename_from_href(href: str) -> str:
    """The upload's own filename, checked before it touches the disk.

    Raises:
        FetchError: If it is not a plain ``*.xlsx`` name.
    """
    name = Path(urlparse(href).path).name
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.xlsx", name):
        raise FetchError(f"unexpected filename {name!r} in {href}")
    return name


def download(href: str, dest: Path, session: requests.Session) -> Path:
    """Save the slate into ``dest`` and return its path.

    Raises:
        FetchError: On a non-200, or a body that is not an .xlsx.
    """
    resp = session.get(href, timeout=HTTP_TIMEOUT)
    if resp.status_code != 200:
        raise FetchError(f"{href} returned {resp.status_code}")
    if not resp.content.startswith(XLSX_MAGIC):
        raise FetchError(f"{href} is not an .xlsx "
                         f"({resp.headers.get('content-type')}, {len(resp.content)} bytes)")
    path = dest / filename_from_href(href)
    path.write_bytes(resp.content)
    return path


def sha256(path: Path) -> str:
    """Hex SHA-256 of a file."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def landed_name(href: str, digest: str) -> str:
    """The landing filename: the upload's own name plus the first 8 hex of its hash.

    The hash is in the name because a corrected re-upload can keep the filename, and
    landing it under the old name would overwrite the raw file the last import came from.
    """
    stem = Path(filename_from_href(href)).stem
    return f"{stem}_{digest[:8]}.xlsx"


# --- status ----------------------------------------------------------------

def load_status(path: Path = STATUS_PATH) -> Dict:
    """The status file, or an empty one."""
    if not path.is_file():
        return {"weeks": {}}
    return json.loads(path.read_text())


def save_status(status: Dict, path: Path = STATUS_PATH) -> None:
    """Write the status file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n")


def record(status: Dict, season: int, week: int, outcome: str, **fields) -> Dict:
    """Note this run in ``status``.

    ``last_run`` is every run; ``weeks`` only moves when a new file lands, so each
    download is compared against the last bytes actually kept, and
    ``Scripts.parse_athletic`` reads ``weeks`` for what is waiting to be imported.
    ``runs`` keeps the last :data:`RUN_HISTORY` outcomes, one line each -- G0 asks
    whether the browser hop survives a week of daily loads, and that is a question
    about a sequence, not the latest run.
    """
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    status["last_run"] = {"at": now, "season": season, "week": week,
                          "outcome": outcome, **fields}
    line = f"{now} week {week} {outcome}"
    if fields.get("error"):
        line += f" -- {fields['error']}"
    status["runs"] = (status.get("runs", []) + [line])[-RUN_HISTORY:]
    if outcome == "fetched":
        status.setdefault("weeks", {})[f"{season}-{week:02d}"] = {
            "fetched_at": now, **fields}
    return status


# --- the run ---------------------------------------------------------------

def run(season: int, week: int, *, today: Optional[datetime.date] = None,
        dry_run: bool = False, push: bool = False, href: Optional[str] = None,
        status_path: Path = STATUS_PATH) -> str:
    """Find, fetch and land week ``week``'s slate. Nothing is parsed here.

    Args:
        season: Season year; the landing folder, and checked against the sheet by the
            parse step.
        week: NFL week; likewise.
        today: Date that picks the sitemap months. Defaults to today.
        dry_run: Stop after reading the link.
        push: After a new file lands, push the ``archive`` tier to S3 -- the raw file
            and the status file, so the parse can be rerun from anywhere.
        href: A link copied by hand. Skips the sitemap and the browser.
        status_path: Status file location.

    Returns:
        str: ``not_published``, ``unchanged``, ``dry_run`` or ``fetched``.

    Raises:
        FetchError: On any hop failing; recorded in the status file first.
    """
    today = today or datetime.date.today()
    status = load_status(status_path)
    key = f"{season}-{week:02d}"
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    def finish(outcome: str, **fields) -> str:
        if not dry_run:
            save_status(record(status, season, week, outcome, **fields), status_path)
        return outcome

    try:
        if href is not None:
            href, article = check_href(href), None
        else:
            article = find_article(fetch_sitemaps(today, session), week)
            if article is None:
                _log(f"  week {week}: no rankings article in the sitemap yet")
                return finish("not_published")
            _log(f"  article  {article}")
            href = read_download_link(article)
        _log(f"  link     {href}")
        if dry_run:
            return "dry_run"

        last = status.get("weeks", {}).get(key, {})
        with tempfile.TemporaryDirectory(prefix="athletic_") as tmp:
            path = download(href, Path(tmp), session)
            digest = sha256(path)
            if digest == last.get("sha256"):
                since = last.get("fetched_at") or last.get("imported_at")
                _log(f"  same bytes as the file landed {since}; nothing new")
                if last.get("href") != href:
                    # Re-uploaded under a new folder with the same bytes.
                    last["href"] = href
                return finish("unchanged", article=article, href=href, sha256=digest)
            name = landed_name(href, digest)
            shutil.copy2(path, landing_dir(SOURCE, season, name))
        _log(f"  landed   {name} (sha256 {digest[:12]}); the nightly parses it")

        outcome = finish("fetched", article=article, href=href, sha256=digest,
                         file=name)
    except Exception as exc:
        finish("failed", error=f"{type(exc).__name__}: {exc}")
        raise

    if push:
        push_archive(season)
    return outcome


def push_archive(season: int) -> None:
    """Push the ``archive`` tier -- the raw slate and the status file.

    Raises:
        FetchError: If any upload failed. The landed file stands either way; the
            nightly's own push is the second chance.
    """
    from Scripts import sync

    uploaded, failures = sync.push(["archive"], season, snapshot_date=None)
    _log(f"  pushed archive tier: {uploaded} object(s)")
    if failures:
        raise FetchError(f"archive push failed for {len(failures)} object(s): "
                         f"{failures[:3]}")


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point. See ``python -m Scripts.fetch_athletic --help``."""
    from Scripts.nfl_utils import current_season, current_week

    p = argparse.ArgumentParser(
        prog="python -m Scripts.fetch_athletic",
        description="Fetch The Athletic's weekly projection slate and land the raw "
                    "file; Scripts.parse_athletic imports it.")
    p.add_argument("--season", type=int, default=None,
                   help="defaults to the schedule's season")
    p.add_argument("--week", type=int, default=None,
                   help="defaults to the first week with unplayed games")
    p.add_argument("--dry-run", action="store_true",
                   help="find the article and link, download and write nothing")
    p.add_argument("--push", action="store_true",
                   help="after a new file lands, push the archive tier to S3 (the "
                        "nightly machine only)")
    p.add_argument("--href", default=None,
                   help="the slate's static01.nyt.com link, copied off the article; "
                        "skips the sitemap and the browser")
    p.add_argument("--status", action="store_true",
                   help="print what the last run recorded and exit")
    args = p.parse_args(argv)

    if args.status:
        print(json.dumps(load_status(), indent=2, sort_keys=True))
        return 0

    season = current_season() if args.season is None else args.season
    week = current_week() if args.week is None else args.week
    _log(f"===== The Athletic weekly fetch: {season} week {week} =====")
    try:
        outcome = run(season, week, dry_run=args.dry_run, push=args.push,
                      href=args.href)
    except FetchError as exc:
        _log(f"FAILED: {exc}")
        return 1
    _log(f"done: {outcome}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
