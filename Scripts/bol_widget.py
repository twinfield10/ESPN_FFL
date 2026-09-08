"""Playwright transport for BetOnline's weekly player props.

BetOnline's player props are served by Digital Sports Tech (DST) at
``bv2-us.digitalsportstech.com``. Every ``/api/dfm/*`` route is signed: the widget's
own HTTP client mints ``X-Req-Challenge`` / ``X-Req-Nonce`` / ``X-Req-Time`` per
request. Measured 2026-09-08:

* ``requests.get`` on any data route -> ``403 invalid_security_headers``
* a raw ``fetch()`` from inside the widget page -> ``403`` (the signing lives in the
  app's HTTP client, not a global patch)
* replaying a captured header set against another URL -> ``403 replay_detected``
  (the nonce is single-use)

So the only honest transport is to let the widget issue its own requests and harvest
the responses. That is what this module does.

No credentials are involved. The embed URL BetOnline uses carries a ``jwtToken``, but
that token is for the bet slip: the widget serves markets identically with an expired
token or none at all, so none is sent and none is stored.

The public shape is deliberately the one :mod:`Scripts.scrape_BOL` already had::

    fetch(route, game_id, statistic) -> list | None

matching what ``requests.get(url).json()`` used to return, so the parsing in that
module is unchanged. Because the browser flow is category-then-game rather than
random access, :meth:`DSTWidget.harvest` walks the UI once and fills a cache, and
:func:`make_fetcher` serves reads out of it.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Sequence, Tuple

LOGGER = logging.getLogger(__name__)

#: The widget BetOnline embeds. ``sb`` selects the sportsbook's price feed.
WIDGET_URL = "https://troya.xyz/betbuilder?sb=betonline"

#: DST's API host, as seen from inside the widget.
API_HOST = "bv2-us.digitalsportstech.com"

#: Mirrors ``Scripts.books.base``: a browser is slower than a socket, not infinitely so.
#:
#: ``SETTLE_MS`` is the only one worth explaining. Clicks are confirmed by waiting on the
#: response they trigger, not by sleeping, so this is just the beat the Angular app needs
#: to re-render between selections -- the whole walk is ~350 clicks, and every extra
#: 100ms here is another 35 seconds on the nightly.
NAV_TIMEOUT_MS = 60_000
RESPONSE_TIMEOUT_MS = 20_000
SETTLE_MS = 400


class BetOnlineWidgetError(RuntimeError):
    """The widget did not behave the way this module expects.

    Raised rather than returning empty so a structural change at DST fails loudly.
    A silent empty scrape is the failure mode this repo has already been bitten by.
    """


@dataclass(frozen=True)
class StatSpec:
    """One prop market: how the scraper names it, and how the widget exposes it.

    :param espn_stat: the key :mod:`Scripts.scrape_BOL` uses downstream.
    :param category: the top-level tab in ``.main-markets__list`` the stat sits under.
    :param label: the stat's row text in ``.main-stat__header``. Matched
        case-insensitively -- the widget writes most of these lower-case
        ("Over/Under (passing yards)") but not all of them ("Tackles + Assists"),
        and that inconsistency is not worth depending on.
    :param route: ``"Ou"`` (two-way over/under) or ``"Ss"`` (the ladder markets).
    :param statistic: the ``statistic=`` query value DST expects. Note the ``%2520``
        double-encoding on multi-word names -- that is what the widget itself sends,
        and it was never the reason the old scraper broke.
    """

    espn_stat: str
    category: str
    label: str
    route: str
    statistic: str

    @property
    def games_route(self) -> str:
        return f"gamesBy{self.route}"

    @property
    def markets_route(self) -> str:
        return f"marketsBy{self.route}"


#: The markets ``Scripts.scrape_BOL`` reads, with the UI row each sits behind.
#:
#: ``defensiveInterceptions`` is deliberately absent, though the old stat map asked for
#: it. DST posts no such market: the Defense category offers exactly "Tackles + Assists"
#: and "Sacks" (checked 2026-09-08), and "Pass Interceptions" is the quarterback's stat,
#: already covered by ``passingInterceptions``. Requesting it would log a miss every
#: night, and a check that is red every night is one nobody reads.
_OU = "Over/Under"

#: The ladder markets -- ``marketsBySs``, which :mod:`Scripts.scrape_BOL` tags
#: ``prop_source="Values"``. These live under the position categories.
LADDER_STATS: Tuple[StatSpec, ...] = (
    StatSpec("anytimeTouchdown",       "Touchdowns", "Touchdowns",         "Ss", "Touchdowns"),
    StatSpec("passingYards",           "Passing",   "Passing Yards",       "Ss", "Passing%2520Yards"),
    StatSpec("passingCompletions",     "Passing",   "Pass Completions",    "Ss", "Pass%2520Completions"),
    StatSpec("passingTouchdowns",      "Passing",   "Passing TDs",         "Ss", "Passing%2520TDs"),
    StatSpec("passingAttempts",        "Passing",   "Pass Attempts",       "Ss", "Pass%2520Attempts"),
    StatSpec("passingInterceptions",   "Passing",   "Pass Interceptions",  "Ss", "Pass%2520Interceptions"),
    StatSpec("rushingYards",           "Rushing",   "Rushing Yards",       "Ss", "Rushing%2520Yards"),
    StatSpec("rushingAttempts",        "Rushing",   "Carries",             "Ss", "Carries"),
    StatSpec("receivingYards",         "Receiving", "Receiving Yards",     "Ss", "Receiving%2520Yards"),
    StatSpec("receivingReceptions",    "Receiving", "Receptions",          "Ss", "Receptions"),
    StatSpec("defensiveTotalTackles",  "Defense",   "Tackles + Assists",   "Ss", "Tackles"),
    StatSpec("defensiveSacks",         "Defense",   "Sacks",               "Ss", "Sacks"),
)

#: The two-way markets -- ``marketsByOu``, tagged ``prop_source="OverUnder"``. All of
#: them sit under the single Over/Under category.
OU_STATS: Tuple[StatSpec, ...] = (
    StatSpec("passingYards",           _OU, "Over/Under (passing yards)",      "Ou", "Passing%2520Yards"),
    StatSpec("passingCompletions",     _OU, "Over/Under (pass completions)",   "Ou", "Pass%2520Completions"),
    StatSpec("passingTouchdowns",      _OU, "Over/Under (passing TDs)",        "Ou", "Passing%2520TDs"),
    StatSpec("passingAttempts",        _OU, "Over/Under (pass attempts)",      "Ou", "Pass%2520Attempts"),
    StatSpec("passingInterceptions",   _OU, "Over/Under (pass interceptions)", "Ou", "Pass%2520Interceptions"),
    StatSpec("rushingYards",           _OU, "Over/Under (rushing yards)",      "Ou", "Rushing%2520Yards"),
    StatSpec("rushingAttempts",        _OU, "Over/Under (carries)",            "Ou", "Carries"),
    StatSpec("receivingYards",         _OU, "Over/Under (receiving yards)",    "Ou", "Receiving%2520Yards"),
    StatSpec("receivingReceptions",    _OU, "Over/Under (receptions)",         "Ou", "Receptions"),
    StatSpec("defensiveTotalTackles",  _OU, "Over/Under (Tackles + Assists)",  "Ou", "Tackles"),
    StatSpec("defensiveSacks",         _OU, "Over/Under (sacks)",              "Ou", "Sacks"),
)

#: Everything worth harvesting. Both routes matter and are not interchangeable:
#: ``get_x_stat`` splits on ``prop_source``, taking the two-way pair for a de-vigged
#: line and the ladder for the distribution around it, and ``Scripts/lab/market.py``
#: reads both. Harvesting a superset is deliberate -- the caller decides what to use.
STATS: Tuple[StatSpec, ...] = LADDER_STATS + OU_STATS

#: The widget's two-level market nav.
#:
#: ``.main-markets__item`` is the category strip (Over/Under, Touchdowns, Passing,
#: Rushing, Receiving, Defense, ...); selecting one populates ``.main-stat__header``
#: with that category's individual markets. Scoping to these two classes matters:
#: an unscoped text search also matches player rows further down the page, which is
#: how an early version ended up requesting Passing Yards while Receiving Yards was
#: selected.
LEAGUE_SELECTOR = ".ligues-slider__item"
CATEGORY_SELECTOR = ".main-markets__item"
STAT_SELECTOR = ".main-stat__header"

#: Match on normalised, case-folded text and click the interactive node.
#:
#: Case-folding is deliberate -- see :class:`StatSpec.label`. Equality is tried before
#: prefix so "Over/Under" cannot swallow "Over/Under (passing yards)".
_JS_CLICK_IN = """
([selector, text]) => {
  const norm = s => (s || '').replace(/\\s+/g, ' ').trim().toLowerCase();
  const want = norm(text);
  const nodes = [...document.querySelectorAll(selector)];
  const hit = nodes.find(e => norm(e.textContent) === want)
           || nodes.find(e => norm(e.textContent).startsWith(want));
  if (!hit) return 'not-found';
  hit.scrollIntoView({block: 'center'});
  let target = hit;
  if (getComputedStyle(hit).cursor !== 'pointer') {
    target = [hit, ...hit.querySelectorAll('*')]
               .find(e => getComputedStyle(e).cursor === 'pointer') || hit;
  }
  target.click();
  return 'clicked';
}
"""

#: The markets offered by the currently selected category.
_JS_STAT_ROWS = """
() => [...document.querySelectorAll('.main-stat__header')]
        .map(e => (e.innerText || '').replace(/\\s+/g, ' ').trim())
"""

#: The game tiles listed once a market is selected: one per game, "Away @ Home".
_JS_GAME_TILES = """
() => [...document.querySelectorAll('div.tiered-block__item__top')]
        .map(e => (e.innerText || '').replace(/\\s+/g, ' ').trim())
        .filter(t => t.includes('@'))
"""

_JS_CLICK_TILE = """
(idx) => {
  const tiles = [...document.querySelectorAll('div.tiered-block__item__top')]
                  .filter(e => (e.innerText || '').includes('@'));
  if (idx >= tiles.length) return 'out-of-range';
  tiles[idx].scrollIntoView({block: 'center'});
  tiles[idx].click();
  return 'clicked';
}
"""


class DSTWidget:
    """A live widget session. Use as a context manager.

    >>> with DSTWidget() as w:                        # doctest: +SKIP
    ...     cache = w.harvest(STATS)
    """

    def __init__(self, league: str = "NFL", headless: bool = True,
                 widget_url: str = WIDGET_URL) -> None:
        self.league = league
        self.headless = headless
        self.widget_url = widget_url
        self._cache: Dict[Tuple[str, int, str], list] = {}
        self._pw = self._browser = self._ctx = self._page = None

    # -- lifecycle ---------------------------------------------------------
    def __enter__(self) -> "DSTWidget":
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:  # pragma: no cover - install-time failure
            raise BetOnlineWidgetError(
                "playwright is required for the BetOnline weekly props scrape. "
                "pip install playwright && playwright install chromium"
            ) from exc

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=self.headless)
        self._ctx = self._browser.new_context(
            viewport={"width": 1600, "height": 1400},
            locale="en-US",
            timezone_id="America/New_York",
        )
        self._page = self._ctx.new_page()
        self._page.on("response", self._on_response)
        self._page.goto(self.widget_url, wait_until="domcontentloaded",
                        timeout=NAV_TIMEOUT_MS)
        self._wait_for_widget()
        if not self._click_in(LEAGUE_SELECTOR, self.league):
            raise BetOnlineWidgetError(
                f"no {self.league!r} chip in the widget's league slider"
            )
        self._page.wait_for_timeout(2_000)
        return self

    def __exit__(self, *exc) -> None:
        for closer in (self._browser, self._pw):
            try:
                if closer is self._pw:
                    closer.stop()
                else:
                    closer.close()
            except Exception:  # pragma: no cover - teardown is best effort
                pass

    # -- internals ---------------------------------------------------------
    def _on_response(self, response) -> None:
        """Cache every ``/api/dfm/markets*`` body the widget fetches."""
        url = response.url
        if API_HOST not in url or "/api/dfm/markets" not in url:
            return
        try:
            payload = response.json()
        except Exception:
            return
        route = url.split("/api/dfm/")[1].split("?")[0]
        try:
            game_id = int(url.split("gameId=")[1].split("&")[0])
        except (IndexError, ValueError):
            return
        statistic = url.split("statistic=")[1].split("&")[0] if "statistic=" in url else ""
        self._cache[(route, game_id, statistic)] = payload

    def _wait_for_widget(self) -> None:
        """Block until the market list renders, not merely until the DOM parses."""
        deadline = time.time() + NAV_TIMEOUT_MS / 1000
        while time.time() < deadline:
            try:
                if self._page.evaluate(
                        "() => document.querySelectorAll('[class*=tiered-block]').length > 0"):
                    return
            except Exception:
                pass
            self._page.wait_for_timeout(500)
        raise BetOnlineWidgetError(
            f"{self.widget_url} never rendered a market list. The widget may have been "
            "restructured, or headless Chromium is being turned away."
        )

    def _click_in(self, selector: str, text: str) -> bool:
        """Click the node in ``selector`` whose text matches ``text``. False if absent."""
        result = self._page.evaluate(_JS_CLICK_IN, [selector, text])
        self._page.wait_for_timeout(SETTLE_MS)
        return result == "clicked"

    def _select_market(self, spec: StatSpec) -> bool:
        """Select a market: its category tab first, then the market row if there is a choice.

        A category holding a single market (Touchdowns, First TD) is *already* selected
        by the category click, and clicking its row afterwards toggles the selection
        back off -- which reads downstream as "this market listed no games". So the row
        is only clicked where the category offers more than one.
        """
        if not self._click_in(CATEGORY_SELECTOR, spec.category):
            LOGGER.warning("BetOnline: no %r category tab", spec.category)
            return False
        self._page.wait_for_timeout(900)
        if len(self._page.evaluate(_JS_STAT_ROWS)) > 1:
            if not self._click_in(STAT_SELECTOR, spec.label):
                return False
            self._page.wait_for_timeout(1_500)
        return True

    # -- the walk ----------------------------------------------------------
    def harvest(self, stats: Sequence[StatSpec] = STATS) -> Dict[Tuple[str, int, str], list]:
        """Walk every market and every game, filling the response cache.

        Returns the cache: ``{(markets_route, game_id, statistic): payload}``.
        """
        for spec in stats:
            if not self._select_market(spec):
                LOGGER.warning("BetOnline: no market tile for %r (%s) -- skipped",
                               spec.label, spec.espn_stat)
                continue
            tiles = self._page.evaluate(_JS_GAME_TILES)
            if not tiles:
                LOGGER.warning("BetOnline: %s listed no games", spec.espn_stat)
                continue
            got = 0
            for idx in range(len(tiles)):
                before = len(self._cache)
                try:
                    with self._page.expect_response(
                            lambda r, s=spec: ("/api/dfm/" + s.markets_route) in r.url,
                            timeout=RESPONSE_TIMEOUT_MS):
                        self._page.evaluate(_JS_CLICK_TILE, idx)
                except Exception:
                    LOGGER.debug("BetOnline: %s tile %d fired no market call",
                                 spec.espn_stat, idx)
                    continue
                self._page.wait_for_timeout(SETTLE_MS)
                got += len(self._cache) > before
            LOGGER.info("BetOnline: %-22s %2d/%2d games", spec.espn_stat, got, len(tiles))
        return dict(self._cache)


def make_fetcher(cache: Dict[Tuple[str, int, str], list]) -> Callable[[str, int, str], Optional[list]]:
    """Adapt a harvested cache to the call shape :mod:`Scripts.scrape_BOL` expects."""

    def fetch(route: str, game_id: int, statistic: str) -> Optional[list]:
        return cache.get((route, int(game_id), statistic))

    return fetch


def harvest_week(stats: Sequence[StatSpec] = STATS, league: str = "NFL",
                 headless: bool = True) -> Dict[Tuple[str, int, str], list]:
    """One-shot convenience: open the widget, walk it, hand back the cache."""
    with DSTWidget(league=league, headless=headless) as widget:
        return widget.harvest(stats)
