"""The contract every book adapter implements, and the fetch they all share.

Two abstract methods and one entry point, ported from
``Rebirtha/python/sportsbooks/base.py``. What that base class did *not* carry, and
what the adapters underneath it each paid for separately, is here instead: an HTTP
fetch with explicit timeouts, bounded retries, and a failure classification.

The timeout is not a nicety. From the other repo's own comment, learned the expensive
way: ``requests`` defaults to **no timeout**, and a socket left ESTAB-but-dead wedged
a run for thirty minutes holding a shared lock, silently freezing every refresh
behind it. Every request here carries an explicit ``(connect, read)`` pair.

The classification matters for the same reason absence matters everywhere in this
repo. A 403 because the book geo-blocks your country and a 403 because your address
is rate-limited want opposite responses -- one is permanent from here, the other
clears on its own -- and a 503 wants neither. Collapsing them into "the fetch failed"
is how a book goes quietly missing.
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Dict, Optional

import polars as pl
import requests

from Scripts.books.schema import BOOK, ODDS_SCHEMA, add_fair_probability

logger = logging.getLogger(__name__)

#: ``(connect, read)`` seconds. See the module docstring -- the default of None is
#: what wedged a run in the repo this is ported from.
REQUEST_TIMEOUT = (5, 20)

#: Attempts per request, including the first. Small on purpose: these are read-only
#: pulls against someone else's book, run four times a day, and a retry storm is how
#: a polite scraper becomes an impolite one.
MAX_ATTEMPTS = 3

#: Seconds before the first retry, doubling after. A 429 waits longer; see
#: :meth:`BaseSportsbook.fetch_json`.
BACKOFF_SECONDS = 2.0

#: Extra pause when the book says "too many requests" outright.
RATE_LIMIT_BACKOFF = 15.0

#: The body key Pinnacle uses to say *why* it refused, e.g. ``"location"``.
GEO_BLOCK_REASON = "location"


class FetchFailure(str, Enum):
    """Why a fetch came back empty. The distinction is the point."""

    NONE = "none"
    #: 403 naming a country restriction. Permanent from this address; rotating an IP
    #: within the same country cannot clear it, so retrying is waste.
    GEO_BLOCK = "geo_block"
    #: 403 for any other reason -- reputation, rate limit, a missing header.
    IP_BLOCK = "ip_block"
    #: 5xx. The book's problem, and usually transient.
    SERVER_ERROR = "server_error"
    #: Timeout, DNS, connection reset, unparseable body.
    TRANSPORT = "transport"
    OTHER = "other"


class BookFetchError(RuntimeError):
    """A book could not be read, carrying the classification.

    Attributes:
        failure: The :class:`FetchFailure` that ended the attempt.
        detail: Whatever the book said about it.
    """

    def __init__(self, message: str, failure: FetchFailure = FetchFailure.OTHER,
                 detail: str = ""):
        super().__init__(message)
        self.failure = failure
        self.detail = detail


class BaseSportsbook(ABC):
    """One book, for one league.

    Subclasses implement :meth:`fetch_odds` and :meth:`transform_to_standard`.
    Everything else -- the HTTP, the de-vig, the timing report -- is inherited, which
    is the difference between adding a book and writing a scraper.

    Attributes:
        sportsbook_name: Derived from the class name with ``Sportsbook`` stripped.
        book_type: ``BOOK`` or ``EXCHANGE``. See :mod:`Scripts.books.schema`.
        last_failure: How the most recent fetch failed, if it did.
    """

    #: Overridden by exchanges. Reaches the stored rows as ``bookType``.
    book_type: str = BOOK

    def __init__(self, sport: str = "football", league: str = "nfl"):
        self.sport = sport
        self.league = league
        self.sportsbook_name = type(self).__name__.replace("Sportsbook", "")
        self.start_time = time.time()
        self.last_failure = FetchFailure.NONE
        self.last_block_reason: Optional[str] = None
        self.session = requests.Session()

    # --- the contract ------------------------------------------------------

    @abstractmethod
    def fetch_odds(self) -> Dict[str, pl.DataFrame]:
        """Pull this book and return its views, already standardised.

        Returns:
            Dict[str, pl.DataFrame]: Named views. Every adapter returns the same keys
            it declares in its docstring, *including on an empty pull* -- a book with
            nothing to say must still return the shape, or every caller grows an
            absence branch.
        """

    @abstractmethod
    def transform_to_standard(self, raw: pl.DataFrame) -> pl.DataFrame:
        """Map this book's rows onto :data:`Scripts.books.schema.ODDS_SCHEMA`."""

    # --- what every adapter gets for free ----------------------------------

    def get_df_dict(self) -> Dict[str, pl.DataFrame]:
        """Main entry point: fetch, report, return.

        Returns:
            Dict[str, pl.DataFrame]: Whatever :meth:`fetch_odds` returned.
        """
        views = self.fetch_odds()
        elapsed = round(time.time() - self.start_time, 2)
        rows = sum(v.height for v in views.values())
        print(f"{self.sportsbook_name}: {rows} prices across {len(views)} views "
              f"in {elapsed}s - {datetime.now():%Y-%m-%d %H:%M:%S}")
        return views

    def fetch_json(self, url: str, headers: Optional[dict] = None,
                   method: str = "GET", body: Optional[dict] = None):
        """One request, with a timeout, bounded retries and a classified failure.

        Returns ``[]`` rather than raising, matching the adapter this is ported from:
        one dead endpoint should degrade a pull, not abort it. The *caller* decides
        whether an empty result is fatal -- and for a scheduled run it should be, or
        an absent source reads as agreement.

        Args:
            url: Absolute URL.
            headers: Request headers.
            method: ``GET`` or ``POST``.
            body: JSON body for a POST.

        Returns:
            The decoded body, or ``[]`` on any failure. :attr:`last_failure` says why.
        """
        self.last_failure = FetchFailure.NONE
        self.last_block_reason = None
        delay = BACKOFF_SECONDS

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                response = self.session.request(
                    method, url, headers=headers, json=body,
                    timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                retry = self._classify_http(exc, status, url)
                if not retry or attempt == MAX_ATTEMPTS:
                    return []
                if status == 429:
                    delay = max(delay, RATE_LIMIT_BACKOFF)

            except (requests.exceptions.RequestException, ValueError) as exc:
                # ValueError covers a 200 whose body is not JSON, which is what a
                # captive portal or an error page looks like from here.
                self.last_failure = FetchFailure.TRANSPORT
                logger.warning("%s: %s (%s)", self.sportsbook_name, exc, url)
                if attempt == MAX_ATTEMPTS:
                    return []

            time.sleep(delay)
            delay *= 2
        return []

    def _classify_http(self, exc, status, url) -> bool:
        """Record why a request failed, and say whether retrying could help.

        Returns:
            bool: True if another attempt is worth making.
        """
        if status == 403:
            reason, detail = self._parse_block(exc.response)
            self.last_block_reason = reason
            if reason == GEO_BLOCK_REASON:
                self.last_failure = FetchFailure.GEO_BLOCK
                logger.warning("%s: 403 geo-block on %s -- %s. Not retrying: every "
                               "address here is in the same country.",
                               self.sportsbook_name, url, detail)
                return False
            self.last_failure = FetchFailure.IP_BLOCK
            logger.warning("%s: 403 on %s (reason=%r)",
                           self.sportsbook_name, url, reason)
            return False
        if status and status >= 500:
            self.last_failure = FetchFailure.SERVER_ERROR
            logger.warning("%s: %s on %s", self.sportsbook_name, status, url)
            return True
        if status == 429:
            self.last_failure = FetchFailure.IP_BLOCK
            logger.warning("%s: rate limited on %s", self.sportsbook_name, url)
            return True
        self.last_failure = FetchFailure.OTHER
        logger.warning("%s: %s on %s", self.sportsbook_name, exc, url)
        return False

    @staticmethod
    def _parse_block(response):
        """Pull ``(reason, detail)`` out of a 403 body of any shape."""
        try:
            body = response.json() if response is not None else {}
        except (ValueError, AttributeError, json.JSONDecodeError):
            return None, ""
        if not isinstance(body, dict):
            return None, ""
        return body.get("reason"), body.get("detail", "")

    def standardise(self, df: pl.DataFrame) -> pl.DataFrame:
        """Stamp the book's identity, de-vig, and conform to the schema.

        The de-vig is not optional and not the adapter's choice. See
        :func:`Scripts.books.schema.add_fair_probability`.

        Args:
            df: Rows already carrying the schema's market columns.

        Returns:
            pl.DataFrame: Exactly :data:`ODDS_SCHEMA`'s columns, in order.
        """
        if df.is_empty():
            return pl.DataFrame(schema=ODDS_SCHEMA)

        df = df.with_columns([
            pl.lit(self.sportsbook_name).alias("sportsbook"),
            pl.lit(self.book_type).alias("bookType"),
            pl.lit(datetime.now().isoformat(timespec="seconds")).alias("snapshot_ts"),
        ])
        df = add_fair_probability(df)

        for name, dtype in ODDS_SCHEMA.items():
            if name not in df.columns:
                df = df.with_columns(pl.lit(None, dtype=dtype).alias(name))
        return df.select(list(ODDS_SCHEMA)).cast(dict(ODDS_SCHEMA))
