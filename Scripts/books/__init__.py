"""One contract for what "a book" is in this repo.

Every scraper here used to be a script that happened to write a file, so each book
got its own answer to authentication, retries, timeouts, storage, staleness and
market coverage -- and two of them answered "at import time". This package is the
shared answer. See ``docs/plans/36-sportsbook-scrapes.md``.

The pieces:

* :mod:`Scripts.books.schema` -- the standard row. One row per posted price, which is
  what lets props and game lines be the same artifact rather than two.
* :mod:`Scripts.books.base` -- :class:`~Scripts.books.base.BaseSportsbook`, plus the
  HTTP fetch every adapter shares: explicit timeouts, bounded retries, and a failure
  *classification* so a geo-block does not read as a transport error.
* :mod:`Scripts.books.pinnacle` -- the first adapter.

The rule that matters most is not in any of them: every price must reach
:mod:`Scripts.market` for de-vig and threshold-to-expectation. That module is this
repo's single derivation of both, and a new book routing around it is precisely the
defect plan 35 was created to fix -- three different juice coefficients in three
files. :func:`Scripts.books.schema.add_fair_probability` makes it the default.
"""

from Scripts.books.base import BaseSportsbook, BookFetchError, FetchFailure
from Scripts.books.schema import ODDS_SCHEMA, EXCHANGE, BOOK, add_fair_probability

__all__ = [
    "BaseSportsbook",
    "BookFetchError",
    "FetchFailure",
    "ODDS_SCHEMA",
    "EXCHANGE",
    "BOOK",
    "add_fair_probability",
]
