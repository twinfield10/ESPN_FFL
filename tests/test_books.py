"""The book adapter contract: the standard row, the de-vig, and the fetch.

Two of these tests exist because the de-vig came out wrong on live data, not because
anyone reasoned their way to them in advance. Both failures were silent -- a plausible
number in the right range, on the right row -- which is the argument for pinning them:

* a *team* total posts four prices per game, two per team, and when both teams are
  priced at the same number the four are indistinguishable without knowing whose side
  each is. That is any pick-em, and it was true of the first game looked at.
* a *spread* posts one market carrying opposite numbers on its two sides. Pairing on
  each side's own number matched the away half of the -1.0 line against the home half
  of the +1.0 line -- two different markets, quietly de-vigged against each other.

Everything here runs offline. The live pull is marked and deselected by default.
"""

import polars as pl
import pytest

from Scripts.books import base, schema
from Scripts.books.pinnacle import PinnacleSportsbook


# --- the standard row -----------------------------------------------------

def test_an_empty_pull_still_has_the_shape():
    """A book with nothing to say must return odds-shaped nothing, or every caller
    downstream grows its own absence branch."""
    empty = schema.empty_frame()
    assert empty.height == 0
    assert list(empty.columns) == list(schema.ODDS_SCHEMA)


def test_american_prices_convert_to_probability():
    assert schema.american_to_probability(-110) == pytest.approx(0.5238, abs=1e-4)
    assert schema.american_to_probability(100) == pytest.approx(0.5)
    assert schema.american_to_probability(128) == pytest.approx(0.4386, abs=1e-4)


def test_the_pairing_key_is_the_line_without_the_side():
    """One market, two sides. Anything else in the grain splits a pair."""
    assert set(schema.PAIR_KEYS) == set(schema.LINE_KEYS) - {"betSide"}
    assert "marketLine" in schema.PAIR_KEYS
    assert "sideOf" in schema.PAIR_KEYS


# --- the de-vig -----------------------------------------------------------

def _row(**kw):
    base_row = dict(sportsbook="Pinnacle", officialDate="2026-09-13", matchup="A vs. B",
                    marketTitle="Total", gamePeriod="GAME", sideOf=None, betSide="over",
                    marketLine=44.0, propType=None, impProb=0.5)
    base_row.update(kw)
    return base_row


def test_a_two_sided_market_devigs_to_one():
    """The hold is the surplus over 1.0. Removing it proportionally is
    Scripts.market.devig_two_way, and routing through it is the point -- this repo
    derives de-vig in exactly one place."""
    df = pl.DataFrame([
        _row(betSide="over", impProb=0.534884),
        _row(betSide="under", impProb=0.502488),
    ])
    out = schema.add_fair_probability(df)
    assert out["fairProb"].sum() == pytest.approx(1.0, abs=1e-9)
    # Proportional: the richer side stays the richer side.
    assert out["fairProb"][0] > out["fairProb"][1]


def test_two_teams_priced_at_the_same_total_stay_two_markets():
    """The pick-em collision. Four rows, one number, two markets."""
    rows = []
    for team, over, under in (("home", 0.52381, 0.514563),
                              ("away", 0.521531, 0.516908)):
        rows.append(_row(marketTitle="TeamTotal", sideOf=team, betSide="over",
                         marketLine=21.5, impProb=over))
        rows.append(_row(marketTitle="TeamTotal", sideOf=team, betSide="under",
                         marketLine=21.5, impProb=under))
    out = schema.add_fair_probability(pl.DataFrame(rows))

    per_team = out.group_by("sideOf").agg(pl.col("fairProb").sum().alias("s"))
    assert per_team["s"].to_list() == pytest.approx([1.0, 1.0], abs=1e-9)


def test_a_spread_pairs_by_the_market_not_by_each_sides_number():
    """Both halves of a two-line alt ladder. Pairing on the side's own number matched
    away(-1.0) with home(-1.0) -- rows from different markets."""
    rows = [
        _row(marketTitle="Spread", marketLine=1.0, betSide="home",
             value=1.0, impProb=0.530516),
        _row(marketTitle="Spread", marketLine=1.0, betSide="away",
             value=-1.0, impProb=0.497512),
        _row(marketTitle="Spread", marketLine=-1.0, betSide="home",
             value=-1.0, impProb=0.470),
        _row(marketTitle="Spread", marketLine=-1.0, betSide="away",
             value=1.0, impProb=0.560),
    ]
    out = schema.add_fair_probability(pl.DataFrame(rows))
    per_market = (out.group_by("marketLine")
                     .agg(pl.col("fairProb").sum().alias("s")).sort("marketLine"))
    assert per_market["s"].to_list() == pytest.approx([1.0, 1.0], abs=1e-9)


def test_a_one_sided_market_keeps_its_vig_and_says_so():
    """Nothing to de-vig against. Leaving impProb is honest, and the two columns
    being equal is the signal that it happened."""
    out = schema.add_fair_probability(pl.DataFrame([_row(impProb=0.55)]))
    assert out["fairProb"][0] == pytest.approx(0.55)


# --- the fetch ------------------------------------------------------------

def test_requests_never_go_out_without_a_timeout():
    """requests defaults to no timeout. A dead socket then wedged a run for thirty
    minutes holding a shared lock, in the repo this was ported from."""
    assert base.REQUEST_TIMEOUT is not None
    connect, read = base.REQUEST_TIMEOUT
    assert connect > 0 and read > 0


def test_a_geo_block_is_not_a_transport_failure():
    """They want opposite responses: one is permanent from this address, the other
    clears. Collapsing them is how a book goes quietly missing."""
    assert base.FetchFailure.GEO_BLOCK != base.FetchFailure.TRANSPORT
    assert base.FetchFailure.GEO_BLOCK != base.FetchFailure.IP_BLOCK


def test_a_geo_block_is_not_retried():
    """Every address available here is in the same country, so a retry is waste."""
    book = PinnacleSportsbook(season=2026)

    class _Response:
        status_code = 403
        @staticmethod
        def json():
            return {"reason": "location", "detail": "Access from United States is prohibited"}

    class _Error(Exception):
        response = _Response()

    assert book._classify_http(_Error(), 403, "http://x") is False
    assert book.last_failure is base.FetchFailure.GEO_BLOCK


def test_a_server_error_is_retried():
    book = PinnacleSportsbook(season=2026)

    class _Error(Exception):
        response = None

    assert book._classify_http(_Error(), 503, "http://x") is True
    assert book.last_failure is base.FetchFailure.SERVER_ERROR


def test_a_403_with_an_unparseable_body_is_still_classified():
    """A book need not answer in the shape we expect when it refuses."""
    book = PinnacleSportsbook(season=2026)

    class _Response:
        status_code = 403
        @staticmethod
        def json():
            raise ValueError("not json")

    class _Error(Exception):
        response = _Response()

    assert book._classify_http(_Error(), 403, "http://x") is False
    assert book.last_failure is base.FetchFailure.IP_BLOCK


# --- Pinnacle's key grammar -----------------------------------------------

@pytest.mark.parametrize("key,expected", [
    ("s;0;m",             ("GAME", "Moneyline", 0.0, None)),
    ("s;0;ou;44.5",       ("GAME", "Total", 44.5, None)),
    ("s;0;s;-2.5",        ("GAME", "Spread", -2.5, None)),
    ("s;0;tt;20.5;home",  ("GAME", "TeamTotal", 20.5, "home")),
    ("s;1;ou;21.5",       ("H1", "Total", 21.5, None)),
])
def test_pinnacle_market_keys_parse(key, expected):
    assert PinnacleSportsbook._parse_key(key) == expected


@pytest.mark.parametrize("key", ["s;0;kickoff", "s;9;ou;44", "malformed", "s;0"])
def test_an_unmodelled_market_is_skipped_rather_than_guessed(key):
    """Pinnacle posts far more families than this repo models. Skipping quietly is
    correct; guessing at one would put a number in the store that nothing checks."""
    assert PinnacleSportsbook._parse_key(key) is None


def test_the_nfl_league_id_is_pinned():
    """Plan 36 recorded this as unknown, with discovery on a blocked route. It was
    already in Scripts/scrape_pinnacle_season.py, and it is 889."""
    from Scripts import scrape_pinnacle_season as sps
    from Scripts.books import pinnacle
    assert pinnacle.NFL_LEAGUE_ID == sps.NFL_LEAGUE_ID == 889


# --- live ------------------------------------------------------------------

@pytest.mark.live
def test_pinnacle_answers_and_prices_the_four_markets():
    """Deselected by default. The assertion is market *coverage*, never a game count:
    Pinnacle prices about the upcoming week and sometimes lookahead lines, so a count
    is context rather than a bar."""
    views = PinnacleSportsbook(season=2026).get_df_dict()
    main = views["Pinnacle"]
    assert main.height > 0, "Pinnacle returned nothing"
    assert set(main["marketTitle"].unique()) == set(schema.MARKET_TITLES)

    paired = (views["All_Bets"].group_by(list(schema.PAIR_KEYS))
              .agg(pl.col("fairProb").sum().alias("s"), pl.len().alias("n"))
              .filter(pl.col("n") == 2))
    assert paired["s"].max() == pytest.approx(1.0, abs=1e-9)
    assert paired["s"].min() == pytest.approx(1.0, abs=1e-9)


# --- BetOnline's offering feed --------------------------------------------
#
# Its payload carries every market whether or not it is posted, with Line and Point
# at zero. Reading those as real would put a stream of 0-point, 0-price rows into the
# store, every one of them de-vigging to nonsense.

from Scripts.books.betonline import BetOnlineSportsbook, _posted


def test_an_unposted_market_is_absent_rather_than_zero():
    assert _posted({"Line": -110, "Point": 44.5}) is True
    assert _posted({"Line": 0, "Point": 0}) is False
    assert _posted({}) is False
    assert _posted(None) is False


def test_a_pick_em_spread_is_a_real_market():
    """Keyed on the price, never the point. A spread of exactly 0 is a pick-em and is
    posted; a price of 0 is the feed saying nothing is offered."""
    assert _posted({"Line": -110, "Point": 0}) is True


def test_both_sides_of_a_spread_share_one_market_line():
    """Or they are not each other's de-vig partner. The convention is the home team's
    number, matching how Pinnacle keys the market."""
    book = BetOnlineSportsbook(season=2026)
    games = [("09/13/2026", {
        "HomeTeam": "Buffalo Bills", "AwayTeam": "New York Jets",
        "HomeRotation": 102, "WagerCutOff": "2026-09-13T13:00:00",
        "HomeLine": {"SpreadLine": {"Point": -2.5, "Line": -125},
                     "MoneyLine": {"Line": -161}},
        "AwayLine": {"SpreadLine": {"Point": 2.5, "Line": 105},
                     "MoneyLine": {"Line": 141}},
        "TotalLine": {"TotalLine": {"Point": 44.5,
                                    "Over": {"Line": -110},
                                    "Under": {"Line": -110}}},
    })]
    df = book.transform_to_standard(book._flatten(games))
    spread = df.filter(pl.col("marketTitle") == "Spread")
    assert spread["marketLine"].unique().to_list() == [-2.5]
    # ...while each side still reports its own number.
    assert sorted(spread["value"].to_list()) == [-2.5, 2.5]
    assert spread["fairProb"].sum() == pytest.approx(1.0, abs=1e-9)


def test_an_unposted_team_total_produces_no_rows():
    """The preseason shape: spread and total posted, team totals all zero."""
    book = BetOnlineSportsbook(season=2026)
    zero = {"Line": 0, "DecimalLine": 0}
    games = [("08/27/2026", {
        "HomeTeam": "Buffalo Bills", "AwayTeam": "Pittsburgh Steelers",
        "HomeRotation": 102, "WagerCutOff": "2026-08-27T19:00:00",
        "HomeLine": {"SpreadLine": {"Point": -2.5, "Line": -125},
                     "MoneyLine": {"Line": -161},
                     "TeamTotalLine": {"Point": 0, "Over": zero, "Under": zero}},
        "AwayLine": {"SpreadLine": {"Point": 2.5, "Line": 105},
                     "MoneyLine": {"Line": 141},
                     "TeamTotalLine": {"Point": 0, "Over": zero, "Under": zero}},
        "TotalLine": {"TotalLine": {"Point": 34.5,
                                    "Over": {"Line": -110},
                                    "Under": {"Line": -110}}},
    })]
    df = book.transform_to_standard(book._flatten(games))
    assert "TeamTotal" not in df["marketTitle"].to_list()
    assert set(df["marketTitle"].unique()) == {"Spread", "Moneyline", "Total"}


def test_betonline_dates_match_the_schedules_gameday():
    """08/27/2026 -> 2026-08-27, which is the only key the two sources agree on."""
    assert BetOnlineSportsbook._date("08/27/2026") == "2026-08-27"
    assert BetOnlineSportsbook._date("bad") is None
    assert BetOnlineSportsbook._date(None) is None


def test_each_book_declares_its_own_expected_markets():
    """A book that does not post team totals is a different book, not a broken one.
    One shared list would either fail on it or stop protecting the books that do."""
    from Scripts.books.pull import BOOKS
    assert "TeamTotal" in BOOKS["Pinnacle"]["expect_markets"]
    assert "TeamTotal" not in BOOKS["BetOnline"]["expect_markets"]


@pytest.mark.live
def test_betonline_answers_and_prices_its_three_markets():
    views = BetOnlineSportsbook(season=2026).get_df_dict()
    df = views["All_Bets"]
    assert df.height > 0
    assert {"Spread", "Total", "Moneyline"} <= set(df["marketTitle"].unique())
    paired = (df.group_by(list(schema.PAIR_KEYS))
                .agg(pl.col("fairProb").sum().alias("s"), pl.len().alias("n"))
                .filter(pl.col("n") == 2))
    assert paired["s"].min() == pytest.approx(1.0, abs=1e-9)
    assert paired["s"].max() == pytest.approx(1.0, abs=1e-9)


# --- 4Casters, the exchange -----------------------------------------------

from Scripts.books.fourcasters import FourCastersSportsbook


def test_an_exchange_is_labelled_as_one():
    """Plan 36 asked that the caveat live in the schema rather than a comment. An
    exchange price is a different quantity and a consumer has to be able to tell."""
    assert FourCastersSportsbook.book_type == schema.EXCHANGE
    assert PinnacleSportsbook.book_type == schema.BOOK


def test_the_best_offer_needs_real_money_behind_it():
    """An order book always has a "best" price. Without a floor it is whoever left a
    five-dollar limit order at a silly number, which is not a market view."""
    best = FourCastersSportsbook._best([
        {"odds": 500, "sumUntaken": 2.0},      # dust at a silly price
        {"odds": -110, "sumUntaken": 400.0},
        {"odds": -105, "sumUntaken": 300.0},
    ])
    assert best["odds"] == -105


def test_no_offer_with_real_money_means_no_price():
    assert FourCastersSportsbook._best([{"odds": 500, "sumUntaken": 1.0}]) is None
    assert FourCastersSportsbook._best([]) is None
    assert FourCastersSportsbook._best(None) is None


def test_missing_credentials_are_a_named_failure_not_a_crash():
    import os
    book = FourCastersSportsbook(season=2026)
    saved = {k: os.environ.pop(k, None)
             for k in ("CAST4_USER", "CAST4_PASS", "CAST4_AUTH_TOKEN")}
    try:
        with pytest.raises(base.BookFetchError, match="CAST4_USER"):
            book._authenticate()
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


def test_an_optional_book_does_not_take_the_run_down():
    """A book needing credentials that may not be configured is optional. A
    *required* book failing stays fatal -- the whole point is that a missing source
    must not read as agreement."""
    from Scripts.books.pull import BOOKS
    assert BOOKS["FourCasters"].get("optional") is True
    assert not BOOKS["Pinnacle"].get("optional")
    assert not BOOKS["BetOnline"].get("optional")


@pytest.mark.live
def test_fourcasters_answers_when_credentials_are_set():
    """Skipped without credentials rather than failed -- they are not in the repo."""
    import os
    if not (os.getenv("CAST4_USER") and os.getenv("CAST4_PASS")):
        pytest.skip("CAST4_USER/CAST4_PASS not set")
    df = FourCastersSportsbook(season=2026).get_df_dict()["All_Bets"]
    assert df.height > 0
    assert set(df["bookType"].unique()) == {schema.EXCHANGE}
    assert {"Spread", "Total", "Moneyline"} <= set(df["marketTitle"].unique())
