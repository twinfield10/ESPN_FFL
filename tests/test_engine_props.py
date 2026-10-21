"""The sportsbook-engine adapter (plan 54): board, conversion, agreement, shadow.

Every rule here was measured against the mini's own scrapes on 2026-10-21 before it
was written down: the spell filter (7% of FanDuel's week-6 board was delisted), the
k - 0.5 ladder (100% of matched rungs), the one-pair FanDuel line (137 -> 0 player-stats
with two lines once delisted series go), and Novig's most balanced rung. The
fixtures are small boards built to pin each one.
"""

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

import Scripts.books.engine as en
from Scripts.books.weekly_props import RAW_SCHEMA

UTC = timezone.utc
T = datetime(2026, 10, 18, 10, 0, tzinfo=UTC)
KICK = datetime(2026, 10, 18, 17, 0, tzinfo=UTC)
GAME = "2026_06_LA_JAX"


def prop(book, market, player, line, side, price, at=T - timedelta(hours=2),
         start=KICK, period="GAME", in_play=False, game=GAME):
    """One engine prop transition."""
    key = player.lower().replace(" ", "_")
    return {"book": book, "source_event_id": f"{book}-{game}", "game_id": game,
            "market": market, "market_uid": f"{period}|{market}|{key}|{line}",
            "player_label": player, "player_key": key, "player_team_id": "LA",
            "side": side, "line": line, "price": float(price), "captured_at": at,
            "start_time": start, "period": period, "in_play": in_play}


def spells_for(rows, started=T - timedelta(days=1), ended=None, last_seen=T - timedelta(minutes=30)):
    """One open (or closed at ``ended``) spell per series in ``rows``."""
    frame = pl.DataFrame(rows).select(en.SERIES_KEY).unique()
    return frame.with_columns(pl.lit(started).alias("started_at"),
                              pl.lit(ended, dtype=pl.Datetime("us", "UTC")).alias("ended_at"),
                              pl.lit(last_seen).alias("last_seen"))


def frame(rows):
    return pl.DataFrame(rows, schema_overrides={"captured_at": pl.Datetime("us", "UTC"),
                                                "start_time": pl.Datetime("us", "UTC")})


FD = next(b for b in en.BOOKS if b.name == "FanDuel")
NV = next(b for b in en.BOOKS if b.name == "Novig")


# --- the board ------------------------------------------------------------------------

def test_the_board_is_each_series_last_price_at_t():
    rows = [prop("FanDuel", "receptions", "Puka Nacua", 7.5, "over", -120, at=T - timedelta(hours=5)),
            prop("FanDuel", "receptions", "Puka Nacua", 7.5, "over", -140, at=T - timedelta(hours=1)),
            prop("FanDuel", "receptions", "Puka Nacua", 7.5, "over", -200, at=T + timedelta(hours=1))]
    board = en.listed_board(frame(rows), spells_for(rows), T)
    assert board.height == 1 and board["price"][0] == -140


def test_an_unchanged_price_from_days_ago_is_still_on_the_board():
    rows = [prop("FanDuel", "receptions", "Puka Nacua", 7.5, "over", -120, at=T - timedelta(days=4))]
    assert en.listed_board(frame(rows), spells_for(rows), T).height == 1


def test_a_delisted_series_is_dropped():
    """The case changes-only rows cannot show: FanDuel moved its line 94.5 -> 96.5 and
    nothing ever closed the 94.5 series."""
    old = [prop("FanDuel", "receiving_yards", "Puka Nacua", 94.5, s, -114) for s in ("over", "under")]
    new = [prop("FanDuel", "receiving_yards", "Puka Nacua", 96.5, s, -114) for s in ("over", "under")]
    spells = pl.concat([spells_for(old, ended=T - timedelta(hours=1)), spells_for(new)])
    board = en.listed_board(frame(old + new), spells, T)
    assert sorted(board["line"].unique().to_list()) == [96.5]


def test_started_games_periods_and_in_play_are_excluded():
    rows = [prop("FanDuel", "receptions", "A", 4.5, "over", -110, start=T - timedelta(minutes=5)),
            prop("FanDuel", "receptions", "B", 4.5, "over", -110, period="1H"),
            prop("FanDuel", "receptions", "C", 4.5, "over", -110, in_play=True),
            prop("FanDuel", "receptions", "D", 4.5, "over", -110)]
    board = en.listed_board(frame(rows), spells_for(rows), T)
    assert board["player_label"].to_list() == ["D"]


# --- conversion ----------------------------------------------------------------------

def test_ladders_come_back_to_value_k_and_prices_to_decimal():
    rows = [prop("Novig", "receptions", "Puka Nacua", 6.5, "over", -150),
            prop("Novig", "receptions", "Puka Nacua", 8.5, "over", 150)]
    raw = en.to_raw(frame(rows), NV)
    assert raw.schema == pl.Schema(RAW_SCHEMA)
    ladder = raw.filter(pl.col("prop_source") == "Values").sort("value")
    assert ladder["value"].to_list() == [7.0, 9.0]
    assert ladder["odds"].to_list() == pytest.approx([1 + 100 / 150, 2.5])
    assert ladder["impProb"].to_list() == pytest.approx([0.6, 0.4])


def test_the_line_is_the_most_balanced_two_sided_rung():
    rows = [prop("Novig", "receiving_yards", "P", 84.5, "over", -300),
            prop("Novig", "receiving_yards", "P", 84.5, "under", 220),
            prop("Novig", "receiving_yards", "P", 94.5, "over", -112),
            prop("Novig", "receiving_yards", "P", 94.5, "under", -108)]
    line = en.to_raw(frame(rows), NV).filter(pl.col("prop_source") == "OverUnder")
    assert set(line["value"]) == {94.5} and sorted(line["type"]) == ["Over", "Under"]


def test_every_rung_ladder_vs_fanduels_one_sided_ladder():
    """Novig's adapter ladders every rung's over side; FanDuel's reads alt markets
    only, so its main line's over is not also a rung."""
    def board(book):
        return frame([prop(book, "receiving_yards", "P", 94.5, "over", -114),
                      prop(book, "receiving_yards", "P", 94.5, "under", -114),
                      prop(book, "receiving_yards", "P", 99.5, "over", 105)])
    nv = en.to_raw(board("Novig"), NV).filter(pl.col("prop_source") == "Values")
    fd = en.to_raw(board("FanDuel"), FD).filter(pl.col("prop_source") == "Values")
    assert sorted(nv["value"]) == [95.0, 100.0]
    assert fd["value"].to_list() == [100.0]


def test_the_anytime_touchdown_is_a_ladder_never_a_line():
    rows = [prop("Novig", "touchdowns", "P", 0.5, "yes", -130),
            prop("Novig", "touchdowns", "P", 0.5, "no", 110),
            prop("Novig", "touchdowns", "P", 1.5, "over", 400)]
    raw = en.to_raw(frame(rows), NV)
    assert set(raw["prop_source"]) == {"Values"}
    assert sorted(raw["value"]) == [1.0, 2.0]
    assert set(raw["espn_stat"]) == {"anytimeTouchdown"}


def test_markets_outside_the_projected_stats_are_dropped():
    rows = [prop("FanDuel", "first_td", "P", 0.5, "yes", 700),
            prop("FanDuel", "longest_reception", "P", 24.5, "over", -110)]
    assert en.to_raw(frame(rows), FD).is_empty()


def test_one_spelling_per_player():
    a = prop("Novig", "receptions", "Kyle Pitts Sr.", 4.5, "over", -110)
    b = {**prop("Novig", "receiving_yards", "Kyle Pitts", 44.5, "over", -110), "player_key": a["player_key"]}
    assert en.to_raw(frame([a, b]), NV)["player_name"].n_unique() == 1


# --- agreement and freshness -----------------------------------------------------------

def test_agreement_compares_fair_prices_with_novigs():
    rows = [prop("Novig", "receptions", "P", 6.5, "over", -110),
            prop("Novig", "receptions", "P", 6.5, "under", -110),
            prop("Kalshi", "receptions", "P", 6.5, "over", -150),
            prop("Kalshi", "receptions", "P", 6.5, "under", 130)]
    out = en.agreement(frame(rows))
    assert out.height == 1 and out["book"][0] == "Kalshi" and out["shared"][0] == 1
    fair_k = 0.6 / (0.6 + 100 / 230)
    assert out["mae"][0] == pytest.approx(abs(fair_k - 0.5))
    assert out["bias"][0] > 0


def test_book_age_is_the_last_poll_before_t():
    rows = [prop("FanDuel", "receptions", "P", 6.5, "over", -110)]
    ages = en.book_age_hours(spells_for(rows, last_seen=T - timedelta(hours=3)), T)
    assert ages == {"FanDuel": pytest.approx(3.0)}


# --- the shadow run ------------------------------------------------------------------------

@pytest.fixture
def shadow(tmp_path, monkeypatch):
    monkeypatch.setattr(en, "SHADOW_DIR", tmp_path / "Shadow")
    monkeypatch.setattr(en, "STATUS_PATH", tmp_path / "Shadow" / "engine_shadow.json")
    return tmp_path / "Shadow"


def _sched():
    return pl.DataFrame({"NFL_game_id": [GAME], "week": [6], "officialDate": [KICK.date()],
                         "Away": ["LA"], "Home": ["JAX"]})


def test_run_writes_the_shadow_and_withholds_a_stale_book(shadow):
    fresh = [prop("FanDuel", "receptions", "Puka Nacua", 7.5, s, -110) for s in ("over", "under")]
    stale = [prop("Novig", "receptions", "Puka Nacua", 7.5, s, -110) for s in ("over", "under")]
    spells = pl.concat([spells_for(fresh),
                        spells_for(stale, last_seen=T - timedelta(hours=en.MAX_AGE_HOURS + 1))])
    status = en.run(2026, 6, T, props=frame(fresh + stale), spells=spells, sched=_sched(), model=None)
    assert status["books"]["FanDuel"]["outcome"] == "projected"
    assert status["books"]["Novig"]["outcome"] == "withheld"
    out = pl.read_parquet(en.shadow_file(FD, 2026))
    assert out["player_name"].to_list() == ["Puka Nacua"]
    assert out["proj_receivingReceptions"][0] == pytest.approx(7.5, abs=0.6)
    assert list((shadow / "FanDuel" / "Season" / "2026" / "Raw").glob("FanDuel_Raw_Week_6_*.parquet"))
    assert not en.shadow_file(NV, 2026).exists()


def test_the_shadow_never_touches_the_real_book_files(shadow, monkeypatch, tmp_path):
    from Scripts import paths
    real = tmp_path / "Projections"
    monkeypatch.setattr(paths, "PROJECTIONS_DIR", real)
    rows = [prop("FanDuel", "receptions", "P", 7.5, s, -110) for s in ("over", "under")]
    en.run(2026, 6, T, props=frame(rows), spells=spells_for(rows), sched=_sched(), model=None)
    assert not real.exists()


def test_a_future_contract_major_is_refused():
    class Body:
        def read(self):
            return b'{"version": "2.0", "changes": []}'

    class Client:
        def get_object(self, **_):
            return {"Body": Body()}

    with pytest.raises(en.EngineError, match="2.0"):
        en.check_contract(Client())


def test_series_age_reads_each_series_last_poll_not_the_books_newest():
    """LowVig's two collectors: an hourly sportsbook poll and a widget walked four times
    a day. The book's newest poll is minutes old; its ladder is not."""
    line = [prop("LowVig", "receptions", "P", 6.5, s, -110) for s in ("over", "under")]
    ladder = [prop("LowVig", "receptions", "P", 8.5, "over", 200)]
    spells = pl.concat([spells_for(line, last_seen=T - timedelta(minutes=10)),
                        spells_for(ladder, last_seen=T - timedelta(hours=4))])
    board = en.listed_board(frame(line + ladder), spells, T)
    assert en.book_age_hours(spells, T)["LowVig"] == pytest.approx(10 / 60)
    ages = en.series_age_hours(board, T)["LowVig"]
    assert ages["max"] == pytest.approx(4.0)
    assert ages["p50"] == pytest.approx(10 / 60, abs=0.01)  # rounded to 0.01h


def test_a_series_seen_after_t_counts_as_fresh():
    rows = [prop("FanDuel", "receptions", "P", 6.5, "over", -110)]
    board = en.listed_board(frame(rows), spells_for(rows, last_seen=T + timedelta(hours=2)), T)
    assert en.series_age_hours(board, T)["FanDuel"]["max"] == 0.0
