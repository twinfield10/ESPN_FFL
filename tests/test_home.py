"""The landing page's decisions: what has been played, what to do, and in what order.

Covers ``app/home.py``. No store on disk and no network -- fixtures are synthesised to
the shape ``team_stats`` and ``lineups`` actually carry.

Three of these pin things that were live in the store the day the tab was built, and
that a reasonable implementation gets wrong:

* **An unplayed fixture is not a tie.** ESPN reports it as 0-0, ``scrape_team_stats``
  records a tie, and ``season_ties`` accumulates it -- so every team in all ten
  leagues read **0-0-1** on 2026-09-09 with the season not started.
  ``box_score_available`` is ``true`` on those rows and is no help. ``test_played_*``
  and ``test_records_*`` are that trap's regression tests.
* **Projection is the tiebreaker, not the ranking.** Which means it is also the
  *whole* ranking in week 1, when every team is 0-0 on 0.0 points -- the difference
  between landing on a real table and landing on six rows of zeroes in alphabetical
  order.
* **A start/sit worth points is a decision; a ruled-out starter is an error.**
  Flattening the two would leave five cards permanently shouting, which is how a
  landing page stops being read.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

# `app/` is not a package: Streamlit runs the page scripts directly with app/ on
# sys.path, so the modules import each other by bare name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import home  # noqa: E402
import lineup as lu  # noqa: E402


# --- fixtures ------------------------------------------------------------

def fixture(owner, week, score, opp_score, *, opponent="Them", regular=True):
    """One ``team_stats`` row, with the columns :mod:`home` reads.

    ``season_ties`` is set the way the artifact really sets it -- counting the
    unplayed weeks -- so the tests are run against the trap rather than around it.
    """
    return {
        "year": 2026, "week": float(week), "team_owner": owner,
        "team_name": f"{owner} FC", "opp_owner": opponent,
        "opp_name": f"{opponent} FC", "team_score": score, "opp_score": opp_score,
        "is_regular_season": regular, "box_score_available": True,
        "season_ties": 1 if (score == 0.0 and opp_score == 0.0) else 0,
    }


def player(name, points, *, slot="BE", owner="Me", status="active", week=1):
    """One ``lineups`` row, with the columns :mod:`home` reads."""
    return {
        "player_id": abs(hash(name)) % 10 ** 6, "player_name": name,
        "player_position": "WR", "primaryPosition": "WR", "slotPosition": slot,
        "eligiblePositions": ["WR", "RB/WR/TE", "BE", "IR"],
        "player_active_status": status, "TRUE_Points": points,
        "team_owner": owner, "week": week,
    }


def upgrade(severity, margin, *, slot="WR", name="Wire Guy"):
    """One :class:`lineup.Upgrade`, with only the fields a card reads set."""
    return lu.Upgrade(
        slot=slot, severity=severity, best={"player_name": name},
        over={"player_name": "Yours", "slotPosition": slot}, margin=margin,
        beaten=1, better=3)


def swap(gain, *, slot="WR"):
    """One :class:`lineup.Swap`."""
    return lu.Swap(slot=slot, start="In", sit="Out", gain=gain,
                   start_row={}, sit_row={})


# --- what counts as played -----------------------------------------------

def test_an_unplayed_fixture_is_not_played():
    """0-0 is the absence of a box score, not a nil-all draw.

    The store's own ``season_ties`` says 1 on these rows and ``box_score_available``
    says ``true``. Believing either is how a table of 0-0-1 records gets rendered
    before a game has been played.
    """
    frame = pl.DataFrame([fixture("Me", 1, 0.0, 0.0), fixture("Them", 1, 0.0, 0.0)])
    assert home.played(frame).is_empty()
    assert frame["season_ties"].sum() == 2, "fixture must reproduce the real trap"


def test_a_played_fixture_survives():
    frame = pl.DataFrame([fixture("Me", 1, 121.4, 108.2)])
    assert home.played(frame).height == 1


def test_a_real_tie_is_played():
    """Two teams that both scored the same is a tie, and must count as one -- the
    test for "unplayed" is that *nobody* scored, not that the scores are level."""
    frame = pl.DataFrame([fixture("Me", 1, 104.6, 104.6)])
    assert home.played(frame).height == 1
    assert home.records(home.played(frame))["ties"][0] == 1


def test_the_playoffs_are_not_in_the_regular_season_table():
    frame = pl.DataFrame([fixture("Me", 1, 120.0, 100.0),
                          fixture("Me", 15, 130.0, 90.0, regular=False)])
    assert home.played(frame)["week"].to_list() == [1.0]


def test_played_on_an_empty_frame_is_empty():
    """Before week 1 the whole season is unplayed, so this is the common case."""
    assert home.played(pl.DataFrame()).is_empty()


# --- records --------------------------------------------------------------

def test_records_count_only_played_weeks():
    """Recomputed rather than read off ``season_wins``/``season_ties``."""
    frame = pl.DataFrame([
        fixture("Me", 1, 120.0, 100.0),      # win
        fixture("Me", 2, 90.0, 110.0),       # loss
        fixture("Me", 3, 105.0, 105.0),      # tie
        fixture("Me", 4, 0.0, 0.0),          # not played
    ])
    row = home.records(home.played(frame)).row(0, named=True)
    assert (row["wins"], row["losses"], row["ties"]) == (1, 1, 1)
    assert row["games"] == 3
    assert row["points_for"] == pytest.approx(315.0)
    assert row["points_against"] == pytest.approx(315.0)


def test_a_tie_is_half_a_win():
    """Which is how every league configured here breaks its table."""
    frame = pl.DataFrame([fixture("Me", 1, 120.0, 100.0),
                          fixture("Me", 2, 105.0, 105.0)])
    assert home.records(home.played(frame))["win_pct"][0] == pytest.approx(0.75)


def test_records_of_nothing_played_is_an_empty_frame_with_columns():
    """Empty, but typed -- :func:`home.standings` joins onto it before week 1, and a
    frame with no columns cannot be joined."""
    empty = home.records(pl.DataFrame())
    assert empty.is_empty()
    assert "team_owner" in empty.columns and "win_pct" in empty.columns


# --- standings ------------------------------------------------------------

def test_before_week_one_the_projection_is_the_whole_order():
    """Every team 0-0 on 0.0 points, so the only thing left to sort on is the blend.

    Without this the landing page's first impression of the season is six rows of
    zeroes in whatever order the group-by happened to emit.
    """
    frame = pl.DataFrame([fixture(name, 1, 0.0, 0.0)
                          for name in ("Alice", "Bob", "Carol")])
    table = home.standings(frame, {"Alice": 101.0, "Bob": 130.0, "Carol": 118.0}, 1)
    assert table["Owner"].to_list() == ["Bob", "Carol", "Alice"]
    assert table["Rk"].to_list() == [1, 2, 3]
    assert table["W-L-T"].to_list() == ["0-0-0"] * 3


def test_the_record_outranks_the_projection():
    """The projection is a tiebreaker. A team that has won must not be sorted below
    one that merely projects better -- which would be the whole table lying."""
    frame = pl.DataFrame([
        fixture("Alice", 1, 120.0, 90.0), fixture("Bob", 1, 90.0, 120.0),
    ])
    table = home.standings(frame, {"Alice": 100.0, "Bob": 180.0}, 1)
    assert table["Owner"].to_list() == ["Alice", "Bob"]


def test_points_for_breaks_a_tie_before_the_projection_does():
    """Two teams level on record split on points for, as ESPN's own table does. The
    projection only speaks when the actual numbers are silent."""
    frame = pl.DataFrame([
        fixture("Alice", 1, 130.0, 90.0), fixture("Bob", 1, 110.0, 100.0),
        fixture("Carol", 1, 90.0, 130.0), fixture("Dave", 1, 100.0, 110.0),
    ])
    table = home.standings(
        frame, {"Alice": 1.0, "Bob": 200.0, "Carol": 1.0, "Dave": 200.0}, 1)
    # Alice and Bob both 1-0; Alice scored more, and outranks Bob despite Bob's
    # enormous projection. Same one rung down for Carol and Dave.
    assert table["Owner"].to_list() == ["Alice", "Bob", "Dave", "Carol"]


def test_this_week_is_empty_until_the_week_is_played():
    """A projection column and an actual column, and the actual one must not read
    0.0 for a week nobody has played -- that is a score, and there isn't one."""
    frame = pl.DataFrame([fixture("Alice", 1, 120.0, 90.0),
                          fixture("Alice", 2, 0.0, 0.0)])
    played_week = home.standings(frame, {"Alice": 111.0}, 1)
    assert played_week["This Week"][0] == pytest.approx(120.0)

    unplayed = home.standings(frame, {"Alice": 111.0}, 2)
    assert unplayed["This Week"][0] is None
    assert unplayed["Projected"][0] == pytest.approx(111.0)


def test_a_team_with_no_fixtures_at_all_still_makes_the_table():
    """Anyone with a roster belongs on it. Before week 1 that is everybody, and a
    table built only from played rows would be empty."""
    table = home.standings(pl.DataFrame(), {"Alice": 120.0, "Bob": 100.0}, 1)
    assert table["Owner"].to_list() == ["Alice", "Bob"]


def test_standings_of_nothing_is_empty():
    assert home.standings(pl.DataFrame(), {}, 1).is_empty()


def test_results_without_projections_still_make_a_table():
    """The mirror of the week-1 case: a league whose rosters are all unset has no
    projections to rank on, and must fall back to the record rather than raise."""
    frame = pl.DataFrame([fixture("Alice", 1, 120.0, 90.0)])
    table = home.standings(frame, {}, 1)
    assert table["Owner"].to_list() == ["Alice"]
    assert table["W-L-T"][0] == "1-0-0"
    assert table["Projected"][0] is None


# --- projections ----------------------------------------------------------

def test_projections_total_the_lineup_as_set_not_the_best_one():
    """Lineup-as-set, so the number matches the one the Matchup tab quotes. Using
    the optimal lineup would rank a league on lineups nobody has submitted."""
    frame = pl.DataFrame([
        player("Starter", 20.0, slot="WR", owner="Me"),
        player("Better Bench", 99.0, slot="BE", owner="Me"),
        player("Theirs", 15.0, slot="WR", owner="Them"),
    ])
    assert home.lineup_projections(frame) == {"Me": 20.0, "Them": 15.0}


def test_an_owner_with_no_starter_is_absent_rather_than_zero():
    """A roster nobody has set is not a roster projected to score nothing, and a
    standings table that says 0.0 would rank it last on a claim it cannot support."""
    frame = pl.DataFrame([player("All Benched", 30.0, slot="BE", owner="Me")])
    assert home.lineup_projections(frame) == {}


# --- actions --------------------------------------------------------------

def test_an_optimal_lineup_raises_no_action():
    assert home.lineup_action([], 0) is None


def test_points_on_the_table_are_a_warning():
    action = home.lineup_action([swap(4.0), swap(2.0)], 0)
    assert action.severity == home.ACTION_WARNING
    assert "+6.0" in action.headline
    assert action.route == home.ROUTE_ROSTER


def test_a_starter_who_cannot_play_is_critical():
    """The one case that is an error rather than a decision. Most weeks produce a
    start/sit and almost none produce this, which is what makes it worth shouting."""
    action = home.lineup_action([swap(4.0)], 1)
    assert action.severity == home.ACTION_CRITICAL
    assert "cannot play" in action.headline


def test_a_ruled_out_starter_is_critical_even_with_nothing_to_gain():
    """No legal replacement to swap in -- the case ``lineup.swaps`` has nothing to
    pair him with, and therefore the one a points-based rule would miss entirely."""
    action = home.lineup_action([], 2)
    assert action.severity == home.ACTION_CRITICAL
    assert "2 starters" in action.headline


def test_a_clean_wire_raises_no_action():
    assert home.waiver_action([]) is None


def test_a_beaten_starter_outranks_a_beaten_bench_player():
    """Critical wins and leads, exactly as ``views.weekly.render_upgrades`` orders
    the same two findings on the Free Agents tab."""
    action = home.waiver_action([
        upgrade(lu.UPGRADE_DEPTH, 8.0, name="Deep Guy"),
        upgrade(lu.UPGRADE_CRITICAL, 1.2, slot="TE", name="Start Him"),
    ])
    assert action.severity == home.ACTION_CRITICAL
    assert "Start Him" in action.headline
    assert "1 starting slot" in action.headline
    assert "1 on the bench" in action.headline
    assert action.route == home.ROUTE_FREE_AGENTS


def test_depth_alone_is_a_warning():
    action = home.waiver_action([upgrade(lu.UPGRADE_DEPTH, 3.1)])
    assert action.severity == home.ACTION_WARNING
    assert "bench slot" in action.headline


def test_the_headline_names_the_biggest_margin():
    """Not the first one found. A card has room for one name, so it should be the
    one worth acting on."""
    action = home.waiver_action([
        upgrade(lu.UPGRADE_CRITICAL, 0.6, name="Marginal"),
        upgrade(lu.UPGRADE_CRITICAL, 14.0, slot="TE", name="Obvious"),
    ])
    assert "Obvious" in action.headline


# --- ordering -------------------------------------------------------------

def summary(name, actions):
    """A :class:`home.LeagueSummary` with only the fields ordering reads."""
    return home.LeagueSummary(
        league_key=name, display_name=name, week=1, owner="Me", projected=100.0,
        opponent=None, opponent_projected=None, win=None, margin=None,
        record=(0, 0, 0), rank=None, teams=10, actions=actions, standings=None,
        notes=[])


def test_the_worst_league_is_read_first():
    clean = summary("clean", [])
    warned = summary("warned", [home.Action(home.ACTION_WARNING, "↕️", "x",
                                            home.ROUTE_ROSTER, "Roster")])
    urgent = summary("urgent", [home.Action(home.ACTION_CRITICAL, "🚨", "x",
                                            home.ROUTE_ROSTER, "Roster")])
    assert [s.league_key for s in home.by_urgency([clean, warned, urgent])] == \
        ["urgent", "warned", "clean"]


def test_a_leagues_worst_action_decides_its_place():
    """Not its total. One critical outranks three warnings, because the critical is
    the thing that loses you the week."""
    one_bad = summary("one_bad", [home.Action(home.ACTION_CRITICAL, "🚨", "x",
                                              home.ROUTE_ROSTER, "Roster")])
    three_meh = summary("three_meh", [
        home.Action(home.ACTION_WARNING, "↕️", "x", home.ROUTE_ROSTER, "Roster")] * 3)
    assert [s.league_key for s in home.by_urgency([three_meh, one_bad])] == \
        ["one_bad", "three_meh"]


def test_the_order_is_stable_within_a_severity():
    """So the quiet cards keep the order they arrived in -- store order, which
    ``auth.visible_leagues`` keeps sorted and stable across seasons -- rather than
    reshuffling weekly. A readout you have to hunt through costs more than the
    sorting saves."""
    same = [summary(name, [home.Action(home.ACTION_CRITICAL, "🚨", "x",
                                       home.ROUTE_ROSTER, "Roster")])
            for name in ("first", "second", "third")]
    assert [s.league_key for s in home.by_urgency(same)] == \
        ["first", "second", "third"]


# --- which week ------------------------------------------------------------

def test_the_selected_week_is_honoured_where_the_league_has_it():
    assert home.week_for({"current_week": 1, "weeks_present": [1, 2, 3]}, 3) == 3


def test_a_league_without_the_selected_week_falls_back_to_its_own():
    """Home draws every league but the Week selector is offered from one of them, so
    the requested week is not always a week this league has."""
    assert home.week_for({"current_week": 2, "weeks_present": [1, 2]}, 7) == 2


def test_a_store_that_records_no_weeks_takes_the_request_on_trust():
    """``weeks_present`` is absent from stores built before it was added, and the
    request is better evidence than nothing."""
    assert home.week_for({"current_week": 1}, 4) == 4


def test_no_request_means_this_leagues_own_week():
    assert home.week_for({"current_week": 5, "weeks_present": [4, 5]}, None) == 5


# --- the viewer's own row -------------------------------------------------
#
# ``app/views/home_tab.py`` is layout, not decision, so almost nothing in it is
# tested here. ``_own_row_styler`` is the exception: it decides *which* row, and
# getting that wrong points the reader at another manager's season.

from views import home_tab as ht  # noqa: E402


def carded(owners, owner, **overrides):
    """A summary whose ``standings`` holds ``owners`` in table order."""
    fields = dict(
        league_key="lg", display_name="lg", week=1, owner=owner, projected=100.0,
        opponent=None, opponent_projected=None, win=None, margin=None,
        record=(0, 0, 0), rank=None, teams=len(owners), actions=[], notes=[],
        standings=pl.DataFrame({
            "Rk": list(range(1, len(owners) + 1)),
            "Owner": owners,
            "W-L-T": ["0-0-0"] * len(owners),
            "Win%": [0.0] * len(owners),
            "PF": [float(i) for i in range(len(owners))],
            "PA": [0.0] * len(owners),
            "This Week": [None] * len(owners),
            "Projected": [100.0 - i for i in range(len(owners))],
        }))
    fields.update(overrides)
    return home.LeagueSummary(**fields)


def rendered_css(card):
    """The CSS Streamlit will actually send for this card's table.

    Marshalled rather than asserted on intent, for the reason
    ``test_weekly_views.css_for`` gives: a ``Styler`` property Streamlit silently
    drops looks identical from Python.
    """
    from streamlit.elements.lib.pandas_styler_utils import marshall_styler
    from streamlit.proto.ArrowData_pb2 import ArrowData

    proto = ArrowData()
    marshall_styler(proto, ht._own_row_styler(card), "uuid")
    return proto.styler.styles


def banded_rows(css):
    """Which row indices carry the band."""
    import re
    out = set()
    for selector, body in re.findall(r"([^{]+)\{([^}]*)\}", css):
        if ht.OWN_ROW_FILL in body:
            out |= {int(r) for r in re.findall(r"_row(\d+)_col", selector)}
    return out


def test_the_owners_row_is_the_one_banded():
    css = rendered_css(carded(["Ann", "Me", "Bo"], "Me"))
    assert banded_rows(css) == {1}


def test_it_follows_the_owner_and_not_the_top_of_the_table():
    """The case that matters: on ``knights_ffl`` the primary owner is tenth of
    fourteen, which is both the hardest row to find by eye and the one a
    rank-based shortcut would get wrong."""
    owners = [f"O{i}" for i in range(14)]
    owners[9] = "Me"
    assert banded_rows(rendered_css(carded(owners, "Me"))) == {9}


def test_the_band_is_carried_on_every_column_of_that_row():
    """A partial band reads as a rendering fault rather than as emphasis."""
    import re
    css = rendered_css(carded(["Ann", "Me"], "Me"))
    cols = {int(c) for selector, body in re.findall(r"([^{]+)\{([^}]*)\}", css)
            if ht.OWN_ROW_FILL in body
            for c in re.findall(r"_row1_col(\d+)", selector)}
    assert cols == set(range(8))


def test_the_row_is_bold_as_well_as_banded():
    """Two channels, so the row is findable where a faint band over an unfamiliar
    background is not -- and weight is one of the three properties Streamlit's grid
    actually honours."""
    css = rendered_css(carded(["Ann", "Me"], "Me"))
    assert "font-weight: 700" in css


def test_the_band_needs_no_theme_because_it_composites():
    """An alpha over whatever is behind it, so this module takes no ``theme``
    argument -- unlike :func:`sheet_view.panel_styler`, which threads one through to
    pick an opaque hex for the same job."""
    assert ht.OWN_ROW_FILL.startswith("rgba(")
    assert "#" not in ht.OWN_ROW_FILL


def test_the_band_carries_no_hue():
    """A colour on your own row would read as a verdict on where you sit, and the
    row is worth finding whether you are first or last."""
    import re
    red, green, blue = (int(n) for n in
                        re.findall(r"[\d.]+", ht.OWN_ROW_FILL)[:3])
    assert red == green == blue


def test_nothing_is_painted_when_the_store_names_no_owner():
    """Every store written before ``primary_owner`` existed."""
    card = carded(["Ann", "Bo"], None)
    assert ht._own_row_styler(card) is card.standings


def test_nothing_is_painted_when_the_owner_is_not_in_the_table():
    """A rename upstream, or a manager who left the league mid-season."""
    card = carded(["Ann", "Bo"], "Me")
    assert ht._own_row_styler(card) is card.standings


def test_nothing_is_painted_without_an_owner_column():
    card = carded(["Ann", "Me"], "Me")
    card = card._replace(standings=card.standings.drop("Owner"))
    assert ht._own_row_styler(card) is card.standings


def test_the_styler_paints_and_does_not_format():
    """``column_config`` owns the number formats -- see
    :data:`views.home_tab.STANDINGS_CONFIG`. A ``Styler`` that also formats hands
    Streamlit display *strings*, and this table is sorted on ``PF`` and
    ``Projected``, so a string column would cost real behaviour. Same division of
    labour as ``sheet_view.panel_styler``."""
    styler = ht._own_row_styler(carded(["Ann", "Me"], "Me"))
    kinds = {name: dtype.kind for name, dtype in styler.data.dtypes.items()}
    assert kinds["Rk"] == "i"
    assert kinds["Win%"] == kinds["PF"] == kinds["Projected"] == "f"


def test_a_missing_number_renders_blank_not_the_word_nan():
    """``This Week`` is null until the week is played, and pandas renders a missing
    value with ``str`` unless told otherwise -- which is how ``Exp Return`` on the
    draft board came to read "None" on 998 of 1,026 rows. ``na_rep`` is the half of
    that fix this styler owns; ``st.dataframe(placeholder="")`` is the other."""
    styler = ht._own_row_styler(carded(["Ann", "Me"], "Me"))
    shown = styler._compute()._display_funcs
    week = list(styler.data.columns).index("This Week")
    assert shown[(0, week)](None) == ""
