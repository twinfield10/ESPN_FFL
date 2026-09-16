"""Player Shares: what a player is worth to a viewer across all his leagues.

The arithmetic is exact and closed-form, so these are real assertions about
numbers rather than smoke tests. The two that matter most are the *sign* (a player
on the opponent's roster must come back negative) and the *weighting* (the same
player is worth more in a close matchup), because those are the two claims the tab
makes on its face.
"""

import sys
from pathlib import Path

import pytest

APP = Path(__file__).resolve().parents[1] / "app"
if str(APP) not in sys.path:
    sys.path.insert(0, str(APP))

import auth                                                      # noqa: E402
import matchup_sim as sim                                        # noqa: E402
import player_shares as ps                                       # noqa: E402
from Scripts.outcomes import weekly as wk                        # noqa: E402

#: A dispersion model with one position and no proportional term, so
#: ``sd = sqrt(phi * mu)`` and every number below can be checked by hand.
FITTED = {
    "version": wk.MODEL_VERSION,
    "positions": {"WR": {"phi": 4.0, "inv_k": 0.0}},
    "pooled": {"phi": 4.0, "inv_k": 0.0},
}


def player(name, points, position="WR", player_id=None):
    """One starting lineups row.

    Args:
        name: Player name.
        points: His projection, used as both the live and the remaining number so
            the row behaves like a pre-kickoff week.
        position: His position.
        player_id: ESPN id. Defaults to a hash of the name, so two rows for the
            same player in different leagues collapse to one share.

    Returns:
        dict: A row shaped like ``lineups.parquet``.
    """
    return {
        "player_id": player_id if player_id is not None else abs(hash(name)) % 10 ** 6,
        "player_name": name,
        "player_position": position,
        "primaryPosition": position,
        "pro_team": "NYJ",
        "slotPosition": position,
        "slot": position,
        "game_state": "pre",
        "LIVE_Points": points,
        "LIVE_remaining_points": points,
        "TRUE_Points": points,
    }


def matchup(display_name, mine_rows, opp_rows, fitted=FITTED, league_key=None):
    """A priced fixture, built without touching a store.

    Args:
        display_name: The league's name.
        mine_rows: The viewer's starters.
        opp_rows: His opponent's.
        fitted: Dispersion model, or None for the unmodelled case.
        league_key: Defaults to a slug of ``display_name``.

    Returns:
        player_shares.LeagueMatchup: Ready for :func:`player_shares.shares`.
    """
    mine = sim.side("Me", mine_rows, "LIVE_Points", fitted)
    theirs = sim.side("Them", opp_rows, "LIVE_Points", fitted)
    return ps.LeagueMatchup(
        league_key=league_key or display_name.lower(), display_name=display_name,
        owner="Me", opponent="Them", mine=mine, theirs=theirs,
        win=sim.outcome(mine, theirs).win, starters=mine_rows,
        opp_starters=opp_rows, points_column="LIVE_Points")


def by_name(shares_):
    """Index shares by player name."""
    return {s.player_name: s for s in shares_}


# --- the sign, which is the whole point -------------------------------------

def test_a_player_you_start_is_worth_rooting_for():
    """Positive stake, and the leagues he is yours in are named."""
    star = player("Star", 20.0)
    m = matchup("A", [star, player("Mine2", 12.0)],
                [player("Theirs1", 16.0), player("Theirs2", 16.0)])
    share = by_name(ps.shares([m], FITTED))["Star"]

    assert share.stake > 0
    assert share.owned == ("A",)
    assert share.faced == ()
    assert not share.conflicted


def test_a_player_your_opponent_starts_is_worth_rooting_against():
    """The same player, on the other roster, comes back negative.

    The sign is not applied by hand anywhere -- it falls out of perturbing the side
    he actually starts on and always reading the *viewer's* win probability. This is
    the test that says so.
    """
    star = player("Star", 20.0)
    m = matchup("A", [player("Mine1", 16.0), player("Mine2", 16.0)],
                [star, player("Theirs2", 12.0)])
    share = by_name(ps.shares([m], FITTED))["Star"]

    assert share.stake < 0
    assert share.faced == ("A",)
    assert share.owned == ()


# --- the weighting, which is what the request was about ----------------------

def test_the_same_player_is_worth_more_in_a_close_matchup():
    """A coin flip beats a blowout, at identical projections.

    Mirrors ``test_matchup_sim.test_the_same_points_are_worth_more_in_a_close_matchup``
    one level down: there it is a lineup change, here it is one player.
    """
    star = player("Star", 20.0)
    others = [player("Mine2", 20.0)]

    close = matchup("Close", [star] + others, [player("T1", 20.0),
                                               player("T2", 20.0)])
    blowout = matchup("Blowout", [star] + others, [player("T1", 5.0),
                                                   player("T2", 5.0)])

    tight = by_name(ps.shares([close], FITTED))["Star"]
    lopsided = by_name(ps.shares([blowout], FITTED))["Star"]

    assert tight.stake > lopsided.stake
    assert abs(tight.rate) > abs(lopsided.rate)


def test_the_rate_is_the_derivative_of_the_win_probability():
    """``rate`` must agree with a finite difference of the probability.

    The rate is published as "what one point is worth", so it has to actually be
    that -- a closed form that has drifted from the function it differentiates
    would be invisible on the page.
    """
    m = matchup("A", [player("Mine1", 20.0), player("Mine2", 18.0)],
                [player("T1", 19.0), player("T2", 18.0)])

    step = 0.001
    before = wk.win_probability(m.mine.projected, m.mine.sd,
                                m.theirs.projected, m.theirs.sd)
    after = wk.win_probability(m.mine.projected + step, m.mine.sd,
                               m.theirs.projected, m.theirs.sd)
    expected = (after - before) / step

    share = by_name(ps.shares([m], FITTED))["Mine1"]
    assert share.rate == pytest.approx(expected, rel=1e-3)


# --- summing across leagues, which is the reason the tab exists --------------

def test_a_player_you_own_twice_sums_both_leagues():
    """Two leagues, one player, one number -- and it is bigger than either half."""
    star = player("Star", 20.0, player_id=1)
    a = matchup("A", [star, player("MineA", 15.0)],
                [player("TA1", 18.0), player("TA2", 17.0)])
    b = matchup("B", [dict(star), player("MineB", 15.0)],
                [player("TB1", 18.0), player("TB2", 17.0)])

    both = by_name(ps.shares([a, b], FITTED))["Star"]
    alone = by_name(ps.shares([a], FITTED))["Star"]

    assert both.owned == ("A", "B")
    assert both.stake == pytest.approx(sum(i.stake for i in both.interests))
    assert both.stake > alone.stake


def test_owning_and_facing_the_same_player_nets_down():
    """The case the tab was built for: on both sides, so the two cancel.

    Symmetric leagues here, so the cancellation is exact and the player is reported
    as *hedged* rather than as a direction -- which is the honest answer to "should
    I want him to score".
    """
    star = player("Star", 20.0, player_id=1)
    rest = [player("Filler", 15.0)]
    a = matchup("A", [star] + rest, [player("TA1", 18.0), player("TA2", 17.0)])
    b = matchup("B", [player("TA1", 18.0), player("TA2", 17.0)], [dict(star)] + rest)

    share = by_name(ps.shares([a, b], FITTED))["Star"]

    assert share.conflicted
    assert share.hedged
    assert share.stake == pytest.approx(0.0, abs=1e-9)
    assert share.owned == ("A",) and share.faced == ("B",)


def test_expected_wins_is_the_sum_of_the_probabilities():
    """The headline number, and the currency every stake is denominated in."""
    a = matchup("A", [player("M", 30.0)], [player("T", 20.0)])
    b = matchup("B", [player("M2", 20.0)], [player("T2", 20.0)])

    assert ps.expected_wins([a, b]) == pytest.approx(a.win + b.win)
    assert ps.expected_wins([b]) == pytest.approx(0.5)


def test_a_stake_is_the_change_in_expected_wins():
    """``stake`` must be exactly what the tab says it is.

    Recompute expected wins with the player pinned at each end of his interval and
    check the difference. This is the definition the caption gives the reader, so it
    is worth pinning rather than trusting the implementation to mean it.
    """
    star = player("Star", 20.0)
    others = [player("Mine2", 18.0)]
    opp = [player("T1", 19.0), player("T2", 18.0)]
    m = matchup("A", [star] + others, opp)

    sd = wk.player_sd(FITTED, "WR", 20.0)
    low = matchup("A", [player("Star", max(0.0, 20.0 - ps.Z * sd))] + others, opp)
    high = matchup("A", [player("Star", 20.0 + ps.Z * sd)] + others, opp)

    share = by_name(ps.shares([m], FITTED))["Star"]
    # The reference matchups re-derive each side's spread from the moved projection,
    # so compare against the probability with the *original* spread held fixed --
    # which is what `_stake` computes.
    rest_sd = max(0.0, m.mine.sd ** 2 - sd ** 2) ** 0.5
    rest_mu = m.mine.projected - 20.0
    expected = (
        wk.win_probability(rest_mu + 20.0 + ps.Z * sd, rest_sd,
                           m.theirs.projected, m.theirs.sd)
        - wk.win_probability(rest_mu + max(0.0, 20.0 - ps.Z * sd), rest_sd,
                             m.theirs.projected, m.theirs.sd))
    assert share.stake == pytest.approx(expected)
    assert low.win < high.win          # the direction the reference agrees on


# --- states that are answers rather than errors ------------------------------

def test_a_finished_player_has_no_stake_left():
    """Zero remaining projection means zero spread means nothing to root for.

    No special case does this -- ``Var = phi*mu + mu^2/k`` at a remaining
    projection of zero *is* zero. It is why the tab drains toward the players who
    still have football left.
    """
    done = player("Done", 24.0)
    done["LIVE_remaining_points"] = 0.0
    done["game_state"] = "post"

    m = matchup("A", [done, player("Live", 18.0)],
                [player("T1", 20.0), player("T2", 20.0)])
    shares_ = by_name(ps.shares([m], FITTED))

    assert shares_["Done"].stake == 0.0
    assert shares_["Live"].stake > 0.0
    # And he ranks below everyone who still has a game, however many points he
    # banked -- 24 of them here, more than anybody else on the page.
    ranked = [s.player_name for s in ps.shares([m], FITTED)]
    assert ranked[-1] == "Done"
    assert all(shares_[n].stake != 0.0 for n in ranked[:-1])


def test_no_fitted_model_means_no_stakes_and_a_points_fallback():
    """The tab must not invent a probability it cannot compute.

    ``matchup_sim.gate_note`` already tells the reader this; here it is the
    numbers agreeing with it.
    """
    star = player("Star", 20.0, player_id=1)
    a = matchup("A", [star, player("M", 10.0)],
                [player("T1", 15.0), player("T2", 15.0)], fitted=None)

    share = by_name(ps.shares([a], None))["Star"]
    assert share.stake == 0.0
    assert share.rate == 0.0
    assert ps.expected_wins([a]) is None
    assert ps.net_projected(share) == pytest.approx(20.0)


def test_net_projected_signs_the_fallback():
    """Owned minus faced, for the no-model case."""
    star = player("Star", 20.0, player_id=1)
    a = matchup("A", [star, player("M", 10.0)], [player("T", 30.0)], fitted=None)
    b = matchup("B", [player("M2", 30.0)], [dict(star), player("T2", 10.0)],
                fitted=None)

    share = by_name(ps.shares([a, b], None))["Star"]
    assert ps.net_projected(share) == pytest.approx(0.0)


def test_scoring_divergence_finds_leagues_that_disagree_about_a_point():
    """The measured version of "never compare points across leagues".

    Real: Josh Allen projects 23.27 in three of this viewer's leagues and 31.00 in
    ``gop_degenerates``, which pays six for a passing touchdown.
    """
    a = matchup("A", [player("Allen", 23.27, player_id=1)], [player("T", 20.0)])
    b = matchup("B", [player("Allen", 31.00, player_id=1)], [player("T2", 20.0)])
    same = matchup("C", [player("Even", 15.0, player_id=2)], [player("T3", 20.0)])
    d = matchup("D", [player("Even", 15.0, player_id=2)], [player("T4", 20.0)])

    found = ps.scoring_divergence(ps.shares([a, b, same, d], FITTED))

    assert [name for name, _, _ in found] == ["Allen"]
    assert found[0][1:] == pytest.approx((23.27, 31.00))


def test_a_player_with_one_league_is_never_divergent():
    """One opinion cannot disagree with itself."""
    a = matchup("A", [player("Solo", 20.0)], [player("T", 20.0)])
    assert ps.scoring_divergence(ps.shares([a], FITTED)) == []


def test_shares_are_ranked_by_what_they_move():
    """Biggest absolute stake first, regardless of sign, then by name.

    The tie-break matters on a settled slate, where every stake is zero and an
    unordered dict would reshuffle the table on every rerun.
    """
    m = matchup("A", [player("Big", 25.0), player("Small", 5.0)],
                [player("Opp", 24.0), player("Tiny", 5.0)])
    names = [s.player_name for s in ps.shares([m], FITTED)]

    assert names[0] in {"Big", "Opp"}
    assert names.index("Big") < names.index("Small")
    assert names.index("Opp") < names.index("Tiny")


def test_a_defence_with_no_id_still_collapses_across_leagues():
    """Team D/ST units match no id anywhere and fall back to the name.

    They are a real population here: the Seahawks D/ST was started in three of the
    viewer's four leagues on week 2, on both sides.
    """
    a = matchup("A", [player("Seahawks D/ST", 9.0, "D/ST", player_id=None)],
                [player("T", 20.0)])
    b = matchup("B", [player("T2", 20.0)],
                [player("Seahawks D/ST", 9.0, "D/ST", player_id=None)])

    # Force the missing-id path both rows would have in a real frame.
    a.starters[0]["player_id"] = None
    b.opp_starters[0]["player_id"] = None

    share = by_name(ps.shares([a, b], FITTED))["Seahawks D/ST"]
    assert share.conflicted
    assert len(share.interests) == 2


# --- the identity seam -------------------------------------------------------

def test_owner_for_picks_the_name_this_league_actually_has():
    """The cross-league join, and its fallback."""
    viewer = auth.Viewer("u", "Greeting Name", (), "x",
                         owner_names=("Roster Name", "Old Spelling"))

    assert auth.owner_for(viewer, ["Someone", "Roster Name"]) == "Roster Name"
    assert auth.owner_for(viewer, ["Someone", "Old Spelling"]) == "Old Spelling"
    assert auth.owner_for(viewer, ["Someone"]) is None


def test_owner_for_falls_back_to_the_display_name():
    """Every Viewer built before ``owner_names`` existed looks like this."""
    viewer = auth.Viewer("u", "Tommy Winfield", (), "x")
    assert auth.owner_for(viewer, ["Tommy Winfield", "Other"]) == "Tommy Winfield"


def test_the_default_viewer_carries_an_owner_name():
    """Without it the tab cannot say which team in a league is his."""
    assert auth.DEFAULT_VIEWER.owner_names == ("Tommy Winfield",)
    assert auth.UNRESTRICTED_VIEWER.owner_names == ("Tommy Winfield",)


def test_normal_pdf_integrates_to_the_cdf():
    """The density this tab differentiates with, checked against the CDF beside it."""
    assert wk.normal_pdf(0.0) == pytest.approx(0.3989422804)
    step = 1e-5
    for x in (-1.5, 0.0, 0.8):
        numeric = (wk.normal_cdf(x + step) - wk.normal_cdf(x - step)) / (2 * step)
        assert wk.normal_pdf(x) == pytest.approx(numeric, rel=1e-5)


# --- the page's two charts ---------------------------------------------------
#
# The layout is part of the deliverable, so the chart's *data* is pinned here.
# The rendering is Altair's problem; what it is handed is this repo's.

def _tab():
    """The view module, imported late so the logic tests do not need Streamlit."""
    from views import shares_tab
    return shares_tab


def _split(shares_):
    """The two chart subsets, the way the page builds them."""
    return ([s for s in shares_ if s.stake > 0],
            [s for s in shares_ if s.stake < 0])


def test_the_two_charts_split_by_direction_and_never_overlap():
    """A player belongs to exactly one chart, and a wash belongs to neither."""
    star, dud = player("Star", 20.0, player_id=1), player("Dud", 20.0, player_id=2)
    a = matchup("A", [star, player("M", 15.0)], [dud, player("T", 15.0)])

    good, bad = _split(ps.shares([a], FITTED))

    assert {s.player_name for s in good} & {s.player_name for s in bad} == set()
    assert "Star" in {s.player_name for s in good}
    assert "Dud" in {s.player_name for s in bad}


def test_a_chart_plots_magnitude_and_labels_it_with_that_magnitude():
    """Both charts grow rightward so their bars can be compared by length.

    The signed value survives in the tooltip field; the label matches the bar it
    sits on, which is the check that a sign error cannot hide.
    """
    tab = _tab()
    a = matchup("A", [player("Mine", 22.0), player("Mine2", 14.0)],
                [player("Theirs", 21.0), player("Theirs2", 14.0)])
    _, bad = _split(ps.shares([a], FITTED))

    data = tab._chart(bad, colour="#e34948", ink={"text": "#000", "muted": "#888",
                                                  "grid": "#eee"},
                      axis_title="x").data

    assert (data["Magnitude"] > 0).all()
    assert (data["Stake"] < 0).all()
    for stake, label in zip(data["Stake"], data["Label"]):
        assert label == f"{abs(stake):.3f}"


def test_a_chart_is_sorted_by_magnitude_descending():
    """The truncation to ten only means something if the order is the ranking."""
    tab = _tab()
    mine = [player(f"M{i}", 5.0 + i) for i in range(6)]
    a = matchup("A", mine, [player("T", 30.0)])
    good, _ = _split(ps.shares([a], FITTED))

    chart = tab._chart(good, colour="#008300",
                       ink={"text": "#000", "muted": "#888", "grid": "#eee"},
                       axis_title="x")
    magnitudes = list(chart.data["Magnitude"])
    assert magnitudes == sorted(magnitudes, reverse=True)

    # The axis order is passed explicitly rather than left to `sort="-x"`, and it
    # has to match the row order -- otherwise the label layer annotates the wrong
    # bars, which is invisible until two players have similar numbers.
    spec = chart.to_dict()
    bar = next(layer for layer in spec["layer"] if layer["mark"]["type"] == "bar")
    assert bar["encoding"]["y"]["sort"] == list(chart.data["Player"])


def test_the_chart_carries_no_colour_legend():
    """One series per chart, so the title names it and no legend is drawn.

    Also the condition under which the green/red pair is legal at all: identity
    never rests on hue, because the two directions are different charts.
    """
    tab = _tab()
    a = matchup("A", [player("M", 20.0)], [player("T", 20.0)])
    good, _ = _split(ps.shares([a], FITTED))
    spec = tab._chart(good, colour="#008300",
                      ink={"text": "#000", "muted": "#888", "grid": "#eee"},
                      axis_title="x").to_dict()

    bar = next(layer for layer in spec["layer"]
               if layer["mark"]["type"] == "bar")
    assert "color" not in bar["encoding"]          # no series encoding, no legend
    assert bar["mark"]["color"] == "#008300"       # a fixed fill instead
    assert bar["mark"]["cornerRadiusEnd"] == 4
    assert any(layer["mark"]["type"] == "text" for layer in spec["layer"])


def test_the_conflicts_table_says_the_reading_in_words():
    """A hedge must not read as a very small opinion."""
    tab = _tab()
    star = player("Star", 20.0, player_id=1)
    rest = [player("Filler", 15.0)]
    a = matchup("A", [star] + rest, [player("T1", 18.0), player("T2", 17.0)])
    b = matchup("B", [player("T1", 18.0), player("T2", 17.0)], [dict(star)] + rest)

    share = by_name(ps.shares([a, b], FITTED))["Star"]
    assert tab._verdict(share) == "A wash"

    solo = matchup("C", [player("Solo", 20.0)], [player("T", 20.0)])
    assert tab._verdict(by_name(ps.shares([solo], FITTED))["Solo"]) == "Root for"
    assert tab._verdict(by_name(ps.shares([solo], FITTED))["T"]) == "Root against"


def test_the_palette_slots_are_the_apps_own():
    """One meaning per colour across every chart in the app."""
    import draft_view as dv
    tab = _tab()
    for theme in ("light", "dark"):
        assert dv.SERIES_COLORS[theme][tab.FOR_SLOT]
        assert dv.SERIES_COLORS[theme][tab.AGAINST_SLOT]
    assert dv.SERIES_COLORS["light"][tab.FOR_SLOT] == "#008300"
    assert dv.SERIES_COLORS["light"][tab.AGAINST_SLOT] == "#e34948"
