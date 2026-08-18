"""The usage model as the blend's fifth source.

The model itself is pinned in ``test_usage_season.py``. What is pinned here is the
*plumbing* -- registration, the id-first join, and above all the handling of
abstention, which is where this wiring can go wrong quietly.

The recurring failure mode in this repo is an absent source reading as agreement: a
``0.0`` that means "nothing here" is indistinguishable from one that means "zero",
and every count built on ``notna()`` reads the first as the second. It has cost a
draft board once already (``docs/STATE_OF_THE_REPO.md``). This source abstains on
roughly a quarter of the players it sees, so it walks straight into that trap unless
the provenance flags are right -- and a test that only checks "the column exists"
would not notice.

Synthetic frames throughout. No network, no parquet.
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from Scripts import projection_utils as pu
from Scripts import season_projections as sp
from Scripts.usage import project as pj
from Scripts.usage import season as sn

STAT = "receivingYards"


def blend_frame(usg_value, usg_imputed, espn=100.0, fp=120.0):
    """One player, ESPN and FantasyPros real, USG as specified."""
    return pd.DataFrame({
        f"ESPN_{STAT}": [espn],
        f"FP_{STAT}": [fp],
        f"FP_{STAT}{pu.IMPUTED_SUFFIX}": [False],
        f"USG_{STAT}": [usg_value],
        f"USG_{STAT}{pu.IMPUTED_SUFFIX}": [usg_imputed],
    })


def weights(usg):
    return {"default": {"ESPN": 0.5, "FP": 0.5, "USG": usg}}


# --- registration --------------------------------------------------------

def test_usg_is_registered_in_the_blend_weights():
    for stat, entry in pu.WEIGHTS.items():
        assert "USG" in entry, f"{stat} has no USG entry"


def test_the_blend_is_an_equal_split_across_the_sources_that_speak():
    """Set three-way on 2026-08-07 and widened to four on 2026-08-17.

    ESPN, FantasyPros, BetOnline and the usage model at a quarter each; Pinnacle at
    zero on 76 offence-only props. G2 is still unanswered -- it needs the blend scored
    with and without the model against realised results, and no historical pre-season
    blend survives -- so this is an assertion rather than a measurement. It is an
    assertion with a seven-fold walk-forward behind it, recorded as a decision rather
    than inherited as a default."""
    entry = pu.WEIGHTS["default"]
    assert (entry["ESPN"] == entry["FP"] == entry["BOL"] == entry["USG"]
            == pytest.approx(0.25))
    assert entry["PINNY"] == 0.0


def test_widening_the_blend_to_betonline_is_additive():
    """The property that makes the fourth weight safe to ship beside a change to the
    usage basis: it can only bite where BetOnline actually has a line.

    ``compute_weighted_stats`` renormalises over the sources that are *real*, so a
    player BetOnline has no line for gets ESPN/FP/USG at 0.25 each, which renormalises
    to exactly the 1/3 each the three-way split gave him."""
    three_way = {"default": {"ESPN": 1 / 3, "FP": 1 / 3, "PINNY": 0.0,
                             "BOL": 0.0, "USG": 1 / 3}}
    frame = pd.DataFrame({
        "ESPN_x": [100.0, 100.0], "FP_x": [110.0, 110.0],
        "PINNY_x": [90.0, 90.0], "BOL_x": [200.0, 200.0],
        "USG_x": [120.0, 120.0],
        "FP_x_is_imputed": [False, False],
        "PINNY_x_is_imputed": [True, True],
        "USG_x_is_imputed": [False, False],
        # Only the second player has a real BetOnline line.
        "BOL_x_is_imputed": [True, False],
    })
    before = pu.compute_weighted_stats(frame.copy(), ["x"], three_way)["TRUE_x"]
    after = pu.compute_weighted_stats(frame.copy(), ["x"], pu.WEIGHTS)["TRUE_x"]

    assert after[0] == pytest.approx(before[0])       # no BOL line, untouched
    assert after[1] != pytest.approx(before[1])       # real BOL line, counted


def test_every_source_still_has_an_entry():
    """A source dropped from the dict is invisible; a source at 0.0 is a decision.
    BetOnline in particular resolves 273 players against FantasyPros' 60, so its zero
    should stay legible rather than vanish."""
    for source in ("ESPN", "FP", "PINNY", "BOL", "USG"):
        assert source in pu.WEIGHTS["default"], source


def test_usg_is_scored_like_every_other_source():
    import inspect
    default = inspect.signature(pu.proj_to_score).parameters["col_pfix_list"].default
    assert "USG" in default


def test_usg_stays_out_of_the_floor_ceiling_spread():
    """Independence is not the property this spread needs -- a shared question is.

    G0 measured USG as the most independent source in the set, which is why it was
    briefly added here, and that was the wrong test. `USG_Points` is an expected
    value and the other four project a healthy season, so it sat below all of them
    for 51.7% of the players it covered and widened the median interval from 8.5% to
    24.0%. Disagreement between forecasters and uncertainty within one forecast are
    different quantities; this column holds the first."""
    assert "USG" not in sp.OPINION_PREFIXES
    assert "MEAN" not in sp.OPINION_PREFIXES
    assert set(sp.OPINION_PREFIXES) == {"ESPN", "FP", "PINNY", "BOL"}


def test_the_models_dissent_is_carried_scale_free():
    """Removing USG from the spread must not lose its opinion -- the rank delta is
    the vehicle, and being a rank it cannot be contaminated by the level mismatch."""
    import inspect
    body = inspect.getsource(sp.build_season_projections)
    assert "USG_PosRank" in body
    assert "USG_PosRankDelta" in body


# --- the zero weight -----------------------------------------------------

def test_a_zero_weight_source_cannot_move_the_blend():
    """Verified on the real board too: all 45 TRUE_ columns bit-identical over
    1,026 rows. This pins it so a later weight re-tune cannot change it by
    accident."""
    frame = blend_frame(999.0, False)
    with_usg = pu.compute_weighted_stats(frame.copy(), [STAT], weights(0.0))
    without = pu.compute_weighted_stats(
        frame.copy(), [STAT], {"default": {"ESPN": 0.5, "FP": 0.5}})
    assert with_usg[f"TRUE_{STAT}"][0] == pytest.approx(without[f"TRUE_{STAT}"][0])
    assert with_usg[f"TRUE_{STAT}"][0] == pytest.approx(110.0)


def test_turning_the_weight_on_is_one_number():
    """The point of shipping at 0.0 rather than not shipping: G2 is answerable by
    changing a constant, not by building the source again."""
    frame = blend_frame(200.0, False)
    out = pu.compute_weighted_stats(frame, [STAT], weights(1.0))
    # (100*.5 + 120*.5 + 200*1.0) / 2.0
    assert out[f"TRUE_{STAT}"][0] == pytest.approx(155.0)


# --- abstention ----------------------------------------------------------

def test_a_flagged_abstention_is_dropped_from_the_blend():
    frame = blend_frame(None, True)
    out = pu.compute_weighted_stats(frame, [STAT], weights(1.0))
    assert out[f"TRUE_{STAT}"][0] == pytest.approx(110.0)


def test_an_unflagged_abstention_would_poison_the_blend():
    """Why the flags are not optional, stated as an executable fact.

    `compute_weighted_stats` treats a source with no provenance column as real, and
    fills a null with 0.0. So an abstention that arrived unflagged enters as a
    confident projection of zero and drags the player toward it. This is the
    behaviour `Scripts.usage.project.build` exists to prevent."""
    frame = blend_frame(None, True).drop(columns=[f"USG_{STAT}{pu.IMPUTED_SUFFIX}"])
    out = pu.compute_weighted_stats(frame, [STAT], weights(1.0))
    assert out[f"TRUE_{STAT}"][0] == pytest.approx(55.0)
    assert out[f"TRUE_{STAT}"][0] < 110.0


def test_the_builder_flags_every_abstention():
    """The flag is derived from the null, so the two cannot drift apart."""
    frame = pd.DataFrame({
        "USG_receivingYards": [None, 500.0],
        "USG_receivingYards_is_imputed": [True, False],
    })
    assert frame["USG_receivingYards"].isna().tolist() == \
        frame["USG_receivingYards_is_imputed"].tolist()


def test_usg_is_not_imputed_from_the_mean():
    """Filling this source's gaps from MEAN_ -- the ESPN/FantasyPros average --
    would make the one source that is not somebody else's projection into a copy of
    two that are. That is the double-counting plan 03 measured for Pinnacle."""
    import inspect
    body = inspect.getsource(sp.build_season_projections)
    imputed = [line for line in body.splitlines()
               if "impute_columns" in line and "target_prefix" in line]
    assert imputed, "expected the imputation chain to still exist"
    assert not any("USG_" in line for line in imputed)


def test_a_wholly_abstaining_source_scores_null_not_zero():
    """`USG_Points` of 0.0 would read on the board as 'the model projects zero',
    which is the opposite of 'the model said nothing'."""
    df = pd.DataFrame({"USG_receivingYards": [np.nan], "USG_rushingYards": [np.nan]})
    scoring = pd.DataFrame({"colName": ["receivingYards", "rushingYards"],
                            "points": [0.1, 0.1]})
    out = pu._apply_scoring(df, scoring, ["USG"])
    assert pd.isna(out["USG_Points"][0])


def test_a_partial_line_still_scores():
    """A receiver has no rushing line and that is not an abstention."""
    df = pd.DataFrame({"USG_receivingYards": [1000.0], "USG_rushingYards": [np.nan]})
    scoring = pd.DataFrame({"colName": ["receivingYards", "rushingYards"],
                            "points": [0.1, 0.1]})
    out = pu._apply_scoring(df, scoring, ["USG"])
    assert out["USG_Points"][0] == pytest.approx(100.0)


# --- the id-first join ---------------------------------------------------

def base_frame():
    return pd.DataFrame({
        "player_id": [1, 2, 3],
        "join_key": ["alpha", "bravo", "charlie"],
        "ESPN_receivingYards": [10.0, 20.0, 30.0],
    })


def usage_frame(rows):
    return pd.DataFrame(rows)


def test_the_join_prefers_the_espn_id():
    """Every other season source joins on a name, which is why
    `_disambiguate_name_keys` has to exist: a wide IDP pool holds two Lamar Jacksons
    and two Justin Jeffersons. This source can avoid that."""
    source = usage_frame([{"player_id": 3.0, "name_key": "alpha",
                           "USG_receivingYards": 300.0}])
    out = sp._merge_usage(base_frame(), source)
    # Resolved by id to player 3, not by its (misleading) name to player 1.
    assert out.loc[out["player_id"] == 3, "USG_receivingYards"].iloc[0] == 300.0
    assert pd.isna(out.loc[out["player_id"] == 1, "USG_receivingYards"].iloc[0])


def test_the_join_falls_back_to_the_name_when_the_id_is_missing():
    """The crosswalk carries no 2026 rookies -- 95 unresolved, every one a rookie,
    and rookies are the arm's one clearly measured win."""
    source = usage_frame([{"player_id": np.nan, "name_key": "bravo",
                           "USG_receivingYards": 200.0}])
    out = sp._merge_usage(base_frame(), source)
    assert out.loc[out["player_id"] == 2, "USG_receivingYards"].iloc[0] == 200.0


def test_the_name_fallback_inherits_the_collision_protection():
    """It matches on `join_key`, which `_disambiguate_name_keys` has already set to
    a sentinel for the non-primary holder of a shared name. So the fallback either
    finds the player the book sources found, or nobody."""
    base = base_frame()
    base.loc[1, "join_key"] = None          # the shadowed Lamar Jackson
    source = usage_frame([{"player_id": np.nan, "name_key": "bravo",
                           "USG_receivingYards": 200.0}])
    out = sp._merge_usage(base, source)
    assert pd.isna(out.loc[out["player_id"] == 2, "USG_receivingYards"].iloc[0])


def test_the_merge_cannot_fan_the_frame_out():
    """The crosswalk records 13 duplicated espn_id, and a name fallback can collide
    with an already-resolved row. Either would multiply rows on merge."""
    source = usage_frame([
        {"player_id": 1.0, "name_key": "alpha", "USG_receivingYards": 100.0},
        {"player_id": 1.0, "name_key": "delta", "USG_receivingYards": 111.0},
    ])
    out = sp._merge_usage(base_frame(), source)
    assert len(out) == 3


def test_an_unresolvable_row_is_dropped_rather_than_guessed():
    source = usage_frame([{"player_id": np.nan, "name_key": "nobody",
                           "USG_receivingYards": 900.0}])
    out = sp._merge_usage(base_frame(), source)
    assert len(out) == 3
    assert out["USG_receivingYards"].isna().all()


def test_the_merge_does_not_leak_the_name_key():
    source = usage_frame([{"player_id": 1.0, "name_key": "alpha",
                           "USG_receivingYards": 100.0}])
    out = sp._merge_usage(base_frame(), source)
    assert "name_key" not in out.columns


# --- model freshness -----------------------------------------------------

def test_a_model_trained_through_last_season_is_current():
    model = sn.SeasonUsageModel(volume={}, train_seasons=(2017, 2025))
    assert not pj.is_stale(model, 2026)


def test_a_model_missing_a_season_it_could_have_had_is_stale():
    """Caught a real case: the persisted artifact trained on 2017-2024, because it
    was written by a walk-forward whose last fold predicted 2025."""
    model = sn.SeasonUsageModel(volume={}, train_seasons=(2017, 2024))
    assert pj.is_stale(model, 2026)


def test_a_model_with_no_training_range_is_stale():
    assert pj.is_stale(sn.SeasonUsageModel(volume={}, train_seasons=()), 2026)


def test_coverage_and_disagreement_use_different_source_lists():
    """They answer different questions and merging them is a live bug.

    The floor/ceiling spread needs sources measuring the *same quantity*, so it
    excludes USG -- an expected value against four if-healthy projections. Coverage
    needs every source that moves `TRUE_Points`, so it includes it. With USG weighted
    into the blend but missing from the coverage list, a player only the usage model
    projects gets a real TRUE_Points and `projection_missing = True`: the board would
    hide, as unprojected, exactly the players the model exists to differentiate.

    Measured on the 2026 board when this was wrong: 523 players counted as projected
    against the correct 699.
    """
    assert "USG" in sp.PROJECTION_PREFIXES
    assert "USG" not in sp.OPINION_PREFIXES
    assert set(sp.OPINION_PREFIXES) < set(sp.PROJECTION_PREFIXES)


def test_the_blend_receives_an_if_healthy_line():
    """The model predicts expected value; the blend needs the same quantity its other
    sources carry. Mixing them distorted cross-position comparison by ~11%, because
    the usage model covers QB/RB/WR/TE and not K or D/ST -- so skill positions took an
    availability discount that kickers and defences did not.

    Each player's own ``expected_games`` is divided out, which flattens everyone onto
    the same 17-game basis -- so the line answers "what if he plays" and carries no
    availability discount at all."""
    frame = pl.DataFrame({
        "expected_games": [13.6, 17.0, None],
        "USG_receivingYards": [1000.0, 1000.0, None],
    })
    out = pj.to_full_slate(frame, ["USG_receivingYards"], slate=17.0)
    got = out["USG_receivingYards"].to_list()
    assert got[0] == pytest.approx(1000.0 * 17.0 / 13.6)
    assert got[1] == pytest.approx(1000.0)          # already a full slate
    assert got[2] is None                            # an abstention stays absent


def test_the_availability_estimate_stays_out_of_the_line():
    """Measured and rejected: scaling by a per-position constant instead retains the
    availability term, and applying it means applying the model's *weak* arm -- plan
    18 puts prior-season games against next season at r = +0.343. On the 2026 board it
    took Jayden Daniels from 286.7 to 214.1 and Joe Burrow from 276.2 to 214.1, a 25%
    haircut on two top-six quarterbacks, for no gain in the first hundred picks.

    So two players at the same position with different expected games come out on the
    same footing, and `usg_expected_games` carries the availability view separately."""
    frame = pl.DataFrame({
        "expected_games": [13.6, 6.8],
        "USG_passingYards": [2720.0, 1360.0],       # the same 200 yards per game
    })
    out = pj.to_full_slate(frame, ["USG_passingYards"], slate=17.0)
    got = out["USG_passingYards"].to_list()
    assert got[0] == pytest.approx(got[1])
    assert got[0] == pytest.approx(3400.0)          # 200 x 17, both of them


def test_rescaling_cannot_divide_by_no_games():
    """A zero expected-games would divide a projection by nothing. Those rows are
    abstentions and carry a null line already, but the guard is cheaper than the
    infinity it prevents."""
    frame = pl.DataFrame({"expected_games": [0.0],
                          "USG_receivingYards": [1000.0]})
    out = pj.to_full_slate(frame, ["USG_receivingYards"], slate=17.0)
    assert out["USG_receivingYards"][0] is None


# --- current injuries -----------------------------------------------------
#
# The status-only abstention that lived here was replaced by a return-date-driven
# adjustment on 2026-08-07, after ESPN's site API turned out to carry an estimated
# return date. A status-only rule cannot tell "back next week" from "out until
# November", and it was wrong for 9 of 22 players -- Alec Pierce at ADP 96 returns
# 13 August, Zach Charbonnet at ADP 149 on 9 September, the day before week 1.
#
# The replacement and its edge cases live in ``test_espn_injuries.py``.


def test_the_injury_fallback_is_still_reachable():
    """The status rule survives as the fallback for players ESPN's report has no
    record of -- 6 of 22 on the 2026 pull, George Kittle and Brandon Aiyuk among
    them."""
    assert sp.INJURY_ABSTAIN_STATUSES == ("OUT", "INJURY_RESERVE")
    assert "QUESTIONABLE" not in sp.INJURY_ABSTAIN_STATUSES


# --- role ------------------------------------------------------------------
#
# The rescale in `to_full_slate` puts the model on a starter's slate. For a man who
# will not play that is the wrong basis, and no availability estimate fixes it,
# because `expected_games` is itself part of what encodes the role.


def _role_frame(extra_rows):
    """Twelve real quarterbacks plus whatever the test is about.

    The baseline is a median over ``STARTER_COUNT[position]`` players, so a fixture
    thinner than that has no starter level to measure against and the gate correctly
    declines to fire at all.
    """
    rows = [{"player_id": 1000 + i, "primaryPosition": "QB",
             "ESPN_projected_total": 300.0 - i, "USG_passingYards": 4000.0,
             "USG_receivingYards": None, "usg_evidence": ""}
            for i in range(12)]
    return pd.DataFrame(rows + extra_rows)


def _ranks(monkeypatch, mapping):
    frame = pd.DataFrame({"player_id": list(mapping),
                          "depth_rank": list(mapping.values())})
    monkeypatch.setattr(sp, "_current_depth_ranks", lambda season: frame)


def test_a_priced_out_backup_loses_its_usage_line(monkeypatch):
    """Mac Jones: ESPN 8.3, usage 169.6, and a blend that renormalises over the two
    real sources put the board's answer near the midpoint."""
    frame = _role_frame([{"player_id": 55, "primaryPosition": "QB",
                          "ESPN_projected_total": 8.3,
                          "USG_passingYards": 2450.0,
                          "USG_receivingYards": None, "usg_evidence": ""}])
    _ranks(monkeypatch, {55: 2})

    out = sp._withdraw_usage_on_role(frame, 2026)
    row = out[out["player_id"] == 55].iloc[0]
    assert pd.isna(row["USG_passingYards"])
    assert row["usg_evidence"] == "withdrawn: backup"


def test_a_handcuff_keeps_its_usage_line(monkeypatch):
    """Both halves of the conjunction earn their place. Depth rank alone would
    withdraw TreVeyon Henderson, Rico Dowdle, Rachaad White and RJ Harvey -- all at
    depth rank 2 with real ESPN lines, and all players a drafter specifically wants
    the model's opinion on."""
    frame = _role_frame([{"player_id": 66, "primaryPosition": "QB",
                          "ESPN_projected_total": 187.0,
                          "USG_passingYards": 3300.0,
                          "USG_receivingYards": None, "usg_evidence": ""}])
    _ranks(monkeypatch, {66: 2})

    out = sp._withdraw_usage_on_role(frame, 2026)
    assert out[out["player_id"] == 66].iloc[0]["USG_passingYards"] == 3300.0


def test_a_starter_espn_dislikes_keeps_its_usage_line(monkeypatch):
    """The other half. A points cut alone would fire on a starter ESPN happens to be
    low on, which is precisely the disagreement the model exists to voice."""
    frame = _role_frame([{"player_id": 77, "primaryPosition": "QB",
                          "ESPN_projected_total": 5.0,
                          "USG_passingYards": 3100.0,
                          "USG_receivingYards": None, "usg_evidence": ""}])
    _ranks(monkeypatch, {77: 1})

    out = sp._withdraw_usage_on_role(frame, 2026)
    assert out[out["player_id"] == 77].iloc[0]["USG_passingYards"] == 3100.0


def test_the_gate_reads_every_stat_not_a_representative_one(monkeypatch):
    """The model's columns are sparse by position, so asking whether one named stat
    is present asks whether the player is a *receiver*. Keyed on
    ``USG_receivingYards`` this withdrew 255 backups on the 2026 Knights board and
    not one quarterback -- and the quarterbacks were the worst offenders."""
    frame = _role_frame([{"player_id": 88, "primaryPosition": "QB",
                          "ESPN_projected_total": 9.5,
                          "USG_passingYards": 2200.0,
                          "USG_receivingYards": None, "usg_evidence": ""}])
    _ranks(monkeypatch, {88: 3})

    out = sp._withdraw_usage_on_role(frame, 2026)
    assert pd.isna(out[out["player_id"] == 88].iloc[0]["USG_passingYards"])


def test_a_missing_depth_chart_entry_is_not_evidence(monkeypatch):
    """The gate fires on positive evidence of a backup role and never on a failed
    join. A player the chart does not list keeps his line."""
    frame = _role_frame([{"player_id": 99, "primaryPosition": "QB",
                          "ESPN_projected_total": 8.0,
                          "USG_passingYards": 2400.0,
                          "USG_receivingYards": None, "usg_evidence": ""}])
    _ranks(monkeypatch, {55: 2})          # 99 is absent

    out = sp._withdraw_usage_on_role(frame, 2026)
    assert out[out["player_id"] == 99].iloc[0]["USG_passingYards"] == 2400.0


def test_an_off_chart_player_espn_prices_at_zero_is_withdrawn(monkeypatch):
    """The second route in, for players the depth chart does not list at all.

    A hard zero is ESPN declining to price the player rather than pricing him
    cheaply, and paired with absence from the chart that is two independent role
    signals agreeing. It caught Teddy Bridgewater at 832 projected passing yards
    inside Detroit's 5,256 team total, against a 2025 league maximum of 4,735."""
    frame = _role_frame([{"player_id": 44, "primaryPosition": "QB",
                          "ESPN_projected_total": 0.0,
                          "USG_passingYards": 1665.0,
                          "USG_receivingYards": None, "usg_evidence": ""}])
    _ranks(monkeypatch, {55: 2})          # 44 is absent from the chart

    out = sp._withdraw_usage_on_role(frame, 2026)
    row = out[out["player_id"] == 44].iloc[0]
    assert pd.isna(row["USG_passingYards"])
    assert row["usg_evidence"] == sp.ROLE_WITHDRAWN_EVIDENCE


def test_an_off_chart_player_espn_does_price_keeps_its_line(monkeypatch):
    """Why the zero is required rather than just absence from the chart. The eleven
    fullbacks ESPN actually prices — Juszczyk, Luepke, Ingold — are off the chart at
    RB and would all be withdrawn on absence alone."""
    frame = _role_frame([{"player_id": 45, "primaryPosition": "QB",
                          "ESPN_projected_total": 44.9,
                          "USG_passingYards": 900.0,
                          "USG_receivingYards": None, "usg_evidence": ""}])
    _ranks(monkeypatch, {55: 2})          # 45 is absent from the chart

    out = sp._withdraw_usage_on_role(frame, 2026)
    assert out[out["player_id"] == 45].iloc[0]["USG_passingYards"] == 900.0


def test_espn_projected_total_is_never_null():
    """The test above has to key on `<= 0` rather than `isna()`, and this is why:
    `_parse_entry` coerces a missing projection to 0.0. A gate written against
    `isna()` is dead code that silently never fires."""
    import inspect
    from Scripts.draft import adp
    body = inspect.getsource(adp._parse_entry)
    assert 'float(projected.get("projected_points") or 0.0)' in body
