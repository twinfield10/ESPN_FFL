"""Per-lineup-slot scoring: ESPN prices the same rule differently by slot.

A sack is worth one thing to a D/ST unit and another to an individual defensive
player. ``espn_api`` 0.45.1 collapses that to one number with a falsy-or, so an
override of exactly ``0.0`` falls through to the base -- the IDP value -- and the
resulting table is neither the D/ST one nor the IDP one.

These tests pin the two things that made that bug survive: the ``0.0`` case, and
the silent fallback that hid it for every completed season.

See ``docs/plans/11-per-slot-scoring.md``.
"""

import types
import warnings

import pandas as pd
import pytest

from Scripts import scoring
from Scripts.scrape_player_stats import (
    SLOT_BASE,
    SLOT_DST,
    build_scoring_rows,
    build_scoring_table,
    fetch_scoring_overrides,
    resolve_slot_points,
    scoring_slots,
)


@pytest.fixture
def registry(tmp_path, monkeypatch):
    monkeypatch.setattr(scoring, "SCORING_DIR", tmp_path)
    monkeypatch.setattr(scoring, "SCORING_CSV", tmp_path / "scoring.csv")
    scoring.reset_caches()
    yield tmp_path / "scoring.csv"
    scoring.reset_caches()


def _league(scoring_rows, overrides=None, name="Test League", year=2026, league_id=1):
    lg = types.SimpleNamespace(
        name=name, year=year, league_id=league_id,
        settings=types.SimpleNamespace(scoring_format=scoring_rows),
    )
    if overrides is not None:
        lg.scoring_overrides = overrides
    return lg


def _rule(stat_id, points, abbr="XX", label="Some Rule"):
    return {"id": stat_id, "abbr": abbr, "label": label, "points": points}


# --- the bug itself ------------------------------------------------------

def test_zero_override_beats_the_base_value():
    """The whole point. `0.0 or 12.0` is 12.0, which is how this shipped wrong."""
    by_slot = {SLOT_BASE: 12.0, SLOT_DST: 0.0}
    assert resolve_slot_points(by_slot, SLOT_DST) == 0.0


def test_base_is_used_when_the_slot_has_no_override():
    by_slot = {SLOT_BASE: 1.5}
    assert resolve_slot_points(by_slot, SLOT_DST) == 1.5


def test_base_slot_ignores_overrides_entirely():
    """An IDP scores the configured base even when D/ST is priced differently."""
    by_slot = {SLOT_BASE: 5.0, SLOT_DST: 1.0}
    assert resolve_slot_points(by_slot, SLOT_BASE) == 5.0


def test_nonzero_override_still_wins():
    by_slot = {SLOT_BASE: 6.0, SLOT_DST: 2.0}
    assert resolve_slot_points(by_slot, SLOT_DST) == 2.0


def test_a_rule_priced_at_zero_for_both_slots_stays_zero():
    assert resolve_slot_points({SLOT_BASE: 0.0, SLOT_DST: 0.0}, SLOT_DST) == 0.0


# --- reading the raw payload --------------------------------------------

class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _payload(items):
    return {"settings": {"scoringSettings": {"scoringItems": items}}}


def _item(stat_id, points, overrides=None):
    d = {"statId": stat_id, "points": points}
    if overrides is not None:
        d["pointsOverrides"] = overrides
    return d


def _patch_get(monkeypatch, payload):
    import Scripts.scrape_player_stats as sps
    monkeypatch.setattr(sps.requests, "get", lambda *a, **k: _Response(payload))


def test_overrides_are_read_including_zeros(monkeypatch):
    _patch_get(monkeypatch, _payload([_item(99, 12.0, {"16": 0.0})]))
    lg = _league([], name="L")
    lg.endpoint, lg.cookies = "http://x?", {}
    assert fetch_scoring_overrides(lg) == {99: {SLOT_BASE: 12.0, SLOT_DST: 0.0}}


def test_a_completed_season_is_wrapped_in_a_list(monkeypatch):
    """The leagueHistory endpoint returns [league], not league.

    Missing this made every pre-2026 season fall back to collapsed scoring, and
    the global warnings filter in fetch_utils meant it did so in silence.
    """
    _patch_get(monkeypatch, [_payload([_item(99, 12.0, {"16": 0.0})])])
    lg = _league([], name="L")
    lg.endpoint, lg.cookies = "http://x?", {}
    assert fetch_scoring_overrides(lg) == {99: {SLOT_BASE: 12.0, SLOT_DST: 0.0}}


def test_an_empty_history_payload_raises_rather_than_returning_nothing(monkeypatch):
    _patch_get(monkeypatch, [])
    lg = _league([], name="L")
    lg.endpoint, lg.cookies = "http://x?", {}
    with pytest.raises(ValueError, match="Empty leagueHistory"):
        fetch_scoring_overrides(lg)


def test_a_null_override_is_not_treated_as_a_value(monkeypatch):
    _patch_get(monkeypatch, _payload([_item(99, 12.0, {"16": None})]))
    lg = _league([], name="L")
    lg.endpoint, lg.cookies = "http://x?", {}
    assert fetch_scoring_overrides(lg) == {99: {SLOT_BASE: 12.0}}


def test_scoring_slots_lists_base_first():
    overrides = {99: {SLOT_BASE: 1.0, SLOT_DST: 2.0}, 53: {SLOT_BASE: 1.0}}
    assert scoring_slots(overrides) == [SLOT_BASE, SLOT_DST]


def test_scoring_slots_is_base_only_when_nothing_is_overridden():
    assert scoring_slots({53: {SLOT_BASE: 1.0}}) == [SLOT_BASE]


# --- the table ------------------------------------------------------------

def test_build_scoring_table_resolves_the_requested_slot():
    lg = _league(
        [_rule(99, 12.0, abbr="SK")],
        overrides={99: {SLOT_BASE: 12.0, SLOT_DST: 0.0}},
    )
    dst = build_scoring_table(lg, slot=SLOT_DST)
    base = build_scoring_table(lg, slot=SLOT_BASE)
    assert dst.loc[dst["source_id"] == 99, "points"].iloc[0] == 0.0
    assert base.loc[base["source_id"] == 99, "points"].iloc[0] == 12.0


def test_a_league_without_overrides_is_left_alone():
    """Eight of the nine leagues must be unaffected by any of this."""
    lg = _league([_rule(53, 1.0, abbr="REC")])
    table = build_scoring_table(lg)
    assert table.loc[table["source_id"] == 53, "points"].iloc[0] == 1.0


def test_modelled_rates_still_win_over_configured_ones():
    """REPL_SCORING rewrites 'every N yards' rules; a slot must not undo that."""
    lg = _league([_rule(8, 1.0, abbr="PY4")], overrides={8: {SLOT_BASE: 1.0}})
    table = build_scoring_table(lg, slot=SLOT_BASE)
    row = table[table["source_id"] == 8].iloc[0]
    assert row["points"] == 0.04 and row["id"] == 3


def test_build_scoring_rows_emits_one_block_per_slot():
    lg = _league(
        [_rule(99, 12.0, abbr="SK"), _rule(53, 1.0, abbr="REC")],
        overrides={99: {SLOT_BASE: 12.0, SLOT_DST: 0.0}, 53: {SLOT_BASE: 1.0}},
    )
    rows = build_scoring_rows(lg)
    assert sorted(rows["slot"].unique()) == [SLOT_DST, SLOT_BASE]
    assert len(rows) == 4


def test_build_scoring_rows_warns_once_not_once_per_slot():
    """Coverage is a property of the rule set, so N slots must not mean N warnings."""
    lg = _league(
        [_rule(9999, 5.0, abbr="???")],
        overrides={9999: {SLOT_BASE: 5.0, SLOT_DST: 1.0}},
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        build_scoring_rows(lg)
    coverage = [w for w in caught
                if w.category.__name__ == "ScoringCoverageWarning"]
    assert len(coverage) == 1


def test_a_league_with_no_overrides_still_yields_one_slot_block():
    rows = build_scoring_rows(_league([_rule(53, 1.0)]))
    assert list(rows["slot"].unique()) == [SLOT_DST]


# --- resolution through the registry ------------------------------------

def _store(registry_path, rows, league_key="some_league", season=2026):
    df = pd.DataFrame(rows)
    df["season"] = season
    df["league_key"] = league_key
    df["league_name"] = "Test League"
    df["recorded_at"] = "2026-08-05T00:00:00"
    scoring._write_registry(df[scoring.REGISTRY_COLUMNS])


def _reg_row(source_id, points, col_name, slot, abbr="XX"):
    return {"source_id": source_id, "id": source_id, "slot": slot,
            "abbr": abbr, "label": "L", "points": points, "colName": col_name}


def test_registry_round_trips_a_zero_override(registry):
    """The value that used to be lost must survive a write and a read."""
    _store(registry, [
        _reg_row(99, 12.0, "defensiveSacks", SLOT_BASE),
        _reg_row(99, 0.0, "defensiveSacks", SLOT_DST),
    ])
    dst = scoring.get_scoring_table(league_key="some_league", season=2026, slot=SLOT_DST)
    base = scoring.get_scoring_table(league_key="some_league", season=2026, slot=SLOT_BASE)
    assert dst["points"].iloc[0] == 0.0
    assert base["points"].iloc[0] == 12.0


def test_a_missing_slot_falls_back_to_the_dst_rows(registry):
    """A league that prices nothing per-slot has only SLOT_DST rows."""
    _store(registry, [_reg_row(53, 1.0, "receivingReceptions", SLOT_DST)])
    table = scoring.get_scoring_table(
        league_key="some_league", season=2026, slot=SLOT_BASE)
    assert table["points"].iloc[0] == 1.0


def test_a_registry_written_before_slots_existed_still_reads(registry, monkeypatch):
    """Backfill to SLOT_DST: that is what a collapsed value meant."""
    pd.DataFrame([{
        "season": 2026, "league_key": "some_league", "league_name": "T",
        "source_id": 53, "id": 53, "abbr": "REC", "label": "L",
        "points": 1.0, "colName": "receivingReceptions",
        "recorded_at": "2026-08-01T00:00:00",
    }]).to_csv(registry, index=False)
    scoring.reset_caches()
    table = scoring.get_scoring_table(league_key="some_league", season=2026)
    assert table["points"].iloc[0] == 1.0


def test_diff_does_not_report_overridden_rules_as_repriced(registry):
    """Keying a diff on source_id alone pairs base against D/ST for every rule."""
    for season in (2025, 2026):
        _store(registry, [
            _reg_row(99, 12.0, "defensiveSacks", SLOT_BASE),
            _reg_row(99, 0.0, "defensiveSacks", SLOT_DST),
        ], season=season)
    assert scoring.diff_scoring("some_league", 2025, 2026).empty


def test_coverage_gaps_counts_a_rule_once_across_slots(registry):
    _store(registry, [
        _reg_row(9999, 5.0, None, SLOT_BASE),
        _reg_row(9999, 1.0, None, SLOT_DST),
    ])
    assert len(scoring.coverage_gaps()) == 1


# --- scoring the projections --------------------------------------------

def _proj_frame():
    return pd.DataFrame({
        "primaryPosition": ["WR", "LB", "D/ST"],
        "league_id": [1, 1, 1],
        "ESPN_defensiveSacks": [0.0, 2.0, 3.0],
        "TRUE_defensiveSacks": [0.0, 2.0, 3.0],
    })


def _stub_tables(monkeypatch, dst_points, base_points):
    """Return a different scoring table per requested slot."""
    import Scripts.projection_utils as pu

    def fake(league=None, **kw):
        pts = base_points if kw.get("slot") == SLOT_BASE else dst_points
        return pd.DataFrame([{
            "id": 99, "abbr": "SK", "label": "Sack", "points": pts,
            "source_id": 99, "colName": "defensiveSacks",
        }])

    monkeypatch.setattr(pu, "get_scoring_table", fake)


def test_only_the_dst_unit_scores_from_the_override(monkeypatch):
    """ESPN keys the override on the D/ST slot, so every other slot takes base."""
    from Scripts.projection_utils import proj_to_score
    _stub_tables(monkeypatch, dst_points=1.0, base_points=5.0)
    out = proj_to_score(_proj_frame(), s_league=None, col_pfix_list=["ESPN"])
    by_pos = out.set_index("primaryPosition")["ESPN_Points"]
    assert by_pos["LB"] == 2.0 * 5.0      # IDP slot: base
    assert by_pos["D/ST"] == 3.0 * 1.0    # unit: slot-16 override
    assert by_pos["WR"] == 0.0            # no stats, so base changes nothing


def test_an_offensive_player_is_scored_from_base_not_the_dst_rate(monkeypatch):
    """A WR's stray imputed defensive stats must not price at the D/ST rate."""
    from Scripts.projection_utils import proj_to_score
    _stub_tables(monkeypatch, dst_points=1.0, base_points=5.0)
    frame = pd.DataFrame({
        "primaryPosition": ["WR", "D/ST"],
        "ESPN_defensiveSacks": [0.1, 3.0],
    })
    out = proj_to_score(frame, s_league=None, col_pfix_list=["ESPN"])
    by_pos = out.set_index("primaryPosition")["ESPN_Points"]
    assert by_pos["WR"] == pytest.approx(0.1 * 5.0)
    assert by_pos["D/ST"] == pytest.approx(3.0 * 1.0)


def test_a_frame_with_no_dst_unit_takes_the_single_table_path(monkeypatch):
    from Scripts.projection_utils import proj_to_score
    _stub_tables(monkeypatch, dst_points=1.0, base_points=5.0)
    frame = _proj_frame()
    frame = frame[frame["primaryPosition"] != "D/ST"]
    out = proj_to_score(frame, s_league=None, col_pfix_list=["ESPN"])
    assert out.set_index("primaryPosition")["ESPN_Points"]["LB"] == 2.0 * 5.0


def test_true_points_comes_from_the_blend_not_a_hardcoded_average(monkeypatch):
    """The IDP branch used to set TRUE_Points = (ESPN + BOL) / 2 by hand."""
    from Scripts.projection_utils import proj_to_score
    _stub_tables(monkeypatch, dst_points=1.0, base_points=5.0)
    out = proj_to_score(_proj_frame(), s_league=None, col_pfix_list=["ESPN", "TRUE"])
    lb = out[out["primaryPosition"] == "LB"].iloc[0]
    # TRUE_defensiveSacks * base rate, not an average of ESPN and BOL.
    assert lb["TRUE_Points"] == 2.0 * 5.0


# --- sparse sources: a missing stat is 0, an absent source is NaN --------

def test_a_stat_the_source_did_not_project_scores_zero_not_nan(monkeypatch):
    """The season path is sparse, and summing straight through collapsed it.

    A running back has no ``ESPN_passingYards``, and passing yards is a scored
    rule in all nine leagues, so one NaN cell made ``ESPN_Points`` NaN -- which is
    why every per-source point column on every stored draft board was NaN for
    every row, 1,026 of 1,026.
    """
    import Scripts.projection_utils as pu

    def fake(league=None, **kw):
        return pd.DataFrame([
            {"id": 1, "abbr": "PY", "label": "Pass Yds", "points": 0.04,
             "source_id": 1, "colName": "passingYards"},
            {"id": 2, "abbr": "RY", "label": "Rush Yds", "points": 0.1,
             "source_id": 2, "colName": "rushingYards"},
        ])

    monkeypatch.setattr(pu, "get_scoring_table", fake)
    frame = pd.DataFrame({
        "primaryPosition": ["RB"],
        "ESPN_passingYards": [float("nan")],
        "ESPN_rushingYards": [1000.0],
    })
    out = pu.proj_to_score(frame, s_league=None, col_pfix_list=["ESPN"])
    assert out["ESPN_Points"].iloc[0] == pytest.approx(100.0)


def test_a_source_with_no_line_at_all_stays_nan(monkeypatch):
    """NaN, not 0.0 -- a book with no line is not a book projecting zero.

    Guards the failure mode plan 03 documents: an absent source reading as
    agreement rather than absence.
    """
    import Scripts.projection_utils as pu

    def fake(league=None, **kw):
        return pd.DataFrame([{"id": 1, "abbr": "RY", "label": "Rush Yds",
                              "points": 0.1, "source_id": 1,
                              "colName": "rushingYards"}])

    monkeypatch.setattr(pu, "get_scoring_table", fake)
    frame = pd.DataFrame({
        "primaryPosition": ["RB", "RB"],
        "ESPN_rushingYards": [500.0, float("nan")],
    })
    out = pu.proj_to_score(frame, s_league=None, col_pfix_list=["ESPN"])
    assert out["ESPN_Points"].iloc[0] == pytest.approx(50.0)
    assert pd.isna(out["ESPN_Points"].iloc[1])


def test_a_dense_frame_scores_exactly_as_before(monkeypatch):
    """The weekly path is dense, so the fix must be a no-op there.

    Verified beyond this unit test: recomputing every prefix over all nine
    leagues' stored 2025 ``lineups.parquet`` gives identical totals before and
    after, max absolute difference 0.0.
    """
    import Scripts.projection_utils as pu
    _stub_tables(monkeypatch, dst_points=1.0, base_points=5.0)
    out = pu.proj_to_score(_proj_frame(), s_league=None, col_pfix_list=["ESPN"])
    assert out.set_index("primaryPosition")["ESPN_Points"].to_dict() == {
        "WR": 0.0, "LB": 10.0, "D/ST": 3.0,
    }


# --- volume in the blend, plan 34 ----------------------------------------

def test_blending_volume_cannot_change_any_points_total():
    """An unscored ``TRUE_`` column must reach no ``*_Points`` total.

    This is the whole safety argument for carrying volume through the blend:
    ``_apply_scoring`` iterates the *scoring table*, not the stat list, so a
    blended column the league does not price contributes nothing. Verified over
    all nine 2026 boards at max |delta| 0.0 when the change landed; pinned here
    on a frame small enough to read, because the property is what makes the
    change safe rather than merely observed to be safe so far.
    """
    import pandas as pd

    from Scripts import projection_utils as pu

    scoring = pd.DataFrame({
        "colName": ["passingYards", "passingTouchdowns"],
        "points": [0.04, 4.0],
    })
    frame = pd.DataFrame({
        "primaryPosition": ["QB", "QB"],
        "ESPN_passingYards": [4000.0, 3500.0],
        "ESPN_passingTouchdowns": [30.0, 22.0],
        "ESPN_passingAttempts": [560.0, 480.0],
        "FP_passingYards": [4200.0, 3400.0],
        "FP_passingTouchdowns": [31.0, 21.0],
        "FP_passingAttempts": [575.0, 470.0],
    })

    scored_only = pu.compute_weighted_stats(frame.copy(), ["passingYards",
                                                           "passingTouchdowns"],
                                            pu.WEIGHTS)
    with_volume = pu.compute_weighted_stats(
        frame.copy(), pu.blended_stats(["passingYards", "passingTouchdowns"]),
        pu.WEIGHTS)

    pu._apply_scoring(scored_only, scoring, ["ESPN", "FP", "TRUE"])
    pu._apply_scoring(with_volume, scoring, ["ESPN", "FP", "TRUE"])

    assert "TRUE_passingAttempts" in with_volume.columns
    assert "TRUE_passingAttempts" not in scored_only.columns
    for prefix in ("ESPN", "FP", "TRUE"):
        column = f"{prefix}_Points"
        pd.testing.assert_series_equal(scored_only[column], with_volume[column])


def test_the_blend_list_drops_an_unmapped_scoring_rule():
    """Plan 01's NaN ``colName`` must not become a ``TRUE_nan`` column.

    It used to: ``compute_weighted_stats`` was handed the raw ``colName`` list,
    found no ``ESPN_nan`` to read, and wrote an all-zero ``TRUE_nan``. Harmless in
    its value and not harmless in what it did to ``_apply_scoring``'s
    ``scored_any``, which is the flag that tells an absent source from a source
    projecting zero.
    """
    from Scripts.projection_utils import blended_stats

    assert "nan" not in blended_stats(["passingYards", float("nan")])


def test_the_blend_list_never_repeats_a_stat():
    """Two leagues score ``rushingAttempts``; it must not be blended twice."""
    from Scripts.projection_utils import blended_stats

    out = blended_stats(["rushingAttempts", "passingYards"])
    assert len(out) == len(set(out))
    assert out.count("rushingAttempts") == 1
