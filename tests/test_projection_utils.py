"""The projection blend primitives, and the no-duplicate-definitions guarantee."""

import ast
import json

import pandas as pd
import pytest

from Scripts import projection_utils as pu
from Scripts.paths import REPO_ROOT

# The functions that used to exist as two drifting copies.
SHARED = {
    "change_col_prefix", "impute_columns", "create_mean_cols", "clean_pinny",
    "clean_bol", "get_match_details", "compute_weighted_stats", "proj_to_score",
    "clean_lineups", "check_week", "get_league_projections", "get_rankings",
}


def _notebook_code_cells():
    nb = json.loads((REPO_ROOT / "FF Analysis Notebook.ipynb").read_text())
    for i, cell in enumerate(nb["cells"]):
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"])
        # comment out ipython magics so ast can parse
        yield i, "\n".join(
            ("# " + ln) if ln.lstrip().startswith(("!", "%")) else ln
            for ln in src.splitlines()
        )


def _top_level_defs(source):
    return {n.name for n in ast.parse(source).body if isinstance(n, ast.FunctionDef)}


def test_module_defines_all_shared_functions():
    assert SHARED <= set(dir(pu))


def test_notebook_does_not_redefine_shared_functions():
    """The core regression guard: 8 of these 12 had drifted between the notebook
    and populateGoogleSheet.py, so the two computed different projections."""
    offenders = {}
    for i, src in _notebook_code_cells():
        dupes = _top_level_defs(src) & SHARED
        if dupes:
            offenders[i] = sorted(dupes)
    assert not offenders, f"shared functions redefined in notebook cells: {offenders}"


def test_driver_script_does_not_redefine_shared_functions():
    src = (REPO_ROOT / "populateGoogleSheet.py").read_text()
    assert not (_top_level_defs(src) & SHARED)


def test_notebook_cells_all_parse():
    for i, src in _notebook_code_cells():
        try:
            ast.parse(src)
        except SyntaxError as e:
            pytest.fail(f"notebook cell {i} does not parse: {e}")


# --- blend primitives ----------------------------------------------------

def test_change_col_prefix():
    df = pd.DataFrame({"proj_rushingYards": [1.0], "week": [1]})
    out = pu.change_col_prefix(df.copy(), old_pfix="proj", new_pfix="ESPN")
    assert "ESPN_rushingYards" in out.columns


def test_impute_columns_fills_only_missing():
    df = pd.DataFrame({
        "ESPN_rushingYards": [100.0, 50.0],
        "FP_rushingYards": [90.0, None],
    })
    out = pu.impute_columns(df.copy(), target_prefix="FP_", source_prefix="ESPN_")
    assert out["FP_rushingYards"].tolist() == [90.0, 50.0]


def _identity_cols(n=1):
    return {
        "week": [1] * n,
        "player_name": [f"Player {i}" for i in range(n)],
        "primaryPosition": ["RB"] * n,
        "player_active_status": ["active"] * n,
    }


def test_create_mean_cols_averages_sources():
    df = pd.DataFrame({
        **_identity_cols(),
        "ESPN_rushingYards": [100.0],
        "FP_rushingYards": [50.0],
    })
    out = pu.create_mean_cols(df.copy(), target_prefix="FP_", source_prefix="ESPN_")
    assert out["MEAN_rushingYards"].iloc[0] == pytest.approx(75.0)


def test_create_mean_cols_returns_only_identity_and_mean_columns():
    """It is a projection step, not an annotation step -- the source columns are
    dropped, which is why it is always merged back onto the base frame."""
    df = pd.DataFrame({
        **_identity_cols(),
        "ESPN_rushingYards": [100.0],
        "FP_rushingYards": [50.0],
    })
    out = pu.create_mean_cols(df.copy(), target_prefix="FP_", source_prefix="ESPN_")
    assert list(out.columns) == [
        "week", "player_name", "primaryPosition", "player_active_status",
        "MEAN_rushingYards",
    ]


def test_create_mean_cols_skips_stats_only_one_source_has():
    df = pd.DataFrame({
        **_identity_cols(),
        "ESPN_rushingYards": [100.0],
        "FP_rushingYards": [50.0],
        "ESPN_receivingYards": [30.0],   # no FP counterpart
    })
    out = pu.create_mean_cols(df.copy(), target_prefix="FP_", source_prefix="ESPN_")
    assert "MEAN_rushingYards" in out.columns
    assert "MEAN_receivingYards" not in out.columns


def test_compute_weighted_stats_uses_per_stat_weights():
    df = pd.DataFrame({
        "ESPN_passingTouchdowns": [2.0],
        "FP_passingTouchdowns": [1.0],
        "ESPN_rushingYards": [100.0],
        "FP_rushingYards": [0.0],
    })
    weights = {
        "passingTouchdowns": {"ESPN": 0.5, "FP": 0.5},
        "default": {"ESPN": 1.0, "FP": 0.0},
    }
    out = pu.compute_weighted_stats(
        df.copy(), stats_list=["passingTouchdowns", "rushingYards"],
        weights_dict=weights,
    )
    assert out["TRUE_passingTouchdowns"].iloc[0] == pytest.approx(1.5)
    assert out["TRUE_rushingYards"].iloc[0] == pytest.approx(100.0)   # default weights


def test_compute_weighted_stats_renormalises_over_absent_sources():
    """A source missing from the frame must not scale the projection down.

    This used to assert 5.0, documenting the old behaviour: an absent source was
    skipped but its weight stayed in the divisor implicitly, so a 10-yard
    projection came out as 5 purely because Pinnacle was not in the frame. The
    remaining weights are renormalised now, so the one source that does have data
    carries it.
    """
    df = pd.DataFrame({"ESPN_rushingYards": [10.0]})
    out = pu.compute_weighted_stats(
        df.copy(), stats_list=["rushingYards"],
        weights_dict={"default": {"ESPN": 0.5, "PINNY": 0.5}},
    )
    assert out["TRUE_rushingYards"].iloc[0] == pytest.approx(10.0)


def test_compute_weighted_stats_face_value_mode_is_the_old_behaviour():
    """renormalise=False is kept so the change can be A/B'd against history."""
    df = pd.DataFrame({"ESPN_rushingYards": [10.0]})
    out = pu.compute_weighted_stats(
        df.copy(), stats_list=["rushingYards"],
        weights_dict={"default": {"ESPN": 0.5, "PINNY": 0.5}},
        renormalise=False,
    )
    assert out["TRUE_rushingYards"].iloc[0] == pytest.approx(5.0)


# --- provenance ----------------------------------------------------------

def test_impute_columns_flags_what_it_filled():
    df = pd.DataFrame({
        "ESPN_rushingYards": [100.0, 50.0],
        "FP_rushingYards": [90.0, None],
    })
    out = pu.impute_columns(df.copy(), target_prefix="FP_", source_prefix="ESPN_")
    assert out["FP_rushingYards_is_imputed"].tolist() == [False, True]


def test_impute_columns_flags_a_wholly_absent_target():
    df = pd.DataFrame({"ESPN_rushingYards": [100.0]})
    out = pu.impute_columns(df.copy(), target_prefix="PINNY_", source_prefix="ESPN_")
    assert out["PINNY_rushingYards"].tolist() == [100.0]
    assert out["PINNY_rushingYards_is_imputed"].tolist() == [True]


def test_impute_flags_accumulate_across_calls():
    """impute_columns runs twice per source in clean_lineups -- once on the merged
    frame, once on base to catch rows that never joined. A cell imputed by either
    call must stay flagged."""
    df = pd.DataFrame({
        "MEAN_rushingYards": [10.0, 20.0],
        "PINNY_rushingYards": [None, 99.0],
    })
    out = pu.impute_columns(df.copy(), target_prefix="PINNY_", source_prefix="MEAN_")
    assert out["PINNY_rushingYards_is_imputed"].tolist() == [True, False]
    # second pass: nothing left to fill, flags must not reset
    out2 = pu.impute_columns(out, target_prefix="PINNY_", source_prefix="MEAN_")
    assert out2["PINNY_rushingYards_is_imputed"].tolist() == [True, False]


def test_imputed_sources_lose_their_weight():
    """The core fix: a filled-in book line must not count as an independent
    opinion. Here Pinnacle is imputed, so ESPN and FP split the weight."""
    df = pd.DataFrame({
        "ESPN_rushingYards": [100.0],
        "FP_rushingYards": [80.0],
        "PINNY_rushingYards": [90.0],
        "PINNY_rushingYards_is_imputed": [True],
    })
    out = pu.compute_weighted_stats(
        df.copy(), stats_list=["rushingYards"],
        weights_dict={"default": {"ESPN": 0.5, "FP": 0.25, "PINNY": 0.25}},
    )
    # (100*0.5 + 80*0.25) / 0.75
    assert out["TRUE_rushingYards"].iloc[0] == pytest.approx(93.3333333, rel=1e-6)


def test_real_sources_are_unaffected_by_renormalisation():
    """A player every source covers must get exactly the old number."""
    df = pd.DataFrame({
        "ESPN_rushingYards": [100.0],
        "FP_rushingYards": [80.0],
        "PINNY_rushingYards": [60.0],
        "ESPN_rushingYards_is_imputed": [False],
        "FP_rushingYards_is_imputed": [False],
        "PINNY_rushingYards_is_imputed": [False],
    })
    weights = {"default": {"ESPN": 0.5, "FP": 0.25, "PINNY": 0.25}}
    new = pu.compute_weighted_stats(df.copy(), ["rushingYards"], weights)
    old = pu.compute_weighted_stats(df.copy(), ["rushingYards"], weights,
                                    renormalise=False)
    assert new["TRUE_rushingYards"].iloc[0] == pytest.approx(
        old["TRUE_rushingYards"].iloc[0])
    assert new["TRUE_rushingYards"].iloc[0] == pytest.approx(85.0)


def test_all_sources_imputed_falls_back_to_face_value():
    """Denominator zero must not become a zero projection."""
    df = pd.DataFrame({
        "ESPN_rushingYards": [100.0],
        "ESPN_rushingYards_is_imputed": [True],
    })
    out = pu.compute_weighted_stats(
        df.copy(), stats_list=["rushingYards"],
        weights_dict={"default": {"ESPN": 0.4}},
    )
    assert out["TRUE_rushingYards"].iloc[0] == pytest.approx(40.0)


def test_coverage_report_counts_real_cells():
    df = pd.DataFrame({
        "ESPN_rushingYards": [1.0, 2.0, 3.0, 4.0],
        "PINNY_rushingYards": [1.0, 2.0, 3.0, 4.0],
        "PINNY_rushingYards_is_imputed": [True, True, True, False],
    })
    rep = pu.coverage_report(df, sources=("ESPN", "PINNY"))
    pinny = rep[rep["source"] == "PINNY"].iloc[0]
    assert pinny["real"] == 1 and pinny["real_pct"] == 25.0
    espn = rep[rep["source"] == "ESPN"].iloc[0]
    assert espn["real"] == 4 and espn["real_pct"] == 100.0


# --- season scoping ------------------------------------------------------

def test_projection_files_are_season_scoped():
    for fn in (pu.fantasypros_parquet, pu.pinnacle_parquet, pu.betonline_parquet):
        assert "2025" in fn(2025).parts
        assert "2026" in fn(2026).parts
        assert fn(2025) != fn(2026)


def test_loaders_require_a_season_or_an_explicit_path():
    with pytest.raises(ValueError, match="season"):
        pu.clean_bol()
    with pytest.raises(ValueError, match="season"):
        pu.clean_pinny()
