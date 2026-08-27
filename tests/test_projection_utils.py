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


# --- absent weekly sources -----------------------------------------------
#
# Weekly Pinnacle and BetOnline props do not exist until the season starts, so
# every August `clean_lineups` hit an unguarded read_parquet and died. That is
# exactly the state the local app launches in -- see
# docs/plans/07-frontend-foundation.md.

#: A season far enough out that no scrape will ever have written a file for it.
UNSCRAPED_SEASON = 2999


@pytest.mark.parametrize("loader,label", [(pu.clean_pinny, "Pinnacle"),
                                          (pu.clean_bol, "BetOnline")])
def test_absent_season_file_degrades_instead_of_raising(loader, label):
    with pytest.warns(pu.MissingProjectionSourceWarning, match=label):
        out = loader(season=UNSCRAPED_SEASON)
    assert out.empty
    assert list(out.columns) == pu.SOURCE_JOIN_KEYS


@pytest.mark.parametrize("loader", [pu.clean_pinny, pu.clean_bol])
def test_absent_frame_merges_onto_an_int_week_column(loader):
    """The empty frame's dtypes are set explicitly because pandas validates merge
    key dtypes even when one side is empty -- object vs int64 would raise."""
    with pytest.warns(pu.MissingProjectionSourceWarning):
        absent = loader(season=UNSCRAPED_SEASON)
    left = pd.DataFrame({"week": [1], "player_name": ["A"], "MEAN_rushingYards": [10.0]})
    merged = left.merge(absent, on=pu.SOURCE_JOIN_KEYS, how="left")
    assert len(merged) == 1


@pytest.mark.parametrize("loader,kwarg", [(pu.clean_pinny, "pinny_path"),
                                         (pu.clean_bol, "bol_path")])
def test_an_explicit_missing_path_still_raises(loader, kwarg, tmp_path):
    """A named file that is not there is a typo, not an absent season. Silently
    returning empty would hide it."""
    with pytest.raises(FileNotFoundError):
        loader(**{kwarg: tmp_path / "nope.parquet"})


def test_absent_source_becomes_fully_imputed_and_drops_out_of_the_blend():
    """The measured pre-season path, in miniature.

    With no Pinnacle file, `impute_columns` creates PINNY_ from MEAN_ and flags
    every cell, then `compute_weighted_stats` renormalises over what is real. The
    result is the ESPN/FP blend at full strength rather than a book-weighted
    number backed by no book.
    """
    df = pd.DataFrame({
        **_identity_cols(),
        "ESPN_rushingYards": [100.0],
        "FP_rushingYards": [50.0],
        "MEAN_rushingYards": [75.0],
    })
    with pytest.warns(pu.MissingProjectionSourceWarning):
        absent = pu.clean_pinny(season=UNSCRAPED_SEASON)

    merged = df.merge(absent, on=pu.SOURCE_JOIN_KEYS, how="left")
    merged = pu.impute_columns(merged, target_prefix="PINNY_", source_prefix="MEAN_")
    assert merged["PINNY_rushingYards"].iloc[0] == pytest.approx(75.0)
    assert bool(merged["PINNY_rushingYards_is_imputed"].iloc[0])

    out = pu.compute_weighted_stats(
        merged, stats_list=["rushingYards"],
        weights_dict={"default": {"ESPN": 0.2, "FP": 0.3, "PINNY": 0.5}},
    )
    # ESPN and FP renormalised over 0.5 total weight: (100*0.2 + 50*0.3) / 0.5
    assert out["TRUE_rushingYards"].iloc[0] == pytest.approx(70.0)


def test_get_match_details_tolerates_a_source_with_no_data(capsys):
    """It indexes check_col2 to count misses, which KeyErrored on an empty
    source frame and took the whole blend down with it."""
    df1 = pd.DataFrame({
        **_identity_cols(),
        "MEAN_rushingYards": [10.0],
    })
    pu.get_match_details(df1=df1, df2=pd.DataFrame(columns=pu.SOURCE_JOIN_KEYS),
                         keys=pu.SOURCE_JOIN_KEYS, check_col2="PINNY_receivingYards",
                         tbl_lab="Pinnacle Sportsbook Table", min_wk=1)
    assert "no data for this season" in capsys.readouterr().out


def test_weekly_sources_present_reports_each_file():
    present = pu.weekly_sources_present(UNSCRAPED_SEASON)
    assert present == {"fantasypros": False, "pinnacle": False, "betonline": False}
    assert set(pu.weekly_sources_present(2025)) == {
        "fantasypros", "pinnacle", "betonline"}


# --- the touchdown allocation --------------------------------------------
#
# A sportsbook prices *any* scrimmage touchdown and this pipeline carries a rushing
# column and a receiving one, so something has to allocate it. BetOnline sent 100%
# to rushing for every back -- 988 of 995 RB player-weeks carried
# `BOL_receivingTouchdowns == 0` -- and Pinnacle split by yardage share, which needs
# both yardage columns and so gave a pure receiver nothing at all. Split by the
# ESPN/FantasyPros consensus, the blend's RB calibration moves from 0.597 to 0.897
# on receiving and 1.099 to 1.022 on rushing. See docs/plans/34-stat-first-audit.md
# F2 and `Scripts.market.allocate_touchdowns`.

def _td_frame():
    """Three players: a back the books mis-split, a passer, and a bench receiver
    the consensus has no opinion on."""
    return pd.DataFrame({
        "player_name": ["back", "passer", "bench"],
        "ESPN_rushingTouchdowns": [0.40, 0.15, 0.0],
        "ESPN_receivingTouchdowns": [0.10, 0.00, 0.0],
        "FP_rushingTouchdowns": [0.40, 0.15, 0.0],
        "FP_receivingTouchdowns": [0.10, 0.00, 0.0],
        # BetOnline's crude split: everything on rushing.
        "BOL_rushingTouchdowns": [0.60, 0.20, 0.30],
        "BOL_receivingTouchdowns": [0.00, 0.00, 0.00],
    })


def test_reallocate_book_touchdowns_uses_the_consensus_ratio():
    out = pu.reallocate_book_touchdowns(_td_frame())
    back = out.iloc[0]
    assert back["BOL_rushingTouchdowns"] == pytest.approx(0.48)   # 0.60 * 0.8
    assert back["BOL_receivingTouchdowns"] == pytest.approx(0.12)


def test_reallocate_book_touchdowns_preserves_each_book_total():
    """It changes *which* stat, never how many. All nine leagues score both types at
    6, so this is worth exactly zero points and every bit of its value is in being
    right about the stat line."""
    before = _td_frame()
    after = pu.reallocate_book_touchdowns(before.copy())
    for frame in (before, after):
        frame["total"] = (frame["BOL_rushingTouchdowns"]
                          + frame["BOL_receivingTouchdowns"])
    assert after["total"].tolist() == pytest.approx(before["total"].tolist())


def test_reallocate_book_touchdowns_leaves_a_passer_alone():
    out = pu.reallocate_book_touchdowns(_td_frame())
    passer = out.iloc[1]
    assert passer["BOL_rushingTouchdowns"] == pytest.approx(0.20)
    assert passer["BOL_receivingTouchdowns"] == 0.0


def test_reallocate_book_touchdowns_keeps_the_book_split_where_consensus_is_silent():
    """The consensus projects this player nothing, so there is no ratio. The book's
    own guess stands rather than a made-up one."""
    out = pu.reallocate_book_touchdowns(_td_frame())
    bench = out.iloc[2]
    assert bench["BOL_rushingTouchdowns"] == pytest.approx(0.30)
    assert bench["BOL_receivingTouchdowns"] == 0.0


def test_reallocate_book_touchdowns_does_not_touch_the_consensus():
    """ESPN and FantasyPros are the ruler, not the thing being measured."""
    before = _td_frame()
    after = pu.reallocate_book_touchdowns(before.copy())
    for column in ("ESPN_rushingTouchdowns", "ESPN_receivingTouchdowns",
                   "FP_rushingTouchdowns", "FP_receivingTouchdowns"):
        assert after[column].tolist() == before[column].tolist()


def test_reallocate_book_touchdowns_without_a_consensus_is_a_no_op():
    """A frame built without ESPN or FantasyPros has no ratio to split by, and
    inventing one would be worse than each book's own guess."""
    frame = _td_frame().drop(columns=[
        "ESPN_rushingTouchdowns", "ESPN_receivingTouchdowns",
        "FP_rushingTouchdowns", "FP_receivingTouchdowns"])
    out = pu.reallocate_book_touchdowns(frame.copy())
    assert out["BOL_rushingTouchdowns"].tolist() == [0.60, 0.20, 0.30]


def test_reallocate_book_touchdowns_skips_a_book_that_is_absent():
    """Pinnacle covers a third of the players BetOnline does; a missing pair of
    columns is normal and must not raise."""
    out = pu.reallocate_book_touchdowns(_td_frame())
    assert "PINNY_rushingTouchdowns" not in out.columns


def test_reallocate_book_touchdowns_runs_inside_the_weekly_blend():
    """The wiring, not the arithmetic: `clean_lineups` must call this before
    `compute_weighted_stats`, or the blend averages the uncorrected columns."""
    source = (REPO_ROOT / "Scripts" / "projection_utils.py").read_text()
    body = source[source.index("def clean_lineups("):]
    called = body.index("reallocate_book_touchdowns(base)")
    blended = body.index("compute_weighted_stats(df=base")
    assert called < blended
