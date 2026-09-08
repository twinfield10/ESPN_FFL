"""The projection blend primitives, and the no-duplicate-definitions guarantee."""

import ast
import json
import warnings

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


def test_a_points_column_is_not_counted_as_coverage():
    """A derived output is not a line, and counting it invented coverage.

    ``<SRC>_Points`` is computed *from* a stat line, so it has no ``MEAN_``
    counterpart to be imputed from and therefore no provenance flag -- which
    dropped it into ``coverage_report``'s ``notna()`` branch at 100%. Measured on
    the 2026 weekly stores on 2026-09-08, the sidebar reported **Pinnacle 4.1% and
    BetOnline 4.1% for two sources with no weekly line at all**, and 4.1% is
    exactly 2/49 columns: ``_Points`` and ``_PosRank``. Every genuine stat column
    was 0.0%.
    """
    df = pd.DataFrame({
        "PINNY_rushingYards": [1.0, 2.0],
        "PINNY_rushingYards_is_imputed": [True, True],
        "PINNY_Points": [10.0, 20.0],
        "PINNY_PosRank": [1, 2],
    })
    rep = pu.coverage_report(df, sources=("PINNY",))
    assert set(rep["stat"]) == {"rushingYards"}
    assert rep["real_pct"].mean() == 0.0


def test_present_prefixes_and_coverage_agree_on_what_is_derived():
    """The two used to name ``Points`` and ``PosRank`` in separate literals."""
    assert pu.DERIVED_SOURCE_COLUMNS == ("Points", "PosRank")
    df = pd.DataFrame({"FP_Points": [1.0], "FP_PosRank": [1]})
    assert pu.present_prefixes(df, candidates=("FP",)) == []
    assert pu.coverage_report(df, sources=("FP",)).empty


# --- the coverage population ---------------------------------------------

def _population_frame():
    """Two rostered players, twenty-five free-agent receivers, three defenders."""
    rows = [{"week": 1, "team_owner": "Tommy", "primaryPosition": "WR",
             "ESPN_Points": 5.0, "player_name": "Rostered WR"},
            {"week": 1, "team_owner": "Tommy", "primaryPosition": "LB",
             "ESPN_Points": 4.0, "player_name": "Rostered LB"}]
    rows += [{"week": 1, "team_owner": pu.FREE_AGENT_OWNER, "primaryPosition": "WR",
              "ESPN_Points": float(i), "player_name": f"FA WR {i}"}
             for i in range(25)]
    rows += [{"week": 1, "team_owner": pu.FREE_AGENT_OWNER, "primaryPosition": "LB",
              "ESPN_Points": float(i), "player_name": f"FA LB {i}"}
             for i in range(3)]
    return pd.DataFrame(rows)


def test_coverage_population_keeps_every_rostered_player():
    out = pu.coverage_population(_population_frame())
    rostered = out[out["team_owner"] != pu.FREE_AGENT_OWNER]
    assert rostered["player_name"].tolist() == ["Rostered WR"]


def test_coverage_population_keeps_the_best_free_agents_per_position():
    """Twenty, ranked by ESPN's projection -- not the first twenty in the frame."""
    out = pu.coverage_population(_population_frame())
    pool = out[out["team_owner"] == pu.FREE_AGENT_OWNER]
    assert len(pool) == 20
    assert pool["ESPN_Points"].min() == 5.0  # the top 20 of 0..24


def test_coverage_population_drops_idp_positions():
    """One league of ten rosters individual defenders, and no source but ESPN
    publishes a line for them, so grading FantasyPros against them measured it on
    players it does not cover."""
    out = pu.coverage_population(_population_frame())
    assert not out["primaryPosition"].isin(pu.IDP_POSITIONS).any()
    assert "Rostered LB" not in out["player_name"].tolist()


def test_coverage_population_ranks_free_agents_within_each_week():
    """``lineups.parquet`` gains a week every Tuesday, and a global top-20 would
    apply week 1's twenty to every week after it."""
    frame = pd.DataFrame([
        {"week": w, "team_owner": pu.FREE_AGENT_OWNER, "primaryPosition": "WR",
         "ESPN_Points": float(i), "player_name": f"w{w} wr{i}"}
        for w in (1, 2) for i in range(25)
    ])
    out = pu.coverage_population(frame)
    assert out.groupby("week").size().to_dict() == {1: 20, 2: 20}


def test_coverage_population_is_a_no_op_without_an_owner_column():
    """The property that makes this safe on the season path.

    ``coverage_report`` is also called by ``print_coverage_report`` inside
    ``build_season_projections``, where the question genuinely is about the whole
    market -- 940 of a 1,036-row board are free agents.
    """
    frame = pd.DataFrame({"primaryPosition": ["WR"] * 3, "ESPN_Points": [1.0, 2.0, 3.0]})
    assert len(pu.coverage_population(frame)) == 3


def test_coverage_population_never_raises_on_a_thin_frame():
    """A coverage annotation must not be able to take a store write down."""
    assert len(pu.coverage_population(pd.DataFrame())) == 0
    only_pool = pd.DataFrame({"team_owner": [pu.FREE_AGENT_OWNER] * 3,
                              "primaryPosition": ["WR"] * 3})
    assert len(pu.coverage_population(only_pool, free_agents_per_position=2)) == 2


# --- what counts as a real line ------------------------------------------

def test_source_contributed_ignores_a_structural_zero():
    """A kicker's ``FP_passingYards`` is 0.0 and unflagged: nobody imputed it and
    nobody asserted it either. Counting those made FantasyPros real for Cameron
    Dicker on the strength of twelve zeros."""
    df = pd.DataFrame({"FP_passingYards": [0.0, 30.0]})
    assert pu.source_contributed(df, "FP", ["passingYards"]).tolist() == [False, True]


def test_source_contributed_ignores_an_imputed_cell():
    df = pd.DataFrame({"FP_rushingYards": [60.0, 60.0],
                       "FP_rushingYards_is_imputed": [False, True]})
    assert pu.source_contributed(df, "FP", ["rushingYards"]).tolist() == [True, False]


def test_source_contributed_falls_back_to_points_without_a_stat_column():
    """``tests/test_lineup.py``'s ``one_real_source`` and ``two_real_sources``
    fixtures carry no stat columns at all, and the Sheets renderer passes frames
    of the same shape."""
    df = pd.DataFrame({"FP_Points": [12.0, None]})
    assert pu.source_contributed(df, "FP", ["rushingYards"]).tolist() == [True, False]
    assert pu.source_contributed(
        df, "FP", ["rushingYards"], points_fallback=False).tolist() == [False, False]


def test_player_coverage_counts_players_rather_than_cells():
    """The two answer different questions, and the label claimed the first.

    Measured on the 2026 stores: FantasyPros read 12.4% of cells and had a real
    line for 21.8% of the players in the league, because the cell average divides
    by 45-odd stats most sources never publish.
    """
    df = pd.DataFrame({
        "TRUE_rushingYards": [1.0, 1.0, 1.0, 1.0],
        "TRUE_receivingYards": [1.0, 1.0, 1.0, 1.0],
        "FP_rushingYards": [60.0, 0.0, 0.0, 0.0],
        "FP_rushingYards_is_imputed": [False, False, False, False],
        "FP_receivingYards": [0.0, 0.0, 0.0, 0.0],
        "FP_receivingYards_is_imputed": [False, False, False, False],
    })
    cells = pu.coverage_report(df, sources=("FP",))["real_pct"].mean()
    players = pu.player_coverage(df, sources=("FP",)).iloc[0]
    # Every flag is False, so every cell is "not imputed" and the cell metric reads
    # a clean 100% -- for a source that said something about exactly one of the four
    # players. That gap is the whole reason this function exists.
    assert cells == pytest.approx(100.0)
    assert players["real_pct"] == pytest.approx(25.0)   # 1 of 4 players


def test_player_coverage_excludes_the_derived_columns():
    """Leaving them in is what made a first draft report 100% for every source."""
    df = pd.DataFrame({
        "TRUE_rushingYards": [1.0], "TRUE_Points": [10.0], "TRUE_PosRank": [1],
        "PINNY_rushingYards": [50.0], "PINNY_rushingYards_is_imputed": [True],
        "PINNY_Points": [10.0], "PINNY_PosRank": [1],
    })
    assert pu.player_coverage(df, sources=("PINNY",)).iloc[0]["real_pct"] == 0.0


def test_player_coverage_is_empty_rather_than_raising():
    assert pu.player_coverage(pd.DataFrame()).empty
    assert list(pu.player_coverage(pd.DataFrame()).columns) == [
        "source", "players", "real", "real_pct"]


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


# --- a weekly source has to be usable, not merely present ----------------

def _weekly_file(tmp_path, name, weeks, age_hours=0.0):
    """Write a weekly-shaped parquet and age it."""
    import os
    import time

    path = tmp_path / name
    pd.DataFrame({"week": list(weeks),
                  "player_name": [f"p{w}" for w in weeks]}).to_parquet(path)
    when = time.time() - age_hours * 3600
    os.utime(path, (when, when))
    return path


def test_a_stale_weekly_file_is_not_reported_as_present(tmp_path, monkeypatch):
    """It used to mean `Path.exists()`, and that reported a dead source as live.

    On 2026-09-08 this returned ``fantasypros: True`` off a file holding 60 rows for
    week 1 written 2026-08-03 -- the anonymous ten-per-position teaser, 25 days
    stale, scraped three weeks before the account that lifts the fence existed.
    """
    fresh = _weekly_file(tmp_path, "fresh.parquet", [1], age_hours=1.0)
    stale = _weekly_file(tmp_path, "stale.parquet", [1], age_hours=25 * 24)
    monkeypatch.setattr(pu, "WEEKLY_SOURCE_FILES",
                        {"fresh": lambda s: fresh, "stale": lambda s: stale})
    assert pu.weekly_sources_present(2026, week=1) == {"fresh": True, "stale": False}


def test_a_weekly_file_with_no_rows_for_this_week_is_not_present(tmp_path,
                                                                 monkeypatch):
    """A week-1 file says nothing about week 5, and the blend would impute it away
    anyway -- but the app would have shown a FantasyPros column of ESPN means."""
    path = _weekly_file(tmp_path, "wk1.parquet", [1], age_hours=1.0)
    monkeypatch.setattr(pu, "WEEKLY_SOURCE_FILES", {"fp": lambda s: path})
    assert pu.weekly_sources_present(2026, week=1) == {"fp": True}
    assert pu.weekly_sources_present(2026, week=5) == {"fp": False}


def test_no_week_means_any_week(tmp_path, monkeypatch):
    """Preserves the old meaning for a caller that does not know the week --
    ``Scripts.refresh`` passes None on a run with no live league."""
    path = _weekly_file(tmp_path, "wk1.parquet", [1], age_hours=1.0)
    monkeypatch.setattr(pu, "WEEKLY_SOURCE_FILES", {"fp": lambda s: path})
    assert pu.weekly_sources_present(2026) == {"fp": True}


def test_an_absent_weekly_file_is_absent_rather_than_an_error(tmp_path, monkeypatch):
    monkeypatch.setattr(pu, "WEEKLY_SOURCE_FILES",
                        {"fp": lambda s: tmp_path / "nope.parquet"})
    assert pu.weekly_sources_present(2026, week=1) == {"fp": False}


def test_the_source_presence_keys_are_the_props_feeds_only():
    """TOMCAT is deliberately not here.

    This dict drives the app's "no weekly props this season for X" caption, and
    TOMCAT is not a feed that went quiet -- it is a head nobody has built. Listing
    it would put a model into a sentence about sportsbooks and make the key set
    unstable for every consumer.
    """
    assert set(pu.WEEKLY_SOURCE_FILES) == {"fantasypros", "pinnacle", "betonline"}


# --- the weekly TOMCAT seam ----------------------------------------------
#
# Follows `tests/test_usage_fifth_source.py`, this repo's template for adding a
# source: registration, abstention, and what must stay out. Built 2026-09-08 as a
# seam only -- no weekly head writes a file yet (docs/plans/19).

def test_the_weekly_prefix_list_carries_usg():
    """So shipping a weekly arm is a data change, not a change to this module."""
    assert "USG" in pu.WEEKLY_PREFIXES


def test_usg_is_dropped_before_scoring_while_it_has_no_stat_columns():
    """The reason listing the prefix is safe.

    Plan 34 added `present_prefixes` after `USG_Points` was written null for all
    3,602 rows of every 2025 weekly store: a column shaped like a source that never
    has an opinion reads as a source that agreed.
    """
    frame = pd.DataFrame({"ESPN_rushingYards": [10.0], "USG_Points": [5.0]})
    assert pu.present_prefixes(frame, candidates=pu.WEEKLY_PREFIXES) == ["ESPN"]


def test_usg_stays_out_of_the_weights_because_that_dict_is_shared():
    """A weekly entry would re-admit TOMCAT to the *draft board* as a side effect.

    It was withdrawn from the season blend on 2026-09-07 on a level error -- its
    projected league runs the ball 384 times a team against a realised 450-465. See
    docs/plans/43-tomcat-out-of-season-blend.md.
    """
    assert "USG" not in pu.WEIGHTS["default"], (
        "TOMCAT is back in the blend; docs/plans/43 and the frozen G2 archive both "
        "need re-reading before that ships")


def test_a_missing_weekly_usage_file_abstains_rather_than_raising():
    """The `clean_pinny` / `clean_bol` contract, which plan 19 step 5 asks it to
    follow -- including the absent-source path."""
    with pytest.warns(pu.MissingProjectionSourceWarning):
        frame = pu.clean_usage_weekly(season=UNSCRAPED_SEASON)
    assert frame.empty
    assert list(frame.columns) == pu.SOURCE_JOIN_KEYS


def test_clean_usage_weekly_requires_a_season_or_a_path():
    with pytest.raises(ValueError, match="season"):
        pu.clean_usage_weekly()


def test_a_weekly_usage_line_is_read_from_an_explicit_path(tmp_path):
    """The path the loader takes once a head exists, exercised now so it is not
    first exercised on the day one ships."""
    path = tmp_path / "Usage_WeeklyProjections.parquet"
    pd.DataFrame({"week": [1], "player_name": ["A"],
                  "proj_rushingYards": [80.0]}).to_parquet(path)
    out = pu.clean_usage_weekly(usage_path=path)
    assert out["proj_rushingYards"].tolist() == [80.0]


def test_an_unmatched_usage_row_abstains_rather_than_projecting_zero():
    """TOMCAT is flagged where it is null, never filled from `MEAN_`.

    Filling the one source that is not derived from the others -- G0 measured its
    residual independence at +0.832 against FantasyPros' +0.988 -- from an average
    of two of them would count those two a third time, which is the double-count
    plan 03 exists to have measured.
    """
    frame = pd.DataFrame({
        "ESPN_rushingYards": [100.0],
        "FP_rushingYards": [120.0],
        "USG_rushingYards": [float("nan")],
        "USG_rushingYards_is_imputed": [True],
    })
    out = pu.compute_weighted_stats(
        frame, ["rushingYards"],
        {"default": {"ESPN": 0.25, "FP": 0.25, "USG": 0.25}})
    assert out["TRUE_rushingYards"].iloc[0] == pytest.approx(110.0)   # not 73.3


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


# --- source freshness -----------------------------------------------------
#
# Both books sat thirteen days stale on the 2026 draft board while every other
# source refreshed nightly, and nothing anywhere said so: the loaders only ever
# asked whether a file *existed*. Under an equal-vote blend that is a stale
# opinion carrying a fifth of the projection, not a missing column, so it is
# invisible by construction. See docs/plans/36-sportsbook-scrapes.md.


def _aged_file(tmp_path, hours):
    """A file whose mtime is *hours* in the past."""
    import os
    import time
    p = tmp_path / "source.parquet"
    p.write_text("x")
    past = time.time() - hours * 3600
    os.utime(p, (past, past))
    return p


def test_a_fresh_source_does_not_warn(tmp_path):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pu.check_source_freshness("Book", _aged_file(tmp_path, 3), "run the thing")
    assert not [w for w in caught
                if issubclass(w.category, pu.StaleProjectionSourceWarning)]


def test_a_stale_source_warns_and_names_the_fix(tmp_path):
    """The warning has to carry the command, or it is a nag rather than a fix."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pu.check_source_freshness("Pinnacle season props",
                                  _aged_file(tmp_path, 13 * 24),
                                  "python -m Scripts.scrape_pinnacle_season")
    stale = [w for w in caught
             if issubclass(w.category, pu.StaleProjectionSourceWarning)]
    assert len(stale) == 1
    msg = str(stale[0].message)
    assert "Pinnacle season props" in msg
    assert "13.0 days" in msg
    assert "python -m Scripts.scrape_pinnacle_season" in msg


def test_one_missed_nightly_is_not_stale(tmp_path):
    """The threshold is two days, not one. A single skipped run is not an
    emergency, and a warning that fires every time it rains gets filtered out."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pu.check_source_freshness("Book", _aged_file(tmp_path, 26), "run it")
    assert not caught
    assert pu.STALE_AFTER_HOURS == 48.0


def test_a_missing_file_is_missing_rather_than_stale(tmp_path):
    """Absent and stale are different findings with different fixes, and the
    loaders report absence themselves before ever asking about age."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        age = pu.check_source_freshness("Book", tmp_path / "nope.parquet", "run it")
    assert age is None
    assert not caught


def test_the_stale_warning_survives_the_global_filter(tmp_path):
    """Scripts/fetch_utils.py calls warnings.filterwarnings("ignore") at module
    scope, which would swallow a plain warnings.warn -- the exact failure mode
    this warning exists to prevent. Mirrors the missing-source guard above."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.filterwarnings("ignore")
        pu.check_source_freshness("Book", _aged_file(tmp_path, 200), "run it")
    assert len(caught) == 1
    assert issubclass(caught[0].category, pu.StaleProjectionSourceWarning)
