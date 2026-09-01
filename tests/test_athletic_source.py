"""The Athletic as the sixth blended source.

Modelled on ``test_usage_fifth_source.py``, which is this repo's template for adding
a source: registration, the one real parse bug, abstention, and what must stay out.

The distinguishing property here is that the source is a **file somebody saved**
rather than a scrape, so the tests that matter most are the ones about what happens
when it is absent, stale, or carries a row it should not.
"""

import numpy as np
import pandas as pd
import pytest

import Scripts.load_athletic as la
import Scripts.projection_utils as pu
import Scripts.refresh_status as rs
import Scripts.season_projections as sp


# --- registration --------------------------------------------------------

def test_ath_is_registered_at_an_equal_vote():
    """0.25, the same as every other external. The equal-vote rule is the invariant;
    `test_usage_fifth_source` holds the general form of it and this pins the value."""
    entry = pu.WEIGHTS["default"]
    assert entry["ATH"] == 0.25
    assert {entry[k] for k in ("ESPN", "FP", "PINNY", "BOL", "ATH", "USG")} == {0.25}


def test_ath_is_scored_like_every_other_source():
    import inspect
    default = inspect.signature(pu.proj_to_score).parameters["col_pfix_list"].default
    assert "ATH" in default


def test_ath_counts_toward_coverage_and_toward_the_spread():
    """Both, and for different reasons -- see the two docstrings on those tuples.

    In ``PROJECTION_PREFIXES`` because it moves ``TRUE_Points``, so a player only it
    projects must not read as unprojected. In ``OPINION_PREFIXES`` because it answers
    the same question the other externals do, which is the membership rule there.
    """
    assert "ATH" in sp.PROJECTION_PREFIXES
    assert "ATH" in sp.OPINION_PREFIXES


def test_ath_is_withdrawn_on_availability_like_the_other_player_keyed_sources():
    assert "ATH_" in sp.AVAILABILITY_WITHDRAWN_PREFIXES


def test_the_source_is_named_in_the_freshness_manifest():
    """It has no nightly stage, which is *why* this matters: nothing else would ever
    notice the file going stale, and it carries a sixth of every projection it covers.
    """
    named = {name for name, _, _ in rs.PROJECTION_SOURCES}
    assert "The Athletic" in named
    resolve = next(r for n, r, _ in rs.PROJECTION_SOURCES if n == "The Athletic")
    path = resolve(2999)
    assert "2999" in str(path) and path.suffix == ".parquet"
    assert not path.parent.exists(), "resolving a path must not create a directory"


def test_ath_has_a_column_on_the_board():
    """A source that moves `Us` must be readable beside it, and before it."""
    import sys
    sys.path.insert(0, "app")
    import draft_view as dv

    labels = [c.label for c in dv.COLUMNS if c.group == "Points"]
    assert "ATH" in labels
    assert labels.index("ATH") < labels.index("Us")
    spec = next(c for c in dv.COLUMNS if c.source == "ATH_Points")
    assert spec.positions == (), "it covers four positions, so it is not scoped"
    assert spec.source_of and spec.how and spec.caveat


# --- the parse -----------------------------------------------------------

def test_the_stat_map_uses_espn_names_matching_fantasypros():
    """Both sources must land on identical ``<PREFIX>_<stat>`` columns or the blend
    is comparing two different vocabularies."""
    assert set(la.STAT_COLUMNS.values()) == {
        "passingAttempts", "passingCompletions", "passingYards",
        "passingTouchdowns", "passingInterceptions",
        "rushingAttempts", "rushingYards", "rushingTouchdowns",
        "receivingTargets", "receivingReceptions", "receivingYards",
        "receivingTouchdowns",
    }
    assert "lostFumbles" not in la.STAT_COLUMNS.values(), (
        "the workbook does not project fumbles; ATH_ must abstain rather than "
        "carry a column of zeroes")


def test_all_thirty_two_team_tabs_are_read():
    assert len(la.TEAM_TABS) == 32
    assert len(set(la.TEAM_TABS)) == 32
    # ESPN's abbreviations, not nflverse's -- so no alias map is needed on this path.
    assert {"WSH", "JAX", "LV", "LAR", "LAC"} <= set(la.TEAM_TABS)
    assert "WAS" not in la.TEAM_TABS and "LA" not in la.TEAM_TABS


def test_a_quarterback_cannot_carry_receiving_stats():
    """The one real bug in the file, as an executable fact.

    The workbook splits team target share across a tab's rows and on the New Orleans
    tab some of it lands on Spencer Rattler, a third-string quarterback: 32.2 targets
    and 258.7 receiving yards. Read straight he scores 59.8 instead of 7.8, which
    would make him a real opinion in the blend.
    """
    assert "receivingYards" not in la.POSITION_STATS["QB"]
    assert "receivingReceptions" not in la.POSITION_STATS["QB"]
    assert "receivingTargets" not in la.POSITION_STATS["QB"]
    # And the converse: no skill position may carry passing stats.
    for pos in ("RB", "WR", "TE"):
        assert not (la.POSITION_STATS[pos] & {
            "passingYards", "passingTouchdowns", "passingAttempts"})


def test_tight_ends_are_not_given_carries():
    """Not defensive coding -- the same share model that produced the Rattler row
    could allocate rush share to a tight end on the next download."""
    assert "rushingAttempts" not in la.POSITION_STATS["TE"]


def test_defence_and_kicker_are_not_ingested():
    """The workbook's DST tab defines all seven points-allowed tiers in `Settings`
    and leaves the bucket columns null for all 32 teams, so its own defence values
    silently omit that component. This repo's DST model is blended at 0.25 instead."""
    assert set(la.POSITION_STATS) == {"QB", "RB", "WR", "TE"}
    assert "D/ST" not in la.POSITION_STATS and "K" not in la.POSITION_STATS


def test_the_masked_stats_flag_stays_out_of_the_blend_namespace():
    """``UPPER_`` is reserved for blendable numerics -- ``compute_weighted_stats`` and
    ``proj_to_score`` scan every uppercase prefix and require it to be numeric."""
    for col in la.DIAGNOSTIC_COLUMNS:
        assert col.islower(), col
        assert not col.startswith(la.PREFIX)


# --- absence and abstention ---------------------------------------------

def test_a_missing_workbook_degrades_rather_than_raising(tmp_path, monkeypatch,
                                                         capsys):
    """Every other loader returns an empty frame with a fix hint when its file is
    absent, and ``build_season_projections`` skips it. A hand-dropped file is the one
    most likely to be missing, so this is the path that must not raise."""
    monkeypatch.setattr(sp, "season_dir",
                        lambda *a, **k: tmp_path / "nope.parquet")
    out = sp.load_theathletic_season(2026)
    assert out.empty and list(out.columns) == ["name_key"]
    assert "load_athletic" in capsys.readouterr().out


def test_an_unmatched_player_abstains_rather_than_projecting_zero():
    """``ATH_`` is in the imputation chain so its gaps arrive flagged. Without a
    provenance column an unmatched row enters as a confident projection of zero and
    drags the player toward it -- the trap ``test_usage_fifth_source`` names.
    """
    frame = pd.DataFrame({
        "ESPN_rushingYards": [100.0],
        "FP_rushingYards": [120.0],
        "ATH_rushingYards": [np.nan],
    })
    frame = pu.impute_columns(frame, target_prefix="ATH_", source_prefix="MEAN_")
    if "ATH_rushingYards_is_imputed" not in frame.columns:
        frame["ATH_rushingYards_is_imputed"] = True
    weights = {"default": {"ESPN": 0.5, "FP": 0.5, "ATH": 0.5}}
    out = pu.compute_weighted_stats(frame, ["rushingYards"], weights)
    assert out["TRUE_rushingYards"][0] == pytest.approx(110.0)


def test_a_real_line_gets_its_equal_vote():
    frame = pd.DataFrame({
        "ESPN_rushingYards": [100.0],
        "FP_rushingYards": [100.0],
        "ATH_rushingYards": [400.0],
        "ESPN_rushingYards_is_imputed": [False],
        "FP_rushingYards_is_imputed": [False],
        "ATH_rushingYards_is_imputed": [False],
    })
    weights = {"default": {"ESPN": 0.25, "FP": 0.25, "ATH": 0.25}}
    out = pu.compute_weighted_stats(frame, ["rushingYards"], weights)
    assert out["TRUE_rushingYards"][0] == pytest.approx(200.0)


# --- the real file, when it is there -------------------------------------

def test_the_shipped_file_parses_to_offence_only():
    """Skipped where the file is absent -- it is a manual download, so CI may not
    have one. Where it is present, these are the numbers the import printed."""
    from Scripts.paths import season_dir

    path = season_dir("TheAthletic", 2026, la.FILENAME, create=False)
    if not path.exists():
        pytest.skip("no 2026 Athletic workbook imported")

    df = pd.read_parquet(path)
    assert set(df["position"]) == {"QB", "RB", "WR", "TE"}
    assert df["player_name"].is_unique
    qb = df[df["position"] == "QB"]
    assert qb["receivingYards"].isna().all(), (
        "a quarterback with receiving yards means the position mask regressed")
    assert df["passingYards"].notna().sum() == len(qb)
