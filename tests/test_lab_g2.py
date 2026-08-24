"""The G2 archive: the counterfactual is real, and the archive is faithful.

Two properties matter here and neither is obvious from reading the code.

**The archive must reproduce the board it claims to archive.** It re-blends from
the board's stored source columns rather than re-running the pipeline, which is the
right call -- re-hitting ESPN would archive a board built from different inputs than
the one being drafted from -- but it means a divergence between this module's
arithmetic and ``build_season_projections``' would be silent, and would not surface
until someone tried to score the archive months later.

**The counterfactual must actually be counterfactual.** A ``without_usg`` variant
that still reads ``USG_`` columns, through imputation or a stray weight, would
produce two near-identical blends and a G2 answer of "no difference" that means
nothing. The measured difference is large -- 889 of 1,026 players change position
rank -- so this is testing a property, not a hypothetical.

Synthetic frames. No network.
"""

import pandas as pd
import pytest

from Scripts.lab import g2
from Scripts.projection_utils import IMPUTED_SUFFIX, WEIGHTS, compute_weighted_stats


def frame(usg_value: float = 100.0, usg_imputed: bool = False) -> pd.DataFrame:
    """One receiver, three real sources plus a usage line."""
    return pd.DataFrame({
        "player_id": ["1"],
        "primaryPosition": ["WR"],
        "ESPN_receivingYards": [900.0],
        "FP_receivingYards": [1000.0],
        "USG_receivingYards": [usg_value],
        "FP_receivingYards" + IMPUTED_SUFFIX: [False],
        "USG_receivingYards" + IMPUTED_SUFFIX: [usg_imputed],
    })



# --- the counterfactual is real ------------------------------------------

def test_the_two_variants_differ_only_in_usg():
    """Anything else differing would confound the comparison."""
    with_usg = g2.VARIANTS["with_usg"]
    without = g2.VARIANTS["without_usg"]
    assert without["USG"] == 0.0
    assert with_usg["USG"] > 0.0
    # Both blends must be complete, or one is quietly scoring on a smaller base.
    #
    # "Complete" cannot be `sum == 1.0`, and the attempts to make it so are the history
    # of this assertion. It read `sum(...values())` until 2026-08-24, when `DST` going to
    # 0.25 broke it; summing only the universal sources fixed that and broke again hours
    # later when Pinnacle joined at 0.25 and the universal total became 1.25.
    #
    # Both failures pointed at the same thing: the absolute total is not the invariant,
    # because `compute_weighted_stats` renormalises whatever survives on a given row.
    # What has to hold is that the sources carrying weight carry it **equally** -- that
    # is what makes the blend 1/n over whoever is real, and it is the property the G2
    # counterfactual depends on, since an unequal arm would score on a different rule
    # rather than a different source set.
    for variant in (with_usg, without):
        live = {v for v in variant.values() if v > 0}
        assert len(live) == 1, f"weighted sources must weight equally: {variant}"


def test_with_usg_is_exactly_the_shipped_weighting():
    """The archived 'with' arm has to be the board that was really drafted from."""
    assert g2.VARIANTS["with_usg"] == WEIGHTS["default"]


def test_without_usg_ignores_the_usg_column_entirely():
    """Move USG a long way; the counterfactual must not move at all."""
    weights = g2.VARIANTS["without_usg"]
    low = compute_weighted_stats(frame(usg_value=10.0), ["receivingYards"],
                                 {"default": weights})
    high = compute_weighted_stats(frame(usg_value=9000.0), ["receivingYards"],
                                  {"default": weights})
    assert low["TRUE_receivingYards"][0] == pytest.approx(
        high["TRUE_receivingYards"][0])
    # And it lands on the plain ESPN/FP average, not something reweighted oddly.
    assert low["TRUE_receivingYards"][0] == pytest.approx(950.0)


def test_with_usg_does_move_when_usg_moves():
    """The mirror of the above -- otherwise the test above proves nothing."""
    weights = g2.VARIANTS["with_usg"]
    low = compute_weighted_stats(frame(usg_value=10.0), ["receivingYards"],
                                 {"default": weights})
    high = compute_weighted_stats(frame(usg_value=9000.0), ["receivingYards"],
                                  {"default": weights})
    assert low["TRUE_receivingYards"][0] != pytest.approx(
        high["TRUE_receivingYards"][0])


def test_an_imputed_usg_line_is_dropped_rather_than_weighted():
    """A usage abstention must not reach the 'with' arm as a real opinion.

    This is the failure plan 03 names: an absent source reading as agreement. If
    an abstained USG cell carried its third, the 'with' arm would be partly a copy
    of ESPN wearing a third source's badge, and G2 would be comparing a blend
    against a slightly different blend of the same two sources.
    """
    weights = g2.VARIANTS["with_usg"]
    out = compute_weighted_stats(frame(usg_value=1.0, usg_imputed=True),
                                 ["receivingYards"], {"default": weights})
    assert out["TRUE_receivingYards"][0] == pytest.approx(950.0)


# --- the archive is faithful ---------------------------------------------

def test_scoring_table_is_resolved_without_a_live_league():
    """The archive must not depend on ESPN still serving the same settings."""
    table = g2.scoring_table("winfield_football", 2026, g2.SLOT_BASE)
    assert not table.empty
    assert "colName" in table.columns and "points" in table.columns


def test_carry_columns_include_the_identity_needed_to_score_later():
    """An archive that cannot be joined to outcomes is a very tidy null result."""
    for column in ("player_id", "player_name", "primaryPosition"):
        assert column in g2.CARRY


@pytest.mark.parametrize("league_key", ["winfield_football", "knights_ffl"])
def test_reblend_reproduces_the_shipped_board(league_key):
    """The whole archive rests on this, so it is pinned rather than assumed.

    Skipped when the store has not been built -- it is gitignored and regenerable,
    so a fresh checkout legitimately has no board to compare against.
    """
    import polars as pl
    from Scripts.paths import store_dir

    path = store_dir(2026, league_key) / "board.parquet"
    if not path.is_file():
        pytest.skip(f"no board for {league_key}; run `python -m Scripts.refresh`")

    board = pl.read_parquet(path).to_pandas()
    rebuilt = g2.blend(board, league_key, 2026,
                       g2.VARIANTS["with_usg"]).set_index("player_id")
    original = board.set_index("player_id")
    shared = original.index.intersection(rebuilt.index)

    for column in ("TRUE_Points", "TRUE_receivingYards", "TRUE_rushingYards"):
        if column not in original.columns:
            continue
        left = pd.to_numeric(original.loc[shared, column], errors="coerce")
        right = pd.to_numeric(rebuilt.loc[shared, column], errors="coerce")
        both = left.notna() & right.notna()
        assert both.sum() > 100
        assert (left[both] - right[both]).abs().max() == pytest.approx(0.0, abs=1e-9)
