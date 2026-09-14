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


def test_the_variants_match_the_manifest_they_were_archived_under():
    """The archived arms have to be the weights the archive was actually taken under.

    This asserted ``VARIANTS["with_usg"] == WEIGHTS["default"]`` until 2026-09-07, when
    TOMCAT was withdrawn from the blend. A live read is exactly the wrong thing here:
    with no ``USG`` entry the two variants collapse to the same blend, and a re-archive
    would overwrite the one artifact in this repo that cannot be rebuilt with a pair
    that answers nothing. ``VARIANTS`` is now pinned literally, and the manifest of the
    archive already on disk is what it must agree with."""
    import json
    from Scripts.paths import DATA_DIR

    manifest = json.loads((DATA_DIR / "G2" / "2026" / "manifest.json").read_text())
    assert manifest["variants"] == {name: dict(weights)
                                    for name, weights in g2.VARIANTS.items()}
    assert g2.VARIANTS["with_usg"]["USG"] > 0.0
    assert "USG" not in WEIGHTS["default"], (
        "TOMCAT is back in the blend; this file's pinned VARIANTS and the frozen "
        "archive both need re-reading before that ships")


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

    **It reproduces exactly, and the residual this used to tolerate was a bug in
    the lab rather than a property of the blend.**

    The story is worth keeping because the wrong diagnosis survived two rounds of
    loosening. ``reconcile_team_totals`` ties ``passingCompletions`` to
    ``receivingReceptions``, and a reblend used to land 1.53 off on receptions and
    6.08 on completions while every yardage stat came back to the bit. That split
    was read as the identity having more than one fixed point -- yards converging
    from either starting state and receptions not -- and the bound was set an
    order of magnitude above the miss to catch it growing. It grew: by 2026-09-14
    the two were **9.69 and 35.96**, sixfold in a fortnight, and the rank check
    tipped over on Jahan Dotson moving WR88 -> WR83.

    Reconcile is idempotent. It was never the composite.
    ``build_season_projections`` blends ``blended_stats(stats)`` -- the scored
    columns **plus** ``VOLUME_STATS`` -- and :func:`Scripts.lab.g2.blend` was
    blending the bare scored list, 43 stats against the pipeline's 47. The four it
    skipped include ``passingCompletions``, which no league here scores and which
    the tail nonetheless reconciles. So the tail ran on a mixture: receptions
    freshly blended from the sources, completions still holding the board's
    already-reconciled, already-redistributed value. Reconcile took the midpoint of
    one fresh side and one finished side -- not the midpoint the board was built
    from -- and dragged both to it.

    Yardage was exact throughout because both its sides are scored everywhere, so
    both were always reblended. **That asymmetry was the evidence all along**, and
    it was read as a fact about the stats rather than about which of them the lab
    happened to recompute.

    The residual grew because the board is rebuilt nightly and in-season source
    movement pulled the stale completions further from a fresh blend of them, which
    is also why this failed now rather than in August.

    So the bound is back where it belongs: every reconciled column, and
    ``TRUE_Points`` with them, reproduces to float precision. A residual here is
    once again a real regression rather than something to be characterised.

    Skipped when the store has not been built -- it is gitignored and regenerable,
    so a fresh checkout legitimately has no board to compare against.
    """
    import polars as pl
    from Scripts.paths import store_dir
    from Scripts.season_projections import reconcile_team_totals

    path = store_dir(2026, league_key) / "board.parquet"
    if not path.is_file():
        pytest.skip(f"no board for {league_key}; run `python -m Scripts.refresh`")

    board = pl.read_parquet(path).to_pandas()
    # **Production weights, not `VARIANTS["with_usg"]`.** What this test checks is that
    # `g2.blend` reproduces the pipeline, so it has to reblend under whatever the
    # shipped board was actually built from. The two were the same thing until
    # 2026-09-07; since TOMCAT's withdrawal `with_usg` is a frozen historical weighting
    # and reblending under it would measure the archive's age rather than this code.
    rebuilt = g2.blend(board, league_key, 2026,
                       dict(WEIGHTS["default"])).set_index("player_id")
    original = board.set_index("player_id")
    shared = original.index.intersection(rebuilt.index)

    def drift(column):
        left = pd.to_numeric(original.loc[shared, column], errors="coerce")
        right = pd.to_numeric(rebuilt.loc[shared, column], errors="coerce")
        both = left.notna() & right.notna()
        assert both.sum() > 100, f"{column} has too few comparable rows to mean anything"
        return (left[both] - right[both]).abs().max()

    # **Every reconciled column, to float precision.** Both halves of each identity
    # pair, not just the side every league happens to score -- the one this used to
    # skip is exactly where the bug lived.
    for column in ("TRUE_passingYards", "TRUE_rushingYards", "TRUE_receivingYards",
                   "TRUE_passingTouchdowns", "TRUE_receivingTouchdowns",
                   "TRUE_receivingReceptions", "TRUE_passingCompletions"):
        if column in original.columns:
            assert drift(column) == pytest.approx(0.0, abs=1e-9), column

    # And the number every league actually reads.
    assert drift("TRUE_Points") == pytest.approx(0.0, abs=1e-9)

    # **The property the board actually has to hold**: it is a fixed point, so
    # nothing about it is mid-convergence.
    settled = reconcile_team_totals(board.copy())
    for column in ("TRUE_receivingReceptions", "TRUE_passingCompletions",
                   "TRUE_receivingYards"):
        if column in board.columns:
            left = pd.to_numeric(board[column], errors="coerce")
            right = pd.to_numeric(settled[column], errors="coerce")
            both = left.notna() & right.notna()
            assert (left[both] - right[both]).abs().max() == pytest.approx(0.0, abs=1e-9), (
                f"the shipped board is not settled: reconcile still moves {column}")

    # **And the thing a board is for.** With the blend reproducing exactly nothing
    # can reorder, so this is now a tautology guarding the two above rather than a
    # tolerance: if a residual ever returns, it says so in the units that matter.
    projected = original.loc[shared, "projection_missing"].fillna(False).eq(False)
    ranks = pd.DataFrame({
        "pos": original.loc[shared, "primaryPosition"],
        "was": pd.to_numeric(original.loc[shared, "TRUE_Points"], errors="coerce"),
        "now": pd.to_numeric(rebuilt.loc[shared, "TRUE_Points"], errors="coerce"),
    }).loc[projected]
    moved = (ranks.groupby("pos")["was"].rank(ascending=False, method="min")
             - ranks.groupby("pos")["now"].rank(ascending=False, method="min")).abs()
    assert moved.max() == 0, "the reblend no longer reproduces the board's ordering"
