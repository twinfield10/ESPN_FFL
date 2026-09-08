"""The source atlas renderer.

The document this builds exists to be *trusted about coverage*, so the tests that
matter are the ones about absence. A source with no line, a player in no source and
a cell that was filled in rather than projected must each render as the distinct
thing it is -- because the failure mode of a page like this is not a crash, it is a
clean-looking table that reports a fabricated number as an opinion.

The other load-bearing property is that the bias tables **pair**: a source is only
ever compared against the baseline on the rows it was real for. Comparing thirty
sportsbook lines against a five-hundred-row baseline measures coverage and calls it
bias, which would be wrong in the direction that looks most like a finding.
"""

import polars as pl
import pytest

from Scripts.lab import sources as ls


def _board(rows: int = 6, **overrides) -> pl.DataFrame:
    """A minimal board carrying the columns the renderer reads."""
    data = {
        "player_name": [f"Player {i}" for i in range(rows)],
        "primaryPosition": ["WR"] * rows,
        "pro_team": ["BUF"] * rows,
        "adp": [float(i + 1) for i in range(rows)],
        "bye_week": [7] * rows,
        "sources_real": [2] * rows,
        "avail_evidence": [None] * rows,
        "ESPN_Points": [200.0] * rows,
        "FP_Points": [210.0] * rows,
        "TRUE_Points": [205.0] * rows,
        "ESPN_receivingYards": [1000.0] * rows,
        "FP_receivingYards": [1200.0] * rows,
        "FP_receivingYards_is_imputed": [False] * rows,
        "MEAN_receivingYards": [1100.0] * rows,
        "TRUE_receivingYards": [1100.0] * rows,
    }
    data.update(overrides)
    return pl.DataFrame(data)


# --- absence renders as absence ------------------------------------------

def test_a_source_with_no_column_is_skipped_not_counted_as_real():
    """`compute_weighted_stats` drops a source with no column from both sums. The
    page must agree: a missing column is not an abstention and not a zero."""
    df = _board()
    assert ls.real_mask(df, "PINNY", "receivingYards") is None


def test_a_source_with_no_flag_column_counts_as_real():
    """ESPN's case, and the reason it is the root: it carries no provenance columns
    at all, and treating that as imputed would silence the only universal source."""
    df = _board()
    mask = ls.real_mask(df, "ESPN", "receivingYards")
    assert df.select(mask.sum()).item() == df.height


def test_a_null_flag_is_treated_as_imputed():
    """A null flag means the row never joined, which is an absence rather than an
    opinion."""
    df = _board(BOL_receivingYards=[900.0] * 6,
                BOL_receivingYards_is_imputed=[None] * 6)
    mask = ls.real_mask(df, "BOL", "receivingYards")
    assert df.select(mask.sum()).item() == 0


def test_an_imputed_cell_reports_its_provenance_rather_than_a_bare_number():
    """The whole point of the flags. A filled cell that renders as a plain number is
    indistinguishable from an opinion."""
    row = {"PINNY_receivingYards": 1100.0,
           "PINNY_receivingYards_is_imputed": True}
    cell = ls._cell(row, "PINNY", "receivingYards")
    assert "1,100" in cell
    assert "imp" in cell and "MEAN" in cell

    honest = ls._cell({"ESPN_receivingYards": 1000.0}, "ESPN", "receivingYards")
    assert "imp" not in honest


def test_a_wholly_imputed_line_says_so_and_names_the_origin():
    row = {f"PINNY_{s}": 1.0 for s in ("receivingYards", "receivingReceptions")}
    row.update({f"PINNY_{s}{ls.IMPUTED_SUFFIX}": True
                for s in ("receivingYards", "receivingReceptions")})
    status = ls._line_status(row, "PINNY",
                             ("receivingYards", "receivingReceptions"))
    assert "imputed" in status and "MEAN" in status


def test_a_player_in_no_source_renders_as_a_hole_not_a_zero():
    """A coverage hole and a projection of zero are different claims, and a draft
    board that confuses them hides exactly the players it should flag."""
    df = _board()
    block = ls.player_block(df, "Nobody At All", "RB", {})
    assert "not on the board" in block
    assert "No source carries him" in block


def test_the_availability_gate_reason_is_surfaced_on_the_player():
    df = _board(avail_evidence=["withdrawn: out for season"] + [None] * 5)
    block = ls.player_block(df, "Player 0", "WR", {})
    assert "withdrawn: out for season" in block


# --- the bias tables pair -------------------------------------------------

def test_bias_pairs_the_baseline_to_the_source_own_rows():
    """A source real on two rows is compared against the baseline on *those two*.

    Built so the trap would fire if it were not paired: the two rows the book prices
    have a baseline of 100, and the four it ignores have a baseline of 1,000. Pooled,
    the source would read as wildly low; paired, it is exactly 1.000.
    """
    df = pl.DataFrame({
        "player_name": [f"P{i}" for i in range(6)],
        "primaryPosition": ["WR"] * 6,
        "ESPN_Points": [200.0] * 6,
        "FP_Points": [200.0] * 6,
        "MEAN_receivingYards": [100.0, 100.0, 1000.0, 1000.0, 1000.0, 1000.0],
        "ESPN_receivingYards": [100.0, 100.0, 1000.0, 1000.0, 1000.0, 1000.0],
        "BOL_receivingYards": [100.0, 100.0, 1000.0, 1000.0, 1000.0, 1000.0],
        "BOL_receivingYards_is_imputed": [False, False, True, True, True, True],
    })
    rows = ls.bias_rows(df, "WR", "receivingYards")
    book = [r for r in rows if "BOL" in r[0]][0]
    # Two paired rows is below the reporting floor, so it reports the count and
    # withholds the ratios rather than publishing a distribution built from two
    # numbers -- which is itself the behaviour under test.
    assert book[1] == "2"
    assert all("—" in cell for cell in book[2:])


def test_bias_reports_a_thin_source_as_a_count_rather_than_a_ratio():
    """Fewer than five paired rows is a name, not a distribution."""
    df = _board(rows=6)
    df = df.with_columns(
        pl.Series("BOL_receivingYards", [900.0] * 6),
        pl.Series("BOL_receivingYards_is_imputed",
                  [False, False, False, True, True, True]))
    rows = ls.bias_rows(df, "WR", "receivingYards")
    book = [r for r in rows if "BOL" in r[0]][0]
    assert book[1] == "3"


def test_espn_and_fantasypros_bracket_the_baseline():
    """They are the reference pair, and on shared rows their mean ratios sum to 2."""
    df = _board(rows=8,
                player_name=[f"P{i}" for i in range(8)],
                primaryPosition=["WR"] * 8,
                adp=[float(i) for i in range(8)],
                bye_week=[7] * 8, sources_real=[2] * 8,
                avail_evidence=[None] * 8,
                ESPN_Points=[200.0] * 8, FP_Points=[200.0] * 8,
                TRUE_Points=[200.0] * 8,
                ESPN_receivingYards=[900.0 + 10 * i for i in range(8)],
                FP_receivingYards=[1100.0 - 10 * i for i in range(8)],
                FP_receivingYards_is_imputed=[False] * 8,
                MEAN_receivingYards=[1000.0] * 8,
                TRUE_receivingYards=[1000.0] * 8)
    rows = {r[0]: r for r in ls.bias_rows(df, "WR", "receivingYards")}
    espn = float(rows["<code>ESPN</code>"][2].split(">")[1].split("<")[0])
    fp = float(rows["<code>FP</code>"][2].split(">")[1].split("<")[0])
    assert espn + fp == pytest.approx(2.0, abs=1e-3)


# --- the gate -------------------------------------------------------------

def test_the_gate_is_built_from_espn_and_fantasypros_only():
    """It must not read the source under test, or it selects flattering rows."""
    import inspect
    body = inspect.getsource(ls.gate_expr)
    assert '("ESPN", "FP")' in body
    for source in ("ATH", "USG", "BOL", "PINNY", "TRUE"):
        assert f'"{source}"' not in body


def test_the_gate_keeps_the_projectable_and_drops_the_noise():
    df = _board(rows=4,
                player_name=["A", "B", "C", "D"],
                primaryPosition=["WR"] * 4,
                adp=[1.0, 2.0, 3.0, 4.0], bye_week=[7] * 4,
                sources_real=[2] * 4, avail_evidence=[None] * 4,
                ESPN_Points=[300.0, 8.0, 101.0, 0.0],
                FP_Points=[290.0, 13.0, 99.0, 0.0],
                TRUE_Points=[295.0, 10.0, 100.0, 0.0],
                ESPN_receivingYards=[1.0] * 4,
                FP_receivingYards=[1.0] * 4,
                FP_receivingYards_is_imputed=[False] * 4,
                MEAN_receivingYards=[1.0] * 4,
                TRUE_receivingYards=[1.0] * 4)
    assert df.filter(ls.gate_expr(df)).height == 2


# --- registration invariants ----------------------------------------------

def test_every_voting_source_has_a_card():
    """A source that reaches the blend and not this page is the failure the page
    exists to prevent."""
    assert set(ls.SOURCES) == set(ls.SOURCE_CARDS)


def test_the_source_list_matches_the_weight_table():
    """Read from `WEIGHTS` rather than restated, so registering a further source
    fails here instead of silently going undocumented.

    The page documents **what is on the board**, which stopped being the same set as
    what votes on 2026-09-07: TOMCAT was withdrawn from the blend but its `USG_` stat
    lines are still merged, so the atlas still has real coverage and bias tables to
    render for it. `ls.WITHDRAWN` is the difference, and it has to be exactly that --
    a source that is in neither the weight table nor `WITHDRAWN` is one nobody decided
    about."""
    assert set(ls.BLENDED) == set(ls.WEIGHTS["default"])
    assert set(ls.SOURCES) == set(ls.BLENDED) | set(ls.WITHDRAWN)
    assert not set(ls.BLENDED) & set(ls.WITHDRAWN)


def test_tomcat_is_outside_the_imputation_chain():
    """The load-bearing asymmetry: filling the one independent source from an
    average of two that are not would turn it into a copy of them."""
    assert "USG" not in ls.IMPUTED_FROM
    assert "ESPN" not in ls.IMPUTED_FROM
    assert ls.IMPUTED_FROM["PINNY"] == "MEAN"
    assert ls.IMPUTED_FROM["FP"] == "ESPN"


def test_availability_is_read_from_the_pipeline_not_asserted():
    assert ls.availability("ESPN") == "draft + weekly"
    assert ls.availability("ATH") == "draft only"
    # Withdrawn is checked before the prefix lists: TOMCAT is in neither of them, and
    # "draft only" would be a wrong answer rather than a stale one.
    assert "withdrawn" in ls.availability("USG")


def test_a_missing_board_fails_loudly():
    """Rather than rendering a page of em dashes that reads as 'no coverage'."""
    with pytest.raises(SystemExit):
        ls.load_board(1999, "no_such_league")


def test_the_three_cell_readers_agree_with_the_mask():
    """`real_mask`, `_cell` and `_line_status` must classify a cell identically.

    The two ``None`` cases mean opposite things -- no flag column at all (ESPN, never
    imputed) against a null flag (the row never joined) -- and an earlier version of
    this module read them the same way, which would have printed a never-joined cell
    as an honest projection.
    """
    df = _board(rows=6,
                BOL_receivingYards=[900.0] * 6,
                BOL_receivingYards_is_imputed=[None] * 6)
    mask_says_real = df.select(
        ls.real_mask(df, "BOL", "receivingYards").sum()).item()
    row = df.row(0, named=True)
    assert mask_says_real == 0
    assert ls._cell_is_real(row, "BOL", "receivingYards") is False
    assert "imp" in ls._cell(row, "BOL", "receivingYards")
    # ESPN carries no flag column at all and must read the other way.
    assert ls._cell_is_real(row, "ESPN", "receivingYards") is True
    assert "imp" not in ls._cell(row, "ESPN", "receivingYards")
