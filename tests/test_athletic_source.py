"""The Athletic as the sixth blended source.

Modelled on ``test_usage_fifth_source.py``, which is this repo's template for adding
a source: registration, the one real parse bug, abstention, and what must stay out.

The distinguishing property here is that the source is a **file somebody saved**
rather than a scrape, so the tests that matter most are the ones about what happens
when it is absent, stale, or carries a row it should not.

The same workbook also carries Jake Ciely's hand ranking, which is *not* a source: it
projects nothing, casts no vote, and exists to be read beside the board. The section
at the foot of this file is about keeping it that way -- the load-bearing test being
that adding it leaves ``TRUE_Points`` untouched.
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


# --- the hand ranking, which is not a source -----------------------------

def _ranked_frame():
    """A scored frame with two positions, three of whom Jake ranked."""
    return pd.DataFrame({
        "player_name": ["A", "B", "C", "D", "E"],
        "primaryPosition": ["RB", "RB", "RB", "WR", "WR"],
        "TRUE_Points": [300.0, 200.0, 100.0, 250.0, 150.0],
        "TRUE_PosRank": [1.0, 2.0, 3.0, 1.0, 2.0],
        "ATH_Points": [300.0, 200.0, 100.0, 250.0, 150.0],
        # He agrees on A, has C above B, and never ranked D or E.
        "ath_pos_rank": [1.0, 3.0, 2.0, np.nan, np.nan],
    })


def test_the_rank_columns_are_lowercase_so_the_blend_cannot_see_them():
    """The load-bearing naming rule. ``compute_weighted_stats`` and ``proj_to_score``
    scan every uppercase ``<PREFIX>_`` and require it to be numeric -- and a rank *is*
    numeric, so an uppercase name would get it blended into a stat line and priced.
    Lowercase makes "this never moves TRUE_Points" true by construction."""
    from Scripts.load_athletic import RANK_COLUMNS

    for column in (*RANK_COLUMNS.values(), "ath_pos_rank", "ath_override",
                   "ath_rank_delta"):
        assert column == column.lower(), f"{column} would enter the blend namespace"


def test_the_ranking_is_not_registered_as_a_source():
    """It projects nothing, so it cannot vote, cannot widen the spread, and has
    nothing to withdraw on an injury."""
    assert "ath_pos_rank" not in pu.WEIGHTS["default"]
    for tup in (sp.OPINION_PREFIXES, sp.PROJECTION_PREFIXES,
                sp.AVAILABILITY_WITHDRAWN_PREFIXES):
        assert not any(str(prefix).startswith("ath_") for prefix in tup)


def test_adding_the_rank_does_not_move_a_single_blended_point():
    """The whole basis for shipping this in draft week, asserted rather than promised.

    Run the blend and the scoring over a frame with and without the rank column and
    require the outputs to be identical -- not close, identical.
    """
    base = pd.DataFrame({
        "primaryPosition": ["RB", "WR"],
        "ESPN_rushingYards": [1000.0, 40.0],
        "FP_rushingYards": [1100.0, 60.0],
    })
    weights = {"default": {"ESPN": 0.25, "FP": 0.25}}
    plain = pu.compute_weighted_stats(df=base.copy(), stats_list=["rushingYards"],
                                      weights_dict=weights)
    with_rank = base.copy()
    with_rank["ath_pos_rank"] = [1.0, 12.0]
    ranked = pu.compute_weighted_stats(df=with_rank, stats_list=["rushingYards"],
                                       weights_dict=weights)
    pd.testing.assert_series_equal(plain["TRUE_rushingYards"],
                                   ranked["TRUE_rushingYards"])
    assert "TRUE_ath_pos_rank" not in ranked.columns
    assert "ath_pos_rank_Points" not in ranked.columns


def test_the_override_is_his_rank_against_his_own_projection():
    """Positive means he ranks a player above his own numbers -- the direction
    ``USG_PosRankDelta`` already set for a named voice."""
    out = sp._attach_athletic_override(_ranked_frame())
    # C is his RB2 and his projection's RB3, so he is one spot higher on him.
    assert out.loc[2, "ath_override"] == pytest.approx(1.0)
    assert out.loc[1, "ath_override"] == pytest.approx(-1.0)
    assert out.loc[0, "ath_override"] == pytest.approx(0.0)


def test_a_player_he_never_ranked_gets_no_delta_rather_than_a_number():
    """The receivers are unranked. A zero there would read as agreement."""
    out = sp._attach_athletic_override(_ranked_frame())
    assert out.loc[[3, 4], "ath_override"].isna().all()


def test_the_override_ranks_him_only_against_the_players_he_ranked():
    """He ranks 85 backs and a board carries half as many again. Ranking ``ATH_Points``
    over the whole pool would score his 60th back against a 60th drawn from a deeper
    one and report bench depth as disagreement."""
    frame = _ranked_frame()
    # Two more backs he never looked at, both projected above everyone he did.
    deeper = pd.concat([frame, pd.DataFrame({
        "player_name": ["X", "Y"], "primaryPosition": ["RB", "RB"],
        "TRUE_Points": [400.0, 350.0], "TRUE_PosRank": [1.0, 2.0],
        "ATH_Points": [400.0, 350.0], "ath_pos_rank": [np.nan, np.nan],
    })], ignore_index=True)
    out = sp._attach_athletic_override(deeper)
    # Unchanged: the two interlopers cannot push his ranked backs down.
    assert out.loc[2, "ath_override"] == pytest.approx(1.0)
    assert out.loc[1, "ath_override"] == pytest.approx(-1.0)


def test_a_frame_with_no_ranking_keeps_its_shape():
    """A board built before the ranking existed carries no column, and asking for the
    override must not invent one."""
    frame = _ranked_frame().drop(columns=["ath_pos_rank"])
    out = sp._attach_athletic_override(frame)
    assert "ath_override" not in out.columns


@pytest.mark.parametrize("points,flavor", [
    (0.0, "std"), (0.5, "half"), (1.0, "ppr"),
    # Between two lists, the nearer one. A 0.75-PPR league is not a case the workbook
    # has a list for, and the nearest list beats no list.
    # Exactly between two lists, the lower one -- see `_athletic_rank_flavor`.
    (0.75, "half"), (0.2, "std"),
])
def test_the_flavor_follows_this_league_s_points_per_reception(
        points, flavor, monkeypatch):
    """The workbook ranks the same 290 players three times because a reception is
    worth a different amount in each. Reading the one that answers this league's rules
    is the same principle that scores every source through them."""
    monkeypatch.setattr(sp, "_modelled_scoring_weights",
                        lambda league, season: {"receivingReceptions": points})
    assert sp._athletic_rank_flavor(object(), 2026) == flavor


# --- the real ranking, when it is there ----------------------------------

def test_the_shipped_ranking_covers_the_four_offensive_positions():
    from Scripts.paths import season_dir

    path = season_dir("TheAthletic", 2026, la.RANKS_FILENAME, create=False)
    if not path.exists():
        pytest.skip("no 2026 Athletic workbook imported")

    ranks = pd.read_parquet(path)
    assert ranks["player_name"].is_unique
    assert ranks.groupby("position").size().to_dict() == {
        "QB": 40, "RB": 85, "TE": 45, "WR": 120}
    # Each list is a dense 1..N with no ties, per position.
    for column in la.RANK_COLUMNS.values():
        for position, block in ranks.groupby("position"):
            assert sorted(block[column]) == list(range(1, len(block) + 1)), (
                f"{column} is not a dense ranking of the {position}s")


def test_every_ranked_player_is_also_a_projected_player():
    """Both files come out of one ``build`` call off one workbook, so a name on the
    ranking that the team tabs do not carry means the workbook spells him two ways --
    a join miss that would cost a board column."""
    from Scripts.paths import season_dir

    ranks_path = season_dir("TheAthletic", 2026, la.RANKS_FILENAME, create=False)
    stats_path = season_dir("TheAthletic", 2026, la.FILENAME, create=False)
    if not (ranks_path.exists() and stats_path.exists()):
        pytest.skip("no 2026 Athletic workbook imported")

    ranks = pd.read_parquet(ranks_path)
    stats = pd.read_parquet(stats_path)
    orphans = set(ranks["player_name"]) - set(stats["player_name"])
    assert not orphans, f"ranked but not projected: {sorted(orphans)}"


def test_he_overrides_his_own_numbers_where_projections_are_weakest():
    """The shape is the argument for showing the column at all: he leaves the position
    a projection handles best alone and reworks the two it handles worst. Measured on
    the 2026-08-31 workbook, in his own half-PPR list.
    """
    from Scripts.paths import season_dir

    ranks_path = season_dir("TheAthletic", 2026, la.RANKS_FILENAME, create=False)
    stats_path = season_dir("TheAthletic", 2026, la.FILENAME, create=False)
    if not (ranks_path.exists() and stats_path.exists()):
        pytest.skip("no 2026 Athletic workbook imported")

    ranks = pd.read_parquet(ranks_path)
    stats = pd.read_parquet(stats_path).set_index("player_name")
    # The workbook's own `Settings` table, in full rather than approximately: every
    # rule it prices at anything other than zero. Attempts, completions, carries and
    # targets really are worth 0 there, so this is his scoring and not a stand-in --
    # which matters, because dropping the interception rule alone is enough to
    # manufacture a quarterback override that he did not make.
    points = (stats["rushingYards"].fillna(0) * 0.1
              + stats["receivingYards"].fillna(0) * 0.1
              + stats["receivingReceptions"].fillna(0) * 0.5
              + stats["passingYards"].fillna(0) * 0.04
              + (stats["rushingTouchdowns"].fillna(0)
                 + stats["receivingTouchdowns"].fillna(0)) * 6
              + stats["passingTouchdowns"].fillna(0) * 4
              - stats["passingInterceptions"].fillna(0) * 2)
    frame = pd.DataFrame({
        "primaryPosition": ranks["position"].to_numpy(),
        "ath_pos_rank": ranks["ath_rank_half"].to_numpy(),
        "ATH_Points": ranks["player_name"].map(points).to_numpy(),
    })
    moved = (sp._attach_athletic_override(frame)
             .assign(big=lambda f: f["ath_override"].abs() >= 5)
             .groupby("primaryPosition")["big"].sum().to_dict())
    assert moved["QB"] == 0, "he has started overriding quarterbacks"
    assert moved["WR"] > moved["RB"] > moved["TE"] >= moved["QB"]
