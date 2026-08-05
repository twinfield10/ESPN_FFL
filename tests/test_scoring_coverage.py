"""Scoring-table coverage: every rule a league scores must reach a stat column.

The failure this guards against is silent. An unrecognised ESPN stat id used to
produce a NaN ``colName``, which ``proj_to_score`` turned into the column name
``"TRUE_nan"``, failed to find, and skipped -- so the rule contributed nothing
and the projections still looked entirely normal. Nothing logged, nothing
raised. League scoring changes between seasons, which is exactly when nobody is
looking closely, so this needs to be a red test rather than a judgement call.
"""

import types
import warnings

import pandas as pd
import pytest

from Scripts.config_utils import build_lg_vars, get_season
from Scripts.scrape_player_stats import (
    REPL_SCORING,
    ScoringCoverageWarning,
    UnmappedScoringRuleError,
    build_scoring_table,
)


def _league(scoring_rows, name="Test League", year=2026, league_id=1):
    """A stand-in for an ESPN League carrying just a scoring format."""
    return types.SimpleNamespace(
        name=name,
        year=year,
        league_id=league_id,
        settings=types.SimpleNamespace(scoring_format=scoring_rows),
    )


def _rule(stat_id, points, abbr="XX", label="Some Rule"):
    return {"id": stat_id, "abbr": abbr, "label": label, "points": points}


# --- the guard -----------------------------------------------------------

def test_unmapped_rule_warns_instead_of_passing_silently():
    lg = _league([_rule(9999, 4.0, abbr="NEW", label="Brand New Rule")])
    with pytest.warns(ScoringCoverageWarning, match="9999"):
        build_scoring_table(lg)


def test_warning_names_the_league_and_the_points():
    lg = _league([_rule(9999, -1.5)], name="Some League", year=2027)
    with pytest.warns(ScoringCoverageWarning) as rec:
        build_scoring_table(lg)
    msg = str(rec[0].message)
    assert "Some League" in msg and "2027" in msg and "-1.5" in msg


def test_strict_mode_raises():
    lg = _league([_rule(9999, 4.0)])
    with pytest.raises(UnmappedScoringRuleError, match="9999"):
        build_scoring_table(lg, strict=True)


def test_zero_point_rules_are_not_flagged():
    """Every league carries a long tail of rules set to 0. They are inert
    whether or not they map, so flagging them would bury the real signal."""
    lg = _league([_rule(9999, 0.0), _rule(3, 0.04, abbr="PY")])
    with warnings.catch_warnings():
        warnings.simplefilter("error", ScoringCoverageWarning)
        build_scoring_table(lg)


def test_mapped_rule_does_not_warn():
    lg = _league([_rule(53, 1.0, abbr="REC", label="Each reception")])
    with warnings.catch_warnings():
        warnings.simplefilter("error", ScoringCoverageWarning)
        table = build_scoring_table(lg)
    assert table.loc[table["id"] == 53, "colName"].item() == "receivingReceptions"


def test_ignored_ids_are_dropped_not_flagged():
    """FG 60+, 2-pt return and 1-pt safety are excluded on purpose, so they must
    not trip the guard even though they carry points."""
    lg = _league([_rule(201, 5.0), _rule(206, 2.0), _rule(209, 1.0)])
    with warnings.catch_warnings():
        warnings.simplefilter("error", ScoringCoverageWarning)
        table = build_scoring_table(lg)
    assert table.empty


def test_warning_survives_the_global_warning_filter():
    """Scripts/fetch_utils.py sets warnings.filterwarnings("ignore") at module
    scope, which would otherwise make this guard useless."""
    with warnings.catch_warnings(record=True) as rec:
        warnings.filterwarnings("ignore")
        build_scoring_table(_league([_rule(9999, 4.0)]))
    assert [w for w in rec if issubclass(w.category, ScoringCoverageWarning)]


# --- the specific rules this plan mapped ---------------------------------

def test_fgy50_is_rewritten_onto_fg_made_yards():
    """221 is an 'every N yards' rule, so it must land on stat 214 rather than
    getting its own column."""
    lg = _league([_rule(221, 5.0, abbr="FGY50", label="Every 50 FG Made yards")])
    with warnings.catch_warnings():
        warnings.simplefilter("error", ScoringCoverageWarning)
        table = build_scoring_table(lg)
    row = table.iloc[0]
    assert row["id"] == 214
    assert row["colName"] == "214"
    assert row["points"] == pytest.approx(0.064)


def test_fgy50_rate_accounts_for_the_per_game_floor():
    """ESPN awards FGY50 as floor(FG yards / 50) per game, discarding the sub-50
    remainder, so the realised rate is well below the naive 5.0/50 = 0.1.
    Measured at 0.0642 pts/yd across the 21 kickers with >=300 FG made yards in
    2025; 0.1 overstates a starting kicker by roughly 2.4 pts/week."""
    assert REPL_SCORING[221]["points"] == pytest.approx(0.064)
    assert REPL_SCORING[221]["points"] < 0.1


@pytest.mark.parametrize(
    "stat_id, expected, abbr",
    [
        (79, "missedFieldGoalsFrom40To49", "FGM40"),     # new to GOP in 2026
        (74, "madeFieldGoalsFrom50Plus", "FG50P"),       # 2016-2019 seasons
        (198, "madeFieldGoalsFrom50Plus", "FG50"),       # modern id for the same
    ],
)
def test_previously_unmapped_kicking_rules_now_resolve(stat_id, expected, abbr):
    lg = _league([_rule(stat_id, 5.0, abbr=abbr)])
    with warnings.catch_warnings():
        warnings.simplefilter("error", ScoringCoverageWarning)
        table = build_scoring_table(lg)
    assert table.loc[table["id"] == stat_id, "colName"].item() == expected


def test_74_and_198_would_double_count_if_a_league_scored_both():
    """They are the old and new ids for the same stat, so both mapping to one
    column is only safe because no league-season scores both. Documented here so
    that assumption is visible if it ever breaks."""
    lg = _league([_rule(74, 5.0), _rule(198, 5.0)])
    table = build_scoring_table(lg)
    assert (table["colName"] == "madeFieldGoalsFrom50Plus").sum() == 2


# --- against the real leagues -------------------------------------------

@pytest.mark.live
def test_no_configured_league_has_unmapped_scoring_rules():
    """The one that catches a league changing its scoring between seasons.

    Builds each table immediately after its own fetch. Do not restructure this
    into fetch-all-then-check: espn_api shares scoring dicts between League
    objects, so a later fetch rewrites an earlier league's points (see
    fetch_utils.isolate_scoring_format).
    """
    from Scripts.fetch_utils import fetch_league

    season = get_season()
    offenders = {}
    for name, cfg in build_lg_vars().items():
        league = fetch_league(
            league_id=cfg["ID"], year=season,
            swid=cfg["SWID"], espn_s2=cfg["ESPN_S2"],
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            build_scoring_table(league)
        found = [
            str(w.message) for w in caught
            if issubclass(w.category, ScoringCoverageWarning)
        ]
        if found:
            offenders[name] = found

    assert not offenders, f"unmapped scoring rules in {season}: {offenders}"


@pytest.mark.live
def test_scoring_format_is_not_shared_between_leagues():
    """espn_api 0.45.1 mutates module-level dicts when parsing scoring settings,
    so without isolation the first league's points get overwritten by the
    second's."""
    from Scripts.fetch_utils import fetch_league

    season = get_season()
    lg_vars = build_lg_vars()
    names = list(lg_vars)[:2]

    first = fetch_league(
        league_id=lg_vars[names[0]]["ID"], year=season,
        swid=lg_vars[names[0]]["SWID"], espn_s2=lg_vars[names[0]]["ESPN_S2"],
    )
    before = build_scoring_table(first)

    fetch_league(
        league_id=lg_vars[names[1]]["ID"], year=season,
        swid=lg_vars[names[1]]["SWID"], espn_s2=lg_vars[names[1]]["ESPN_S2"],
    )
    after = build_scoring_table(first)

    pd.testing.assert_frame_equal(before, after)
