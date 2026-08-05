"""The scoring registry: resolution, isolation, drift detection and auditing.

The registry exists so that scoring is a recorded fact rather than whatever the
last ESPN fetch happened to say. These tests cover the three things that make
that safe: reads never hand out a shared object, a cold registry degrades to live
derivation rather than silence, and a mismatch between stored and live settings
is reported.
"""

import types
import warnings

import pandas as pd
import pytest

from Scripts import scoring
from Scripts.scrape_player_stats import build_scoring_table


@pytest.fixture
def registry(tmp_path, monkeypatch):
    """Redirect the registry to a temp file and clear the memoised readers."""
    monkeypatch.setattr(scoring, "SCORING_DIR", tmp_path)
    monkeypatch.setattr(scoring, "SCORING_CSV", tmp_path / "scoring.csv")
    scoring.reset_caches()
    yield tmp_path / "scoring.csv"
    scoring.reset_caches()


def _league(scoring_rows, name="Test League", year=2026, league_id=1):
    return types.SimpleNamespace(
        name=name, year=year, league_id=league_id,
        settings=types.SimpleNamespace(scoring_format=scoring_rows),
    )


def _rule(stat_id, points, abbr="XX", label="Some Rule"):
    return {"id": stat_id, "abbr": abbr, "label": label, "points": points}


def _store(registry_path, league_key, season, rows, name="Test League"):
    """Write rows straight to the registry, bypassing ESPN."""
    df = pd.DataFrame(rows)
    df["season"] = season
    df["league_key"] = league_key
    df["league_name"] = name
    df["recorded_at"] = "2026-08-03T00:00:00"
    scoring._write_registry(df[scoring.REGISTRY_COLUMNS])
    return df


def _row(source_id, points, col_name, abbr="XX", label="L", stat_id=None):
    return {
        "source_id": source_id,
        "id": source_id if stat_id is None else stat_id,
        "abbr": abbr, "label": label, "points": points, "colName": col_name,
    }


# --- the mutation trap ---------------------------------------------------

def test_get_scoring_table_returns_a_fresh_copy_each_call(registry):
    """The trap that makes a naive cache dangerous. ``proj_to_score`` mutates the
    table it is given -- 13 ``s_df.loc[...] = ...`` writes re-scoring sacks and
    tackles for the IDP league. If the loader handed out a shared frame, the first
    call would corrupt the rules for every later one, which is exactly the
    espn_api bug this registry exists to get away from."""
    _store(registry, "some_league", 2026, [_row(99, 5.0, "defensiveSacks")])

    first = scoring.get_scoring_table(league_key="some_league", season=2026)
    first.loc[first["id"] == 99, "points"] = 999.0

    second = scoring.get_scoring_table(league_key="some_league", season=2026)
    assert second.loc[second["id"] == 99, "points"].item() == 5.0


def test_load_scoring_registry_returns_a_fresh_copy(registry):
    _store(registry, "some_league", 2026, [_row(99, 5.0, "defensiveSacks")])
    first = scoring.load_scoring_registry()
    first.loc[first.index, "points"] = 0.0
    assert scoring.load_scoring_registry()["points"].tolist() == [5.0]


# --- resolution ----------------------------------------------------------

def test_registry_hit_is_used_and_espn_is_not_consulted(registry):
    """A stored value must win over the live object, or the registry is decorative."""
    _store(registry, "some_league", 2026, [_row(53, 1.5, "receivingReceptions")])
    live = _league([_rule(53, 99.0)])

    table = scoring.get_scoring_table(
        live, league_key="some_league", season=2026, verify=False,
    )
    assert table.loc[table["id"] == 53, "points"].item() == 1.5


def test_cold_registry_falls_back_to_live_and_says_so(registry):
    live = _league([_rule(53, 1.0, abbr="REC")])
    with pytest.warns(scoring.ScoringRegistryWarning, match="No registry entry"):
        table = scoring.get_scoring_table(live, league_key="some_league", season=2026)
    assert table.loc[table["id"] == 53, "colName"].item() == "receivingReceptions"


def test_cold_registry_without_a_league_is_an_error(registry):
    """Silently returning nothing here would reintroduce the original bug."""
    with pytest.raises(ValueError, match="No registry entry"):
        scoring.get_scoring_table(league_key="missing", season=2026)


def test_needs_either_a_league_or_a_key_and_season(registry):
    with pytest.raises(ValueError, match="either a league object"):
        scoring.get_scoring_table()


def test_season_and_key_default_from_the_league(registry, monkeypatch):
    monkeypatch.setattr(
        scoring, "_league_key_for_id", lambda league_id: "some_league",
    )
    _store(registry, "some_league", 2026, [_row(53, 1.5, "receivingReceptions")])
    table = scoring.get_scoring_table(_league([_rule(53, 1.5)], year=2026), verify=False)
    assert table.loc[table["id"] == 53, "points"].item() == 1.5


def test_unconfigured_league_warns_and_derives_live(registry, monkeypatch):
    monkeypatch.setattr(scoring, "_league_key_for_id", lambda league_id: None)
    with pytest.warns(scoring.ScoringRegistryWarning, match="not in config.yaml"):
        table = scoring.get_scoring_table(_league([_rule(53, 1.0)], league_id=999))
    assert table.loc[table["id"] == 53, "colName"].item() == "receivingReceptions"


# --- drift detection ----------------------------------------------------

def test_drift_between_stored_and_live_scoring_warns(registry):
    """A commissioner changing scoring mid-season was previously undetectable."""
    _store(registry, "some_league", 2026, [_row(53, 1.0, "receivingReceptions")])
    live = _league([_rule(53, 0.5)])          # PPR halved since the registry write

    with pytest.warns(scoring.ScoringRegistryWarning, match="disagrees with live"):
        scoring.get_scoring_table(live, league_key="some_league", season=2026)


def test_drift_can_be_made_fatal(registry):
    _store(registry, "some_league", 2026, [_row(53, 1.0, "receivingReceptions")])
    live = _league([_rule(53, 0.5)])
    with pytest.raises(scoring.ScoringDriftError, match="disagrees with live"):
        scoring.get_scoring_table(
            live, league_key="some_league", season=2026, strict=True,
        )


def test_a_new_rule_appearing_live_counts_as_drift(registry):
    _store(registry, "some_league", 2026, [_row(53, 1.0, "receivingReceptions")])
    live = _league([_rule(53, 1.0), _rule(4, 6.0, abbr="PTD")])
    with pytest.warns(scoring.ScoringRegistryWarning, match="disagrees with live"):
        scoring.get_scoring_table(live, league_key="some_league", season=2026)


def test_matching_scoring_does_not_warn(registry):
    _store(registry, "some_league", 2026, [_row(53, 1.0, "receivingReceptions")])
    live = _league([_rule(53, 1.0)])
    with warnings.catch_warnings():
        warnings.simplefilter("error", scoring.ScoringRegistryWarning)
        scoring.get_scoring_table(live, league_key="some_league", season=2026)


def test_verify_false_skips_the_comparison(registry):
    _store(registry, "some_league", 2026, [_row(53, 1.0, "receivingReceptions")])
    live = _league([_rule(53, 0.5)])
    with warnings.catch_warnings():
        warnings.simplefilter("error", scoring.ScoringRegistryWarning)
        scoring.get_scoring_table(
            live, league_key="some_league", season=2026, verify=False,
        )


# --- writing ------------------------------------------------------------

def test_refreshing_one_league_season_leaves_the_others_alone(registry):
    _store(registry, "league_a", 2025, [_row(53, 1.0, "receivingReceptions")])
    _store(registry, "league_b", 2025, [_row(53, 0.5, "receivingReceptions")])
    _store(registry, "league_a", 2026, [_row(53, 1.5, "receivingReceptions")])

    reg = scoring.load_scoring_registry()
    assert len(reg) == 3
    assert reg[(reg["league_key"] == "league_b")]["points"].tolist() == [0.5]


def test_rewriting_a_league_season_replaces_rather_than_appends(registry):
    _store(registry, "league_a", 2026, [_row(53, 1.0, "receivingReceptions")])
    _store(registry, "league_a", 2026, [_row(53, 2.0, "receivingReceptions")])
    reg = scoring.load_scoring_registry()
    assert reg["points"].tolist() == [2.0]


def test_unchanged_rules_keep_their_original_timestamp(registry):
    """Otherwise every refresh restamps all ~1,900 rows and `git diff` becomes
    full-file noise, which is most of the reason to commit the file at all."""
    _store(registry, "league_a", 2026, [_row(53, 1.0, "receivingReceptions")])
    original = scoring.load_scoring_registry()["recorded_at"].item()

    later = pd.DataFrame([_row(53, 1.0, "receivingReceptions")])
    later["season"], later["league_key"] = 2026, "league_a"
    later["league_name"], later["recorded_at"] = "A", "2099-01-01T00:00:00"
    scoring._write_registry(later[scoring.REGISTRY_COLUMNS])

    assert scoring.load_scoring_registry()["recorded_at"].item() == original


def test_a_changed_rule_gets_a_new_timestamp(registry):
    _store(registry, "league_a", 2026, [_row(53, 1.0, "receivingReceptions")])
    later = pd.DataFrame([_row(53, 0.5, "receivingReceptions")])    # repriced
    later["season"], later["league_key"] = 2026, "league_a"
    later["league_name"], later["recorded_at"] = "A", "2099-01-01T00:00:00"
    scoring._write_registry(later[scoring.REGISTRY_COLUMNS])

    assert scoring.load_scoring_registry()["recorded_at"].item() == "2099-01-01T00:00:00"


def test_unmapped_rules_keep_their_timestamp_too(registry):
    """A null colName must still match on re-write; pandas merge treats NaN keys
    as equal, and this pins that behaviour."""
    _store(registry, "league_a", 2026, [_row(9999, 4.0, None)])
    original = scoring.load_scoring_registry()["recorded_at"].item()

    later = pd.DataFrame([_row(9999, 4.0, None)])
    later["season"], later["league_key"] = 2026, "league_a"
    later["league_name"], later["recorded_at"] = "A", "2099-01-01T00:00:00"
    scoring._write_registry(later[scoring.REGISTRY_COLUMNS])

    assert scoring.load_scoring_registry()["recorded_at"].item() == original


def test_a_write_invalidates_the_read_cache(registry):
    _store(registry, "league_a", 2026, [_row(53, 1.0, "receivingReceptions")])
    assert scoring.get_scoring_table(
        league_key="league_a", season=2026)["points"].tolist() == [1.0]
    _store(registry, "league_a", 2026, [_row(53, 2.0, "receivingReceptions")])
    assert scoring.get_scoring_table(
        league_key="league_a", season=2026)["points"].tolist() == [2.0]


# --- source_id, and why auditing needs it -------------------------------

def test_source_id_survives_the_every_n_yards_rewrite():
    """REPL_SCORING rewrites `id` onto the stat the rule counts, so `id` alone
    cannot tell a reprice from a replacement."""
    table = build_scoring_table(_league([_rule(221, 5.0, abbr="FGY50")]))
    row = table.iloc[0]
    assert row["source_id"] == 221      # what the commissioner configured
    assert row["id"] == 214             # the stat it is scored against


def test_source_id_equals_id_for_ordinary_rules():
    table = build_scoring_table(_league([_rule(53, 1.0, abbr="REC")]))
    assert table.iloc[0]["source_id"] == table.iloc[0]["id"] == 53


def test_diff_distinguishes_a_replacement_from_a_reprice(registry):
    """The GOP 2025->2026 kicker change. Keyed on `id` this reads as "214
    repriced 0.1 -> 0.064", conflating a rule the commissioner deleted with a
    modelling rate we chose. Keyed on source_id it reads correctly."""
    _store(registry, "gop", 2025, [_row(214, 0.1, "214", abbr="FGY")])
    _store(registry, "gop", 2026, [
        _row(221, 0.064, "214", abbr="FGY", stat_id=214),
        _row(79, -1.0, "missedFieldGoalsFrom40To49", abbr="FGM40"),
    ])

    diff = scoring.diff_scoring("gop", 2025, 2026)
    changes = dict(zip(diff["source_id"], diff["change"]))
    assert changes == {214: "removed", 221: "added", 79: "added"}


def test_diff_reports_a_reprice(registry):
    _store(registry, "gop", 2025, [_row(99, 12.0, "defensiveSacks", abbr="SK")])
    _store(registry, "gop", 2026, [_row(99, 1.0, "defensiveSacks", abbr="SK")])
    diff = scoring.diff_scoring("gop", 2025, 2026)
    assert diff["change"].tolist() == ["repriced"]
    assert diff["points_2025"].tolist() == [12.0]
    assert diff["points_2026"].tolist() == [1.0]


def test_diff_is_empty_when_nothing_changed(registry):
    for season in (2025, 2026):
        _store(registry, "gop", season, [_row(53, 1.0, "receivingReceptions")])
    assert scoring.diff_scoring("gop", 2025, 2026).empty


def test_coverage_gaps_reports_scored_but_unmodelled_rules(registry):
    _store(registry, "league_a", 2026, [
        _row(53, 1.0, "receivingReceptions"),
        _row(9999, 4.0, None, abbr="NEW"),        # scored, not modelled
        _row(8888, 0.0, None, abbr="INERT"),      # unmapped but worth nothing
    ])
    gaps = scoring.coverage_gaps()
    assert gaps["source_id"].tolist() == [9999]


# --- against the real registry ------------------------------------------

@pytest.mark.live
def test_registry_matches_live_espn_for_every_league_season():
    """The registry is only trustworthy if it still equals what ESPN reports.

    Also the regression guard for the espn_api sharing bug: each table is built
    immediately after its own fetch, and a stale or contaminated registry entry
    shows up here as a mismatch.
    """
    from Scripts.config_utils import build_lg_vars, get_season
    from Scripts.fetch_utils import fetch_league

    season = get_season()
    mismatches = {}
    for name, cfg in build_lg_vars().items():
        league = fetch_league(
            league_id=cfg["ID"], year=season,
            swid=cfg["SWID"], espn_s2=cfg["ESPN_S2"],
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            scoring.get_scoring_table(league, verify=True)
        drift = [
            str(w.message) for w in caught
            if issubclass(w.category, scoring.ScoringRegistryWarning)
        ]
        if drift:
            mismatches[name] = drift

    assert not mismatches, f"registry disagrees with live ESPN: {mismatches}"


@pytest.mark.live
def test_every_configured_league_season_is_recorded():
    from Scripts.config_utils import build_lg_vars

    reg = scoring.load_scoring_registry()
    assert not reg.empty, "registry is empty; run `python -m Scripts.scoring --all`"

    recorded = set(zip(reg["league_key"], reg["season"]))
    missing = [
        (cfg["key"], year)
        for cfg in build_lg_vars().values()
        for year in range(int(cfg["start"]), int(cfg["end"]) + 1)
        if (cfg["key"], year) not in recorded
    ]
    assert not missing, f"league-seasons absent from the registry: {missing}"
