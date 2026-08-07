"""The usage layer's leakage guarantee, and the stat mapping it rests on.

Leakage is the risk ``docs/plans/16-usage-data-layer.md`` calls out as the one
that will silently make a backtest look excellent and the live model useless, so
it is pinned here rather than reasoned about. Everything below runs on synthetic
frames: no network, no dependency on what nflverse published today, and no
dependency on the gitignored parquet pulls.

Two data-dependent checks are marked and skipped when the pulls are absent, on the
same principle ``tests/test_crosswalk.py`` uses for the committed crosswalk: the
column mapping is a claim about upstream's schema and should fail loudly when
upstream changes it.
"""

import numpy as np
import polars as pl
import pytest

from Scripts.usage import baseline as bl
from Scripts.usage.nflverse import (
    ACTUAL_PREFIX,
    EXPECTED_PREFIX,
    USAGE_STATS,
    opportunity_path,
)

STAT = "receivingYards"
ACTUAL = f"{ACTUAL_PREFIX}{STAT}"
EXPECTED = f"{EXPECTED_PREFIX}{STAT}"
TRAILING_ACTUAL, TRAILING_EXPECTED = bl.trailing_columns(STAT)


def usage_frame(rows):
    """Build a minimal single-season usage frame.

    Args:
        rows: Iterable of ``(gsis_id, week, actual, expected)``.

    Returns:
        pl.DataFrame: In the shape ``load_opportunity`` returns.
    """
    return pl.DataFrame(
        {
            "season": [2025] * len(rows),
            "week": [r[1] for r in rows],
            "gsis_id": [r[0] for r in rows],
            "posteam": [r[4] if len(r) > 4 else "SEA" for r in rows],
            ACTUAL: [float(r[2]) for r in rows],
            EXPECTED: [float(r[3]) for r in rows],
        },
        schema_overrides={"week": pl.Int32, "season": pl.Int32},
    )


@pytest.fixture
def frame():
    """One player, five weeks, values chosen so every mean is distinguishable."""
    return usage_frame([
        ("00-0001", 1, 10.0, 20.0),
        ("00-0001", 2, 30.0, 40.0),
        ("00-0001", 3, 50.0, 60.0),
        ("00-0001", 4, 70.0, 80.0),
        ("00-0001", 5, 90.0, 100.0),
    ])


# --- the trailing means --------------------------------------------------

def test_trailing_mean_excludes_the_target_week(frame):
    """Week N's own value must not appear in week N's feature."""
    out = bl.trailing_features(frame, stats=[STAT], window=3)
    assert out[TRAILING_ACTUAL].to_list() == [None, 10.0, 20.0, 30.0, 50.0]
    assert out[TRAILING_EXPECTED].to_list() == [None, 20.0, 30.0, 40.0, 60.0]


def test_features_for_week_n_ignore_week_n_values(frame):
    """Altering week N's stats leaves week N's features untouched.

    The first of the two tests the plan specifies.
    """
    before = bl.trailing_features(frame, stats=[STAT])
    tampered = frame.with_columns(
        pl.when(pl.col("week") == 4).then(pl.lit(9999.0))
        .otherwise(pl.col(ACTUAL)).alias(ACTUAL)
    )
    after = bl.trailing_features(tampered, stats=[STAT])

    week4 = lambda f: f.filter(pl.col("week") == 4)[TRAILING_ACTUAL][0]  # noqa: E731
    assert week4(before) == week4(after)
    # And the tampering does reach week 5, or the test would pass vacuously.
    week5 = lambda f: f.filter(pl.col("week") == 5)[TRAILING_ACTUAL][0]  # noqa: E731
    assert week5(before) != week5(after)


def test_features_built_early_match_features_built_later(frame):
    """A frame built at week N equals the same slice of one built at week N+5.

    The second test the plan specifies. Truncating the season is what a live run
    at week 3 actually sees.
    """
    early = bl.trailing_features(frame.filter(pl.col("week") <= 3), stats=[STAT])
    late = bl.trailing_features(frame, stats=[STAT]).filter(pl.col("week") <= 3)
    assert early[TRAILING_ACTUAL].to_list() == late[TRAILING_ACTUAL].to_list()


def test_window_is_counted_in_appearances_not_weeks():
    """A missed week shortens the history rather than blanking the window."""
    sparse = usage_frame([
        ("00-0001", 1, 10.0, 10.0),
        ("00-0001", 8, 20.0, 20.0),   # weeks 2-7 missing
        ("00-0001", 9, 30.0, 30.0),
    ])
    out = bl.trailing_features(sparse, stats=[STAT], window=3)
    assert out[TRAILING_ACTUAL].to_list() == [None, 10.0, 15.0]


def test_history_does_not_cross_players(frame):
    """One player's history must not leak into another's."""
    two = pl.concat([frame, usage_frame([("00-0002", 5, 1.0, 1.0)])])
    out = bl.trailing_features(two, stats=[STAT])
    other = out.filter(pl.col("gsis_id") == "00-0002")
    assert other[TRAILING_ACTUAL][0] is None


def test_history_does_not_cross_seasons(frame):
    """Week 1 of a season starts from nothing, as a real week 1 does."""
    prior = frame.with_columns(pl.lit(2024).cast(pl.Int32).alias("season"))
    out = bl.trailing_features(pl.concat([prior, frame]), stats=[STAT])
    week1 = out.filter((pl.col("season") == 2025) & (pl.col("week") == 1))
    assert week1[TRAILING_ACTUAL][0] is None


# --- the as-of grid ------------------------------------------------------

def test_as_of_features_match_trailing_features_on_the_appearance_grid(frame):
    """The two builders must agree wherever they both apply.

    ``as_of_features`` exists to describe player-weeks with no usage row; on the
    rows that *do* have one it has to reproduce ``trailing_features`` exactly, or
    the model is fitted on one definition and scored on another.
    """
    shifted = bl.trailing_features(frame, stats=[STAT])
    as_of = bl.as_of_features(frame, frame.select(["gsis_id", "week"]),
                              stats=[STAT])
    joined = shifted.join(as_of, on=["gsis_id", "week"], suffix="_asof")
    assert (joined[TRAILING_ACTUAL].to_list()
            == joined[f"{TRAILING_ACTUAL}_asof"].to_list())


def test_as_of_features_cover_a_week_the_player_missed(frame):
    """A player with no usage row still gets the snapshot that preceded the week."""
    grid = pl.DataFrame({"gsis_id": ["00-0001"], "week": [6]},
                        schema_overrides={"week": pl.Int32})
    out = bl.as_of_features(frame, grid, stats=[STAT])
    # Weeks 3-5 are the last three appearances before week 6.
    assert out[TRAILING_ACTUAL][0] == pytest.approx((50.0 + 70.0 + 90.0) / 3)
    assert out["last_posteam"][0] == "SEA"
    assert out["weeks_of_history"][0] == 5


def test_as_of_features_refuse_two_seasons_at_once(frame):
    """Its window is partitioned by player only, so it must be given one season."""
    prior = frame.with_columns(pl.lit(2024).cast(pl.Int32).alias("season"))
    with pytest.raises(ValueError, match="one season at a time"):
        bl.as_of_features(pl.concat([prior, frame]),
                          frame.select(["gsis_id", "week"]), stats=[STAT])


def test_as_of_features_abstain_before_any_history(frame):
    """Week 1 has nothing to look back on, whoever asks."""
    grid = pl.DataFrame({"gsis_id": ["00-0001", "00-0009"], "week": [1, 4]},
                        schema_overrides={"week": pl.Int32})
    out = bl.as_of_features(frame, grid, stats=[STAT]).sort("week")
    assert out[TRAILING_ACTUAL].to_list() == [None, None]


# --- the fit and its abstentions ----------------------------------------

@pytest.fixture
def fitted():
    """A baseline fitted on a frame with a known linear relationship."""
    rng = np.random.default_rng(0)
    weeks, players = 12, 60
    rows = []
    for player in range(players):
        level = 20.0 + player
        for week in range(1, weeks + 1):
            expected = level + rng.normal(0, 2)
            rows.append((f"00-{player:04d}", week, expected + rng.normal(0, 3),
                         expected))
    train = usage_frame(rows).with_columns(pl.lit(2024).cast(pl.Int32).alias("season"))
    return bl.fit(bl.trailing_features(train, stats=[STAT]), stats=[STAT])


def test_fit_recovers_a_sensible_relationship(fitted):
    """The two coefficients should be positive and the fit non-degenerate."""
    fit = fitted.fits[STAT]
    assert fit.beta_actual + fit.beta_expected > 0.5
    assert fit.r2 > 0.5
    assert fit.n > 100


def test_predictions_are_clipped_at_zero():
    """A negative stat line would be priced by the blend, so it cannot escape."""
    negative = bl.UsageBaseline(
        fits={STAT: bl.StatFit(STAT, intercept=-50.0, beta_actual=0.0,
                               beta_expected=0.0, n=1000, r2=0.0)},
        train_seasons=(2024,),
    )
    features = pl.DataFrame({TRAILING_ACTUAL: [5.0], TRAILING_EXPECTED: [5.0]})
    assert negative.predict(features)[f"{bl.USAGE_PREFIX}{STAT}"][0] == 0.0


def test_model_abstains_without_trailing_opportunity(fitted):
    """No opportunity of this kind means no opinion, not the intercept.

    A wide receiver's trailing passing yards are zero, and a fit estimated on
    quarterbacks would hand him its intercept -- apparent full coverage, dragging
    the blend toward a positional average for exactly the players a board needs to
    tell apart. That is the failure the plan's positional-coverage risk names.
    """
    features = pl.DataFrame({
        TRAILING_ACTUAL: [0.0, 0.0, 12.0, None],
        TRAILING_EXPECTED: [0.0, 3.0, 0.0, 5.0],
    })
    predicted = features.pipe(fitted.predict)[f"{bl.USAGE_PREFIX}{STAT}"]
    assert predicted[0] is None            # no opportunity at all
    assert predicted[1] is not None        # expected opportunity, none realised
    assert predicted[2] is not None        # realised opportunity, none expected
    assert predicted[3] is None            # no history


def test_predict_season_refuses_an_in_sample_season(fitted):
    """Predicting a training season would make the residuals meaningless."""
    with pytest.raises(ValueError, match="in-sample"):
        bl.predict_season(fitted, 2024)


# --- the stat mapping, against the real pull ----------------------------

@pytest.mark.skipif(not opportunity_path(2025).is_file(),
                    reason="needs `Rscript R/GetUsage.R 2025 2025`")
def test_usage_stats_map_onto_the_real_columns():
    """Every mapped column must exist upstream, or a rename breaks silently."""
    columns = set(pl.read_parquet(opportunity_path(2025)).columns)
    for stat, (actual, expected) in USAGE_STATS.items():
        assert actual in columns, f"{stat}: {actual} is gone from ffopportunity"
        assert expected in columns, f"{stat}: {expected} is gone from ffopportunity"


@pytest.mark.skipif(not opportunity_path(2025).is_file(),
                    reason="needs `Rscript R/GetUsage.R 2025 2025`")
def test_pull_is_regular_season_and_uniquely_keyed():
    """The two guards in GetUsage.R, checked from this side of the boundary."""
    frame = pl.read_parquet(opportunity_path(2025),
                            columns=["season", "week", "gsis_id"])
    assert frame["week"].max() <= 18
    assert frame.height == frame.unique(["season", "week", "gsis_id"]).height
    assert frame["gsis_id"].null_count() == 0
