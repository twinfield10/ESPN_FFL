"""Build the season usage projection as a **projection source**.

:mod:`Scripts.usage.season` holds the model and :mod:`Scripts.usage.backtest`
measures it. Neither produces anything the blend can consume: the backtest fits and
predicts in memory for a past season, and the fitted coefficients persist as JSON
that, until now, nothing read back.

This module closes that gap. It emits one file per season --
``Data/Projections/Usage/Season/<season>/Usage_SeasonProjections.parquet`` -- in the
shape the other three season loaders in :mod:`Scripts.season_projections` already
produce, so ``USG`` becomes the fifth entry in ``WEIGHTS`` rather than a special
case threaded through the pipeline.

Two things here are deliberately unlike the other sources.

**It joins on an id, not a name.** The model keys on ``gsis_id`` and the ESPN
universe keys on ``player_id``; :mod:`Scripts.crosswalk` maps between them at
98.5-99% coverage. Every other season source joins on a normalised name, which is
why ``_disambiguate_name_keys`` has to exist -- with two Lamar Jacksons and two
Justin Jeffersons in an IDP pool, a name join hands the receiver's line to the
linebacker. ``USG`` sidesteps that entirely, and is the first source to do so.

**It records where it abstains.** The model speaks for roughly 80% of rostered
players and declines the rest -- no prior season, or a position it measured worse on
(:data:`Scripts.usage.season.ABSTAIN_POSITIONS`). An abstention has to reach the
blend as *absence*, not as zero. ``compute_weighted_stats`` reads a missing
``_is_imputed`` companion as "this source is real here", and fills a null value with
0.0 -- so an unflagged abstention would enter the blend as a confident zero and drag
every declined player toward it. The flags are what make the weight get dropped and
the remaining sources renormalised.

That failure mode is the one ``docs/STATE_OF_THE_REPO.md`` already names: *a 0.0
that means "nothing here" is indistinguishable from one that means "zero"*.

Usage::

    python -m Scripts.usage.project --season 2026
"""

from __future__ import annotations

import json
from typing import List, Optional, Sequence

import polars as pl

from Scripts.paths import season_dir
from Scripts.usage import features as ft
from Scripts.usage import season as sn

#: Provider directory name, matching ``Data/Projections/<source>/Season/<year>/``.
SOURCE = "Usage"

#: Output filename beneath that directory.
FILENAME = "Usage_SeasonProjections.parquet"

#: First season of training history.
HISTORY_START = 2016

#: Columns carried through beside the stat lines, for the board to show.
#:
#: Plan 18 asks for "18 points per game x 14.2 games" to stay visible rather than
#: collapsing into one number, and a drafter wants the volume terms next to the
#: projection. These are diagnostics, not stats -- they carry no ``USG_`` prefix, so
#: ``proj_to_score`` will not try to price them.
CONTEXT_COLUMNS = ("expected_games", "games_sd", "games_low", "games_high",
                   "usg_arm", "position",
                   "pred_targets_pg", "pred_carries_pg", "pred_pass_attempts_pg")


def projection_path(season: int, create: bool = False):
    """Where this season's artifact lives.

    Args:
        season: Season year.
        create: Create the directory. Readers pass False -- see
            :func:`Scripts.paths.season_dir`.

    Returns:
        Path: Absolute path to the parquet file.
    """
    return season_dir(SOURCE, season, FILENAME, create=create)


def is_stale(model: sn.SeasonUsageModel, season: int) -> bool:
    """Whether a fitted model is missing training seasons it could have had.

    A season projection for 2026 should be fitted through 2025. The persisted
    artifact carries its own ``train_seasons``, so this is checkable rather than
    assumed -- and it caught a real case: the model saved on 2026-08-07 trained on
    2017-2024, because it was written by a walk-forward whose last fold predicted
    2025 and therefore trained through 2024.

    Args:
        model: A loaded model.
        season: Season being projected.

    Returns:
        bool: True when the model trained on less than everything available.
    """
    return not model.train_seasons or max(model.train_seasons) < season - 1


def load_or_fit(season: int, refit: bool = False,
                history_start: int = HISTORY_START) -> sn.SeasonUsageModel:
    """The fitted model for this season, from disk when it is current.

    Args:
        season: Season being projected.
        refit: Fit from scratch even when a current model exists.
        history_start: First season of training history.

    Returns:
        SeasonUsageModel: Fitted through ``season - 1``.
    """
    from datetime import datetime

    if not refit:
        path = sn.SeasonUsageModel.default_path()
        if path.exists():
            model = sn.SeasonUsageModel.load(path)
            if not is_stale(model, season):
                print(f"  model: loaded {path.name}, "
                      f"trained {min(model.train_seasons)}-{max(model.train_seasons)}")
                return model
            print(f"  model: {path.name} trained through "
                  f"{max(model.train_seasons) if model.train_seasons else 'nothing'}, "
                  f"refitting through {season - 1}")

    train_seasons = list(range(history_start + 1, season))
    train = sn.training_frame(train_seasons, history_start)
    model = sn.fit(
        train, train_seasons,
        fitted_at=datetime.now().astimezone().isoformat(timespec="seconds"))
    saved = model.save()
    print(f"  model: fitted on {train.height} player-seasons "
          f"({min(train_seasons)}-{max(train_seasons)}), saved {saved.name}")
    return model


def attach_espn_id(frame: pl.DataFrame) -> pl.DataFrame:
    """Map ``gsis_id`` to ESPN's ``player_id`` through the crosswalk.

    Args:
        frame: Prediction frame carrying ``gsis_id``.

    Returns:
        pl.DataFrame: ``frame`` with a nullable integer ``player_id``.
    """
    from Scripts.crosswalk import id_map

    mapping = id_map("gsis_id", "espn_id")
    return frame.with_columns(
        pl.col("gsis_id").replace_strict(mapping, default=None)
        .cast(pl.Float64).cast(pl.Int64).alias("player_id")
    )


def build(season: int, refit: bool = False,
          history_start: int = HISTORY_START,
          abstain_positions: Optional[Sequence[str]] = None) -> pl.DataFrame:
    """Project ``season`` and return the source frame.

    Args:
        season: Season to project.
        refit: Force a refit rather than loading the persisted coefficients.
        history_start: First season of training history.
        abstain_positions: Override
            :data:`Scripts.usage.season.ABSTAIN_POSITIONS`.

    Returns:
        pl.DataFrame: One row per player -- ``player_id``, ``gsis_id``,
        ``full_name``, the :data:`CONTEXT_COLUMNS`, and ``USG_<stat>`` with a
        ``USG_<stat>_is_imputed`` companion that is True wherever the model
        abstained.
    """
    print(f"\n===== Usage season projection: {season} =====")
    model = load_or_fit(season, refit=refit, history_start=history_start)

    history = list(range(history_start, season))
    features = ft.season_features(season, history)
    print(f"  features: {features.height} players")

    predicted = model.predict(features, abstain_positions=abstain_positions)

    # The second moment on games played. Point estimates are the weakest thing this
    # model reports (R-squared 0.19), and a board that shows 13.6 games without
    # showing that the p10 is 7 invites reading it as a forecast.
    predicted = model.games_interval(predicted)

    stat_columns = [f"{sn.USAGE_PREFIX}{stat}" for stat in sn.STAT_TERMS
                    if f"{sn.USAGE_PREFIX}{stat}" in predicted.columns]

    # The provenance flags, and the reason this function exists. A null here means
    # the model declined -- no prior season, a declined position, or no opportunity
    # of that kind -- and the blend has to see that as an absent source rather than
    # as a projection of zero.
    frame = predicted.with_columns(
        [pl.col(column).is_null().alias(f"{column}_is_imputed")
         for column in stat_columns]
    )

    frame = attach_espn_id(frame)

    # A normalised name beside the id, because the crosswalk does not yet carry
    # 2026 rookies -- 95 of them resolve to no ESPN id, and they are exactly the
    # population the rookie arm exists to project. Joining on the id alone would
    # drop the model's one clearly measured win. `build_season_projections` uses
    # this only where the id is missing, so the collision safety of an id join is
    # kept for everyone it covers.
    # Imported inside the function: `Scripts.season_projections` gains a
    # `load_usage_season` that reads this module's output, so a module-level import
    # in either direction would be a cycle.
    from Scripts.season_projections import normalise_name

    frame = frame.with_columns(
        pl.col("full_name")
        .map_elements(normalise_name, return_dtype=pl.Utf8)
        .alias("name_key")
    )

    keep = (["player_id", "gsis_id", "name_key", "full_name"]
            + [c for c in CONTEXT_COLUMNS if c in frame.columns]
            + stat_columns
            + [f"{c}_is_imputed" for c in stat_columns])
    out = frame.select([c for c in keep if c in frame.columns])

    speaks = out.filter(pl.col("usg_arm") != "abstain")
    matched = out.filter(pl.col("player_id").is_not_null())
    print(f"  projected: {speaks.height} of {out.height} "
          f"({speaks.height / max(out.height, 1):.1%}); "
          f"abstained {out.height - speaks.height}")
    print(f"  crosswalk: {matched.height} of {out.height} "
          f"({matched.height / max(out.height, 1):.1%}) resolved to an ESPN id")
    for arm, count in sorted(out.group_by("usg_arm").len().rows()):
        print(f"    {arm:<9} {count}")

    return out


def write(season: int, frame: pl.DataFrame):
    """Persist the source frame, with a metadata sidecar.

    Args:
        season: Season year.
        frame: Output of :func:`build`.

    Returns:
        Path: Where it was written.
    """
    path = projection_path(season, create=True)
    frame.write_parquet(path)

    meta = sn.SeasonUsageModel.default_path()
    (path.parent / "meta.json").write_text(json.dumps({
        "season": season,
        "rows": frame.height,
        "projected": int(frame.filter(pl.col("usg_arm") != "abstain").height),
        "model_version": sn.MODEL_VERSION,
        "model_file": meta.name,
        "abstain_positions": list(sn.ABSTAIN_POSITIONS),
    }, indent=2, sort_keys=True))
    print(f"  wrote {path}")
    return path


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    import argparse

    from Scripts.nfl_utils import current_season

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--season", type=int, default=None,
                        help="Season to project. Defaults to the current one.")
    parser.add_argument("--refit", action="store_true",
                        help="Refit rather than loading persisted coefficients.")
    parser.add_argument("--all-positions", action="store_true",
                        help="Project every position, including the ones the "
                             "model measured worse on.")
    args = parser.parse_args(argv)

    season = args.season if args.season is not None else current_season()
    frame = build(season, refit=args.refit,
                  abstain_positions=() if args.all_positions else None)
    write(season, frame)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
