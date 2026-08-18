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
                   "usg_arm", "usg_evidence", "usg_thin_evidence", "position",
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


#: Games in a prior season below which its rates are treated as thin.
#:
#: Eight, and it is the largest single error inflator this model has: on the
#: 2019-2025 walk-forward, players whose prior season ran under 8 games ordered
#: **42% worse** than the pool median, measured as within-position rank error as a
#: share of the position pool.
THIN_PRIOR_GAMES = 8

#: Quantile of prior-season volume below which a projection is treated as thin.
LOW_VOLUME_QUANTILE = 0.25

#: What a thin-evidence flag is worth, measured rather than asserted.
#:
#: Median |rank error| as a share of the position pool, 2019-2025 walk-forward,
#: baseline **0.096**:
#:
#: ===========================  ======  ========  ==========
#: condition                         n    median    vs base
#: thin prior season (<8 games)   1,330     0.137       +42%
#: changed teams                  1,121     0.127       +32%
#: low prior volume (bottom q)      932     0.118       +23%
#: ---------------------------  ------  --------  ----------
#: no second prior season         2,525     0.089        -7%
#: rookie arm                     1,497     0.083       -14%
#: ===========================  ======  ========  ==========
#:
#: The last two are the point of measuring. Both look like thin evidence and neither
#: is: a rookie projected from draft capital orders **better** than the pool, which is
#: the rookie arm's whole result (rho ~ 0.64 against ~0), and a player with only one
#: prior season is no worse than one with two. Flagging them would have marked the
#: model's strongest arm as its weakest.
THIN_EVIDENCE_REASONS = ("thin prior season", "changed teams", "low prior volume")


def attach_evidence(frame: pl.DataFrame) -> pl.DataFrame:
    """Name the conditions under which this projection orders players worse.

    ``USG_PosRankDelta`` reads the same whether the model is standing on nine
    seasons of stable usage or extrapolating from four games at a new team. This
    says which, in words, so the disagreement can be weighed at the point of
    decision rather than taken flat.

    Only the three conditions in :data:`THIN_EVIDENCE_REASONS` are flagged, because
    only those measured. See that constant for the table.

    Args:
        frame: Prediction frame, after :meth:`SeasonUsageModel.predict`.

    Returns:
        pl.DataFrame: ``frame`` plus ``usg_evidence`` -- a semicolon-joined list of
        reasons, empty where none apply -- and ``usg_thin_evidence``.
    """
    veteran = pl.col("usg_arm") == "veteran"

    columns = [c for c in (f"{ft.LAG1_PREFIX}targets_pg",
                           f"{ft.LAG1_PREFIX}carries_pg",
                           f"{ft.LAG1_PREFIX}pass_attempts_pg")
               if c in frame.columns]
    volume = (pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in columns])
              if columns else pl.lit(0.0))

    # The threshold is computed as its own column first. `quantile(...).over(...)` is
    # not a window aggregation in polars and silently yields nulls, which made the
    # comparison never fire -- the flag was simply absent rather than wrong, which is
    # the kind of quiet failure a count check catches and a spot check does not.
    scored = frame.with_columns(volume.alias("_volume"))
    # Masked to veterans before the quantile. Rookies and abstentions carry no prior
    # volume, which fills to 0.0 -- and there are enough of them that they *are* the
    # bottom quartile, putting the cut at 0.0 and making the comparison unsatisfiable.
    # The flag was silently never set.
    scored = scored.with_columns(
        pl.when(veteran).then(pl.col("_volume")).otherwise(None)
        .quantile(LOW_VOLUME_QUANTILE).over("position").alias("_low_cut"))

    reasons = {
        # Rookies are excluded from all three. They have no prior season by
        # definition, so every veteran test would fire on them -- and they order
        # *better* than the pool, so flagging them would invert the meaning.
        "thin prior season": veteran & (
            pl.col(f"{ft.LAG1_PREFIX}games").fill_null(99) < THIN_PRIOR_GAMES),
        "changed teams": veteran & pl.col("team_changed").fill_null(False),
        "low prior volume": veteran & (pl.col("_volume") < pl.col("_low_cut")),
    }

    scored = scored.with_columns(
        [expression.alias(f"_reason_{i}")
         for i, expression in enumerate(reasons.values())])

    # `None` rather than an empty string for a reason that does not apply, so
    # `ignore_nulls` drops it outright. Empty strings survive the concat and leave
    # "thin prior season; ; " behind, which then needs unpicking with regexes.
    label = pl.concat_str(
        [pl.when(pl.col(f"_reason_{i}")).then(pl.lit(name)).otherwise(None)
         for i, name in enumerate(reasons)],
        separator="; ", ignore_nulls=True)

    return (scored.with_columns(
        label.fill_null("").alias("usg_evidence"),
        pl.any_horizontal([pl.col(f"_reason_{i}") for i in range(len(reasons))])
        .alias("usg_thin_evidence"))
        .drop(["_volume", "_low_cut"]
              + [f"_reason_{i}" for i in range(len(reasons))]))


def to_full_slate(frame: pl.DataFrame, columns: Sequence[str],
                  slate: float = sn.DEFAULT_TARGET_SLATE) -> pl.DataFrame:
    """Rescale the stat lines from expected games to a full healthy season.

    **The model predicts an expected value and the blend needs an if-healthy line.
    Those are different quantities, and mixing them is a measured error rather than a
    stylistic one.**

    :meth:`SeasonUsageModel.predict` multiplies per-game production by *expected*
    games -- about 13.6 for a rostered starter -- because that is what predicts a
    realised season, and it is what :mod:`Scripts.usage.backtest` scores. ESPN and
    FantasyPros project a healthy 17-game slate and apply no availability discount at
    all. Blending the two at equal weight therefore produced something that was
    neither, and did so **unevenly across positions**: the usage model covers
    QB/RB/WR/TE and not kickers or team defences, so on the 2026 board the skill
    positions came out at 0.887-0.900 of their ESPN/FantasyPros level while K and
    D/ST sat at exactly 1.000. Roughly 11% of cross-position distortion, in a blend
    whose entire job is to be comparable across positions.

    Rescaling here rather than in the model keeps both quantities available and each
    where it belongs: the backtest keeps measuring the expected-value line against
    realised outcomes, and the blend receives a line on the same footing as its
    neighbours. The availability estimate is not lost -- it travels as
    ``usg_expected_games`` with its own interval, which is where a per-player discount
    should be applied if one is wanted, and applied to the *whole* blend rather than
    to one third of it.

    **Dividing out each player's own ``expected_games`` is the right mechanism, and
    it was tested against the alternative rather than assumed.** The obvious worry is
    that ``17/expected_games`` is an unbounded multiplier -- x1.17 at 14.5 games,
    x5.97 at 3.0. It is not amplifying error: it exactly inverts the multiplication
    :meth:`predict` just performed, recovering per-game production, which is estimated
    independently of the games term. What it does do is flatten every player onto 17
    games, which is correct for a starter and wrong for a man who will not play.

    Scaling by a per-position constant instead -- lifting the expected-value line so a
    typical starter lands on a full slate -- was built and measured on the 2026
    Knights board. Median ``USG_Points / ESPN_Points`` by ADP band, over players the
    role gate keeps:

    ========================  =====  ======  =======  =======
    basis                      1-50  50-100  100-150  150-200
    ========================  =====  ======  =======  =======
    per-player (this)          0.94    0.95     1.03     1.27
    per-position constant      0.94    0.94     1.00     1.11
    ========================  =====  ======  =======  =======

    **It was rejected despite the better tail**, because of what it costs at the top.
    Retaining the availability term in the line means applying it, and the
    availability head is this model's weak arm -- plan 18 measures prior-season games
    against next season at r = +0.343. On the 2026 board it took Jayden Daniels from
    286.7 to 214.1 and Joe Burrow from 276.2 to 214.1: a 25% haircut on two top-six
    quarterbacks, sourced from the least trustworthy thing the model reports, for no
    gain in the first hundred picks. ``TRUE_Points`` is meant to be differenceable
    against ESPN, and ESPN applies no such discount.

    So the availability estimate stays out of the line and travels as
    ``usg_expected_games`` instead, where it can be applied deliberately and to the
    *whole* blend rather than to one quarter of it.

    The 0.94 at the top is not an error to tune away either. The model shrinks toward
    positional baselines while ESPN extrapolates, and draftable players are selected
    on being top-of-pool, so it is genuinely below ESPN there.

    **What this function cannot fix is the tail**, and that is not a units problem:
    ESPN prices depth-chart role and had Mac Jones at 8.3 points against a usage line
    of 169.6. ``_withdraw_usage_on_role`` in :mod:`Scripts.season_projections` handles
    it by withdrawing the source outright for backups ESPN has priced out. With that
    gate in place the 150-200 band lands at 1.00 across every position, K and D/ST
    included.

    Args:
        frame: Prediction frame carrying ``expected_games`` and the columns to scale.
        columns: Stat and interval columns to rescale.
        slate: Games a healthy season offers.

    Returns:
        pl.DataFrame: ``frame`` with those columns on a full-slate basis.
    """
    if "expected_games" not in frame.columns:
        return frame

    # Guarded: a zero or null expected-games would divide a projection by nothing.
    # Those rows are abstentions and their stat lines are already null, so the ratio
    # is only ever applied to rows that have something to scale.
    ratio = (pl.lit(float(slate))
             / pl.col("expected_games").cast(pl.Float64))
    safe = pl.when(pl.col("expected_games").cast(pl.Float64) > 0).then(ratio) \
             .otherwise(None)

    return frame.with_columns(
        [(pl.col(column).cast(pl.Float64) * safe).alias(column)
         for column in columns if column in frame.columns])


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

    # And an interval on each stat line itself.
    predicted = model.stat_intervals(predicted)

    # Which conditions, if any, make this projection order players worse.
    predicted = attach_evidence(predicted)

    stat_columns = [f"{sn.USAGE_PREFIX}{stat}" for stat in sn.STAT_TERMS
                    if f"{sn.USAGE_PREFIX}{stat}" in predicted.columns]
    # The interval columns travel with the stat lines but are not stat lines: they
    # must not be scored by `proj_to_score` or blended, so they are carried through
    # `CONTEXT_COLUMNS` rather than picked up by the `USG_` prefix scan.
    interval_columns = [f"{c}{suffix}" for c in stat_columns
                        for suffix in ("_sd", "_low", "_high")
                        if f"{c}{suffix}" in predicted.columns]

    predicted = to_full_slate(predicted, stat_columns + interval_columns)

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
            + interval_columns
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
    # `slate` is recorded because "USG_receivingYards" is meaningless without it: the
    # stat lines are an if-healthy season, not the expected value the model predicts,
    # and a reader comparing two artifacts needs to know which.
    (path.parent / "meta.json").write_text(json.dumps({
        "season": season,
        "rows": frame.height,
        "projected": int(frame.filter(pl.col("usg_arm") != "abstain").height),
        "model_version": sn.MODEL_VERSION,
        "model_file": meta.name,
        "abstain_positions": list(sn.ABSTAIN_POSITIONS),
        "slate": sn.DEFAULT_TARGET_SLATE,
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
