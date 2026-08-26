"""Archive the 2026 board with and without ``USG_``, so G2 can finally be answered.

Plan 18 leaves one question open and records it as **unmeasurable on history**:
does adding the usage head to the blend make the blend better? There is no
historical blend to test against — FantasyPros' URLs take no season parameter, so
the 2025 board cannot be reconstructed by re-scraping, and no earlier one survives.

Which makes 2026 the first and only chance, and makes it a chance with an expiry
date. The counterfactual has to be built from the *pre-season* board, before any
game is played. Once week 1 happens the inputs move and the question is gone for
another year.

So this writes both blends to ``Data/G2/<season>/``, committed rather than
gitignored — unlike ``Data/Store``, which is regenerable by definition and would be
overwritten by the next ``python -m Scripts.refresh``. That distinction is the whole
point of the file existing: **this is the one artifact in the repo that cannot be
rebuilt.**

It runs offline. Everything needed is already on the board: each source's stat line,
the imputation flags, and the scoring registry. Re-hitting ESPN would risk archiving
a board built from different inputs than the one being drafted from.

Scoring the archive against realised 2026 is :func:`score` and is deliberately not
run here — there is nothing to score yet.

Usage:
    python -m Scripts.lab.g2 --archive
    python -m Scripts.lab.g2 --score      # after the season, or mid-season
"""

import argparse
import json
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Sequence

import pandas as pd
import polars as pl

from Scripts.config_utils import build_lg_vars
from Scripts.paths import DATA_DIR, REPO_ROOT, store_dir
from Scripts.projection_utils import (
    DST_POSITIONS,
    WEIGHTS,
    _apply_scoring,
    compute_weighted_stats,
)
from Scripts.scoring import SLOT_BASE, SLOT_DST, get_scoring_table

#: Where the archive lives. Committed: see the module docstring.
G2_DIR = DATA_DIR / "G2"

#: The two blends under test. ``without_usg`` is not "the old weights" -- plan 03
#: never shipped a two-source blend -- it is the counterfactual the shipped weights
#: are being compared against, with USG's third redistributed over the sources that
#: would have carried it.
VARIANTS: Dict[str, Dict[str, float]] = {
    "with_usg": dict(WEIGHTS["default"]),
    "without_usg": {"ESPN": 0.5, "FP": 0.5, "PINNY": 0.0, "BOL": 0.0, "USG": 0.0},
}

#: Identity and context columns carried into the archive alongside the projections.
#: Enough to join to realised outcomes and to read the archive on its own terms a
#: year from now, without needing the 1,236-column board it came from.
CARRY = ("player_id", "name_key", "player_name", "primaryPosition", "pro_team",
         "adp", "auction_value", "tier", "vor", "vor_rank", "replacement_rank",
         "usg_arm", "usg_evidence", "usg_thin_evidence", "usg_expected_games",
         "sources_real", "bye_week")


def git_sha() -> Optional[str]:
    """The commit the archive was built at, or None outside a git checkout.

    Recorded because the archive is evidence, and evidence that cannot be traced to
    the code that produced it is an anecdote.
    """
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True,
            text=True, check=True).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def scoring_table(league_key: str, season: int, slot: str) -> pd.DataFrame:
    """One league-season's scoring table, from the registry, without a network call.

    ``proj_to_score`` resolves its table by passing a **live** ``espn_api`` League
    through, which is right for the pipeline and wrong here: this module exists to
    archive the board that was actually built, so it must not depend on ESPN still
    serving the same thing. ``verify=False`` is the same reason — there is no live
    league to verify against.
    """
    return get_scoring_table(league_key=league_key, season=season,
                             verify=False, slot=slot)


def score_offline(frame: pd.DataFrame, league_key: str, season: int,
                  prefixes: Sequence[str] = ("ESPN", "FP", "MEAN", "PINNY",
                                             "BOL", "USG", "TRUE")) -> pd.DataFrame:
    """``proj_to_score``'s per-slot logic, against the registry instead of a league.

    Reusing :func:`Scripts.projection_utils._apply_scoring` rather than
    reimplementing the summation is deliberate. A second copy of the scoring
    arithmetic is exactly the drift this repo has already paid for twice -- plan 11
    found a hardcoded per-league block whose IDP values were wrong against live
    settings, and plan 01 found a silent NaN rule. One implementation, two callers.
    """
    is_dst = frame["primaryPosition"].isin(DST_POSITIONS)
    if not is_dst.any():
        return _apply_scoring(
            frame, scoring_table(league_key, season, SLOT_BASE), list(prefixes))

    dst = frame[is_dst].copy()
    rest = frame[~is_dst].copy()
    _apply_scoring(dst, scoring_table(league_key, season, SLOT_DST), list(prefixes))
    _apply_scoring(rest, scoring_table(league_key, season, SLOT_BASE), list(prefixes))
    return pd.concat([rest, dst])


def blend(board: pd.DataFrame, league_key: str, season: int,
          weights: Dict[str, float]) -> pd.DataFrame:
    """Re-blend a board's source columns under one weighting, and score it.

    The same calls ``build_season_projections`` makes, on a frame that already carries
    every source and its imputation flags. Renormalisation is left on, so a source
    that abstained on a player still contributes nothing rather than a filled-in value
    wearing a real one's weight.

    **The team reconciliation runs here too, and has to.** It sits between the blend
    and the scoring in the real pipeline, so a variant built without it is not the
    blend this repo ships -- it would be a counterfactual against a straw man, and the
    archive's whole value is that the comparison is honest. ``test_lab_g2`` caught
    exactly that: re-blending without it missed the shipped board by up to 20.6 points.

    Args:
        board: A league's ``board.parquet`` as pandas.
        league_key: League whose scoring rules price the result.
        season: Season the board is for.
        weights: ``{source: weight}`` for every stat.

    Returns:
        pd.DataFrame: ``board`` plus recomputed ``TRUE_<stat>`` and
        ``<prefix>_Points`` columns.
    """
    from Scripts.injury.transfer import redistribute
    from Scripts.season_projections import reconcile_team_totals

    scoring = scoring_table(league_key, season, SLOT_BASE)
    stats = [c for c in scoring["colName"].dropna().unique()]
    out = compute_weighted_stats(df=board.copy(), stats_list=stats,
                                 weights_dict={"default": weights})
    # The shipping path's tail, in the same order, because a lab that reproduces a
    # different object than the board measures the wrong thing. Plan 28 phase 6's
    # vacancy transfer sits between the two reconciliations, and leaving it out here
    # was caught by `test_reblend_reproduces_the_shipped_board` rather than by anyone
    # noticing the lab and the board had drifted apart.
    out = reconcile_team_totals(out)
    out = redistribute(out)
    out = reconcile_team_totals(out)
    return score_offline(out, league_key, season)


def archive_league(league_key: str, season: int) -> Optional[Dict]:
    """Write one league's two-variant archive.

    Args:
        league_key: League to archive.
        season: Season the board is for.

    Returns:
        dict | None: Summary of what was written, or None when the board is absent.
    """
    board_path = store_dir(season, league_key) / "board.parquet"
    if not board_path.is_file():
        return None

    board = pl.read_parquet(board_path).to_pandas()
    scoring = scoring_table(league_key, season, SLOT_BASE)
    stats = [c for c in scoring["colName"].dropna().unique()]

    frames = {}
    for name, weights in VARIANTS.items():
        scored = blend(board, league_key, season, weights)
        keep = [c for c in CARRY if c in scored.columns]
        keep += [f"TRUE_{s}" for s in stats if f"TRUE_{s}" in scored.columns]
        keep += [c for c in scored.columns if c.endswith("_Points")]
        frame = scored[keep].copy()
        frame["variant"] = name
        frame["league_key"] = league_key
        frame["season"] = season
        # Within-position rank under this blend, which is what a G2 comparison
        # actually reads. Computed here rather than at scoring time so the archive
        # answers the question without needing this module to still exist.
        frame["variant_pos_rank"] = (
            frame.groupby("primaryPosition")["TRUE_Points"]
            .rank(ascending=False, method="min"))
        frames[name] = frame

    out_dir = G2_DIR / str(season)
    out_dir.mkdir(parents=True, exist_ok=True)
    combined = pd.concat(frames.values(), ignore_index=True)
    path = out_dir / f"{league_key}.parquet"
    pl.from_pandas(combined).write_parquet(path)

    # How far apart the two blends actually are. If this is near zero the whole
    # exercise is moot, and it is better to find that out now than in February.
    wide = frames["with_usg"].merge(
        frames["without_usg"][["player_id", "TRUE_Points", "variant_pos_rank"]],
        on="player_id", suffixes=("_with", "_without"))
    priced = wide[wide["TRUE_Points_with"].notna()
                  & wide["TRUE_Points_without"].notna()]
    moved = (priced["variant_pos_rank_with"]
             != priced["variant_pos_rank_without"]).sum()

    return {
        "league_key": league_key,
        "rows": int(len(board)),
        "path": str(path.relative_to(REPO_ROOT)),
        "players_priced_both": int(len(priced)),
        "players_whose_position_rank_moved": int(moved),
        "mean_abs_points_difference": float(
            (priced["TRUE_Points_with"] - priced["TRUE_Points_without"]).abs().mean()),
        "max_abs_points_difference": float(
            (priced["TRUE_Points_with"] - priced["TRUE_Points_without"]).abs().max()),
    }


def archive(season: int, leagues: Optional[Sequence[str]] = None) -> Dict:
    """Archive every league's two blends and write the manifest.

    Args:
        season: Season to archive.
        leagues: League keys. Defaults to every league in the config.

    Returns:
        dict: The manifest, as written.
    """
    keys = (list(leagues) if leagues
            else sorted({lg["key"] for lg in build_lg_vars().values()}))

    results = [r for r in (archive_league(k, season) for k in keys) if r]

    manifest = {
        "season": season,
        "archived_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "git_sha": git_sha(),
        "variants": VARIANTS,
        "question": (
            "Does including USG_ in the blend improve the blend? Score both "
            "variants against realised 2026 with Scripts.lab.g2 --score. This "
            "archive cannot be rebuilt once the season starts."),
        "leagues": results,
    }
    out_dir = G2_DIR / str(season)
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "manifest.json").open("w") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
    return manifest


def score(season: int) -> Optional[pd.DataFrame]:
    """Score both archived variants against realised production.

    Deliberately separate from :func:`archive`, and useless until games have been
    played. Realised season totals come from ``player_weeks.parquet``, which
    ``Rscript R/GetUsage.R`` refreshes, so this can be run mid-season for a
    partial read as well as after week 18 for the real one.

    Args:
        season: Season to score.

    Returns:
        pd.DataFrame | None: One row per league and variant, or None when the
        archive or the realised data is missing.
    """
    from Scripts.usage import features as ft

    out_dir = G2_DIR / str(season)
    if not out_dir.is_dir():
        return None
    try:
        weekly = ft.load_player_weeks([season])
    except FileNotFoundError:
        return None

    totals = ft.season_totals(weekly)
    crosswalk = pl.read_parquet(DATA_DIR / "NFL" / "player_ids.parquet")
    id_columns = [c for c in ("gsis_id", "espn_id") if c in crosswalk.columns]
    if len(id_columns) < 2:
        return None
    realised = totals.join(crosswalk.select(id_columns), on="gsis_id", how="inner")

    rows = []
    for path in sorted(out_dir.glob("*.parquet")):
        archived = pl.read_parquet(path)
        joined = archived.with_columns(
            pl.col("player_id").cast(pl.String)
        ).join(
            realised.with_columns(pl.col("espn_id").cast(pl.String)),
            left_on="player_id", right_on="espn_id", how="inner")
        if not joined.height:
            continue
        for variant in joined["variant"].unique().to_list():
            part = joined.filter(
                (pl.col("variant") == variant)
                & pl.col("TRUE_Points").is_not_null())
            if part.height < 20:
                continue
            ranked = part.with_columns(
                pl.col("TRUE_Points").rank().over("primaryPosition").alias("_rp"),
                pl.col("games").rank().over("primaryPosition").alias("_ra"))
            rows.append({
                "league_key": path.stem,
                "variant": variant,
                "n": part.height,
                "within_position_spearman_vs_games":
                    float(ranked.select(pl.corr("_rp", "_ra")).item() or 0.0),
            })
    return pd.DataFrame(rows) if rows else None


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.lab.g2",
        description="Archive the pre-season board with and without USG_, for G2.")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--archive", action="store_true")
    parser.add_argument("--score", action="store_true")
    parser.add_argument("--league", action="append", default=[])
    args = parser.parse_args(argv)

    if not args.archive and not args.score:
        parser.error("pass --archive or --score")

    if args.archive:
        manifest = archive(args.season, args.league or None)
        print(f"Archived {len(manifest['leagues'])} leagues to "
              f"Data/G2/{args.season}/\n")
        print(f"  {'league':26}{'rows':>7}{'both':>7}{'ranks moved':>13}"
              f"{'mean |Δpts|':>13}{'max |Δpts|':>12}")
        for entry in manifest["leagues"]:
            print(f"  {entry['league_key']:26}{entry['rows']:>7}"
                  f"{entry['players_priced_both']:>7}"
                  f"{entry['players_whose_position_rank_moved']:>13}"
                  f"{entry['mean_abs_points_difference']:>13.2f}"
                  f"{entry['max_abs_points_difference']:>12.2f}")
        print(f"\n  git {manifest['git_sha'][:12] if manifest['git_sha'] else '—'}"
              f"  ·  {manifest['archived_at']}")
        print("\n  Commit this directory. It cannot be rebuilt once week 1 is "
              "played.")

    if args.score:
        result = score(args.season)
        if result is None:
            print(f"Nothing to score yet for {args.season} — no archive, or no "
                  f"realised data. Re-run after `Rscript R/GetUsage.R "
                  f"{args.season} {args.season}`.")
        else:
            print(result.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
