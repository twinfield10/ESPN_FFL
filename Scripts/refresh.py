"""Build the local data store. The slow, explicit half of the app.

Everything expensive happens here so that nothing expensive happens in a render
path. One league is roughly 8s pre-season and rises toward 23s once a full season
of box scores exists; reading the result back from parquet is 0.01s. That gap is
the whole reason the store exists -- see
``docs/plans/07-frontend-foundation.md``.

    python -m Scripts.refresh --league Knights_FFL
    python -m Scripts.refresh --all
    python -m Scripts.refresh --all --what board            # draft boards
    python -m Scripts.refresh --all --what draft            # picks + tendencies
    python -m Scripts.refresh --all --what lineups,team_stats
    python -m Scripts.refresh --league Knights_FFL --season 2025

Ingest is not reimplemented here. :func:`Scripts.equivalence.build_league_frame`
is the single path from ESPN to a blended frame, and it is what the equivalence
harness snapshots -- so the store cannot drift from what that harness verifies.

``team_stats`` is opt-in because it re-derives a league's entire history: for
Winfield_Football that is 2016-2026, eleven seasons of box scores. Nothing about
the current week changes 2019, so it does not belong in a weekly refresh.
"""

import argparse
import sys
import time
from typing import Dict, List, Optional, Sequence, Tuple

from Scripts import store
from Scripts.config_utils import build_lg_vars, get_season, resolve_league
from Scripts.paths import REPO_ROOT

#: Artifacts ``--what`` accepts. ``draft`` builds two of them -- the pick history
#: and the owner tendencies read off it.
WHAT_CHOICES = ("lineups", "team_stats", "board", "draft")

#: Built unless ``--what`` says otherwise. ``team_stats`` is excluded on purpose --
#: see the module docstring. ``board`` is excluded because it is a pre-season
#: artifact: nothing about week 9 changes your draft. ``draft`` is excluded for a
#: stronger version of the same reason: a finished draft never changes at all, so
#: rebuilding it weekly re-reads ten seasons to write the same bytes.
DEFAULT_WHAT = ("lineups",)


def _log(msg: str) -> None:
    """Print a progress line, flushed.

    The app streams this from a subprocess, and Python buffers stdout when it is
    not a terminal -- without the flush the user watches an empty box for the
    whole run.

    Args:
        msg: Line to print.
    """
    print(msg, flush=True)


def refresh_league(
    name: str,
    season: int,
    what: Sequence[str] = DEFAULT_WHAT,
) -> Dict[str, float]:
    """Build one league-season's store.

    Args:
        name: League display name or config key.
        season: Season year.
        what: Artifacts to build, from :data:`WHAT_CHOICES`.

    Returns:
        dict: Per-step elapsed seconds, for the summary table.

    Raises:
        ValueError: On an unknown league or an unknown artifact name.
        Exception: Whatever ingest raises -- ``ESPNAccessDenied`` for expired
            cookies, ``FileNotFoundError`` for a missing schedule, and so on.
            Callers doing more than one league should catch per league.
    """
    unknown = [w for w in what if w not in WHAT_CHOICES]
    if unknown:
        raise ValueError(
            f"Unknown --what value(s) {unknown}. Known: {list(WHAT_CHOICES)}."
        )

    cfg = resolve_league(name)
    league_key = cfg["key"]
    timings: Dict[str, float] = {}

    _log(f"\n===== {cfg['display_name']} ({league_key}) {season} =====")

    lineups = None
    team_stats = None
    board = None
    draft = None
    tendencies = None
    league = None

    def _league():
        """Fetch the league once, however many artifacts end up needing it."""
        nonlocal league
        if league is None:
            from Scripts.fetch_utils import fetch_league
            league = fetch_league(league_id=cfg["ID"], year=season,
                                  swid=cfg["SWID"], espn_s2=cfg["ESPN_S2"])
        return league

    if "lineups" in what:
        # Deferred: the ESPN and blend stack is several seconds of imports, and a
        # --what team_stats run does not need it.
        from Scripts.equivalence import build_league_frame

        start = time.time()
        lineups, league = build_league_frame(name, season, return_league=True)
        timings["lineups"] = time.time() - start
        _log(f"  lineups     {lineups.shape[0]:>6} rows x {lineups.shape[1]:>3} cols "
             f"  {timings['lineups']:.2f}s")

    if "board" in what:
        from Scripts.draft.adp import fetch_draft_market, market_summary
        from Scripts.draft.board import board_summary, build_board
        from Scripts.season_projections import build_season_projections

        start = time.time()
        # One request per league, not one per run: the pool ESPN returns reflects
        # that league's roster slots, so the IDP league's response carries
        # individual defenders and nobody else's does. See adp._MARKET_CACHE.
        market = fetch_draft_market(_league(), season)
        _log(f"  market      {market_summary(market)}")

        projections = build_season_projections(_league(), season=season,
                                               market=market)
        board = build_board(_league(), projections, market, season=season)
        timings["board"] = time.time() - start
        _log(f"  board       {board_summary(board)}")
        _log(f"  board       {board.shape[0]:>6} rows x {board.shape[1]:>4} cols "
             f"  {timings['board']:.2f}s")

    if "draft" in what:
        from Scripts.draft.history import fetch_draft_history, history_summary
        from Scripts.draft.tendencies import build_tendencies, tendencies_summary

        start = time.time()
        # Every season the league has existed, not just this one: a tendency is
        # the whole point and one draft is not a tendency. No League object is
        # needed -- this is one JSON request per season.
        seasons = range(int(cfg["start"]), season + 1)
        history = fetch_draft_history(cfg["ID"], seasons, swid=cfg["SWID"],
                                      espn_s2=cfg["ESPN_S2"], current_season=season)
        timings["draft"] = time.time() - start
        _log(f"  draft       {history_summary(history)}   {timings['draft']:.2f}s")

        if history.is_empty():
            # Pre-draft in a league's first season. Writing an empty artifact
            # would make the page offer a tendencies table with nothing in it.
            _log("  draft       no drafts on record yet; nothing written")
        else:
            owners = build_tendencies(history)
            _log(f"  tendencies  {tendencies_summary(owners)}")
            # Pandas at the store boundary: write_league_store's contract is
            # pandas, and one conversion here is cheaper than a second write path.
            draft = history.to_pandas()
            tendencies = owners.to_pandas()

    if "team_stats" in what:
        from Scripts.scrape_team_stats import scrape_team_stats

        start_year = int(cfg["start"])
        if season <= start_year:
            # scrape_team_stats normalises every season's scores against the
            # median of `end_year - 1`; with a single season in the frame that
            # lookup has nothing to divide by.
            _log(f"  team_stats  skipped: {season} is this league's first season "
                 f"({start_year}), and the adjusted-score baseline needs at least "
                 f"one prior season.")
        else:
            start = time.time()
            team_stats = scrape_team_stats(
                league_id=cfg["ID"], start_year=start_year, end_year=season,
                swid=cfg["SWID"], espn_s2=cfg["ESPN_S2"],
            )
            timings["team_stats"] = time.time() - start
            _log(f"  team_stats  {team_stats.shape[0]:>6} rows x "
                 f"{team_stats.shape[1]:>3} cols   {timings['team_stats']:.2f}s "
                 f"({start_year}-{season})")

    if all(artifact is None for artifact in
           (lineups, team_stats, board, draft, tendencies)):
        _log("  nothing to write")
        return timings

    # Deferred for the same reason as build_league_frame above.
    from Scripts.projection_utils import weekly_sources_present

    start = time.time()
    directory = store.write_league_store(
        season, league_key,
        lineups=lineups, team_stats=team_stats, board=board, draft=draft,
        tendencies=tendencies, league=league,
        meta_extra={
            "display_name": cfg["display_name"],
            "primary_owner": cfg["primary_own"],
            "league_id": int(cfg["ID"]),
            "weekly_sources_present": weekly_sources_present(season),
        },
    )
    timings["write"] = time.time() - start
    try:
        shown = directory.relative_to(REPO_ROOT)
    except ValueError:
        # A redirected store (tests) lives outside the repo.
        shown = directory
    _log(f"  wrote       {shown}   {timings['write']:.2f}s")
    return timings


def refresh(
    leagues: Optional[Sequence[str]] = None,
    season: Optional[int] = None,
    what: Sequence[str] = DEFAULT_WHAT,
) -> Tuple[Dict[str, str], Dict[str, Dict[str, float]]]:
    """Build stores for several leagues, isolating failures.

    One league failing must not abort the rest -- expired cookies on a leaguemate's
    league should not cost you your own store. A failing league's existing store is
    left untouched, so the app keeps showing the older build time rather than
    nothing, which is the honest outcome.

    Args:
        leagues: Display names or config keys. Defaults to every configured league.
        season: Season year. Defaults to the configured season.
        what: Artifacts to build.

    Returns:
        tuple: ``({league: "ok" | error string}, {league: timings})``.
    """
    season = get_season() if season is None else int(season)
    targets = list(build_lg_vars()) if leagues is None else list(leagues)

    results: Dict[str, str] = {}
    timings: Dict[str, Dict[str, float]] = {}

    for name in targets:
        try:
            timings[name] = refresh_league(name, season, what)
            results[name] = "ok"
        except Exception as e:                      # noqa: BLE001 - reported, not hidden
            results[name] = f"{type(e).__name__}: {e}"
            _log(f"  FAILED  {type(e).__name__}: {e}")
            _log("  previous store left in place")

    _summarise(results, timings, season)
    return results, timings


def _summarise(
    results: Dict[str, str],
    timings: Dict[str, Dict[str, float]],
    season: int,
) -> None:
    """Print the per-league outcome and elapsed time.

    Args:
        results: League to status.
        timings: League to per-step seconds.
        season: Season year, for the header.
    """
    _log(f"\n===== store: {season} =====")
    total = 0.0
    for name, status in results.items():
        elapsed = sum(timings.get(name, {}).values())
        total += elapsed
        mark = "ok    " if status == "ok" else "FAILED"
        detail = "" if status == "ok" else f"  {status}"
        _log(f"  {name:<28} {mark} {elapsed:>6.2f}s{detail}")
    _log(f"  {'TOTAL':<28}        {total:>6.2f}s")

    failed = [n for n, s in results.items() if s != "ok"]
    if failed:
        _log(f"\n{len(failed)} league(s) failed: {failed}")


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point. See ``python -m Scripts.refresh --help``."""
    p = argparse.ArgumentParser(
        prog="python -m Scripts.refresh",
        description="Build the local data store the app reads from.",
    )
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--league", action="append", dest="leagues", metavar="NAME",
                        help="repeatable; display name or config key")
    target.add_argument("--all", action="store_true",
                        help="every league in config.yaml")
    p.add_argument("--season", type=int, help="defaults to config.yaml season")
    p.add_argument("--what", default=",".join(DEFAULT_WHAT),
                   help=f"comma-separated, from {list(WHAT_CHOICES)} "
                        f"(default: {','.join(DEFAULT_WHAT)}). team_stats "
                        f"re-derives a league's whole history and is slow; board "
                        f"is the pre-season draft board.")
    args = p.parse_args(argv)

    what = [w.strip() for w in args.what.split(",") if w.strip()]
    unknown = [w for w in what if w not in WHAT_CHOICES]
    if unknown:
        p.error(f"unknown --what value(s) {unknown}; known: {list(WHAT_CHOICES)}")

    results, _ = refresh(
        leagues=None if args.all else args.leagues,
        season=args.season,
        what=what,
    )
    return 0 if all(v == "ok" for v in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
