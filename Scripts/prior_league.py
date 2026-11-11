"""A league replaced mid-season: earlier weeks from a frozen copy, later ones from ESPN.

    python -m Scripts.prior_league --freeze jeffs_league     # before switching the id
    python -m Scripts.prior_league --show jeffs_league       # what is frozen, and where

**Why this exists.** Jeffs_League 2026 (ESPN 1650608988) was deleted after week 9
and recreated as 1976327133 with the same teams, rosters, settings and remaining
schedule, to move the commissioner role. ESPN serves the new league's weeks 1-9 as
nothing at all (``box_scores(1)`` raises ``KeyError: rosterForCurrentScoringPeriod``),
its records start at 0-0, and the old id answers ``LEAGUE_NOT_FOUND_DELETED``. Every
artifact the nightly rebuilds *for the whole season* -- ``lineups``, ``team_stats``,
``results`` -- would therefore lose weeks 1-9, and ``draft`` would read the offline
roster entry as the league's draft.

**The shape of the fix.** ``config.yaml`` names the old league on the new one::

    jeffs_league:
      id: 1976327133
      prior:
        id: 1650608988
        season: 2026
        through_week: 9
        owners: {2: 'Jeff Wilhelm', ...}     # new team id -> owner, see below

Weeks ``<= through_week`` come from ``Data/Store/<season>/<key>/prior/``, written
once by ``--freeze`` from the store the old league built. Weeks after it come from
ESPN as usual. The stitch happens at the store boundary in ``Scripts.refresh``, so
the app and S3 only ever see the joined season.

**``owners``** is the team-identity half. Everything downstream groups by
``team_owner``, and the new league reports an unclaimed team as ``Unknown Owner
(<team name>)`` -- which would split each franchise in two at the cutover. The map
pins each new team id to the owner name the frozen weeks carry, and wins over
whatever ESPN reports, so a manager claiming with a differently spelt account does
not split the franchise either. Edit it if a team genuinely changes hands.

**The frozen files are irreproducible** -- the league they came from is gone -- so
``--freeze`` refuses to overwrite them, and they are mirrored to
``archive/backups/`` in S3 alongside the raw ESPN backup.
"""

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from Scripts import store
from Scripts.config_utils import build_lg_vars, get_season, resolve_league

#: Artifacts whose rows are per week and rebuilt for the whole season, so the frozen
#: weeks are spliced in front of the fresh ones.
WEEKLY_ARTIFACTS = ("lineups", "team_stats", "results")

#: Artifacts frozen whole: the draft happened in the old league and nowhere else.
WHOLE_ARTIFACTS = ("draft", "tendencies")

PRIOR_DIRNAME = "prior"


@dataclass(frozen=True)
class PriorSegment:
    """The earlier league a configured league replaced, within one season.

    Attributes:
        league_id: The replaced league's ESPN id.
        season: The season the replacement happened in.
        through_week: Last matchup period that belongs to the replaced league.
        owners: New league team id -> owner name, applied over ESPN's.
    """

    league_id: int
    season: int
    through_week: int
    owners: Dict[int, str] = field(default_factory=dict)

    @property
    def first_week(self) -> int:
        """The first matchup period the current league is responsible for."""
        return self.through_week + 1


def parse_prior(raw: Optional[Dict[str, Any]]) -> Optional[PriorSegment]:
    """Validate a league's ``prior`` block from ``config.yaml``.

    Args:
        raw: The block, or None when the league has none.

    Returns:
        PriorSegment | None: None when the league was never replaced.

    Raises:
        ValueError: When a required key is missing or ``through_week`` is not
            positive. A half-written block must fail loudly: read leniently, it would
            silently fall back to fetching weeks ESPN cannot serve.
    """
    if not raw:
        return None
    missing = [k for k in ("id", "season", "through_week") if k not in raw]
    if missing:
        raise ValueError(f"`prior` block is missing {missing}: {raw!r}")
    through = int(raw["through_week"])
    if through < 1:
        raise ValueError(f"`prior.through_week` must be >= 1, got {through}")
    owners = {int(k): str(v) for k, v in (raw.get("owners") or {}).items()}
    return PriorSegment(int(raw["id"]), int(raw["season"]), through, owners)


def segment_for(cfg: Dict[str, Any], season: int) -> Optional[PriorSegment]:
    """The prior segment that applies to one league-season, if any.

    Args:
        cfg: A ``build_lg_vars`` entry.
        season: Season being built.

    Returns:
        PriorSegment | None: Only when the replacement happened in ``season``; every
        other season of the league is untouched.
    """
    segment = cfg.get("prior")
    if segment is not None and segment.season == int(season):
        return segment
    return None


def first_week(cfg: Dict[str, Any], season: int) -> int:
    """First matchup period to fetch from ESPN for this league-season.

    Args:
        cfg: A ``build_lg_vars`` entry.
        season: Season being built.

    Returns:
        int: 1 for an ordinary league.
    """
    segment = segment_for(cfg, season)
    return 1 if segment is None else segment.first_week


def owners_for_league_id(league_id: int, season: int) -> Dict[int, str]:
    """The pinned owner map for an ESPN league id, for :func:`fetch_league`.

    Looked up by id because that is all the fetch layer has.

    Args:
        league_id: ESPN league id being fetched.
        season: Season being fetched.

    Returns:
        dict: Team id -> owner name; empty for any league without a prior segment.
    """
    try:
        lg_vars = build_lg_vars()
    except FileNotFoundError:
        return {}
    for cfg in lg_vars.values():
        if int(cfg["ID"]) == int(league_id):
            segment = segment_for(cfg, season)
            return dict(segment.owners) if segment else {}
    return {}


def prior_dir(season: int, league_key: str) -> Path:
    """Where a league-season's frozen weeks live.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.

    Returns:
        Path: ``Data/Store/<season>/<key>/prior``.
    """
    return store.league_store_dir(season, league_key) / PRIOR_DIRNAME


def read_prior(season: int, league_key: str, artifact: str) -> Optional[pd.DataFrame]:
    """Read one frozen artifact.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        artifact: A name from :data:`WEEKLY_ARTIFACTS` or :data:`WHOLE_ARTIFACTS`.

    Returns:
        pd.DataFrame | None: None when nothing was frozen for it -- ``results`` was
        never built for some leagues, and that is a gap to report, not a crash.
    """
    path = prior_dir(season, league_key) / f"{artifact}.parquet"
    return pd.read_parquet(path) if path.exists() else None


def stitch_weeks(
    fresh: Optional[pd.DataFrame],
    prior: Optional[pd.DataFrame],
    through_week: int,
) -> Optional[pd.DataFrame]:
    """Frozen weeks ``<= through_week`` followed by fresh weeks after it.

    The fresh frame's own early weeks are dropped even when present: the new league
    holds placeholder matchups for them, and a placeholder 0-0 tie is not a result.

    Args:
        fresh: What the current league produced. None passes through, so a step that
            built nothing still writes nothing.
        prior: The frozen frame, or None.
        through_week: Last week the frozen frame owns.

    Returns:
        pd.DataFrame | None: The joined season, sorted by week, index reset.
    """
    if fresh is None:
        return None
    week = pd.to_numeric(fresh["week"], errors="coerce")
    later = fresh[week > through_week]
    if prior is None or prior.empty:
        return later.reset_index(drop=True)
    early = prior[pd.to_numeric(prior["week"], errors="coerce") <= through_week]
    joined = pd.concat([early, later], ignore_index=True, sort=False)
    return joined.sort_values("week", kind="stable").reset_index(drop=True)


def freeze(season: int, league_key: str, through_week: int,
           *, force: bool = False) -> List[Path]:
    """Copy a store built by the old league into ``prior/``, cut at ``through_week``.

    Run it **before** pointing the league at its new id: after that, the store's
    early weeks are the stitched copy of these files, and the old league that built
    them no longer answers.

    Args:
        season: Season year.
        league_key: ``config.yaml`` league key.
        through_week: Last week to keep from the weekly artifacts.
        force: Overwrite existing frozen files. Off by default because they cannot
            be rebuilt.

    Returns:
        list: Paths written.

    Raises:
        FileExistsError: When ``prior/`` already holds files and ``force`` is off.
        FileNotFoundError: When the store has none of the artifacts to freeze.
    """
    target = prior_dir(season, league_key)
    existing = sorted(target.glob("*.parquet")) if target.exists() else []
    if existing and not force:
        raise FileExistsError(
            f"{target} already holds {[p.name for p in existing]}; these cannot be "
            "rebuilt, so --force is required to replace them.")
    target.mkdir(parents=True, exist_ok=True)

    written: List[Path] = []
    for artifact in WEEKLY_ARTIFACTS + WHOLE_ARTIFACTS:
        source = store.artifact_path(season, league_key, artifact)
        if not source.exists():
            print(f"  {artifact:<11} not in the store; nothing frozen")
            continue
        frame = pd.read_parquet(source)
        if artifact in WEEKLY_ARTIFACTS:
            frame = frame[pd.to_numeric(frame["week"], errors="coerce") <= through_week]
        out = target / f"{artifact}.parquet"
        frame.to_parquet(out)
        written.append(out)
        print(f"  {artifact:<11} {frame.shape[0]:>6} rows -> {out}")
    if not written:
        raise FileNotFoundError(f"no store artifacts to freeze for {league_key} {season}")
    return written


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point. See the module docstring."""
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("league", help="display name or config key")
    p.add_argument("--season", type=int, default=None)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--freeze", action="store_true",
                      help="copy the store's weeks <= through_week into prior/")
    mode.add_argument("--show", action="store_true", help="list the frozen files")
    p.add_argument("--through-week", type=int, default=None,
                   help="defaults to the config's prior.through_week")
    p.add_argument("--force", action="store_true")
    args = p.parse_args(argv)

    cfg = resolve_league(args.league)
    season = args.season or get_season()
    segment = segment_for(cfg, season)
    through = args.through_week or (segment.through_week if segment else None)

    if args.show:
        target = prior_dir(season, cfg["key"])
        print(f"{cfg['display_name']} {season}: {segment or 'no prior segment configured'}")
        for path in sorted(target.glob("*.parquet")) if target.exists() else []:
            frame = pd.read_parquet(path)
            weeks = (sorted(pd.to_numeric(frame["week"]).dropna().astype(int).unique())
                     if "week" in frame.columns else "-")
            print(f"  {path.name:<20} {frame.shape[0]:>6} rows  weeks {weeks}")
        return 0

    if through is None:
        print("--through-week is required when the config has no prior block yet")
        return 2
    freeze(season, cfg["key"], through, force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
