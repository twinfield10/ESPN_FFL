"""The scoring registry: one audited source of truth for every league's rules.

Scoring used to be re-derived from a live ESPN ``League`` object on every call --
four times per league per pipeline run, from an object whose contents
``espn_api`` mutates behind your back (see
:func:`Scripts.fetch_utils.isolate_scoring_format`). Nothing recorded what a
league's rules *were*, so answering "what changed this season" meant a live scan
of every league-season, and a commissioner changing scoring mid-season was
undetectable.

This module persists the derived tables to ``Data/Scoring/scoring.csv`` and
resolves them by ``(league_key, season)``. Writes are explicit
(:func:`refresh_scoring`); reads are cheap and never touch ESPN
(:func:`get_scoring_table`).

The registry is small enough to commit -- roughly 4,500 rows -- which makes
``git log -p Data/Scoring/scoring.csv`` a readable history of every scoring
change. It is keyed on ``league_key`` rather than the numeric ESPN league id so
that committing it publishes nothing ``.gitignore`` currently withholds.

Typical use::

    # once, or whenever a league's settings might have changed
    python -m Scripts.scoring --all

    # everywhere else
    table = get_scoring_table(league)
"""

import argparse
import datetime
import functools
import warnings
from typing import Iterable, List, Optional

import pandas as pd

from Scripts.config_utils import build_lg_vars, get_season
from Scripts.paths import DATA_DIR
from Scripts.scrape_player_stats import ScoringCoverageWarning, build_scoring_table

SCORING_DIR = DATA_DIR / "Scoring"
SCORING_CSV = SCORING_DIR / "scoring.csv"

#: Registry column order. ``league_id`` is deliberately absent -- see module docs.
#:
#: ``source_id`` is the rule id the commissioner configured; ``id`` is the stat it
#: is scored against after ``REPL_SCORING`` rewrites "every N yards" rules. They
#: differ only for those rules, and auditing must use ``source_id`` -- otherwise
#: replacing rule 214 with rule 221 looks like a reprice of 214.
REGISTRY_COLUMNS = [
    "season",
    "league_key",
    "league_name",
    "source_id",
    "id",
    "abbr",
    "label",
    "points",
    "colName",
    "recorded_at",
]

#: The columns that constitute a rule's content. Two rows agreeing on all of
#: these are the same rule, unchanged, and keep their original ``recorded_at``.
_CONTENT_COLUMNS = [
    "season", "league_key", "source_id", "id", "abbr", "label", "points", "colName",
]

#: Columns that define a scoring rule's identity within a league-season.
_KEY_COLUMNS = ["season", "league_key", "source_id"]


class ScoringRegistryWarning(UserWarning):
    """The registry could not answer a lookup, or disagrees with live ESPN."""


class ScoringDriftError(ValueError):
    """Strict-mode counterpart to a drift warning."""


def _warn(msg: str) -> None:
    """Emit a registry warning that the global filter cannot swallow.

    ``Scripts/fetch_utils.py`` calls ``warnings.filterwarnings("ignore")`` at
    module scope, which would otherwise silence these. Filtering here rather than
    narrowing that global call keeps this independent of import order -- see
    ``docs/plans/06-performance.md``.

    Args:
        msg: The warning text.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("always", ScoringRegistryWarning)
        warnings.warn(msg, ScoringRegistryWarning, stacklevel=3)


# --- reading -------------------------------------------------------------

@functools.lru_cache(maxsize=8)
def _load_registry_cached(path: str, mtime: float) -> pd.DataFrame:
    """Read and normalise the registry, memoised on path and mtime.

    Keying on ``mtime`` means a refresh invalidates the cache automatically, with
    no explicit cache-clear call anywhere. ``path`` is part of the key so that a
    redirected registry (tests, an alternate store) cannot collide with the real
    one when the two happen to share an mtime.

    Args:
        path: Registry location, as a string so it is hashable.
        mtime: The file's modification time.

    Returns:
        pd.DataFrame: The full registry.
    """
    df = pd.read_csv(path)
    df["season"] = df["season"].astype(int)
    df["id"] = df["id"].astype(int)
    df["source_id"] = df["source_id"].astype(int)
    df["points"] = df["points"].astype(float)
    # An all-empty colName column reads back as float NaN, which would break the
    # isna()/string handling downstream.
    df["colName"] = df["colName"].astype("object")
    return df


def reset_caches() -> None:
    """Drop the memoised registry and league-key lookups.

    Needed when the registry path or ``config.yaml`` changes inside a live
    process -- chiefly tests, which redirect :data:`SCORING_CSV` to a temp file.
    """
    # getattr: tests monkeypatch _league_key_for_id with a plain function, which
    # has no cache to clear.
    for fn in (_load_registry_cached, _league_key_for_id):
        clear = getattr(fn, "cache_clear", None)
        if clear is not None:
            clear()


def load_scoring_registry() -> pd.DataFrame:
    """The whole registry.

    Returns:
        pd.DataFrame: All league-seasons, or an empty frame with the right
        columns when the registry has never been written.
    """
    if not SCORING_CSV.exists():
        return pd.DataFrame(columns=REGISTRY_COLUMNS)
    return _load_registry_cached(str(SCORING_CSV), SCORING_CSV.stat().st_mtime).copy()


@functools.lru_cache(maxsize=None)
def _league_key_for_id(league_id: int) -> Optional[str]:
    """Map an ESPN league id to its config key.

    Args:
        league_id: Numeric ESPN league id.

    Returns:
        str | None: The ``config.yaml`` league key, or None if not configured.
    """
    for cfg in build_lg_vars().values():
        if int(cfg["ID"]) == int(league_id):
            return cfg["key"]
    return None


def get_scoring_table(
    league=None,
    *,
    league_key: Optional[str] = None,
    season: Optional[int] = None,
    verify: bool = True,
    strict: bool = False,
) -> pd.DataFrame:
    """Resolve one league-season's scoring table.

    Resolution order:

    1. A registry hit for ``(league_key, season)`` is used.
    2. On a miss, the table is derived from ``league`` and a warning notes that
       the registry is cold. Passing no ``league`` on a miss is an error.
    3. When both a registry hit and a live ``league`` are available and
       ``verify`` is set, the two are compared and any disagreement warns. This
       is what catches a commissioner changing scoring mid-season; it costs
       nothing, because the league has already been fetched.

    Args:
        league: A live ESPN ``League``. Supplies ``league_key``/``season`` when
            they are not given, and enables verification.
        league_key: Config key, e.g. ``"gop_degenerates"``. Inferred from
            ``league`` when omitted.
        season: Season year. Taken from ``league.year`` when omitted.
        verify: Compare a registry hit against ``league``'s live settings.
        strict: Raise instead of warning on drift.

    Returns:
        pd.DataFrame: The scoring table, in the same shape
        :func:`Scripts.scrape_player_stats.build_scoring_table` returns. Always a
        fresh copy -- callers such as ``proj_to_score`` mutate it in place.

    Raises:
        ValueError: Neither ``league`` nor both of ``league_key``/``season``.
        ScoringDriftError: When ``strict`` and the registry disagrees with live
            ESPN settings.
    """
    if league is None and (league_key is None or season is None):
        raise ValueError(
            "get_scoring_table needs either a league object, or both "
            "league_key and season."
        )

    if season is None:
        season = int(league.year)
    if league_key is None:
        league_key = _league_key_for_id(league.league_id)
        if league_key is None:
            _warn(
                f"league_id {league.league_id} is not in config.yaml, so it has "
                f"no registry entry. Deriving scoring from live ESPN settings."
            )
            return build_scoring_table(league)

    registry = load_scoring_registry()
    hit = registry[
        (registry["league_key"] == league_key) & (registry["season"] == season)
    ]

    if hit.empty:
        if league is None:
            raise ValueError(
                f"No registry entry for {league_key} {season}, and no league "
                f"object to derive one from. Run `python -m Scripts.scoring "
                f"--league {league_key} --season {season}` first."
            )
        _warn(
            f"No registry entry for {league_key} {season}; deriving from live "
            f"ESPN settings. Run `python -m Scripts.scoring --all` to record it."
        )
        return build_scoring_table(league)

    table = _to_scoring_table(hit)

    if verify and league is not None:
        _verify_against_live(league, league_key, season, table, strict=strict)

    return table


def _to_scoring_table(rows: pd.DataFrame) -> pd.DataFrame:
    """Project registry rows into the ``build_scoring_table`` output shape.

    Args:
        rows: Registry rows for a single league-season.

    Returns:
        pd.DataFrame: Columns ``id``, ``abbr``, ``label``, ``points``,
        ``source_id``, ``colName``, sorted by ``id``, with a fresh index.
    """
    table = rows[["id", "abbr", "label", "points", "source_id", "colName"]].copy()
    table["colName"] = table["colName"].where(table["colName"].notna(), None)
    return table.sort_values("id").reset_index(drop=True)


def _verify_against_live(
    league, league_key: str, season: int, stored: pd.DataFrame, strict: bool = False
) -> pd.DataFrame:
    """Warn when stored scoring no longer matches the league's live settings.

    Args:
        league: Live ESPN ``League``.
        league_key: Config key, for the message.
        season: Season year, for the message.
        stored: The table resolved from the registry.
        strict: Raise instead of warning.

    Returns:
        pd.DataFrame: The differing rules, empty when they agree.

    Raises:
        ScoringDriftError: When ``strict`` and they disagree.
    """
    live = build_scoring_table(league)
    # Compare on source_id: two rules can share an `id` after REPL_SCORING
    # rewrites them, which would make the merge ambiguous.
    merged = stored[["source_id", "points"]].merge(
        live[["source_id", "points"]], on="source_id", how="outer",
        suffixes=("_stored", "_live"), indicator=True,
    )
    drift = merged[
        (merged["_merge"] != "both")
        | (merged["points_stored"] != merged["points_live"])
    ]
    if drift.empty:
        return drift

    detail = ", ".join(
        f"rule={int(r.source_id)} stored={r.points_stored} live={r.points_live}"
        for r in drift.itertuples()
    )
    msg = (
        f"{league_key} {season}: stored scoring disagrees with live ESPN "
        f"settings on {len(drift)} rule(s): {detail}. Someone changed the "
        f"league's scoring since the registry was written. Re-run "
        f"`python -m Scripts.scoring --league {league_key} --season {season}` "
        f"and re-derive anything computed from the old rules."
    )
    if strict:
        raise ScoringDriftError(msg)
    _warn(msg)
    return drift


# --- writing -------------------------------------------------------------

def refresh_scoring(
    leagues: Optional[Iterable[str]] = None,
    seasons: Optional[Iterable[int]] = None,
    all_seasons: bool = False,
    strict: bool = False,
) -> pd.DataFrame:
    """Fetch scoring from ESPN, validate coverage, and write the registry.

    Existing rows for a written ``(league_key, season)`` are replaced; every
    other row is left alone, so refreshing one league never drops another's
    history.

    Args:
        leagues: Display names or config keys. Defaults to every configured
            league.
        seasons: Seasons to fetch. Defaults to the configured current season, or
            each league's full ``start``..``end`` range when ``all_seasons``.
        all_seasons: Fetch each league's whole configured history. Ignored when
            ``seasons`` is given.
        strict: Fail on a league that scores a rule this pipeline cannot model,
            rather than recording it with a null ``colName``.

    Returns:
        pd.DataFrame: The rows written, not the whole registry.

    Raises:
        Scripts.scrape_player_stats.UnmappedScoringRuleError: When ``strict``
            and a league has an unmapped scoring rule.
    """
    from Scripts.fetch_utils import fetch_league   # deferred: avoids a cycle

    lg_vars = build_lg_vars()
    by_key = {cfg["key"]: (name, cfg) for name, cfg in lg_vars.items()}

    if leagues is None:
        selected = list(lg_vars.items())
    else:
        selected = []
        for want in leagues:
            if want in lg_vars:
                selected.append((want, lg_vars[want]))
            elif want in by_key:
                selected.append(by_key[want])
            else:
                raise ValueError(
                    f"Unknown league {want!r}. Configured: "
                    f"{sorted(lg_vars)} or {sorted(by_key)}."
                )

    recorded_at = datetime.datetime.now().isoformat(timespec="seconds")
    written: List[pd.DataFrame] = []
    unmapped_report = {}

    for name, cfg in selected:
        if seasons is not None:
            years = list(seasons)
        elif all_seasons:
            years = list(range(int(cfg["start"]), int(cfg["end"]) + 1))
        else:
            years = [get_season()]

        for year in years:
            if year < int(cfg["start"]) or year > int(cfg["end"]):
                print(f"  skip {name} {year}: outside configured range")
                continue
            try:
                league = fetch_league(
                    league_id=cfg["ID"], year=year,
                    swid=cfg["SWID"], espn_s2=cfg["ESPN_S2"],
                )
                # Capture coverage warnings here so the summary below is the one
                # place a scoring gap is reported, rather than once per call site.
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    table = build_scoring_table(league, strict=strict)
            except Exception as e:
                print(f"  FAIL {name} {year}: {type(e).__name__}: {e}")
                continue

            gaps = [
                str(w.message) for w in caught
                if issubclass(w.category, ScoringCoverageWarning)
            ]
            if gaps:
                unmapped_report[f"{cfg['key']} {year}"] = gaps

            rows = table.copy()
            rows["season"] = year
            rows["league_key"] = cfg["key"]
            rows["league_name"] = name
            rows["recorded_at"] = recorded_at
            written.append(rows[REGISTRY_COLUMNS])
            print(f"  {cfg['key']:<26} {year}  {len(rows):>3} rules")

    if not written:
        raise RuntimeError("Nothing fetched; registry left unchanged.")

    new_rows = pd.concat(written, ignore_index=True)
    _write_registry(new_rows)

    print(f"\nWrote {len(new_rows)} rows to {SCORING_CSV.relative_to(DATA_DIR.parent)}")
    if unmapped_report:
        print("\nScoring rules this pipeline cannot model (recorded with a null")
        print("colName, so they contribute nothing to projections):")
        for where, msgs in unmapped_report.items():
            for m in msgs:
                print(f"  {where}: {m}")
    else:
        print("Coverage complete: every scored rule maps to a stat column.")

    return new_rows


def _write_registry(new_rows: pd.DataFrame) -> pd.DataFrame:
    """Merge rows into the registry on disk and write it back.

    ``recorded_at`` is carried over for any rule whose content is unchanged, so
    ``recorded_at`` means "first seen with these values" and a refresh that finds
    nothing new produces an empty git diff. Without that, every refresh would
    restamp all ~1,900 rows and bury real changes in noise.

    Args:
        new_rows: Rows to upsert, carrying every column in
            :data:`REGISTRY_COLUMNS`.

    Returns:
        pd.DataFrame: The full registry as written.
    """
    SCORING_DIR.mkdir(parents=True, exist_ok=True)

    existing = load_scoring_registry()
    if not existing.empty:
        # pandas merge treats NaN keys as equal, so unmapped rules (null colName)
        # match correctly here.
        prior = (
            existing[_CONTENT_COLUMNS + ["recorded_at"]]
            .rename(columns={"recorded_at": "_prior"})
        )
        new_rows = new_rows.merge(prior, on=_CONTENT_COLUMNS, how="left")
        new_rows["recorded_at"] = new_rows["_prior"].fillna(new_rows["recorded_at"])
        new_rows = new_rows.drop(columns=["_prior"])

        refreshed = set(
            zip(new_rows["league_key"], new_rows["season"].astype(int))
        )
        keep = ~existing.apply(
            lambda r: (r["league_key"], int(r["season"])) in refreshed, axis=1
        )
        combined = pd.concat([existing[keep], new_rows], ignore_index=True)
    else:
        combined = new_rows

    combined = combined.sort_values(_KEY_COLUMNS).reset_index(drop=True)
    combined[REGISTRY_COLUMNS].to_csv(SCORING_CSV, index=False)
    _load_registry_cached.cache_clear()
    return combined


# --- auditing ------------------------------------------------------------

def diff_scoring(league_key: str, season_a: int, season_b: int) -> pd.DataFrame:
    """What changed in a league's scoring between two seasons.

    This is the question that motivated the registry: answering it previously
    meant a live scan of every league-season.

    Args:
        league_key: Config key, e.g. ``"gop_degenerates"``.
        season_a: Baseline season.
        season_b: Comparison season.

    Returns:
        pd.DataFrame: One row per changed rule keyed on ``source_id`` -- the id
        the commissioner configured -- with ``change`` set to ``added``,
        ``removed`` or ``repriced``. Empty when nothing changed.
    """
    reg = load_scoring_registry()
    a = reg[(reg["league_key"] == league_key) & (reg["season"] == season_a)]
    b = reg[(reg["league_key"] == league_key) & (reg["season"] == season_b)]

    cols = ["source_id", "abbr", "label", "points"]
    merged = a[cols].merge(
        b[cols], on="source_id", how="outer",
        suffixes=(f"_{season_a}", f"_{season_b}"), indicator=True,
    )
    pa, pb = f"points_{season_a}", f"points_{season_b}"
    changed = merged[(merged["_merge"] != "both") | (merged[pa] != merged[pb])].copy()
    changed["change"] = changed["_merge"].map(
        {"left_only": "removed", "right_only": "added", "both": "repriced"}
    )
    changed["abbr"] = changed[f"abbr_{season_a}"].fillna(changed[f"abbr_{season_b}"])
    changed["label"] = changed[f"label_{season_a}"].fillna(changed[f"label_{season_b}"])
    return (
        changed[["source_id", "abbr", "label", pa, pb, "change"]]
        .sort_values(["change", "source_id"])
        .reset_index(drop=True)
    )


def coverage_gaps() -> pd.DataFrame:
    """Every recorded rule that is scored but not modelled.

    Returns:
        pd.DataFrame: Registry rows with points but no ``colName``, empty when
        coverage is complete.
    """
    reg = load_scoring_registry()
    if reg.empty:
        return reg
    return reg[reg["colName"].isna() & (reg["points"] != 0)].reset_index(drop=True)


# --- CLI -----------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point. See ``python -m Scripts.scoring --help``."""
    p = argparse.ArgumentParser(
        prog="python -m Scripts.scoring",
        description="Fetch, record and audit league scoring rules.",
    )
    p.add_argument("--league", action="append", dest="leagues", metavar="NAME",
                   help="League display name or config key. Repeatable. "
                        "Defaults to every configured league.")
    p.add_argument("--season", action="append", type=int, dest="seasons",
                   metavar="YEAR", help="Season to fetch. Repeatable. "
                                        "Defaults to the configured season.")
    p.add_argument("--all", action="store_true", dest="all_seasons",
                   help="Fetch each league's full configured season range.")
    p.add_argument("--strict", action="store_true",
                   help="Fail on a scoring rule that cannot be mapped.")
    p.add_argument("--diff", nargs=3, metavar=("LEAGUE_KEY", "SEASON_A", "SEASON_B"),
                   help="Report scoring changes between two seasons and exit.")
    p.add_argument("--gaps", action="store_true",
                   help="List recorded rules that are scored but not modelled.")
    args = p.parse_args(argv)

    if args.diff:
        key, a, b = args.diff
        out = diff_scoring(key, int(a), int(b))
        print(out.to_string(index=False) if not out.empty
              else f"No scoring changes for {key} between {a} and {b}.")
        return 0

    if args.gaps:
        out = coverage_gaps()
        if out.empty:
            print("No coverage gaps recorded.")
            return 0
        print(out[["season", "league_key", "source_id", "abbr", "label", "points"]]
              .to_string(index=False))
        return 1

    refresh_scoring(
        leagues=args.leagues, seasons=args.seasons,
        all_seasons=args.all_seasons, strict=args.strict,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
