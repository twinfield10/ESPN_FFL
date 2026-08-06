"""Cross-provider player identity: the ID join this pipeline has never had.

Everything else here matches players by name. ``(week, player_name)`` string
equality, patched by roughly 140 hand-curated rename entries in
``projection_utils.py`` and ``scrape_pinnacle.py`` that need re-curating every
August as suffixes change (Jr./Sr./II/III) and rookies arrive.

Two failure modes, and the second is worse:

* A **miss** silently drops a player from a source. Visible if you look for it.
* A **collision** silently misattributes one. Building the draft board found 16
  shared names in GOP Degenerates' 2,503-player pool -- two Lamar Jacksons (a
  Ravens quarterback and a cornerback), two Justin Jeffersons (a Vikings receiver
  and a Browns linebacker) -- and the book sources were attaching the receiver's
  projected line to the linebacker, inflating him into the league's top-projected
  individual defender on somebody else's numbers.

``nflreadr::load_ff_playerids()`` carries ``gsis_id``, ``espn_id`` and
``fantasypros_id`` in one table, refreshed daily upstream. That is simultaneously
the join key for play-by-play data (``gsis_id``) and for the ESPN store and draft
board (``espn_id``). ``R/GetPlayerIDs.R`` persists it; this module reads it.

Measured against a live 2026 draft board: **182 of 199 market-priced players
matched (91%), and all 17 misses were D/ST team units**, which correctly have no
individual player id. Every individual player matched.

Typical use::

    from Scripts.crosswalk import attach_gsis_id, coverage

    board = attach_gsis_id(board, espn_id_column="player_id")
    print(coverage(board, "gsis_id"))

See ``docs/plans/16-usage-data-layer.md``.
"""

import functools
import warnings
from typing import Dict, List, NamedTuple, Optional

import pandas as pd

from Scripts.paths import PLAYER_IDS_PARQUET

#: Provider id columns the crosswalk carries. All held as strings: ESPN's own
#: payloads use integers, but an id is an opaque label rather than a number, and
#: round-tripping through int turns a missing one into a silent 0.
ID_COLUMNS = ("gsis_id", "espn_id", "fantasypros_id", "sleeper_id", "yahoo_id",
              "sportradar_id", "pfr_id")


class CrosswalkWarning(UserWarning):
    """The crosswalk is missing, stale, or could not resolve something."""


def _warn(msg: str) -> None:
    """Warn past the process-wide ``filterwarnings("ignore")`` in fetch_utils.

    Args:
        msg: The warning text.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("always", CrosswalkWarning)
        warnings.warn(msg, CrosswalkWarning, stacklevel=3)


@functools.lru_cache(maxsize=4)
def _load_cached(path: str, mtime: float) -> pd.DataFrame:
    """Read and normalise the crosswalk, memoised on path and mtime.

    Keying on ``mtime`` means re-running ``R/GetPlayerIDs.R`` invalidates the cache
    with no explicit clear -- the same trick :mod:`Scripts.scoring` uses for the
    scoring registry.

    Args:
        path: Parquet location, as a string so it is hashable.
        mtime: The file's modification time.

    Returns:
        pd.DataFrame: The crosswalk, id columns as nullable strings.
    """
    df = pd.read_parquet(path)
    for column in ID_COLUMNS:
        if column in df.columns:
            df[column] = df[column].astype("string").str.strip()
            # Upstream writes empty strings as well as nulls.
            df.loc[df[column].isin(["", "NA", "nan", "None"]), column] = pd.NA
    return df


def load_crosswalk() -> pd.DataFrame:
    """The full player identity table.

    Returns:
        pd.DataFrame: One row per player, with ``name``, ``position``, ``team`` and
        the :data:`ID_COLUMNS` present in the file.

    Raises:
        FileNotFoundError: When the crosswalk has not been generated, naming the
            command that generates it.
    """
    if not PLAYER_IDS_PARQUET.exists():
        raise FileNotFoundError(
            f"{PLAYER_IDS_PARQUET} is missing. Generate it with "
            f"`Rscript R/GetPlayerIDs.R`."
        )
    return _load_cached(str(PLAYER_IDS_PARQUET), PLAYER_IDS_PARQUET.stat().st_mtime)


def reset_cache() -> None:
    """Drop the memoised crosswalk. Mainly for tests."""
    _load_cached.cache_clear()


def ambiguous_ids(column: str) -> List[str]:
    """Ids that more than one player claims, and so cannot be joined on.

    Upstream carries a handful of genuine data errors where two distinct players
    share an id: Nate Jones (WR) and Nathan Jones (CB) both hold ``espn_id`` 5730
    *and* ``gsis_id`` 00-0022828. Measured on the 2026-08-06 file: 13 duplicated
    ``espn_id``, 10 duplicated ``gsis_id``, 2 duplicated ``fantasypros_id``.

    They are all long-retired players, but a merge on a duplicated key **fans out
    rows** -- which on a draft board would silently duplicate a player and, because
    every downstream rank is computed by position, quietly shift everyone below
    them. So these are excluded from every lookup rather than resolved.

    Args:
        column: An id column name.

    Returns:
        list: The ambiguous id values, sorted.
    """
    table = load_crosswalk()
    if column not in table.columns:
        return []
    present = table[column].dropna()
    counts = present.value_counts()
    return sorted(counts[counts > 1].index.tolist())


@functools.lru_cache(maxsize=16)
def _mapping(from_column: str, to_column: str, mtime: float) -> Dict[str, str]:
    """Build an unambiguous id-to-id mapping. Memoised on the file's mtime."""
    table = load_crosswalk()
    for column in (from_column, to_column):
        if column not in table.columns:
            raise KeyError(
                f"{column!r} is not in the crosswalk. Available: "
                f"{sorted(table.columns)}."
            )

    pairs = table[[from_column, to_column]].dropna()
    bad = set(ambiguous_ids(from_column))
    if bad:
        pairs = pairs[~pairs[from_column].isin(bad)]
    # Two rows agreeing on both ids are the same player listed twice; harmless.
    pairs = pairs.drop_duplicates()
    return dict(zip(pairs[from_column], pairs[to_column]))


def id_map(from_column: str, to_column: str) -> Dict[str, str]:
    """A dict from one provider's id to another's, excluding ambiguous keys.

    Args:
        from_column: Source id column, e.g. ``"espn_id"``.
        to_column: Target id column, e.g. ``"gsis_id"``.

    Returns:
        dict: ``{from_id: to_id}``, string keys and values.

    Raises:
        KeyError: If either column is absent from the crosswalk.
    """
    return _mapping(from_column, to_column, PLAYER_IDS_PARQUET.stat().st_mtime)


class Coverage(NamedTuple):
    """How well a join landed.

    Attributes:
        total: Rows considered.
        matched: Rows that resolved to an id.
        pct: Matched share, 0-100.
        unmatched_sample: A few unmatched labels, for the message.
    """
    total: int
    matched: int
    pct: float
    unmatched_sample: List[str]

    def __str__(self) -> str:
        head = f"{self.matched}/{self.total} matched ({self.pct:.0f}%)"
        if not self.unmatched_sample:
            return head
        return f"{head}; unmatched e.g. {', '.join(self.unmatched_sample)}"


def coverage(df: pd.DataFrame, id_column: str,
             label_column: str = "player_name", sample: int = 6) -> Coverage:
    """Measure how much of ``df`` resolved to an id.

    Reporting this is the point of using an ID join at all: a name join fails
    silently, and the whole reason to move off it is to be able to see the misses.

    Args:
        df: Frame after an ``attach_*`` call.
        id_column: The attached id column to measure.
        label_column: Column naming the unmatched rows.
        sample: How many unmatched labels to include.

    Returns:
        Coverage: Counts, percentage, and a sample of what missed.
    """
    if id_column not in df.columns:
        return Coverage(len(df), 0, 0.0, [])
    matched = df[id_column].notna()
    labels: List[str] = []
    if label_column in df.columns:
        labels = df.loc[~matched, label_column].astype(str).head(sample).tolist()
    total = len(df)
    pct = 100.0 * int(matched.sum()) / total if total else 0.0
    return Coverage(total, int(matched.sum()), pct, labels)


def _attach(df: pd.DataFrame, from_column: str, to_column: str,
            source_column: str, target_column: Optional[str],
            warn_below: Optional[float]) -> pd.DataFrame:
    """Map one id column onto another via the crosswalk.

    Uses ``Series.map`` rather than a merge, deliberately: a merge on a key the
    crosswalk duplicates would add rows, and a draft board that silently gains a
    duplicated player shifts every positional rank below it.

    Args:
        df: Frame to annotate.
        from_column: Crosswalk column matching ``source_column``.
        to_column: Crosswalk column to attach.
        source_column: Column in ``df`` holding the source id.
        target_column: Name for the new column. Defaults to ``to_column``.
        warn_below: Warn when matched share falls under this percentage.

    Returns:
        pd.DataFrame: A copy of ``df`` with the id attached.

    Raises:
        KeyError: If ``source_column`` is not in ``df``.
    """
    if source_column not in df.columns:
        raise KeyError(
            f"{source_column!r} is not in the frame; got "
            f"{sorted(df.columns)[:12]}..."
        )

    target_column = target_column or to_column
    out = df.copy()
    keys = out[source_column].astype("string").str.strip()
    out[target_column] = keys.map(id_map(from_column, to_column)).astype("string")

    if warn_below is not None:
        result = coverage(out, target_column)
        if result.pct < warn_below:
            _warn(
                f"crosswalk {from_column}->{to_column} matched only {result}. "
                f"Re-run `Rscript R/GetPlayerIDs.R` if this is a new season; "
                f"team D/ST units never match and are expected to miss."
            )
    return out


def attach_gsis_id(df: pd.DataFrame, espn_id_column: str = "player_id",
                   target_column: str = "gsis_id",
                   warn_below: Optional[float] = 70.0) -> pd.DataFrame:
    """Attach the play-by-play id to an ESPN-keyed frame.

    This is the join that lets ``nflfastR`` usage data reach the store and the
    draft board.

    Args:
        df: An ESPN-keyed frame -- the store's lineups, or a draft board.
        espn_id_column: Column holding ESPN player ids.
        target_column: Name for the attached column.
        warn_below: Warn when the matched share falls below this percentage.
            Defaults to 70, which a normal board clears comfortably at 91%.

    Returns:
        pd.DataFrame: A copy with ``gsis_id`` attached, NA where unmatched.
    """
    return _attach(df, "espn_id", "gsis_id", espn_id_column, target_column,
                   warn_below)


def attach_espn_id(df: pd.DataFrame, gsis_id_column: str = "gsis_id",
                   target_column: str = "espn_id",
                   warn_below: Optional[float] = 70.0) -> pd.DataFrame:
    """Attach the ESPN id to a play-by-play-keyed frame.

    The reverse of :func:`attach_gsis_id` -- used to bring ``nflfastR`` output into
    the ESPN world rather than the other way round.

    Args:
        df: A gsis-keyed frame, e.g. ``calculate_stats(..., "week", "player")``.
        gsis_id_column: Column holding gsis ids.
        target_column: Name for the attached column.
        warn_below: Warn when the matched share falls below this percentage.

    Returns:
        pd.DataFrame: A copy with ``espn_id`` attached, NA where unmatched.
    """
    return _attach(df, "gsis_id", "espn_id", gsis_id_column, target_column,
                   warn_below)


def attach_fantasypros_id(df: pd.DataFrame, espn_id_column: str = "player_id",
                          target_column: str = "fantasypros_id",
                          warn_below: Optional[float] = None) -> pd.DataFrame:
    """Attach the FantasyPros id to an ESPN-keyed frame.

    The route to retiring the name-based FantasyPros join. Coverage is genuinely
    lower than gsis/espn (4,784 of 12,470 rows carry one), so no warning threshold
    is set by default -- measure with :func:`coverage` before relying on it.

    Args:
        df: An ESPN-keyed frame.
        espn_id_column: Column holding ESPN player ids.
        target_column: Name for the attached column.
        warn_below: Warn when the matched share falls below this percentage.

    Returns:
        pd.DataFrame: A copy with ``fantasypros_id`` attached.
    """
    return _attach(df, "espn_id", "fantasypros_id", espn_id_column,
                   target_column, warn_below)


def summary() -> str:
    """A one-line description of the crosswalk on disk, for CLI output.

    Returns:
        str: Row count, usable pair count, and ambiguous key counts.
    """
    table = load_crosswalk()
    pairs = table[["gsis_id", "espn_id"]].dropna()
    bad = {c: len(ambiguous_ids(c)) for c in ("gsis_id", "espn_id", "fantasypros_id")
           if c in table.columns}
    return (f"{len(table)} players, {len(pairs)} gsis<->espn pairs, "
            f"ambiguous {bad}")


def main() -> int:
    """Print a coverage report for the crosswalk against every built board."""
    from Scripts import store

    print(summary())
    for season in store.list_seasons():
        for league_key in store.list_leagues(season):
            try:
                board = store.read_league_store(season, league_key, "board")
            except FileNotFoundError:
                continue
            annotated = attach_gsis_id(board, warn_below=None)
            overall = coverage(annotated, "gsis_id")
            individuals = annotated[annotated["primaryPosition"] != "D/ST"]
            print(f"  {season} {league_key:<24} all {overall.pct:>5.1f}%  "
                  f"excluding D/ST {coverage(individuals, 'gsis_id').pct:>5.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
