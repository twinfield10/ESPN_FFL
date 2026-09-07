"""Import The Athletic's season projection workbook into a tidy stat table.

**A hand-dropped file, not a scrape.** The workbook is a paid download from The
Athletic (Jake Ciely's spreadsheet) with no API behind it, so this runs when a new
copy is saved rather than nightly. Everything else about it matches the other
season sources: raw stat lines in, ``Data/Projections/TheAthletic/Season/<season>/``
out, and :func:`Scripts.season_projections.load_theathletic_season` reads it from
there.

Why the *team* tabs rather than the flattened ``QB``/``RB``/``WR``/``TE`` ones: the
team tabs are the model. Each is a team-budget times usage-share calculation --
``PASS ATT = team_pass_attempts * player_pass_share`` -- and the per-position tabs
are ``VLOOKUP``s off them that silently drop columns. The QB tab has no receiving
columns at all, which is what hides the one real bug in the file (see
:data:`POSITION_STATS`).

What this deliberately does not read:

- ``FPS``, ``Custom``, ``VORP``, ``AUC$`` -- points and derived values. We score raw
  stats through each league's own rules like every other source, and two of those
  four are wrong anyway: ``OVR & VORP Ranks`` adds 45% of the *row-aligned* running
  back's VORP to each quarterback's, and the QB replacement rank resolves to 2 in a
  one-QB league.
- The ``DST`` tab. ``Settings`` defines all seven points-allowed tiers but the
  ``0 PT GAMES``..``35+ PT GAMES`` columns are null for all 32 teams, so the
  workbook's own defence values omit the points-allowed component entirely. This
  repo's ``DST`` model is blended at 0.25 and is the better number.

And what it reads *besides* the stat lines: the ``Rankings`` tab, which carries Jake
Ciely's hand ranking of 290 players. An ordering is not a stat line and has nowhere to
go in a blend that works in stat space, so it is written to a **separate** file and
reaches the board as lowercase ``ath_`` columns that no blend or scoring pass can see.
See :data:`RANK_TAB` for why it is that tab and not the one named after him.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd

from Scripts.paths import landing_dir, season_dir

#: Provider directory name under ``Data/Projections``.
SOURCE = "TheAthletic"

#: Blend prefix. Uppercase, because ``compute_weighted_stats`` and ``proj_to_score``
#: scan every uppercase prefix and require it to be numeric.
PREFIX = "ATH"

#: Output filename, matching the FantasyPros season file's shape.
FILENAME = f"{SOURCE}_Projections_Season.parquet"

#: The 32 team tabs, in workbook order. Already on ESPN's abbreviations -- ``WSH``,
#: ``JAX``, ``LV``, ``LAR``, ``LAC`` -- so no alias map is needed here, unlike the
#: nflverse-keyed sources that need :data:`Scripts.draft.board.ESPN_TEAM_ALIASES`.
TEAM_TABS: Sequence[str] = (
    "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE",
    "DAL", "DEN", "DET", "GB", "HOU", "IND", "JAX", "KC",
    "LV", "LAC", "LAR", "MIA", "MIN", "NE", "NO", "NYG",
    "NYJ", "PHI", "PIT", "SF", "SEA", "TB", "TEN", "WSH",
)

#: Workbook column header -> ESPN stat name.
#:
#: The same twelve stats FantasyPros supplies, under ESPN's names, so both sources
#: land on identical ``<PREFIX>_<stat>`` columns and the blend compares like with
#: like. See ``Scripts/scrape_FP.py`` ``final_cols`` for the other end of it.
#:
#: **No ``lostFumbles``.** The workbook does not project fumbles, so ``ATH_`` simply
#: has no column for it and ``compute_weighted_stats`` renormalises the sources that
#: do -- the same handling a book with no line on a stat already gets.
STAT_COLUMNS: Dict[str, str] = {
    "PASS ATT": "passingAttempts",
    "COMP": "passingCompletions",
    "PASS YARDS": "passingYards",
    "PASS TD": "passingTouchdowns",
    "INT": "passingInterceptions",
    "RUSH ATT": "rushingAttempts",
    "RUSH YARDS": "rushingYards",
    "RUSH TD": "rushingTouchdowns",
    "TARGETS": "receivingTargets",
    "REC": "receivingReceptions",
    "RECV YARDS": "receivingYards",
    "RECV TD": "receivingTouchdowns",
}

#: Which stats each position is allowed to carry, and why this is not paranoia.
#:
#: The workbook allocates team target share across a tab's rows, and on the New
#: Orleans tab some of it lands on a **quarterback**: Spencer Rattler carries 32.2
#: targets, 23.7 receptions, 258.7 receiving yards and 2.38 receiving touchdowns.
#: The workbook's own ``QB`` tab has no receiving columns so it never sees them and
#: scores him 7.8; read the team tab straight and he scores **59.8**, which would
#: make a third-string quarterback a real opinion in the blend.
#:
#: Verified across all 32 tabs as of the 2026-08-31 workbook: exactly one player is
#: affected, and no running back, receiver or tight end carries passing stats. The
#: mask is kept anyway -- it is the share model that produced this, so the next
#: download can produce it somewhere else.
POSITION_STATS: Dict[str, frozenset] = {
    "QB": frozenset({
        "passingAttempts", "passingCompletions", "passingYards",
        "passingTouchdowns", "passingInterceptions",
        "rushingAttempts", "rushingYards", "rushingTouchdowns",
    }),
    "RB": frozenset({
        "rushingAttempts", "rushingYards", "rushingTouchdowns",
        "receivingTargets", "receivingReceptions", "receivingYards",
        "receivingTouchdowns",
    }),
    "WR": frozenset({
        "rushingAttempts", "rushingYards", "rushingTouchdowns",
        "receivingTargets", "receivingReceptions", "receivingYards",
        "receivingTouchdowns",
    }),
    "TE": frozenset({
        "receivingTargets", "receivingReceptions", "receivingYards",
        "receivingTouchdowns",
    }),
}

#: Positions read out of the team tabs. Kickers and defences are not on them.
POSITIONS: Sequence[str] = tuple(POSITION_STATS)


#: The tab the hand ranking actually lives on.
#:
#: Not ``Jake's Ranks``, which looks like the source and is not. Every cell on that tab
#: is a lookup: ``=VLOOKUP(<rank>,Rankings!A:T,3,FALSE)`` for the name, and
#: ``=VLOOKUP(<player>,QB!B:O,4,FALSE)`` for the stat columns beside it. Those stats are
#: the same projections the team tabs already give us -- checked against the built
#: parquet, all 85 running backs match to the float -- so the ordering is the only thing
#: on it we do not already have, and the ordering comes from here.
RANK_TAB = "Rankings"

#: Rank flavors, in the order their column blocks appear on :data:`RANK_TAB`.
#:
#: The workbook says which block is which rather than leaving it to be inferred:
#: ``Jake PPR`` reads ``Rankings`` column 23 for its backs and ``Jake Non`` reads column
#: 38 -- the second and third running-back blocks. The first is what the default
#: ``Jake's Ranks`` tab renders, and ``Settings`` prices a reception at 0.5, so it is the
#: half-PPR list. Cross-checked against the other two rather than taken on faith: the
#: first block's rank sits between them for 77 of 85 backs, which is what a half-point
#: list must do.
#:
#: Quarterbacks get one block, not three. Receptions do not move them, and the workbook
#: agrees -- all three ``Jake`` tabs read the same column 3 for their quarterbacks.
RANK_FLAVORS: Sequence[str] = ("half", "ppr", "std")

#: Flavor -> output column. Lowercase for the reason :data:`DIAGNOSTIC_COLUMNS` gives:
#: ``UPPER_`` is the blendable namespace and a rank must never enter it.
RANK_COLUMNS: Dict[str, str] = {f: f"ath_rank_{f}" for f in RANK_FLAVORS}

#: Output filename for the ranking.
#:
#: A second file rather than more columns on the first. The stat table is 434 players and
#: the ranking is 290 of them, so merging them here would write nulls meaning "he is not
#: ranked" into a file whose nulls already mean "this source does not project that stat".
#: Two different facts should not share a hole.
RANKS_FILENAME = f"{SOURCE}_Ranks_Season.parquet"


def _header_map(row: Sequence) -> Dict[str, int]:
    """Column index for each header on a team tab.

    Read per tab rather than hard-coded, so a workbook that gains a column shifts
    nothing. Verified identical across all 32 tabs, but reading it is free.

    Args:
        row: The tab's first row, as values.

    Returns:
        Dict[str, int]: Header text -> zero-based column index.
    """
    return {str(cell).strip(): i
            for i, cell in enumerate(row) if cell is not None}


def read_workbook(path: Path) -> pd.DataFrame:
    """Parse the 32 team tabs into one tidy row per player.

    Args:
        path: The ``.xlsx`` workbook.

    Returns:
        pd.DataFrame: ``player_name``, ``pro_team``, ``position``, ``bye`` and one
        column per ESPN stat name, masked to what the position can hold.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        KeyError: If a team tab is missing or its header lacks ``PLAYER``/``POS``.
    """
    import openpyxl

    if not path.exists():
        raise FileNotFoundError(path)

    book = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        rows: List[dict] = []
        for tab in TEAM_TABS:
            if tab not in book.sheetnames:
                raise KeyError(f"team tab {tab!r} missing from {path.name}")
            sheet = book[tab]
            header: Optional[Dict[str, int]] = None
            for raw in sheet.iter_rows(values_only=True):
                if header is None:
                    header = _header_map(raw)
                    if "PLAYER" not in header or "POS" not in header:
                        raise KeyError(f"{tab}: header has no PLAYER/POS column")
                    continue
                name = raw[header["PLAYER"]] if header["PLAYER"] < len(raw) else None
                pos = raw[header["POS"]] if header["POS"] < len(raw) else None
                if not name or not pos:
                    # Blank spacer rows separate the position blocks, and the
                    # team-totals block sits below them with no POS at all.
                    continue
                pos = str(pos).strip().upper()
                if pos not in POSITION_STATS:
                    continue
                allowed = POSITION_STATS[pos]
                record = {
                    "player_name": str(name).strip(),
                    "pro_team": tab,
                    "position": pos,
                    "bye": raw[header["BYE"]] if "BYE" in header else None,
                }
                masked = []
                for head, stat in STAT_COLUMNS.items():
                    idx = header.get(head)
                    value = raw[idx] if idx is not None and idx < len(raw) else None
                    if stat in allowed:
                        record[stat] = value
                        continue
                    record[stat] = None
                    if value is not None and float(value or 0.0) != 0.0:
                        masked.append(stat)
                # Recorded rather than merely dropped, so `audit` can name what the
                # mask caught. Measuring the output frame cannot: by then the
                # offending values are already None and every count reads zero.
                record["masked_stats"] = ",".join(masked)
                rows.append(record)
    finally:
        book.close()

    frame = pd.DataFrame(rows)
    for stat in STAT_COLUMNS.values():
        frame[stat] = pd.to_numeric(frame[stat], errors="coerce")
    return frame


def read_rankings(path: Path) -> pd.DataFrame:
    """Parse the ``Rankings`` tab into one tidy row per ranked player.

    The tab is ten position blocks laid side by side sharing one ``RK`` column down
    the left: four for the half-PPR list, then three each for the full-PPR and
    non-PPR lists. Quarterbacks appear once, in the first group only, so their rank
    is carried to all three flavors.

    Block positions are read from the header row rather than hard-coded to
    spreadsheet letters, the way :func:`_header_map` does for the team tabs, and the
    grouping is derived from where a position *repeats* rather than from a count --
    a workbook that gains a flavor or a column then shifts instead of silently
    reading a receiver's rank into the tight ends.

    Args:
        path: The ``.xlsx`` workbook.

    Returns:
        pd.DataFrame: ``player_name``, ``position``, ``pro_team`` and one column per
        entry in :data:`RANK_COLUMNS`. One row per ranked player, ordered by the
        half-PPR list within position.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        KeyError: If :data:`RANK_TAB` is missing, its rank column is not ``RK``, or
            the header does not yield one block per flavor and position.
    """
    import openpyxl

    if not path.exists():
        raise FileNotFoundError(path)

    book = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        if RANK_TAB not in book.sheetnames:
            raise KeyError(f"tab {RANK_TAB!r} missing from {path.name}")
        rows = list(book[RANK_TAB].iter_rows(values_only=True))
    finally:
        book.close()

    if not rows or str(rows[0][0]).strip().upper() != "RK":
        raise KeyError(f"{RANK_TAB}: first column is not RK")

    heads = [(i, str(cell).strip().upper())
             for i, cell in enumerate(rows[0])
             if isinstance(cell, str) and str(cell).strip().upper() in POSITION_STATS]

    # A repeat marks the next flavor. Counting four-then-three-then-three would work
    # on this download and break silently on the one where he ranks kickers.
    groups: List[List[tuple]] = []
    current: List[tuple] = []
    seen: set = set()
    for index, position in heads:
        if position in seen:
            groups.append(current)
            current, seen = [], set()
        current.append((index, position))
        seen.add(position)
    if current:
        groups.append(current)

    if len(groups) != len(RANK_FLAVORS):
        raise KeyError(f"{RANK_TAB}: found {len(groups)} rank blocks, "
                       f"expected {len(RANK_FLAVORS)} ({', '.join(RANK_FLAVORS)})")

    # Quarterbacks are ranked once. Their block sits in the first group and the other
    # two inherit it, which is what the workbook's own `Jake PPR`/`Jake Non` tabs do.
    later = {position for group in groups[1:] for _, position in group}
    shared = {position: index for index, position in groups[0]
              if position not in later}

    records: Dict[str, dict] = {}
    for flavor, group in zip(RANK_FLAVORS, groups):
        block = dict((position, index) for index, position in group)
        block.update({p: i for p, i in shared.items() if p not in block})
        missing = sorted(set(POSITION_STATS) - set(block))
        if missing:
            raise KeyError(f"{RANK_TAB}: {flavor} block has no "
                           f"{'/'.join(missing)} column")
        for position, index in block.items():
            for raw in rows[2:]:
                rank = raw[0]
                name = raw[index] if index < len(raw) else None
                if not isinstance(name, str) or not isinstance(rank, (int, float)):
                    continue
                record = records.setdefault(str(name).strip(), {
                    "player_name": str(name).strip(),
                    "position": position,
                    "pro_team": (str(raw[index + 1]).strip()
                                 if index + 1 < len(raw)
                                 and isinstance(raw[index + 1], str) else None),
                })
                record[RANK_COLUMNS[flavor]] = int(rank)

    frame = pd.DataFrame(list(records.values()),
                         columns=["player_name", "position", "pro_team",
                                  *RANK_COLUMNS.values()])
    for column in RANK_COLUMNS.values():
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.sort_values(
        ["position", RANK_COLUMNS[RANK_FLAVORS[0]]]).reset_index(drop=True)


#: Columns of :func:`read_workbook` that are diagnostics rather than stats.
#:
#: ``masked_stats`` is lowercase and stays out of the ``ATH_`` namespace on purpose:
#: ``UPPER_`` is reserved for blendable numerics, and both ``compute_weighted_stats``
#: and ``proj_to_score`` scan every uppercase prefix and require it to be numeric.
#: :func:`Scripts.season_projections.load_theathletic_season` reads only the stat
#: columns, so this never reaches the board.
DIAGNOSTIC_COLUMNS: Sequence[str] = ("masked_stats",)


def audit(frame: pd.DataFrame, top: int = 12) -> None:
    """Print what parsed and how much of it the ID crosswalk can confirm.

    The join into the board is by **name** -- ``normalise_name`` then ``join_key``,
    the same path FantasyPros takes -- because only 289 of the workbook's players
    carry an ID at all. This is therefore an audit rather than the join: it says how
    much of the file the crosswalk agrees exists, and names the biggest players it
    cannot place, which is where a name-join failure would cost the most.

    Args:
        frame: Output of :func:`read_workbook`.
        top: How many unmatched names to name.
    """
    counts = frame["position"].value_counts().to_dict()
    print(f"  parsed {len(frame)} players: "
          + " / ".join(f"{p} {counts.get(p, 0)}" for p in POSITIONS))

    bled = frame[frame["masked_stats"].astype(str).str.len() > 0]
    print(f"  position mask: dropped off-position stats from {len(bled)} rows")
    for _, row in bled.iterrows():
        print(f"    masked: {row['player_name']} ({row['position']}, "
              f"{row['pro_team']}) -- {row['masked_stats']}")

    try:
        from Scripts.crosswalk import load_crosswalk
        from Scripts.season_projections import normalise_name
    except Exception as exc:                                   # pragma: no cover
        print(f"  crosswalk audit skipped: {exc}")
        return

    try:
        cross = load_crosswalk()
    except Exception as exc:
        print(f"  crosswalk audit skipped: {exc}")
        return

    known = {normalise_name(n) for n in cross["name"].to_list() if n}
    keys = frame["player_name"].map(normalise_name)
    missing = frame.loc[~keys.isin(known)].copy()
    matched = len(frame) - len(missing)
    print(f"  crosswalk: {matched}/{len(frame)} names resolve "
          f"({100 * matched / max(len(frame), 1):.0f}%)")
    if len(missing):
        order = [c for c in ("receivingYards", "rushingYards", "passingYards")
                 if c in missing.columns]
        missing["_size"] = missing[order].fillna(0).sum(axis=1)
        worst = missing.sort_values("_size", ascending=False).head(top)
        for _, row in worst.iterrows():
            print(f"    unmatched: {row['player_name']} "
                  f"({row['position']}, {row['pro_team']})")


def audit_ranks(ranks: pd.DataFrame, frame: pd.DataFrame) -> None:
    """Print what the ranking covers and whether it agrees with the stat table.

    The second check is the one worth running. Both frames come out of the same
    download, so every ranked player must also be a projected player -- 290 of the
    434. A name on the ranking that the team tabs do not carry means the workbook
    spells him two ways, and that is a join miss waiting to happen downstream where
    it costs a board column rather than a print.

    Args:
        ranks: Output of :func:`read_rankings`.
        frame: Output of :func:`read_workbook`, from the same workbook.
    """
    counts = ranks["position"].value_counts().to_dict()
    print(f"  ranked {len(ranks)} players: "
          + " / ".join(f"{p} {counts.get(p, 0)}" for p in POSITIONS))

    projected = set(frame["player_name"])
    orphans = sorted(set(ranks["player_name"]) - projected)
    if orphans:
        print(f"  ranked but not projected: {len(orphans)} "
              f"-- {', '.join(orphans[:8])}")
    else:
        print(f"  every ranked player is also projected ({len(ranks)}/{len(ranks)})")

    # Where the three flavors actually disagree, so a download that silently ships one
    # list three times is visible rather than assumed away.
    half, ppr, std = (RANK_COLUMNS[f] for f in RANK_FLAVORS)
    moved = int(((ranks[ppr] - ranks[std]).abs() >= 5).sum())
    print(f"  flavors: {moved} players move 5+ spots between PPR and non-PPR")


def build(season: int, path: Path) -> pd.DataFrame:
    """Import the workbook and write the season files.

    Keeps the ``.xlsx`` under ``Landing/`` unmodified, so the tidy tables can always
    be rebuilt from what was actually downloaded, and writes both parquet and csv
    like every other source -- parquet is authoritative, the csv is for eyeballing.

    **Both artifacts, always, from one call.** The stat table and the hand ranking are
    two files off one workbook, and writing them separately would let a ranking from an
    older download sit beside fresh projections with nothing to notice. That is the
    failure this module's own third trap is about: a hand-dropped file goes stale
    because nobody downloaded a new one, and here it could go half-stale.

    Args:
        season: Season year, for the output path.
        path: The workbook to read.

    Returns:
        pd.DataFrame: The tidy stat table. The ranking is written beside it and is
        read back with :func:`read_rankings` or from :data:`RANKS_FILENAME`.
    """
    frame = read_workbook(path)
    audit(frame)
    ranks = read_rankings(path)
    audit_ranks(ranks, frame)

    kept = landing_dir(SOURCE, season, path.name)
    if path.resolve() != kept.resolve():
        shutil.copy2(path, kept)
    print(f"  landed {kept.relative_to(kept.parents[4])}")

    out = season_dir(SOURCE, season, FILENAME)
    frame.to_parquet(out)
    frame.to_csv(out.with_suffix(".csv"), index=False)
    print(f"The Athletic season-long {season}: {len(frame)} rows, "
          f"{frame['player_name'].nunique()} players -> {out.name}")

    ranks_out = season_dir(SOURCE, season, RANKS_FILENAME)
    ranks.to_parquet(ranks_out)
    ranks.to_csv(ranks_out.with_suffix(".csv"), index=False)
    print(f"The Athletic hand ranking {season}: {len(ranks)} players "
          f"-> {ranks_out.name}")
    return frame


def main(argv=None):
    """Command-line entry point.

    No side effects at import: the workbook is only read when this is called, so
    importing the module cannot overwrite a season's file.
    """
    import argparse

    from Scripts.nfl_utils import current_season

    p = argparse.ArgumentParser(
        prog="python -m Scripts.load_athletic",
        description="Import The Athletic's projection workbook (a manual download).",
    )
    p.add_argument("--season", type=int, default=None,
                   help="defaults to the schedule's season")
    p.add_argument("--file", required=True, type=Path,
                   help="path to the .xlsx workbook")
    args = p.parse_args(argv)

    season = current_season() if args.season is None else args.season
    build(int(season), args.file)


if __name__ == "__main__":
    main()
