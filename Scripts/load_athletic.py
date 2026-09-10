"""Import The Athletic's projection workbooks into tidy stat tables.

**Hand-dropped files, not scrapes.** These are paid downloads from The Athletic (Jake
Ciely's spreadsheets) with no API behind them, so this runs when a new copy is saved
rather than nightly. Everything else about them matches the other sources: raw stat
lines in, ``Data/Projections/TheAthletic/Season/<season>/`` out.

**Two workbooks, two grains, one module.** The season book is 32 team tabs and is read
by :func:`read_workbook`; the weekly slate is a single sheet of four side-by-side
position blocks and is read by :func:`read_weekly_workbook`. They share a provider, a
prefix and a position mask and almost nothing else -- different filenames, different
column spellings, and the weekly one carries nine of the twelve stats. Both live here
so that the rules the provider needs (points are never read, the mask is always
applied) are stated once. ``--what season|weekly`` selects, following
``Scripts/scrape_FP.py``. The readers are
:func:`Scripts.season_projections.load_theathletic_season` and
:func:`Scripts.projection_utils.clean_ath_weekly`.

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

import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

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
#:
#: **True of this workbook only.** The weekly slate is the same provider on a different
#: set again (``JAC``, ``WAS``); see :data:`WEEKLY_TEAM_ALIASES`.
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


# --- the weekly slate ----------------------------------------------------
#
# A second workbook, not a second tab of the first. It arrives every week as its own
# hand-dropped download (`Week_1_Proj_0909.xlsx`) and holds one slate: four position
# blocks laid side by side on a single sheet. Everything above this line is the
# season book and is untouched by it.

#: Weekly sheet name -> the season, week and scoring flavour it holds.
#:
#: **The file identifies its own week, and that is the guard that matters.** A
#: hand-dropped file does not fail by failing to parse, it fails by being the wrong
#: download -- last week's copy still sitting in `~/Downloads` beside this week's. A
#: filename is a label a human typed; the sheet name is what the publisher generated,
#: so the week is taken from there and `--week` only confirms it.
WEEKLY_SHEET = re.compile(r"^NFL_(?P<season>\d{4})_Week_(?P<week>\d+)_"
                          r"(?P<flavor>.+?)_Weekly$")

#: Output filename, matching the FantasyPros weekly file's shape.
WEEKLY_FILENAME = f"{SOURCE}_Projections_Week_All.parquet"

#: The columns a weekly row is unique on.
#:
#: The same pair as ``Scripts.scrape_FP.WEEKLY_KEYS`` and
#: ``Scripts.projection_utils.SOURCE_JOIN_KEYS``, because it is the same join:
#: ``clean_lineups`` merges this file onto the lineup frame on ``week`` and
#: ``player_name``.
WEEKLY_KEYS: Sequence[str] = ("week", "player_name")

#: Weekly column header -> ESPN stat name.
#:
#: **A separate map from :data:`STAT_COLUMNS`, deliberately.** The two workbooks come
#: from the same publisher and are otherwise unrelated files: this one spells them
#: ``Pass YD`` where the season book says ``PASS YARDS``, and it carries **nine** of
#: the twelve -- no ``PASS ATT``, no ``COMP``, no ``TARGETS``. Sharing one dict would
#: mean a lookup that silently misses on six of nine headers.
#:
#: The three it omits are created by ``impute_columns`` from ``MEAN_`` with the
#: provenance flag set on every row, so ``compute_weighted_stats`` drops the weight and
#: renormalises -- the handling :data:`STAT_COLUMNS` describes for the season file's
#: missing ``lostFumbles``. ``receivingTargets`` therefore stays an ESPN-plus-season-
#: Athletic number on the weekly path, which is worth knowing: it is the thinnest
#: covered stat on the board and the reason ``ATH`` joined ``MEAN_SOURCES`` at all.
WEEKLY_STAT_COLUMNS: Dict[str, str] = {
    "Pass YD": "passingYards",
    "Pass TD": "passingTouchdowns",
    "INT": "passingInterceptions",
    "Rush Att": "rushingAttempts",
    "Rush YD": "rushingYards",
    "Rush TD": "rushingTouchdowns",
    "REC": "receivingReceptions",
    "REC YD": "receivingYards",
    "REC TD": "receivingTouchdowns",
}

#: Headers that identify a player rather than projecting him.
#:
#: ``FPS`` is not here and is not in :data:`WEEKLY_STAT_COLUMNS` either -- it is the
#: workbook's own half-PPR total, and points are what a league's rules do to a stat
#: line. It is also derived from the nine columns above rather than independent of
#: them (Joe Burrow, week 1: 19.3 published against 19.66 recomputed), so reading it
#: would ship somebody else's rounding as though it were an opinion.
WEEKLY_ID_COLUMNS: Sequence[str] = ("Name", "Team", "Opp")

#: Weekly ``Team`` value -> ESPN's abbreviation for that club.
#:
#: **The two Athletic workbooks do not agree with each other**, which
#: :data:`TEAM_TABS` above asserts they do -- its comment says the provider is "already
#: on ESPN's abbreviations ... so no alias map is needed here". That is true of the
#: season book and false of the weekly slate. Measured on the 2026 week-1 download, the
#: weekly sheet's 32 teams are a *third* set: ``JAC`` (which is neither ESPN's ``JAX``
#: nor nflverse's ``JAX``) and ``WAS`` (nflverse's spelling, against ESPN's ``WSH``),
#: while ``LAR`` follows ESPN rather than nflverse's ``LA``.
#:
#: Normalised to ESPN here rather than left to the reader, so both grains write the same
#: ``pro_team`` and :func:`_bye_teams` can translate once through
#: :data:`Scripts.draft.board.ESPN_TEAM_ALIASES` like every other ESPN-keyed caller.
#: Left alone, the bye-week check reported Jacksonville absent from a week it played --
#: a guard that cries wolf every week is one nobody reads.
WEEKLY_TEAM_ALIASES: Dict[str, str] = {"JAC": "JAX", "WAS": "WSH"}


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


def _cell(row: Sequence, index: Optional[int]):
    """One cell of a block, or None when the column is absent or the row is short.

    Rows on the weekly sheet are ragged -- ``openpyxl`` stops a row at its last
    populated cell, so a tight end with no rushing line yields a shorter tuple than
    the header. Indexing that directly is the ``IndexError`` this exists to not have.

    Args:
        row: One block's slice of a spreadsheet row.
        index: Zero-based column index within the block, or None when the block has
            no such header.

    Returns:
        The cell value, or None.
    """
    if index is None or index >= len(row):
        return None
    return row[index]


def _weekly_team(value) -> Optional[str]:
    """A weekly ``Team`` cell as ESPN spells it.

    Args:
        value: The raw cell, or None.

    Returns:
        str | None: The ESPN abbreviation, or None for a blank cell. An abbreviation
        with no entry in :data:`WEEKLY_TEAM_ALIASES` passes through -- 30 of the 32
        already agree, and a new club is not this function's problem to invent.
    """
    if value is None:
        return None
    team = str(value).strip().upper()
    if not team:
        return None
    return WEEKLY_TEAM_ALIASES.get(team, team)


def _weekly_sheet(book, path: Path) -> Tuple[str, int, int, str]:
    """The one sheet holding a weekly slate, and what it says it holds.

    Args:
        book: An open ``openpyxl`` workbook.
        path: The file, for the message.

    Returns:
        tuple: ``(sheet_name, season, week, flavor)``.

    Raises:
        KeyError: If the number of sheets matching :data:`WEEKLY_SHEET` is not exactly
            one. Both directions are refused rather than resolved: no match means the
            file is not a weekly slate, and two matches means choosing which week to
            import, which is not a choice this function should make silently.
    """
    matched = [(name, WEEKLY_SHEET.match(name)) for name in book.sheetnames]
    matched = [(name, m) for name, m in matched if m is not None]
    if len(matched) != 1:
        raise KeyError(
            f"{path.name}: expected exactly one sheet named like "
            f"NFL_<season>_Week_<n>_<flavor>_Weekly, found {len(matched)} "
            f"among {book.sheetnames}")
    name, m = matched[0]
    return name, int(m.group("season")), int(m.group("week")), m.group("flavor")


def read_weekly_workbook(path: Path) -> Tuple["pd.DataFrame", int, int, str]:
    """Parse a weekly slate's position blocks into one tidy row per player.

    **The sheet is four blocks laid side by side, not four tabs.** A sparse banner row
    names each block, a header row sits beneath it, players start on the third row, and
    a blank spacer column separates one block from the next. The blocks do *not* share
    a column set: the receiver block has no ``Rush Att`` and the tight-end block has no
    rushing columns at all.

    So the geometry is read rather than assumed, twice over. Blocks are bounded
    **banner to banner**, which makes the spacer columns need no special case and stops
    an unequal block bleeding into the next one. And each block's headers go through
    :func:`_header_map`, the same function the season tabs use -- for the season book
    that is merely free, and here it is load-bearing: a download that gains a kicker
    block or drops a column shifts instead of reading a receiver's carries into a tight
    end.

    Args:
        path: The ``.xlsx`` workbook.

    Returns:
        tuple: ``(frame, season, week, flavor)``. The frame carries ``week``,
        ``player_name``, ``pro_team``, ``position``, ``masked_stats`` and one
        ``proj_<stat>`` column per :data:`WEEKLY_STAT_COLUMNS`.

        **The ``proj_`` prefix is required, not stylistic.**
        :func:`Scripts.projection_utils.clean_lineups` renames it to ``ATH_`` with
        ``change_col_prefix``, exactly as it does for the three sources already there.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        KeyError: If the sheet cannot be identified (:func:`_weekly_sheet`), the banner
            row names no known position, a block has no ``Name`` column, or no player
            rows parse at all.
    """
    import openpyxl

    if not path.exists():
        raise FileNotFoundError(path)

    book = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        sheet_name, season, week, flavor = _weekly_sheet(book, path)
        rows = list(book[sheet_name].iter_rows(values_only=True))
    finally:
        book.close()

    if len(rows) < 3:
        raise KeyError(f"{path.name}: {sheet_name} holds {len(rows)} row(s); expected "
                       f"a banner row, a header row and at least one player")

    banners = [(i, str(value).strip().upper())
               for i, value in enumerate(rows[0])
               if value is not None and str(value).strip()]
    starts = [(i, position) for i, position in banners if position in POSITION_STATS]
    if not starts:
        raise KeyError(f"{path.name}: {sheet_name} row 1 names no known position; "
                       f"found {[p for _, p in banners]}")

    records: List[dict] = []
    for index, (start, position) in enumerate(starts):
        end = starts[index + 1][0] if index + 1 < len(starts) else len(rows[1])
        header = _header_map(rows[1][start:end])
        if "Name" not in header:
            raise KeyError(f"{path.name}: {sheet_name} {position} block has no Name "
                           f"column; found {sorted(header)}")
        allowed = POSITION_STATS[position]

        for raw in rows[2:]:
            block = raw[start:end]
            name = _cell(block, header["Name"])
            if name is None or not str(name).strip():
                # Blocks are different lengths -- 95 receivers against 30 tight ends --
                # so every block but the longest runs out of players mid-sheet.
                continue
            record = {
                "week": week,
                "player_name": str(name).strip(),
                "pro_team": _weekly_team(_cell(block, header.get("Team"))),
                "position": position,
            }
            masked = []
            for head, stat in WEEKLY_STAT_COLUMNS.items():
                value = _cell(block, header.get(head))
                if stat in allowed:
                    record[f"proj_{stat}"] = value
                    continue
                record[f"proj_{stat}"] = None
                if value is not None and float(value or 0.0) != 0.0:
                    masked.append(stat)
            # Recorded rather than merely dropped, for the reason `read_workbook` gives:
            # by the time the output frame exists the offending values are None and
            # every count reads zero. The mask is near-trivially satisfied here because
            # the blocks are position-partitioned by construction -- it is kept because
            # a download whose tight-end block gains a rush column is exactly the drift
            # that would hand tight ends carries with nothing to notice.
            record["masked_stats"] = ",".join(masked)
            records.append(record)

    if not records:
        raise KeyError(f"{path.name}: {sheet_name} parsed no players")

    frame = pd.DataFrame(records)
    for stat in WEEKLY_STAT_COLUMNS.values():
        column = f"proj_{stat}"
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    # int64 rather than whatever pandas infers, because it is a merge key and pandas
    # validates dtype compatibility on one even when the other side is empty -- the
    # same reason `absent_weekly_source` sets it explicitly.
    frame["week"] = frame["week"].astype("int64")

    rows_in = len(frame)
    frame = frame.drop_duplicates(subset=list(WEEKLY_KEYS), keep="first")
    if len(frame) != rows_in:
        print(f"  dropped {rows_in - len(frame)} duplicate player row(s)")

    return frame.reset_index(drop=True), season, week, flavor


def _bye_teams(frame: "pd.DataFrame", season: int, week: int) -> List[str]:
    """Teams in the workbook that are not playing in the week it claims to be.

    Args:
        frame: Output of :func:`read_weekly_workbook`.
        season: Season year.
        week: Week the workbook names.

    Returns:
        list: Team abbreviations on the sheet with no game that week, sorted. Empty
        when the schedule cannot be read -- an unreadable schedule is not evidence
        about the workbook.
    """
    try:
        from Scripts.draft.board import ESPN_TEAM_ALIASES
        from Scripts.nfl_utils import load_schedule
        schedule = load_schedule()
    except Exception:                                          # noqa: BLE001
        return []

    playing = set()
    for row in schedule.filter(
            (schedule["season"] == int(season)) & (schedule["week"] == int(week))
    ).iter_rows(named=True):
        playing.update({row["home_team"], row["away_team"]})
    if not playing:
        return []

    # The schedule is nflverse-keyed and this workbook is on ESPN's abbreviations, so
    # `WSH` and `LAR` have to be translated or they read as byes every single week.
    on_sheet = {str(t).strip() for t in frame["pro_team"].dropna().unique()}
    return sorted(t for t in on_sheet
                  if ESPN_TEAM_ALIASES.get(t, t) not in playing)


def audit_weekly(frame: "pd.DataFrame", season: int, week: int, flavor: str,
                 top: int = 12) -> None:
    """Print what parsed, whether it is the week it claims, and what the crosswalk knows.

    The middle check is why this is not just a row count. A hand-dropped file's
    realistic failure is not a parse error, it is importing the wrong download -- and
    a team on bye in week N cannot appear in a week-N slate. That is the cheapest
    signal available that the file is stale, and it costs one schedule read.

    Printed rather than raised, in both directions: the schedule on disk can itself be
    stale (``R/GetNFL.R`` writes it), and the sheet name has already refused the
    obvious mistake.

    Args:
        frame: Output of :func:`read_weekly_workbook`.
        season: Season year.
        week: Week the workbook names.
        flavor: The workbook's scoring flavour, printed rather than acted on -- ``FPS``
            is not read, so a switch from the half-PPR download to the full-PPR one
            cannot change a stat line. It should still be visible in the log rather
            than silent.
        top: How many unmatched names to name.
    """
    counts = frame["position"].value_counts().to_dict()
    print(f"  week {week} of {season} ({flavor}): parsed {len(frame)} players -- "
          + " / ".join(f"{p} {counts.get(p, 0)}" for p in POSITIONS))

    bled = frame[frame["masked_stats"].astype(str).str.len() > 0]
    if len(bled):
        print(f"  position mask: dropped off-position stats from {len(bled)} rows")
        for _, row in bled.iterrows():
            print(f"    masked: {row['player_name']} ({row['position']}, "
                  f"{row['pro_team']}) -- {row['masked_stats']}")

    byes = _bye_teams(frame, season, week)
    if byes:
        print(f"  WARNING: {len(byes)} team(s) on the sheet are not playing in week "
              f"{week}: {', '.join(byes)}. Is this the right download?")

    try:
        from Scripts.crosswalk import load_crosswalk
        from Scripts.season_projections import normalise_name
        cross = load_crosswalk()
    except Exception as exc:                                   # noqa: BLE001
        print(f"  crosswalk audit skipped: {exc}")
        return

    known = {normalise_name(n) for n in cross["name"].to_list() if n}
    keys = frame["player_name"].map(normalise_name)
    missing = frame.loc[~keys.isin(known)].copy()
    matched = len(frame) - len(missing)
    print(f"  crosswalk: {matched}/{len(frame)} names resolve "
          f"({100 * matched / max(len(frame), 1):.0f}%)")
    if len(missing):
        order = [c for c in ("proj_receivingYards", "proj_rushingYards",
                             "proj_passingYards") if c in missing.columns]
        missing["_size"] = missing[order].fillna(0).sum(axis=1)
        worst = missing.sort_values("_size", ascending=False).head(top)
        for _, row in worst.iterrows():
            print(f"    unmatched: {row['player_name']} "
                  f"({row['position']}, {row['pro_team']})")


def build_weekly(path: Path, season: Optional[int] = None, week: Optional[int] = None,
                 merge: bool = True, force: bool = False) -> "pd.DataFrame":
    """Import one weekly slate and write the cumulative weekly file.

    **The newest download wins for the week it names**, and only for that week -- every
    other week in the file is left exactly as it was. This is a deliberate departure
    from ``Scripts.scrape_FP.scrape_weekly``, which merges ``keep="first"`` so that a
    re-scrape cannot rewrite the pre-game opinion the blend voted with. The difference
    is that this file is downloaded by hand: an updated copy on Sunday morning is the
    normal case rather than an accident, and the owner asked for the fresh numbers. The
    cost is that a re-import after kickoff *does* rewrite history, so the write says
    out loud how many rows it replaced.

    **The file must stay cumulative**, whichever way it is written.
    :func:`Scripts.projection_utils.clean_lineups` re-merges it onto every week in the
    lineup frame, and that frame gains a week every Tuesday -- a current-week-only file
    would blank The Athletic for every prior week and turn stored history into a
    four-source board retroactively.

    Args:
        path: The ``.xlsx`` workbook.
        season: Override the season. Defaults to the sheet name's, which is the
            authority; a mismatch raises unless ``force``.
        week: Override the week. Same rule.
        merge: Combine with whatever the file already holds. False rewrites it from
            this workbook alone, which is how to discard a bad backfill on purpose.
        force: Accept a ``season``/``week`` that disagrees with the sheet name. For the
            download whose sheet is mislabelled, which is the only case where a human
            knows better than the file.

    Returns:
        pd.DataFrame: The whole file as written -- every week it now holds, not just
        the one this call imported, so a caller can row-count what shipped.

    Raises:
        ValueError: If ``season`` or ``week`` disagrees with the sheet name and
            ``force`` is not set.
    """
    frame, sheet_season, sheet_week, flavor = read_weekly_workbook(path)

    for label, given, found in (("season", season, sheet_season),
                                ("week", week, sheet_week)):
        if given is not None and int(given) != found:
            if not force:
                raise ValueError(
                    f"{path.name} holds {label} {found} but --{label} says "
                    f"{int(given)}. This is usually the wrong file rather than a "
                    f"mislabelled sheet; pass --force to import it as "
                    f"{label} {int(given)} anyway.")
            print(f"  WARNING: --force overrides the sheet's {label} {found} "
                  f"with {int(given)}")

    season = sheet_season if season is None else int(season)
    week = sheet_week if week is None else int(week)
    if week != sheet_week:
        frame["week"] = week

    audit_weekly(frame, season, week, flavor)

    kept = landing_dir(SOURCE, season, path.name)
    if path.resolve() != kept.resolve():
        shutil.copy2(path, kept)
    print(f"  landed {kept.relative_to(kept.parents[4])}")

    out = season_dir(SOURCE, season, WEEKLY_FILENAME)
    written = frame
    if merge and out.is_file():
        existing = pd.read_parquet(out)
        replaced = int((existing["week"] == week).sum())
        written = pd.concat([existing[existing["week"] != week], frame],
                            ignore_index=True)
        written = written.sort_values(list(WEEKLY_KEYS)).reset_index(drop=True)
        if replaced:
            print(f"  replaced {replaced} row(s) already held for week {week}")

    written.to_parquet(out)
    written.to_csv(out.with_suffix(".csv"), index=False)
    print(f"The Athletic weekly {season}: imported week {week} -- {len(frame)} rows, "
          f"{frame['player_name'].nunique()} players; file now holds "
          f"{len(written)} rows over weeks "
          f"{sorted(pd.unique(written['week']))} -> {out.name}")
    return written


def main(argv=None):
    """Command-line entry point.

    No side effects at import: the workbook is only read when this is called, so
    importing the module cannot overwrite a season's file.

    ``--what`` defaults to ``season`` so that every existing invocation keeps working
    unchanged -- including the fix hint
    :data:`Scripts.refresh_status.PROJECTION_SOURCES` prints, which is the one a reader
    copies at 6am when the status check goes red.
    """
    import argparse

    from Scripts.nfl_utils import current_season

    p = argparse.ArgumentParser(
        prog="python -m Scripts.load_athletic",
        description="Import The Athletic's projection workbooks (manual downloads).",
    )
    p.add_argument("--what", choices=("season", "weekly"), default="season",
                   help="which workbook this is (default: season)")
    p.add_argument("--season", type=int, default=None,
                   help="season: defaults to the schedule's season. weekly: defaults "
                        "to the sheet name's, and must agree with it")
    p.add_argument("--week", type=int, default=None,
                   help="weekly only: defaults to the sheet name's, and must agree "
                        "with it")
    p.add_argument("--file", required=True, type=Path,
                   help="path to the .xlsx workbook")
    p.add_argument("--no-merge", dest="merge", action="store_false",
                   help="weekly only: rewrite the file from this workbook alone "
                        "instead of adding this week to it")
    p.add_argument("--force", action="store_true",
                   help="weekly only: import even though --season/--week disagrees "
                        "with the sheet name")
    args = p.parse_args(argv)

    if args.what == "weekly":
        build_weekly(args.file, season=args.season, week=args.week,
                     merge=args.merge, force=args.force)
        return

    for flag, value in (("--week", args.week), ("--force", args.force)):
        if value:
            p.error(f"{flag} applies to --what weekly")
    season = current_season() if args.season is None else args.season
    build(int(season), args.file)


if __name__ == "__main__":
    main()
