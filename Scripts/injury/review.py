"""The weekly injury review: who needs a human, ranked by how much it matters.

The override file in ``config/injuries/<season>.yaml`` is the highest-value-per-minute
input in this package -- the fitted curve was rejected as a multiplier, while a hand-written
severity moves a player's duration bucket outright. But it is only worth writing where the
automatic ladder is *materially* wrong, and finding those players by reading a board is the
kind of manual scan that quietly stops happening in week 4.

So this prints the list. Three groups, in the order you should act on them:

**Needs a severity** -- a player inside the ADP cutoff whose reading came off a weak rung.
The `news text` rung is a regex over one sentence and the `report` rung is a body part with
no severity at all; both resolve to a group average. If a beat report says worse, that is
the gap an override closes.

**Stale** -- an override older than :data:`Scripts.injury.severity.STALE_OVERRIDE_DAYS`. A
severity written from one report stops describing the present, and a stale entry keeps
discounting a player who has been healthy for a month.

**Expired** -- an override whose own stated window has elapsed. "Four to six weeks from
18 August" is spent by early October, and that is checkable from the entry alone without
asking a feed that may still disagree.

Read off a stored board rather than rebuilt, because ADP and the resolved severity are
league-independent and one league's board is 11ms to read against ~3s to build.

Usage::

    python -m Scripts.injury.review
    python -m Scripts.injury.review --adp 200
"""

from __future__ import annotations

import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import polars as pl

from Scripts.injury import severity as sv

#: Rungs a human can beat.
#:
#: ``comment`` is a regex over a news sentence and ``report`` is a body part with no
#: severity attached; both fall back to a body-part average. ``none`` means the player is
#: listed as hurt and nothing could be read at all -- the strongest signal that a human
#: should look.
WEAK_RUNGS = ("comment", "report", "none")

#: Rungs that already carry real severity, and do not need a person.
#:
#: ``espn_structured`` is a published diagnosis and ``return_date`` is ESPN's own estimate.
#: Both beat a prior; leave them alone.
STRONG_RUNGS = ("override", "espn_structured", "return_date")

#: How deep into the board to look.
#:
#: 150 covers a 12-team roster plus the bench a trade or a bye reaches for. Past it the
#: automatic reading is good enough because nobody is starting the player.
DEFAULT_ADP_CUTOFF = 150.0

#: Expected absence, in games, below which the automatic reading needs no help.
#:
#: A camp knock resolves to about half a game on its own and a human adds nothing. The
#: measured tail worth writing about is the ~1.2 injuries a week that cost three games or
#: more.
TRIVIAL_ABSENCE = 1.0


def _short_status(status: Optional[str]) -> str:
    """ESPN's fantasy status, abbreviated for a narrow column."""
    if not status:
        return ""
    text = str(status).upper()
    return {"ACTIVE": "A", "QUESTIONABLE": "Q", "DOUBTFUL": "D", "OUT": "O",
            "INJURY_RESERVE": "IR", "SUSPENSION": "SUS"}.get(text, text[:3])


def load_board(season: int, league: Optional[str] = None) -> pl.DataFrame:
    """Read one league's stored board.

    Args:
        season: Season year.
        league: ``config.yaml`` league key. The first available when None.

    Returns:
        pl.DataFrame: The board.

    Raises:
        FileNotFoundError: When no board has been built for the season.
    """
    from Scripts import paths

    directory = paths.DATA_DIR / "Store" / str(season)
    candidates = ([directory / league / "board.parquet"] if league
                  else sorted(directory.glob("*/board.parquet")))
    for path in candidates:
        if path.is_file():
            return pl.read_parquet(path)
    raise FileNotFoundError(
        f"No draft board for {season} under {directory}. Build one with "
        f"`python -m Scripts.refresh --all --what board`.")


def needs_severity(board: pl.DataFrame,
                   adp_cutoff: float = DEFAULT_ADP_CUTOFF) -> pl.DataFrame:
    """Players inside the cutoff whose severity came off a weak rung.

    Args:
        board: A stored board carrying the ``inj_`` columns.
        adp_cutoff: How deep to look.

    Returns:
        pl.DataFrame: Sorted by ADP, most valuable first.
    """
    if "inj_severity_source" not in board.columns:
        return pl.DataFrame()
    # ``injury_status`` rather than ``injury_code``: the abbreviation is a view-layer
    # derivation in ``app/draft_view.py`` and is not on the stored artifact, so selecting it
    # here silently produced an empty column.
    columns = [c for c in ["player_name", "primaryPosition", "pro_team", "adp",
                           "injury_status", "inj_body_part", "inj_detail",
                           "inj_expected_absence_weeks", "inj_recovery_cost",
                           "inj_reinjury_prob", "inj_severity_source", "inj_evidence"]
               if c in board.columns]
    return (board
            .filter(pl.col("inj_severity_source").is_in(list(WEAK_RUNGS))
                    & (pl.col("adp").fill_null(9999) <= adp_cutoff))
            .select(columns)
            .sort("adp"))


def override_health(season: int, board: pl.DataFrame,
                    today: Optional[datetime.date] = None
                    ) -> Tuple[List[Dict], List[Dict]]:
    """Which existing overrides have gone stale, and which the feeds now contradict.

    Args:
        season: Season year.
        board: A stored board.
        today: Date to age against. Defaults to today.

    Returns:
        tuple: ``(stale, resolved)``, each a list of readable dicts.
    """
    today = today or datetime.date.today()
    try:
        overrides = sv.load_overrides(season)
    except (ValueError, FileNotFoundError) as error:
        print(f"  override file unreadable -- {error}")
        return [], []

    seen: Dict[str, Dict] = {}
    for entry in overrides.values():
        seen.setdefault(entry["_where"], entry)

    stale, resolved = [], []
    for entry in seen.values():
        as_of = entry.get("as_of")
        if isinstance(as_of, datetime.datetime):
            as_of = as_of.date()
        age = (today - as_of).days if isinstance(as_of, datetime.date) else None

        name = entry.get("player") or entry.get("name_key") or entry.get("espn_id")
        record = {"player": name, "body_part": entry.get("_part"),
                  "weeks_out": [entry.get("_low"), entry.get("_high")],
                  "as_of": str(as_of), "age_days": age,
                  "source": entry.get("source")}

        # Resolved is judged against **the override's own window**, not against ESPN's
        # status. The first draft used "ESPN now lists him active", which is exactly
        # backwards for the case this file exists to handle: Jeremiyah Love is listed
        # Active *while carrying a high ankle sprain*, and that disagreement is the whole
        # reason someone wrote him down. A check that flags every entry it was created to
        # contradict trains you to skip the section.
        #
        # An override saying "four to six weeks from 18 August" is spent by early October
        # whatever any feed says, and that is checkable from the entry alone.
        high = entry.get("_high")
        if age is not None and high is not None and age > high * 7 + 7:
            record["expired_after"] = f"{high:.0f} weeks"
            resolved.append(record)
        elif age is not None and age > sv.STALE_OVERRIDE_DAYS:
            stale.append(record)
    return stale, resolved


def report(season: int, board: pl.DataFrame,
           adp_cutoff: float = DEFAULT_ADP_CUTOFF,
           today: Optional[datetime.date] = None) -> str:
    """The weekly review, as printable text."""
    lines: List[str] = []
    candidates = needs_severity(board, adp_cutoff)
    stale, resolved = override_health(season, board, today)

    strong = 0
    if "inj_severity_source" in board.columns:
        strong = (board.filter(pl.col("inj_severity_source").is_in(list(STRONG_RUNGS))
                               & (pl.col("adp").fill_null(9999) <= adp_cutoff))
                  .height)

    lines.append(f"  Inside ADP {adp_cutoff:.0f}: {candidates.height} player(s) on a weak "
                 f"rung, {strong} already carrying real severity.")
    lines.append(f"  Override file: {sv.overrides_path(season)}")

    if candidates.is_empty():
        lines.append("")
        lines.append("  Nothing needs a severity written. Every hurt player inside the "
                     "cutoff has either a published diagnosis or an ESPN return date.")
    else:
        lines.append("")
        lines.append("  NEEDS A SEVERITY -- the automatic reading is a group average. "
                     "Correct the ones a beat report says are worse.")
        lines.append(f"    {'player':22s}{'pos':>4}{'adp':>7}{'st':>4}  "
                     f"{'reading':26s}{'wks':>5}{'cost':>6}  how")
        for row in candidates.iter_rows(named=True):
            detail = row.get("inj_detail") or row.get("inj_body_part") or "?"
            weeks = row.get("inj_expected_absence_weeks")
            cost = row.get("inj_recovery_cost")
            flag = "  <-- worth a look" if (weeks or 0) > TRIVIAL_ABSENCE else ""
            lines.append(
                f"    {str(row['player_name'])[:21]:22s}"
                f"{str(row.get('primaryPosition') or ''):>4}"
                f"{row.get('adp') or 0:7.1f}"
                f"{_short_status(row.get('injury_status')):>4}  "
                f"{str(detail)[:25]:26s}"
                f"{weeks if weeks is not None else float('nan'):5.1f}"
                f"{cost if cost is not None else float('nan'):6.2f}  "
                f"{row.get('inj_severity_source')}{flag}")
        lines.append("")
        lines.append(f"    Rows without the marker are camp knocks under "
                     f"{TRIVIAL_ABSENCE:.0f} game and need nothing.")

    if stale:
        lines.append("")
        lines.append(f"  STALE -- older than {sv.STALE_OVERRIDE_DAYS} days. Re-read the "
                     f"beat report or delete the entry.")
        for entry in stale:
            lines.append(f"    {entry['player']} -- {entry['body_part']}, written "
                         f"{entry['as_of']} ({entry['age_days']} days ago)")

    if resolved:
        lines.append("")
        lines.append("  EXPIRED -- the stated window has elapsed. Confirm he is back, "
                     "then delete.")
        for entry in resolved:
            lines.append(f"    {entry['player']} -- {entry['body_part']}, written "
                         f"{entry['as_of']}")

    lines.append("")
    lines.append("  After editing, rebuild so the boards see it:")
    lines.append("    python -m Scripts.refresh --all --what board")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Command-line entry point."""
    import argparse

    from Scripts.nfl_utils import current_season

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--season", type=int, default=None)
    parser.add_argument("--league", default=None,
                        help="Board to read. Any will do -- ADP and the resolved "
                             "severity are league-independent.")
    parser.add_argument("--adp", type=float, default=DEFAULT_ADP_CUTOFF,
                        help="How deep into the board to look.")
    args = parser.parse_args(argv)

    season = args.season if args.season is not None else current_season()
    print(f"\n===== Injury review: {season} =====")
    try:
        board = load_board(season, args.league)
    except FileNotFoundError as error:
        print(f"  {error}")
        return 1
    print(report(season, board, adp_cutoff=args.adp))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
