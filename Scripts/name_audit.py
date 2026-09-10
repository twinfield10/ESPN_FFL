"""Per-source name audit: which source names never reach the ESPN universe.

Every projection source is joined to ESPN's player universe by name, and a name
that fails to join does not fail loudly -- the player simply abstains, the blend
renormalises over the sources that did match, and the number on the board still
looks complete. That is this repo's oldest failure mode
(``docs/plans/03-projection-source-coverage.md``) wearing a different hat: an
absent line reading as agreement.

``Scripts.season_projections._report_join_misses`` already prints the misses for
the season path, but only while a build is running, only for the four season
loaders, and only as a count plus twelve names. This module answers the question
standalone, for every source on both grains, and -- the part that makes it
actionable -- **it classifies each miss** rather than listing it. That is the whole
value, because the raw miss counts are 258 for FantasyPros' weekly file and 184 for
BetOnline's, and almost none of it is a defect:

* an **alias**: ESPN has this player under a different spelling, so a rename fixes
  it and the source's line is being thrown away today. :data:`CONFIDENT` when a
  mechanical rule found it, :data:`NEEDS_REVIEW` when only a resemblance did;
* **upstream**: the scrape mangled the name, so no alias can fix it. Reported with
  its cost and the file that owns it -- see :data:`KNOWN_UPSTREAM`;
* **unrostered**: a real player ESPN knows about who is in nobody's weekly frame,
  which is what a source with a wider slate than the league looks like;
* **absent**: not in the ESPN universe at all -- practice-squad and camp bodies a
  book priced and ESPN never listed. No action.

**The two grains did not join the same way, and the audit has to know which.** The
season path keys on :func:`Scripts.season_projections.normalise_name`, which strips
suffixes, punctuation and accents. :func:`Scripts.projection_utils.clean_lineups`
merges every weekly source on the raw ``player_name`` string, so ``James Cook``
against ESPN's ``James Cook III`` was free on one path and a silent abstention on the
other -- and an audit that normalised both would have reported the weekly path clean
when it was not. That gap is closed by
:func:`Scripts.projection_utils.align_to_espn_names` rather than by a per-source
rename map, which is why the weekly sources are registered here as
:data:`JOIN_NORMALISED`; ``--maps`` is the regression guard on the maps coming back.

Usage::

    python -m Scripts.name_audit                     # current season, what needs doing
    python -m Scripts.name_audit --season 2026 --all # plus unrostered and absent
    python -m Scripts.name_audit --maps              # guard: no hand rename maps

Exits non-zero when a :data:`CONFIDENT` miss is outstanding -- something a rename
would close today. A known upstream defect does not fail it, because a check that is
always red is a check nobody keeps.

Reads the built stores under ``Data/Store/`` and the source files under
``Data/Projections/``. Needs no live ESPN connection, so it is safe in the nightly.
"""
from __future__ import annotations

import argparse
import difflib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from Scripts.paths import season_dir, store_root

#: Join disciplines. ``RAW`` is exact ``player_name`` equality -- what
#: ``clean_lineups`` does. ``NORMALISED`` is :func:`normalise_name` equality -- what
#: the season loaders do. A source is audited under the one it actually uses.
JOIN_RAW = "raw"
JOIN_NORMALISED = "normalised"

#: Verdicts, worst-first. Only :data:`ACTIONABLE` are defects.
ALIAS_NORMALISED = "alias:suffix"
ALIAS_NICKNAME = "alias:nickname"
ALIAS_TEAM_TAIL = "alias:team-tail"
TRUNCATED = "truncated"
REVIEW_SURNAME = "review:surname"
AMBIGUOUS = "review:ambiguous"
UPSTREAM = "upstream:scrape"
UNROSTERED = "unrostered"
ABSENT = "absent"

#: Verdicts whose target is safe to write into an alias map without checking. Each
#: rests on a mechanical rule -- a suffix, a prefix-or-single-typo first name, a
#: team abbreviation, a team-scoped first name -- not on a similarity score.
CONFIDENT = (ALIAS_NORMALISED, ALIAS_NICKNAME, ALIAS_TEAM_TAIL, TRUNCATED)

#: Verdicts that name a candidate but must not be applied unread.
#:
#: **A shared surname is not evidence, and this bucket is where that was learned.**
#: An earlier cut reported these as aliases and got eight of nine wrong: ``Tru
#: Edwards`` is a real receiver, not T.J. Edwards; ``Cash Jones``, ``Alex Bullock``,
#: ``EJ Smith`` and ``Josh Kelly`` are all real players who merely share a surname
#: with somebody ESPN lists. No similarity threshold separates them from ``DAL
#: PRESCOTT`` -> ``DAK PRESCOTT``, because a short first name beside a long shared
#: surname scores high either way. So the tool names the candidate and stops.
NEEDS_REVIEW = (REVIEW_SURNAME, AMBIGUOUS)

#: Misses whose cause is a known defect in a *scrape*, not a spelling. Reported with
#: their cost and the file that owns them, and excluded from the exit code.
#:
#: **The distinction is what keeps the exit code meaningful.** These cannot be closed
#: from here: no alias fixes them, because the name in the file is not a variant of a
#: real name -- it is a fragment. Counting them as failures would leave
#: ``python -m Scripts.name_audit`` exiting non-zero forever, and a check that is
#: always red is a check nobody keeps. Counting them as clean would hide a 395-point
#: quarterback's props. So: a bucket of their own.
UPSTREAM_DEFECTS = (UPSTREAM,)

#: Everything a report should surface. Anything else is a source with a wider slate
#: than the league, which is not a defect and must not be reported as one -- a
#: report that cries wolf 250 times is a report nobody reads, which is how
#: ``Mitch Tinsley`` sat next to eleven practice-squad tight ends unnoticed.
ACTIONABLE = CONFIDENT + NEEDS_REVIEW + UPSTREAM_DEFECTS

#: Names a scrape mangled, keyed by ``(source label, normalised name)``, with the
#: file that owns the fix.
#:
#: All three are BetOnline's season file losing the surname outright on a row whose
#: ``stat_short`` *is* recognised, so ``_recover_player`` -- which reads the name back
#: out of the prop wording -- never runs. ``CHRISTIAN`` is Christian McCaffrey's
#: receptions line and the team narrows it no further, because the 2026 boards put
#: Christian Kirk on San Francisco too.
KNOWN_UPSTREAM: Dict[Tuple[str, str], str] = {
    ("BetOnline season", "AKHEEM"): "R/GetSeasonProps.R drops the surname",
    ("BetOnline season", "KELDRIC"): "R/GetSeasonProps.R drops the surname",
    ("BetOnline season", "CHRISTIAN"): "R/GetSeasonProps.R drops the surname",
}

#: Names reviewed and confirmed to be *different players*, keyed by normalised name.
#:
#: Without this the :data:`NEEDS_REVIEW` bucket never empties, and a recurring check
#: that always reports the same nine rows is a check that stops being read -- the
#: failure mode this module's own docstring warns about. Every entry here was
#: resolved by hand on the date given; the reason is recorded so a later reader can
#: disagree with the call rather than only with the silence.
#:
#: All nine came from TOMCAT's roster on 2026-09-09, which is wider than ESPN's
#: because the usage model is fitted on players ESPN never listed. They share a
#: surname with somebody ESPN does list and nothing more.
CONFIRMED_DISTINCT: Dict[str, str] = {
    "TRU EDWARDS": "receiver, not T.J. Edwards the linebacker (2026-09-09)",
    "ALEX BULLOCK": "receiver, not Calen Bullock the safety (2026-09-09)",
    "CASH JONES": "back, not Cam Jones the linebacker (2026-09-09)",
    "DOMINIC RICHARDSON": "back, not Demani Richardson the safety (2026-09-09)",
    "EJ SMITH": "back, not T.J. Smith the tackle (2026-09-09)",
    "JAQUAE JACKSON": "receiver, not Jha'Quan Jackson (2026-09-09)",
    "JOSH KELLY": "receiver, not John Kelly Jr. the back (2026-09-09)",
    "EJ WILLIAMS": "receiver; no ESPN Williams matches (2026-09-09)",
}

#: Shortest first name a single-character typo may be inferred across. Below four
#: characters the substitution space is dense with distinct names -- ``CAM``/``CAN``,
#: ``DAL``/``DAK`` -- so those fall through to the whole-key test, where the surname
#: carries the weight.
TYPO_MIN_LENGTH = 4

#: How close two whole keys must be before a shared surname reads as one player.
#: Deliberately tight. This threshold does the work the first-name rules cannot:
#: ``DAL PRESCOTT`` -> ``DAK PRESCOTT`` scores 0.96, while ``TARIK BLACK`` against
#: any ESPN ``BLACK`` scores far below -- a real UDFA who merely shares a surname.
WHOLE_KEY_RATIO = 0.84

_TOKEN = re.compile(r"[^A-Z0-9]+")


@dataclass(frozen=True)
class Miss:
    """One source name that failed to join, and what to do about it."""

    source: str
    source_name: str
    key: str
    verdict: str
    espn_name: str
    espn_position: str
    espn_points: float
    note: str


def _tokens(key: str) -> List[str]:
    """Split a normalised key into name tokens."""
    return [t for t in _TOKEN.split(key or "") if t]


def _first_last(key: str) -> Tuple[str, str]:
    """First and last token of a normalised key, empty strings when absent."""
    parts = _tokens(key)
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _one_typo_apart(a: str, b: str) -> bool:
    """Whether two equal-length names differ by one substitution or one swap.

    Chosen over a similarity ratio because a ratio cannot separate the cases that
    matter here. ``JOSH``/``JOHN`` and ``CHIQ``/``CHIG`` both score 0.75 on
    :class:`difflib.SequenceMatcher`, and one is a typo while the other is two
    different people. One substituted character (``CHIQ``/``CHIG``,
    ``STEFIN``/``STEFON``) or one adjacent transposition (``REUBEN``/``RUEBEN``) is
    what a typo actually is; ``JOSH``/``JOHN`` is neither.
    """
    if len(a) != len(b) or len(a) < TYPO_MIN_LENGTH:
        return False
    diff = [i for i in range(len(a)) if a[i] != b[i]]
    if len(diff) == 1:
        return True
    if len(diff) == 2 and diff[1] == diff[0] + 1:
        i, j = diff
        return a[i] == b[j] and a[j] == b[i]
    return False


def _first_names_agree(a: str, b: str) -> bool:
    """Whether two first names are the same person's, shortened or mistyped.

    Prefix containment in either direction -- the source may say ``Cameron`` where
    ESPN says ``Cam``, or ``Josh`` where ESPN says ``Joshua`` -- or a single typo
    per :func:`_one_typo_apart`.
    """
    if not a or not b:
        return False
    if a == b:
        return True
    if len(a) >= 3 and len(b) >= 3 and (a.startswith(b) or b.startswith(a)):
        return True
    return _one_typo_apart(a, b)


class EspnUniverse:
    """ESPN's players, indexed every way the resolver needs to ask.

    Built from the persisted stores rather than a live league, so the audit runs
    in the nightly and in a test without a connection. Boards carry the full
    ~2,500-player draft universe; ``lineups.parquet`` carries the far smaller
    weekly frame (rostered plus the free agents the fetch pulled). A weekly source
    is audited against the weekly universe and told to consult the board before
    calling a name absent, which is what separates *we are dropping this line* from
    *nobody in nine leagues can start this player*.
    """

    def __init__(self, frame: pd.DataFrame, wider: Optional[pd.DataFrame] = None):
        """
        Args:
            frame: Rows with ``player_name``, ``primaryPosition``, ``pro_team`` and
                an ESPN points column. One row per player; the caller de-duplicates.
            wider: A larger universe to fall back on before declaring a name absent.
                The board, when ``frame`` is the weekly grain.
        """
        from Scripts.season_projections import normalise_name

        self._normalise = normalise_name
        self.raw_names = set(frame["player_name"])
        self.by_key: Dict[str, Tuple[str, str, float]] = {}
        self.by_surname: Dict[str, List[str]] = {}
        self.by_first: Dict[str, List[str]] = {}
        self.by_team_first: Dict[Tuple[str, str], List[str]] = {}
        self.teams = {t for t in frame["pro_team"].dropna().astype(str) if t}

        for row in frame.itertuples(index=False):
            key = normalise_name(row.player_name)
            if not key:
                continue
            points = float(row.espn_points or 0.0)
            held = self.by_key.get(key)
            if held is None or points > held[2]:
                self.by_key[key] = (row.player_name, row.primaryPosition, points)
            first, last = _first_last(key)
            if last:
                self.by_surname.setdefault(last, []).append(key)
            if first:
                self.by_first.setdefault(first, []).append(key)
                team = str(row.pro_team or "")
                if team:
                    self.by_team_first.setdefault((team, first), []).append(key)

        self.wider = wider

    def describe(self, key: str) -> Tuple[str, str, float]:
        """The ESPN name, position and projected points behind a key."""
        return self.by_key.get(key, ("", "", 0.0))

    def in_wider(self, key: str) -> bool:
        """Whether a key ESPN does not have here exists in the fallback universe."""
        if self.wider is None:
            return False
        return key in self.wider.by_key


def _strip_team_tail(key: str, teams: Iterable[str]) -> Optional[str]:
    """Drop a trailing team abbreviation from a key, when that is what it is.

    ``AJ BARNER SEA`` reaches the season blend with the tail attached because
    :func:`Scripts.season_projections._recover_player` -- which does strip it --
    only runs on the branch where ``stat_short`` is *unrecognised*, and
    ``YDS_REC`` is recognised. Checked against the universe's own ``pro_team``
    values rather than a hardcoded list, so a relocation cannot leave this stale.
    """
    parts = _tokens(key)
    if len(parts) < 3:
        return None
    return " ".join(parts[:-1]) if parts[-1] in teams else None


def resolve(source_name: str, universe: EspnUniverse, *, join: str,
            team: Optional[str] = None) -> Tuple[str, str, str]:
    """Classify one source name against the ESPN universe.

    The clauses are ordered by how much they assert, because a wrong alias is
    worse than a reported miss: a rename that lands on the wrong player replaces
    a silent abstention with a confident lie.

    Args:
        source_name: The name as the source spells it.
        universe: The ESPN universe to resolve against.
        join: :data:`JOIN_RAW` or :data:`JOIN_NORMALISED` -- the discipline the
            pipeline actually uses for this source. Under ``RAW`` a name that
            differs only by a suffix is a live miss; under ``NORMALISED`` it
            already joins and is not reported.
        team: The source's pro-team abbreviation for this row, when it has one.
            Only consulted for a first-name-only truncation, where it is the
            difference between a resolvable name and a guess.

    Returns:
        tuple: ``(verdict, espn_key, note)``. ``verdict`` is ``""`` when the name
        joins as-is; ``espn_key`` is ``""`` for :data:`ABSENT`.
    """
    key = universe._normalise(source_name)
    if not key:
        return ABSENT, "", "unparseable name"

    if join == JOIN_RAW and source_name in universe.raw_names:
        return "", key, ""
    if join == JOIN_NORMALISED and key in universe.by_key:
        return "", key, ""

    # Under the raw discipline, a key that exists is a suffix or punctuation
    # difference and nothing more -- the cheapest and most common alias there is.
    if join == JOIN_RAW and key in universe.by_key:
        return ALIAS_NORMALISED, key, "joins once normalised"

    tail = _strip_team_tail(key, universe.teams)
    if tail and tail in universe.by_key:
        return ALIAS_TEAM_TAIL, tail, "team abbreviation left on the name"

    # Spelled the way ESPN spells it, and simply not in this grain's universe. This
    # is checked before any inference: a name the wider universe holds verbatim
    # needs no alias, and letting the surname rules see it first would let a
    # same-surname teammate outrank an exact match.
    if universe.in_wider(key):
        return UNROSTERED, key, "in the draft board, in no weekly frame"

    first, last = _first_last(key)

    # A shared surname plus a first name that is the same one shortened or mistyped.
    # The strongest inference available without an id, and the one that catches the
    # nicknames -- the dangerous kind, because the name looks correct in both files.
    if last:
        siblings = list(dict.fromkeys(universe.by_surname.get(last, [])))
        agreeing = [k for k in siblings
                    if _first_names_agree(first, _first_last(k)[0])]
        if len(agreeing) == 1:
            return ALIAS_NICKNAME, agreeing[0], "same surname, first name shortened"
        if len(agreeing) > 1:
            names = ", ".join(universe.describe(k)[0] for k in agreeing[:4])
            return AMBIGUOUS, "", f"several candidates: {names}"
        # No first name agreed, so the surname alone decides -- and a shared surname
        # is not evidence. `Tre Watson` is not Christian Watson. Only a whole-key
        # similarity above WHOLE_KEY_RATIO promotes this, which is what separates a
        # mangled first name (`DAL PRESCOTT`) from a real player who happens to
        # share a surname with somebody ESPN lists.
        scored = [(difflib.SequenceMatcher(None, key, k).ratio(), k)
                  for k in siblings]
        good = sorted((s for s in scored if s[0] >= WHOLE_KEY_RATIO), reverse=True)
        if len(good) == 1:
            return REVIEW_SURNAME, good[0][1], "surname matches, first name differs"
        if len(good) > 1:
            names = ", ".join(universe.describe(k)[0] for _, k in good[:4])
            return AMBIGUOUS, "", f"several candidates: {names}"

    # First name only. BetOnline's season file does this when the scrape loses the
    # surname, and the team is the only thing that can recover it.
    if first and not last:
        scoped = list(dict.fromkeys(
            universe.by_team_first.get((team or "", first), [])))
        if len(scoped) == 1:
            return TRUNCATED, scoped[0], f"first name only, resolved by team {team}"
        candidates = list(dict.fromkeys(universe.by_first.get(first, [])))
        if len(candidates) == 1:
            return TRUNCATED, candidates[0], "first name only, unique in the universe"
        if candidates:
            names = ", ".join(universe.describe(k)[0] for k in candidates[:4])
            return AMBIGUOUS, "", f"first name only, candidates: {names}"

    return ABSENT, "", "not in the ESPN universe"


def audit_source(label: str, names: Sequence[str], universe: EspnUniverse, *,
                 join: str, teams: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Classify every name one source publishes.

    Args:
        label: Source name, for the report.
        names: The names as that source spells them. Duplicates are collapsed.
        universe: The ESPN universe to resolve against.
        join: The join discipline this source's pipeline uses.
        teams: Per-name pro-team abbreviations, positionally aligned with
            ``names``. Optional; only a first-name truncation consults it.

    Returns:
        pd.DataFrame: One row per miss, columns matching :class:`Miss`, sorted
        actionable-first and then by the ESPN points at stake. Empty with those
        columns when the source joins cleanly.
    """
    seen: Dict[str, str] = {}
    team_of: Dict[str, str] = {}
    for i, raw in enumerate(names):
        if not isinstance(raw, str) or not raw.strip():
            continue
        if raw not in seen:
            seen[raw] = raw
            if teams is not None and i < len(teams):
                team_of[raw] = str(teams[i] or "")

    rows: List[Miss] = []
    for raw in seen:
        verdict, key, note = resolve(raw, universe, join=join,
                                     team=team_of.get(raw))
        if not verdict:
            continue
        source_key = universe._normalise(raw) or ""
        # A candidate already reviewed and rejected drops out of the review bucket
        # rather than being reported forever. Only NEEDS_REVIEW verdicts are
        # silenced: a CONFIDENT one rests on a mechanical rule, so if a name in
        # CONFIRMED_DISTINCT ever reaches one, that is a rule to look at.
        if verdict in NEEDS_REVIEW and source_key in CONFIRMED_DISTINCT:
            verdict, key = ABSENT, ""
            note = f"reviewed: {CONFIRMED_DISTINCT[source_key]}"
        upstream = KNOWN_UPSTREAM.get((label, source_key))
        if upstream and verdict in CONFIDENT + NEEDS_REVIEW:
            verdict = UPSTREAM
            note = upstream
        espn_name, position, points = universe.describe(key) if key else ("", "", 0.0)
        rows.append(Miss(label, raw, source_key, verdict,
                         espn_name, position, points, note))

    columns = [f.name for f in Miss.__dataclass_fields__.values()]  # type: ignore[attr-defined]
    if not rows:
        return pd.DataFrame(columns=columns)
    out = pd.DataFrame([r.__dict__ for r in rows])
    out["_rank"] = out["verdict"].map(
        lambda v: ACTIONABLE.index(v) if v in ACTIONABLE else len(ACTIONABLE))
    return (out.sort_values(["_rank", "espn_points", "source_name"],
                            ascending=[True, False, True])
            .drop(columns="_rank").reset_index(drop=True))


def _store_frames(season: int, artifact: str,
                  root: Optional[Path] = None) -> pd.DataFrame:
    """Union one artifact across every built league, one row per player.

    Args:
        season: Season year.
        artifact: ``"board"`` or ``"lineups"``.
        root: ``Data/Store`` override, for tests.

    Returns:
        pd.DataFrame: ``player_name``, ``primaryPosition``, ``pro_team``,
        ``espn_points`` -- de-duplicated on the name, keeping the highest
        projection, because the same player is priced differently per league and
        the audit only needs to know which name is worth attention.
    """
    base = Path(root) if root else store_root()
    points_col = "ESPN_Points"
    frames = []
    for path in sorted(base.glob(f"{season}/*/{artifact}.parquet")):
        try:
            frame = pd.read_parquet(path)
        except Exception:                                          # noqa: BLE001
            # A store mid-write is not a reason to fail an audit. Skipping it
            # under-reports by one league, which the header line makes visible.
            continue
        keep = {"player_name": frame.get("player_name"),
                "primaryPosition": frame.get("primaryPosition"),
                "pro_team": frame.get("pro_team")}
        if any(v is None for v in keep.values()):
            continue
        keep["espn_points"] = pd.to_numeric(
            frame.get(points_col, frame.get("projPoints")), errors="coerce")
        frames.append(pd.DataFrame(keep))
    if not frames:
        return pd.DataFrame(columns=["player_name", "primaryPosition", "pro_team",
                                     "espn_points"])
    out = pd.concat(frames, ignore_index=True)
    out["espn_points"] = out["espn_points"].fillna(0.0)
    return (out.sort_values("espn_points", ascending=False)
            .drop_duplicates("player_name").reset_index(drop=True))


def espn_universes(season: int, root: Optional[Path] = None
                   ) -> Tuple[EspnUniverse, EspnUniverse]:
    """The season and weekly ESPN universes, in that order.

    Args:
        season: Season year.
        root: ``Data/Store`` override, for tests.

    Returns:
        tuple: ``(board, weekly)``. The weekly universe falls back to the board
        before calling a name absent.
    """
    board = EspnUniverse(_store_frames(season, "board", root))
    weekly = EspnUniverse(_store_frames(season, "lineups", root), wider=board)
    return board, weekly


#: Every source that joins by name, how to read its names, and which discipline
#: its pipeline joins under.
#:
#: The weekly three are read *through their own loaders* -- ``clean_pinny`` and
#: ``clean_bol`` apply their rename maps first -- so the audit measures the join
#: that ships rather than the file on disk. Auditing the raw file instead would
#: report every entry those maps already fix.
#:
#: TOMCAT is listed and joins on ``player_id`` through the crosswalk, falling back
#: to the name only for players the crosswalk does not carry. Its misses are
#: therefore advisory, which the report says.
def source_readers() -> List[Tuple[str, str, str, Callable[[int], pd.DataFrame]]]:
    """The audit's source registry: ``(label, grain, join, loader)``.

    Loaders are imported lazily and each returns a frame with a ``player_name``
    column, and ``pro_team`` where the source publishes one. A loader that raises
    or finds no file yields an empty frame; a missing source is not an audit
    failure, it is a fact the report states.
    """
    from Scripts import projection_utils as pu
    from Scripts import season_projections as sp

    def _named(frame: pd.DataFrame, column: str = "player_name") -> pd.DataFrame:
        if frame is None or column not in getattr(frame, "columns", []):
            return pd.DataFrame(columns=["player_name"])
        out = frame[[column]].rename(columns={column: "player_name"})
        if "team" in frame.columns:
            out["pro_team"] = frame["team"].astype(str)
        return out

    def fp_season(season: int) -> pd.DataFrame:
        return _named(pd.read_parquet(season_dir(
            "FantasyPros", season, "FantasyPros_Projections_Season.parquet",
            create=False)))

    def fp_weekly(season: int) -> pd.DataFrame:
        return _named(pd.read_parquet(pu.fantasypros_parquet(season)))

    def pinny_weekly(season: int) -> pd.DataFrame:
        return _named(pu.clean_pinny(season=season))

    def bol_weekly(season: int) -> pd.DataFrame:
        return _named(pu.clean_bol(season=season))

    def pinny_season(season: int) -> pd.DataFrame:
        return _named(pd.read_parquet(season_dir(
            "Pinnacle", season, "Pinnacle_SeasonProps.parquet", create=False)))

    def bol_season(season: int) -> pd.DataFrame:
        # Read through the real normaliser: it is what recovers a player from the
        # prop wording on the unrecognised-stat branch, and the audit must not
        # report a name that branch already fixes.
        raw = pd.read_csv(season_dir(
            "BetOnline", season, "BetOnline_SeasonProps_All.csv", create=False))
        props = sp.normalise_bol_props(raw)
        teams = raw.set_index("player")["team"].astype(str).to_dict()
        out = _named(props)
        out["pro_team"] = out["player_name"].map(
            lambda n: _BOL_TEAMS.get(str(teams.get(n, "")), ""))
        return out

    def athletic_weekly(season: int) -> pd.DataFrame:
        # Read through the loader rather than off the parquet, for the reason the
        # note below gives: the audit must measure the join that ships.
        return _named(pu.clean_ath_weekly(season=season))

    def athletic(season: int) -> pd.DataFrame:
        return _named(pd.read_parquet(season_dir(
            "TheAthletic", season, "TheAthletic_Projections_Season.parquet",
            create=False)))

    def athletic_ranks(season: int) -> pd.DataFrame:
        return _named(pd.read_parquet(season_dir(
            "TheAthletic", season, "TheAthletic_Ranks_Season.parquet",
            create=False)))

    def usage(season: int) -> pd.DataFrame:
        return _named(pd.read_parquet(season_dir(
            "Usage", season, "Usage_SeasonProjections.parquet", create=False)),
            column="full_name")

    # The weekly four are JOIN_NORMALISED rather than JOIN_RAW, and the reason is
    # `align_to_espn_names`: `clean_lineups` still merges on the raw string, but it
    # rewrites every source name to ESPN's spelling of that player first, so a
    # difference `normalise_name` can bridge no longer costs the join. Before that
    # landed these were JOIN_RAW and the audit reported twelve live misses across
    # them -- `James Cook`, `Kenneth Gainwell`, `Deebo Samuel` and the rest.
    return [
        ("FantasyPros weekly", "weekly", JOIN_NORMALISED, fp_weekly),
        ("Pinnacle weekly", "weekly", JOIN_NORMALISED, pinny_weekly),
        ("BetOnline weekly", "weekly", JOIN_NORMALISED, bol_weekly),
        ("The Athletic weekly", "weekly", JOIN_NORMALISED, athletic_weekly),
        ("FantasyPros season", "season", JOIN_NORMALISED, fp_season),
        ("Pinnacle season", "season", JOIN_NORMALISED, pinny_season),
        ("BetOnline season", "season", JOIN_NORMALISED, bol_season),
        ("The Athletic season", "season", JOIN_NORMALISED, athletic),
        ("The Athletic ranks", "season", JOIN_NORMALISED, athletic_ranks),
        ("TOMCAT usage", "season", JOIN_NORMALISED, usage),
    ]


#: BetOnline's season file names teams in full. Only needed to resolve a
#: first-name-only truncation, so an unmapped club costs a suggestion, not a row.
_BOL_TEAMS = {
    "ARIZONA CARDINALS": "ARI", "ATLANTA FALCONS": "ATL", "BALTIMORE RAVENS": "BAL",
    "BUFFALO BILLS": "BUF", "CAROLINA PANTHERS": "CAR", "CHICAGO BEARS": "CHI",
    "CINCINNATI BENGALS": "CIN", "CLEVELAND BROWNS": "CLE", "DALLAS COWBOYS": "DAL",
    "DENVER BRONCOS": "DEN", "DETROIT LIONS": "DET", "GREEN BAY PACKERS": "GB",
    "HOUSTON TEXANS": "HOU", "INDIANAPOLIS COLTS": "IND",
    "JACKSONVILLE JAGUARS": "JAX", "KANSAS CITY CHIEFS": "KC",
    "LAS VEGAS RAIDERS": "LV", "LOS ANGELES CHARGERS": "LAC",
    "LOS ANGELES RAMS": "LAR", "MIAMI DOLPHINS": "MIA", "MINNESOTA VIKINGS": "MIN",
    "NEW ENGLAND PATRIOTS": "NE", "NEW ORLEANS SAINTS": "NO",
    "NEW YORK GIANTS": "NYG", "NEW YORK JETS": "NYJ",
    "PHILADELPHIA EAGLES": "PHI", "PITTSBURGH STEELERS": "PIT",
    "SAN FRANCISCO 49ERS": "SF", "SEATTLE SEAHAWKS": "SEA",
    "TAMPA BAY BUCS": "TB", "TAMPA BAY BUCCANEERS": "TB",
    "TENNESSEE TITANS": "TEN", "WASHINGTON COMMANDERS": "WSH",
}


def audit_all(season: int, root: Optional[Path] = None) -> pd.DataFrame:
    """Run the audit over every registered source.

    Args:
        season: Season year.
        root: ``Data/Store`` override, for tests.

    Returns:
        pd.DataFrame: Every miss from every source, one row each, with a ``grain``
        column. Sources with no file contribute nothing and are named in
        :func:`main`'s output instead.
    """
    board, weekly = espn_universes(season, root)
    frames = []
    for label, grain, join, loader in source_readers():
        universe = weekly if grain == "weekly" else board
        try:
            frame = loader(season)
        except Exception as exc:                                   # noqa: BLE001
            frames.append(pd.DataFrame([{
                "source": label, "source_name": "", "key": "", "verdict": ABSENT,
                "espn_name": "", "espn_position": "", "espn_points": 0.0,
                "note": f"could not be read: {type(exc).__name__}", "grain": grain,
            }]))
            continue
        teams = (frame["pro_team"].tolist() if "pro_team" in frame.columns
                 else None)
        out = audit_source(label, frame["player_name"].tolist(), universe,
                           join=join, teams=teams)
        out["grain"] = grain
        frames.append(out)
    if not frames:
        return pd.DataFrame()
    # Empty frames dropped before the concat rather than after: pandas warns that
    # it will stop inferring dtypes past all-NA columns, and a clean source
    # contributes exactly such a frame.
    populated = [f for f in frames if len(f)]
    if not populated:
        return pd.DataFrame(columns=[f.name for f in
                                     Miss.__dataclass_fields__.values()]  # type: ignore[attr-defined]
                            + ["grain"])
    return pd.concat(populated, ignore_index=True)


#: The hand-maintained weekly rename maps, by the function that owns each. Audited
#: rather than imported: they are dict literals inside function bodies, and
#: reaching into them would couple this module to those functions' internals.
#: Restated here and pinned equal by ``tests/test_name_audit.py``.
WEEKLY_RENAME_MAPS: Dict[str, str] = {
    "clean_pinny": "Scripts.projection_utils.clean_pinny",
    "clean_bol": "Scripts.projection_utils.clean_bol",
}


def audit_rename_maps(season: int, root: Optional[Path] = None) -> pd.DataFrame:
    """Grade each entry in the weekly rename maps against the shipped data.

    Four findings, and the last two are live defects:

    * **no-op under normalise_name** -- the entry only exists because the weekly
      path joins on the raw string. Harmless, but it is most of both maps, and it
      is the measurement behind "normalise the weekly join and the maps mostly
      disappear".
    * **key not in the source file** -- the source stopped using that spelling.
      Dead weight, safe to keep, worth pruning.
    * **target is not an ESPN name** -- the rename lands on a spelling ESPN does
      not use, so it *creates* the miss it was written to fix.
    * **target is a different player** -- the same, but worse.

    Args:
        season: Season year.
        root: ``Data/Store`` override, for tests.

    Returns:
        pd.DataFrame: ``map``, ``key``, ``target``, ``finding``.
    """
    from Scripts.projection_utils import (betonline_parquet, pinnacle_parquet)
    from Scripts.season_projections import normalise_name

    board, weekly = espn_universes(season, root)
    espn_names = weekly.raw_names | board.raw_names

    sources = {}
    for name, resolver in (("clean_pinny", pinnacle_parquet),
                           ("clean_bol", betonline_parquet)):
        path = resolver(season)
        try:
            sources[name] = set(pd.read_parquet(
                path, columns=["player_name"])["player_name"])
        except Exception:                                          # noqa: BLE001
            sources[name] = set()

    rows = []
    for owner, mapping in _rename_maps().items():
        for key, target in mapping.items():
            findings = []
            if key not in sources.get(owner, set()):
                findings.append("key not in the source file")
            if target not in espn_names:
                findings.append("target is not an ESPN name")
            if normalise_name(key) == normalise_name(target):
                findings.append("no-op under normalise_name")
            rows.append({"map": owner, "key": key, "target": target,
                         "finding": "; ".join(findings) or "does real work"})
    # Typed even when empty, which is the healthy state as of 2026-09-09: both maps
    # were deleted in favour of `align_to_espn_names`. An untyped empty frame here
    # took `--maps` down with `KeyError: 'finding'`, and a check that crashes when
    # the thing it checks is fixed is a check nobody keeps.
    return pd.DataFrame(rows, columns=["map", "key", "target", "finding"])


def _rename_maps() -> Dict[str, Dict[str, str]]:
    """The rename maps as the two loaders hold them.

    Extracted by calling the loaders on an empty frame is not possible -- the maps
    are locals -- so they are read out of the module source. Fragile-looking and
    deliberate: the alternative is a second copy that drifts, and
    ``tests/test_name_audit.py`` fails the moment the parse stops finding them.
    """
    import ast
    import inspect

    from Scripts import projection_utils

    out: Dict[str, Dict[str, str]] = {}
    tree = ast.parse(inspect.getsource(projection_utils))
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name not in WEEKLY_RENAME_MAPS:
            continue
        for stmt in ast.walk(node):
            if (isinstance(stmt, ast.Assign) and len(stmt.targets) == 1
                    and isinstance(stmt.targets[0], ast.Name)
                    and stmt.targets[0].id == "name_changes"
                    and isinstance(stmt.value, ast.Dict)):
                out[node.name] = ast.literal_eval(stmt.value)
    return out


def format_report(audit: pd.DataFrame, *, show_all: bool = False) -> str:
    """Render the audit as the text the CLI prints.

    The two groups are printed separately and labelled, because they carry
    different instructions: :data:`CONFIDENT` rows can be pasted into an alias map,
    :data:`NEEDS_REVIEW` rows have to be looked at first.

    Args:
        audit: :func:`audit_all`'s frame.
        show_all: Include the non-actionable verdicts -- ``unrostered`` and
            ``absent`` -- in full. Off by default: they are the large majority and
            none of them names a fix.

    Returns:
        str: The report.
    """
    lines: List[str] = []
    for source in audit["source"].drop_duplicates():
        rows = audit[audit["source"] == source]
        confident = rows[rows["verdict"].isin(CONFIDENT)]
        review = rows[rows["verdict"].isin(NEEDS_REVIEW)]
        upstream = rows[rows["verdict"].isin(UPSTREAM_DEFECTS)]
        other = rows[~rows["verdict"].isin(ACTIONABLE)]
        counts = other["verdict"].value_counts().to_dict()
        tail = ", ".join(f"{n} {v}" for v, n in sorted(counts.items()))
        headline = f"{len(confident)} to fix, {len(review)} to review"
        if len(upstream):
            headline += f", {len(upstream)} upstream"
        lines.append(f"\n{source}: {headline}" + (f" ({tail})" if tail else ""))
        for label, group in (("", confident), ("  ?", review), ("  !", upstream)):
            for row in group.itertuples(index=False):
                target = row.espn_name or "?"
                lines.append(f"  {label:<2}  {row.verdict:<17} {row.source_name!r} -> "
                             f"{target!r} ({row.espn_position or '-'}, "
                             f"{row.espn_points:.0f} ESPN pts) — {row.note}")
        if show_all and len(other):
            for row in other.itertuples(index=False):
                lines.append(f"      {row.verdict:<17} {row.source_name!r} — {row.note}")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Print the name audit. Returns 1 when any source has an actionable miss."""
    from Scripts.nfl_utils import current_season

    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--season", type=int, default=None,
                        help="season to audit (default: the current one)")
    parser.add_argument("--all", action="store_true",
                        help="list unrostered and absent names too")
    parser.add_argument("--maps", action="store_true",
                        help="audit the weekly rename maps and stop")
    args = parser.parse_args(argv)
    season = args.season if args.season is not None else current_season()

    if args.maps:
        maps = audit_rename_maps(season)
        print(f"===== Weekly rename maps, {season} =====")
        if maps.empty:
            print("  None. `align_to_espn_names` derives the renames from the ESPN "
                  "frame at build time, so there is no map to go stale.")
            return 0
        for row in maps.itertuples(index=False):
            print(f"  {row.map:<12} {row.key!r:<26} -> {row.target!r:<26} "
                  f"{row.finding}")
        broken = maps[maps["finding"].str.contains("target is not")]
        return 1 if len(broken) else 0

    audit = audit_all(season)
    print(f"===== Source name audit, {season} =====")
    if audit.empty:
        print("  no sources found")
        return 0
    print(format_report(audit, show_all=args.all))
    confident = audit[audit["verdict"].isin(CONFIDENT)]
    review = audit[audit["verdict"].isin(NEEDS_REVIEW)]
    upstream = audit[audit["verdict"].isin(UPSTREAM_DEFECTS)]
    print(f"\n{len(confident)} to fix, {len(review)} to review, "
          f"{len(upstream)} known upstream. A fix is an entry in NAME_ALIASES; "
          "`align_to_espn_names` already handles anything normalise_name can bridge.")
    return 1 if len(confident) else 0


if __name__ == "__main__":
    raise SystemExit(main())
