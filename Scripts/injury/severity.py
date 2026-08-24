"""What is wrong with a player, and how long it keeps him out.

The live half of the package. :mod:`Scripts.injury.episodes` looks backwards and can use
*observed* duration as its severity signal; a projection has to look forwards, where
duration is the thing being predicted. So this module turns whatever evidence exists on a
given day into an expected absence and a duration bucket, and the bucket is what keys the
fitted recovery curve.

**The evidence is thin, and the ladder is built around admitting that.** Measured on the
2026-08-18 ESPN pull: 800 records, of which **114 carry a structured ``injury_type``** and
``"Undisclosed"`` is the second most common value in that field. ``report_primary_injury``
from nflverse says ``"Ankle"`` and never ``"high ankle"``, for all ten seasons. Exactly
**one** of 800 comments anywhere contains the phrase "high ankle".

Jeremiyah Love is the case this was written against: status ``Active``, no
``returnDate``, no ``injury_type``, and the only trace of a high ankle sprain is the
parenthetical "(ankle)" in a news comment. Every automatic channel either misses him or
reads him as a generic ankle, which is why the top of the ladder is a human.

Precedence, most trusted first:

1. a **user override** -- someone who read the beat report knows more than any of this;
2. ESPN's **structured** ``injury_type`` / ``injury_detail`` -- low coverage, high value:
   ``"Knee - ACL"`` is a real diagnosis;
3. ESPN's **estimated return date** -- ESPN's own arithmetic, and it beats a prior;
4. the news **comment**, through an auditable phrase list;
5. nflverse's weekly **report** body part, in season;
6. **nothing** -- abstain, and say so.

Abstention is a first-class outcome. :data:`Severity.source` of ``"none"`` carries a
multiplier of exactly 1.0 and a non-empty evidence string, so a reader can always tell
"the model looked and found no effect" from "the model could not look".

Polars and plain dicts; no frame is required to ask about one player.
"""

from __future__ import annotations

import datetime
import re
import warnings
from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple

from Scripts import paths
from Scripts.injury import lexicon

#: Weeks a group keeps a player out, absent anything more specific.
#:
#: Measured from :mod:`Scripts.injury.episodes` over 2016-2025, returned episodes only --
#: the mean ``weeks_out`` per body-part group. Committed as a literal so that resolving a
#: severity never depends on the episode table having been built, and refreshed from
#: ``Data/NFL/injury_meta.json`` when it is available. The ranges are +/- roughly one
#: standard deviation, rounded, because a beat report supports a range and a point
#: estimate would be false precision.
GROUP_PRIORS: Dict[str, Tuple[float, float]] = {
    "ankle": (1.0, 5.0),
    "knee": (1.0, 7.0),
    "hamstring": (1.0, 5.0),
    "concussion": (1.0, 3.0),
    "soft_tissue_lower": (1.0, 6.0),
    "foot_toe": (1.0, 6.0),
    "shoulder": (1.0, 5.0),
    "hand_wrist_arm": (1.0, 7.0),
    "back_core": (1.0, 6.0),
    "ribs_chest": (1.0, 5.0),
    "illness": (1.0, 7.0),
    "other": (1.0, 8.0),
}

#: ESPN ``injury_type`` structures, which arrive as ``"<part> - <structure>"``.
#:
#: The one place in the whole pipeline where a real diagnosis is available, and it is
#: worth special-casing for that reason: an ACL is a season, an MCL is a month, and
#: ``report_primary_injury`` calls both of them ``"Knee"``.
STRUCTURE_WEEKS: Dict[str, Tuple[float, float]] = {
    "acl": (40.0, 52.0),
    "acl + mcl": (44.0, 52.0),
    "pcl": (4.0, 12.0),
    "mcl": (3.0, 8.0),
    "lcl": (4.0, 10.0),
    "meniscus": (3.0, 10.0),
    "achilles": (40.0, 52.0),
}

#: ``injury_detail`` values that carry a severity signal, and how they scale a prior.
#:
#: Multiplicative on the group prior rather than absolute, because "a fracture" spans a
#: fortnight and a season depending on which bone.
DETAIL_SCALE: Dict[str, float] = {
    "surgery": 3.0,
    "fracture": 2.0,
    "tear": 2.5,
    "sprain": 1.0,
    "strain": 1.0,
    "soreness": 0.4,
    "stinger": 0.3,
    "concussion": 0.7,
    "not specified": 1.0,
}

#: How a generic severity word found in free text scales a group prior.
MODIFIER_SCALE: Dict[str, float] = {
    "season_ending": 99.0,
    "surgery": 3.0,
    "tear": 2.5,
    "fracture": 2.0,
    "grade_3": 2.5,
    "grade_2": 1.5,
    "grade_1": 0.6,
    "multi_week": 1.3,
    "sprain": 1.0,
    "strain": 1.0,
    "minor": 0.4,
}

#: Weeks out at or beyond which a player is treated as gone for the season.
SEASON_ENDING_WEEKS = 18.0

#: An override with no corroborating feed evidence goes stale after this long.
STALE_OVERRIDE_DAYS = 28

#: Fields an override entry may carry.
_OVERRIDE_FIELDS = frozenset({
    "espn_id", "name_key", "player", "body_part", "weeks_out", "as_of", "source",
    "note", "multiplier_ladder", "detail",
})

_REQUIRED_OVERRIDE_FIELDS = ("body_part", "weeks_out", "as_of", "source")

_COMPILED_PHRASES = [(re.compile(pattern, re.IGNORECASE), value)
                     for pattern, value in lexicon.SEVERITY_PHRASES]
_COMPILED_MODIFIERS = [(re.compile(pattern, re.IGNORECASE), value)
                       for pattern, value in lexicon.SEVERITY_MODIFIERS]


class Severity(NamedTuple):
    """What is known about one player's injury, and how confident we are.

    Attributes:
        body_part: A :data:`Scripts.injury.lexicon.GROUPS` value.
        detail: The specific structure or severity word, when one was found.
        weeks_low: Low end of the expected absence, in games.
        weeks_high: High end.
        weeks_expected: Midpoint, and what channel A subtracts.
        duration_bucket: The key into the fitted recovery curve.
        source: Which rung of the ladder answered.
        confidence: ``high``, ``medium`` or ``low``.
        ladder: A hand-supplied multiplier ramp, when an override gave one.
        evidence: One readable sentence. Never empty.
    """

    body_part: str
    detail: Optional[str]
    weeks_low: float
    weeks_high: float
    weeks_expected: float
    duration_bucket: str
    source: str
    confidence: str
    ladder: Optional[List[float]]
    evidence: str

    @property
    def season_ending(self) -> bool:
        """Whether this injury should be read as out for the year."""
        return self.weeks_expected >= SEASON_ENDING_WEEKS

    @property
    def abstained(self) -> bool:
        """Whether nothing was known. A multiplier of 1.0, with a reason."""
        return self.source == "none"


#: How close a severity phrase must sit to the player's own surname to count as his.
#:
#: Beat reports routinely describe one player's injury in another player's blurb, and the
#: extractor has no idea whose body it is reading about. Measured case: Tyler Allgeier's
#: comment reads "Allgeier could open the regular season as the Cardinals' primary running
#: back, as Adam Schefter reports that Jeremiyah Love sustained a high-ankle sprain" --
#: which tagged **Allgeier** with Love's high ankle sprain, on the same board where Love
#: carries it correctly from an override.
#:
#: So a phrase has to be attributable, and there are only two ways it can be: the
#: parenthetical convention ("Metcalf (undisclosed) will be...") anchors on the subject's
#: own surname, or the phrase sits close enough behind the surname to be about him. Beyond
#: that window the extractor abstains rather than guessing whose knee it is.
ATTRIBUTION_WINDOW = 60

#: Weeks a player ESPN still calls available is expected to miss.
#:
#: Not zero, because he is on the injury report for a reason and camp tweaks do cost the
#: odd game; not the group prior either, and that distinction is the fix. See
#: :func:`reads_as_active`.
ACTIVE_ABSENCE = (0.0, 1.0)


def reads_as_active(record: Dict) -> bool:
    """Whether ESPN's own status says this player is available.

    Load-bearing for duration, not just for display. The group priors in
    :data:`GROUP_PRIORS` answer "given a player is **out** with this body part, how long
    for?" -- they are means over episodes that cost at least one game. A player merely
    *mentioned* with a body part is a different population: on the 2026-08-18 pull, 686
    of 800 records are Active, and their comments are mostly "returned to practice".

    Applied without this check the comment rung put Puka Nacua at 3.5 expected weeks
    missed from a note saying he had practised, at ADP 4.4. A board that says that about
    its fourth-best player is worse than one that says nothing.

    Args:
        record: One injury-report row.

    Returns:
        bool: True when ``status`` is in
        :data:`Scripts.scrape_espn_injuries.ACTIVE_STATUSES`.
    """
    from Scripts.scrape_espn_injuries import ACTIVE_STATUSES

    status = record.get("status")
    return bool(status) and str(status) in ACTIVE_STATUSES


def abstain(reason: str) -> Severity:
    """The sixth rung: nothing is known, and the caller must be told so.

    Args:
        reason: Why there is no severity.

    Returns:
        Severity: ``source="none"``, zero expected absence, and a non-empty evidence
        string -- the flag, in a field that arithmetic cannot poison.
    """
    return Severity(body_part="other", detail=None, weeks_low=0.0, weeks_high=0.0,
                    weeks_expected=0.0, duration_bucket="1", source="none",
                    confidence="low", ladder=None,
                    evidence=f"abstain: {reason}")


# --- the override file ----------------------------------------------------

def overrides_path(season: int, create: bool = False):
    """Where the hand-maintained severity file lives.

    Args:
        season: Season year.
        create: Create the directory.

    Returns:
        Path: ``config/injuries/<season>.yaml``.
    """
    directory = paths.INJURY_OVERRIDES_DIR
    if create:
        directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{season}.yaml"


def load_overrides(season: int) -> Dict[str, Dict]:
    """Read the override file, validating loudly.

    **The file is tracked in git**, unlike every other injury artifact. It moved out of
    ``Data/`` for that reason: everything there is untracked because S3 is the system of
    record for *data*, and this is not data. It is a set of judgements a human made from a
    beat report, each stamped with an ``as_of`` and a ``source``, and version history is the
    useful thing about it -- what was believed, and when. A severity written in August that
    looks wrong in November is a question you want ``git log`` to answer.

    One file per season, so a rollover leaves last year's reasoning readable rather than
    overwriting it.

    Validation is loud on purpose. An override exists because a human noticed something
    the feeds do not carry, so an override that silently does nothing is the worst
    possible outcome -- worse than not having the file, because the reader believes the
    correction landed.

    Args:
        season: Season year.

    Returns:
        dict: Lookup key -> entry. Every entry appears under its ``espn_id`` (as a
        string) and its ``name_key``, so either join works. Empty when there is no file.

    Raises:
        ValueError: On an unknown body part, an unknown field, a missing required field,
            or a malformed ``weeks_out``.
    """
    import yaml

    path = overrides_path(season)
    if not path.is_file():
        return {}

    with open(path) as handle:
        document = yaml.safe_load(handle) or {}
    entries = document.get("players") or []
    if not isinstance(entries, list):
        raise ValueError(
            f"{path}: 'players' must be a list, got {type(entries).__name__}.")

    lookup: Dict[str, Dict] = {}
    for index, entry in enumerate(entries):
        where = f"{path} entry {index + 1}"
        if not isinstance(entry, dict):
            raise ValueError(f"{where}: expected a mapping, got "
                             f"{type(entry).__name__}.")

        unknown = set(entry) - _OVERRIDE_FIELDS
        if unknown:
            raise ValueError(
                f"{where}: unknown field(s) {sorted(unknown)}. Known fields are "
                f"{sorted(_OVERRIDE_FIELDS)}.")
        missing = [f for f in _REQUIRED_OVERRIDE_FIELDS if entry.get(f) is None]
        if missing:
            raise ValueError(
                f"{where}: missing required field(s) {missing}. An override with no "
                f"'as_of' and 'source' is a guess that outlives the reason for it.")

        part = str(entry["body_part"]).strip().lower()
        group = lexicon.group(part)
        if group == "other" and part not in ("other", "undisclosed"):
            raise ValueError(
                f"{where}: unknown body_part {entry['body_part']!r}. It must map to one "
                f"of {list(lexicon.GROUPS)} -- a typo here would silently do nothing. "
                f"Use 'other' if the part is genuinely unknown.")

        low, high = _parse_weeks(entry["weeks_out"], where)
        record = dict(entry)
        record.update({"_group": group, "_part": part, "_low": low, "_high": high,
                       "_where": where})

        if entry.get("espn_id") is not None:
            lookup[str(entry["espn_id"])] = record
        if entry.get("name_key"):
            lookup[str(entry["name_key"]).strip().upper()] = record
        if entry.get("espn_id") is None and not entry.get("name_key"):
            raise ValueError(
                f"{where}: needs an 'espn_id' or a 'name_key' to join on.")
    return lookup


def _parse_weeks(value, where: str) -> Tuple[float, float]:
    """Read ``weeks_out`` as a range, however it was written."""
    if isinstance(value, (int, float)):
        return float(value), float(value)
    if isinstance(value, (list, tuple)):
        if len(value) != 2:
            raise ValueError(
                f"{where}: weeks_out must be a number or a two-item [low, high] "
                f"range, got {value!r}.")
        low, high = float(value[0]), float(value[1])
        if low > high:
            raise ValueError(f"{where}: weeks_out low {low} exceeds high {high}.")
        return low, high
    raise ValueError(
        f"{where}: weeks_out must be a number or a [low, high] range, got "
        f"{type(value).__name__}.")


def check_overrides(lookup: Dict[str, Dict], known_keys: Optional[Sequence[str]] = None,
                    today: Optional[datetime.date] = None) -> List[str]:
    """Warn about overrides that will not do what their author intended.

    Two failure modes, both silent without this:

    *A name that matches nobody.* The ESPN report joins on a normalised name and is the
    most fragile join in the repo -- suffixes are the usual culprit -- so an override
    keyed on a misspelling simply never fires.

    *An override that has rotted.* A severity written in August is a description of
    August. Left in place it keeps discounting a player who has been healthy for a
    month.

    Args:
        lookup: :func:`load_overrides` output.
        known_keys: Every ``espn_id`` and ``name_key`` in the player universe. None
            skips the match check.
        today: Date to age against. Defaults to today.

    Returns:
        list: Warning strings, also emitted through :mod:`warnings`.
    """
    today = today or datetime.date.today()
    messages: List[str] = []
    seen = set()

    for key, entry in lookup.items():
        identity = entry["_where"]
        if identity in seen:
            continue
        seen.add(identity)

        if known_keys is not None:
            keys = {str(entry.get("espn_id")), str(entry.get("name_key") or "").upper()}
            if not (keys & set(known_keys)):
                messages.append(
                    f"{identity}: matches no player in the universe "
                    f"({entry.get('name_key') or entry.get('espn_id')}). Check the "
                    f"spelling -- an override that matches nobody does nothing.")

        as_of = entry.get("as_of")
        if isinstance(as_of, datetime.datetime):
            as_of = as_of.date()
        if isinstance(as_of, datetime.date):
            age = (today - as_of).days
            if age > STALE_OVERRIDE_DAYS:
                messages.append(
                    f"{identity}: as_of is {age} days old "
                    f"({as_of}). Re-read the beat report or delete the entry.")

    for message in messages:
        warnings.warn(message, stacklevel=2)
    return messages


# --- the ladder -----------------------------------------------------------

def resolve(record: Optional[Dict] = None,
            overrides: Optional[Dict[str, Dict]] = None,
            week_one: Optional[datetime.date] = None,
            report_body_part: Optional[str] = None,
            season_ending_after: Optional[datetime.date] = None) -> Severity:
    """Read every available channel and return the most trusted answer.

    Args:
        record: One row of ``Data/Injuries/<season>/espn_injuries.parquet`` as a dict --
            ``status``, ``return_date``, ``injury_type``, ``injury_detail``,
            ``comment``, ``name_key``, and ``espn_id`` when the caller has attached one.
        overrides: :func:`load_overrides` output.
        week_one: First gameday, for turning a return date into weeks. When None, the
            return-date rung is skipped rather than guessed at.
        report_body_part: nflverse ``report_primary_injury``, in season.
        season_ending_after: ESPN's injured-reserve sentinel boundary. Defaults to
            :data:`Scripts.scrape_espn_injuries.SEASON_ENDING_AFTER`.

    Returns:
        Severity: Never None. :func:`abstain` when nothing is known.
    """
    record = record or {}
    overrides = overrides or {}

    found = _from_override(record, overrides)
    if found is not None:
        return found
    # The return date comes **before** the diagnosis, and Malik Nabers is why. ESPN had
    # him ``injury_type="Knee - ACL"``, ``injury_detail="Surgery"``, status
    # ``Questionable`` -- and ``returnDate`` 2026-08-15, four weeks before the opener,
    # with a comment describing 11-on-11 reps in a non-contact jersey. The ACL is real and
    # it is *last season's*. Read diagnosis-first he was 46 weeks out at ADP 36; read
    # date-first he is available in week 1 with an ACL in his history, which is what is
    # true.
    #
    # The general rule that falls out of it: **a diagnosis names an injury and a return
    # date times it.** The label is the more durable fact and the date is the more current
    # one, so the date sets the duration and the diagnosis still supplies the body part.
    found = _from_return_date(record, week_one, season_ending_after)
    if found is not None:
        return found
    found = _from_structured(record)
    if found is not None:
        return found
    found = _from_comment(record)
    if found is not None:
        return found
    found = _from_report(report_body_part)
    if found is not None:
        return found
    return abstain("no severity evidence")


def _bucket(weeks: float) -> str:
    """The duration bucket a weeks-out figure lands in.

    Shares its cut points with :func:`Scripts.injury.episodes.duration_bucket`, which is
    what makes a live lookup hit the cell the historical fit populated.
    """
    if weeks <= 1:
        return "1"
    if weeks <= 2:
        return "2"
    if weeks <= 4:
        return "3-4"
    return "5+"


def _make(group: str, detail: Optional[str], low: float, high: float, source: str,
          confidence: str, evidence: str,
          ladder: Optional[List[float]] = None) -> Severity:
    low = max(float(low), 0.0)
    high = max(float(high), low)
    expected = (low + high) / 2.0
    return Severity(body_part=group, detail=detail, weeks_low=low, weeks_high=high,
                    weeks_expected=expected, duration_bucket=_bucket(expected),
                    source=source, confidence=confidence, ladder=ladder,
                    evidence=evidence)


def _from_override(record: Dict, overrides: Dict[str, Dict]) -> Optional[Severity]:
    """Rung 1. Keyed on ``espn_id`` first, because the name join is the fragile one."""
    if not overrides:
        return None
    entry = None
    espn_id = record.get("espn_id")
    if espn_id is not None:
        entry = overrides.get(str(espn_id))
    if entry is None and record.get("name_key"):
        entry = overrides.get(str(record["name_key"]).strip().upper())
    if entry is None:
        return None

    ladder = entry.get("multiplier_ladder")
    note = entry.get("note") or entry.get("source")
    return _make(entry["_group"], entry["_part"], entry["_low"], entry["_high"],
                 source="override", confidence="high",
                 evidence=f"override ({entry.get('source')}): {note}",
                 ladder=[float(x) for x in ladder] if ladder else None)


def _from_structured(record: Dict) -> Optional[Severity]:
    """Rung 2. ESPN's own diagnosis, where it has one.

    Only 114 of 800 records carry ``injury_type`` and ``"Undisclosed"`` is common, so
    this rung fires seldom and is worth a lot when it does: ``"Knee - ACL"`` is a season
    and ``"Knee - MCL"`` is a month, and every other channel calls both of them a knee.
    """
    raw = record.get("injury_type")
    if not raw:
        return None
    text = str(raw).strip()
    if text.lower() in ("undisclosed", "not disclosed", "suspension", ""):
        return None

    base, _, structure = text.partition(" - ")
    group = lexicon.group(base)
    structure = structure.strip().lower() or None

    if structure and structure in STRUCTURE_WEEKS:
        low, high = STRUCTURE_WEEKS[structure]
        return _make(group, structure, low, high, source="espn_structured",
                     confidence="high",
                     evidence=f"ESPN diagnosis: {text}")

    detail = (record.get("injury_detail") or "").strip().lower()
    low, high = GROUP_PRIORS.get(group, GROUP_PRIORS["other"])
    if detail and detail != "not specified":
        scale = DETAIL_SCALE.get(detail, 1.0)
        return _make(group, detail, low * scale, high * scale,
                     source="espn_structured", confidence="medium",
                     evidence=f"ESPN diagnosis: {text} ({detail})")

    # A bare body part with no structure and ``Not Specified`` detail -- 72 of the 114
    # structured records -- carries no duration information at all, so it must **not**
    # outrank the return date below it. ESPN naming the joint and ESPN estimating when he
    # is back are two different facts, and the estimate is the one with a number in it.
    # Falling through costs nothing: the return-date rung reads ``injury_type`` for the
    # body part itself, so the label survives either way.
    return None


def _from_return_date(record: Dict, week_one: Optional[datetime.date],
                      season_ending_after: Optional[datetime.date]
                      ) -> Optional[Severity]:
    """Rung 3. ESPN's own estimate, which beats any prior this package could form.

    The sentinel has to be separated from an estimate first. ESPN stamps injured reserve
    with a date past the end of the schedule, and read literally it means "returns in
    week 23" -- the trap :data:`Scripts.scrape_espn_injuries.SEASON_ENDING_AFTER` exists
    for.
    """
    raw = record.get("return_date")
    if raw is None or week_one is None:
        return None
    if season_ending_after is None:
        from Scripts.scrape_espn_injuries import SEASON_ENDING_AFTER
        season_ending_after = SEASON_ENDING_AFTER

    date = raw.date() if hasattr(raw, "date") else raw
    if not isinstance(date, datetime.date):
        return None

    raw_type = record.get("injury_type") or ""
    base, _, structure = str(raw_type).partition(" - ")
    group = lexicon.group(base)
    detail = structure.strip().lower() or None

    if date > season_ending_after:
        return _make(group, detail or "season_ending", SEASON_ENDING_WEEKS,
                     SEASON_ENDING_WEEKS, source="return_date", confidence="high",
                     evidence=f"ESPN return date {date} is past the schedule -- "
                              f"out for the season")

    weeks = max((date - week_one).days / 7.0, 0.0)
    when = "already back" if weeks <= 0 else f"a return around {date}"
    return _make(group, detail, weeks, weeks, source="return_date", confidence="high",
                 evidence=f"ESPN estimates {when}"
                          + (f"; diagnosis {raw_type}" if raw_type else ""))


def _from_comment(record: Dict) -> Optional[Severity]:
    r"""Rung 4. The news text, through a list you can read.

    The highest-coverage channel -- every one of 800 records carries a comment -- and the
    weakest by a distance. Deliberately a flat ordered list of regexes with no fuzzy
    matching and no scoring, because a lexicon you can read is a lexicon you can correct,
    and because this rung's job on most days is to abstain.

    Three things it has to get right, each of which it got wrong first:

    *Whose injury is it.* See :data:`ATTRIBUTION_WINDOW`.

    *Is it current.* Text is undated and ESPN's status is not. "Pacheco is dealing with a
    sprained MCL, but head coach Dan Campbell believes he will be ready for the season
    opener" contains a real diagnosis and describes a player who will not miss a game, so
    the diagnosis sets the label and the status sets the duration.

    *What to call it.* The detail is the matched text, not the pattern that matched --
    "high ankle", not ``high[\s-]*ankle``.

    A body part alone is enough to fire. Love's comment says only "(ankle)" and that beats
    nothing -- it just resolves to the generic ankle prior rather than to a high ankle
    sprain, which is the gap the override file fills.
    """
    text = record.get("comment")
    if not text:
        return None
    text = str(text)
    surname = _surname(record.get("full_name"))

    active = reads_as_active(record)
    active_note = " -- ESPN still lists him active" if active else ""

    for pattern, (group, (low, high)) in _COMPILED_PHRASES:
        match = pattern.search(text)
        if match is None:
            continue
        if not _attributable(text, match.start(), surname):
            continue
        named = re.sub(r"\s+", " ", match.group(0)).strip().lower().replace("-", " ")
        if active:
            return _make(group, named, *ACTIVE_ABSENCE, source="comment",
                         confidence="low",
                         evidence=f"news text names {named}{active_note}")
        return _make(group, named, low, high, source="comment", confidence="medium",
                     evidence=f"news text names a specific injury: {named}")

    part, position = _body_part_in_text(text, surname)
    if part is None:
        return None
    group = lexicon.group(part)
    low, high = GROUP_PRIORS.get(group, GROUP_PRIORS["other"])

    # ESPN calling him Active is a statement about *this week*; the prior is a statement
    # about players who missed a game. The group priors answer "given he is out with this
    # body part, how long?" -- means over episodes costing at least one game -- and a
    # player merely mentioned with a body part is a different population. Applied without
    # this check the rung put Puka Nacua at 3.5 expected weeks missed from a note saying he
    # had practised, at ADP 4.4.
    if active:
        low, high = ACTIVE_ABSENCE

    for pattern, modifier in _COMPILED_MODIFIERS:
        match = pattern.search(text)
        if match is None or not _attributable(text, match.start(), surname,
                                              anchor=position):
            continue
        if modifier == "season_ending":
            # Beats a stale Active status: a report saying he is done for the year is
            # newer information than a status field that has not caught up.
            return _make(group, f"{part}, season ending", SEASON_ENDING_WEEKS,
                         SEASON_ENDING_WEEKS, source="comment", confidence="medium",
                         evidence=f"news text: {part}, out for the season")
        scale = MODIFIER_SCALE.get(modifier, 1.0)
        # ``detail`` names the part with the modifier attached rather than the modifier
        # alone: "multi week" in a Body Part column is not a body part, and Egbuka's
        # "(toe) is day-to-day, week-to-week" rendered as exactly that.
        return _make(group, f"{part}, {modifier.replace('_', ' ')}",
                     low * scale, high * scale, source="comment", confidence="low",
                     evidence=f"news text: {part}, "
                              f"{modifier.replace('_', ' ')}{active_note}")

    # Keep the raw parenthetical when the group is the catch-all. ``other`` covers "leg",
    # "undisclosed" and a kidney, and a board cell reading "other" tells a drafter strictly
    # less than the word the beat writer used.
    return _make(group, part if group == "other" else None, low, high, source="comment",
                 confidence="low",
                 evidence=f"news text mentions {part} and nothing about "
                          f"severity{active_note}")


def _surname(full_name: Optional[str]) -> Optional[str]:
    """The last word of a name, minus a generational suffix.

    Args:
        full_name: e.g. ``"Michael Pittman Jr."``.

    Returns:
        str | None: ``"pittman"``, lowercased.
    """
    if not full_name:
        return None
    parts = [w.strip(".,").lower() for w in str(full_name).split()
             if w.strip(".,").lower() not in ("jr", "sr", "ii", "iii", "iv", "v")]
    return parts[-1] if parts else None


def _attributable(text: str, position: int, surname: Optional[str],
                  anchor: Optional[int] = None) -> bool:
    """Whether an injury mentioned at ``position`` can be pinned on this player.

    Args:
        text: The comment.
        position: Character offset of the match.
        surname: The subject's surname, lowercased.
        anchor: Offset of an already-attributed body part, when one was found. A
            modifier is judged against that rather than against the surname, because
            "Allgeier (knee) needs surgery" puts the word after the part, not the name.

    Returns:
        bool: True when the mention sits within :data:`ATTRIBUTION_WINDOW` characters
        after the surname or the anchor. True when there is no surname to check against --
        an unknown subject is not evidence of a misattribution, and the caller has no
        better option than the text it was handed.
    """
    if anchor is not None:
        return abs(position - anchor) <= ATTRIBUTION_WINDOW
    if not surname:
        return True
    lowered = text.lower()
    for match in re.finditer(rf"\b{re.escape(surname)}\b", lowered):
        if 0 <= position - match.start() <= ATTRIBUTION_WINDOW:
            return True
    return False


def _body_part_in_text(text: str, surname: Optional[str] = None
                       ) -> Tuple[Optional[str], Optional[int]]:
    """Find a body part named in free text, and say where it was found.

    Beat reports put the injury in parentheses straight after the name -- "Metcalf
    (undisclosed) will be hard pressed to work this week" -- so a parenthetical anchored on
    the subject's own surname is tried first and is the only reading that is reliably
    about him. A bare parenthetical comes next, then a bare mention anywhere, on a word
    boundary so "hand" does not fire on "handoff".

    Args:
        text: The comment.
        surname: The subject's surname, lowercased, for the anchored reading.

    Returns:
        tuple: ``(part, offset)``, or ``(None, None)``.
    """
    lowered = text.lower()
    known = lexicon.as_dict()

    # "<Surname> (part)" -- the convention, and the only self-evidently attributed form.
    if surname:
        for match in re.finditer(
                rf"\b{re.escape(surname)}\b[^(]{{0,12}}\(([^)]{{1,40}})\)", lowered):
            part = lexicon.normalise_body_part(match.group(1))
            if part and part in known:
                return part, match.start(1)

    for match in re.finditer(r"\(([^)]{1,40})\)", text):
        part = lexicon.normalise_body_part(match.group(1))
        if part and part in known:
            if _attributable(text, match.start(1), surname):
                return part, match.start(1)

    for part in sorted(known, key=len, reverse=True):
        match = re.search(rf"\b{re.escape(part)}\b", lowered)
        if match and _attributable(text, match.start(), surname):
            return part, match.start()
    return None, None


def _from_report(body_part: Optional[str]) -> Optional[Severity]:
    """Rung 5. The nflverse weekly report, which exists only in season.

    A body part and nothing else -- no severity, no duration -- so it resolves to the
    group prior and says as much.
    """
    if not body_part or not lexicon.is_injury(body_part):
        return None
    group = lexicon.group(body_part)
    low, high = GROUP_PRIORS.get(group, GROUP_PRIORS["other"])
    return _make(group, None, low, high, source="report", confidence="low",
                 evidence=f"injury report lists {body_part} with no severity")


def refresh_group_priors() -> Dict[str, Tuple[float, float]]:
    """Re-read the group priors from the built episode table, if it exists.

    :data:`GROUP_PRIORS` is committed as a literal so resolving a severity never depends
    on the episode table having been built. This is how the literal gets checked against
    the data that produced it.

    Returns:
        dict: Group -> (low, high), from ``injury_meta.json``, or
        :data:`GROUP_PRIORS` unchanged when it is absent.
    """
    from Scripts.injury import episodes

    meta = episodes.load_meta()
    if not meta:
        return dict(GROUP_PRIORS)
    priors = dict(GROUP_PRIORS)
    for row in meta.get("by_body_part", []):
        part, mean = row.get("body_part"), row.get("mean_weeks_out")
        if part and mean:
            priors[part] = (max(1.0, mean * 0.4), mean * 1.8)
    return priors
