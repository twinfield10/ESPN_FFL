"""Body parts, severity words, and the groups they collapse into.

Two vocabularies, kept in one file because they have to agree with each other.

**Body-part groups.** ``nflreadr``'s ``report_primary_injury`` is free text with about
forty distinct values across ten seasons, including laterality (``"right Shoulder"``),
singular/plural drift (``"Rib"`` and ``"Ribs"``, ``"Tricep"`` and ``"Triceps"``) and
four spellings of "this is not an injury". Fitting a recovery curve per raw string would
put ``"right Hamstring"`` in a cell of its own with two episodes in it, so they collapse
into eleven groups.

**Why the mapping is pinned into the fitted artifact as well as living here.** The
group a body part lands in *is* the cell whose coefficients get fitted. If the mapping
lives only in code, editing it silently repoints a body part at coefficients that were
never fitted for it, and nothing reports the mismatch --
:mod:`Scripts.injury.model` writes :func:`as_dict` into the artifact and a test pins
them equal. Same reasoning as ``ROLE_WITHDRAWN_EVIDENCE`` being duplicated in
``app/draft_view.py`` and pinned by a test.

**Severity words** are the weakest channel in :mod:`Scripts.injury.severity` and
deliberately the most auditable: a flat list of regexes you can read and correct, with
no fuzzy matching and no scoring. Measured on the 2026-08-18 ESPN pull, exactly one of
800 comments contains "high ankle" -- so this earns its keep on the days it fires, not
on average, and abstains the rest of the time.
"""

from __future__ import annotations

import re
from typing import Dict, Optional

#: The eleven groups a body part can land in.
#:
#: Deliberately coarse. Finding 7 of ``docs/plans/27-injury-model.md`` measured 4-152
#: episodes per body-part x position cell across ten seasons, most under 40, so the
#: grouping is set by how much data a cell needs rather than by anatomy: ``hamstring``
#: is its own group because it is the most common soft-tissue injury *and* the one with
#: a published recurrence rate to validate against, while ``fibula`` and ``shin`` fall
#: into ``other`` because a cell of nine cannot be fitted whatever it is called.
GROUPS = (
    "ankle",
    "knee",
    "foot_toe",
    "hamstring",
    "soft_tissue_lower",
    "shoulder",
    "hand_wrist_arm",
    "concussion",
    "back_core",
    "ribs_chest",
    "illness",
    "other",
)

#: Raw ``report_primary_injury`` value (lowercased, laterality stripped) -> group.
_BODY_PARTS: Dict[str, str] = {
    # --- ankle -------------------------------------------------------------
    "ankle": "ankle",
    "high ankle": "ankle",
    "ankle_high": "ankle",
    "syndesmosis": "ankle",
    # --- knee --------------------------------------------------------------
    "knee": "knee",
    "acl": "knee",
    "mcl": "knee",
    "pcl": "knee",
    "lcl": "knee",
    "meniscus": "knee",
    "patella": "knee",
    # --- foot and toe ------------------------------------------------------
    "foot": "foot_toe",
    "toe": "foot_toe",
    "heel": "foot_toe",
    "plantar fascia": "foot_toe",
    "lisfranc": "foot_toe",
    # --- hamstring ---------------------------------------------------------
    "hamstring": "hamstring",
    # --- other lower-body soft tissue --------------------------------------
    # Grouped because they share a mechanism -- a strained muscle that heals on its own
    # timetable -- and individually none of them clears a fittable cell.
    "calf": "soft_tissue_lower",
    "groin": "soft_tissue_lower",
    "quadricep": "soft_tissue_lower",
    "quadriceps": "soft_tissue_lower",
    "quad": "soft_tissue_lower",
    "thigh": "soft_tissue_lower",
    "hip": "soft_tissue_lower",
    "adductor": "soft_tissue_lower",
    "achilles": "soft_tissue_lower",
    "glute": "soft_tissue_lower",
    "hip flexor": "soft_tissue_lower",
    # --- shoulder ----------------------------------------------------------
    "shoulder": "shoulder",
    "collarbone": "shoulder",
    "clavicle": "shoulder",
    "ac joint": "shoulder",
    "rotator cuff": "shoulder",
    "labrum": "shoulder",
    # --- hand, wrist, arm --------------------------------------------------
    # The group that matters most at quarterback and least at running back, which is
    # why position stays a reported split even where it cannot carry a multiplier.
    "hand": "hand_wrist_arm",
    "wrist": "hand_wrist_arm",
    "thumb": "hand_wrist_arm",
    "finger": "hand_wrist_arm",
    "elbow": "hand_wrist_arm",
    "forearm": "hand_wrist_arm",
    "arm": "hand_wrist_arm",
    "biceps": "hand_wrist_arm",
    "bicep": "hand_wrist_arm",
    "triceps": "hand_wrist_arm",
    "tricep": "hand_wrist_arm",
    # --- head --------------------------------------------------------------
    "concussion": "concussion",
    "head": "concussion",
    # --- back and core -----------------------------------------------------
    "back": "back_core",
    "neck": "back_core",
    "abdomen": "back_core",
    "oblique": "back_core",
    "core muscle": "back_core",
    "core": "back_core",
    "spine": "back_core",
    "stinger": "back_core",
    # --- ribs and chest ----------------------------------------------------
    "ribs": "ribs_chest",
    "rib": "ribs_chest",
    "chest": "ribs_chest",
    "pectoral": "ribs_chest",
    "sternum": "ribs_chest",
    "lung": "ribs_chest",
    # --- illness -----------------------------------------------------------
    "illness": "illness",
    "covid-19": "illness",
    "covid": "illness",
    "non-football illness": "illness",
    # --- everything else ---------------------------------------------------
    "shin": "other",
    "fibula": "other",
    "tibia": "other",
    "eye": "other",
    "jaw": "other",
    "nose": "other",
    "ear": "other",
    "undisclosed": "other",
    "not disclosed": "other",
    # Plurals the upstream feed uses interchangeably with the singular.
    "ankles": "ankle",
    "knees": "knee",
    "hamstrings": "hamstring",
    "hands": "hand_wrist_arm",
    # Long-tail values seen 1-5 times across ten seasons, mapped so they stop showing
    # up in :func:`unmapped` and drawing attention they do not deserve.
    "sternoclavicular": "shoulder",
    "lumbar": "back_core",
    "pelvis": "soft_tissue_lower",
    "leg": "other",
    "medical illness": "illness",
    "covid protocols": "illness",
    "covid/reserve": "illness",
    "core muscle injury": "back_core",
}

#: Report values that are not injuries and must never open an episode.
#:
#: ``report_primary_injury`` carries roster bookkeeping alongside diagnoses. A rested
#: starter and a suspended one are both absent, and counting either as an injury would
#: put a healthy player into the recovery fit -- with a *baseline* drawn from the four
#: weeks he was fine, which is exactly the shape of a spurious effect.
NOT_INJURY = (
    "not injury related",
    "not injury related - personal matter",
    "not injury related - other",
    "not injury related - resting player",
    "not injury related - resting",
    "personal",
    "personal matter",
    "suspension",
    "suspended",
    "holdout",
    "coach's decision",
    "coaches decision",
    "coaching decision",
    "inactive",
    "returning from suspension",
    "non football injury",
    "non-football injury",
    "rest",
)

#: Groups excluded from the recovery-curve fit, though they still open episodes.
#:
#: An illness costs availability and nothing else: there is no tissue healing on a
#: timetable, so a post-return efficiency ramp has no mechanism behind it. Keeping
#: illness in the episode table but out of the curve fit means availability still counts
#: it while the ramp does not invent a reason for it.
RECOVERY_EXCLUDED_GROUPS = ("illness", "other")

#: Laterality and qualifier prefixes that carry no diagnostic content.
_QUALIFIERS = re.compile(
    r"^(right|left|r\.?|l\.?|bilateral|lower|upper)\s+", re.IGNORECASE)

_WHITESPACE = re.compile(r"\s+")

#: Separators the feed uses when a player is carrying more than one injury.
#:
#: ``"toe, pec, knee, hip"`` and ``"foot/wrist/hip"`` both appear. The first-listed part
#: is the report's own ordering and the closest thing to a primary diagnosis available,
#: so it wins -- and the secondary columns already exist for anyone who wants the rest.
_MULTI = re.compile(r"\s*[,/;]\s*|\s+and\s+")


def normalise_body_part(raw: Optional[str]) -> Optional[str]:
    """Strip laterality and casing from a raw report value.

    Args:
        raw: A ``report_primary_injury`` value, or None.

    Returns:
        str | None: The lowercased, unqualified value, or None when there is nothing
        to read. ``"right Shoulder"`` becomes ``"shoulder"``.
    """
    if raw is None:
        return None
    text = _WHITESPACE.sub(" ", str(raw)).strip().lower()
    if not text:
        return None
    text = _MULTI.split(text)[0].strip()
    if not text:
        return None
    # Loop: "lower right leg" carries two qualifiers.
    while True:
        stripped = _QUALIFIERS.sub("", text).strip()
        if stripped == text:
            break
        text = stripped
    return text or None


def is_injury(raw: Optional[str]) -> bool:
    """Whether a report value describes an injury at all.

    Args:
        raw: A ``report_primary_injury`` value, or None.

    Returns:
        bool: False for the roster-bookkeeping values in :data:`NOT_INJURY`, and for
        None -- an absence with no stated reason is not evidence of an injury, and
        :mod:`Scripts.injury.episodes` requires corroboration for those.
    """
    text = normalise_body_part(raw)
    if text is None:
        return False
    if text in NOT_INJURY:
        return False
    # "Not injury related - <anything>" is an open-ended family upstream.
    return not text.startswith("not injury related")


def group(raw: Optional[str]) -> str:
    """The group a raw report value belongs to.

    Unknown values land in ``"other"`` rather than raising. The upstream vocabulary
    grows without notice -- ``"Core Muscle"`` first appears in 2023 -- and a pull that
    crashed the whole episode build over one new noun would be worse than one that
    files it somewhere honest and lets :func:`unmapped` report it.

    Args:
        raw: A ``report_primary_injury`` value, or None.

    Returns:
        str: One of :data:`GROUPS`.
    """
    text = normalise_body_part(raw)
    if text is None:
        return "other"
    return _BODY_PARTS.get(text, "other")


def unmapped(values) -> Dict[str, int]:
    """Report values that fell through to ``"other"``, with their counts.

    The counterpart to :func:`group`'s permissiveness: the build cannot crash on a new
    noun, so it has to say which nouns it did not recognise. A value appearing often
    enough to matter belongs in :data:`_BODY_PARTS`.

    Args:
        values: Iterable of raw report values.

    Returns:
        dict: Unrecognised normalised value -> count, excluding non-injuries.
    """
    counts: Dict[str, int] = {}
    for raw in values:
        text = normalise_body_part(raw)
        if text is None or not is_injury(raw):
            continue
        if text not in _BODY_PARTS:
            counts[text] = counts.get(text, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: -kv[1]))


def as_dict() -> Dict[str, str]:
    """The body-part mapping, for pinning into the fitted artifact.

    Returns:
        dict: A copy of the raw-value -> group mapping.
    """
    return dict(_BODY_PARTS)


# --- severity vocabulary, read by Scripts.injury.severity ------------------

#: Severity phrases and the duration signal they carry, most specific first.
#:
#: Order is load-bearing: ``"high ankle sprain"`` has to be tested before ``"ankle"``
#: and before ``"sprain"``, or the specific reading is lost to a general one. The
#: values are weeks-out ranges, which is what a beat report actually supports -- a
#: point estimate would be false precision on a sentence written by a journalist.
SEVERITY_PHRASES = (
    (r"high[\s-]*ankle", ("ankle", (3, 6))),
    (r"\btorn\s+achilles|achilles\s+(?:tear|rupture)", ("soft_tissue_lower", (40, 52))),
    (r"\btorn\s+acl\b|\bacl\s+(?:tear|rupture)|tore\s+(?:his\s+)?acl", ("knee", (40, 52))),
    (r"\bacl\b", ("knee", (36, 52))),
    (r"\bmcl\b", ("knee", (3, 8))),
    (r"\b(?:pcl|lcl)\b", ("knee", (4, 10))),
    (r"meniscus", ("knee", (3, 10))),
    (r"lisfranc", ("foot_toe", (8, 16))),
    (r"jones\s+fracture", ("foot_toe", (6, 12))),
    (r"plantar\s+fascii?t?is|plantar\s+fascia", ("foot_toe", (2, 8))),
    (r"turf\s+toe", ("foot_toe", (2, 8))),
    (r"ac\s+joint", ("shoulder", (2, 6))),
    (r"rotator\s+cuff", ("shoulder", (4, 12))),
    (r"labrum|labral", ("shoulder", (4, 16))),
    (r"\bstinger\b", ("back_core", (0, 1))),
    (r"concussion", ("concussion", (1, 3))),
    (r"core\s+muscle", ("back_core", (4, 8))),
)

#: Generic severity words, applied only after :data:`SEVERITY_PHRASES` finds nothing.
#:
#: These modify a duration rather than naming one, because "a sprain" spans a week and a
#: season depending on the joint and the grade.
SEVERITY_MODIFIERS = (
    (r"\bseason[\s-]*ending\b", "season_ending"),
    (r"\bout\s+for\s+the\s+(?:season|year)\b", "season_ending"),
    (r"\bsurger(?:y|ies)\b|\bprocedure\b|\bunderwent\b", "surgery"),
    (r"\btorn?\b|\brupture", "tear"),
    (r"\bfractur|\bbroken?\b|\bhairline\b", "fracture"),
    (r"\bgrade\s*(?:3|iii|three)\b", "grade_3"),
    (r"\bgrade\s*(?:2|ii|two)\b", "grade_2"),
    (r"\bgrade\s*(?:1|i|one)\b", "grade_1"),
    (r"\bmulti[\s-]*week\b|\bweek[\s-]*to[\s-]*week\b", "multi_week"),
    (r"\bsprain", "sprain"),
    (r"\bstrain", "strain"),
    (r"\bsore(?:ness)?\b|\btightness\b|\bbanged\s+up\b", "minor"),
    (r"\bday[\s-]*to[\s-]*day\b", "minor"),
)
