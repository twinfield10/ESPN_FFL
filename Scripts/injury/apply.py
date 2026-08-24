"""Where the injury model meets a projection.

**Nothing here multiplies anything yet, and that is the phase-3 design.** This module
attaches what :mod:`Scripts.injury.severity` resolved -- body part, expected absence,
which channel answered, and one readable evidence string -- as diagnostic columns beside
the projection. A haircut waits on the fitted curve clearing its gates in phase 4.

The ordering is not arbitrary and matters later, so it is written down now.
``reconcile_team_totals`` scales each team's passing and receiving sides to their
midpoint, so docking a receiver's ``TRUE_receivingYards`` *before* that runs would drag
the midpoint down, scale his **quarterback** down and his healthy **teammates up** -- the
injury would leak across the roster in both directions. Any multiplier this module grows
attaches **after** reconciliation and **before** ``proj_to_score``, so the haircut stays
on the one player and every league's scoring rules inherit it.

**Abstention here is a multiplier of exactly 1.0 with a non-empty evidence string, and
that reads as a violation of the repo's null-and-flag convention until the reason is
given.** Everywhere else in this pipeline a source that has nothing to say is nulled and
flagged, so ``compute_weighted_stats`` can drop its weight and renormalise and
``sources_real`` can count honestly. A multiplier cannot do that: it is applied by
arithmetic, and a null would propagate into ``TRUE_Points`` -- which is the exact failure
``_apply_scoring``'s docstring records having already cost every per-source ``*_Points``
column on every stored board. So the convention is honoured in substance rather than in
form: ``inj_evidence`` is never empty, and a reader can always distinguish "the model
looked and found no effect" from "the model could not look". ``inj_severity_source`` of
``"none"`` is the second.

Pandas at the boundary, because the projection blend is pandas. The severity resolution
underneath is plain Python.
"""

from __future__ import annotations

import datetime
from typing import Dict, List, Optional

import pandas as pd

from Scripts.injury import severity as sv

#: The diagnostic columns this module owns.
#:
#: Lowercase, per the convention that ``<UPPER>_<stat>`` is reserved for blendable stat
#: columns -- ``compute_weighted_stats`` and ``proj_to_score`` scan every ``UPPER_``
#: prefix and require it to be numeric, so a string column named ``INJ_evidence`` would
#: be picked up by both. Same reasoning as ``usg_arm`` and ``usg_evidence``.
INJURY_COLUMNS = (
    "inj_body_part",
    "inj_detail",
    "inj_expected_absence_weeks",
    "inj_absence_low",
    "inj_absence_high",
    "inj_duration_bucket",
    "inj_severity_source",
    "inj_confidence",
    "inj_evidence",
    "inj_season_ending",
    "inj_reinjury_prob",
    "inj_recovery_cost",
    "inj_recovery_ladder",
)

#: What ``inj_evidence`` says when the player is simply healthy.
#:
#: Distinct from an abstention. "The report has no record of him" and "the report has a
#: record and it says nothing severe" are different states, and collapsing them is the
#: mistake ``usg_evidence`` exists to avoid -- an empty column there meant three things
#: at once and all three looked like agreement.
NO_INJURY_EVIDENCE = ""


def attach_severity(frame: pd.DataFrame, season: int,
                    week_one: Optional[datetime.date] = None,
                    report: Optional[pd.DataFrame] = None,
                    overrides: Optional[Dict[str, Dict]] = None,
                    join_column: str = "join_key") -> pd.DataFrame:
    """Attach the resolved severity for every player, as diagnostics only.

    Degrades rather than fails at every step. A missing override file, a missing injury
    pull, a frame with no join key: each yields the columns with nothing in them, because
    a board that will not build is worse than a board with an empty column. That is the
    same contract :func:`Scripts.season_projections.load_espn_injuries` already keeps.

    Args:
        frame: The merged projection frame, carrying ``join_column`` and ideally
            ``player_id`` (ESPN's id, which is what the override file prefers).
        season: Season being projected.
        week_one: First gameday, for reading a return date. When None it is looked up
            from the committed schedule.
        report: ESPN injury report, for tests. Loaded from disk when None.
        overrides: Parsed override file, for tests. Loaded from disk when None.
        join_column: The normalised-name column to join the report on.

    Returns:
        pd.DataFrame: ``frame`` with :data:`INJURY_COLUMNS` added.
    """
    frame = _blank(frame)

    if report is None:
        from Scripts.season_projections import load_espn_injuries
        report = load_espn_injuries(season)
    if week_one is None:
        from Scripts.season_projections import _week_one
        week_one = _week_one(season)
    if overrides is None:
        try:
            overrides = sv.load_overrides(season)
        except ValueError as error:
            # A malformed override file is a human error worth surfacing loudly, but not
            # one that should stop nine boards from building.
            print(f"  Injury overrides ignored -- {error}")
            overrides = {}

    if join_column not in frame.columns:
        return frame

    records: Dict[str, Dict] = {}
    if report is not None and not report.empty and "name_key" in report.columns:
        records = {row["name_key"]: row
                   for row in report.to_dict("records") if row.get("name_key")}

    if overrides:
        keys = set(frame[join_column].dropna().astype(str).str.upper())
        if "player_id" in frame.columns:
            keys |= set(frame["player_id"].dropna().astype(str))
        sv.check_overrides(overrides, known_keys=sorted(keys))

    resolved: List[sv.Severity] = []
    ids = (frame["player_id"].astype("object")
           if "player_id" in frame.columns else pd.Series(None, index=frame.index,
                                                          dtype="object"))
    for key, player_id in zip(frame[join_column], ids):
        record = dict(records.get(key) or {})
        record.setdefault("name_key", key)
        if player_id is not None and not pd.isna(player_id):
            record["espn_id"] = player_id
        # A player the report has no record of and no override for is healthy as far as
        # anything here can tell, and must not be given an abstention -- that would read
        # as "we could not look" for ~600 of the ~1,000 players on a board.
        if not record.get("status") and not _has_override(record, overrides):
            resolved.append(None)
            continue

        found = sv.resolve(record, overrides=overrides, week_one=week_one)

        # An abstention on a player ESPN calls Active is not an abstention -- it is a
        # healthy player. Measured on the 2026-08-18 pull: 686 of 800 records are Active,
        # and most of their comments are ordinary news ("completed three of five
        # passes"). Reporting those as "no severity evidence" would put an abstention on
        # 299 board rows and make the flag meaningless, which is exactly the failure
        # ``usg_evidence`` was created to fix -- one blank column standing for three
        # different states, all of which looked like agreement.
        #
        # An abstention on a player listed Questionable, Doubtful, Out or on reserve is
        # real and worth surfacing: he is hurt and nothing here can say how badly. That
        # is the row where the override file earns its keep.
        if found.abstained and sv.reads_as_active(record):
            resolved.append(None)
            continue
        resolved.append(found)

    return _write(frame, resolved)


def _has_override(record: Dict, overrides: Dict[str, Dict]) -> bool:
    if not overrides:
        return False
    espn_id = record.get("espn_id")
    if espn_id is not None and str(espn_id) in overrides:
        return True
    name = record.get("name_key")
    return bool(name) and str(name).strip().upper() in overrides


def _blank(frame: pd.DataFrame) -> pd.DataFrame:
    """Name every column up front, so a downstream reader never sees a KeyError."""
    frame = frame.copy()
    dtypes = {
        "inj_body_part": "object", "inj_detail": "object",
        "inj_expected_absence_weeks": "float64", "inj_absence_low": "float64",
        "inj_absence_high": "float64", "inj_duration_bucket": "object",
        "inj_severity_source": "object", "inj_confidence": "object",
        "inj_evidence": "object", "inj_season_ending": "boolean",
        "inj_reinjury_prob": "float64", "inj_recovery_cost": "float64",
        "inj_recovery_ladder": "object",
    }
    for column, dtype in dtypes.items():
        if dtype == "object":
            frame[column] = pd.Series(None, index=frame.index, dtype="object")
        elif dtype == "boolean":
            # pandas' nullable extension dtype takes pd.NA.
            frame[column] = pd.Series(pd.NA, index=frame.index, dtype=dtype)
        else:
            # A numpy float64 column cannot be filled with pd.NA -- pandas tries
            # ``float(pd.NA)`` and raises. It has to be a real nan.
            frame[column] = pd.Series(float("nan"), index=frame.index, dtype=dtype)
    frame["inj_evidence"] = NO_INJURY_EVIDENCE
    return frame


def _write(frame: pd.DataFrame, resolved: List[Optional[sv.Severity]]) -> pd.DataFrame:
    """Fill the diagnostic columns from the resolved severities."""
    rows = []
    for found in resolved:
        if found is None:
            rows.append({"inj_evidence": NO_INJURY_EVIDENCE})
            continue
        rows.append({
            "inj_body_part": None if found.abstained else found.body_part,
            "inj_detail": found.detail,
            "inj_expected_absence_weeks": found.weeks_expected,
            "inj_absence_low": found.weeks_low,
            "inj_absence_high": found.weeks_high,
            "inj_duration_bucket": None if found.abstained else found.duration_bucket,
            "inj_severity_source": found.source,
            "inj_confidence": found.confidence,
            "inj_evidence": found.evidence,
            "inj_season_ending": found.season_ending,
        })

    filled = pd.DataFrame(rows, index=frame.index)
    for column in filled.columns:
        frame[column] = filled[column]
    frame["inj_season_ending"] = frame["inj_season_ending"].astype("boolean")
    frame["inj_evidence"] = frame["inj_evidence"].fillna(NO_INJURY_EVIDENCE)
    return frame


def attach_model_diagnostics(frame: pd.DataFrame, model=None,
                             season: Optional[int] = None) -> pd.DataFrame:
    """Attach the fitted model's readings, **as diagnostics only**.

    The walk-forward rejected both heads against their pre-committed gates: the recovery
    curve gains about 1% accuracy against a 2% bar, and the weekly hazard's Brier score is
    0.9898 of a constant base rate's against a 0.98 bar. So nothing here multiplies a
    projection, and that is the outcome ``docs/plans/27-injury-model.md`` named in advance
    rather than a shortfall discovered late.

    What survives rejection is worth having anyway. The curve is well calibrated -- slope
    1.05, so a cell predicted to lose 20% loses 20% -- it is just not accurate enough per
    player to multiply by. And the pooled per-body-part recurrence rate passes an external
    check the fitted weekly hazard was never asked to: 9.8% for a hamstring against a
    published 11.9%. Both are the kind of thing a drafter wants beside a projection.

    Three readings, chosen because none of them needs to know where in the ramp a player
    currently sits -- which a pre-season board cannot know:

    ``inj_recovery_cost``
        Games-equivalent the ramp costs in total, the sum of the shortfalls across the
        window. Directly interpretable, and it is what a season multiplier would have been
        built from.
    ``inj_recovery_ladder``
        The ramp itself, so the number above can be checked rather than trusted.
    ``inj_reinjury_prob``
        Probability the same body part goes again within six weeks of returning.

    Args:
        frame: A frame already carrying :data:`INJURY_COLUMNS` from
            :func:`attach_severity`.
        model: A fitted :class:`Scripts.injury.model.InjuryModel`. Loaded from disk when
            None; a missing artifact leaves the columns empty rather than failing, because
            a board that will not build is worse than a board with an empty column.
        season: Season being projected, for the staleness check. Skipped when None.

    Returns:
        pd.DataFrame: ``frame`` with the three columns added.
    """
    frame = frame.copy()
    for column in ("inj_reinjury_prob", "inj_recovery_cost"):
        frame[column] = pd.Series(float("nan"), index=frame.index, dtype="float64")
    frame["inj_recovery_ladder"] = pd.Series(None, index=frame.index, dtype="object")

    if model is None:
        from Scripts.injury.model import InjuryModel
        try:
            model = InjuryModel.load()
        except FileNotFoundError as error:
            print(f"  Injury model not fitted; diagnostics blank -- {error}")
            return frame

    # Said out loud, because ``InjuryModel.is_stale`` existed for a while with nothing
    # calling it -- which is the same as not having it. Its sibling in
    # ``usage/project.py`` caught a real artifact trained through 2024 when it should
    # have been 2025, and an injury curve fitted before a season was played is the same
    # error with a quieter failure: every number still renders.
    if season is not None and model.is_stale(season - 1):
        trained = max(model.train_seasons) if model.train_seasons else "nothing"
        print(f"  Injury model trained through {trained}, projecting {season} -- refit "
              f"with `python -m Scripts.injury.episodes --rebuild` then "
              f"`python -m Scripts.injury.model --fit`.")

    if "inj_body_part" not in frame.columns:
        return frame

    ladders, costs, risks = [], [], []
    for part, bucket, weeks in zip(frame["inj_body_part"],
                                   frame.get("inj_duration_bucket",
                                             pd.Series(None, index=frame.index)),
                                   frame.get("inj_expected_absence_weeks",
                                             pd.Series(None, index=frame.index))):
        if part is None or pd.isna(part):
            ladders.append(None)
            costs.append(float("nan"))
            risks.append(float("nan"))
            continue
        bucket = None if (bucket is None or pd.isna(bucket)) else str(bucket)
        ramp = model.ladder(part, bucket)
        ladders.append(" ".join(f"{value:.2f}" for value in ramp))
        costs.append(sum(1.0 - value for value in ramp))
        out = 0.0 if (weeks is None or pd.isna(weeks)) else float(weeks)
        risks.append(model.reinjury_probability(part, out))

    frame["inj_recovery_ladder"] = ladders
    frame["inj_recovery_cost"] = costs
    frame["inj_reinjury_prob"] = risks
    return frame


def summary(frame: pd.DataFrame) -> str:
    """A printable account of what the ladder resolved, for the build log.

    Reported per rung rather than as a total, because the interesting number is how often
    the *weak* rungs are carrying the answer. A board where most severities came from the
    news text is a board where the override file is the highest-value thing to edit.

    Args:
        frame: A frame carrying :data:`INJURY_COLUMNS`.

    Returns:
        str: One or more indented lines.
    """
    if "inj_severity_source" not in frame.columns:
        return "  Injury severity: not attached."
    counts = frame["inj_severity_source"].value_counts(dropna=True).to_dict()
    if not counts:
        return "  Injury severity: no player carries an injury record."
    order = ["override", "espn_structured", "return_date", "comment", "report", "none"]
    parts = [f"{name}={counts[name]}" for name in order if name in counts]
    lines = [f"  Injury severity: {', '.join(parts)}"]
    ending = frame["inj_season_ending"].fillna(False).sum()
    if ending:
        lines.append(f"    {int(ending)} read as out for the season")
    return "\n".join(lines)
