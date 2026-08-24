"""Injuries as three separate quantities, not one availability flag.

The pipeline has always treated an injury as one thing: a discount on games available,
applied to one source. ``Scripts/season_projections.py::_apply_injury_adjustment``
multiplies ``USG_`` by ``games_available / 17``, and ``clean_lineups`` has no injury
layer at all. Three different questions are collapsed into that one number:

**A. Availability** -- how long until he plays again.
**B. Return to form** -- how good he is once he does.
**C. Recurrence** -- how likely he is to go again.

They are separate because they behave separately. Measured over 2016-2025 on this repo's
own data: hamstrings show essentially no lasting efficiency cost after a return but a
9.9% chance of recurring inside six weeks, while feet and hands cost 35%+ of production
in the first game back. A model priced on availability alone calls both of them free; a
model priced on the ramp alone calls a hamstring cheap.

**Two findings shape every design decision here.**

*Snap share does not drop.* Median snap-share ratio to baseline is 0.99-1.02 in every
appearance after a return -- teams put a returning player straight back on the field. The
loss is per-snap efficiency, which is why the volume and opportunity layers structurally
cannot be pricing it, and why a multiplier here does not double-count them.

*The placebo is not 1.0.* Healthy players passing the identical baseline filter come in
at 0.98 and stay flat, because a four-game mean is a selected high point and weekly
scoring is right-skewed. Fitting a recovery curve against 1.0 instead of against the
control would attribute that skew to injury and haircut every returning player for
reasons that have nothing to do with his ankle. Every net figure this package reports is
a ratio to the matched control, and :func:`Scripts.injury.episodes.control_cohort` is the
frame that makes it honest.

Modules::

    lexicon.py    body-part groups and severity vocabulary
    episodes.py   the episode table, the control cohort, the descriptive report

Build the data layer with::

    python -m Scripts.injury.episodes --rebuild

See ``docs/plans/27-injury-model.md``.
"""
