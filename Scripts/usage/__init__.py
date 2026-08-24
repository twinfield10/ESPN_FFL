"""TOMCAT: the shared layer under this repo's own projection model.

**TOMCAT** -- Touches, Opportunity, Market, Context, Availability, Tiers -- is the name
of the internal model, and the six words are its feature families rather than a
decoration:

===============  =========================================================  ============
Letter           What it names                                              Arm
===============  =========================================================  ============
**T**ouches      Carries and targets. Raw volume                            usage
**O**pportunity  *Expected* production rather than realised. Beats actual   usage
                 at predicting next week (R-squared 0.2907 against 0.2702)
**M**arket       Implied team totals, implied points allowed, the spread    defence, kicking
**C**ontext      Depth chart, scheme, play-caller, red-zone role. The       all three
                 depth chart is the only feature that has ever moved this
                 model (+0.048 R-squared on veteran carries)
**A**vailability Expected games. Travels *beside* the projection rather     usage
                 than inside it, so `USG_Points` stays on a full-slate
                 footing comparable to every other source
**T**iers        Non-linear scoring ladders, integrated over a weekly       defence
                 distribution rather than evaluated at the season mean
===============  =========================================================  ============

One source with three arms, not three sources. A kicker and a team defence are positions
like any other -- they start every week and they need a projection. That the usage arm
reads snap share, the defence arm reads implied points allowed and the kicking arm reads
red-zone volume is an implementation detail below the line where a source casts its vote.

**The column prefix stays ``USG_``.** Renaming it to ``TOM_`` would orphan
``Data/G2/2026/``, the pre-season counterfactual archived to answer gate G2 after the
2026 season is played -- the one artifact in this repo that cannot be rebuilt, because no
historical pre-season blend survives. A prefix rename therefore needs a compatibility
shim on the archive read rather than a find-and-replace, and it is not worth doing before
a draft. The name is TOMCAT; the columns say ``USG_``; this docstring is where the two
are reconciled.

nflverse usage data is the shared layer underneath.

Every projection source this pipeline already blends -- ESPN, FantasyPros,
Pinnacle, BetOnline -- is somebody else's model output. Usage data is a different
kind of input: observed process data. Not "we think he'll get 62 yards" but "he
earned 107 yards of opportunity and returned 55".

This package holds the Python half. ``R/GetUsage.R`` pulls the data; the modules
here read it, build features from it, and -- first of all -- measure whether a
model built on it would add anything the four existing sources do not already
know. That measurement is the gate, not an afterthought:

    python -m Scripts.usage.gates --season 2025

See ``docs/plans/16-usage-data-layer.md``.
"""
