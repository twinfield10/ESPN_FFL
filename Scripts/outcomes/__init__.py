"""Outcome distributions: what a season could be, not just its mean.

The board publishes a mean and a *disagreement* interval. Nothing publishes forecast
uncertainty in points -- every stat has a distribution
(:mod:`Scripts.usage.predictive`), games played has one
(:mod:`Scripts.usage.availability`), and the one number a drafter reads has neither.

The architectural claim, and the reason this is not just an aggregation:

**The unit of simulation is the position room, not the player.** A backup running back's
season is bimodal -- a 70-point flier if the man ahead of him plays seventeen games, a
170-point starter if that man misses eight -- and which world he is in is a function of
*another player's* row. A per-player marginal interval has no channel to carry that, so it
reports one smear over both worlds and calls the middle of it a projection.

Two measurements decide the shape of the fix, and both are in :mod:`Scripts.outcomes.evidence`:

*A backfield is near zero-sum; a receiver room is not.* A lead back's 17.42 opportunities
a game go 81% to the next three backs and the room keeps 93% of its volume. A lead
receiver's understudy gains 0.60 of 7.72 targets, the room recaptures 44%, and the offence
throws 1.25 fewer times. So a redistribution rule belongs on RB and TE rooms and nowhere else.

*A fragile starter is not a tradeable edge for his backup.* The transfer is real
conditional on the absence (+5.72 points a game for an RB2), but the absence is not
forecastable enough to price into a mean -- RB2 season points are 111.7 behind a clean
incumbent and 92.4 behind one who missed 3-5 games. Which is the argument for pricing it
into a **variance**.

Modules::

    evidence.py       the measurements behind docs/plans/28-outcome-distributions.md
    distribution.py   season points as a distribution, from the per-stat marginals
    vacancy.py        where a missing starter's work goes, fitted
    simulate.py       the room-level joint draw over a shared availability draw
    backtest.py       walk-forward scoring against the pre-committed gates

Reproduce every figure in that plan with::

    python -m Scripts.outcomes.evidence
    python -m Scripts.outcomes.vacancy --report
    python -m Scripts.outcomes.backtest --seasons 2021-2025

**What the gates decided, so the next reader does not have to re-derive it.** ``G-D1``
passed -- 80% interval coverage 0.730 with a calibration slope of 1.072, both inside
windows fixed before the run -- so the distribution ships. ``G-D2`` **failed**: the
room-level joint draw puts backup coverage only **+2.1pp** closer to nominal against a
5pp bar, so phase 1 ships alone and ``BOARD_USES_JOINT_DRAW`` is False.

The way it failed is worth keeping. Entrenched starters move **+0.0pp** -- the false
positive clause found nothing -- so the vacancy effect is exactly as specific as the
mechanism claims. It is the magnitude that fails, not the direction. The room machinery
stays here, measured and rejected, the way plan 27's recovery curve did.

See ``docs/plans/28-outcome-distributions.md``.
"""
