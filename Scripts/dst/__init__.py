"""Team defence, modelled once and scored nine ways.

The measurements behind this package, 2016-2025 (see ``docs/plans/30-dst-model.md``):

**Nine ladders, one defence.** Scoring every team-defence season under each league's own
slot-16 rules gives a median pairwise rank correlation of **0.968**; all nine leagues rank
the same defence first. So this projects a component *vector* and lets ``proj_to_score``
apply each ladder, rather than building nine models.

**Most of what leagues pay for does not persist.** In six of the nine, sacks +
interceptions + fumble recoveries are **71%** of the score, with year-over-year
correlations of 0.203, 0.113 and **0.015**. Defensive touchdowns, the most valuable single
event, correlate **-0.052** with themselves. Total D/ST points stick at r = 0.22-0.27 in
every league, so last season's defence is nearly worthless as a predictor.

**The market is the way out, and for more than the obvious component.** Implied points
allowed beats the prior season on seven of eight components -- sacks 0.464 against 0.203,
interceptions 0.357 against 0.113, fumble recoveries 0.193 against **0.015**. Opponent
offences drive defensive events, and the market prices opponent offences.

**The spread is a second, separate channel** -- unlike for kickers, where it collapsed into
the implied total. Within a band of implied points allowed it still adds 2.20 to 2.75 D/ST
points a game and moves sacks 1.86 to 2.50, because implied points allowed prices the
*opponent's offence* while the spread prices *who will be ahead*, and being ahead is what
makes an opponent throw: pass share runs 47.8% in a blowout loss against 63.2% in a
blowout win.

**A tiered ladder must be integrated, not evaluated at a mean.** Weekly points allowed has
a standard deviation of 9.57 against tiers 4 to 7 points wide, so scoring the season mean
understates the best third of defences by 12.24 points and overstates the worst by 4.26 --
a 16.5-point compression of the range the component exists to create. So the output is a
**distribution over tiers**, expressed as expected games in each, which is the same shape
ESPN publishes and sums to the slate.

Build it with::

    python -m Scripts.dst.model --season 2026 --write

See ``docs/plans/30-dst-model.md``.
"""
