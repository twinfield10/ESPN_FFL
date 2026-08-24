"""Kickers, modelled as two team quantities and no individual skill.

The measurements this package is built on, 2016-2025 (see
``docs/plans/29-kicker-model.md``):

**A kicker's field-goal conversion rate has a year-over-year correlation of 0.009** across
222 kicker-season pairs, and his own field-goal attempts per game correlate -0.006 with his
previous season. There is no projectable individual skill here, so accuracy is a positional
constant and every per-kicker accuracy term is a fitted coincidence.

**What is sticky belongs to the offence.** Extra-point attempts per game persist at 0.346
for a kicker and 0.399 for a team -- and they do not travel: a kicker changing team takes
his PAT correlation from 0.386 to **-0.040**. So the model projects the team and hands the
result to whoever is kicking.

Two channels, because the evidence says two problems:

*Channel P, extra points*, is a Vegas read. Implied team total predicts PAT attempts at
r = 0.844 and team touchdowns at 0.848.

*Channel F, field goals*, is a red-zone read, and it is an **interaction**: a team in the
top third for red-zone volume and the bottom third for red-zone conversion gives its kicker
2.24 attempts a game against 1.74 for a low-volume, high-conversion offence. In the
good-conversion column volume stops mattering at all (1.74 / 1.80 / 1.78), because an
offence that finishes does not kick. Vegas cannot see this: implied total correlates just
0.117 with field-goal attempts, since the market prices scoring rather than failing to score.

**The spread is deliberately absent.** Net of the implied total it is flat -- 8.81 / 8.75 /
8.81 across its whole range in the top band -- so favouritism is expected scoring under
another name. ``docs/plans/30-dst-model.md`` reaches the opposite conclusion for defences,
which is why the two models do not share a feature list.

Build it with::

    python -m Scripts.kicking.model --season 2026 --write

See ``docs/plans/29-kicker-model.md``.
"""
