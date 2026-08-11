"""Draft-day machinery: the market, and a league-aware board over it.

``Scripts.draft.adp`` pulls what the room thinks (ADP, auction values) and what
ESPN projects. ``Scripts.draft.board`` turns that into what *this* league should
do about it -- replacement level from its real starting slots, VOR, tiers.

The split matters because the market half is league-independent and the valuation
half is not: one ADP request serves all nine leagues, then each league is valued
separately. See ``docs/plans/15-draft-board.md``.

``Scripts.draft.history`` and ``Scripts.draft.tendencies`` are the other half of a
draft: not who to take, but who the eleven other people in the room are.
``history`` pulls every pick the league has ever made; ``tendencies`` measures each
manager against the room they were sitting in and writes a sentence about it. See
``docs/plans/23-owner-tendencies.md``.

This package replaces ``Scripts/draft_utils.py``, which was dead code copied from
another project -- never imported, reading ``./src/doritostats/pick_value.csv``,
and carrying a different league's owner map.
"""
