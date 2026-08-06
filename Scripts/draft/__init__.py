"""Draft-day machinery: the market, and a league-aware board over it.

``Scripts.draft.adp`` pulls what the room thinks (ADP, auction values) and what
ESPN projects. ``Scripts.draft.board`` turns that into what *this* league should
do about it -- replacement level from its real starting slots, VOR, tiers.

The split matters because the market half is league-independent and the valuation
half is not: one ADP request serves all nine leagues, then each league is valued
separately. See ``docs/plans/15-draft-board.md``.

This package replaces ``Scripts/draft_utils.py``, which was dead code copied from
another project -- never imported, reading ``./src/doritostats/pick_value.csv``,
and carrying a different league's owner map.
"""
