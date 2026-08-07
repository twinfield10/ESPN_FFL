"""nflverse usage data: the shared layer under the two usage models.

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
