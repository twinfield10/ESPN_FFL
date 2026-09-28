"""The D/ST model priced off each book's own lines, as that book's weekly D/ST line.

Neither book posts a D/ST market, so on the weekly board ``PINNY_`` and ``BOL_`` abstained
on every defence and the D/ST blend was ESPN plus whatever FantasyPros had. But the D/ST
model's only inputs are a spread and a total (plan 30), and each book posts both. So each
book's D/ST line here is the fitted model evaluated on **that book's** game lines -- its
main spread, its main total, and its own team total where it quotes one.

**What this is and is not.** It is one model read through two sets of lines, not two
independent opinions: Pinnacle and BetOnline agree within 0.27 points on spreads (2026
weeks 1-4), so the two lines differ by little, and together they carry two votes for the
model in the equal-vote blend. That is the requested weighting and it is written here so
it is a choice rather than a surprise.

Only stats the model produces are written. Blocked kicks and kick/punt return touchdowns
are not modelled, so those cells stay null and the book abstains on them, which is how
:func:`Scripts.projection_utils.compute_weighted_stats` treats any stat a source has no
line for.
"""

from __future__ import annotations

from typing import Dict, Optional

import polars as pl

from Scripts import vegas
from Scripts.dst import model as dm

#: Blend prefix to odds-store book name.
BOOKS: Dict[str, str] = {"PINNY": "Pinnacle", "BOL": "BetOnline"}


def weekly(season: int, prefix: str, model: Optional[Dict] = None) -> pl.DataFrame:
    """One book's D/ST component vector for every team-game it has priced.

    Args:
        season: Season year.
        prefix: ``"PINNY"`` or ``"BOL"``.
        model: Output of :func:`Scripts.dst.model.fit`. Loaded when None.

    Returns:
        pl.DataFrame: ``season``, ``week``, ``team`` (schedule abbreviation),
        ``implied_source`` and ``proj_<stat>``. Empty when the book has priced nothing.
    """
    lines = vegas.book_game_lines(season, BOOKS[prefix])
    if lines.is_empty():
        return lines.select("season", "week", "team", "implied_source")
    out = dm.project_games(lines, model)
    return out.join(lines.select("season", "week", "team", "implied_source"),
                    on=["season", "week", "team"], how="left")
