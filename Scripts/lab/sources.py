"""Render ``docs/projection_sources.html`` -- what every projection source is.

A generated file, committed. Generated because most of what it reports --
coverage, per-position bias, the sample lines -- is measured against whatever the
board held that morning, and rosters, depth charts and injuries all move daily
through camp; a hand-maintained table would be wrong within a week. Committed
because the point of it is to be readable without running anything, and diffable,
so "what changed between drafts" is a question git can answer.

It reads exactly one file: ``Data/Store/<season>/<league>/board.parquet``. Every
source's stat line, every ``_is_imputed`` flag and every ``<SRC>_Points`` column
already sit side by side there, so nothing here re-joins a source, refits a model
or touches the network.

**Two of the numbers here are league-specific and the rest are not.** Points and
positional ranks are what one league's rules did to a stat line, and comparing
them across leagues is meaningless. The stat-level bias tables are not scored at
all, so they hold everywhere. The page says which is which rather than leaving a
reader to guess.

Usage::

    python -m Scripts.lab.sources
    python -m Scripts.lab.sources --league gop_degenerates --out /tmp/preview.html
"""

import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import polars as pl

from Scripts.lab.report import STYLE as BASE_STYLE, esc, table
from Scripts.paths import REPO_ROOT, store_dir
from Scripts.projection_utils import IMPUTED_SUFFIX, WEEKLY_PREFIXES, WEIGHTS

#: Where the rendered atlas lands.
OUTPUT_PATH = REPO_ROOT / "docs" / "projection_sources.html"

#: The board this renders against unless told otherwise. Knights is the 14-team
#: full-PPR league plan 38 measured on, so its numbers line up with that document.
DEFAULT_LEAGUE = "knights_ffl"
DEFAULT_SEASON = 2026

#: The measurement gate, and the first place in this repo it is executable.
#:
#: ``docs/plans/38-the-athletic.md`` defines it in prose and nothing in ``Scripts/``
#: or ``app/`` holds it as a constant. It exists because percentage disagreement
#: divides by the projection, so two sources 8 and 13 points apart on a third-string
#: quarterback read as 47% disagreement about nobody -- and ungated, that noise
#: dominated every position row and made ``ATH`` look like the most independent
#: source in the blend, which it is not.
#:
#: **Built from ESPN and FantasyPros only, never from the source under test**, so no
#: source can be accused of having selected the rows that flatter it.
GATE_POINTS = 100.0

#: The sources that carry a stat line, in the order the page presents them.
#:
#: TOMCAT is **one** entry, not three. Its usage, kicking and defence arms all write
#: ``USG_`` and share a single vote -- they differ in which positions they can speak
#: about, which is not what a source is. Position scoping falls out of the provenance
#: flags: a quarterback's ``USG_madeExtraPoints`` is null and flagged, so the weight is
#: dropped and the rest renormalise, exactly as for a book with no line.
SOURCES: Tuple[str, ...] = ("ESPN", "FP", "PINNY", "BOL", "ATH", "USG")

#: Sources that fill their gaps from another source, and from which.
#:
#: This is the dependency graph, and it is the answer to "does this source depend on
#: another one". A book with no line on a player is ESPN and FantasyPros wearing a
#: sportsbook badge; the flags exist so that copy cannot vote as though it were an
#: independent opinion.
#:
#: ``USG`` is deliberately absent. Filling the one genuinely independent source from
#: an average of two that are not is the double-counting plan 03 measured, so its
#: gaps stay null and stay flagged.
IMPUTED_FROM: Dict[str, str] = {
    "FP": "ESPN",
    "PINNY": "MEAN",
    "BOL": "MEAN",
    "ATH": "MEAN",
}

#: Which stats to report for which position, and in what order.
#:
#: Trimmed to what the position actually accumulates. A table of receiving yards for
#: quarterbacks is a row of zeroes that pushes the rows a reader wants off the screen.
STATS_BY_POSITION: Dict[str, Tuple[str, ...]] = {
    "QB": ("passingAttempts", "passingYards", "passingTouchdowns",
           "passingInterceptions", "rushingAttempts", "rushingYards",
           "rushingTouchdowns"),
    "RB": ("rushingAttempts", "rushingYards", "rushingTouchdowns",
           "receivingTargets", "receivingReceptions", "receivingYards",
           "receivingTouchdowns"),
    "WR": ("receivingTargets", "receivingReceptions", "receivingYards",
           "receivingTouchdowns"),
    "TE": ("receivingTargets", "receivingReceptions", "receivingYards",
           "receivingTouchdowns"),
}

POSITIONS: Tuple[str, ...] = ("QB", "RB", "WR", "TE")

#: Short labels, so a stat column head does not wrap in four places.
STAT_LABELS: Dict[str, str] = {
    "passingAttempts": "pass att", "passingCompletions": "comp",
    "passingYards": "pass yds", "passingTouchdowns": "pass TD",
    "passingInterceptions": "INT", "rushingAttempts": "carries",
    "rushingYards": "rush yds", "rushingTouchdowns": "rush TD",
    "receivingTargets": "targets", "receivingReceptions": "rec",
    "receivingYards": "rec yds", "receivingTouchdowns": "rec TD",
}

#: The players the page renders in full, chosen to be a mix rather than a top list:
#: the consensus top of each position, the loudest disagreements, and the rows where
#: availability is the whole question -- injury, suspension, or a rookie nobody has
#: a prior season for.
SAMPLE_PLAYERS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("QB", ("Josh Allen", "Lamar Jackson", "Fernando Mendoza", "Deshaun Watson")),
    ("RB", ("Bijan Robinson", "Chase Brown", "Bhayshul Tuten", "Jeremiyah Love",
            "Josh Jacobs", "MarShawn Lloyd")),
    ("WR", ("Puka Nacua", "Justin Jefferson", "George Pickens", "Garrett Wilson",
            "Jayden Higgins", "DK Metcalf", "Carnell Tate")),
    ("TE", ("Trey McBride", "Brock Bowers", "Mark Andrews", "Sam LaPorta",
            "Mason Taylor")),
)


# --------------------------------------------------------------------------- #
# Reading the board
# --------------------------------------------------------------------------- #

def load_board(season: int, league: str) -> pl.DataFrame:
    """Read one league's draft board.

    Args:
        season: Season year, e.g. 2026.
        league: Store league key, e.g. ``"knights_ffl"``.

    Returns:
        pl.DataFrame: One row per draftable player.

    Raises:
        SystemExit: When the board has not been built. Raised rather than returning
            an empty frame, because every table below would render as a page of em
            dashes and read as "the sources have no coverage".
    """
    path = store_dir(season, league, "board.parquet")
    if not path.is_file():
        raise SystemExit(
            f"No board at {path}.\n"
            f"Build it with: python -m Scripts.refresh --league {league} --what board")
    return pl.read_parquet(path)


def has(df: pl.DataFrame, column: str) -> bool:
    """Whether a column is on the frame."""
    return column in df.columns


def real_mask(df: pl.DataFrame, source: str, stat: str) -> Optional[pl.Expr]:
    """Rows where ``source`` holds a genuine, non-imputed line for ``stat``.

    A source with no provenance column counts as real -- that is
    :func:`Scripts.projection_utils.compute_weighted_stats`' own rule, and ESPN is
    the case that matters: it is the root everything else imputes from and carries no
    flags at all.

    A null flag means the row never joined, which is an absence rather than an
    opinion, so it is treated as imputed.

    Args:
        df: The board.
        source: Prefix, e.g. ``"PINNY"``.
        stat: ESPN stat name, e.g. ``"receivingYards"``.

    Returns:
        A boolean expression, or None when the source has no such column at all.
    """
    column = f"{source}_{stat}"
    if not has(df, column):
        return None
    expression = pl.col(column).is_not_null()
    flag = column + IMPUTED_SUFFIX
    if has(df, flag):
        # Cast before filling. A source that joined for *nobody* leaves an all-null
        # flag column, which polars types as Null -- and `fill_null(True)` on it
        # raises rather than returning all-True. That is the one shape this function
        # most needs to survive: a source with no coverage at all must read as "no
        # opinion anywhere", not as a crash.
        expression = expression & ~pl.col(flag).cast(pl.Boolean,
                                                     strict=False).fill_null(True)
    return expression


def gate_expr(df: pl.DataFrame) -> pl.Expr:
    """The 100-point gate, built from ESPN and FantasyPros only."""
    parts = [pl.col(f"{s}_Points").fill_null(0.0) > GATE_POINTS
             for s in ("ESPN", "FP") if has(df, f"{s}_Points")]
    if not parts:
        return pl.lit(True)
    combined = parts[0]
    for part in parts[1:]:
        combined = combined | part
    return combined


def pct(numerator: int, denominator: int) -> str:
    """A percentage, or an em dash when there is nothing to divide by."""
    if not denominator:
        return '<span class="muted">—</span>'
    return f"{100.0 * numerator / denominator:.1f}%"


def num(value: Optional[float], decimals: int = 1) -> str:
    """A number, or an em dash when it is absent."""
    if value is None:
        return '<span class="muted">—</span>'
    return f"{value:,.{decimals}f}"


def ratio(value: Optional[float]) -> str:
    """A ratio against the ESPN/FantasyPros baseline, coloured by direction."""
    if value is None:
        return '<span class="muted">—</span>'
    cls = "over" if value >= 1.10 else ("under" if value <= 0.90 else "")
    span = f'<span class="{cls}">' if cls else "<span>"
    return f"{span}{value:.3f}</span>"


# --------------------------------------------------------------------------- #
# Measurements
# --------------------------------------------------------------------------- #

def coverage(df: pl.DataFrame, stats: Sequence[str]) -> List[List[str]]:
    """Share of rows where each source holds a real line, per stat.

    Ungated on purpose, and it is the one table here that is: coverage is a
    statement about the whole pool, and gating it would answer a different question
    ("how well covered are the good players") while looking like this one.
    """
    rows = []
    for stat in stats:
        row = [f"<code>{esc(stat)}</code>"]
        for source in SOURCES:
            mask = real_mask(df, source, stat)
            if mask is None:
                row.append('<span class="muted">no column</span>')
                continue
            row.append(pct(int(df.select(mask.sum()).item() or 0), df.height))
        rows.append(row)
    return rows


def _quantiles(frame: pl.DataFrame, column: str) -> Dict[str, Optional[float]]:
    """Mean, median and the 10th/90th percentiles of one column."""
    if not frame.height:
        return {"mean": None, "median": None, "p10": None, "p90": None}
    series = frame[column].drop_nulls()
    if not series.len():
        return {"mean": None, "median": None, "p10": None, "p90": None}
    return {"mean": series.mean(), "median": series.median(),
            "p10": series.quantile(0.10), "p90": series.quantile(0.90)}


def _safe_ratio(top: Optional[float], bottom: Optional[float]) -> Optional[float]:
    """``top / bottom``, or None where the denominator cannot carry a ratio."""
    if top is None or bottom is None or abs(bottom) < 1e-9:
        return None
    return top / bottom


def bias_rows(df: pl.DataFrame, position: str, stat: str) -> List[List[str]]:
    """Each source against the ESPN/FantasyPros baseline, on identical rows.

    **The pairing is the whole method.** A source's real cells are a biased sample of
    the pool -- a book prices the players worth pricing -- so comparing its 30 lines
    against a 500-row baseline measures coverage and calls it bias. Every row here
    restricts the baseline to exactly the rows the source was real for, so the only
    thing left between them is disagreement.

    The baseline is ``MEAN_`` = avg(ESPN, FantasyPros). ESPN and FantasyPros are shown
    against it too, and the two are near-symmetric around 1.000 by construction: that
    pair is the reference, because it is how far apart the two sources everyone
    already trusts are on the same quantity.

    Returns:
        One row per source: n, mean ratio, median ratio, p10-p90 spread ratio.
    """
    baseline_column = f"MEAN_{stat}"
    if not has(df, baseline_column):
        return []
    gated = df.filter(
        (pl.col("primaryPosition") == position)
        & gate_expr(df)
        & pl.col(baseline_column).is_not_null()
        & (pl.col(baseline_column) > 0.0))

    rows = []
    for source in SOURCES:
        column = f"{source}_{stat}"
        mask = real_mask(gated, source, stat)
        if mask is None:
            continue
        paired = gated.filter(mask)
        if paired.height < 5:
            # Fewer than five paired rows is a name, not a distribution. Reported as
            # a count with no ratios rather than dropped, because "this source has
            # almost nothing to say here" is itself the finding.
            rows.append([f"<code>{esc(source)}</code>", str(paired.height)]
                        + ['<span class="muted">—</span>'] * 3)
            continue
        source_stats = _quantiles(paired, column)
        base_stats = _quantiles(paired, baseline_column)
        spread_source = (None if source_stats["p90"] is None
                         else source_stats["p90"] - source_stats["p10"])
        spread_base = (None if base_stats["p90"] is None
                       else base_stats["p90"] - base_stats["p10"])
        rows.append([
            f"<code>{esc(source)}</code>",
            str(paired.height),
            ratio(_safe_ratio(source_stats["mean"], base_stats["mean"])),
            ratio(_safe_ratio(source_stats["median"], base_stats["median"])),
            ratio(_safe_ratio(spread_source, spread_base)),
        ])
    return rows


def realised_weights(df: pl.DataFrame, position: str, stat: str
                     ) -> Dict[str, Optional[float]]:
    """What each source's 0.25 actually renormalises to, on this position's rows.

    The nominal weight is almost never the realised one, and that is the design
    rather than a defect: because every universal source carries the same 0.25,
    renormalising over the sources that are *real* makes "one equal vote per source
    that has an opinion" literal -- four real sources weight 0.25 each, three weight
    0.333, two weight 0.5.

    Averaged per row rather than pooled, because the question is what the typical
    player's blend looked like, not what the sum of all of them did.
    """
    gated = df.filter((pl.col("primaryPosition") == position) & gate_expr(df))
    if not gated.height:
        return {source: None for source in SOURCES}

    weights = WEIGHTS["default"]
    contributions: Dict[str, pl.Series] = {}
    denominator = pl.Series("d", [0.0] * gated.height)
    for source in SOURCES:
        mask = real_mask(gated, source, stat)
        weight = float(weights.get(source, 0.0))
        if mask is None or weight == 0.0:
            contributions[source] = pl.Series(source, [0.0] * gated.height)
            continue
        is_real = gated.select(mask.alias("r"))["r"].fill_null(False)
        share = pl.Series(source, [weight if flag else 0.0 for flag in is_real])
        contributions[source] = share
        denominator = denominator + share

    out: Dict[str, Optional[float]] = {}
    live = [i for i, value in enumerate(denominator) if value and value > 0.0]
    if not live:
        return {source: None for source in SOURCES}
    for source in SOURCES:
        share = contributions[source]
        out[source] = sum(share[i] / denominator[i] for i in live) / len(live)
    return out


def source_ranks(df: pl.DataFrame, source: str) -> Dict[int, int]:
    """Positional rank by one source's points, keyed by row index.

    Computed here rather than read off the board because only ``TRUE_`` and ``USG_``
    ship a ``PosRank`` column, and the comparison this page exists for needs all of
    them on the same footing.
    """
    column = f"{source}_Points"
    if not has(df, column):
        return {}
    frame = (df.with_row_index("_row")
               .select(["_row", "primaryPosition", column])
               .filter(pl.col(column).is_not_null() & (pl.col(column) > 0.0)))
    if not frame.height:
        return {}
    ranked = frame.sort(column, descending=True).with_columns(
        pl.col(column).rank("ordinal", descending=True)
          .over("primaryPosition").alias("_rank"))
    return {int(row): int(rank)
            for row, rank in zip(ranked["_row"], ranked["_rank"])}


# --------------------------------------------------------------------------- #
# What each source is
# --------------------------------------------------------------------------- #

#: The prose half of each source card.
#:
#: Hand-written because it describes mechanism, which does not change nightly; every
#: *number* on the page is measured. Where a claim here has a measurement behind it,
#: the plan that made it is named, so a reader can check rather than trust.
SOURCE_CARDS: Dict[str, Dict[str, object]] = {
    "ESPN": {
        "name": "ESPN",
        "ingest": "Fantasy API. One <code>view=kona_player_info</code> request per "
                  "league with an <code>x-fantasy-filter</code> header, parsed "
                  "through <code>espn_api</code>.",
        "where": "<code>Scripts/season_projections.py</code>, "
                 "<code>Scripts/draft/adp.py</code>",
        "file": "None. It arrives with the league fetch and goes straight into the "
                "store, which is why it is the one source with no folder under "
                "<code>Data/Projections/</code>.",
        "when": "Draft and weekly.",
        "depends": "Nothing. It is the root, and the only source never flagged "
                   "imputed — everything else imputes <em>from</em> it.",
        "carries": "Raw stats, plus the market: <code>adp</code>, "
                   "<code>auction_value</code>, <code>espn_draft_rank</code>, "
                   "<code>percent_owned</code>. It is also the player universe — "
                   "a player ESPN does not list cannot appear on a board at all.",
        "strengths": [
            "Total coverage. Every player in every league, always real, never "
            "imputed — the only source that can say that.",
            "Carries the market (ADP, auction values) that VOR and dollar "
            "allocation are priced against.",
        ],
        "weaknesses": [
            "Because it is universal and never imputed, it carries the largest "
            "realised weight on almost every row — 0.69–0.91 on stats where the "
            "others abstain. An absent source reads as agreement.",
            "Three season stats are <em>per-game</em> and must be multiplied out "
            "(<code>PER_GAME_IN_SEASON_ROW</code>); return and defensive yardage "
            "must not be. Scaling <code>puntReturnYards</code> once turned a "
            "422-point defence into 2,294.",
            "Weekly yardage arrives doubled for some rows, corrected against "
            "ESPN's own published points by <code>halve_doubled_espn_yardage</code>.",
        ],
    },
    "FP": {
        "name": "FantasyPros",
        "ingest": "HTML scrape of <code>/nfl/projections/&lt;pos&gt;.php</code> with "
                  "BeautifulSoup, honouring <code>robots.txt</code>'s "
                  "<code>Crawl-delay: 5</code>.",
        "where": "<code>Scripts/scrape_FP.py</code>",
        "file": "<code>Data/Projections/FantasyPros/Season/&lt;season&gt;/"
                "FantasyPros_Projections_Season.parquet</code>",
        "when": "Draft and weekly. The season pull uses the sentinel "
                "<code>week=\"draft\"</code> in place of a week number.",
        "depends": "Joins on player name, needs no ids. But it is half of "
                   "<code>MEAN_</code>, so both books and The Athletic inherit it "
                   "wherever they abstain.",
        "carries": "Raw stats only — its <code>STD_FantasyPoints</code> column is "
                   "read and thrown away, because points are what a league's rules "
                   "do to a stat line. Publishes <strong>no "
                   "<code>receivingTargets</code></strong>.",
        "strengths": [
            "Best raw coverage of any external source once a free account cookie "
            "is set: 592 players against the registration fence's 60.",
            "An aggregator of many underlying projections, so it is stable — it "
            "rarely produces the lone strange line a single model can.",
        ],
        "weaknesses": [
            "<strong>The least independent source in the blend.</strong> Its "
            "residuals correlate <strong>+0.988</strong> with ESPN's, which is why "
            "plan 03's fitted weight re-tune failed its own stability clause by "
            "7–10×: no amount of data can split weight between near-copies.",
            "Over-spread on receiving volume — calibration 0.65–0.95 against "
            "realised, the opposite direction to TOMCAT's 1.30–1.37.",
            "Projects deep benches, so it is one of the two sources the 100-point "
            "gate moves most.",
        ],
    },
    "PINNY": {
        "name": "Pinnacle",
        "ingest": "Season: two plain-JSON requests to "
                  "<code>guest.api.arcadia.pinnacle.com</code> (league 889) — "
                  "deliberately <em>not</em> Selenium. Weekly: a browser driven "
                  "against the live DOM.",
        "where": "<code>Scripts/scrape_pinnacle_season.py</code>, "
                 "<code>Scripts/scrape_pinnacle.py</code>",
        "file": "<code>Data/Projections/Pinnacle/Season/&lt;season&gt;/"
                "Pinnacle_SeasonProps.parquet</code>",
        "when": "Draft and weekly, but only the season scraper runs nightly.",
        "depends": "Imputes from <code>MEAN_</code> — so on a player it has no line "
                   "for, this source <em>is</em> ESPN and FantasyPros. Its "
                   "anytime-TD market is re-split by the ESPN+FantasyPros consensus "
                   "ratio.",
        "carries": "Prices, converted to stat lines. Ten markets, long format: one "
                   "row per player-stat, with <code>line</code>, "
                   "<code>over_odds</code>, <code>no_vig_over_prob</code>.",
        "strengths": [
            "<strong>Converted onto a full healthy slate since 2026-09-02.</strong> A season-long prop settles on what a player actually accumulates, so a book must price the games he misses, while ESPN projects a median of 17.0 and FantasyPros the same. Its lines are divided by <code>0.895</code> — the realised share of a 17-game slate a fantasy starter plays, measured over 2021–2025 — so all six sources describe the same quantity. Anchored on that base rate rather than on the observed gap, deliberately: fitting the gap would force agreement by construction and dissolve the disagreement the blend exists to measure.",
            "Real money against real money. A sharp book's line is the single "
            "best-informed number available on the players it prices.",
            "The 100-point gate moves it <strong>+0.0</strong> — a book only "
            "prices what is worth pricing, so it has no deep bench to distort.",
        ],
        "weaknesses": [
            "<strong>Thinnest coverage in the blend, by a distance.</strong> It "
            "prices 19 players' rushing yards and 30 players' receiving yards — "
            "fewer than any other source, and roughly a seventeenth of "
            "FantasyPros. On most rows its vote is not a vote.",
            "<strong>The season path never de-vigs.</strong> "
            "<code>_pivot_props</code> pivots on the raw <code>line</code>; the "
            "<code>no_vig_over_prob</code> its own scraper computes is never read "
            "there. BetOnline's season path uses a juice-adjusted "
            "<code>True_Line</code> and Pinnacle's <em>weekly</em> path inverts "
            "properly, so the draft board is the one place this is not done.",
            "Measured last on independence of the four externals (+0.036).",
        ],
    },
    "BOL": {
        "name": "BetOnline",
        "ingest": "Season: HTTP JSON against "
                  "<code>api-offering.betonline.ag</code>, pulled by "
                  "<code>R/GetSeasonProps.R</code>. Weekly: <strong>broken</strong> "
                  "— a different host, now 403 without a signed header.",
        "where": "<code>R/GetSeasonProps.R</code>, <code>Scripts/scrape_BOL.py</code>",
        "file": "<code>Data/Projections/BetOnline/Season/&lt;season&gt;/"
                "BetOnline_SeasonProps_All.csv</code>",
        "when": "<strong>Draft only.</strong> The weekly endpoint has been dead "
                "since before this season.",
        "depends": "Imputes from <code>MEAN_</code>. Its IDP tackle split depends on "
                   "<code>Data/NFL_Tackles_By_Position.csv</code>.",
        "carries": "Prices with a juice adjustment (<code>True_Line = line + "
                   "juice_diff × line × 0.5</code>). <strong>The only source with "
                   "IDP props</strong> — sacks, interceptions, tackles — which is "
                   "the whole of the second opinion in a 16-team IDP league.",
        "strengths": [
            "<strong>Converted onto a full healthy slate since 2026-09-02.</strong> A season-long prop settles on what a player actually accumulates, so a book must price the games he misses, while ESPN projects a median of 17.0 and FantasyPros the same. Its lines are divided by <code>0.895</code> — the realised share of a 17-game slate a fantasy starter plays, measured over 2021–2025 — so all six sources describe the same quantity. Anchored on that base rate rather than on the observed gap, deliberately: fitting the gap would force agreement by construction and dissolve the disagreement the blend exists to measure.",
            "The only non-ESPN opinion that exists at all for individual "
            "defenders.",
            "Wider season coverage than Pinnacle: 273 players, 13 stat columns.",
            "De-vigged before it is treated as a projection, unlike the season "
            "Pinnacle path.",
        ],
        "weaknesses": [
            "Weekly is gone, so it is a draft-time source that a weekly reader may "
            "still see a stale column for.",
            "<strong>Sends 100% of the anytime-TD market to rushing</strong> for "
            "every quarterback and running back — 988 of 995 back-weeks had "
            "<code>BOL_receivingTouchdowns == 0</code> before "
            "<code>reallocate_book_touchdowns</code>.",
            "Badly over-spread: calibration 0.21–0.81 against realised.",
            "Was still posting a 575-yard season line for a player nine days into "
            "season-ending IR, which is the bug that built the availability gates.",
        ],
    },
    "ATH": {
        "name": "The Athletic — Jake Ciely",
        "ingest": "<strong>A hand-dropped paid <code>.xlsx</code>.</strong> No API, "
                  "no scraper, no nightly stage. Read from the 32 <em>team</em> "
                  "tabs, because those are the model — <code>PASS ATT = "
                  "team_pass_attempts × player_pass_share</code> — while the "
                  "per-position tabs are <code>VLOOKUP</code>s that drop columns.",
        "where": "<code>Scripts/load_athletic.py</code>",
        "file": "<code>Data/Projections/TheAthletic/Season/&lt;season&gt;/"
                "TheAthletic_Projections_Season.parquet</code> (434 players) and "
                "<code>…_Ranks_Season.parquet</code> (290)",
        "when": "<strong>Draft only.</strong> One file per season, and it only goes "
                "stale by human neglect — which is why "
                "<code>Scripts.refresh_status</code> names it.",
        "depends": "Imputes from <code>MEAN_</code>. Contributed four name aliases: "
                   "two nicknames and two of the workbook's own typos.",
        "carries": "Twelve raw stats. <strong>No <code>lostFumbles</code></strong>. "
                   "Its own <code>VORP</code>, <code>AUC$</code> and "
                   "<code>DST</code> outputs are deliberately not read.",
        "strengths": [
            "<strong>Genuinely well covered, which a sixth source need not have "
            "been.</strong> It publishes real rushing lines for 323 players against "
            "ESPN's 241 and either book's fewer than 40 — second only to "
            "FantasyPros on rushing, third on receiving. Those counts are the same "
            "on every board; only the denominator in the table above moves, because "
            "an IDP pool is twice the size.",
            "Genuinely independent: 18–22% from every other source on gated "
            "players, against 9.9% for ESPN-vs-FantasyPros. About as far away as a "
            "sportsbook is.",
            "Team-budget × share by construction, so its lines are internally "
            "coherent within a team before anything reconciles them.",
            "A human with a view. Its biggest call — Travis Hunter 65 ranks below "
            "consensus — is independently agreed by TOMCAT, which shares none of "
            "its inputs.",
        ],
        "weaknesses": [
            "<strong>Not automated.</strong> A source carrying a sixth of every "
            "projection that updates only when somebody saves a file.",
            "<strong>Its out-of-sample MAE has never been measured.</strong> It "
            "was wired straight to a full equal vote rather than shipping dark at "
            "0.0 first, five days before an auction. Plan 20's rule — measure "
            "per-stat MAE before wiring a feed in — was not followed, and the "
            "measurement is owed rather than skipped.",
            "The team tabs leak: on one tab target share lands on a quarterback, "
            "scoring him 59.8 instead of 7.8. Masked by position, and every masked "
            "row is named rather than dropped.",
            "<strong>A live join miss it did not cause:</strong> the workbook "
            "spells Kenny Gainwell <em>Kenneth</em>, so a 165-point back's real "
            "line has been imputed away from the ESPN/FantasyPros mean since the "
            "merge.",
        ],
    },
    "USG": {
        "name": "TOMCAT \u2014 our model, three backends",
        "ingest": "<strong>Built here, not fetched.</strong> Fitted from nflverse "
                  "play-by-play back to 2016 — usage, opportunity, depth charts, "
                  "snap counts, routes, red zone, contracts. Every stat is a volume "
                  "term × an efficiency rate.",
        "where": "<code>Scripts/usage/</code> — <code>season.py</code> fits, "
                 "<code>project.py</code> writes",
        "file": "<code>Data/Projections/Usage/Season/&lt;season&gt;/"
                "Usage_SeasonProjections.parquet</code>",
        "when": "<strong>Draft only.</strong> There is no weekly head — "
                "<code>docs/plans/19-weekly-usage-model.md</code> is not started.",
        "depends": "<strong>Deliberately outside the imputation chain.</strong> "
                   "Filling the one independent source from an average of two that "
                   "are not would turn it into a copy of them. Its gaps stay null. "
                   "The only source that joins on an <em>id</em> "
                   "(<code>gsis_id</code> → <code>player_id</code>) rather than a "
                   "name.",
        "carries": "Eight raw stats from the usage arm, 33 from the defence arm and "
                   "13 from the kicking arm \u2014 <strong>disjoint sets, so one "
                   "<code>USG_</code> namespace holds all three without collision</strong>. "
                   "Usage stats each carry a fitted <code>_sd</code>, "
                   "<code>_low</code> and <code>_high</code>, plus "
                   "<code>usg_expected_games</code>, <code>usg_arm</code> and "
                   "<code>usg_role_cohort</code> travelling <em>beside</em> the "
                   "line rather than inside it. <strong>Publishes volume since "
                   "2026-09-02</strong> — targets, carries and pass attempts, which "
                   "the model always predicted (every stat is a volume term × a rate) "
                   "but carried as a diagnostic and never blended.",
        "strengths": [
            "<strong>The most independent source in the blend by a distance</strong> "
            "— +0.113 residual independence against the best external's +0.068, "
            "and +0.371 partialled against +0.180. External consensus is a "
            "saturated channel; this is not part of it.",
            "<strong>The best-calibrated single source</strong> on realised "
            "results, against FantasyPros over-spread at 0.65–0.95 and BetOnline "
            "at 0.21–0.81.",
            "Has a rookie arm (ρ ≈ 0.61) where a prior season does not exist, which "
            "is exactly the population every other source guesses at.",
            "<strong>Prices the role rather than the depth chart.</strong> Plan 33 "
            "measured the listed chart as right about a settled starter only "
            "<strong>58.8%</strong> of the time; since 2026-09-03 each volume term is "
            "evaluated at every rank the chart could be wrong about and averaged by how "
            "often it is wrong that way. It cuts a listed WR1 about 4% and gives a listed "
            "rank 2 the 16.7% of the time he is actually the lead.",
            "<strong>A veteran who missed a whole season is projected rather than "
            "skipped.</strong> The rate ladder falls back to a pooled positional baseline "
            "\u2014 QB 7.12 yards an attempt, RB 4.27 a carry \u2014 so a hundred carries "
            "cannot come out as five yards. The <code>baseline</code> arm names them.",
            "Decomposes into volume × efficiency × games, so a disagreement can be "
            "read rather than only noted.",
        ],
        "weaknesses": [
            "<strong>Under-spread on receiving volume</strong> — RB receptions "
            "1.315, TE receiving yards 1.370, QB rushing TDs 1.510 against "
            "realised. It shrinks toward positional baselines where the others "
            "extrapolate. <em>Correcting it alone would make the board worse</em>: "
            "its error and FantasyPros' point in opposite directions and partly "
            "cancel.",
            "Its availability arm is its weakest: prior-season games predict next "
            "season at r = <strong>+0.343</strong>.",
            "Covers only QB/RB/WR/TE — silent on kickers and defences.",
            "Excluded from the floor/ceiling spread on purpose: it answers "
            "\"what does the model expect\" rather than \"what do forecasters "
            "disagree about\", and mixing the two makes the interval read as "
            "bearish rather than uncertain.",
        ],
    },
}


# --------------------------------------------------------------------------- #
# Sections
# --------------------------------------------------------------------------- #

def availability(source: str) -> str:
    """Draft, weekly or both — read from the pipeline's own prefix lists."""
    return "draft + weekly" if source in WEEKLY_PREFIXES else "draft only"


def register_section(df: pl.DataFrame, league: str) -> str:
    """Every prefix on the board, and what it is."""
    weights = WEIGHTS["default"]
    rows = []
    for source in SOURCES:
        card = SOURCE_CARDS[source]
        weight = float(weights.get(source, 0.0))
        scope = ("QB/RB/WR/TE + K + D/ST, three backends"
                 if source == "USG" else "universal")
        rows.append([
            f"<code>{esc(source)}_</code>",
            str(card["name"]),
            availability(source),
            (str(IMPUTED_FROM[source]) if source in IMPUTED_FROM
             else '<span class="muted">nothing</span>'),
            f"{weight:.2f}",
            scope,
        ])
    rows.append(["<code>MEAN_</code>", "avg(ESPN, FantasyPros, The Athletic) over "
                 "their <em>real</em> cells — the imputation basis, not an opinion",
                 "draft + weekly", "ESPN + FP + ATH", '<span class="muted">n/a</span>', "derived"])
    rows.append(["<code>TRUE_</code>", "<strong>the blend</strong> — what the board "
                 "ranks on", "draft + weekly", "all of the above",
                 '<span class="muted">n/a</span>', "derived"])

    overlays = [
        ["ESPN draft market", "<code>adp</code>, <code>auction_value</code>, "
         "<code>espn_draft_rank</code>, <code>percent_owned</code>",
         "Prices the board against. Also <em>is</em> the player universe."],
        ["Jake's Ranks", "<code>ath_pos_rank</code>, <code>ath_override</code>, "
         "<code>ath_rank_delta</code>",
         "Lower-case by construction, so <code>compute_weighted_stats</code> cannot "
         "see them. A hand ranking is an ordering, and the blend works in stat "
         "space — there is nowhere to put it. <code>TRUE_Points</code> and "
         "<code>vor_rank</code> are byte-identical with and without it."],
        ["ESPN injury report", "<code>inj_*</code>, <code>avail_evidence</code>",
         "Drives the availability gates. <strong>Carries no player id</strong> and "
         "joins on a normalised name — the most fragile join in the repo."],
        ["Depth charts", "<code>usg_depth_rank</code>, <code>usg_role_cohort</code>",
         "Withdraws TOMCAT where the chart says backup <em>and</em> ESPN has priced "
         "him out."],
        ["Scoring registry", "<code>Data/Scoring/scoring.csv</code>",
         "One row per league-season-<em>slot</em>-stat. An input to ingest, not an "
         "output: it is what turns one stat line into nine different points "
         "columns."],
    ]

    return f"""
<section id="register">
  <h2>The register</h2>
  <p>Six sources vote, and five more things reach the board without voting. This table
  is where that is said once.</p>
  <p class="note">It was eight prefixes until 2026-09-02. TOMCAT's kicking and defence
  backends wrote <code>KIK_</code> and <code>DST_</code> and carried entries of their
  own, which made the blend look wider than it is \u2014 they share a model family, a
  fitting harness and an owner, and differ only in which positions they can speak
  about. That is not what a source is, so they now write <code>USG_</code> and TOMCAT
  casts one vote across every position.</p>
  {table(["prefix", "source", "available", "imputes from", "weight", "scope"],
         rows, 2)}
  <p class="note">The weights are nominal and <strong>never normalised to
  1.0</strong>. <code>compute_weighted_stats</code> divides by whichever sources
  turned out to be real on each row. A source that abstains on a position simply has
  a flagged cell there and drops its weight, so <strong>abstention is expressed in the
  flags rather than in this table</strong> \u2014 which is why TOMCAT covers every position
  from one entry.</p>

  <h3>On the board, voting on nothing</h3>
  {table(["input", "columns", "what it does"], overlays, 99)}
</section>"""


def dependency_section() -> str:
    """The imputation chain, drawn — because it is the dependency graph."""
    return """
<section id="dependency">
  <h2>Which sources depend on which</h2>
  <p>The honest answer to &ldquo;is this source independent?&rdquo; is the
  <em>imputation chain</em>. When a source has no line for a player, its cell is
  filled from somewhere else and flagged. A book with no opinion on a player is
  therefore <strong>ESPN and FantasyPros wearing a sportsbook badge</strong> — and
  if that copy voted at face value it would count the same two sources three or four
  times over.</p>
  <figure>
  <svg class="chart" viewBox="0 0 880 300" width="880" role="img"
       aria-label="ESPN feeds FantasyPros; ESPN and FantasyPros average into MEAN;
                   MEAN fills Pinnacle, BetOnline and The Athletic; TOMCAT stands
                   outside the chain.">
    <defs>
      <marker id="arw" viewBox="0 0 10 10" refX="9" refY="5"
              markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 0 L 10 5 L 0 10 z" class="head"/>
      </marker>
    </defs>

    <rect x="10"  y="20"  width="130" height="46" rx="7" class="node root"/>
    <text x="75"  y="41"  class="lbl" text-anchor="middle">ESPN</text>
    <text x="75"  y="57"  class="sub" text-anchor="middle">never imputed</text>

    <rect x="10"  y="104" width="130" height="46" rx="7" class="node"/>
    <text x="75"  y="125" class="lbl" text-anchor="middle">FantasyPros</text>
    <text x="75"  y="141" class="sub" text-anchor="middle">fills from ESPN</text>

    <rect x="240" y="62"  width="130" height="46" rx="7" class="node derived"/>
    <text x="305" y="83"  class="lbl" text-anchor="middle">MEAN</text>
    <text x="305" y="99"  class="sub" text-anchor="middle">avg of real cells</text>

    <rect x="500" y="10"  width="150" height="40" rx="7" class="node"/>
    <text x="575" y="35"  class="lbl" text-anchor="middle">Pinnacle</text>
    <rect x="500" y="60"  width="150" height="40" rx="7" class="node"/>
    <text x="575" y="85"  class="lbl" text-anchor="middle">BetOnline</text>
    <rect x="500" y="110" width="150" height="40" rx="7" class="node feeds"/>
    <text x="575" y="130" class="lbl" text-anchor="middle">The Athletic</text>
    <text x="575" y="145" class="sub" text-anchor="middle">also feeds MEAN</text>

    <rect x="500" y="196" width="150" height="52" rx="7" class="node alone"/>
    <text x="575" y="217" class="lbl" text-anchor="middle">TOMCAT</text>
    <text x="575" y="234" class="sub" text-anchor="middle">gaps stay null</text>

    <path d="M 75 66 L 75 104"    class="edge" marker-end="url(#arw)"/>
    <path d="M 140 43 L 240 78"   class="edge" marker-end="url(#arw)"/>
    <path d="M 140 127 L 240 96"  class="edge" marker-end="url(#arw)"/>
    <path d="M 370 78 L 500 32"   class="edge" marker-end="url(#arw)"/>
    <path d="M 370 85 L 500 80"   class="edge" marker-end="url(#arw)"/>
    <path d="M 370 92 L 500 128"  class="edge" marker-end="url(#arw)"/>

    <text x="575" y="272" class="sub" text-anchor="middle">no arrow in — on purpose</text>
    <text x="305" y="200" class="sub" text-anchor="middle">every arrow is a filled cell,</text>
    <text x="305" y="216" class="sub" text-anchor="middle">flagged and dropped from the vote</text>
  </svg>
  </figure>
  <p><strong>The Athletic feeds the mean as well as filling from it, and that is not
  circular.</strong> <code>MEAN_</code> averages only the cells that are genuine
  opinions, and a source's gaps are filled after it is built \u2014 so where The
  Athletic speaks it contributes, and where it is silent the mean is exactly what it
  was before. It joined on 2026-09-02 for one reason above all: <code>MEAN_</code>
  decides what a book "says" about the ~97% of players it does not price, and
  FantasyPros publishes no <code>receivingTargets</code> at all, so the basis for the
  thinnest-covered stat on the board had been ESPN averaged with a copy of itself.</p>
  <p><strong>TOMCAT has no arrow into it, and that is a decision rather than an
  oversight.</strong> Filling the one genuinely independent source from an average of
  two that are not would turn it into a copy of them — the exact double-counting the
  flags exist to prevent. Its gaps stay null, stay flagged, and simply do not
  vote.</p>
</section>"""


ALL_STATS: Tuple[str, ...] = (
    "passingAttempts", "passingYards", "passingTouchdowns", "passingInterceptions",
    "rushingAttempts", "rushingYards", "rushingTouchdowns",
    "receivingTargets", "receivingReceptions", "receivingYards",
    "receivingTouchdowns",
)


def cards_section(df: pl.DataFrame) -> str:
    """One card per source: mechanism hand-written, every number measured."""
    key_stats = ("passingYards", "rushingYards", "receivingYards")
    blocks = []
    for source in SOURCES:
        card = SOURCE_CARDS[source]
        cov = []
        for stat in key_stats:
            mask = real_mask(df, source, stat)
            if mask is None:
                cov.append([f"<code>{esc(stat)}</code>",
                            '<span class="muted">no column</span>',
                            '<span class="muted">—</span>'])
                continue
            real = int(df.select(mask.sum()).item() or 0)
            cov.append([f"<code>{esc(stat)}</code>", f"{real:,}", pct(real, df.height)])

        strengths = "".join(f"<li>{s}</li>" for s in card["strengths"])
        weaknesses = "".join(f"<li>{w}</li>" for w in card["weaknesses"])
        weight = float(WEIGHTS["default"].get(source, 0.0))
        badge = ("" if weight else
                 '<span class="badge reject">weight 0.0</span>')
        blocks.append(f"""
  <div class="experiment" id="src-{esc(source.lower())}">
    <h3><code>{esc(source)}_</code> &nbsp;{card["name"]}{badge}</h3>
    <div class="scroll"><table><tbody>
      <tr><th>ingest</th><td>{card["ingest"]}</td></tr>
      <tr><th>code</th><td>{card["where"]}</td></tr>
      <tr><th>lands in</th><td>{card["file"]}</td></tr>
      <tr><th>available</th><td>{card["when"]}</td></tr>
      <tr><th>depends on</th><td>{card["depends"]}</td></tr>
      <tr><th>carries</th><td>{card["carries"]}</td></tr>
    </tbody></table></div>
    <h4>Coverage — rows with a real, non-imputed line</h4>
    {table(["stat", "real rows", "of board"], cov, 1)}
    <div class="twoup">
      <div><h4>Strengths</h4><ul>{strengths}</ul></div>
      <div><h4>Weaknesses and bias</h4><ul>{weaknesses}</ul></div>
    </div>
  </div>""")

    return f"""
<section id="cards">
  <h2>The sources, one at a time</h2>
  <p>Mechanism is hand-written and does not change nightly. Every number —
  coverage here, bias below — is measured off the board this page was rendered
  from.</p>
  {"".join(blocks)}
</section>"""


def bias_section(df: pl.DataFrame) -> str:
    """Each source's stat distribution against ESPN/FantasyPros, per position."""
    blocks = []
    for position in POSITIONS:
        stat_tables = []
        for stat in STATS_BY_POSITION[position]:
            rows = bias_rows(df, position, stat)
            if not rows:
                continue
            stat_tables.append(
                f'<h4>{esc(position)} &middot; <code>{esc(stat)}</code></h4>'
                + table(["source", "n", "mean", "median", "p10–p90 spread"], rows, 1))
        if stat_tables:
            blocks.append(f'<h3>{esc(position)}</h3>' + "".join(stat_tables))

    return f"""
<section id="bias">
  <h2>Which way each source is biased</h2>
  <p>Every number is a <strong>ratio against the ESPN/FantasyPros mean on exactly
  the rows that source was real for</strong>. Above 1.000 means it projects more of
  that stat than ESPN and FantasyPros do about the same players; below means less.
  The <code>p10–p90 spread</code> column is the same comparison applied to the width
  of the distribution rather than its centre, so a source that agrees on the average
  player but is too timid at the top reads as a low number there and a 1.000 beside
  it.</p>
  <p><strong>The pairing is the method.</strong> A source's real cells are a biased
  sample of the pool — a book prices the players worth pricing — so comparing its 30
  lines against a 500-row baseline would measure coverage and call it bias. The
  baseline is restricted to the source's own rows every time.</p>
  <p><strong>Gated at {GATE_POINTS:.0f} projected points by ESPN or
  FantasyPros</strong>, and built from those two only so no source can be accused of
  selecting the rows that flatter it. Ungated, percentage disagreement divides by the
  projection, so two sources eight and thirteen points apart on a third-string
  quarterback read as 47% disagreement about nobody — and that noise swamps every
  row that matters.</p>
  <p class="note">Stat-level bias is <strong>league-independent</strong>: these are
  raw stat lines, and nothing here has been scored. Unlike every points number on
  this page, this section holds for all nine leagues.</p>
  <p class="note"><strong>Read the ESPN and FantasyPros rows as the reference.</strong>
  They bracket the baseline by construction — where both are real on the same rows
  their two mean ratios sum to exactly 2.000, and they differ only where one of them
  is null and the other is not. How far apart they sit is how far apart the two
  sources everyone already trusts are, on this stat, at this position. A third source
  sitting closer to 1.000 than that gap is not disagreeing enough to be worth
  adjusting for.</p>
  <p><strong>A ratio here is not always a level bias, and the rushing rows are the
  worked example.</strong> The Athletic reads <strong>0.77</strong> on
  <code>rushingAttempts</code>, which looks like a source that is bearish on running
  backs. It is not. Summed to <em>team</em> rushing budgets it sits at
  <strong>0.970</strong> of ESPN \u2014 the two agree almost exactly on how often a team
  runs. What they disagree about is who gets the ball: the lead back's share of his
  own backfield is <strong>0.675</strong> for ESPN and <strong>0.645</strong> for The
  Athletic, so on backs ESPN gives 150+ carries it reads <strong>0.923</strong> and on
  backs ESPN gives under 60 it reads <strong>1.495</strong>. It flattens backfields.</p>
  <p><strong>The 100-point gate is what turns that into a level ratio</strong>, and it
  is worth knowing before adjusting anything. The gate keeps the top of every room and
  drops the bench, so a source that redistributes <em>within</em> a room shows up here
  as though it were low on the whole stat. Correcting that with a multiplier would be
  precisely wrong \u2014 it would restore the starters and leave the committee view,
  which is the actual opinion, uncorrected and now double-counted. Read a rushing row
  next to the team-budget check, not on its own.</p>
  <p class="note">Note what the two reference rows do <em>not</em> show: a level
  difference. Across gated receivers their mean receiving yards agree to within 0.1%,
  while individual players disagree by around 10%. <strong>ESPN and FantasyPros
  disagree about players, not about totals</strong> — so a source that is off at the
  aggregate is saying something neither of them is.</p>
  {"".join(blocks)}
</section>"""


def weights_section(df: pl.DataFrame) -> str:
    """Nominal 0.25 against what renormalisation actually produced."""
    blocks = []
    for position in POSITIONS:
        rows = []
        for stat in STATS_BY_POSITION[position]:
            realised = realised_weights(df, position, stat)
            row = [f"<code>{esc(stat)}</code>"]
            for source in ("ESPN", "FP", "PINNY", "BOL", "ATH", "USG"):
                value = realised.get(source)
                row.append('<span class="muted">—</span>' if value is None
                           else f"{value:.3f}")
            rows.append(row)
        if rows:
            blocks.append(
                f'<h3>{esc(position)}</h3>'
                + table(["stat", "ESPN", "FP", "PINNY", "BOL", "ATH", "USG"], rows, 1))

    return f"""
<section id="weights">
  <h2>The weight that is nominal, and the weight that is real</h2>
  <p>Every universal source carries a nominal <strong>0.25</strong>. Almost none of
  them ever gets it. Because the nominal figures are identical,
  renormalising over the sources that are <em>real</em> on a row makes the rule
  literal — <strong>one equal vote per source that has an opinion</strong>. Four real
  sources weight 0.25 each, three weight 0.333, two weight 0.5.</p>
  <p>So this table, not the 0.25, is what to reason with when deciding whose bias is
  worth correcting. A source biased 20% low on a stat where it realises 0.05 of the
  weight is moving the board by one point in a hundred.</p>
  {"".join(blocks)}
  <p class="note">Averaged per row over gated players, so it reads as "what did the
  typical player's blend look like" rather than what the pooled totals did.</p>
  <p><strong>The volume rows used to be the thin ones, and volume is the half that
  persists.</strong> Until 2026-09-02 <code>receivingTargets</code> was a
  <em>two</em>-source blend \u2014 ESPN and The Athletic at 0.500 each \u2014 because
  FantasyPros publishes no targets column, TOMCAT emitted no volume terms, and neither
  book prices a target. That was the thinnest coverage on the board sitting on its most
  forecastable quantity: carries per game persist year over year at
  <strong>+0.895</strong> against <strong>+0.260</strong> for the efficiency rate they
  get multiplied by. <strong>TOMCAT now publishes the volume it always modelled</strong>
  \u2014 every stat it emits is a volume term \u00d7 a rate, so the prediction existed
  and was being carried as a diagnostic and discarded \u2014 which gives targets, carries
  and pass attempts a third independent voter. The rows below are after that change.</p>
</section>"""


# --------------------------------------------------------------------------- #
# Sample players
# --------------------------------------------------------------------------- #


def _cell_is_real(row: Dict, source: str, stat: str) -> bool:
    """Whether one cell holds a genuine line, by the same rule as :func:`real_mask`.

    Shared rather than repeated, because the two ``None`` cases mean opposite things
    and conflating them is the easy mistake: *no flag column* means the source is
    never imputed (ESPN), while *a null flag* means the row never joined, which is an
    absence. One reads as real and the other must not.
    """
    flag = f"{source}_{stat}{IMPUTED_SUFFIX}"
    if flag not in row:
        return True
    return row[flag] is False

def _cell(row: Dict, source: str, stat: str, decimals: int = 1) -> str:
    """One stat cell, marked when the number was filled in rather than projected."""
    column = f"{source}_{stat}"
    if column not in row:
        return '<span class="muted">—</span>'
    value = row[column]
    if value is None:
        return '<span class="muted">—</span>'
    text = f"{value:,.{decimals}f}"
    if _cell_is_real(row, source, stat):
        return text
    origin = IMPUTED_FROM.get(source, "MEAN")
    return (f'<span class="imp" title="imputed from {esc(origin)}">'
            f'{text}<sup>i</sup></span>')


def _line_status(row: Dict, source: str, stats: Sequence[str]) -> str:
    """How much of a player's line this source actually projected."""
    present = [s for s in stats if f"{source}_{s}" in row]
    if not present:
        return '<span class="muted">no columns</span>'
    real = 0
    seen = 0
    for stat in present:
        column = f"{source}_{stat}"
        if row[column] is None:
            continue
        seen += 1
        if _cell_is_real(row, source, stat):
            real += 1
    if not seen:
        return '<span class="muted">no line</span>'
    if real == seen:
        return f"real, {real}/{seen}"
    origin = IMPUTED_FROM.get(source, "MEAN")
    if real == 0:
        return (f'<span class="imp">imputed &larr; {esc(origin)}</span> '
                f'<span class="muted">(0/{seen})</span>')
    return (f'<span class="imp">part imputed &larr; {esc(origin)}</span> '
            f'<span class="muted">({real}/{seen} real)</span>')


def player_block(df: pl.DataFrame, name: str, position: str,
                 ranks: Dict[str, Dict[int, int]]) -> str:
    """One sample player, every source side by side."""
    indexed = df.with_row_index("_row")
    match = indexed.filter(pl.col("player_name") == name)
    if not match.height:
        return f"""
  <div class="experiment">
    <h3>{esc(name)} <span class="badge reject">not on the board</span></h3>
    <p><strong>No source carries him.</strong> He is not in ESPN's player universe
    for this league, and ESPN <em>is</em> the universe — a player it does not list
    cannot appear on a board however many other sources have an opinion. This is a
    coverage hole rather than a projection of zero, and the two are worth keeping
    apart.</p>
  </div>"""

    row = match.row(0, named=True)
    index = int(row["_row"])
    stats = STATS_BY_POSITION.get(position, ALL_STATS)

    points_rows = []
    for source in SOURCES:
        column = f"{source}_Points"
        if column not in row:
            continue
        value = row[column]
        rank = ranks.get(source, {}).get(index)
        points_rows.append([
            f"<code>{esc(source)}</code>",
            '<span class="muted">—</span>' if value is None else f"{value:,.1f}",
            '<span class="muted">—</span>' if rank is None
            else f"{esc(position)}{rank}",
            _line_status(row, source, stats),
        ])
    true_rank = ranks.get("TRUE", {}).get(index)
    points_rows.append([
        "<code>TRUE</code>",
        '<span class="muted">—</span>' if row.get("TRUE_Points") is None
        else f"<strong>{row['TRUE_Points']:,.1f}</strong>",
        '<span class="muted">—</span>' if true_rank is None
        else f"<strong>{esc(position)}{true_rank}</strong>",
        "the blend",
    ])

    columns = ["ESPN", "FP", "MEAN", "PINNY", "BOL", "ATH", "USG", "TRUE"]
    stat_rows = []
    for stat in stats:
        decimals = 1 if "Yards" in stat or "Attempts" in stat else 2
        stat_rows.append([f"<code>{esc(STAT_LABELS.get(stat, stat))}</code>"]
                         + [_cell(row, source, stat, decimals) for source in columns])

    meta = []
    if row.get("pro_team"):
        meta.append(esc(str(row["pro_team"])))
    if row.get("adp") is not None:
        meta.append(f"ADP {row['adp']:.1f}")
    if row.get("sources_real") is not None:
        meta.append(f"{int(row['sources_real'])} real sources")
    if row.get("bye_week") is not None:
        meta.append(f"bye {int(row['bye_week'])}")

    evidence = row.get("avail_evidence")
    note = ""
    if evidence:
        note = (f'<p class="verdict"><strong>Availability gate fired:</strong> '
                f'<code>{esc(str(evidence))}</code>. Every non-ESPN source was '
                f'withdrawn, which is why the line above is empty rather than '
                f'confidently wrong — a single surviving source would otherwise '
                f'carry 100% of the projection.</p>')

    return f"""
  <div class="experiment">
    <h3>{esc(name)}</h3>
    <p class="note">{" &middot; ".join(meta)}</p>
    {table(["source", "points", "pos rank", "line"], points_rows, 1)}
    <h4>The stat line each source actually published</h4>
    {table(["stat"] + columns, stat_rows, 1)}
    <p class="note"><sup>i</sup> marks a filled cell. Compare it against the
    <code>MEAN</code> column — where they match, the source had no opinion and the
    number you are looking at is ESPN and FantasyPros averaged.</p>
    {note}
  </div>"""


def sample_section(df: pl.DataFrame, league: str) -> str:
    """The named players, rendered in full."""
    ranks = {source: source_ranks(df, source) for source in tuple(SOURCES) + ("TRUE",)}
    blocks = []
    for position, names in SAMPLE_PLAYERS:
        players = "".join(player_block(df, name, position, ranks) for name in names)
        blocks.append(f"<h3>{esc(position)}</h3>{players}")

    return f"""
<section id="samples">
  <h2>Twenty-two players, every source side by side</h2>
  <p>A mix rather than a top list: the consensus best at each position, the loudest
  disagreements, and the rows where availability is the whole question — injury,
  suspension, or a rookie nobody has a prior season for. These are where the holes
  show.</p>
  <p class="note">Points and positional ranks below are
  <strong>{esc(league)}</strong>'s, scored in that league's own rules. They do not
  transfer to another league; the stat lines do.</p>
  {"".join(blocks)}
</section>"""


# --------------------------------------------------------------------------- #
# How a projection is built
# --------------------------------------------------------------------------- #

#: The player the arithmetic is walked through, and the stat it is walked through on.
#:
#: Chosen because every step is visible on one row: two sources real, two imputed
#: from the mean of those two, one abstaining entirely, and a reconciliation pass
#: that moves the answer afterwards. Falls back to any row with the same shape if he
#: is not on the board, so the section cannot go blank between seasons.
WORKED_PLAYER = "Deshaun Watson"
WORKED_STAT = "passingYards"


def _pick_worked_row(df: pl.DataFrame) -> Optional[Dict]:
    """The sample row for the arithmetic: some sources real, some imputed."""
    stat = WORKED_STAT
    named = df.filter(pl.col("player_name") == WORKED_PLAYER)
    if named.height:
        return named.row(0, named=True)
    real = real_mask(df, "ESPN", stat)
    imputed = real_mask(df, "PINNY", stat)
    if real is None or imputed is None:
        return None
    candidates = df.filter(real & ~imputed & (pl.col(f"MEAN_{stat}") > 0))
    return candidates.row(0, named=True) if candidates.height else None


def worked_example(df: pl.DataFrame) -> str:
    """The blend arithmetic on one real row, start to finish."""
    row = _pick_worked_row(df)
    if row is None:
        return ""
    stat = WORKED_STAT
    weights = WEIGHTS["default"]

    # Two passes, and the order matters: the realised weight of a source is its
    # share of the *final* denominator, so accumulating one while printing the other
    # would show each source the denominator as it stood when its row was written.
    contributions = []
    numerator = 0.0
    denominator = 0.0
    for source in SOURCES:
        column = f"{source}_{stat}"
        if column not in row:
            continue
        value = row[column]
        weight = float(weights.get(source, 0.0))
        if value is None:
            contributions.append((source, None, "abstains", weight, False))
            continue
        is_real = _cell_is_real(row, source, stat)
        if is_real:
            numerator += float(value) * weight
            denominator += weight
        contributions.append((source, float(value), "real" if is_real else "imputed",
                              weight, is_real))

    rows = []
    for source, value, status, weight, is_real in contributions:
        if status == "imputed":
            shown = (f'<span class="imp">imputed &larr; '
                     f'{esc(IMPUTED_FROM.get(source, "MEAN"))}</span>')
        else:
            shown = status
        rows.append([
            f"<code>{esc(source)}</code>",
            '<span class="muted">—</span>' if value is None else f"{value:,.1f}",
            shown,
            f"{weight:.2f}",
            (f"{weight / denominator:.3f}" if is_real and denominator
             else '<span class="muted">0 — dropped</span>'),
        ])

    # `MEAN_` is shown even though it never votes, because it is the number every
    # imputed cell above was filled from and the point is for a reader to match them
    # by eye rather than take it on trust.
    if row.get(f"MEAN_{stat}") is not None:
        rows.append(["<code>MEAN</code>", f"{row[f'MEAN_{stat}']:,.1f}",
                     '<span class="muted">derived, never votes</span>',
                     '<span class="muted">n/a</span>',
                     '<span class="muted">n/a</span>'])

    blended = numerator / denominator if denominator else None
    true_value = row.get(f"TRUE_{stat}")
    mean_value = row.get(f"MEAN_{stat}")
    espn_value = row.get(f"ESPN_{stat}")
    fp_value = row.get(f"FP_{stat}")

    drift = ""
    if blended is not None and true_value is not None:
        delta = true_value - blended
        drift = (
            f"<p><strong>The blend is not the last step.</strong> The weighted "
            f"average above is <strong>{blended:,.1f}</strong>; the board's "
            f"<code>TRUE_{esc(stat)}</code> is <strong>{true_value:,.1f}</strong>, "
            f"a move of <strong>{delta:+,.1f}</strong>. That gap is "
            f"<code>reconcile_team_totals</code>, which scales a team's passing and "
            f"receiving sides to their midpoint so the line describes a team that "
            f"could actually have produced it. No single source is complete enough "
            f"to hold that identity alone, which is why it happens after the blend "
            f"rather than before.</p>")

    mean_note = ""
    if None not in (espn_value, fp_value, mean_value):
        mean_note = (
            f"<p>Read the <code>MEAN</code> row against the imputed ones: "
            f"<code>({espn_value:,.1f} + {fp_value:,.1f}) / 2 = "
            f"{mean_value:,.1f}</code>, and every imputed cell is that number "
            f"exactly. That is what a filled cell <em>is</em> — not a projection, "
            f"but the two sources it was filled from, which is precisely why it "
            f"must not vote.</p>")

    return f"""
  <h3>The same thing on one real row</h3>
  <p><strong>{esc(str(row.get("player_name", "")))}</strong>,
  <code>{esc(stat)}</code>:</p>
  {table(["source", "value", "status", "nominal w", "realised w"], rows, 1)}
  {mean_note}
  {drift}"""


def calculation_section(df: pl.DataFrame) -> str:
    """The order the pipeline actually runs in, and what comes out."""
    return f"""
<section id="calculation">
  <h2>How a final projection is built</h2>
  <p><strong>Sources are blended as stat lines and scored afterwards.</strong> That
  one decision is what lets a single pipeline serve nine leagues with different
  rules: there is one blended stat line per player, and nine different sets of rules
  priced against it. It also means the headline identity —</p>
  <p class="formula"><code>TRUE_Points = score(TRUE_ stat line)</code>, <em>not</em>
  <code>mean(ESPN_Points, FP_Points, …)</code></p>
  <p>— and the two are genuinely different numbers. Averaging the points columns on
  this page will not reproduce the blend, and it is not supposed to.</p>

  <h3>The weighting rule</h3>
  <p class="formula"><code>TRUE_&lt;stat&gt; = &Sigma;(w<sub>s</sub> &middot;
  x<sub>s</sub> &middot; real<sub>s</sub>) / &Sigma;(w<sub>s</sub> &middot;
  real<sub>s</sub>)</code></p>
  <p>where <code>real<sub>s</sub></code> is 0 when the cell was filled in and 1 when
  the source genuinely projected it. A source with no column at all is skipped from
  <em>both</em> sums — an earlier bug skipped it from the numerator only, which
  halved projections purely because Pinnacle was absent. Where every source for a
  stat is imputed the denominator is zero and it falls back to the face-value sum,
  which cannot happen in production because ESPN always carries weight and is never
  imputed.</p>

  <h3>The order it runs in</h3>
  <ol>
    <li>Load every source into <code>&lt;SRC&gt;_&lt;stat&gt;</code> columns,
        joining on a disambiguated name key — or on <code>player_id</code> for
        TOMCAT, the only source with ids.</li>
    <li><code>_apply_injury_adjustment</code> and
        <code>_withdraw_usage_on_role</code> — TOMCAT alone is scaled for expected
        games, because ESPN and FantasyPros already price a known absence.</li>
    <li><strong><code>_withdraw_sources_on_availability</code></strong> — three
        gates. Out for the season; ESPN prices zero <em>and</em> he is out; ESPN
        prices zero <em>and</em> only one source is left. Two sources disagreeing
        with an ESPN zero are deliberately left alone.</li>
    <li><code>impute_columns</code> — fill the holes, flag every filled cell.</li>
    <li><code>_make_usage_coherent</code> — rescale TOMCAT to a full 17-game slate
        so all six sources describe the same quantity.</li>
    <li><strong><code>compute_weighted_stats</code></strong> — the formula above.</li>
    <li><code>reconcile_team_totals</code> — close the team identities.</li>
    <li><code>redistribute</code> (vacancy transfer), then
        <code>reconcile_team_totals</code> <strong>again</strong> — a back who
        inherits receiving work was thrown to by somebody, and the identity is what
        carries the matching volume onto the quarterbacks who threw it.</li>
    <li><code>attach_milestone_bands</code> — after the blend, because a milestone
        bonus is a step function of a weekly quantity rather than a rate on a season
        total.</li>
    <li><code>proj_to_score</code> — every source's line and the blend, priced
        through each league's own rules, so they can be compared rather than only
        the blend published.</li>
    <li>VOR, tiers, <code>vor_rank</code>, dollar allocation; Jake's Ranks attached
        last as a display overlay that changes nothing.</li>
  </ol>
  {worked_example(df)}
</section>"""


def tools_section(df: pl.DataFrame) -> str:
    """The draft board and the weekly view do not see the same sources."""
    rows = []
    for source in SOURCES:
        weekly = source in WEEKLY_PREFIXES
        note = {
            "BOL": "In the weekly list, but the weekly scraper has been 403 since "
                   "before this season — so the column exists and the data does not.",
            "ATH": "Season-only file. No weekly stat line exists to score.",
            "USG": "No weekly head. Scoring it weekly wrote a column that was null "
                   "3,602 times out of 3,602.",
        }.get(source, "")
        rows.append([
            f"<code>{esc(source)}</code>",
            str(SOURCE_CARDS[source]["name"]),
            "yes",
            ("yes" if weekly and source != "BOL"
             else '<span class="badge reject">'
                  + ("listed, no data" if source == "BOL" else "no")
                  + "</span>"),
            note or '<span class="muted">—</span>',
        ])

    return f"""
<section id="tools">
  <h2>Two tools, and they do not see the same sources</h2>
  <p>This is the largest structural asymmetry in the system and it is easy to miss,
  because both views render the same column names.</p>
  {table(["prefix", "source", "draft board", "weekly view", "why"], rows, 2)}
  <p><strong>Three of the six votes that build a draft board are structurally absent
  week to week</strong>, and a fourth has no live data. The draft board is a
  six-source blend; the weekly view is an ESPN/FantasyPros blend with a thin
  sportsbook overlay. They are not the same instrument and should not be read as
  though a number carried the same weight of evidence in both.</p>
  <p>The gap is named work rather than an oversight —
  <code>docs/plans/19-weekly-usage-model.md</code> is the weekly usage head, and it
  is where the larger edge is thought to be, precisely because the weekly path is
  the thinner of the two.</p>
</section>"""


# --------------------------------------------------------------------------- #
# What would make it more accurate
# --------------------------------------------------------------------------- #

def _gainwell_state(df: pl.DataFrame) -> str:
    """Whether the known Athletic join miss is still live on this board.

    Checked rather than asserted, so the page stops claiming a bug the moment the
    one-line alias lands.
    """
    match = df.filter(pl.col("player_name").str.to_uppercase()
                      .str.contains("GAINWELL"))
    if not match.height:
        return ("Not on this board, so nothing to check here — the miss was measured "
                "on a league whose pool carries him.")
    row = match.row(0, named=True)
    flag = row.get("ATH_rushingYards" + IMPUTED_SUFFIX)
    points = row.get("TRUE_Points")
    total = f"{points:,.1f}" if points is not None else "—"
    if flag:
        return (f"<strong>Still live on this board.</strong> His "
                f"<code>ATH_rushingYards</code> is flagged imputed, so a "
                f"{total}-point back's real Athletic line is the ESPN/FantasyPros "
                f"mean wearing an Athletic badge.")
    return ("<strong>Resolved on this board</strong> — his Athletic line is real. "
            "The alias has landed.")


def improve_section(df: pl.DataFrame) -> str:
    """Ranked, each tied to a measurement rather than an instinct."""
    items = [
        ("The Athletic's Kenny Gainwell join miss",
         f"The workbook spells him <em>Kenneth</em>, so he silently fell through the "
         f"name join and his cells were filled from <code>MEAN_</code> instead. "
         f"{_gainwell_state(df)} One line in <code>NAME_ALIASES</code> fixes it. It "
         f"is deliberately unapplied in draft week because it moves "
         f"<code>TRUE_Points</code>, and that is a decision to take on purpose "
         f"rather than as a side effect of a docs change."),
        ("De-vig Pinnacle's season props",
         "The season path pivots on the raw <code>line</code> and never reads the "
         "<code>no_vig_over_prob</code> its own scraper already computes. "
         "BetOnline's season path uses a juice-adjusted <code>True_Line</code>, and "
         "Pinnacle's <em>weekly</em> path inverts the line properly — so the draft "
         "board is the one place in the system where a book's vig is left in. The "
         "arithmetic already exists; it is a wiring gap, not a modelling one."),
        ("Measure The Athletic out of sample, on a season basis",
         "It carries a full equal vote and its per-stat MAE has never been "
         "measured. The existing harness cannot do it: <code>lab.accuracy</code>, "
         "<code>usage.gates</code> and <code>g1_season</code> all score "
         "<em>player-week</em> rows out of <code>lineups.parquet</code>, and this "
         "source is season-only with no column there. <strong>That is the trap</strong> "
         "— run as-is, they would produce a clean table in January that looked like "
         "the source had been judged when it had not. What is owed is a season-basis "
         "harness scoring <code>board.parquet</code>'s <code>ATH_&lt;stat&gt;</code> "
         "against realised season totals."),
        ("Fix <code>rushingTouchdowns</code>, the one stat the blend loses on",
         "Scored per stat, the shipping blend is worse than ESPN, FantasyPros "
         "<em>and</em> Pinnacle on rushing touchdowns, on all three populations, "
         "while the fantasy-point MAE for the same league-season reads a clean win. "
         "Points MAE cannot see it because yardage carries the variance — which is "
         "the general lesson: <strong>a points-level metric will hide a stat-level "
         "defect every time</strong>, and this page's bias tables exist to be read "
         "at the stat level for that reason."),
        ("Do <em>not</em> correct TOMCAT's receiving spread on its own",
         "It is under-spread on receiving volume — RB receptions 1.315, TE "
         "receiving yards 1.370 against realised — and it is also the "
         "best-calibrated single source in the blend. FantasyPros runs 0.65–0.95 "
         "and BetOnline 0.21–0.81 in the <em>opposite</em> direction, so the errors "
         "partly cancel and fixing one arm in isolation makes the board worse. This "
         "is the strongest argument on the page for adjusting sources "
         "<em>jointly</em>, against the blended line, rather than one at a time."),
        ("Build the weekly heads",
         "Half the sources vanish week to week. The weekly usage head is the named "
         "work and is where the larger edge is thought to be."),
        ("Re-run the kicking gate now the column names are right",
         "The arm was held at 0.0 on G-K2, and the points that made it look "
         "unusable were a defect rather than a model: it emitted "
         "<code>madeFieldGoalsFromFrom40To49</code> for two of three distance bands, "
         "which no league scores, so two thirds of a kicker's field-goal value was "
         "silently zero and the arm read at <strong>0.577\u00d7 ESPN</strong>. Corrected "
         "it reads 0.946\u00d7 and it now votes. <strong>G-K2 was measured on stats, so "
         "the typo does not overturn it</strong> \u2014 but it was last run against a "
         "model whose own report tables were built from the miswritten columns, and "
         "re-running it is cheap. Until then a kicker's number is a deliberate "
         "override of a failed gate."),
        ("Give Jake's Ranks somewhere to go",
         "The hand ranking is display-only because the blend works in stat space "
         "and an ordering has nowhere to sit in it. But its shape is informative: "
         "against the order his own projections imply, <strong>no quarterback moves "
         "five spots and 55 of 120 receivers do</strong> — he leaves alone the "
         "position a projection handles best and reworks the two it handles worst. "
         "That is the same inversion the 100-point gate finds in the stat lines, "
         "reached independently, and it is an argument for using it as a tier prior "
         "at receiver rather than a column."),
    ]
    blocks = "".join(
        f"<li><strong>{title}</strong><br>{body}</li>" for title, body in items)
    return f"""
<section id="improve">
  <h2>What would make this more accurate</h2>
  <p>Ranked by how much they would move a projection against how well understood
  they are. Each is tied to a measurement already in the repo rather than an
  instinct.</p>
  <ol class="spaced">{blocks}</ol>
</section>"""


EXTRA_STYLE = """
.twoup { display: flex; flex-wrap: wrap; gap: 1.6rem; }
.twoup > div { flex: 1 1 20rem; min-width: 0; }
.twoup ul { margin: .3rem 0; }
.twoup li { margin-bottom: .45rem; }
ol.spaced > li { margin-bottom: .9rem; }
sup { font-size: .7em; }
.imp { color: var(--negative); }
.over { color: var(--positive); font-weight: 600; }
.under { color: var(--negative); font-weight: 600; }
.formula { background: var(--surface); border: 1px solid var(--border);
           border-radius: 8px; padding: .7rem .9rem; font-size: .95rem; }
.node { fill: var(--surface); stroke: var(--axis); stroke-width: 1.25; }
.node.root { stroke: var(--ink-2); stroke-width: 2; }
.node.derived { stroke-dasharray: 4 3; }
.node.alone { stroke: var(--positive); stroke-width: 2; }
.node.feeds { stroke: var(--ink-2); stroke-width: 1.8; }
.lbl { font-size: 13px; fill: var(--ink); font-weight: 600; }
.sub { font-size: 10.5px; fill: var(--muted); }
.edge { stroke: var(--axis); stroke-width: 1.4; fill: none; }
.head { fill: var(--axis); }
.toc { display: flex; flex-wrap: wrap; gap: .4rem 1.1rem; padding: 0; margin: 1rem 0;
       list-style: none; font-size: .9rem; }
"""

STYLE = BASE_STYLE + EXTRA_STYLE


# --------------------------------------------------------------------------- #
# The page
# --------------------------------------------------------------------------- #

TOC = (
    ("register", "The register"),
    ("dependency", "Which depends on which"),
    ("cards", "The sources"),
    ("bias", "Bias by stat and position"),
    ("weights", "Nominal vs realised weight"),
    ("samples", "Twenty-two players"),
    ("calculation", "How a projection is built"),
    ("tools", "Draft vs weekly"),
    ("improve", "What to improve"),
)


def render(df: pl.DataFrame, season: int, league: str) -> str:
    """Assemble the page.

    Args:
        df: One league's board.
        season: Season year, for the stamp.
        league: League key, for the stamp and the points caveats.

    Returns:
        str: A complete HTML document.
    """
    stamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    gated = df.filter(gate_expr(df)).height
    links = "".join(f'<li><a href="#{esc(key)}">{esc(label)}</a></li>'
                    for key, label in TOC)

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Every source that becomes a projection</title>
<style>{STYLE}</style>
</head>
<body>
<main>
  <h1>Every source that becomes a projection</h1>
  <p class="lede">What each one is, how it gets here, when it is available, what it
  depends on, and which way it is biased — so that adjusting a stat line is a
  decision with evidence behind it rather than a guess about whose number looks
  wrong.</p>
  <p class="note">Generated by <code>python -m Scripts.lab.sources</code> from
  <code>Data/Store/{esc(str(season))}/{esc(league)}/board.parquet</code>
  ({df.height:,} players, {gated:,} clearing the {GATE_POINTS:.0f}-point gate) on
  {esc(stamp)}. Do not edit by hand — re-run it. Companion to
  <a href="projection_pipeline.html">projection_pipeline.html</a>, which is the
  narrative of how the blend works; this is the reference for what goes into it.</p>
  <ul class="toc">{links}</ul>
  <p class="note"><strong>Two reading rules.</strong> Points and ranks are
  <em>{esc(league)}</em>'s, scored in that league's own rules — they never transfer
  to another league. Stat lines and the bias tables are unscored and hold
  everywhere. And a board is a snapshot: rosters, depth charts, injuries and ADP all
  move daily through camp, so quote these with the date above and read the shape
  rather than the last digit.</p>
  {register_section(df, league)}
  {dependency_section()}
  {cards_section(df)}
  {bias_section(df)}
  {weights_section(df)}
  {sample_section(df, league)}
  {calculation_section(df)}
  {tools_section(df)}
  {improve_section(df)}
</main>
</body>
</html>
"""


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.lab.sources",
        description="Render the projection source atlas to HTML.")
    parser.add_argument("--league", default=DEFAULT_LEAGUE,
                        help=f"store league key (default {DEFAULT_LEAGUE})")
    parser.add_argument("--season", type=int, default=DEFAULT_SEASON,
                        help=f"season year (default {DEFAULT_SEASON})")
    parser.add_argument("--out", default=str(OUTPUT_PATH))
    args = parser.parse_args(argv)

    df = load_board(args.season, args.league)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(df, args.season, args.league))
    print(f"wrote {out} from {args.league} {args.season} ({df.height:,} players)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
