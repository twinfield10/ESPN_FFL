"""Season-long projections: the draft-board input (roadmap Phase 2).

Pre-season there are no weekly markets, so ``clean_lineups`` has nothing to blend.
What *does* exist is season-long data from all four sources, which is what a draft
board wants anyway. This module is the season-long counterpart to
``clean_lineups``: same provenance-and-renormalisation machinery, same per-league
scoring, one row per player instead of per player-week.

Sources, and the trap in each:

- **ESPN** -- ``player.stats[0]['projected_breakdown']`` mixes units. Counts
  (receptions, attempts, touchdowns) are season totals, but **yardage is a
  per-game average**. Verified against two independent cross-checks: for Puka
  Nacua, ``receivingYards`` reads 93.5 while ``receivingReceptions x
  receivingYardsPerReception`` gives 1590 and the every-5-receiving-yards counter
  gives 1585. Yardage is multiplied by games played (stat ``210``) here. Blending
  the raw value against a season-long book line would put ESPN 17x low.
- **FantasyPros** -- ``week=draft`` gives season lines, but the public table is
  capped at 10 rows per position (60 players). Real for those, absent elsewhere.
- **BetOnline** -- the only source with IDP props. Its descriptions are typed by
  hand and contain typos (``Receiving Yrads``, ``Passing INT's``,
  ``Tackles & Assist``), combined ``Receiving & Rushing`` markets, and rows where
  the player/stat split failed upstream and left ``player == 'UNKNOWN'`` with the
  name embedded in the stat text. All handled in :func:`normalise_bol_props`.
- **Pinnacle** -- clean, but only 76 props and offence-only.

Names are matched on a normalised key rather than raw strings: BetOnline is
uppercase, and contains real misspellings (``Dalton Kinciad``).
"""

import re
import unicodedata
from typing import Dict, List, Optional

import pandas as pd

from Scripts.config_utils import get_season
from Scripts.paths import season_dir
from Scripts.projection_utils import (
    WEIGHTS,
    compute_weighted_stats,
    impute_columns,
    print_coverage_report,
)
from Scripts.scoring import get_scoring_table

#: Stats ESPN reports per-game in the season row, needing x games played.
#:
#: Only these three. ESPN is inconsistent about it, so each entry is verified
#: against an independent cross-check rather than assumed:
#:
#: - ``receivingYards`` for Puka Nacua reads 93.5, while
#:   ``receivingReceptions x receivingYardsPerReception`` gives 1590 and the
#:   every-5-receiving-yards counter gives 1585. 93.5 x 17 = 1590. Per-game.
#: - ``rushingYards`` for Jahmyr Gibbs reads 80.83 against 283.19 attempts at 4.85
#:   per attempt = 1373, and the every-5-rushing-yards counter gives 1370. Per-game.
#: - ``passingYards`` for Josh Allen reads 232.1 against an every-25-passing-yards
#:   counter of 157 x 25 = 3925. 232.1 x 17 = 3945. Per-game.
#:
#: **Return and defensive yardage are season totals and must NOT be scaled.**
#: A D/ST unit's ``puntReturnYards`` reads 302.98 against ``puntsReturned`` of 30 --
#: 10.1 yards per return, the league average, so 302.98 is already the season
#: figure. Scaling it turned a 422-point D/ST projection into 2294.
PER_GAME_IN_SEASON_ROW = {
    "passingYards", "rushingYards", "receivingYards",
}

#: ESPN stat id for games played, present in the season breakdown.
GAMES_PLAYED_KEY = "210"

#: Fallback when the games-played counter is missing.
DEFAULT_GAMES = 17

#: A combined market that cannot be split into its components. Kept as its own
#: column rather than guessed apart -- in most leagues rushing and receiving yards
#: score at the same rate, so this is still directly usable for points, but that is
#: a scoring-time decision, not a parsing-time one.
COMBINED_YARDS = "rushingPlusReceivingYards"
COMBINED_TDS = "rushingPlusReceivingTouchdowns"

#: BetOnline stat wording -> ESPN stat name. Keys are already normalised by
#: :func:`_normalise_stat_text`, so typos and punctuation variants collapse here.
BOL_STAT_MAP = {
    "passing yards": "passingYards",
    "passing tds": "passingTouchdowns",
    "passing interceptions": "passingInterceptions",
    "rushing yards": "rushingYards",
    "rushing tds": "rushingTouchdowns",
    "receiving yards": "receivingYards",
    "receiving tds": "receivingTouchdowns",
    "receptions": "receivingReceptions",
    "sacks": "defensiveSacks",
    "interceptions": "defensiveInterceptions",
    "tackles assists": "defensiveTotalTackles",
    "receiving rushing yards": COMBINED_YARDS,
    "rushing receiving yards": COMBINED_YARDS,
    "receiving rushing tds": COMBINED_TDS,
    "rushing receiving tds": COMBINED_TDS,
}

#: Already-mapped short codes the R script emits when its own parse succeeded.
BOL_SHORT_MAP = {
    "YDS_PASS": "passingYards", "TD_PASS": "passingTouchdowns",
    "INT_PASS": "passingInterceptions",
    "YDS_RUSH": "rushingYards", "TD_RUSH": "rushingTouchdowns",
    "YDS_REC": "receivingYards", "TD_REC": "receivingTouchdowns",
    "REC_REC": "receivingReceptions",
    "SK_DEF": "defensiveSacks", "INT_DEF": "defensiveInterceptions",
    "TKL_DEF": "defensiveTotalTackles",
}

#: Known cross-source misspellings, keyed by normalised name.
NAME_ALIASES = {
    "DALTON KINCIAD": "DALTON KINCAID",
    "PATRICK MAHOMES II": "PATRICK MAHOMES",
    "GARDNER MINSHEW II": "GARDNER MINSHEW",
}

_SUFFIXES = re.compile(r"\b(JR|SR|II|III|IV|V)\b")

#: Dropped outright rather than turned into a space, so ``A.J.`` matches ``AJ`` and
#: ``De'Von`` matches ``DeVon``. Substituting a space instead would split those and
#: silently fail to join exactly the punctuated names -- A.J. Brown, Ja'Marr Chase,
#: De'Von Achane.
_DROPPED_PUNCT = re.compile(r"[.'’`]")

#: Everything else non-alphanumeric becomes a separator, so ``Amon-Ra`` matches
#: ``Amon Ra``.
_SEPARATORS = re.compile(r"[^A-Z0-9 ]")

_TEAM_TAIL = re.compile(r"\s+[A-Z]{2,3}\s*-?\s*$")


def normalise_name(name) -> Optional[str]:
    """Canonical join key for a player name.

    BetOnline is uppercase, ESPN and Pinnacle are title case, and punctuation
    differs (``A.J. Brown`` / ``AJ BROWN``). Accents, suffixes and punctuation are
    all stripped so the four sources land on one key.

    Args:
        name: Raw name from any source.

    Returns:
        str | None: Uppercase alphanumeric key, or None for missing input.
    """
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return None
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(c for c in s if not unicodedata.combining(c)).upper()
    s = _DROPPED_PUNCT.sub("", s)
    s = _SEPARATORS.sub(" ", s)
    s = _SUFFIXES.sub(" ", s)
    s = " ".join(s.split())
    return NAME_ALIASES.get(s, s)


def _normalise_stat_text(text: str) -> str:
    """Collapse BetOnline's hand-typed stat wording to a canonical phrase.

    Handles the observed typos: ``Yrads``/``Yrards`` for yards, ``IND's`` and
    ``INT's`` for interceptions, ``TD;s``/``TD"s`` for TDs, singular
    ``Reception``/``Assist``.

    Args:
        text: Raw ``stat_type`` text.

    Returns:
        str: Lowercase canonical phrase, e.g. ``"receiving yards"``.
    """
    s = str(text).lower()
    s = s.replace("yrards", "yards").replace("yrads", "yards")
    s = re.sub(r"\byard\b", "yards", s)
    s = re.sub(r"ind[''\"?;]*s\b", "interceptions", s)
    s = re.sub(r"\bint[''\"?;]*s\b", "interceptions", s)
    s = re.sub(r"\btd[''\"?;]*s?\b", "tds", s)
    s = re.sub(r"[''\"?;.,\-&]", " ", s)
    s = s.replace("total", " ")
    s = re.sub(r"\breception\b", "receptions", s)
    s = re.sub(r"\bassist\b", "assists", s)
    s = re.sub(r"\btackles\b", "tackles", s)
    return " ".join(s.split())


def _recover_player(raw_player, stat_text: str):
    """Pull the player name out of the stat text when the upstream split failed.

    Some BetOnline descriptions use irregular separators (``PHI-``, ``KC -``,
    ``-Total``) that defeated the R parser, leaving ``player == 'UNKNOWN'`` and a
    stat field like ``"De'Von Achane MIA -Total Receiving & Rushing Yards"``.

    Args:
        raw_player: The ``player`` value as parsed upstream.
        stat_text: The ``stat_type`` value, possibly name-prefixed.

    Returns:
        tuple: ``(player, stat_text)`` with the name removed from the stat text.
    """
    known = normalise_name(raw_player)
    if known and known != "UNKNOWN":
        return raw_player, stat_text

    # The stat vocabulary always starts at one of these words.
    marker = re.search(
        r"\b(Total\s+)?(Passing|Rushing|Receiving|Receptions?|Sacks|"
        r"Interceptions?|Tackles)\b", str(stat_text), flags=re.IGNORECASE)
    if not marker:
        return raw_player, stat_text

    head = str(stat_text)[:marker.start()]
    tail = str(stat_text)[marker.start():]
    head = _TEAM_TAIL.sub("", head.strip().rstrip("-").strip())
    return (head or raw_player), tail


def normalise_bol_props(df: pd.DataFrame) -> pd.DataFrame:
    """Map BetOnline's long prop file onto ESPN stat names.

    Args:
        df: ``BetOnline_SeasonProps_All.csv`` as read.

    Returns:
        pd.DataFrame: ``name_key``, ``player_name``, ``stat``, ``line``,
        ``stat_text`` -- one row per prop, with ``stat`` null where the wording is
        still unrecognised.
    """
    rows = []
    for r in df.itertuples():
        short = getattr(r, "stat_short", None)
        stat_text = getattr(r, "stat_type", "") or ""
        player = getattr(r, "player", None)

        if short in BOL_SHORT_MAP:
            stat = BOL_SHORT_MAP[short]
        else:
            player, stat_text = _recover_player(player, stat_text)
            stat = BOL_STAT_MAP.get(_normalise_stat_text(stat_text))

        line = getattr(r, "True_Line", None)
        if line is None or pd.isna(line):
            line = getattr(r, "line", None)

        rows.append({
            "name_key": normalise_name(player),
            "player_name": player,
            "stat": stat,
            "stat_text": stat_text,
            "line": line,
        })

    out = pd.DataFrame(rows)
    return out[out["name_key"].notna() & (out["name_key"] != "UNKNOWN")]


def _pivot_props(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Long props -> one row per player with ``<prefix>_<stat>`` columns.

    Args:
        df: Frame with ``name_key``, ``stat``, ``line``.
        prefix: Source prefix, e.g. ``"BOL"``.

    Returns:
        pd.DataFrame: Indexed by ``name_key``.
    """
    usable = df[df["stat"].notna() & df["line"].notna()]
    if usable.empty:
        return pd.DataFrame(columns=["name_key"])
    wide = (
        usable.pivot_table(index="name_key", columns="stat", values="line",
                           aggfunc="max")
        .rename(columns=lambda c: f"{prefix}_{c}")
        .reset_index()
    )
    wide.columns.name = None
    return wide


def espn_season_projections(league) -> pd.DataFrame:
    """Season-long ESPN projections for every rostered player and free agent.

    Yardage is converted from ESPN's per-game season-row representation to a
    season total. See the module docstring.

    Args:
        league: A live ESPN ``League``.

    Returns:
        pd.DataFrame: ``name_key``, ``player_name``, ``primaryPosition``,
        ``pro_team``, ``ESPN_projected_total``, ``games`` and ``ESPN_<stat>``.
    """
    seen, rows = set(), []

    def add(player, owner):
        if player.playerId in seen:
            return
        seen.add(player.playerId)
        season = player.stats.get(0) or {}
        breakdown = season.get("projected_breakdown") or {}
        games = float(breakdown.get(GAMES_PLAYED_KEY) or DEFAULT_GAMES) or DEFAULT_GAMES

        row = {
            "name_key": normalise_name(player.name),
            "player_name": player.name,
            "primaryPosition": player.position,
            "pro_team": player.proTeam,
            "team_owner": owner,
            "games": games,
            "ESPN_projected_total": player.projected_total_points,
        }
        for stat, value in breakdown.items():
            if not isinstance(value, (int, float)):
                continue
            if stat in PER_GAME_IN_SEASON_ROW:
                value = value * games
            row[f"ESPN_{stat}"] = float(value)
        rows.append(row)

    for team in league.teams:
        for player in team.roster:
            add(player, getattr(team, "owner", "Unknown"))

    for position in ("QB", "RB", "WR", "TE", "K", "D/ST", "LB", "DE", "CB", "S", "DT"):
        try:
            for player in league.free_agents(size=60, position=position):
                add(player, "Free Agent")
        except Exception:                    # noqa: BLE001 - a slot the league lacks
            continue

    return pd.DataFrame(rows)


def load_fantasypros_season(season: int) -> pd.DataFrame:
    """FantasyPros season-long projections, prefixed ``FP_``.

    Args:
        season: Season year.

    Returns:
        pd.DataFrame: ``name_key`` plus ``FP_<stat>`` columns. Empty if absent.
    """
    path = season_dir("FantasyPros", season, "FantasyPros_Projections_Season.parquet")
    if not path.exists():
        print(f"  FantasyPros season file missing ({path.name}); "
              f"run `python -m Scripts.scrape_FP --what season`.")
        return pd.DataFrame(columns=["name_key"])

    df = pd.read_parquet(path)
    out = pd.DataFrame({"name_key": df["player_name"].map(normalise_name)})
    for col in df.columns:
        if col.startswith("proj_"):
            out[f"FP_{col[len('proj_'):]}"] = pd.to_numeric(df[col], errors="coerce")
    return out.dropna(subset=["name_key"]).drop_duplicates("name_key")


def load_betonline_season(season: int) -> pd.DataFrame:
    """BetOnline season props, prefixed ``BOL_``.

    Args:
        season: Season year.

    Returns:
        pd.DataFrame: ``name_key`` plus ``BOL_<stat>`` columns. Empty if absent.
    """
    path = season_dir("BetOnline", season, "BetOnline_SeasonProps_All.csv")
    if not path.exists():
        print(f"  BetOnline season file missing ({path.name}); "
              f"run `Rscript R/GetSeasonProps.R {season}`.")
        return pd.DataFrame(columns=["name_key"])

    props = normalise_bol_props(pd.read_csv(path))
    unmapped = props[props["stat"].isna()]
    if not unmapped.empty:
        wordings = sorted(unmapped["stat_text"].map(_normalise_stat_text).unique())
        print(f"  BetOnline: {len(unmapped)} prop(s) with unrecognised wording, "
              f"excluded: {wordings[:6]}")
    return _pivot_props(props, "BOL")


def load_pinnacle_season(season: int) -> pd.DataFrame:
    """Pinnacle season props, prefixed ``PINNY_``.

    Args:
        season: Season year.

    Returns:
        pd.DataFrame: ``name_key`` plus ``PINNY_<stat>`` columns. Empty if absent.
    """
    path = season_dir("Pinnacle", season, "Pinnacle_SeasonProps.parquet")
    if not path.exists():
        print(f"  Pinnacle season file missing ({path.name}); "
              f"run `python -m Scripts.scrape_pinnacle_season`.")
        return pd.DataFrame(columns=["name_key"])

    df = pd.read_parquet(path).rename(columns={"player_name": "player_name"})
    df["name_key"] = df["player_name"].map(normalise_name)
    return _pivot_props(df, "PINNY")


def build_season_projections(league, season: Optional[int] = None,
                             weights: Optional[Dict] = None) -> pd.DataFrame:
    """Blend every source's season-long projection and score it for this league.

    Args:
        league: A live ESPN ``League`` -- supplies the player universe and the
            scoring rules.
        season: Projection season. Defaults to ``league.year``.
        weights: Blend weights. Defaults to :data:`Scripts.projection_utils.WEIGHTS`.

    Returns:
        pd.DataFrame: One row per player with ``ESPN_``/``FP_``/``PINNY_``/``BOL_``
        and blended ``TRUE_`` stat columns, plus ``TRUE_Points`` -- the season
        projection in this league's own scoring.
    """
    season = league.year if season is None else season
    weights = WEIGHTS if weights is None else weights

    print(f"\n===== Season projections: {league.name} {season} =====")
    base = espn_season_projections(league)
    print(f"  ESPN: {len(base)} players")

    for loader, label in (
        (load_fantasypros_season, "FantasyPros"),
        (load_pinnacle_season, "Pinnacle"),
        (load_betonline_season, "BetOnline"),
    ):
        source = loader(season)
        if source.empty or "name_key" not in source:
            continue
        matched = base["name_key"].isin(source["name_key"]).sum()
        print(f"  {label}: {len(source)} players, {matched} matched to this league")
        base = base.merge(source, on="name_key", how="left")

    # Same imputation chain as clean_lineups, so provenance and renormalisation
    # behave identically -- book columns fall back to the ESPN/FP mean.
    base = impute_columns(base, target_prefix="FP_", source_prefix="ESPN_")

    # Built as a block and concatenated once. Inserting ~45 columns individually
    # into a wide frame is what triggers pandas' fragmentation warning (plan 06).
    means = {}
    for stat_col in [c for c in base.columns if c.startswith("ESPN_")]:
        stat = stat_col[len("ESPN_"):]
        fp_col = f"FP_{stat}"
        if fp_col in base.columns:
            means[f"MEAN_{stat}"] = base[[stat_col, fp_col]].mean(axis=1)
    if means:
        base = pd.concat([base, pd.DataFrame(means, index=base.index)], axis=1)

    base = impute_columns(base, target_prefix="PINNY_", source_prefix="MEAN_")
    base = impute_columns(base, target_prefix="BOL_", source_prefix="MEAN_")

    scoring = get_scoring_table(league)
    stats = [c for c in scoring["colName"].dropna().unique()]

    print_coverage_report(base, weights_dict=weights)
    final = compute_weighted_stats(df=base, stats_list=stats, weights_dict=weights)

    # Score every source's line, so they can be compared, not just the blend.
    #
    # LIMITATION: this applies the scoring table as-is. ``proj_to_score`` additionally
    # patches per-slot values for the IDP league, because espn_api collapses ESPN's
    # per-lineup-slot scoring to one number. That patch is hardcoded and, as of
    # 2026-08-03, matches neither season's real settings (docs/plans/11). Rather than
    # copy a known-wrong workaround, IDP point totals here are left computed from the
    # unpatched table and flagged below. Offensive scoring is unaffected.
    for prefix in ("ESPN", "FP", "MEAN", "PINNY", "BOL", "TRUE"):
        points = pd.Series(0.0, index=final.index)
        for row in scoring.itertuples():
            col = f"{prefix}_{row.colName}"
            if isinstance(row.colName, str) and col in final.columns:
                points = points + pd.to_numeric(final[col], errors="coerce").fillna(0.0) * row.points
        final[f"{prefix}_Points"] = points

    final = final.sort_values("TRUE_Points", ascending=False).reset_index(drop=True)
    final["TRUE_PosRank"] = final.groupby("primaryPosition")["TRUE_Points"].rank(
        ascending=False, method="min")

    # Only leagues with individual-defensive-player slots are exposed to the
    # per-slot scoring bug: they are the ones whose commissioners zero a stat out
    # for the D/ST slot, and a 0.0 override is what espn_api misreads as the base
    # value. Leagues with D/ST only are unaffected in practice (plan 11).
    slots = set((getattr(league, "roster_settings", {}) or {}).get("roster_slots", {}))
    if slots & {"DP", "DL", "DE", "LB", "CB", "S", "DT", "DB"}:
        affected = final["primaryPosition"].isin(
            ["DL", "DE", "LB", "NT", "CB", "S", "DT", "DB", "OLB", "D/ST"])
        print(f"  WARNING: this league has IDP slots, so {int(affected.sum())} "
              f"defensive rows (IDP and D/ST) are scored from a table that "
              f"espn_api collapses across lineup slots. Their point totals are "
              f"not trustworthy until docs/plans/11 lands. Offence is unaffected.")

    return final


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    import argparse

    from Scripts.config_utils import build_lg_vars
    from Scripts.fetch_utils import fetch_league

    p = argparse.ArgumentParser(
        prog="python -m Scripts.season_projections",
        description="Build season-long projections for a league (draft board input).",
    )
    p.add_argument("--league", required=True, help="display name or config key")
    p.add_argument("--season", type=int, help="defaults to config.yaml season")
    p.add_argument("--top", type=int, default=20, help="rows to print")
    p.add_argument("--out", help="optional parquet path to write")
    args = p.parse_args(argv)

    season = get_season() if args.season is None else args.season
    lg_vars = build_lg_vars()
    by_key = {c["key"]: c for c in lg_vars.values()}
    cfg = lg_vars.get(args.league) or by_key.get(args.league)
    if cfg is None:
        raise SystemExit(f"Unknown league {args.league!r}. "
                         f"Configured: {sorted(lg_vars)}")

    league = fetch_league(league_id=cfg["ID"], year=season,
                          swid=cfg["SWID"], espn_s2=cfg["ESPN_S2"])
    df = build_season_projections(league, season=season)

    cols = ["player_name", "primaryPosition", "pro_team", "TRUE_Points",
            "ESPN_Points", "BOL_Points", "PINNY_Points", "TRUE_PosRank"]
    print()
    print(df[[c for c in cols if c in df.columns]].head(args.top).to_string(index=False))

    if args.out:
        df.to_parquet(args.out)
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
