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
    proj_to_score,
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


def espn_season_projections(league, market: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Season-long ESPN projections for the league's player universe.

    Yardage is converted from ESPN's per-game season-row representation to a
    season total. See the module docstring.

    Args:
        league: A live ESPN ``League``.
        market: A :func:`Scripts.draft.adp.fetch_draft_market` frame to use as the
            universe. Strongly preferred for a draft board: it reaches ~1,000
            players in one 0.4s request, where the fallback below reaches ~329 at
            4.8s per ``free_agents`` call. When omitted, the roster-plus-free-agents
            walk is used, which is what the weekly path has always done.

    Returns:
        pd.DataFrame: ``player_id``, ``name_key``, ``player_name``,
        ``primaryPosition``, ``pro_team``, ``team_owner``, ``games``,
        ``ESPN_projected_total`` and ``ESPN_<stat>`` columns.

        ``player_id`` is the point of interest -- it lets the board join to the
        market exactly, instead of through the ~140-entry name-rename maps the
        book sources still need.
    """
    if market is not None:
        return _projections_from_market(league, market)

    seen, rows = set(), []

    def add(player, owner):
        if player.playerId in seen:
            return
        seen.add(player.playerId)
        season = player.stats.get(0) or {}
        breakdown = season.get("projected_breakdown") or {}
        games = float(breakdown.get(GAMES_PLAYED_KEY) or DEFAULT_GAMES) or DEFAULT_GAMES

        row = {
            "player_id": int(player.playerId),
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


def _disambiguate_name_keys(base: pd.DataFrame) -> pd.DataFrame:
    """Add a ``join_key`` that only the primary holder of a shared name matches on.

    Two different NFL players really do share a name, and a wide IDP player pool
    surfaces it: GOP Degenerates' 2,503-player universe has **11 colliding names**,
    including Lamar Jackson the Ravens quarterback (ADP 40) alongside Lamar Jackson
    a cornerback (ADP 170), and Justin Jefferson the Vikings receiver alongside
    Justin Jefferson a Browns linebacker.

    The book sources join on ``name_key``, and they carry one row per name. Left
    alone, a left merge attaches the receiver's projected receiving line to the
    linebacker as well -- inflating him into the league's top-projected IDP on
    somebody else's numbers.

    So the collision is resolved in favour of the player the book almost certainly
    means: the one ESPN projects highest. The others keep a sentinel ``join_key``
    that matches nothing, which drops them onto the "absent source" path that plan
    03 already handles correctly -- imputed from the ESPN/FP mean and renormalised
    out of the blend.

    Args:
        base: The ESPN universe, with ``name_key`` and ``ESPN_projected_total``.

    Returns:
        pd.DataFrame: ``base`` with a ``join_key`` column added.
    """
    base = base.copy()
    if "name_key" not in base.columns:
        base["join_key"] = None
        return base

    counts = base["name_key"].value_counts()
    shared = set(counts[counts > 1].index) - {None}
    base["join_key"] = base["name_key"]
    if not shared:
        return base

    rank_column = ("ESPN_projected_total" if "ESPN_projected_total" in base.columns
                   else None)
    ambiguous = base["name_key"].isin(shared)
    if rank_column:
        # Highest ESPN projection within each colliding name keeps the real key.
        primary = base[ambiguous].groupby("name_key")[rank_column].idxmax()
        losers = base.index[ambiguous].difference(pd.Index(primary))
    else:
        losers = base.index[ambiguous][1:]

    base.loc[losers, "join_key"] = [f"__ambiguous_{i}__" for i in losers]
    print(f"  {len(shared)} shared name(s) across {int(ambiguous.sum())} players; "
          f"book projections kept for the highest-projected of each, withheld from "
          f"{len(losers)} other(s) rather than misattributed.")
    return base


def _report_join_misses(base: pd.DataFrame, source: pd.DataFrame, label: str,
                        top: int = 12) -> None:
    """Name the players a source has that failed to join, highest-projected first.

    The book sources join on ``name_key`` -- string equality patched by ~140
    hardcoded renames -- so a suffix change silently drops a player. On a draft
    board that is worse than a visible gap, because the number still looks
    complete. This prints the misses that would matter most.

    Args:
        base: The ESPN universe.
        source: One book/projection source with a ``name_key`` column.
        label: Source name, for the message.
        top: How many missed players to name.
    """
    if base.empty or source.empty:
        return
    key = "join_key" if "join_key" in base.columns else "name_key"
    missing = base[~base[key].isin(source["name_key"])]
    if missing.empty:
        return

    rank_col = "ESPN_projected_total" if "ESPN_projected_total" in missing.columns else None
    if rank_col:
        missing = missing.sort_values(rank_col, ascending=False)
    names = missing["player_name"].head(top).tolist()
    print(f"    {len(missing)} not in {label}; highest projected: "
          f"{', '.join(names)}{' ...' if len(missing) > top else ''}")


def _projections_from_market(league, market: pd.DataFrame) -> pd.DataFrame:
    """Reshape a market frame into what the blend downstream expects.

    The market pull already carries identity, ``games`` and the ``ESPN_<stat>``
    columns -- it is built from the same ``projected_breakdown`` through the same
    per-game conversion. All that is left is attaching the owner and dropping the
    market-only columns, which belong to the board rather than to the blend.

    Args:
        league: A live ESPN ``League``, for the team-id to owner map.
        market: A :func:`Scripts.draft.adp.fetch_draft_market` frame.

    Returns:
        pd.DataFrame: The same shape :func:`espn_season_projections` returns.
    """
    owners = {
        int(team.team_id): getattr(team, "owner", "Unknown")
        for team in getattr(league, "teams", []) or []
    }

    keep = ["player_id", "name_key", "player_name", "primaryPosition", "pro_team",
            "games"]
    keep += [c for c in market.columns if c.startswith("ESPN_")]

    out = market[[c for c in keep if c in market.columns]].copy()
    out["team_owner"] = market.get(
        "on_team_id", pd.Series(0, index=market.index)
    ).map(lambda tid: owners.get(int(tid or 0), "Free Agent"))
    return out.reset_index(drop=True)


def load_fantasypros_season(season: int) -> pd.DataFrame:
    """FantasyPros season-long projections, prefixed ``FP_``.

    Args:
        season: Season year.

    Returns:
        pd.DataFrame: ``name_key`` plus ``FP_<stat>`` columns. Empty if absent.
    """
    path = season_dir("FantasyPros", season,
                      "FantasyPros_Projections_Season.parquet", create=False)
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
    path = season_dir("BetOnline", season, "BetOnline_SeasonProps_All.csv",
                      create=False)
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
    path = season_dir("Pinnacle", season, "Pinnacle_SeasonProps.parquet",
                      create=False)
    if not path.exists():
        print(f"  Pinnacle season file missing ({path.name}); "
              f"run `python -m Scripts.scrape_pinnacle_season`.")
        return pd.DataFrame(columns=["name_key"])

    df = pd.read_parquet(path).rename(columns={"player_name": "player_name"})
    df["name_key"] = df["player_name"].map(normalise_name)
    return _pivot_props(df, "PINNY")


def build_season_projections(league, season: Optional[int] = None,
                             weights: Optional[Dict] = None,
                             market: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Blend every source's season-long projection and score it for this league.

    Args:
        league: A live ESPN ``League`` -- supplies the scoring rules and, without
            ``market``, the player universe.
        season: Projection season. Defaults to ``league.year``.
        weights: Blend weights. Defaults to :data:`Scripts.projection_utils.WEIGHTS`.
        market: A :func:`Scripts.draft.adp.fetch_draft_market` frame to use as the
            universe -- ~1,000 players in one request instead of ~329 across
            twelve. Pass it when building a draft board.

    Returns:
        pd.DataFrame: One row per player with ``ESPN_``/``FP_``/``PINNY_``/``BOL_``
        and blended ``TRUE_`` stat columns, plus ``TRUE_Points`` -- the season
        projection in this league's own scoring.
    """
    season = league.year if season is None else season
    weights = WEIGHTS if weights is None else weights

    print(f"\n===== Season projections: {league.name} {season} =====")
    base = espn_season_projections(league, market=market)
    print(f"  ESPN: {len(base)} players")

    base = _disambiguate_name_keys(base)

    for loader, label in (
        (load_fantasypros_season, "FantasyPros"),
        (load_pinnacle_season, "Pinnacle"),
        (load_betonline_season, "BetOnline"),
    ):
        source = loader(season)
        if source.empty or "name_key" not in source:
            continue
        matched = base["join_key"].isin(source["name_key"]).sum()
        print(f"  {label}: {len(source)} players, {matched} matched to this league")
        _report_join_misses(base, source, label)
        base = base.merge(source, left_on="join_key", right_on="name_key",
                          how="left", suffixes=("", f"_{label}"))
        base = base.drop(columns=[c for c in base.columns
                                  if c == f"name_key_{label}"], errors="ignore")

    base = base.drop(columns=["join_key"], errors="ignore")

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
    # Through proj_to_score rather than a local loop over one scoring table, so
    # that ESPN's per-lineup-slot scoring is honoured: a D/ST unit is priced from
    # the slot-16 override and everything else -- offence, kicker, and every
    # individual defender -- from the rule's base value.
    #
    # This used to be a local loop with a comment explaining that it could not do
    # that. The comment predated plan 11, which gave the registry a `slot`
    # dimension and made proj_to_score read it. Left as it was, GOP Degenerates'
    # draft board priced defensiveSoloTackles at the D/ST override of 0.0 for
    # individual defenders too -- so linebackers, whose points are almost entirely
    # tackles, projected near zero and LB replacement level came out at LB1.
    final = proj_to_score(proj_df=final, s_league=league)

    final = final.sort_values("TRUE_Points", ascending=False).reset_index(drop=True)
    final["TRUE_PosRank"] = final.groupby("primaryPosition")["TRUE_Points"].rank(
        ascending=False, method="min")

    return final


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    import argparse

    from Scripts.config_utils import resolve_league
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
    try:
        cfg = resolve_league(args.league)
    except ValueError as e:
        raise SystemExit(str(e)) from e

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
