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

import datetime
import re
import unicodedata
from typing import Dict, List, Optional

import pandas as pd
import polars as pl

from Scripts.config_utils import get_season
from Scripts.paths import season_dir
from Scripts.projection_utils import (
    IMPUTED_SUFFIX,
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
            "games", "injury_status"]
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


def load_usage_season(season: int) -> pd.DataFrame:
    """The usage model's season projection, prefixed ``USG_``.

    The fifth source, and the only one that is not somebody else's projection --
    :mod:`Scripts.usage.season` fits it from observed usage, roster context and
    draft capital. Plan 16 measured its independence: its residuals correlate
    +0.832 with ESPN's, where FantasyPros' correlate +0.988.

    Two differences from the other three loaders, both deliberate:

    It returns ``player_id`` as well as ``name_key``, because it is the one source
    that can be joined on an id (see :func:`_merge_usage`).

    It returns the ``_is_imputed`` flags that :func:`Scripts.usage.project.build`
    wrote. The other loaders have their flags applied afterwards by
    :func:`impute_columns`, which fills a gap from the ESPN/FP mean. **This source
    must not be filled that way.** A usage model that abstains and is then
    backfilled from the mean of two other sources is not a fifth opinion, it is
    ESPN wearing a fifth badge -- exactly the double-counting plan 03 documents for
    Pinnacle. So its gaps stay null and stay flagged, and
    :func:`compute_weighted_stats` drops its weight for those rows.

    Args:
        season: Season year.

    Returns:
        pd.DataFrame: ``player_id``, ``name_key``, ``USG_<stat>`` and
        ``USG_<stat>_is_imputed``. Empty if the file is absent.
    """
    from Scripts.usage.project import projection_path

    path = projection_path(season)
    if not path.exists():
        print(f"  Usage season file missing ({path.name}); "
              f"run `python -m Scripts.usage.project --season {season}`.")
        return pd.DataFrame(columns=["name_key"])

    df = pd.read_parquet(path)
    # Carried lowercase so they stay out of the `<SOURCE>_<stat>` namespace that
    # `compute_weighted_stats` and `proj_to_score` scan. `usg_arm` is a string, and
    # a string in that namespace is a trap waiting for the first caller that
    # assumes every `USG_` column is numeric.
    context = {"expected_games": "usg_expected_games",
               "usg_evidence": "usg_evidence",
               "usg_thin_evidence": "usg_thin_evidence",
               "games_sd": "usg_games_sd",
               "games_low": "usg_games_low",
               "games_high": "usg_games_high",
               "usg_arm": "usg_arm"}
    out = df[[c for c in ["player_id", "name_key"] if c in df.columns]
             + [c for c in df.columns if c.startswith("USG_")]].copy()
    for source_col, target_col in context.items():
        if source_col in df.columns:
            out[target_col] = df[source_col]
    return out.dropna(subset=["name_key"])


def _merge_usage(base: pd.DataFrame, source: pd.DataFrame,
                 label: str = "Usage") -> pd.DataFrame:
    """Attach the usage source, joining on ESPN id first and name only as fallback.

    Every other season source joins on a normalised name, which is why
    :func:`_disambiguate_name_keys` has to exist: a wide IDP pool holds two Lamar
    Jacksons and two Justin Jeffersons, and a name join hands the receiver's line to
    the linebacker. The usage model keys on ``gsis_id``, which
    :mod:`Scripts.crosswalk` maps to ESPN's ``player_id``, so it can avoid that
    entirely.

    It cannot avoid it for everyone. The crosswalk file does not yet carry 2026
    rookies -- measured at 95 unresolved, every one of them a rookie -- and those are
    precisely the players the rookie arm exists to project, the model's one clearly
    measured win (rho ~ 0.61 within position against ~0 for a projection with no
    draft information). Dropping them to keep the join pure would discard the
    benefit to protect against a collision that mostly is not there.

    So: id join where the id resolved, ``join_key`` where it did not. The fallback
    uses ``join_key`` rather than ``name_key``, which means it inherits the
    collision protection already built -- the non-primary holder of a shared name
    carries a sentinel that matches nothing.

    Args:
        base: The ESPN universe, carrying ``player_id`` and ``join_key``.
        source: Output of :func:`load_usage_season`.
        label: Name used in the printed report.

    Returns:
        pd.DataFrame: ``base`` with the ``USG_`` columns attached.
    """
    source = source.copy()
    resolved = pd.to_numeric(source.get("player_id"), errors="coerce")

    by_id = int(resolved.notna().sum())

    # Fall back through `join_key`, so a shared name resolves to the same player the
    # book sources resolve to -- or to nobody, which is the safe outcome.
    if "join_key" in base.columns:
        name_to_id = (base.dropna(subset=["join_key"])
                      .drop_duplicates("join_key")
                      .set_index("join_key")["player_id"])
        fallback = source["name_key"].map(name_to_id)
        resolved = resolved.fillna(fallback)

    source["player_id"] = resolved
    source = source.dropna(subset=["player_id"])
    source["player_id"] = source["player_id"].astype("int64")
    # Two gsis ids can map to one ESPN id (the crosswalk records 13 duplicated
    # espn_id), and a name fallback can collide with an already-resolved row. Either
    # way a duplicate would fan the base frame out on merge.
    source = source.drop_duplicates("player_id", keep="first")

    by_name = len(source) - by_id
    matched = base["player_id"].isin(source["player_id"]).sum()
    print(f"  {label}: {len(source)} players "
          f"({by_id} by id, {max(by_name, 0)} by name), "
          f"{matched} matched to this league")

    source = source.drop(columns=["name_key"], errors="ignore")
    return base.merge(source, on="player_id", how="left")


#: ESPN *fantasy* statuses on which the usage model declines, when nothing better is
#: known.
#:
#: The fallback, not the rule. :func:`_apply_injury_adjustment` prefers ESPN's site
#: API, which carries an estimated return date; this handles the players it has no
#: record for -- 6 of 22 on the 2026-08-07 pull, George Kittle and Brandon Aiyuk among
#: them.
#:
#: **The model cannot see a current injury and the other sources can.** nflreadr
#: refuses 2026 injuries outright, so ``expected_games`` is built from prior-season
#: availability, snap share and age -- statistics about a player who was healthy last
#: August. Left alone the model overrode ESPN and FantasyPros in the worst direction:
#: across 22 players listed OUT or on IR it **lifted** the blend by a mean of +15.7
#: points, while lowering active draftable players by 2.7. ESPN and FantasyPros both
#: projected Ricky Pearsall at 0.0 -- they know he is on IR for the season -- and the
#: model pulled the blend to 72.4.
#:
#: ``QUESTIONABLE`` is excluded: pre-season it is week-to-week noise on 64 players.
INJURY_ABSTAIN_STATUSES = ("OUT", "INJURY_RESERVE")

#: Games in a full season, for converting a return date into a share of the slate.
FULL_SLATE = 17.0


def load_espn_injuries(season: int) -> pd.DataFrame:
    """ESPN's injury report with estimated return dates, if it has been pulled.

    Args:
        season: Season year.

    Returns:
        pd.DataFrame: ``name_key``, ``status``, ``return_date``. Empty when absent --
        a missing pull degrades to the status-only fallback rather than failing.
    """
    from Scripts.scrape_espn_injuries import injuries_path

    path = injuries_path(season)
    if not path.exists():
        print(f"  ESPN injuries not pulled for {season}; falling back to fantasy "
              f"status alone. Run `python -m Scripts.scrape_espn_injuries`.")
        return pd.DataFrame(columns=["name_key", "status", "return_date"])
    frame = pd.read_parquet(path)
    return frame.dropna(subset=["name_key"]).drop_duplicates("name_key")


def games_available(return_date, week_one, slate: float = FULL_SLATE) -> float:
    """Games a player can still play, given when he is expected back.

    Args:
        return_date: Estimated return, or None.
        week_one: Date of the season's first game.
        slate: Games in a full season.

    Returns:
        float: Games available, ``slate`` when he is back before week 1 and 0.0 when
        the date is a season-ending sentinel.
    """
    from Scripts.scrape_espn_injuries import SEASON_ENDING_AFTER

    if return_date is None or pd.isna(return_date):
        return slate
    if hasattr(return_date, "date"):
        return_date = return_date.date()
    if return_date > SEASON_ENDING_AFTER:
        return 0.0
    missed = (return_date - week_one).days / 7.0
    return float(min(max(slate - missed, 0.0), slate))


def _week_one(season: int):
    """First gameday of the season, from the committed schedule.

    Read rather than assumed: the opener moves by several days year to year, and a
    hardcoded date would silently mis-count the weeks a player misses.

    Args:
        season: Season year.

    Returns:
        date | None: The earliest week-1 gameday, or None when unavailable.
    """
    from Scripts.draft.handcuff import load_schedules

    try:
        schedules = load_schedules()
    except FileNotFoundError:
        return None
    week_one = schedules.filter((pl.col("season") == season)
                                & (pl.col("week") == 1))
    if week_one.is_empty() or "gameday" not in week_one.columns:
        return None
    earliest = week_one["gameday"].min()
    if earliest is None:
        return None
    return (datetime.date.fromisoformat(str(earliest))
            if not hasattr(earliest, "year") else earliest)


def _apply_injury_adjustment(base: pd.DataFrame, season: int) -> pd.DataFrame:
    """Scale the usage model's line by the games a current injury leaves.

    **Only the usage model is scaled, and that is the point.** ESPN and FantasyPros
    already price a known absence -- they had Ricky Pearsall at 0.0 -- so discounting
    the whole blend would count the same injury twice. What the model lacks is any
    sight of the current season: nflreadr refuses 2026 injuries, so its
    ``expected_games`` describes a player who was healthy last August. Scaling its
    line by ``games_available / 17`` makes that one source injury-aware and leaves the
    others to speak for themselves.

    The graded form matters. A status-only rule cannot tell "back next week" from "out
    until November", and on the 2026-08-07 pull it was wrong for **9 of 22** players
    -- Alec Pierce (ADP 96) returns 13 August, Zach Charbonnet (ADP 149) on 9
    September, the day before week 1. Withdrawing the model for them threw away its
    opinion on draftable players who will be fine.

    Where ESPN's site API has no record, the fantasy status falls back to a straight
    abstention -- see :data:`INJURY_ABSTAIN_STATUSES`.

    Args:
        base: The merged frame, after the usage source has been joined.
        season: Season being projected.

    Returns:
        pd.DataFrame: ``base`` with ``USG_`` scaled, or withdrawn where the player is
        out for the year or unknown to the report.
    """
    columns = [c for c in base.columns
               if c.startswith("USG_") and not c.endswith(IMPUTED_SUFFIX)]
    if not columns:
        return base

    week_one = _week_one(season)
    report = load_espn_injuries(season) if week_one else pd.DataFrame()

    share = pd.Series(1.0, index=base.index)
    if week_one is not None and not report.empty and "join_key" in base.columns:
        dates = base["join_key"].map(
            report.set_index("name_key")["return_date"].to_dict())
        known = dates.notna()
        share[known] = dates[known].map(
            lambda d: games_available(d, week_one)) / FULL_SLATE
    else:
        known = pd.Series(False, index=base.index)

    # Players the report says nothing about keep the old status-only rule.
    if "injury_status" in base.columns:
        unknown_and_out = (~known) & base["injury_status"].isin(
            INJURY_ABSTAIN_STATUSES)
        share[unknown_and_out] = 0.0
    else:
        unknown_and_out = pd.Series(False, index=base.index)

    scaled = share < 1.0
    withdrawn = share <= 0.0
    for column in columns:
        base.loc[scaled, column] = base.loc[scaled, column] * share[scaled]
        base.loc[withdrawn, column] = float("nan")
        flag = f"{column}{IMPUTED_SUFFIX}"
        if flag in base.columns:
            base.loc[withdrawn, flag] = True

    if scaled.any():
        print(f"  Usage: injury-adjusted {int(scaled.sum())} player(s) "
              f"({int(withdrawn.sum())} withdrawn outright, "
              f"{int(unknown_and_out.sum())} of those on fantasy status alone).")
    return base


#: The sources that hold an independent opinion, for the floor/ceiling spread.#: The sources that hold an independent opinion, for the floor/ceiling spread.
#:
#: ``MEAN`` is excluded because it is not an opinion -- it is the ESPN/FantasyPros
#: average, and including it would pull the spread toward the middle of two sources
#: already in the set. ``TRUE`` is excluded because it is the blend being bracketed.
#:
#: **``USG`` is excluded, and it was briefly included by mistake.** The reasoning for
#: adding it was that plan 16's G0 measured it as the most independent source in the
#: set (+0.832 residual correlation with ESPN against FantasyPros' +0.988). That is a
#: statement about *information content*, and this function needs a different
#: property: that the sources are answering the same question. They are not.
#: ``USG_Points`` is an expected value -- per-game production times ~13.5 expected
#: games -- while ESPN and FantasyPros project a healthy 17-game season. So it does
#: not disagree with them so much as measure something else, and it sat below all
#: four for **51.7% of the players it covered**, taking the median floor-to-ceiling
#: width on Winfield Football's draftable pool from 8.5% to 24.0%.
#:
#: Rescaling it to a common if-healthy basis (per-game x 17) was measured too, and is
#: better but still wrong: the width lands at 13.6% and ``USG`` is still the minimum
#: for 47% of draftable players. That residual is not a units problem, it is the
#: model shrinking toward positional baselines while the other sources extrapolate,
#: and draftable players are by definition the top of the pool. Real disagreement --
#: but it makes the interval asymmetric, so it reads as "the model is bearish" rather
#: than "here is the uncertainty".
#:
#: Those are two different quantities and this column should only ever hold one of
#: them. Disagreement *between* forecasters belongs here; uncertainty *within* a
#: forecast is a predictive interval, which the usage model can supply properly
#: because it decomposes into volume x efficiency x games. Until that exists, the
#: model's dissent is carried by ``USG_PosRankDelta``, which is scale-free and cannot
#: contaminate the spread.
OPINION_PREFIXES = ("ESPN", "FP", "PINNY", "BOL")



#: Sources that can supply a player's projection at all, for coverage counting.
#:
#: **Deliberately wider than :data:`OPINION_PREFIXES`, and the two must not be
#: merged.** They answer different questions:
#:
#: - *Do the forecasters disagree, and by how much?* -- the floor/ceiling spread, which
#:   needs sources measuring the same quantity, and therefore excludes ``USG``.
#: - *Does this player have a projection at all?* -- ``projection_missing`` and
#:   ``sources_real``, which must include every source that moves ``TRUE_Points``.
#:
#: Conflating them is a live bug, not a hypothetical: with ``USG`` weighted into the
#: blend but absent from the coverage list, a player the usage model projects and
#: nobody else does gets a real ``TRUE_Points`` and a ``projection_missing`` of True.
#: The board would then hide, as unprojected, exactly the players the model exists to
#: differentiate.
PROJECTION_PREFIXES = ("ESPN", "FP", "PINNY", "BOL", "USG")


def attach_source_spread(df: pd.DataFrame, stats: List[str],
                         prefixes: tuple = OPINION_PREFIXES) -> pd.DataFrame:
    """Bracket the blend with the range of the sources that really have a line.

    Plan 09 asks the draft board for a floor and a ceiling. The honest version of
    that is how far the sources disagree -- but only the sources that *have* an
    opinion. The blend imputes a missing book from the ESPN/FantasyPros mean, so
    taking a naive min/max across four columns would report a **narrow** range for
    exactly the players nobody has priced, which is backwards: an unpriced player
    is the uncertain one. This is the same trap
    ``docs/plans/03-projection-source-coverage.md`` documents for the weights.

    So a source counts for a player only when it contributed at least one real,
    non-imputed cell to a scored stat. Fewer than two such sources means there is
    no disagreement to measure and ``floor``/``ceiling`` are NaN rather than equal
    to the projection -- a single source is not a confidence interval.

    Prior-season variance, the other half of plan 09's floor/ceiling, is **not**
    included: it needs per-week 2025 actuals joined per player, which is a
    different data path (``Data/Store/2025/*/lineups.parquet``) and is recorded as
    outstanding in the plan.

    Args:
        df: Frame carrying ``<prefix>_<stat>`` columns, their ``_is_imputed``
            flags, and the ``<prefix>_Points`` columns ``proj_to_score`` writes.
        stats: Scored stat names -- the scoring table's ``colName`` values. Only
            these matter, because an unscored stat cannot move a point total.
        prefixes: Sources to compare.

    Returns:
        pd.DataFrame: ``df`` with ``sources_real``, ``floor`` and ``ceiling``.
    """
    real_points = {}
    for prefix in prefixes:
        points_col = f"{prefix}_Points"
        if points_col not in df.columns:
            continue

        # A scored cell this source really supplied. ESPN carries no imputation
        # flags -- it is the source everything else is imputed *from*.
        #
        # A zero does not count. The frame is dense with structural zeros -- a
        # kicker's ``FP_passingYards`` is 0.0 and unflagged, because nobody imputed
        # it and nobody asserted it either. Counting those made FantasyPros a
        # "real" source for Cameron Dicker on the strength of twelve zeros, and his
        # floor and ceiling came back exactly equal to ESPN's total: a spread of
        # zero reported as measured agreement. That is the same mistake this
        # function exists to avoid, one level down.
        contributed = pd.Series(False, index=df.index)
        for stat in stats:
            stat_col = f"{prefix}_{stat}"
            if stat_col not in df.columns:
                continue
            has_value = df[stat_col].notna() & (df[stat_col] != 0)
            imputed_col = f"{stat_col}_is_imputed"
            if imputed_col in df.columns:
                has_value &= ~df[imputed_col].fillna(False).astype(bool)
            contributed |= has_value

        real_points[prefix] = df[points_col].where(contributed)

    if not real_points:
        df["sources_real"] = 0
        df["floor"] = float("nan")
        df["ceiling"] = float("nan")
        return df

    opinions = pd.DataFrame(real_points, index=df.index)
    counts = opinions.notna().sum(axis=1)
    comparable = counts >= 2

    df["sources_real"] = counts
    df["floor"] = opinions.min(axis=1).where(comparable)
    df["ceiling"] = opinions.max(axis=1).where(comparable)
    return df


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

    # The usage model joins on an ESPN id rather than a name, so it merges through
    # its own path -- and while `join_key` still exists, which it uses as a fallback
    # for the 2026 rookies the crosswalk does not yet carry.
    usage = load_usage_season(season)
    if not usage.empty and "player_id" in base.columns:
        base = _merge_usage(base, usage)

    base = _apply_injury_adjustment(base, season)

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

    # `USG_` is deliberately absent from that chain. Filling an abstention from
    # `MEAN_` -- the ESPN/FantasyPros average -- would turn the one source that is
    # not somebody else's projection into a copy of two that are, which is the
    # double-counting plan 03 measured for Pinnacle. Its gaps arrive already
    # flagged from `Scripts.usage.project`, so `compute_weighted_stats` drops its
    # weight on those rows and renormalises the sources that did speak.
    #
    # Rows that never joined at all have no flag column value either; NaN reads as
    # imputed, which is the same outcome and the reason that default exists.
    for stat_col in [c for c in base.columns
                     if c.startswith("USG_") and not c.endswith(IMPUTED_SUFFIX)]:
        flag = f"{stat_col}{IMPUTED_SUFFIX}"
        if flag in base.columns:
            base[flag] = base[flag].fillna(True).astype(bool)

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

    # Floor/ceiling from how much the sources that really have a line disagree.
    final = attach_source_spread(final, stats)

    final = final.sort_values("TRUE_Points", ascending=False).reset_index(drop=True)
    final["TRUE_PosRank"] = final.groupby("primaryPosition")["TRUE_Points"].rank(
        ascending=False, method="min")

    # The usage model's own ordering, and its disagreement with the consensus.
    #
    # Ordering is the only thing worth comparing between these two columns, because
    # they are not the same quantity. `USG_Points` is an expected value -- per-game
    # production times *expected games*, which for a rostered starter is around 13.5
    # -- while ESPN and FantasyPros project a healthy 17-game season. So `USG_Points`
    # sits roughly 20% below `TRUE_Points` for almost everyone, and reading that gap
    # as bearishness would be reading the injury discount as an opinion about talent.
    #
    # Rank removes the level. Measured on Knights_FFL's 2026 draftable pool, the two
    # orderings agree at Spearman 0.78 (RB), 0.70 (WR) and 0.44 (TE), so the column
    # carries real information rather than restating the blend.
    #
    # `USG_PosRankDelta` is positive where the model likes a player more than the
    # consensus does. The two tails decompose cleanly and are worth knowing before
    # trusting either: the fades are mostly the availability head discounting injury
    # history (Nabers at 9.2 expected games, Garrett Wilson 10.3), and the buys are
    # mostly rookies the four sources price thinly (Tate, Tyson, Lemon). Availability
    # is the weaker of the two -- plan 18 measured prior-season games at r = +0.343
    # among players who managed 8+, so a fade is an injury-risk discount and not a
    # verdict on the player.
    if "USG_Points" in final.columns:
        final["USG_PosRank"] = final.groupby("primaryPosition")["USG_Points"].rank(
            ascending=False, method="min")
        final["USG_PosRankDelta"] = final["TRUE_PosRank"] - final["USG_PosRank"]

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
