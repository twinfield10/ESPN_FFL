"""Pinnacle season-long player props, via the public guest API.

Pre-season there are no weekly markets to scrape, but season-long player props are
posted from the offseason onward -- which is exactly what a draft board wants.

This deliberately does *not* use Selenium. ``Scripts/scrape_pinnacle.py`` drives a
browser against the DOM and currently times out on
``div[class*="matchupMetadata"]``; the same data is available as plain JSON from
``guest.api.arcadia.pinnacle.com``, which is what the site itself calls. Two
requests total, no browser.

It also keeps its work behind ``main()``. The three existing scrapers run at import
time with no ``__main__`` guard, so importing them fires a live scrape -- which is
how a stray ``import Scripts.scrape_FP`` ended up writing a projections file.

Usage::

    python -m Scripts.scrape_pinnacle_season            # configured season
    python -m Scripts.scrape_pinnacle_season --season 2026
"""

import argparse
import re
import sys
from typing import Dict, List, Optional

import pandas as pd
import requests

from Scripts.config_utils import get_season
from Scripts.paths import season_dir

GUEST_API = "https://guest.api.arcadia.pinnacle.com/0.1"

#: NFL league id in Pinnacle's taxonomy.
NFL_LEAGUE_ID = 889

#: The site's own public guest key. Sent by pinnacle.com on every page load; this
#: is not a credential and grants nothing beyond the public odds feed.
GUEST_API_KEY = "CmX2KcMrXuFmNg6YFbmTxE0y9CIrOi0R"

SPECIAL_CATEGORY = "Season Long Player Props"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept": "application/json",
    "X-API-Key": GUEST_API_KEY,
}

#: ``"NFL 2026/2027 - CeeDee Lamb Regular Season Receiving Yards"``
DESCRIPTION_RE = re.compile(
    r"^NFL\s+(?P<seasons>[\d/]+)\s*-\s*(?P<player>.+?)\s+Regular Season\s+(?P<stat>.+)$"
)

#: ``"Over 1199.5 yards"`` / ``"Under 3.5"``
LINE_RE = re.compile(r"^(?P<side>Over|Under)\s+(?P<line>-?[\d.]+)")

#: Pinnacle's wording mapped onto the ESPN stat names the pipeline blends on.
STAT_MAP = {
    "Passing Yards": "passingYards",
    "Passing Touchdowns": "passingTouchdowns",
    "Rushing Yards": "rushingYards",
    "Rushing Touchdowns": "rushingTouchdowns",
    "Receiving Yards": "receivingYards",
    "Receiving Touchdowns": "receivingTouchdowns",
    "Receptions": "receivingReceptions",
    "Sacks": "defensiveSacks",
    "Interceptions": "defensiveInterceptions",
    "Tackles": "defensiveTotalTackles",
}


def american_to_prob(price: float) -> float:
    """Convert American odds to implied probability.

    Args:
        price: American odds, e.g. ``-111`` or ``+105``.

    Returns:
        float: Implied probability in [0, 1].
    """
    price = float(price)
    if price < 0:
        return -price / (-price + 100.0)
    return 100.0 / (price + 100.0)


def fetch_matchups(session: requests.Session) -> List[dict]:
    """All NFL matchups, including specials.

    Args:
        session: Session to reuse.

    Returns:
        list: Raw matchup dicts.

    Raises:
        requests.HTTPError: On a non-2xx response.
    """
    r = session.get(f"{GUEST_API}/leagues/{NFL_LEAGUE_ID}/matchups",
                    headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_prices(session: requests.Session) -> Dict[int, Dict[int, float]]:
    """Moneyline prices for every NFL market, keyed by matchup then participant.

    One league-wide request rather than one per prop.

    Args:
        session: Session to reuse.

    Returns:
        dict: ``{matchup_id: {participant_id: american_price}}``.
    """
    r = session.get(f"{GUEST_API}/leagues/{NFL_LEAGUE_ID}/markets/straight",
                    headers=HEADERS, timeout=30)
    r.raise_for_status()

    out: Dict[int, Dict[int, float]] = {}
    for market in r.json():
        if market.get("type") != "moneyline":
            continue
        mid = market.get("matchupId")
        if mid is None:
            continue
        bucket = out.setdefault(mid, {})
        for price in market.get("prices") or []:
            pid = price.get("participantId")
            if pid is not None and price.get("price") is not None:
                bucket[pid] = float(price["price"])
    return out


def parse_props(matchups: List[dict], prices: Dict[int, Dict[int, float]]) -> pd.DataFrame:
    """Turn season-long player-prop matchups into one row per player-stat.

    Args:
        matchups: Output of :func:`fetch_matchups`.
        prices: Output of :func:`fetch_prices`.

    Returns:
        pd.DataFrame: Columns ``player_name``, ``stat``, ``stat_raw``, ``line``,
        ``over_odds``, ``under_odds``, ``over_prob``, ``under_prob``,
        ``no_vig_over_prob``, ``seasons``, ``matchup_id``. Empty if none are posted.
    """
    rows = []
    for m in matchups:
        special = m.get("special") or {}
        if special.get("category") != SPECIAL_CATEGORY:
            continue

        parsed = DESCRIPTION_RE.match(special.get("description", "") or "")
        if not parsed:
            continue

        stat_raw = parsed.group("stat").strip()
        row = {
            "player_name": parsed.group("player").strip(),
            "stat_raw": stat_raw,
            "stat": STAT_MAP.get(stat_raw),
            "seasons": parsed.group("seasons"),
            "matchup_id": m.get("id"),
            "line": None,
            "over_odds": None, "under_odds": None,
        }

        by_participant = prices.get(m.get("id"), {})
        for participant in m.get("participants") or []:
            hit = LINE_RE.match(participant.get("name", "") or "")
            if not hit:
                continue
            row["line"] = float(hit.group("line"))
            side = hit.group("side").lower()
            price = by_participant.get(participant.get("id"))
            if price is not None:
                row[f"{side}_odds"] = price

        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["over_prob"] = df["over_odds"].map(american_to_prob, na_action="ignore")
    df["under_prob"] = df["under_odds"].map(american_to_prob, na_action="ignore")
    # Strip the bookmaker's margin so the two sides sum to 1. Without this the
    # line reads as more likely than the market actually implies.
    total = df["over_prob"] + df["under_prob"]
    df["no_vig_over_prob"] = (df["over_prob"] / total).where(total > 0)

    return df.sort_values(["stat_raw", "player_name"]).reset_index(drop=True)


def scrape(season: Optional[int] = None, write: bool = True) -> pd.DataFrame:
    """Fetch, parse and optionally persist Pinnacle season-long player props.

    Args:
        season: Season for the output path. Defaults to the configured season.
        write: Write parquet + CSV under
            ``Data/Projections/Pinnacle/Season/<season>/``.

    Returns:
        pd.DataFrame: The parsed props.
    """
    season = get_season() if season is None else season

    with requests.Session() as session:
        matchups = fetch_matchups(session)
        prices = fetch_prices(session)

    df = parse_props(matchups, prices)
    print(f"Pinnacle season-long player props for {season}: {len(df)} props, "
          f"{df['player_name'].nunique() if not df.empty else 0} players")

    if not df.empty:
        unmapped = sorted(df.loc[df["stat"].isna(), "stat_raw"].unique())
        if unmapped:
            print(f"  stat wordings with no ESPN mapping (add to STAT_MAP): {unmapped}")
        counts = df.groupby("stat_raw").size().sort_values(ascending=False)
        for stat_raw, n in counts.items():
            print(f"    {n:>4}  {stat_raw}")

    if write and not df.empty:
        out = season_dir("Pinnacle", season, "Pinnacle_SeasonProps.parquet")
        df.to_parquet(out)
        df.to_csv(out.with_suffix(".csv"), index=False)
        print(f"  wrote {out.parent}")

    return df


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    p = argparse.ArgumentParser(
        prog="python -m Scripts.scrape_pinnacle_season",
        description="Scrape Pinnacle season-long NFL player props.",
    )
    p.add_argument("--season", type=int, help="defaults to config.yaml season")
    p.add_argument("--dry-run", action="store_true", help="do not write files")
    args = p.parse_args(argv)

    df = scrape(season=args.season, write=not args.dry_run)
    return 0 if not df.empty else 1


if __name__ == "__main__":
    sys.exit(main())
