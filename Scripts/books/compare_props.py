"""How much the weekly prop books cover, and how much they agree, at one moment.

Written for the question asked on 2026-10-13: four books were added beside
BetOnline and Pinnacle, and there is no outcome history to backtest them against
yet. What *can* be measured the day they land is a snapshot:

1. **Coverage** -- of the players that matter this week (ESPN projects at least
   :data:`RELEVANT_POINTS`, skill position, not on bye), how many each book prices,
   per stat. Counted as *real* lines, matched to ESPN names the way the blend
   matches them.
2. **Agreement** -- where several books price the same player-stat, how far each
   sits from the books' own median, and how far that median sits from ESPN.
3. **New waiver coverage** -- free agents in the owner's leagues that gain a book
   line from the new four and had none from BetOnline or Pinnacle.

Usage::

    python -m Scripts.books.compare_props --week 6
"""

import argparse
from typing import Dict, List, Optional

import polars as pl

from Scripts.paths import store_dir
from Scripts.projection_utils import betonline_parquet, pinnacle_parquet
from Scripts.books.weekly_props import ADAPTERS, load_adapter, season_file

#: A player is "relevant" when ESPN projects at least this many points for him.
RELEVANT_POINTS = 5.0
#: League whose ESPN projections and positions define the population.
REFERENCE_LEAGUE = "winfield_football"
#: Leagues whose free agents the waiver section lists.
OWNER_LEAGUES = ("winfield_football", "jeffs_league", "gop_degenerates", "knights_ffl")
SKILL = ("QB", "RB", "WR", "TE")
STATS = {
    "receivingYards": "Rec Yds",
    "receivingReceptions": "Rec",
    "rushingYards": "Rush Yds",
    "passingYards": "Pass Yds",
    "anytimeTD": "Any TD",
}
OLD_BOOKS = ("BOL", "PINNY")


def _norm(col: str = "player_name") -> pl.Expr:
    from Scripts.season_projections import normalise_name
    return pl.col(col).map_elements(normalise_name, return_dtype=pl.Utf8).alias("key")


def load_books(season: int, week: int) -> Dict[str, pl.DataFrame]:
    """Each book's week rows: ``key`` plus one column per :data:`STATS` entry."""
    paths = {"BOL": betonline_parquet(season), "PINNY": pinnacle_parquet(season)}
    for book in ADAPTERS:
        adapter = load_adapter(book)
        paths[adapter.PREFIX] = season_file(adapter.DIR, season, create=False)
    out = {}
    for prefix, path in paths.items():
        if not path.exists():
            continue
        frame = pl.read_parquet(path).filter(pl.col("week") == week)
        cols = []
        for stat in STATS:
            if stat == "anytimeTD":
                parts = [c for c in ("proj_rushingTouchdowns", "proj_receivingTouchdowns")
                         if c in frame.columns]
                cols.append((pl.sum_horizontal(parts) if parts else pl.lit(None, pl.Float64))
                            .alias(stat))
            else:
                column = f"proj_{stat}"
                cols.append((pl.col(column) if column in frame.columns
                             else pl.lit(None, pl.Float64)).cast(pl.Float64).alias(stat))
        out[prefix] = (frame.with_columns(_norm()).select(["key", *cols])
                            .unique("key", keep="first"))
    return out


def load_population(season: int, week: int) -> pl.DataFrame:
    """The reference league's skill players this week, with ESPN's line and points."""
    lineups = pl.read_parquet(store_dir(season, REFERENCE_LEAGUE, "lineups.parquet"))
    espn = {f"ESPN_{s}": s for s in STATS if f"ESPN_{s}" in lineups.columns}
    return (lineups
            .filter((pl.col("week") == week) & pl.col("primaryPosition").is_in(SKILL)
                    & (pl.col("player_active_status") != "bye"))
            .with_columns(_norm(),
                          (pl.col("ESPN_rushingTouchdowns") + pl.col("ESPN_receivingTouchdowns"))
                          .alias("ESPN_anytimeTD"))
            .select(["key", "player_name", pl.col("primaryPosition").alias("pos"),
                     "pro_team", "projPoints", *espn, "ESPN_anytimeTD"])
            .unique("key", keep="first"))


def coverage(pop: pl.DataFrame, books: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Players priced per book and stat, among the relevant population."""
    relevant = pop.filter(pl.col("projPoints") >= RELEVANT_POINTS)
    rows = []

    def count(frames: List[pl.DataFrame], label: str) -> None:
        row = {"source": label}
        joined = relevant.select("key")
        any_line = pl.lit(False)
        stat_line = pl.lit(False)
        for stat, title in STATS.items():
            have = pl.lit(False)
            for i, frame in enumerate(frames):
                joined = joined.join(frame.select("key", pl.col(stat).alias(f"{stat}_{i}")),
                                     on="key", how="left")
                have = have | pl.col(f"{stat}_{i}").is_not_null()
            n = joined.select(have.sum()).item()
            row[title] = n
            any_line = any_line | have
            if stat != "anytimeTD":
                stat_line = stat_line | have
        row["Stat line"] = joined.select(stat_line.sum()).item()
        row["Any"] = joined.select(any_line.sum()).item()
        rows.append(row)

    for prefix, frame in books.items():
        count([frame], prefix)
    old = [books[b] for b in OLD_BOOKS if b in books]
    new = [f for p, f in books.items() if p not in OLD_BOOKS]
    count(old, "BOL+PINNY")
    count(new, "4 new")
    count(old + new, "All 6")
    out = pl.DataFrame(rows)
    return out.with_columns(pl.lit(relevant.height).alias("of"))


def agreement(pop: pl.DataFrame, books: Dict[str, pl.DataFrame], min_books: int = 3) -> pl.DataFrame:
    """Per book and stat: median |book - books' median|, and the median's gap to ESPN."""
    rows = []
    for stat, title in STATS.items():
        wide = pop.select("key", f"ESPN_{stat}") if f"ESPN_{stat}" in pop.columns else pop.select("key")
        present = []
        for prefix, frame in books.items():
            wide = wide.join(frame.select("key", pl.col(stat).alias(prefix)), on="key", how="left")
            present.append(prefix)
        wide = (wide.with_columns(pl.concat_list(present).list.drop_nulls().alias("_v"))
                    .with_columns(pl.col("_v").list.len().alias("_n"),
                                  pl.col("_v").list.median().alias("_med"))
                    .filter(pl.col("_n") >= min_books))
        if wide.is_empty():
            continue
        for prefix in present:
            dev = wide.filter(pl.col(prefix).is_not_null()).select(
                (pl.col(prefix) - pl.col("_med")).abs().median().alias("mad"),
                (pl.col(prefix) - pl.col("_med")).median().alias("bias"),
                pl.len().alias("n"))
            if dev["n"][0]:
                rows.append({"stat": title, "source": prefix, "n": dev["n"][0],
                             "median_abs_dev": round(dev["mad"][0], 3),
                             "median_signed_dev": round(dev["bias"][0], 3)})
        if f"ESPN_{stat}" in wide.columns:
            dev = wide.select((pl.col(f"ESPN_{stat}") - pl.col("_med")).abs().median().alias("mad"),
                              (pl.col(f"ESPN_{stat}") - pl.col("_med")).median().alias("bias"),
                              pl.len().alias("n"))
            rows.append({"stat": title, "source": "ESPN", "n": dev["n"][0],
                         "median_abs_dev": round(dev["mad"][0], 3),
                         "median_signed_dev": round(dev["bias"][0], 3)})
    return pl.DataFrame(rows)


def new_waiver_coverage(season: int, week: int, books: Dict[str, pl.DataFrame],
                        min_points: float = 3.0) -> pl.DataFrame:
    """Free agents in the owner's leagues priced by a new book and by neither old one."""
    old_keys = set()
    for prefix in OLD_BOOKS:
        if prefix in books:
            frame = books[prefix]
            real = frame.filter(pl.any_horizontal([pl.col(s).is_not_null() & (pl.col(s) > 0)
                                                   for s in STATS if s != "anytimeTD"]))
            old_keys |= set(real["key"].to_list())
    new_books = {p: f for p, f in books.items() if p not in OLD_BOOKS}
    rows = []
    for league in OWNER_LEAGUES:
        path = store_dir(season, league, "lineups.parquet")
        if not path.exists():
            continue
        fa = (pl.read_parquet(path)
                .filter((pl.col("week") == week) & (pl.col("team_owner") == "Free Agent")
                        & pl.col("primaryPosition").is_in(SKILL)
                        & (pl.col("projPoints") >= min_points))
                .with_columns(_norm())
                .select("key", "player_name", "primaryPosition", "pro_team", "projPoints"))
        for row in fa.iter_rows(named=True):
            if row["key"] in old_keys:
                continue
            priced = [p for p, f in new_books.items()
                      if f.filter((pl.col("key") == row["key"])
                                  & pl.any_horizontal([pl.col(s).is_not_null()
                                                       for s in STATS if s != "anytimeTD"])).height]
            if priced:
                rows.append({"league": league, **{k: row[k] for k in
                             ("player_name", "primaryPosition", "pro_team", "projPoints")},
                             "new_books": ",".join(priced)})
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def main(argv: Optional[List[str]] = None) -> int:
    from Scripts.nfl_utils import current_season, current_week

    p = argparse.ArgumentParser(prog="python -m Scripts.books.compare_props")
    p.add_argument("--season", type=int)
    p.add_argument("--week", type=int)
    args = p.parse_args(argv)
    season = args.season or current_season()
    week = args.week or current_week()

    books = load_books(season, week)
    pop = load_population(season, week)
    with pl.Config(tbl_rows=60, tbl_cols=20, tbl_width_chars=160, tbl_hide_dataframe_shape=True):
        print(f"\nCoverage, week {week}: skill players ESPN projects >= {RELEVANT_POINTS:g} pts "
              f"({REFERENCE_LEAGUE} scoring), not on bye\n")
        print(coverage(pop, books))
        print("\nAgreement where >= 3 sources price the player (deviation from the books' median)\n")
        print(agreement(pop, books))
        waivers = new_waiver_coverage(season, week, books)
        print(f"\nFree agents (>= 3 ESPN pts) newly priced by the four new books: {waivers.height}\n")
        if waivers.height:
            print(waivers.sort(["league", "projPoints"], descending=[False, True]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
