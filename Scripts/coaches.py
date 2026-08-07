"""Head coach and coordinators per team-season, from two sources for two reasons.

    python -m Scripts.coaches            # current season from Wikipedia, rest from nflverse
    python -m Scripts.coaches --offline  # nflverse only, no network

Writes ``Data/NFL/coaching_staff.parquet``, which is **committed**: it is small,
hand-auditable, and upstream can change under you -- the same argument that keeps
``player_ids.parquet`` in git.

**Why two sources.** For a season that has been played, the coach who coached each
game is a matter of record, and ``R/GetCoaches.R`` extracts it from nflverse's
schedules. For a season that has *not* been played, that file is
partially-updated-and-looks-complete, which is the worst state a source can be in:
checked live 2026-08-07 it recorded seven of the offseason's head-coach changes and
missed Arizona's, still listing Jonathan Gannon where the real answer is Mike
LaFleur. So the current season comes from Wikipedia's ``<year>_<Team>_season``
infobox, which is also the only free source for coordinators at all.

**What Wikipedia does not have** is who calls the plays.
``docs/plans/21-coaching-and-scheme.md`` measures how much that costs: with the same
head coach, a team's running-back target share still moves more than a league
standard deviation in 30% of seasons. Head-coach identity gets most of the way --
usage persistence roughly halves when the head coach changes -- and the residual is
carried as uncertainty rather than guessed at.

Coordinator coverage is patchy and that is recorded rather than papered over: the
2025 Arizona article carries no ``off_coach`` at all while the 2026 one carries all
three.
"""

import argparse
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Tuple

import polars as pl

from Scripts.config_utils import get_season
from Scripts.paths import DATA_DIR

#: Where the committed table lands.
COACHING_STAFF_PARQUET = DATA_DIR / "NFL" / "coaching_staff.parquet"

#: Inputs from ``R/GetCoaches.R``.
COACHES_BY_GAME_PARQUET = DATA_DIR / "NFL" / "coaches_by_game.parquet"
TEAM_NAMES_PARQUET = DATA_DIR / "NFL" / "team_names.parquet"

#: Wikipedia's API. A descriptive User-Agent is their stated requirement.
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
USER_AGENT = ("ESPN-FFL-research/1.0 (personal fantasy football projections; "
              "nflverse-derived, low volume)")

#: Titles per request. MediaWiki accepts 50; 32 teams fit in one.
#:
#: **Batched because one-at-a-time was unreliable, not merely slow.** Thirty-two
#: sequential requests at 1.1s apart still failed for four teams a run -- and a
#: different four each run, which is the signature of throttling rather than missing
#: articles. Minnesota reported "no page" on one run and returned 62,488 characters on
#: the next. One request for all of them removes the failure mode instead of retrying
#: around it, and is kinder to Wikipedia besides.
TITLES_PER_REQUEST = 40

#: Retries for the batched request, with the delay doubling each time.
MAX_RETRIES = 4

#: Seconds before the first request, and the base for backoff.
REQUEST_INTERVAL = 0.5

#: Infobox fields to read, mapped to output column names.
INFOBOX_FIELDS: Dict[str, str] = {
    "coach": "head_coach",
    "off_coach": "offensive_coordinator",
    "def_coach": "defensive_coordinator",
}


def strip_noise(wikitext: str) -> str:
    """Remove references and comments, which contain pipes of their own.

    Ordered before field extraction for the same reason link resolution is. A
    citation reads ``<ref>{{cite web|url=…}}</ref>``, and that pipe is
    indistinguishable from a field separator — extraction terminated inside the
    template and returned ``"A B{{cite web"`` as a coach's name.

    The infobox is itself a template, so ``{{…}}`` cannot be removed wholesale; the
    pipe-bearing templates that matter live inside refs, which go as a unit.

    Args:
        wikitext: Raw wikitext.

    Returns:
        str: The text with refs and HTML comments removed.
    """
    wikitext = re.sub(r"<ref[^>]*/>", "", wikitext)
    wikitext = re.sub(r"<ref[^>]*>.*?</ref>", "", wikitext,
                      flags=re.DOTALL | re.IGNORECASE)
    return re.sub(r"<!--.*?-->", "", wikitext, flags=re.DOTALL)


def resolve_links(wikitext: str) -> str:
    """Replace every wikilink with its display text.

    Done to the **whole** document before any field is extracted, and the order
    matters. ``[[Sean Mannion (American football)|Sean Mannion]]`` contains a pipe,
    which is also the infobox's field separator, so a field regex that terminates at
    the first pipe truncates inside the link. Resolving links first removes every
    pipe that is not a separator.

    Args:
        wikitext: Raw wikitext.

    Returns:
        str: The same text with links flattened.
    """
    return re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", r"\1", wikitext)


def _clean_value(value: str) -> Optional[str]:
    """Reduce an extracted infobox value to a plain name.

    Args:
        value: Field value, with links already resolved.

    Returns:
        str | None: The name, or None when nothing survives.
    """
    value = re.sub(r"<[^>]+>", "", value)
    value = re.sub(r"\{\{[^{}]*\}\}", "", value)
    # A template fragment left by extraction terminating inside one.
    value = value.split("{{")[0]
    # An interim appointment often reads "Name (interim)", and a shared job lists two
    # names. Keep the first, which is who held it going into the season.
    value = re.split(r"\s*(?:<br\s*/?>|,|\band\b|\()", value)[0]
    value = value.strip().strip("|}").strip()
    return value or None


def parse_infobox(wikitext: str) -> Dict[str, Optional[str]]:
    """Pull the coaching fields out of a season article's infobox.

    Fields are **not** line-anchored. The Philadelphia 2026 article writes
    ``|off_coach=...|def_coach=...}}`` on a single line, so a ``^\\|`` pattern finds
    the first and captures the second along with it, and never finds the second at
    all.

    Args:
        wikitext: Section-0 wikitext of a ``<year>_<Team>_season`` article.

    Returns:
        dict: One entry per :data:`INFOBOX_FIELDS` value, None where absent.
    """
    out: Dict[str, Optional[str]] = {name: None
                                    for name in INFOBOX_FIELDS.values()}
    if not wikitext:
        return out
    flattened = resolve_links(strip_noise(wikitext))
    for field, column in INFOBOX_FIELDS.items():
        match = re.search(rf"[|\n]\s*{field}\s*=\s*([^|\n]*)", flattened)
        if match:
            out[column] = _clean_value(match.group(1))
    return out


def lead_section(wikitext: str) -> str:
    """The article's lead, which is where the infobox lives.

    The batched API returns whole articles rather than section 0, and a 60,000-word
    season article mentions coaches in plenty of places. Cutting at the first heading
    restores the narrow window the per-article call gave for free.

    Args:
        wikitext: Full article wikitext.

    Returns:
        str: Everything before the first section heading.
    """
    return re.split(r"\n==", wikitext, maxsplit=1)[0]


def fetch_many(titles: List[str], verbose: bool = True
               ) -> Dict[str, Optional[str]]:
    """Lead-section wikitext for several articles, in as few requests as possible.

    Args:
        titles: Article titles.
        verbose: Print a line per request.

    Returns:
        dict: ``{title: lead wikitext}``. A title maps to None when the article does
        not exist; a title absent from the result means the request failed.
    """
    out: Dict[str, Optional[str]] = {}
    for start in range(0, len(titles), TITLES_PER_REQUEST):
        batch = titles[start:start + TITLES_PER_REQUEST]
        query = urllib.parse.urlencode({
            "action": "query", "prop": "revisions", "rvslots": "main",
            "rvprop": "content", "format": "json", "formatversion": 2,
            "redirects": 1, "titles": "|".join(batch),
        })
        payload = None
        delay = REQUEST_INTERVAL
        for attempt in range(MAX_RETRIES):
            time.sleep(delay)
            request = urllib.request.Request(f"{WIKIPEDIA_API}?{query}",
                                            headers={"User-Agent": USER_AGENT})
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    payload = json.load(response)
                break
            except (urllib.error.HTTPError, urllib.error.URLError,
                    json.JSONDecodeError, TimeoutError) as error:
                if verbose:
                    print(f"  request failed ({error}); "
                          f"retry {attempt + 1}/{MAX_RETRIES}")
                delay *= 2
        if payload is None or "query" not in payload:
            continue

        # Redirects and normalisation mean the returned title need not match the
        # requested one, so map both back.
        alias: Dict[str, str] = {}
        for group in ("normalized", "redirects"):
            for entry in payload["query"].get(group, []):
                alias[entry["to"]] = entry["from"]

        for page in payload["query"].get("pages", []):
            title = page.get("title", "")
            requested = alias.get(title, title).replace(" ", "_")
            if page.get("missing"):
                out[requested] = None
                continue
            revisions = page.get("revisions") or [{}]
            content = (revisions[0].get("slots", {}).get("main", {})
                       .get("content", ""))
            out[requested] = lead_section(content) if content else None
        if verbose:
            print(f"  fetched {len(batch)} article(s) in one request")
    return out


def team_titles(season: int, names: pl.DataFrame,
                teams: List[str]) -> Dict[str, str]:
    """Wikipedia article title per team abbreviation.

    Only ever used for the *current* season, so current team names are correct --
    the Washington and Oakland renamings that break a historical mapping do not
    arise.

    Args:
        season: Season year.
        names: ``team_names.parquet``.
        teams: Abbreviations to cover.

    Returns:
        dict: ``{abbr: article title}``, omitting abbreviations with no name.
    """
    lookup = dict(zip(names["team_abbr"], names["team_name"]))
    out = {}
    for abbr in teams:
        name = lookup.get(abbr)
        if name:
            out[abbr] = f"{season}_{name.replace(' ', '_')}_season"
    return out


def from_nflverse(by_game: pl.DataFrame) -> pl.DataFrame:
    """One row per team-season, from the games actually coached.

    The modal coach, plus the evidence that the choice was needed:
    ``games_coached`` against ``team_games``, and ``coach_changed_midseason``.

    Args:
        by_game: ``coaches_by_game.parquet``.

    Returns:
        pl.DataFrame: ``season``, ``team``, ``head_coach``, ``games_coached``,
        ``team_games``, ``coach_changed_midseason``, ``source``.
    """
    regular = by_game.filter(pl.col("game_type") == "REG")
    totals = (regular.group_by(["season", "team"])
              .agg(pl.len().alias("team_games"),
                   pl.col("coach").n_unique().alias("n_coaches")))
    counted = (regular.group_by(["season", "team", "coach"])
               .agg(pl.len().alias("games_coached")))
    modal = (counted.sort(["season", "team", "games_coached", "coach"],
                          descending=[False, False, True, False])
             .group_by(["season", "team"], maintain_order=True)
             .first())
    return (
        modal.join(totals, on=["season", "team"], how="left")
        .select(
            pl.col("season").cast(pl.Int32), "team",
            pl.col("coach").alias("head_coach"),
            pl.col("games_coached").cast(pl.Int32),
            pl.col("team_games").cast(pl.Int32),
            (pl.col("n_coaches") > 1).alias("coach_changed_midseason"),
            pl.lit(None, dtype=pl.String).alias("offensive_coordinator"),
            pl.lit(None, dtype=pl.String).alias("defensive_coordinator"),
            pl.lit("nflverse").alias("source"),
        )
        .sort(["season", "team"])
    )


def from_wikipedia(season: int, titles: Dict[str, str],
                   verbose: bool = True) -> pl.DataFrame:
    """Coaching staff for one season, from the season articles.

    Args:
        season: Season year.
        titles: ``{abbr: article title}`` from :func:`team_titles`.
        verbose: Print per-team progress.

    Returns:
        pl.DataFrame: One row per team reached, with ``source`` set to
        ``"wikipedia"``. Teams whose article could not be read are omitted rather
        than filled in.
    """
    fetched = fetch_many(sorted(titles.values()), verbose=verbose)

    rows = []
    for abbr, title in sorted(titles.items()):
        # Three outcomes, kept distinct. Absent from `fetched` means the request
        # failed; None means the article does not exist; present-but-no-field means
        # a real gap in the article. Collapsing these is what made a transient
        # failure on New England look like missing data when its article had all
        # three fields, and reported Minnesota as having no page at all.
        if title not in fetched:
            if verbose:
                print(f"  {abbr:<4} request failed for {title}")
            continue
        wikitext = fetched[title]
        if wikitext is None:
            if verbose:
                print(f"  {abbr:<4} no article yet: {title}")
            continue
        parsed = parse_infobox(wikitext)
        if not parsed["head_coach"]:
            if verbose:
                print(f"  {abbr:<4} no coach field in {title}")
            continue
        rows.append({
            "season": season, "team": abbr, **parsed,
            "games_coached": None, "team_games": None,
            "coach_changed_midseason": False, "source": "wikipedia",
        })
        if verbose:
            extras = [v for k, v in parsed.items() if k != "head_coach" and v]
            print(f"  {abbr:<4} {parsed['head_coach']:<22}"
                  f"{'  +' + ', '.join(extras) if extras else ''}")

    if not rows:
        return pl.DataFrame(schema={
            "season": pl.Int32, "team": pl.String, "head_coach": pl.String,
            "offensive_coordinator": pl.String, "defensive_coordinator": pl.String,
            "games_coached": pl.Int32, "team_games": pl.Int32,
            "coach_changed_midseason": pl.Boolean, "source": pl.String,
        })
    return pl.DataFrame(rows).with_columns(
        pl.col("season").cast(pl.Int32),
        pl.col("games_coached").cast(pl.Int32),
        pl.col("team_games").cast(pl.Int32),
    )


def build(current_season: Optional[int] = None, offline: bool = False,
          verbose: bool = True) -> pl.DataFrame:
    """Assemble the committed coaching table.

    Args:
        current_season: The unplayed season to take from Wikipedia. Defaults to
            ``config.yaml``'s season.
        offline: Skip Wikipedia entirely and use nflverse for every season,
            accepting that the current season may be stale.
        verbose: Print progress.

    Returns:
        pl.DataFrame: One row per team-season.

    Raises:
        FileNotFoundError: When ``R/GetCoaches.R`` has not been run.
    """
    for path in (COACHES_BY_GAME_PARQUET, TEAM_NAMES_PARQUET):
        if not path.is_file():
            raise FileNotFoundError(
                f"{path} is missing. Generate it with `Rscript R/GetCoaches.R`."
            )

    by_game = pl.read_parquet(COACHES_BY_GAME_PARQUET)
    names = pl.read_parquet(TEAM_NAMES_PARQUET)
    season = get_season() if current_season is None else current_season

    played = from_nflverse(by_game)
    if offline:
        return played

    teams = sorted(played.filter(pl.col("season") == season)["team"].unique()
                   .to_list())
    if not teams:
        teams = sorted(by_game.filter(pl.col("season") == season)["team"]
                       .unique().to_list())
    if verbose:
        print(f"Wikipedia, {season} ({len(teams)} teams, "
              f"~{len(teams) * REQUEST_INTERVAL:.0f}s):")

    live = from_wikipedia(season, team_titles(season, names, teams), verbose)

    # Wikipedia wins for the current season, and only for teams it actually
    # resolved. A team it could not read keeps the nflverse row, flagged by its
    # `source` column so a stale coach is identifiable rather than invisible.
    kept = played.filter(
        (pl.col("season") != season)
        | ~pl.col("team").is_in(live["team"].to_list() or [""])
    )
    return pl.concat([kept, live], how="diagonal").sort(["season", "team"])


def summarise(staff: pl.DataFrame) -> str:
    """A printable description of the built table.

    Args:
        staff: :func:`build` output.

    Returns:
        str: Coverage by source, and coordinator coverage.
    """
    lines = [f"{staff.height} team-seasons, "
             f"{staff['season'].min()}-{staff['season'].max()}, "
             f"{staff['head_coach'].n_unique()} distinct head coaches"]
    for (source,), rows in staff.group_by(["source"], maintain_order=True):
        oc = rows["offensive_coordinator"].is_not_null().sum()
        lines.append(f"  {source:<10}{rows.height:>5} rows, "
                     f"{oc} with an offensive coordinator")
    mid = staff.filter(pl.col("coach_changed_midseason")).height
    lines.append(f"  {mid} team-seasons had more than one head coach")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.coaches",
        description="Build the committed coaching-staff table.")
    parser.add_argument("--season", type=int,
                        help="the unplayed season to read from Wikipedia")
    parser.add_argument("--offline", action="store_true",
                        help="nflverse only; the current season may be stale")
    parser.add_argument("--out", help="destination parquet")
    args = parser.parse_args(argv)

    staff = build(current_season=args.season, offline=args.offline)
    path = COACHING_STAFF_PARQUET if args.out is None else args.out
    COACHING_STAFF_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    staff.write_parquet(path)
    print()
    print(summarise(staff))
    print(f"\nwrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
