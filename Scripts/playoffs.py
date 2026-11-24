"""Playoff seeding and the bracket it produces, "if the season ended today".

Pure functions over a table of played games -- no ESPN client, no store reads -- so the
app can call them in a render path and the tests can call them on six-row frames.

**The rules come from ESPN, not from us.** ``mSettings.scheduleSettings`` carries every
league's playoff team count, seeding tiebreaker (``playoffSeedingRule``), reseeding flag
and the matchup periods the rounds are played in. :func:`settings_from_espn` reduces it
to the dict ``meta.json`` stores as ``playoff_settings``. ``Scripts.simulation_utils``
used to *assume* total points because "the API doesn't seem to get this data
correctly" -- it does; ``espn_api`` just never read it.

**Division winners are seeded first.** ESPN has no setting for it and no flag says so,
but every 2026 division league shows it: GOP_Degenerates' 6-5 division leaders sit
above a 7-3-1 wildcard, John_ATL's 7-4 two seed above an 8-3 team. Within each group
(division winners, then wildcards) teams order by win percentage, then the league's
tiebreaker, then points for.

**Head-to-head among three or more teams** uses each team's win percentage in the
games the tied teams played against one another. Where that separates some teams but
not others, the still-tied subset is broken again on its own games, and anything H2H
cannot separate falls to points for. Checked against ESPN's live ``playoffSeed`` in
every league that has a tie under the rule -- see ``tests/test_playoffs.py``.

**Jeffs_League is why the input is a games table rather than ESPN's standings.** The
league was recreated after week 9, so ESPN's records -- and its ``playoffSeed`` -- cover
weeks 10 onward only. The stitched ``team_stats`` (``Scripts.prior_league``) carries
the whole season with both owners pinned, which is everything head-to-head needs.

See ``docs/plans/57-playoff-bracket.md``.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

import polars as pl

#: ESPN ``playoffSeedingRule`` values this module can apply, and their labels.
TIEBREAK_LABELS = {
    "H2H_RECORD": "Head-to-head record",
    "TOTAL_POINTS_SCORED": "Total points for",
    "INTRA_DIVISION_RECORD": "Division record",
    "TOTAL_POINTS_AGAINST": "Total points against",
}

#: Scores are floats summed over a season; two teams "level on points" are level to
#: the hundredth ESPN displays, not to the last bit.
POINTS_DECIMALS = 2


# --- settings -------------------------------------------------------------

def settings_from_espn(schedule_settings: Dict[str, Any],
                       team_count: Optional[int] = None) -> Dict[str, Any]:
    """Reduce ESPN's ``scheduleSettings`` to what seeding and the bracket need.

    Args:
        schedule_settings: ``settings.scheduleSettings`` from ``view=mSettings``.
        team_count: Teams in the league, recorded for display.

    Returns:
        dict: ``playoff_team_count``, ``seeding_rule``, ``reseed``,
        ``regular_season_periods``, ``divisions`` (``{id: name}``) and
        ``playoff_periods`` -- ``[[matchup_period, [scoring weeks]], ...]`` for every
        matchup period after the regular season, in order. JSON-safe.
    """
    regular = int(schedule_settings.get("matchupPeriodCount") or 0)
    periods = schedule_settings.get("matchupPeriods") or {}
    playoff_periods = sorted(
        [int(mp), [int(w) for w in weeks]]
        for mp, weeks in periods.items() if int(mp) > regular
    )
    return {
        "playoff_team_count": int(schedule_settings.get("playoffTeamCount") or 0),
        "seeding_rule": schedule_settings.get("playoffSeedingRule") or "TOTAL_POINTS_SCORED",
        "reseed": bool(schedule_settings.get("playoffReseed")),
        "regular_season_periods": regular,
        "divisions": {str(d["id"]): d.get("name") or f"Division {d['id']}"
                      for d in schedule_settings.get("divisions") or []},
        "playoff_periods": playoff_periods,
        "team_count": team_count,
    }


# --- standings table ------------------------------------------------------

@dataclass
class Table:
    """Standings with everything a tiebreak can ask for, in plain Python.

    Plain dicts rather than a polars frame because the odds simulation seeds tens of
    thousands of simulated seasons; :func:`seed` is the frame-shaped wrapper over the
    same ordering, so there is one implementation of ESPN's rules, not two.

    Attributes:
        teams: Every team key, sorted.
        divisions: ``{team: division name}``.
        w, l, t: Wins, losses, ties.
        pf, pa: Points for and against.
        vs: ``{(team, opp): [score, games]}`` -- ``score`` counts a win as 1 and a
            tie as 0.5, from ``team``'s side.
    """

    teams: List[str]
    divisions: Dict[str, str]
    w: Dict[str, int] = field(default_factory=dict)
    l: Dict[str, int] = field(default_factory=dict)
    t: Dict[str, int] = field(default_factory=dict)
    pf: Dict[str, float] = field(default_factory=dict)
    pa: Dict[str, float] = field(default_factory=dict)
    vs: Dict[Tuple[str, str], List[float]] = field(default_factory=dict)

    @classmethod
    def empty(cls, divisions: Dict[str, str]) -> "Table":
        teams = sorted(divisions)
        z = {x: 0 for x in teams}
        return cls(teams, dict(divisions), dict(z), dict(z), dict(z),
                   {x: 0.0 for x in teams}, {x: 0.0 for x in teams}, {})

    def copy(self) -> "Table":
        return Table(self.teams, self.divisions, dict(self.w), dict(self.l), dict(self.t),
                     dict(self.pf), dict(self.pa), {k: list(v) for k, v in self.vs.items()})

    def add(self, a: str, b: str, sa: float, sb: float) -> None:
        """Record one game between ``a`` and ``b``."""
        for x, y, sx, sy in ((a, b, sa, sb), (b, a, sb, sa)):
            if x not in self.w:
                continue
            res = 1.0 if sx > sy else 0.0 if sx < sy else 0.5
            self.w[x] += res == 1.0
            self.l[x] += res == 0.0
            self.t[x] += res == 0.5
            self.pf[x] += sx
            self.pa[x] += sy
            cell = self.vs.setdefault((x, y), [0.0, 0])
            cell[0] += res
            cell[1] += 1

    def pct(self, x: str) -> float:
        games = self.w[x] + self.l[x] + self.t[x]
        return (self.w[x] + 0.5 * self.t[x]) / games if games else 0.0

    def points(self, x: str) -> float:
        return round(self.pf[x], POINTS_DECIMALS)

    def played_all(self, group: Sequence[str]) -> bool:
        """Whether every pair in ``group`` has met at least once."""
        return all(self.vs.get((a, b), (0, 0))[1] > 0
                   for i, a in enumerate(group) for b in group[i + 1:])

    def among(self, x: str, group: Sequence[str]) -> Tuple[float, int]:
        """``x``'s ``(score, games)`` against the rest of ``group``."""
        score, games = 0.0, 0
        for y in group:
            if y != x:
                s, g = self.vs.get((x, y), (0.0, 0))
                score += s
                games += g
        return score, games


def table_from_games(games: pl.DataFrame, divisions: Dict[str, str]) -> Table:
    """Build a :class:`Table` from one row per team per game.

    Args:
        games: ``team``, ``opp``, ``team_score``, ``opp_score`` -- both rows of each
            game, as :func:`games_from_team_stats` returns them.
        divisions: ``{team: division}`` for every team.
    """
    table = Table.empty(divisions)
    if games.is_empty():
        return table
    for a, b, sa, sb in games.select("team", "opp", "team_score", "opp_score").iter_rows():
        # Each game arrives twice; record it from the lexically first side only.
        if (a < b) or (b not in table.w):
            table.add(a, b, float(sa), float(sb))
    return table


def records(games: pl.DataFrame) -> pl.DataFrame:
    """Win-loss-tie, win percentage and points for/against per team.

    Args:
        games: One row per team per played game: ``team``, ``opp``, ``team_score``,
            ``opp_score``.

    Returns:
        pl.DataFrame: ``team``, ``wins``, ``losses``, ``ties``, ``games``, ``win_pct``,
        ``points_for``, ``points_against``.
    """
    won = pl.col("team_score") > pl.col("opp_score")
    lost = pl.col("team_score") < pl.col("opp_score")
    return (
        games.group_by("team")
        .agg(wins=won.sum().cast(pl.Int64), losses=lost.sum().cast(pl.Int64),
             ties=(~won & ~lost).sum().cast(pl.Int64), games=pl.len().cast(pl.Int64),
             points_for=pl.col("team_score").sum(),
             points_against=pl.col("opp_score").sum())
        .with_columns(win_pct=(pl.col("wins") + 0.5 * pl.col("ties")) / pl.col("games"))
    )


# --- ordering ---------------------------------------------------------------

def _tiers(group: List[str], key) -> List[List[str]]:
    """Split ``group`` into tiers of equal ``key(team)``, best (largest) first."""
    values = {x: key(x) for x in group}
    return [[x for x in group if values[x] == v]
            for v in sorted(set(values.values()), reverse=True)]


def _h2h_best(group: List[str], table: Table) -> str:
    """The team that takes the next seed out of a group level on win percentage.

    Head-to-head counts only when every pair in the group has played; otherwise, and
    when head-to-head leaves everyone level, the most points for takes it. A smaller
    group still level at the top is settled the same way on its own games.
    """
    by_points = sorted(group, key=lambda x: (-table.points(x), x))
    if len(group) == 1 or not table.played_all(group):
        return by_points[0]

    def h2h(x: str) -> float:
        score, games = table.among(x, group)
        return score / games if games else 0.0

    top = _tiers(group, h2h)[0]
    if len(top) == len(group):
        return by_points[0]
    return _h2h_best(top, table)


def _break_tie(group: List[str], table: Table, rule: str) -> List[str]:
    """Order a group of teams level on win percentage, by the league's rule.

    Never leaves a tie: points for, then team key, settle whatever the rule cannot.
    """
    if len(group) == 1:
        return group

    def by_points(teams: List[str]) -> List[str]:
        return sorted(teams, key=lambda x: (-table.points(x), x))

    if rule == "H2H_RECORD":
        # One seed at a time: find the best of what is left, take it, and start over
        # with the rest -- a three-way circle broken on points leaves two teams who
        # *did* settle it between them.
        out: List[str] = []
        remaining = list(group)
        while remaining:
            best = _h2h_best(remaining, table)
            out.append(best)
            remaining.remove(best)
        return out

    if rule == "TOTAL_POINTS_AGAINST":
        return [x for tier in _tiers(group, lambda x: round(table.pa[x], POINTS_DECIMALS))
                for x in by_points(tier)]

    if rule == "INTRA_DIVISION_RECORD":
        def div_pct(x: str) -> float:
            mates = [y for y in table.teams
                     if y != x and table.divisions[y] == table.divisions[x]]
            score, games = table.among(x, [x, *mates])
            return score / games if games else -1.0
        return [x for tier in _tiers(group, div_pct) for x in by_points(tier)]

    return by_points(group)


def rank(teams: Sequence[str], table: Table, rule: str) -> List[str]:
    """Order ``teams`` by win percentage, breaking ties by ``rule``."""
    return [x for tier in _tiers(list(teams), table.pct)
            for x in _break_tie(tier, table, rule)]


def order(table: Table, rule: str) -> Tuple[List[str], List[str]]:
    """Every team in seed order, and the division winners.

    Division winners are seeded first when the league has more than one division.

    Returns:
        tuple: ``(order, division_winners)``.
    """
    names = sorted(set(table.divisions.values()))
    winners: List[str] = []
    if len(names) > 1:
        for name in names:
            members = [x for x in table.teams if table.divisions[x] == name]
            winners.append(rank(members, table, rule)[0])
    rest = [x for x in table.teams if x not in winners]
    return rank(winners, table, rule) + rank(rest, table, rule), winners


def seed(games: pl.DataFrame, divisions: Dict[str, str], settings: Dict[str, Any]) -> pl.DataFrame:
    """Seed every team as if the regular season ended after the games given.

    Args:
        games: One row per team per **played regular-season** game: ``team``,
            ``opp``, ``team_score``, ``opp_score``.
        divisions: ``{team: division name}`` for every team, including any that have
            not played.
        settings: From :func:`settings_from_espn`.

    Returns:
        pl.DataFrame: One row per team in seed order -- ``seed``, ``team``,
        ``division``, ``division_winner``, ``in_playoffs``, ``wins``, ``losses``,
        ``ties``, ``win_pct``, ``points_for``, ``points_against``, ``tiebreak``.
    """
    table = table_from_games(games, divisions)
    rule = settings.get("seeding_rule") or "TOTAL_POINTS_SCORED"
    seeded, winners = order(table, rule)
    n = int(settings.get("playoff_team_count") or 0)
    return pl.DataFrame({
        "seed": list(range(1, len(seeded) + 1)),
        "team": seeded,
        "division": [table.divisions[x] for x in seeded],
        "division_winner": [x in winners for x in seeded],
        "in_playoffs": [i < n for i in range(len(seeded))],
        "wins": [table.w[x] for x in seeded],
        "losses": [table.l[x] for x in seeded],
        "ties": [table.t[x] for x in seeded],
        "win_pct": [table.pct(x) for x in seeded],
        "points_for": [table.points(x) for x in seeded],
        "points_against": [round(table.pa[x], POINTS_DECIMALS) for x in seeded],
        "tiebreak": _tiebreak_notes(seeded, winners, table, rule),
    }, schema_overrides={"wins": pl.Int64, "losses": pl.Int64, "ties": pl.Int64})


def _tiebreak_notes(seeded: List[str], winners: List[str], table: Table,
                    rule: str) -> List[str]:
    """One line per team saying which tie it was in and what settled it.

    Empty for a team level with nobody in its group (division winners and wildcards
    are ranked separately, so a 7-4 division winner is not "tied" with a 7-4
    wildcard). The wording names the rule that applied, not every step of it.
    """
    out = []
    for x in seeded:
        pool = winners if x in winners else [y for y in seeded if y not in winners]
        level = [y for y in pool if table.pct(y) == table.pct(x)]
        if len(level) == 1:
            out.append("")
            continue
        record = f"{table.w[x]}-{table.l[x]}" + (f"-{table.t[x]}" if table.t[x] else "")
        how = TIEBREAK_LABELS.get(rule, "Total points for")
        if rule == "H2H_RECORD":
            if not table.played_all(level):
                how = "Total points for (not every pair has played)"
            else:
                wins = sum(table.vs.get((x, y), (0, 0))[0] for y in level if y != x)
                games = sum(table.vs.get((x, y), (0, 0))[1] for y in level if y != x)
                how = f"Head-to-head, {wins:g}-{games - wins:g} vs the tied teams"
        out.append(f"{len(level)}-way tie at {record}: {how}")
    return out


# --- bracket --------------------------------------------------------------

@dataclass(frozen=True)
class Slot:
    """One side of a bracket game: a seed, or the winner of an earlier game."""

    seed: Optional[int] = None
    winner_of: Optional[int] = None  # index into the previous round's games

    @property
    def is_bye(self) -> bool:
        return self.seed is None and self.winner_of is None


@dataclass(frozen=True)
class Game:
    """One bracket game. ``high`` is the better seed's side, ``low`` the other."""

    round: int
    index: int
    high: Slot
    low: Slot


def byes(playoff_teams: int) -> int:
    """First-round byes: the gap to the next power of two, given to the top seeds."""
    if playoff_teams < 2:
        return 0
    size = 1
    while size < playoff_teams:
        size *= 2
    return size - playoff_teams


def _bracket_order(size: int) -> List[int]:
    """Seeds in bracket order: 1 v size, then the half that meets them, and so on.

    ``[1, 8, 4, 5, 2, 7, 3, 6]`` for 8 -- adjacent pairs are first-round games, and
    adjacent winners meet next, so 1 and 2 can only meet in the final.
    """
    order = [1]
    while len(order) < size:
        n = len(order) * 2
        order = [s for x in order for s in (x, n + 1 - x)]
    return order


def bracket(playoff_teams: int) -> List[List[Game]]:
    """The fixed (non-reseeding) bracket for ``playoff_teams`` seeds.

    The field rounds up to a power of two and the missing seeds are byes, which go to
    the top seeds: a 6-team bracket is 3 v 6 and 4 v 5, with 1 and 2 waiting.

    A bye is not represented as a game: round 1 lists only the games actually played,
    and a team on a bye enters round 2 as a seeded :class:`Slot`.

    Args:
        playoff_teams: ``playoff_team_count``.

    Returns:
        list: One list of :class:`Game` per round. Empty for fewer than two teams.
    """
    if playoff_teams < 2:
        return []
    size = 1
    while size < playoff_teams:
        size *= 2
    order = _bracket_order(size)

    # Each entry is what advances out of that position in the bracket.
    current: List[Slot] = [Slot(seed=s) if s <= playoff_teams else Slot() for s in order]
    rounds: List[List[Game]] = []
    r = 0
    while len(current) > 1:
        r += 1
        games: List[Game] = []
        nxt: List[Slot] = []
        for a, b in zip(current[0::2], current[1::2]):
            if a.is_bye or b.is_bye:
                nxt.append(b if a.is_bye else a)
                continue
            high, low = (a, b) if _rank_key(a) <= _rank_key(b) else (b, a)
            games.append(Game(round=r, index=len(games), high=high, low=low))
            nxt.append(Slot(winner_of=len(games) - 1))
        rounds.append(games)
        current = nxt
    return rounds


def _rank_key(slot: Slot) -> float:
    """Seeds sort first, by seed; a winner-of slot after them."""
    return float(slot.seed) if slot.seed is not None else float("inf")


def round_names(n_rounds: int) -> List[str]:
    """``["Quarterfinals", "Semifinals", "Final"]`` and so on, ending at the final."""
    tail = ["Final", "Semifinals", "Quarterfinals"]
    return [tail[n_rounds - 1 - i] if n_rounds - 1 - i < len(tail)
            else f"Round {i + 1}" for i in range(n_rounds)]


def round_weeks(settings: Dict[str, Any], n_rounds: int) -> List[List[int]]:
    """Scoring weeks each round is played in, from ``playoff_periods``.

    One matchup period per round in every league here, two scoring weeks long in
    Winfield_Football. Missing periods come back as empty lists rather than a guess.
    """
    periods = [weeks for _, weeks in settings.get("playoff_periods") or []]
    return [periods[i] if i < len(periods) else [] for i in range(n_rounds)]


# --- from the store -------------------------------------------------------

def games_from_team_stats(team_stats: pl.DataFrame, season: int,
                          through_week: Optional[int] = None) -> pl.DataFrame:
    """Played regular-season games from a ``team_stats`` artifact.

    Args:
        team_stats: The stitched artifact (every season the league has).
        season: The season to keep.
        through_week: Last completed week. Games after it are dropped even when
            they carry points, so a week in progress never counts as a result.

    Returns:
        pl.DataFrame: ``week``, ``team``, ``opp``, ``team_score``, ``opp_score``.
    """
    df = team_stats.filter(pl.col("year") == season)
    if "is_regular_season" in df.columns:
        df = df.filter(pl.col("is_regular_season").fill_null(True))
    if through_week is not None:
        df = df.filter(pl.col("week") <= through_week)
    # ESPN reports an unplayed fixture as 0-0; see app/home.py `played`.
    df = df.filter((pl.col("team_score").fill_null(0) > 0) | (pl.col("opp_score").fill_null(0) > 0))
    return df.select(
        pl.col("week").cast(pl.Int64),
        pl.col("team_owner").alias("team"), pl.col("opp_owner").alias("opp"),
        pl.col("team_score").cast(pl.Float64), pl.col("opp_score").cast(pl.Float64))


def divisions_from_team_stats(team_stats: pl.DataFrame, season: int) -> Dict[str, str]:
    """``{team_owner: division}`` for the season, from each team's latest row."""
    df = team_stats.filter(pl.col("year") == season).sort("week")
    return {r["team_owner"]: r["team_division"] or ""
            for r in df.group_by("team_owner").agg(pl.col("team_division").last())
            .iter_rows(named=True)}


def fixtures_from_league(league: Any, regular_season_periods: int) -> List[List[Any]]:
    """Every regular-season fixture, as ``[matchup_period, owner, owner]``.

    ``team_stats`` stops at the current week, so the weeks still to be played exist
    only on the live league; the nightly records them in ``meta.json`` for the odds
    simulation. Owners rather than team ids, because ``team_stats`` keys on
    ``team_owner`` -- and for Jeffs_League those are the names pinned across the
    recreation (``fetch_utils.pin_owner_names`` runs before this is read).

    Args:
        league: An ``espn_api`` League whose teams carry ``owner`` and ``schedule``.
        regular_season_periods: ``matchupPeriodCount``.

    Returns:
        list: One entry per game, each listed once.
    """
    out: List[List[Any]] = []
    for team in getattr(league, "teams", []) or []:
        for period, opp in enumerate(getattr(team, "schedule", []) or [], start=1):
            if period > regular_season_periods:
                break
            if opp is None or getattr(opp, "team_id", None) is None:
                continue
            if team.team_id < opp.team_id:
                out.append([period, team.owner, opp.owner])
    return sorted(out)
