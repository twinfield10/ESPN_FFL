"""Home — every league you are in, and what each one needs from you today.

The landing page's question is not "how is this team doing", it is **"which of my
five teams needs me before kickoff"**. That is a different shape from every other
tab: Roster, Free Agents and Matchup each answer deeply about *one* league, and to
find the league with a ruled-out starter in it you had to open all five.

So this module summarises one league into a handful of decisions and a record, and
the Home tab draws five of them side by side. It reads the same functions the deep
tabs read -- :func:`lineup.swaps`, :func:`lineup.upgrades`, :func:`matchup_sim.side`
-- rather than re-deriving anything, which is what makes a card and the tab it links
to agree. A landing page that disagreed with the page it sends you to would be worse
than no landing page.

Streamlit-free in the sense that matters: no ``st.*`` call is made here, so every
decision is unit-testable. It imports :mod:`matchup_sim`, which reaches for
``st.cache_resource`` on one function, exactly as that module does itself -- and
:mod:`tests.test_matchup_sim` is the precedent for testing through that.

**No store reads either.** The caller loads the frames and passes them in, so the
per-league summary is cacheable on ``(season, league_key, week)`` in the view rather
than being an uncacheable IO call in here. See :mod:`views.home_tab`.

**On what counts as a played game.** An unplayed fixture is not absent from
``team_stats`` -- it is present, scored 0-0, and therefore recorded as a *tie*. Every
team in all ten leagues read 0-0-1 on 2026-09-09 with the season not yet started, and
``season_ties`` says so. So the record is recomputed from the weeks that actually have
points in them rather than read off the cumulative columns. See :func:`played`.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

from typing import Dict, List, NamedTuple, Optional, Sequence

import polars as pl

import lineup as lu
import matchup_sim as sim

#: The lineup you are about to play is worse than one you could field today.
#:
#: The same word :data:`lineup.UPGRADE_CRITICAL` uses, and deliberately: a card that
#: called something critical which the Free Agents tab called depth would be teaching
#: you to distrust both.
ACTION_CRITICAL = "critical"

#: Worth knowing, and nothing about Sunday changes if you ignore it.
ACTION_WARNING = "warning"

#: Severities in the order a card lists them.
SEVERITY_ORDER = (ACTION_CRITICAL, ACTION_WARNING)

#: Where an action sends you. The file path, because that is what ``st.switch_page``
#: takes and what ``main.PAGES`` registers.
ROUTE_ROSTER = "routes/roster.py"
ROUTE_FREE_AGENTS = "routes/free_agents.py"

#: Below this a fixture has not been played.
#:
#: Zero is not a threshold, it is the *absence* of a box score -- see the module
#: docstring. A real fantasy team cannot score nothing in a game that happened; it
#: would need an empty starting lineup, which ESPN does not allow you to submit.
#: Both sides at zero is an unplayed week, every time.
PLAYED_MIN_POINTS = 0.01


class Action(NamedTuple):
    """One thing to do about one league, and where to go and do it.

    Attributes:
        severity: :data:`ACTION_CRITICAL` or :data:`ACTION_WARNING`.
        icon: What the callout leads with. Matches
            :data:`views.weekly.UPGRADE_CALLOUTS` where the action came from an
            upgrade, so the same finding wears the same icon on both tabs.
        headline: One line of markdown. States the decision, not the metric.
        route: The route file that can act on it.
        button: What the link to that route is labelled.
    """
    severity: str
    icon: str
    headline: str
    route: str
    button: str


class LeagueSummary(NamedTuple):
    """One league, reduced to what a card shows.

    Attributes:
        league_key: ``config.yaml`` league key. What a card's buttons write into
            the league selector before navigating.
        display_name: The league's name.
        week: The week this summary is for.
        owner: Whose team is summarised -- ``meta["primary_owner"]``.
        projected: That team's projected points from the lineup ESPN has set.
        opponent: Who they play, or None when nothing knows the fixture.
        opponent_projected: The opponent's projection on the same basis.
        win: Probability ``owner`` outscores ``opponent``, or None with no fitted
            dispersion or no fixture. See :func:`matchup_sim.outcome`.
        margin: Projected margin, owner minus opponent. None without a fixture.
        record: ``(wins, losses, ties)`` over played weeks. Zeros before week 1
            resolves, which is honest -- see :func:`played`.
        rank: Where ``owner`` sits in :attr:`standings`, 1-based.
        teams: How many teams the league has.
        actions: Most urgent first. Empty is the common answer and the good one.
        standings: From :func:`standings`, or None without ``team_stats``.
        notes: Why something above is missing, in the reader's terms. Drawn as
            captions rather than swallowed.
    """
    league_key: str
    display_name: str
    week: int
    owner: Optional[str]
    projected: Optional[float]
    opponent: Optional[str]
    opponent_projected: Optional[float]
    win: Optional[float]
    margin: Optional[float]
    record: tuple
    rank: Optional[int]
    teams: Optional[int]
    actions: List[Action]
    standings: Optional[pl.DataFrame]
    notes: List[str]

    @property
    def worst(self) -> int:
        """Rank of this league's most urgent action, for ordering cards.

        Lower is more urgent. A league with nothing to do sorts last, which is the
        whole point of a landing page: the five cards put themselves in the order you
        should read them.

        Returns:
            int: Index into :data:`SEVERITY_ORDER`, or one past the end when clean.
        """
        return min((SEVERITY_ORDER.index(a.severity) for a in self.actions),
                   default=len(SEVERITY_ORDER))


def week_for(meta: dict, requested: Optional[int]) -> int:
    """Which week to summarise one league at.

    The sidebar's Week selector is offered from the *selected* league's
    ``weeks_present``, and Home draws every league -- so the requested week is not
    always a week this league has. Rather than showing an empty card or ignoring the
    control, the requested week is honoured where it exists and this league's own
    ``current_week`` is used where it does not. The card says when the two differ,
    because a card silently answering about a different week than the one the selector
    reads would be the worst of the three options.

    Args:
        meta: The store's ``meta.json``.
        requested: The globally selected week, or None to always use this league's own.

    Returns:
        int: A week this league actually has, where it has any.
    """
    current = int(meta.get("current_week") or 1)
    present = {int(w) for w in (meta.get("weeks_present") or [])}
    if requested is None:
        return current
    requested = int(requested)
    if not present or requested in present:
        return requested
    return current if current in present else max(present)


def played(fixtures: pl.DataFrame) -> pl.DataFrame:
    """The team-weeks in a ``team_stats`` frame that have actually been played.

    **This is not a defensive filter, it is the difference between a real record and
    a fabricated one.** ESPN reports an unplayed fixture as a 0-0 result, which
    ``scrape_team_stats`` faithfully records as a tie, which the cumulative
    ``season_ties`` column then faithfully accumulates. On 2026-09-09, with no game
    yet played, every team in every league read **0-0-1**. ``box_score_available`` is
    no help -- it is ``true`` for those rows too.

    Args:
        fixtures: A ``team_stats`` frame, already narrowed to one season.

    Returns:
        pl.DataFrame: The subset with points on the board. Empty before week 1.
    """
    if fixtures.is_empty():
        return fixtures
    scored = ((pl.col("team_score").fill_null(0.0) >= PLAYED_MIN_POINTS)
              | (pl.col("opp_score").fill_null(0.0) >= PLAYED_MIN_POINTS))
    if "is_regular_season" in fixtures.columns:
        scored = scored & pl.col("is_regular_season").fill_null(True)
    return fixtures.filter(scored)


def lineup_projections(rostered: pl.DataFrame, points_column: str = "TRUE_Points"
                       ) -> Dict[str, float]:
    """Every team's projected points from **the lineup ESPN currently has set**.

    Lineup-as-set rather than best-available, so this number is the same one the
    Matchup tab quotes and the same one a card's win probability is computed from.
    Using the optimal lineup here would rank the league on lineups nobody has
    submitted.

    Args:
        rostered: One week of one league's rows, free agents already excluded.
        points_column: Which projection to total.

    Returns:
        dict: ``{team_owner: projected}``. Missing an owner whose rows hold no
        starter, which is a roster nobody has set rather than a zero.
    """
    if rostered.is_empty() or "team_owner" not in rostered.columns:
        return {}

    out: Dict[str, float] = {}
    for owner, rows in rostered.partition_by("team_owner", as_dict=True).items():
        name = owner[0] if isinstance(owner, tuple) else owner
        starters, total = lu.current_lineup(rows.to_dicts(), points_column)
        if starters:
            out[str(name)] = total
    return out


def records(fixtures: pl.DataFrame) -> pl.DataFrame:
    """Win-loss-tie and points for/against, over played weeks only.

    Recomputed rather than read off ``season_wins``/``season_losses``/``season_ties``
    because the third of those counts unplayed fixtures as ties -- see :func:`played`,
    which this expects to have been applied already.

    Args:
        fixtures: From :func:`played`.

    Returns:
        pl.DataFrame: ``team_owner``, ``wins``, ``losses``, ``ties``, ``games``,
        ``points_for``, ``points_against``, ``win_pct``. Empty in, empty out.
    """
    if fixtures.is_empty():
        return pl.DataFrame(schema={
            "team_owner": pl.String, "wins": pl.Int64, "losses": pl.Int64,
            "ties": pl.Int64, "games": pl.Int64, "points_for": pl.Float64,
            "points_against": pl.Float64, "win_pct": pl.Float64})

    won = pl.col("team_score") > pl.col("opp_score")
    lost = pl.col("team_score") < pl.col("opp_score")
    return (
        fixtures.group_by("team_owner")
        .agg(
            wins=won.sum(),
            losses=lost.sum(),
            ties=(~won & ~lost).sum(),
            games=pl.len(),
            points_for=pl.col("team_score").sum(),
            points_against=pl.col("opp_score").sum(),
        )
        # A tie is half a win, which is how every league here breaks the table, and
        # it is `games` rather than wins+losses so a team on a bye is not flattered.
        .with_columns(win_pct=(pl.col("wins") + 0.5 * pl.col("ties"))
                      / pl.col("games"))
    )


def standings(fixtures: pl.DataFrame, projections: Dict[str, float],
              week: int) -> pl.DataFrame:
    """The league table: what has happened, and what this week projects.

    **Projection is the tiebreaker, not the ranking.** Order is win percentage, then
    points for -- the two things that actually decide a fantasy table -- and only
    where those are level does this week's projected lineup separate two teams. That
    is a real tiebreak in November and it is the *whole* order in September: with no
    game played every team is 0-0 on 0.0 points, so the table you land on in week 1
    is ranked by the blend rather than being six rows of zeroes in alphabetical
    order.

    Both numbers are shown side by side on purpose. ``This Week`` is what was scored
    and ``Projected`` is what we think will be, and the pair is the only honest way to
    show a week that is halfway through -- one column would have to lie about the
    other half.

    Args:
        fixtures: One season of ``team_stats``, unfiltered. :func:`played` is applied
            here.
        projections: From :func:`lineup_projections`.
        week: The week ``This Week`` and ``Projected`` are for.

    Returns:
        pl.DataFrame: ``Rk``, ``Owner``, ``W-L-T``, ``Win%``, ``PF``, ``PA``,
        ``This Week``, ``Projected``. Empty when there is nothing to rank at all.
    """
    table = records(played(fixtures))

    # Everyone with a roster belongs on the table, including a team whose fixtures
    # are all unplayed -- which before week 1 is every team in the league.
    owners = sorted(set(projections) | set(table["team_owner"].to_list()))
    if not owners:
        return pl.DataFrame()

    base = pl.DataFrame({"team_owner": owners}).join(table, on="team_owner",
                                                     how="left")
    scored = played(fixtures)
    this_week = (
        scored.filter(pl.col("week") == week).select(["team_owner", "team_score"])
        if not scored.is_empty() and "week" in scored.columns
        else pl.DataFrame(schema={"team_owner": pl.String,
                                  "team_score": pl.Float64})
    )

    return (
        base
        .join(this_week, on="team_owner", how="left")
        .with_columns(
            projected=pl.col("team_owner").replace_strict(
                projections, default=None, return_dtype=pl.Float64),
            wins=pl.col("wins").fill_null(0),
            losses=pl.col("losses").fill_null(0),
            ties=pl.col("ties").fill_null(0),
            win_pct=pl.col("win_pct").fill_null(0.0),
            points_for=pl.col("points_for").fill_null(0.0),
            points_against=pl.col("points_against").fill_null(0.0),
        )
        .sort(["win_pct", "points_for", "projected"],
              descending=[True, True, True], nulls_last=True)
        .with_row_index("Rk", offset=1)
        .select(
            pl.col("Rk").cast(pl.Int64),
            pl.col("team_owner").alias("Owner"),
            pl.format("{}-{}-{}", "wins", "losses", "ties").alias("W-L-T"),
            pl.col("win_pct").alias("Win%"),
            pl.col("points_for").alias("PF"),
            pl.col("points_against").alias("PA"),
            pl.col("team_score").alias("This Week"),
            pl.col("projected").alias("Projected"),
        )
    )


def lineup_action(changes: Sequence[lu.Swap], unavailable_starters: int) -> Optional[Action]:
    """The start/sit decision, as one line, or None when the lineup is right.

    **Critical only when someone who cannot play is in the lineup.** A start/sit
    worth points is a decision; a ruled-out starter is an error, and the difference is
    worth keeping because most weeks produce the first and almost none produce the
    second. Flattening them would mean five cards permanently shouting.

    Args:
        changes: From :func:`lineup.swaps`.
        unavailable_starters: How many players in the *set* lineup cannot play.

    Returns:
        Action | None: None when there is nothing to change.
    """
    if unavailable_starters:
        return Action(
            severity=ACTION_CRITICAL, icon="🚨",
            headline=(f"**{unavailable_starters} starter"
                      f"{'s' if unavailable_starters > 1 else ''}** cannot play"
                      + (f", and **{sum(c.gain for c in changes):+.1f}** points are "
                         f"available from {len(changes)} change"
                         f"{'s' if len(changes) > 1 else ''}"
                         if changes else "")),
            route=ROUTE_ROSTER, button="Roster")
    if not changes:
        return None
    return Action(
        severity=ACTION_WARNING, icon="↕️",
        headline=(f"**{sum(c.gain for c in changes):+.1f} points** from "
                  f"{len(changes)} lineup change"
                  f"{'s' if len(changes) > 1 else ''}"),
        route=ROUTE_ROSTER, button="Roster")


def waiver_action(upgrades: Sequence[lu.Upgrade]) -> Optional[Action]:
    """The wire, as one line, or None when nobody available beats anybody rostered.

    One action rather than one per slot. A card has room for a count and a verdict;
    which slot and which player is what the Free Agents tab is for, and it will say
    the same thing because it is the same :func:`lineup.upgrades` call.

    Critical wins when there is any -- a starter being out-projected is a different
    claim from a bench player being out-projected, and the card leads with the
    stronger one, exactly as :func:`views.weekly.render_upgrades` does.

    Args:
        upgrades: From :func:`lineup.upgrades`, over the **whole** pool.

    Returns:
        Action | None: None when the roster is clean.
    """
    if not upgrades:
        return None

    critical = [u for u in upgrades if u.severity == lu.UPGRADE_CRITICAL]
    depth = [u for u in upgrades if u.severity == lu.UPGRADE_DEPTH]

    if critical:
        best = max(critical, key=lambda u: u.margin)
        extra = f", {len(depth)} on the bench" if depth else ""
        return Action(
            severity=ACTION_CRITICAL, icon="🚨",
            headline=(f"**{len(critical)} starting slot"
                      f"{'s' if len(critical) > 1 else ''}** beaten by the wire{extra}"
                      f" — best is **{best.best.get('player_name')}** at "
                      f"`{best.slot}`, `{best.margin:+.1f}`"),
            route=ROUTE_FREE_AGENTS, button="Free Agents")

    best = max(depth, key=lambda u: u.margin)
    return Action(
        severity=ACTION_WARNING, icon="↕️",
        headline=(f"**{len(depth)} bench slot{'s' if len(depth) > 1 else ''}** "
                  f"beaten by the wire — best is "
                  f"**{best.best.get('player_name')}** at `{best.slot}`, "
                  f"`{best.margin:+.1f}`"),
        route=ROUTE_FREE_AGENTS, button="Free Agents")


def summarise(league_key: str, display_name: str, week: int, meta: dict,
              lineups: pl.DataFrame, team_stats: Optional[pl.DataFrame],
              fitted: Optional[dict], free_agent_owner: str,
              points_column: str = "TRUE_Points") -> LeagueSummary:
    """Reduce one league-week to a card.

    Every number here is computed by the function the deep tab computes it with, so
    a card cannot disagree with the page it links to.

    Args:
        league_key: ``config.yaml`` league key.
        display_name: The league's name.
        week: Week to summarise.
        meta: The store's ``meta.json``.
        lineups: The league's whole ``lineups`` frame. Narrowed to ``week`` here.
        team_stats: The league's ``team_stats``, or None when it was never built --
            which costs the fixture, the win probability and the table, and nothing
            else.
        fitted: From :func:`matchup_sim.model`. Read once by the caller and passed
            in, so five cards do not look it up five times.
        free_agent_owner: ``session.FREE_AGENT_OWNER``. Passed rather than imported
            because :mod:`session` draws widgets and this module must not.
        points_column: Which projection to work in.

    Returns:
        LeagueSummary: With ``notes`` naming anything it could not answer.
    """
    notes: List[str] = []
    blank = LeagueSummary(
        league_key=league_key, display_name=display_name, week=week, owner=None,
        projected=None, opponent=None, opponent_projected=None, win=None,
        margin=None, record=(0, 0, 0), rank=None, teams=meta.get("team_count"),
        actions=[], standings=None, notes=notes)

    week_rows = lineups.filter(pl.col("week") == week)
    if week_rows.is_empty():
        notes.append(f"Nothing stored for week {week}.")
        return blank

    rostered = week_rows.filter(pl.col("team_owner") != free_agent_owner)
    pool = week_rows.filter(pl.col("team_owner") == free_agent_owner)
    owner = meta.get("primary_owner")
    if not owner or owner not in set(rostered["team_owner"].to_list()):
        notes.append("This store does not say which team is yours "
                     "(`meta['primary_owner']`).")
        return blank

    slots = lu.slot_counts(rostered, meta)
    rows = rostered.filter(pl.col("team_owner") == owner).to_dicts()

    current, projected = lu.current_lineup(rows, points_column)
    optimal, _ = lu.optimal_lineup(rows, slots, points_column)
    changes = lu.swaps(current, optimal, points_column)
    starting_ids = {r.get("player_id") for r in current}
    unavailable = [r for r in lu.bye_and_out(rows)
                   if r.get("player_id") in starting_ids]

    actions = [a for a in (lineup_action(changes, len(unavailable)),
                           waiver_action(lu.upgrades(pool.to_dicts(), rows, slots,
                                                     points_column)))
               if a is not None]
    actions.sort(key=lambda a: SEVERITY_ORDER.index(a.severity))

    projections = lineup_projections(rostered, points_column)

    # --- the fixture, which only `team_stats` knows ----------------------
    opponent = opponent_projected = win = margin = None
    table: Optional[pl.DataFrame] = None
    record, rank = (0, 0, 0), None

    if team_stats is None:
        notes.append("No `team_stats`, so no fixture, no win probability and no "
                     "table. It is opt-in — about 40 seconds of ESPN round-trips.")
    else:
        season = int(meta.get("season") or 0)
        year = (team_stats.filter(pl.col("year") == season)
                if season and "year" in team_stats.columns else team_stats)
        table = standings(year, projections, week)

        if not table.is_empty():
            mine = table.filter(pl.col("Owner") == owner)
            if not mine.is_empty():
                rank = int(mine["Rk"][0])
            counts = records(played(year)).filter(pl.col("team_owner") == owner)
            if not counts.is_empty():
                record = (int(counts["wins"][0]), int(counts["losses"][0]),
                          int(counts["ties"][0]))

        fixtures = year.filter(pl.col("week") == week)
        if fixtures.is_empty():
            notes.append(f"Week {week} is not in `team_stats` yet.")
        else:
            identities = (rostered.select(["team_owner", "team_name"]).unique()
                          .iter_rows() if "team_name" in rostered.columns
                          else [(o, None) for o in projections])
            pairs, identity_notes = sim.opponent_map(
                fixtures.select(["team_owner", "team_name", "opp_owner", "opp_name"])
                .to_dicts(), list(identities))
            notes.extend(identity_notes)

            opponent = pairs.get(owner)
            if opponent and opponent in projections:
                opponent_rows = (rostered.filter(pl.col("team_owner") == opponent)
                                 .to_dicts())
                theirs, _ = lu.current_lineup(opponent_rows, points_column)
                result = sim.outcome(sim.side(owner, current, points_column, fitted),
                                     sim.side(opponent, theirs, points_column,
                                              fitted))
                opponent_projected = projections[opponent]
                win, margin = result.win, result.margin
            elif opponent:
                notes.append(f"**{opponent}** has no stored roster this week.")
            else:
                notes.append("No opponent this week — a bye, or a team the fixture "
                             "list and the rosters spell differently.")

    return LeagueSummary(
        league_key=league_key, display_name=display_name, week=week, owner=owner,
        projected=projected, opponent=opponent,
        opponent_projected=opponent_projected, win=win, margin=margin,
        record=record, rank=rank, teams=meta.get("team_count") or len(projections),
        actions=actions, standings=table, notes=notes)


def by_urgency(summaries: Sequence[LeagueSummary]) -> List[LeagueSummary]:
    """Cards in the order they should be read: worst first.

    Stable, so within a severity the leagues keep the order the viewer's own
    :attr:`auth.Viewer.leagues` put them in -- which is roughly how often each is
    opened. A landing page that reshuffled every card every week would cost more in
    hunting than the ordering saves.

    Args:
        summaries: One per league.

    Returns:
        list: Same summaries, most urgent first.
    """
    return sorted(summaries, key=lambda s: s.worst)
