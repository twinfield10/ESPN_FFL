"""Who to root for on Sunday, across every league at once.

The app answers "do I win *this* matchup" one league at a time. This answers the
question you actually have on the couch: **when Ja'Marr Chase catches a touchdown,
is that good for me or not?** Sometimes it is good twice and bad once. On week 2 of
2026, eighteen players were started in more than one of the viewer's four leagues
and eight of those were on both sides of the ledger -- owned in one, faced in
another.

**The currency is expected wins, and that choice is load-bearing rather than
cosmetic.** ``docs/DATA_CATALOGUE.md`` §8 is blunt about it: *never compare points
across leagues; ranks compare, points do not*. It is true inside this viewer's own
four -- Josh Allen projects 23.27 in three of them and **31.00** in
``gop_degenerates``, which pays six for a passing touchdown. So nothing here adds a
point in one league to a point in another. Each league's points become a
*probability*, inside that league's own scoring, and probabilities are unitless.
:func:`expected_wins` is the sum, and :attr:`Share.stake` is the only cross-league
number this module publishes as a quantity.

:attr:`Share.rate` is the exception and it is documented as one -- see
:func:`scoring_divergence`, which measures the problem rather than asserting it.

**Everything is closed form.** ``docs/plans/42-weekly-matchup-odds.md`` established
that the normal approximation is calibrated to within 0.2pp at team level, so a
Monte Carlo would contribute nothing but its own sampling noise. Differentiating it
with respect to one player needs no more machinery than the CDF already there.

**Streamlit-free, and takes loaded frames.** Same split as :mod:`home`: the view
does the IO so it can cache on ``store.version``, and this module is unit-testable
without a store. See ``docs/plans/40-frontend-restructure.md``.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple

import polars as pl

import lineup as lu
import matchup_sim as sim
from Scripts.live import LIVE_REMAINING
from Scripts.outcomes import weekly as wk

#: How far apart the two ends of a player's week are taken to be.
#:
#: The p10/p90 pair plan 28 publishes and :meth:`matchup_sim.Side.band` already
#: draws, so "his good week" means the same thing on this tab as on Matchup.
Z = wk.Z_P90

#: Below this, a player's summed stake is reported as a hedge rather than as a
#: direction, and gets no fill.
#:
#: **Measured rather than picked.** On week 2 an ordinary starter in a single league
#: carries ``|stake|`` of 0.22 and the smallest of the 45 of them is 0.14, so this is
#: about a tenth of the quietest real rooting interest on the page. The two players
#: it catches are the ones it should: Josh Allen at 0.007 and Omarion Hampton at
#: 0.011, each owned in one league and faced in another at leverages that almost
#: exactly matched. Calling those a direction -- colouring them green and printing
#: "root for him" -- would be the table saying something it does not mean.
HEDGE_EPSILON = 0.02

#: How far two leagues' projections for the same player may differ before the
#: summed :attr:`Share.rate` stops being a quantity. Well above float noise and
#: well below a real scoring difference -- Josh Allen's 23.27 against 31.00 is 33%.
SCORING_TOLERANCE = 0.02


class Interest(NamedTuple):
    """One league's reason to care about one player.

    Attributes:
        league_key: ``config.yaml`` league key.
        display_name: The league's name, for the chip on the row.
        sign: ``+1`` when the viewer starts him, ``-1`` when the opponent does.
        projected: His points in *this league's* scoring -- which is why they are
            never added across leagues. See the module docstring.
        sd: His fitted weekly spread, from :func:`Scripts.outcomes.weekly.player_sd`
            at his **remaining** projection. Zero once his game is final, which is a
            statement rather than a gap.
        rate: What one of his points is worth to the viewer's win probability here,
            signed. ``phi(d) / sd_margin``.
        stake: The viewer's win probability at his p90 minus at his p10, signed.
    """
    league_key: str
    display_name: str
    sign: int
    projected: float
    sd: float
    rate: float
    stake: float


class Share(NamedTuple):
    """One player, totalled across every league the viewer meets him in.

    Attributes:
        player_id: ESPN's player id, which is the same integer in every league --
            verified, and the reason this tab is possible at all.
        player_name: ESPN's spelling, which ``clean_lineups`` treats as the naming
            authority.
        position: His position, for the colour ruler.
        pro_team: His NFL team.
        game_state: ``pre`` | ``in`` | ``post`` | ``bye``, from ESPN's scoreboard.
        stake: **Δ expected wins.** How much the viewer's expected wins across all
            his leagues move if this player has a p90 week instead of a p10 one.
            Signed: positive means root for him. Safe to compare across players and
            across leagues, because it is built out of probabilities.
        rate: Summed :attr:`Interest.rate`. Win probability per fantasy point --
            an index rather than a quantity whenever the leagues disagree about
            what a point is. See :func:`scoring_divergence`.
        owned: Display names of the leagues where the viewer starts him.
        faced: Display names of the leagues where his opponent does.
        interests: The per-league rows behind the totals, in store order.
    """
    player_id: Optional[int]
    player_name: str
    position: Optional[str]
    pro_team: Optional[str]
    game_state: Optional[str]
    stake: float
    rate: float
    owned: Tuple[str, ...]
    faced: Tuple[str, ...]
    interests: Tuple[Interest, ...]

    @property
    def conflicted(self) -> bool:
        """Whether the viewer is on both sides of this player."""
        return bool(self.owned) and bool(self.faced)

    @property
    def hedged(self) -> bool:
        """Whether being on both sides has cancelled out.

        The interesting corner, and it happens: Jaxon Smith-Njigba netted +0.000 on
        week 2 -- started by the viewer in one league and by his opponent in
        another, at leverages that matched to three decimals.
        """
        return self.conflicted and abs(self.stake) < HEDGE_EPSILON


class LeagueMatchup(NamedTuple):
    """One league's fixture for the week, priced.

    Attributes:
        league_key: ``config.yaml`` league key.
        display_name: The league's name.
        owner: The viewer's ``team_owner`` in this league.
        opponent: Who he plays.
        mine: The viewer's side.
        theirs: His opponent's.
        win: The viewer's win probability, or None with no fitted dispersion.
        starters: His starting rows, each carrying ``"slot"``.
        opp_starters: His opponent's.
        points_column: Which points column both sides were totalled on. Carried
            rather than assumed downstream, because :func:`lineup.live_points_column`
            resolves it per store and a league whose store predates live scoring
            would otherwise have its players read off a column its totals did not
            use -- every stake wrong, and nothing about it looking wrong.
    """
    league_key: str
    display_name: str
    owner: str
    opponent: str
    mine: sim.Side
    theirs: sim.Side
    win: Optional[float]
    starters: List[dict]
    opp_starters: List[dict]
    points_column: str


def _player_sd(fitted: Optional[dict], row: dict) -> float:
    """One starter's remaining spread.

    Priced off what is **still to come**, exactly as :func:`matchup_sim.side` prices
    a team: once a player's game is over his points are a fact, and
    ``Var = phi*mu + mu^2/k`` at his remaining projection returns zero for him. That
    is what makes this tab drain toward the players who still have football left.

    Args:
        fitted: From :func:`matchup_sim.model`, or None.
        row: A lineups row.

    Returns:
        float: Standard deviation, 0.0 with no fitted model.
    """
    if fitted is None:
        return 0.0
    remaining = row.get(LIVE_REMAINING)
    if remaining is None:
        remaining = row.get("TRUE_Points")
    return wk.player_sd(fitted, row.get("player_position"), remaining)


def _stake(side_mu: float, side_sd: float, other_mu: float, other_sd: float,
           mu: float, sd: float, sign: int, z: float = Z) -> float:
    """The viewer's win probability at a player's p90, minus at his p10.

    **Exact, not a derivative.** The player is removed from his side's mean *and*
    its spread and then re-inserted at each end of his own interval, so the answer
    stays right for a player who is large relative to his matchup -- a quarterback
    in a superflex league is a fifth of a lineup. :func:`matchup_sim.swing` moves
    the mean and leaves the spread alone, which is correct for its own question
    (what is a lineup change worth) and wrong for this one.

    The sign takes care of itself. When ``sign`` is negative the player is on the
    opponent's side, the perturbation is applied there, and a better game for him
    comes back as a lower number for the viewer.

    Args:
        side_mu: Total of the side this player starts on.
        side_sd: That side's spread.
        other_mu: The other side's total.
        other_sd: The other side's spread.
        mu: The player's projection.
        sd: His spread.
        sign: ``+1`` if his side is the viewer's, ``-1`` if it is the opponent's.
        z: Normal quantile for the two ends.

    Returns:
        float: Signed change in the **viewer's** win probability. Zero when he has
        no spread left, which is a settled game rather than a missing model.
    """
    if sd <= 0:
        return 0.0
    rest_mu = side_mu - mu
    rest_sd = max(0.0, side_sd ** 2 - sd ** 2) ** 0.5

    # Floored at zero: no player in any of these leagues can score negative points,
    # so a negative low end is a modelling artefact of a symmetric interval on a
    # right-skewed distribution. Same reasoning as `matchup_sim.Side.band`.
    low, high = max(0.0, mu - z * sd), mu + z * sd

    if sign > 0:
        return (wk.win_probability(rest_mu + high, rest_sd, other_mu, other_sd)
                - wk.win_probability(rest_mu + low, rest_sd, other_mu, other_sd))
    return (wk.win_probability(other_mu, other_sd, rest_mu + high, rest_sd)
            - wk.win_probability(other_mu, other_sd, rest_mu + low, rest_sd))


def _rate(mine: sim.Side, theirs: sim.Side, sign: int) -> float:
    """What one fantasy point is worth, in win probability.

    The derivative of :func:`Scripts.outcomes.weekly.win_probability` with respect
    to either mean: ``phi(d) / sd_margin``. It is the number that answers the
    original question about weighting -- a point in a coin flip is worth more than a
    point in a matchup already won -- and it depends on the matchup alone, so every
    starter on a side shares it.

    Args:
        mine: The viewer's side.
        theirs: His opponent's.
        sign: ``+1`` for a player of the viewer's, ``-1`` for one of the opponent's.

    Returns:
        float: Signed probability per point. Zero when nothing is uncertain.
    """
    spread = (mine.sd ** 2 + theirs.sd ** 2) ** 0.5
    if spread <= 0:
        return 0.0
    d = (mine.projected - theirs.projected) / spread
    return sign * wk.normal_pdf(d) / spread


def league_matchup(league_key: str, display_name: str, owner: str, week: int,
                   meta: dict, lineups: pl.DataFrame,
                   team_stats: Optional[pl.DataFrame],
                   fitted: Optional[dict], free_agent_owner: str,
                   points_column: Optional[str] = None
                   ) -> Tuple[Optional[LeagueMatchup], List[str]]:
    """One league's fixture for the week, or a reason there is none.

    Two artifacts, because neither has both halves: ``lineups`` knows every
    player's projection and whose roster he is on and nothing about who plays whom;
    ``team_stats`` knows the fixture and nothing about players. The join between
    them is :func:`matchup_sim.opponent_map`, which is a three-pass reconciliation
    because ESPN spells a team differently in its box-score and roster views.

    Args:
        league_key: ``config.yaml`` league key.
        display_name: The league's name.
        owner: The viewer's ``team_owner`` here, from :func:`auth.owner_for`.
        week: Week to read, already resolved by :func:`home.week_for`.
        meta: The store's ``meta.json``.
        lineups: The league's whole ``lineups`` frame.
        team_stats: Its ``team_stats``, or None when that was never built -- which
            costs the fixture and therefore the whole league, since there is nothing
            to root for without an opponent.
        fitted: From :func:`matchup_sim.model`. Read once by the caller.
        free_agent_owner: ``session.FREE_AGENT_OWNER``. Passed rather than imported
            because :mod:`session` draws widgets and this module must not.
        points_column: None resolves to the live number where the store has one.

    Returns:
        tuple: ``(matchup, notes)``. The matchup is None whenever the league cannot
        contribute, and ``notes`` then says why -- named rather than dropped,
        because a league silently missing from a cross-league total is how a wrong
        number looks completely normal.
    """
    notes: List[str] = []
    if points_column is None:
        points_column = lu.live_points_column(lineups.columns)

    week_rows = lineups.filter(pl.col("week") == week)
    if week_rows.is_empty():
        return None, [f"**{display_name}** has nothing stored for week {week}."]

    rostered = week_rows.filter(pl.col("team_owner") != free_agent_owner)
    if owner not in set(rostered["team_owner"].to_list()):
        return None, [f"**{display_name}** has no roster for {owner} this week."]

    if team_stats is None:
        return None, [
            f"**{display_name}** has no `team_stats`, so nothing knows who plays "
            f"whom. It is opt-in and costs the league's whole history."]

    season = int(meta.get("season") or 0)
    year = (team_stats.filter(pl.col("year") == season)
            if season and "year" in team_stats.columns else team_stats)
    fixtures = year.filter(pl.col("week") == week)
    if fixtures.is_empty():
        return None, [f"**{display_name}** has no week {week} fixture stored yet."]

    identities = list(
        rostered.select(["team_owner", "team_name"]).unique().iter_rows()
        if "team_name" in rostered.columns
        else [(o, None) for o in rostered["team_owner"].unique().to_list()])
    fixture_rows = fixtures.select(
        ["team_owner", "team_name", "opp_owner", "opp_name"]
        + [c for c in sim.ADJUSTMENT_COLUMNS
           if c in fixtures.columns]).to_dicts()
    pairs, identity_notes = sim.opponent_map(fixture_rows, identities)
    adjusted = sim.adjustments(fixture_rows, identities)
    notes.extend(identity_notes)

    opponent = pairs.get(owner)
    if not opponent:
        return None, notes + [
            f"**{display_name}**: no opponent this week — a bye, or a team the "
            f"fixture list and the rosters spell differently."]
    if opponent not in set(rostered["team_owner"].to_list()):
        return None, notes + [
            f"**{display_name}**: **{opponent}** has no stored roster this week."]

    mine_rows = rostered.filter(pl.col("team_owner") == owner).to_dicts()
    opp_rows = rostered.filter(pl.col("team_owner") == opponent).to_dicts()
    starters, _ = lu.current_lineup(mine_rows, points_column)
    opp_starters, _ = lu.current_lineup(opp_rows, points_column)

    # The commissioner's points are part of the total and no part of the spread --
    # ESPN folds them into the score it publishes, so leaving them out would put
    # this tab's probability at odds with the Matchup tab's and with the scoreboard.
    mine = sim.side(owner, starters, points_column, fitted,
                    adjustment=adjusted.get(owner, 0.0))
    theirs = sim.side(opponent, opp_starters, points_column, fitted,
                      adjustment=adjusted.get(opponent, 0.0))

    return LeagueMatchup(
        league_key=league_key, display_name=display_name, owner=owner,
        opponent=opponent, mine=mine, theirs=theirs,
        win=sim.outcome(mine, theirs).win,
        starters=starters, opp_starters=opp_starters,
        points_column=points_column), notes


def expected_wins(matchups: Sequence[LeagueMatchup]) -> Optional[float]:
    """The viewer's expected wins this week, summed over his leagues.

    The one aggregate that is safe to add across leagues, and the currency
    :attr:`Share.stake` is denominated in. See the module docstring on why points
    are not.

    Args:
        matchups: From :func:`league_matchup`.

    Returns:
        float | None: 0.0 to ``len(matchups)``. None when no league could be
        priced, which is what a missing dispersion model looks like.
    """
    priced = [m.win for m in matchups if m.win is not None]
    return sum(priced) if priced else None


def shares(matchups: Sequence[LeagueMatchup], fitted: Optional[dict],
           z: float = Z) -> List[Share]:
    """Every player in the viewer's eight starting lineups, ranked by what he moves.

    Bench players are absent by construction rather than by a filter: they cannot
    score for anybody, so their stake is exactly zero and there is no row to draw.
    So is every player on a third team -- this prices the viewer's own fixtures, and
    what another team's result does to his seeding is playoff-odds swing, which
    ``docs/plans/42-weekly-matchup-odds.md`` still lists as owed.

    Args:
        matchups: From :func:`league_matchup`, in store order.
        fitted: From :func:`matchup_sim.model`. Passed in rather than looked up,
            because that function is ``st.cache_resource``-wrapped and this module
            must stay runnable without a Streamlit session.
        z: Normal quantile for the two ends of a player's week.

    Returns:
        list: One :class:`Share` per distinct player, sorted by ``|stake|``
        descending and then by name, so the order is stable when a whole slate is
        settled and every stake is zero.
    """
    collected: Dict[object, List[Tuple[Interest, dict]]] = {}

    for matchup in matchups:
        for rows, sign, side, other in (
                (matchup.starters, 1, matchup.mine, matchup.theirs),
                (matchup.opp_starters, -1, matchup.theirs, matchup.mine)):
            rate = _rate(matchup.mine, matchup.theirs, sign)
            for row in rows:
                mu = float(row.get(matchup.points_column) or 0.0)
                sd = _player_sd(fitted, row) if side.modelled else 0.0
                interest = Interest(
                    league_key=matchup.league_key,
                    display_name=matchup.display_name,
                    sign=sign, projected=mu, sd=sd, rate=rate,
                    stake=_stake(side.projected, side.sd, other.projected,
                                 other.sd, mu, sd, sign, z))
                # Keyed on the ESPN id, which is the same integer in every league.
                # Falls back to the name for the one population that has no id
                # anywhere -- team defences match on name alone.
                key = row.get("player_id")
                if key is None:
                    key = ("name", row.get("player_name"))
                collected.setdefault(key, []).append((interest, row))

    out: List[Share] = []
    for entries in collected.values():
        interests = tuple(i for i, _ in entries)
        head = entries[0][1]
        out.append(Share(
            player_id=head.get("player_id"),
            player_name=head.get("player_name") or "—",
            position=head.get("player_position") or head.get("primaryPosition"),
            pro_team=head.get("pro_team"),
            # The furthest-along state he is in anywhere. Identical in every league
            # -- it is a fact about his NFL game, not about a roster -- but read off
            # the rows rather than assumed, since a league can be mid-patch.
            game_state=_game_state(entries),
            stake=sum(i.stake for i in interests),
            rate=sum(i.rate for i in interests),
            owned=tuple(i.display_name for i in interests if i.sign > 0),
            faced=tuple(i.display_name for i in interests if i.sign < 0),
            interests=interests))

    out.sort(key=lambda s: (-abs(s.stake), s.player_name))
    return out


def _game_state(entries: Sequence[Tuple[Interest, dict]]) -> Optional[str]:
    """The player's NFL game state, from whichever league's row carries one.

    Args:
        entries: His ``(interest, row)`` pairs.

    Returns:
        str | None: ``pre`` | ``in`` | ``post`` | ``bye``, or None on a store
        written before live scoring.
    """
    for _, row in entries:
        state = row.get("game_state")
        if state:
            return str(state)
    return None


def scoring_divergence(shares_: Sequence[Share]
                       ) -> List[Tuple[str, float, float]]:
    """Players whose projection differs between the leagues that start him.

    The measured version of ``docs/DATA_CATALOGUE.md`` §8 -- *never compare points
    across leagues* -- applied to the leagues actually on the page rather than
    asserted in the abstract. It is what licenses or withdraws the summed
    :attr:`Share.rate`: a rate is per *point*, and two leagues that disagree about
    what a point is cannot have their rates added.

    :attr:`Share.stake` is unaffected and needs no such guard, which is the whole
    reason it is the headline.

    Args:
        shares_: From :func:`shares`.

    Returns:
        list: ``(player_name, low, high)`` for each player the leagues disagree
        about, worst first. Empty means the rate column is a quantity.
    """
    out: List[Tuple[str, float, float]] = []
    for share in shares_:
        values = [i.projected for i in share.interests if i.projected > 0]
        if len(values) < 2:
            continue
        low, high = min(values), max(values)
        if high - low > SCORING_TOLERANCE * high:
            out.append((share.player_name, low, high))
    out.sort(key=lambda row: row[1] / row[2])
    return out


def net_projected(share: Share) -> float:
    """Owned minus faced projected points, for the no-model fallback.

    What the tab can still rank on when there is no fitted dispersion: it cannot
    weight by how close a matchup is, which is most of the point, but "you own 36
    points of him and face 20" is still an answer. Carries the same
    across-league caveat as :attr:`Share.rate` and more so.

    Args:
        share: From :func:`shares`.

    Returns:
        float: Signed points.
    """
    return sum(i.sign * i.projected for i in share.interests)
