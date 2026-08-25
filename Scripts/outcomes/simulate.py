"""The room-level joint draw: a backup's season is decided on another player's row.

:mod:`Scripts.outcomes.distribution` gives each player a season-points distribution of his
own. That is the cheap half. The expensive half is that **the unit of simulation is the
position room, not the player** -- and no per-player marginal has a channel for it.

Measured, paired within player-season: an RB2 goes from 5.09 opportunities a game to
12.93, and 4.15 points a game to 9.86, when the lead back sits. Over an eight-game absence
that is **+46 points**, roughly 3.5x the entire range of the one board column that tries to
price a backup. His season is genuinely bimodal, and which mode he is in is a function of
the RB1's availability. A marginal interval fitted on his own residuals reports one smear
over both worlds and calls the middle of it his projection.

So: draw each room's availability once, redistribute the vacated opportunity inside the
room week by week, and let the bimodality emerge from the shared draw rather than
asserting it.

**Four things this deliberately does not do**, each of them a decision rather than an
omission:

* *Receiver rooms get no transfer.* See :mod:`Scripts.outcomes.vacancy`.
* *Absences are not contiguous.* A player's missed weeks are drawn as an exchangeable
  subset rather than as a block. Real absences cluster, and modelling that would change
  how two absences in one room overlap -- it changes no mean, and it changes the variance
  of the overlap only. Named here rather than hidden.
* *Role is not drawn.* Who the RB1 *is* comes from the pre-season depth chart and is
  treated as known. ``docs/plans/33-role-resolution.md`` measures that as wrong about a
  third of the time and owns the fix; :func:`room_order` is the seam it plugs into, so
  that lands without a rewrite here. Keeping it out means G-D2 measures one mechanism.
* *Efficiency does not move with the transfer.* A player who inherits work gets a scalar
  multiplier on his opportunity, not a new per-opportunity rate.
  :mod:`Scripts.usage.predictive` measures why: conditional on opportunity, bounded rates
  are 1.08x-1.79x overdispersed against 5.6-8.1x for games and 13x-99x for volume. The
  transfer is opportunity; what he does with it is close to sampling noise.

See ``docs/plans/28-outcome-distributions.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts.usage import availability as av
from Scripts.usage import context as ctx
from Scripts.usage import season as sn

#: Rank a player is given when the depth chart does not list him.
#:
#: Matching :data:`Scripts.usage.season.DEFAULT_DEPTH_RANK`, and for the same reason: an
#: unlisted player is buried, not a starter, and 589 of 909 rows on the 2026 pull are in
#: that bucket. Sending them to rank 1 would hand every room several leads.
UNLISTED_RANK: int = 3

#: Whether the shipped simulation draws a role rather than trusting the depth chart.
#:
#: **False, measured.** Plan 33 phase 3's G-R2 asked whether drawing the true rank from
#: ``P(true | listed, cohort)`` makes the interval better. Walk-forward 2021-2025 it is
#: worth **+0.1pp** of coverage overall -- +1.3pp for a room's lead, whose job turns out
#: to be the uncertain one, and **-0.7pp** for the backups it was expected to help.
#:
#: The interval it would have to improve sits at 0.730 against a nominal 0.800, seven
#: points out, so it fails G-R2's five-point bar on its own before any comparison. Kept
#: and tested so the measurement is reproducible; off by default.
ROLE_DRAW: bool = False

#: Positions whose rooms are simulated jointly.
#:
#: Quarterbacks are absent and it is not an oversight: a quarterback room has one job and
#: no measured transfer table, and ``docs/plans/31-team-coherent-tomcat.md`` shows the
#: expected-games term there is so role-contaminated that two-QB teams sum to 21 projected
#: quarterback-games. Redistributing on that basis would be building on a known fault.
ROOM_POSITIONS: Tuple[str, ...] = ("RB", "TE")


@dataclass(frozen=True)
class Modulation:
    """How much of his projected season a player gets, split by where it came from.

    **The two halves are applied differently, and collapsing them into one number is
    wrong in a measurable direction.**

    *Availability* is weeks played over weeks the model expected, and it must be damped by
    the fitted games elasticity. ``expected_games`` carries role as well as health, so a
    player who plays twice the games the model expected does not produce twice the output
    -- :func:`Scripts.usage.season._conditional_mean` measures the undamped version
    over-projecting a realised total by up to 27%, and the elasticity that removes the bias
    lands at 0.32-0.49.

    *Gain* is opportunity inherited from an absent team-mate, and it is **not** damped. The
    elasticity corrects a role confound inside the model's own expected-games term; work
    handed over by a starter who is not there is not that. Damping it would shrink the one
    effect this whole module exists to price.

    **And the gain re-distributes rather than adds, which is a measurement rather than a
    modelling preference.** The season head is fitted on season totals, and a historical
    RB2's season total already contains the weeks his lead back missed. Measured: an RB2
    averages **9.86** opportunities a game across a season, against **5.09** in the weeks
    his lead plays and **12.93** in the weeks he does not. The projection is therefore
    already the blend, so adding the full transfer on top counts the expected inheritance
    twice -- on the 2026 board it lifted the median backup back from 123 points to 156,
    which is not a wider interval, it is a different projection.

    So the combined multiplier is rescaled to leave its own mean where the availability
    term alone puts it. What survives is the *variation*: the same expected season,
    correctly split between the world where the man ahead plays and the world where he
    does not. That is the bimodality this module exists for, and it is the half the
    projection genuinely does not have.

    Attributes:
        availability: ``(n_sims, n_players)``, weeks played over weeks expected.
        gain: ``(n_sims, n_players)``, inherited opportunity as a fraction of the player's
            own projected season.
        elasticity: :func:`Scripts.usage.predictive.key` to the fitted exponent.
        centre: Rescale so the transfer moves no mean. False leaves the double-count in
            place, which exists only so the backtest can measure what it is worth.
    """

    availability: np.ndarray
    gain: np.ndarray
    elasticity: Dict[str, float]
    centre: bool = True

    def scale(self, position: str, stat: str) -> np.ndarray:
        """The multiplier on ``mu`` for one (position, stat).

        Args:
            position: Position, to pick the fitted elasticity.
            stat: Stat name without the ``USG_`` prefix.

        Returns:
            np.ndarray: ``(n_sims, n_players)``.
        """
        from Scripts.usage import predictive as pv

        beta = self.elasticity.get(pv.key(position, stat),
                                   sn.DEFAULT_GAMES_ELASTICITY)
        factor = np.clip(self.availability, 0.0, None) ** beta
        if not self.gain.any():
            return factor

        total = factor + self.gain
        if not self.centre:
            return total
        # Multiplicative, so nothing can be pushed negative -- which subtracting the mean
        # would do for a player with a large expected inheritance and a bad availability
        # draw.
        wanted = factor.mean(axis=0, keepdims=True)
        got = total.mean(axis=0, keepdims=True)
        return np.divide(total * wanted, got, out=factor.copy(), where=got > 0)


@dataclass(frozen=True)
class Room:
    """One team's depth chart at one position.

    Attributes:
        team: Team abbreviation.
        position: Position group.
        players: Frame row indices, **ordered by depth rank**, best first.
        rank: Depth rank per player, parallel to ``players``.
    """

    team: str
    position: str
    players: Tuple[int, ...]
    rank: Tuple[int, ...]


def room_order(frame: pl.DataFrame, season: int,
               positions: Sequence[str] = ROOM_POSITIONS,
               baseline: Optional[np.ndarray] = None) -> List[Room]:
    """Who is in each room, and in what order.

    **The seam.** Everything about who leads a room is resolved here and nowhere else, so
    ``docs/plans/33-role-resolution.md`` phase 3 -- which draws the true rank from
    ``P(true | listed, cohort)`` instead of taking the chart at face value -- replaces this
    function and changes nothing downstream.

    Read from :func:`Scripts.usage.context.preseason_snapshot` rather than
    :func:`Scripts.usage.context.depth_features`, because the latter drops ``position`` and
    a room is a ``(team, position)`` pair. Positions are filtered **before** the per-player
    dedupe, or a back also listed as a kick returner loses the position that matters --
    ``KR`` sorts before ``RB``.

    **A depth rank alone does not identify a lead, and on nine of ten seasons it does not
    even try.** ``depth_rank`` is clipped to 3, and in the 2016-2024 upstream schema rank 1
    means "a starter" rather than "the best one" -- measured on the pre-season snapshots,
    **19 to 23 of the 64 RB and TE rooms list more than one rank-1 player**, up to three.
    (The 2025-onward schema is a strict ordering and has none.) Picking a lead from the
    tie by row order would make a third of every backtest fold's rooms arbitrary, and
    would put genuine co-starters into the understudy group that receives transfers.

    So ties inside a rank are broken by the model's own projected per-game opportunity,
    which is the quantity that actually separates a lead back from a committee-mate and is
    available before a snap is played. ``gsis_id`` remains the final tie-break, because a
    total order is what stopped :mod:`Scripts.outcomes.evidence` returning different
    numbers on consecutive runs.

    Args:
        frame: The player frame being simulated, carrying ``gsis_id`` and ``position``.
        season: Season whose pre-season chart to read.
        positions: Position groups to build rooms for.
        baseline: Per player, projected per-game opportunity, from
            :func:`baseline_opportunity`. None falls back to the chart alone, which is
            only safe on the strictly-ordered schema.

    Returns:
        list: One :class:`Room` per (team, position) with at least two players. A room of
        one has nobody to transfer to and is left out rather than special-cased later.

    Raises:
        FileNotFoundError: When the season's depth chart has not been pulled.
    """
    snapshot = ctx.preseason_snapshot(ctx.load_depth_charts([season]), season)
    scoped = (snapshot.filter(pl.col("position").is_in(list(positions)))
              .sort(["gsis_id", "depth_rank"])
              .unique(subset=["gsis_id"], keep="first")
              .select("gsis_id", "team", "position", "depth_rank"))

    index = {player: i for i, player in enumerate(frame["gsis_id"].to_list())}
    listed = {row["gsis_id"]: row for row in scoped.iter_rows(named=True)}

    grouped: Dict[Tuple[str, str], List[Tuple[int, float, str, int]]] = {}
    for player, position in zip(frame["gsis_id"].to_list(), frame["position"].to_list()):
        entry = listed.get(player)
        if entry is None or position not in positions:
            continue
        row = index[player]
        opportunity = (float(baseline[row])
                       if baseline is not None and np.isfinite(baseline[row]) else 0.0)
        grouped.setdefault((entry["team"], position), []).append(
            (int(entry["depth_rank"]), -opportunity, player, row))

    rooms = []
    for (team, position), members in sorted(grouped.items()):
        if len(members) < 2:
            continue
        # Rank first, then projected opportunity within a tied rank, then `gsis_id` to
        # make the order total -- the same discipline `evidence._ranked` needed after
        # tied reserves swapped places between runs and moved a room's volume.
        members.sort(key=lambda m: (m[0], m[1], m[2]))
        rooms.append(Room(team=team, position=position,
                          players=tuple(m[3] for m in members),
                          rank=tuple(m[0] for m in members)))
    return rooms


def draw_role_order(rng: np.random.Generator, frame: pl.DataFrame,
                    rooms: Sequence[Room], n_sims: int,
                    probabilities: Optional[Dict[Tuple[str, int], List[float]]] = None,
                    cohort_column: str = "usg_role_cohort"
                    ) -> List[np.ndarray]:
    """Who leads each room, drawn per simulation rather than read off the chart.

    **Plan 33 phase 3.** The rest of this module treats the pre-season depth chart as a
    fact. ``docs/plans/33-role-resolution.md`` measures how often it is one: a listed
    *settled* rank-1 really leads his room **58.8%** of the time, a mover **44.8%**, and a
    **rookie 35.6%** -- and the remainder is not noise, it is a distribution
    (a listed rookie RB1 is rank 2 22.6% of the time and rank 3 **41.8%**). Treating that
    as certain removes a variance channel that is largest exactly where a projection is
    least knowable, which is the finding plan 33 says is worth building on.

    Each member draws a true rank from his own ``P(true | listed, cohort)`` row,
    independently, and the room is ordered by the draw. **Independent draws do not
    constrain a room to exactly one rank-1**, and that is deliberate rather than
    overlooked: the ordering is what the simulation consumes, not the raw ranks, and two
    men drawing rank 1 is resolved by the same projected-opportunity tie-break that
    resolves the chart's own ties. The effect is that a listed lead keeps his job somewhat
    more often than his diagonal alone implies, which is the right direction -- the
    calibration's "true rank" is realised early-season opportunity, and the tie-break is
    projected opportunity.

    Args:
        rng: Explicit generator.
        frame: The player frame, carrying ``cohort_column``.
        rooms: From :func:`room_order`.
        n_sims: Simulations.
        probabilities: :func:`Scripts.usage.role.rank_probabilities` output. None loads
            it; empty leaves every room at its listed order, which is the behaviour
            before this existed.
        cohort_column: Where the player's cohort lives.

    Returns:
        list: One ``(n_sims, len(room.players))`` array per room, holding **positions
        within the room** ordered best-first. The listed order repeated ``n_sims`` times
        wherever the calibration has nothing to say.
    """
    from Scripts.usage import role as rl

    if probabilities is None:
        probabilities = rl.rank_probabilities()
    cohorts = (frame[cohort_column].to_list() if cohort_column in frame.columns
               else [None] * frame.height)

    out: List[np.ndarray] = []
    for room in rooms:
        size = len(room.players)
        listed = np.tile(np.arange(size), (n_sims, 1))
        if not probabilities:
            out.append(listed)
            continue

        drawn = np.empty((n_sims, size), dtype=float)
        known = False
        for position, (player, rank) in enumerate(zip(room.players, room.rank)):
            cohort = cohorts[player]
            vector = probabilities.get((str(cohort), int(rank))) if cohort else None
            if vector is None:
                # No cell for him -- an unlisted cohort, or a rank the table never saw.
                # He keeps his listed rank rather than being handed a distribution
                # nobody measured.
                drawn[:, position] = float(rank)
                continue
            known = True
            drawn[:, position] = rng.choice(
                len(vector), size=n_sims, p=vector) + 1.0

        if not known:
            out.append(listed)
            continue

        # Ties inside a drawn rank fall back to the listed order, which `room_order`
        # already resolved by projected opportunity and then by `gsis_id`. Adding the
        # position as a fractional term makes that a total order in one sort.
        keys = drawn + np.arange(size)[None, :] / (size + 1.0)
        out.append(np.argsort(keys, axis=1, kind="stable"))
    return out


def draw_weeks(rng: np.random.Generator, expected_games: np.ndarray,
               kappa: np.ndarray, slate: int, n_sims: int) -> np.ndarray:
    """Which weeks each player is available for, in each simulation.

    Games played comes from the model's own Beta-Binomial
    (:mod:`Scripts.usage.availability`) by inverse transform on its exact PMF -- the
    distribution is strongly left-skewed, most players are fine and a few miss most of the
    year, so a normal approximation would be wrong in the tail that decides whether a
    backup ever plays.

    The *count* is drawn from the model; **which** weeks are missed is then an exchangeable
    subset. That makes two players' absences overlap at the right rate on average. It does
    not make them cluster, which real absences do -- see the module docstring.

    Args:
        rng: Explicit generator.
        expected_games: Per player, the model's expected games.
        kappa: Per player, the Beta-Binomial concentration for his position.
        slate: Games the season offers.
        n_sims: Simulations.

    Returns:
        np.ndarray: Boolean ``(n_sims, n_players, slate)``, True where available.
    """
    n_players = expected_games.size
    mu = np.clip(expected_games / float(slate), 1e-6, 1.0 - 1e-6)

    games = np.empty((n_sims, n_players))
    for value in np.unique(kappa):
        rows = np.flatnonzero(kappa == value)
        if not rows.size:
            continue
        # One cumulative PMF per distinct concentration -- there are at most four --
        # rather than one per player.
        cdf = np.cumsum(av.pmf(slate, mu[rows], float(value)), axis=1)
        draws = rng.random((n_sims, rows.size, 1))
        games[:, rows] = (cdf[None, :, :] < draws).sum(axis=2)

    # Rank `slate` uniforms per player-sim and keep the lowest `games` of them. An
    # argsort-of-argsort is the rank; comparing it against the count marks exactly that
    # many weeks available, with no loop over weeks.
    noise = rng.random((n_sims, n_players, slate))
    order = noise.argsort(axis=2).argsort(axis=2)
    return order < games[:, :, None]


def opportunity_multiplier(rng: np.random.Generator, frame: pl.DataFrame,
                           rooms: Sequence[Room], shares: Dict[str, Dict[str, float]],
                           model: sn.SeasonUsageModel, slate: int, n_sims: int,
                           baseline: np.ndarray,
                           transfer: bool = True,
                           centre: bool = True,
                           role_order: Optional[Sequence[np.ndarray]] = None
                           ) -> "Modulation":
    """Each player's season opportunity, as a multiple of what the model projected.

    The quantity :mod:`Scripts.outcomes.distribution` consumes as ``mu_scale``. One number
    per player-simulation, because the transfer moves *opportunity* and the per-opportunity
    rates are close to sampling noise once opportunity is known.

    Week by week: a room's rank-1 player, when absent, vacates his own per-game
    opportunity. ``rank_2`` of it goes to the best **available** player below him -- which
    is how the cascade works, since an absent understudy simply is not the best available
    one. ``rank_rest`` is split among the others who are available, in proportion to their
    own projected opportunity: a division the fitted rank ordering cannot supply and the
    projection can. The man who took the ``rank_2`` share is excluded from that split, or
    one vacancy would pay him twice; if he is the only one there, it falls back to him,
    because a team with one healthy back gives him the carries. Whatever nobody available
    takes leaves the room, which is the measured 7% (RB) and 32% (TE) group shrinkage.

    Args:
        rng: Explicit generator.
        frame: The player frame, carrying ``expected_games`` and ``position``.
        rooms: From :func:`room_order`.
        shares: From :func:`Scripts.outcomes.vacancy.applied_rule`.
        model: Supplies the per-position Beta-Binomial concentration.
        slate: Games the season offers.
        n_sims: Simulations.
        baseline: Per player, projected per-game opportunity. Only ratios of it are used,
            so its units do not matter.
        transfer: False runs the identical availability draw with **no** redistribution --
            the control that isolates whether the room, rather than the decomposition,
            is what earns the complexity.
        centre: Passed to :class:`Modulation`. Leave True unless deliberately measuring
            the double-count it removes.
        role_order: From :func:`draw_role_order`, one array per room. None treats the
            depth chart as certain, which is what plan 33 measures as wrong about a third
            of the time.

    Returns:
        Modulation: the availability and transfer factors kept **apart**, because they
        must be applied differently. See :class:`Modulation`.
    """
    expected = frame["expected_games"].cast(pl.Float64).to_numpy()
    kappa = model.dispersion_for(frame["position"].to_list())
    known = np.isfinite(expected) & (expected > 0)
    safe = np.where(known, expected, float(slate))

    available = draw_weeks(rng, safe, kappa, slate, n_sims)
    # Weeks played over weeks the model expected: 1.0 for a player who gets exactly the
    # season he was projected for. Everything below adds inherited work on top.
    weeks = available.sum(axis=2).astype(float)
    multiplier = np.where(known[None, :], weeks / safe[None, :], 1.0)

    gains = np.zeros_like(multiplier)
    if not transfer:
        return Modulation(availability=multiplier, gain=gains,
                          elasticity=model.games_elasticity, centre=centre)

    own = np.where(np.isfinite(baseline) & (baseline > 0), baseline, 0.0)
    orders = (role_order if role_order is not None
              else [None] * len(rooms))

    for room, order in zip(rooms, orders):
        rule = shares.get(room.position)
        if rule is None or len(room.players) < 2:
            continue
        members = np.array(room.players)
        size = members.size

        # **Everything below is in role order, not listed order**, because with a role
        # draw who leads varies by simulation. With no draw the order is the identity and
        # this reduces exactly to indexing `members[0]` and `members[1:]` -- a test pins
        # that equivalence, so the generalisation cannot silently change the base case.
        if order is None:
            order = np.tile(np.arange(size), (n_sims, 1))

        present = np.take_along_axis(available[:, members, :], order[:, :, None], axis=1)
        own_ord = np.take_along_axis(np.tile(own[members], (n_sims, 1)), order, axis=1)
        safe_ord = np.take_along_axis(np.tile(safe[members], (n_sims, 1)), order, axis=1)

        vacated = own_ord[:, 0][:, None] * (~present[:, 0, :])    # (n_sims, slate)
        if not vacated.any():
            continue

        present_rest = present[:, 1:, :]                          # (n_sims, |rest|, slate)
        own_rest = own_ord[:, 1:]                                 # (n_sims, |rest|)
        if not present_rest.shape[1]:
            continue

        # The understudy's share goes to the best *available* man below the lead, which
        # is how the cascade falls out: if the rank-2 back is himself out, the first
        # available player behind him takes it. A cumulative count picks exactly one.
        understudy = (present_rest.cumsum(axis=1) == 1) & present_rest
        second = rule["rank_2"] * vacated[:, None, :] * understudy

        # The pooled ranks 3+ share goes to everyone else who is available, split by
        # projected role -- and explicitly **not** to the man who just took the rank-2
        # share, or he would be paid twice out of one vacancy.
        eligible = present_rest & ~understudy
        weights = own_rest[:, :, None] * eligible
        total = weights.sum(axis=1, keepdims=True)

        # Nobody behind the understudy is available: the work he cannot cover falls back
        # to him rather than evaporating, because a team with one healthy back gives him
        # the carries. Nobody at all available and it leaves the room, which is the group
        # shrinkage the closure table measures anyway.
        fallback = own_rest[:, :, None] * understudy
        empty = total <= 0
        weights = np.where(empty, fallback, weights)
        total = np.where(empty, fallback.sum(axis=1, keepdims=True), total)

        share = np.divide(weights, total, out=np.zeros_like(weights), where=total > 0)
        inherited = second + share * (rule["rank_rest"] * vacated)[:, None, :]

        # Back into per-projected-season units: sum the inherited per-game opportunity
        # over weeks, divide by what a full projected season of his own would have been.
        denominator = own_rest * safe_ord[:, 1:]
        gained = np.divide(inherited.sum(axis=2), denominator,
                           out=np.zeros_like(denominator), where=denominator > 0)

        # Scatter back out of role order onto the room's own positions, then onto the
        # frame. `members` is unique, so the fancy-index assignment cannot collide.
        buffer = np.zeros((n_sims, size))
        np.put_along_axis(buffer, order[:, 1:], gained, axis=1)
        gains[:, members] += buffer

    return Modulation(availability=multiplier, gain=gains,
                      elasticity=model.games_elasticity, centre=centre)


def baseline_opportunity(frame: pl.DataFrame) -> np.ndarray:
    """Projected per-game opportunity, for splitting a vacancy in proportion to role.

    The model's own volume heads, which is the only per-game opportunity a projection has:
    ``pred_carries_pg`` plus ``pred_targets_pg`` for a backfield, targets alone elsewhere,
    mirroring :data:`Scripts.outcomes.evidence.OPPORTUNITY`.

    Args:
        frame: Prediction frame carrying the ``pred_*_pg`` columns.

    Returns:
        np.ndarray: Per player, non-negative, zero where nothing is projected.
    """
    total = np.zeros(frame.height)
    for column in ("pred_carries_pg", "pred_targets_pg"):
        if column in frame.columns:
            values = frame[column].cast(pl.Float64).to_numpy()
            total = total + np.nan_to_num(values, nan=0.0)
    return np.clip(total, 0.0, None)
