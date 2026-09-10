"""Who plays whom, and what that matchup is worth.

Thin by design. The measurement lives in :mod:`Scripts.outcomes.weekly`, which fits
the per-position weekly dispersion and back-tests the probability against
pre-committed gates; this module reads that fitted model and applies it to a lineup.
Streamlit-free, so the arithmetic is testable without a browser.

**Why the projected margin is not enough.** "You are projected to win by 6" is not a
decision, because a six-point edge on a lineup whose weekly standard deviation is 23
is barely an edge at all. The probability is the number that carries that.

**Closed form, not Monte Carlo.** The margin between two independent normals is
normal, and the normal approximation calibrates to within 0.2pp of nominal at team
level on 2025 -- 1,776 team-weeks, 9,940 matchups. A simulation would add sampling
noise to a number the closed form already gets right, and a seed to remember. See
:mod:`Scripts.outcomes.weekly` for the evidence and for the two caveats that ride
with it: kickers and defences carry no fitted spread, and independence is measured
rather than assumed.

The other half is identity. The fixture list and the projections come from two
different artifacts, written by two different commands, and ESPN does not always
describe a team the same way to both -- see :func:`opponent_map`.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

from typing import Dict, List, NamedTuple, Optional, Sequence

import streamlit as st

from Scripts.live import LIVE_REMAINING
from Scripts.outcomes import weekly as wk


def opponent_map(fixtures: Sequence[dict], rosters: Sequence[tuple]
                 ) -> tuple:
    """Match the fixture list to the rosters, and say who plays whom.

    **Two artifacts describe the same ten teams and do not always agree what they are
    called.** ``team_stats`` reads a team from ESPN's box-score view and
    ``lineups.parquet`` from its roster view; they are built by different commands on
    different days. On Weenieless_Wanderers week 1 the box-score view served no owner
    at all for one team -- ``"Unknown Owner"`` playing as ``"Team 11"`` -- while the
    roster view has the same team as Stephen Touchstone's ``"Sandusky Shower Pals"``.
    Matching on owner alone left that team unresolvable, and since it was *Tommy's
    week 1 opponent* it left the whole tab empty for the league it mattered in.

    Three passes, in decreasing confidence:

    1. **Owner.** Best coverage in practice -- exact for eight of nine leagues.
    2. **Team name.** ``docs/plans/25-results-backfill.md`` states the rule this
       follows: join within a season on ``team_name``, group across seasons on
       ``owner_id``. It catches a team whose owner ESPN dropped and misses one whose
       manager renamed it between the two builds, so it is the complement of pass 1
       rather than a better version of it.
    3. **The last one standing.** When exactly one team is unmatched on each side,
       they are each other -- there is nowhere else for either to go. Reported in the
       notes, because it is an inference and not a lookup.

    Args:
        fixtures: ``team_stats`` rows for one week, carrying ``team_owner``,
            ``team_name``, ``opp_owner`` and ``opp_name``.
        rosters: ``(owner, team_name)`` for every team with a stored roster.

    Returns:
        tuple: ``(pairs, notes)``. ``pairs`` maps a roster owner to their opponent's
        roster owner, in the lineups' own vocabulary so callers never see the
        fixture list's spelling. ``notes`` are sentences about any team matched by
        something other than its owner.
    """
    by_owner = {owner: owner for owner, _ in rosters if owner}
    by_name = {name: owner for owner, name in rosters if name and owner}

    alias: Dict[str, str] = {}
    notes: List[str] = []

    # Passes 1 and 2, over both sides of every fixture so a bye or a missing row
    # cannot hide a team.
    seen = []
    for row in fixtures:
        for owner_key, name_key in (("team_owner", "team_name"),
                                    ("opp_owner", "opp_name")):
            seen.append((row.get(owner_key), row.get(name_key)))

    for owner, name in seen:
        if not owner or owner in alias:
            continue
        if owner in by_owner:
            alias[owner] = by_owner[owner]
        elif name in by_name:
            alias[owner] = by_name[name]
            notes.append(
                f"The fixture list records **{by_name[name]}**'s team under the "
                f"owner \"{owner}\"; matched on the team name instead.")

    # Pass 3.
    unmatched_fixture = [owner for owner, _ in seen
                         if owner and owner not in alias]
    unmatched_roster = [owner for owner, _ in rosters
                        if owner and owner not in set(alias.values())]
    if len(set(unmatched_fixture)) == 1 and len(unmatched_roster) == 1:
        stray = unmatched_fixture[0]
        alias[stray] = unmatched_roster[0]
        notes.append(
            f"One team is described differently by the two artifacts — "
            f"**{unmatched_roster[0]}** on the rosters, \"{stray}\" on the fixture "
            f"list. It is the only one left unmatched on either side, so they are "
            f"the same team. Both spellings are ESPN's: it hands its box-score and "
            f"roster views different answers, and whichever was built more recently "
            f"tends to have the real name.")

    pairs: Dict[str, str] = {}
    for row in fixtures:
        home = alias.get(row.get("team_owner"))
        away = alias.get(row.get("opp_owner"))
        if home and away:
            pairs[home] = away
    return pairs, notes


class Side(NamedTuple):
    """One team's projected week.

    Attributes:
        owner: Whose team.
        projected: Summed projection of the starting lineup.
        sd: Standard deviation of that total.
        starters: How many starters it holds.
        priced: How many of them carry a fitted spread. Below ``starters`` when the
            lineup includes a kicker or a defence, which have none -- and, since live
            scoring, also when a starter's game is over, which is a *statement* that
            his points are settled rather than a gap in the model.
        modelled: Whether a fitted dispersion was available at all.

            This exists because ``sd == 0`` stopped being one thing. It used to mean
            only "there is no fitted model, so no probability can be quoted"; now it
            also means "every game is final, so the outcome is certain" -- opposite
            readings from the same number. :func:`outcome` needs to tell them apart,
            and nothing else on the frame can.
    """
    owner: str
    projected: float
    sd: float
    starters: int
    priced: int
    modelled: bool = True

    def band(self, z: float = wk.Z_P90) -> tuple:
        """An 80% interval on the total.

        Args:
            z: Normal quantile. Defaults to the p10/p90 pair plan 28 publishes.

        Returns:
            tuple: ``(low, high)``, floored at zero -- a lineup cannot score
            negative points in any league here, and a negative floor reads as a
            modelling artefact because it is one.
        """
        return max(0.0, self.projected - z * self.sd), self.projected + z * self.sd


@st.cache_resource(show_spinner=False)
def model() -> Optional[dict]:
    """The fitted weekly dispersion, or None when it has not been fitted.

    ``cache_resource`` rather than ``cache_data``: it is one small immutable dict
    read once per process, not a frame keyed by arguments.

    Returns:
        dict | None: None rather than raising, so the Matchup tab can fall back to
        showing the projected margin and say why.
    """
    try:
        return wk.load()
    except (FileNotFoundError, ValueError):
        return None


def side(owner: str, starters: Sequence[dict], points_column: str = "TRUE_Points",
         fitted: Optional[dict] = None,
         variance_column: str = LIVE_REMAINING) -> Side:
    """Total, spread and counts for one starting lineup.

    **The spread is computed from what is still to come, not from the total.** Once a
    player's game is over his points are a fact with no uncertainty left, and
    ``Var = phi*mu + mu^2/k`` evaluated at his *remaining* projection returns exactly
    zero for him. Evaluated at the total instead it would hand a player who has
    already banked 40 points the spread of a 40-point projection -- so a team whose
    games were all final would still show a win probability around 70% rather than
    the 100% it has earned. That is the difference between a live number and a
    number that merely moves.

    The fit itself is untouched and needs no refit: at ``elapsed = 0`` the remaining
    projection *is* the projection, so before kickoff this reproduces the shipped
    numbers exactly (``docs/plans/42-weekly-matchup-odds.md``'s coverage of 0.802
    still describes it).

    Args:
        owner: Whose team.
        starters: Rows from :func:`lineup.current_lineup` or
            :func:`lineup.optimal_lineup`.
        points_column: Which points column to total.
        fitted: From :func:`model`. Read once by the caller and passed in, so a page
            drawing two sides does not look it up twice.
        variance_column: What the spread is computed from. Falls back to
            ``points_column`` per row when absent, which is what a store written
            before live scoring carries.

    Returns:
        Side: With ``sd`` of 0.0 when there is no fitted model, which
        :func:`outcome` reads as "no probability available".
    """
    projected = sum(float(row.get(points_column) or 0.0) for row in starters)

    if fitted is None:
        return Side(owner, projected, 0.0, len(starters), 0, modelled=False)

    variance, priced = 0.0, 0
    for row in starters:
        remaining = row.get(variance_column)
        if remaining is None:
            remaining = row.get(points_column)
        deviation = wk.player_sd(fitted, row.get("player_position"), remaining)
        if deviation > 0:
            priced += 1
        variance += deviation ** 2
    return Side(owner, projected, variance ** 0.5, len(starters), priced,
                modelled=True)


class Outcome(NamedTuple):
    """What the two sides add up to.

    Attributes:
        win: Probability the first side outscores the second, or None when there is
            no fitted spread to compute it from.
        margin: Projected margin, first side minus second. Always available.
    """
    win: Optional[float]
    margin: float


def outcome(home: Side, away: Side) -> Outcome:
    """The projected margin and, where possible, the probability.

    Args:
        home: The side the probability is quoted for.
        away: The opponent.

    Returns:
        Outcome: ``win`` is None only when there is **no fitted model** -- reported
        rather than papered over with a 50%.

        A zero spread on both sides used to be read as that same case, and since
        live scoring it is usually the opposite one: once every game in a matchup is
        final there is nothing left to be uncertain about, and the honest answer is
        1.0 or 0.0 rather than "unavailable". ``win_probability`` has always returned
        exactly that for a zero spread; this function was discarding it. Hence
        :attr:`Side.modelled`.
    """
    margin = home.projected - away.projected
    if not home.modelled and not away.modelled:
        return Outcome(None, margin)
    return Outcome(wk.win_probability(home.projected, home.sd,
                                      away.projected, away.sd), margin)


def swing(home: Side, away: Side, delta: float) -> Optional[float]:
    """How much the win probability moves if the home lineup gains ``delta`` points.

    What makes the start/sit list worth acting on: two points of projection is worth
    a different amount of win probability in a close matchup than in a blowout, and
    that is exactly the case where you want to know before spending a waiver claim.

    Args:
        home: The side.
        away: The opponent.
        delta: Points to add to ``home``.

    Returns:
        float | None: Change in probability, or None with no fitted spread.
    """
    before = outcome(home, away).win
    if before is None:
        return None
    after = outcome(home._replace(projected=home.projected + delta), away).win
    return after - before


def gate_note(fitted: Optional[dict]) -> Dict[str, str]:
    """What to tell the reader about where the probability comes from.

    Args:
        fitted: From :func:`model`.

    Returns:
        dict: ``kind`` is ``"ok"`` or ``"missing"``; ``text`` is the caption.
    """
    if fitted is None:
        return {
            "kind": "missing",
            "text": "No fitted weekly dispersion, so there is no honest win "
                    "probability to show — only the projected margin. Fit it with "
                    "`python -m Scripts.outcomes.weekly --fit --report`.",
        }
    seasons = ", ".join(str(s) for s in fitted.get("train_seasons", []))
    return {
        "kind": "ok",
        "text": f"Probability from the per-position weekly dispersion fitted on "
                f"{seasons} ({fitted.get('n_rows', 0):,} started player-weeks). "
                f"Back-tested at 0.802 interval coverage against a nominal 0.800, "
                f"with every predicted decile inside 3pp of its realised win rate. "
                f"Kickers and defences carry no fitted spread, so they add their "
                f"means and no variance.",
    }
