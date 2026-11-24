"""Playoff odds: the rest of the regular season and the bracket, simulated.

    python -m Scripts.refresh --all --what playoff_odds

Plays the remaining regular-season weeks and the bracket many times, seeds every
simulated season with :func:`Scripts.playoffs.order` (the ESPN-matched rules), and
counts: each seed, the playoffs, a bye, the division, the final, the title, and each
team's likeliest first playoff opponent. Written per league as ``playoff_odds.parquet``.

**Who starts is decided by ROS rank; what they score is decided by value.** Every
simulated manager fields the best legal lineup by FantasyPros' rest-of-season ranking
-- managers act on rankings. FantasyPros ranks each position on its own page, so the
rank alone cannot settle a flex spot (is RB20 better than WR25?). The ranking is
therefore turned into a **rank-implied value**: within each position, the league's
rostered players sorted by ROS rank take that position's values sorted high to low,
so the k-th ranked back is worth what the k-th best back projects. The order within a
position is FantasyPros'; the scale across positions is this league's own scoring --
the ordering transfers and the points do not, as :mod:`Scripts.ros` measured. A
player FantasyPros does not rank (IDP, and the fringe it skips) is ordered by his own
value. The optimiser is :func:`Scripts.lineup_solver.optimal_lineup`, the one the
Roster tab uses.

**A player's value is this week's blended projection, and nothing else.** Calibrated
on 2026 weeks 2-11 over all nine leagues (27,671 player-games, own scoring each):
predicting a game 0-3 weeks ahead, the best mix of the projection and the player's
season per-game average put **all** of its weight on the projection at every horizon
-- MAE 5.84 against 6.57 for the average one week out, 6.10 against 6.70 three weeks
out. RB and D/ST took 0.80-0.85 on the projection for under 0.03 points of MAE. The
average is therefore only a fallback: a player with no projection this week (bye, or
ruled out) carries his most recent projection, then his season average.

**Availability:** a player on bye that week is out. This week, ESPN's own status
decides -- an ESPN projection of zero is the out signal, and ``player_active_status``
carries it. A player out this week is back the week after, unless he is on injured
reserve and inactive, in which case he is out for the rest of the season.

**Spread:** each starter's weekly SD comes from :mod:`Scripts.outcomes.weekly`, the
dispersion model the Matchup tab's win probability is calibrated on; a lineup's SD is
the independent sum, as that model measured. A player whose game this week is over
counts his actual with no spread.

**Not modelled:** waivers and trades (rosters are frozen), commissioner adjustments in
future weeks, and correlation between players.

See ``docs/plans/57-playoff-bracket.md``.
"""

from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from Scripts import playoffs as po
from Scripts.lineup_solver import optimal_lineup, slot_counts
from Scripts.live import ACT_POINTS, LIVE_POINTS, LIVE_REMAINING, LOCKED_COLUMN, STATE_COLUMN

#: Simulated seasons per league. 10,000 puts the standard error of a 50% figure at
#: 0.5 points, which is finer than the table displays.
N_SIMS = 10_000

#: Fixed so two builds of the same inputs publish the same odds.
DEFAULT_SEED = 57

BLEND_POINTS = "TRUE_Points"
FREE_AGENT = "Free Agent"

@dataclass(frozen=True)
class Strength:
    """One team's score distribution for one week."""

    mu: float
    sd: float
    starters: int


# --- inputs -------------------------------------------------------------------

def player_values(lineups: pl.DataFrame, week: int) -> pl.DataFrame:
    """Every rostered player this week, with the value he carries into later weeks.

    Args:
        lineups: The league's ``lineups`` artifact, all weeks.
        week: The current week.

    Returns:
        pl.DataFrame: This week's rostered rows plus ``value`` (points per game for
        weeks after this one) and ``value_source`` (``projection``,
        ``last_projection``, ``season_average`` or ``none``).
    """
    now = lineups.filter((pl.col("week") == week) & (pl.col("team_owner") != FREE_AGENT))
    past = lineups.filter(pl.col("week") < week)

    played = past.filter((pl.col("player_active_status") == "active")
                         & (pl.col(STATE_COLUMN) == "post"))
    avg = played.group_by("player_id").agg(season_average=pl.col(ACT_POINTS).mean())
    last = (past.filter((pl.col("player_active_status") == "active")
                        & (pl.col(BLEND_POINTS) > 0))
            .sort("week").group_by("player_id")
            .agg(last_projection=pl.col(BLEND_POINTS).last()))

    proj = pl.col(BLEND_POINTS)
    usable = (pl.col("player_active_status") == "active") & (proj > 0)
    return (
        now.join(avg, on="player_id", how="left").join(last, on="player_id", how="left")
        .with_columns(
            value=pl.when(usable).then(proj)
            .otherwise(pl.coalesce("last_projection", "season_average", pl.lit(0.0))),
            value_source=pl.when(usable).then(pl.lit("projection"))
            .when(pl.col("last_projection").is_not_null()).then(pl.lit("last_projection"))
            .when(pl.col("season_average").is_not_null()).then(pl.lit("season_average"))
            .otherwise(pl.lit("none")))
    )


def attach_order(values: pl.DataFrame, ros_frame) -> pl.DataFrame:
    """Add ``_order``, the weight a simulated manager starts players by.

    The rank-implied value for a player FantasyPros ranks (see the module docstring),
    his own ``value`` for one it does not.

    Args:
        values: From :func:`player_values`.
        ros_frame: :func:`Scripts.ros.load_ros`, or None to order by value alone.

    Returns:
        pl.DataFrame: ``values`` plus ``ros_rank_ecr`` (FantasyPros' rank within the
        position) and ``_order``.
    """
    if ros_frame is None:
        return values.with_columns(ros_rank_ecr=pl.lit(None, dtype=pl.Float64),
                                   _order=pl.col("value"))
    from Scripts import ros  # deferred: pulls pandas and the name normaliser

    attached = ros.attach_ros(values.select("player_id", "player_name", "player_position",
                                            "pro_team").to_pandas(), ros_frame)
    ranks = (pl.from_pandas(attached[["player_id", "ros_rank_ecr"]])
             .unique("player_id").with_columns(pl.col("ros_rank_ecr").cast(pl.Float64)))
    out = values.join(ranks, on="player_id", how="left")
    ranked = out.filter(pl.col("ros_rank_ecr").is_not_null())
    implied = (
        ranked.sort("ros_rank_ecr", "player_id")
        .with_columns(_k=pl.int_range(pl.len()).over("player_position"))
        .join(ranked.sort("value", descending=True)
              .with_columns(_k=pl.int_range(pl.len()).over("player_position"))
              .select("player_position", "_k", implied=pl.col("value")),
              on=["player_position", "_k"])
        .select("player_id", "implied"))
    return (out.join(implied, on="player_id", how="left")
            .with_columns(_order=pl.coalesce("implied", "value")).drop("implied"))


def available(row: dict, week: int, current: int, byes: Dict[str, int]) -> bool:
    """Whether a player can start in ``week``. See the module docstring."""
    if byes.get(row.get("pro_team") or "") == week:
        return False
    active_now = (row.get("player_active_status") or "active") == "active"
    if week == current:
        return active_now
    if not active_now and (row.get("injury_status") or "") == "INJURY_RESERVE":
        return False
    return True


def team_strengths(values: pl.DataFrame, slots: Dict[str, int], weeks: Sequence[int],
                   current: int, byes: Dict[str, int], model: dict) -> Dict[Tuple[str, int], Strength]:
    """Each team's score distribution for each week in ``weeks``.

    Args:
        values: From :func:`attach_order`.
        slots: Starting slots, from :func:`Scripts.lineup_solver.slot_counts`.
        weeks: Weeks to cover -- the rest of the regular season and the playoffs.
        current: The current week; only it honours kickoff locks and live points.
        byes: ``{pro_team: bye week}``.
        model: :func:`Scripts.outcomes.weekly.load`.

    Returns:
        dict: ``{(team_owner, week): Strength}``.
    """
    from Scripts.outcomes import weekly

    out: Dict[Tuple[str, int], Strength] = {}
    for (owner,), team in values.partition_by("team_owner", as_dict=True).items():
        rows = team.to_dicts()
        for week in weeks:
            this_week = week == current
            prepared = []
            for r in rows:
                mu = r["value"]
                sd_mu = mu
                if this_week:
                    live = r.get(LIVE_POINTS)
                    mu = float(live) if live is not None else float(r.get(BLEND_POINTS) or 0.0)
                    if r.get(STATE_COLUMN) == "post":
                        sd_mu = 0.0
                    elif r.get(LIVE_REMAINING) is not None and r.get(LOCKED_COLUMN):
                        sd_mu = float(r[LIVE_REMAINING])
                    else:
                        sd_mu = mu
                prepared.append({
                    **r, "_mu": mu, "_sd_mu": sd_mu,
                    "player_active_status":
                        "active" if available(r, week, current, byes) else "inactive",
                    LOCKED_COLUMN: bool(r.get(LOCKED_COLUMN)) if this_week else False,
                })
            starters, _ = optimal_lineup(prepared, slots, "_order", respect_locks=this_week)
            mu = sum(float(s["_mu"] or 0.0) for s in starters)
            sd = weekly.team_sd(model, [(s.get("player_position"), s["_sd_mu"])
                                        for s in starters])
            out[(owner, week)] = Strength(mu=mu, sd=sd, starters=len(starters))
    return out


# --- simulation -----------------------------------------------------------------

def simulate(table: po.Table, fixtures: Sequence[Tuple[int, str, str]],
             strengths: Dict[Tuple[str, int], Strength], settings: Dict[str, Any],
             n: int = N_SIMS, seed: int = DEFAULT_SEED) -> pl.DataFrame:
    """Play out the remaining regular season and the bracket ``n`` times.

    Args:
        table: Standings over the completed weeks.
        fixtures: ``(week, team, team)`` for every regular-season game still to play.
        strengths: From :func:`team_strengths`, covering every fixture week and every
            playoff week.
        settings: ``meta["playoff_settings"]``.
        n: Simulations.
        seed: RNG seed.

    Returns:
        pl.DataFrame: One row per team -- ``team_owner``, ``p_seed_<k>`` for each
        playoff seed, ``p_playoffs``, ``p_bye``, ``p_division`` (null in a league
        without divisions), ``p_final``, ``p_champion``, ``exp_wins``,
        ``exp_losses``, ``exp_ties``, ``likeliest_opponent``, ``likeliest_opponent_p``
        (given the team makes the playoffs), ``first_round_bye_p``.
    """
    rng = np.random.default_rng(seed)
    rule = settings.get("seeding_rule") or "TOTAL_POINTS_SCORED"
    k = int(settings.get("playoff_team_count") or 0)
    rounds = po.bracket(k)
    round_weeks = po.round_weeks(settings, len(rounds))
    n_byes = po.byes(k)
    multi_div = len(set(table.divisions.values())) > 1

    weeks = sorted({w for w, _, _ in fixtures} | {w for r in round_weeks for w in r})
    draws: Dict[Tuple[str, int], np.ndarray] = {}
    for team in table.teams:
        for w in weeks:
            s = strengths.get((team, w))
            draws[(team, w)] = (rng.normal(s.mu, s.sd, n) if s is not None and s.sd > 0
                                else np.full(n, s.mu if s is not None else 0.0))

    seeds = {t: Counter() for t in table.teams}
    divs, finals, titles = Counter(), Counter(), Counter()
    opponents = {t: Counter() for t in table.teams}
    wins, losses, ties = Counter(), Counter(), Counter()

    for i in range(n):
        sim = table.copy()
        for w, a, b in fixtures:
            sim.add(a, b, draws[(a, w)][i], draws[(b, w)][i])
        seeded, winners = po.order(sim, rule)
        for pos, team in enumerate(seeded, start=1):
            seeds[team][pos] += 1
            wins[team] += sim.w[team]
            losses[team] += sim.l[team]
            ties[team] += sim.t[team]
        if multi_div:
            divs.update(winners)
        if not rounds:
            continue

        by_seed = {s: seeded[s - 1] for s in range(1, k + 1)}
        previous: List[str] = []
        for r, games in enumerate(rounds):
            winners_now: List[str] = []
            for g in games:
                hi = by_seed[g.high.seed] if g.high.seed is not None else previous[g.high.winner_of]
                lo = by_seed[g.low.seed] if g.low.seed is not None else previous[g.low.winner_of]
                score_hi = sum(draws[(hi, w)][i] for w in round_weeks[r]) if round_weeks[r] else 0.0
                score_lo = sum(draws[(lo, w)][i] for w in round_weeks[r]) if round_weeks[r] else 0.0
                # A tie goes to the better seed, as ESPN's bracket does.
                winners_now.append(hi if score_hi >= score_lo else lo)
                if r == len(rounds) - 1:
                    finals.update([hi, lo])
            previous = winners_now
        titles[previous[0]] += 1
        _first_opponents(rounds, by_seed, opponents, i, draws, round_weeks)

    return _summarise(table, seeds, divs, finals, titles, opponents, wins, losses, ties,
                      n, k, n_byes, multi_div)


def _first_opponents(rounds, by_seed, opponents, i, draws, round_weeks) -> None:
    """Record each playoff team's first opponent in simulation ``i``.

    A team in round 1 meets its round-1 opponent. A team on a bye meets the winner of
    the round-1 game feeding its round-2 slot, which this resolves with the same draws.
    """
    r1_winners = []
    for g in rounds[0]:
        hi, lo = by_seed[g.high.seed], by_seed[g.low.seed]
        opponents[hi][lo] += 1
        opponents[lo][hi] += 1
        s_hi = sum(draws[(hi, w)][i] for w in round_weeks[0]) if round_weeks[0] else 0.0
        s_lo = sum(draws[(lo, w)][i] for w in round_weeks[0]) if round_weeks[0] else 0.0
        r1_winners.append(hi if s_hi >= s_lo else lo)
    if len(rounds) < 2:
        return
    for g in rounds[1]:
        for side, other in ((g.high, g.low), (g.low, g.high)):
            if side.seed is None:
                continue
            bye_team = by_seed[side.seed]
            opp = (by_seed[other.seed] if other.seed is not None
                   else r1_winners[other.winner_of])
            opponents[bye_team][opp] += 1


def _summarise(table, seeds, divs, finals, titles, opponents, wins, losses, ties,
               n, k, n_byes, multi_div) -> pl.DataFrame:
    rows = []
    for t in table.teams:
        made = sum(seeds[t][s] for s in range(1, k + 1))
        top = opponents[t].most_common(1)
        row = {
            "team_owner": t,
            "division": table.divisions[t],
            **{f"p_seed_{s}": seeds[t][s] / n for s in range(1, k + 1)},
            "p_playoffs": made / n,
            "p_bye": sum(seeds[t][s] for s in range(1, n_byes + 1)) / n,
            "p_division": divs[t] / n if multi_div else None,
            "p_final": finals[t] / n,
            "p_champion": titles[t] / n,
            "exp_wins": wins[t] / n,
            "exp_losses": losses[t] / n,
            "exp_ties": ties[t] / n,
            "likeliest_opponent": top[0][0] if top else None,
            "likeliest_opponent_p": (top[0][1] / made) if top and made else None,
        }
        rows.append(row)
    return (pl.DataFrame(rows, schema_overrides={"p_division": pl.Float64,
                                                 "likeliest_opponent": pl.String,
                                                 "likeliest_opponent_p": pl.Float64})
            .sort(["p_playoffs", "exp_wins"], descending=True))


# --- from the store -----------------------------------------------------------------

def build(lineups: pl.DataFrame, team_stats: pl.DataFrame, meta: dict, season: int,
          ros_frame=None, byes: Optional[Dict[str, int]] = None, model: Optional[dict] = None,
          n: int = N_SIMS, seed: int = DEFAULT_SEED) -> pl.DataFrame:
    """The ``playoff_odds`` artifact for one league.

    Args:
        lineups: The ``lineups`` artifact.
        team_stats: The stitched ``team_stats`` artifact.
        meta: ``meta.json``; needs ``playoff_settings``, ``fixtures`` and
            ``current_matchup_period``.
        season: Season year.
        ros_frame: :func:`Scripts.ros.load_ros`; None orders every player by value.
        byes: :func:`Scripts.draft.board.bye_weeks`; None treats nobody as on bye.
        model: :func:`Scripts.outcomes.weekly.load`; loaded when None.

    Returns:
        pl.DataFrame: :func:`simulate`'s frame plus ``through_week``, ``n_sims`` and
        ``proj_per_week`` (mean projected starting lineup over the remaining
        regular-season weeks).

    Raises:
        ValueError: When ``meta`` has no playoff settings or fixtures -- the nightly
            lineups build writes both.
    """
    settings = meta.get("playoff_settings")
    fixtures_all = meta.get("fixtures")
    if not settings or not fixtures_all:
        raise ValueError("meta.json has no playoff_settings/fixtures; run --what lineups first.")
    if model is None:
        from Scripts.outcomes import weekly
        model = weekly.load()

    regular = int(settings["regular_season_periods"])
    current = int(meta.get("current_matchup_period") or meta.get("current_week") or 1)
    through = max(0, min(current - 1, regular))

    games = po.games_from_team_stats(team_stats, season, through_week=through)
    divisions = po.divisions_from_team_stats(team_stats, season)
    table = po.table_from_games(games, divisions)
    fixtures = [(int(w), a, b) for w, a, b in fixtures_all
                if through < int(w) <= regular and a in divisions and b in divisions]

    playoff_weeks = [w for _, ws in settings.get("playoff_periods") or [] for w in ws]
    weeks = sorted({w for w, _, _ in fixtures} | set(playoff_weeks))
    # Free agents are excluded: their rows carry their position as a slot, which
    # slot_counts would read as a team starting twenty quarterbacks.
    week_frame = lineups.filter((pl.col("week") == current)
                                & (pl.col("team_owner") != FREE_AGENT))
    values = attach_order(player_values(lineups, current), ros_frame)
    slots = slot_counts(week_frame, meta)
    strengths = team_strengths(values, slots, weeks, current, byes or {}, model)

    odds = simulate(table, fixtures, strengths, settings, n=n, seed=seed)
    reg_weeks = sorted({w for w, _, _ in fixtures})
    per_week = {t: (np.mean([strengths[(t, w)].mu for w in reg_weeks]) if reg_weeks else None)
                for t in table.teams}
    return odds.with_columns(
        proj_per_week=pl.col("team_owner").replace_strict(per_week, default=None,
                                                          return_dtype=pl.Float64),
        through_week=pl.lit(through), n_sims=pl.lit(n))
