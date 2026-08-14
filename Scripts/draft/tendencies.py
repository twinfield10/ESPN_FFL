"""Owner tendencies: what each manager reliably does that the room does not.

Everything here is measured **against the room the manager was actually sitting
in**, leaving that manager out of the baseline. That choice does the work of a
dozen special cases: it needs no external ADP, it is immune to league size,
scoring and format, and it survives the fact that the nine configured leagues run
6 to 16 teams and 14 to 17 rounds. "Took a quarterback in round 4" means nothing
on its own. "Took a quarterback in round 4 when the other five managers averaged
round 4.9" is a fact about the manager.

The room baseline is leave-one-out for a concrete reason: in a six-team league a
manager is a sixth of the room, so including them shrinks their own deviation by
17% -- exactly the leagues with the longest history are the ones where it matters
most.

**What is not measured here: whether any of it worked.** Points over expectation
per manager needs a season of results scored in that league's rules for every
past season, which the store does not hold. These are tendencies, not verdicts --
a manager who reaches for a kicker every year is predictable, and predictable is
what a draft board can use.

Two families, chosen by draft format, because a nomination order is not a
valuation:

* **Snake** -- when each position first comes off a manager's board, and what
  their first three rounds are made of.
* **Auction** -- what share of budget goes where, and how much of it lands on
  three players.

And four that apply to both: which NFL team they over-draft, which player they
keep coming back to, how they treat rookies, and how often they were not in the
room at all.

See ``docs/plans/23-owner-tendencies.md``.
"""

from typing import Dict, List, Optional, Sequence

import polars as pl

#: Drafts a manager needs before anything is called a tendency. One draft is an
#: anecdote; the description says so rather than inventing a trait.
MIN_SEASONS = 2

#: Positions whose timing is worth reporting. Ordered by how early they normally
#: go, which is the order the detail table reads in.
TIMED_POSITIONS = ("QB", "RB", "WR", "TE", "K", "D/ST")

#: Rounds earlier or later than the room before positional timing is a trait.
#: A full round is the smallest gap a manager can actually act on -- half a round
#: is one pick either side of the snake turn.
TIMING_MIN_DELTA = 1.0

#: Share of a manager's drafts that must fall on the same side of the room for
#: timing to count. Two thirds, so a manager with three drafts needs two and one
#: with nine needs six -- it rejects a single wild year dragging a mean around.
TIMING_MIN_CONSISTENCY = 2 / 3

#: Picks spent on one NFL team, and standard deviations above what the room's own
#: drafted pool predicts, before it is called a lean.
TEAM_MIN_PICKS = 5
TEAM_MIN_Z = 2.0

#: Seasons a manager must have drafted a player, and the share of the drafts in
#: which anyone took that player, before it is called loyalty.
LOYALTY_MIN_TIMES = 3
LOYALTY_MIN_RATE = 0.5

#: Percentage points of rookie share away from the room.
ROOKIE_MIN_DELTA = 0.06

#: Share of picks made by ESPN rather than by the manager before it is worth
#: saying out loud. One autodrafted pick is a bathroom break; a fifth of a draft
#: is a manager who was not there.
AUTO_MIN_RATE = 0.20

#: Percentage points of first-three-round positional share away from the room.
BUILD_MIN_DELTA = 0.18

#: Percentage points of budget away from the room, for auction spend shape.
SPEND_MIN_DELTA = 0.08

#: How many clauses a description carries. Three is what fits on a card and about
#: what a manager can hold in mind while the clock runs.
MAX_TRAITS = 3

#: At most this many timing clauses, so a manager whose whole profile is "early on
#: everything" still shows their team lean or their guy.
MAX_TIMING_TRAITS = 2

SNAKE = "SNAKE"
AUCTION = "AUCTION"


# --- shared helpers ------------------------------------------------------

def _owner_seasons(history: pl.DataFrame) -> pl.DataFrame:
    """One row per manager: drafts, span, picks.

    Args:
        history: A :func:`Scripts.draft.history.fetch_draft_history` frame.

    Returns:
        pl.DataFrame: ``owner``, ``owner_id``, ``seasons``, ``first_season``,
        ``last_season``, ``picks``.
    """
    return (
        history.group_by("owner")
        .agg(pl.col("owner_id").last(),
             pl.col("season").n_unique().alias("seasons"),
             pl.col("season").min().alias("first_season"),
             pl.col("season").max().alias("last_season"),
             pl.len().alias("picks"))
        .sort("owner")
    )


def _leave_one_out(frame: pl.DataFrame, value: str, over: Sequence[str],
                   alias: str = "room") -> pl.DataFrame:
    """Add the mean of ``value`` over ``over``, excluding each row itself.

    Args:
        frame: Any frame with one row per manager per group.
        value: Column to average.
        over: Grouping columns defining "the room". Always includes ``season``:
            every baseline in this module is season-matched, so there is no
            caller that wants a room pooled across a decade.
        alias: Name for the new column.

    Returns:
        pl.DataFrame: ``frame`` with ``alias`` added. Null where a group has a
        single row, since a room of one has no baseline.
    """
    total = pl.col(value).sum().over(over)
    count = pl.len().over(over)
    return frame.with_columns(
        pl.when(count > 1)
        .then((total - pl.col(value)) / (count - 1))
        .otherwise(None)
        .alias(alias)
    )


# --- snake: positional timing -------------------------------------------

def positional_timing(history: pl.DataFrame) -> pl.DataFrame:
    """When each position first comes off each manager's board, against the room.

    A manager who never drafts the position in a season is censored to one round
    past the end of that draft rather than dropped -- never taking a kicker *is*
    the tendency, and dropping it would silently rank that manager as average.

    Args:
        history: A pick-history frame. Auction seasons are ignored; nomination
            order is not a valuation.

    Returns:
        pl.DataFrame: ``owner``, ``position``, ``own_round``, ``room_round``,
        ``delta`` (negative is earlier than the room), ``seasons``,
        ``consistency``.
    """
    snake = history.filter(pl.col("draft_type") == SNAKE)
    if snake.is_empty():
        return pl.DataFrame(schema={
            "owner": pl.Utf8, "position": pl.Utf8, "own_round": pl.Float64,
            "room_round": pl.Float64, "delta": pl.Float64, "seasons": pl.UInt32,
            "consistency": pl.Float64})

    # Every manager-season crossed with every timed position, so a position a
    # manager never took still gets a row.
    entrants = snake.select("season", "owner", "rounds").unique()
    grid = entrants.join(pl.DataFrame({"position": list(TIMED_POSITIONS)}),
                         how="cross")
    firsts = (snake.filter(pl.col("position").is_in(list(TIMED_POSITIONS)))
              .group_by("season", "owner", "position")
              .agg(pl.col("round").min().alias("first_round")))

    per_season = (
        grid.join(firsts, on=["season", "owner", "position"], how="left")
        .with_columns(pl.col("first_round").fill_null(pl.col("rounds") + 1)
                      .cast(pl.Float64))
    )
    per_season = _leave_one_out(per_season, "first_round",
                                ["season", "position"], "room_round")
    per_season = per_season.drop_nulls("room_round").with_columns(
        (pl.col("first_round") - pl.col("room_round")).alias("dev"))

    return (
        per_season.group_by("owner", "position")
        .agg(pl.col("first_round").mean().alias("own_round"),
             pl.col("room_round").mean().alias("room_round"),
             pl.col("dev").mean().alias("delta"),
             pl.col("season").n_unique().alias("seasons"),
             # Share of drafts falling on the same side as the average, which is
             # what separates a habit from one loud year.
             pl.col("dev").alias("devs"))
        .with_columns(
            pl.struct("delta", "devs").map_elements(
                lambda row: _same_side(row["delta"], row["devs"]),
                return_dtype=pl.Float64).alias("consistency"))
        .drop("devs")
        .sort("owner", "delta")
    )


def _same_side(mean_dev: Optional[float], devs: Optional[Sequence[float]]) -> float:
    """Share of a manager's drafts on the same side of the room as their average.

    Args:
        mean_dev: The manager's mean deviation.
        devs: Their per-season deviations.

    Returns:
        float: 0-1. Zero when the average is exactly the room's.
    """
    if not devs or mean_dev is None or mean_dev == 0:
        return 0.0
    sign = 1 if mean_dev > 0 else -1
    hits = sum(1 for dev in devs if dev is not None and dev * sign > 0)
    return hits / len(devs)


def _share_against_room(units: pl.DataFrame, weight: str) -> pl.DataFrame:
    """Per-position share of a manager's ``weight``, against the same season's room.

    The comparison is built season by season and only then averaged over the
    manager's own seasons. Pooling first would compare a manager who left in 2018
    against a room baseline set largely by 2024 and 2025 -- and positional norms
    move: the first-three-rounds running-back share in these leagues is not what
    it was, so a departed manager would be scored against a market they never saw.

    Args:
        units: One row per ``season``, ``owner``, ``position`` with a ``weight``
            column -- picks, or dollars.
        weight: The column to take shares of.

    Returns:
        pl.DataFrame: ``owner``, ``position``, ``own_share``, ``room_share``,
        ``delta``, ``picks``, averaged over the manager's seasons.
    """
    totals = units.group_by("season", "owner").agg(pl.col(weight).sum().alias("n"))
    # Every manager needs a row per position, per season, or a manager who took no
    # running back is simply absent from the running-back comparison rather than
    # scored at zero -- which is what "skips RB early" means.
    grid = (totals.join(units.select("position").unique(), how="cross")
            .join(units, on=["season", "owner", "position"], how="left")
            .with_columns(pl.col(weight).fill_null(0.0)))
    grid = grid.filter(pl.col("n") > 0).with_columns(
        (pl.col(weight) / pl.col("n")).alias("own_share"))
    grid = _leave_one_out(grid, "own_share", ["season", "position"], "room_share")

    return (grid.drop_nulls("room_share")
            .with_columns((pl.col("own_share") - pl.col("room_share")).alias("delta"))
            .group_by("owner", "position")
            .agg(pl.col("own_share").mean(), pl.col("room_share").mean(),
                 pl.col("delta").mean(),
                 pl.col(weight).sum().alias("picks"))
            .sort(["owner", "delta"], descending=[False, True]))


def early_build(history: pl.DataFrame, rounds: int = 3) -> pl.DataFrame:
    """What a manager's opening rounds are made of, against the room.

    Args:
        history: A pick-history frame. Snake seasons only.
        rounds: How many opening rounds count as "the build".

    Returns:
        pl.DataFrame: ``owner``, ``position``, ``own_share``, ``room_share``,
        ``delta``, ``picks``.
    """
    early = history.filter((pl.col("draft_type") == SNAKE)
                           & (pl.col("round") <= rounds)
                           & (pl.col("position") != ""))
    if early.is_empty():
        return pl.DataFrame(schema={
            "owner": pl.Utf8, "position": pl.Utf8, "own_share": pl.Float64,
            "room_share": pl.Float64, "delta": pl.Float64, "picks": pl.Float64})

    units = (early.group_by("season", "owner", "position")
             .agg(pl.len().cast(pl.Float64).alias("taken")))
    return _share_against_room(units, "taken")


# --- auction: where the money goes ---------------------------------------

def auction_spend(history: pl.DataFrame) -> pl.DataFrame:
    """Share of budget by position, and how concentrated it is, against the room.

    Keeper contracts are included: they are spent budget, and a manager who keeps
    two backs at $40 has made exactly the allocation decision this measures.

    Args:
        history: A pick-history frame. Auction seasons only.

    Returns:
        pl.DataFrame: ``owner``, ``position``, ``own_share``, ``room_share``,
        ``delta``, plus ``top3_share`` / ``room_top3_share`` repeated per row --
        concentration is a manager-level number and the description reads both.
    """
    auction = history.filter((pl.col("draft_type") == AUCTION)
                             & (pl.col("position") != ""))
    schema = {"owner": pl.Utf8, "position": pl.Utf8, "own_share": pl.Float64,
              "room_share": pl.Float64, "delta": pl.Float64,
              "top3_share": pl.Float64, "room_top3_share": pl.Float64}
    if auction.is_empty() or auction["bid"].sum() == 0:
        return pl.DataFrame(schema=schema)

    per_season = auction.group_by("season", "owner").agg(
        pl.col("bid").sum().alias("spend"),
        pl.col("bid").top_k(3).sum().alias("top3"))
    per_season = per_season.filter(pl.col("spend") > 0).with_columns(
        (pl.col("top3") / pl.col("spend")).alias("top3_share"))
    per_season = _leave_one_out(per_season, "top3_share", ["season"],
                                "room_top3_share")
    concentration = per_season.group_by("owner").agg(
        pl.col("top3_share").mean(), pl.col("room_top3_share").mean())

    units = (auction.group_by("season", "owner", "position")
             .agg(pl.col("bid").sum().alias("spent")))
    return (_share_against_room(units, "spent")
            .join(concentration, on="owner", how="left")
            .select(list(schema))
            .sort(["owner", "delta"], descending=[False, True]))


# --- both formats --------------------------------------------------------

def team_lean(history: pl.DataFrame) -> pl.DataFrame:
    """Which NFL team a manager over-drafts, against what the league drafts.

    The baseline is the league's *own* drafted pool, not the NFL, so a season when
    everyone chased Detroit does not make everyone a Lions homer.

    Args:
        history: A pick-history frame.

    Returns:
        pl.DataFrame: ``owner``, ``pro_team``, ``picks``, ``expected``, ``lift``,
        ``z``, sorted by ``z`` descending within manager.
    """
    named = history.filter(pl.col("pro_team") != "")
    schema = {"owner": pl.Utf8, "pro_team": pl.Utf8, "picks": pl.UInt32,
              "expected": pl.Float64, "lift": pl.Float64, "z": pl.Float64}
    if named.is_empty():
        return pl.DataFrame(schema=schema)

    # Expected is accumulated season by season, so a manager is only ever measured
    # against the pool that existed in the drafts they sat in. Pooling across the
    # league's whole history instead would have charged a manager who left in 2018
    # for avoiding a team that only became draftable later.
    season_pool = (named.group_by("season", "pro_team").len().rename({"len": "team_n"})
                   .join(named.group_by("season").len().rename({"len": "season_n"}),
                         on="season")
                   .with_columns((pl.col("team_n") / pl.col("season_n")).alias("p")))
    owner_season = named.group_by("season", "owner").len().rename({"len": "n"})

    expectation = (owner_season.join(season_pool, on="season")
                   .with_columns((pl.col("p") * pl.col("n")).alias("expected"),
                                 (pl.col("n") * pl.col("p") * (1 - pl.col("p")))
                                 .alias("variance"))
                   .group_by("owner", "pro_team")
                   .agg(pl.col("expected").sum(), pl.col("variance").sum()))

    counts = named.group_by("owner", "pro_team").len().rename({"len": "picks"})
    return (
        expectation.join(counts, on=["owner", "pro_team"], how="left")
        .with_columns(pl.col("picks").fill_null(0))
        .with_columns(
            ((pl.col("picks") - pl.col("expected")) / pl.col("expected")).alias("lift"),
            pl.when(pl.col("variance") > 0)
            .then((pl.col("picks") - pl.col("expected")) / pl.col("variance").sqrt())
            .otherwise(None).alias("z"))
        .select(list(schema))
        .sort(["owner", "z"], descending=[False, True])
    )


def player_loyalty(history: pl.DataFrame) -> pl.DataFrame:
    """The players a manager keeps going back to.

    Rate is over *opportunities* -- the drafts in which the manager took part and
    somebody took that player -- not over the manager's whole career. A player who
    only entered the league in 2023 should not read as disloyalty in 2016.

    Args:
        history: A pick-history frame.

    Returns:
        pl.DataFrame: ``owner``, ``player_name``, ``position``, ``times``,
        ``opportunities``, ``rate``.
    """
    schema = {"owner": pl.Utf8, "player_name": pl.Utf8, "position": pl.Utf8,
              "times": pl.UInt32, "opportunities": pl.UInt32, "rate": pl.Float64}
    if history.is_empty():
        return pl.DataFrame(schema=schema)

    drafted_in = (history.group_by("player_id")
                  .agg(pl.col("season").unique().alias("player_seasons")))
    owner_in = (history.group_by("owner")
                .agg(pl.col("season").unique().alias("owner_seasons")))

    taken = (history.group_by("owner", "player_id")
             .agg(pl.col("player_name").last(), pl.col("position").last(),
                  pl.col("season").n_unique().alias("times"))
             .join(drafted_in, on="player_id")
             .join(owner_in, on="owner"))

    return (
        taken.with_columns(
            pl.col("player_seasons").list.set_intersection(pl.col("owner_seasons"))
            .list.len().cast(pl.UInt32).alias("opportunities"))
        .filter(pl.col("opportunities") > 0)
        .with_columns((pl.col("times") / pl.col("opportunities")).alias("rate"))
        .select(list(schema))
        .sort(["owner", "times", "rate"], descending=[False, True, True])
    )


def habits(history: pl.DataFrame) -> pl.DataFrame:
    """Rookie appetite, autodraft rate and keeper use, against the room.

    Args:
        history: A pick-history frame.

    Returns:
        pl.DataFrame: ``owner``, ``rookie_rate``, ``room_rookie_rate``,
        ``rookie_delta``, ``auto_rate``, ``auto_picks``, ``keeper_rate``.
    """
    schema = {"owner": pl.Utf8, "rookie_rate": pl.Float64,
              "room_rookie_rate": pl.Float64, "rookie_delta": pl.Float64,
              "auto_rate": pl.Float64, "auto_picks": pl.UInt32,
              "keeper_rate": pl.Float64}
    if history.is_empty():
        return pl.DataFrame(schema=schema)

    # Rookie share is over picks the rookie flag actually resolved -- team
    # defences and anyone nflverse does not carry are excluded rather than
    # counted as veterans, which would dilute every manager toward the mean.
    #
    # And it is compared season by season, because rookie appetite is not a
    # constant: these leagues drafted 5 rookies in 2016 and 168 in 2025. Against a
    # pooled baseline every manager who has been here since 2016 would read as
    # rookie-averse and every recent arrival as rookie-hungry, purely from when
    # they showed up.
    resolved = history.filter(pl.col("is_rookie").is_not_null())
    if resolved.is_empty():
        rookie = (history.select("owner").unique().with_columns(
            pl.lit(None, dtype=pl.Float64).alias("rookie_rate"),
            pl.lit(None, dtype=pl.Float64).alias("room_rookie_rate")))
    else:
        per_season = (resolved.group_by("season", "owner")
                      .agg(pl.col("is_rookie").mean().alias("rate")))
        per_season = _leave_one_out(per_season, "rate", ["season"], "room_rate")
        rookie = (per_season.drop_nulls("room_rate").group_by("owner")
                  .agg(pl.col("rate").mean().alias("rookie_rate"),
                       pl.col("room_rate").mean().alias("room_rookie_rate")))

    other = history.group_by("owner").agg(
        pl.col("auto_drafted").mean().alias("auto_rate"),
        pl.col("auto_drafted").sum().cast(pl.UInt32).alias("auto_picks"),
        pl.col("keeper").mean().alias("keeper_rate"))

    return (rookie.join(other, on="owner", how="full", coalesce=True)
            .with_columns((pl.col("rookie_rate") - pl.col("room_rookie_rate"))
                          .alias("rookie_delta"))
            .select(list(schema))
            .sort("owner"))


# --- turning measurements into sentences ---------------------------------

def _round_word(value: float) -> str:
    """Format a round number the way a draft room says it.

    Args:
        value: A mean round, e.g. ``5.33``.

    Returns:
        str: e.g. ``"5.3"``.
    """
    return f"{value:.1f}"


def _pct(value: float) -> str:
    """Format a share as whole percent.

    Args:
        value: A 0-1 share.

    Returns:
        str: e.g. ``"31%"``.
    """
    return f"{round(value * 100):.0f}%"


def sentence_case(text: str) -> str:
    """Capitalise a clause's first letter without touching the rest.

    ``str.capitalize`` lowercases everything after the first character, which
    turned every clause's content into nonsense -- "justin tucker", "qb", "d/st".

    Args:
        text: A clause.

    Returns:
        str: The clause with an initial capital and a full stop.
    """
    if not text:
        return ""
    body = text[0].upper() + text[1:]
    return body if body.endswith(".") else body + "."


def display_name(owner: str) -> str:
    """Capitalise a manager's name without destroying the ones already right.

    ``str.title`` -- which :func:`Scripts.fetch_utils.set_owner_names` uses -- is
    wrong here: it fixes ESPN profiles stored as "hank Winfield" but breaks
    "Gates McGavick" into "Gates Mcgavick". Only all-lowercase words are touched.

    Args:
        owner: The name as ESPN stores it.

    Returns:
        str: e.g. ``"Hank Winfield"``, ``"Gates McGavick"`` unchanged.
    """
    return " ".join(word.capitalize() if word.islower() else word
                    for word in (owner or "").split())


def _timing_traits(rows: pl.DataFrame) -> List[dict]:
    """Clauses for positions taken earlier or later than the room.

    Args:
        rows: One manager's :func:`positional_timing` rows.

    Returns:
        list: Candidate traits, each with ``kind``, ``score``, ``text``.
    """
    traits = []
    for row in rows.iter_rows(named=True):
        delta, position = row["delta"], row["position"]
        if (delta is None or abs(delta) < TIMING_MIN_DELTA
                or row["seasons"] < MIN_SEASONS
                or row["consistency"] < TIMING_MIN_CONSISTENCY):
            continue
        own, room = _round_word(row["own_round"]), _round_word(row["room_round"])
        if delta < 0:
            text = (f"takes {position} early — round {own} against the room's {room}")
        else:
            text = (f"waits on {position} — round {own} against the room's {room}")
        traits.append({"kind": "timing", "score": abs(delta) / TIMING_MIN_DELTA,
                       "text": text})
    return traits


def _build_traits(rows: pl.DataFrame) -> List[dict]:
    """Clauses for an unusual first three rounds.

    Args:
        rows: One manager's :func:`early_build` rows.

    Returns:
        list: Candidate traits.
    """
    traits = []
    for row in rows.iter_rows(named=True):
        delta = row["delta"]
        if delta is None or abs(delta) < BUILD_MIN_DELTA:
            continue
        shape = "leans on" if delta > 0 else "skips"
        traits.append({
            "kind": "build",
            "score": abs(delta) / BUILD_MIN_DELTA,
            "text": (f"{shape} {row['position']} early — {_pct(row['own_share'])} of "
                     f"his first three rounds against the room's "
                     f"{_pct(row['room_share'])}"),
        })
    return traits


def _spend_traits(rows: pl.DataFrame) -> List[dict]:
    """Clauses for auction budget shape and concentration.

    Args:
        rows: One manager's :func:`auction_spend` rows.

    Returns:
        list: Candidate traits.
    """
    if rows.is_empty():
        return []
    traits = []
    first = rows.row(0, named=True)
    top3, room_top3 = first["top3_share"], first["room_top3_share"]
    if top3 is not None and room_top3 is not None:
        gap = top3 - room_top3
        if abs(gap) >= SPEND_MIN_DELTA:
            text = (f"stars and scrubs — {_pct(top3)} of budget on three players "
                    f"against the room's {_pct(room_top3)}") if gap > 0 else (
                    f"spreads the budget — {_pct(top3)} on his top three against "
                    f"the room's {_pct(room_top3)}")
            traits.append({"kind": "spend", "score": abs(gap) / SPEND_MIN_DELTA,
                           "text": text})
    for row in rows.iter_rows(named=True):
        delta = row["delta"]
        if delta is None or abs(delta) < SPEND_MIN_DELTA:
            continue
        verb = "pays up at" if delta > 0 else "will not pay for"
        traits.append({
            "kind": "spend",
            "score": abs(delta) / SPEND_MIN_DELTA,
            "text": (f"{verb} {row['position']} — {_pct(row['own_share'])} of budget "
                     f"against the room's {_pct(row['room_share'])}"),
        })
    return traits


def _team_traits(rows: pl.DataFrame) -> List[dict]:
    """Clause for an NFL-team lean.

    Args:
        rows: One manager's :func:`team_lean` rows, best first.

    Returns:
        list: At most one trait.
    """
    for row in rows.iter_rows(named=True):
        if row["picks"] < TEAM_MIN_PICKS or row["z"] is None or row["z"] < TEAM_MIN_Z:
            continue
        return [{
            "kind": "team",
            "score": row["z"] / TEAM_MIN_Z,
            "text": (f"leans {row['pro_team']} — {row['picks']} {row['pro_team']} "
                     f"picks against {row['expected']:.1f} expected"),
        }]
    return []


def _loyalty_traits(rows: pl.DataFrame) -> List[dict]:
    """Clause for the player a manager keeps drafting.

    Args:
        rows: One manager's :func:`player_loyalty` rows, most-drafted first.

    Returns:
        list: At most one trait.
    """
    for row in rows.iter_rows(named=True):
        if row["times"] < LOYALTY_MIN_TIMES or row["rate"] < LOYALTY_MIN_RATE:
            continue
        return [{
            "kind": "loyalty",
            "score": row["rate"] * row["times"] / (LOYALTY_MIN_RATE * LOYALTY_MIN_TIMES),
            "text": (f"has drafted {row['player_name']} in {row['times']} of the "
                     f"{row['opportunities']} drafts he was available in"),
        }]
    return []


def _habit_traits(row: dict) -> List[dict]:
    """Clauses for rookie appetite and autodrafting.

    Args:
        row: One manager's :func:`habits` row.

    Returns:
        list: Candidate traits.
    """
    traits = []
    delta = row.get("rookie_delta")
    if delta is not None and abs(delta) >= ROOKIE_MIN_DELTA:
        verb = "buys rookies" if delta > 0 else "avoids rookies"
        traits.append({
            "kind": "rookies",
            "score": abs(delta) / ROOKIE_MIN_DELTA,
            "text": (f"{verb} — {_pct(row['rookie_rate'])} of his picks against the "
                     f"room's {_pct(row['room_rookie_rate'])}"),
        })
    auto = row.get("auto_rate")
    if auto is not None and auto >= AUTO_MIN_RATE:
        traits.append({
            "kind": "auto",
            "score": auto / AUTO_MIN_RATE,
            # Ranked first among habits on purpose: every other tendency below is
            # a statement about a manager's judgement, and this one says the
            # judgement was ESPN's.
            "text": (f"autodrafted {_pct(auto)} of his picks ({row['auto_picks']}) — "
                     f"often not in the room"),
        })
    return traits


#: Short headline per trait kind, for the summary table.
HEADLINES: Dict[str, str] = {
    "auto": "Autodrafter", "team": "Homer", "loyalty": "Has his guys",
    "timing": "Off-market timing", "build": "Distinct opening",
    "spend": "Distinct budget", "rookies": "Rookie view",
}


def _compose(traits: List[dict], owner: str, seasons: int) -> str:
    """Assemble a manager's description from their ranked traits.

    Args:
        traits: Candidate traits, each with ``kind``, ``score``, ``text``.
        owner: The manager's name, for the sentence.
        seasons: Drafts on record, for the honest empty case.

    Returns:
        str: Up to three sentences, or a statement that there is nothing to say
        yet. Each trait is its own sentence: three clauses strung together with
        commas and em-dashed numbers is unreadable at draft speed, which is the
        only speed this gets read at.
    """
    owner = display_name(owner)
    if seasons < MIN_SEASONS:
        return (f"One draft on record. Nothing here is a tendency yet — "
                f"{owner} needs a second.")
    if not traits:
        return (f"Drafts near consensus: {seasons} drafts on record and nothing "
                f"separates {owner} from the room.")

    chosen: List[dict] = []
    timing_used = 0
    kinds_used = set()
    for trait in sorted(traits, key=lambda t: -t["score"]):
        if trait["kind"] == "timing":
            if timing_used >= MAX_TIMING_TRAITS:
                continue
            timing_used += 1
        elif trait["kind"] in kinds_used:
            # One clause each for team, loyalty, rookies -- a manager with two
            # NFL-team leans is a manager with one.
            continue
        kinds_used.add(trait["kind"])
        chosen.append(trait)
        if len(chosen) == MAX_TRAITS:
            break

    sentences = [sentence_case(trait["text"]) for trait in chosen]
    return " ".join(sentences) + f" ({seasons} drafts)"


def build_tendencies(history: pl.DataFrame) -> pl.DataFrame:
    """One row per manager: the measurements, and a description built from them.

    Args:
        history: A :func:`Scripts.draft.history.fetch_draft_history` frame for one
            league, all seasons.

    Returns:
        pl.DataFrame: ``owner``, ``seasons``, ``picks``, ``headline``,
        ``description``, ``traits`` (the full ranked list), and the headline
        numbers behind each family -- ``earliest_position`` / ``earliest_delta``,
        ``latest_position`` / ``latest_delta``, ``favourite_team`` /
        ``favourite_team_excess``, ``favourite_player`` / ``favourite_player_times``,
        ``rookie_rate``, ``auto_rate``, ``top3_share``. Empty in, empty out.
    """
    schema = {
        "owner": pl.Utf8, "owner_display": pl.Utf8,
        "owner_id": pl.Utf8, "seasons": pl.UInt32,
        "first_season": pl.Int64, "last_season": pl.Int64, "picks": pl.UInt32,
        "headline": pl.Utf8, "description": pl.Utf8,
        "traits": pl.List(pl.Utf8), "earliest_position": pl.Utf8,
        "earliest_delta": pl.Float64, "latest_position": pl.Utf8,
        "latest_delta": pl.Float64, "favourite_team": pl.Utf8,
        "favourite_team_excess": pl.Float64, "favourite_player": pl.Utf8,
        "favourite_player_times": pl.UInt32, "rookie_rate": pl.Float64,
        "auto_rate": pl.Float64, "top3_share": pl.Float64,
    }
    if history.is_empty():
        return pl.DataFrame(schema=schema)

    owners = _owner_seasons(history)
    timing = positional_timing(history)
    builds = early_build(history)
    spend = auction_spend(history)
    teams = team_lean(history)
    loyalty = player_loyalty(history)
    habit = habits(history)

    rows = []
    for owner_row in owners.iter_rows(named=True):
        owner = owner_row["owner"]
        own_timing = timing.filter(pl.col("owner") == owner)
        own_build = builds.filter(pl.col("owner") == owner)
        own_spend = spend.filter(pl.col("owner") == owner)
        own_team = teams.filter(pl.col("owner") == owner)
        own_loyalty = loyalty.filter(pl.col("owner") == owner)
        own_habit = habit.filter(pl.col("owner") == owner)
        habit_row = own_habit.row(0, named=True) if not own_habit.is_empty() else {}

        seasons = int(owner_row["seasons"])
        candidates: List[dict] = []
        if seasons >= MIN_SEASONS:
            candidates += _timing_traits(own_timing)
            candidates += _build_traits(own_build)
            candidates += _spend_traits(own_spend)
            candidates += _team_traits(own_team)
            candidates += _loyalty_traits(own_loyalty)
            candidates += _habit_traits(habit_row)
        ranked = sorted(candidates, key=lambda t: -t["score"])

        earliest = own_timing.sort("delta").head(1)
        latest = own_timing.sort("delta", descending=True).head(1)
        best_team = own_team.head(1)
        best_player = own_loyalty.head(1)

        rows.append({
            # Raw and rendered spellings both kept: the raw one is what
            # ``meta.json``'s primary_owner matches on, the rendered one is what a
            # page shows. Overwriting it in place would break the highlight of
            # the manager whose league it is.
            "owner": owner,
            "owner_display": display_name(owner),
            "owner_id": owner_row["owner_id"],
            "seasons": seasons,
            "first_season": owner_row["first_season"],
            "last_season": owner_row["last_season"],
            "picks": owner_row["picks"],
            "headline": (HEADLINES.get(ranked[0]["kind"], "") if ranked
                         else ("Too new" if seasons < MIN_SEASONS else "Consensus")),
            "description": _compose(candidates, owner, seasons),
            "traits": [trait["text"] for trait in ranked],
            "earliest_position": (earliest["position"][0] if not earliest.is_empty()
                                  else None),
            "earliest_delta": (earliest["delta"][0] if not earliest.is_empty()
                               else None),
            "latest_position": (latest["position"][0] if not latest.is_empty()
                                else None),
            "latest_delta": (latest["delta"][0] if not latest.is_empty() else None),
            "favourite_team": (best_team["pro_team"][0] if not best_team.is_empty()
                               else None),
            "favourite_team_excess": (
                float(best_team["picks"][0] - best_team["expected"][0])
                if not best_team.is_empty() else None),
            "favourite_player": (best_player["player_name"][0]
                                 if not best_player.is_empty() else None),
            "favourite_player_times": (best_player["times"][0]
                                       if not best_player.is_empty() else None),
            "rookie_rate": habit_row.get("rookie_rate"),
            "auto_rate": habit_row.get("auto_rate"),
            "top3_share": (own_spend["top3_share"][0] if not own_spend.is_empty()
                           else None),
        })

    return (pl.DataFrame(rows, schema_overrides=schema)
            .sort(["seasons", "picks"], descending=True))


def tendencies_summary(tendencies: pl.DataFrame) -> str:
    """A one-line description of a tendencies frame, for refresh output.

    Args:
        tendencies: A :func:`build_tendencies` frame.

    Returns:
        str: e.g. ``"6 managers, 5 with a tendency, 2 too new"``.
    """
    if tendencies.is_empty():
        return "no managers"
    too_new = int((tendencies["seasons"] < MIN_SEASONS).sum())
    described = int((tendencies["traits"].list.len() > 0).sum())
    return (f"{tendencies.height} managers, {described} with a tendency, "
            f"{too_new} too new")
