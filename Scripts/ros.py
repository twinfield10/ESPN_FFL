"""Rest-of-season value: FantasyPros' consensus, used for the one thing it can do.

`Scripts/scrape_FP.scrape_ros` captures a rest-of-season ranking every night. This
module joins it to a league's weekly frame and decides what may be read off it.

**The ordering is transferable and the points are not.** Three separate reasons,
each measured rather than argued:

1. ``r2p_pts`` is a total over the games *FantasyPros* thinks a player will play.
   Dividing it by *our* games-remaining mixes their numerator with our denominator,
   so a player whose absence they have already priced gets discounted twice. That
   is `docs/plans/43-tomcat-out-of-season-blend.md`'s level error with different
   column names, and it is the reason this module publishes no rest-of-season
   points into the blend.
2. It is denominated in STD scoring, and this repo scores every league from stat
   lines in its own rules. Points never compare across leagues; ranks do --
   `docs/DATA_CATALOGUE.md` says so, and `Scripts/draft/board.replacement_ranks`
   already turns a rank into a league-specific verdict.
3. ``total_experts`` is **2-3** on these pages, against 100+ on the in-season
   consensus. ``rank_std`` is a spread over three opinions. A number that thin can
   order players; it cannot set a level.

So the columns this attaches are a **rank**, its spread, and an explicitly
approximate per-game figure for display. :func:`ros_ppg` exists because a reader
wants a magnitude, and it is built through a measured per-position conversion --
but nothing in the decision path reads it, and deleting it would leave the gates
untouched.

**No column here is named ``*_Points``.** That suffix is how
``projection_utils.WEEKLY_PREFIXES``, ``present_prefixes`` and
``lineup.points_columns`` recognise a blend *source*, and this is not one -- it
would grow a spurious column in every weekly table and make ``real_sources`` lie.

See ``docs/plans/49-rest-of-season-waivers.md``.
"""

from typing import Dict, Optional, Sequence

import pandas as pd

from Scripts.paths import season_dir
from Scripts.season_projections import normalise_name

#: Positions FantasyPros publishes a rest-of-season consensus for.
#:
#: **IDP is absent and that is permanent as far as this repo is concerned.** On
#: `gop_degenerates` -- the only IDP league configured -- that is 150 of 284
#: free-agent rows with no rest-of-season number available at any price. They must
#: read as an abstention, never as zero.
RANKED_POSITIONS = frozenset({"QB", "RB", "WR", "TE", "K", "D/ST"})

#: Why a row has no rest-of-season number. Never null on an unmatched row.
MISSING_NO_PUBLICATION = "no_ros_publication"
MISSING_NOT_RANKED = "not_ranked"
MISSING_NO_POINTS = "no_r2p_pts"

#: Back-compat alias. The reason used to be called ``unmatched_name``, which
#: asserted more than it knew: a row can fail to join because the spelling differs
#: *or* because FantasyPros simply does not rank that player, and the two want
#: opposite responses. Checked on the first capture -- Jayden Higgins, Ricky
#: Pearsall, Justin Tucker and Younghoe Koo are absent from the source outright,
#: no surname variant anywhere -- so the common case is a gap, not a defect.
#: ``python -m Scripts.name_audit`` is what tells the two apart, and this source is
#: registered there.
MISSING_UNMATCHED = MISSING_NOT_RANKED

#: Minimum experts behind a ranking before it is allowed to mean anything.
#:
#: Two, because with one there is no consensus, only a person. K and D/ST sit
#: exactly on this floor.
MIN_EXPERTS = 2

#: Floor on the weekly FantasyPros number used as the conversion denominator.
#:
#: Below this the ratio is dominated by its denominator, and it is exactly the
#: deep-pool players this feature is about who sit there. They take the position
#: median instead.
MIN_STD_POINTS = 3.0


def ros_path(season: int):
    """Where :func:`Scripts.scrape_FP.scrape_ros` writes."""
    return season_dir("FantasyPros", season, "FantasyPros_ROS_Ranks.parquet")


def load_ros(season: int, on_or_before: Optional[str] = None
             ) -> Optional[pd.DataFrame]:
    """The most recent capture of the rest-of-season consensus.

    Args:
        season: Season year.
        on_or_before: ``YYYY-MM-DD``. Capped to captures at or before this date,
            which is what makes a replay leak-free -- scoring week 5's suggestions
            against week 12's ranking would be marking your own homework.

    Returns:
        pd.DataFrame | None: One row per ranked player, or None when nothing has
        been captured yet. None is not an error: the scrape is a nightly stage and
        the app must render the day before it first runs.
    """
    path = ros_path(season)
    if not path.exists():
        return None
    frame = pd.read_parquet(path)
    if frame.empty:
        return None
    if on_or_before:
        frame = frame[frame["captured_date"] <= str(on_or_before)]
        if frame.empty:
            return None
    latest = frame["captured_date"].max()
    return frame[frame["captured_date"] == latest].reset_index(drop=True)


def scoring_factors(lineups: pd.DataFrame, weekly_fp: Optional[pd.DataFrame],
                    week: Optional[int] = None) -> Dict[str, float]:
    """Per-position STD-to-this-league conversion, measured off the two files.

    ``FP_Points`` on the weekly artifact is FantasyPros' stat line scored in *this
    league's* rules; ``STD_FantasyPoints`` on the source file is the same line in
    STD. Their ratio is the conversion, and it is **taken as a position median
    rather than per player**: measured on 2026 week 1 across three leagues the
    within-position coefficient of variation is 0.018-0.025 at QB, 0.05-0.12 at the
    skill positions and 0.08-0.22 at K and D/ST, so the median carries nearly all
    of it -- and a per-player ratio would divide by a near-zero projection for
    exactly the deep-pool players this is for.

    The spread *between* leagues is large (D/ST 2.66 on GOP against 0.88 on
    Winfield) and is the conversion doing its job.

    Args:
        lineups: A league's weekly frame, carrying ``FP_Points``.
        weekly_fp: ``FantasyPros_Projections_Week_All.parquet``, or None.
        week: Week to measure on. Defaults to the frame's latest.

    Returns:
        dict: Position to factor. Empty when either input is missing, which the
        caller must treat as "no per-game figure", not as a factor of 1.0.
    """
    if (lineups is None or lineups.empty or weekly_fp is None or weekly_fp.empty
            or "FP_Points" not in lineups.columns
            or "STD_FantasyPoints" not in weekly_fp.columns):
        return {}

    week = int(lineups["week"].max()) if week is None else int(week)
    left = lineups[lineups["week"] == week].copy()
    right = weekly_fp[weekly_fp["week"] == week].copy()
    if left.empty or right.empty:
        return {}

    left["_key"] = left["player_name"].map(normalise_name)
    right["_key"] = right["player_name"].map(normalise_name)
    right = right.drop_duplicates(subset=["_key"])

    merged = left.merge(right[["_key", "STD_FantasyPoints"]], on="_key", how="inner")
    merged = merged[(merged["STD_FantasyPoints"] >= MIN_STD_POINTS)
                    & merged["FP_Points"].notna()]
    if merged.empty:
        return {}

    merged["_k"] = merged["FP_Points"] / merged["STD_FantasyPoints"]
    factors = merged.groupby("player_position")["_k"].median()
    return {str(pos): float(k) for pos, k in factors.items() if pd.notna(k)}


def factor_spread(lineups: pd.DataFrame, weekly_fp: Optional[pd.DataFrame],
                  week: Optional[int] = None) -> Dict[str, float]:
    """Within-position coefficient of variation of the conversion factor.

    The error bar on :func:`scoring_factors`, and the quantity gate **G-R1** reads.
    A position whose factor does not hold together has no business publishing a
    per-game number.

    Args:
        lineups: A league's weekly frame.
        weekly_fp: The FantasyPros weekly file.
        week: Week to measure on.

    Returns:
        dict: Position to ``sd / mean``.
    """
    if (lineups is None or lineups.empty or weekly_fp is None or weekly_fp.empty
            or "FP_Points" not in lineups.columns
            or "STD_FantasyPoints" not in weekly_fp.columns):
        return {}

    week = int(lineups["week"].max()) if week is None else int(week)
    left = lineups[lineups["week"] == week].copy()
    right = weekly_fp[weekly_fp["week"] == week].copy()
    if left.empty or right.empty:
        return {}

    left["_key"] = left["player_name"].map(normalise_name)
    right["_key"] = right["player_name"].map(normalise_name)
    merged = left.merge(right.drop_duplicates(subset=["_key"])[
        ["_key", "STD_FantasyPoints"]], on="_key", how="inner")
    merged = merged[(merged["STD_FantasyPoints"] >= MIN_STD_POINTS)
                    & merged["FP_Points"].notna()]
    if merged.empty:
        return {}

    merged["_k"] = merged["FP_Points"] / merged["STD_FantasyPoints"]
    grouped = merged.groupby("player_position")["_k"]
    out = {}
    for pos, series in grouped:
        if len(series) < 3 or not series.mean():
            continue
        out[str(pos)] = float(series.std() / series.mean())
    return out


def attach_ros(frame: pd.DataFrame, ros: Optional[pd.DataFrame],
               factors: Optional[Dict[str, float]] = None,
               games_remaining: Optional[int] = None) -> pd.DataFrame:
    """Join the rest-of-season consensus onto a weekly frame.

    Two join keys, because one does not work for both. Skill positions join on the
    normalised name; **D/ST joins on ``pro_team``**, because FantasyPros calls a
    defence "Houston Texans" where the store says "Texans D/ST" and 0 of 14 match
    by name.

    Args:
        frame: A weekly lineups frame, or any slice of one.
        ros: :func:`load_ros` output, or None.
        factors: :func:`scoring_factors` output. Without it no per-game figure is
            published -- which is right, not a degradation.
        games_remaining: Weeks left for the per-game figure. See the caveat on
            :func:`ros_ppg`.

    Returns:
        pd.DataFrame: ``frame`` plus ``ros_pos_rank``, ``ros_rank_ecr``,
        ``ros_rank_sd``, ``ros_experts``, ``ros_std_total``, ``ros_owned``,
        ``ros_captured_at``, ``ros_ppg`` and ``ros_missing_reason``. Every ROS
        column is null where the source says nothing; none is ever ``0.0``.
    """
    out = frame.copy()
    columns = ["ros_pos_rank", "ros_rank_ecr", "ros_rank_sd", "ros_experts",
               "ros_std_total", "ros_owned", "ros_captured_at", "ros_ppg"]
    for column in columns:
        out[column] = None

    if ros is None or ros.empty:
        out["ros_missing_reason"] = MISSING_UNMATCHED
        out.loc[~out["player_position"].isin(RANKED_POSITIONS),
                "ros_missing_reason"] = MISSING_NO_PUBLICATION
        return out

    source = ros.copy()
    source["_key"] = source["player_name"].map(normalise_name)

    by_name = source[source["position"] != "DST"].drop_duplicates(subset=["_key"])
    by_team = source[source["position"] == "DST"].drop_duplicates(subset=["pro_team"])

    lookup_name = by_name.set_index("_key")
    lookup_team = by_team.set_index("pro_team")

    picked = []
    for position, name, team in zip(out.get("player_position"),
                                    out.get("player_name"),
                                    out.get("pro_team")):
        if position == "D/ST":
            row = lookup_team.loc[team] if team in lookup_team.index else None
        else:
            key = normalise_name(name)
            row = lookup_name.loc[key] if key in lookup_name.index else None
        picked.append(row)

    out["ros_pos_rank"] = [None if r is None else r.get("pos_rank") for r in picked]
    out["ros_rank_ecr"] = [None if r is None else r.get("rank_ecr") for r in picked]
    out["ros_rank_sd"] = [None if r is None else r.get("rank_std") for r in picked]
    out["ros_experts"] = [None if r is None else r.get("total_experts") for r in picked]
    out["ros_std_total"] = [None if r is None else r.get("r2p_pts") for r in picked]
    out["ros_owned"] = [None if r is None else r.get("player_owned_avg") for r in picked]
    out["ros_captured_at"] = [None if r is None else r.get("captured_at")
                              for r in picked]

    reasons = []
    for position, row in zip(out.get("player_position"), picked):
        if position not in RANKED_POSITIONS:
            reasons.append(MISSING_NO_PUBLICATION)
        elif row is None:
            reasons.append(MISSING_UNMATCHED)
        elif pd.isna(row.get("r2p_pts")):
            reasons.append(MISSING_NO_POINTS)
        else:
            reasons.append(None)
    out["ros_missing_reason"] = reasons

    if factors and games_remaining:
        out["ros_ppg"] = [
            ros_ppg(total, factors.get(position), games_remaining)
            for total, position in zip(out["ros_std_total"], out["player_position"])
        ]
    return out


def ros_ppg(std_total, factor, games_remaining) -> Optional[float]:
    """A per-game rest-of-season figure in this league's points. **Display only.**

    ``std_total`` is a total over the games FantasyPros expects the player to play,
    and this divides by the games *we* count, so for anybody they have already
    discounted the two disagree and this reads low. That is why nothing in the
    decision path uses it: the gates compare ranks, which carry no such assumption.

    Args:
        std_total: ``r2p_pts``.
        factor: This position's conversion from :func:`scoring_factors`.
        games_remaining: Weeks left in the regular season.

    Returns:
        float | None: None whenever any input is missing -- never 0.0.
    """
    if std_total is None or factor is None or not games_remaining:
        return None
    if pd.isna(std_total):
        return None
    return float(std_total) * float(factor) / float(games_remaining)


def coverage(attached: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    """Join coverage per position, which is what gate **G-R0** reads.

    Args:
        attached: :func:`attach_ros` output.

    Returns:
        dict: Position to ``{"rows", "ranked", "no_publication", "unmatched"}``.
    """
    out: Dict[str, Dict[str, int]] = {}
    if attached is None or attached.empty:
        return out
    for position, group in attached.groupby("player_position"):
        out[str(position)] = {
            "rows": int(len(group)),
            "ranked": int(group["ros_pos_rank"].notna().sum()),
            "no_publication": int(
                (group["ros_missing_reason"] == MISSING_NO_PUBLICATION).sum()),
            "unmatched": int(
                (group["ros_missing_reason"] == MISSING_UNMATCHED).sum()),
        }
    return out
