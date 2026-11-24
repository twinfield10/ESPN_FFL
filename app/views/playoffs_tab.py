"""The Playoffs tab: seeds and the bracket, "if the season ended today".

Three sections. **Seeds** is the whole league in ESPN's order -- division winners first,
then the league's own tiebreaker -- with the playoff line drawn and every tie
explained, because a seed that differs from the table you would sort yourself (Jeffs
2026: the league's top scorer fourth on head-to-head) needs its reason on the page.
**Bracket** is the fixed bracket those seeds produce, one column per round with the
scoring weeks it is played in. **Odds** is the nightly simulation of the rest of the
season and the bracket (:mod:`Scripts.playoff_odds`): each seed, the playoffs, a bye,
the division, the title, and each team's likeliest first playoff opponent.

The rules come from ``meta["playoff_settings"]``, which the nightly records from
ESPN's ``mSettings`` (see :func:`Scripts.playoffs.settings_from_espn`); the results
from the stitched ``team_stats``, so Jeffs_League includes the weeks played before
it was recreated. Seeding logic lives in :mod:`Scripts.playoffs`; this module only
draws.

See ``docs/plans/57-playoff-bracket.md``.
"""

from typing import Dict, List, Optional

import polars as pl
import streamlit as st

import session
import store
from views.home_tab import OWN_ROW_FILL

from Scripts import playoffs as po

SEEDS_CONFIG: Dict[str, object] = {
    "Seed": st.column_config.NumberColumn(format="%d", pinned=True, width="small"),
    "Owner": st.column_config.TextColumn(pinned=True),
    "W-L-T": st.column_config.TextColumn(width="small"),
    "PF": st.column_config.NumberColumn(format="%.1f", width="small", help="Points for."),
    "PA": st.column_config.NumberColumn(format="%.1f", width="small",
                                        help="Points against."),
    "Status": st.column_config.TextColumn(
        width="small",
        help="Bye: skips the first round. In: plays the first round. Out: below the "
             "playoff line."),
    "Tiebreak": st.column_config.TextColumn(
        help="Which teams this one is level with on win percentage, and the league "
             "rule that ordered them. Division winners and wildcards are ranked "
             "separately, so they are never tied with each other."),
}


def odds_frame(odds: pl.DataFrame, settings: dict) -> pl.DataFrame:
    """The odds table as displayed: percentages, and only the columns that apply."""
    k = int(settings.get("playoff_team_count") or 0)
    pct = lambda c: (pl.col(c) * 100).alias(c)                      # noqa: E731
    seeds = [pl.col(f"p_seed_{i}").mul(100).alias(f"#{i}") for i in range(1, k + 1)
             if f"p_seed_{i}" in odds.columns]
    record = pl.format("{}-{}", pl.col("exp_wins").round(1), pl.col("exp_losses").round(1))
    opponent = (pl.when(pl.col("likeliest_opponent").is_not_null())
                .then(pl.format("{} ({}%)", "likeliest_opponent",
                                (pl.col("likeliest_opponent_p") * 100).round(0).cast(pl.Int64)))
                .otherwise(pl.lit("")))
    cols = [pl.col("team_owner").alias("Owner"),
            pl.col("proj_per_week").alias("Proj/Wk"), record.alias("Exp W-L"),
            pct("p_playoffs").alias("Playoffs")]
    if po.byes(k):
        cols.append(pct("p_bye").alias("Bye"))
    if odds["p_division"].null_count() < odds.height:
        cols.append(pct("p_division").alias("Division"))
    cols += [*seeds, pct("p_final").alias("Final"), pct("p_champion").alias("Title"),
             opponent.alias("Likeliest First Opponent")]
    return odds.select(cols)


def odds_config(frame: pl.DataFrame) -> Dict[str, object]:
    """Column config for :func:`odds_frame`'s output."""
    pct = lambda h: st.column_config.NumberColumn(format="%.1f%%", width="small", help=h)  # noqa: E731
    cfg: Dict[str, object] = {
        "Owner": st.column_config.TextColumn(pinned=True),
        "Proj/Wk": st.column_config.NumberColumn(
            format="%.1f", width="small",
            help="Mean projected starting lineup over the regular-season weeks left, "
                 "after byes and injuries, with lineups set by ROS rank."),
        "Exp W-L": st.column_config.TextColumn(
            width="small", help="Expected final regular-season record."),
        "Playoffs": st.column_config.ProgressColumn(
            format="%.1f%%", min_value=0, max_value=100, width="small"),
        "Bye": pct("A top seed: skips the first round."),
        "Division": pct("Finishes first in its division — and is seeded above every wildcard."),
        "Final": pct("Reaches the championship game."),
        "Title": st.column_config.ProgressColumn(
            format="%.1f%%", min_value=0, max_value=100, width="small"),
        "Likeliest First Opponent": st.column_config.TextColumn(
            help="Most common opponent in this team's first playoff game — the round-2 "
                 "opponent for a team on a bye — and how often, among the simulations "
                 "where it makes the playoffs."),
    }
    for c in frame.columns:
        if c.startswith("#"):
            cfg[c] = pct(f"Finishes as the {c[1:]} seed.")
    return cfg


def render_odds(selection: session.Selection, settings: dict) -> None:
    """The simulated odds, when the nightly has built them."""
    st.subheader("Odds")
    if not store.has_artifact(selection.season, selection.league_key, "playoff_odds"):
        st.info("No playoff odds in this store yet. The nightly builds them after "
                "`team_stats`.")
        st.code(f"python -m Scripts.refresh --league {selection.display_name} "
                f"--season {selection.season} --what playoff_odds", language="bash")
        return
    odds = store.load_playoff_odds(selection.season, selection.league_key)
    if odds.is_empty():
        st.info("The odds artifact is empty.")
        return
    frame = odds_frame(odds, settings)
    n = int(odds["n_sims"][0])
    through = int(odds["through_week"][0])
    regular = settings.get("regular_season_periods")
    st.dataframe(_styled_odds(frame, selection.my_owner), width="content", hide_index=True,
                 placeholder="", lazy=False, column_config=odds_config(frame))
    st.caption(
        f"{n:,} simulations of weeks {through + 1}–{regular} and the bracket, seeded by "
        f"the league's own rules. Each manager starts the best lineup by FantasyPros' "
        f"rest-of-season ranking; each starter scores around this week's projection — "
        f"which beat the season average at every horizon on 2026's results — with the "
        f"Matchup tab's calibrated spread. Byes and ESPN's in/out status are honoured; "
        f"rosters are frozen, so waivers and trades are not.")


def _styled_odds(frame: pl.DataFrame, owner: Optional[str]):
    """Band the viewer's row. Never raises."""
    try:
        pdf = frame.to_pandas()
        band = f"background-color: {OWN_ROW_FILL}; font-weight: 700"
        return pdf.style.apply(
            lambda row: [band if owner is not None and row["Owner"] == owner else ""]
            * len(row), axis=1).format(na_rep="")
    except Exception:                    # noqa: BLE001 - cosmetic, never fatal
        return frame


def completed_through(meta: dict, settings: dict) -> int:
    """The last regular-season week whose results count.

    The week in progress never counts -- ESPN seeds on completed matchups -- and
    nothing after the regular season does.
    """
    current = int(meta.get("current_matchup_period") or meta.get("current_week") or 1)
    regular = int(settings.get("regular_season_periods") or current)
    return max(0, min(current - 1, regular))


def seeds_frame(seeds: pl.DataFrame, settings: dict) -> pl.DataFrame:
    """The seeds table as displayed."""
    n = int(settings.get("playoff_team_count") or 0)
    byes = po.byes(n)
    multi_div = seeds["division"].n_unique() > 1
    record = pl.format("{}-{}-{}", "wins", "losses", "ties")
    status = (pl.when(pl.col("seed") <= byes).then(pl.lit("Bye"))
              .when(pl.col("in_playoffs")).then(pl.lit("In"))
              .otherwise(pl.lit("Out")))
    division = (pl.when(pl.col("division_winner"))
                .then(pl.col("division") + " ★").otherwise(pl.col("division")))
    cols = [pl.col("seed").alias("Seed"), pl.col("team").alias("Owner"),
            *([division.alias("Division")] if multi_div else []),
            record.alias("W-L-T"), pl.col("points_for").alias("PF"),
            pl.col("points_against").alias("PA"), status.alias("Status"),
            pl.col("tiebreak").alias("Tiebreak")]
    return seeds.select(cols)


def _styled(frame: pl.DataFrame, owner: Optional[str], cut: int):
    """Band the viewer's row and rule the playoff line. Never raises."""
    try:
        pdf = frame.to_pandas()
        band = f"background-color: {OWN_ROW_FILL}; font-weight: 700"
        line = "border-bottom: 2px solid rgba(128, 128, 128, 0.7)"

        def row_style(row) -> List[str]:
            styles = []
            if owner is not None and row["Owner"] == owner:
                styles.append(band)
            if int(row["Seed"]) == cut:
                styles.append(line)
            return ["; ".join(styles)] * len(row)

        return pdf.style.apply(row_style, axis=1).format(na_rep="")
    except Exception:                    # noqa: BLE001 - cosmetic, never fatal
        return frame


def slot_label(slot: po.Slot, names: Dict[int, str], prev_round: str) -> str:
    """``(3) Sam Giammalva``, or ``Winner of Quarterfinal 2``."""
    if slot.seed is not None:
        return f"({slot.seed}) {names.get(slot.seed, '—')}"
    return f"Winner of {prev_round} {slot.winner_of + 1}"


def bracket_rows(seeds: pl.DataFrame, settings: dict) -> List[dict]:
    """Every bracket game as display text, round by round.

    Returns:
        list: ``{round, name, weeks, game, high, low}`` dicts in bracket order.
    """
    n = int(settings.get("playoff_team_count") or 0)
    rounds = po.bracket(n)
    names = {int(r["seed"]): r["team"] for r in seeds.iter_rows(named=True)}
    labels = po.round_names(len(rounds))
    weeks = po.round_weeks(settings, len(rounds))
    out = []
    for r, games in enumerate(rounds):
        prev = _singular(labels[r - 1]) if r else ""
        for g in games:
            out.append({
                "round": r + 1, "name": labels[r], "weeks": weeks[r], "game": g.index + 1,
                "high": slot_label(g.high, names, prev),
                "low": slot_label(g.low, names, prev),
            })
    return out


def _singular(name: str) -> str:
    return name[:-1] if name.endswith("s") else name


def _weeks_text(weeks: List[int]) -> str:
    if not weeks:
        return ""
    return f"Week {weeks[0]}" if len(weeks) == 1 else f"Weeks {weeks[0]}–{weeks[-1]}"


def render_bracket(seeds: pl.DataFrame, settings: dict, owner: Optional[str]) -> None:
    """One column per round; byes listed above the first."""
    rows = bracket_rows(seeds, settings)
    if not rows:
        st.info("This league's settings have fewer than two playoff teams.")
        return
    n_rounds = max(r["round"] for r in rows)
    byes = po.byes(int(settings.get("playoff_team_count") or 0))
    names = {int(r["seed"]): r["team"] for r in seeds.iter_rows(named=True)}

    for col, r in zip(st.columns(n_rounds), range(1, n_rounds + 1)):
        games = [g for g in rows if g["round"] == r]
        with col:
            st.markdown(f"**{games[0]['name']}**")
            st.caption(_weeks_text(games[0]["weeks"]))
            if r == 1 and byes:
                st.caption("Byes: " + ", ".join(
                    f"({s}) {names.get(s, '—')}" for s in range(1, byes + 1)))
            for g in games:
                with st.container(border=True):
                    for side in (g["high"], g["low"]):
                        mine = owner is not None and side.endswith(f") {owner}")
                        st.markdown(f"**{side}**" if mine else side)


def render_playoffs(selection: session.Selection) -> None:
    """Draw the tab for the selected league."""
    st.title("Playoffs")
    settings = (selection.meta or {}).get("playoff_settings")
    if not settings:
        st.info(
            "This store has no playoff settings yet. They are recorded by the "
            "nightly lineups build from ESPN's league settings; the next 06:00 run "
            "adds them.")
        st.code(f"python -m Scripts.refresh --league {selection.display_name} "
                f"--season {selection.season} --what lineups", language="bash")
        return
    if not store.has_artifact(selection.season, selection.league_key, "team_stats"):
        st.info("No `team_stats` in this store, which is where results come from.")
        st.code(f"python -m Scripts.refresh --league {selection.display_name} "
                f"--season {selection.season} --what team_stats", language="bash")
        return

    team_stats = store.load_team_stats(selection.season, selection.league_key)
    through = completed_through(selection.meta, settings)
    games = po.games_from_team_stats(team_stats, selection.season, through_week=through)
    divisions = po.divisions_from_team_stats(team_stats, selection.season)
    if not divisions:
        st.info(f"No {selection.season} results in `team_stats` yet.")
        return
    seeds = po.seed(games, divisions, settings)

    n = int(settings.get("playoff_team_count") or 0)
    rule = po.TIEBREAK_LABELS.get(settings.get("seeding_rule"), settings.get("seeding_rule"))
    multi_div = seeds["division"].n_unique() > 1
    regular = settings.get("regular_season_periods")
    st.caption(
        f"If the season ended after week {through} of {regular}. "
        f"{n} of {len(divisions)} teams make it"
        + (f", top {po.byes(n)} on a bye" if po.byes(n) else "")
        + f". Tiebreaker: {rule.lower()}"
        + (", with division winners (★) seeded first" if multi_div else "")
        + "."
        + (" Seeding is fixed — no reseeding between rounds." if not settings.get("reseed")
           else " ESPN reseeds between rounds; later rounds here assume it does not."))

    st.subheader("Seeds")
    st.dataframe(_styled(seeds_frame(seeds, settings), selection.my_owner, n),
                 width="content", hide_index=True, placeholder="", lazy=False,
                 column_config=SEEDS_CONFIG)

    render_odds(selection, settings)

    st.subheader("Bracket")
    st.caption("If the season ended today.")
    render_bracket(seeds, settings, selection.my_owner)
