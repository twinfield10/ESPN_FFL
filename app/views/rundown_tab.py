"""The Rundown -- how the draft you just had grades against the room.

Shown only once a league has picks for this season. Before that it says so, because
seven of ten leagues were in exactly that state the day this was written and an empty
table would have read as a bug.

Three bases side by side rather than one verdict, for the reason :mod:`rundown` gives:
"did I draft well" depends on whose projections you believe, and the useful answer is
where the three agree and where they do not. A team the room and The Athletic disagree
about is the interesting row on this table.

Graded against the **frozen** board where one exists -- see
:func:`store.draft_basis`. The caption says which basis it used, always, because a
rundown read off a board four months of news later measures luck rather than drafting.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import polars as pl
import streamlit as st

import rundown as rd
import store
from views.draft_tabs import BoardContext


def _team_config(bases) -> dict:
    """Column config for the league table, built from the bases in play.

    Args:
        bases: From :func:`rundown.available_bases`.

    Returns:
        dict: Column label to a ``st.column_config`` object.
    """
    config = {
        "Manager": st.column_config.TextColumn(pinned=True),
        "Consensus": st.column_config.NumberColumn(
            format="%.1f",
            help="Mean of this team's rank under each basis. 1.0 means every source "
                 "agrees they drafted best; a spread means the sources disagree "
                 "about them, which is more interesting than either rank alone."),
        "Picks": st.column_config.NumberColumn(format="%.0f"),
        "VOR": st.column_config.NumberColumn(
            format="%+.0f",
            help="Points their best starting lineup projects above replacement, on "
                 "our numbers. Measured over starters only, because replacement "
                 "level is defined by this league's starting slots — a deep bench "
                 "is not a penalty."),
        "Value": st.column_config.NumberColumn(
            format="%+.0f",
            help="Summed rank-against-ADP across the whole roster. Positive means "
                 "the room let their players fall past what they are worth here. "
                 "This is the one column about the draft rather than the roster."),
        "Floor": st.column_config.NumberColumn(
            format="%.0f",
            help="10th percentile of the starting lineup's season total, from the "
                 "per-player spreads added independently. Understates the width — "
                 "real starters are correlated — and kickers and defences carry no "
                 "spread at all, so they add their means and no variance."),
        "Ceiling": st.column_config.NumberColumn(
            format="%.0f", help="90th percentile, on the same construction as Floor."),
    }
    for basis in bases:
        config[basis.label] = st.column_config.NumberColumn(
            format="%.0f", help=f"Best legal starting lineup, projected. {basis.blurb}")
        config[f"{basis.label} #"] = st.column_config.NumberColumn(
            format="%.0f", help=f"Rank in the league on {basis.label}'s numbers.")
        config[f"{basis.label} Grade"] = st.column_config.TextColumn(
            help="A rescaling of the rank beside it and nothing more — no new "
                 "evidence, and it cannot disagree with that rank.")
    return config


def _display(table: pl.DataFrame, bases) -> pl.DataFrame:
    """Relabel :func:`rundown.team_table` for the screen.

    Args:
        table: From :func:`rundown.team_table`.
        bases: From :func:`rundown.available_bases`.

    Returns:
        pl.DataFrame: House-style Title Case labels, columns in reading order.
    """
    columns = {"owner": "Manager", "consensus_rank": "Consensus"}
    order = ["Manager", "Consensus"]
    for basis in bases:
        columns[f"{basis.key}_points"] = basis.label
        columns[f"{basis.key}_rank"] = f"{basis.label} #"
        columns[f"{basis.key}_grade"] = f"{basis.label} Grade"
        order += [basis.label, f"{basis.label} #", f"{basis.label} Grade"]
    columns.update({"floor": "Floor", "ceiling": "Ceiling", "vor": "VOR",
                    "value": "Value", "picks": "Picks"})
    order += ["Floor", "Ceiling", "VOR", "Value", "Picks"]

    present = {old: new for old, new in columns.items() if old in table.columns}
    renamed = table.rename(present)
    return renamed.select([c for c in order if c in renamed.columns])


def _not_drafted(selection) -> None:
    """Say the draft has not happened, and what will be here when it has.

    Args:
        selection: The active :class:`session.Selection`.
    """
    st.info(
        f"**{selection.display_name} has not drafted yet.** Once it has, this is "
        f"where every roster in the league gets scored under ESPN's projections, "
        f"The Athletic's and ours — best legal starting lineup, rank, and where the "
        f"three disagree."
    )
    st.caption(
        "Picks arrive with `python -m Scripts.refresh --league "
        f"{selection.display_name} --what draft`, which is not part of the nightly: "
        "a finished draft never changes, so it is pulled once rather than every "
        "morning."
    )


def render_rundown(ctx: BoardContext) -> None:
    """Render the Rundown sub-tab -- the morning-after view.

    Args:
        ctx: From :func:`views.draft_tabs.prepare`.
    """
    selection = ctx.selection

    if not store.has_artifact(selection.season, selection.league_key, "draft"):
        _not_drafted(selection)
        return

    # Graded on the frozen board where there is one. Deliberately a separate read
    # from ctx.board, which is the live, budget-rescaled board the other five
    # sub-tabs work from -- those answer "what should I do now", this one answers
    # "what did I do then", and they are different boards on purpose.
    basis_board, basis_label = store.draft_basis(
        selection.season, selection.league_key)
    picks = store.load_draft(selection.season, selection.league_key)
    joined = rd.picks_with_board(picks, basis_board, selection.season)

    if joined.is_empty():
        _not_drafted(selection)
        return

    bases = rd.available_bases(basis_board)
    if not bases:
        st.warning(
            "This board carries none of the season-projection columns the rundown "
            "grades on. Rebuild it with `--what board`."
        )
        return

    starting_slots = ctx.meta.get("starting_slots") or {}
    table = rd.team_table(joined, starting_slots, bases)
    for basis in bases:
        table = rd.median_gap(table, basis)

    st.subheader("How The Room Drafted")

    if basis_label == "frozen":
        frozen_at = (ctx.meta.get("frozen_at") or "")[:16].replace("T", " ")
        st.caption(
            f"Graded on the **frozen** board, taken {frozen_at}. That is the point "
            f"of freezing it: `board.parquet` rebuilds every morning, so grading a "
            f"September draft against it in December would measure who got lucky."
        )
    else:
        st.caption(
            "Graded on the **live** board, which rebuilds every morning at 06:00 — "
            "so these numbers will drift as the season moves. Run "
            "`python -m Scripts.freeze --all` to pin them to the day the drafts "
            "finished."
        )

    mine = rd.my_row(table, selection.my_owner)
    if mine:
        _my_headline(mine, table, bases, selection)

    st.dataframe(
        _display(table, bases), width="stretch", hide_index=True,
        column_config=_team_config(bases), placeholder="", lazy=False,
    )
    st.caption(
        f"Best **legal starting lineup** under each basis, not the sum of the "
        f"roster — a season is scored out of "
        f"{sum(starting_slots.values())} slots, so four good quarterbacks are one "
        f"good quarterback. Slots a roster cannot fill contribute nothing."
    )

    _disagreement(table, bases)

    if selection.my_owner:
        _my_picks(joined, selection.my_owner, starting_slots, bases)


def _my_headline(mine: dict, table: pl.DataFrame, bases, selection) -> None:
    """The viewer's own result, as a metric row.

    Args:
        mine: From :func:`rundown.my_row`.
        table: From :func:`rundown.team_table`.
        bases: The bases in play.
        selection: The active selection.
    """
    teams = table.height
    columns = st.columns(len(bases) + 1)
    for column, basis in zip(columns, bases):
        rank = int(mine[f"{basis.key}_rank"])
        gap = mine.get(f"{basis.key}_vs_median")
        column.metric(
            f"{basis.label} · {mine[f'{basis.key}_grade']}",
            f"{rank} of {teams}",
            None if gap is None else f"{gap:+.0f} vs median",
            delta_color="normal",
            help=basis.blurb,
        )
    floor, ceiling = mine.get("floor"), mine.get("ceiling")
    columns[-1].metric(
        "Our Range",
        "—" if floor is None else f"{floor:.0f}–{ceiling:.0f}",
        f"{mine.get('interval_starters') or 0} starters priced",
        delta_color="off",
        help="80% band on your starting lineup's season total. Independent sum of "
             "the per-player spreads, so it is a lower bound on its own width.",
    )
    st.caption(f"**{selection.my_owner}**, on each source's numbers.")


def _disagreement(table: pl.DataFrame, bases) -> None:
    """Name the teams the bases disagree most about.

    The genuinely additional thing a three-basis table can say, and the reason this
    is not one averaged grade: a roster ESPN likes and The Athletic does not is a
    roster whose value rests on a claim you can go and check.

    Args:
        table: From :func:`rundown.team_table`.
        bases: The bases in play.
    """
    if len(bases) < 2 or table.height < 3:
        return

    ranks = [pl.col(f"{basis.key}_rank") for basis in bases]
    spread = table.with_columns(
        (pl.max_horizontal(ranks) - pl.min_horizontal(ranks)).alias("spread")
    ).sort("spread", descending=True)

    worst = spread.head(1).to_dicts()[0]
    if (worst["spread"] or 0) < 2:
        st.caption("The three bases rank every team within a place of each other.")
        return

    parts = ", ".join(
        f"{basis.label} **{int(worst[f'{basis.key}_rank'])}**" for basis in bases)
    st.caption(
        f"**Widest disagreement: {worst['owner']}** — {parts}. A gap that size is "
        f"one source's view of a few players rather than a view of the draft, and "
        f"the Calibration sub-tab is where you find out which players."
    )


def _my_picks(joined: pl.DataFrame, owner: str, starting_slots: dict,
              bases) -> None:
    """The viewer's steals, reaches and roster.

    Args:
        joined: From :func:`rundown.picks_with_board`.
        owner: Whose picks.
        starting_slots: This league's real starting slots.
        bases: The bases in play.
    """
    steals, reaches = rd.notable_picks(joined, owner)
    if steals.is_empty():
        return

    st.divider()
    st.subheader("Your Picks Against The Room")

    columns = ("player_name", "position", "round", "overall_pick", "adp", "value")
    labels = {"player_name": "Player", "position": "Pos", "round": "Rd",
              "overall_pick": "Pick", "adp": "ADP", "value": "Value"}
    config = {
        "Player": st.column_config.TextColumn(pinned=True),
        "ADP": st.column_config.NumberColumn(format="%.1f"),
        "Pick": st.column_config.NumberColumn(format="%.0f"),
        "Rd": st.column_config.NumberColumn(format="%.0f"),
        "Value": st.column_config.NumberColumn(
            format="%+.0f",
            help="Rank against ADP. Positive means the room let him fall past what "
                 "he is worth in this league."),
    }

    def _show(frame: pl.DataFrame, heading: str, note: str) -> None:
        st.markdown(f"**{heading}**")
        present = [c for c in columns if c in frame.columns]
        st.dataframe(frame.select(present).rename({c: labels[c] for c in present}),
                     width="stretch", hide_index=True, column_config=config,
                     placeholder="", lazy=False)
        st.caption(note)

    pair = st.columns(2)
    with pair[0]:
        _show(steals, "Fell To You",
              "Measured only on market-priced players — ESPN parks everyone it has "
              "no opinion about on one ADP plateau, and a steal against a plateau "
              "is an artefact of the plateau.")
    with pair[1]:
        _show(reaches, "You Paid Up For",
              "Not necessarily wrong. Reaching for a player the market is late on "
              "is what a board is *for*; this is the list to check that claim on.")

    with st.expander("Your Full Roster"):
        roster = rd.roster_frame(joined, owner, starting_slots, bases)
        keep = ["slot", "player_name", "position", "round", "overall_pick"]
        keep += [b.points for b in bases] + ["tier", "vor", "value", "bye_week"]
        present = [c for c in keep if c in roster.columns]
        renames = {"slot": "Slot", "player_name": "Player", "position": "Pos",
                   "round": "Rd", "overall_pick": "Pick", "tier": "Tier",
                   "vor": "VOR", "value": "Value", "bye_week": "Bye"}
        renames.update({b.points: b.label for b in bases})
        st.dataframe(
            roster.select(present).rename({c: renames[c] for c in present
                                           if c in renames}),
            width="stretch", hide_index=True, placeholder="", lazy=False,
            column_config={
                "Slot": st.column_config.TextColumn(
                    pinned=True,
                    help="The starting slot this player fills in your best legal "
                         "lineup on our numbers. `BE` means he does not make it."),
                "Player": st.column_config.TextColumn(pinned=True),
                **{b.label: st.column_config.NumberColumn(format="%.0f")
                   for b in bases},
                "VOR": st.column_config.NumberColumn(format="%+.0f"),
                "Value": st.column_config.NumberColumn(format="%+.0f"),
                "Tier": st.column_config.NumberColumn(format="%.0f"),
                "Bye": st.column_config.NumberColumn(format="%.0f"),
            },
        )
