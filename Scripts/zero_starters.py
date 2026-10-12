"""Starters who scored zero or less -- the Knights FFL punishment log.

A league-side table rather than a projection one: it reads only ESPN's actuals
(``points``) and lineup slots off the stored lineup frame, so it is the same
whatever the blend said. Published as the ``Zero_Starters`` tab by
``populateGoogleSheet.py`` for the leagues in ``ZERO_STARTER_LEAGUES``.

``points`` is ``0.0`` both for a player who took the field and did nothing and for
one who has not kicked off yet, so a row only counts once ``game_state`` says the
game is over (``post``) or never happened (``bye`` -- starting a player on bye is
exactly the mistake this table exists to record).

The tab is half generated, half hand-kept: the league marks ``COMPLETE`` in the
Sheet once a punishment is served. A publish rewrites the tab, so it reads the
tab first and :func:`carry_marks` puts every hand-kept column back on its row.
"""

import pandas as pd

#: Config keys of the leagues whose Sheet carries the tab.
ZERO_STARTER_LEAGUES = {"knights_ffl"}

#: The punishment started after week 1; earlier offences are not logged.
PUNISHMENT_START_WEEK = 2

#: Owner name the store gives the free-agent market. Those rows carry the
#: player's position in ``slotPosition``, so a slot alone does not mean a start.
FREE_AGENT = "Free Agent"

#: Slots that are not starting slots.
NON_STARTING_SLOTS = {"BE", "IR"}

#: Game states in which a starter's ``points`` is final.
FINAL_STATES = {"post", "bye"}

#: Column order of the generated log. Hand-kept columns follow these.
LOG_COLUMNS = ["WK", "OWNER", "TEAM", "PLAYER", "SLOT", "PTS", "PROJ", "STATUS"]

#: Columns this module generated once and no longer does. They are still in a tab
#: published before they were dropped, and must not be mistaken for hand-kept.
RETIRED_COLUMNS = {"BEST_BENCH", "BENCH_PTS", "LOST"}

#: The hand-kept column the tab always carries, even before anyone has added it.
COMPLETE = "COMPLETE"

#: What identifies one offence across publishes.
ROW_KEY = ["WK", "OWNER", "PLAYER"]

#: Column order of the per-owner tally. ``OPEN`` counts rows not yet ``COMPLETE``.
TALLY_COLUMNS = ["OWNER", "COUNT", "OPEN", "PTS"]


def zero_point_starters(lineups: pd.DataFrame,
                        start_week: int = PUNISHMENT_START_WEEK) -> pd.DataFrame:
    """Every started player whose final score was zero or less.

    Args:
        lineups: The stored lineup frame for one league-season.
        start_week: First week that counts.

    Returns:
        pd.DataFrame: One row per offending start, newest week first and worst
        score first within a week, in ``LOG_COLUMNS`` order. Empty -- with the
        columns -- when there are none, or when the frame predates ``game_state``
        and no score can be confirmed final.
    """
    if "game_state" not in lineups:
        return pd.DataFrame(columns=LOG_COLUMNS)

    starts = lineups[(lineups["team_owner"] != FREE_AGENT)
                     & ~lineups["slotPosition"].isin(NON_STARTING_SLOTS)
                     & lineups["game_state"].isin(FINAL_STATES)
                     & (lineups["points"] <= 0)
                     & (lineups["week"] >= start_week)]
    if starts.empty:
        return pd.DataFrame(columns=LOG_COLUMNS)

    log = starts.rename(columns={
        "week": "WK", "team_owner": "OWNER", "team_name": "TEAM",
        "player_name": "PLAYER", "slotPosition": "SLOT", "points": "PTS",
        "TRUE_Points": "PROJ",
    })
    # ``inactive`` versus ``active`` is the difference between a player who sat and
    # one who played and did nothing, so it is worth a column; frames without the
    # status fall back to the game state, which at least separates a bye.
    status = "player_active_status" if "player_active_status" in log else "game_state"
    log["STATUS"] = log[status]
    log["PROJ"] = log["PROJ"].round(2)
    return (log.sort_values(["WK", "PTS"], ascending=[False, True])
               [LOG_COLUMNS].reset_index(drop=True))


def carry_marks(log: pd.DataFrame, previous: list | None,
                start_week: int = PUNISHMENT_START_WEEK):
    """Put the hand-kept columns from the published tab back onto a fresh log.

    Every column in the tab's header that the log does not generate is treated as
    hand-kept -- ``COMPLETE`` today, anything the league adds tomorrow -- and is
    matched back to its row on ``ROW_KEY``.

    Args:
        log: :func:`zero_point_starters` output.
        previous: The tab as ``get_all_values`` returns it (header row first), or
            ``None`` when the tab does not exist yet.
        start_week: Rows before this week are dropped on purpose, so are not
            reported as lost.

    Returns:
        tuple: ``(log, lost)``. ``log`` gains the hand-kept columns, ``COMPLETE``
        always among them, blank where nothing was marked. ``lost`` is the marked
        rows whose offence is no longer in the log -- a stat correction lifted
        the score above zero, say -- so the caller can say so rather than drop
        them silently.
    """
    log = log.copy()
    header = previous[0] if previous else []
    kept = [c for c in header
            if c and c not in LOG_COLUMNS and c not in RETIRED_COLUMNS]
    if COMPLETE not in kept:
        kept.append(COMPLETE)

    old = pd.DataFrame(previous[1:] if previous else [], columns=header or None)
    if old.empty or not set(ROW_KEY) <= set(old.columns):
        for col in kept:
            log[col] = ""
        return log, pd.DataFrame(columns=ROW_KEY + kept)

    old = old.loc[:, ~old.columns.duplicated()]
    for col in kept:
        if col not in old:
            old[col] = ""
    old["WK"] = pd.to_numeric(old["WK"], errors="coerce")
    old = (old.dropna(subset=["WK"]).astype({"WK": int})
              [ROW_KEY + kept].drop_duplicates(ROW_KEY))

    log["WK"] = log["WK"].astype(int)
    merged = log.merge(old, how="left", on=ROW_KEY)
    merged[kept] = merged[kept].fillna("")

    marked = old[(old[kept] != "").any(axis=1) & (old["WK"] >= start_week)]
    lost = (marked.merge(log[ROW_KEY], how="left", on=ROW_KEY, indicator=True)
                  .query("_merge == 'left_only'").drop(columns="_merge"))
    return merged, lost.reset_index(drop=True)


def owner_tally(lineups: pd.DataFrame, log: pd.DataFrame) -> pd.DataFrame:
    """Per-owner totals of the log, including owners with a clean record.

    Args:
        lineups: The stored lineup frame, for the full list of owners.
        log: :func:`zero_point_starters` output, after :func:`carry_marks` when
            ``OPEN`` should reflect what has been served.

    Returns:
        pd.DataFrame: ``TALLY_COLUMNS``; most open punishments first, then most
        offences.
    """
    owners = sorted(set(lineups["team_owner"]) - {FREE_AGENT})
    log = log.assign(_open=(log[COMPLETE].astype(str).str.strip() == "")
                     if COMPLETE in log else True)
    tally = (log.groupby("OWNER")
                .agg(COUNT=("PLAYER", "size"), OPEN=("_open", "sum"),
                     PTS=("PTS", "sum"))
                .reindex(owners, fill_value=0)
                .rename_axis("OWNER").reset_index())
    tally["OPEN"] = tally["OPEN"].astype(int)
    tally["PTS"] = tally["PTS"].astype(float).round(2)
    return (tally.sort_values(["OPEN", "COUNT"], ascending=False)
                 [TALLY_COLUMNS].reset_index(drop=True))


#: Row highlights on the log tab: an open punishment from the latest week is
#: yellow, an open one from any earlier week is red -- it is overdue.
CURRENT_OPEN_COLOR = {"red": 1.0, "green": 0.898, "blue": 0.6}     # #ffe599
OVERDUE_OPEN_COLOR = {"red": 0.918, "green": 0.6, "blue": 0.6}     # #ea9999


def _col_letter(index: int) -> str:
    """Zero-based column index to its A1 letter(s): 0 -> A, 26 -> AA."""
    letters = ""
    index += 1
    while index:
        index, rem = divmod(index - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def highlight_rules(sheet_id: int, columns: list) -> list:
    """Conditional-format requests that colour open punishments by age.

    Conditional rather than painted: the rules are formulas over the sheet, so a
    ``YES`` typed into ``COMPLETE`` clears its row's colour at once, without
    waiting for the next publish. "Latest week" is the highest ``WK`` on the tab.

    Args:
        sheet_id: The worksheet's ``sheetId``.
        columns: The tab's columns, in order; must include ``WK`` and ``COMPLETE``.

    Returns:
        list: ``addConditionalFormatRule`` requests for ``batch_update``. Clear the
        tab's existing rules first, or they pile up one set per publish.
    """
    wk = _col_letter(columns.index("WK"))
    done = _col_letter(columns.index(COMPLETE))
    # Data rows only, and never an empty row: MAX ignores blanks, so a blank WK
    # would otherwise read as an old week and paint the empty grid red.
    open_row = f'${wk}2<>"", ${done}2<>"YES"'
    latest = f"MAX(${wk}$2:${wk})"

    def rule(index, formula, color):
        return {"addConditionalFormatRule": {"index": index, "rule": {
            "ranges": [{"sheetId": sheet_id, "startRowIndex": 1,
                        "startColumnIndex": 0, "endColumnIndex": len(columns)}],
            "booleanRule": {
                "condition": {"type": "CUSTOM_FORMULA",
                              "values": [{"userEnteredValue": formula}]},
                "format": {"backgroundColor": color},
            },
        }}}

    return [
        rule(0, f"=AND({open_row}, ${wk}2={latest})", CURRENT_OPEN_COLOR),
        rule(1, f"=AND({open_row}, ${wk}2<{latest})", OVERDUE_OPEN_COLOR),
    ]
