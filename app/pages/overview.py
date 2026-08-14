"""Store overview — the placeholder page that ships with the foundation.

Its job is to prove the boundary works: what is in the store, when it was built,
how much of each projection source is real, and what the blend produced. The
real views land in ``docs/plans/08-frontend-weekly-views.md`` and
``docs/plans/09-frontend-draft-views.md``.

Every number here comes from parquet. Nothing on this page talks to ESPN.
"""

import _bootstrap  # noqa: F401  -- must precede the Scripts imports

import polars as pl
import streamlit as st

import store
from components.header import render_sidebar

#: Store column to the label the table shows it under.
#:
#: The stored names are the schema and the page exists to verify it, so the mapping
#: is spelled out here rather than title-cased by a rule -- ``projPoints`` is ESPN's
#: projection and calling it "Proj Points" would name it after its column instead of
#: after what it is. The caption under the table names the source columns.
DISPLAY_LABELS = {
    "player_name": "Player",
    "primaryPosition": "Pos",
    "slotPosition": "Slot",
    "team_owner": "Owner",
    "pro_team": "NFL",
    "projPoints": "ESPN",
    "FP_Points": "FantasyPros",
    "PINNY_Points": "Pinnacle",
    "BOL_Points": "BetOnline",
    "TRUE_Points": "Blended",
    "points": "Scored",
}

selection = render_sidebar()
meta = selection.meta

st.title(f"{selection.display_name} · {selection.season}")
st.caption(
    "Foundation only — this page exists to verify the store. Weekly views are "
    "plan 08, draft views plan 09."
)

# `lineups` where there is one, `results` otherwise -- and the page says which.
#
# Not defensiveness: plan 25 backfilled Winfield_Football to 2019 and **cannot** build
# `lineups` for a past season, because it carries FantasyPros columns and FantasyPros
# serves no season parameter. So every backfilled season holds `results.parquet` and
# `meta.json` and nothing else, and this page raised `FileNotFoundError` out of the
# render the moment you picked one from the season selector it draws itself.
if store.has_artifact(selection.season, selection.league_key, "lineups"):
    frame = store.load_lineups(selection.season, selection.league_key)
    artifact = "lineups"
elif store.has_artifact(selection.season, selection.league_key, "results"):
    frame = store.load_results(selection.season, selection.league_key)
    artifact = "results"
else:
    st.warning(
        f"This store holds no player rows for {selection.season} — neither "
        f"`lineups` nor `results`. The metadata below is all there is."
    )
    st.code(
        f"python -m Scripts.refresh --league {selection.display_name} "
        f"--season {selection.season}",
        language="bash",
    )
    frame, artifact = pl.DataFrame(), None

# --- what's in the store -------------------------------------------------

cols = st.columns(4)
cols[0].metric("Rows", f"{frame.height:,}")
cols[1].metric("Columns", f"{frame.width:,}")
cols[2].metric("Current Week", meta.get("current_week", "?"))
cols[3].metric("Weeks Stored", len(meta.get("weeks_present") or []))

if artifact == "results":
    st.info(
        f"Reading `results.parquet`: what was actually scored in {selection.season}, "
        "with no projections in it. A finished season has no `lineups` and never "
        "will — that artifact carries FantasyPros columns and FantasyPros serves no "
        "season parameter."
    )
elif artifact and not meta.get("weeks_present"):
    st.info(
        "No weeks in the store yet. Pre-draft, ESPN reports `current_week` as 0 "
        "and the pipeline clamps it to 1, so a store built now holds week 1 "
        "placeholder rosters."
    )

# --- this week's rows ----------------------------------------------------

if artifact:
    st.subheader(
        f"Week {selection.week} — "
        + ("Blended Projections" if artifact == "lineups" else "What Was Scored")
    )

    point_cols = [c for c in ("projPoints", "FP_Points", "PINNY_Points",
                              "BOL_Points", "TRUE_Points", "points")
                  if c in frame.columns]
    show_cols = [c for c in ("player_name", "primaryPosition", "slotPosition",
                             "team_owner", "pro_team")
                 if c in frame.columns] + point_cols

    week_rows = frame.filter(pl.col("week") == selection.week)
    if week_rows.is_empty():
        st.warning(f"No rows for week {selection.week} in this store.")
    else:
        sort_col = ("TRUE_Points" if "TRUE_Points" in week_rows.columns
                    else point_cols[0])
        st.dataframe(
            week_rows.select([pl.col(c).alias(DISPLAY_LABELS.get(c, c))
                              for c in show_cols])
            .sort(DISPLAY_LABELS.get(sort_col, sort_col), descending=True),
            width="stretch", hide_index=True, height=420,
        )
        st.caption(
            f"From `{artifact}.parquet`. Source columns, left to right: "
            + " · ".join(f"`{c}`" for c in show_cols)
        )

# --- provenance ----------------------------------------------------------

with st.expander("Store Metadata"):
    left, right = st.columns(2)

    with left:
        st.caption("Build")
        st.write({
            "built_at": meta.get("built_at"),
            "schema_version": meta.get("schema_version"),
            "artifacts": meta.get("artifacts"),
            "league_id": meta.get("league_id"),
            "primary_owner": meta.get("primary_owner"),
        })
        st.caption("Versions")
        st.write(meta.get("versions"))

    with right:
        st.caption("Roster Slots")
        st.write(meta.get("roster_slots") or "not recorded")
        st.caption("Weekly Source Files Present")
        st.write(meta.get("weekly_sources_present") or "not recorded")

    key_stats = (meta.get("coverage") or {}).get("key_stats") or {}
    if key_stats:
        st.caption("Coverage by Key Stat (% Real, Not Imputed)")
        st.dataframe(
            pl.DataFrame([{"Stat": stat, **pcts} for stat, pcts in key_stats.items()]),
            width="stretch", hide_index=True,
        )

if not store.has_artifact(selection.season, selection.league_key, "team_stats"):
    st.caption(
        "`team_stats` is not in this store. It re-derives a league's whole "
        "history, so it is opt-in: "
        "`python -m Scripts.refresh --league "
        f"{selection.display_name} --what team_stats`"
    )
