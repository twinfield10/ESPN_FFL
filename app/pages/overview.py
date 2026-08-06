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

selection = render_sidebar()
meta = selection.meta

st.title(f"{selection.display_name} · {selection.season}")
st.caption(
    "Foundation only — this page exists to verify the store. Weekly views are "
    "plan 08, draft views plan 09."
)

lineups = store.load_lineups(selection.season, selection.league_key)

# --- what's in the store -------------------------------------------------

cols = st.columns(4)
cols[0].metric("Rows", f"{lineups.height:,}")
cols[1].metric("Columns", f"{lineups.width:,}")
cols[2].metric("Current week", meta.get("current_week", "?"))
cols[3].metric("Weeks stored", len(meta.get("weeks_present") or []))

if not meta.get("weeks_present"):
    st.info(
        "No weeks in the store yet. Pre-draft, ESPN reports `current_week` as 0 "
        "and the pipeline clamps it to 1, so a store built now holds week 1 "
        "placeholder rosters."
    )

# --- this week's blended projections ------------------------------------

st.subheader(f"Week {selection.week} — blended projections")

point_cols = [c for c in ("projPoints", "FP_Points", "PINNY_Points", "BOL_Points",
                         "TRUE_Points") if c in lineups.columns]
show_cols = [c for c in ("player_name", "primaryPosition", "slotPosition",
                         "team_owner", "pro_team") if c in lineups.columns] + point_cols

week_rows = lineups.filter(pl.col("week") == selection.week)
if week_rows.is_empty():
    st.warning(f"No rows for week {selection.week} in this store.")
else:
    sort_col = "TRUE_Points" if "TRUE_Points" in week_rows.columns else point_cols[0]
    st.dataframe(
        week_rows.select(show_cols).sort(sort_col, descending=True),
        width="stretch", hide_index=True, height=420,
    )

# --- provenance ----------------------------------------------------------

with st.expander("Store metadata"):
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
        st.caption("Roster slots")
        st.write(meta.get("roster_slots") or "not recorded")
        st.caption("Weekly source files present")
        st.write(meta.get("weekly_sources_present") or "not recorded")

    key_stats = (meta.get("coverage") or {}).get("key_stats") or {}
    if key_stats:
        st.caption("Coverage by key stat (% real, not imputed)")
        st.dataframe(
            pl.DataFrame([{"stat": stat, **pcts} for stat, pcts in key_stats.items()]),
            width="stretch", hide_index=True,
        )

if not store.has_artifact(selection.season, selection.league_key, "team_stats"):
    st.caption(
        "`team_stats` is not in this store. It re-derives a league's whole "
        "history, so it is opt-in: "
        "`python -m Scripts.refresh --league "
        f"{selection.display_name} --what team_stats`"
    )
