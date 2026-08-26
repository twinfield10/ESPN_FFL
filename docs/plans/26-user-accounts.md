# 26 — User accounts: one app, one viewer's leagues

**Status:** IN PROGRESS

**Priority:** Medium · **Effort:** Medium · **Where it stands:** **Seam built 2026-08-14,
login not started**
**Depends on:** [07 (foundation)](07-frontend-foundation.md),
[24 (S3 as the system of record)](24-s3-data-flow.md)

## Problem

`config.yaml` holds nine leagues across five owners. The app offered all nine to
whoever opened it, because the only person who ever opened it was the person who
owns the laptop. That is fine right up until it is not:

- **Four of the nine are the viewer's.** The other five belong to other owners, who
  read their numbers off the Google Sheet ([plan 14](14-thin-google-sheets.md)).
  Offering them in the same picker is how the wrong board gets opened on draft
  night — the leagues are one alphabetical neighbour apart and the page looks
  identical.
- **Retrofitting identity later is a sweep, not a change.** Every page that reads
  `store.list_leagues()` directly is a page that has to be found and edited. Today
  that is one component; the plan-08 weekly views will add eight more.

## What is built

`app/auth.py`, and nothing else. It is a **seam**, not a login:

```python
class Viewer(NamedTuple):
    user_id: str
    display_name: str
    leagues: Tuple[str, ...]      # empty means unrestricted
    default_league: str

current_viewer() -> Viewer            # the one function a login replaces
sign_in(viewer) / sign_out()          # where a login callback hands its answer over
visible_leagues(viewer, keys) -> list # nine narrow to four, in exactly one place
default_league(viewer, keys) -> str   # where the app lands
```

`components/header.py` is the only caller. It asks who is looking, narrows the
league list, and lands on the viewer's default. Everything downstream reads
`Selection.league_key` and never learns that a filter happened.

The single account until login lands is the repo's owner, scoped to
**Winfield_Football** (the default), **Knights_FFL**, **GOP_Degenerates** and
**Weenieless_Wanderers**.

**`ESPN_FFL_ALL_LEAGUES=1` drops the scope.** Not a backdoor — see below on why
this is not a security boundary. It exists because when another owner's Sheet looks
wrong, the app is where you go to find out why, and scoping the picker must not
cost the ability to answer that.

### Design notes worth keeping

- **Empty `leagues` is the "unrestricted" sentinel**, rather than a separate flag or
  a role enum. One rule, and both the escape hatch and any future admin view get it
  for free.
- **The store's order wins, not the viewer's.** The store lists leagues sorted, and
  that order is stable across seasons; a viewer's preference order would move the
  picker under you as leagues get built.
- **A viewer with no built leagues for a season is not an error.** It is a season
  the refresh has not reached. The sidebar says so, names the viewer's leagues, and
  prints the commands — rather than falling through to "no store", which would be
  answering a question nobody asked while five other leagues sit in the store.
- **The viewer lives in `st.session_state`, not a module global.** Sessions are
  per-browser-tab; a global leaks one user's identity into another's session the
  moment this is served to more than one person, which is the entire premise of
  adding login.

## What this is not

**It is not a security boundary, and the module says so twice.** It scopes a local,
single-user Streamlit app whose data comes from a store the same laptop already has
full read access to. Filtering the league picker is a statement about what is worth
showing, not about what is reachable.

That distinction is the whole design constraint on the next step.

## What is left

1. **Pick the identity mechanism.** Streamlit's native `st.login` (OIDC) is the
   least code and the right first stop; the alternative is a reverse proxy in front
   of the app doing the auth and passing a header. Both end at
   `current_viewer()` returning a `Viewer` built from a token.
2. **Map an authenticated identity to leagues.** `config.yaml` already carries
   `primary_owner` per league, which is most of the mapping — but it is a display
   name, and ESPN spells people inconsistently across its own endpoints (see
   `draft_view._franchise_key`, which exists because of exactly that). The stable
   key is ESPN's `owner_id` GUID, which plan 25 established has been constant for
   every manager since 2019. A `viewers:` block in `config.yaml` keyed on email →
   league keys is the smaller move and does not need the GUID at all.
3. **Move enforcement to the store read.** The moment the app is served to more
   than the person who owns the laptop, `visible_leagues` in the sidebar stops
   being sufficient: a league key typed into a URL or left in session state must
   fail at `store.load_board`, not just be missing from a dropdown. `app/store.py`
   is the chokepoint — every artifact read already goes through `_artifact`.
4. **Decide what a shared league looks like.** Two owners in the same league is the
   obvious next case and the data model already supports it — `leagues` is a list,
   not an owner field.

Step 3 is the one that must not be skipped, and it is deliberately *not* built now:
implementing it while the only viewer is the laptop's owner would be enforcement
against nobody, with no way to tell whether it works.

## Effort

Small for step 1 if `st.login` fits (it is a config file and a callback). Medium
overall, dominated by step 3 and by whatever the identity provider turns out to
want.
