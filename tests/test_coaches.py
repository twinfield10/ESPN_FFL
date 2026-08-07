"""The coaching-staff table: two sources, and the parsing that nearly ate it.

Every parsing case pinned here is one that actually failed against a live 2026
article. Wikipedia infoboxes are hand-written, so "it worked on Arizona" is not
coverage -- Philadelphia puts two fields on one line, and a name can carry a
disambiguating pipe that is also the field separator.

No network: the fetch layer is stubbed.
"""

import polars as pl
import pytest

from Scripts import coaches as co


# --- parsing -------------------------------------------------------------

def test_a_plain_infobox_parses():
    text = ("{{Infobox NFL season\n"
            "| coach = [[Mike LaFleur]]\n"
            "| off_coach = [[Nathaniel Hackett]]\n"
            "| def_coach = [[Nick Rallis]]\n}}")
    out = co.parse_infobox(text)
    assert out["head_coach"] == "Mike LaFleur"
    assert out["offensive_coordinator"] == "Nathaniel Hackett"
    assert out["defensive_coordinator"] == "Nick Rallis"


def test_two_fields_on_one_line_both_parse():
    """Philadelphia's 2026 article. A line-anchored pattern found off_coach and
    swallowed def_coach into its value, then never found def_coach at all --
    producing 'Sean Mannion|def_coach=Vic Fangio}}' as the coordinator."""
    text = ("| coach           = [[Nick Sirianni]]\n"
            "|off_coach=[[Sean Mannion (American football)|Sean Mannion]]"
            "|def_coach=[[Vic Fangio]]}}")
    out = co.parse_infobox(text)
    assert out["head_coach"] == "Nick Sirianni"
    assert out["offensive_coordinator"] == "Sean Mannion"
    assert out["defensive_coordinator"] == "Vic Fangio"


def test_a_piped_wikilink_is_not_mistaken_for_a_field_separator():
    """`[[Sean Mannion (American football)|Sean Mannion]]` contains the same pipe
    the infobox uses to separate fields, so links have to be resolved first."""
    assert co.resolve_links("[[A (disambig)|A]]") == "A"
    assert "|" not in co.resolve_links("|x=[[A (d)|A]]|y=[[B]]").split("=", 1)[1]\
        .split("|")[0]


def test_a_missing_field_is_none_not_an_error():
    """The 2025 Arizona article carries no off_coach at all."""
    out = co.parse_infobox("| coach = [[Jonathan Gannon]]")
    assert out["head_coach"] == "Jonathan Gannon"
    assert out["offensive_coordinator"] is None


def test_an_interim_appointment_keeps_the_name_not_the_parenthetical():
    out = co.parse_infobox("| coach = [[Some Body]] (interim)")
    assert out["head_coach"] == "Some Body"


def test_two_names_in_one_field_keeps_the_first():
    out = co.parse_infobox("| off_coach = [[First Guy]] and [[Second Guy]]")
    assert out["offensive_coordinator"] == "First Guy"


def test_a_reference_is_stripped():
    out = co.parse_infobox("| coach = [[A B]]<ref>{{cite web|url=x}}</ref>")
    assert out["head_coach"] == "A B"


def test_empty_wikitext_yields_all_none():
    out = co.parse_infobox("")
    assert set(out.values()) == {None}


def test_the_lead_section_stops_at_the_first_heading():
    """The batched API returns whole articles, and a season article mentions coaches
    in many places. The infobox is in the lead."""
    text = "| coach = [[Real Coach]]\n\n==Coaching staff==\n| coach = [[Wrong]]"
    assert co.parse_infobox(co.lead_section(text))["head_coach"] == "Real Coach"


# --- the nflverse side ---------------------------------------------------

def games(rows):
    """A coaches_by_game frame from ``(season, team, week, coach)``."""
    return pl.DataFrame({
        "season": [r[0] for r in rows],
        "team": [r[1] for r in rows],
        "week": [r[2] for r in rows],
        "coach": [r[3] for r in rows],
        "game_id": [f"{r[0]}_{r[2]}_{r[1]}" for r in rows],
        "game_type": ["REG"] * len(rows),
        "home": [True] * len(rows),
    })


def test_the_modal_coach_wins_a_mid_season_change():
    """28 team-seasons since 2010 had more than one head coach, so this is not an
    edge case."""
    rows = ([(2023, "SEA", w, "First Guy") for w in range(1, 5)]
            + [(2023, "SEA", w, "Second Guy") for w in range(5, 18)])
    out = co.from_nflverse(games(rows))
    row = out.row(0, named=True)
    assert row["head_coach"] == "Second Guy"
    assert row["games_coached"] == 13
    assert row["team_games"] == 17
    assert row["coach_changed_midseason"] is True


def test_a_single_coach_season_is_not_flagged_as_changed():
    rows = [(2023, "SEA", w, "Only Guy") for w in range(1, 18)]
    out = co.from_nflverse(games(rows))
    assert out["coach_changed_midseason"][0] is False
    assert out["games_coached"][0] == out["team_games"][0]


def test_playoff_games_do_not_count_toward_the_season():
    rows = [(2023, "SEA", w, "Guy") for w in range(1, 18)]
    frame = games(rows)
    post = games([(2023, "SEA", 19, "Guy")]).with_columns(
        pl.lit("WC").alias("game_type"))
    out = co.from_nflverse(pl.concat([frame, post]))
    assert out["team_games"][0] == 17


def test_the_nflverse_side_leaves_coordinators_null():
    """nflverse has no coordinator data at all; the columns exist so the two sources
    concatenate."""
    out = co.from_nflverse(games([(2023, "SEA", 1, "Guy")]))
    assert out["offensive_coordinator"][0] is None
    assert out["source"][0] == "nflverse"


# --- combining the two ---------------------------------------------------

@pytest.fixture
def sources(tmp_path, monkeypatch):
    """Point the module at synthetic inputs."""
    rows = ([(2025, t, w, f"Old {t}") for t in ("AAA", "BBB") for w in range(1, 18)]
            + [(2026, t, w, f"Stale {t}") for t in ("AAA", "BBB")
               for w in range(1, 18)])
    by_game = tmp_path / "coaches_by_game.parquet"
    games(rows).write_parquet(by_game)
    names = tmp_path / "team_names.parquet"
    pl.DataFrame({"team_abbr": ["AAA", "BBB"],
                  "team_name": ["Aaa Ants", "Bbb Bees"],
                  "team_nick": ["Ants", "Bees"]}).write_parquet(names)
    monkeypatch.setattr(co, "COACHES_BY_GAME_PARQUET", by_game)
    monkeypatch.setattr(co, "TEAM_NAMES_PARQUET", names)
    return tmp_path


def test_wikipedia_wins_for_the_current_season(sources, monkeypatch):
    """nflverse is partially-updated-and-looks-complete for an unplayed season: it
    recorded seven of 2026's coaching changes and missed Arizona's, still listing
    Jonathan Gannon where the answer is Mike LaFleur."""
    monkeypatch.setattr(co, "fetch_many", lambda titles, verbose=True: {
        "2026_Aaa_Ants_season": "| coach = [[Real AAA]]",
        "2026_Bbb_Bees_season": "| coach = [[Real BBB]]",
    })
    out = co.build(current_season=2026, verbose=False)
    current = out.filter(pl.col("season") == 2026).sort("team")
    assert current["head_coach"].to_list() == ["Real AAA", "Real BBB"]
    assert current["source"].to_list() == ["wikipedia", "wikipedia"]
    # History is untouched.
    past = out.filter(pl.col("season") == 2025)
    assert past["source"].unique().to_list() == ["nflverse"]


def test_a_team_wikipedia_cannot_resolve_keeps_nflverse_and_says_so(sources,
                                                                   monkeypatch):
    """A stale coach has to be identifiable rather than invisible."""
    monkeypatch.setattr(co, "fetch_many", lambda titles, verbose=True: {
        "2026_Aaa_Ants_season": "| coach = [[Real AAA]]",
        "2026_Bbb_Bees_season": None,          # article does not exist yet
    })
    out = co.build(current_season=2026, verbose=False)
    current = out.filter(pl.col("season") == 2026).sort("team")
    by_team = dict(zip(current["team"], current["source"]))
    assert by_team["AAA"] == "wikipedia"
    assert by_team["BBB"] == "nflverse"
    assert current.filter(pl.col("team") == "BBB")["head_coach"][0] == "Stale BBB"


def test_offline_never_touches_the_network(sources, monkeypatch):
    def explode(*a, **k):
        raise AssertionError("offline must not fetch")
    monkeypatch.setattr(co, "fetch_many", explode)
    out = co.build(current_season=2026, offline=True, verbose=False)
    assert out["source"].unique().to_list() == ["nflverse"]


def test_a_missing_input_names_the_command_that_fixes_it(tmp_path, monkeypatch):
    monkeypatch.setattr(co, "COACHES_BY_GAME_PARQUET", tmp_path / "nope.parquet")
    with pytest.raises(FileNotFoundError, match="Rscript R/GetCoaches.R"):
        co.build(current_season=2026, verbose=False)


def test_article_titles_use_the_full_team_name(sources):
    names = pl.read_parquet(sources / "team_names.parquet")
    titles = co.team_titles(2026, names, ["AAA", "BBB"])
    assert titles["AAA"] == "2026_Aaa_Ants_season"


# --- the committed artifact ---------------------------------------------

def test_the_built_table_has_no_near_duplicate_coach_names():
    """nflverse spells the 2026 Raiders coach 'Klint Kubliak' against Wikipedia's
    'Klint Kubiak'. A typo in what will be a groupby key silently splits one coach
    into two, halving both his sample sizes."""
    import difflib
    path = co.COACHING_STAFF_PARQUET
    if not path.is_file():
        pytest.skip("coaching_staff.parquet not built")
    names = sorted(pl.read_parquet(path)["head_coach"].drop_nulls().unique()
                   .to_list())
    close = [(a, b) for i, a in enumerate(names) for b in names[i + 1:]
             if difflib.SequenceMatcher(None, a, b).ratio() > 0.9]
    assert close == []
