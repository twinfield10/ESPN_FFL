import copy
import datetime
import functools
import os
import re
import requests
import warnings
from typing import Optional

import numpy as np
import pandas as pd
import datetime
from typing import Optional
from espn_api.football import League
from espn_api.requests.constant import FANTASY_BASE_ENDPOINT

warnings.filterwarnings("ignore")


def set_league_endpoint(league: League) -> None:
    """Set the league's endpoint."""

    # "This" year is considered anything after June
    now = datetime.datetime.today()
    if now.month > 6:
        current_year = now.year
    else:
        current_year = now.year - 1

    # Current season
    if league.year >= current_year:
        league.endpoint = f"{FANTASY_BASE_ENDPOINT}ffl/seasons/{league.year}/segments/0/leagues/{league.league_id}?"

    # Old season
    else:
        league.endpoint = f"{FANTASY_BASE_ENDPOINT}ffl/leagueHistory/{league.league_id}?seasonId={league.year}&"


def get_roster_settings(league: League) -> None:
    """This grabs the roster and starting lineup settings for the league
    - Grabs the dictionary containing the number of players of each position a roster contains
    - Creates a dictionary roster_slots{} that only inlcludes slotIds that have a non-zero number of players on the roster
    - Creates a dictionary starting_roster_slots{} that is a subset of roster_slots{} and only includes slotIds that are on the starting roster
    - Add roster_slots{} and starting_roster_slots{} to the League attribute League.rosterSettings
    """

    # This dictionary maps each slotId to the position it represents
    rosterMap = {
        0: "QB",
        1: "TQB",
        2: "RB",
        3: "RB/WR",
        4: "WR",
        5: "WR/TE",
        6: "TE",
        7: "OP",
        8: "DT",
        9: "DE",
        10: "LB",
        11: "DL",
        12: "CB",
        13: "S",
        14: "DB",
        15: "DP",
        16: "D/ST",
        17: "K",
        18: "P",
        19: "HC",
        20: "BE",
        21: "IR",
        22: "",
        23: "RB/WR/TE",
        24: " ",
    }

    endpoint = "{}view=mMatchupScore&view=mTeam&view=mSettings".format(league.endpoint)
    r = requests.get(endpoint, cookies=league.cookies).json()
    if type(r) == list:
        r = r[0]
    settings = r["settings"]
    league.name = settings["name"]

    # Grab the dictionary containing the number of players of each position a roster contains
    roster = settings["rosterSettings"]["lineupSlotCounts"]
    # Create an empty dictionary that will replace roster{}
    roster_slots = {}
    # Create an empty dictionary that will be a subset of roster_slots{} containing only starting players
    starting_roster_slots = {}
    for positionId in roster:
        position = rosterMap[int(positionId)]
        # Only inlclude slotIds that have a non-zero number of players on the roster
        if roster[positionId] != 0:
            roster_slots[position] = roster[positionId]
            # Include all slotIds in the starting_roster_slots{} unless they are bench, injured reserve, or ' '
            if positionId not in ["20", "21", "24"]:
                starting_roster_slots[position] = roster[positionId]
    # Add roster_slots{} and starting_roster_slots{} as a league attribute
    league.roster_settings = {
        "roster_slots": roster_slots,
        "starting_roster_slots": starting_roster_slots,
    }
    return


def set_owner_names(league: League) -> None:
    """This function sets the owner names for each team in the league.
    The team.owners attribute contains a dictionary of information with owner details, not a simple name.

    Args:
        league (League): ESPN League object
    """
    # Set the owner name for each team
    for team in league.teams:
        if team.owners and all(
            [key in team.owners[0].keys() for key in ["firstName", "lastName"]]
        ):
            team.owner = re.sub(
                " +",
                " ",
                team.owners[0]["firstName"] + " " + team.owners[0]["lastName"],
            ).title()
        else:
            team.owner = "Unknown Owner"


def isolate_scoring_format(league: League) -> None:
    """Detach the league's scoring format from ``espn_api``'s shared dicts.

    ``espn_api.football.Settings.__init__`` (0.45.1) builds each scoring row with
    ``SETTINGS_SCORING_FORMAT_MAP.get(stat_id, ...)`` and then writes ``id`` and
    ``points`` onto the returned dict -- which is the *module-level* dict, not a
    copy. Every League in the process therefore shares one set of scoring rows,
    and each new fetch retroactively overwrites the points of every league
    fetched before it.

    Measured on this repo's config: fetching Weenieless_Wanderers after
    GOP_Degenerates silently changed GOP's passing TD from 6.0 to 4.0, plus six
    more rows. Any code holding two League objects at once -- backfills, the
    all-league loops in populateGoogleSheet.py, the coverage test -- was scoring
    with another league's rules.

    Deep-copying at fetch time pins this league's values before another fetch can
    clobber them.

    Args:
        league (League): ESPN League object
    """
    league.settings.scoring_format = copy.deepcopy(league.settings.scoring_format)


def set_additional_settings(league: League) -> None:
    """This function adds additional league settings to the League object.

    Args:
        league (League): ESPN League object
    """
    # Create a dictionary that maps each week to the matchup period it is in
    # This is necessary because some matchup periods span multiple weeks
    league.settings.week_to_matchup_period = {}
    for matchup_period, weeks in league.settings.matchup_periods.items():
        for week in weeks:
            league.settings.week_to_matchup_period[week] = int(matchup_period)


def fetch_league(
    league_id: int, year: int, swid: Optional[str] = None, espn_s2: Optional[str] = None
) -> League:
    """Instantiate a League and attach the extra details the pipeline expects.

    Adds ``league.cookies``, ``league.endpoint``, ``league.roster_settings``
    (roster_slots / starting_roster_slots), owner names, a memoised
    ``box_scores``, and loads the roster for the current week. Also isolates the
    scoring format -- see :func:`isolate_scoring_format`.

    Args:
        league_id: ESPN league id.
        year: Season year.
        swid: ESPN SWID cookie.
        espn_s2: ESPN espn_s2 cookie.

    Returns:
        League: The configured league.

    Raises:
        Exception: Propagates whatever ``espn_api`` raises -- typically
            ``ESPNAccessDenied`` (bad or expired cookies),
            ``ESPNInvalidLeague`` (wrong id, or the season does not exist yet),
            or a ``requests`` error.

    Note:
        This previously wrapped everything in a bare ``except:`` that fell back
        to constructing a 2024 league, printed "Not Accessable", and then
        returned a possibly-unbound local -- raising ``UnboundLocalError`` and
        hiding the real cause. Failures now surface directly.
    """

    league = League(league_id=league_id, year=year, swid=swid, espn_s2=espn_s2)
    # Pin the scoring rules before any later fetch can mutate them
    isolate_scoring_format(league)
    # Set cookies
    league.cookies = {"swid": swid, "espn_s2": espn_s2}
    # Set league endpoint
    set_league_endpoint(league)
    # Get roster information
    get_roster_settings(league)
    # Set additinoal settings
    set_additional_settings(league)
    # Set the owners for each team
    set_owner_names(league)
    # Cache this function to speed up processing
    league.box_scores = functools.cache(league.box_scores)
    # Load current league data
    print(f"BUILDING {year} Season For {league.name} ")

    # Pre-draft, ESPN reports current_week as 0; clamp so the matchup-period
    # lookup below resolves.
    league.current_week = max(league.current_week, 1)
    current_matchup_period = league.settings.week_to_matchup_period[league.current_week]
    league.load_roster_week(current_matchup_period)

    return league

