"""Publish each league's weekly tables to its Google Sheet.

This is a **renderer over the store**, not a pipeline. It reads
``Data/Store/<season>/<league_key>/`` and talks to Google; it does not talk to
ESPN. Run ``python -m Scripts.refresh --all`` first.

That split matters because ``run()`` used to hold a line-for-line copy of the
ingest sequence in ``Scripts.equivalence.build_league_frame`` -- fetch, lineups by
matchup, free-agent market, concat/fillna/dedupe, blend. Two copies of the same
sequence is the shape that already cost this repo once: 12 projection functions
existed here and in the notebook, and 8 had drifted, so the notebook used to
*decide* a lineup and this script computed different numbers. Reading the store
means there is one ingest path and Sheets cannot disagree with the app.

The Sheet is deliberately kept alongside the app rather than retired: it is a
*published artifact*, readable from a phone away from home with the laptop shut,
which a locally-served app structurally cannot be. See
``docs/plans/14-thin-google-sheets.md``.
"""

# Base
import time

# Data Manipulation
import numpy as np

# Google Sheet
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

# Config Leagues
from Scripts.config_utils import build_lg_vars, get_season, load_config, resolve_league

config = load_config()
lg_vars = build_lg_vars(config)
SEASON = get_season(config)

# The store: this script's only data source.
from Scripts.store import read_league_store, read_meta, store_age_minutes

# The view builders. The blend itself ran during `Scripts.refresh`, so the
# projection primitives this file used to import are no longer needed here --
# only the three functions that turn a stored frame into a table.
from Scripts.projection_utils import (
    check_week,
    get_league_projections,
    get_rankings,
)


def write_to_google(df_dict, league_name, primary_owner):
    """Publish one league's tables to its Google Sheet.

    Args:
        df_dict: Sheet name to frame. Keys drive which formatter runs.
        league_name: Display name; must match the spreadsheet name exactly.
        primary_owner: The owner whose players are kept alongside free agents on
            the ``FA_*`` tabs.

            This used to be read as ``lg_vars[select_league]['primary_own']``,
            where ``select_league`` was a module-level global assigned by a
            top-level loop. Commit 304ba39 moved that loop into ``run()``, making
            it a local -- so every ``FA_*`` tab raised ``NameError``, which the
            bare ``except`` below reported as "Position Does Not Exist in
            League". Introduced 2026-08-05 and caught the same day, so no
            published Sheet was affected -- but nothing would have reported it.
    """
    # Set up authentication
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    # Option 1: Using service account (recommended for automation)
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        'gs4creds.json',
        scope
    )

    # Get Min and Max
    points_cols = ['projPoints', 'FP_Points', 'BOL_Points', 'PINNY_Points', 'TRUE_Points']
    scale_dict = {
        "MAX": {
            "TEAM": df_dict["League_Projections"][points_cols].max().max(),
            "QBs": df_dict["FA_QBs"][points_cols].max().max(),
            "RBs": df_dict["FA_RBs"][points_cols].max().max(),
            "WRs": df_dict["FA_WRs"][points_cols].max().max(),
            "TEs": df_dict["FA_TEs"][points_cols].max().max(),
            "FLX": df_dict["FA_FLX"][points_cols].max().max(),
            "DST": df_dict["FA_DST"][points_cols].max().max(),
            "KCK": df_dict["FA_KCK"][points_cols].max().max(),
            "IDP": df_dict["FA_IDP"][points_cols].max().max(),
            "Bench": 50
        },
        "MEDIAN": {
            "TEAM": df_dict["League_Projections"][points_cols].replace(0, np.nan).median().median(),
            "QBs": df_dict["FA_QBs"][df_dict["FA_QBs"]["team_owner"] != "Free Agent"][points_cols].replace(0, np.nan).median().median(), #
            "RBs": df_dict["FA_RBs"][df_dict["FA_RBs"]["team_owner"] != "Free Agent"][points_cols].replace(0, np.nan).median().median(), #
            "WRs": df_dict["FA_WRs"][df_dict["FA_WRs"]["team_owner"] != "Free Agent"][points_cols].replace(0, np.nan).median().median(), #
            "TEs": df_dict["FA_TEs"][df_dict["FA_TEs"]["team_owner"] != "Free Agent"][points_cols].replace(0, np.nan).median().median(), #
            "FLX": df_dict["FA_FLX"][df_dict["FA_FLX"]["team_owner"] != "Free Agent"][points_cols].replace(0, np.nan).median().median(), #
            "DST": df_dict["FA_DST"][points_cols].replace(0, np.nan).median().median(), #
            "KCK": df_dict["FA_KCK"][points_cols].replace(0, np.nan).median().median(),
            "IDP": df_dict["FA_IDP"][points_cols].replace(0, np.nan).median().median(),
            "Bench": 25
        },
        "MIN": {
            "TEAM": df_dict["League_Projections"][points_cols].min().min(),
            "QBs": 0,
            "RBs": 0,
            "WRs": 0,
            "TEs": 0,
            "FLX": 0,
            "DST": 0,
            "KCK": 0,
            "IDP": 0,
            "Bench": 0
        }
    }

    # Authorize and connect
    client = gspread.authorize(creds)

    # Open or create a spreadsheet
    spreadsheet = client.open(league_name)

    for sheet_name, df in df_dict.items():
        try:
            # Try to get existing worksheet
            worksheet = spreadsheet.worksheet(sheet_name)
            # Clear existing content
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            # Create new worksheet if it doesn't exist
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name, 
                rows=str(len(df) + 1), 
                cols=str(len(df.columns))
            )

        # Formatting Functions

        # Clear Formatting
        def clear_sheet_formatting(worksheet, data_df):
            """
            Clear all conditional formatting, text formatting, and borders from the worksheet.

            Args:
                worksheet: gspread worksheet object
                data_df: pandas DataFrame (to determine number of rows)
            """
            num_rows = len(data_df) + 1
            num_cols = len(data_df.columns)

            requests = []

            # 1. Clear all conditional format rules
            try:
                sheet_metadata = worksheet.spreadsheet.fetch_sheet_metadata()
                for sheet in sheet_metadata['sheets']:
                    if sheet['properties']['sheetId'] == worksheet.id:
                        rules_count = len(sheet.get('conditionalFormats', []))
                        # Delete all conditional format rules
                        for _ in range(rules_count):
                            requests.append({
                                "deleteConditionalFormatRule": {
                                    "sheetId": worksheet.id,
                                    "index": 0
                                }
                            })
                        break
            except Exception as e:
                print(f"Note clearing conditional formatting: {e}")

            # 2. Clear text formatting, borders, and background colors
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": num_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {},
                            "borders": {},
                            "backgroundColor": {"red": 1, "green": 1, "blue": 1}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat,userEnteredFormat.borders,userEnteredFormat.backgroundColor"
                }
            })

            # Execute all requests
            #if requests:
            #    worksheet.spreadsheet.batch_update({"requests": requests})

            return requests
        
        # Format Lineup Sheet
        def format_lineup_rows(worksheet, data_df, target_values=['Starting Lineup', 'Bench']):
            """
            Apply bold formatting and top/bottom borders to rows where a specific column
            contains one of the target values. Also applies gradient color formatting 
            (green to white) to columns F:J for rows NOT matching target values.

            Args:
                worksheet: gspread worksheet object
                data_df: pandas DataFrame (to determine number of rows)
                column_letter: Column to check (default 'D')
                target_values: List of values to match (default ['Starting Lineup', 'Bench'])
            """
            requests = []

            # Clear Formatting
            clear_requests = clear_sheet_formatting(worksheet, data_df)
            requests.append(clear_requests)

            # Get DF Stats + Indicies
            num_rows = len(data_df) + 1
            rospos_col_num = ord('D'.upper()) - ord('A') + 1
            primpos_col_num = ord('E'.upper()) - ord('A') + 1

            # 1) Format header row - bold, white text, navy blue background
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(data_df.columns)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {
                                "bold": True,
                                "foregroundColor": {"red": 1, "green": 1, "blue": 1}
                            },
                            "backgroundColor": {"red": 0, "green": 0, "blue": 0.5}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"
                }
            })

            # Format columns F:J to 2 decimal places
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,  # Row 2 (0-indexed)
                        "endRowIndex": num_rows,
                        "startColumnIndex": 5,  # Column F (0-indexed)
                        "endColumnIndex": 10  # Column J (0-indexed, exclusive)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "0.00"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

            # Find row numbers for Starting Lineup and Bench
            starting_lineup_row = None
            bench_row = None

            starting_lineup_cells = worksheet.findall('Starting Lineup', in_column=rospos_col_num)
            if starting_lineup_cells:
                starting_lineup_row = starting_lineup_cells[0].row

            bench_cells = worksheet.findall('Bench', in_column=rospos_col_num)
            if bench_cells:
                bench_row = bench_cells[0].row

            ## Conditional Formatting Of Points
            num_rows = len(data_df) + 1

            position_mapping = {
                'QB': 'QBs',
                'RB': 'RBs',
                'WR': 'WRs',
                'TE': 'TEs',
                'D/ST': 'DST',
                'LB': 'IDP',
                'S':  'IDP',
                'CB': 'IDP',
                'DE': 'IDP',
                'DT': 'IDP',
                'K':  'KCK',
                'Starting Lineup': "TEAM",
                "Bench": "Bench"
            }

            rules = []
            rule_index = 0
            for position_value, scale_key in position_mapping.items():
                matching_cells = worksheet.findall(position_value, in_column=primpos_col_num)

                if not matching_cells:
                    continue

                row_numbers = [cell.row for cell in matching_cells]
                #print(f"Found {len(row_numbers)} rows for position {position_value}: {row_numbers}")

                # Get the max and median values for this position
                max_value = scale_dict["MAX"][scale_key]
                median_value = scale_dict["MEDIAN"][scale_key]
                min_value = scale_dict["MIN"][scale_key]

                # Create a range for each row with this position
                for row_num in row_numbers:
                    rule = {
                        "addConditionalFormatRule": {
                            "rule": {
                                "ranges": [{
                                    "sheetId": worksheet.id,
                                    "startRowIndex": row_num - 1,  # Convert to 0-indexed
                                    "endRowIndex": row_num,  # Exclusive, so this is just the one row
                                    "startColumnIndex": 5,  # Column F (0-indexed)
                                    "endColumnIndex": 10  # Column J (0-indexed, exclusive)
                                }],
                                "gradientRule": {
                                    "minpoint": {
                                        "color": {"red": 0.95, "green": 0.42, "blue": 0.42},  # Red
                                        "type": "NUMBER",
                                        "value": str(min_value)
                                    },
                                    "midpoint": {
                                        "color": {"red": 1, "green": 1, "blue": 1},  # White
                                        "type": "NUMBER",
                                        "value": str(median_value)
                                    },
                                    "maxpoint": {
                                        "color": {"red": 0.42, "green": 0.66, "blue": 0.42},  # Green
                                        "type": "NUMBER",
                                        "value": str(max_value)
                                    }
                                }
                            },
                            "index": rule_index
                        }
                    }
                    rules.append(rule)
                    rule_index += 1

                #print(f"Applied {len(rules)} gradient formatting rules to F:J")

            ## Bold And Border Totals Rows
            matching_cells = []
            for value in target_values:
                cells = worksheet.findall(value, in_column=rospos_col_num)
                matching_cells.extend(cells)

            # Loop Bold And Border
            for cell in matching_cells:
                row_number = cell.row

                # Add request for this row
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startRowIndex": row_number - 1,  # Convert to 0-indexed
                            "endRowIndex": row_number,
                            "startColumnIndex": 0,
                            "endColumnIndex": len(data_df.columns)  # Or use a fixed number
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "bold": True
                                },
                                "borders": {
                                    "top": {
                                        "style": "SOLID",
                                        "width": 2,
                                        "color": {"red": 0, "green": 0, "blue": 0}
                                    },
                                    "bottom": {
                                        "style": "SOLID",
                                        "width": 2,
                                        "color": {"red": 0, "green": 0, "blue": 0}
                                    }
                                }
                            }
                        },
                        "fields": "userEnteredFormat.textFormat,userEnteredFormat.borders"
                    }
                })

            # Execute all row formatting in one batch update (after the loop)
            if rules:
                worksheet.spreadsheet.batch_update({"requests": requests + rules})
            else:
                worksheet.spreadsheet.batch_update({"requests": requests})

        # Format League Projections Sheet
        def format_league_projections(worksheet, data_df):
            """
            Format the League Projections sheet with header styling and gradient color scale.

            Args:
                worksheet: gspread worksheet object
                data_df: pandas DataFrame (to determine number of rows)
                scale_dict: Dictionary with MAX and MEDIAN values
            """
            # Initialize Write Requests
            requests = []

            # Clear Formatting
            clear_requests = clear_sheet_formatting(worksheet, data_df)
            requests.append(clear_requests)

            num_rows = len(data_df) + 1  # +1 for header

            # 1) Format header row - bold, white text, navy blue background
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(data_df.columns)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {
                                "bold": True,
                                "foregroundColor": {"red": 1, "green": 1, "blue": 1}
                            },
                            "backgroundColor": {"red": 0, "green": 0, "blue": 0.5}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"
                }
            })

            # 2) Format columns E:I to 2 decimal places
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,  # Row 2 (0-indexed)
                        "endRowIndex": num_rows,
                        "startColumnIndex": 4,  # Column E (0-indexed)
                        "endColumnIndex": 9  # Column I (0-indexed, exclusive)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "0.00"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

            # 3) Get TEAM values from scale_dict for gradient
            max_value = scale_dict["MAX"]["TEAM"]
            median_value = scale_dict["MEDIAN"]["TEAM"]
            min_value = scale_dict["MIN"]["TEAM"]

            # Create gradient rule for columns E:I (rows 2 onwards)
            rule = {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": worksheet.id,
                            "startRowIndex": 1,  # Start at row 2 (0-indexed)
                            "endRowIndex": num_rows,
                            "startColumnIndex": 4,  # Column E (0-indexed)
                            "endColumnIndex": 9  # Column I (0-indexed, exclusive)
                        }],
                        "gradientRule": {
                            "minpoint": {
                                "color": {"red": 0.95, "green": 0.42, "blue": 0.42},  # Red
                                "type": "NUMBER",
                                "value": str(min_value)
                            },
                            "midpoint": {
                                "color": {"red": 1, "green": 1, "blue": 1},  # White
                                "type": "NUMBER",
                                "value": str(median_value)
                            },
                            "maxpoint": {
                                "color": {"red": 0.42, "green": 0.66, "blue": 0.42},  # Green
                                "type": "NUMBER",
                                "value": str(max_value)
                            }
                        }
                    },
                    "index": 0
                }
            }

            # Execute the formatting
            worksheet.spreadsheet.batch_update({"requests": requests + [rule]})

        # Format Free Agents
        def format_free_agents(worksheet, data_df, position):
            """
            Format the League Projections sheet with header styling and gradient color scale.

            Args:
                worksheet: gspread worksheet object
                data_df: pandas DataFrame (to determine number of rows)
                scale_dict: Dictionary with MAX and MEDIAN values
            """
            requests = []

            # Clear Formatting
            clear_requests = clear_sheet_formatting(worksheet, data_df)
            requests.append(clear_requests)

            num_rows = len(data_df) + 1  # +1 for header

            # 1) Format header row - bold, white text, navy blue background
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(data_df.columns)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {
                                "bold": True,
                                "foregroundColor": {"red": 1, "green": 1, "blue": 1}
                            },
                            "backgroundColor": {"red": 0, "green": 0, "blue": 0.5}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"
                }
            })

            # 2) Format columns F:J to 2 decimal places
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,  # Row 2 (0-indexed)
                        "endRowIndex": num_rows,
                        "startColumnIndex": 5,  # Column F (0-indexed)
                        "endColumnIndex": 10  # Column J (0-indexed, exclusive)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "0.00"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

            # 2) Get TEAM values from scale_dict for gradient
            max_value = scale_dict["MAX"][position]
            median_value = scale_dict["MEDIAN"][position]
            min_value = scale_dict["MIN"][position]

            # Create gradient rule for columns E:I (rows 2 onwards)
            rule = {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": worksheet.id,
                            "startRowIndex": 1,  # Start at row 2 (0-indexed)
                            "endRowIndex": num_rows,
                            "startColumnIndex": 4,  # Column E (0-indexed)
                            "endColumnIndex": 10  # Column J (0-indexed, exclusive)
                        }],
                        "gradientRule": {
                            "minpoint": {
                                "color": {"red": 0.95, "green": 0.42, "blue": 0.42},  # Red
                                "type": "NUMBER",
                                "value": str(min_value)
                            },
                            "midpoint": {
                                "color": {"red": 1, "green": 1, "blue": 1},  # White
                                "type": "NUMBER",
                                "value": str(median_value)
                            },
                            "maxpoint": {
                                "color": {"red": 0.42, "green": 0.66, "blue": 0.42},  # Green
                                "type": "NUMBER",
                                "value": str(max_value)
                            }
                        }
                    },
                    "index": 0
                }
            }

            # Execute the formatting
            worksheet.spreadsheet.batch_update({"requests": requests + [rule]})

        if sheet_name == "Lineup":
            df.columns = ['WK', 'TEAM', 'PLAYER', 'SLOT', 'POS', 'ESPN_PTS', 'FP_PTS', 'PINNY_PTS', 'BOL_PTS', 'TRUE_PTS', 'DIFF_PTS', 'ESPN', 'FP', 'PINNY', 'BOL', 'TRUE']
            set_with_dataframe(worksheet, df, include_index=False)
            format_lineup_rows(worksheet, df)
            time.sleep(5)
        elif sheet_name == "League_Projections":
            df.columns = ['WK', 'OWNER', 'TEAM', 'ACTUAL', 'ESPN', 'FP', 'BOL', 'PINNY', 'TRUE', 'DIFF']
            set_with_dataframe(worksheet, df, include_index=False)
            format_league_projections(worksheet, df)
            time.sleep(5)
        elif "FA_" in sheet_name:
            position = sheet_name.split("FA_")[1]
            try:
                fa_df = df[df["team_owner"].isin(["Free Agent", primary_owner])]
                fa_df.columns = ['WK', 'POS', 'PLAYER', 'OWNER', 'TEAM', 'ESPN_PTS', 'FP_PTS', 'BOL_PTS', 'PINNY_PTS', 'TRUE_PTS', 'ESPN', 'FP', 'BOL', 'PINNY', 'TRUE']
                set_with_dataframe(worksheet, fa_df, include_index=False)
                format_free_agents(worksheet, fa_df, position)
                print(f"Written {sheet_name} to Google Sheets")
                time.sleep(5)
            except (KeyError, ValueError, IndexError) as e:
                # A league without this position yields an empty frame, so the
                # column rename raises ValueError -- that one is expected and
                # benign. Everything else is reported with its real cause: this
                # was a bare `except` printing "Position Does Not Exist in
                # League" for any failure, so a NameError in here was
                # indistinguishable from a league simply having no kicker.
                print(f"Skipped {sheet_name}: {type(e).__name__}: {e}")

all = ['GOP_Degenerates', 'Knights_FFL', 'Weenieless_Wanderers', 'John_PC_League', 'John_ATL_League', "12 Dudes one Cup", 'Big Red Fantasy Football', 'Washed_Up_Fijians'] #, 'Winfield_Football'
john = ['John_PC_League', 'John_ATL_League']
tommy = ['Winfield_Football', 'Knights_FFL', 'GOP_Degenerates', 'Weenieless_Wanderers']
will = ["12 Dudes one Cup"]
cooleen = ['Big Red Fantasy Football']
fields = ['Washed_Up_Fijians']


def build_tables(lineups, curr_week, primary_own):
    """Turn a stored lineup frame into the ten tables the Sheet publishes.

    Split out of :func:`run` so it can be tested and diffed without a Google
    connection -- the equivalence check for the store migration compares these
    ten frames, not the rendered Sheet.

    Args:
        lineups: ``clean_lineups`` output, as read from the store.
        curr_week: Week to report on.
        primary_own: Team owner whose lineup and roster are highlighted.

    Returns:
        dict: Sheet name to frame, in publication order.
    """
    def fa(*positions):
        return get_rankings(pos=list(positions), week=curr_week, lu=lineups,
                            primary_owner=primary_own, visualize=False,
                            check_fa=False)

    return {
        "League_Projections": get_league_projections(week=curr_week, lu=lineups),
        "Lineup": check_week(lu=lineups, week=curr_week, own=primary_own),
        "FA_QBs": fa('QB'),
        "FA_RBs": fa('RB'),
        "FA_WRs": fa('WR'),
        "FA_TEs": fa('TE'),
        "FA_FLX": fa('RB', 'WR', 'TE'),
        "FA_DST": fa('D/ST'),
        "FA_KCK": fa('K'),
        "FA_IDP": fa('LB', 'DE', 'S', 'CB', 'DT'),
    }


def run(leagues=None, season=None):
    """Publish the weekly Sheets for each league, from the store.

    Reads ``Data/Store``; does not touch ESPN. Run
    ``python -m Scripts.refresh --all`` first, or a league with no store is
    skipped with the command that would build it.

    A league that fails is reported and skipped rather than aborting the run --
    publishing eight leagues should not be lost to one bad Sheet.

    Args:
        leagues: Display names or config keys. Defaults to the ``all`` cohort.
        season: Season to publish. Defaults to each league's configured ``end``.

    Returns:
        dict: ``{league: "ok" | reason}``.
    """
    leagues = all if leagues is None else leagues
    results = {}

    for select_league in leagues:
        cfg = resolve_league(select_league)
        year = cfg['end'] if season is None else int(season)
        primary_own = cfg['primary_own']

        try:
            LINEUPS = read_league_store(year, cfg['key'], "lineups")
            meta = read_meta(year, cfg['key'])
        except (FileNotFoundError, ValueError) as e:
            print(f"\nSkipping {select_league}: {e}")
            results[select_league] = f"no store: {type(e).__name__}"
            continue

        # The store's current_week, recorded from league.current_week at refresh
        # time. Taking it from here rather than re-fetching is the point: the Sheet
        # and the app then report the same week from the same build.
        curr_week = meta.get("current_week") or 1
        age = store_age_minutes(meta)
        age_note = "unknown age" if age is None else f"built {age:.0f} min ago"
        print(f"\n===== {select_league}: week {curr_week}, {age_note} =====")

        df_dict = build_tables(LINEUPS, curr_week, primary_own)
        _publish(df_dict, select_league, primary_own)
        results[select_league] = "ok"

    failed = {k: v for k, v in results.items() if v != "ok"}
    if failed:
        print(f"\n{len(failed)} league(s) not published: {failed}")
        print("Build their stores with `python -m Scripts.refresh --all`.")
    return results


def _publish(df_dict, league_name, primary_own):
    """Write one league's tables to its Sheet, then wait out the rate limit.

    Args:
        df_dict: Sheet name to frame, from :func:`build_tables`.
        league_name: Display name; must match the spreadsheet name exactly.
        primary_own: Owner kept alongside free agents on the ``FA_*`` tabs.
    """
    print(f"========= Now Writing Data For {league_name} ========")
    write_to_google(df_dict=df_dict, league_name=league_name,
                    primary_owner=primary_own)
    print(f"========= Successful Save For {league_name} ========")

    # Google Sheets rate-limits aggressively on consecutive writes. This plus the
    # 5s per sheet is ~9 min of sleeping across all nine leagues -- the dominant
    # cost of a Sheets run now that ingest is a store read. See
    # docs/plans/14-thin-google-sheets.md.
    sleep_secs = 20
    print(f"Now Sleeping For {sleep_secs} Seconds")
    time.sleep(sleep_secs)


if __name__ == "__main__":
    run()


