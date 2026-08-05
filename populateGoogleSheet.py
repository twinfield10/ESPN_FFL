# Base
import time
import datetime

# Data Manipulation
import pandas as pd
import polars as pl
import numpy as np

# Modeling
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

# Scripts
from Scripts.fetch_utils import fetch_league
from Scripts.analytic_utils import *
from Scripts.scrape_player_stats import *
from Scripts.scrape_team_stats import *
from Scripts.luck_index import *
from Scripts.tidbit_utils import *
from Scripts.simulation_utils import *

# Google Sheet
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

# Config Leagues
from Scripts.config_utils import build_lg_vars, get_season, load_config

config = load_config()
lg_vars = build_lg_vars(config)
SEASON = get_season(config)

# Projection blending pipeline. These 12 functions used to be defined here and
# pasted into FF Analysis Notebook.ipynb as a second, drifting copy; they now
# live in one place and both callers import them.
from Scripts.projection_utils import (
    change_col_prefix,
    check_week,
    clean_bol,
    clean_lineups,
    clean_pinny,
    compute_weighted_stats,
    create_mean_cols,
    get_league_projections,
    get_match_details,
    get_rankings,
    impute_columns,
    proj_to_score,
)


def write_to_google(df_dict, league_name):
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
            try:
                position = sheet_name.split("FA_")[1]
                fa_df = df[df["team_owner"].isin(["Free Agent", lg_vars[select_league]['primary_own']])]
                fa_df.columns = ['WK', 'POS', 'PLAYER', 'OWNER', 'TEAM', 'ESPN_PTS', 'FP_PTS', 'BOL_PTS', 'PINNY_PTS', 'TRUE_PTS', 'ESPN', 'FP', 'BOL', 'PINNY', 'TRUE']
                set_with_dataframe(worksheet, fa_df, include_index=False)
                format_free_agents(worksheet, fa_df, position)
                print(f"Written {sheet_name} to Google Sheets")
                time.sleep(5)
            except:
                print(f"Error Loading {sheet_name} | Position Does Not Exist in League")

all = ['GOP_Degenerates', 'Knights_FFL', 'Weenieless_Wanderers', 'John_PC_League', 'John_ATL_League', "12 Dudes one Cup", 'Big Red Fantasy Football', 'Washed_Up_Fijians'] #, 'Winfield_Football'
john = ['John_PC_League', 'John_ATL_League']
tommy = ['Winfield_Football', 'Knights_FFL', 'GOP_Degenerates', 'Weenieless_Wanderers']
will = ["12 Dudes one Cup"]
cooleen = ['Big Red Fantasy Football']
fields = ['Washed_Up_Fijians']


def run(leagues=None):
    """Build and publish the weekly Sheets for each league.

    Args:
        leagues: Display names to process. Defaults to the ``all`` cohort.
    """
    leagues = all if leagues is None else leagues
    for select_league in leagues:
        # Get League. The year comes from config for both calls below -- these
        # used to disagree, with fetch_league reading config and the lineup
        # fetch hardcoding 2025, so league metadata and player stats could
        # silently come from different seasons.
        year = lg_vars[select_league]['end']
        league = fetch_league(
            league_id=lg_vars[select_league]['ID'],
            year=year,
            swid=lg_vars[select_league]['SWID'],
            espn_s2=lg_vars[select_league]['ESPN_S2']
        )

        # Lineup Tables
        lineups = get_ply_stats_by_matchup(league_id=lg_vars[select_league]['ID'],
                        year=year,
                        swid=lg_vars[select_league]['SWID'],
                        espn_s2=lg_vars[select_league]['ESPN_S2'])
        free_agents = build_fa_market(league=league)
        curr_week = league.current_week
        lineups = pd.concat([lineups, free_agents])
        lineups.fillna(0, inplace=True)
        lineups = lineups.drop_duplicates(subset=['week', 'player_name'])

        ## Build Combined Lineup Table
        LINEUPS = clean_lineups(df=lineups, lg=league)

        # Data Dictionary
        primary_own = lg_vars[select_league]['primary_own']
        df_dict = {
            "League_Projections": get_league_projections(week = curr_week, lu=LINEUPS),
            "Lineup": check_week(lu = LINEUPS, week = curr_week, own=primary_own),
            "FA_QBs": get_rankings(pos = ['QB'], week = curr_week, lu=LINEUPS, primary_owner=primary_own, visualize=False, check_fa=False),
            "FA_RBs": get_rankings(pos = ['RB'], week = curr_week, lu=LINEUPS, primary_owner=primary_own, visualize=False, check_fa=False),
            "FA_WRs": get_rankings(pos = ['WR'], week = curr_week, lu=LINEUPS, primary_owner=primary_own, visualize=False, check_fa=False),
            "FA_TEs": get_rankings(pos = ['TE'], week = curr_week, lu=LINEUPS, primary_owner=primary_own, visualize=False, check_fa=False),
            "FA_FLX": get_rankings(pos = ['RB', 'WR','TE'], week = curr_week, lu=LINEUPS, primary_owner=primary_own, visualize=False, check_fa=False),
            "FA_DST": get_rankings(pos = ['D/ST'], week = curr_week, lu=LINEUPS, primary_owner=primary_own, visualize=False, check_fa=False),
            "FA_KCK": get_rankings(pos = ['K'], week = curr_week, lu=LINEUPS, primary_owner=primary_own, visualize=False, check_fa=False),
            "FA_IDP": get_rankings(pos = ['LB', 'DE', 'S', 'CB', 'DT'], week = curr_week, lu=LINEUPS, primary_owner=primary_own, visualize=False, check_fa=False),
        }
        # Save to Google Sheet
        print("")
        print(f"========= Now Writing Data For {select_league} ========")
        print("")
        write_to_google(df_dict=df_dict, league_name=select_league)
        print("")
        print(f"========= Successful Save For {select_league} ========")
        print("")

        # Sleep. Google Sheets rate-limits aggressively on consecutive writes.
        sleep_secs = 20
        print(f"Now Sleeping For {sleep_secs} Seconds")
        time.sleep(sleep_secs)


if __name__ == "__main__":
    run()


