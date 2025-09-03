# GetBOLFutures.R

# getLowvig.R

# Packages
#suppressMessages(library(tidyverse))
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))
suppressMessages(library(stringr))
suppressMessages(library(magrittr))
suppressMessages(library(purrr))
suppressWarnings(suppressMessages(library(jsonlite)))
suppressMessages(library(lubridate))
suppressMessages(library(httr))

nfl_teams <- c(
  # AFC East
  'buffalo-bills',
  'miami-dolphins', 
  'new-england-patriots',
  'new-york-jets',
  
  # AFC North  
  'baltimore-ravens',
  'cincinnati-bengals',
  'cleveland-browns',
  'pittsburgh-steelers',
  
  # AFC South
  'houston-texans',
  'indianapolis-colts',
  'jacksonville-jaguars', 
  'tennessee-titans',
  
  # AFC West
  'denver-broncos',
  'kansas-city-chiefs',
  'las-vegas-raiders',
  'los-angeles-chargers',
  
  # NFC East
  'dallas-cowboys',
  'new-york-giants',
  'philadelphia-eagles',
  'washington-commanders',
  
  # NFC North
  'chicago-bears',
  'detroit-lions',
  'green-bay-packers',
  'minnesota-vikings',
  
  # NFC South
  'atlanta-falcons',
  'carolina-panthers', 
  'new-orleans-saints',
  'tampa-bay-bucs',
  
  # NFC West
  'arizona-cardinals',
  'los-angeles-rams',
  'san-francisco-49ers',
  'seattle-seahawks'
)

# Function to parse BetOnline JSON response
get_bol_raw <- function(t){
  # Define the URL
  url <- "https://api-offering.betonline.ag/api/offering/Sports/get-contests-by-contest-type2"
  cookie_handle <- handle("https://api-offering.betonline.ag")
  
  # Create the request body
  body <- paste0('{"ContestType":"nfl-player-performance","ContestType2":"', t, '","filterTime":0}')
  
  # Make the POST request with headers directly in the add_headers call
  response <- POST(
    url = url,
    handle = cookie_handle,
    add_headers(
      "Accept" = "application/json, text/plain, */*",
      "Accept-Language" = "en-US,en;q=0.9",
      "Content-Type" = "application/json",
      "gsetting" = "bolsassite",
      "Origin" = "https://www.betonline.ag",
      "Referer" = "https://www.betonline.ag/sportsbook/futures-and-props/nfl-player-performance/",
      "User-Agent" = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
      "utc-offset" = "240"
    ),
    body = body,
    encode = "raw",
    config = config(
      http_version = 1.1,  # Force HTTP/1.1
      ssl_verifypeer = FALSE,  # May help with SSL issues
      timeout = 30
    )
  )
  
  # Check if the request was successful and print detailed response information
  status_code <- status_code(response)
  
  if (status_code == 200) {
    # Parse the JSON response
    content <- content(response, "text", encoding = "UTF-8")
    return(content)
  } else {
    # Log detailed error information
    message(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] Error: "), http_status(response)$message)
    message("Response content: ", content(response, "text", encoding = "UTF-8"))
    return(NULL)
  }
}
parse_betonline_json <- function(team) {
  
  # Parse the JSON
  data <- fromJSON(get_bol_raw(t=team))
  
  # Navigate to the main betting data
  contest_offerings <- data$ContestOfferings
  #print(contest_offerings)
  
  if (is.null(contest_offerings) || is.null(contest_offerings$DateGroup)) {
    cat("Contest Offerings Do Not Exist for ", team,"\n")
    return(data.frame())
  }
  
  
  # Extract team and contest type info
  team_name <- contest_offerings$ContestType2
  contest_type <- contest_offerings$ContestType
  
  # Get the date groups (usually just one for season props)
  date_groups <- contest_offerings$DateGroup
  
  # Initialize results list
  all_props <- list()
  
  for (i in 1:nrow(date_groups)) {
    date_info <- date_groups[i, ]
    description_groups <- date_info$DescriptionGroup[[1]]
    
    if (is.null(description_groups) || nrow(description_groups) == 0) {
      cat("Date Groups Do Not Exist\n")
      next
    }
    
    # Parse each player prop
    for (j in 1:nrow(description_groups)) {
      prop_info <- description_groups[j, ]
      
      # Extract player and stat from description
      description <- prop_info$Description
      
      # Parse player name and stat type
      if (grepl(" - Total ", description)) {
        parts <- strsplit(description, " - Total ")[[1]]
        player_team <- parts[1]
        stat_type <- parts[2]
        
        # Extract just player name (remove team abbreviation)
        player_parts <- strsplit(player_team, " ")[[1]]
        team_abbr <- tail(player_parts, 1)
        player_name <- paste(head(player_parts, -1), collapse = " ")
      } else {
        player_name <- "Unknown"
        stat_type <- description
        team_abbr <- team_name
      }
      
      # Get betting information
      time_groups <- prop_info$TimeGroup[[1]]
      
      if (is.null(time_groups) || nrow(time_groups) == 0) {
        next
      }
      
      for (k in 1:nrow(time_groups)) {
        time_info <- time_groups[k, ]
        contest_extended <- time_info$ContestExtended[[1]][[1]]
        
        
        betting_line <- contest_extended$GroupLine
        contestants <- contest_extended$Contestants[[1]]
        
        
        if (is.null(contestants) || nrow(contestants) == 0) {
          next
        }
        
        # Extract over/under odds
        over_contestant <- contestants[contestants$Name == "Over", ]
        under_contestant <- contestants[contestants$Name == "Under", ]
        
        over_odds <- if(nrow(over_contestant) > 0) over_contestant$Line$MoneyLine$Line[1] else NA
        under_odds <- if(nrow(under_contestant) > 0) under_contestant$Line$MoneyLine$Line[1] else NA
        
        over_dec <- if(nrow(over_contestant) > 0) over_contestant$Line$MoneyLine$DecimalLine[1] else NA
        under_dec <- if(nrow(under_contestant) > 0) under_contestant$Line$MoneyLine$DecimalLine[1] else NA
        
        over_line <- if(nrow(over_contestant) > 0) over_contestant$ThresholdLine[1] else NA
        under_line <- if(nrow(under_contestant) > 0) under_contestant$Line$MoneyLine$Line[1] else NA
        
        over_id <- if(nrow(over_contestant) > 0) over_contestant$ID[1] else NA
        under_id <- if(nrow(under_contestant) > 0) under_contestant$ID[1] else NA
        
        # Get additional info
        contest_id <- contest_extended$ID[1]
        contest_datetime <- time_info$ContestExtended[[1]]$ContestDateTime[1]
        
        # Create row for this prop
        prop_row <- data.frame(
          team = team_name,
          player = player_name,
          stat_type = stat_type,
          line = betting_line,
          over_odds = over_odds,
          under_odds = under_odds,
          over_dec = over_dec,
          under_dec = under_dec,
          over_line = over_line,
          under_line = under_line,
          over_id = over_id,
          under_id = under_id,
          full_description = description,
          stringsAsFactors = FALSE
        )
        
        all_props[[length(all_props) + 1]] <- prop_row
      }
    }
  }
  
  # Combine all props into one dataframe
  if (length(all_props) > 0) {
    result_df <- bind_rows(all_props)
    
    # Clean up and format
    result_df <- result_df %>%
      arrange(player, stat_type)
    
    return(result_df)
  } else {
    return(data.frame())
  }
}
df_to_stats <- function(clean_df){
  
  df <- clean_df %>%
    mutate(
      across(c(player, team), ~ toupper(.)),
      stat_short = case_when(
        stat_type == 'Passing Interceptions' ~ 'INT_PASS',
        stat_type == "Passing TD's" ~ "TD_PASS",
        stat_type == "Passing Yards" ~ "YDS_PASS",
        
        stat_type == "Receiving TD's" ~ "TD_REC",
        stat_type == "Receiving Yards" ~ "YDS_REC",
        stat_type == "Receptions" ~ "REC_REC",
        
        stat_type == "Rushing TD's" ~ "TD_RUSH",
        stat_type == "Rushing Yards" ~ "YDS_RUSH",
        
        stat_type == "Sacks" ~ 'SK_DEF',
        stat_type == "Interceptions" ~ "INT_DEF",
        stat_type == "Tackles & Assists" ~ "TKL_DEF",
        str_detect(stat_type, "& Assists") | str_detect(stat_type, "Tackles & assists") ~ 'TKL_DEF',
        TRUE ~ stat_type
      ),
      over_imp_prob = 1 / over_dec,
      under_imp_prob = 1 / under_dec,
      
      over_juice = (1 / over_imp_prob) - 1,
      under_juice = (1 / under_imp_prob) - 1,
      
      juice = -(1 - (over_imp_prob + under_imp_prob)),
      juice_diff = under_juice - over_juice,
      True_Line = line + (juice_diff * line * 0.5),
      
      
      
      over_imp_prob = over_imp_prob / (1+juice),
      under_imp_prob = under_imp_prob / (1+juice),
      
      PPR_PTS = case_when(
        stat_type == 'Passing Interceptions' ~ -2* coalesce(True_Line, 0),
        stat_type == "Passing TD's" ~ 4* coalesce(True_Line, 0),
        stat_type == "Passing Yards" ~ 0.04* coalesce(True_Line, 0),
        
        stat_type == "Receiving TD's" ~ 6* coalesce(True_Line, 0),
        stat_type == "Receiving Yards" ~ 0.1* coalesce(True_Line, 0),
        stat_type == "Receptions" ~ 1* coalesce(True_Line, 0),
        
        stat_type == "Rushing TD's" ~ 6* coalesce(True_Line, 0),
        stat_type == "Rushing Yards" ~ 0.1* coalesce(True_Line, 0),
        
        stat_type == "Sacks" ~ 0* coalesce(True_Line, 0),
        stat_type == "Interceptions" ~ 0* coalesce(True_Line, 0),
        stat_type == "Tackles & Assists" ~ 0* coalesce(True_Line, 0),
        str_detect(stat_type, "& Assists") ~ 0* coalesce(True_Line, 0),
        TRUE ~ 0
      ),
    ) %>%
    group_by(team, player) %>%
    mutate(
      PPR_PTS = sum(PPR_PTS, na.rm = TRUE)
    ) %>%
    ungroup() %>%
    select(team, player, stat_short, stat_type, line, True_Line, over_odds, over_imp_prob, under_odds, under_imp_prob, PPR_PTS) #, PPR_PTS, STD_PTS
  
  # Pivot
  wide_data <- df %>%
    select(team, player, stat_short, True_Line, PPR_PTS) %>%
    pivot_wider(
      id_cols = c(team, player, PPR_PTS),
      names_from = stat_short,
      values_from = True_Line
    ) %>%
    mutate(
      pos = case_when(
        !is.na(YDS_PASS) ~ 'QB',
        !is.na(YDS_RUSH) & is.na(YDS_PASS) ~ 'RB',
        (!is.na(YDS_REC) | !is.na(REC_REC)) & is.na(YDS_RUSH) ~ 'WR/TE',
        TRUE ~ "DEF"
      )
    ) %>%
    select(
      team, player, pos, PPR_PTS,
      ends_with("_PASS"),
      ends_with("_RUSH"),
      ends_with("_REC")
    ) %>%
    group_by(pos) %>%
    mutate(pos_rank = rank(desc(PPR_PTS), ties.method = "min")) %>%
    ungroup() %>%
    arrange(pos, pos_rank)
  
  
  return(wide_data)
}

bol_raw <- map_dfr(nfl_teams, parse_betonline_json) 
bol_clean <- df_to_stats(bol_raw) %>% filter(pos != 'DEF')
bol_clean[is.na(bol_clean)] <- 0

write_csv(bol_clean, '2025_BetOnlineProps_Offense.csv')

bol_clean %>% filter(pos == 'WR/TE')


get_bol_raw(team='pittsburgh-steelers')

pmap_dfr()
parse_mlb_data <- function(json_data) {
  
  # Extract the games array
  games_list <- json_data$EventOffering$PeriodEvents
  
  # Initialize an empty list to store game data
  game_rows <- list()
  
  # Loop through each game and extract the relevant information
  for(i in seq_along(games_list)) {
    game_info <- games_list[[i]]
    period <- ifelse(game_info$Name == '1st 5 Innings', 'F5', toupper(game_info$Name))
    game_data <- game_info$Event
    
    # Create a list with the game data
    game_row <- list(
      officialDate = game_info$GameDateTime,
      GameID = game_data$GameId,
      Time_Local = game_data$WagerCutOff,
      gamePeriod = period,
      matchup = paste0(game_data$AwayTeam," vs. ", game_data$HomeTeam),
      RotNum_Away = game_data$AwayRotation,
      Team_Away = game_data$AwayTeam,
      Pitcher_Away = game_data$AwayPitcher,
      RotNum_Home = game_data$HomeRotation,
      Team_Home = game_data$HomeTeam,
      Pitcher_Home = game_data$HomePitcher,
      scheduleText = game_data$ScheduleText,
      
      # Moneyline
      Moneyline_Price_Away = game_data$AwayLine$MoneyLine$Line,
      Moneyline_Price_Home = game_data$HomeLine$MoneyLine$Line,
      
      # Total
      Total_Value_Away = game_data$TotalLine$TotalLine$Point,
      Total_Value_Home = game_data$TotalLine$TotalLine$Point,
      Total_Price_Away = game_data$TotalLine$TotalLine$Over$Line,
      Total_Price_Home = game_data$TotalLine$TotalLine$Under$Line,
      
      # Run lines (spread)
      Spread_Value_Away = game_data$AwayLine$SpreadLine$Point,
      Spread_Price_Away = game_data$AwayLine$SpreadLine$Line,
      Spread_Value_Home = game_data$HomeLine$SpreadLine$Point,
      Spread_Price_Home = game_data$HomeLine$SpreadLine$Line,
      
      # Team totals
      TeamTotal_Value_Away = game_data$AwayLine$TeamTotalLine$Point,
      TeamTotal_Over_Price_Away = game_data$AwayLine$TeamTotalLine$Over$Line,
      TeamTotal_Under_Price_Away = game_data$AwayLine$TeamTotalLine$Under$Line,
      TeamTotal_Value_Home = game_data$HomeLine$TeamTotalLine$Point,
      TeamTotal_Over_Price_Home = game_data$HomeLine$TeamTotalLine$Over$Line,
      TeamTotal_Under_Price_Home = game_data$HomeLine$TeamTotalLine$Under$Line
    )
    
    game_rows[[i]] <- game_row
  }
  
  # Convert list of lists to a dataframe
  games_df <- bind_rows(game_rows)
  
  return(games_df)
}
get_bol_game <- function(game_id){
  # Define the URL
  url <- "https://api-offering.betonline.ag/api/offering/sports/get-event"
  cookie_handle <- handle("https://api-offering.betonline.ag")
  
  # Create the request body
  body <- sprintf('{"Sport":"baseball","League":"mlb","gameID":%d,"ScheduleText":null}', game_id)
  
  Sys.sleep(runif(1, 0.1, 1))
  # Make the POST request with headers directly in the add_headers call
  response <- POST(
    url = url,
    handle = cookie_handle,
    add_headers(
      "Accept" = "application/json, text/plain, */*",
      "Accept-Language" = "en-US,en;q=0.9",
      "Content-Type" = "application/json",
      "gsetting" = "bolsassite",
      "Origin" = "https://www.betonline.ag",
      "Referer" = "https://www.betonline.ag/sportsbook/baseball/mlb",
      "User-Agent" = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15",
      "utc-offset" = "240"
    ),
    body = body,
    encode = "raw",
    config = config(
      http_version = 1.1,  # Force HTTP/1.1
      ssl_verifypeer = FALSE,  # May help with SSL issues
      timeout = 30
    )
  )
  
  # Check if the request was successful and print detailed response information
  status_code <- status_code(response)
  
  if (status_code == 200) {
    # Parse the JSON response
    content <- content(response, "text", encoding = "UTF-8")
    data <- fromJSON(content, simplifyVector = FALSE)
    clean <- parse_mlb_data(data)
    return(clean)
  } else {
    # Log detailed error information
    message(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] Error: "), http_status(response)$message)
    message("Response content: ", content(response, "text", encoding = "UTF-8"))
    return(NULL)
  }
}
clean_bol <- function(data){
  # Team Abbreviations
  tms <- teamabbr_build() %>% select(full_name, bp_teamabbr, mlb_teamabbr, TeamID)
  
  # Base Data Manipulation
  df <- data %>%
    mutate(
      Time_Local = ymd_hms(Time_Local),
      officialDate = format(Time_Local, "%Y-%m-%d"),
      Time_Local = format(Time_Local, "%I:%M %p"),
      game_number = case_when(
        str_detect(Team_Away, "Game #1") ~ 1,
        str_detect(Team_Away, "Game #2") ~ 2,
        TRUE ~ 1
      ),
      across(c(Team_Away, Team_Home, matchup), ~ case_when(
        str_detect(., " - Game #1") ~ str_remove(., " - Game #1"),
        str_detect(., " - Game #2") ~ str_remove(., " - Game #2"),
        TRUE ~ .
      )),
      matchup = case_when(
        str_detect(matchup, " - Game #1") ~ str_remove(matchup, " - Game #1"),
        str_detect(matchup, " - Game #2") ~ str_remove(matchup, " - Game #2"),
        TRUE ~ matchup
      ),
      P_Name_Home = trimws(str_split(Pitcher_Home, "-", simplify = TRUE)[,1]),
      P_Hand_Home = trimws(str_split(Pitcher_Home, "-", simplify = TRUE)[,2]),
      P_Name_Away = trimws(str_split(Pitcher_Away, "-", simplify = TRUE)[,1]),
      P_Hand_Away = trimws(str_split(Pitcher_Away, "-", simplify = TRUE)[,2]),
      sportsbook = 'LowVig'
    ) %>%
    left_join(teamabbr_build() %>% select(full_name, bp_teamabbr, mlb_teamabbr, TeamID), by = c('Team_Home' = 'full_name'), suffix = c('', '_Home')) %>%
    left_join(teamabbr_build() %>% select(full_name, bp_teamabbr, mlb_teamabbr, TeamID), by = c('Team_Away' = 'full_name'), suffix = c('', '_Away')) %>%
    pivot_longer(
      cols = starts_with("RotNum_"),
      values_to = "rotNum"
    ) %>%
    rename(bp_teamabbr_Home = bp_teamabbr, mlb_teamabbr_Home = mlb_teamabbr, TeamID_Home = TeamID)
  
  # Game Information
  Info <- df %>%
    rename(Home = Team_Home, Away = Team_Away) %>%
    mutate(rotNum = as.numeric(rotNum)) %>%
    select(officialDate, Time_Local, rotNum, game_number, matchup,
           Home, P_Name_Home, P_Hand_Home, bp_teamabbr_Home, mlb_teamabbr_Home, TeamID_Home,
           Away, P_Name_Away, P_Hand_Away, bp_teamabbr_Away, mlb_teamabbr_Away, TeamID_Away,
           sportsbook) %>%
    distinct()
  
  # Line INformation
  Lines <- df %>%
    mutate(
      Site = if_else((rotNum %% 2) == 0, 'Home', 'Away'),
      Moneyline_Side = if_else(Site == "Away", Team_Away, Team_Home),
      Spread_Side = if_else(Site == "Away", Team_Away, Team_Home),
      Total_Side = if_else(Site == "Away", "Over", "Under")
    ) %>%
    pivot_longer(
      cols = c(starts_with("Spread_"), starts_with("Moneyline_"), starts_with("Total_"), starts_with("TeamTotal_")),
      names_to = c("marketTitle", ".value"),
      names_pattern = "(Spread|Moneyline|Total|TeamTotal)_(.*)"
    ) %>%
    rename(betSide = Side, Home = Team_Home, Away = Team_Away) %>%
    distinct()
  
  TeamTotals <- Lines %>%
    filter(marketTitle == "TeamTotal", Value_Away > 0, Value_Home > 0) %>%
    select(-c(Price_Away, Price_Home)) %>%
    pivot_longer(
      cols = c(Over_Price_Away, Under_Price_Away, Over_Price_Home, Under_Price_Home),
      names_to = c("bet_type", "price_type", "team"),
      names_pattern = "(Over|Under)_(Price)_(Away|Home)",
      values_to = "price"
    ) %>%
    pivot_wider(
      names_from = team,
      values_from = price,
      names_prefix = "Price_"
    ) %>%
    mutate(
      betSide = paste0(Site, "_", bet_type)
    )
  
  # Combine
  Lines <- Lines %>% filter(marketTitle != "TeamTotal") %>% select(officialDate,matchup,rotNum,game_number,Home,Away,marketTitle,gamePeriod,betSide,Site,starts_with("Value_"),starts_with("Price_"))
  Lines <- if('Price_Home' %in% names(Lines)){rbind(Lines, TeamTotals %>% select(all_of(names(Lines))))} else {Lines}
  
  Lines <- Lines %>%
    mutate(
      value = if_else(Site == 'Home', Value_Home, Value_Away),
      price = as.numeric(if_else(Site == 'Home', Price_Home, Price_Away)),
      value = if_else(marketTitle == 'Moneyline', 0, value),
      impProb = ml_to_impprob(price),
      sportsbook = "LowVig"
    ) %>%
    select(officialDate,matchup,rotNum,game_number,Home,Away,marketTitle,gamePeriod,betSide,value,price,impProb,sportsbook)
  
  return(list(
    compare_df = Lines,
    info_df = Info
  ))
}

REG_List <- clean_bol(pmap_dfr(list(get_bol_ids()), get_bol_game))
LV_Info_DF <- REG_List$info_df
LV_DF <- REG_List$compare_df