# UsageEvidence.R
#
# Regenerates every measured claim in docs/plans/16-usage-data-layer.md. The
# plans there decide what to build and what not to build, so the numbers behind
# them have to be re-runnable rather than trusted -- the same standard plans 03,
# 11 and 15 already hold themselves to.
#
# Four tables, in the order the plan uses them:
#
#   1. Year-over-year stickiness      -- how hard to shrink each half of the
#                                        volume x efficiency split
#   2. Weekly predictive power        -- expected production vs actual
#                                        production at predicting next week
#   3. The same, split by position    -- where the signal actually lives
#   4. Injury report -> availability  -- whether the free injury table earns a
#                                        place in the model
#
# Usage:
#   Rscript R/UsageEvidence.R              # 2016-2025, prints four tables
#   Rscript R/UsageEvidence.R 2019 2025    # explicit season range
#
# Roughly 90s cold (downloads and caches ~150 MB), ~35s warm. Read-only: it
# writes nothing.

suppressMessages({
  library(nflreadr)
  library(nflfastR)
  library(dplyr)
  library(tidyr)
})
options(nflreadr.verbose = FALSE, dplyr.summarise.inform = FALSE)

args <- commandArgs(trailingOnly = TRUE)
FIRST <- if (length(args) >= 1) as.integer(args[1]) else 2016L
LAST  <- if (length(args) >= 2) as.integer(args[2]) else 2025L
SEASONS <- FIRST:LAST
message(sprintf("Seasons %d-%d", FIRST, LAST))

SKILL <- c("QB", "RB", "WR", "TE")

rule <- function(title) cat(sprintf("\n=== %s ===\n", title))

#' Mean of the previous `k` observations, excluding the current one.
#'
#' Written out rather than pulled from zoo so the script has no dependency
#' beyond what GetNFL.R already needs. Excluding the current observation is the
#' whole point: every feature for week N must be built from weeks before N, and
#' a rolling mean that includes the target is the single easiest way to produce
#' a backtest that looks excellent and a live model that is useless.
#'
#' @param x Numeric vector, ordered by week.
#' @param k Window length.
#' @return Numeric vector the same length as `x`, NA where there is no history.
trailing_mean <- function(x, k) {
  n <- length(x)
  out <- rep(NA_real_, n)
  if (n < 2) return(out)
  for (i in 2:n) {
    v <- x[max(1, i - k):(i - 1)]
    v <- v[!is.na(v)]
    if (length(v)) out[i] <- mean(v)
  }
  out
}

# --- Load ---------------------------------------------------------------
# Two frames keyed on gsis_id: observed production, and expected production.
#
# ff_opportunity returns season and week as *character*. The join below fails
# loudly on that rather than silently producing nothing, which is the good
# outcome, but the cast still has to happen somewhere -- here, once, on read.
message("  calculate_stats ...")
stats <- nflfastR::calculate_stats(SEASONS, "week", "player") %>%
  filter(season_type == "REG") %>%
  select(season, week, gsis_id = player_id, name = player_display_name,
         position, team, target_share, air_yards_share, wopr, targets,
         receptions, receiving_yards, receiving_tds, carries, rushing_tds,
         fantasy_points_ppr)

message("  load_ff_opportunity ...")
opportunity <- nflreadr::load_ff_opportunity(
    SEASONS, stat_type = "weekly", model_version = "latest") %>%
  select(season, week, gsis_id = player_id,
         exp_fp = total_fantasy_points_exp,
         actual_fp = total_fantasy_points) %>%
  mutate(season = as.integer(season), week = as.integer(week))

weeks <- stats %>% left_join(opportunity, by = c("season", "week", "gsis_id"))
message(sprintf("  %d player-weeks, %.1f%% matched to an expected line",
                nrow(weeks), 100 * mean(!is.na(weeks$exp_fp))))

# --- 1. Year-over-year stickiness ---------------------------------------
# The volume x efficiency split, measured. Opportunity metrics sit around 0.86
# to 0.92; touchdown rate and points-over-expected sit near 0.2. That gap is
# the whole argument for modelling the two halves separately and shrinking the
# second one hard.
seasonal <- weeks %>%
  filter(position %in% SKILL) %>%
  group_by(season, gsis_id, position) %>%
  summarise(
    games     = n(),
    tgt_share = mean(target_share, na.rm = TRUE),
    ay_share  = mean(air_yards_share, na.rm = TRUE),
    wopr      = mean(wopr, na.rm = TRUE),
    car_pg    = mean(carries, na.rm = TRUE),
    ypt       = sum(receiving_yards, na.rm = TRUE) /
                  pmax(sum(targets, na.rm = TRUE), 1),
    td_rate   = sum(receiving_tds + rushing_tds, na.rm = TRUE) /
                  pmax(sum(targets + carries, na.rm = TRUE), 1),
    ppg       = mean(fantasy_points_ppr, na.rm = TRUE),
    exp_ppg   = mean(exp_fp, na.rm = TRUE),
    oe_pg     = mean(actual_fp - exp_fp, na.rm = TRUE),
    .groups   = "drop"
  ) %>%
  filter(games >= 8)

# Join each season to the next by shifting the key back a year.
pairs <- seasonal %>%
  inner_join(seasonal %>% mutate(season = season - 1L),
             by = c("season", "gsis_id", "position"),
             suffix = c("", "_next"))

rule(sprintf("1. Year-over-year stickiness (n=%d player-season pairs, >=8 games both years)",
             nrow(pairs)))
cat("   this season's metric vs next season's same metric\n\n")
for (m in c("car_pg", "ay_share", "wopr", "tgt_share", "exp_ppg", "ppg",
            "ypt", "td_rate", "oe_pg")) {
  cat(sprintf("     %-10s r = %+.3f\n", m,
              cor(pairs[[m]], pairs[[paste0(m, "_next")]], use = "complete.obs")))
}
cat("\n   predicting NEXT season's points per game:\n\n")
for (m in c("ppg", "exp_ppg", "wopr", "tgt_share", "oe_pg")) {
  cat(sprintf("     %-10s -> ppg_next  r = %+.3f\n", m,
              cor(pairs[[m]], pairs$ppg_next, use = "complete.obs")))
}

# --- 2. Weekly: expected production vs actual production ----------------
# The headline result. Trailing expected fantasy points beats trailing actual
# fantasy points at predicting next week, and fitted jointly the model puts
# more than twice the weight on expected. That is the case for this whole
# workstream, and it is why the weekly head (plan 19) matters more than the
# season head (plan 18).
weekly <- weeks %>%
  filter(position %in% SKILL) %>%
  arrange(gsis_id, season, week) %>%
  group_by(gsis_id, season) %>%
  mutate(
    t3_actual = trailing_mean(fantasy_points_ppr, 3),
    t3_exp    = trailing_mean(exp_fp, 3),
    t6_exp    = trailing_mean(exp_fp, 6),
    t3_tgtsh  = trailing_mean(target_share, 3),
    t3_wopr   = trailing_mean(wopr, 3),
    t3_oe     = trailing_mean(actual_fp - exp_fp, 3)
  ) %>%
  ungroup() %>%
  filter(!is.na(t3_actual), !is.na(t3_exp))

receivers <- weekly %>% filter(position %in% c("RB", "WR", "TE"))

rule(sprintf("2. Weekly signals vs next week's PPR (RB/WR/TE, n=%d player-weeks)",
             nrow(receivers)))
cat("\n")
for (m in c("t3_actual", "t3_exp", "t6_exp", "t3_tgtsh", "t3_wopr", "t3_oe")) {
  cat(sprintf("     %-10s r = %+.3f\n", m,
              cor(receivers[[m]], receivers$fantasy_points_ppr, use = "complete.obs")))
}

# In-sample OLS. Deliberately so: this is a signal-strength measurement to
# decide what to build, not a model evaluation. The honest out-of-sample
# version is the walk-forward backtest specified in plans 18 and 19.
# "both + t6" is separated from "+ usage" on purpose. Bundled together they
# suggest usage shares are worth more than they are: most of the increment over
# `both` is simply a longer expected-production window, not the shares.
models <- list(
  "t3_actual"          = fantasy_points_ppr ~ t3_actual,
  "t3_exp"             = fantasy_points_ppr ~ t3_exp,
  "both"               = fantasy_points_ppr ~ t3_actual + t3_exp,
  "both + t6"          = fantasy_points_ppr ~ t3_actual + t3_exp + t6_exp,
  "both + t6 + usage"  = fantasy_points_ppr ~ t3_actual + t3_exp + t6_exp +
                           t3_tgtsh + t3_wopr + t3_oe
)
cat("\n   in-sample OLS on next week's PPR:\n\n")
for (nm in names(models)) {
  fit <- lm(models[[nm]], data = receivers)
  cat(sprintf("     %-18s R2 = %.4f   resid sd = %.2f\n",
              nm, summary(fit)$r.squared, sd(residuals(fit))))
}
cat("\n   coefficients, t3_actual + t3_exp:\n\n")
print(round(coef(lm(models[["both"]], data = receivers)), 4))

# --- 3. The same, by position -------------------------------------------
# Usage shares add most for WR and TE and least for QB, and everywhere they add
# far less than the expected-production term -- because ff_opportunity already
# encodes the usage. That finding is what keeps the feature set small.
rule("3. Incremental R2 by position (in-sample OLS on next week's PPR)")
cat(sprintf("\n     %-4s %8s   %9s %9s %9s %9s %9s\n",
            "pos", "n", "t3_actual", "t3_exp", "both", "+t6", "+usage"))
for (p in SKILL) {
  s <- weekly %>% filter(position == p)
  r2 <- function(f) summary(lm(f, data = s))$r.squared
  cat(sprintf("     %-4s %8d   %9.4f %9.4f %9.4f %9.4f %9.4f\n", p, nrow(s),
              r2(models[["t3_actual"]]), r2(models[["t3_exp"]]),
              r2(models[["both"]]), r2(models[["both + t6"]]),
              r2(models[["both + t6 + usage"]])))
}

# --- 4. Injury report -> availability -----------------------------------
# NOTE: load_injuries()'s `season_type` column is populated only for 2025.
# Filtering on it silently drops 2016-2024 and leaves a plausible-looking table
# built from one season. Filter on `week <= 18` instead.
injuries <- nflreadr::load_injuries(SEASONS) %>%
  mutate(season = as.integer(season), week = as.integer(week)) %>%
  filter(week <= 18) %>%
  distinct(season, week, gsis_id, .keep_all = TRUE) %>%
  select(season, week, gsis_id, report_status, practice_status)

# One row per week the player's *team* played, so a missed game is a row rather
# than an absence. Without this the analysis only ever sees players who played,
# which is exactly the population the question is about excluding.
schedule <- nflreadr::load_schedules(SEASONS) %>%
  filter(game_type == "REG") %>%
  select(season, week, home_team, away_team) %>%
  pivot_longer(c(home_team, away_team), values_to = "team") %>%
  select(season, week, team)

baselines <- weekly %>%
  group_by(gsis_id, season, position) %>%
  summarise(team = last(team), first_week = min(week),
            baseline = mean(t3_exp, na.rm = TRUE), .groups = "drop")

played <- weeks %>% transmute(season, week, gsis_id, played = 1L,
                              actual = fantasy_points_ppr)

grid <- baselines %>%
  inner_join(schedule, by = c("season", "team"), relationship = "many-to-many") %>%
  filter(week >= first_week) %>%
  left_join(played, by = c("season", "week", "gsis_id")) %>%
  mutate(played = coalesce(played, 0L), actual = coalesce(actual, 0)) %>%
  left_join(injuries, by = c("season", "week", "gsis_id")) %>%
  mutate(
    status   = ifelse(is.na(report_status), "(not on report)", report_status),
    practice = ifelse(is.na(practice_status) | trimws(practice_status) == "",
                      "(none)", practice_status)
  )

availability <- function(df, group) {
  df %>%
    group_by(across(all_of(group))) %>%
    summarise(n = n(),
              pct_missed = round(100 * mean(played == 0), 1),
              pct_of_baseline = round(100 * mean(actual) /
                                        mean(baseline, na.rm = TRUE), 0),
              .groups = "drop") %>%
    arrange(desc(n)) %>%
    as.data.frame()
}

rule(sprintf("4. Injury report vs availability (n=%d player-weeks, %.1f%% on a report)",
             nrow(grid), 100 * mean(grid$status != "(not on report)")))
cat("\n   'pct_of_baseline' is mean actual points as a share of mean baseline expected points.\n")
cat("\n   week-N game-status designation:\n\n")
print(availability(grid, "status"))
cat("\n   week-N practice participation:\n\n")
print(availability(grid, "practice"))
cat("\n   Questionable, split by practice participation -- the actionable cell:\n\n")
print(availability(filter(grid, status == "Questionable"), "practice"))

# The 'not on report' miss rate looks high because the weekly injury report
# drops a player once he lands on IR, and because this grid runs to the end of
# the season for anyone who was ever active. Separating "hurt" from "not on the
# roster" needs load_rosters_weekly()$status, which is why plans 16 and 19 list
# it as an availability input rather than an optional extra.
cat("\nDone.\n")
