library(tidyverse)
library(tools)

Sys.setenv("AWS_DEFAULT_REGION" = "data",
           "AWS_S3_ENDPOINT" = "ecoforecast.org")

efi_server <- TRUE
#FUNCTIONS

download_theme <- function(theme, local_dir){
  parent_theme <- unlist(stringr::str_split(theme, "_"))[1]
  prefix <- paste(parent_theme,theme, sep="/")
  index <- aws.s3::get_bucket("forecasts",
                              prefix = prefix,
                              region = "data",
                              base_url = "ecoforecast.org",
                              max = Inf)
  keys <- vapply(index, `[[`, "", "Key", USE.NAMES = FALSE)
  empty <- grepl("/$", keys)
  keys <- keys[!empty]
  lapply(keys, function(x) aws.s3::save_object(x,
                                               bucket = "forecasts",
                                               file = file.path(local_dir, x),
                                               region = "data",
                                               base_url = "ecoforecast.org"))
}

download_theme_scores <- function(theme, local_dir){
  parent_theme <- unlist(stringr::str_split(theme, "_"))[1]
  prefix <- paste0(parent_theme,"/scores-", theme)
  index <- aws.s3::get_bucket("scores",
                              prefix = prefix,
                              region = "data",
                              base_url = "ecoforecast.org",
                              max = Inf)
  keys <- vapply(index, `[[`, "", "Key", USE.NAMES = FALSE)
  empty <- grepl("/$", keys)
  keys <- keys[!empty]
  lapply(keys, function(x) aws.s3::save_object(x,
                                               bucket = "scores",
                                               file = file.path(local_dir, x),
                                               region = "data",
                                               base_url = "ecoforecast.org"))
}

combine_forecasts <- function(base_dir, target){
  
  fnames <- tibble(files = list.files(path = base_dir, recursive = TRUE, full.names = TRUE)) %>%
    filter(file_ext(files) %in% c("nc","csv","gz")) %>%
    filter(!str_detect(files, "not_in_standard")) %>%
    mutate(basename = basename(files))
  
  d <-  unlist(str_split_fixed(tools::file_path_sans_ext(fnames$basename), pattern = "-", 5))
  
  data <- unlist(str_split_fixed(tools::file_path_sans_ext(fnames$basename), pattern = "-", 5)) %>%
    as_tibble() %>%
    unite("date", V2:V4, sep = "-") %>%
    rename("theme" = V1,
           "team" = V5) %>%
    bind_cols(fnames) %>%
    select(-basename)
  
  combined <- NULL
  
  #for(i in 1:100){
  for(i in 1:nrow(data)){
    
    print(i)
    
    d <- neon4cast:::read_forecast(data$files[i])
    
    if(target %in% names(d)){
      
      if("ensemble" %in% colnames(d)){
        d <- d %>%
          group_by(time, siteID, forecast_start_time, horizon, team, theme) %>%
          summarise(mean = mean(get(target), na.rm = TRUE),
                    sd = sd(get(target), na.rm = TRUE),
                    upper95 = quantile(get(target), 0.975, na.rm = TRUE),
                    lower95 = quantile(get(target), 0.025, na.rm = TRUE)) %>%
          pivot_longer(cols = c("mean","sd", "upper95","lower95"), names_to = "statistic", values_to = "value") %>%
          select(time, siteID, forecast_start_time, horizon, team, theme, statistic, value) %>%
          mutate(target = target,
                 time = lubridate::as_datetime(time),
                 forecast_start_time = lubridate::as_datetime(forecast_start_time))
        
      }else{
        d <- d %>%
          select(time, siteID, forecast_start_time, horizon, team, theme, statistic, target) %>%
          pivot_wider(names_from = statistic, values_from = target, values_fn = mean) %>%
          mutate(upper95 = mean + 1.96 * sd,
                 lower95 = mean - 1.96 * sd) %>%
          pivot_longer(cols = c("mean","sd", "upper95","lower95"), names_to = "statistic", values_to = "value") %>%
          select(time, siteID, forecast_start_time, horizon, team, theme, statistic, value) %>%
          mutate(target = target,
                 time = lubridate::as_datetime(time),
                 forecast_start_time = lubridate::as_datetime(forecast_start_time))
      }
      combined <- bind_rows(combined, d)
    }
  }
  return(combined)
}

combine_scores <- function(files){
  teams_tmp <- (str_split(basename(files), c("-")))
  score <- NULL
  for(i in 1:length(teams_tmp)){
    curr_score <- readr::read_csv(files[i])
    if(nrow(curr_score) > 0 & c("crps","logs") %in% names(curr_score)){
      dates <- sort(lubridate::as_date(unique(curr_score$time)))
      time_step <- dates[2] - dates[1]
      first_date <- dates[1] - time_step
      combined <- tibble(time = curr_score$time,
                         siteID = curr_score$siteID,
                         forecast_start_time = first_date,
                         theme = curr_score$theme,
                         target = curr_score$target,
                         team = curr_score$team,
                         crps = curr_score$crps,
                         logs = curr_score$logs
      )
      score <- rbind(score, combined)
    }
  }
  return(score)
}

if(!efi_server){
  local_dir <- "~/Documents/scripts/neon4cast-shared-utilities/scores"
  
  # DOWNLOAD SCORES
  download_theme_scores(theme = "phenology", local_dir = local_dir)
  download_theme_scores(theme = "aquatics", local_dir = local_dir)
  download_theme_scores(theme = "terrestrial", local_dir = local_dir)
  download_theme_scores(theme = "ticks", local_dir = local_dir)
  download_theme_scores(theme = "beetles", local_dir = local_dir)
  
  local_dir <- "~/Documents/scripts/neon4cast-shared-utilities/forecasts"
  
  # DOWNLOAD FORECASTS
  download_theme(theme = "aquatics", local_dir = "forecasts")
  download_theme(theme = "phenology", local_dir = "forecasts")
  download_theme(theme = "terrestrial", local_dir = "forecasts")
  download_theme(theme = "ticks", local_dir = "forecasts")
  download_theme(theme = "beetles", local_dir = "forecasts")
}
# COMBINED SCORES
files <- list.files("/efi_neon_challenge/scores", full.names = TRUE, recursive = TRUE)
combined_scores <- combine_scores(files)
combined_scores$logs <- as.numeric(combined_scores$logs)

# COMBINE FORECASTS
combined_temperature <- combine_forecasts(base_dir = '/efi_neon_challenge/forecasts/aquatics/', target = "temperature")
combined_oxygen <- combine_forecasts(base_dir = '/efi_neon_challenge/forecasts/aquatics/', target = "oxygen")
combined_gcc_90 <- combine_forecasts(base_dir = '/efi_neon_challenge/forecasts/phenology/', target = "gcc_90")
combined_rcc_90 <- combine_forecasts(base_dir = '/efi_neon_challenge/forecasts/phenology/', target = "rcc_90")
combined_nee <- combine_forecasts(base_dir = '/efi_neon_challenge/forecasts/terrestrial/', target = "nee")
combined_le <- combine_forecasts(base_dir = '/efi_neon_challenge/forecasts/terrestrial/', target = "le")
combined_ixodes <- combine_forecasts(base_dir = '/efi_neon_challenge/forecasts/ticks/', target = "ixodes_scapularis")
combined_amblyomma <- combine_forecasts(base_dir = '/efi_neon_challenge/forecasts/ticks/', target = "amblyomma_americanum")
combined_beetle_abundance <- combine_forecasts(base_dir = '/efi_neon_challenge/forecasts/beetles/', target = "abundance")
combined_beetle_richness <- combine_forecasts(base_dir = '/efi_neon_challenge/forecasts/beetles/', target = "richness")

combined <- bind_rows(combined_temperature,
                      combined_oxygen,
                      combined_gcc_90,
                      combined_rcc_90,
                      combined_nee,
                      combined_le,
                      combined_ixodes,
                      combined_amblyomma,
                      combined_beetle_abundance,
                      combined_beetle_richness)

#REMOVE DUPLICATED ROWS (WHY ARE THEY THERE?)
combined <- combined %>% distinct(time, siteID, forecast_start_time, team, theme, horizon, statistic, target, .keep_all = TRUE)

combined_wide <- pivot_wider(combined, names_from = statistic, values_from = value)
combined_forecast_scores <- left_join(combined_wide, combined_scores)

write_csv(combined_forecast_scores, file = "/efi_neon_challenge/forecasts/combined_forecasts_scores.csv")
