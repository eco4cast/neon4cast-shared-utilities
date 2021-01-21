#' Reading NOAA GEFS Forecasts from netcdf
#'
#' @param base_dir directory of the particular NOAA GEFS model (i.e., NOAAGEFS_1hr)
#' @param date date in YYYY-MM-DD of the forecast
#' @param cycle cycle of the forecast; the options are 00, 06, 12, 18
#' @param sites list if siteID
#'
#' @return data frame
#' @export
#'
#' @examples
#' 
#' library(tidyverse)
#' base_dir <- "/efi_neon_challenge/drivers/noaa/NOAAGEFS_1hr"
#' date <- "2020-10-21"
#' cycle <- "00"
#' sites <- c("ABBY", "BART")
#' noaa_gefs_read(base_dir, date, cycle, sites)
#' 
#' 

library(tidyverse)

noaa_gefs_read <- function(base_dir, date, cycle, sites){
  
  if(!(cycle %in% c("00","06","12","18"))){
    stop("cycle not available cycles of 00, 06,12,18")
  }
  
  cf_met_vars <- c("air_temperature",
                   "surface_downwelling_shortwave_flux_in_air",
                   "surface_downwelling_longwave_flux_in_air",
                   "relative_humidity",
                   "wind_speed",
                   "precipitation_flux")
  
  combined_met <- NULL
  
  for(i in 1:length(sites)){
    
    forecast_dir <- file.path(base_dir, sites[i], lubridate::as_date(date),cycle)
    
    forecast_files <- list.files(forecast_dir, full.names = TRUE)
    
    if(length(forecast_files) == 0){
      stop(paste0("no files in ", forecast_dir))
    }
    
    nfiles <-   length(forecast_files)
    
    for(j in 1:nfiles){
    
      ens <- dplyr::last(unlist(stringr::str_split(basename(forecast_files[j]),"_")))
      ens <- stringr::str_sub(ens,1,5)
      noaa_met_nc <- ncdf4::nc_open(forecast_files[j])
      noaa_met_time <- ncdf4::ncvar_get(noaa_met_nc, "time")
      origin <- stringr::str_sub(ncdf4::ncatt_get(noaa_met_nc, "time")$units, 13, 28)
      origin <- lubridate::ymd_hm(origin)
      noaa_met_time <- origin + lubridate::hours(noaa_met_time)
      noaa_met <- tibble::tibble(time = noaa_met_time)
      
      for(v in 1:length(cf_met_vars)){
        noaa_met <- cbind(noaa_met, ncdf4::ncvar_get(noaa_met_nc, cf_met_vars[v]))
      }
      
      ncdf4::nc_close(noaa_met_nc)
      
      names(noaa_met) <- c("time", cf_met_vars)
      
      noaa_met <- noaa_met %>% 
        dplyr::mutate(siteID = sites[i],
                      ensemble = as.numeric(stringr::str_sub(ens,4,5))) %>% 
        dplyr::select("siteID","ensemble","time",all_of(cf_met_vars))

      combined_met <- rbind(combined_met, noaa_met)
      
    }
  }
  return(combined_met)
}
