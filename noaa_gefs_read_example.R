library(tidyverse)

base_dir <- "/efi_neon_challenge/drivers/noaa/NOAAGEFS_1hr"

date <- "2020-10-21"

cycle <- "00"

sites <- c("ABBY", "BART")

noaa_gefs_read(base_dir, date, cycle, sites)