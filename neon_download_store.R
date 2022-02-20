library(neonstore)
library(readr)

Sys.setenv("NEONSTORE_HOME" = "/efi_neon_challenge/neonstore")
Sys.setenv("NEONSTORE_DB" = "/efi_neon_challenge/neonstore")
Sys.setenv("duckdb_restart"="TRUE")


## Terrestrial
# #DP4.00200.001 & DP1.00094.001
# site_table <- read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-terrestrial/master/Terrestrial_NEON_Field_Site_Metadata_20210928.csv")
# sites <- site_table$field_site_id
# 
# print("Downloading: DP4.00200.001")
# neonstore::neon_download(product = "DP4.00200.001", site = sites, type = "basic")
# neon_store(product = "DP4.00200.001") 
# #print("Downloading: DP1.00094.001")
# #x <- neonstore::neon_download(product = "DP1.00094.001", site = sites, type = "basic")
# #neon_store(table = "SWS_30_minute", n = 50) 

## Aquatics
#site_table <- read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-aquatics/master/Aquatic_NEON_Field_Site_Metadata_20210928.csv")
#sites <- site_table$field_site_id

#message("Downloading: DP1.20288.001")
#neonstore::neon_download("DP1.20288.001",site = sites, type = "basic")
#neonstore::neon_store(table = "waq_instantaneous", n = 50)
#message("Downloading: DP1.20264.001")
#neonstore::neon_download("DP1.20264.001", site =  sites, type = "basic")
#neonstore::neon_store(table = "TSD_30_min")
#message("Downloading: DP1.20053.001")
#neonstore::neon_download("DP1.20053.001", site =  sites, type = "basic")
#neonstore::neon_store(table = "TSW_30min")


## Beetle
message("Downloading: DP1.10022.001")
neonstore::neon_download(product="DP1.10022.001", 
                         type = "expanded", 
                         start_date = NA,
                         .token = Sys.getenv("NEON_TOKEN"))
neon_store(product = "DP1.10022.001")

## Ticks

# tick data product, target sites, and dates
target.sites <- c("BLAN", "ORNL", "SCBI", "SERC", "KONZ", "TALL", "UKFS")
end.date <- "2019-12-31"

message("Downloading: Tick data (DP1.10093.001)")
neonstore::neon_download(product = "DP1.10093.001",
                                      site = target.sites, 
                                      type = "basic", 
                                      end_date = end.date)

neon_store(table = "tck_taxonomyProcessed-basic")
neon_store(table = "tck_fielddata-basic")

neon_download(product = "DP4.00001.001", # Summary weather statistics
              start_date = NA,
              end_date = "2019-12-31",   # end date for training data
              site = target.sites,       # target sites defined from 00_Target_Species_EDA.Rmd
              type = "basic")
neon_store(product = "DP4.00001.001")

