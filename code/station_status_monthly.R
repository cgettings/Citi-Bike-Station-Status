###########################################################################################-
###########################################################################################-
##
## Exporting monthly station status CSV
##
###########################################################################################-
###########################################################################################-

# This script exports data from the station status database as monthly CSV files.

#=========================================================================================#
# Setting up ----
#=========================================================================================#

#-----------------------------------------------------------------------------------------#
# Loading libraries
#-----------------------------------------------------------------------------------------#

suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RSQLite))
suppressPackageStartupMessages(library(dbplyr))
suppressPackageStartupMessages(library(fs))
suppressPackageStartupMessages(library(here))
suppressPackageStartupMessages(library(glue))
suppressPackageStartupMessages(library(iterators))
suppressPackageStartupMessages(library(foreach))
suppressPackageStartupMessages(library(doParallel))

#-----------------------------------------------------------------------------------------#
# Connecting to database
#-----------------------------------------------------------------------------------------#

citibike_trip_db <- dbConnect(SQLite(), here("data/citibike_trip_db.sqlite3"))

#-----------------------------------------------------------------------------------------#
# Listing existing monthly files
#-----------------------------------------------------------------------------------------#

local_dir <- here("data/station_status/monthly_csv")

file_list <- 
    dir_info(local_dir,
             recurse = FALSE,
             regexp = "[.]bz2") %>%
    arrange(path) %>%
    pull(path)

file_names <- 
    file_list %>%
    path_file() %>% 
    str_remove(".csv") %>% 
    str_remove(".bz2")

# latest existing monthly file

max_file_date <- 
    file_names %>% 
    str_extract("(\\d{4}-\\d{2})") %>% 
    parse_date("%Y-%m") %>% 
    max()


#-----------------------------------------------------------------------------------------#
# month x year combos not in file list
#-----------------------------------------------------------------------------------------#

year_month <- 
    citibike_trip_db %>%
    tbl("station_status") %>%
    filter(date >= !!as.integer(max_file_date + months(1))) %>% 
    select(year, month) %>% 
    collect() %>% 
    distinct() %>% 
    drop_na() %>% 
    slice(1:nrow(.) - 1) # dropping most recent month, because we're in it


#=========================================================================================#
# pulling months ----
#=========================================================================================#

if (nrow(year_month) < 4) {
    
    n_processes <- nrow(year_month)
    
} else {
    
    n_processes <- 4
    
}

cl <- makePSOCKcluster(n_processes)
registerDoParallel(cl)

foreach(
    i = 1:nrow(year_month),
    .errorhandling = "pass",
    .inorder = FALSE,
    .packages = c("tidyverse", "fs", "here", "glue", "DBI", "RSQLite")
    
) %dopar% {
    
    citibike_trip_db <- dbConnect(SQLite(), here("data/citibike_trip_db.sqlite3"))
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # pulling months
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    station_status <- 
        citibike_trip_db %>%
        tbl("station_status") %>%
        filter(year == !!year_month$year[i], month == !!year_month$month[i]) %>% 
        collect()
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # writing to csv
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    con <- 
        bzfile(
            here(
                str_c(
                    "data/station_status/monthly_csv/",
                    "station_status_", year_month$year[i], "-", sprintf("%02.f", year_month$month[i]), ".csv.bz2"
                )
            ),
            open = "wb",
            compression = 4
        )
    
    write_csv(station_status, con)
    close(con)
    
    dbDisconnect(citibike_trip_db)
}


stopCluster(cl)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# #
# #                             ---- THIS IS THE END! ----
# #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
