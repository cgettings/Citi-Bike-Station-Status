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

station_status_db <- dbConnect(SQLite(), here("data/station_status_db_new.sqlite3"))

#-----------------------------------------------------------------------------------------#
# Listing existing monthly files
#-----------------------------------------------------------------------------------------#

local_dir <- here("data/daily_csv")

file_list <- 
    dir_info(
        local_dir,
        recurse = FALSE,
        regexp = "[.]bz2"
    ) %>%
    arrange(path) %>%
    pull(path)

file_names <- character()

file_names <- 
    file_list %>%
    path_file() %>% 
    str_remove(".csv") %>% 
    str_remove(".bz2")

# latest existing monthly file

if (length(file_names) > 0) {
    
    max_file_date <- 
        file_names %>% 
        str_extract("(\\d{4}-\\d{2})") %>% 
        parse_date("%Y-%m") %>% 
        max()
    
} else {
    
    max_file_date <- as_date("1900-01-01")
    
}


#-----------------------------------------------------------------------------------------#
# month x year combos not in file list
#-----------------------------------------------------------------------------------------#

all_dates <- 
    station_status_db %>%
    tbl("station_status") %>%
    select(date) %>% 
    arrange(date) %>% 
    filter(date >= {{ max_file_date }}) %>% 
    distinct() %>% 
    collect() %>% 
    drop_na() 


#=========================================================================================#
# pulling months ----
#=========================================================================================#

cl <- makePSOCKcluster(4)
registerDoParallel(cl)

foreach(
    i = 1:nrow(all_dates),
    .errorhandling = "pass",
    .inorder = FALSE,
    .packages = c("dplyr", "stringr", "readr", "fs", "here", "glue", "DBI", "RSQLite")
    
) %dopar% {
    
    station_status_db <- dbConnect(SQLite(), here("data/station_status_db_new.sqlite3"))
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # pulling months
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    station_status <- 
        station_status_db %>%
        tbl("station_status") %>%
        filter(date == {{ all_dates$date[i] }}) %>% 
        select(-station_status, -legacy_id) %>% 
        collect()
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # writing to csv
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    con <- 
        bzfile(
            here(glue("data/daily_csv/station_status_{all_dates$date[i]}.csv.bz2")),
            open = "wb",
            compression = 4
        )
    
    write_csv(station_status, con)
    close(con)
    
    dbDisconnect(station_status_db)
}


stopCluster(cl)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# #
# #                             ---- THIS IS THE END! ----
# #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
