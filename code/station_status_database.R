###########################################################################################-
###########################################################################################-
##
## Station status text files to database: NYC ----
##
###########################################################################################-
###########################################################################################-

#=========================================================================================#
# Setting up ----
#=========================================================================================#

#-----------------------------------------------------------------------------------------#
# Loading libraries
#-----------------------------------------------------------------------------------------#

library(jsonlite)
library(tidyverse)
library(data.table)
library(lubridate)
library(DBI)
library(RSQLite)
library(dbplyr)
library(fs)
library(here)
library(glue)
library(iterators)
library(foreach)
library(doParallel)
library(tictoc)
library(zip)
library(dtplyr)

#-----------------------------------------------------------------------------------------#
# Loading custom functions
#-----------------------------------------------------------------------------------------#

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# make `fromJSON()` robust to errors (e.g., empty files)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

fromJSON_possibly <-
    possibly(
        fromJSON,
        otherwise = tibble()
    )

#-----------------------------------------------------------------------------------------#
# Connecting to database
#-----------------------------------------------------------------------------------------#

station_status_db <- dbConnect(SQLite(), here("data/station_status_db.sqlite3"))

#-----------------------------------------------------------------------------------------#
# Getting date of most recent data
#-----------------------------------------------------------------------------------------#

database_has_station_status <- station_status_db %>% dbExistsTable("station_status")

if (database_has_station_status) {
    
    query <- 
        sql(
            glue(
                "SELECT last_reported",
                "FROM station_status",
                "ORDER BY last_reported DESC",
                .sep = " "
            )
        )
    
    #-----------------------------------------------------------------------------------------#
    # Setting search parameters
    #-----------------------------------------------------------------------------------------#
    
    most_recent_day <- 
        station_status_db %>% 
        db_collect(query, n = 1) %>% 
        mutate(date = last_reported %>% as_datetime() %>% as_date()) %>% 
        pull(date)
    
} else {
    
    most_recent_day <- as_date("1900-01-01")
    
}


#-----------------------------------------------------------------------------------------#
# Listing raw data files
#-----------------------------------------------------------------------------------------#

local_dir <- here("data/raw")

file_list <- 
    dir_info(
        local_dir,
        recurse = FALSE,
        regexp = "[.]json"
    ) %>%
    arrange(path) %>%
    pull(path)

file_names <- 
    file_list %>%
    path_file() %>% 
    str_remove(".bz2") %>% 
    str_remove(".json")

file_dates <- 
    file_names %>% 
    str_extract("(\\d{4}-\\d{2}-\\d{2})") %>% 
    as_date()

station_status_files <- 
    tibble(
        file_list,
        file_names,
        file_dates
    ) %>%
    arrange(desc(file_dates))


#-----------------------------------------------------------------------------------------#
# Finding most recent raw data file
#-----------------------------------------------------------------------------------------#

# Dates that are later than the latest in local folder

station_status_files_to_add <- 
    station_status_files %>% 
    filter(file_dates >= most_recent_day) %>% 
    arrange(file_dates)


#=========================================================================================#
# Reading JSON data and saving to database ----
#=========================================================================================#

cl <- makePSOCKcluster(4)
registerDoParallel(cl)

#-----------------------------------------------------------------------------------------#
# Cleaning
#-----------------------------------------------------------------------------------------#

# station info for legacy_id

station_information <- 
    station_status_db %>% 
    tbl("station_information") %>% 
    select(.station_id = station_id, .legacy_id = legacy_id) %>% 
    collect()

# setting initial values of range

chunk_size <- 16000
start      <- 1
end        <- chunk_size

# iterating through list of files, in 24000 file chunks, until there are no more files

repeat {
    
    files_subset <- 
        station_status_files_to_add$file_list[start:end] %>% 
        na.omit()
    
    # if no more files, break
    
    if (length(files_subset) == 0) break
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # Print some pretty details of your progress
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    cat("=========================================\n")
    cat("Loop:", end/chunk_size, "\n")
    cat("- - - - - - - - - - - - - - - - - - - - -\n")
    cat("start:", "|", as.character(station_status_files_to_add$file_dates[start]), "[", start, "]", "\n")
    cat("end:  ", "|", as.character(station_status_files_to_add$file_dates[end]), "[", end, "]", "\n")
    
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # cleaning the data
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    station_status_parsed <- 
        
        foreach(
            
            i = 1:length(files_subset),
            .combine = function(...) rbind(..., fill = TRUE),
            .multicombine = TRUE,
            .errorhandling = "pass",
            .inorder = FALSE,
            .packages = c("dplyr", "jsonlite", "fs", "data.table")
            
        ) %dopar% {
            
            if (path_ext(files_subset[i]) == "json") {
                
                fromJSON_possibly(files_subset[i])$data$stations %>%
                    as_tibble() %>%
                    
                    # necessary to get data.table's rbind to work (i think)
                    
                    select(-where(is.list)) %>%
                    select_at(
                        vars(
                            -matches("rental_access_method"),
                            -matches("eightd"),
                            -matches("num_scooters_unavailable"),
                            -matches("num_scooters_available"),
                        )
                    ) %>% 
                    as.data.table()
                
            } else if (path_ext(files_subset[i]) == "bz2") {
                
                fromJSON_possibly(bzfile(files_subset[i]))$data$stations %>%
                    as_tibble() %>%
                    select(-where(is.list)) %>%
                    select_at(
                        vars(
                            -matches("rental_access_method"),
                            -matches("eightd"),
                            -matches("num_scooters_unavailable"),
                            -matches("num_scooters_available"),
                        )
                    ) %>% 
                    as.data.table()
                
            }
            
        }
    
    
    # if there are rows, mutate them
    
    if (nrow(station_status_parsed) > 0) {
        
        # if no legacy_id, add it
        
        if (!"legacy_id" %in% colnames(station_status_parsed)) {
            
            station_status_parsed <- station_status_parsed %>% mutate(legacy_id = NA_character_)
            
        }
        
        
        station_status <- 
            
            station_status_parsed %>% 
            
            lazy_dt() %>% 
            
            left_join(
                .,
                station_information,
                by = c("station_id" = ".legacy_id"),
                relationship = "many-to-many"
            ) %>% 
            
            mutate(
                
                station_id  = as.character(station_id),
                legacy_id   = as.character(legacy_id),
                
                date = last_reported %>% as_date(tz = "America/New_York") %>% as.character(),
                
                # clean legacy_id and station_id
                
                legacy_id = 
                    if_else(
                        station_id != .station_id, 
                        station_id, 
                        legacy_id
                    ),
                station_id_2 = 
                    if_else(
                        station_id != .station_id,
                        .station_id,
                        station_id
                    ),
                station_id = 
                    if_else(
                        is.na(station_id_2),
                        station_id,
                        station_id_2
                    ),
                legacy_id = 
                    if_else(
                        is.na(legacy_id),
                        station_id,
                        legacy_id
                    ),
                
                last_reported = as_datetime(last_reported, tz = "America/New_York")
                
            ) %>% 
            
            # removing broken dates by removing years before 2010 (e.g., 1970)
            
            filter(year > 2010) %>% 
            
            distinct(station_id, last_reported, .keep_all = TRUE) %>% 
            
            select(-.station_id, -station_id_2) %>%
            
            as_tibble()
        
        
        # free some memory
        
        invisible(gc(verbose = FALSE))
            
        
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
        # Print some pretty details of your progress
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
        
        cat("- - - - - - - - - - - - - - - - - - - - -\n")
        cat("Y:", unique(station_status$year), "M:", sort(unique(station_status$month)))
        cat("\n")
        
        
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
        # writing to database
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
        
        dbWriteTable(
            station_status_db,
            "station_status",
            value = station_status,
            append = TRUE,
            temporary = FALSE
        )
        
    }
    
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # Print some pretty details of your progress
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    cat("- - - - - - - - - - - - - - - - - - - - -\n")
    cat(format(nrow(station_status), big.mark = ","), "rows added")
    cat("\n")
    
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # updating range
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    start <- start + chunk_size
    end   <- end   + chunk_size
    
}

# clean up

stopCluster(cl)

dbDisconnect(station_status_db)


#-----------------------------------------------------------------------------------------#
# Cleaning up raw files ----
#-----------------------------------------------------------------------------------------#

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# adding to archive
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

if (file_exists(here("data/raw/added_raw_files_all.zip"))) {
    
    zipr_append(
        zipfile = here("data/raw/added_raw_files_all.zip"), 
        files = station_status_files_to_add$file_list,
        compression_level = 4
    )
    
} else {
    
    zipr(
        zipfile = here("data/raw/added_raw_files_all.zip"), 
        files = station_status_files_to_add$file_list,
        compression_level = 4
    )
    
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# deleting
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

file_delete(station_status_files_to_add$file_list)

#-----------------------------------------------------------------------------------------#
# Creating indexes ----
#-----------------------------------------------------------------------------------------#

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# Connecting to database
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

station_status_db <- dbConnect(SQLite(), here("data/station_status_db.sqlite3"))

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# listing indexes
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

index_tbl <- 
    station_status_db %>% 
    dbGetQuery(
        "SELECT * FROM sqlite_master WHERE type = 'index'"
    ) %>% 
    as_tibble()


if (all(index_tbl$tbl_name != "station_status")) {
    
    station_status_db %>% db_create_index("station_status", "station_id")
    station_status_db %>% db_create_index("station_status", "date")
    station_status_db %>% db_create_index("station_status", "last_reported")
    
    station_status_db %>% 
        db_create_index(
            "station_status", 
            c("station_id", "last_reported"),
            unique = FALSE
        )
    
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # vacuuming
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    station_status_db %>% dbExecute("VACUUM")
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # checking indexes
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    station_status_db %>% 
        dbGetQuery(
            "SELECT * FROM sqlite_master WHERE type = 'index'"
        ) %>% 
        as_tibble()
    
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# disconnecting from database
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

dbDisconnect(station_status_db)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# #
# #                             ---- THIS IS THE END! ----
# #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
