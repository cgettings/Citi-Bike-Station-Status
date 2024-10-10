###########################################################################################-
###########################################################################################-
##
## Compiling all station_information
##
###########################################################################################-
###########################################################################################-

#=========================================================================================#
# Setting up ----
#=========================================================================================#

#-----------------------------------------------------------------------------------------#
# Loading libraries
#-----------------------------------------------------------------------------------------#

library(tidyverse)
library(jsonlite)
library(fs)
library(DBI)
library(RSQLite)
library(dbplyr)

#-----------------------------------------------------------------------------------------#
# Connecting to database ----
#-----------------------------------------------------------------------------------------#

station_status_db <- dbConnect(SQLite(), here("data/station_status_db.sqlite3"))

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# make `fromJSON()` robust to errors (e.g., empty files)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

fromJSON_possibly <-
    possibly(
        fromJSON,
        otherwise = tibble()
    )


#=========================================================================================#
# data ops ----
#=========================================================================================#

#-----------------------------------------------------------------------------------------#
# from saved archive files ----
#-----------------------------------------------------------------------------------------#

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# loop thru files
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

station_information_files <- dir_ls("data", regexp = "station_information_.*json")

all_station_information <- tibble()

for (i in 1:length(station_information_files)) {
    
    station_information_path <- station_information_files[i]
    
    
    station_information_date <- 
        station_information_path %>% 
        path_file() %>% 
        path_ext_remove() %>% 
        str_remove("station_information_") %>% 
        parse_date_time(orders = c("Ymd", "Ymd HMS"), tz = "America/New_York") %>% 
        as_date()
    
    tryCatch({
        
        all_station_information <- 
            fromJSON_possibly(station_information_path)$data$stations %>%
            as_tibble() %>% 
            select(
                matches("legacy_id"),
                matches("station_id"),
                matches("short_name"),
                matches("capacity"),
                matches("name"),
                matches("^lat"),
                matches("^lon"),
            ) %>%
            mutate(across(everything(), ~ as.character(.x))) %>% 
            distinct() %>% 
            mutate(date = {{ station_information_date }}) %>% 
            bind_rows(
                ., 
                all_station_information
            )
        
        },
        error = function(e) {
            print(station_information_date)
            print(e)
            next
        }
    )
    
}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# join and deduplicate
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

station_information <- 
    all_station_information %>% 
    relocate(
        short_name,
        station_id, 
        legacy_id, 
        name
    ) %>% 
    mutate(id_length = str_length(station_id)) %>% 
    arrange(
        short_name, 
        legacy_id, 
        -id_length
    ) %>%
    distinct(
        short_name,
        .keep_all = TRUE
    ) %>% 
    mutate(legacy_id = if_else(is.na(legacy_id), station_id, legacy_id)) %>% 
    arrange(
        -id_length, 
        desc(date)
    ) %>% 
    distinct(
        legacy_id,
        .keep_all = TRUE
    ) %>% 
    select(-id_length)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# write to database
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

dbWriteTable(
    station_status_db,
    "station_information",
    value = station_information,
    append = FALSE,
    overwrite = TRUE,
    temporary = FALSE
)

#-----------------------------------------------------------------------------------------#
# get yearly ----
#-----------------------------------------------------------------------------------------#

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# join and deduplicate
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

yearly_station_information <- 
    left_join(
        station_information %>% select(short_name, station_id, legacy_id),
        all_station_information %>% select(-station_id, -legacy_id),
        by = "short_name"
    ) %>% 
    mutate(year = year(date)) %>% 
    arrange(short_name, desc(date)) %>% 
    distinct(
        short_name,
        year,
        .keep_all = TRUE
    )


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# save
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

write_csv(yearly_station_information, "data/yearly_station_information.csv")


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# write to database
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

dbWriteTable(
    station_status_db,
    "yearly_station_information",
    value = yearly_station_information,
    append = FALSE,
    overwrite = TRUE,
    temporary = FALSE
)


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


if (all(index_tbl$tbl_name != "station_information")) {
    
    station_status_db %>% db_create_index("station_information", "short_name")
    station_status_db %>% db_create_index("station_information", "station_id")
    station_status_db %>% db_create_index("station_information", "legacy_id")
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # vacuuming
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    # station_status_db %>% dbExecute("VACUUM")
    
}


if (all(index_tbl$tbl_name != "yearly_station_information")) {
    
    station_status_db %>% db_create_index("yearly_station_information", "short_name")
    station_status_db %>% db_create_index("yearly_station_information", "station_id")
    station_status_db %>% db_create_index("yearly_station_information", "legacy_id")
    station_status_db %>% db_create_index("yearly_station_information", "year")
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # vacuuming
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    
    # station_status_db %>% dbExecute("VACUUM")
}

station_status_db %>% dbExecute("VACUUM")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# checking indexes
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

station_status_db %>% 
    dbGetQuery(
        "SELECT * FROM sqlite_master WHERE type = 'index'"
    ) %>% 
    as_tibble()


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
