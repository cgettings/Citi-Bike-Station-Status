#####################################################################################

library(data.table)
library(tidyverse)
library(timetk)
library(fs)
library(tictoc)
library(dtplyr)

options(dplyr.summarise.inform = FALSE)
setDTthreads(6)

dir_create("data/rush_hour")

all_files <- dir_ls("data/monthly_csv")

# add population datasets


#####################################################################################

#####################################################################################

# bikes:
#   total number of bikes in a neighborhood
#   density of bikes per 1000 people in a neighborhood
#   rate of bikes per station in a neighborhood
# 
# stations:
#   total number of stations per neighborhood
#   density of stations per square mile in a neighborhood
#   rate of docks per station in a neighborhood
#   density of docks per 1000 people in a neighborhood

#####################################################################################

station_status_list <- list()

tic("overall")

for (i in 1:length(all_files)) {
    
    file_date <- 
        all_files[i] %>% 
        path_file() %>% 
        str_remove("station_status_") %>% 
        str_remove(".csv.bz2")
    
    tic(file_date)
    
    station_status_list[[i]] <- 
        fread(all_files[i]) %>% 
        lazy_dt() %>%
        select(
            last_reported, 
            station_id, 
            num_bikes_available, 
            num_docks_available
        ) %>% 
        mutate(last_reported = as_datetime(last_reported, tz = "US/Eastern")) %>% 
        mutate(
            last_reported_date = as_date(last_reported),
            last_reported_hour = floor_date(last_reported, "hours"),
            weekday = wday(last_reported) %in% 2:6
        ) %>% 
        arrange(station_id, last_reported) %>% 
        group_by(station_id) %>% 
        as_tibble() %>% 
        pad_by_time(
            .by = "hour", 
            .date_var = last_reported_hour, 
            .fill_na_direction = "up"
        ) %>% 
        lazy_dt() %>% 
        mutate(
            lr_hour = hour(last_reported_hour),
            rush_hour = if_else(lr_hour %in% 6:9, "AM", NA_character_),
            rush_hour = if_else(lr_hour %in% 16:19, "PM", rush_hour, NA_character_)
        ) %>% 
        filter(!is.na(rush_hour)) %>% 
        arrange(station_id, desc(last_reported)) %>% 
        distinct(station_id, last_reported, .keep_all = TRUE) %>% 
        group_by(station_id, last_reported_date, rush_hour) %>% 
        summarise(
            num_bikes_available_mdn = median(num_bikes_available, na.rm = TRUE),
            num_docks_available_mdn = median(num_docks_available, na.rm = TRUE),
            num_bikes_available_avg = mean(num_bikes_available, na.rm = TRUE),
            num_docks_available_avg = mean(num_docks_available, na.rm = TRUE)
        ) %>% 
        ungroup() %>% 
        as_tibble()
    
    # write_rds(station_status_mdn, paste0("data/rush_hour/station_status_mdn_", file_date, ".rds"))
    
    toc(TRUE)
    
    invisible(gc(verbose = FALSE))
    
}

station_status <- bind_rows(station_status_list)

write_rds(station_status, paste0("data/rush_hour/station_status.rds"))

toc(TRUE)

#####################################################################################

station_status_all <- 
    station_status %>% 
    mutate(year = year(last_reported_date)) %>% 
    group_by(station_id, year, rush_hour) %>% 
    summarise(
        num_bikes_available_mdn = median(num_bikes_available_mdn, na.rm = TRUE),
        num_docks_available_mdn = median(num_docks_available_mdn, na.rm = TRUE),
        num_bikes_available_avg = mean(num_bikes_available_avg, na.rm = TRUE),
        num_docks_available_avg = mean(num_docks_available_avg, na.rm = TRUE)
    ) %>% 
    ungroup()

write_rds(station_status_all, paste0("data/rush_hour/station_status_all.rds"))


station_status_summer <- 
    station_status %>% 
    mutate(
        year = year(last_reported_date),
        month = month(last_reported_date)
    ) %>% 
    filter(month %in% 5:9) %>% 
    group_by(station_id, year, rush_hour) %>% 
    summarise(
        num_bikes_available_mdn = median(num_bikes_available_mdn, na.rm = TRUE),
        num_docks_available_mdn = median(num_docks_available_mdn, na.rm = TRUE),
        num_bikes_available_avg = mean(num_bikes_available_avg, na.rm = TRUE),
        num_docks_available_avg = mean(num_docks_available_avg, na.rm = TRUE)
    ) %>% 
    ungroup()

write_rds(station_status_summer, paste0("data/rush_hour/station_status_summer.rds"))


#####################################################################################
