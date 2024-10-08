###########################################################################################-
###########################################################################################-
##
## Downloading station status json dumps ----
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
library(ssh)
library(here)
library(fs)
library(callr)
library(glue)
library(furrr)

#-----------------------------------------------------------------------------------------#
# for parallel map
#-----------------------------------------------------------------------------------------#

plan(multisession, workers = 4)

#-----------------------------------------------------------------------------------------#
# setting local path
#-----------------------------------------------------------------------------------------#

local_path <- "station_status/data/raw"

#=========================================================================================#
# downloading
#=========================================================================================#

#-----------------------------------------------------------------------------------------#
# establishing SSH session
#-----------------------------------------------------------------------------------------#

public_ipv4 <- Sys.getenv("aws_ip")
keyfile <- Sys.getenv("aws_key_path")

session <-
    ssh_connect(
        host = glue("ubuntu@{public_ipv4}"),
        keyfile = keyfile
    )

#-----------------------------------------------------------------------------------------#
# getting files to download
#-----------------------------------------------------------------------------------------#

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# listing files by city
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

cities <- 
    c(
        "boston",
        "chicago",
        "dc",
        "la",
        "montreal",
        "minneapolis",
        "nyc",
        "sf",
        "toronto"
    )

# naming elementa, so `map` will name output elements

names(cities) <- cities


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# listing remote files  ----
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

remote_files <- 
    cities %>% 
    future_map(
        ~ {
            this_session <-
                ssh_connect(
                    host = paste0("ubuntu@", public_ipv4),
                    keyfile = keyfile
                )
            
            files <- 
                ssh_exec_internal(this_session, glue("ls /home/ubuntu/station_status/{.x}"))$stdout %>% 
                rawToChar() %>% 
                str_split("\\n") %>% 
                flatten_chr() %>% 
                str_subset("json")
            
            ssh_disconnect(this_session)
            
            return(files)
        }
    )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# creating folders if they don't exist
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

cities %>% 
    walk(
        ~ dir_create(here(path(.x, local_path)))
    )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# listing local files
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

local_files <- 
    cities %>% 
    future_map(
        ~ list.files(here(path(.x, local_path)), pattern = "[.]json") %>% 
            path_file() %>% 
            sort()
    )


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# listing local files
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

files_to_download <- 
    cities %>% 
    map(
        ~ setdiff(remote_files[[.x]], local_files[[.x]]) %>% 
            path(
                "/home/ubuntu/station_status", 
                .x, 
                .
            ) %>% 
            as.character() %>% 
            sort() %>% 
            str_replace("\\[", "[") %>% 
            str_replace("\\]", "]") %>% 
            str_replace("\\(", "(") %>% 
            str_replace("\\)", ")")
    )


files_to_download %>% map_int(~length(.x)) %>% print()

#-----------------------------------------------------------------------------------------#
# downloading ----
#-----------------------------------------------------------------------------------------#

result <- 
    
    cities %>%
    
    map(
        ~ r_bg(
            
            func = function(files, city, public_ipv4, local_path, keyfile) {
                
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                # starting sessions
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                
                this_session <-
                    ssh::ssh_connect(
                        host = glue::glue("ubuntu@{public_ipv4}"),
                        keyfile = here::here(keyfile)
                    )
                
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                # iterating over list of files
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                
                out <- 
                    
                    capture.output(
                        purrr::walk(
                            
                            files, # fun arg given to r_bg [[ list elem mapping over ]]
                            
                            ~ ssh::scp_download(

                                this_session, # SSH session established within r_bg
                                files = ..1, # file path element walked over within r_bg
                                to = here::here(fs::path(city, local_path)), 
                                verbose = TRUE

                            )
                            
                        )
                    )
                
                cat(out, "\n\n")
                
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                # disconnecting from sessions
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                
                ssh::ssh_disconnect(this_session)
                
                return(out)
                
            }, 
            
            args = 
                list(
                    files = files_to_download[[.x]],
                    city = .x,
                    public_ipv4 = public_ipv4,
                    local_path = local_path,
                    keyfile = keyfile
                ),
            
            stdout = glue("out_{.x}_{public_ipv4}.txt"),
            stderr = glue("err_{.x}_{public_ipv4}.txt")
        )
        
    )

result %>% map( ~ .x)

#-----------------------------------------------------------------------------------------#
# waiting for downloads to finish ----
#-----------------------------------------------------------------------------------------#

# this is a clumsy way of waiting until all processes are finished -- it won't finish until 
#   all processes are finished

result %>% walk( ~ .x$wait())

#-----------------------------------------------------------------------------------------#
# deleting downloaded files
#-----------------------------------------------------------------------------------------#

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# listing local files ----
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

local_files_new <- 
    cities %>% 
    future_map(
        ~ list.files(here(path(.x, local_path)), pattern = "[.]json") %>% 
            path_file() %>% 
            sort()
    )

local_files_new %>% map_int(~length(.x)) %>% print()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# listing local files
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

# deleting files that i've already downloaded

files_to_delete <- 
    cities %>% 
    map(
        ~ intersect(remote_files[[.x]], local_files_new[[.x]]) %>%
            path(
                "/home/ubuntu/station_status", 
                .x, 
                .
            ) %>% 
            as.character() %>% 
            sort()
    )

files_to_delete %>% map_int(~length(.x)) %>% print()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# deleting ----
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

deleted_result <- 
    
    cities %>% 
    
    map(
        
        ~ r_bg(
            
            func = function(files, city, public_ipv4, keyfile) {
                
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                # starting sessions
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                
                this_session <-
                    ssh::ssh_connect(
                        host = glue::glue("ubuntu@{public_ipv4}"),
                        keyfile = here::here(keyfile)
                    )
                
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                # iterating over list of files
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                
                # creating empty list for console output
                
                out <- list()
                
                # setting initial value of list element iterator
                
                i <- 1
                
                # setting initial values of range
                
                start <- 1
                end <- 1000
                
                # iterating through list of files, in 1000 file chunks, 
                #   until there are no more files
                
                repeat {
                    
                    if (length(files) == 0) break
                    
                    files_to_delete_in_bg <- na.omit(files[start:end])
                    
                    # if no more files, break
                    
                    if (length(files_to_delete_in_bg) == 0) break
                    
                    # send `rm` command to server
                    
                    tryCatch({
                        
                        rawToChar(
                            ssh::ssh_exec_internal(
                                this_session, 
                                
                                stringr::str_c(
                                    "rm '", 
                                    stringr::str_c(
                                        files_to_delete_in_bg, 
                                        collapse = "' '"
                                    ), 
                                    "'",
                                    sep = ""
                                )
                                
                            )$stdout
                        )
                    }, finally = {
                        
                        # updating list element iterator
                        
                        i <- i + 1
                        
                        # updating range
                        
                        start <- start + 1000
                        end   <- end   + 1000
                        
                    }
                    )
                }
                
                
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                # disconnecting from sessions
                # - - - - - - - - - - - - - - - - - - - - - - - - - #
                
                ssh::ssh_disconnect(this_session)
                
                return(out)
                
            }, 
            
            args = 
                list(
                    files = files_to_delete[[.x]],
                    city = .x,
                    public_ipv4 = public_ipv4,
                    keyfile = keyfile
                ),
            
            stdout = glue("out_delete_{.x}_{public_ipv4}.txt"),
            stderr = glue("err_delete_{.x}_{public_ipv4}.txt")
        )
        
    )

deleted_result %>% map( ~ .x)

#-----------------------------------------------------------------------------------------#
# waiting for deletions to finish ----
#-----------------------------------------------------------------------------------------#

deleted_result %>% walk( ~ .x$wait())

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# checking files still on server ----
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

# wait for delete to finish on server

# Sys.sleep(20)

remote_files_new <- 
    cities %>% 
    future_map(
        ~ {
            this_session <-
                ssh_connect(
                    host = paste0("ubuntu@", public_ipv4),
                    keyfile = keyfile
                )
            
            files <- 
                ssh_exec_internal(this_session, glue("ls /home/ubuntu/station_status/{.x}"))$stdout %>% 
                rawToChar() %>% 
                str_split("\\n") %>% 
                flatten_chr() %>% 
                str_subset("json")
            
            ssh_disconnect(this_session)
            
            return(files)
        }
    )

remote_files_new %>% map_int(~length(.x)) %>% print()

#-----------------------------------------------------------------------------------------#
# kill ssh session ----
#-----------------------------------------------------------------------------------------#

ssh_disconnect(session)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# #
# #                             ---- THIS IS THE END! ----
# #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
