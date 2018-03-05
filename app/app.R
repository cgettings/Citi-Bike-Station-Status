##################################################
##################################################
##
## Station status pie charts
##
##################################################
##################################################


#=========================#
# Loading packages ----
#=========================#

library(readr)
library(tibble)
library(ggplot2)
library(maptools)
library(rgeos)
library(rgdal)
library(magrittr)
library(dplyr)
library(gepaf)
library(sf)
library(sp)
library(tidyr)
library(geosphere)
library(shiny)
library(leaflet)
library(htmlwidgets)


ui <- fluidPage(leaflet::leafletOutput('myMap', height = 600))

server <- function(input, output) {
    
    
    # =================================#
    # ### Reading in mapping data ####
    # =================================#
    
    #-------------------------------------------#
    # ---- Parks ----
    #-------------------------------------------#
    
    park_shp_df <- read_sf("park_shp_df.shp")
    
    # =================================#
    # ### Map boundaries ####
    # =================================#
    
    ew_distance <- 12500
    ns_distance <- 12500
    
    lower_left <-
        data_frame(lon = -74.05,
                   lat =  40.665)
    
    lower_right <-
        destPoint(p = as.matrix(lower_left),
                  b = 90,
                  d = ew_distance) %>%
        as_tibble()
    
    upper_left <-
        destPoint(p = as.matrix(lower_left),
                  b = 0,
                  d = ns_distance) %>%
        as_tibble()
    
    map_center <-
        destPoint(p = as.matrix(lower_left),
                  b = 90,
                  d = ew_distance / 2) %>%
        as.matrix() %>%
        
        destPoint(p = .,
                  b = 360,
                  d = ns_distance / 2) %>%
        as_tibble()
    
    
    # =================================#
    # ### citibike data ####
    # =================================#
    
    citibike_info_1 <- read_rds("citibike_info_1.RDS")
    
    # =================================#
    # ### Mapping ####
    # =================================#
    
    
    map <- 
        leaflet() %>%
        
        addProviderTiles(providers$Esri.WorldGrayCanvas) %>%
    
    addPolygons(
        data = park_shp_df,
        color = NA,
        weight = 0.01,
        smoothFactor = 0.5,
        opacity = 0,
        fillOpacity = 0,
        label = ~park_name,
        popup = ~park_name,
        highlightOptions = highlightOptions(
            stroke = TRUE,
            opacity = .675,
            color = "forestgreen",
            weight = 1.5,
            bringToFront = TRUE,
            sendToBack = TRUE
        ),
        popupOptions = popupOptions(closeOnClick = TRUE)
        
    ) %>%
        
    addMarkers(
        data = citibike_info_1,
        lng  = ~lon,
        lat  = ~lat,
        icon = ~icons(
            iconUrl = citibike_info_1$pie_filenames,
            iconWidth  = 20,
            iconHeight = 20),

        popup = ~paste0("<strong>", name, "</strong>",
                        "<br>",
                        "Total Capacity: ",
                        "<var>", as.character(capacity), "</var>",
                        "<br>",
                        "Available Bikes: ",
                        "<var>", as.character(bikes), "</var>",
                        "<br>",
                        "Available Docks: ",
                        "<var>", as.character(docks), "</var>"
        ),
        label = ~name,
        popupOptions = popupOptions(offset = c(0, -5)),
        options = markerOptions(riseOnHover = FALSE)) %>%
    
    setView(
        lng = -73.980053,
        lat = 40.765569,
        zoom = 13)
    
    # output$myMap <- shinyRenderWidget(map)
    output$myMap <- leaflet::renderLeaflet(map)
}

shinyApp(ui, server)

# app <- shinyApp(ui, server)
# print(app)

# if (interactive()) print(app)


##################################################
##################################################
##
## END
##
##################################################
##################################################
