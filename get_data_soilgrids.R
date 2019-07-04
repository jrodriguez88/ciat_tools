# Script to download soil data from SOILGRIDS
# Source of data: https://www.soilgrids.org/  
# Author: Rodriguez-Espinoza J.
# Repository: https://github.com/jrodriguez88/ciat_tools
# 2019

## Load Packages
library(tidyverse)
library(curl)
library(jsonlite)
library(soiltexture)
library(tictoc)


#Set arguments. REST SoilGrids API : https://rest.soilgrids.org/ https://www.isric.org/explore/soilgrids/faq-soilgrids 
#path <- getwd()
soil_vars <- c("BLDFIE","CLYPPT","SNDPPT","CRFVOL","ORCDRC","WWP","AWCh1","AWCtS")
depths <- c("sl1", "sl2", "sl3", "sl4", "sl5")
lat <- 6.8
lon <- -58.1



### 'get_data_soilgrids' function for download soilgriddata
get_data_soilgrids <- function(soil_vars, lat, lon, depths){
    
    link <- paste0("https://rest.soilgrids.org/query?lon=", lon, "&lat=", lat, "&attributes=",
                   paste0(soil_vars, collapse = ","),"&depths=", paste0(depths, collapse = ","))
    
    json_data <- fromJSON(link)
    
    data <- map(.x = soil_vars, ~json_data$properties[[.x]]$M) %>% set_names(soil_vars)
    
    dept_value <- json_data$properties$depthCodesMeters %>% 
        bind_cols %>% dplyr::select(contains("sl")) %>% 
        gather(sl, depth)
    
    data %>% bind_rows(.id = "soil_vars") 
        
}


