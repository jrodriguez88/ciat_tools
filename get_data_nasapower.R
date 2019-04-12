# Script to download weather data from NASA-POWER
# Source of data: NASA Prediction Of Worldwide Energy Resources https://power.larc.nasa.gov/
# Author: Rodriguez-Espinoza J.
# Repository: https://github.com/jrodriguez88/ciat_tools
# 2019

## Load Packages
library(tidyverse)
library(curl)
library(lubridate)
library(jsonlite)
library(naniar)
library(tictoc)

#Set arguments. Information about params and data access : https://power.larc.nasa.gov/docs/v1/
path <- getwd()
params <- c("PRECTOT" , 
            "ALLSKY_SFC_SW_DWN", 
            "RH2M",
            "T2M",
            "T2M_MAX",
            "T2M_MIN",
            "WS2M",
            "KT_CLEAR")
ini_date <- 19810101
end_date <- 20100101
lat <- 3.5
lon <- -76.3

## `get_data_nasapower()`function: Download weather data from https://power.larc.nasa.gov/
get_data_nasapower <- function(path, params, ini_date, end_date, lat, lon){
    
link <- paste0("https://power.larc.nasa.gov/cgi-bin/v1/DataAccess.py?request=execute&identifier=SinglePoint&parameters=",
           paste0(params, collapse = ","),"&startDate=", ini_date, "&endDate=", end_date,
           "&userCommunity=SSE&tempAverage=DAILY&outputList=ASCII&lat=",
           lat,"&lon=", lon, "&user=anonymous")

json_data <- fromJSON(link)
    
data <- json_data$features$properties$parameter 

map(data, ~gather(., "date", "value")) %>% 
    bind_rows(.id = "var") %>%
    spread(var, value) %>%
    mutate(date = ymd(date))
    
}

# Usage and time-test
tic()
data <- get_data_nasapower(params, ini_date, end_date, lat, lon)
toc()

## Plot data example
data %>% ggplot(aes(date, PRECTOT)) + geom_line()
    
## Replace "NA" id and export to .csv
data %>% replace_with_na_all(condition = ~.x == -999) %>% write_csv("data.csv")
