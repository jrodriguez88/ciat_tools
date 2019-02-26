### Load libraries

library(curl)  ## working with readr (its necessary to update the las version)
library(tidyverse)
library(stringi)
library(lubridate)
library(naniar)
library(sf)
library(fs)
library(glue)
library(furrr)
library(purrr)
library(lubridate)
library(tictoc)


source(file "ideam_tools.R")
# url <- "https://www.datos.gov.co/api/views/hp9r-jxuu/rows.csv?accessType=DOWNLOAD"

catalogo <- "/mnt/data_cluster_4/observed/weather_station/col-ideam/daily-raw/Copy of stations_catalog_daily.csv"  # on linux
# catalogo <- "//dapadfs/data_cluster_4/observed/weather_station/col-ideam/Copy of ideam_general_catalog.csv"      # on Windows

# folder with the IDEAM information (daily raw)
wheater_stations <- '/mnt/data_cluster_4/observed/weather_station/col-ideam/daily-raw'                             # on linux
# wheater_stations <- '//dapadfs/data_cluster_4/observed/weather_station/col-ideam/daily-raw'                      # on Windows


## select by
# CP = Climatologica Principal
# CO = Climatologia Odinaria
# AM = Agrometeorologica
# SP = Sinoptica Principal
# SS = Sinoptica Secundaria
# PM = Pluviometrica

tipo <- c("CP", "CO", "AM", "SP", "SS", "PM")

# 
var_select <- c("prec", "tmax", "tmin", "sbright", "rhum")




tic("Loading IDEAM information")
IDEAM <- get_ideam_files(ideam_catalogo = catalogo, 
                vars = var_select,
                tipo = tipo, 
                raw_path = wheater_stations,
                parallel = T)
toc()
