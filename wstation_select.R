




### Load libraries

library(curl)
library(tidyverse)
library(stringi)
library(lubridate)
library(naniar)

#url <- "https://www.datos.gov.co/api/views/hp9r-jxuu/rows.csv?accessType=DOWNLOAD"
#file <- download.file(url, destfile = paste0(getwd(), "/", "ideam_cat22.csv"))
#
#data <- read.csv("ideam_cat.csv", header = T, stringsAsFactors = F)
#data <- read_csv("ideam_cat.csv")
#
#b <- read_csv(url)
#data <- b

### Load data v1
#load("ws_rice_zones.RData")

#ws_susp <- data %>% filter(Estado=="SUSPENDIDA")
   


# unique(nchar(data$Fecha_instalacion))
# unique(nchar(data$Fecha_suspension))
url <- "https://www.datos.gov.co/api/views/hp9r-jxuu/rows.csv?accessType=DOWNLOAD"

get_ideam_catalog <- function(url) {
    
    stopifnot(require(curl))
    
    read_csv(url) %>%
        mutate(idate = dmy(str_sub(Fecha_instalacion, 1,10)),
               sdate = dmy(str_sub(Fecha_suspension, 1,10)), #select(sdate)
               fdate = case_when(is.na(sdate) ~ Sys.Date(),
                                 TRUE ~ sdate),
               yfunc = time_length(fdate-idate, "years"),
               coor = str_remove_all(`Ubicación`, "[(|)]")) %>%
        separate(coor, into = c("lat", "lon"), sep = ",", convert = T) 
    
}

catalog <- get_ideam_catalog(url) %>%
    mutate(id=Codigo)

## Print data structure
str(catalog)

## Explore Printing variables, filters in data
unique(catalog$Departamento)
unique(catalog$Categoria)
unique(catalog$Areaoperativa)
unique(catalog$Estado)



## Filter data by Departamento, Categoria, suspended date, altitud and total years in use (yfunc)

dpto <- c("CASANARE", "META", "TOLIMA", "HUILA", "CORDOBA", "SANTANDER")

cate <- c("CLIMATOLOGICA PRINCIPAL", "CLIMATOLOGICA ORDINARIA", 
          "AGROMETEOROLOGICA", "SINOPTICA PRINCIPAL", "SINOPTICA SECUNDARIA")

status <- c("ACTIVA", "SUSPENDIDA", "FUERA DE SERVICIO")


## Filter IDEAM catalog 
filter_ideam <- function (catalog, dpto , cate, status = c("ACTIVA"), min_alt = 0, max_alt = 3500, min_year = 1) {
    
    catalog %>% 
        mutate(Departamento = toupper(stri_trans_general(Departamento,"Latin-ASCII")),
               Municipio = toupper(stri_trans_general(Municipio,"Latin-ASCII"))) %>%
        filter(Departamento %in% dpto,
               Categoria %in% cate,
               Estado %in% status,
               Altitud < max_alt,
               Altitud > min_alt,
               yfunc > min_year)
    
    
}

data_filter <- filter_ideam(catalog, dpto, cate, status)




## Test For rice

for_rice <- data_filter %>%
    filter(Departamento != "SANTANDER",
           Altitud <1300)

### read from censo arrocero
#censo <- read.csv("municipios_arroceros.csv", na.strings = " -   ",
#                    header = T, stringsAsFactors = F)
#load("ws_rice_zones.RData")
censo <- readRDS("censo.RDS")
str(censo)


## Join censo 
pot_ws <- censo %>% 
    mutate(Departamento = toupper(stri_trans_general(departamento,"Latin-ASCII")),
           Municipio = toupper(iconv(stri_trans_general(municipio,"Latin-ASCII")))) %>%
    filter(part > 0.09, 
           Departamento %in% dpto) %>%
    inner_join(for_rice)


pot_ws %>%
    ggplot(aes(Departamento)) +
    geom_bar() +
    facet_wrap(Categoria ~., scales = "free_y") +
    theme_bw()+
    scale_y_continuous(
        breaks = 1:100)


#save(data, censo, file = "ws_rice_zones.RData")




## Scan in data_cluster4
path <- "X:/observed/weather_station/col-ideam/daily-raw"

## Set variables to join
var_selec <- c("prec", "tmax", "tmin", "sbright", "rhum")

## Set IDEAM id station 
ideam_id <- pot_ws$id


scan_ciat_db <- function (path, ideam_id, var_selec) {
    
    list_folder <- list.dirs(path, recursive = F)
    pat_finder <- paste0(ideam_id, collapse = "|")
    var_names <- str_extract(list_folder, pattern = "(?<=raw/)(.*\n?)(?=-per)")
    
    
    ## Create a list with file names by var
    list_files_var <- pmap(list(path = list_folder, pattern = pat_finder), list.files)
    names(list_files_var) <- var_names
    
    ## Select station id tha contain all variable c("prec", "tmax", "tmin", "sbright", "rhum") 
    list_id <- map(list_files_var, ~str_extract(.,  "[^_]+")) %>% 
        map(., ~ enframe(., name = NULL)) %>%
        .[var_selec] %>%
        reduce(inner_join, by = "value")
    
    ## List files of select vars
    list_files_select <- pmap(list(
        path = str_subset(list_folder, 
                        paste(var_selec, collapse = "|")),
        pattern = paste0(list_id$value, collapse = "|")),
        list.files)
    
    ## Set list name as path folder
    names(list_files_select) <- str_subset(list_folder, paste(var_selec, collapse = "|"))
    
    return(list_files_select)
    
}

list_files_select <- scan_ciat_db(path, ideam_id, var_selec)

## Read data for each id. Convert to Tibble. This function take a list of path files by var, return a tibble
get_station_data <- function(list_files_select) {
    
    list_files_select %>% 
        map(., ~enframe(., name = NULL)) %>% bind_rows(.id = "path") %>%
        mutate(file = paste0(path, "/", value),
               data = map(file, ~ read_delim(., delim ="\t", col_types = cols(
                   Date = col_date(format = "%Y%m%d"),
                   Value = col_double()))),
               id = as.numeric(str_extract(.$value,  "[^_]+")),
               var = str_extract(file, "(?<=raw/)(.*\n?)(?=-per)")) %>%
        select(id, var, path, data)
    
}

wdata_tb <- get_station_data(list_files_select)

## Function sto summarize daily data from ideam, CIAT_DB
summary_by_year <- function(wdata_tb) {
    
    wdata_tb %>% group_by(year = year(Date)) %>% 
        summarise(mean = mean(Value, na.rm = T), 
                  median = median(Value, na.rm = T),
                  min = min(Value, na.rm = T), 
                  max = max(Value, na.rm = T),
                  sum = sum(Value, na.rm = T), 
                  sd = sd(Value, na.rm = T)) %>% 
        ungroup()
    
    
}
summary_hist <- function(wdata_tb) {
    
    wdata_tb %>% group_by(year = year(Date)) %>% 
        summarize(mean = mean(Value, na.rm = T), 
                  median = median(Value, na.rm = T),
                  min = min(Value, na.rm = T), 
                  max = max(Value, na.rm = T),
                  sum = sum(Value, na.rm = T)) %>% 
        ungroup() %>%
        summarize(mean = mean(mean, na.rm = T), 
                  median = median(median, na.rm = T),
                  min = min(min, na.rm = T), 
                  max = max(max, na.rm = T),
                  sum = mean(sum, na.rm = T))
    
}
        

## getMetrics calculate Na percent, extract time of use (years)
getMetrics <- function (wdata_tb){
    
    wdata_tb %>%
        mutate(idate = map(data, ~ min(.x$Date)) %>% do.call("c", .),
               fdate = map(data, ~ max(.x$Date)) %>% do.call("c", .),
               years = time_length(fdate-idate, "years"),
               na_percent = map(data, ~ pct_miss(.x$Value)) %>% flatten_dbl(),
               summary = map(data, ~summary_hist(.) %>% reduce(paste, sep=","))) %>%
        separate(summary, into = c("mean", "median", "min", "max", "sum"), sep = ",", convert = T) %>%
        select(-c(path, data))
    
}



test_data <- getMetrics(wdata_tb)


test_data %>%
    ggplot(aes(factor(id), years)) + 
    geom_point() +
    facet_grid(var ~.) + 
    theme_bw()+
    theme(axis.text.x = element_text(angle = 90, hjust = 1))

test_data %>% filter(na_percent>20) %>% left_join(catalog[c("id", "Departamento")], by = c("id")) %>%
    ggplot(aes(factor(id), na_percent)) + 
    geom_point() +
    facet_grid(var ~ Departamento, scales = "free") + 
    theme_bw()+
    theme(axis.text.x = element_text(angle = 90, hjust = 1))



