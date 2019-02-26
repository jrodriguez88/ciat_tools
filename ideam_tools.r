
get_ideam_catalog <- function(url, spatial = FALSE) {
  
  encoding <- readr::guess_encoding(url) %>%
    dplyr::top_n(1) %>%
    dplyr::pull(encoding)
  
  if(isTRUE(spatial)){
    
    x = readr::read_csv(url, locale = locale(encoding = encoding)) %>%
      mutate(init_date = dmy(str_sub(Fecha_instalacion, 1,10)),
             stop_date = dmy(str_sub(Fecha_suspension, 1,10)),
             fdate = if_else(is.na(stop_date), Sys.Date(), stop_date),
             years_working = time_length(fdate-init_date, "years"),
             coord = stringr::str_remove_all(`Ubicación`, "[(|)]"),
             Estado = as_factor(Estado)) %>%
      tidyr::separate(coord, into = c("long", "lat"), sep = ",",  convert = T) %>%
      sf::st_as_sf(coords = c("lat", "long")) %>%
      st_set_crs( "+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
    
  }else{
    
    x = readr::read_csv(url, locale = locale(encoding = encoding)) %>%
      mutate(init_date = dmy(str_sub(Fecha_instalacion, 1,10)),
             stop_date = dmy(str_sub(Fecha_suspension, 1,10)),
             fdate = if_else(is.na(stop_date), Sys.Date(), stop_date),
             years_working = time_length(fdate-init_date, "years"),
             coord = stringr::str_remove_all(`Ubicación`, "[(|)]"),
             Estado = as_factor(Estado)) %>%
      tidyr::separate(coord, into = c("long", "lat"), sep = ",",  convert = T)
    
  }
  
  return(x)
  
}



get_ideam_ciat <- function(file, spatial = FALSE){
  
  if(isTRUE(spatial)){
    
    encoding <- readr::guess_encoding(file) %>%
      dplyr::top_n(1) %>%
      dplyr::pull(encoding)
    
    x <- readr::read_csv(file = file, locale = locale(encoding = encoding)) %>%
      mutate(LongitudeDD = as.numeric(LongitudeDD), 
             LatitudeDD = as.numeric(LatitudeDD)) %>%
      sf::st_as_sf(coords = c("LongitudeDD", "LatitudeDD")) %>%
      st_set_crs( "+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
    
  }else{
    
    encoding <- readr::guess_encoding(file) %>%
      dplyr::top_n(1) %>%
      dplyr::pull(encoding)
    
    x <- readr::read_csv(file = file, locale = locale(encoding = encoding),
                         col_types = cols(
                         `Start Date` = col_date(format = "%Y%m%d"),
                         `End Date`= col_date(format = "%Y%m%d"))) %>%
      filter(!is.na(LongitudeDD), !is.na(LatitudeDD)) 
   
    
  }
  
}


get_path <- function(folder, vars){
  
  list_folder <- dir_ls(folder) %>%
    tibble(folder = .) %>%
    filter(str_detect(folder, glue_collapse(glue('{vars}'), sep = "|"))) %>%
    pull(folder)
  
  return(list_folder)
  
}


get_daily_raw <- function(cod, path){
  
    z = fs::dir_ls(fs::path(path), regexp = cod, recursive = TRUE) %>%
    as.character() %>%
    tibble(path_raw = .) %>%
    mutate(variable = str_extract(basename(path_raw), pattern = "(?<=raw_)(.*\n?)(?=.txt)")) %>%
    filter(!is.na(variable))
  
  return(z)
  
}

read_raw <- function(file, variable){
  
  x = readr::read_delim(file, delim ="\t", col_types = cols(
    Date = col_date(format = "%Y%m%d"),
    Value = col_double())) %>%
    dplyr::mutate(type = variable,
                  day = lubridate::day(Date),
                  month = lubridate::month(Date), 
                  year = lubridate::year(Date))
  
  return(x)
} 

read_weather <- function(x){
 
  x %>%
  mutate(data = purrr::map2(.x  = path_raw,
                            .y = variable, 
                            .f = read_raw)) %>%
    dplyr::select(data) %>%
    unnest() %>%
    spread(type, Value)
  
}

miss_values <- function(x){

  x %>%
    dplyr::select(one_of(c("prec", "tmax", "tmin"))) %>%
    miss_var_summary() %>%
    summarise(avg_na = mean(pct_miss)) %>%
    dplyr::pull()

}

date_values <- function(x){
  
  
  tibble::tibble(init_date = dplyr::first(x$Date),
                 last_date = dplyr::last(x$Date))
 
  
}

get_ideam_files <- function(ideam_catalogo, vars, tipo, raw_path, parallel = T){
  
  x <- get_ideam_ciat(ideam_catalogo) 
  
  x <- filter(x, Category %in% tipo, Variable %in% vars) %>%
    dplyr::select(-Variable) %>%
    distinct(Code, .keep_all = T)
  
  path_vars <- get_path(raw_path, vars)
  
  
  if(isTRUE(parallel)){
    
    plan(multiprocess)
    options(future.globals.maxSize= 891289600)
    x <- x %>%
      mutate(raw_file = furrr::future_map(.x  = Code,
                                              .f = get_daily_raw,
                                              path = raw_path))
    
    x <- x %>%
      mutate(data = furrr::future_map(.x  = raw_file,
                                       .f = read_weather)) # %>%
    gc()
    gc(reset = T)
    
    x <- x %>%
      mutate(percent_na = furrr::future_map_dbl(.x = data,
                                            .f = miss_values), 
             range_date = furrr::future_map(.x = data,
                                            .f = date_values)) %>%
      unnest(range_date) 
       
    gc()
    gc(reset = T)
    return(x)
       
  }else{
    
    
  }
  
  
}
