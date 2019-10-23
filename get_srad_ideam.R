## Script to convert sunshine hours ( or temperatures) to solar radiation
# https://github.com/jrodriguez88/
# Author: Rodriguez-Espinoza J.
# 2019

## Load Packages
library(tidyverse)
library(lubridate)
library(sirad)

data <- read_csv("test_ideam.csv")
data %>% 
    group_by(year = year(Date), month = month(Date)) %>%
    summarise(shour = mean(sbright, na.rm = T)) %>% ungroup() %>%
    ggplot(aes(factor(month), shour)) + geom_boxplot() + theme_light()

#max(data$sbright, na.rm = T)

lat <- 2.25
lon <- -75.5
alt <- 1275

# data must have "Date" and 'sbright' (or 'tmax' and 'tmin') variable,
## A & B parameters from FAO, 
#Frère M, Popov GF. 1979. Agrometeorological crop monitoring and forecasting. Plant
#Production Protection Paper 17. Rome: Food and Agricultural Organization.
#64 p.
# kRs adjustment coefficient (0.16.. 0.19) -- for interior (kRs = 0.16) and coastal (kRs = 0.19) regions
# Ese ultimo parametro toca machetearlo...para colombia toca poner un kRs mas bajo que 0.145 para interior y 0.17 para costa
# Concluyo eso despues de probar varias localidades, para huila usar 0.145 o 0.15


get_srad_ideam <- function(data, lat, lon, alt, A = 0.29, B = 0.45, kRs = 0.175){
    
    stopifnot(require(sirad))
    
    if(!"sbright" %in% colnames(data))
    {
        data <- mutate(data, sbright = NA_real_)   #Aqui crea la variable brillo por si no existe
    }
    
    step1 <- data %>% 
        mutate(
            extraT = extrat(lubridate::yday(Date), radians(lat))$ExtraTerrestrialSolarRadiationDaily, # Calcula la radiacion extraterrestre
            srad = ap(Date, lat = lat, lon = lon,    # aqui aplica Angstrom-Prescott
                      extraT, A, B, sbright),
            srad = if_else(is.na(srad), kRs*sqrt(tmax - tmin)*extraT, srad))  # Aqui aplica Hargreaves
    
    max_srad <- mean(step1$extraT)*0.80     # calcula el maximo teorico de radiacion
    
    step2 <- step1 %>%    ## aca no me acuerdo, como que macheteo con la mediana
        mutate(
            srad = if_else(srad>max_srad|srad<0|is.na(srad),  median(step1$srad, na.rm = T), srad)) %>%
        dplyr::pull(srad)
    
    return(step2)   # retorna la fuckin radiacion en MJ/m2*dia
    
    
}
    
    
test1 <- data %>%
    mutate(srad = get_srad_ideam(., lat, lon, alt))

test2 <- data %>% dplyr::select(-sbright) %>%
    mutate(srad = get_srad_ideam(., lat, lon, alt, kRs = 0.165))

tibble(date = test1$Date, srad_from_shour = test1$srad, srad_from_temp = test2$srad) %>% 
    group_by(year = year(date), month = month(date)) %>%
    summarise(srad_from_shour  = mean(srad_from_shour, na.rm = T),
              srad_from_temp = mean(srad_from_temp, na.rm = T)) %>% #write.csv("climate_data_monthly.csv")
    ungroup() %>% gather(var, value, -c(year, month))%>%
    ggplot(aes(factor(month), value, fill = var)) + 
    geom_boxplot() + theme_light()
    

    
    