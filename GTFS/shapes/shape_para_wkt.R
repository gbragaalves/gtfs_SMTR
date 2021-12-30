library(sf)
library(dplyr)
library(spatialEco)
library(data.table)
library(PAR)

setwd("D:/gtfs_rio")

source("./codigos/gtfs_rio_funcoes.R")

pegarDadoSIGMOB("trips")

shps_full <- list.files("D:/WKTs/shapes_para_correcao/", full.names = T, pattern = "*.gpx")
shps <- list.files("D:/WKTs/shapes_para_correcao/", pattern = "*.gpx")

for (i in 1:length(shps)){
  # Linha <- st_read(shps_full[i], layer = "tracks")
  # Linha <- Linha %>% select(geometry)
  # Linha <- st_line_merge(Linha)
  # Linha$shape_id <- shps[i]
  # shape <- as.character(shps[i])
  # Linha <- st_as_sf(Linha)
  # b <- st_transform(Linha,32723)
  # c <- st_line_sample(b, density = 1/1) %>%
  #   st_sf() %>%
  #   st_cast('POINT') %>%
  #   mutate(dist = 1:n())
  #
  #
  # c <- st_transform(c,4326)
  # c$dir <- round(calc_direction(c),2)
  # q <- c %>%
  #   filter(lag(dir)!=dir | c$dist == 1 | c$dist==nrow(c)) %>%
  #   select(geometry,dist)
  # q <- q %>%
  #   rename(shape_dist_traveled = dist) %>%
  #   mutate(shape_id = gsub('.gpx', '', shps[i])) %>%
  #   distinct(shape_dist_traveled, .keep_all = T) %>%
  #   arrange(shape_dist_traveled) %>%
  #   mutate(shape_pt_sequence = 1:n()) %>%
  #   mutate(shape_pt_lat = sf::st_coordinates(.)[,2],
  #          shape_pt_lon = sf::st_coordinates(.)[,1]) %>%
  #   select(shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled) %>%
  #   mutate(shape_dist_traveled = shape_dist_traveled-1) %>%
  #   mutate(across(c(1:3,5), as.character))
  #
  #
  # linha_ver <- q
  #
  # linha_ver <- linha_ver %>%
  #   left_join(select(trips, trip_id, id), by = c("shape_id" = "trip_id")) %>%
  #   rename(TripID = id)
  #
  # shape_SIG <- linha_ver
  #
  # rm(b,c,q)
  # linha_ver$shape_id <- paste0("('", linha_ver$shape_id, "',")
  # linha_ver$shape_pt_lon <- paste0("'", linha_ver$shape_pt_lon, "',")
  # linha_ver$shape_pt_lat <- paste0("'", linha_ver$shape_pt_lat, "',")
  # linha_ver$shape_pt_sequence <- paste0("'", linha_ver$shape_pt_sequence, "',")
  # if (is.na(linha_ver$TripID)){
  #   linha_ver$shape_dist_traveled <- paste0("'", linha_ver$shape_dist_traveled, "');")
  # } else {
  #   linha_ver$shape_dist_traveled <- paste0("'", linha_ver$shape_dist_traveled, "',")
  #   linha_ver$TripID <- paste0("'", linha_ver$TripID, "');")
  # }
  #

  intro_del <- paste0("DELETE FROM shapes WHERE shape_id ='",gsub(".gpx","",shps[i]),"';")
  # if (is.na(linha_ver$TripID)){
  #   intro <- "INSERT INTO shapes(shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled) VALUES "
  # } else {
  #   intro <- "INSERT INTO shapes(shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled,TripID) VALUES "
  # }
  # linha_ver <- cbind(intro,linha_ver)
  #
  # linha_ver <- as.data.table(linha_ver)
  #
  # if (is.na(linha_ver$TripID)){
  #   linha_ver <- linha_ver %>% select(intro,shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled)
  # } else {
  #   linha_ver <- linha_ver %>% select(intro,shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled,TripID)
  # }
  #
  # cols <- colnames(linha_ver)
  #
  # linha_ver$texto <- apply( linha_ver[ , ..cols ] , 1 , paste , collapse = "" )
  # linha_ver <- as.data.table(linha_ver$texto)
  #
  # if (exists("geral")){
  #   geral <- rbind(geral,linha_ver)
  # } else {
  #   geral <- linha_ver
  # }
  #
  if (exists("del")){
    del <- rbind(del,intro_del)
  } else {
    del <- intro_del
  }
  #
  # if (exists("shapes_SIG")){
  #   shapes_SIG <- rbind(shapes_SIG,shape_SIG)
  # } else {
  #   shapes_SIG <- shape_SIG
  # }
  #
  # rm(linha_ver,Linha,shape_SIG,cols,intro)
}

write.table(geral, paste0("D:/WKTs/shapes_para_correcao/geral.txt"), row.names = F, col.names = F, quote = F)

geral_split <- split(geral,rep(1:ceiling(nrow(geral)/3000),each=3000))

for(i in 1:length(geral_split)){
  write.table(geral_split[[i]], paste0("D:/WKTs/shapes_para_correcao/geral-",i, ".txt"), row.names = F, col.names = F, quote = F)
}
write.table(del, paste0("D:/WKTs/shapes_para_correcao/del.txt"), row.names = F, col.names = F, quote = F)

rm(geral,geral_split,del)
