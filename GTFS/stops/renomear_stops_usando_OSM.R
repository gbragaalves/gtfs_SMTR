library(data.table)
library(dplyr)
library(sf)


setwd("D:/gtfs_rio")

source("./codigos/gtfs_rio_funcoes.R")

pegarDadoSIGMOB("stops")

bairro <- "Botafogo"

stops_bairro <- stops %>%
  filter(Bairro == bairro) %>%
  select(stop_id,stop_name,nome_ponto,BRS,stop_lat,stop_lon,IDTipoParada,IDPropriedadeParada) %>%
  rename(name = nome_ponto)

stops_bairro_geo <- st_as_sf(stops_bairro,coords=c("stop_lon","stop_lat"),crs = 4326, remove = T, na.fail = T) %>%
  mutate(highway = "bus_stop")

end_geo <- paste0("D:/Edificacoes_2013/bairros/",bairro,"/paradas_onibus_",bairro,".geojson")

st_write(stops_bairro_geo,end_geo, append = F)

paradas_proc <- as.data.table(st_read(end_geo)) %>%
  mutate(stop_name = case_when(IDPropriedadeParada == "2" ~ paste0("Ponto Final: ",name),
                               IDPropriedadeParada == "3" ~ paste0("Ponto Regulador: ",name),
                               IDTipoParada == "6" ~ paste0("BRS ",BRS,": ",name),
                               TRUE ~ name)) %>%
  rename(nome_ponto = name) %>%
  mutate(col1 = "UPDATE stops SET nome_ponto = '",
         col2 = "', stop_name = '",
         col3 = "' WHERE stop_id = '",
         col4 = "';") %>%
  select(col1,nome_ponto,col2,stop_name,col3,stop_id,col4)

cols <- colnames(paradas_proc)

paradas_proc$texto <- apply( paradas_proc[ , ..cols ] , 1 , paste , collapse = "" )
paradas_proc <- as.data.table(paradas_proc$texto)

write.table(paradas_proc, paste0("D:/Edificacoes_2013/bairros/",bairro,"/paradas_onibus_",bairro,".txt"), row.names = F, col.names = F, quote = F)

