library(data.table)
library(dplyr)

set_wd("D:/")


freq_bruta <- fread("./frequencia_bruta.csv",encoding = "UTF-8") %>%
  #filter(route_id == "???") %>%
  select(route_id,MODELO_OPERACAO,OPERACAO_ESPELHADA,QUADRO_HORARIO,TEMPO_DE_CICLO_ESTIMADO,HrTransac,Intervalo)

colnames(freq_bruta) <- c("route_id","modelo","espelhada","qd_horario","tempo_ciclo","hora","intervalo")

freq_bruta <- freq_bruta %>%
  mutate(hora = paste0(hora,":00"),
         start_time = as.ITime(hora),
         end_time = as.ITime(hora)+3600,
         viagens = (as.numeric(end_time)-as.numeric(start_time)) / intervalo,
         inteiro = (viagens - as.integer(viagens)),
         inteiro = ifelse(inteiro==0,TRUE,FALSE))

### Colapsa frequencias caso elas se repitam em mais de uma hora.
### Loop de 48 vezes é um exagero preguicoso, mas nao atrapalha execucao
### do codigo.

for (i in 1:48){
  freq_bruta <- freq_bruta %>%
    group_by(route_id) %>%
    mutate(end_time = as.ITime(ifelse(lead(intervalo)==intervalo & !is.na(lead(intervalo)),lead(end_time),end_time)))
}

freq_bruta <- freq_bruta %>%
  group_by(route_id) %>%
  filter(end_time != lag(end_time) | is.na(lag(end_time)))

### Verifica se a quantidade de viagens naquele intervalo eh um numero inteiro.

freq_bruta <- freq_bruta %>%
  mutate(viagens = (as.numeric(end_time)-as.numeric(start_time)) / intervalo,
         inteiro = (viagens - as.integer(viagens)),
         inteiro = ifelse(inteiro==0,TRUE,FALSE),
         minutos = as.numeric(end_time-start_time)/60)

for (i in 1:48){

  ### Se qt_viagens nao for int, ajusta fim do periodo.
  ### Se headway posterior for menor, suprime fracao de viagem e adianta headway posterior.
  ### Se headway posterior for maior, acrescenta fracao de viagem e atrasa headway posterior.

  freq_bruta <- freq_bruta %>%
    group_by(route_id) %>%
    mutate(end_time=
             as.ITime(
               ifelse(
                 inteiro,end_time,
                 ifelse(lead(intervalo)<intervalo | is.na(lead(intervalo)),
                        as.numeric(start_time)+as.integer(minutos/(intervalo/60))*intervalo,
                        as.numeric(start_time)+as.integer(minutos/(intervalo/60)+1)*intervalo))))

  ### Modifica start_time apos ajustes no fim do periodo.

  freq_bruta <- freq_bruta %>%
    group_by(route_id) %>%
    mutate(start_time=as.ITime(ifelse(!is.na(lag(end_time)),lag(end_time),start_time)))

  ### Verifica se resultado se tornou inteiro. Se nao, repete procedimento ate ajustar.

  freq_bruta <- freq_bruta %>%
    group_by(route_id) %>%
    mutate(viagens = (as.numeric(end_time)-as.numeric(start_time)) / intervalo,
           inteiro = (viagens - as.integer(viagens)),
           inteiro = ifelse(inteiro==0,TRUE,FALSE),
           minutos = as.numeric(end_time-start_time)/60)
}

frequencies <- freq_bruta %>%
  select(route_id,start_time,end_time,intervalo) %>%
  rename(headway_secs = intervalo)

fwrite(frequencies,"./rascunho_freq_Intersul.csv")


### to-do: verificar casos de servico noturno, ainda com erros.
### to-do: ajustar codigo para lidar com trips e nao routes.
