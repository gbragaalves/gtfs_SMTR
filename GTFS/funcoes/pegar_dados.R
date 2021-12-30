### Pega dados do SIGMOB sem salvar permanentemente.

library(jsonlite)
library(httr)
library(dplyr)
library(data.table)
library(sf)

pegarDadoSIGMOB <- function(variavel_sigmob){
  HaDados <- TRUE
  url <- paste0("http://jeap.rio.rj.gov.br/MOB/get_",variavel_sigmob,".rule?sys=MOB&INDICE=0")
  dado_baixado <- data.table()
  T1 <- Sys.time()

  while(HaDados){
    dado_bruto <- RETRY("GET",url,.encoding = "latin-1",times = 10)
    dado_baixado <- fromJSON(rawToChar(dado_bruto$content),
                             flatten = T)[["data"]]

    url <- fromJSON(rawToChar(dado_bruto$content),
                    flatten = T)[["next"]]

    if(is.null(dado_baixado)){
      dado_baixado <- fromJSON(rawToChar(dado_bruto$content),
                               flatten = T)[["result"]]
    }

    if (exists("dado_sigmob")){
      dado_sigmob <- rbind(dado_sigmob,dado_baixado)
    } else {
      dado_sigmob <- dado_baixado
    }

    if (is.null(url) || url == "EOF"){
      HaDados <- FALSE
    }

  }
  assign(variavel_sigmob, dado_sigmob,.GlobalEnv)
  a <- round(Sys.time()-T1,2)
  result <- print(paste0("Tarefa executada em ", a, " ", units(a), "."))
}
