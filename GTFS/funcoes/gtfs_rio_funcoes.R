library(jsonlite)
library(httr)
library(dplyr)
library(data.table)
library(sf)

setwd("D:/gtfs_rio")

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
  saveRDS(dado_sigmob,paste0("./arquivos_brutos/",variavel_sigmob,".RDS"))
  assign(variavel_sigmob, dado_sigmob,.GlobalEnv)
  a <- round(Sys.time()-T1,2)
  result <- print(paste0("Tarefa executada em ", a, " ", units(a), "."))
}

filtrarStops <- function(stops,stop_times){
  stops_used <- as.character(unique(stop_times$stop_id))
  parent_stations_used <- stops$parent_station[stops$stop_id %in% stops_used]
  stops_to_filter <- c(stops_used,parent_stations_used)
  stops_to_filter <- unique(stops_to_filter)

  stops <<- stops %>%
    filter(stop_id %in% stops_to_filter)
}

calcularStopTimes <- function(id_trips){

  ### Remove campos do processamento anterior. Limpa a memoria.
  ### Mostra id da viagem em processamento.

  gc()
  print(id_trips)
  browser()
  print(Sys.time()-T2)
  T2 <- Sys.time()

  ### Seleciona sequencia de paradas para processamento atraves do trip_id.
  ### Zera shape_dist_traveled e dist_shape.
  ### (dist_shape: distancia entre o ponto de parada e o shape da viagem.
  ### Util para verificar qualidade do metodo, problemas no shape ou nas paradas.)

  paradas_to_proc <- paradas[paradas$trip_id %like% id_trips,]
  paradas_to_proc$shape_dist_traveled <- NA
  paradas_to_proc$dist_shape <- 0

  ### a: seleciona shapes referentes a viagem.

  a <- shapes[shapes$shape_id==unique(paradas_to_proc$trip_id),]

  ### b: transforma shapes em linestring.
  ### Reprojeta para sistema de coordenadas que e requisito para funcao executada posteriormente.

  b <- a %>%
    arrange(shape_pt_sequence) %>%
    summarise(do_union = FALSE) %>%
    st_cast("LINESTRING")
  b <- st_transform(b,32723)

  ### c: cria um ponto a cada metro do shape da viagem.

  c <- st_line_sample(b, density = 1/1) %>%
    st_sf() %>%
    st_cast('POINT') %>%
    mutate(dist = 1:n(),dist = dist-1)

  ### g: Busca o ponto c mais proximo de cada parada da viagem.
  ### Converte g em data table e renomeia as colunas.
  ### Converte shape dist traveled em variavel numerica.
  ### (Como e gerado um ponto a cada metro do shape, o shape_dist_traveled e igual
  ### ao numero do ponto c.)

  g <- nngeo::st_nn(st_transform(paradas_to_proc,32723),c, k = 1, returnDist = T)
  g <- as.data.table(g)
  colnames(g) <- c("shape_dist_traveled","dist_shape")
  g$shape_dist_traveled <- as.integer(g$shape_dist_traveled)
  g$dist_shape <- round(as.double(g$dist_shape),2)

  ### Calcula dist. E a distancia entre o ponto de parada e o ponto anterior,
  ### sobre o shape da viagem. Define que NA, resultado no primeiro ponto de parada,
  ### recebe 0.

  g$dist <- g$shape_dist_traveled-lag(g$shape_dist_traveled)
  g <- g %>%
    replace(is.na(.), 0)

  ### Se nao houver dist negativo, ou seja, se todos os pontos estiverem
  ### a frente do ponto anterior, tudo esta certo.
  ### Neste caso, shape_dist_traveled e dist_shape sao preenchidos e o processo
  ### se encerra, avancando para a proxima viagem.

  if (nrow(g[g$dist<0,]) == 0){
    paradas_to_proc$shape_dist_traveled <- g$shape_dist_traveled
    paradas_to_proc$dist_shape <- g$dist_shape
    substituirShapes(c,g,shapes,viagem)
    ### Se houver dist negativo, avancar para proxima etapa.
  } else {

    ### Criar campo parada, para obter ordem das paradas.

    g <- g %>% mutate(parada = 1:n())

    ### Criar objeto m, para verificar quantas paradas estao a frente de uma parada posterior.

    m <- g[lead(g$dist<0,)]

    ### Se houver apenas uma parada a frente de uma parada posterior,
    if (nrow(m) == 1){
      print(id_trips)

      ### Verificar se ela e a ultima.

      if (tail(g$parada,1)==g$parada[g$dist<0]){

        ### Criar objeto h, pegando ultima parada.

        h <- tail(g,1)

        ### Separar ultima parada das demais.

        l <- anti_join(g,h)

        ### Zerar distancia para o shape desta parada.

        h$dist_shape <- 0

        ### Informar que esta se usando este metodo para o caso de ultima parada
        ### e adicionar o id da trip na lista.

        print("Metodo 1 - Ultimo")
        linhas_metodo1_ultimo <- c(linhas_metodo1_ultimo,id_trips)

        ### Criar objeto d, pegando o fim do shape.

        d <- tail(c,1)

        ### Usar distancia do ultimo ponto como shape_dist_traveled da ultima parada,
        ### e calcular distancia entre a ultima parada e o ultimo ponto do shape.
        ### (Util para avaliar se o metodo funcionou bem ou se as distancias ficaram muito grandes).
        h$shape_dist_traveled <- d$dist
        h$dist_shape <- round(as.double(st_distance(st_transform(tail(paradas_to_proc,1), 32723), d)),2)
        h$dist_shape <- round(as.double(h$dist_shape),2)

        ### Recriar o arquivo g, associando as demais paradas a ultima parada,
        ### e reprocessando o campo dist entre a parada e a parada anterior.
        g <- bind_rows(h,l) %>%
          arrange(parada) %>%
          mutate(dist = shape_dist_traveled-lag(shape_dist_traveled)) %>%
          replace(is.na(.), 0)
      }

      ### Verificar se ela e a primeira.

      if (m$parada == 1){

        ### Criar objeto h, pegando primeira parada.
        h <- g[lead(g$dist<0,)]

        ### Separar primeira parada das demais.

        l <- anti_join(g,h)

        ### Zerar distancia para o shape desta parada.

        h$dist_shape <- 0

        ### Informar que esta se usando este metodo para o caso de primeira parada
        ### e adicionar o id da trip na lista.

        print("Metodo 1 - Primeiro")
        linhas_metodo1_primeiro <- c(linhas_metodo1_ultimo,id_trips)

        ### Se a distancia entre a primeira parada e o primeiro no do shape for
        ### menor que a distancia para o segundo no do shape, assumir que o no do shape
        ### de primeira parada e o no da primeira parada.
        ### Se nao, pegar o no do shape mais proximo da primeira parada.

        if (st_distance(st_transform(paradas_to_proc[1,], 32723), c[1,])<st_distance(st_transform(paradas_to_proc[1,], 32723), c[2,])){
          d <- c[c$dist==0,]
        } else {
          d <- c[order(st_distance(st_transform(paradas_to_proc[1,], 32723), c)),] %>%
            mutate(posicoes = 1:n()) %>%
            slice_head(n = 1)
        }

        ### Usar distancia do primeiro no do shape ou no mais proximo, conforme resultado do if acima.
        ### Calcular distancia entre a primeira parada e o ponto do shape selecionado.
        ### (Util para avaliar se o metodo funcionou bem ou se as distancias ficaram muito grandes).
        h$shape_dist_traveled <- d$dist
        h$dist_shape <- round(as.double(st_distance(st_transform(paradas_to_proc[1,], 32723), d)),2)
        h$dist_shape <- round(as.double(h$dist_shape),2)

        ### Recriar o arquivo g, associando a primeira parada as demais paradas
        ### e reprocessando o campo dist entre a parada e a parada anterior.

        g <- bind_rows(h,l) %>%
          arrange(parada) %>%
          mutate(dist = shape_dist_traveled-lag(shape_dist_traveled)) %>%
          replace(is.na(.), 0)
      }
    }

    ### Se nao houver dist negativo, ou seja, se todos os pontos estiverem
    ### a frente do ponto anterior, o metodo 1 funcionou.
    ### Neste caso, shape_dist_traveled e dist_shape sao preenchidos e o processo
    ### se encerra, avancando para a proxima viagem.

    if (nrow(g[g$dist<0,]) == 0){
      paradas_to_proc$shape_dist_traveled <- g$shape_dist_traveled
      paradas_to_proc$dist_shape <- g$dist_shape
      substituirShapes(c,g,shapes,viagem)
    } else {

      ### Caso contrario, inicia-se o metodo 2.

      linhas_metodo2 <- c(linhas_metodo2,id_trips)
      paradas_para_rodar <- paradas_to_proc$stop_sequence

      sapply(paradas_para_rodar,metodoDois,paradas_to_proc,c)
      c <- st_line_sample(b, density = 1/1) %>%
        st_sf() %>%
        st_cast('POINT') %>%
        mutate(dist = 1:n())
      substituirShapes(c,g,shapes,viagem)
    }

  }

  ### Adiciona paradas da trip ao dataframe com as paradas das viagens ja processadas.
  paradas_finished <<- bind_rows(paradas_finished,paradas_to_proc)
  j <- j+1
}

metodoDois <- function(paradas_para_rodar,paradas_to_proc,c){

  ### Caso o tryCatch fique ativo, pode haver shape_dist_traveled == NA no fim da execucao.
  ### Fazer teste para validar o resultado. Se ele for inativado, corre o risco de quebrar
  ### a execucao muitas vezes, exigindo maior atencao do operador para acompanhar execucao
  ### do codigo.
  tryCatch({

    ### Mostrar que esta sendo usado o metodo 2, mostrar a trip processada e a ordem da parada.

    print("Metodo 2")
    print(id_trips)
    print(paradas_para_rodar)


    if (paradas_to_proc[paradas_para_rodar,]$stop_sequence == 1){

      ### Se a distancia entre a primeira parada e o primeiro no do shape for
      ### menor que a distancia para o segundo no do shape, assumir que o no do shape
      ### de primeira parada e o no da primeira parada.
      ### Se nao, pegar o no do shape mais proximo da primeira parada.

      if (st_distance(st_transform(paradas_to_proc[paradas_para_rodar,], 32723), c[1,])<
          st_distance(st_transform(paradas_to_proc[paradas_para_rodar,], 32723), c[2,])){

        d <- c[c$dist==1,]

      } else {
        d <- c[order(st_distance(st_transform(paradas_to_proc[paradas_para_rodar,], 32723), c)),] %>%
          ### Cria um campo posicoes para saber a ordem de proximidade do no
          ### em relacao a parada (1 e o mais proximo, 2 e o segundo mais proximo...)
          mutate(posicoes = 1:n()) %>%
          ### Seleciona os tres nos mais proximos.
          filter(posicoes < 4) %>%
          ### Ordena pelo campo dist (distancia entre primeiro no do shape e o no atual)
          ### Ou seja, da prioridade aos nos no inicio do shape.
          arrange(dist) %>%
          ### Cria campo dist_rel, distancia do no em relacao ao no anterior (ordenado pela
          ### distancia em relacao ao inicio).
          mutate(dist_rel = dist-lag(dist)) %>%
          ### Troca NA por 0, para o caso do primeiro no.
          replace(is.na(.), 0) %>%
          ### Remove aqueles com dist_rel maior que 3. Ou seja, caso os nos mais proximos
          ### de uma parada sejam o 1500, o 1501 e o 29000, o 29000 sera removido porque
          ### a dist_rel dele sera 29000 - 1501 = 27499.
          filter(dist_rel < 4) %>%
          ### Ordena valores restantes pelas posicoes em relacao a proximidade com parada.
          ### Assim, caso o no mais proximo seja o 1501, ele entra na frente do 1500.
          arrange(posicoes) %>%
          ### Seleciona o no mais proximo.
          slice_head(n = 1)
      }

      paradas_to_proc[paradas_para_rodar,]$shape_dist_traveled <- d$dist

      paradas_to_proc[paradas_para_rodar,]$dist_shape <-
        round(as.double(st_distance(st_transform(paradas_to_proc[paradas_para_rodar,], 32723), d)),2)

      d <- d %>%
        rename(shape_dist_traveled = dist) %>%
        mutate(dist_shape =
                 round(
                   as.double(
                     st_distance(st_transform(paradas_to_proc[paradas_para_rodar,], 32723), d)
                   )
                   ,2),
               dist = 0,
               parada = paradas_to_proc[paradas_para_rodar,]$stop_sequence)

      d <- as.data.table(d)
      d <- d %>% select(shape_dist_traveled,dist_shape,dist,parada)
      g <- g[0]

      g <<- bind_rows(g,d)

    } else {

      ### c: recebe os nos do shape (1 a 1 metro) com ordem maior
      ### que o no associado a parada anterior.

      c <- c[c$dist>paradas_to_proc[paradas_para_rodar-1,]$shape_dist_traveled,]

      ### d: Ordena cada no do shape em relacao a distancia com a parada em questao.

      d <- c[order(st_distance(st_transform(paradas_to_proc[paradas_para_rodar,], 32723), c)),] %>%
        ### Cria um campo posicoes para saber a ordem de proximidade do no
        ### em relacao a parada (1 e o mais proximo, 2 e o segundo mais proximo...)
        mutate(posicoes = 1:n()) %>%
        ### Seleciona os tres nos mais proximos.
        filter(posicoes < 4) %>%
        ### Ordena pelo campo dist (distancia entre primeiro no do shape e o no atual)
        ### Ou seja, da prioridade aos nos no inicio do shape.
        arrange(dist) %>%
        ### Cria campo dist_rel, distancia do no em relacao ao no anterior (ordenado pela
        ### distancia em relacao ao inicio).
        mutate(dist_rel = dist-lag(dist)) %>%
        ### Troca NA por 0, para o caso do primeiro no.
        replace(is.na(.), 0) %>%
        ### Remove aqueles com dist_rel maior que 3. Ou seja, caso os nos mais proximos
        ### de uma parada sejam o 1500, o 1501 e o 29000, o 29000 sera removido porque
        ### a dist_rel dele sera 29000 - 1501 = 27499.
        filter(dist_rel < 4) %>%
        ### Ordena valores restantes pelas posicoes em relacao a proximidade com parada.
        ### Assim, caso o no mais proximo seja o 1501, ele entra na frente do 1500.
        arrange(posicoes) %>%
        ### Seleciona o no mais proximo.
        slice_head(n = 1)

      ### Associa os valores de d a parada relacionada. Remove d.
      paradas_to_proc[paradas_para_rodar,]$shape_dist_traveled <- d$dist
      paradas_to_proc[paradas_para_rodar,]$dist_shape <-
        round(as.double(st_distance(st_transform(paradas_to_proc[paradas_para_rodar,], 32723), d)),2)
      d <- d %>%
        rename(shape_dist_traveled = dist) %>%
        mutate(dist_shape =
                 round(
                   as.double(
                     st_distance(st_transform(paradas_to_proc[paradas_para_rodar,], 32723), d)
                   )
                   ,2),
               dist = 0,
               parada = paradas_to_proc[paradas_para_rodar,]$stop_sequence)
      d <- as.data.table(d)
      d <- d %>% select(shape_dist_traveled,dist_shape,dist,parada)
      g <<- bind_rows(g,d)
      g$dist <<- g$shape_dist_traveled-lag(g$shape_dist_traveled)
      g <<- g %>%
        replace(is.na(.), 0)
    }
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
}

substituirShapes <- function(c,g){
  shapes_adicionais <- c[c$dist %in% g$shape_dist_traveled,]
  c <- st_transform(c,4326)
  shapes_adicionais <- st_transform(shapes_adicionais,4326)
  c$dir <- round(calc_direction(c),2)
  q <- c %>%
    filter(lag(dir)!=dir | c$dist == 1 | c$dist==nrow(c)) %>%
    select(geometry,dist)
  q <- bind_rows(q,shapes_adicionais)
  q <- q %>%
    rename(shape_dist_traveled = dist) %>%
    mutate(shape_id = trips$shape_id[trips$trip_id == id_trips[j]]) %>%
    distinct(shape_dist_traveled, .keep_all = T) %>%
    arrange(shape_dist_traveled) %>%
    mutate(shape_pt_sequence = 1:n()) %>%
    mutate(shape_pt_lat = sf::st_coordinates(.)[,2],
           shape_pt_lon = sf::st_coordinates(.)[,1]) %>%
    select(shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled) %>%
    filter(shape_dist_traveled >= g[1]$shape_dist_traveled & shape_dist_traveled <= g[nrow(g)]$shape_dist_traveled) %>%
    mutate(shape_dist_traveled = shape_dist_traveled-1) %>%
    mutate(across(c(1:3,5), as.character))

  shapes <<- shapes %>%
    filter(shape_id != trips$shape_id[trips$trip_id == id_trips[j]],) %>%
    bind_rows(.,q)
}

