# ETL Commodity

## Descrição

Este projeto realiza o processo ETL para dados do indicador de preço do Boi Gordo CEPEA, corrigindo os valores pela inflação utilizando o IPCA (Índice Nacional de Preços ao Consumidor Amplo). O pipeline lê um arquivo Excel com dados históricos, obtém a inflação mensal via API do Banco Central, aplica o ajuste dos valores e salva os dados corrigidos em formatos CSV e Parquet para análises futuras.

## Funcionalidades

- Leitura e tratamento do arquivo Excel original com dados do CEPEA;
- Consulta à API do Banco Central para obter a série histórica do IPCA no intervalo de datas do dataset;
- Cálculo do valor real corrigido pela inflação acumulada do IPCA;
- Cálculo da variação percentual mensal do preço corrigido;
- Upsert no arquivo CSV base, mantendo histórico atualizado;
- Exportação dos dados tratados em arquivo Parquet otimizado para análises;
- Logging detalhado para acompanhamento do processo ETL.

## Requisitos

- Python 3.8 ou superior
- Bibliotecas Python:
  - pandas;
  - pyarrow (para salvar em Parquet).
- Conexão com internet para requisição da API do Banco Central

## Instalação

1. Clone este repositório:
   ```bash
   git clone https://github.com/Anacaloi/desafioCmdty.git
   cd desafioCmdty
   ```

## Dicionário de Dados

Campos após ETL:
dt_cmdty: data do commodity;
nome_cmdty: Boi_Gordo;
tipo_cmdty: Indicador do Boi Gordo CEPEA/B3;
cmdty_um: 15 Kg/carcaça;
cmdty_vl_rs_um: valor real do commodity;
cmdty_var_mes_perc: valor do cálculo realizado de variação percentual;
dt_etl: data de processamento ETL.
