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
  - requests;
  - datetime;
  - os;
  - logging;
  - gdown;
  - openpyxl (para ler Excel .xlsx);
  - pyarrow.
- Conexão com internet para requisição da API do Banco Central

## Instalação

1. Clone este repositório:
   ```bash
   git clone https://github.com/Anacaloi/desafioCmdty.git
   cd desafioCmdty
   ```
2. Instalação das dependências:
   ```bash
   pip install pandas requests gdown openpyxl pyarrow
   ```
## Execução do ETL
Execute o script principal para iniciar o pipeline de ETL:
   ```bash
   python etl_commodity.py
   ```

  - O script realiza as seguintes etapas:
     
    - Criação de diretórios raw/ e refined/ caso não existam.
    - Download de dados de commodity e base histórica.
    - Obtenção do IPCA via API do Banco Central.
    - Cálculo do valor real corrigido pela inflação.
    - Atualização incremental (upsert) do histórico em CSV.
    - Exportação final em formato Parquet.

      
## Estrutura dos Dados

Arquivo Parquet (refined/boi_gordo_refined.parquet)
Inclui as colunas padronizadas para consumo em data warehouse:

- dt_cmdty: data do commodity;
- nome_cmdty: Boi_Gordo;
- tipo_cmdty: Indicador do Boi Gordo CEPEA/B3;
- cmdty_um: 15 Kg/carcaça;
- cmdty_vl_rs_um: valor real do commodity;
- cmdty_var_mes_perc: valor do cálculo realizado de variação percentual;
- dt_etl: data de processamento ETL.

| dt\_cmdty  | nome\_cmdty | tipo\_cmdty                     | cmdty\_um     | cmdty\_vl\_rs\_um | cmdty\_var\_mes\_perc | dt\_etl    |
| ---------- | ----------- | ------------------------------- | ------------- | ----------------- | --------------------- | ---------- |
| 2024-01-01 | Boi\_Gordo  | Indicador do Boi Gordo CEPEA/B3 | 15 Kg/carcaça | 300.00            | 0.00%                 | 2025-05-20 |


## Logs
Todos os eventos e possíveis erros são registrados em etl_commodity.log, disponível no diretório raiz.

## Sugestões de Melhoria / Próximos Passos
Para tornar este projeto ainda mais robusto, escalável e manutenível, as seguintes melhorias podem ser consideradas:

- Modularização do Código: Reestruturar o script principal em funções e módulos menores e mais coesos. Isso facilita a leitura, manutenção e reutilização de partes do código, seguindo o princípio da responsabilidade única.

- Implementação de Testes Unitários: Adicionar testes unitários para as funções críticas (e.g., obter_ipca, funções de tratamento de dados). Isso garante que cada parte do código funcione conforme o esperado e ajuda a prevenir regressões ao adicionar novas funcionalidades ou refatorar o código. Bibliotecas como pytest são ideais para isso.

- Conteinerização (Docker): Criar um Dockerfile para empacotar a aplicação e suas dependências em um contêiner Docker. Isso garante um ambiente de execução consistente em qualquer lugar, simplifica o deploy e a execução do pipeline em diferentes máquinas ou serviços de orquestração.

- Agendamento de Tarefas: Para execução em produção, integrar o pipeline com ferramentas de agendamento de tarefas (ex: Apache Airflow) para automatizar a execução diária ou mensal do ETL.

- Tratamento de Erros Aprimorado e Retries: Implementar estratégias de retry com backoff exponencial para requisições de rede (API do BCB) e verificações mais robustas para dados ausentes ou inválidos, aumentando a resiliência do pipeline.

