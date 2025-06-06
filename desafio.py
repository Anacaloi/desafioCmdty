import pandas as pd
import requests
from datetime import datetime
import os
import logging
import gdown

# 0. Preparação do ambiente
# Configuração do log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_commodity.log"), logging.StreamHandler()],
)
# Verificando as pastas necessárias na estrutura
pastas = ["raw", "refined"]
pasta_atual = os.getcwd()

for nome_pasta in pastas:
    caminho = os.path.join(pasta_atual, nome_pasta)
    if not os.path.exists(caminho):
        os.makedirs(caminho)
        logging.info(f"Pasta criada: {caminho}")
    else:
        logging.info(f"Pasta já existe: {caminho}")

# fazendo o download dos arquivos
files = {
    "CEPEA-20250416134013.xlsx": "1Zr8WnX6GYHGKJmB6deuwUAgqlQiONvwd",
    "boi_gordo_base.csv": "16bG6TvYIjacnD_pAZYF7cGkVwzObGcbR",
}

for nome_arquivo, file_id in files.items():
    url = f"https://drive.google.com/uc?id={file_id}"
    output_path = os.path.join("raw", nome_arquivo)
    logging.info(f"Baixando {nome_arquivo}...")
    gdown.download(url, output_path, quiet=False)


# 1. Configurando a função para obtenção do IPCA
def obter_ipca(inicio: str, fim: str) -> pd.DataFrame:
    try:
        inicio_formatado = datetime.strptime(inicio, "%d/%m/%Y").strftime("%d/%m/%Y")
        fim_formatado = datetime.strptime(fim, "%d/%m/%Y").strftime("%d/%m/%Y")

        url = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?"
            f"formato=json&dataInicial={inicio_formatado}&dataFinal={fim_formatado}"
        )

        logging.info(f"Requisitando IPCA de {inicio_formatado} até {fim_formatado}")
        resposta = requests.get(url, timeout=10)
        resposta.raise_for_status()

        dados = resposta.json()
        df = pd.DataFrame(dados)
        df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
        df["valor"] = df["valor"].str.replace(",", ".").astype(float)

        logging.info("IPCA obtido com sucesso.")
        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro de requisição ao acessar a API do BCB: {e}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado ao processar dados do IPCA: {e}")
        raise


# 2. Leitura e tratamento do arquivo .xlsx
try:
    logging.info("Lendo e tratando o arquivo CEPEA...")
    df_cepea = pd.read_excel(
        "./raw/CEPEA-20250416134013.xlsx",
        skiprows=3,
        usecols=[0, 1],
        names=["Data", "Valor"],
    )
    df_cepea = df_cepea.dropna(how="all").dropna(subset=["Data"])
    df_cepea["Valor"] = (
        df_cepea["Valor"].astype(str).str.replace(",", ".").astype(float)
    )
    df_cepea["Data"] = pd.to_datetime(df_cepea["Data"], format="%m/%Y")
    df_cepea = df_cepea.drop_duplicates(subset=["Data"])
    df_cepea.rename(columns={"Data": "dt_cmdty"}, inplace=True)

    # Preencher calendário mensal
    calendario = pd.date_range(
        start=df_cepea["dt_cmdty"].min(), end=df_cepea["dt_cmdty"].max(), freq="MS"
    )
    df_cepea = pd.DataFrame({"dt_cmdty": calendario}).merge(
        df_cepea, on="dt_cmdty", how="left"
    )
    df_cepea["Valor"] = df_cepea["Valor"].ffill()
    logging.info("Tratamento do Excel concluído.")
except Exception as e:
    logging.critical(f"Erro ao tratar o arquivo CEPEA: {e}")
    exit(1)

# 3. Obtendo o IPCA
data_inicio = df_cepea["dt_cmdty"].min().strftime("%d/%m/%Y")
data_fim = df_cepea["dt_cmdty"].max().strftime("%d/%m/%Y")

try:
    df_ipca = obter_ipca(data_inicio, data_fim)
    df_ipca.rename(columns={"data": "dt_cmdty", "valor": "ipca"}, inplace=True)
    df_cepea = df_cepea.merge(df_ipca, on="dt_cmdty", how="left")
    df_cepea["ipca"] = df_cepea["ipca"].fillna(0)
    df_cepea["ipca_acumulado"] = df_cepea["ipca"].cumsum()
except Exception:
    logging.critical("Falha ao obter ou processar IPCA. ETL interrompido.")
    exit(1)

# 4. Corrigindo valores pelo IPCA
try:
    ipca_marco = df_cepea[df_cepea["dt_cmdty"] == pd.Timestamp("2025-03-01")][
        "ipca_acumulado"
    ].values[0]
    df_cepea["cmdty_vl_rs_um"] = df_cepea["Valor"] * (
        1 + ((ipca_marco - df_cepea["ipca_acumulado"]) / 100)
    )
    logging.info("Cálculo do valor real (corrigido pelo IPCA) concluído.")
except Exception as e:
    logging.critical(f"Erro ao calcular valor corrigido pelo IPCA: {e}")
    exit(1)

# 5. Preparação do arquivo .csv
csv_path = "./raw/boi_gordo_base.csv"

try:
    if os.path.exists(csv_path):
        df_base = pd.read_csv(csv_path, sep=",", decimal=".", encoding="utf-8")
        df_base["dt_cmdty"] = pd.to_datetime(df_base["dt_cmdty"])
        logging.info("Arquivo CSV existente carregado.")
    else:
        df_base = pd.DataFrame(columns=["dt_cmdty", "cmdty_vl_rs_um"])
        logging.info("Arquivo CSV não encontrado. Será criado um novo.")
except Exception as e:
    logging.error(f"Erro ao ler o CSV de base: {e}")
    exit(1)

# Merge e cálculo da variação percentual
df_merge = pd.merge(
    df_cepea[["dt_cmdty", "cmdty_vl_rs_um"]],
    df_base,
    on="dt_cmdty",
    how="outer",
    suffixes=("_novo", "_antigo"),
)

df_merge["cmdty_var_mes_perc"] = (
    (df_merge["cmdty_vl_rs_um_novo"] - df_merge["cmdty_vl_rs_um_antigo"])
    / df_merge["cmdty_vl_rs_um_antigo"]
).fillna(0)

# Formatar variação percentual (ex: "3.21")
df_merge["cmdty_var_mes_perc"] = (df_merge["cmdty_var_mes_perc"] * 100).round(2)
df_atualizado = df_merge[
    ["dt_cmdty", "cmdty_vl_rs_um_novo", "cmdty_var_mes_perc"]
].rename(columns={"cmdty_vl_rs_um_novo": "cmdty_vl_rs_um"})

# upsert dos dados formatados no arquivo .csv
try:
    df_atualizado.to_csv(csv_path, sep=",", index=False, decimal=".")
    logging.info("Upsert no arquivo CSV realizado com sucesso.")
except Exception as e:
    logging.error(f"Erro ao salvar o CSV atualizado: {e}")
    exit(1)

# 6. Output PARQUET
try:
    df_cepea = df_cepea.merge(
        df_atualizado[["dt_cmdty", "cmdty_var_mes_perc"]], on="dt_cmdty", how="left"
    )

    df_cepea["nome_cmdty"] = "Boi_Gordo"
    df_cepea["tipo_cmdty"] = "Indicador do Boi Gordo CEPEA/B3"
    df_cepea["cmdty_um"] = "15 Kg/carcaça"
    df_cepea["dt_etl"] = pd.to_datetime(datetime.today().date())

    df_refined = df_cepea[
        [
            "dt_cmdty",
            "nome_cmdty",
            "tipo_cmdty",
            "cmdty_um",
            "cmdty_vl_rs_um",
            "cmdty_var_mes_perc",
            "dt_etl",
        ]
    ]

    parquet_path = "./refined/boi_gordo_refined.parquet.parquet"
    df_refined.to_parquet(parquet_path, index=False)
    logging.info(f"Arquivo Parquet salvo com sucesso em: {parquet_path}")
except Exception as e:
    logging.error(f"Erro ao salvar o arquivo Parquet: {e}")
    exit(1)
logging.info("Processo ETL concluído com sucesso.")
