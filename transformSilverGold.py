import os
import numpy as np
import pandas as pd
import boto3
from dotenv import load_dotenv

# ==========================================
# 1. CONFIGURAÇÕES DA REGRA DE NEGÓCIO
# ==========================================
load_dotenv()
PASTA_SILVER, PASTA_GOLD = "silver", "gold"
INTERVALO_S = int(os.getenv("INTERVALO_COLETA_S", "10"))

LIMITES_ALERTAS = {
    "cpu_pct": (70.0, 85.0),
    "ram_pct": (70.0, 85.0),
    "flash_pct": (70.0, 85.0),
    "temp_c": (70.0, 85.0),
}

FASES_VOO = [
    ("BOOT", 1), ("PREFLIGHT", 15), ("TAXI_OUT", 15), ("TAKEOFF", 3),
    ("CLIMB", 20), ("CRUISE", 60), ("DESCENT", 20), ("APPROACH", 10),
    ("TAXI_IN", 6), ("DONE", 5)
]

CHAVE_AGRUPAMENTO = ["empresa_id", "aeronave_id", "fmc_id", "fmc_tipo", "placa_id", "origem", "sessao"]

# ==========================================
# 2. BRONZE -> SILVER (Limpeza e Tipagem)
# ==========================================
def processar_camada_silver(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """Limpa dados, calcula deltas e infere fases do voo[cite: 12]."""
    df = df_bronze.copy()

    # Tipagem
    df["ts"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts", "fmc_id"]).sort_values(["fmc_id", "placa_id", "ts"]).reset_index(drop=True)

    # Identificação de Sessões (Detecta resets)[cite: 12]
    por_placa = df.groupby(["fmc_id", "placa_id"])
    df["sessao"] = ((por_placa["uptime_proc_s"].diff() < 0) | (por_placa["seq"].diff() < 0)).groupby([df["fmc_id"], df["placa_id"]]).cumsum().astype(int) + 1

    # Cálculos Temporais e Deltas
    ps = df.groupby(["fmc_id", "placa_id", "sessao"])
    df["dt_s"] = ps["ts"].diff().dt.total_seconds()
    df["t_sessao_s"] = (df["ts"] - ps["ts"].transform("min")).dt.total_seconds()

    # Normalização de Métricas em Porcentagem[cite: 12]
    df["cpu_pct"] = df["cpu_percent"] / df["cpu_nucleos"].fillna(1).clip(lower=1)
    df["ram_logica_bytes"] = (df["ram_usada_bytes"] - df["ram_baseline_bytes"].fillna(0)).clip(lower=0)
    df["ram_pct"] = df["ram_logica_bytes"] / df["ram_total_bytes"] * 100
    df["flash_pct"] = df["flash_usada_bytes"] / df["flash_total_bytes"] * 100
    df["temp_c"] = df["temperatura_c"]

    # Taxas de I/O em KB/s
    for io_tipo in ("leitura", "escrita"):
        delta = ps[f"io_{io_tipo}_bytes"].diff()
        df[f"io_{io_tipo}_kbps"] = delta.where(delta >= 0) / df["dt_s"] / 1024

    # Inferência de Fase
    limites_s = np.cumsum([minutos for _, minutos in FASES_VOO]) * 60
    bins = [0] + limites_s.tolist() + [np.inf]
    labels = [nome for nome, _ in FASES_VOO] + ["POS_DONE"]
    df["fase"] = pd.cut(df["uptime_proc_s"], bins=bins, labels=labels, right=False).astype(str)

    return df

# ==========================================
# 3. SILVER -> GOLD (Agregações e KPIs)
# ==========================================
def calcular_kpis_sessao(df_sessao: pd.DataFrame, chaves_dict: dict) -> dict:
    """Extrai os KPIs globais de uma única sessão de voo."""
    duracao_s = df_sessao["t_sessao_s"].max()
    
    kpis = {
        **chaves_dict,
        "inicio": df_sessao["ts"].min(),
        "fim": df_sessao["ts"].max(),
        "duracao_min": duracao_s / 60,
        "amostras": len(df_sessao),
        "fase_final": df_sessao["fase"].iloc[-1],
        "cpu_media": df_sessao["cpu_pct"].mean(),
        "ram_media": df_sessao["ram_pct"].mean(),
    }

    # Estabilidade e Variação[cite: 12]
    cpu_std = df_sessao["cpu_pct"].std()
    kpis["cpu_coef_variacao"] = (cpu_std / kpis["cpu_media"]) if kpis["cpu_media"] > 0 else np.nan

    # Autonomia da Flash[cite: 12]
    fl = df_sessao[["t_sessao_s", "flash_usada_bytes"]].dropna()
    if len(fl) >= 2:
        usada_final = fl["flash_usada_bytes"].iloc[-1]
        taxa_bps = (usada_final - fl["flash_usada_bytes"].iloc[0]) / (fl["t_sessao_s"].iloc[-1] - fl["t_sessao_s"].iloc[0])
        restante = df_sessao["flash_total_bytes"].iloc[0] - usada_final
        kpis["flash_autonomia_h"] = (restante / taxa_bps / 3600) if taxa_bps > 0 else np.nan
    else:
        kpis["flash_autonomia_h"] = np.nan

    return kpis

def processar_camada_gold(df_silver: pd.DataFrame):
    """Gera tabelas finais agregadas para os Dashboards[cite: 12]."""
    kpis_sessao = []
    
    for chave, df_sessao in df_silver.groupby(CHAVE_AGRUPAMENTO):
        chaves_dict = dict(zip(CHAVE_AGRUPAMENTO, chave))
        kpis_sessao.append(calcular_kpis_sessao(df_sessao, chaves_dict))

    df_kpis = pd.DataFrame(kpis_sessao)
    
    # Série Temporal Agregada de 1 minuto para Gráficos[cite: 12]
    df_serie_1min = (
        df_silver.groupby(CHAVE_AGRUPAMENTO + ["fase", pd.Grouper(key="ts", freq="1min")])
        .agg(cpu_media=("cpu_pct", "mean"), ram_media=("ram_pct", "mean"), temp_max=("temp_c", "max"))
        .reset_index()
    )

    return df_kpis, df_serie_1min

# ==========================================
# 4. INTEGRAÇÃO AWS S3
# ==========================================
def enviar_arquivos_s3(caminhos_locais, prefixo_camada, aeronave_id):
    """Sobe arquivos processados para o S3."""
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket or not os.getenv("AWS_ACCESS_KEY_ID"):
        return
        
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    empresa = os.getenv("EMPRESA_ID", "EMP-01")
    
    for caminho in caminhos_locais:
        chave = f"{prefixo_camada}/empresa={empresa}/aeronave={aeronave_id}/{os.path.basename(caminho)}"
        try:
            s3.upload_file(caminho, bucket, chave)
            print(f"Enviado S3: s3://{bucket}/{chave}")
        except Exception as e:
            print(f"Erro S3 ({caminho}): {e}")

# ==========================================
# 5. EXECUÇÃO DO PIPELINE
# ==========================================
if __name__ == "__main__":
    caminho_bronze = "bronze_bruto.csv"
    if not os.path.exists(caminho_bronze):
        exit("Arquivo Bronze não encontrado.")

    print("Lendo Bronze...")
    df_bronze = pd.read_csv(caminho_bronze, sep=";")
    
    print("Processando Silver...")
    df_silver = processar_camada_silver(df_bronze)
    os.makedirs(PASTA_SILVER, exist_ok=True)
    caminho_silver = f"{PASTA_SILVER}/silver_telemetria.csv"
    df_silver.to_csv(caminho_silver, sep=";", index=False)

    print("Processando Gold...")
    df_gold_sessao, df_gold_serie = processar_camada_gold(df_silver)
    os.makedirs(PASTA_GOLD, exist_ok=True)
    
    caminhos_gold = [
        f"{PASTA_GOLD}/gold_kpi_sessao.csv",
        f"{PASTA_GOLD}/gold_serie_1min.csv"
    ]
    df_gold_sessao.to_csv(caminhos_gold[0], sep=";", index=False)
    df_gold_serie.to_csv(caminhos_gold[1], sep=";", index=False)
    
    aeronave_id = df_silver["aeronave_id"].iloc[0] if not df_silver.empty else "desconhecida"
    enviar_arquivos_s3([caminho_silver], "silver", aeronave_id)
    enviar_arquivos_s3(caminhos_gold, "gold", aeronave_id)

    print("\nResumo Gold Final:")
    print(df_gold_sessao[["fmc_id", "placa_id", "sessao", "duracao_min", "cpu_media", "flash_autonomia_h"]].head())