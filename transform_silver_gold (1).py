"""
AirPulse - Transform (T do ETL): Bronze -> Silver -> Gold

Entrada : um ou mais CSVs do agente (sep=";"), do modo real e/ou semana. O esquema é o
          mesmo; 1 agente = 1 placa, e cada FMC tem 2 placas. Todas as séries passam
          pelo MESMO código: o gold não depende do modo de origem.
Silver  : dado tipado, deduplicado, com sessões, fase, deltas e flags de qualidade
          (nada é corrigido em silêncio: problemas viram flag - seção 11 do doc de visão)
Gold    : KPIs prontos para o dashboard
          - gold_kpi_sessao.csv  (1 linha por voo/sessão do FMC)
          - gold_kpi_fase.csv    (1 linha por fase de voo)
          - gold_alertas.csv     (eventos com persistência + histerese)
          - gold_serie_1min.csv  (série agregada p/ gráficos)
          - gold_resumo_fmc.csv  (1 linha por FMC: 2 placas consolidadas)

Uso:
  python transform_silver_gold.py                                    # bronze_bruto.csv
  python transform_silver_gold.py bronze_P1.csv bronze_P2.csv        # várias placas
  python transform_silver_gold.py dados_semana/bronze_semana.csv
"""
import argparse
import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# ==========================================
# CONFIGURAÇÃO
# ==========================================
load_dotenv()

ENTRADA_BRONZE = ["bronze_bruto.csv"]   # padrão; sobrescrito pelos argumentos da linha de comando
PASTA_SILVER = "silver"
PASTA_GOLD = "gold"

INTERVALO_S = int(os.getenv("INTERVALO_COLETA_S", "10"))   # mesmo valor do agente
PLACAS_POR_FMC = 2
TOL_LACUNA = 1.5           # dt > 1.5x o intervalo vira "lacuna"

# (atenção, crítico) - configuráveis por métrica (RF-006)
LIMITES = {
    "cpu_pct": (70.0, 85.0),
    "ram_pct": (70.0, 85.0),
    "flash_pct": (70.0, 85.0),
    "temp_c": (70.0, 85.0),
}
PERSISTENCIA_AMOSTRAS = 3  # amostras seguidas acima do limite p/ abrir alerta
HISTERESE_PTS = 5.0        # alerta só fecha abaixo de (limite - 5)
MAX_FORA_ESCALA = 0.05     # >5% das amostras >100% => métrica não confiável

# Mesmas fases do simulador (nome, minutos). A fase é inferida pelo uptime.
FASES = [
    ("BOOT", 1), ("PREFLIGHT", 15), ("TAXI_OUT", 15), ("TAKEOFF", 3),
    ("CLIMB", 20), ("CRUISE", 60), ("DESCENT", 20), ("APPROACH", 10),
    ("TAXI_IN", 6), ("DONE", 5),
]

COLS_NUM = [
    "seq", "placa_id", "uptime_proc_s", "cpu_percent", "cpu_nucleos", "qtd_tarefas", "ram_usada_bytes",
    "ram_baseline_bytes", "ram_total_bytes", "flash_usada_bytes", "flash_total_bytes",
    "io_leitura_bytes", "io_escrita_bytes", "temperatura_c",
]
IDENT = ["empresa_id", "aeronave_id", "fmc_id", "fmc_tipo", "placa_id", "origem"]
PLACA = ["aeronave_id", "fmc_id", "placa_id"]   # uma placa = um agente = uma série temporal
CHAVE = IDENT + ["sessao"]
PADROES_BRONZE_ANTIGO = {"ram_baseline_bytes": 0, "cpu_nucleos": 1, "placa_id": 0,
                         "fmc_tipo": "desconhecido", "origem": "DESCONHECIDA"}


# ==========================================
# BRONZE -> SILVER
# ==========================================
def bronze_para_silver(bronze: pd.DataFrame) -> pd.DataFrame:
    df = bronze.copy()

    # tipagem
    # ISO8601: o modo real tem microssegundos e o semana não; sem isso o pandas
    # infere o formato da 1ª linha e transforma o resto em NaT
    df["ts"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce", format="ISO8601")
    for col, padrao in PADROES_BRONZE_ANTIGO.items():   # bronze antigo, sem essas colunas
        if col not in df.columns:
            df[col] = padrao
    for c in COLS_NUM:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["placa_id"] = df["placa_id"].fillna(0).astype(int)
    df["cpu_nucleos"] = df["cpu_nucleos"].fillna(1).clip(lower=1)
    df["fmc_tipo"] = df["fmc_tipo"].fillna("desconhecido")
    df["origem"] = df["origem"].fillna("DESCONHECIDA")

    # limpeza estrutural: sem timestamp/identidade não há como rastrear a leitura
    df = df.dropna(subset=["ts", "fmc_id"])
    df = df.drop_duplicates(subset=PLACA + ["ts", "seq"])
    df = df.sort_values(PLACA + ["ts"]).reset_index(drop=True)

    # sessões (por placa): uptime (ou seq) que cai = simulador/agente reiniciou
    por_placa = df.groupby(PLACA)
    reset = (por_placa["uptime_proc_s"].diff() < 0) | (por_placa["seq"].diff() < 0)
    df["sessao"] = reset.groupby([df[c] for c in PLACA]).cumsum().astype(int) + 1

    ps = df.groupby(PLACA + ["sessao"])
    df["dt_s"] = ps["ts"].diff().dt.total_seconds()
    df["t_sessao_s"] = (df["ts"] - ps["ts"].transform("min")).dt.total_seconds()

    # métricas em %
    # CPU do agente = soma dos núcleos; dividir por cpu_nucleos (gravado pelo agente) traz
    # para 0-100% da placa. No modo semana cpu_nucleos = 1. Não há clip: >100 vira flag.
    df["cpu_pct"] = df["cpu_percent"] / df["cpu_nucleos"]
    # RSS bruto menos o baseline do interpretador = RAM "lógica" do FMC simulado
    df["ram_logica_bytes"] = (df["ram_usada_bytes"] - df["ram_baseline_bytes"].fillna(0)).clip(lower=0)
    df["ram_pct"] = df["ram_logica_bytes"] / df["ram_total_bytes"] * 100
    df["temp_c"] = df["temperatura_c"]

    # Flash: tamanho do arquivo que a bancada usa como flash (medido pelo agente)
    df["flash_pct"] = df["flash_usada_bytes"] / df["flash_total_bytes"] * 100

    # contadores cumulativos -> taxa (KB/s); delta negativo = reset do contador
    for nome in ("leitura", "escrita"):
        delta = ps[f"io_{nome}_bytes"].diff()
        df[f"io_{nome}_kbps"] = delta.where(delta >= 0) / df["dt_s"] / 1024

    # fase inferida pelo uptime do processo
    limites_s = np.cumsum([m for _, m in FASES]) * 60
    bins = [0, *limites_s, np.inf]
    labels = [n for n, _ in FASES] + ["POS_DONE"]
    df["fase"] = (
        pd.cut(df["uptime_proc_s"], bins=bins, labels=labels, right=False)
        .astype("object").fillna("DESCONHECIDA")
    )

    # flags de qualidade (marcar, não corrigir)
    df["q_lacuna"] = df["dt_s"] > INTERVALO_S * TOL_LACUNA
    df["q_nulo"] = df[["cpu_pct", "ram_pct", "temp_c"]].isna().any(axis=1)
    df["q_cpu_acima_100"] = df["cpu_pct"] > 100
    df["q_ram_acima_total"] = df["ram_pct"] > 100
    df["q_flash_acima_total"] = df["flash_pct"] > 100
    df["q_ram_abaixo_baseline"] = df["ram_usada_bytes"] < df["ram_baseline_bytes"].fillna(0)

    colunas = [
        "ts", *CHAVE, "hostname", "seq", "fase", "uptime_proc_s",
        "t_sessao_s", "dt_s", "cpu_pct", "cpu_nucleos", "qtd_tarefas", "ram_pct",
        "ram_usada_bytes", "ram_logica_bytes", "ram_total_bytes", "flash_pct",
        "flash_usada_bytes", "flash_total_bytes", "io_leitura_kbps",
        "io_escrita_kbps", "temp_c", "q_lacuna", "q_nulo", "q_cpu_acima_100",
        "q_ram_acima_total", "q_flash_acima_total", "q_ram_abaixo_baseline",
    ]
    return df[colunas]


# ==========================================
# GOLD - helpers
# ==========================================
def detectar_eventos(d: pd.DataFrame, metrica: str, limite: float, severidade: str):
    """Abre evento após N amostras >= limite; fecha só abaixo de (limite - histerese)."""
    vals = d[metrica].to_numpy(dtype=float)
    ts = d["ts"].to_numpy()
    eventos, em_evento, cont, ini, pico = [], False, 0, 0, -np.inf

    for i, v in enumerate(vals):
        if np.isnan(v):
            continue
        if not em_evento:
            cont = cont + 1 if v >= limite else 0
            if cont >= PERSISTENCIA_AMOSTRAS:
                em_evento, ini = True, i - PERSISTENCIA_AMOSTRAS + 1
                pico = np.nanmax(vals[ini:i + 1])
        else:
            pico = max(pico, v)
            if v < limite - HISTERESE_PTS:
                eventos.append((ini, i, pico, False))
                em_evento, cont = False, 0
    if em_evento:
        eventos.append((ini, len(vals) - 1, pico, True))

    return [{
        "metrica": metrica, "severidade": severidade, "limite": limite,
        "inicio": pd.Timestamp(ts[a]), "fim": pd.Timestamp(ts[b]),
        "duracao_s": (pd.Timestamp(ts[b]) - pd.Timestamp(ts[a])).total_seconds(),
        "pico": p, "aberto": aberto,
    } for a, b, p, aberto in eventos]


def estatisticas(d: pd.DataFrame, col: str, prefixo: str) -> dict:
    s = d[col].dropna()
    if s.empty:
        return {f"{prefixo}_{k}": np.nan for k in ("media", "p95", "max", "std")}
    return {
        f"{prefixo}_media": s.mean(),
        f"{prefixo}_p95": s.quantile(0.95),
        f"{prefixo}_max": s.max(),
        f"{prefixo}_std": s.std(),
    }


def faixa_saude(score: float) -> str:
    return "SAUDAVEL" if score >= 85 else "ATENCAO" if score >= 60 else "CRITICO"


# ==========================================
# SILVER -> GOLD
# ==========================================
def silver_para_gold(silver: pd.DataFrame):
    kpis_sessao, kpis_fase, alertas = [], [], []

    for chave, d in silver.groupby(CHAVE):
        d = d.sort_values("ts")
        ident = dict(zip(CHAVE, chave))
        dur_s = d["t_sessao_s"].max()
        esperadas = int(dur_s // INTERVALO_S) + 1
        k = {
            **ident,
            "inicio": d["ts"].min(), "fim": d["ts"].max(),
            "duracao_min": dur_s / 60,
            "amostras": len(d),
            "completude_pct": min(100.0, len(d) / esperadas * 100),
            "lacunas": int(d["q_lacuna"].sum()),
            "maior_lacuna_s": d["dt_s"].max(),
            "fase_final": d["fase"].iloc[-1],
        }

        penalidade = 0.0
        for metrica, (lim_at, lim_cr) in LIMITES.items():
            s = metrica.split("_")[0]
            k.update(estatisticas(d, metrica, s))

            # métrica em % fora de escala (>100%) não é confiável: sai do score
            fora = (d[metrica] > 100).mean() if metrica != "temp_c" else 0.0
            confiavel = fora <= MAX_FORA_ESCALA
            k[f"{s}_confiavel"] = confiavel

            pct_at = (d[metrica] >= lim_at).mean() * 100
            pct_cr = (d[metrica] >= lim_cr).mean() * 100
            k[f"{s}_pct_tempo_atencao"] = pct_at
            k[f"{s}_pct_tempo_critico"] = pct_cr
            if confiavel:
                penalidade += 0.5 * pct_at + 2.0 * pct_cr

            ev = (detectar_eventos(d, metrica, lim_at, "ATENCAO")
                  + detectar_eventos(d, metrica, lim_cr, "CRITICO"))
            for e in ev:
                alertas.append({**ident, **e})
            k[f"{s}_eventos_atencao"] = sum(e["severidade"] == "ATENCAO" for e in ev)
            k[f"{s}_eventos_criticos"] = sum(e["severidade"] == "CRITICO" for e in ev)

        # estabilidade/determinismo: num FMC (RTOS) a carga deveria ser plana
        k["cpu_coef_variacao"] = (k["cpu_std"] / k["cpu_media"]) if k["cpu_media"] else np.nan
        k["cpu_headroom_p95"] = 100 - k["cpu_p95"]
        k["ram_headroom_bytes"] = (d["ram_total_bytes"] - d["ram_logica_bytes"]).min()
        k["qtd_tarefas_max"] = d["qtd_tarefas"].max()

        # tendência de RAM (KB/h): positivo e sustentado = suspeita de vazamento
        r = d[["t_sessao_s", "ram_logica_bytes"]].dropna()
        k["ram_tendencia_kb_h"] = (
            np.polyfit(r["t_sessao_s"], r["ram_logica_bytes"], 1)[0] * 3600 / 1024
            if len(r) >= 3 and r["ram_logica_bytes"].std() > 0 else 0.0
        )

        # Flash: taxa de escrita e autonomia até esgotar
        fl = d[["t_sessao_s", "flash_usada_bytes"]].dropna()
        usada = fl["flash_usada_bytes"].iloc[-1] if len(fl) else np.nan
        taxa_bps = (
            (usada - fl["flash_usada_bytes"].iloc[0]) / (fl["t_sessao_s"].iloc[-1] - fl["t_sessao_s"].iloc[0])
            if len(fl) >= 2 and fl["t_sessao_s"].iloc[-1] > fl["t_sessao_s"].iloc[0] else 0.0
        )
        restante = d["flash_total_bytes"].iloc[0] - usada
        k["flash_usada_final_mb"] = usada / 1024**2
        k["flash_taxa_kbps"] = taxa_bps / 1024
        k["flash_autonomia_h"] = restante / taxa_bps / 3600 if taxa_bps > 0 else np.nan

        # score de saúde (heurística - calibrar com especialista, risco R-03)
        crit = sum(k[f"{m.split('_')[0]}_eventos_criticos"] for m in LIMITES)
        penalidade += min(20.0, 5.0 * crit)
        penalidade += (100 - k["completude_pct"]) * 0.5
        k["health_score"] = float(np.clip(100 - penalidade, 0, 100))
        k["health_faixa"] = faixa_saude(k["health_score"])
        kpis_sessao.append(k)

        # KPIs por fase
        for fase, f in d.groupby("fase"):
            kf = {**ident, "fase": fase, "amostras": len(f),
                  "duracao_min": (f["ts"].max() - f["ts"].min()).total_seconds() / 60}
            kf.update(estatisticas(f, "cpu_pct", "cpu"))
            kf.update(estatisticas(f, "ram_pct", "ram"))
            kf["temp_max"] = f["temp_c"].max()
            kf["io_escrita_kbps_medio"] = f["io_escrita_kbps"].mean()
            kpis_fase.append(kf)

    # série agregada de 1 minuto p/ gráficos
    serie = (
        silver.groupby(CHAVE + ["fase", pd.Grouper(key="ts", freq="1min")])
        .agg(cpu_media=("cpu_pct", "mean"), cpu_max=("cpu_pct", "max"),
             ram_pct_media=("ram_pct", "mean"), flash_pct_max=("flash_pct", "max"),
             temp_max=("temp_c", "max"), amostras=("seq", "count"))
        .reset_index().dropna(subset=["amostras"])
    )
    serie = serie[serie["amostras"] > 0]

    return (pd.DataFrame(kpis_sessao), pd.DataFrame(kpis_fase),
            pd.DataFrame(alertas), serie)


def resumo_fmc(g_sessao: pd.DataFrame, g_alertas: pd.DataFrame) -> pd.DataFrame:
    """Consolida as 2 placas de cada FMC (sessão mais recente de cada placa)."""
    chave_placa = ["empresa_id", "aeronave_id", "fmc_id", "placa_id"]
    ult = g_sessao.sort_values("fim").groupby(chave_placa, as_index=False).tail(1).copy()

    if g_alertas.empty:
        ult["alertas_abertos"] = 0
    else:
        ab = (g_alertas[g_alertas["aberto"]].groupby(chave_placa + ["sessao"]).size()
              .rename("alertas_abertos").reset_index())
        ult = ult.merge(ab, on=chave_placa + ["sessao"], how="left")
        ult["alertas_abertos"] = ult["alertas_abertos"].fillna(0).astype(int)

    r = (ult.groupby(["empresa_id", "aeronave_id", "fmc_id", "fmc_tipo", "origem"], as_index=False)
         .agg(placas=("placa_id", "nunique"), ultima_coleta=("fim", "max"),
              alertas_abertos=("alertas_abertos", "sum"), health_score_min=("health_score", "min")))
    r["placas_faltando"] = (PLACAS_POR_FMC - r["placas"]).clip(lower=0)
    r["health_faixa"] = r["health_score_min"].map(faixa_saude)
    return r


# ==========================================
# IO
# ==========================================
def salvar(df: pd.DataFrame, pasta: str, nome: str) -> str:
    os.makedirs(pasta, exist_ok=True)
    caminho = os.path.join(pasta, nome)
    df.to_csv(caminho, sep=";", index=False, encoding="utf-8")
    print(f"  {caminho}  ({len(df)} linhas)")
    return caminho


def enviar_s3(caminhos: list, camada: str, ref: pd.Series, aeronave: str):
    bucket = os.getenv("S3_BUCKET_NAME")
    if not (bucket and os.getenv("AWS_ACCESS_KEY_ID")):
        return
    import boto3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    for c in caminhos:
        chave = (f"{camada}/empresa={ref['empresa_id']}/aeronave={aeronave}/"
                 f"{os.path.basename(c)}")
        try:
            s3.upload_file(c, bucket, chave)
            print(f"  s3://{bucket}/{chave}")
        except Exception as e:
            print(f"  erro S3 ({c}): {e}")


def avisos_qualidade(silver: pd.DataFrame):
    for flag, msg in [
        ("q_ram_acima_total", "RAM lógica > 100% do total do FMC (baseline mal calibrado? ajuste RAM_BASELINE_BYTES)"),
        ("q_flash_acima_total", "Flash > 100% do total do FMC"),
        ("q_ram_abaixo_baseline", "RSS menor que o baseline (RAM lógica zerada; baseline superestimado)"),
        ("q_cpu_acima_100", "CPU > 100% (processo multi-thread/multi-core)"),
    ]:
        pct = silver[flag].mean() * 100
        if pct > 0:
            print(f"  [aviso] {pct:.0f}% das amostras: {msg}")


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("bronze", nargs="*", default=ENTRADA_BRONZE,
                    help="CSVs do agente (real e/ou semana); padrão: bronze_bruto.csv")
    args = ap.parse_args()

    print(f"Lendo bronze: {', '.join(args.bronze)}")
    bronze = pd.concat([pd.read_csv(c, sep=";") for c in args.bronze], ignore_index=True)

    print("Bronze -> Silver")
    silver = bronze_para_silver(bronze)
    c_silver = salvar(silver, PASTA_SILVER, "silver_telemetria.csv")
    avisos_qualidade(silver)

    print("Silver -> Gold")
    g_sessao, g_fase, g_alertas, g_serie = silver_para_gold(silver)
    g_fmc = resumo_fmc(g_sessao, g_alertas)
    c_gold = [
        salvar(g_sessao, PASTA_GOLD, "gold_kpi_sessao.csv"),
        salvar(g_fase, PASTA_GOLD, "gold_kpi_fase.csv"),
        salvar(g_alertas, PASTA_GOLD, "gold_alertas.csv"),
        salvar(g_serie, PASTA_GOLD, "gold_serie_1min.csv"),
        salvar(g_fmc, PASTA_GOLD, "gold_resumo_fmc.csv"),
    ]

    ref = silver.iloc[0]
    aeronaves = silver["aeronave_id"].unique()
    aeronave_s3 = aeronaves[0] if len(aeronaves) == 1 else "todas"
    enviar_s3([c_silver], "silver", ref, aeronave_s3)
    enviar_s3(c_gold, "gold", ref, aeronave_s3)

    print("\nResumo por placa/sessão:")
    print(g_sessao[["fmc_id", "fmc_tipo", "placa_id", "sessao", "duracao_min", "completude_pct",
                    "cpu_media", "cpu_p95", "ram_media", "flash_autonomia_h",
                    "health_score", "health_faixa"]].round(2).to_string(index=False))
    print("\nResumo por FMC:")
    print(g_fmc.to_string(index=False))
