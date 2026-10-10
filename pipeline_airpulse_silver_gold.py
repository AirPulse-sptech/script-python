"""ETL AirPulse: Bronze -> Silver -> Gold.

Contrato novo do Bronze:
- ``fase`` e ``sessao_origem`` identificam o estado da fonte quando disponíveis;
- ``intervalo_coleta_s`` registra a cadência prevista da origem;
- ``cpu_pct_host`` é CPU normalizada para a capacidade do host (0..100);
- contadores de I/O incluem unidades explícitas (bytes ou chars);
- temperatura é estimativa sintética, salvo declaração diferente da fonte.

Compatibilidade: colunas do Bronze legado são reconhecidas quando a origem e a
semântica podem ser identificadas. Quando isso não é possível, a métrica fica
indeterminada em vez de se atribuir uma unidade ou fase sem evidência.
"""

import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 1. CONFIGURAÇÕES E CONTRATOS
# ============================================================

ARQUIVO_BRONZE = (
    "bronze_semana.csv" if os.path.exists("bronze_semana.csv") else "bronze_bruto.csv"
)
PASTA_SILVER = Path("silver")
PASTA_GOLD = Path("gold")

INTERVALO_COLETA_S = max(1, int(os.getenv("INTERVALO_COLETA_S", "10")))
ACELERADOR_DE_TEMPO = float(os.getenv("ACELERADOR_DE_TEMPO", "10"))
if ACELERADOR_DE_TEMPO <= 0:
    raise ValueError("ACELERADOR_DE_TEMPO deve ser maior que zero.")

# nome, duração em minutos simulados, CPU esperada, RAM esperada
FASES_VOO = [
    ("BOOT", 1, 65, 0),
    ("PREFLIGHT", 15, 30, 13),
    ("TAXI_OUT", 15, 25, 16),
    ("TAKEOFF", 3, 50, 19),
    ("CLIMB", 20, 42, 21),
    ("CRUISE", 60, 22, 22),
    ("DESCENT", 20, 45, 24),
    ("APPROACH", 10, 58, 27),
    ("TAXI_IN", 6, 25, 25),
    ("DONE", 5, 18, 23),
]

# A fundamentação explicita 70%/85% para CPU, não para as outras métricas.
# Outras métricas só recebem limiares se configuradas pelo banco.
LIMITES_PADRAO = {"cpu_pct": (70.0, 85.0)}
PERSISTENCIA_AMOSTRAS = 3
HISTERESE_PTS = 5.0
PESO_ATENCAO = 0.5
PESO_CRITICO = 2.0
PENALIDADE_COMPLETUDE_POR_PONTO = 0.3
MIN_AMOSTRAS_HEALTH = 3  # Decisão conservadora de implementação; não é requisito textual.

CHAVES_BASE = [
    "empresa_id", "aeronave_id", "fmc_id", "fmc_tipo", "placa_id", "origem", "hostname",
]
CHAVES_AGRUPAMENTO = CHAVES_BASE + ["sessao"]
CHAVES_FMC = ["empresa_id", "aeronave_id", "fmc_id", "fmc_tipo", "placa_id"]

BYTES_POR_KB = 1024
BYTES_POR_MB = 1024 * 1024
METRICAS_ALERTAVEIS = ("cpu_pct", "ram_pct", "flash_pct", "temp_c")


# ============================================================
# 2. LIMIARES OPCIONAIS DO MYSQL
# ============================================================

def carregar_limiares_do_banco() -> dict:
    """Carrega limites configurados; apenas CPU tem padrão documentado no projeto."""
    conexao = None
    limiares = dict(LIMITES_PADRAO)
    try:
        import pymysql  # Importação tardia; MySQL continua opcional.

        conexao = pymysql.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_DATABASE", "airpulse"),
            user=os.getenv("DB_USER", ""),
            password=os.getenv("DB_PASSWORD", ""),
            port=int(os.getenv("DB_PORT", "3306")),
            connect_timeout=5,
        )
        with conexao.cursor() as cursor:
            cursor.execute(
                """
                SELECT tipo, AVG(limiteAtencao), AVG(limiteCritico)
                FROM componente
                WHERE statusMonitoramento = 'ATIVO'
                GROUP BY tipo
                """
            )
            resultados = cursor.fetchall()

        mapa_metricas = {
            "CPU": "cpu_pct", "RAM": "ram_pct", "DISCO": "flash_pct",
            "FLASH": "flash_pct", "TEMPERATURA": "temp_c",
        }
        for tipo, atencao, critico in resultados:
            metrica = mapa_metricas.get(str(tipo).upper())
            if not metrica or atencao is None or critico is None:
                continue
            atencao, critico = float(atencao), float(critico)
            if not np.isfinite(atencao) or not np.isfinite(critico) or atencao >= critico:
                print(f"Limiares inválidos para {tipo}; configuração ignorada.")
                continue
            limiares[metrica] = (atencao, critico)
        print(f"Limiares de monitoramento carregados: {limiares}")
    except Exception as erro:
        print(f"MySQL indisponível ({erro}); usando limites disponíveis no código: {limiares}")
    finally:
        if conexao is not None:
            conexao.close()
    return limiares


def _garantir_coluna(df: pd.DataFrame, nome: str, valor_padrao=np.nan) -> None:
    if nome not in df.columns:
        df[nome] = valor_padrao


def _numero_positivo(valor) -> bool:
    try:
        return bool(np.isfinite(float(valor)) and float(valor) > 0)
    except (TypeError, ValueError):
        return False


def _inferir_fase_legacy(linha: pd.Series) -> str:
    """Fallback somente para telemetria legada da bancada com uptime e fator válidos."""
    if str(linha.get("origem", "")).upper() != "SIMULADA_BANCADA":
        return "NAO_DETERMINADA"
    uptime = linha.get("uptime_proc_s")
    fator = linha.get("fator_tempo")
    if pd.isna(uptime) or not _numero_positivo(fator) or not np.isfinite(float(uptime)):
        return "NAO_DETERMINADA"
    tempo_simulado_s = max(0.0, float(uptime)) * float(fator)
    limites_s = np.cumsum([minutos * 60 for _, minutos, _, _ in FASES_VOO])
    if tempo_simulado_s >= limites_s[-1]:
        return "NAO_DETERMINADA"
    indice = int(np.searchsorted(limites_s, tempo_simulado_s, side="right"))
    return FASES_VOO[indice][0]


def _unidade_io_legada(df: pd.DataFrame, tipo: str, coluna_origem: str | None) -> pd.Series:
    """Infere bytes apenas quando a fonte permite; legado de bancada fica desconhecido."""
    origem = df["origem"].fillna("").astype(str).str.upper()
    unidade = pd.Series("desconhecida", index=df.index, dtype="object")
    if f"io_{tipo}_unidade" in df.columns:
        fornecida = df[f"io_{tipo}_unidade"].astype("string").str.lower()
        permitidas = fornecida.isin(["bytes", "chars"])
        unidade.loc[permitidas] = fornecida.loc[permitidas].astype(str)
    else:
        unidade.loc[origem.eq("SIMULADA_GERADOR")] = "bytes"
        if coluna_origem == f"io_{tipo}_bytes":
            # Esse nome não é suficiente para o antigo agente de bancada, que usava chars.
            unidade.loc[~origem.eq("SIMULADA_BANCADA")] = "bytes"
    unidade.loc[origem.eq("SIMULADA_BANCADA") & ~df.get(
        f"io_{tipo}_unidade", pd.Series(index=df.index, dtype="object")
    ).astype("string").str.lower().isin(["bytes", "chars"])] = "desconhecida"
    return unidade


def _inferir_intervalo(grupo: pd.DataFrame) -> tuple[float, str]:
    """Usa metadado consistente; sem metadado exige cadência repetida nos timestamps."""
    if "intervalo_coleta_s" in grupo.columns:
        valores = pd.to_numeric(grupo["intervalo_coleta_s"], errors="coerce")
        valores = valores[np.isfinite(valores) & (valores > 0)].round(6).unique()
        if len(valores) == 1:
            return float(valores[0]), "METADADO_FONTE"
        if len(valores) > 1:
            return np.nan, "METADADO_INCONSISTENTE"

    deltas = grupo["ts"].sort_values().diff().dt.total_seconds()
    deltas = deltas[np.isfinite(deltas) & (deltas > 0)].round(3)
    if len(deltas) < 2:
        return np.nan, "INDETERMINADO"
    frequencias = deltas.value_counts()
    maior_frequencia = int(frequencias.max())
    if maior_frequencia < 2:
        return np.nan, "INDETERMINADO"
    moda = sorted(float(valor) for valor, contagem in frequencias.items() if contagem == maior_frequencia)
    return moda[0], "INFERIDO_TIMESTAMPS"


# ============================================================
# 3. BRONZE -> SILVER: VALIDAÇÃO, TIPAGEM, QUALIDADE E DERIVAÇÕES
# ============================================================

def processar_camada_silver(df_bronze: pd.DataFrame, limiares: dict | None = None) -> pd.DataFrame:
    """Transforma Bronze sem preencher métricas/identidades desconhecidas com valores falsos."""
    if not isinstance(df_bronze, pd.DataFrame):
        raise TypeError("Bronze deve ser um pandas.DataFrame.")
    if df_bronze.empty:
        raise ValueError("O Bronze não contém amostras.")

    df = df_bronze.copy()
    obrigatorias = ["timestamp_utc", "fmc_id", "aeronave_id", "cpu_percent"]
    faltantes = [coluna for coluna in obrigatorias if coluna not in df.columns]
    if faltantes:
        raise ValueError("Colunas obrigatórias ausentes no Bronze: " + ", ".join(faltantes))

    df["ts"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"]).copy()
    if df.empty:
        raise ValueError("Nenhuma amostra tem timestamp_utc válido; Bronze foi preservado sem alteração.")
    for coluna in ("fmc_id", "aeronave_id"):
        if df[coluna].isna().any() or df[coluna].astype(str).str.strip().eq("").any():
            quantidade = int((df[coluna].isna() | df[coluna].astype(str).str.strip().eq("")).sum())
            raise ValueError(
                f"Identificador obrigatório '{coluna}' ausente em {quantidade} amostra(s); "
                "não é seguro agrupar equipamentos desconhecidos."
            )

    # Campos de identidade opcionais permanecem nulos; não são convertidos em IDs fictícios.
    for coluna in CHAVES_BASE:
        _garantir_coluna(df, coluna, np.nan)
    _garantir_coluna(df, "sessao_origem", np.nan)
    _garantir_coluna(df, "fase", np.nan)
    df["fase"] = df["fase"].astype("object")
    _garantir_coluna(df, "modo_execucao", "desconhecido")
    _garantir_coluna(df, "intervalo_coleta_s", np.nan)
    _garantir_coluna(df, "cpu_nucleos", np.nan)
    _garantir_coluna(df, "cpu_semantica", np.nan)
    _garantir_coluna(df, "cpu_pct_host", np.nan)
    _garantir_coluna(df, "ram_baseline_bytes", np.nan)
    _garantir_coluna(df, "ram_baseline_fonte", np.nan)
    _garantir_coluna(df, "ram_usada_bytes", np.nan)
    _garantir_coluna(df, "ram_total_bytes", np.nan)
    _garantir_coluna(df, "ram_disponivel_bytes", np.nan)
    _garantir_coluna(df, "ram_medicao_tipo", np.nan)
    _garantir_coluna(df, "flash_usada_bytes", np.nan)
    _garantir_coluna(df, "flash_total_bytes", np.nan)
    _garantir_coluna(df, "temperatura_c", np.nan)
    _garantir_coluna(df, "temperatura_tipo", np.nan)
    df["temperatura_tipo"] = df["temperatura_tipo"].astype("object")
    _garantir_coluna(df, "uptime_proc_s", np.nan)
    _garantir_coluna(df, "fator_tempo", np.nan)
    _garantir_coluna(df, "seq", np.nan)
    _garantir_coluna(df, "qtd_tarefas", np.nan)

    colunas_numericas = [
        "seq", "uptime_proc_s", "fator_tempo", "cpu_percent", "cpu_nucleos", "cpu_pct_host",
        "ram_usada_bytes", "ram_baseline_bytes", "ram_total_bytes", "ram_disponivel_bytes", "flash_usada_bytes",
        "flash_total_bytes", "temperatura_c", "qtd_tarefas", "intervalo_coleta_s",
    ]
    for coluna in colunas_numericas:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
    df["cpu_nucleos"] = df["cpu_nucleos"].where(df["cpu_nucleos"] > 0)

    # Reconhece nomes antigos sem declarar que read_chars/write_chars são bytes.
    origem_contador = {}
    for tipo in ("leitura", "escrita"):
        canonico = f"io_{tipo}_contador_acum"
        alternativas = [f"io_{tipo}_bytes", f"io_{tipo}_bytes_acum"]
        if canonico not in df.columns:
            coluna_origem = next((col for col in alternativas if col in df.columns), None)
            origem_contador[tipo] = coluna_origem
            df[canonico] = df[coluna_origem] if coluna_origem else np.nan
        else:
            origem_contador[tipo] = canonico
        df[canonico] = pd.to_numeric(df[canonico], errors="coerce")
        unidade_col = f"io_{tipo}_unidade"
        if unidade_col not in df.columns:
            df[unidade_col] = _unidade_io_legada(df, tipo, origem_contador[tipo])
        else:
            df[unidade_col] = df[unidade_col].fillna("desconhecida").astype(str).str.lower()
            df.loc[~df[unidade_col].isin(["bytes", "chars"]), unidade_col] = "desconhecida"

        # Nomes históricos *_bytes_acum só carregam valores se a unidade for bytes comprovados.
        legado_bytes = f"io_{tipo}_bytes_acum"
        if legado_bytes not in df.columns:
            df[legado_bytes] = np.nan
        df[legado_bytes] = pd.to_numeric(df[legado_bytes], errors="coerce")
        df[legado_bytes] = df[legado_bytes].where(df[unidade_col].eq("bytes"))

    if "cpu_pct_host" in df.columns:
        cpu_host_fornecida = pd.to_numeric(df["cpu_pct_host"], errors="coerce")
    else:
        cpu_host_fornecida = pd.Series(np.nan, index=df.index)

    cpu_semantica = df["cpu_semantica"].fillna("").astype(str).str.lower()
    origem_upper = df["origem"].fillna("").astype(str).str.upper()
    cpu_legacy = pd.Series(np.nan, index=df.index, dtype="float64")
    soma_por_nucleo = cpu_semantica.eq("soma_percentual_por_nucleo") | origem_upper.eq("SIMULADA_BANCADA")
    percentual_host = cpu_semantica.isin([
        "percentual_host_sintetico", "percentual_host", "host_percent",
    ]) | origem_upper.eq("SIMULADA_GERADOR")
    cpu_legacy.loc[soma_por_nucleo] = (
        df.loc[soma_por_nucleo, "cpu_percent"] / df.loc[soma_por_nucleo, "cpu_nucleos"]
    )
    cpu_legacy.loc[percentual_host] = df.loc[percentual_host, "cpu_percent"]
    uma_cpu = df["cpu_nucleos"].eq(1)
    cpu_legacy.loc[~soma_por_nucleo & ~percentual_host & uma_cpu] = df.loc[
        ~soma_por_nucleo & ~percentual_host & uma_cpu, "cpu_percent"
    ]
    candidato_cpu = cpu_host_fornecida.where(cpu_host_fornecida.notna(), cpu_legacy)
    df["q_cpu_semantica_indeterminada"] = candidato_cpu.isna()
    df["q_cpu_acima_100"] = candidato_cpu > 100
    df["q_cpu_negativa"] = candidato_cpu < 0
    df["q_cpu_valor_invalido"] = df["q_cpu_acima_100"] | df["q_cpu_negativa"]
    df["cpu_pct"] = candidato_cpu.where(candidato_cpu.between(0, 100, inclusive="both"))

    # RAM: baseline só é aplicada quando a origem declara calibração do processo monitorado.
    ram_usada_valida = df["ram_usada_bytes"].where(df["ram_usada_bytes"] >= 0)
    baseline_valida = (
        df["ram_baseline_fonte"].fillna("").astype(str).str.upper()
        == "CALIBRADA_NO_PROCESSO_MONITORADO"
    ) & df["ram_baseline_bytes"].notna() & (df["ram_baseline_bytes"] >= 0)
    baseline_aplicada = df["ram_baseline_bytes"].where(baseline_valida, 0.0)
    df["ram_logica_bytes"] = (ram_usada_valida - baseline_aplicada).clip(lower=0)
    df["q_ram_baseline_ignorada"] = (
        df["ram_baseline_bytes"].fillna(0).gt(0) & ~baseline_valida
    )
    ram_total_valido = df["ram_total_bytes"].where(df["ram_total_bytes"] > 0)
    # Compatibilidade: somente o gerador semanal antigo permitia derivar a disponibilidade
    # do valor de RAM sintético (não derivar RAM livre de RSS de um processo real).
    disponibilidade_legada_semana = (
        df["ram_disponivel_bytes"].isna()
        & origem_upper.eq("SIMULADA_GERADOR")
        & df["ram_usada_bytes"].notna()
        & ram_total_valido.notna()
    )
    df.loc[disponibilidade_legada_semana, "ram_disponivel_bytes"] = (
        ram_total_valido.loc[disponibilidade_legada_semana]
        - df.loc[disponibilidade_legada_semana, "ram_usada_bytes"]
    )
    df["q_ram_disponivel_invalida"] = (
        df["ram_disponivel_bytes"].notna()
        & ((df["ram_disponivel_bytes"] < 0)
           | (ram_total_valido.notna() & (df["ram_disponivel_bytes"] > ram_total_valido)))
    )
    df["ram_disponivel_bytes"] = df["ram_disponivel_bytes"].where(~df["q_ram_disponivel_invalida"])
    df["ram_disponivel_pct"] = df["ram_disponivel_bytes"] / ram_total_valido * 100
    flash_total_valido = df["flash_total_bytes"].where(df["flash_total_bytes"] > 0)
    df["ram_pct"] = df["ram_logica_bytes"] / ram_total_valido * 100
    df["flash_pct"] = df["flash_usada_bytes"].where(df["flash_usada_bytes"] >= 0) / flash_total_valido * 100
    df["q_ram_acima_100"] = df["ram_pct"] > 100
    df["q_flash_acima_100"] = df["flash_pct"] > 100

    # Fase explícita é prioritária; fallback é restrito ao legado da bancada.
    duracoes_permitidas = df["fase"].astype("string").str.strip()
    fase_ausente = duracoes_permitidas.isna() | duracoes_permitidas.eq("") | duracoes_permitidas.str.upper().eq("NAO_DETERMINADA")
    if fase_ausente.any():
        df.loc[fase_ausente, "fase"] = df.loc[fase_ausente].apply(_inferir_fase_legacy, axis=1)
    df["fase"] = df["fase"].fillna("NAO_DETERMINADA").astype(str).str.strip()
    df.loc[df["fase"].eq(""), "fase"] = "NAO_DETERMINADA"

    # Temperatura sintética comum aos modos: 35 + 0,5 * CPU normalizada para o host.
    temperatura_estimada = 35.0 + 0.5 * df["cpu_pct"]
    temperatura_fonte = pd.to_numeric(df["temperatura_c"], errors="coerce")
    df["temp_c"] = temperatura_fonte.where(temperatura_fonte.notna(), temperatura_estimada)
    temp_tipo_vazio = df["temperatura_tipo"].isna() | df["temperatura_tipo"].astype(str).str.strip().eq("")
    simuladas = origem_upper.isin(["SIMULADA_BANCADA", "SIMULADA_GERADOR"])
    df.loc[temp_tipo_vazio & simuladas, "temperatura_tipo"] = "ESTIMATIVA_SINTETICA_LEGADA"
    df.loc[temp_tipo_vazio & ~simuladas & temperatura_fonte.isna(), "temperatura_tipo"] = "ESTIMATIVA_SINTETICA_FALLBACK"
    df.loc[temp_tipo_vazio & ~simuladas & temperatura_fonte.notna(), "temperatura_tipo"] = "ORIGEM_NAO_IDENTIFICADA"

    # State Detection (Silver): níveis e flags usam os limiares desta execução.
    # Na ausência de configuração explícita, somente CPU recebe os limites documentados.
    limiares_estado = dict(LIMITES_PADRAO) if limiares is None else dict(limiares)
    for metrica in METRICAS_ALERTAVEIS:
        nome_nivel = f"nivel_{metrica}"
        nome_atencao = f"q_{metrica}_atencao"
        nome_critico = f"q_{metrica}_critico"
        nome_limiar_atencao = f"limiar_{metrica}_atencao"
        nome_limiar_critico = f"limiar_{metrica}_critico"
        limites = _limiar_valido(limiares_estado, metrica)
        if limites is None:
            df[nome_nivel] = "NAO_CONFIGURADO"
            df[nome_atencao] = pd.Series(pd.NA, index=df.index, dtype="boolean")
            df[nome_critico] = pd.Series(pd.NA, index=df.index, dtype="boolean")
            df[nome_limiar_atencao] = np.nan
            df[nome_limiar_critico] = np.nan
            continue

        lim_atencao, lim_critico = limites
        valores = pd.to_numeric(df[metrica], errors="coerce")
        nivel = pd.Series("NORMAL", index=df.index, dtype="object")
        nivel.loc[valores.isna()] = "NAO_DETERMINADO"
        nivel.loc[valores.gt(lim_atencao) & valores.le(lim_critico)] = "ATENCAO"
        nivel.loc[valores.gt(lim_critico)] = "CRITICO"
        df[nome_nivel] = nivel
        flag_atencao = (valores.gt(lim_atencao) & valores.le(lim_critico)).astype("boolean")
        flag_critico = valores.gt(lim_critico).astype("boolean")
        flag_atencao.loc[valores.isna()] = pd.NA
        flag_critico.loc[valores.isna()] = pd.NA
        df[nome_atencao] = flag_atencao
        df[nome_critico] = flag_critico
        df[nome_limiar_atencao] = lim_atencao
        df[nome_limiar_critico] = lim_critico

    # Ordena somente depois que as chaves foram garantidas.
    df = df.sort_values(CHAVES_BASE + ["ts"], kind="mergesort", na_position="last").reset_index(drop=True)
    duplicidade_chaves = CHAVES_BASE + ["ts"]
    if "sessao_origem" in df.columns:
        duplicidade_chaves.append("sessao_origem")
    quantidade_duplicadas = int(df.duplicated(subset=duplicidade_chaves, keep="last").sum())
    if quantidade_duplicadas:
        df = df.drop_duplicates(subset=duplicidade_chaves, keep="last").reset_index(drop=True)
        print(f"Silver: descartadas {quantidade_duplicadas} amostras duplicadas; Bronze mantido intacto.")

    # Reinícios de processo/sessão: não se inventam IDs de aeronave/FMC.
    reinicio = pd.Series(False, index=df.index, dtype=bool)
    grupos_base = df.groupby(CHAVES_BASE, dropna=False, sort=False)
    for coluna in ("uptime_proc_s", "seq"):
        diferenca = grupos_base[coluna].diff()
        reinicio |= diferenca.lt(0).fillna(False)
    sessao_anterior = grupos_base["sessao_origem"].shift()
    mudanca_sessao = (
        df["sessao_origem"].notna() & sessao_anterior.notna()
        & df["sessao_origem"].astype(str).ne(sessao_anterior.astype(str))
    )
    reinicio |= mudanca_sessao.fillna(False)
    df["_reinicio"] = reinicio
    df["sessao"] = (
        df.groupby(CHAVES_BASE, dropna=False, sort=False)["_reinicio"]
        .cumsum().astype(int) + 1
    )

    # Intervalo: metadado de fonte consistente; fallback apenas se timestamps repetem uma cadência.
    df["intervalo_esperado_s"] = np.nan
    df["intervalo_origem"] = "INDETERMINADO"
    for _, grupo in df.groupby(CHAVES_AGRUPAMENTO, dropna=False, sort=False):
        intervalo, origem_intervalo = _inferir_intervalo(grupo)
        df.loc[grupo.index, "intervalo_esperado_s"] = intervalo
        df.loc[grupo.index, "intervalo_origem"] = origem_intervalo

    grupos_sessao = df.groupby(CHAVES_AGRUPAMENTO, dropna=False, sort=False)
    df["dt_s"] = grupos_sessao["ts"].diff().dt.total_seconds()
    df["t_sessao_s"] = (df["ts"] - grupos_sessao["ts"].transform("min")).dt.total_seconds()
    intervalo_seguro = df["intervalo_esperado_s"].where(df["intervalo_esperado_s"] > 0)
    df["q_intervalo_indeterminado"] = intervalo_seguro.isna()
    q_lacuna = df["dt_s"].gt(intervalo_seguro * 1.5).astype("boolean")
    # A primeira amostra não tem intervalo anterior; com cadência conhecida, isso não é lacuna.
    q_lacuna.loc[df["dt_s"].isna() & intervalo_seguro.notna()] = False
    # Cadência desconhecida => lacuna não classificável, e não uma falsa ausência de lacuna.
    q_lacuna.loc[intervalo_seguro.isna()] = pd.NA
    df["q_lacuna"] = q_lacuna
    razao_intervalo = df["dt_s"] / intervalo_seguro
    ausentes_estimados = np.floor(razao_intervalo + 1e-9) - 1
    df["q_amostras_ausentes"] = ausentes_estimados.where(
        np.isfinite(ausentes_estimados) & (ausentes_estimados > 0), 0
    ).clip(lower=0)
    df.loc[intervalo_seguro.isna(), "q_amostras_ausentes"] = np.nan
    df["q_dt_invalido"] = df["dt_s"].isna() | (df["dt_s"] <= 0)

    # Taxas diferenciais de I/O, sem converter caracteres em bytes e sem transformar reset em zero.
    for tipo in ("leitura", "escrita"):
        coluna = f"io_{tipo}_contador_acum"
        unidade = f"io_{tipo}_unidade"
        delta = grupos_sessao[coluna].diff()
        unidade_anterior = grupos_sessao[unidade].shift()
        unidade_estavel = df[unidade].eq(unidade_anterior) & df[unidade].isin(["bytes", "chars"])
        reiniciou = delta < 0
        dt_valido = df["dt_s"] > 0
        delta_valido = delta.where(unidade_estavel & ~reiniciou & dt_valido)
        df[f"q_io_{tipo}_contador_reiniciado"] = reiniciou.fillna(False)
        df[f"io_{tipo}_kbps"] = (delta_valido / df["dt_s"] / BYTES_POR_KB).where(df[unidade].eq("bytes"))
        df[f"io_{tipo}_chars_s"] = (delta_valido / df["dt_s"]).where(df[unidade].eq("chars"))

    # Coluna auxiliar interna não precisa ir à Silver persistida.
    df = df.drop(columns=["_reinicio"])
    df.attrs["duplicatas_descartadas"] = quantidade_duplicadas
    return df


# ============================================================
# 4. FUNÇÕES AUXILIARES GOLD
# ============================================================

def regressao_linear(x: pd.Series, y: pd.Series, minimo_pontos: int = 3) -> float:
    """Inclinação OLS de y por x; exige pontos e instantes distintos suficientes."""
    dados = pd.DataFrame({"x": x, "y": y}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(dados) < minimo_pontos or dados["x"].nunique() < 2:
        return np.nan
    return float(np.polyfit(dados["x"].astype(float), dados["y"].astype(float), 1)[0])


def percentual_acima(serie: pd.Series, limite: float) -> float:
    """Percentual acima do limite entre valores válidos; sem dados, retorna NaN."""
    valores_validos = pd.to_numeric(serie, errors="coerce").dropna()
    if valores_validos.empty:
        return np.nan
    return float((valores_validos > limite).mean() * 100)


def _limiar_valido(limiares: dict, metrica: str):
    limites = limiares.get(metrica)
    if not limites or len(limites) != 2:
        return None
    try:
        atencao, critico = float(limites[0]), float(limites[1])
    except (TypeError, ValueError):
        return None
    if not np.isfinite(atencao) or not np.isfinite(critico) or atencao >= critico:
        return None
    return atencao, critico


def _calcular_health_score(grupo: pd.DataFrame, limiares: dict, completude: float) -> tuple[float, str, float, float]:
    """Score por severidade máxima por amostra; crítico não acumula também atenção."""
    metricas_configuradas = [
        metrica for metrica in METRICAS_ALERTAVEIS if _limiar_valido(limiares, metrica)
    ]
    if not metricas_configuradas or pd.isna(completude):
        return np.nan, "DADOS_INSUFICIENTES", np.nan, np.nan

    atencao_alguma = pd.Series(False, index=grupo.index)
    critico_algum = pd.Series(False, index=grupo.index)
    amostra_valida = pd.Series(False, index=grupo.index)
    for metrica in metricas_configuradas:
        serie = pd.to_numeric(grupo[metrica], errors="coerce")
        limite_atencao, limite_critico = _limiar_valido(limiares, metrica)
        valido = serie.notna() & np.isfinite(serie)
        amostra_valida |= valido
        critico_algum |= valido & (serie > limite_critico)
        atencao_alguma |= valido & (serie > limite_atencao) & (serie <= limite_critico)

    denominador = int(amostra_valida.sum())
    if denominador < MIN_AMOSTRAS_HEALTH:
        return np.nan, "AMOSTRAS_INSUFICIENTES", np.nan, np.nan
    pct_critico = float((critico_algum & amostra_valida).sum() / denominador * 100)
    pct_atencao = float((atencao_alguma & ~critico_algum & amostra_valida).sum() / denominador * 100)
    penalidade = (
        PESO_ATENCAO * pct_atencao
        + PESO_CRITICO * pct_critico
        + max(0.0, 100.0 - float(completude)) * PENALIDADE_COMPLETUDE_POR_PONTO
    )
    score = max(0.0, min(100.0, 100.0 - penalidade))
    return round(score, 1), "OK", round(pct_atencao, 2), round(pct_critico, 2)


def calcular_kpis_sessao(grupo: pd.DataFrame, identificadores: dict, limiares: dict) -> dict:
    """Calcula indicadores da sessão sem mascarar ausência de dados ou intervalos desconhecidos."""
    grupo = grupo.sort_values("ts").reset_index(drop=True)
    total_amostras = len(grupo)
    duracao_s = float(grupo["t_sessao_s"].max()) if total_amostras else 0.0
    intervalos = pd.to_numeric(grupo["intervalo_esperado_s"], errors="coerce").dropna()
    intervalo = float(intervalos.iloc[0]) if len(intervalos) and intervalos.nunique() == 1 else np.nan
    if np.isfinite(intervalo) and intervalo > 0:
        esperadas = max(1.0, duracao_s / intervalo + 1.0)
        completude = min(100.0, total_amostras / esperadas * 100.0)
        amostras_esperadas = round(esperadas, 1)
    else:
        completude = np.nan
        amostras_esperadas = np.nan

    cpu = pd.to_numeric(grupo["cpu_pct"], errors="coerce")
    ram = pd.to_numeric(grupo["ram_pct"], errors="coerce")
    ram_bytes = pd.to_numeric(grupo["ram_logica_bytes"], errors="coerce")
    temp = pd.to_numeric(grupo["temp_c"], errors="coerce")
    flash_pct = pd.to_numeric(grupo["flash_pct"], errors="coerce")

    cpu_media, cpu_std, cpu_p95, cpu_pico = cpu.mean(), cpu.std(), cpu.quantile(0.95), cpu.max()
    ram_media, ram_pico = ram.mean(), ram.max()
    temp_media, temp_pico = temp.mean(), temp.max()
    ram_disponivel = pd.to_numeric(grupo["ram_disponivel_bytes"], errors="coerce")
    ram_disponivel_pct = pd.to_numeric(grupo["ram_disponivel_pct"], errors="coerce")
    ram_headroom_bytes = float(ram_disponivel.min()) if ram_disponivel.notna().any() else np.nan
    ram_headroom_pct = float(ram_disponivel_pct.min()) if ram_disponivel_pct.notna().any() else np.nan
    tipos_ram = sorted(set(grupo["ram_medicao_tipo"].dropna().astype(str)))
    tipos_temperatura = sorted(set(grupo["temperatura_tipo"].dropna().astype(str)))

    kpis = {
        **identificadores,
        "inicio": grupo["ts"].min().isoformat(),
        "fim": grupo["ts"].max().isoformat(),
        "duracao_min": round(duracao_s / 60, 2),
        "amostras": total_amostras,
        "amostras_esperadas": amostras_esperadas,
        "amostras_ausentes_estimadas": (
            int(pd.to_numeric(grupo["q_amostras_ausentes"], errors="coerce").sum())
            if intervalo == intervalo and pd.notna(intervalo) else np.nan
        ),
        "intervalo_esperado_s": intervalo,
        "intervalo_origem": ";".join(sorted(set(grupo["intervalo_origem"].dropna().astype(str)))),
        "completude_pct": round(completude, 2) if pd.notna(completude) else np.nan,
        "fase_final": str(grupo["fase"].iloc[-1]) if total_amostras else "NAO_DETERMINADA",
        # CPU em percentagem da capacidade total do host.
        "cpu_amostras_validas": int(cpu.count()),
        "cpu_media": round(cpu_media, 2) if pd.notna(cpu_media) else np.nan,
        "cpu_pico": round(cpu_pico, 2) if pd.notna(cpu_pico) else np.nan,
        "cpu_std": round(cpu_std, 2) if pd.notna(cpu_std) else np.nan,
        "cpu_p95": round(cpu_p95, 2) if pd.notna(cpu_p95) else np.nan,
        "cpu_headroom_p95": round(100.0 - cpu_p95, 2) if pd.notna(cpu_p95) else np.nan,
        "cpu_coef_variacao": round(cpu_std / cpu_media, 4) if pd.notna(cpu_std) and pd.notna(cpu_media) and cpu_media > 0 else np.nan,
        # RAM é memória residente observada/estimada com base na origem; não cota ARINC certificada.
        "ram_amostras_validas": int(ram.count()),
        "ram_media": round(ram_media, 2) if pd.notna(ram_media) else np.nan,
        "ram_pico": round(ram_pico, 2) if pd.notna(ram_pico) else np.nan,
        # Margem é a menor disponibilidade observada no host, não total_host menos RSS do processo.
        "ram_headroom_bytes": round(ram_headroom_bytes, 0) if pd.notna(ram_headroom_bytes) else np.nan,
        "ram_headroom_pct": round(ram_headroom_pct, 2) if pd.notna(ram_headroom_pct) else np.nan,
        "ram_medicao_tipos": ";".join(tipos_ram),
        "temp_media": round(temp_media, 2) if pd.notna(temp_media) else np.nan,
        "temp_max": round(temp_pico, 2) if pd.notna(temp_pico) else np.nan,
        "temperatura_tipos": ";".join(tipos_temperatura),
        "flash_media_pct": round(flash_pct.mean(), 2) if flash_pct.notna().any() else np.nan,
        "flash_max_pct": round(flash_pct.max(), 2) if flash_pct.notna().any() else np.nan,
    }

    inclinacao_ram = regressao_linear(grupo["t_sessao_s"], ram_bytes, minimo_pontos=3)
    kpis["ram_tendencia_kb_h"] = (
        round(inclinacao_ram * 3600 / BYTES_POR_KB, 4)
        if pd.notna(inclinacao_ram) else np.nan
    )

    # Projeção de preenchimento do arquivo/log observado, não vida útil física da memória Flash.
    flash = grupo[["t_sessao_s", "flash_usada_bytes", "flash_total_bytes"]].copy()
    flash = flash.replace([np.inf, -np.inf], np.nan).dropna().sort_values("t_sessao_s")
    flash_taxa_bps = np.nan
    flash_autonomia_h = np.nan
    if len(flash) >= 2 and flash["t_sessao_s"].nunique() >= 2:
        deltas_flash = flash["flash_usada_bytes"].diff().dropna()
        tempo_decorrido = float(flash["t_sessao_s"].iloc[-1] - flash["t_sessao_s"].iloc[0])
        sem_reset = bool((deltas_flash >= 0).all())
        if tempo_decorrido > 0 and sem_reset:
            taxa_regressao = regressao_linear(
                flash["t_sessao_s"], flash["flash_usada_bytes"], minimo_pontos=2
            )
            if pd.notna(taxa_regressao) and taxa_regressao > 0:
                flash_taxa_bps = float(taxa_regressao)
                capacidade = pd.to_numeric(flash["flash_total_bytes"], errors="coerce").dropna()
                ultimo_usado = float(flash["flash_usada_bytes"].iloc[-1])
                if len(capacidade) and float(capacidade.iloc[-1]) > 0:
                    bytes_restantes = max(0.0, float(capacidade.iloc[-1]) - ultimo_usado)
                    flash_autonomia_h = bytes_restantes / flash_taxa_bps / 3600

    kpis["flash_taxa_kbps"] = round(flash_taxa_bps / BYTES_POR_KB, 6) if pd.notna(flash_taxa_bps) else np.nan
    kpis["flash_capacidade_restante_h"] = round(flash_autonomia_h, 3) if pd.notna(flash_autonomia_h) else np.nan
    # Mantido por compatibilidade; semântica documentada explicitamente como projeção de capacidade.
    kpis["flash_autonomia_h"] = kpis["flash_capacidade_restante_h"]
    kpis["flash_autonomia_tipo"] = "PROJECAO_CAPACIDADE_LOG" if pd.notna(flash_autonomia_h) else "INDETERMINADA"

    health_score, health_status, pct_atencao, pct_critico = _calcular_health_score(grupo, limiares, completude)
    kpis["health_score"] = health_score
    kpis["health_score_status"] = health_status
    kpis["health_pct_amostras_atencao"] = pct_atencao
    kpis["health_pct_amostras_criticas"] = pct_critico
    return kpis


# ============================================================
# 5. ALERTAS COM PERSISTÊNCIA, HISTERESE E HISTÓRICO AUDITÁVEL
# ============================================================

def nivel_alerta(valor: float, limite_atencao: float, limite_critico: float):
    if pd.isna(valor):
        return None
    if valor > limite_critico:
        return "CRITICO"
    if valor > limite_atencao:
        return "ATENCAO"
    return None


def _registro_evento_alerta(identificadores, metrica, tipo_evento, nivel, valor,
                            limiar_violado, ts_abertura, linha_evento, fase_abertura,
                            alerta_id, ts_encerramento=None, duracao_s=np.nan):
    return {
        **identificadores,
        "alerta_id": alerta_id,
        "metrica": metrica,
        "tipo_evento": tipo_evento,
        "nivel": nivel,
        "valor": round(float(valor), 4) if pd.notna(valor) else np.nan,
        "limiar_violado": limiar_violado,
        "ts_abertura": ts_abertura,
        "ts_evento": linha_evento["ts"],
        "ts_encerramento": ts_encerramento,
        "duracao_s": round(float(duracao_s), 3) if pd.notna(duracao_s) else np.nan,
        "fase": fase_abertura,
        "fase_evento": str(linha_evento.get("fase", "NAO_DETERMINADA")),
        "em_aberto_apos_evento": tipo_evento != "ENCERRAMENTO",
    }


def gerar_alertas(df_silver: pd.DataFrame, limiares: dict) -> pd.DataFrame:
    """Gera eventos de abertura, escala, rebaixamento e encerramento de alertas."""
    registros = []
    colunas_saida = [
        *CHAVES_AGRUPAMENTO, "alerta_id", "metrica", "tipo_evento", "nivel", "valor",
        "limiar_violado", "ts_abertura", "ts_evento", "ts_encerramento", "duracao_s",
        "fase", "fase_evento", "em_aberto_apos_evento",
    ]

    for chave, grupo_original in df_silver.groupby(CHAVES_AGRUPAMENTO, dropna=False, sort=False):
        chave = chave if isinstance(chave, tuple) else (chave,)
        identificadores = dict(zip(CHAVES_AGRUPAMENTO, chave))
        grupo = grupo_original.sort_values("ts").reset_index(drop=True)

        for metrica in METRICAS_ALERTAVEIS:
            limites = _limiar_valido(limiares, metrica)
            if limites is None:
                continue
            limite_atencao, limite_critico = limites
            col_nivel = f"nivel_{metrica}"
            col_atencao = f"limiar_{metrica}_atencao"
            col_critico = f"limiar_{metrica}_critico"
            usa_state_detection_silver = all(
                coluna in grupo.columns for coluna in (col_nivel, col_atencao, col_critico)
            )
            if usa_state_detection_silver:
                at_configurados = pd.to_numeric(grupo[col_atencao], errors="coerce").dropna().unique()
                cr_configurados = pd.to_numeric(grupo[col_critico], errors="coerce").dropna().unique()
                usa_state_detection_silver = (
                    len(at_configurados) == 1 and len(cr_configurados) == 1
                    and np.isclose(float(at_configurados[0]), limite_atencao)
                    and np.isclose(float(cr_configurados[0]), limite_critico)
                )
            nivel_ativo = None
            ts_abertura = None
            fase_abertura = None
            alerta_id = None
            nivel_pendente = None
            contador_pendente = 0
            indice_inicio_pendente = None

            for i, linha in grupo.iterrows():
                valor = pd.to_numeric(pd.Series([linha.get(metrica)]), errors="coerce").iloc[0]
                dt = linha.get("dt_s", np.nan)
                intervalo = linha.get("intervalo_esperado_s", np.nan)
                lacuna = bool(linha.get("q_lacuna", False)) if pd.notna(linha.get("q_lacuna", False)) else False
                intervalo_valido = pd.notna(intervalo) and float(intervalo) > 0
                intervalo_consecutivo = (
                    i == 0 or (
                        intervalo_valido and pd.notna(dt) and float(dt) > 0
                        and float(dt) <= float(intervalo) * 1.5 and not lacuna
                    )
                )
                if not intervalo_consecutivo:
                    nivel_pendente = None
                    contador_pendente = 0
                    indice_inicio_pendente = None

                if usa_state_detection_silver:
                    nivel_fonte = linha.get(col_nivel)
                    nivel_atual = (
                        str(nivel_fonte) if nivel_fonte in ("ATENCAO", "CRITICO") else None
                    )
                else:
                    nivel_atual = nivel_alerta(valor, limite_atencao, limite_critico)
                if pd.isna(valor):
                    nivel_pendente = None
                    contador_pendente = 0
                    indice_inicio_pendente = None
                    continue

                if nivel_ativo is None:
                    if nivel_atual is None:
                        nivel_pendente = None
                        contador_pendente = 0
                        indice_inicio_pendente = None
                        continue
                    if nivel_atual != nivel_pendente or contador_pendente == 0:
                        nivel_pendente = nivel_atual
                        contador_pendente = 1
                        indice_inicio_pendente = i
                    else:
                        contador_pendente += 1

                    if contador_pendente >= PERSISTENCIA_AMOSTRAS:
                        nivel_ativo = nivel_pendente
                        primeira = grupo.iloc[indice_inicio_pendente]
                        ts_abertura = primeira["ts"]
                        fase_abertura = str(primeira.get("fase", "NAO_DETERMINADA"))
                        alerta_id = f"{identificadores.get('fmc_id')}:{metrica}:{ts_abertura.isoformat()}"
                        limiar = limite_critico if nivel_ativo == "CRITICO" else limite_atencao
                        registros.append(_registro_evento_alerta(
                            identificadores, metrica, "ABERTURA", nivel_ativo, valor, limiar,
                            ts_abertura, linha, fase_abertura, alerta_id,
                        ))
                        nivel_pendente = None
                        contador_pendente = 0
                        indice_inicio_pendente = None

                elif nivel_ativo == "ATENCAO":
                    if nivel_atual == "CRITICO":
                        if not intervalo_consecutivo or nivel_pendente != "CRITICO":
                            nivel_pendente = "CRITICO"
                            contador_pendente = 1
                            indice_inicio_pendente = i
                        else:
                            contador_pendente += 1
                        if contador_pendente >= PERSISTENCIA_AMOSTRAS:
                            nivel_ativo = "CRITICO"
                            primeira = grupo.iloc[indice_inicio_pendente]
                            registros.append(_registro_evento_alerta(
                                identificadores, metrica, "ESCALONAMENTO", "CRITICO", valor,
                                limite_critico, ts_abertura, linha, fase_abertura, alerta_id,
                            ))
                            nivel_pendente = None
                            contador_pendente = 0
                            indice_inicio_pendente = None
                    else:
                        nivel_pendente = None
                        contador_pendente = 0
                        indice_inicio_pendente = None
                        if valor < limite_atencao - HISTERESE_PTS:
                            duracao = (linha["ts"] - ts_abertura).total_seconds() if ts_abertura is not None else np.nan
                            registros.append(_registro_evento_alerta(
                                identificadores, metrica, "ENCERRAMENTO", "ATENCAO", valor,
                                limite_atencao, ts_abertura, linha, fase_abertura, alerta_id,
                                ts_encerramento=linha["ts"], duracao_s=duracao,
                            ))
                            nivel_ativo, ts_abertura, fase_abertura, alerta_id = None, None, None, None

                else:  # alerta CRITICO
                    nivel_pendente = None
                    contador_pendente = 0
                    indice_inicio_pendente = None
                    if valor < limite_critico - HISTERESE_PTS:
                        if valor >= limite_atencao - HISTERESE_PTS:
                            nivel_ativo = "ATENCAO"
                            registros.append(_registro_evento_alerta(
                                identificadores, metrica, "REBAIXAMENTO", "ATENCAO", valor,
                                limite_atencao, ts_abertura, linha, fase_abertura, alerta_id,
                            ))
                        else:
                            duracao = (linha["ts"] - ts_abertura).total_seconds() if ts_abertura is not None else np.nan
                            registros.append(_registro_evento_alerta(
                                identificadores, metrica, "ENCERRAMENTO", "CRITICO", valor,
                                limite_critico, ts_abertura, linha, fase_abertura, alerta_id,
                                ts_encerramento=linha["ts"], duracao_s=duracao,
                            ))
                            nivel_ativo, ts_abertura, fase_abertura, alerta_id = None, None, None, None

    resultado = pd.DataFrame(registros, columns=colunas_saida)
    if not resultado.empty:
        resultado = resultado.sort_values(["ts_evento", "alerta_id"], kind="mergesort").reset_index(drop=True)
    return resultado


# ============================================================
# 6. KPIs GOLD POR FASE, SESSÃO E MINUTO
# ============================================================

def calcular_kpis_por_fase(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Resume métricas por fase explícita/inferida; fases não modeladas não recebem baseline inventado."""
    chaves = CHAVES_AGRUPAMENTO + ["fase"]
    resultado = (
        df_silver.groupby(chaves, dropna=False, observed=True)
        .agg(
            registros=("ts", "count"),
            cpu_amostras_validas=("cpu_pct", "count"),
            cpu_media=("cpu_pct", "mean"),
            cpu_pico=("cpu_pct", "max"),
            ram_media=("ram_pct", "mean"),
            flash_media=("flash_pct", "mean"),
            temp_max=("temp_c", "max"),
            lacunas=("q_lacuna", lambda valores: valores.sum(min_count=1)),
            amostras_ausentes_estimadas=("q_amostras_ausentes", lambda valores: valores.sum(min_count=1)),
        )
        .reset_index()
    )
    for coluna in ("cpu_media", "cpu_pico", "ram_media", "flash_media", "temp_max"):
        resultado[coluna] = resultado[coluna].round(2)

    mapa_cpu_esperada = {nome: cpu for nome, _, cpu, _ in FASES_VOO}
    mapa_ram_esperada = {nome: ram for nome, _, _, ram in FASES_VOO}
    resultado["cpu_esperado"] = resultado["fase"].map(mapa_cpu_esperada)
    resultado["ram_esperada"] = resultado["fase"].map(mapa_ram_esperada)
    resultado["cpu_desvio_esperado"] = (resultado["cpu_media"] - resultado["cpu_esperado"]).round(2)
    resultado["ram_desvio_esperado"] = (resultado["ram_media"] - resultado["ram_esperada"]).round(2)
    return resultado


def processar_camada_gold(df_silver: pd.DataFrame, limiares: dict):
    """Mantém a assinatura original de retorno dos quatro conjuntos Gold principais."""
    kpis_sessao = []
    for chave, grupo in df_silver.groupby(CHAVES_AGRUPAMENTO, dropna=False, sort=False):
        chave = chave if isinstance(chave, tuple) else (chave,)
        identificadores = dict(zip(CHAVES_AGRUPAMENTO, chave))
        kpis_sessao.append(calcular_kpis_sessao(grupo, identificadores, limiares))
    df_kpis_sessao = pd.DataFrame(kpis_sessao)

    df_serie_1min = (
        df_silver.groupby(
            CHAVES_AGRUPAMENTO + ["fase", pd.Grouper(key="ts", freq="1min")],
            dropna=False, observed=True,
        )
        .agg(
            amostras=("ts", "count"),
            cpu_media=("cpu_pct", "mean"),
            ram_media=("ram_pct", "mean"),
            flash_media=("flash_pct", "mean"),
            temp_media=("temp_c", "mean"),
            temp_max=("temp_c", "max"),
            lacunas=("q_lacuna", lambda valores: valores.sum(min_count=1)),
        )
        .reset_index()
    )
    for coluna in ("cpu_media", "ram_media", "flash_media", "temp_media", "temp_max"):
        df_serie_1min[coluna] = df_serie_1min[coluna].round(3)

    df_kpis_fase = calcular_kpis_por_fase(df_silver)
    df_alertas = gerar_alertas(df_silver, limiares)
    return df_kpis_sessao, df_serie_1min, df_kpis_fase, df_alertas


def _chave_normalizada(linha: pd.Series, colunas: list[str]) -> tuple:
    return tuple(None if pd.isna(linha.get(coluna)) else str(linha.get(coluna)) for coluna in colunas)


def criar_resumo_fmc(df_silver: pd.DataFrame, df_kpis_sessao: pd.DataFrame, df_alertas: pd.DataFrame) -> pd.DataFrame:
    """Consolida a última amostra/KPI por FMC e o estado atual de cada alerta auditável."""
    colunas_saida = [
        *CHAVES_FMC, "ultima_amostra_utc", "ultima_fase", "ultima_sessao", "amostras_sessao",
        "completude_pct", "health_score", "health_score_status", "alertas_abertos",
        "alertas_criticos_abertos", "alertas_atencao_abertos", "estado_saude",
    ]
    ativos = {}
    if not df_alertas.empty:
        for _, evento in df_alertas.sort_values("ts_evento", kind="mergesort").iterrows():
            chave_fmc = _chave_normalizada(evento, CHAVES_FMC)
            chave_alerta = (chave_fmc, str(evento["alerta_id"]))
            tipo = evento["tipo_evento"]
            if tipo == "ABERTURA":
                ativos[chave_alerta] = {"nivel": evento["nivel"], "metrica": evento["metrica"]}
            elif tipo in ("ESCALONAMENTO", "REBAIXAMENTO") and chave_alerta in ativos:
                ativos[chave_alerta]["nivel"] = evento["nivel"]
            elif tipo == "ENCERRAMENTO":
                ativos.pop(chave_alerta, None)

    linhas = []
    for _, grupo in df_silver.groupby(CHAVES_FMC, dropna=False, sort=False):
        grupo = grupo.sort_values("ts")
        ultima = grupo.iloc[-1]
        chave_fmc = _chave_normalizada(ultima, CHAVES_FMC)
        abertos = [valor for (chave, _), valor in ativos.items() if chave == chave_fmc]

        kpi = pd.DataFrame()
        if not df_kpis_sessao.empty:
            candidatos = df_kpis_sessao.copy()
            for coluna in CHAVES_FMC:
                valor = ultima.get(coluna)
                if pd.isna(valor):
                    candidatos = candidatos[candidatos[coluna].isna()]
                else:
                    candidatos = candidatos[candidatos[coluna].astype(str).eq(str(valor))]
            if not candidatos.empty:
                kpi = candidatos.sort_values("fim").iloc[-1]

        criticos = sum(1 for alerta in abertos if alerta["nivel"] == "CRITICO")
        atencao = sum(1 for alerta in abertos if alerta["nivel"] == "ATENCAO")
        health = kpi.get("health_score", np.nan) if not kpi.empty else np.nan
        health_status = kpi.get("health_score_status", "DADOS_INSUFICIENTES") if not kpi.empty else "DADOS_INSUFICIENTES"
        if criticos:
            estado = "CRITICO"
        elif atencao:
            estado = "ATENCAO"
        elif pd.isna(health):
            estado = "DADOS_INSUFICIENTES"
        else:
            estado = "SEM_ALERTAS_ATIVOS"
        linhas.append({
            **{coluna: ultima.get(coluna, np.nan) for coluna in CHAVES_FMC},
            "ultima_amostra_utc": ultima["ts"].isoformat(),
            "ultima_fase": str(ultima.get("fase", "NAO_DETERMINADA")),
            "ultima_sessao": ultima.get("sessao", np.nan),
            "amostras_sessao": kpi.get("amostras", np.nan) if not kpi.empty else np.nan,
            "completude_pct": kpi.get("completude_pct", np.nan) if not kpi.empty else np.nan,
            "health_score": health,
            "health_score_status": health_status,
            "alertas_abertos": len(abertos),
            "alertas_criticos_abertos": criticos,
            "alertas_atencao_abertos": atencao,
            "estado_saude": estado,
        })

    return pd.DataFrame(linhas, columns=colunas_saida)


# ============================================================
# 7. UPLOAD OPCIONAL PARA S3
# ============================================================

def enviar_arquivos_s3(caminhos_locais: list, prefixo: str, df_silver: pd.DataFrame) -> None:
    """Envia resultados quando há credenciais; falha de S3 não impede uso local."""
    bucket = os.getenv("S3_BUCKET_NAME")
    chave_acesso = os.getenv("AWS_ACCESS_KEY_ID")
    chave_secreta = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not bucket or not chave_acesso or not chave_secreta:
        print(f"S3 não configurado; arquivos {prefixo} permaneceram locais.")
        return
    try:
        import boto3
        s3 = boto3.client(
            "s3", aws_access_key_id=chave_acesso, aws_secret_access_key=chave_secreta,
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )
    except Exception as erro:
        print(f"Não foi possível criar cliente S3: {erro}")
        return

    empresas = df_silver["empresa_id"].dropna().astype(str).unique()
    aeronaves = df_silver["aeronave_id"].dropna().astype(str).unique()
    empresa_path = empresas[0] if len(empresas) == 1 else "MULTI_EMPRESA"
    aeronave_path = aeronaves[0] if len(aeronaves) == 1 else "FROTA"
    for caminho in caminhos_locais:
        if not os.path.exists(caminho):
            continue
        chave_s3 = f"{prefixo}/empresa={empresa_path}/aeronave={aeronave_path}/{os.path.basename(caminho)}"
        try:
            s3.upload_file(caminho, bucket, chave_s3)
            print(f"S3 OK: s3://{bucket}/{chave_s3}")
        except Exception as erro:
            print(f"Erro ao enviar {caminho} para o S3: {erro}")


# ============================================================
# 8. EXECUÇÃO DO PIPELINE
# ============================================================

if __name__ == "__main__":
    if not os.path.exists(ARQUIVO_BRONZE):
        raise SystemExit(f"Arquivo Bronze '{ARQUIVO_BRONZE}' não encontrado. Execute o agente primeiro.")

    print(f"Lendo Bronze: {ARQUIVO_BRONZE}")
    df_bronze = pd.read_csv(ARQUIVO_BRONZE, sep=";")
    if df_bronze.empty:
        raise SystemExit("O arquivo Bronze existe, mas não contém amostras.")

    limiares = carregar_limiares_do_banco()
    print("Processando Silver...")
    try:
        df_silver = processar_camada_silver(df_bronze, limiares)
    except (ValueError, TypeError) as erro:
        raise SystemExit(f"Erro de esquema/qualidade no Bronze: {erro}") from erro

    PASTA_SILVER.mkdir(parents=True, exist_ok=True)
    caminho_silver = PASTA_SILVER / "silver_telemetria.csv"
    df_silver.to_csv(caminho_silver, sep=";", index=False, encoding="utf-8")
    print(f"Silver salvo: {caminho_silver} ({len(df_silver)} linhas)")

    print("Processando Gold...")
    df_sessao, df_serie, df_fase, df_alertas = processar_camada_gold(df_silver, limiares)
    df_resumo_fmc = criar_resumo_fmc(df_silver, df_sessao, df_alertas)
    PASTA_GOLD.mkdir(parents=True, exist_ok=True)
    arquivos_gold = {
        PASTA_GOLD / "gold_kpi_sessao.csv": df_sessao,
        PASTA_GOLD / "gold_serie_1min.csv": df_serie,
        PASTA_GOLD / "gold_kpi_fase.csv": df_fase,
        PASTA_GOLD / "gold_alertas.csv": df_alertas,
        PASTA_GOLD / "gold_resumo_fmc.csv": df_resumo_fmc,
    }
    for caminho, dados in arquivos_gold.items():
        dados.to_csv(caminho, sep=";", index=False, encoding="utf-8")
        print(f"Gold salvo: {caminho} ({len(dados)} linhas)")

    colunas_resumo = [
        "fmc_id", "placa_id", "sessao", "duracao_min", "amostras", "completude_pct",
        "cpu_media", "cpu_p95", "ram_media", "ram_tendencia_kb_h", "flash_autonomia_h",
        "health_score", "health_score_status",
    ]
    colunas_presentes = [coluna for coluna in colunas_resumo if coluna in df_sessao.columns]
    print("\nResumo Gold por sessão:")
    print(df_sessao[colunas_presentes].to_string(index=False))
    if not df_alertas.empty:
        print(f"\nEventos de alerta registrados: {len(df_alertas)}")
        print(df_alertas[["fmc_id", "metrica", "tipo_evento", "nivel", "valor", "fase"]].to_string(index=False))
    else:
        print("\nNenhum evento de alerta gerado nesta execução.")

    enviar_arquivos_s3([str(caminho_silver)], "silver", df_silver)
    enviar_arquivos_s3([str(caminho) for caminho in arquivos_gold], "gold", df_silver)
