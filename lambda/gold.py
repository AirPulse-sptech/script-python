"""Calcula séries, KPIs e eventos de alerta usando o contrato Silver."""
import numpy as np
import pandas as pd

from contratos import BYTES_POR_KB, CHAVES_BASE, CHAVES_AGRUPAMENTO, CHAVES_FMC, COLUNAS_CADASTRO, METRICAS_ALERTAVEIS


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

PERSISTENCIA_AMOSTRAS = 3  # Número de amostras consecutivas para confirmar um alerta

HISTERESE_PTS = 5.0  # Diferença entre limites para evitar mudanças frequentes de estado

PESO_ATENCAO = 0.5  # Peso da penalidade por ocorrências em nível de atenção

PESO_CRITICO = 2.0  # Peso da penalidade por ocorrências em nível crítico

PENALIDADE_COMPLETUDE_POR_PONTO = 0.3  # Penalidade no Health Score por ponto de incompletude

MIN_AMOSTRAS_HEALTH = 3  # Mínimo de amostras para calcular o Health Score


def anexar_cadastro(df_gold: pd.DataFrame, df_silver: pd.DataFrame):
    # Coloca empresa/aeronave/computador (vindos do Silver) nas tabelas Gold, pelo hostname.
    if df_gold.empty:
        return df_gold
    
    ref = df_silver[["hostname", *COLUNAS_CADASTRO]].drop_duplicates("hostname")

    saida = df_gold.merge(ref, on="hostname", how="left")

    primeiras = ["hostname", *COLUNAS_CADASTRO]

    return saida[primeiras + [c for c in saida.columns if c not in primeiras]]


def vincular_componente_alertas(df_alertas, df_silver):
    # Usa os mesmos IDs de componentes e limites associados na etapa Silver.
    chaves = ["hostname", "placa_serie"]
    colunas = [f"componente_id_{metrica}" for metrica in METRICAS_ALERTAVEIS]
    componentes = df_silver[chaves + colunas].drop_duplicates(chaves)
    componentes = componentes.melt(
        id_vars=chaves, value_vars=colunas, var_name="metrica", value_name="componente_id",
    )
    componentes["metrica"] = componentes["metrica"].str.removeprefix("componente_id_")
    return df_alertas.merge(componentes, on=chaves + ["metrica"], how="left")


# BLOCO 6: MOTOR DE ALERTAS

# Reconstrói alertas ativos e gera eventos auditáveis com persistência e histerese.

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

def gerar_alertas(df_silver: pd.DataFrame):
    # Gera eventos de abertura, escala, rebaixamento e encerramento de alertas.
    registros = []
    colunas_saida = [
        *CHAVES_AGRUPAMENTO, "alerta_id", "metrica", "tipo_evento", "nivel", "valor",
        "limiar_violado", "ts_abertura", "ts_evento", "ts_encerramento", "duracao_s",
        "fase", "fase_evento", "em_aberto_apos_evento",]
    
    for chave, grupo_original in df_silver.groupby(CHAVES_AGRUPAMENTO, dropna=False, sort=False):
        chave = chave if isinstance(chave, tuple) else (chave,)
        identificadores = dict(zip(CHAVES_AGRUPAMENTO, chave))
        grupo = grupo_original.sort_values("ts").reset_index(drop=True)

        for metrica in METRICAS_ALERTAVEIS:
            limites = _limites_do_grupo(grupo, metrica)

            if limites is None:
                continue
            limite_atencao, limite_critico = limites
            col_nivel = f"nivel_{metrica}"
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
                        and float(dt) <= float(intervalo) * 1.5 and not lacuna)
                )
                
                if not intervalo_consecutivo:
                    nivel_pendente = None
                    contador_pendente = 0
                    indice_inicio_pendente = None

                nivel_fonte = linha.get(col_nivel)
                nivel_atual = str(nivel_fonte) if nivel_fonte in ("ATENCAO", "CRITICO") else None

                if pd.isna(valor):
                    nivel_pendente = None
                    contador_pendente = 0
                    indice_inicio_pendente = None
                    continue
                # Sem alerta ativo, exige amostras consecutivas acima do limite para abrir.
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

                        alerta_id = (
                        f"{identificadores.get('hostname')}:{identificadores.get('placa_serie')}"
                        f":{metrica}:{ts_abertura.isoformat()}"
                        )

                        limiar = limite_critico if nivel_ativo == "CRITICO" else limite_atencao
                        registros.append(_registro_evento_alerta(
                            identificadores, metrica, "ABERTURA", nivel_ativo, valor, limiar,
                            ts_abertura, linha, fase_abertura, alerta_id,
                        ))

                        nivel_pendente = None
                        contador_pendente = 0
                        indice_inicio_pendente = None
                elif nivel_ativo == "ATENCAO":
                    # Um alerta de atenção só sobe para crítico após persistência crítica.
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
                    # A histerese evita alternar de nível quando o valor fica perto do limite.
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


#============================================================================================================================================
# BLOCO 7: KPIs E RESUMOS GOLD

# Calcula tendências, health score, KPIs por sessão/fase/minuto e resumos de placa/FMC.

def regressao_linear(x: pd.Series, y: pd.Series, minimo_pontos: int = 3):
    # Calcula a tendência linear de y ao longo do tempo; exige pontos distintos.
    # Remove valores inválidos antes do cálculo, para não contaminar a inclinação.
    dados = pd.DataFrame({"x": x, "y": y}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(dados) < minimo_pontos or dados["x"].nunique() < 2:
        return np.nan
    
    return float(np.polyfit(dados["x"].astype(float), dados["y"].astype(float), 1)[0])

def _limites_do_grupo(grupo: pd.DataFrame, metrica: str):
    # Lê os limites do próprio computador, gravados na Silver; sem par único e válido, não há limite.
    vazio = pd.Series(dtype=float)
    atencao = pd.to_numeric(grupo.get(f"limiar_{metrica}_atencao", vazio), errors="coerce").dropna().unique()
    critico = pd.to_numeric(grupo.get(f"limiar_{metrica}_critico", vazio), errors="coerce").dropna().unique()
    if not atencao[0] < critico[0]:
        return None
    return float(atencao[0]), float(critico[0])

def _calcular_health_score(grupo: pd.DataFrame, completude: float):
    # Score por severidade máxima por amostra; crítico não acumula atenção também.
    # Primeiro identifica quais métricas têm limites válidos para execução.
    metricas_configuradas = [
        metrica for metrica in METRICAS_ALERTAVEIS if _limites_do_grupo(grupo, metrica)
    ]
    if not metricas_configuradas or pd.isna(completude):
        return np.nan, "DADOS_INSUFICIENTES", np.nan, np.nan
    atencao_alguma = pd.Series(False, index=grupo.index)
    critico_algum = pd.Series(False, index=grupo.index)
    amostra_valida = pd.Series(False, index=grupo.index)
    for metrica in metricas_configuradas:
        # Cada amostra entra no denominador se pelo menos uma métrica configurada é válida.
        serie = pd.to_numeric(grupo[metrica], errors="coerce")
        limite_atencao, limite_critico = _limites_do_grupo(grupo, metrica)
        valido = serie.notna() & np.isfinite(serie)
        amostra_valida = amostra_valida | valido
        critico_algum = critico_algum | valido & (serie >= limite_critico)
        atencao_alguma = atencao_alguma | valido & (serie >= limite_atencao) & (serie < limite_critico)
    # Usa só as amostras em que ao menos uma métrica configurada tem valor válido.
    denominador = int(amostra_valida.sum())
    if denominador < MIN_AMOSTRAS_HEALTH:
        return np.nan, "AMOSTRAS_INSUFICIENTES", np.nan, np.nan
    pct_critico = float((critico_algum & amostra_valida).sum() / denominador * 100)
    pct_atencao = float((atencao_alguma & ~critico_algum & amostra_valida).sum() / denominador * 100)
    # Penaliza atenção e crítico com pesos diferentes e também considera a falta de dados.
    penalidade = (
        PESO_ATENCAO * pct_atencao
        + PESO_CRITICO * pct_critico
        + max(0.0, 100.0 - float(completude)) * PENALIDADE_COMPLETUDE_POR_PONTO
    )

    score = max(0.0, min(100.0, 100.0 - penalidade))
    return round(score, 1), "OK", round(pct_atencao, 2), round(pct_critico, 2)

def calcular_kpis_sessao(grupo: pd.DataFrame, identificadores: dict):
    # Converte as colunas numéricas
    colunas_numericas = [
        "t_sessao_s",
        "intervalo_esperado_s",
        "q_amostras_ausentes",
        "cpu_pct",
        "ram_pct",
        "ram_logica_bytes",
        "ram_disponivel_bytes",
        "ram_disponivel_pct",
        "flash_pct",
        "flash_usada_bytes",
        "flash_total_bytes",
    ]
    for coluna in colunas_numericas:
        grupo[coluna] = pd.to_numeric(grupo[coluna], errors="coerce")

    # Calcula indicadores da sessão sem mascarar ausência de dados ou intervalos desconhecidos.
    grupo = grupo.sort_values("ts").reset_index(drop=True)
    total_amostras = len(grupo)
    duracao_s = float(grupo["t_sessao_s"].max()) if total_amostras else 0.0

    intervalos = grupo["intervalo_esperado_s"].dropna()
    intervalo = (
        float(intervalos.iloc[0])
        if len(intervalos) and intervalos.nunique() == 1
        else np.nan
    )

    # Sem uma cadência confiável não estimamos amostras esperadas nem completude.
    if pd.notna(intervalo) and intervalo > 0:
        esperadas = max(1.0, duracao_s / intervalo + 1.0)
        completude = min(100.0, total_amostras / esperadas * 100.0)
        amostras_esperadas = round(esperadas, 1)
    else:
        completude = np.nan
        amostras_esperadas = np.nan

    cpu = grupo["cpu_pct"]
    ram = grupo["ram_pct"]
    ram_bytes = grupo["ram_logica_bytes"]
    flash_pct = grupo["flash_pct"]

    cpu_media, cpu_std, cpu_p95, cpu_pico = (
        cpu.mean(), cpu.std(), cpu.quantile(0.95), cpu.max()
    )
    ram_media, ram_pico = ram.mean(), ram.max()

    ram_disponivel = grupo["ram_disponivel_bytes"]
    ram_disponivel_pct = grupo["ram_disponivel_pct"]

    ram_headroom_bytes = (
        float(ram_disponivel.min())
        if ram_disponivel.notna().any()
        else np.nan
    )
    ram_headroom_pct = (
        float(ram_disponivel_pct.min())
        if ram_disponivel_pct.notna().any()
        else np.nan
    )

    # Guarda o tipo de medição para diferenciar RSS do processo de estimativa de host.
    tipos_ram = sorted(set(grupo["ram_medicao_tipo"].dropna().astype(str)))

    kpis = {
        **identificadores,
        "inicio": grupo["ts"].min().isoformat(),
        "fim": grupo["ts"].max().isoformat(),
        "duracao_min": round(duracao_s / 60, 2),
        "amostras": total_amostras,
        "amostras_esperadas": amostras_esperadas,
        "amostras_ausentes_estimadas": (
            int(grupo["q_amostras_ausentes"].sum())
            if pd.notna(intervalo)
            else np.nan
        ),
        "intervalo_esperado_s": intervalo,
        "intervalo_origem": ";".join(
            sorted(set(grupo["intervalo_origem"].dropna().astype(str)))
        ),
        "completude_pct": (
            round(completude, 2) if pd.notna(completude) else np.nan
        ),
        "fase_final": (
            str(grupo["fase"].iloc[-1])
            if total_amostras
            else "NAO_DETERMINADA"
        ),
        # CPU em percentagem da capacidade total do host.
        "cpu_amostras_validas": int(cpu.count()),
        "cpu_media": round(cpu_media, 2) if pd.notna(cpu_media) else np.nan,
        "cpu_pico": round(cpu_pico, 2) if pd.notna(cpu_pico) else np.nan,
        "cpu_std": round(cpu_std, 2) if pd.notna(cpu_std) else np.nan,
        "cpu_p95": round(cpu_p95, 2) if pd.notna(cpu_p95) else np.nan,
        "cpu_headroom_p95": (
            round(100.0 - cpu_p95, 2)
            if pd.notna(cpu_p95)
            else np.nan
        ),
        "cpu_coef_variacao": (
            round(cpu_std / cpu_media, 4)
            if (
                pd.notna(cpu_std)
                and pd.notna(cpu_media)
                and cpu_media > 0
            )
            else np.nan
        ),
        # RAM é memória residente observada/estimada com base na origem; não cota ARINC certificada.
        "ram_amostras_validas": int(ram.count()),
        "ram_media": round(ram_media, 2) if pd.notna(ram_media) else np.nan,
        "ram_pico": round(ram_pico, 2) if pd.notna(ram_pico) else np.nan,
        # Margem é a menor disponibilidade observada no host, não total_host menos RSS do processo.
        "ram_headroom_bytes": (
            round(ram_headroom_bytes, 0)
            if pd.notna(ram_headroom_bytes)
            else np.nan
        ),
        "ram_headroom_pct": (
            round(ram_headroom_pct, 2)
            if pd.notna(ram_headroom_pct)
            else np.nan
        ),
        "ram_medicao_tipos": ";".join(tipos_ram),
        "flash_media_pct": (
            round(flash_pct.mean(), 2)
            if flash_pct.notna().any()
            else np.nan
        ),
        "flash_max_pct": (
            round(flash_pct.max(), 2)
            if flash_pct.notna().any()
            else np.nan
        ),
    }

    inclinacao_ram = regressao_linear(
        grupo["t_sessao_s"], ram_bytes, minimo_pontos=3
    )
    kpis["ram_tendencia_kb_h"] = (
        round(inclinacao_ram * 3600 / BYTES_POR_KB, 4)
        if pd.notna(inclinacao_ram)
        else np.nan
    )

    # Projeção de preenchimento do arquivo/log observado, não vida útil física da memória Flash.
    flash = grupo[
        ["t_sessao_s", "flash_usada_bytes", "flash_total_bytes"]
    ].replace([np.inf, -np.inf], np.nan).dropna().sort_values("t_sessao_s")

    flash_taxa_bps = flash_autonomia_h = np.nan

    if len(flash) >= 2 and flash["t_sessao_s"].nunique() >= 2:
        deltas_flash = flash["flash_usada_bytes"].diff().dropna()

        if (deltas_flash >= 0).all():  # sem reset de contador
            taxa = regressao_linear(
                flash["t_sessao_s"],
                flash["flash_usada_bytes"],
                minimo_pontos=2,
            )

            if pd.notna(taxa) and taxa > 0:
                flash_taxa_bps = float(taxa)
                capacidade = float(flash["flash_total_bytes"].iloc[-1])

                if capacidade > 0:
                    flash_autonomia_h = (
                        max(
                            0.0,
                            capacidade - flash["flash_usada_bytes"].iloc[-1],
                        )
                        / taxa
                        / 3600
                    )

    kpis["flash_taxa_kbps"] = (
        round(flash_taxa_bps / BYTES_POR_KB, 6)
        if pd.notna(flash_taxa_bps)
        else np.nan
    )
    kpis["flash_capacidade_restante_h"] = (
        round(flash_autonomia_h, 3)
        if pd.notna(flash_autonomia_h)
        else np.nan
    )

    # Alias de nome antigo mantido para não quebrar dashboards que já leem esse KPI.
    # O valor continua sendo projeção de capacidade do arquivo/log, não RUL física da Flash.
    kpis["flash_autonomia_h"] = kpis["flash_capacidade_restante_h"]
    kpis["flash_autonomia_tipo"] = (
        "PROJECAO_CAPACIDADE_LOG"
        if pd.notna(flash_autonomia_h)
        else "INDETERMINADA"
    )

    health_score, health_status, pct_atencao, pct_critico = (
        _calcular_health_score(grupo, completude)
    )
    kpis["health_score"] = health_score
    kpis["health_score_status"] = health_status
    kpis["health_pct_amostras_atencao"] = pct_atencao
    kpis["health_pct_amostras_criticas"] = pct_critico

    return kpis


_ORDEM_SEVERIDADE = {"CRITICO": 2, "ATENCAO": 1, "NORMAL": 0}

def calcular_serie_diaria(df_silver: pd.DataFrame):
    # Resume por dia a PIOR severidade entre CPU/RAM/Flash, para o mapa de calor.
    # Métrica não configurada/indeterminada não entra na conta (nunca inventa severidade).
    colunas_nivel = [f"nivel_{m}" for m in METRICAS_ALERTAVEIS if f"nivel_{m}" in df_silver.columns]
    severidade_amostra = pd.Series(np.nan, index=df_silver.index)
    for coluna in colunas_nivel:
        severidade_amostra = np.fmax(severidade_amostra, df_silver[coluna].map(_ORDEM_SEVERIDADE))
    df = df_silver.assign(_severidade_amostra=severidade_amostra)

    resultado = (
        df.groupby(CHAVES_BASE + [pd.Grouper(key="ts", freq="1D")], dropna=False, observed=True)
        .agg(
            amostras=("ts", "count"),
            cpu_media=("cpu_pct", "mean"),
            ram_media=("ram_pct", "mean"),
            flash_media=("flash_pct", "mean"),
            severidade_max=("_severidade_amostra", "max"),
            lacunas=("q_lacuna", lambda v: v.sum(min_count=1)),
        )
        .reset_index()
    )
    for coluna in ("cpu_media", "ram_media", "flash_media"):
        resultado[coluna] = resultado[coluna].round(2)

    mapa_nome = {2: "CRITICO", 1: "ATENCAO", 0: "NORMAL"}
    resultado["nivel_dia"] = resultado["severidade_max"].map(mapa_nome)
    return resultado.drop(columns=["severidade_max"])


def calcular_kpis_por_fase(df_silver: pd.DataFrame):
    # Resume métricas por fase; fases não modeladas não recebem baseline inventado.
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
            lacunas=("q_lacuna", lambda valores: valores.sum(min_count=1)),
            amostras_ausentes_estimadas=("q_amostras_ausentes", lambda valores: valores.sum(min_count=1)),
        )

        .reset_index()
    )

    for coluna in ("cpu_media", "cpu_pico", "ram_media", "flash_media"):
        resultado[coluna] = resultado[coluna].round(2)
    mapa_cpu_esperada = {nome: cpu for nome, _, cpu, _ in FASES_VOO}
    mapa_ram_esperada = {nome: ram for nome, _, _, ram in FASES_VOO}
    resultado["cpu_esperado"] = resultado["fase"].map(mapa_cpu_esperada)
    resultado["ram_esperada"] = resultado["fase"].map(mapa_ram_esperada)
    resultado["cpu_desvio_esperado"] = (resultado["cpu_media"] - resultado["cpu_esperado"]).round(2)
    resultado["ram_desvio_esperado"] = (resultado["ram_media"] - resultado["ram_esperada"]).round(2)
    return resultado

def processar_camada_gold(df_silver):
    # Cria KPIs por sessão, série por minuto, KPIs por fase, eventos de alerta e série diária.
    kpis_sessao = [
        calcular_kpis_sessao(grupo, dict(zip(CHAVES_AGRUPAMENTO, chave)))
        for chave, grupo in df_silver.groupby(CHAVES_AGRUPAMENTO, dropna=False, sort=False)
    ]
    # Agrega cada minuto e mantém a fase para não misturar, por exemplo, subida e cruzeiro.
    df_serie_1min = (
        df_silver.groupby(CHAVES_AGRUPAMENTO + ["fase", pd.Grouper(key="ts", freq="1min")], dropna=False, observed=True)
        .agg(amostras=("ts", "count"), cpu_media=("cpu_pct", "mean"), ram_media=("ram_pct", "mean"),
             flash_media=("flash_pct", "mean"), lacunas=("q_lacuna", lambda v: v.sum(min_count=1)))
        .reset_index()
    )
    medias = ["cpu_media", "ram_media", "flash_media"]
    df_serie_1min[medias] = df_serie_1min[medias].round(3)
    return (pd.DataFrame(kpis_sessao), df_serie_1min, calcular_kpis_por_fase(df_silver),
            vincular_componente_alertas(gerar_alertas(df_silver), df_silver), calcular_serie_diaria(df_silver))

def criar_resumo_fmc(df_silver: pd.DataFrame, df_resumo_placa: pd.DataFrame) -> pd.DataFrame:
    # FMC = soma dos alertas das placas; saúde = pior placa (decisão a confirmar).
    colunas_saida = [
        *CHAVES_FMC, "ultima_amostra_utc", "placas_monitoradas", "placas_com_alerta",
        "alertas_abertos", "alertas_criticos_abertos", "alertas_atencao_abertos",
        "completude_pct", "health_score", "health_score_status", "pior_placa", "estado_saude",
    ]
    linhas = []

    for host, placas in df_resumo_placa.groupby("hostname", sort=False):
        criticos = 0
        atencao = 0
        com_alerta = 0
        pior_health = np.nan
        pior_placa = None
        completude = np.nan

        for _, placa in placas.iterrows():
            criticos += int(placa["alertas_criticos_abertos"])
            atencao += int(placa["alertas_atencao_abertos"])
            if placa["alertas_abertos"] > 0:
                com_alerta += 1
            if pd.notna(placa["health_score"]) and (pd.isna(pior_health) or placa["health_score"] < pior_health):
                pior_health = placa["health_score"]
                pior_placa = placa["placa_serie"]
            if pd.notna(placa["completude_pct"]) and (pd.isna(completude) or placa["completude_pct"] < completude):
                completude = placa["completude_pct"]

        ultima_ts = df_silver.loc[df_silver["hostname"] == host, "ts"].max()
        linhas.append({
            "hostname": host,
            "ultima_amostra_utc": ultima_ts.isoformat(),
            "placas_monitoradas": len(placas),
            "placas_com_alerta": com_alerta,
            "alertas_abertos": criticos + atencao,
            "alertas_criticos_abertos": criticos,
            "alertas_atencao_abertos": atencao,
            "completude_pct": completude,
            "health_score": pior_health,
            "health_score_status": "OK" if pd.notna(pior_health) else "DADOS_INSUFICIENTES",
            "pior_placa": pior_placa,
            "estado_saude": _estado_saude(criticos, atencao, pior_health),
        })

    return pd.DataFrame(linhas, columns=colunas_saida)

def criar_resumo_placa(df_silver, df_kpis_sessao, df_alertas):
    # Um estado por placa (hostname + placa_serie): último KPI de sessão + alertas abertos.
    ativos = _alertas_ativos(df_alertas)
    linhas = []
    for (host, serie), grupo in df_silver.groupby(CHAVES_BASE, sort=False):
        ultima = grupo.sort_values("ts").iloc[-1]
        niveis = [n for (h, p, _), n in ativos.items() if h == host and p == serie]
        criticos, atencao = niveis.count("CRITICO"), niveis.count("ATENCAO")
        kpis = df_kpis_sessao[(df_kpis_sessao["hostname"] == host) & (df_kpis_sessao["placa_serie"] == serie)]
        kpi = kpis.sort_values("fim").iloc[-1] if not kpis.empty else pd.Series(dtype=object)
        health = kpi.get("health_score", np.nan)
        linhas.append({
            "hostname": host, "placa_serie": serie, "placa_componentes": ultima["placa_componentes"],
            "ultima_amostra_utc": ultima["ts"].isoformat(), "ultima_fase": ultima["fase"],
            "ultima_sessao": ultima["sessao"], "amostras_sessao": kpi.get("amostras", np.nan),
            "completude_pct": kpi.get("completude_pct", np.nan), "health_score": health,
            "health_score_status": kpi.get("health_score_status", "DADOS_INSUFICIENTES"),
            "alertas_abertos": criticos + atencao, "alertas_criticos_abertos": criticos,
            "alertas_atencao_abertos": atencao, "estado_saude": _estado_saude(criticos, atencao, health),
            "cpu_pico": kpi.get("cpu_pico", np.nan), "ram_pico": kpi.get("ram_pico", np.nan),
            "flash_max_pct": kpi.get("flash_max_pct", np.nan),
            "reinicios_detectados": int(ultima["sessao"]) - 1,
        })
    return pd.DataFrame(linhas)

def anexar_placa(df_gold: pd.DataFrame, df_silver: pd.DataFrame) -> pd.DataFrame:
     # Coloca os componentes da placa nas tabelas Gold, por (hostname, placa_serie).
    if df_gold.empty or "placa_componentes" in df_gold.columns:
        return df_gold
    ref = df_silver[CHAVES_BASE + ["placa_componentes"]].drop_duplicates(CHAVES_BASE)
    return df_gold.merge(ref, on=CHAVES_BASE, how="left")


def _alertas_ativos(df_alertas: pd.DataFrame) -> dict:
    # Repassa os eventos em ordem temporal e devolve só os alertas ainda abertos.
    ativos = {}
    if df_alertas.empty:
        return ativos
    eventos = df_alertas.sort_values("ts_evento", kind="mergesort")
    for _, evento in eventos.iterrows():
        chave = (str(evento["hostname"]), str(evento["placa_serie"]), str(evento["alerta_id"]))
        tipo = evento["tipo_evento"]
        if tipo == "ABERTURA":
            ativos[chave] = evento["nivel"]
        elif tipo in ("ESCALONAMENTO", "REBAIXAMENTO") and chave in ativos:
            ativos[chave] = evento["nivel"]
        elif tipo == "ENCERRAMENTO":
            ativos.pop(chave, None)
    return ativos


def _estado_saude(criticos: int, atencao: int, health) -> str:
    if criticos:
        return "CRITICO"
    if atencao:
        return "ATENCAO"
    if pd.isna(health):
        return "DADOS_INSUFICIENTES"
    return "SEM_ALERTAS_ATIVOS"

def gerar_datasets_gold(df_silver):
    df_sessao, df_serie, df_fase, df_alertas, df_diaria = processar_camada_gold(df_silver)
    df_resumo_placa = criar_resumo_placa(df_silver, df_sessao, df_alertas)
    df_resumo_fmc = criar_resumo_fmc(df_silver, df_resumo_placa)
    return {
        "gold_kpi_sessao": anexar_cadastro(anexar_placa(df_sessao, df_silver), df_silver),
        "gold_serie_1min": anexar_cadastro(anexar_placa(df_serie, df_silver), df_silver),
        "gold_serie_diaria": anexar_cadastro(anexar_placa(df_diaria, df_silver), df_silver),
        "gold_kpi_fase": anexar_cadastro(anexar_placa(df_fase, df_silver), df_silver),
        "gold_alertas": anexar_cadastro(df_alertas, df_silver),
        "gold_resumo_placa": anexar_cadastro(df_resumo_placa, df_silver),
        "gold_resumo_fmc": anexar_cadastro(df_resumo_fmc, df_silver),
    }
