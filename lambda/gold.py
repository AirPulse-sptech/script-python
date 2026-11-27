"""Calcula séries, KPIs e eventos de alerta usando o contrato Silver."""
import numpy as np
import pandas as pd

from contratos import CHAVES_AGRUPAMENTO, CHAVES_BASE, METRICAS_ALERTAVEIS


FUSO_BRASILIA = "America/Sao_Paulo"
UMA_HORA = pd.Timedelta(hours=1)
UM_DIA = pd.Timedelta(days=1)
UM_MINUTO = pd.Timedelta(minutes=1)
UM_SEGUNDO = pd.Timedelta(seconds=1)
TOLERANCIA_HORA = pd.Timedelta(minutes=5)
SAUDE_ATENCAO = 70
SAUDE_CRITICO = 40
SEVERIDADE = {"NORMAL": 0, "ATENCAO": 1, "CRITICO": 2}
NIVEIS = {codigo: nivel for nivel, codigo in SEVERIDADE.items()}
IDS_CADASTRO = ["computador_id", "aeronave_id", "empresa_id"]
NOMES_METRICAS = {"cpu_pct": "cpu", "ram_pct": "ram", "flash_pct": "flash"}

PERSISTENCIA_AMOSTRAS = 3
HISTERESE_PTS = 5.0
PESO_ATENCAO = 0.5
PESO_CRITICO = 2.0
PENALIDADE_COMPLETUDE_POR_PONTO = 0.3
MIN_AMOSTRAS_HEALTH = 3


def vincular_componente_alertas(df_alertas, df_silver):
    # Usa os mesmos IDs de componentes e limites associados na etapa Silver.
    chaves = CHAVES_BASE
    colunas = [f"componente_id_{metrica}" for metrica in METRICAS_ALERTAVEIS]
    componentes = df_silver[chaves + colunas].drop_duplicates(chaves)
    componentes = componentes.melt(
        id_vars=chaves, value_vars=colunas, var_name="metrica", value_name="componente_id",
    )
    componentes["metrica"] = componentes["metrica"].str.removeprefix("componente_id_")
    cadastro = df_silver[["hostname", *IDS_CADASTRO]].drop_duplicates("hostname")
    return df_alertas.merge(componentes, on=chaves + ["metrica"], how="left").merge(
        cadastro, on="hostname", how="left"
    )


def _amostras_consecutivas(grupo):
    ausente = pd.Series(np.nan, index=grupo.index)
    intervalo = grupo.get("intervalo_esperado_s", ausente)
    tempo_entre_amostras = grupo.get("dt_s", ausente)
    consecutivas = tempo_entre_amostras.gt(0) & tempo_entre_amostras.le(intervalo * 1.5)
    lacunas = grupo.get("q_lacuna", ausente).eq(True).fillna(False)
    return (consecutivas & ~lacunas).fillna(False)


def _eventos_componente(grupo, identificadores, metrica, limites, ocorrencias):
    limite_atencao, limite_critico = limites
    valores = pd.to_numeric(grupo[metrica], errors="coerce")
    niveis = grupo.get(f"nivel_{metrica}", pd.Series(None, index=grupo.index))
    consecutivas = _amostras_consecutivas(grupo)
    nivel_ativo = None
    nivel_pendente, quantidade = None, 0

    for indice, (ts, valor, nivel_leitura, consecutiva) in enumerate(
        zip(grupo["ts"], valores, niveis, consecutivas)
    ):
        if pd.isna(valor):
            nivel_pendente, quantidade = None, 0
            continue

        # Abertura e escalonamento exigem três leituras seguidas no mesmo nível.
        nivel_leitura = nivel_leitura if pd.notna(nivel_leitura) else None
        pode_abrir = nivel_ativo is None and nivel_leitura in ("ATENCAO", "CRITICO")
        pode_subir = nivel_ativo == "ATENCAO" and nivel_leitura == "CRITICO"
        if pode_abrir or pode_subir:
            if not consecutiva or nivel_leitura != nivel_pendente:
                nivel_pendente, quantidade = nivel_leitura, 0
                inicio_sequencia = indice
            quantidade += 1
            if quantidade < PERSISTENCIA_AMOSTRAS:
                continue
            if pode_abrir:
                evento = "ABERTURA"
                ts_abertura = grupo.at[inicio_sequencia, "ts"]
                hostname, placa_serie = identificadores["hostname"], identificadores["placa_serie"]
                alerta_id = f"{hostname}:{placa_serie}:{metrica}:{ts_abertura.isoformat()}"
                ocorrencias[alerta_id] = {
                    "valor_abertura_pct": round(float(valores.iat[inicio_sequencia]), 4),
                    "maior_nivel": nivel_leitura,
                }
            else:
                evento = "ESCALONAMENTO"
                ocorrencias[alerta_id]["maior_nivel"] = "CRITICO"
            nivel_evento = nivel_leitura
        else:
            # A margem na recuperação evita mudanças quando o valor oscila perto do limite.
            nivel_pendente, quantidade = None, 0
            if nivel_ativo is not None and valor < limite_atencao - HISTERESE_PTS:
                evento, nivel_evento = "ENCERRAMENTO", nivel_ativo
            elif nivel_ativo == "CRITICO" and valor < limite_critico - HISTERESE_PTS:
                evento, nivel_evento = "REBAIXAMENTO", "ATENCAO"
            else:
                continue

        nivel_pendente, quantidade = None, 0
        em_aberto = evento != "ENCERRAMENTO"
        nivel_ativo = nivel_evento if em_aberto else None
        yield {
            **identificadores,
            "alerta_id": alerta_id,
            "metrica": metrica,
            "tipo_evento": evento,
            "nivel": nivel_evento,
            "nivel_apos_evento": nivel_ativo or "NORMAL",
            "valor": round(float(valor), 4),
            "limiar_violado": limite_critico if nivel_evento == "CRITICO" else limite_atencao,
            "ts_abertura": ts_abertura,
            "ts_evento": ts,
            "ts_encerramento": None if em_aberto else ts,
            "em_aberto_apos_evento": em_aberto,
        }


def gerar_alertas(df_silver: pd.DataFrame, *, incluir_sessao=False):
    registros = []
    ocorrencias = {}
    for chaves, leituras in df_silver.groupby(CHAVES_AGRUPAMENTO, dropna=False, sort=False):
        identificadores = dict(zip(CHAVES_AGRUPAMENTO, chaves))
        grupo = leituras.sort_values("ts", kind="stable").reset_index(drop=True)
        for metrica in METRICAS_ALERTAVEIS:
            limites = _limites_do_grupo(grupo, metrica)
            if limites is not None:
                registros.extend(_eventos_componente(grupo, identificadores, metrica, limites, ocorrencias))

    for registro in registros:
        registro.update(ocorrencias[registro["alerta_id"]])
    colunas = [
        *(CHAVES_AGRUPAMENTO if incluir_sessao else CHAVES_BASE),
        "alerta_id", "metrica", "tipo_evento", "nivel", "nivel_apos_evento", "valor",
        "limiar_violado", "ts_abertura", "ts_evento", "ts_encerramento",
        "em_aberto_apos_evento", "valor_abertura_pct", "maior_nivel",
    ]
    return (
        pd.DataFrame(registros, columns=colunas)
        .sort_values(["ts_evento", "alerta_id"], kind="stable")
        .reset_index(drop=True)
    )


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
    if len(atencao) != 1 or len(critico) != 1 or not atencao[0] < critico[0]:
        return None
    return float(atencao[0]), float(critico[0])

def _calcular_health_score(grupo: pd.DataFrame, completude: float):
    # Score por severidade máxima por amostra; crítico não acumula atenção também.
    # Primeiro identifica quais métricas têm limites válidos para execução.
    metricas_configuradas = [
        metrica for metrica in METRICAS_ALERTAVEIS if _limites_do_grupo(grupo, metrica)
    ]
    if not metricas_configuradas or pd.isna(completude):
        return np.nan
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
        return np.nan
    pct_critico = float((critico_algum & amostra_valida).sum() / denominador * 100)
    pct_atencao = float((atencao_alguma & ~critico_algum & amostra_valida).sum() / denominador * 100)
    # Penaliza atenção e crítico com pesos diferentes e também considera a falta de dados.
    penalidade = (
        PESO_ATENCAO * pct_atencao
        + PESO_CRITICO * pct_critico
        + max(0.0, 100.0 - float(completude)) * PENALIDADE_COMPLETUDE_POR_PONTO
    )

    score = max(0.0, min(100.0, 100.0 - penalidade))
    return round(score, 1)

def _minutos_criticos(grupo, metrica):
    niveis = grupo[f"nivel_{metrica}"]
    if not niveis.isin(SEVERIDADE).any():
        return np.nan
    cadencia = grupo["intervalo_esperado_s"]
    dt = grupo["dt_s"]
    mesma_sessao = grupo["coleta_id"].eq(grupo["coleta_id"].shift()) & grupo["sessao"].eq(grupo["sessao"].shift())
    consecutivo = mesma_sessao & dt.gt(0) & dt.le(cadencia * 1.5)
    nivel_anterior = niveis.shift()
    tempo_medido = dt.where(consecutivo & nivel_anterior.isin(SEVERIDADE) & niveis.isin(SEVERIDADE))
    if not tempo_medido.notna().any():
        return np.nan
    return round(tempo_medido.where(nivel_anterior.eq("CRITICO"), 0).sum() / 60, 2)


def _agrupar_sessoes(grupo):
    # O identificador do voo continua valendo quando há mais de um arquivo de coleta.
    chave_local = grupo["coleta_id"] + ":" + grupo["sessao"].astype(str)
    voo = grupo["sessao_origem"].replace("", pd.NA)
    return grupo.groupby(("voo:" + voo).fillna("coleta:" + chave_local), sort=False)


def calcular_kpis_sessoes(grupo, coleta_id):
    sessoes = list(_agrupar_sessoes(grupo))
    for sessao_id, atual in sessoes:
        if not atual["coleta_id"].eq(coleta_id).any():
            continue
        inicio = atual["ts"].min()
        anteriores = [
            outra for outra_id, outra in sessoes
            if outra_id != sessao_id and outra["ts"].max() < inicio
        ]
        anterior = max(anteriores, key=lambda sessao: sessao["ts"].max(), default=None)
        kpis = {
            "sessao_id": sessao_id, "inicio": inicio, "fim": atual["ts"].max(),
        }
        for metrica, nome in NOMES_METRICAS.items():
            valores = atual[metrica].dropna()
            pico = valores.max()
            amostra_pico = atual.loc[valores.idxmax()] if not valores.empty else None
            pico_anterior = anterior[metrica].max() if anterior is not None else np.nan
            kpis.update({
                f"{nome}_pico": round(pico, 2),
                f"{nome}_ts_pico": amostra_pico["ts"] if amostra_pico is not None else pd.NaT,
                f"{nome}_fase_pico": amostra_pico["fase"] if amostra_pico is not None else None,
                f"{nome}_p95": round(valores.quantile(0.95), 2),
                f"{nome}_delta_pico_pp": round(pico - pico_anterior, 2),
                f"{nome}_minutos_acima_critico": _minutos_criticos(atual, metrica),
            })
        yield kpis


def _duracao_alertas_minuto(silver, proximos_instantes, metrica, nome):
    nivel = silver[f"nivel_{metrica}"]
    intervalos = silver.loc[nivel.isin(["ATENCAO", "CRITICO"]) & proximos_instantes.notna(), CHAVES_BASE + ["ts"]]
    intervalos = intervalos.assign(fim=proximos_instantes, nivel=nivel)
    registros = []
    for hostname, placa, inicio, fim, estado in intervalos.itertuples(index=False, name=None):
        while inicio < fim:
            minuto = inicio.floor("min")
            fim_trecho = min(fim, minuto + UM_MINUTO)
            registros.append((hostname, placa, minuto, estado, (fim_trecho - inicio).value))
            inicio = fim_trecho

    chaves_minuto = CHAVES_BASE + ["ts"]
    colunas_duracao = {"CRITICO": f"{nome}_segundos_critico", "ATENCAO": f"{nome}_segundos_atencao"}
    if not registros:
        return silver[chaves_minuto].iloc[:0].assign(**dict.fromkeys(colunas_duracao.values(), 0.0))
    duracoes = pd.DataFrame(registros, columns=chaves_minuto + ["nivel", "nanossegundos"])
    por_nivel = duracoes.groupby(chaves_minuto + ["nivel"])["nanossegundos"].sum().unstack("nivel")
    return (
        por_nivel.reindex(columns=colunas_duracao).div(UM_SEGUNDO.value)
        .rename(columns=colunas_duracao).reset_index().rename_axis(columns=None)
    )


def calcular_serie_minuto(silver, alertas=None):
    if alertas is None:
        alertas = gerar_alertas(silver, incluir_sessao=True)
    silver = silver.sort_values("ts", kind="stable").reset_index(drop=True)
    chaves_amostras = silver[CHAVES_AGRUPAMENTO + ["ts"]]
    # Conta até a próxima leitura consecutiva da sessão, sem extrapolar lacunas ou a última amostra.
    ts_consecutivo = silver["ts"].where(_amostras_consecutivas(silver))
    proximos_instantes = ts_consecutivo.groupby(
        [silver[chave] for chave in CHAVES_AGRUPAMENTO], dropna=False, sort=False
    ).shift(-1)
    agregacoes = {}
    for metrica, nome in NOMES_METRICAS.items():
        nivel = f"nivel_{metrica}"
        avaliada = silver[nivel].isin(SEVERIDADE)
        estado = pd.Series("NORMAL", index=silver.index).where(avaliada)
        eventos = alertas.loc[
            alertas["metrica"].eq(metrica), CHAVES_AGRUPAMENTO + ["ts_evento", "nivel_apos_evento"]
        ]
        if not eventos.empty:
            # Cada amostra herda o estado da última transição confirmada da própria sessão.
            eventos = eventos.rename(columns={"ts_evento": "ts"}).astype(chaves_amostras.dtypes.to_dict())
            estados_confirmados = pd.merge_asof(
                chaves_amostras, eventos, on="ts", by=CHAVES_AGRUPAMENTO,
            )
            estado = estados_confirmados["nivel_apos_evento"].fillna(estado)
        silver[nivel] = estado
        agregacoes[f"{nome}_media"] = (metrica, "mean")
        agregacoes[f"{nome}_pico"] = (metrica, "max")
        agregacoes[f"{nome}_nivel"] = (nivel, lambda estados: estados.iloc[-1])
    serie = (
        silver.groupby(CHAVES_BASE + [pd.Grouper(key="ts", freq="1min")], observed=True)
        .agg(**agregacoes)
        .reset_index()
    )
    serie = serie.round({coluna: 3 for coluna in agregacoes if not coluna.endswith("_nivel")})
    for metrica, nome in NOMES_METRICAS.items():
        duracoes = _duracao_alertas_minuto(silver, proximos_instantes, metrica, nome)
        serie = serie.merge(duracoes, on=CHAVES_BASE + ["ts"], how="outer")
        critico, atencao = f"{nome}_segundos_critico", f"{nome}_segundos_atencao"
        serie[[critico, atencao]] = serie[[critico, atencao]].fillna(0.0)
        nivel = pd.Series("NORMAL", index=serie.index).where(serie[f"{nome}_nivel"].notna())
        nivel = nivel.mask(serie[atencao].gt(serie[critico]), "ATENCAO")
        nivel = nivel.mask(serie[critico].gt(0) & serie[critico].ge(serie[atencao]), "CRITICO")
        serie[f"{nome}_nivel_predominante"] = nivel
    return serie


def classificar_eventos_por_minuto(alertas, serie):
    eventos = alertas.drop(columns=["sessao", "nivel_apos_evento"])
    if eventos.empty:
        return eventos

    campos = {f"{nome}_nivel_predominante": metrica for metrica, nome in NOMES_METRICAS.items()}
    chaves = CHAVES_BASE + ["minuto"]
    niveis = serie.rename(columns={"ts": "minuto", **campos}).melt(
        id_vars=chaves, value_vars=list(campos.values()), var_name="metrica", value_name="nivel_predominante",
    )
    niveis = niveis.loc[niveis["nivel_predominante"].isin(["ATENCAO", "CRITICO"])]
    eventos = eventos.assign(minuto=eventos["ts_evento"].dt.floor("min")).merge(
        niveis, on=chaves + ["metrica"], how="left", validate="many_to_one",
    )
    eventos["nivel"] = eventos["nivel_predominante"].fillna(eventos["nivel"])
    return eventos.drop(columns=["minuto", "nivel_predominante"])


def _saude_hora(grupo, fim):
    janela = grupo.loc[grupo["ts"].gt(fim - UMA_HORA) & grupo["ts"].le(fim)]
    cadencia = janela["intervalo_esperado_s"].dropna().unique()
    if len(cadencia) != 1 or cadencia[0] <= 0:
        return np.nan
    completude = min(100.0, len(janela) * float(cadencia[0]) / 3600 * 100)
    return _calcular_health_score(janela, completude)


def _valor_referencia(grupo, metrica, instante):
    leituras = grupo.loc[grupo[metrica].notna() & grupo["ts"].lt(instante)]
    if metrica == "flash_pct":
        ontem = instante.tz_convert(FUSO_BRASILIA).date() - UM_DIA
        dias_leitura = leituras["ts"].dt.tz_convert(FUSO_BRASILIA).dt.date
        leituras = leituras.loc[dias_leitura.eq(ontem), metrica]
        return leituras.iloc[-1] if not leituras.empty else np.nan

    distancia = (leituras["ts"] - (instante - UMA_HORA)).abs()
    perto_da_referencia = distancia.loc[distancia.le(TOLERANCIA_HORA)]
    if perto_da_referencia.empty:
        return np.nan
    return leituras.at[perto_da_referencia.idxmin(), metrica]


def _dias_ate_limite_disco(grupo, instante, valor, limite):
    recentes = grupo.loc[grupo["ts"].gt(instante - UM_DIA) & grupo["ts"].le(instante)]
    por_minuto = recentes.groupby(pd.Grouper(key="ts", freq="1min"))["flash_pct"].mean().dropna()
    horas = (por_minuto.index - instante).total_seconds() / 3600
    tendencia = regressao_linear(pd.Series(horas), por_minuto.reset_index(drop=True))
    if tendencia > 0 and valor < limite:
        return round((limite - valor) / tendencia / 24, 2)
    return np.nan


def criar_resumo_placa(grupo, atual):
    ultima = atual.iloc[-1]
    fim = ultima["ts"]
    saude = _saude_hora(grupo, fim)
    saude_anterior = _saude_hora(grupo, fim - UMA_HORA)
    if pd.isna(saude):
        estado_saude = "SEM_DADOS"
    elif saude < SAUDE_CRITICO:
        estado_saude = "CRITICO"
    elif saude < SAUDE_ATENCAO:
        estado_saude = "ATENCAO"
    else:
        estado_saude = "NORMAL"

    resumo = {
        **{coluna: ultima[coluna] for coluna in CHAVES_BASE},
        "ultima_amostra_utc": fim,
        "health_score": saude, "health_delta_pontos": round(saude - saude_anterior, 1),
        "health_limite_atencao": SAUDE_ATENCAO, "health_limite_critico": SAUDE_CRITICO,
        "estado_saude": estado_saude,
    }
    severidades = []
    for metrica, nome in NOMES_METRICAS.items():
        leituras = atual.loc[atual[metrica].notna()]
        leitura = leituras.iloc[-1] if not leituras.empty else ultima
        instante = leitura["ts"]
        nivel = leitura[f"nivel_{metrica}"]
        if nivel in SEVERIDADE:
            severidades.append(SEVERIDADE[nivel])
        valor = leitura[metrica]
        referencia = _valor_referencia(grupo, metrica, instante)
        limite_atencao = leitura[f"limiar_{metrica}_atencao"]
        resumo.update({
            f"{nome}_pct": round(valor, 2),
            f"{nome}_ts": instante if pd.notna(valor) else pd.NaT,
            f"{nome}_nivel": nivel if nivel in SEVERIDADE else None,
            f"{nome}_limite_atencao": limite_atencao,
            f"{nome}_limite_critico": leitura[f"limiar_{metrica}_critico"],
            f"{nome}_delta_pp": round(valor - referencia, 2),
        })
        if metrica == "flash_pct":
            resumo["flash_dias_ate_atencao"] = _dias_ate_limite_disco(grupo, instante, valor, limite_atencao)
    resumo["estado_geral"] = NIVEIS[max(severidades)] if severidades else "SEM_DADOS"
    return resumo


def gerar_datasets_gold(silver, historico, coleta_id):
    alertas = gerar_alertas(silver, incluir_sessao=True)
    serie = calcular_serie_minuto(silver, alertas)
    grupos_historico = dict(tuple(historico.groupby(CHAVES_BASE, sort=False)))
    monitoramento_placas = []
    for identidade, atual in silver.groupby(CHAVES_BASE, sort=False):
        passado = grupos_historico.get(identidade, atual.iloc[:0])
        passado = passado.loc[passado["ts"].le(atual["ts"].max())]
        grupo = pd.concat([passado, atual], ignore_index=True).sort_values("ts", kind="stable")
        grupo = grupo.drop_duplicates(["ts", "sessao_origem"], keep="last").reset_index(drop=True)
        monitoramento = criar_resumo_placa(grupo, atual.sort_values("ts"))
        monitoramento["sessoes"] = list(calcular_kpis_sessoes(grupo, coleta_id))
        monitoramento_placas.append(monitoramento)

    return {
        "gold_alertas": vincular_componente_alertas(classificar_eventos_por_minuto(alertas, serie), silver),
        "gold_monitoramento_placa": pd.DataFrame(monitoramento_placas),
        "gold_serie_1min": serie,
    }
