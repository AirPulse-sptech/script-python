"""Normaliza o Bronze e enriquece as amostras com o cadastro do MySQL."""
import os

import numpy as np
import pandas as pd

from contratos import BYTES_POR_KB, CHAVES_BASE, CHAVES_AGRUPAMENTO, COLUNAS_CADASTRO, METRICAS_ALERTAVEIS


COLUNAS_BRONZE = [  # Contrato do agente.py usado aqui; faltou alguma = erro de esquema
    "timestamp_utc", "seq", "hostname", "placa_nome", "uptime_proc_s", "intervalo_coleta_s",
    "sessao_origem", "fase", "cpu_pct_host", "ram_usada_bytes", "ram_baseline_bytes",
    "ram_baseline_fonte", "ram_total_bytes", "ram_disponivel_bytes", "ram_medicao_tipo",
    "flash_usada_bytes", "flash_total_bytes",
    "io_leitura_contador_acum", "io_escrita_contador_acum", "io_leitura_unidade", "io_escrita_unidade",
]


MAPA_TIPO_METRICA = {"CPU": "cpu_pct", "RAM": "ram_pct", "DISCO": "flash_pct", "FLASH": "flash_pct"} #Disco e Flash são a mesma medida.

SQL_CADASTRO = (
    "SELECT c.hostname, c.idComputador, c.tipoFmc, c.statusComputador, "
    "a.idAeronave, a.nome, a.modelo, a.companhiaAerea, "
    "e.idEmpresaFabricante, COALESCE(NULLIF(e.nomeFantasia, ''), e.razaoSocial) "
    "FROM computador c "
    "JOIN aeronave a ON a.idAeronave = c.fkAeronave "
    "JOIN empresaFabricante e ON e.idEmpresaFabricante = a.fkEmpresaFabricante"
)

SQL_COMPONENTES = (
    "SELECT c.hostname, co.idComponente, co.tipo, p.nome, p.numeroSerie, "
    "co.limiteAtencao, co.limiteCritico "
    "FROM componente co "
    "JOIN placa p ON p.idPlaca = co.fkPlaca "
    "JOIN computador c ON c.idComputador = p.fkComputador "
    "WHERE co.statusMonitoramento = 'ATIVO'"
)


def _chave_texto(serie: pd.Series):
    # Compara texto sem diferenciar maiúsculas/minúsculas (Windows costuma devolver hostname em MAIÚSCULAS).
    return serie.astype(str).str.strip().str.lower()


def carregar_cadastro_do_banco():
    # Devolve (df_cadastro, df_componentes). Se o banco não responder, devolve (None, None).
    conexao = None
    try:
        import pymysql  # Importação tardia; MySQL continua opcional para testes locais.
        conexao = pymysql.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_DATABASE", "airpulse"),
            user=os.getenv("DB_USER", ""),
            password=os.getenv("DB_PASSWORD", ""),
            port=int(os.getenv("DB_PORT", "3306")),
            connect_timeout=5,
        )

        with conexao.cursor() as cursor:
            cursor.execute(SQL_CADASTRO)
            linhas_cadastro = cursor.fetchall()
            cursor.execute(SQL_COMPONENTES)
            linhas_componentes = cursor.fetchall()

    except Exception as erro:
        print(f"MySQL indisponível ({erro}).")
        return None, None
    
    finally:
        if conexao is not None:
            conexao.close()

    cadastro = pd.DataFrame(linhas_cadastro, columns=["hostname", *COLUNAS_CADASTRO])

    for coluna in ("computador_id", "aeronave_id", "empresa_id"):
        cadastro[coluna] = cadastro[coluna].astype("Int64")

    componentes = pd.DataFrame(linhas_componentes, columns=[
    "hostname", "componente_id", "tipo", "placa_nome", "placa_serie",
    "limiteAtencao", "limiteCritico"])

    componentes["metrica"] = componentes["tipo"].astype(str).str.upper().map(MAPA_TIPO_METRICA)

    componentes = componentes.dropna(subset=["metrica"]).reset_index(drop=True)

    for coluna in ("limiteAtencao", "limiteCritico"):
        componentes[coluna] = pd.to_numeric(componentes[coluna], errors="coerce")

    print(f"Cadastro carregado: {len(cadastro)} computador(es), {len(componentes)} componente(s) ativo(s).")

    return cadastro, componentes


def _vincular_cadastro(df, cadastro, componentes):
    # Liga hostname -> computador/aeronave/empresa e (hostname, placa_nome do Bronze) -> placa.numeroSerie, componentes e limites.
    df = df.copy()
    df["_chave_host"] = _chave_texto(df["hostname"])
    df["_chave_placa"] = _chave_texto(df["placa_nome"])

    # BLOCO A: computador -> aeronave -> empresa (hostname duplicado no cadastro estoura no validate).
    ref_cadastro = cadastro.assign(_chave_host=_chave_texto(cadastro["hostname"])).drop(columns="hostname")
    df = df.merge(ref_cadastro, on="_chave_host", how="left", validate="many_to_one")
    df["q_hostname_nao_cadastrado"] = df["computador_id"].isna()

    # BLOCO B: placa ativa do hostname -> placa_serie e tipos de componente.
    comp = componentes.assign(
        _chave_host=_chave_texto(componentes["hostname"]),
        _chave_placa=_chave_texto(componentes["placa_nome"]),
        tipo=componentes["tipo"].astype(str).str.upper().replace("FLASH", "DISCO"),
    )
    chaves_placa = ["_chave_host", "_chave_placa"]
    if (comp.groupby(chaves_placa)["placa_serie"].nunique() > 1).any():
        raise ValueError("Placas com o mesmo nome no mesmo computador: o Bronze só traz placa_nome. Renomeie no cadastro.")
    ref_placas = (
        comp.groupby(chaves_placa, sort=False)
        .agg(placa_serie=("placa_serie", "first"),
             placa_componentes=("tipo", lambda t: ",".join(sorted(set(t))))) #lambda cria uma função como o => ("arrow function") 
        .reset_index()
    )
    df = df.merge(ref_placas, on=chaves_placa, how="left", validate="many_to_one")
    df["q_placa_nao_cadastrada"] = df["placa_serie"].isna()

    # BLOCO C: limites por hostname + placa + métrica; o par só vale com os dois valores e atenção < crítico.
    chaves = pd.MultiIndex.from_frame(df[chaves_placa])
    for metrica in METRICAS_ALERTAVEIS:
        lim = comp[comp["metrica"] == metrica].drop_duplicates(chaves_placa).set_index(chaves_placa)
        atencao = pd.Series(lim["limiteAtencao"].reindex(chaves).to_numpy(), index=df.index)
        critico = pd.Series(lim["limiteCritico"].reindex(chaves).to_numpy(), index=df.index)
        valido = atencao < critico
        ids = lim["componente_id"].reindex(chaves).array
        df[f"componente_id_{metrica}"] = pd.array(ids, dtype="Int64")
        df[f"limiar_{metrica}_atencao"] = atencao.where(valido)
        df[f"limiar_{metrica}_critico"] = critico.where(valido)
    return df.drop(columns=chaves_placa)


def _inferir_intervalo(grupo: pd.DataFrame):
    # Usa metadado consistente; sem metadado exige cadência repetida nos timestamps.
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


def processar_camada_silver(df_bronze: pd.DataFrame, cadastro, componentes):
    # Transforma Bronze sem preencher métricas ou identidades desconhecidas com valores mentirosos.
    faltantes = [c for c in COLUNAS_BRONZE if c not in df_bronze.columns]
    if faltantes:
        raise ValueError("Colunas ausentes no Bronze: " + ", ".join(faltantes))
    # Timestamp inválido vira NaT e a amostra sai; sem horário não há ordenação temporal.
    df = df_bronze.assign(ts=pd.to_datetime(df_bronze["timestamp_utc"], utc=True, errors="coerce")).dropna(subset=["ts"])
    if df.empty:
        raise ValueError("Nenhuma amostra tem timestamp_utc válido; Bronze foi preservado sem alteração.")
    for coluna in ("hostname", "placa_nome"):
        vazios = df[coluna].isna() | df[coluna].astype(str).str.strip().eq("")
        if vazios.any():
            raise ValueError(f"Identificador obrigatório '{coluna}' ausente em {int(vazios.sum())} amostra(s); "
                             "não é seguro agrupar equipamentos desconhecidos.")
    # Placa sem DISCO não mede I/O: unidade ausente = desconhecida (nunca vira zero).
    for tipo in ("leitura", "escrita"):
        df[f"io_{tipo}_unidade"] = df[f"io_{tipo}_unidade"].fillna("desconhecida")

    # CPU normalizada vem do agente; não divide de novo por núcleos.
    df["cpu_pct"] = df["cpu_pct_host"].where(df["cpu_pct_host"].between(0, 100))

    # RAM: baseline só é aplicada quando a origem declara calibração (fonte diferente de SEM_SUBTRACAO).
    baseline_valida = df["ram_baseline_fonte"].notna() & df["ram_baseline_fonte"].ne("SEM_SUBTRACAO")
    baseline_aplicada = df["ram_baseline_bytes"].where(baseline_valida, 0.0)
    df["ram_logica_bytes"] = (df["ram_usada_bytes"].where(df["ram_usada_bytes"] >= 0) - baseline_aplicada).clip(lower=0)
    # Total igual a zero não pode servir como divisor; disponível maior que o total de referência é inválido.
    ram_total_valido = df["ram_total_bytes"].where(df["ram_total_bytes"] > 0)
    ram_disp_invalida = df["ram_disponivel_bytes"].notna() & (
        (df["ram_disponivel_bytes"] < 0) | (df["ram_disponivel_bytes"] > ram_total_valido))
    df["ram_disponivel_bytes"] = df["ram_disponivel_bytes"].where(~ram_disp_invalida)
    df["ram_disponivel_pct"] = df["ram_disponivel_bytes"] / ram_total_valido * 100
    df["ram_pct"] = df["ram_logica_bytes"] / ram_total_valido * 100
    flash_total_valido = df["flash_total_bytes"].where(df["flash_total_bytes"] > 0)
    df["flash_pct"] = df["flash_usada_bytes"].where(df["flash_usada_bytes"] >= 0) / flash_total_valido * 100
    # A fase vem dos metadados da bancada; sem ela, desconhecida (não deduz pelo uptime).
    df["fase"] = df["fase"].fillna("NAO_DETERMINADA").astype(str).str.strip().replace("", "NAO_DETERMINADA")

    df = _vincular_cadastro(df, cadastro, componentes)
    # Compara cada métrica com os limites do próprio computador; sem limite, não inventa estado de alerta.
    for metrica in METRICAS_ALERTAVEIS:
        lim_atencao, lim_critico = df[f"limiar_{metrica}_atencao"], df[f"limiar_{metrica}_critico"]
        valores = df[metrica]
        configurado = lim_atencao.notna() & lim_critico.notna()
        em_critico = configurado & valores.ge(lim_critico)
        em_atencao = configurado & valores.ge(lim_atencao) & valores.lt(lim_critico)
        nivel = pd.Series("NORMAL", index=df.index, dtype="object")
        nivel.loc[~configurado] = "NAO_CONFIGURADO"
        nivel.loc[configurado & valores.isna()] = "NAO_DETERMINADO"
        nivel.loc[em_atencao] = "ATENCAO"
        nivel.loc[em_critico] = "CRITICO"
        df[f"nivel_{metrica}"] = nivel
        nao_avaliavel = ~configurado | valores.isna()
        df[f"q_{metrica}_atencao"] = em_atencao.astype("boolean").mask(nao_avaliavel)
        df[f"q_{metrica}_critico"] = em_critico.astype("boolean").mask(nao_avaliavel)

    # Ordena por identidade e timestamp para que diferenças e alertas sigam a ordem temporal.
    df = df.sort_values(CHAVES_BASE + ["ts"], kind="mergesort", na_position="last").reset_index(drop=True)
    # Duplicata é do Bronze (placa_nome), antes do mapeamento p/ série; keep="last" preserva a última versão.
    duplicidade = ["hostname", "placa_nome", "ts", "sessao_origem"]
    quantidade_duplicadas = int(df.duplicated(subset=duplicidade, keep="last").sum())
    if quantidade_duplicadas:
        df = df.drop_duplicates(subset=duplicidade, keep="last").reset_index(drop=True)
        print(f"Silver: descartadas {quantidade_duplicadas} amostras duplicadas; Bronze mantido intacto.")

    # Reinício = contador de tempo/seq volta para trás ou sessão muda; abre um novo grupo de sessão.
    grupos_base = df.groupby(CHAVES_BASE, dropna=False, sort=False)
    reinicio = grupos_base["uptime_proc_s"].diff().lt(0) | grupos_base["seq"].diff().lt(0)
    sessao_anterior = grupos_base["sessao_origem"].shift()
    reinicio |= df["sessao_origem"].notna() & sessao_anterior.notna() & df["sessao_origem"].ne(sessao_anterior)
    df["_reinicio"] = reinicio
    df["sessao"] = df.groupby(CHAVES_BASE, dropna=False, sort=False)["_reinicio"].cumsum().astype(int) + 1

    # Descobre a cadência por grupo de sessão. Se o agente informou um intervalo
    # consistente, usa esse valor; se não, só infere quando os timestamps repetem a cadência.
    df["intervalo_esperado_s"] = np.nan
    df["intervalo_origem"] = "INDETERMINADO"
    for _, grupo in df.groupby(CHAVES_AGRUPAMENTO, dropna=False, sort=False):
        intervalo, origem_intervalo = _inferir_intervalo(grupo)
        df.loc[grupo.index, "intervalo_esperado_s"] = intervalo
        df.loc[grupo.index, "intervalo_origem"] = origem_intervalo
    grupos_sessao = df.groupby(CHAVES_AGRUPAMENTO, dropna=False, sort=False)
    # diff compara cada timestamp com a amostra anterior do mesmo grupo.
    # O resultado é o tempo real decorrido entre duas leituras consecutivas.
    df["dt_s"] = grupos_sessao["ts"].diff().dt.total_seconds()
    df["t_sessao_s"] = (df["ts"] - grupos_sessao["ts"].transform("min")).dt.total_seconds()
    intervalo_seguro = df["intervalo_esperado_s"].where(df["intervalo_esperado_s"] > 0)
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
    # Taxas diferenciais de I/O, sem converter caracteres em bytes e sem transformar reset em zero.
    for tipo in ("leitura", "escrita"):
        # diff obtém quanto o contador cresceu desde a amostra anterior.
        # Se mudou a unidade, houve reset ou o intervalo é inválido, a taxa fica desconhecida.
        coluna = f"io_{tipo}_contador_acum"
        unidade = f"io_{tipo}_unidade"
        delta = grupos_sessao[coluna].diff()
        unidade_estavel = df[unidade].eq(grupos_sessao[unidade].shift()) & df[unidade].isin(["bytes", "chars"])
        delta_valido = delta.where(unidade_estavel & (delta >= 0) & (df["dt_s"] > 0))
        df[f"io_{tipo}_kbps"] = (delta_valido / df["dt_s"] / BYTES_POR_KB).where(df[unidade].eq("bytes"))
        df[f"io_{tipo}_chars_s"] = (delta_valido / df["dt_s"]).where(df[unidade].eq("chars"))
        
    return df.drop(columns="_reinicio")
