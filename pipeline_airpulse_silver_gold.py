# ETL AirPulse: Bronze -> Silver -> Gold usando o esquema do agente atual.
#
# O agente fornece timestamps, o hostname (= computador.nome no cadastro), CPU normalizada para o
# host, métricas de RAM/Flash, fase, intervalo de coleta e contadores de I/O com
# unidades explícitas. Este pipeline não reconstrói métricas a partir de colunas
# antigas: quando uma medição canônica estiver ausente, ela permanece desconhecida.
import argparse
import io
import json
import os
import re
from pathlib import Path
import numpy as np
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
# ============================================================
# 1. CONFIGURAÇÕES E CONTRATOS
# ============================================================
# Configuração comum de S3 entre agente e ETL. Em EC2, use IAM Role.
BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_PREFIXO_BRONZE = os.getenv("S3_PREFIXO_BRONZE", "01-bronze").strip("/")
ARQUIVO_BRONZE = os.getenv("ARQUIVO_BRONZE")
ARQUIVO_ESTADO_BRONZE = os.getenv("ARQUIVO_ESTADO_BRONZE", "bronze_ativo.json")
ETL_ORIGEM_PADRAO = os.getenv("ETL_ORIGEM", "auto").strip().lower()
BRONZE_HOSTNAME_PADRAO = os.getenv("BRONZE_HOSTNAME")
BRONZE_S3_KEY_PADRAO = os.getenv("BRONZE_S3_KEY")
BRONZE_MANIFEST_KEY_PADRAO = os.getenv("BRONZE_MANIFEST_KEY")
PASTA_SILVER = Path(os.getenv("PASTA_SILVER", "silver"))
PASTA_GOLD = Path(os.getenv("PASTA_GOLD", "gold"))


def _segmento_s3(valor):
    segmento = re.sub(r"[^A-Za-z0-9._-]+", "_", str(valor or "desconhecido").strip())
    return segmento.strip("._-") or "desconhecido"


def _chave_manifesto_por_hostname(hostname):
    return (
        f"{S3_PREFIXO_BRONZE}/manifestos/hostname={_segmento_s3(hostname)}/bronze_ativo.json"
    )


def obter_cliente_s3():
    #Usa credenciais quando fornecidas, senão, usa o padrão do boto3/IAM Role
    import boto3

    configuracao = {"region_name": AWS_REGION}
    chave = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("aws_access_key_id")
    segredo = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("aws_secret_access_key")
    token = os.getenv("AWS_SESSION_TOKEN") or os.getenv("aws_session_token")
    if chave and segredo:
        configuracao["aws_access_key_id"] = chave
        configuracao["aws_secret_access_key"] = segredo
        if token:
            configuracao["aws_session_token"] = token
    elif chave or segredo or token:
        raise ValueError(
            "Credenciais AWS incompletas. Informe Access Key e Secret Key juntas, "
            "ou remova credenciais estáticas para usar perfil/IAM Role."
        )
    return boto3.client("s3", **configuracao)


def selecionar_arquivo_bronze():
    #Localiza o Bronze local para modo local ou auto, sem depender de um CSV na EC2.
    if ARQUIVO_BRONZE:
        if os.path.exists(ARQUIVO_BRONZE):
            return ARQUIVO_BRONZE
        raise FileNotFoundError(f"ARQUIVO_BRONZE='{ARQUIVO_BRONZE}' não existe neste computador.")
    if os.path.exists(ARQUIVO_ESTADO_BRONZE):
        try:
            with open(ARQUIVO_ESTADO_BRONZE, encoding="utf-8") as arquivo:
                estado = json.load(arquivo)
        except (OSError, json.JSONDecodeError) as erro:
            raise RuntimeError(f"Não foi possível ler {ARQUIVO_ESTADO_BRONZE}: {erro}") from erro
        caminho = estado.get("arquivo_bronze") if isinstance(estado, dict) else None
        if caminho and os.path.exists(caminho):
            return caminho
        # Se o manifesto só tem a referência S3, o modo auto pode cair para S3 abaixo.
    for caminho in ("bronze_bruto.csv", "bronze_semana.csv"):
        if os.path.exists(caminho):
            return caminho
    raise FileNotFoundError(
        "Nenhum Bronze local foi encontrado. Use --origem s3 --hostname HOST ou --s3-key CHAVE."
    )


def _interpretar_referencia_s3(referencia, bucket_padrao):
    if not referencia:
        return bucket_padrao, None
    referencia = str(referencia).strip()
    if referencia.startswith("s3://"):
        resto = referencia[5:]
        partes = resto.split("/", 1)
        if len(partes) != 2 or not partes[0] or not partes[1]:
            raise ValueError(f"URI S3 inválida: {referencia}")
        return partes[0], partes[1]
    return bucket_padrao, referencia.lstrip("/")


def carregar_bronze_s3(*, bucket=None, s3_key=None, hostname=None, manifest_key=None):
    #Lê a chave do Bronze via boto3; não exige que o CSV exista no disco da EC2.
    bucket = bucket or BUCKET_NAME
    if not bucket:
        raise ValueError("Configure BUCKET_NAME/S3_BUCKET_NAME ou informe --bucket.")
    s3 = obter_cliente_s3()

    metadata = {}
    if s3_key:
        bucket_ref, chave_objeto = _interpretar_referencia_s3(s3_key, bucket)
        bucket = bucket_ref or bucket
    else:
        if manifest_key:
            bucket_manifesto, chave_manifesto = _interpretar_referencia_s3(manifest_key, bucket)
            bucket = bucket_manifesto or bucket
        elif hostname:
            chave_manifesto = _chave_manifesto_por_hostname(hostname)
        else:
            # Só seleciona automaticamente se houver exatamente um manifesto de host.
            resposta = s3.list_objects_v2(
                Bucket=bucket,
                Prefix=f"{S3_PREFIXO_BRONZE}/manifestos/",
            )
            chaves = sorted(
                item["Key"] for item in resposta.get("Contents", [])
                if item.get("Key", "").endswith("/bronze_ativo.json")
            )
            if not chaves:
                raise FileNotFoundError(
                    f"Nenhum manifesto encontrado em s3://{bucket}/{S3_PREFIXO_BRONZE}/manifestos/. "
                    "Informe --hostname ou --manifest-key."
                )
            if len(chaves) > 1:
                hosts = ", ".join(chaves[:10])
                raise ValueError(
                    "Há manifestos de mais de um host; informe --hostname para escolher a coleta. "
                    f"Manifestos encontrados: {hosts}"
                )
            chave_manifesto = chaves[0]

        obj_manifesto = s3.get_object(Bucket=bucket, Key=chave_manifesto)
        metadata = json.loads(obj_manifesto["Body"].read().decode("utf-8"))
        if not isinstance(metadata, dict):
            raise ValueError(f"O manifesto S3 {chave_manifesto} não contém um objeto JSON.")
        bucket_manifesto = metadata.get("bucket")
        if bucket_manifesto:
            bucket = bucket_manifesto
        referencia = (
            metadata.get("chave_s3") or metadata.get("s3_key") or metadata.get("s3_uri")
        )
        bucket_ref, chave_objeto = _interpretar_referencia_s3(referencia, bucket)
        bucket = bucket_ref or bucket
        if not chave_objeto:
            raise ValueError(
                f"O manifesto s3://{bucket}/{chave_manifesto} não informa chave_s3/s3_uri. "
                "O agente precisa ser atualizado para publicar a chave do Bronze."
            )

    resposta = s3.get_object(Bucket=bucket, Key=chave_objeto)
    conteudo = resposta["Body"].read()
    if not conteudo:
        raise ValueError(f"O objeto s3://{bucket}/{chave_objeto} está vazio.")
    df = pd.read_csv(io.BytesIO(conteudo), sep=";", encoding="utf-8")
    if df.empty:
        raise ValueError(f"O objeto s3://{bucket}/{chave_objeto} não contém amostras.")
    if metadata.get("estado_coleta") == "EM_ANDAMENTO":
        print("Aviso: o manifesto indica que a coleta ainda está EM_ANDAMENTO; o Bronze pode ser parcial.")
    return df, f"s3://{bucket}/{chave_objeto}", metadata, bucket, chave_objeto

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
# A identidade do equipamento é só o hostname (= computador.nome cadastrado pelo gestor).
# Empresa, aeronave e placas são resolvidas depois, por join com o cadastro.
CHAVES_BASE = ["hostname"]
CHAVES_AGRUPAMENTO = CHAVES_BASE + ["sessao"]
CHAVES_FMC = ["hostname"]
BYTES_POR_KB = 1024
METRICAS_ALERTAVEIS = ("cpu_pct", "ram_pct", "flash_pct")
# ============================================================
# 2. CADASTRO DO MYSQL: hostname -> computador -> aeronave -> empresa (+ placas e limites)
# ============================================================
# Premissa do projeto: computador.nome é o hostname da máquina e é único.
COLUNAS_CADASTRO = [
    "computador_id", "tipo_fmc", "status_computador",
    "aeronave_id", "aeronave_nome", "aeronave_modelo", "companhia_aerea",
    "empresa_id", "empresa_nome",
]
MAPA_TIPO_METRICA = {"CPU": "cpu_pct", "RAM": "ram_pct", "DISCO": "flash_pct", "FLASH": "flash_pct"}
SQL_CADASTRO = (
    "SELECT c.nome, c.idComputador, c.tipoFmc, c.statusComputador, "
    "a.idAeronave, a.nome, a.modelo, a.companhiaAerea, "
    "e.idEmpresaFabricante, COALESCE(NULLIF(e.nomeFantasia, ''), e.razaoSocial) "
    "FROM computador c "
    "JOIN aeronave a ON a.idAeronave = c.fkAeronave "
    "JOIN empresaFabricante e ON e.idEmpresaFabricante = a.fkEmpresaFabricante"
)
SQL_COMPONENTES = (
    "SELECT c.nome, co.id_componente, co.tipo, p.idPlaca, p.nome, "
    "co.limiteAtencao, co.limiteCritico "
    "FROM componente co "
    "JOIN placa p ON p.idPlaca = co.fkPlaca "
    "JOIN computador c ON c.idComputador = p.fkComputador "
    "WHERE co.statusMonitoramento = 'ATIVO'"
)
def _chave_hostname(serie: pd.Series) -> pd.Series:
    # Compara hostnames sem diferenciar maiúsculas/minúsculas (Windows costuma devolver em MAIÚSCULAS).
    return serie.astype(str).str.strip().str.lower()
def carregar_cadastro_do_banco():
    # Devolve (df_cadastro, df_componentes). Se o banco não responder, devolve (None, None):
    # o pipeline continua por hostname, só que sem empresa/aeronave e com os limites padrão.
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
        print(f"MySQL indisponível ({erro}); seguindo só por hostname, com limites padrão.")
        return None, None
    finally:
        if conexao is not None:
            conexao.close()
    cadastro = pd.DataFrame(linhas_cadastro, columns=["hostname", *COLUNAS_CADASTRO])
    for coluna in ("computador_id", "aeronave_id", "empresa_id"):
        cadastro[coluna] = cadastro[coluna].astype("Int64")
    componentes = pd.DataFrame(linhas_componentes, columns=[
        "hostname", "componente_id", "tipo", "placa_id", "placa_nome", "limiteAtencao", "limiteCritico",
    ])
    componentes["metrica"] = componentes["tipo"].astype(str).str.upper().map(MAPA_TIPO_METRICA)
    componentes = componentes.dropna(subset=["metrica"]).reset_index(drop=True)
    for coluna in ("limiteAtencao", "limiteCritico"):
        componentes[coluna] = pd.to_numeric(componentes[coluna], errors="coerce")
    print(f"Cadastro carregado: {len(cadastro)} computador(es), {len(componentes)} componente(s) ativo(s).")
    return cadastro, componentes
def _vincular_cadastro(df: pd.DataFrame, cadastro, componentes) -> pd.DataFrame:
    # Liga cada amostra ao cadastro pelo hostname e traz os limites de cada computador.
    df["_chave_host"] = _chave_hostname(df["hostname"])
    if cadastro is None:
        for coluna in COLUNAS_CADASTRO:
            df[coluna] = np.nan
        # Sem banco não dá para saber se o hostname existe: fica desconhecido, não "falso".
        df["q_hostname_nao_cadastrado"] = pd.Series(pd.NA, index=df.index, dtype="boolean")
    else:
        ref = cadastro.assign(_chave_host=_chave_hostname(cadastro["hostname"])).drop(columns="hostname")
        duplicados = ref["_chave_host"].duplicated(keep="first")
        if duplicados.any():
            print("Aviso: hostnames repetidos no cadastro (a premissa é nome único): "
                  + ", ".join(sorted(ref.loc[duplicados, "_chave_host"].unique())))
            ref = ref[~duplicados]
        df = df.merge(ref, on="_chave_host", how="left")
        df["q_hostname_nao_cadastrado"] = df["computador_id"].isna()
    # Limites por computador (cadastrados pelo gestor); CPU cai no padrão documentado se faltar.
    for metrica in METRICAS_ALERTAVEIS:
        df[f"limiar_{metrica}_atencao"] = np.nan
        df[f"limiar_{metrica}_critico"] = np.nan
    if componentes is not None and not componentes.empty:
        lim = componentes.assign(_chave_host=_chave_hostname(componentes["hostname"]))
        for metrica in METRICAS_ALERTAVEIS:
            do_host = lim[lim["metrica"] == metrica].groupby("_chave_host")[["limiteAtencao", "limiteCritico"]].mean()
            df[f"limiar_{metrica}_atencao"] = pd.to_numeric(df["_chave_host"].map(do_host["limiteAtencao"]), errors="coerce")
            df[f"limiar_{metrica}_critico"] = pd.to_numeric(df["_chave_host"].map(do_host["limiteCritico"]), errors="coerce")
    for metrica, (padrao_atencao, padrao_critico) in LIMITES_PADRAO.items():
        sem_limite = df[f"limiar_{metrica}_atencao"].isna() | df[f"limiar_{metrica}_critico"].isna()
        df.loc[sem_limite, f"limiar_{metrica}_atencao"] = padrao_atencao
        df.loc[sem_limite, f"limiar_{metrica}_critico"] = padrao_critico
    for metrica in METRICAS_ALERTAVEIS:
        colunas = [f"limiar_{metrica}_atencao", f"limiar_{metrica}_critico"]
        invalido = ~(df[colunas[0]] < df[colunas[1]])  # também pega NaN
        df.loc[invalido, colunas] = np.nan
    return df
def anexar_cadastro(df_gold: pd.DataFrame, df_silver: pd.DataFrame) -> pd.DataFrame:
    # Coloca empresa/aeronave/computador (vindos do Silver) nas tabelas Gold, pelo hostname.
    if df_gold.empty:
        return df_gold
    ref = df_silver[["hostname", *COLUNAS_CADASTRO]].drop_duplicates("hostname")
    saida = df_gold.merge(ref, on="hostname", how="left")
    primeiras = ["hostname", *COLUNAS_CADASTRO]
    return saida[primeiras + [c for c in saida.columns if c not in primeiras]]
def vincular_componente_alertas(df_alertas: pd.DataFrame, componentes) -> pd.DataFrame:
    # Liga cada alerta ao componente e à placa monitorados (cpu->CPU, ram->RAM, flash->DISCO).
    novas = ["componente_id", "placa_id", "placa_nome"]
    if componentes is None or componentes.empty or df_alertas.empty:
        for coluna in novas:
            df_alertas[coluna] = np.nan
        return df_alertas
    comp = componentes[["hostname", "metrica", *novas]].copy()
    comp["_chave_host"] = _chave_hostname(comp["hostname"])
    comp = comp.drop(columns="hostname").drop_duplicates(["_chave_host", "metrica"])
    saida = df_alertas.assign(_chave_host=_chave_hostname(df_alertas["hostname"]))
    return saida.merge(comp, on=["_chave_host", "metrica"], how="left").drop(columns="_chave_host")
def _garantir_coluna(df: pd.DataFrame, nome: str, valor_padrao=np.nan) -> None:
    if nome not in df.columns:
        df[nome] = valor_padrao
def _inferir_intervalo(grupo: pd.DataFrame) -> tuple[float, str]:
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
# ============================================================
# 3. BRONZE -> SILVER: VALIDAÇÃO, TIPAGEM, QUALIDADE E DERIVAÇÕES
# ============================================================
def processar_camada_silver(df_bronze: pd.DataFrame, cadastro=None, componentes=None) -> pd.DataFrame:
    # Transforma Bronze sem preencher métricas/identidades desconhecidas com valores falsos.
    if not isinstance(df_bronze, pd.DataFrame):
        raise TypeError("Bronze deve ser um pandas.DataFrame.")
    if df_bronze.empty:
        raise ValueError("O Bronze não contém amostras.")
    df = df_bronze.copy()
    # São os campos mínimos para ordenar as amostras, identificar o FMC e medir CPU.
    # cpu_percent fica como dado bruto; os cálculos usam cpu_pct_host já normalizado.
    obrigatorias = ["timestamp_utc", "hostname", "cpu_pct_host"]
    faltantes = [coluna for coluna in obrigatorias if coluna not in df.columns]
    if faltantes:
        raise ValueError("Colunas obrigatórias ausentes no Bronze: " + ", ".join(faltantes))
    # Converte o timestamp para data/hora UTC; texto inválido vira NaT.
    # Depois remove só as amostras sem horário válido para não quebrar a ordenação temporal.
    df["ts"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"]).copy()
    if df.empty:
        raise ValueError("Nenhuma amostra tem timestamp_utc válido; Bronze foi preservado sem alteração.")
    for coluna in ("hostname",):
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
    _garantir_coluna(df, "cpu_percent", np.nan)  # Mantém a leitura bruta para auditoria.
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
    _garantir_coluna(df, "uptime_proc_s", np.nan)
    _garantir_coluna(df, "fator_tempo", np.nan)
    _garantir_coluna(df, "seq", np.nan)
    _garantir_coluna(df, "qtd_tarefas", np.nan)
    # Converte as colunas numéricas de uma vez; texto que não virar número fica como NaN.
    # Isso evita que uma única string inválida interrompa o processamento inteiro.
    colunas_numericas = [
        "seq", "uptime_proc_s", "fator_tempo", "cpu_percent", "cpu_nucleos", "cpu_pct_host",
        "ram_usada_bytes", "ram_baseline_bytes", "ram_total_bytes", "ram_disponivel_bytes", "flash_usada_bytes",
        "flash_total_bytes", "qtd_tarefas", "intervalo_coleta_s",
    ]
    for coluna in colunas_numericas:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
    # Número de núcleos precisa ser positivo; zero ou negativo não é um valor válido.
    df["cpu_nucleos"] = df["cpu_nucleos"].where(df["cpu_nucleos"] > 0)
    # Trata os contadores de I/O que o agente novo já entrega.
    # A unidade é necessária para saber se o contador representa bytes ou caracteres.
    # Se faltar a coluna ou a unidade, o valor fica desconhecido em vez de usar o nome antigo.
    for tipo in ("leitura", "escrita"):
        coluna_contador = f"io_{tipo}_contador_acum"
        coluna_unidade = f"io_{tipo}_unidade"
        _garantir_coluna(df, coluna_contador, np.nan)
        _garantir_coluna(df, coluna_unidade, "desconhecida")
        # Texto ou valores inválidos viram NaN; não são convertidos em zero.
        df[coluna_contador] = pd.to_numeric(df[coluna_contador], errors="coerce")
        df[coluna_unidade] = df[coluna_unidade].fillna("desconhecida").astype(str).str.lower()
        df.loc[~df[coluna_unidade].isin(["bytes", "chars"]), coluna_unidade] = "desconhecida"
    # A CPU normalizada vem diretamente do agente.
    # Não divide de novo por núcleos, porque isso reduziria a CPU duas vezes.
    # Também não tenta adivinhar a semântica pelo nome da origem.
    candidato_cpu = pd.to_numeric(df["cpu_pct_host"], errors="coerce")
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
    # Total igual a zero não pode servir como divisor para calcular percentuais.
    ram_total_valido = df["ram_total_bytes"].where(df["ram_total_bytes"] > 0)
    # A disponibilidade de RAM precisa vir do agente. Não a calculamos por diferença,
    # pois RAM usada pode representar RSS do processo e RAM total pode ser a do host.
    df["q_ram_disponivel_invalida"] = (
        df["ram_disponivel_bytes"].notna()
        & ((df["ram_disponivel_bytes"] < 0)
           | (ram_total_valido.notna() & (df["ram_disponivel_bytes"] > ram_total_valido)))
    )
    df["ram_disponivel_bytes"] = df["ram_disponivel_bytes"].where(~df["q_ram_disponivel_invalida"])
    # Divide a RAM disponível pela capacidade declarada para obter a percentagem livre.
    df["ram_disponivel_pct"] = df["ram_disponivel_bytes"] / ram_total_valido * 100
    # A Flash também precisa ter capacidade positiva antes de calcular o percentual ocupado.
    flash_total_valido = df["flash_total_bytes"].where(df["flash_total_bytes"] > 0)
    df["ram_pct"] = df["ram_logica_bytes"] / ram_total_valido * 100
    df["flash_pct"] = df["flash_usada_bytes"].where(df["flash_usada_bytes"] >= 0) / flash_total_valido * 100
    df["q_ram_acima_100"] = df["ram_pct"] > 100
    df["q_flash_acima_100"] = df["flash_pct"] > 100
    # O agente novo grava a fase observada nos metadados da bancada.
    # Se ela não vier, não tentamos deduzi-la pelo uptime: marcamos como desconhecida.
    df["fase"] = df["fase"].fillna("NAO_DETERMINADA").astype(str).str.strip()
    df.loc[df["fase"].eq(""), "fase"] = "NAO_DETERMINADA"
    # Liga hostname -> computador/aeronave/empresa e traz os limites de cada computador.
    df = _vincular_cadastro(df, cadastro, componentes)
    # Compara cada métrica com os limites do próprio computador (cadastrados pelo gestor).
    # Sem limite configurado, a métrica não recebe estado de alerta inventado.
    for metrica in METRICAS_ALERTAVEIS:
        lim_atencao = df[f"limiar_{metrica}_atencao"]
        lim_critico = df[f"limiar_{metrica}_critico"]
        configurado = lim_atencao.notna() & lim_critico.notna()
        valores = pd.to_numeric(df[metrica], errors="coerce")
        em_atencao = configurado & valores.gt(lim_atencao) & valores.le(lim_critico)
        em_critico = configurado & valores.gt(lim_critico)
        nivel = pd.Series("NORMAL", index=df.index, dtype="object")
        nivel.loc[~configurado] = "NAO_CONFIGURADO"
        nivel.loc[configurado & valores.isna()] = "NAO_DETERMINADO"
        nivel.loc[em_atencao] = "ATENCAO"
        nivel.loc[em_critico] = "CRITICO"
        df[f"nivel_{metrica}"] = nivel
        flag_atencao = em_atencao.astype("boolean")
        flag_critico = em_critico.astype("boolean")
        nao_avaliavel = ~configurado | valores.isna()
        flag_atencao.loc[nao_avaliavel] = pd.NA
        flag_critico.loc[nao_avaliavel] = pd.NA
        df[f"q_{metrica}_atencao"] = flag_atencao
        df[f"q_{metrica}_critico"] = flag_critico
    # Ordena por identidade e timestamp para que diferenças e alertas sigam a ordem temporal.
    df = df.sort_values(CHAVES_BASE + ["ts"], kind="mergesort", na_position="last").reset_index(drop=True)
    duplicidade_chaves = CHAVES_BASE + ["ts"]
    if "sessao_origem" in df.columns:
        duplicidade_chaves.append("sessao_origem")
    # Usa as chaves de identidade, timestamp e sessão para reconhecer amostras repetidas.
    # keep="last" preserva a última versão da amostra duplicada.
    quantidade_duplicadas = int(df.duplicated(subset=duplicidade_chaves, keep="last").sum())
    if quantidade_duplicadas:
        df = df.drop_duplicates(subset=duplicidade_chaves, keep="last").reset_index(drop=True)
        print(f"Silver: descartadas {quantidade_duplicadas} amostras duplicadas; Bronze mantido intacto.")
    # Detecta reinício quando o contador de tempo/seq volta para trás ou a sessão muda.
    # A mudança abre um novo grupo de sessão, sem alterar os IDs do FMC.
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
        # diff obtém quanto o contador cresceu desde a amostra anterior.
        # Se mudou a unidade, houve reset ou o intervalo é inválido, a taxa fica desconhecida.
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
    df = df.drop(columns=["_reinicio", "_chave_host"])
    df.attrs["duplicatas_descartadas"] = quantidade_duplicadas
    return df
# ============================================================
# 4. FUNÇÕES AUXILIARES GOLD
# ============================================================
def regressao_linear(x: pd.Series, y: pd.Series, minimo_pontos: int = 3) -> float:
    # Calcula a tendência linear de y ao longo do tempo; exige pontos distintos.
    # Remove valores inválidos antes do cálculo, para não contaminar a inclinação.
    dados = pd.DataFrame({"x": x, "y": y}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(dados) < minimo_pontos or dados["x"].nunique() < 2:
        return np.nan
    return float(np.polyfit(dados["x"].astype(float), dados["y"].astype(float), 1)[0])
def percentual_acima(serie: pd.Series, limite: float) -> float:
    # Percentual acima do limite entre valores válidos; sem dados, retorna NaN.
    valores_validos = pd.to_numeric(serie, errors="coerce").dropna()
    if valores_validos.empty:
        return np.nan
    return float((valores_validos > limite).mean() * 100)
def _limiar_valido(limiares: dict, metrica: str):
    # Só aceita um par numérico completo em que atenção seja menor que crítico.
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
def _limites_do_grupo(grupo: pd.DataFrame, metrica: str):
    # Lê os limites do próprio computador, gravados na Silver; sem par único e válido, não há limite.
    vazio = pd.Series(dtype=float)
    atencao = pd.to_numeric(grupo.get(f"limiar_{metrica}_atencao", vazio), errors="coerce").dropna().unique()
    critico = pd.to_numeric(grupo.get(f"limiar_{metrica}_critico", vazio), errors="coerce").dropna().unique()
    if len(atencao) != 1 or len(critico) != 1:
        return None
    return _limiar_valido({metrica: (atencao[0], critico[0])}, metrica)
def _calcular_health_score(grupo: pd.DataFrame, completude: float) -> tuple[float, str, float, float]:
    # Score por severidade máxima por amostra; crítico não acumula também atenção.
    # Primeiro identifica quais métricas têm limites válidos para esta execução.
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
        amostra_valida |= valido
        critico_algum |= valido & (serie > limite_critico)
        atencao_alguma |= valido & (serie > limite_atencao) & (serie <= limite_critico)
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
def calcular_kpis_sessao(grupo: pd.DataFrame, identificadores: dict) -> dict:
    # Calcula indicadores da sessão sem mascarar ausência de dados ou intervalos desconhecidos.
    grupo = grupo.sort_values("ts").reset_index(drop=True)
    total_amostras = len(grupo)
    duracao_s = float(grupo["t_sessao_s"].max()) if total_amostras else 0.0
    intervalos = pd.to_numeric(grupo["intervalo_esperado_s"], errors="coerce").dropna()
    intervalo = float(intervalos.iloc[0]) if len(intervalos) and intervalos.nunique() == 1 else np.nan
    # Sem uma cadência confiável não estimamos amostras esperadas nem completude.
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
    flash_pct = pd.to_numeric(grupo["flash_pct"], errors="coerce")
    cpu_media, cpu_std, cpu_p95, cpu_pico = cpu.mean(), cpu.std(), cpu.quantile(0.95), cpu.max()
    ram_media, ram_pico = ram.mean(), ram.max()
    ram_disponivel = pd.to_numeric(grupo["ram_disponivel_bytes"], errors="coerce")
    ram_disponivel_pct = pd.to_numeric(grupo["ram_disponivel_pct"], errors="coerce")
    ram_headroom_bytes = float(ram_disponivel.min()) if ram_disponivel.notna().any() else np.nan
    ram_headroom_pct = float(ram_disponivel_pct.min()) if ram_disponivel_pct.notna().any() else np.nan
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
    # Alias de nome antigo mantido para não quebrar dashboards que já leem esse KPI.
    # O valor continua sendo projeção de capacidade do arquivo/log, não RUL física da Flash.
    kpis["flash_autonomia_h"] = kpis["flash_capacidade_restante_h"]
    kpis["flash_autonomia_tipo"] = "PROJECAO_CAPACIDADE_LOG" if pd.notna(flash_autonomia_h) else "INDETERMINADA"
    health_score, health_status, pct_atencao, pct_critico = _calcular_health_score(grupo, completude)
    kpis["health_score"] = health_score
    kpis["health_score_status"] = health_status
    kpis["health_pct_amostras_atencao"] = pct_atencao
    kpis["health_pct_amostras_criticas"] = pct_critico
    return kpis
# ============================================================
# 5. ALERTAS COM PERSISTÊNCIA, HISTERESE E HISTÓRICO AUDITÁVEL
# ============================================================
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
def gerar_alertas(df_silver: pd.DataFrame) -> pd.DataFrame:
    # Gera eventos de abertura, escala, rebaixamento e encerramento de alertas.
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
                        and float(dt) <= float(intervalo) * 1.5 and not lacuna
                    )
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
                        alerta_id = f"{identificadores.get('hostname')}:{metrica}:{ts_abertura.isoformat()}"
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
# ============================================================
# 6. KPIs GOLD POR FASE, SESSÃO E MINUTO
# ============================================================
def calcular_kpis_por_fase(df_silver: pd.DataFrame) -> pd.DataFrame:
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
def processar_camada_gold(df_silver: pd.DataFrame, componentes=None):
    # Cria KPIs por sessão, série por minuto, KPIs por fase e eventos de alerta.
    kpis_sessao = []
    for chave, grupo in df_silver.groupby(CHAVES_AGRUPAMENTO, dropna=False, sort=False):
        chave = chave if isinstance(chave, tuple) else (chave,)
        identificadores = dict(zip(CHAVES_AGRUPAMENTO, chave))
        kpis_sessao.append(calcular_kpis_sessao(grupo, identificadores))
    df_kpis_sessao = pd.DataFrame(kpis_sessao)
    # Agrega cada minuto e mantém a fase para não misturar, por exemplo, subida e cruzeiro.
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
            lacunas=("q_lacuna", lambda valores: valores.sum(min_count=1)),
        )
        .reset_index()
    )
    for coluna in ("cpu_media", "ram_media", "flash_media"):
        df_serie_1min[coluna] = df_serie_1min[coluna].round(3)
    df_kpis_fase = calcular_kpis_por_fase(df_silver)
    df_alertas = vincular_componente_alertas(gerar_alertas(df_silver), componentes)
    return df_kpis_sessao, df_serie_1min, df_kpis_fase, df_alertas
def _chave_normalizada(linha: pd.Series, colunas: list[str]) -> tuple:
    return tuple(None if pd.isna(linha.get(coluna)) else str(linha.get(coluna)) for coluna in colunas)
def criar_resumo_fmc(df_silver: pd.DataFrame, df_kpis_sessao: pd.DataFrame, df_alertas: pd.DataFrame) -> pd.DataFrame:
    # Consolida último estado, health score e quantidade/severidade dos alertas por FMC.
    # Repassa os eventos na ordem do tempo para saber quais alertas continuam abertos.
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
# 7. UPLOAD OPCIONAL PARA S3

def enviar_arquivos_s3(caminhos_locais: list, prefixo: str, hostname: str, sessao_id: str) -> None:
    #Envia Silver/Gold com chaves por host e sessão.
    # usa IAM Role se tiver disponível

    habilitado = os.getenv("ENVIAR_S3", "true").strip().lower() in ("1", "true", "yes", "sim")
    bucket = BUCKET_NAME
    if not habilitado:
        print(f"ENVIAR_S3=false; arquivos {prefixo} permaneceram locais.")
        return
    if not bucket:
        print(f"BUCKET_NAME/S3_BUCKET_NAME não configurado; arquivos {prefixo} permaneceram locais.")
        return
    try:
        s3 = obter_cliente_s3()
    except Exception as erro:
        print(f"Não foi possível inicializar S3; arquivos {prefixo} permaneceram locais: {erro}")
        return

    prefixo_chave = (
        f"{prefixo.strip('/')}/hostname={_segmento_s3(hostname)}"
        f"/sessao={_segmento_s3(sessao_id)}"
    )

    for caminho in caminhos_locais:
        if not os.path.exists(caminho):
            print(f"Arquivo local não encontrado para upload: {caminho}")
            continue

        #Avisa q o dict é json se o nome termina em .json
        extra = None
        if str(caminho).endswith(".json"):
            extra = {"ContentType": "application/json"}

        chave_s3 = f"{prefixo_chave}/{os.path.basename(caminho)}"
        try:
            s3.upload_file(caminho, bucket, chave_s3, ExtraArgs=extra) #ExtraArgs é pra passar metadados (tipo falar q o dict é json)
            
            print(f"S3 OK: s3://{bucket}/{chave_s3}")
        except Exception as erro:
            print(f"Erro ao enviar {caminho} para o S3: {erro}")


# EXECUÇÃO DO PIPELINE
if __name__ == "__main__":
    terminal = argparse.ArgumentParser(
        description="Pipeline ETL AirPulse: Bronze no local ou S3 -> Silver -> Gold"
    )
    terminal.add_argument(
        "--origem", choices=("auto", "local", "s3"), default=ETL_ORIGEM_PADRAO,
        help="Origem do Bronze. Use s3 na EC2; padrão configurável por ETL_ORIGEM.",
    )
    terminal.add_argument(
        "--bronze", default=None,
        help="Caminho de CSV local (força leitura local quando --origem=auto).",
    )
    terminal.add_argument(
        "--hostname", default=BRONZE_HOSTNAME_PADRAO,
        help="Host cujo manifesto bronze_ativo.json será lido no S3.",
    )
    terminal.add_argument(
        "--s3-key", default=BRONZE_S3_KEY_PADRAO,
        help="Chave do objeto Bronze no S3 ou URI completa s3://bucket/chave.",
    )
    terminal.add_argument(
        "--manifest-key", default=BRONZE_MANIFEST_KEY_PADRAO,
        help="Chave/URI S3 do manifesto JSON; tem precedência sobre --hostname.",
    )
    terminal.add_argument(
        "--bucket", default=BUCKET_NAME,
        help="Bucket S3 (ou configure BUCKET_NAME/S3_BUCKET_NAME no .env).",
    )
    comandos = terminal.parse_args()
    origem = comandos.origem
    manifesto = {}
    bucket_resultado = comandos.bucket
    chave_bronze = None

    # Uma origem explicitamente S3 ou um seletor remoto deve ignorar qualquer CSV local antigo.
    selecao_s3_explicita = bool(comandos.s3_key or comandos.manifest_key or comandos.hostname)
    if origem == "s3" or (origem == "auto" and selecao_s3_explicita and not comandos.bronze):
        try:
            df_bronze, local_origem, manifesto, bucket_resultado, chave_bronze = carregar_bronze_s3(
                bucket=comandos.bucket,
                s3_key=comandos.s3_key,
                hostname=None if comandos.manifest_key else comandos.hostname,
                manifest_key=comandos.manifest_key,
            )
        except Exception as erro:
            raise SystemExit(f"Não foi possível carregar Bronze do S3: {erro}") from erro
        sessao_resultado = str(manifesto.get("sessao_id") or _segmento_s3(os.path.basename(chave_bronze)))
        print(f"Bronze carregado do S3: {local_origem} ({len(df_bronze)} linhas)")
    else:
        caminho_local = comandos.bronze
        if not caminho_local:
            try:
                caminho_local = selecionar_arquivo_bronze()
            except (RuntimeError, FileNotFoundError):
                if origem == "local":
                    raise SystemExit("Nenhum Bronze local encontrado; confira --bronze/ARQUIVO_BRONZE.")
                if not comandos.bucket:
                    raise SystemExit(
                        "Nenhum Bronze local encontrado e BUCKET_NAME não está configurado. "
                        "Use --origem s3 --hostname HOST."
                    )
                try:
                    df_bronze, local_origem, manifesto, bucket_resultado, chave_bronze = carregar_bronze_s3(
                        bucket=comandos.bucket,
                        s3_key=comandos.s3_key,
                        hostname=comandos.hostname,
                        manifest_key=comandos.manifest_key,
                    )
                except Exception as erro:
                    raise SystemExit(f"Não foi possível localizar Bronze local nem carregar do S3: {erro}") from erro
                sessao_resultado = str(manifesto.get("sessao_id") or _segmento_s3(os.path.basename(chave_bronze)))
                print(f"Bronze carregado do S3: {local_origem} ({len(df_bronze)} linhas)")
                caminho_local = None
        if caminho_local:
            if not os.path.exists(caminho_local):
                raise SystemExit(f"Arquivo Bronze local '{caminho_local}' não existe.")
            print(f"Lendo Bronze local: {caminho_local}")
            df_bronze = pd.read_csv(caminho_local, sep=";", encoding="utf-8")
            if df_bronze.empty:
                raise SystemExit("O arquivo Bronze existe, mas não contém amostras.")
            local_origem = str(caminho_local)
            # Usa uma sessão explícita do CSV, se possível; senão, associa os resultados à execução.
            sessoes = (
                df_bronze["sessao_origem"].dropna().astype(str).unique().tolist()
                if "sessao_origem" in df_bronze.columns else []
            )
            sessao_resultado = sessoes[0] if len(sessoes) == 1 else datetime.now().strftime("%Y%m%dT%H%M%S")
            # Se o estado local contiver os novos campos, reaproveita a referência de sessão.
            try:
                with open(ARQUIVO_ESTADO_BRONZE, encoding="utf-8") as arquivo_estado:
                    estado_local = json.load(arquivo_estado)
                if (
                    isinstance(estado_local, dict)
                    and estado_local.get("arquivo_bronze")
                    and os.path.abspath(estado_local["arquivo_bronze"]) == os.path.abspath(caminho_local)
                ):
                    manifesto = estado_local
                    sessao_resultado = str(estado_local.get("sessao_id") or sessao_resultado)
            except (OSError, json.JSONDecodeError):
                pass

    if df_bronze.empty:
        raise SystemExit("O Bronze selecionado não contém amostras.")

    cadastro, componentes = carregar_cadastro_do_banco()
    print("Processando Silver...")
    try:
        df_silver = processar_camada_silver(df_bronze, cadastro, componentes)
    except (ValueError, TypeError) as erro:
        raise SystemExit(f"Erro de esquema/qualidade no Bronze: {erro}") from erro

    # Hostname que não está no cadastro vai para quarentena; nunca é descartado em silêncio.
    PASTA_SILVER.mkdir(parents=True, exist_ok=True)
    nao_cadastrado = df_silver["q_hostname_nao_cadastrado"].fillna(False).astype(bool)
    if nao_cadastrado.any():
        caminho_quarentena = PASTA_SILVER / "silver_quarentena_hostname.csv"
        df_silver[nao_cadastrado].to_csv(caminho_quarentena, sep=";", index=False, encoding="utf-8")
        hosts = ", ".join(sorted(df_silver.loc[nao_cadastrado, "hostname"].astype(str).unique()))
        print(
            f"ATENÇÃO: {int(nao_cadastrado.sum())} amostra(s) de hostname não cadastrado "
            f"({hosts}) enviadas para {caminho_quarentena}."
        )
        df_silver = df_silver[~nao_cadastrado].reset_index(drop=True)
    if df_silver.empty:
        raise SystemExit(
            "Nenhuma amostra de hostname cadastrado para processar. "
            "Confirme se hostname no Bronze corresponde a computador.nome no MySQL."
        )

    caminho_silver = PASTA_SILVER / "silver_telemetria.csv"
    df_silver.to_csv(caminho_silver, sep=";", index=False, encoding="utf-8")
    print(f"Silver salvo: {caminho_silver} ({len(df_silver)} linhas)")

    print("Processando Gold...")
    df_sessao, df_serie, df_fase, df_alertas = processar_camada_gold(df_silver, componentes)
    df_resumo_fmc = criar_resumo_fmc(df_silver, df_sessao, df_alertas)
    PASTA_GOLD.mkdir(parents=True, exist_ok=True)
    #Arquivos enviados ao bucket gold devem ser JSON
    arquivos_gold = {
        PASTA_GOLD / "gold_kpi_sessao.json": anexar_cadastro(df_sessao, df_silver),
        PASTA_GOLD / "gold_serie_1min.json": anexar_cadastro(df_serie, df_silver),
        PASTA_GOLD / "gold_kpi_fase.json": anexar_cadastro(df_fase, df_silver),
        PASTA_GOLD / "gold_alertas.json": anexar_cadastro(df_alertas, df_silver),
        PASTA_GOLD / "gold_resumo_fmc.json": anexar_cadastro(df_resumo_fmc, df_silver),
    }
    for caminho, dados in arquivos_gold.items():
        dados.to_json(caminho, orient="records", date_format="iso",
                    force_ascii=False, indent=2)
        print(f"Gold salvo: {caminho} ({len(dados)} linhas)")

    df_sessao = anexar_cadastro(df_sessao, df_silver)
    colunas_resumo = [
        "hostname", "empresa_nome", "aeronave_nome", "sessao", "duracao_min", "amostras",
        "completude_pct", "cpu_media", "cpu_p95", "ram_media", "ram_tendencia_kb_h",
        "flash_autonomia_h", "health_score", "health_score_status",
    ]
    colunas_presentes = [coluna for coluna in colunas_resumo if coluna in df_sessao.columns]
    print("\nResumo Gold por sessão:")
    print(df_sessao[colunas_presentes].to_string(index=False))
    if not df_alertas.empty:
        print(f"\nEventos de alerta registrados: {len(df_alertas)}")
        print(df_alertas[["hostname", "metrica", "tipo_evento", "nivel", "valor", "fase"]].to_string(index=False))
    else:
        print("\nNenhum evento de alerta gerado nesta execução.")

    hosts_saida = df_silver["hostname"].dropna().astype(str).unique().tolist()
    hostname_saida = hosts_saida[0] if len(hosts_saida) == 1 else "MULTI_HOST"
    enviar_arquivos_s3([str(caminho_silver)], "02-silver", hostname_saida, sessao_resultado)
    enviar_arquivos_s3([str(caminho) for caminho in arquivos_gold], "03-gold", hostname_saida, sessao_resultado)
