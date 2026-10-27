# ETL AirPulse: Bronze -> Silver -> Gold.
import argparse
from datetime import datetime
import io
import json
import os
import re
from pathlib import Path
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

#===========================================================================================================================================
# BLOCO 1: CONFIGURAÇÕES, PARÂMETROS E CONTRATOS DE DADOS

# Variáveis de ambiente, caminhos e configuração do S3.

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


# Parâmetros de negócio, chaves de agrupamento, métricas e consultas SQL.

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

CHAVES_BASE = ["hostname", "placa_serie"]  # Identificam qual pc e qual placa (placa = placa.numeroSerie)

COLUNAS_BRONZE = [  # Contrato do agente.py usado aqui; faltou alguma = erro de esquema
    "timestamp_utc", "seq", "hostname", "placa_nome", "uptime_proc_s", "intervalo_coleta_s",
    "sessao_origem", "fase", "cpu_pct_host", "ram_usada_bytes", "ram_baseline_bytes",
    "ram_baseline_fonte", "ram_total_bytes", "ram_disponivel_bytes", "ram_medicao_tipo",
    "flash_usada_bytes", "flash_total_bytes",
    "io_leitura_contador_acum", "io_escrita_contador_acum", "io_leitura_unidade", "io_escrita_unidade",
]

CHAVES_AGRUPAMENTO = CHAVES_BASE + ["sessao"]  # Agrupa por pc, placa e sessão.

CHAVES_FMC = ["hostname"]  # Agrupam os dados do FMC por pc

BYTES_POR_KB = 1024  # Conversão de bytes para kilobytes

METRICAS_ALERTAVEIS = ("cpu_pct", "ram_pct", "flash_pct")  # Métricas monitoradas para geração de alertas.

COLUNAS_CADASTRO = [
    "computador_id", "tipo_fmc", "status_computador",
    "aeronave_id", "aeronave_nome", "aeronave_modelo", "companhia_aerea",
    "empresa_id", "empresa_nome",
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

#==========================================================================================================================================
# BLOCO 2: LOCALIZAÇÃO E LEITURA DO BRONZE LOCAL/S3

# Resolve caminhos, lê por hostname e carrega o CSV Bronze da origem escolhida.

def _segmento_s3(valor):
    # se existe um valor.
    if valor:
        texto = str(valor)
    else:
        texto = "desconhecido"

    texto = texto.strip()

    # Define caracteres permitidos
    caracteres_permitidos = (
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789"
        "._-"
    )

    segmento = ""
    caractere_anterior_invalido = False

    for caractere in texto:

        if caractere in caracteres_permitidos:
            # Mantém caractere permitido
            segmento = segmento + caractere

            # Reinicia controle dos q tão inválidos
            caractere_anterior_invalido = False

        else:
            # Só adiciona "_" no primeiro inválido da sequência
            if caractere_anterior_invalido == False:
                segmento = segmento + "_"

            caractere_anterior_invalido = True

    # remove ._- dos cantos
    segmento = segmento.strip("._-")

    # o resultado não fica vazio
    if segmento:
        return segmento
    else:
        return "desconhecido"

def _chave_manifesto_por_hostname(hostname):
    return (
        f"{S3_PREFIXO_BRONZE}/manifestos/hostname={_segmento_s3(hostname)}/bronze_ativo.json"
    )

def obter_cliente_s3():
    # Usa credenciais quando fornecidas, senão, usa o padrão do boto3/IAM Role
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
    # Localiza o Bronze local para modo local ou auto, sem precisar de CSV na EC2
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
        # Se o manifesto só tem a referência S3, o modo auto vai pro S3 abaixo
    for caminho in ("bronze_bruto.csv", "bronze_semana.csv"):
        if os.path.exists(caminho):
            return caminho
        
    raise FileNotFoundError("Nenhum Bronze local foi encontrado. Use --origem s3 --hostname HOST ou 5--s3-key CHAVE.")

def _interpretar_referencia_s3(referencia, bucket_padrao):
    if not referencia:
        return bucket_padrao, None
    referencia = str(referencia).strip()

    if referencia.startswith("s3://"):
        resto = referencia[5:] # Remove os 5 primeiros caracteres e guarda o resto.
        partes = resto.split("/", 1)

        if len(partes) != 2 or not partes[0] or not partes[1]:
            raise ValueError(f"URI S3 inválida: {referencia}")
        
        return partes[0], partes[1]
    
    return bucket_padrao, referencia.lstrip("/") # O l no strip remove só os da esquerda 

def carregar_bronze_s3(*, bucket=None, s3_key=None, hostname=None, manifest_key=None):
    #Lê a chave do Bronze via boto3
    #não exige que o CSV exista no disco da EC2.
    bucket = bucket or BUCKET_NAME
    if not bucket:
        raise ValueError("Configure BUCKET_NAME ou informe --bucket.")
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


#===========================================================================================================================================
# BLOCO 3: CADASTRO MYSQL, PLACAS E VÍNCULOS

# Carrega o cadastro, normaliza chaves e associa hostname, placa, componentes e limites.

def _chave_hostname(serie: pd.Series): 
    # Compara hostnames sem diferenciar maiúsculas/minúsculas (Windows costuma devolver em MAIÚSCULAS).
    return serie.astype(str).str.strip().str.lower()

def _segmento_s3(valor):
    # Troca sequências de caracteres fora de [A-Za-z0-9._-] por "_" e nunca devolve vazio.
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(valor or "desconhecido").strip()).strip("._-") or "desconhecido"

def _chave_texto(serie: pd.Series):
    # Compara texto sem diferenciar maiúsculas/minúsculas (Windows costuma devolver hostname em MAIÚSCULAS).
    return serie.astype(str).str.strip().str.lower()   # (renomeia _chave_hostname)

def _limites_do_grupo(grupo: pd.DataFrame, metrica: str):
    # Lê os limites do próprio computador gravados na Silver; sem par único e válido, não há limite.
    atencao = grupo[f"limiar_{metrica}_atencao"].dropna().unique()
    critico = grupo[f"limiar_{metrica}_critico"].dropna().unique()
    if len(atencao) == 1 and len(critico) == 1:
        return float(atencao[0]), float(critico[0])  # Silver já garante atenção < crítico
    return None

def carregar_cadastro_do_banco():
    # Devolve (df_cadastro, df_componentes). Se o banco não responder, devolve (None, None).
    # Nesse caso, segue sem IDs de negócio e sem limites cadastrados; alertas ficam não configurados.
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
        print(f"MySQL indisponível ({erro}); seguindo por hostname, sem IDs de negócio e sem limites de alerta.")
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
        df[f"limiar_{metrica}_atencao"] = atencao.where(valido)
        df[f"limiar_{metrica}_critico"] = critico.where(valido)
    return df.drop(columns=chaves_placa)


def anexar_cadastro(df_gold: pd.DataFrame, df_silver: pd.DataFrame):
    # Coloca empresa/aeronave/computador (vindos do Silver) nas tabelas Gold, pelo hostname.
    if df_gold.empty:
        return df_gold
    
    ref = df_silver[["hostname", *COLUNAS_CADASTRO]].drop_duplicates("hostname")

    saida = df_gold.merge(ref, on="hostname", how="left")

    primeiras = ["hostname", *COLUNAS_CADASTRO]

    return saida[primeiras + [c for c in saida.columns if c not in primeiras]]

def vincular_componente_alertas(df_alertas, componentes):
    # Liga cada alerta ao componente monitorado (cpu->CPU, ram->RAM, flash->DISCO) por (hostname, placa_serie, métrica).
    chaves = ["_chave_host", "placa_serie", "metrica"]
    comp = (componentes.assign(_chave_host=_chave_texto(componentes["hostname"]))
            [chaves + ["componente_id"]].drop_duplicates(chaves))
    saida = df_alertas.assign(_chave_host=_chave_texto(df_alertas["hostname"]))
    return saida.merge(comp, on=chaves, how="left").drop(columns="_chave_host")
    

#===========================================================================================================================================
# BLOCO 4: FUNÇÕES AUXILIARES DE QUALIDADE E TEMPO

# Garante colunas e infere o intervalo de coleta com base nos metadados ou timestamps.

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


#=============================================================================================================================================
# BLOCO 5: BRONZE -> SILVER

# Valida o esquema, converte tipos, calcula métricas derivadas e flags, cria sessões e taxas de I/O.

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


#============================================================================================================================================
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
    if len(atencao) != 1 or len(critico) != 1:
        return None
    return _limiar_valido({metrica: (atencao[0], critico[0])}, metrica)

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

def processar_camada_gold(df_silver, componentes):
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
            vincular_componente_alertas(gerar_alertas(df_silver), componentes), calcular_serie_diaria(df_silver))

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

#============================================================================================================================================
# BLOCO 8: PUBLICAÇÃO DOS ARQUIVOS NO S3

# Faz upload opcional das saídas Silver/Gold, organizadas por prefixo, hostname e sessão.

def enviar_arquivos_s3(caminhos_locais: list, prefixo: str, hostname: str, sessao_id: str):
    #Envia Silver/Gold com chaves por host e sessão.
    # usa IAM Role se tiver disponível

    habilitado = os.getenv("ENVIAR_S3", "true").strip().lower() in ("1", "true", "yes", "sim")
    bucket = BUCKET_NAME
    if not habilitado:
        print(f"ENVIAR_S3=false; arquivos {prefixo} permaneceram locais.")
        return
    if not bucket:
        print(f"BUCKET_NAME não configurado; arquivos {prefixo} permaneceram locais.")
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

def quarentenar(df_silver, mascara, sufixo, timestamp):
    # Hostname/placa fora do cadastro vai para quarentena por host; nunca é descartado em silêncio.
    for host in sorted(df_silver.loc[mascara, "hostname"].astype(str).unique()):
        nome = _segmento_s3(host)
        pasta = PASTA_SILVER / f"hostname={nome}"
        pasta.mkdir(parents=True, exist_ok=True)
        caminho = pasta / f"{nome}_{timestamp}_quarentena_{sufixo}.csv"
        df_silver[mascara & df_silver["hostname"].astype(str).eq(host)].to_csv(caminho, sep=";", index=False, encoding="utf-8")
        print(f"ATENÇÃO: amostras de {sufixo} não cadastrado(a) em quarentena: {caminho}")
    return df_silver[~mascara].reset_index(drop=True)


#============================================================================================================================================
# BLOCO 9: EXECUÇÃO DO PIPELINE
# Interpreta argumentos, executa Bronze -> Silver -> Gold, salva os arquivos e publica no S3.
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
        help="Bucket S3 (ou configure BUCKET_NAME no .env).",
    )
    comandos = terminal.parse_args()
    comandos = terminal.parse_args()
    origem = comandos.origem

    # Seleção S3 explícita (ou origem s3) ignora qualquer CSV local antigo.
    usa_s3 = origem == "s3" or (origem == "auto" and not comandos.bronze
                                and bool(comandos.s3_key or comandos.manifest_key or comandos.hostname))
    if not usa_s3:
        try:
            caminho_local = comandos.bronze or selecionar_arquivo_bronze()
        except (RuntimeError, FileNotFoundError):
            if origem == "local" or not comandos.bucket:
                raise SystemExit("Nenhum Bronze local; use --bronze ou --origem s3 --hostname HOST (com BUCKET_NAME).")
            usa_s3 = True
    if usa_s3:
        try:
            df_bronze, local_origem, *_ = carregar_bronze_s3(
                bucket=comandos.bucket, s3_key=comandos.s3_key,
                hostname=comandos.hostname, manifest_key=comandos.manifest_key)
        except Exception as erro:
            raise SystemExit(f"Não foi possível carregar Bronze do S3: {erro}") from erro
    else:
        if not os.path.exists(caminho_local):
            raise SystemExit(f"Arquivo Bronze local '{caminho_local}' não existe.")
        local_origem = caminho_local
        df_bronze = pd.read_csv(caminho_local, sep=";", encoding="utf-8")
    if df_bronze.empty:
        raise SystemExit("O Bronze selecionado não contém amostras.")
    print(f"Bronze carregado: {local_origem} ({len(df_bronze)} linhas)")

    cadastro, componentes = carregar_cadastro_do_banco()
    if cadastro is None or componentes.empty:
        raise SystemExit("Sem cadastro/componentes ativos no MySQL não há placa.numeroSerie; ETL interrompida.")
    print("Processando Silver...")
    try:
        df_silver = processar_camada_silver(df_bronze, cadastro, componentes)
    except (ValueError, TypeError) as erro:
        raise SystemExit(f"Erro de esquema/qualidade no Bronze: {erro}") from erro

    timestamp_execucao = datetime.now().strftime("%Y%m%dT%H%M%S")
    df_silver = quarentenar(df_silver, df_silver["q_hostname_nao_cadastrado"], "hostname", timestamp_execucao)
    df_silver = quarentenar(df_silver, df_silver["q_placa_nao_cadastrada"], "placa", timestamp_execucao)
    if df_silver.empty:
        raise SystemExit("Nenhuma amostra de hostname/placa cadastrados para processar. "
                         "Confira hostname e placa_nome do Bronze contra o MySQL.")

    print("Processando Gold...")
    df_sessao, df_serie, df_fase, df_alertas, df_diaria = processar_camada_gold(df_silver, componentes)
    df_resumo_placa = criar_resumo_placa(df_silver, df_sessao, df_alertas)
    df_resumo_fmc = criar_resumo_fmc(df_silver, df_resumo_placa)
    gold_por_nome = {
        "gold_kpi_sessao": anexar_cadastro(anexar_placa(df_sessao, df_silver), df_silver),
        "gold_serie_1min": anexar_cadastro(anexar_placa(df_serie, df_silver), df_silver),
        "gold_serie_diaria": anexar_cadastro(anexar_placa(df_diaria, df_silver), df_silver),
        "gold_kpi_fase": anexar_cadastro(anexar_placa(df_fase, df_silver), df_silver),
        "gold_alertas": anexar_cadastro(df_alertas, df_silver),
        "gold_resumo_placa": anexar_cadastro(df_resumo_placa, df_silver),
        "gold_resumo_fmc": anexar_cadastro(df_resumo_fmc, df_silver),
    }

    # Cada hostname recebe sua própria pasta e arquivos identificados pelo hostname + timestamp.
    for hostname_saida in sorted(df_silver["hostname"].astype(str).unique()):
        nome = _segmento_s3(hostname_saida)
        pasta_silver_host = PASTA_SILVER / f"hostname={nome}"
        pasta_gold_host = PASTA_GOLD / f"hostname={nome}"
        pasta_silver_host.mkdir(parents=True, exist_ok=True)
        pasta_gold_host.mkdir(parents=True, exist_ok=True)

        silver_host = df_silver[df_silver["hostname"].astype(str) == hostname_saida]
        caminho_silver = pasta_silver_host / f"{nome}_{timestamp_execucao}_silver.csv"
        silver_host.to_csv(caminho_silver, sep=";", index=False, encoding="utf-8")
        print(f"Silver salvo: {caminho_silver} ({len(silver_host)} linhas)")

        arquivos_gold_host = []
        for nome_base, dados in gold_por_nome.items():
            dados_host = dados[dados["hostname"].astype(str) == hostname_saida]
            caminho_gold = pasta_gold_host / f"{nome}_{timestamp_execucao}_{nome_base}.json"
            dados_host.to_json(caminho_gold, orient="records", date_format="iso", force_ascii=False, indent=2)
            arquivos_gold_host.append(str(caminho_gold))
            print(f"Gold salvo: {caminho_gold} ({len(dados_host)} linhas)")

        # Publica cada arquivo no prefixo do próprio hostname, sem misturar hosts.
        enviar_arquivos_s3([str(caminho_silver)], "02-silver", hostname_saida, timestamp_execucao)
        enviar_arquivos_s3(arquivos_gold_host, "03-gold", hostname_saida, timestamp_execucao)

    colunas_resumo = [
        "hostname", "placa_serie", "empresa_nome", "aeronave_nome", "sessao", "duracao_min", "amostras",
        "completude_pct", "cpu_media", "cpu_p95", "ram_media", "ram_tendencia_kb_h",
        "flash_autonomia_h", "health_score", "health_score_status",
    ]
    print("\nResumo Gold por sessão:")
    print(gold_por_nome["gold_kpi_sessao"][colunas_resumo].to_string(index=False))
    if not df_alertas.empty:
        print(f"\nEventos de alerta registrados: {len(df_alertas)}")
        print(df_alertas[["hostname", "placa_serie", "metrica", "tipo_evento", "nivel", "valor", "fase"]].to_string(index=False))
    else:
        print("\nNenhum evento de alerta gerado nesta execução.")