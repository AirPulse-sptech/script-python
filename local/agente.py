# Agente AirPulse: coleta telemetria para Bronze.
#
# Os campos observados são registrados no Bronze. As métricas derivadas,
# validações e avaliações de saúde ficam para as camadas Silver e Gold.

# ============================================================================
# Importações e carregamento do .env

import json
import os
import re
import socket
import time
from contextlib import closing
from http.client import HTTPConnection, HTTPException
from urllib.parse import urlsplit
from pathlib import Path
from tempfile import gettempdir
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import psutil
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / "config" / ".env", override=True)


# ============================================================================
# Configuração do S3, da sessão e da coleta

# Destino dos arquivos Bronze e credenciais usadas para acessar o S3.
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("aws_secret_access_key")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN") or os.getenv("aws_session_token")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("BUCKET_NAME") or os.getenv("S3_BUCKET_NAME")
S3_PREFIXO = os.getenv("S3_PREFIXO_BRONZE", "01-bronze").strip("/")
S3_PREFIXO_MANIFESTOS = f"{S3_PREFIXO}/manifestos"
HOSTNAME = socket.gethostname()
INICIO_EXECUCAO = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%dT%H%M%S%z")
SESSAO_EXECUCAO = INICIO_EXECUCAO
NOME_OBJETO_S3 = f"{HOSTNAME}_{INICIO_EXECUCAO}.csv"

# Cada execução tem seu próprio CSV para manter as amostras separadas por sessão.
PASTA_DADOS = os.path.join(gettempdir(), "airpulse")
CAMINHO_CSV = os.path.join(PASTA_DADOS, f"bronze_bruto_{NOME_OBJETO_S3}")
INTERVALO_COLETA_S = max(1, int(os.getenv("INTERVALO_COLETA_S", "2")))
AMOSTRAS_POR_LOTE = max(1, 60 // INTERVALO_COLETA_S)
RAM_TOTAL_REFERENCIA_BYTES = int(os.getenv("RAM_TOTAL_BYTES", psutil.virtual_memory().total))
FMC_FLASH_TOTAL_BYTES = 32 * 1024 * 1024
NOME_PROCESSO_ALVO = os.getenv("NOME_PROCESSO_ALVO", "bancada_voo")
ARQUIVO_ESTADO_BRONZE = os.path.join(
    PASTA_DADOS, os.getenv("ARQUIVO_ESTADO_BRONZE", "bronze_ativo.json"),
)
JANELA_DETECCAO_BANCADA_S = max(0.0, float(os.getenv("JANELA_DETECCAO_BANCADA_S", "5")))
ARQUIVO_FLASH = "fmc_flash_bite.log"
ARQUIVO_META = "bancada_meta.json"
COMPONENTES_TODOS = {"CPU", "RAM", "DISCO"}  # Componentes medidos quando a placa não tem metadados próprios.


# ============================================================================
# Colunas do CSV Bronze

# As unidades de leitura e escrita são registradas junto dos contadores de I/O.
COLUNAS = [
    "timestamp_utc", "seq",
    "hostname", "placa_nome", "origem", "uptime_proc_s", "fator_tempo", "cpu_percent", "cpu_nucleos",
    "qtd_tarefas", "ram_usada_bytes", "ram_baseline_bytes", "ram_total_bytes",
    "ram_disponivel_bytes", "flash_usada_bytes", "flash_total_bytes",
    "modo_execucao", "intervalo_coleta_s", "sessao_origem", "fase",
    "cpu_pct_host", "cpu_semantica", "ram_baseline_fonte", "ram_medicao_tipo",
    "io_leitura_contador_acum", "io_escrita_contador_acum",
    "io_leitura_unidade", "io_escrita_unidade",
]


# ============================================================================
# Cadastro das placas e metadados da bancada

def tipos_validos(lista):
    tipos = set()
    for tipo in lista:
        tipo = str(tipo).strip().upper()
        if tipo == "FLASH":
            tipo = "DISCO"
        if tipo in ("CPU", "RAM", "DISCO"):
            tipos.add(tipo)
    return tipos


def carregar_placas(metadados):
    # Os metadados associam o nome da placa ao PID, aos componentes e ao arquivo de flash.
    placas = []
    for item in metadados.get("placas") or []:
        try:
            nome, pid = str(item["placa_nome"]).strip(), int(item["pid"])
        except (KeyError, TypeError, ValueError):
            continue
        componentes = tipos_validos(item.get("componentes") or [])
        if nome and componentes:
            placas.append({"placa_nome": nome, "pid": pid, "componentes": componentes,
                           "arquivo_flash": item.get("arquivo_flash")})
    return placas


def buscar_placas_da_api(hostname):
    # O login no Node também retorna as placas cadastradas para o hostname.
    endereco = os.getenv("AGENTE_API_URL", "").strip()
    email = os.getenv("AGENTE_EMAIL", "").strip()
    senha = os.getenv("AGENTE_SENHA", "")
    faltantes = [
        nome for nome, valor in (
            ("AGENTE_API_URL", endereco), ("AGENTE_EMAIL", email), ("AGENTE_SENHA", senha),
        ) if not valor
    ]
    if faltantes:
        print(f"Login do agente sem configuração para: {', '.join(faltantes)}.")
        return []

    try:
        url = urlsplit(endereco)
        if not url.hostname or url.username is not None or url.query or url.fragment:
            raise ValueError("AGENTE_API_URL deve conter o endereço do Node.")

        if url.scheme != "http":
            raise ValueError("AGENTE_API_URL deve começar com http://.")
        conexao = HTTPConnection(url.hostname, url.port, timeout=5)

        base = url.path.rstrip("/").removesuffix("/agente/placas")
        rota = f"{base}/usuario/autenticar"
        print(f"Consultando placas do host {hostname}: POST {url.netloc}{rota}")
        credenciais = json.dumps({
            "emailServer": email, "senhaServer": senha, "cliente": "agente", "hostname": hostname,
        })
        with closing(conexao):
            conexao.request(
                "POST", rota, body=credenciais,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            with conexao.getresponse() as resposta:
                if resposta.status != 200:
                    detalhe = resposta.read(1024).decode("utf-8", errors="replace")
                    try:
                        mensagem = json.loads(detalhe)
                    except ValueError:
                        mensagem = detalhe
                    if isinstance(mensagem, dict):
                        mensagem = mensagem.get("mensagem") or mensagem.get("message") or mensagem.get("error")
                    if isinstance(mensagem, str):
                        campos_sensiveis = (email, senha, json.dumps(senha)[1:-1])
                        for valor in campos_sensiveis:
                            mensagem = mensagem.replace(valor, "[oculto]")
                        mensagem = " ".join(mensagem.split())[:300]
                    else:
                        mensagem = ""
                    raise ValueError(f"Login retornou HTTP {resposta.status}: {mensagem or 'sem detalhe'}.")
                resultado = json.load(resposta)

        placas = resultado.get("placas") if isinstance(resultado, dict) else None
        if not isinstance(placas, list) or any(not isinstance(placa, str) for placa in placas):
            raise ValueError("Resposta sem uma lista válida de placas.")
        nomes = list(dict.fromkeys(placa.strip() for placa in placas if placa.strip()))
        print(f"Login retornou HTTP 200 com {len(nomes)} placa(s) para o host {hostname}.")
        return nomes
    except (HTTPException, OSError, ValueError) as erro:
        print(f"Falha ao consultar placas no Node ({type(erro).__name__}): {erro}")
        return []

# ============================================================================
# Montagem da amostra de telemetria

def amostrar_placa(placa, proc, cache, cwd, ts, metadados, fator_tempo, baseline_ram, cpu_pct_host):
    comp = placa["componentes"]
    a = dict.fromkeys(COLUNAS)  # vazio por padrão: só preenche o que a placa mede
    cpu_bruto, tarefas = medir_cpu_e_tarefas(proc, cache)
    a.update({
        "timestamp_utc": ts, "hostname": HOSTNAME, "placa_nome": placa["placa_nome"],
        "origem": "SIMULADA_BANCADA", "uptime_proc_s": max(0, int(time.time() - proc.create_time())),
        "fator_tempo": fator_tempo, "qtd_tarefas": tarefas, "modo_execucao": "voo",
        "intervalo_coleta_s": INTERVALO_COLETA_S, "sessao_origem": metadados.get("sessao_origem"),
        "fase": metadados.get("fase") or "NAO_DETERMINADA",
    })
    if "CPU" in comp:
        nucleos = psutil.cpu_count(logical=True) or 1
        a.update({"cpu_percent": cpu_bruto, "cpu_nucleos": nucleos, "cpu_pct_host": cpu_pct_host,
                  "cpu_semantica": "soma_percentual_por_nucleo"})
    if "RAM" in comp:
        a.update({"ram_usada_bytes": proc.memory_info().rss, "ram_baseline_bytes": baseline_ram,
                  "ram_total_bytes": RAM_TOTAL_REFERENCIA_BYTES,
                  "ram_disponivel_bytes": psutil.virtual_memory().available,
                  "ram_baseline_fonte": "SEM_SUBTRACAO", "ram_medicao_tipo": "RSS_PROCESSO"})
    if "DISCO" in comp:
        try:
            flash = os.path.getsize(os.path.join(cwd, placa["arquivo_flash"]))
        except (OSError, TypeError):
            flash = None
        rc, ru = _ler_contador_io(proc, "read")
        wc, wu = _ler_contador_io(proc, "write")
        a.update({"flash_usada_bytes": flash, "flash_total_bytes": FMC_FLASH_TOTAL_BYTES,
                  "io_leitura_contador_acum": rc, "io_escrita_contador_acum": wc,
                  "io_leitura_unidade": ru, "io_escrita_unidade": wu})
    return a


def calibrar_baseline_ram():
    # Não subtrai o RSS de outro processo; retorna zero para usar RSS observado.
    return 0


# ============================================================================
# Identificação do processo e leitura dos metadados da bancada

def _normalizar_nome_processo(valor):
    # Normaliza nome de script independentemente do SO e de separadores.
    
    # Exemplos equivalentes: bancadaVoo.py, bancada_voo.py e
    # C:\projeto\bancada-voo.py.
    
    nome = str(valor or "").strip().strip('"').strip("'")
    # Trata caminhos Windows e POSIX mesmo quando o agente roda outro SO.
    nome = nome.replace("\\", "/").rsplit("/", 1)[-1].casefold()
    if nome.endswith(".py"):
        nome = nome[:-3]
    return "".join(caractere for caractere in nome if caractere.isalnum())


def encontrar_processo_simulador():
    # Aceita o nome configurado no .env e o nome padrão do script da bancada.
    
    
    alvos = {
        _normalizar_nome_processo(NOME_PROCESSO_ALVO),
        _normalizar_nome_processo("bancadaVoo.py"),
    }
    alvos.discard("")
    candidatos = []
    for proc in psutil.process_iter(["pid", "ppid", "cmdline"]):
        try:
            if proc.pid == os.getpid():
                continue
            cmd = proc.info.get("cmdline") or []
            if any(_normalizar_nome_processo(arg) in alvos for arg in cmd[1:]):
                candidatos.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # O processo pode encerrar ou ficar inacessível durante a varredura.
            continue
    # Launchers do Python no Windows podem repetir a linha de comando da bancada.
    for proc in candidatos:
        if ler_metadados_bancada(proc, permitir_cwd=False):
            return proc

    # Metadados antigos no cwd são compartilhados entre o launcher e seu filho.
    pids_pai = {proc.info.get("ppid") for proc in candidatos}
    for proc in candidatos:
        if proc.pid not in pids_pai and ler_metadados_bancada(proc):
            return proc
    return None


def ler_metadados_bancada(proc, *, permitir_cwd=True):
    # Procura os metadados da sessão do PID e depois na pasta de execução.
    try:
        caminho = os.path.join(PASTA_DADOS, f"bancada-{proc.pid}", ARQUIVO_META)
        if not os.path.exists(caminho):
            if not permitir_cwd:
                return {}
            caminho = os.path.join(proc.cwd(), ARQUIVO_META)
        with open(caminho, encoding="utf-8") as arquivo:
            dados = json.load(arquivo)
        if not isinstance(dados, dict):
            return {}
        return dados
    except (OSError, ValueError, TypeError, psutil.Error):
        return {}


def ler_fator_tempo(proc):
    metadados = ler_metadados_bancada(proc)
    try:
        fator = float(metadados.get("acelerador", os.getenv("FATOR_TEMPO", "1")))
        return fator if fator > 0 else 1.0
    except (TypeError, ValueError):
        return 1.0


# ============================================================================
# Leitura dos contadores de CPU, threads e I/O

def medir_cpu_e_tarefas(proc, cache_filhos):
    # Soma o uso de CPU em escala por núcleo e as threads do processo e de seus filhos.
    cpu_total = proc.cpu_percent(interval=None)
    tarefas_total = proc.num_threads()
    vivos = set()
    try:
        filhos = proc.children(recursive=True)
    except psutil.NoSuchProcess:
        filhos = []
    for filho in filhos:
        try:
            medido = cache_filhos.setdefault(filho.pid, filho)
            cpu_total += medido.cpu_percent(interval=None)
            tarefas_total += medido.num_threads()
            vivos.add(filho.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            cache_filhos.pop(filho.pid, None)
    for pid in list(cache_filhos):
        if pid not in vivos:
            del cache_filhos[pid]
    return cpu_total, tarefas_total


def _ler_contador_io(proc, tipo):
    # Registra a unidade do contador de I/O disponível no sistema.
    try:
        contadores = proc.io_counters()
    except (psutil.Error, OSError, AttributeError):
        return None, "desconhecida"
    atributo_bytes = f"{tipo}_bytes"
    atributo_chars = f"{tipo}_chars"
    valor_bytes = getattr(contadores, atributo_bytes, None)
    if valor_bytes is not None:
        return int(valor_bytes), "bytes"
    valor_chars = getattr(contadores, atributo_chars, None)
    if valor_chars is not None:
        return int(valor_chars), "chars"
    return None, "desconhecida"


# ============================================================================
# Gravação do CSV de preparação para o S3

def salvar_localmente(amostra, caminho_destino=CAMINHO_CSV):
    os.makedirs(os.path.dirname(caminho_destino) or ".", exist_ok=True)
    arquivo_novo = not os.path.exists(caminho_destino)
    # Preserva o arquivo Bronze anterior como backup se o esquema mudar.
    if not arquivo_novo and caminho_destino == CAMINHO_CSV:
        with open(caminho_destino, encoding="utf-8") as arquivo:
            cabecalho_antigo = arquivo.readline().strip().split(";")
        if cabecalho_antigo != COLUNAS:
            backup = f"{caminho_destino}.{int(time.time())}.bak"
            os.rename(caminho_destino, backup)
            arquivo_novo = True
    pd.DataFrame([amostra], columns=COLUNAS).to_csv(
        caminho_destino, sep=";", mode="a", index=False, header=arquivo_novo,
        encoding="utf-8",
    )
    if caminho_destino == CAMINHO_CSV:
        cpu_exibida = amostra.get("cpu_pct_host")
        if cpu_exibida is not None:
            print(
                f"[{amostra['timestamp_utc']}] Amostra #{amostra['seq']} | "
                f"CPU total do host: {float(cpu_exibida):.1f}%"
            )


# ============================================================================
# Publicação do Bronze e dos manifestos no S3

def obter_cliente_s3():
    # Usa as credenciais configuradas no .env ou as encontradas pelo boto3.
    import boto3

    if not BUCKET_NAME:
        raise ValueError("BUCKET_NAME (ou S3_BUCKET_NAME) não está configurado.")
    configuracao = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        configuracao["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        configuracao["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
        if AWS_SESSION_TOKEN:
            configuracao["aws_session_token"] = AWS_SESSION_TOKEN
    elif AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY or AWS_SESSION_TOKEN:
        raise ValueError(
            "Credenciais AWS incompletas: configure Access Key e Secret Key juntas, "
            "ou remova as chaves para usar o perfil/IAM Role."
        )
    # Sem chaves explícitas, boto3 usa profile/SSO/variáveis padrão/IAM Role da EC2.
    sessao = boto3.session.Session(**configuracao)
    if sessao.get_credentials() is None:
        raise RuntimeError("Credenciais AWS não encontradas para enviar o Bronze ao S3.")
    return sessao.client("s3")


def _segmento_s3(valor):
    # Trata o valor como um único segmento da chave do objeto no S3.
    segmento = re.sub(r"[^A-Za-z0-9._-]+", "_", str(valor or "desconhecido").strip())
    return segmento.strip("._-") or "desconhecido"


def _chave_manifesto_s3(hostname=HOSTNAME):
    return f"{S3_PREFIXO_MANIFESTOS}/hostname={_segmento_s3(hostname)}/bronze_ativo.json"


def registrar_bronze_ativo(
    caminho_csv,
    *,
    s3_client=None,
    chave_s3=None,
    hostname=HOSTNAME,
    sessao_id=SESSAO_EXECUCAO,
    estado_coleta="EM_ANDAMENTO",
):
    # O manifesto indica qual CSV pertence à sessão e informa o estado da coleta.
    estado = {
        "arquivo_bronze": os.path.abspath(caminho_csv),
        "bucket": BUCKET_NAME if chave_s3 else None,
        "chave_s3": chave_s3,
        "s3_uri": f"s3://{BUCKET_NAME}/{chave_s3}" if chave_s3 else None,
        "chave_manifesto_s3": _chave_manifesto_s3(hostname) if chave_s3 else None,
        "hostname": hostname,
        "sessao_id": sessao_id,
        "modo": "real",
        "estado_coleta": estado_coleta,
        "atualizado_em_utc": datetime.now(timezone.utc).isoformat(),
        "data_inicio": None,
    }
    if s3_client is not None and chave_s3:
        chave_manifesto = _chave_manifesto_s3(hostname)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=chave_manifesto,
            Body=json.dumps(estado, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"Manifesto publicado: s3://{BUCKET_NAME}/{chave_manifesto}")
        if estado_coleta == "CONCLUIDA":
            # O marcador por sessão dispara a ETL só depois do upload completo.
            chave_concluida = f"{os.path.splitext(chave_s3)[0]}_concluido.json"
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=chave_concluida,
                Body=json.dumps({"chave_s3": chave_s3}).encode("utf-8"),
                ContentType="application/json",
            )

    # Substituição atômica evita que outro processo leia um JSON pela metade.
    os.makedirs(os.path.dirname(ARQUIVO_ESTADO_BRONZE) or ".", exist_ok=True)
    temporario = f"{ARQUIVO_ESTADO_BRONZE}.tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        json.dump(estado, arquivo, ensure_ascii=False, indent=2)
    os.replace(temporario, ARQUIVO_ESTADO_BRONZE)
    return estado


def enviar_lote_nuvem(
    s3_client,
    caminho_csv,
    hostname=HOSTNAME,
    *,
    sessao_id=None,
    estado_coleta="EM_ANDAMENTO",
):
    # Envia o CSV antes de publicar o manifesto que aponta para ele.
    if not os.path.isfile(caminho_csv):
        raise FileNotFoundError(f"CSV Bronze não encontrado: {caminho_csv}")

    nome_arquivo = os.path.basename(caminho_csv)
    sessao = _segmento_s3(sessao_id or os.path.splitext(nome_arquivo)[0])
    host_s3 = _segmento_s3(hostname)
    chave_s3 = (
        f"{S3_PREFIXO}/hostname={host_s3}/sessao={sessao}/{_segmento_s3(nome_arquivo)}"
    )
    try:
        # Cada lote atualiza o mesmo objeto com todas as amostras acumuladas na sessão.
        s3_client.upload_file(caminho_csv, BUCKET_NAME, chave_s3)
        print(f"Bronze enviado: s3://{BUCKET_NAME}/{chave_s3}")
        registrar_bronze_ativo(
            caminho_csv,
            s3_client=s3_client,
            chave_s3=chave_s3,
            hostname=hostname,
            sessao_id=sessao,
            estado_coleta=estado_coleta,
        )
    except Exception as erro:
        raise RuntimeError(
            f"Falha ao publicar Bronze ou manifesto no S3: {erro}. "
            f"CSV da coleta: {caminho_csv}"
        ) from erro


# ============================================================================
# Espera pela bancada e associação das placas aos processos

def aguardar_bancada(janela_s=None):
    janela = JANELA_DETECCAO_BANCADA_S if janela_s is None else max(0.0, janela_s)
    prazo = time.monotonic() + janela
    while True:
        simulador = encontrar_processo_simulador()
        if simulador is not None:
            return simulador
        restante = prazo - time.monotonic()
        if restante <= 0:
            return None
        # Dá alguns segundos para a bancada iniciar.
        time.sleep(min(0.5, restante))


def obter_placas_real(metadados, simulador, nomes_placas):
    # Os metadados complementam as placas retornadas pelo Node.
    metadados_por_placa = {placa["placa_nome"]: placa for placa in carregar_placas(metadados)}
    arquivo_flash = metadados.get("arquivo_flash") or ARQUIVO_FLASH
    placas = []
    for nome in nomes_placas:
        placa = metadados_por_placa.get(nome)
        if placa is None:
            placa = {"placa_nome": nome, "pid": simulador.pid,
                     "componentes": COMPONENTES_TODOS, "arquivo_flash": arquivo_flash}
        placas.append(placa)
    return placas


# ============================================================================
# Ciclo de coleta e envio dos lotes

def rodar_modo_real(simulador_inicial=None):
    simulador = simulador_inicial or encontrar_processo_simulador()
    if simulador is None:
        raise RuntimeError("A bancada não foi encontrada para a coleta real.")
    nomes_placas = buscar_placas_da_api(HOSTNAME)
    if not nomes_placas:
        raise RuntimeError(f"O Node não retornou placas para o host {HOSTNAME}. Coleta encerrada.")

    s3_client = obter_cliente_s3()
    # Registra a sessão para localizar o CSV local do último voo.
    registrar_bronze_ativo(
        CAMINHO_CSV, hostname=HOSTNAME, sessao_id=SESSAO_EXECUCAO,
        estado_coleta="EM_ANDAMENTO",
    )

    baseline_ram = calibrar_baseline_ram()
    seq = 0
    procs_placa = {}   # Guarda o processo e o cache dos filhos por placa.
    seq_placa = {}
    ciclos = 0
    print(f"Bancada detectada (PID {simulador.pid}). Coletando placas no host {HOSTNAME}.")
    psutil.cpu_percent(interval=None)  # Prepara a medição do host para o primeiro intervalo de coleta.
    proximo_ciclo = time.monotonic() + INTERVALO_COLETA_S
    while True:
        try:
            if not simulador.is_running():
                break
            time.sleep(max(0, proximo_ciclo - time.monotonic()))
            if not simulador.is_running():
                break
            agora_monotonico = time.monotonic()
            proximo_ciclo += INTERVALO_COLETA_S
            if proximo_ciclo <= agora_monotonico:
                proximo_ciclo = agora_monotonico + INTERVALO_COLETA_S
            cpu_pct_host = psutil.cpu_percent(interval=None)
            metadados = ler_metadados_bancada(simulador)
            if not metadados:
                continue
            try:
                fator_tempo = float(metadados.get("acelerador", 1))
            except (TypeError, ValueError):
                fator_tempo = 1.0
            ts = datetime.now(timezone.utc).isoformat()
            cwd = simulador.cwd()
            gravou = False
            placas = obter_placas_real(metadados, simulador, nomes_placas)
            for placa in placas:
                nome = placa["placa_nome"]
                try:
                    if nome not in procs_placa:
                        proc, cache = psutil.Process(placa["pid"]), {}
                        medir_cpu_e_tarefas(proc, cache)  # A primeira leitura prepara o contador de CPU para o próximo ciclo.
                        procs_placa[nome] = (proc, cache)
                        continue
                    proc, cache = procs_placa[nome]
                    amostra = amostrar_placa(
                        placa, proc, cache, cwd, ts, metadados, fator_tempo, baseline_ram, cpu_pct_host
                    )
                except psutil.Error:
                    procs_placa.pop(nome, None)  # Descarta o processo para localizá-lo novamente no próximo ciclo.
                    continue
                seq_placa[nome] = seq_placa.get(nome, 0) + 1
                amostra["seq"] = seq_placa[nome]
                salvar_localmente(amostra)
                gravou = True
            if gravou:
                ciclos += 1
                if ciclos % AMOSTRAS_POR_LOTE == 0:
                    enviar_lote_nuvem(s3_client, CAMINHO_CSV, hostname=HOSTNAME,
                                      sessao_id=SESSAO_EXECUCAO, estado_coleta="EM_ANDAMENTO")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            break
    if ciclos:
        enviar_lote_nuvem(s3_client, CAMINHO_CSV, hostname=HOSTNAME,
                          sessao_id=SESSAO_EXECUCAO, estado_coleta="CONCLUIDA")
    print(f"Coleta encerrada. Bronze real salvo em {CAMINHO_CSV} ({sum(seq_placa.values())} novas amostras).")


# ============================================================================
# Execução pelo terminal

def executar_automaticamente():
    simulador = aguardar_bancada()
    if simulador is None:
        print("Bancada não encontrada. Inicie local/bancadaVoo.py e tente novamente.")
        return

    rodar_modo_real(simulador)


if __name__ == "__main__":
    import argparse

    argparse.ArgumentParser(description="Agente AirPulse para coleta da bancada").parse_args()
    executar_automaticamente()
