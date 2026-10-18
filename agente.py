# Agente AirPulse: coleta telemetria para Bronze.
#
# Os campos observados são registrados no Bronze. As métricas derivadas,
# validações e avaliações de saúde ficam para as camadas Silver e Gold.
import json
import os
import random
import re
import socket
import time
from datetime import date, datetime, timedelta, timezone, time as hora
import pandas as pd
import psutil
from dotenv import load_dotenv

load_dotenv(override=True)

# Identidade e nuvem. Em EC2, prefira uma IAM Role em vez de chaves fixas no .env.
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("aws_secret_access_key")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN") or os.getenv("aws_session_token")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("BUCKET_NAME") or os.getenv("S3_BUCKET_NAME", "mateus-airpulse")
S3_PREFIXO = os.getenv("S3_PREFIXO_BRONZE", "01-bronze").strip("/")
S3_PREFIXO_MANIFESTOS = f"{S3_PREFIXO}/manifestos"
HOSTNAME = socket.gethostname()
INICIO_EXECUCAO = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
SESSAO_EXECUCAO = INICIO_EXECUCAO
NOME_OBJETO_S3 = f"{HOSTNAME}_{INICIO_EXECUCAO}.csv"
# Cada execução usa um CSV local independente: amostras antigas não entram na sessão nova.
CAMINHO_CSV = f"bronze_bruto_{NOME_OBJETO_S3}"
INTERVALO_COLETA_S = max(1, int(os.getenv("INTERVALO_COLETA_S", "2")))
AMOSTRAS_POR_LOTE = max(1, 60 // INTERVALO_COLETA_S)
RAM_TOTAL_REFERENCIA_BYTES = int(os.getenv("RAM_TOTAL_BYTES", psutil.virtual_memory().total))
FMC_FLASH_TOTAL_BYTES = 32 * 1024 * 1024
NOME_PROCESSO_ALVO = os.getenv("NOME_PROCESSO_ALVO", "bancada_voo")
ARQUIVO_ESTADO_BRONZE = os.getenv("ARQUIVO_ESTADO_BRONZE", "bronze_ativo.json")
JANELA_DETECCAO_BANCADA_S = max(0.0, float(os.getenv("JANELA_DETECCAO_BANCADA_S", "5")))
ARQUIVO_FLASH = "fmc_flash_bite.log"
ARQUIVO_META = "bancada_meta.json"
# Lista as colunas do contrato atual do Bronze.
# Os contadores canônicos vêm acompanhados da unidade, para não confundir bytes com caracteres.
# Campos derivados ficam para as camadas seguintes; os aliases repetidos de I/O foram retirados.
COLUNAS = [
    "timestamp_utc", "seq",
    "hostname", "origem", "uptime_proc_s", "fator_tempo", "cpu_percent", "cpu_nucleos",
    "qtd_tarefas", "ram_usada_bytes", "ram_baseline_bytes", "ram_total_bytes",
    "ram_disponivel_bytes", "flash_usada_bytes", "flash_total_bytes",
    "modo_execucao", "intervalo_coleta_s", "sessao_origem", "fase",
    "cpu_pct_host", "cpu_semantica", "ram_baseline_fonte", "ram_medicao_tipo",
    "io_leitura_contador_acum", "io_escrita_contador_acum",
    "io_leitura_unidade", "io_escrita_unidade",
]

# Identidades virtuais disponíveis para a simulação.
PLACAS_SIMULADAS = [
    "FMC-SIM-PLACA-01",
    "FMC-SIM-PLACA-02",
]

ARQUIVO_ROTACAO = "placa_rotacao.json"


def selecionar_hostname_logico():
    # Por padrão, começa pela primeira identidade.
    indice = 0

    # Recupera qual identidade deve ser usada nesta execução.
    if os.path.exists(ARQUIVO_ROTACAO):
        try:
            with open(ARQUIVO_ROTACAO, encoding="utf-8") as arquivo:
                estado = json.load(arquivo)
            indice = int(estado.get("proximo_indice", 0))
        except (OSError, ValueError, TypeError):
            indice = 0

    # Garante que o índice fique dentro da lista.
    indice %= len(PLACAS_SIMULADAS)
    hostname_logico = PLACAS_SIMULADAS[indice]

    # Deixa a próxima identidade preparada para a próxima execução.
    estado = {
        "proximo_indice": (indice + 1) % len(PLACAS_SIMULADAS)
    }

    # Grava o estado de forma atômica para evitar JSON incompleto.
    temporario = f"{ARQUIVO_ROTACAO}.tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        json.dump(estado, arquivo, indent=2)

    os.replace(temporario, ARQUIVO_ROTACAO)

    return hostname_logico


def obter_cliente_s3():
    """Cria cliente S3 com chaves explícitas ou com a cadeia padrão boto3/IAM Role."""
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
    return boto3.client("s3", **configuracao)


def _segmento_s3(valor):
    """Evita barras e caracteres problemáticos em segmentos de chaves S3."""
    segmento = re.sub(r"[^A-Za-z0-9._-]+", "_", str(valor or "desconhecido").strip())
    return segmento.strip("._-") or "desconhecido"


def _chave_manifesto_s3(hostname=HOSTNAME):
    return f"{S3_PREFIXO_MANIFESTOS}/hostname={_segmento_s3(hostname)}/bronze_ativo.json"


def registrar_bronze_ativo(
    caminho_csv,
    modo,
    data_inicio=None,
    *,
    s3_client=None,
    chave_s3=None,
    hostname=HOSTNAME,
    sessao_id=SESSAO_EXECUCAO,
    estado_coleta="EM_ANDAMENTO",
):
    """Registra localmente e, quando houver referência S3, publica o manifesto no bucket."""
    estado = {
        "arquivo_bronze": os.path.abspath(caminho_csv),
        "bucket": BUCKET_NAME if chave_s3 else None,
        "chave_s3": chave_s3,
        "s3_uri": f"s3://{BUCKET_NAME}/{chave_s3}" if chave_s3 else None,
        "chave_manifesto_s3": _chave_manifesto_s3(hostname) if chave_s3 else None,
        "hostname": hostname,
        "sessao_id": sessao_id,
        "modo": modo,
        "estado_coleta": estado_coleta,
        "atualizado_em_utc": datetime.now(timezone.utc).isoformat(),
        "data_inicio": data_inicio.isoformat() if data_inicio else None,
    }
    # Substituição atômica evita que outro processo leia um JSON pela metade.
    temporario = f"{ARQUIVO_ESTADO_BRONZE}.tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        json.dump(estado, arquivo, ensure_ascii=False, indent=2)
    os.replace(temporario, ARQUIVO_ESTADO_BRONZE)

    if s3_client is not None and chave_s3:
        chave_manifesto = _chave_manifesto_s3(hostname)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=chave_manifesto,
            Body=json.dumps(estado, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"Manifesto publicado: s3://{BUCKET_NAME}/{chave_manifesto}")
    return estado


def enviar_lote_nuvem(
    s3_client,
    caminho_csv,
    hostname=HOSTNAME,
    nome_arquivo=None,
    *,
    sessao_id=None,
    modo="real",
    data_inicio=None,
    estado_coleta="EM_ANDAMENTO",
):
    """Envia o CSV completo da sessão e publica o manifesto somente após o upload."""
    if s3_client is None:
        print("Erro: cliente S3 não foi inicializado.")
        return False
    if not os.path.exists(caminho_csv):
        print(f"Erro: arquivo não encontrado: {caminho_csv}")
        return False

    nome_arquivo = nome_arquivo or os.path.basename(caminho_csv)
    sessao = _segmento_s3(sessao_id or os.path.splitext(nome_arquivo)[0])
    host_s3 = _segmento_s3(hostname)
    chave_s3 = (
        f"{S3_PREFIXO}/hostname={host_s3}/sessao={sessao}/{_segmento_s3(nome_arquivo)}"
    )
    try:
        # Cada upload substitui o objeto da MESMA sessão com o CSV local completo.
        s3_client.upload_file(caminho_csv, BUCKET_NAME, chave_s3)
        print(f"Bronze enviado: s3://{BUCKET_NAME}/{chave_s3}")
        registrar_bronze_ativo(
            caminho_csv,
            modo,
            data_inicio,
            s3_client=s3_client,
            chave_s3=chave_s3,
            hostname=hostname,
            sessao_id=sessao,
            estado_coleta=estado_coleta,
        )
        return True
    except Exception as erro:
        print(f"Erro ao enviar Bronze/manifesto ao S3: {erro}")
        return False


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
                f"CPU host estimada: {float(cpu_exibida):.1f}%"
            )

def calibrar_baseline_ram():
    # Não subtrai o RSS de outro processo; retorna zero para usar RSS observado.
    return 0

def _normalizar_nome_processo(valor):
    # Normaliza nome de script independentemente do SO e de separadores.
    #
    # Exemplos equivalentes: bancadaVoo.py, bancada_voo.py e
    # C:\projeto\bancada-voo.py.
    #
    nome = str(valor or "").strip().strip('"').strip("'")
    # Trate caminhos Windows e POSIX mesmo quando o agente roda noutro SO.
    nome = nome.replace("\\\\", "/").rsplit("/", 1)[-1].casefold()
    if nome.endswith(".py"):
        nome = nome[:-3]
    return "".join(caractere for caractere in nome if caractere.isalnum())

def encontrar_processo_simulador():
    # Localiza a bancada por nome normalizado; permite configurar o nome alvo.
    # Por compatibilidade, reconhece o nome padrão configurado e o nome real
    # usado neste repositório (bancadaVoo.py), mesmo com underscore/hífen.
    #
    alvos = {
        _normalizar_nome_processo(NOME_PROCESSO_ALVO),
        _normalizar_nome_processo("bancadaVoo.py"),
    }
    alvos.discard("")
    candidatos = {}
    for proc in psutil.process_iter(["pid", "ppid", "cmdline"]):
        try:
            if proc.pid == os.getpid():
                continue
            cmd = proc.info.get("cmdline") or []
            if any(_normalizar_nome_processo(arg) in alvos for arg in cmd[1:]):
                candidatos[proc.pid] = proc
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # O processo pode encerrar ou ficar inacessível durante a varredura.
            continue
    # Se o mesmo comando também aparecer nos filhos, prefira o processo raiz.
    for proc in candidatos.values():
        try:
            if proc.info.get("ppid") not in candidatos:
                return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return None

def ler_metadados_bancada(proc):
    # Lê metadados explícitos do simulador; tolera arquivos de versões antigas.
    try:
        with open(os.path.join(proc.cwd(), ARQUIVO_META), encoding="utf-8") as arquivo:
            dados = json.load(arquivo)
        if not isinstance(dados, dict):
            return {}
        return dados
    except (OSError, ValueError, TypeError, psutil.Error):
        return {}
def ler_fator_tempo(proc):
    # Mantém a interface usada em versões antigas do agente.
    metadados = ler_metadados_bancada(proc)
    try:
        fator = float(metadados.get("acelerador", os.getenv("FATOR_TEMPO", "1")))
        return fator if fator > 0 else 1.0
    except (TypeError, ValueError):
        return 1.0
def medir_cpu_e_tarefas(proc, cache_filhos):
    # Soma CPU percentual por processo (escala psutil, por núcleo) e threads.
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
    # Prioriza contadores de bytes; fallback para caracteres fica identificado.
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
def detectar_modo_automatico(janela_s=None):
    # Escolhe uma vez: bancada ativa = coleta real; senão = semana sintética.
    janela = JANELA_DETECCAO_BANCADA_S if janela_s is None else max(0.0, janela_s)
    prazo = time.monotonic() + janela
    while True:
        simulador = encontrar_processo_simulador()
        if simulador is not None:
            return "real", simulador
        restante = prazo - time.monotonic()
        if restante <= 0:
            return "semana", None
        # Dá alguns segundos para a bancada iniciar, sem pedir um modo no terminal.
        time.sleep(min(0.5, restante))
def rodar_modo_real(simulador_inicial=None):
    print(
    f"Agente AirPulse iniciado no host {HOSTNAME}. "
    "Coletando a bancada...")
    simulador = simulador_inicial or encontrar_processo_simulador()
    if simulador is None:
        raise RuntimeError("A bancada não foi encontrada para a coleta real.")
    # A ETL usa este arquivo para saber que deve tratar o Bronze da coleta real.
    registrar_bronze_ativo(
        CAMINHO_CSV, "real", hostname=HOSTNAME, sessao_id=SESSAO_EXECUCAO,
        estado_coleta="EM_ANDAMENTO",
    )
    s3_client = obter_cliente_s3()
    baseline_ram = calibrar_baseline_ram()
    seq = 0
    cache_filhos = {}
    try:
        metadados = ler_metadados_bancada(simulador)
        fator_tempo = ler_fator_tempo(simulador)
        print(
            f"Bancada detectada (PID {simulador.pid}) | "
            f"Fator de tempo: {fator_tempo:g}x"
        )
        # A primeira chamada inicializa os contadores de CPU; evita um pico falso.
        medir_cpu_e_tarefas(simulador, cache_filhos)
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        print("A bancada encerrou antes da primeira amostra. Nenhum dado novo foi coletado.")
        return
    proximo_ciclo = time.monotonic() + INTERVALO_COLETA_S
    while True:
        try:
            if not simulador.is_running():
                break
            time.sleep(max(0, proximo_ciclo - time.monotonic()))
            # Se a bancada encerrou durante a espera, não cria uma amostra artificial.
            if not simulador.is_running():
                break
            agora_monotonico = time.monotonic()
            proximo_ciclo += INTERVALO_COLETA_S
            if proximo_ciclo <= agora_monotonico:
                proximo_ciclo = agora_monotonico + INTERVALO_COLETA_S
            cpu_percent_bruto, qtd_tarefas = medir_cpu_e_tarefas(simulador, cache_filhos)
            nucleos = psutil.cpu_count(logical=True) or 1
            cpu_pct_host = cpu_percent_bruto / nucleos
            metadados = ler_metadados_bancada(simulador)
            try:
                fator_tempo = float(metadados.get("acelerador", ler_fator_tempo(simulador)))
            except (TypeError, ValueError):
                fator_tempo = ler_fator_tempo(simulador)
            # Lê o contador cumulativo e guarda explicitamente sua unidade.
            # Se o sistema não oferecer o contador, o valor continua desconhecido.
            read_cont, read_unit = _ler_contador_io(simulador, "read")
            write_cont, write_unit = _ler_contador_io(simulador, "write")
            try:
                flash_usada = os.path.getsize(os.path.join(simulador.cwd(), ARQUIVO_FLASH))
            except FileNotFoundError:
                flash_usada = 0
            except (OSError, psutil.Error):
                flash_usada = None
            amostra = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "seq": seq + 1,
                "hostname": HOSTNAME,
                "origem": "SIMULADA_BANCADA",
                "uptime_proc_s": max(0, int(time.time() - simulador.create_time())),
                "fator_tempo": fator_tempo,
                "cpu_percent": cpu_percent_bruto,
                "cpu_nucleos": nucleos,
                "qtd_tarefas": qtd_tarefas,
                "ram_usada_bytes": simulador.memory_info().rss,
                "ram_baseline_bytes": baseline_ram,
                "ram_total_bytes": RAM_TOTAL_REFERENCIA_BYTES,
                "ram_disponivel_bytes": psutil.virtual_memory().available,
                "flash_usada_bytes": flash_usada,
                "flash_total_bytes": FMC_FLASH_TOTAL_BYTES,
                "modo_execucao": "voo",
                "intervalo_coleta_s": INTERVALO_COLETA_S,
                "sessao_origem": metadados.get("sessao_origem"),
                "fase": metadados.get("fase") or "NAO_DETERMINADA",
                "cpu_pct_host": cpu_pct_host,
                "cpu_semantica": "soma_percentual_por_nucleo",
                "ram_baseline_fonte": "SEM_SUBTRACAO",
                "ram_medicao_tipo": "RSS_PROCESSO",
                "io_leitura_contador_acum": read_cont,
                "io_escrita_contador_acum": write_cont,
                "io_leitura_unidade": read_unit,
                "io_escrita_unidade": write_unit,
            }
            seq += 1
            salvar_localmente(amostra)
            if seq % AMOSTRAS_POR_LOTE == 0:
                enviar_lote_nuvem(
                    s3_client, CAMINHO_CSV, hostname=HOSTNAME,
                    sessao_id=SESSAO_EXECUCAO, modo="real", estado_coleta="EM_ANDAMENTO",
                )
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # A amostra perdida não é sintetizada; a Silver calculará a lacuna temporal.
            break
    # Envia o snapshot final sempre, inclusive quando o último lote coincidiu com a cadência.
    # O manifesto passa a CONCLUIDA somente depois do upload final do CSV completo.
    if seq:
        enviado_final = enviar_lote_nuvem(
            s3_client, CAMINHO_CSV, hostname=HOSTNAME,
            sessao_id=SESSAO_EXECUCAO, modo="real", estado_coleta="CONCLUIDA",
        )
        if not enviado_final:
            print("Bronze foi mantido localmente, mas o envio final ao S3 falhou.")
    print(f"Coleta encerrada. Bronze real salvo em {CAMINHO_CSV} ({seq} novas amostras).")
def estado_simulado_voo(instante_atual, agenda_de_voos):
    for partida, chegada in agenda_de_voos:
        if partida <= instante_atual < chegada:
            minutos_partida = (instante_atual - partida).total_seconds() / 60
            minutos_chegada = (chegada - instante_atual).total_seconds() / 60
            if minutos_partida < 15 or minutos_chegada <= 15:
                return 72, True
            return 38, True
    return 12, False
def fase_semana(instante_atual, agenda_de_voos):
    # O modo semanal não simula as dez fases detalhadas do roteiro da bancada.
    for partida, chegada in agenda_de_voos:
        if partida <= instante_atual < chegada:
            minutos_partida = (instante_atual - partida).total_seconds() / 60
            minutos_chegada = (chegada - instante_atual).total_seconds() / 60
            if minutos_partida < 15 or minutos_chegada <= 15:
                return "TRANSICAO"
            return "EM_VOO"
    return "SOLO"
def criar_agenda_simples(data_inicio):
    fuso = timezone(timedelta(hours=-3))
    voos_por_dia = [6, 6, 6, 6, 7, 5, 6]
    primeira_partida = [360, 370, 380, 360, 360, 420, 390]
    rng = random.Random(42)
    agenda = []
    for numero_do_dia in range(7):
        dia_atual = data_inicio + timedelta(days=numero_do_dia)
        dia_da_semana = dia_atual.weekday()
        qtd_voos = voos_por_dia[dia_da_semana]
        hora_da_partida = datetime.combine(dia_atual, hora(), tzinfo=fuso) + timedelta(
            minutes=primeira_partida[dia_da_semana]
        )
        for _ in range(qtd_voos):
            hora_da_chegada = hora_da_partida + timedelta(
                minutes=rng.choice([95, 100, 105, 110, 115])
            )
            agenda.append((hora_da_partida, hora_da_chegada))
            hora_da_partida = hora_da_chegada + timedelta(minutes=rng.choice([40, 45, 50]))
    return agenda
def rodar_modo_semana(data_inicio, enviar_s3):
    # O modo é escolhido uma vez na inicialização: sem bancada ativa, gera a semana sintética.
    # Depois de escolhido, não fica verificando nem cancela a geração no meio do caminho.
    print("Modo SEMANA: gerando 7 dias em intervalos de 300 segundos...")
    fuso = timezone(timedelta(hours=-3))
    rng = random.Random(42)
    agenda_de_voos = criar_agenda_simples(data_inicio)
    primeiro_instante = datetime.combine(data_inicio, hora(), tzinfo=fuso)
    sessao_origem = f"SEMANA-{data_inicio.isoformat()}"
    linhas_do_csv = []
    ruido_cpu = 0.0
    ruido_ram = 0.0
    io_escrita_acum = 0
    io_leitura_acum = 0
    # Sete dias com amostragem a cada cinco minutos: 7 * 24 * 12 = 2016 amostras.
    for amostra in range(2016):
        instante_atual = primeiro_instante + timedelta(minutes=5 * amostra)
        base_cpu, aviao_em_operacao = estado_simulado_voo(instante_atual, agenda_de_voos)
        fase = fase_semana(instante_atual, agenda_de_voos)
        ruido_cpu = (0.55 * ruido_cpu) + rng.gauss(0, 2)
        cpu_final = round(max(0, min(100, base_cpu + ruido_cpu)), 1)
        ruido_ram = (0.8 * ruido_ram) + rng.gauss(0, 0.4)
        ram_base_pct = 48 + (6 if aviao_em_operacao else 0)
        ram_final_pct = max(0, min(100, ram_base_pct + ruido_ram))
        flash_final_pct = 52 + (0.20 * amostra / 288)
        io_escrita_acum += rng.randint(15000, 60000)
        io_leitura_acum += rng.randint(5000, 20000)
        cpu_pct_host = cpu_final
        pacote = {
            "timestamp_utc": instante_atual.astimezone(timezone.utc).isoformat(),
            "seq": amostra + 1,
            "hostname": "GERADOR_SINTETICO",
            "origem": "SIMULADA_GERADOR",
            # Não é uptime de processo no modo semanal.
            "uptime_proc_s": None,
            "fator_tempo": 1.0,
            "cpu_percent": cpu_final,
            "cpu_nucleos": 1,
            "qtd_tarefas": 18 + (4 if aviao_em_operacao else 0) + rng.randint(0, 3),
            "ram_usada_bytes": int((ram_final_pct / 100) * RAM_TOTAL_REFERENCIA_BYTES),
            "ram_baseline_bytes": 0,
            "ram_total_bytes": RAM_TOTAL_REFERENCIA_BYTES,
            "ram_disponivel_bytes": int((1 - ram_final_pct / 100) * RAM_TOTAL_REFERENCIA_BYTES),
            "flash_usada_bytes": int((flash_final_pct / 100) * FMC_FLASH_TOTAL_BYTES),
            "flash_total_bytes": FMC_FLASH_TOTAL_BYTES,
            "modo_execucao": "semana",
            "intervalo_coleta_s": 300,
            "sessao_origem": sessao_origem,
            "fase": fase,
            "cpu_pct_host": cpu_pct_host,
            "cpu_semantica": "percentual_host_sintetico",
            "ram_baseline_fonte": "SEM_SUBTRACAO",
            "ram_medicao_tipo": "SINTETICA_PERCENTUAL_RAM_HOST",
            "io_leitura_contador_acum": io_leitura_acum,
            "io_escrita_contador_acum": io_escrita_acum,
            "io_leitura_unidade": "bytes",
            "io_escrita_unidade": "bytes",
        }
        linhas_do_csv.append(pacote)
    # Toda a semana cabe em um Bronze único; cada linha representa uma amostra de 5 minutos.
    sessao_semana = f"SEMANA-{data_inicio.isoformat()}-{INICIO_EXECUCAO}"
    caminho_semana = os.getenv("CAMINHO_SEMANA", f"bronze_semana_{INICIO_EXECUCAO}.csv")
    pd.DataFrame(linhas_do_csv, columns=COLUNAS).to_csv(
        caminho_semana, sep=";", index=False, encoding="utf-8"
    )
    registrar_bronze_ativo(
        caminho_semana, "semana", data_inicio, hostname="GERADOR_SINTETICO",
        sessao_id=sessao_semana, estado_coleta="EM_ANDAMENTO",
    )
    print(f"Arquivo {caminho_semana} gerado com {len(linhas_do_csv)} amostras.")
    if enviar_s3:
        s3_client = obter_cliente_s3()
        nome_arquivo = os.path.basename(caminho_semana)
        enviado = enviar_lote_nuvem(
            s3_client=s3_client,
            caminho_csv=caminho_semana,
            hostname="GERADOR_SINTETICO",
            nome_arquivo=nome_arquivo,
            sessao_id=sessao_semana,
            modo="semana",
            data_inicio=data_inicio,
            estado_coleta="CONCLUIDA",
        )
        if not enviado:
            raise RuntimeError("Bronze semanal não foi enviado ao S3; verifique as permissões e as credenciais.")
    else:
        print(
            "Envio ao S3 desabilitado. "
            "Execute com --enviar-s3 para habilitar."
        )
def executar_automaticamente(data_inicio=None, enviar_s3=False):
    # Usa coleta real se encontrar a bancada; sem ela, gera o Bronze semanal.
    modo, simulador = detectar_modo_automatico()
    if modo == "real":
        print("Bancada encontrada. O modo automático será COLETA REAL.")
        rodar_modo_real(simulador)
        return "real"
    if data_inicio is None:
        # Usa os últimos sete dias completos para não gerar timestamps no futuro.
        data_inicio = date.today() - timedelta(days=7)
    print("Bancada não encontrada. O modo automático será SEMANA SINTÉTICA.")
    rodar_modo_semana(data_inicio, enviar_s3)
    return "semana"
if __name__ == "__main__":
    import argparse
    # o próprio agente decide qual fonte usar.
    terminal = argparse.ArgumentParser(description="Agente AirPulse com seleção automática de fonte")
    terminal.add_argument("--inicio", default=None, help="Data inicial opcional do semanal (AAAA-MM-DD).")
    terminal.add_argument("--enviar-s3", action="store_true", help="Envia também o Bronze semanal ao S3.")
    comandos = terminal.parse_args()
    enviar_s3_env = os.getenv("ENVIAR_S3", "false").strip().lower() in ("1", "true", "yes", "sim")
    data_inicio = date.fromisoformat(comandos.inicio) if comandos.inicio else None
    executar_automaticamente(data_inicio, comandos.enviar_s3 or enviar_s3_env)
