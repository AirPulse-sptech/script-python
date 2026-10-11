# Agente AirPulse: coleta telemetria para Bronze.
#
# Os campos observados são registrados no Bronze. As métricas derivadas,
# validações e avaliações de saúde ficam para as camadas Silver e Gold.

import json
import os
import random
import socket
import time
from datetime import date, datetime, timedelta, timezone, time as hora

import pandas as pd
import psutil
from dotenv import load_dotenv

load_dotenv()

# Identidade e nuvem
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
HOSTNAME = socket.gethostname()

INICIO_EXECUCAO = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
NOME_OBJETO_S3 = f"{HOSTNAME}_{INICIO_EXECUCAO}.csv"

CAMINHO_CSV = "bronze_bruto.csv"
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
    "timestamp_utc", "seq", "empresa_id", "aeronave_id", "fmc_id", "fmc_tipo", "placa_id",
    "hostname", "origem", "uptime_proc_s", "fator_tempo", "cpu_percent", "cpu_nucleos",
    "qtd_tarefas", "ram_usada_bytes", "ram_baseline_bytes", "ram_total_bytes",
    "ram_disponivel_bytes", "flash_usada_bytes", "flash_total_bytes",
    "modo_execucao", "intervalo_coleta_s", "sessao_origem", "fase",
    "cpu_pct_host", "cpu_semantica", "ram_baseline_fonte", "ram_medicao_tipo",
    "io_leitura_contador_acum", "io_escrita_contador_acum",
    "io_leitura_unidade", "io_escrita_unidade",
]


def obter_cliente_s3():
    if BUCKET_NAME and AWS_ACCESS_KEY_ID:
        import boto3
        return boto3.client(
            "s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION,
        )
    return None


def enviar_lote_nuvem(s3_client):
    """Envia o CSV acumulado para o bucket, em bronze/."""
    if s3_client and os.path.exists(CAMINHO_CSV):
        chave_s3 = f"bronze/{NOME_OBJETO_S3}"
        try:
            s3_client.upload_file(CAMINHO_CSV, BUCKET_NAME, chave_s3)
            print(f"Lote enviado ao S3: s3://{BUCKET_NAME}/{chave_s3}")
        except Exception as e:
            print(f"Erro ao enviar para S3: {e}")


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
    nome = nome.replace("\\", "/").rsplit("/", 1)[-1].casefold()
    if nome.endswith(".py"):
        nome = nome[:-3]
    return "".join(caractere for caractere in nome if caractere.isalnum())


def encontrar_processo_simulador():
    # Localiza a bancada por nome normalizado; permite configurar o nome alvo.
    #
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


def registrar_bronze_ativo(caminho_csv, modo, data_inicio=None):
    # Guarda qual Bronze a ETL deve ler depois que o agente terminar.
    estado = {
        "arquivo_bronze": caminho_csv,
        "modo": modo,
        "atualizado_em_utc": datetime.now(timezone.utc).isoformat(),
        "data_inicio": data_inicio.isoformat() if data_inicio else None,
    }
    # Escreve primeiro num temporário para a ETL não ler um JSON pela metade.
    temporario = f"{ARQUIVO_ESTADO_BRONZE}.tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        json.dump(estado, arquivo, ensure_ascii=False, indent=2)
    os.replace(temporario, ARQUIVO_ESTADO_BRONZE)


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
    print(f"🤖 Agente AirPulse iniciado (Placa {PLACA_ID}). Coletando a bancada...")
    simulador = simulador_inicial or encontrar_processo_simulador()
    if simulador is None:
        raise RuntimeError("A bancada não foi encontrada para a coleta real.")

    # A ETL usa este arquivo para saber que deve tratar o Bronze da coleta real.
    registrar_bronze_ativo(CAMINHO_CSV, "real")
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
                "empresa_id": EMPRESA_ID,
                "aeronave_id": AERONAVE_ID,
                "fmc_id": FMC_ID,
                "fmc_tipo": FMC_TIPO,
                "placa_id": PLACA_ID,
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
                enviar_lote_nuvem(s3_client, CAMINHO_CSV, AERONAVE_ID)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # A amostra perdida não é sintetizada; a Silver calculará a lacuna temporal.
            break

    # Envia também o último lote, mesmo se a bancada terminou antes de completar um minuto.
    if seq and seq % AMOSTRAS_POR_LOTE != 0:
        enviar_lote_nuvem(s3_client, CAMINHO_CSV, AERONAVE_ID)
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
    print("📅 Modo SEMANA: gerando 7 dias em intervalos de 300 segundos...")
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
            "empresa_id": EMPRESA_ID,
            "aeronave_id": "SIM-AIR-001",
            "fmc_id": FMC_ID,
            "fmc_tipo": FMC_TIPO,
            "placa_id": PLACA_ID,
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
    caminho_semana = "bronze_semana.csv"
    pd.DataFrame(linhas_do_csv, columns=COLUNAS).to_csv(
        caminho_semana, sep=";", index=False, encoding="utf-8"
    )
    registrar_bronze_ativo(caminho_semana, "semana", data_inicio)
    print(f"✅ Arquivo {caminho_semana} gerado com {len(linhas_do_csv)} amostras.")
    if enviar_s3:
        enviar_lote_nuvem(obter_cliente_s3(), caminho_semana, "SIM-AIR-001")


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

    # Não existe mais --modo: o próprio agente decide qual fonte usar.
    terminal = argparse.ArgumentParser(description="Agente AirPulse com seleção automática de fonte")
    terminal.add_argument("--inicio", default=None, help="Data inicial opcional do semanal (AAAA-MM-DD).")
    terminal.add_argument("--enviar-s3", action="store_true", help="Envia também o Bronze semanal ao S3.")
    comandos = terminal.parse_args()
    enviar_s3_env = os.getenv("ENVIAR_S3", "false").strip().lower() in ("1", "true", "yes", "sim")
    data_inicio = date.fromisoformat(comandos.inicio) if comandos.inicio else None
    executar_automaticamente(data_inicio, comandos.enviar_s3 or enviar_s3_env)

