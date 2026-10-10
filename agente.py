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
EMPRESA_ID = os.getenv("EMPRESA_ID", "EMP-01")
AERONAVE_ID = os.getenv("AERONAVE_ID", "PR-XYZ")
FMC_ID = os.getenv("FMC_ID", "FMC-01")
FMC_TIPO = os.getenv("FMC_TIPO", "principal").lower()
PLACA_ID = int(os.getenv("PLACA_ID", "1"))
HOSTNAME = socket.gethostname()

CAMINHO_CSV = "bronze_bruto.csv"
INTERVALO_COLETA_S = max(1, int(os.getenv("INTERVALO_COLETA_S", "10")))
AMOSTRAS_POR_LOTE = max(1, 60 // INTERVALO_COLETA_S)
RAM_TOTAL_REFERENCIA_BYTES = int(os.getenv("RAM_TOTAL_BYTES", psutil.virtual_memory().total))
FMC_FLASH_TOTAL_BYTES = 32 * 1024 * 1024
NOME_PROCESSO_ALVO = os.getenv("NOME_PROCESSO_ALVO", "bancada_voo")
ARQUIVO_FLASH = "fmc_flash_bite.log"
ARQUIVO_META = "bancada_meta.json"

# contador e unidade são os campos canônicos para que caracteres não sejam chamados de bytes.
COLUNAS = [
    "timestamp_utc", "seq", "empresa_id", "aeronave_id", "fmc_id", "fmc_tipo", "placa_id",
    "hostname", "origem", "uptime_proc_s", "fator_tempo", "cpu_percent", "cpu_nucleos",
    "qtd_tarefas", "ram_usada_bytes", "ram_baseline_bytes", "ram_total_bytes",
    "ram_disponivel_bytes", "flash_usada_bytes", "flash_total_bytes",
    "io_leitura_bytes_acum", "io_escrita_bytes_acum",
    "temperatura_c", "modo_execucao", "intervalo_coleta_s", "sessao_origem", "fase",
    "cpu_pct_host", "cpu_semantica", "ram_baseline_fonte", "ram_medicao_tipo",
    "temperatura_tipo", "io_leitura_contador_acum", "io_escrita_contador_acum",
    "io_leitura_unidade", "io_escrita_unidade",
]


def estimar_temperatura_c(cpu_pct_host: float) -> float:
    """Estimativa sintética comum aos modos real e semanal; não é temperatura física."""
    return round(35.0 + (float(cpu_pct_host) * 0.5), 1)


def obter_cliente_s3():
    if BUCKET_NAME and AWS_ACCESS_KEY_ID:
        import boto3
        return boto3.client(
            "s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION,
        )
    return None


def enviar_lote_nuvem(s3_client, caminho_arquivo, aeronave_destino):
    if s3_client and os.path.exists(caminho_arquivo):
        chave_s3 = (
            f"bronze/empresa={EMPRESA_ID}/aeronave={aeronave_destino}/"
            f"{FMC_ID}/placa={PLACA_ID}/{os.path.basename(caminho_arquivo)}"
        )
        try:
            s3_client.upload_file(caminho_arquivo, BUCKET_NAME, chave_s3)
            print(f"Arquivo enviado ao S3 -> {chave_s3}")
        except Exception as erro:
            print(f" Erro no S3: {erro}")


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
    """Não subtrai o RSS de outro processo; retorna zero para usar RSS observado."""
    return 0


def _normalizar_nome_processo(valor):
    "Normaliza nome de script independentemente do SO e de separadores."
    nome = str(valor or "").strip().strip('"').strip("'")
    # Trata caminhos Windows e POSIX mesmo quando o agente roda noutro SO.
    nome = nome.replace("\\", "/").rsplit("/", 1)[-1].casefold()
    if nome.endswith(".py"):
        nome = nome[:-3]
    return "".join(caractere for caractere in nome if caractere.isalnum())


def encontrar_processo_simulador():
    """Localiza a bancada por nome normalizado e permite configurar o nome alvo.

    Por compatibilidade, reconhece o nome padrão configurado e o nome real
    usado neste repositório (bancadaVoo.py), mesmo com underscore/hífen.
    """
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
    """Lê metadados explícitos do simulador; tolera arquivos de versões antigas."""
    try:
        with open(os.path.join(proc.cwd(), ARQUIVO_META), encoding="utf-8") as arquivo:
            dados = json.load(arquivo)
        if not isinstance(dados, dict):
            return {}
        return dados
    except (OSError, ValueError, TypeError, psutil.Error):
        return {}


def ler_fator_tempo(proc):
    """Mantém a interface usada em versões antigas do agente."""
    metadados = ler_metadados_bancada(proc)
    try:
        fator = float(metadados.get("acelerador", os.getenv("FATOR_TEMPO", "1")))
        return fator if fator > 0 else 1.0
    except (TypeError, ValueError):
        return 1.0


def medir_cpu_e_tarefas(proc, cache_filhos):
    """Soma CPU percentual por processo (escala psutil, por núcleo) e threads."""
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
    """Prioriza contadores de bytes; fallback para caracteres fica identificado."""
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


def _amostra_io(contador, unidade):
    """Preenche os nomes legados apenas quando o contador é de bytes reais."""
    bytes_legado = contador if unidade == "bytes" else None
    return contador, bytes_legado, unidade


def rodar_modo_real():
    print(f"Agente AirPulse iniciado (Placa {PLACA_ID}). Aguardando a bancada...")
    s3_client = obter_cliente_s3()
    baseline_ram = calibrar_baseline_ram()

    simulador = None
    seq = 0
    cache_filhos = {}
    proximo_ciclo = time.monotonic() + INTERVALO_COLETA_S

    while True:
        if simulador is None or not simulador.is_running():
            simulador = encontrar_processo_simulador()
            if simulador:
                metadados = ler_metadados_bancada(simulador)
                fator_tempo = ler_fator_tempo(simulador)
                print(
                    f"Bancada detectada (PID {simulador.pid}) | "
                    f"Fator de tempo: {fator_tempo:g}x"
                )
                cache_filhos.clear()
                medir_cpu_e_tarefas(simulador, cache_filhos)  # Inicializa contadores psutil.
                proximo_ciclo = time.monotonic() + INTERVALO_COLETA_S
            else:
                time.sleep(1)
                continue

        time.sleep(max(0, proximo_ciclo - time.monotonic()))
        # Evita rajadas artificiais após atraso de execução; não inventa amostras perdidas.
        agora_monotonico = time.monotonic()
        proximo_ciclo += INTERVALO_COLETA_S
        if proximo_ciclo <= agora_monotonico:
            proximo_ciclo = agora_monotonico + INTERVALO_COLETA_S

        try:
            cpu_percent_bruto, qtd_tarefas = medir_cpu_e_tarefas(simulador, cache_filhos)
            nucleos = psutil.cpu_count(logical=True) or 1
            cpu_pct_host = cpu_percent_bruto / nucleos
            metadados = ler_metadados_bancada(simulador)
            try:
                fator_tempo = float(metadados.get("acelerador", ler_fator_tempo(simulador)))
            except (TypeError, ValueError):
                fator_tempo = ler_fator_tempo(simulador)

            read_cont, read_unit = _ler_contador_io(simulador, "read")
            write_cont, write_unit = _ler_contador_io(simulador, "write")
            read_cont, read_bytes_legacy, read_unit = _amostra_io(read_cont, read_unit)
            write_cont, write_bytes_legacy, write_unit = _amostra_io(write_cont, write_unit)

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
                "io_leitura_bytes_acum": read_bytes_legacy,
                "io_escrita_bytes_acum": write_bytes_legacy,
                "temperatura_c": estimar_temperatura_c(cpu_pct_host),
                "modo_execucao": "voo",
                "intervalo_coleta_s": INTERVALO_COLETA_S,
                "sessao_origem": metadados.get("sessao_origem"),
                "fase": metadados.get("fase") or "NAO_DETERMINADA",
                "cpu_pct_host": cpu_pct_host,
                "cpu_semantica": "soma_percentual_por_nucleo",
                "ram_baseline_fonte": "SEM_SUBTRACAO",
                "ram_medicao_tipo": "RSS_PROCESSO",
                "temperatura_tipo": "ESTIMATIVA_SINTETICA_CPU",
                "io_leitura_contador_acum": read_cont,
                "io_escrita_contador_acum": write_cont,
                "io_leitura_unidade": read_unit,
                "io_escrita_unidade": write_unit,
            }

            seq += 1
            salvar_localmente(amostra)
            if seq % AMOSTRAS_POR_LOTE == 0:
                enviar_lote_nuvem(s3_client, CAMINHO_CSV, AERONAVE_ID)

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            # A amostra perdida não é sintetizada; a Silver calculará a lacuna temporal.
            simulador = None


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
    """O modo semanal não simula as dez fases detalhadas do roteiro da bancada."""
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
    print(" Modo SEMANA: gerando 7 dias em intervalos de 300 segundos...")
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
            "io_leitura_bytes_acum": io_leitura_acum,
            "io_escrita_bytes_acum": io_escrita_acum,
            "temperatura_c": estimar_temperatura_c(cpu_pct_host),
            "modo_execucao": "semana",
            "intervalo_coleta_s": 300,
            "sessao_origem": sessao_origem,
            "fase": fase,
            "cpu_pct_host": cpu_pct_host,
            "cpu_semantica": "percentual_host_sintetico",
            "ram_baseline_fonte": "SEM_SUBTRACAO",
            "ram_medicao_tipo": "SINTETICA_PERCENTUAL_RAM_HOST",
            "temperatura_tipo": "ESTIMATIVA_SINTETICA_CPU",
            "io_leitura_contador_acum": io_leitura_acum,
            "io_escrita_contador_acum": io_escrita_acum,
            "io_leitura_unidade": "bytes",
            "io_escrita_unidade": "bytes",
        }
        linhas_do_csv.append(pacote)

    caminho_semana = "bronze_semana.csv"
    pd.DataFrame(linhas_do_csv, columns=COLUNAS).to_csv(
        caminho_semana, sep=";", index=False, encoding="utf-8"
    )
    print(f"Arquivo {caminho_semana} gerado com {len(linhas_do_csv)} amostras.")
    if enviar_s3:
        enviar_lote_nuvem(obter_cliente_s3(), caminho_semana, "SIM-AIR-001")


if __name__ == "__main__":
    import argparse

    terminal = argparse.ArgumentParser(description="Agente Híbrido AirPulse")
    terminal.add_argument("--modo", choices=["real", "semana"], default="real")
    terminal.add_argument("--inicio", default="2026-10-05")
    terminal.add_argument("--enviar-s3", action="store_true")
    comandos = terminal.parse_args()

    if comandos.modo == "real":
        rodar_modo_real()
    else:
        rodar_modo_semana(date.fromisoformat(comandos.inicio), comandos.enviar_s3)
