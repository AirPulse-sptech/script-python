"""
AirPulse - Agente híbrido (camada Bronze)

Dois modos, mesmo esquema de saída (COLUNAS):
  --modo real    coleta ao vivo, a cada 10 s, do processo da bancada de voo (psutil).
                 O agente roda NA PLACA: 1 agente por placa (PLACA_ID = 1 ou 2 dentro do FMC)
  --modo semana  gera 7 dias simulados (1 aeronave, 2 FMCs, 2 placas por FMC, leitura a cada 10 s)

Exemplos:
  python agente.py                                  # coleta real (placa definida no .env)
  python agente.py --modo semana --enviar-s3        # semana simulada + upload
"""
import argparse
import json
import os
import random
import socket
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from datetime import time as hora

import pandas as pd
import psutil
from dotenv import load_dotenv

# ==========================================
# CONFIGURAÇÕES E TENANT (Identidade)
# ==========================================
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

EMPRESA_ID = os.getenv("EMPRESA_ID", "EMP-01")
AERONAVE_ID = os.getenv("AERONAVE_ID", "PR-XYZ")                    # modo real
AERONAVE_ID_SEMANA = os.getenv("AERONAVE_ID_SEMANA", "SIM-AIR-001")  # modo semana (não colide com o real)
FMC_ID = os.getenv("FMC_ID", "FMC-01")                  # modo real
FMC_TIPO = os.getenv("FMC_TIPO", "principal").lower()   # modo real: principal | secundario
PLACAS_POR_FMC = 2
PLACA_ID = int(os.getenv("PLACA_ID", "1"))              # modo real: placa onde este agente roda (1..2)
CPU_NUCLEOS = int(os.getenv("CPU_NUCLEOS", psutil.cpu_count() or 1))  # núcleos da placa (normaliza a CPU no silver)
INTERVALO_COLETA_S = int(os.getenv("INTERVALO_COLETA_S", "10"))      # os dois modos usam o mesmo passo
HOSTNAME = socket.gethostname()

# Premissas físicas: 4 MB SRAM e 32 MB flash = placa de CPU do UNS-1Lw.
# No modo semana o mesmo valor é aplicado às placas (simplificação da PoC):
# ajuste CAPACIDADE_PLACA se quiser capacidades diferentes por placa.
FMC_RAM_TOTAL_BYTES = 4 * 1024 * 1024
FMC_FLASH_TOTAL_BYTES = 32 * 1024 * 1024
CAPACIDADE_PLACA = {n: (FMC_RAM_TOTAL_BYTES, FMC_FLASH_TOTAL_BYTES) for n in range(1, PLACAS_POR_FMC + 1)}

ORIGEM_REAL = "SIMULADA_BANCADA"    # processo da bancada medido com psutil
ORIGEM_SEMANA = "SIMULADA_GERADOR"  # série sintética, nada foi medido

COLUNAS = [
    "timestamp_utc", "seq", "empresa_id", "aeronave_id", "fmc_id", "fmc_tipo", "placa_id",
    "hostname", "origem", "uptime_proc_s", "cpu_percent", "cpu_nucleos", "qtd_tarefas",
    "ram_usada_bytes", "ram_baseline_bytes", "ram_total_bytes",
    "flash_usada_bytes", "flash_total_bytes",
    "io_leitura_bytes", "io_escrita_bytes", "temperatura_c",
]

def temperatura_simulada(cpu_pct):
    """Mesma fórmula nos dois modos (nenhum sensor real): 35 °C + 0,15 por ponto de CPU."""
    return 35.0 + cpu_pct * 0.15


# ==========================================
# CLOUD (import tardio: o agente roda sem boto3 se não houver bucket)
# ==========================================
def criar_cliente_s3():
    if not (BUCKET_NAME and AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        return None
    try:
        import boto3
        return boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            region_name=AWS_REGION)
    except Exception as e:
        print(f"Erro S3: {e}")
        return None


def enviar_para_s3(s3, caminho, chave):
    if s3 is None:
        print(" S3 não configurado (S3_BUCKET_NAME / credenciais no .env): envio ignorado.")
        return
    if not os.path.exists(caminho):
        return
    try:
        s3.upload_file(caminho, BUCKET_NAME, chave)
        print(f" Enviado -> s3://{BUCKET_NAME}/{chave}")
    except Exception as e:
        print(f" Erro S3: {e}")


def garantir_csv_compativel(caminho):
    """Se o cabeçalho mudou, arquiva o CSV antigo em vez de misturar esquemas."""
    if not os.path.exists(caminho):
        return
    with open(caminho, encoding="utf-8") as f:
        cabecalho = f.readline().strip().split(";")
    if cabecalho != COLUNAS:
        backup = f"{caminho}.{int(time.time())}.bak"
        os.rename(caminho, backup)
        print(f"Esquema mudou: CSV anterior arquivado em {backup}")


# ==========================================
# MODO REAL: coleta da bancada de voo
# ==========================================
CAMINHO_CSV = "bronze_bruto.csv"                       # local à placa
AMOSTRAS_POR_LOTE = max(1, 60 // INTERVALO_COLETA_S)   # envio ao S3 a cada ~60 s
NOME_PROCESSO_ALVO = "bancada_voo"
ARQUIVO_FLASH = "fmc_flash_bite.log"      # flash simulada, criada pela bancada

_cache_filhos = {}   # cpu_percent é relativo à chamada anterior NO MESMO objeto


def calibrar_baseline_ram():
    """RSS de um interpretador ocioso (o RSS bruto inclui o Python). Override: RAM_BASELINE_BYTES."""
    manual = os.getenv("RAM_BASELINE_BYTES")
    if manual:
        return int(manual)
    try:
        p = subprocess.Popen([sys.executable, "-c",
                              "import time, os, multiprocessing, datetime; time.sleep(10)"])
        time.sleep(1.0)
        rss = psutil.Process(p.pid).memory_info().rss
        p.kill()
        p.wait()
        return rss
    except Exception as e:
        print(f"Aviso: baseline de RAM não calibrado ({e}); gravando 0.")
        return 0


def encontrar_simulador():
    """Processo raiz da bancada: algum argumento é exatamente bancada_voo(.py).
    Compara o nome do arquivo (não texto solto da linha de comando) e ignora os
    filhos do multiprocessing, que herdam a mesma linha de comando."""
    alvos = (NOME_PROCESSO_ALVO, NOME_PROCESSO_ALVO + ".py")
    candidatos = {}
    for proc in psutil.process_iter(["pid", "ppid", "cmdline"]):
        try:
            cmd = proc.info.get("cmdline") or []
            if proc.pid != os.getpid() and any(os.path.basename(a).lower() in alvos for a in cmd[1:]):
                candidatos[proc.pid] = proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    for pid, proc in candidatos.items():
        if proc.info["ppid"] not in candidatos:
            return proc
    return None


def medir_arvore(proc):
    """CPU e threads do processo + filhos (a carga do FMC roda em mp.Process filho)."""
    cpu = proc.cpu_percent(interval=None)
    tarefas = proc.num_threads()
    vivos = set()
    try:
        filhos = proc.children(recursive=True)
    except psutil.NoSuchProcess:
        filhos = []
    for f in filhos:
        try:
            p = _cache_filhos.setdefault(f.pid, f)
            cpu += p.cpu_percent(interval=None)
            tarefas += p.num_threads()
            vivos.add(f.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            _cache_filhos.pop(f.pid, None)
    for pid in list(_cache_filhos):
        if pid not in vivos:
            del _cache_filhos[pid]
    return cpu, tarefas


def tamanho_flash(proc):
    try:
        return os.path.getsize(os.path.join(proc.cwd(), ARQUIVO_FLASH))
    except (OSError, psutil.Error):
        return None


def coletar_amostra(seq, proc, baseline_ram):
    timestamp_utc = datetime.now(timezone.utc).isoformat()
    try:
        if not proc.is_running():
            raise psutil.NoSuchProcess(proc.pid)
        uptime_proc_s = int(time.time() - proc.create_time())
        cpu_percent, qtd_tarefas = medir_arvore(proc)
        ram_usada_bytes = proc.memory_info().rss
        try:
            io = proc.io_counters()
            io_leitura, io_escrita = io.read_bytes, io.write_bytes
        except (AttributeError, psutil.AccessDenied):
            io_leitura = io_escrita = None
        temperatura_c = temperatura_simulada(cpu_percent / CPU_NUCLEOS)  # SIMULADA a partir da carga
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None

    return {
        "timestamp_utc": timestamp_utc, "seq": seq,
        "empresa_id": EMPRESA_ID, "aeronave_id": AERONAVE_ID,
        "fmc_id": FMC_ID, "fmc_tipo": FMC_TIPO, "placa_id": PLACA_ID,
        "hostname": HOSTNAME, "origem": ORIGEM_REAL,
        "uptime_proc_s": uptime_proc_s, "cpu_percent": cpu_percent,
        "cpu_nucleos": CPU_NUCLEOS, "qtd_tarefas": qtd_tarefas,
        "ram_usada_bytes": ram_usada_bytes, "ram_baseline_bytes": baseline_ram,
        "ram_total_bytes": FMC_RAM_TOTAL_BYTES,
        "flash_usada_bytes": tamanho_flash(proc), "flash_total_bytes": FMC_FLASH_TOTAL_BYTES,
        "io_leitura_bytes": io_leitura, "io_escrita_bytes": io_escrita,
        "temperatura_c": temperatura_c,
    }


def salvar_csv_local(amostra):
    pd.DataFrame([amostra], columns=COLUNAS).to_csv(
        CAMINHO_CSV, sep=";", mode="a", index=False,
        header=not os.path.exists(CAMINHO_CSV), encoding="utf-8")
    print(f"[{amostra['timestamp_utc']}] Amostra #{amostra['seq']} | "
          f"CPU {amostra['cpu_percent']:.1f}% | tarefas {amostra['qtd_tarefas']}")


def rodar_modo_real():
    if PLACA_ID not in range(1, PLACAS_POR_FMC + 1):
        sys.exit(f"PLACA_ID={PLACA_ID} inválido: cada FMC tem {PLACAS_POR_FMC} placas (1..{PLACAS_POR_FMC}).")
    if FMC_TIPO not in ("principal", "secundario"):
        sys.exit(f"FMC_TIPO={FMC_TIPO!r} inválido: use principal ou secundario.")
    print(f"Iniciando Agente AirPulse (modo real) em {FMC_ID}/{FMC_TIPO} placa {PLACA_ID}... aguardando a bancada de voo.")
    s3 = criar_cliente_s3()
    chave_s3 = f"bronze/empresa={EMPRESA_ID}/aeronave={AERONAVE_ID}/{FMC_ID}/placa={PLACA_ID}/bronze_bruto.csv"
    garantir_csv_compativel(CAMINHO_CSV)
    baseline_ram = calibrar_baseline_ram()
    print(f"Baseline de RAM do interpretador: {baseline_ram / 1024**2:.1f} MB")

    proc_simulador, seq = None, 0
    proximo = time.monotonic() + INTERVALO_COLETA_S

    while True:
        # 1. (Re)adquire o alvo e zera o pacing para não recuperar atraso em rajada
        if proc_simulador is None or not proc_simulador.is_running():
            proc_simulador = encontrar_simulador()
            if proc_simulador is None:
                time.sleep(1)
                continue
            print(f"Simulador encontrado (PID {proc_simulador.pid})")
            _cache_filhos.clear()
            medir_arvore(proc_simulador)  # aquecimento dos contadores de CPU
            proximo = time.monotonic() + INTERVALO_COLETA_S

        # 2. Espera o tick ANTES de medir: a janela de CPU é sempre ~5 s
        time.sleep(max(0, proximo - time.monotonic()))
        proximo += INTERVALO_COLETA_S

        # 3. seq só avança em leitura bem-sucedida
        amostra = coletar_amostra(seq + 1, proc_simulador, baseline_ram)
        if amostra is None:
            continue
        seq += 1
        salvar_csv_local(amostra)

        # 4. Envio em lote (~60 s)
        if seq % AMOSTRAS_POR_LOTE == 0:
            enviar_para_s3(s3, CAMINHO_CSV, chave_s3)


# ==========================================
# MODO SEMANA: gerador de 7 dias, 1 leitura a cada INTERVALO_COLETA_S por placa
# ==========================================
FUSO = timezone(timedelta(hours=-3))  # Brasília no período de teste
DIAS = ["segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo"]
VOOS_POR_DIA = [6, 6, 6, 6, 7, 5, 6]
PRIMEIRA_PARTIDA = [360, 370, 380, 360, 360, 420, 390]   # minutos desde 00:00 local
ULTIMA_CHEGADA = [1260, 1260, 1260, 1260, 1350, 1230, 1290]
FATOR_PLACA = [1.0, .93]                                 # CPU de P1..P2


def montar_cadastro():
    """1 aeronave, 2 FMCs (principal/secundário), 2 placas cada. Recorte da PoC.
    placa_id é o número da placa DENTRO do FMC (1..2), igual ao PLACA_ID do modo real."""
    return [{"fmc_id": f"SIM_FMC_{f:02d}", "tipo": tipo, "placa_id": n, "numero": n}
            for f, tipo in enumerate(["principal", "secundario"], start=1)
            for n in range(1, PLACAS_POR_FMC + 1)]


def montar_agenda(inicio, rng):
    """Verdade artificial do gerador (gabarito); NÃO vai no bronze."""
    voos = []
    for numero_dia in range(7):
        dia = inicio + timedelta(days=numero_dia)
        semana = dia.weekday()
        qtd = VOOS_POR_DIA[semana]
        duracoes = [rng.choice([95, 100, 105, 110, 115]) for _ in range(qtd)]
        solo = ULTIMA_CHEGADA[semana] - PRIMEIRA_PARTIDA[semana] - sum(duracoes)
        pesos = [rng.uniform(.85, 1.15) for _ in range(qtd - 1)]
        intervalos = [round(solo * p / sum(pesos) / 5) * 5 for p in pesos]
        intervalos[-1] += solo - sum(intervalos)
        partida = datetime.combine(dia, hora(), tzinfo=FUSO) + timedelta(minutes=PRIMEIRA_PARTIDA[semana])
        for numero, duracao in enumerate(duracoes):
            chegada = partida + timedelta(minutes=duracao)
            voos.append({"voo_simulado_id": f"SIM-{numero_dia + 1:02d}-{numero + 1:02d}",
                         "dia_semana": DIAS[semana],
                         "partida_local": partida.isoformat(), "chegada_local": chegada.isoformat(),
                         "duracao_min": duracao})
            if numero < qtd - 1:
                partida = chegada + timedelta(minutes=intervalos[numero])
    return pd.DataFrame(voos)


def estado_simulado(instante, agenda):
    """(CPU base %, em_operacao): 72 nos 15 min das pontas do voo, 38 no meio, 12 em solo."""
    for partida, chegada in agenda:
        if partida <= instante < chegada:
            minutos = (instante - partida).total_seconds() / 60
            restante = (chegada - instante).total_seconds() / 60
            return (72 if minutos < 15 or restante <= 15 else 38), True
    return 12, False


def gerar_semana(inicio, voos, semente, incidentes):
    """Devolve linhas já no esquema bronze (bytes, sem converter depois)."""
    rng = random.Random(semente + 1)
    agenda = [(datetime.fromisoformat(v.partida_local), datetime.fromisoformat(v.chegada_local))
              for v in voos.itertuples()]
    primeiro = datetime.combine(inicio, hora(), tzinfo=FUSO)
    placas = montar_cadastro()
    ruido_cpu = {(p["fmc_id"], p["placa_id"]): 0 for p in placas}
    ruido_ram = {(p["fmc_id"], p["placa_id"]): 0 for p in placas}
    linhas = []

    # Os parâmetros do ruído foram calibrados para passos de 5 min; reescala para o
    # intervalo atual mantendo a mesma autocorrelação no tempo e o mesmo desvio.
    passo = INTERVALO_COLETA_S / 300
    phi_cpu, phi_ram = .55 ** passo, .8 ** passo
    sig_cpu = 2 * ((1 - phi_cpu ** 2) / (1 - .55 ** 2)) ** .5
    sig_ram = .4 * ((1 - phi_ram ** 2) / (1 - .8 ** 2)) ** .5

    for amostra in range(7 * 86400 // INTERVALO_COLETA_S):
        instante = primeiro + timedelta(seconds=INTERVALO_COLETA_S * amostra)
        dia = amostra * INTERVALO_COLETA_S // 86400
        minuto_dia = instante.hour * 60 + instante.minute
        base_cpu, em_operacao = estado_simulado(instante, agenda)

        for p in placas:
            pid, n = p["placa_id"], p["numero"]
            chave = (p["fmc_id"], pid)
            ruido_cpu[chave] = phi_cpu * ruido_cpu[chave] + rng.gauss(0, sig_cpu)
            cpu = base_cpu * FATOR_PLACA[n - 1] + ruido_cpu[chave]
            ruido_ram[chave] = phi_ram * ruido_ram[chave] + rng.gauss(0, sig_ram)
            ram = 46 + n * 2 + (6 if em_operacao else 0) + ruido_ram[chave]
            flash = 49 + n * 3 + .20 * amostra * INTERVALO_COLETA_S / 86400   # +0,2 pp/dia, sem seguir a CPU
            tarefas = 16 + n * 2 + (4 if em_operacao else 0) + rng.randint(0, 3)   # "tarefas ativas" (mesma coluna que threads no modo real)

            if incidentes:
                sec = p["tipo"] == "secundario"
                if not sec and n == 2 and dia == 2 and 1090 <= minuto_dia < 1130:
                    cpu = rng.uniform(88, 94)
                if sec and n == 1 and dia == 4 and 780 <= minuto_dia < 1140:
                    ram = min(96, 72 + (minuto_dia - 780) / 15)
                if sec and n == 2 and dia == 5 and 900 <= minuto_dia < 1320:
                    flash = 92 + (minuto_dia - 900) / 420

            ram_total, flash_total = CAPACIDADE_PLACA[n]
            ram_livre = round(ram_total * (1 - min(100, max(0, ram)) / 100))
            flash_livre = round(flash_total * (1 - flash / 100))
            cpu = min(100, max(0, cpu))
            linhas.append({
                "timestamp_utc": instante.astimezone(timezone.utc).isoformat(),
                "seq": amostra + 1, "empresa_id": EMPRESA_ID, "aeronave_id": AERONAVE_ID_SEMANA,
                "fmc_id": p["fmc_id"], "fmc_tipo": p["tipo"], "placa_id": pid,
                "hostname": "gerador_semana", "origem": ORIGEM_SEMANA,
                "uptime_proc_s": None,                        # sem uptime: sem sessões/fase
                "cpu_percent": round(cpu, 1), "cpu_nucleos": 1, "qtd_tarefas": tarefas,
                "ram_usada_bytes": ram_total - ram_livre,
                "ram_baseline_bytes": 0,                      # já é a RAM lógica do FMC
                "ram_total_bytes": ram_total,
                "flash_usada_bytes": flash_total - flash_livre,
                "flash_total_bytes": flash_total,
                "io_leitura_bytes": None, "io_escrita_bytes": None,
                "temperatura_c": round(temperatura_simulada(cpu), 1),
            })
    return pd.DataFrame(linhas, columns=COLUNAS)


def rodar_modo_semana(args):
    inicio = date.fromisoformat(args.inicio)
    pasta = args.saida
    os.makedirs(pasta, exist_ok=True)

    voos = montar_agenda(inicio, random.Random(args.semente))
    dados = gerar_semana(inicio, voos, args.semente, not args.sem_incidentes)

    caminho = os.path.join(pasta, "bronze_semana.csv")
    garantir_csv_compativel(caminho)
    dados.to_csv(caminho, sep=";", index=False, encoding="utf-8")
    voos.to_csv(os.path.join(pasta, "agenda_gabarito.csv"), sep=";", index=False)
    with open(caminho + ".meta.json", "w", encoding="utf-8") as f:
        json.dump({"origem": "SIMULADO", "coletado_com_psutil": False,
                   "inicio_local": inicio.isoformat(), "semente": args.semente,
                   "intervalo_s": INTERVALO_COLETA_S, "linhas": len(dados),
                   "voos_semana": len(voos), "incidentes_ativados": not args.sem_incidentes,
                   "nota": "agenda_gabarito.csv é a verdade artificial; não usar para deduzir fases reais"},
                  f, ensure_ascii=False, indent=2)

    print(f"{caminho}: {len(dados)} linhas | {dados['placa_id'].nunique()} placas | {len(voos)} voos")
    if args.enviar_s3:
        enviar_para_s3(criar_cliente_s3(), caminho,
                       f"bronze/empresa={EMPRESA_ID}/aeronave={AERONAVE_ID_SEMANA}/semana/bronze_semana.csv")


# ==========================================
# CLI
# ==========================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--modo", choices=["real", "semana"], default="real")
    ap.add_argument("--inicio", default="2026-10-05", help="semana: primeiro dia local, AAAA-MM-DD")
    ap.add_argument("--semente", type=int, default=42)
    ap.add_argument("--saida", default="dados_semana", help="semana: pasta de saída")
    ap.add_argument("--sem-incidentes", action="store_true")
    ap.add_argument("--enviar-s3", action="store_true", help="semana: envia o bronze ao bucket")
    args = ap.parse_args()

    rodar_modo_real() if args.modo == "real" else rodar_modo_semana(args)
