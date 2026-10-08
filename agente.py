import os
import sys
import time
import socket
import subprocess
from datetime import datetime, timezone
import psutil
import pandas as pd
from dotenv import load_dotenv

# ==========================================
# 1. CONFIGURAÇÕES E IDENTIDADE
# ==========================================
load_dotenv()

# Credenciais e Roteamento AWS S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Identidade do Equipamento
EMPRESA_ID = os.getenv("EMPRESA_ID", "EMP-01")
AERONAVE_ID = os.getenv("AERONAVE_ID", "PR-XYZ")
FMC_ID = os.getenv("FMC_ID", "FMC-01")
FMC_TIPO = os.getenv("FMC_TIPO", "principal").lower()
PLACA_ID = int(os.getenv("PLACA_ID", "1"))
HOSTNAME = socket.gethostname()

# Parâmetros Físicos e de Coleta[cite: 10]
CAMINHO_CSV = "bronze_bruto.csv"
INTERVALO_COLETA_S = int(os.getenv("INTERVALO_COLETA_S", "10"))
AMOSTRAS_POR_LOTE = max(1, 60 // INTERVALO_COLETA_S)
FMC_RAM_TOTAL_BYTES = 4 * 1024 * 1024
FMC_FLASH_TOTAL_BYTES = 32 * 1024 * 1024

NOME_PROCESSO_ALVO = "bancada_voo"
ARQUIVO_FLASH = "fmc_flash_bite.log"

# ==========================================
# 2. CONEXÃO S3
# ==========================================
def obter_cliente_s3():
    """Inicializa o cliente S3 se as credenciais existirem no .env."""
    if BUCKET_NAME and AWS_ACCESS_KEY_ID:
        import boto3
        return boto3.client(
            "s3", 
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    return None

def enviar_lote_nuvem(s3_client):
    """Envia o CSV acumulado para o Data Lake no S3."""
    if s3_client and os.path.exists(CAMINHO_CSV):
        chave_s3 = f"bronze/empresa={EMPRESA_ID}/aeronave={AERONAVE_ID}/{FMC_ID}/placa={PLACA_ID}/bronze_bruto.csv"
        try:
            s3_client.upload_file(CAMINHO_CSV, BUCKET_NAME, chave_s3)
            print(f"Lote enviado ao S3 -> s3://{BUCKET_NAME}/{chave_s3}")
        except Exception as e:
            print(f"Erro ao enviar para S3: {e}")

# ==========================================
# 3. FUNÇÕES AUXILIARES (Sensores e Busca)
# ==========================================
def calibrar_baseline_ram():
    """Mede a RAM base gasta apenas pelo Python, para depois isolar a RAM do simulador[cite: 10]."""
    if os.getenv("RAM_BASELINE_BYTES"):
        return int(os.getenv("RAM_BASELINE_BYTES"))
    try:
        p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(5)"])
        time.sleep(1)
        rss_base = psutil.Process(p.pid).memory_info().rss
        p.kill()
        return rss_base
    except Exception:
        return 0

def encontrar_processo_simulador():
    """Varre os processos procurando a bancada de voo[cite: 10]."""
    for proc in psutil.process_iter(["pid", "ppid", "cmdline"]):
        cmd = proc.info.get("cmdline")
        if cmd and any(NOME_PROCESSO_ALVO in c.lower() for c in cmd) and proc.pid != os.getpid():
            return proc
    return None

def medir_cpu_e_tarefas(proc, cache_filhos):
    """Mede a CPU e as threads do processo pai e de seus filhos[cite: 10]."""
    cpu_total = proc.cpu_percent(interval=None)
    tarefas_total = proc.num_threads()
    try:
        for filho in proc.children(recursive=True):
            p = cache_filhos.setdefault(filho.pid, filho)
            cpu_total += p.cpu_percent(interval=None)
            tarefas_total += p.num_threads()
    except psutil.NoSuchProcess:
        pass
    return cpu_total, tarefas_total

# ==========================================
# 4. EXTRAÇÃO E CARGA (ETL Bronze)
# ==========================================
def coletar_amostra(seq, proc, baseline_ram, cache_filhos):
    """Extrai os dados brutos e formata no padrão da Camada Bronze[cite: 10]."""
    try:
        cpu_pct, qtd_tarefas = medir_cpu_e_tarefas(proc, cache_filhos)
        io = proc.io_counters()
        flash_size = os.path.getsize(os.path.join(proc.cwd(), ARQUIVO_FLASH)) if os.path.exists(ARQUIVO_FLASH) else 0
        
        return {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "seq": seq,
            "empresa_id": EMPRESA_ID, "aeronave_id": AERONAVE_ID,
            "fmc_id": FMC_ID, "fmc_tipo": FMC_TIPO, "placa_id": PLACA_ID,
            "hostname": HOSTNAME, "origem": "SIMULADA_BANCADA",
            "uptime_proc_s": int(time.time() - proc.create_time()),
            "cpu_percent": cpu_pct,
            "cpu_nucleos": psutil.cpu_count() or 1,
            "qtd_tarefas": qtd_tarefas,
            "ram_usada_bytes": proc.memory_info().rss,
            "ram_baseline_bytes": baseline_ram,
            "ram_total_bytes": FMC_RAM_TOTAL_BYTES,
            "flash_usada_bytes": flash_size,
            "flash_total_bytes": FMC_FLASH_TOTAL_BYTES,
            "io_leitura_bytes": io.read_bytes if io else None,
            "io_escrita_bytes": io.write_bytes if io else None,
            "temperatura_c": 35.0 + (cpu_pct * 0.15)
        }
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None

def salvar_localmente(amostra):
    """Salva a amostra no CSV local (Buffer)."""
    arquivo_novo = not os.path.exists(CAMINHO_CSV)
    pd.DataFrame([amostra]).to_csv(CAMINHO_CSV, sep=";", mode="a", index=False, header=arquivo_novo)
    print(f"[{amostra['timestamp_utc']}] Amostra #{amostra['seq']} | CPU: {amostra['cpu_percent']:.1f}%")

# ==========================================
# 5. LOOP PRINCIPAL
# ==========================================
if __name__ == "__main__":
    print(f"Iniciando Agente AirPulse em {FMC_ID} (Placa {PLACA_ID})... aguardando bancada.")
    s3_client = obter_cliente_s3()
    baseline_ram = calibrar_baseline_ram()
    
    simulador, seq = None, 0
    cache_filhos = {}
    proximo_ciclo = time.monotonic() + INTERVALO_COLETA_S

    while True:
        # 1. Recupera o alvo se não existir ou tiver morrido[cite: 10]
        if not simulador or not simulador.is_running():
            simulador = encontrar_processo_simulador()
            if simulador:
                cache_filhos.clear()
                medir_cpu_e_tarefas(simulador, cache_filhos) # Descarte/Aquecimento da CPU
                proximo_ciclo = time.monotonic() + INTERVALO_COLETA_S
            else:
                time.sleep(1)
                continue

        # 2. Pacing (Pausa exata)
        time.sleep(max(0, proximo_ciclo - time.monotonic()))
        proximo_ciclo += INTERVALO_COLETA_S

        # 3. Coleta e salva
        amostra = coletar_amostra(seq + 1, simulador, baseline_ram, cache_filhos)
        if amostra:
            seq += 1
            salvar_localmente(amostra)

            # 4. Batching: Envia para a nuvem a cada N amostras[cite: 10]
            if seq % AMOSTRAS_POR_LOTE == 0:
                enviar_lote_nuvem(s3_client)