# simulador_mes.py
"""
Gera 30 dias de dados Bronze para 8 FMCs e envia para S3.
- Cada voo = 1 sessão = 1 arquivo CSV + 1 _concluido.json + manifesto
- Variação natural + anomalias por componente monitorado
- Formato idêntico ao agente.py (compatível com Lambda)
- Upload S3: replica comportamento do agente.py (obter_cliente_s3, enviar_lote_nuvem, registrar_bronze_ativo)
"""

import os
import re
import random
import time
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from tempfile import gettempdir
from zoneinfo import ZoneInfo

import pandas as pd
import boto3
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / "config" / ".env", override=True)

# ============================================================================
# CONFIG
# ============================================================================

DIAS = int(os.getenv("DIAS_SIMULACAO", "30"))
ACELERADOR = float(os.getenv("ACELERADOR", "100"))
INTERVALO_S = max(1, int(os.getenv("INTERVALO_COLETA_S", "2")))
SEED = int(os.getenv("SEED", "12345"))

random.seed(SEED)

# Diretório local temporário para CSVs antes do upload
PASTA_BRONZE_LOCAL = Path(os.getenv("PASTA_BRONZE_LOCAL", gettempdir())) / "airpulse_bronze"
PASTA_BRONZE_LOCAL.mkdir(parents=True, exist_ok=True)

# COLUNAS IDÊNTICAS ao agente.py
COLUNAS = [
    "timestamp_utc", "seq", "hostname", "placa_nome", "origem", "uptime_proc_s",
    "fator_tempo", "cpu_percent", "cpu_nucleos", "qtd_tarefas", "ram_usada_bytes",
    "ram_baseline_bytes", "ram_total_bytes", "ram_disponivel_bytes",
    "flash_usada_bytes", "flash_total_bytes", "modo_execucao", "intervalo_coleta_s",
    "sessao_origem", "fase", "cpu_pct_host", "cpu_semantica", "ram_baseline_fonte",
    "ram_medicao_tipo", "io_leitura_contador_acum", "io_escrita_contador_acum",
    "io_leitura_unidade", "io_escrita_unidade",
]

# Hardware simulado
RAM_TOTAL = 16 * 1024**3
FLASH_TOTAL = 32 * 1024**2
CPU_NUCLEOS = 4

# Fases (do bancadaVoo.py)
FASES = [
    {"nome": "BOOT", "min": 1, "cpu": 65, "ram": 0},
    {"nome": "PREFLIGHT", "min": 15, "cpu": 30, "ram": 13},
    {"nome": "TAXI_OUT", "min": 15, "cpu": 25, "ram": 16},
    {"nome": "TAKEOFF", "min": 3, "cpu": 50, "ram": 19},
    {"nome": "CLIMB", "min": 20, "cpu": 42, "ram": 21},
    {"nome": "CRUISE", "min": 60, "cpu": 22, "ram": 22},
    {"nome": "DESCENT", "min": 20, "cpu": 45, "ram": 24},
    {"nome": "APPROACH", "min": 10, "cpu": 58, "ram": 27},
    {"nome": "TAXI_IN", "min": 6, "cpu": 25, "ram": 25},
    {"nome": "DONE", "min": 5, "cpu": 18, "ram": 23},
]

# Ruído natural por componente
RUIDO = {"CPU": 3.0, "RAM": 2.0, "DISCO": 1.0}

# Distribuição alertas (por leitura de componente ativo)
DIST = {"normal": 0.45, "aviso": 0.40, "critico": 0.15}
DESVIO = {"aviso": 0.10, "critico": 0.06}

# Limites por componente (do BD)
LIMITES = {
    "CPU": {"atencao": 75.0, "critico": 90.0},
    "RAM": {"atencao": 80.0, "critico": 95.0},
    "DISCO": {"atencao": 80.0, "critico": 90.0},
}

# ============================================================================
# CONFIGURAÇÃO S3 (igual agente.py)
# ============================================================================

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("aws_secret_access_key")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN") or os.getenv("aws_session_token")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("BUCKET_NAME") or os.getenv("S3_BUCKET_NAME")
S3_PREFIXO = os.getenv("S3_PREFIXO_BRONZE", "01-bronze").strip("/")
S3_PREFIXO_MANIFESTOS = f"{S3_PREFIXO}/manifestos"

# ============================================================================
# DADOS BASE: 8 FMCs (do scriptBd.sql)
# ============================================================================

# placa_nome DEVE bater com o nome exato da tabela `placa` no MySQL
# (INSERT INTO placa (nome, ...) VALUES ('Placa principal FMC PR-AER MESTRE', ...))
FMCs = [
    # PR-AER (GOL) - ATIVA - todos componentes ATIVO
    {"id": 1, "nome": "Placa principal FMC PR-AER MESTRE", "hostname": "pr-aer-mestre", "tipo": "MESTRE",
     "aeronave": "PR-AER", "modelo": "Boeing 737-8", "cia": "GOL", "status": "ATIVO",
     "componentes": {"CPU": True, "RAM": True, "DISCO": True}},
    {"id": 2, "nome": "Placa principal FMC PR-AER SPARE", "hostname": "pr-aer-spare", "tipo": "SPARE",
     "aeronave": "PR-AER", "modelo": "Boeing 737-8", "cia": "GOL", "status": "ATIVO",
     "componentes": {"CPU": True, "RAM": True, "DISCO": True}},

    # PR-ANV (LATAM) - ATIVA
    {"id": 3, "nome": "Placa principal FMC PR-ANV MESTRE", "hostname": "pr-anv-mestre", "tipo": "MESTRE",
     "aeronave": "PR-ANV", "modelo": "Airbus A320neo", "cia": "LATAM", "status": "ATIVO",
     "componentes": {"CPU": True, "RAM": True, "DISCO": True}},
    {"id": 4, "nome": "Placa principal FMC PR-ANV SPARE", "hostname": "pr-anv-spare", "tipo": "SPARE",
     "aeronave": "PR-ANV", "modelo": "Airbus A320neo", "cia": "LATAM", "status": "ATIVO",
     "componentes": {"CPU": True, "RAM": True, "DISCO": True}},

    # PR-MNT (Azul) - MESTRE em MANUTENCAO, SPARE ATIVO
    {"id": 5, "nome": "Placa principal FMC PR-MNT MESTRE", "hostname": "pr-mnt-mestre", "tipo": "MESTRE",
     "aeronave": "PR-MNT", "modelo": "Embraer E195-E2", "cia": "Azul", "status": "MANUTENCAO",
     "componentes": {"CPU": True, "RAM": True, "DISCO": True}},
    {"id": 6, "nome": "Placa principal FMC PR-MNT SPARE", "hostname": "pr-mnt-spare", "tipo": "SPARE",
     "aeronave": "PR-MNT", "modelo": "Embraer E195-E2", "cia": "Azul", "status": "ATIVO",
     "componentes": {"CPU": True, "RAM": True, "DISCO": True}},

    # PR-RES (GOL) - INATIVA - componentes INATIVO no BD
    {"id": 7, "nome": "Placa principal FMC PR-RES MESTRE", "hostname": "pr-res-mestre", "tipo": "MESTRE",
     "aeronave": "PR-RES", "modelo": "Boeing 737-800", "cia": "GOL", "status": "INATIVO",
     "componentes": {"CPU": False, "RAM": False, "DISCO": False}},
    {"id": 8, "nome": "Placa principal FMC PR-RES SPARE", "hostname": "pr-res-spare", "tipo": "SPARE",
     "aeronave": "PR-RES", "modelo": "Boeing 737-800", "cia": "GOL", "status": "INATIVO",
     "componentes": {"CPU": False, "RAM": False, "DISCO": False}},
]

# ============================================================================
# HELPERS DE SESSÃO
# ============================================================================

def _sessao_origem(data_inicio):
    """Formato completo para coluna sessao_origem no CSV: VOO-20261126T143000.000-0300"""
    return data_inicio.strftime("VOO-%Y%m%dT%H%M%S.%f%z")

def _sessao_s3(data_inicio):
    """Formato limpo para pasta S3 sessao=: 20261126T143000-0300 (sem VOO-)"""
    return data_inicio.strftime("%Y%m%dT%H%M%S%z")


# ============================================================================
# FUNÇÕES S3 (replicadas do agente.py)
# ============================================================================

def obter_cliente_s3():
    """Cria cliente S3 com credenciais do .env ou perfil/IAM Role."""
    if not BUCKET_NAME:
        raise ValueError("BUCKET_NAME (ou S3_BUCKET_NAME) não está configurado no .env")
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
    sessao = boto3.session.Session(**configuracao)
    if sessao.get_credentials() is None:
        raise RuntimeError("Credenciais AWS não encontradas para enviar o Bronze ao S3.")
    return sessao.client("s3")


def _segmento_s3(valor):
    """Trata o valor como um único segmento da chave do objeto no S3."""
    segmento = re.sub(r"[^A-Za-z0-9._-]+", "_", str(valor or "desconhecido").strip())
    return segmento.strip("._-") or "desconhecido"


def _chave_manifesto_s3(hostname):
    return f"{S3_PREFIXO_MANIFESTOS}/hostname={_segmento_s3(hostname)}/bronze_ativo.json"


def registrar_bronze_ativo_s3(
    caminho_csv,
    *,
    s3_client,
    chave_s3,
    hostname,
    sessao_id,
    estado_coleta="EM_ANDAMENTO",
):
    """Publica/atualiza manifesto no S3 (igual agente.py:registrar_bronze_ativo)."""
    estado = {
        "arquivo_bronze": os.path.abspath(caminho_csv),
        "bucket": BUCKET_NAME,
        "chave_s3": chave_s3,
        "s3_uri": f"s3://{BUCKET_NAME}/{chave_s3}",
        "chave_manifesto_s3": _chave_manifesto_s3(hostname),
        "hostname": hostname,
        "sessao_id": sessao_id,
        "modo": "simulado",
        "estado_coleta": estado_coleta,
        "atualizado_em_utc": datetime.now(timezone.utc).isoformat(),
        "data_inicio": sessao_id.replace("VOO-", "").replace("BOOT-", "")[:19],
    }
    
    # Publica manifesto no S3
    chave_manifesto = _chave_manifesto_s3(hostname)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=chave_manifesto,
        Body=json.dumps(estado, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"  📋 Manifesto S3: s3://{BUCKET_NAME}/{chave_manifesto} ({estado_coleta})")
    
    # Se CONCLUIDA, cria marcador _concluido.json no S3 (dispara ETL)
    if estado_coleta == "CONCLUIDA":
        chave_concluida = f"{os.path.splitext(chave_s3)[0]}_concluido.json"
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=chave_concluida,
            Body=json.dumps({"chave_s3": chave_s3}).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"  ✅ Marcador ETL S3: s3://{BUCKET_NAME}/{chave_concluida}")
    
    return estado


def enviar_para_s3(s3_client, caminho_csv, hostname, sessao_origem, sessao_s3, estado_coleta="CONCLUIDA"):
    """Envia CSV para S3 e publica/atualiza manifesto (igual agente.py:enviar_lote_nuvem).
    
    sessao_origem: formato completo VOO-20261126T143000.000-0300 (para conteúdo do manifesto)
    sessao_s3: formato limpo 20261126T143000-0300 (para path S3 sessao=)
    """
    if not os.path.isfile(caminho_csv):
        raise FileNotFoundError(f"CSV Bronze não encontrado: {caminho_csv}")

    nome_arquivo = os.path.basename(caminho_csv)
    host_s3 = _segmento_s3(hostname)
    # sessao_s3 já está limpo (20261126T143000-0300), usa direto no path
    chave_s3 = f"{S3_PREFIXO}/hostname={host_s3}/sessao={sessao_s3}/{_segmento_s3(nome_arquivo)}"

    try:
        # Upload do CSV
        s3_client.upload_file(caminho_csv, BUCKET_NAME, chave_s3)
        print(f"  ☁️  Bronze S3: s3://{BUCKET_NAME}/{chave_s3}")
        
        # Publica/atualiza manifesto (sessao_id = VOO-... completo)
        registrar_bronze_ativo_s3(
            caminho_csv,
            s3_client=s3_client,
            chave_s3=chave_s3,
            hostname=hostname,
            sessao_id=sessao_origem,
            estado_coleta=estado_coleta,
        )
    except Exception as erro:
        raise RuntimeError(
            f"Falha ao publicar Bronze ou manifesto no S3: {erro}. "
            f"CSV: {caminho_csv}"
        ) from erro


# ============================================================================
# GERADOR DE ANOMALIAS POR COMPONENTE
# ============================================================================

class GeradorAnomalias:
    """Gera anomalias independentes por componente monitorado."""

    def __init__(self, rng, fmc):
        self.rng = rng
        self.fmc = fmc
        self.ativos = [c for c, ativo in fmc["componentes"].items() if ativo]

    def _sortear_tipo(self):
        r = self.rng.random()
        if r < DIST["normal"]:
            return "normal"
        if r < DIST["normal"] + DIST["aviso"]:
            return "aviso"
        return "critico"

    def _valor_alvo(self, componente, tipo):
        """Calcula valor alvo baseado no tipo e limites."""
        lim_at, lim_cr = LIMITES[componente]["atencao"], LIMITES[componente]["critico"]
        if tipo == "aviso":
            return self.rng.uniform(lim_at + 1, lim_cr - 1)
        elif tipo == "critico":
            return self.rng.uniform(lim_cr + 1, min(100, lim_cr + 15))
        return None

    def gerar_para_voo(self):
        """
        Gera anomalias para UM VOO (sessão).
        Retorna: {fase_nome: {componente: [anomalias]}}
        """
        anom_por_fase = {f["nome"]: {c: [] for c in self.ativos} for f in FASES}

        # Escolhe 1-3 fases longas para ter anomalias
        fases_longas = [f for f in FASES if f["min"] > 5]
        num_fases = self.rng.randint(1, min(3, len(fases_longas)))
        escolhidas = self.rng.sample(fases_longas, num_fases)

        for fase in escolhidas:
            for comp in self.ativos:
                # Cada componente tem sua própria sorteio de anomalia
                tipo = self._sortear_tipo()
                if tipo == "normal":
                    continue

                base = fase["cpu"] if comp == "CPU" else fase["ram"]
                alvo = self._valor_alvo(comp, tipo)

                anom = {
                    "tipo": tipo,
                    "componente": comp,
                    "base": base,
                    "alvo": alvo,
                    "fase": fase["nome"],
                }

                # Comportamento específico por componente
                if comp == "RAM" and self.rng.random() < 0.8:
                    # RAM: vazamento = rampa gradual
                    anom["modo"] = "rampa"
                    anom["de"] = 0.0
                    anom["ate"] = self.rng.uniform(0.3, 0.8)
                elif comp == "DISCO" and self.rng.random() < 0.6:
                    # DISCO: rajada de escrita
                    anom["modo"] = "rajada"
                    anom["kb_extra"] = self.rng.randint(20, 50)
                    anom["de"] = self.rng.uniform(0.1, 0.4)
                    anom["ate"] = self.rng.uniform(0.6, 0.9)
                else:
                    # CPU ou outros: pico com bordas suaves
                    anom["modo"] = "pico"
                    anom["de"] = self.rng.uniform(0.1, 0.3)
                    anom["ate"] = self.rng.uniform(0.7, 0.95)

                anom_por_fase[fase["nome"]][comp].append(anom)

        return anom_por_fase

    def aplicar(self, base, anoms, fase_pct, componente, flash_atual, io_write_atual):
        """
        Aplica anomalias + ruído natural ao valor base.
        Retorna: (valor_final, flash_extra, io_extra)
        """
        val = base
        flash_extra = 0
        io_extra = 0

        for a in anoms:
            if a["componente"] != componente:
                continue
            if fase_pct < a["de"] or fase_pct > a["ate"]:
                continue

            if a["modo"] == "rampa" and componente == "RAM":
                prog = (fase_pct - a["de"]) / (a["ate"] - a["de"])
                val = a["base"] + (a["alvo"] - a["base"]) * min(1.0, max(0.0, prog))

            elif a["modo"] == "rajada" and componente == "DISCO":
                flash_extra += a["kb_extra"] * 1024
                io_extra += a["kb_extra"] * 1024

            else:  # pico
                prog = (fase_pct - a["de"]) / (a["ate"] - a["de"])
                if prog < 0.1:
                    val = base + (a["alvo"] - base) * (prog / 0.1)
                elif prog > 0.9:
                    val = a["alvo"] + (base - a["alvo"]) * ((prog - 0.9) / 0.1)
                else:
                    val = a["alvo"]

        # Ruído natural gaussiano
        val += self.rng.gauss(0, RUIDO[componente])
        val = max(0.0, min(100.0, val))

        return val, flash_extra, io_extra


# ============================================================================
# SIMULAÇÃO DE 1 VOO (1 SESSÃO)
# ============================================================================

def simular_voo(fmc, anom_gen, data_inicio, writer):
    """
    Simula UM VOO completo para um FMC.
    Escreve todas as amostras no CSV (uma sessão = um seq contínuo).
    """
    anom_voo = anom_gen.gerar_para_voo()
    sessao_origem = _sessao_origem(data_inicio)   # VOO-20261126T143000.000-0300 (coluna CSV)
    sessao_s3 = _sessao_s3(data_inicio)           # 20261126T143000-0300 (pasta S3)
    seq = 0
    flash = 0
    io_read = 0
    io_write = 0
    uptime0 = time.time()

    ativos = [c for c, v in fmc["componentes"].items() if v]

    for fase in FASES:
        anoms_fase = anom_voo[fase["nome"]]
        seg_fase = fase["min"] * 60 / ACELERADOR
        t0 = time.time()

        while time.time() - t0 < seg_fase:
            seq += 1
            pct = (time.time() - t0) / seg_fase

            # Calcula cada componente ativo
            vals = {}
            for comp in ativos:
                base = fase["cpu"] if comp == "CPU" else fase["ram"]
                val, f_extra, io_extra = anom_gen.aplicar(base, anoms_fase[comp], pct, comp, flash, io_write)
                vals[comp] = val
                flash += f_extra
                io_write += io_extra

            # I/O normal
            io_read += anom_gen.rng.randint(1024, 10240)
            io_write += anom_gen.rng.randint(1024, 10240)
            flash += anom_gen.rng.randint(1024, 5120)

            # Valores para CSV (componentes inativos = None/0)
            cpu_val = vals.get("CPU", 0)
            ram_val = vals.get("RAM", 0)
            ram_bytes = int(RAM_TOTAL * ram_val / 100)

            row = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "seq": seq,
                "hostname": fmc["hostname"],
                "placa_nome": fmc["nome"],
                "origem": "SIMULADA_BANCADA",
                "uptime_proc_s": int(time.time() - uptime0),
                "fator_tempo": ACELERADOR,
                "cpu_percent": round(cpu_val, 2) if "CPU" in ativos else None,
                "cpu_nucleos": CPU_NUCLEOS if "CPU" in ativos else None,
                "qtd_tarefas": anom_gen.rng.randint(50, 200),
                "ram_usada_bytes": ram_bytes if "RAM" in ativos else None,
                "ram_baseline_bytes": 0,
                "ram_total_bytes": RAM_TOTAL if "RAM" in ativos else None,
                "ram_disponivel_bytes": RAM_TOTAL - ram_bytes if "RAM" in ativos else None,
                "flash_usada_bytes": flash if "DISCO" in ativos else None,
                "flash_total_bytes": FLASH_TOTAL if "DISCO" in ativos else None,
                "modo_execucao": "voo",
                "intervalo_coleta_s": INTERVALO_S,
                "sessao_origem": sessao_origem,
                "fase": fase["nome"],
                "cpu_pct_host": round(cpu_val / CPU_NUCLEOS, 2) if "CPU" in ativos else None,
                "cpu_semantica": "soma_percentual_por_nucleo" if "CPU" in ativos else None,
                "ram_baseline_fonte": "SEM_SUBTRACAO" if "RAM" in ativos else None,
                "ram_medicao_tipo": "RSS_PROCESSO" if "RAM" in ativos else None,
                "io_leitura_contador_acum": io_read if "DISCO" in ativos else None,
                "io_escrita_contador_acum": io_write if "DISCO" in ativos else None,
                "io_leitura_unidade": "bytes" if "DISCO" in ativos else None,
                "io_escrita_unidade": "bytes" if "DISCO" in ativos else None,
            }

            # Remove chaves None para não escrever "None" no CSV
            row = {k: v for k, v in row.items() if v is not None}
            writer.escrever(fmc, row, sessao_origem, sessao_s3)

            time.sleep(INTERVALO_S / ACELERADOR)

    # Fim do voo = finaliza sessão (cria _concluido.json local + upload S3)
    writer.finalizar_sessao(fmc, sessao_origem, sessao_s3)


# ============================================================================
# VOO PARA FMC INATIVO (só BOOT)
# ============================================================================

def simular_boot_apenas(fmc, anom_gen, data_inicio, writer):
    """FMC INATIVO: só fase BOOT (1 min), sem anomalias."""
    sessao_origem = _sessao_origem(data_inicio).replace("VOO-", "BOOT-")  # BOOT-20261126T143000.000-0300
    sessao_s3 = _sessao_s3(data_inicio)                                     # 20261126T143000-0300
    seq = 0
    flash = io_read = io_write = 0
    uptime0 = time.time()

    fase = FASES[0]  # BOOT
    seg = fase["min"] * 60 / ACELERADOR
    t0 = time.time()

    while time.time() - t0 < seg:
        seq += 1
        pct = (time.time() - t0) / seg

        # Só ruído natural, sem anomalias
        cpu = max(0, min(100, fase["cpu"] + anom_gen.rng.gauss(0, RUIDO["CPU"])))
        ram = max(0, min(100, fase["ram"] + anom_gen.rng.gauss(0, RUIDO["RAM"])))
        ram_b = int(RAM_TOTAL * ram / 100)

        io_read += anom_gen.rng.randint(1024, 10240)
        io_write += anom_gen.rng.randint(1024, 10240)
        flash += anom_gen.rng.randint(1024, 5120)

        row = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "seq": seq, "hostname": fmc["hostname"], "placa_nome": fmc["nome"],
            "origem": "SIMULADA_BANCADA", "uptime_proc_s": int(time.time() - uptime0),
            "fator_tempo": ACELERADOR, "cpu_percent": round(cpu, 2),
            "cpu_nucleos": CPU_NUCLEOS, "qtd_tarefas": anom_gen.rng.randint(50, 200),
            "ram_usada_bytes": ram_b, "ram_baseline_bytes": 0,
            "ram_total_bytes": RAM_TOTAL, "ram_disponivel_bytes": RAM_TOTAL - ram_b,
            "flash_usada_bytes": flash, "flash_total_bytes": FLASH_TOTAL,
            "modo_execucao": "voo", "intervalo_coleta_s": INTERVALO_S,
            "sessao_origem": sessao_origem, "fase": "BOOT",
            "cpu_pct_host": round(cpu / CPU_NUCLEOS, 2),
            "cpu_semantica": "soma_percentual_por_nucleo",
            "ram_baseline_fonte": "SEM_SUBTRACAO", "ram_medicao_tipo": "RSS_PROCESSO",
            "io_leitura_contador_acum": io_read, "io_escrita_contador_acum": io_write,
            "io_leitura_unidade": "bytes", "io_escrita_unidade": "bytes",
        }
        writer.escrever(fmc, row, sessao_origem, sessao_s3)
        time.sleep(INTERVALO_S / ACELERADOR)

    # Fim do boot = finaliza sessão (cria _concluido.json local + upload S3)
    writer.finalizar_sessao(fmc, sessao_origem, sessao_s3)


# ============================================================================
# WRITER CSV BRONZE + MANIFESTO LOCAL + UPLOAD S3
# ============================================================================

class BronzeWriter:
    """
    Escreve CSV Bronze local + Manifesto local (bronze_ativo.json) + Upload S3.
    Replica exatamente o comportamento do agente.py (linhas 394-442).
    """

    def __init__(self, base_dir, s3_client=None):
        self.base = Path(base_dir)
        self.handles = {}  # hostname -> (file, first_write, csv_path)
        self.sessao_atual = {}  # hostname -> sessao_origem
        self.manifestos = {}  # hostname -> manifesto_dict
        self.s3_client = s3_client

    def _get_handle(self, fmc, sessao_origem, sessao_s3):
        hn = fmc["hostname"]

        # Nova sessão = novo arquivo CSV
        if hn not in self.handles or self.sessao_atual.get(hn) != sessao_origem:
            # Fecha anterior se existir
            if hn in self.handles:
                self.handles[hn][0].close()

            pasta = self.base / hn
            pasta.mkdir(parents=True, exist_ok=True)

            # timestamp limpo para nome do arquivo CSV (sem VOO-/BOOT-)
            ts_sessao = sessao_s3.replace(":", "").replace("-", "")
            caminho = pasta / f"bronze_bruto_{hn}_{ts_sessao}.csv"

            f = open(caminho, "w", encoding="utf-8", newline="")
            pd.DataFrame(columns=COLUNAS).to_csv(f, sep=";", index=False, header=True)
            self.handles[hn] = (f, True, caminho)
            self.sessao_atual[hn] = sessao_origem

            # Inicia manifesto local EM_ANDAMENTO
            self._atualizar_manifesto_local(fmc, sessao_origem, caminho, "EM_ANDAMENTO")
            
            # Se tem S3, inicia manifesto S3 EM_ANDAMENTO
            if self.s3_client:
                chave_s3 = self._chave_s3_da_sessao(fmc, sessao_s3, caminho)
                registrar_bronze_ativo_s3(
                    str(caminho),
                    s3_client=self.s3_client,
                    chave_s3=chave_s3,
                    hostname=hn,
                    sessao_id=sessao_origem,  # sessao_id no manifesto = VOO-... (completo)
                    estado_coleta="EM_ANDAMENTO",
                )

        return self.handles[hn]

    def _chave_s3_da_sessao(self, fmc, sessao_s3, csv_path):
        """Gera chave S3 para esta sessão (pasta sessao= usa formato limpo)."""
        hn = fmc["hostname"]
        nome_arquivo = os.path.basename(csv_path)
        host_s3 = _segmento_s3(hn)
        # sessao_s3 já está no formato limpo: 20261126T143000-0300
        return f"{S3_PREFIXO}/hostname={host_s3}/sessao={sessao_s3}/{_segmento_s3(nome_arquivo)}"

    def _atualizar_manifesto_local(self, fmc, sessao_origem, csv_path, estado_coleta):
        """Cria/atualiza bronze_ativo.json local (igual agente.py)."""
        hn = fmc["hostname"]
        agora = datetime.now(timezone.utc).isoformat()
        # Para o manifesto local, geramos a chave S3 com sessao_s3 limpa
        sessao_s3 = sessao_origem.replace("VOO-", "").replace("BOOT-", "")
        
        manifesto = {
            "arquivo_bronze": str(csv_path),
            "bucket": BUCKET_NAME if self.s3_client else None,
            "chave_s3": self._chave_s3_da_sessao(fmc, sessao_s3, csv_path) if self.s3_client else None,
            "s3_uri": f"s3://{BUCKET_NAME}/{self._chave_s3_da_sessao(fmc, sessao_s3, csv_path)}" if self.s3_client else None,
            "chave_manifesto_s3": _chave_manifesto_s3(hn) if self.s3_client else None,
            "hostname": hn,
            "sessao_id": sessao_origem,
            "modo": "simulado",
            "estado_coleta": estado_coleta,
            "atualizado_em_utc": agora,
            "data_inicio": sessao_origem.replace("VOO-", "").replace("BOOT-", "")[:19],
        }

        self.manifestos[hn] = manifesto

        # Escreve manifesto local (substituição atômica)
        pasta = self.base / hn
        manifesto_path = pasta / "bronze_ativo.json"
        tmp_path = manifesto_path.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as mf:
            json.dump(manifesto, mf, ensure_ascii=False, indent=2)
        os.replace(tmp_path, manifesto_path)

    def escrever(self, fmc, row, sessao_origem, sessao_s3):
        f, _, _ = self._get_handle(fmc, sessao_origem, sessao_s3)
        df = pd.DataFrame([row], columns=COLUNAS)
        df.to_csv(f, sep=";", index=False, header=False)

    def finalizar_sessao(self, fmc, sessao_origem, sessao_s3):
        """Fecha CSV, atualiza manifestos para CONCLUIDA, cria _concluido.json local + upload S3."""
        hn = fmc["hostname"]
        if hn in self.handles and self.sessao_atual.get(hn) == sessao_origem:
            f, _, csv_path = self.handles[hn]
            f.close()

            # 1. Atualiza manifesto local para CONCLUIDA
            self._atualizar_manifesto_local(fmc, sessao_origem, csv_path, "CONCLUIDA")

            # 2. Cria _concluido.json local
            marcador_path = Path(csv_path).with_name(Path(csv_path).stem + "_concluido.json")
            with open(marcador_path, "w", encoding="utf-8") as mf:
                json.dump({"arquivo_bronze": str(csv_path)}, mf, ensure_ascii=False, indent=2)
            print(f"  📄 Local: {csv_path.name} + _concluido.json")

            # 3. Upload para S3 (se configurado) - usa sessao_s3 para caminho S3
            if self.s3_client:
                try:
                    enviar_para_s3(self.s3_client, str(csv_path), hn, sessao_origem, sessao_s3, "CONCLUIDA")
                except Exception as e:
                    print(f"  ⚠️  Erro upload S3: {e}")

            # 4. Limpa cache
            del self.handles[hn]
            del self.sessao_atual[hn]

    def fechar_todos(self):
        for hn, (f, _, _) in list(self.handles.items()):
            f.close()
        self.handles.clear()
        self.sessao_atual.clear()


# ============================================================================
# CRONOGRAMA: 30 DIAS
# ============================================================================

def gerar_cronograma(dias, fmc_list):
    """Gera lista de (fmc, datetime_inicio) para 30 dias."""
    voos = []
    base = datetime.now(ZoneInfo("America/Sao_Paulo")).replace(hour=0, minute=0, second=0, microsecond=0)

    ativos = [f for f in fmc_list if f["status"] == "ATIVO"]
    manut = [f for f in fmc_list if f["status"] == "MANUTENCAO"]
    inativos = [f for f in fmc_list if f["status"] == "INATIVO"]

    for d in range(dias):
        dia = base + timedelta(days=d)

        # ATIVOS: 4-6 voos/dia (6h-23h)
        for fmc in ativos:
            for v in range(random.randint(4, 6)):
                h = 6 + (v * 17 // 6)
                m = random.randint(0, 59)
                voos.append((fmc, dia.replace(hour=h, minute=m)))

        # MANUTENCAO: 1-2 voos/dia (testes)
        for fmc in manut:
            for v in range(random.randint(1, 2)):
                h = 8 + v * 4
                m = random.randint(0, 59)
                voos.append((fmc, dia.replace(hour=h, minute=m)))

        # INATIVOS: 1 boot/dia (madrugada)
        for fmc in inativos:
            voos.append((fmc, dia.replace(hour=2, minute=0)))

    return voos


# ============================================================================
# MAIN
# ============================================================================

def main():
    print(f"🚀 Simulando {DIAS} dias para {len(FMCs)} FMCs")
    print(f"   Acelerador: {ACELERADOR}x | Intervalo: {INTERVALO_S}s | Seed: {SEED}")
    print(f"   Componentes ativos por FMC: {[(f['nome'], [c for c,v in f['componentes'].items() if v]) for f in FMCs]}")
    
    # S3 setup
    s3_client = None
    if BUCKET_NAME:
        try:
            s3_client = obter_cliente_s3()
            print(f"   ☁️  S3: {BUCKET_NAME} ({AWS_REGION}) - prefixo: {S3_PREFIXO}")
        except Exception as e:
            print(f"   ⚠️  S3 não configurado: {e}")
            print(f"   📁 Salvando apenas local em: {PASTA_BRONZE_LOCAL}")
    else:
        print(f"   📁 BUCKET_NAME não definido - salvando apenas local em: {PASTA_BRONZE_LOCAL}")

    writer = BronzeWriter(PASTA_BRONZE_LOCAL, s3_client)
    voos = gerar_cronograma(DIAS, FMCs)
    print(f"📅 Total sessões (voos): {len(voos)}")

    stats = {"voos": 0, "por_fmc": {}, "por_status": {}}

    for i, (fmc, inicio) in enumerate(voos, 1):
        if i % 50 == 0:
            print(f"   {i}/{len(voos)}...")

        # Cria gerador de anomalias específico para este FMC
        rng_fmc = random.Random(SEED + fmc["id"] * 1000 + i)
        anom_gen = GeradorAnomalias(rng_fmc, fmc)

        if fmc["status"] == "INATIVO":
            simular_boot_apenas(fmc, anom_gen, inicio, writer)
        else:
            simular_voo(fmc, anom_gen, inicio, writer)

        stats["voos"] += 1
        stats["por_fmc"][fmc["hostname"]] = stats["por_fmc"].get(fmc["hostname"], 0) + 1
        stats["por_status"][fmc["status"]] = stats["por_status"].get(fmc["status"], 0) + 1

    writer.fechar_todos()

    print(f"\n✅ Concluído!")
    print(f"   Total sessões: {stats['voos']}")
    print(f"   Por FMC: {stats['por_fmc']}")
    print(f"   Por status: {stats['por_status']}")
    print(f"   Bronze local: {PASTA_BRONZE_LOCAL}")
    if s3_client:
        print(f"   Bronze S3: s3://{BUCKET_NAME}/{S3_PREFIXO}/hostname=*/sessao=*/")
        print(f"   Manifestos S3: s3://{BUCKET_NAME}/{S3_PREFIXO_MANIFESTOS}/hostname=*/bronze_ativo.json")
    print(f"   Compatível com pipeline Lambda (Silver → Gold → Alertas)")

if __name__ == "__main__":
    main()