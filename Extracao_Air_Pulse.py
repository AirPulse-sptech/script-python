import os
import time
import socket
from datetime import datetime, timezone
import psutil
from dotenv import load_dotenv
import boto3

# Carrega as variáveis do arquivo .env
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Conecta ao S3
s3_client = None
if BUCKET_NAME and AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    except Exception as e:
        print(f"Erro ao inicializar cliente S3: {e}")

CAMINHO_CSV = "bronze_bruto.csv"
CABECALHO = "timestamp_utc;computador_id;cpu_percent;ram_percent;ram_disponivel_bytes;disco_percent;disco_livre_bytes;io_leitura_bytes;io_escrita_bytes\n"

def extrair_e_enviar():
    timestamp_utc = datetime.now(timezone.utc).isoformat()
    hostname = socket.gethostname()
    
    # Extração ultraleve e proteço contra falhas 
    try:
        vmem = psutil.virtual_memory()
        ram_pct, ram_livre = vmem.percent, vmem.available
    except Exception:
        ram_pct = ram_livre = ""

    try:
        disco = psutil.disk_usage('C:\\' if os.name == 'nt' else '/')
        disco_pct, disco_livre = disco.percent, disco.free
    except Exception:
        disco_pct = disco_livre = ""
        
    try:
        io = psutil.disk_io_counters()
        io_read, io_write = (io.read_bytes, io.write_bytes) if io else ("", "")
    except Exception:
        io_read = io_write = ""

    cpu_pct = psutil.cpu_percent(interval=None)

    # Monta a linha CSV diretamente na memória para ficar mais leve que o pandas
    linha_csv = f"{timestamp_utc};{hostname};{cpu_pct};{ram_pct};{ram_livre};{disco_pct};{disco_livre};{io_read};{io_write}\n"

    # Escrita direta de arquiv
    arquivo_novo = not os.path.exists(CAMINHO_CSV)
    with open(CAMINHO_CSV, "a", encoding="utf-8") as f:
        if arquivo_novo:
            f.write(CABECALHO)
        f.write(linha_csv)

    print(f"[{timestamp_utc}] Extração salva localmente.")

    # Envio para o Bucket S3
    if s3_client and BUCKET_NAME:
        try:
            chave_s3 = f"bronze/{hostname}/bronze_bruto.csv"
            s3_client.upload_file(CAMINHO_CSV, BUCKET_NAME, chave_s3)
            print(f"[{timestamp_utc}] S3 Upload OK: s3://{BUCKET_NAME}/{chave_s3}")
        except Exception as e:
            print(f"Erro ao enviar para o S3: {e}")

if __name__ == "__main__":
    psutil.cpu_percent(interval=None) # Aquecimento obrigatório
    print("Iniciando Extrator Bronze Ultraleve + S3... (Ctrl+C para parar)")
    
    while True:
        extrair_e_enviar()
        time.sleep(5)