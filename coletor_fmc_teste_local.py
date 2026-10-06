import os
import time
import socket
from datetime import datetime, timezone
import psutil
import pandas as pd

def extrair_dados_bronze():
  
    timestamp_utc = datetime.now(timezone.utc).isoformat()
    
    try:
        vmem = psutil.virtual_memory()
        ram_pct = vmem.percent
        ram_livre = vmem.available
    except Exception:
        ram_pct = ram_livre = None

    try:
        disco = psutil.disk_usage('/' if os.name != 'nt' else 'C:\\')
        disco_pct = disco.percent
        disco_livre = disco.free
    except Exception:
        disco_pct = disco_livre = None
        
    try:
        io = psutil.disk_io_counters()
        io_read = io.read_bytes
        io_write = io.write_bytes
    except Exception:
        io_read = io_write = None

    dados_brutos = {
        "timestamp_utc": [timestamp_utc],
        "computador_id": [socket.gethostname()],
        "cpu_percent": [psutil.cpu_percent(interval=None)],
        "ram_percent": [ram_pct],
        "ram_disponivel_bytes": [ram_livre],
        "disco_percent": [disco_pct],
        "disco_livre_bytes": [disco_livre],
        "io_leitura_bytes": [io_read],
        "io_escrita_bytes": [io_write]
    }

    caminho_csv = "bronze_bruto.csv"
    arquivo_existe = os.path.exists(caminho_csv)
    
    pd.DataFrame(dados_brutos).to_csv(
        caminho_csv,
        sep=';',
        mode='a',
        index=False,
        header=not arquivo_existe,
        encoding='utf-8'
    )
    print(f"[{timestamp_utc}] Extração concluída.")

if __name__ == "__main__":
    psutil.cpu_percent(interval=None) # Descarta a primeira medida coletada
    print("Iniciando Leitura de Dados...(Ctrl+C para parar)")
    
    while True:
        extrair_dados_bronze()
        time.sleep(5)