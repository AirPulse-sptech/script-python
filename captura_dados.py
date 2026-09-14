import psutil
import csv
import time
from datetime import datetime

CONFIG = {
    "username": "FMC-I5A3Y",
    "write_interval": 1,
    "csv_lines": 10
}

def capturar_dados():
    print("Iniciando a Captura dos Dados: ")
    linhas_csv = CONFIG["csv_lines"]

    with open(f'./dados_{CONFIG["username"]}.csv', 'w', newline='') as csvfile:
        fieldNames = ['username', 'timestamp' ,'cpu', 'boot_time' ,'ram', 'ram_total' ,'disco', 'disco_total']
        writer = csv.DictWriter(csvfile, fieldnames=fieldNames)
        writer.writeheader()

        while(linhas_csv > 0):
            cpu_percent = psutil.cpu_percent(interval=CONFIG["write_interval"])
            boot_time = time.time() - psutil.boot_time()
            
            mem = psutil.virtual_memory()
            mem_percent = mem.percent
            mem_total = mem.total
        
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            disk_total = disk.total

            now = datetime.now()
            now_formated = now.strftime("%Y-%m-%d %H:%M:%S")

            writer.writerow({'username': CONFIG["username"], 'timestamp': now_formated, 'cpu': cpu_percent, 'boot_time': boot_time, 'ram': mem_percent, 'ram_total': mem_total, 'disco': disk_percent, 'disco_total': disk_total})

            print(f"Usuário: {CONFIG["username"]} | Timestamp: {now_formated} | Uso de CPU: {cpu_percent}% | Uso de Memória RAM: {mem_percent}% | Uso de Disco: {disk_percent}%")
            linhas_csv -= 1

    print("Encerrando a Captura dos Dados.")

capturar_dados()
