import os
import time
import random
import multiprocessing as mp
from datetime import datetime, timedelta
import psutil

# ==========================================
# 1. CONFIGURAÇÕES DA SIMULAÇÃO
# ==========================================
ARQUIVO_FLASH = "fmc_flash_bite.log"
ESCALA_TEMPO = 10  # Acelera o tempo (1 min real = 10 min de voo)

# Fases do voo: (Nome, Duração em minutos)
FASES_VOO = [
    ("BOOT", 1), ("PREFLIGHT", 15), ("TAXI_OUT", 15), ("TAKEOFF", 3),
    ("CLIMB", 20), ("CRUISE", 60), ("DESCENT", 20), ("APPROACH", 10),
    ("TAXI_IN", 6), ("DONE", 5)
]

# Carga de hardware por fase: (CPU_Alvo_%, RAM_Extra_%)
CARGA = {
    "BOOT": (65, 0), "PREFLIGHT": (30, 13), "TAXI_OUT": (25, 16),
    "TAKEOFF": (50, 19), "CLIMB": (42, 21), "CRUISE": (22, 22),
    "DESCENT": (45, 24), "APPROACH": (58, 27), "TAXI_IN": (25, 25), "DONE": (18, 23)
}

# ==========================================
# 2. MOTORES DE CARGA (CPU, RAM e FLASH)
# ==========================================
def estressar_cpu(duty_cycle, evento_parada):
    """Mantém os núcleos do PC ocupados na porcentagem alvo[cite: 11]."""
    while not evento_parada.is_set():
        tempo_ocupado = duty_cycle.value * 0.1
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < tempo_ocupado:
            pass
        time.sleep(0.1 - tempo_ocupado)

class GerenciadorMemoria:
    """Força a alocação de RAM preenchendo blocos de bytes na memória[cite: 11]."""
    def __init__(self, bloco_mb=50):
        self.bloco_tamanho = bloco_mb * 1024 * 1024
        self.blocos_segurados = []

    def ajustar_carga(self, pct_extra):
        memoria_total = psutil.virtual_memory().total
        blocos_alvo = int((memoria_total * (pct_extra / 100)) / self.bloco_tamanho)
        while len(self.blocos_segurados) < blocos_alvo:
            self.blocos_segurados.append(bytearray(b"\x01") * self.bloco_tamanho)
        while len(self.blocos_segurados) > blocos_alvo:
            self.blocos_segurados.pop()

def gravar_flash_simulada():
    """Escreve bytes em um arquivo de log para simular o desgaste da Flash."""
    with open(ARQUIVO_FLASH, "ab") as f:
        f.write(os.urandom(1024 * random.randint(10, 50))) # Grava de 10KB a 50KB

# ==========================================
# 3. LOOP DE VOO (Main)
# ==========================================
if __name__ == "__main__":
    print(f"PID DO SIMULADOR (FMC): {os.getpid()}")
    if os.path.exists(ARQUIVO_FLASH):
        os.remove(ARQUIVO_FLASH) # Zera a flash de voos anteriores

    duty_cpu = mp.Value("d", 0.0)
    evento_parada = mp.Event()
    
    # Inicia os trabalhadores de CPU em background[cite: 11]
    processos = [mp.Process(target=estressar_cpu, args=(duty_cpu, evento_parada), daemon=True) for _ in range(os.cpu_count())]
    for p in processos: p.start()
    
    memoria = GerenciadorMemoria()

    try:
        for fase, minutos in FASES_VOO:
            cpu_alvo, ram_extra = CARGA[fase]
            duty_cpu.value = cpu_alvo / 100.0
            memoria.ajustar_carga(ram_extra)
            
            duracao_real_segundos = (minutos * 60) / ESCALA_TEMPO
            print(f"Fase: {fase:<10} | Duração (Acelerada): {duracao_real_segundos:.1f}s | CPU: {cpu_alvo}% | RAM Extra: {ram_extra}%")
            
            tempo_fim_fase = time.time() + duracao_real_segundos
            while time.time() < tempo_fim_fase:
                gravar_flash_simulada()
                time.sleep(1) # Pulsa a cada 1 segundo
    finally:
        evento_parada.set()
        print("Simulação do Voo Encerrada.")