from datetime import datetime, timedelta, timezone
import multiprocessing as mp
import os
import random
import time

import pandas as pd
import psutil

# Reaproveita a coleta do extract_3: mesmas colunas, mesma medição via psutil.
# Se você mudar as colunas lá (ex.: tirar TempCPU), a bancada acompanha.
from extract_3 import INTERVALO_S, coletar

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
CONFIG = {
    "hostname": None,                     # None = nome real da máquina; ou ex.: "FMC-SIM-01"
    "partida_local": "2026-10-07 06:30",  # horário de Brasília em que a aeronave sai do portão (pushback)
    "cruzeiro_min": 60,                   # duração do cruzeiro (depende da rota)
    "escala": 1,                          # 1 = tempo real | 10 = voo 10x mais rápido
    "seed": 42,                           # mesma seed = mesma sequência de durações
    "ram_max_pct": 85,                    # trava de segurança: a bancada nunca passa disso
    "bloco_mb": 50,                       # granularidade da alocação de RAM
    "saida": "Dados_sim.csv",             # dados no formato do extract_3
    "saida_fases": "Fases_sim.csv",       # gabarito: quando cada fase ocorreu
}

FUSO_LOCAL = timezone(timedelta(hours=-3))   # Brasília

# ----------------------------------------------------------------------
# CARGA POR FASE: (cpu_alvo_%, ram_extra_%)
#   cpu_alvo : carga total de CPU que a bancada gera
#   ram_extra: RAM (% do total) segurada ALÉM do que o PC já usa
# HIPÓTESES de carga relativa por fase; TAXI_OUT e TAXI_IN são as mais incertas.
# ----------------------------------------------------------------------
CARGA = {
    "BOOT":      (65,  0),
    "PREFLIGHT": (30, 13),
    "TAXI_OUT":  (25, 16),
    "TAKEOFF":   (50, 19),
    "CLIMB":     (42, 21),
    "CRUISE":    (22, 22),
    "DESCENT":   (45, 24),
    "APPROACH":  (58, 27),
    "TAXI_IN":   (25, 25),
    "DONE":      (18, 23),
}


# ----------------------------------------------------------------------
# Linha do tempo coerente com as evidências (relatorio_causalidade_temporal.md)
# ----------------------------------------------------------------------
def montar_voo(rng):
    """Lista de (fase, minutos). Durações variam de voo para voo, dentro de faixas."""
    preflight = rng.uniform(15, 25)                          # alinhamento IRS (~10 min) + configuração do FMC
    taxi_out = min(25, max(10, rng.gauss(15, 3)))            # BTS: média ~14-17 min
    taxi_in = min(12, max(3, rng.gauss(6, 1.5)))             # BTS: ~5,5-7 min
    return [
        ("BOOT", 1),
        ("PREFLIGHT", preflight),
        ("TAXI_OUT", taxi_out),
        ("TAKEOFF", 3),
        ("CLIMB", 20),
        ("CRUISE", CONFIG["cruzeiro_min"]),
        ("DESCENT", 20),
        ("APPROACH", 10),
        ("TAXI_IN", taxi_in),
        ("DONE", 5),
    ]


def instante_ligado(voo):
    """O FMC é ligado antes do pushback: partida - (BOOT + PREFLIGHT)."""
    partida = datetime.strptime(CONFIG["partida_local"], "%Y-%m-%d %H:%M").replace(tzinfo=FUSO_LOCAL)
    antes_da_partida = sum(minutos for fase, minutos in voo if fase in ("BOOT", "PREFLIGHT"))
    return (partida - timedelta(minutes=antes_da_partida)).astimezone(timezone.utc)


# ----------------------------------------------------------------------
# Geradores de carga (CPU e RAM reais, medidos depois pelo psutil)
# ----------------------------------------------------------------------
def trabalhador(duty, parar):
    """Mantém o núcleo ocupado 'duty' do tempo em ciclos de 100 ms."""
    while not parar.is_set():
        ocupado = duty.value * 0.1
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < ocupado:
            pass
        time.sleep(0.1 - ocupado)


class MemoriaSegurada:
    """Segura blocos preenchidos para forçar o SO a alocar RAM de verdade."""

    def __init__(self, bloco_mb):
        self.bloco = bloco_mb * 1024 * 1024
        self.blocos = []

    def ajustar(self, extra_pct, teto_pct):
        total = psutil.virtual_memory().total
        alvo = int(total * extra_pct / 100 / self.bloco)
        while len(self.blocos) < alvo and psutil.virtual_memory().percent < teto_pct:
            self.blocos.append(bytearray(b"\x01") * self.bloco)
        while len(self.blocos) > alvo:
            self.blocos.pop()


# ----------------------------------------------------------------------
# Gravação (mesmo estilo do extract_3)
# ----------------------------------------------------------------------
def gravar(linha, caminho):
    pd.DataFrame([linha]).to_csv(
        caminho, mode="a", index=False, header=not os.path.exists(caminho),
        encoding="utf-8", sep=";"
    )


def simular():
    for caminho in (CONFIG["saida"], CONFIG["saida_fases"]):
        if os.path.exists(caminho):
            os.remove(caminho)                      # cada execução gera um voo novo

    rng = random.Random(CONFIG["seed"])
    voo = montar_voo(rng)
    ligado = instante_ligado(voo)
    escala = CONFIG["escala"]
    espera = INTERVALO_S / escala                   # intervalo real entre amostras

    duty = mp.Value("d", 0.0)
    parar = mp.Event()
    procs = [mp.Process(target=trabalhador, args=(duty, parar), daemon=True)
             for _ in range(os.cpu_count())]
    for p in procs:
        p.start()
    memoria = MemoriaSegurada(CONFIG["bloco_mb"])

    print(f"FMC ligado (UTC): {ligado.isoformat()} | pushback (local): {CONFIG['partida_local']}")
    psutil.cpu_percent(interval=None)               # descarta a 1ª leitura
    amostra = 0
    proximo = time.monotonic() + espera

    try:
        for fase, minutos in voo:
            cpu_alvo, ram_extra = CARGA[fase]
            duty.value = cpu_alvo / 100
            inicio_fase = ligado + timedelta(seconds=amostra * INTERVALO_S)
            n = round(minutos * 60 / INTERVALO_S)
            print(f"[{fase:<9}] {minutos:5.1f} min | cpu alvo {cpu_alvo}% | ram extra {ram_extra}%")

            for _ in range(n):
                memoria.ajustar(ram_extra, CONFIG["ram_max_pct"])
                time.sleep(max(0, proximo - time.monotonic()))
                proximo += espera

                linha = coletar()
                linha["TimeStamp"] = (ligado + timedelta(seconds=amostra * INTERVALO_S)).isoformat()
                linha["BootTime"] = ligado.isoformat()
                if CONFIG["hostname"]:
                    linha["Hostname"] = CONFIG["hostname"]
                gravar(linha, CONFIG["saida"])
                amostra += 1

            fim_fase = ligado + timedelta(seconds=amostra * INTERVALO_S)
            gravar({"Hostname": linha["Hostname"], "Fase": fase,
                    "InicioUTC": inicio_fase.isoformat(), "FimUTC": fim_fase.isoformat()},
                   CONFIG["saida_fases"])
    finally:
        parar.set()
        memoria.blocos.clear()

    print(f"Fim. {amostra} amostras em {CONFIG['saida']} | fases em {CONFIG['saida_fases']}")


if __name__ == "__main__":
    simular()
