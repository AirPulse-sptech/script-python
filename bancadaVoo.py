import os
import sys
import json
import time
import random
import argparse
import multiprocessing as mp
from datetime import datetime, timezone, timedelta
import psutil


# GERENCIADOR DE MEMÓRIA RAM

blocos_de_memoria_ram = []
#TAMANHO_DO_BLOCO_BYTES = 50 * 1024 * 1024  # Tijolos de 50 Megabytes
#Se usar RAM simulada de 4mb
TAMANHO_DO_BLOCO_BYTES = 1 * 1024 * 1024
MARGEM_SEGURANCA = 0.10                    # Nunca deixa o PC com menos de 10% de RAM
RAM_MAX_PCT = 85                           # Teto de segurança (85% da RAM total do PC)

def ajustar_memoria_ram(porcentagem_desejada):
    # Calcula quantos tijolos de 50MB precisamos criar ou destruir para bater a meta de RAM.
    global blocos_de_memoria_ram
    
    memoria_do_pc = psutil.virtual_memory()
    
    # Garante que não vamos pedir mais do que o limite de 85%
    porcentagem_desejada = min(porcentagem_desejada, RAM_MAX_PCT)
    
    # Descobre quantos tijolos de 50MB precisamos no total
    bytes_alvo = memoria_do_pc.total * (porcentagem_desejada / 100)
    blocos_necessarios = int(bytes_alvo / TAMANHO_DO_BLOCO_BYTES)

    blocos_faltantes = blocos_necessarios - len(blocos_de_memoria_ram)
    
    # Trava de segurança: Se o PC estiver ficando sem memória real, limitamos a alocação
    if blocos_faltantes > 0:
        memoria_de_folga = memoria_do_pc.available - (MARGEM_SEGURANCA * memoria_do_pc.total)
        blocos_que_cabem_na_folga = int(max(0, memoria_de_folga) // TAMANHO_DO_BLOCO_BYTES)
        
        if blocos_que_cabem_na_folga < blocos_faltantes:
            print(f"PC sem memória real suficiente. Limitando a {blocos_que_cabem_na_folga} blocos extras.")
            blocos_necessarios = len(blocos_de_memoria_ram) + blocos_que_cabem_na_folga

    # Se faltam blocos, adiciona novos
    while len(blocos_de_memoria_ram) < blocos_necessarios:
        novo_bloco = bytearray(b"\x01") * TAMANHO_DO_BLOCO_BYTES
        blocos_de_memoria_ram.append(novo_bloco)
        
    # Se sobejou blocos, joga fora
    while len(blocos_de_memoria_ram) > blocos_necessarios:
        blocos_de_memoria_ram.pop()


# GERENCIADOR DE CPU E DISCO

def estressar_cpu_processador(meta_de_uso_compartilhada, sinal_de_parada):
    # Trabalha e dorme em frações de segundo para bater a meta exata de CPU.
    while not sinal_de_parada.is_set():
        tempo_trabalhando = meta_de_uso_compartilhada.value * 0.1
        tempo_inicio = time.perf_counter()
        
        # Loop vazio: força a CPU a 100% durante esse micro-intervalo
        while time.perf_counter() - tempo_inicio < tempo_trabalhando:
            pass
            
        # Dorme o resto do tempo para a média cair para o valor desejado
        tempo_para_dormir = 0.1 - tempo_trabalhando
        time.sleep(max(0, tempo_para_dormir))

def gravar_arquivos_no_disco(fator_escrita, kb_extra=0):
    # Escreve dados falsos na 'Flash' para gerar desgaste e I/O reais.
    with open("fmc_flash_bite.log", "ab") as arquivo:
        # Sorteia entre 10KB e 50KB e multiplica pela aceleração e pelos extras do cenário
        quantidade_kb = random.randint(10, 50) * fator_escrita + kb_extra
        quantidade_de_bytes = int(1024 * quantidade_kb)
        
        bytes_aleatorios = os.urandom(quantidade_de_bytes)
        arquivo.write(bytes_aleatorios)


# ROTEIRO DO VOO E CENÁRIOS

FASES_DO_VOO = [
    {"nome": "BOOT",      "minutos": 1,  "uso_cpu": 65, "uso_ram": 0},
    {"nome": "PREFLIGHT", "minutos": 15, "uso_cpu": 30, "uso_ram": 13},
    {"nome": "TAXI_OUT",  "minutos": 15, "uso_cpu": 25, "uso_ram": 16},
    {"nome": "TAKEOFF",   "minutos": 3,  "uso_cpu": 50, "uso_ram": 19},
    {"nome": "CLIMB",     "minutos": 20, "uso_cpu": 42, "uso_ram": 21},
    {"nome": "CRUISE",    "minutos": 60, "uso_cpu": 22, "uso_ram": 22},
    {"nome": "DESCENT",   "minutos": 20, "uso_cpu": 45, "uso_ram": 24},
    {"nome": "APPROACH",  "minutos": 10, "uso_cpu": 58, "uso_ram": 27},
    {"nome": "TAXI_IN",   "minutos": 6,  "uso_cpu": 25, "uso_ram": 25},
    {"nome": "DONE",      "minutos": 5,  "uso_cpu": 18, "uso_ram": 23}
]

# Modificadores que sobrescrevem o roteiro normal em momentos específicos
CENARIOS = {
    "normal": [],
    "pico_cpu": [{"tipo": "cpu", "fase": "CRUISE", "de": 0.2, "ate": 0.8, "valor": 92}],
    "vazamento_ram": [{"tipo": "ram_rampa", "fase": "CRUISE", "de": 0.0, "ate": 0.5, "valor": 92}],
    "rajada_flash": [{"tipo": "flash", "fase": "CRUISE", "de": 0.3, "ate": 0.7, "valor": 30}],
    "misto": [
        {"tipo": "cpu", "fase": "CLIMB", "de": 0.1, "ate": 0.9, "valor": 88},
        {"tipo": "ram_rampa", "fase": "CRUISE", "de": 0.0, "ate": 0.5, "valor": 92},
        {"tipo": "flash", "fase": "CRUISE", "de": 0.3, "ate": 0.7, "valor": 30},
    ],
}

def calcular_alvos_neste_segundo(fase_atual, porcentagem_concluida_da_fase, anomalias_ativas):
    # Verifica se neste exato momento existe alguma anomalia substituindo os valores normais.
    cpu = fase_atual["uso_cpu"]
    ram = fase_atual["uso_ram"]
    kb_extra_flash = 0
    
    for anomalia in anomalias_ativas:
        # Se a anomalia não é para a fase atual, ou o tempo dela ainda não chegou, ignora
        if anomalia["fase"] != fase_atual["nome"] or porcentagem_concluida_da_fase < anomalia["de"]:
            continue
            
        # Se for um pico de CPU e estiver dentro do período
        if anomalia["tipo"] == "cpu" and porcentagem_concluida_da_fase <= anomalia["ate"]:
            cpu = anomalia["valor"]
            
        # Se for um vazamento de RAM (vai subindo aos poucos até o limite e trava)
        elif anomalia["tipo"] == "ram_rampa":
            tamanho_do_periodo = anomalia["ate"] - anomalia["de"]
            if tamanho_do_periodo <= 0: tamanho_do_periodo = 0.001 # Evita divisão por zero
            
            progresso_da_anomalia = (porcentagem_concluida_da_fase - anomalia["de"]) / tamanho_do_periodo
            progresso_da_anomalia = min(1.0, progresso_da_anomalia) # Trava em 100% do crescimento
            
            diferenca = anomalia["valor"] - fase_atual["uso_ram"]
            ram = fase_atual["uso_ram"] + (diferenca * progresso_da_anomalia)
            
        # Se for rajada de gravação na Flash
        elif anomalia["tipo"] == "flash" and porcentagem_concluida_da_fase <= anomalia["ate"]:
            kb_extra_flash += anomalia["valor"]
            
    return cpu, ram, kb_extra_flash


def gravar_metadados_atomicos(caminho, metadados):
    # Atualiza os metadados sem expor JSON parcialmente escrito ao agente.
    temporario = f"{caminho}.tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        json.dump(metadados, arquivo, ensure_ascii=False)
    os.replace(temporario, caminho)



# EXECUÇÃO PRINCIPAL

if __name__ == "__main__":
    # Permite escolher o cenário via linha de comando
    terminal = argparse.ArgumentParser(description="Simulador de Hardware FMC")
    terminal.add_argument("--cenario", choices=list(CENARIOS), default=os.getenv("CENARIO", "normal"))
    terminal.add_argument("--acelerador", type=float, default=float(os.getenv("ACELERADOR_DE_TEMPO", "10")))
    terminal.add_argument("--ram-max-pct", type=float, default=RAM_MAX_PCT)
    args = terminal.parse_args()

    if args.acelerador <= 0:
        terminal.error("--acelerador deve ser maior que zero.")
    if not 0 < args.ram_max_pct <= 100:
        terminal.error("--ram-max-pct deve estar entre 0 e 100.")

    RAM_MAX_PCT = args.ram_max_pct
    ACELERADOR_DE_TEMPO = args.acelerador
    anomalias_escolhidas = CENARIOS[args.cenario]
    fator_escrita = ACELERADOR_DE_TEMPO / 10

    print(f"LIGANDO O SIMULADOR... (PID {os.getpid()}) | cenário: {args.cenario} | acelerador: {ACELERADOR_DE_TEMPO:g}x")

    # Limpa a lixeira do voo antigo
    if os.path.exists("fmc_flash_bite.log"):
        os.remove("fmc_flash_bite.log")

    # Metadados lidos pelo agente: fase explícita evita inferência cega por uptime.
    sessao_origem = datetime.now(timezone.utc).strftime("VOO-%Y%m%dT%H%M%S.%fZ")
    caminho_meta = os.path.abspath("bancada_meta.json")
    meta_base = {
        "acelerador": ACELERADOR_DE_TEMPO,
        "cenario": args.cenario,
        "sessao_origem": sessao_origem,
        "fase": "INICIALIZANDO",
        "modo_execucao": "voo",
        "intervalo_coleta_s": float(os.getenv("INTERVALO_COLETA_S", "10")),
    }
    gravar_metadados_atomicos(caminho_meta, meta_base)

    # LIGA OS MOTORES DA CPU EM SEGUNDO PLANO
    meta_cpu_atual = mp.Value("d", 0.0)
    sinal_desligar_cpu = mp.Event()
    
    for _ in range(os.cpu_count()):
        motor = mp.Process(target=estressar_cpu_processador, args=(meta_cpu_atual, sinal_desligar_cpu))
        motor.daemon = True
        motor.start()

    gabarito = {"cenario": args.cenario, "acelerador": ACELERADOR_DE_TEMPO, "anomalias": []}
    
    try:
        # Percorre todas as fases do voo
        for fase in FASES_DO_VOO:
            segundos_reais_espera = (fase["minutos"] * 60) / ACELERADOR_DE_TEMPO
            inicio_fase = time.time()
            fim_fase = inicio_fase + segundos_reais_espera
            inicio_fase_utc = datetime.fromtimestamp(inicio_fase, timezone.utc).isoformat()
            fim_fase_utc = datetime.fromtimestamp(fim_fase, timezone.utc).isoformat()
            metadados_fase = {
                **meta_base,
                "fase": fase["nome"],
                "fase_inicio_utc": inicio_fase_utc,
                "fase_fim_utc": fim_fase_utc,
                "duracao_fase_simulada_s": fase["minutos"] * 60,
            }
            gravar_metadados_atomicos(caminho_meta, metadados_fase)

            print(f"Fase: {fase['nome']:<10} | Duração: {segundos_reais_espera:.1f}s | CPU Base: {fase['uso_cpu']}% | RAM Base: {fase['uso_ram']}%")

            # Salva no gabarito os horários exatos que as anomalias vão acontecer
            for a in anomalias_escolhidas:
                if a["fase"] == fase["nome"]:
                    inicio_anomalia = inicio_fase + (a["de"] * segundos_reais_espera)
                    fim_anomalia = inicio_fase + ((1 if a["tipo"] == "ram_rampa" else a["ate"]) * segundos_reais_espera)
                    
                    anomalia_com_datas = a.copy()
                    anomalia_com_datas["inicio_utc"] = datetime.fromtimestamp(inicio_anomalia, timezone.utc).isoformat()
                    anomalia_com_datas["fim_utc"] = datetime.fromtimestamp(fim_anomalia, timezone.utc).isoformat()
                    gabarito["anomalias"].append(anomalia_com_datas)

            # Loop de execução segundo a segundo
            while time.time() < fim_fase:
                porcentagem_concluida = (time.time() - inicio_fase) / segundos_reais_espera
                metadados_fase["progresso_fase_pct"] = min(100.0, max(0.0, porcentagem_concluida * 100))
                gravar_metadados_atomicos(caminho_meta, metadados_fase)

                # 1. Calcula o alvo exato
                cpu_alvo, ram_alvo, kb_extra = calcular_alvos_neste_segundo(fase, porcentagem_concluida, anomalias_escolhidas)
                
                # 2. Aplica o esforço
                meta_cpu_atual.value = cpu_alvo / 100.0
                ajustar_memoria_ram(ram_alvo)
                gravar_arquivos_no_disco(fator_escrita, kb_extra * fator_escrita)
                
                # Aguarda 1 segundo antes do próximo ciclo
                time.sleep(1)
                
    finally:
        # Quando acaba, abaixa a bandeira para desligar os processos de CPU e salva o gabarito
        sinal_desligar_cpu.set()
        with open("bancada_gabarito.json", "w", encoding="utf-8") as f:
            json.dump(gabarito, f, ensure_ascii=False, indent=2)
        print("Simulação do Voo Encerrada. Pode fechar o simulador.")