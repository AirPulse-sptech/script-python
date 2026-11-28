"""Cadastro, fases e formato Bronze usados pelos simuladores mensais."""

COLUNAS = [
    "timestamp_utc", "seq", "hostname", "placa_nome", "origem", "uptime_proc_s",
    "fator_tempo", "cpu_percent", "cpu_nucleos", "qtd_tarefas", "ram_usada_bytes",
    "ram_baseline_bytes", "ram_total_bytes", "ram_disponivel_bytes",
    "flash_usada_bytes", "flash_total_bytes", "modo_execucao", "intervalo_coleta_s",
    "sessao_origem", "fase", "cpu_pct_host", "cpu_semantica", "ram_baseline_fonte",
    "ram_medicao_tipo", "io_leitura_contador_acum", "io_escrita_contador_acum",
    "io_leitura_unidade", "io_escrita_unidade",
]

RAM_TOTAL = 16 * 1024**3

FLASH_TOTAL = 32 * 1024**2

CPU_NUCLEOS = 4

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

LIMITES = {
    "CPU": {"atencao": 75.0, "critico": 90.0},
    "RAM": {"atencao": 80.0, "critico": 95.0},
    "DISCO": {"atencao": 80.0, "critico": 90.0},
}

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
