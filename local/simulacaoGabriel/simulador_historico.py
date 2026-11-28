"""Gera sessões Bronze históricas de FMC sem esperar o tempo dos voos."""

import argparse
import csv
import json
import os
import random
from datetime import date, datetime, time, timedelta, timezone
from io import StringIO
from itertools import accumulate
from pathlib import Path
from zoneinfo import ZoneInfo

from dados_simulador import COLUNAS, CPU_NUCLEOS, FASES, FLASH_TOTAL, FMCs, LIMITES, RAM_TOTAL


FINS_DE_FASE = tuple(accumulate(fase["min"] * 60 for fase in FASES))
DURACAO_VOO = timedelta(seconds=FINS_DE_FASE[-1])
INICIO_CRUZEIRO = FINS_DE_FASE[next(i for i, fase in enumerate(FASES) if fase["nome"] == "CRUISE") - 1]
CPU_POR_FASE = {
    "PREFLIGHT": 35, "TAXI_OUT": 33, "CLIMB": 45, "CRUISE": 30,
    "DESCENT": 48, "APPROACH": 60, "TAXI_IN": 35, "DONE": 25,
}
INCIDENTES = (
    ("CPU", "ATENCAO"), ("RAM", "ATENCAO"), ("DISCO", "ATENCAO"),
    ("CPU", "CRITICO"), ("DISCO", "CRITICO"), ("RAM", "ATENCAO"),
)


def gerar_cronograma(inicio, fim, fmcs, seed):
    voos = []
    for fmc in fmcs:
        # O voo recente dá ao painel pontos mesmo na janela de uma hora.
        recente = fim - DURACAO_VOO - timedelta(minutes=5 + fmc["id"] * 2)
        dia = inicio.date()
        while dia <= fim.date():
            rng = random.Random(f"agenda/{seed}/{fmc['aeronave']}/{dia}")
            horario = datetime.combine(dia, time(rng.randint(7, 17), rng.randrange(60)), tzinfo=fim.tzinfo)
            em_manutencao = fmc["status"] == "MANUTENCAO"
            if (not em_manutencao or dia.toordinal() % 4 == 0) and inicio <= horario <= recente - DURACAO_VOO:
                voos.append((horario, fmc))
            dia += timedelta(days=1)
        voos.append((recente, fmc))
    return sorted(voos, key=lambda voo: (voo[0], voo[1]["id"]))


def planejar_incidentes(voos, quantidade, seed, quantidade_fmcs):
    rng = random.Random(f"incidentes/{seed}")
    ocorrencias = {}
    for indice in range(quantidade):
        inicio = indice * len(voos) // quantidade
        fim = (indice + 1) * len(voos) // quantidade
        if indice == quantidade - 1:
            inicio = max(inicio, len(voos) - quantidade_fmcs)
        posicao = rng.randrange(inicio, fim)
        possibilidades = [incidente for incidente in INCIDENTES if voos[posicao][1]["componentes"][incidente[0]]]
        ocorrencias[posicao] = possibilidades[indice % len(possibilidades)]
    return ocorrencias


def amostras_voo(fmc, inicio, intervalo, seed, incidente=None):
    hostname = fmc["hostname"]
    rng = random.Random(f"voo/{seed}/{hostname}/{inicio.isoformat()}")
    perfil = random.Random(f"perfil/{seed}/{hostname}")
    desvio_cpu = perfil.uniform(-6, 8) - (5 if fmc["tipo"] == "SPARE" else 0) + rng.uniform(-3, 3)
    desvio_ram = perfil.uniform(-3, 6) + rng.uniform(-1.5, 1.5)
    flash_normal = FLASH_TOTAL * (perfil.uniform(0.22, 0.51) + rng.uniform(-0.02, 0.02))
    taxa_flash = perfil.randint(110, 220)
    inicio_utc = inicio.astimezone(timezone.utc)
    base = {
        "hostname": hostname, "placa_nome": fmc["nome"], "origem": "SIMULADA_BANCADA",
        "fator_tempo": 1, "modo_execucao": "voo", "intervalo_coleta_s": intervalo,
        "sessao_origem": inicio.strftime("VOO-%Y%m%dT%H%M%S.%f%z"),
    }
    cpu_ativo, ram_ativa, flash_ativa = (fmc["componentes"][c] for c in ("CPU", "RAM", "DISCO"))
    componente = None
    if incidente:
        componente, nivel = incidente
        limites = LIMITES[componente]
        alvo = min(99, limites["critico"] + 7) if nivel == "CRITICO" else (limites["atencao"] + limites["critico"]) / 2
        inicio_incidente = INICIO_CRUZEIRO + rng.randint(5 * 60, 42 * 60)
        subida = 75 if componente == "RAM" else 25
        duracao = max(rng.randint(240, 390), subida + 3 * intervalo + 45)

    ruido_cpu = ruido_ram = 0.0
    io_leitura = io_escrita = flash_anterior = 0
    indice_fase = 0
    for seq, segundos in enumerate(range(0, FINS_DE_FASE[-1], intervalo), 1):
        while segundos >= FINS_DE_FASE[indice_fase]:
            indice_fase += 1
        fase = FASES[indice_fase]
        ruido_cpu = 0.82 * ruido_cpu + rng.uniform(-2, 2)
        ruido_ram = 0.92 * ruido_ram + rng.uniform(-0.35, 0.35)
        cpu = min(68, max(0, CPU_POR_FASE.get(fase["nome"], fase["cpu"]) + desvio_cpu + ruido_cpu))
        ram = max(0, fase["ram"] + desvio_ram + ruido_ram)
        escrita_normal = intervalo * max(0, taxa_flash + rng.randint(-30, 30)) if segundos else 0
        flash_normal += escrita_normal
        flash = flash_normal

        if componente:
            desde = segundos - inicio_incidente
            intensidade = max(0, min(1, desde / subida, (duracao - desde) / 30))
            if componente == "CPU":
                cpu += (alvo - cpu) * intensidade
            elif componente == "RAM":
                ram += (alvo - ram) * intensidade
            else:
                flash += (FLASH_TOTAL * alvo / 100 - flash) * intensidade

        flash_extra = int(flash - flash_normal)
        io_leitura += intervalo * rng.randint(250, 1600) if segundos else 0
        io_escrita += escrita_normal + max(0, flash_extra - flash_anterior)
        flash_anterior = flash_extra
        ram_bytes = int(RAM_TOTAL * ram / 100)
        row = {
            **base, "timestamp_utc": (inicio_utc + timedelta(seconds=segundos)).isoformat(),
            "seq": seq, "uptime_proc_s": segundos, "fase": fase["nome"],
            "qtd_tarefas": round(45 + cpu * 0.8 + rng.randint(-4, 4)),
        }
        if cpu_ativo:
            row.update(cpu_percent=round(cpu * CPU_NUCLEOS, 2), cpu_nucleos=CPU_NUCLEOS,
                       cpu_pct_host=round(cpu, 2), cpu_semantica="soma_percentual_por_nucleo")
        if ram_ativa:
            row.update(ram_usada_bytes=ram_bytes, ram_baseline_bytes=0, ram_total_bytes=RAM_TOTAL,
                       ram_disponivel_bytes=RAM_TOTAL - ram_bytes, ram_baseline_fonte="SEM_SUBTRACAO",
                       ram_medicao_tipo="RSS_PROCESSO")
        if flash_ativa:
            row.update(flash_usada_bytes=int(flash), flash_total_bytes=FLASH_TOTAL,
                       io_leitura_contador_acum=io_leitura, io_escrita_contador_acum=io_escrita,
                       io_leitura_unidade="bytes", io_escrita_unidade="bytes")
        yield row


def configurar_s3():
    import boto3
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / "config" / ".env", override=True)
    bucket = os.getenv("BUCKET_NAME") or os.getenv("S3_BUCKET_NAME")
    if not bucket:
        raise ValueError("Defina BUCKET_NAME ou S3_BUCKET_NAME no config/.env.")

    configuracao = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
    for campo in ("aws_access_key_id", "aws_secret_access_key", "aws_session_token"):
        valor = os.getenv(campo.upper()) or os.getenv(campo)
        if valor:
            configuracao[campo] = valor
    if len(configuracao) > 1 and not {"aws_access_key_id", "aws_secret_access_key"} <= configuracao.keys():
        raise ValueError("Configure as duas chaves AWS ou use um perfil ou uma role IAM.")
    prefixo = os.getenv("S3_PREFIXO_BRONZE", "01-bronze").strip("/")
    return boto3.client("s3", **configuracao), bucket, prefixo


def amostras_desligado(fmc, inicio, fim):
    cpu, ram, flash = (fmc["componentes"][c] for c in ("CPU", "RAM", "DISCO"))
    base = {
        "hostname": fmc["hostname"], "placa_nome": fmc["nome"], "origem": "SIMULADA_BANCADA",
        "fator_tempo": 1, "modo_execucao": "voo", "fase": "OFF", "intervalo_coleta_s": 60,
        "sessao_origem": inicio.strftime("OFF-%Y%m%dT%H%M%S.%f%z"), "qtd_tarefas": 0,
    }
    if cpu:
        base.update(cpu_percent=0, cpu_pct_host=0, cpu_nucleos=CPU_NUCLEOS,
                    cpu_semantica="soma_percentual_por_nucleo")
    if ram:
        base.update(ram_usada_bytes=0, ram_baseline_bytes=0, ram_total_bytes=RAM_TOTAL,
                    ram_disponivel_bytes=RAM_TOTAL, ram_baseline_fonte="SEM_SUBTRACAO",
                    ram_medicao_tipo="RSS_PROCESSO")
    if flash:
        base.update(flash_usada_bytes=0, flash_total_bytes=FLASH_TOTAL, io_leitura_contador_acum=0,
                    io_escrita_contador_acum=0, io_leitura_unidade="bytes", io_escrita_unidade="bytes")

    instante = inicio.astimezone(timezone.utc)
    fim = fim.astimezone(timezone.utc)
    seq = 1
    while instante < fim:
        yield {**base, "timestamp_utc": instante.isoformat(), "seq": seq, "uptime_proc_s": 0}
        instante += timedelta(minutes=1)
        seq += 1


def proximas_partidas(voos, fim):
    partidas = [fim] * len(voos)
    por_host = {}
    for i in range(len(voos) - 1, -1, -1):
        inicio, fmc = voos[i]
        hostname = fmc["hostname"]
        partidas[i] = por_host.get(hostname, fim)
        por_host[hostname] = inicio
    return partidas


def publicar_voo(s3, bucket, prefixo, fmc, inicio, intervalo, seed, incidente, fim_desligado=None):
    sessao = inicio.strftime("%Y%m%dT%H%M%S%z")
    hostname = fmc["hostname"]
    base = f"{prefixo}/hostname={hostname}/sessao={sessao}/bronze_bruto_{hostname}_{sessao}"
    chave_csv = f"{base}.csv"
    with StringIO(newline="") as arquivo:
        writer = csv.DictWriter(arquivo, fieldnames=COLUNAS, delimiter=";")
        writer.writeheader()
        writer.writerows(amostras_voo(fmc, inicio, intervalo, seed, incidente))
        if fim_desligado is not None:
            inicio_off = (inicio.astimezone(timezone.utc) + DURACAO_VOO).astimezone(inicio.tzinfo)
            writer.writerows(amostras_desligado(fmc, inicio_off, fim_desligado))
        s3.put_object(Bucket=bucket, Key=chave_csv, Body=arquivo.getvalue().encode("utf-8"),
                      ContentType="text/csv; charset=utf-8")
    manifesto = f"{prefixo}/manifestos/hostname={hostname}/bronze_ativo.json"
    estado = {
        "bucket": bucket, "chave_s3": chave_csv, "s3_uri": f"s3://{bucket}/{chave_csv}",
        "chave_manifesto_s3": manifesto, "hostname": hostname,
        "sessao_id": inicio.strftime("VOO-%Y%m%dT%H%M%S.%f%z"),
        "modo": "simulado", "estado_coleta": "CONCLUIDA",
        "atualizado_em_utc": datetime.now(timezone.utc).isoformat(), "data_inicio": inicio.isoformat(),
    }
    for chave, dados in ((manifesto, estado), (f"{base}_concluido.json", {"chave_s3": chave_csv})):
        s3.put_object(Bucket=bucket, Key=chave, Body=json.dumps(dados).encode("utf-8"),
                      ContentType="application/json")
    return chave_csv


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dias", type=int, default=30, help="dias retroativos (padrão: 30)")
    parser.add_argument("--ate", type=date.fromisoformat, help="último dia em Brasília, no formato AAAA-MM-DD")
    parser.add_argument("--intervalo", type=int, default=10, help="segundos simulados entre leituras (padrão: 10)")
    parser.add_argument("--incidentes", type=int, default=6, help="total de falhas planejadas (padrão: 6)")
    parser.add_argument("--seed", type=int, default=12345)
    args = parser.parse_args()
    if args.dias < 1 or not 1 <= args.intervalo <= 60 or args.incidentes < 0:
        parser.error("Informe --dias >= 1, --intervalo entre 1 e 60 e --incidentes >= 0.")

    brasilia = ZoneInfo("America/Sao_Paulo")
    fim = datetime.now(brasilia).replace(second=0, microsecond=0)
    if args.ate:
        if args.ate > fim.date():
            parser.error("--ate deve ser uma data de hoje ou do passado.")
        if args.ate < fim.date():
            fim = datetime.combine(args.ate + timedelta(days=1), time.min, tzinfo=brasilia)
    inicio = fim - timedelta(days=args.dias)
    fmcs = [fmc for fmc in FMCs if fmc["status"] != "INATIVO" and any(fmc["componentes"].values())]
    voos = gerar_cronograma(inicio, fim, fmcs, args.seed)
    if args.incidentes > len(voos):
        parser.error(f"Há {len(voos)} voos para distribuir os incidentes.")
    incidentes = planejar_incidentes(voos, args.incidentes, args.seed, len(fmcs))
    try:
        s3, bucket, prefixo = configurar_s3()
    except ValueError as erro:
        parser.error(str(erro))

    print(f"{len(voos)} voos, {len(fmcs)} FMCs monitorados e {len(incidentes)} incidentes planejados.")
    print(f"Período em Brasília: {inicio:%d/%m/%Y %H:%M} a {fim:%d/%m/%Y %H:%M}.")
    for i, ((data, fmc), proxima_partida) in enumerate(zip(voos, proximas_partidas(voos, fim), strict=True)):
        chave = publicar_voo(s3, bucket, prefixo, fmc, data, args.intervalo, args.seed,
                             incidentes.get(i), proxima_partida)
        print(f"[{i + 1}/{len(voos)}] s3://{bucket}/{chave}")


if __name__ == "__main__":
    main()
