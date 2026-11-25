"""Publica os datasets Gold a partir de uma sessão Silver no S3."""
from datetime import datetime
from io import BytesIO

import pandas as pd

from contratos import METRICAS_ALERTAVEIS
from etl_s3 import cliente_s3, ler_objeto, objetos, particao, segmento_s3
from gold import FUSO_BRASILIA, gerar_datasets_gold
from silver import classificar_metrica


PREFIXO_SILVER = "02-silver"
SUFIXO_SILVER = "_silver.csv"


def ler_silver(s3, bucket, chave):
    hostname, sessao = particao(chave, PREFIXO_SILVER, SUFIXO_SILVER)
    tipos = {
        "hostname": str,
        "placa_serie": str,
        "placa_componentes": str,
        "ts": str,
        "sessao_origem": "string",
        "fase": str,
        "q_lacuna": "boolean",
        "aeronave_id": "Int64",
        "computador_id": "Int64",
        "empresa_id": "Int64",
    }
    for metrica in METRICAS_ALERTAVEIS:
        tipos[f"componente_id_{metrica}"] = "Int64"
        for nivel in ("atencao", "critico"):
            tipos[f"q_{metrica}_{nivel}"] = "boolean"
    silver = pd.read_csv(
        BytesIO(ler_objeto(s3, bucket, chave)), sep=";", encoding="utf-8",
        dtype=tipos, keep_default_na=False, na_values=[""], float_precision="round_trip",
    )
    silver["ts"] = pd.to_datetime(silver["ts"], format="ISO8601", utc=True)
    if silver.empty or not silver["hostname"].map(segmento_s3).eq(hostname).all():
        raise ValueError(f"Silver vazio ou com hostname divergente: {chave}")

    # CSVs anteriores à normalização ainda podem chegar com RAM acima de 100%.
    ram = silver["ram_pct"]
    if (ram.lt(0) | ram.gt(100)).any():
        silver["ram_pct"] = ram.clip(0, 100)
        classificar_metrica(silver, "ram_pct")

    return silver.assign(coleta_id=sessao)


def _inicio_coleta(sessao):
    try:
        return datetime.strptime(sessao, "%Y%m%dT%H%M%S%z")
    except ValueError:
        return None


def _chaves_do_historico(s3, bucket, hostname, chave_atual, inicio_coleta):
    datadas, sem_data = [], []
    paginas = s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{PREFIXO_SILVER}/hostname={hostname}/"
    )
    for pagina in paginas:
        for objeto in pagina.get("Contents", []):
            chave = objeto["Key"]
            if chave == chave_atual or not chave.endswith(SUFIXO_SILVER):
                continue
            _, sessao = particao(chave, PREFIXO_SILVER, SUFIXO_SILVER)
            inicio = _inicio_coleta(sessao)
            if inicio is None:
                sem_data.append(chave)
            elif inicio_coleta is None or inicio < inicio_coleta:
                datadas.append((inicio, chave))
    # A data no nome permite começar pela coleta anterior mais recente.
    for chave in sem_data:
        yield chave, False
    for _, chave in sorted(datadas, reverse=True):
        yield chave, True


def carregar_historico(s3, bucket, chave_atual, hostname, silver):
    inicio_atual = silver["ts"].min().tz_convert(FUSO_BRASILIA)
    inicio_de_ontem = (inicio_atual.normalize() - pd.DateOffset(days=1)).tz_convert("UTC")
    fim_atual = silver["ts"].max()
    inicio_coleta = _inicio_coleta(silver["coleta_id"].iat[0])
    placas_atuais = dict(iter(silver.groupby("placa_serie", sort=False)))
    placas_sem_sessao_anterior = set(placas_atuais)
    partes = [silver.iloc[:0]]
    for chave, tem_data in _chaves_do_historico(s3, bucket, hostname, chave_atual, inicio_coleta):
        passado = ler_silver(s3, bucket, chave)
        if inicio_coleta is None and passado["ts"].min() >= fim_atual:
            continue
        dados_placas = passado.loc[passado["placa_serie"].isin(placas_atuais)]
        if not dados_placas.empty:
            partes.append(dados_placas)
        for placa, dados in dados_placas.groupby("placa_serie", sort=False):
            atual = placas_atuais[placa]
            voos_atuais = atual["sessao_origem"].dropna()
            outros_voos = dados.loc[~dados["sessao_origem"].isin(voos_atuais)]
            if not outros_voos.empty and outros_voos["ts"].max() < atual["ts"].min():
                placas_sem_sessao_anterior.discard(placa)
        # As duas referências são a leitura de ontem e a sessão anterior de cada placa.
        if tem_data and not placas_sem_sessao_anterior and passado["ts"].max() < inicio_de_ontem:
            break
    return pd.concat(partes, ignore_index=True)


def processar_objeto(s3, bucket, chave):
    hostname, sessao = particao(chave, PREFIXO_SILVER, SUFIXO_SILVER)
    silver = ler_silver(s3, bucket, chave)
    historico = carregar_historico(s3, bucket, chave, hostname, silver)
    pasta = f"03-gold/hostname={hostname}/sessao={sessao}"
    saidas = []
    for nome, dados in gerar_datasets_gold(silver, historico, sessao).items():
        destino = f"{pasta}/{hostname}_{sessao}_{nome}.json"
        s3.put_object(
            Bucket=bucket, Key=destino,
            Body=dados.to_json(orient="records", date_format="iso", force_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        saidas.append(destino)
    print(f"Gold publicado: s3://{bucket}/{pasta}/ ({len(saidas)} arquivos)")
    return saidas


def lambda_handler(evento, contexto):
    saidas = []
    s3 = None
    for bucket, chave in objetos(evento, PREFIXO_SILVER, SUFIXO_SILVER):
        if s3 is None:
            s3 = cliente_s3()
        saidas.extend(processar_objeto(s3, bucket, chave))
    return {"gold": saidas}
