"""Publica os datasets Gold a partir de uma sessão Silver no S3."""
from io import BytesIO

import pandas as pd

from contratos import METRICAS_ALERTAVEIS
from etl_s3 import cliente_s3, ler_objeto, objetos, particao, segmento_s3
from gold import gerar_datasets_gold


def processar_objeto(s3, bucket, chave):
    hostname, sessao = particao(chave, "02-silver", "_silver.csv")
    tipos = {
        "hostname": str,
        "placa_serie": str,
        "placa_componentes": str,
        "ts": str,
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

    pasta = f"03-gold/hostname={hostname}/sessao={sessao}"
    saidas = []
    for nome, dados in gerar_datasets_gold(silver).items():
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
    for bucket, chave in objetos(evento, "02-silver", "_silver.csv"):
        if s3 is None:
            s3 = cliente_s3()
        saidas.extend(processar_objeto(s3, bucket, chave))
    return {"gold": saidas}
