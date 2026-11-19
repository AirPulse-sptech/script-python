"""Transforma uma sessão Bronze concluída em Silver no S3."""
from io import BytesIO
import json
import os

import pandas as pd

from etl_s3 import cliente_s3, ler_objeto, objetos, particao, segmento_s3
from silver import carregar_cadastro_do_banco, processar_camada_silver

S3_PREFIXO_BRONZE = os.getenv("S3_PREFIXO_BRONZE", "01-bronze").strip("/")


def processar_objeto(s3, bucket, chave):
    hostname, sessao = particao(chave, S3_PREFIXO_BRONZE, ".csv")
    bronze = pd.read_csv(BytesIO(ler_objeto(s3, bucket, chave)), sep=";", encoding="utf-8")
    if bronze.empty:
        raise ValueError(f"CSV Bronze vazio: {chave}")
    if "hostname" in bronze and not bronze["hostname"].map(segmento_s3).eq(hostname).all():
        raise ValueError(f"O hostname do CSV difere da chave Bronze: {chave}")

    cadastro, componentes = carregar_cadastro_do_banco()
    if cadastro is None or cadastro.empty or componentes is None or componentes.empty:
        raise RuntimeError("Sem cadastro ou componentes ativos no MySQL.")
    silver = processar_camada_silver(bronze, cadastro, componentes)

    pasta = f"02-silver/hostname={hostname}/sessao={sessao}"
    fora_cadastro = silver["q_hostname_nao_cadastrado"] | silver["q_placa_nao_cadastrada"]
    if fora_cadastro.any():
        s3.put_object(
            Bucket=bucket, Key=f"{pasta}/{hostname}_{sessao}_quarentena.csv",
            Body=silver.loc[fora_cadastro].to_csv(sep=";", index=False).encode("utf-8"),
            ContentType="text/csv; charset=utf-8",
        )
        silver = silver.loc[~fora_cadastro].reset_index(drop=True)
    if silver.empty:
        raise ValueError(f"Nenhum hostname/placa do Bronze está cadastrado no MySQL: {chave}")

    destino = f"{pasta}/{hostname}_{sessao}_silver.csv"
    s3.put_object(
        Bucket=bucket, Key=destino, Body=silver.to_csv(sep=";", index=False).encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )
    print(f"Silver publicado: s3://{bucket}/{destino} ({len(silver)} linhas)")
    return destino


def lambda_handler(evento, contexto):
    # Invocação manual para um Bronze já carregado e confirmado como concluído.
    if "bronze_key" in evento:
        bucket = os.environ["BUCKET_NAME"]
        s3 = cliente_s3()
        return {"silver": [processar_objeto(s3, bucket, evento["bronze_key"])]}

    saidas = []
    s3 = None
    for bucket, marcador in objetos(evento, S3_PREFIXO_BRONZE, "_concluido.json"):
        host_sessao = particao(marcador, S3_PREFIXO_BRONZE, "_concluido.json")
        if s3 is None:
            s3 = cliente_s3()
        chave = json.loads(ler_objeto(s3, bucket, marcador))["chave_s3"]
        if particao(chave, S3_PREFIXO_BRONZE, ".csv") != host_sessao:
            raise ValueError(f"Marcador aponta para outra sessão: {marcador}")
        saidas.append(processar_objeto(s3, bucket, chave))
    return {"silver": saidas}
