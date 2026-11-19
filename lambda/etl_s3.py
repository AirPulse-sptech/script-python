"""Leitura de objetos e validação das chaves usadas pelas duas Lambdas."""
from contextlib import closing
import os
import re
from urllib.parse import unquote_plus

import boto3


def objetos(evento, prefixo, sufixo):
    bucket_configurado = os.getenv("BUCKET_NAME")
    if evento.get("Event") == "s3:TestEvent":
        return
    for registro in evento["Records"]:
        if registro.get("eventSource") != "aws:s3":
            raise ValueError("Evento da ETL deve vir do S3.")
        if not registro["eventName"].startswith("ObjectCreated:"):
            continue
        bucket = registro["s3"]["bucket"]["name"]
        chave = unquote_plus(registro["s3"]["object"]["key"])
        if bucket_configurado and bucket != bucket_configurado:
            raise ValueError(f"Bucket inesperado: {bucket}")
        if chave.startswith(prefixo + "/") and chave.endswith(sufixo):
            yield bucket, chave


def particao(chave, prefixo, sufixo):
    partes = chave.removeprefix(prefixo + "/").split("/")
    if len(partes) != 3 or not chave.startswith(prefixo + "/") or not chave.endswith(sufixo):
        raise ValueError(f"Chave fora do padrão da ETL: {chave}")
    host, sessao, arquivo = partes
    if not host.startswith("hostname=") or not sessao.startswith("sessao="):
        raise ValueError(f"Chave sem hostname ou sessão: {chave}")
    hostname, sessao_id = host[9:], sessao[7:]
    if not all(re.fullmatch(r"[A-Za-z0-9._-]+", valor) and valor not in (".", "..")
               for valor in (hostname, sessao_id)) or len(arquivo) <= len(sufixo):
        raise ValueError(f"Hostname, sessão ou nome de arquivo inválido: {chave}")
    return hostname, sessao_id


def ler_objeto(s3, bucket, chave):
    with closing(s3.get_object(Bucket=bucket, Key=chave)["Body"]) as corpo:
        return corpo.read()


def cliente_s3():
    return boto3.client("s3")


def segmento_s3(valor):
    segmento = re.sub(r"[^A-Za-z0-9._-]+", "_", str(valor or "desconhecido").strip())
    return segmento.strip("._-") or "desconhecido"
