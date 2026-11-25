"""Empacota os handlers do AirPulse para AWS Lambda Python 3.14 x86_64."""

from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile


LIMITE_UPLOAD_BYTES = 50_000_000
ARQUIVOS_OBRIGATORIOS = {
    "bronze_to_silver.py",
    "silver_to_gold.py",
    "silver.py",
    "gold.py",
    "contratos.py",
    "etl_s3.py",
    "boto3/__init__.py",
    "numpy/__init__.py",
    "pandas/__init__.py",
    "pymysql/__init__.py",
    "tzdata/zoneinfo/America/Sao_Paulo",
}


def main():
    pasta_lambda = Path(__file__).resolve().parent
    raiz = pasta_lambda.parent
    destino = raiz / "airpulse-etl.zip"
    fontes = sorted(p for p in pasta_lambda.glob("*.py") if p.name != Path(__file__).name)
    nomes_fontes = {fonte.name for fonte in fontes}

    # A área de trabalho fica no mesmo volume do ZIP para publicar com uma troca atômica.
    with TemporaryDirectory(prefix=".airpulse-build-", dir=raiz) as temporario:
        build = Path(temporario)
        dependencias = build / "deps"
        subprocess.run(
            [
                sys.executable, "-m", "pip", "install", "--no-compile",
                "--platform", "manylinux_2_28_x86_64",
                "--platform", "manylinux2014_x86_64",
                "--implementation", "cp", "--python-version", "3.14",
                "--only-binary=:all:", "--target", str(dependencias),
                "-r", str(pasta_lambda / "requirements.txt"),
            ],
            check=True,
        )

        candidato = build / destino.name
        with ZipFile(candidato, "w", compression=ZIP_DEFLATED, compresslevel=6) as pacote:
            for fonte in fontes:
                pacote.write(fonte, fonte.name)
            for caminho in sorted(dependencias.rglob("*")):
                relativo = caminho.relative_to(dependencias)
                if caminho.is_file() and "__pycache__" not in relativo.parts and caminho.suffix != ".pyc":
                    nome = relativo.as_posix()
                    if nome in nomes_fontes:
                        raise SystemExit(f"Nome duplicado entre código e dependências: {nome}")
                    pacote.write(caminho, nome)

            nomes = set(pacote.namelist())
            ausentes = ARQUIVOS_OBRIGATORIOS - nomes
            if ausentes:
                raise SystemExit(f"Pacote incompleto: {', '.join(sorted(ausentes))}")
            for biblioteca in ("numpy", "pandas"):
                if not any(
                    nome.startswith(biblioteca + "/") and nome.endswith("cpython-314-x86_64-linux-gnu.so")
                    for nome in nomes
                ):
                    raise SystemExit(f"Dependência incompatível com Python 3.14 x86_64: {biblioteca}")

        tamanho = candidato.stat().st_size
        if tamanho >= LIMITE_UPLOAD_BYTES:
            raise SystemExit(f"O pacote ficou com {tamanho / 1_000_000:.1f} MB e ultrapassa o limite de upload direto.")
        candidato.replace(destino)

    print(f"ZIP gerado: {destino} ({tamanho / 1_000_000:.1f} MB)")


if __name__ == "__main__":
    main()
