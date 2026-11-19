# AirPulse — Python

Repositório responsável pelos códigos Python do projeto AirPulse.

O objetivo deste repositório é cuidar da **simulação, coleta, tratamento e carga dos dados de telemetria** usados nas análises.

---

## Gerar o ZIP das Lambdas

Na raiz do projeto, execute:

```powershell
python lambda/build_zip.py
```

O comando instala as dependências para Linux x86_64 com Python 3.14 e inclui
os módulos da Lambda na raiz do arquivo. Após validar a estrutura e o limite
para upload direto, ele grava `airpulse-etl.zip` na raiz do projeto. Se a
validação falhar, o ZIP anterior fica disponível.

No console da AWS, envie o mesmo ZIP para as duas funções e configure os
handlers `bronze_to_silver.lambda_handler` e `silver_to_gold.lambda_handler`.

## Variáveis de ambiente na AWS Lambda

Mantenha `config/.env` no computador para a coleta local. Na AWS, abra cada
função e vá em **Configuration → Environment variables → Edit → Add environment
variable**. Cadastre cada chave e valor separadamente e clique em **Save**.

### Bronze → Silver

| Variável | Valor |
| --- | --- |
| `BUCKET_NAME` | `airpulse-s3` |
| `DB_HOST` | Endereço do servidor MySQL acessível pela Lambda |
| `DB_PORT` | `3306`, se o MySQL usar a porta padrão |
| `DB_DATABASE` | Nome do banco do AirPulse no MySQL |
| `DB_USER` | Usuário do MySQL |
| `DB_PASSWORD` | Senha do usuário do MySQL |

Use o endereço real do servidor em `DB_HOST`. Os valores `localhost` e
`127.0.0.1` apontam para o próprio ambiente da Lambda. Se mudar o prefixo
padrão `01-bronze`, configure `S3_PREFIXO_BRONZE` com o mesmo prefixo usado
pelo coletor e pelo gatilho S3.

### Silver → Gold

Cadastre apenas `BUCKET_NAME=airpulse-s3`. O CSV Silver já contém os dados
de cadastro e os limites utilizados nessa etapa.

As duas funções acessam o S3 com suas respectivas **roles de execução**.
Mantenha as chaves AWS do `.env` local fora do ZIP.
