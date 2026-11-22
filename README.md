# AirPulse — Python

Repositório responsável pelos códigos Python do projeto AirPulse.

O objetivo deste repositório é cuidar da **simulação, coleta, tratamento e carga dos dados de telemetria** usados nas análises.

---

## Configurar a consulta de placas no agente

Inicie a bancada com `python local/bancadaVoo.py` e, em outro terminal,
execute `python local/agente.py`. Sem encontrar a bancada, o agente encerra.

Na inicialização, o agente envia o hostname e o login do site para
`POST /usuario/autenticar`. O Node autentica o usuário e devolve
`{"placas":["P01","P02","P03"]}` para os hosts da empresa dele, na mesma
resposta.

Configure o endereço do Node e o login no `config/.env` do agente.

```dotenv
AGENTE_API_URL=http://IP_PUBLICO_DA_EC2
AGENTE_EMAIL=seu.email@empresa.com
AGENTE_SENHA='SUA_SENHA_DO_SITE'
```

Para teste na mesma máquina do Node, use `http://127.0.0.1:3333`. Na EC2,
use o endereço da instância com a porta publicada pelo Node, se for diferente
de 80. Em HTTP, o login trafega sem criptografia. Use credenciais de teste
se a conexão passar pela internet pública.

A coleta começa somente se o Node retornar pelo menos uma placa para o
hostname do agente. Se a lista vier vazia ou a consulta falhar, o agente
encerra antes de criar a sessão de coleta.

O agente exige `BUCKET_NAME` e credenciais AWS para iniciar a coleta.
O CSV local é usado para os uploads ao S3. Se uma publicação falhar, o
agente encerra com erro e o CSV fica disponível no disco.

## Horário das sessões

Os nomes das pastas e dos arquivos de sessão usam o horário de Brasília
(`America/Sao_Paulo`) em qualquer máquina. Por exemplo, em
`sessao=20261119T093524-0300`, a sessão começou às 09:35:24 no horário de Brasília.
O identificador de voo da bancada segue o mesmo fuso. Os campos com sufixo
`_utc` continuam em UTC para o processamento da telemetria.

Para atualizar as dependências do agente e da bancada, execute na raiz do projeto
`python -m pip install -r local/requirements.txt`. A dependência `tzdata` fornece
os dados de fuso em sistemas como o Windows.

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

## Testar as Lambdas

Use **Test → Create new event** na função correspondente, dê um nome ao
evento, cole o JSON e clique em **Test**. Use objetos de uma sessão de teste
concluída. A execução manual lê e grava objetos no bucket configurado.
[Guia de testes no console da AWS](https://docs.aws.amazon.com/lambda/latest/dg/testing-functions.html)

### Teste manual da Bronze → Silver

Na função com handler `bronze_to_silver.lambda_handler`, use o evento abaixo.
Troque o exemplo pela **chave completa do CSV** no S3, incluindo as pastas
e o nome do arquivo. Use a chave sem `s3://` nem o nome do bucket.

```json
{
  "bronze_key": "01-bronze/hostname=FMC-TESTE/sessao=SESSAO-TESTE/bronze_bruto_FMC-TESTE_SESSAO-TESTE.csv"
}
```

A resposta deve trazer uma lista `silver` com a chave do arquivo
`_silver.csv` em `02-silver/`. Use essa chave para testar a segunda função.

### Teste manual da Silver → Gold

Na função com handler `silver_to_gold.lambda_handler`, cole o evento abaixo.
Troque `s3.object.key` dentro de `Records` pela chave que a primeira função
retornou. Se estiver usando outro bucket, troque também `s3.bucket.name`.

```json
{
  "Records": [
    {
      "eventSource": "aws:s3",
      "eventName": "ObjectCreated:Put",
      "s3": {
        "bucket": { "name": "airpulse-s3" },
        "object": {
          "key": "02-silver/hostname=FMC-TESTE/sessao=SESSAO-TESTE/FMC-TESTE_SESSAO-TESTE_silver.csv"
        }
      }
    }
  ]
}
```

A resposta deve trazer sete chaves em `gold`. Confira os JSONs no prefixo
`03-gold/` da mesma sessão.

### Teste do fluxo automático

Configure os dois gatilhos S3 com **All object create events**.

| Função | Prefixo | Sufixo |
| --- | --- | --- |
| Bronze → Silver | `01-bronze/` | `_concluido.json` |
| Silver → Gold | `02-silver/` | `_silver.csv` |

Rode uma nova coleta e deixe a bancada terminar com o agente em execução.
No S3, confirme o marcador `_concluido.json` na pasta Bronze da sessão, o
`_silver.csv` em Silver e os sete JSONs em Gold. O agente só publica o
marcador quando conclui o envio da sessão.
[Notificações S3 com filtro de prefixo e sufixo](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-filtering.html)

Se o fluxo parar, confira a primeira saída que falta e abra os logs da
função correspondente em **Monitor → View CloudWatch logs**.
[Guia de logs da AWS](https://docs.aws.amazon.com/lambda/latest/dg/monitoring-cloudwatchlogs-view.html)

| Sintoma | O que conferir |
| --- | --- |
| `Runtime.ImportModuleError` | Runtime Python 3.14, arquitetura x86_64, nome do handler e módulos na raiz do ZIP gerado pelo build. |
| `MySQL indisponível` | Leia a causa registrada acima do erro genérico. `Connection refused` indica que a conexão foi recusada. O erro `1044` indica falta de acesso ao banco. |
| `{"gold": []}` | Confira se o evento de criação contém a chave Silver com prefixo `02-silver/` e sufixo `_silver.csv`. |
| O Bronze tem CSV, mas a Silver não aparece | Confira se existe um marcador `_concluido.json` para a mesma sessão e se o filtro do gatilho corresponde à chave. |

## Renomear arquivos e recursos

| O que mudar | O que atualizar |
| --- | --- |
| Nome de um evento salvo em **Test** | Escolha o nome que quiser. O código usa o JSON do evento. |
| Nome da função na AWS | Se criar outra função com o novo nome, aponte os gatilhos e as referências ao ARN para ela. |
| Nome de `bronze_to_silver.py`, `silver_to_gold.py` ou `lambda_handler` | Atualize o campo **Handler** da função e gere um novo ZIP. Se mudar o nome do arquivo, atualize também a lista `ARQUIVOS_OBRIGATORIOS` em `lambda/build_zip.py`. |
| Nome de `silver.py`, `gold.py`, `contratos.py` ou `etl_s3.py` | Atualize os imports, a lista `ARQUIVOS_OBRIGATORIOS` e gere um novo ZIP. |
| Nome do bucket | Atualize o coletor, `BUCKET_NAME` das duas funções, as permissões S3, os gatilhos e os serviços que leem o bucket. |
| Prefixo ou sufixo de um objeto no S3 | Atualize o código que grava a chave, o que a lê e o filtro do gatilho. Por exemplo, `03-gold/` e `_gold_alertas.json` também são usados pelo backend de relatórios. |

O campo **Handler** segue o formato `modulo.funcao`. Ao renomear um módulo,
use um nome Python válido, como `bronze_para_silver`.
[Nome do handler na AWS](https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html#python-handler-naming)
