# AirPulse · ETL de telemetria

Este repositório simula o uso de CPU, RAM e disco de um FMC, coleta as leituras por placa e prepara os dados para os painéis e relatórios do AirPulse.

```text
Bancada → Agente → Bronze (CSV bruto) → Silver (CSV tratado) → Gold (3 JSONs)
```

- [Rodar a coleta local](#rodar-a-coleta-local)
- [Configurar as Lambdas na AWS](#configurar-as-lambdas-na-aws)
- [Testar o fluxo automático na AWS](#testar-o-fluxo-automático-na-aws)
- [Entender os dados Gold](#entender-os-dados-gold)
- [Encontrar o código](#encontrar-o-código)
- [Diagnosticar falhas](#diagnosticar-falhas)

## Rodar a coleta local

Você vai precisar de Python 3.14, acesso ao backend do AirPulse, um usuário do site com uma placa cadastrada para o seu hostname e credenciais AWS com acesso ao bucket.

1. Na raiz do repositório, instale as dependências e crie o arquivo de configuração. Reaproveite o `config/.env` se ele já existir.

   ```powershell
   python -m pip install -r local/requirements.txt
   if (-not (Test-Path config/.env)) { Copy-Item config/.env.example config/.env }
   ```

2. Preencha o `config/.env` com os dados do seu ambiente. O exemplo assume que o backend Node está na mesma máquina.

   ```dotenv
   AGENTE_API_URL=http://127.0.0.1:3333
   AGENTE_EMAIL=seu.email@empresa.com
   AGENTE_SENHA='SUA_SENHA_DO_SITE'
   BUCKET_NAME=airpulse-s3
   AWS_REGION=us-east-1
   INTERVALO_COLETA_S=10
   ```

   Se o backend estiver em outra máquina, use o endereço e a porta publicados. Use HTTP quando enviar o login pela internet. Para o S3, configure um perfil ou role acessível ao Boto3 ou informe as credenciais no `.env`. Mantenha o `.env` fora do Git.

   Por padrão, o agente usa a RAM total do computador como referência. Se precisar informar uma capacidade específica, configure `RAM_TOTAL_BYTES` com um valor em bytes compatível com a memória medida.

3. Inicie a bancada em um terminal e o agente em outro.

   ```powershell
   python local/bancadaVoo.py --acelerador 30
   ```

   ```powershell
   python local/agente.py
   ```

   Com `--acelerador 30`, o roteiro leva cerca de cinco minutos reais. O agente identifica a bancada, consulta `POST /usuario/autenticar` para obter a lista de placas e envia os CSVs ao S3. Se a bancada ou as placas não forem encontradas, a coleta encerra.

4. Deixe a bancada terminar com o agente em execução. Ao completar o envio, o agente grava o marcador `_concluido.json`. O terminal informa onde ficou o CSV local, que permanece disponível se o envio falhar.

## Configurar as Lambdas na AWS

Gere o pacote na raiz do repositório.

```powershell
python lambda/build_zip.py
```

O comando monta e valida `airpulse-etl.zip` para Python 3.14 em Linux x86_64. Envie esse mesmo ZIP para as duas funções e configure o handler e o gatilho de cada uma.

| Função | Handler | Prefixo S3 | Sufixo S3 |
| --- | --- | --- | --- |
| Bronze → Silver | `bronze_to_silver.lambda_handler` | `01-bronze/` | `_concluido.json` |
| Silver → Gold | `silver_to_gold.lambda_handler` | `02-silver/` | `_silver.csv` |

Use **All object create events** nos dois gatilhos. O marcador de conclusão inicia a etapa Bronze → Silver, e o CSV Silver inicia a etapa seguinte.

### Variáveis e permissões

| Configuração | Bronze → Silver | Silver → Gold |
| --- | --- | --- |
| Bucket | `BUCKET_NAME=airpulse-s3` | `BUCKET_NAME=airpulse-s3` |
| MySQL | `DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USER` e `DB_PASSWORD` | O Silver já traz cadastro e limites |
| Role de execução | Ler objetos Bronze e gravar objetos Silver | Listar o prefixo `02-silver/` com `s3:ListBucket`, ler os CSVs Silver e gravar objetos Gold |

Use um endereço MySQL que a Lambda consiga alcançar em `DB_HOST` e informe a porta do servidor, normalmente `3306`. Se mudar o prefixo Bronze, atualize `S3_PREFIXO_BRONZE` no agente e na primeira Lambda, o filtro do gatilho e as permissões. As Lambdas usam as próprias roles de execução para o S3.

## Testar o fluxo automático na AWS

Depois de configurar os gatilhos, rode uma coleta até o fim. No S3, procure a mesma pasta `hostname=.../sessao=...` nas três camadas.

| Camada | O que deve aparecer |
| --- | --- |
| Bronze | CSV bruto e marcador `_concluido.json` |
| Silver | CSV `_silver.csv`. Linhas sem computador ou placa cadastrada vão para `_quarentena.csv`. |
| Gold | Três arquivos JSON descritos na próxima seção |

## Entender os dados Gold

Cada coleta publica três arquivos em `03-gold/hostname=<host>/sessao=<id>/`. Processar a mesma coleta de novo grava nas mesmas chaves.

| Sufixo | Conteúdo |
| --- | --- |
| `_gold_monitoramento_placa.json` | Um registro por placa com o uso atual, horários, limites, estados, variações, Health Points e previsão do disco. A lista `sessoes` traz pico, horário e fase do pico, P95, minutos acima do crítico e diferença para o pico anterior. |
| `_gold_serie_1min.json` | Média, pico, estado na última amostra e classificação pela duração do alerta. Para CPU, os novos campos são `cpu_segundos_critico`, `cpu_segundos_atencao` e `cpu_nivel_predominante`. RAM e Flash seguem o mesmo padrão. |
| `_gold_alertas.json` | Eventos de abertura, mudança de nível e encerramento com os horários exatos. `nivel` usa o estado predominante no minuto de `ts_evento`. Se ainda não houver duração medida naquele minuto, usa o nível do evento. `limiar_violado` e `maior_nivel` descrevem a transição e a ocorrência observadas. |

No monitoramento, `sessao_id` identifica o voo ou, se faltar o ID do voo, a coleta e sua sessão interna. Os arquivos antigos de resumo e KPIs permanecem no S3 mesmo após reprocessar uma coleta.

- CPU e RAM comparam o uso com a leitura mais próxima de uma hora antes, dentro de cinco minutos de tolerância. Disco usa a última leitura válida do dia anterior no horário de Brasília. Nos JSONs, o disco usa o prefixo `flash`. Se faltar referência, a variação fica `null`.
- O uso de RAM fica entre 0% e 100%. Uma leitura que atinge ou excede a capacidade de referência aparece como 100%. O valor em bytes fica disponível no Silver.
- A variação da saúde compara a nota da última hora com a da hora anterior. A estimativa do disco usa médias por minuto das últimas 24 horas.
- Abertura e escalonamento de alertas exigem três leituras consecutivas no mesmo nível. Para recuperar, o valor precisa cair mais de cinco pontos percentuais abaixo do limite correspondente.
- A duração começa na confirmação do alerta (`ts_evento`). O Gold divide os intervalos consecutivos na virada de cada minuto e classifica pelo nível com mais segundos, com desempate em favor de `CRITICO`. Lacunas de coleta e o tempo após a última leitura ficam fora da contagem.
- O nome da pasta usa o horário de Brasília. Os horários nos registros ficam em UTC. Por exemplo, `2026-11-24T21:53:00.000Z` corresponde a **24/11/2026 às 18:53** em Brasília.

## Encontrar o código

| Arquivo | O que procurar |
| --- | --- |
| [local/bancadaVoo.py](local/bancadaVoo.py) | Fases, duração e cenários do voo simulado |
| [local/agente.py](local/agente.py) | Consulta de placas, leitura dos componentes e envio do Bronze |
| [lambda/bronze_to_silver.py](lambda/bronze_to_silver.py) e [lambda/silver.py](lambda/silver.py) | Leitura do Bronze, limpeza, vínculo com o MySQL e quarentena |
| [lambda/silver_to_gold.py](lambda/silver_to_gold.py) | Busca do histórico no S3 e publicação dos JSONs. Comece por `processar_objeto`. |
| [lambda/gold.py](lambda/gold.py) | Regras de alertas, resumo, KPIs e série por minuto. Comece por `gerar_datasets_gold`. |
| [lambda/etl_s3.py](lambda/etl_s3.py) e [lambda/contratos.py](lambda/contratos.py) | Validação dos eventos e das chaves S3, campos e métricas compartilhados |

Se renomear um módulo da Lambda, atualize os imports, o handler correspondente e a lista de arquivos obrigatórios em [lambda/build_zip.py](lambda/build_zip.py). Mudanças no bucket ou nas chaves dos objetos também precisam ser refletidas nos gatilhos, nas permissões e no backend que lê os arquivos.

## Diagnosticar falhas

Comece pela primeira saída que faltou e abra os logs da função que deveria gravá-la. [Consultar logs da Lambda no CloudWatch](https://docs.aws.amazon.com/lambda/latest/dg/monitoring-cloudwatchlogs-view.html)

| Sintoma | O que conferir |
| --- | --- |
| O agente encerra antes de coletar | A bancada precisa estar rodando e o Node precisa devolver ao menos uma placa para o hostname do agente. |
| Há CSV Bronze, mas o Silver não aparece | Procure o marcador `_concluido.json` na mesma sessão e confira o filtro do gatilho. Se houver só quarentena, confira o cadastro no MySQL. |
| `MySQL indisponível` | Confira o endereço, a porta e o acesso ao banco. O log registra a causa da falha. |
| `Runtime.ImportModuleError` | Confira o handler, Python 3.14, arquitetura x86_64 e os módulos na raiz do ZIP. |
| A função Gold retorna `{"gold":[]}` | A chave do evento precisa começar com `02-silver/` e terminar em `_silver.csv`. |
| A função Gold retorna `AccessDenied` ao buscar histórico | Confira `s3:ListBucket` no bucket e a permissão de leitura dos CSVs Silver na role de execução. |
| Uma variação aparece como `null` | Verifique se há amostras na janela de comparação. Uma coleta curta já gera pontos no gráfico, mas precisa de histórico para comparar com uma hora antes ou com o dia anterior. |
