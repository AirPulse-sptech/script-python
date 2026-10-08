
Load
```

Cada etapa tem uma responsabilidade espec¡fica e deve evitar misturar fun‡äes de outras etapas.

---

## 1. Simula‡Æo

**Responsabilidade:** gerar dados que representem o comportamento de um FMC durante uma sessÆo de voo.

O c¢digo de simula‡Æo deve:

* representar as fases do voo;
* gerar varia‡äes de CPU, mem¢ria e I/O;
* criar cen rios normais e de degrada‡Æo;
* gerar dados em uma sequˆncia temporal;
* salvar os dados para serem usados pelo agente/tratamento.

Exemplo:

```text
simulacao_voo.py
```

Os valores gerados pela simula‡Æo sÆo **sint‚ticos**. Eles servem para testar o pipeline e reproduzir cen rios de degrada‡Æo de forma controlada.

---

## 2. Agente

**Responsabilidade:** coletar a telemetria do ambiente de execu‡Æo.

O agente deve:

* coletar as m‚tricas do computador;
* registrar o timestamp;
* identificar a origem da coleta;
* coletar CPU, mem¢ria, I/O e uptime;
* manter a frequˆncia de coleta definida;
* gerar/enviar os dados para a camada Bronze.

A ideia ‚ que o agente seja respons vel pela **aquisi‡Æo**, e nÆo pela an lise dos dados.

---

## 3. Bronze

**Responsabilidade:** armazenar os dados brutos coletados pelo agente.

Os dados devem permanecer pr¢ximos do formato original, evitando transforma‡äes desnecess rias nessa etapa.

Exemplo:

```text
bronze_bruto.csv
```

A Bronze deve facilitar:

* rastreabilidade;
* reprocessamento;
* auditoria;
* recupera‡Æo dos dados originais.

---

## 4. Tratamento

**Responsabilidade:** transformar os dados brutos em dados prontos para an lise.

O tratamento deve:

* limpar e organizar os dados;
* corrigir tipos;
* calcular m‚tricas derivadas;
* calcular taxas de I/O;
* calcular m‚tricas de mem¢ria;
* identificar lacunas na coleta;
* identificar condi‡äes de CPU;
* separar os dados por sessÆo/fase de voo;
* gerar os resultados das camadas Silver e Gold.

Entre as m‚tricas utilizadas estÆo:

```text
cpu_media
cpu_std
cpu_coef_variacao
cpu_headroom_p95

ram_logica_bytes
ram_pct
ram_headroom_bytes
ram_tendencia_kb_h

io_leitura_kbps
io_escrita_kbps

temp_max
health_score
flash_autonomia_h
```

Os c lculos e limiares devem seguir a fundamenta‡Æo te¢rica do projeto.

---

## 5. Detec‡Æo de alertas

A etapa de tratamento tamb‚m deve identificar condi‡äes de:

```text
Normal
Aten‡Æo
Cr¡tico
```

Para evitar falsos alarmes, o processamento utiliza conceitos de **persistˆncia** e **histerese**.

Na configura‡Æo documentada na fundamenta‡Æo:


Banco / Data Lake / destino final
```

A ideia ‚ manter **tratamento** e **persistˆncia** separados.

---

## 9. Organiza‡Æo recomendada

```text
/
ÃÄÄ simulacao/
³   ÀÄÄ simulacao_voo.py
³
ÃÄÄ agente/
³   ÀÄÄ agente.py
³
ÃÄÄ tratamento/
³   ÀÄÄ tratamento.py
³
ÃÄÄ load/
³   ÀÄÄ load.py
³
ÃÄÄ dados/
³   ÃÄÄ bronze/
³   ÃÄÄ silver/
³   ÀÄÄ gold/
³
ÀÄÄ README.md
```

Os nomes e pastas podem mudar conforme a implementa‡Æo, mas a separa‡Æo das responsabilidades deve ser mantida.

---

## 10. Rela‡Æo com a fundamenta‡Æo te¢rica

As decisäes de tratamento e as principais m‚tricas do c¢digo sÆo baseadas na fundamenta‡Æo te¢rica do projeto, especialmente nos conceitos de:

* ISO 13374;
* ARINC 653;
* DO-178C;
* Rate Monotonic Scheduling;
* teoria de filas;
* Software Aging;
* regressÆo linear;
* RUL;
* Arrhenius;
* persistˆncia e histerese.

A fundamenta‡Æo completa est  no documento:

**Fundamenta‡Æo Te¢rica e Comprova‡Æo Cient¡fica das M‚tricas de Telemetria e KPIs em Sistemas Avi“nicos (PHM).**

---

## 11. Regra principal deste reposit¢rio

Cada c¢digo deve ter uma responsabilidade clara:

```text
Simula‡Æo  gerar
Agente     coletar
Bronze     armazenar bruto
Tratamento  transformar e analisar
Load       carregar
```

Isso mant‚m o pipeline organizado e facilita testes, manuten‡Æo e reprocessamento.
```text
PERSISTENCIA_AMOSTRAS = 3
HISTERESE_PTS = 5.0
```

Tamb‚m sÆo consideradas condi‡äes de qualidade dos dados, como lacunas na sequˆncia de coleta.

---

## 6. Health Score e progn¢stico

Depois do tratamento das m‚tricas, o c¢digo pode consolidar os resultados em indicadores de sa£de.

### Health Score

O `health_score` re£ne diferentes indicadores em uma pontua‡Æo £nica.

A l¢gica utiliza penaliza‡äes diferentes para situa‡äes de aten‡Æo e situa‡äes cr¡ticas.

### Tendˆncia de mem¢ria

A m‚trica:


     Load
      ```text
ram_tendencia_kb_h
```

‚ obtida por regressÆo linear e representa a tendˆncia de crescimento da mem¢ria ao longo da sessÆo.

### RUL da Flash

A m‚trica:

```text
flash_autonomia_h
```

‚ uma estimativa de autonomia baseada na taxa de utiliza‡Æo observada.

---

## 7. Arquivos de sa¡da

Os resultados tratados podem gerar arquivos como:

```text
gold_alertas.csv
gold_resumo_fmc.csv
gold_kpi_fase.csv
```

Esses arquivos representam os dados j  preparados para consumo pelas pr¢ximas partes do projeto.

---

## 8. Load

**Responsabilidade:** pegar os dados tratados e carreg -los no destino definido pelo projeto.

O load nÆo deve refazer os c lculos do tratamento.

Fluxo:

```text
Dados tratados
      
Silver / Gold
   
Tratamento
   
Bronze
   
Agente
   # AirPulse - Python

Reposit¢rio respons vel pelos c¢digos Python do projeto AirPulse.

O objetivo deste reposit¢rio ‚ cuidar da **simula‡Æo, coleta, tratamento e carga dos dados de telemetria** usados nas an lises.

---

## Estrutura do fluxo

```text
Simula‡Æo
   
