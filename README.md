# AirPulse — Python

Repositório responsável pelos códigos Python do projeto AirPulse.

O objetivo deste repositório é cuidar da **simulação, coleta, tratamento e carga dos dados de telemetria** usados nas análises.

---

## Estrutura do fluxo

```text
Simulação
   ↓
Agente
   ↓
Bronze
   ↓
Tratamento
   ↓
Silver / Gold
   ↓
Load
```

Cada etapa tem uma responsabilidade específica e deve evitar misturar funções de outras etapas.

---

## 1. Simulação

**Responsabilidade:** gerar dados que representem o comportamento de um FMC durante uma sessão de voo.

O código de simulação deve:

* representar as fases do voo;
* gerar variações de CPU, memória e I/O;
* criar cenários normais e de degradação;
* gerar dados em uma sequência temporal;
* salvar os dados para serem usados pelo agente/tratamento.

Exemplo:

```text
simulacao_voo.py
```

Os valores gerados pela simulação são **sintéticos**. Eles servem para testar o pipeline e reproduzir cenários de degradação de forma controlada.

---

## 2. Agente

**Responsabilidade:** coletar a telemetria do ambiente de execução.

O agente deve:

* coletar as métricas do computador;
* registrar o timestamp;
* identificar a origem da coleta;
* coletar CPU, memória, I/O e uptime;
* manter a frequência de coleta definida;
* gerar/enviar os dados para a camada Bronze.

A ideia é que o agente seja responsável pela **aquisição**, e não pela análise dos dados.

---

## 3. Bronze

**Responsabilidade:** armazenar os dados brutos coletados pelo agente.

Os dados devem permanecer próximos do formato original, evitando transformações desnecessárias nessa etapa.

Exemplo:

```text
bronze_bruto.csv
```

A Bronze deve facilitar:

* rastreabilidade;
* reprocessamento;
* auditoria;
* recuperação dos dados originais.

---

## 4. Tratamento

**Responsabilidade:** transformar os dados brutos em dados prontos para análise.

O tratamento deve:

* limpar e organizar os dados;
* corrigir tipos;
* calcular métricas derivadas;
* calcular taxas de I/O;
* calcular métricas de memória;
* identificar lacunas na coleta;
* identificar condições de CPU;
* separar os dados por sessão/fase de voo;
* gerar os resultados das camadas Silver e Gold.

Entre as métricas utilizadas estão:

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

Os cálculos e limiares devem seguir a fundamentação teórica do projeto.

---

## 5. Detecção de alertas

A etapa de tratamento também deve identificar condições de:

```text
Normal
Atenção
Crítico
```

Para evitar falsos alarmes, o processamento utiliza conceitos de **persistência** e **histerese**.

Na configuração documentada na fundamentação:

```text
PERSISTENCIA_AMOSTRAS = 3
HISTERESE_PTS = 5.0
```

Também são consideradas condições de qualidade dos dados, como lacunas na sequência de coleta.

---

## 6. Health Score e prognóstico

Depois do tratamento das métricas, o código pode consolidar os resultados em indicadores de saúde.

### Health Score

O `health_score` reúne diferentes indicadores em uma pontuação única.

A lógica utiliza penalizações diferentes para situações de atenção e situações críticas.

### Tendência de memória

A métrica:

```text
ram_tendencia_kb_h
```

é obtida por regressão linear e representa a tendência de crescimento da memória ao longo da sessão.

### RUL da Flash

A métrica:

```text
flash_autonomia_h
```

é uma estimativa de autonomia baseada na taxa de utilização observada.

---

## 7. Arquivos de saída

Os resultados tratados podem gerar arquivos como:

```text
gold_alertas.csv
gold_resumo_fmc.csv
gold_kpi_fase.csv
```

Esses arquivos representam os dados já preparados para consumo pelas próximas partes do projeto.

---

## 8. Load

**Responsabilidade:** pegar os dados tratados e carregá-los no destino definido pelo projeto.

O load não deve refazer os cálculos do tratamento.

Fluxo:

```text
Dados tratados
      ↓
     Load
      ↓
Banco / Data Lake / destino final
```

A ideia é manter **tratamento** e **persistência** separados.

---

## 9. Organização recomendada

```text
/
├── simulacao/
│   └── simulacao_voo.py
│
├── agente/
│   └── agente.py
│
├── tratamento/
│   └── tratamento.py
│
├── load/
│   └── load.py
│
├── dados/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── README.md
```

Os nomes e pastas podem mudar conforme a implementação, mas a separação das responsabilidades deve ser mantida.

---

## 10. Relação com a fundamentação teórica

As decisões de tratamento e as principais métricas do código são baseadas na fundamentação teórica do projeto, especialmente nos conceitos de:

* ISO 13374;
* ARINC 653;
* DO-178C;
* Rate Monotonic Scheduling;
* teoria de filas;
* Software Aging;
* regressão linear;
* RUL;
* Arrhenius;
* persistência e histerese.

A fundamentação completa está no documento:

**Fundamentação Teórica e Comprovação Científica das Métricas de Telemetria e KPIs em Sistemas Aviônicos (PHM).**

---

## 11. Regra principal deste repositório

Cada código deve ter uma responsabilidade clara:

```text
Simulação → gerar
Agente    → coletar
Bronze    → armazenar bruto
Tratamento → transformar e analisar
Load      → carregar
```

Isso mantém o pipeline organizado e facilita testes, manutenção e reprocessamento.
