"""Campos compartilhados pelas etapas Silver e Gold."""


CHAVES_BASE = ["hostname", "placa_serie"]  # Identificam qual pc e qual placa (placa = placa.numeroSerie)


CHAVES_AGRUPAMENTO = CHAVES_BASE + ["sessao"]  # Agrupa por pc, placa e sessão.

CHAVES_FMC = ["hostname"]  # Agrupam os dados do FMC por pc

BYTES_POR_KB = 1024  # Conversão de bytes para kilobytes

METRICAS_ALERTAVEIS = ("cpu_pct", "ram_pct", "flash_pct")  # Métricas monitoradas para geração de alertas.

COLUNAS_CADASTRO = [
    "computador_id", "tipo_fmc", "status_computador",
    "aeronave_id", "aeronave_nome", "aeronave_modelo", "companhia_aerea",
    "empresa_id", "empresa_nome",
]
