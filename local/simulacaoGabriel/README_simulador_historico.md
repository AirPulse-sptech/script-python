# Popular o histórico simulado

O [simulador histórico](simulador_historico.py) gera 30 dias retroativos de Bronze com amostras a cada 10 segundos simulados. Cada FMC monitorado tem um perfil próprio de CPU, RAM e Flash. O padrão distribui seis incidentes pelo histórico, com atenção e crítico, incluindo um voo recente. Os FMCs inativos continuam aparecendo no cadastro.

```powershell
python -B local/simulacaoGabriel/simulador_historico.py
python -B local/simulacaoGabriel/simulador_historico.py --dias 7 --ate 2026-11-26 --incidentes 2
```

O simulador gera um CSV por vez em memória e publica direto no S3. Configure o bucket e o acesso AWS no `config/.env` antes de rodar. Após enviar cada CSV, o simulador publica o manifesto e o marcador `_concluido.json` que dispara as Lambdas.

`--ate` fixa o último dia em Brasília, `--intervalo` muda a cadência e `--incidentes 0` gera apenas voos normais. O Silver usa os limiares cadastrados no MySQL para classificar as leituras.

Entre o fim de um voo e a próxima partida do mesmo FMC, o simulador grava uma amostra zerada por minuto com `fase=OFF` em uma sessão própria. Depois do último voo, esse período vai até o fim da simulação.
