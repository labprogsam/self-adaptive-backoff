# Pilotos

Execuções piloto usadas para calibrar e justificar os valores de referência do experimento
(Design_experimental.md, Seção 7.3). Cada piloto fica num diretório próprio e guarda tudo o
que é necessário para servir de evidência: o plano executado, a carga usada, os dados brutos
e o relatório com resultados e decisões.

| Piloto | Data | Pergunta | Principal conclusão |
|---|---|---|---|
| [01 — piloto inicial](01_piloto_inicial/README.md) | 2026-10-14 | Quantas mensagens por execução bastam? Os valores de referência são adequados? | 1000 mensagens por execução bastam; `MAX_BACKOFF` e Fator D precisam de revisão |

## Estrutura de cada piloto

```
pilots/NN_nome_do_piloto/
├── README.md     # objetivo, configuração, como reproduzir, resultados, conclusões
├── plan.json     # plano executado pelo orquestrador
├── tasks.json    # cópia da carga usada
└── results/      # saída do orquestrador e das análises
    ├── <round>/<run_id>.csv
    ├── runs_summary.csv
    └── ...
```

## Como rodar um novo piloto

A partir da raiz do repositório:

```bash
P=pilots/NN_nome_do_piloto
mkdir -p $P
# 1. escreva $P/plan.json
# 2. guarde a carga usada
cp tasks.json $P/tasks.json
# 3. rode gravando direto em $P/results
TASKS_FILE=$P/tasks.json go run ./cmd/orchestrator -plan $P/plan.json \
  -parallel 20 -metrics-dir $P/results -drain-timeout 3h
# 4. escreva $P/README.md e adicione uma linha na tabela acima
```

Os CSVs dentro de `pilots/` são versionados no git de propósito (exceção no `.gitignore`):
são a evidência das escolhas do experimento. O piloto 01 ocupa ~29MB.
