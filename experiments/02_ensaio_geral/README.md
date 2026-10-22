# Ensaio geral — verificação antes do experimento principal

- **Objetivo:** rodar o desenho exato do experimento principal (Seção 8) com poucas repetições,
  para confirmar o tempo estimado, o funcionamento completo do pipeline e a variação entre
  repetições antes da execução de ~7h.

## Desenho

Igual ao experimento principal, mudando só o número de repetições:

- Fatorial completo **A × B × C × E**: 108 combinações × **2 repetições** = 216 execuções.
- 1.000 mensagens por execução (`tasks.json` deste diretório, o mesmo do experimento principal).
- Parâmetros nos valores calibrados (Seção 5.2); Fator D em 2,5 msgs/s.
- Ordem embaralhada (seed `1792550938665269187`, em `results/execution_order.csv`), 20 execuções
  concorrentes.

## Arquivos

| Arquivo | Conteúdo |
|---|---|
| `gen_plan.py` / `plan.json` | Gerador e plano executado |
| `tasks.json` | Carga usada (1.000 mensagens) |
| `check.py` | Verificações deste relatório |
| `results/` | Dados brutos, `runs_summary.csv`, `failure_windows.csv`, `execution_order.csv` e `check.txt` (saída do `check.py`) |
| `run.log` | Saída do orquestrador |

## Como reproduzir

```bash
G=experiments/02_ensaio_geral
python3 $G/gen_plan.py
TASKS_FILE=$G/tasks.json go run ./cmd/orchestrator -plan $G/plan.json -concurrency 20 \
  -metrics-dir $G/results -drain-timeout 2h -seed 1792550938665269187
python3 $G/check.py
```

## Resultados

| Verificação | Resultado | OK? |
|---|---|---|
| Execuções | 216 de 216 com `status=ok`, todas com as 1.000 mensagens processadas | sim |
| Cronograma de falhas | Todas as 108 execuções com indisponibilidade têm janelas registradas; das 132.975 tentativas feitas dentro de janelas de indisponibilidade, 100% falharam | sim |
| Tempo do ensaio | 1,85 h (execução mais longa: HPA, B=50%, indisponibilidade, E=10, 50 min) | — |
| Projeção do experimento principal | ~7,4 h de trabalho dividido entre as 20 vagas; com o efeito das execuções longas que caem no fim da ordem, **~8 h** no total | sim (meta: 10 h) |
| Variação entre repetições | Desvio padrão da taxa de sucesso: mediana de 0,14 p.p., percentil 90 de 0,78 p.p.; IC95% esperado com 10 repetições: **±0,56 p.p.** | sim |

Sobre a projeção de tempo: no ensaio, o tempo real (1,85 h) ficou ~25% acima do trabalho
dividido entre as vagas (1,48 h), porque as execuções mais longas, de até 50 minutos, podem
cair no fim da ordem embaralhada e rodar com vagas ociosas. Esse atraso no fim é de no máximo
uma execução longa, então pesa menos no experimento principal, que é 5 vezes maior.

Sobre a variação: com 1.000 mensagens, as repetições variaram menos que na triagem (que usou
500): o IC95% esperado com 10 repetições ficou em ±0,56 p.p., contra ±1,05 p.p. projetado a
partir da triagem. Com só 2 repetições por combinação, essa estimativa é grosseira, mas
confirma que 10 repetições são suficientes.

## Conclusão

O pipeline está pronto para o experimento principal: todas as execuções concluíram, o
cronograma de falhas está completo e alinhado, o tempo projetado (~8 h) cabe na janela de
10 h e a variação entre repetições confirma a escolha de 10 repetições.
