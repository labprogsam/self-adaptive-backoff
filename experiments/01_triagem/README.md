# Triagem (fase 1) — efeito do Fator E

- **Etapa do design experimental:** fase 1, triagem (Seção 8)
- **Pergunta:** o número máximo de tentativas extras (Fator E) e suas interações têm efeito
  relevante sobre as métricas? Se não, E é fixado em 5 no experimento principal.

## Desenho

Fatorial completo **A × B × C × E**: 6 × 3 × 2 × 3 = 108 combinações × 3 repetições = 324
execuções, 500 mensagens cada (`tasks.json` deste diretório), em ordem embaralhada (seed
`1792355972321381106`, em `results/execution_order.csv`), 20 execuções concorrentes.

| Fator | Níveis |
|---|---|
| A — estratégia | none, fixed, exponential, exponential_jitter, aimd, hpa |
| B — taxa de erro | 10%, 30%, 50% |
| C — padrão temporal | uniforme, indisponibilidade (2s/2s) |
| E — tentativas extras | 3, 5, 10 |

Demais parâmetros nos defaults calibrados (escala 1/5, pilotos 02 e 03); Fator D fixado em
2,5 msgs/s.

**Critério (Seção 8):** medir quanto das diferenças entre os resultados vem do número de
tentativas (E), sozinho ou combinado com a estratégia. Se fosse menos de 5% em todas as
métricas, E teria pouca influência e seria fixado em 5.

## Arquivos

| Arquivo | Conteúdo |
|---|---|
| `gen_plan.py` / `plan.json` | Gerador e plano executado |
| `tasks.json` | Carga usada (500 mensagens; a mesma do piloto 03) |
| `analyze.py` | Alocação de variação (somas de quadrados do fatorial completo balanceado) e aplicação do critério |
| `results/<round>/<run_id>.csv` | Dados brutos por tentativa |
| `results/runs_summary.csv` | Registro por execução (níveis efetivos dos fatores, parâmetros, início do ciclo do apisim) |
| `results/failure_windows.csv` | Cronograma das janelas de indisponibilidade de cada execução |
| `results/execution_order.csv` | Ordem embaralhada e semente |
| `results/alocacao_variacao.csv` | Saída do `analyze.py` |
| `run.log` | Saída do orquestrador |

## Como reproduzir

```bash
T=experiments/01_triagem
python3 $T/gen_plan.py
TASKS_FILE=$T/tasks.json go run ./cmd/orchestrator -plan $T/plan.json -concurrency 20 \
  -metrics-dir $T/results -drain-timeout 1h -seed 1792355972321381106
python3 $T/analyze.py --out $T/results/alocacao_variacao.csv
```

## Resultados

Todas as 324 execuções terminaram com `status=ok`; desenho balanceado (108/108 combinações,
3 repetições cada).

### Por que E não foi fixado

**Resumo:** o número de tentativas muda qual estratégia se sai melhor. Se E fosse fixado, a
conclusão do estudo dependeria do valor escolhido.

Separamos as diferenças entre os resultados de acordo com a causa: a estratégia, a taxa de
erro, o padrão de falha, o número de tentativas e as combinações entre eles. A parte que vem
do número de tentativas (sozinho ou combinado com a estratégia) foi:

| Métrica | Parte que vem de E | Limite |
|---|---|---|
| Taxa de sucesso | 8,5% | 5% |
| Tentativas por mensagem | 10,4% | 5% |
| Espera de backoff por mensagem | 1,9% | 5% |

Em duas das três métricas, o efeito passou do limite, então E não foi fixado.

A variação entre repetições da mesma combinação foi muito pequena (menos de 1% do total):
quase toda a diferença nos resultados vem dos fatores, não do acaso.

<details>
<summary>Tabela completa: quanto cada fator e cada combinação explica</summary>

Porcentagem da variação total explicada por cada fator e combinação de fatores ("A×E" é a
combinação entre estratégia e número de tentativas, e assim por diante). A taxa de sucesso e
o descarte são complementares e têm os mesmos valores; tentativas e chamadas à API por
mensagem também são iguais.

| Efeito | Taxa de sucesso | Espera de backoff por msg | Tentativas por msg |
|---|---|---|---|
| A | 25,06% | 13,64% | 18,32% |
| B | 1,36% | 17,02% | 12,74% |
| C | 33,27% | 16,24% | 29,28% |
| **E** | **7,19%** | 1,10% | 2,71% |
| A×B | 0,09% | 15,20% | 0,13% |
| A×C | 24,91% | 11,25% | 18,67% |
| **A×E** | 1,35% | 0,77% | **7,64%** |
| B×C | 0,12% | 5,78% | 0,08% |
| B×E | 0,77% | 0,95% | 0,15% |
| C×E | 4,07% | 0,69% | 2,34% |
| A×B×C | 0,09% | 14,61% | 0,13% |
| A×B×E | 0,04% | 0,69% | 0,04% |
| A×C×E | 1,38% | 0,42% | 7,44% |
| B×C×E | 0,02% | 0,56% | 0,05% |
| A×B×C×E | 0,03% | 0,49% | 0,07% |
| erro (variação entre repetições) | 0,23% | 0,59% | 0,21% |

</details>

### Onde o efeito de E aparece

Taxa de sucesso (%), média sobre os níveis de B:

| Estratégia | Uniforme E=3 | E=5 | E=10 | Indisponibilidade E=3 | E=5 | E=10 |
|---|---|---|---|---|---|---|
| none | 97,6 | 99,3 | 100,0 | 50,1 | 50,9 | 53,4 |
| fixed | 97,6 | 99,4 | 100,0 | 62,5 | 75,9 | 90,2 |
| exponential | 97,7 | 99,4 | 100,0 | 79,4 | 96,4 | 99,5 |
| exponential_jitter | 97,7 | 99,3 | 100,0 | 80,1 | 96,2 | 99,0 |
| aimd | 97,5 | 99,4 | 100,0 | **85,2** | 90,5 | 99,2 |
| hpa | 97,9 | 99,4 | 100,0 | 80,5 | 89,5 | 97,6 |

Tentativas por mensagem, média sobre os níveis de B:

| Estratégia | Uniforme E=3 | E=5 | E=10 | Indisponibilidade E=3 | E=5 | E=10 |
|---|---|---|---|---|---|---|
| none | 1,5 | 1,5 | 1,5 | 2,7 | 3,7 | **6,1** |
| fixed | 1,5 | 1,5 | 1,5 | 2,5 | 2,7 | 3,0 |
| exponential | 1,5 | 1,5 | 1,5 | 2,1 | 1,9 | 1,9 |
| exponential_jitter | 1,5 | 1,5 | 1,5 | 2,0 | 1,9 | 2,0 |
| aimd | 1,5 | 1,5 | 1,5 | 1,9 | 2,0 | 2,1 |
| hpa | 1,5 | 1,5 | 1,5 | 2,0 | 2,2 | 2,3 |

Espera de backoff por mensagem (ms), média sobre os níveis de B:

| Estratégia | Uniforme E=3 | E=5 | E=10 | Indisponibilidade E=3 | E=5 | E=10 |
|---|---|---|---|---|---|---|
| none | 0 | 0 | 0 | 0 | 0 | 0 |
| fixed | 48 | 52 | 49 | 146 | 175 | 195 |
| exponential | 74 | 123 | 131 | 221 | 364 | 522 |
| exponential_jitter | 81 | 140 | 140 | 231 | 431 | 630 |
| aimd | 117 | 126 | 125 | 405 | 456 | 510 |
| hpa | 40 | 34 | 33 | 744 | 877 | 943 |

Leitura:

1. **No padrão uniforme, E quase não diferencia as estratégias**: todas sobem de ~97,6% para
   100% de sucesso de E=3 para E=10, e as tentativas por mensagem não mudam.
2. **Na indisponibilidade, E muda a ordem das estratégias.** Com E=3, o AIMD é a melhor
   estratégia (85,2%), à frente do exponencial (79,4%); com E=5, o exponencial passa à
   frente (96,4% contra 90,5%); com E=10, as quatro estratégias com espera crescente ficam
   entre 97,6% e 99,5%. Fixar um único nível de E determinaria qual estratégia "vence".
3. **O retry imediato não melhora com mais tentativas na indisponibilidade** (50% → 53%),
   mas multiplica a carga sobre a API (2,7 → 6,1 tentativas por mensagem): é o efeito de
   "tempestade de retentativas".
4. **E custa pouco tempo:** a duração média por execução foi de 3,8 min (E=3), 4,0 min (E=5)
   e 4,3 min (E=10).

### Precisão por número de repetições

Desvio padrão da taxa de sucesso entre as 3 repetições de cada combinação: mediana de 0,31
p.p., percentil 90 de 1,47 p.p. e máximo de 2,93 p.p. Meia-largura esperada do IC95% da
taxa de sucesso (usando o percentil 90 do desvio):

| Repetições | IC95% |
|---|---|
| 10 | ±1,05 p.p. |
| 15 | ±0,82 p.p. |
| 20 | ±0,69 p.p. |
| 30 | ±0,55 p.p. |

### Tempo estimado do experimento principal

1000 mensagens por execução (duração da triagem × 2), 20 execuções concorrentes:

| Opção | n=10 | n=15 | n=20 | n=30 |
|---|---|---|---|---|
| E fixado em 5 (36 combinações) | 2,4 h | 3,6 h | 4,8 h | 7,2 h |
| E fixado em 10 (36 combinações) | 2,6 h | 3,9 h | 5,1 h | 7,7 h |
| E como fator, níveis {3, 10} (72 combinações) | 4,8 h | 7,2 h | 9,6 h | 14,5 h |
| E como fator, níveis {3, 5, 10} (108 combinações) | 7,2 h | 10,8 h | 14,4 h | 21,6 h |

## Conclusões e decisões

| # | Conclusão | Decisão |
|---|---|---|
| 1 | O número de tentativas pesa mais que o limite de 5% na taxa de sucesso (8,5%) e nas tentativas por mensagem (10,4%) | E não é fixado |
| 2 | Na indisponibilidade, o número de tentativas muda qual estratégia é a melhor | E é variado no experimento principal, com os níveis 3, 5 e 10 (108 combinações) |
| 3 | Repetições da mesma combinação dão resultados muito parecidos | 10 repetições por combinação bastam (~7,2h no total) |
