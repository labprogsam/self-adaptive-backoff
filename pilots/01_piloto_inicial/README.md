# Piloto 01 — piloto inicial (volume de mensagens e valores de referência)

- **Etapa do design experimental:** calibração (Seção 7)
- **Pergunta principal:** quantas mensagens por execução bastam para métricas estáveis?
- **Perguntas secundárias:** o simulador gera as falhas configuradas? Executar rodadas em
  paralelo distorce as medições? Os valores de referência (`MAX_BACKOFF`, taxa do producer,
  janelas de indisponibilidade) são adequados?

## Configuração

4 combinações × 10 repetições = 40 execuções, 3000 mensagens cada, 20 execuções em paralelo.

| Combinação (`round`) | Fator A | Fator B | Fator C |
|---|---|---|---|
| `none_b50_uniform` | retry imediato | 50% | uniforme |
| `none_b50_outage` | retry imediato | 50% | indisponibilidade (10s saudável + 10s fora do ar) |
| `exponential_b50_uniform` | exponencial sem jitter | 50% | uniforme |
| `exponential_b50_outage` | exponencial sem jitter | 50% | indisponibilidade (10s saudável + 10s fora do ar) |

Parâmetros fixos:

| Parâmetro | Valor |
|---|---|
| Mensagens por execução | 3000 (`tasks.json` deste diretório, ~94 bytes cada) |
| Fator E (`MAX_RETRIES`) | 3 |
| `BASE_BACKOFF` / `MAX_BACKOFF` | 500ms / 30s |
| Fator D (`PRODUCER_RATE`) | 20 msgs/s, chegadas Poisson |
| Latência da API simulada | 50ms + variação uniforme de 0 a 20ms (padrões do `apisim`) |
| Paralelismo do orquestrador | 20 execuções simultâneas |

Combinações extremas de propósito: B=50% é o nível mais alto de erro, e as duas estratégias
são os extremos de "não esperar" e "esperar cada vez mais". AIMD e HPA ficaram de fora porque
ainda dependem de calibração.

## Arquivos

| Arquivo | Conteúdo |
|---|---|
| `plan.json` | Plano executado pelo orquestrador |
| `tasks.json` | Cópia da carga usada (3000 mensagens) |
| `results/<round>/<run_id>.csv` | Dados brutos: uma linha por tentativa (schema da Seção 8) |
| `results/runs_summary.csv` | Status, início, fim e duração de cada execução |
| `results/prefix_analysis.csv` | Saída completa da análise por prefixo |

## Como reproduzir

A partir da raiz do repositório, com o RabbitMQ rodando:

```bash
TASKS_FILE=pilots/01_piloto_inicial/tasks.json \
go run ./cmd/orchestrator -plan pilots/01_piloto_inicial/plan.json \
  -parallel 20 -metrics-dir pilots/01_piloto_inicial/results -drain-timeout 3h

python3 experiments/prefix_analysis.py pilots/01_piloto_inicial/results \
  --prefixes 250,500,750,1000,1500,2000,2500,3000 --tol 0.02 \
  --out pilots/01_piloto_inicial/results/prefix_analysis.csv
```

## Resultados

### 1. O simulador gera as falhas configuradas

No padrão uniforme com B=50%:

| Resultado | Esperado | Retry imediato | Exponencial |
|---|---|---|---|
| R1 (sucesso na 1ª tentativa) | 50% | 49,4% | 50,2% |
| R3 (4 tentativas falham) | 0,5⁴ = 6,25% | 6,5% | 6,2% |

Latência média das chamadas: 60,5ms, contra 60ms esperados (50ms + média de 10ms da variação).

No padrão de indisponibilidade com B=50% e janelas de 10s/10s, a taxa média de erro esperada
é de 75%; o retry imediato observou 74,6% das tentativas com falha.

### 2. Executar 20 rodadas em paralelo não distorceu as medições

A latência das chamadas ficou em 60,5ms em todas as combinações, tanto nas tentativas com
sucesso quanto nas com falha: não houve contenção de CPU ou do broker. A duração variou menos
de 3% entre repetições da mesma combinação.

| Combinação | Duração média | Mín. – máx. |
|---|---|---|
| `exponential_b50_outage` | 70,1 min | 67,6 – 72,0 min |
| `exponential_b50_uniform` | 43,1 min | 42,0 – 44,5 min |
| `none_b50_outage` | 7,8 min | 7,7 – 7,8 min |
| `none_b50_uniform` | 5,8 min | 5,7 – 5,9 min |

### 3. Volume: 1000 mensagens por execução bastam

Análise por prefixo (`experiments/prefix_analysis.py`): as métricas da Seção 3 foram
recalculadas usando só as N primeiras mensagens de cada execução. Um N é considerado estável
quando a média de todas as métricas fica dentro do IC95% da média com N=3000, ou a até 2% dela.

| Combinação | Estável a partir de |
|---|---|
| `exponential_b50_outage` | 250 |
| `exponential_b50_uniform` | 250 |
| `none_b50_outage` | 1000 |
| `none_b50_uniform` | 1000 |

Taxa de sucesso (média ± meia-largura do IC95%, 10 repetições):

| Combinação | N=1000 | N=3000 |
|---|---|---|
| `exponential_b50_outage` | 76,82% ± 0,89 | 76,93% ± 0,62 |
| `exponential_b50_uniform` | 93,86% ± 0,51 | 93,84% ± 0,22 |
| `none_b50_outage` | 65,00% ± 1,05 | 64,48% ± 0,22 |
| `none_b50_uniform` | 93,68% ± 0,48 | 93,51% ± 0,19 |

Com N=1000, as execuções de indisponibilidade ainda cobrem pelo menos ~8 ciclos completos de
falha (a mais curta, retry imediato, dura ~2,6 min), e a duração das execuções cai para cerca
de um terço.

### 4. Com 3 retentativas, `MAX_BACKOFF` = 30s nunca é atingido

A maior espera observada foi de 2000ms: o terceiro delay do exponencial (500ms, 1s, 2s).

| Fator E | Maior delay possível | Atinge 30s? |
|---|---|---|
| 3 | 2s | não |
| 5 | 8s | não |
| 10 | 30s, a partir da 7ª tentativa | sim |

### 5. Com 20 msgs/s, o consumer opera sempre saturado

Vazão do consumer (3000 mensagens / duração média):

| Combinação | Vazão |
|---|---|
| `none_b50_uniform` | 8,6 msgs/s |
| `none_b50_outage` | 6,4 msgs/s |
| `exponential_b50_uniform` | 1,16 msgs/s |
| `exponential_b50_outage` | 0,71 msgs/s |

O producer publica a 20 msgs/s: a fila cresce em todas as combinações, e a duração da
execução é determinada pelo consumer. Os níveis previstos para o Fator D (5, 20 e 50 msgs/s)
estão todos acima da capacidade do consumer quando há backoff.

### 6. Primeiro resultado sobre as estratégias (preliminar)

N=3000, 10 repetições:

| | Retry imediato | Exponencial | Diferença |
|---|---|---|---|
| Uniforme — taxa de sucesso | 93,5% | 93,8% | nenhuma |
| Uniforme — espera de backoff por mensagem | 0 | 747ms | só custo |
| Uniforme — tentativas que falharam | 50,5% | 49,9% | nenhuma |
| Indisponibilidade — taxa de sucesso | 64,5% | 76,9% | +12,4 p.p. |
| Indisponibilidade — chamadas à API por mensagem | 2,54 | 2,26 | −11% |
| Indisponibilidade — espera de backoff por mensagem | 0 | 1263ms | custo |
| Indisponibilidade — tentativas que falharam | 74,6% | 66,0% | −8,6 p.p. |

Com falhas independentes no tempo, o backoff só acrescenta espera. Com falhas concentradas no
tempo, ele aumenta a taxa de sucesso e reduz a carga sobre a API, porque as novas tentativas
caem depois da janela de indisponibilidade. É uma interação entre os Fatores A e C.

## Conclusões e decisões

| # | Conclusão | Decisão |
|---|---|---|
| 1 | O simulador reproduz as taxas de falha configuradas | Usar o `apisim` como está |
| 2 | Paralelismo de 20 execuções não altera a latência medida | Permitir paralelismo entre execuções no experimento principal |
| 3 | Métricas estáveis a partir de 1000 mensagens | Usar 1000 mensagens por execução |
| 4 | Com E=3 ou E=5, `MAX_BACKOFF`=30s não tem efeito | Rever `MAX_BACKOFF` (ex.: 4s ou 8s) ou registrar que só atua com E=10 |
| 5 | Com 20 msgs/s, o consumer fica sempre saturado | Recalibrar os níveis do Fator D antes da triagem, ou registrar que o sistema opera saturado |
| 6 | Janelas de 10s/10s produzem a diferença esperada entre padrões | Manter janelas de 10s/10s |
| 7 | O efeito do backoff depende do padrão temporal de falha | Manter o Fator C (uniforme × indisponibilidade) |

As decisões 4 e 5 ainda estão em aberto.

## Limitações

- AIMD e HPA não foram testados: confirmar N=1000 com eles depois de calibrados.
- Só um nível de B (50%) e de E (3).
- 10 repetições por combinação; o experimento principal terá 30.
