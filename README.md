# self-adaptive-backoff

Serviço em Go que consome mensagens JSON de uma fila do RabbitMQ, repassa cada
mensagem via `POST` para uma API de terceiro e imprime o resultado retornado.
Em caso de erro (JSON inválido, falha de comunicação, timeout ou resposta de
erro da API), o serviço tenta novamente segundo uma **estratégia de backoff
configurável** antes de desistir da mensagem.

Este repositório é a base de código de um TCC sobre análise comparativa de
estratégias de backoff em arquiteturas orientadas a eventos — o roteiro
experimental completo (objetivos, métricas, fatores, design dos experimentos)
está em [`Design_experimental.md`](Design_experimental.md), seguindo a
metodologia de Raj Jain.

## Arquitetura

![arquitetura](image.png)

https://excalidraw.com/#json=VpGgq4FiHOlLCOuhQ3M6t,BSBUnwGBCuWeysPIMm8DvQ

- **Producer** — publica na fila do RabbitMQ as mensagens de `tasks.json`
  (1000 pedidos de ~100 bytes, fixos entre execuções), numa taxa configurável
  (Seção 7.1 do design experimental).
- **Broker (RabbitMQ)** — fila que desacopla producer e consumer.
- **Consumer** — consome uma mensagem por vez (`Qos(1)`), delega o envio ao
  `apiclient` + `backoff`, grava um registro por tentativa em CSV e decide
  `ack`/`nack` conforme o resultado.
- **backoff** (`internal/backoff`) — as seis estratégias comparadas no TCC
  (Fator A), cada uma em seu próprio arquivo, selecionáveis em runtime.
- **apisim** (`cmd/apisim`) — simulador de API de terceiro: injeta taxa de
  erro (Fator B) e padrão temporal de falha (Fator C) configuráveis, no lugar
  de uma API real — necessário para rodar os experimentos de forma
  controlada e reprodutível (Seção 7.2 do design experimental).
- **orchestrator** (`cmd/orchestrator`) — roda um plano de experimento (várias
  combinações de fatores × repetições) automaticamente: cada rodada é uma
  execução isolada e sequencial de apisim+consumer+producer (fila, porta e
  arquivo de métricas próprios), rodando em paralelo com as demais rodadas
  até um limite configurável — a concorrência fica **entre** rodadas, nunca
  dentro de uma rodada (que continua sequencial, preservando `Qos(1)` e o
  estado entre mensagens do AIMD/HPA).

## Estrutura do projeto

```
cmd/
  consumer/main.go     # entrypoint do consumidor
  producer/main.go     # entrypoint do producer
  apisim/main.go       # entrypoint do simulador de API de terceiro
  orchestrator/main.go # entrypoint do orquestrador de experimentos
internal/
  config/       # leitura de configuração (env vars) do consumer/producer
  backoff/      # as 6 estratégias do Fator A, uma por arquivo, + laço de retry genérico
  apiclient/    # cliente HTTP: valida JSON, faz o POST, classifica o tipo de erro
  consumer/     # loop de consumo do RabbitMQ + gravação de métricas por tentativa
  producer/     # leitura de tasks.json e publicação na fila, com controle de taxa
  apisim/       # lógica do simulador de API (injeção de falha e latência)
  metrics/      # writer CSV do schema "por tentativa" (Design_experimental.md, Seção 8)
  orchestrator/ # execução paralela de rodadas isoladas a partir de um plano (JSON)
experiments/
  example_plan.json # plano de exemplo (template, não é a triagem/design final)
  gen_tasks.py      # gera tasks.json (semente fixa: sempre o mesmo conteúdo)
  prefix_analysis.py # análise por prefixo: quantas mensagens por execução bastam
pilots/      # pilotos de calibração: plano, carga, dados brutos e relatório de cada um
tasks.json   # carga de trabalho do producer: 1000 mensagens de ~100 bytes (Seção 7.1)
```

- `internal/backoff` — `strategy.go` define a interface `Strategy` e a
  fábrica `New(name, Params)`; cada estratégia (`none.go`, `fixed.go`,
  `exponential.go`, `exponential_jitter.go`, `aimd.go`, `hpa.go`) implementa
  essa interface; `backoff.go` tem o laço genérico `Backoff[T]`, agnóstico de
  transporte (não depende de HTTP nem AMQP).
- `internal/apiclient` — classifica cada erro em `sucesso | erro_dado |
  timeout | erro_comunicacao` (função `Classify`), usado para o schema de
  saída por tentativa.
- `internal/metrics` — grava um CSV (`METRICS_FILE`) com um registro por
  tentativa: `run_id`, `message_id`, número da tentativa, timestamp,
  duração, tempo de espera e resultado — a fonte de dados para as métricas
  da Seção 3 do design experimental.
- `cmd/consumer`, `cmd/producer`, `cmd/apisim` — binários finos que carregam
  configuração e chamam o pacote correspondente.

## Pré-requisitos

- Go 1.22+
- Um RabbitMQ acessível (local ou via Docker)

## Configuração (variáveis de ambiente)

Os padrões de tempo estão na **escala 1/5** validada no piloto 02
(`pilots/02_escala_e_capacidade`): todas as constantes de tempo divididas por 5
em relação a valores realistas (ex.: delay base de 500ms → 100ms, latência da
API de 50ms → 10ms), o que preserva as métricas sem unidade de tempo e deixa o
experimento principal ~5× mais rápido. Os parâmetros do AIMD e do HPA são os
calibrados no piloto 03 (`pilots/03_calibracao_aimd_hpa`).

### Consumer / Producer (`internal/config`)

| Variável | Descrição | Padrão |
|---|---|---|
| `AMQP_URL` | URL de conexão do RabbitMQ | `amqp://guest:guest@localhost:5672/` |
| `QUEUE_NAME` | Nome da fila | `tasks` |
| `API_URL` | Endpoint chamado via `POST` | `http://localhost:9091/process` |
| `HTTP_TIMEOUT` | Timeout do cliente HTTP | `6s` |
| `TASKS_FILE` | Arquivo de mensagens lido pelo producer | `tasks.json` |
| `RUN_ID` | Identifica a execução no CSV de métricas | timestamp atual |
| `METRICS_FILE` | Caminho do CSV de métricas por tentativa | `metrics/attempts.csv` |

**Fator A — estratégia de backoff:**

| Variável | Descrição | Padrão |
|---|---|---|
| `BACKOFF_STRATEGY` | `none` \| `fixed` \| `exponential` \| `exponential_jitter` \| `aimd` \| `hpa` | `exponential_jitter` |
| `BASE_BACKOFF` | Delay base (estratégias estáticas) / delay inicial (AIMD, HPA) | `100ms` |
| `MAX_BACKOFF` | Teto de delay | `1600ms` |
| `MIN_BACKOFF` | Piso de delay (AIMD, HPA) | `10ms` |
| `MAX_RETRIES` | Fator E — nº máximo de tentativas extras antes do descarte | `3` |
| `AIMD_INCREMENT` | Δ+ somado ao delay a cada falha (nível 5) | `100ms` |
| `AIMD_DECREASE_FACTOR` | Multiplicador aplicado ao delay a cada sucesso (nível 5) | `0.5` |
| `HPA_SETPOINT` | Taxa de erro-alvo (nível 6) | `0.5` |
| `HPA_WINDOW_SIZE` | Tamanho da janela observável, em tentativas (nível 6) | `50` |

**Carga de trabalho do producer (Seção 7.1):**

| Variável | Descrição | Padrão |
|---|---|---|
| `PRODUCER_RATE` | Taxa de publicação, em mensagens/segundo | `2.5` |
| `PRODUCER_POISSON` | `true`: intervalos entre publicações com distribuição Poisson; `false`: intervalo fixo | `true` |

### apisim (`internal/apisim`, variáveis próprias — serviço independente)

| Variável | Descrição | Padrão |
|---|---|---|
| `APISIM_ADDR` | Endereço de escuta | `:9091` |
| `APISIM_ERROR_RATE` | Fator B — taxa de erro de base (0 a 1) | `0.1` |
| `APISIM_PATTERN` | Fator C — `uniform` \| `outage` | `uniform` |
| `APISIM_HEALTHY_WINDOW` | Duração da janela saudável, com taxa `APISIM_ERROR_RATE` (`outage`) | `2s` |
| `APISIM_OUTAGE_WINDOW` | Duração da janela de indisponibilidade, com 100% de falha (`outage`) | `2s` |
| `APISIM_LATENCY_MEAN` | Latência simulada de resposta em caso de sucesso | `10ms` |
| `APISIM_LATENCY_JITTER` | Variação aleatória (uniforme) somada à latência | `4ms` |
| `APISIM_TIMEOUT_SHARE` | Fração das falhas que viram resposta lenta (simula timeout no cliente) | `0` |
| `APISIM_TIMEOUT_DELAY` | Duração da resposta lenta | `1s` |
| `APISIM_RESET_SHARE` | Fração das falhas que viram reset abrupto de conexão (simula erro de comunicação) | `0` |
| `APISIM_ID` | Identificador devolvido por `GET /__health` (usado pelo orquestrador para saber que o apisim certo está pronto) | vazio |
| `APISIM_INFO_FILE` | Se definido, recebe um JSON com o instante de início do ciclo de falhas e a configuração efetiva | vazio |
| `APISIM_WINDOWS_FILE` | Se definido (padrão `outage`), recebe uma linha por troca de janela, com o horário da troca | vazio |

## Como executar

### 1. Suba um RabbitMQ (se ainda não tiver um)

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

A interface de gerenciamento fica disponível em `http://localhost:15672`
(usuário/senha padrão: `guest`/`guest`).

### 2. Suba o simulador de API (ou aponte `API_URL` para uma API real)

```bash
go run ./cmd/apisim
```

### 3. Rode o consumidor, escolhendo a estratégia de backoff

```bash
go mod tidy
BACKOFF_STRATEGY=exponential_jitter go run ./cmd/consumer
```

### 4. Publique mensagens de teste com o producer

O producer publica as 1000 mensagens de `tasks.json`, a `PRODUCER_RATE`
msgs/s (padrão 2,5):

```bash
go run ./cmd/producer
```

### 5. Confira as métricas coletadas

Cada tentativa de processamento vira uma linha em `METRICS_FILE` (padrão
`metrics/attempts.csv`), com o schema descrito na Seção 8 do design
experimental.

## Exemplo: rodando uma combinação de fatores (triagem)

```bash
# Terminal 1 — API simulada com 30% de erro e indisponibilidade de 2s a cada 4s
APISIM_PATTERN=outage APISIM_ERROR_RATE=0.3 go run ./cmd/apisim

# Terminal 2 — consumidor com AIMD
RUN_ID=screening_aimd_b30_outage \
BACKOFF_STRATEGY=aimd MAX_RETRIES=5 \
METRICS_FILE=metrics/screening_aimd_b30_outage.csv \
  go run ./cmd/consumer

# Terminal 3 — carga de tasks.json (1000 mensagens de ~100 bytes) a 2,5 msgs/s
RUN_ID=screening_aimd_b30_outage go run ./cmd/producer
```

Repita trocando `BACKOFF_STRATEGY`/`APISIM_*`/`RUN_ID` para cobrir as demais
combinações dos Fatores A, B e C (Seção 8 do design experimental) — ou use o
orquestrador (próxima seção) para automatizar isso.

## Orquestrador: rodando várias combinações em paralelo

Rodar as 36 combinações × 30 repetições (Seção 8) uma de cada vez, manualmente,
é inviável em tempo. O orquestrador roda um **plano** (arquivo JSON: rodadas ×
repetições) automaticamente, com paralelismo limitado **entre rodadas** — cada
rodada continua sequencial por dentro (preserva `Qos(1)` e o estado
compartilhado do AIMD/HPA; ver a discussão sobre concorrência registrada nesta
conversa/commits). Cada rodada roda isolada: porta de `apisim`, fila do
RabbitMQ e arquivo de métricas próprios, alocados automaticamente.

```bash
go run ./cmd/orchestrator \
  -plan experiments/example_plan.json \
  -parallel 4 \
  -drain-timeout 20m
```

`experiments/example_plan.json` é só um **exemplo/template** (3 rodadas, 1000
mensagens cada, parâmetros calibrados por padrão) para validar o mecanismo — não é o
plano de triagem nem o do experimento principal, que ainda dependem dos
valores de calibração da Seção 7.3 e da escolha do desenho fatorial
fracionado da triagem (Seção 8). Monte o plano real editando esse arquivo:
`shared_env` vale para todas as rodadas (é onde entra a taxa de produção
única — `PRODUCER_RATE` — decidida após a triagem, ver Seção 5); cada entrada
de `rounds` é uma combinação de fatores, com overrides em `consumer_env`
(Fator A e seus parâmetros, Fator E) e `apisim_env` (Fatores B e C).

O orquestrador conta as mensagens do arquivo de tarefas (`TASKS_FILE`, padrão
`tasks.json`) para saber quando uma rodada terminou de processar, sem depender
da API de management do RabbitMQ (ele conta, no CSV de métricas da própria
rodada, quantas mensagens distintas já têm um resultado final registrado).

Ordem das execuções: embaralhada por padrão (Seção 8 do design experimental:
ordem aleatorizada entre combinações). A semente vem de `-seed` (ou do relógio,
se omitida) e a ordem usada fica registrada em `metrics/execution_order.csv`
(posição, `run_id` e semente) — rodar de novo com a mesma `-seed` reproduz a
mesma ordem. `-shuffle=false` mantém a ordem do plano (útil só para depuração).

Saída (Seção 8 do design experimental):
- **Por tentativa:** um CSV por execução, agrupado por rodada
  (`metrics/<round_name>/<run_id>.csv`).
- **Por execução:** `metrics/runs_summary.csv`, com status, início/fim, duração,
  os níveis efetivos dos Fatores A–E (incluindo os defaults), os parâmetros
  fixados e calibrados e o instante de início do ciclo de falhas do apisim.
- **Cronograma das janelas de falha:** `metrics/failure_windows.csv`, com o
  início e o fim de cada janela saudável/indisponível de cada execução no padrão
  `outage`, registrados pelo próprio apisim no momento de cada troca (assim o
  cronograma fica alinhado com os timestamps das tentativas mesmo se o relógio
  do sistema for ajustado durante a execução).
- **Ordem das execuções:** `metrics/execution_order.csv`.

 A saída dos processos filhos é descartada por padrão; para depurar,
passe `-log-dir logs` e cada processo grava em
`logs/<run_id>_{consumer,apisim,producer}.log`.

## Análise por prefixo: quantas mensagens por execução?

Para escolher o volume de mensagens por execução no piloto (Seção 7.3), rode
algumas combinações com o `tasks.json` completo e recalcule as métricas da
Seção 3 usando só as primeiras N mensagens de cada execução:

```bash
python3 experiments/prefix_analysis.py metrics --prefixes 250,500,1000,1500,2000,3000 --tol 0.02
```

Para cada rodada, o script mostra média ± IC95% (entre execuções) de cada
métrica por N, e indica a partir de qual N as médias ficam dentro do IC95% da
referência (o maior N), ou a até `--tol` dela. `--out arquivo.csv` grava todos
os números. Funciona porque as mensagens são publicadas e processadas sempre
na mesma ordem: as N primeiras de uma execução longa equivalem a uma execução
de N mensagens. Só usa a biblioteca padrão do Python.

## Testes

```bash
go test ./...
```

Cobrem o estado do AIMD e do algoritmo do HPA (incremento/decremento,
cálculo sobre a janela observável, piso e teto), o controle de taxa do
producer (intervalo fixo e distribuição Poisson) e a
expansão de plano/detecção de rodada drenada do orquestrador. Não cobrem o
disparo real de processos (`internal/orchestrator` roda `exec.Command`) —
isso foi validado manualmente de ponta a ponta (RabbitMQ real + plano de
exemplo), não por teste automatizado.
