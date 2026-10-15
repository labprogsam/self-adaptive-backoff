---
name: self-adaptive-backoff
description: Use when working on the self-adaptive-backoff project — implementing or comparing backoff strategies, touching the RabbitMQ consumer/producer pipeline or apiclient, running/extending the apisim fault simulator, running/extending the experiment orchestrator, or running/extending the experiments defined in Design_experimental.md. Covers project layout, config, conventions, and how to add a new strategy or metric without breaking the experimental design.
---

# self-adaptive-backoff

Serviço em Go (consumer + producer + apisim) que processa eventos de uma fila RabbitMQ,
repassa cada evento via `POST` a uma API de terceiro (real ou simulada), e reprocessa com
backoff em caso de erro. É a base de código de um TCC sobre análise comparativa de
estratégias de backoff em arquiteturas orientadas a eventos — o roteiro experimental
completo está em `Design_experimental.md` (metodologia de Raj Jain, 10 passos). Leia esse
arquivo antes de propor mudanças que afetem métricas, fatores ou parâmetros do experimento.

## Estrutura do projeto

```
cmd/consumer/main.go        # entrypoint do consumidor
cmd/producer/main.go        # entrypoint do producer
cmd/apisim/main.go          # entrypoint do simulador de API de terceiro
cmd/orchestrator/main.go    # entrypoint do orquestrador de experimentos
internal/config/            # config via env vars do consumer/producer (internal/config/config.go)
internal/backoff/           # as 6 estratégias do Fator A (uma por arquivo) + Backoff genérico
internal/apiclient/         # cliente HTTP: valida JSON, faz POST, classifica o erro (Classify)
internal/consumer/          # loop RabbitMQ (Qos=1), reconexão, ack/nack, grava métricas por tentativa
internal/producer/          # leitura de tasks.json, controle de taxa (Poisson/constante)
internal/apisim/            # injeção de falha (Fator B/C) e latência configuráveis
internal/metrics/           # writer do CSV "por tentativa" (Seção 8 do design experimental)
internal/orchestrator/      # roda um plano (JSON) de rodadas × repetições, paralelo entre rodadas
experiments/example_plan.json # plano de exemplo do orquestrador (template, não é o plano real)
experiments/gen_tasks.py    # gera tasks.json (semente fixa, saída idêntica a cada execução)
experiments/prefix_analysis.py # métricas da Seção 3 recalculadas por prefixo de N mensagens (piloto, Seção 7.3)
pilots/                     # pilotos de calibração (Seção 7.3): um diretório por piloto, ver pilots/README.md
tasks.json                  # carga do producer: 3000 mensagens de ~100 bytes, fixas entre execuções
Design_experimental.md      # roteiro experimental (Raj Jain) — objetivos, métricas, fatores, design
```

`internal/backoff`, arquivo por estratégia (nomes = níveis do Fator A, Seção 5):
`none.go` (1 — controle), `fixed.go` (2), `exponential.go` (3), `exponential_jitter.go`
(4), `aimd.go` (5), `hpa.go` (6). `strategy.go` define a interface `Strategy` e a fábrica
`New(name, Params)`; `backoff.go` tem o laço genérico `Backoff[T]` e o tipo `Outcome[T]`
(histórico de tentativas, usado tanto para decidir ack/nack quanto para alimentar as
métricas).

## Convenções e pontos de atenção

- **`internal/backoff` é agnóstico de transporte.** Nem `Strategy` nem `Backoff[T any]`
  importam `net/http` ou `amqp091-go` — `Backoff` recebe uma `func() (T, error)` genérica.
  Qualquer nova estratégia deve preservar essa independência.
- **Toda estratégia implementa `backoff.Strategy`** (`Name() string`, `NextDelay(attempt
  int) time.Duration`, `Observe(success bool)`). As estáticas (`None`, `Fixed`,
  `Exponential`, `ExponentialJitter`) ignoram `Observe` — o delay depende só do número da
  tentativa dentro da mensagem atual. **AIMD e HPA mantêm estado entre mensagens**: o
  delay atual persiste e é ajustado em `Observe`, não recalculado do zero a cada mensagem
  (ver nota da Seção 5 do design experimental). Por isso `consumer.Run` cria a estratégia
  **uma única vez** por processo (não por mensagem) e a reutiliza entre reconexões do
  RabbitMQ.
- **Fonte da verdade dos parâmetros é `internal/config/config.go`**, não o README — mas
  agora ambos devem bater, já que todo parâmetro experimental (Fator A–E, carga do
  producer) é configurável via env var com default documentado no README. Ao mudar um
  default em `config.go`, atualize a tabela do README na mesma edição.
- **Jitter do nível 4 é proporcional (20% do delay), não "full jitter" nem "decorrelated
  jitter"** (`exponential_jitter.go`) — preservado assim de propósito, é um tratamento do
  experimento. Não o altere para testar outra variante de jitter; crie uma nova estratégia.
- **HPA recalcula uma vez por janela observável completa, não a cada tentativa** — ver o
  comentário em `hpa.go`. Recalcular a cada `Observe` faria o ajuste compor repetidamente
  sobre a mesma leva de falhas antes da janela "girar", crescendo muito mais rápido do que
  a fórmula pretende. Se for mexer em `hpa.go`, rode `go test ./test/backoff/...`
  depois — os testes fixam esse comportamento.
- **`Qos(1)` e uma única instância de consumidor são deliberadamente fixos** — fora do
  escopo do estudo (Seção 1.3 e 5). Não alterar como "otimização" sem checar se isso
  invalida um fator já definido como controlado.
- **`nack` sem requeue em falha definitiva é comportamento fixo**, para não travar a fila
  com mensagem "envenenada" — preserve ao adicionar estratégias.
- **`apiclient.Classify` é a fonte única de classificação de erro por tentativa**
  (`sucesso | erro_dado | timeout | erro_comunicacao`) — usa erros sentinela
  (`ErrInvalidData`, `ErrTimeout`, `ErrCommunication`, `ErrAPIStatus`) com `errors.Is`. Uma
  resposta HTTP de erro (`ErrAPIStatus`, ex. 5xx) é classificada junto com erro de
  comunicação — decisão deliberada para caber no schema de 4 categorias da Seção 8; não
  crie uma 5ª categoria sem atualizar o design experimental primeiro.
- **`internal/apisim` é um serviço separado**, sem depender de `internal/config` nem
  `internal/apiclient` — tem seu próprio `LoadConfig()`. Mantenha assim: ele representa a
  "API de terceiro", não faz parte do sistema sob teste (Seção 1.3).
- Sem dependências externas além de `github.com/rabbitmq/amqp091-go` (ver `go.mod`) — tudo
  que foi construído (apisim, métricas CSV, orchestrator) usa só stdlib.
- **A concorrência do orquestrador fica só entre rodadas, nunca dentro de uma rodada.**
  Cada rodada (`internal/orchestrator/orchestrator.go`, `runExecution`) continua
  sequencial por dentro — um `apisim` + um `consumer` processando uma mensagem por vez,
  exatamente como rodar manualmente. Isso não é um detalhe de implementação incidental: é
  uma decisão deliberada, porque paralelizar dentro de uma rodada quebraria `Qos(1)` (fora
  do escopo do estudo) e tornaria não-determinística a ordem de `Observe` no AIMD/HPA — o
  que afetaria as duas estratégias com estado de um jeito que não afeta as estratégias
  estáticas, introduzindo um viés de medição assimétrico entre os níveis do Fator A que
  são exatamente o que se quer comparar. Não "otimize" isso para rodar várias mensagens da
  mesma rodada em paralelo.
- **Isolamento entre rodadas paralelas é por fila, porta e arquivo, nunca por processo
  compartilhado.** Cada `Execution` recebe seu próprio nome de fila RabbitMQ
  (`tasks_<run_id>`), sua própria porta de `apisim` (alocada dinamicamente,
  `internal/orchestrator/process.go:freePort`) e seu próprio `METRICS_FILE`. Um único
  broker RabbitMQ é compartilhado entre rodadas paralelas (filas diferentes, mesma
  conexão/broker) — isso é intencional, não suba um broker por rodada.
- **O orquestrador detecta o fim de uma rodada pelo próprio CSV de métricas**
  (`internal/orchestrator/drain.go:waitDrained`), contando mensagens distintas com
  `final_result` preenchido, não pela API de management do RabbitMQ. O total esperado vem
  do número de itens do arquivo de tarefas (`TASKS_FILE`, padrão `tasks.json`).
- **O producer sempre lê `tasks.json`** — não existe mais modo de geração sintética. O
  arquivo tem 3000 pedidos de ~100 bytes (Seção 7.1: payload pequeno e volume fixos entre
  execuções — pequeno porque nada valida o conteúdo além de ser um objeto JSON). Não o regenere com conteúdo diferente no meio de uma campanha de
  experimentos: todas as rodadas comparadas precisam da mesma carga. O arquivo é gerado
  por `experiments/gen_tasks.py` (`python3 experiments/gen_tasks.py tasks.json`).

## Como rodar localmente

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
go run ./cmd/apisim      # simulador de API de terceiro em :9091
BACKOFF_STRATEGY=exponential_jitter go run ./cmd/consumer
go run ./cmd/producer      # publica as 3000 mensagens de tasks.json
```

Ver a tabela completa de variáveis de ambiente (Fator A–E, `apisim`, carga do producer) no
README. Cada tentativa processada vira uma linha em `METRICS_FILE` (padrão
`metrics/attempts.csv`).

Para rodar várias combinações automaticamente (em vez de um terminal por processo):

```bash
go run ./cmd/orchestrator -plan experiments/example_plan.json -parallel 4
```

## Tarefas comuns neste projeto

**Adicionar uma nova estratégia de backoff (para comparação no experimento):**
1. Criar um arquivo novo em `internal/backoff/` implementando `Strategy`. Para uma
   estratégia estática, `Observe` é um no-op e `NextDelay` só olha `attempt` (veja
   `exponential.go` como modelo). Para uma estratégia adaptativa, mantenha estado interno
   atualizado em `Observe` e ignore `attempt` em `NextDelay` (veja `aimd.go`/`hpa.go`).
2. Registrar no `switch` de `strategy.go` (`New`), com um nome em snake_case — esse nome é
   o valor aceito por `BACKOFF_STRATEGY`.
3. Se a estratégia tiver parâmetros próprios, adicioná-los a `backoff.Params`, a
   `config.Config` (com env var e default), e à tabela do README.
4. Atualizar a tabela de fatores em `Design_experimental.md` (Seção 5) e, se precisar de
   calibração antes do experimento principal, a Seção 7.3.
5. Escrever um teste em `test/backoff/` (`*_test.go`, pacote `backoff_test`) cobrindo o cálculo do delay e o
   comportamento de piso/teto — veja `test/backoff/aimd_test.go`/`hpa_test.go`.

**Mexer no simulador de API (`internal/apisim`):** os Fatores B (taxa de erro) e C
(padrão temporal) já estão implementados. O Fator C tem só dois níveis, `uniform` e
`outage` (ciclos de `APISIM_HEALTHY_WINDOW` com taxa `APISIM_ERROR_RATE` seguidos de
`APISIM_OUTAGE_WINDOW` com 100% de falha) — o nível "rajada" (`burst`) foi removido do
design de propósito; a apisim recusa qualquer outro valor de `APISIM_PATTERN`. `TimeoutShare`/`ResetShare`
são opcionais (default 0) para gerar timeouts/reset de conexão reais além do 5xx simples,
úteis se for necessário diferenciar R5/R6 no schema de saída.

**Mexer na coleta de métricas (`internal/metrics`):** o schema atual é "por tentativa"
(Seção 8). Não existe hoje um registro "por execução" (run-level: cronograma das janelas
de falha, timestamp de início/fim) — isso ficou como lacuna consciente (ver conversa que
gerou esta implementação): o cronograma de falha do `apisim` é determinístico a partir de
`APISIM_PATTERN` + janelas + horário de início do processo, então pode ser reconstruído
sem precisar que o `apisim` emita eventos, mas não há hoje nenhuma automação que faça essa
correlação — ao analisar os dados, isso precisa ser calculado manualmente a partir dos
parâmetros usados em cada execução.

**Antes de mudar um parâmetro "fixado"** (BaseDelay, MaxDelay, jitter, HTTPTimeout,
Qos, nº de instâncias) — confira a Seção 5 de `Design_experimental.md`: esses valores
foram deliberadamente fixados para reduzir a dimensionalidade do experimento fatorial.
Se a mudança for necessária, atualize o documento junto, não só o código.

**Rodar um piloto de calibração:** cada piloto vive em `pilots/NN_nome/` com `plan.json`,
uma cópia do `tasks.json` usado, `results/` (rode o orquestrador com
`-metrics-dir pilots/NN_nome/results`) e um `README.md` com objetivo, configuração, como
reproduzir, resultados e decisões — siga `pilots/01_piloto_inicial/` como modelo e adicione
o piloto ao índice em `pilots/README.md`. Os CSVs de `pilots/` são versionados de propósito
(exceção no `.gitignore`): são a evidência das escolhas do experimento; não os apague nem
os mova para `metrics/`.

**Montar/editar um plano de experimento para o orquestrador:** edite um JSON com
`repetitions`, `shared_env` (vale para todas as rodadas — é onde fica `PRODUCER_RATE`, a
taxa única decidida após a triagem, Seção 5) e `rounds` (uma entrada por combinação de
fatores, com overrides em `consumer_env`/`apisim_env`/`producer_env`). Veja
`experiments/example_plan.json` como referência de formato — não é o plano real, é só
para validar o mecanismo. Ao montar o plano da triagem ou do experimento principal de
verdade, gere os `rounds` programaticamente (loop sobre os níveis dos Fatores A/B/C) em
vez de escrever as dezenas de combinações à mão.

## O que falta para rodar o experimento completo

A infraestrutura de software está pronta: estratégias, simulador, carga configurável,
coleta por tentativa, e um orquestrador que roda rodadas isoladas em paralelo a partir de
um plano. Ainda não existe neste repositório:
- O plano real (JSON) da triagem e do experimento principal — `experiments/example_plan.json`
  é só um template de 3 rodadas; falta decidir o desenho fatorial fracionado da triagem
  (Seção 8) e, depois dela, os valores calibrados (Seção 7.3: taxa de produção única,
  ganhos do AIMD, setpoint/janela do HPA) antes de gerar os `rounds` de verdade.
- Qualquer análise/agregação dos CSVs gerados (Seção 9) — hoje a saída é só o dado bruto
  por tentativa (mais o resumo de status/duração por rodada do orquestrador).
