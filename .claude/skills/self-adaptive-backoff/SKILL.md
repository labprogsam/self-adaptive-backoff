---
name: self-adaptive-backoff
description: Use when working on the self-adaptive-backoff project — implementing or comparing backoff strategies, touching the RabbitMQ consumer/producer pipeline or apiclient, building the API fault simulator, or running/extending the experiments defined in Design_experimental.md. Covers project layout, config, conventions, and how to add a new strategy or metric without breaking the experimental design.
---

# self-adaptive-backoff

Serviço em Go (consumer + producer) que processa eventos de uma fila RabbitMQ,
repassa cada evento via `POST` a uma API de terceiro, e reprocessa com backoff em caso
de erro. É a base de código de um TCC sobre análise comparativa de estratégias de
backoff em arquiteturas orientadas a eventos — o roteiro experimental completo está em
`Design_experimental.md` (metodologia de Raj Jain, 10 passos). Leia esse arquivo antes
de propor mudanças que afetem métricas, fatores ou parâmetros do experimento.

## Estrutura do projeto

```
cmd/consumer/main.go   # entrypoint do consumidor
cmd/producer/main.go   # entrypoint do producer
internal/config/       # leitura de config via env vars (internal/config/config.go)
internal/backoff/      # retry genérico (via generics), sem dependência de HTTP/AMQP
internal/apiclient/    # cliente HTTP: valida JSON e faz POST síncrono à API
internal/consumer/     # loop RabbitMQ (Qos(1)), reconexão, ack/nack, chama apiclient+backoff
internal/producer/     # lê tasks.json e publica na fila
tasks.json              # payloads de exemplo para o producer
Design_experimental.md # roteiro experimental (Raj Jain) — objetivos, métricas, fatores, design
```

## Convenções e pontos de atenção

- **`internal/backoff` é agnóstico de transporte.** `Retry[T any]` recebe uma
  `func() (T, error)` — não deve importar `net/http` nem `amqp091-go`. Qualquer nova
  estratégia de backoff deve manter essa independência para continuar testável de forma
  isolada.
- **Fonte da verdade dos parâmetros de backoff é `internal/config/config.go`**, não o
  README. Antes de citar valores padrão (MaxRetries, BaseBackoff, MaxBackoff,
  HTTPTimeout) em código, testes ou no TCC, confira o `config.go` atual — já houve
  divergência entre o texto do README e o valor real de `MaxRetries` (README dizia 5,
  código usa 3); mantenha os dois sincronizados ao editar qualquer um.
- **Jitter atual é proporcional (20% do delay), não "full jitter" nem "decorrelated
  jitter"** (`backoff.go`: `d * 0.2 * rand.Float64()`). Se for implementar outras
  variantes de jitter para a comparação do TCC, não sobrescreva essa função — crie
  estratégias adicionais e selecionáveis (ver seção abaixo), para preservar a estratégia
  atual como um dos tratamentos do experimento.
- **`Qos(1)` e uma única instância de consumidor são deliberadamente fixos** — estão fora
  do escopo do estudo (ver `Design_experimental.md`, Seção 1.3 e 5). Não alterar como
  "otimização" sem checar se isso invalida um fator já definido como controlado.
- **`nack` sem requeue em falha definitiva é comportamento fixo**, para não travar a fila
  com mensagem "envenenada" — preserve esse comportamento ao adicionar estratégias.
- Sem dependências externas além de `github.com/rabbitmq/amqp091-go` (ver `go.mod`) —
  ao propor um simulador de API ou coleta de métricas, prefira `net/http`/stdlib antes de
  introduzir uma nova dependência.

## Como rodar localmente

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
# API de terceiro precisa estar no ar em API_URL (padrão http://localhost:9091/process)
go run ./cmd/consumer
go run ./cmd/producer   # publica tasks.json na fila
```

Variáveis de ambiente relevantes: `AMQP_URL`, `QUEUE_NAME`, `API_URL`, `TASKS_FILE` (ver
tabela no README; parâmetros de backoff ainda não são configuráveis via env — estão
hardcoded em `config.Load()`).

## Tarefas comuns neste projeto

**Adicionar uma nova estratégia de backoff (para comparação no experimento):**
1. Implementar como uma função de delay compatível com o padrão de `internal/backoff`
   (mesma assinatura de `delay(base, maxDelay time.Duration, attempt int) time.Duration`),
   sem tocar na estratégia exponencial-com-jitter existente — vale para as estratégias
   estáticas (sem backoff, fixo, exponencial sem/com jitter), que são *stateless*: o delay
   depende só do número da tentativa dentro daquela mensagem.
2. Tornar a estratégia selecionável via `Options` (ex.: um campo `Strategy` ou uma
   função injetada), não via variável global — cada execução de experimento precisa
   poder fixar a estratégia de forma determinística.
3. **AIMD e o algoritmo do HPA (Fator A, níveis 5 e 6) são diferentes: exigem estado
   compartilhado entre mensagens**, não só entre tentativas da mesma mensagem — o delay
   atual é ajustado incrementalmente a partir de uma taxa de erro observada numa janela
   observável, então não cabem na assinatura stateless de `delay()`. Implementar como
   um componente à parte (ex.: um `Controller` com estado, injetado no `consumer` e
   atualizado a cada resultado de mensagem), não force-se a encaixar no formato atual de
   `backoff.Options`.
4. Atualizar a tabela de fatores em `Design_experimental.md` (Seção 5) se a estratégia
   corresponder a um dos níveis já previstos ali (sem backoff, fixo, exponencial sem/com
   jitter, AIMD, algoritmo do HPA) — e a Seção 4.1, que já lista os parâmetros de AIMD
   (incremento/decremento) e do algoritmo do HPA (setpoint, janela observável). O HPA usa a
   fórmula original do Kubernetes sem ganho adicional (`Kp` foi removido por decisão do
   orientador, para manter fidelidade ao algoritmo original) — não reintroduza um termo de
   ganho sem atualizar o documento primeiro.

**Construir o simulador de API de terceiro** (previsto na Seção 7.2 do
`Design_experimental.md`, ainda não implementado no código): serviço HTTP simples,
configurável por env var ou endpoint de controle, para taxa de erro, padrão temporal de
falha (aleatório/rajada/indisponibilidade total) e distribuição de latência — mantenha-o
como um binário/pacote separado (ex.: `cmd/apisim`), nunca dentro de `internal/apiclient`.

**Instrumentar métricas de experimento:** as métricas primárias (Seção 3 do design
experimental) exigem granularidade por mensagem — timestamps de cada tentativa,
resultado final (R1–R6), contagem de tentativas. Ao adicionar logging/telemetria,
prefira emitir um registro estruturado (JSON/CSV) por mensagem em vez de apenas logs de
texto livre (`log.Printf` atual em `backoff.go` é só para debug interativo, não serve
como fonte de dados de experimento).

**Antes de mudar um parâmetro "fixado"** (BaseDelay, MaxDelay, jitter, HTTPTimeout,
Qos, nº de instâncias) — confira a Seção 5 de `Design_experimental.md`: esses valores
foram deliberadamente fixados para reduzir a dimensionalidade do experimento fatorial.
Se a mudança for necessária, atualize o documento junto, não só o código.
