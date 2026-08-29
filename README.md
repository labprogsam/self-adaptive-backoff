# self-adaptive-backoff

Serviço em Go que consome mensagens JSON de uma fila do RabbitMQ, repassa cada
mensagem via `POST` para uma API local (porta `9091`) e imprime o resultado
retornado. Em caso de erro (JSON inválido, falha de comunicação, timeout ou
resposta de erro da API), o serviço tenta novamente usando **backoff
exponencial** antes de desistir da mensagem.

## Arquitetura

![arquitetura](image.png)

https://excalidraw.com/#json=mryVBov8aefkTbzBQ_hP-,pJGGjYXRunkoADG37VzG6A

- **Producer** — lê `tasks.json` e publica cada mensagem na fila do RabbitMQ.
- **Broker (RabbitMQ)** — fila `tasks` que desacopla producer e consumer.
- **Consumer** — consome uma mensagem por vez (`Qos(1)`), delega o envio ao `apiclient` e decide `ack`/`nack` conforme o resultado.
- **backoff** (`internal/backoff`) — componente isolado e genérico responsável pelas tentativas de reenvio; é o ponto principal onde serão exploradas variações da estratégia (exponencial atual, jitter, tetos, número de tentativas etc.).
- **API externa** — serviço HTTP (`API_URL`) que efetivamente processa a tarefa; a comunicação com ela é o gatilho para acionar o backoff em caso de falha.

## Como funciona

1. Conecta ao RabbitMQ e consome mensagens da fila (uma por vez, via `Qos(1)`).
2. Para cada mensagem:
   - Valida se o corpo é um JSON válido (`unmarshal`).
   - Faz um `POST` síncrono para a API (`API_URL`), aguardando a resposta.
   - Em caso de sucesso (`2xx`), imprime o resultado em tela e confirma o
     processamento da mensagem (`ack`).
3. Se qualquer etapa falhar, a mensagem é reprocessada com backoff
   exponencial: espera inicial de 500ms, dobrando a cada tentativa (com
   jitter e teto de 30s), até 5 tentativas extras. Se todas falharem, a
   mensagem é descartada (`nack` sem requeue) para não travar a fila com uma
   mensagem "envenenada".
4. Se a conexão com o RabbitMQ cair, o serviço reconecta automaticamente.

Há também um producer, que lê um arquivo JSON na raiz do projeto
(`tasks.json` por padrão) contendo uma lista de mensagens e publica cada uma
delas na fila — útil para popular a fila e testar o consumidor. Sem
dependências além do cliente oficial do RabbitMQ
(`github.com/rabbitmq/amqp091-go`).

## Estrutura do projeto

```
cmd/
  consumer/main.go   # entrypoint do consumidor
  producer/main.go   # entrypoint do producer
internal/
  config/    # leitura de configuração (env vars)
  backoff/   # retry com backoff exponencial (genérico, reutilizável)
  apiclient/ # chamada HTTP POST à API local
  consumer/  # loop de consumo do RabbitMQ
  producer/  # leitura do arquivo de tasks e publicação na fila
tasks.json   # exemplo de lista de mensagens para o producer
```

- `internal/config` — carrega `AMQP_URL`, `QUEUE_NAME`, `API_URL`, `TASKS_FILE`
  e os parâmetros de backoff a partir de variáveis de ambiente.
- `internal/backoff` — implementação genérica (via generics) de retry com
  backoff exponencial e jitter, sem depender de RabbitMQ ou HTTP.
- `internal/apiclient` — cliente HTTP responsável por validar o JSON e fazer
  o `POST` síncrono para a API.
- `internal/consumer` — conecta ao RabbitMQ, consome a fila, reconecta em
  caso de queda, e usa `apiclient` + `backoff` para processar cada mensagem.
- `internal/producer` — lê o arquivo de tasks e publica cada item na fila.
- `cmd/consumer` e `cmd/producer` — binários finos que apenas carregam a
  configuração e chamam o pacote correspondente.

## Pré-requisitos

- Go 1.22+
- Um RabbitMQ acessível (local ou via Docker)
- Uma API rodando em `http://localhost:9091` que aceite `POST` com um corpo
  JSON e responda com status `2xx` em caso de sucesso

## Configuração (variáveis de ambiente)

| Variável     | Descrição                          | Padrão                                 |
|--------------|-------------------------------------|-----------------------------------------|
| `AMQP_URL`   | URL de conexão do RabbitMQ          | `amqp://guest:guest@localhost:5672/`    |
| `QUEUE_NAME` | Nome da fila a ser consumida        | `tasks`                                 |
| `API_URL`    | Endpoint da API chamada via `POST`  | `http://localhost:9091/process`         |
| `TASKS_FILE` | Arquivo JSON lido pelo producer      | `tasks.json`                           |

## Como executar

### 1. Suba um RabbitMQ (se ainda não tiver um)

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

A interface de gerenciamento fica disponível em `http://localhost:15672`
(usuário/senha padrão: `guest`/`guest`).

### 2. Garanta que sua API esteja no ar na porta 9091

O serviço espera que `API_URL` aceite `POST` com o JSON da mensagem e
responda `2xx` com o resultado do processamento.

### 3. Baixe as dependências e rode o consumidor

```bash
go mod tidy
go run ./cmd/consumer
```

Ou compile e execute o binário:

```bash
go build -o consumer ./cmd/consumer
./consumer
```

Para customizar fila/API/conexão:

```bash
AMQP_URL="amqp://user:pass@meu-host:5672/" \
QUEUE_NAME="minha-fila" \
API_URL="http://localhost:9091/process" \
go run ./cmd/consumer
```

### 4. Publique mensagens de teste na fila com o producer

```bash
go run ./cmd/producer
```

Por padrão ele lê `tasks.json` na raiz do projeto, que deve conter uma lista
(array) de objetos JSON, por exemplo:

```json
[
  { "order_id": 1, "action": "process" },
  { "order_id": 2, "action": "process", "priority": "high" },
  { "order_id": 3, "action": "cancel" }
]
```

Cada objeto da lista é publicado como uma mensagem separada na fila
(`QUEUE_NAME`). Para usar outro arquivo:

```bash
TASKS_FILE=outra-lista.json go run ./cmd/producer
```

Com o consumidor rodando (passo 3), ele deve logar o recebimento de cada
mensagem, chamar a API e imprimir o resultado retornado no terminal.

Alternativamente, dá para publicar manualmente pela interface de
gerenciamento do RabbitMQ (`http://localhost:15672`), publicando um único
JSON (não uma lista) diretamente na fila `tasks`.
