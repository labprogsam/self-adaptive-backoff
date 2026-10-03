# Design Experimental — Análise Comparativa de Estratégias de Backoff em Arquitetura Orientada a Eventos

Este roteiro segue a metodologia sistemática de **Raj Jain** (*The Art of Computer Systems
Performance Analysis*, 1991) para avaliação de desempenho, composta por dez passos:

1. Definição dos objetivos do estudo e dos limites do sistema
2. Listagem de serviços do sistema e resultados possíveis
3. Seleção de métricas de desempenho
4. Listagem de parâmetros
5. Seleção dos fatores a estudar
6. Seleção da técnica de avaliação
7. Seleção da carga de trabalho
8. Design dos experimentos
9. Análise e interpretação dos dados
10. Apresentação dos resultados

Cada passo é detalhado a seguir, e aplicado ao sistema em questão: um consumidor
Go que lê mensagens de uma fila RabbitMQ, repassa cada mensagem via `POST` a uma API de
terceiro e, em caso de erro (JSON inválido, falha de comunicação, timeout ou resposta de
erro da API), reprocessa a mensagem segundo uma estratégia de backoff. (/image.png)

---

## 1. Objetivo geral

Comparar, sob condições controladas e reproduzíveis, o desempenho de diferentes
estratégias de backoff aplicadas ao reprocessamento de eventos que falham durante o
tratamento do dado ou durante a chamada a uma API de terceiro, em um pipeline orientado a
eventos.

---

## 2. Serviços do sistema e resultados possíveis

O serviço prestado pelo **mecanismo de backoff** é: dado que uma tentativa falhou, decidir
se e quando reprocessar, até a mensagem ser confirmada ou descartada. Para cada mensagem
consumida, os resultados terminais possíveis — mutuamente exclusivos, um por mensagem — são:

| # | Resultado | Descrição |
|---|-----------|-----------|
| R1 | Sucesso imediato | A primeira tentativa retorna `2xx`; mensagem é confirmada (`ack`) sem o backoff ter sido acionado. |
| R2 | Sucesso após retry | Uma ou mais tentativas falham, uma tentativa subsequente (dentro do limite) tem sucesso — o backoff resolveu a falha. |
| R3 | Falha definitiva (esgotamento) | Todas as tentativas (inicial + retries) falham; mensagem é descartada (`nack` sem requeue) — o orçamento de tentativas do backoff se esgotou. |

Esses três resultados serão contabilizados para compor as métricas do Passo 3 e são a
variável de resposta central para comparar as estratégias do Fator A.

---

## 3. Seleção das métricas de desempenho

1. **Taxa de sucesso** — proporção de mensagens que terminam em R1 ou R2, sobre o total
   processado.
2. **Tempo total de espera induzido pelo backoff, por mensagem** — soma das durações de
   `Sleep` entre tentativas (exclui o tempo da própria chamada HTTP) — isola o custo que o
   algoritmo impôs, independente de quão lenta a API estava naquele momento.
3. **Número de tentativas por mensagem** — média e distribuição; indica o "custo" de
   recuperação de cada estratégia.
4. **Sobrecarga de chamadas à API** — total de requisições HTTP disparadas (incluindo retries)
   dividido pelo número de mensagens únicas — mede a carga extra imposta ao serviço de terceiro.
5. **Percentual de descarte** — percentual de mensagens que terminam em R3.

---

## 4. Listagem de parâmetros

### 4.1 Parâmetros do sistema
- Estratégia de backoff.
- Delay base — hoje `500ms`.
- Delay máximo/teto — hoje `30s`.
- Número máximo de tentativas extras — hoje `3`.
- Fator/amplitude de jitter — hoje `20%` do delay calculado.
- Timeout HTTP do cliente http — hoje `30s`.
- `Qos` do consumidor RabbitMQ (prefetch) — hoje `1`.
- Política de requeue em falha definitiva — hoje `nack` sem requeue.
- Controladores (self-adaptive-backoffs).
- **Parâmetros específicos do AIMD (Fator A, nível 5):** incremento aditivo por falha
  (Δ+, ex. `+100ms`), fator de decremento multiplicativo por sucesso (ex. `×0.5`), delay
  mínimo/máximo do estado adaptativo.
- **Parâmetros específicos do algoritmo do HPA:** taxa de erro-alvo (*setpoint*), tamanho da
  janela observável usada para estimar a taxa de erro observada (variável de processo). Fórmula
  usada tal qual o Kubernetes a define, sem ganho adicional (`delay_novo = delay_atual ×
  taxa_observada/setpoint`) — decisão tomada com o orientador para manter fidelidade ao
  algoritmo original em vez de generalizá-lo com um termo de controle Proporcional externo.
- Número de instâncias do consumidor — hoje `1`.

- Hardware/contêineres usados para RabbitMQ, consumidor e simulador de API.
- Versão do Go e das dependências (`go.mod`).
  - Hardware:Ryzen 5800x3D 3401 Mhz. 8 núcleos, 16 proc. lógicos, 16G de memória DDR4,
  GeForce RTX 3070 8GB;
  - Sistema Operacional: Windows 11 Pro, versão 25H2;
  - Interface de rede: Desligada;
  - Fonte de alimentação: Rede elétrica;
  - Processos em execução: Apenas os estritamente necessário para execuções dos experimentos;
  - Linguagem de Programação: Go 1.22.3
- Isolamento de rede (todos os componentes na mesma máquina/rede local, para eliminar
  variação de latência de rede externa como fator de ruído).

### 4.2 Parâmetros de carga de trabalho -
- Tamanho/complexidade do payload JSON.
- Taxa de produção de mensagens (msgs/s) e seu padrão (constante vs. rajada de chegada).
- Taxa de erro induzida na API simulada (probabilidade de falha por requisição).
- Padrão temporal de falha da API simulada (aleatório uniforme, rajada/burst,
  indisponibilidade total por janela de tempo).
- Tempo de processamento de resposta da API (distribuição, ex.: constante, normal, longa).

---

## 5. Seleção dos fatores a estudar

Dos parâmetros listados acima, nem todos serão variados — alguns serão fixados em um valor
de controle para reduzir a dimensionalidade do experimento. Os fatores selecionados para variação são:

| Fator | Descrição | Níveis propostos |
|-------|-----------|-------------------|
| **A — Estratégia de backoff** | Algoritmo de retry | (1) sem backoff / retry imediato [controle], (2) backoff fixo, (3) backoff exponencial sem jitter, (4) backoff exponencial com jitter, (5) AIMD (Δ+, ×decremento), (6) algoritmo do HPA (delay_novo = delay_atual × taxa_observada/setpoint) |
| **B — Taxa de erro da API** | Probabilidade de falha por requisição no simulador | 10%, 30%, 50% |
| **C — Padrão temporal de falha** | Regime de erro da API simulada | aleatório uniforme, em rajada, indisponibilidade total temporária | 

| **D — Carga de mensagens** | Taxa de produção do producer | baixa, média, alta (valores calibrados na Seção 7) |
| **E — Número máximo de tentativas** | Numero máximo de tentativas até o descarte | 3, 5, 10 |

Justificativa da seleção: os fatores A, B e C são os que mais diretamente respondem às
perguntas de pesquisa. O fator D avalia se a vantagem de uma estratégia se mantém sob
pressão de carga. O fator E é incluído numa etapa de triagem porque estratégias
adaptativas podem depender de forma diferente do número de tentativas disponíveis.

**Nota sobre os níveis 5 e 6 do Fator A:** ambos exigem um sinal de realimentação — a taxa
de erro observada numa janela observável de mensagens (ex.: últimas N tentativas) —
e, ao contrário dos níveis 1–4, mantêm **estado entre mensagens** (o delay atual não é
recalculado do zero a cada mensagem, mas ajustado incrementalmente pelo histórico
observado). AIMD e o algoritmo do HPA foram escolhidos por serem instâncias simples e
bem definidas de duas famílias distintas de controle por realimentação;

---

## 6. Seleção da técnica de avaliação
Existem três técnicas de avaliação de desempenho: medição, modelagem analítica e simulação.
Para estes experimentos optamos por:

**Técnica principal: medição direta**, técnica de medição em ambiente controlado, com
falhas injetadas artificialmente, pois permite reprodutibilidade e controle experimental.

---

## 7. Seleção da carga de trabalho

- **Carga:** gerada por um produtor, em 3 níveis (baixa/média/alta, ex.:5/20/50 msgs/s,
  calibrados para a carga "alta" aproximar o consumidor da saturação).
  Payload JSON de 1KB a 3KB, fixo entre execuções. Volume fixo por *run* (ex.: 3.000
  mensagens), suficiente para observar rajadas de falha completas.
- **Simulador de API de terceiro:** serviço HTTP que decide, por configuração, se cada
  requisição responde `2xx` (latência configurável) ou falha, segundo o padrão do Fator C —
  aleatório uniforme (falha com probabilidade *p*), rajada (alterna janelas
  saudável/degradada) ou indisponibilidade total (100% de falha por uma janela, depois
  normal — testa teto de delay e tempo de recuperação).
- **Calibração:** rodada piloto para garantir que `MaxDelay` seja de fato alcançado dentro
  do número de tentativas testado, que a carga "alta" gere pressão real na fila sem
  estourar o broker, e que os parâmetros do AIMD (Δ+, ×decremento) e do algoritmo do HPA
  (*setpoint*, janela observável) produzam respostas na mesma ordem de grandeza de delay das
  demais estratégias — para que a comparação não seja enviesada por uma calibração ruim.
  O HPA usa a fórmula original do Kubernetes sem ganho adicional (ver Seção 4.1); só
  *setpoint* e janela observável precisam de calibração.

---

## 8. Design dos experimentos

- **Duas fases:** (1) triagem — fatorial fracionado cobrindo os fatores A–E, com
  poucas repetições, para descartar fatores/interações sem efeito significativo; (2)
  principal — fatorial completo **A × B × C** (6 estratégias × 3 taxas de erro × 3 padrões
  = 54 combinações), com D fixado em dois níveis (baixa/alta carga, analisados
  separadamente) e E fixado no valor indicado pela triagem.
- **Repetições:** n = 30 execuções independentes por combinação. Ordem aleatorizada entre
  combinações, para não confundir efeito de fator com efeito de tempo/ambiente. Cada
  execução parte de uma instância nova do simulador de API e do consumidor.
- **Controles:** hardware, versão do Go e rede fixos entre execuções.
- **Saída:** dois níveis de registro (CSV/JSON), ligados por `run_id`:
  - **por execução:** `run_id`, níveis dos fatores A–E, timestamp de início/fim, e o
    cronograma das janelas de falha aplicadas pelo simulador (início/fim de cada janela
    degradada/indisponível) — necessário para alinhar as curvas de recuperação entre
    execuções.
  - **por tentativa:** `run_id`, id da mensagem, nº da tentativa, timestamp, resultado
    daquela tentativa (sucesso, timeout, erro de comunicação, erro de dado) e resultado
    final da mensagem (R1–R6). Registrar por tentativa é o que permite excluir da sobrecarga
    de chamadas as tentativas que falharam por erro de dado (nunca chegam a acionar a API).

---

## 9. Análise e interpretação dos dados

- **Estatística descritiva:** médias, medianas, percentis (p95/p99) e intervalos de
  confiança (95%) para cada métrica, por combinação de fatores — calculados sobre as n=30
  execuções de cada combinação (não sobre mensagens individuais, para não sub-representar
  a variabilidade entre execuções).
- **Comparação entre estratégias por sobreposição de IC:** duas estratégias são
  consideradas diferentes numa dada condição (B, C) quando seus intervalos de confiança
  não se sobrepõem — uma leitura visual/tabular, sem p-valor.
- **Análise de trade-off:** gráficos de dispersão tempo de espera induzido pelo backoff ×
  sobrecarga de chamadas por estratégia, para visualizar a fronteira de Pareto entre
  "recuperar rápido" e "não sobrecarregar a API de terceiro".
- **Análise de séries temporais** (janelas de rajada/indisponibilidade): comparação visual
  da recuperação da taxa de sucesso entre estratégias.

---

## 10. Apresentação dos resultados

- **Tabelas resumo** por estratégia (A) × condição de erro (B, C): média ± IC95% de cada
  métrica primária.
- **Gráficos de barras com intervalo de confiança** comparando estratégias para cada
  métrica, agrupados por padrão de falha (C).
- **Boxplots** da distribuição do tempo de espera induzido pelo backoff por estratégia
  (métrica principal), evidenciando cauda (p95/p99); opcionalmente ao lado da latência
  total (métrica de contexto), para mostrar quanto dela é atribuível ao backoff.
- **Gráficos de linha temporal** mostrando taxa de sucesso ao longo do tempo durante e após
  uma janela de indisponibilidade simulada, uma linha por estratégia — ilustra diretamente
  o "tempo de recuperação".
- **Gráfico de dispersão (Pareto)** tempo de espera induzido pelo backoff vs. sobrecarga de
  chamadas à API, uma série por estratégia, para comunicar visualmente o trade-off central
  do estudo.

  OBS: Todos os gráficos devem indicar explicitamente os fatores fixados na respectiva figura,
  e o texto de acompanhamento deve declarar o nível de confiança usado e o tamanho da
  amostra (n de repetições), seguindo a recomendação de Jain de sempre apresentar
  resultados com sua variabilidade associada, nunca apenas a média pontual.

---

## Referência metodológica
JAIN, Raj. **The Art of Computer Systems Performance Analysis: Techniques for
Experimental Design, Measurement, Simulation, and Modeling.** John Wiley & Sons, 1991.
(Capítulo 3 — "The Art of Systematic Performance Evaluation")
