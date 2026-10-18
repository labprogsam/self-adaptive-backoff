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
- Delay base — hoje `100ms`.
- Delay máximo/teto — hoje `1,6s`.
- Número máximo de tentativas extras — hoje `3`.
- Fator/amplitude de jitter — hoje `20%` do delay calculado.
- Timeout HTTP do cliente http — hoje `6s`.
- `Qos` do consumidor RabbitMQ (prefetch) — hoje `1`.
- Política de requeue em falha definitiva — hoje `nack` sem requeue.
- Controladores (self-adaptive-backoffs).
- **Parâmetros específicos do AIMD (Fator A, nível 5):** incremento aditivo por falha
  (Δ+, hoje `+100ms`), fator de decremento multiplicativo por sucesso (hoje `×0.5`), delay
  mínimo/máximo do estado adaptativo (hoje `10ms` / `1,6s`).
- **Parâmetros específicos do algoritmo do HPA:** taxa de erro-alvo (*setpoint*), tamanho da
  janela observável usada para estimar a taxa de erro observada (variável de processo). Fórmula
  usada tal qual o Kubernetes a define, sem ganho adicional (`delay_novo = delay_atual ×
  taxa_observada/setpoint`) — decisão tomada com o orientador para manter fidelidade ao
  algoritmo original em vez de generalizá-lo com um termo de controle Proporcional externo.
  Hoje: *setpoint* `50%`, janela observável de `50` tentativas, delay mínimo/máximo
  `10ms` / `1,6s`.
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
- Padrão temporal de falha da API simulada (aleatório uniforme, indisponibilidade total
  por janela de tempo).
- Tempo de processamento de resposta da API (distribuição, ex.: constante, normal, longa).

---

## 5. Seleção dos fatores a estudar

Dos parâmetros listados acima, nem todos serão variados — alguns serão fixados em um valor
de controle para reduzir a dimensionalidade do experimento. Os fatores selecionados para variação são:

| Fator | Descrição | Níveis propostos |
|-------|-----------|-------------------|
| **A — Estratégia de backoff** | Algoritmo de retry | (1) sem backoff / retry imediato [controle], (2) backoff fixo, (3) backoff exponencial sem jitter, (4) backoff exponencial com jitter, (5) AIMD (Δ+, ×decremento), (6) algoritmo do HPA (delay_novo = delay_atual × taxa_observada/setpoint) |
| **B — Taxa de erro da API** | Probabilidade de falha de cada requisição enquanto a API simulada está em operação — durante todo o tempo no padrão uniforme, e apenas na janela saudável no padrão de indisponibilidade (na janela de indisponibilidade a falha é sempre 100%) | 10%, 30%, 50% |
| **C — Padrão temporal de falha** | Regime de erro da API simulada (como essas falhas se distribuem no tempo) | aleatório uniforme, indisponibilidade total temporária |
| **D — Carga de mensagens** | Taxa de produção do producer | **fixado em 2,5 msgs/s** — não é variado (ver justificativa abaixo) |
| **E — Número máximo de tentativas** | Numero máximo de tentativas até o descarte | 3, 5, 10 |

Justificativa da seleção: os fatores A, B e C são os que mais diretamente respondem às
perguntas de pesquisa. O fator E é incluído na etapa de triagem porque estratégias
adaptativas podem depender de forma diferente do número de tentativas disponíveis; também é
fixado no experimento principal.

**Justificativa da fixação do Fator D em 2,5 msgs/s:** o fator D foi originalmente previsto
para ser variado na triagem, mas os pilotos mostraram que a taxa de produção não pode ser
tratada como um fator comparativo neste sistema:

- Com consumidor único e `Qos(1)`, a espera de backoff bloqueia o processamento das
  mensagens seguintes, então a capacidade do consumidor depende da estratégia. No piloto 02
  (`pilots/02_escala_e_capacidade`), com B=50% e E=10, a capacidade medida variou de 33,9
  msgs/s (retry imediato) a 2,47 msgs/s (exponencial com jitter) no padrão uniforme, e
  chegou a 0,29–1,0 msgs/s nas estratégias que mais esperam sob indisponibilidade.
- Acima da capacidade, o consumidor opera saturado durante toda a execução (piloto 01, com
  20 msgs/s): a fila cresce sem limite, não há regime estacionário, o tempo de recuperação
  após uma indisponibilidade não pode ser observado e a quantidade de mensagens expostas a
  cada janela de falha passa a depender da velocidade da estratégia, e não da chegada. Os
  níveis previstos (5/20/50 msgs/s) estariam todos nessa condição para parte das
  estratégias, e variar D só mudaria o tamanho da fila.
- A taxa de 2,5 msgs/s fica no limite da capacidade da estratégia estática mais lenta no
  pior caso medido (2,47 msgs/s com B=50% e E=10), e abaixo dela nas demais condições. Assim,
  todas as estratégias recebem a mesma chegada de mensagens, e só as que esperam mais do
  que essa chegada permite acumulam fila — o que passa a ser um custo mensurável dessas
  estratégias, e não uma condição imposta a todas.
- Com essa taxa, o experimento principal fica estimado em ~7,7h (piloto 03,
  `pilots/03_calibracao_aimd_hpa`), dentro da janela de execução disponível.

**Nota sobre os níveis do Fator C:** o fator compara falhas independentes no tempo
(uniforme) com falhas concentradas no tempo (indisponibilidade total por uma janela) — é
essa diferença que determina se esperar antes de reprocessar aumenta a chance de sucesso.
No padrão de indisponibilidade, B é a taxa de erro da janela saudável; a taxa média efetiva é,
portanto, maior que B (ex.: B=50% com janelas de 10s/10s resulta em ~75%).

```
taxa média = (janela_saudável × B + janela_indisponível × 100%) / (janela_saudável + janela_indisponível)
```

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

- **Carga:** gerada por um produtor, a uma taxa fixa de 2,5 msgs/s com chegadas Poisson,
  igual em todas as execuções da triagem e do experimento principal (Fator D fixado; ver
  justificativa na Seção 5 e piloto 02).
  Payload JSON pequeno (~100 bytes), fixo entre execuções. Volume fixo por *run*: 1.000
  mensagens, suficiente para observar janelas de indisponibilidade completas (piloto 01:
  as métricas se estabilizam a partir de 1.000 mensagens).
- **Simulador de API de terceiro:** serviço HTTP que decide, por configuração, se cada
  requisição responde `2xx` (latência configurável) ou falha, segundo o padrão do Fator C —
  aleatório uniforme (falha com probabilidade *p*) ou indisponibilidade total (100% de
  falha por uma janela, depois normal — testa teto de delay e tempo de recuperação).
- **Calibração:** rodada piloto para garantir que `MaxDelay` seja de fato alcançado dentro
  do número de tentativas testado, que a taxa de produção fique abaixo da capacidade do
  consumidor (ver Fator D, Seção 5), e que os parâmetros do AIMD (Δ+, ×decremento) e do algoritmo do HPA
  (*setpoint*, janela observável) produzam respostas na mesma ordem de grandeza de delay das
  demais estratégias — para que a comparação não seja enviesada por uma calibração ruim.
  O HPA usa a fórmula original do Kubernetes sem ganho adicional (ver Seção 4.1); só
  *setpoint* e janela observável precisam de calibração. Os pilotos de calibração, com
  planos, dados brutos e conclusões, estão em `pilots/`:
  - **Piloto 01** — volume de 1.000 mensagens por *run*; com 3 tentativas extras, o teto de
    delay não era atingido; com a taxa de produção original, o consumidor operava sempre
    saturado.
  - **Piloto 02** — escala de tempo 1/5: todas as constantes de tempo (delays, latência da
    API, janelas de indisponibilidade, timeout) divididas por 5, o que preserva as métricas
    sem unidade de tempo e reduz a duração das execuções na mesma proporção. Valores
    resultantes: delay base `100ms`, teto `1,6s` (atingido na 5ª tentativa extra do
    exponencial), latência da API `10ms` + até `4ms`, janelas de `2s`/`2s`, taxa de
    produção de `2,5` msgs/s (abaixo da capacidade das estratégias estáticas).
  - **Piloto 03** — AIMD com Δ+ = `100ms` e ×`0.5` (delay de equilíbrio
    `d* = p·Δ/((1−p)·(1−f))`, na mesma ordem de grandeza do exponencial); HPA com
    *setpoint* `50%` e janela de `50` tentativas. No padrão uniforme, o HPA não tem ponto
    de equilíbrio — o delay vai ao teto se B > *setpoint* e ao piso se B < *setpoint* —,
    porque a taxa de erro da API simulada não depende do delay; o *setpoint* de 50% é o que
    menos prende o HPA no teto entre os valores testados.

---

## 8. Design dos experimentos

- **Duas fases:** (1) triagem — fatorial fracionado cobrindo os fatores A, B, C e E, com
  poucas repetições, para descartar fatores/interações sem efeito significativo; (2)
  principal — fatorial completo **A × B × C** (6 estratégias × 3 taxas de erro × 2 padrões
  = 36 combinações), com E fixado no valor indicado pela triagem. Nas duas fases, **D fica
  fixado em 2,5 msgs/s** (Seção 5).
- **Repetições:** n = 30 execuções independentes por combinação. Ordem aleatorizada entre
  combinações, para não confundir efeito de fator com efeito de tempo/ambiente. Cada
  execução parte de uma instância nova do simulador de API e do consumidor.
- **Controles:** hardware, versão do Go e rede fixos entre execuções.
- **Saída:** dois níveis de registro (CSV/JSON), ligados por `run_id`:
  - **por execução:** `run_id`, níveis dos fatores A–E, timestamp de início/fim, e o
    cronograma das janelas de falha aplicadas pelo simulador (início/fim de cada janela
    degradada/indisponível) — necessário para alinhar as curvas de recuperação entre
    execuções. Gravado pelo orquestrador em `runs_summary.csv` e `failure_windows.csv`;
    o cronograma é registrado pelo próprio simulador no momento de cada troca de janela,
    para permanecer alinhado aos timestamps das tentativas mesmo que o relógio do sistema
    seja ajustado durante a execução.
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
- **Análise de séries temporais** (janelas de indisponibilidade): comparação visual
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
