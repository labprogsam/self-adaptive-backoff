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

As escolhas de valores deste documento são sustentadas por execuções piloto e pela fase de
triagem, cujos planos, dados brutos e análises estão no repositório:

| Evidência | Diretório | O que sustenta |
|---|---|---|
| Piloto 01 | `pilots/01_piloto_inicial/` | volume de mensagens por execução; necessidade de operar abaixo da capacidade do consumidor |
| Piloto 02 | `pilots/02_escala_e_capacidade/` | escala de tempo 1/5; capacidade do consumidor por estratégia; taxa de produção |
| Piloto 03 | `pilots/03_calibracao_aimd_hpa/` | parâmetros do AIMD e do HPA |
| Triagem | `experiments/01_triagem/` | inclusão do Fator E no experimento principal; número de repetições |

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

Parâmetros que podem influenciar as métricas. Cada um é variado como fator ou fixado em um
valor de controle; os valores e os motivos estão na Seção 5.

### 4.1 Parâmetros do sistema
- Estratégia de backoff.
- Delay base.
- Delay máximo/teto.
- Número máximo de tentativas extras.
- Fator/amplitude de jitter.
- Timeout HTTP do cliente.
- `Qos` do consumidor RabbitMQ (prefetch).
- Política de requeue em falha definitiva.
- Número de instâncias do consumidor.
- **Parâmetros específicos do AIMD (Fator A, nível 5):** incremento aditivo por falha (Δ+),
  fator de decremento multiplicativo por sucesso (×f) e delay mínimo/máximo do estado
  adaptativo.
- **Parâmetros específicos do algoritmo do HPA (Fator A, nível 6):** taxa de erro-alvo
  (*setpoint*), tamanho da janela observável usada para estimar a taxa de erro observada
  (variável de processo) e delay mínimo/máximo. A fórmula é usada tal qual o Kubernetes a
  define, sem ganho adicional: `delay_novo = delay_atual × taxa_observada / setpoint`.
- Ambiente de execução:
  - Hardware: Ryzen 5800x3D 3401 Mhz. 8 núcleos, 16 proc. lógicos, 16G de memória DDR4,
    GeForce RTX 3070 8GB;
  - Sistema Operacional: Windows 11 Pro, versão 25H2;
  - Interface de rede: Desligada;
  - Fonte de alimentação: Rede elétrica;
  - Processos em execução: Apenas os estritamente necessário para execuções dos experimentos;
  - Linguagem de Programação: Go 1.22.3, dependências em `go.mod`;
  - Isolamento de rede: todos os componentes na mesma máquina, para eliminar variação de
    latência de rede externa como fator de ruído.

### 4.2 Parâmetros de carga de trabalho
- Tamanho/complexidade do payload JSON.
- Volume de mensagens por execução.
- Taxa de produção de mensagens (msgs/s) e seu padrão de chegada.
- Taxa de erro induzida na API simulada (probabilidade de falha por requisição).
- Padrão temporal de falha da API simulada.
- Tempo de resposta da API simulada.

---

## 5. Seleção dos fatores a estudar

### 5.1 Fatores variados

| Fator | Descrição | Níveis |
|-------|-----------|--------|
| **A — Estratégia de backoff** | Algoritmo de retry | (1) sem backoff / retry imediato [controle], (2) backoff fixo, (3) backoff exponencial sem jitter, (4) backoff exponencial com jitter, (5) AIMD (Δ+, ×decremento), (6) algoritmo do HPA (delay_novo = delay_atual × taxa_observada/setpoint) |
| **B — Taxa de erro da API** | Probabilidade de falha de cada requisição enquanto a API simulada está em operação — durante todo o tempo no padrão uniforme, e apenas na janela saudável no padrão de indisponibilidade (na janela de indisponibilidade a falha é sempre 100%) | 10%, 30%, 50% |
| **C — Padrão temporal de falha** | Regime de erro da API simulada (como essas falhas se distribuem no tempo) | aleatório uniforme, indisponibilidade total temporária |
| **E — Número máximo de tentativas** | Número máximo de tentativas extras até o descarte | 3, 5, 10 |

A letra D é reservada à taxa de produção, que é fixada (Seção 5.2).

### 5.2 Parâmetros fixados

**Escala de tempo.** Todas as constantes de tempo (delays, latência da API, janelas de
indisponibilidade e timeout) estão a 1/5 de valores de referência realistas — ex.: delay base
de 500ms → 100ms, latência da API de 50ms → 10ms. Como as proporções entre os tempos se
mantêm, as métricas sem unidade de tempo (taxa de sucesso, tentativas e chamadas por
mensagem, descarte) não mudam, e a espera de backoff escala exatamente na mesma proporção;
em troca, cada execução fica ~5× mais curta (piloto 02: diferenças dentro do IC95% em todas
as métricas sem unidade de tempo, e espera 4,9–5,0× menor). Os valores abaixo já estão nessa
escala.

| Parâmetro | Valor | Motivo | Evidência |
|---|---|---|---|
| Delay base | 100ms | Delay inicial das estratégias fixa e exponenciais e estado inicial do AIMD e do HPA (500ms na escala de referência). | — |
| Delay máximo | 1,6s | Atingido na 5ª tentativa extra do exponencial (100ms × 2⁴), de modo que o teto tem efeito com E=5 e E=10; com um teto maior, ele nunca seria alcançado nos níveis de E estudados. | pilotos 01 e 02 |
| Delay mínimo (AIMD, HPA) | 10ms | Piso do estado adaptativo (50ms na escala de referência). | — |
| Jitter (nível 4) | 20% do delay, proporcional | Tratamento do nível 4; mantido fixo para que a diferença entre os níveis 3 e 4 isole o efeito do jitter. | — |
| Timeout HTTP | 6s | Maior que qualquer resposta da API simulada; não interfere nas tentativas. | — |
| `Qos` (prefetch) | 1 | Fora do escopo do estudo; com uma mensagem por vez, a ordem das observações que alimentam o AIMD e o HPA é determinística. | — |
| Instâncias do consumidor | 1 | Fora do escopo do estudo. | — |
| Requeue em falha definitiva | `nack` sem requeue | Evita que uma mensagem que sempre falha trave a fila. | — |
| AIMD: Δ+ e ×f | 100ms e ×0,5 | O delay de equilíbrio é `d* = p·Δ/((1−p)·(1−f))` para taxa de erro p; com esses valores, o AIMD fica na mesma ordem de grandeza de espera do exponencial em todas as condições. | piloto 03 |
| HPA: *setpoint* e janela | 50% e 50 tentativas | Com taxa de erro independente do delay (padrão uniforme), o HPA não tem equilíbrio: o delay vai ao teto se B > *setpoint* e ao piso se B < *setpoint*. O *setpoint* de 50% é o que menos prende o HPA no teto entre os valores testados (1 de 6 condições); a janela de 50 é a mais estável quando B = *setpoint*. | piloto 03 |
| **D — Taxa de produção** | 2,5 msgs/s, chegadas Poisson | Ver justificativa abaixo. | pilotos 01 e 02 |
| Volume por execução | 1.000 mensagens | As médias de todas as métricas ficam dentro do IC95% (ou a até 2%) do valor com 3.000 mensagens. | piloto 01 |
| Payload | JSON de ~100 bytes, fixo | Nem o consumidor nem a API simulada validam o conteúdo além de ser um objeto JSON; o tamanho não influencia o que é medido. | — |
| Tempo de resposta da API | 10ms + variação uniforme de 0 a 4ms | Latência local estável (50ms + até 20ms na escala de referência). | — |
| Janelas de indisponibilidade | 2s saudável + 2s fora do ar | Cada execução cobre dezenas de ciclos completos de falha. | pilotos 01 e 02 |

### 5.3 Justificativas

**Fatores A, B e C** são os que mais diretamente respondem às perguntas de pesquisa.

**Fator E é variado** porque o número de tentativas disponíveis muda a ordem entre as
estratégias. Na triagem (Seção 8), E e a interação A×E explicaram 8,5% da variação da taxa de
sucesso e 10,4% das tentativas por mensagem. O efeito se concentra no padrão de
indisponibilidade: com E=3, o AIMD tem a maior taxa de sucesso (85,2%, contra 79,4% do
exponencial); com E=5, o exponencial passa à frente (96,4% contra 90,5%). Fixar E faria a
conclusão depender do nível escolhido.

**Fator D é fixado em 2,5 msgs/s** porque a taxa de produção não pode ser tratada como fator
comparativo neste sistema:

- Com consumidor único e `Qos(1)`, a espera de backoff bloqueia o processamento das mensagens
  seguintes, então a capacidade do consumidor depende da estratégia: com B=50% e E=10, ela
  vai de 33,9 msgs/s (retry imediato) a 2,47 msgs/s (exponencial com jitter) no padrão
  uniforme, e a 0,29–1,0 msgs/s nas estratégias que mais esperam sob indisponibilidade
  (piloto 02).
- Acima da capacidade, o consumidor opera saturado durante toda a execução: a fila cresce sem
  limite, não há regime estacionário, o tempo de recuperação após uma indisponibilidade não
  pode ser observado e a quantidade de mensagens expostas a cada janela de falha passa a
  depender da velocidade da estratégia, e não da chegada (piloto 01, a 20 msgs/s). Variar a
  taxa acima desse ponto só mudaria o tamanho da fila.
- A 2,5 msgs/s, a chegada fica no limite da capacidade da estratégia estática mais lenta no
  pior caso medido e abaixo dela nas demais condições. Todas as estratégias recebem a mesma
  chegada de mensagens, e só as que esperam mais do que essa chegada permite acumulam fila —
  um custo mensurável dessas estratégias, e não uma condição imposta a todas.

**Níveis do Fator C:** o fator compara falhas independentes no tempo (uniforme) com falhas
concentradas no tempo (indisponibilidade total por uma janela) — é essa diferença que
determina se esperar antes de reprocessar aumenta a chance de sucesso.

**Níveis 5 e 6 do Fator A:** ambos exigem um sinal de realimentação — a taxa de erro observada
numa janela observável de tentativas — e, ao contrário dos níveis 1–4, mantêm **estado entre
mensagens** (o delay atual não é recalculado do zero a cada mensagem, mas ajustado
incrementalmente pelo histórico observado). AIMD e o algoritmo do HPA foram escolhidos por
serem instâncias simples e bem definidas de duas famílias distintas de controle por
realimentação.

---

## 6. Seleção da técnica de avaliação
Existem três técnicas de avaliação de desempenho: medição, modelagem analítica e simulação.
Para estes experimentos optamos por:

**Técnica principal: medição direta**, técnica de medição em ambiente controlado, com
falhas injetadas artificialmente, pois permite reprodutibilidade e controle experimental.

---

## 7. Seleção da carga de trabalho

- **Carga:** gerada por um produtor, a 2,5 msgs/s com chegadas Poisson, igual em todas as
  execuções. Cada execução publica as mesmas 1.000 mensagens (`tasks.json`, JSON de ~100
  bytes, gerado com semente fixa por `experiments/gen_tasks.py`), na mesma ordem.
- **Simulador de API de terceiro:** serviço HTTP que decide, por configuração, se cada
  requisição responde `2xx` (latência configurável) ou falha, segundo o padrão do Fator C —
  aleatório uniforme (falha com probabilidade B) ou indisponibilidade total (janelas
  saudáveis com taxa de erro B alternadas com janelas de 100% de falha — testa teto de delay
  e tempo de recuperação).
- **Calibração:** os parâmetros fixados da Seção 5.2 atendem a três critérios, verificados nos
  pilotos:
  - o delay máximo é alcançado dentro dos níveis de E estudados;
  - a taxa de produção fica abaixo da capacidade do consumidor nas estratégias estáticas
    (ver Fator D, Seção 5.3);
  - os parâmetros do AIMD (Δ+, ×f) e do HPA (*setpoint*, janela observável) produzem esperas
    na mesma ordem de grandeza das demais estratégias, para que a comparação não seja
    enviesada por uma calibração ruim. Para o HPA, esse critério não é atingível em todas as
    condições, pela ausência de equilíbrio no padrão uniforme (Seção 5.2); o *setpoint* foi
    escolhido para minimizar as condições em que ele fica preso no teto.

---

## 8. Design dos experimentos

O estudo tem duas fases. Nas duas, o Fator D fica fixado em 2,5 msgs/s e os demais
parâmetros nos valores da Seção 5.2.

**Fase 1 — Triagem** (`experiments/01_triagem/`)

- **Objetivo:** medir o efeito do Fator E e de suas interações, para decidir se E pode ser
  fixado no experimento principal.
- **Desenho:** fatorial completo **A × B × C × E** (6 × 3 × 2 × 3 = 108 combinações), 3
  repetições, 500 mensagens por execução. Com apenas quatro fatores e execuções na escala de
  tempo 1/5, o fatorial completo é viável (~1,5h) e evita confundir efeitos principais com
  interações, o que um fatorial fracionado faria.
- **Critério de decisão:** **alocação de variação** (Jain) — para cada métrica da Seção 3,
  calcula-se a porcentagem da variação total explicada por cada efeito e interação. Se
  E + A×E explicam menos de 5% da variação em todas as métricas, E é fixado no nível
  intermediário (5); caso contrário, E é mantido como fator.
- **Resultado:** E + A×E explicam 8,5% (taxa de sucesso) e 10,4% (tentativas por mensagem);
  E é mantido como fator (Seção 5.3).

**Fase 2 — Experimento principal**

- **Desenho:** fatorial completo **A × B × C × E** (6 × 3 × 2 × 3 = 108 combinações), 1.000
  mensagens por execução.
- **Repetições:** n = 10 execuções independentes por combinação (1.080 execuções, ~7,2h). Na
  triagem, o erro experimental (variação entre repetições) explicou só 0,2%–0,6% da variação
  total: as diferenças entre combinações são muito maiores que o ruído entre execuções.

**Condução das execuções (ambas as fases)**

- **Ordem:** aleatorizada entre combinações, para não confundir efeito de fator com efeito de
  tempo/ambiente. A semente e a ordem usada são registradas.
- **Isolamento:** cada execução parte de uma instância nova do simulador de API, do
  consumidor e do produtor, com porta, fila e arquivos próprios; a fila é esvaziada antes e
  apagada depois de cada execução. Até 20 execuções rodam em paralelo; dentro de uma
  execução, o processamento é sequencial (`Qos(1)`).
- **Controles:** hardware, versão do Go e rede fixos entre execuções.
- **Saída:** dois níveis de registro (CSV), ligados por `run_id`:
  - **por execução:** `run_id`, níveis dos fatores A–E, timestamp de início/fim, e o
    cronograma das janelas de falha aplicadas pelo simulador (início/fim de cada janela
    saudável/indisponível) — necessário para alinhar as curvas de recuperação entre
    execuções. O cronograma é registrado pelo próprio simulador no momento de cada troca de
    janela, para permanecer alinhado aos timestamps das tentativas mesmo que o relógio do
    sistema seja ajustado durante a execução.
  - **por tentativa:** `run_id`, id da mensagem, nº da tentativa, timestamp, resultado
    daquela tentativa (sucesso, timeout, erro de comunicação, erro de dado) e resultado
    final da mensagem (R1–R3). Registrar por tentativa é o que permite excluir da sobrecarga
    de chamadas as tentativas que falharam por erro de dado (nunca chegam a acionar a API).

---

## 9. Análise e interpretação dos dados

- **Estatística descritiva:** médias, medianas, percentis (p95/p99) e intervalos de
  confiança (95%) para cada métrica, por combinação de fatores — calculados sobre as n=10
  execuções de cada combinação (não sobre mensagens individuais, para não sub-representar
  a variabilidade entre execuções).
- **Comparação entre estratégias por sobreposição de IC:** duas estratégias são
  consideradas diferentes numa dada condição (B, C, E) quando seus intervalos de confiança
  não se sobrepõem — uma leitura visual/tabular, sem p-valor.
- **Análise de trade-off:** gráficos de dispersão tempo de espera induzido pelo backoff ×
  sobrecarga de chamadas por estratégia, para visualizar a fronteira de Pareto entre
  "recuperar rápido" e "não sobrecarregar a API de terceiro".
- **Análise de séries temporais** (janelas de indisponibilidade): comparação visual
  da recuperação da taxa de sucesso entre estratégias.

---

## 10. Apresentação dos resultados

- **Tabelas resumo** por estratégia (A) × condição de erro (B, C) × número de tentativas
  (E): média ± IC95% de cada métrica primária.
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
