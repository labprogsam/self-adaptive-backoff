# Piloto 03 — calibração do AIMD e do HPA

- **Etapa do design experimental:** calibração (Seção 7)
- **Motivação:** no piloto 02, o HPA com `HPA_SETPOINT` = 5% ficou no teto de delay em 98%
  das esperas e dominou o tempo total do experimento; o AIMD com os parâmetros padrão ficou
  muito mais barato que as demais estratégias. A Seção 7 pede que os parâmetros do AIMD
  (Δ+, ×decremento) e do HPA (setpoint, janela observável) produzam delays na mesma ordem de
  grandeza das demais estratégias.
- **Pergunta:** quais valores de Δ e f (AIMD) e de setpoint e janela (HPA) produzem delays
  comparáveis aos do exponencial, sem prender a estratégia no teto ou no piso, e mantendo o
  experimento principal dentro de 10 horas?

## Configuração

Escala de tempo 1/5 (piloto 02), no regime planejado para o experimento principal: taxa de
chegada abaixo da capacidade das estratégias estáticas.

| Parâmetro | Valor |
|---|---|
| Mensagens por execução | 500 (`tasks.json` deste diretório: as 500 primeiras do `tasks.json` de 3000) |
| Repetições | 3 |
| Fator E (`MAX_RETRIES`) | 5 (nível intermediário) |
| `PRODUCER_RATE` | 2,5 msgs/s, chegadas Poisson |
| `BASE_BACKOFF` / `MIN_BACKOFF` / `MAX_BACKOFF` | 100ms / 10ms / 1,6s |
| Latência da API simulada | 10ms + variação de 0 a 4ms |
| Janelas de indisponibilidade | 2s saudável + 2s fora do ar |
| Condições (B × C) | B ∈ {10%, 30%, 50%} × {uniforme, indisponibilidade} |

Configurações testadas, cada uma nas 6 condições:

| Estratégia | Valores |
|---|---|
| HPA | `HPA_SETPOINT` ∈ {10%, 30%, 50%} × `HPA_WINDOW_SIZE` ∈ {10, 20, 50} tentativas |
| AIMD | `AIMD_INCREMENT` (Δ) ∈ {20ms, 100ms} × `AIMD_DECREASE_FACTOR` (f) ∈ {0,5, 0,8} |
| Exponencial | referência de ordem de grandeza (sem parâmetros a calibrar) |

Total: 14 configurações × 6 condições = 84 rodadas × 3 repetições = 252 execuções, em ordem
embaralhada (seed `1792037885401100362`, registrada em `results/execution_order.csv`).

## Arquivos

| Arquivo | Conteúdo |
|---|---|
| `gen_plan.py` | Gera o `plan.json` (a grade de configurações acima) |
| `plan.json` | Plano executado |
| `tasks.json` | Carga usada (500 mensagens) |
| `analyze.py` | Calcula, por rodada, o delay efetivo, % de esperas no teto/piso, espera por mensagem, sucesso, chamadas por mensagem e vazão |
| `results/<round>/<run_id>.csv` | Dados brutos |
| `results/runs_summary.csv` | Status e duração de cada execução |
| `results/execution_order.csv` | Ordem embaralhada das execuções e semente |
| `results/resumo.csv` | Saída completa do `analyze.py` |
| `run.log` | Saída do orquestrador |

## Como reproduzir

A partir da raiz do repositório, com o RabbitMQ rodando:

```bash
P=pilots/03_calibracao_aimd_hpa
python3 $P/gen_plan.py
TASKS_FILE=$P/tasks.json go run ./cmd/orchestrator -plan $P/plan.json -concurrency 20 \
  -metrics-dir $P/results -drain-timeout 1h -seed 1792037885401100362
python3 $P/analyze.py --out $P/results/resumo.csv
```

## Resultados

Todas as 252 execuções terminaram com `status=ok`. Métricas abaixo: média das 3 repetições.
"Delay efetivo" é a média das esperas que a estratégia de fato usou (só tentativas que
falharam e não eram a última).

### Referência: exponencial

| Condição | Delay efetivo | Espera por msg | Sucesso | Vazão (msgs/s) |
|---|---|---|---|---|
| B=10%, uniforme | 111ms | 12ms | 100,0% | 2,36 |
| B=30%, uniforme | 172ms | 73ms | 99,9% | 2,34 |
| B=50%, uniforme | 260ms | 260ms | 98,7% | 2,21 |
| B=10%, indisponibilidade | 493ms | 276ms | 99,5% | 2,28 |
| B=30%, indisponibilidade | 407ms | 327ms | 97,2% | 2,24 |
| B=50%, indisponibilidade | 408ms | 500ms | 93,1% | 1,74 |

### AIMD: o equilíbrio segue uma fórmula fechada

O AIMD soma Δ a cada falha e multiplica o delay por f a cada sucesso. Com taxa de erro p, o
delay se estabiliza em:

```
d* = p · Δ / ((1 − p) · (1 − f))
```

Como a espera acontece logo depois de uma falha (que acabou de somar Δ), o delay efetivo
esperado é d* + Δ. No padrão uniforme, onde p = B:

| Δ, f | B | Previsto (d* + Δ) | Observado |
|---|---|---|---|
| 20ms, 0,5 | 10% | 24ms | 33ms |
| | 30% | 37ms | 43ms |
| | 50% | 60ms | 60ms |
| 100ms, 0,5 | 10% | 122ms | 127ms |
| | 30% | 186ms | 179ms |
| | 50% | 300ms | 295ms |

A fórmula permite escolher Δ e f de forma justificada, em vez de por tentativa e erro.

Comparação com o exponencial (espera de backoff por mensagem):

| Condição | Exponencial | AIMD 20ms/0,5 (padrão atual) | AIMD 100ms/0,5 | AIMD 100ms/0,8 |
|---|---|---|---|---|
| B=10%, uniforme | 12ms | 4ms | 14ms | 16ms |
| B=30%, uniforme | 73ms | 18ms | 76ms | 128ms |
| B=50%, uniforme | 260ms | 57ms | 290ms | 624ms |
| B=10%, indisponibilidade | 276ms | 150ms | 220ms | 228ms |
| B=30%, indisponibilidade | 327ms | 179ms | 330ms | 475ms |
| B=50%, indisponibilidade | 500ms | 251ms | 823ms | 1599ms |

Com Δ=100ms e f=0,5, o AIMD fica praticamente sobreposto ao exponencial no padrão uniforme
e na mesma ordem de grandeza na indisponibilidade. Com f=0,8, ele satura em B=50% com
indisponibilidade (vazão de 0,58 msgs/s). O padrão atual (20ms/0,5) espera de 2 a 4 vezes
menos que o exponencial.

### HPA: no padrão uniforme, não existe equilíbrio

No padrão uniforme, a taxa de erro da API não depende do delay. A fórmula
`delay × observada/setpoint` então só tem dois destinos: o teto, se B > setpoint, ou o piso,
se B < setpoint. Quando B = setpoint, o delay faz um passeio aleatório. Os dados confirmam:

| Setpoint | B=10%, uniforme | B=30%, uniforme | B=50%, uniforme |
|---|---|---|---|
| 10% | piso/aleatório (95–192ms) | **teto** (76–91% no teto) | **teto** (90–98% no teto) |
| 30% | **piso** (82–96% no piso) | aleatório (40–71ms) | **teto** (68–84% no teto) |
| 50% | **piso** (89–100% no piso) | **piso** (68–90% no piso) | aleatório (49–168ms) |

No padrão de indisponibilidade, esperar reduz o erro observado, e o HPA funciona como
controlador de fato, desde que a taxa média de erro do ciclo não ultrapasse o setpoint. Com
B=50% e janelas de 2s/2s, a taxa média é de ~75%, acima de qualquer setpoint testado: o HPA
vai para o teto e satura (vazão de 0,34–0,43 msgs/s).

Delay efetivo e vazão do HPA com setpoint 50%, por janela:

| Condição | Exponencial | Janela 10 | Janela 20 | Janela 50 |
|---|---|---|---|---|
| B=10%, uniforme | 111ms | 10ms | 12ms | 16ms |
| B=30%, uniforme | 172ms | 14ms | 13ms | 26ms |
| B=50%, uniforme | 260ms | 168ms | 49ms | 81ms |
| B=10%, indisponibilidade | 493ms | 148ms | 211ms | 197ms |
| B=30%, indisponibilidade | 407ms | 277ms | 364ms | 288ms |
| B=50%, indisponibilidade | 408ms | 1427ms (0,37 msgs/s) | 1446ms (0,37 msgs/s) | 1239ms (0,43 msgs/s) |

Delay efetivo por repetição em B=50%, uniforme (B = setpoint, o caso instável):

| Janela | Rep. 1 | Rep. 2 | Rep. 3 |
|---|---|---|---|
| 10 | 339ms | 125ms | 39ms |
| 20 | 22ms | 87ms | 38ms |
| 50 | 86ms | 92ms | 64ms |

A janela de 50 tentativas reduz o ruído da estimativa de erro e é a mais estável. Em troca,
ela cobre ~11–20s de execução (2,5–5 tentativas/s), ou seja, vários ciclos de
indisponibilidade de 4s: o HPA reage à taxa média de erro, não a cada indisponibilidade
individual. Com janela de 10, ele acompanha cada ciclo, mas fica instável.

Nenhum setpoint atende ao critério da Seção 7 em todas as condições, por causa da ausência
de equilíbrio no padrão uniforme. O setpoint de 50% é o que mais se aproxima: o HPA só fica
preso no teto em uma das 6 condições (B=50% com indisponibilidade), contra 3 com setpoint 30%
e 5 com setpoint 10%.

### Estimativa de tempo do experimento principal

36 combinações × 30 repetições, 1000 mensagens, E=5, taxa de chegada de 2,5 msgs/s, 20
execuções concorrentes. Duração de cada execução ≈ 1000 / vazão medida neste piloto para a
mesma condição. Retry imediato e fixo assumidos limitados pela taxa de chegada (capacidade
bem acima de 2,5 msgs/s no piloto 02); exponencial com jitter assumido igual ao exponencial,
exceto em B=50% com indisponibilidade (×0,76, proporção do piloto 02).

| Configuração do HPA | Tempo total estimado |
|---|---|
| setpoint 50%, janela 50 | 7,7 h |
| setpoint 50%, janela 20 | 7,9 h |

A execução isolada mais longa (HPA, B=50%, indisponibilidade) leva ~39 minutos. O tempo
total muda se a triagem escolher outro nível de E.

## Conclusões e decisões

| # | Conclusão | Decisão |
|---|---|---|
| 1 | O delay de equilíbrio do AIMD segue `d* = p·Δ/((1−p)(1−f))`, confirmado pelos dados | Usar a fórmula para justificar Δ e f |
| 2 | AIMD com Δ=100ms e f=0,5 fica na mesma ordem de grandeza do exponencial em todas as condições | Adotar Δ=100ms (500ms na escala original) e f=0,5 |
| 3 | No padrão uniforme, o HPA não tem equilíbrio: vai ao teto se B > setpoint e ao piso se B < setpoint | Registrar como característica estrutural do algoritmo quando a taxa de erro não depende da carga |
| 4 | Setpoint 50% é o único testado que só prende o HPA no teto em uma condição (B=50%, indisponibilidade) | Adotar `HPA_SETPOINT` = 0,5 |
| 5 | Janela de 50 tentativas é a mais estável quando B = setpoint, com efeito semelhante nas demais condições | Adotar `HPA_WINDOW_SIZE` = 50 (ver ressalva sobre reação a cada ciclo) |
| 6 | Com os parâmetros calibrados, o experimento principal fica em ~7,7h | Meta de 10h viável com E=5 |

## Limitações

- 3 repetições e 500 mensagens: suficiente para calibrar ordem de grandeza, não para comparar
  estratégias.
- Só E=5. Com E=10, as estratégias que saturam levam mais tempo.
- Setpoints e janelas fora da grade (ex.: setpoint 40%) não foram testados.
- O critério "mesma ordem de grandeza" da Seção 7 não é atingível para o HPA em todas as
  condições, por razão estrutural (conclusão 3). A escolha do setpoint 50% minimiza as
  condições em que ele fica preso no teto, mas faz o HPA quase não esperar no padrão
  uniforme com B ≤ 30%.
