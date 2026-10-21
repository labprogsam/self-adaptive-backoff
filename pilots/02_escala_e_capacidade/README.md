# Piloto 02 — escala de tempo 1/5 e capacidade das estratégias

- **Etapa do design experimental:** calibração (Seção 7)
- **Motivação:** o piloto 01 mostrou que, com 20 msgs/s, o consumer opera sempre saturado, e
  que reduzir a taxa do producer com as constantes de tempo atuais tornaria o experimento
  principal longo demais (~30h). A alternativa é encolher todas as constantes de tempo na
  mesma proporção, o que aumenta a capacidade do consumer sem mudar a dinâmica.
- **Perguntas:**
  - **Parte A:** dividir todas as constantes de tempo por 5 preserva os resultados?
  - **Parte B:** qual a vazão máxima do consumer com cada uma das 6 estratégias, no pior
    caso? Isso define a taxa de chegada viável e o tempo total do experimento principal.

## Escala de tempo 1/5

Só os parâmetros medidos em tempo mudam; proporções e contagens ficam iguais.

| Parâmetro | Escala original | Escala 1/5 |
|---|---|---|
| `BASE_BACKOFF` | 500ms | 100ms |
| `MAX_BACKOFF` | 30s | 6s (parte A) / 1,6s (parte B) |
| `MIN_BACKOFF` | 50ms | 10ms |
| `AIMD_INCREMENT` | 100ms | 20ms |
| `HTTP_TIMEOUT` | 30s | 6s |
| `APISIM_LATENCY_MEAN` / `_JITTER` | 50ms / 20ms | 10ms / 4ms |
| `APISIM_HEALTHY_WINDOW` / `_OUTAGE_WINDOW` | 10s / 10s | 2s / 2s |
| B, E, `AIMD_DECREASE_FACTOR`, `HPA_SETPOINT`, `HPA_WINDOW_SIZE` | — | sem mudança |

Na parte B, `MAX_BACKOFF` = 1,6s (8s na escala original) foi escolhido para que o teto seja
atingido na 5ª retentativa do exponencial, respondendo à conclusão 4 do piloto 01.

## Configuração

Todas as execuções: 1000 mensagens (`tasks.json` deste diretório), B=50%, 20 execuções em
paralelo, `PRODUCER_RATE` = 100 msgs/s com chegadas Poisson (acima da capacidade de todas
as estratégias, de propósito).

**Parte A** (`plan_a_escala.json`): as 4 combinações do piloto 01 (retry imediato e
exponencial × uniforme e indisponibilidade), E=3, 10 repetições. Taxa do producer também
multiplicada por 5 (20 → 100 msgs/s), para manter o mesmo regime saturado do piloto 01 e
permitir a comparação direta.

**Parte B** (`plan_b_capacidade.json`): as 6 estratégias × uniforme e indisponibilidade,
E=10 (o nível mais lento do Fator E), 3 repetições. AIMD e HPA com os parâmetros padrão
(ainda não calibrados): `AIMD_DECREASE_FACTOR` = 0,5, `HPA_SETPOINT` = 5%,
`HPA_WINDOW_SIZE` = 20.

## Arquivos

| Arquivo | Conteúdo |
|---|---|
| `plan_a_escala.json`, `plan_b_capacidade.json` | Planos executados |
| `tasks.json` | Carga usada: as 1000 primeiras mensagens do `tasks.json` de 3000 |
| `results_a/`, `results_b/` | Dados brutos (`<round>/<run_id>.csv`) e `runs_summary.csv` de cada parte |
| `run_a.log`, `run_b.log` | Saída do orquestrador |

## Como reproduzir

A partir da raiz do repositório, com o RabbitMQ rodando:

```bash
P=pilots/02_escala_e_capacidade
export TASKS_FILE=$P/tasks.json
go run ./cmd/orchestrator -plan $P/plan_a_escala.json -parallel 20 \
  -metrics-dir $P/results_a -drain-timeout 1h
go run ./cmd/orchestrator -plan $P/plan_b_capacidade.json -parallel 20 \
  -metrics-dir $P/results_b -drain-timeout 1h

# comparação da parte A com o piloto 01 (N=1000)
python3 experiments/prefix_analysis.py $P/results_a --prefixes 1000
```

O simulador usa `math/rand` sem semente fixa: uma reprodução dá números próximos, não
idênticos. Atenção ao `-drain-timeout`: as execuções do HPA com indisponibilidade levaram
~53 minutos, perto do limite de 1h.

## Resultados

Todas as 76 execuções (40 da parte A, 36 da parte B) terminaram com `status=ok`.

### Parte A — a escala 1/5 preserva a dinâmica

Comparação com o piloto 01 em N=1000 (média ± meia-largura do IC95%, 10 repetições):

| Combinação | Métrica | Piloto 01 | Escala 1/5 |
|---|---|---|---|
| exponencial, uniforme | sucesso | 93,86% ± 0,51 | 93,64% ± 0,51 |
| | espera de backoff por mensagem | 746,75ms | 148,40ms (5,03× menor) |
| | chamadas à API por mensagem | 1,87 | 1,86 |
| exponencial, indisponibilidade | sucesso | 76,82% ± 0,89 | 76,53% ± 0,70 |
| | espera de backoff por mensagem | 1268,45ms | 257,19ms (4,93× menor) |
| | chamadas à API por mensagem | 2,27 | 2,29 |
| retry imediato, uniforme | sucesso | 93,68% ± 0,48 | 93,84% ± 0,38 |
| | chamadas à API por mensagem | 1,88 | 1,88 |
| retry imediato, indisponibilidade | sucesso | 65,00% ± 1,05 | 64,93% ± 0,78 |
| | chamadas à API por mensagem | 2,53 | 2,53 |

Todas as métricas sem unidade de tempo coincidem dentro do IC95%, e a espera de backoff cai
na proporção exata da escala (≈5×). A latência média das chamadas foi de 12,4ms, contra 12ms
esperados (10ms + média de 2ms da variação).

Duração média por execução:

| Combinação | Escala 1/5 |
|---|---|
| `exponential_b50_outage` | 4,8 min |
| `exponential_b50_uniform` | 2,9 min |
| `none_b50_outage` | 0,6 min |
| `none_b50_uniform` | 0,4 min |

### Parte B — capacidade das 6 estratégias no pior caso

Vazão = mensagens concluídas / tempo entre a primeira e a última tentativa (média das 3
repetições). B=50%, E=10, `MAX_BACKOFF` = 1,6s.

| Estratégia | Padrão | Vazão (msgs/s) | Sucesso | Espera por msg | Chamadas por msg |
|---|---|---|---|---|---|
| Retry imediato | uniforme | 33,85 | 100,0% | 0ms | 2,04 |
| | indisponibilidade | 20,65 | 84,3% | 0ms | 3,43 |
| AIMD | uniforme | 11,35 | 99,9% | 55ms | 1,92 |
| | indisponibilidade | 4,21 | 94,7% | 186ms | 2,48 |
| Fixo | uniforme | 7,14 | 99,9% | 102ms | 2,02 |
| | indisponibilidade | 4,10 | 92,5% | 187ms | 2,87 |
| Exponencial | uniforme | 2,65 | 99,9% | 321ms | 2,03 |
| | indisponibilidade | 1,00 | 98,7% | 895ms | 2,48 |
| Exponencial com jitter | uniforme | 2,47 | 99,9% | 346ms | 2,03 |
| | indisponibilidade | 0,76 | 98,5% | 1190ms | 2,67 |
| HPA | uniforme | 0,56 | 100,0% | 1641ms | 2,04 |
| | indisponibilidade | 0,29 | 97,6% | 3128ms | 2,97 |

### O HPA degenera em "delay fixo no teto"

Em 98,1% (uniforme) e 98,9% (indisponibilidade) das esperas, o HPA usou exatamente o teto de
1600ms. Já o AIMD nunca atingiu o teto (espera média de 60ms por espera).

A causa é estrutural. Com `HPA_SETPOINT` = 5% e B ≥ 10%, a taxa de erro observada fica
sempre acima do alvo, e a fórmula `delay × observada/setpoint` só cresce. No simulador, a
taxa de erro não depende do volume de requisições: no padrão uniforme, esperar mais não
reduz o erro observado, então o controlador não tem como alcançar o alvo e o delay sobe até
o teto. O AIMD não sofre disso porque reduz o delay pela metade a cada sucesso.

### Estimativa de tempo do experimento principal

36 combinações × 30 repetições, 1000 mensagens, 20 execuções em paralelo. Duração de cada
execução ≈ 1000 / min(taxa de chegada, vazão da estratégia). Usa a vazão do pior caso
(B=50%, E=10) para os três níveis de B, então é um limite superior.

| Taxa de chegada | HPA como está | HPA calibrado (vazão do exponencial com jitter) | Limite inferior (tudo na taxa de chegada) |
|---|---|---|---|
| 2,0 msgs/s | 14,4 h | 10,2 h | 7,5 h |
| 2,5 msgs/s | 13,4 h | 9,1 h | 6,0 h |
| 3,0 msgs/s | 12,9 h | 8,5 h | 5,0 h |

## Conclusões e decisões

| # | Conclusão | Decisão |
|---|---|---|
| 1 | A escala 1/5 preserva todas as métricas sem unidade de tempo e reduz a espera exatamente na proporção da escala | Adotar a escala 1/5 no experimento principal |
| 2 | Com B=50% e E=10, a estratégia estática mais lenta (exponencial com jitter, uniforme) processa 2,47 msgs/s | Taxa de chegada em torno de 2,5 msgs/s; com indisponibilidade, exponencial e HPA saturam temporariamente, o que é custo legítimo dessas estratégias |
| 3 | O HPA com setpoint de 5% fica no teto em 98% das esperas e domina o tempo total | Calibrar o HPA antes do experimento principal (em aberto) |
| 4 | O AIMD ficou muito mais barato que as demais (60ms de espera média) | Calibrar o AIMD para delays na mesma ordem de grandeza (em aberto) |
| 5 | Com E=10, o padrão uniforme quase não tem descarte (99,9%–100% de sucesso) | Considerar na escolha do Fator E durante a triagem |
| 6 | Com o HPA calibrado e taxa de 2,5–3 msgs/s, o experimento principal fica entre ~6h e ~9h | A meta de 10h é viável, condicionada à decisão 3 |

## Limitações

- A parte B usa só o pior caso (B=50%, E=10). Com B=10% e 30%, ou E menor, as estratégias
  são mais rápidas e o tempo total real fica abaixo do limite superior.
- 3 repetições por combinação na parte B: suficiente para estimar vazão, não para comparar
  estratégias.
- AIMD e HPA rodaram com parâmetros padrão, não calibrados.
