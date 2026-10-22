"""Gera plan.json do ensaio geral: o desenho do experimento principal (Seção 8) com 2 repetições.

Fatorial completo A × B × C × E: 6 estratégias × 3 taxas de erro × 2 padrões ×
3 níveis de número máximo de tentativas extras = 108 combinações. Todos os
demais parâmetros ficam nos defaults calibrados (escala 1/5, pilotos 02 e 03);
Fator D fixado em 2,5 msgs/s (default de PRODUCER_RATE).

Uso (a partir da raiz do repositório): python3 experiments/02_ensaio_geral/gen_plan.py
"""
import json
from pathlib import Path

HERE = Path(__file__).parent

STRATEGIES = ["none", "fixed", "exponential", "exponential_jitter", "aimd", "hpa"]  # Fator A
ERROR_RATES = [10, 30, 50]                                                        # Fator B (%)
PATTERNS = ["uniform", "outage"]                                                  # Fator C
MAX_RETRIES = [3, 5, 10]                                                          # Fator E

rounds = []
for a in STRATEGIES:
    for b in ERROR_RATES:
        for c in PATTERNS:
            for e in MAX_RETRIES:
                rounds.append({
                    "name": f"{a}_b{b}_{c}_e{e}",
                    "consumer_env": {"BACKOFF_STRATEGY": a, "MAX_RETRIES": str(e)},
                    "apisim_env": {"APISIM_ERROR_RATE": str(b / 100), "APISIM_PATTERN": c},
                })

plan = {
    "repetitions": 2,
    "shared_env": {"AMQP_URL": "amqp://guest:guest@localhost:5672/"},
    "rounds": rounds,
}
(HERE / "plan.json").write_text(json.dumps(plan, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
print(f"{len(rounds)} combinações × {plan['repetitions']} repetições = {len(rounds) * plan['repetitions']} execuções")
