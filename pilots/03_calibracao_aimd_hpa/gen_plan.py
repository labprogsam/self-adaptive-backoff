"""Gera plan.json do piloto 03 (calibração do AIMD e do HPA).

Uso (a partir da raiz do repositório): python3 pilots/03_calibracao_aimd_hpa/gen_plan.py
"""
import json
from pathlib import Path

HERE = Path(__file__).parent

shared = {
    "AMQP_URL": "amqp://guest:guest@localhost:5672/",
    # escala de tempo 1/5 (piloto 02)
    "BASE_BACKOFF": "100ms",
    "MAX_BACKOFF": "1600ms",
    "MIN_BACKOFF": "10ms",
    "HTTP_TIMEOUT": "6s",
    "APISIM_LATENCY_MEAN": "10ms",
    "APISIM_LATENCY_JITTER": "4ms",
    "APISIM_HEALTHY_WINDOW": "2s",
    "APISIM_OUTAGE_WINDOW": "2s",
    # nível intermediário do Fator E; taxa de chegada planejada (piloto 02)
    "MAX_RETRIES": "5",
    "PRODUCER_RATE": "2.5",
    "PRODUCER_POISSON": "true",
}

strategies = []
for sp in (10, 30, 50):
    for w in (10, 20, 50):
        strategies.append((f"hpa_sp{sp}_w{w}", {
            "BACKOFF_STRATEGY": "hpa", "HPA_SETPOINT": str(sp / 100), "HPA_WINDOW_SIZE": str(w)}))
for inc in (20, 100):
    for f in (0.5, 0.8):
        strategies.append((f"aimd_inc{inc}_f{int(f * 10)}", {
            "BACKOFF_STRATEGY": "aimd", "AIMD_INCREMENT": f"{inc}ms", "AIMD_DECREASE_FACTOR": str(f)}))
strategies.append(("exponential_ref", {"BACKOFF_STRATEGY": "exponential"}))

rounds = []
for name, consumer in strategies:
    for b in (10, 30, 50):
        for pattern in ("uniform", "outage"):
            rounds.append({
                "name": f"{name}_b{b}_{pattern}",
                "consumer_env": consumer,
                "apisim_env": {"APISIM_ERROR_RATE": str(b / 100), "APISIM_PATTERN": pattern},
            })

plan = {"repetitions": 3, "shared_env": shared, "rounds": rounds}
(HERE / "plan.json").write_text(json.dumps(plan, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
print(f"{len(rounds)} rodadas × {plan['repetitions']} repetições = {len(rounds) * plan['repetitions']} execuções")
