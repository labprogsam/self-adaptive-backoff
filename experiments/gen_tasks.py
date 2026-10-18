"""Gera tasks.json: 1000 pedidos sintéticos pequenos (~100 bytes cada).

O payload é mantido pequeno de propósito: nem o consumer nem o apisim
validam o conteúdo além de ser um objeto JSON, então o tamanho não
influencia o que o experimento mede (Design_experimental.md, Seção 7.1).

Semente fixa para o arquivo ser idêntico se for regenerado.
Uso (a partir da raiz do repositório): python3 experiments/gen_tasks.py tasks.json
"""
import json
import random
import sys

COUNT = 1000  # volume por execução definido no piloto 01 (pilots/01_piloto_inicial)

rng = random.Random(42)
actions = ["process"] * 8 + ["cancel", "refund"]
priorities = ["low", "normal", "normal", "normal", "high"]


def make(i):
    return {
        "order_id": i,
        "action": rng.choice(actions),
        "priority": rng.choice(priorities),
        "customer_id": "C-%06d" % rng.randint(1, 999999),
        "total": round(rng.uniform(5, 2000), 2),
    }


orders = [make(i) for i in range(1, COUNT + 1)]
with open(sys.argv[1], "w", encoding="utf-8") as f:
    f.write("[\n")
    f.write(",\n".join(json.dumps(o, separators=(",", ":"), ensure_ascii=False) for o in orders))
    f.write("\n]\n")
