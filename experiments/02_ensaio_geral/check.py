"""Verificações do ensaio geral antes do experimento principal.

1. Execuções: todas terminaram com status ok e processaram todas as mensagens?
2. Tempo: quanto durou o ensaio e qual a projeção para o experimento principal (10 repetições)?
3. Cronograma de falhas: toda execução no padrão de indisponibilidade tem janelas registradas,
   e as tentativas feitas dentro das janelas de indisponibilidade falharam?
4. Variação entre repetições: qual IC95% esperar na taxa de sucesso com 10 repetições?

Uso (a partir da raiz do repositório): python3 experiments/02_ensaio_geral/check.py
"""
import csv
import re
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent
RESULTS = HERE / "results"
MAIN_REPS = 10
CONCURRENCY = 20
EDGE_S = 0.02  # tentativas a menos de 20ms de uma troca de janela não entram na verificação


def ts(s):
    return datetime.fromisoformat(re.sub(r"\.(\d{1,9})", lambda m: "." + m.group(1)[:6].ljust(6, "0"), s))


def main():
    runs = list(csv.DictReader(open(RESULTS / "runs_summary.csv", newline="", encoding="utf-8")))
    plan_total = sum(1 for _ in csv.DictReader(open(RESULTS / "execution_order.csv", newline="", encoding="utf-8")))

    # 1. execuções
    status = defaultdict(int)
    for r in runs:
        status[r["status"]] += 1
    print("1. Execuções")
    print(f"   {len(runs)}/{plan_total} registradas; status: {dict(status)}")
    for r in runs:
        if r["status"] != "ok":
            print(f"   FALHOU: {r['run_id']}: {r['detail']}")

    # 2. tempo
    start = min(ts(r["started_at"]) for r in runs)
    end = max(ts(r["finished_at"]) for r in runs)
    wall_h = (end - start).total_seconds() / 3600
    work_h = sum(float(r["duration_s"]) for r in runs) / 3600
    reps = max(int(r["repetition"]) for r in runs)
    longest = max(runs, key=lambda r: float(r["duration_s"]))
    print("\n2. Tempo")
    print(f"   duração total do ensaio ({reps} repetições): {wall_h:.2f} h")
    print(f"   execução mais longa: {longest['run_id']} ({float(longest['duration_s']) / 60:.1f} min)")
    print(f"   projeção do experimento principal ({MAIN_REPS} repetições, concorrência {CONCURRENCY}): "
          f"{work_h * MAIN_REPS / reps / CONCURRENCY:.1f} h de trabalho dividido entre as vagas "
          f"(o tempo real tende a ficar um pouco acima, pelas execuções longas no fim)")

    # 3. cronograma de falhas
    windows = defaultdict(list)
    for w in csv.DictReader(open(RESULTS / "failure_windows.csv", newline="", encoding="utf-8")):
        windows[w["run_id"]].append((ts(w["start"]).timestamp(), ts(w["end"]).timestamp(), w["kind"]))
    outage_runs = [r for r in runs if r["factor_c_pattern"] == "outage" and r["status"] == "ok"]
    missing = [r["run_id"] for r in outage_runs if not windows.get(r["run_id"])]
    inside = failed = 0
    for r in outage_runs:
        wins = windows.get(r["run_id"], [])
        for a in csv.DictReader(open(RESULTS / r["round_name"] / f"{r['run_id']}.csv", newline="", encoding="utf-8")):
            t = ts(a["timestamp"]).timestamp()
            for s, e, kind in wins:
                if kind == "outage" and s + EDGE_S <= t < e - EDGE_S:
                    inside += 1
                    failed += a["attempt_result"] != "sucesso"
                    break
    print("\n3. Cronograma de falhas")
    print(f"   execuções com indisponibilidade: {len(outage_runs)}; sem janelas registradas: {len(missing)}")
    print(f"   tentativas dentro de janelas de indisponibilidade: {inside}; falharam: {failed} "
          f"({100 * failed / max(inside, 1):.2f}%, esperado 100%)")

    # 4. variação entre repetições
    succ = defaultdict(list)
    for r in runs:
        if r["status"] != "ok":
            continue
        msgs = {}
        for a in csv.DictReader(open(RESULTS / r["round_name"] / f"{r['run_id']}.csv", newline="", encoding="utf-8")):
            if a["final_result"]:
                msgs[a["message_id"]] = a["final_result"]
        succ[r["round_name"]].append(100 * sum(v in ("R1", "R2") for v in msgs.values()) / len(msgs))
    sds = sorted(statistics.stdev(v) for v in succ.values() if len(v) > 1)
    p90 = sds[int(0.9 * len(sds))]
    t9 = 2.262  # t de Student, 95%, 9 graus de liberdade
    print("\n4. Variação entre repetições (taxa de sucesso)")
    print(f"   desvio padrão por combinação: mediana {statistics.median(sds):.2f} p.p., "
          f"percentil 90 {p90:.2f} p.p., máximo {sds[-1]:.2f} p.p.")
    print(f"   IC95% esperado com {MAIN_REPS} repetições (percentil 90): ±{t9 * p90 / MAIN_REPS ** 0.5:.2f} p.p.")
    print("   (com só 2 repetições por combinação, o desvio de cada uma é uma estimativa grosseira)")


if __name__ == "__main__":
    main()
