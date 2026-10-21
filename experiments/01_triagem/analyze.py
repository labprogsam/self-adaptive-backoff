"""Análise da triagem: alocação de variação (Jain, cap. 20–22) do fatorial A × B × C × E.

Para cada execução, calcula as métricas da Seção 3 do Design_experimental.md:
  - taxa de sucesso (%) — complementar ao percentual de descarte, então as duas
    têm exatamente a mesma alocação de variação;
  - espera de backoff por mensagem (ms);
  - tentativas por mensagem (= chamadas à API por mensagem: não há erro de dado).

Depois decompõe a variação total de cada métrica entre os efeitos principais
(A, B, C, E), todas as interações e o erro experimental (variação entre
repetições), pelas somas de quadrados de um fatorial completo balanceado:

    SS(S) = Σ_{T ⊆ S} (−1)^{|S|−|T|} · Q(T),   Q(T) = Σ_células de T  n · média²

A fração SS(efeito) / SST é a "porcentagem da variação explicada" pelo efeito.

Critério de decisão sobre E (Seção 8): se E + A×E explicam menos de 5% da
variação em todas as métricas, E é fixado no nível intermediário (5) no
experimento principal; caso contrário, a decisão volta para análise.

Uso (a partir da raiz do repositório):
    python3 experiments/01_triagem/analyze.py [--out alocacao.csv]
"""
import argparse
import csv
import itertools
import statistics
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).parent
FACTORS = [("A", "factor_a_strategy"), ("B", "factor_b_error_rate"),
           ("C", "factor_c_pattern"), ("E", "factor_e_max_retries")]
METRICS = [("sucesso_pct", "Taxa de sucesso (%)"),
           ("espera_por_msg_ms", "Espera de backoff por msg (ms)"),
           ("tentativas_por_msg", "Tentativas por msg")]
THRESHOLD = 5.0  # % da variação: limite do critério sobre E


def run_metrics(path):
    msgs = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            m = msgs.setdefault(r["message_id"], {"calls": 0, "wait": 0.0, "final": ""})
            m["calls"] += 1
            m["wait"] += float(r["wait_after_ms"] or 0)
            if r["final_result"]:
                m["final"] = r["final_result"]
    done = [m for m in msgs.values() if m["final"]]
    n = len(done)
    return n, {
        "sucesso_pct": 100.0 * sum(m["final"] in ("R1", "R2") for m in done) / n,
        "espera_por_msg_ms": sum(m["wait"] for m in done) / n,
        "tentativas_por_msg": sum(m["calls"] for m in done) / n,
    }


def load(results):
    """Uma observação por execução: (níveis dos fatores, métricas)."""
    obs = []
    for r in csv.DictReader(open(results / "runs_summary.csv", newline="", encoding="utf-8")):
        if r["status"] != "ok":
            continue
        n, m = run_metrics(results / r["round_name"] / f"{r['run_id']}.csv")
        if n != int(r["messages"]):
            continue
        obs.append((tuple(r[col] for _, col in FACTORS), m))
    return obs


def allocation(obs, metric):
    """Fração da variação total explicada por cada efeito (e pelo erro)."""
    ys = [m[metric] for _, m in obs]
    k = len(FACTORS)

    def q(subset):
        groups = defaultdict(list)
        for levels, m in obs:
            groups[tuple(levels[i] for i in subset)].append(m[metric])
        return sum(len(v) * statistics.fmean(v) ** 2 for v in groups.values())

    qs = {s: q(s) for r in range(k + 1) for s in itertools.combinations(range(k), r)}
    sst = sum(y * y for y in ys) - qs[()]
    ss = {}
    for r in range(1, k + 1):
        for s in itertools.combinations(range(k), r):
            ss[s] = sum((-1) ** (len(s) - len(t)) * qs[t]
                        for rr in range(len(s) + 1) for t in itertools.combinations(s, rr))
    sse = sst - sum(ss.values())
    name = lambda s: "×".join(FACTORS[i][0] for i in s)
    pct = {name(s): 100.0 * v / sst for s, v in ss.items()}
    pct["erro"] = 100.0 * sse / sst
    return pct


def check_balance(obs):
    cells = defaultdict(int)
    for levels, _ in obs:
        cells[levels] += 1
    levels = [sorted({lv[i] for lv, _ in obs}) for i in range(len(FACTORS))]
    expected = 1
    for lv in levels:
        expected *= len(lv)
    reps = set(cells.values())
    return len(cells), expected, reps


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--results", default=str(HERE / "results"))
    ap.add_argument("--out", help="grava a alocação de variação também em CSV")
    args = ap.parse_args()

    obs = load(Path(args.results))
    cells, expected, reps = check_balance(obs)
    print(f"{len(obs)} execuções válidas em {cells}/{expected} combinações; repetições por combinação: {sorted(reps)}")
    if cells != expected or len(reps) != 1:
        print("AVISO: desenho desbalanceado — a decomposição abaixo não é exata.")

    table = {metric: allocation(obs, metric) for metric, _ in METRICS}
    effects = sorted(next(iter(table.values())).keys(),
                     key=lambda e: (e == "erro", e.count("×"), e))

    print(f"\n{'Efeito':12}" + "".join(f"{label:>34}" for _, label in METRICS))
    for e in effects:
        print(f"{e:12}" + "".join(f"{table[m][e]:>33.2f}%" for m, _ in METRICS))

    print(f"\nCritério sobre E: E + A×E < {THRESHOLD:.0f}% da variação em todas as métricas")
    ok = True
    for metric, label in METRICS:
        e_part = table[metric]["E"] + table[metric]["A×E"]
        e_all = sum(v for k, v in table[metric].items() if "E" in k.split("×"))
        ok &= e_part < THRESHOLD
        print(f"  {label:34} E + A×E = {e_part:6.2f}%   (todos os termos com E: {e_all:6.2f}%)")
    print("-> critério atendido: fixar E = 5 no experimento principal" if ok
          else "-> critério NÃO atendido: E tem efeito relevante; decisão volta para análise")

    if args.out:
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["efeito"] + [m for m, _ in METRICS])
            for e in effects:
                w.writerow([e] + [f"{table[m][e]:.4f}" for m, _ in METRICS])
        print(f"\nCSV gravado em {args.out}")


if __name__ == "__main__":
    main()
