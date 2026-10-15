"""Análise por prefixo: quantas mensagens por execução bastam?

Recalcula as métricas da Seção 3 do Design_experimental.md usando só as
primeiras N mensagens de cada execução, para vários N, e mostra a partir de
qual N as médias entre execuções deixam de mudar de forma relevante.

Isso é válido porque o producer publica as mensagens de tasks.json sempre na
mesma ordem e o consumer as processa uma por vez (Qos=1): as primeiras N
mensagens de uma execução longa se comportam como uma execução de N mensagens.

Entrada: o diretório de métricas do orquestrador (metrics/<rodada>/<run_id>.csv).
A unidade de análise é a execução (Seção 9): para cada rodada e cada N, cada
execução vira um valor por métrica, e a média/IC95% é calculada sobre as
execuções.

Critério de estabilidade: um N é considerado estável quando, para todas as
métricas, a média com N mensagens cai dentro do IC95% da média calculada com
o maior N disponível (a referência). Quando as execuções variam muito pouco
entre si, esse IC fica estreito demais e reprova diferenças sem importância
prática; --tol aceita também diferenças de até tol × |média de referência|
(ex.: --tol 0.02 = 2%).

Só usa a biblioteca padrão do Python.
Uso (a partir da raiz do repositório):
    python3 experiments/prefix_analysis.py metrics
    python3 experiments/prefix_analysis.py metrics --prefixes 250,500,1000,2000 --tol 0.02 --out prefix.csv
"""
import argparse
import csv
import math
import statistics
import sys
from pathlib import Path

DEFAULT_PREFIXES = [250, 500, 1000, 1500, 2000, 2500, 3000]

# Métricas da Seção 3, todas calculadas por execução.
METRICS = [
    ("taxa_sucesso", "Taxa de sucesso (%)"),
    ("espera_media_ms", "Espera de backoff por msg (ms)"),
    ("tentativas_media", "Tentativas por msg"),
    ("sobrecarga_api", "Chamadas à API por msg"),
    ("descarte", "Descarte (%)"),
]

# t de Student bicaudal, 95%, por graus de liberdade (1..30).
T95 = [
    12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
    2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
    2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042,
]


def t95(df):
    if df <= 0:
        return float("nan")
    if df <= len(T95):
        return T95[df - 1]
    if df <= 60:
        return 2.000
    if df <= 120:
        return 1.980
    return 1.960


def load_run(path):
    """Lê o CSV de uma execução e devolve {índice_da_mensagem: dados}.

    O message_id é "<run_id>-<i>", onde i é a posição da mensagem em
    tasks.json (0-indexado) — é o que define a ordem do prefixo.
    """
    messages = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                idx = int(row["message_id"].rsplit("-", 1)[1])
            except (IndexError, ValueError):
                continue
            m = messages.setdefault(idx, {"attempts": 0, "api_calls": 0, "wait_ms": 0.0, "final": ""})
            m["attempts"] += 1
            # Erro de dado nunca chega a acionar a API (Seção 8), então não
            # conta como sobrecarga de chamadas.
            if row["attempt_result"] != "erro_dado":
                m["api_calls"] += 1
            m["wait_ms"] += float(row["wait_after_ms"] or 0)
            if row["final_result"]:
                m["final"] = row["final_result"]
    return messages


def run_metrics(messages, n):
    """Métricas de uma execução considerando só as mensagens 0..n-1.

    Devolve None se a execução não tiver resultado final para todas elas.
    """
    prefix = [messages.get(i) for i in range(n)]
    if any(m is None or not m["final"] for m in prefix):
        return None
    success = sum(1 for m in prefix if m["final"] in ("R1", "R2"))
    discarded = sum(1 for m in prefix if m["final"] == "R3")
    return {
        "taxa_sucesso": 100.0 * success / n,
        "espera_media_ms": sum(m["wait_ms"] for m in prefix) / n,
        "tentativas_media": sum(m["attempts"] for m in prefix) / n,
        "sobrecarga_api": sum(m["api_calls"] for m in prefix) / n,
        "descarte": 100.0 * discarded / n,
    }


def summarize(values):
    """Média, desvio padrão e meia-largura do IC95% entre execuções."""
    k = len(values)
    mean = statistics.fmean(values)
    if k < 2:
        return mean, float("nan"), float("nan")
    sd = statistics.stdev(values)
    return mean, sd, t95(k - 1) * sd / math.sqrt(k)


def analyze_round(round_dir, prefixes):
    runs = {p.stem: load_run(p) for p in sorted(round_dir.glob("*.csv"))}
    if not runs:
        return None

    complete = {rid: sum(1 for m in msgs.values() if m["final"]) for rid, msgs in runs.items()}
    max_n = max(complete.values())
    ns = [n for n in prefixes if n <= max_n]
    if not ns:
        return None

    results = {}
    for n in ns:
        per_run = [run_metrics(msgs, n) for msgs in runs.values()]
        valid = [r for r in per_run if r is not None]
        if not valid:
            continue
        results[n] = {
            "runs": len(valid),
            "skipped": len(per_run) - len(valid),
            "stats": {key: summarize([r[key] for r in valid]) for key, _ in METRICS},
        }
    return results


def tolerance(ref_mean, ref_hw, tol):
    """Diferença aceita em relação à referência: o IC95% ou tol relativo, o maior."""
    hw = 0.0 if math.isnan(ref_hw) else ref_hw
    return max(hw, tol * abs(ref_mean)) + 1e-9


def stable_from(results, tol):
    """Menor N cujas médias ficam todas dentro da tolerância da referência (maior N)."""
    ref_n = max(results)
    ref = results[ref_n]["stats"]
    stable = None
    for n in sorted(results, reverse=True):
        ok = True
        for key, _ in METRICS:
            mean, _, _ = results[n]["stats"][key]
            ref_mean, _, ref_hw = ref[key]
            if abs(mean - ref_mean) > tolerance(ref_mean, ref_hw, tol):
                ok = False
                break
        if not ok:
            break
        stable = n
    return ref_n, stable


def fmt(mean, hw):
    if math.isnan(hw):
        return f"{mean:.2f}"
    return f"{mean:.2f} ± {hw:.2f}"


def print_round(name, results, tol):
    ref_n, stable = stable_from(results, tol)
    print(f"\n=== {name} (referência: N={ref_n}) ===")
    header = ["N", "execuções"] + [label for _, label in METRICS]
    rows = []
    for n in sorted(results):
        r = results[n]
        runs = str(r["runs"]) + (f" (-{r['skipped']})" if r["skipped"] else "")
        rows.append([str(n), runs] + [fmt(r["stats"][k][0], r["stats"][k][2]) for k, _ in METRICS])
    widths = [max(len(h), *(len(row[i]) for row in rows)) for i, h in enumerate(header)]
    print("  ".join(h.ljust(w) for h, w in zip(header, widths)))
    for row in rows:
        print("  ".join(c.ljust(w) for c, w in zip(row, widths)))
    if results[ref_n]["runs"] < 2:
        print("-> só 1 execução: sem IC95%, não dá para avaliar estabilidade.")
    else:
        crit = "IC95% da referência" + (f" ou {tol:.0%} dela" if tol else "")
        if stable == ref_n:
            print(f"-> nenhum N menor que {ref_n} ficou dentro do {crit} em todas as métricas.")
        else:
            print(f"-> estável a partir de N={stable} (médias dentro do {crit} em todas as métricas).")


def write_csv(path, all_results, tol):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["round", "n_messages", "runs", "metric", "mean", "std", "ci95_half_width",
                    "ci95_low", "ci95_high", "delta_vs_ref", "within_ref_ci"])
        for name, results in all_results.items():
            ref_n = max(results)
            for n in sorted(results):
                for key, _ in METRICS:
                    mean, sd, hw = results[n]["stats"][key]
                    ref_mean, _, ref_hw = results[ref_n]["stats"][key]
                    delta = mean - ref_mean
                    within = str(abs(delta) <= tolerance(ref_mean, ref_hw, tol)).lower()
                    w.writerow([name, n, results[n]["runs"], key, f"{mean:.6f}", f"{sd:.6f}", f"{hw:.6f}",
                                f"{mean - hw:.6f}", f"{mean + hw:.6f}", f"{delta:.6f}", within])


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("metrics_dir", help="diretório de métricas do orquestrador (ex.: metrics)")
    ap.add_argument("--prefixes", default=",".join(map(str, DEFAULT_PREFIXES)),
                    help="valores de N separados por vírgula (padrão: %(default)s)")
    ap.add_argument("--tol", type=float, default=0.0,
                    help="tolerância relativa extra à referência, ex. 0.02 = 2%% (padrão: 0, só o IC95%%)")
    ap.add_argument("--rounds", help="só estas rodadas (nomes separados por vírgula)")
    ap.add_argument("--out", help="grava também um CSV com todos os números")
    args = ap.parse_args()

    prefixes = sorted({int(x) for x in args.prefixes.split(",") if x.strip()})
    base = Path(args.metrics_dir)
    round_dirs = sorted(d for d in base.iterdir() if d.is_dir())
    if args.rounds:
        wanted = set(args.rounds.split(","))
        round_dirs = [d for d in round_dirs if d.name in wanted]
    if not round_dirs:
        sys.exit(f"nenhuma rodada encontrada em {base} (esperado: {base}/<rodada>/<run_id>.csv)")

    all_results = {}
    for d in round_dirs:
        results = analyze_round(d, prefixes)
        if not results:
            print(f"\n=== {d.name} === sem dados suficientes para os prefixos pedidos")
            continue
        all_results[d.name] = results
        print_round(d.name, results, args.tol)

    if args.out and all_results:
        write_csv(args.out, all_results, args.tol)
        print(f"\nCSV gravado em {args.out}")


if __name__ == "__main__":
    main()
