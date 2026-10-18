"""Análise do piloto 03: comportamento do delay do AIMD e do HPA por configuração.

Para cada rodada (configuração × B × padrão), calcula a média entre as repetições de:
  - espera média por espera (ms): o delay que a estratégia de fato usou;
  - % das esperas no teto (MAX_BACKOFF) e no piso (MIN_BACKOFF);
  - espera de backoff por mensagem (ms), taxa de sucesso, chamadas à API por mensagem;
  - vazão (mensagens / tempo entre a primeira e a última tentativa).

Uso (a partir da raiz do repositório):
    python3 pilots/03_calibracao_aimd_hpa/analyze.py [--out resumo.csv]
"""
import argparse
import csv
import json
import re
import statistics
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent


def parse_duration_ms(s):
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s)", s)
    return float(m.group(1)) * (1000 if m.group(2) == "s" else 1)


def parse_ts(s):
    return datetime.fromisoformat(re.sub(r"\.(\d{1,9})", lambda m: "." + m.group(1)[:6].ljust(6, "0"), s))


def run_stats(path, max_ms, min_ms):
    msgs = {}
    waits = []
    first = last = None
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t = parse_ts(r["timestamp"])
            first = first or t
            last = t
            m = msgs.setdefault(r["message_id"], {"calls": 0, "wait": 0.0, "final": ""})
            m["calls"] += 1
            w = float(r["wait_after_ms"] or 0)
            m["wait"] += w
            if w > 0:
                waits.append(w)
            if r["final_result"]:
                m["final"] = r["final_result"]
    done = [m for m in msgs.values() if m["final"]]
    n = len(done)
    elapsed = (last - first).total_seconds() if first and last and last > first else float("nan")
    return {
        "espera_por_espera_ms": statistics.fmean(waits) if waits else 0.0,
        "pct_teto": 100.0 * sum(w >= max_ms for w in waits) / len(waits) if waits else 0.0,
        "pct_piso": 100.0 * sum(w <= min_ms for w in waits) / len(waits) if waits else 0.0,
        "espera_por_msg_ms": sum(m["wait"] for m in done) / n,
        "sucesso_pct": 100.0 * sum(m["final"] in ("R1", "R2") for m in done) / n,
        "chamadas_por_msg": sum(m["calls"] for m in done) / n,
        "vazao_msg_s": n / elapsed,
        "mensagens": n,
    }


COLUMNS = ["espera_por_espera_ms", "pct_teto", "pct_piso", "espera_por_msg_ms",
           "sucesso_pct", "chamadas_por_msg", "vazao_msg_s"]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--results", default=str(HERE / "results"))
    ap.add_argument("--out", help="grava a tabela também em CSV")
    args = ap.parse_args()

    plan = json.loads((HERE / "plan.json").read_text(encoding="utf-8"))
    max_ms = parse_duration_ms(plan["shared_env"]["MAX_BACKOFF"])
    min_ms = parse_duration_ms(plan["shared_env"]["MIN_BACKOFF"])
    expected = len(json.loads((HERE / "tasks.json").read_text(encoding="utf-8")))

    rows = []
    for rnd in plan["rounds"]:
        d = Path(args.results) / rnd["name"]
        runs = [run_stats(p, max_ms, min_ms) for p in sorted(d.glob("*.csv"))] if d.is_dir() else []
        runs = [r for r in runs if r["mensagens"] == expected]
        if not runs:
            continue
        row = {"rodada": rnd["name"], "execucoes": len(runs)}
        for c in COLUMNS:
            row[c] = statistics.fmean(r[c] for r in runs)
        rows.append(row)

    header = ["rodada", "exec", "espera/espera", "%teto", "%piso", "espera/msg", "sucesso", "chamadas/msg", "vazão"]
    print("  ".join(f"{h:>13}" if i else f"{h:34}" for i, h in enumerate(header)))
    for r in rows:
        vals = [f"{r['execucoes']:>13}", f"{r['espera_por_espera_ms']:>11.0f}ms", f"{r['pct_teto']:>12.1f}%",
                f"{r['pct_piso']:>12.1f}%", f"{r['espera_por_msg_ms']:>11.0f}ms", f"{r['sucesso_pct']:>12.1f}%",
                f"{r['chamadas_por_msg']:>13.2f}", f"{r['vazao_msg_s']:>13.2f}"]
        print(f"{r['rodada']:34}" + "  ".join(vals))

    if args.out:
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["rodada", "execucoes"] + COLUMNS)
            w.writeheader()
            for r in rows:
                w.writerow({k: (f"{v:.4f}" if isinstance(v, float) else v) for k, v in r.items()})
        print(f"\nCSV gravado em {args.out}")


if __name__ == "__main__":
    main()
