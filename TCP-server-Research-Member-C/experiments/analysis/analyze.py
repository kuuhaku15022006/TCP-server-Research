#!/usr/bin/env python3
"""Phân tích dữ liệu thô và vẽ biểu đồ (vai trò F).

Đọc experiments/raw-data/c{N}_r{k}/ (requests.csv[.gz], summary.json, cpu.csv) và tạo:
  experiments/analysis/results_per_run.csv    một dòng cho mỗi lần chạy
  experiments/analysis/results_summary.csv    mỗi mức Client: mean, độ lệch chuẩn (n-1), min, max giữa các lần chạy
  experiments/figures/*.png                   latency, throughput, CPU, error rate theo số Client

Công thức đúng theo docs/DESIGN-DECISIONS.md mục 6 (p95 nearest-rank, CPU chỉ tính trong cửa sổ đo).
Cần: pip install pandas matplotlib
"""
import argparse
import json
import math
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RUN_RE = re.compile(r"^c(\d+)_r(\d+)$")


def analyze_run(run: Path):
    summ = json.loads((run / "summary.json").read_text())
    warm, meas = summ["warmup_s"], summ["measure_s"]
    req = run / "requests.csv.gz"
    if not req.exists():
        req = run / "requests.csv"
    df = pd.read_csv(req, usecols=["status", "latency_us"])
    ok = df.loc[df["status"] == "ok", "latency_us"].to_numpy(dtype=float)
    n_ok, n_err = len(ok), int((df["status"] != "ok").sum())
    ok.sort()
    p95 = ok[math.ceil(0.95 * n_ok) - 1] if n_ok else float("nan")  # nearest-rank

    cpu = float("nan")
    cpu_file = run / "cpu.csv"
    if cpu_file.exists():
        c = pd.read_csv(cpu_file)
        w = c[(c["timestamp_s"] > warm) & (c["timestamp_s"] <= warm + meas + 0.5)]
        if len(w):
            cpu = float(w["cpu_percent"].mean())

    m = RUN_RE.match(run.name)
    return {
        "run": run.name, "clients": int(m.group(1)), "repeat": int(m.group(2)),
        "requests_ok": n_ok, "errors": n_err,
        "throughput_rps": n_ok / meas,
        "error_rate": n_err / (n_ok + n_err) if (n_ok + n_err) else 0.0,
        "mean_us": float(ok.mean()) if n_ok else float("nan"),
        "p95_us": float(p95), "min_us": float(ok[0]) if n_ok else float("nan"),
        "max_us": float(ok[-1]) if n_ok else float("nan"),
        "cpu_percent": cpu,
    }


METRICS = ["throughput_rps", "mean_us", "p95_us", "cpu_percent", "error_rate"]


def summarize(per_run: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for clients, g in per_run.groupby("clients"):
        row = {"clients": clients, "runs": len(g)}
        for m in METRICS:
            v = g[m].astype(float)
            row[f"{m}_mean"] = v.mean()
            row[f"{m}_std"] = v.std(ddof=1) if len(v) > 1 else float("nan")
            row[f"{m}_min"] = v.min()
            row[f"{m}_max"] = v.max()
        rows.append(row)
    return pd.DataFrame(rows).sort_values("clients")


def line_plot(per_run, summ, series, ylabel, title, path, logy=False):
    fig, ax = plt.subplots(figsize=(6.4, 4.2))
    for col, label, style in series:
        ax.errorbar(summ["clients"], summ[f"{col}_mean"], yerr=summ[f"{col}_std"].fillna(0), marker=style,
                    capsize=4, linewidth=1.8, label=f"{label} (TB ± độ lệch chuẩn)")
        ax.scatter(per_run["clients"], per_run[col], s=14, alpha=0.45, color="gray", zorder=1)
    ax.set_xscale("log")
    if logy:
        ax.set_yscale("log")
    ax.set_xticks(list(summ["clients"]))
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.set_xlabel("Số Client đồng thời")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.text(0.01, 0.005, "Chấm xám: từng lần chạy", fontsize=7, color="gray")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=str(ROOT / "experiments" / "raw-data"))
    ap.add_argument("--out", default=str(ROOT / "experiments" / "analysis"))
    ap.add_argument("--figures", default=str(ROOT / "experiments" / "figures"))
    args = ap.parse_args()

    runs = sorted(p for p in Path(args.raw).iterdir() if p.is_dir() and RUN_RE.match(p.name)
                  and (p / "summary.json").exists())
    if not runs:
        raise SystemExit(f"Không có dữ liệu trong {args.raw}")
    per_run = pd.DataFrame([analyze_run(r) for r in runs]).sort_values(["clients", "repeat"])
    summ = summarize(per_run)

    out, fig = Path(args.out), Path(args.figures)
    out.mkdir(parents=True, exist_ok=True)
    fig.mkdir(parents=True, exist_ok=True)
    per_run.to_csv(out / "results_per_run.csv", index=False, float_format="%.6g")
    summ.to_csv(out / "results_summary.csv", index=False, float_format="%.6g")

    line_plot(per_run, summ, [("mean_us", "Latency trung bình", "o"), ("p95_us", "Latency p95", "s")],
              "Latency (µs)", "Latency theo số Client", fig / "latency_vs_clients.png")
    line_plot(per_run, summ, [("throughput_rps", "Throughput", "o")], "Throughput (request/giây)",
              "Throughput theo số Client", fig / "throughput_vs_clients.png")
    line_plot(per_run, summ, [("cpu_percent", "CPU Server", "o")], "CPU Server (% của 1 core)",
              "Mức dùng CPU của Server theo số Client", fig / "cpu_vs_clients.png")
    line_plot(per_run, summ, [("error_rate", "Error rate", "o")], "Error rate",
              "Error rate theo số Client", fig / "error_rate_vs_clients.png")

    pd.set_option("display.width", 200)
    show = summ[["clients", "runs"] + [f"{m}_{s}" for m in ["throughput_rps", "mean_us", "p95_us", "cpu_percent"]
                                        for s in ("mean", "std")]]
    print(show.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))
    print(f"\nĐã ghi: {out/'results_per_run.csv'}, {out/'results_summary.csv'} và 4 biểu đồ trong {fig}")


if __name__ == "__main__":
    main()
