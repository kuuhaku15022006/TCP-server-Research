#!/usr/bin/env python3
"""Kiểm chứng ĐỘC LẬP kết quả thí nghiệm (vai trò E).

Chỉ dùng thư viện chuẩn, KHÔNG dùng code của load generator hay analyze.py.
Với mỗi lần chạy trong experiments/raw-data/c{N}_r{k}/ tính lại từ requests.csv(.gz):
  - số request thành công, số lỗi theo loại
  - throughput và error rate
  - latency trung bình và p95 (nearest-rank)
rồi so với summary.json (do loadgen ghi) và với results_per_run.csv (do analyze.py ghi, nếu có).
Ngoài ra kiểm tra tính hợp lệ: mọi request nằm trong cửa sổ đo, clientId trong requestId khớp client_id,
requestId không trùng, và số Client thực sự khớp cấu hình.

Ví dụ:
    python tests/verify_results.py
    python tests/verify_results.py --raw experiments/raw-data --out tests/verification_report.csv
Mã thoát 0 nếu mọi kiểm tra đạt.
"""
import argparse
import csv
import gzip
import json
import math
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOL = 1e-6


def open_requests(run: Path):
    gz, plain = run / "requests.csv.gz", run / "requests.csv"
    if gz.exists():
        return gzip.open(gz, "rt", newline="")
    if plain.exists():
        return open(plain, newline="")
    return None


def recompute(run: Path):
    summ = json.loads((run / "summary.json").read_text())
    warm_us = summ["warmup_s"] * 1_000_000
    end_us = (summ["warmup_s"] + summ["measure_s"]) * 1_000_000
    f = open_requests(run)
    if f is None:
        raise FileNotFoundError("thiếu requests.csv(.gz)")
    ok_lat, errs = [], {"timeout": 0, "mismatch": 0, "conn_error": 0}
    problems, clients, seen = [], set(), set()
    with f:
        for row in csv.DictReader(f):
            cid, rid = int(row["client_id"]), int(row["request_id"])
            st, t = row["status"], int(row["send_time_us"])
            clients.add(cid)
            if not (warm_us <= t < end_us):
                problems.append("request ngoài cửa sổ đo")
            if rid != 0:
                if (rid >> 32) != cid:
                    problems.append("clientId trong requestId không khớp client_id")
                if rid in seen:
                    problems.append("requestId bị trùng")
                seen.add(rid)
            if st == "ok":
                if row["latency_us"] == "":
                    problems.append("dòng ok thiếu latency")
                else:
                    ok_lat.append(int(row["latency_us"]))
            elif st in errs:
                errs[st] += 1
            else:
                problems.append("status lạ: " + st)
    n_ok, n_err = len(ok_lat), sum(errs.values())
    ok_lat.sort()
    res = {
        "requests_ok": n_ok,
        "errors": n_err,
        "throughput": n_ok / summ["measure_s"],
        "error_rate": n_err / (n_ok + n_err) if (n_ok + n_err) else 0.0,
        "mean": sum(ok_lat) / n_ok if n_ok else 0.0,
        "p95": ok_lat[math.ceil(0.95 * n_ok) - 1] if n_ok else 0.0,
    }
    if len(clients) > summ["clients"]:
        problems.append("số client_id nhiều hơn cấu hình")
    return summ, res, sorted(set(problems))


def close(a, b, rel=1e-6, absol=1e-3):
    return abs(a - b) <= max(absol, rel * max(abs(a), abs(b)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=str(ROOT / "experiments" / "raw-data"))
    ap.add_argument("--per-run", default=str(ROOT / "experiments" / "analysis" / "results_per_run.csv"))
    ap.add_argument("--out", help="ghi báo cáo kiểm chứng ra CSV")
    args = ap.parse_args()

    runs = sorted(p for p in Path(args.raw).glob("c*_r*") if (p / "summary.json").exists())
    if not runs:
        print("Không có dữ liệu trong", args.raw)
        return 2

    per_run = {}
    pr = Path(args.per_run)
    if pr.exists():
        with open(pr, newline="") as f:
            for row in csv.DictReader(f):
                per_run[row["run"]] = row

    report, failed = [], 0
    print(f"{'run':10} {'check':28} {'recomputed':>14} {'reported':>14}  result")
    for run in runs:
        try:
            summ, r, problems = recompute(run)
        except Exception as e:  # noqa: BLE001
            print(f"{run.name:10} {'READ':28} {'-':>14} {'-':>14}  FAIL ({e})")
            failed += 1
            continue
        checks = [
            ("requests_ok (summary.json)", r["requests_ok"], summ["requests_ok"], 0),
            ("errors (summary.json)", r["errors"], sum(summ["errors"].values()), 0),
            ("throughput_rps (summary.json)", r["throughput"], summ["throughput_rps"], 1),
            ("error_rate (summary.json)", r["error_rate"], summ["error_rate"], 1),
            ("latency mean (summary.json)", r["mean"], summ["latency_us"]["mean"], 1),
            ("latency p95 (summary.json)", r["p95"], summ["latency_us"]["p95"], 1),
        ]
        if run.name in per_run:
            row = per_run[run.name]
            checks += [
                ("requests_ok (analyze.py)", r["requests_ok"], float(row["requests_ok"]), 0),
                ("throughput (analyze.py)", r["throughput"], float(row["throughput_rps"]), 1),
                ("error_rate (analyze.py)", r["error_rate"], float(row["error_rate"]), 1),
                ("p95 (analyze.py)", r["p95"], float(row["p95_us"]), 1),
            ]
        for name, mine, theirs, approx in checks:
            ok = close(mine, theirs, 1e-5, 1e-3) if approx else mine == theirs
            failed += 0 if ok else 1
            report.append([run.name, name, mine, theirs, "PASS" if ok else "FAIL"])
            print(f"{run.name:10} {name:28} {mine:>14.4f} {theirs:>14.4f}  {'PASS' if ok else 'FAIL'}")
        for pbl in problems:
            failed += 1
            report.append([run.name, "validity: " + pbl, "", "", "FAIL"])
            print(f"{run.name:10} {'validity':28} {'':>14} {'':>14}  FAIL ({pbl})")
        if not problems:
            report.append([run.name, "validity (window, ids, clients)", "", "", "PASS"])
            print(f"{run.name:10} {'validity (window/ids/clients)':28} {'':>14} {'':>14}  PASS")

    print(f"\n{len(runs)} lần chạy được kiểm chứng; {failed} kiểm tra thất bại")
    if len(runs) < 3:
        print("CẢNH BÁO: đề yêu cầu kiểm chứng ít nhất 3 trường hợp, hiện chỉ có", len(runs))
    if args.out:
        with open(args.out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["run", "check", "recomputed", "reported", "result"])
            w.writerows(report)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
