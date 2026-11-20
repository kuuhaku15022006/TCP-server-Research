#!/usr/bin/env python3
"""Experiment runner (vai trò D): chạy 1/10/50 Client x 3 lần, đo CPU, lưu dữ liệu thô.

Mỗi lần chạy:
  1. Khởi động Server mới (trạng thái sạch), chờ port mở.
  2. Chạy loadgen; khi loadgen in "START" (= t0) thì bắt đầu lấy mẫu CPU của process Server mỗi 1 giây.
  3. Ghi vào experiments/raw-data/c{N}_r{k}/ :
       requests.csv.gz, summary.json (loadgen) + cpu.csv, meta.json, server.log, loadgen.log (runner)
  4. Tắt Server, nghỉ cooldown_s giây.
Thứ tự xen kẽ: lần 1 của mọi mức, rồi lần 2 của mọi mức, ...

Ví dụ:
    python experiments/analysis/run_experiments.py                       # cấu hình chính thức default.json
    python experiments/analysis/run_experiments.py --config experiments/configs/quick.json --raw-dir /tmp/raw
Cần: pip install psutil
"""
import argparse
import datetime as dt
import gzip
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import psutil

ROOT = Path(__file__).resolve().parents[2]


def find_exe(name, explicit=None):
    if explicit:
        p = Path(explicit)
        if not p.exists():
            sys.exit(f"Không thấy {p}")
        return p
    exe = name + (".exe" if os.name == "nt" else "")
    sub = "server" if name == "server" else "load-generator"
    for base in [ROOT / "build" / "src" / sub, ROOT / "build" / "src" / sub / "Release", ROOT / "build", ROOT]:
        if (base / exe).exists():
            return base / exe
    sys.exit(f"Không tìm thấy {exe}. Hãy build trước (xem README) hoặc dùng --{name} <đường dẫn>")


def wait_port(ip, port, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((ip, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def tool_version(cmd):
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return (out.stdout or out.stderr).strip().splitlines()[0]
    except Exception:  # noqa: BLE001
        return None


def machine_info(server, loadgen):
    freq = psutil.cpu_freq()
    git = tool_version(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"])
    return {
        "os": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_logical_cores": psutil.cpu_count(logical=True),
        "cpu_physical_cores": psutil.cpu_count(logical=False),
        "cpu_max_mhz": round(freq.max) if freq else None,
        "ram_gb": round(psutil.virtual_memory().total / 2**30, 1),
        "python": platform.python_version(),
        "compiler": tool_version(["g++", "--version"]) or tool_version(["clang++", "--version"]),
        "git_commit": git,
        "server_binary": str(server),
        "loadgen_binary": str(loadgen),
    }


def stop_server(proc):
    if proc.poll() is not None:
        return
    if os.name == "nt":
        proc.terminate()
    else:
        proc.send_signal(signal.SIGINT)  # để Server in thống kê tổng kết vào server.log
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def run_one(cfg, level, rep, raw_dir, server, loadgen, keep_csv, sysinfo):
    s, l = cfg["server"], cfg["load"]
    run_dir = raw_dir / f"c{level}_r{rep}"
    run_dir.mkdir(parents=True, exist_ok=True)
    started = dt.datetime.now().astimezone().isoformat(timespec="seconds")

    with open(run_dir / "server.log", "w") as slog:
        srv = subprocess.Popen(
            [str(server), "--ip", s["ip"], "--port", str(s["port"]), "--threads", str(s["threads"]),
             "--max-payload", str(s["max_payload"]), "--idle-timeout", str(s["idle_timeout_s"])],
            stdout=subprocess.DEVNULL, stderr=slog)
        try:
            if not wait_port(s["ip"], s["port"]):
                raise RuntimeError("Server không khởi động được (port bận?)")
            ps = psutil.Process(srv.pid)
            ps.cpu_percent(interval=None)  # mồi bộ đếm

            lg = subprocess.Popen(
                [str(loadgen), "--ip", s["ip"], "--port", str(s["port"]), "--clients", str(level),
                 "--payload", str(l["payload_bytes"]), "--warmup", str(l["warmup_s"]),
                 "--duration", str(l["measure_s"]), "--timeout-ms", str(l["request_timeout_ms"]),
                 "--out-dir", str(run_dir)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            # Chờ dòng START = t0
            got_start = False
            for line in lg.stdout:
                if line.strip() == "START":
                    got_start = True
                    break
            samples = []
            if got_start:
                t0 = time.monotonic()
                ps.cpu_percent(interval=None)  # mồi lại đúng tại t0
                k = 1
                while lg.poll() is None:
                    time.sleep(max(0.0, t0 + k - time.monotonic()))
                    samples.append((round(time.monotonic() - t0, 3), ps.cpu_percent(interval=None)))
                    k += 1
            out, err = lg.communicate()
            (run_dir / "loadgen.log").write_text((out or "") + (err or ""))
            if lg.returncode != 0:
                raise RuntimeError(f"loadgen lỗi (mã {lg.returncode}): {(err or '').strip()}")
        finally:
            stop_server(srv)

    with open(run_dir / "cpu.csv", "w") as f:
        f.write("timestamp_s,cpu_percent\n")
        for t, c in samples:
            f.write(f"{t},{c}\n")

    csv_path = run_dir / "requests.csv"
    if csv_path.exists() and not keep_csv:  # nén để repo gọn (hàng triệu dòng)
        with open(csv_path, "rb") as fin, gzip.open(run_dir / "requests.csv.gz", "wb", compresslevel=6) as fout:
            shutil.copyfileobj(fin, fout)
        csv_path.unlink()

    meta = {"run": run_dir.name, "clients": level, "repeat": rep, "started": started,
            "server_args": s, "load_args": l, "machine": sysinfo}
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    return json.loads((run_dir / "summary.json").read_text())


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(ROOT / "experiments" / "configs" / "default.json"))
    ap.add_argument("--raw-dir", default=str(ROOT / "experiments" / "raw-data"))
    ap.add_argument("--server")
    ap.add_argument("--loadgen")
    ap.add_argument("--levels", type=int, nargs="+", help="ghi đè client_levels")
    ap.add_argument("--repeats", type=int, help="ghi đè repeats")
    ap.add_argument("--overwrite", action="store_true", help="chạy lại cả lần đã có kết quả")
    ap.add_argument("--keep-csv", action="store_true", help="không nén requests.csv")
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text())
    levels = args.levels or cfg["client_levels"]
    repeats = args.repeats or cfg["repeats"]
    raw_dir = Path(args.raw_dir)
    server, loadgen = find_exe("server", args.server), find_exe("loadgen", args.loadgen)
    if max(levels) > cfg["server"]["threads"]:
        sys.exit("--threads của Server phải >= số Client lớn nhất (nếu không Client dư phải chờ trong hàng đợi)")
    sysinfo = machine_info(server, loadgen)

    total = len(levels) * repeats
    print(f"Cấu hình: {Path(args.config).name} | mức Client {levels} x {repeats} lần | {total} lần chạy")
    print("Máy:", sysinfo["os"], "|", sysinfo["cpu_logical_cores"], "core logic |", sysinfo["ram_gb"], "GB RAM")
    print("Gợi ý: đóng ứng dụng nền, cắm điện (laptop) trong lúc chạy.\n")

    done = 0
    for rep in range(1, repeats + 1):          # xen kẽ: mọi mức ở lần 1, rồi lần 2, ...
        for level in levels:
            done += 1
            run_dir = raw_dir / f"c{level}_r{rep}"
            tag = f"[{done}/{total}] {level} Client, lần {rep}"
            if (run_dir / "summary.json").exists() and not args.overwrite:
                print(tag, "- đã có, bỏ qua (dùng --overwrite để chạy lại)")
                continue
            summ = run_one(cfg, level, rep, raw_dir, server, loadgen, args.keep_csv, sysinfo)
            print(f"{tag}: {summ['throughput_rps']:.0f} req/s, mean {summ['latency_us']['mean']:.0f} us, "
                  f"p95 {summ['latency_us']['p95']:.0f} us, error_rate {summ['error_rate']:.4f}")
            if done < total:
                time.sleep(cfg.get("cooldown_s", 5))
    print("\nXong. Dữ liệu thô:", raw_dir)
    print("Bước tiếp: python experiments/analysis/analyze.py && python tests/verify_results.py")


if __name__ == "__main__":
    main()
