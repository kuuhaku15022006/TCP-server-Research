import argparse
import csv
import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def now_iso():
    return datetime.now().astimezone().isoformat(timespec="seconds")


def wait_for_server(host, port, process, timeout_s=15):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Server exited early (exit code {process.returncode})")
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"Server did not become ready at {host}:{port} within {timeout_s}s")


def stop_server(proc):
    if proc.poll() is not None:
        return "already_exited"
    try:
        if os.name == "nt":
            proc.send_signal(getattr(signal, "CTRL_BREAK_EVENT", signal.SIGTERM))
        else:
            proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
        return "signal_stopped"
    except Exception:
        try:
            proc.terminate()
            proc.wait(timeout=3)
            return "terminated"
        except Exception:
            proc.kill()
            proc.wait(timeout=3)
            return "killed"


def run_one(root, cfg, clients, repeat):
    sc, lc, ec, mc = cfg["server"], cfg["load_generator"], cfg["experiment"], cfg["monitor"]
    run_dir = root / "experiments" / "raw-data" / f"clients_{clients}" / f"run_{repeat}"
    run_dir.mkdir(parents=True, exist_ok=True)
    for name in ("requests.csv", "summary.json", "cpu.csv", "meta.json", "server.log"):
        p = run_dir / name
        if p.exists():
            p.unlink()

    server_exe = root / sc["executable"]
    load_exe = root / lc["executable"]
    if not server_exe.is_file():
        raise FileNotFoundError(f"Server exe missing: {server_exe}")
    if not load_exe.is_file():
        raise FileNotFoundError(f"Loadgen exe missing: {load_exe}")

    server_cmd = [str(server_exe), "--ip", sc["ip"], "--port", str(sc["port"]),
                 "--threads", str(sc["threads"]), "--max-payload", str(sc["max_payload"]),
                 "--idle-timeout", str(sc["idle_timeout"])]
    load_cmd = [str(load_exe), "--ip", lc["ip"], "--port", str(lc["port"]),
                "--clients", str(clients), "--payload", str(lc["payload"]),
                "--warmup", str(lc["warmup"]), "--duration", str(lc["duration"]),
                "--timeout-ms", str(lc["timeout_ms"]), "--out-dir", str(run_dir)]

    server_proc = load_proc = monitor_proc = None
    server_log = open(run_dir / "server.log", "w", encoding="utf-8")
    start_time = now_iso()
    status, error, load_rc, monitor_rc, stop_method = "failed", None, None, None, "not_stopped"
    try:
        launch_opts = {"stdout": server_log, "stderr": subprocess.STDOUT}
        if os.name == "nt":
            launch_opts["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        server_proc = subprocess.Popen(server_cmd, **launch_opts)
        wait_for_server("127.0.0.1", sc["port"], server_proc)

        load_proc = subprocess.Popen(load_cmd, stdout=subprocess.PIPE, stderr=None, text=True, bufsize=1)
        marker = load_proc.stdout.readline().strip()
        if marker != "START":
            raise RuntimeError(f"Loadgen START marker expected; got {marker!r}")

        cpu_path = run_dir / "cpu.csv"
        monitor_cmd = [sys.executable, str(root / "experiments" / "scripts" / "cpu_monitor.py"),
                      "--pid", str(server_proc.pid), "--output", str(cpu_path),
                      "--start-delay", str(lc["warmup"]), "--duration", str(lc["duration"]),
                      "--interval", str(mc["cpu_interval"])]
        monitor_proc = subprocess.Popen(monitor_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        load_rc = load_proc.wait(timeout=lc["warmup"] + lc["duration"] + lc["timeout_ms"] / 1000 + 30)
        monitor_rc = monitor_proc.wait(timeout=lc["duration"] + mc["cpu_interval"] * 3 + 15)
        status = "completed" if load_rc == 0 and monitor_rc == 0 else "completed_with_errors"
        if load_rc != 0 or monitor_rc != 0:
            error = f"loadgen_rc={load_rc}; cpu_monitor_rc={monitor_rc}"
    except Exception as exc:
        error = str(exc)
        status = "failed"
        for child in (load_proc, monitor_proc):
            if child and child.poll() is None:
                child.kill()
                child.wait()
    finally:
        if server_proc:
            stop_method = stop_server(server_proc)
        server_log.close()

    meta = {
        "status": status, "error": error, "clients": clients, "repeat": repeat,
        "started_at": start_time, "ended_at": now_iso(),
        "server_pid": server_proc.pid if server_proc else None,
        "server_stop_method": stop_method, "server_command": server_cmd,
        "load_generator_command": load_cmd, "load_generator_exit_code": load_rc,
        "cpu_monitor_exit_code": monitor_rc, "config": cfg,
        "platform": sys.platform, "python_version": sys.version
    }
    with open(run_dir / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    print(f"[{status}] clients={clients} repeat={repeat}: {run_dir.relative_to(root)}")
    if error:
        print("  Details:", error)
    return status


def main():
    ap = argparse.ArgumentParser(description="Run TCP benchmark experiment matrix")
    ap.add_argument("--config", default="experiments/configs/default.json")
    ap.add_argument("--project-root", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    script_folder = Path(__file__).resolve().parent
    root = Path(args.project_root).resolve() if args.project_root else script_folder.parent.parent.resolve()
    with open(root / args.config, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    runs = [(c, r) for r in range(1, cfg["experiment"]["repeats"] + 1) for c in cfg["experiment"]["clients"]]
    if args.dry_run:
        print("Run order (interleaved):")
        for n, (c, r) in enumerate(runs, 1):
            print(f"{n:02d}. clients={c}, repeat={r}")
        return

    results = []
    for idx, (clients, repeat) in enumerate(runs):
        results.append(run_one(root, cfg, clients, repeat))
        if idx < len(runs) - 1:
            pause = cfg["experiment"]["break_between_runs"]
            print(f"Pausing {pause} seconds before next run...")
            time.sleep(pause)
    summary = root / "experiments" / "raw-data" / "runner_summary.csv"
    with open(summary, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["run_index", "clients", "repeat", "status"])
        for idx, ((clients, repeat), result) in enumerate(zip(runs, results), 1):
            writer.writerow([idx, clients, repeat, result])
    print(f"\nFinished {len(results)} runs. Summary: {summary}")
    print(f"Successful: {results.count('completed')}/{len(results)}")
    if any(s != "completed" for s in results):
        print("Check per-run meta.json and server.log for failures.")


if __name__ == "__main__":
    main()
