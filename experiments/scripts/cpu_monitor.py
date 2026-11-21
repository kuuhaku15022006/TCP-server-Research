import argparse
import csv
import os
import time
from datetime import datetime

import psutil


def monitor_process(pid: int, output_file: str, duration: float, interval: float, start_delay: float):
    proc = psutil.Process(pid)
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    if start_delay > 0:
        time.sleep(start_delay)
    if not proc.is_running():
        raise RuntimeError(f"Process PID {pid} is not running when monitoring begins")

    # Prime psutil; the first CPU percentage sample is not meaningful.
    proc.cpu_percent(interval=None)
    start = time.perf_counter()
    stop_at = start + duration

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "elapsed_sec", "cpu_percent", "memory_mb"])
        while time.perf_counter() < stop_at:
            remaining = stop_at - time.perf_counter()
            time.sleep(min(interval, max(0.0, remaining)))
            if not proc.is_running():
                break
            try:
                cpu = proc.cpu_percent(interval=None)
                rss_mb = proc.memory_info().rss / (1024 * 1024)
                elapsed = time.perf_counter() - start
                writer.writerow([
                    datetime.now().isoformat(timespec="milliseconds"),
                    round(elapsed, 3), round(cpu, 2), round(rss_mb, 2)
                ])
                f.flush()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
    print(f"CPU samples saved: {output_file}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Monitor CPU/RAM for a target process")
    p.add_argument("--pid", type=int, required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--duration", type=float, default=20)
    p.add_argument("--interval", type=float, default=1)
    p.add_argument("--start-delay", type=float, default=0)
    a = p.parse_args()
    monitor_process(a.pid, a.output, a.duration, a.interval, a.start_delay)
