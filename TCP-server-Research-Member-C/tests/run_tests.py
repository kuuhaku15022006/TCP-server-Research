#!/usr/bin/env python3
"""Chạy bộ test Server (vai trò E).

Tự khởi động Server với --max-payload 65536 --idle-timeout 2, chạy test_server, rồi tắt Server.
Ví dụ:
    python tests/run_tests.py
    python tests/run_tests.py --server build/src/server/server --test build/tests/test_server
Mã thoát 0 nếu mọi test đạt.
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def find_exe(name, explicit=None):
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None
    exe = name + (".exe" if os.name == "nt" else "")
    for base in [ROOT / "build" / "src" / "server", ROOT / "build" / "src" / "load-generator", ROOT / "build" / "tests",
                 ROOT / "build" / "src" / "server" / "Release", ROOT / "build" / "tests" / "Release",
                 ROOT / "build" / "src" / "load-generator" / "Release", ROOT / "build", ROOT]:
        if (base / exe).exists():
            return base / exe
    return None


def wait_port(ip, port, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((ip, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", help="đường dẫn file server")
    ap.add_argument("--test", help="đường dẫn file test_server")
    ap.add_argument("--ip", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9300)
    args = ap.parse_args()

    server = find_exe("server", args.server)
    test = find_exe("test_server", args.test)
    if not server or not test:
        print("Không tìm thấy server hoặc test_server. Hãy build trước (xem README) hoặc dùng --server/--test.")
        return 2

    srv = subprocess.Popen([str(server), "--ip", args.ip, "--port", str(args.port), "--threads", "64",
                            "--max-payload", "65536", "--idle-timeout", "2"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    try:
        if not wait_port(args.ip, args.port):
            print("Server không khởi động được")
            return 2
        res = subprocess.run([str(test), "--ip", args.ip, "--port", str(args.port),
                              "--max-payload", "65536", "--idle-timeout", "2"])
        rc = res.returncode
    finally:
        if os.name == "nt":
            srv.terminate()
        else:
            srv.send_signal(signal.SIGINT)
        try:
            _, err = srv.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            srv.kill()
            _, err = srv.communicate()
    print("\n--- Thống kê Server sau khi test ---")
    print((err or "").strip().splitlines()[-3:] and "\n".join((err or "").strip().splitlines()[-3:]))
    return rc


if __name__ == "__main__":
    sys.exit(main())
