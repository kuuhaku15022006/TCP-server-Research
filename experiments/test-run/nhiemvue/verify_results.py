#!/usr/bin/env python3
"""
Recalculate success count, throughput and error rate from loadgen requests.csv.

Example (Windows CMD / PowerShell):
python verify_results.py --csv-1 "requests.csv" --csv-10 "requests(1).csv" --csv-20 "requests(2).csv" --duration 5
"""
import argparse
import csv
import sys
from pathlib import Path


def analyze_csv(path: Path, label: str, duration_s: float) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"Không tìm thấy file: {path}")

    total = 0
    success = 0
    errors_by_status = {}
    client_ids = set()

    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {"client_id", "status"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError(
                f"{path}: CSV cần có các cột client_id và status."
            )

        for row in reader:
            total += 1
            status = (row.get("status") or "").strip().lower()
            if status == "ok":
                success += 1
            else:
                errors_by_status[status or "empty"] = (
                    errors_by_status.get(status or "empty", 0) + 1
                )
            client_id = (row.get("client_id") or "").strip()
            if client_id:
                client_ids.add(client_id)

    errors = total - success
    throughput = success / duration_s
    error_rate = (errors / total * 100.0) if total else 0.0

    return {
        "case": label,
        "file": str(path),
        "clients_seen": len(client_ids),
        "total_requests": total,
        "successful_requests": success,
        "errors": errors,
        "throughput_req_s": throughput,
        "error_rate_percent": error_rate,
        "error_details": errors_by_status,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Kiểm chứng số request thành công, throughput và error rate."
    )
    parser.add_argument("--csv-1", required=True, help="CSV lần chạy 1 Client")
    parser.add_argument("--csv-10", required=True, help="CSV lần chạy 10 Client")
    parser.add_argument("--csv-20", required=True, help="CSV lần chạy 20 Client")
    parser.add_argument(
        "--duration", type=float, default=5,
        help="Thời gian đo chính thức tính bằng giây (mặc định: 5)"
    )
    parser.add_argument(
        "--output", default="verification_summary.csv",
        help="Tên file CSV tổng hợp (mặc định: verification_summary.csv)"
    )
    args = parser.parse_args()

    if args.duration <= 0:
        parser.error("--duration phải lớn hơn 0")

    cases = [
        ("1 Client", Path(args.csv_1)),
        ("10 Client", Path(args.csv_10)),
        ("20 Client", Path(args.csv_20)),
    ]

    try:
        results = [
            analyze_csv(path, label, args.duration)
            for label, path in cases
        ]
    except (OSError, ValueError) as exc:
        print(f"LỖI: {exc}", file=sys.stderr)
        return 1

    print("\nKET QUA KIEM CHUNG")
    print("-" * 92)
    print(f"{'Trường hợp':<12} {'Tổng request':>14} {'Thành công':>14} "
          f"{'Lỗi':>8} {'Throughput (req/s)':>21} {'Error rate':>12}")
    print("-" * 92)
    for r in results:
        print(f"{r['case']:<12} {r['total_requests']:>14,} "
              f"{r['successful_requests']:>14,} {r['errors']:>8,} "
              f"{r['throughput_req_s']:>21,.1f} "
              f"{r['error_rate_percent']:>11.4f}%")
        if r["error_details"]:
            print(f"  Chi tiết lỗi: {r['error_details']}")

    output_path = Path(args.output)
    fieldnames = [
        "case", "file", "clients_seen", "total_requests",
        "successful_requests", "errors", "throughput_req_s",
        "error_rate_percent", "error_details"
    ]
    with output_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            row = dict(r)
            row["error_details"] = str(r["error_details"])
            writer.writerow(row)

    print(f"\nĐã lưu bảng tổng hợp tại: {output_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
