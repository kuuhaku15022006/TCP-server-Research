# Khảo sát ảnh hưởng của số lượng Client đồng thời đến hiệu năng TCP Server

TCP Echo Server (C++17, blocking socket + thread pool) và load generator chạy ở hai process riêng.
Đo latency trung bình, p95, throughput, error rate và CPU ở 1 / 10 / 50 Client.

## Cấu trúc
```
src/common/          protocol.hpp, net.hpp dùng chung (frame, socket Windows/Linux)
src/server/          TCP Echo Server
src/load-generator/  load generator (loadgen)
tests/               test_server.cpp (C++), run_tests.py, verify_results.py (kiểm chứng độc lập)
experiments/
  configs/           default.json (chính thức), quick.json (chạy thử nhanh)
  raw-data/          dữ liệu thô: c{N}_r{k}/
  analysis/          run_experiments.py (runner), analyze.py (thống kê + biểu đồ)
  figures/           biểu đồ xuất ra
docs/                tài liệu
```

## Yêu cầu
- Trình biên dịch C++17 (g++ / clang++ / MSVC) và CMake ≥ 3.10 (hoặc dùng g++ trực tiếp)
- Python 3.8+ và `pip install -r requirements.txt`


