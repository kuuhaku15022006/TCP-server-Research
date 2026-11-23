// main.cpp - LOAD GENERATOR cho Echo Benchmark (vai trò C)
//
// Hành vi (xem docs/DESIGN-DECISIONS.md mục 3-6):
//   - N Client logic, mỗi Client = 1 thread + 1 kết nối TCP riêng (blocking).
//   - Closed-loop: gửi 1 request, chờ đủ response, gửi tiếp ngay.
//   - Tạo đủ N kết nối trước, rồi tất cả bắt đầu cùng lúc tại t0 (in dòng "START" ra stdout).
//   - Warm-up [t0, t0+warmup) bỏ khỏi thống kê; đo chính thức [t0+warmup, t0+warmup+duration).
//   - Request được tính nếu THỜI ĐIỂM GỬI nằm trong cửa sổ đo.
//   - Latency = từ ngay trước send đến khi nhận đủ frame response (µs, steady_clock).
//   - Timeout / sai requestId-payload / lỗi kết nối = 1 lỗi; sau lỗi đóng và mở lại kết nối.
//   - Ghi requests.csv và summary.json vào --out-dir.
//
// Chạy: ./loadgen --ip 127.0.0.1 --port 9000 --clients 10 --warmup 5 --duration 20 --out-dir <dir>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net.h"
#include "protocol.h"

using Clock = std::chrono::steady_clock;

struct Config {
    std::string ip = "127.0.0.1";
    uint16_t port = 9000;
    int clients = 10;
    uint32_t payload = proto::DEFAULT_PAYLOAD;
    int warmup_s = 5;
    int duration_s = 20;
    int timeout_ms = 5000;
    std::string out_dir;
};

enum class Status : uint8_t { Ok, Timeout, Mismatch, ConnError };

static const char* status_str(Status s) {
    switch (s) {
        case Status::Ok: return "ok";
        case Status::Timeout: return "timeout";
        case Status::Mismatch: return "mismatch";
        case Status::ConnError: return "conn_error";
    }
    return "conn_error";
}

struct Record {
    uint32_t client_id;
    uint64_t request_id;
    int64_t send_us;     // tính từ t0
    int64_t latency_us;  // -1 nếu không phải ok
    Status status;
};

// Đồng bộ khởi động giữa các Client
struct Barrier {
    std::mutex mu;
    std::condition_variable cv;
    int ready = 0;
    bool go = false;
    Clock::time_point t0;
};

static socket_t try_connect(const Config& cfg) {
    socket_t s = ::socket(AF_INET, SOCK_STREAM, 0);
    if (s == INVALID_SOCK) return INVALID_SOCK;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg.port);
    if (inet_pton(AF_INET, cfg.ip.c_str(), &addr.sin_addr) != 1 ||
        ::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        net::close_socket(s);
        return INVALID_SOCK;
    }
    net::set_nodelay(s);
    net::set_recv_timeout(s, cfg.timeout_ms);
    return s;
}

// Gửi 1 request, nhận đủ 1 response, kiểm tra. Trả về Status.
static Status do_request(socket_t s, const std::vector<uint8_t>& req, std::vector<uint8_t>& resp, uint64_t rid) {
    if (net::send_all(s, req.data(), req.size()) != net::IoResult::Ok) return Status::ConnError;

    uint8_t hdr[proto::LENGTH_FIELD_SIZE];
    net::IoResult r = net::recv_all(s, hdr, sizeof(hdr));
    if (r == net::IoResult::Timeout) return Status::Timeout;
    if (r != net::IoResult::Ok) return Status::ConnError;

    uint32_t len = proto::get_u32(hdr);
    // Response phải dài đúng bằng request (echo); sai thì không đọc tiếp (tránh đọc length bậy)
    if (len != req.size() - proto::LENGTH_FIELD_SIZE) return Status::Mismatch;

    resp.resize(proto::LENGTH_FIELD_SIZE + len);
    std::memcpy(resp.data(), hdr, sizeof(hdr));
    r = net::recv_all(s, resp.data() + proto::LENGTH_FIELD_SIZE, len);
    if (r == net::IoResult::Timeout) return Status::Timeout;
    if (r != net::IoResult::Ok) return Status::ConnError;

    return proto::verify_frame(resp.data(), resp.size(), rid) ? Status::Ok : Status::Mismatch;
}

static void client_thread(uint32_t client_id, const Config& cfg, Barrier& bar, std::vector<Record>& out) {
    // 1) Kết nối trước (thử lại tối đa ~3 giây)
    socket_t s = INVALID_SOCK;
    for (int i = 0; i < 30 && s == INVALID_SOCK; ++i) {
        s = try_connect(cfg);
        if (s == INVALID_SOCK) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 2) Chờ tất cả Client sẵn sàng rồi cùng bắt đầu
    Clock::time_point t0;
    {
        std::unique_lock<std::mutex> lk(bar.mu);
        bar.ready++;
        bar.cv.notify_all();
        bar.cv.wait(lk, [&] { return bar.go; });
        t0 = bar.t0;
    }
    const auto win_start = t0 + std::chrono::seconds(cfg.warmup_s);
    const auto win_end = win_start + std::chrono::seconds(cfg.duration_s);

    std::vector<uint8_t> req(proto::HEADER_SIZE + cfg.payload), resp;
    proto::put_u32(req.data(), proto::REQUEST_ID_SIZE + cfg.payload);
    out.reserve(1 << 16);

    uint32_t seq = 0;
    for (;;) {
        auto now = Clock::now();
        if (now >= win_end) break;

        // Mất kết nối thì mở lại (đếm là 1 lỗi conn_error nếu rơi vào cửa sổ đo)
        if (s == INVALID_SOCK) {
            s = try_connect(cfg);
            if (s == INVALID_SOCK) {
                if (now >= win_start)
                    out.push_back({client_id, 0, std::chrono::duration_cast<std::chrono::microseconds>(now - t0).count(),
                                   -1, Status::ConnError});
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
        }

        ++seq;
        const uint64_t rid = proto::make_request_id(client_id, seq);
        proto::put_u64(req.data() + proto::LENGTH_FIELD_SIZE, rid);
        proto::fill_payload(req.data() + proto::HEADER_SIZE, cfg.payload, rid);

        const auto ts = Clock::now();
        Status st = do_request(s, req, resp, rid);
        const auto te = Clock::now();

        if (ts >= win_start && ts < win_end) {
            Record rec;
            rec.client_id = client_id;
            rec.request_id = rid;
            rec.send_us = std::chrono::duration_cast<std::chrono::microseconds>(ts - t0).count();
            rec.latency_us = (st == Status::Ok) ? std::chrono::duration_cast<std::chrono::microseconds>(te - ts).count() : -1;
            rec.status = st;
            out.push_back(rec);
        }
        if (st != Status::Ok) {  // luồng byte có thể lệch -> bỏ kết nối này, mở lại ở vòng sau
            net::close_socket(s);
            s = INVALID_SOCK;
        }
    }
    if (s != INVALID_SOCK) net::close_socket(s);
}

static bool parse_args(int argc, char** argv, Config& c) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        const char* v = (i + 1 < argc) ? argv[i + 1] : nullptr;
        if (!v) { std::cerr << "Missing value for " << a << "\n"; return false; }
        if (a == "--ip") c.ip = v;
        else if (a == "--port") c.port = static_cast<uint16_t>(std::atoi(v));
        else if (a == "--clients") c.clients = std::atoi(v);
        else if (a == "--payload") c.payload = static_cast<uint32_t>(std::atoi(v));
        else if (a == "--warmup") c.warmup_s = std::atoi(v);
        else if (a == "--duration") c.duration_s = std::atoi(v);
        else if (a == "--timeout-ms") c.timeout_ms = std::atoi(v);
        else if (a == "--out-dir") c.out_dir = v;
        else { std::cerr << "Unknown option " << a << "\n"; return false; }
        ++i;
    }
    if (c.out_dir.empty()) { std::cerr << "--out-dir is required\n"; return false; }
    if (c.clients < 1 || c.duration_s < 1 || c.warmup_s < 0 || c.timeout_ms < 1 || c.payload > proto::DEFAULT_MAX_PAYLOAD) {
        std::cerr << "Invalid parameter value\n";
        return false;
    }
    return true;
}

int main(int argc, char** argv) {
    Config cfg;
    if (!parse_args(argc, argv, cfg)) {
        std::cerr << "Usage: loadgen --out-dir DIR [--ip IP] [--port N] [--clients N] [--payload B]"
                     " [--warmup S] [--duration S] [--timeout-ms MS]\n";
        return 2;
    }
    if (!net::init()) { std::cerr << "Winsock init failed\n"; return 1; }
    std::error_code ec;
    std::filesystem::create_directories(cfg.out_dir, ec);
    if (ec) { std::cerr << "Cannot create out-dir: " << ec.message() << "\n"; return 1; }

    std::vector<std::vector<Record>> per_client(cfg.clients);
    std::vector<std::thread> threads;
    Barrier bar;
    for (int i = 0; i < cfg.clients; ++i)
        threads.emplace_back(client_thread, static_cast<uint32_t>(i + 1), std::cref(cfg), std::ref(bar), std::ref(per_client[i]));

    {   // Chờ đủ N Client sẵn sàng rồi phát lệnh bắt đầu
        std::unique_lock<std::mutex> lk(bar.mu);
        bar.cv.wait(lk, [&] { return bar.ready == cfg.clients; });
        bar.t0 = Clock::now();
        bar.go = true;
    }
    bar.cv.notify_all();
    std::printf("START\n");
    std::fflush(stdout);

    for (auto& t : threads) t.join();

    // ---- Thống kê ----
    uint64_t n_ok = 0, n_timeout = 0, n_mismatch = 0, n_conn = 0;
    std::vector<int64_t> lat;
    for (auto& v : per_client)
        for (auto& r : v) {
            switch (r.status) {
                case Status::Ok: ++n_ok; lat.push_back(r.latency_us); break;
                case Status::Timeout: ++n_timeout; break;
                case Status::Mismatch: ++n_mismatch; break;
                case Status::ConnError: ++n_conn; break;
            }
        }
    std::sort(lat.begin(), lat.end());
    const uint64_t n_err = n_timeout + n_mismatch + n_conn;
    double mean = 0, p95 = 0;
    int64_t mn = 0, mx = 0;
    if (!lat.empty()) {
        long double sum = 0;
        for (auto x : lat) sum += x;
        mean = static_cast<double>(sum / lat.size());
        size_t idx = static_cast<size_t>(std::ceil(0.95 * lat.size()));  // nearest-rank
        p95 = static_cast<double>(lat[idx - 1]);
        mn = lat.front();
        mx = lat.back();
    }
    const double throughput = static_cast<double>(n_ok) / cfg.duration_s;
    const double err_rate = (n_ok + n_err) ? static_cast<double>(n_err) / (n_ok + n_err) : 0.0;

    // ---- requests.csv ----
    {
        std::string path = cfg.out_dir + "/requests.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        if (!f) { std::cerr << "Cannot write " << path << "\n"; return 1; }
        std::fprintf(f, "client_id,request_id,send_time_us,latency_us,status\n");
        for (auto& v : per_client)
            for (auto& r : v) {
                if (r.status == Status::Ok)
                    std::fprintf(f, "%u,%llu,%lld,%lld,%s\n", r.client_id, (unsigned long long)r.request_id,
                                 (long long)r.send_us, (long long)r.latency_us, status_str(r.status));
                else
                    std::fprintf(f, "%u,%llu,%lld,,%s\n", r.client_id, (unsigned long long)r.request_id,
                                 (long long)r.send_us, status_str(r.status));
            }
        std::fclose(f);
    }

    // ---- summary.json ----
    {
        std::string path = cfg.out_dir + "/summary.json";
        FILE* f = std::fopen(path.c_str(), "w");
        if (!f) { std::cerr << "Cannot write " << path << "\n"; return 1; }
        std::fprintf(f,
            "{\n"
            "  \"clients\": %d, \"payload_bytes\": %u, \"warmup_s\": %d, \"measure_s\": %d,\n"
            "  \"requests_ok\": %llu,\n"
            "  \"errors\": {\"timeout\": %llu, \"mismatch\": %llu, \"conn_error\": %llu},\n"
            "  \"error_rate\": %.6f,\n"
            "  \"throughput_rps\": %.3f,\n"
            "  \"latency_us\": {\"mean\": %.3f, \"p95\": %.3f, \"min\": %lld, \"max\": %lld}\n"
            "}\n",
            cfg.clients, cfg.payload, cfg.warmup_s, cfg.duration_s,
            (unsigned long long)n_ok, (unsigned long long)n_timeout, (unsigned long long)n_mismatch,
            (unsigned long long)n_conn, err_rate, throughput, mean, p95, (long long)mn, (long long)mx);
        std::fclose(f);
    }

    std::printf("clients=%d ok=%llu errors=%llu (timeout=%llu mismatch=%llu conn=%llu)\n", cfg.clients,
                (unsigned long long)n_ok, (unsigned long long)n_err, (unsigned long long)n_timeout,
                (unsigned long long)n_mismatch, (unsigned long long)n_conn);
    std::printf("throughput=%.1f req/s  latency mean=%.1f us  p95=%.1f us  error_rate=%.4f\n", throughput, mean, p95,
                err_rate);
    std::printf("results written to %s\n", cfg.out_dir.c_str());
    net::cleanup();
    return 0;
}
